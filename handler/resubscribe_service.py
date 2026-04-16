# handler/resubscribe_service.py
import logging
import json
from database.connection import get_db_connection

logger = logging.getLogger(__name__)

class WashingService:
    # 统一的层级定义 (数字越大越好)
    RES_TIER = {"8k": 5, "4k": 4, "2160p": 4, "1080p": 3, "720p": 2, "480p": 1}
    CODEC_TIER = {"av1": 3, "hevc": 2, "h265": 2, "avc": 1, "h264": 1}
    EFFECT_TIER = {"dovi_p8": 7, "dovi_p7": 6, "dovi_p5": 5, "dovi_other": 4, "hdr10+": 3, "hdr10": 2, "hdr": 2, "sdr": 1}

    @classmethod
    def _normalize_lang(cls, lang_str: str) -> str:
        """将各种语言标识统一归一化为标准 3 字母代码"""
        if not lang_str: return ""
        lang_str = str(lang_str).lower().strip()
        if lang_str in ['chi', 'zho', 'zh', 'cn', 'tw', 'hk', 'chs', 'cht', '国语', '粤语', '简体', '繁体', '中文']: return 'chi'
        if lang_str in ['eng', 'en', '英语', '英文']: return 'eng'
        if lang_str in ['jpn', 'ja', 'jp', '日语', '日文']: return 'jpn'
        if lang_str in ['kor', 'ko', 'kr', '韩语', '韩文']: return 'kor'
        return lang_str

    @classmethod
    def _normalize_info(cls, info: dict, is_db_asset=False) -> dict:
        """将 115/ffprobe 的 video_info 或 数据库的 asset_details 统一标准化"""
        norm = {
            "res_tier": 0, "codec_tier": 0, "effect_tier": 0,
            "audio_langs": set(), "sub_langs": set(), "size_gb": 0.0
        }

        if not info: return norm

        if is_db_asset:
            # 解析数据库 asset_details_json 格式 (旧文件)
            res_str = str(info.get('resolution_display', '')).lower()
            norm["res_tier"] = cls.RES_TIER.get(res_str, 0)
            
            codec_str = str(info.get('codec_display', '')).lower()
            norm["codec_tier"] = max([v for k, v in cls.CODEC_TIER.items() if k in codec_str] + [0])
            
            effect_str = str(info.get('effect_display', '')).lower()
            if 'dovi' in effect_str or 'dolby vision' in effect_str:
                if 'p8' in effect_str: norm["effect_tier"] = 7
                elif 'p7' in effect_str: norm["effect_tier"] = 6
                elif 'p5' in effect_str: norm["effect_tier"] = 5
                else: norm["effect_tier"] = 4
            else:
                norm["effect_tier"] = cls.EFFECT_TIER.get(effect_str, 1)

            norm["audio_langs"] = {cls._normalize_lang(a) for a in info.get('audio_languages_raw', [])}
            norm["sub_langs"] = {cls._normalize_lang(s) for s in info.get('subtitle_languages_raw', [])}
            norm["size_gb"] = info.get('size_bytes', 0) / (1024**3)
        else:
            # 解析 115/ffprobe 的 video_info 格式 (新文件)
            res_str = str(info.get('resolution', '')).lower()
            norm["res_tier"] = cls.RES_TIER.get(res_str, 0)
            
            codec_str = str(info.get('codec', '')).lower()
            norm["codec_tier"] = max([v for k, v in cls.CODEC_TIER.items() if k in codec_str] + [0])
            
            effect_str = str(info.get('effect', '')).lower()
            if 'dv' in effect_str or 'dovi' in effect_str:
                if 'p8' in effect_str: norm["effect_tier"] = 7
                elif 'p7' in effect_str: norm["effect_tier"] = 6
                elif 'p5' in effect_str: norm["effect_tier"] = 5
                else: norm["effect_tier"] = 4
            else:
                norm["effect_tier"] = cls.EFFECT_TIER.get(effect_str, 1)
                
            # ★ 严格读取由 ffprobe/中心缓存 提取的真实语言数组
            norm["audio_langs"] = {cls._normalize_lang(a) for a in info.get('audio_langs', [])}
            norm["sub_langs"] = {cls._normalize_lang(s) for s in info.get('sub_langs', [])}
            
            norm["size_gb"] = info.get('_file_size', 0) / (1024**3)

        return norm

    @classmethod
    def _match_priority(cls, norm_info: dict, priority_rule: dict) -> tuple[bool, str]:
        """检查标准化信息是否满足某一个优先级规则 (宁缺毋滥)"""
        # 1. 分辨率
        req_res = priority_rule.get('resolution', [])
        if req_res:
            req_tier = min([cls.RES_TIER.get(r.lower(), 0) for r in req_res])
            if norm_info['res_tier'] < req_tier: return False, f"分辨率未达标"
            
        # 2. 编码
        req_codec = priority_rule.get('codec', [])
        if req_codec:
            req_tier = min([cls.CODEC_TIER.get(c.lower(), 0) for c in req_codec])
            if norm_info['codec_tier'] < req_tier: return False, f"编码未达标"
            
        # 3. 特效
        req_effect = priority_rule.get('effect', [])
        if req_effect:
            req_tier = min([cls.EFFECT_TIER.get(e.lower(), 0) for e in req_effect])
            if norm_info['effect_tier'] < req_tier: return False, f"特效未达标"
            
        # 4. 音轨 (宁缺毋滥：如果规则要求了，但文件没提取到，直接拦截！)
        req_audio = priority_rule.get('audio', [])
        if req_audio:
            if not norm_info['audio_langs']: return False, "未提取到音轨语言"
            if not any(a in norm_info['audio_langs'] for a in req_audio): return False, f"缺少必须的音轨"
            
        # 5. 字幕 (宁缺毋滥)
        req_sub = priority_rule.get('subtitle', [])
        if req_sub:
            if not norm_info['sub_langs']: return False, "未提取到字幕语言"
            if not any(s in norm_info['sub_langs'] for s in req_sub): return False, f"缺少必须的字幕"
            
        # 6. 文件大小
        min_size = priority_rule.get('min_size_gb')
        max_size = priority_rule.get('max_size_gb')
        if min_size and norm_info['size_gb'] > 0 and norm_info['size_gb'] < float(min_size): return False, f"体积过小"
        if max_size and norm_info['size_gb'] > 0 and norm_info['size_gb'] > float(max_size): return False, f"体积过大"

        return True, "匹配成功"

    @classmethod
    def get_level(cls, norm_info: dict, priorities: list) -> tuple[int, str]:
        """获取匹配的优先级级别 (1 是最高级，0 表示不合格)"""
        fail_reasons = []
        for i, p_rule in enumerate(priorities):
            is_match, reason = cls._match_priority(norm_info, p_rule)
            if is_match:
                return i + 1, f"命中优先级 {i + 1}"
            else:
                fail_reasons.append(f"P{i+1}[{reason}]")
        return 0, " | ".join(fail_reasons)

    @classmethod
    def decide_washing_action(cls, new_video_info: dict, file_size: int, target_cid: str, media_type: str, tmdb_id: str, season_num: int=None, episode_num: int=None) -> tuple[str, str]:
        """
        核心决策函数
        返回: (ACTION, reason)
        ACTION: 'ACCEPT' (入库), 'REPLACE' (替换旧版), 'SKIP' (已有更好版本), 'REJECT' (不合格)
        """
        new_video_info['_file_size'] = file_size
        norm_new = cls._normalize_info(new_video_info, is_db_asset=False)
        
        # ★ 核心修复：精准映射底层 media_type 到数据库存储的类型
        db_media_type = 'Movie' if media_type.lower() == 'movie' else 'Series'
        
        # 1. 查找适用的规则组
        rule_group = None
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT * FROM washing_priority_groups WHERE media_type = %s ORDER BY sort_order ASC", (db_media_type,))
                    for row in cursor.fetchall():
                        # ★ 核心修复：确保 JSONB 字段被正确解析为 Python 列表
                        cids = row.get('target_cids', [])
                        if isinstance(cids, str):
                            try: cids = json.loads(cids)
                            except: cids = []
                            
                        if not cids or str(target_cid) in cids:
                            rule_group = dict(row)
                            # 顺手解析 priorities
                            priorities = rule_group.get('priorities', [])
                            if isinstance(priorities, str):
                                try: priorities = json.loads(priorities)
                                except: priorities = []
                            rule_group['priorities'] = priorities
                            break
        except Exception as e:
            logger.warning(f"  ➜ 获取洗版优先级规则失败: {e}")

        # 如果没有配置规则，默认放行
        if not rule_group or not rule_group.get('priorities'):
            return 'ACCEPT', "未配置优先级规则，默认放行"

        priorities = rule_group['priorities']
        
        # 2. 评估新文件级别
        new_level, new_reason_detail = cls.get_level(norm_new, priorities)
        if new_level == 0:
            return 'REJECT', f"未达标 ({new_reason_detail})"

        # 3. 获取库内现有资产
        existing_assets = []
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    if db_media_type == 'Series' and season_num is not None and episode_num is not None:
                        cursor.execute("SELECT asset_details_json FROM media_metadata WHERE parent_series_tmdb_id = %s AND season_number = %s AND episode_number = %s AND item_type = 'Episode'", (str(tmdb_id), season_num, episode_num))
                    else:
                        cursor.execute("SELECT asset_details_json FROM media_metadata WHERE tmdb_id = %s AND item_type = 'Movie'", (str(tmdb_id),))
                    
                    row = cursor.fetchone()
                    if row and row['asset_details_json']:
                        assets = row['asset_details_json']
                        existing_assets = json.loads(assets) if isinstance(assets, str) else assets
        except Exception as e:
            logger.warning(f"  ➜ 获取现有资产失败: {e}")

        # 4. 如果库内没有旧文件，直接入库
        if not existing_assets:
            return 'ACCEPT', f"命中优先级 {new_level}，库内无旧版，直接入库"

        # 5. 评估旧文件级别，寻找最好的一个 (数字越小越好)
        best_old_level = 999
        for asset in existing_assets:
            norm_old = cls._normalize_info(asset, is_db_asset=True)
            old_level, _ = cls.get_level(norm_old, priorities)
            if old_level == 0: old_level = 999 # 旧版不合格，视为最低级
            if old_level < best_old_level:
                best_old_level = old_level

        # 6. 核心对比逻辑
        if new_level < best_old_level:
            return 'REPLACE', f"新版(优先级{new_level}) 优于 旧版(优先级{best_old_level if best_old_level!=999 else '不合格'})，执行洗版替换"
        elif new_level == best_old_level:
            return 'SKIP', f"新版(优先级{new_level}) 与旧版同级，跳过"
        else:
            return 'SKIP', f"新版(优先级{new_level}) 劣于 旧版(优先级{best_old_level})，跳过"