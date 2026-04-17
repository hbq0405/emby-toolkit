# handler/resubscribe_service.py
import logging
import json
from database.connection import get_db_connection

logger = logging.getLogger(__name__)

class WashingService:
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
        """将 115/ffprobe 的 video_info 或 数据库的 asset_details 统一标准化为字符串标签"""
        # 局部导入 helpers 中成熟的分析引擎，避免循环依赖
        from tasks.helpers import _get_resolution_tier, _get_standardized_effect

        norm = {
            "resolution": "unknown",
            "codec": "unknown",
            "effect": "sdr",
            "original_lang": "",
            "audio_langs": set(),
            "sub_langs": set(),
            "size_gb": 0.0
        }

        if not info: return norm

        # 1. 提取基础字符串与特效
        if is_db_asset:
            norm["resolution"] = str(info.get('resolution_display', '')).lower()
            norm["codec"] = str(info.get('codec_display', '')).lower()
            
            effect_str = str(info.get('effect_display', '')).lower()
            if 'dovi' in effect_str or 'dv' in effect_str or 'dolby vision' in effect_str:
                if 'p8' in effect_str: norm["effect"] = 'dovi p8'
                elif 'p7' in effect_str: norm["effect"] = 'dovi p7'
                elif 'p5' in effect_str: norm["effect"] = 'dovi p5'
                else: norm["effect"] = 'dovi'
            elif 'hdr10+' in effect_str: norm["effect"] = 'hdr10+'
            elif 'hdr10' in effect_str: norm["effect"] = 'hdr10'
            elif 'hdr' in effect_str: norm["effect"] = 'hdr'
            else: norm["effect"] = 'sdr'
        else:
            # ★ 核心修复：调用 helpers.py 的成熟引擎分析视频流
            
            # A. 分辨率 (通过宽高计算)
            width = info.get('width') or info.get('Width') or 0
            height = info.get('height') or info.get('Height') or 0
            try:
                width, height = int(width), int(height)
            except (ValueError, TypeError):
                width, height = 0, 0

            if width > 0 or height > 0:
                _, res_str = _get_resolution_tier(width, height)
                norm["resolution"] = res_str.lower()
            else:
                # 兜底：如果流里没有宽高，尝试解析 resolution 字符串 (如 "3840x2160")
                raw_res = str(info.get('resolution', '')).lower()
                if 'x' in raw_res:
                    try:
                        w, h = map(int, raw_res.split('x'))
                        _, res_str = _get_resolution_tier(w, h)
                        norm["resolution"] = res_str.lower()
                    except:
                        norm["resolution"] = raw_res
                else:
                    norm["resolution"] = raw_res

            # B. 编码 (兼容 ffprobe 字段)
            raw_codec = str(info.get('codec') or info.get('video_codec') or info.get('codec_name') or '').lower()
            if raw_codec in ['hevc', 'h265', 'x265']: norm["codec"] = 'hevc'
            elif raw_codec in ['h264', 'avc', 'x264']: norm["codec"] = 'h264'
            else: norm["codec"] = raw_codec

            # C. 特效 (调用 helpers 引擎)
            filename = str(info.get('filename') or info.get('name') or info.get('Path') or '').lower()
            effect_tag = _get_standardized_effect(filename, info)
            # 将 helpers 的输出 (如 dovi_p8, dovi_other) 映射为洗版规则的标准名称 (dovi p8, dovi)
            norm["effect"] = effect_tag.replace('_', ' ').replace('dovi other', 'dovi').strip()

        # 2. 语言与大小归一化
        def _safe_parse_list(val):
            if isinstance(val, list): return val
            if isinstance(val, str):
                try:
                    import ast
                    parsed = ast.literal_eval(val)
                    if isinstance(parsed, list): return parsed
                except: pass
            return []
            
        if is_db_asset:
            raw_audio_langs = _safe_parse_list(info.get('audio_languages_raw', []))
            raw_sub_langs = _safe_parse_list(info.get('subtitle_languages_raw', []))
            norm["size_gb"] = info.get('size_bytes', 0) / (1024**3)
        else:
            raw_audio_langs = _safe_parse_list(info.get('audio_langs', []))
            raw_sub_langs = _safe_parse_list(info.get('sub_langs', []))
            norm["size_gb"] = info.get('_file_size', 0) / (1024**3)
            
        norm["audio_langs"] = {cls._normalize_lang(a) for a in raw_audio_langs if a}
        norm["sub_langs"] = {cls._normalize_lang(s) for s in raw_sub_langs if s}

        raw_original_lang = (
            info.get('_original_lang')
            or info.get('original_lang')
            or info.get('lang_code')
            or ""
        )
        norm["original_lang"] = cls._normalize_lang(raw_original_lang)

        return norm

    @classmethod
    def _match_priority(cls, norm_info: dict, priority_rule: dict) -> tuple[bool, str]:
        """★ 核心重构：严格白名单匹配逻辑 (宁缺毋滥)"""
        
        # 1. 分辨率 (白名单)
        req_res = priority_rule.get('resolution', [])
        if req_res:
            req_res_lower = [r.lower() for r in req_res]
            file_res = norm_info['resolution']
            match = False
            for r in req_res_lower:
                # 兼容 4K 和 2160p 的同义词
                if r == file_res or (r in ['4k', '2160p'] and file_res in ['4k', '2160p']):
                    match = True
                    break
            if not match: return False, f"分辨率未命中 ({file_res})"
            
        # 2. 编码 (白名单)
        req_codec = priority_rule.get('codec', [])
        if req_codec:
            req_codec_lower = [c.lower() for c in req_codec]
            file_codec = norm_info['codec']
            match = False
            for c in req_codec_lower:
                # 兼容 hevc 10bit 这种带后缀的情况
                if c in file_codec or file_codec in c: 
                    match = True
                    break
                # 兼容同义词
                if c in ['hevc', 'h265'] and ('hevc' in file_codec or 'h265' in file_codec): match = True
                if c in ['avc', 'h264'] and ('avc' in file_codec or 'h264' in file_codec): match = True
            if not match: return False, f"编码未命中 ({file_codec})"
            
        # 3. 特效 (白名单)
        req_effect = priority_rule.get('effect', [])
        if req_effect:
            req_effect_lower = [e.lower() for e in req_effect]
            file_effect = norm_info['effect']
            if file_effect not in req_effect_lower:
                return False, f"特效未命中 ({file_effect})"
            
        # 4. 音轨 (必须包含，但“原语言=规则语言”时自动豁免)
        original_lang = norm_info.get('original_lang') or ""

        req_audio = priority_rule.get('audio', [])
        if req_audio:
            normalized_req_audio = {
                cls._normalize_lang(a) for a in req_audio if a
            }

            # ★ 原语言就是规则要求的音轨时，忽略这条音轨要求
            effective_req_audio = {
                a for a in normalized_req_audio
                if a and a != original_lang
            }

            if effective_req_audio:
                if not norm_info['audio_langs']:
                    return False, "未提取到音轨语言"
                if not any(a in norm_info['audio_langs'] for a in effective_req_audio):
                    return False, "缺少必须的音轨"

        # 5. 字幕 (必须包含，但“原语言=规则语言”时自动豁免)
        req_sub = priority_rule.get('subtitle', [])
        if req_sub:
            normalized_req_sub = {
                cls._normalize_lang(s) for s in req_sub if s
            }

            # ★ 原语言就是规则要求的字幕时，忽略这条字幕要求
            effective_req_sub = {
                s for s in normalized_req_sub
                if s and s != original_lang
            }

            if effective_req_sub:
                if not norm_info['sub_langs']:
                    return False, "未提取到字幕语言"
                if not any(s in norm_info['sub_langs'] for s in effective_req_sub):
                    return False, "缺少必须的字幕"
            
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
                fail_reasons.append(f"优先级{i+1}[{reason}]")
        return 0, " | ".join(fail_reasons)

    @classmethod
    def decide_washing_action(
            cls,
            new_video_info: dict,
            file_size: int,
            target_cid: str,
            media_type: str,
            tmdb_id: str,
            season_num: int=None,
            episode_num: int=None,
            original_lang: str=None
        ) -> tuple[str, str]:
        """
        核心决策函数
        返回: (ACTION, reason)
        ACTION: 'ACCEPT' (入库), 'REPLACE' (替换旧版), 'SKIP' (已有更好版本), 'REJECT' (不合格)
        """
        new_video_info = dict(new_video_info or {})
        new_video_info['_file_size'] = file_size
        new_video_info['_original_lang'] = original_lang
        norm_new = cls._normalize_info(new_video_info, is_db_asset=False)
        
        db_media_type = 'Movie' if media_type.lower() == 'movie' else 'Series'
        
        # 1. 查找适用的规则组
        rule_group = None
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT * FROM washing_priority_groups WHERE media_type = %s ORDER BY sort_order ASC", (db_media_type,))
                    for row in cursor.fetchall():
                        cids = row.get('target_cids', [])
                        if isinstance(cids, str):
                            try: cids = json.loads(cids)
                            except: cids = []
                            
                        if not cids or str(target_cid) in cids:
                            rule_group = dict(row)
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
            old_asset = dict(asset or {})
            old_asset['_original_lang'] = original_lang
            norm_old = cls._normalize_info(old_asset, is_db_asset=True)
            old_level, _ = cls.get_level(norm_old, priorities)
            if old_level == 0: old_level = 999 # 旧版不合格，视为最低级
            if old_level < best_old_level:
                best_old_level = old_level

        # 6. 核心对比逻辑 (直接对比命中的优先级阶梯)
        if new_level < best_old_level:
            return 'REPLACE', f"新版(优先级{new_level}) 优于 旧版(优先级{best_old_level if best_old_level!=999 else '不合格'})，执行洗版替换"
        elif new_level == best_old_level:
            return 'SKIP', f"新版(优先级{new_level}) 与旧版同级，跳过"
        else:
            return 'SKIP', f"新版(优先级{new_level}) 劣于 旧版(优先级{best_old_level})，跳过"