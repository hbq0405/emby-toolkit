# handler/resubscribe_service.py
import ast
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from database.connection import get_db_connection

logger = logging.getLogger(__name__)


class WashingService:
    @classmethod
    def _normalize_lang(cls, lang_str: str) -> str:
        """
        统一规则值：
        音轨：chi=国语, yue=粤语
        字幕：chi=简体, yue=繁体
        """
        if not lang_str:
            return ""

        lang_str = str(lang_str).lower().strip()

        # 中文相关：明确拆分
        if lang_str in ['chi', 'zho', 'zh', 'cn', 'chs', 'zh-cn', 'zh-hans', 'cmn', '国语', '中文', '简体', '简中']:
            return 'chi'
        if lang_str in ['yue', 'cht', 'zh-hk', 'zh-tw', 'hk', 'tw', '粤语', '繁体', '繁中']:
            return 'yue'

        # 其他语言
        if lang_str in ['eng', 'en', '英语', '英文']:
            return 'eng'
        if lang_str in ['jpn', 'ja', 'jp', '日语', '日文']:
            return 'jpn'
        if lang_str in ['kor', 'ko', 'kr', '韩语', '韩文']:
            return 'kor'

        return lang_str

    @classmethod
    def _safe_parse_jsonish(cls, val: Any) -> Any:
        if val is None:
            return None

        if isinstance(val, (dict, list)):
            return val

        if isinstance(val, str):
            s = val.strip()
            if not s:
                return None

            try:
                return json.loads(s)
            except Exception:
                try:
                    return ast.literal_eval(s)
                except Exception:
                    return None

        return None

    @classmethod
    def _safe_parse_list(cls, val: Any) -> List[Any]:
        parsed = cls._safe_parse_jsonish(val)
        return parsed if isinstance(parsed, list) else []

    @classmethod
    def _extract_media_source_info(cls, info: Any) -> Dict[str, Any]:
        """
        支持三种原始格式：
        1. [{"MediaSourceInfo": {...}}]
        2. {"MediaSourceInfo": {...}}
        3. {"MediaStreams": [...]}
        """
        parsed = cls._safe_parse_jsonish(info)
        if parsed is None:
            parsed = info

        if isinstance(parsed, list):
            if not parsed:
                return {}
            first = parsed[0]
            if isinstance(first, dict):
                if isinstance(first.get("MediaSourceInfo"), dict):
                    return first["MediaSourceInfo"]
                return first
            return {}

        if isinstance(parsed, dict):
            if isinstance(parsed.get("MediaSourceInfo"), dict):
                return parsed["MediaSourceInfo"]
            return parsed

        return {}

    @classmethod
    def _extract_media_streams(cls, info: Any) -> List[Dict[str, Any]]:
        media_source = cls._extract_media_source_info(info)
        streams = media_source.get("MediaStreams", [])
        return streams if isinstance(streams, list) else []

    @classmethod
    def _extract_video_stream(cls, info: Any) -> Dict[str, Any]:
        media_source = cls._extract_media_source_info(info)
        media_streams = cls._extract_media_streams(info)

        for stream in media_streams:
            if isinstance(stream, dict) and str(stream.get("Type", "")).lower() == "video":
                return stream

        if isinstance(media_source, dict) and str(media_source.get("Type", "")).lower() == "video":
            return media_source

        return {}

    @classmethod
    def _normalize_info(cls, info: dict) -> dict:
        """
        只按原始媒体信息标准化。
        不再兼容 asset_details_json。
        """
        from tasks.helpers import (
            _get_detected_languages_from_streams,
            _get_resolution_tier,
            _get_standardized_effect,
        )

        norm = {
            "resolution": "unknown",
            "codec": "unknown",
            "effect": "sdr",
            "original_lang": "",
            "audio_langs": set(),
            "sub_langs": set(),
            "size_gb": 0.0,
        }

        if not info:
            return norm

        parsed = cls._safe_parse_jsonish(info)
        if parsed is None:
            parsed = info

        media_source = cls._extract_media_source_info(parsed)
        media_streams = cls._extract_media_streams(parsed)
        video_stream = cls._extract_video_stream(parsed)

        # 1. 分辨率
        width = (
            video_stream.get("Width")
            or media_source.get("Width")
            or (parsed.get("width") if isinstance(parsed, dict) else 0)
            or (parsed.get("Width") if isinstance(parsed, dict) else 0)
            or 0
        )
        height = (
            video_stream.get("Height")
            or media_source.get("Height")
            or (parsed.get("height") if isinstance(parsed, dict) else 0)
            or (parsed.get("Height") if isinstance(parsed, dict) else 0)
            or 0
        )

        try:
            width, height = int(width), int(height)
        except (ValueError, TypeError):
            width, height = 0, 0

        if width > 0 or height > 0:
            _, res_str = _get_resolution_tier(width, height)
            norm["resolution"] = str(res_str).lower()

        # 2. 编码
        raw_codec = str(
            video_stream.get("Codec")
            or media_source.get("Codec")
            or (parsed.get("codec") if isinstance(parsed, dict) else "")
            or (parsed.get("video_codec") if isinstance(parsed, dict) else "")
            or (parsed.get("codec_name") if isinstance(parsed, dict) else "")
            or ""
        ).lower()

        if raw_codec in ["hevc", "h265", "x265"]:
            norm["codec"] = "hevc"
        elif raw_codec in ["h264", "avc", "x264"]:
            norm["codec"] = "h264"
        else:
            norm["codec"] = raw_codec or "unknown"

        # 3. 特效
        filename = str(
            (parsed.get("path") if isinstance(parsed, dict) else "")
            or (parsed.get("Path") if isinstance(parsed, dict) else "")
            or (parsed.get("filename") if isinstance(parsed, dict) else "")
            or (parsed.get("name") if isinstance(parsed, dict) else "")
            or media_source.get("Path")
            or ""
        ).lower()

        effect_tag = _get_standardized_effect(filename, video_stream or media_source or {})
        norm["effect"] = str(effect_tag).lower().strip()

        # 4. 音轨 / 字幕语言
        raw_audio_langs = set()
        raw_sub_langs = set()

        if media_streams:
            try:
                raw_audio_langs |= set(_get_detected_languages_from_streams(media_streams, "Audio"))
            except Exception:
                pass

            try:
                raw_sub_langs |= set(_get_detected_languages_from_streams(media_streams, "Subtitle"))
            except Exception:
                pass

        # 扁平字段兜底
        if isinstance(parsed, dict):
            for track in cls._safe_parse_list(parsed.get("audio_tracks", [])):
                if not isinstance(track, dict):
                    continue
                lang = track.get("language") or track.get("Language")
                if lang:
                    raw_audio_langs.add(cls._normalize_lang(lang))

            for sub in cls._safe_parse_list(parsed.get("subtitles", [])):
                if not isinstance(sub, dict):
                    continue
                lang = sub.get("language") or sub.get("Language")
                if lang:
                    raw_sub_langs.add(cls._normalize_lang(lang))

            for lang in cls._safe_parse_list(parsed.get("audio_langs", [])):
                if lang:
                    raw_audio_langs.add(cls._normalize_lang(lang))

            for lang in cls._safe_parse_list(parsed.get("sub_langs", [])):
                if lang:
                    raw_sub_langs.add(cls._normalize_lang(lang))

            for lang in cls._safe_parse_list(parsed.get("audio_languages_raw", [])):
                if lang:
                    raw_audio_langs.add(cls._normalize_lang(lang))

            for lang in cls._safe_parse_list(parsed.get("subtitle_languages_raw", [])):
                if lang:
                    raw_sub_langs.add(cls._normalize_lang(lang))

        norm["audio_langs"] = {cls._normalize_lang(a) for a in raw_audio_langs if a}
        norm["sub_langs"] = {cls._normalize_lang(s) for s in raw_sub_langs if s}

        # 5. 体积
        size_bytes = (
            media_source.get("Size")
            or (parsed.get("size_bytes") if isinstance(parsed, dict) else 0)
            or (parsed.get("_file_size") if isinstance(parsed, dict) else 0)
            or (parsed.get("Size") if isinstance(parsed, dict) else 0)
            or 0
        )
        try:
            size_bytes = int(size_bytes)
        except (ValueError, TypeError):
            size_bytes = 0

        norm["size_gb"] = size_bytes / (1024 ** 3)

        # 6. 原语言
        raw_original_lang = ""
        if isinstance(parsed, dict):
            raw_original_lang = (
                parsed.get("_original_lang")
                or parsed.get("original_lang")
                or parsed.get("lang_code")
                or ""
            )
        norm["original_lang"] = cls._normalize_lang(raw_original_lang)

        return norm

    @classmethod
    def _match_priority(cls, norm_info: dict, priority_rule: dict) -> tuple[bool, str]:
        # 1. 分辨率
        req_res = priority_rule.get("resolution", [])
        if req_res:
            req_res_lower = [str(r).lower().strip() for r in req_res]
            file_res = norm_info["resolution"]

            match = False
            for r in req_res_lower:
                if r == file_res or (r in ["4k", "2160p"] and file_res in ["4k", "2160p"]):
                    match = True
                    break

            if not match:
                return False, f"分辨率未命中 ({file_res})"

        # 2. 编码
        req_codec = priority_rule.get("codec", [])
        if req_codec:
            req_codec_lower = [str(c).lower().strip() for c in req_codec]
            file_codec = norm_info["codec"]

            match = False
            for c in req_codec_lower:
                if c in file_codec or file_codec in c:
                    match = True
                    break
                if c in ["hevc", "h265"] and ("hevc" in file_codec or "h265" in file_codec):
                    match = True
                if c in ["avc", "h264"] and ("avc" in file_codec or "h264" in file_codec):
                    match = True

            if not match:
                return False, f"编码未命中 ({file_codec})"

        # 3. 特效
        req_effect = priority_rule.get("effect", [])
        if req_effect:
            req_effect_lower = [str(e).lower().strip() for e in req_effect]
            file_effect = norm_info["effect"]
            if file_effect not in req_effect_lower:
                return False, f"特效未命中 ({file_effect})"

        # 4. 音轨
        original_lang = norm_info.get("original_lang") or ""

        req_audio = priority_rule.get("audio", [])
        if req_audio:
            normalized_req_audio = {cls._normalize_lang(a) for a in req_audio if a}
            effective_req_audio = {a for a in normalized_req_audio if a and a != original_lang}

            if effective_req_audio:
                if not norm_info["audio_langs"]:
                    return False, "未提取到音轨语言"
                if not any(a in norm_info["audio_langs"] for a in effective_req_audio):
                    return False, "缺少必须的音轨"

        # 5. 字幕
        req_sub = priority_rule.get("subtitle", [])
        if req_sub:
            normalized_req_sub = {cls._normalize_lang(s) for s in req_sub if s}
            effective_req_sub = {s for s in normalized_req_sub if s and s != original_lang}

            if effective_req_sub:
                if not norm_info["sub_langs"]:
                    return False, "未提取到字幕语言"
                if not any(s in norm_info["sub_langs"] for s in effective_req_sub):
                    return False, "缺少必须的字幕"

        # 6. 体积
        min_size = priority_rule.get("min_size_gb")
        max_size = priority_rule.get("max_size_gb")
        if min_size and norm_info["size_gb"] > 0 and norm_info["size_gb"] < float(min_size):
            return False, "体积过小"
        if max_size and norm_info["size_gb"] > 0 and norm_info["size_gb"] > float(max_size):
            return False, "体积过大"

        return True, "匹配成功"

    @classmethod
    def get_level(cls, norm_info: dict, priorities: list) -> tuple[int, str]:
        fail_reasons = []
        for i, p_rule in enumerate(priorities):
            is_match, reason = cls._match_priority(norm_info, p_rule)
            if is_match:
                return i + 1, f"命中优先级 {i + 1}"
            fail_reasons.append(f"优先级{i+1}[{reason}]")
        return 0, " | ".join(fail_reasons)

    @classmethod
    def _load_rule_group(cls, db_media_type: str, target_cid: str) -> Optional[dict]:
        rule_group = None
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT * FROM washing_priority_groups WHERE media_type = %s ORDER BY sort_order ASC",
                        (db_media_type,)
                    )
                    for row in cursor.fetchall():
                        cids = row.get("target_cids", [])
                        if isinstance(cids, str):
                            try:
                                cids = json.loads(cids)
                            except Exception:
                                cids = []

                        if not cids or str(target_cid) in cids:
                            rule_group = dict(row)
                            priorities = rule_group.get("priorities", [])
                            if isinstance(priorities, str):
                                try:
                                    priorities = json.loads(priorities)
                                except Exception:
                                    priorities = []
                            rule_group["priorities"] = priorities
                            break
        except Exception as e:
            logger.warning(f"  ➜ 获取洗版优先级规则失败: {e}")

        return rule_group

    @classmethod
    def _load_existing_raw_infos(
        cls,
        db_media_type: str,
        tmdb_id: str,
        season_num: Optional[int] = None,
        episode_num: Optional[int] = None,
    ) -> List[dict]:
        """
        从 media_metadata.file_sha1_json -> p115_mediainfo_cache.mediainfo_json
        读取库内现有版本的原始媒体信息。
        """
        rows = []

        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    if db_media_type == "Series" and season_num is not None and episode_num is not None:
                        where_sql = """
                            mm.parent_series_tmdb_id = %s
                            AND mm.season_number = %s
                            AND mm.episode_number = %s
                            AND mm.item_type = 'Episode'
                        """
                        params = (str(tmdb_id), season_num, episode_num)
                    elif db_media_type == "Series" and season_num is not None:
                        where_sql = """
                            mm.parent_series_tmdb_id = %s
                            AND mm.season_number = %s
                            AND mm.item_type = 'Season'
                        """
                        params = (str(tmdb_id), season_num)
                    elif db_media_type == "Series":
                        where_sql = """
                            mm.tmdb_id = %s
                            AND mm.item_type = 'Series'
                        """
                        params = (str(tmdb_id),)
                    else:
                        where_sql = """
                            mm.tmdb_id = %s
                            AND mm.item_type = 'Movie'
                        """
                        params = (str(tmdb_id),)

                    sql = f"""
                        SELECT DISTINCT pmc.sha1, pmc.mediainfo_json
                        FROM media_metadata mm
                        JOIN LATERAL jsonb_array_elements_text(
                            CASE
                                WHEN mm.file_sha1_json IS NOT NULL
                                     AND jsonb_typeof(mm.file_sha1_json) = 'array'
                                THEN mm.file_sha1_json
                                ELSE '[]'::jsonb
                            END
                        ) AS sha(sha1) ON TRUE
                        JOIN p115_mediainfo_cache pmc
                          ON pmc.sha1 = sha.sha1
                        WHERE {where_sql}
                    """

                    cursor.execute(sql, params)
                    rows = cursor.fetchall() or []

        except Exception as e:
            logger.warning(f"  ➜ 从 p115_mediainfo_cache 获取库内原始视频流失败: {e}")

        raw_infos = []
        for row in rows:
            raw = row.get("mediainfo_json")
            parsed = cls._safe_parse_jsonish(raw)
            if parsed:
                raw_infos.append(parsed)

        return raw_infos

    @classmethod
    def _get_raw_info_by_sha1(cls, sha1: str) -> Optional[dict]:
        if not sha1:
            return None
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT mediainfo_json FROM p115_mediainfo_cache WHERE sha1 = %s", (str(sha1),))
                    row = cursor.fetchone()
                    if row and row['mediainfo_json']:
                        return cls._safe_parse_jsonish(row['mediainfo_json'])
        except Exception as e:
            logger.warning(f"  ➜ 从 p115_mediainfo_cache 获取 SHA1 {sha1} 失败: {e}")
        return None
   
    @classmethod
    def decide_washing_action(
        cls,
        sha1: str,
        file_name: str,
        file_size: int,
        target_cid: str,
        media_type: str,
        tmdb_id: str,
        season_num: int = None,
        episode_num: int = None,
        original_lang: str = None,
    ) -> tuple[str, str]:
        """
        返回:
        ACCEPT  直接入库
        REPLACE 替换旧版
        SKIP    已有更好版本/同级版本
        REJECT  不符合优先级规则
        """
        # 1. ★ 直接通过 SHA1 获取最原始的视频流 JSON
        raw_info = cls._get_raw_info_by_sha1(sha1)
        
        # ★★★ 新增拦截逻辑：如果没有获取到媒体信息，直接视为不达标 ★★★
        if not raw_info:
            return "REJECT", "无法获取媒体流信息(可能是不支持的格式如ISO或文件损坏)"
        
        # 2. 转换为字典以便注入辅助信息
        if isinstance(raw_info, list) and len(raw_info) > 0:
            new_video_info = dict(raw_info[0])
        elif isinstance(raw_info, dict):
            new_video_info = dict(raw_info)
        else:
            new_video_info = {}

        # 3. 注入辅助信息供 _normalize_info 兜底使用
        new_video_info["filename"] = file_name
        new_video_info["_file_size"] = file_size
        new_video_info["_original_lang"] = original_lang

        # 4. 统一调用标准化解析
        norm_new = cls._normalize_info(new_video_info)

        db_media_type = "Movie" if media_type.lower() == "movie" else "Series"

        # 1. 规则组
        rule_group = cls._load_rule_group(db_media_type, target_cid)
        if not rule_group or not rule_group.get("priorities"):
            return "ACCEPT", "未配置优先级规则，默认放行"

        priorities = rule_group["priorities"]

        # 2. 新文件是否达标
        new_level, new_reason_detail = cls.get_level(norm_new, priorities)
        if new_level == 0:
            return "REJECT", f"未达标 ({new_reason_detail})"

        # 3. 取库内旧版原始流
        existing_raw_infos = cls._load_existing_raw_infos(
            db_media_type=db_media_type,
            tmdb_id=tmdb_id,
            season_num=season_num,
            episode_num=episode_num,
        )

        # 4. 没有旧版
        if not existing_raw_infos:
            return "ACCEPT", f"命中优先级 {new_level}，库内无旧版，直接入库"

        # 5. 找最优旧版
        best_old_level = 999
        for raw_old_info in existing_raw_infos:
            
            # 安全地将旧版 JSON 转换为字典，防止列表强转报错 ★★★
            if isinstance(raw_old_info, list) and len(raw_old_info) > 0:
                old_info = dict(raw_old_info[0])
            elif isinstance(raw_old_info, dict):
                old_info = dict(raw_old_info)
            else:
                old_info = {}

            old_info["_original_lang"] = original_lang
            norm_old = cls._normalize_info(old_info)

            old_level, _ = cls.get_level(norm_old, priorities)
            if old_level == 0:
                old_level = 999

            if old_level < best_old_level:
                best_old_level = old_level

        # 6. 比较结果
        if new_level < best_old_level:
            return "REPLACE", (
                f"新版(优先级{new_level}) 优于 "
                f"旧版(优先级{best_old_level if best_old_level != 999 else '不合格'})，执行洗版替换"
            )
        elif new_level == best_old_level:
            return "SKIP", f"新版(优先级{new_level}) 与旧版同级，跳过"
        else:
            return "SKIP", f"新版(优先级{new_level}) 劣于 旧版(优先级{best_old_level})，跳过"