import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
from typing import Any, Dict, List, Optional, Tuple

import config_manager
import constants
from ai_translator import AITranslator
from database.connection import get_db_connection
from handler import emby
from handler.resubscribe_service import WashingService

logger = logging.getLogger(__name__)

TEXT_SUBTITLE_EXTS = {".srt", ".ass", ".ssa", ".vtt"}
TEXT_SUBTITLE_CODECS = {"subrip", "srt", "ass", "ssa", "webvtt", "mov_text", "text"}
CHINESE_LANGS = {"chi", "zho", "zh", "chs", "cht", "cmn", "yue", "zh-cn", "zh-hans", "zh-tw", "zh-hant"}
TRANSLATABLE_LANGS = {"eng", "en", "jpn", "ja", "kor", "ko"}
VIDEO_EXTS = {".mkv", ".mp4", ".mov", ".avi", ".ts", ".m2ts", ".iso", ".wmv", ".rmvb"}
EMBEDDED_SUBTITLE_EXTRACT_TIMEOUT = 150


def process_ai_subtitle_translation_for_emby_items(
    emby_item_ids: List[str],
    item_name_for_log: str = "",
    emby_url: str = "",
    emby_api_key: str = "",
    emby_user_id: str = "",
) -> None:
    """Webhook 入库后的异步字幕翻译入口。"""
    if not config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_AI_TRANSLATE_SUBTITLE, False):
        return

    ids = [str(x).strip() for x in (emby_item_ids or []) if str(x or "").strip()]
    if not ids:
        return

    translator = None
    translated_count = 0
    for emby_item_id in ids:
        try:
            row = _load_media_row_by_emby_id(emby_item_id)
            if not row:
                logger.debug(f"  ➜ [AI字幕] 未找到媒体元数据，跳过：EmbyID={emby_item_id}")
                continue

            if _is_chinese_original_media(row):
                logger.debug(f"  ➜ [AI字幕] 华语媒体不触发字幕翻译，跳过：《{row.get('title') or item_name_for_log}》")
                continue

            if not _washing_requires_chinese_subtitle(row):
                logger.debug(f"  ➜ [AI字幕] 洗版规则未要求中文字幕，跳过：《{row.get('title') or item_name_for_log}》")
                continue

            assets = _media_assets_for_emby_id(row, emby_item_id)
            if not assets:
                logger.debug(f"  ➜ [AI字幕] 未找到媒体文件资产，跳过：《{row.get('title') or item_name_for_log}》")
                continue

            for asset in assets:
                if _has_chinese_subtitle(asset):
                    logger.debug(f"  ➜ [AI字幕] 已有中文字幕，跳过：《{row.get('title') or item_name_for_log}》")
                    continue

                if translator is None:
                    try:
                        translator = AITranslator(config_manager.APP_CONFIG)
                    except Exception as e:
                        logger.warning(f"  ➜ [AI字幕] AI 服务未就绪，跳过字幕翻译：{e}")
                        return

                result = _translate_one_asset(row, asset, translator)
                if not result:
                    continue

                translated_count += 1
                if emby_url and emby_api_key and emby_user_id:
                    emby.refresh_emby_item_metadata(
                        emby_item_id,
                        emby_url,
                        emby_api_key,
                        emby_user_id,
                        replace_all_metadata_param=False,
                        replace_all_images_param=False,
                        item_name_for_log=row.get("title") or item_name_for_log,
                    )
        except Exception as e:
            logger.warning(f"  ➜ [AI字幕] 处理字幕翻译任务失败：EmbyID={emby_item_id}，错误={e}", exc_info=True)

    if translated_count:
        logger.info(f"  ➜ [AI字幕] 本轮字幕翻译完成，生成 {translated_count} 个中文字幕文件。")


def process_ai_subtitle_translation_for_series(
    series_tmdb_id: str,
    item_name_for_log: str = "",
    emby_url: str = "",
    emby_api_key: str = "",
    emby_user_id: str = "",
) -> None:
    episode_ids = _load_episode_emby_ids_by_series_tmdb(series_tmdb_id)
    if not episode_ids:
        logger.debug(f"  ➜ [AI字幕] 未找到可处理分集，跳过：《{item_name_for_log or series_tmdb_id}》")
        return
    process_ai_subtitle_translation_for_emby_items(
        episode_ids,
        item_name_for_log=item_name_for_log,
        emby_url=emby_url,
        emby_api_key=emby_api_key,
        emby_user_id=emby_user_id,
    )


def _load_media_row_by_emby_id(emby_item_id: str) -> Optional[Dict[str, Any]]:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT m.tmdb_id, m.item_type, m.title, m.release_year, m.season_number, m.episode_number,
                       m.parent_series_tmdb_id, m.asset_details_json, m.emby_item_ids_json,
                       m.file_pickcode_json, m.file_sha1_json,
                       m.washing_level, m.washing_snapshot_json,
                       COALESCE(NULLIF(m.original_language, ''), NULLIF(parent.original_language, '')) AS original_language
                FROM media_metadata m
                LEFT JOIN media_metadata parent
                  ON m.item_type = 'Episode'
                 AND parent.item_type = 'Series'
                 AND parent.tmdb_id = m.parent_series_tmdb_id
                WHERE m.in_library IS TRUE
                  AND m.item_type IN ('Movie', 'Episode')
                  AND m.emby_item_ids_json @> %s::jsonb
                ORDER BY m.date_added DESC NULLS LAST
                LIMIT 1
                """,
                (json.dumps([str(emby_item_id)]),),
            )
            return cursor.fetchone()


def _load_episode_emby_ids_by_series_tmdb(series_tmdb_id: str) -> List[str]:
    if not series_tmdb_id:
        return []
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT jsonb_array_elements_text(
                    CASE WHEN jsonb_typeof(emby_item_ids_json) = 'array' THEN emby_item_ids_json ELSE '[]'::jsonb END
                ) AS emby_id
                FROM media_metadata
                WHERE parent_series_tmdb_id = %s
                  AND item_type = 'Episode'
                  AND in_library IS TRUE
                  AND emby_item_ids_json IS NOT NULL
                """,
                (str(series_tmdb_id),),
            )
            return [str(row.get("emby_id") or "").strip() for row in cursor.fetchall() if row.get("emby_id")]


def _safe_json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return default
    return value


def _is_chinese_original_media(row: Dict[str, Any]) -> bool:
    return _is_chinese_language(str((row or {}).get("original_language") or ""))


def _media_assets_for_emby_id(row: Dict[str, Any], emby_item_id: str) -> List[Dict[str, Any]]:
    assets = _safe_json(row.get("asset_details_json"), [])
    if not isinstance(assets, list):
        return []

    matched = [
        a for a in assets
        if isinstance(a, dict) and str(a.get("emby_item_id") or "") == str(emby_item_id)
    ]
    if matched:
        return matched
    return [a for a in assets if isinstance(a, dict)]


def _washing_requires_chinese_subtitle(row: Dict[str, Any]) -> bool:
    try:
        level = row.get("washing_level")
        level = float(level) if level is not None else None
    except Exception:
        level = None
    if not level or level <= 0:
        return False
    base_level = int(level)
    if base_level <= 0:
        return False

    snapshot = _safe_json(row.get("washing_snapshot_json"), {})
    if not isinstance(snapshot, dict):
        return False

    target_cid = str(snapshot.get("target_cid") or "").strip()
    if not target_cid:
        for version in snapshot.get("versions") or []:
            if isinstance(version, dict) and version.get("target_cid"):
                target_cid = str(version.get("target_cid")).strip()
                break
    if not target_cid:
        return False

    db_media_type = "Movie" if row.get("item_type") == "Movie" else "Series"
    priorities = WashingService._load_priorities(db_media_type, target_cid)
    normal_index = 0
    for rule in priorities:
        if not isinstance(rule, dict) or rule.get("is_exclude"):
            continue
        normal_index += 1
        if normal_index != base_level:
            continue
        required_subtitles = rule.get("subtitle") or []
        if isinstance(required_subtitles, str):
            required_subtitles = [required_subtitles]
        return any(WashingService._is_chinese_lang(x) for x in required_subtitles)
    return False


def _has_chinese_subtitle(asset: Dict[str, Any]) -> bool:
    langs = asset.get("subtitle_languages_raw") or []
    if isinstance(langs, str):
        langs = [langs]
    if any(str(x).strip().lower() in CHINESE_LANGS for x in langs):
        return True

    display = str(asset.get("subtitle_display") or "")
    if any(token in display for token in ("中文", "简体", "繁体", "简中", "繁中", "中字")):
        return True

    for sub in asset.get("subtitles") or []:
        if not isinstance(sub, dict):
            continue
        text = " ".join(str(sub.get(k) or "") for k in ("language", "display_title", "title")).lower()
        if any(lang in text for lang in CHINESE_LANGS) or any(token in text for token in ("中文", "简体", "繁体", "中字")):
            return True
    return False


def _translate_one_asset(row: Dict[str, Any], asset: Dict[str, Any], translator: AITranslator) -> bool:
    media_path = str(asset.get("path") or "").strip()
    if not media_path or media_path.startswith(("http://", "https://")):
        logger.debug(f"  ➜ [AI字幕] 媒体路径不可用，跳过：《{row.get('title') or ''}》")
        return False

    output_path = _output_subtitle_path(media_path)
    if os.path.exists(output_path):
        logger.debug(f"  ➜ [AI字幕] 中文字幕文件已存在，跳过：{output_path}")
        return False

    source_path, source_lang, temp_paths = _find_source_subtitle(media_path, asset, row)
    if not source_path:
        logger.debug(f"  ➜ [AI字幕] 未找到可翻译的文本字幕，跳过：《{row.get('title') or ''}》")
        return False

    try:
        srt_path = source_path if source_path.lower().endswith(".srt") else _convert_subtitle_to_srt(source_path, temp_paths)
        cues = _read_srt_cues(srt_path)
        if not cues:
            logger.debug(f"  ➜ [AI字幕] 字幕内容为空或格式不可识别，跳过：{source_path}")
            return False

        texts = [cue["text"] for cue in cues]
        translated = translator.translate_subtitle_lines(
            texts,
            title=row.get("title") or "",
            year=row.get("release_year"),
            source_language=source_lang,
        )
        if len(translated) != len(cues):
            logger.warning(f"  ➜ [AI字幕] AI 返回行数不一致，跳过写入：{os.path.basename(media_path)}")
            return False

        _write_srt(output_path, cues, translated)
        logger.info(f"  ➜ [AI字幕] 已生成中文字幕：{os.path.basename(output_path)}")
        return True
    finally:
        for path in temp_paths:
            try:
                if path and os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass


def _output_subtitle_path(media_path: str) -> str:
    base, _ext = os.path.splitext(media_path)
    return f"{base}.zh-Hans.srt"


def _find_source_subtitle(media_path: str, asset: Dict[str, Any], row: Dict[str, Any]) -> Tuple[Optional[str], str, List[str]]:
    source = _find_external_text_subtitle(media_path)
    if source:
        return source[0], source[1], []

    extracted = _extract_embedded_text_subtitle(media_path, asset, row)
    if extracted:
        return extracted
    return None, "", []


def _find_external_text_subtitle(media_path: str) -> Optional[Tuple[str, str]]:
    folder = os.path.dirname(media_path)
    base = os.path.splitext(os.path.basename(media_path))[0]
    if not folder or not os.path.isdir(folder):
        return None

    candidates = []
    for name in os.listdir(folder):
        full_path = os.path.join(folder, name)
        if not os.path.isfile(full_path):
            continue
        stem, ext = os.path.splitext(name)
        if ext.lower() not in TEXT_SUBTITLE_EXTS:
            continue
        if not stem.startswith(base):
            continue
        lang = _detect_language_from_text(name)
        if _is_chinese_language(lang) or _looks_chinese_text(name):
            continue
        candidates.append((full_path, lang))

    preferred = [x for x in candidates if x[1] in TRANSLATABLE_LANGS]
    if preferred:
        return sorted(preferred, key=lambda x: x[0].lower())[0]
    return sorted(candidates, key=lambda x: x[0].lower())[0] if candidates else None


def _extract_embedded_text_subtitle(media_path: str, asset: Dict[str, Any], row: Dict[str, Any]) -> Optional[Tuple[str, str, List[str]]]:
    input_url, user_agent, label = _resolve_subtitle_probe_input(media_path, asset, row)
    if not input_url:
        return None
    if not shutil.which("ffprobe") or not shutil.which("ffmpeg"):
        return None

    streams = _cached_text_subtitle_streams(row, asset)
    if streams:
        logger.debug(f"  ➜ [AI字幕] 已从媒体信息缓存命中 {len(streams)} 条文本字幕轨道，跳过在线 ffprobe。")
    else:
        started = time.monotonic()
        try:
            probe = subprocess.run(
                _ffmpeg_input_args("ffprobe", input_url, user_agent, ["-v", "error", "-print_format", "json", "-show_streams"]),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=60,
                check=False,
            )
            data = json.loads(probe.stdout or "{}")
            streams = data.get("streams") or []
            logger.debug(f"  ➜ [AI字幕] 在线 ffprobe 检查字幕完成，耗时 {time.monotonic() - started:.1f}s。")
        except Exception as e:
            logger.debug(f"  ➜ [AI字幕] ffprobe 检查字幕失败：{e}")
            return None

    candidates = []
    for stream in streams:
        if str(stream.get("codec_type") or "").lower() != "subtitle":
            continue
        codec = str(stream.get("codec_name") or "").lower()
        if codec not in TEXT_SUBTITLE_CODECS:
            continue
        tags = stream.get("tags") if isinstance(stream.get("tags"), dict) else {}
        lang = _normalize_lang(tags.get("language") or "")
        if _is_chinese_language(lang):
            continue
        candidates.append((stream, lang))

    if not candidates:
        return None

    candidates.sort(key=lambda x: (0 if x[1] in TRANSLATABLE_LANGS else 1, int(x[0].get("index") or 0)))
    logger.info(f"  ➜ [AI字幕] 找到 {len(candidates)} 条可尝试的源语言文本字幕轨道，成功抽取一条后立即翻译。")

    for stream, lang in candidates:
        tmp = tempfile.NamedTemporaryFile(prefix="etk-ai-sub-", suffix=".srt", delete=False)
        tmp.close()
        stream_index = stream.get("index")
        cmd = _ffmpeg_input_args(
            "ffmpeg",
            input_url,
            user_agent,
            ["-y", "-loglevel", "error"],
            ["-map", f"0:{stream_index}", tmp.name],
        )
        try:
            started = time.monotonic()
            logger.info(f"  ➜ [AI字幕] 正在抽取源语言字幕轨道：轨道={stream_index}，语言={lang or '未知'}，来源={label}")
            subprocess.run(cmd, capture_output=True, text=True, timeout=EMBEDDED_SUBTITLE_EXTRACT_TIMEOUT, check=True)
            logger.info(f"  ➜ [AI字幕] 已从{label}抽取源语言文本字幕，轨道={stream_index}，语言={lang or '未知'}，耗时 {time.monotonic() - started:.1f}s。")
            return tmp.name, lang, [tmp.name]
        except subprocess.TimeoutExpired:
            logger.warning(
                f"  ➜ [AI字幕] 抽取字幕轨道超时，继续尝试下一条：轨道={stream_index}，语言={lang or '未知'}，超时={EMBEDDED_SUBTITLE_EXTRACT_TIMEOUT}s"
            )
        except Exception as e:
            logger.debug(f"  ➜ [AI字幕] 抽取内封文本字幕失败，继续尝试下一条：轨道={stream_index}，语言={lang or '未知'}，错误={e}")
        try:
            os.remove(tmp.name)
        except Exception:
            pass
    return None


def _cached_text_subtitle_streams(row: Dict[str, Any], asset: Dict[str, Any]) -> List[Dict[str, Any]]:
    sha1 = _sha1_for_asset(row, asset)
    if not sha1:
        return []
    try:
        from handler.p115_service import P115CacheManager
        raw = P115CacheManager.get_raw_ffprobe_cache(sha1) or {}
    except Exception as e:
        logger.debug(f"  ➜ [AI字幕] 读取媒体信息缓存失败：sha1={sha1[:12]}...，错误={e}")
        return []
    if not isinstance(raw, dict):
        return []
    streams = raw.get("streams") or []
    if not isinstance(streams, list):
        return []
    return [
        s for s in streams
        if isinstance(s, dict)
        and str(s.get("codec_type") or "").lower() == "subtitle"
        and str(s.get("codec_name") or "").lower() in TEXT_SUBTITLE_CODECS
        and s.get("index") is not None
    ]


def _sha1_for_asset(row: Dict[str, Any], asset: Dict[str, Any]) -> str:
    sha1 = str(asset.get("sha1") or asset.get("file_sha1") or "").strip().upper()
    if sha1:
        return sha1

    emby_id = str(asset.get("emby_item_id") or "").strip()
    emby_ids = _safe_json(row.get("emby_item_ids_json"), [])
    sha1s = _safe_json(row.get("file_sha1_json"), [])
    if emby_id and isinstance(emby_ids, list) and isinstance(sha1s, list) and emby_id in emby_ids:
        idx = emby_ids.index(emby_id)
        if idx < len(sha1s) and sha1s[idx]:
            return str(sha1s[idx]).strip().upper()
    return ""


def _resolve_subtitle_probe_input(media_path: str, asset: Dict[str, Any], row: Dict[str, Any]) -> Tuple[str, str, str]:
    ext = os.path.splitext(media_path)[1].lower()
    if os.path.exists(media_path) and ext in VIDEO_EXTS:
        return media_path, "", "本地文件"

    pick_code = _pick_code_for_asset(row, asset)
    if not pick_code:
        logger.debug(f"  ➜ [AI字幕] 未找到 115 pick_code，无法在线抽取内封字幕：{os.path.basename(media_path)}")
        return "", "", ""

    try:
        from handler.p115_service import P115Service, get_115_api_priority, get_115_tokens, get_115_ua

        client = P115Service.get_client()
        if not client:
            return "", "", ""
        try:
            _, _, _, app_type = get_115_tokens()
        except Exception:
            app_type = "web"
        user_agent = get_115_ua(app_type or "web") or "Mozilla/5.0"
        priority = get_115_api_priority()
        methods = [("download_url", "Cookie"), ("openapi_downurl", "OpenAPI")] if priority == "cookie" else [("openapi_downurl", "OpenAPI"), ("download_url", "Cookie")]

        last_error = None
        for method_name, label in methods:
            method = getattr(client, method_name, None)
            if not callable(method):
                continue
            try:
                direct_url = method(pick_code, user_agent=user_agent)
                if direct_url:
                    return str(direct_url), user_agent, f"115直链({label})"
            except Exception as e:
                last_error = e
                logger.debug(f"  ➜ [AI字幕] {label} 获取 115 直链失败：{e}")
        if last_error:
            logger.debug(f"  ➜ [AI字幕] 获取 115 直链失败，无法在线抽取字幕：{last_error}")
    except Exception as e:
        logger.debug(f"  ➜ [AI字幕] 初始化 115 直链客户端失败：{e}")
    return "", "", ""


def _pick_code_for_asset(row: Dict[str, Any], asset: Dict[str, Any]) -> str:
    emby_id = str(asset.get("emby_item_id") or "").strip()
    emby_ids = _safe_json(row.get("emby_item_ids_json"), [])
    pickcodes = _safe_json(row.get("file_pickcode_json"), [])

    if emby_id and isinstance(emby_ids, list) and isinstance(pickcodes, list) and emby_id in emby_ids:
        idx = emby_ids.index(emby_id)
        if idx < len(pickcodes) and pickcodes[idx]:
            return str(pickcodes[idx]).strip()

    if emby_id:
        try:
            from database import media_db
            return str(media_db.get_pickcode_by_emby_id(emby_id) or "").strip()
        except Exception as e:
            logger.debug(f"  ➜ [AI字幕] 按 EmbyID 反查 pick_code 失败：{e}")
    return ""


def _ffmpeg_input_args(binary: str, input_url: str, user_agent: str = "", before_input: Optional[List[str]] = None, after_input: Optional[List[str]] = None) -> List[str]:
    args = [binary]
    args.extend(before_input or [])
    if user_agent:
        args.extend(["-user_agent", user_agent])
    args.extend(["-i", input_url])
    args.extend(after_input or [])
    return args


def _convert_subtitle_to_srt(source_path: str, temp_paths: List[str]) -> str:
    if not shutil.which("ffmpeg"):
        raise RuntimeError("容器内未安装 ffmpeg，无法转换字幕格式")

    tmp = tempfile.NamedTemporaryFile(prefix="etk-ai-sub-", suffix=".srt", delete=False)
    tmp.close()
    temp_paths.append(tmp.name)
    subprocess.run(
        ["ffmpeg", "-y", "-loglevel", "error", "-i", source_path, tmp.name],
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    )
    return tmp.name


def _read_text(path: str) -> str:
    for encoding in ("utf-8-sig", "utf-8", "gb18030"):
        try:
            with open(path, "r", encoding=encoding) as f:
                return f.read()
        except UnicodeDecodeError:
            continue
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()


def _read_srt_cues(path: str) -> List[Dict[str, str]]:
    content = _read_text(path).replace("\r\n", "\n").replace("\r", "\n").strip()
    if not content:
        return []

    cues = []
    for block in re.split(r"\n\s*\n", content):
        lines = [line.rstrip() for line in block.split("\n") if line.strip()]
        if not lines:
            continue
        time_index = 1 if lines[0].strip().isdigit() and len(lines) > 1 else 0
        if "-->" not in lines[time_index]:
            continue
        text = "\n".join(lines[time_index + 1:]).strip()
        if not text:
            continue
        cues.append({
            "index": lines[0].strip() if time_index == 1 else str(len(cues) + 1),
            "time": lines[time_index].strip(),
            "text": text,
        })
    return cues


def _write_srt(path: str, cues: List[Dict[str, str]], translated: List[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    blocks = []
    for idx, cue in enumerate(cues, start=1):
        text = str(translated[idx - 1] or cue["text"]).replace("\r\n", "\n").replace("\r", "\n").strip()
        blocks.append(f"{idx}\n{cue['time']}\n{text}")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n\n".join(blocks) + "\n")


def _detect_language_from_text(text: str) -> str:
    lowered = str(text or "").lower()
    patterns = {
        "eng": (r"(^|[.\-_ ])(?:eng|en|english)([.\-_ ]|$)",),
        "jpn": (r"(^|[.\-_ ])(?:jpn|jp|ja|japanese)([.\-_ ]|$)", r"日文|日语"),
        "kor": (r"(^|[.\-_ ])(?:kor|kr|ko|korean)([.\-_ ]|$)", r"韩文|韩语"),
        "chi": (r"(^|[.\-_ ])(?:chi|chs|cht|zho|zh|cn|sc|tc)([.\-_ ]|$)", r"中文|中字|简中|繁中|简体|繁体"),
    }
    for lang, regs in patterns.items():
        if any(re.search(reg, lowered, re.IGNORECASE) for reg in regs):
            return lang
    return ""


def _normalize_lang(lang: str) -> str:
    text = str(lang or "").strip().lower()
    mapping = {
        "en": "eng",
        "english": "eng",
        "ja": "jpn",
        "jp": "jpn",
        "japanese": "jpn",
        "ko": "kor",
        "kr": "kor",
        "korean": "kor",
        "zh": "chi",
        "cn": "chi",
        "zh-cn": "chi",
        "zh-hans": "chi",
        "zh-tw": "chi",
        "zh-hant": "chi",
        "zho": "chi",
        "chs": "chi",
        "cht": "chi",
        "cmn": "chi",
        "yue": "chi",
    }
    return mapping.get(text, text)


def _is_chinese_language(lang: str) -> bool:
    return _normalize_lang(lang) in CHINESE_LANGS


def _looks_chinese_text(text: str) -> bool:
    lowered = str(text or "").lower()
    return any(token in lowered for token in ("中文", "中字", "简中", "繁中", "简体", "繁体", ".chi.", ".chs.", ".cht.", ".zh."))
