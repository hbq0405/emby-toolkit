# handler/shared_intro_service.py
import json
import logging
import os
import re
from typing import Any, Dict, List

from database import settings_db
from database.connection import get_db_connection
from handler.p115_service import P115CacheManager
from handler.shared_center_client import SharedCenterClient, shared_center_enabled

logger = logging.getLogger(__name__)


INTRO_MARKER_TYPES = {"IntroStart", "IntroEnd"}


def shared_intro_enabled() -> bool:
    try:
        cfg = settings_db.get_shared_resource_config() or {}
        return bool(cfg.get("p115_shared_resource_enabled")) and bool(cfg.get("p115_shared_intro_enabled"))
    except Exception as e:
        logger.debug(f"  ➜ [共享片头] 读取配置失败，按未启用处理: {e}")
        return False


def _norm_sha1(value: Any) -> str:
    text = str(value or "").strip().upper()
    return text if re.fullmatch(r"[A-F0-9]{40}", text) else ""


def extract_intro_chapters(mediainfo: Any) -> List[Dict[str, Any]]:
    """从神医/Emby mediainfo JSON 中提取片头章节。

    神医把片头结束写成 MarkerType=IntroEnd，名称可能显示“片尾”，这里按 MarkerType
    判断，不按 Name 判断。
    """
    root = mediainfo
    if isinstance(root, list) and root:
        root = root[0]
    if not isinstance(root, dict):
        return []
    raw_chapters = root.get("Chapters")
    if not isinstance(raw_chapters, list):
        return []
    chapters = []
    for index, item in enumerate(raw_chapters):
        if not isinstance(item, dict):
            continue
        marker = str(item.get("MarkerType") or "").strip()
        if marker not in INTRO_MARKER_TYPES:
            continue
        if "#SA" in str(item.get("Name") or ""):
            return []
        try:
            ticks = int(float(item.get("StartPositionTicks")))
        except Exception:
            continue
        if ticks < 0:
            continue
        chapter = {
            "StartPositionTicks": ticks,
            "Name": str(item.get("Name") or ("片头" if marker == "IntroStart" else "片头结束")),
            "MarkerType": marker,
            "ChapterIndex": int(item.get("ChapterIndex") if item.get("ChapterIndex") is not None else index),
        }
        chapters.append(chapter)
    marker_set = {x.get("MarkerType") for x in chapters}
    if not {"IntroStart", "IntroEnd"}.issubset(marker_set):
        return []
    chapters.sort(key=lambda x: (int(x.get("StartPositionTicks") or 0), str(x.get("MarkerType") or "")))
    return chapters


def has_intro_chapters(mediainfo: Any) -> bool:
    return bool(extract_intro_chapters(mediainfo))


def merge_intro_chapters(mediainfo: Any, chapters: List[Dict[str, Any]]) -> Any:
    if not isinstance(chapters, list) or not chapters:
        return mediainfo
    if isinstance(mediainfo, list):
        if not mediainfo or not isinstance(mediainfo[0], dict):
            return mediainfo
        target = mediainfo[0]
    elif isinstance(mediainfo, dict):
        target = mediainfo
    else:
        return mediainfo
    existing = target.get("Chapters")
    if not isinstance(existing, list):
        existing = []
    kept = [x for x in existing if not (isinstance(x, dict) and str(x.get("MarkerType") or "") in INTRO_MARKER_TYPES)]
    target["Chapters"] = kept + [dict(x) for x in chapters if isinstance(x, dict)]
    return mediainfo


def _load_json_file(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json_file(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _candidate_local_paths_for_mediainfo(mediainfo_path: str) -> List[str]:
    path = os.path.abspath(mediainfo_path)
    base = path[:-len("-mediainfo.json")] if path.lower().endswith("-mediainfo.json") else os.path.splitext(path)[0]
    dirname = os.path.dirname(path)
    basename = os.path.basename(base)
    candidates = []
    for ext in (".mkv", ".mp4", ".ts", ".m2ts", ".avi", ".mov", ".wmv", ".flv", ".rmvb", ".webm", ".iso", ".strm"):
        candidates.append(os.path.join(dirname, basename + ext))
    return candidates


def _local_root() -> str:
    try:
        import config_manager
        import constants
        return str((config_manager.APP_CONFIG or {}).get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT) or "").strip()
    except Exception:
        return ""


def sha1_for_mediainfo_path(mediainfo_path: str) -> str:
    local_root = _local_root()
    for candidate in _candidate_local_paths_for_mediainfo(mediainfo_path):
        rel = ""
        if local_root:
            try:
                rel = os.path.relpath(candidate, local_root).replace("\\", "/")
            except Exception:
                rel = ""
        for value in (rel, candidate.replace("\\", "/")):
            if not value:
                continue
            try:
                row = P115CacheManager.get_file_cache_by_local_path(value)
                sha1 = _norm_sha1((row or {}).get("sha1"))
                if sha1:
                    return sha1
            except Exception:
                pass
    return ""


def _mediainfo_path_for_sha1(sha1: str) -> str:
    sha1 = _norm_sha1(sha1)
    if not sha1:
        return ""
    local_root = _local_root()
    if not local_root:
        return ""
    try:
        row = P115CacheManager.get_file_cache_by_sha1(sha1) or {}
    except Exception:
        row = {}
    local_path = str(row.get("local_path") or "").strip().replace("\\", "/").lstrip("/")
    if not local_path:
        return ""
    full_path = os.path.join(local_root, local_path)
    base, ext = os.path.splitext(full_path)
    candidates = []
    if ext:
        candidates.append(base + "-mediainfo.json")
    candidates.append(full_path + "-mediainfo.json")
    for path in candidates:
        if path and os.path.exists(path):
            return path
    return ""


def upload_intro_for_mediainfo_path(mediainfo_path: str, *, reason: str = "") -> Dict[str, Any]:
    if not shared_intro_enabled():
        return {"ok": False, "skipped": True, "reason": "shared_intro_disabled"}
    if not shared_center_enabled():
        return {"ok": False, "skipped": True, "reason": "shared_center_disabled"}
    try:
        data = _load_json_file(mediainfo_path)
    except Exception as e:
        return {"ok": False, "message": f"读取 mediainfo 失败: {e}"}
    chapters = extract_intro_chapters(data)
    if not chapters:
        return {"ok": False, "skipped": True, "reason": "no_intro_chapters"}
    sha1 = sha1_for_mediainfo_path(mediainfo_path)
    if not sha1:
        return {"ok": False, "skipped": True, "reason": "sha1_not_found"}
    resp = SharedCenterClient().upload_intro_chapters(
        sha1,
        chapters,
        file_name=os.path.basename(mediainfo_path).replace("-mediainfo.json", ""),
        reason=reason,
    )
    return {"ok": bool(resp.get("ok", True)), "sha1": sha1, "chapters": chapters, "center": resp}


def upload_intro_for_cache_sha1(sha1: str, mediainfo: Any, *, file_name: str = "", reason: str = "") -> Dict[str, Any]:
    if not shared_intro_enabled():
        return {"ok": False, "skipped": True, "reason": "shared_intro_disabled"}
    chapters = extract_intro_chapters(mediainfo)
    sha1 = _norm_sha1(sha1)
    if not sha1 or not chapters or not shared_center_enabled():
        return {"ok": False, "skipped": True}
    resp = SharedCenterClient().upload_intro_chapters(sha1, chapters, file_name=file_name, reason=reason)
    return {"ok": bool(resp.get("ok", True)), "sha1": sha1, "center": resp}


def fetch_intro_map(sha1_list: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    if not shared_intro_enabled():
        return {}
    sha1s = []
    for value in sha1_list or []:
        sha1 = _norm_sha1(value)
        if sha1 and sha1 not in sha1s:
            sha1s.append(sha1)
    if not sha1s:
        return {}
    try:
        resp = SharedCenterClient().intro_chapters_batch(sha1s)
    except Exception as e:
        logger.debug(f"  ➜ [共享片头] 拉取中心片头失败: {e}")
        return {}
    out = {}
    for item in resp.get("items") or []:
        sha1 = _norm_sha1((item or {}).get("sha1"))
        chapters = (item or {}).get("chapters")
        if sha1 and isinstance(chapters, list) and chapters:
            out[sha1] = chapters
    return out


def merge_intro_into_local_cache(sha1: str, chapters: List[Dict[str, Any]]) -> bool:
    if not shared_intro_enabled():
        return False
    sha1 = _norm_sha1(sha1)
    if not sha1 or not chapters:
        return False
    try:
        text = P115CacheManager.get_mediainfo_cache_text(sha1)
        if not text:
            return False
        data = json.loads(text)
        if extract_intro_chapters(data):
            return False
        merge_intro_chapters(data, chapters)
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE p115_mediainfo_cache SET mediainfo_json=%s WHERE sha1=%s",
                    (json.dumps(data, ensure_ascii=False), sha1),
                )
            conn.commit()
        return True
    except Exception as e:
        logger.debug(f"  ➜ [共享片头] 合并片头到本地缓存失败: {sha1[:12]} -> {e}")
        return False


def merge_intro_into_mediainfo_file(mediainfo_path: str, sha1: str = "") -> Dict[str, Any]:
    if not shared_intro_enabled():
        return {"ok": False, "skipped": True, "reason": "shared_intro_disabled"}
    try:
        data = _load_json_file(mediainfo_path)
    except Exception as e:
        return {"ok": False, "message": str(e)}
    if extract_intro_chapters(data):
        return {"ok": True, "skipped": True, "reason": "already_has_intro"}
    sha1 = _norm_sha1(sha1) or sha1_for_mediainfo_path(mediainfo_path)
    intro_map = fetch_intro_map([sha1]) if sha1 else {}
    chapters = intro_map.get(sha1)
    if not chapters:
        return {"ok": False, "skipped": True, "reason": "no_center_intro"}
    merge_intro_chapters(data, chapters)
    _write_json_file(mediainfo_path, data)
    merge_intro_into_local_cache(sha1, chapters)
    return {"ok": True, "sha1": sha1, "chapters": chapters}


def scan_and_upload_local_intro(limit: int = 500) -> Dict[str, Any]:
    if not shared_intro_enabled():
        return {"ok": False, "skipped": True, "reason": "shared_intro_disabled"}
    scanned = uploaded = failed = skipped = 0
    errors = []
    try:
        resp = SharedCenterClient().intro_chapters_missing(limit=limit)
    except Exception as e:
        return {"ok": False, "scanned": 0, "uploaded": 0, "skipped": 0, "failed": 1, "errors": [{"message": str(e)}]}

    items = [x for x in (resp.get("items") or []) if isinstance(x, dict)]
    for item in items:
        scanned += 1
        sha1 = _norm_sha1(item.get("sha1"))
        if not sha1:
            skipped += 1
            continue
        path = _mediainfo_path_for_sha1(sha1)
        if not path:
            skipped += 1
            continue
        try:
            data = _load_json_file(path)
            chapters = extract_intro_chapters(data)
            if not chapters:
                skipped += 1
                continue
            resp = SharedCenterClient().upload_intro_chapters(
                sha1,
                chapters,
                file_name=str(item.get("file_name") or "").strip() or os.path.basename(path).replace("-mediainfo.json", ""),
                reason="maintenance_backfill",
            )
            if resp.get("duplicate"):
                skipped += 1
            elif resp.get("ok", True):
                uploaded += 1
            else:
                failed += 1
                errors.append({"file": path, "message": resp.get("message") or resp.get("detail") or "upload_failed"})
        except Exception as e:
            failed += 1
            errors.append({"file": path, "message": str(e)})
    return {"ok": failed == 0, "scanned": scanned, "uploaded": uploaded, "skipped": skipped, "failed": failed, "errors": errors[:20]}
