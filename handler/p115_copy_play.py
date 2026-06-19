import json
import logging
import time
from datetime import datetime, timezone

import config_manager
import constants
from database import settings_db
from database.connection import get_db_connection
from handler.p115_service import P115CacheManager, P115Service

logger = logging.getLogger(__name__)

COPY_PLAY_CLONES_KEY = "p115_copy_play_clones"
COPY_PLAY_TTL_SECONDS = 12 * 60 * 60


def is_copy_play_enabled():
    cfg = config_manager.APP_CONFIG or {}
    return bool(cfg.get(constants.CONFIG_OPTION_115_COPY_PLAY_ENABLED))


def _copy_play_temp_cid():
    cfg = config_manager.APP_CONFIG or {}
    cid = str(cfg.get(constants.CONFIG_OPTION_115_COPY_PLAY_TEMP_CID) or "").strip()
    return cid if cid and cid != "0" else ""


def _safe_json(value, limit=800):
    try:
        text = json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        text = str(value)
    return text[:limit] + ("..." if len(text) > limit else "")


def _now_ts():
    return time.time()


def _load_clones():
    data = settings_db.get_setting(COPY_PLAY_CLONES_KEY) or []
    return data if isinstance(data, list) else []


def _save_clones(clones):
    settings_db.save_setting(COPY_PLAY_CLONES_KEY, clones[-300:])


def _extract_ids_from_copy_response(resp):
    found = []

    def walk(value):
        if isinstance(value, dict):
            for key in ("fid", "file_id", "id"):
                raw = value.get(key)
                if raw not in (None, "", [], {}):
                    found.append(str(raw))
            for val in value.values():
                walk(val)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(resp)
    return list(dict.fromkeys(found))


def _norm_size(value):
    try:
        return int(float(value or 0))
    except Exception:
        return 0


def _list_temp_candidates(client, temp_cid, source_row, file_name):
    expected_name = str(file_name or source_row.get("name") or "").strip()
    payload = {
        "cid": temp_cid,
        "limit": 100,
        "offset": 0,
        "show_dir": 0,
        "record_open_time": 0,
        "count_folders": 0,
    }
    if expected_name:
        payload["search_value"] = expected_name

    resp = client.fs_files(payload)
    items = resp.get("data") if isinstance(resp, dict) else []
    logger.info(
        "  ➜ [复制播放] 临时目录回查完成：候选=%s，文件名=%s",
        len(items or []),
        expected_name or "-",
    )
    logger.debug("  ➜ [复制播放] 临时目录回查摘要：%s", _safe_json(resp))
    return items if isinstance(items, list) else []


def _find_clone_in_temp_dir(client, temp_cid, source_row, file_name, exclude_ids=None):
    expected_name = str(file_name or source_row.get("name") or "").strip()
    expected_size = _norm_size(source_row.get("size"))
    exclude_ids = {str(x) for x in (exclude_ids or []) if x not in (None, "")}
    items = _list_temp_candidates(client, temp_cid, source_row, file_name)

    for item in items:
        item_name = str(item.get("name") or item.get("file_name") or item.get("fn") or "").strip()
        item_size = _norm_size(item.get("size") or item.get("fs"))
        fid = str(item.get("fid") or item.get("file_id") or item.get("id") or "").strip()
        if fid in exclude_ids:
            continue
        if expected_name and item_name != expected_name:
            continue
        if expected_size and item_size and item_size != expected_size:
            continue
        if fid:
            return item
    return {}


def _info_to_clone(info_resp, fallback_parent_id="", fallback_name=""):
    if not isinstance(info_resp, dict) or not info_resp.get("state"):
        return {}
    data = info_resp.get("data") or {}
    if not isinstance(data, dict):
        return {}
    fid = str(data.get("fid") or data.get("file_id") or data.get("id") or "").strip()
    pc = str(data.get("pick_code") or data.get("pc") or data.get("pickcode") or "").strip()
    name = str(data.get("name") or data.get("file_name") or data.get("fn") or fallback_name or "").strip()
    parent_id = str(data.get("parent_id") or data.get("pid") or fallback_parent_id or "").strip()
    if not fid or not pc:
        return {}
    return {
        "fid": fid,
        "pick_code": pc,
        "name": name,
        "parent_id": parent_id,
        "sha1": str(data.get("sha1") or data.get("sha") or "").strip().upper(),
        "size": _norm_size(data.get("size") or data.get("fs")),
    }


def _record_clone(record):
    clones = _load_clones()
    clones.append(record)
    _save_clones(clones)


def _lookup_emby_item_id_by_pick_code(pick_code):
    pc = str(pick_code or "").strip()
    if not pc:
        return ""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT emby_item_ids_json, file_pickcode_json
                    FROM media_metadata
                    WHERE file_pickcode_json ? %s
                    LIMIT 5
                    """,
                    (pc,),
                )
                for row in cursor.fetchall() or []:
                    emby_ids = row.get("emby_item_ids_json") or []
                    pcs = row.get("file_pickcode_json") or []
                    if not isinstance(emby_ids, list) or not isinstance(pcs, list):
                        continue
                    for idx, value in enumerate(pcs):
                        if str(value or "").strip() == pc and idx < len(emby_ids):
                            return str(emby_ids[idx] or "").strip()
    except Exception as e:
        logger.debug(f"  ➜ [复制播放] 按 PC 反查 Emby 项失败：pc={pc[:8]}..., err={e}")
    return ""


def _delete_clone(client, clone, reason):
    fid = str(clone.get("clone_fid") or "").strip()
    if not fid:
        return False
    resp = client.fs_delete([fid])
    ok = bool(isinstance(resp, dict) and resp.get("state"))
    if ok:
        logger.info(
            "  ➜ [复制播放] 已删除临时克隆文件：%s，原因=%s",
            clone.get("file_name") or fid,
            reason,
        )
    else:
        logger.warning(
            "  ➜ [复制播放] 删除临时克隆文件失败：%s，原因=%s，返回=%s",
            clone.get("file_name") or fid,
            reason,
            _safe_json(resp),
        )
    return ok


def cleanup_expired_clones(client=None):
    clones = _load_clones()
    if not clones:
        return 0
    client = client or P115Service.get_client()
    if not client:
        return 0
    now = _now_ts()
    kept = []
    removed = 0
    for clone in clones:
        created_at = float(clone.get("created_at") or 0)
        if created_at and now - created_at > COPY_PLAY_TTL_SECONDS:
            if _delete_clone(client, clone, "过期清理"):
                removed += 1
                continue
        kept.append(clone)
    if removed:
        _save_clones(kept)
    return removed


def prepare_copy_play_pick_code(source_pick_code, *, file_name="", item_id="", play_session_id="", user_id="", source=""):
    if not is_copy_play_enabled():
        return source_pick_code

    temp_cid = _copy_play_temp_cid()
    if not temp_cid:
        logger.warning("  ➜ [复制播放] 已开启但未配置临时目录，终止本次点播。")
        return ""

    client = P115Service.get_client()
    if not client:
        logger.warning("  ➜ [复制播放] 115 客户端未初始化，终止本次点播。")
        return ""

    try:
        cleanup_expired_clones(client)
    except Exception as e:
        logger.debug(f"  ➜ [复制播放] 过期临时文件清理失败：{e}")

    source_row = P115CacheManager.get_file_cache_by_pickcode(source_pick_code) or {}
    source_fid = str(source_row.get("id") or "").strip()
    if not source_fid:
        logger.warning("  ➜ [复制播放] 无法通过 PC 找到源文件 FID，终止本次点播：pc=%s", str(source_pick_code)[:8] + "...")
        return ""

    display_name = file_name or source_row.get("name") or source_pick_code
    if not item_id:
        item_id = _lookup_emby_item_id_by_pick_code(source_pick_code)
        if item_id:
            logger.info("  ➜ [复制播放] 已按源 PC 反查到 Emby 媒体项：%s", item_id)
    logger.info(
        "  ➜ [复制播放] 开始复制播放：%s，源FID=%s，源PC=%s，临时目录=%s",
        display_name,
        source_fid,
        str(source_pick_code)[:8] + "...",
        temp_cid,
    )

    before_ids = set()
    try:
        for item in _list_temp_candidates(client, temp_cid, source_row, display_name):
            fid = str(item.get("fid") or item.get("file_id") or item.get("id") or "").strip()
            if fid:
                before_ids.add(fid)
        logger.info("  ➜ [复制播放] 复制前临时目录已有同名候选：%s 个。", len(before_ids))
    except Exception as e:
        logger.debug(f"  ➜ [复制播放] 复制前临时目录快照失败：{e}")

    try:
        copy_resp = client.fs_copy([source_fid], temp_cid)
    except Exception as e:
        logger.warning("  ➜ [复制播放] 复制接口异常，终止本次点播：%s", e)
        return ""

    logger.info("  ➜ [复制播放] 复制接口返回：%s", _safe_json(copy_resp))
    if not isinstance(copy_resp, dict) or not copy_resp.get("state"):
        logger.warning("  ➜ [复制播放] 复制失败，终止本次点播。")
        return ""

    clone = {}
    for fid in _extract_ids_from_copy_response(copy_resp):
        if fid == source_fid:
            continue
        info_resp = client.fs_get_info(fid)
        logger.debug("  ➜ [复制播放] 复制返回 FID 详情：fid=%s，返回=%s", fid, _safe_json(info_resp))
        clone = _info_to_clone(info_resp, fallback_parent_id=temp_cid, fallback_name=display_name)
        if clone:
            break

    if not clone:
        for attempt in range(1, 9):
            item = _find_clone_in_temp_dir(client, temp_cid, source_row, display_name, exclude_ids=before_ids)
            clone_fid = str(item.get("fid") or item.get("file_id") or item.get("id") or "").strip()
            if clone_fid:
                info_resp = client.fs_get_info(clone_fid)
                logger.debug("  ➜ [复制播放] 回查克隆 FID 详情：fid=%s，返回=%s", clone_fid, _safe_json(info_resp))
                clone = _info_to_clone(info_resp, fallback_parent_id=temp_cid, fallback_name=display_name)
                if clone:
                    break
            logger.info("  ➜ [复制播放] 第 %s 次未查到可播放克隆体，等待后重试。", attempt)
            time.sleep(1)

    if not clone or not clone.get("pick_code"):
        logger.warning("  ➜ [复制播放] 已复制但未拿到克隆 PC，终止本次点播。")
        return ""

    record = {
        "source_pick_code": str(source_pick_code),
        "source_fid": source_fid,
        "clone_pick_code": clone["pick_code"],
        "clone_fid": clone["fid"],
        "temp_cid": temp_cid,
        "file_name": clone.get("name") or display_name,
        "item_id": str(item_id or ""),
        "play_session_id": str(play_session_id or ""),
        "user_id": str(user_id or ""),
        "source": source,
        "created_at": _now_ts(),
        "created_at_text": datetime.now(timezone.utc).isoformat(),
    }
    _record_clone(record)
    logger.info(
        "  ➜ [复制播放] 已准备临时克隆体：%s，克隆FID=%s，克隆PC=%s",
        record["file_name"],
        record["clone_fid"],
        record["clone_pick_code"][:8] + "...",
    )
    return record["clone_pick_code"]


def cleanup_for_playback_stop(data):
    playback_info = data.get("PlaybackInfo") or {}
    item = data.get("Item") or {}
    user = data.get("User") or {}
    play_session_id = str(playback_info.get("PlaySessionId") or data.get("PlaySessionId") or "").strip()
    item_id = str(item.get("Id") or "").strip()
    user_id = str(user.get("Id") or "").strip()
    source_pick_code = ""
    if item_id:
        try:
            from database import media_db
            source_pick_code = str(media_db.get_pickcode_by_emby_id(item_id) or "").strip()
        except Exception as e:
            logger.debug(f"  ➜ [复制播放] 停止播放按 Emby 项反查 PC 失败：item={item_id}, err={e}")

    clones = _load_clones()
    if not clones:
        return 0
    client = P115Service.get_client()
    if not client:
        logger.warning("  ➜ [复制播放] 停止播放后清理失败：115 客户端未初始化。")
        return 0
    try:
        cleanup_expired_clones(client)
    except Exception as e:
        logger.debug(f"  ➜ [复制播放] 停止播放时清理过期克隆失败：{e}")

    kept = []
    removed = 0
    for clone in clones:
        match_session = play_session_id and play_session_id == str(clone.get("play_session_id") or "")
        match_item = item_id and item_id == str(clone.get("item_id") or "")
        match_source_pc = source_pick_code and source_pick_code == str(clone.get("source_pick_code") or "")
        match_user_item = match_item and (not user_id or user_id == str(clone.get("user_id") or ""))
        if match_session or match_user_item or match_source_pc:
            if _delete_clone(client, clone, "停止播放"):
                removed += 1
                continue
        kept.append(clone)
    if removed:
        _save_clones(kept)
        logger.info("  ➜ [复制播放] 停止播放清理完成：删除 %s 个临时克隆文件。", removed)
    return removed
