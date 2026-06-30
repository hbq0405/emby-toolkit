import json
import logging
import re
import threading
import time
from datetime import datetime, timezone

import config_manager
import constants
from database import settings_db
from database.connection import get_db_connection
from handler.p115_temp_dir import ensure_temp_dir
from handler.p115_service import P115CacheManager, P115Service

logger = logging.getLogger(__name__)

COPY_PLAY_CLONES_KEY = "p115_copy_play_clones"
COPY_PLAY_ACTIVE_SOURCES_KEY = "p115_copy_play_active_sources"
COPY_PLAY_TTL_SECONDS = 12 * 60 * 60
COPY_PLAY_RECYCLE_DELAY_SECONDS = 10 * 60
MEDIA_EXTENSIONS = ("mkv", "mp4", "avi", "mov", "ts", "m2ts", "wmv", "flv", "webm", "iso")
_PREPARE_LOCKS = {}
_PREPARE_LOCKS_GUARD = threading.Lock()
_ACTIVE_SOURCES_LOCK = threading.Lock()


def is_copy_play_enabled():
    cfg = config_manager.APP_CONFIG or {}
    return bool(cfg.get(constants.CONFIG_OPTION_115_COPY_PLAY_ENABLED))


def _item_id(item):
    if not isinstance(item, dict):
        return ""
    return str(item.get("fid") or item.get("file_id") or item.get("id") or item.get("cid") or "").strip()


def _item_name(item):
    if not isinstance(item, dict):
        return ""
    return str(item.get("name") or item.get("file_name") or item.get("fn") or item.get("n") or "").strip()


def _item_pick_code(item):
    if not isinstance(item, dict):
        return ""
    return str(item.get("pick_code") or item.get("pc") or item.get("pickcode") or "").strip()


def _item_is_dir(item):
    if not isinstance(item, dict):
        return False
    item_type = item.get("fc")
    if item_type is None:
        item_type = item.get("file_category")
    if item_type is not None:
        return str(item_type) == "0"
    icon = str(item.get("ico") or item.get("icon") or "").lower()
    return icon in ("folder", "dir", "directory") or str(item.get("is_dir")).lower() in ("1", "true")


def _list_child_folders(client, parent_cid):
    resp = client.fs_files({"cid": str(parent_cid), "limit": 1000, "offset": 0})
    if not isinstance(resp, dict) or not resp.get("state"):
        raise RuntimeError((resp or {}).get("error_msg") or (resp or {}).get("message") or "读取复制播放目录失败")
    folders = []
    for item in resp.get("data") or []:
        if _item_is_dir(item):
            cid = _item_id(item)
            name = _item_name(item)
            if cid and name:
                folders.append({"cid": cid, "name": name})
    return folders


def _ensure_copy_play_temp_dir(client):
    return ensure_temp_dir(client)


def _safe_json(value, limit=800):
    try:
        text = json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        text = str(value)
    return text[:limit] + ("..." if len(text) > limit else "")


def _item_brief(item):
    if not isinstance(item, dict):
        return {"type": type(item).__name__}
    keys = (
        "fid", "file_id", "id", "cid", "pid", "parent_id",
        "name", "file_name", "fn", "n",
        "pick_code", "pc", "pickcode",
        "sha1", "sha", "size", "fs", "file_size",
        "fc", "file_category", "ico", "is_dir",
        "status", "errno", "code", "message", "error_msg",
    )
    return {key: item.get(key) for key in keys if key in item and item.get(key) not in (None, "", [], {})}


def _json_text(value):
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        return str(value)


def _log_copy_response(resp, backend=""):
    backend_label = backend or "auto"
    text = _json_text(resp)
    if isinstance(resp, dict):
        data = resp.get("data")
        top_keys = ",".join(map(str, resp.keys()))
        data_type = type(data).__name__ if data is not None else "None"
        if isinstance(data, dict):
            data_hint = "data_keys=" + ",".join(map(str, data.keys()))
        elif isinstance(data, list):
            data_hint = f"data_len={len(data)}"
        else:
            data_hint = "data_empty"
        logger.debug(
            "  ➜ [复制播放] %s 复制接口返回结构：长度=%s，顶层字段=%s，data类型=%s，%s",
            backend_label,
            len(text),
            top_keys or "-",
            data_type,
            data_hint,
        )
    else:
        logger.debug("  ➜ [复制播放] %s 复制接口返回结构：类型=%s，长度=%s", backend_label, type(resp).__name__, len(text))

    chunk_size = 1800
    total = max(1, (len(text) + chunk_size - 1) // chunk_size)
    for index in range(total):
        chunk = text[index * chunk_size:(index + 1) * chunk_size]
        logger.debug("  ➜ [复制播放] %s 复制接口完整返回[%s/%s]：%s", backend_label, index + 1, total, chunk)


def _now_ts():
    return time.time()


def _load_clones():
    data = settings_db.get_setting(COPY_PLAY_CLONES_KEY) or []
    return data if isinstance(data, list) else []


def _save_clones(clones):
    settings_db.save_setting(COPY_PLAY_CLONES_KEY, clones[-300:])


def _load_active_sources():
    data = settings_db.get_setting(COPY_PLAY_ACTIVE_SOURCES_KEY) or []
    return data if isinstance(data, list) else []


def _save_active_sources(records):
    settings_db.save_setting(COPY_PLAY_ACTIVE_SOURCES_KEY, records[-500:])


def _active_source_key(record):
    return "|".join([
        str(record.get("source_pick_code") or "").strip(),
        str(record.get("item_id") or "").strip(),
        str(record.get("play_session_id") or "").strip(),
        str(record.get("user_id") or "").strip(),
        str(record.get("client_key") or "").strip(),
    ])


def _active_sources_without_expired(records=None):
    now = _now_ts()
    records = _load_active_sources() if records is None else records
    return [
        record for record in records
        if now - float(record.get("updated_at") or record.get("created_at") or 0) <= COPY_PLAY_TTL_SECONDS
    ]


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


def _extract_clones_from_copy_response(resp, source_fid="", temp_cid="", fallback_name=""):
    clones = []
    source_fid = str(source_fid or "").strip()

    def walk(value):
        if isinstance(value, dict):
            clone = _item_to_clone(value, fallback_parent_id=temp_cid, fallback_name=fallback_name)
            if clone and clone.get("fid") != source_fid:
                clones.append(clone)
            for val in value.values():
                walk(val)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(resp)
    return clones


def _norm_size(value):
    try:
        return int(float(value or 0))
    except Exception:
        return 0


def _duplicate_name_index(actual_name, expected_name):
    actual = str(actual_name or "").strip()
    expected = str(expected_name or "").strip()
    if not expected:
        return 0
    if actual == expected:
        return 0
    ext_pattern = "|".join(re.escape(ext) for ext in MEDIA_EXTENSIONS)
    expected_ext_match = re.fullmatch(r"(.+)\.(" + ext_pattern + r")", expected, flags=re.IGNORECASE)
    if not expected_ext_match:
        match = re.fullmatch(re.escape(expected) + r"\((\d+)\)", actual)
        if match:
            return int(match.group(1))
        match = re.fullmatch(re.escape(expected) + r"(?:\((\d+)\))?\.(" + ext_pattern + r")", actual, flags=re.IGNORECASE)
        if match:
            return int(match.group(1) or 0)
        match = re.fullmatch(re.escape(expected) + r"\((\d+)\)\.[^.]+", actual)
        return int(match.group(1)) if match else -1
    stem, ext = expected_ext_match.group(1), expected_ext_match.group(2)
    match = re.fullmatch(re.escape(stem) + r"\((\d+)\)\." + re.escape(ext), actual)
    return int(match.group(1)) if match else -1


def _item_to_clone(item, fallback_parent_id="", fallback_name=""):
    if not isinstance(item, dict):
        return {}
    fid = _item_id(item)
    pc = _item_pick_code(item)
    name = _item_name(item) or str(fallback_name or "").strip()
    parent_id = str(item.get("parent_id") or item.get("pid") or item.get("cid") or fallback_parent_id or "").strip()
    if not fid or not pc:
        return {}
    return {
        "fid": fid,
        "pick_code": pc,
        "name": name,
        "parent_id": parent_id,
        "sha1": str(item.get("sha1") or item.get("sha") or "").strip().upper(),
        "size": _norm_size(item.get("size") or item.get("fs") or item.get("s")),
    }


def _source_sha1(source_row):
    return str(source_row.get("sha1") or source_row.get("sha") or "").strip().upper()


def _item_sha1(item):
    if not isinstance(item, dict):
        return ""
    return str(item.get("sha1") or item.get("sha") or item.get("file_sha1") or "").strip().upper()




def _probe_clone_direct_url(client, pick_code):
    pc = str(pick_code or "").strip()
    if not pc:
        return {"ok": False, "reason": "missing_pick_code"}
    user_agent = "Mozilla/5.0"
    attempts = []
    for method_name in ("download_url", "openapi_downurl"):
        method = getattr(client, method_name, None)
        if not callable(method):
            continue
        try:
            url = method(pc, user_agent=user_agent)
            ok = bool(url)
            attempts.append({"method": method_name, "ok": ok})
            if ok:
                return {"ok": True, "method": method_name}
        except Exception as e:
            attempts.append({"method": method_name, "ok": False, "error": str(e)[:200]})
    return {"ok": False, "attempts": attempts}


def _diagnose_copy_clone_failure(client, temp_cid, source_row, file_name, *, backend="", copy_resp=None, response_fids=None):
    expected_name = str(source_row.get("name") or file_name or "").strip()
    expected_size = _norm_size(source_row.get("size"))
    response_fids = response_fids or []
    try:
        items = _list_temp_candidates(client, temp_cid, source_row, file_name)
    except Exception as e:
        logger.warning("  ➜ [复制播放诊断] 临时目录回查异常：%s", e)
        return

    matched = []
    for item in items:
        if _item_is_dir(item):
            continue
        item_name = _item_name(item)
        item_size = _norm_size(item.get("size") or item.get("fs"))
        duplicate_index = _duplicate_name_index(item_name, expected_name)
        if duplicate_index < 0:
            continue
        if expected_size and item_size and item_size != expected_size:
            continue
        matched.append((duplicate_index, item))
    matched.sort(key=lambda pair: pair[0], reverse=True)

    logger.warning(
        "  ➜ [复制播放诊断] %s 复制成功但未拿到可播放克隆 PC：源FID=%s，源PC=%s，SHA1=%s，size=%s，目录候选=%s，同名匹配=%s，复制返回FID=%s",
        backend or "-",
        source_row.get("id") or "-",
        (str(source_row.get("pick_code") or "")[:8] + "...") if source_row.get("pick_code") else "-",
        (str(source_row.get("sha1") or "")[:12] + "...") if source_row.get("sha1") else "-",
        source_row.get("size") or "-",
        len(items),
        len(matched),
        response_fids[:8],
    )
    logger.debug("  ➜ [复制播放诊断] 复制接口摘要：%s", _safe_json(copy_resp, limit=1200))
    if not matched:
        candidate_briefs = []
        for item in items[:10]:
            if _item_is_dir(item):
                continue
            candidate_briefs.append(_item_brief(item))
        logger.warning(
            "  ➜ [复制播放诊断] 未匹配到同名克隆：期望文件=%s，期望大小=%s，目录文件=%s",
            expected_name or "-",
            expected_size or "-",
            _safe_json(candidate_briefs, limit=1500),
        )

    for index, item in matched[:5]:
        fid = _item_id(item)
        pc = _item_pick_code(item)
        detail = {}
        detail_resp = None
        if fid and hasattr(client, "fs_get_info"):
            try:
                detail_resp = client.fs_get_info(fid)
                detail = detail_resp.get("data") if isinstance(detail_resp, dict) else {}
            except Exception as e:
                detail_resp = {"state": False, "error_msg": str(e)}
        detail_pc = _item_pick_code(detail)
        probe_pc = detail_pc or pc
        probe = _probe_clone_direct_url(client, probe_pc) if probe_pc else {"ok": False, "reason": "detail_missing_pick_code"}
        logger.warning(
            "  ➜ [复制播放诊断] 匹配克隆：序号=%s，列表=%s，详情=%s，直链探测=%s",
            index,
            _safe_json(_item_brief(item), limit=500),
            _safe_json(_item_brief(detail) if detail else detail_resp, limit=700),
            _safe_json(probe, limit=500),
        )


def _list_temp_candidates(client, temp_cid, source_row, file_name):
    expected_name = str(source_row.get("name") or file_name or "").strip()
    payload = {
        "cid": temp_cid,
        "limit": 1000,
        "offset": 0,
        "show_dir": 0,
        "record_open_time": 0,
        "count_folders": 0,
    }

    resp = client.fs_files(payload)
    items = resp.get("data") if isinstance(resp, dict) else []
    logger.debug(
        "  ➜ [复制播放] 临时目录回查完成：候选=%s，文件名=%s",
        len(items or []),
        expected_name or "-",
    )
    if logger.isEnabledFor(logging.DEBUG):
        names = []
        for item in (items or [])[:8]:
            names.append(_item_name(item))
        logger.debug("  ➜ [复制播放] 临时目录回查摘要：候选文件=%s", names)
    return items if isinstance(items, list) else []


def _find_clone_in_temp_dir(client, temp_cid, source_row, file_name):
    expected_name = str(source_row.get("name") or file_name or "").strip()
    expected_size = _norm_size(source_row.get("size"))
    expected_sha1 = _source_sha1(source_row)
    items = _list_temp_candidates(client, temp_cid, source_row, file_name)
    best_item = {}
    best_index = -1

    for item in items:
        if _item_is_dir(item):
            continue
        item_name = _item_name(item)
        item_size = _norm_size(item.get("size") or item.get("fs"))
        item_sha1 = _item_sha1(item)
        fid = _item_id(item)
        duplicate_index = _duplicate_name_index(item_name, expected_name)
        if duplicate_index < 0:
            continue
        if expected_sha1 and item_sha1 and item_sha1 == expected_sha1:
            if fid and duplicate_index >= best_index:
                best_item = item
                best_index = duplicate_index
            continue
        if expected_size and item_size and item_size != expected_size:
            continue
        if fid and duplicate_index >= best_index:
            best_item = item
            best_index = duplicate_index
    if not best_item and expected_size:
        same_size_items = []
        for item in items:
            if _item_is_dir(item):
                continue
            item_size = _norm_size(item.get("size") or item.get("fs"))
            fid = _item_id(item)
            if fid and item_size == expected_size:
                same_size_items.append(item)
        if len(same_size_items) == 1:
            best_item = same_size_items[0]
            best_index = 0
            logger.warning(
                "  ➜ [复制播放] 临时目录按大小兜底命中克隆体：期望文件=%s，实际文件=%s，size=%s",
                expected_name or "-",
                _item_name(best_item) or "-",
                expected_size,
            )
    if best_item:
        logger.debug(
            "  ➜ [复制播放] 临时目录命中克隆体：文件=%s，重复序号=%s",
            _item_name(best_item) or "-",
            best_index,
        )
    return best_item


def _copy_backend_order():
    primary = str(
        (config_manager.APP_CONFIG or {}).get(constants.CONFIG_OPTION_115_API_PRIORITY, "openapi") or "openapi"
    ).strip().lower()
    if primary == "cookie":
        return ["cookie", "openapi"]
    return ["openapi", "cookie"]


def _copy_file_with_backend(client, source_fid, temp_cid, backend):
    if hasattr(client, "fs_copy_backend"):
        return client.fs_copy_backend([source_fid], temp_cid, backend=backend)
    return client.fs_copy([source_fid], temp_cid)


def _record_clone(record):
    clones = _load_clones()
    clones.append(record)
    _save_clones(clones)


def _find_reusable_clone(source_pick_code, source_fid, temp_cid, *, item_id="", play_session_id="", user_id="", client_key=""):
    play_session_id = str(play_session_id or "").strip()
    if not play_session_id:
        return {}
    now = _now_ts()
    for clone in reversed(_load_clones()):
        if str(clone.get("source_pick_code") or "") != str(source_pick_code or ""):
            continue
        if str(clone.get("play_session_id") or "").strip() != play_session_id:
            continue
        if source_fid and str(clone.get("source_fid") or "") != str(source_fid):
            continue
        if temp_cid and str(clone.get("temp_cid") or "") != str(temp_cid):
            continue
        if not clone.get("clone_pick_code") or not clone.get("clone_fid"):
            continue
        created_at = float(clone.get("created_at") or 0)
        if created_at and now - created_at > COPY_PLAY_TTL_SECONDS:
            continue
        if clone.get("recycled_after_direct_url"):
            recycle_after_ts = float(clone.get("recycle_after_ts") or 0)
            if not recycle_after_ts or now >= recycle_after_ts:
                continue
        return clone
    return {}


def is_copy_play_missing_error(error):
    text = str(error or "").lower()
    return bool(
        "50015" in text
        or "不存在" in text
        or "已删除" in text
        or "not exist" in text
        or "deleted" in text
    )


def discard_copy_play_clone(clone_pick_code):
    pc = str(clone_pick_code or "").strip()
    if not pc:
        return False
    clones = _load_clones()
    kept = [clone for clone in clones if str(clone.get("clone_pick_code") or "").strip() != pc]
    if len(kept) == len(clones):
        return False
    _save_clones(kept)
    logger.debug("  ➜ [复制播放] 克隆体已失效，丢弃旧记录：%s", pc[:8] + "...")
    return True


def _client_key_from_webhook(data):
    session = data.get("Session") or {}
    device_id = str(session.get("DeviceId") or data.get("DeviceId") or "").strip()
    remote_addr = str(data.get("_etk_webhook_remote_addr") or "").strip()
    client_name = str(session.get("Client") or "").strip()
    device_name = str(session.get("DeviceName") or "").strip()
    return "|".join([device_id or remote_addr, client_name, device_name])


def _client_key_device_id(client_key):
    return str(client_key or "").split("|", 1)[0].strip()


def _is_same_viewer(record, *, play_session_id="", user_id="", client_key=""):
    record_session = str(record.get("play_session_id") or "").strip()
    record_user_id = str(record.get("user_id") or "").strip()

    if play_session_id and record_session == str(play_session_id):
        return True
    if user_id and record_user_id and record_user_id == str(user_id):
        return True
    return False


def _matches_playback_stop(record, *, play_session_id="", item_id="", user_id="", client_key=""):
    match_session = play_session_id and play_session_id == str(record.get("play_session_id") or "")
    match_item = item_id and item_id == str(record.get("item_id") or "")
    record_user_id = str(record.get("user_id") or "")
    match_user = user_id and record_user_id and user_id == record_user_id
    return bool(match_session or (match_item and match_user))


def should_use_copy_play_for_source(source_pick_code, *, file_name="", item_id="", play_session_id="", user_id="", source="", client_key="", client_name=""):
    pc = str(source_pick_code or "").strip()
    if not pc or not is_copy_play_enabled():
        return False

    with _ACTIVE_SOURCES_LOCK:
        records = _load_active_sources()
        active_records = _active_sources_without_expired(records)
        if len(active_records) != len(records):
            _save_active_sources(active_records)

        for record in active_records:
            if str(record.get("source_pick_code") or "").strip() != pc:
                continue
            if _is_same_viewer(record, play_session_id=play_session_id, user_id=user_id, client_key=client_key):
                logger.debug(
                    "  ➜ [复制播放] 同源播放来自同一用户或播放会话，继续直链：%s",
                    file_name or pc[:8] + "...",
                )
                continue
            logger.debug(
                "  ➜ [复制播放] 检测到同源并发播放，启用复制播放：%s",
                file_name or pc[:8] + "...",
            )
            return True
    return False


def record_source_play(source_pick_code, *, file_name="", item_id="", play_session_id="", user_id="", source="", client_key="", client_name=""):
    pc = str(source_pick_code or "").strip()
    if not pc or not is_copy_play_enabled():
        return False

    now = _now_ts()
    record = {
        "source_pick_code": pc,
        "file_name": str(file_name or ""),
        "item_id": str(item_id or ""),
        "play_session_id": str(play_session_id or ""),
        "user_id": str(user_id or ""),
        "client_key": str(client_key or ""),
        "source": source,
        "created_at": now,
        "updated_at": now,
        "created_at_text": datetime.now(timezone.utc).isoformat(),
    }
    record_key = _active_source_key(record)

    with _ACTIVE_SOURCES_LOCK:
        records = _active_sources_without_expired()
        kept = []
        replaced = False
        for existing in records:
            if _active_source_key(existing) == record_key:
                record["created_at"] = float(existing.get("created_at") or now)
                record["created_at_text"] = existing.get("created_at_text") or record["created_at_text"]
                kept.append(record)
                replaced = True
            else:
                kept.append(existing)
        if not replaced:
            kept.append(record)
        _save_active_sources(kept)
    return True


def _friendly_client_name(value):
    text = str(value or "").strip()
    if not text:
        return ""
    lower = text.lower()
    known_clients = [
        ("senplayer", "SenPlayer"),
        ("embymedia.embytheater", "Emby Theater"),
        ("emby theater", "Emby Theater"),
        ("infuse", "Infuse"),
        ("applecoremedia", "Apple Core Media"),
        ("emby for ios", "Emby for iOS"),
        ("emby for android", "Emby for Android"),
        ("androidtv", "Android TV"),
        ("lavf", "Emby 服务端"),
    ]
    for marker, label in known_clients:
        if marker in lower:
            return label
    return text.split("/", 1)[0].strip()[:40]


def _prepare_lock_key(source_pick_code, item_id="", play_session_id="", user_id="", client_key=""):
    source_pick_code = str(source_pick_code or "").strip()
    play_session_id = str(play_session_id or "").strip()
    if play_session_id:
        return f"{source_pick_code}|ps:{play_session_id}"
    return source_pick_code


def _get_prepare_lock(key):
    with _PREPARE_LOCKS_GUARD:
        lock = _PREPARE_LOCKS.get(key)
        if not lock:
            lock = threading.Lock()
            _PREPARE_LOCKS[key] = lock
        return lock


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
        logger.debug(
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


def recycle_clone_after_direct_url(clone_pick_code, reason="起播后清理"):
    pc = str(clone_pick_code or "").strip()
    if not pc or not is_copy_play_enabled():
        return False

    clones = _load_clones()
    if not clones:
        return False

    target = None
    for clone in clones:
        if pc == str(clone.get("clone_pick_code") or "").strip():
            target = clone
            break
    if not target or target.get("recycled_after_direct_url"):
        return False

    now = _now_ts()
    target["recycled_after_direct_url"] = True
    target["recycled_reason"] = reason
    target["recycle_after_ts"] = now + COPY_PLAY_RECYCLE_DELAY_SECONDS
    target["recycle_marked_at"] = now
    _save_clones(clones)
    logger.debug("  ➜ [复制播放] 克隆体已标记延迟清理：%s，%s 秒后可删除。", target.get("file_name") or pc[:8] + "...", COPY_PLAY_RECYCLE_DELAY_SECONDS)
    return True


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
        recycle_after_ts = float(clone.get("recycle_after_ts") or 0)
        if recycle_after_ts and now >= recycle_after_ts:
            if _delete_clone(client, clone, clone.get("recycled_reason") or "延迟清理"):
                removed += 1
                continue
        if created_at and now - created_at > COPY_PLAY_TTL_SECONDS:
            if _delete_clone(client, clone, "过期清理"):
                removed += 1
                continue
        kept.append(clone)
    if removed:
        _save_clones(kept)
    return removed


def prepare_copy_play_pick_code(source_pick_code, *, file_name="", item_id="", play_session_id="", user_id="", source="", client_key="", client_name="", force_new=False):
    lock_key = _prepare_lock_key(source_pick_code, item_id, play_session_id, user_id, client_key)
    lock = _get_prepare_lock(lock_key)
    with lock:
        return _prepare_copy_play_pick_code_locked(
            source_pick_code,
            file_name=file_name,
            item_id=item_id,
            play_session_id=play_session_id,
            user_id=user_id,
            source=source,
            client_key=client_key,
            client_name=client_name,
            force_new=force_new,
        )


def _prepare_copy_play_pick_code_locked(source_pick_code, *, file_name="", item_id="", play_session_id="", user_id="", source="", client_key="", client_name="", force_new=False):
    if not is_copy_play_enabled():
        return source_pick_code

    client = P115Service.get_client()
    if not client:
        logger.warning("  ➜ [复制播放] 115 客户端未初始化，终止本次点播。")
        return ""

    try:
        temp_cid = _ensure_copy_play_temp_dir(client)
    except Exception as e:
        logger.warning("  ➜ [复制播放] 无法确认复制播放目录，终止本次点播：%s", e)
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

    reusable = {} if force_new else _find_reusable_clone(
        source_pick_code,
        source_fid,
        temp_cid,
        item_id=str(item_id or ""),
        play_session_id=str(play_session_id or ""),
        user_id=str(user_id or ""),
        client_key=str(client_key or ""),
    )
    if reusable:
        return reusable["clone_pick_code"]

    friendly_client = _friendly_client_name(client_name)
    logger.info(
        "  ➜ [复制播放] 开始复制播放：%s%s",
        display_name,
        f"，客户端：{friendly_client}" if friendly_client else "",
    )

    clone = {}
    last_copy_error = ""
    for backend in _copy_backend_order():
        try:
            copy_resp = _copy_file_with_backend(client, source_fid, temp_cid, backend)
        except Exception as e:
            last_copy_error = str(e)
            logger.warning("  ➜ [复制播放] %s 复制接口异常，准备尝试备用接口：%s", backend, e)
            continue

        _log_copy_response(copy_resp, backend=backend)
        if not isinstance(copy_resp, dict) or not copy_resp.get("state"):
            last_copy_error = _safe_json(copy_resp)
            logger.warning("  ➜ [复制播放] %s 复制失败，准备尝试备用接口。", backend)
            continue

        response_clones = _extract_clones_from_copy_response(copy_resp, source_fid=source_fid, temp_cid=temp_cid, fallback_name=display_name)
        response_fids = _extract_ids_from_copy_response(copy_resp)
        logger.debug(
            "  ➜ [复制播放] %s 复制返回解析结果：克隆候选=%s，FID候选=%s",
            backend,
            len(response_clones),
            len(response_fids),
        )
        if response_clones:
            clone = response_clones[0]
            logger.debug(
                "  ➜ [复制播放] 已从 %s 复制接口返回中拿到克隆 PC：文件=%s，FID=%s，PC=%s",
                backend,
                clone.get("name") or display_name,
                clone.get("fid"),
                clone.get("pick_code", "")[:8] + "...",
            )
            break

        for attempt in range(1, 6):
            item = _find_clone_in_temp_dir(client, temp_cid, source_row, display_name)
            clone = _item_to_clone(item, fallback_parent_id=temp_cid, fallback_name=display_name)
            if clone:
                logger.debug(
                    "  ➜ [复制播放] 已从临时目录列表拿到 %s 克隆 PC：文件=%s，FID=%s，PC=%s",
                    backend,
                    clone.get("name") or display_name,
                    clone.get("fid"),
                    clone.get("pick_code", "")[:8] + "...",
                )
                break
            logger.debug("  ➜ [复制播放] %s 第 %s 次未查到可播放克隆体，等待后重试。", backend, attempt)
            time.sleep(1)
        if clone:
            break
        last_copy_error = f"{backend} 已复制但未拿到克隆 PC"
        _diagnose_copy_clone_failure(
            client,
            temp_cid,
            source_row,
            display_name,
            backend=backend,
            copy_resp=copy_resp,
            response_fids=response_fids,
        )
        logger.warning("  ➜ [复制播放] %s 已确认复制成功，但临时目录仍未刷出克隆体，终止本次点播，避免重复复制。", backend)
        break

    if not clone or not clone.get("pick_code"):
        logger.warning("  ➜ [复制播放] 未拿到克隆 PC，终止本次点播：%s", last_copy_error or "-")
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
        "client_key": str(client_key or ""),
        "source": source,
        "created_at": _now_ts(),
        "created_at_text": datetime.now(timezone.utc).isoformat(),
    }
    _record_clone(record)
    logger.debug(
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
    client_key = _client_key_from_webhook(data)

    removed_active = 0
    with _ACTIVE_SOURCES_LOCK:
        previous_active_records = _load_active_sources()
        active_records = _active_sources_without_expired(previous_active_records)
        kept_active = []
        for record in active_records:
            if _matches_playback_stop(
                record,
                play_session_id=play_session_id,
                item_id=item_id,
                user_id=user_id,
                client_key=client_key,
            ):
                removed_active += 1
                continue
            kept_active.append(record)
        if removed_active or len(kept_active) != len(previous_active_records):
            _save_active_sources(kept_active)
        if removed_active:
            logger.debug("  ➜ [复制播放] 停止播放清理源文件活跃记录：%s", removed_active)

    client = P115Service.get_client()
    if not client:
        return removed_active
    try:
        cleanup_expired_clones(client)
    except Exception as e:
        logger.debug(f"  ➜ [复制播放] 停止播放时清理过期克隆失败：{e}")
    return removed_active
