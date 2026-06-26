import json
import hashlib
import logging
import re
import threading
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests

from database import settings_db, user_db
from handler.p115_play_pool_client import P115PlayPoolClient
from handler.p115_service import P115CacheManager, P115Service

logger = logging.getLogger(__name__)

PLAY_POOL_CONFIG_KEY = "p115_play_pool_config"
PLAY_POOL_SESSIONS_KEY = "p115_play_pool_sessions"
PLAY_POOL_TEMP_DIR_NAME = "ETK小号播放临时目录"
PLAY_POOL_SESSION_TTL_SECONDS = 12 * 60 * 60
_PREPARE_LOCKS = {}
_PREPARE_LOCKS_GUARD = threading.Lock()
_ALLOWED_USER_EXPAND_CACHE = {}
_ALLOWED_USER_EXPAND_TTL_SECONDS = 60


def _now_ts():
    return time.time()


def _now_text():
    return datetime.now(timezone.utc).isoformat()


def _today_key():
    return datetime.now().strftime("%Y-%m-%d")


def _safe_int(value, default=0):
    try:
        if value in (None, "", [], {}):
            return default
        return int(float(value))
    except Exception:
        return default


def _safe_float(value, default=0.0):
    try:
        if value in (None, "", [], {}):
            return default
        return float(value)
    except Exception:
        return default


def _mask_cookie(cookie):
    text = str(cookie or "")
    if not text:
        return ""
    uid = re.search(r"(?:^|;\s*)UID=([^;]+)", text)
    if uid:
        raw = uid.group(1)
        return f"UID={raw[:4]}***{raw[-3:]}" if len(raw) > 7 else "UID=***"
    return text[:8] + "***"


def _human_bytes(value):
    size = float(_safe_int(value, 0))
    if size <= 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    idx = 0
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024.0
        idx += 1
    return f"{int(size)} {units[idx]}" if idx == 0 else f"{size:.2f} {units[idx]}"


def _limit_gb_to_bytes(value):
    gb = _safe_float(value, 0.0)
    return int(gb * 1024 * 1024 * 1024) if gb > 0 else 0


def _human_gb_limit(value):
    bytes_value = _limit_gb_to_bytes(value)
    return _human_bytes(bytes_value) if bytes_value > 0 else ""


def _account_daily_limited(account):
    daily_limit_bytes = _limit_gb_to_bytes((account or {}).get("daily_traffic_limit_gb"))
    return bool(daily_limit_bytes and _safe_int((account or {}).get("daily_traffic_bytes"), 0) >= daily_limit_bytes)


def _normalize_daily_traffic(account):
    today = _today_key()
    if str(account.get("daily_traffic_date") or "") != today:
        account["daily_traffic_date"] = today
        account["daily_traffic_bytes"] = 0
    else:
        account["daily_traffic_bytes"] = _safe_int(account.get("daily_traffic_bytes"), 0)


def _display_title(file_name):
    text = str(file_name or "").strip()
    if not text:
        return "未知影片"
    text = re.sub(r"\.[A-Za-z0-9]{2,5}$", "", text)
    episode_match = re.search(r"\bS\d{1,2}E\d{1,3}\b", text, re.IGNORECASE)
    episode_text = episode_match.group(0).upper() if episode_match else ""
    match = re.match(r"^(.+?\(\d{4}\))", text)
    if match:
        title = match.group(1).strip()
    else:
        title = text.split(" · ", 1)[0].strip() or text
    return f"{title} - {episode_text}" if episode_text else title


def _display_user_name(user_id):
    user_id = str(user_id or "").strip()
    if not user_id:
        return "未知用户"
    try:
        return user_db.get_username_by_id(user_id) or user_id
    except Exception as e:
        logger.debug("  ➜ [小号播放] 查询 Emby 用户名失败: user_id=%s, err=%s", user_id, e)
        return user_id


def _normalize_user_ids(value):
    if not isinstance(value, list):
        return []
    seen = set()
    result = []
    for item in value:
        user_id = str(item or "").strip()
        if user_id and user_id not in seen:
            seen.add(user_id)
            result.append(user_id)
    return result


def _expand_allowed_user_ids(selected_user_ids):
    selected = _normalize_user_ids(selected_user_ids)
    if not selected:
        return []
    cache_key = tuple(selected)
    cached = _ALLOWED_USER_EXPAND_CACHE.get(cache_key)
    now = _now_ts()
    if cached and now - float(cached.get("ts") or 0) < _ALLOWED_USER_EXPAND_TTL_SECONDS:
        return list(cached.get("value") or [])
    try:
        expanded = user_db.expand_template_user_ids(selected)
    except Exception as e:
        logger.warning("  ➜ [小号播放] 展开小号可用用户失败，将仅使用原始选择: %s", e)
        expanded = selected
    result = _normalize_user_ids(list(selected) + list(expanded or []))
    _ALLOWED_USER_EXPAND_CACHE[cache_key] = {"ts": now, "value": result}
    return result


def _account_allowed_for_user(account, user_id):
    raw_allowed = _normalize_user_ids((account or {}).get("allowed_user_ids"))
    allowed = _expand_allowed_user_ids(raw_allowed) if raw_allowed else []
    if not allowed:
        allowed = _normalize_user_ids((account or {}).get("allowed_effective_user_ids"))
    if not allowed:
        return True
    user_id = str(user_id or "").strip()
    return bool(user_id and user_id in set(allowed))


def _load_config():
    data = settings_db.get_setting(PLAY_POOL_CONFIG_KEY) or {}
    if not isinstance(data, dict):
        data = {}
    accounts = data.get("accounts")
    if not isinstance(accounts, list):
        accounts = []
    clean_accounts = []
    for item in accounts:
        if not isinstance(item, dict):
            continue
        account = dict(item)
        account["id"] = str(account.get("id") or uuid.uuid4().hex)
        account["alias"] = str(account.get("alias") or "小号").strip()[:40] or "小号"
        account["cookie"] = str(account.get("cookie") or "").strip()
        account["app_type"] = str(account.get("app_type") or "alipaymini").strip() or "alipaymini"
        account["enabled"] = bool(account.get("enabled", True))
        account["temp_cid"] = str(account.get("temp_cid") or "").strip()
        account["play_count"] = _safe_int(account.get("play_count"), 0)
        account["traffic_bytes"] = _safe_int(account.get("traffic_bytes"), 0)
        account["daily_traffic_limit_gb"] = _safe_float(account.get("daily_traffic_limit_gb"), 0.0)
        _normalize_daily_traffic(account)
        account["active_count"] = _safe_int(account.get("active_count"), 0)
        account["last_speed_bps"] = _safe_int(account.get("last_speed_bps"), 0)
        account["allowed_user_ids"] = _normalize_user_ids(account.get("allowed_user_ids"))
        account["allowed_effective_user_ids"] = _normalize_user_ids(account.get("allowed_effective_user_ids"))
        account.pop("last_failed_at", None)
        clean_accounts.append(account)
    return {
        "enabled": bool(data.get("enabled", False)),
        "accounts": clean_accounts,
        "updated_at": data.get("updated_at") or "",
    }


def _save_config(config):
    payload = {
        "enabled": bool(config.get("enabled", False)),
        "accounts": config.get("accounts") if isinstance(config.get("accounts"), list) else [],
        "updated_at": _now_text(),
    }
    settings_db.save_setting(PLAY_POOL_CONFIG_KEY, payload)
    return payload


def _public_account(account):
    out = dict(account or {})
    out.pop("cookie", None)
    out.pop("allowed_effective_user_ids", None)
    out["cookie_mask"] = _mask_cookie(account.get("cookie"))
    out["traffic_text"] = _human_bytes(account.get("traffic_bytes"))
    out["daily_traffic_text"] = _human_bytes(account.get("daily_traffic_bytes"))
    out["daily_traffic_limit_text"] = _human_gb_limit(account.get("daily_traffic_limit_gb"))
    out["daily_traffic_limited"] = _account_daily_limited(account)
    speed = _safe_int(account.get("last_speed_bps"), 0)
    out["last_speed_text"] = f"{_human_bytes(speed)}/s" if speed > 0 else "未测速"
    return out


def get_public_config():
    config = _load_config()
    return {
        "enabled": config["enabled"],
        "accounts": [_public_account(x) for x in config["accounts"]],
        "usable_count": len([x for x in config["accounts"] if x.get("enabled") and x.get("cookie") and not _account_daily_limited(x)]),
        "temp_dir_name": PLAY_POOL_TEMP_DIR_NAME,
    }


def save_pool_enabled(enabled):
    config = _load_config()
    config["enabled"] = bool(enabled)
    _save_config(config)
    return get_public_config()


def upsert_account(payload, account_id=None):
    payload = payload if isinstance(payload, dict) else {}
    config = _load_config()
    accounts = config["accounts"]
    target = None
    if account_id:
        for item in accounts:
            if str(item.get("id")) == str(account_id):
                target = item
                break
    if target is None:
        target = {
            "id": uuid.uuid4().hex,
            "play_count": 0,
            "traffic_bytes": 0,
            "active_count": 0,
            "created_at": _now_text(),
        }
        accounts.append(target)

    if "alias" in payload:
        target["alias"] = str(payload.get("alias") or "小号").strip()[:40] or "小号"
    elif not target.get("alias"):
        target["alias"] = "小号"
    if "cookie" in payload:
        target["cookie"] = str(payload.get("cookie") or "").strip()
    if "app_type" in payload:
        target["app_type"] = str(payload.get("app_type") or "alipaymini").strip() or "alipaymini"
    elif not target.get("app_type"):
        target["app_type"] = "alipaymini"
    if "enabled" in payload:
        target["enabled"] = bool(payload.get("enabled"))
    elif "enabled" not in target:
        target["enabled"] = True
    if "daily_traffic_limit_gb" in payload:
        target["daily_traffic_limit_gb"] = max(0.0, _safe_float(payload.get("daily_traffic_limit_gb"), 0.0))
    if "allowed_user_ids" in payload:
        target["allowed_user_ids"] = _normalize_user_ids(payload.get("allowed_user_ids"))
        target["allowed_effective_user_ids"] = _expand_allowed_user_ids(target["allowed_user_ids"])
    else:
        target["allowed_user_ids"] = _normalize_user_ids(target.get("allowed_user_ids"))
        target["allowed_effective_user_ids"] = _normalize_user_ids(target.get("allowed_effective_user_ids"))
    target["updated_at"] = _now_text()
    target.setdefault("temp_cid", "")
    _save_config(config)
    return _public_account(target)


def delete_account(account_id):
    config = _load_config()
    before = len(config["accounts"])
    config["accounts"] = [x for x in config["accounts"] if str(x.get("id")) != str(account_id)]
    _save_config(config)
    return len(config["accounts"]) < before


def _load_sessions():
    data = settings_db.get_setting(PLAY_POOL_SESSIONS_KEY) or []
    return data if isinstance(data, list) else []


def _save_sessions(sessions):
    settings_db.save_setting(PLAY_POOL_SESSIONS_KEY, sessions[-500:])


def _account_client(account):
    return P115PlayPoolClient(account.get("cookie"), account.get("app_type") or "alipaymini")


def _extract_cid(resp):
    if not isinstance(resp, dict):
        return ""
    for key in ("cid", "file_id", "fid", "id"):
        value = resp.get(key)
        if value not in (None, "", [], {}):
            return str(value)
    data = resp.get("data")
    if isinstance(data, dict):
        for key in ("cid", "file_id", "fid", "id"):
            value = data.get(key)
            if value not in (None, "", [], {}):
                return str(value)
    return ""


def _ensure_temp_cid(account, client):
    cid = str(account.get("temp_cid") or "").strip()
    if cid:
        return cid

    found = ""
    try:
        resp = client.fs_files({
            "cid": "0",
            "search_value": PLAY_POOL_TEMP_DIR_NAME,
            "show_dir": 1,
            "limit": 100,
            "offset": 0,
            "record_open_time": 0,
            "count_folders": 0,
        })
        for item in resp.get("data") or []:
            name = item.get("name") or item.get("file_name") or item.get("fn")
            fc = str(item.get("fc") if item.get("fc") is not None else item.get("type") or "")
            item_id = item.get("fid") or item.get("file_id") or item.get("id") or item.get("cid")
            if name == PLAY_POOL_TEMP_DIR_NAME and item_id and (not fc or fc == "0"):
                found = str(item_id)
                break
    except Exception as e:
        logger.debug(f"  ➜ [小号播放] 查询小号临时目录失败: {account.get('alias')}, err={e}")

    if not found:
        resp = client.fs_mkdir(PLAY_POOL_TEMP_DIR_NAME, "0")
        if not resp.get("state"):
            raise RuntimeError(f"创建小号临时目录失败: {resp.get('error_msg') or resp.get('message') or resp}")
        found = _extract_cid(resp)
    if not found:
        raise RuntimeError("创建小号临时目录后未拿到 CID")

    account["temp_cid"] = found
    return found


def _extract_clone_from_rapid_response(resp, client, temp_cid, file_name, sha1, size):
    candidates = []

    def walk(value):
        if isinstance(value, dict):
            fid = str(value.get("fid") or value.get("file_id") or value.get("id") or "").strip()
            pc = str(value.get("pick_code") or value.get("pickcode") or value.get("pc") or "").strip()
            name = str(value.get("name") or value.get("file_name") or value.get("fn") or file_name or "").strip()
            if fid or pc:
                candidates.append({"fid": fid, "pick_code": pc, "name": name})
            for sub in value.values():
                walk(sub)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(resp)
    for item in candidates:
        if item.get("fid") and item.get("pick_code"):
            return item

    resp = client.fs_files({
        "cid": temp_cid,
        "search_value": file_name,
        "show_dir": 0,
        "limit": 100,
        "offset": 0,
        "record_open_time": 0,
        "count_folders": 0,
    })
    expected_size = _safe_int(size, 0)
    expected_sha1 = str(sha1 or "").upper()
    for item in resp.get("data") or []:
        item_name = str(item.get("name") or item.get("file_name") or item.get("fn") or "").strip()
        item_size = _safe_int(item.get("size") or item.get("fs"), 0)
        item_sha1 = str(item.get("sha1") or item.get("sha") or "").upper()
        if file_name and item_name and item_name != file_name:
            stem = re.escape(file_name.rsplit(".", 1)[0]) if "." in file_name else re.escape(file_name)
            if not re.match(stem + r"(?:\(\d+\))?(?:\.[^.]+)?$", item_name):
                continue
        if expected_size and item_size and item_size != expected_size:
            continue
        if expected_sha1 and item_sha1 and item_sha1 != expected_sha1:
            continue
        fid = str(item.get("fid") or item.get("file_id") or item.get("id") or "").strip()
        pc = str(item.get("pick_code") or item.get("pickcode") or item.get("pc") or "").strip()
        if fid and pc:
            return {"fid": fid, "pick_code": pc, "name": item_name or file_name}
    return {}


def _select_account(config, user_id=""):
    sessions = _load_sessions()
    active_counts = {}
    now = _now_ts()
    for session in sessions:
        created_at = float(session.get("created_at") or 0)
        if created_at and now - created_at > PLAY_POOL_SESSION_TTL_SECONDS:
            continue
        account_id = str(session.get("account_id") or "")
        if account_id:
            active_counts[account_id] = active_counts.get(account_id, 0) + 1

    candidates = []
    for account in config.get("accounts") or []:
        if not account.get("enabled") or not account.get("cookie"):
            continue
        if not _account_allowed_for_user(account, user_id):
            continue
        _normalize_daily_traffic(account)
        if _account_daily_limited(account):
            continue
        account = dict(account)
        account["_active_count"] = active_counts.get(str(account.get("id")), 0)
        candidates.append(account)
    if not candidates:
        return None
    candidates.sort(key=lambda x: (x.get("_active_count", 0), -_safe_int(x.get("last_speed_bps"), 0), float(x.get("last_used_at") or 0)))
    return candidates[0]


def has_usable_pool():
    config = _load_config()
    return bool(config.get("enabled") and _select_account(config))


def has_usable_pool_for_user(user_id=""):
    config = _load_config()
    return bool(config.get("enabled") and _select_account(config, user_id=user_id))


def _mark_account(account_id, patch):
    config = _load_config()
    for account in config["accounts"]:
        if str(account.get("id")) == str(account_id):
            account.update(patch)
            account["updated_at"] = _now_text()
            break
    _save_config(config)


def _record_session(record):
    sessions = _load_sessions()
    sessions.append(record)
    _save_sessions(sessions)


def _patch_session(session_id, patch):
    session_id = str(session_id or "").strip()
    if not session_id:
        return False
    sessions = _load_sessions()
    changed = False
    for item in sessions:
        if session_id == str(item.get("session_id") or "").strip():
            item.update(patch)
            changed = True
            break
    if changed:
        _save_sessions(sessions)
    return changed


def _prepare_lock_key(source_pick_code, item_id, play_session_id, user_id, client_key):
    source_pick_code = str(source_pick_code or "").strip()
    play_session_id = str(play_session_id or "").strip()
    if play_session_id:
        return f"{source_pick_code}|ps:{play_session_id}"
    return "|".join([
        source_pick_code,
        str(item_id or "").strip(),
        str(user_id or "").strip(),
        str(client_key or "").strip(),
    ])


def _get_prepare_lock(key):
    with _PREPARE_LOCKS_GUARD:
        lock = _PREPARE_LOCKS.get(key)
        if not lock:
            lock = threading.Lock()
            _PREPARE_LOCKS[key] = lock
        return lock


def _find_reusable_session(*, source_pick_code="", item_id="", play_session_id="", user_id="", client_key=""):
    source_pick_code = str(source_pick_code or "").strip()
    item_id = str(item_id or "").strip()
    play_session_id = str(play_session_id or "").strip()
    user_id = str(user_id or "").strip()
    client_key = str(client_key or "").strip()
    if not source_pick_code:
        return {}

    now = _now_ts()
    for session in reversed(_load_sessions()):
        created_at = float(session.get("created_at") or 0)
        if created_at and now - created_at > PLAY_POOL_SESSION_TTL_SECONDS:
            continue
        if source_pick_code != str(session.get("source_pick_code") or ""):
            continue
        has_direct_url = bool(str(session.get("direct_url") or "").strip())
        has_temp_file = bool(session.get("temp_pick_code") and session.get("temp_fid"))
        if not has_direct_url and not has_temp_file:
            continue

        session_play_session_id = str(session.get("play_session_id") or "")
        if play_session_id and session_play_session_id and play_session_id == session_play_session_id:
            return dict(session)
        if play_session_id and session_play_session_id:
            continue

        session_item_id = str(session.get("item_id") or "")
        if item_id and session_item_id and item_id != session_item_id:
            continue
        session_user_id = str(session.get("user_id") or "")
        if user_id and session_user_id and user_id != session_user_id:
            continue
        session_client_key = str(session.get("client_key") or "")
        if client_key and session_client_key and client_key != session_client_key:
            continue
        if item_id or user_id or client_key:
            return dict(session)
    return {}


def _cleanup_superseded_sessions(*, source_pick_code="", user_id="", client_key=""):
    source_pick_code = str(source_pick_code or "").strip()
    user_id = str(user_id or "").strip()
    client_key = str(client_key or "").strip()
    if not source_pick_code or not (user_id or client_key):
        return 0

    sessions = _load_sessions()
    if not sessions:
        return 0
    account_map = {str(a.get("id")): a for a in _load_config().get("accounts") or []}
    kept = []
    removed = 0
    for session in sessions:
        if source_pick_code == str(session.get("source_pick_code") or ""):
            kept.append(session)
            continue
        session_user_id = str(session.get("user_id") or "")
        session_client_key = str(session.get("client_key") or "")
        match_user = bool(user_id and session_user_id and user_id == session_user_id)
        match_client = bool(client_key and session_client_key and client_key == session_client_key)
        if match_user and (match_client or not session_client_key):
            if session.get("recycled_after_direct_url"):
                removed += 1
                continue
            account = account_map.get(str(session.get("account_id") or ""))
            if account and _delete_session_file(account, session, "下一集起播清理"):
                removed += 1
                continue
        kept.append(session)
    if removed:
        _save_sessions(kept)
    return removed


def _pick_upload_file_name(request_name, cache_name, fallback):
    request_name = str(request_name or "").strip()
    cache_name = str(cache_name or "").strip()
    if cache_name and "." in cache_name.rsplit("/", 1)[-1]:
        return cache_name
    if request_name and "." in request_name.rsplit("/", 1)[-1]:
        return request_name
    return cache_name or request_name or fallback


def _is_plain_upload_response(resp):
    if not isinstance(resp, dict):
        return False
    if resp.get("_rapid_cookie_need_plain_upload"):
        return True
    text = str(resp.get("error_msg") or resp.get("message") or resp)
    return "status=1" in text or "普通上传" in text


def _force_refresh_preid(source_row, pick_code, sha1, file_name):
    pick_code = str(pick_code or "").strip()
    if not pick_code:
        return ""
    chunk = P115CacheManager._extract_preid_range_bytes(pick_code, 0, 131071)
    if not chunk:
        return ""
    preid = hashlib.sha1(chunk).hexdigest().upper()
    try:
        P115CacheManager._update_preid_for_existing_cache(
            preid,
            fid=(source_row or {}).get("id") or (source_row or {}).get("fid"),
            parent_id=(source_row or {}).get("parent_id") or (source_row or {}).get("pid") or (source_row or {}).get("cid"),
            name=file_name,
            sha1=sha1,
            pick_code=pick_code,
        )
    except Exception as e:
        logger.debug(f"  ➜ [小号播放] 强制刷新 preid 后回写缓存失败: pc={pick_code[:8]}..., err={e}")
    return preid


def prepare_play_pool_pick_code(source_pick_code, *, file_name="", item_id="", play_session_id="", user_id="", source="", client_key="", user_agent=""):
    lock_key = _prepare_lock_key(source_pick_code, item_id, play_session_id, user_id, client_key)
    lock = _get_prepare_lock(lock_key)
    with lock:
        return _prepare_play_pool_pick_code_locked(
            source_pick_code,
            file_name=file_name,
            item_id=item_id,
            play_session_id=play_session_id,
            user_id=user_id,
            source=source,
            client_key=client_key,
            user_agent=user_agent,
        )


def _prepare_play_pool_pick_code_locked(source_pick_code, *, file_name="", item_id="", play_session_id="", user_id="", source="", client_key="", user_agent=""):
    config = _load_config()
    if not config.get("enabled"):
        return {}
    account = _select_account(config, user_id=user_id)
    if not account:
        return {}

    source_row = P115CacheManager.get_file_cache_by_pickcode(source_pick_code) or {}
    sha1 = str(source_row.get("sha1") or "").strip().upper()
    size = _safe_int(source_row.get("size"), 0)
    preid = str(source_row.get("preid") or source_row.get("pre_sha1") or "").strip().upper()
    display_name = _pick_upload_file_name(file_name, source_row.get("name"), f"{sha1 or source_pick_code}.mkv")
    if not re.fullmatch(r"[A-F0-9]{40}", sha1 or "") or size <= 0:
        raise RuntimeError("小号播放需要源文件 SHA1 和 size，本地 115 缓存未命中完整信息")
    if not re.fullmatch(r"[A-F0-9]{40}", preid or ""):
        try:
            preid = P115CacheManager.ensure_file_preid(
                source_row,
                sha1=sha1,
                pick_code=source_pick_code,
                file_name=display_name,
            ) or preid
        except Exception as e:
            logger.debug(f"  ➜ [小号播放] 秒传前补齐 preid 失败: pc={str(source_pick_code)[:8]}..., err={e}")

    reusable = _find_reusable_session(
        source_pick_code=source_pick_code,
        item_id=item_id,
        play_session_id=play_session_id,
        user_id=user_id,
        client_key=client_key,
    )
    if reusable:
        account = next((x for x in config.get("accounts") or [] if str(x.get("id")) == str(reusable.get("account_id"))), None)
        if account and not _account_allowed_for_user(account, user_id):
            account = None
        if reusable.get("direct_url") or (account and account.get("enabled") and account.get("cookie")):
            logger.debug(
                "  ➜ [小号播放] 复用已有小号播放记录：%s | account=%s | session=%s | direct_url=%s",
                reusable.get("file_name") or display_name,
                (account or {}).get("alias") or (account or {}).get("id") or reusable.get("account_alias") or "-",
                reusable.get("session_id") or "-",
                "yes" if reusable.get("direct_url") else "no",
            )
            return {
                "pick_code": reusable.get("temp_pick_code") or "",
                "client": _account_client(account) if account and account.get("cookie") else None,
                "account": account or {},
                "session": reusable,
                "direct_url": reusable.get("direct_url") or "",
            }

    _cleanup_superseded_sessions(
        source_pick_code=source_pick_code,
        user_id=user_id,
        client_key=client_key,
    )

    client = _account_client(account)
    temp_cid = _ensure_temp_cid(account, client)
    payload = {
        "cid": temp_cid,
        "sha1": sha1,
        "size": size,
        "file_name": display_name,
        "pick_code": source_pick_code,
    }
    if preid:
        payload["preid"] = preid
    logger.info(
        "  ➜ [小号播放] 准备播放：%s %s/%s",
        _display_title(display_name),
        account.get("alias") or account.get("id") or "小号",
        _display_user_name(user_id),
    )

    resp = client.rapid_upload(payload)
    if _is_plain_upload_response(resp):
        fresh_preid = _force_refresh_preid(source_row, source_pick_code, sha1, display_name)
        if fresh_preid and fresh_preid != preid:
            logger.debug(
                "  ➜ [小号播放] 小号秒传返回普通上传，已强制刷新 preid 后重试：%s -> %s",
                (preid[:12] + "...") if preid else "-",
                fresh_preid[:12] + "...",
            )
            preid = fresh_preid
            payload["preid"] = preid
            resp = client.rapid_upload(payload)
    if isinstance(resp, dict) and resp.get("_rapid_sign_required"):
        main_client = P115Service.get_client()
        if not main_client:
            raise RuntimeError("小号秒传需要主号签名，但主号 115 客户端未初始化")
        sign = main_client.rapid_sign_value({
            "sha1": sha1,
            "pick_code": source_pick_code,
            "sign_check": resp.get("_rapid_sign_check"),
            "size": size,
            "file_name": display_name,
            "user_agent": user_agent,
        })
        payload["sign_key"] = resp.get("_rapid_sign_key")
        payload["sign_val"] = sign.get("sign_val")
        resp = client.rapid_upload(payload)

    if not isinstance(resp, dict) or not resp.get("state"):
        _mark_account(account["id"], {"last_error": str((resp or {}).get("error_msg") or resp)})
        raise RuntimeError(f"小号秒传失败: {(resp or {}).get('error_msg') or resp}")

    clone = _extract_clone_from_rapid_response(resp, client, temp_cid, display_name, sha1, size)
    if not clone.get("pick_code") or not clone.get("fid"):
        raise RuntimeError("小号秒传成功但未找到临时文件 pick_code")

    record = {
        "session_id": uuid.uuid4().hex,
        "account_id": account["id"],
        "account_alias": account.get("alias") or "",
        "source_pick_code": str(source_pick_code),
        "source_sha1": sha1,
        "source_size": size,
        "temp_cid": temp_cid,
        "temp_fid": clone["fid"],
        "temp_pick_code": clone["pick_code"],
        "file_name": clone.get("name") or display_name,
        "item_id": str(item_id or ""),
        "play_session_id": str(play_session_id or ""),
        "user_id": str(user_id or ""),
        "client_key": str(client_key or ""),
        "source": source,
        "created_at": _now_ts(),
        "created_at_text": _now_text(),
    }
    _record_session(record)
    today = _today_key()
    account_daily_bytes = _safe_int(account.get("daily_traffic_bytes"), 0)
    if str(account.get("daily_traffic_date") or "") != today:
        account_daily_bytes = 0
    _mark_account(account["id"], {
        "last_used_at": _now_ts(),
        "last_error": "",
        "play_count": _safe_int(account.get("play_count"), 0) + 1,
        "traffic_bytes": _safe_int(account.get("traffic_bytes"), 0) + size,
        "daily_traffic_date": today,
        "daily_traffic_bytes": account_daily_bytes + size,
        "temp_cid": temp_cid,
    })
    return {"pick_code": clone["pick_code"], "client": client, "account": account, "session": record}


def get_direct_url(play_result, user_agent=""):
    cached_url = str((play_result or {}).get("direct_url") or ((play_result or {}).get("session") or {}).get("direct_url") or "").strip()
    if cached_url:
        return cached_url
    client = play_result.get("client")
    pick_code = play_result.get("pick_code")
    if not client or not pick_code:
        return ""
    url = client.download_url(pick_code, user_agent=user_agent)
    session_id = str(((play_result or {}).get("session") or {}).get("session_id") or "").strip()
    if url and session_id:
        _patch_session(session_id, {
            "direct_url": url,
            "direct_url_cached_at": _now_ts(),
        })
        session = play_result.get("session") or {}
        session["direct_url"] = url
        session["direct_url_cached_at"] = _now_ts()
    return url


def recycle_session_after_direct_url(play_result, reason="起播后清理"):
    session = (play_result or {}).get("session") or {}
    if session.get("recycled_after_direct_url"):
        return False
    temp_pick_code = str((play_result or {}).get("pick_code") or session.get("temp_pick_code") or "").strip()
    session_id = str(session.get("session_id") or "").strip()
    if not temp_pick_code and not session_id:
        return False

    sessions = _load_sessions()
    if not sessions:
        return False

    target = None
    for item in sessions:
        if session_id and session_id == str(item.get("session_id") or ""):
            target = item
            break
        if temp_pick_code and temp_pick_code == str(item.get("temp_pick_code") or "").strip():
            target = item
            break
    if not target:
        return False

    account_id = str(target.get("account_id") or "")
    account = next((x for x in _load_config().get("accounts") or [] if str(x.get("id")) == account_id), None)
    if not account:
        return False
    if not _delete_session_file(account, target, reason):
        return False

    target["recycled_after_direct_url"] = True
    target["recycled_at"] = _now_ts()
    _save_sessions(sessions)
    return True


def cleanup_expired_sessions():
    sessions = _load_sessions()
    if not sessions:
        return 0
    now = _now_ts()
    kept = []
    removed = 0
    account_map = {str(a.get("id")): a for a in _load_config().get("accounts") or []}
    for session in sessions:
        created_at = float(session.get("created_at") or 0)
        if created_at and now - created_at > PLAY_POOL_SESSION_TTL_SECONDS:
            if session.get("recycled_after_direct_url"):
                removed += 1
                continue
            account = account_map.get(str(session.get("account_id") or ""))
            if account and _delete_session_file(account, session, "过期清理"):
                removed += 1
                continue
        kept.append(session)
    if removed:
        _save_sessions(kept)
    return removed


def _delete_session_file(account, session, reason):
    fid = str(session.get("temp_fid") or "").strip()
    if not fid:
        return False
    try:
        client = _account_client(account)
        resp = client.fs_delete([fid])
        ok = bool(isinstance(resp, dict) and resp.get("state"))
        if ok:
            logger.debug("  ➜ [小号播放] 已删除临时文件: %s，原因=%s", session.get("file_name") or fid, reason)
        else:
            logger.warning("  ➜ [小号播放] 删除临时文件失败: %s，resp=%s", session.get("file_name") or fid, resp)
        return ok
    except Exception as e:
        logger.warning("  ➜ [小号播放] 删除临时文件异常: %s，err=%s", session.get("file_name") or fid, e)
        return False


def _client_key_from_webhook(data):
    session = data.get("Session") or {}
    device_id = str(session.get("DeviceId") or data.get("DeviceId") or "").strip()
    remote_addr = str(data.get("_etk_webhook_remote_addr") or "").strip()
    client_name = str(session.get("Client") or "").strip()
    device_name = str(session.get("DeviceName") or "").strip()
    return "|".join([device_id or remote_addr, client_name, device_name])


def cleanup_for_playback_stop(data):
    playback_info = data.get("PlaybackInfo") or {}
    item = data.get("Item") or {}
    user = data.get("User") or {}
    play_session_id = str(playback_info.get("PlaySessionId") or data.get("PlaySessionId") or "").strip()
    item_id = str(item.get("Id") or "").strip()
    user_id = str(user.get("Id") or "").strip()
    client_key = _client_key_from_webhook(data)
    device_id = client_key.split("|", 1)[0]

    sessions = _load_sessions()
    if not sessions:
        return 0
    account_map = {str(a.get("id")): a for a in _load_config().get("accounts") or []}
    has_session_match = False
    for session in sessions:
        if not play_session_id or play_session_id != str(session.get("play_session_id") or ""):
            continue
        session_item_id = str(session.get("item_id") or "")
        session_user_id = str(session.get("user_id") or "")
        if item_id and session_item_id and item_id != session_item_id:
            continue
        if user_id and session_user_id and user_id != session_user_id:
            continue
        has_session_match = True
        break
    kept = []
    removed = 0
    for session in sessions:
        match_session = play_session_id and play_session_id == str(session.get("play_session_id") or "")
        if has_session_match:
            session_item_id = str(session.get("item_id") or "")
            session_user_id = str(session.get("user_id") or "")
            should_delete = bool(
                match_session
                and (not item_id or not session_item_id or item_id == session_item_id)
                and (not user_id or not session_user_id or user_id == session_user_id)
            )
        else:
            match_item = item_id and item_id == str(session.get("item_id") or "")
            session_client_key = str(session.get("client_key") or "")
            match_client = bool(
                (client_key and client_key == session_client_key)
                or (device_id and session_client_key.split("|", 1)[0] == device_id)
            )
            session_user_id = str(session.get("user_id") or "")
            match_user = not user_id or not session_user_id or user_id == session_user_id
            should_delete = bool(match_item and match_user and (match_client or not session_client_key))
        if should_delete:
            account = account_map.get(str(session.get("account_id") or ""))
            if account and _delete_session_file(account, session, "播放停止"):
                removed += 1
                continue
        kept.append(session)
    if removed:
        _save_sessions(kept)
    else:
        logger.debug(
            "  ➜ [小号播放] 播放停止未匹配到临时文件: item_id=%s, play_session_id=%s, user_id=%s, client_key=%s, sessions=%s",
            item_id or "-",
            play_session_id or "-",
            user_id or "-",
            client_key or "-",
            len(sessions),
        )
    return removed


def _speedtest_url(url, user_agent="", max_bytes=16 * 1024 * 1024, max_seconds=8):
    headers = {"User-Agent": user_agent or "Mozilla/5.0", "Accept": "*/*", "Range": f"bytes=0-{max_bytes - 1}"}
    start = time.time()
    total = 0
    with requests.get(url, headers=headers, stream=True, timeout=(8, max_seconds + 4), allow_redirects=True) as resp:
        resp.raise_for_status()
        for chunk in resp.iter_content(chunk_size=256 * 1024):
            if not chunk:
                continue
            total += len(chunk)
            if total >= max_bytes or time.time() - start >= max_seconds:
                break
    elapsed = max(0.001, time.time() - start)
    bps = int(total / elapsed)
    return {"bytes": total, "seconds": round(elapsed, 3), "bps": bps, "speed_text": f"{_human_bytes(bps)}/s"}


def speedtest_account(account_id, sample_pick_code="", user_agent=""):
    config = _load_config()
    account = next((x for x in config["accounts"] if str(x.get("id")) == str(account_id)), None)
    if not account:
        raise RuntimeError("小号不存在")
    if not account.get("cookie"):
        raise RuntimeError("小号未配置 Cookie")
    sample_pick_code = str(sample_pick_code or "").strip()
    if not sample_pick_code:
        try:
            from routes.p115 import _p115_pick_speedtest_sample_from_library
            sample = _p115_pick_speedtest_sample_from_library() or {}
            sample_pick_code = sample.get("pick_code") or ""
        except Exception:
            sample_pick_code = ""
    if not sample_pick_code:
        raise RuntimeError("未找到可用于测速的 115 样本文件")

    source_row = P115CacheManager.get_file_cache_by_pickcode(sample_pick_code) or {}
    sha1 = str(source_row.get("sha1") or "").strip().upper()
    size = _safe_int(source_row.get("size"), 0)
    preid = str(source_row.get("preid") or source_row.get("pre_sha1") or "").strip().upper()
    file_name = _pick_upload_file_name("", source_row.get("name"), "play-pool-speedtest.mkv")
    if not re.fullmatch(r"[A-F0-9]{40}", sha1 or "") or size <= 0:
        raise RuntimeError("测速样本缺少 SHA1 或 size")
    if not re.fullmatch(r"[A-F0-9]{40}", preid or ""):
        try:
            preid = P115CacheManager.ensure_file_preid(
                source_row,
                sha1=sha1,
                pick_code=sample_pick_code,
                file_name=file_name,
            ) or preid
        except Exception as e:
            logger.debug(f"  ➜ [小号播放] 测速前补齐 preid 失败: pc={sample_pick_code[:8]}..., err={e}")

    client = _account_client(account)
    temp_cid = _ensure_temp_cid(account, client)
    payload = {"cid": temp_cid, "sha1": sha1, "size": size, "file_name": file_name, "pick_code": sample_pick_code}
    if preid:
        payload["preid"] = preid
    resp = client.rapid_upload(payload)
    if _is_plain_upload_response(resp):
        fresh_preid = _force_refresh_preid(source_row, sample_pick_code, sha1, file_name)
        if fresh_preid and fresh_preid != preid:
            logger.debug(
                "  ➜ [小号播放] 测速样本返回普通上传，已强制刷新 preid 后重试：%s -> %s",
                (preid[:12] + "...") if preid else "-",
                fresh_preid[:12] + "...",
            )
            preid = fresh_preid
            payload["preid"] = preid
            resp = client.rapid_upload(payload)
    if isinstance(resp, dict) and resp.get("_rapid_sign_required"):
        main_client = P115Service.get_client()
        if not main_client:
            raise RuntimeError("测速秒传需要主号签名，但主号 115 客户端未初始化")
        sign = main_client.rapid_sign_value({
            "sha1": sha1,
            "pick_code": sample_pick_code,
            "sign_check": resp.get("_rapid_sign_check"),
            "size": size,
            "file_name": file_name,
            "user_agent": user_agent,
        })
        payload["sign_key"] = resp.get("_rapid_sign_key")
        payload["sign_val"] = sign.get("sign_val")
        resp = client.rapid_upload(payload)
    if not isinstance(resp, dict) or not resp.get("state"):
        raise RuntimeError(f"测速样本秒传失败: {(resp or {}).get('error_msg') or resp}")
    clone = _extract_clone_from_rapid_response(resp, client, temp_cid, file_name, sha1, size)
    if not clone.get("pick_code"):
        raise RuntimeError("测速样本秒传成功但未找到临时文件")
    try:
        url = client.download_url(clone["pick_code"], user_agent=user_agent or "Mozilla/5.0")
        if not url:
            raise RuntimeError("小号获取测速直链失败")
        result = _speedtest_url(url, user_agent=user_agent or "Mozilla/5.0")
        _mark_account(account_id, {"last_speed_bps": result["bps"], "last_speed_at": _now_text(), "last_error": ""})
        return result
    finally:
        if clone.get("fid"):
            try:
                client.fs_delete([clone["fid"]])
            except Exception:
                pass
