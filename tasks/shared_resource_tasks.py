# tasks/shared_resource_tasks.py
# Rapid v2 共享资源任务：登记本地媒体库索引、长轮询消费中心事件。
import hashlib
import json
import logging
import math
import re
import threading
import time
from contextlib import contextmanager
from typing import Any, Dict, List

import requests
import task_manager
import config_manager
import constants
import utils
from database import shared_credit_db, shared_share_db, settings_db
from database.connection import get_db_connection
from handler.shared_center_client import SharedCenterClient, shared_center_enabled, _current_server_id_hash
from handler import shared_subscription_service as shared_subscription_service
from handler.shared_subscription_service import poll_and_consume_once as _raw_poll_and_consume_once
from handler import tmdb as tmdb_handler
from tasks.helpers import extract_quality_source_from_filename, normalize_quality_source

logger = logging.getLogger(__name__)

_LISTENER_THREAD = None
_SIGN_LISTENER_THREAD = None
_LISTENER_STOP = threading.Event()
_LISTENER_LOCK = threading.Lock()
_FULL_SHARE_LOCK = threading.Lock()
_CENTER_CONFIG_WARNED_REASONS = set()
_LISTENER_FAILURE_WARN_THRESHOLD = 3
_LISTENER_BACKOFF_MAX_SECONDS = 300


def _cfg_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in ('1', 'true', 'yes', 'y', 'on', '启用', '开启'):
        return True
    if text in ('0', 'false', 'no', 'n', 'off', '停用', '关闭'):
        return False
    return bool(default)


def _shared_resource_switch_enabled() -> bool:
    try:
        cfg = settings_db.get_shared_resource_config() or {}
        return _cfg_bool(cfg.get('p115_shared_resource_enabled'), False)
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 读取共享资源总开关失败，按未启用处理: {e}")
        return False


def _shared_center_runtime_config() -> Dict[str, str]:
    """读取共享中心运行必要配置。

    中心端身份只认 Emby ServerID；长轮询/签名轮询启动前只需要确认中心 URL
    和本机可读取 ServerID。
    """
    try:
        cfg = settings_db.get_shared_resource_config() or {}
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 读取共享中心配置失败，按未配置处理: {e}")
        return {'center_url': '', 'server_id_hash': ''}
    return {
        'center_url': str(cfg.get('p115_shared_center_url') or '').strip().rstrip('/'),
        'server_id_hash': _current_server_id_hash(),
    }


def _shared_center_runtime_ready(*, log_missing: bool = False) -> bool:
    if not _shared_resource_switch_enabled():
        return False
    cfg = _shared_center_runtime_config()
    missing = []
    if not cfg.get('center_url'):
        missing.append('center_url')
    if not cfg.get('server_id_hash'):
        missing.append('server_id')
    if missing:
        reason = 'missing_' + '_'.join(missing)
        if log_missing and reason not in _CENTER_CONFIG_WARNED_REASONS:
            _CENTER_CONFIG_WARNED_REASONS.add(reason)
            logger.warning(
                "  ➜ [共享资源] 共享中心配置不完整，跳过启动长轮询/签名监听：%s",
                ', '.join(missing),
            )
        return False
    try:
        return bool(shared_center_enabled())
    except Exception as e:
        reason = 'shared_center_enabled_error'
        if log_missing and reason not in _CENTER_CONFIG_WARNED_REASONS:
            _CENTER_CONFIG_WARNED_REASONS.add(reason)
            logger.warning(f"  ➜ [共享资源] 检查共享中心启用状态失败，跳过长轮询启动: {e}")
        return False


def _enabled() -> bool:
    return _shared_center_runtime_ready(log_missing=False)


def _safe_int(value, default=0):
    try:
        if value in (None, ''):
            return default
        return int(float(value))
    except Exception:
        return default


def _file_size_from_cache(file_info: Dict[str, Any]) -> int:
    """登记源前补齐 size；中心端秒传必须依赖 size。"""
    size = _safe_int(file_info.get('size') or file_info.get('file_size') or file_info.get('size_bytes') or file_info.get('fileSize'), 0)
    if size > 0:
        return size
    sha1 = _norm_sha1(file_info.get('sha1'))
    pc = str(file_info.get('pick_code') or file_info.get('pc') or '').strip()
    try:
        from handler.p115_service import P115CacheManager
        row = None
        if sha1 and hasattr(P115CacheManager, 'get_file_cache_by_sha1'):
            row = P115CacheManager.get_file_cache_by_sha1(sha1)
        if not row and pc and hasattr(P115CacheManager, 'get_file_cache_by_pickcode'):
            row = P115CacheManager.get_file_cache_by_pickcode(pc)
        if row:
            row = dict(row)
            size = _safe_int(row.get('size'), 0)
            if size > 0:
                file_info['size'] = size
                if not file_info.get('file_name') and row.get('name'):
                    file_info['file_name'] = row.get('name')
                return size
    except Exception:
        pass
    try:
        clauses, args = [], []
        if sha1:
            clauses.append('UPPER(sha1)=%s')
            args.append(sha1)
        if pc:
            clauses.append('pick_code=%s')
            args.append(pc)
        if clauses:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT size, name FROM p115_filesystem_cache WHERE {' OR '.join(clauses)} ORDER BY CASE WHEN COALESCE(size,0)>0 THEN 0 ELSE 1 END LIMIT 1", args)
                    row = cur.fetchone()
                    if row:
                        row = dict(row)
                        size = _safe_int(row.get('size'), 0)
                        if size > 0:
                            file_info['size'] = size
                            if not file_info.get('file_name') and row.get('name'):
                                file_info['file_name'] = row.get('name')
                            return size
    except Exception:
        pass
    return 0


def _safe_int_or_none(value):
    try:
        if value in (None, ''):
            return None
        return int(float(value))
    except Exception:
        return None


def _norm_sha1(value: str) -> str:
    text = str(value or '').strip().upper()
    return text if re.fullmatch(r'[A-F0-9]{40}', text) else ''


def _norm_preid(value: str) -> str:
    return _norm_sha1(value)


def _json_object(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _row_has_adult_rating(row: Dict[str, Any]) -> bool:
    row = dict(row or {})
    if row.get('adult') is True or str(row.get('adult') or '').strip().lower() == 'true':
        return True
    if str(row.get('custom_rating') or '').strip().upper() == 'XXX':
        return True
    ratings = _json_object(row.get('official_rating_json'))
    if str(ratings.get('US') or ratings.get('us') or '').strip().upper() == 'XXX':
        return True
    return str(row.get('official_rating') or row.get('mpaa') or row.get('certification') or '').strip().upper() == 'XXX'


def _adult_rating_rows(candidate: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = [dict(candidate or {})]
    raw = _json_object((candidate or {}).get('raw_json'))
    if raw:
        rows.append(raw)
        for key in ('candidate', 'media_row', 'source', 'shared_source'):
            nested = raw.get(key)
            if isinstance(nested, dict):
                rows.append(nested)
    return rows


def _adult_rating_block_reason(candidate: Dict[str, Any]) -> str:
    candidate = dict(candidate or {})
    for row in _adult_rating_rows(candidate):
        if _row_has_adult_rating(row):
            title = row.get('title') or candidate.get('title') or row.get('tmdb_id') or candidate.get('tmdb_id') or ''
            return f'adult rating XXX: {title}'.strip()

    item_type = str(candidate.get('item_type') or '').strip()
    season = _safe_int_or_none(candidate.get('season_number'))
    episode = _safe_int_or_none(candidate.get('episode_number'))
    parent_tmdb_id = str(candidate.get('parent_series_tmdb_id') or candidate.get('series_tmdb_id') or candidate.get('tmdb_id') or '').strip()
    movie_tmdb_id = str(candidate.get('tmdb_id') or '').strip()
    if item_type not in ('Movie', 'Season', 'Episode'):
        return ''

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT title, tmdb_id
                    FROM media_metadata
                    WHERE (
                            (%s='Movie' AND item_type='Movie' AND tmdb_id=%s)
                         OR (%s<>'Movie' AND (
                                (item_type='Series' AND tmdb_id=%s)
                             OR (item_type IN ('Season','Episode')
                                 AND COALESCE(NULLIF(parent_series_tmdb_id, ''), tmdb_id)=%s
                                 AND (%s IS NULL OR season_number=%s)
                                 AND (%s IS NULL OR item_type<>'Episode' OR episode_number=%s))
                            ))
                    )
                      AND (
                            UPPER(COALESCE(NULLIF(custom_rating, ''), ''))='XXX'
                         OR UPPER(COALESCE(official_rating_json->>'US', official_rating_json->>'us', ''))='XXX'
                      )
                    LIMIT 1
                    """,
                    (item_type, movie_tmdb_id, item_type, parent_tmdb_id, parent_tmdb_id, season, season, episode, episode),
                )
                row = dict(cur.fetchone() or {})
                if row:
                    title = row.get('title') or candidate.get('title') or row.get('tmdb_id') or parent_tmdb_id or movie_tmdb_id
                    return f'adult rating XXX: {title}'.strip()
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 成人分级登记前检查失败，按非成人继续: {candidate.get('title') or candidate.get('tmdb_id')} -> {e}")
    return ''


def _adult_block_result(candidate: Dict[str, Any], reason: str) -> Dict[str, Any]:
    title = (candidate or {}).get('title') or (candidate or {}).get('tmdb_id') or 'unknown'
    return {
        'ok': False,
        'message': f'成人资源不参与共享登记：{title}',
        'adult_blocked': True,
        'reason': reason,
        'registered_count': 0,
        'raw_uploaded_count': 0,
        'raw_ready_count': 0,
        'raw_skipped_existing': 0,
        'errors': [],
        'fingerprint_repair': {},
    }


def _rapid_size_to_int(value, default=0) -> int:
    """把 size / file_size / 27.9 GB 这类值统一转成字节数。"""
    try:
        if value in (None, '', [], {}):
            return default
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return int(value)
        text = str(value).strip().replace(',', '')
        if not text:
            return default
        if re.fullmatch(r'\d+(?:\.0+)?', text):
            return int(float(text))
        match = re.search(r'([0-9]+(?:\.[0-9]+)?)\s*(TB|T|GB|G|MB|M|KB|K|B)?', text, re.I)
        if not match:
            return default
        number = float(match.group(1))
        unit = (match.group(2) or 'B').upper()
        if unit in ('TB', 'T'):
            number *= 1024 ** 4
        elif unit in ('GB', 'G'):
            number *= 1024 ** 3
        elif unit in ('MB', 'M'):
            number *= 1024 ** 2
        elif unit in ('KB', 'K'):
            number *= 1024
        return int(number)
    except Exception:
        return default


def _extract_p115_down_url(resp: Any) -> str:
    if isinstance(resp, str):
        return resp
    if not isinstance(resp, dict):
        return ''
    data = resp.get('data')
    if isinstance(data, dict):
        for item in data.values():
            if not isinstance(item, dict):
                continue
            url_obj = item.get('url')
            if isinstance(url_obj, dict) and url_obj.get('url'):
                return str(url_obj.get('url'))
            if isinstance(url_obj, str) and url_obj:
                return url_obj
            for key in ('downurl', 'download_url', 'url'):
                if isinstance(item.get(key), str) and item.get(key):
                    return str(item.get(key))
    for key in ('url', 'downurl', 'download_url'):
        if isinstance(resp.get(key), str) and resp.get(key):
            return str(resp.get(key))
    return ''


def _p115_range_bytes_by_pick_code(pick_code: str, start: int, end: int) -> bytes:
    """按用户配置的 115 API 优先级读取文件 Range。

    这里是为了计算 preid（文件前 128KB SHA1）。它只是读取源文件直链，
    不属于 upload/init 秒传调度，所以必须尊重 p115_api_priority：
    - cookie 优先：先 Cookie download_url，再 OpenAPI downurl；
    - openapi 优先：先 OpenAPI downurl，再 Cookie download_url。

    取直链和 Range GET 必须使用同一个 User-Agent，否则 115 可能返回 403。
    """
    pick_code = str(pick_code or '').strip()
    if not pick_code:
        return b''

    try:
        from handler.p115_service import P115Service, get_115_api_priority, get_115_tokens, get_115_ua
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 导入 115 客户端失败，无法计算 preid: {e}")
        return b''

    try:
        p115 = P115Service.get_client()
        if not p115:
            return b''

        priority = get_115_api_priority()
        try:
            _, _, _, app_type = get_115_tokens()
        except Exception:
            app_type = 'web'

        ua_candidates = []
        for ua in (
            get_115_ua(app_type or 'web'),
            get_115_ua('web'),
            get_115_ua('mac'),
        ):
            ua = str(ua or '').strip()
            if ua and ua not in ua_candidates:
                ua_candidates.append(ua)
        if not ua_candidates:
            ua_candidates.append('Mozilla/5.0')

        if priority == 'cookie':
            method_order = [('download_url', 'Cookie'), ('openapi_downurl', 'OpenAPI')]
        else:
            method_order = [('openapi_downurl', 'OpenAPI'), ('download_url', 'Cookie')]

        range_header = f'bytes={int(start)}-{int(end)}'
        last_status = None

        for method_name, label in method_order:
            method = getattr(p115, method_name, None)
            if not callable(method):
                continue

            for ua in ua_candidates:
                down_url = ''
                try:
                    # 关键：获取直链和后续 Range GET 必须使用同一个 UA。
                    down_url = _extract_p115_down_url(method(pick_code, user_agent=ua))
                except TypeError:
                    try:
                        down_url = _extract_p115_down_url(method(pick_code, ua))
                    except Exception as e:
                        logger.debug(f"  ➜ [共享资源] 获取 115 直链失败({label}, positional-ua): {e}")
                except Exception as e:
                    logger.debug(f"  ➜ [共享资源] 获取 115 直链失败({label}): {e}")

                if not down_url:
                    continue

                try:
                    headers = {
                        'Range': range_header,
                        'User-Agent': ua,
                        'Accept': '*/*',
                        'Connection': 'close',
                    }
                    r = requests.get(down_url, headers=headers, timeout=45, allow_redirects=True)
                    last_status = r.status_code
                    if r.status_code == 206 and r.content:
                        logger.debug(
                            f"  ➜ [共享资源] preid Range 读取成功: api={label}, "
                            f"range={range_header}, bytes={len(r.content)}, pc={pick_code[:8]}..."
                        )
                        return r.content or b''

                    # 必须拒绝 200，避免服务端忽略 Range 后拉完整大文件。
                    logger.warning(
                        f"  ➜ [共享资源] 读取 preid Range 失败: api={label}, "
                        f"HTTP={r.status_code}, range={range_header}, pc={pick_code[:8]}..."
                    )
                except Exception as e:
                    logger.debug(
                        f"  ➜ [共享资源] Range GET 异常: api={label}, "
                        f"range={range_header}, pc={pick_code[:8]}..., err={e}"
                    )

        if last_status:
            logger.warning(
                f"  ➜ [共享资源] 已按 {priority} 优先级尝试读取 preid Range，仍失败，"
                f"最后 HTTP={last_status}: pc={pick_code[:8]}..."
            )
        return b''

    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 读取 115 文件 Range 计算 preid 失败: pc={pick_code[:8]}..., err={e}")
        return b''


def _save_preid_to_mediainfo_cache(file_info: Dict[str, Any], preid: str) -> None:
    preid = _norm_preid(preid)
    sha1 = _norm_sha1(file_info.get('sha1'))
    if not preid or not sha1:
        return
    try:
        from psycopg2.extras import Json
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT raw_ffprobe_json FROM p115_mediainfo_cache WHERE sha1=%s", (sha1,))
                row = cur.fetchone()
                raw = (row or {}).get('raw_ffprobe_json') if row else None
                if isinstance(raw, str):
                    try:
                        raw = json.loads(raw)
                    except Exception:
                        raw = {}
                if not isinstance(raw, dict):
                    raw = {}
                etk = raw.get('_etk') if isinstance(raw.get('_etk'), dict) else {}
                etk = dict(etk or {})
                etk.setdefault('sha1', sha1)
                etk['preid'] = preid
                raw['_etk'] = etk
                cur.execute(
                    """
                    INSERT INTO p115_mediainfo_cache (sha1, raw_ffprobe_json, created_at, hit_count)
                    VALUES (%s, %s, NOW(), 0)
                    ON CONFLICT (sha1)
                    DO UPDATE SET raw_ffprobe_json = EXCLUDED.raw_ffprobe_json
                    """,
                    (sha1, Json(raw, dumps=lambda obj: json.dumps(obj, ensure_ascii=False))),
                )
            conn.commit()
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 回写 p115_mediainfo_cache RAW preid 失败: {e}")


def _lookup_preid_from_raw_cache(file_info: Dict[str, Any]) -> str:
    meta = file_info.get('rapid_meta_json') if isinstance(file_info.get('rapid_meta_json'), dict) else {}
    preid = _norm_preid(file_info.get('preid') or meta.get('preid') or meta.get('pre_sha1') or meta.get('pre_sha1_128k'))
    if preid:
        return preid
    raw = file_info.get('raw_ffprobe_json') if isinstance(file_info.get('raw_ffprobe_json'), dict) else file_info.get('raw')
    etk = raw.get('_etk') if isinstance(raw, dict) and isinstance(raw.get('_etk'), dict) else {}
    preid = _norm_preid(etk.get('preid') or etk.get('pre_sha1') or etk.get('pre_sha1_128k'))
    if preid:
        _save_preid_to_mediainfo_cache(file_info, preid)
        return preid
    sha1 = _norm_sha1(file_info.get('sha1'))
    if not sha1:
        return ''
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT raw_ffprobe_json
                    FROM p115_mediainfo_cache
                    WHERE sha1=%s
                    LIMIT 1
                    """,
                    (sha1,),
                )
                row = cur.fetchone()
                raw = (row or {}).get('raw_ffprobe_json') if row else None
                if isinstance(raw, str):
                    raw = json.loads(raw)
                etk = raw.get('_etk') if isinstance(raw, dict) and isinstance(raw.get('_etk'), dict) else {}
                return _norm_preid(etk.get('preid') or etk.get('pre_sha1') or etk.get('pre_sha1_128k'))
    except Exception:
        return ''


def _ensure_file_preid(file_info: Dict[str, Any]) -> str:
    """确保单个 115 文件拥有 preid。

    preid = 文件前 128KB SHA1，是 115 upload/init 的基础秒传参数。
    只读取 128KB，不读取完整文件；计算结果写回 p115_mediainfo_cache.raw_ffprobe_json._etk。
    """
    if not isinstance(file_info, dict):
        return ''
    preid = _lookup_preid_from_raw_cache(file_info)
    if not preid:
        pc = str(file_info.get('pick_code') or file_info.get('pc') or '').strip()
        chunk = _p115_range_bytes_by_pick_code(pc, 0, 131071)
        if chunk:
            preid = hashlib.sha1(chunk).hexdigest().upper()
            _save_preid_to_mediainfo_cache(file_info, preid)
            logger.info(f"  ➜ [共享资源] 已缓存秒传校验片段：{file_info.get('file_name') or file_info.get('name') or file_info.get('sha1')}")
    if preid:
        file_info['preid'] = preid
        meta = file_info.get('rapid_meta_json') if isinstance(file_info.get('rapid_meta_json'), dict) else {}
        meta = dict(meta or {})
        meta.setdefault('preid', preid)
        file_info['rapid_meta_json'] = meta
    return preid


def _json_obj(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            obj = json.loads(value)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}


def _center_nested_parts_for_gate(source: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if isinstance(source, dict):
        out.append(source)
        for key in ('version_summary', 'summary_json', 'media_signature_json', 'raw_summary_json'):
            value = _json_obj(source.get(key))
            if value:
                out.append(value)
        for key in ('versions', 'children', 'pack_items'):
            value = source.get(key)
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        out.append(item)
                        for sub_key in ('version_summary', 'summary_json', 'media_signature_json'):
                            sub = _json_obj(item.get(sub_key))
                            if sub:
                                out.append(sub)
    return out


def _source_is_tv_for_runtime_tags(source_info: Dict[str, Any]) -> bool:
    """短剧/纯净版只对电视剧资源生效。

    Movie 即使物理时长低于 25 分钟，也不能被打短剧/纯净版标签；
    中心消费门禁同样只在明确是 Series/Season/Episode/season_hub 时拦截。
    """
    if not isinstance(source_info, dict):
        return False

    parts = _center_nested_parts_for_gate(source_info)
    for part in parts:
        if not isinstance(part, dict):
            continue

        raw = part.get('raw_ffprobe_json') if isinstance(part.get('raw_ffprobe_json'), dict) else part
        etk = raw.get('_etk') if isinstance(raw, dict) and isinstance(raw.get('_etk'), dict) else {}

        item_type = str(
            part.get('item_type')
            or part.get('share_item_type')
            or part.get('media_type')
            or part.get('type')
            or etk.get('item_type')
            or etk.get('type')
            or ''
        ).strip().lower()
        source_kind = str(part.get('source_kind') or part.get('kind') or '').strip().lower()
        source_id = str(part.get('source_id') or part.get('center_source_id') or '').strip().lower()

        if item_type in {'series', 'season', 'episode', 'tv'}:
            return True
        if source_kind in {'season', 'episode', 'episode_group', 'completed_season', 'season_hub'}:
            return True
        if source_id.startswith(('season_hub:', 'episode:', 'completed_season:')):
            return True
        if part.get('season_number') not in (None, '') or part.get('episode_number') not in (None, ''):
            return True
        if part.get('parent_series_tmdb_id') or part.get('series_tmdb_id'):
            return True

    return False


def _center_flag_meta_for_gate(source: Dict[str, Any], flag_key: str, meta_key: str) -> Dict[str, Any]:
    for part in _center_nested_parts_for_gate(source):
        meta = _json_obj(part.get(meta_key))
        if _cfg_bool(part.get(flag_key), False) or _cfg_bool(meta.get(flag_key), False):
            meta.setdefault(flag_key, True)
            return meta
    return {}


def _shared_transfer_gate(source: Dict[str, Any]) -> Dict[str, Any]:
    cfg = settings_db.get_shared_resource_config() or {}
    title = str((source or {}).get('title') or (source or {}).get('file_name') or (source or {}).get('source_id') or '').strip() or '该资源'
    is_tv_resource = _source_is_tv_for_runtime_tags(source or {})
    if is_tv_resource and _cfg_bool(cfg.get('p115_shared_block_clean_version_transfer'), False):
        if _center_flag_meta_for_gate(source or {}, 'is_clean_version', 'clean_version_meta_json'):
            return {'ok': False, 'reason': 'blocked_clean_version', 'message': f'已开启“不秒传纯净版”，跳过《{title}》。'}
    if is_tv_resource and _cfg_bool(cfg.get('p115_shared_block_short_drama_transfer'), False):
        if _center_flag_meta_for_gate(source or {}, 'is_short_drama', 'short_drama_meta_json'):
            return {'ok': False, 'reason': 'blocked_short_drama', 'message': f'已开启“不秒传短剧”，跳过《{title}》。'}
    return {'ok': True}


def _event_source_payload(event: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(event, dict):
        return {}
    payload = event.get('payload_json') if isinstance(event.get('payload_json'), dict) else {}
    if payload:
        return payload
    return event


# 只用于识别并拒绝中心端遗留事件；客户端不再实现 completed_season 创建分享。
COMPLETED_SEASON_SHARE_CREATE_EVENT_TYPE = 'create_completed_season_share'
LOGICAL_SEASON_SHARE_CREATE_EVENT_TYPE = 'create_logical_season_filelist_share'
PRO_QUOTA_AUTH_EVENT_TYPE = 'pro_quota_auth_check'
_COMPLETED_SHARE_SYNC_LOCK = threading.Lock()
_COMPLETED_SHARE_DELETE_RETRY_SECONDS = 300
_COMPLETED_SHARE_DELETE_ATTEMPTS: Dict[str, float] = {}


def _completed_share_event_type(event: Dict[str, Any]) -> str:
    payload = _event_source_payload(event)
    return str((event or {}).get('event_type') or payload.get('event_type') or payload.get('command') or '').strip()


def _completed_share_receive_code(channel_id: str, source_id: str = '') -> str:
    seed = f"{channel_id or ''}:{source_id or ''}:{int(time.time() // 86400)}"
    return hashlib.sha1(seed.encode('utf-8')).hexdigest()[:4]


def _p115_ok(resp: Any) -> bool:
    if not isinstance(resp, dict):
        return False
    state = resp.get('state')
    if state is True or state == 1 or state == '1' or str(state).lower() == 'true':
        return True
    if resp.get('errno') in (0, '0') and not resp.get('error'):
        return True
    if resp.get('code') in (0, 200, '0', '200') and not (resp.get('error') or resp.get('error_msg') or resp.get('message')):
        return True
    return False


def _p115_error(resp: Any) -> str:
    if isinstance(resp, dict):
        return str(resp.get('error_msg') or resp.get('error') or resp.get('message') or resp.get('msg') or resp)[:2000]
    return str(resp)[:2000]


def _completed_share_resp_text(resp: Any) -> str:
    try:
        return json.dumps(resp, ensure_ascii=False, default=str)
    except Exception:
        return str(resp or '')


def _completed_share_delete_ok(resp: Any) -> bool:
    if _p115_ok(resp):
        return True
    text = _completed_share_resp_text(resp).lower()
    return any(x in text for x in (
        '已删除', '删除成功', '已取消', '取消成功', '不存在', '失效', '过期',
        'not found', 'deleted', 'delete success', 'cancelled', 'canceled', 'expired', 'success'
    ))


def _delete_completed_share_from_115(p115, share_code: str) -> Dict[str, Any]:
    """删除 ETK 托管的 115 分享记录；不要只 cancel 留垃圾记录。"""
    share_code = str(share_code or '').strip()
    if not share_code:
        return {'state': True, 'skipped': True, 'message': '无 share_code，无需删除'}
    attempts = []
    delete_method = getattr(p115, 'share_delete', None)
    cancel_method = getattr(p115, 'share_cancel', None)

    if callable(delete_method):
        try:
            resp = delete_method(share_code)
            attempts.append({'method': 'share_delete', 'response': resp})
            if _completed_share_delete_ok(resp):
                return {'state': True, 'deleted': True, 'method': 'share_delete', 'attempts': attempts}
        except Exception as e:
            attempts.append({'method': 'share_delete', 'error': str(e)})

    if callable(cancel_method):
        try:
            resp = cancel_method(share_code)
            attempts.append({'method': 'share_cancel', 'response': resp})
        except Exception as e:
            attempts.append({'method': 'share_cancel', 'error': str(e)})

    if callable(delete_method):
        try:
            resp = delete_method(share_code)
            attempts.append({'method': 'share_delete_after_cancel', 'response': resp})
            if _completed_share_delete_ok(resp):
                return {'state': True, 'deleted': True, 'method': 'share_delete_after_cancel', 'attempts': attempts}
        except Exception as e:
            attempts.append({'method': 'share_delete_after_cancel', 'error': str(e)})

    cancel_ok = any(_completed_share_delete_ok(a.get('response')) for a in attempts if a.get('method') == 'share_cancel')
    return {'state': bool(cancel_ok), 'deleted': False, 'cancelled_only': bool(cancel_ok), 'attempts': attempts}


def _delete_completed_share_channels_for_source(row: Dict[str, Any], reason: str, *, client: SharedCenterClient = None) -> Dict[str, Any]:
    row = dict(row or {})
    local_id = int(row.get('id') or 0)
    center_source_id = str(row.get('center_source_id') or '').strip()
    channels = shared_share_db.list_completed_season_share_channels_by_source(
        local_source_id=local_id,
        center_source_id=center_source_id,
    )
    if not channels:
        return {'ok': True, 'checked': 0, 'deleted_115': 0, 'deleted_local': 0, 'kept_audit': 0, 'items': []}

    p115 = None
    deleted_115 = 0
    deleted_local = 0
    kept_audit = 0
    failed = 0
    items = []
    for channel in channels:
        channel_id = str(channel.get('channel_id') or '').strip()
        share_code = str(channel.get('share_code') or '').strip()
        item = {
            'channel_id': channel_id,
            'center_source_id': channel.get('center_source_id'),
            'share_code': share_code,
        }
        if not channel_id:
            continue
        if not share_code:
            if _keep_missing_share_code_channel(channel):
                kept_audit += 1
                item.update({'ok': True, 'kept_audit': True, 'reason': 'missing_share_code_audit'})
                items.append(item)
                continue
            deleted = shared_share_db.delete_completed_season_share_channel(channel_id)
            deleted_local += 1 if deleted else 0
            item.update({'ok': True, 'deleted_local_channel': bool(deleted), 'reason': 'missing_share_code_no_115_share'})
            items.append(item)
            continue

        if p115 is None:
            try:
                from handler.p115_service import P115Service
                p115 = P115Service.get_client()
            except Exception as e:
                p115 = None
                item.update({'ok': False, 'error': f'115 客户端初始化失败: {e}'})
                items.append(item)
                failed += 1
                continue
        if not p115:
            item.update({'ok': False, 'error': '115 客户端未初始化，无法删除旧版分享'})
            items.append(item)
            failed += 1
            continue

        now_ts = time.time()
        last_attempt = float(_COMPLETED_SHARE_DELETE_ATTEMPTS.get(share_code) or 0)
        if last_attempt and now_ts - last_attempt < _COMPLETED_SHARE_DELETE_RETRY_SECONDS:
            wait_seconds = int(_COMPLETED_SHARE_DELETE_RETRY_SECONDS - (now_ts - last_attempt))
            item.update({'ok': False, 'error': f'115 分享删除刚失败过，{wait_seconds}s 后再重试', 'retry_after': wait_seconds})
            items.append(item)
            failed += 1
            continue

        delete_resp = _delete_completed_share_from_115(p115, share_code)
        item['share_delete_response'] = delete_resp
        if delete_resp.get('state') is False:
            _COMPLETED_SHARE_DELETE_ATTEMPTS[share_code] = now_ts
            msg = f'删除旧版共享源前删除 115 分享失败: {delete_resp}'
            try:
                shared_share_db.update_completed_season_share_channel(
                    channel_id,
                    status_message=msg[:1000],
                    raw_json={'share_delete_response': delete_resp, 'cleanup_reason': reason},
                    last_checked_at='NOW()',
                )
            except Exception:
                pass
            item.update({'ok': False, 'error': msg})
            items.append(item)
            failed += 1
            continue

        _COMPLETED_SHARE_DELETE_ATTEMPTS.pop(share_code, None)
        deleted_115 += 1 if delete_resp.get('deleted') or delete_resp.get('cancelled_only') or delete_resp.get('skipped') else 0
        center_resp = {}
        try:
            client = client or SharedCenterClient()
            center_resp = _update_center_share_channel_status(client, channel, channel_id, {
                'status': 'disabled',
                'review_status': 'disabled',
                'status_message': reason,
                'raw_json': {'share_delete_response': delete_resp, 'cleanup_reason': reason},
            })
        except Exception as e:
            center_resp = {'ok': False, 'error': str(e)}
        if isinstance(center_resp, dict) and center_resp.get('ok') is False:
            item.update({'ok': False, 'error': f'中心分享通道状态同步失败: {center_resp}', 'center': center_resp})
            items.append(item)
            failed += 1
            continue

        deleted = shared_share_db.delete_completed_season_share_channel(channel_id)
        deleted_local += 1 if deleted else 0
        item.update({'ok': True, 'deleted_115_share': True, 'deleted_local_channel': bool(deleted), 'center': center_resp})
        items.append(item)

    return {
        'ok': failed == 0,
        'checked': len(channels),
        'deleted_115': deleted_115,
        'deleted_local': deleted_local,
        'kept_audit': kept_audit,
        'failed': failed,
        'items': items,
    }


def _delete_logical_share_channels_for_fids(fid_list: List[str], reason: str, *, client: SharedCenterClient = None) -> Dict[str, Any]:
    fid_set = {str(x).strip() for x in (fid_list or []) if str(x).strip()}
    if not fid_set:
        return {'ok': True, 'checked': 0, 'deleted_115': 0, 'deleted_local': 0, 'failed': 0, 'items': []}
    statuses = ['valid', 'pending_review', 'creating']
    channels = shared_share_db.list_completed_season_share_channels(statuses=statuses, limit=1000, need_check=False)
    matched = []
    for channel in channels or []:
        if not _share_channel_is_logical(channel):
            continue
        share_ids = _logical_share_row_file_ids(channel)
        hit_fids = sorted(fid_set.intersection({str(x).strip() for x in share_ids if str(x).strip()}))
        if hit_fids:
            matched.append((channel, hit_fids))
    if not matched:
        return {'ok': True, 'checked': 0, 'deleted_115': 0, 'deleted_local': 0, 'failed': 0, 'items': []}

    p115 = None
    deleted_115 = 0
    deleted_local = 0
    failed = 0
    items = []
    for channel, hit_fids in matched:
        channel_id = str(channel.get('channel_id') or '').strip()
        share_code = str(channel.get('share_code') or '').strip()
        item = {
            'channel_id': channel_id,
            'center_source_id': channel.get('center_source_id'),
            'share_code': share_code,
            'matched_fids': hit_fids[:20],
        }
        if not channel_id:
            continue
        if not share_code:
            deleted = shared_share_db.delete_completed_season_share_channel(channel_id)
            deleted_local += 1 if deleted else 0
            item.update({'ok': True, 'deleted_local_channel': bool(deleted), 'reason': 'missing_share_code_no_115_share'})
            items.append(item)
            continue

        if p115 is None:
            try:
                from handler.p115_service import P115Service
                p115 = P115Service.get_client()
            except Exception as e:
                p115 = None
                item.update({'ok': False, 'error': f'115 客户端初始化失败: {e}'})
                items.append(item)
                failed += 1
                continue
        if not p115:
            item.update({'ok': False, 'error': '115 客户端未初始化，无法删除旧版逻辑季分享'})
            items.append(item)
            failed += 1
            continue

        delete_resp = _delete_completed_share_from_115(p115, share_code)
        item['share_delete_response'] = delete_resp
        if delete_resp.get('state') is False:
            msg = f'删除旧版逻辑季分享失败: {delete_resp}'
            try:
                shared_share_db.update_completed_season_share_channel(
                    channel_id,
                    status_message=msg[:1000],
                    raw_json={'share_delete_response': delete_resp, 'cleanup_reason': reason, 'matched_fids': hit_fids[:50]},
                    last_checked_at='NOW()',
                )
            except Exception:
                pass
            item.update({'ok': False, 'error': msg})
            items.append(item)
            failed += 1
            continue

        deleted_115 += 1 if delete_resp.get('deleted') or delete_resp.get('cancelled_only') or delete_resp.get('skipped') else 0
        center_resp = {}
        try:
            client = client or SharedCenterClient()
            center_resp = _update_center_share_channel_status(client, channel, channel_id, {
                'status': 'disabled',
                'review_status': 'disabled',
                'status_message': reason,
                'raw_json': {'share_delete_response': delete_resp, 'cleanup_reason': reason, 'matched_fids': hit_fids[:50]},
            })
        except Exception as e:
            center_resp = {'ok': False, 'error': str(e)}
        if isinstance(center_resp, dict) and center_resp.get('ok') is False:
            item.update({'ok': False, 'error': f'中心逻辑季分享通道状态同步失败: {center_resp}', 'center': center_resp})
            items.append(item)
            failed += 1
            continue

        deleted = shared_share_db.delete_completed_season_share_channel(channel_id)
        deleted_local += 1 if deleted else 0
        item.update({'ok': True, 'deleted_115_share': True, 'deleted_local_channel': bool(deleted), 'center': center_resp})
        items.append(item)

    return {
        'ok': failed == 0,
        'checked': len(matched),
        'deleted_115': deleted_115,
        'deleted_local': deleted_local,
        'failed': failed,
        'items': items,
    }


def delete_logical_share_channels_from_center_rows(channels: List[Dict[str, Any]], reason: str, *, client: SharedCenterClient = None) -> Dict[str, Any]:
    """Delete local 115 shares for logical channels returned by center scope-disable."""
    seen = set()
    rows: List[Dict[str, Any]] = []
    for channel in channels or []:
        if not isinstance(channel, dict):
            continue
        channel_id = str(channel.get('channel_id') or '').strip()
        share_code = str(channel.get('share_code') or '').strip()
        if not channel_id or not share_code or channel_id in seen:
            continue
        seen.add(channel_id)
        rows.append(channel)
    if not rows:
        return {'ok': True, 'checked': 0, 'deleted_115': 0, 'deleted_local': 0, 'failed': 0, 'items': []}

    p115 = None
    deleted_115 = 0
    deleted_local = 0
    failed = 0
    items = []
    reason = str(reason or 'center_scope_disabled').strip() or 'center_scope_disabled'
    for channel in rows:
        channel_id = str(channel.get('channel_id') or '').strip()
        share_code = str(channel.get('share_code') or '').strip()
        group_id = str(channel.get('group_id') or '').strip()
        item = {'channel_id': channel_id, 'center_source_id': group_id, 'share_code': share_code}

        if p115 is None:
            try:
                from handler.p115_service import P115Service
                p115 = P115Service.get_client()
            except Exception as e:
                p115 = None
                item.update({'ok': False, 'error': f'115 客户端初始化失败: {e}'})
                items.append(item)
                failed += 1
                continue
        if not p115:
            item.update({'ok': False, 'error': '115 客户端未初始化，无法删除逻辑季分享'})
            items.append(item)
            failed += 1
            continue

        delete_resp = _delete_completed_share_from_115(p115, share_code)
        item['share_delete_response'] = delete_resp
        if delete_resp.get('state') is False:
            msg = f'中心范围下架后删除 115 逻辑季分享失败: {delete_resp}'
            try:
                shared_share_db.update_completed_season_share_channel(
                    channel_id,
                    status_message=msg[:1000],
                    raw_json={'share_delete_response': delete_resp, 'cleanup_reason': reason},
                    last_checked_at='NOW()',
                )
            except Exception:
                pass
            item.update({'ok': False, 'error': msg})
            items.append(item)
            failed += 1
            continue

        deleted_115 += 1 if delete_resp.get('deleted') or delete_resp.get('cancelled_only') or delete_resp.get('skipped') else 0
        local_row = shared_share_db.get_completed_season_share_channel(channel_id)
        row_for_report = local_row or {
            'channel_id': channel_id,
            'center_source_id': group_id,
            'share_code': share_code,
            'share_url': channel.get('share_url') or '',
            'raw_json': {'share_kind': 'logical_season'},
        }
        center_resp = {}
        try:
            client = client or SharedCenterClient()
            center_resp = _update_center_share_channel_status(client, row_for_report, channel_id, {
                'status': 'disabled',
                'review_status': 'admin_deleted',
                'status_message': reason,
                'raw_json': {'share_delete_response': delete_resp, 'cleanup_reason': reason},
            })
        except Exception as e:
            center_resp = {'ok': False, 'error': str(e)}
        if isinstance(center_resp, dict) and center_resp.get('ok') is False:
            item.update({'ok': False, 'error': f'中心逻辑季分享通道状态同步失败: {center_resp}', 'center': center_resp})
            items.append(item)
            failed += 1
            continue

        deleted = shared_share_db.delete_completed_season_share_channel(channel_id)
        deleted_local += 1 if deleted else 0
        item.update({'ok': True, 'deleted_115_share': True, 'deleted_local_channel': bool(deleted), 'center': center_resp})
        items.append(item)

    return {
        'ok': failed == 0,
        'checked': len(rows),
        'deleted_115': deleted_115,
        'deleted_local': deleted_local,
        'failed': failed,
        'items': items,
    }


def _extract_completed_share_payload(resp: Dict[str, Any], *, receive_code: str = '') -> Dict[str, Any]:
    resp = resp if isinstance(resp, dict) else {}
    data = resp.get('data') if isinstance(resp.get('data'), dict) else {}
    candidates = [data, resp]
    share_code = ''
    share_url = ''
    got_receive_code = str(receive_code or '').strip()
    for item in candidates:
        if not isinstance(item, dict):
            continue
        share_code = share_code or str(item.get('share_code') or item.get('shareCode') or item.get('code') or '').strip()
        share_url = share_url or str(item.get('share_url') or item.get('shareUrl') or item.get('url') or '').strip()
        got_receive_code = got_receive_code or str(item.get('receive_code') or item.get('receiveCode') or item.get('receive_code_text') or item.get('pass_code') or '').strip()
    if not share_url and share_code:
        share_url = f'https://115.com/s/{share_code}'
    return {
        'share_code': share_code,
        'receive_code': got_receive_code,
        'share_url': share_url,
        'raw_json': resp,
    }


def _completed_share_status_from_info(resp: Dict[str, Any], *, allow_implicit_valid: bool = True) -> Dict[str, str]:
    """把 115 分享状态响应映射为中心状态。

    注意：115 刚创建分享时，/share/snap 或部分接口可能 state=True，
    但 Web 列表仍显示“处理中”。state=True 只能说明接口请求成功，不能等价于审核通过。
    创建后的首次上报一律不允许仅凭 state=True 判定 valid。

    返回里的 ``explicit`` 表示状态来自 115 明确审核/分享状态字段或状态文案；
    只有这种结果才允许覆盖 share_list 定点对账结果。这样避免把“处理中”的
    分享因为 state=True 误判为可转存。
    """
    text = json.dumps(resp if isinstance(resp, dict) else {'value': str(resp)}, ensure_ascii=False, default=str).lower()
    message = _p115_error(resp)
    if any(x in text for x in ('审核不通过', '审核失败', '审核未通过', '未通过审核', '违规', '违法', '违反', '涉嫌', '涉政', '暴恐', '政治', '恐怖', '风险', '禁止分享', '不允许分享', '不能分享', '封禁', 'risk', 'violation', 'forbidden')):
        return {'status': 'review_failed', 'review_status': 'failed', 'message': message or '115 分享审核失败/违规', 'explicit': True}
    if any(x in text for x in ('审核中', '待审核', '处理中', '正在处理', 'reviewing', 'pending_review', 'pending review', 'processing')):
        return {'status': 'pending_review', 'review_status': 'pending', 'message': message or '115 分享审核中/处理中', 'explicit': True}
    if any(x in text for x in ('不存在', '已取消', '已删除', '取消分享', '取消了分享', '过期', '失效', 'expired', 'not found', 'cancelled', 'canceled', 'deleted')):
        return {'status': 'expired', 'review_status': 'expired', 'message': message or '115 分享已失效', 'explicit': True}
    if any(x in text for x in ('审核通过', '通过审核', '已通过', '分享可用', '可转存', '正常', '已生效', 'approved', 'review_passed', 'passed_review', 'normal', 'available')):
        return {'status': 'valid', 'review_status': 'passed', 'message': '115 分享审核通过', 'explicit': True}
    if allow_implicit_valid and _p115_ok(resp):
        return {'status': 'valid', 'review_status': 'passed', 'message': '115 分享可用', 'explicit': False}
    if _p115_ok(resp):
        return {'status': 'pending_review', 'review_status': 'pending', 'message': '115 分享已创建，等待 115 审核', 'explicit': False}
    return {'status': 'failed', 'review_status': 'unknown', 'message': message or '115 分享状态未知', 'explicit': False}


def _logical_share_provider_forbidden_status(value: Any) -> Dict[str, Any]:
    text = json.dumps(value if isinstance(value, (dict, list)) else {'value': str(value)}, ensure_ascii=False, default=str).lower()
    if any(x in text for x in ('审核不通过', '审核失败', '审核未通过', '未通过审核', '违规', '违法', '违反', '涉嫌', '涉政', '暴恐', '政治', '恐怖', '风险', '禁止分享', '不允许分享', '不能分享', '封禁', 'risk', 'violation', 'forbidden')):
        return {
            'status': 'review_failed',
            'review_status': 'failed',
            'message': _p115_error(value) or '115 判定该账号不可分享此资源',
            'share_forbidden_by_provider': True,
        }
    return {
        'status': 'failed',
        'review_status': 'failed',
        'message': _p115_error(value) or '115 分享创建失败',
        'share_forbidden_by_provider': False,
    }


def _completed_share_list_items(resp: Any) -> List[Dict[str, Any]]:
    """从 115 share_list 响应里提取分享条目列表，兼容不同 Cookie 库返回结构。"""
    out: List[Dict[str, Any]] = []
    seen = set()

    def walk(value: Any, depth: int = 0) -> None:
        if depth > 5:
            return
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    code = str(item.get('share_code') or item.get('shareCode') or item.get('code') or '').strip()
                    marker = code or id(item)
                    if marker not in seen:
                        seen.add(marker)
                        out.append(item)
                elif isinstance(item, (list, tuple, dict)):
                    walk(item, depth + 1)
            return
        if not isinstance(value, dict):
            return
        for key in ('list', 'data', 'items', 'share_list', 'shareList', 'rows', 'result'):
            child = value.get(key)
            if child is not None:
                walk(child, depth + 1)

    walk(resp)
    return out


def _completed_share_code_from_list_item(item: Dict[str, Any]) -> str:
    item = item if isinstance(item, dict) else {}
    return str(
        item.get('share_code')
        or item.get('shareCode')
        or item.get('code')
        or item.get('share_id')
        or item.get('shareId')
        or ''
    ).strip()


def _completed_share_known_list_map(p115, share_codes: List[str], *, max_pages: int = 20) -> Dict[str, Dict[str, Any]]:
    """批量拉取 115 分享列表，并只提取 ETK 本地 channel 已知 share_code。

    这里不会把 115 全量分享列表作为共享池基准，也不会把用户私人分享导入 ETK；
    share_list 只作为“这些已知 share_code 当前在 115 后台显示什么状态”的查询来源。

    重要：同步任务禁止对每个 share_code 再调用 /share/shareinfo。
    share_list 查不到或状态字段不明确时，只保持本地原状态，失效由消费端转存失败兜底下架。
    """
    wanted = {str(x or '').strip() for x in (share_codes or []) if str(x or '').strip()}
    if not wanted or not hasattr(p115, 'share_list'):
        return {}
    found: Dict[str, Dict[str, Any]] = {}
    limit = 100
    # 115 分享列表只能分页；这里按本次需要对账的 share_code 数量动态多翻几页，
    # 仍然有上限，避免把用户私人分享全量扫爆。
    try:
        dynamic_pages = max(1, math.ceil(len(wanted) / limit) + 3)
    except Exception:
        dynamic_pages = 5
    page_limit = max(1, min(int(max_pages or 20), max(dynamic_pages, 5)))
    for page in range(page_limit):
        if not wanted:
            break
        offset = page * limit
        try:
            resp = p115.share_list({'limit': limit, 'offset': offset, 'show_cancel_share': 1, 'order': 'create_time', 'asc': 0})
        except Exception as e:
            logger.debug(f"  ➜ [完结季分享] 批量查询 115 分享列表失败：offset={offset}, err={e}")
            break
        items = _completed_share_list_items(resp)
        if not items:
            break
        for item in items:
            code = _completed_share_code_from_list_item(item)
            if code in wanted:
                found[code] = item
                wanted.discard(code)
        if len(items) < limit:
            break
    logger.debug(
        f"  ➜ [完结季分享] 批量分享列表对账完成：wanted={len(share_codes or [])}, "
        f"matched={len(found)}, pages<={page_limit}"
    )
    return found


def _completed_share_recent_list_items(p115, *, max_pages: int = 20, limit: int = 100) -> List[Dict[str, Any]]:
    if not hasattr(p115, 'share_list'):
        return []
    out: List[Dict[str, Any]] = []
    seen = set()
    page_limit = max(1, min(int(max_pages or 20), 50))
    page_size = max(1, min(int(limit or 100), 100))
    for page in range(page_limit):
        offset = page * page_size
        try:
            resp = p115.share_list({'limit': page_size, 'offset': offset, 'show_cancel_share': 1, 'order': 'create_time', 'asc': 0})
        except Exception as e:
            logger.debug(f"  ➜ [完结季分享] 拉取 115 分享列表失败：offset={offset}, err={e}")
            break
        items = _completed_share_list_items(resp)
        if not items:
            break
        for item in items:
            code = _completed_share_code_from_list_item(item)
            marker = code or id(item)
            if marker in seen:
                continue
            seen.add(marker)
            out.append(item)
        if len(items) < page_size:
            break
    return out


def _completed_share_full_list_scan(p115, *, max_pages: int = 50, limit: int = 100) -> Dict[str, Any]:
    if not hasattr(p115, 'share_list'):
        return {'items': [], 'complete': False, 'pages': 0, 'page_limit': 0}
    out: List[Dict[str, Any]] = []
    seen = set()
    page_limit = max(1, min(int(max_pages or 50), 50))
    page_size = max(1, min(int(limit or 100), 100))
    pages = 0
    complete = False
    for page in range(page_limit):
        offset = page * page_size
        try:
            resp = p115.share_list({'limit': page_size, 'offset': offset, 'show_cancel_share': 1, 'order': 'create_time', 'asc': 0})
        except Exception as e:
            logger.debug(f"  ➜ [完结季分享] 全量拉取 115 分享列表失败：offset={offset}, err={e}")
            break
        items = _completed_share_list_items(resp)
        pages += 1
        if not items:
            complete = True
            break
        for item in items:
            code = _completed_share_code_from_list_item(item)
            marker = code or id(item)
            if marker in seen:
                continue
            seen.add(marker)
            out.append(item)
        if len(items) < page_size:
            complete = True
            break
    return {'items': out, 'complete': complete, 'pages': pages, 'page_limit': page_limit}


def _completed_share_list_item_title(item: Dict[str, Any]) -> str:
    item = item if isinstance(item, dict) else {}
    for key in ('share_title', 'shareTitle', 'file_name', 'fileName', 'name', 'title'):
        text = str(item.get(key) or '').strip()
        if text:
            return text
    data = item.get('data')
    if isinstance(data, dict):
        for key in ('share_title', 'shareTitle', 'file_name', 'fileName', 'name', 'title'):
            text = str(data.get(key) or '').strip()
            if text:
                return text
    return ''


def _completed_share_list_item_receive_code(item: Dict[str, Any]) -> str:
    item = item if isinstance(item, dict) else {}
    for key in ('receive_code', 'receiveCode', 'pass_code', 'passCode', 'extract_code', 'extractCode'):
        text = str(item.get(key) or '').strip()
        if text:
            return text
    return ''


def _completed_share_list_item_file_count(item: Dict[str, Any]) -> int:
    item = item if isinstance(item, dict) else {}
    for key in ('file_count', 'fileCount', 'share_file_count', 'shareFileCount', 'file_num', 'fileNum', 'count', 'cnt', 'total'):
        count = _safe_int(item.get(key), 0)
        if count > 0:
            return count
    title = _completed_share_list_item_title(item)
    match = re.search(r'等\s*(\d+)\s*个文件', title)
    if match:
        return _safe_int(match.group(1), 0)
    return 0


def _completed_share_list_item_total_size(item: Dict[str, Any]) -> int:
    item = item if isinstance(item, dict) else {}
    for key in (
        'total_size', 'totalSize', 'share_size', 'shareSize', 'size',
        'file_size', 'fileSize', 'file_size_bytes', 'fileSizeBytes',
        'bytes', 'total_bytes', 'totalBytes',
    ):
        size = _rapid_size_to_int(item.get(key), 0)
        if size > 0:
            return size
    data = item.get('data')
    if isinstance(data, dict):
        return _completed_share_list_item_total_size(data)
    return 0


def _completed_share_list_item_url(item: Dict[str, Any], share_code: str = '') -> str:
    item = item if isinstance(item, dict) else {}
    for key in ('share_url', 'shareUrl', 'url', 'share_link', 'shareLink'):
        text = str(item.get(key) or '').strip()
        if text:
            return text
    code = str(share_code or _completed_share_code_from_list_item(item) or '').strip()
    return f'https://115.com/s/{code}' if code else ''


def _completed_share_list_item_root_id(item: Dict[str, Any], *keys: str) -> str:
    item = item if isinstance(item, dict) else {}
    for key in keys:
        text = str(item.get(key) or '').strip()
        if text:
            return text
    return ''


def _norm_share_match_text(value: Any) -> str:
    text = str(value or '').lower()
    return re.sub(r'[\s\W_]+', '', text, flags=re.UNICODE)


def _completed_share_row_title_hints(row: Dict[str, Any]) -> List[str]:
    row = row if isinstance(row, dict) else {}
    raw = _share_channel_raw_json(row)
    event = raw.get('event') if isinstance(raw.get('event'), dict) else {}
    hints = [
        row.get('share_title'),
        row.get('root_name'),
        event.get('title'),
        event.get('share_title'),
        event.get('root_name'),
    ]
    out: List[str] = []
    for hint in hints:
        text = str(hint or '').strip()
        if text and text not in out:
            out.append(text)
    return out


def _completed_share_row_expected_file_count(row: Dict[str, Any]) -> int:
    row = row if isinstance(row, dict) else {}
    count = _safe_int(row.get('file_count'), 0)
    if count > 0:
        return count
    raw = _share_channel_raw_json(row)
    share_ids = raw.get('share_ids')
    if isinstance(share_ids, list) and share_ids:
        return len(share_ids)
    event = raw.get('event') if isinstance(raw.get('event'), dict) else {}
    return _safe_int(event.get('file_count') or event.get('episode_total'), 0)


def _completed_share_item_matches_missing_row(row: Dict[str, Any], item: Dict[str, Any]) -> bool:
    receive_code = str((row or {}).get('receive_code') or '').strip()
    item_receive = _completed_share_list_item_receive_code(item)
    if receive_code and item_receive and receive_code != item_receive:
        return False

    expected_count = _completed_share_row_expected_file_count(row)
    item_count = _completed_share_list_item_file_count(item)
    if expected_count > 0 and item_count > 0 and expected_count != item_count:
        return False
    if expected_count > 0 and item_count <= 0:
        return False
    if receive_code and item_receive and expected_count > 0 and item_count == expected_count:
        return True

    title_hints = [_norm_share_match_text(x) for x in _completed_share_row_title_hints(row)]
    title_hints = [x for x in title_hints if x]
    item_title = _norm_share_match_text(_completed_share_list_item_title(item))
    title_matched = bool(title_hints and item_title and any(h in item_title or item_title in h for h in title_hints))
    if title_hints and item_title and not title_matched:
        return False

    if receive_code and item_receive:
        return bool(expected_count > 0 or title_matched)
    return bool(expected_count > 0 and title_matched)


def _find_completed_share_list_item_by_receive_and_count(p115, *, receive_code: str = '', file_count: int = 0,
                                                         max_pages: int = 5) -> Dict[str, Any]:
    receive_code = str(receive_code or '').strip()
    expected_count = _safe_int(file_count, 0)
    if not receive_code or expected_count <= 0:
        return {}
    matches = []
    for item in _completed_share_recent_list_items(p115, max_pages=max_pages, limit=100):
        code = _completed_share_code_from_list_item(item)
        if not code:
            continue
        if _completed_share_list_item_receive_code(item) != receive_code:
            continue
        if _completed_share_list_item_file_count(item) != expected_count:
            continue
        matches.append(item)
    return matches[0] if len(matches) == 1 else {}


def _logical_share_row_file_ids(row: Dict[str, Any]) -> List[str]:
    raw = _share_channel_raw_json(row)
    share_ids = raw.get('share_ids')
    if isinstance(share_ids, list):
        return [str(x).strip() for x in share_ids if str(x).strip()]
    event = raw.get('event') if isinstance(raw.get('event'), dict) else {}
    ids = []
    for value in event.get('file_ids') or []:
        fid = str(value).strip()
        if fid and fid not in ids:
            ids.append(fid)
    best_asset_map = event.get('best_asset_map') if isinstance(event.get('best_asset_map'), dict) else {}
    for ep in sorted(best_asset_map.keys(), key=lambda x: _safe_int(x, 0)):
        item = best_asset_map.get(ep) if isinstance(best_asset_map.get(ep), dict) else {}
        fid = str(item.get('file_id') or item.get('fid') or '').strip()
        if fid and fid not in ids:
            ids.append(fid)
    return ids


def _same_logical_share_file_ids(left: List[str], right: List[str]) -> bool:
    left_ids = [str(x).strip() for x in (left or []) if str(x).strip()]
    right_ids = [str(x).strip() for x in (right or []) if str(x).strip()]
    return bool(left_ids and right_ids and len(left_ids) == len(right_ids) and set(left_ids) == set(right_ids))


def _local_existing_logical_share_for_create(group_id: str, manifest_hash: str = '', share_ids: List[str] = None) -> Dict[str, Any]:
    statuses = ['valid', 'pending_review', 'creating']
    share_ids = [str(x).strip() for x in (share_ids or []) if str(x).strip()]
    manifest_hash = str(manifest_hash or '').strip()
    rows = shared_share_db.list_completed_season_share_channels_by_source(center_source_id=group_id, statuses=statuses)
    for item in rows or []:
        if not _share_channel_is_logical(item):
            continue
        item_manifest = str(item.get('manifest_hash') or '').strip()
        if manifest_hash and item_manifest == manifest_hash:
            return item
        if _same_logical_share_file_ids(_logical_share_row_file_ids(item), share_ids):
            return item
    if not manifest_hash and not share_ids:
        return {}
    rows = shared_share_db.list_completed_season_share_channels(statuses=statuses, limit=1000, need_check=False)
    for item in rows or []:
        if not _share_channel_is_logical(item):
            continue
        if manifest_hash and str(item.get('manifest_hash') or '').strip() == manifest_hash:
            return item
        if _same_logical_share_file_ids(_logical_share_row_file_ids(item), share_ids):
            return item
    return {}


def _completed_share_status_from_list_item(item: Dict[str, Any], *, current_status: str = '') -> Dict[str, str]:
    """从 115 /share/slist 单条记录推断状态。

    share_list 的字段在不同 Cookie/p115client 版本里不稳定：有的给“正常/处理中”文案，
    有的只给 state/share_state/is_valid 之类数字。这里优先解析明确文案；如果本地
    channel 已经是 valid 且列表里仍能看到该 share_code，就把它当作 valid，避免再逐条
    请求 /share/shareinfo。
    """
    item = item if isinstance(item, dict) else {}
    if not item:
        return {}
    status_info = _completed_share_status_from_info({'share_list_item': item}, allow_implicit_valid=False)
    if status_info.get('explicit'):
        return status_info

    current = str(current_status or '').strip().lower()
    text = json.dumps(item, ensure_ascii=False, default=str).lower()
    # 常见数值/布尔字段兜底。只把“显式取消/删除/过期”判终态；
    # 正常态如果没有明确文案，仅在本地已经 valid 时用于继续补报中心。
    if any(k in text for k in ('cancel', 'deleted', 'expired', '已取消', '已删除', '过期', '失效')):
        return {'status': 'expired', 'review_status': 'expired', 'message': '115 分享已失效', 'explicit': True}

    for key in ('is_valid', 'valid', 'is_pass', 'is_passed', 'is_normal'):
        val = item.get(key)
        if val is True or str(val).strip() in {'1', 'true', 'True'}:
            return {'status': 'valid', 'review_status': 'passed', 'message': '115 分享审核通过', 'explicit': True}

    raw_state = str(item.get('share_state') or item.get('share_status') or item.get('status') or item.get('state') or '').strip().lower()
    if raw_state in {'normal', 'valid', 'available', 'alive', 'pass', 'passed', 'success'}:
        return {'status': 'valid', 'review_status': 'passed', 'message': '115 分享审核通过', 'explicit': True}
    if raw_state in {'pending', 'pending_review', 'reviewing', 'processing'}:
        return {'status': 'pending_review', 'review_status': 'pending', 'message': '115 分享审核中/处理中', 'explicit': True}
    if raw_state in {'expired', 'cancelled', 'canceled', 'deleted', 'invalid'}:
        return {'status': 'expired', 'review_status': 'expired', 'message': '115 分享已失效', 'explicit': True}

    if current == 'valid':
        return {'status': 'valid', 'review_status': 'passed', 'message': '115 分享审核通过', 'explicit': True, 'assumed_from_local_valid': True}
    return {'status': current or 'pending_review', 'review_status': 'pending', 'message': '115 未返回明确审核状态，保持本地状态', 'explicit': False}






def _share_channel_raw_json(row: Dict[str, Any]) -> Dict[str, Any]:
    raw = (row or {}).get('raw_json') if isinstance(row, dict) else {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _share_channel_is_logical(row: Dict[str, Any] = None, source_id: str = '') -> bool:
    row = row if isinstance(row, dict) else {}
    raw = _share_channel_raw_json(row)
    sid = str(source_id or row.get('center_source_id') or '').strip()
    return (
        str(raw.get('share_kind') or '').strip() == 'logical_season'
        or str(raw.get('event') or '').find('create_logical_season_filelist_share') >= 0
        or sid.startswith('svg_')
    )


def _keep_missing_share_code_channel(row: Dict[str, Any]) -> bool:
    """Keep rows that are useful as a review-failure/violation audit trail."""
    row = row if isinstance(row, dict) else {}
    status = str(row.get('status') or '').strip().lower()
    if status == 'review_failed':
        return True
    try:
        raw_text = json.dumps(_share_channel_raw_json(row), ensure_ascii=False, default=str).lower()
    except Exception:
        raw_text = ''
    text = f"{row.get('status_message') or ''}\n{raw_text}".lower()
    return any(x in text for x in ('违规', '违法', '审核不通过', '审核失败', 'violation', 'forbidden', 'risk'))


def _logical_share_report_payload_from_row(row: Dict[str, Any], channel_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """把本地 logical channel 的完整分享字段拼成中心 report payload。

    逻辑季分享不能再走 status-only 接口：旧 status 接口没有 share_code/share_url
    字段，中心会知道“115 审核通过”却拿不到转存凭证，最终前端只能显示秒传。
    """
    row = row if isinstance(row, dict) else {}
    report_payload = dict(payload or {})
    report_payload.update({
        'channel_id': channel_id,
        'share_code': row.get('share_code') or report_payload.get('share_code') or '',
        'receive_code': row.get('receive_code') or report_payload.get('receive_code') or '',
        'share_url': row.get('share_url') or report_payload.get('share_url') or '',
        'share_title': row.get('share_title') or row.get('root_name') or report_payload.get('share_title') or '',
        'root_fid': row.get('root_fid') or report_payload.get('root_fid') or '',
        'root_cid': row.get('root_cid') or report_payload.get('root_cid') or '',
        'root_name': row.get('root_name') or report_payload.get('root_name') or '',
        'file_count': report_payload.get('file_count') or row.get('file_count') or 0,
        'total_size': report_payload.get('total_size') or row.get('total_size') or 0,
    })
    raw = report_payload.get('raw_json') if isinstance(report_payload.get('raw_json'), dict) else {}
    raw = dict(raw or {})
    raw.setdefault('share_kind', 'logical_season')
    raw.setdefault('report_source', 'local_share_status_sync')
    report_payload['raw_json'] = raw
    if str(report_payload.get('status') or '').strip().lower() in {'review_failed', 'expired', 'import_failed', 'disabled', 'source_unavailable', 'failed'}:
        report_payload['share_code'] = ''
        report_payload['receive_code'] = ''
        report_payload['share_url'] = ''
    return report_payload


def _update_center_share_channel_status(client: SharedCenterClient, row: Dict[str, Any], channel_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if _share_channel_is_logical(row):
        group_id = str((row or {}).get('center_source_id') or '').strip()
        if not group_id:
            return {'ok': False, 'message': '逻辑季分享本地记录缺少 group_id(center_source_id)'}
        if not hasattr(client, 'report_logical_season_share'):
            return {'ok': False, 'message': 'SharedCenterClient 缺少 report_logical_season_share'}
        # 逻辑季一律走 report 接口，携带 share_code / receive_code / share_url。
        # 不再调用 status-only 接口，避免中心只收到“审核通过”状态却没有转存凭证。
        return client.report_logical_season_share(
            group_id,
            _logical_share_report_payload_from_row(row, channel_id, payload),
        )
    return {'ok': True, 'skipped': True, 'message': '旧 completed_season 分享通道本地跳过，中心只维护 logical_season_share_channels。'}


def _report_logical_share_failure(client: SharedCenterClient, *, group_id: str, channel_id: str,
                                  status: str = 'failed', message: str = '', raw_json: Dict[str, Any] | None = None,
                                  event_id: str = '', payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    raw_json = dict(raw_json or {})
    raw_json.setdefault('share_kind', 'logical_season')
    raw_json.setdefault('event_id', event_id)
    report_payload = {
        'channel_id': channel_id,
        'status': status,
        'review_status': status,
        'status_message': (message or status)[:1000],
        'share_title': payload.get('title') or payload.get('share_title') or '',
        'root_fid': '',
        'root_name': payload.get('title') or payload.get('share_title') or '',
        'file_count': payload.get('file_count') or len(payload.get('file_ids') or []),
        'total_size': payload.get('total_size') or 0,
        'raw_json': raw_json,
    }
    try:
        resp = client.report_logical_season_share(group_id, report_payload)
        shared_share_db.upsert_completed_season_share_channel({
            'channel_id': channel_id,
            'center_source_id': group_id,
            'hub_id': payload.get('hub_id') or '',
            'manifest_hash': payload.get('package_fingerprint') or payload.get('manifest_hash') or '',
            'status': status,
            'review_status': status,
            'status_message': message,
            'root_fid': '',
            'root_name': payload.get('title') or payload.get('share_title') or '',
            'file_count': report_payload['file_count'],
            'total_size': report_payload['total_size'],
            'raw_json': {'share_kind': 'logical_season', 'report_response': resp, 'event_id': event_id, **raw_json},
            'reported': True,
        })
        return resp
    except Exception as e:
        shared_share_db.upsert_completed_season_share_channel({
            'channel_id': channel_id,
            'center_source_id': group_id,
            'hub_id': payload.get('hub_id') or '',
            'manifest_hash': payload.get('package_fingerprint') or payload.get('manifest_hash') or '',
            'status': status,
            'status_message': f'{message}; 上报中心失败: {e}',
            'raw_json': {'share_kind': 'logical_season', 'event_id': event_id, **raw_json},
        })
        return {'ok': False, 'error': str(e)}


def _reject_legacy_completed_share_event(event: Dict[str, Any], *, ack: bool = True) -> Dict[str, Any]:
    """硬切逻辑季后，旧 create_completed_season_share 事件不再创建任何 115 分享。"""
    client = SharedCenterClient()
    event_id = str((event or {}).get('event_id') or '')
    message = '旧 create_completed_season_share 已删除：请由中心派发 create_logical_season_filelist_share。'
    if ack and event_id:
        try:
            client.ack_device_events([event_id], result='failed', message=message[:500])
        except Exception:
            pass
    logger.warning(f"  ➜ [共享资源] {message}")
    return {'ok': False, 'skipped': True, 'deprecated': True, 'event_id': event_id, 'message': message}




def _logical_share_file_ids_from_payload(client: SharedCenterClient, group_id: str, payload: Dict[str, Any]) -> List[str]:
    ids: List[str] = []
    for value in payload.get('file_ids') or []:
        text = str(value or '').strip()
        if text and text not in ids:
            ids.append(text)
    if ids:
        return ids
    best_asset_map = payload.get('best_asset_map') if isinstance(payload.get('best_asset_map'), dict) else {}
    for ep in sorted(best_asset_map.keys(), key=lambda x: _safe_int(x, 0)):
        item = best_asset_map.get(ep) if isinstance(best_asset_map.get(ep), dict) else {}
        fid = str(item.get('file_id') or item.get('fid') or '').strip()
        if fid and fid not in ids:
            ids.append(fid)
    if ids or not group_id:
        return ids
    try:
        manifest = client.logical_season_manifest(group_id) if hasattr(client, 'logical_season_manifest') else {}
        for item in manifest.get('files') or []:
            if not isinstance(item, dict):
                continue
            meta = item.get('rapid_meta_json') if isinstance(item.get('rapid_meta_json'), dict) else {}
            fid = str(item.get('file_id') or item.get('fid') or meta.get('fid') or '').strip()
            if fid and fid not in ids:
                ids.append(fid)
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 从中心 manifest 兜底获取 file_id 失败: group={group_id}, err={e}")
    return ids


def _logical_share_filelist_log_label(payload: Dict[str, Any], fallback_title: str = '', file_count: int = 0) -> str:
    """给逻辑季文件列表分享日志生成可读标题，不直接显示中心内部分享名。"""
    payload = payload if isinstance(payload, dict) else {}
    fallback_title = str(fallback_title or '').strip() or '逻辑完结季'

    def first_text(*keys: str) -> str:
        for key in keys:
            value = str(payload.get(key) or '').strip()
            if value:
                return value
        return ''

    parent = first_text('parent_series_tmdb_id', 'series_tmdb_id', 'series_id', 'tmdb_id', 'tv_id')
    season = _safe_int_or_none(payload.get('season_number') or payload.get('season'))
    if (not parent or season is None):
        for value in (fallback_title, first_text('title', 'share_title', 'root_name', 'name')):
            match = re.search(r'(?P<tmdb>\d{3,})\s+S(?P<season>\d{1,3})\b', str(value or ''), re.I)
            if match:
                parent = parent or match.group('tmdb')
                season = season if season is not None else _safe_int_or_none(match.group('season'))
                break

    identity = _series_identity_from_db(parent, season) if parent and season is not None else {}
    title = str(
        identity.get('title')
        or first_text('series_title', 'series_name', 'show_title', 'parent_title')
        or ''
    ).strip()
    if not title:
        title = fallback_title

    count = (
        _safe_int_or_none(file_count)
        or _safe_int_or_none(payload.get('file_count'))
        or _safe_int_or_none(payload.get('episode_total'))
        or _safe_int_or_none(payload.get('expected_episode_count'))
        or _safe_int_or_none(payload.get('total_episodes'))
        or _safe_int_or_none(identity.get('expected_episode_count'))
    )

    label = f"《{title}》"
    if season is not None:
        label += f"第 {season} 季"
    if count:
        label += f"，共 {count} 集"
    return label


def handle_create_logical_season_filelist_share_event(event: Dict[str, Any], *, ack: bool = True) -> Dict[str, Any]:
    """处理中心端 create_logical_season_filelist_share 事件：按中心传入的 file_id 列表创建 115 分享。"""
    client = SharedCenterClient()
    event_id = str((event or {}).get('event_id') or '')
    payload = _event_source_payload(event)
    channel_id = str(payload.get('channel_id') or (event or {}).get('source_ref_id') or '').strip()
    group_id = str(payload.get('group_id') or payload.get('source_id') or payload.get('source_ref_id') or '').strip()
    title = str(payload.get('title') or payload.get('share_title') or group_id or channel_id or '逻辑完结季').strip()

    if not channel_id or not group_id:
        message = '创建逻辑季分享事件缺少 channel_id/group_id'
        if ack and event_id:
            client.ack_device_events([event_id], result='failed', message=message)
        return {'ok': False, 'event_id': event_id, 'message': message}

    manifest_hash = str(payload.get('package_fingerprint') or payload.get('manifest_hash') or '').strip()
    share_ids = _logical_share_file_ids_from_payload(client, group_id, payload)
    log_label = _logical_share_filelist_log_label(payload, fallback_title=title, file_count=len(share_ids))
    existing_share = _local_existing_logical_share_for_create(group_id, manifest_hash, share_ids)
    if existing_share:
        status = str(existing_share.get('status') or 'pending_review').strip().lower()
        has_share_code = bool(str(existing_share.get('share_code') or '').strip())
        message = f'本地已存在逻辑季 115 分享，跳过重复创建：{title}'
        if has_share_code:
            message = f'本地已存在逻辑季 115 分享，复用已有分享：{title}'
        report_payload = {
            'status': status if status in {'valid', 'pending_review', 'creating'} else 'pending_review',
            'review_status': existing_share.get('review_status') or ('passed' if status == 'valid' else 'pending'),
            'status_message': message,
            'raw_json': {
                'share_kind': 'logical_season',
                'event_id': event_id,
                'event': payload,
                'reuse_existing_channel_id': existing_share.get('channel_id') or '',
                'report_source': 'local_logical_share_idempotent_reuse',
            },
        }
        try:
            report_resp = _update_center_share_channel_status(client, existing_share, channel_id, report_payload)
            reported = not (isinstance(report_resp, dict) and report_resp.get('ok') is False)
        except Exception as e:
            report_resp = {'ok': False, 'error': str(e)}
            reported = False
        shared_share_db.upsert_completed_season_share_channel({
            'channel_id': channel_id,
            'center_source_id': group_id,
            'hub_id': payload.get('hub_id') or existing_share.get('hub_id') or '',
            'manifest_hash': manifest_hash or existing_share.get('manifest_hash') or '',
            'status': report_payload['status'],
            'review_status': report_payload['review_status'],
            'status_message': message,
            'share_code': existing_share.get('share_code') or '',
            'receive_code': existing_share.get('receive_code') or '',
            'share_url': existing_share.get('share_url') or '',
            'share_title': existing_share.get('share_title') or existing_share.get('root_name') or title,
            'root_fid': existing_share.get('root_fid') or '',
            'root_cid': existing_share.get('root_cid') or '',
            'root_name': existing_share.get('root_name') or title,
            'file_count': payload.get('file_count') or existing_share.get('file_count') or 0,
            'total_size': payload.get('total_size') or existing_share.get('total_size') or 0,
            'raw_json': {
                'share_kind': 'logical_season',
                'event': payload,
                'reused_from_channel_id': existing_share.get('channel_id') or '',
                'center_report_response': report_resp,
            },
            'checked': True,
            'reported': reported,
        })
        if ack and event_id:
            client.ack_device_events([event_id], result='ok' if reported else 'failed', message=message[:500])
        logger.info(f"  ➜ [共享资源] 复用已有 115 文件列表分享：{log_label}。")
        logger.debug(
            f"  ➜ [共享资源] 复用文件列表分享详情：channel={channel_id}, "
            f"existing={existing_share.get('channel_id')}"
        )
        return {
            'ok': bool(reported),
            'event_id': event_id,
            'reused_existing_share': True,
            'channel_id': channel_id,
            'existing_channel_id': existing_share.get('channel_id') or '',
            'share_code': existing_share.get('share_code') or '',
            'report': report_resp,
        }

    expected_count = _safe_int(payload.get('file_count') or payload.get('episode_total'), 0)
    if not share_ids or (expected_count > 0 and len(share_ids) < expected_count):
        message = f'逻辑季分享缺少完整 file_id 列表：{len(share_ids)}/{expected_count or "?"}，无法创建 115 分享：{title}'
        report = _report_logical_share_failure(client, group_id=group_id, channel_id=channel_id, status='failed', message=message,
                                               raw_json={'share_ids': share_ids, 'payload': payload}, event_id=event_id, payload=payload)
        if ack and event_id:
            client.ack_device_events([event_id], result='ok', message=message[:500])
        return {'ok': False, 'event_id': event_id, 'message': message, 'report': report}

    receive_code = str(payload.get('receive_code') or '').strip() or _completed_share_receive_code(channel_id, group_id)
    shared_share_db.upsert_completed_season_share_channel({
        'channel_id': channel_id,
        'center_source_id': group_id,
        'hub_id': payload.get('hub_id') or '',
        'manifest_hash': payload.get('package_fingerprint') or payload.get('manifest_hash') or '',
        'status': 'creating',
        'status_message': f'正在创建逻辑季 115 文件列表分享：{title}',
        'receive_code': receive_code,
        'root_fid': '',
        'root_name': title,
        'file_count': len(share_ids),
        'total_size': payload.get('total_size') or 0,
        'raw_json': {'share_kind': 'logical_season', 'event': payload, 'share_ids': share_ids},
    })

    try:
        from handler.p115_service import P115Service
        p115 = P115Service.get_client()
        if not p115:
            raise RuntimeError('115 客户端未初始化')
        logger.info(f"  ➜ [共享资源] 开始创建 115 文件列表分享：{log_label}。")
        logger.debug(f"  ➜ [共享资源] 文件列表分享通道：{channel_id}")
        create_resp = p115.share_create(share_ids, share_duration=-1, receive_code=receive_code)
    except Exception as e:
        failure_status = _logical_share_provider_forbidden_status(str(e))
        message = f"创建逻辑季 115 分享异常：{failure_status['message']}"
        report = _report_logical_share_failure(client, group_id=group_id, channel_id=channel_id, status=failure_status['status'], message=message,
                                               raw_json={
                                                   'exception': str(e),
                                                   'share_ids': share_ids,
                                                   'share_forbidden_by_provider': failure_status['share_forbidden_by_provider'],
                                               }, event_id=event_id, payload=payload)
        if ack and event_id:
            client.ack_device_events([event_id], result='ok', message=message[:500])
        return {'ok': False, 'event_id': event_id, 'message': message, 'report': report}

    share_payload = _extract_completed_share_payload(create_resp, receive_code=receive_code)
    if not share_payload.get('share_code'):
        recovered_item = _find_completed_share_list_item_by_receive_and_count(
            p115,
            receive_code=receive_code,
            file_count=len(share_ids),
            max_pages=5,
        )
        recovered_code = _completed_share_code_from_list_item(recovered_item)
        if recovered_code:
            share_payload.update({
                'share_code': recovered_code,
                'receive_code': _completed_share_list_item_receive_code(recovered_item) or receive_code,
                'share_url': _completed_share_list_item_url(recovered_item, recovered_code),
                'raw_json': {
                    'create_response': create_resp,
                    'recovered_from_share_list': True,
                    'share_list_item': recovered_item,
                },
            })
    recovered_from_share_list = bool((share_payload.get('raw_json') or {}).get('recovered_from_share_list')) if isinstance(share_payload.get('raw_json'), dict) else False
    if (not _p115_ok(create_resp) and not recovered_from_share_list) or not share_payload.get('share_code'):
        failure_status = _logical_share_provider_forbidden_status(create_resp)
        message = f"创建逻辑季 115 分享失败：{failure_status['message']}"
        report = _report_logical_share_failure(client, group_id=group_id, channel_id=channel_id, status=failure_status['status'], message=message,
                                               raw_json={
                                                   'create_response': create_resp,
                                                   'share_ids': share_ids,
                                                   'share_forbidden_by_provider': failure_status['share_forbidden_by_provider'],
                                               }, event_id=event_id, payload=payload)
        if ack and event_id:
            client.ack_device_events([event_id], result='ok', message=message[:500])
        return {'ok': False, 'event_id': event_id, 'message': message, 'report': report, 'create_response': create_resp}

    info_resp = {}
    try:
        info_resp = p115.share_info(share_payload.get('share_code'))
    except Exception as e:
        info_resp = {'state': False, 'error_msg': str(e), 'stage': 'share_info_after_create'}
    status_info = _completed_share_status_from_info(info_resp, allow_implicit_valid=False)
    if status_info.get('status') == 'failed':
        status_info = {'status': 'pending_review', 'review_status': 'pending', 'message': '115 分享已创建，等待 115 审核'}

    report_payload = {
        'channel_id': channel_id,
        'status': status_info.get('status') or 'pending_review',
        'review_status': status_info.get('review_status') or '',
        'status_message': status_info.get('message') or '115 分享已创建',
        'share_code': share_payload.get('share_code') or '',
        'receive_code': share_payload.get('receive_code') or receive_code,
        'share_url': share_payload.get('share_url') or '',
        'share_title': title,
        'root_fid': '',
        'root_name': title,
        'file_count': len(share_ids),
        'total_size': payload.get('total_size') or 0,
        'raw_json': {
            'share_kind': 'logical_season',
            'event_id': event_id,
            'share_ids': share_ids,
            'create_response': create_resp,
            'share_info_response': info_resp,
            'share_forbidden_by_provider': status_info.get('status') == 'review_failed',
        },
    }
    try:
        report_resp = client.report_logical_season_share(group_id, report_payload)
        reported = True
    except Exception as e:
        report_resp = {'ok': False, 'error': str(e)}
        reported = False

    local_saved = shared_share_db.upsert_completed_season_share_channel({
        'channel_id': channel_id,
        'center_source_id': group_id,
        'hub_id': payload.get('hub_id') or '',
        'manifest_hash': payload.get('package_fingerprint') or payload.get('manifest_hash') or '',
        'status': report_payload['status'],
        'review_status': report_payload['review_status'],
        'status_message': report_payload['status_message'],
        'share_code': report_payload['share_code'],
        'receive_code': report_payload['receive_code'],
        'share_url': report_payload['share_url'],
        'share_title': title,
        'root_fid': '',
        'root_name': title,
        'file_count': report_payload['file_count'],
        'total_size': report_payload['total_size'],
        'raw_json': {'share_kind': 'logical_season', 'center_report_response': report_resp, 'event': payload, 'share_ids': share_ids},
        'checked': True,
        'reported': reported,
    })

    if ack and event_id:
        client.ack_device_events([event_id], result='ok' if reported else 'failed', message=(report_payload['status_message'] or '')[:500])
    status_text = {
        'valid': '已生效',
        'pending_review': '等待 115 审核',
        'review_failed': '审核未通过',
        'failed': '创建失败',
    }.get(report_payload['status'], report_payload['status'] or '状态未知')
    logger.info(f"  ➜ [共享资源] 115 文件列表分享已创建：{log_label}，状态：{status_text}。")
    logger.debug(f"  ➜ [共享资源] 文件列表分享上报详情：status={report_payload['status']}, channel={channel_id}")
    return {
        'ok': bool(reported),
        'event_id': event_id,
        'channel_id': channel_id,
        'group_id': group_id,
        'status': report_payload['status'],
        'share_code': report_payload['share_code'],
        'local': local_saved,
        'report': report_resp,
    }



def _direct_center_share_sync_heartbeat(payload: Dict[str, Any]) -> Dict[str, Any]:
    """直连中心端分享同步签到接口。SharedCenterClient 未升级时兜底使用。"""
    cfg = settings_db.get_shared_resource_config() or {}
    base_url = str(cfg.get('p115_shared_center_url') or '').strip().rstrip('/')
    server_id_hash = _current_server_id_hash()
    if not base_url or not server_id_hash:
        raise RuntimeError('共享中心地址或 Emby ServerID 未配置')
    headers = {
        'X-Server-ID-Hash': server_id_hash,
        'Content-Type': 'application/json',
        'X-Client-Version': str(getattr(constants, 'APP_VERSION', '0.0.0') or '0.0.0'),
    }
    kwargs = {'timeout': 20}
    getter = getattr(config_manager, 'get_proxies_for_requests', None)
    if callable(getter):
        try:
            proxies = getter()
            if proxies:
                kwargs['proxies'] = proxies
        except Exception:
            pass
    with requests.post(f'{base_url}/api/v1/devices/share-sync/heartbeat', headers=headers, json=payload, **kwargs) as resp:
        try:
            data = resp.json()
        except Exception:
            data = {'raw_text': resp.text[:1000]}
        if resp.status_code >= 400:
            raise RuntimeError(f'中心分享同步签到接口 HTTP {resp.status_code}: {data}')
    return data if isinstance(data, dict) else {'data': data}


def _report_share_sync_heartbeat(summary: Dict[str, Any] = None, *, status: str = 'ok',
                                 valid_logical_share_channels: List[Dict[str, Any]] | None = None,
                                 valid_logical_share_channels_full: bool = False) -> Dict[str, Any]:
    """向中心端签到：客户端分享同步任务硬编码 10 分钟一次，中心三次缺失判离线。"""
    if not _enabled():
        return {'ok': False, 'skipped': True, 'message': '共享资源未启用'}
    payload = {
        'task_name': 'shared_share_status_sync_high_freq',
        'task_interval_seconds': 600,
        'client_version': str(getattr(constants, 'APP_VERSION', '0.0.0') or '0.0.0'),
        'status': str(status or 'ok')[:80],
        'summary_json': summary if isinstance(summary, dict) else {},
    }
    if valid_logical_share_channels is not None:
        payload['valid_logical_share_channels'] = valid_logical_share_channels
        payload['valid_logical_share_channels_full'] = bool(valid_logical_share_channels_full)
    client = SharedCenterClient()
    method = getattr(client, 'share_sync_heartbeat', None)
    if callable(method):
        return method(payload)
    return _direct_center_share_sync_heartbeat(payload)

def _sync_completed_season_share_channels_once(limit: int = 50) -> Dict[str, Any]:
    """轻量同步本机已创建的完结季分享状态；供高频任务调用。"""
    if not _enabled():
        return {'ok': False, 'message': '共享资源未启用', 'checked': 0}
    if not _COMPLETED_SHARE_SYNC_LOCK.acquire(blocking=False):
        return {'ok': True, 'skipped': True, 'message': '已有分享状态同步正在运行', 'checked': 0}
    try:
        raw_rows = shared_share_db.list_completed_season_share_channels(
            # 本地表名仍是历史命名，但高频同步主链路只处理逻辑完结季文件列表分享。
            # valid 也周期性补报完整 share_code/share_url，确保中心端转存按钮不丢。
            statuses=['pending_review', 'creating', 'valid', 'failed', 'expired', 'review_failed', 'import_failed', 'source_unavailable', 'disabled'],
            limit=max(int(limit or 50) * 3, int(limit or 50)),
            need_check=False,
        )
        rows = [r for r in (raw_rows or []) if _share_channel_is_logical(r)]
        skipped_legacy = len(raw_rows or []) - len(rows)
        if limit and len(rows) > int(limit):
            rows = rows[:int(limit)]
        if not rows:
            return {'ok': True, 'checked': 0, 'skipped_legacy_completed_season': skipped_legacy, 'items': [], 'valid_logical_share_channels': []}
        from handler.p115_service import P115Service
        p115 = P115Service.get_client()
        if not p115:
            return {'ok': False, 'message': '115 客户端未初始化', 'checked': 0}
        client = SharedCenterClient()
        share_codes = [str(r.get('share_code') or '').strip() for r in rows if str(r.get('share_code') or '').strip()]
        # 只拿本地 ETK channel 已知的 share_code 做定点状态对账；不会把 115 私人分享导入共享池。
        share_list_map = _completed_share_known_list_map(p115, share_codes)
        items = []
        valid_logical_share_channels = []
        for row in rows:
            channel_id = str(row.get('channel_id') or '').strip()
            share_code = str(row.get('share_code') or '').strip()
            source_id = str(row.get('center_source_id') or '').strip()
            if not channel_id:
                continue
            try:
                row_status = str(row.get('status') or '').strip().lower()
                if not share_code:
                    if _keep_missing_share_code_channel(row):
                        items.append({
                            'channel_id': channel_id,
                            'source_id': source_id,
                            'status': row_status or 'review_failed',
                            'ok': True,
                            'kept_local_channel': True,
                            'reason': 'missing_share_code_review_failure_audit',
                        })
                        continue

                    msg = '本地逻辑季分享通道没有 share_code，按创建失败垃圾数据清理'
                    raw_status_json = {
                        'share_kind': 'logical_season',
                        'status_source': 'missing_share_code_cleanup',
                    }
                    try:
                        center_resp = _update_center_share_channel_status(client, row, channel_id, {
                            'status': 'failed',
                            'review_status': 'failed',
                            'status_message': msg,
                            'share_code': '',
                            'receive_code': '',
                            'share_url': '',
                            'raw_json': raw_status_json,
                        })
                    except Exception as ce:
                        center_resp = {'ok': False, 'error': str(ce)}
                    saved = shared_share_db.update_completed_season_share_channel(
                        channel_id,
                        status='failed',
                        review_status='failed',
                        status_message=msg,
                        raw_json={**raw_status_json, 'center_status_response': center_resp},
                        last_checked_at='NOW()',
                        last_reported_at='NOW()',
                    )
                    local_deleted = {}
                    center_ok = not (isinstance(center_resp, dict) and center_resp.get('ok') is False)
                    if center_ok:
                        local_deleted = shared_share_db.delete_completed_season_share_channel(channel_id)
                    items.append({
                        'channel_id': channel_id,
                        'source_id': source_id,
                        'status': 'failed',
                        'ok': bool(center_ok),
                        'reported_center': True,
                        'deleted_local_channel': bool(local_deleted),
                        'local': local_deleted or saved,
                        'center': center_resp,
                    })
                    continue

                if row_status == 'disabled':
                    delete_resp = _delete_completed_share_from_115(p115, share_code)
                    msg = '用户已取消完结季分享；已尝试删除 115 分享记录'
                    raw_status_json = {
                        'share_delete_response': delete_resp,
                        'status_source': 'local_disabled_cleanup',
                    }
                    try:
                        center_resp = _update_center_share_channel_status(client, row, channel_id, {
                            'status': 'disabled',
                            'review_status': 'disabled',
                            'status_message': msg,
                            'raw_json': raw_status_json,
                        })
                    except Exception as ce:
                        center_resp = {'ok': False, 'error': str(ce)}
                    saved = shared_share_db.update_completed_season_share_channel(
                        channel_id,
                        status='disabled',
                        review_status='disabled',
                        status_message=msg,
                        raw_json={**raw_status_json, 'center_status_response': center_resp},
                        last_checked_at='NOW()',
                        last_reported_at='NOW()',
                    )
                    local_deleted = {}
                    center_ok = not (isinstance(center_resp, dict) and center_resp.get('ok') is False)
                    if center_ok and delete_resp.get('state') is not False:
                        # disabled 是本地已取消的终态；中心也收到终态后，删除本地 channel 缓存，
                        # 防止每轮同步继续拿同一 share_code 调 115 API 删除。
                        local_deleted = shared_share_db.delete_completed_season_share_channel(channel_id)
                    items.append({
                        'channel_id': channel_id,
                        'source_id': source_id,
                        'status': 'disabled',
                        'ok': True,
                        'cleanup_deleted': bool(delete_resp.get('deleted')),
                        'deleted_local_channel': bool(local_deleted),
                        'local': local_deleted or saved,
                    })
                    continue

                list_item = share_list_map.get(share_code) or {}
                # 只使用批量 share_list 的结果；不再对每个 share_code 调 /share/shareinfo。
                # 高频任务只处理明确返回的状态变化；share_list 查不到或字段不明确时保持本地原状态。
                # 该设备分享是否缺失由共享维护任务的全量对账负责，避免部分列表误判导致中心端反复派发创建分享。
                if list_item:
                    status_info = _completed_share_status_from_list_item(list_item, current_status=row_status)
                    list_file_count = _completed_share_list_item_file_count(list_item)
                    list_total_size = _completed_share_list_item_total_size(list_item)
                else:
                    keep_status = str(row.get('status') or '').strip() or 'pending_review'
                    keep_review = str(row.get('review_status') or '').strip() or ('passed' if keep_status == 'valid' else 'pending')
                    msg = '本轮 115 分享列表未命中该 share_code，保持本地状态，等待共享维护任务全量对账'
                    saved = shared_share_db.update_completed_season_share_channel(
                        channel_id,
                        status=keep_status,
                        review_status=keep_review,
                        status_message=msg,
                        raw_json={
                            'share_list_item': {},
                            'status_source': 'share_list_partial_miss',
                            'center_status_skipped': True,
                            'reason': 'partial_share_list_miss',
                        },
                        last_checked_at='NOW()',
                    )
                    items.append({
                        'channel_id': channel_id,
                        'source_id': source_id,
                        'status': keep_status,
                        'ok': True,
                        'skipped_center_update': True,
                        'reason': 'partial_share_list_miss',
                        'local': saved,
                    })
                    continue

                raw_status_json = {
                    'share_list_item': list_item,
                    'status_source': 'share_list',
                }

                if not status_info.get('explicit'):
                    keep_status = str(row.get('status') or '').strip() or 'pending_review'
                    keep_review = str(row.get('review_status') or '').strip() or ('passed' if keep_status == 'valid' else 'pending')
                    msg = (
                        f"115 未返回明确审核状态，保持原状态 {keep_status}；"
                        f"{status_info.get('message') or '等待下轮同步'}"
                    )
                    saved = shared_share_db.update_completed_season_share_channel(
                        channel_id,
                        status=keep_status,
                        review_status=keep_review,
                        status_message=msg[:1000],
                        raw_json={**raw_status_json, 'center_status_skipped': True, 'reason': 'batch_share_list_not_explicit'},
                        last_checked_at='NOW()',
                    )
                    items.append({
                        'channel_id': channel_id,
                        'source_id': source_id,
                        'status': keep_status,
                        'ok': True,
                        'skipped_center_update': True,
                        'reason': 'batch_share_list_not_explicit',
                        'local': saved,
                    })
                    continue

                status = status_info.get('status') or row_status or 'failed'
                msg = status_info.get('message') or status
                delete_resp = {}
                # 115 Web 列表显示已取消/失效/违规时，顺手删除分享记录，
                # 避免链接分享页面长期堆一堆“已取消”的垃圾。只处理 ETK 本地 channel 表里的 share_code。
                if status in {'expired', 'review_failed'}:
                    delete_resp = _delete_completed_share_from_115(p115, share_code)
                    raw_status_json['share_delete_response'] = delete_resp
                    if delete_resp.get('deleted'):
                        msg = (msg + '；已删除 115 分享记录')[:1000]
                    elif delete_resp.get('cancelled_only'):
                        msg = (msg + '；已取消分享但删除记录失败')[:1000]

                # 增量上报：只有状态发生变化，或命中失效/违规终态时，才写中心事件。
                # 典型噪音是 valid -> valid / pending_review -> pending_review，
                # 这些只更新本地 last_checked_at，不再制造 completed_season_share_status_update。
                terminal_status = status in {'expired', 'review_failed'}
                status_changed = bool(row_status and row_status != status)
                should_report_center = bool(terminal_status or status_changed)

                if should_report_center:
                    center_resp = _update_center_share_channel_status(client, row, channel_id, {
                        'status': status,
                        'review_status': status_info.get('review_status') or '',
                        'status_message': msg,
                        'file_count': list_file_count or row.get('file_count') or 0,
                        'total_size': list_total_size or row.get('total_size') or 0,
                        'raw_json': {**raw_status_json, 'report_source': 'full_logical_share_credential_resync'},
                    })
                    saved = shared_share_db.update_completed_season_share_channel(
                        channel_id,
                        status=status,
                        review_status=status_info.get('review_status') or '',
                        status_message=msg,
                        file_count=list_file_count or row.get('file_count') or 0,
                        total_size=list_total_size or row.get('total_size') or 0,
                        raw_json={**raw_status_json, 'center_status_response': center_resp},
                        last_checked_at='NOW()',
                        last_reported_at='NOW()',
                    )
                else:
                    center_resp = {'ok': True, 'skipped': True, 'reason': 'status_unchanged'}
                    saved = shared_share_db.update_completed_season_share_channel(
                        channel_id,
                        status=status,
                        review_status=status_info.get('review_status') or '',
                        status_message=msg,
                        file_count=list_file_count or row.get('file_count') or 0,
                        total_size=list_total_size or row.get('total_size') or 0,
                        raw_json={**raw_status_json, 'center_status_skipped': True, 'reason': 'status_unchanged'},
                        last_checked_at='NOW()',
                    )

                local_deleted = {}
                center_ok = not (isinstance(center_resp, dict) and center_resp.get('ok') is False)
                if terminal_status and should_report_center and center_ok and delete_resp.get('state') is not False:
                    # 失效/违规也是分享通道终态。中心已同步后删除本地缓存，避免下轮继续打 115 API。
                    local_deleted = shared_share_db.delete_completed_season_share_channel(channel_id)
                if status == 'valid' and share_code:
                    valid_logical_share_channels.append({
                        'channel_id': channel_id,
                        'source_id': source_id,
                        'share_code': share_code,
                    })
                items.append({
                    'channel_id': channel_id,
                    'source_id': source_id,
                    'status': status,
                    'ok': True,
                    'reported_center': bool(should_report_center),
                    'skipped_center_update': not should_report_center,
                    'deleted_local_channel': bool(local_deleted),
                    'local': local_deleted or saved,
                })
            except Exception as e:
                shared_share_db.update_completed_season_share_channel(
                    channel_id,
                    status_message=f'同步分享状态失败: {e}',
                    last_checked_at='NOW()',
                )
                items.append({'channel_id': channel_id, 'source_id': source_id, 'ok': False, 'error': str(e)})
        return {
            'ok': True,
            'checked': len(items),
            'skipped_legacy_completed_season': skipped_legacy,
            'valid_logical_share_channels': valid_logical_share_channels,
            'items': items,
        }
    finally:
        _COMPLETED_SHARE_SYNC_LOCK.release()


def repair_logical_season_share_channels_from_115(*, max_pages: int = 20, dry_run: bool = False) -> Dict[str, Any]:
    """回填本地缺 share_code 的逻辑季分享，并清理未登记的违规 115 分享。"""
    if not _enabled():
        return {'ok': False, 'message': '共享资源未启用', 'backfilled': 0, 'deleted_untracked_invalid': 0}
    if not _COMPLETED_SHARE_SYNC_LOCK.acquire(blocking=False):
        return {'ok': True, 'skipped': True, 'message': '已有分享状态同步正在运行', 'backfilled': 0, 'deleted_untracked_invalid': 0}
    try:
        from handler.p115_service import P115Service
        p115 = P115Service.get_client()
        if not p115:
            return {'ok': False, 'message': '115 客户端未初始化', 'backfilled': 0, 'deleted_untracked_invalid': 0}

        share_items = _completed_share_recent_list_items(p115, max_pages=max_pages, limit=100)
        if not share_items:
            return {'ok': True, 'message': '115 分享列表为空或不可用', 'scanned_115': 0, 'backfilled': 0, 'deleted_untracked_invalid': 0}

        raw_rows = shared_share_db.list_completed_season_share_channels(
            statuses=['creating', 'pending_review', 'valid', 'failed', 'expired', 'review_failed', 'import_failed', 'source_unavailable', 'disabled'],
            limit=1000,
            need_check=False,
        )
        rows = [r for r in (raw_rows or []) if _share_channel_is_logical(r)]
        known_codes = {str(r.get('share_code') or '').strip() for r in rows if str(r.get('share_code') or '').strip()}
        missing_rows = [
            r for r in rows
            if not str(r.get('share_code') or '').strip()
            and str(r.get('status') or '').strip().lower() not in {'disabled'}
        ]

        client = SharedCenterClient()
        used_codes = set()
        backfilled_items = []
        ambiguous_items = []
        processed_codes = set()
        share_items_with_code = [
            item for item in share_items
            if _completed_share_code_from_list_item(item)
        ]

        for row in missing_rows:
            candidates = [
                item for item in share_items_with_code
                if _completed_share_code_from_list_item(item) not in used_codes
                and _completed_share_item_matches_missing_row(row, item)
            ]
            channel_id = str(row.get('channel_id') or '').strip()
            source_id = str(row.get('center_source_id') or '').strip()
            if not candidates:
                continue
            if len(candidates) != 1:
                ambiguous_items.append({
                    'channel_id': channel_id,
                    'source_id': source_id,
                    'candidates': [
                        {
                            'share_code': _completed_share_code_from_list_item(item),
                            'share_title': _completed_share_list_item_title(item),
                            'file_count': _completed_share_list_item_file_count(item),
                        }
                        for item in candidates[:5]
                    ],
                })
                continue

            item = candidates[0]
            share_code = _completed_share_code_from_list_item(item)
            used_codes.add(share_code)
            known_codes.add(share_code)
            processed_codes.add(share_code)

            row_status = str(row.get('status') or '').strip().lower()
            status_info = _completed_share_status_from_list_item(item, current_status=row_status)
            status = str(status_info.get('status') or '').strip() or ('pending_review' if row_status in {'', 'creating', 'failed'} else row_status)
            review_status = str(status_info.get('review_status') or '').strip() or ('passed' if status == 'valid' else 'pending')
            message = status_info.get('message') or '已从 115 分享列表回填本地分享记录'
            raw = _share_channel_raw_json(row)
            raw['share_backfill'] = {
                'share_kind': 'logical_season',
                'source': '115_share_list',
                'share_list_item': item,
                'dry_run': bool(dry_run),
            }
            share_title = _completed_share_list_item_title(item) or row.get('share_title') or row.get('root_name') or ''
            file_count = _completed_share_list_item_file_count(item) or _completed_share_row_expected_file_count(row)
            total_size = _completed_share_list_item_total_size(item) or row.get('total_size') or 0
            share_url = _completed_share_list_item_url(item, share_code)
            root_fid = _completed_share_list_item_root_id(item, 'root_fid', 'rootFid', 'fid', 'file_id', 'fileId') or row.get('root_fid') or ''
            root_cid = _completed_share_list_item_root_id(item, 'root_cid', 'rootCid', 'cid', 'parent_id', 'parentId') or row.get('root_cid') or ''

            if dry_run:
                backfilled_items.append({
                    'channel_id': channel_id,
                    'source_id': source_id,
                    'share_code': share_code,
                    'share_title': share_title,
                    'status': status,
                    'dry_run': True,
                })
                continue

            saved = shared_share_db.update_completed_season_share_channel(
                channel_id,
                share_code=share_code,
                receive_code=_completed_share_list_item_receive_code(item) or row.get('receive_code') or '',
                share_url=share_url,
                share_title=share_title,
                root_fid=root_fid,
                root_cid=root_cid,
                root_name=row.get('root_name') or share_title,
                file_count=file_count,
                total_size=total_size,
                status=status,
                review_status=review_status,
                status_message=message,
                raw_json=raw,
                last_checked_at='NOW()',
            )

            delete_resp = {}
            terminal_status = status in {'expired', 'review_failed'}
            if terminal_status:
                delete_resp = _delete_completed_share_from_115(p115, share_code)
                if delete_resp.get('deleted'):
                    message = (message + '；已删除 115 分享记录')[:1000]
                elif delete_resp.get('cancelled_only'):
                    message = (message + '；已取消分享但删除记录失败')[:1000]

            try:
                center_resp = _update_center_share_channel_status(client, saved or row, channel_id, {
                    'status': status,
                    'review_status': review_status,
                    'status_message': message,
                    'total_size': total_size,
                    'raw_json': {
                        'share_kind': 'logical_season',
                        'report_source': 'local_share_backfill',
                        'share_list_item': item,
                        'share_delete_response': delete_resp,
                    },
                })
            except Exception as e:
                center_resp = {'ok': False, 'error': str(e)}

            raw['share_backfill']['center_status_response'] = center_resp
            if delete_resp:
                raw['share_backfill']['share_delete_response'] = delete_resp
            saved = shared_share_db.update_completed_season_share_channel(
                channel_id,
                status=status,
                review_status=review_status,
                status_message=message,
                raw_json=raw,
                last_reported_at='NOW()',
            )
            local_deleted = {}
            center_ok = not (isinstance(center_resp, dict) and center_resp.get('ok') is False)
            if terminal_status and center_ok and delete_resp.get('state') is not False:
                local_deleted = shared_share_db.delete_completed_season_share_channel(channel_id)

            backfilled_items.append({
                'channel_id': channel_id,
                'source_id': source_id,
                'share_code': share_code,
                'share_title': share_title,
                'status': status,
                'reported_center': bool(center_ok),
                'deleted_terminal_share': bool(local_deleted),
                'local': local_deleted or saved,
            })

        deleted_items = []
        skipped_untracked = 0
        for item in share_items_with_code:
            share_code = _completed_share_code_from_list_item(item)
            if not share_code or share_code in known_codes or share_code in processed_codes:
                continue
            status_info = _completed_share_status_from_list_item(item, current_status='')
            status = str(status_info.get('status') or '').strip()
            if not status_info.get('explicit') or status != 'review_failed':
                skipped_untracked += 1
                continue
            title = _completed_share_list_item_title(item)
            if dry_run:
                delete_resp = {'state': True, 'dry_run': True}
            else:
                delete_resp = _delete_completed_share_from_115(p115, share_code)
            deleted_items.append({
                'share_code': share_code,
                'share_title': title,
                'status': status,
                'message': status_info.get('message') or '',
                'deleted': bool(delete_resp.get('deleted') or delete_resp.get('cancelled_only') or delete_resp.get('dry_run')),
                'response': delete_resp,
            })

        return {
            'ok': True,
            'dry_run': bool(dry_run),
            'scanned_115': len(share_items_with_code),
            'local_logical_channels': len(rows),
            'missing_share_code': len(missing_rows),
            'backfilled': len(backfilled_items),
            'ambiguous': len(ambiguous_items),
            'deleted_untracked_invalid': len(deleted_items),
            'skipped_untracked_normal': skipped_untracked,
            'items': backfilled_items[:20],
            'ambiguous_items': ambiguous_items[:10],
            'deleted_items': deleted_items[:20],
        }
    finally:
        _COMPLETED_SHARE_SYNC_LOCK.release()


def reconcile_logical_season_share_channels_full(*, max_pages: int = 50) -> Dict[str, Any]:
    """共享维护任务使用的全量 115 分享对账；高频同步不要调用。"""
    if not _enabled():
        return {'ok': False, 'message': '共享资源未启用', 'checked': 0}
    if not _COMPLETED_SHARE_SYNC_LOCK.acquire(blocking=False):
        return {'ok': True, 'skipped': True, 'message': '已有分享状态同步正在运行', 'checked': 0}
    try:
        from handler.p115_service import P115Service
        p115 = P115Service.get_client()
        if not p115:
            return {'ok': False, 'message': '115 客户端未初始化', 'checked': 0}

        scan = _completed_share_full_list_scan(p115, max_pages=max_pages, limit=100)
        share_items = scan.get('items') if isinstance(scan, dict) else []
        share_items = [item for item in (share_items or []) if _completed_share_code_from_list_item(item)]
        if not scan.get('complete'):
            return {
                'ok': False,
                'message': '115 分享列表未完整扫完，本轮不做全量清理',
                'checked': len(share_items),
                'pages': scan.get('pages', 0),
                'page_limit': scan.get('page_limit', max_pages),
            }

        raw_rows = shared_share_db.list_completed_season_share_channels(
            statuses=['creating', 'pending_review', 'valid'],
            limit=5000,
            need_check=False,
        )
        local_codes = {
            str(r.get('share_code') or '').strip()
            for r in (raw_rows or [])
            if _share_channel_is_logical(r) and str(r.get('share_code') or '').strip()
        }
        valid_channels = []
        for item in share_items:
            share_code = _completed_share_code_from_list_item(item)
            if not share_code or share_code not in local_codes:
                continue
            status_info = _completed_share_status_from_list_item(item, current_status='valid')
            if str(status_info.get('status') or '').strip().lower() != 'valid':
                continue
            valid_channels.append({'share_code': share_code})

        heartbeat = _report_share_sync_heartbeat(
            {
                'stage': 'maintenance_full_reconcile',
                'scanned_115': len(share_items),
                'valid_logical_share_channels': len(valid_channels),
                'pages': scan.get('pages', 0),
            },
            valid_logical_share_channels=valid_channels,
            valid_logical_share_channels_full=True,
        )
        return {
            'ok': not (isinstance(heartbeat, dict) and heartbeat.get('ok') is False),
            'checked': len(share_items),
            'valid_logical_share_channels': len(valid_channels),
            'pages': scan.get('pages', 0),
            'heartbeat': heartbeat,
        }
    finally:
        _COMPLETED_SHARE_SYNC_LOCK.release()




def _looks_like_logical_season_group_id(value: Any) -> bool:
    text = str(value or '').strip().lower()
    return bool(text and re.match(r'^(svg_|lsg_|logical_season_)', text))


def _logical_group_id_from_transfer_payload(payload: Dict[str, Any], fallback: Any = '') -> str:
    payload = payload if isinstance(payload, dict) else {}
    logical_group = payload.get('logical_group') if isinstance(payload.get('logical_group'), dict) else {}
    for value in (
        payload.get('logical_group_id'),
        payload.get('group_id'),
        logical_group.get('group_id'),
        logical_group.get('source_id'),
        payload.get('logical_season_group_id'),
        payload.get('source_id'),
        payload.get('source_ref_id'),
        fallback,
    ):
        text = str(value or '').strip()
        if _looks_like_logical_season_group_id(text):
            return text
    for value in (payload.get('logical_group_id'), payload.get('group_id'), logical_group.get('group_id')):
        text = str(value or '').strip()
        if text:
            return text
    return ''


def _normalize_event_source_for_transfer(event: Dict[str, Any], source: Dict[str, Any] = None) -> Dict[str, str]:
    payload = _event_source_payload(event)
    source = source if isinstance(source, dict) else payload
    source_kind = str(
        payload.get('source_kind')
        or source.get('source_kind')
        or (event or {}).get('source_kind')
        or ''
    ).strip().lower().replace('-', '_')
    source_id = str(
        payload.get('source_id')
        or payload.get('source_ref_id')
        or source.get('source_id')
        or source.get('source_ref_id')
        or (event or {}).get('source_ref_id')
        or ''
    ).strip()
    if source_kind == 'completed_season':
        group_id = _logical_group_id_from_transfer_payload(payload or source, source_id)
        if group_id:
            logical_group = payload.get('logical_group') if isinstance(payload.get('logical_group'), dict) else {}
            channel = (
                payload.get('share_channel')
                or payload.get('logical_season_share_channel')
                or payload.get('completed_season_share_channel')
                or {}
            )
            channel = channel if isinstance(channel, dict) else {}
            raw_channel = channel.get('raw_json') if isinstance(channel.get('raw_json'), dict) else {}
            if (
                _looks_like_logical_season_group_id(group_id)
                or payload.get('logical_pool_complete')
                or payload.get('pool_complete')
                or payload.get('logical_shadow_only')
                or payload.get('logical_import_available')
                or payload.get('logical_group_id')
                or payload.get('group_id')
                or logical_group
                or isinstance(payload.get('best_asset_map'), dict)
                or str((channel or {}).get('share_kind') or raw_channel.get('share_kind') or '').strip() == 'logical_season'
            ):
                source_kind = 'logical_season'
                source_id = group_id
                try:
                    payload['source_kind'] = 'logical_season'
                    payload['source_id'] = source_id
                    payload['source_ref_id'] = source_id
                    event['source_kind'] = 'logical_season'
                    event['source_ref_id'] = source_id
                    event['payload_json'] = payload
                except Exception:
                    pass
    return {'source_kind': source_kind, 'source_id': source_id}


def _event_transfer_lease_identity(event: Dict[str, Any]) -> Dict[str, str]:
    payload = _event_source_payload(event)
    normalized = _normalize_event_source_for_transfer(event, payload)
    source_kind = normalized.get('source_kind') or ''
    source_id = normalized.get('source_id') or ''
    sha1 = _norm_sha1(payload.get('sha1'))
    event_type = str((event or {}).get('event_type') or payload.get('event_type') or '').strip()
    if source_kind not in {'movie', 'episode', 'logical_episode', 'logical_season'} or not source_id:
        return {}
    if event_type in {COMPLETED_SEASON_SHARE_CREATE_EVENT_TYPE, LOGICAL_SEASON_SHARE_CREATE_EVENT_TYPE, PRO_QUOTA_AUTH_EVENT_TYPE}:
        return {}
    return {'source_kind': source_kind, 'source_id': source_id, 'sha1': sha1}


def _direct_center_transfer_lease(payload: Dict[str, Any]) -> Dict[str, Any]:
    """SharedCenterClient 未升级 acquire_transfer_lease 时，直接 POST 中心 lease 接口。

    前端手动秒传和后台监听都走这里兜底，避免客户端文件已打补丁，
    但 handler.shared_center_client 还没新增方法时静默退回旧链路。
    """
    cfg = settings_db.get_shared_resource_config() or {}
    base_url = str(cfg.get('p115_shared_center_url') or '').strip().rstrip('/')
    server_id_hash = _current_server_id_hash()
    if not base_url or not server_id_hash:
        raise RuntimeError('共享中心地址或 Emby ServerID 未配置')
    headers = {
        'X-Server-ID-Hash': server_id_hash,
        'Content-Type': 'application/json',
        'X-Client-Version': str(getattr(constants, 'APP_VERSION', '0.0.0') or '0.0.0'),
    }
    kwargs = {'timeout': 30}
    getter = getattr(config_manager, 'get_proxies_for_requests', None)
    if callable(getter):
        proxies = getter()
        if proxies:
            kwargs['proxies'] = proxies
    with requests.post(f'{base_url}/api/v1/transfers/lease', headers=headers, json=payload, **kwargs) as resp:
        try:
            data = resp.json()
        except Exception:
            data = {'raw_text': resp.text[:1000]}
        if resp.status_code >= 400:
            raise RuntimeError(f'中心秒传许可接口 HTTP {resp.status_code}: {data}')
    return data if isinstance(data, dict) else {'data': data}



def _event_transfer_lease_label(event: Dict[str, Any], identity: Dict[str, str] = None) -> str:
    """生成秒传许可日志里给人看的资源名，不把 payload/source_id 整坨打出来。"""
    identity = identity if isinstance(identity, dict) else {}
    payload = _event_source_payload(event)
    candidates = []
    for obj in (payload, event if isinstance(event, dict) else {}):
        if not isinstance(obj, dict):
            continue
        for key in ('file_name', 'name', 'title', 'share_title', 'root_name'):
            value = str(obj.get(key) or '').strip()
            if value:
                candidates.append(value)
    for key in ('files', 'items', 'pack_items', 'children'):
        value = payload.get(key)
        if isinstance(value, list) and value:
            first = next((x for x in value if isinstance(x, dict)), {}) or {}
            for sub_key in ('file_name', 'name', 'title'):
                sub_value = str(first.get(sub_key) or '').strip()
                if sub_value:
                    if len(value) > 1:
                        candidates.append(f"{sub_value} 等 {len(value)} 个文件")
                    else:
                        candidates.append(sub_value)
                    break
            if candidates:
                break
    label = next((x for x in candidates if x), '')
    if not label:
        label = f"{identity.get('source_kind') or '-'}:{identity.get('source_id') or '-'}"
    label = label.replace('\r', ' ').replace('\n', ' ').strip()
    return label[:180] + ('...' if len(label) > 180 else '')

def _client_call_transfer_lease(client: SharedCenterClient, identity: Dict[str, str], event: Dict[str, Any]) -> Dict[str, Any]:
    payload = {
        'source_kind': identity.get('source_kind'),
        'source_id': identity.get('source_id'),
        'sha1': identity.get('sha1') or None,
        'transfer_mode': 'rapid',
        'request_meta_json': {
            'event_id': str((event or {}).get('event_id') or ''),
            'event_type': str((event or {}).get('event_type') or ''),
            'client_gate': 'shared_resource_tasks_transfer_lease_v1',
        },
    }
    # 兼容不同版本 SharedCenterClient：优先走显式方法，其次走常见私有 request 方法，最后直连中心。
    method = getattr(client, 'acquire_transfer_lease', None)
    if callable(method):
        return method(payload)
    for name in ('post', '_post'):
        fn = getattr(client, name, None)
        if callable(fn):
            return fn('/api/v1/transfers/lease', payload)
    for name in ('request', '_request'):
        fn = getattr(client, name, None)
        if callable(fn):
            try:
                return fn('POST', '/api/v1/transfers/lease', json=payload)
            except TypeError:
                return fn('POST', '/api/v1/transfers/lease', payload)
    return _direct_center_transfer_lease(payload)


def _wait_transfer_lease_for_event(event: Dict[str, Any], *, max_wait_seconds: int = 60,
                                   stop_event: threading.Event | None = None) -> Dict[str, Any]:
    identity = _event_transfer_lease_identity(event)
    if not identity:
        return {'ok': True, 'skipped': True, 'reason': 'not_rapid_transfer_event'}
    client = SharedCenterClient()
    deadline = time.time() + max(10, int(max_wait_seconds or 60))
    attempts = 0
    last_resp: Dict[str, Any] = {}
    while True:
        if stop_event is not None and stop_event.is_set():
            return {'ok': True, 'lease_timeout': True, 'stopped': True, 'lease': last_resp, 'attempts': attempts}
        attempts += 1
        try:
            resp = _client_call_transfer_lease(client, identity, event) or {}
        except Exception as e:
            # 中心未升级 lease 接口时，不阻断老链路；只记录 DEBUG，继续原消费流程。
            logger.debug(f"  ➜ [共享资源] 秒传许可接口不可用，按旧流程继续：{identity}，err={e}")
            return {'ok': True, 'skipped': True, 'reason': 'lease_api_unavailable', 'error': str(e)}
        last_resp = resp if isinstance(resp, dict) else {'raw': resp}
        if last_resp.get('ok') and last_resp.get('allow') is False and not last_resp.get('deferred'):
            logger.info(
                f"  ➜ [共享资源] 中心秒传许可明确拒绝：{_event_transfer_lease_label(event, identity)}，"
                f"reason={last_resp.get('reason') or 'not_allowed'}"
            )
            return {'ok': True, 'blocked': True, 'lease': last_resp, 'attempts': attempts}
        if last_resp.get('allow') or (last_resp.get('ok') and not last_resp.get('deferred') and not last_resp.get('allow') is False):
            lease_id = str(last_resp.get('lease_id') or '').strip()
            if lease_id:
                payload = _event_source_payload(event)
                payload['rapid_transfer_lease_id'] = lease_id
                payload['transfer_lease_id'] = lease_id
                # 如果原消费端把 event.payload_json 作为 source 使用，这里把 lease_id 写回去；
                # sign_job request_meta/report_transfer 新版可继续透传。
                try:
                    event['payload_json'] = payload
                except Exception:
                    pass
            label = _event_transfer_lease_label(event, identity)
            logger.info(f"  ➜ [共享资源] 秒传许可已发放：{label}")
            return {'ok': True, 'lease': last_resp, 'attempts': attempts}
        retry_after = _safe_int(last_resp.get('retry_after'), 30)
        retry_after = max(5, min(retry_after, 120))
        reason = str(last_resp.get('reason') or 'deferred')
        if time.time() + retry_after > deadline:
            # 不把事件消费成失败扣点；给原流程一个机会，中心签名并发阀仍会兜底。
            logger.warning(
                f"  ➜ [共享资源] 秒传许可等待超时，转入旧流程并交由中心签名阀兜底："
                f"{identity}，reason={reason}，last={last_resp}"
            )
            return {'ok': True, 'lease_timeout': True, 'lease': last_resp, 'attempts': attempts}
        logger.debug(
            f"  ➜ [共享资源] 中心秒传许可排队中：{_event_transfer_lease_label(event, identity)}，"
            f"{retry_after}s 后重试，reason={reason}"
        )
        if stop_event is not None:
            if stop_event.wait(retry_after):
                return {'ok': True, 'lease_timeout': True, 'stopped': True, 'lease': last_resp, 'attempts': attempts}
        else:
            time.sleep(retry_after)


def _local_event_should_bypass_transfer_lease(event: Dict[str, Any], source: Dict[str, Any]) -> Dict[str, Any]:
    """本地必跳过的事件不要先去中心排 lease。

    consume_device_event 自己已经会 ACK 并返回跳过；这里仅提前识别，
    让禁止单集/本机源这类本地跳过不参与中心许可证排队。
    """
    source = source if isinstance(source, dict) else {}
    try:
        episode_guard = getattr(shared_subscription_service, '_event_episode_transfer_disabled_guard', None)
        if callable(episode_guard):
            blocked = episode_guard(event if isinstance(event, dict) else {}, source) or {}
            if blocked.get('blocked'):
                return {
                    'bypass': True,
                    'reason': 'episode_transfer_disabled',
                    'message': blocked.get('message') or '已按配置跳过单集秒传',
                    'details': blocked,
                }
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 预判单集秒传禁用失败，交给消费端兜底：{e}")

    try:
        own_checker = getattr(shared_subscription_service, '_file_is_own_center_source', None)
        if callable(own_checker):
            client = SharedCenterClient()
            if own_checker(source, source, client):
                return {
                    'bypass': True,
                    'reason': 'self_owned_source',
                    'message': '本机共享源事件，跳过中心秒传许可排队，交给消费端直接 ACK 跳过。',
                }
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 预判本机源失败，交给消费端兜底：{e}")

    return {'bypass': False}


def _completed_season_share_lease_bypass(event: Dict[str, Any], source: Dict[str, Any]) -> Dict[str, Any]:
    """完结季已有有效 115 分享通道时，不提前排 Rapid 秒传许可。

    分享转存不需要签名 holder，也不应该被 Rapid lease 的短窗口节流挡住；
    如果后续 share_import 失败，消费端会在回退 Rapid 前再单独申请 lease。
    """
    payload = _event_source_payload(event)
    source = source if isinstance(source, dict) else {}
    normalized = _normalize_event_source_for_transfer(event, source)
    source_kind = normalized.get('source_kind') or ''
    source_id = normalized.get('source_id') or ''
    if source_kind != 'logical_season' or not source_id:
        return {'bypass': False}

    try:
        from_payload = getattr(shared_subscription_service, '_completed_share_channel_from_payload', None)
        if callable(from_payload):
            channel = from_payload(payload) or {}
            if str(channel.get('status') or '').strip().lower() == 'valid' and channel.get('share_code'):
                return {
                    'bypass': True,
                    'reason': 'valid_completed_or_logical_season_share_channel_in_payload',
                    'channel_id': str(channel.get('channel_id') or ''),
                }
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 预判 payload 分享通道失败，交给消费端兜底：{e}")

    try:
        channel_for_transfer = getattr(shared_subscription_service, '_completed_share_channel_for_transfer', None)
        if callable(channel_for_transfer):
            client = SharedCenterClient()
            channel = channel_for_transfer(client, source_id, payload) or {}
            if str(channel.get('status') or '').strip().lower() == 'valid' and channel.get('share_code'):
                return {
                    'bypass': True,
                    'reason': 'valid_completed_or_logical_season_share_channel',
                    'channel_id': str(channel.get('channel_id') or ''),
                }
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 查询完结季/逻辑季分享通道失败，仍按 Rapid 许可门禁处理：{e}")

    return {'bypass': False}


def _consume_device_event_with_transfer_gate(original_consume, event, *args, **kwargs):
    stop_event = kwargs.pop('_stop_event', None)
    lease_max_wait_seconds = kwargs.pop('_lease_max_wait_seconds', 60)
    event_type = _completed_share_event_type(event)
    if event_type == COMPLETED_SEASON_SHARE_CREATE_EVENT_TYPE:
        return _reject_legacy_completed_share_event(event, ack=bool(kwargs.get('ack', True)))
    if event_type == LOGICAL_SEASON_SHARE_CREATE_EVENT_TYPE:
        return handle_create_logical_season_filelist_share_event(event, ack=bool(kwargs.get('ack', True)))
    source = _event_source_payload(event)
    bypass = _local_event_should_bypass_transfer_lease(event, source)
    if bypass.get('bypass'):
        logger.info(f"  ➜ [共享资源] 跳过中心秒传许可：{bypass.get('message') or bypass.get('reason')}")
        return original_consume(event, *args, **kwargs)
    gate = _shared_transfer_gate(source)
    if not gate.get('ok'):
        logger.info(f"  ➜ [共享资源] 秒传拦截：{gate.get('message') or gate.get('reason')}")
        return {
            'ok': True,
            'skipped': True,
            'blocked': True,
            'blocked_reason': gate.get('reason'),
            'success_count': 0,
            'total': 0,
            'message': gate.get('message') or '该资源已被共享资源配置拦截',
        }
    share_bypass = _completed_season_share_lease_bypass(event, source)
    if share_bypass.get('bypass'):
        logger.info(
            f"  ➜ [共享资源] 跳过中心秒传许可：完结季/逻辑季存在有效 115 分享通道，"
            f"直接尝试分享转存，channel={share_bypass.get('channel_id') or '-'}"
        )
        return original_consume(event, *args, **kwargs)
    lease_result = _wait_transfer_lease_for_event(
        event,
        max_wait_seconds=lease_max_wait_seconds,
        stop_event=stop_event,
    )
    if lease_result.get('blocked'):
        lease = lease_result.get('lease') if isinstance(lease_result.get('lease'), dict) else {}
        return {
            'ok': True,
            'skipped': True,
            'blocked': True,
            'blocked_reason': lease.get('reason') or 'transfer_lease_blocked',
            'success_count': 0,
            'total': 0,
            'message': lease.get('message') or '中心秒传许可拒绝，跳过该共享事件',
        }
    return original_consume(event, *args, **kwargs)


def consume_device_event_with_transfer_gate(event, *args, **kwargs):
    """公开给前端手动秒传路由使用的消费入口。

    后台监听通过 poll_and_consume_once 临时包装 consume_device_event；
    前端手动秒传是 Flask 路由直接调用消费函数，不经过长轮询包装，
    所以必须显式走同一层秒传许可/本地门禁。
    """
    original = getattr(shared_subscription_service, 'consume_device_event', None)
    if not callable(original):
        return {'ok': False, 'message': 'consume_device_event 不可用', 'success_count': 0, 'total': 0, 'errors': []}
    if getattr(original, '_etk_transfer_gate_wrapped', False):
        return original(event, *args, **kwargs)
    return _consume_device_event_with_transfer_gate(original, event, *args, **kwargs)


def poll_and_consume_once(*args, **kwargs):
    """长轮询消费前加本地配置门禁，保证自动秒传也能拦截纯净版/短剧。"""
    stop_event = kwargs.pop('stop_event', None)
    lease_max_wait_seconds = kwargs.pop('lease_max_wait_seconds', 60)
    original = getattr(shared_subscription_service, 'consume_device_event', None)
    if not callable(original) or getattr(original, '_etk_transfer_gate_wrapped', False):
        return _raw_poll_and_consume_once(*args, **kwargs)

    def _wrapped(event, *a, **kw):
        kw['_stop_event'] = stop_event
        kw['_lease_max_wait_seconds'] = lease_max_wait_seconds
        return _consume_device_event_with_transfer_gate(original, event, *a, **kw)

    _wrapped._etk_transfer_gate_wrapped = True
    shared_subscription_service.consume_device_event = _wrapped
    try:
        return _raw_poll_and_consume_once(*args, **kwargs)
    finally:
        shared_subscription_service.consume_device_event = original


def _raw_for_file(file_info: Dict[str, Any]) -> Dict[str, Any]:
    sha1 = _norm_sha1(file_info.get('sha1'))
    if not sha1:
        return {}

    def _read_raw() -> Dict[str, Any]:
        row = shared_share_db.raw_ffprobe_for_sha1(sha1) or {}
        return row.get('raw_ffprobe_json') if isinstance(row.get('raw_ffprobe_json'), dict) else {}

    def _has_required_etk(raw_obj: Dict[str, Any]) -> bool:
        etk_obj = raw_obj.get('_etk') if isinstance(raw_obj, dict) and isinstance(raw_obj.get('_etk'), dict) else {}
        return bool(str(etk_obj.get('tmdb_id') or '').strip() and str(etk_obj.get('type') or '').strip())

    def _probe_missing_raw() -> Dict[str, Any]:
        pick_code = str(
            file_info.get('pick_code') or file_info.get('pickcode') or file_info.get('pc') or ''
        ).strip()
        if not pick_code:
            return {}
        try:
            from handler.p115_media_analyzer import P115MediaAnalyzerMixin
            from handler.p115_service import P115CacheManager, P115Service
            analyzer = P115MediaAnalyzerMixin()
            analyzer.client = P115Service.get_client()
            if not analyzer.client:
                return {}
            probe_file = {
                **dict(file_info or {}),
                'sha1': sha1,
                'pc': pick_code,
                'pick_code': pick_code,
                'file_name': file_info.get('file_name') or file_info.get('name') or sha1,
                'n': file_info.get('name') or file_info.get('file_name') or sha1,
            }
            result = analyzer._probe_mediainfo_with_ffprobe(probe_file, sha1=sha1, silent_log=True)
            if not result:
                return {}
            emby_json, raw_probe = result if isinstance(result, tuple) else (result, None)
            if not isinstance(raw_probe, dict) or not raw_probe:
                return {}
            P115CacheManager.save_mediainfo_cache(sha1, emby_json if isinstance(emby_json, dict) else {}, raw_probe, file_info=probe_file)
            logger.info(
                "  ➜ [共享资源] 已自动补齐缺失 RAW：%s",
                file_info.get('file_name') or file_info.get('name') or sha1,
            )
            return raw_probe
        except Exception as e:
            logger.warning(
                "  ➜ [共享资源] 自动补齐 RAW 失败：%s，err=%s",
                file_info.get('file_name') or file_info.get('name') or sha1,
                e,
            )
            return {}

    raw = _read_raw()
    if not raw:
        raw = _probe_missing_raw()
        if raw:
            raw = _read_raw() or raw
    if raw and not _has_required_etk(raw):
        try:
            from handler.p115_service import P115CacheManager
            P115CacheManager.get_raw_ffprobe_cache(sha1)
            raw = _read_raw()
        except Exception as e:
            logger.debug(f"  ➜ [共享资源] RAW 上传前自检补齐 _etk 失败: sha1={sha1[:12]}..., err={e}")

    if not raw:
        return {}
    # 补齐中心需要的 _etk。中心会清理 cookie/pc/url，不泄露 CK。
    raw = dict(raw)
    etk = raw.get('_etk') if isinstance(raw.get('_etk'), dict) else {}
    etk = dict(etk or {})
    etk.setdefault('sha1', sha1)
    preid = _norm_preid(file_info.get('preid') or etk.get('preid'))
    if preid:
        etk['preid'] = preid
    if file_info.get('tmdb_id'):
        etk.setdefault('tmdb_id', str(file_info.get('tmdb_id')))
    if file_info.get('item_type') in ('Movie', 'Episode', 'Season'):
        etk.setdefault('type', 'movie' if file_info.get('item_type') == 'Movie' else 'tv')
    if file_info.get('season_number') not in (None, ''):
        etk.setdefault('season_number', _safe_int(file_info.get('season_number')))
    if file_info.get('episode_number') not in (None, ''):
        etk.setdefault('episode_number', _safe_int(file_info.get('episode_number')))
    raw['_etk'] = etk
    if not _has_required_etk(raw):
        logger.warning(
            "  ➜ [共享资源] RAW 缺少 TMDb 身份，拒绝上传中心：%s",
            file_info.get('file_name') or file_info.get('name') or sha1,
        )
        return {}
    return raw


def _raw_video_stream(raw: Dict[str, Any]) -> Dict[str, Any]:
    raw = raw or {}
    if isinstance(raw.get('MediaSourceInfo'), dict):
        streams = raw.get('MediaSourceInfo', {}).get('MediaStreams') or []
        return next((s for s in streams if str(s.get('Type') or '').lower() == 'video'), {}) or {}
    if isinstance(raw.get('MediaStreams'), list):
        return next((s for s in raw.get('MediaStreams') if str(s.get('Type') or '').lower() == 'video'), {}) or {}
    if isinstance(raw.get('streams'), list):
        return next((s for s in raw.get('streams') if str(s.get('codec_type') or s.get('type') or '').lower() == 'video'), {}) or {}
    return {}


def _video_resolution(video: Dict[str, Any]) -> str:
    try:
        width = int(video.get('Width') or video.get('width') or 0)
        height = int(video.get('Height') or video.get('height') or 0)
    except Exception:
        width = height = 0
    if width >= 7600: return '8K'
    if width >= 3800: return '4K'
    if width >= 1900: return '1080p'
    if width >= 1200: return '720p'
    return f'{height}p' if height else ''


def _effect_key(raw: Dict[str, Any]) -> str:
    text = json.dumps(_raw_video_stream(raw), ensure_ascii=False, default=str).upper()
    if 'DOLBY' in text or 'DOVI' in text or 'DVHE' in text:
        return 'DV'
    if 'HDR10+' in text or 'SMPTE2094' in text:
        return 'HDR10+'
    if 'HLG' in text or 'ARIB-STD-B67' in text:
        return 'HLG'
    if 'HDR10' in text or 'SMPTE2084' in text or 'BT2020' in text:
        return 'HDR10'
    if 'HDR' in text:
        return 'HDR'
    return 'SDR'


def _stream_type(stream: Dict[str, Any]) -> str:
    return str(stream.get('Type') or stream.get('codec_type') or stream.get('type') or '').strip().lower()


def _raw_streams(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw = raw or {}
    if isinstance(raw.get('MediaSourceInfo'), dict):
        streams = raw.get('MediaSourceInfo', {}).get('MediaStreams') or []
    elif isinstance(raw.get('MediaStreams'), list):
        streams = raw.get('MediaStreams') or []
    elif isinstance(raw.get('streams'), list):
        streams = raw.get('streams') or []
    else:
        streams = []
    return [s for s in streams if isinstance(s, dict)]


def _codec_display(value: Any) -> str:
    text = str(value or '').strip()
    low = text.lower()
    return {
        'hevc': 'HEVC', 'h265': 'HEVC', 'x265': 'HEVC',
        'h264': 'H.264', 'avc': 'H.264', 'x264': 'H.264',
        'av1': 'AV1', 'vp9': 'VP9',
    }.get(low, text.upper() if text else '')


def _fraction_to_float(value: Any):
    text = str(value or '').strip()
    if not text:
        return None
    try:
        if '/' in text:
            a, b = text.split('/', 1)
            b = float(b)
            return float(a) / b if b else None
        return float(text)
    except Exception:
        return None


def _fps_display(video: Dict[str, Any]) -> str:
    value = (
        video.get('AverageFrameRate') or video.get('RealFrameRate') or video.get('FrameRate') or
        video.get('avg_frame_rate') or video.get('r_frame_rate')
    )
    fps = _fraction_to_float(value)
    if fps is None or fps <= 0:
        return ''
    if abs(fps - round(fps)) < 0.03:
        return f'{int(round(fps))} fps'
    return f'{fps:.2f} fps'


def _track_display(stream: Dict[str, Any]) -> Dict[str, Any]:
    lang = str(stream.get('Language') or stream.get('language') or stream.get('lang') or '').strip()
    codec = _codec_display(stream.get('Codec') or stream.get('codec_name') or stream.get('codec'))
    channels = stream.get('Channels') or stream.get('channels') or stream.get('channel_layout')
    title = str(stream.get('DisplayTitle') or stream.get('Title') or stream.get('title') or '').strip()
    parts = []
    if lang:
        parts.append(lang)
    if codec:
        parts.append(codec)
    if channels:
        ch_text = str(channels)
        if ch_text.isdigit():
            ch_text = f'{ch_text}ch'
        parts.append(ch_text)
    if title and title not in parts:
        parts.append(title)
    return {
        'language': lang,
        'codec': codec,
        'channels': channels,
        'title': title,
        'display': ' · '.join([str(x) for x in parts if str(x).strip()]),
    }


def _media_signature(raw: Dict[str, Any], source: Dict[str, Any] = None) -> Dict[str, Any]:
    """生成中心源表里的轻量媒体签名。

    这里不能再直接从 ffprobe raw 抽 codec_name，否则字幕会变成
    HDMV_PGS_SUBTITLE 这类原始值。优先走和 summary_json 相同的
    _build_emby_mediainfo_from_ffprobe 格式化链路；失败时才退回旧的
    raw 轻量抽取。
    """
    source = source or {}
    if isinstance(raw, dict) and raw:
        try:
            summary = _summarize_raw_ffprobe(raw, source)
            if isinstance(summary, dict) and summary:
                effect = summary.get('effect') or ''
                codec = summary.get('codec') or summary.get('video_codec') or ''
                resolution = summary.get('resolution') or ''
                # 兼容旧的洗版/一致性字段命名。
                summary.setdefault('resolution_display', resolution)
                summary.setdefault('effect_key', effect)
                summary.setdefault('codec_display', codec)
                summary.setdefault('frame_rate', summary.get('fps') or '')
                return summary
        except Exception as e:
            logger.debug(f"  ➜ [共享资源] 生成格式化媒体签名失败，退回 raw 轻量签名: {e}")

    streams = _raw_streams(raw)
    video = next((s for s in streams if _stream_type(s) == 'video'), {}) or {}
    audio_list = [_track_display(s) for s in streams if _stream_type(s) == 'audio']
    subtitle_list = [_track_display(s) for s in streams if _stream_type(s) in ('subtitle', 'subtitles')]
    codec = _codec_display(video.get('Codec') or video.get('codec_name') or video.get('codec'))
    bit_depth = video.get('BitDepth') or video.get('bits_per_raw_sample') or video.get('bits_per_sample')
    fps = _fps_display(video)
    resolution = _video_resolution(video)
    effect = _effect_key(raw)
    sig = {
        'resolution': resolution,
        'resolution_display': resolution,
        'effect': effect,
        'effect_key': effect,
        'codec': codec,
        'video_codec': codec,
        'codec_display': codec,
        'bit_depth': bit_depth,
        'fps': fps,
        'frame_rate': fps,
        'audio_list': [x for x in audio_list if x.get('display') or x.get('codec') or x.get('language')],
        'subtitle_list': [x for x in subtitle_list if x.get('display') or x.get('codec') or x.get('language')],
    }
    return _apply_runtime_meta(sig, raw)


def _center_format_rate(value: Any) -> str:
    """把 ffprobe 的 24000/1001 这类帧率转成前端友好格式。"""
    try:
        if value in [None, '', '0/0']:
            return ''
        text = str(value)
        if '/' in text:
            a, b = text.split('/', 1)
            b_val = float(b)
            if b_val == 0:
                return ''
            rate = float(a) / b_val
        else:
            rate = float(text)
        if rate <= 0:
            return ''
        return f"{rate:.3f}".rstrip('0').rstrip('.') + ' fps'
    except Exception:
        return str(value or '')


def _center_codec_label(codec: Any) -> str:
    c = str(codec or '').lower()
    return {
        'hevc': 'HEVC', 'h265': 'HEVC', 'h264': 'AVC', 'avc': 'AVC',
        'av1': 'AV1', 'mpeg2video': 'MPEG2', 'vc1': 'VC-1',
        'eac3': 'DDP', 'ac3': 'AC3', 'truehd': 'TrueHD', 'dts': 'DTS',
        'aac': 'AAC', 'flac': 'FLAC', 'opus': 'OPUS', 'subrip': 'SRT',
        'ass': 'ASS', 'ssa': 'SSA', 'hdmv_pgs_subtitle': 'PGS', 'pgssub': 'PGS',
        'webvtt': 'VTT', 'mov_text': 'MOV_TEXT',
    }.get(c, c.upper() if c else '')


def _center_resolution(width: int, height: int) -> str:
    try:
        width = int(width or 0)
        height = int(height or 0)
    except Exception:
        width, height = 0, 0
    if width >= 7600:
        return '8K'
    if width >= 3800:
        return '4K'
    if width >= 1900:
        return '1080p'
    if width >= 1200:
        return '720p'
    return f'{height}p' if height else ''


def _center_limit_track_list(value, limit: int):
    if not isinstance(value, list):
        return value
    limit = _safe_int(limit, 0)
    if limit <= 0 or len(value) <= limit:
        return value
    return value[:limit]


def _center_video_effect(video: Dict[str, Any]) -> str:
    if not video:
        return ''
    ev_type = str(video.get('ExtendedVideoType') or '')
    ev_sub = str(video.get('ExtendedVideoSubType') or '')
    ev_desc = str(video.get('ExtendedVideoSubTypeDescription') or '')
    video_range = str(video.get('VideoRange') or '')
    if ev_type.lower() == 'dolbyvision' or ev_sub.lower().startswith('dovi'):
        profile = ''
        m = re.search(r'DoviProfile(\d+)', ev_sub, re.IGNORECASE)
        if m:
            raw = m.group(1)
            profile = f"P{raw[0]}.{raw[1:]}" if len(raw) > 1 else f"P{raw}"
        elif ev_desc:
            m = re.search(r'Profile\s*([0-9.]+)', ev_desc, re.IGNORECASE)
            if m:
                profile = f"P{m.group(1)}"
        base = f"Dolby Vision {profile}".strip()
        if 'HDR10' in video_range.upper() and 'HDR10' not in base:
            base += ' / HDR10'
        return base
    vr = video_range.upper()
    if 'HDR10+' in vr:
        return 'HDR10+'
    if 'HDR10' in vr:
        return 'HDR10'
    if vr == 'HDR':
        return 'HDR'
    return ''


def _center_track_display(stream: Dict[str, Any], stream_type: str) -> str:
    """优先使用 _build_emby_mediainfo_from_ffprobe 已经净化过的 DisplayTitle。"""
    if not stream:
        return ''
    display = str(stream.get('DisplayTitle') or '').strip()
    if display:
        return display
    parts = []
    lang = stream.get('DisplayLanguage') or stream.get('Language') or ''
    title = stream.get('Title') or ''
    codec = _center_codec_label(stream.get('Codec'))
    if lang and lang != '未知':
        parts.append(str(lang))
    if codec:
        parts.append(codec)
    if stream_type == 'Audio':
        channels = stream.get('Channels')
        if channels:
            parts.append(f"{channels}ch")
    if title and title not in parts:
        parts.append(str(title))
    return ' '.join([x for x in parts if x])


def _center_track_title(stream: Dict[str, Any]) -> str:
    """只保留格式化轨道 Title。

    摘要不是 RAW，音轨/字幕展示继续用 DisplayTitle；标签识别只需要
    p115_media_analyzer 写好的 Title，例如“国语（台配）”、
    “中英双语特效简体（上译）”。
    """
    if not isinstance(stream, dict):
        return ''
    return str(stream.get('Title') or stream.get('title') or '').strip()


def _center_track_summary(stream: Dict[str, Any], stream_type: str) -> Dict[str, Any]:
    display = _center_track_display(stream, stream_type)
    title = _center_track_title(stream)
    out = {}
    if display:
        out['display'] = display
    if title:
        out['title'] = title
    return out


_CENTER_MEDIAINFO_FORMATTER = None


def _get_center_mediainfo_formatter():
    """懒加载 formatter，复用 P115MediaAnalyzerMixin 的音轨/字幕格式化逻辑。"""
    global _CENTER_MEDIAINFO_FORMATTER
    if _CENTER_MEDIAINFO_FORMATTER is not None:
        return _CENTER_MEDIAINFO_FORMATTER
    from handler.p115_media_analyzer import P115MediaAnalyzerMixin

    class _Formatter(P115MediaAnalyzerMixin):
        def __init__(self):
            try:
                import utils
                self.language_map = settings_db.get_setting('language_mapping') or utils.DEFAULT_LANGUAGE_MAPPING
                self.stream_feature_map = settings_db.get_setting('stream_feature_mapping') or getattr(utils, 'DEFAULT_STREAM_FEATURE_MAPPING', [])
            except Exception:
                self.language_map = []
                self.stream_feature_map = []

    _CENTER_MEDIAINFO_FORMATTER = _Formatter()
    return _CENTER_MEDIAINFO_FORMATTER


def _infer_size_from_raw(raw: Dict[str, Any]) -> int:
    if not isinstance(raw, dict):
        return 0
    try:
        fmt = raw.get('format') or {}
        size = fmt.get('size')
        if size is not None and str(size).strip():
            return _rapid_size_to_int(size, 0)
    except Exception:
        pass
    try:
        msi = raw.get('MediaSourceInfo') if isinstance(raw.get('MediaSourceInfo'), dict) else {}
        size = msi.get('Size') or raw.get('Size')
        if size is not None and str(size).strip():
            return _rapid_size_to_int(size, 0)
    except Exception:
        pass
    return 0


def _build_center_emby_info(raw: Dict[str, Any], source: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(raw, dict) or not raw:
        return {}
    if raw.get('MediaSourceInfo'):
        return raw.get('MediaSourceInfo') or {}
    if raw.get('MediaStreams'):
        return raw

    size = _rapid_size_to_int(source.get('size') or (raw.get('format') or {}).get('size'), 0)
    file_node = {
        'fn': source.get('file_name') or source.get('title') or source.get('sha1') or 'unknown.mkv',
        'n': source.get('file_name') or source.get('title') or source.get('sha1') or 'unknown.mkv',
        'fs': size,
        'size': size,
        'sha1': source.get('sha1') or '',
    }
    try:
        formatter = _get_center_mediainfo_formatter()
        if not hasattr(formatter, '_build_emby_mediainfo_from_ffprobe'):
            return {}
        built = formatter._build_emby_mediainfo_from_ffprobe(raw, file_node, sha1=source.get('sha1') or '')
        if isinstance(built, list) and built:
            return (built[0] or {}).get('MediaSourceInfo') or {}
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] raw ffprobe 格式化失败，使用轻量兜底: {e}")
    return {}


def _summarize_raw_ffprobe(raw: Dict[str, Any], source: Dict[str, Any] = None) -> Dict[str, Any]:
    """中心资源库展示用：优先复用 _build_emby_mediainfo_from_ffprobe 得到标准化音轨/字幕标题。"""
    source = source or {}
    if not isinstance(raw, dict):
        raw = {}

    media_info = _build_center_emby_info(raw, source)
    streams = media_info.get('MediaStreams') or []
    video = next((s for s in streams if str(s.get('Type') or '').lower() == 'video'), {})
    audios = [s for s in streams if str(s.get('Type') or '').lower() == 'audio']
    subs = [s for s in streams if str(s.get('Type') or '').lower() == 'subtitle']

    size = source.get('size') or media_info.get('Size') or (raw.get('format') or {}).get('size') or 0
    size = _rapid_size_to_int(size, 0)

    if video:
        width = int(video.get('Width') or 0)
        height = int(video.get('Height') or 0)
        codec = _center_codec_label(video.get('Codec'))
        bit_depth = video.get('BitDepth') or ''
        fps = video.get('AverageFrameRate') or video.get('RealFrameRate') or ''
        fps_text = _center_format_rate(fps)
        effect = _center_video_effect(video)
        bitrate = video.get('BitRate') or media_info.get('Bitrate') or ''
        video_display = ' · '.join([x for x in [
            _center_resolution(width, height),
            effect,
            codec,
            f"{bit_depth}bit" if bit_depth else '',
            fps_text,
        ] if x])
    else:
        raw_streams = raw.get('streams') or []
        raw_video = next((s for s in raw_streams if str(s.get('codec_type')).lower() == 'video'), {})
        width = int(raw_video.get('width') or 0) if raw_video else 0
        height = int(raw_video.get('height') or 0) if raw_video else 0
        codec = _center_codec_label(raw_video.get('codec_name')) if raw_video else ''
        bit_depth = raw_video.get('bits_per_raw_sample') or raw_video.get('bits_per_sample') or ''
        fps_text = _center_format_rate(raw_video.get('avg_frame_rate') or raw_video.get('r_frame_rate') or '') if raw_video else ''
        effect = ''
        bitrate = (raw.get('format') or {}).get('bit_rate') or ''
        video_display = ' · '.join([x for x in [_center_resolution(width, height), codec, f"{bit_depth}bit" if bit_depth else '', fps_text] if x])

    audio_list = [_center_track_display(s, 'Audio') for s in audios]
    subtitle_list = [_center_track_display(s, 'Subtitle') for s in subs]
    audio_list = [x for x in audio_list if x]
    subtitle_list = [x for x in subtitle_list if x]
    # 摘要只保留展示文本 + Title。Title 是标签识别的最小必要字段，
    # 不把 Language/Codec/Index/IsDefault 等完整轨道对象塞进 summary_json。
    audio_items = [x for x in (_center_track_summary(s, 'Audio') for s in audios) if x.get('display') or x.get('title')]
    subtitle_items = [x for x in (_center_track_summary(s, 'Subtitle') for s in subs) if x.get('display') or x.get('title')]

    summary = {
        'resolution': _center_resolution(width, height),
        'width': width,
        'height': height,
        'video_codec': codec,
        'codec': codec,
        'effect': effect,
        'effect_key': effect,
        'resolution_display': _center_resolution(width, height),
        'codec_display': codec,
        'bit_depth': bit_depth,
        'fps': fps_text,
        'frame_rate': fps_text,
        'bitrate': bitrate,
        'container': media_info.get('Container') or '',
        'video_display': video_display,
        'size': size,
        'size_gb': round(size / 1024 / 1024 / 1024, 2) if size else 0,
        'audio_count': len(audios),
        'subtitle_count': len(subs),
        'audio_list': _center_limit_track_list(audio_list, 16),
        'subtitle_list': subtitle_list,
        'audios': _center_limit_track_list(audio_items, 16),
        'subtitles': subtitle_items,
        'formatted_by': 'emby_mediainfo' if media_info else 'raw_fallback',
    }
    return _apply_runtime_meta(summary, raw)


def _build_raw_ffprobe_summary_for_center(raw: Dict[str, Any], item: Dict[str, Any], final_size: int = 0) -> Dict[str, Any]:
    """上传 RAW 时同步生成中心列表页轻量 MediaInfo 摘要。"""
    if not isinstance(raw, dict) or not raw:
        return {}

    source = {
        'sha1': str((item or {}).get('sha1') or '').strip().upper(),
        'file_name': (item or {}).get('file_name') or (item or {}).get('name') or (item or {}).get('title') or '',
        'title': (item or {}).get('title') or (item or {}).get('file_name') or '',
        'size': final_size or (item or {}).get('size') or _infer_size_from_raw(raw),
        'tmdb_id': (item or {}).get('tmdb_id') or (raw.get('_etk') or {}).get('tmdb_id'),
        'item_type': (item or {}).get('item_type') or (item or {}).get('share_type') or (raw.get('_etk') or {}).get('type'),
    }

    try:
        summary = _summarize_raw_ffprobe(raw, source)
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 生成中心轻量 MediaInfo 摘要失败: sha1={source.get('sha1')[:8]}, err={e}")
        return {}

    if not isinstance(summary, dict):
        return {}

    allowed_keys = {
        'resolution', 'width', 'height', 'video_codec', 'codec', 'effect', 'bit_depth',
        'fps', 'bitrate', 'container', 'video_display', 'size', 'size_gb',
        'audio_count', 'subtitle_count', 'audio_list', 'subtitle_list',
        'audios', 'subtitles', 'formatted_by',
        'resolution_display', 'codec_display', 'effect_key', 'frame_rate',
        'duration_minutes',
    }
    compact = {k: summary.get(k) for k in allowed_keys if k in summary}
    for key, max_len in (('audio_list', 16), ('subtitle_list', 0), ('audios', 16), ('subtitles', 0)):
        value = compact.get(key)
        if isinstance(value, list):
            compact[key] = _center_limit_track_list(value, max_len)

    try:
        return json.loads(json.dumps(compact, ensure_ascii=False, default=str))
    except Exception:
        return {}




def _summary_json_usable_for_center(summary: Dict[str, Any]) -> bool:
    """中心资源库展示摘要必须非空且包含至少一个可展示字段。"""
    if not isinstance(summary, dict) or not summary:
        return False
    for key in (
        'resolution', 'width', 'height', 'video_codec', 'codec', 'video_display',
        'fps', 'frame_rate', 'bitrate', 'audio_list', 'subtitle_list', 'audios', 'subtitles',
    ):
        value = summary.get(key)
        if value in (None, '', [], {}):
            continue
        if isinstance(value, (int, float)) and float(value) <= 0:
            continue
        return True
    return False

def _summary_json_preserves_track_titles(summary: Dict[str, Any]) -> bool:
    """旧摘要只有 display，会丢掉字幕 Title 里的“特效”。

    重新登记时，中心已有 RAW 但 summary_json 缺少 Title，也需要补传新版摘要。
    """
    if not isinstance(summary, dict) or not summary:
        return False

    def _items(key):
        value = summary.get(key)
        return value if isinstance(value, list) else []

    def _has_title(items):
        return any(isinstance(x, dict) and str(x.get('title') or x.get('Title') or '').strip() for x in items)

    audio_count = _safe_int(summary.get('audio_count'), 0)
    subtitle_count = _safe_int(summary.get('subtitle_count'), 0)
    if audio_count > 0 and not _has_title(_items('audios')):
        return False
    if subtitle_count > 0 and not _has_title(_items('subtitles')):
        return False
    return True

def _prepare_raw_upload_entry(file_info: Dict[str, Any]) -> Dict[str, Any]:
    sha1 = _norm_sha1(file_info.get('sha1'))
    if not sha1:
        return {}
    raw = _raw_for_file(file_info)
    if not raw:
        return {}
    final_size = _file_size_from_cache(file_info) or _infer_size_from_raw(raw) or None
    summary_json = _build_raw_ffprobe_summary_for_center(raw, file_info, final_size or 0)
    if not _summary_json_usable_for_center(summary_json):
        logger.warning(
            f"  ➜ [共享资源] RAW 存在但无法生成中心展示摘要，拒绝上传/登记: "
            f"{file_info.get('file_name') or file_info.get('name') or sha1}"
        )
        return {}
    logger.debug(
        f"  ➜ [共享资源] 已生成中心格式化 MediaInfo 摘要: "
        f"sha1={sha1[:8]}..., formatted_by={summary_json.get('formatted_by') or '-'}, "
        f"audio={summary_json.get('audio_count')}, subtitle={summary_json.get('subtitle_count')}"
    )
    return {
        'sha1': sha1,
        'size': final_size,
        'raw_ffprobe_json': raw,
        'summary_json': summary_json,
    }


def _prime_candidate_files_for_registration(candidate: Dict[str, Any], files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    candidate = dict(candidate or {})
    item_type = str(candidate.get('item_type') or '').strip()
    candidate_tmdb_id = str(candidate.get('parent_series_tmdb_id') or candidate.get('series_tmdb_id') or candidate.get('tmdb_id') or '').strip()
    if item_type == 'Movie':
        candidate_tmdb_id = str(candidate.get('tmdb_id') or '').strip()
    for file_info in files or []:
        if item_type == 'Movie':
            file_info.setdefault('item_type', 'Movie')
            file_info.setdefault('tmdb_id', candidate_tmdb_id)
        elif item_type in ('Season', 'Episode'):
            file_info.setdefault('item_type', 'Episode')
            file_info.setdefault('tmdb_id', candidate_tmdb_id)
            file_info.setdefault('parent_series_tmdb_id', candidate_tmdb_id)
            file_info.setdefault('series_tmdb_id', candidate_tmdb_id)
            file_info.setdefault('season_number', candidate.get('season_number'))
            if item_type == 'Episode':
                file_info.setdefault('episode_number', candidate.get('episode_number'))
        _ensure_file_preid(file_info)
    return files


def _upload_raw_batch(client: SharedCenterClient, files: List[Dict[str, Any]]) -> Dict[str, Any]:
    """按需上传 RAW。

    Rapid v2 的 RAW/summary_json 是 SHA1 级缓存，不是“来源客户端”级数据。
    所以登记前先问中心哪些 SHA1 已经 ready；已有可用 RAW 的直接复用，
    只给中心补传缺失、对象文件丢失或 summary_json 不可用的 SHA1。
    """
    by_sha1: Dict[str, Dict[str, Any]] = {}
    for f in files or []:
        sha1 = _norm_sha1((f or {}).get('sha1'))
        if sha1 and sha1 not in by_sha1:
            by_sha1[sha1] = f

    uploaded: Dict[str, bool] = {}
    errors = []
    skipped_existing = 0
    need_upload_sha1s = set(by_sha1.keys())

    if by_sha1 and hasattr(client, 'raw_batch'):
        try:
            resp = client.raw_batch(list(by_sha1.keys())) or {}
            for item in resp.get('items') or []:
                sha = _norm_sha1((item or {}).get('sha1'))
                if not sha:
                    continue
                center_summary = (item or {}).get('summary_json') or {}
                ready = (
                    item.get('raw_ready') is not False
                    and _summary_json_usable_for_center(center_summary)
                    and _summary_json_preserves_track_titles(center_summary)
                )
                if ready:
                    uploaded[sha] = True
                    need_upload_sha1s.discard(sha)
                    skipped_existing += 1
            missing = {_norm_sha1(x) for x in (resp.get('missing') or []) if _norm_sha1(x)}
            # 中心明确 missing 的保留在 need_upload_sha1s；其余未返回项也按需补传，兼容异常响应。
            need_upload_sha1s = {sha for sha in need_upload_sha1s if sha in missing or sha not in uploaded}
        except Exception as e:
            logger.debug(f"  ➜ [共享资源] 查询中心 RAW 状态失败，改为按旧逻辑上传: {e}")
            uploaded = {}
            skipped_existing = 0
            need_upload_sha1s = set(by_sha1.keys())

    entries = []
    for sha1, f in by_sha1.items():
        if sha1 not in need_upload_sha1s:
            continue
        entry = _prepare_raw_upload_entry(f)
        if entry:
            entries.append(entry)

    skipped_existing_sha1s = [sha for sha, ok in uploaded.items() if ok]
    if not entries:
        return {
            'ok': True,
            'uploaded': uploaded,
            'count': len(uploaded),
            'uploaded_count': 0,
            'skipped_existing': skipped_existing,
            'skipped_existing_sha1s': skipped_existing_sha1s,
            'fresh_uploaded_sha1s': [],
            'errors': errors,
        }

    try:
        if hasattr(client, 'upload_raw_ffprobe_batch'):
            resp = client.upload_raw_ffprobe_batch(entries)
            ok_items = resp.get('items') or resp.get('uploaded') or []
            before = set(uploaded.keys())
            for item in ok_items:
                sha = _norm_sha1((item or {}).get('sha1'))
                if sha:
                    uploaded[sha] = True
            # 中心旧版本可能只返回 count；这种情况下视本批次都成功，避免回退逐个再刷屏。
            if len(uploaded) == len(before) and int(resp.get('count') or 0) == len(entries) and not resp.get('errors'):
                for x in entries:
                    sha = _norm_sha1(x.get('sha1'))
                    if sha:
                        uploaded[sha] = True
            errors = resp.get('errors') or []
            uploaded_count = len(set(uploaded.keys()) - before)
            if uploaded_count or skipped_existing:
                return {
                    'ok': not errors,
                    'uploaded': uploaded,
                    'count': len(uploaded),
                    'uploaded_count': uploaded_count,
                    'skipped_existing': skipped_existing,
                    'skipped_existing_sha1s': skipped_existing_sha1s,
                    'fresh_uploaded_sha1s': [sha for sha in uploaded.keys() if sha not in before],
                    'errors': errors,
                }
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] RAW 批量上传失败，回退逐个上传: {e}")

    # 兼容未升级中心：单个逐个传，但只作为 fallback。
    before = set(uploaded.keys())
    for entry in entries:
        sha = _norm_sha1(entry.get('sha1'))
        if not sha or uploaded.get(sha):
            continue
        try:
            client.upload_raw_ffprobe(sha, entry.get('raw_ffprobe_json') or {}, size=entry.get('size'), summary_json=entry.get('summary_json'))
            uploaded[sha] = True
        except Exception as e:
            errors.append({'sha1': sha, 'error': str(e)})
    return {
        'ok': not errors,
        'uploaded': uploaded,
        'count': len(uploaded),
        'uploaded_count': len(set(uploaded.keys()) - before),
        'skipped_existing': skipped_existing,
        'skipped_existing_sha1s': skipped_existing_sha1s,
        'fresh_uploaded_sha1s': [sha for sha in uploaded.keys() if sha not in before],
        'errors': errors,
    }


def _raw_batch_missing_for_files(files: List[Dict[str, Any]], uploaded_sha1s: Dict[str, bool]) -> List[Dict[str, Any]]:
    missing = []
    seen = set()
    uploaded_sha1s = uploaded_sha1s or {}
    for f in files or []:
        sha1 = _norm_sha1((f or {}).get('sha1'))
        if not sha1 or sha1 in seen:
            continue
        seen.add(sha1)
        if not uploaded_sha1s.get(sha1):
            missing.append({'sha1': sha1, 'file_name': (f or {}).get('file_name') or (f or {}).get('name') or sha1})
    return missing


def _backfill_center_raw_repair_queue(limit: int = 200) -> Dict[str, Any]:
    client = SharedCenterClient()
    result = {'checked': 0, 'prepared': 0, 'uploaded': 0, 'failed': 0, 'missing_local': 0, 'errors': []}
    try:
        client.scan_raw_repair_queue(limit=200000)
    except Exception as e:
        result['scan_error'] = str(e)
    try:
        resp = client.my_raw_repair_queue(limit=limit) or {}
    except Exception as e:
        result['error'] = str(e)
        return result

    items = resp.get('items') or []
    result['checked'] = len(items)
    if not items:
        return result

    files = []
    for item in items:
        sha1 = _norm_sha1((item or {}).get('sha1'))
        if not sha1:
            continue
        try:
            cache = P115CacheManager.get_file_cache_by_sha1(sha1) or {}
        except Exception as e:
            result['errors'].append({'sha1': sha1, 'error': str(e)})
            continue
        if not cache:
            result['missing_local'] += 1
            continue
        cache = dict(cache)
        cache.setdefault('sha1', sha1)
        cache.setdefault('file_name', cache.get('name') or sha1)
        cache.setdefault('fid', cache.get('id') or '')
        cache.setdefault('file_id', cache.get('id') or '')
        cache.setdefault('pc', cache.get('pick_code') or '')
        files.append(cache)

    result['prepared'] = len(files)
    if not files:
        return result

    uploaded = _upload_raw_batch(client, files)
    result['uploaded'] = int(uploaded.get('uploaded_count') if uploaded.get('uploaded_count') is not None else uploaded.get('count') or 0)
    errors = uploaded.get('errors') or []
    result['failed'] = len(errors)
    result['errors'].extend(errors[:20])
    return result


def _files_missing_pick_code(files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    missing = []
    for f in files or []:
        pc = str((f or {}).get('pick_code') or (f or {}).get('pickcode') or (f or {}).get('pc') or '').strip()
        if pc:
            continue
        missing.append({
            'sha1': _norm_sha1((f or {}).get('sha1')),
            'file_name': (f or {}).get('file_name') or (f or {}).get('name') or '',
        })
    return missing


def _missing_pick_code_reject_result(files: List[Dict[str, Any]]) -> Dict[str, Any]:
    missing = _files_missing_pick_code(files)
    names = '、'.join([str(x.get('file_name') or x.get('sha1') or 'unknown') for x in missing[:5]])
    if len(missing) > 5:
        names += f" 等 {len(missing)} 个"
    return {
        'ok': False,
        'message': f'缺少 115 pick_code，已拒绝登记中心：{names}',
        'missing_pick_code': missing,
        'fingerprint_repair': {},
    }


def _file_payload_common(file_info: Dict[str, Any], raw_uploaded: bool = False, animation_meta: Dict[str, Any] = None) -> Dict[str, Any]:
    raw = _raw_for_file(file_info) if raw_uploaded else {}
    sig = _media_signature(raw, file_info) if raw else {}
    preid = _ensure_file_preid(file_info)
    # size 不能只信 p115_filesystem_cache。旧库补齐 RAW 时，
    # cache 可能缺 size，但 RAW 里通常有 MediaSourceInfo.Size / format.size。
    # 如果这里写 None，中心端整季 total_size 会被 0 或单集大小污染。
    final_size = _file_size_from_cache(file_info) or (_infer_size_from_raw(raw) if raw else 0) or 0
    if final_size > 0:
        file_info['size'] = final_size
    rapid_meta = {
        'fid': file_info.get('fid') or file_info.get('file_id') or '',
        'pick_code': file_info.get('pick_code') or file_info.get('pc') or '',
        'relative_path': file_info.get('relative_path') or '',
        'preid': preid or '',
    }
    quality = normalize_quality_source(
        file_info.get('quality')
        or file_info.get('quality_source')
        or file_info.get('source')
        or (sig.get('quality') if isinstance(sig, dict) else '')
        or extract_quality_source_from_filename(file_info.get('file_name') or file_info.get('name') or '')
    )
    if quality in ('未知', '鏈煡'):
        quality = ''
    return {
        'sha1': _norm_sha1(file_info.get('sha1')),
        'preid': preid or None,
        'size': final_size or None,
        'file_name': file_info.get('file_name') or file_info.get('name') or '',
        'quality': quality,
        'has_raw_ffprobe': bool(raw_uploaded),
        'media_signature_json': sig,
        'rapid_meta_json': rapid_meta,
    }


def _center_episode_air_date(*values: Any) -> str:
    for value in values:
        text = str(value or '').strip()
        if not text:
            continue
        match = re.match(r'^(\d{4}-\d{2}-\d{2})', text)
        if match:
            return match.group(1)
    return ''


# 完结季纯净版识别：只在登记 completed_season_source 时执行。
# 秒传消费端不再现场兜底识别，只信中心端保存的 is_clean_version 标签。
_CLEAN_VERSION_MIN_DELTA_MINUTES = 2.5
_CLEAN_VERSION_MAX_RUNTIME_RATIO = 0.94
_CLEAN_VERSION_MIN_COMPARABLE_EPISODES = 2
_CLEAN_VERSION_HIT_RATIO = 0.70

# 短剧：单个视频实际时长低于 25 分钟。短剧片头更短，纯净版阈值单独收窄到约 1 分钟。
_SHORT_DRAMA_MAX_RUNTIME_MINUTES = 25.0
_SHORT_DRAMA_HIT_RATIO = 0.70
_CLEAN_VERSION_SHORT_DRAMA_MIN_DELTA_MINUTES = 1.0
_CLEAN_VERSION_SHORT_DRAMA_MAX_RUNTIME_RATIO = 0.98


def _runtime_minutes_from_ticks(value) -> float:
    try:
        if value in (None, '', 0, '0'):
            return 0.0
        return float(value) / 600000000.0
    except Exception:
        return 0.0


def _runtime_minutes_from_seconds(value) -> float:
    try:
        if value in (None, '', 0, '0'):
            return 0.0
        return float(value) / 60.0
    except Exception:
        return 0.0


def _physical_runtime_minutes_from_raw(raw: Dict[str, Any]) -> float:
    if not isinstance(raw, dict):
        return 0.0
    msi = raw.get('MediaSourceInfo') if isinstance(raw.get('MediaSourceInfo'), dict) else {}
    runtime = _runtime_minutes_from_ticks(msi.get('RunTimeTicks'))
    if runtime > 0:
        return runtime
    runtime = _runtime_minutes_from_ticks(raw.get('RunTimeTicks'))
    if runtime > 0:
        return runtime
    fmt = raw.get('format') if isinstance(raw.get('format'), dict) else {}
    runtime = _runtime_minutes_from_seconds(fmt.get('duration'))
    if runtime > 0:
        return runtime
    for stream in raw.get('streams') or []:
        if not isinstance(stream, dict):
            continue
        runtime = _runtime_minutes_from_seconds(stream.get('duration'))
        if runtime > 0:
            return runtime
    return 0.0



_SHORT_DRAMA_GENRE_CACHE: Dict[str, bool] = {}


def _json_list(value: Any) -> List[Any]:
    if value in (None, '', []):
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return [value]
    if isinstance(value, dict):
        if isinstance(value.get('genres'), list):
            return value.get('genres') or []
        return [value]
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def _genres_include_animation(genres_json: Any) -> bool:
    """判断 TMDb genres_json 是否包含动画。

    兼容常见格式：[{"id": 16, "name": "动画"}]、[{"id": 16, "name": "Animation"}]、
    以及老数据里可能出现的纯字符串数组。只有命中动画类型时才拦截短剧判断，
    genres_json 缺失时不扩大误杀。
    """
    for item in _json_list(genres_json):
        values = []
        if isinstance(item, dict):
            for key in ('id', 'genre_id', 'tmdb_id'):
                if item.get(key) not in (None, ''):
                    values.append(item.get(key))
            for key in ('name', 'Name', 'title', 'Title', 'zh_name', 'en_name'):
                if item.get(key) not in (None, ''):
                    values.append(item.get(key))
        else:
            values.append(item)
        for value in values:
            text = str(value or '').strip()
            if not text:
                continue
            if text == '16':
                return True
            low = text.lower()
            if low in {'animation', 'animated'} or '动画' in text or '動漫' in text or '动漫' in text:
                return True
    return False


def _short_drama_source_has_animation_genre(source_info: Dict[str, Any]) -> bool:
    """短剧识别前置门禁：genres_json 只要包含动画，就不允许打短剧标签。"""
    source_info = source_info if isinstance(source_info, dict) else {}

    # 先吃调用方已经带进来的完整媒体行，避免不必要查库。
    for value in (
        source_info.get('genres_json'),
        (source_info.get('raw_json') or {}).get('genres_json') if isinstance(source_info.get('raw_json'), dict) else None,
        ((source_info.get('raw_json') or {}).get('media_row') or {}).get('genres_json')
        if isinstance(source_info.get('raw_json'), dict) and isinstance((source_info.get('raw_json') or {}).get('media_row'), dict)
        else None,
    ):
        if _genres_include_animation(value):
            return True

    item_type = str(source_info.get('item_type') or source_info.get('share_item_type') or '').strip()
    tmdb_id = str(source_info.get('tmdb_id') or source_info.get('share_tmdb_id') or '').strip()
    parent = str(source_info.get('parent_series_tmdb_id') or source_info.get('series_tmdb_id') or '').strip()
    if item_type in ('Series', 'Season', 'Episode') and not parent:
        parent = tmdb_id
    if not tmdb_id and parent:
        tmdb_id = parent
    if not tmdb_id and not parent:
        return False

    cache_key = f"{item_type}|{tmdb_id}|{parent}"
    if cache_key in _SHORT_DRAMA_GENRE_CACHE:
        return bool(_SHORT_DRAMA_GENRE_CACHE.get(cache_key))

    try:
        clauses = []
        args = []
        if item_type == 'Movie' and tmdb_id:
            clauses.append("(tmdb_id=%s AND item_type='Movie')")
            args.append(tmdb_id)
        else:
            # 剧集/季/集都以 Series genres_json 为准；同时兼容 Season/Episode 行自身带 genres_json 的情况。
            if parent:
                clauses.append("(tmdb_id=%s AND item_type='Series')")
                args.append(parent)
                clauses.append("(parent_series_tmdb_id=%s AND item_type IN ('Season','Episode'))")
                args.append(parent)
            if tmdb_id and tmdb_id != parent:
                clauses.append("(tmdb_id=%s AND item_type IN ('Series','Season','Episode'))")
                args.append(tmdb_id)
        if not clauses:
            _SHORT_DRAMA_GENRE_CACHE[cache_key] = False
            return False
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT genres_json
                    FROM media_metadata
                    WHERE ({' OR '.join(clauses)})
                      AND genres_json IS NOT NULL
                    LIMIT 30
                    """,
                    args,
                )
                hit = any(_genres_include_animation((row or {}).get('genres_json')) for row in (cur.fetchall() or []))
                _SHORT_DRAMA_GENRE_CACHE[cache_key] = bool(hit)
                return bool(hit)
    except Exception as e:
        logger.debug(
            "  ➜ [共享资源] 检查短剧动画类型门禁失败，按非动画继续: tmdb=%s, parent=%s, item_type=%s, err=%s",
            tmdb_id,
            parent,
            item_type,
            e,
        )
        _SHORT_DRAMA_GENRE_CACHE[cache_key] = False
        return False


def _animation_meta_for_candidate(candidate: Dict[str, Any]) -> Dict[str, Any]:
    """生成中心资源库动漫标签。

    复用短剧动画门禁的 TMDb genres_json 判断：只要媒体类型包含 TMDb 动画类型
    id=16 / Animation / 动画 / 动漫，就给中心源写入 is_animation。
    标签写进 media_signature_json / rapid_meta_json，避免中心端额外建列。
    """
    hit = _short_drama_source_has_animation_genre(candidate or {})
    return {
        'is_animation': bool(hit),
        'animation_checked': True,
        'reason': 'tmdb_genres_animation' if hit else 'tmdb_genres_not_animation',
        'source': 'local_media_metadata.genres_json',
        'genres_json_contains_animation': bool(hit),
    }


def _apply_animation_tag(meta: Dict[str, Any], animation_meta: Dict[str, Any] = None) -> Dict[str, Any]:
    out = dict(meta or {})
    animation_meta = animation_meta if isinstance(animation_meta, dict) else {}
    if animation_meta.get('animation_checked'):
        out['is_animation'] = bool(animation_meta.get('is_animation'))
        out['animation_meta_json'] = animation_meta
    labels = list(out.get('tag_labels') or []) if isinstance(out.get('tag_labels'), list) else []
    if animation_meta.get('is_animation') and '动漫' not in labels:
        labels.append('动漫')
    if labels:
        out['tag_labels'] = labels
    return out


def _apply_completed_certified_tag(meta: Dict[str, Any], completed_meta: Dict[str, Any] = None) -> Dict[str, Any]:
    out = dict(meta or {})
    completed_meta = completed_meta if isinstance(completed_meta, dict) else {}
    if completed_meta.get('is_completed_certified'):
        out['is_completed_certified'] = True
        out['is_completed'] = True
        out['completed_certified_meta_json'] = completed_meta
        labels = list(out.get('tag_labels') or []) if isinstance(out.get('tag_labels'), list) else []
        if '已完结' not in labels:
            labels.append('已完结')
        out['tag_labels'] = labels
    return out


def _short_drama_meta_from_runtime(
    runtime_minutes: float,
    *,
    source: str = 'raw_ffprobe',
    animation_genre: bool = False,
) -> Dict[str, Any]:
    runtime = float(runtime_minutes or 0)
    checked = runtime > 0
    if animation_genre:
        return {
            'is_short_drama': False,
            'short_drama_checked': True,
            'reason': 'animation_genre_skipped',
            'runtime_minutes': round(runtime, 2) if checked else None,
            'max_runtime_minutes': _SHORT_DRAMA_MAX_RUNTIME_MINUTES,
            'runtime_source': source,
            'genre_gate_checked': True,
            'genre_gate_passed': False,
            'genres_json_contains_animation': True,
        }
    is_short = bool(checked and runtime < _SHORT_DRAMA_MAX_RUNTIME_MINUTES)
    return {
        'is_short_drama': is_short,
        'short_drama_checked': checked,
        'reason': 'runtime_lt_25min' if is_short else ('runtime_not_short_enough' if checked else 'missing_runtime'),
        'runtime_minutes': round(runtime, 2) if checked else None,
        'max_runtime_minutes': _SHORT_DRAMA_MAX_RUNTIME_MINUTES,
        'runtime_source': source,
        'genre_gate_checked': True,
        'genre_gate_passed': True,
        'genres_json_contains_animation': False,
    }


def _short_drama_meta_from_raw(raw: Dict[str, Any], source_info: Dict[str, Any] = None) -> Dict[str, Any]:
    runtime = _physical_runtime_minutes_from_raw(raw)
    checked = runtime > 0
    if not _source_is_tv_for_runtime_tags(source_info or {}):
        return {
            'is_short_drama': False,
            'short_drama_checked': False,
            'reason': 'non_tv_skipped',
            'runtime_minutes': round(runtime, 2) if checked else None,
            'max_runtime_minutes': _SHORT_DRAMA_MAX_RUNTIME_MINUTES,
            'runtime_source': 'raw_ffprobe',
            'tv_type_required': True,
            'genre_gate_checked': False,
            'genre_gate_passed': None,
            'genres_json_contains_animation': False,
        }
    return _short_drama_meta_from_runtime(
        runtime,
        source='raw_ffprobe',
        animation_genre=_short_drama_source_has_animation_genre(source_info or {}),
    )


def _apply_short_drama_meta(summary: Dict[str, Any], raw: Dict[str, Any], source_info: Dict[str, Any] = None) -> Dict[str, Any]:
    summary = summary if isinstance(summary, dict) else {}
    meta = _short_drama_meta_from_raw(raw, source_info=source_info)
    if meta.get('runtime_minutes') is not None:
        summary['duration_minutes'] = meta.get('runtime_minutes')
    summary['is_short_drama'] = bool(meta.get('is_short_drama'))
    summary['short_drama_meta_json'] = meta
    return summary


def _apply_runtime_meta(summary: Dict[str, Any], raw: Dict[str, Any]) -> Dict[str, Any]:
    summary = summary if isinstance(summary, dict) else {}
    runtime = _physical_runtime_minutes_from_raw(raw)
    if runtime > 0:
        summary['duration_minutes'] = round(runtime, 2)
    return summary


def _detect_short_drama_for_completed_season(candidate: Dict[str, Any], completed_files: List[Dict[str, Any]], source_files: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not _source_is_tv_for_runtime_tags(candidate or {}):
        return {
            'is_short_drama': False,
            'short_drama_checked': False,
            'reason': 'non_tv_skipped',
            'comparable_count': 0,
            'max_runtime_minutes': _SHORT_DRAMA_MAX_RUNTIME_MINUTES,
            'tv_type_required': True,
            'genre_gate_checked': False,
            'genre_gate_passed': None,
            'genres_json_contains_animation': False,
        }
    if _short_drama_source_has_animation_genre(candidate or {}):
        return {
            'is_short_drama': False,
            'short_drama_checked': True,
            'reason': 'animation_genre_skipped',
            'comparable_count': 0,
            'max_runtime_minutes': _SHORT_DRAMA_MAX_RUNTIME_MINUTES,
            'genre_gate_checked': True,
            'genre_gate_passed': False,
            'genres_json_contains_animation': True,
        }

    raw_by_sha1 = {}
    for f in source_files or []:
        sha1 = _norm_sha1(f.get('sha1'))
        if sha1 and sha1 not in raw_by_sha1:
            raw_by_sha1[sha1] = _raw_for_file(f)

    rows = []
    for item in completed_files or []:
        sha1 = _norm_sha1(item.get('sha1'))
        raw = raw_by_sha1.get(sha1) or {}
        runtime = _physical_runtime_minutes_from_raw(raw)
        if runtime <= 0:
            continue
        rows.append({
            'episode_number': _safe_int(item.get('episode_number'), 0),
            'runtime_minutes': round(runtime, 2),
            'short_drama_hit': bool(runtime < _SHORT_DRAMA_MAX_RUNTIME_MINUTES),
            'file_name': item.get('file_name') or '',
            'sha1': sha1,
        })

    comparable = len(rows)
    if comparable <= 0:
        return {
            'is_short_drama': False,
            'short_drama_checked': False,
            'reason': 'missing_runtime',
            'comparable_count': 0,
            'max_runtime_minutes': _SHORT_DRAMA_MAX_RUNTIME_MINUTES,
            'genre_gate_checked': True,
            'genre_gate_passed': True,
            'genres_json_contains_animation': False,
        }

    hit_count = len([x for x in rows if x.get('short_drama_hit')])
    required_hits = max(1, int(math.ceil(comparable * _SHORT_DRAMA_HIT_RATIO)))
    avg_runtime = sum(float(x.get('runtime_minutes') or 0) for x in rows) / comparable
    is_short = hit_count >= required_hits
    return {
        'is_short_drama': bool(is_short),
        'short_drama_checked': True,
        'reason': 'majority_runtime_lt_25min' if is_short else 'runtime_not_short_enough',
        'comparable_count': comparable,
        'hit_count': hit_count,
        'required_hits': required_hits,
        'avg_runtime_minutes': round(avg_runtime, 2),
        'max_runtime_minutes': _SHORT_DRAMA_MAX_RUNTIME_MINUTES,
        'hit_ratio': _SHORT_DRAMA_HIT_RATIO,
        'genre_gate_checked': True,
        'genre_gate_passed': True,
        'genres_json_contains_animation': False,
        'episodes': rows[:80],
    }


def _load_local_runtime_map_for_season(parent_series_tmdb_id: str, season_number) -> Dict[int, float]:
    parent_series_tmdb_id = str(parent_series_tmdb_id or '').strip()
    season = _safe_int_or_none(season_number)
    if not parent_series_tmdb_id or season is None:
        return {}
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT episode_number, runtime_minutes
                    FROM media_metadata
                    WHERE item_type='Episode'
                      AND parent_series_tmdb_id=%s
                      AND season_number=%s
                      AND runtime_minutes IS NOT NULL
                      AND runtime_minutes > 0
                    ORDER BY episode_number ASC
                    """,
                    (parent_series_tmdb_id, season),
                )
                out = {}
                for row in cur.fetchall() or []:
                    ep = _safe_int(row.get('episode_number'), 0)
                    runtime = float(row.get('runtime_minutes') or 0)
                    if ep > 0 and runtime > 0:
                        out[ep] = runtime
                return out
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 读取本地 TMDb 分集时长失败: tmdb={parent_series_tmdb_id}, S{season}, err={e}")
        return {}


def _tmdb_api_key_for_clean_detect() -> str:
    candidates = []
    for attr in (
        'CONFIG_OPTION_TMDB_API_KEY', 'CONFIG_OPTION_TMDB_APIKEY', 'CONFIG_OPTION_TMDB_KEY',
        'CONFIG_OPTION_TMDB_V3_API_KEY', 'CONFIG_OPTION_TMDB_API_TOKEN',
    ):
        key = getattr(constants, attr, None)
        if key:
            candidates.append(key)
    candidates.extend(['tmdb_api_key', 'tmdb_key', 'TMDB_API_KEY', 'themoviedb_api_key'])
    for key in candidates:
        val = (config_manager.APP_CONFIG or {}).get(key)
        if val:
            return str(val).strip()
    return ''


def _load_tmdb_runtime_map_for_season(parent_series_tmdb_id: str, season_number) -> Dict[int, float]:
    parent_series_tmdb_id = str(parent_series_tmdb_id or '').strip()
    try:
        series_id = int(float(parent_series_tmdb_id))
        season = int(float(season_number))
    except Exception:
        return {}
    api_key = _tmdb_api_key_for_clean_detect()
    if not api_key:
        logger.warning("  ➜ [共享资源] 未配置 TMDb API Key，手动完结季无法实时识别纯净版。")
        return {}
    try:
        data = tmdb_handler.get_season_details_tmdb(
            tv_id=series_id,
            season_number=season,
            api_key=api_key,
            append_to_response=None,
        )
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 手动完结季实时查询 TMDb 时长失败: tv={series_id}, S{season}, err={e}")
        return {}
    out: Dict[int, float] = {}
    for ep in (data or {}).get('episodes') or []:
        if not isinstance(ep, dict):
            continue
        try:
            ep_no = int(ep.get('episode_number'))
            runtime = float(ep.get('runtime') or 0)
            if ep_no > 0 and runtime > 0:
                out[ep_no] = runtime
        except Exception:
            continue
    return out


def _is_manual_clean_detect_source(source_provider: str, candidate: Dict[str, Any]) -> bool:
    text = ' '.join([
        str(source_provider or ''),
        str((candidate or {}).get('source_provider') or ''),
        str((candidate or {}).get('register_source') or ''),
    ]).lower()
    return 'manual' in text or '手动' in text


def _detect_clean_version_for_completed_season(
    candidate: Dict[str, Any],
    completed_files: List[Dict[str, Any]],
    source_files: List[Dict[str, Any]],
    *,
    source_provider: str = '',
) -> Dict[str, Any]:
    """登记完结季时识别纯净版。

    口径：
    - 连载季不调用本函数；
    - 动漫剧集不做纯净版判断，命中 TMDb 动画类型后直接跳过；
    - 自动/完结任务使用本地 media_metadata.runtime_minutes；
    - 手动共享完结季实时查询 TMDb 季详情；
    - 只生成中心端标签，消费端不再二次兜底识别。
    """
    candidate = dict(candidate or {})
    if not _source_is_tv_for_runtime_tags(candidate):
        return {
            'is_clean_version': False,
            'clean_version_checked': False,
            'reason': 'non_tv_skipped',
            'tv_type_required': True,
        }
    parent = str(candidate.get('parent_series_tmdb_id') or candidate.get('series_tmdb_id') or candidate.get('tmdb_id') or '').strip()
    season = _safe_int_or_none(candidate.get('season_number'))
    if not parent or season is None:
        return {'is_clean_version': False, 'clean_version_checked': False, 'reason': 'missing_identity'}

    animation_genre = _short_drama_source_has_animation_genre(candidate)
    if animation_genre:
        return {
            'is_clean_version': False,
            'clean_version_checked': False,
            'clean_version_skipped': True,
            'reason': 'animation_genre_skipped',
            'parent_series_tmdb_id': parent,
            'season_number': season,
            'animation_genre_checked': True,
            'genres_json_contains_animation': True,
        }

    manual = _is_manual_clean_detect_source(source_provider, candidate)
    runtime_map = _load_tmdb_runtime_map_for_season(parent, season) if manual else _load_local_runtime_map_for_season(parent, season)
    runtime_source = 'tmdb_realtime' if manual else 'local_media_metadata'
    if not runtime_map:
        return {
            'is_clean_version': False,
            'clean_version_checked': False,
            'reason': 'missing_official_runtime',
            'runtime_source': runtime_source,
            'parent_series_tmdb_id': parent,
            'season_number': season,
        }

    raw_by_sha1 = {}
    for f in source_files or []:
        sha1 = _norm_sha1(f.get('sha1'))
        if sha1 and sha1 not in raw_by_sha1:
            raw_by_sha1[sha1] = _raw_for_file(f)

    by_episode: Dict[int, Dict[str, Any]] = {}
    for item in completed_files or []:
        ep_no = _safe_int(item.get('episode_number'), 0)
        if ep_no <= 0 or ep_no not in runtime_map:
            continue
        sha1 = _norm_sha1(item.get('sha1'))
        raw = raw_by_sha1.get(sha1) or {}
        actual = _physical_runtime_minutes_from_raw(raw)
        official = float(runtime_map.get(ep_no) or 0)
        if actual <= 0 or official <= 0:
            continue
        current = by_episode.get(ep_no)
        if current is None or actual < float(current.get('actual_runtime_minutes') or 0):
            by_episode[ep_no] = {
                'episode_number': ep_no,
                'official_runtime_minutes': round(official, 2),
                'actual_runtime_minutes': round(actual, 2),
                'delta_minutes': round(official - actual, 2),
                'file_name': item.get('file_name') or '',
                'sha1': sha1,
            }

    episode_rows = sorted(by_episode.values(), key=lambda x: x.get('episode_number') or 0)
    comparable = len(episode_rows)
    if comparable < _CLEAN_VERSION_MIN_COMPARABLE_EPISODES:
        return {
            'is_clean_version': False,
            'clean_version_checked': False,
            'reason': 'not_enough_comparable_episodes',
            'runtime_source': runtime_source,
            'parent_series_tmdb_id': parent,
            'season_number': season,
            'comparable_count': comparable,
        }

    short_hits = [ep for ep in episode_rows if 0 < float(ep.get('actual_runtime_minutes') or 0) < _SHORT_DRAMA_MAX_RUNTIME_MINUTES]
    short_required_hits = max(1, int(math.ceil(comparable * _SHORT_DRAMA_HIT_RATIO)))
    is_short_drama = bool((not animation_genre) and len(short_hits) >= short_required_hits)
    min_delta = _CLEAN_VERSION_SHORT_DRAMA_MIN_DELTA_MINUTES if is_short_drama else _CLEAN_VERSION_MIN_DELTA_MINUTES
    max_runtime_ratio = _CLEAN_VERSION_SHORT_DRAMA_MAX_RUNTIME_RATIO if is_short_drama else _CLEAN_VERSION_MAX_RUNTIME_RATIO

    hits = []
    for ep in episode_rows:
        official = float(ep.get('official_runtime_minutes') or 0)
        actual = float(ep.get('actual_runtime_minutes') or 0)
        delta = float(ep.get('delta_minutes') or 0)
        ratio = (actual / official) if official > 0 else 1.0
        ep['runtime_ratio'] = round(ratio, 4)
        ep['short_drama_hit'] = bool((not animation_genre) and 0 < actual < _SHORT_DRAMA_MAX_RUNTIME_MINUTES)
        ep['clean_hit'] = bool(delta >= min_delta and ratio <= max_runtime_ratio)
        if ep['clean_hit']:
            hits.append(ep)

    required_hits = max(2, int(math.ceil(comparable * _CLEAN_VERSION_HIT_RATIO)))
    avg_delta = sum(float(ep.get('delta_minutes') or 0) for ep in episode_rows) / comparable if comparable else 0.0
    is_clean = len(hits) >= required_hits
    confidence = round(len(hits) / comparable, 4) if comparable else 0.0
    result = {
        'is_clean_version': bool(is_clean),
        'clean_version_checked': True,
        'reason': 'majority_runtime_shorter' if is_clean else 'runtime_not_short_enough',
        'runtime_source': runtime_source,
        'parent_series_tmdb_id': parent,
        'season_number': season,
        'comparable_count': comparable,
        'hit_count': len(hits),
        'required_hits': required_hits,
        'avg_delta_minutes': round(avg_delta, 2),
        'min_delta_minutes': min_delta,
        'max_runtime_ratio': max_runtime_ratio,
        'hit_ratio': _CLEAN_VERSION_HIT_RATIO,
        'is_short_drama': bool(is_short_drama),
        'short_drama_hit_count': len(short_hits),
        'short_drama_required_hits': short_required_hits,
        'short_drama_max_runtime_minutes': _SHORT_DRAMA_MAX_RUNTIME_MINUTES,
        'short_drama_genre_gate_checked': True,
        'short_drama_genre_gate_passed': not animation_genre,
        'genres_json_contains_animation': bool(animation_genre),
        'clean_version_confidence': confidence,
        'episodes': episode_rows[:80],
    }
    if is_clean:
        logger.info(
            f"  ➜ [共享资源] 完结季识别为疑似纯净版: {candidate.get('title') or parent} "
            f"S{season:02d}, 命中 {len(hits)}/{comparable} 集, 来源={runtime_source}, 平均短 {avg_delta:.1f} 分钟"
        )
    else:
        logger.debug(
            f"  ➜ [共享资源] 完结季纯净版识别未命中: {candidate.get('title') or parent} "
            f"S{season:02d}, 命中 {len(hits)}/{comparable} 集, 来源={runtime_source}"
        )
    return result


def _completed_status_from_files(files: List[Dict[str, Any]], expected_count: int = 0) -> Dict[str, Any]:
    eps = sorted({_safe_int(f.get('episode_number'), 0) for f in files if _safe_int(f.get('episode_number'), 0) > 0})
    if expected_count and len(eps) < expected_count:
        return {'status': 'incomplete', 'message': f'未集齐：{len(eps)}/{expected_count}'}
    signatures = []
    for f in files:
        sig = f.get('media_signature_json') if isinstance(f.get('media_signature_json'), dict) else {}
        if sig.get('resolution') or sig.get('effect_key'):
            signatures.append((sig.get('resolution') or '', sig.get('effect_key') or '', sig.get('codec') or ''))
    if signatures and len(set(signatures)) > 1:
        return {'status': 'inconsistent', 'message': '完结季文件存在分辨率/HDR/编码不一致，暂不作为收藏季源派发'}
    if not files:
        return {'status': 'incomplete', 'message': '没有可登记的视频文件'}
    return {'status': 'available', 'message': '完结季一致性校验通过'}




_COMPLETED_CONSISTENCY_TRANSIENT_REASONS = {'repair_error', 'check_error', 'exception'}


def _completed_season_consistency_gate(candidate: Dict[str, Any], *, log_result: bool = True) -> Dict[str, Any]:
    """完结季登记前的硬门禁。

    连载季不进这个门禁；完结季必须通过 helpers.check_season_consistency，
    未通过就禁止向中心登记 completed_season_source。维护任务可复用本结果，
    对已经登记过但仍不合格的完结季源做中心下架 + 本地删除。
    """
    candidate = _normalize_series_candidate_identity(dict(candidate or {}))
    item_type = str(candidate.get('item_type') or '').strip()
    if item_type != 'Season' or not _candidate_is_completed_season(candidate, source_provider=candidate.get('source_provider') or ''):
        return {'ok': True, 'skipped': True, 'reason': 'not_completed_season'}

    try:
        consistency = shared_share_db.repair_candidate_fingerprints(
            {**candidate, '_require_expected_episode_count': True},
            log_result=log_result,
        )
    except Exception as e:
        consistency = {'ok': False, 'reason': 'check_error', 'message': str(e)}

    if isinstance(consistency, dict) and consistency.get('ok'):
        return {
            'ok': True,
            'reason': consistency.get('reason') or 'passed',
            'message': consistency.get('message') or '完结季一致性校验通过',
            'consistency': consistency,
        }

    reason = str((consistency or {}).get('reason') or 'consistency_failed')
    message = str((consistency or {}).get('message') or reason).strip()
    title = _maintenance_candidate_label(candidate)
    return {
        'ok': False,
        'reason': reason,
        'message': f'完结季一致性校验未通过，禁止登记中心：{title}，{message}',
        'consistency': consistency or {},
        'final_failure': reason not in _COMPLETED_CONSISTENCY_TRANSIENT_REASONS,
    }


def _completed_gate_status_from_consistency(consistency: Dict[str, Any]) -> Dict[str, Any]:
    """把严格一致性结果映射成旧 completed_season_source status 口径。"""
    consistency = consistency if isinstance(consistency, dict) else {}
    if consistency.get('ok'):
        return {'status': 'available', 'message': consistency.get('message') or '完结季一致性校验通过'}
    reason = str(consistency.get('reason') or '')
    if reason in ('episode_count_insufficient', 'expected_episode_count_missing'):
        return {'status': 'incomplete', 'message': consistency.get('message') or '完结季本地集数不足/缺少官方总集数'}
    if reason in ('season_asset_inconsistent', 'asset_details_missing'):
        return {'status': 'inconsistent', 'message': consistency.get('message') or '完结季一致性校验失败'}
    return {'status': 'inconsistent', 'message': consistency.get('message') or '完结季一致性校验异常'}


def _disable_local_episode_sources_for_completed_season(parent_series_tmdb_id: str, season_number, *, center_client: SharedCenterClient = None) -> int:
    """完结季包可用后，停用本机同季单集源。

    连载阶段按 episode_source 供给；一旦本机登记出 available 的 completed_season_source，
    同一设备同一季的单集源就不应继续参与中心公共包，避免中心资源库同时出现单集和完结包。
    """
    parent = str(parent_series_tmdb_id or '').strip()
    try:
        season_no = int(float(season_number))
    except Exception:
        return 0
    if not parent or season_no <= 0:
        return 0

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, center_source_id, status, center_status
                    FROM shared_rapid_sources
                    WHERE source_kind='episode'
                      AND tmdb_id=%s
                      AND season_number=%s
                      AND COALESCE(status, '') NOT IN ('disabled', 'cancelled')
                    ORDER BY id ASC
                    """,
                    (parent, season_no),
                )
                rows = [dict(r) for r in (cur.fetchall() or [])]
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 查询待停用单集源失败: tmdb={parent}, S{season_no}, err={e}")
        return 0

    if not rows:
        return 0

    client = center_client or SharedCenterClient()
    disabled = 0
    for row in rows:
        center_resp = {}
        center_source_id = str(row.get('center_source_id') or '').strip()
        if center_source_id:
            try:
                center_resp = client.disable_source('episode', center_source_id, message='completed season source available') or {}
            except Exception as e:
                center_resp = {'ok': False, 'message': str(e)}
                logger.debug(
                    f"  ➜ [共享资源] 中心停用单集源失败，仍停用本地源: "
                    f"tmdb={parent}, S{season_no}, source={center_source_id}, err={e}"
                )
        try:
            shared_share_db.update_local_source(
                int(row.get('id')),
                status='disabled',
                center_status='disabled',
                disabled_at='NOW()',
                raw_json={'reason': 'completed_season_source_available', 'center_response': center_resp},
            )
            disabled += 1
        except Exception as e:
            logger.debug(f"  ➜ [共享资源] 本地停用单集源失败: id={row.get('id')}, err={e}")

    if disabled:
        logger.info(f"  ➜ [共享资源] 完结季包已可用，已停用第 {season_no} 季的 {disabled} 个单集共享源。")
        logger.debug(f"  ➜ [共享资源] 停用单集源详情：tmdb={parent}, season={season_no}, count={disabled}")
    return disabled


def _candidate_is_completed_season(candidate: Dict[str, Any], *, source_provider: str = '', files: List[Dict[str, Any]] = None) -> bool:
    """判断 Season 是否应该登记为“客户端完结季包”。

    普通手动/自动登记只信 media_metadata.watching_status：
    - Completed：完结季，允许进入 completed_season_source 一致性校验；
    - Watching / Paused / 空值：都视为连载季，只登记分集到公共 season_hub。

    完结季专用入口仍通过 source_provider=rapid_completed_season 强制登记，避免影响追剧完结任务。
    """
    candidate = dict(candidate or {})
    if str(candidate.get('item_type') or '').strip() != 'Season':
        return False
    if _cfg_bool(candidate.get('_force_completed_season'), False):
        return True
    provider = str(source_provider or candidate.get('source_provider') or '').strip().lower()
    if provider == 'rapid_completed_season':
        return True
    watching_status = str(candidate.get('watching_status') or '').strip().lower()
    return watching_status == 'completed'


def _title_looks_invalid_for_center(title: Any, tmdb_id: str = '') -> bool:
    """中心公共标题防污染：空值、纯数字、等于 TMDb ID 都视为无效。"""
    text = str(title or '').strip()
    if not text:
        return True
    if text.lower() in {'none', 'null', 'undefined', 'nan'}:
        return True
    if re.fullmatch(r'\d+', text):
        return True
    tmdb_id = str(tmdb_id or '').strip()
    if tmdb_id and text == tmdb_id:
        return True
    return False


def _strip_season_suffix_from_title(title: str) -> str:
    text = str(title or '').strip()
    if not text:
        return ''
    text = re.sub(r'\s*[-·]\s*S\d{1,3}\s*$', '', text, flags=re.IGNORECASE).strip()
    text = re.sub(r'\s+S\d{1,3}\s*$', '', text, flags=re.IGNORECASE).strip()
    text = re.sub(r'\s*[-·]\s*第\s*\d+\s*季\s*$', '', text).strip()
    return text


def _series_identity_from_db(parent_series_tmdb_id: str, season_number=None) -> Dict[str, Any]:
    """从本地 media_metadata 取公共季标题/年份/官方集数。"""
    parent = str(parent_series_tmdb_id or '').strip()
    if not parent:
        return {}
    season = _safe_int_or_none(season_number)
    out: Dict[str, Any] = {}
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT title, original_title, release_year
                    FROM media_metadata
                    WHERE tmdb_id=%s AND item_type='Series'
                    ORDER BY in_library DESC NULLS LAST, last_updated_at DESC NULLS LAST, date_added DESC NULLS LAST
                    LIMIT 1
                    """,
                    (parent,),
                )
                row = cur.fetchone()
                if row:
                    row = dict(row)
                    title = str(row.get('title') or row.get('original_title') or '').strip()
                    if not _title_looks_invalid_for_center(title, parent):
                        out['title'] = _strip_season_suffix_from_title(title)
                    if row.get('release_year') not in (None, ''):
                        out['release_year'] = row.get('release_year')

                if season is not None:
                    cur.execute(
                        """
                        SELECT title, total_episodes, release_year
                        FROM media_metadata
                        WHERE parent_series_tmdb_id=%s AND item_type='Season' AND season_number=%s
                        ORDER BY in_library DESC NULLS LAST, last_updated_at DESC NULLS LAST, date_added DESC NULLS LAST
                        LIMIT 1
                        """,
                        (parent, season),
                    )
                    season_row = cur.fetchone()
                    if season_row:
                        season_row = dict(season_row)
                        if not out.get('title'):
                            season_title = _strip_season_suffix_from_title(str(season_row.get('title') or '').strip())
                            if not _title_looks_invalid_for_center(season_title, parent):
                                out['title'] = season_title
                        if season_row.get('release_year') not in (None, '') and not out.get('release_year'):
                            out['release_year'] = season_row.get('release_year')
                        total = _safe_int_or_none(season_row.get('total_episodes'))
                        if total and total > 0:
                            out['expected_episode_count'] = total

                    cur.execute(
                        """
                        SELECT COUNT(DISTINCT episode_number)::integer AS n
                        FROM media_metadata
                        WHERE parent_series_tmdb_id=%s
                          AND item_type='Episode'
                          AND season_number=%s
                          AND in_library=TRUE
                          AND episode_number IS NOT NULL
                        """,
                        (parent, season),
                    )
                    cnt = cur.fetchone()
                    count = _safe_int((cnt or {}).get('n'), 0) if cnt else 0
                    if count > 0:
                        out['local_episode_count'] = count
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 查询剧集公共标题失败: tmdb={parent}, season={season}, err={e}")
    return out


def _normalize_series_candidate_identity(candidate: Dict[str, Any]) -> Dict[str, Any]:
    """Season/Episode 登记前统一修正公共剧名，避免中心 hub 被 TMDb 数字标题污染。"""
    candidate = dict(candidate or {})
    item_type = str(candidate.get('item_type') or '').strip()
    if item_type not in {'Season', 'Episode'}:
        return candidate

    parent = str(candidate.get('parent_series_tmdb_id') or candidate.get('series_tmdb_id') or candidate.get('tmdb_id') or '').strip()
    season = candidate.get('season_number')
    identity = _series_identity_from_db(parent, season)
    title = str(candidate.get('title') or candidate.get('series_title') or '').strip()
    if _title_looks_invalid_for_center(title, parent):
        fixed_title = identity.get('title') or ''
        if fixed_title:
            candidate['title'] = fixed_title
            candidate['series_title'] = fixed_title
        elif parent:
            candidate['title'] = f'TMDb{parent}'
    else:
        fixed_title = _strip_season_suffix_from_title(title)
        if fixed_title:
            candidate['title'] = fixed_title
            candidate.setdefault('series_title', fixed_title)

    if not candidate.get('release_year') and identity.get('release_year') not in (None, ''):
        candidate['release_year'] = identity.get('release_year')
    if not candidate.get('expected_episode_count') and identity.get('expected_episode_count'):
        candidate['expected_episode_count'] = identity.get('expected_episode_count')
    if not candidate.get('total_episodes') and identity.get('expected_episode_count'):
        candidate['total_episodes'] = identity.get('expected_episode_count')
    return candidate



def _safe_json_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def _first_display_text(*values: Any) -> str:
    for value in values:
        text = str(value or '').strip()
        if text:
            return text
    return ''


def _person_id_from_credit(value: Dict[str, Any]):
    if not isinstance(value, dict):
        return None
    for key in ('tmdb_person_id', 'person_id', 'id', 'tmdb_id'):
        raw = value.get(key)
        try:
            if raw not in (None, ''):
                n = int(float(raw))
                if n > 0:
                    return n
        except Exception:
            continue
    return None


def _credit_character_text(value: Dict[str, Any]) -> str:
    if not isinstance(value, dict):
        return ''
    raw = value.get('character') or value.get('role') or value.get('role_name') or value.get('character_name') or ''
    if isinstance(raw, list):
        raw = ' / '.join(str(x or '').strip() for x in raw if str(x or '').strip())
    return str(raw or '').strip()


def _credit_order_value(value: Dict[str, Any], default: int = 0) -> int:
    for key in ('order', 'sort_order', 'sort', 'index'):
        try:
            if value.get(key) not in (None, ''):
                return int(float(value.get(key)))
        except Exception:
            pass
    return default


def _lookup_people_for_display(person_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    ids = sorted({int(x) for x in person_ids if x})[:64]
    if not ids:
        return {}
    out: Dict[int, Dict[str, Any]] = {}
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT tmdb_person_id, primary_name, original_name, profile_path
                    FROM person_metadata
                    WHERE tmdb_person_id = ANY(%s)
                    """,
                    (ids,),
                )
                for row in cur.fetchall() or []:
                    item = dict(row)
                    pid = _safe_int(item.get('tmdb_person_id'), 0)
                    if pid > 0:
                        out[pid] = item
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 查询人物展示元数据失败: {e}")
    return out


def _build_display_credits_bundle(meta_row: Dict[str, Any]) -> Dict[str, Any]:
    """从本地媒体元数据提取“前 9 位主演 + 1 位导演”。

    中心端只存轻量展示缓存：人物基础信息进 center_person_metadata，
    角色名/排序进 center_media_credits，不镜像完整 TMDb cast。
    """
    actors_raw = _safe_json_list((meta_row or {}).get('actors_json'))
    directors_raw = _safe_json_list((meta_row or {}).get('directors_json'))

    actor_items = []
    for idx, raw in enumerate(actors_raw):
        if not isinstance(raw, dict):
            continue
        pid = _person_id_from_credit(raw)
        if not pid:
            continue
        actor_items.append((
            _credit_order_value(raw, idx),
            pid,
            raw,
        ))
    actor_items.sort(key=lambda x: x[0])
    actor_items = actor_items[:9]

    director_items = []
    for idx, raw in enumerate(directors_raw):
        if not isinstance(raw, dict):
            continue
        pid = _person_id_from_credit(raw)
        if not pid:
            continue
        director_items.append((_credit_order_value(raw, idx), pid, raw))
    director_items.sort(key=lambda x: x[0])

    person_ids = [x[1] for x in actor_items] + [x[1] for x in director_items]
    person_map = _lookup_people_for_display(person_ids)

    def person_display_name(pid: int, raw: Dict[str, Any]) -> str:
        info = person_map.get(pid) or {}
        return _first_display_text(info.get('primary_name'), raw.get('primary_name'), raw.get('name'), raw.get('actor_name'))

    actor_items = [
        (sort_order, pid, raw)
        for sort_order, pid, raw in actor_items
        if utils.contains_chinese(person_display_name(pid, raw)) and utils.contains_chinese(_credit_character_text(raw))
    ][:9]
    director_items = [
        (sort_order, pid, raw)
        for sort_order, pid, raw in director_items
        if utils.contains_chinese(person_display_name(pid, raw))
    ][:1]

    if not actor_items:
        logger.debug(
            "  ➜ [共享资源] 本地没有可上传的中文演员表，跳过演职员补传：%s",
            (meta_row or {}).get('title') or (meta_row or {}).get('tmdb_id') or 'unknown',
        )
        return {'people_json': [], 'credits_json': []}

    people = []
    credits = []

    def add_person(pid: int, raw: Dict[str, Any]) -> None:
        info = person_map.get(pid) or {}
        people.append({
            'tmdb_person_id': pid,
            'primary_name': person_display_name(pid, raw),
            'original_name': _first_display_text(info.get('original_name'), raw.get('original_name'), raw.get('originalName')),
            'profile_path': _first_display_text(info.get('profile_path'), raw.get('profile_path'), raw.get('profile')),
        })

    seen_people = set()
    for sort_order, pid, raw in actor_items:
        if pid not in seen_people:
            add_person(pid, raw)
            seen_people.add(pid)
        credits.append({
            'tmdb_person_id': pid,
            'credit_type': 'actor',
            'character_name': _credit_character_text(raw),
            'sort_order': sort_order,
        })

    for sort_order, pid, raw in director_items:
        if pid not in seen_people:
            add_person(pid, raw)
            seen_people.add(pid)
        credits.append({
            'tmdb_person_id': pid,
            'credit_type': 'director',
            'character_name': '',
            'sort_order': 1000 + sort_order,
        })

    return {'people_json': people, 'credits_json': credits}


def _display_image_path_for_center(*values: Any) -> str:
    """中心元数据补齐时，图片字段只取本地库第一个非空值。

    本地 media_metadata 里的 poster_path/backdrop_path 已经是标准图片字段，
    这里不再做 TMDb/Emby/局域网路径白名单判断。
    """
    for value in values:
        text = str(value or '').strip()
        if text:
            return text
    return ''


def _display_title_is_season_only(value: Any) -> bool:
    """避免把“第 1 季 / S01 / Season 1”写进 Series 公共壳标题。"""
    text = str(value or '').strip()
    if not text:
        return False
    compact = re.sub(r'\s+', '', text, flags=re.I)
    if re.fullmatch(r'第\d{1,3}季', compact):
        return True
    if re.fullmatch(r'S\d{1,3}', compact, flags=re.I):
        return True
    if re.fullmatch(r'Season\d{1,3}', compact, flags=re.I):
        return True
    if re.fullmatch(r'(?:第)?\d{1,3}(?:季|期)', compact):
        return True
    return False


def _strip_display_season_suffix(value: Any) -> str:
    """Series 壳标题只能是剧名；客户端即使只拿到“剧名 第 X 季”也先剥掉季号。"""
    text = _first_display_text(value)
    if not text:
        return ''
    for pattern in (
        r'\s*(?:[-·—–_]+\s*)?S\d{1,3}\s*$',
        r'\s*(?:[-·—–_]+\s*)?Season\s*\d{1,3}\s*$',
        r'\s*(?:[-·—–_]+\s*)?第\s*\d{1,3}\s*季\s*$',
    ):
        text = re.sub(pattern, '', text, flags=re.I).strip()
    return '' if _display_title_is_season_only(text) else text


def _safe_series_title(*values: Any) -> str:
    for value in values:
        text = _strip_display_season_suffix(value)
        if text:
            return text
    return ''


def _local_display_meta_rows_for_candidate(candidate: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """读取展示元数据源行。剧集拆成 Season 行和 Series 行。

    口径：
    - Movie：电影行。
    - Season：仅供兼容旧完结季源/手动季源登记使用。
    - Episode：不再随单集源上传公共展示元数据；剧元数据与可信总集数由 watchlist_processor 统一补传。
    """
    candidate = candidate if isinstance(candidate, dict) else {}
    item_type = str(candidate.get('item_type') or '').strip()
    tmdb_id = str(candidate.get('tmdb_id') or '').strip()
    series_id = str(candidate.get('parent_series_tmdb_id') or candidate.get('series_tmdb_id') or tmdb_id).strip()
    season_no = _safe_int_or_none(candidate.get('season_number'))
    out = {'movie': {}, 'season': {}, 'series': {}, 'episode': {}}
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                if item_type == 'Movie' and tmdb_id:
                    cur.execute(
                        """
                        SELECT * FROM media_metadata
                        WHERE item_type='Movie' AND tmdb_id=%s
                        ORDER BY last_updated_at DESC NULLS LAST
                        LIMIT 1
                        """,
                        (tmdb_id,),
                    )
                    row = cur.fetchone()
                    if row:
                        out['movie'] = dict(row)
                    return out

                if item_type == 'Series' and series_id:
                    cur.execute(
                        """
                        SELECT * FROM media_metadata
                        WHERE item_type='Series' AND tmdb_id=%s
                        ORDER BY last_updated_at DESC NULLS LAST
                        LIMIT 1
                        """,
                        (series_id,),
                    )
                    row = cur.fetchone()
                    if row:
                        out['series'] = dict(row)
                    return out

                if item_type in ('Season', 'Episode') and series_id:
                    if item_type == 'Episode' and season_no is not None and _safe_int_or_none(candidate.get('episode_number')) is not None:
                        cur.execute(
                            """
                            SELECT * FROM media_metadata
                            WHERE item_type='Episode'
                              AND parent_series_tmdb_id=%s
                              AND season_number=%s
                              AND episode_number=%s
                            ORDER BY last_updated_at DESC NULLS LAST
                            LIMIT 1
                            """,
                            (series_id, season_no, _safe_int_or_none(candidate.get('episode_number'))),
                        )
                        row = cur.fetchone()
                        if row:
                            out['episode'] = dict(row)
                    if season_no is not None:
                        cur.execute(
                            """
                            SELECT * FROM media_metadata
                            WHERE item_type='Season'
                              AND season_number=%s
                              AND (tmdb_id=%s OR parent_series_tmdb_id=%s)
                            ORDER BY CASE WHEN parent_series_tmdb_id=%s THEN 0 ELSE 1 END,
                                     last_updated_at DESC NULLS LAST
                            LIMIT 1
                            """,
                            (season_no, series_id, series_id, series_id),
                        )
                        row = cur.fetchone()
                        if row:
                            out['season'] = dict(row)
                        expected = _safe_int_or_none(
                            (out.get('season') or {}).get('total_episodes')
                            or candidate.get('expected_episode_count')
                            or candidate.get('total_episodes')
                        )
                        if expected and expected > 0:
                            cur.execute(
                                """
                                SELECT release_date
                                FROM media_metadata
                                WHERE item_type='Episode'
                                  AND parent_series_tmdb_id=%s
                                  AND season_number=%s
                                  AND episode_number=%s
                                  AND release_date IS NOT NULL
                                ORDER BY last_updated_at DESC NULLS LAST
                                LIMIT 1
                                """,
                                (series_id, season_no, expected),
                            )
                            row = cur.fetchone()
                            air_date = _center_episode_air_date((row or {}).get('release_date') if row else None)
                            if air_date:
                                out['season']['final_episode_air_date'] = air_date
                                out['season']['last_episode_air_date'] = air_date
                    cur.execute(
                        """
                        SELECT * FROM media_metadata
                        WHERE item_type='Series' AND tmdb_id=%s
                        ORDER BY last_updated_at DESC NULLS LAST
                        LIMIT 1
                        """,
                        (series_id,),
                    )
                    row = cur.fetchone()
                    if row:
                        out['series'] = dict(row)
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 查询本地展示元数据失败: {e}")
    return out


def _center_display_meta_bundle_for_candidate(candidate: Dict[str, Any]) -> Dict[str, Any]:
    """登记时上传“公共媒体壳”元数据，而不是把海报/简介绑在某个资源 source 上。

    口径：
    - Movie：创建/补充 Movie 壳，演职员挂 Movie 壳。
    - Season：仅兼容旧完结季源/手动季源登记。
    - Episode：只补 TMDb 官方集时长 runtime_minutes，不上传标题/海报/简介。
    - 中心端负责按“缺失才补、中文优先”合并，不让某个客户端拥有壳的所有权。
    """
    candidate = candidate if isinstance(candidate, dict) else {}
    item_type = str(candidate.get('item_type') or '').strip()
    rows = _local_display_meta_rows_for_candidate(candidate)

    def bundle_for(meta_items: List[Dict[str, Any]], credits: Dict[str, Any]) -> Dict[str, Any]:
        filtered_items = [
            x for x in (_filter_display_meta_for_center_upload(item) for item in (meta_items or []))
            if x
        ]
        bundle = {
            'display_meta_json': filtered_items[-1] if filtered_items else {},
            'display_meta_items_json': filtered_items,
        }
        bundle.update(credits if isinstance(credits, dict) else {'people_json': [], 'credits_json': []})
        return bundle

    def compact(meta: Dict[str, Any]) -> Dict[str, Any]:
        out = {}
        for k, v in (meta or {}).items():
            if v in (None, '', [], {}):
                continue
            out[k] = v
        return out

    def meta_from_row(*, tmdb_id: str, item_type: str, season_number=None, row: Dict[str, Any] = None,
                      fallback_title: str = '', fallback_year=None, include_series_fields: bool = True) -> Dict[str, Any]:
        row = row if isinstance(row, dict) else {}
        genres = _safe_json_list(row.get('genres_json')) if row and include_series_fields else []
        title_value = _first_display_text(row.get('title'), fallback_title)
        original_title_value = _first_display_text(row.get('original_title'), candidate.get('original_title'))
        if item_type == 'Series':
            title_value = _safe_series_title(row.get('title'), fallback_title, candidate.get('series_title'), candidate.get('name'))
            if _display_title_is_season_only(original_title_value):
                original_title_value = ''
        meta = {
            'tmdb_id': str(tmdb_id or '').strip(),
            'item_type': item_type,
            'season_number': season_number,
            'title': title_value,
            'original_title': original_title_value,
            'overview': _first_display_text(row.get('overview')),  # Season 不拿 Series 简介，由中心详情合并兜底。
            'poster_path': _display_image_path_for_center(row.get('poster_path'), row.get('poster_url'), row.get('image'), row.get('cover')),
            'backdrop_path': _display_image_path_for_center(row.get('backdrop_path'), row.get('backdrop_url'), row.get('background')),
            'release_year': _safe_int_or_none(row.get('release_year')) or _safe_int_or_none(fallback_year),
            'release_date': str(row.get('release_date') or '') or None,
        }
        if item_type == 'Season':
            expected = _safe_int_or_none(row.get('total_episodes') or candidate.get('expected_episode_count') or candidate.get('total_episodes'))
            if expected and expected > 0:
                meta.update({
                    'expected_episode_count': expected,
                    'total_episodes': expected,
                    'episode_count': expected,
                })
            final_air_date = _center_episode_air_date(
                row.get('final_episode_air_date'),
                row.get('last_episode_air_date'),
                candidate.get('final_episode_air_date'),
                candidate.get('last_episode_air_date'),
            )
            if final_air_date:
                meta.update({
                    'final_episode_air_date': final_air_date,
                    'last_episode_air_date': final_air_date,
                })
            for src_key, dst_key in (
                ('watching_status', 'watching_status'),
                ('season_status', 'watching_status'),
                ('watchlist_tmdb_status', 'watchlist_tmdb_status'),
                ('total_episodes_locked', 'episode_count_locked'),
            ):
                value = row.get(src_key) if row.get(src_key) not in (None, '', [], {}) else candidate.get(src_key)
                if value not in (None, '', [], {}):
                    meta[dst_key] = value
            season_status = str(meta.get('watching_status') or '').strip()
            if season_status:
                meta['watchlist_is_airing'] = season_status in ('Watching', 'Paused')
        if include_series_fields:
            meta.update({
                'rating': row.get('rating'),
                'genres_json': genres[:12],
                'original_language': _first_display_text(row.get('original_language'), candidate.get('original_language')),
            })
        return compact(meta)

    if item_type == 'Movie':
        media_tmdb_id = str(candidate.get('tmdb_id') or '').strip()
        if not media_tmdb_id:
            return {}
        movie_row = rows.get('movie') or {}
        movie_meta = meta_from_row(
            tmdb_id=media_tmdb_id,
            item_type='Movie',
            season_number=None,
            row=movie_row,
            fallback_title=candidate.get('title'),
            fallback_year=candidate.get('release_year'),
            include_series_fields=True,
        )
        return bundle_for(
            [movie_meta] if movie_meta else [],
            _build_display_credits_bundle(movie_row) if movie_row else {'people_json': [], 'credits_json': []},
        )

    if item_type == 'Episode':
        series_id = str(candidate.get('parent_series_tmdb_id') or candidate.get('series_tmdb_id') or candidate.get('tmdb_id') or '').strip()
        season_no = _safe_int_or_none(candidate.get('season_number'))
        episode_no = _safe_int_or_none(candidate.get('episode_number'))
        episode_row = rows.get('episode') or {}
        runtime = _safe_int_or_none(episode_row.get('runtime_minutes'))
        if not series_id or season_no is None:
            return {}
        season_row = rows.get('season') or {}
        series_row = rows.get('series') or {}
        series_meta = meta_from_row(
            tmdb_id=series_id,
            item_type='Series',
            season_number=None,
            row=series_row,
            fallback_title=candidate.get('title'),
            fallback_year=candidate.get('release_year'),
            include_series_fields=True,
        ) if (series_row or series_id) else {}
        season_meta = meta_from_row(
            tmdb_id=series_id,
            item_type='Season',
            season_number=season_no,
            row=season_row,
            fallback_title=(season_row or {}).get('title') or candidate.get('title'),
            fallback_year=(season_row or {}).get('release_year') or candidate.get('release_year'),
            include_series_fields=False,
        )
        episode_meta = {}
        if episode_no is not None and runtime is not None:
            episode_meta = {
                'tmdb_id': series_id,
                'item_type': 'Episode',
                'season_number': season_no,
                'episode_number': episode_no,
                'runtime_minutes': runtime,
            }
        # 演职员始终只从 Series 条目取，避免季/集演员污染整剧壳。
        return bundle_for(
            [x for x in (series_meta, season_meta, episode_meta) if x],
            _build_display_credits_bundle(series_row) if series_row else {'people_json': [], 'credits_json': []},
        )

    series_id = str(candidate.get('parent_series_tmdb_id') or candidate.get('series_tmdb_id') or candidate.get('tmdb_id') or '').strip()
    season_no = _safe_int_or_none(candidate.get('season_number'))
    if not series_id:
        return {}

    season_row = rows.get('season') or {}
    series_row = rows.get('series') or {}
    series_meta = meta_from_row(
        tmdb_id=series_id,
        item_type='Series',
        season_number=None,
        row=series_row,
        fallback_title=candidate.get('title'),
        fallback_year=candidate.get('release_year'),
        include_series_fields=True,
    ) if (series_row or series_id) else {}
    season_meta = meta_from_row(
        tmdb_id=series_id,
        item_type='Season',
        season_number=season_no,
        row=season_row,
        fallback_title=(season_row or {}).get('title') or candidate.get('title'),
        fallback_year=(season_row or {}).get('release_year') or candidate.get('release_year'),
        include_series_fields=False,
    ) if season_no is not None else {}

    # 演职员只从 Series 条目取；没有 Series 行就不上传，避免 Season/分集演员污染整剧壳。
    return bundle_for(
        [x for x in (series_meta, season_meta) if x],
        _build_display_credits_bundle(series_row) if series_row else {'people_json': [], 'credits_json': []},
    )

def register_candidate_to_center(candidate: Dict[str, Any], *, source_provider: str = 'manual_rapid', preuploaded_raw_state: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """把本地电影/分集/季登记到 Rapid v2 中心。

    新口径：客户端只登记可秒传的电影/分集资产；Season 也拆成 episode_source
    入中心资产池，由中心端 season_version_groups 负责凑齐逻辑完结季、打“已完结”
    和派发文件列表分享。客户端不再创建/更新 completed_season_source，
    也不再做季包一致性硬校验。
    """
    if not _enabled():
        return {'ok': False, 'message': '共享资源未启用或中心未配置'}
    candidate = _normalize_series_candidate_identity(dict(candidate or {}))
    item_type = str(candidate.get('item_type') or '').strip()
    effective_provider = 'rapid_logical_season' if str(source_provider or '').strip().lower() == 'rapid_completed_season' else source_provider

    adult_reason = _adult_rating_block_reason(candidate)
    if adult_reason:
        logger.warning(f"  ➜ [共享资源] 跳过成人资源登记: {candidate.get('title') or candidate.get('tmdb_id') or 'unknown'}，reason={adult_reason}")
        return _adult_block_result(candidate, adult_reason)

    files = shared_share_db.collect_files_for_candidate(candidate)
    if not files:
        return {
            'ok': False,
            'message': '未找到可共享的视频文件，请先确认 p115_filesystem_cache / media_metadata 已补齐 SHA1、PC 和大小',
            'fingerprint_repair': {},
        }

    # RAW 摘要生成发生在正式登记前，先把候选类型补进 file_info。
    files = _prime_candidate_files_for_registration(candidate, files)
    missing_pick_code = _files_missing_pick_code(files)
    if missing_pick_code:
        logger.warning(
            "  ➜ [共享资源] 跳过缺少 115 pick_code 的资源登记: %s",
            '、'.join([str(x.get('file_name') or x.get('sha1') or 'unknown') for x in missing_pick_code[:5]]),
        )
        return _missing_pick_code_reject_result(files)

    root = shared_share_db.candidate_root_from_files(files)
    client = SharedCenterClient()
    raw_batch_result = preuploaded_raw_state if isinstance(preuploaded_raw_state, dict) else _upload_raw_batch(client, files)
    uploaded_sha1s = raw_batch_result.get('uploaded') or {}
    candidate_sha1s = {_norm_sha1((f or {}).get('sha1')) for f in files if _norm_sha1((f or {}).get('sha1'))}
    fresh_uploaded_sha1s = {_norm_sha1(x) for x in (raw_batch_result.get('fresh_uploaded_sha1s') or []) if _norm_sha1(x)}
    skipped_existing_sha1s = {_norm_sha1(x) for x in (raw_batch_result.get('skipped_existing_sha1s') or []) if _norm_sha1(x)}
    uploaded = len(candidate_sha1s & fresh_uploaded_sha1s) if isinstance(preuploaded_raw_state, dict) else int(raw_batch_result.get('uploaded_count') if raw_batch_result.get('uploaded_count') is not None else (raw_batch_result.get('count') or 0))
    raw_ready_count = len({sha for sha in candidate_sha1s if uploaded_sha1s.get(sha)}) if isinstance(preuploaded_raw_state, dict) else int(raw_batch_result.get('count') or 0)
    skipped_existing_raw = len(candidate_sha1s & skipped_existing_sha1s) if isinstance(preuploaded_raw_state, dict) else int(raw_batch_result.get('skipped_existing') or 0)
    errors = [
        err for err in (raw_batch_result.get('errors') or [])
        if not isinstance(preuploaded_raw_state, dict) or _norm_sha1((err or {}).get('sha1')) in candidate_sha1s
    ]
    raw_missing = _raw_batch_missing_for_files(files, uploaded_sha1s)
    if raw_missing:
        names = '、'.join([str(x.get('file_name') or x.get('sha1')) for x in raw_missing[:5]])
        if len(raw_missing) > 5:
            names += f" 等 {len(raw_missing)} 个"
        return {
            'ok': False,
            'message': f'RAW/summary_json 缺失，已拒绝登记中心：{names}',
            'raw_uploaded_count': uploaded,
            'raw_ready_count': raw_ready_count,
            'raw_skipped_existing': skipped_existing_raw,
            'missing_raw': raw_missing,
            'errors': errors,
            'fingerprint_repair': {},
        }

    display_meta_bundle = _center_display_meta_bundle_for_candidate(candidate) if item_type in ('Movie', 'Season', 'Episode') else {}
    results = []
    tmdb_id = str(candidate.get('parent_series_tmdb_id') or candidate.get('series_tmdb_id') or candidate.get('tmdb_id') or '').strip()
    if item_type == 'Movie':
        tmdb_id = str(candidate.get('tmdb_id') or '').strip()

    for f in files:
        f.setdefault('tmdb_id', tmdb_id)
        if item_type == 'Movie':
            f.setdefault('item_type', 'Movie')
        else:
            f.setdefault('item_type', 'Episode')
            f.setdefault('season_number', candidate.get('season_number'))
        try:
            sha_for_raw = _norm_sha1(f.get('sha1'))
            raw_ok = bool(uploaded_sha1s.get(sha_for_raw))
            common = _file_payload_common(f, raw_uploaded=raw_ok)
            if item_type == 'Movie':
                payload = {
                    'tmdb_id': tmdb_id,
                    'item_type': 'Movie',
                    'title': candidate.get('title'),
                    'release_year': candidate.get('release_year'),
                    'source_provider': effective_provider,
                    **display_meta_bundle,
                    **common,
                }
                resp = client.register_movie_source(payload)
                center_item = resp.get('item') or {}
                local = shared_share_db.upsert_local_source({
                    'source_kind': 'movie', 'center_source_id': center_item.get('source_id'), 'tmdb_id': tmdb_id, 'item_type': 'Movie',
                    'title': candidate.get('title'), 'release_year': candidate.get('release_year'), 'sha1': common.get('sha1'), 'preid': common.get('preid'),
                    'size': common.get('size'), 'file_name': common.get('file_name'), 'source_provider': effective_provider,
                    'root_fid': root.get('root_fid'), 'root_name': root.get('root_name'),
                    'status': 'active', 'center_status': 'reported', 'media_signature_json': common.get('media_signature_json'),
                    'rapid_meta_json': common.get('rapid_meta_json'), 'raw_json': {'candidate': candidate, 'center_response': resp},
                })
                shared_share_db.replace_source_files(local['id'], [{**f, 'raw_ffprobe_uploaded': raw_ok, **common}])
                results.append(resp)
            else:
                ep_no = _safe_int_or_none(f.get('episode_number')) or _safe_int_or_none(candidate.get('episode_number'))
                season_no = _safe_int_or_none(f.get('season_number')) or _safe_int_or_none(candidate.get('season_number'))
                if season_no is None or ep_no is None:
                    errors.append({'file': f.get('file_name'), 'error': '缺少 season_number/episode_number'})
                    continue
                expected_count = _safe_int_or_none(candidate.get('expected_episode_count') or candidate.get('total_episodes') or candidate.get('episode_count'))
                episode_air_date = _center_episode_air_date(
                    f.get('episode_air_date'),
                    f.get('air_date'),
                    f.get('release_date'),
                    f.get('premiere_date'),
                    f.get('PremiereDate'),
                    candidate.get('episode_air_date'),
                    candidate.get('air_date'),
                    candidate.get('release_date'),
                    candidate.get('premiere_date'),
                    candidate.get('PremiereDate'),
                )
                payload = {
                    'tmdb_id': tmdb_id,
                    'item_type': 'Episode',
                    'season_number': season_no,
                    'episode_number': ep_no,
                    'title': candidate.get('title'),
                    'release_year': candidate.get('release_year'),
                    'episode_air_date': episode_air_date or None,
                    'expected_episode_count': expected_count,
                    'source_provider': effective_provider,
                    **display_meta_bundle,
                    **common,
                }
                resp = client.register_episode_source(payload)
                center_item = resp.get('item') or {}
                local = shared_share_db.upsert_local_source({
                    'source_kind': 'episode', 'center_source_id': center_item.get('source_id'), 'tmdb_id': tmdb_id,
                    'item_type': 'Episode', 'season_number': season_no, 'episode_number': ep_no,
                    'title': candidate.get('title'), 'release_year': candidate.get('release_year'), 'sha1': common.get('sha1'), 'preid': common.get('preid'),
                    'size': common.get('size'), 'file_name': common.get('file_name'), 'source_provider': effective_provider,
                    'root_fid': root.get('root_fid'), 'root_name': root.get('root_name'),
                    'status': 'active', 'center_status': 'reported', 'media_signature_json': common.get('media_signature_json'),
                    'rapid_meta_json': common.get('rapid_meta_json'), 'raw_json': {'candidate': candidate, 'center_response': resp},
                })
                shared_share_db.replace_source_files(local['id'], [{**f, 'raw_ffprobe_uploaded': raw_ok, **common}])
                results.append(resp)
        except Exception as e:
            logger.warning(f"  ➜ [共享资源] 登记分集/电影失败: {f.get('file_name') or f.get('sha1')} -> {e}")
            errors.append({'file': f.get('file_name') or f.get('sha1'), 'error': str(e)})

    return {
        'ok': bool(results),
        'registered_count': len(results),
        'raw_uploaded_count': uploaded,
        'raw_ready_count': raw_ready_count,
        'raw_skipped_existing': skipped_existing_raw,
        'completed_season': None,
        'episode_cancelled': 0,
        'errors': errors,
        'root': root,
        'fingerprint_repair': {},
        'message': (
            f"已登记 {len(results)} 个分集/电影源"
            + ("，季资源已交由中心逻辑完结季池聚合" if item_type == 'Season' else '')
        ),
    }


def _candidate_from_emby_item_id(emby_item_id: str) -> Dict[str, Any]:
    emby_item_id = str(emby_item_id or '').strip()
    if not emby_item_id:
        return {}
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM media_metadata
                    WHERE emby_item_ids_json::text ILIKE %s
                    ORDER BY date_added DESC NULLS LAST
                    LIMIT 1
                    """,
                    (f'%{emby_item_id}%',),
                )
                row = cur.fetchone()
                return dict(row) if row else {}
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 按 Emby ID 查询媒体行失败: {emby_item_id} -> {e}")
        return {}


def _library_register_candidate_from_kwargs(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    kwargs = dict(kwargs or {})
    db_row = _candidate_from_emby_item_id(kwargs.get('emby_item_id'))
    item_type = kwargs.get('item_type') or kwargs.get('share_item_type') or db_row.get('item_type')
    candidate = {
        'tmdb_id': kwargs.get('tmdb_id') or db_row.get('tmdb_id') or kwargs.get('parent_series_tmdb_id'),
        'parent_series_tmdb_id': kwargs.get('parent_series_tmdb_id') or db_row.get('parent_series_tmdb_id'),
        'item_type': item_type,
        'season_number': kwargs.get('season_number') if kwargs.get('season_number') not in (None, '') else db_row.get('season_number'),
        'episode_number': kwargs.get('episode_number') if kwargs.get('episode_number') not in (None, '') else db_row.get('episode_number'),
        'title': kwargs.get('title') or kwargs.get('name') or db_row.get('title'),
        'release_year': kwargs.get('year') or kwargs.get('release_year') or db_row.get('release_year'),
        'watching_status': kwargs.get('watching_status') or db_row.get('watching_status'),
        'total_episodes': kwargs.get('total_episodes') or db_row.get('total_episodes'),
        'expected_episode_count': kwargs.get('expected_episode_count') or db_row.get('total_episodes'),
        'official_rating_json': kwargs.get('official_rating_json') or db_row.get('official_rating_json'),
        'custom_rating': kwargs.get('custom_rating') or db_row.get('custom_rating'),
    }
    if candidate.get('item_type') == 'Episode' and db_row.get('tmdb_id'):
        candidate['tmdb_id'] = db_row.get('tmdb_id')
        candidate['parent_series_tmdb_id'] = candidate.get('parent_series_tmdb_id') or kwargs.get('parent_series_tmdb_id')
    return candidate


def trigger_shared_rapid_register_batch_for_library_items(processor=None, register_items: List[Dict[str, Any]] | None = None) -> Dict[str, Any]:
    if not _enabled():
        return {'ok': False, 'created': 0, 'message': '共享资源未启用'}
    items = [dict(x or {}) for x in (register_items or []) if isinstance(x, dict)]
    if not items:
        return {'ok': False, 'created': 0, 'message': '没有可登记的条目'}

    prepared = []
    all_files: List[Dict[str, Any]] = []
    for raw_item in items:
        candidate = _library_register_candidate_from_kwargs(raw_item)
        candidate = _normalize_series_candidate_identity(dict(candidate or {}))
        adult_reason = _adult_rating_block_reason(candidate)
        if adult_reason:
            prepared.append({
                'item': raw_item,
                'candidate': candidate,
                'blocked_result': _adult_block_result(candidate, adult_reason),
            })
            continue
        files = shared_share_db.collect_files_for_candidate(candidate)
        files = _prime_candidate_files_for_registration(candidate, files)
        if files and _files_missing_pick_code(files):
            prepared.append({
                'item': raw_item,
                'candidate': candidate,
                'blocked_result': _missing_pick_code_reject_result(files),
            })
            continue
        prepared.append({'item': raw_item, 'candidate': candidate, 'has_files': bool(files)})
        all_files.extend(files)

    raw_batch_result = _upload_raw_batch(SharedCenterClient(), all_files) if all_files else {
        'ok': True,
        'uploaded': {},
        'count': 0,
        'uploaded_count': 0,
        'skipped_existing': 0,
        'skipped_existing_sha1s': [],
        'fresh_uploaded_sha1s': [],
        'errors': [],
    }

    created_total = 0
    failed_total = 0
    results = []
    for entry in prepared:
        if entry.get('blocked_result'):
            result = entry['blocked_result']
            failed_total += 1
            results.append({
                'item': entry['item'],
                'candidate': entry['candidate'],
                'result': result,
            })
            continue
        try:
            result = register_candidate_to_center(
                entry['candidate'],
                source_provider='rapid_auto_library',
                preuploaded_raw_state=raw_batch_result,
            )
        except Exception as e:
            result = {'ok': False, 'message': f'登记异常: {e}', 'error': str(e)}
        try:
            created_total += int(result.get('created', 0) or result.get('registered_count', 0) or 0)
        except Exception:
            pass
        if not result.get('ok'):
            failed_total += 1
        results.append({
            'item': entry['item'],
            'candidate': entry['candidate'],
            'result': result,
        })

    return {
        'ok': failed_total == 0,
        'created': created_total,
        'failed': failed_total,
        'raw_batch_result': raw_batch_result,
        'items': results,
    }

def trigger_shared_rapid_register_for_library_item(processor=None, **kwargs) -> Dict[str, Any]:
    if not _enabled():
        return {'ok': False, 'created': 0, 'message': '共享资源未启用'}
    candidate = _library_register_candidate_from_kwargs(kwargs)
    result = register_candidate_to_center(candidate, source_provider='rapid_auto_library')
    result['created'] = result.get('registered_count', 0)
    return result


def trigger_shared_auto_share_for_library_item(processor=None, **kwargs) -> Dict[str, Any]:
    """兼容旧调用名：Rapid v2 不创建 115 分享，只登记本地秒传源。"""
    return trigger_shared_rapid_register_for_library_item(processor, **kwargs)


def trigger_completed_season_pack_share_task(processor=None, *, parent_series_tmdb_id: str = '', season_number=None, title: str = '', year: str = '', **kwargs) -> Dict[str, Any]:
    """兼容旧调用名：完结季不再创建客户端季包；只登记分集资产，中心端自动生成逻辑完结季。"""
    parent = str(parent_series_tmdb_id or kwargs.get('tmdb_id') or '').strip()
    if not parent:
        return {'ok': False, 'created': 0, 'message': '缺少父剧 TMDb ID'}
    try:
        season_no = int(float(season_number))
    except Exception:
        return {'ok': False, 'created': 0, 'message': f'无效季号: {season_number}'}
    identity = _series_identity_from_db(parent, season_no)
    raw_title = title or kwargs.get('series_name') or kwargs.get('name') or identity.get('title') or ''
    if _title_looks_invalid_for_center(raw_title, parent):
        raw_title = identity.get('title') or f'TMDb{parent}'
    raw_title = _strip_season_suffix_from_title(raw_title) or raw_title
    expected = kwargs.get('expected_episode_count') or kwargs.get('total_episodes') or identity.get('expected_episode_count')
    candidate = {
        'tmdb_id': parent,
        'parent_series_tmdb_id': parent,
        'series_title': raw_title,
        'item_type': 'Season',
        'season_number': season_no,
        'title': raw_title,
        'release_year': year or kwargs.get('release_year') or identity.get('release_year'),
        'expected_episode_count': expected,
        'total_episodes': expected,
        'watching_status': 'Completed',
    }
    result = register_candidate_to_center(candidate, source_provider='rapid_logical_season')
    result['created'] = result.get('registered_count', 0)
    result.setdefault('episode_cancelled', 0)
    return result


def _task_status(progress: int, message: str) -> None:
    try:
        task_manager.update_status_from_thread(progress, message)
    except Exception:
        pass


def share_all_library(processor=None, max_items: int = 100000) -> Dict[str, Any]:
    """一键登记媒体库。

    扫描阶段通过 progress_callback 持续更新顶部任务栏；登记阶段再逐个更新进度。
    兼容旧调用 share_all_library(1000)，任务系统调用时第一个参数为 processor。
    """
    if isinstance(processor, (int, float)) and max_items == 100000:
        max_items = int(processor)
        processor = None

    if not _enabled():
        _task_status(0, '共享资源未启用，跳过一键登记。')
        return {'ok': False, 'message': '共享资源未启用'}
    if not _FULL_SHARE_LOCK.acquire(blocking=False):
        _task_status(0, '全库登记任务正在运行，本次跳过。')
        return {'ok': False, 'message': '全库登记任务正在运行'}
    try:
        _task_status(1, '正在扫描本地媒体库，并加载已有有效共享索引...')
        logger.info('  ➜ [共享资源] 一键登记媒体库开始：扫描本地媒体库，增量排除已有有效共享。')

        def _scan_progress(progress: int, message: str):
            # 扫描阶段只占用 1%~10%；登记阶段从 10% 推到 95%。
            mapped = max(1, min(10, int(progress or 1)))
            _task_status(mapped, message)

        candidate_stats = shared_share_db.all_library_share_candidates(
            limit=max_items,
            exclude_existing=True,
            return_stats=True,
            progress_callback=_scan_progress,
        )
        candidates = candidate_stats.get('items') or []
        total = len(candidates)
        skipped_existing = int(candidate_stats.get('skipped_existing') or 0)
        skipped_duplicate = int(candidate_stats.get('skipped_duplicate') or 0)
        skipped_season_candidate = int(candidate_stats.get('skipped_season_candidate') or 0)
        scanned = int(candidate_stats.get('scanned') or 0)
        existing_summary = candidate_stats.get('existing_index') or {}
        timings = candidate_stats.get('timings') or {}

        _task_status(10, f'扫描完成：媒体候选 {scanned}，已排除有效共享 {skipped_existing}，已跳过季级条目 {skipped_season_candidate}，待登记 {total}。')
        logger.info(
            '  ➜ [共享资源] 一键登记媒体库扫描完成：扫描 %s，跳过已有有效共享 %s，跳过季级条目 %s，跳过重复候选 %s，待登记 %s，已有索引=%s，耗时=%s',
            scanned, skipped_existing, skipped_season_candidate, skipped_duplicate, total, existing_summary, timings,
        )

        if total <= 0:
            msg = f'无需登记：扫描 {scanned} 个候选，已排除有效共享 {skipped_existing} 个，已跳过季级条目 {skipped_season_candidate} 个。'
            _task_status(100, msg)
            logger.info(f"  ➜ [共享资源] 一键登记媒体库完成：{msg}")
            return {
                'ok': True,
                'total': 0,
                'success': 0,
                'failed': 0,
                'skipped_existing': skipped_existing,
                'skipped_duplicate': skipped_duplicate,
                'skipped_season_candidate': skipped_season_candidate,
                'scanned': scanned,
                'timings': timings,
                'message': msg,
            }

        ok = failed = 0
        items = []
        for idx, cand in enumerate(candidates, 1):
            if processor is not None and hasattr(processor, 'is_stop_requested') and processor.is_stop_requested():
                msg = f'任务已中断：已处理 {idx - 1}/{total}，成功 {ok}，失败 {failed}。'
                _task_status(max(10, min(99, int(10 + ((idx - 1) / max(total, 1)) * 85))), msg)
                logger.info(f"  ➜ [共享资源] 一键登记媒体库中断：{msg}")
                return {
                    'ok': False,
                    'cancelled': True,
                    'total': total,
                    'success': ok,
                    'failed': failed,
                    'skipped_existing': skipped_existing,
                    'skipped_duplicate': skipped_duplicate,
                    'skipped_season_candidate': skipped_season_candidate,
                    'scanned': scanned,
                    'items': items[:50],
                    'timings': timings,
                    'message': msg,
                }

            title = cand.get('title') or cand.get('display_title') or cand.get('tmdb_id') or f'候选 {idx}'
            progress = max(10, min(95, int(10 + ((idx - 1) / max(total, 1)) * 85)))
            _task_status(progress, f'正在登记 {idx}/{total}：{title}（成功 {ok}，失败 {failed}，已跳过 {skipped_existing}）')
            try:
                res = register_candidate_to_center(cand, source_provider='rapid_all_library')
                item = {
                    'title': title,
                    'ok': bool(res.get('ok')),
                    'skipped': False,
                    'reason': res.get('reason') or '',
                    'message': res.get('message') or '',
                }
                if res.get('ok'):
                    ok += 1
                else:
                    failed += 1
                items.append(item)
                if idx % 10 == 0 or idx == total:
                    _task_status(
                        max(10, min(95, int(10 + (idx / max(total, 1)) * 85))),
                        f'一键登记进度：{idx}/{total}，成功 {ok}，失败 {failed}，已跳过 {skipped_existing}。'
                    )
                    logger.info(f"  ➜ [共享资源] 一键登记媒体库进度：{idx}/{total}，成功 {ok}，失败 {failed}，已跳过 {skipped_existing}")
            except Exception as e:
                failed += 1
                items.append({'title': title, 'ok': False, 'message': str(e)})
                logger.warning(f"  ➜ [共享资源] 一键登记媒体库失败: {title} -> {e}")

        msg = f'一键登记完成：扫描 {scanned}，跳过已有有效共享 {skipped_existing}，跳过季级条目 {skipped_season_candidate}，登记成功 {ok}，失败 {failed}。'
        _task_status(100, msg)
        logger.info(f"  ➜ [共享资源] 一键登记媒体库完成：候选 {total}，成功 {ok}，失败 {failed}，跳过季级条目 {skipped_season_candidate}，已跳过 {skipped_existing}")
        return {
            'ok': True,
            'total': total,
            'success': ok,
            'failed': failed,
            'skipped_existing': skipped_existing,
            'skipped_duplicate': skipped_duplicate,
            'skipped_season_candidate': skipped_season_candidate,
            'scanned': scanned,
            'items': items[:50],
            'timings': timings,
            'message': msg,
        }
    finally:
        _FULL_SHARE_LOCK.release()



def _human_title_from_sign_job(job: Dict[str, Any], fallback: str = '该资源') -> str:
    """签名日志面向人展示：只保留片名，机器字段放 DEBUG。"""
    job = job if isinstance(job, dict) else {}
    for key in ('title', 'display_title', 'media_title', 'name', 'file_name'):
        value = str(job.get(key) or '').strip()
        if value:
            text = value
            break
    else:
        text = str(fallback or '').strip()

    text = text.replace('\\', '/').split('/')[-1].strip()
    text = re.sub(r'\.(?:mkv|mp4|ts|m2ts|avi|mov|wmv|flv|rmvb|webm|iso)$', '', text, flags=re.IGNORECASE).strip()

    # 常见中心文件名：片名 (年份) · UHD BluRay · HDR10 ...
    for sep in (' · ', '｜', ' | '):
        if sep in text:
            text = text.split(sep, 1)[0].strip()
            break

    # 兜底：没有“·”时，遇到明显质量标签也截断。避免误砍“片名 - 正片”这类合法标题。
    quality_match = re.search(
        r'\s[-–—]\s(?:UHD|BluRay|WEB[- ]?DL|WEBRip|HDTV|DVDRip|HDR10|DoVi|DV|2160p|1080p|720p)\b',
        text,
        flags=re.IGNORECASE,
    )
    if quality_match:
        text = text[:quality_match.start()].strip()

    text = re.sub(r'\s*[（(](?:19|20)\d{2}[）)]\s*$', '', text).strip()
    text = re.sub(r'\s+', ' ', text).strip(' -–—·|｜_')
    if not text:
        text = str(fallback or '该资源').strip() or '该资源'
    return text[:80] + ('…' if len(text) > 80 else '')


class _SignNoiseFilter(logging.Filter):
    """签名任务里压掉 115 客户端的直链成功明细，避免 INFO 日志刷机器字段。"""
    _NOISE = (
        '成功获取直链 ->',
        '成功获取直链：',
    )

    def filter(self, record):
        try:
            msg = record.getMessage()
            if any(x in msg for x in self._NOISE):
                return False
        except Exception:
            pass
        return True


@contextmanager
def _suppress_sign_noise_logs():
    noise_filter = _SignNoiseFilter()
    target_loggers = [
        logging.getLogger('handler.p115_service'),
        logging.getLogger('p115_service'),
    ]
    for target in target_loggers:
        try:
            target.addFilter(noise_filter)
        except Exception:
            pass
    try:
        yield
    finally:
        for target in target_loggers:
            try:
                target.removeFilter(noise_filter)
            except Exception:
                pass


def poll_and_process_rapid_sign_jobs_once(timeout: int = 1, limit: int = 3) -> Dict[str, Any]:
    """Holder 端处理中心下发的 sign_job。CK/PC 只在本机使用，只回传 sign_val。"""
    if not _enabled():
        return {'ok': False, 'message': '共享资源未启用'}
    client = SharedCenterClient()
    if not client.ready:
        return {'ok': False, 'message': '共享中心未配置'}
    resp = client.poll_rapid_sign_jobs(timeout=timeout, limit=limit)
    jobs = resp.get('items') or []
    if not jobs:
        return {'ok': True, 'count': 0, 'items': []}
    results = []
    for job in jobs:
        job_id = str(job.get('job_id') or '').strip()
        sha1 = _norm_sha1(job.get('sha1'))
        sign_check = str(job.get('sign_check') or '').strip()
        file_name = str(job.get('file_name') or sha1 or '').strip()
        display_title = _human_title_from_sign_job(job, fallback=file_name or sha1 or '该资源')
        logger.info(f"  ➜ [负载均衡签名] 收到中心派发的签名请求：《{display_title}》")
        logger.debug(
            "  ➜ [负载均衡签名] 签名请求详情：job_id=%s, sha1=%s, sign_check=%s, requester=%s, file=%s",
            job_id or '-',
            sha1[:12] + '...' if sha1 else '-',
            sign_check or '-',
            job.get('requester_id') or '-',
            file_name or '-',
        )
        try:
            from handler.p115_service import P115Service
            p115 = P115Service.get_client()
            if not p115 or not hasattr(p115, 'rapid_sign_value'):
                raise RuntimeError('当前 115 客户端不支持 rapid_sign_value')
            with _suppress_sign_noise_logs():
                sign_res = p115.rapid_sign_value(job)
            sign_val = _norm_sha1((sign_res or {}).get('sign_val'))
            if not sign_val:
                raise RuntimeError(f'未计算出合法 sign_val: {sign_res}')
            submit_payload = {
                'status': 'done',
                'sign_val': sign_val,
                'message': 'holder sign ok',
                'byte_len': (sign_res or {}).get('byte_len'),
                'range_start': (sign_res or {}).get('start'),
                'range_end': (sign_res or {}).get('end'),
                'result_meta_json': {
                    'backend': (sign_res or {}).get('backend') or '',
                    'file_name': file_name,
                },
            }
            submit = client.submit_rapid_sign_job(job_id, submit_payload)
            logger.info(f"  ➜ [负载均衡签名] 《{display_title}》的签名已成功获取并回传")
            logger.debug(
                "  ➜ [负载均衡签名] 签名回传详情：job_id=%s, sign_val=%s, bytes=%s",
                job_id or '-',
                sign_val[:12] + '...' if sign_val else '-',
                (sign_res or {}).get('byte_len') or '-',
            )
            results.append({'job_id': job_id, 'ok': True, 'submit': submit})
        except Exception as e:
            logger.warning(f"  ➜ [负载均衡签名] 《{display_title}》的签名获取或回传失败：{e}")
            logger.debug(
                "  ➜ [负载均衡签名] 签名失败详情：job_id=%s, sha1=%s, sign_check=%s",
                job_id or '-',
                sha1[:12] + '...' if sha1 else '-',
                sign_check or '-',
                exc_info=True,
            )
            try:
                err_text = str(e)[:1000]
                stale_holder = any(x in err_text for x in (
                    '本机不是可签名 holder', '未找到 sha1', '未找到 pick_code', '对应 pick_code'
                ))
                submit = client.submit_rapid_sign_job(job_id, {
                    'status': 'failed',
                    'message': err_text,
                    'result_meta_json': {'stale_holder': stale_holder, 'file_name': file_name},
                })
            except Exception as submit_err:
                submit = {'ok': False, 'error': str(submit_err)}
            results.append({'job_id': job_id, 'ok': False, 'error': str(e), 'submit': submit})
    return {'ok': True, 'count': len(jobs), 'items': results}

def _listener_failure_backoff_seconds(consecutive_failures: int, *, base: int) -> int:
    if consecutive_failures <= 0:
        return 0
    if consecutive_failures < _LISTENER_FAILURE_WARN_THRESHOLD:
        return int(base)
    exponent = min(consecutive_failures - _LISTENER_FAILURE_WARN_THRESHOLD, 6)
    return min(_LISTENER_BACKOFF_MAX_SECONDS, int(base) * (2 ** exponent))


def _listener_should_log_failure(consecutive_failures: int) -> bool:
    if consecutive_failures <= _LISTENER_FAILURE_WARN_THRESHOLD:
        return True
    return consecutive_failures in {5, 8, 13, 21, 34, 55, 89}


def _sign_listener_loop():
    """独立处理中心 sign_job。

    不能和资源事件消费共用一个循环：资源事件长轮询可能阻塞 25 秒，
    而请求端正在同步等待 sign_val。签名任务必须独立长轮询，避免 pending
    阶段因为 holder 没及时领取而被中心误判超时。
    """
    logger.debug('  ➜ [共享签名监听] Rapid v2 sign_job 长轮询监听已启动。')
    consecutive_failures = 0
    while not _LISTENER_STOP.is_set():
        try:
            if not _enabled():
                _LISTENER_STOP.wait(5)
                continue
            poll_and_process_rapid_sign_jobs_once(timeout=15, limit=10)
            consecutive_failures = 0
        except Exception as e:
            consecutive_failures += 1
            wait_seconds = _listener_failure_backoff_seconds(consecutive_failures, base=3)
            if _listener_should_log_failure(consecutive_failures):
                if consecutive_failures >= _LISTENER_FAILURE_WARN_THRESHOLD:
                    logger.warning(
                        "  ➜ [共享签名监听] 连续 %s 轮处理失败，暂停 %s 秒后重试: %s",
                        consecutive_failures, wait_seconds, e,
                    )
                else:
                    logger.debug(f"  ➜ [共享签名监听] 本轮处理失败: {e}")
            _LISTENER_STOP.wait(wait_seconds)
    logger.info('  ➜ [共享签名监听] Rapid v2 sign_job 长轮询监听已停止。')


def _event_listener_loop():
    logger.debug('  ➜ [共享事件监听] Rapid v2 长轮询监听已启动。')
    consecutive_failures = 0
    while not _LISTENER_STOP.is_set():
        try:
            if not _enabled():
                _LISTENER_STOP.wait(15)
                continue
            poll_and_consume_once(
                timeout=15,
                limit=5,
                stop_event=_LISTENER_STOP,
                lease_max_wait_seconds=20,
            )
            consecutive_failures = 0
        except Exception as e:
            consecutive_failures += 1
            wait_seconds = _listener_failure_backoff_seconds(consecutive_failures, base=10)
            if _listener_should_log_failure(consecutive_failures):
                if consecutive_failures >= _LISTENER_FAILURE_WARN_THRESHOLD:
                    logger.warning(
                        "  ➜ [共享事件监听] 连续 %s 轮处理失败，暂停 %s 秒后重试: %s",
                        consecutive_failures, wait_seconds, e,
                    )
                else:
                    logger.debug(f"  ➜ [共享事件监听] 本轮处理失败: {e}")
            _LISTENER_STOP.wait(wait_seconds)
    logger.info('  ➜ [共享事件监听] Rapid v2 长轮询监听已停止。')


def ensure_shared_device_event_listener() -> bool:
    global _LISTENER_THREAD, _SIGN_LISTENER_THREAD
    if not _shared_center_runtime_ready(log_missing=True):
        stop_shared_device_event_listener(timeout=0.2)
        return False
    with _LISTENER_LOCK:
        started = False
        _LISTENER_STOP.clear()
        if not (_SIGN_LISTENER_THREAD and _SIGN_LISTENER_THREAD.is_alive()):
            _SIGN_LISTENER_THREAD = threading.Thread(
                target=_sign_listener_loop,
                name='shared-rapid-sign-listener',
                daemon=True,
            )
            _SIGN_LISTENER_THREAD.start()
            started = True
        if not (_LISTENER_THREAD and _LISTENER_THREAD.is_alive()):
            _LISTENER_THREAD = threading.Thread(
                target=_event_listener_loop,
                name='shared-rapid-event-listener',
                daemon=True,
            )
            _LISTENER_THREAD.start()
            started = True
        return True


def stop_shared_device_event_listener(timeout: float = 3.0) -> bool:
    """停止 Rapid v2 中心事件监听线程和签名监听线程。

    web_app.py 在共享开关关闭、配置重载和应用退出时会调用这个函数。
    旧版任务文件没有导出该函数，导致启动阶段导入失败。
    """
    global _LISTENER_THREAD, _SIGN_LISTENER_THREAD
    with _LISTENER_LOCK:
        event_thread = _LISTENER_THREAD
        sign_thread = _SIGN_LISTENER_THREAD
        _LISTENER_STOP.set()
    for thread in (event_thread, sign_thread):
        if thread and thread.is_alive():
            try:
                thread.join(timeout=max(0.1, float(timeout or 0)))
            except Exception:
                pass
    with _LISTENER_LOCK:
        if _LISTENER_THREAD and not _LISTENER_THREAD.is_alive():
            _LISTENER_THREAD = None
        if _SIGN_LISTENER_THREAD and not _SIGN_LISTENER_THREAD.is_alive():
            _SIGN_LISTENER_THREAD = None
        return _LISTENER_THREAD is None and _SIGN_LISTENER_THREAD is None


def ensure_share_request_event_listener() -> bool:
    return ensure_shared_device_event_listener()


def _sync_center_credit() -> Dict[str, Any]:
    client = SharedCenterClient()
    me = client.me()
    stats = client.stats()
    ledger = client.credit_ledger(limit=500)
    display_series = {}
    try:
        display_series = client.list_display_sources(item_type='Series', limit=1, offset=0)
    except Exception as e:
        logger.debug(f"  ➜ [共享资源维护] 拉取中心剧集展示统计失败: {e}")
    movie_source_count = int(stats.get('movie_sources') or 0)
    episode_source_count = int(stats.get('episode_sources') or 0)
    logical_group_count = int(stats.get('logical_season_groups') or 0)
    video_count = movie_source_count + episode_source_count
    raw_media_stats = (stats.get('media_stats') or {}) if isinstance(stats.get('media_stats'), dict) else {}
    display_movie_count = int(stats.get('display_movie_count') or raw_media_stats.get('movie_count') or movie_source_count)
    display_series_count = int(
        display_series.get('total')
        or stats.get('display_series_count')
        or raw_media_stats.get('series_count')
        or stats.get('display_season_count')
        or raw_media_stats.get('season_count')
        or logical_group_count
        or 0
    )
    display_season_count = int(stats.get('display_season_count') or raw_media_stats.get('season_count') or display_series_count or logical_group_count)
    media_stats = {
        **raw_media_stats,
        'movie_count': display_movie_count,
        'series_count': display_series_count,
        'season_count': display_season_count,
        'video_count': video_count,
    }
    enriched_stats = {
        **stats,
        'display_movie_count': display_movie_count,
        'display_series_count': display_series_count,
        'display_season_count': display_season_count,
        'video_count': video_count,
        'media_stats': media_stats,
    }
    snapshot = {
        'device_id': me.get('id'),
        'credit': int(me.get('credit') or 0),
        'wanted_gaps': int(stats.get('share_requests') or stats.get('active_gap_devices') or 0),
        'shared_sources': video_count,
        'raw_ffprobe': int(stats.get('raw_ffprobe') or 0),
        'remote_devices': int(stats.get('online_devices') if stats.get('online_devices') is not None else stats.get('devices') or 0),
        'raw_json': {
            'me': me,
            'stats': {
                **enriched_stats,
                'online_devices': int(stats.get('online_devices') if stats.get('online_devices') is not None else stats.get('devices') or 0),
                'devices': int(stats.get('devices') or 0),
            },
        },
    }
    saved = shared_credit_db.upsert_credit_snapshot(snapshot)
    synced = shared_credit_db.sync_center_credit_ledger(ledger.get('items') or [], device_snapshot=me)
    return {'snapshot': saved, 'synced_ledger': synced}




def _candidate_from_local_source(row: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(row or {})
    source_kind = str(row.get('source_kind') or '').strip().lower()
    item_type = str(row.get('item_type') or '').strip()
    tmdb_id = str(row.get('tmdb_id') or row.get('parent_series_tmdb_id') or row.get('series_tmdb_id') or '').strip()
    season = _safe_int_or_none(row.get('season_number'))
    episode = _safe_int_or_none(row.get('episode_number'))
    if source_kind == 'movie' or item_type == 'Movie':
        final_type = 'Movie'
    elif source_kind in ('episode', 'episode_group') or item_type == 'Episode' or episode is not None:
        final_type = 'Episode'
    elif item_type == 'Season' or season is not None:
        final_type = 'Season'
    else:
        final_type = item_type or 'Movie'
    candidate = {
        'tmdb_id': tmdb_id,
        'parent_series_tmdb_id': tmdb_id if final_type in ('Season', 'Episode') else row.get('parent_series_tmdb_id'),
        'series_tmdb_id': tmdb_id if final_type in ('Season', 'Episode') else row.get('series_tmdb_id'),
        'item_type': final_type,
        'season_number': season,
        'episode_number': episode if final_type == 'Episode' else None,
        'title': row.get('title') or row.get('root_name') or row.get('file_name') or '',
        'release_year': row.get('release_year'),
        'expected_episode_count': row.get('expected_episode_count') or row.get('total_episodes') or row.get('file_count'),
        'total_episodes': row.get('expected_episode_count') or row.get('total_episodes') or row.get('file_count'),
        'sha1': row.get('sha1') if final_type == 'Movie' else None,
        'file_name': row.get('file_name') if final_type == 'Movie' else None,
        'root_fid': row.get('root_fid'),
        'root_name': row.get('root_name'),
        'raw_json': row.get('raw_json'),
        '_original_source_kind': source_kind,
        '_original_source_provider': row.get('source_provider') or '',
        '_original_center_source_id': row.get('center_source_id') or '',
    }
    return {k: v for k, v in candidate.items() if v not in (None, '')}


def _reregister_provider_for_row(row: Dict[str, Any], requested: str = '') -> str:
    """重新登记应保持原 provider，避免生成 manual_reregister 影子源。"""
    row = dict(row or {})
    original = str(row.get('source_provider') or '').strip()
    requested = str(requested or '').strip()
    if requested and requested != 'manual_reregister':
        return requested
    return original or 'manual_rapid'


def _local_source_missing_pick_code(row: Dict[str, Any]) -> bool:
    row = dict(row or {})
    source_id = int(row.get('id') or 0)
    if not source_id:
        return False
    rapid_meta = row.get('rapid_meta_json') if isinstance(row.get('rapid_meta_json'), dict) else {}
    if str(rapid_meta.get('pick_code') or rapid_meta.get('pickcode') or rapid_meta.get('pc') or '').strip():
        return False
    try:
        files = shared_share_db.list_source_files(source_id)
    except Exception:
        files = []
    if not files:
        return False
    return all(
        not str((item or {}).get('pick_code') or (item or {}).get('pickcode') or (item or {}).get('pc') or '').strip()
        for item in files
    )


def reregister_local_source(source_id: int, *, source_provider: str = '') -> Dict[str, Any]:
    row = shared_share_db.get_local_source(int(source_id or 0))
    if not row:
        return {'ok': False, 'message': '本地共享源不存在', 'source_id': source_id}
    if _local_source_missing_pick_code(row):
        return {'ok': True, 'skipped': True, 'reason': 'missing_pick_code_virtual_library', 'message': '缺少 115 pick_code，按虚拟入库/不可秒传记录跳过维护补登', 'source_id': source_id}
    candidate = _candidate_from_local_source(row)
    # “重新登记”是原共享源的原地修复：重新上传 RAW/summary_json，
    # provider 保持原值，避免生成 manual_reregister 影子源。
    candidate['_raw_repair_only'] = True
    provider = _reregister_provider_for_row(row, source_provider)
    result = register_candidate_to_center(candidate, source_provider=provider)
    if not result.get('ok'):
        try:
            shared_share_db.update_local_source(int(source_id), last_error=result.get('message') or '重新登记失败')
        except Exception:
            pass
    return {'ok': bool(result.get('ok')), 'source_id': source_id, 'candidate': candidate, 'provider': provider, 'result': result, 'message': result.get('message') or ''}




def _maintenance_candidate_label(candidate: Dict[str, Any], fallback: str = '') -> str:
    """维护任务日志里显示可读片名 + SxxExx，避免只剩成功/失败数字。"""
    candidate = candidate if isinstance(candidate, dict) else {}
    title = str(
        candidate.get('title')
        or candidate.get('standard_title')
        or candidate.get('display_title')
        or candidate.get('series_title')
        or fallback
        or candidate.get('tmdb_id')
        or candidate.get('parent_series_tmdb_id')
        or '未知资源'
    ).strip()
    item_type = str(candidate.get('item_type') or candidate.get('share_item_type') or '').strip()
    season = _safe_int_or_none(candidate.get('season_number'))
    episode = _safe_int_or_none(candidate.get('episode_number'))
    if item_type == 'Episode' and season is not None and episode is not None:
        suffix = f"S{season:02d}E{episode:02d}"
        if suffix.lower() not in title.lower():
            title = f"{title} {suffix}"
    elif item_type == 'Season' and season is not None:
        suffix = f"S{season:02d}"
        if suffix.lower() not in title.lower():
            title = f"{title} {suffix}"
    return title


def _maintenance_result_reason(result: Dict[str, Any]) -> str:
    """把登记失败结果压缩成一行日志原因。"""
    result = result if isinstance(result, dict) else {}
    parts = []
    message = str(result.get('message') or '').strip()
    if message:
        parts.append(message)
    missing_raw = result.get('missing_raw') if isinstance(result.get('missing_raw'), list) else []
    if missing_raw:
        names = []
        for item in missing_raw[:3]:
            if isinstance(item, dict):
                names.append(str(item.get('file_name') or item.get('sha1') or '').strip())
            else:
                names.append(str(item or '').strip())
        names = [x for x in names if x]
        parts.append(f"RAW/摘要缺失 {len(missing_raw)} 个" + (f"：{'、'.join(names)}" if names else ''))
    errors = result.get('errors') if isinstance(result.get('errors'), list) else []
    if errors:
        err_texts = []
        for err in errors[:3]:
            if isinstance(err, dict):
                err_texts.append(str(err.get('error') or err.get('message') or err).strip())
            else:
                err_texts.append(str(err or '').strip())
        err_texts = [x for x in err_texts if x]
        if err_texts:
            parts.append('错误：' + '；'.join(err_texts))
    repair = result.get('fingerprint_repair') if isinstance(result.get('fingerprint_repair'), dict) else {}
    if repair and repair.get('message'):
        parts.append('指纹体检：' + str(repair.get('message')))
    if not parts:
        parts.append(str(result.get('error') or '未知原因'))
    # 去重并截断，防止中心返回过长内容刷屏。
    out = []
    for part in parts:
        part = str(part or '').strip()
        if part and part not in out:
            out.append(part)
    text = '；'.join(out)
    return text[:600] + ('...' if len(text) > 600 else '')

def reregister_local_sources(source_ids: List[int], *, source_provider: str = '') -> Dict[str, Any]:
    rows = []
    for sid in source_ids or []:
        try:
            row = shared_share_db.get_local_source(int(sid))
        except Exception:
            row = None
        if row:
            rows.append(dict(row))
    if not rows:
        return {'ok': False, 'message': '没有找到可重新登记的本地共享源', 'items': []}
    deduped = []
    seen = set()
    for row in rows:
        cand = _candidate_from_local_source(row)
        key = (cand.get('item_type'), cand.get('tmdb_id') or cand.get('parent_series_tmdb_id'), cand.get('season_number'), cand.get('episode_number') if cand.get('item_type') == 'Episode' else None, cand.get('sha1') if cand.get('item_type') == 'Movie' else None)
        if key in seen:
            continue
        seen.add(key)
        deduped.append((row, cand))
    items = []
    ok_count = 0
    failed = 0
    for row, cand in deduped:
        if _local_source_missing_pick_code(row):
            items.append({'id': row.get('id'), 'candidate': cand, 'provider': _reregister_provider_for_row(row, source_provider), 'ok': True, 'skipped': True, 'reason': 'missing_pick_code_virtual_library', 'message': '缺少 115 pick_code，按虚拟入库/不可秒传记录跳过维护补登'})
            continue
        cand['_raw_repair_only'] = True
        provider = _reregister_provider_for_row(row, source_provider)
        res = register_candidate_to_center(cand, source_provider=provider)
        if res.get('ok'):
            ok_count += 1
        else:
            failed += 1
            try:
                shared_share_db.update_local_source(int(row.get('id') or 0), last_error=res.get('message') or '重新登记失败')
            except Exception:
                pass
        items.append({'id': row.get('id'), 'candidate': cand, 'provider': provider, 'ok': bool(res.get('ok')), 'message': res.get('message') or '', 'result': res})
    return {
        'ok': ok_count > 0,
        'success_count': ok_count,
        'failed_count': failed,
        'items': items,
        'message': f'重新登记完成：成功 {ok_count}，失败 {failed}',
    }



def _delete_bad_completed_source_from_center_and_local(row: Dict[str, Any], gate: Dict[str, Any], *, client: SharedCenterClient = None) -> Dict[str, Any]:
    """完结季复检仍不合格时：中心取消登记，本地彻底删除共享索引。"""
    row = dict(row or {})
    gate = gate if isinstance(gate, dict) else {}
    local_id = int(row.get('id') or 0)
    source_kind = str(row.get('source_kind') or 'completed_season').strip() or 'completed_season'
    center_source_id = str(row.get('center_source_id') or '').strip()
    candidate = _candidate_from_local_source(row)
    label = _maintenance_candidate_label(candidate, fallback=row.get('title') or row.get('tmdb_id') or str(local_id))
    reason = str(gate.get('message') or gate.get('reason') or '完结季一致性校验未通过').strip()
    center_resp = {}

    share_cleanup = _delete_completed_share_channels_for_source(row, reason, client=client)
    if not share_cleanup.get('ok'):
        try:
            shared_share_db.update_local_source(local_id, last_error=f'旧版 115 分享删除失败: {share_cleanup}')
        except Exception:
            pass
        logger.warning(
            "  ➜ [共享资源维护] 删除旧版共享源前清理 115 分享失败，保留本地记录等待下次重试：%s，id=%s，center=%s，share_cleanup=%s",
            label,
            local_id,
            center_source_id or '-',
            share_cleanup,
        )
        return {
            'ok': False,
            'id': local_id,
            'title': label,
            'center_source_id': center_source_id,
            'message': '旧版 115 分享删除失败',
            'reason': reason,
            'share_cleanup': share_cleanup,
        }

    if center_source_id:
        try:
            client = client or SharedCenterClient()
            delete_method = getattr(client, 'delete_source', None)
            if callable(delete_method):
                center_resp = delete_method(source_kind, center_source_id, message=reason) or {}
            else:
                center_resp = client.disable_source(source_kind, center_source_id, message=reason) or {}
            if center_resp.get('ok') is False:
                raise RuntimeError(center_resp.get('message') or center_resp.get('error') or center_resp)
        except Exception as e:
            try:
                shared_share_db.update_local_source(local_id, last_error=f'不合格完结季中心取消登记失败: {e}')
            except Exception:
                pass
            logger.warning(
                "  ➜ [共享资源维护] 不合格完结季中心取消登记失败，保留本地记录等待下次重试：%s，id=%s，center=%s，err=%s",
                label,
                local_id,
                center_source_id,
                e,
            )
            return {
                'ok': False,
                'id': local_id,
                'title': label,
                'center_source_id': center_source_id,
                'message': str(e),
                'reason': reason,
            }

    deleted = shared_share_db.delete_local_source(local_id) if local_id else {}
    logger.info(
        "  ➜ [共享资源维护] 已删除不合格完结季共享源：%s，id=%s，center=%s，原因=%s",
        label,
        local_id or '-',
        center_source_id or '-',
        reason,
    )
    return {
        'ok': True,
        'id': local_id,
        'title': label,
        'center_source_id': center_source_id,
        'reason': reason,
        'center_response': center_resp,
        'share_cleanup': share_cleanup,
        'deleted': deleted,
    }



def _reregister_non_effective_local_sources(limit: int = 300) -> Dict[str, Any]:
    """维护任务：处理本地非有效共享源。

    - 旧 completed_season_source：不再复检一致性，直接下架/删除，避免继续走客户端季包旧链路。
    - 电影/分集/其他：保留原来的重新登记修复流程。
    """
    rows = shared_share_db.list_non_effective_local_sources(limit=limit)
    if not rows:
        return {
            'ok': True,
            'checked': 0,
            'need_reregister': 0,
            'reregistered': 0,
            'failed': 0,
            'removed_bad_completed': 0,
            'remove_failed': 0,
            'items': [],
            'removed_items': [],
        }

    client = SharedCenterClient()
    reregister_rows = []
    removed_items = []
    remove_failed_items = []
    consistency_checked = 0
    consistency_failed = 0

    for row in rows:
        row = dict(row or {})
        source_kind = str(row.get('source_kind') or '').strip().lower()
        if source_kind != 'completed_season':
            reregister_rows.append(row)
            continue

        gate = {
            'ok': False,
            'reason': 'legacy_completed_season_source_removed',
            'message': '旧 completed_season_source 已停用；完结季由中心逻辑季包管理。',
            'final_failure': True,
        }
        consistency_checked += 1
        consistency_failed += 1
        removed = _delete_bad_completed_source_from_center_and_local(row, gate, client=client)
        if removed.get('ok'):
            removed_items.append(removed)
        else:
            remove_failed_items.append(removed)

    # reregister_local_sources 内部会按电影/季/集身份去重，并保持原 source_provider，
    # 避免生成 maintenance_reregister 影子源。
    source_ids = [int(r.get('id')) for r in reregister_rows if r.get('id')]
    res = reregister_local_sources(source_ids, source_provider='') if source_ids else {
        'items': [],
        'success_count': 0,
        'failed_count': 0,
    }
    items = res.get('items') or []
    success = int(res.get('success_count') or 0)
    failed = int(res.get('failed_count') or 0) + len(remove_failed_items)
    if rows:
        logger.info(
            "  ➜ [共享资源维护] 非有效状态处理完成：候选=%s，完结季复检=%s，不合格=%s，已删除=%s，删除失败=%s，重登去重后=%s，重登成功=%s，重登失败=%s",
            len(rows),
            consistency_checked,
            consistency_failed,
            len(removed_items),
            len(remove_failed_items),
            len(items),
            success,
            int(res.get('failed_count') or 0),
        )
    return {
        'ok': failed == 0,
        'checked': len(rows),
        'consistency_checked': consistency_checked,
        'bad_completed': consistency_failed,
        'removed_bad_completed': len(removed_items),
        'remove_failed': len(remove_failed_items),
        'need_reregister': len(items),
        'reregistered': success,
        'failed': failed,
        'items': items[:50],
        'removed_items': removed_items[:50],
        'remove_failed_items': remove_failed_items[:20],
    }

def _backfill_airing_episode_sources(limit: int = 500) -> Dict[str, Any]:
    """维护任务：为连载/追更季补登记新入库但尚未共享的分集。

    只看本地 media_metadata + shared_rapid_sources 的差异，不触发季级一致性校验。
    每个新入库分集按 Episode 粒度登记到中心公共 season_hub。
    """
    candidates = shared_share_db.list_unregistered_airing_episode_candidates(limit=limit)
    if not candidates:
        return {'ok': True, 'checked': 0, 'need_register': 0, 'registered': 0, 'failed': 0, 'items': []}

    registered = 0
    failed = 0
    skipped = 0
    items = []
    failed_items = []
    for cand in candidates:
        cand = dict(cand or {})
        # 双保险：维护补齐不能借道 collect_files_for_candidate 触发 helpers.check_season_consistency。
        cand['_skip_fingerprint_repair'] = True
        cand['_raw_repair_only'] = True
        label = _maintenance_candidate_label(cand)
        try:
            files = shared_share_db.collect_files_for_candidate(cand)
        except Exception:
            files = []
        if files and _files_missing_pick_code(files):
            skipped += 1
            message = '缺少 115 pick_code，按虚拟入库/不可秒传记录跳过维护补登'
            logger.info(
                "  ➜ [共享资源维护] 追更补齐跳过虚拟/不可秒传记录：%s，tmdb=%s，season=%s，episode=%s",
                label,
                cand.get('tmdb_id') or cand.get('parent_series_tmdb_id'),
                cand.get('season_number'),
                cand.get('episode_number'),
            )
            items.append({
                'candidate': cand,
                'title': label,
                'ok': True,
                'skipped': True,
                'reason': 'missing_pick_code_virtual_library',
                'message': message,
            })
            continue
        try:
            res = register_candidate_to_center(cand, source_provider='rapid_followup_backfill')
        except Exception as e:
            res = {'ok': False, 'message': f'登记异常: {e}', 'error': str(e)}
            logger.warning(
                "  ➜ [共享资源维护] 追更补齐异常：%s，tmdb=%s，season=%s，episode=%s，err=%s",
                label,
                cand.get('tmdb_id') or cand.get('parent_series_tmdb_id'),
                cand.get('season_number'),
                cand.get('episode_number'),
                e,
                exc_info=True,
            )
        ok = bool(res.get('ok'))
        reason = '' if ok else _maintenance_result_reason(res)
        if ok:
            registered += 1
            logger.info(
                "  ➜ [共享资源维护] 追更补齐成功：%s，tmdb=%s，season=%s，episode=%s",
                label,
                cand.get('tmdb_id') or cand.get('parent_series_tmdb_id'),
                cand.get('season_number'),
                cand.get('episode_number'),
            )
        else:
            failed += 1
            fail_item = {
                'title': label,
                'tmdb_id': cand.get('tmdb_id') or cand.get('parent_series_tmdb_id'),
                'season_number': cand.get('season_number'),
                'episode_number': cand.get('episode_number'),
                'message': reason,
            }
            failed_items.append(fail_item)
            logger.warning(
                "  ➜ [共享资源维护] 追更补齐失败：%s，tmdb=%s，season=%s，episode=%s，原因=%s",
                label,
                fail_item.get('tmdb_id'),
                fail_item.get('season_number'),
                fail_item.get('episode_number'),
                reason,
            )
        items.append({
            'candidate': cand,
            'title': label,
            'ok': ok,
            'message': reason or res.get('message') or '',
            'result': res,
        })

    if registered or failed or skipped:
        sample = '；'.join([f"{x.get('title')}：{x.get('message')}" for x in failed_items[:5]])
        logger.info(
            "  ➜ [共享资源维护] 追更补齐完成：待补=%s，成功=%s，失败=%s，跳过=%s%s",
            len(candidates),
            registered,
            failed,
            skipped,
            f"，失败明细={sample}" if sample else '',
        )
    return {
        'ok': failed == 0,
        'checked': len(candidates),
        'need_register': len(candidates),
        'registered': registered,
        'failed': failed,
        'skipped': skipped,
        'failed_items': failed_items[:20],
        'items': items[:50],
    }


def _backfill_center_missing_quality_sources(limit: int = 300) -> Dict[str, Any]:
    client = SharedCenterClient()
    try:
        resp = client.list_missing_quality_sources(limit=limit)
    except Exception as e:
        return {'ok': False, 'checked': 0, 'reregistered': 0, 'failed': 1, 'skipped': 0, 'message': f'获取中心缺来源列表失败: {e}'}

    rows = [dict(x or {}) for x in (resp.get('items') or []) if isinstance(x, dict)]
    if not rows:
        return {'ok': True, 'checked': 0, 'reregistered': 0, 'failed': 0, 'skipped': 0, 'items': []}

    checked = 0
    reregistered = 0
    failed = 0
    skipped = 0
    items = []
    seen = set()
    for item in rows:
        source_kind = str(item.get('source_kind') or '').strip()
        center_source_id = str(item.get('center_source_id') or '').strip()
        key = (source_kind, center_source_id)
        if not source_kind or not center_source_id or key in seen:
            continue
        seen.add(key)
        checked += 1
        local = shared_share_db.get_local_source_by_center(source_kind, center_source_id)
        if not local:
            skipped += 1
            items.append({'source_kind': source_kind, 'center_source_id': center_source_id, 'ok': True, 'skipped': True, 'reason': 'local_source_missing'})
            continue
        if _local_source_missing_pick_code(local):
            skipped += 1
            items.append({'source_kind': source_kind, 'center_source_id': center_source_id, 'id': local.get('id'), 'ok': True, 'skipped': True, 'reason': 'missing_pick_code_virtual_library'})
            continue
        res = reregister_local_source(int(local.get('id') or 0), source_provider='')
        ok = bool(res.get('ok')) and not res.get('skipped')
        if ok:
            reregistered += 1
        elif res.get('skipped'):
            skipped += 1
        else:
            failed += 1
        items.append({
            'source_kind': source_kind,
            'center_source_id': center_source_id,
            'id': local.get('id'),
            'ok': bool(res.get('ok')),
            'skipped': bool(res.get('skipped')),
            'message': res.get('message') or '',
        })

    if checked:
        logger.info(
            "  ➜ [共享资源维护] 中心缺来源补齐完成：待补=%s，重登=%s，跳过=%s，失败=%s",
            checked,
            reregistered,
            skipped,
            failed,
        )
    return {
        'ok': failed == 0,
        'checked': checked,
        'reregistered': reregistered,
        'failed': failed,
        'skipped': skipped,
        'items': items[:50],
    }

def disable_shared_sources_for_deleted_fids(
    fids: List[Any],
    *,
    reason: str = 'washing_replaced_old_version',
    message: str = '',
) -> Dict[str, Any]:
    """洗版删除旧版 115 文件前，主动把相关 Rapid 共享源从中心下架。

    维护任务里的离线清理是兜底扫描，存在时间窗口；洗版替换旧版时已明确知道
    将要删除哪些 115 fid，因此这里同步做一次精准下架，避免中心在维护窗口期继续
    派发 holder 签名任务，导致源端因为旧文件已删除而白白扣贡献点。

    策略与维护任务保持一致：
    - 中心下架成功：本地源标记 disabled，不再参与重登/签名；
    - 中心下架失败：只写 last_error，保留 active/reported 锚点，交给后续维护任务重试；
    - 尚未上报中心的本地源：直接本地 disabled。
    """
    fid_list: List[str] = []
    seen = set()
    for value in fids or []:
        text = str(value or '').strip()
        if not text or text in seen:
            continue
        seen.add(text)
        fid_list.append(text)

    if not fid_list:
        return {'ok': True, 'matched': 0, 'disabled': 0, 'failed': 0, 'items': []}

    reason_text = str(reason or 'washing_replaced_old_version').strip()
    center_message = str(message or '').strip() or f'local file deleted by washing: {reason_text}'
    disabled = 0
    deleted = 0
    failed = 0
    items: List[Dict[str, Any]] = []
    logical_share_cleanup = _delete_logical_share_channels_for_fids(fid_list, reason_text)
    if not logical_share_cleanup.get('ok'):
        failed += int(logical_share_cleanup.get('failed') or 1)
        items.append({
            'ok': False,
            'error': '旧版逻辑季 115 分享删除失败',
            'logical_share_cleanup': logical_share_cleanup,
        })
    elif logical_share_cleanup.get('checked'):
        items.append({
            'ok': True,
            'logical_share_cleanup': logical_share_cleanup,
        })

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        s.*,
                        ARRAY_REMOVE(ARRAY_AGG(DISTINCT f.fid) FILTER (WHERE f.fid = ANY(%s)), NULL) AS matched_file_fids
                    FROM shared_rapid_sources s
                    LEFT JOIN shared_rapid_source_files f
                      ON f.local_source_id = s.id
                    WHERE COALESCE(s.status, '') NOT IN ('disabled', 'cancelled', 'canceled', 'deleted')
                      AND COALESCE(s.center_status, '') <> 'disabled'
                      AND (
                            COALESCE(s.root_fid, '') = ANY(%s)
                         OR COALESCE(f.fid, '') = ANY(%s)
                      )
                    GROUP BY s.id
                    ORDER BY s.id ASC
                    """,
                    (fid_list, fid_list, fid_list),
                )
                rows = [dict(r) for r in (cur.fetchall() or [])]
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 洗版下架旧共享源：查询待下架源失败: fids={len(fid_list)}, err={e}")
        return {'ok': False, 'matched': 0, 'disabled': 0, 'failed': 1, 'error': str(e), 'items': []}

    if not rows:
        return {
            'ok': failed == 0,
            'matched': 0,
            'disabled': 0,
            'failed': failed,
            'logical_share_cleanup': logical_share_cleanup,
            'items': items[:50],
        }

    client = None

    for row in rows:
        local_id = int(row.get('id') or 0)
        source_kind = str(row.get('source_kind') or '').strip()
        center_source_id = str(row.get('center_source_id') or '').strip()
        title = str(row.get('title') or row.get('file_name') or row.get('tmdb_id') or local_id)
        matched_file_fids = [str(x) for x in (row.get('matched_file_fids') or []) if str(x or '').strip()]
        center_resp: Dict[str, Any] = {}

        if source_kind == 'completed_season':
            delete_reason = 'legacy_completed_season_source_removed'
            try:
                shared_share_db.delete_local_source(local_id)
                deleted += 1
                item = {
                    'id': local_id,
                    'source_kind': source_kind,
                    'center_source_id': center_source_id,
                    'title': title,
                    'ok': True,
                    'deleted': True,
                    'reason': delete_reason,
                    'matched_file_fids': matched_file_fids,
                    'root_fid_matched': str(row.get('root_fid') or '') in fid_list,
                }
                items.append(item)
                logger.info(
                    "  ➜ [共享资源] 洗版删除旧版前已删除废弃 completed_season 共享源: id=%s, center=%s, title=%s, matched_files=%s",
                    local_id,
                    center_source_id or '-',
                    title,
                    len(matched_file_fids) or ('root' if item['root_fid_matched'] else 0),
                )
            except Exception as e:
                failed += 1
                logger.warning(
                    "  ➜ [共享资源] 洗版废弃 completed_season 本地删除失败: id=%s, center=%s, title=%s, err=%s",
                    local_id,
                    center_source_id or '-',
                    title,
                    e,
                )
                items.append({
                    'id': local_id,
                    'source_kind': source_kind,
                    'center_source_id': center_source_id,
                    'title': title,
                    'ok': False,
                    'error': str(e),
                })
            continue

        share_cleanup = _delete_completed_share_channels_for_source(row, reason_text, client=client)
        if not share_cleanup.get('ok'):
            failed += 1
            try:
                shared_share_db.update_local_source(local_id, last_error=f'洗版旧版 115 分享删除失败: {share_cleanup}')
            except Exception:
                pass
            logger.warning(
                "  ➜ [共享资源] 洗版旧源 115 分享删除失败，保留本地 active 等维护任务重试: "
                "id=%s, kind=%s, center=%s, title=%s, share_cleanup=%s",
                local_id,
                source_kind,
                center_source_id or '-',
                title,
                share_cleanup,
            )
            items.append({
                'id': local_id,
                'source_kind': source_kind,
                'center_source_id': center_source_id,
                'title': title,
                'ok': False,
                'error': '旧版 115 分享删除失败',
                'share_cleanup': share_cleanup,
            })
            continue

        if center_source_id:
            try:
                if not _enabled():
                    raise RuntimeError('共享资源中心未启用，无法主动下架中心源')
                if client is None:
                    client = SharedCenterClient()
                if getattr(client, 'ready', True) is False:
                    raise RuntimeError('共享中心未配置，无法主动下架中心源')
                center_resp = client.disable_source(source_kind, center_source_id, message=center_message) or {}
                if center_resp.get('ok') is False:
                    raise RuntimeError(center_resp.get('message') or center_resp.get('error') or center_resp)
            except Exception as e:
                failed += 1
                err = f'洗版删除旧版前中心下架失败: {e}'
                try:
                    shared_share_db.update_local_source(local_id, last_error=err)
                except Exception:
                    pass
                logger.warning(
                    "  ➜ [共享资源] 洗版旧源中心下架失败，保留本地 active 等维护任务重试: "
                    "id=%s, kind=%s, center=%s, title=%s, matched_fids=%s, err=%s",
                    local_id,
                    source_kind,
                    center_source_id,
                    title,
                    len(matched_file_fids) or ('root' if str(row.get('root_fid') or '') in fid_list else 0),
                    e,
                )
                items.append({
                    'id': local_id,
                    'source_kind': source_kind,
                    'center_source_id': center_source_id,
                    'title': title,
                    'ok': False,
                    'error': str(e),
                })
                continue

        try:
            saved = shared_share_db.disable_local_source(
                local_id,
                reason=reason_text,
                center_response=center_resp,
                source='washing_delete_old_version',
            )
            disabled += 1
            item = {
                'id': local_id,
                'source_kind': source_kind,
                'center_source_id': center_source_id,
                'title': title,
                'ok': True,
                'matched_file_fids': matched_file_fids,
                'root_fid_matched': str(row.get('root_fid') or '') in fid_list,
                'share_cleanup': share_cleanup,
            }
            items.append(item)
            logger.info(
                "  ➜ [共享资源] 洗版删除旧版前已下架旧共享源: id=%s, kind=%s, center=%s, title=%s, matched_files=%s",
                local_id,
                source_kind,
                center_source_id or '-',
                title,
                len(matched_file_fids) or ('root' if item['root_fid_matched'] else 0),
            )
        except Exception as e:
            failed += 1
            logger.warning(
                "  ➜ [共享资源] 洗版旧源本地禁用失败: id=%s, kind=%s, center=%s, title=%s, err=%s",
                local_id,
                source_kind,
                center_source_id or '-',
                title,
                e,
            )
            items.append({
                'id': local_id,
                'source_kind': source_kind,
                'center_source_id': center_source_id,
                'title': title,
                'ok': False,
                'error': str(e),
            })

    return {
        'ok': failed == 0,
        'matched': len(rows),
        'disabled': disabled,
        'deleted': deleted,
        'failed': failed,
        'logical_share_cleanup': logical_share_cleanup,
        'items': items[:50],
    }


def _cleanup_offline_local_sources(limit: int = 300) -> Dict[str, Any]:
    """维护任务里的轻量离线清理。

    只用本地共享索引里的 SHA1 反查 media_metadata.in_library=true。
    不访问 115，不走 collect_files_for_candidate，不触发指纹修复。
    """
    rows = shared_share_db.list_offline_local_sources(limit=limit)
    if not rows:
        return {'ok': True, 'offline_found': 0, 'disabled': 0, 'failed': 0}

    client = SharedCenterClient()
    disabled = 0
    failed = 0
    items = []
    for row in rows:
        local_id = int(row.get('id') or 0)
        source_kind = str(row.get('source_kind') or '').strip()
        center_source_id = str(row.get('center_source_id') or '').strip()
        title = str(row.get('title') or row.get('file_name') or row.get('tmdb_id') or local_id)
        reason = str(row.get('offline_reason') or 'sha1_not_in_library')
        msg = f'local library removed or replaced: {reason}'
        center_resp = {}
        share_cleanup = _delete_completed_share_channels_for_source(row, reason, client=client)

        # share_cleanup 已在中心下架前完成，这里只复用结果，避免维护重试时重复删除 115 分享。
        if not share_cleanup.get('ok'):
            failed += 1
            try:
                shared_share_db.update_local_source(local_id, last_error=f'失效源 115 分享删除失败: {share_cleanup}')
            except Exception:
                pass
            logger.warning(
                "  ➜ [共享资源维护] 本机失效共享源 115 分享删除失败，保留 active 等下次重试: "
                "id=%s, kind=%s, center=%s, title=%s, share_cleanup=%s",
                local_id,
                source_kind,
                center_source_id or '-',
                title,
                share_cleanup,
            )
            items.append({
                'id': local_id,
                'source_kind': source_kind,
                'center_source_id': center_source_id,
                'title': title,
                'ok': False,
                'error': '失效源 115 分享删除失败',
                'share_cleanup': share_cleanup,
            })
            continue

        if center_source_id:
            try:
                center_resp = client.disable_source(source_kind, center_source_id, message=msg) or {}
                if center_resp.get('ok') is False:
                    raise RuntimeError(center_resp.get('message') or center_resp.get('error') or center_resp)
            except Exception as e:
                failed += 1
                try:
                    shared_share_db.update_local_source(local_id, last_error=f'中心下架失败: {e}')
                except Exception:
                    pass
                logger.warning(
                    "  ➜ [共享资源维护] 本机失效共享源中心下架失败，保留 active 等下次重试: "
                    "id=%s, kind=%s, center=%s, title=%s, err=%s",
                    local_id,
                    source_kind,
                    center_source_id,
                    title,
                    e,
                )
                continue

        shared_share_db.disable_local_source(local_id, reason=reason, center_response=center_resp)
        disabled += 1
        item = {
            'id': local_id,
            'source_kind': source_kind,
            'center_source_id': center_source_id,
            'title': title,
            'reason': reason,
            'total_files': int(row.get('total_files') or 0),
            'live_files': int(row.get('live_files') or 0),
            'share_cleanup': share_cleanup,
        }
        items.append(item)
        logger.info(
            "  ➜ [共享资源维护] 已下架本机失效共享源: id=%s, kind=%s, center=%s, title=%s, files=%s/%s, reason=%s",
            local_id,
            source_kind,
            center_source_id or '-',
            title,
            item['live_files'],
            item['total_files'],
            reason,
        )

    return {'ok': failed == 0, 'offline_found': len(rows), 'disabled': disabled, 'failed': failed, 'items': items[:50]}




def _center_request_headers_for_display_meta() -> Dict[str, str]:
    return {
        'X-Server-ID-Hash': _current_server_id_hash(),
        'X-Client-Version': str(getattr(constants, 'APP_VERSION', '0.0.0') or '0.0.0'),
        'Content-Type': 'application/json',
    }


def _center_base_url_for_display_meta() -> str:
    cfg = settings_db.get_shared_resource_config() or {}
    return str(cfg.get('p115_shared_center_url') or '').strip().rstrip('/')


def _center_request_kwargs_for_display_meta(timeout: int = 60) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {'timeout': timeout}
    getter = getattr(config_manager, 'get_proxies_for_requests', None)
    if callable(getter):
        try:
            proxies = getter()
            if proxies:
                kwargs['proxies'] = proxies
        except Exception:
            pass
    return kwargs


def _display_meta_key(meta: Dict[str, Any]) -> tuple:
    meta = meta if isinstance(meta, dict) else {}
    typ = str(meta.get('item_type') or '').strip()
    tmdb = str(meta.get('tmdb_id') or '').strip()
    if typ == 'Episode':
        return (
            tmdb,
            typ,
            _safe_int_or_none(meta.get('season_number')),
            _safe_int_or_none(meta.get('episode_number')),
        )
    season = _safe_int_or_none(meta.get('season_number')) if typ == 'Season' else None
    return (tmdb, typ, season, None)


def _display_meta_has_useful_payload(meta: Dict[str, Any]) -> bool:
    meta = meta if isinstance(meta, dict) else {}
    if str(meta.get('item_type') or '').strip() == 'Episode':
        return meta.get('runtime_minutes') not in (None, '', [], {})
    for key in (
        'title', 'original_title', 'overview', 'poster_path', 'backdrop_path',
        'release_year', 'release_date', 'rating', 'genres_json', 'original_language',
        'expected_episode_count', 'total_episodes', 'episode_count',
        'final_episode_air_date', 'last_episode_air_date',
    ):
        if meta.get(key) not in (None, '', [], {}):
            return True
    return False


def _display_meta_text_is_chinese(meta: Dict[str, Any]) -> bool:
    meta = meta if isinstance(meta, dict) else {}
    return utils.contains_chinese(str(meta.get('title') or '')) or utils.contains_chinese(str(meta.get('overview') or ''))


def _filter_display_meta_for_center_upload(meta: Dict[str, Any]) -> Dict[str, Any]:
    meta = meta if isinstance(meta, dict) else {}
    item_type = str(meta.get('item_type') or '').strip()
    if item_type == 'Episode':
        if meta.get('runtime_minutes') in (None, '', [], {}):
            return {}
        return {
            key: meta.get(key)
            for key in ('tmdb_id', 'item_type', 'season_number', 'episode_number', 'runtime_minutes')
            if meta.get(key) not in (None, '', [], {})
        }

    out = dict(meta)
    for key in ('title', 'original_title', 'overview'):
        if out.get(key) not in (None, '', [], {}) and not utils.contains_chinese(str(out.get(key) or '')):
            out.pop(key, None)
    if not _display_meta_text_is_chinese(out):
        for key in ('title', 'original_title', 'overview', 'poster_path', 'backdrop_path', 'genres_json', 'original_language'):
            out.pop(key, None)
    return out if _display_meta_has_useful_payload(out) else {}


def _row_has_chinese_credits(row: Dict[str, Any]) -> bool:
    actors = _safe_json_list((row or {}).get('actors_json'))
    if not actors:
        return False
    person_ids = []
    for raw in actors:
        if isinstance(raw, dict):
            pid = _person_id_from_credit(raw)
            if pid:
                person_ids.append(pid)
    person_map = _lookup_people_for_display(person_ids)
    for raw in actors:
        if not isinstance(raw, dict):
            continue
        pid = _person_id_from_credit(raw)
        if not pid:
            continue
        info = person_map.get(pid) or {}
        name = _first_display_text(info.get('primary_name'), raw.get('primary_name'), raw.get('name'), raw.get('actor_name'))
        if utils.contains_chinese(name) and utils.contains_chinese(_credit_character_text(raw)):
            return True
    return False


def _fetch_center_missing_display_meta_rows(limit: int = 500) -> Dict[str, Any]:
    """向中心端询问哪些公共媒体壳缺展示元数据。

    维护补齐不能再从本机 shared_rapid_sources 反推范围：
    只要中心资源库里某个电影/季壳缺海报/简介/标题等展示字段，
    任意客户端本地库有对应 media_metadata，就可以补传。
    """
    base_url = _center_base_url_for_display_meta()
    headers = _center_request_headers_for_display_meta()
    if not base_url or not headers.get('X-Server-ID-Hash'):
        return {'ok': False, 'items': [], 'message': '共享中心 URL 或 Emby ServerID 未配置'}
    try:
        with requests.get(
            f"{base_url}/api/v1/metadata/display/missing",
            headers=headers,
            params={'limit': max(1, min(int(limit or 500), 5000))},
            **_center_request_kwargs_for_display_meta(timeout=60),
        ) as resp:
            if resp.status_code >= 400:
                return {'ok': False, 'items': [], 'message': f"HTTP {resp.status_code}: {resp.text[:300]}"}
            data = resp.json() if resp.content else {}
        items = [x for x in (data.get('items') or []) if isinstance(x, dict)]
        return {'ok': True, 'items': items, 'count': len(items), 'raw': data}
    except Exception as e:
        return {'ok': False, 'items': [], 'message': str(e)}


def _list_display_meta_backfill_source_rows(limit: int = 500) -> List[Dict[str, Any]]:
    """维护任务只按中心缺失壳补齐，不再扫描本机已登记共享源。"""
    limit = max(1, min(int(limit or 500), 5000))
    fetched = _fetch_center_missing_display_meta_rows(limit=limit)
    if not fetched.get('ok'):
        logger.warning(f"  ➜ [共享资源维护] 查询中心缺失海报/元数据失败: {fetched.get('message') or 'unknown'}")
        return []

    rows: List[Dict[str, Any]] = []
    for item in fetched.get('items') or []:
        item_type = str(item.get('item_type') or '').strip()
        tmdb_id = str(item.get('tmdb_id') or '').strip()
        if item_type not in ('Movie', 'Series', 'Season', 'Episode') or not tmdb_id:
            continue
        season_no = _safe_int_or_none(item.get('season_number')) if item_type in ('Season', 'Episode') else None
        episode_no = _safe_int_or_none(item.get('episode_number')) if item_type == 'Episode' else None
        if item_type in ('Season', 'Episode') and season_no is None:
            continue
        if item_type == 'Episode' and episode_no is None:
            continue
        rows.append({
            'id': item.get('media_key') or f"center-missing:{item_type}:{tmdb_id}:{season_no or ''}:{episode_no or ''}",
            'source_kind': 'movie' if item_type == 'Movie' else ('series' if item_type == 'Series' else ('episode' if item_type == 'Episode' else 'season_hub')),
            'center_source_id': '',
            'tmdb_id': tmdb_id,
            'parent_series_tmdb_id': tmdb_id if item_type in ('Season', 'Episode') else '',
            'series_tmdb_id': tmdb_id if item_type in ('Season', 'Episode') else '',
            'item_type': item_type,
            'season_number': season_no,
            'episode_number': episode_no,
            'title': item.get('title') or item.get('fallback_title') or '',
            'release_year': item.get('release_year'),
            'expected_episode_count': item.get('expected_episode_count'),
            'total_episodes': item.get('expected_episode_count'),
            'missing_fields': item.get('missing_fields') or [],
            'center_missing': True,
        })
    return rows


def _local_rows_have_display_payload(rows: Dict[str, Dict[str, Any]], item_type: str, missing_fields: List[str] = None) -> bool:
    """确认本地 media_metadata 真有中心缺的字段，避免无效反复补传。"""
    rows = rows if isinstance(rows, dict) else {}
    item_type = str(item_type or '').strip()
    missing = {str(x or '').strip() for x in (missing_fields or []) if str(x or '').strip()}

    def has_any(row: Dict[str, Any], keys) -> bool:
        row = row if isinstance(row, dict) else {}
        return any(row.get(k) not in (None, '', [], {}) for k in keys)

    def has_image(row: Dict[str, Any], *keys) -> bool:
        row = row if isinstance(row, dict) else {}
        return bool(_display_image_path_for_center(*(row.get(k) for k in keys)))

    def has_chinese(row: Dict[str, Any], keys) -> bool:
        row = row if isinstance(row, dict) else {}
        return any(utils.contains_chinese(str(row.get(k) or '')) for k in keys)

    def has_chinese_display(row: Dict[str, Any]) -> bool:
        return has_chinese(row, ('title', 'original_title', 'overview'))

    movie = rows.get('movie') or {}
    series = rows.get('series') or {}
    season = rows.get('season') or {}
    episode = rows.get('episode') or {}

    if missing:
        if item_type == 'Movie':
            checks = {
                'title': has_chinese(movie, ('title', 'original_title')),
                'poster_path': has_chinese_display(movie) and has_image(movie, 'poster_path', 'poster_url', 'image', 'cover'),
                'backdrop_path': has_chinese_display(movie) and has_image(movie, 'backdrop_path', 'backdrop_url', 'background'),
                'overview': has_chinese(movie, ('overview',)),
                'rating_refresh': has_any(movie, ('rating', 'vote_average')),
                'movie_meta': has_any(movie, (
                    'release_year', 'release_date', 'rating',
                )) or has_chinese_display(movie),
                'credits': _row_has_chinese_credits(movie),
            }
        elif item_type == 'Episode':
            checks = {
                'episode_runtime': has_any(episode, ('runtime_minutes',)),
                'runtime_minutes': has_any(episode, ('runtime_minutes',)),
            }
        else:
            checks = {
                'title': has_chinese(series, ('title', 'original_title')) or has_chinese(season, ('title', 'original_title')),
                'poster_path': (has_chinese_display(season) and has_image(season, 'poster_path', 'poster_url', 'image', 'cover')) or (has_chinese_display(series) and has_image(series, 'poster_path', 'poster_url', 'image', 'cover')),
                'season_poster_path': has_chinese_display(season) and has_image(season, 'poster_path', 'poster_url', 'image', 'cover'),
                'series_poster_path': has_chinese_display(series) and has_image(series, 'poster_path', 'poster_url', 'image', 'cover'),
                'backdrop_path': (has_chinese_display(season) and has_image(season, 'backdrop_path', 'backdrop_url', 'background')) or (has_chinese_display(series) and has_image(series, 'backdrop_path', 'backdrop_url', 'background')),
                'season_backdrop_path': has_chinese_display(season) and has_image(season, 'backdrop_path', 'backdrop_url', 'background'),
                'series_backdrop_path': has_chinese_display(series) and has_image(series, 'backdrop_path', 'backdrop_url', 'background'),
                'overview': has_chinese(season, ('overview',)) or has_chinese(series, ('overview',)),
                'rating_refresh': has_any(series, ('rating', 'vote_average')),
                'season_overview': has_chinese(season, ('overview',)),
                'series_overview': has_chinese(series, ('overview',)),
                'series_meta': has_any(series, (
                    'release_year', 'release_date', 'rating',
                )) or has_chinese_display(series),
                'watchlist_status': has_any(season, ('watching_status', 'watchlist_tmdb_status')),
                'credits': _row_has_chinese_credits(series),
                'season_meta': has_any(season, ('release_year', 'release_date', 'watching_status', 'watchlist_tmdb_status')) or has_chinese_display(season),
                'season_total': has_any(season, ('total_episodes',)),
                'expected_episode_count': has_any(season, ('total_episodes',)),
                'total_episodes': has_any(season, ('total_episodes',)),
                'final_episode_air_date': has_any(season, ('final_episode_air_date', 'last_episode_air_date')),
            }
        return any(checks.get(field, False) for field in missing)

    candidates = [movie] if item_type == 'Movie' else ([episode] if item_type == 'Episode' else [series, season])
    return any(
        has_any(row, ('release_year', 'release_date', 'rating', 'runtime_minutes', 'watching_status', 'watchlist_tmdb_status'))
        or has_chinese_display(row)
        or _row_has_chinese_credits(row)
        for row in candidates
    )

def _build_display_meta_backfill_bundles(limit: int = 500) -> Dict[str, Any]:
    rows = _list_display_meta_backfill_source_rows(limit=limit)
    bundles: List[Dict[str, Any]] = []
    seen_meta_keys = set()
    scanned_meta_items = 0
    skipped_empty = 0

    for row in rows:
        candidate = _candidate_from_local_source(row)
        if not candidate:
            skipped_empty += 1
            continue
        try:
            candidate = _normalize_series_candidate_identity(candidate)
            local_rows = _local_display_meta_rows_for_candidate(candidate)
            if not _local_rows_have_display_payload(local_rows, str(candidate.get('item_type') or ''), row.get('missing_fields') or []):
                skipped_empty += 1
                continue
            bundle = _center_display_meta_bundle_for_candidate(candidate)
        except Exception as e:
            logger.debug(
                "  ➜ [共享资源维护] 构建展示元数据补齐包失败: id=%s, tmdb=%s, err=%s",
                row.get('id'), row.get('tmdb_id'), e,
            )
            skipped_empty += 1
            continue

        meta_items = bundle.get('display_meta_items_json') if isinstance(bundle.get('display_meta_items_json'), list) else []
        if not meta_items and isinstance(bundle.get('display_meta_json'), dict):
            meta_items = [bundle.get('display_meta_json')]

        filtered_items = []
        for meta in meta_items:
            if not isinstance(meta, dict):
                continue
            key = _display_meta_key(meta)
            if not key[0] or key[1] not in ('Movie', 'Series', 'Season', 'Episode'):
                continue
            scanned_meta_items += 1
            if key in seen_meta_keys:
                continue
            meta = _filter_display_meta_for_center_upload(meta)
            if not meta:
                continue
            seen_meta_keys.add(key)
            filtered_items.append(meta)

        if not filtered_items:
            skipped_empty += 1
            continue

        out = dict(bundle)
        out['display_meta_items_json'] = filtered_items
        out['display_meta_json'] = filtered_items[-1]
        out['_local_source_id'] = row.get('id')
        out['_source_kind'] = row.get('source_kind')
        bundles.append(out)

    return {
        'rows': rows,
        'bundles': bundles,
        'candidate_count': len(rows),
        'meta_item_count': len(seen_meta_keys),
        'scanned_meta_items': scanned_meta_items,
        'skipped_empty': skipped_empty,
    }


def _post_center_display_meta_backfill(bundles: List[Dict[str, Any]], *, batch_size: int = 100) -> Dict[str, Any]:
    base_url = _center_base_url_for_display_meta()
    headers = _center_request_headers_for_display_meta()
    if not base_url or not headers.get('X-Server-ID-Hash'):
        return {'ok': False, 'message': '共享中心 URL 或 Emby ServerID 未配置', 'posted_batches': 0, 'accepted_meta_items': 0}

    batch_size = max(1, min(int(batch_size or 100), 300))
    accepted_meta_items = 0
    accepted_bundles = 0
    posted_batches = 0
    errors = []
    url = f"{base_url}/api/v1/metadata/display/upsert"

    for start in range(0, len(bundles or []), batch_size):
        batch = bundles[start:start + batch_size]
        if not batch:
            continue
        try:
            with requests.post(
                url,
                headers=headers,
                json={'items': batch, 'skip_logical_share_dispatch': True},
                **_center_request_kwargs_for_display_meta(timeout=90),
            ) as resp:
                posted_batches += 1
                if resp.status_code >= 400:
                    raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
                data = resp.json() if resp.content else {}
            accepted_meta_items += _safe_int(data.get('accepted_meta_items'), 0)
            accepted_bundles += _safe_int(data.get('accepted_bundles'), 0)
            if data.get('errors'):
                errors.extend(data.get('errors') or [])
        except Exception as e:
            errors.append({'batch_start': start, 'error': str(e)[:500]})

    return {
        'ok': not errors,
        'posted_batches': posted_batches,
        'accepted_meta_items': accepted_meta_items,
        'accepted_bundles': accepted_bundles,
        'errors': errors[:20],
    }


def upload_center_display_metadata_for_library_item(processor=None, **kwargs) -> Dict[str, Any]:
    """补传单个媒体壳展示元数据，供追剧刷新等链路复用。"""
    candidate = _normalize_series_candidate_identity(_library_register_candidate_from_kwargs(kwargs))
    item_type = str(candidate.get('item_type') or '').strip()
    if item_type not in ('Movie', 'Series', 'Season', 'Episode'):
        return {'ok': False, 'message': f'unsupported item_type: {item_type or "-"}'}

    bundle = _center_display_meta_bundle_for_candidate(candidate)
    meta_items = bundle.get('display_meta_items_json') if isinstance(bundle.get('display_meta_items_json'), list) else []
    if not meta_items and isinstance(bundle.get('display_meta_json'), dict):
        meta_items = [bundle.get('display_meta_json')]
    filtered_items = [
        x for x in (_filter_display_meta_for_center_upload(item) for item in meta_items)
        if x
    ]
    if not filtered_items:
        return {'ok': False, 'message': 'no local display metadata'}

    out = dict(bundle)
    out['display_meta_items_json'] = filtered_items
    out['display_meta_json'] = filtered_items[-1]
    if kwargs.get('reason'):
        out['_reason'] = str(kwargs.get('reason'))

    posted = _post_center_display_meta_backfill([out], batch_size=1)
    return {
        'ok': bool(posted.get('ok')),
        'uploaded_meta_items': _safe_int(posted.get('accepted_meta_items'), 0),
        'uploaded_bundles': _safe_int(posted.get('accepted_bundles'), 0),
        'errors': posted.get('errors') or [],
    }


def _backfill_center_display_metadata(limit: int = 500) -> Dict[str, Any]:
    """共享维护：把本机 media_metadata 中的海报/简介/演职员补传到中心公共壳。"""
    built = _build_display_meta_backfill_bundles(limit=limit)
    bundles = built.get('bundles') or []
    if not bundles:
        return {
            'ok': True,
            'candidate_count': built.get('candidate_count', 0),
            'prepared_bundles': 0,
            'prepared_meta_items': 0,
            'uploaded_meta_items': 0,
            'skipped_empty': built.get('skipped_empty', 0),
        }

    posted = _post_center_display_meta_backfill(bundles, batch_size=100)
    uploaded = _safe_int(posted.get('accepted_meta_items'), 0)
    if uploaded:
        logger.info(
            "  ➜ [共享资源维护] 中心海报/元数据补齐完成：候选=%s，补传壳=%s，中心接受=%s，批次=%s",
            built.get('candidate_count', 0),
            len(bundles),
            uploaded,
            posted.get('posted_batches', 0),
        )
    elif posted.get('errors'):
        logger.warning(
            "  ➜ [共享资源维护] 中心海报/元数据补齐失败：候选=%s，待传=%s，错误=%s",
            built.get('candidate_count', 0),
            len(bundles),
            posted.get('errors'),
        )
    return {
        'ok': bool(posted.get('ok')),
        'candidate_count': built.get('candidate_count', 0),
        'prepared_bundles': len(bundles),
        'prepared_meta_items': built.get('meta_item_count', 0),
        'uploaded_meta_items': uploaded,
        'uploaded_bundles': _safe_int(posted.get('accepted_bundles'), 0),
        'posted_batches': posted.get('posted_batches', 0),
        'skipped_empty': built.get('skipped_empty', 0),
        'errors': posted.get('errors') or [],
    }

def _shared_maintenance_log_summary(result: Dict[str, Any]) -> str:
    result = result or {}
    parts = [f"监听={'已启动' if result.get('device_event_listener') else '未启动'}"]
    cleanup = result.get('offline_cleanup') if isinstance(result.get('offline_cleanup'), dict) else {}
    if cleanup:
        parts.append(f"失效清理={cleanup.get('disabled', 0)}/{cleanup.get('offline_found', 0)}")
        if cleanup.get('failed'):
            parts.append(f"下架失败={cleanup.get('failed')}")
    credit = result.get('credit') if isinstance(result.get('credit'), dict) else {}
    snapshot = credit.get('snapshot') if isinstance(credit.get('snapshot'), dict) else {}
    if snapshot:
        parts.append(f"贡献值={snapshot.get('credit', 0)}")
        parts.append(f"资源={snapshot.get('shared_sources', 0)}")
        parts.append(f"RAW={snapshot.get('raw_ffprobe', 0)}")
        parts.append(f"设备={snapshot.get('remote_devices', 0)}")
    reregister = result.get('non_effective_reregister') if isinstance(result.get('non_effective_reregister'), dict) else {}
    if reregister:
        parts.append(f"非有效重登记={reregister.get('reregistered', 0)}/{reregister.get('need_reregister', 0)}")
        if reregister.get('removed_bad_completed'):
            removed_items = reregister.get('removed_items') if isinstance(reregister.get('removed_items'), list) else []
            names = '、'.join([str(x.get('title') or '').strip() for x in removed_items[:3] if isinstance(x, dict) and str(x.get('title') or '').strip()])
            parts.append(f"不合格完结季删除={reregister.get('removed_bad_completed')}" + (f"（{names}）" if names else ''))
        if reregister.get('remove_failed'):
            parts.append(f"不合格删除失败={reregister.get('remove_failed')}")
        if reregister.get('failed'):
            parts.append(f"重登失败={reregister.get('failed')}")
    followup = result.get('airing_episode_backfill') if isinstance(result.get('airing_episode_backfill'), dict) else {}
    if followup:
        parts.append(f"追更补齐={followup.get('registered', 0)}/{followup.get('need_register', 0)}")
        if followup.get('skipped'):
            parts.append(f"追更跳过={followup.get('skipped')}")
        if followup.get('failed'):
            failed_items = followup.get('failed_items') if isinstance(followup.get('failed_items'), list) else []
            names = '、'.join([str(x.get('title') or '').strip() for x in failed_items[:3] if isinstance(x, dict) and str(x.get('title') or '').strip()])
            parts.append(f"补登失败={followup.get('failed')}" + (f"（{names}）" if names else ''))
    quality_backfill = result.get('quality_source_backfill') if isinstance(result.get('quality_source_backfill'), dict) else {}
    if quality_backfill:
        parts.append(f"来源补齐={quality_backfill.get('reregistered', 0)}/{quality_backfill.get('checked', 0)}")
        if quality_backfill.get('skipped'):
            parts.append(f"来源跳过={quality_backfill.get('skipped')}")
        if quality_backfill.get('failed'):
            parts.append(f"来源补齐失败={quality_backfill.get('failed')}")
    display_meta = result.get('display_meta_backfill') if isinstance(result.get('display_meta_backfill'), dict) else {}
    if display_meta:
        parts.append(f"海报元数据补齐={display_meta.get('uploaded_meta_items', 0)}/{display_meta.get('prepared_meta_items', 0)}")
        if display_meta.get('errors'):
            parts.append(f"元数据补齐失败={len(display_meta.get('errors') or [])}")
    raw_repair = result.get('raw_repair_backfill') if isinstance(result.get('raw_repair_backfill'), dict) else {}
    if raw_repair:
        parts.append(f"残缺RAW补齐={raw_repair.get('uploaded', 0)}/{raw_repair.get('checked', 0)}")
        if raw_repair.get('missing_local'):
            parts.append(f"本地缺失RAW={raw_repair.get('missing_local')}")
    intro_backfill = result.get('intro_backfill') if isinstance(result.get('intro_backfill'), dict) else {}
    intro_disabled = intro_backfill.get('skipped') is True and bool(intro_backfill.get('reason'))
    if intro_backfill and not intro_disabled:
        parts.append(f"片头补齐={intro_backfill.get('uploaded', 0)}/{intro_backfill.get('scanned', 0)}")
        if intro_backfill.get('skipped'):
            parts.append(f"片头跳过={intro_backfill.get('skipped')}")
        if intro_backfill.get('failed'):
            parts.append(f"片头补齐失败={intro_backfill.get('failed')}")
    share_repair = result.get('logical_season_share_repair') if isinstance(result.get('logical_season_share_repair'), dict) else {}
    if share_repair:
        parts.append(f"分享回填={share_repair.get('backfilled', 0)}/{share_repair.get('missing_share_code', 0)}")
        if share_repair.get('deleted_untracked_invalid'):
            parts.append(f"未登记违规分享清理={share_repair.get('deleted_untracked_invalid')}")
        if share_repair.get('ambiguous'):
            parts.append(f"分享回填待确认={share_repair.get('ambiguous')}")
    share_reconcile = result.get('logical_season_share_full_reconcile') if isinstance(result.get('logical_season_share_full_reconcile'), dict) else {}
    if share_reconcile:
        parts.append(f"分享全量对账={share_reconcile.get('valid_logical_share_channels', 0)}/{share_reconcile.get('checked', 0)}")
        if not share_reconcile.get('ok') and share_reconcile.get('message'):
            parts.append(f"分享全量对账跳过={share_reconcile.get('message')}")
    if credit:
        parts.append(f"同步流水={credit.get('synced_ledger', 0)}")
    for key in ('listener_error', 'offline_cleanup_error', 'non_effective_reregister_error', 'airing_episode_backfill_error', 'quality_source_backfill_error', 'display_meta_backfill_error', 'raw_repair_backfill_error', 'intro_backfill_error', 'logical_season_share_repair_error', 'credit_error'):
        if result.get(key):
            parts.append(f"{key}={result.get(key)}")
    return '，'.join(parts)


def task_shared_resource_maintenance(processor=None, maintenance_silent: bool = False):
    if not _enabled():
        if not maintenance_silent:
            logger.info('  ➜ [共享资源维护] 共享资源未启用，跳过。')
        return {'ok': False, 'message': '共享资源未启用'}
    result = {'ok': True}
    try:
        result['device_event_listener'] = ensure_shared_device_event_listener()
    except Exception as e:
        result['listener_error'] = str(e)
    try:
        result['offline_cleanup'] = _cleanup_offline_local_sources(limit=300)
    except Exception as e:
        result['offline_cleanup_error'] = str(e)
    try:
        result['non_effective_reregister'] = _reregister_non_effective_local_sources(limit=300)
    except Exception as e:
        result['non_effective_reregister_error'] = str(e)
    try:
        result['airing_episode_backfill'] = _backfill_airing_episode_sources(limit=500)
    except Exception as e:
        result['airing_episode_backfill_error'] = str(e)
    try:
        result['quality_source_backfill'] = _backfill_center_missing_quality_sources(limit=300)
    except Exception as e:
        result['quality_source_backfill_error'] = str(e)
    try:
        result['display_meta_backfill'] = _backfill_center_display_metadata(limit=3000)
    except Exception as e:
        result['display_meta_backfill_error'] = str(e)
    try:
        result['raw_repair_backfill'] = _backfill_center_raw_repair_queue(limit=200)
    except Exception as e:
        result['raw_repair_backfill_error'] = str(e)
    try:
        from handler.shared_intro_service import scan_and_upload_local_intro
        result['intro_backfill'] = scan_and_upload_local_intro(limit=1000)
    except Exception as e:
        result['intro_backfill_error'] = str(e)
    try:
        result['logical_season_share_repair'] = repair_logical_season_share_channels_from_115(max_pages=20, dry_run=False)
    except Exception as e:
        result['logical_season_share_repair_error'] = str(e)
    try:
        result['logical_season_share_full_reconcile'] = reconcile_logical_season_share_channels_full(max_pages=50)
    except Exception as e:
        result['logical_season_share_full_reconcile_error'] = str(e)
    try:
        result['credit'] = _sync_center_credit()
    except Exception as e:
        result['credit_error'] = str(e)
    if not maintenance_silent:
        logger.info(f"  ➜ [共享资源维护] Rapid v2 维护完成：{_shared_maintenance_log_summary(result)}")
    return result


def trigger_shared_resource_maintenance_task() -> bool:
    try:
        return bool(task_manager.submit_task(task_shared_resource_maintenance, task_name='共享资源维护', processor_type='media'))
    except Exception:
        threading.Thread(target=task_shared_resource_maintenance, name='shared-rapid-maintenance', daemon=True).start()
        return True


def trigger_share_all_library_task() -> bool:
    try:
        return bool(task_manager.submit_task(share_all_library, task_name='一键登记媒体库', processor_type='media'))
    except Exception:
        threading.Thread(target=share_all_library, name='shared-rapid-share-all', daemon=True).start()
        return True


def task_shared_share_status_sync_high_freq(processor=None, maintenance_silent: bool = True):
    """系统硬编码高频后台任务入口。

    周期由 scheduler_manager.py 固定控制，不进入用户可配置任务链；
    本任务只做两件事：
    1. 向中心端签到，供中心按 3 次缺失判定客户端离线；
    2. 同步逻辑完结季 115 文件列表分享状态。

    completed_season 分享链路已停用，不再同步、不再上报。
    maintenance_silent 参数仅为兼容 scheduler_manager.py 旧调用签名保留。
    """
    heartbeat = {}
    try:
        # 签到必须放在分享状态同步前面：即便 115 share_list 或同步逻辑异常，
        # 也不能让中心误判本客户端离线。
        heartbeat = _report_share_sync_heartbeat({'stage': 'task_start'})
    except Exception as e:
        heartbeat = {'ok': False, 'error': str(e)}
        logger.warning(f"  ➜ [共享资源] 分享同步中心签到失败：{e}")

    try:
        share_sync = _sync_completed_season_share_channels_once(limit=50)
    except Exception as e:
        share_sync = {'ok': False, 'checked': 0, 'error': str(e)}
        logger.warning(f"  ➜ [共享资源] 逻辑季分享状态同步失败：{e}")

    final_heartbeat = {}
    if share_sync.get('ok'):
        try:
            final_heartbeat = _report_share_sync_heartbeat(
                {
                    'stage': 'task_done',
                    'checked': share_sync.get('checked', 0),
                },
            )
        except Exception as e:
            final_heartbeat = {'ok': False, 'error': str(e)}
            logger.warning(f"  ➜ [共享资源] 分享有效清单上报失败：{e}")

    return {
        'ok': True,
        'share_sync_heartbeat': heartbeat,
        'share_sync_final_heartbeat': final_heartbeat,
        'logical_season_share_sync': share_sync,
    }
