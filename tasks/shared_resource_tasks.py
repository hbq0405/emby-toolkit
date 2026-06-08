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
from database import shared_credit_db, shared_share_db, settings_db
from database.connection import get_db_connection
from handler.shared_center_client import SharedCenterClient, shared_center_enabled
from handler import shared_subscription_service as shared_subscription_service
from handler.shared_subscription_service import poll_and_consume_once as _raw_poll_and_consume_once
from handler import tmdb as tmdb_handler

logger = logging.getLogger(__name__)

_LISTENER_THREAD = None
_SIGN_LISTENER_THREAD = None
_LISTENER_STOP = threading.Event()
_LISTENER_LOCK = threading.Lock()
_FULL_SHARE_LOCK = threading.Lock()


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


def _enabled() -> bool:
    return _shared_resource_switch_enabled() and shared_center_enabled()


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


def _save_preid_to_p115_cache(file_info: Dict[str, Any], preid: str) -> None:
    preid = _norm_preid(preid)
    if not preid:
        return
    sha1 = _norm_sha1(file_info.get('sha1'))
    fid = str(file_info.get('fid') or file_info.get('file_id') or '').strip()
    pc = str(file_info.get('pick_code') or file_info.get('pc') or '').strip()
    clauses, args = [], []
    if fid:
        clauses.append('id=%s')
        args.append(fid)
    if pc:
        clauses.append('pick_code=%s')
        args.append(pc)
    if sha1:
        clauses.append('UPPER(sha1)=%s')
        args.append(sha1)
    if not clauses:
        return
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE p115_filesystem_cache
                    SET preid=%s, updated_at=NOW()
                    WHERE {' OR '.join(clauses)}
                    """,
                    [preid, *args],
                )
            conn.commit()
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 回写 p115_filesystem_cache.preid 失败: {e}")


def _lookup_preid_from_p115_cache(file_info: Dict[str, Any]) -> str:
    meta = file_info.get('rapid_meta_json') if isinstance(file_info.get('rapid_meta_json'), dict) else {}
    preid = _norm_preid(file_info.get('preid') or meta.get('preid') or meta.get('pre_sha1') or meta.get('pre_sha1_128k'))
    if preid:
        return preid
    sha1 = _norm_sha1(file_info.get('sha1'))
    fid = str(file_info.get('fid') or file_info.get('file_id') or '').strip()
    pc = str(file_info.get('pick_code') or file_info.get('pc') or meta.get('pick_code') or meta.get('pc') or '').strip()
    clauses, args = [], []
    if fid:
        clauses.append('id=%s')
        args.append(fid)
    if pc:
        clauses.append('pick_code=%s')
        args.append(pc)
    if sha1:
        clauses.append('UPPER(sha1)=%s')
        args.append(sha1)
    if not clauses:
        return ''
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT preid
                    FROM p115_filesystem_cache
                    WHERE {' OR '.join(clauses)}
                      AND preid IS NOT NULL AND preid <> ''
                    ORDER BY updated_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    args,
                )
                row = cur.fetchone()
                return _norm_preid((row or {}).get('preid')) if row else ''
    except Exception:
        return ''


def _ensure_file_preid(file_info: Dict[str, Any]) -> str:
    """确保单个 115 文件拥有 preid。

    preid = 文件前 128KB SHA1，是 115 upload/init 的基础秒传参数。
    只读取 128KB，不读取完整文件；计算结果写回 p115_filesystem_cache，后续登记中心一同带上。
    """
    if not isinstance(file_info, dict):
        return ''
    preid = _lookup_preid_from_p115_cache(file_info)
    if not preid:
        pc = str(file_info.get('pick_code') or file_info.get('pc') or '').strip()
        chunk = _p115_range_bytes_by_pick_code(pc, 0, 131071)
        if chunk:
            preid = hashlib.sha1(chunk).hexdigest().upper()
            _save_preid_to_p115_cache(file_info, preid)
            logger.info(f"  ➜ [共享资源] 已计算并缓存 preid: {file_info.get('file_name') or file_info.get('name') or file_info.get('sha1')} -> {preid[:12]}...")
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


def _consume_device_event_with_transfer_gate(original_consume, event, *args, **kwargs):
    source = _event_source_payload(event)
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
    return original_consume(event, *args, **kwargs)


def poll_and_consume_once(*args, **kwargs):
    """长轮询消费前加本地配置门禁，保证自动秒传也能拦截纯净版/短剧。"""
    original = getattr(shared_subscription_service, 'consume_device_event', None)
    if not callable(original) or getattr(original, '_etk_transfer_gate_wrapped', False):
        return _raw_poll_and_consume_once(*args, **kwargs)

    def _wrapped(event, *a, **kw):
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
    row = shared_share_db.raw_ffprobe_for_sha1(sha1) or {}
    raw = row.get('raw_ffprobe_json') if isinstance(row.get('raw_ffprobe_json'), dict) else {}
    if not raw:
        return {}
    # 补齐中心需要的 _etk。中心会清理 cookie/pc/url，不泄露 CK。
    raw = dict(raw)
    etk = raw.get('_etk') if isinstance(raw.get('_etk'), dict) else {}
    etk = dict(etk or {})
    etk.setdefault('sha1', sha1)
    if file_info.get('tmdb_id'):
        etk.setdefault('tmdb_id', str(file_info.get('tmdb_id')))
    if file_info.get('item_type') in ('Movie', 'Episode', 'Season'):
        etk.setdefault('type', 'movie' if file_info.get('item_type') == 'Movie' else 'tv')
    if file_info.get('season_number') not in (None, ''):
        etk.setdefault('season_number', _safe_int(file_info.get('season_number')))
    if file_info.get('episode_number') not in (None, ''):
        etk.setdefault('episode_number', _safe_int(file_info.get('episode_number')))
    raw['_etk'] = etk
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
    return _apply_short_drama_meta(sig, raw, source)


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
        'audio_list': audio_list[:16],
        'subtitle_list': subtitle_list[:24],
        'audios': [{'display': x} for x in audio_list[:16]],
        'subtitles': [{'display': x} for x in subtitle_list[:24]],
        'formatted_by': 'emby_mediainfo' if media_info else 'raw_fallback',
    }
    return _apply_short_drama_meta(summary, raw, source)


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
        'duration_minutes', 'is_short_drama', 'short_drama_meta_json',
    }
    compact = {k: summary.get(k) for k in allowed_keys if k in summary}
    for key, max_len in (('audio_list', 16), ('subtitle_list', 24), ('audios', 16), ('subtitles', 24)):
        value = compact.get(key)
        if isinstance(value, list):
            compact[key] = value[:max_len]

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


def _upload_raw_batch(client: SharedCenterClient, files: List[Dict[str, Any]]) -> Dict[str, Any]:
    entries = []
    for f in files or []:
        entry = _prepare_raw_upload_entry(f)
        if entry:
            entries.append(entry)
    if not entries:
        return {'ok': True, 'uploaded': {}, 'count': 0, 'errors': []}

    uploaded = {}
    errors = []
    try:
        if hasattr(client, 'upload_raw_ffprobe_batch'):
            resp = client.upload_raw_ffprobe_batch(entries)
            ok_items = resp.get('items') or resp.get('uploaded') or []
            for item in ok_items:
                sha = _norm_sha1((item or {}).get('sha1'))
                if sha:
                    uploaded[sha] = True
            # 中心旧版本可能只返回 count；这种情况下视本批次都成功，避免回退逐个再刷屏。
            if not uploaded and int(resp.get('count') or 0) == len(entries) and not resp.get('errors'):
                uploaded = {_norm_sha1(x.get('sha1')): True for x in entries if _norm_sha1(x.get('sha1'))}
            errors = resp.get('errors') or []
            if uploaded:
                return {'ok': not errors, 'uploaded': uploaded, 'count': len(uploaded), 'errors': errors}
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] RAW 批量上传失败，回退逐个上传: {e}")

    # 兼容未升级中心：单个逐个传，但只作为 fallback。
    for entry in entries:
        sha = _norm_sha1(entry.get('sha1'))
        try:
            client.upload_raw_ffprobe(sha, entry.get('raw_ffprobe_json') or {}, size=entry.get('size'), summary_json=entry.get('summary_json'))
            uploaded[sha] = True
        except Exception as e:
            errors.append({'sha1': sha, 'error': str(e)})
    return {'ok': not errors, 'uploaded': uploaded, 'count': len(uploaded), 'errors': errors}




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


def _center_raw_need_repair(client: SharedCenterClient, files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    sha1s = []
    by_sha1 = {}
    for f in files or []:
        sha1 = _norm_sha1((f or {}).get('sha1'))
        if sha1 and sha1 not in by_sha1:
            by_sha1[sha1] = f
            sha1s.append(sha1)
    if not sha1s:
        return []
    try:
        resp = client.raw_batch(sha1s)
    except Exception as e:
        logger.debug(f"  ➜ [共享资源维护] 查询中心 RAW 状态失败: {e}")
        return []
    need = []
    missing = set(_norm_sha1(x) for x in (resp.get('missing') or []) if _norm_sha1(x))
    for sha1 in missing:
        f = by_sha1.get(sha1) or {}
        need.append({'sha1': sha1, 'file_name': f.get('file_name') or f.get('name') or sha1, 'reason': 'center_raw_missing'})
    for item in resp.get('items') or []:
        sha1 = _norm_sha1((item or {}).get('sha1'))
        if not sha1:
            continue
        if item.get('raw_ready') is False:
            f = by_sha1.get(sha1) or {}
            need.append({'sha1': sha1, 'file_name': f.get('file_name') or f.get('name') or sha1, 'reason': item.get('raw_ready_reason') or 'center_raw_not_ready'})
            continue
        if not _summary_json_usable_for_center((item or {}).get('summary_json') or {}):
            f = by_sha1.get(sha1) or {}
            need.append({'sha1': sha1, 'file_name': f.get('file_name') or f.get('name') or sha1, 'reason': 'center_summary_missing'})
    return need

def _upload_raw_if_needed(client: SharedCenterClient, file_info: Dict[str, Any]) -> bool:
    entry = _prepare_raw_upload_entry(file_info)
    if not entry:
        return False
    client.upload_raw_ffprobe(entry['sha1'], entry['raw_ffprobe_json'], size=entry.get('size'), summary_json=entry.get('summary_json'))
    return True


def _file_payload_common(file_info: Dict[str, Any], raw_uploaded: bool = False, animation_meta: Dict[str, Any] = None) -> Dict[str, Any]:
    raw = _raw_for_file(file_info) if raw_uploaded else {}
    sig = _media_signature(raw, file_info) if raw else {}
    sig = _apply_animation_tag(sig, animation_meta)
    preid = _ensure_file_preid(file_info)
    # size 不能只信 p115_filesystem_cache。第三方 STRM/旧库补齐 RAW 时，
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
    rapid_meta = _apply_animation_tag(rapid_meta, animation_meta)
    return {
        'sha1': _norm_sha1(file_info.get('sha1')),
        'preid': preid or None,
        'size': final_size or None,
        'file_name': file_info.get('file_name') or file_info.get('name') or '',
        'quality': sig.get('resolution') or '',
        'has_raw_ffprobe': bool(raw_uploaded),
        'media_signature_json': sig,
        'rapid_meta_json': rapid_meta,
    }


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

    animation_genre = _short_drama_source_has_animation_genre(candidate)
    short_hits = [] if animation_genre else [ep for ep in episode_rows if 0 < float(ep.get('actual_runtime_minutes') or 0) < _SHORT_DRAMA_MAX_RUNTIME_MINUTES]
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
        logger.info(f"  ➜ [共享资源] 完结季包已可用，已停用同季单集源: tmdb={parent}, S{season_no:02d}, count={disabled}")
    return disabled


def _candidate_bool(candidate: Dict[str, Any], *keys: str) -> bool:
    for key in keys:
        value = (candidate or {}).get(key)
        if isinstance(value, bool):
            if value:
                return True
            continue
        text = str(value or '').strip().lower()
        if text in ('1', 'true', 'yes', 'y', 'on', 'completed', 'complete', 'ended', 'end', '完结', '已完结'):
            return True
    return False


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


def register_candidate_to_center(candidate: Dict[str, Any], *, source_provider: str = 'manual_rapid') -> Dict[str, Any]:
    """把本地媒体库中的电影/分集/季登记到 Rapid v2 中心。

    规则：
    - Movie：登记 movie_sources；
    - Episode：登记 season_episode_sources，追更池不做版本一致性要求；
    - Season：先把每集登记到追更池，锚定中心公共 season_hub；只有明确完结才登记 completed_season_source。
    """
    if not _enabled():
        return {'ok': False, 'message': '共享资源未启用或中心未配置'}
    candidate = _normalize_series_candidate_identity(dict(candidate or {}))
    item_type = str(candidate.get('item_type') or '').strip()
    raw_repair_only = bool(candidate.get('_raw_repair_only'))
    should_register_completed = item_type == 'Season' and _candidate_is_completed_season(
        candidate,
        source_provider=source_provider,
        files=None,
    )
    allow_consistency_check = bool(should_register_completed)

    files = shared_share_db.collect_files_for_candidate(candidate)
    repair_result = {}
    if not files and allow_consistency_check:
        # 只有真正登记完结季包时，才允许走一致性/指纹修复链路。
        # RAW/summary 补齐、维护任务、手动重新登记只做已有本地文件的 RAW 上传，
        # 不能在维护任务里对所有连载季刷一致性校验。
        repair_result = shared_share_db.repair_candidate_fingerprints(candidate, log_result=True)
        files = shared_share_db.collect_files_for_candidate(candidate)
    if not files:
        return {
            'ok': False,
            'message': '未找到可共享的视频文件，请先确认 p115_filesystem_cache / media_metadata 已补齐 SHA1、PC 和大小',
            'fingerprint_repair': repair_result or {},
        }

    completed_consistency_gate = {}
    if allow_consistency_check:
        completed_consistency_gate = _completed_season_consistency_gate(candidate, log_result=True)
        if not completed_consistency_gate.get('ok'):
            return {
                'ok': False,
                'skipped': True,
                'reason': 'completed_season_consistency_failed',
                'message': completed_consistency_gate.get('message') or '完结季一致性校验未通过，禁止登记中心',
                'consistency': completed_consistency_gate.get('consistency') or {},
                'fingerprint_repair': repair_result or {},
            }

    # RAW 摘要生成发生在正式登记前，先把候选类型补进 file_info。
    # 否则电影/剧集的时长标签门禁拿不到 item_type，容易把电影短片误判成短剧。
    candidate_tmdb_id = str(candidate.get('parent_series_tmdb_id') or candidate.get('series_tmdb_id') or candidate.get('tmdb_id') or '').strip()
    if item_type == 'Movie':
        candidate_tmdb_id = str(candidate.get('tmdb_id') or '').strip()
    for _f in files:
        if item_type == 'Movie':
            _f.setdefault('item_type', 'Movie')
            _f.setdefault('tmdb_id', candidate_tmdb_id)
        elif item_type in ('Season', 'Episode'):
            _f.setdefault('item_type', 'Episode')
            _f.setdefault('tmdb_id', candidate_tmdb_id)
            _f.setdefault('parent_series_tmdb_id', candidate_tmdb_id)
            _f.setdefault('series_tmdb_id', candidate_tmdb_id)
            _f.setdefault('season_number', candidate.get('season_number'))
            if item_type == 'Episode':
                _f.setdefault('episode_number', candidate.get('episode_number'))
        _ensure_file_preid(_f)
    root = shared_share_db.candidate_root_from_files(files)
    client = SharedCenterClient()
    raw_batch_result = _upload_raw_batch(client, files)
    uploaded_sha1s = raw_batch_result.get('uploaded') or {}
    uploaded = int(raw_batch_result.get('count') or 0)
    errors = list(raw_batch_result.get('errors') or [])
    raw_missing = _raw_batch_missing_for_files(files, uploaded_sha1s)
    if raw_missing:
        names = '、'.join([str(x.get('file_name') or x.get('sha1')) for x in raw_missing[:5]])
        if len(raw_missing) > 5:
            names += f" 等 {len(raw_missing)} 个"
        return {
            'ok': False,
            'message': f'RAW/summary_json 缺失，已拒绝登记中心：{names}',
            'raw_uploaded_count': uploaded,
            'missing_raw': raw_missing,
            'errors': errors,
            'fingerprint_repair': repair_result or {},
        }
    animation_meta = _animation_meta_for_candidate(candidate)
    results = []
    if item_type == 'Season' and not should_register_completed:
        should_register_completed = _candidate_is_completed_season(candidate, source_provider=source_provider, files=files)
        allow_consistency_check = bool(should_register_completed)
    register_episode_pool = not (item_type == 'Season' and should_register_completed)
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
            common = _file_payload_common(f, raw_uploaded=raw_ok, animation_meta=animation_meta)
            if item_type == 'Movie':
                payload = {
                    'tmdb_id': tmdb_id,
                    'item_type': 'Movie',
                    'title': candidate.get('title'),
                    'release_year': candidate.get('release_year'),
                    'source_provider': source_provider,
                    **common,
                }
                resp = client.register_movie_source(payload)
                center_item = resp.get('item') or {}
                local = shared_share_db.upsert_local_source({
                    'source_kind': 'movie', 'center_source_id': center_item.get('source_id'), 'tmdb_id': tmdb_id, 'item_type': 'Movie',
                    'title': candidate.get('title'), 'release_year': candidate.get('release_year'), 'sha1': common.get('sha1'), 'preid': common.get('preid'),
                    'size': common.get('size'), 'file_name': common.get('file_name'), 'source_provider': source_provider,
                    'root_fid': root.get('root_fid'), 'root_name': root.get('root_name'),
                    'status': 'active', 'center_status': 'reported', 'media_signature_json': common.get('media_signature_json'),
                    'rapid_meta_json': common.get('rapid_meta_json'), 'raw_json': {'candidate': candidate, 'center_response': resp},
                })
                shared_share_db.replace_source_files(local['id'], [{**f, 'raw_ffprobe_uploaded': raw_ok, **common}])
                results.append(resp)
            else:
                if not register_episode_pool:
                    continue
                ep_no = _safe_int_or_none(f.get('episode_number')) or _safe_int_or_none(candidate.get('episode_number'))
                season_no = _safe_int_or_none(f.get('season_number')) or _safe_int_or_none(candidate.get('season_number'))
                if season_no is None or ep_no is None:
                    errors.append({'file': f.get('file_name'), 'error': '缺少 season_number/episode_number'})
                    continue
                expected_count = _safe_int_or_none(candidate.get('expected_episode_count') or candidate.get('total_episodes') or candidate.get('episode_count'))
                payload = {
                    'tmdb_id': tmdb_id,
                    'item_type': 'Episode',
                    'season_number': season_no,
                    'episode_number': ep_no,
                    'title': candidate.get('title'),
                    'release_year': candidate.get('release_year'),
                    'expected_episode_count': expected_count,
                    'source_provider': source_provider,
                    **common,
                }
                resp = client.register_episode_source(payload)
                center_item = resp.get('item') or {}
                local = shared_share_db.upsert_local_source({
                    'source_kind': 'episode', 'center_source_id': center_item.get('source_id'), 'tmdb_id': tmdb_id,
                    'item_type': 'Episode', 'season_number': season_no, 'episode_number': ep_no,
                    'title': candidate.get('title'), 'release_year': candidate.get('release_year'), 'sha1': common.get('sha1'), 'preid': common.get('preid'),
                    'size': common.get('size'), 'file_name': common.get('file_name'), 'source_provider': source_provider,
                    'root_fid': root.get('root_fid'), 'root_name': root.get('root_name'),
                    'status': 'active', 'center_status': 'reported', 'media_signature_json': common.get('media_signature_json'),
                    'rapid_meta_json': common.get('rapid_meta_json'), 'raw_json': {'candidate': candidate, 'center_response': resp},
                })
                shared_share_db.replace_source_files(local['id'], [{**f, 'raw_ffprobe_uploaded': raw_ok, **common}])
                results.append(resp)
        except Exception as e:
            logger.warning(f"  ➜ [共享资源] 登记分集/电影失败: {f.get('file_name') or f.get('sha1')} -> {e}")
            errors.append({'file': f.get('file_name') or f.get('sha1'), 'error': str(e)})

    completed_resp = None
    episode_cancelled = 0
    if item_type == 'Season' and should_register_completed:
        completed_files = []
        for f in files:
            sha1 = _norm_sha1(f.get('sha1'))
            if not sha1:
                continue
            raw = _raw_for_file(f)
            sig = _media_signature(raw, f) if raw else {}
            sig = _apply_animation_tag(sig, animation_meta)
            preid = _ensure_file_preid(f)
            final_size = _file_size_from_cache(f) or _infer_size_from_raw(raw) or 0
            if final_size > 0:
                f['size'] = final_size
            completed_files.append({
                'episode_number': _safe_int(f.get('episode_number'), 0), 'sha1': sha1, 'preid': preid or None, 'size': final_size or None,
                'file_name': f.get('file_name') or '', 'quality': sig.get('resolution') or '', 'media_signature_json': sig,
                'rapid_meta_json': _apply_animation_tag({'fid': f.get('fid'), 'pick_code': f.get('pick_code'), 'relative_path': f.get('relative_path'), 'preid': preid or ''}, animation_meta),
            })
        expected = _safe_int(candidate.get('expected_episode_count') or candidate.get('total_episodes'), 0)
        consistency = (completed_consistency_gate.get('consistency') if isinstance(completed_consistency_gate, dict) else {}) or {}
        status = _completed_status_from_files(completed_files, expected)
        common_signature = {}
        for _cf in completed_files:
            sig = _cf.get('media_signature_json') if isinstance(_cf.get('media_signature_json'), dict) else {}
            if sig:
                common_signature = sig
                break
        short_detection = _detect_short_drama_for_completed_season(candidate, completed_files, files)
        common_signature = dict(common_signature or {})
        common_signature = _apply_animation_tag(common_signature, animation_meta)
        common_signature['is_short_drama'] = bool(short_detection.get('is_short_drama'))
        common_signature['short_drama_meta_json'] = short_detection
        if isinstance(consistency, dict) and consistency:
            status = _completed_gate_status_from_consistency(consistency)
        clean_detection = {'is_clean_version': False, 'clean_version_checked': False, 'reason': 'status_not_available'}
        if status.get('status') == 'available':
            clean_detection = _detect_clean_version_for_completed_season(
                candidate,
                completed_files,
                files,
                source_provider=source_provider,
            )
            logger.info(
                "  ➜ [共享资源] 完结季纯净版识别结果: %s S%02d clean=%s checked=%s reason=%s comparable=%s hits=%s/%s",
                candidate.get('title') or tmdb_id,
                _safe_int(candidate.get('season_number'), 0),
                bool(clean_detection.get('is_clean_version')),
                bool(clean_detection.get('clean_version_checked')),
                clean_detection.get('reason') or '',
                clean_detection.get('comparable_count') or 0,
                clean_detection.get('hit_count') or 0,
                clean_detection.get('required_hits') or 0,
            )
        else:
            logger.debug(
                "  ➜ [共享资源] 完结季状态不是 available，跳过纯净版识别: %s S%s status=%s message=%s",
                candidate.get('title') or tmdb_id,
                candidate.get('season_number'),
                status.get('status'),
                status.get('message') or '',
            )
        is_clean_version = bool(clean_detection.get('is_clean_version'))
        clean_confidence = clean_detection.get('clean_version_confidence') if clean_detection.get('clean_version_checked') else None
        is_completed_certified = status.get('status') == 'available'
        completed_certified_meta = {
            'is_completed_certified': bool(is_completed_certified),
            'certified_by': 'season_consistency_check',
            'status': status.get('status'),
            'message': status.get('message') or '',
            'expected_episode_count': expected or None,
            'file_count': len(completed_files),
            'consistency': consistency if isinstance(consistency, dict) else {},
        } if is_completed_certified else {}
        common_signature = _apply_completed_certified_tag(common_signature, completed_certified_meta)
        season_rapid_meta = _apply_animation_tag({
            'root_fid': root.get('root_fid'),
            'root_name': root.get('root_name'),
        }, animation_meta)
        season_rapid_meta = _apply_completed_certified_tag(season_rapid_meta, completed_certified_meta)
        try:
            payload = {
                'tmdb_id': tmdb_id,
                'item_type': 'Season',
                'season_number': _safe_int(candidate.get('season_number'), 0),
                'title': candidate.get('title'),
                'release_year': candidate.get('release_year'),
                'expected_episode_count': expected or None,
                'status': status['status'],
                'status_message': status['message'],
                'manifest_hash': shared_share_db.manifest_hash(completed_files),
                'source_provider': 'rapid_completed_season',
                'is_clean_version': is_clean_version,
                'clean_version_confidence': clean_confidence,
                'clean_version_meta_json': clean_detection,
                'media_signature_json': common_signature,
                'rapid_meta_json': season_rapid_meta,
                'files': completed_files,
            }
            completed_resp = client.register_completed_season_source(payload)
            center_item = completed_resp.get('item') or {}
            local = shared_share_db.upsert_local_source({
                'source_kind': 'completed_season', 'center_source_id': center_item.get('source_id'), 'tmdb_id': tmdb_id,
                'item_type': 'Season', 'season_number': candidate.get('season_number'), 'title': candidate.get('title'),
                'release_year': candidate.get('release_year'), 'source_provider': 'rapid_completed_season',
                'root_fid': root.get('root_fid'), 'root_name': root.get('root_name'),
                'status': status['status'], 'center_status': 'reported', 'manifest_hash': payload['manifest_hash'],
                'file_count': len(completed_files), 'total_size': sum(_safe_int(x.get('size'), 0) for x in completed_files),
                'is_clean_version': payload['is_clean_version'], 'clean_version_confidence': payload['clean_version_confidence'],
                'clean_version_meta_json': payload['clean_version_meta_json'], 'raw_json': {'candidate': candidate, 'center_response': completed_resp, 'status': status, 'consistency': consistency, 'clean_detection': clean_detection, 'short_detection': short_detection, 'animation_meta': animation_meta, 'completed_certified_meta': completed_certified_meta, 'root': root},
            })
            shared_share_db.replace_source_files(local['id'], [{**f, 'raw_ffprobe_uploaded': bool(_raw_for_file(f))} for f in files])
            if status.get('status') == 'available':
                episode_cancelled = _disable_local_episode_sources_for_completed_season(
                    tmdb_id,
                    candidate.get('season_number'),
                    center_client=client,
                )
        except Exception as e:
            logger.warning(f"  ➜ [共享资源] 登记完结季源失败: {candidate.get('title') or tmdb_id} -> {e}")
            errors.append({'completed_season': candidate.get('title') or tmdb_id, 'error': str(e)})

    return {
        'ok': bool(results or completed_resp),
        'registered_count': len(results),
        'raw_uploaded_count': uploaded,
        'completed_season': completed_resp,
        'episode_cancelled': episode_cancelled,
        'errors': errors,
        'root': root,
        'fingerprint_repair': repair_result or {},
        'message': (
            f"已登记 {len(results)} 个分集/电影源"
            + ("，已更新完结季源" + (f"，已停用 {episode_cancelled} 个同季单集源" if episode_cancelled else '') if completed_resp else ("，连载季已聚合到中心公共包" if item_type == 'Season' else ''))
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

def trigger_shared_rapid_register_for_library_item(processor=None, **kwargs) -> Dict[str, Any]:
    if not _enabled():
        return {'ok': False, 'created': 0, 'message': '共享资源未启用'}
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
    }
    if candidate.get('item_type') == 'Episode' and db_row.get('tmdb_id'):
        candidate['tmdb_id'] = db_row.get('tmdb_id')
        candidate['parent_series_tmdb_id'] = candidate.get('parent_series_tmdb_id') or kwargs.get('parent_series_tmdb_id')
    result = register_candidate_to_center(candidate, source_provider='rapid_auto_library')
    result['created'] = result.get('registered_count', 0)
    return result


def trigger_shared_auto_share_for_library_item(processor=None, **kwargs) -> Dict[str, Any]:
    """兼容旧调用名：Rapid v2 不创建 115 分享，只登记本地秒传源。"""
    return trigger_shared_rapid_register_for_library_item(processor, **kwargs)


def trigger_completed_season_pack_share_task(processor=None, *, parent_series_tmdb_id: str = '', season_number=None, title: str = '', year: str = '', **kwargs) -> Dict[str, Any]:
    """兼容旧调用名：完结季不再创建分享包，只更新 completed_season_source manifest。"""
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
        'is_clean_version': kwargs.get('is_clean_version', False),
        'clean_version_confidence': kwargs.get('clean_version_confidence'),
        'clean_version_meta_json': kwargs.get('clean_version_meta_json') or {},
    }
    result = register_candidate_to_center(candidate, source_provider='rapid_completed_season')
    result['created'] = result.get('registered_count', 0) + (1 if result.get('completed_season') else 0)
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
        skipped_completed_episode = int(candidate_stats.get('skipped_completed_episode') or 0)
        scanned = int(candidate_stats.get('scanned') or 0)
        existing_summary = candidate_stats.get('existing_index') or {}
        timings = candidate_stats.get('timings') or {}

        _task_status(10, f'扫描完成：媒体候选 {scanned}，已排除有效共享 {skipped_existing}，已屏蔽完结季分集 {skipped_completed_episode}，待登记 {total}。')
        logger.info(
            '  ➜ [共享资源] 一键登记媒体库扫描完成：扫描 %s，跳过已有有效共享 %s，屏蔽完结季分集 %s，跳过重复候选 %s，待登记 %s，已有索引=%s，耗时=%s',
            scanned, skipped_existing, skipped_completed_episode, skipped_duplicate, total, existing_summary, timings,
        )

        if total <= 0:
            msg = f'无需登记：扫描 {scanned} 个候选，已排除有效共享 {skipped_existing} 个，已屏蔽完结季分集 {skipped_completed_episode} 个。'
            _task_status(100, msg)
            logger.info(f"  ➜ [共享资源] 一键登记媒体库完成：{msg}")
            return {
                'ok': True,
                'total': 0,
                'success': 0,
                'failed': 0,
                'skipped_existing': skipped_existing,
                'skipped_duplicate': skipped_duplicate,
                'skipped_completed_episode': skipped_completed_episode,
                'scanned': scanned,
                'timings': timings,
                'message': msg,
            }

        ok = failed = 0
        skipped_bad_completed = 0
        items = []
        for idx, cand in enumerate(candidates, 1):
            if processor is not None and hasattr(processor, 'is_stop_requested') and processor.is_stop_requested():
                msg = f'任务已中断：已处理 {idx - 1}/{total}，成功 {ok}，失败 {failed}，不合格跳过 {skipped_bad_completed}。'
                _task_status(max(10, min(99, int(10 + ((idx - 1) / max(total, 1)) * 85))), msg)
                logger.info(f"  ➜ [共享资源] 一键登记媒体库中断：{msg}")
                return {
                    'ok': False,
                    'cancelled': True,
                    'total': total,
                    'success': ok,
                    'failed': failed,
                    'skipped_bad_completed': skipped_bad_completed,
                    'skipped_existing': skipped_existing,
                    'skipped_duplicate': skipped_duplicate,
                    'skipped_completed_episode': skipped_completed_episode,
                    'scanned': scanned,
                    'items': items[:50],
                    'timings': timings,
                    'message': msg,
                }

            title = cand.get('title') or cand.get('display_title') or cand.get('tmdb_id') or f'候选 {idx}'
            progress = max(10, min(95, int(10 + ((idx - 1) / max(total, 1)) * 85)))
            _task_status(progress, f'正在登记 {idx}/{total}：{title}（成功 {ok}，失败 {failed}，不合格跳过 {skipped_bad_completed}，已跳过 {skipped_existing}）')
            try:
                res = register_candidate_to_center(cand, source_provider='rapid_all_library')
                is_bad_completed_skip = (
                    not res.get('ok')
                    and res.get('reason') == 'completed_season_consistency_failed'
                )
                item = {
                    'title': title,
                    'ok': bool(res.get('ok')),
                    'skipped': bool(is_bad_completed_skip),
                    'reason': res.get('reason') or '',
                    'message': res.get('message') or '',
                }
                if res.get('ok'):
                    ok += 1
                elif is_bad_completed_skip:
                    skipped_bad_completed += 1
                    logger.info(f"  ➜ [共享资源] 一键登记跳过不合格完结季: {title} -> {res.get('message') or ''}")
                else:
                    failed += 1
                items.append(item)
                if idx % 10 == 0 or idx == total:
                    _task_status(
                        max(10, min(95, int(10 + (idx / max(total, 1)) * 85))),
                        f'一键登记进度：{idx}/{total}，成功 {ok}，失败 {failed}，不合格跳过 {skipped_bad_completed}，已跳过 {skipped_existing}。'
                    )
                    logger.info(f"  ➜ [共享资源] 一键登记媒体库进度：{idx}/{total}，成功 {ok}，失败 {failed}，不合格跳过 {skipped_bad_completed}，已跳过 {skipped_existing}")
            except Exception as e:
                failed += 1
                items.append({'title': title, 'ok': False, 'message': str(e)})
                logger.warning(f"  ➜ [共享资源] 一键登记媒体库失败: {title} -> {e}")

        msg = f'一键登记完成：扫描 {scanned}，跳过已有有效共享 {skipped_existing}，屏蔽完结季分集 {skipped_completed_episode}，完结季不合格跳过 {skipped_bad_completed}，登记成功 {ok}，失败 {failed}。'
        _task_status(100, msg)
        logger.info(f"  ➜ [共享资源] 一键登记媒体库完成：候选 {total}，成功 {ok}，失败 {failed}，屏蔽完结季分集 {skipped_completed_episode}，完结季不合格跳过 {skipped_bad_completed}，已跳过 {skipped_existing}")
        return {
            'ok': True,
            'total': total,
            'success': ok,
            'failed': failed,
            'skipped_bad_completed': skipped_bad_completed,
            'skipped_existing': skipped_existing,
            'skipped_duplicate': skipped_duplicate,
            'skipped_completed_episode': skipped_completed_episode,
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
                submit = client.submit_rapid_sign_job(job_id, {'status': 'failed', 'message': str(e)[:1000]})
            except Exception as submit_err:
                submit = {'ok': False, 'error': str(submit_err)}
            results.append({'job_id': job_id, 'ok': False, 'error': str(e), 'submit': submit})
    return {'ok': True, 'count': len(jobs), 'items': results}

def _sign_listener_loop():
    """独立处理中心 sign_job。

    不能和资源事件消费共用一个循环：资源事件长轮询可能阻塞 25 秒，
    而请求端正在同步等待 sign_val。签名任务必须独立长轮询，避免 pending
    阶段因为 holder 没及时领取而被中心误判超时。
    """
    logger.debug('  ➜ [共享签名监听] Rapid v2 sign_job 长轮询监听已启动。')
    while not _LISTENER_STOP.is_set():
        try:
            if not _enabled():
                time.sleep(5)
                continue
            poll_and_process_rapid_sign_jobs_once(timeout=20, limit=10)
        except Exception as e:
            logger.warning(f"  ➜ [共享签名监听] 本轮处理失败: {e}")
            time.sleep(3)
    logger.info('  ➜ [共享签名监听] Rapid v2 sign_job 长轮询监听已停止。')


def _event_listener_loop():
    logger.debug('  ➜ [共享事件监听] Rapid v2 长轮询监听已启动。')
    while not _LISTENER_STOP.is_set():
        try:
            if not _enabled():
                time.sleep(15)
                continue
            poll_and_consume_once(timeout=25, limit=10)
        except Exception as e:
            logger.warning(f"  ➜ [共享事件监听] 本轮处理失败: {e}")
            time.sleep(10)
    logger.info('  ➜ [共享事件监听] Rapid v2 长轮询监听已停止。')


def ensure_shared_device_event_listener() -> bool:
    global _LISTENER_THREAD, _SIGN_LISTENER_THREAD
    if not _enabled():
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
    snapshot = {
        'device_id': me.get('id'),
        'credit': int(me.get('credit') or 0),
        'wanted_gaps': int(stats.get('active_gap_devices') or 0),
        'shared_sources': int(stats.get('movie_sources') or 0) + int(stats.get('episode_sources') or 0) + int(stats.get('completed_season_sources') or 0),
        'raw_ffprobe': int(stats.get('raw_ffprobe') or 0),
        'remote_devices': int(stats.get('devices') or 0),
        'raw_json': {'me': me, 'stats': stats},
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
    elif source_kind in ('completed_season', 'episode', 'episode_group') or item_type in ('Season', 'Episode') or season is not None:
        # Rapid v2 手动补齐/重新登记按“季”粒度处理，避免单集源继续散铺。
        final_type = 'Season' if season is not None else 'Episode'
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
        '_original_source_kind': source_kind,
        '_original_source_provider': row.get('source_provider') or '',
        '_original_center_source_id': row.get('center_source_id') or '',
    }
    # 完结季源重新登记必须继续走 completed_season_source，不能降级成连载分集池。
    # 否则中心会出现同季公共 season_hub，甚至用单集版本摘要/大小污染整季展示。
    if source_kind == 'completed_season':
        candidate['watching_status'] = 'Completed'
        candidate['_force_completed_season'] = True
    return {k: v for k, v in candidate.items() if v not in (None, '')}


def _reregister_provider_for_row(row: Dict[str, Any], requested: str = '') -> str:
    """重新登记应保持原 provider，避免生成 manual_reregister 影子源。"""
    row = dict(row or {})
    source_kind = str(row.get('source_kind') or '').strip().lower()
    if source_kind == 'completed_season':
        return 'rapid_completed_season'
    original = str(row.get('source_provider') or '').strip()
    requested = str(requested or '').strip()
    if requested and requested != 'manual_reregister':
        return requested
    return original or 'manual_rapid'


def reregister_local_source(source_id: int, *, source_provider: str = '') -> Dict[str, Any]:
    row = shared_share_db.get_local_source(int(source_id or 0))
    if not row:
        return {'ok': False, 'message': '本地共享源不存在', 'source_id': source_id}
    candidate = _candidate_from_local_source(row)
    # “重新登记”是原共享源的原地修复：重新上传 RAW/summary_json，
    # provider 保持原值，完结季保持 completed_season_source，避免生成影子源或降级成连载分集池。
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
        'deleted': deleted,
    }



def _reregister_non_effective_local_sources(limit: int = 300) -> Dict[str, Any]:
    """维护任务：处理本地非有效共享源。

    - 完结季：先严格复检一致性；仍不通过就中心取消登记 + 本地删除，
      不再反复重登同一个垃圾 completed_season_source。
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

        candidate = _candidate_from_local_source(row)
        consistency_checked += 1
        gate = _completed_season_consistency_gate(candidate, log_result=True)
        label = _maintenance_candidate_label(candidate, fallback=row.get('title') or row.get('tmdb_id') or str(row.get('id') or ''))
        if gate.get('ok'):
            reregister_rows.append(row)
            logger.info(
                "  ➜ [共享资源维护] 非有效完结季复检通过，准备重新登记：%s，id=%s",
                label,
                row.get('id'),
            )
            continue

        consistency_failed += 1
        if gate.get('final_failure', True):
            removed = _delete_bad_completed_source_from_center_and_local(row, gate, client=client)
            if removed.get('ok'):
                removed_items.append(removed)
            else:
                remove_failed_items.append(removed)
            continue

        # 临时校验异常不直接删，保留到重登记/下次维护暴露错误。
        try:
            shared_share_db.update_local_source(int(row.get('id') or 0), last_error=gate.get('message') or '完结季一致性校验异常')
        except Exception:
            pass
        reregister_rows.append(row)
        logger.warning(
            "  ➜ [共享资源维护] 非有效完结季复检异常，暂不删除：%s，id=%s，原因=%s",
            label,
            row.get('id'),
            gate.get('message') or gate.get('reason'),
        )

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
    每个缺口按 Episode 粒度登记到中心公共 season_hub。
    """
    candidates = shared_share_db.list_unregistered_airing_episode_candidates(limit=limit)
    if not candidates:
        return {'ok': True, 'checked': 0, 'need_register': 0, 'registered': 0, 'failed': 0, 'items': []}

    registered = 0
    failed = 0
    items = []
    failed_items = []
    for cand in candidates:
        cand = dict(cand or {})
        # 双保险：维护补齐不能借道 collect_files_for_candidate 触发 helpers.check_season_consistency。
        cand['_skip_fingerprint_repair'] = True
        cand['_raw_repair_only'] = True
        label = _maintenance_candidate_label(cand)
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

    if registered or failed:
        sample = '；'.join([f"{x.get('title')}：{x.get('message')}" for x in failed_items[:5]])
        logger.info(
            "  ➜ [共享资源维护] 追更补齐完成：待补=%s，成功=%s，失败=%s%s",
            len(candidates),
            registered,
            failed,
            f"，失败明细={sample}" if sample else '',
        )
    return {
        'ok': failed == 0,
        'checked': len(candidates),
        'need_register': len(candidates),
        'registered': registered,
        'failed': failed,
        'failed_items': failed_items[:20],
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
        parts.append(f"缺口={snapshot.get('wanted_gaps', 0)}")
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
        if followup.get('failed'):
            failed_items = followup.get('failed_items') if isinstance(followup.get('failed_items'), list) else []
            names = '、'.join([str(x.get('title') or '').strip() for x in failed_items[:3] if isinstance(x, dict) and str(x.get('title') or '').strip()])
            parts.append(f"补登失败={followup.get('failed')}" + (f"（{names}）" if names else ''))
    if credit:
        parts.append(f"同步流水={credit.get('synced_ledger', 0)}")
    for key in ('listener_error', 'offline_cleanup_error', 'non_effective_reregister_error', 'airing_episode_backfill_error', 'credit_error'):
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
    """兼容旧任务名：Rapid v2 没有 115 分享状态同步，转为共享资源维护。"""
    return task_shared_resource_maintenance(processor=processor, maintenance_silent=maintenance_silent)
