# handler/shared_subscription_service.py
# Rapid v2 共享资源消费入口：中心调度，本机 CK 执行秒传/入库。
import json
import logging
import re
import threading
import time
from typing import Any, Dict, List, Tuple

import config_manager
import constants
from database import settings_db
from handler.p115_service import P115Service
from handler.shared_center_client import SharedCenterClient, shared_center_enabled

logger = logging.getLogger(__name__)

_ORGANIZE_KICK_LOCK = threading.Lock()
_LAST_ORGANIZE_KICK_AT = 0


def _kick_115_organize_detached(reason: str = '', delay: float = 3.0) -> Dict[str, Any]:
    global _LAST_ORGANIZE_KICK_AT
    now = time.time()
    with _ORGANIZE_KICK_LOCK:
        if now - _LAST_ORGANIZE_KICK_AT < 10:
            return {'started': False, 'message': '115 整理扫描刚触发过，本次不重复启动'}
        _LAST_ORGANIZE_KICK_AT = now

    def _runner():
        if delay and delay > 0:
            time.sleep(delay)
        try:
            from tasks.p115 import task_scan_and_organize_115
            logger.info(f"  ➜ [共享资源] 异步触发 115 待整理扫描: {reason or 'rapid-import'}")
            task_scan_and_organize_115()
        except Exception as e:
            logger.error(f"  ➜ [共享资源] 异步触发 115 待整理扫描失败: {e}", exc_info=True)

    threading.Thread(target=_runner, name='shared-rapid-import-organize', daemon=True).start()
    return {'started': True, 'message': '已异步触发 115 待整理扫描'}


def _cfg(name: str, fallback: str, default=None):
    key = getattr(constants, name, fallback)
    return (config_manager.APP_CONFIG or {}).get(key, default)


def _safe_int(value, default=0):
    try:
        if value in (None, ''):
            return default
        return int(float(value))
    except Exception:
        return default


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


def _target_cid() -> str:
    cid = str(_cfg('CONFIG_OPTION_115_SAVE_PATH_CID', 'p115_save_path_cid', '') or '').strip()
    if not cid or cid == '0':
        raise RuntimeError('未配置 115 待整理目录 CID（p115_save_path_cid），无法秒传共享资源')
    return cid


def _rapid_success(resp: Any) -> bool:
    if isinstance(resp, dict):
        if resp.get('state') is True or resp.get('success') is True:
            return True
        code = str(resp.get('errno') if resp.get('errno') is not None else resp.get('code') if resp.get('code') is not None else '')
        if code in ('0', '200'):
            return True
        data = resp.get('data') if isinstance(resp.get('data'), dict) else {}
        status = str(data.get('status') if data.get('status') is not None else resp.get('status') if resp.get('status') is not None else '')
        if status in ('2', 'success', 'done'):
            return True
    text = json.dumps(resp, ensure_ascii=False, default=str).lower() if isinstance(resp, (dict, list)) else str(resp or '').lower()
    return any(k in text for k in ('成功', '已存在', 'already', 'exist', 'success', '秒传成功'))


def _call_rapid_method(p115, *, target_cid: str, sha1: str, size: int, file_name: str, pick_code: str = '', rapid_meta: Dict[str, Any] = None):
    """适配不同 115 客户端的秒传方法名。CK 只在本机使用，不上传中心。"""
    rapid_meta = dict(rapid_meta or {})
    candidates = [
        ('rapid_upload', ({'cid': target_cid, 'target_cid': target_cid, 'sha1': sha1, 'size': size, 'file_size': size, 'file_name': file_name, **rapid_meta},)),
        ('upload_file_by_sha1', (target_cid, sha1, size, file_name)),
        ('fs_rapid_upload', (target_cid, sha1, size, file_name)),
        ('fs_upload_by_sha1', (target_cid, sha1, size, file_name)),
        ('upload_by_sha1', (target_cid, sha1, size, file_name)),
        ('add_file_by_sha1', (target_cid, sha1, size, file_name)),
        ('rapid_save', (target_cid, sha1, size, file_name)),
    ]
    last_error = None
    for method_name, args in candidates:
        method = getattr(p115, method_name, None)
        if not callable(method):
            continue
        try:
            return method(*args)
        except TypeError as e:
            last_error = e
            try:
                return method(cid=target_cid, target_cid=target_cid, sha1=sha1, size=size, file_size=size, file_name=file_name, **rapid_meta)
            except Exception as e2:
                last_error = e2
        except Exception as e:
            last_error = e
    if last_error:
        raise RuntimeError(f'调用 115 秒传接口失败：{last_error}')
    raise RuntimeError('当前 P115Service 客户端未提供秒传方法，请补充 rapid_upload/upload_file_by_sha1 等接口')


def rapid_save_file(file_info: Dict[str, Any], *, target_cid: str = '') -> Dict[str, Any]:
    p115 = P115Service.get_client()
    if not p115:
        raise RuntimeError('115 客户端未初始化')
    target_cid = str(target_cid or _target_cid()).strip()
    sha1 = _norm_sha1(file_info.get('sha1'))
    size = _safe_int(file_info.get('size'), 0)
    file_name = str(file_info.get('file_name') or file_info.get('name') or sha1).strip() or sha1
    if not sha1:
        raise RuntimeError('缺少合法 SHA1，无法秒传')
    if size <= 0:
        raise RuntimeError(f'缺少文件大小，无法秒传：{file_name}')
    resp = _call_rapid_method(
        p115,
        target_cid=target_cid,
        sha1=sha1,
        size=size,
        file_name=file_name,
        pick_code=str(file_info.get('pick_code') or ''),
        rapid_meta=file_info.get('rapid_meta_json') if isinstance(file_info.get('rapid_meta_json'), dict) else {},
    )
    return {'ok': _rapid_success(resp), 'response': resp, 'sha1': sha1, 'file_name': file_name, 'target_cid': target_cid}


def _event_payload(event: Dict[str, Any]) -> Dict[str, Any]:
    payload = event.get('payload_json') if isinstance(event.get('payload_json'), dict) else None
    if payload is None:
        payload = event.get('payload') if isinstance(event.get('payload'), dict) else {}
    return dict(payload or {})


def _event_sources(event: Dict[str, Any], client: SharedCenterClient) -> Tuple[str, str, List[Dict[str, Any]]]:
    payload = _event_payload(event)
    source_kind = str(event.get('source_kind') or payload.get('source_kind') or '').strip()
    source_id = str(event.get('source_ref_id') or payload.get('source_id') or payload.get('source_ref_id') or '').strip()
    if not source_kind:
        source_kind = str(payload.get('kind') or payload.get('item_type') or '').strip().lower()
        if source_kind == 'movie':
            source_kind = 'movie'
        elif source_kind == 'episode':
            source_kind = 'episode'
        elif source_kind in ('season', 'completed_season'):
            source_kind = 'completed_season'

    # 兼容中心返回的 completed season 包：列表接口只给源摘要，真正文件清单要再取 manifest。
    # 如果 manifest 为空，不能再显示“秒传完成 0/0”，这属于 manifest 缺失/旧数据，需要重新登记该季。
    if source_kind == 'completed_season':
        manifest = client.completed_season_manifest(source_id)
        files = (manifest.get('files') or manifest.get('items') or []) if isinstance(manifest, dict) else []
        if not files and isinstance(manifest, dict):
            data = manifest.get('data') if isinstance(manifest.get('data'), dict) else {}
            files = data.get('files') or data.get('items') or []
        if not files and isinstance(payload.get('files'), list):
            files = payload.get('files') or []
        files = [dict(f or {}) for f in files if isinstance(f, dict)]
        for f in files:
            f.setdefault('tmdb_id', payload.get('tmdb_id'))
            f.setdefault('item_type', 'Episode')
            f.setdefault('season_number', payload.get('season_number'))
        return source_kind, source_id, files

    file_info = dict(payload or {})
    file_info.setdefault('source_kind', source_kind)
    file_info.setdefault('source_id', source_id)
    return source_kind, source_id, [file_info]


def consume_device_event(event: Dict[str, Any], *, ack: bool = True) -> Dict[str, Any]:
    client = SharedCenterClient()
    event_id = str(event.get('event_id') or '')
    payload = _event_payload(event)

    # 调试/验收阶段允许本机手动秒传自己的共享源。
    # 中心长轮询仍会自然排除 provider=consumer 的事件，不影响线上调度。

    source_kind, source_id, files = _event_sources(event, client)
    if not source_kind or not source_id:
        if ack and event_id:
            client.ack_device_events([event_id], result='failed', message='事件缺少 source_kind/source_id')
        return {'ok': False, 'message': '事件缺少 source_kind/source_id', 'event_id': event_id, 'success_count': 0, 'total': 0, 'errors': []}

    if not files:
        message = '中心返回的文件清单为空，无法秒传；如果这是完结季收藏源，请重新手动登记/一键全库登记该季，让中心重建 manifest。'
        if ack and event_id:
            try:
                client.ack_device_events([event_id], result='failed', message=message)
            except Exception:
                pass
        return {
            'ok': False, 'message': message, 'event_id': event_id,
            'source_kind': source_kind, 'source_id': source_id,
            'success_count': 0, 'total': 0, 'errors': [{'error': message}],
        }

    target_cid = _target_cid()
    ok_count = 0
    errors = []
    for f in files:
        try:
            result = rapid_save_file(f, target_cid=target_cid)
            if result.get('ok'):
                ok_count += 1
            else:
                errors.append({'file': f.get('file_name') or f.get('sha1'), 'response': result.get('response')})
        except Exception as e:
            errors.append({'file': f.get('file_name') or f.get('sha1'), 'error': str(e)})

    if ok_count:
        try:
            client.report_transfer(source_kind, source_id, 'success', message=f'本机秒传成功 {ok_count}/{len(files)} 个文件')
        except Exception as e:
            logger.warning(f"  ➜ [共享资源] 上报秒传成功失败: {e}")
        _kick_115_organize_detached(reason=f'rapid:{source_kind}:{source_id}')
    else:
        try:
            client.report_transfer(source_kind, source_id, 'failed', message=json.dumps(errors, ensure_ascii=False)[:1000])
        except Exception:
            pass

    if ack and event_id:
        try:
            client.ack_device_events([event_id], result='ok' if ok_count else 'failed', message=f'秒传 {ok_count}/{len(files)}')
        except Exception as e:
            logger.debug(f"  ➜ [共享资源] ACK 中心事件失败: {e}")

    message = f'秒传完成：{ok_count}/{len(files)}' if ok_count else (errors[0].get('error') if errors and isinstance(errors[0], dict) and errors[0].get('error') else f'秒传失败：0/{len(files)}')
    return {
        'ok': ok_count > 0, 'message': message, 'event_id': event_id,
        'source_kind': source_kind, 'source_id': source_id,
        'success_count': ok_count, 'total': len(files), 'errors': errors
    }


def poll_and_consume_once(timeout: int = 25, limit: int = 5) -> Dict[str, Any]:
    if not shared_center_enabled():
        return {'ok': False, 'message': '共享资源未启用'}
    client = SharedCenterClient()
    if not client.ready:
        return {'ok': False, 'message': '共享中心未配置'}
    resp = client.poll_device_events(timeout=timeout, limit=limit)
    events = resp.get('items') or resp.get('events') or []
    results = [consume_device_event(event) for event in events]
    return {'ok': True, 'event_count': len(events), 'results': results}


def _build_gap_query(item: Dict[str, Any], title: str = '', tmdb_id=None, item_type: str = '', parent_tmdb_id=None, season_number=None, year='') -> Dict[str, Any]:
    item = item or {}
    typ = item_type or item.get('item_type') or 'Movie'
    if typ == 'Episode':
        typ = 'Season'
    parent = parent_tmdb_id or item.get('parent_series_tmdb_id') or item.get('series_tmdb_id')
    query_tmdb = str(parent or tmdb_id or item.get('tmdb_id') or '').strip()
    if typ == 'Movie':
        query_tmdb = str(tmdb_id or item.get('tmdb_id') or '').strip()
    return {
        'tmdb_id': query_tmdb,
        'item_type': typ,
        'season_number': season_number if season_number not in (None, '') else item.get('season_number'),
        'episode_number': None,
        'title': title or item.get('title'),
        'release_year': year or item.get('release_year'),
    }


def report_shared_gap(item: Dict[str, Any], title: str = '', tmdb_id=None, item_type: str = '', parent_tmdb_id=None, season_number=None, year='') -> bool:
    if not shared_center_enabled():
        return False
    client = SharedCenterClient()
    if not client.ready:
        logger.warning('  ➜ [共享资源] 已启用但中心地址/token 未配置，跳过缺口登记。')
        return False
    try:
        client.report_gaps([_build_gap_query(item, title, tmdb_id, item_type, parent_tmdb_id, season_number, year)])
        return True
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 登记缺口失败: {e}")
        return False


def _normalize_probe_item_for_center(raw: Dict[str, Any]) -> Dict[str, Any]:
    """把 subscriptions.py 传来的 prepared context 转成中心查询对象。

    subscriptions.py 传入的对象通常长这样：
    {item, tmdb_id, item_type, title, season_number, parent_tmdb_id, year}
    其中 Season 的 tmdb_id 可能是季自身 ID；中心 Rapid v2 必须使用父剧 TMDb + season_number。
    """
    raw = raw or {}
    if isinstance(raw.get('item'), dict):
        return _build_gap_query(
            raw.get('item') or {},
            title=raw.get('title') or '',
            tmdb_id=raw.get('tmdb_id'),
            item_type=raw.get('item_type') or '',
            parent_tmdb_id=raw.get('parent_tmdb_id') or raw.get('parent_series_tmdb_id') or raw.get('series_tmdb_id'),
            season_number=raw.get('season_number'),
            year=raw.get('year') or raw.get('release_year') or '',
        )
    return _build_gap_query(
        raw,
        title=raw.get('title') or '',
        tmdb_id=raw.get('tmdb_id'),
        item_type=raw.get('item_type') or '',
        parent_tmdb_id=raw.get('parent_tmdb_id') or raw.get('parent_series_tmdb_id') or raw.get('series_tmdb_id'),
        season_number=raw.get('season_number'),
        year=raw.get('year') or raw.get('release_year') or '',
    )


def batch_probe_shared_resources(items: List[Dict[str, Any]], limit_per_item: int = 200) -> Dict[str, Any]:
    if not shared_center_enabled():
        return {'supported': False, 'items': [], 'message': 'shared center disabled', 'by_key': {}}
    client = SharedCenterClient()
    queries = [_normalize_probe_item_for_center(x) for x in (items or []) if isinstance(x, dict)]
    queries = [q for q in queries if q.get('tmdb_id') and q.get('item_type')]
    if not queries:
        return {'supported': True, 'items': [], 'hit_count': 0, 'gap_count': 0, 'by_key': {}}
    resp = client.probe_subscriptions_batch(queries, limit_per_item=limit_per_item)
    # subscriptions.py 期待 by_key；这里按 Rapid v2 规范键补一个映射。
    by_key = {}
    for row in resp.get('items') or resp.get('results') or []:
        query = row.get('query') if isinstance(row.get('query'), dict) else {}
        key = row.get('request_key') or query.get('request_key')
        if not key:
            key = _subscription_probe_request_key(query)
        if key:
            by_key[key] = row
    resp['by_key'] = by_key
    resp.setdefault('supported', True)
    return resp


def _subscription_probe_request_key(query: Dict[str, Any]) -> str:
    query = query or {}
    return '|'.join([
        str(query.get('item_type') or ''),
        str(query.get('tmdb_id') or ''),
        str(query.get('season_number') if query.get('season_number') is not None else ''),
        str(query.get('episode_number') if query.get('episode_number') is not None else ''),
    ])


def _flatten_sources_from_probe(resp_or_row: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = resp_or_row or {}
    if isinstance(data.get('sources'), list):
        return [x for x in data.get('sources') or [] if isinstance(x, dict)]
    out = []
    for item in data.get('items') or data.get('results') or []:
        for src in item.get('sources') or []:
            if isinstance(src, dict):
                out.append(src)
    return out


def _reported_gap_from_probe(resp_or_row: Dict[str, Any]) -> bool:
    data = resp_or_row or {}
    if data.get('reported_gap') or data.get('gap') or data.get('status') in ('gap_registered', 'reported_gap'):
        return True
    if int(data.get('gap_count') or 0) > 0:
        return True
    for item in data.get('items') or data.get('results') or []:
        if item.get('reported_gap') or item.get('gap') or item.get('status') in ('gap_registered', 'reported_gap'):
            return True
    return False


def _consume_sources(sources: List[Dict[str, Any]], *, report_gap: bool = False) -> Dict[str, Any]:
    if not sources:
        return {'enabled': True, 'success': False, 'reported_gap': bool(report_gap), 'mode': 'rapid', 'count': 0}
    ok = 0
    errors = []
    tried = 0
    for src in sources[:20]:
        tried += 1
        event = {
            'event_id': '',
            'source_kind': src.get('source_kind'),
            'source_ref_id': src.get('source_id') or src.get('source_ref_id'),
            'payload_json': src,
        }
        result = consume_device_event(event, ack=False)
        if result.get('ok'):
            ok += int(result.get('success_count') or 1)
            # 电影 / 单集命中一个即可；完结季一次事件会包含多文件。
            if src.get('source_kind') in ('movie', 'episode'):
                break
        else:
            errors.extend(result.get('errors') or [{'source_id': src.get('source_id'), 'message': result.get('message')}])
    return {
        'enabled': True,
        'success': ok > 0,
        'reported_gap': bool(report_gap),
        'mode': 'rapid',
        'action_type': '共享资源秒传',
        'count': ok,
        'tried_sources': tried,
        'errors': errors,
    }


def try_consume_shared_resource(item: Dict[str, Any], title: str = '', tmdb_id=None, item_type: str = '', parent_tmdb_id=None, season_number=None, year='', **_kwargs) -> Dict[str, Any]:
    if not shared_center_enabled():
        return {'enabled': False, 'success': False, 'reported_gap': False}
    query = _build_gap_query(item or {}, title, tmdb_id, item_type, parent_tmdb_id, season_number, year)
    client = SharedCenterClient()
    try:
        resp = client.probe_subscriptions_batch([query], limit_per_item=50)
        sources = _flatten_sources_from_probe(resp)
        reported_gap = _reported_gap_from_probe(resp)
        result = _consume_sources(sources, report_gap=reported_gap)
        if not result.get('success') and not reported_gap:
            # list 没命中时，保险登记缺口，等待中心事件监听异步处理。
            try:
                client.report_gaps([query])
                result['reported_gap'] = True
            except Exception:
                pass
        return result
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 秒传中心资源失败: {e}")
        return {'enabled': True, 'success': False, 'reported_gap': False, 'message': str(e)}


def try_consume_preprobed_shared_resource(probe_row: Dict[str, Any] = None, item: Dict[str, Any] = None, **kwargs) -> Dict[str, Any]:
    sources = _flatten_sources_from_probe(probe_row or {})
    reported_gap = _reported_gap_from_probe(probe_row or {})
    if sources:
        return _consume_sources(sources, report_gap=reported_gap)
    if reported_gap:
        return {'enabled': True, 'success': False, 'reported_gap': True, 'mode': 'rapid', 'count': 0}
    return try_consume_shared_resource(item or {}, **kwargs)
