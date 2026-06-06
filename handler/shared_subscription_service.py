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


def _rapid_size_to_int(value, default=0) -> int:
    """把中心端/本地缓存里的 size / file_size / 0.69 GB 统一转成字节。"""
    try:
        if value in (None, '', [], {}):
            return default
        if isinstance(value, (int, float)):
            return int(value)
        text = str(value).strip().replace(',', '')
        if not text:
            return default
        if re.fullmatch(r'\d+(?:\.0+)?', text):
            return int(float(text))
        m = re.search(r'([0-9]+(?:\.[0-9]+)?)\s*(TB|GB|G|MB|M|KB|K|B)?', text, re.I)
        if not m:
            return default
        n = float(m.group(1))
        unit = (m.group(2) or 'B').upper()
        if unit == 'TB': n *= 1024 ** 4
        elif unit in ('GB', 'G'): n *= 1024 ** 3
        elif unit in ('MB', 'M'): n *= 1024 ** 2
        elif unit in ('KB', 'K'): n *= 1024
        return int(n)
    except Exception:
        return default


def _dict_size_candidates(data: Dict[str, Any]) -> List[Any]:
    if not isinstance(data, dict):
        return []
    values = []
    for key in ('size', 'file_size', 'filesize', 'size_bytes', 'fileSize', 'file_size_bytes', 'total_size'):
        values.append(data.get(key))
    for nested_key in ('rapid_meta_json', 'rapid_meta', 'media_signature_json', 'summary_json', 'raw_summary_json', 'version_summary'):
        nested = data.get(nested_key)
        if isinstance(nested, str):
            try:
                nested = json.loads(nested)
            except Exception:
                nested = None
        if isinstance(nested, dict):
            for key in ('size', 'file_size', 'filesize', 'size_bytes', 'fileSize', 'file_size_bytes'):
                values.append(nested.get(key))
    return values


def _lookup_p115_cache_for_file(file_info: Dict[str, Any]) -> Dict[str, Any]:
    from database.connection import get_db_connection
    from handler.p115_service import P115CacheManager
    sha1 = _norm_sha1((file_info or {}).get('sha1'))
    meta = file_info.get('rapid_meta_json') if isinstance(file_info.get('rapid_meta_json'), dict) else {}
    pick_code = str((file_info or {}).get('pick_code') or (file_info or {}).get('pc') or meta.get('pick_code') or meta.get('pc') or '').strip()
    if not sha1 and not pick_code:
        return {}
    manager_row = {}
    try:
        if pick_code and hasattr(P115CacheManager, 'get_file_cache_by_pickcode'):
            row = P115CacheManager.get_file_cache_by_pickcode(pick_code)
            if row:
                manager_row = dict(row)
        if not manager_row and sha1 and hasattr(P115CacheManager, 'get_file_cache_by_sha1'):
            row = P115CacheManager.get_file_cache_by_sha1(sha1)
            if row:
                manager_row = dict(row)
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 查询 P115CacheManager 补 size 失败: {e}")
    try:
        clauses, args = [], []
        if sha1:
            clauses.append("UPPER(sha1)=%s")
            args.append(sha1)
        if pick_code:
            clauses.append("pick_code=%s")
            args.append(pick_code)
        if not clauses:
            return {}
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id, parent_id, name, local_path, sha1, pick_code, preid, size, updated_at
                    FROM p115_filesystem_cache
                    WHERE {' OR '.join(clauses)}
                    ORDER BY CASE WHEN COALESCE(size,0) > 0 THEN 0 ELSE 1 END,
                             updated_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    args,
                )
                row = cur.fetchone()
                sql_row = dict(row) if row else {}
                if manager_row:
                    merged = dict(manager_row)
                    merged.update({k: v for k, v in sql_row.items() if v not in (None, '')})
                    return merged
                return sql_row
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 直接查询 p115_filesystem_cache 补 size/preid 失败: {e}")
    return manager_row or {}


def _normalize_rapid_file_info(file_info: Dict[str, Any]) -> Dict[str, Any]:
    info = dict(file_info or {})
    meta = info.get('rapid_meta_json') if isinstance(info.get('rapid_meta_json'), dict) else {}
    preid = _norm_sha1(info.get('preid') or meta.get('preid') or meta.get('pre_sha1') or meta.get('pre_sha1_128k'))
    sha1 = _norm_sha1(info.get('sha1') or info.get('file_sha1') or meta.get('sha1') or meta.get('file_sha1'))
    if sha1:
        info['sha1'] = sha1
    if preid:
        info['preid'] = preid
        meta = dict(meta)
        meta.setdefault('preid', preid)
        info['rapid_meta_json'] = meta
    file_name = str(info.get('file_name') or info.get('name') or meta.get('file_name') or meta.get('name') or '').strip()
    if file_name:
        info['file_name'] = file_name
    size = 0
    for candidate in _dict_size_candidates(info):
        size = _rapid_size_to_int(candidate, 0)
        if size > 0:
            break
    cache_row = None
    if size <= 0 or not preid:
        cache_row = _lookup_p115_cache_for_file(info)
        if cache_row:
            size = _rapid_size_to_int(cache_row.get('size'), 0)
            if not info.get('file_name') and cache_row.get('name'):
                info['file_name'] = cache_row.get('name')
            meta = dict(meta)
            if cache_row.get('id') and not meta.get('fid'):
                meta['fid'] = str(cache_row.get('id'))
            cache_preid = _norm_sha1(cache_row.get('preid'))
            if cache_row.get('pick_code') and not meta.get('pick_code'):
                meta['pick_code'] = str(cache_row.get('pick_code'))
            if cache_preid and not info.get('preid'):
                info['preid'] = cache_preid
                meta.setdefault('preid', cache_preid)
            if meta:
                info['rapid_meta_json'] = meta
    if size > 0:
        info['size'] = size
        info['file_size'] = size
    return info


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
        ('rapid_upload', ({'cid': target_cid, 'target_cid': target_cid, 'sha1': sha1, 'size': size, 'file_size': size, 'file_name': file_name, 'preid': rapid_meta.get('preid') or rapid_meta.get('pre_sha1') or '', **rapid_meta},)),
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



def _rapid_sign_request_from_response(resp: Any) -> Dict[str, Any]:
    """从 p115_service 的 status=7 响应里提取 sign_key/sign_check。"""
    if not isinstance(resp, dict):
        return {}
    candidates = [resp]
    for key in ('response', 'signed_response'):
        if isinstance(resp.get(key), dict):
            candidates.append(resp.get(key))
    for item in candidates:
        if not isinstance(item, dict):
            continue
        sign_key = item.get('_rapid_sign_key') or item.get('sign_key')
        sign_check = item.get('_rapid_sign_check') or item.get('sign_check')
        data = item.get('data') if isinstance(item.get('data'), dict) else {}
        sign_key = sign_key or data.get('sign_key')
        sign_check = sign_check or data.get('sign_check')
        if sign_key and sign_check:
            return {
                'sign_key': str(sign_key),
                'sign_check': str(sign_check),
                'backend': str(item.get('_rapid_sign_backend') or item.get('_rapid_upload_backend') or item.get('backend') or ''),
                'stage': str(item.get('_rapid_sign_stage') or ''),
                'required': bool(item.get('_rapid_sign_required', True)),
            }
    return {}


def _register_local_rapid_holder(client: SharedCenterClient, *, source_kind: str, source_id: str, file_info: Dict[str, Any], message_prefix: str = '') -> None:
    try:
        meta = file_info.get('rapid_meta_json') if isinstance(file_info.get('rapid_meta_json'), dict) else {}
        sha1 = _norm_sha1(file_info.get('sha1') or meta.get('sha1'))
        if not sha1:
            return
        client.register_rapid_sign_holder({
            'sha1': sha1,
            'size': _rapid_size_to_int(file_info.get('size') or file_info.get('file_size') or meta.get('size'), 0) or None,
            'source_kind': source_kind or file_info.get('source_kind') or '',
            'source_id': source_id or file_info.get('source_id') or file_info.get('source_ref_id') or '',
            'file_name': file_info.get('file_name') or file_info.get('name') or meta.get('file_name') or '',
            'preid': file_info.get('preid') or meta.get('preid') or '',
            'meta_json': {'from': 'rapid_transfer_success'},
        })
        logger.info(f"  ➜ [Rapid蜂群签名] 已登记本机为 holder：sha1={sha1[:12]}..., source={source_kind}:{source_id}")
    except Exception as e:
        logger.debug(f"  ➜ [Rapid蜂群签名] 登记本机 holder 失败: {e}")


def _retry_rapid_with_center_sign(*, client: SharedCenterClient, p115, file_info: Dict[str, Any], target_cid: str, sha1: str, size: int, file_name: str, rapid_meta: Dict[str, Any], first_resp: Any) -> Dict[str, Any]:
    sign_req = _rapid_sign_request_from_response(first_resp)
    if not sign_req:
        return {'ok': False, 'response': first_resp, 'skipped': True, 'message': '未发现 sign_key/sign_check'}
    source_kind = str(file_info.get('source_kind') or rapid_meta.get('source_kind') or '').strip()
    source_id = str(file_info.get('source_id') or file_info.get('source_ref_id') or rapid_meta.get('source_id') or rapid_meta.get('source_ref_id') or '').strip()
    if not source_kind or not source_id:
        logger.warning(
            f"  ➜ [Rapid蜂群签名] 秒传需要签名但缺少 source_kind/source_id，无法向中心创建 sign_job: "
            f"sha1={sha1[:12]}..., file={file_name}"
        )
        return {'ok': False, 'response': first_resp, 'skipped': True, 'message': '缺少 source_kind/source_id'}

    logger.warning(
        f"  ➜ [Rapid蜂群签名] 秒传返回 status=7，准备请求中心调度在线 holder："
        f"source={source_kind}:{source_id}, sha1={sha1[:12]}..., backend={sign_req.get('backend') or '-'}, "
        f"sign_check={sign_req.get('sign_check')}"
    )
    create_resp = client.create_rapid_sign_job({
        'source_kind': source_kind,
        'source_id': source_id,
        'sha1': sha1,
        'size': size,
        'file_name': file_name,
        'preid': rapid_meta.get('preid') or file_info.get('preid') or '',
        'backend': sign_req.get('backend') or '',
        'sign_key': sign_req.get('sign_key'),
        'sign_check': sign_req.get('sign_check'),
        'request_meta_json': {'stage': sign_req.get('stage') or '', 'target_cid': target_cid},
    })
    job_id = str(create_resp.get('job_id') or (create_resp.get('job') or {}).get('job_id') or '').strip()
    holder_id = str(create_resp.get('holder_id') or (create_resp.get('job') or {}).get('holder_id') or '').strip()
    if not job_id:
        raise RuntimeError(f'中心未返回 sign_job id: {create_resp}')
    logger.info(f"  ➜ [Rapid蜂群签名] sign_job 已创建：job_id={job_id}, holder={holder_id or '-'}，等待 sign_val...")
    wait_resp = client.wait_rapid_sign_job(job_id, timeout=45)
    status = str(wait_resp.get('status') or (wait_resp.get('job') or {}).get('status') or '')
    sign_val = str(wait_resp.get('sign_val') or (wait_resp.get('job') or {}).get('sign_val') or '').strip().upper()
    if status != 'done' or not _norm_sha1(sign_val):
        logger.warning(f"  ➜ [Rapid蜂群签名] sign_job 未完成：job_id={job_id}, status={status}, resp={str(wait_resp)[:500]}")
        return {'ok': False, 'response': first_resp, 'sign_job': wait_resp, 'message': f'sign_job 未完成: {status}'}

    signed_meta = dict(rapid_meta or {})
    signed_meta['sign_key'] = sign_req.get('sign_key')
    signed_meta['sign_val'] = sign_val
    logger.info(
        f"  ➜ [Rapid蜂群签名] 已收到 sign_val，准备带签名重试秒传："
        f"job_id={job_id}, sign_val={sign_val[:12]}..., file={file_name}"
    )
    signed_resp = _call_rapid_method(
        p115,
        target_cid=target_cid,
        sha1=sha1,
        size=size,
        file_name=file_name,
        pick_code=str(file_info.get('pick_code') or ''),
        rapid_meta=signed_meta,
    )
    ok = _rapid_success(signed_resp)
    logger.info(
        f"  ➜ [Rapid蜂群签名] 带中心 sign_val 重试完成：ok={ok}, "
        f"source={source_kind}:{source_id}, sha1={sha1[:12]}..."
    )
    return {'ok': ok, 'response': signed_resp, 'sign_job': wait_resp, 'sha1': sha1, 'file_name': file_name, 'target_cid': target_cid}

def rapid_save_file(file_info: Dict[str, Any], *, target_cid: str = '') -> Dict[str, Any]:
    p115 = P115Service.get_client()
    if not p115:
        raise RuntimeError('115 客户端未初始化')
    target_cid = str(target_cid or _target_cid()).strip()
    file_info = _normalize_rapid_file_info(file_info or {})
    sha1 = _norm_sha1(file_info.get('sha1'))
    size = _rapid_size_to_int(file_info.get('size') or file_info.get('file_size'), 0)
    file_name = str(file_info.get('file_name') or file_info.get('name') or sha1).strip() or sha1
    if not sha1:
        raise RuntimeError('缺少合法 SHA1，无法秒传')
    if size <= 0:
        raise RuntimeError(
            f'中心源缺少文件大小，无法秒传：{file_name}；'
            f'请源端重新登记该资源，或先修复 p115_filesystem_cache.size 后再登记。'
        )
    rapid_meta = file_info.get('rapid_meta_json') if isinstance(file_info.get('rapid_meta_json'), dict) else {}
    rapid_meta = dict(rapid_meta or {})
    preid = _norm_sha1(file_info.get('preid') or rapid_meta.get('preid') or rapid_meta.get('pre_sha1') or rapid_meta.get('pre_sha1_128k'))
    if preid:
        rapid_meta.setdefault('preid', preid)
    logger.info(f"  ➜ [共享资源] 准备执行 115 秒传：{file_name}, sha1={sha1[:8]}..., preid={(preid[:8] + '...') if preid else '-'}, size={size}, target_cid={target_cid}")
    resp = _call_rapid_method(
        p115,
        target_cid=target_cid,
        sha1=sha1,
        size=size,
        file_name=file_name,
        pick_code=str(file_info.get('pick_code') or ''),
        rapid_meta=rapid_meta,
    )
    if _rapid_success(resp):
        return {'ok': True, 'response': resp, 'sha1': sha1, 'file_name': file_name, 'target_cid': target_cid}

    sign_req = _rapid_sign_request_from_response(resp)
    if sign_req and shared_center_enabled():
        try:
            client = SharedCenterClient()
            retry = _retry_rapid_with_center_sign(
                client=client, p115=p115, file_info=file_info, target_cid=target_cid,
                sha1=sha1, size=size, file_name=file_name, rapid_meta=rapid_meta, first_resp=resp,
            )
            if retry.get('ok'):
                return retry
            resp = retry.get('response') or resp
        except Exception as e:
            logger.warning(f"  ➜ [Rapid蜂群签名] 中心 holder 签名闭环失败：{e}")

    return {'ok': False, 'response': resp, 'sha1': sha1, 'file_name': file_name, 'target_cid': target_cid}


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
            f.setdefault('source_kind', 'completed_season')
            f.setdefault('source_id', source_id)
            f.setdefault('source_ref_id', source_id)
        return source_kind, source_id, files

    # 公共连载季包：中心 display-list 返回 season_hub，真正可秒传文件在 pack_items/children 中。
    # 每个子项仍然是 episode 源；转存和贡献流水按 episode 上报，不把 season_hub 当作某个设备的源。
    if source_kind == 'season_hub':
        raw_files = []
        for key in ('pack_items', 'children', 'files', 'items'):
            value = payload.get(key)
            if isinstance(value, list) and value:
                raw_files = value
                break
        files = []
        for item in raw_files or []:
            if not isinstance(item, dict):
                continue
            f = dict(item)
            f.setdefault('tmdb_id', payload.get('tmdb_id'))
            f.setdefault('item_type', 'Episode')
            f.setdefault('season_number', payload.get('season_number'))
            f.setdefault('title', payload.get('title'))
            f.setdefault('release_year', payload.get('release_year'))
            f['source_kind'] = 'episode'
            f['source_id'] = f.get('source_id') or f.get('source_ref_id') or f.get('episode_source_id') or ''
            f['source_ref_id'] = f.get('source_ref_id') or f.get('source_id') or ''
            files.append(f)
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
    success_sources = []
    for f in files:
        try:
            f.setdefault('source_kind', source_kind)
            f.setdefault('source_id', source_id)
            f.setdefault('source_ref_id', source_id)
            file_source_kind = str(f.get('source_kind') or source_kind or '').strip()
            file_source_id = str(f.get('source_id') or f.get('source_ref_id') or source_id or '').strip()
            result = rapid_save_file(f, target_cid=target_cid)
            if result.get('ok'):
                ok_count += 1
                success_sources.append((file_source_kind, file_source_id, f))
                _register_local_rapid_holder(client, source_kind=file_source_kind, source_id=file_source_id, file_info=f)
            else:
                errors.append({'file': f.get('file_name') or f.get('sha1'), 'response': result.get('response')})
        except Exception as e:
            errors.append({'file': f.get('file_name') or f.get('sha1'), 'error': str(e)})

    if ok_count:
        reported = set()
        for report_kind, report_id, report_file in success_sources:
            if report_kind not in ('movie', 'episode', 'completed_season') or not report_id:
                continue
            key = (report_kind, report_id)
            if key in reported:
                continue
            reported.add(key)
            try:
                client.report_transfer(report_kind, report_id, 'success', message=f'本机秒传成功：{report_file.get("file_name") or report_file.get("sha1") or report_id}')
            except Exception as e:
                logger.warning(f"  ➜ [共享资源] 上报秒传成功失败: {e}")
        _kick_115_organize_detached(reason=f'rapid:{source_kind}:{source_id}')
    else:
        fail_kind = source_kind if source_kind in ('movie', 'episode', 'completed_season') else ''
        if fail_kind and source_id:
            try:
                client.report_transfer(fail_kind, source_id, 'failed', message=json.dumps(errors, ensure_ascii=False)[:1000])
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



def consume_center_source_payload(source: Dict[str, Any], mode: str = 'rapid', context: Dict[str, Any] = None) -> Dict[str, Any]:
    if not shared_center_enabled():
        return {'enabled': False, 'ok': False, 'success': False, 'message': '共享资源未启用'}
    source = dict(source or {})
    if context:
        for k, v in dict(context or {}).items():
            source.setdefault(k, v)
    source_kind = str(source.get('source_kind') or source.get('kind') or '').strip()
    source_id = str(source.get('source_id') or source.get('source_ref_id') or source.get('episode_source_id') or '').strip()
    if not source_kind:
        item_type = str(source.get('item_type') or source.get('display_type') or '').strip().lower()
        if item_type == 'movie': source_kind = 'movie'
        elif item_type == 'episode': source_kind = 'episode'
        elif item_type in ('season', 'completed_season'): source_kind = 'completed_season'
    if not source_id and source_kind == 'episode':
        source_id = str(source.get('episode_source_id') or '').strip()
    if not source_kind or not source_id:
        return {'enabled': True, 'ok': False, 'success': False, 'message': '中心源缺少 source_kind/source_id，无法秒传'}
    event = {'event_id': '', 'source_kind': source_kind, 'source_ref_id': source_id, 'payload_json': source}
    result = consume_device_event(event, ack=False)
    result['success'] = bool(result.get('ok'))
    result['count'] = int(result.get('success_count') or 0)
    result['action_type'] = '共享资源秒传'
    return result


def consume_center_sources(source_ids: List[str] = None, mode: str = 'rapid', context: Dict[str, Any] = None, source: Dict[str, Any] = None) -> Dict[str, Any]:
    if isinstance(source, dict) and source:
        return consume_center_source_payload(source, mode=mode, context=context)
    ids = [str(x or '').strip() for x in (source_ids or []) if str(x or '').strip()]
    if not ids:
        return {'enabled': True, 'success': False, 'ok': False, 'message': '缺少 Rapid v2 source payload'}
    client = SharedCenterClient()
    try:
        resp = client.list_sources(source_ids=ids, limit=max(len(ids), 1))
        sources = [x for x in (resp.get('items') or []) if isinstance(x, dict)]
    except Exception as e:
        return {'enabled': True, 'success': False, 'ok': False, 'message': f'按 source_id 查询中心源失败: {e}'}
    return _consume_sources(sources, report_gap=False)
