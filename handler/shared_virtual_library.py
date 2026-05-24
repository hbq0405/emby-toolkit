# handler/shared_virtual_library.py
# 共享资源虚拟入库播放服务：反代层按需触发临时转存，并返回真实 115 pickcode。
import json
import logging
import os
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

import requests

import config_manager
import constants
from database import shared_virtual_db
from database.connection import get_db_connection
from handler.p115_service import P115Service

logger = logging.getLogger(__name__)

_LOCKS: Dict[str, threading.Lock] = {}
_LOCKS_GUARD = threading.Lock()

VIDEO_EXTS = {'.mkv', '.mp4', '.ts', '.m2ts', '.avi', '.mov', '.wmv', '.flv', '.rmvb', '.webm', '.iso'}


def _cfg(name: str, default=None):
    key = getattr(constants, name, None)
    if key:
        return config_manager.APP_CONFIG.get(key, default)
    fallback = {
        'CONFIG_OPTION_115_SHARED_RESOURCE_ENABLED': 'p115_shared_resource_enabled',
        'CONFIG_OPTION_115_SHARED_RESOURCE_MODE': 'p115_shared_resource_mode',
        'CONFIG_OPTION_115_SHARED_CACHE_CID': 'p115_shared_cache_cid',
        'CONFIG_OPTION_115_SHARED_CACHE_NAME': 'p115_shared_cache_name',
        'CONFIG_OPTION_115_SHARED_CACHE_RETENTION_DAYS': 'p115_shared_cache_retention_days',
        'CONFIG_OPTION_115_SHARED_CENTER_URL': 'p115_shared_center_url',
        'CONFIG_OPTION_115_SHARED_DEVICE_TOKEN': 'p115_shared_device_token',
    }.get(name)
    return config_manager.APP_CONFIG.get(fallback, default) if fallback else default


def _is_enabled() -> bool:
    value = _cfg('CONFIG_OPTION_115_SHARED_RESOURCE_ENABLED', False)
    if isinstance(value, str):
        value = value.strip().lower() in ('1', 'true', 'yes', 'on', '启用')
    return bool(value)


def _virtual_mode_enabled() -> bool:
    return str(_cfg('CONFIG_OPTION_115_SHARED_RESOURCE_MODE', 'permanent') or '').lower() == 'virtual'


def _safe_int(value, default=0) -> int:
    try:
        if value is None or value == '':
            return default
        return int(float(value))
    except Exception:
        return default


def _norm_sha1(value: str) -> str:
    return str(value or '').strip().upper()


def _pick(d: Dict[str, Any], *keys, default=''):
    if not isinstance(d, dict):
        return default
    for key in keys:
        value = d.get(key)
        if value not in (None, ''):
            return value
    return default


def _iter_dicts(obj: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            yield from _iter_dicts(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from _iter_dicts(item)


def _extract_list_from_fs_response(resp: Any) -> List[Dict[str, Any]]:
    if not isinstance(resp, dict):
        return []
    candidates = []
    data = resp.get('data')
    if isinstance(data, list):
        candidates = data
    elif isinstance(data, dict):
        for key in ('list', 'items', 'data', 'files'):
            if isinstance(data.get(key), list):
                candidates = data.get(key)
                break
    if not candidates:
        for key in ('list', 'items', 'files'):
            if isinstance(resp.get(key), list):
                candidates = resp.get(key)
                break
    return [x for x in candidates if isinstance(x, dict)]


def _is_folder_node(node: Dict[str, Any]) -> bool:
    fc = node.get('fc')
    if fc is None:
        fc = node.get('file_category') or node.get('type')
    if str(fc) == '0':
        return True
    if any(str(node.get(k)).lower() in ('1', 'true', 'yes', 'folder', 'dir') for k in ('is_dir', 'is_directory', 'is_folder')):
        return True
    return bool(_pick(node, 'cid')) and not any(_pick(node, k) for k in ('pc', 'pick_code', 'pickcode', 'sha1', 'sha', 'size', 'fs'))


def _normalize_node(node: Dict[str, Any], parent_id: str = '') -> Dict[str, Any]:
    node = dict(node or {})
    is_dir = _is_folder_node(node)
    raw_fid = _pick(node, 'fid', 'file_id', 'id')
    raw_cid = _pick(node, 'cid')
    fid = raw_fid or (raw_cid if is_dir else '')
    pid = _pick(node, 'pid', 'parent_id', 'parentId') or ('' if is_dir else raw_cid) or parent_id
    name = _pick(node, 'fn', 'n', 'file_name', 'name', 'title')
    pc = _pick(node, 'pc', 'pick_code', 'pickcode')
    sha1 = _norm_sha1(_pick(node, 'sha1', 'sha', 'file_sha1'))
    size = _safe_int(_pick(node, 'fs', 'size', 'file_size', 's'), 0)
    return {
        **node,
        'fid': str(fid or ''),
        'parent_id': str(pid or ''),
        'name': str(name or ''),
        'pick_code': str(pc or ''),
        'sha1': sha1,
        'size': size,
        'is_dir': is_dir,
    }


def _resp_ok(resp: Any) -> bool:
    if not isinstance(resp, dict):
        return False
    if resp.get('state') is True or resp.get('success') is True:
        return True
    code = resp.get('code') or resp.get('errno') or resp.get('errNo')
    if code in (0, '0', 200, '200'):
        return True
    text = json.dumps(resp, ensure_ascii=False).lower()
    # 115 有时重复转存/秒传会返回“已存在”，对播放而言可以继续定位文件。
    return any(k in text for k in ('已存在', 'already', 'exist'))


def _list_children(client, cid: str, limit=1000) -> List[Dict[str, Any]]:
    try:
        resp = client.fs_files({'cid': str(cid), 'limit': limit, 'offset': 0, 'show_dir': 1})
        return [_normalize_node(x, parent_id=str(cid)) for x in _extract_list_from_fs_response(resp)]
    except Exception as e:
        logger.warning(f"  ➜ [共享虚拟播放] 列出 115 目录失败: cid={cid}, err={e}")
        return []


def _find_file_recursive(client, root_cid: str, sha1: str = '', file_name: str = '', size: int = 0, max_depth: int = 5) -> Optional[Dict[str, Any]]:
    target_sha1 = _norm_sha1(sha1)
    target_name = os.path.basename(str(file_name or '')).lower()
    target_stem = os.path.splitext(target_name)[0]
    queue = [(str(root_cid), 0)]
    seen = set()

    while queue:
        cid, depth = queue.pop(0)
        if not cid or cid in seen or depth > max_depth:
            continue
        seen.add(cid)
        for node in _list_children(client, cid):
            if node.get('is_dir'):
                if node.get('fid'):
                    queue.append((node['fid'], depth + 1))
                continue

            node_name = str(node.get('name') or '').lower()
            node_sha1 = _norm_sha1(node.get('sha1'))
            node_size = _safe_int(node.get('size'), 0)

            if target_sha1 and node_sha1 and node_sha1 == target_sha1:
                return node
            if target_name and node_name == target_name:
                return node
            if target_stem and target_stem in os.path.splitext(node_name)[0]:
                if not size or not node_size or abs(node_size - size) < 1024 * 1024:
                    return node
    return None


def _find_file_by_fs_search(client, cache_cid: str, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    keyword = os.path.basename(str(item.get('file_name') or item.get('title') or '')).strip()
    if not keyword:
        return None
    keyword = re.sub(r'\.strm$', '', keyword, flags=re.IGNORECASE)
    try:
        resp = client.fs_search({'cid': str(cache_cid), 'search_value': keyword, 'limit': 100, 'offset': 0, 'show_dir': 1})
        candidates = [_normalize_node(x) for x in _extract_list_from_fs_response(resp)]
    except Exception as e:
        logger.debug(f"  ➜ [共享虚拟播放] fs_search 失败: {e}")
        return None

    sha1 = _norm_sha1(item.get('sha1'))
    size = _safe_int(item.get('size'), 0)
    for node in candidates:
        if node.get('is_dir'):
            continue
        if sha1 and node.get('sha1') == sha1:
            return node
        if size and node.get('size') and abs(node.get('size') - size) < 1024 * 1024:
            return node
    return candidates[0] if candidates else None


def _upsert_p115_cache(node: Dict[str, Any], item: Dict[str, Any], cache_cid: str):
    fid = str(node.get('fid') or '')
    if not fid:
        return
    parent_id = str(node.get('parent_id') or cache_cid or '')
    name = node.get('name') or item.get('file_name') or fid
    sha1 = _norm_sha1(node.get('sha1') or item.get('sha1'))
    pick_code = node.get('pick_code') or ''
    size = _safe_int(node.get('size') or item.get('size'), 0)
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT local_path FROM p115_filesystem_cache WHERE id=%s", (parent_id,))
                prow = cur.fetchone()
                parent_path = (dict(prow).get('local_path') if prow else '') or ''
                local_path = f"{parent_path.rstrip('/')}/{name}" if parent_path else name
                cur.execute(
                    """
                    INSERT INTO p115_filesystem_cache(id, parent_id, name, local_path, sha1, pick_code, size, updated_at)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT(id) DO UPDATE SET
                        parent_id=EXCLUDED.parent_id,
                        name=EXCLUDED.name,
                        local_path=COALESCE(NULLIF(EXCLUDED.local_path,''), p115_filesystem_cache.local_path),
                        sha1=COALESCE(NULLIF(EXCLUDED.sha1,''), p115_filesystem_cache.sha1),
                        pick_code=COALESCE(NULLIF(EXCLUDED.pick_code,''), p115_filesystem_cache.pick_code),
                        size=CASE WHEN EXCLUDED.size > 0 THEN EXCLUDED.size ELSE p115_filesystem_cache.size END,
                        updated_at=NOW()
                    """,
                    (fid, parent_id, name, local_path, sha1, pick_code, size),
                )
                conn.commit()
    except Exception as e:
        logger.debug(f"  ➜ [共享虚拟播放] 回写 p115_filesystem_cache 失败: {e}")


def _report_transfer_to_center(item: Dict[str, Any], node: Dict[str, Any], result='success', message=''):
    source_id = item.get('source_id') or ''
    if not source_id:
        return
    center_url = str(_cfg('CONFIG_OPTION_115_SHARED_CENTER_URL', 'https://shared.55565576.xyz') or '').rstrip('/')
    token = str(_cfg('CONFIG_OPTION_115_SHARED_DEVICE_TOKEN', '') or '').strip()
    if not center_url or not token:
        return
    payload = {
        'source_id': source_id,
        'result': result,
        'expected_sha1': _norm_sha1(item.get('sha1')),
        'actual_sha1': _norm_sha1(node.get('sha1') or item.get('sha1')),
        'expected_size': _safe_int(item.get('size'), 0) or None,
        'actual_size': _safe_int(node.get('size') or item.get('size'), 0) or None,
        'message': message,
    }
    try:
        requests.post(
            f"{center_url}/api/v1/transfers/report",
            headers={'X-Device-Token': token, 'Content-Type': 'application/json'},
            json=payload,
            timeout=12,
        )
    except Exception as e:
        logger.debug(f"  ➜ [共享虚拟播放] 上报中心转存结果失败: {e}")


def _lock_for(virtual_id: str) -> threading.Lock:
    with _LOCKS_GUARD:
        lock = _LOCKS.get(virtual_id)
        if not lock:
            lock = threading.Lock()
            _LOCKS[virtual_id] = lock
        return lock


def ensure_playable_by_emby_item(
    emby_item_id: str = '',
    user_id: str = '',
    strm_path: str = '',
    media_source: Dict[str, Any] = None,
    display_name: str = '',
) -> Dict[str, Any]:
    """反代播放入口。

    返回：
    - 未命中虚拟入库：None
    - 命中且成功：{'success': True, 'pick_code': '...', ...}
    - 命中但失败：{'matched': True, 'success': False, 'message': '...'}
    """
    if not _is_enabled() or not _virtual_mode_enabled():
        return None

    media_source = media_source or {}
    media_source_id = str(media_source.get('Id') or media_source.get('MediaSourceId') or '')
    item = shared_virtual_db.get_virtual_item_for_playback(
        emby_item_id=str(emby_item_id or ''),
        strm_path=str(strm_path or ''),
        media_source_id=media_source_id,
    )
    if not item:
        return None

    virtual_id = item.get('virtual_id')
    lock = _lock_for(virtual_id)
    with lock:
        # 可能其他并发请求已经完成转存，重新读一次。
        item = shared_virtual_db.get_virtual_item(virtual_id) or item
        if item.get('real_pick_code'):
            shared_virtual_db.mark_virtual_played(virtual_id)
            return {
                'matched': True,
                'success': True,
                'virtual_id': virtual_id,
                'pick_code': item.get('real_pick_code'),
                'real_pick_code': item.get('real_pick_code'),
                'real_fid': item.get('real_fid'),
                'file_name': item.get('file_name') or display_name,
                'title': item.get('title') or display_name,
                'cached': True,
            }

        share_code = str(item.get('share_code') or '').strip()
        receive_code = str(item.get('receive_code') or '').strip()
        if not share_code:
            msg = '虚拟入库记录缺少 share_code，无法临时转存'
            shared_virtual_db.mark_virtual_error(virtual_id, msg)
            return {'matched': True, 'success': False, 'virtual_id': virtual_id, 'message': msg}

        cache_cid = str(item.get('cache_parent_id') or _cfg('CONFIG_OPTION_115_SHARED_CACHE_CID', '0') or '0').strip()
        cache_name = str(item.get('cache_parent_name') or _cfg('CONFIG_OPTION_115_SHARED_CACHE_NAME', '共享资源临时区') or '共享资源临时区')
        retention_days = max(1, _safe_int(_cfg('CONFIG_OPTION_115_SHARED_CACHE_RETENTION_DAYS', 7), 7))
        expires_at = datetime.now(timezone.utc) + timedelta(days=retention_days)

        client = P115Service.get_client()
        if not client:
            msg = '115 客户端未初始化，无法临时转存共享资源'
            shared_virtual_db.mark_virtual_error(virtual_id, msg)
            return {'matched': True, 'success': False, 'virtual_id': virtual_id, 'message': msg}

        shared_virtual_db.mark_virtual_transferring(virtual_id, '播放触发临时转存')
        logger.info(f"  ➜ [共享虚拟播放] 开始临时转存: {item.get('title') or item.get('file_name')} -> cid={cache_cid}")

        import_resp = None
        try:
            import_resp = client.share_import(share_code, receive_code, cache_cid)
        except Exception as e:
            msg = f'调用 115 share_import 失败: {e}'
            shared_virtual_db.mark_virtual_error(virtual_id, msg)
            _report_transfer_to_center(item, {}, result='failed', message=msg)
            return {'matched': True, 'success': False, 'virtual_id': virtual_id, 'message': msg}

        if not _resp_ok(import_resp):
            msg = f"115 share_import 返回失败: {json.dumps(import_resp, ensure_ascii=False)[:300]}"
            shared_virtual_db.mark_virtual_error(virtual_id, msg)
            _report_transfer_to_center(item, {}, result='failed', message=msg)
            return {'matched': True, 'success': False, 'virtual_id': virtual_id, 'message': msg, 'raw': import_resp}

        # 115 转存后不一定直接返回目标文件 PC，统一按 SHA1 / 文件名在临时目录里定位。
        node = _find_file_recursive(
            client,
            cache_cid,
            sha1=item.get('sha1') or '',
            file_name=item.get('file_name') or display_name,
            size=_safe_int(item.get('size'), 0),
            max_depth=6,
        ) or _find_file_by_fs_search(client, cache_cid, item)

        if not node or not node.get('pick_code'):
            msg = '转存成功但未能在临时目录定位到目标视频或 pickcode'
            shared_virtual_db.mark_virtual_error(virtual_id, msg)
            _report_transfer_to_center(item, node or {}, result='failed', message=msg)
            return {'matched': True, 'success': False, 'virtual_id': virtual_id, 'message': msg, 'raw': import_resp}

        _upsert_p115_cache(node, item, cache_cid)
        row = shared_virtual_db.mark_virtual_cached(
            virtual_id,
            real_fid=node.get('fid') or '',
            real_pick_code=node.get('pick_code') or '',
            real_parent_id=node.get('parent_id') or cache_cid,
            cache_parent_id=cache_cid,
            cache_parent_name=cache_name,
            expires_at=expires_at,
            message='播放触发临时转存成功',
            raw_json={'last_import_resp': import_resp, 'last_import_node': node, 'last_play_user_id': user_id},
        ) or item
        shared_virtual_db.mark_virtual_played(virtual_id)
        shared_virtual_db.add_credit_ledger(
            event_type='virtual_play_imported',
            delta=0,
            reason=f"播放触发临时转存：{row.get('title') or row.get('file_name')}",
            ref_id=str(row.get('source_id') or virtual_id),
            source_id=str(row.get('source_id') or ''),
            virtual_id=virtual_id,
            tmdb_id=str(row.get('tmdb_id') or ''),
            item_type=str(row.get('item_type') or ''),
            title=str(row.get('title') or row.get('file_name') or ''),
            raw_json={'fid': node.get('fid'), 'pick_code': node.get('pick_code'), 'cache_cid': cache_cid},
        )
        _report_transfer_to_center(row, node, result='success', message='播放触发临时转存成功')

        return {
            'matched': True,
            'success': True,
            'virtual_id': virtual_id,
            'pick_code': node.get('pick_code'),
            'real_pick_code': node.get('pick_code'),
            'real_fid': node.get('fid'),
            'file_name': node.get('name') or item.get('file_name') or display_name,
            'title': item.get('title') or display_name,
            'cached': False,
        }
