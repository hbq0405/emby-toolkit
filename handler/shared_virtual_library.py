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

def _center_request_kwargs(timeout: int) -> Dict[str, Any]:
    """共享中心 HTTP 请求参数。

    复用全局 Network 代理配置，只影响向共享中心上报的请求。
    """
    kwargs = {'timeout': timeout}
    getter = getattr(config_manager, 'get_proxies_for_requests', None)
    if callable(getter):
        proxies = getter()
        if proxies:
            kwargs['proxies'] = proxies
    return kwargs

_LOCKS: Dict[str, threading.Lock] = {}
_LOCKS_GUARD = threading.Lock()
_LAST_PLAY_MARK: Dict[str, float] = {}
_LAST_PLAY_GUARD = threading.Lock()

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


def _is_video_name(name: str) -> bool:
    return os.path.splitext(str(name or '').lower())[1] in VIDEO_EXTS


def _is_folder_node(node: Dict[str, Any]) -> bool:
    fc = node.get('fc')
    if fc is None:
        fc = node.get('file_category') or node.get('type')
    if str(fc) == '0':
        return True
    if any(str(node.get(k)).lower() in ('1', 'true', 'yes', 'folder', 'dir') for k in ('is_dir', 'is_directory', 'is_folder')):
        return True

    name = _pick(node, 'fn', 'n', 'file_name', 'name', 'title')
    sha1 = _pick(node, 'sha1', 'sha', 'file_sha1')
    size = _safe_int(_pick(node, 'fs', 'size', 'file_size', 's'), 0)

    # 115 对目录有时也会返回 pick_code，但目录的 sha1 为空、size 为 0、且没有视频扩展名。
    # 这类节点绝不能当成可播放文件，否则会把整季目录的 pickcode 传给播放直链接口。
    if (not sha1) and size <= 0 and name and not _is_video_name(name):
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
    if code in (0, '0', 200, '200', 4100024, '4100024'):
        return True
    text = json.dumps(resp, ensure_ascii=False).lower()
    # 115 有时重复转存/秒传会返回“已存在/已经转存过”，对播放而言可以继续定位文件。
    return any(k in text for k in ('已存在', '已经转存', '转存过', 'already', 'exist'))


def _is_already_transferred_resp(resp: Any) -> bool:
    """判断 115 share_import 是否返回“本账号已经转存过”。

    这类返回不能向中心上报 failed，因为分享本身通常是可用的；
    但如果本地临时区已经找不到目标文件，就必须把 115 原始语义返回给用户，
    避免显示“转存成功但未定位到目标视频”这种容易误导的提示。
    """
    if not isinstance(resp, dict):
        return False
    code = resp.get('errno') or resp.get('code') or resp.get('errNo')
    if code in (4100024, '4100024'):
        return True
    try:
        text = json.dumps(resp, ensure_ascii=False).lower()
    except Exception:
        text = str(resp).lower()
    return any(k in text for k in ('你已经转存过', '已经转存过', '已经转存', '转存过该文件'))


def _share_import_error_message(resp: Any) -> str:
    if isinstance(resp, dict):
        err = (
            resp.get('error')
            or resp.get('error_msg')
            or resp.get('message')
            or resp.get('msg')
        )
        if err:
            return f"115 share_import 返回失败: {err}"
    try:
        raw = json.dumps(resp, ensure_ascii=False)
    except Exception:
        raw = str(resp)
    return f"115 share_import 返回失败: {raw[:300]}"


def _list_children(client, cid: str, limit=1000) -> List[Dict[str, Any]]:
    try:
        resp = client.fs_files({'cid': str(cid), 'limit': limit, 'offset': 0, 'show_dir': 1})
        return [_normalize_node(x, parent_id=str(cid)) for x in _extract_list_from_fs_response(resp)]
    except Exception as e:
        logger.warning(f"  ➜ [共享虚拟播放] 列出 115 目录失败: cid={cid}, err={e}")
        return []


def _norm_for_match(value: str) -> str:
    text = str(value or '').lower()
    text = os.path.splitext(os.path.basename(text))[0]
    return re.sub(r'[^a-z0-9\u4e00-\u9fa5]+', '', text)


def _node_matches_virtual_item(node: Dict[str, Any], item: Dict[str, Any], file_name: str = '', size: int = 0) -> bool:
    """严格判断临时区节点是否就是当前虚拟项对应的视频文件。

    不能用“搜索结果第一条”或“目录 pickcode”兜底，否则不同分享包会串台。
    """
    if not node or node.get('is_dir'):
        return False
    node_name = str(node.get('name') or '')
    node_pick = str(node.get('pick_code') or '')
    if not node_pick or not _is_video_name(node_name):
        return False

    target_sha1 = _norm_sha1(item.get('sha1'))
    node_sha1 = _norm_sha1(node.get('sha1'))
    if target_sha1:
        return bool(node_sha1 and node_sha1 == target_sha1)

    target_name = os.path.basename(str(file_name or item.get('file_name') or '')).lower()
    node_name_lower = node_name.lower()
    if target_name and node_name_lower == target_name:
        return True

    target_size = _safe_int(size or item.get('size'), 0)
    node_size = _safe_int(node.get('size'), 0)
    target_stem = _norm_for_match(target_name)
    node_stem = _norm_for_match(node_name_lower)
    name_close = bool(target_stem and node_stem and (target_stem in node_stem or node_stem in target_stem))
    size_close = bool(target_size and node_size and abs(node_size - target_size) < 1024 * 1024)
    return name_close and (size_close or not target_size)


def _find_file_recursive(client, root_cid: str, sha1: str = '', file_name: str = '', size: int = 0, max_depth: int = 5, item: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
    item = item or {'sha1': sha1, 'file_name': file_name, 'size': size}
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

            if _node_matches_virtual_item(node, item, file_name=file_name, size=size):
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

    for node in candidates:
        if _node_matches_virtual_item(node, item):
            return node

    # 绝不返回 candidates[0]。115 搜索可能返回目录或其他资源包，直接兜底会导致串台。
    return None


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


def _collect_virtual_pack_items(item: Dict[str, Any]) -> List[Dict[str, Any]]:
    """收集同一个分享包里的全部虚拟项。

    虚拟剧集包的核心语义是“播放任意一集，115 会整包转存”。因此临时转存成功后
    必须把同包所有分集的 fid/pickcode 一次性回填，否则下一集还会重新走 share_import，
    既慢又容易被 115 返回“你已经转存过该文件”打断播放。
    """
    item = item or {}
    share_code = str(item.get('share_code') or '').strip()
    if not share_code:
        return [item]

    season = item.get('season_number')
    contributor_id = str(item.get('contributor_id') or '').strip()
    args = [share_code]
    where = [
        "share_code=%s",
        "status NOT IN ('deleted','promoted','promote_pending')",
    ]
    if season not in [None, '']:
        where.append("COALESCE(season_number, -1) = %s")
        try:
            args.append(int(season))
        except Exception:
            args.append(season)
    if contributor_id:
        where.append("COALESCE(contributor_id, '') = %s")
        args.append(contributor_id)

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT *
                    FROM shared_virtual_items
                    WHERE {' AND '.join(where)}
                    ORDER BY COALESCE(episode_number, 999999), file_name, updated_at DESC
                    """,
                    args,
                )
                rows = [dict(r) for r in cur.fetchall()]
        if rows:
            by_vid = {}
            for row in rows:
                vid = str(row.get('virtual_id') or '')
                if vid and vid not in by_vid:
                    by_vid[vid] = row
            cur_vid = str(item.get('virtual_id') or '')
            if cur_vid and cur_vid not in by_vid:
                by_vid[cur_vid] = item
            return list(by_vid.values())
    except Exception as e:
        logger.debug(f"  ➜ [共享虚拟播放] 收集虚拟包条目失败: {e}")
    return [item]


def _collect_pack_items_for_transfer_report(item: Dict[str, Any]) -> List[Dict[str, Any]]:
    """同一个 115 share_code 下的同季虚拟项视作一次包转存，用于中心记账上报。"""
    rows = _collect_virtual_pack_items(item)
    report_rows = []
    seen = set()
    for row in rows:
        sid = str(row.get('source_id') or '')
        if sid and sid not in seen:
            seen.add(sid)
            report_rows.append(row)
    return report_rows or [item]


def _extract_receive_titles(import_resp: Any) -> List[str]:
    titles = []
    if isinstance(import_resp, dict):
        for d in _iter_dicts(import_resp):
            for key in ('receive_title', 'receive_name', 'file_name', 'name', 'title'):
                val = d.get(key)
                if isinstance(val, str) and val.strip():
                    titles.append(val.strip())
    seen = set()
    out = []
    for title in titles:
        key = title.lower()
        if key not in seen:
            seen.add(key)
            out.append(title)
    return out


def _path_node_cid(node: Dict[str, Any]) -> str:
    if not isinstance(node, dict):
        return ''
    return str(node.get('cid') or node.get('file_id') or node.get('fid') or node.get('id') or '').strip()


def _path_node_name(node: Dict[str, Any]) -> str:
    if not isinstance(node, dict):
        return ''
    return str(node.get('name') or node.get('file_name') or node.get('fn') or node.get('n') or '').strip()


def _find_import_root_node(client, cache_cid: str, current_node: Dict[str, Any] = None, import_resp: Any = None) -> Dict[str, str]:
    """定位本次 share_import 在临时区生成的顶层目录。

    115 分享包常见结构是：
    共享临时区/cache_cid -> 分享根目录 -> Season 01 -> Exx.mkv。
    real_parent_id 只会记录到 Season 01，删除时只删它会留下空的分享根目录。
    因此这里尽量记录“分享根目录”的 CID，供后续整包删除。
    """
    cache_cid = str(cache_cid or '').strip()
    if not cache_cid:
        return {}

    # 1. share_import 返回的 receive_title 通常就是顶层分享根目录名。
    title_keys = {str(t).strip().lower() for t in _extract_receive_titles(import_resp) if str(t).strip()}
    if title_keys:
        try:
            for child in _list_children(client, cache_cid):
                name = str(child.get('name') or '').strip()
                if child.get('is_dir') and child.get('fid') and name.lower() in title_keys:
                    return {'fid': str(child.get('fid') or ''), 'name': name, 'source': 'receive_title'}
        except Exception as e:
            logger.debug(f"  ➜ [共享虚拟播放] 按 receive_title 定位导入根目录失败: {e}")

    # 2. 用当前文件父目录的 115 path 反推 cache_cid 下的第一层子目录。
    parent_id = str((current_node or {}).get('parent_id') or '').strip()
    if parent_id and parent_id != cache_cid:
        try:
            res = client.fs_files({'cid': parent_id, 'limit': 1, 'record_open_time': 0, 'count_folders': 0})
            path_nodes = (res or {}).get('path') or []
            for idx, path_node in enumerate(path_nodes):
                if _path_node_cid(path_node) == cache_cid and idx + 1 < len(path_nodes):
                    root_node = path_nodes[idx + 1] or {}
                    root_cid = _path_node_cid(root_node)
                    if root_cid and root_cid != cache_cid:
                        return {'fid': root_cid, 'name': _path_node_name(root_node), 'source': 'path'}
        except Exception as e:
            logger.debug(f"  ➜ [共享虚拟播放] 按 115 path 反推导入根目录失败: parent={parent_id}, err={e}")

    return {}


def _find_import_root_candidates(client, cache_cid: str, item: Dict[str, Any], current_node: Dict[str, Any] = None, import_resp: Any = None) -> List[str]:
    """尽量把整包回填扫描范围限制在本次导入的目录内，避免临时区多包串台。"""
    candidates = []

    import_root = _find_import_root_node(client, cache_cid, current_node=current_node, import_resp=import_resp)
    if import_root.get('fid'):
        candidates.append(str(import_root.get('fid')))

    titles = _extract_receive_titles(import_resp)
    title_keys = {str(t).strip().lower() for t in titles if str(t).strip()}
    if title_keys:
        for child in _list_children(client, cache_cid):
            name = str(child.get('name') or '').strip().lower()
            if name in title_keys and child.get('is_dir') and child.get('fid'):
                candidates.append(str(child.get('fid')))

    # 如果当前文件已经定位出来，再扫它所在的目录。这通常是 Season 01 子目录，
    # 仅作为导入根目录识别失败时的兜底，不能作为删除根目录的唯一依据。
    if current_node and current_node.get('parent_id'):
        candidates.append(str(current_node.get('parent_id')))

    # 最后才扫整个临时区。实际匹配仍然要求 sha1/文件名/大小严格命中。
    candidates.append(str(cache_cid))

    seen = set()
    out = []
    for cid in candidates:
        if cid and cid not in seen:
            seen.add(cid)
            out.append(cid)
    return out


def _collect_matching_nodes_for_items(client, root_cid: str, pack_items: List[Dict[str, Any]], max_depth: int = 6) -> Dict[str, Dict[str, Any]]:
    """在 root_cid 下遍历一次，按 sha1/文件名/大小严格匹配包内所有虚拟项。"""
    result: Dict[str, Dict[str, Any]] = {}
    items = [x for x in (pack_items or []) if x and x.get('virtual_id')]
    if not items:
        return result

    queue = [(str(root_cid), 0)]
    seen = set()
    while queue and len(result) < len(items):
        cid, depth = queue.pop(0)
        if not cid or cid in seen or depth > max_depth:
            continue
        seen.add(cid)
        for node in _list_children(client, cid):
            if node.get('is_dir'):
                if node.get('fid'):
                    queue.append((node['fid'], depth + 1))
                continue
            for pack_item in items:
                vid = str(pack_item.get('virtual_id') or '')
                if not vid or vid in result:
                    continue
                if _node_matches_virtual_item(node, pack_item, file_name=pack_item.get('file_name'), size=_safe_int(pack_item.get('size'), 0)):
                    result[vid] = node
                    break
    return result


def _backfill_virtual_pack_cache(
    client,
    cache_cid: str,
    cache_name: str,
    expires_at,
    item: Dict[str, Any],
    current_node: Dict[str, Any],
    import_resp: Any = None,
    user_id: str = '',
    message: str = '整包临时转存成功，批量回填 pickcode',
) -> Dict[str, Dict[str, Any]]:
    """整包转存后批量回填同包所有分集的 real_fid/real_pick_code。"""
    pack_items = _collect_virtual_pack_items(item)
    if len(pack_items) <= 1:
        return {str(item.get('virtual_id') or ''): current_node} if current_node else {}

    matched: Dict[str, Dict[str, Any]] = {}
    cur_vid = str(item.get('virtual_id') or '')
    if cur_vid and current_node:
        matched[cur_vid] = current_node

    import_root = _find_import_root_node(client, cache_cid, current_node=current_node, import_resp=import_resp)
    import_root_cid = str(import_root.get('fid') or '').strip()
    import_root_name = str(import_root.get('name') or '').strip()

    for root_cid in _find_import_root_candidates(client, cache_cid, item, current_node=current_node, import_resp=import_resp):
        more = _collect_matching_nodes_for_items(client, root_cid, pack_items)
        if more and not import_root_cid and str(root_cid) != str(cache_cid):
            # 老接口没返回 receive_title 时，至少把实际命中扫描根记录下来，供删除兜底。
            import_root_cid = str(root_cid)
        matched.update({k: v for k, v in more.items() if k not in matched})
        if len(matched) >= len(pack_items):
            break

    updated = 0
    for pack_item in pack_items:
        vid = str(pack_item.get('virtual_id') or '')
        node = matched.get(vid)
        if not vid or not node or not node.get('pick_code'):
            continue
        _upsert_p115_cache(node, pack_item, cache_cid)
        shared_virtual_db.mark_virtual_cached(
            vid,
            real_fid=node.get('fid') or '',
            real_pick_code=node.get('pick_code') or '',
            real_parent_id=node.get('parent_id') or cache_cid,
            cache_parent_id=cache_cid,
            cache_parent_name=cache_name,
            expires_at=expires_at,
            message=message,
            raw_json={
                'last_import_resp': import_resp or {},
                'last_import_node': node,
                'last_import_root_cid': import_root_cid,
                'last_import_root_name': import_root_name,
                'last_play_user_id': user_id,
                'pack_backfilled_by': cur_vid,
            },
        )
        updated += 1

    logger.info(
        "  ➜ [共享虚拟播放] 整包临时转存完成，已批量回填 %s/%s 集 pickcode。",
        updated, len(pack_items)
    )
    return matched


def _report_transfer_to_center(item: Dict[str, Any], node: Dict[str, Any], result='success', message='', whole_pack: bool = False):
    center_url = str(_cfg('CONFIG_OPTION_115_SHARED_CENTER_URL', 'https://shared.55565576.xyz') or '').rstrip('/')
    token = str(_cfg('CONFIG_OPTION_115_SHARED_DEVICE_TOKEN', '') or '').strip()
    if not center_url or not token:
        return

    report_items = _collect_pack_items_for_transfer_report(item) if whole_pack and result == 'success' else [item]
    reported = 0
    for report_item in report_items:
        source_id = report_item.get('source_id') or ''
        if not source_id:
            continue
        expected_sha1 = _norm_sha1(report_item.get('sha1'))
        expected_size = _safe_int(report_item.get('size'), 0) or None
        payload = {
            'source_id': source_id,
            'result': result,
            'expected_sha1': expected_sha1,
            # 包转存时不一定逐条定位每个文件，这里用登记源自身的 sha1/size 上报，
            # 让中心按 source_id 逐条记账，避免只扣当前播放的一集。
            'actual_sha1': expected_sha1 if whole_pack else _norm_sha1(node.get('sha1') or report_item.get('sha1')),
            'expected_size': expected_size,
            'actual_size': expected_size if whole_pack else (_safe_int(node.get('size') or report_item.get('size'), 0) or None),
            'message': message,
        }
        try:
            resp = requests.post(
                f"{center_url}/api/v1/transfers/report",
                headers={'X-Device-Token': token, 'Content-Type': 'application/json'},
                json=payload,
                **_center_request_kwargs(12),
            )
            if resp.ok:
                reported += 1
        except Exception as e:
            logger.debug(f"  ➜ [共享虚拟播放] 上报中心转存结果失败: {e}")
    if whole_pack and reported > 1:
        logger.info(f"  ➜ [共享虚拟播放] 已按整包转存向中心上报 {reported} 个 source_id。")


def _lock_for(virtual_id: str) -> threading.Lock:
    with _LOCKS_GUARD:
        lock = _LOCKS.get(virtual_id)
        if not lock:
            lock = threading.Lock()
            _LOCKS[virtual_id] = lock
        return lock


def _mark_played_debounced(virtual_id: str, interval_seconds: int = 60):
    """播放器会对同一媒体发起多次 stream/original/range 请求。

    这些请求不应该反复增加 play_count，也不应该让日志看起来像重复临时转存。
    首次命中立即记一次，后续短时间内只复用已缓存 pickcode。
    """
    now = time.time()
    with _LAST_PLAY_GUARD:
        last = _LAST_PLAY_MARK.get(virtual_id, 0)
        if last and now - last < interval_seconds:
            return None
        _LAST_PLAY_MARK[virtual_id] = now
    return shared_virtual_db.mark_virtual_played(virtual_id)


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
            cached_node = None
            raw_json = item.get('raw_json') if isinstance(item.get('raw_json'), dict) else {}
            if isinstance(raw_json, dict) and isinstance(raw_json.get('last_import_node'), dict):
                cached_node = _normalize_node(raw_json.get('last_import_node') or {})

            # 老数据没有 last_import_node 时只能保守沿用；新链路必须校验，避免把其他剧/目录 pickcode 当成本集。
            if not cached_node or _node_matches_virtual_item(cached_node, item, file_name=item.get('file_name') or display_name, size=_safe_int(item.get('size'), 0)):
                _mark_played_debounced(virtual_id)
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

            logger.warning(
                "  ➜ [共享虚拟播放] 已缓存 pickcode 与当前虚拟项不匹配，忽略旧缓存并重新定位: virtual_id=%s, cached=%s, current=%s",
                virtual_id, cached_node.get('name') or cached_node.get('fid'), item.get('file_name') or display_name
            )

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

        def _finalize_cached_node(node: Dict[str, Any], import_resp=None, message='播放触发临时转存成功', cached=False):
            if not _node_matches_virtual_item(node, item, file_name=item.get('file_name') or display_name, size=_safe_int(item.get('size'), 0)):
                msg = f"临时区定位到的节点不属于当前虚拟资源，拒绝复用: {node.get('name') or node.get('fid')}"
                logger.warning(f"  ➜ [共享虚拟播放] {msg}")
                shared_virtual_db.mark_virtual_error(virtual_id, msg)
                return {'matched': True, 'success': False, 'virtual_id': virtual_id, 'message': msg}
            _upsert_p115_cache(node, item, cache_cid)
            import_root = _find_import_root_node(client, cache_cid, current_node=node, import_resp=import_resp or {})
            row = shared_virtual_db.mark_virtual_cached(
                virtual_id,
                real_fid=node.get('fid') or '',
                real_pick_code=node.get('pick_code') or '',
                real_parent_id=node.get('parent_id') or cache_cid,
                cache_parent_id=cache_cid,
                cache_parent_name=cache_name,
                expires_at=expires_at,
                message=message,
                raw_json={
                    'last_import_resp': import_resp or {},
                    'last_import_node': node,
                    'last_import_root_cid': str(import_root.get('fid') or ''),
                    'last_import_root_name': str(import_root.get('name') or ''),
                    'last_play_user_id': user_id,
                },
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
                raw_json={'fid': node.get('fid'), 'pick_code': node.get('pick_code'), 'cache_cid': cache_cid, 'cached': cached},
            )
            # 性能优化：将耗时的整包回填和中心上报放入后台线程，让当前集秒播 
            def _background_pack_tasks():
                try:
                    # 整季分享包是“一次 share_import 转入整包”。一旦当前集定位成功，立刻把同包所有分集
                    # 的 fid/pickcode 回填到 shared_virtual_items，下一集直接读缓存起播。
                    _backfill_virtual_pack_cache(
                        client,
                        cache_cid,
                        cache_name,
                        expires_at,
                        row,
                        node,
                        import_resp=import_resp or {},
                        user_id=user_id,
                        message='临时区已存在，复用整包转存结果' if cached else '整包临时转存成功，批量回填 pickcode',
                    )
                except Exception as e:
                    logger.error(f"  ➜ [共享虚拟播放] 后台回填整包 pickcode 失败: {e}")

                try:
                    # 中心贡献值/扣分按包内所有 source_id 上报；中心有唯一约束，重复上报不会重复扣分。
                    _report_transfer_to_center(row, node, result='success', message=message, whole_pack=True)
                except Exception as e:
                    logger.error(f"  ➜ [共享虚拟播放] 后台上报中心转存结果失败: {e}")

            # 启动幽灵线程执行耗时任务
            threading.Thread(
                target=_background_pack_tasks, 
                name=f"VirtualBackfill-{virtual_id}", 
                daemon=True
            ).start()

            # 立刻返回当前集的 pickcode 给播放器！
            return {
                'matched': True,
                'success': True,
                'virtual_id': virtual_id,
                'pick_code': node.get('pick_code'),
                'real_pick_code': node.get('pick_code'),
                'real_fid': node.get('fid'),
                'file_name': node.get('name') or item.get('file_name') or display_name,
                'title': item.get('title') or display_name,
                'cached': bool(cached),
            }

        # 如果同一季包之前已经被播放任意一集触发过整包转存，下一集不应该再调用 share_import。
        # 直接在临时目录按 SHA1/文件名定位目标文件即可，避免 115 返回 4100024 “你已经转存过该文件”。
        existing_node = _find_file_recursive(
            client,
            cache_cid,
            sha1=item.get('sha1') or '',
            file_name=item.get('file_name') or display_name,
            size=_safe_int(item.get('size'), 0),
            max_depth=6,
            item=item,
        ) or _find_file_by_fs_search(client, cache_cid, item)
        if existing_node and existing_node.get('pick_code'):
            logger.info(f"  ➜ [共享虚拟播放] 临时区已存在目标文件，复用 pickcode: {existing_node.get('name') or item.get('file_name')}")
            return _finalize_cached_node(
                existing_node,
                import_resp={'state': True, '_already_cached_before_import': True},
                message='临时区已存在，复用整包转存结果',
                cached=True,
            )

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
            item=item,
        ) or _find_file_by_fs_search(client, cache_cid, item)

        if not node or not node.get('pick_code'):
            if _is_already_transferred_resp(import_resp):
                msg = _share_import_error_message(import_resp)
                shared_virtual_db.mark_virtual_error(virtual_id, msg)
                # 注意：4100024 只是“本账号已接收过该分享”，不是共享源失效。
                # 本地临时缓存被释放后无法再次定位目标文件时，不要向中心上报 failed。
                return {
                    'matched': True,
                    'success': False,
                    'virtual_id': virtual_id,
                    'message': msg,
                    'raw': import_resp,
                }

            msg = '转存成功但未能在临时目录定位到目标视频或 pickcode'
            shared_virtual_db.mark_virtual_error(virtual_id, msg)
            _report_transfer_to_center(item, node or {}, result='failed', message=msg)
            return {'matched': True, 'success': False, 'virtual_id': virtual_id, 'message': msg, 'raw': import_resp}

        return _finalize_cached_node(node, import_resp=import_resp, message='播放触发临时转存成功', cached=False)
