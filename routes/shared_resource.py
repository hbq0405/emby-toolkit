# routes/shared_resource.py
# 共享资源：我的分享、中心资源转存、贡献值管理 API
import logging
import os
import re
import json
import time
import threading
import uuid
import socket
from datetime import datetime
from typing import Dict, Any, List

import requests
from flask import Blueprint, jsonify, request

import config_manager
import constants
from extensions import admin_required
from database import shared_credit_db, shared_share_db, settings_db
from database.connection import get_db_connection
from handler.p115_service import P115Service, P115CacheManager
from handler import tmdb as tmdb_handler
import tasks.helpers as helpers

shared_resource_bp = Blueprint('shared_resource_bp', __name__, url_prefix='/api/shared/resources')
logger = logging.getLogger(__name__)

_SCAN_KICK_LOCK = threading.Lock()
_LAST_SCAN_KICK_AT = 0

VIDEO_EXTENSIONS = {'.mp4', '.mkv', '.avi', '.ts', '.mov', '.m2ts', '.iso', '.wmv', '.flv'}
CENTER_SOURCE_STATUS_REPLENISH = 'replenish'
CENTER_DISPLAY_SOURCE_STATUSES = 'alive,pending,replenish'

def _center_request_kwargs(timeout: int) -> Dict[str, Any]:
    """共享中心 HTTP 请求参数。

    复用全局 Network 代理配置，只影响共享中心接口请求；
    未开启代理时不传 proxies，保持原来的直连行为。
    """
    kwargs = {'timeout': timeout}
    getter = getattr(config_manager, 'get_proxies_for_requests', None)
    if callable(getter):
        proxies = getter()
        if proxies:
            kwargs['proxies'] = proxies
    return kwargs

def _client_app_version() -> str:
    """当前 ETK 客户端版本号。

    中心服务器版本门禁只认请求头 X-Client-Version；
    这里直接读取 constants.APP_VERSION，避免再增加一套用户配置。
    """
    return str(getattr(constants, 'APP_VERSION', '0.0.0') or '0.0.0').strip() or '0.0.0'

def _center_headers_for_cfg(cfg: Dict[str, Any]) -> Dict[str, str]:
    return {
        'X-Device-Token': str((cfg or {}).get('device_token') or '').strip(),
        'Content-Type': 'application/json',
        'X-Client-Version': _client_app_version(),
    }

def _request_json() -> Dict[str, Any]:
    """安全读取 JSON 请求体。

    Flask 3.x 下直接访问 request.json 时，如果前端 POST 没有带
    application/json，会抛 415 Unsupported Media Type。共享资源按钮
    有些请求本来就不需要 body，所以统一 silent 读取。
    """
    try:
        data = request.get_json(silent=True)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def _get_shared_config() -> Dict[str, Any]:
    cfg = settings_db.get_shared_resource_config()
    return {
        "enabled": bool(cfg.get('p115_shared_resource_enabled', False)),
        "center_url": (cfg.get('p115_shared_center_url') or "https://shared.55565576.xyz").rstrip('/'),
        "device_token": cfg.get('p115_shared_device_token') or "",
        "mode": "permanent",
        "install_id": cfg.get('p115_shared_install_id') or "",
    }

def _shared_resource_config_payload() -> Dict[str, Any]:
    payload = settings_db.get_shared_resource_config()
    if isinstance(payload, dict):
        auto_enabled = _boolish(
            payload.get('p115_shared_auto_share_requests_enabled', payload.get('shared_auto_share_requests_enabled')),
            False,
        )
        payload.setdefault('p115_shared_auto_share_requests_enabled', auto_enabled)
        payload.setdefault('shared_auto_share_requests_enabled', auto_enabled)
        block_clean = _boolish(payload.get('p115_shared_block_clean_version_transfer'), False)
        payload.setdefault('p115_shared_block_clean_version_transfer', block_clean)
    return payload

def _fetch_center_credit() -> Dict[str, Any]:
    cfg = _get_shared_config()
    if not cfg["device_token"]:
        return {"ok": False, "message": "未配置共享中心 device_token"}

    headers = _center_headers_for_cfg(cfg)
    me_resp = requests.get(f"{cfg['center_url']}/api/v1/me", headers=headers, **_center_request_kwargs(12))
    me_resp.raise_for_status()
    me = me_resp.json() or {}

    stats = {}
    try:
        stats_resp = requests.get(f"{cfg['center_url']}/api/v1/stats", headers=headers, **_center_request_kwargs(12))
        if stats_resp.ok:
            stats = stats_resp.json() or {}
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 拉取中心统计失败，仅保存 credit: {e}")

    center_ledger_items = []
    try:
        ledger_resp = requests.get(
            f"{cfg['center_url']}/api/v1/credit/ledger",
            headers=headers,
            params={"limit": 300},
            **_center_request_kwargs(12),
        )
        if ledger_resp.ok:
            center_ledger_items = (ledger_resp.json() or {}).get("items") or []
        else:
            logger.warning(f"  ➜ [共享资源] 拉取中心贡献值流水失败: HTTP {ledger_resp.status_code} {ledger_resp.text[:200]}")
    except Exception as e:
        # 兼容未升级中心服务器：只同步快照，不影响页面打开。
        logger.warning(f"  ➜ [共享资源] 拉取中心贡献值流水失败，仅保存 credit: {e}")

    snapshot = {
        "device_id": me.get("id"),
        "credit": int(me.get("credit") or 0),
        "wanted_gaps": int(stats.get("wanted_gaps") or 0),
        "shared_sources": int(stats.get("shared_sources") or 0),
        "raw_ffprobe": int(stats.get("raw_ffprobe") or 0),
        "remote_devices": int(stats.get("devices") or 0),
        "raw_json": {"me": me, "stats": stats},
    }
    saved = shared_credit_db.upsert_credit_snapshot(snapshot)
    synced_ledger = shared_credit_db.sync_center_credit_ledger(center_ledger_items, device_snapshot=me)
    return {"ok": True, "snapshot": saved, "synced_ledger": synced_ledger}

def _looks_like_video_name(name: str) -> bool:
    return os.path.splitext(str(name or ''))[1].lower() in VIDEO_EXTENSIONS

def _is_folder(node: Dict[str, Any]) -> bool:
    """
    115 不同接口对目录字段返回不一致：
    - 有些目录没有 fc/is_dir，只给 cid/name；
    - 文件通常会有 sha1 / pick_code / size，并且文件名有视频扩展名。
    之前只认 fc=0，导致剧集目录被误判成普通文件，直接返回 0 个分享项。
    """
    node = node or {}
    fc = str(node.get('fc') if node.get('fc') is not None else node.get('file_category') if node.get('file_category') is not None else '').strip()
    if fc == '0':
        return True
    if fc == '1':
        return False
    if bool(node.get('is_dir') or node.get('is_folder') or node.get('is_directory')):
        return True

    name = _node_name(node)
    has_file_identity = bool(
        node.get('sha1') or node.get('sha') or node.get('file_sha1') or
        node.get('pc') or node.get('pick_code') or node.get('pickcode')
    )
    if has_file_identity or _looks_like_video_name(name):
        return False

    # 目录项常见只有 cid/name/pid，没有 sha1/pc/视频扩展名。
    if node.get('cid') or node.get('file_id') or node.get('id') or node.get('fid'):
        return True
    return False

def _node_name(node: Dict[str, Any]) -> str:
    return str(node.get('fn') or node.get('n') or node.get('file_name') or node.get('name') or node.get('title') or '')

def _node_id(node: Dict[str, Any]) -> str:
    return str(node.get('fid') or node.get('file_id') or node.get('id') or node.get('cid') or '')

def _guess_episode_number(name: str):
    text = str(name or '')
    patterns = [r'[Ss]\d{1,2}[Ee](\d{1,3})', r'第\s*(\d{1,3})\s*[集话]', r'\bE(\d{1,3})\b']
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None
    return None

def _safe_size_bytes(value, default=0) -> int:
    """把 115/中心返回的文件大小统一转成字节数。

    115 部分接口会返回展示字符串，例如 "3.78GB"，而数据库写入和中心登记
    都要求纯数字字节数。这里集中兜底，避免 int("3.78GB") 直接炸。
    """
    if value in (None, ''):
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        try:
            return int(value)
        except Exception:
            return default

    text = str(value).strip()
    if not text:
        return default

    # 先处理纯数字/小数字符串；逗号分隔的大数字也兼容。
    normalized = text.replace(',', '').strip()
    try:
        return int(float(normalized))
    except Exception:
        pass

    upper = normalized.upper().replace(' ', '')
    upper = upper.replace('（', '(').replace('）', ')')
    m = re.match(r'^([0-9]+(?:\.[0-9]+)?)([KMGTPE]?I?B?|BYTE|BYTES)$', upper)
    if not m:
        return default

    number = float(m.group(1))
    unit = m.group(2) or 'B'
    unit = unit.replace('BYTES', 'B').replace('BYTE', 'B')
    # 115 的 GB/MB 展示通常按二进制体积理解；KiB/MiB 同样归一处理。
    if unit in ('B', ''):
        multiplier = 1
    elif unit in ('K', 'KB', 'KIB'):
        multiplier = 1024
    elif unit in ('M', 'MB', 'MIB'):
        multiplier = 1024 ** 2
    elif unit in ('G', 'GB', 'GIB'):
        multiplier = 1024 ** 3
    elif unit in ('T', 'TB', 'TIB'):
        multiplier = 1024 ** 4
    elif unit in ('P', 'PB', 'PIB'):
        multiplier = 1024 ** 5
    elif unit in ('E', 'EB', 'EIB'):
        multiplier = 1024 ** 6
    else:
        return default
    return int(number * multiplier)

def _collect_files_from_cache(root_fid: str, root_name: str = '', max_depth: int = 6) -> List[Dict[str, Any]]:
    rows = shared_share_db.get_p115_files_from_cache_tree(root_fid, max_depth)
    files = []
    for row in rows:
        name = str(row.get('name') or '')
        if not _looks_like_video_name(name):
            continue
        rel = row.get('rel_path') or name
        files.append({
            'fid': str(row.get('id') or ''),
            'sha1': (str(row.get('sha1')).upper() if row.get('sha1') else None),
            'pick_code': row.get('pick_code') or row.get('pc') or row.get('pickcode'),
            'size': _safe_size_bytes(row.get('size')),
            'file_name': name,
            'relative_path': rel,
            'episode_number': _guess_episode_number(name),
            'raw_json': {'source': 'p115_filesystem_cache', 'root_fid': root_fid, 'row': row},
        })
    return files

def _collect_files_from_media_payload(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """用 media_metadata 的 PC/SHA1 反查 p115_filesystem_cache，作为剧集/季分享兜底。"""
    tmdb_id = str(data.get('tmdb_id') or '').strip()
    item_type = str(data.get('item_type') or '').strip()
    if not tmdb_id or not item_type:
        return []

    row = _get_media_row(tmdb_id, item_type)
    if not row:
        # 中心登记和前端候选对 Season/Episode 会统一用“父剧 TMDb ID + S/E”作为定位键，
        # 但 media_metadata 里的 Season/Episode 行自身 tmdb_id 可能是 TMDb 的季/集 ID。
        # 因此这里不能只按 (tmdb_id, item_type) 查不到就放弃，要用父剧+季号合成一条虚拟行，
        # 再由 _collect_media_identifiers 下钻 Episode 的 PC/SHA1。
        if item_type == 'Season':
            parent_series_id = data.get('parent_series_tmdb_id') or data.get('series_tmdb_id') or tmdb_id
            row = {
                'tmdb_id': str(parent_series_id or tmdb_id),
                'item_type': 'Season',
                'parent_series_tmdb_id': str(parent_series_id or tmdb_id),
                'season_number': data.get('season_number'),
                'title': data.get('title') or data.get('root_name') or '',
            }
        elif item_type == 'Episode':
            parent_series_id = data.get('parent_series_tmdb_id') or data.get('series_tmdb_id') or tmdb_id
            row = {
                'tmdb_id': str(data.get('episode_tmdb_id') or tmdb_id),
                'item_type': 'Episode',
                'parent_series_tmdb_id': str(parent_series_id or ''),
                'season_number': data.get('season_number'),
                'episode_number': data.get('episode_number'),
                'file_sha1_json': data.get('file_sha1_json'),
                'file_pickcode_json': data.get('file_pickcode_json'),
                'title': data.get('title') or data.get('root_name') or '',
            }
        else:
            return []

    ids = _collect_media_identifiers(row)
    file_rows = _get_p115_file_rows(ids.get('pickcodes') or [], ids.get('sha1s') or [])
    if not file_rows:
        return []

    episode_meta_by_sha1 = {}
    episode_meta_by_pc = {}
    for ep in ids.get('episode_rows') or []:
        for sha in _norm_sha1_list(_json_array_values(ep.get('file_sha1_json'))):
            episode_meta_by_sha1[sha] = ep
        for pc in _norm_pc_list(_json_array_values(ep.get('file_pickcode_json'))):
            episode_meta_by_pc[pc] = ep

    files = []
    seen = set()
    for r in file_rows:
        fid = str(r.get('id') or '')
        if not fid or fid in seen:
            continue
        seen.add(fid)
        name = str(r.get('name') or '')
        if not _looks_like_video_name(name):
            continue
        sha1 = str(r.get('sha1') or '').upper()
        pc = str(r.get('pick_code') or '')
        ep = episode_meta_by_sha1.get(sha1) or episode_meta_by_pc.get(pc) or {}
        ep_no = ep.get('episode_number') or _guess_episode_number(name)
        season_no = ep.get('season_number') or data.get('season_number')
        item_tmdb_id = ep.get('tmdb_id') or data.get('tmdb_id') or ''
        item_type_for_file = 'Episode' if ep or ep_no else data.get('item_type')
        files.append({
            'fid': fid,
            'sha1': sha1 or None,
            'pick_code': pc or None,
            'size': _safe_size_bytes(r.get('size')),
            'file_name': name,
            'relative_path': r.get('local_path') or name,
            'tmdb_id': str(item_tmdb_id),
            'item_type': item_type_for_file,
            'season_number': season_no,
            'episode_number': ep_no,
            'raw_json': {'source': 'media_metadata+p115_filesystem_cache', 'cache_row': r, 'episode_meta': ep},
        })
    return files

def _collect_files_from_115(client, root_fid: str, root_name: str = '', max_depth: int = 3, current_path: str = '', assume_dir=None) -> List[Dict[str, Any]]:
    """递归收集分享目录下的视频文件。
    目录识别优先使用前端/搜索阶段传来的 root_is_dir；远程返回不完整时自动兜底本地缓存。
    """
    info_resp = client.fs_get_info(root_fid)
    root_info = (info_resp or {}).get('data') or {}
    if not root_name:
        root_name = _node_name(root_info) or str(root_fid)

    is_dir = bool(assume_dir) if assume_dir is not None else _is_folder(root_info)

    if root_info and not is_dir:
        name = _node_name(root_info)
        ext = os.path.splitext(name)[1].lower()
        if ext not in VIDEO_EXTENSIONS:
            # 115 fs_get_info 有时会把目录返回成普通节点；不要直接放弃，
            # 先尝试用本地 p115_filesystem_cache 递归兜底。
            cached_files = _collect_files_from_cache(root_fid, root_name=root_name, max_depth=max_depth + 3)
            if cached_files:
                return cached_files
            return []
        return [{
            'fid': _node_id(root_info) or str(root_fid),
            'sha1': root_info.get('sha1') or root_info.get('sha') or root_info.get('file_sha1'),
            'pick_code': root_info.get('pc') or root_info.get('pick_code') or root_info.get('pickcode'),
            'size': _safe_size_bytes(root_info.get('size') or root_info.get('fs') or root_info.get('s')),
            'file_name': name,
            'relative_path': name,
            'episode_number': _guess_episode_number(name),
            'raw_json': root_info,
        }]

    files = []

    def walk(cid: str, prefix: str, depth: int):
        if depth < 0:
            return
        resp = client.fs_files({'cid': cid, 'limit': 1000, 'offset': 0, 'show_dir': 1})
        for node in (resp or {}).get('data') or []:
            name = _node_name(node)
            if not name:
                continue
            node_id = _node_id(node)
            rel = f"{prefix}/{name}" if prefix else name
            if _is_folder(node):
                if node_id:
                    walk(node_id, rel, depth - 1)
                continue
            ext = os.path.splitext(name)[1].lower()
            if ext not in VIDEO_EXTENSIONS:
                continue
            files.append({
                'fid': node_id,
                'sha1': node.get('sha1') or node.get('sha') or node.get('file_sha1'),
                'pick_code': node.get('pc') or node.get('pick_code') or node.get('pickcode'),
                'size': _safe_size_bytes(node.get('size') or node.get('fs') or node.get('s')),
                'file_name': name,
                'relative_path': rel,
                'episode_number': _guess_episode_number(name),
                'raw_json': node,
            })

    walk(root_fid, '', max_depth)

    # 远程接口没有拿到时，用本地 p115_filesystem_cache 兜底。
    # 不再只在 is_dir=True 时兜底，因为部分 115 接口会把目录详情误返回成文件节点。
    if not files:
        files = _collect_files_from_cache(root_fid, root_name=root_name, max_depth=max_depth + 3)

    return files

def _parse_share_status(snap_resp: Dict[str, Any]) -> Dict[str, str]:
    if not snap_resp or not snap_resp.get('state'):
        msg = str((snap_resp or {}).get('error') or (snap_resp or {}).get('error_msg') or (snap_resp or {}).get('message') or snap_resp)
        # 115 审核中有时会表现为暂不可访问，先归为 pending_review，避免误判死链。
        return {'status': 'pending_review', 'review_status': 'pending_review', 'message': msg}
    data = snap_resp.get('data') or {}
    info = data.get('shareinfo') or {}
    share_state = str(info.get('share_state') or data.get('share_state') or '')
    forbid = info.get('forbid_reason') or ''
    if share_state == '1' and not forbid:
        return {'status': 'alive', 'review_status': 'alive', 'message': '分享可用'}
    if forbid or info.get('have_vio_file'):
        return {'status': 'rejected', 'review_status': 'rejected', 'message': forbid or '分享包含违规/被屏蔽文件'}
    return {'status': 'pending_review', 'review_status': 'pending_review', 'message': f'分享状态 {share_state or "未知"}'}

def _center_headers():
    cfg = _get_shared_config()
    if not cfg['device_token']:
        raise RuntimeError('未配置共享中心 device_token')
    return cfg, _center_headers_for_cfg(cfg)

def _center_json_request(method: str, path: str, *, params: Dict[str, Any] = None, json_body: Dict[str, Any] = None, timeout: int = 25) -> Dict[str, Any]:
    cfg, headers = _center_headers()
    method = str(method or 'GET').upper()
    url = f"{cfg['center_url']}{path}"
    if method == 'GET':
        resp = requests.get(url, headers=headers, params=params or {}, **_center_request_kwargs(timeout))
    else:
        resp = requests.request(method, url, headers=headers, params=params or {}, json=json_body or {}, **_center_request_kwargs(timeout))
    if not resp.ok:
        try:
            data = resp.json() if resp.text else {}
            msg = data.get('message') or data.get('detail') or resp.text[:300]
        except Exception:
            msg = resp.text[:300]
        raise RuntimeError(msg or f'中心接口 HTTP {resp.status_code}')
    return resp.json() if resp.text else {}


def _looks_resource_violation_response(resp: Any) -> bool:
    try:
        text = json.dumps(resp, ensure_ascii=False).lower()
    except Exception:
        text = str(resp or '').lower()

    resource_violation_keywords = (
        '文件违规',
        '内容违规',
        '资源违规',
        '涉嫌违规',
        '违规文件',
        '违规资源',
        '禁止分享该文件',
        '禁止分享文件',
        '禁止分享此文件',
        '审核失败',
        '审核不通过',
        '被屏蔽',
        '侵权',
        '违法',
        '敏感',
        '暴恐',
        '涉政',
        '暴恐涉政',
        '恐怖',
        '恐怖主义',
        '政治敏感',
        'violation',
        'illegal',
        'copyright',
        'forbidden by policy',
        'risk file',
    )

    account_limit_keywords = (
        '24小时',
        '24 小时',
        '账号',
        '账户',
        '功能权限',
        '分享功能',
        '分享功能受限',
        '分享功能被限制',
        '禁止分享功能',
        '限制分享',
        '被限制分享',
        '你已被限制分享',
        '接收功能',
        '接收功能受限',
        '接收功能被限制',
        '限制接收',
        '被限制接收',
        '你已被限制接收',
        '转存功能',
        '转存功能受限',
        '转存功能被限制',
        '限制转存',
        '被限制转存',
        '频繁',
        '4200041',
        'rate limit',
        'too many',
        'account',
        'permission',
    )

    # 明确出现资源违规关键词时，优先认为是资源级风险。
    # 这样“账号因分享暴恐涉政资源被限制”也能把资源上报中心黑名单。
    if any(k in text for k in resource_violation_keywords):
        return True

    # 没有资源违规关键词，只是账号/频率/功能限制时，不上报资源黑名单。
    if any(k in text for k in account_limit_keywords):
        return False

    return False


def _center_blacklist_item_for_share(data: Dict[str, Any], standard_identity: Dict[str, Any] = None) -> Dict[str, Any]:
    data = dict(data or {})
    standard_identity = dict(standard_identity or {})
    share_type = str(data.get('share_type') or '').strip().lower()
    item_type = standard_identity.get('item_type') or data.get('item_type') or ''
    if share_type in ('season_pack', 'tv_pack'):
        item_type = 'Season'
    elif share_type == 'series_pack':
        item_type = 'Series'
    elif share_type == 'episode_file':
        item_type = 'Episode'
    tmdb_id = standard_identity.get('parent_series_tmdb_id') or standard_identity.get('tmdb_id') or data.get('parent_series_tmdb_id') or data.get('tmdb_id')
    return {
        'tmdb_id': str(tmdb_id or '').strip(),
        'item_type': item_type or ('Movie' if share_type in ('movie', 'movie_file', 'movie_folder') else 'Season'),
        'season_number': standard_identity.get('season_number') if standard_identity.get('season_number') not in (None, '') else data.get('season_number'),
        'episode_number': standard_identity.get('episode_number') if standard_identity.get('episode_number') not in (None, '') else data.get('episode_number'),
        'title': standard_identity.get('title') or data.get('title') or data.get('root_name') or '',
        'release_year': standard_identity.get('release_year') or data.get('release_year'),
    }


def _check_center_resource_blacklist(item: Dict[str, Any]) -> Dict[str, Any]:
    cfg = _get_shared_config()
    if not cfg.get('enabled') or not cfg.get('device_token'):
        return {}
    try:
        resp = _center_json_request('POST', '/api/v1/blacklist/check', json_body={'item': item}, timeout=15)
        if resp.get('blacklisted'):
            return resp.get('first_match') or {'blacklisted': True, 'message': '命中中心黑名单'}
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 中心黑名单检查失败，为安全起见阻止创建分享: {e}")
        return {'blacklisted': True, 'message': f'中心黑名单检查失败：{e}'}
    return {}


def _report_center_resource_blacklist(item: Dict[str, Any], resp: Any, reason: str = 'share_blocked') -> None:
    cfg = _get_shared_config()
    if not cfg.get('enabled') or not cfg.get('device_token'):
        return
    try:
        _center_json_request(
            'POST', '/api/v1/blacklist/report', timeout=20,
            json_body={**(item or {}), 'reason': reason, 'source': 'manual_share', 'message': json.dumps(resp, ensure_ascii=False, default=str)},
        )
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 上报中心资源黑名单失败: {e}")

def _tmdb_api_key_for_share_request() -> str:
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
        val = config_manager.APP_CONFIG.get(key)
        if val:
            return str(val).strip()
    return ''

def _normalize_tmdb_search_item(item: Dict[str, Any]) -> Dict[str, Any]:
    item = item or {}
    media_type = str(item.get('media_type') or '').strip().lower()
    if not media_type:
        media_type = 'movie' if item.get('title') or item.get('release_date') else 'tv'
    title = item.get('title') or item.get('name') or item.get('original_title') or item.get('original_name') or ''
    release_date = item.get('release_date') or item.get('first_air_date') or ''
    year = None
    m = re.search(r'((?:19|20)\d{2})', str(release_date))
    if m:
        try:
            year = int(m.group(1))
        except Exception:
            year = None
    return {
        'tmdb_id': str(item.get('id') or ''),
        'media_type': 'movie' if media_type == 'movie' else 'tv',
        'title': title,
        'release_year': year,
        'release_date': release_date,
        'poster_path': item.get('poster_path') or '',
        'overview': item.get('overview') or '',
        'raw': item,
    }

def _cancel_center_sources_for_share(record_id: int, record: Dict[str, Any]) -> Dict[str, Any]:
    """撤销共享中心登记的源，并触发中心贡献值重算。"""
    cfg = _get_shared_config()
    if not cfg.get('enabled'):
        return {'ok': True, 'skipped': True, 'message': '共享中心未启用'}
    if not cfg.get('device_token'):
        return {'ok': False, 'skipped': True, 'message': '未配置共享中心 device_token'}

    share_code = str((record or {}).get('share_code') or '').strip()
    source_ids = set()
    sha1_list = set()
    if (record or {}).get('center_source_id'):
        source_ids.add(str(record.get('center_source_id')).strip())
    try:
        for item in shared_share_db.list_share_items(record_id) or []:
            sid = str(item.get('center_source_id') or '').strip()
            if sid:
                source_ids.add(sid)
            sha1 = str(item.get('sha1') or '').strip().upper()
            if sha1:
                sha1_list.add(sha1)
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 收集中心 source_id/sha1 失败: record={record_id}, err={e}")

    if not share_code and not source_ids and not sha1_list:
        return {'ok': True, 'skipped': True, 'message': '本地没有 share_code/source_id/sha1，无需撤销中心源'}

    headers = _center_headers_for_cfg(cfg)
    payload = {
        'share_code': share_code or None,
        'source_ids': sorted(source_ids),
        'sha1_list': sorted(sha1_list),
        'delete_raw_ffprobe': True,
        'reason': 'share_cancelled',
        'local_record_id': record_id,
    }
    try:
        resp = requests.post(f"{cfg['center_url']}/api/v1/sources/cancel", headers=headers, json=payload, **_center_request_kwargs(25))
        if not resp.ok:
            return {'ok': False, 'status_code': resp.status_code, 'message': resp.text[:300], 'payload': payload}
        data = resp.json() if resp.text else {}
        data.setdefault('ok', True)
        data['payload'] = payload
        return data
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 撤销中心共享源失败: record={record_id}, err={e}", exc_info=True)
        return {'ok': False, 'message': str(e), 'payload': payload}

def _center_status_after_cancel_result(center_result: Dict[str, Any]) -> str:
    """中心撤销接口可能把源保留为待补充；本地中心状态要如实显示。"""
    if not isinstance(center_result, dict):
        return 'cancelled'
    if int(center_result.get('replenish_count') or 0) > 0 or str(center_result.get('status') or '').lower() == CENTER_SOURCE_STATUS_REPLENISH:
        return CENTER_SOURCE_STATUS_REPLENISH
    return 'cancelled'

def _center_cancel_result_text(center_result: Dict[str, Any]) -> str:
    if not isinstance(center_result, dict):
        return str(center_result or '')
    replenish_count = int(center_result.get('replenish_count') or 0)
    removed_count = int(center_result.get('removed_count') or 0)
    raw_removed = int(center_result.get('removed_raw_ffprobe_count') or 0)
    raw_file_removed = int(center_result.get('removed_raw_file_count') or 0)
    credit = center_result.get('credit')
    parts = []
    if replenish_count:
        parts.append(f"中心已将 {replenish_count} 个共享源转为待补充")
    if removed_count:
        parts.append(f"中心已撤销 {removed_count} 个共享源")
    if raw_removed or raw_file_removed:
        parts.append(f"删除媒体信息 {raw_removed} 条/文件 {raw_file_removed} 个")
    if credit is not None:
        parts.append(f"当前贡献值 {credit}")
    return '，'.join(parts) if parts else (center_result.get('message') or '中心无匹配源需要处理')

def _safe_json_obj(value):
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            obj = json.loads(value)
            return obj if isinstance(obj, dict) else None
        except Exception:
            return None
    return None

# ----------------------------------------------------------------------
# 我的分享来源标记
# ----------------------------------------------------------------------
def _decorate_my_share_source_row(item: Dict[str, Any]) -> Dict[str, Any]:
    """给“我的分享”本地记录补充来源字段。

    判定原则只认明确结构化标记：
    - 自动分享：tasks/shared_resource_tasks.py 创建记录时写入 raw_json.auto_gap；
    - 手动分享：manual-create 接口创建记录时写入 raw_json.manual_payload；
    - 已登记到中心后的 source_provider 只作为辅助展示，不再用 raw_json 全文里
      的 task/gap/维护 等词做模糊扫描，避免手动分享被误判为自动分享。
    """
    item = item or {}
    raw = _safe_json_obj(item.get('raw_json')) or {}

    def _norm(value: Any) -> str:
        return str(value or '').strip().lower().replace('-', '_').replace(' ', '_')

    provider = str(
        item.get('source_provider') or item.get('share_source') or item.get('create_mode') or
        raw.get('source_provider') or raw.get('share_source') or raw.get('create_mode') or ''
    ).strip()
    provider_norm = _norm(provider)

    label_text = ' '.join([
        str(item.get('source_provider_label') or ''),
        str(item.get('source_label') or ''),
        str(raw.get('source_provider_label') or ''),
        str(raw.get('source_label') or ''),
    ]).strip().lower()

    backup_providers = {'backup_mirror', 'backup_share', 'auto_backup_share'}
    auto_providers = {
        'auto_gap_share', 'request_share', 'auto_share', 'auto_task', 'auto',
        'maintenance', 'maintenance_task', 'maintenance_share', 'maintenance_auto_share',
        'scheduler', 'scheduled_share', 'gap_share', 'watching_gap_share',
    }
    manual_providers = {
        'user_share', 'manual_share', 'manual', 'user', 'local_manual', 'manual_create', 'manual_created',
    }

    raw_backup = any(raw.get(k) for k in (
        'auto_backup_share', 'backup_share', 'backup_mirror', 'backup_instruction',
    ))
    raw_auto = any(raw.get(k) for k in (
        'auto_gap', 'auto_payload', 'auto_task', 'maintenance_payload', 'maintenance_task',
        'auto_share_payload', 'auto_context',
    ))
    raw_manual = any(raw.get(k) for k in (
        'manual_payload', 'manual_share', 'manual_create', 'manual_created', 'manual_context',
    ))

    is_backup = False
    # 备份分享是中心指令自动生成的镜像源，不能兜底成“手动分享”。
    if raw_backup or provider_norm in backup_providers or '备份分享' in label_text:
        is_auto = True
        is_backup = True
        provider = provider or 'backup_mirror'
        label = '备份分享'
    # 明确手动 > 明确自动。手动创建的记录即便后续由维护任务自动检查/登记中心，
    # 备注仍应显示“手动分享”，因为资源来源是用户手动创建。
    elif raw_manual or provider_norm in manual_providers or '手动分享' in label_text:
        is_auto = False
        provider = provider or 'manual_share'
        label = '手动分享'
    elif raw_auto or provider_norm in auto_providers or '自动分享' in label_text:
        is_auto = True
        provider = provider or 'auto_gap_share'
        label = '自动分享'
    else:
        # 老数据没有来源字段时，按本地前端手动分享兜底。
        is_auto = False
        provider = provider or 'manual_share'
        label = '手动分享'

    item['source_provider'] = provider
    item['source_provider_label'] = label
    item['source_label'] = label
    item['is_auto_share'] = bool(is_auto)
    item['is_manual_share'] = not bool(is_auto) and not is_backup
    item['is_backup_share'] = bool(is_backup)
    return item

def _raw_ffprobe_has_media_payload(raw: Dict[str, Any]) -> bool:
    """判断 raw_ffprobe_json 是否包含可用于展示/整理的真实媒体信息。"""
    if not isinstance(raw, dict):
        return False
    if isinstance(raw.get('format'), dict) or isinstance(raw.get('streams'), list):
        return True
    if isinstance(raw.get('MediaSourceInfo'), dict) or isinstance(raw.get('MediaStreams'), list):
        return True
    return False

def _normalize_raw_etk_type(value: Any) -> str:
    text = str(value or '').strip().lower()
    if text in {'movie', 'movies', 'film', '电影'}:
        return 'movie'
    if text in {'tv', 'series', 'season', 'episode', '电视剧', '剧集', '季', '集', '分集'}:
        return 'tv'
    return ''

def _find_nested_raw_value(value: Any, keys: set, max_depth: int = 4):
    """在分享 item/raw_json/cache_row 等嵌套结构里提取 PC/FID/名称等稳定字段。"""
    if max_depth < 0:
        return None
    if isinstance(value, dict):
        for key in keys:
            if value.get(key) not in [None, '', [], {}]:
                return value.get(key)
        for child in value.values():
            found = _find_nested_raw_value(child, keys, max_depth - 1)
            if found not in [None, '', [], {}]:
                return found
    elif isinstance(value, (list, tuple)):
        for child in value:
            found = _find_nested_raw_value(child, keys, max_depth - 1)
            if found not in [None, '', [], {}]:
                return found
    return None

def _share_item_raw_metadata_context(item: Dict[str, Any]) -> Dict[str, Any]:
    """从分享文件行构建 raw_ffprobe_json._etk 所需的媒体身份。

    P115CacheManager.get_raw_ffprobe_cache 会优先按 SHA1 从 media_metadata 回填；这里再用
    分享上下文兜底，覆盖自动分享/手动分享/维护任务里还没落库或 Episode 行只带在
    raw_json.episode_meta 内的场景。
    """
    item = item or {}
    raw = item.get('raw_json') if isinstance(item.get('raw_json'), dict) else {}
    episode_meta = raw.get('episode_meta') if isinstance(raw.get('episode_meta'), dict) else {}

    item_type = (
        item.get('item_type')
        or item.get('share_type')
        or episode_meta.get('item_type')
        or raw.get('item_type')
    )
    normalized_type = _normalize_raw_etk_type(item_type)

    tmdb_id = (
        item.get('parent_series_tmdb_id')
        or item.get('series_tmdb_id')
        or episode_meta.get('parent_series_tmdb_id')
        or episode_meta.get('series_tmdb_id')
        or item.get('tmdb_id')
        or episode_meta.get('tmdb_id')
        or raw.get('tmdb_id')
    )

    season_number = (
        item.get('season_number')
        if item.get('season_number') not in [None, '']
        else episode_meta.get('season_number')
    )
    episode_number = (
        item.get('episode_number')
        if item.get('episode_number') not in [None, '']
        else episode_meta.get('episode_number')
    )

    ctx = {
        'tmdb_id': str(tmdb_id).strip() if tmdb_id not in [None, ''] else None,
        'type': normalized_type or None,
        'original_language': (
            str(item.get('original_language') or episode_meta.get('original_language') or raw.get('original_language') or '').strip()
            or None
        ),
        'season_number': _safe_int(season_number, None) if season_number not in [None, ''] else None,
        'episode_number': _safe_int(episode_number, None) if episode_number not in [None, ''] else None,
        'sha1': str(item.get('sha1') or '').strip().upper() or None,
    }
    return {k: v for k, v in ctx.items() if v not in [None, '', [], {}]}

def _raw_ffprobe_missing_etk_fields(raw: Dict[str, Any], expected_ctx: Dict[str, Any] = None) -> List[str]:
    """返回 RAW 上传中心前仍缺失的 _etk 字段。

    tmdb_id/type/sha1 是共享中心识别和消费端免二次猜测的硬字段；
    original_language/season_number/episode_number 如果本地上下文已经明确，也必须写回。
    """
    if not isinstance(raw, dict):
        return ['_etk']
    ctx = raw.get('_etk') if isinstance(raw.get('_etk'), dict) else {}
    expected_ctx = expected_ctx or {}
    missing = []

    for key in ('tmdb_id', 'type', 'sha1'):
        if not ctx.get(key):
            missing.append(key)

    for key in ('original_language', 'season_number', 'episode_number'):
        if expected_ctx.get(key) not in [None, '', [], {}] and ctx.get(key) in [None, '', [], {}]:
            missing.append(key)

    return list(dict.fromkeys(missing))

def _patch_raw_ffprobe_etk_from_item(sha1: str, item: Dict[str, Any], *, force_identity: bool = False) -> bool:
    """把分享上下文写回 p115_mediainfo_cache.raw_ffprobe_json._etk。"""
    ctx = _share_item_raw_metadata_context({**(item or {}), 'sha1': sha1})
    if not ctx:
        return False
    try:
        return bool(P115CacheManager.patch_raw_ffprobe_etk_context(
            sha1,
            tmdb_id=ctx.get('tmdb_id'),
            media_type=ctx.get('type'),
            original_language=ctx.get('original_language'),
            season_number=ctx.get('season_number'),
            episode_number=ctx.get('episode_number'),
            force_identity=force_identity,
        ))
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 修复 raw_ffprobe _etk 失败: sha1={str(sha1)[:12]}..., err={e}")
        return False

def _extract_pick_code_from_share_item(item: Dict[str, Any], sha1: str = '', client=None) -> str:
    """尽量从分享行、p115_filesystem_cache 或 115 实时详情中找 pick_code。"""
    item = item or {}
    pc = _find_nested_raw_value(item, {'pc', 'pick_code', 'pickcode'})
    if pc:
        return str(pc).strip()

    raw = item.get('raw_json') if isinstance(item.get('raw_json'), dict) else {}
    # shared_share_items.id 是本地数据库行 ID，不是 115 FID；顶层不能用 id 兜底。
    fid = str(item.get('fid') or item.get('file_id') or _find_nested_raw_value(raw, {'fid', 'file_id', 'id'}) or '').strip()

    try:
        cache_row = None
        if sha1:
            cache_row = P115CacheManager.get_file_cache_by_sha1(sha1)
        if not cache_row and fid:
            cache_row = P115CacheManager.get_file_cache_by_id(fid)
        if cache_row:
            pc = cache_row.get('pick_code') or cache_row.get('pc') or cache_row.get('pickcode')
            if pc:
                return str(pc).strip()
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 查询 p115_filesystem_cache 获取 PC 失败: sha1={sha1[:12]}..., fid={fid}, err={e}")

    if fid and client:
        try:
            info = client.fs_get_info(fid)
            node = (info or {}).get('data') or {}
            pc = _find_nested_raw_value(node, {'pc', 'pick_code', 'pickcode'})
            if pc:
                return str(pc).strip()
        except Exception as e:
            logger.debug(f"  ➜ [共享资源] 实时查询 115 文件详情获取 PC 失败: fid={fid}, err={e}")

    return ''

def _build_raw_probe_file_node(item: Dict[str, Any], sha1: str, pick_code: str) -> Dict[str, Any]:
    item = item or {}
    raw = item.get('raw_json') if isinstance(item.get('raw_json'), dict) else {}
    name = (
        item.get('file_name')
        or item.get('relative_path')
        or item.get('name')
        or item.get('title')
        or _find_nested_raw_value(raw, {'fn', 'n', 'file_name', 'name', 'title'})
        or sha1
        or 'unknown.mkv'
    )
    fid = item.get('fid') or item.get('file_id') or _find_nested_raw_value(raw, {'fid', 'file_id', 'id'})
    size = _safe_size_bytes(
        item.get('size')
        or item.get('fs')
        or _find_nested_raw_value(raw, {'size', 'fs', 'file_size', 's'})
    )
    return {
        'fid': str(fid or ''),
        'file_id': str(fid or ''),
        'pc': str(pick_code or '').strip(),
        'pick_code': str(pick_code or '').strip(),
        'pickcode': str(pick_code or '').strip(),
        'fn': os.path.basename(str(name)),
        'n': os.path.basename(str(name)),
        'file_name': os.path.basename(str(name)),
        'original_name': os.path.basename(str(name)),
        'sha1': sha1,
        'fs': size,
        'size': size,
    }

def _probe_and_cache_raw_ffprobe_for_share_item(item: Dict[str, Any], sha1: str, metadata_context: Dict[str, Any]) -> Dict[str, Any]:
    """本地没有完整 RAW 时，通过 115 直链在线 ffprobe 并写回 p115_mediainfo_cache。"""
    sha1 = str(sha1 or '').strip().upper()
    if not re.fullmatch(r'[A-Fa-f0-9]{40}', sha1):
        return {'ok': False, 'message': '缺少合法 SHA1，无法在线提取 RAW'}

    client = P115Service.get_client()
    if not client:
        return {'ok': False, 'message': '未配置可用的 115 客户端，无法在线提取 RAW'}

    pick_code = _extract_pick_code_from_share_item(item, sha1=sha1, client=client)
    if not pick_code:
        return {'ok': False, 'message': '缺少 PickCode，无法通过 115 直链在线提取 RAW'}

    try:
        from handler.p115_service import SmartOrganizer
        analyzer = SmartOrganizer.__new__(SmartOrganizer)
        analyzer.client = client
        try:
            import utils as _utils
            analyzer.language_map = settings_db.get_setting('language_mapping') or getattr(_utils, 'DEFAULT_LANGUAGE_MAPPING', [])
            analyzer.stream_feature_map = settings_db.get_setting('stream_feature_mapping') or getattr(_utils, 'DEFAULT_STREAM_FEATURE_MAPPING', [])
        except Exception:
            pass

        file_node = _build_raw_probe_file_node(item, sha1, pick_code)
        probe_result = analyzer._probe_mediainfo_with_ffprobe(
            file_node=file_node,
            sha1=sha1,
            silent_log=True,
            metadata_context=metadata_context or {},
        ) or (None, None)
        emby_json, raw_ffprobe = probe_result
        raw_ffprobe = _safe_json_obj(raw_ffprobe)

        if not emby_json or not raw_ffprobe or not _raw_ffprobe_has_media_payload(raw_ffprobe):
            return {'ok': False, 'message': '在线 ffprobe 未能提取到完整 format/streams RAW'}

        if not P115CacheManager.save_mediainfo_cache(sha1, emby_json, raw_ffprobe):
            return {'ok': False, 'message': '在线提取成功，但写入 p115_mediainfo_cache 失败'}

        logger.info(f"  ➜ [共享资源] 已在线提取并写回 raw_ffprobe_json: sha1={sha1[:12]}...")
        return {'ok': True, 'raw': raw_ffprobe, 'message': '已在线提取 raw_ffprobe_json'}
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 在线提取 raw_ffprobe_json 失败: sha1={sha1[:12]}..., err={e}", exc_info=True)
        return {'ok': False, 'message': f'在线提取 raw_ffprobe_json 失败: {e}'}

def _ensure_local_raw_ffprobe_for_share_item(item: Dict[str, Any], *, allow_online_probe: bool = True) -> Dict[str, Any]:
    """统一 RAW 校验入口。

    适用于手动创建、手动上传、自动登记、维护任务：
    1. 本地有完整 RAW 但缺 _etk 字段：只重新计算/回写 _etk；
    2. 本地没有完整 format/streams RAW：通过 115 直链在线 ffprobe 并写回缓存；
    3. 最终仍没有完整 RAW 或核心 _etk，则返回 missing，禁止继续登记中心。
    """
    item = item or {}
    sha1 = str(item.get('sha1') or '').strip().upper()
    if not re.fullmatch(r'[A-Fa-f0-9]{40}', sha1):
        return {'ok': False, 'status': 'missing_sha1', 'message': '缺少 SHA1'}

    changed = False
    metadata_context = _share_item_raw_metadata_context(item)

    try:
        raw = _safe_json_obj(P115CacheManager.get_raw_ffprobe_cache(sha1))
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 读取 raw_ffprobe 缓存失败: sha1={sha1[:12]}..., err={e}")
        raw = None

    # 有完整媒体 RAW 时，只补 _etk，不重新跑 ffprobe。
    if raw and _raw_ffprobe_has_media_payload(raw):
        missing_etk = _raw_ffprobe_missing_etk_fields(raw, metadata_context)
        if missing_etk:
            if _patch_raw_ffprobe_etk_from_item(sha1, item):
                changed = True
                raw = _safe_json_obj(P115CacheManager.get_raw_ffprobe_cache(sha1)) or raw
                missing_etk = _raw_ffprobe_missing_etk_fields(raw, metadata_context)
        if missing_etk:
            return {
                'ok': False,
                'status': 'missing_etk',
                'message': f"raw_ffprobe_json 缺少 _etk.{','.join(missing_etk)}，且无法从本地媒体库回填",
                'raw': raw,
                'changed': changed,
            }
        return {'ok': True, 'status': 'ready', 'raw': raw, 'changed': changed}

    # 没有完整 RAW：只在这里在线 ffprobe，避免为了补 _etk 重扫媒体流。
    if allow_online_probe:
        probe_result = _probe_and_cache_raw_ffprobe_for_share_item(item, sha1, metadata_context)
        if probe_result.get('ok'):
            changed = True
            raw = _safe_json_obj(P115CacheManager.get_raw_ffprobe_cache(sha1)) or _safe_json_obj(probe_result.get('raw'))
            if raw and _raw_ffprobe_has_media_payload(raw):
                missing_etk = _raw_ffprobe_missing_etk_fields(raw, metadata_context)
                if missing_etk and _patch_raw_ffprobe_etk_from_item(sha1, item):
                    raw = _safe_json_obj(P115CacheManager.get_raw_ffprobe_cache(sha1)) or raw
                    missing_etk = _raw_ffprobe_missing_etk_fields(raw, metadata_context)
                if not missing_etk:
                    return {'ok': True, 'status': 'probed', 'raw': raw, 'changed': changed}
                return {
                    'ok': False,
                    'status': 'missing_etk',
                    'message': f"在线提取 RAW 成功，但 _etk.{','.join(missing_etk)} 仍缺失",
                    'raw': raw,
                    'changed': changed,
                }
        return {
            'ok': False,
            'status': 'missing_raw',
            'message': probe_result.get('message') or '本地 p115_mediainfo_cache 缺少完整 raw_ffprobe_json',
            'changed': changed,
        }

    return {
        'ok': False,
        'status': 'missing_raw',
        'message': '本地 p115_mediainfo_cache 缺少完整 raw_ffprobe_json',
        'changed': changed,
    }

def _load_local_raw_ffprobe(sha1: str, item: Dict[str, Any] = None, *, allow_online_probe: bool = False):
    """兼容旧调用：读取并校验本地 RAW；显式允许时才在线提取。"""
    sha1 = str(sha1 or '').strip().upper()
    source_item = dict(item or {})
    source_item.setdefault('sha1', sha1)
    result = _ensure_local_raw_ffprobe_for_share_item(source_item, allow_online_probe=allow_online_probe)
    return result.get('raw') if result.get('ok') else None

def _infer_size_from_raw(raw: Dict[str, Any]) -> int:
    if not isinstance(raw, dict):
        return 0
    try:
        fmt = raw.get('format') or {}
        size = fmt.get('size')
        if size is not None and str(size).strip():
            return _safe_size_bytes(size)
    except Exception:
        pass
    return 0

def _build_raw_ffprobe_summary_for_center(raw: Dict[str, Any], item: Dict[str, Any], final_size: int = 0) -> Dict[str, Any]:
    """上传 RAW 时同步生成中心列表页轻量 MediaInfo 摘要。

    完整 raw_ffprobe_json 仍然作为资产上传和保存；summary_json 只服务中心资源库列表页，
    避免前端打开列表时再拉完整 RAW / 解压 zst / 重跑 MediaInfo 格式化。
    """
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
    }
    compact = {k: summary.get(k) for k in allowed_keys if k in summary}

    # 防御性压缩：列表页只需要展示语言/参数，不要让异常数据把 summary_json 撑大。
    for key, max_len in (('audio_list', 16), ('subtitle_list', 24), ('audios', 16), ('subtitles', 24)):
        value = compact.get(key)
        if isinstance(value, list):
            compact[key] = value[:max_len]

    try:
        # 确保可 JSON 序列化，同时顺手把 Decimal/datetime 等意外对象转成字符串。
        return json.loads(json.dumps(compact, ensure_ascii=False, default=str))
    except Exception:
        return {}

def _upload_item_raw_ffprobe_to_center(item: Dict[str, Any], cfg: Dict[str, Any], headers: Dict[str, str], force: bool = False) -> Dict[str, Any]:
    """上传单个分享文件的 raw_ffprobe_json 到中心服务器。返回 ok/missing/error。"""
    sha1 = str(item.get('sha1') or '').strip().upper()
    if not re.fullmatch(r'[A-Fa-f0-9]{40}', sha1):
        return {'ok': False, 'status': 'missing_sha1', 'message': '缺少 SHA1'}

    ensure_result = _ensure_local_raw_ffprobe_for_share_item(item, allow_online_probe=True)
    if not ensure_result.get('ok'):
        return {
            'ok': False,
            'status': ensure_result.get('status') or 'missing_raw',
            'message': ensure_result.get('message') or '本地 p115_mediainfo_cache 没有完整 raw_ffprobe_json',
        }

    raw = ensure_result.get('raw')
    if item.get('raw_ffprobe_uploaded') and not force and not ensure_result.get('changed'):
        return {'ok': True, 'status': 'already_uploaded', 'message': '已标记上传过'}

    raw_size = _infer_size_from_raw(raw)
    item_size = _safe_size_bytes(item.get('size'))
    final_size = item_size if item_size > 0 else raw_size

    summary_json = _build_raw_ffprobe_summary_for_center(raw, item, final_size)

    payload = {
        'sha1': sha1,
        'size': final_size or None,
        'raw_ffprobe_json': raw,
        'summary_json': summary_json or None,
    }
    try:
        resp = requests.post(f"{cfg['center_url']}/api/v1/rawffprobe/upload", headers=headers, json=payload, **_center_request_kwargs(45))
        if not resp.ok:
            return {'ok': False, 'status': 'http_error', 'message': f'HTTP {resp.status_code} {resp.text[:160]}'}
        shared_share_db.mark_item_raw_uploaded(item['id'], True)
        if final_size > 0 and item_size <= 0:
            shared_share_db.update_share_item_size(item['id'], final_size)
        return {'ok': True, 'status': 'uploaded', 'message': '已上传 raw_ffprobe_json', 'size': final_size}
    except Exception as e:
        return {'ok': False, 'status': 'error', 'message': str(e)}

def _is_season_pack_record(record: Dict[str, Any]) -> bool:
    """只有显式季包/剧集包才走批量链路，单集和电影保持原单条接口。"""
    return str((record or {}).get('share_type') or '').strip().lower() in ('season_pack', 'series_pack', 'season', 'tv_pack')

def _upload_share_raw_ffprobe_to_center_single_loop(items: List[Dict[str, Any]], cfg: Dict[str, Any], headers: Dict[str, str], force: bool = False) -> Dict[str, Any]:
    uploaded = 0
    skipped = 0
    missing = 0
    errors = []
    size_fixed = 0
    for item in items or []:
        before_size = _safe_size_bytes(item.get('size'))
        result = _upload_item_raw_ffprobe_to_center(item, cfg, headers, force=force)
        if result.get('ok'):
            if result.get('status') == 'uploaded':
                uploaded += 1
                if before_size <= 0 and _safe_size_bytes(result.get('size')) > 0:
                    size_fixed += 1
            else:
                skipped += 1
        else:
            if result.get('status') in {'missing_raw', 'missing_sha1'}:
                missing += 1
            else:
                errors.append(f"{item.get('file_name')}: {result.get('message')}")
    return {
        'total': len(items or []),
        'uploaded': uploaded,
        'skipped': skipped,
        'missing': missing,
        'size_fixed': size_fixed,
        'errors': errors,
        'batch_used': False,
        'all_ok': (uploaded + skipped == len(items or []) and missing == 0 and not errors),
    }

def _upload_share_raw_ffprobe_to_center_batch(items: List[Dict[str, Any]], cfg: Dict[str, Any], headers: Dict[str, str], force: bool = False) -> Dict[str, Any]:
    uploaded = 0
    skipped = 0
    missing = 0
    errors = []
    size_fixed = 0
    prepared = []

    for item in items or []:
        sha1 = str(item.get('sha1') or '').strip().upper()
        if not re.fullmatch(r'[A-Fa-f0-9]{40}', sha1):
            missing += 1
            continue
        ensure_result = _ensure_local_raw_ffprobe_for_share_item(item, allow_online_probe=True)
        if not ensure_result.get('ok'):
            missing += 1
            continue
        if item.get('raw_ffprobe_uploaded') and not force and not ensure_result.get('changed'):
            skipped += 1
            continue
        raw = ensure_result.get('raw')
        raw_size = _infer_size_from_raw(raw)
        item_size = _safe_size_bytes(item.get('size'))
        final_size = item_size if item_size > 0 else raw_size
        summary_json = _build_raw_ffprobe_summary_for_center(raw, item, final_size)
        prepared.append({
            'item': item,
            'before_size': item_size,
            'final_size': final_size,
            'payload': {
                'sha1': sha1,
                'size': final_size or None,
                'raw_ffprobe_json': raw,
                'summary_json': summary_json or None,
            },
        })

    chunk_size = 10
    batch_unavailable = False
    for start in range(0, len(prepared), chunk_size):
        chunk = prepared[start:start + chunk_size]
        payload = {'items': [x['payload'] for x in chunk]}
        try:
            resp = requests.post(
                f"{cfg['center_url']}/api/v1/rawffprobe/upload-batch",
                headers=headers,
                json=payload,
                **_center_request_kwargs(120),
            )
            if resp.status_code in (404, 405):
                batch_unavailable = True
                raise RuntimeError(f"中心不支持批量RAW上传接口: HTTP {resp.status_code}")
            if not resp.ok:
                raise RuntimeError(f"HTTP {resp.status_code} {resp.text[:160]}")
            data = resp.json() or {}
            result_items = data.get('items') or []
        except Exception as e:
            # 批量请求整体失败时，对本 chunk 自动退回单条重传，避免季包因网络抖动整包失败。
            logger.warning(f"  ➜ [共享资源] 季包批量上传 RAW 失败，自动改为单条重传: {e}")
            result_items = [{'index': i, 'ok': False, 'message': str(e)} for i in range(len(chunk))]

        by_index = {int(r.get('index')): r for r in result_items if isinstance(r, dict) and str(r.get('index', '')).lstrip('-').isdigit()}
        for idx, info in enumerate(chunk):
            item = info['item']
            result = by_index.get(idx) or {}
            ok = bool(result.get('ok'))
            if ok:
                shared_share_db.mark_item_raw_uploaded(item['id'], True)
                if info['final_size'] > 0 and info['before_size'] <= 0:
                    shared_share_db.update_share_item_size(item['id'], info['final_size'])
                    size_fixed += 1
                uploaded += 1
                continue

            # 批量返回单项失败时，按要求自动单条重传一次。
            retry = _upload_item_raw_ffprobe_to_center(item, cfg, headers, force=True)
            if retry.get('ok'):
                if retry.get('status') == 'uploaded':
                    uploaded += 1
                    if info['before_size'] <= 0 and _safe_size_bytes(retry.get('size')) > 0:
                        size_fixed += 1
                else:
                    skipped += 1
            else:
                if retry.get('status') in {'missing_raw', 'missing_sha1'}:
                    missing += 1
                else:
                    errors.append(f"{item.get('file_name')}: 批量失败后单条重传失败 {retry.get('message') or result.get('message')}")

    return {
        'total': len(items or []),
        'uploaded': uploaded,
        'skipped': skipped,
        'missing': missing,
        'size_fixed': size_fixed,
        'errors': errors,
        'batch_used': True,
        'batch_unavailable': batch_unavailable,
        'all_ok': (uploaded + skipped == len(items or []) and missing == 0 and not errors),
    }

def _upload_share_raw_ffprobe_to_center(record_id: int, cfg: Dict[str, Any], headers: Dict[str, str], force: bool = False) -> Dict[str, Any]:
    record = shared_share_db.get_share_record(record_id) or {}
    items = shared_share_db.list_share_items(record_id) or []
    if _is_season_pack_record(record):
        return _upload_share_raw_ffprobe_to_center_batch(items, cfg, headers, force=force)
    return _upload_share_raw_ffprobe_to_center_single_loop(items, cfg, headers, force=force)

def _files_missing_raw_ffprobe(files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """返回缺失 raw_ffprobe_json 的分享文件列表。

    共享资源展示和中心入库都依赖 raw_ffprobe_json 解析清晰度、编码、音轨、字幕等信息。
    没有 raw 的文件禁止创建/登记分享，避免中心出现全是 "-" 的垃圾版本。
    """
    missing = []
    seen = set()
    for item in files or []:
        sha1 = str((item or {}).get('sha1') or '').strip().upper()
        name = str((item or {}).get('file_name') or (item or {}).get('name') or sha1 or '未知文件')
        key = sha1 or f"no-sha1:{name}"
        if key in seen:
            continue
        seen.add(key)
        if not re.fullmatch(r'[A-Fa-f0-9]{40}', sha1 or ''):
            missing.append({'sha1': sha1, 'file_name': name, 'reason': '缺少 SHA1，无法匹配 raw_ffprobe_json'})
            continue
        ensure_result = _ensure_local_raw_ffprobe_for_share_item(item, allow_online_probe=True)
        if not ensure_result.get('ok'):
            missing.append({'sha1': sha1, 'file_name': name, 'reason': ensure_result.get('message') or '本地 p115_mediainfo_cache 缺少完整 raw_ffprobe_json'})
    return missing

def _raw_missing_message(missing: List[Dict[str, Any]], limit: int = 6) -> str:
    shown = []
    for item in (missing or [])[:max(1, int(limit or 6))]:
        name = str(item.get('file_name') or item.get('sha1') or '未知文件')
        reason = str(item.get('reason') or '缺少 raw_ffprobe_json')
        shown.append(f"{name}（{reason}）")
    suffix = ''
    if len(missing or []) > len(shown):
        suffix = f" 等 {len(missing)} 个文件"
    return "缺少 raw_ffprobe_json，禁止分享/登记中心：" + "；".join(shown) + suffix

def _guess_season_episode_numbers(text: str):
    """从文件名/标题里尽量解析 SxxEyy，用于老数据和外来分享兜底。"""
    text = str(text or '')
    patterns = [
        r'[Ss](\d{1,3})[.\s_-]*[Ee](\d{1,4})',
        r'第\s*(\d{1,3})\s*季\s*第?\s*(\d{1,4})\s*[集话]',
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if not m:
            continue
        try:
            return int(m.group(1)), int(m.group(2))
        except Exception:
            return None, None
    return None, None

def _season_pack_consistency_identity(files: List[Dict[str, Any]], context: Dict[str, Any] = None) -> Dict[str, Any]:
    """从手动分享 payload / 本地分享记录 / 自动维护候选中解析父剧 TMDb 与季号。"""
    context = dict(context or {})
    files = files or []

    if not context and files:
        context = dict(files[0] or {})

    # 有些调用方把候选身份放在 share_tmdb_id/share_item_type，有些放在 tmdb_id/item_type。
    identity_payload = {
        'tmdb_id': context.get('share_tmdb_id') or context.get('tmdb_id'),
        'item_type': context.get('share_item_type') or context.get('item_type') or ('Season' if str(context.get('share_type') or '').lower() in ('season_pack', 'series_pack', 'tv_pack') else ''),
        'parent_series_tmdb_id': context.get('parent_series_tmdb_id') or context.get('series_tmdb_id'),
        'season_number': context.get('season_number'),
        'episode_number': context.get('episode_number'),
        'title': context.get('standard_title') or context.get('title') or context.get('display_title') or context.get('root_name'),
        'release_year': context.get('release_year'),
        'share_type': context.get('share_type') or 'season_pack',
    }

    # 文件明细可能有更准确的季号/父剧 ID。
    for item in files:
        if not identity_payload.get('parent_series_tmdb_id'):
            identity_payload['parent_series_tmdb_id'] = item.get('parent_series_tmdb_id') or item.get('series_tmdb_id')
        if identity_payload.get('season_number') in (None, ''):
            identity_payload['season_number'] = item.get('season_number')
        if not identity_payload.get('tmdb_id'):
            identity_payload['tmdb_id'] = item.get('tmdb_id')
        if identity_payload.get('parent_series_tmdb_id') and identity_payload.get('season_number') not in (None, ''):
            break

    try:
        identity = _standard_media_identity_for_share(identity_payload)
    except Exception:
        identity = {}

    parent = str(
        identity.get('parent_series_tmdb_id') or
        identity_payload.get('parent_series_tmdb_id') or
        identity.get('tmdb_id') or
        identity_payload.get('tmdb_id') or ''
    ).strip()
    season = identity.get('season_number') if identity.get('season_number') not in (None, '') else identity_payload.get('season_number')

    expected_count = _safe_int(
        context.get('expected_episode_count') or context.get('total_episodes') or context.get('episode_count') or context.get('file_count') or 0,
        0,
    )
    title = identity.get('title') or identity_payload.get('title') or ''
    return {
        'parent_series_tmdb_id': parent,
        'season_number': season,
        'expected_episode_count': expected_count,
        'title': title,
        'identity': identity,
        'identity_payload': identity_payload,
    }

def _validate_season_pack_file_scope(files: List[Dict[str, Any]], ident: Dict[str, Any]) -> Dict[str, Any]:
    """校验季包文件明细确实都属于目标季。

    注意不要相信上游统一 setdefault 的 season_number；优先从 relative_path/file_name
    重新解析 SxxEyy。这样即使误把整剧目录作为 root，也会在创建 115 分享前拦住。
    """
    expected_season = _safe_int((ident or {}).get('season_number'), None)
    if expected_season is None:
        return {'ok': True}

    bad = []
    for item in files or []:
        name = str((item or {}).get('relative_path') or (item or {}).get('file_name') or '')
        parsed_s, parsed_e = _guess_season_episode_numbers(name)
        item_s = _safe_int((item or {}).get('season_number'), None)
        actual_s = parsed_s if parsed_s is not None else item_s
        if actual_s is not None and actual_s != expected_season:
            bad.append({
                'file_name': (item or {}).get('file_name') or name,
                'relative_path': (item or {}).get('relative_path') or '',
                'parsed_season': actual_s,
                'expected_season': expected_season,
            })

    if bad:
        shown = []
        for item in bad[:8]:
            shown.append(f"{item.get('file_name')} => S{_safe_int(item.get('parsed_season'), 0):02d}")
        return {
            'ok': False,
            'reason': 'season_pack_scope_mismatch',
            'message': (
                f"季包文件范围不匹配：目标是 S{expected_season:02d}，但分享目录里混入其它季文件："
                + '；'.join(shown)
                + (f" 等 {len(bad)} 个" if len(bad) > len(shown) else '')
                + '。已阻止创建/登记，避免误分享整剧目录。'
            ),
            'scope_mismatch': bad,
            'source': 'local_file_scope_guard',
            'season_pack_identity': ident,
        }
    return {'ok': True}

def _validate_season_pack_consistency(files: List[Dict[str, Any]], context: Dict[str, Any] = None) -> Dict[str, Any]:
    """季包一致性校验统一入口。

    只调用 tasks.helpers.check_season_consistency，复用追剧完结洗版的统一口径：
    分辨率、制作组、编码必须完全一致。

    父剧 TMDb ID 或季号定位失败时直接判失败，禁止创建/登记季包；
    季包都不知道属于哪部剧哪一季，就没有可靠依据确认“集齐且一致”。
    """
    ident = _season_pack_consistency_identity(files, context)
    parent = str(ident.get('parent_series_tmdb_id') or '').strip()
    season = ident.get('season_number')

    if not parent or season in (None, ''):
        return {
            'ok': False,
            'reason': 'missing_identity',
            'message': '季包一致性校验失败：无法定位父剧 TMDb ID 或季号，禁止创建/登记季包。',
            'source': 'helpers.asset_details_json',
            'season_pack_identity': ident,
        }

    scope_guard = _validate_season_pack_file_scope(files, ident)
    if not scope_guard.get('ok'):
        return scope_guard

    result = helpers.check_season_consistency(
        tmdb_id=parent,
        season_number=season,
        expected_episode_count=ident.get('expected_episode_count') or 0,
        series_name=ident.get('title') or '',
    )
    result = dict(result or {})
    result['source'] = 'helpers.asset_details_json'
    result['season_pack_identity'] = ident
    return result

def _json_array_values(value):
    """media_metadata 的 file_sha1_json / file_pickcode_json 可能是数组、字符串或对象，这里尽量提取稳定标识。"""
    if value is None:
        return []
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return _json_array_values(parsed)
        except Exception:
            return [value] if value.strip() else []
    if isinstance(value, (list, tuple, set)):
        out = []
        for item in value:
            out.extend(_json_array_values(item))
        return [str(x).strip() for x in out if str(x or '').strip()]
    if isinstance(value, dict):
        out = []
        for key in ('pick_code', 'pickcode', 'pc', 'sha1', 'sha', 'file_sha1'):
            if value.get(key):
                out.append(value.get(key))
        if not out:
            # 兼容 {"xxxpc": true} / {"sha1": "name"} 这类历史结构，保守抽 key。
            for k, v in value.items():
                if isinstance(v, (str, int)) and str(v).strip():
                    out.append(v)
                elif isinstance(k, str) and k.strip():
                    out.append(k)
        return [str(x).strip() for x in out if str(x or '').strip()]
    return [str(value).strip()] if str(value or '').strip() else []

def _norm_sha1_list(values):
    out = []
    for v in values or []:
        text = str(v or '').strip().upper()
        if re.fullmatch(r'[A-Fa-f0-9]{40}', text):
            out.append(text)
    return list(dict.fromkeys(out))

def _norm_pc_list(values):
    out = []
    for v in values or []:
        text = str(v or '').strip()
        if text and not re.fullmatch(r'[A-Fa-f0-9]{40}', text):
            out.append(text)
    return list(dict.fromkeys(out))

def _safe_int(value, default=0):
    try:
        if value in (None, ''):
            return default
        return int(float(value))
    except Exception:
        return default

def _safe_float(value, default=0.0):
    try:
        if value in (None, ''):
            return default
        return float(value)
    except Exception:
        return default

# ----------------------------------------------------------------------
# 季包纯净版识别（登记中心前计算并上报中心）
# ----------------------------------------------------------------------
_CLEAN_VERSION_MIN_DELTA_MINUTES = 2.5
_CLEAN_VERSION_MAX_RUNTIME_RATIO = 0.94
_CLEAN_VERSION_MIN_COMPARABLE_EPISODES = 2
_CLEAN_VERSION_HIT_RATIO = 0.70
_CLEAN_TMDB_RUNTIME_CACHE: Dict[str, Dict[str, Any]] = {}
_CLEAN_TMDB_RUNTIME_CACHE_TTL = 6 * 3600

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

def _episode_number_for_clean_detect(item: Dict[str, Any], raw: Dict[str, Any]) -> int | None:
    raw_etk = raw.get('_etk') if isinstance(raw, dict) and isinstance(raw.get('_etk'), dict) else {}
    for value in (item.get('episode_number'), raw_etk.get('episode_number')):
        try:
            if value not in (None, ''):
                ep = int(float(value))
                return ep if ep > 0 else None
        except Exception:
            pass
    text = str(item.get('relative_path') or item.get('file_name') or item.get('name') or '')
    for pat in (r'[Ss]\d{1,3}[. _-]*[Ee](\d{1,4})', r'第\s*(\d{1,4})\s*[集话]', r'\bE(\d{1,4})\b'):
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                ep = int(m.group(1))
                return ep if ep > 0 else None
            except Exception:
                pass
    return None

def _load_local_tmdb_runtime_map_for_clean_detect(parent_series_tmdb_id: str, season_number) -> Dict[int, float]:
    """自动完结季包分享专用：读取智能追剧刚刷新入库的 TMDb episode.runtime。

    只在 raw_json.auto_completed_season_pack 场景使用。这个场景的前置流程刚刚
    调过智能追剧刷新，media_metadata.runtime_minutes 的语义已经被修正为 TMDb
    官方时长，因此可以避免每次自动分享再实时请求 TMDb。手动分享和消费端
    不走这个函数。
    """
    parent_series_tmdb_id = str(parent_series_tmdb_id or '').strip()
    try:
        season = int(float(season_number))
    except Exception:
        return {}
    if not parent_series_tmdb_id:
        return {}

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT episode_number, runtime_minutes
                    FROM media_metadata
                    WHERE parent_series_tmdb_id = %s
                      AND lower(item_type) = 'episode'
                      AND COALESCE(season_number, -1) = COALESCE(%s, -1)
                      AND runtime_minutes IS NOT NULL
                      AND runtime_minutes > 0
                    ORDER BY episode_number ASC
                    """,
                    (parent_series_tmdb_id, season),
                )
                rows = cur.fetchall() or []
    except Exception as e:
        logger.debug(
            f"  ➜ [共享资源] 读取本地刚刷新 TMDb 时长失败: "
            f"parent={parent_series_tmdb_id}, S{season:02d}, err={e}"
        )
        return {}

    result: Dict[int, float] = {}
    for row in rows:
        try:
            ep_no = int(row.get('episode_number'))
            runtime = float(row.get('runtime_minutes') or 0)
            if ep_no > 0 and runtime > 0:
                result[ep_no] = runtime
        except Exception:
            continue
    if result:
        logger.debug(
            f"  ➜ [共享资源] 使用智能追剧刚刷新 TMDb 时长识别纯净版: "
            f"parent={parent_series_tmdb_id}, S{season:02d}, episodes={len(result)}"
        )
    return result


def _load_realtime_tmdb_runtime_map_for_clean_detect(parent_series_tmdb_id: str, season_number) -> Dict[int, float]:
    """手动分享专用：实时读取 TMDb 官方分集时长。

    手动分享没有“刚刷新 TMDb 元数据”的前置保证，不能先信本地库。
    只认 TMDb 当前接口返回的 episode.runtime；TMDb 查不到 runtime 就返回空，
    不用本地历史值冒充官方时长。
    """
    parent_series_tmdb_id = str(parent_series_tmdb_id or '').strip()
    try:
        series_id = int(float(parent_series_tmdb_id))
        season = int(float(season_number))
    except Exception:
        return {}
    if not series_id:
        return {}

    cache_key = f"realtime:{series_id}:{season}"
    now = time.time()
    cached = _CLEAN_TMDB_RUNTIME_CACHE.get(cache_key)
    if cached and now - float(cached.get('ts') or 0) < _CLEAN_TMDB_RUNTIME_CACHE_TTL:
        return dict(cached.get('data') or {})

    api_key = _tmdb_api_key_for_share_request()
    if not api_key:
        logger.debug("  ➜ [共享资源] 未配置 TMDb API Key，无法实时识别季包纯净版。")
        return {}

    try:
        data = tmdb_handler.get_season_details_tmdb(
            tv_id=series_id,
            season_number=season,
            api_key=api_key,
            append_to_response=None,
        )
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 实时查询 TMDb 季时长失败: tv={series_id}, season={season}, err={e}")
        return {}

    result: Dict[int, float] = {}
    for ep in (data or {}).get('episodes') or []:
        if not isinstance(ep, dict):
            continue
        try:
            ep_no = int(ep.get('episode_number'))
            runtime = float(ep.get('runtime') or 0)
            if ep_no > 0 and runtime > 0:
                result[ep_no] = runtime
        except Exception:
            continue

    if result:
        _CLEAN_TMDB_RUNTIME_CACHE[cache_key] = {'ts': now, 'data': dict(result)}
        logger.debug(f"  ➜ [共享资源] 实时 TMDb 时长读取完成: tv={series_id}, S{season:02d}, episodes={len(result)}")
    return result


def _share_record_uses_fresh_local_tmdb_for_clean_detect(record: Dict[str, Any], source_provider: str = '') -> bool:
    """区分自动/手动分享的 TMDb 时长来源。

    - 完结季自动季包：智能追剧刚刷新 TMDb 元数据，可用本地 media_metadata.runtime_minutes。
    - 手动分享：没有刷新前置保证，必须实时查 TMDb。
    - 其它维护/补源/备份场景默认实时查 TMDb，避免误信陈旧本地数据。
    """
    record = record or {}
    raw = record.get('raw_json') if isinstance(record.get('raw_json'), dict) else {}
    provider = str(source_provider or raw.get('source_provider') or record.get('source_provider') or '').strip().lower()

    if raw.get('manual_payload') or raw.get('manual_share') or raw.get('manual_create') or raw.get('manual_created'):
        return False
    if provider in ('user_share', 'manual_share', 'manual', 'local_manual', 'manual_create'):
        return False

    # 这是 watchlist_processor -> trigger_completed_season_pack_share_task 的自动季包链路，
    # 前置已经刷新了该剧/季 TMDb 元数据。
    if raw.get('auto_completed_season_pack'):
        return True
    auto_gap = raw.get('auto_gap') if isinstance(raw.get('auto_gap'), dict) else {}
    if str(auto_gap.get('type') or '').strip() == 'season_completed_rollup':
        return True

    return False


def _load_tmdb_runtime_map_for_clean_detect(parent_series_tmdb_id: str, season_number, *, use_fresh_local_tmdb: bool = False) -> Dict[int, float]:
    if use_fresh_local_tmdb:
        local = _load_local_tmdb_runtime_map_for_clean_detect(parent_series_tmdb_id, season_number)
        if local:
            return local
        # 自动链路理论上一定有本地新鲜 TMDb。这里兜底实时查一次，避免异常情况下漏标。
        logger.debug("  ➜ [共享资源] 自动季包本地 TMDb 时长缺失，退回实时 TMDb 识别纯净版。")
    return _load_realtime_tmdb_runtime_map_for_clean_detect(parent_series_tmdb_id, season_number)

def _detect_clean_version_for_local_season_pack(record: Dict[str, Any], items: List[Dict[str, Any]], *, source_provider: str = '') -> Dict[str, Any]:
    """登记中心前识别季包是否疑似纯净版，并把结果随共享源上报中心。

    自动完结季包使用智能追剧刚刷新到本地的 TMDb 时长；手动分享直接实时查 TMDb。
    """
    if not _is_season_pack_record(record):
        return {'is_clean_version': False, 'reason': 'not_season_pack'}

    ident = _season_pack_consistency_identity(items, record)
    parent = str(ident.get('parent_series_tmdb_id') or '').strip()
    season = ident.get('season_number')
    if not parent or season in (None, ''):
        return {'is_clean_version': False, 'reason': 'missing_identity', 'season_pack_identity': ident}

    use_fresh_local_tmdb = _share_record_uses_fresh_local_tmdb_for_clean_detect(record, source_provider)
    tmdb_runtime_map = _load_tmdb_runtime_map_for_clean_detect(
        parent,
        season,
        use_fresh_local_tmdb=use_fresh_local_tmdb,
    )
    if not tmdb_runtime_map:
        return {'is_clean_version': False, 'reason': 'missing_tmdb_runtime', 'parent_series_tmdb_id': parent, 'season_number': season}

    by_episode: Dict[int, Dict[str, Any]] = {}
    for item in items or []:
        sha1 = str((item or {}).get('sha1') or '').strip().upper()
        if not sha1:
            continue
        raw = _safe_json_obj(P115CacheManager.get_raw_ffprobe_cache(sha1)) or {}
        ep = _episode_number_for_clean_detect(item or {}, raw)
        if ep is None or ep not in tmdb_runtime_map:
            continue
        actual = _physical_runtime_minutes_from_raw(raw)
        tmdb_runtime = float(tmdb_runtime_map.get(ep) or 0)
        if actual <= 0 or tmdb_runtime <= 0:
            continue
        current = by_episode.get(ep)
        if current is None or actual < current.get('actual_runtime_minutes', 0):
            by_episode[ep] = {
                'episode_number': ep,
                'tmdb_runtime_minutes': round(tmdb_runtime, 2),
                'actual_runtime_minutes': round(actual, 2),
                'delta_minutes': round(tmdb_runtime - actual, 2),
                'file_name': (item or {}).get('file_name') or '',
                'sha1': sha1,
            }

    episode_rows = sorted(by_episode.values(), key=lambda x: x.get('episode_number') or 0)
    comparable = len(episode_rows)
    if comparable < _CLEAN_VERSION_MIN_COMPARABLE_EPISODES:
        return {
            'is_clean_version': False,
            'reason': 'not_enough_comparable_episodes',
            'parent_series_tmdb_id': parent,
            'season_number': season,
            'comparable_count': comparable,
        }

    hits = []
    for ep in episode_rows:
        tmdb_runtime = float(ep.get('tmdb_runtime_minutes') or 0)
        actual = float(ep.get('actual_runtime_minutes') or 0)
        delta = float(ep.get('delta_minutes') or 0)
        ratio = (actual / tmdb_runtime) if tmdb_runtime > 0 else 1.0
        ep['runtime_ratio'] = round(ratio, 4)
        ep['clean_hit'] = bool(delta >= _CLEAN_VERSION_MIN_DELTA_MINUTES and ratio <= _CLEAN_VERSION_MAX_RUNTIME_RATIO)
        if ep['clean_hit']:
            hits.append(ep)

    required_hits = max(2, int(comparable * _CLEAN_VERSION_HIT_RATIO + 0.999999))
    avg_delta = sum(float(ep.get('delta_minutes') or 0) for ep in episode_rows) / comparable if comparable else 0
    confidence = round(min(1.0, len(hits) / float(required_hits or 1)), 4)
    is_clean = len(hits) >= required_hits
    return {
        'is_clean_version': bool(is_clean),
        'clean_version_confidence': confidence if is_clean else 0.0,
        'reason': 'majority_runtime_shorter' if is_clean else 'runtime_not_short_enough',
        'parent_series_tmdb_id': parent,
        'season_number': season,
        'comparable_count': comparable,
        'hit_count': len(hits),
        'required_hits': required_hits,
        'avg_delta_minutes': round(avg_delta, 2),
        'min_delta_minutes': _CLEAN_VERSION_MIN_DELTA_MINUTES,
        'max_runtime_ratio': _CLEAN_VERSION_MAX_RUNTIME_RATIO,
        'hit_ratio': _CLEAN_VERSION_HIT_RATIO,
        'algorithm': 'tmdb_vs_physical_runtime_v1',
        'episodes': episode_rows[:80],
    }

def _db_truthy(value) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in ('1', 'true', 'yes', 'y', 'on', 'enabled', '启用', '开启', '是')

def _safe_json_value(value, fallback=None):
    if value is None:
        return fallback
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return fallback
        try:
            return json.loads(text)
        except Exception:
            return fallback
    return fallback

def _asset_detail_items(value) -> List[Dict[str, Any]]:
    parsed = _safe_json_value(value, fallback=[])

    def meaningful_asset(item: Dict[str, Any]) -> bool:
        if not isinstance(item, dict) or not item:
            return False
        # 只把真正的视频资产明细当成可匹配对象。Series/Season 行常见 asset_details_json={}，
        # 如果直接返回 [{}]，全剧/季包只要勾一个参数就必然匹配失败。
        return any(item.get(k) not in (None, '', [], {}) for k in (
            'resolution_display', 'codec_display', 'effect_display', 'frame_rate',
            'audio_display', 'subtitle_display', 'size_bytes', 'video_codec', 'width', 'height'
        ))

    if isinstance(parsed, dict):
        # asset_details_json 历史上可能是单对象，也可能包在 files/items/assets 里。
        for key in ('items', 'files', 'assets', 'asset_details'):
            if isinstance(parsed.get(key), list):
                return [x for x in parsed.get(key) if meaningful_asset(x)]
        return [parsed] if meaningful_asset(parsed) else []
    if isinstance(parsed, list):
        return [x for x in parsed if meaningful_asset(x)]
    return []

def _norm_match_text(value: Any) -> str:
    text = str(value or '').strip().lower()
    if not text:
        return ''
    return re.sub(r'[\s._\-]+', '', text)

def _split_display_values(value: Any) -> List[str]:
    text = str(value or '').strip()
    if not text:
        return []
    parts = re.split(r'[,，/|、]+', text)
    return [p.strip() for p in parts if p.strip()]

def _asset_numeric_frame_rate(asset: Dict[str, Any]) -> float:
    value = asset.get('frame_rate') or asset.get('fps') or asset.get('average_frame_rate')
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or '').strip()
    if not text:
        return 0.0
    if '/' in text:
        try:
            a, b = text.split('/', 1)
            b = float(b)
            return float(a) / b if b else 0.0
        except Exception:
            return 0.0
    m = re.search(r'(\d+(?:\.\d+)?)', text)
    return float(m.group(1)) if m else 0.0

def _parse_request_size_range(text: str):
    text = str(text or '').strip().lower().replace('gb', '').replace('g', '').replace(' ', '')
    if not text:
        return None
    try:
        m = re.match(r'^(\d+(?:\.\d+)?)\s*[-~]\s*(\d+(?:\.\d+)?)$', text)
        if m:
            a, b = float(m.group(1)), float(m.group(2))
            return (min(a, b), max(a, b))
        m = re.match(r'^(?:>=|≥)(\d+(?:\.\d+)?)$', text)
        if m:
            return (float(m.group(1)), None)
        m = re.match(r'^(?:<=|≤)(\d+(?:\.\d+)?)$', text)
        if m:
            return (None, float(m.group(1)))
        m = re.match(r'^(\d+(?:\.\d+)?)\+$', text)
        if m:
            return (float(m.group(1)), None)
    except Exception:
        return None
    return None

def _asset_matches_share_request_params(asset: Dict[str, Any], params: Dict[str, Any]) -> bool:
    asset = asset or {}
    params = params or {}

    resolution = str(params.get('resolution') or '').strip()
    if resolution and _norm_match_text(asset.get('resolution_display') or asset.get('resolution')) != _norm_match_text(resolution):
        return False

    codec = str(params.get('codec') or '').strip()
    if codec and _norm_match_text(asset.get('codec_display') or asset.get('video_codec')) != _norm_match_text(codec):
        return False

    effect = str(params.get('effect') or '').strip()
    if effect and _norm_match_text(asset.get('effect_display') or asset.get('effect')) != _norm_match_text(effect):
        return False

    frame_rate = str(params.get('frame_rate') or '').strip()
    if frame_rate:
        fps = _asset_numeric_frame_rate(asset)
        target = _safe_float(frame_rate, 0.0)
        if target >= 30:
            if fps + 0.01 < target:
                return False
        elif target > 0:
            # 24fps 这类偏“标准帧率”的选项按近似匹配，兼容 23.976/24.000。
            if not (target - 1.0 <= fps <= target + 1.5):
                return False

    audio = str(params.get('audio') or '').strip()
    if audio:
        values = _split_display_values(asset.get('audio_display'))
        if _norm_match_text(audio) not in {_norm_match_text(v) for v in values}:
            return False

    subtitle = str(params.get('subtitle') or '').strip()
    if subtitle:
        sub_display = str(asset.get('subtitle_display') or '').strip()
        values = _split_display_values(sub_display)
        if subtitle == '无':
            if sub_display and _norm_match_text(sub_display) not in {'无', 'none', 'no'}:
                return False
        elif _norm_match_text(subtitle) not in {_norm_match_text(v) for v in values}:
            return False

    size_range = str(params.get('size_range') or '').strip()
    if size_range:
        bounds = _parse_request_size_range(size_range)
        if bounds:
            size_bytes = _safe_float(asset.get('size_bytes') or asset.get('size') or 0, 0.0)
            size_gb = size_bytes / (1024 ** 3) if size_bytes else 0.0
            low, high = bounds
            if low is not None and size_gb + 1e-6 < low:
                return False
            if high is not None and size_gb - 1e-6 > high:
                return False

    return True

def _load_share_request_candidate_assets(candidate: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidate = candidate or {}
    share_item_type = str(candidate.get('share_item_type') or candidate.get('item_type') or '').strip()
    share_type = str(candidate.get('share_type') or '').strip().lower()

    if share_item_type not in {'Series', 'Season'} and share_type not in ('series_pack', 'tv_pack', 'season_pack'):
        inline = _asset_detail_items(candidate.get('asset_details_json'))
        if inline:
            return inline

    tmdb_id = str(candidate.get('share_tmdb_id') or candidate.get('tmdb_id') or '').strip()
    parent_series_id = str(candidate.get('parent_series_tmdb_id') or '').strip()
    season_number = candidate.get('season_number')
    episode_number = candidate.get('episode_number')

    try:
        rows = shared_share_db.get_asset_details_for_candidate(share_item_type, tmdb_id, parent_series_id, season_number, episode_number, share_type)
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 读取候选 asset_details_json 失败: {candidate.get('display_title') or candidate.get('title')} -> {e}")
        return []

    assets: List[Dict[str, Any]] = []
    for row in rows or []:
        assets.extend(_asset_detail_items(row.get('asset_details_json')))
    return assets

def _share_request_filter_from_args(args) -> Dict[str, Any]:
    args = args or {}
    params = _safe_json_value(args.get('request_params_json'), fallback={})
    if not isinstance(params, dict):
        params = {}
    episode_numbers = _safe_json_value(args.get('request_episode_numbers_json'), fallback=[])
    if not isinstance(episode_numbers, list):
        episode_numbers = []
    return {
        'tmdb_id': str(args.get('request_tmdb_id') or '').strip(),
        'media_type': str(args.get('request_media_type') or '').strip().lower(),
        'target_type': str(args.get('request_target_type') or '').strip().lower(),
        'season_number': _safe_int(args.get('request_season_number'), None),
        'season_count': _safe_int(args.get('request_season_count'), None),
        'episode_number': _safe_int(args.get('request_episode_number'), None),
        'episode_numbers': [_safe_int(x, None) for x in episode_numbers if _safe_int(x, None) is not None],
        'params': params,
    }

def _candidate_matches_share_request_target(candidate: Dict[str, Any], request_filter: Dict[str, Any]) -> bool:
    request_tmdb_id = str((request_filter or {}).get('tmdb_id') or '').strip()
    if not request_tmdb_id:
        return True
    target_type = str((request_filter or {}).get('target_type') or '').strip().lower()
    media_type = str((request_filter or {}).get('media_type') or '').strip().lower()

    cand_type = str(candidate.get('share_item_type') or candidate.get('item_type') or '').strip()
    cand_tmdb = str(candidate.get('share_tmdb_id') or candidate.get('tmdb_id') or '').strip()
    cand_parent = str(candidate.get('parent_series_tmdb_id') or cand_tmdb).strip()

    if media_type == 'movie' or target_type == 'movie':
        return cand_type == 'Movie' and cand_tmdb == request_tmdb_id

    if cand_parent != request_tmdb_id and cand_tmdb != request_tmdb_id:
        return False

    req_season = request_filter.get('season_number')
    req_episode = request_filter.get('episode_number')
    req_episodes = set(request_filter.get('episode_numbers') or [])
    cand_season = _safe_int(candidate.get('season_number'), None)
    cand_episode = _safe_int(candidate.get('episode_number'), None)

    if target_type in ('series', 'tv'):
        return True
    if target_type == 'season':
        return req_season is None or cand_season == req_season
    if target_type == 'episode':
        # 求的是单集，就只能展示/创建单集分享。
        # 115 本身没有“按分享包指定只分享某一集”的能力，不能拿整季包冒充单集响应，
        # 否则前端会创建 season_pack，中心登记也会变成剧集包。
        if cand_type != 'Episode' or str(candidate.get('share_type') or '').lower() == 'season_pack':
            return False
        if req_season is not None and cand_season != req_season:
            return False
        return req_episode is None or cand_episode == req_episode
    if target_type == 'episode_batch':
        # 历史兼容：曾经的集数范围现在按单季季包处理。
        return req_season is None or cand_season == req_season
    return True

def _candidate_size_matches_share_request(assets: List[Dict[str, Any]], size_range: str, aggregate: bool = False) -> bool:
    size_range = str(size_range or '').strip()
    if not size_range:
        return True
    bounds = _parse_request_size_range(size_range)
    if not bounds:
        return True
    low, high = bounds
    if aggregate:
        size_bytes = sum(_safe_float((asset or {}).get('size_bytes') or (asset or {}).get('size') or 0, 0.0) for asset in assets or [])
        size_gb = size_bytes / (1024 ** 3) if size_bytes else 0.0
        if low is not None and size_gb + 1e-6 < low:
            return False
        if high is not None and size_gb - 1e-6 > high:
            return False
        return True
    return all(_asset_matches_share_request_params(asset, {'size_range': size_range}) for asset in assets or [])

def _candidate_matches_share_request_params(candidate: Dict[str, Any], params: Dict[str, Any]) -> bool:
    params = {k: v for k, v in (params or {}).items() if str(v or '').strip()}
    if not params:
        return True
    assets = _load_share_request_candidate_assets(candidate)
    if not assets:
        return False
    share_type = str(candidate.get('share_type') or '').strip().lower()
    share_item_type = str(candidate.get('share_item_type') or candidate.get('item_type') or '').strip()
    strict_all = share_item_type in {'Season', 'Series'} or share_type in ('season_pack', 'series_pack', 'tv_pack') or bool(candidate.get('root_is_dir') and _safe_int(candidate.get('file_count'), 1) > 1)

    # 多文件包的体积范围按整包总大小匹配；其它画质/编码/HDR/帧率/音轨/字幕仍要求包内每个视频都满足。
    # 否则“全剧 50-100GB”会被误解成每一集都必须 50-100GB。
    params_without_size = {k: v for k, v in params.items() if k != 'size_range'}
    size_range = params.get('size_range')
    if strict_all:
        if params_without_size and not all(_asset_matches_share_request_params(asset, params_without_size) for asset in assets):
            return False
        return _candidate_size_matches_share_request(assets, size_range, aggregate=len(assets) > 1)

    if params_without_size and not any(_asset_matches_share_request_params(asset, params_without_size) for asset in assets):
        return False
    return _candidate_size_matches_share_request(assets, size_range, aggregate=False)

def _load_series_row_for_share_request(request_filter: Dict[str, Any], fallback_row: Dict[str, Any] = None) -> Dict[str, Any]:
    request_filter = request_filter or {}
    fallback_row = dict(fallback_row or {})
    series_tmdb_id = str(
        request_filter.get('tmdb_id') or
        fallback_row.get('parent_series_tmdb_id') or
        fallback_row.get('tmdb_id') or ''
    ).strip()
    if not series_tmdb_id:
        return {}
    try:
        row = shared_share_db.get_series_row_for_share_request(series_tmdb_id)
        if row: return row
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 求分享全剧候选定位 Series 行失败: series={series_tmdb_id}, err={e}")

    identity = _get_series_identity(series_tmdb_id) or {}
    return {
        'tmdb_id': series_tmdb_id,
        'item_type': 'Series',
        'title': identity.get('title') or fallback_row.get('series_title') or fallback_row.get('title') or '',
        'original_title': fallback_row.get('original_title') or '',
        'parent_series_tmdb_id': '',
        'season_number': None,
        'episode_number': None,
        'release_year': identity.get('release_year') or fallback_row.get('release_year'),
        'release_date': fallback_row.get('release_date'),
        'last_air_date': fallback_row.get('last_air_date'),
        'in_library': True,
        'subscription_status': fallback_row.get('subscription_status'),
        'total_episodes': fallback_row.get('total_episodes'),
        'watching_status': fallback_row.get('watching_status'),
        'watchlist_tmdb_status': fallback_row.get('watchlist_tmdb_status'),
    }

def _load_real_completed_season_info_for_share_request(series_id: str) -> Dict[str, Any]:
    series_id = str(series_id or '').strip()
    if not series_id:
        return {'real_completed': [], 'force_ended': [], 'completed_not_in_library': [], 'empty_shell': [], 'rows': []}
    try:
        rows = shared_share_db.get_real_completed_season_info(series_id)
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 读取全剧真实完结季状态失败: series={series_id}, err={e}")
        return {'real_completed': [], 'force_ended': [], 'completed_not_in_library': [], 'empty_shell': [], 'rows': [], 'error': str(e)}

    real_completed, force_ended, completed_not_in_library, empty_shell = [], [], [], []
    for row in rows:
        sn = _safe_int(row.get('season_number'), None)
        if sn is None or sn <= 0: continue
        total_episodes = _safe_int(row.get('total_episodes'), None)
        if total_episodes is not None and total_episodes <= 0:
            empty_shell.append(sn)
            continue
        completed = str(row.get('watching_status') or '').strip().lower() == 'completed'
        forced = _db_truthy(row.get('force_ended'))
        if completed and forced:
            force_ended.append(sn)
        elif completed and not forced:
            if row.get('in_library'): real_completed.append(sn)
            else: completed_not_in_library.append(sn)
    return {
        'real_completed': sorted(set(real_completed)),
        'force_ended': sorted(set(force_ended)),
        'completed_not_in_library': sorted(set(completed_not_in_library)),
        'empty_shell': sorted(set(empty_shell)),
        'rows': rows,
    }

def _resolve_series_pack_root_for_share_request(series_row: Dict[str, Any], request_filter: Dict[str, Any] = None) -> Dict[str, Any]:
    """求分享响应专用：全剧请求允许按父剧目录创建整剧包分享。"""
    series_row = dict(series_row or {})
    series_id = str(series_row.get('tmdb_id') or '').strip()
    if not series_id:
        return {
            'resolvable': False,
            'root_fid': '',
            'root_name': '',
            'root_is_dir': True,
            'file_count': 0,
            'matched_pickcodes': 0,
            'matched_sha1s': 0,
            'share_type': 'series_pack',
            'share_item_type': 'Series',
            'message': '缺少父剧 TMDb ID，无法定位全剧目录',
        }

    request_filter = request_filter or {}
    expected_season_count = _safe_int(request_filter.get('season_count'), 0)
    season_info = _load_real_completed_season_info_for_share_request(series_id)
    real_completed_seasons = season_info.get('real_completed') or []
    force_ended_seasons = season_info.get('force_ended') or []
    empty_shell_seasons = season_info.get('empty_shell') or []
    ignored_empty_shell_note = ''
    if expected_season_count > 0 and len(real_completed_seasons) < expected_season_count:
        # 兼容历史求分享：旧版全剧计价可能把 TMDb/本地 0 集空壳季也算进 season_count。
        # 只要缺口完全由 0 集空壳季造成，就自动忽略这些季，避免别人永远无法响应。
        if empty_shell_seasons and len(real_completed_seasons) + len(empty_shell_seasons) >= expected_season_count:
            ignored_empty_shell_note = '已忽略 0 集空壳季：' + ','.join(f'S{int(x):02d}' for x in empty_shell_seasons[:20])
            expected_season_count = len(real_completed_seasons)
        else:
            reason_parts = [f"TMDb 正季 {expected_season_count} 季，本地真实完结季 {len(real_completed_seasons)} 季"]
            if empty_shell_seasons:
                reason_parts.append('0 集空壳季不计入可分享全剧：' + ','.join(f'S{int(x):02d}' for x in empty_shell_seasons[:20]))
            if force_ended_seasons:
                reason_parts.append('强制完结季不计入真实完结：' + ','.join(f'S{int(x):02d}' for x in force_ended_seasons[:20]))
            return {
                'resolvable': False,
                'root_fid': '',
                'root_name': '',
                'root_is_dir': True,
                'file_count': 0,
                'matched_pickcodes': 0,
                'matched_sha1s': 0,
                'share_type': 'series_pack',
                'share_item_type': 'Series',
                'message': '全剧求分享季数校验失败：' + '；'.join(reason_parts),
                'season_check': season_info,
            }
    if expected_season_count > 0:
        selected_seasons = real_completed_seasons[:expected_season_count]
    else:
        selected_seasons = real_completed_seasons
    ids = _collect_media_identifiers({
        **series_row,
        'item_type': 'Series',
        'tmdb_id': series_id,
        'season_numbers_filter': selected_seasons,
        'positive_seasons_only': True,
    })
    file_rows = _get_p115_file_rows(ids.get('pickcodes') or [], ids.get('sha1s') or [])
    if not file_rows:
        return {
            'resolvable': False,
            'root_fid': '',
            'root_name': '',
            'root_is_dir': True,
            'file_count': 0,
            'matched_pickcodes': len(ids.get('pickcodes') or []),
            'matched_sha1s': len(ids.get('sha1s') or []),
            'share_type': 'series_pack',
            'share_item_type': 'Series',
            'message': '该剧在 media_metadata 中有记录，但没有通过 PC/SHA1 在 p115_filesystem_cache 反查到全剧文件',
        }

    parent_ids = [str(r.get('parent_id') or '') for r in file_rows if r.get('parent_id')]
    root_id = ''
    root_name = ''
    messages = []
    if parent_ids:
        chains = [_ancestor_chain(pid) for pid in parent_ids]
        common = []
        if chains:
            for node_id in chains[0]:
                if all(node_id in ch for ch in chains[1:]):
                    common.append(node_id)
        root_id = common[0] if common else parent_ids[0]
        root_node = _get_p115_node(root_id) or {}
        root_name = root_node.get('name') or root_id
        season_numbers = sorted({int(r.get('season_number')) for r in (ids.get('episode_rows') or []) if r.get('season_number') not in (None, '') and int(r.get('season_number') or 0) > 0})
        if season_numbers:
            expected_note = f"/TMDb {expected_season_count} 季" if expected_season_count else ''
            messages.append(f"已定位全剧 {len(season_numbers)} 季{expected_note}、{len(file_rows)} 个视频文件")
        else:
            messages.append(f"已定位全剧 {len(file_rows)} 个视频文件")
        if ignored_empty_shell_note:
            messages.append(ignored_empty_shell_note)
        if len(set(parent_ids)) > 1:
            messages.append(f"文件分布在 {len(set(parent_ids))} 个目录，已自动选择共同上级目录：{root_name}")
    else:
        root_id = str(file_rows[0].get('id') or '')
        root_name = file_rows[0].get('name') or root_id

    if not root_id:
        return {
            'resolvable': False,
            'root_fid': '',
            'root_name': '',
            'root_is_dir': True,
            'file_count': len(file_rows),
            'matched_pickcodes': len(ids.get('pickcodes') or []),
            'matched_sha1s': len(ids.get('sha1s') or []),
            'share_type': 'series_pack',
            'share_item_type': 'Series',
            'message': '已找到全剧文件，但无法定位可分享的 115 FID/CID',
        }

    return {
        'resolvable': True,
        'root_fid': root_id,
        'root_name': root_name,
        'root_is_dir': True,
        'file_count': len(file_rows),
        'matched_pickcodes': len(ids.get('pickcodes') or []),
        'matched_sha1s': len(ids.get('sha1s') or []),
        'share_type': 'series_pack',
        'share_item_type': 'Series',
        'message': '；'.join(messages) if messages else '已通过 PC/SHA1 定位到可分享的全剧目录',
        'season_check': season_info,
    }

def _build_series_pack_candidate_for_share_request(series_row: Dict[str, Any], request_filter: Dict[str, Any] = None) -> Dict[str, Any]:
    """构造“全剧求分享”的整剧包候选，不影响普通手动分享的季包优先策略。"""
    row = dict(series_row or {})
    series_id = str(row.get('tmdb_id') or '').strip()
    identity = _get_series_identity(series_id) if series_id else {}
    title = identity.get('title') or row.get('title') or row.get('original_title') or series_id
    year = identity.get('release_year') or _parse_release_year(row)
    resolved = _resolve_series_pack_root_for_share_request({**row, 'tmdb_id': series_id, 'item_type': 'Series'}, request_filter=request_filter)
    return {
        **row,
        'item_type': 'Series',
        'display_title': f"{title} 全剧" if title else '全剧',
        'series_title': title,
        'standard_title': title,
        'release_year': year,
        'parent_series_tmdb_id': series_id,
        'share_tmdb_id': series_id,
        'share_item_type': 'Series',
        **resolved,
    }

def _load_exact_episode_row_for_share_request(request_filter: Dict[str, Any]) -> Dict[str, Any]:
    request_filter = request_filter or {}
    if str(request_filter.get('target_type') or '').strip().lower() != 'episode': return {}
    parent_tmdb_id = str(request_filter.get('tmdb_id') or '').strip()
    season_number = _safe_int(request_filter.get('season_number'), None)
    episode_number = _safe_int(request_filter.get('episode_number'), None)
    if not parent_tmdb_id or season_number is None or episode_number is None: return {}
    try:
        return shared_share_db.get_exact_episode_row_for_share_request(parent_tmdb_id, season_number, episode_number) or {}
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 求分享单集候选定位失败: series={parent_tmdb_id}, S{season_number}E{episode_number}, err={e}")
        return {}

def _expand_share_candidates_for_share_request(row: Dict[str, Any], request_filter: Dict[str, Any]) -> List[Dict[str, Any]]:
    """求分享响应专用展开。

    普通“手动分享”搜索为了少展示碎片，会把已完结季的单集提升为季包；
    但求分享如果目标是单集，就必须创建单集分享，不能展示季包候选。
    """
    request_filter = request_filter or {}
    target_type = str(request_filter.get('target_type') or '').strip().lower()
    if target_type in ('series', 'tv'):
        series_row = _load_series_row_for_share_request(request_filter, row)
        return [_build_series_pack_candidate_for_share_request(series_row, request_filter=request_filter)] if series_row else []
    if target_type == 'episode':
        ep_row = _load_exact_episode_row_for_share_request(request_filter)
        return [_build_media_candidate(ep_row)] if ep_row else []
    return _expand_share_candidates(row)

def _filter_candidates_for_share_request(candidates: List[Dict[str, Any]], request_filter: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not request_filter or not request_filter.get('tmdb_id'):
        return candidates or []
    out = []
    for cand in candidates or []:
        if not _candidate_matches_share_request_target(cand, request_filter):
            continue
        if not _candidate_matches_share_request_params(cand, request_filter.get('params') or {}):
            continue
        out.append(cand)
    return out

def _boolish(value, default=False):
    """兼容前端传来的 bool / 0/1 / true/false / yes/no。

    这里不能直接 bool("false")，否则字符串 false 会被误判为 True。
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in ('1', 'true', 'yes', 'y', 'on', 'dir', 'folder', 'directory'):
        return True
    if text in ('0', 'false', 'no', 'n', 'off', 'file'):
        return False
    return default

def _parse_release_year(row: Dict[str, Any]):
    if row.get('release_year'):
        return row.get('release_year')
    for key in ('release_date', 'last_air_date', 'date_added'):
        val = row.get(key)
        if val:
            m = re.search(r'((?:19|20)\d{2})', str(val))
            if m:
                try:
                    return int(m.group(1))
                except Exception:
                    pass
    return None

def _get_media_row(tmdb_id: str, item_type: str):
    return shared_share_db.get_media_metadata_row(tmdb_id, item_type)

def _media_title_value(row: Dict[str, Any]) -> str:
    """只取 media_metadata.title 作为标准片名；没有时才兜底 original_title。"""
    row = row or {}
    return str(row.get('title') or row.get('original_title') or '').strip()

def _media_release_year_value(row: Dict[str, Any]):
    row = row or {}
    return _parse_release_year(row)

def _get_series_identity(series_tmdb_id: str) -> Dict[str, Any]:
    row = shared_share_db.get_series_identity(series_tmdb_id)
    if not row: return {}
    return {
        'tmdb_id': str(row.get('tmdb_id') or series_tmdb_id),
        'item_type': 'Series',
        'title': _media_title_value(row),
        'release_year': _media_release_year_value(row),
        'raw_row': row,
    }

def _get_media_row_loose(tmdb_id: str, item_type: str = ''):
    return shared_share_db.get_media_metadata_row_loose(tmdb_id, item_type)

def _standard_media_identity_for_share(data: Dict[str, Any], item: Dict[str, Any] = None) -> Dict[str, Any]:
    """返回共享展示/登记使用的标准媒体身份。

    规则：
    - Movie 用电影自身 media_metadata.title；
    - Series/Season/Episode 统一用父剧 Series 行的 media_metadata.title；
    - release_year 同步取对应标准条目的年份；
    - 查不到数据库时才兜底调用方传入标题，绝不从 115 文件名反推片名。
    """
    data = dict(data or {})
    item = dict(item or {})

    item_type = str(
        data.get('item_type') or item.get('item_type') or data.get('share_item_type') or item.get('share_item_type') or ''
    ).strip()
    share_type = str(data.get('share_type') or item.get('share_type') or '').strip().lower()
    if share_type in ('season_pack', 'series_pack', 'tv_pack'):
        item_type = 'Season'
    elif share_type in ('movie_file', 'movie_folder') and not item_type:
        item_type = 'Movie'

    tmdb_id = str(data.get('tmdb_id') or item.get('tmdb_id') or data.get('share_tmdb_id') or item.get('share_tmdb_id') or '').strip()
    parent_series_id = str(
        data.get('parent_series_tmdb_id') or item.get('parent_series_tmdb_id') or
        data.get('series_tmdb_id') or item.get('series_tmdb_id') or ''
    ).strip()
    season_number = data.get('season_number', item.get('season_number'))
    episode_number = data.get('episode_number', item.get('episode_number'))

    row = _get_media_row_loose(tmdb_id, item_type) if tmdb_id else None

    # 季/集分享登记使用父剧 TMDb ID 和父剧标准片名。
    if item_type in ('Series', 'Season', 'Episode') or share_type in ('season_pack', 'series_pack', 'episode_file', 'tv_pack'):
        if not parent_series_id and row:
            parent_series_id = str(row.get('parent_series_tmdb_id') or '').strip()
            if not parent_series_id and row.get('item_type') == 'Series':
                parent_series_id = str(row.get('tmdb_id') or '').strip()

        # 中心缺口/分享记录对 Season/Episode 常直接传父剧 tmdb_id。
        if not parent_series_id and tmdb_id:
            series_identity = _get_series_identity(tmdb_id)
            if series_identity:
                parent_series_id = str(series_identity.get('tmdb_id') or tmdb_id)

        # 仍未拿到父剧时，用 season/episode 条件从 media_metadata 反查。
        if not parent_series_id and tmdb_id:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    if item_type == 'Season' and season_number not in (None, ''):
                        cur.execute("""
                            SELECT parent_series_tmdb_id
                            FROM media_metadata
                            WHERE item_type='Season' AND (tmdb_id=%s OR parent_series_tmdb_id=%s) AND season_number=%s
                            LIMIT 1
                        """, (tmdb_id, tmdb_id, int(season_number)))
                    elif item_type == 'Episode' and season_number not in (None, '') and episode_number not in (None, ''):
                        cur.execute("""
                            SELECT parent_series_tmdb_id
                            FROM media_metadata
                            WHERE item_type='Episode' AND (tmdb_id=%s OR parent_series_tmdb_id=%s)
                              AND season_number=%s AND episode_number=%s
                            LIMIT 1
                        """, (tmdb_id, tmdb_id, int(season_number), int(episode_number)))
                    else:
                        cur.execute("""
                            SELECT parent_series_tmdb_id
                            FROM media_metadata
                            WHERE tmdb_id=%s AND parent_series_tmdb_id IS NOT NULL
                            LIMIT 1
                        """, (tmdb_id,))
                    parent_row = cur.fetchone()
                    if parent_row:
                        parent_series_id = str(dict(parent_row).get('parent_series_tmdb_id') or '').strip()

        series_identity = _get_series_identity(parent_series_id or tmdb_id)
        if series_identity:
            return {
                'tmdb_id': str(series_identity.get('tmdb_id') or parent_series_id or tmdb_id),
                'item_type': item_type or 'Series',
                'parent_series_tmdb_id': str(series_identity.get('tmdb_id') or parent_series_id or tmdb_id),
                'title': series_identity.get('title') or '',
                'release_year': series_identity.get('release_year'),
                'season_number': season_number,
                'episode_number': episode_number,
                'source': 'media_metadata.series',
            }

        # 找不到父剧行时，最多兜底当前 media_metadata 行 title。
        if row:
            return {
                'tmdb_id': parent_series_id or tmdb_id,
                'item_type': item_type or row.get('item_type'),
                'parent_series_tmdb_id': parent_series_id or row.get('parent_series_tmdb_id') or '',
                'title': _media_title_value(row),
                'release_year': _media_release_year_value(row),
                'season_number': season_number or row.get('season_number'),
                'episode_number': episode_number or row.get('episode_number'),
                'source': 'media_metadata.fallback_row',
            }

    if item_type == 'Movie' and tmdb_id:
        movie_row = row if row and row.get('item_type') == 'Movie' else _get_media_row_loose(tmdb_id, 'Movie')
        if movie_row:
            return {
                'tmdb_id': str(movie_row.get('tmdb_id') or tmdb_id),
                'item_type': 'Movie',
                'parent_series_tmdb_id': '',
                'title': _media_title_value(movie_row),
                'release_year': _media_release_year_value(movie_row),
                'season_number': None,
                'episode_number': None,
                'source': 'media_metadata.movie',
            }

    return {
        'tmdb_id': tmdb_id,
        'item_type': item_type,
        'parent_series_tmdb_id': parent_series_id,
        'title': str(data.get('title') or item.get('title') or '').strip(),
        'release_year': data.get('release_year') or item.get('release_year'),
        'season_number': season_number,
        'episode_number': episode_number,
        'source': 'payload_fallback',
    }

def _standard_share_identity(record: Dict[str, Any], item: Dict[str, Any] = None, center_item_type: str = '') -> Dict[str, Any]:
    """按本地分享记录 + 文件明细解析中心登记用标准标题。"""
    record = dict(record or {})
    item = dict(item or {})
    payload = dict(record)
    if item:
        # item 的 season/episode/sha1 可补充，但 title 仍以 record/media_metadata 为准。
        for key in ('tmdb_id', 'item_type', 'season_number', 'episode_number', 'parent_series_tmdb_id'):
            if item.get(key) not in (None, ''):
                payload[key] = item.get(key)
    if center_item_type:
        payload['item_type'] = center_item_type
    if str(record.get('share_type') or '').lower() in ('season_pack', 'series_pack', 'tv_pack'):
        payload['item_type'] = 'Season'
    identity = _standard_media_identity_for_share(payload)
    if not identity.get('title'):
        identity['title'] = str(record.get('title') or '').strip()
    if not identity.get('release_year'):
        identity['release_year'] = record.get('release_year')
    return identity

def _collect_media_identifiers(row: Dict[str, Any]) -> Dict[str, List[str]]:
    if not row: return {'pickcodes': [], 'sha1s': [], 'episode_rows': []}
    rows = [row]
    item_type = row.get('item_type')
    series_id = row.get('parent_series_tmdb_id') or row.get('tmdb_id')
    season_number = row.get('season_number')

    if item_type == 'Season' and series_id and season_number is not None:
        episode_rows = shared_share_db.get_episode_rows_by_season(series_id, season_number)
        if episode_rows: rows = episode_rows
    elif item_type == 'Series':
        season_filter = []
        for value in (row.get('season_numbers_filter') or []):
            sn = _safe_int(value, None)
            if sn is not None and sn not in season_filter: season_filter.append(sn)
        episode_rows = shared_share_db.get_episode_rows_by_series_filter(series_id, season_filter, row.get('positive_seasons_only'))
        if episode_rows: rows = episode_rows

    pickcodes, sha1s = [], []
    for r in rows:
        pickcodes.extend(_json_array_values(r.get('file_pickcode_json')))
        sha1s.extend(_json_array_values(r.get('file_sha1_json')))

    return {
        'pickcodes': _norm_pc_list(pickcodes),
        'sha1s': _norm_sha1_list(sha1s),
        'episode_rows': rows if rows != [row] else [],
    }

def _get_p115_file_rows(pickcodes: List[str], sha1s: List[str]) -> List[Dict[str, Any]]:
    rows = shared_share_db.get_p115_file_rows_by_pc_sha1(pickcodes, sha1s)
    seen, out = set(), []
    for r in rows:
        key = str(r.get('id') or '')
        if key and key not in seen:
            seen.add(key)
            out.append(r)
    return out

def _get_p115_node(node_id: str):
    return shared_share_db.get_p115_node_by_id(node_id)

def _ancestor_chain(parent_id: str, max_depth: int = 20) -> List[str]:
    chain = []
    curr = str(parent_id or '')
    for _ in range(max_depth):
        if not curr or curr in chain:
            break
        chain.append(curr)
        if curr == '0':
            break
        node = _get_p115_node(curr)
        if not node:
            break
        curr = str(node.get('parent_id') or '')
    return chain

def _season_dir_name_matches(name: str, season_number) -> bool:
    season = _safe_int(season_number, None)
    if season is None:
        return False
    text = str(name or '').strip().lower()
    if not text:
        return False
    names = {
        f"season {season}".lower(),
        f"season {season:02d}".lower(),
        f"s{season}".lower(),
        f"s{season:02d}".lower(),
        f"第{season}季".lower(),
    }
    return text in names

def _narrow_season_pack_root(root_id: str, root_name: str, parent_ids: List[str], season_number) -> Dict[str, Any]:
    """季包分享根目录二次收窄，避免 common ancestor 退到整剧目录。"""
    root_id = str(root_id or '').strip()
    parent_ids = [str(x or '').strip() for x in (parent_ids or []) if str(x or '').strip()]
    if not root_id or not parent_ids:
        return {'root_id': root_id, 'root_name': root_name, 'ok': True, 'message': ''}

    if _season_dir_name_matches(root_name, season_number):
        return {'root_id': root_id, 'root_name': root_name, 'ok': True, 'message': ''}

    direct_children = set()
    for pid in parent_ids:
        chain = _ancestor_chain(pid)
        if root_id not in chain:
            continue
        idx = chain.index(root_id)
        child_id = chain[idx - 1] if idx > 0 else root_id
        if child_id:
            direct_children.add(child_id)

    if len(direct_children) == 1:
        child_id = next(iter(direct_children))
        if child_id != root_id:
            child_node = _get_p115_node(child_id) or {}
            child_name = child_node.get('name') or child_id
            if _season_dir_name_matches(child_name, season_number):
                return {
                    'root_id': child_id,
                    'root_name': child_name,
                    'ok': True,
                    'message': f'季包根目录已从上级目录收窄到 {child_name}，避免误分享整剧目录',
                }

    if len(direct_children) > 1:
        return {
            'root_id': root_id,
            'root_name': root_name,
            'ok': False,
            'message': '本季文件跨多个直属目录，无法安全定位单季目录，禁止按上级目录创建季包分享',
        }

    return {'root_id': root_id, 'root_name': root_name, 'ok': True, 'message': ''}

def _episode_label_from_row(row: Dict[str, Any], series_title: str = '') -> str:
    s = row.get('season_number')
    e = row.get('episode_number')
    base = series_title or row.get('title') or row.get('original_title') or row.get('tmdb_id') or ''
    try:
        if s is not None and e is not None:
            return f"{base} S{int(s):02d}E{int(e):02d}"
    except Exception:
        pass
    return str(base or row.get('file_name') or 'Episode')

def _get_episode_rows_for_media(row: Dict[str, Any], only_with_files: bool = False, season_number=None) -> List[Dict[str, Any]]:
    row = row or {}
    item_type = row.get('item_type')
    if item_type == 'Episode':
        rows = [dict(row)]
    else:
        parent_series_id = row.get('parent_series_tmdb_id') or row.get('tmdb_id')
        target_season = season_number if season_number not in (None, '') else row.get('season_number')
        if item_type == 'Season' and not parent_series_id:
            parent_series_id = str(row.get('tmdb_id') or '').split('_')[0] if '_' in str(row.get('tmdb_id') or '') else ''
        if not parent_series_id: return []
        
        # 复用下沉的 DB 方法
        if target_season not in (None, ''):
            rows = shared_share_db.get_episode_rows_by_season(parent_series_id, target_season)
        else:
            rows = shared_share_db.get_episode_rows_by_series_filter(parent_series_id, [], False)

    if not only_with_files: return rows
    out = []
    for r in rows:
        if not r.get('in_library'): continue
        if _norm_pc_list(_json_array_values(r.get('file_pickcode_json'))) or _norm_sha1_list(_json_array_values(r.get('file_sha1_json'))):
            out.append(r)
    return out

def _season_completion_info(row: Dict[str, Any]) -> Dict[str, Any]:
    row = row or {}
    parent_series_id = row.get('parent_series_tmdb_id') or row.get('tmdb_id')
    if row.get('item_type') == 'Episode':
        parent_series_id = row.get('parent_series_tmdb_id')
    season_number = row.get('season_number')
    try: season_number = int(season_number) if season_number not in (None, '') else None
    except Exception: season_number = None

    expected = _safe_int(row.get('total_episodes'), 0)
    season_title = ''
    watching_status = ''
    force_ended = _db_truthy(row.get('force_ended'))

    if parent_series_id and season_number is not None:
        season_row = shared_share_db.get_season_completion_status(parent_series_id, season_number)
        if season_row:
            expected = _safe_int(season_row.get('total_episodes'), expected)
            season_title = season_row.get('title') or ''
            watching_status = str(season_row.get('watching_status') or '').strip()
            force_ended = _db_truthy(season_row.get('force_ended'))
        else:
            watching_status = str(row.get('watching_status') or '').strip()
    else:
        watching_status = str(row.get('watching_status') or '').strip()

    episode_rows = _get_episode_rows_for_media(
        {**row, 'parent_series_tmdb_id': parent_series_id, 'season_number': season_number, 'item_type': 'Season'},
        only_with_files=False,
    )
    local_rows = []
    for ep in episode_rows:
        if ep.get('in_library') and (_norm_pc_list(_json_array_values(ep.get('file_pickcode_json'))) or _norm_sha1_list(_json_array_values(ep.get('file_sha1_json')))):
            local_rows.append(ep)

    known_count = len(episode_rows)
    local_count = len(local_rows)
    season_completed = watching_status.lower() == 'completed' and not force_ended

    expected_source = 'season.status'
    if not expected and known_count > 0:
        expected = known_count
        expected_source = 'known_episode_rows'

    complete = bool(season_completed and local_count > 0)

    if force_ended: reason = '本季 force_ended=true，仅为用户手动强制完结，不算真实完结，禁止季包，改按单集分享'
    elif watching_status.lower() != 'completed': reason = f"Season.watching_status={watching_status or 'NONE'}，不是 Completed，禁止季包，改按单集分享"
    elif complete: reason = f'本季 Season.watching_status=Completed 且非强制完结，本地已有 {local_count} 个视频标识，允许按季包分享'
    else: reason = '本季 Season.watching_status=Completed 且非强制完结，但本地没有可分享 PC/SHA1，仍按单集分享更安全'

    return {
        'complete': complete, 'expected': expected, 'expected_source': expected_source,
        'known_count': known_count, 'local_count': local_count, 'reason': reason,
        'season_title': season_title, 'watching_status': watching_status, 'force_ended': force_ended,
    }

def _share_policy_for_media(row: Dict[str, Any]) -> Dict[str, Any]:
    item_type = str((row or {}).get('item_type') or '')
    if item_type == 'Movie':
        return {'allowed': True, 'share_type': 'movie_folder', 'share_item_type': 'Movie', 'message': '电影允许分享'}
    if item_type == 'Episode':
        return {'allowed': True, 'share_type': 'episode_file', 'share_item_type': 'Episode', 'message': '未完结剧集按单集分享'}
    if item_type == 'Season':
        info = _season_completion_info(row)
        if info.get('complete'):
            return {'allowed': True, 'share_type': 'season_pack', 'share_item_type': 'Season', 'message': info.get('reason'), 'completion': info}
        return {'allowed': False, 'share_type': 'episode_file', 'share_item_type': 'Episode', 'message': info.get('reason'), 'completion': info}
    if item_type == 'Series':
        return {'allowed': False, 'share_type': 'season_pack', 'share_item_type': 'Season', 'message': '不直接分享整剧：已完结季按季包，未完结季按单集分享'}
    return {'allowed': False, 'share_type': '', 'share_item_type': item_type, 'message': '未知媒体类型，无法分享'}

def _resolve_share_root(media_row: Dict[str, Any]) -> Dict[str, Any]:
    ids = _collect_media_identifiers(media_row)
    file_rows = _get_p115_file_rows(ids['pickcodes'], ids['sha1s'])
    item_type = media_row.get('item_type')
    # 季包根目录收窄需要目标季号。这里原补丁漏定义 season_number，
    # 会在 _narrow_season_pack_root 调用处触发未定义变量。
    season_number = media_row.get('season_number')
    policy = _share_policy_for_media(media_row)
    share_type = policy.get('share_type') or 'movie_folder'
    share_item_type = policy.get('share_item_type') or item_type
    messages = []
    if policy.get('message'):
        messages.append(policy.get('message'))

    if not policy.get('allowed'):
        return {
            'resolvable': False,
            'root_fid': '',
            'root_name': '',
            'root_is_dir': True,
            'file_count': 0,
            'matched_pickcodes': len(ids['pickcodes']),
            'matched_sha1s': len(ids['sha1s']),
            'share_type': share_type,
            'share_item_type': share_item_type,
            'message': '；'.join(messages),
            'completion': policy.get('completion'),
        }

    if not file_rows:
        return {
            'resolvable': False,
            'root_fid': '',
            'root_name': '',
            'root_is_dir': True,
            'file_count': 0,
            'matched_pickcodes': len(ids['pickcodes']),
            'matched_sha1s': len(ids['sha1s']),
            'share_type': share_type,
            'share_item_type': share_item_type,
            'message': 'media_metadata 中有记录，但没有通过 PC/SHA1 在 p115_filesystem_cache 反查到文件',
            'completion': policy.get('completion'),
        }

    parent_ids = [str(r.get('parent_id') or '') for r in file_rows if r.get('parent_id')]
    root_id, root_is_dir = '', True

    if item_type == 'Movie' and len(file_rows) == 1:
        # 单文件电影直接分享文件，避免误把上级“电影分类目录”分享出去。
        root_id = str(file_rows[0].get('id') or '')
        root_is_dir = False
        share_type = 'movie_file'
        root_name = file_rows[0].get('name') or root_id
    elif item_type == 'Episode':
        # 未完结剧集只允许单集分享。若同一集存在多个版本，默认选 size 最大的那个视频文件。
        candidates = sorted(file_rows, key=lambda r: _safe_size_bytes(r.get('size')), reverse=True)
        picked = candidates[0]
        root_id = str(picked.get('id') or '')
        root_is_dir = False
        root_name = picked.get('name') or root_id
        share_type = 'episode_file'
        share_item_type = 'Episode'
        if len(candidates) > 1:
            messages.append(f'该集存在 {len(candidates)} 个版本，已默认选择体积最大的文件')
    elif parent_ids:
        chains = [_ancestor_chain(pid) for pid in parent_ids]
        common = []
        if chains:
            for node_id in chains[0]:
                if all(node_id in ch for ch in chains[1:]):
                    common.append(node_id)
        root_id = common[0] if common else parent_ids[0]
        root_node = _get_p115_node(root_id) or {}
        root_name = root_node.get('name') or root_id
        if item_type == 'Season':
            narrowed = _narrow_season_pack_root(root_id, root_name, parent_ids, season_number)
            if not narrowed.get('ok'):
                return {
                    'resolvable': False,
                    'root_fid': '',
                    'root_name': root_name,
                    'root_is_dir': True,
                    'file_count': len(file_rows),
                    'matched_pickcodes': len(ids['pickcodes']),
                    'matched_sha1s': len(ids['sha1s']),
                    'share_type': share_type,
                    'share_item_type': share_item_type,
                    'message': narrowed.get('message') or '无法安全定位单季分享目录',
                    'completion': policy.get('completion'),
                }
            if narrowed.get('root_id') and narrowed.get('root_id') != root_id:
                root_id = narrowed.get('root_id')
                root_name = narrowed.get('root_name') or root_id
                messages.append(narrowed.get('message'))
        if len(set(parent_ids)) > 1:
            messages.append(f'文件分布在 {len(set(parent_ids))} 个目录，已自动选择共同上级目录：{root_name}')
    else:
        root_id = str(file_rows[0].get('id') or '')
        root_is_dir = False
        root_name = file_rows[0].get('name') or root_id

    if not root_id:
        return {
            'resolvable': False,
            'root_fid': '',
            'root_name': '',
            'root_is_dir': True,
            'file_count': len(file_rows),
            'matched_pickcodes': len(ids['pickcodes']),
            'matched_sha1s': len(ids['sha1s']),
            'share_type': share_type,
            'share_item_type': share_item_type,
            'message': '已找到文件，但无法定位可分享的 115 FID/CID',
            'completion': policy.get('completion'),
        }

    return {
        'resolvable': True,
        'root_fid': root_id,
        'root_name': root_name,
        'root_is_dir': root_is_dir,
        'file_count': 1 if item_type == 'Episode' else len(file_rows),
        'matched_pickcodes': len(ids['pickcodes']),
        'matched_sha1s': len(ids['sha1s']),
        'share_type': share_type,
        'share_item_type': share_item_type,
        'message': '；'.join(messages) if messages else '已通过 PC/SHA1 定位到可分享目录/文件',
        'completion': policy.get('completion'),
    }

def _build_media_candidate(row: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(row)
    parent_series_id = row.get('parent_series_tmdb_id')
    if row.get('item_type') in {'Season', 'Episode'} and not parent_series_id:
        parent_series_id = str(row.get('tmdb_id') or '').split('_')[0] if '_' in str(row.get('tmdb_id') or '') else ''

    series_identity = _get_series_identity(parent_series_id) if parent_series_id else {}
    series_title = series_identity.get('title') or ''
    series_year = series_identity.get('release_year')

    title = row.get('title') or row.get('original_title') or row.get('tmdb_id')
    year = series_year if row.get('item_type') in {'Season', 'Episode'} and series_year else _parse_release_year(row)
    standard_title = series_title if row.get('item_type') in {'Season', 'Episode'} and series_title else title

    display_title = title
    if row.get('item_type') == 'Season':
        display_title = f"{series_title or title} S{int(row.get('season_number') or 0):02d}" if row.get('season_number') else (series_title or title)
    elif row.get('item_type') == 'Episode':
        display_title = _episode_label_from_row(row, series_title)

    resolved = _resolve_share_root(row)
    # 中心端对剧集类资源统一用父剧 TMDb ID + season/episode 定位，避免 Season/Episode TMDb ID 不稳定。
    share_tmdb_id = row.get('tmdb_id')
    if resolved.get('share_item_type') in {'Season', 'Episode'}:
        share_tmdb_id = parent_series_id or row.get('parent_series_tmdb_id') or row.get('tmdb_id')

    return {
        **row,
        'display_title': display_title,
        'series_title': series_title,
        'standard_title': standard_title,
        'release_year': year,
        'parent_series_tmdb_id': parent_series_id,
        'share_tmdb_id': str(share_tmdb_id or ''),
        'share_item_type': resolved.get('share_item_type') or row.get('item_type'),
        **resolved,
    }

def _load_completed_season_row_for_episode(row: Dict[str, Any]) -> Dict[str, Any]:
    """Episode 搜索命中时，如果所属 Season 已 Completed，返回 Season 行用于提升为季包候选。"""
    row = row or {}
    parent_series_id = str(row.get('parent_series_tmdb_id') or '').strip()
    season_number = row.get('season_number')
    if not parent_series_id or season_number in (None, ''):
        return {}
    try:
        season_number = int(season_number)
    except Exception:
        return {}
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT tmdb_id, item_type, title, original_title, parent_series_tmdb_id,
                           season_number, episode_number, release_year, release_date, last_air_date,
                           file_sha1_json, file_pickcode_json, in_library, subscription_status,
                           total_episodes, watching_status, watchlist_tmdb_status,
                           CASE WHEN LOWER(COALESCE(to_jsonb(media_metadata)->>'force_ended', '')) IN ('1','true','yes','on','t','y') THEN TRUE ELSE FALSE END AS force_ended
                    FROM media_metadata
                    WHERE item_type='Season'
                      AND parent_series_tmdb_id=%s
                      AND season_number=%s
                      AND LOWER(COALESCE(watching_status, ''))='completed'
                      AND LOWER(COALESCE(to_jsonb(media_metadata)->>'force_ended', '')) NOT IN ('1','true','yes','on','t','y')
                    ORDER BY tmdb_id
                    LIMIT 1
                """, (parent_series_id, season_number))
                season_row = cur.fetchone()
                return dict(season_row) if season_row else {}
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 查询单集所属 Completed 季失败: series={parent_series_id}, season={season_number}, err={e}")
        return {}

def _expand_share_candidates(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    """搜索结果展开策略：
    - Movie：一个候选；
    - Series：不直接分享整剧，展开为各季/单集；
    - Season.watching_status=Completed 且齐集：只显示季包候选；
    - Season 未 Completed 或未齐集：展开已有文件的 Episode 候选；
    - Episode 命中时，如果所属季已 Completed 且齐集，提升为对应季包，避免完整季被拆成单集。
    """
    row = dict(row or {})
    item_type = row.get('item_type')
    if item_type == 'Movie':
        return [_build_media_candidate(row)]
    if item_type == 'Episode':
        season_row = _load_completed_season_row_for_episode(row)
        if season_row:
            policy = _share_policy_for_media(season_row)
            if policy.get('allowed'):
                return [_build_media_candidate(season_row)]
        return [_build_media_candidate(row)]
    if item_type == 'Season':
        policy = _share_policy_for_media(row)
        if policy.get('allowed'):
            return [_build_media_candidate(row)]
        episodes = _get_episode_rows_for_media(row, only_with_files=True)
        if episodes:
            return [_build_media_candidate(ep) for ep in episodes]
        disabled = _build_media_candidate(row)
        disabled['resolvable'] = False
        disabled['message'] = policy.get('message') or disabled.get('message')
        return [disabled]
    if item_type == 'Series':
        out = []
        series_id = str(row.get('tmdb_id') or '')
        # 优先展开 Season 行。Completed 的季只出季包；未 Completed 的季才展开单集。
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT tmdb_id, item_type, title, original_title, parent_series_tmdb_id,
                        season_number, episode_number, release_year, release_date, last_air_date,
                        file_sha1_json, file_pickcode_json, in_library, subscription_status,
                        total_episodes, watching_status, watchlist_tmdb_status
                    FROM media_metadata
                    WHERE item_type='Season'
                    AND parent_series_tmdb_id=%s
                    AND in_library=TRUE
                    ORDER BY season_number NULLS LAST, tmdb_id
                """, (series_id,))
                seasons = [dict(r) for r in cur.fetchall()]
        if seasons:
            for season in seasons:
                out.extend(_expand_share_candidates(season))
                if len(out) >= 100:
                    break
        else:
            episodes = _get_episode_rows_for_media(row, only_with_files=True)
            for ep in episodes[:100]:
                out.extend(_expand_share_candidates(ep))
        if not out:
            disabled = _build_media_candidate(row)
            disabled['resolvable'] = False
            disabled['message'] = '不直接分享整剧；未找到可分享的已入库季/单集'
            out.append(disabled)
        return out
    return [_build_media_candidate(row)]

@shared_resource_bp.route('/media/search', methods=['GET'])
@admin_required
def api_search_shareable_media():
    keyword = str(request.args.get('keyword') or '').strip()
    if len(keyword) < 1:
        return jsonify({"success": True, "items": []})
    limit = max(1, min(int(request.args.get('limit', 20) or 20), 50))
    search_limit = min(300, max(limit * 5, 100))
    result_limit = min(500, max(limit * 10, 150))
    
    rows = shared_share_db.search_shareable_media(keyword, search_limit, result_limit)
    request_filter = _share_request_filter_from_args(request.args)

    items = []
    seen = set()
    for row in rows:
        try:
            candidates = _expand_share_candidates_for_share_request(row, request_filter)
            for cand in candidates:
                key = (cand.get('share_tmdb_id') or cand.get('tmdb_id'), cand.get('share_item_type') or cand.get('item_type'), cand.get('season_number'), cand.get('episode_number'), cand.get('root_fid'))
                if key in seen: continue
                seen.add(key)
                items.append(cand)
        except Exception as e:
            logger.warning(f"  ➜ [共享资源] 构建可分享候选失败: {row.get('title') or row.get('tmdb_id')} -> {e}")
            row['resolvable'] = False
            row['message'] = str(e)
            items.append(row)

    before_filter_count = len(items)
    items = _filter_candidates_for_share_request(items, request_filter)
    return jsonify({
        "success": True,
        "items": items[:100],
        "filtered_by_share_request": bool(request_filter.get('tmdb_id')),
        "before_filter_count": before_filter_count,
    })

@shared_resource_bp.route('/config', methods=['GET', 'POST'])
@admin_required
def api_shared_resource_config():
    if request.method == 'GET':
        return jsonify({'success': True, 'data': _shared_resource_config_payload()})

    data = _request_json()
    auto_enabled = _boolish(
        data.get('p115_shared_auto_share_requests_enabled', data.get('shared_auto_share_requests_enabled')),
        False,
    )
    # 兼容前端短字段 shared_auto_share_requests_enabled 与后端现有 p115_ 前缀字段。
    data['p115_shared_auto_share_requests_enabled'] = auto_enabled
    data['shared_auto_share_requests_enabled'] = auto_enabled
    block_clean = _boolish(data.get('p115_shared_block_clean_version_transfer'), False)
    data['p115_shared_block_clean_version_transfer'] = block_clean
    payload = settings_db.save_shared_resource_config(data)
    if isinstance(payload, dict):
        payload.setdefault('p115_shared_auto_share_requests_enabled', auto_enabled)
        payload.setdefault('shared_auto_share_requests_enabled', auto_enabled)
        payload.setdefault('p115_shared_block_clean_version_transfer', block_clean)
    return jsonify({'success': True, 'message': '共享资源配置已保存', 'data': payload})

@shared_resource_bp.route('/115/folders', methods=['GET'])
@admin_required
def api_shared_115_folders():
    client = P115Service.get_client()
    if not client:
        return jsonify({'success': False, 'message': '未配置可用的 115 客户端'}), 400
    cid = str(request.args.get('cid') or '0').strip() or '0'
    try:
        resp = client.fs_files({'cid': cid, 'limit': 1000, 'offset': 0, 'show_dir': 1, 'record_open_time': 0, 'count_folders': 0})
        folders = []
        for node in (resp or {}).get('data') or []:
            if not _is_folder(node):
                continue
            name = _node_name(node)
            fid = _node_id(node)
            if not fid:
                continue
            folders.append({'id': str(fid), 'name': name or str(fid), 'parent_id': cid})
        path = []
        for node in (resp or {}).get('path') or []:
            path.append({'id': str(node.get('cid') or node.get('file_id') or node.get('fid') or node.get('id') or ''), 'name': str(node.get('name') or node.get('file_name') or node.get('fn') or node.get('n') or '')})
        return jsonify({'success': True, 'data': folders, 'path': path, 'cid': cid})
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 读取 115 目录失败: cid={cid}, err={e}", exc_info=True)
        return jsonify({'success': False, 'message': f'读取 115 目录失败: {e}'}), 500

@shared_resource_bp.route('/summary', methods=['GET'])
@admin_required
def api_shared_summary():
    summary = shared_credit_db.get_shared_resource_summary()
    return jsonify({"success": True, "data": summary})

def _resp_text(resp) -> str:
    if resp is None:
        return ''
    try:
        if isinstance(resp, dict):
            return json.dumps(resp, ensure_ascii=False)
    except Exception:
        pass
    return str(resp)

@shared_resource_bp.route('/shares', methods=['GET'])
@admin_required
def api_list_my_shares():
    order_by = str(request.args.get('order_by') or 'created_desc').strip() or 'created_desc'
    items, total = shared_share_db.list_share_records(
        status=request.args.get('status', 'all'),
        keyword=request.args.get('keyword', ''),
        page=int(request.args.get('page', 1) or 1),
        page_size=int(request.args.get('page_size', 30) or 30),
        order_by=order_by,
    )

    for item in items:
        try:
            identity = _standard_share_identity(item)
            if identity.get('title'):
                item['title'] = identity.get('title')
            if identity.get('release_year'):
                item['release_year'] = identity.get('release_year')
            if identity.get('tmdb_id'):
                item['tmdb_id'] = str(identity.get('tmdb_id'))
            if identity.get('parent_series_tmdb_id'):
                item['parent_series_tmdb_id'] = identity.get('parent_series_tmdb_id')
                
            # 增强版老数据兼容：JSON 提取不到就用正则从文件名提取。
            if item.get('episode_number') is None:
                ep = None
                raw = _safe_json_obj(item.get('raw_json')) or {}
                ep = (raw.get('standard_identity') or {}).get('episode_number') or \
                     (raw.get('manual_payload') or {}).get('episode_number') or \
                     (raw.get('auto_gap') or {}).get('episode_number')
                
                # 终极兜底：如果 JSON 里没有，直接从 root_name 或 title 里正则提取 (例如 S01E27 -> 27)
                if ep is None:
                    ep = _guess_episode_number(item.get('root_name') or item.get('title'))
                    
                if ep is not None:
                    item['episode_number'] = ep

            _decorate_my_share_source_row(item)
                    
        except Exception:
            try:
                _decorate_my_share_source_row(item)
            except Exception:
                pass
    return jsonify({"success": True, "items": items, "total": total})

@shared_resource_bp.route('/shares/<int:record_id>/items', methods=['GET'])
@admin_required
def api_list_share_items(record_id):
    return jsonify({"success": True, "items": shared_share_db.list_share_items(record_id)})

def _manual_share_business_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    """统一整理手动分享 payload，不创建 115 分享。"""
    data = dict(data or {})
    share_request_payload = data.get('share_request_payload') if isinstance(data.get('share_request_payload'), dict) else {}

    # 响应“单集求分享”时强制创建单集分享。
    if str((share_request_payload or {}).get('target_type') or '').strip().lower() == 'episode':
        data['share_type'] = 'episode_file'
        data['item_type'] = 'Episode'
        if (share_request_payload or {}).get('season_number') not in (None, ''):
            data['season_number'] = share_request_payload.get('season_number')
        if (share_request_payload or {}).get('episode_number') not in (None, ''):
            data['episode_number'] = share_request_payload.get('episode_number')

    share_type = str(data.get('share_type') or '').strip()
    item_type = str(data.get('item_type') or '').strip()
    if share_type == 'series_pack':
        if str((share_request_payload or {}).get('target_type') or '').strip().lower() not in ('series', 'tv'):
            return {'ok': False, 'message': '普通手动分享仍禁用整剧分享；只有响应全剧求分享时才允许创建全剧包', 'data': data}
        data['item_type'] = 'Series'
        data['season_number'] = None
        data['episode_number'] = None
        item_type = 'Series'

    if share_type == 'season_pack':
        check_row = {
            'item_type': 'Season',
            'tmdb_id': data.get('parent_series_tmdb_id') or data.get('tmdb_id'),
            'parent_series_tmdb_id': data.get('parent_series_tmdb_id') or data.get('tmdb_id'),
            'season_number': data.get('season_number'),
        }
        policy = _share_policy_for_media(check_row)
        if not policy.get('allowed'):
            return {'ok': False, 'message': policy.get('message') or '未完结季禁止按季包分享，请选择单集分享', 'data': data, 'policy': policy}

    if item_type == 'Episode':
        data['share_type'] = 'episode_file'

    return {'ok': True, 'data': data}

def _collect_manual_share_files_for_payload(data: Dict[str, Any], client=None) -> Dict[str, Any]:
    """按手动分享候选实际收集待分享视频文件，供预校验和正式创建共用。"""
    normalized = _manual_share_business_payload(data)
    if not normalized.get('ok'):
        return {**normalized, 'files': [], 'root_name': '', 'root_is_dir': True, 'info_resp': {}}
    data = normalized.get('data') or {}

    root_fid = str(data.get('root_fid') or '').strip()
    if not root_fid:
        return {'ok': False, 'message': '缺少要分享的 115 文件/目录 FID/CID', 'data': data, 'files': [], 'root_name': '', 'root_is_dir': True, 'info_resp': {}}

    if client is None:
        client = P115Service.get_client()
    if not client:
        return {'ok': False, 'message': '未配置可用的 115 Cookie 客户端，无法创建分享', 'data': data, 'files': [], 'root_name': '', 'root_is_dir': True, 'info_resp': {}}

    info_resp = client.fs_get_info(root_fid)
    node = (info_resp or {}).get('data') or {}
    root_name = data.get('root_name') or _node_name(node) or root_fid
    if 'root_is_dir' in data:
        root_is_dir = _boolish(data.get('root_is_dir'), default=True)
    else:
        root_is_dir = _is_folder(node) if node else True

    max_depth = int(data.get('max_depth') or 6)
    files = _collect_files_from_115(client, root_fid, root_name=root_name, max_depth=max_depth, assume_dir=root_is_dir)
    if not files:
        files = _collect_files_from_media_payload(data)
    if not files:
        return {
            'ok': False,
            'message': '未能定位到可分享的视频文件，禁止创建空分享',
            'data': data,
            'files': [],
            'root_name': root_name,
            'root_is_dir': root_is_dir,
            'info_resp': info_resp,
        }

    share_type_now = str(data.get('share_type') or '').strip().lower()
    for item in files:
        if not item.get('tmdb_id'):
            item['tmdb_id'] = str(data.get('tmdb_id') or '')

        if share_type_now == 'series_pack':
            parsed_s, parsed_e = _guess_season_episode_numbers(item.get('relative_path') or item.get('file_name') or '')
            if not item.get('season_number') and parsed_s is not None:
                item['season_number'] = parsed_s
            if not item.get('episode_number') and parsed_e is not None:
                item['episode_number'] = parsed_e
            item['item_type'] = 'Episode' if item.get('episode_number') else 'Series'
        elif share_type_now == 'episode_file':
            item['item_type'] = 'Episode'
            if not item.get('episode_number') and data.get('episode_number'):
                item['episode_number'] = data.get('episode_number')
        elif not item.get('item_type'):
            item['item_type'] = 'Episode' if share_type_now in ('season_pack', 'series_pack') and item.get('episode_number') else data.get('item_type')

        if share_type_now != 'series_pack' and not item.get('season_number'):
            item['season_number'] = data.get('season_number')
        if not item.get('episode_number') and data.get('episode_number'):
            item['episode_number'] = data.get('episode_number')

    return {
        'ok': True,
        'message': '已定位到可分享视频文件',
        'data': data,
        'files': files,
        'root_name': root_name,
        'root_is_dir': root_is_dir,
        'info_resp': info_resp,
    }

@shared_resource_bp.route('/shares/manual-validate', methods=['POST'])
@admin_required
def api_manual_validate_share():
    """前端选择候选后立即预校验，避免点“创建分享”后才失败。"""
    data = _request_json()
    client = P115Service.get_client()
    prepared = _collect_manual_share_files_for_payload(data, client=client)
    share_data = prepared.get('data') or data or {}
    share_type = str(share_data.get('share_type') or '').strip().lower()
    result = {
        'valid': False,
        'share_type': share_type,
        'item_type': share_data.get('item_type'),
        'file_count': len(prepared.get('files') or []),
        'root_fid': share_data.get('root_fid'),
        'root_name': prepared.get('root_name') or share_data.get('root_name'),
        'root_is_dir': prepared.get('root_is_dir'),
        'message': prepared.get('message') or '校验失败',
    }

    if not prepared.get('ok'):
        return jsonify({'success': True, 'message': result['message'], 'data': result})

    files = prepared.get('files') or []
    missing_raw = _files_missing_raw_ffprobe(files)
    result['missing_raw'] = missing_raw
    if missing_raw:
        result['message'] = _raw_missing_message(missing_raw)
        return jsonify({'success': True, 'message': result['message'], 'data': result})

    standard_identity = _standard_media_identity_for_share({
        **share_data,
        'item_type': share_data.get('item_type') or 'Season',
        'share_type': share_data.get('share_type') or ('season_pack' if share_data.get('season_number') else 'movie_folder'),
    })
    blacklist_item = _center_blacklist_item_for_share(share_data, standard_identity)
    blacklist_hit = _check_center_resource_blacklist(blacklist_item)
    result['blacklist'] = blacklist_hit or None
    if blacklist_hit:
        result['message'] = blacklist_hit.get('message') or '命中中心黑名单，禁止创建分享'
        return jsonify({'success': True, 'message': result['message'], 'data': result})

    if share_type == 'season_pack':
        consistency = _validate_season_pack_consistency(files, share_data)
        result['season_pack_consistency'] = consistency
        if not consistency.get('ok'):
            result['message'] = consistency.get('message') or '季包媒体参数不一致，禁止创建分享'
            return jsonify({'success': True, 'message': result['message'], 'data': result})
        result['valid'] = True
        result['message'] = consistency.get('message') or f"季包一致性校验通过：共 {len(files)} 个视频文件，可创建分享"
        return jsonify({'success': True, 'message': result['message'], 'data': result})

    result['valid'] = True
    result['message'] = f"预校验通过：共 {len(files)} 个视频文件，可创建分享"
    return jsonify({'success': True, 'message': result['message'], 'data': result})

@shared_resource_bp.route('/shares/manual-create', methods=['POST'])
@admin_required
def api_manual_create_share():
    data = _request_json()

    # 响应“单集求分享”时强制创建单集分享。
    # 即使前端/旧缓存误传了 season_pack，也不能把单集悬赏登记成剧集包。
    share_request_payload = data.get('share_request_payload') if isinstance(data.get('share_request_payload'), dict) else {}
    if str((share_request_payload or {}).get('target_type') or '').strip().lower() == 'episode':
        data['share_type'] = 'episode_file'
        data['item_type'] = 'Episode'
        if (share_request_payload or {}).get('season_number') not in (None, ''):
            data['season_number'] = share_request_payload.get('season_number')
        if (share_request_payload or {}).get('episode_number') not in (None, ''):
            data['episode_number'] = share_request_payload.get('episode_number')

    root_fid = str(data.get('root_fid') or '').strip()
    if not root_fid:
        return jsonify({"success": False, "message": "缺少要分享的 115 文件/目录 FID/CID"}), 400

    share_type = str(data.get('share_type') or '').strip()
    item_type = str(data.get('item_type') or '').strip()
    if share_type == 'series_pack':
        if str((share_request_payload or {}).get('target_type') or '').strip().lower() not in ('series', 'tv'):
            return jsonify({"success": False, "message": "普通手动分享仍禁用整剧分享；只有响应全剧求分享时才允许创建全剧包"}), 400
        data['item_type'] = 'Series'
        data['season_number'] = None
        data['episode_number'] = None
        item_type = 'Series'
    if share_type == 'season_pack':
        check_row = {
            'item_type': 'Season',
            'tmdb_id': data.get('parent_series_tmdb_id') or data.get('tmdb_id'),
            'parent_series_tmdb_id': data.get('parent_series_tmdb_id') or data.get('tmdb_id'),
            'season_number': data.get('season_number'),
        }
        policy = _share_policy_for_media(check_row)
        if not policy.get('allowed'):
            return jsonify({"success": False, "message": policy.get('message') or "未完结季禁止按季包分享，请选择单集分享"}), 400
    if item_type == 'Episode':
        data['share_type'] = 'episode_file'

    client = P115Service.get_client()
    if not client:
        return jsonify({"success": False, "message": "未配置可用的 115 Cookie 客户端，无法创建分享"}), 400

    # 先定位分享文件并校验 raw_ffprobe_json；缺 raw 时不创建 115 分享，避免生成垃圾 share_code。
    info_resp = client.fs_get_info(root_fid)
    node = (info_resp or {}).get('data') or {}
    root_name = data.get('root_name') or _node_name(node) or root_fid
    # 前端搜索阶段已经根据 PC/SHA1 和 p115_filesystem_cache 判断过 root_is_dir，
    # 这里应优先信任前端传入值；fs_get_info 对目录偶尔返回字段不完整，会误判成文件。
    if 'root_is_dir' in data:
        root_is_dir = _boolish(data.get('root_is_dir'), default=True)
    else:
        root_is_dir = _is_folder(node) if node else True

    max_depth = int(data.get('max_depth') or 6)
    files = _collect_files_from_115(client, root_fid, root_name=root_name, max_depth=max_depth, assume_dir=root_is_dir)
    if not files:
        files = _collect_files_from_media_payload(data)
    if not files:
        return jsonify({"success": False, "message": "未能定位到可分享的视频文件，禁止创建空分享"}), 400

    for item in files:
        if not item.get('tmdb_id'):
            item['tmdb_id'] = str(data.get('tmdb_id') or '')

        # 单集分享必须强制登记为 Episode。之前只在 item_type 为空时才填充，
        # 如果上游兜底 payload 把 item_type 带成 Season，就会把每一集错误登记成“剧集包”。
        share_type_now = str(data.get('share_type') or '').strip().lower()
        if share_type_now == 'series_pack':
            parsed_s, parsed_e = _guess_season_episode_numbers(item.get('relative_path') or item.get('file_name') or '')
            if not item.get('season_number') and parsed_s is not None:
                item['season_number'] = parsed_s
            if not item.get('episode_number') and parsed_e is not None:
                item['episode_number'] = parsed_e
            # 本地明细尽量保留到 Episode 级，方便展示/排查；中心登记时会按 series_pack 统一为 Series。
            item['item_type'] = 'Episode' if item.get('episode_number') else 'Series'
        elif share_type_now == 'episode_file':
            item['item_type'] = 'Episode'
            if not item.get('episode_number') and data.get('episode_number'):
                item['episode_number'] = data.get('episode_number')
        elif not item.get('item_type'):
            item['item_type'] = 'Episode' if share_type_now in ('season_pack', 'series_pack') and item.get('episode_number') else data.get('item_type')

        if share_type_now != 'series_pack' and not item.get('season_number'):
            item['season_number'] = data.get('season_number')
        if not item.get('episode_number') and data.get('episode_number'):
            item['episode_number'] = data.get('episode_number')

    missing_raw = _files_missing_raw_ffprobe(files)
    if missing_raw:
        return jsonify({
            "success": False,
            "message": _raw_missing_message(missing_raw),
            "missing_raw": missing_raw,
        }), 400

    if str(data.get('share_type') or '').strip().lower() == 'season_pack':
        consistency = _validate_season_pack_consistency(files, data)
        if not consistency.get('ok'):
            return jsonify({
                "success": False,
                "message": consistency.get('message') or "季包媒体参数不一致，禁止创建分享",
                "season_pack_consistency": consistency,
            }), 400

    standard_identity = _standard_media_identity_for_share({
        **data,
        'item_type': data.get('item_type') or 'Season',
        'share_type': data.get('share_type') or ('season_pack' if data.get('season_number') else 'movie_folder'),
    })
    blacklist_item = _center_blacklist_item_for_share(data, standard_identity)
    blacklist_hit = _check_center_resource_blacklist(blacklist_item)
    if blacklist_hit:
        return jsonify({"success": False, "message": blacklist_hit.get('message') or "命中中心黑名单，禁止创建分享", "blacklist": blacklist_hit}), 400

    receive_code = str(data.get('receive_code') or '').strip() or None
    share_resp = client.share_create([root_fid], share_duration=-1, receive_code=receive_code)
    if not share_resp or not share_resp.get('state'):
        if _looks_resource_violation_response(share_resp):
            _report_center_resource_blacklist(blacklist_item, share_resp, reason='share_blocked')
        return jsonify({"success": False, "message": f"创建 115 分享失败: {share_resp}"}), 500

    share_data = share_resp.get('data') or {}
    share_code = share_data.get('share_code') or share_resp.get('share_code')
    share_url = share_data.get('share_url') or (f"https://115.com/s/{share_code}" if share_code else '')
    receive_code = receive_code or share_data.get('receive_code') or ''

    standard_identity = _standard_media_identity_for_share({
        **data,
        'item_type': data.get('item_type') or 'Season',
        'share_type': data.get('share_type') or ('season_pack' if data.get('season_number') else 'movie_folder'),
    })
    standard_title = standard_identity.get('title') or str(data.get('title') or '').strip() or root_name
    standard_year = standard_identity.get('release_year') or data.get('release_year')

    record = shared_share_db.create_share_record({
        'share_code': share_code,
        'receive_code': receive_code,
        'share_url': share_url,
        'share_type': data.get('share_type') or ('season_pack' if data.get('season_number') else 'movie_folder'),
        'root_fid': root_fid,
        'root_name': root_name,
        'root_is_dir': root_is_dir,
        'tmdb_id': str(standard_identity.get('tmdb_id') or data.get('tmdb_id') or ''),
        'item_type': data.get('item_type') or 'Season',
        'parent_series_tmdb_id': standard_identity.get('parent_series_tmdb_id') or data.get('parent_series_tmdb_id'),
        'season_number': data.get('season_number'),
        'episode_number': data.get('episode_number'),
        'title': standard_title,
        'release_year': standard_year,
        'status': 'pending_review',
        'review_status': 'pending_review',
        'center_status': 'not_reported',
        'raw_json': {
            'share_response': share_resp,
            'root_info': info_resp,
            'manual_payload': data,
            'standard_identity': standard_identity,
            'share_request_group_id': data.get('share_request_group_id') or None,
            'share_request_payload': data.get('share_request_payload') or None,
        },
    })
    count = shared_share_db.replace_share_items(record['id'], files)
    record = shared_share_db.update_share_record(record['id'], item_count=count)
    shared_credit_db.add_credit_ledger('share_created', 0, '手动创建115分享，等待审核', ref_id=str(record['id']), title=record.get('title') or '', raw_json={'share_code': share_code, 'item_count': count})

    return jsonify({"success": True, "message": "分享已创建，等待 115 审核通过后再登记中心", "data": record, "items": files})

@shared_resource_bp.route('/shares/<int:record_id>/check', methods=['POST'])
@admin_required
def api_check_share(record_id):
    record = shared_share_db.get_share_record(record_id)
    if not record:
        return jsonify({"success": False, "message": "分享记录不存在"}), 404
    client = P115Service.get_client()
    if not client:
        return jsonify({"success": False, "message": "未配置可用的 115 Cookie 客户端"}), 400

    snap = client.share_info(record.get('share_code'), record.get('receive_code'), cid=0, limit=1)
    parsed = _parse_share_status(snap)

    # 分享审核通过后，如果创建时没有收集到包内文件，借“检查”按钮自动补扫一次。
    added_count = None
    try:
        current_items = shared_share_db.list_share_items(record_id)
        if parsed['status'] == 'alive' and not current_items:
            root_fid = str(record.get('root_fid') or '')
            files = []
            if root_fid:
                files = _collect_files_from_115(
                    client,
                    root_fid,
                    root_name=record.get('root_name') or '',
                    max_depth=6,
                    assume_dir=_boolish(record.get('root_is_dir'), default=True),
                )
            if not files:
                files = _collect_files_from_media_payload(record)
            raw_payload = {}
            try:
                raw_payload = (record.get('raw_json') or {}).get('manual_payload') or {}
            except Exception:
                raw_payload = {}
            for item in files:
                if not item.get('tmdb_id'):
                    item['tmdb_id'] = str(record.get('tmdb_id') or '')

                share_type_now = str(record.get('share_type') or '').strip().lower()
                if share_type_now == 'episode_file':
                    item['item_type'] = 'Episode'
                    if not item.get('episode_number') and raw_payload.get('episode_number'):
                        item['episode_number'] = raw_payload.get('episode_number')
                elif not item.get('item_type'):
                    item['item_type'] = 'Episode' if share_type_now in ('season_pack', 'series_pack') and (item.get('episode_number') or raw_payload.get('episode_number')) else record.get('item_type')

                if not item.get('season_number'):
                    item['season_number'] = record.get('season_number')
                if not item.get('episode_number') and raw_payload.get('episode_number'):
                    item['episode_number'] = raw_payload.get('episode_number')
            if files:
                added_count = shared_share_db.replace_share_items(record_id, files)
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 检查分享时补扫包内文件失败: record={record_id}, err={e}", exc_info=True)

    parsed_message = str(parsed.get('message') or '').strip()
    is_share_ok = parsed.get('status') == 'alive' and parsed.get('review_status') == 'alive'
    # last_error 只保存真正的异常/审核说明。分享正常时不要写“分享可用”，
    # 否则前端“错误”列会把正常结果显示成错误信息。
    last_error = '' if is_share_ok else parsed_message

    old_raw_json = _safe_json_obj(record.get('raw_json')) or {}
    update_kwargs = dict(
        status=parsed['status'], review_status=parsed['review_status'], last_checked_at='NOW()',
        last_error=last_error, raw_json={**old_raw_json, 'last_snap': snap},
    )
    if added_count is not None:
        update_kwargs['item_count'] = added_count
    row = shared_share_db.update_share_record(record_id, **update_kwargs)
    msg = parsed_message or ('分享可用' if is_share_ok else '检查完成')
    if added_count is not None:
        msg = f"{msg}，已补扫到 {added_count} 个视频文件"
    return jsonify({"success": True, "message": msg, "data": row, "raw": snap})

def _share_request_group_id_for_record(record: Dict[str, Any]) -> str:
    raw = record.get('raw_json') if isinstance(record.get('raw_json'), dict) else {}
    manual = raw.get('manual_payload') if isinstance(raw.get('manual_payload'), dict) else {}
    payload = raw.get('share_request_payload') if isinstance(raw.get('share_request_payload'), dict) else {}
    for value in (
        record.get('share_request_group_id'),
        raw.get('share_request_group_id'),
        manual.get('share_request_group_id'),
        payload.get('group_id'),
        payload.get('share_request_group_id'),
    ):
        text = str(value or '').strip()
        if text:
            return text
    return ''

def _ensure_share_request_listener_async():
    """有 open 求分享时启动客户端后台长轮询，不依赖前端页面存活。"""
    try:
        from tasks.shared_resource_tasks import ensure_share_request_event_listener
        ensure_share_request_event_listener()
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 启动求分享事件监听失败: {e}")

def _build_center_source_payload(record: Dict[str, Any], item: Dict[str, Any], *, source_provider: str = 'user_share') -> Dict[str, Any]:
    """把本地 share_record/share_item 转成中心登记 payload。"""
    sha1 = str(item.get('sha1') or '').strip().upper()
    record_share_type = str((record or {}).get('share_type') or '').strip().lower()
    is_season_pack = _is_season_pack_record(record)
    if record_share_type == 'series_pack':
        center_item_type = 'Series'
    elif is_season_pack:
        center_item_type = 'Season'
    else:
        center_item_type = item.get('item_type') or record.get('item_type') or 'Movie'
    if record_share_type == 'episode_file':
        center_item_type = 'Episode'
    center_episode_number = None if is_season_pack else item.get('episode_number')
    standard_identity = _standard_share_identity(record, item, center_item_type=center_item_type)
    payload = {
        'tmdb_id': str(standard_identity.get('tmdb_id') or item.get('tmdb_id') or record.get('tmdb_id') or ''),
        'item_type': center_item_type,
        'season_number': item.get('season_number') or record.get('season_number'),
        'episode_number': center_episode_number,
        'title': standard_identity.get('title') or record.get('title') or '',
        'release_year': standard_identity.get('release_year') or record.get('release_year'),
        'sha1': sha1,
        'size': _safe_size_bytes(item.get('size')),
        'file_name': item.get('file_name') or '',
        'quality': '',
        'source_provider': source_provider or 'user_share',
        'share_code': record.get('share_code'),
        'receive_code': record.get('receive_code') or '',
        'has_raw_ffprobe': bool(item.get('raw_ffprobe_uploaded')),
    }
    share_request_group_id = _share_request_group_id_for_record(record)
    if share_request_group_id:
        payload['share_request_group_id'] = share_request_group_id
    return payload

def _register_single_source_payload(payload: Dict[str, Any], item: Dict[str, Any], cfg: Dict[str, Any], headers: Dict[str, str]) -> Dict[str, Any]:
    resp = requests.post(
        f"{cfg['center_url']}/api/v1/sources/register",
        headers=headers,
        json=payload,
        **_center_request_kwargs(20),
    )

    # 中心提示 raw 缺失时，强制重传 raw 后再登记一次。
    if resp.status_code == 400 and 'raw_ffprobe_json required before source register' in (resp.text or ''):
        raw_retry = _upload_item_raw_ffprobe_to_center(item, cfg, headers, force=True)
        if raw_retry.get('ok'):
            payload = dict(payload)
            payload['has_raw_ffprobe'] = True
            resp = requests.post(
                f"{cfg['center_url']}/api/v1/sources/register",
                headers=headers,
                json=payload,
                **_center_request_kwargs(20),
            )
        else:
            return {'ok': False, 'message': f"raw重传失败 {raw_retry.get('message')}"}

    if not resp.ok:
        return {'ok': False, 'message': f"HTTP {resp.status_code} {resp.text[:160]}"}
    data = resp.json() or {}
    return {'ok': True, 'source_id': data.get('source_id'), 'data': data}

def _source_provider_for_share_record(record: Dict[str, Any], fallback: str = 'user_share') -> str:
    raw = record.get('raw_json') if isinstance(record.get('raw_json'), dict) else {}
    if raw.get('auto_backup_share') or raw.get('backup_share') or raw.get('backup_mirror') or raw.get('backup_instruction'):
        return 'backup_mirror'
    if raw.get('share_request_group_id') or raw.get('share_request_payload') or ((raw.get('manual_payload') or {}).get('share_request_group_id') if isinstance(raw.get('manual_payload'), dict) else None):
        return 'request_share'
    if raw.get('auto_gap'):
        return 'auto_gap_share'
    return fallback or 'user_share'

def _register_share_items_to_center(record: Dict[str, Any], items: List[Dict[str, Any]], cfg: Dict[str, Any], headers: Dict[str, str], *, source_provider: str = 'user_share') -> Dict[str, Any]:
    """登记分享项到中心。

    单集/电影沿用单条接口；季包走批量接口。季包批量失败时只重传 RAW，再整包重试，最终必须全成功才标记入池。
    """
    items = items or []
    errors = []
    first_source_id = None
    reported = 0
    is_season_pack = _is_season_pack_record(record)

    clean_version_meta = _detect_clean_version_for_local_season_pack(record, items, source_provider=source_provider) if is_season_pack else {'is_clean_version': False}
    if is_season_pack and clean_version_meta.get('is_clean_version'):
        logger.info(
            "  ➜ [共享资源] 季包识别为疑似纯净版：%s S%s，命中 %s/%s 集，平均短 %s 分钟",
            record.get('title') or record.get('root_name') or '',
            clean_version_meta.get('season_number'),
            clean_version_meta.get('hit_count'),
            clean_version_meta.get('comparable_count'),
            clean_version_meta.get('avg_delta_minutes'),
        )

    payloads = []
    payload_items = []
    for item in items:
        sha1 = str(item.get('sha1') or '').strip().upper()
        if not sha1:
            errors.append(f"{item.get('file_name')} 缺少 SHA1")
            continue
        payload = _build_center_source_payload(record, item, source_provider=source_provider)
        if is_season_pack:
            payload['is_clean_version'] = bool(clean_version_meta.get('is_clean_version'))
            payload['clean_version_confidence'] = clean_version_meta.get('clean_version_confidence')
            payload['clean_version_meta_json'] = clean_version_meta if clean_version_meta else None
        payloads.append(payload)
        payload_items.append(item)

    if errors:
        return {
            'reported': 0,
            'errors': errors,
            'first_source_id': None,
            'batch_used': is_season_pack,
            'item_results': [],
        }

    if not is_season_pack:
        item_results = []
        for item, payload in zip(payload_items, payloads):
            try:
                result = _register_single_source_payload(payload, item, cfg, headers)
            except Exception as e:
                result = {'ok': False, 'message': str(e)}
            if result.get('ok'):
                source_id = result.get('source_id') or ''
                shared_share_db.mark_item_reported(item['id'], source_id)
                first_source_id = first_source_id or source_id
                reported += 1
            else:
                errors.append(f"{item.get('file_name')}: {result.get('message')}")
            item_results.append({
                'sha1': payload.get('sha1'),
                'file_name': item.get('file_name') or '',
                'ok': bool(result.get('ok')),
                'source_id': result.get('source_id') or '',
                'message': result.get('message') or '',
            })
        return {
            'reported': reported,
            'errors': errors,
            'first_source_id': first_source_id,
            'batch_used': False,
            'item_results': item_results,
        }

    def _post_batch_register() -> Dict[str, Any]:
        resp = requests.post(
            f"{cfg['center_url']}/api/v1/sources/register-batch",
            headers=headers,
            json={'items': payloads},
            **_center_request_kwargs(90),
        )
        if not resp.ok:
            return {
                'ok': False,
                'items': [{'index': i, 'ok': False, 'message': f"HTTP {resp.status_code} {resp.text[:160]}"} for i in range(len(payloads))],
                'message': f"HTTP {resp.status_code} {resp.text[:160]}",
            }
        return resp.json() or {}

    data = _post_batch_register()
    retried_raw = 0
    if not data.get('ok'):
        # 批量返回 raw 缺失时，按失败项自动单条重传 RAW，然后整包重新登记一次。
        for result in data.get('items') or []:
            msg = str(result.get('message') or '')
            idx = result.get('index')
            try:
                idx = int(idx)
            except Exception:
                idx = -1
            if idx < 0 or idx >= len(payload_items):
                continue
            if 'raw_ffprobe_json required' not in msg:
                continue
            raw_retry = _upload_item_raw_ffprobe_to_center(payload_items[idx], cfg, headers, force=True)
            if raw_retry.get('ok'):
                payloads[idx]['has_raw_ffprobe'] = True
                retried_raw += 1
        if retried_raw > 0:
            data = _post_batch_register()

    if data.get('ok'):
        by_index = {}
        for result in data.get('items') or []:
            try:
                by_index[int(result.get('index'))] = result
            except Exception:
                continue
        for idx, item in enumerate(payload_items):
            result = by_index.get(idx) or {}
            source_id = result.get('source_id') or ''
            if not source_id:
                errors.append(f"{item.get('file_name')}: 批量登记返回缺少 source_id")
                continue
            shared_share_db.mark_item_reported(item['id'], source_id)
            first_source_id = first_source_id or source_id
            reported += 1
        if reported != len(payload_items):
            errors.append(f"季包登记未全量成功：{reported}/{len(payload_items)}")
    else:
        for result in data.get('items') or []:
            idx = result.get('index')
            try:
                idx = int(idx)
            except Exception:
                idx = -1
            item_name = payload_items[idx].get('file_name') if 0 <= idx < len(payload_items) else result.get('file_name')
            msg = result.get('message') or data.get('message') or '批量登记失败'
            errors.append(f"{item_name or '未知文件'}: {msg}")
        if not errors:
            errors.append(data.get('message') or '季包批量登记失败')

    # 季包必须整包全成功才算入池；不允许 partial。
    if reported != len(payload_items):
        return {
            'reported': 0,
            'errors': errors[:20],
            'first_source_id': None,
            'batch_used': True,
            'retried_raw': retried_raw,
            'item_results': data.get('items') or [],
        }

    return {
        'reported': reported,
        'errors': [],
        'first_source_id': first_source_id,
        'batch_used': True,
        'retried_raw': retried_raw,
        'item_results': data.get('items') or [],
    }

@shared_resource_bp.route('/shares/<int:record_id>/report-center', methods=['POST'])
@admin_required
def api_report_share_to_center(record_id):
    record = shared_share_db.get_share_record(record_id)
    if not record:
        return jsonify({"success": False, "message": "分享记录不存在"}), 404
    if record.get('review_status') != 'alive' and record.get('status') != 'alive':
        return jsonify({"success": False, "message": "分享尚未审核通过，请先检查分享状态"}), 400

    cfg, headers = _center_headers()
    items = shared_share_db.list_share_items(record_id)
    if not items:
        # 兼容旧记录：上一版可能因为 115 目录误判导致创建时 item_count=0。
        # 登记中心前再做一次自动补扫，避免用户必须先点“检查”。
        try:
            client = P115Service.get_client()
            files = []
            if client and record.get('root_fid'):
                files = _collect_files_from_115(
                    client,
                    str(record.get('root_fid')),
                    root_name=record.get('root_name') or '',
                    max_depth=8,
                    assume_dir=_boolish(record.get('root_is_dir'), default=True),
                )
            if not files:
                files = _collect_files_from_media_payload(record)
            raw_payload = {}
            try:
                raw_payload = (record.get('raw_json') or {}).get('manual_payload') or {}
            except Exception:
                raw_payload = {}
            for item in files:
                if not item.get('tmdb_id'):
                    item['tmdb_id'] = str(record.get('tmdb_id') or '')

                share_type_now = str(record.get('share_type') or '').strip().lower()
                if share_type_now == 'episode_file':
                    item['item_type'] = 'Episode'
                    if not item.get('episode_number') and raw_payload.get('episode_number'):
                        item['episode_number'] = raw_payload.get('episode_number')
                elif not item.get('item_type'):
                    item['item_type'] = 'Episode' if share_type_now in ('season_pack', 'series_pack') and (item.get('episode_number') or raw_payload.get('episode_number')) else record.get('item_type')

                if not item.get('season_number'):
                    item['season_number'] = record.get('season_number')
                if not item.get('episode_number') and raw_payload.get('episode_number'):
                    item['episode_number'] = raw_payload.get('episode_number')
            if files:
                shared_share_db.replace_share_items(record_id, files)
                shared_share_db.update_share_record(record_id, item_count=len(files))
                items = shared_share_db.list_share_items(record_id)
        except Exception as e:
            logger.warning(f"  ➜ [共享资源] 登记中心前自动补扫包内文件失败: record={record_id}, err={e}", exc_info=True)

    if not items:
        return jsonify({"success": False, "message": "分享包内没有可登记的视频文件；已尝试从115目录和本地缓存补扫但仍未命中，请确认 root_fid 是季目录且 p115_filesystem_cache 已同步该目录"}), 400

    # 登记中心前先上传 raw_ffprobe_json；同时用 raw.format.size 回填本地 size=0 的条目。
    raw_summary = _upload_share_raw_ffprobe_to_center(record_id, cfg, headers, force=True)
    # 重新读取 items，确保 size/raw_ffprobe_uploaded 是最新状态。
    items = shared_share_db.list_share_items(record_id)

    missing_raw = _files_missing_raw_ffprobe(items)
    not_uploaded = [
        i for i in items
        if str(i.get('sha1') or '').strip()
        and not i.get('raw_ffprobe_uploaded')
        and not any(str(m.get('sha1') or '').upper() == str(i.get('sha1') or '').upper() for m in missing_raw)
    ]
    if missing_raw or not_uploaded or raw_summary.get('errors'):
        errors = []
        if missing_raw:
            errors.append(_raw_missing_message(missing_raw))
        if not_uploaded:
            errors.append('存在 raw_ffprobe_json 尚未成功上传中心的分享项，禁止登记中心')
        if raw_summary.get('errors'):
            errors.extend(raw_summary.get('errors')[:5])
        row = shared_share_db.update_share_record(
            record_id,
            center_status='failed',
            last_error='；'.join(errors[:8]),
        )
        shared_credit_db.add_credit_ledger(
            'share_raw_missing_blocked', 0,
            f"分享缺少 raw_ffprobe_json，已阻止登记中心：{len(missing_raw)} 个缺失，{len(not_uploaded)} 个未上传",
            ref_id=str(record_id), title=record.get('title') or '',
            raw_json={'missing_raw': missing_raw, 'not_uploaded': not_uploaded, 'raw_summary': raw_summary}
        )
        return jsonify({
            "success": False,
            "message": '；'.join(errors[:5]) or "缺少 raw_ffprobe_json，禁止登记中心",
            "data": row,
            "missing_raw": missing_raw,
            "raw_summary": raw_summary,
        }), 400

    record_share_type_for_check = str(record.get('share_type') or '').strip().lower()
    if record_share_type_for_check in ('season_pack', 'series_pack', 'season', 'tv_pack'):
        consistency = _validate_season_pack_consistency(items, record)
        if not consistency.get('ok'):
            row = shared_share_db.update_share_record(
                record_id,
                center_status='failed',
                last_error=consistency.get('message') or '季包媒体参数不一致，禁止登记中心',
            )
            shared_credit_db.add_credit_ledger(
                'share_season_pack_inconsistent_blocked', 0,
                '季包分辨率或 HDR/杜比不一致，已阻止登记中心',
                ref_id=str(record_id), title=record.get('title') or '',
                raw_json={'season_pack_consistency': consistency},
            )
            return jsonify({
                "success": False,
                "message": consistency.get('message') or "季包媒体参数不一致，禁止登记中心",
                "data": row,
                "season_pack_consistency": consistency,
            }), 400

    source_provider = _source_provider_for_share_record(record, 'user_share')
    source_register = _register_share_items_to_center(record, items, cfg, headers, source_provider=source_provider)
    reported = int(source_register.get('reported') or 0)
    errors = list(source_register.get('errors') or [])
    first_source_id = source_register.get('first_source_id') or None

    center_status = 'reported' if reported > 0 and not errors else ('partial' if reported > 0 else 'failed')
    row = shared_share_db.update_share_record(
        record_id,
        center_status=center_status,
        status='reported' if center_status == 'reported' else record.get('status'),
        center_source_id=first_source_id,
        reported_count=reported,
        reported_at='NOW()' if reported > 0 else None,
        last_error='；'.join(errors[:5]),
    )
    shared_credit_db.add_credit_ledger(
        'share_reported_center', 0,
        f"登记中心 {reported}/{len(items)} 条；raw上传 {raw_summary.get('uploaded', 0)} 条，缺失 {raw_summary.get('missing', 0)} 条",
        ref_id=str(record_id), title=record.get('title') or '',
        raw_json={'errors': errors, 'raw_summary': raw_summary, 'source_register': source_register}
    )
    msg = (
        f"已登记 {reported}/{len(items)} 条；"
        f"raw上传 {raw_summary.get('uploaded', 0)} 条，"
        f"已跳过 {raw_summary.get('skipped', 0)} 条，"
        f"缺失 {raw_summary.get('missing', 0)} 条，"
        f"补全大小 {raw_summary.get('size_fixed', 0)} 条"
    )
    if raw_summary.get('errors'):
        errors.extend(raw_summary.get('errors')[:5])
    return jsonify({"success": reported > 0 and not errors, "message": msg, "data": row, "errors": errors, "raw_summary": raw_summary, "source_register": source_register})

@shared_resource_bp.route('/shares/<int:record_id>/upload-rawffprobe', methods=['POST'])
@admin_required
def api_upload_share_raw_ffprobe(record_id):
    record = shared_share_db.get_share_record(record_id)
    if not record:
        return jsonify({"success": False, "message": "分享记录不存在"}), 404
    cfg, headers = _center_headers()
    req = _request_json()
    force = bool(req.get('force', True))
    summary = _upload_share_raw_ffprobe_to_center(record_id, cfg, headers, force=force)
    shared_credit_db.add_credit_ledger(
        'share_raw_uploaded', 0,
        f"上传媒体信息 {summary.get('uploaded', 0)}/{summary.get('total', 0)} 条",
        ref_id=str(record_id), title=record.get('title') or '', raw_json=summary
    )
    ok_count = int(summary.get('uploaded') or 0) + int(summary.get('skipped') or 0)
    msg = (
        f"raw上传 {summary.get('uploaded', 0)} 条，"
        f"已跳过 {summary.get('skipped', 0)} 条，"
        f"缺失 {summary.get('missing', 0)} 条，"
        f"补全大小 {summary.get('size_fixed', 0)} 条"
    )
    return jsonify({"success": ok_count > 0 or summary.get('total', 0) == 0, "message": msg, "data": summary})

@shared_resource_bp.route('/shares/<int:record_id>/cancel', methods=['POST'])
@admin_required
def api_cancel_share(record_id):
    logger.info(f"  ➜ [共享资源] 收到取消分享请求: record_id={record_id}")
    record = shared_share_db.get_share_record(record_id)
    if not record:
        return jsonify({"success": False, "message": "分享记录不存在"}), 404
    client = P115Service.get_client()
    if not client:
        return jsonify({"success": False, "message": "未配置可用的 115 Cookie 客户端"}), 400

    data = _request_json()
    share_code = str(record.get('share_code') or '').strip()
    if not share_code:
        return jsonify({"success": False, "message": "分享码为空，无法取消", "data": record}), 400

    logger.info(f"  ➜ [共享资源] 准备取消/删除115分享: record_id={record_id}, title={record.get('title')}, share_code={share_code}")

    attempts = []

    def _try_cancel(label, func):
        try:
            resp = func()
            attempts.append({'label': label, 'response': resp})
            logger.info(f"  ➜ [共享资源] 取消分享尝试 {label} 返回: {_resp_text(resp)}")
            return resp
        except Exception as e:
            attempts.append({'label': label, 'error': str(e)})
            logger.exception(f"  ➜ [共享资源] 取消分享尝试 {label} 异常")
            return {'state': False, 'error_msg': str(e)}

    # 115 分两层：cancel 只是把分享状态变成“已取消”；delete 才尽量从分享列表里移除。
    cancel_resp = _try_cancel('share_cancel', lambda: client.share_cancel(share_code))
    if not cancel_resp or not cancel_resp.get('state'):
        if hasattr(client, 'share_update'):
            cancel_resp = _try_cancel('share_update_action_cancel', lambda: client.share_update(share_code, action='cancel'))

    delete_resp = None
    if hasattr(client, 'share_delete'):
        delete_resp = _try_cancel('share_delete_after_cancel', lambda: client.share_delete(share_code))

    resp = delete_resp if (delete_resp and delete_resp.get('state')) else cancel_resp

    if not resp or not resp.get('state'):
        text = _resp_text({'last': resp, 'attempts': attempts})
        # 分享已经不存在/已取消时，本地可以安全标记取消，并继续撤销中心源。
        if any(k in text for k in ['分享不存在', '不存在该分享', '已取消', '取消分享', 'share not found', 'not found', '没有该分享']):
            center_result = _cancel_center_sources_for_share(record_id, record)
            center_status = _center_status_after_cancel_result(center_result)
            row = shared_share_db.update_share_record(
                record_id,
                status='cancelled', review_status='cancelled', center_status=center_status,
                cancelled_at='NOW()',
                last_error=f"远端分享已不存在，已同步本地状态；{_center_cancel_result_text(center_result)}"
            )
            shared_credit_db.add_credit_ledger('share_cancelled', 0, '同步已取消/不存在的115分享并撤销中心源', ref_id=str(record_id), title=record.get('title') or '', raw_json={'attempts': attempts, 'center': center_result})
            return jsonify({"success": True, "message": "远端分享已不存在，已同步本地/中心取消状态", "data": row, "debug": attempts, "center": center_result})
        # 调试/抢救用：允许只标记本地，避免界面卡死；默认不开。
        if data.get('force_local'):
            row = shared_share_db.update_share_record(record_id, status='cancel_failed', review_status=record.get('review_status') or '', last_error=f"远端取消失败，仅本地标记: {text}")
            return jsonify({"success": True, "message": "远端取消失败，已仅本地标记为取消失败；分享可能仍然有效", "data": row, "debug": attempts})
        row = shared_share_db.update_share_record(record_id, last_error=f"取消分享失败: {text}")
        logger.warning(f"  ➜ [共享资源] 取消分享最终失败: record_id={record_id}, attempts={text}")
        return jsonify({"success": False, "message": f"取消分享失败: {text}", "data": row, "debug": attempts}), 500

    center_result = _cancel_center_sources_for_share(record_id, record)
    delete_ok = bool(delete_resp and delete_resp.get('state'))
    center_ok = bool(center_result.get('ok'))

    msg_parts = []
    msg_parts.append('115分享已删除' if delete_ok else '115分享已取消，但分享列表删除接口未确认成功')
    if center_result.get('skipped'):
        msg_parts.append(center_result.get('message') or '中心撤销已跳过')
    elif center_ok:
        msg_parts.append(_center_cancel_result_text(center_result))
    else:
        msg_parts.append(f"中心撤销失败: {center_result.get('message')}")
    final_msg = '；'.join([p for p in msg_parts if p])

    update_fields = dict(
        status='cancelled',
        review_status='cancelled',
        cancelled_at='NOW()',
        last_error=final_msg,
    )
    if center_ok or center_result.get('skipped'):
        update_fields['center_status'] = _center_status_after_cancel_result(center_result)
    row = shared_share_db.update_share_record(record_id, **update_fields)
    shared_credit_db.add_credit_ledger('share_cancelled', 0, '手动取消115分享并撤销中心源', ref_id=str(record_id), title=record.get('title') or '', raw_json={'response': resp, 'attempts': attempts, 'center': center_result})
    logger.info(f"  ➜ [共享资源] 已取消/删除115分享: record_id={record_id}, share_code={share_code}, center={center_result}")

    # 撤销中心源成功后，顺手刷新一次贡献值快照；失败不影响取消分享主流程。
    if center_ok:
        try:
            _fetch_center_credit()
        except Exception as e:
            logger.warning(f"  ➜ [共享资源] 取消分享后刷新中心贡献值失败: {e}")

    return jsonify({"success": True, "message": final_msg, "data": row, "debug": attempts, "center": center_result})

@shared_resource_bp.route('/center/device/register', methods=['POST'])
@admin_required
def api_register_center_device():
    """首次连接共享中心：注册设备并写入共享资源独立配置。"""
    data = _request_json()
    cfg = _get_shared_config()

    center_url = str(data.get('center_url') or cfg.get('center_url') or '').strip().rstrip('/')
    if not center_url:
        return jsonify({'success': False, 'message': '共享中心地址未配置'}), 400

    install_id = str(cfg.get('install_id') or '').strip()
    if not install_id:
        install_id = f"etk-{uuid.uuid4().hex}"

    default_name = ''
    try:
        default_name = socket.gethostname() or ''
    except Exception:
        default_name = ''
    if not default_name:
        default_name = f"ETK-{install_id[-6:]}"
    device_name = str(data.get('name') or default_name).strip()[:80]

    try:
        from handler.shared_center_client import SharedCenterClient
        client = SharedCenterClient()
        client.base_url = center_url
        result = client.register_device(
            name=device_name,
            install_id=install_id,
            admin_token=str(data.get('admin_token') or '').strip(),
        )
        device_token = str(result.get('device_token') or '').strip()
        device_id = str(result.get('device_id') or '').strip()
        if not device_token:
            return jsonify({'success': False, 'message': '中心服务器未返回 device_token', 'data': result}), 502

        settings_db.save_shared_resource_config({
            'p115_shared_center_url': center_url,
            'p115_shared_device_token': device_token,
            'p115_shared_install_id': install_id,
            'p115_shared_resource_enabled': True,
        })
        logger.info(f"  ➜ [共享资源] 中心设备注册成功: device_id={device_id or '-'}, center={center_url}")

        credit_result = None
        try:
            credit_result = _fetch_center_credit()
        except Exception as e:
            logger.warning(f"  ➜ [共享资源] 注册后刷新贡献值失败: {e}")

        return jsonify({
            'success': True,
            'message': '中心设备已注册，device_token 已保存到共享资源独立配置',
            'device_id': device_id,
            'device_token_masked': device_token[:8] + '...' + device_token[-6:] if len(device_token) > 16 else '******',
            'data': {'device_id': device_id, 'credit': credit_result},
        })
    except Exception as e:
        logger.error(f"  ➜ [共享资源] 注册中心设备失败: {e}", exc_info=True)
        return jsonify({'success': False, 'message': f'注册中心设备失败: {e}'}), 500

@shared_resource_bp.route('/credit/refresh', methods=['POST'])
@admin_required
def api_refresh_credit():
    try:
        result = _fetch_center_credit()
        if not result.get('ok'):
            return jsonify({"success": False, "message": result.get('message') or '刷新贡献值失败'}), 400
        return jsonify({"success": True, "data": result.get('snapshot')})
    except Exception as e:
        logger.error(f"  ➜ [共享资源] 刷新中心贡献值失败: {e}", exc_info=True)
        return jsonify({"success": False, "message": str(e)}), 500

@shared_resource_bp.route('/credit/ledger', methods=['GET'])
@admin_required
def api_credit_ledger():
    limit = int(request.args.get('limit', 120) or 120)
    sync_center = str(request.args.get('sync_center', '1')).lower() not in ('0', 'false', 'no')
    actual_only = str(request.args.get('actual_only', '1')).lower() not in ('0', 'false', 'no')
    sync_result = None
    if sync_center:
        try:
            sync_result = _fetch_center_credit()
        except Exception as e:
            logger.warning(f"  ➜ [共享资源] 同步中心贡献值流水失败，返回本地缓存: {e}")
    rows = shared_credit_db.list_credit_ledger(limit=limit, actual_only=actual_only)
    return jsonify({"success": True, "items": rows, "sync": sync_result})

# ======================================================================
# 求分享 API：客户端代理中心端 + TMDb 搜索
# ======================================================================
def _share_request_default_param_options() -> Dict[str, Any]:
    return {
        'resolution': [
            {'label': '4K', 'value': '4k'},
            {'label': '1080p', 'value': '1080p'},
            {'label': '720p', 'value': '720p'},
            {'label': '480p', 'value': '480p'},
        ],
        'codec': [
            {'label': 'HEVC', 'value': 'HEVC'},
            {'label': 'H.264', 'value': 'H.264'},
            {'label': 'AV1', 'value': 'AV1'},
            {'label': 'VP9', 'value': 'VP9'},
        ],
        'effect': [
            {'label': 'DoVi P8', 'value': 'DoVi_P8'},
            {'label': 'DoVi P7', 'value': 'DoVi_P7'},
            {'label': 'DoVi P5', 'value': 'DoVi_P5'},
            {'label': 'DoVi', 'value': 'DoVi'},
            {'label': 'HDR10+', 'value': 'HDR10+'},
            {'label': 'HDR', 'value': 'HDR'},
            {'label': 'SDR', 'value': 'SDR'},
        ],
        'frame_rate': [
            {'label': '≥ 60 fps', 'value': '60'},
            {'label': '≥ 50 fps', 'value': '50'},
            {'label': '≥ 30 fps', 'value': '30'},
            {'label': '24 fps', 'value': '24'},
        ],
        'audio': [
            {'label': '国语', 'value': '国语'},
            {'label': '粤语', 'value': '粤语'},
            {'label': '英语', 'value': '英语'},
            {'label': '日语', 'value': '日语'},
            {'label': '韩语', 'value': '韩语'},
        ],
        'subtitle': [
            {'label': '简体', 'value': '简体'},
            {'label': '繁体', 'value': '繁体'},
            {'label': '英文', 'value': '英文'},
            {'label': '日文', 'value': '日文'},
            {'label': '韩文', 'value': '韩文'},
            {'label': '无', 'value': '无'},
        ],
    }

def _share_request_param_options_from_helpers() -> Dict[str, Any]:
    try:
        from tasks.helpers import get_standard_asset_option_values
        options = get_standard_asset_option_values()
        if isinstance(options, dict) and options:
            return options
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 读取 helpers 标准媒体参数失败，使用内置兜底: {e}")
    return _share_request_default_param_options()

@shared_resource_bp.route('/share-requests/param-options', methods=['GET'])
@admin_required
def api_share_request_param_options():
    return jsonify({'success': True, 'data': _share_request_param_options_from_helpers()})

@shared_resource_bp.route('/share-requests/tmdb/search', methods=['GET'])
@admin_required
def api_share_request_tmdb_search():
    keyword = str(request.args.get('keyword') or request.args.get('query') or '').strip()
    if len(keyword) < 1:
        return jsonify({'success': True, 'items': []})
    page = max(1, int(request.args.get('page', 1) or 1))
    api_key = _tmdb_api_key_for_share_request()
    if not api_key:
        return jsonify({'success': False, 'message': '未配置 TMDb API Key，无法搜索求分享目标'}), 400
    try:
        from handler import tmdb as tmdb_handler
        media_type = str(request.args.get('media_type') or request.args.get('type') or 'all').strip().lower()
        if media_type in {'movie', 'film'}:
            data = tmdb_handler.search_media_for_discover(keyword, api_key, item_type='movie', page=page) or {}
            raw_items = [dict(it, media_type='movie') for it in (data.get('results') or [])]
        elif media_type in {'tv', 'series', 'show'}:
            data = tmdb_handler.search_media_for_discover(keyword, api_key, item_type='tv', page=page) or {}
            raw_items = [dict(it, media_type='tv') for it in (data.get('results') or [])]
        else:
            data = tmdb_handler.search_multi_media(keyword, api_key, page=page) or {}
            raw_items = data.get('results') or []
        items = [_normalize_tmdb_search_item(it) for it in raw_items]
        items = [it for it in items if it.get('tmdb_id') and it.get('title')]
        return jsonify({
            'success': True,
            'items': items,
            'total': int(data.get('total_results') or len(items)),
            'page': int(data.get('page') or page),
            'total_pages': int(data.get('total_pages') or 1),
        })
    except Exception as e:
        logger.error(f"  ➜ [共享资源] TMDb 求分享搜索失败: {e}", exc_info=True)
        return jsonify({'success': False, 'message': f'TMDb 搜索失败: {e}'}), 500

@shared_resource_bp.route('/share-requests', methods=['GET'])
@admin_required
def api_list_share_requests():
    try:
        data = _center_json_request('GET', '/api/v1/share-requests', params={
            'status': request.args.get('status', 'open'),
            'keyword': request.args.get('keyword', ''),
            'media_type': request.args.get('media_type', ''),
            'target_type': request.args.get('target_type', ''),
            'limit': int(request.args.get('limit', 50) or 50),
            'offset': int(request.args.get('offset', 0) or 0),
        }, timeout=25)
        if any(bool(item.get('joined_by_me')) and item.get('status') == 'open' for item in (data.get('items') or [])):
            _ensure_share_request_listener_async()
        return jsonify({'success': True, 'items': data.get('items') or [], 'total': int(data.get('total') or 0)})
    except Exception as e:
        logger.error(f"  ➜ [共享资源] 拉取求分享列表失败: {e}", exc_info=True)
        return jsonify({'success': False, 'message': f'拉取求分享列表失败: {e}'}), 500

def _enrich_share_request_payload_for_quote(payload: Dict[str, Any]) -> Dict[str, Any]:
    """本地代理侧为求分享报价补充可验证的 TMDb 元数据。

    全剧求分享的基准悬赏必须以 TMDb 为准，而不是以本地 media_metadata 已入库季数为准。
    否则求片人本地一集都没有时，全剧基准会被低估成 1 季甚至 0 季；
    本地库只负责“我有资源”候选匹配，不参与需求侧计价。
    """
    data = dict(payload or {})
    target_type = str(data.get('target_type') or '').strip().lower()
    media_type = str(data.get('media_type') or '').strip().lower()
    if target_type not in {'series', 'tv'} and not (media_type in {'tv', 'series'} and target_type in {'', 'series'}):
        return data

    # 前端/TMDb 选择结果如果已经带了可信季数，也可以直接使用。
    # 但不要用本地 media_metadata 的季数覆盖它。
    if _safe_int(data.get('season_count') or data.get('number_of_seasons'), 0) > 0:
        return data

    tmdb_id = str(data.get('tmdb_id') or '').strip()
    if not tmdb_id:
        return data

    api_key = _tmdb_api_key_for_share_request()
    if not api_key:
        return data

    tmdb_season_numbers = []
    try:
        from handler import tmdb as tmdb_handler
        details = tmdb_handler.get_tv_details(int(tmdb_id), api_key, append_to_response='seasons') or {}
        for season in (details.get('seasons') or []):
            if not isinstance(season, dict):
                continue
            try:
                sn = int(season.get('season_number'))
            except Exception:
                continue
            if sn <= 0:
                # TMDb season_number=0 是 Specials，不计入全剧基础季数。
                continue
            episode_count = season.get('episode_count')
            try:
                episode_count_int = int(episode_count) if episode_count not in (None, '') else None
            except Exception:
                episode_count_int = None
            if episode_count_int is not None and episode_count_int <= 0:
                # TMDb 有时会提前建“空壳季”（正季但 0 集），这种季别人永远无法分享，不能计入全剧悬赏和季数校验。
                continue
            if sn not in tmdb_season_numbers:
                tmdb_season_numbers.append(sn)

        if tmdb_season_numbers:
            tmdb_season_numbers.sort()
            data['season_count'] = len(tmdb_season_numbers)
            data['season_numbers'] = tmdb_season_numbers
            return data

        # 只有 seasons 列表不可用/没有 episode_count 信息时才兜底 number_of_seasons；
        # 正常情况下不能用它覆盖上面的真实正季列表，因为它可能把 0 集空壳季也算进去。
        number_of_seasons = _safe_int(details.get('number_of_seasons'), 0)
        if number_of_seasons > 0:
            data['season_count'] = number_of_seasons
            data['season_numbers'] = list(range(1, number_of_seasons + 1))
            return data
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 按 TMDb 补充求分享全剧季数失败: tmdb={tmdb_id}, err={e}")

    # TMDb 拉取失败时保持 payload 原样，由中心端按最低 1 季兜底；
    # 不再使用本地 media_metadata 季数，避免需求侧计价被本地库污染。
    return data

@shared_resource_bp.route('/share-requests/quote', methods=['POST'])
@admin_required
def api_quote_share_request():
    try:
        payload = _enrich_share_request_payload_for_quote(_request_json())
        data = _center_json_request('POST', '/api/v1/share-requests/quote', json_body=payload, timeout=20)
        return jsonify({'success': True, 'data': data})
    except Exception as e:
        return jsonify({'success': False, 'message': f'求分享报价失败: {e}'}), 400

@shared_resource_bp.route('/share-requests', methods=['POST'])
@admin_required
def api_create_share_request():
    try:
        payload = _enrich_share_request_payload_for_quote(_request_json())
        data = _center_json_request('POST', '/api/v1/share-requests', json_body=payload, timeout=30)
        _fetch_center_credit()
        _ensure_share_request_listener_async()
        return jsonify({'success': True, 'message': '求分享已发布，贡献值已冻结', 'data': data})
    except Exception as e:
        logger.error(f"  ➜ [共享资源] 创建求分享失败: {e}", exc_info=True)
        return jsonify({'success': False, 'message': f'创建求分享失败: {e}'}), 400

@shared_resource_bp.route('/share-requests/<group_id>/co-request', methods=['POST'])
@admin_required
def api_co_request_share(group_id):
    try:
        data = _center_json_request('POST', f'/api/v1/share-requests/{group_id}/co-request', json_body=_request_json(), timeout=25)
        _fetch_center_credit()
        _ensure_share_request_listener_async()
        return jsonify({'success': True, 'message': data.get('message') or '同求成功，贡献值已冻结', 'data': data})
    except Exception as e:
        logger.error(f"  ➜ [共享资源] 同求失败: {e}", exc_info=True)
        return jsonify({'success': False, 'message': f'同求失败: {e}'}), 400

@shared_resource_bp.route('/share-requests/<group_id>/cancel', methods=['POST'])
@admin_required
def api_cancel_share_request(group_id):
    try:
        data = _center_json_request('POST', f'/api/v1/share-requests/{group_id}/cancel', json_body=_request_json(), timeout=25)
        _fetch_center_credit()
        return jsonify({'success': True, 'message': data.get('message') or '已取消求分享', 'data': data})
    except Exception as e:
        logger.error(f"  ➜ [共享资源] 取消求分享失败: {e}", exc_info=True)
        return jsonify({'success': False, 'message': f'取消求分享失败: {e}'}), 400

# ======================================================================
# 共享中心资源库 / 维护任务 API（v8.1）
# ======================================================================
def _center_format_rate(value) -> str:
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

def _center_codec_label(codec: str) -> str:
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
        # DoviProfile81 -> P8.1, DoviProfile8 -> P8
        m = re.search(r'DoviProfile(\d+)', ev_sub, re.IGNORECASE)
        if m:
            raw = m.group(1)
            profile = f"P{raw[0]}.{raw[1:]}" if len(raw) > 1 else f"P{raw}"
        elif ev_desc:
            m = re.search(r'Profile\s*([0-9.]+)', ev_desc, re.IGNORECASE)
            if m:
                profile = f"P{m.group(1)}"
        base = f"Dolby Vision {profile}".strip()
        # Profile 8.1 这类兼容 HDR10，展示更完整一些。
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
    """懒加载 formatter，避免 routes 导入时和 p115_service / analyzer 互相循环。"""
    global _CENTER_MEDIAINFO_FORMATTER
    if _CENTER_MEDIAINFO_FORMATTER is not None:
        return _CENTER_MEDIAINFO_FORMATTER
    from handler.p115_media_analyzer import P115MediaAnalyzerMixin

    class _Formatter(P115MediaAnalyzerMixin):
        def __init__(self):
            try:
                from database import settings_db
                import utils
                self.language_map = settings_db.get_setting('language_mapping') or utils.DEFAULT_LANGUAGE_MAPPING
                self.stream_feature_map = settings_db.get_setting('stream_feature_mapping') or getattr(utils, 'DEFAULT_STREAM_FEATURE_MAPPING', [])
            except Exception:
                self.language_map = []
                self.stream_feature_map = []

    _CENTER_MEDIAINFO_FORMATTER = _Formatter()
    return _CENTER_MEDIAINFO_FORMATTER

def _build_center_emby_info(raw: Dict[str, Any], source: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(raw, dict) or not raw:
        return {}

    # 如果中心以后直接返回 Emby MediaSourceInfo，也能兼容。
    if raw.get('MediaSourceInfo'):
        return raw.get('MediaSourceInfo') or {}
    if raw.get('MediaStreams'):
        return raw

    file_node = {
        'fn': source.get('file_name') or source.get('title') or source.get('sha1') or 'unknown.mkv',
        'n': source.get('file_name') or source.get('title') or source.get('sha1') or 'unknown.mkv',
        'fs': _safe_size_bytes(source.get('size') or (raw.get('format') or {}).get('size')),
        'size': _safe_size_bytes(source.get('size') or (raw.get('format') or {}).get('size')),
        'sha1': source.get('sha1') or '',
    }
    metadata_context = {
        'tmdb_id': source.get('tmdb_id'),
        'item_type': source.get('item_type'),
        'type': source.get('item_type'),
    }
    try:
        formatter = _get_center_mediainfo_formatter()
        if not hasattr(formatter, '_build_emby_mediainfo_from_ffprobe'):
            return {}
        built = formatter._build_emby_mediainfo_from_ffprobe(
            raw,
            file_node,
            sha1=source.get('sha1') or '',
        )
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
    try:
        size = _safe_size_bytes(size)
    except Exception:
        size = 0

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
        # 轻量兜底：raw 没法被 formatter 接管时仍尽量展示基础参数。
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

    return {
        'resolution': _center_resolution(width, height),
        'width': width,
        'height': height,
        'video_codec': codec,
        'codec': codec,  # 兼容旧前端字段
        'effect': effect,
        'bit_depth': bit_depth,
        'fps': fps_text,
        'bitrate': bitrate,
        'container': media_info.get('Container') or '',
        'video_display': video_display,
        'size': size,
        'size_gb': round(size / 1024 / 1024 / 1024, 2) if size else 0,
        'audio_count': len(audios),
        'subtitle_count': len(subs),
        'audio_list': audio_list[:16],
        'subtitle_list': subtitle_list[:24],
        # 兼容旧字段，避免其他地方还在读 audios/subtitles。
        'audios': [{'display': x} for x in audio_list[:16]],
        'subtitles': [{'display': x} for x in subtitle_list[:24]],
        'formatted_by': 'emby_mediainfo' if media_info else 'raw_fallback',
    }

def _center_norm_item_type(value: str) -> str:
    """把中心/本地历史遗留 item_type 归一化到展示用三类。"""
    return str(value or '').strip().lower().replace('-', '_').replace(' ', '_')

def _center_is_movie_row(item: Dict[str, Any]) -> bool:
    t = _center_norm_item_type(item.get('item_type'))
    share_type = _center_norm_item_type(item.get('share_type'))
    return t in {'movie', 'movies', 'film', 'movie_file', 'movie_folder'} or share_type in {'movie_file', 'movie_folder'}

def _center_is_episode_row(item: Dict[str, Any]) -> bool:
    t = _center_norm_item_type(item.get('item_type'))
    if t in {'episode', 'episodes', 'episode_file'}:
        return True
    return item.get('episode_number') not in [None, ''] and not _center_is_movie_row(item)

def _center_is_pack_like_row(item: Dict[str, Any]) -> bool:
    t = _center_norm_item_type(item.get('item_type'))
    share_type = _center_norm_item_type(item.get('share_type'))

    # 只有中心明确折叠后的行，或来源语义明确是季包/剧集包时，才按 Pack。
    # v3 曾把 single_rows 也带上的 pack_item_count=1 当成 Pack，导致电影显示成“1集”，
    # 单集也被误折叠成多版本。这里不能用 pack_item_count 作为首要判断。
    if item.get('is_collapsed_pack') is True:
        return True
    if t in {'season', 'seasons', 'season_pack', 'series', 'series_pack', 'tv', 'show'}:
        return True
    if share_type in {'season_pack', 'series_pack', 'tv_pack'}:
        return True

    # 历史/外部数据 item_type 可能为空；只有在没有电影/单集语义、且确实包含多文件时，
    # 才把 pack_item_count 作为兜底包判断。
    try:
        pack_count = int(item.get('pack_item_count') or 0)
    except Exception:
        pack_count = 0
    if pack_count > 1 and t not in {'movie', 'movies', 'film', 'movie_file', 'movie_folder', 'episode', 'episodes', 'episode_file'}:
        return True
    return False

def _center_created_ts(item: Dict[str, Any]) -> float:
    value = (item or {}).get('created_at')
    if not value:
        return 0.0
    if hasattr(value, 'timestamp'):
        try:
            return float(value.timestamp())
        except Exception:
            return 0.0
    text = str(value).strip()
    if text.endswith('Z'):
        text = text[:-1] + '+00:00'
    try:
        return float(datetime.fromisoformat(text).timestamp())
    except Exception:
        return 0.0

def _center_infer_episode_number(item: Dict[str, Any]):
    """从中心源记录里尽量推断单集集号，用于修复历史误登记的“假剧集包”。"""
    item = item or {}
    value = item.get('episode_number')
    if value not in (None, ''):
        try:
            return int(value)
        except Exception:
            return value
    for key in ('file_name', 'relative_path', 'title', 'root_name'):
        ep = _guess_episode_number(str(item.get(key) or ''))
        if ep is not None:
            return ep
    raw = item.get('raw_ffprobe_json')
    if isinstance(raw, dict):
        etk = raw.get('_etk') if isinstance(raw.get('_etk'), dict) else {}
        for key in ('episode_number', 'episode'):
            if etk.get(key) not in (None, ''):
                try:
                    return int(etk.get(key))
                except Exception:
                    return etk.get(key)
    return None

def _center_mark_as_episode_row(item: Dict[str, Any], episode_number=None) -> Dict[str, Any]:
    row = dict(item or {})
    ep = episode_number if episode_number not in (None, '') else _center_infer_episode_number(row)
    if ep not in (None, ''):
        row['episode_number'] = ep
    row['item_type'] = 'Episode'
    row['share_type'] = 'episode_file'
    row['display_type'] = 'Episode'
    row['is_collapsed_pack'] = False
    row.pop('pack_item_count', None)
    row.pop('pack_source_ids', None)
    row.pop('pack_episode_numbers', None)
    row.pop('pack_tmdb_ids', None)
    return row

def _center_display_type(item: Dict[str, Any]) -> str:
    """中心资源库只暴露三类：Movie / Pack / Episode。"""
    if not item:
        return 'Unknown'

    # 判断优先级必须是：电影 > 单集 > 明确剧集包。
    # single_rows 会携带 pack_item_count=1 给前端做文件状态标记，不能因此覆盖 Movie/Episode。
    if _center_is_movie_row(item):
        return 'Movie'
    if _center_is_episode_row(item):
        return 'Episode'
    if item.get('is_collapsed_pack') or _center_is_pack_like_row(item):
        return 'Pack'

    # 兜底：有季号没集号，多半是季包；否则按原类型保留。
    if item.get('season_number') not in [None, ''] and item.get('episode_number') in [None, '']:
        return 'Pack'
    t = _center_norm_item_type(item.get('item_type'))
    if t == 'movie':
        return 'Movie'
    return 'Unknown'

def _center_match_display_type(item: Dict[str, Any], wanted: str) -> bool:
    wanted = str(wanted or '').strip().lower()
    if not wanted or wanted in {'all', '全部类型'}:
        return True
    dtype = _center_display_type(item).lower()
    alias = {
        'movie': 'movie', 'movies': 'movie', '电影': 'movie',
        'pack': 'pack', 'season': 'pack', 'series': 'pack', 'tv': 'pack', 'season_pack': 'pack', 'series_pack': 'pack', '剧集包': 'pack', '季': 'pack', '剧集': 'pack',
        'episode': 'episode', 'episodes': 'episode', 'episode_file': 'episode', 'single': 'episode', '单集': 'episode',
    }
    return alias.get(wanted, wanted) == alias.get(dtype, dtype)

def _collapse_center_season_pack_rows(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """中心资源库展示层去重：同一分享码下的一组剧集文件视作一个“剧集包”。

    展示模型统一成三类：电影、剧集包、单集。
    - Movie/movie_file/movie_folder 永远按电影展示；
    - 同一个 contributor + share_code + season 下存在多条剧集/分集记录时折叠为剧集包；
    - Season/Series/tv/season_pack 只有在同分享码下确实包含多集，或无法推断为单集时，才按剧集包展示；
    - 历史误登记的“每个 share_code 只有一个文件、file_name 可推断集号”的 Season 行，按 Episode 单集展示。
    """
    groups: Dict[str, List[Dict[str, Any]]] = {}
    passthrough: List[Dict[str, Any]] = []

    def _norm_season(value):
        if value in [None, '']:
            return ''
        try:
            return str(int(value))
        except Exception:
            return str(value).strip()

    def _group_key(item: Dict[str, Any]) -> str:
        return '|'.join([
            str(item.get('contributor_id') or ''),
            str(item.get('share_code') or '').strip(),
            _norm_season(item.get('season_number')),
        ])

    for item in items or []:
        if _center_is_movie_row(item):
            row = dict(item)
            row['display_type'] = 'Movie'
            passthrough.append(row)
            continue

        share_code = str(item.get('share_code') or '').strip()
        season = _norm_season(item.get('season_number'))
        if share_code and season and (_center_is_episode_row(item) or _center_is_pack_like_row(item)):
            groups.setdefault(_group_key(item), []).append(item)
        else:
            row = dict(item)
            row['display_type'] = _center_display_type(row)
            passthrough.append(row)

    collapsed: List[Dict[str, Any]] = []
    for rows in groups.values():
        # 多条同 share_code + season 的源才是“包”。单条源如果能从文件名/raw 推断集号，
        # 多半是旧版本把 episode_file 错登记成 Season，展示层先按单集兜底，避免“每集都是剧集包”。
        if len(rows) <= 1:
            row = dict(rows[0])
            inferred_ep = _center_infer_episode_number(row)
            if inferred_ep not in (None, '') and _center_norm_item_type(row.get('source_provider')) != 'season_pack':
                passthrough.append(_center_mark_as_episode_row(row, inferred_ep))
            else:
                row['display_type'] = 'Pack' if _center_is_pack_like_row(row) else 'Episode'
                passthrough.append(row)
            continue

        rows_sorted = sorted(rows, key=lambda r: (1 if r.get('raw_ffprobe_json') else 0, _safe_size_bytes(r.get('size'))), reverse=True)
        rep = dict(rows_sorted[0])
        newest_row = max(rows, key=_center_created_ts)
        if newest_row.get('created_at'):
            rep['created_at'] = newest_row.get('created_at')
        total_size = 0
        total_success = 0
        episode_numbers = []
        source_ids = []
        tmdb_ids = []
        pack_items = []
        has_any_episode = False

        for r in rows:
            try:
                total_size += _safe_size_bytes(r.get('size'))
            except Exception:
                pass
            total_success += int(r.get('success_count') or 0)
            sid = r.get('source_id')
            if sid:
                source_ids.append(sid)
            if r.get('tmdb_id') not in [None, '']:
                tmdb_ids.append(str(r.get('tmdb_id')))
            pack_items.append({
                'source_id': r.get('source_id'),
                'sha1': str(r.get('sha1') or '').strip().upper(),
                'file_name': r.get('file_name') or r.get('title') or '',
                'relative_path': r.get('relative_path') or '',
                'tmdb_id': r.get('tmdb_id'),
                'item_type': r.get('item_type'),
                'season_number': r.get('season_number'),
                'episode_number': r.get('episode_number'),
                'size': r.get('size'),
            })
            try:
                ep = r.get('episode_number')
                if ep is not None and ep != '':
                    episode_numbers.append(int(ep))
                    has_any_episode = True
            except Exception:
                pass

        unique_source_ids = list(dict.fromkeys([x for x in source_ids if x]))
        unique_tmdb_ids = list(dict.fromkeys(tmdb_ids))
        unique_eps = sorted(set(episode_numbers))

        rep['display_type'] = 'Pack'
        rep['item_type'] = 'Season'
        rep['episode_number'] = None
        rep['pack_item_count'] = len(rows)
        rep['pack_episode_numbers'] = unique_eps
        rep['pack_source_ids'] = unique_source_ids
        rep['pack_tmdb_ids'] = unique_tmdb_ids
        rep['pack_items'] = pack_items
        rep['share_type'] = 'season_pack'
        rep['is_collapsed_pack'] = True
        rep['success_count'] = total_success
        if not has_any_episode and not unique_eps:
            rep['pack_note'] = f"同一分享码下 {len(rows)} 个文件"

        if total_size > 0:
            rep['size'] = total_size
            if isinstance(rep.get('version_summary'), dict):
                rep['version_summary']['size'] = total_size
                rep['version_summary']['size_gb'] = round(total_size / 1024 / 1024 / 1024, 2)

        collapsed.append(rep)

    return passthrough + collapsed

def _expand_center_pack_page_items(client, items: List[Dict[str, Any]], status: str = CENTER_DISPLAY_SOURCE_STATUSES) -> List[Dict[str, Any]]:
    """分页展示前补全同一分享码的剧集包条目，避免 30 条分页把 36 集包截成 30 集。"""
    items = list(items or [])
    if not client or not items:
        return items

    def _norm_season(value):
        if value in [None, '']:
            return ''
        try:
            return str(int(value))
        except Exception:
            return str(value).strip()

    group_counts: Dict[str, int] = {}
    code_by_key: Dict[str, str] = {}
    for item in items:
        if _center_is_movie_row(item):
            continue
        share_code = str(item.get('share_code') or '').strip()
        season = _norm_season(item.get('season_number'))
        if not share_code or not season:
            continue
        if not (_center_is_episode_row(item) or _center_is_pack_like_row(item)):
            continue
        key = '|'.join([str(item.get('contributor_id') or ''), share_code, season])
        group_counts[key] = group_counts.get(key, 0) + 1
        code_by_key[key] = share_code

    share_codes = []
    for key, count in group_counts.items():
        # 多条同包分集必须补全；Season/Series 单条也尽量补全，避免只拿到代表目录时看不到全集。
        code = code_by_key.get(key)
        if code and code not in share_codes and count >= 1:
            share_codes.append(code)

    if not share_codes:
        return items

    share_codes = share_codes[:8]
    by_id: Dict[str, Dict[str, Any]] = {}
    ordered_ids: List[str] = []

    def _put(row: Dict[str, Any], prefer_existing_raw: bool = True):
        sid = str((row or {}).get('source_id') or '').strip()
        if not sid:
            return
        if sid not in by_id:
            by_id[sid] = dict(row or {})
            ordered_ids.append(sid)
            return
        old = by_id[sid]
        merged = dict(row or {})
        if prefer_existing_raw and old.get('raw_ffprobe_json') and not merged.get('raw_ffprobe_json'):
            merged['raw_ffprobe_json'] = old.get('raw_ffprobe_json')
        for k in ('version_summary', '_local_share_record_exists'):
            if k in old and k not in merged:
                merged[k] = old[k]
        by_id[sid] = merged

    for item in items:
        _put(item, prefer_existing_raw=True)

    for code in share_codes:
        try:
            res = client.list_sources(
                q=code,
                status=status or CENTER_DISPLAY_SOURCE_STATUSES,
                limit=500,
                offset=0,
                include_raw=False,
            )
            fetched = res.get('items') or []
            matched = 0
            for row in fetched:
                if str(row.get('share_code') or '').strip() != code:
                    continue
                _put(row, prefer_existing_raw=True)
                matched += 1
            total = int(res.get('total') or matched or 0)
            if total > matched:
                logger.warning("  ➜ [共享资源] 分享码 %s 可能超过 500 条，当前仅补全 %s/%s 条。", code, matched, total)
            logger.trace("  ➜ [共享资源] 剧集包分享码 %s 分页补全 %s 条。", code, matched)
        except Exception as e:
            logger.warning(f"  ➜ [共享资源] 剧集包分页补全失败 share_code={code}: {e}")

    return [by_id[sid] for sid in ordered_ids if sid in by_id]

def _center_norm_sha1(value: str) -> str:
    text = str(value or '').strip().upper()
    return text if re.fullmatch(r'[A-F0-9]{40}', text) else ''

def _center_row_file_entries(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    """把中心展示行拆成文件级条目，用于本地入库状态判断。"""
    row = row or {}
    candidates = row.get('pack_items') if isinstance(row.get('pack_items'), list) else None
    if not candidates:
        candidates = [row]

    entries = []
    seen = set()
    for item in candidates or []:
        if not isinstance(item, dict):
            continue
        sha1 = _center_norm_sha1(item.get('sha1'))
        name = str(item.get('file_name') or item.get('relative_path') or item.get('title') or sha1 or '').strip()
        key = sha1 or f"{item.get('source_id') or ''}|{name}|{item.get('episode_number') or ''}"
        if key in seen:
            continue
        seen.add(key)
        entries.append({
            'source_id': item.get('source_id'),
            'sha1': sha1,
            'file_name': name,
            'relative_path': item.get('relative_path') or '',
            'season_number': item.get('season_number'),
            'episode_number': item.get('episode_number'),
            'size': item.get('size'),
        })

    # 如果折叠包只有代表行，但中心给了 pack_item_count，至少保留总数，避免误判为 1/1。
    expected = _safe_int(row.get('pack_item_count'), 0)
    if expected > len(entries):
        for idx in range(len(entries), expected):
            entries.append({
                'source_id': '',
                'sha1': '',
                'file_name': f'未知文件 #{idx + 1}',
                'relative_path': '',
                'season_number': row.get('season_number'),
                'episode_number': None,
                'size': 0,
            })
    return entries

def _center_file_entry_label(entry: Dict[str, Any]) -> str:
    entry = entry or {}
    season = entry.get('season_number')
    episode = entry.get('episode_number')
    prefix = ''
    try:
        if season not in (None, '') and episode not in (None, ''):
            prefix = f"S{int(season):02d}E{int(episode):02d} "
        elif season not in (None, ''):
            prefix = f"S{int(season):02d} "
    except Exception:
        pass
    return (prefix + str(entry.get('file_name') or entry.get('sha1') or '未知文件')).strip()

def _annotate_center_rows_local_library(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """为中心展示行补充文件级/包级本地入库状态。"""
    rows = list(rows or [])
    all_sha1s = []
    row_entries = []
    for row in rows:
        entries = _center_row_file_entries(row)
        row_entries.append(entries)
        all_sha1s.extend([e.get('sha1') for e in entries if e.get('sha1')])

    local_index = shared_share_db.load_local_library_sha1_index(all_sha1s)

    for row, entries in zip(rows, row_entries):
        files = []
        hit_count = 0
        known_count = 0
        for entry in entries:
            sha1 = entry.get('sha1') or ''
            hit = local_index.get(sha1) if sha1 else None
            media_hits = (hit or {}).get('media_metadata') or []
            p115_hits = (hit or {}).get('p115_filesystem_cache') or []
            is_in = bool(media_hits or p115_hits)
            if sha1:
                known_count += 1
            if is_in:
                hit_count += 1
            files.append({
                **entry,
                'label': _center_file_entry_label(entry),
                'in_library': is_in,
                'library_sources': [x for x in [
                    'media_metadata' if media_hits else '',
                    'p115_filesystem_cache' if p115_hits else '',
                ] if x],
                'media_hit_count': len(media_hits),
                'p115_hit_count': len(p115_hits),
            })

        total = max(len(entries), _safe_int(row.get('pack_item_count'), 0), 1)
        unknown_count = max(0, total - known_count)
        missing_count = max(0, total - hit_count)

        if known_count <= 0:
            status = 'unknown'
            label = '无法判断'
            tag_type = 'default'
        elif hit_count >= total and unknown_count == 0:
            status = 'full'
            label = '已入库' if total <= 1 else f'已入库 {hit_count}/{total}'
            tag_type = 'success'
        elif hit_count > 0:
            status = 'partial'
            label = f'部分入库 {hit_count}/{total}'
            tag_type = 'warning'
        else:
            status = 'none'
            label = '未入库' if total <= 1 else f'未入库 0/{total}'
            tag_type = 'default'

        row['local_library'] = {
            'status': status,
            'label': label,
            'tag_type': tag_type,
            'hit_count': hit_count,
            'known_count': known_count,
            'unknown_count': unknown_count,
            'missing_count': missing_count,
            'total_count': total,
            'is_fully_in_library': status == 'full',
            'is_not_fully_in_library': status != 'full',
            'files': files[:200],
        }
    return rows

def _load_center_sources_for_display(client, *, keyword: str = '', tmdb_id: str = '', display_type: str = '', status: str = CENTER_DISPLAY_SOURCE_STATUSES, order_by: str = 'latest', limit: int = 30, offset: int = 0, local_filter: str = '') -> Dict[str, Any]:
    """按展示口径加载中心资源库。

    新中心优先走 /api/v1/sources/display-list：分页、排序、剧集包聚合都在中心端完成，
    本地只补当前页的本地入库状态。旧中心没有该接口时，自动回退到旧的本地扫描折叠逻辑。
    local_filter 已废弃：中心端不知道本地 media_metadata / p115_filesystem_cache，前端也不再提供“只看未入库”。
    """
    limit = max(1, min(int(limit or 30), 100))
    offset = max(0, int(offset or 0))

    # 新接口：中心直接返回展示行 + summary_json，不再补拉完整 raw_ffprobe_json。
    try:
        cfg = _get_shared_config()
        if cfg.get('center_url') and cfg.get('device_token'):
            resp = requests.get(
                f"{cfg['center_url']}/api/v1/sources/display-list",
                headers=_center_headers_for_cfg(cfg),
                params={
                    'q': keyword or '',
                    'tmdb_id': tmdb_id or '',
                    'item_type': display_type or '',
                    'status': status or CENTER_DISPLAY_SOURCE_STATUSES,
                    'order_by': order_by or 'latest',
                    'limit': limit,
                    'offset': offset,
                },
                **_center_request_kwargs(30),
            )
            if resp.ok:
                data = resp.json() or {}
                page_rows = list(data.get('items') or [])
                page_rows = _annotate_center_rows_local_library(page_rows)
                return {
                    'items': page_rows,
                    'total': int(data.get('total') or len(page_rows)),
                    'raw_total': int(data.get('raw_total') or data.get('total') or len(page_rows)),
                    'scanned_raw': int(data.get('scanned_raw') or len(page_rows)),
                }
            if resp.status_code not in (404, 405):
                logger.debug(f"  ➜ [共享资源] 中心 display-list 返回异常，回退旧列表逻辑: HTTP {resp.status_code} {resp.text[:160]}")
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 中心 display-list 不可用，回退旧列表逻辑: {e}")

    # 旧中心兼容逻辑：仍然只用无 raw 原始窗口做折叠；不再为了列表展示补拉完整 RAW。
    target_count = offset + limit
    raw_rows: List[Dict[str, Any]] = []
    raw_total = None
    raw_offset = 0
    raw_page_size = 500
    max_scan = 3000
    display_rows: List[Dict[str, Any]] = []

    while raw_offset < max_scan:
        res = client.list_sources(
            q=keyword or '',
            tmdb_id=tmdb_id or '',
            item_type='',
            status=status or CENTER_DISPLAY_SOURCE_STATUSES,
            order_by=order_by,
            limit=raw_page_size,
            offset=raw_offset,
            include_raw=False,
        )
        page_items = list(res.get('items') or [])
        if raw_total is None:
            try:
                raw_total = int(res.get('total') or 0)
            except Exception:
                raw_total = 0
        if not page_items:
            break

        raw_rows.extend(page_items)
        expanded = _expand_center_pack_page_items(client, raw_rows, status=status)
        collapsed = _collapse_center_season_pack_rows(expanded)
        display_rows = [r for r in collapsed if _center_match_display_type(r, display_type)]
        if order_by == 'popular':
            display_rows.sort(key=lambda r: (int(r.get('success_count') or 0), _center_created_ts(r)), reverse=True)
        elif order_by == 'name':
            display_rows.sort(key=lambda r: (str(r.get('title') or ''), -_center_created_ts(r)))
        elif order_by == 'size':
            display_rows.sort(key=lambda r: (_safe_size_bytes(r.get('size')), _center_created_ts(r)), reverse=True)
        else:
            display_rows.sort(key=lambda r: (_center_created_ts(r), str(r.get('source_id') or '')), reverse=True)

        if len(display_rows) >= target_count:
            break
        raw_offset += len(page_items)
        if raw_total is not None and raw_offset >= raw_total:
            break
        if len(page_items) < raw_page_size:
            break

    display_total = len(display_rows)
    page_rows = display_rows[offset:offset + limit]
    page_rows = _annotate_center_rows_local_library(page_rows)

    return {
        'items': page_rows,
        'total': display_total,
        'raw_total': raw_total if raw_total is not None else len(raw_rows),
        'scanned_raw': len(raw_rows),
    }

def _center_episode_hidden_by_config() -> bool:
    try:
        return bool(settings_db.get_shared_resource_config().get('p115_shared_disable_episode_transfer', False))
    except Exception:
        return False

def _filter_center_rows_by_episode_policy(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not _center_episode_hidden_by_config():
        return list(rows or [])
    filtered = []
    blocked = 0
    for row in rows or []:
        dtype = str(_center_display_type(row) or '').strip().lower()
        item_type = str((row or {}).get('item_type') or '').strip().lower()
        if dtype == 'episode' or item_type == 'episode':
            blocked += 1
            continue
        filtered.append(row)
    if blocked:
        logger.info(f"  ➜ [共享资源] 中心资源库已按配置隐藏单集资源 {blocked} 条。")
    return filtered

_CENTER_STATUS_LABELS = {
    'alive': ('可用', 'success'),
    'pending': ('待验证', 'warning'),
    CENTER_SOURCE_STATUS_REPLENISH: ('待补充', 'error'),
    'dead': ('失效', 'error'),
    'rejected': ('已拒绝', 'error'),
    'expired': ('已过期', 'default'),
    'cancelled': ('已撤销', 'default'),
}

_CENTER_SOURCE_PROVIDER_LABELS = {
    'user_share': '用户主动分享',
    'manual_share': '用户主动分享',
    'auto_gap_share': '本机缺口自动分享',
    'request_share': '求分享响应',
    'hdhive': '影巢外来分享',
    'tg_channel': 'TG频道外来分享',
    'tg_channel_hdhive': 'TG频道影巢外来分享',
    'backup_mirror': '备份分享',
    'backup_share': '备份分享',
    'auto_backup_share': '备份分享',
}

def _load_local_share_code_set(items: List[Dict[str, Any]]) -> set:
    try:
        return shared_share_db.get_existing_share_code_set(items)
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 查询本地分享来源失败: {e}")
        return set()

def _decorate_center_source_row(item: Dict[str, Any]) -> Dict[str, Any]:
    """给中心资源库行补充面向前端的可读来源/状态。"""
    item = item or {}
    # 本机能命中 media_metadata 时，用标准片名/年份修正中心历史标题；远端未知来源则保持中心返回值。
    try:
        identity = _standard_media_identity_for_share(item)
        if identity.get('title'):
            item['title'] = identity.get('title')
        if identity.get('release_year') and not item.get('release_year'):
            item['release_year'] = identity.get('release_year')
        if identity.get('parent_series_tmdb_id'):
            item['parent_series_tmdb_id'] = identity.get('parent_series_tmdb_id')
    except Exception:
        pass
    status = str(item.get('status') or '').strip()
    status_text, status_type = _CENTER_STATUS_LABELS.get(status, (status or '未知', 'default'))
    provider = str(item.get('source_provider') or '').strip() or 'user_share'
    provider_label = _CENTER_SOURCE_PROVIDER_LABELS.get(provider, provider or '未知来源')
    is_mine = bool(item.get('is_mine'))

    local_share_exists = bool(item.get('_local_share_record_exists'))
    if is_mine:
        if provider in ('hdhive', 'tg_channel', 'tg_channel_hdhive'):
            scope_label = '本机外来转存'
        elif provider == 'auto_gap_share':
            scope_label = '本机自动补缺'
        elif provider in ('user_share', 'manual_share') and not local_share_exists:
            # v8.4 之前自动登记的影巢/TG 外来分享没有 source_provider，
            # 但本地“我的分享”没有对应 share_code，可以按历史外来分享兜底展示。
            provider_label = '历史外来分享'
            scope_label = '本机外来转存'
        else:
            scope_label = '本机用户分享'
    else:
        scope_label = '其他设备共享'

    item['status_label'] = status_text
    item['status_type'] = status_type
    item['source_provider'] = provider
    item['source_provider_label'] = provider_label
    item['source_scope_label'] = scope_label
    item['source_label'] = f"{scope_label} · {provider_label}" if provider_label not in scope_label else scope_label
    return item

@shared_resource_bp.route('/center/sources', methods=['GET'])
@admin_required
def api_center_sources():
    """前端中心资源库：按电影 / 剧集包 / 单集三类展示中心已有共享源。"""
    try:
        from handler.shared_center_client import SharedCenterClient
        client = SharedCenterClient()
        if not client.ready:
            return jsonify({'success': False, 'message': '共享中心地址或 device_token 未配置'}), 400

        page_data = _load_center_sources_for_display(
            client,
            keyword=request.args.get('keyword', ''),
            tmdb_id=request.args.get('tmdb_id', ''),
            display_type=request.args.get('item_type', ''),
            status=request.args.get('status', CENTER_DISPLAY_SOURCE_STATUSES),
            order_by=request.args.get('order_by', 'latest'),
            limit=int(request.args.get('limit', 30) or 30),
            offset=int(request.args.get('offset', 0) or 0),
        )
        raw_items = _filter_center_rows_by_episode_policy(page_data.get('items') or [])

        # 列表接口只消费中心端预生成的 summary_json。
        # 不再为老数据缺摘要做 include_raw=True 兜底，否则打开中心资源库时仍会批量拉完整 RAW，越改越慢。

        local_share_codes = _load_local_share_code_set(raw_items)
        items = []
        for item in raw_items:
            item['_local_share_record_exists'] = str(item.get('share_code') or '').strip() in local_share_codes
            summary_json = item.get('summary_json') if isinstance(item.get('summary_json'), dict) else None
            item['version_summary'] = summary_json or {}
            # 双保险：列表响应不携带完整 RAW，避免调试/兼容路径把大对象带回前端。
            item.pop('raw_ffprobe_json', None)
            item['display_type'] = _center_display_type(item)
            items.append(_decorate_center_source_row(item))

        return jsonify({
            'success': True,
            'items': items,
            'total': int(page_data.get('total') or len(items)),
            'raw_total': int(page_data.get('raw_total') or len(items)),
            'scanned_raw': int(page_data.get('scanned_raw') or 0),
        })
    except Exception as e:
        logger.error(f"  ➜ [共享资源] 拉取中心资源库失败: {e}", exc_info=True)
        return jsonify({'success': False, 'message': f'拉取中心资源库失败: {e}'}), 500

def _center_replenish_file_entries(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    """从中心待补充展示行提取需要补齐的 SHA1 清单。"""
    row = row or {}
    entries = _center_row_file_entries(row)
    local = row.get('local_library') if isinstance(row.get('local_library'), dict) else {}
    local_files = local.get('files') if isinstance(local.get('files'), list) else []

    # 新中心 display-list 可能不返回完整 pack_items；本地入库标记里的 files 往往更完整。
    entry_sha_count = len([e for e in entries if _center_norm_sha1(e.get('sha1'))])
    local_sha_count = len([e for e in local_files if _center_norm_sha1(e.get('sha1'))])
    if local_sha_count > entry_sha_count:
        entries = local_files

    out = []
    seen = set()
    for item in entries or []:
        if not isinstance(item, dict):
            continue
        sha1 = _center_norm_sha1(item.get('sha1'))
        if not sha1 or sha1 in seen:
            continue
        seen.add(sha1)
        out.append({
            'source_id': item.get('source_id') or row.get('source_id'),
            'sha1': sha1,
            'file_name': item.get('file_name') or item.get('relative_path') or item.get('label') or row.get('file_name') or row.get('title') or sha1,
            'relative_path': item.get('relative_path') or item.get('file_name') or '',
            'season_number': item.get('season_number') if item.get('season_number') not in (None, '') else row.get('season_number'),
            'episode_number': item.get('episode_number') if item.get('episode_number') not in (None, '') else row.get('episode_number'),
            'size': item.get('size') or 0,
        })
    return out

def _prepare_center_replenish_manual_candidate(row: Dict[str, Any]) -> Dict[str, Any]:
    """把中心“待补充”行转换为手动分享模态框可直接选择的候选。只准备，不创建分享。"""
    row = dict(row or {})
    status = str(row.get('status') or '').strip().lower()
    if status != CENTER_SOURCE_STATUS_REPLENISH:
        return {'ok': False, 'message': '只有待补充资源才能执行补充'}

    local = row.get('local_library') if isinstance(row.get('local_library'), dict) else {}
    if local and not local.get('is_fully_in_library'):
        return {'ok': False, 'message': local.get('label') or '本机没有完整相同资源，不能补充'}

    entries = _center_replenish_file_entries(row)
    expected = max(_safe_int(row.get('pack_item_count'), 0), _safe_int(local.get('total_count'), 0), len(entries))
    if not entries:
        return {'ok': False, 'message': '中心行没有可用于补充的 SHA1 明细'}
    if expected > len(entries):
        return {'ok': False, 'message': f'中心资源需要 {expected} 个文件，但当前只拿到 {len(entries)} 个 SHA1，不能确认完全相同'}

    sha1s = [e['sha1'] for e in entries]
    file_rows = _get_p115_file_rows([], sha1s)
    by_sha: Dict[str, List[Dict[str, Any]]] = {}
    for r in file_rows or []:
        sha = _center_norm_sha1(r.get('sha1'))
        if sha:
            by_sha.setdefault(sha, []).append(r)

    chosen_rows = []
    files = []
    missing = []
    used_fids = set()
    for entry in entries:
        sha = entry.get('sha1')
        candidates = by_sha.get(sha) or []
        if not candidates:
            missing.append(entry.get('file_name') or sha)
            continue
        target_size = _safe_size_bytes(entry.get('size'))
        candidates = sorted(candidates, key=lambda r: (
            1 if target_size and _safe_size_bytes(r.get('size')) == target_size else 0,
            _safe_size_bytes(r.get('size')),
        ), reverse=True)
        picked = candidates[0]
        fid = str(picked.get('id') or '')
        if not fid or fid in used_fids:
            continue
        used_fids.add(fid)
        chosen_rows.append(picked)
        name = str(picked.get('name') or entry.get('file_name') or sha)
        files.append({
            'fid': fid,
            'sha1': sha,
            'size': _safe_size_bytes(picked.get('size') or entry.get('size')),
            'file_name': name,
            'relative_path': picked.get('local_path') or entry.get('relative_path') or name,
            'tmdb_id': str(row.get('tmdb_id') or row.get('share_tmdb_id') or ''),
            'item_type': 'Episode',
            'season_number': entry.get('season_number') if entry.get('season_number') not in (None, '') else row.get('season_number'),
            'episode_number': entry.get('episode_number') if entry.get('episode_number') not in (None, '') else _guess_episode_number(entry.get('file_name') or name),
            'raw_json': {'source': 'center_replenish_prepare+p115_filesystem_cache', 'center_entry': entry, 'cache_row': picked},
        })

    if missing:
        shown = '；'.join(str(x) for x in missing[:8])
        return {'ok': False, 'message': f'本地 p115_filesystem_cache 未命中这些 SHA1，不能补充：{shown}' + (f' 等 {len(missing)} 个' if len(missing) > 8 else '')}
    if len(files) != len(entries):
        return {'ok': False, 'message': f'本地可用文件数不完整：{len(files)}/{len(entries)}，不能确认完全相同'}

    display_type = _center_display_type(row)
    src_share_type = _center_norm_item_type(row.get('share_type'))
    if display_type == 'Movie':
        share_type = 'movie_file' if len(files) == 1 else 'movie_folder'
        item_type = 'Movie'
        for f in files:
            f['item_type'] = 'Movie'
            f['season_number'] = None
            f['episode_number'] = None
    elif display_type == 'Episode':
        share_type = 'episode_file'
        item_type = 'Episode'
        for f in files:
            f['item_type'] = 'Episode'
            if f.get('season_number') in (None, ''):
                f['season_number'] = row.get('season_number')
            if f.get('episode_number') in (None, ''):
                f['episode_number'] = row.get('episode_number')
    else:
        share_type = 'series_pack' if src_share_type == 'series_pack' else 'season_pack'
        item_type = 'Series' if share_type == 'series_pack' else 'Season'
        for f in files:
            f['item_type'] = 'Episode'
            if f.get('season_number') in (None, ''):
                f['season_number'] = row.get('season_number')

    parent_series_id = str(row.get('parent_series_tmdb_id') or row.get('series_tmdb_id') or row.get('share_tmdb_id') or row.get('tmdb_id') or '').strip()
    identity = _standard_media_identity_for_share({
        **row,
        'item_type': item_type,
        'share_type': share_type,
        'tmdb_id': row.get('share_tmdb_id') or row.get('tmdb_id'),
        'parent_series_tmdb_id': parent_series_id if item_type in ('Series', 'Season', 'Episode') else '',
        'season_number': row.get('season_number'),
        'episode_number': row.get('episode_number'),
    })

    if share_type in ('movie_file', 'episode_file') and len(chosen_rows) == 1:
        root_fid = str(chosen_rows[0].get('id') or '')
        root_is_dir = False
        root_name = chosen_rows[0].get('name') or files[0].get('file_name') or root_fid
    else:
        parent_ids = [str(r.get('parent_id') or '') for r in chosen_rows if r.get('parent_id')]
        if not parent_ids:
            return {'ok': False, 'message': '已命中文件，但 p115_filesystem_cache 缺少 parent_id，无法定位可分享目录'}
        chains = [_ancestor_chain(pid) for pid in parent_ids]
        common = []
        if chains:
            for node_id in chains[0]:
                if all(node_id in ch for ch in chains[1:]):
                    common.append(node_id)
        root_fid = common[0] if common else parent_ids[0]
        root_node = _get_p115_node(root_fid) or {}
        root_name = root_node.get('name') or root_fid
        root_is_dir = True
        if share_type == 'season_pack':
            narrowed = _narrow_season_pack_root(root_fid, root_name, parent_ids, row.get('season_number'))
            if not narrowed.get('ok'):
                return {'ok': False, 'message': narrowed.get('message') or '无法安全定位单季目录，已阻止补充'}
            root_fid = narrowed.get('root_id') or root_fid
            root_name = narrowed.get('root_name') or root_name

    if root_is_dir:
        root_files = _collect_files_from_cache(root_fid, root_name=root_name, max_depth=8)
        root_sha1s = {_center_norm_sha1(f.get('sha1')) for f in root_files or [] if _center_norm_sha1(f.get('sha1'))}
        expected_sha1s = set(sha1s)
        extra_sha1s = sorted(root_sha1s - expected_sha1s)
        if extra_sha1s:
            return {
                'ok': False,
                'message': f'已定位到目录 {root_name}，但目录内还有 {len(extra_sha1s)} 个额外视频，不能确认是完全相同资源；请改用手动分享重新选择更精确目录',
            }

    title = identity.get('title') or row.get('title') or row.get('media_title') or row.get('file_name') or root_name
    display_title = title
    if item_type == 'Season' and row.get('season_number') not in (None, ''):
        try:
            display_title = f"{title} S{int(row.get('season_number')):02d}"
        except Exception:
            display_title = f"{title} S{row.get('season_number')}"
    elif item_type == 'Episode':
        s = row.get('season_number')
        e = row.get('episode_number')
        try:
            if s not in (None, '') and e not in (None, ''):
                display_title = f"{title} S{int(s):02d}E{int(e):02d}"
        except Exception:
            pass

    candidate = {
        'resolvable': True,
        'display_title': display_title,
        'series_title': title,
        'standard_title': title,
        'title': title,
        'release_year': identity.get('release_year') or row.get('release_year'),
        'tmdb_id': str(identity.get('tmdb_id') or row.get('tmdb_id') or row.get('share_tmdb_id') or ''),
        'share_tmdb_id': str(identity.get('tmdb_id') or row.get('share_tmdb_id') or row.get('tmdb_id') or ''),
        'parent_series_tmdb_id': identity.get('parent_series_tmdb_id') or row.get('parent_series_tmdb_id') or '',
        'item_type': item_type,
        'share_item_type': item_type,
        'share_type': share_type,
        'season_number': row.get('season_number'),
        'episode_number': row.get('episode_number') if share_type == 'episode_file' else None,
        'root_fid': root_fid,
        'root_name': root_name,
        'root_is_dir': root_is_dir,
        'file_count': len(files),
        'message': f'已按中心待补充资源的 SHA1 在本机定位到完全相同文件 {len(files)}/{expected}，请确认后创建永久分享',
        'center_replenish_source_id': row.get('source_id'),
        'center_replenish_payload': row,
    }
    return {'ok': True, 'candidate': candidate, 'files': files}

@shared_resource_bp.route('/center/replenish/prepare', methods=['POST'])
@admin_required
def api_prepare_center_replenish_share():
    """中心资源库待补充行：本机有完全相同 SHA1 时，生成手动分享模态框候选。"""
    data = _request_json()
    row = data.get('source') if isinstance(data.get('source'), dict) else (data.get('context') if isinstance(data.get('context'), dict) else {})
    if not row:
        return jsonify({'success': False, 'message': '缺少待补充资源信息'}), 400
    prepared = _prepare_center_replenish_manual_candidate(row)
    if not prepared.get('ok'):
        return jsonify({'success': False, 'message': prepared.get('message') or '该资源不能补充', 'data': prepared}), 400
    return jsonify({
        'success': True,
        'message': '已自动填入本机完全相同资源，请在弹窗中确认后点击“创建永久分享”',
        'data': prepared.get('candidate'),
    })

@shared_resource_bp.route('/center/import', methods=['POST'])
@admin_required
def api_center_import_sources():
    data = _request_json()
    source_ids = data.get('source_ids') or ([] if not data.get('source_id') else [data.get('source_id')])
    context = data.get('context') or {}
    if str(context.get('status') or '').strip().lower() == CENTER_SOURCE_STATUS_REPLENISH:
        return jsonify({'success': False, 'message': '该中心资源处于待补充状态，只用于精准补源展示，不能转存'}), 400
    mode = str(data.get('mode') or 'permanent').strip().lower()
    if mode == 'virtual':
        return jsonify({'success': False, 'message': '虚拟入库已移除，请使用“转存”。'}), 410
    mode = 'permanent'
    try:
        from handler.shared_subscription_service import consume_center_sources
        result = consume_center_sources(source_ids, mode=mode, context=context)
        status = 200 if result.get('success') else 400
        return jsonify({'success': bool(result.get('success')), 'message': result.get('message') or result.get('action_type') or '处理完成', 'data': result}), status
    except Exception as e:
        logger.error(f"  ➜ [共享资源] 手动转存中心资源失败: {e}", exc_info=True)
        return jsonify({'success': False, 'message': f'手动转存失败: {e}'}), 500
