# routes/shared_resource.py
# 共享资源：虚拟入库、我的分享、贡献值管理 API
import logging
import os
import re
import json
from typing import Dict, Any, List

import requests
from flask import Blueprint, jsonify, request

import config_manager
import constants
from extensions import admin_required
from database import shared_virtual_db, shared_share_db
from database.connection import get_db_connection
from handler.p115_service import P115Service

shared_resource_bp = Blueprint('shared_resource_bp', __name__, url_prefix='/api/shared/resources')
logger = logging.getLogger(__name__)

VIDEO_EXTENSIONS = {'.mp4', '.mkv', '.avi', '.ts', '.mov', '.m2ts', '.iso', '.wmv', '.flv'}


def _get_shared_config() -> Dict[str, Any]:
    cfg = config_manager.APP_CONFIG or {}
    return {
        "enabled": bool(cfg.get(constants.CONFIG_OPTION_115_SHARED_RESOURCE_ENABLED, False)),
        "center_url": (cfg.get(constants.CONFIG_OPTION_115_SHARED_CENTER_URL) or "https://shared.55565576.xyz").rstrip('/'),
        "device_token": cfg.get(constants.CONFIG_OPTION_115_SHARED_DEVICE_TOKEN) or "",
        "mode": cfg.get(constants.CONFIG_OPTION_115_SHARED_RESOURCE_MODE) or "permanent",
    }


def _remove_file_quietly(path: str) -> bool:
    if not path:
        return False
    try:
        if os.path.exists(path) and os.path.isfile(path):
            os.remove(path)
            return True
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 删除本地文件失败: {path} -> {e}")
    return False


def _fetch_center_credit() -> Dict[str, Any]:
    cfg = _get_shared_config()
    if not cfg["device_token"]:
        return {"ok": False, "message": "未配置共享中心 device_token"}

    headers = {"X-Device-Token": cfg["device_token"]}
    me_resp = requests.get(f"{cfg['center_url']}/api/v1/me", headers=headers, timeout=12)
    me_resp.raise_for_status()
    me = me_resp.json() or {}

    stats = {}
    try:
        stats_resp = requests.get(f"{cfg['center_url']}/api/v1/stats", headers=headers, timeout=12)
        if stats_resp.ok:
            stats = stats_resp.json() or {}
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 拉取中心统计失败，仅保存 credit: {e}")

    snapshot = {
        "device_id": me.get("id"),
        "credit": int(me.get("credit") or 0),
        "wanted_gaps": int(stats.get("wanted_gaps") or 0),
        "shared_sources": int(stats.get("shared_sources") or 0),
        "raw_ffprobe": int(stats.get("raw_ffprobe") or 0),
        "remote_devices": int(stats.get("devices") or 0),
        "raw_json": {"me": me, "stats": stats},
    }
    saved = shared_virtual_db.upsert_credit_snapshot(snapshot)
    return {"ok": True, "snapshot": saved}


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


def _collect_files_from_cache(root_fid: str, root_name: str = '', max_depth: int = 6) -> List[Dict[str, Any]]:
    """从 p115_filesystem_cache 递归收集 root_fid 下的视频文件。
    用作 115 远程目录接口字段不完整/审核中返回不全时的兜底。
    """
    if not root_fid:
        return []
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH RECURSIVE tree AS (
                        SELECT id, parent_id, name, local_path, sha1, pick_code, size,
                               0 AS depth, CAST('' AS text) AS rel_path
                        FROM p115_filesystem_cache
                        WHERE id = %s
                        UNION ALL
                        SELECT c.id, c.parent_id, c.name, c.local_path, c.sha1, c.pick_code, c.size,
                               t.depth + 1 AS depth,
                               CASE WHEN t.rel_path = '' THEN c.name ELSE t.rel_path || '/' || c.name END AS rel_path
                        FROM p115_filesystem_cache c
                        JOIN tree t ON c.parent_id = t.id
                        WHERE t.depth < %s
                    )
                    SELECT * FROM tree
                    ORDER BY depth, rel_path, name
                    """,
                    (str(root_fid), int(max_depth)),
                )
                rows = [dict(r) for r in cur.fetchall()]
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 从 p115_filesystem_cache 收集分享文件失败: root={root_fid}, err={e}")
        return []

    files = []
    for row in rows:
        name = str(row.get('name') or '')
        if not _looks_like_video_name(name):
            continue
        rel = row.get('rel_path') or name
        files.append({
            'fid': str(row.get('id') or ''),
            'sha1': (str(row.get('sha1')).upper() if row.get('sha1') else None),
            'size': row.get('size') or 0,
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
            'size': r.get('size') or 0,
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
            return []
        return [{
            'fid': _node_id(root_info) or str(root_fid),
            'sha1': root_info.get('sha1') or root_info.get('sha') or root_info.get('file_sha1'),
            'size': root_info.get('size') or root_info.get('fs') or root_info.get('s') or 0,
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
                'size': node.get('size') or node.get('fs') or node.get('s') or 0,
                'file_name': name,
                'relative_path': rel,
                'episode_number': _guess_episode_number(name),
                'raw_json': node,
            })

    walk(root_fid, '', max_depth)

    # 远程接口没有拿到时，用本地 p115_filesystem_cache 兜底。
    if not files and is_dir:
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
        return {'status': 'alive', 'review_status': 'alive', 'message': '分享可访问'}
    if forbid or info.get('have_vio_file'):
        return {'status': 'rejected', 'review_status': 'rejected', 'message': forbid or '分享包含违规/被屏蔽文件'}
    return {'status': 'pending_review', 'review_status': 'pending_review', 'message': f'分享状态 {share_state or "未知"}'}


def _center_headers():
    cfg = _get_shared_config()
    if not cfg['device_token']:
        raise RuntimeError('未配置共享中心 device_token')
    return cfg, {'X-Device-Token': cfg['device_token'], 'Content-Type': 'application/json'}



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


def _load_local_raw_ffprobe(sha1: str):
    """从本地 p115_mediainfo_cache 读取 raw_ffprobe_json。"""
    sha1 = str(sha1 or '').strip().upper()
    if not re.fullmatch(r'[A-Fa-f0-9]{40}', sha1):
        return None
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT raw_ffprobe_json
                    FROM p115_mediainfo_cache
                    WHERE sha1=%s AND raw_ffprobe_json IS NOT NULL
                    LIMIT 1
                    """,
                    (sha1,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                raw = dict(row).get('raw_ffprobe_json')
                return _safe_json_obj(raw)
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 查询本地 raw_ffprobe_json 失败: sha1={sha1}, err={e}")
        return None


def _infer_size_from_raw(raw: Dict[str, Any]) -> int:
    if not isinstance(raw, dict):
        return 0
    try:
        fmt = raw.get('format') or {}
        size = fmt.get('size')
        if size is not None and str(size).strip():
            return int(float(size))
    except Exception:
        pass
    return 0


def _upload_item_raw_ffprobe_to_center(item: Dict[str, Any], cfg: Dict[str, Any], headers: Dict[str, str], force: bool = False) -> Dict[str, Any]:
    """上传单个分享文件的 raw_ffprobe_json 到中心服务器。返回 ok/missing/error。"""
    sha1 = str(item.get('sha1') or '').strip().upper()
    if not re.fullmatch(r'[A-Fa-f0-9]{40}', sha1):
        return {'ok': False, 'status': 'missing_sha1', 'message': '缺少 SHA1'}

    if item.get('raw_ffprobe_uploaded') and not force:
        return {'ok': True, 'status': 'already_uploaded', 'message': '已标记上传过'}

    raw = _load_local_raw_ffprobe(sha1)
    if not raw:
        return {'ok': False, 'status': 'missing_raw', 'message': '本地 p115_mediainfo_cache 没有 raw_ffprobe_json'}

    raw_size = _infer_size_from_raw(raw)
    item_size = int(item.get('size') or 0)
    final_size = item_size if item_size > 0 else raw_size

    payload = {
        'sha1': sha1,
        'size': final_size or None,
        'raw_ffprobe_json': raw,
    }
    try:
        resp = requests.post(f"{cfg['center_url']}/api/v1/rawffprobe/upload", headers=headers, json=payload, timeout=45)
        if not resp.ok:
            return {'ok': False, 'status': 'http_error', 'message': f'HTTP {resp.status_code} {resp.text[:160]}'}
        shared_share_db.mark_item_raw_uploaded(item['id'], True)
        if final_size > 0 and item_size <= 0:
            shared_share_db.update_share_item_size(item['id'], final_size)
        return {'ok': True, 'status': 'uploaded', 'message': '已上传 raw_ffprobe_json', 'size': final_size}
    except Exception as e:
        return {'ok': False, 'status': 'error', 'message': str(e)}


def _upload_share_raw_ffprobe_to_center(record_id: int, cfg: Dict[str, Any], headers: Dict[str, str], force: bool = False) -> Dict[str, Any]:
    items = shared_share_db.list_share_items(record_id)
    uploaded = 0
    skipped = 0
    missing = 0
    errors = []
    size_fixed = 0
    for item in items:
        before_size = int(item.get('size') or 0)
        result = _upload_item_raw_ffprobe_to_center(item, cfg, headers, force=force)
        if result.get('ok'):
            if result.get('status') == 'uploaded':
                uploaded += 1
                if before_size <= 0 and int(result.get('size') or 0) > 0:
                    size_fixed += 1
            else:
                skipped += 1
        else:
            if result.get('status') in {'missing_raw', 'missing_sha1'}:
                missing += 1
            else:
                errors.append(f"{item.get('file_name')}: {result.get('message')}")
    return {
        'total': len(items),
        'uploaded': uploaded,
        'skipped': skipped,
        'missing': missing,
        'size_fixed': size_fixed,
        'errors': errors,
    }


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
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tmdb_id, item_type, title, original_title, parent_series_tmdb_id,
                       season_number, episode_number, release_year, release_date, last_air_date,
                       file_sha1_json, file_pickcode_json, in_library, subscription_status
                FROM media_metadata
                WHERE tmdb_id=%s AND item_type=%s
                LIMIT 1
            """, (str(tmdb_id), str(item_type)))
            row = cur.fetchone()
            return dict(row) if row else None


def _get_series_title(series_tmdb_id: str):
    if not series_tmdb_id:
        return ''
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT title FROM media_metadata
                WHERE tmdb_id=%s AND item_type='Series'
                LIMIT 1
            """, (str(series_tmdb_id),))
            row = cur.fetchone()
            return (dict(row).get('title') if row else '') or ''


def _collect_media_identifiers(row: Dict[str, Any]) -> Dict[str, List[str]]:
    """根据媒体层级收集 PC/SHA1。Season/Series 会向下找 Episode。"""
    if not row:
        return {'pickcodes': [], 'sha1s': [], 'episode_rows': []}

    rows = [row]
    item_type = row.get('item_type')
    series_id = row.get('parent_series_tmdb_id') or row.get('tmdb_id')
    season_number = row.get('season_number')

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if item_type == 'Season':
                # 优先按父剧集 + 季号找本季所有分集；兼容 tmdb_id 季ID场景。
                if series_id and season_number is not None:
                    cur.execute("""
                        SELECT tmdb_id, item_type, title, original_title, parent_series_tmdb_id,
                               season_number, episode_number, release_year, release_date, last_air_date,
                               file_sha1_json, file_pickcode_json, in_library, subscription_status
                        FROM media_metadata
                        WHERE item_type='Episode'
                          AND parent_series_tmdb_id=%s
                          AND season_number=%s
                        ORDER BY episode_number NULLS LAST, tmdb_id
                    """, (str(series_id), int(season_number)))
                    episode_rows = [dict(r) for r in cur.fetchall()]
                    if episode_rows:
                        rows = episode_rows
            elif item_type == 'Series':
                cur.execute("""
                    SELECT tmdb_id, item_type, title, original_title, parent_series_tmdb_id,
                           season_number, episode_number, release_year, release_date, last_air_date,
                           file_sha1_json, file_pickcode_json, in_library, subscription_status
                    FROM media_metadata
                    WHERE item_type='Episode' AND parent_series_tmdb_id=%s
                    ORDER BY season_number NULLS LAST, episode_number NULLS LAST, tmdb_id
                """, (str(series_id),))
                episode_rows = [dict(r) for r in cur.fetchall()]
                if episode_rows:
                    rows = episode_rows

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
    if not pickcodes and not sha1s:
        return []
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, parent_id, name, local_path, sha1, pick_code, size
                FROM p115_filesystem_cache
                WHERE (%s::text[] <> '{}'::text[] AND pick_code = ANY(%s))
                   OR (%s::text[] <> '{}'::text[] AND UPPER(sha1) = ANY(%s))
                ORDER BY parent_id, name
            """, (pickcodes, pickcodes, sha1s, sha1s))
            rows = [dict(r) for r in cur.fetchall()]
    # 去重，避免 PC/SHA1 同时命中同一文件。
    seen, out = set(), []
    for r in rows:
        key = str(r.get('id') or '')
        if key and key not in seen:
            seen.add(key)
            out.append(r)
    return out


def _get_p115_node(node_id: str):
    if not node_id:
        return None
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, parent_id, name, local_path, sha1, pick_code, size
                FROM p115_filesystem_cache
                WHERE id=%s
                LIMIT 1
            """, (str(node_id),))
            row = cur.fetchone()
            return dict(row) if row else None


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


def _resolve_share_root(media_row: Dict[str, Any]) -> Dict[str, Any]:
    ids = _collect_media_identifiers(media_row)
    file_rows = _get_p115_file_rows(ids['pickcodes'], ids['sha1s'])
    item_type = media_row.get('item_type')
    share_type = 'movie_folder'
    share_item_type = item_type
    messages = []

    if item_type == 'Movie':
        share_type = 'movie_folder'
        share_item_type = 'Movie'
    elif item_type == 'Season':
        share_type = 'season_pack'
        share_item_type = 'Season'
    elif item_type == 'Series':
        share_type = 'series_pack'
        share_item_type = 'Series'
    elif item_type == 'Episode':
        # 手动分享不鼓励单集；如果只搜到某集，自动提升为“该集所在季目录”。
        share_type = 'season_pack'
        share_item_type = 'Season'

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
        }

    parent_ids = [str(r.get('parent_id') or '') for r in file_rows if r.get('parent_id')]
    root_id, root_is_dir = '', True

    if item_type == 'Movie' and len(file_rows) == 1:
        # 单文件电影直接分享文件，避免误把上级“电影分类目录”分享出去。
        root_id = str(file_rows[0].get('id') or '')
        root_is_dir = False
        share_type = 'movie_file'
        root_name = file_rows[0].get('name') or root_id
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
        }

    return {
        'resolvable': True,
        'root_fid': root_id,
        'root_name': root_name,
        'root_is_dir': root_is_dir,
        'file_count': len(file_rows),
        'matched_pickcodes': len(ids['pickcodes']),
        'matched_sha1s': len(ids['sha1s']),
        'share_type': share_type,
        'share_item_type': share_item_type,
        'message': '；'.join(messages) if messages else '已通过 PC/SHA1 定位到可分享目录/文件',
    }


def _build_media_candidate(row: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(row)
    year = _parse_release_year(row)
    parent_series_id = row.get('parent_series_tmdb_id')
    if row.get('item_type') in {'Season', 'Episode'} and not parent_series_id:
        parent_series_id = str(row.get('tmdb_id') or '').split('_')[0] if '_' in str(row.get('tmdb_id') or '') else ''
    series_title = _get_series_title(parent_series_id) if parent_series_id else ''
    title = row.get('title') or row.get('original_title') or row.get('tmdb_id')
    display_title = title
    if row.get('item_type') == 'Season':
        display_title = f"{series_title or title} S{int(row.get('season_number') or 0):02d}" if row.get('season_number') else (series_title or title)
    elif row.get('item_type') == 'Episode':
        s = row.get('season_number')
        e = row.get('episode_number')
        display_title = f"{series_title or title} S{int(s or 0):02d}E{int(e or 0):02d}" if s and e else f"{series_title or title} · {title}"

    resolved = _resolve_share_root(row)
    share_tmdb_id = row.get('tmdb_id')
    if resolved.get('share_item_type') in {'Season', 'Episode'}:
        share_tmdb_id = parent_series_id or row.get('parent_series_tmdb_id') or row.get('tmdb_id')

    return {
        **row,
        'display_title': display_title,
        'series_title': series_title,
        'release_year': year,
        'parent_series_tmdb_id': parent_series_id,
        'share_tmdb_id': str(share_tmdb_id or ''),
        'share_item_type': resolved.get('share_item_type') or row.get('item_type'),
        **resolved,
    }


@shared_resource_bp.route('/media/search', methods=['GET'])
@admin_required
def api_search_shareable_media():
    keyword = str(request.args.get('keyword') or '').strip()
    if len(keyword) < 1:
        return jsonify({"success": True, "items": []})
    limit = max(1, min(int(request.args.get('limit', 20) or 20), 50))
    kw = f'%{keyword}%'
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tmdb_id, item_type, title, original_title, parent_series_tmdb_id,
                       season_number, episode_number, release_year, release_date, last_air_date,
                       file_sha1_json, file_pickcode_json, in_library, subscription_status
                FROM media_metadata
                WHERE item_type IN ('Movie','Series','Season','Episode')
                  AND (
                    title ILIKE %s OR original_title ILIKE %s OR tmdb_id ILIKE %s
                  )
                ORDER BY
                  CASE item_type WHEN 'Movie' THEN 0 WHEN 'Season' THEN 1 WHEN 'Series' THEN 2 ELSE 3 END,
                  in_library DESC,
                  COALESCE(release_year, 0) DESC,
                  title NULLS LAST
                LIMIT %s
            """, (kw, kw, kw, limit))
            rows = [dict(r) for r in cur.fetchall()]

    items = []
    for row in rows:
        try:
            items.append(_build_media_candidate(row))
        except Exception as e:
            logger.warning(f"  ➜ [共享资源] 构建可分享候选失败: {row.get('title') or row.get('tmdb_id')} -> {e}")
            row['resolvable'] = False
            row['message'] = str(e)
            items.append(row)
    return jsonify({"success": True, "items": items})


@shared_resource_bp.route('/summary', methods=['GET'])
@admin_required
def api_shared_summary():
    summary = shared_virtual_db.get_local_summary()
    return jsonify({"success": True, "data": summary})


@shared_resource_bp.route('/virtual', methods=['GET'])
@admin_required
def api_list_virtual_items():
    items, total = shared_virtual_db.list_virtual_items(
        status=request.args.get('status', 'all'),
        item_type=request.args.get('item_type', 'all'),
        keyword=request.args.get('keyword', ''),
        page=int(request.args.get('page', 1) or 1),
        page_size=int(request.args.get('page_size', 30) or 30),
    )
    return jsonify({"success": True, "items": items, "total": total})


@shared_resource_bp.route('/virtual/<virtual_id>/delete', methods=['POST'])
@admin_required
def api_delete_virtual_item(virtual_id):
    item = shared_virtual_db.get_virtual_item(virtual_id)
    if not item:
        return jsonify({"success": False, "message": "虚拟资源不存在"}), 404

    data = request.json or {}
    delete_remote = data.get('delete_remote', True)
    delete_local = data.get('delete_local', True)
    messages = []

    if delete_remote and item.get('real_fid'):
        client = P115Service.get_client()
        if not client:
            return jsonify({"success": False, "message": "未配置可用的 115 客户端，无法删除临时转存文件"}), 400
        resp = client.fs_delete([str(item['real_fid'])])
        if not resp or not resp.get('state'):
            return jsonify({"success": False, "message": f"115 删除失败: {resp}"}), 500
        messages.append("115临时文件已删除")

    if delete_local:
        removed = []
        for key in ('strm_path', 'mediainfo_path', 'nfo_path'):
            if _remove_file_quietly(item.get(key)):
                removed.append(key)
        if removed:
            messages.append("本地投影文件已删除")

    row = shared_virtual_db.mark_virtual_deleted(virtual_id, message='；'.join(messages) or '手动删除')
    shared_virtual_db.add_credit_ledger(
        event_type='virtual_deleted', delta=0, reason='手动删除虚拟入库资源',
        virtual_id=virtual_id, source_id=item.get('source_id') or '', tmdb_id=item.get('tmdb_id') or '',
        item_type=item.get('item_type') or '', title=item.get('title') or '', raw_json=data,
    )
    return jsonify({"success": True, "message": "已删除虚拟资源", "data": row})


@shared_resource_bp.route('/virtual/<virtual_id>/promote', methods=['POST'])
@admin_required
def api_promote_virtual_item(virtual_id):
    item = shared_virtual_db.get_virtual_item(virtual_id)
    if not item:
        return jsonify({"success": False, "message": "虚拟资源不存在"}), 404
    if item.get('status') == 'promoted':
        return jsonify({"success": True, "message": "该资源已经是永久转存", "data": item})
    if not item.get('real_fid'):
        return jsonify({"success": False, "message": "该虚拟资源还没有播放转存记录，无法转正"}), 400

    data = request.json or {}
    target_cid = data.get('target_cid') or item.get('target_parent_id')
    if not target_cid or str(target_cid) == '0':
        return jsonify({"success": False, "message": "缺少正式媒体目录 CID，无法移动转正"}), 400

    client = P115Service.get_client()
    if not client:
        return jsonify({"success": False, "message": "未配置可用的 115 客户端，无法转正"}), 400

    resp = client.fs_move([str(item['real_fid'])], str(target_cid))
    if not resp or not resp.get('state'):
        return jsonify({"success": False, "message": f"115 移动失败: {resp}"}), 500

    row = shared_virtual_db.mark_virtual_promoted(
        virtual_id, promoted_fid=str(item.get('real_fid') or ''),
        promoted_pick_code=item.get('real_pick_code') or '', message=f"手动转正到CID {target_cid}",
    )
    shared_virtual_db.add_credit_ledger(
        event_type='virtual_promoted', delta=0, reason='手动将虚拟资源转为永久转存',
        virtual_id=virtual_id, source_id=item.get('source_id') or '', tmdb_id=item.get('tmdb_id') or '',
        item_type=item.get('item_type') or '', title=item.get('title') or '', raw_json={"target_cid": target_cid, "move_response": resp},
    )
    return jsonify({"success": True, "message": "已转为永久转存", "data": row})


@shared_resource_bp.route('/shares', methods=['GET'])
@admin_required
def api_list_my_shares():
    items, total = shared_share_db.list_share_records(
        status=request.args.get('status', 'all'),
        keyword=request.args.get('keyword', ''),
        page=int(request.args.get('page', 1) or 1),
        page_size=int(request.args.get('page_size', 30) or 30),
    )
    return jsonify({"success": True, "items": items, "total": total})


@shared_resource_bp.route('/shares/<int:record_id>/items', methods=['GET'])
@admin_required
def api_list_share_items(record_id):
    return jsonify({"success": True, "items": shared_share_db.list_share_items(record_id)})


@shared_resource_bp.route('/shares/manual-create', methods=['POST'])
@admin_required
def api_manual_create_share():
    data = request.json or {}
    root_fid = str(data.get('root_fid') or '').strip()
    if not root_fid:
        return jsonify({"success": False, "message": "缺少要分享的 115 文件/目录 FID/CID"}), 400

    client = P115Service.get_client()
    if not client:
        return jsonify({"success": False, "message": "未配置可用的 115 Cookie 客户端，无法创建分享"}), 400

    receive_code = str(data.get('receive_code') or '').strip() or None
    share_resp = client.share_create([root_fid], share_duration=-1, receive_code=receive_code)
    if not share_resp or not share_resp.get('state'):
        return jsonify({"success": False, "message": f"创建 115 分享失败: {share_resp}"}), 500

    share_data = share_resp.get('data') or {}
    share_code = share_data.get('share_code') or share_resp.get('share_code')
    share_url = share_data.get('share_url') or (f"https://115.com/s/{share_code}" if share_code else '')
    receive_code = receive_code or share_data.get('receive_code') or ''

    info_resp = client.fs_get_info(root_fid)
    node = (info_resp or {}).get('data') or {}
    root_name = data.get('root_name') or _node_name(node) or root_fid
    root_is_dir = _is_folder(node) if node else bool(data.get('root_is_dir', True))

    max_depth = int(data.get('max_depth') or 6)
    files = _collect_files_from_115(client, root_fid, root_name=root_name, max_depth=max_depth, assume_dir=root_is_dir)
    if not files:
        files = _collect_files_from_media_payload(data)

    for item in files:
        if not item.get('tmdb_id'):
            item['tmdb_id'] = str(data.get('tmdb_id') or '')
        if not item.get('item_type'):
            item['item_type'] = 'Episode' if data.get('share_type') in ('season_pack', 'series_pack') and item.get('episode_number') else data.get('item_type')
        if not item.get('season_number'):
            item['season_number'] = data.get('season_number')

    record = shared_share_db.create_share_record({
        'share_code': share_code,
        'receive_code': receive_code,
        'share_url': share_url,
        'share_type': data.get('share_type') or ('season_pack' if data.get('season_number') else 'movie_folder'),
        'root_fid': root_fid,
        'root_name': root_name,
        'root_is_dir': root_is_dir,
        'tmdb_id': str(data.get('tmdb_id') or ''),
        'item_type': data.get('item_type') or 'Season',
        'parent_series_tmdb_id': data.get('parent_series_tmdb_id'),
        'season_number': data.get('season_number'),
        'title': data.get('title') or root_name,
        'release_year': data.get('release_year'),
        'status': 'pending_review',
        'review_status': 'pending_review',
        'center_status': 'not_reported',
        'raw_json': {'share_response': share_resp, 'root_info': info_resp},
    })
    count = shared_share_db.replace_share_items(record['id'], files)
    record = shared_share_db.update_share_record(record['id'], item_count=count)
    shared_virtual_db.add_credit_ledger('share_created', 0, '手动创建115分享，等待审核', ref_id=str(record['id']), title=record.get('title') or '', raw_json={'share_code': share_code, 'item_count': count})

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
                    assume_dir=bool(record.get('root_is_dir', True)),
                )
            if not files:
                files = _collect_files_from_media_payload(record)
            for item in files:
                if not item.get('tmdb_id'):
                    item['tmdb_id'] = str(record.get('tmdb_id') or '')
                if not item.get('item_type'):
                    item['item_type'] = 'Episode' if record.get('share_type') in ('season_pack', 'series_pack') and item.get('episode_number') else record.get('item_type')
                if not item.get('season_number'):
                    item['season_number'] = record.get('season_number')
            if files:
                added_count = shared_share_db.replace_share_items(record_id, files)
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 检查分享时补扫包内文件失败: record={record_id}, err={e}", exc_info=True)

    update_kwargs = dict(
        status=parsed['status'], review_status=parsed['review_status'], last_checked_at='NOW()',
        last_error=parsed['message'], raw_json={'last_snap': snap},
    )
    if added_count is not None:
        update_kwargs['item_count'] = added_count
    row = shared_share_db.update_share_record(record_id, **update_kwargs)
    msg = parsed['message']
    if added_count is not None:
        msg = f"{msg}，已补扫到 {added_count} 个视频文件"
    return jsonify({"success": True, "message": msg, "data": row, "raw": snap})


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
        return jsonify({"success": False, "message": "分享包内没有可登记的视频文件"}), 400

    # 登记中心前先上传 raw_ffprobe_json；同时用 raw.format.size 回填本地 size=0 的条目。
    raw_summary = _upload_share_raw_ffprobe_to_center(record_id, cfg, headers, force=False)
    # 重新读取 items，确保 size/raw_ffprobe_uploaded 是最新状态。
    items = shared_share_db.list_share_items(record_id)

    reported = 0
    errors = []
    first_source_id = None
    for item in items:
        sha1 = str(item.get('sha1') or '').strip().upper()
        if not sha1:
            errors.append(f"{item.get('file_name')} 缺少 SHA1，跳过")
            continue
        payload = {
            'tmdb_id': str(item.get('tmdb_id') or record.get('tmdb_id') or ''),
            'item_type': item.get('item_type') or record.get('item_type') or 'Movie',
            'season_number': item.get('season_number') or record.get('season_number'),
            'episode_number': item.get('episode_number'),
            'title': record.get('title') or item.get('file_name'),
            'release_year': record.get('release_year'),
            'sha1': sha1,
            'size': int(item.get('size') or 0),
            'file_name': item.get('file_name') or '',
            'quality': '',
            'share_code': record.get('share_code'),
            'receive_code': record.get('receive_code') or '',
            'has_raw_ffprobe': bool(item.get('raw_ffprobe_uploaded')),
        }
        try:
            resp = requests.post(f"{cfg['center_url']}/api/v1/sources/register", headers=headers, json=payload, timeout=20)
            if not resp.ok:
                errors.append(f"{item.get('file_name')}: HTTP {resp.status_code} {resp.text[:120]}")
                continue
            data = resp.json() or {}
            source_id = data.get('source_id')
            first_source_id = first_source_id or source_id
            shared_share_db.mark_item_reported(item['id'], source_id or '')
            reported += 1
        except Exception as e:
            errors.append(f"{item.get('file_name')}: {e}")

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
    shared_virtual_db.add_credit_ledger(
        'share_reported_center', 0,
        f"登记中心 {reported}/{len(items)} 条；raw上传 {raw_summary.get('uploaded', 0)} 条，缺失 {raw_summary.get('missing', 0)} 条",
        ref_id=str(record_id), title=record.get('title') or '',
        raw_json={'errors': errors, 'raw_summary': raw_summary}
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
    return jsonify({"success": reported > 0, "message": msg, "data": row, "errors": errors, "raw_summary": raw_summary})


@shared_resource_bp.route('/shares/<int:record_id>/upload-rawffprobe', methods=['POST'])
@admin_required
def api_upload_share_raw_ffprobe(record_id):
    record = shared_share_db.get_share_record(record_id)
    if not record:
        return jsonify({"success": False, "message": "分享记录不存在"}), 404
    cfg, headers = _center_headers()
    force = bool((request.json or {}).get('force'))
    summary = _upload_share_raw_ffprobe_to_center(record_id, cfg, headers, force=force)
    shared_virtual_db.add_credit_ledger(
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
    record = shared_share_db.get_share_record(record_id)
    if not record:
        return jsonify({"success": False, "message": "分享记录不存在"}), 404
    client = P115Service.get_client()
    if not client:
        return jsonify({"success": False, "message": "未配置可用的 115 Cookie 客户端"}), 400
    resp = client.share_cancel(record.get('share_code'))
    if not resp or not resp.get('state'):
        row = shared_share_db.update_share_record(record_id, last_error=f"取消分享失败: {resp}")
        return jsonify({"success": False, "message": f"取消分享失败: {resp}", "data": row}), 500
    row = shared_share_db.update_share_record(record_id, status='cancelled', review_status='cancelled', cancelled_at='NOW()', last_error='手动取消分享')
    shared_virtual_db.add_credit_ledger('share_cancelled', 0, '手动取消115分享', ref_id=str(record_id), title=record.get('title') or '', raw_json=resp)
    return jsonify({"success": True, "message": "已取消分享", "data": row})


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
    limit = int(request.args.get('limit', 50) or 50)
    rows = shared_virtual_db.list_credit_ledger(limit=limit)
    return jsonify({"success": True, "items": rows})
