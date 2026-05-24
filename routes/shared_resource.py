# routes/shared_resource.py
# 共享资源：虚拟入库、我的分享、贡献值管理 API
import logging
import os
import re
from typing import Dict, Any, List

import requests
from flask import Blueprint, jsonify, request

import config_manager
import constants
from extensions import admin_required
from database import shared_virtual_db, shared_share_db
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


def _is_folder(node: Dict[str, Any]) -> bool:
    fc = str(node.get('fc') if node.get('fc') is not None else node.get('file_category') if node.get('file_category') is not None else '')
    return fc == '0' or bool(node.get('is_dir') or node.get('is_folder'))


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


def _collect_files_from_115(client, root_fid: str, root_name: str = '', max_depth: int = 3, current_path: str = '') -> List[Dict[str, Any]]:
    """递归收集分享目录下的视频文件。测试阶段够用；正式自动分享后可改为基于 filesystem_cache 精准组包。"""
    info_resp = client.fs_get_info(root_fid)
    root_info = (info_resp or {}).get('data') or {}
    if not root_name:
        root_name = _node_name(root_info) or str(root_fid)

    if root_info and not _is_folder(root_info):
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

    files = _collect_files_from_115(client, root_fid, root_name=root_name, max_depth=int(data.get('max_depth') or 3))
    for item in files:
        item['tmdb_id'] = str(data.get('tmdb_id') or '')
        item['item_type'] = 'Episode' if data.get('share_type') == 'season_pack' and item.get('episode_number') else data.get('item_type')
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
    row = shared_share_db.update_share_record(
        record_id,
        status=parsed['status'], review_status=parsed['review_status'], last_checked_at='NOW()',
        last_error=parsed['message'], raw_json={'last_snap': snap},
    )
    return jsonify({"success": True, "message": parsed['message'], "data": row, "raw": snap})


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
            'has_raw_ffprobe': False,
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
    shared_virtual_db.add_credit_ledger('share_reported_center', 0, f'登记中心 {reported}/{len(items)} 条', ref_id=str(record_id), title=record.get('title') or '', raw_json={'errors': errors})
    return jsonify({"success": reported > 0, "message": f"已登记 {reported}/{len(items)} 条", "data": row, "errors": errors})


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
