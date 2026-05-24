# routes/shared_resource.py
# 共享资源：虚拟入库管理与贡献值面板 API
import logging
import os
from typing import Dict, Any

import requests
from flask import Blueprint, jsonify, request

import config_manager
import constants
from extensions import admin_required
from database import shared_virtual_db
from handler.p115_service import P115Service

shared_resource_bp = Blueprint('shared_resource_bp', __name__, url_prefix='/api/shared/resources')
logger = logging.getLogger(__name__)


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
        event_type='virtual_deleted',
        delta=0,
        reason='手动删除虚拟入库资源',
        virtual_id=virtual_id,
        source_id=item.get('source_id') or '',
        tmdb_id=item.get('tmdb_id') or '',
        item_type=item.get('item_type') or '',
        title=item.get('title') or '',
        raw_json={"delete_remote": delete_remote, "delete_local": delete_local},
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
        virtual_id,
        promoted_fid=str(item.get('real_fid') or ''),
        promoted_pick_code=item.get('real_pick_code') or '',
        message=f"手动转正到CID {target_cid}",
    )
    shared_virtual_db.add_credit_ledger(
        event_type='virtual_promoted',
        delta=0,
        reason='手动将虚拟资源转为永久转存',
        virtual_id=virtual_id,
        source_id=item.get('source_id') or '',
        tmdb_id=item.get('tmdb_id') or '',
        item_type=item.get('item_type') or '',
        title=item.get('title') or '',
        raw_json={"target_cid": target_cid, "move_response": resp},
    )
    return jsonify({"success": True, "message": "已转为永久转存", "data": row})


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
