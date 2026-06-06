# routes/shared_resource.py
# Rapid v2 共享资源 API：不再创建/管理 115 分享；只登记秒传资源索引与消费中心事件。
import json
import logging
import socket
import threading
import uuid
from typing import Any, Dict

import requests
from flask import Blueprint, jsonify, request

import constants
from extensions import admin_required
from database import shared_credit_db, shared_share_db, settings_db
from handler.shared_center_client import SharedCenterClient
from handler.shared_subscription_service import consume_device_event
import tasks.shared_resource_tasks as shared_tasks

shared_resource_bp = Blueprint('shared_resource_bp', __name__, url_prefix='/api/shared/resources')
logger = logging.getLogger(__name__)


def _boolish(value, default=False):
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in ('1', 'true', 'yes', 'y', 'on', '启用', '开启', '是'):
        return True
    if text in ('0', 'false', 'no', 'n', 'off', '停用', '关闭', '否'):
        return False
    return bool(default)


def _request_json() -> Dict[str, Any]:
    try:
        data = request.get_json(silent=True)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _shared_resource_config_payload() -> Dict[str, Any]:
    payload = settings_db.get_shared_resource_config() or {}
    payload.setdefault('p115_shared_resource_enabled', False)
    payload.setdefault('p115_shared_center_url', 'https://shared.55565576.xyz')
    payload.setdefault('p115_shared_device_token', '')
    payload.setdefault('p115_shared_install_id', '')
    payload['p115_shared_resource_mode'] = 'rapid'
    payload.setdefault('p115_shared_disable_episode_transfer', False)
    payload.setdefault('p115_shared_block_clean_version_transfer', False)
    payload.setdefault('p115_shared_auto_share_requests_enabled', False)
    # Rapid v2 没有分享上限；字段保留给前端展示为 0。
    payload['p115_shared_max_active_shares'] = 0
    return payload


def _save_shared_config(data: Dict[str, Any]) -> Dict[str, Any]:
    data = dict(data or {})
    data['p115_shared_resource_enabled'] = _boolish(data.get('p115_shared_resource_enabled'), False)
    data['p115_shared_center_url'] = str(data.get('p115_shared_center_url') or 'https://shared.55565576.xyz').rstrip('/')
    data['p115_shared_device_token'] = str(data.get('p115_shared_device_token') or '').strip()
    data['p115_shared_resource_mode'] = 'rapid'
    data['p115_shared_max_active_shares'] = 0
    data['p115_shared_disable_episode_transfer'] = _boolish(data.get('p115_shared_disable_episode_transfer'), False)
    data['p115_shared_block_clean_version_transfer'] = _boolish(data.get('p115_shared_block_clean_version_transfer'), False)
    data['p115_shared_auto_share_requests_enabled'] = _boolish(data.get('p115_shared_auto_share_requests_enabled'), False)
    install_id = str(data.get('p115_shared_install_id') or '').strip()
    if not install_id:
        install_id = uuid.uuid4().hex
    data['p115_shared_install_id'] = install_id
    return settings_db.save_shared_resource_config(data)


def _center_request_kwargs(timeout: int) -> Dict[str, Any]:
    import config_manager
    kwargs = {'timeout': timeout}
    getter = getattr(config_manager, 'get_proxies_for_requests', None)
    if callable(getter):
        proxies = getter()
        if proxies:
            kwargs['proxies'] = proxies
    return kwargs


def _center_headers_for_cfg(cfg: Dict[str, Any]) -> Dict[str, str]:
    return {
        'X-Device-Token': str((cfg or {}).get('p115_shared_device_token') or '').strip(),
        'Content-Type': 'application/json',
        'X-Client-Version': str(getattr(constants, 'APP_VERSION', '0.0.0') or '0.0.0'),
    }


def _fetch_center_credit() -> Dict[str, Any]:
    client = SharedCenterClient()
    me = client.me()
    stats = client.stats()
    ledger = {}
    try:
        ledger = client.credit_ledger(limit=500)
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 拉取中心贡献值流水失败: {e}")
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
    synced_ledger = shared_credit_db.sync_center_credit_ledger(ledger.get('items') or [], device_snapshot=me)
    return {'ok': True, 'snapshot': saved, 'synced_ledger': synced_ledger}


def _decorate_local_source(row: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(row or {})
    # 前端旧列名兼容显示，但语义已变成 Rapid 源。
    row['share_code'] = row.get('center_source_id') or row.get('source_key') or ''
    row.setdefault('receive_code', '')
    row.setdefault('share_url', '')
    row['share_type'] = row.get('source_kind')
    row['review_status'] = row.get('status')
    row['root_is_dir'] = row.get('source_kind') == 'completed_season'
    row['raw_uploaded_count'] = row.get('file_count') or (1 if row.get('center_status') == 'reported' else 0)
    row['center_reported_count'] = row.get('file_count') or (1 if row.get('center_status') == 'reported' else 0)
    row['source_provider_label'] = {
        'manual_rapid': '手动登记',
        'rapid_auto_library': '入库自动登记',
        'rapid_all_library': '一键全库登记',
        'auto_library': '入库自动登记',
        'share_all_library': '一键全库登记',
        'rapid_completed_season': '完结季收藏源',
    }.get(row.get('source_provider'), row.get('source_provider') or '本地秒传源')
    return row


@shared_resource_bp.route('/config', methods=['GET', 'POST'])
@admin_required
def api_shared_resource_config():
    if request.method == 'GET':
        return jsonify({'success': True, 'data': _shared_resource_config_payload()})
    payload = _save_shared_config(_request_json())
    return jsonify({'success': True, 'message': '共享资源配置已保存（Rapid v2：中心不存 CK、不创建 115 分享）', 'data': payload})


@shared_resource_bp.route('/summary', methods=['GET'])
@admin_required
def api_shared_resource_summary():
    return jsonify({'success': True, 'data': shared_credit_db.get_shared_resource_summary()})


@shared_resource_bp.route('/shares', methods=['GET'])
@admin_required
def api_list_local_sources():
    status = request.args.get('status') or 'all'
    keyword = request.args.get('keyword') or request.args.get('q') or ''
    page = int(request.args.get('page') or 1)
    page_size = int(request.args.get('page_size') or 30)
    rows, total = shared_share_db.list_local_sources(status=status, keyword=keyword, page=page, page_size=page_size)
    return jsonify({'success': True, 'items': [_decorate_local_source(r) for r in rows], 'total': total})


@shared_resource_bp.route('/shares/<int:source_id>/check', methods=['POST'])
@admin_required
def api_check_local_source(source_id: int):
    row = shared_share_db.get_local_source(source_id)
    if not row:
        return jsonify({'success': False, 'message': '本地共享源不存在'}), 404
    return jsonify({'success': True, 'message': 'Rapid v2 本地源无需 115 分享审核', 'item': _decorate_local_source(row)})


@shared_resource_bp.route('/shares/<int:source_id>/report-center', methods=['POST'])
@admin_required
def api_report_local_source(source_id: int):
    row = shared_share_db.get_local_source(source_id)
    if not row:
        return jsonify({'success': False, 'message': '本地共享源不存在'}), 404
    candidate = {
        'tmdb_id': row.get('tmdb_id'), 'item_type': row.get('item_type'), 'season_number': row.get('season_number'),
        'episode_number': row.get('episode_number'), 'title': row.get('title'), 'release_year': row.get('release_year'),
    }
    result = shared_tasks.register_candidate_to_center(candidate, source_provider=row.get('source_provider') or 'manual_rapid')
    return jsonify({'success': bool(result.get('ok')), 'message': result.get('message') or '已登记中心', 'data': result})


@shared_resource_bp.route('/shares/<int:source_id>/cancel', methods=['POST'])
@admin_required
def api_disable_local_source(source_id: int):
    row = shared_share_db.get_local_source(source_id)
    if not row:
        return jsonify({'success': False, 'message': '本地共享源不存在'}), 404
    center_resp = {}
    if row.get('center_source_id'):
        try:
            center_resp = SharedCenterClient().disable_source(row.get('source_kind'), row.get('center_source_id'), message='local disabled')
        except Exception as e:
            center_resp = {'ok': False, 'message': str(e)}
    saved = shared_share_db.update_local_source(source_id, status='disabled', center_status='disabled', disabled_at='NOW()', raw_json={'center_response': center_resp})
    return jsonify({'success': True, 'message': '已停用本地共享源；不会再主动供给该资源', 'item': _decorate_local_source(saved), 'center': center_resp})


@shared_resource_bp.route('/media/search', methods=['GET'])
@admin_required
def api_search_shareable_media():
    keyword = request.args.get('keyword') or request.args.get('q') or ''
    limit = int(request.args.get('limit') or 100)
    items = shared_share_db.search_shareable_media(keyword, search_limit=max(limit * 3, 100), result_limit=limit)
    return jsonify({'success': True, 'items': items})


@shared_resource_bp.route('/shares/manual-validate', methods=['POST'])
@admin_required
def api_manual_validate():
    """Rapid v2 手动登记前预校验。

    前端仍沿用 /shares/manual-validate 这个路由名，但这里不再校验 115 分享码/提取码，
    只确认本地能定位到可秒传文件，并检查 RAW 媒体信息是否可用于中心展示/匹配。
    """
    data = _request_json()
    files = shared_share_db.collect_files_for_candidate(data)
    missing_raw = []
    for f in files:
        sha1 = str(f.get('sha1') or '').upper()
        if sha1 and not (shared_share_db.raw_ffprobe_for_sha1(sha1) or {}).get('raw_ffprobe_json'):
            missing_raw.append({'sha1': sha1, 'file_name': f.get('file_name')})

    if not files:
        message = '没有找到可登记视频文件'
        valid = False
    elif missing_raw:
        message = f'找到 {len(files)} 个视频文件，但有 {len(missing_raw)} 个缺少 RAW 媒体信息，暂不登记中心'
        valid = False
    else:
        message = f'找到 {len(files)} 个可登记视频文件，可登记为 Rapid v2 共享源'
        valid = True

    data_payload = {
        'valid': valid,
        'message': message,
        'file_count': len(files),
        'missing_raw': missing_raw,
        'files': files,
    }
    return jsonify({
        'success': True,
        'message': message,
        'data': data_payload,
        'files': files,
        'missing_raw': missing_raw,
    })


@shared_resource_bp.route('/shares/manual-create', methods=['POST'])
@admin_required
def api_manual_create():
    data = _request_json()
    result = shared_tasks.register_candidate_to_center(data, source_provider='manual_rapid')
    status = 200 if result.get('ok') else 400
    return jsonify({'success': bool(result.get('ok')), 'message': result.get('message') or '共享源已登记中心', 'data': result}), status


@shared_resource_bp.route('/shares/share-library', methods=['POST'])
@admin_required
def api_share_library():
    data = _request_json()
    max_items = int(data.get('max_items') or 100000)
    # 默认异步，避免前端等待全库扫描。
    def _runner():
        try:
            shared_tasks.share_all_library(max_items=max_items)
        except Exception as e:
            logger.error(f"  ➜ [共享资源] 一键登记媒体库任务失败: {e}", exc_info=True)
    threading.Thread(target=_runner, name='shared-rapid-register-all-library', daemon=True).start()
    return jsonify({'success': True, 'message': '已启动一键登记媒体库任务；不会创建 115 分享，只登记中心秒传索引'})


@shared_resource_bp.route('/center/sources', methods=['GET'])
@admin_required
def api_center_sources():
    try:
        client = SharedCenterClient()
        resp = client.list_sources(
            q=request.args.get('q') or request.args.get('keyword') or '',
            status=request.args.get('status') or 'alive,available,updating,inconsistent,incomplete',
            mine_only=_boolish(request.args.get('mine_only'), False),
            source_kind=request.args.get('source_kind') or '',
            item_type=request.args.get('item_type') or '',
            tmdb_id=request.args.get('tmdb_id') or '',
            limit=int(request.args.get('limit') or request.args.get('page_size') or 200),
            offset=int(request.args.get('offset') or 0),
        )
        return jsonify({'success': True, **resp})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e), 'items': [], 'total': 0}), 500


@shared_resource_bp.route('/center/import', methods=['POST'])
@admin_required
def api_center_import():
    data = _request_json()
    source = data.get('source') if isinstance(data.get('source'), dict) else data
    source_kind = source.get('source_kind') or source.get('kind') or ''
    source_id = source.get('source_id') or source.get('source_ref_id') or ''
    event = {'event_id': '', 'source_kind': source_kind, 'source_ref_id': source_id, 'payload_json': source}
    result = consume_device_event(event)
    status = 200 if result.get('ok') else 400
    return jsonify({'success': bool(result.get('ok')), 'message': f"秒传完成：{result.get('success_count', 0)}/{result.get('total', 0)}", 'data': result}), status


@shared_resource_bp.route('/center/device/register', methods=['POST'])
@admin_required
def api_register_center_device():
    cfg = _shared_resource_config_payload()
    install_id = str(cfg.get('p115_shared_install_id') or '').strip() or uuid.uuid4().hex
    name = socket.gethostname() or 'ETK Device'
    try:
        client = SharedCenterClient()
        resp = client.register_device(name=name, install_id=install_id)
        cfg.update({
            'p115_shared_device_token': resp.get('device_token') or '',
            'p115_shared_install_id': install_id,
            'p115_shared_resource_enabled': True,
        })
        saved = _save_shared_config(cfg)
        try:
            shared_tasks.ensure_shared_device_event_listener()
        except Exception:
            pass
        return jsonify({'success': True, 'message': '共享中心设备注册成功', 'data': saved, 'device': resp})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@shared_resource_bp.route('/credit/refresh', methods=['POST'])
@admin_required
def api_refresh_credit():
    try:
        data = _fetch_center_credit()
        return jsonify({'success': True, 'message': '贡献值已同步', 'data': data})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@shared_resource_bp.route('/credit/ledger', methods=['GET'])
@admin_required
def api_credit_ledger():
    if _boolish(request.args.get('sync_center'), False):
        try:
            _fetch_center_credit()
        except Exception as e:
            logger.warning(f"  ➜ [共享资源] 同步中心贡献值失败: {e}")
    limit = int(request.args.get('limit') or 200)
    items = shared_credit_db.list_credit_ledger(limit=limit, actual_only=_boolish(request.args.get('actual_only'), False))
    return jsonify({'success': True, 'items': items})


@shared_resource_bp.route('/maintenance/run', methods=['POST'])
@admin_required
def api_run_maintenance():
    ok = shared_tasks.trigger_shared_resource_maintenance_task()
    return jsonify({'success': bool(ok), 'message': '共享资源维护任务已提交' if ok else '共享资源维护任务提交失败'})


# 前端旧入口：执行维护任务是通过 /api/tasks/run 调度，这里保留中心补源接口为空实现。
@shared_resource_bp.route('/center/replenish/prepare', methods=['POST'])
@admin_required
def api_replenish_prepare():
    return jsonify({'success': False, 'message': 'Rapid v2 没有待补充分享，直接登记本地资源即可'}), 400


# Rapid v2 暂不保留求分享 UI 的中心接口，返回空列表，避免前端报错。
@shared_resource_bp.route('/share-requests/quote', methods=['POST'])
@admin_required
def api_share_request_quote():
    return jsonify({'success': True, 'ok': True, 'current_bounty': 0, 'max_bounty': 0, 'breakdown': [], 'message': 'Rapid v2 暂不使用求分享悬赏'})


@shared_resource_bp.route('/share-requests/tmdb/search', methods=['GET'])
@admin_required
def api_share_request_tmdb_search():
    return jsonify({'success': True, 'items': []})


@shared_resource_bp.route('/share-requests', methods=['GET', 'POST'])
@admin_required
def api_share_requests():
    if request.method == 'GET':
        return jsonify({'success': True, 'items': [], 'total': 0, 'message': 'Rapid v2 暂不使用求分享悬赏'})
    return jsonify({'success': False, 'message': 'Rapid v2 暂不使用求分享悬赏'}), 400


@shared_resource_bp.route('/share-requests/<group_id>/co-request', methods=['POST'])
@admin_required
def api_share_request_co(group_id):
    return jsonify({'success': False, 'message': 'Rapid v2 暂不使用求分享悬赏'}), 400


@shared_resource_bp.route('/share-requests/<group_id>/cancel', methods=['POST'])
@admin_required
def api_share_request_cancel(group_id):
    return jsonify({'success': True, 'message': 'Rapid v2 暂不使用求分享悬赏'})
