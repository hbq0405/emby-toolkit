# routes/shared_resource.py
# Rapid v2 共享资源 API：不再创建/管理 115 分享；只登记秒传资源索引与消费中心事件。
import json
import logging
import socket
import threading
import uuid
from typing import Any, Dict, List

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
    row['share_type'] = row.get('share_type') or row.get('source_kind')
    row['review_status'] = row.get('status')
    row['root_is_dir'] = row.get('source_kind') == 'completed_season' or row.get('is_aggregated_season')
    item_count = _safe_int(row.get('item_count') or row.get('file_count'), 0)
    if item_count <= 0:
        item_count = 1 if row.get('center_status') == 'reported' else 0
    row['item_count'] = item_count
    row['raw_uploaded_count'] = _safe_int(row.get('raw_uploaded_count'), 0) if row.get('raw_uploaded_count') is not None else item_count
    row['center_reported_count'] = _safe_int(row.get('center_reported_count'), 0) if row.get('center_reported_count') is not None else (item_count if row.get('center_status') == 'reported' else 0)
    row['reported_count'] = _safe_int(row.get('reported_count'), row.get('center_reported_count') or 0)
    row['source_provider_label'] = {
        'manual_rapid': '手动登记',
        'rapid_auto_library': '入库自动登记',
        'rapid_all_library': '一键全库登记',
        'rapid_completed_season': '完结季收藏源',
    }.get(row.get('source_provider'), row.get('source_provider') or '本地秒传源')
    return row


def _safe_int(value, default=0):
    try:
        if value in (None, ''):
            return default
        return int(float(value))
    except Exception:
        return default


def _max_text(values: List[Any]) -> str:
    vals = [str(v) for v in values if v not in (None, '')]
    return max(vals) if vals else ''


def _min_text(values: List[Any]) -> str:
    vals = [str(v) for v in values if v not in (None, '')]
    return min(vals) if vals else ''


def _aggregate_local_sources(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """我的共享源展示聚合。

    Rapid v2 里本机登记到中心的是电影源/分集源/完结季包源。
    UI 不应该把同一季的分集散铺成几十行，否则停用只能一集集点。
    这里只改变本机管理页展示口径：同一 tmdb_id + season_number 的 episode 源聚合为一个季行，
    真正停用时仍按 source_ids 批量停用每个本机源。
    """
    decorated = [_decorate_local_source(r) for r in (rows or []) if isinstance(r, dict)]
    groups: Dict[str, Dict[str, Any]] = {}
    singles: List[Dict[str, Any]] = []

    for row in decorated:
        source_kind = str(row.get('source_kind') or '').strip().lower()
        item_type = str(row.get('item_type') or '').strip().lower()
        season = row.get('season_number')
        tmdb_id = str(row.get('tmdb_id') or '').strip()
        is_episode = source_kind == 'episode' or item_type == 'episode'
        if is_episode and tmdb_id and season not in (None, ''):
            key = f"episode-season:{tmdb_id}:{season}"
            g = groups.get(key)
            if not g:
                g = dict(row)
                g.update({
                    'id': row.get('id'),
                    'source_ids': [],
                    'source_kind': 'episode_group',
                    'share_type': 'season_pack',
                    'item_type': 'Season',
                    'episode_number': None,
                    'center_source_id': '',
                    'source_key': '',
                    'is_aggregated_season': True,
                    'aggregated_source_count': 0,
                    'episode_numbers': [],
                    'item_count': 0,
                    'reported_count': 0,
                    'center_reported_count': 0,
                    'raw_uploaded_count': 0,
                    'size_missing_count': 0,
                    'status_values': [],
                    'center_status_values': [],
                    'created_at_values': [],
                    'updated_at_values': [],
                })
                groups[key] = g
            sid = row.get('id')
            if sid not in g['source_ids']:
                g['source_ids'].append(sid)
            ep_no = row.get('episode_number')
            if ep_no not in (None, ''):
                g['episode_numbers'].append(_safe_int(ep_no, 0))
            count = max(1, _safe_int(row.get('item_count') or row.get('file_count'), 1))
            g['item_count'] += count
            g['reported_count'] += _safe_int(row.get('reported_count') or row.get('center_reported_count'), 0)
            g['center_reported_count'] = g['reported_count']
            g['raw_uploaded_count'] += _safe_int(row.get('raw_uploaded_count'), count)
            g['size_missing_count'] += _safe_int(row.get('size_missing_count'), 0)
            g['aggregated_source_count'] += 1
            g['status_values'].append(str(row.get('status') or ''))
            g['center_status_values'].append(str(row.get('center_status') or ''))
            g['created_at_values'].append(row.get('created_at'))
            g['updated_at_values'].append(row.get('updated_at') or row.get('created_at'))
            continue
        singles.append(row)

    out: List[Dict[str, Any]] = []
    for g in groups.values():
        statuses = [s for s in g.pop('status_values', []) if s]
        center_statuses = [s for s in g.pop('center_status_values', []) if s]
        created_values = g.pop('created_at_values', [])
        updated_values = g.pop('updated_at_values', [])
        if statuses and all(s == 'disabled' for s in statuses):
            g['status'] = 'disabled'
        elif any(s == 'active' for s in statuses):
            g['status'] = 'active'
        if center_statuses and all(s == 'reported' for s in center_statuses):
            g['center_status'] = 'reported'
        elif any(s == 'reported' for s in center_statuses):
            g['center_status'] = 'partial'
        elif center_statuses and all(s == 'disabled' for s in center_statuses):
            g['center_status'] = 'disabled'
        g['episode_numbers'] = sorted({x for x in g.get('episode_numbers') or [] if x})
        g['file_count'] = g['item_count']
        g['created_at'] = _min_text(created_values) or g.get('created_at')
        g['updated_at'] = _max_text(updated_values) or g.get('updated_at')
        g['share_remark'] = f"聚合显示：{g.get('aggregated_source_count') or len(g.get('source_ids') or [])} 个本机分集源"
        out.append(_decorate_local_source(g))

    out.extend(singles)
    out.sort(key=lambda r: str(r.get('updated_at') or r.get('created_at') or ''), reverse=True)
    return out


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
    page = max(1, int(request.args.get('page') or 1))
    page_size = max(1, min(int(request.args.get('page_size') or 30), 200))

    # 先取本机匹配源再聚合，避免同一季 30 多集被分页拆成多页重复季。
    # 这是本机管理页，不是中心资源库；中心资源库仍由中心端 display-list 做分页/筛选/聚合。
    rows, _raw_total = shared_share_db.list_local_sources(status=status, keyword=keyword, page=1, page_size=100000)
    aggregated = _aggregate_local_sources(rows)
    start = (page - 1) * page_size
    end = start + page_size
    return jsonify({'success': True, 'items': aggregated[start:end], 'total': len(aggregated)})


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


@shared_resource_bp.route('/shares/cancel-batch', methods=['POST'])
@admin_required
def api_disable_local_sources_batch():
    data = _request_json()
    raw_ids = data.get('ids') or data.get('source_ids') or []
    ids = []
    for value in raw_ids if isinstance(raw_ids, list) else []:
        sid = _safe_int(value, 0)
        if sid > 0 and sid not in ids:
            ids.append(sid)
    if not ids:
        return jsonify({'success': False, 'message': '缺少要停用的本地源 ID'}), 400

    disabled = []
    missing = []
    center_results = []
    for sid in ids:
        row = shared_share_db.get_local_source(sid)
        if not row:
            missing.append(sid)
            continue
        center_resp = {}
        if row.get('center_source_id'):
            try:
                center_resp = SharedCenterClient().disable_source(row.get('source_kind'), row.get('center_source_id'), message='local disabled batch')
            except Exception as e:
                center_resp = {'ok': False, 'message': str(e)}
        saved = shared_share_db.update_local_source(
            sid, status='disabled', center_status='disabled', disabled_at='NOW()', raw_json={'center_response': center_resp}
        )
        disabled.append(_decorate_local_source(saved))
        center_results.append({'id': sid, 'center': center_resp})

    return jsonify({
        'success': True,
        'message': f'已停用 {len(disabled)} 个本地共享源' + (f'，{len(missing)} 个不存在' if missing else ''),
        'items': disabled,
        'center': center_results,
        'missing': missing,
    })


@shared_resource_bp.route('/media/search', methods=['GET'])
@admin_required
def api_search_shareable_media():
    keyword = request.args.get('keyword') or request.args.get('q') or ''
    limit = int(request.args.get('limit') or 100)
    # 手动添加共享源的最小粒度是“电影 / 季”。
    # 单集可作为连载季公共包下的子项登记，但不再出现在搜索候选里，避免用户选择单集后无法追踪整季。
    rows = shared_share_db.search_shareable_media(keyword, search_limit=max(limit * 6, 200), result_limit=max(limit * 4, 100))
    items = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        item_type = str(row.get('share_item_type') or row.get('item_type') or '').strip().lower()
        share_type = str(row.get('share_type') or '').strip().lower()
        if item_type in ('episode', 'episode_file') or share_type == 'episode_file' or row.get('episode_number') not in (None, '', 0):
            continue
        if item_type in ('movie', 'season', 'series') or share_type in ('movie_file', 'movie_folder', 'season_pack', 'series_pack'):
            items.append(row)
        if len(items) >= limit:
            break
    return jsonify({'success': True, 'items': items})


@shared_resource_bp.route('/shares/manual-validate', methods=['POST'])
@admin_required
def api_manual_validate():
    """Rapid v2 手动登记前预校验。

    前端仍沿用 /shares/manual-validate 这个路由名，但这里不再校验 115 分享码/提取码，
    只确认本地能定位到可秒传文件，并检查 RAW 媒体信息是否可用于中心展示/匹配。
    """
    data = _request_json()
    consistency = shared_share_db.repair_candidate_fingerprints(data, log_result=True)
    files = shared_share_db.collect_files_for_candidate(data)
    root = shared_share_db.candidate_root_from_files(files)
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
        'root': root,
        'root_fid': root.get('root_fid') or '',
        'consistency': consistency or {},
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



def _json_dict(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            obj = json.loads(value)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}


def _first_text(*values):
    for value in values:
        if value not in (None, '', [], {}):
            return value
    return ''


def _center_version_summary(row: Dict[str, Any]) -> Dict[str, Any]:
    """中心资源库展示用版本摘要。

    Rapid v2 中心源的媒体参数来自 media_signature_json；中心若同时返回 raw_summary_json/summary_json，
    则作为兜底。前端表格只读 version_summary，避免字段散落导致整列都是“-”。
    """
    row = row or {}
    sig = _json_dict(row.get('media_signature_json') or row.get('media_signature'))
    raw = _json_dict(row.get('raw_summary_json') or row.get('summary_json') or row.get('version_summary'))
    out = {}
    out.update(raw)
    out.update(sig)
    resolution = _first_text(out.get('resolution'), out.get('resolution_display'), raw.get('resolution'), sig.get('resolution'))
    effect = _first_text(out.get('effect'), out.get('effect_display'), out.get('effect_key'), raw.get('effect'), sig.get('effect_key'))
    video_codec = _first_text(out.get('video_codec'), out.get('codec_display'), out.get('codec'), raw.get('video_codec'))
    bit_depth = _first_text(out.get('bit_depth'), raw.get('bit_depth'), sig.get('bit_depth'))
    fps = _first_text(out.get('fps'), out.get('frame_rate'), raw.get('fps'), raw.get('frame_rate'))
    audio_list = out.get('audio_list') or out.get('audio_tracks') or out.get('audios') or []
    subtitle_list = out.get('subtitle_list') or out.get('subtitle_tracks') or out.get('subtitles') or []
    out.update({
        'resolution': resolution,
        'effect': effect,
        'video_codec': video_codec,
        'codec': video_codec,
        'bit_depth': bit_depth,
        'fps': fps,
        'audio_list': audio_list if isinstance(audio_list, list) else ([audio_list] if audio_list else []),
        'subtitle_list': subtitle_list if isinstance(subtitle_list, list) else ([subtitle_list] if subtitle_list else []),
    })
    return {k: v for k, v in out.items() if v not in (None, '', [], {})}

@shared_resource_bp.route('/center/sources', methods=['GET'])
@admin_required
def api_center_sources():
    try:
        client = SharedCenterClient()
        resp = client.list_display_sources(
            q=request.args.get('q') or request.args.get('keyword') or '',
            status=request.args.get('status') or 'alive,available,updating,inconsistent,incomplete',
            item_type=request.args.get('item_type') or '',
            tmdb_id=request.args.get('tmdb_id') or '',
            order_by=request.args.get('order_by') or 'latest',
            limit=int(request.args.get('limit') or request.args.get('page_size') or 200),
            offset=int(request.args.get('offset') or 0),
        )

        def _decorate_center_row(row):
            if not isinstance(row, dict):
                return {}
            row = dict(row)
            for key in ('versions', 'children', 'pack_items'):
                if isinstance(row.get(key), list):
                    row[key] = [_decorate_center_row(x) for x in row.get(key) if isinstance(x, dict)]
            # 连载季公共包没有统一版本参数；完结季包/电影/展开后的集才展示。
            if row.get('is_ongoing_hub') or row.get('source_kind') == 'season_hub':
                row['version_summary'] = {}
                row['summary_json'] = {}
                row['media_signature_json'] = {}
            else:
                row['version_summary'] = _center_version_summary(row)
            if not row.get('size') and row.get('total_size'):
                row['size'] = row.get('total_size')
            return row

        resp['items'] = [_decorate_center_row(row) for row in (resp.get('items') or []) if isinstance(row, dict)]
        return jsonify({'success': True, **resp})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e), 'items': [], 'total': 0}), 500


@shared_resource_bp.route('/center/import', methods=['POST'])
@admin_required
def api_center_import():
    data = _request_json()
    source = data.get('source') if isinstance(data.get('source'), dict) else data

    # 旧前端曾只提交 source_ids/context，没有提交完整中心源行；Rapid v2 需要 source_kind/source_id/sha1 等字段。
    # 这里给出明确错误，避免前端继续显示“秒传完成 0/0”。
    if not isinstance(source, dict):
        source = {}
    source_kind = source.get('source_kind') or source.get('kind') or ''
    source_id = source.get('source_id') or source.get('source_ref_id') or ''
    if (not source_kind or not source_id) and data.get('source_ids'):
        return jsonify({
            'success': False,
            'message': '前端提交的还是旧 source_ids 转存参数，缺少 Rapid v2 的 source_kind/source_id；请覆盖最新 SharedResourceManagerPage.vue。',
            'data': {'ok': False, 'success_count': 0, 'total': 0}
        }), 400

    event = {'event_id': '', 'source_kind': source_kind, 'source_ref_id': source_id, 'payload_json': source}
    result = consume_device_event(event, ack=False)
    status = 200 if result.get('ok') else 400
    message = result.get('message') or f"秒传完成：{result.get('success_count', 0)}/{result.get('total', 0)}"
    return jsonify({'success': bool(result.get('ok')), 'message': message, 'data': result}), status


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
