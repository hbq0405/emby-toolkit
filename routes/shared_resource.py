# routes/shared_resource.py
# Rapid v2 共享资源 API：不再创建/管理 115 分享；只登记秒传资源索引与消费中心事件。
import json
import logging
import re
import socket
import threading
import uuid
from typing import Any, Dict, List

import requests
from flask import Blueprint, jsonify, request

import constants
import config_manager
from extensions import admin_required
from database import shared_credit_db, shared_share_db, settings_db
from handler.shared_center_client import SharedCenterClient
from handler.shared_subscription_service import consume_device_event
from handler import tmdb as tmdb_handler
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
    payload.setdefault('p115_shared_block_short_drama_transfer', False)
    payload.setdefault('p115_shared_auto_share_requests_enabled', False)
    return payload


def _save_shared_config(data: Dict[str, Any]) -> Dict[str, Any]:
    data = dict(data or {})
    data['p115_shared_resource_enabled'] = _boolish(data.get('p115_shared_resource_enabled'), False)
    data['p115_shared_center_url'] = str(data.get('p115_shared_center_url') or 'https://shared.55565576.xyz').rstrip('/')
    data['p115_shared_device_token'] = str(data.get('p115_shared_device_token') or '').strip()
    data['p115_shared_resource_mode'] = 'rapid'
    data.pop('p115_shared_max_active_shares', None)
    data['p115_shared_disable_episode_transfer'] = _boolish(data.get('p115_shared_disable_episode_transfer'), False)
    data['p115_shared_block_clean_version_transfer'] = _boolish(data.get('p115_shared_block_clean_version_transfer'), False)
    data['p115_shared_block_short_drama_transfer'] = _boolish(data.get('p115_shared_block_short_drama_transfer'), False)
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
    pro_report = {}
    try:
        pro_report = client.report_current_pro_quota_auth()
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] Pro 额度认证上报失败，继续同步贡献点: {e}")
    me = client.me()
    stats = client.stats()
    ledger = {}
    try:
        ledger = client.credit_ledger(limit=500)
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 拉取中心贡献点流水失败: {e}")
    snapshot = {
        'device_id': me.get('id'),
        'credit': int(me.get('credit') or 0),
        # Rapid v2 入库即共享后，普通缺口数量不再作为首页统计；首页展示主动“求共享”数量。
        'wanted_gaps': int(stats.get('active_share_requests') if stats.get('active_share_requests') is not None else stats.get('active_gap_devices') or 0),
        'share_requests': int(stats.get('active_share_requests') if stats.get('active_share_requests') is not None else stats.get('active_gap_devices') or 0),
        'shared_sources': int(stats.get('movie_sources') or 0) + int(stats.get('episode_sources') or 0) + int(stats.get('completed_season_sources') or 0),
        'raw_ffprobe': int(stats.get('raw_ffprobe') or 0),
        'display_movie_count': int(stats.get('display_movie_count') or (stats.get('media_stats') or {}).get('movie_count') or stats.get('movie_sources') or 0),
        'display_season_count': int(stats.get('display_season_count') or (stats.get('media_stats') or {}).get('season_count') or 0),
        'video_count': int(stats.get('video_count') or (stats.get('media_stats') or {}).get('video_count') or stats.get('raw_ffprobe') or 0),
        'media_stats': stats.get('media_stats') or {
            'movie_count': int(stats.get('display_movie_count') or stats.get('movie_sources') or 0),
            'season_count': int(stats.get('display_season_count') or 0),
            'video_count': int(stats.get('video_count') or stats.get('raw_ffprobe') or 0),
        },
        'pro_quota': (pro_report.get('pro_quota') or pro_report.get('quota') or stats.get('pro_quota') or me.get('pro_quota') or {}),
        'remote_devices': int(stats.get('devices') or 0),
        'raw_json': {'me': me, 'stats': stats},
    }
    saved = shared_credit_db.upsert_credit_snapshot(snapshot)
    center_ledger_items = ledger.get('items') or []
    synced_ledger = shared_credit_db.sync_center_credit_ledger(center_ledger_items, device_snapshot=me)
    return {'ok': True, 'snapshot': saved, 'synced_ledger': synced_ledger, 'center_ledger_items': center_ledger_items}


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
    provider = str(row.get('source_provider') or '').strip()
    raw_json = row.get('raw_json') if isinstance(row.get('raw_json'), dict) else {}
    raw_provider_text = ' '.join(str(raw_json.get(k) or '') for k in (
        'source_provider', 'register_source', 'register_from', 'task_source',
        'task_type', 'source_provider_label', 'source_label', 'message', 'reason'
    ))
    auto_provider_values = {'rapid_auto_library', 'rapid_all_library'}
    auto_provider_keywords = ('入库自动', '自动登记', '自动共享', '一键全库')
    row['is_auto_share'] = bool(
        provider in auto_provider_values
        or row.get('is_auto_share')
        or row.get('auto_created')
        or row.get('auto_registered')
        or row.get('created_by_task')
        or row.get('from_maintenance')
        or any(k in raw_provider_text for k in auto_provider_keywords)
    )
    row['source_provider_label'] = {
        'manual_rapid': '手动登记',
        'rapid_auto_library': '入库自动登记',
        'rapid_all_library': '一键全库登记',
        'rapid_completed_season': '完结季源',
    }.get(provider, provider or '本地秒传源')
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
    """我的共享源展示聚合。"""
    # 【核心优化】直接使用轻量级字典，不执行任何耗时操作
    raw_rows = [r for r in (rows or []) if isinstance(r, dict)]
    groups: Dict[str, Dict[str, Any]] = {}
    singles: List[Dict[str, Any]] = []

    for row in raw_rows:
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
        out.append(g)

    out.extend(singles)
    out.sort(key=lambda r: str(r.get('updated_at') or r.get('created_at') or ''), reverse=True)
    return out


def _lookup_local_season_meta(tmdb_id: str, season_number) -> Dict[str, Any]:
    """从本机 media_metadata 补齐季总集数和追剧状态。

    中心端公共连载季只知道当前已聚合的集数；总集数应优先相信本机
    media_metadata.total_episodes，追剧状态只看 watching_status。
    """
    tmdb_id = str(tmdb_id or '').strip()
    season_no = _safe_int(season_number, 0)
    if not tmdb_id or season_no <= 0:
        return {}
    try:
        from database.connection import get_db_connection
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT tmdb_id, parent_series_tmdb_id, season_number, total_episodes, watching_status
                    FROM media_metadata
                    WHERE item_type='Season'
                      AND season_number=%s
                      AND (tmdb_id=%s OR parent_series_tmdb_id=%s)
                    ORDER BY CASE WHEN parent_series_tmdb_id=%s THEN 0 ELSE 1 END,
                             last_updated_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (season_no, tmdb_id, tmdb_id, tmdb_id),
                )
                row = cur.fetchone()
                return dict(row) if row else {}
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 查询本机季元数据失败: tmdb_id={tmdb_id}, season={season_no}, err={e}")
        return {}


def _apply_local_season_meta(row: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(row or {})
    item_type = str(row.get('item_type') or row.get('display_type') or '').strip().lower()
    source_kind = str(row.get('source_kind') or '').strip().lower()
    if item_type not in ('season', 'pack') and source_kind not in ('season_hub', 'completed_season'):
        return row
    meta = _lookup_local_season_meta(row.get('tmdb_id'), row.get('season_number'))
    if not meta:
        return row
    total = _safe_int(meta.get('total_episodes'), 0)
    if total > 0:
        row['expected_episode_count'] = total
        row['total_episodes'] = total
        row['progress_total'] = total
        current = _safe_int(row.get('progress_current') or row.get('pack_item_count') or row.get('file_count'), 0)
        if current > 0:
            row['progress_text'] = f"{current}/{total}"
    watching_status = str(meta.get('watching_status') or '').strip()
    if watching_status:
        row['watching_status'] = watching_status
    return row


def _share_status_tokens(value: Any) -> List[str]:
    tokens = []
    for token in str(value or 'usable').split(','):
        token = token.strip().lower()
        if token and token not in tokens:
            tokens.append(token)
    return tokens or ['usable']


def _share_row_matches_filter(row: Dict[str, Any], status_filter: str) -> bool:
    """我的共享源筛选口径。

    Rapid v2 本地源的 status 表示本地源状态，center_status 表示中心登记状态。
    旧分享模式的“已登记/部分登记”直接按 status 查会把 available/active 源过滤没，
    所以这里统一在聚合后按 Rapid 语义筛选。
    """
    tokens = _share_status_tokens(status_filter)
    if any(t in {'all', '全部', '全部状态'} for t in tokens):
        return True

    row = row if isinstance(row, dict) else {}
    status = str(row.get('status') or row.get('review_status') or '').strip().lower()
    center_status = str(row.get('center_status') or '').strip().lower()
    source_kind = str(row.get('source_kind') or '').strip().lower()
    has_center_id = bool(str(row.get('center_source_id') or '').strip())

    live = status in {'active', 'available'}
    disabled = status in {'disabled', 'cancelled', 'canceled', 'deleted'} or center_status in {'disabled', 'cancelled', 'canceled'}
    failed = status in {'failed', 'error', 'dead', 'expired', 'rejected', 'inconsistent', 'incomplete', 'raw_missing'} or center_status in {'failed', 'error', 'dead', 'expired', 'rejected', 'raw_missing'}
    reported = center_status in {'reported', 'partial'} or has_center_id
    local_only = not has_center_id and center_status in {'', 'local', 'pending', 'not_reported'}

    for token in tokens:
        if token in {'usable', 'active', 'alive', 'valid', 'valid_share', '有效', '有效共享'}:
            if live and not disabled and not failed:
                return True
        elif token in {'reported', 'center_reported', 'registered', '已登记', '已登记中心', '已上报'}:
            if reported:
                return True
        elif token in {'partial', '部分登记'}:
            if center_status == 'partial':
                return True
        elif token in {'local', 'local_only', 'unreported', 'not_reported', '本地', '本地未登记', '未登记'}:
            if local_only:
                return True
        elif token in {'failed', 'error', 'abnormal', 'invalid', '失败', '异常', '不合格'}:
            if failed:
                return True
        elif token in {'disabled', 'cancelled', 'canceled', 'deleted', '停用', '已停用', '已取消'}:
            if disabled:
                return True
        elif token == status or token == center_status or token == source_kind:
            return True
    return False


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
    status = request.args.get('status') or 'usable'
    keyword = request.args.get('keyword') or request.args.get('q') or ''
    page = max(1, int(request.args.get('page') or 1))
    page_size = max(1, min(int(request.args.get('page_size') or 30), 200))
    raw_limit = max(1000, min(int(request.args.get('raw_limit') or 200000), 200000))

    from database.connection import get_db_connection
    from database.shared_share_db import _local_sources_where_sql, _local_sources_order_sql, _rows

    where_sql, args = _local_sources_where_sql(status='all', keyword=keyword)
    order_sql = _local_sources_order_sql(order_by='updated_desc')

    # 1. 极速轻量级查询：绝不查任何 JSONB 字段，只查聚合和过滤需要的基础列
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS n FROM shared_rapid_sources {where_sql}", args)
            row = cur.fetchone()
            raw_total = dict(row)['n'] if row else 0

            cur.execute(f"""
                SELECT 
                    id, source_kind, item_type, tmdb_id, season_number, episode_number, 
                    status, center_status, center_source_id, created_at, updated_at, 
                    file_count, title, file_name, source_provider
                FROM shared_rapid_sources 
                {where_sql} 
                ORDER BY {order_sql} 
                LIMIT %s
            """, args + [raw_limit])
            light_rows = _rows(cur.fetchall())

    # 2. 在内存中进行聚合和过滤（因为没有庞大的 JSON，这一步只需 1-2 毫秒）
    aggregated = _aggregate_local_sources(light_rows)
    filtered = [row for row in aggregated if _share_row_matches_filter(row, status)]
    
    start = (page - 1) * page_size
    end = start + page_size
    page_items = filtered[start:end]

    # 3. 提取当前页需要展示的真实数据库 ID
    needed_ids = set()
    for item in page_items:
        if item.get('source_ids'):
            needed_ids.update(item['source_ids'])
        elif item.get('id'):
            needed_ids.add(item['id'])

    # 4. 只为这 30 条数据去数据库拉取完整的 JSONB 字段 (耗时极低)
    full_rows_map = {}
    if needed_ids:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT id, raw_json FROM shared_rapid_sources WHERE id = ANY(%s)", (list(needed_ids),))
                for r in _rows(cur.fetchall()):
                    full_rows_map[r['id']] = r['raw_json']

    # 5. 将完整的 raw_json 拼装回去，并执行装饰器
    final_items = []
    for item in page_items:
        if item.get('source_ids'):
            # 聚合季包：取第一个子项的 raw_json 作为代表
            rep_id = item['source_ids'][0] if item['source_ids'] else None
            item['raw_json'] = full_rows_map.get(rep_id, {})
        else:
            item['raw_json'] = full_rows_map.get(item.get('id'), {})
        
        final_items.append(_decorate_local_source(item))

    return jsonify({
        'success': True,
        'items': final_items,
        'total': len(filtered),
        'raw_total': raw_total,
        'scanned_raw': len(light_rows),
        'page': page,
        'page_size': page_size,
    })


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



@shared_resource_bp.route('/shares/<int:source_id>/reregister', methods=['POST'])
@admin_required
def api_reregister_local_source(source_id: int):
    """重新登记本地源：重新上传 RAW/summary_json，并恢复中心可用状态。"""
    row = shared_share_db.get_local_source(source_id)
    if not row:
        return jsonify({'success': False, 'message': '本地共享源不存在'}), 404
    result = shared_tasks.reregister_local_source(source_id)
    status = 200 if result.get('ok') else 400
    return jsonify({'success': bool(result.get('ok')), 'message': result.get('message') or '重新登记完成', 'data': result}), status


@shared_resource_bp.route('/shares/reregister-batch', methods=['POST'])
@admin_required
def api_reregister_local_sources_batch():
    data = _request_json()
    raw_ids = data.get('ids') or data.get('source_ids') or []
    ids = []
    for value in raw_ids if isinstance(raw_ids, list) else []:
        sid = _safe_int(value, 0)
        if sid > 0 and sid not in ids:
            ids.append(sid)
    if not ids:
        return jsonify({'success': False, 'message': '缺少要重新登记的本地源 ID'}), 400
    result = shared_tasks.reregister_local_sources(ids)
    status = 200 if result.get('ok') else 400
    return jsonify({'success': bool(result.get('ok')), 'message': result.get('message') or '重新登记完成', 'data': result}), status



def _local_source_requires_center_cancel(row: Dict[str, Any]) -> bool:
    row = row if isinstance(row, dict) else {}
    if not str(row.get('center_source_id') or '').strip():
        return False
    status = str(row.get('status') or '').strip().lower()
    center_status = str(row.get('center_status') or '').strip().lower()
    if status in {'disabled', 'cancelled', 'canceled', 'deleted'}:
        return False
    if center_status in {'disabled', 'cancelled', 'canceled', 'deleted'}:
        return False
    return bool(status in {'active', 'available', 'updating', 'pending', ''} or center_status in {'reported', 'partial', 'local', 'pending', ''})


def _cancel_center_source_for_local_row(row: Dict[str, Any], message: str = 'local delete') -> Dict[str, Any]:
    row = row if isinstance(row, dict) else {}
    if not _local_source_requires_center_cancel(row):
        return {'ok': True, 'skipped': True, 'reason': 'local_only_or_already_disabled'}
    try:
        return SharedCenterClient().disable_source(row.get('source_kind'), row.get('center_source_id'), message=message)
    except Exception as e:
        return {'ok': False, 'message': str(e)}


def _delete_local_source_with_center_cancel(source_id: int, *, message: str = 'local delete') -> Dict[str, Any]:
    row = shared_share_db.get_local_source(source_id)
    if not row:
        return {'ok': False, 'missing': True, 'id': source_id, 'message': '本地共享源不存在'}
    center_resp = _cancel_center_source_for_local_row(row, message=message)
    if _local_source_requires_center_cancel(row) and center_resp.get('ok') is False:
        return {'ok': False, 'id': source_id, 'center': center_resp, 'message': center_resp.get('message') or '中心取消登记失败，未删除本地数据'}
    deleted = shared_share_db.delete_local_source(source_id)
    return {
        'ok': bool(deleted),
        'id': source_id,
        'item': _decorate_local_source(row),
        'deleted': deleted,
        'center': center_resp,
        'center_cancelled': bool(center_resp and not center_resp.get('skipped') and center_resp.get('ok') is not False),
    }


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



@shared_resource_bp.route('/shares/<int:source_id>/delete', methods=['POST', 'DELETE'])
@admin_required
def api_delete_local_source(source_id: int):
    result = _delete_local_source_with_center_cancel(source_id, message='local delete')
    status = 200 if result.get('ok') else (404 if result.get('missing') else 400)
    message = '已删除本地共享源'
    center = result.get('center') or {}
    if result.get('center_cancelled'):
        message = '已同步中心取消登记，并删除本地共享源'
    elif center.get('skipped'):
        message = '共享源已停用或未登记中心，已直接删除本地数据'
    return jsonify({'success': bool(result.get('ok')), 'message': result.get('message') or message, 'data': result}), status


@shared_resource_bp.route('/shares/delete-batch', methods=['POST'])
@admin_required
def api_delete_local_sources_batch():
    data = _request_json()
    raw_ids = data.get('ids') or data.get('source_ids') or []
    ids = []
    for value in raw_ids if isinstance(raw_ids, list) else []:
        sid = _safe_int(value, 0)
        if sid > 0 and sid not in ids:
            ids.append(sid)
    if not ids:
        return jsonify({'success': False, 'message': '缺少要删除的本地源 ID'}), 400

    deleted = []
    missing = []
    failed = []
    center_results = []
    for sid in ids:
        result = _delete_local_source_with_center_cancel(sid, message='local delete batch')
        if result.get('missing'):
            missing.append(sid)
            continue
        if not result.get('ok'):
            failed.append({'id': sid, 'message': result.get('message'), 'center': result.get('center')})
            continue
        deleted.append(result.get('item') or {'id': sid})
        center_results.append({'id': sid, 'center': result.get('center'), 'center_cancelled': result.get('center_cancelled')})

    success = not failed
    parts = [f'已删除 {len(deleted)} 个本地共享源']
    cancelled_count = len([x for x in center_results if x.get('center_cancelled')])
    if cancelled_count:
        parts.append(f'同步取消中心登记 {cancelled_count} 个')
    if missing:
        parts.append(f'{len(missing)} 个不存在')
    if failed:
        parts.append(f'{len(failed)} 个中心取消失败未删除')
    return jsonify({
        'success': success,
        'message': '，'.join(parts),
        'items': deleted,
        'center': center_results,
        'missing': missing,
        'failed': failed,
    }), 200 if success else 400


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
        row = _apply_local_season_meta(row)
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
    data = shared_tasks._normalize_series_candidate_identity(_request_json())
    # 手动预校验只检查本地是否能定位视频、是否能生成 RAW/summary_json。
    # 不再调用 repair_candidate_fingerprints，避免连载季/维护前预检触发季包一致性校验。
    data['_skip_fingerprint_repair'] = True
    consistency = {}
    files = shared_share_db.collect_files_for_candidate(data)
    root = shared_share_db.candidate_root_from_files(files)
    missing_raw = []
    for f in files:
        sha1 = str(f.get('sha1') or '').upper()
        # 不只检查 RAW 是否存在，还要确认能生成中心资源库展示用 summary_json。
        try:
            entry = shared_tasks._prepare_raw_upload_entry(f)
        except Exception:
            entry = {}
        if sha1 and not entry:
            missing_raw.append({'sha1': sha1, 'file_name': f.get('file_name'), 'reason': 'RAW 或 summary_json 缺失'})

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


def _center_nested_rows(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if isinstance(row, dict):
        out.append(row)
        for key in ('versions', 'children', 'pack_items'):
            value = row.get(key)
            if isinstance(value, list):
                out.extend([x for x in value if isinstance(x, dict)])
    return out


def _bool_state(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in ('1', 'true', 'yes', 'y', 'on', '启用', '开启', '是'):
        return True
    if text in ('0', 'false', 'no', 'n', 'off', '停用', '关闭', '否'):
        return False
    return None


def _center_direct_flag_state(row: Dict[str, Any], flag_key: str, meta_key: str):
    """只读取顶层/顶层摘要容器里的显式标签状态。

    中心端会给聚合季包写入聚合后的 is_short_drama=false。
    这时不能再递归子项，否则历史子项残留的 true 会把季包重新污染。
    """
    row = row if isinstance(row, dict) else {}
    for container_key in (
        '', meta_key, 'version_summary', 'summary_json', 'media_signature_json', 'raw_summary_json', 'rapid_meta_json'
    ):
        container = row if not container_key else _json_dict(row.get(container_key))
        if not isinstance(container, dict):
            continue
        state = _bool_state(container.get(flag_key)) if flag_key in container else None
        if state is not None:
            return state
        meta = _json_dict(container.get(meta_key))
        state = _bool_state(meta.get(flag_key)) if flag_key in meta else None
        if state is not None:
            return state
    return None


def _center_flag_meta(row: Dict[str, Any], flag_key: str, meta_key: str) -> Dict[str, Any]:
    for part in _center_nested_rows(row):
        for container_key in (
            '', 'version_summary', 'summary_json', 'media_signature_json', 'raw_summary_json',
            'rapid_meta_json', 'clean_version_meta_json', 'short_drama_meta_json',
            'animation_meta_json', 'completed_certified_meta_json',
        ):
            container = part if not container_key else _json_dict(part.get(container_key))
            if not isinstance(container, dict):
                continue
            meta = _json_dict(container.get(meta_key))
            if _boolish(container.get(flag_key), False) or _boolish(meta.get(flag_key), False):
                if not meta:
                    meta = {flag_key: True}
                meta.setdefault(flag_key, True)
                return meta
    return {}


def _center_source_is_animation(row: Dict[str, Any]) -> bool:
    return bool(_center_flag_meta(row, 'is_animation', 'animation_meta_json'))


def _center_source_is_completed_certified(row: Dict[str, Any]) -> bool:
    """中心资源库“已完结认证”只认 available 的 completed_season。

    不能用 Season 类型、进度满、watching_status=Completed 或 source_kind=completed_season 兜底，
    否则历史脏数据 status=alive 的 completed_season 也会被前端打上“已完结”。
    """
    row = row if isinstance(row, dict) else {}
    source_kind = str(row.get('source_kind') or '').strip().lower()
    status = str(row.get('status') or '').strip().lower()
    if source_kind == 'completed_season':
        return status == 'available'
    if source_kind == 'season_hub' or row.get('is_ongoing_hub'):
        return False
    return bool(_center_flag_meta(row, 'is_completed_certified', 'completed_certified_meta_json'))


def _center_source_is_clean_version(row: Dict[str, Any]) -> bool:
    return bool(_center_flag_meta(row, 'is_clean_version', 'clean_version_meta_json'))


def _center_source_is_short_drama(row: Dict[str, Any]) -> bool:
    direct = _center_direct_flag_state(row, 'is_short_drama', 'short_drama_meta_json')
    if direct is not None:
        return bool(direct)
    return bool(_center_flag_meta(row, 'is_short_drama', 'short_drama_meta_json'))


def _center_source_transfer_preflight(source: Dict[str, Any]) -> Dict[str, Any]:
    cfg = settings_db.get_shared_resource_config() or {}
    title = str((source or {}).get('title') or (source or {}).get('file_name') or (source or {}).get('source_id') or '').strip()
    if _boolish(cfg.get('p115_shared_block_clean_version_transfer'), False) and _center_source_is_clean_version(source):
        return {
            'ok': False,
            'reason': 'blocked_clean_version',
            'message': f"已开启“不秒传纯净版”，跳过《{title or '该资源'}》。",
        }
    if _boolish(cfg.get('p115_shared_block_short_drama_transfer'), False) and _center_source_is_short_drama(source):
        return {
            'ok': False,
            'reason': 'blocked_short_drama',
            'message': f"已开启“不秒传短剧”，跳过《{title or '该资源'}》。",
        }
    return {'ok': True}


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


def _strip_center_display_children(row: Dict[str, Any]) -> Dict[str, Any]:
    """中心资源库列表兜底瘦身。

    即使中心端旧版本在搜索/筛选时仍返回 children/pack_items，客户端列表接口也只把
    季包壳传给前端；真正展开时再调用 /center/sources/children 按需加载。
    """
    if not isinstance(row, dict):
        return {}
    row = dict(row)
    for key in ('versions',):
        if isinstance(row.get(key), list):
            row[key] = [_strip_center_display_children(x) for x in row.get(key) if isinstance(x, dict)]
    child_values = []
    for key in ('children', 'pack_items'):
        value = row.get(key)
        if isinstance(value, list) and value:
            child_values.extend([x for x in value if isinstance(x, dict)])
        row.pop(key, None)
    if child_values:
        row['has_children'] = True
        row['children_loaded'] = False
        if not row.get('children_count'):
            row['children_count'] = len(child_values)
        if not row.get('child_count'):
            row['child_count'] = row.get('children_count')
        if not row.get('pack_item_count'):
            row['pack_item_count'] = row.get('children_count')
        if not row.get('lazy_children_kind'):
            kind = str(row.get('source_kind') or '').strip().lower()
            row['lazy_children_kind'] = 'season_hub' if kind == 'season_hub' else 'completed_season'
    return row

@shared_resource_bp.route('/center/sources', methods=['GET'])
@admin_required
def api_center_sources():
    try:
        client = SharedCenterClient()
        resp = client.list_display_sources(
            q=request.args.get('q') or request.args.get('keyword') or '',
            status=request.args.get('status') or 'alive,available',
            item_type=request.args.get('item_type') or '',
            tmdb_id=request.args.get('tmdb_id') or '',
            order_by=request.args.get('order_by') or 'latest',
            limit=int(request.args.get('limit') or request.args.get('page_size') or 200),
            offset=int(request.args.get('offset') or 0),
            force_refresh=_boolish(
                request.args.get('force_refresh') or request.args.get('refresh') or request.args.get('no_cache'),
                False,
            ),
        )

        def _decorate_center_row(row):
            if not isinstance(row, dict):
                return {}
            row = dict(row)
            for key in ('versions', 'children', 'pack_items'):
                if isinstance(row.get(key), list):
                    row[key] = [_decorate_center_row(x) for x in row.get(key) if isinstance(x, dict)]
            short_direct = _center_direct_flag_state(row, 'is_short_drama', 'short_drama_meta_json')
            if short_direct is False:
                row['is_short_drama'] = False
                row['short_drama_meta_json'] = row.get('short_drama_meta_json') or {'is_short_drama': False, 'manual_override': True}
            else:
                short_meta = _center_flag_meta(row, 'is_short_drama', 'short_drama_meta_json')
                if short_meta:
                    row['is_short_drama'] = True
                    row['short_drama_meta_json'] = short_meta
            animation_meta = _center_flag_meta(row, 'is_animation', 'animation_meta_json')
            if animation_meta:
                row['is_animation'] = True
                row['animation_meta_json'] = animation_meta
            completed_certified = _center_source_is_completed_certified(row)
            completed_meta = _center_flag_meta(row, 'is_completed_certified', 'completed_certified_meta_json') if completed_certified else {}
            if completed_certified:
                row['is_completed_certified'] = True
                row['is_completed'] = True
                row['completed_certified_meta_json'] = completed_meta or {
                    'is_completed_certified': True,
                    'certified_by': 'completed_season_source',
                    'status': row.get('status'),
                }
            else:
                row['is_completed_certified'] = False
                row['is_completed'] = False
                row.pop('completed_certified_meta_json', None)
            # 连载季公共包没有统一版本参数；完结季包/电影/展开后的集才展示。
            if row.get('is_ongoing_hub') or row.get('source_kind') == 'season_hub':
                row['version_summary'] = {}
                row['summary_json'] = {}
                row['media_signature_json'] = {}
            else:
                row['version_summary'] = _center_version_summary(row)
            if not row.get('size') and row.get('total_size'):
                row['size'] = row.get('total_size')
            row = _apply_local_season_meta(row)
            return row

        resp['items'] = [
            _strip_center_display_children(_decorate_center_row(row))
            for row in (resp.get('items') or [])
            if isinstance(row, dict)
        ]
        return jsonify({'success': True, **resp})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e), 'items': [], 'total': 0}), 500


@shared_resource_bp.route('/center/sources/children', methods=['GET'])
@admin_required
def api_center_source_children():
    """中心资源库季包展开懒加载：默认列表不再携带 children/pack_items。"""
    try:
        raw_ids = request.args.get('source_ids') or ''
        source_ids = []
        for value in str(raw_ids).replace(';', ',').split(','):
            value = value.strip()
            if value and value not in source_ids:
                source_ids.append(value)
        source_id = str(request.args.get('source_id') or '').strip()
        if source_id and source_id not in source_ids:
            source_ids.insert(0, source_id)
        client = SharedCenterClient()
        resp = client.list_display_children(
            source_kind=request.args.get('source_kind') or '',
            source_id=source_id,
            source_ids=source_ids,
            hub_id=request.args.get('hub_id') or '',
            limit=int(request.args.get('limit') or 5000),
            offset=int(request.args.get('offset') or 0),
        )

        def _decorate_center_row(row):
            if not isinstance(row, dict):
                return {}
            row = dict(row)
            for key in ('versions', 'children', 'pack_items'):
                if isinstance(row.get(key), list):
                    row[key] = [_decorate_center_row(x) for x in row.get(key) if isinstance(x, dict)]
            short_direct = _center_direct_flag_state(row, 'is_short_drama', 'short_drama_meta_json')
            if short_direct is False:
                row['is_short_drama'] = False
                row['short_drama_meta_json'] = row.get('short_drama_meta_json') or {'is_short_drama': False, 'manual_override': True}
            else:
                short_meta = _center_flag_meta(row, 'is_short_drama', 'short_drama_meta_json')
                if short_meta:
                    row['is_short_drama'] = True
                    row['short_drama_meta_json'] = short_meta
            animation_meta = _center_flag_meta(row, 'is_animation', 'animation_meta_json')
            if animation_meta:
                row['is_animation'] = True
                row['animation_meta_json'] = animation_meta
            completed_certified = _center_source_is_completed_certified(row)
            completed_meta = _center_flag_meta(row, 'is_completed_certified', 'completed_certified_meta_json') if completed_certified else {}
            if completed_certified:
                row['is_completed_certified'] = True
                row['is_completed'] = True
                row['completed_certified_meta_json'] = completed_meta or {
                    'is_completed_certified': True,
                    'certified_by': 'completed_season_source',
                    'status': row.get('status'),
                }
            elif row.get('source_kind') != 'completed_season':
                row['is_completed_certified'] = False
                row['is_completed'] = False
                row.pop('completed_certified_meta_json', None)
            if row.get('is_ongoing_hub') or row.get('source_kind') == 'season_hub':
                row['version_summary'] = {}
                row['summary_json'] = {}
                row['media_signature_json'] = {}
            else:
                row['version_summary'] = _center_version_summary(row)
            if not row.get('size') and row.get('total_size'):
                row['size'] = row.get('total_size')
            row = _apply_local_season_meta(row)
            return row

        for key in ('items', 'children', 'pack_items', 'parents'):
            if isinstance(resp.get(key), list):
                resp[key] = [_decorate_center_row(row) for row in resp.get(key) if isinstance(row, dict)]
        return jsonify({'success': True, **resp})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e), 'items': [], 'children': [], 'pack_items': [], 'total': 0}), 500


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
            'message': '前端提交的还是旧 source_ids 秒传参数，缺少 Rapid v2 的 source_kind/source_id；请覆盖最新 SharedResourceManagerPage.vue。',
            'data': {'ok': False, 'success_count': 0, 'total': 0}
        }), 400

    preflight = _center_source_transfer_preflight(source)
    if not preflight.get('ok'):
        return jsonify({'success': False, 'message': preflight.get('message') or '该资源已被配置拦截', 'data': preflight}), 400

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
        return jsonify({'success': True, 'message': '贡献点已同步', 'data': data})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500



LEDGER_EVENT_LABEL_MAP = {
    'center_initial_credit': '基础贡献点',
    'center_source_registered': '中心登记共享源',
    'center_source_registered_group': '中心登记共享源',
    'center_backup_source_registered': '备份共享入池',
    'center_backup_source_registered_group': '备份共享入池',
    'center_deleted_shared_source_summary': '已删除共享源',
    'center_shared_source_served': '共享被秒传',
    'center_shared_source_served_group': '共享被秒传',
    'center_shared_source_consumed': '秒传共享资源',
    'center_shared_source_consumed_group': '秒传共享资源',
    'rapid_source_served': '共享视频被秒传',
    'rapid_source_consumed': '秒传共享视频',
    'center_rapid_source_registered': '中心登记秒传源',
    'center_rapid_source_registered_group': '中心登记秒传源',
    'center_rapid_source_served': '共享资源被秒传',
    'center_rapid_source_served_group': '共享资源被秒传',
    'center_rapid_source_consumed': '秒传共享资源',
    'center_rapid_source_consumed_group': '秒传共享资源',
    'center_rapid_sign_success': '秒传签名成功',
    'center_rapid_sign_failed': '秒传签名失败',
    'center_rapid_sign_timeout': '秒传签名超时',
    'center_rapid_sign_job_success': '秒传签名成功',
    'center_rapid_sign_job_failed': '秒传签名失败',
    'center_rapid_raw_uploaded': '上传媒体信息',
    'center_rapid_raw_ffprobe_uploaded': '上传媒体信息',
    'center_daily_grant': 'Pro每日赠送额度',
    'center_rapid_quota_consumed': 'Pro额度抵扣',
    'center_tier_cap_adjust': 'Pro等级上限调整',
    'center_pro_expired_clear': 'Pro过期清空额度',
    'center_pro_inactive_clear': 'Pro认证失效清空额度',
    'share_created': '登记共享源',
    'share_reported_center': '登记中心',
    'share_raw_uploaded': '上传媒体信息',
    'share_cancelled': '取消共享',
    'share_request_escrow': '求共享冻结',
    'share_request_refund': '求共享退款',
    'share_request_bounty_paid': '求共享悬赏支付',
    'share_request_bounty_received': '求共享悬赏收入',
    'share_request_service_fee': '求共享服务费',
    'center_share_request_escrow': '求共享冻结',
    'center_share_request_refund': '求共享退款',
    'center_share_request_bounty_paid': '求共享悬赏支付',
    'center_share_request_bounty_received': '求共享悬赏收入',
    'center_share_request_service_fee': '求共享服务费',
}

LEDGER_REASON_LABEL_MAP = {
    'rapid_sign_success': '响应中心秒传签名成功',
    'rapid_sign_failed': '响应中心秒传签名失败',
    'rapid_sign_timeout': '响应中心秒传签名超时',
    'rapid_source_consumed': '从共享中心秒传资源',
    'rapid_source_served': '本机共享资源被他人秒传',
    'source_registered': '共享资源登记入池',
    'center_initial_credit': '基础贡献点',
    'backup_source_registered': '备份共享入池',
    'shared_source_served': '共享资源被他人秒传',
    'shared_source_consumed': '从共享中心秒传资源',
    'daily_grant': 'Pro每日赠送额度',
    'rapid_quota_consumed': 'Pro额度抵扣',
    'tier_cap_adjust': 'Pro等级上限调整',
    'pro_expired_clear': 'Pro过期清空额度',
    'pro_inactive_clear': 'Pro认证失效清空额度',
    'center_daily_grant': 'Pro每日赠送额度',
    'center_rapid_quota_consumed': 'Pro额度抵扣',
    'center_tier_cap_adjust': 'Pro等级上限调整',
    'center_pro_expired_clear': 'Pro过期清空额度',
    'center_pro_inactive_clear': 'Pro认证失效清空额度',
}


def _ledger_event_label(event_type: Any) -> str:
    text = str(event_type or '').strip()
    if not text:
        return '-'
    if text in LEDGER_EVENT_LABEL_MAP:
        return LEDGER_EVENT_LABEL_MAP[text]
    low = text.lower()
    if 'rapid' in low and 'sign' in low:
        if 'fail' in low or 'error' in low:
            return '秒传签名失败'
        if 'timeout' in low:
            return '秒传签名超时'
        return '秒传签名'
    if 'rapid' in low and 'consume' in low:
        return '秒传共享资源'
    if 'rapid' in low and 'serv' in low:
        return '共享资源被秒传'
    if 'source' in low and 'register' in low:
        return '登记共享源'
    return text


def _ledger_json(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            obj = json.loads(value)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}


def _ledger_extract_sha1(row: Dict[str, Any]) -> str:
    raw = _ledger_json((row or {}).get('raw_json'))
    values = [
        (row or {}).get('sha1'), (row or {}).get('ref_id'), raw.get('sha1'), raw.get('file_sha1'),
        raw.get('sign_check'), raw.get('source_ref_id'), raw.get('source_id'),
    ]
    for key in ('media', 'source', 'shared_source', 'job'):
        obj = raw.get(key) if isinstance(raw.get(key), dict) else {}
        values.extend([obj.get('sha1'), obj.get('file_sha1'), obj.get('sign_check'), obj.get('ref_id')])
    for value in values:
        m = re.search(r'([A-Fa-f0-9]{40})', str(value or ''))
        if m:
            return m.group(1).upper()
    return ''


def _ledger_local_media_by_sha1(sha1: str) -> Dict[str, Any]:
    sha1 = str(sha1 or '').strip().upper()
    if not re.fullmatch(r'[A-F0-9]{40}', sha1):
        return {}
    cache = getattr(_ledger_local_media_by_sha1, '_cache', None)
    if not isinstance(cache, dict):
        cache = {}
        setattr(_ledger_local_media_by_sha1, '_cache', cache)
    if sha1 in cache:
        return cache[sha1]
    try:
        from database.connection import get_db_connection
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        m.tmdb_id, m.item_type, m.parent_series_tmdb_id, m.season_number, m.episode_number,
                        m.title, m.original_title, m.release_year,
                        p.title AS series_title, p.original_title AS series_original_title, p.release_year AS series_release_year
                    FROM media_metadata m
                    LEFT JOIN media_metadata p
                      ON p.item_type='Series' AND p.tmdb_id=m.parent_series_tmdb_id
                    WHERE COALESCE(m.file_sha1_json::text, '') ILIKE %s
                    ORDER BY
                        CASE m.item_type WHEN 'Episode' THEN 0 WHEN 'Movie' THEN 1 WHEN 'Season' THEN 2 ELSE 3 END,
                        COALESCE(m.in_library, FALSE) DESC,
                        COALESCE(m.last_updated_at, m.date_added, m.created_at) DESC NULLS LAST
                    LIMIT 1
                    """,
                    (f'%{sha1}%',),
                )
                row = cur.fetchone()
                cache[sha1] = dict(row) if row else {}
    except Exception:
        cache[sha1] = {}
    return cache[sha1]


def _ledger_sxx(season) -> str:
    try:
        return f"S{int(season):02d}"
    except Exception:
        return ''


def _ledger_exx(episode) -> str:
    try:
        return f"E{int(episode):02d}"
    except Exception:
        return ''


def _ledger_media_context(row: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(row or {})
    sha1 = _ledger_extract_sha1(row)
    raw = _ledger_json(row.get('raw_json'))
    center_ledger = raw.get('center_ledger') if isinstance(raw.get('center_ledger'), dict) else {}
    out: Dict[str, Any] = {}

    # 贡献点明细的媒体名应优先来自中心 /credit/ledger 的 join 结果。
    # 本地库只做最后兜底，避免本地重组/重命名导致“标题 S03 S03E08”这种重复拼接。
    for source in (center_ledger, row):
        for key in ('tmdb_id', 'item_type', 'season_number', 'episode_number', 'source_kind', 'title', 'file_name', 'release_year', 'file_count'):
            if source.get(key) not in (None, ''):
                out[key] = source.get(key)

    for key in ('title', 'file_name', 'name'):
        if out.get('title') in (None, '') and raw.get(key):
            out['title'] = raw.get(key)

    if out.get('title') in (None, '') and sha1:
        local = _ledger_local_media_by_sha1(sha1)
        for key in ('tmdb_id', 'item_type', 'season_number', 'episode_number', 'source_kind', 'title', 'file_name', 'release_year'):
            if out.get(key) in (None, '') and local.get(key) not in (None, ''):
                out[key] = local.get(key)

    if sha1:
        out['sha1'] = sha1
    return out


def _ledger_clean_base_title(base: str, season=None, episode=None) -> str:
    text = str(base or '').strip()
    if not text:
        return ''
    sxx = _ledger_sxx(season)
    exx = _ledger_exx(episode)
    # 去掉标题末尾已经带着的 Sxx / SxxEyy，后面统一追加一次，防止 S03S03。
    if sxx and exx:
        text = re.sub(rf'\s*{re.escape(sxx)}\s*{re.escape(exx)}\s*$', '', text, flags=re.I)
        text = re.sub(rf'\s*{re.escape(sxx)}{re.escape(exx)}\s*$', '', text, flags=re.I)
    if sxx:
        text = re.sub(rf'\s*{re.escape(sxx)}\s*$', '', text, flags=re.I)
    text = re.sub(r'\s+', ' ', text).strip(' -·._')
    return text or str(base or '').strip()


def _ledger_title_from_context(ctx: Dict[str, Any], *, aggregate: bool = False) -> str:
    ctx = ctx or {}
    item_type = str(ctx.get('item_type') or '').strip().lower()
    source_kind = str(ctx.get('source_kind') or '').strip().lower()
    season = ctx.get('season_number')
    episode = ctx.get('episode_number')
    base = str(
        ctx.get('series_title')
        or ctx.get('series_original_title')
        or ctx.get('title')
        or ctx.get('file_name')
        or ctx.get('name')
        or ''
    ).strip()
    sxx = _ledger_sxx(season)
    exx = _ledger_exx(episode)
    base = _ledger_clean_base_title(base, season, episode)

    if item_type == 'episode' or source_kind == 'episode' or (sxx and exx):
        if not base:
            return ''
        return f"{base} {sxx}" if aggregate and sxx else f"{base} {sxx}{exx}".strip()
    if item_type == 'season' or source_kind == 'completed_season' or (sxx and not exx):
        if not base:
            return ''
        return f"{base} {sxx}" if sxx else base
    return base


def _ledger_aggregate_key_for_row(row: Dict[str, Any], ctx: Dict[str, Any]) -> str:
    event = str((row or {}).get('event_type') or (row or {}).get('reason') or '').strip()
    item_type = str((ctx or {}).get('item_type') or '').strip().lower()
    source_kind = str((ctx or {}).get('source_kind') or '').strip().lower()
    tmdb_id = str((ctx or {}).get('tmdb_id') or '').strip()
    season = (ctx or {}).get('season_number')
    episode = (ctx or {}).get('episode_number')
    sha1 = str((ctx or {}).get('sha1') or _ledger_extract_sha1(row) or '').strip().upper()
    sxx = _ledger_sxx(season)

    # 签名成功按“具体文件/具体集”聚合：家业 S01E01 +1*3。
    if _ledger_is_sign_row(row):
        if sha1:
            return f"{event}:sign:sha1:{sha1}"
        if tmdb_id and sxx and episode not in (None, ''):
            return f"{event}:sign:{tmdb_id}:{sxx}:E{episode}"

    # 秒传扣分按“季”聚合：家业 S01 -1*42。
    if _ledger_is_consumed_row(row) and tmdb_id and (sxx or source_kind == 'completed_season' or item_type in {'episode', 'season'}):
        return f"{event}:consume-season:{tmdb_id}:{sxx or season or ''}"

    if tmdb_id and (item_type in {'episode', 'season'} or source_kind in {'episode', 'completed_season'}):
        return f"{event}:season:{tmdb_id}:{sxx or season or ''}"
    if tmdb_id:
        return f"{event}:movie:{tmdb_id}"
    if sha1:
        return f"{event}:sha1:{sha1}"
    return f"{event}:{str((row or {}).get('ref_id') or (row or {}).get('id') or '').strip()}"

def _ledger_title(row: Dict[str, Any], *, aggregate: bool = False) -> str:
    ctx = _ledger_media_context(row)
    media_title = _ledger_title_from_context(ctx, aggregate=aggregate)
    if media_title:
        return media_title

    raw = _ledger_json((row or {}).get('raw_json'))
    nested = [raw.get(k) for k in ('media', 'request', 'source', 'shared_source', 'job') if isinstance(raw.get(k), dict)]
    values = [raw.get('title'), raw.get('name'), raw.get('file_name')]
    for obj in nested:
        values.extend([obj.get('title'), obj.get('name'), obj.get('file_name')])
    values.extend([row.get('title'), row.get('file_name')])
    for value in values:
        text = str(value or '').strip()
        if not text or re.match(r'^srq_[0-9a-f]', text, re.I):
            continue
        if text.lower().startswith('rapid_sign:'):
            continue
        if re.fullmatch(r'(?:rapid_sign:)?[A-Fa-f0-9]{40}(?::.*)?', text):
            continue
        return text
    event = str((row or {}).get('event_type') or '').lower()
    if 'share_request' in event:
        return '求共享'
    sha = _ledger_extract_sha1(row)
    if sha:
        return f"未知资源 {sha[:12]}..."
    return '-'




def _ledger_event_code(row: Dict[str, Any]) -> str:
    row = row if isinstance(row, dict) else {}
    return str(row.get('event_type') or row.get('reason') or '').strip().lower()


def _ledger_reason_code(row: Dict[str, Any]) -> str:
    row = row if isinstance(row, dict) else {}
    return str(row.get('reason') or row.get('event_type') or '').strip().lower().replace('center_', '')


def _ledger_is_sign_row(row: Dict[str, Any]) -> bool:
    code = _ledger_event_code(row)
    reason = _ledger_reason_code(row)
    return 'rapid_sign' in code or reason.startswith('rapid_sign')


def _ledger_is_consumed_row(row: Dict[str, Any]) -> bool:
    code = _ledger_event_code(row)
    reason = _ledger_reason_code(row)
    return 'rapid_source_consumed' in code or reason == 'rapid_source_consumed' or 'shared_source_consumed' in code or reason == 'shared_source_consumed'


def _ledger_is_served_row(row: Dict[str, Any]) -> bool:
    code = _ledger_event_code(row)
    reason = _ledger_reason_code(row)
    return 'rapid_source_served' in code or reason == 'rapid_source_served' or 'shared_source_served' in code or reason == 'shared_source_served'


def _ledger_is_pro_quota_row(row: Dict[str, Any]) -> bool:
    row = row if isinstance(row, dict) else {}
    code = _ledger_event_code(row)
    reason = _ledger_reason_code(row)
    ledger_type = str(row.get('ledger_type') or '').strip().lower()
    return ledger_type == 'pro_quota' or reason in {
        'daily_grant', 'rapid_quota_consumed', 'tier_cap_adjust',
        'pro_expired_clear', 'pro_inactive_clear',
    } or code in {
        'center_daily_grant', 'center_rapid_quota_consumed', 'center_tier_cap_adjust',
        'center_pro_expired_clear', 'center_pro_inactive_clear',
    }


def _ledger_credit_text(row: Dict[str, Any]) -> str:
    try:
        n = int(float((row or {}).get('delta') or 0))
    except Exception:
        n = 0
    sign = '+' if n > 0 else ''
    if _ledger_is_pro_quota_row(row):
        if abs(n) > 1 and _ledger_reason_code(row) == 'rapid_quota_consumed':
            unit = '+1' if n > 0 else '-1'
            return f"Pro额度 {unit}*{abs(n)}"
        return f"Pro额度 {sign}{n}"
    # Rapid v2 这里的绝对值就是“视频数/签名次数”：-42 应显示为 -1*42。
    if (_ledger_is_consumed_row(row) or _ledger_is_served_row(row) or _ledger_is_sign_row(row)) and abs(n) > 1:
        unit = '+1' if n > 0 else '-1'
        return f"贡献点 {unit}*{abs(n)}"
    return f"贡献点 {sign}{n}"


def _normalize_center_credit_ledger_item(row: Dict[str, Any]) -> Dict[str, Any]:
    """把中心 /credit/ledger 返回行转换成本地前端兼容格式。

    本地旧同步表可能只留下 rapid_sign:SHA1，中心实时返回的 ledger 已经携带 title / season_number / episode_number，
    sync_center=1 时优先使用这份富信息，避免贡献点明细展示 SHA1。
    """
    row = dict(row or {})
    reason = str(row.get('reason') or '').strip()
    if reason and not row.get('event_type'):
        row['event_type'] = reason if reason.startswith('center_') else f'center_{reason}'
    raw = row.get('raw_json') if isinstance(row.get('raw_json'), dict) else {}
    raw.setdefault('center_ledger', {k: v for k, v in row.items() if k != 'raw_json'})
    row['raw_json'] = raw
    if row.get('ref_id') and not row.get('source_id') and str(row.get('source_kind') or '') in {'movie', 'episode', 'completed_season'}:
        row['source_id'] = row.get('ref_id')
    return row

def _ledger_delta_text(delta: Any) -> str:
    try:
        n = int(float(delta or 0))
    except Exception:
        n = 0
    return f'+{n}' if n > 0 else str(n)


def _decorate_credit_ledger_item(row: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(row or {})
    event = str(row.get('event_type') or '').strip()
    reason = str(row.get('reason') or '').strip()
    ctx = _ledger_media_context(row)
    title = _ledger_title(row)
    aggregate_title = (_ledger_title(row, aggregate=True) if _ledger_is_consumed_row(row) else title) or title
    delta_text = _ledger_credit_text(row)
    event_label = _ledger_event_label(event)
    reason_label = LEDGER_REASON_LABEL_MAP.get(reason) or LEDGER_REASON_LABEL_MAP.get(event.replace('center_', ''))
    if not reason_label and event.startswith('center_share_request_'):
        reason_label = event_label
    if not reason_label and event.startswith('share_request_'):
        reason_label = event_label
    row['event_label'] = event_label
    row['title_display'] = title
    row['ledger_aggregate_title'] = aggregate_title
    row['ledger_aggregate_key'] = _ledger_aggregate_key_for_row(row, ctx)
    row['ledger_sha1'] = ctx.get('sha1') or _ledger_extract_sha1(row)
    # 让前端即使不重新解析 raw_json，也能按季聚合签名贡献点。
    for key in ('tmdb_id', 'item_type', 'season_number', 'episode_number', 'source_kind'):
        if row.get(key) in (None, '') and ctx.get(key) not in (None, ''):
            row[key] = ctx.get(key)
    if _ledger_is_pro_quota_row(row):
        tier_label = {'M': '月卡', 'Y': '年卡', 'L': '终身'}.get(str(row.get('pro_tier') or '').upper(), str(row.get('pro_tier') or '').upper())
        title = title if title and title != '-' else (f'Pro{tier_label}' if tier_label else 'Pro额度')
        balance = row.get('balance_after')
        balance_text = f'，余额 {balance}' if balance not in (None, '') else ''
        row['reason_display'] = f'{reason_label or event_label}：{title}，{delta_text}{balance_text}'
    else:
        row['reason_display'] = f'{reason_label or event_label}：{title}，{delta_text}' if (reason_label or not reason) else reason
    row['delta_display'] = _ledger_delta_text(row.get('delta'))
    return row


@shared_resource_bp.route('/credit/ledger', methods=['GET'])
@admin_required
def api_credit_ledger():
    center_items = []
    if _boolish(request.args.get('sync_center'), False):
        try:
            sync_result = _fetch_center_credit()
            center_items = sync_result.get('center_ledger_items') or []
        except Exception as e:
            logger.warning(f"  ➜ [共享资源] 同步中心贡献点失败: {e}")
    limit = int(request.args.get('limit') or 200)
    if center_items:
        items = [_normalize_center_credit_ledger_item(x) for x in center_items[:limit] if isinstance(x, dict)]
    else:
        items = shared_credit_db.list_credit_ledger(limit=limit, actual_only=_boolish(request.args.get('actual_only'), False))
    items = [_decorate_credit_ledger_item(x) for x in (items or []) if isinstance(x, dict)]
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


def _tmdb_api_key() -> str:
    for key in ('tmdb_api_key', 'tmdb_key', 'TMDB_API_KEY', 'themoviedb_api_key'):
        value = (config_manager.APP_CONFIG or {}).get(key)
        if value:
            return str(value).strip()
    return ''


def _share_request_target_type(value: str, media_type: str = '') -> str:
    text = str(value or '').strip().lower()
    media = str(media_type or '').strip().lower()
    if media == 'movie' or text in ('movie', 'film'):
        return 'movie'
    if text in ('series', 'tv', 'show'):
        return 'series'
    # Rapid v2 求共享不再支持单集，单集/多集统一提升到单季。
    if text in ('season', 'episode', 'episode_batch', 'single'):
        return 'season'
    return 'movie' if media == 'movie' else 'season'


def _share_request_count_from_tmdb(payload: Dict[str, Any]) -> int:
    """按 TMDb 官方集数估算求共享视频个数；单季上限 10 点。"""
    target = _share_request_target_type(payload.get('target_type'), payload.get('media_type'))
    if target == 'movie':
        return 1
    try:
        tv_id = int(str(payload.get('tmdb_id') or '0'))
    except Exception:
        tv_id = 0
    api_key = _tmdb_api_key()
    if not tv_id or not api_key:
        return 1
    if target == 'season':
        season = _safe_int(payload.get('season_number'), 0)
        if season <= 0:
            return 1
        try:
            detail = tmdb_handler.get_season_details_tmdb(tv_id=tv_id, season_number=season, api_key=api_key, append_to_response=None) or {}
            count = len([x for x in (detail.get('episodes') or []) if isinstance(x, dict) and _safe_int(x.get('episode_number'), 0) > 0])
            return max(1, min(count or _safe_int(payload.get('expected_episode_count'), 0) or 1, 10))
        except Exception as e:
            logger.warning(f"  ➜ [求共享] 查询 TMDb 季集数失败: tv={tv_id}, S{season}, err={e}")
            return max(1, min(_safe_int(payload.get('expected_episode_count'), 1), 10))
    try:
        detail = tmdb_handler.get_tv_details(tv_id, api_key, append_to_response=None, allow_english_fallback=False) or {}
        total = 0
        for season in detail.get('seasons') or []:
            if not isinstance(season, dict):
                continue
            if _safe_int(season.get('season_number'), 0) <= 0:
                continue
            total += min(_safe_int(season.get('episode_count'), 0), 10)
        return max(1, total or _safe_int(detail.get('number_of_episodes'), 0) or 1)
    except Exception as e:
        logger.warning(f"  ➜ [求共享] 查询 TMDb 全剧集数失败: tv={tv_id}, err={e}")
        return 1


def _enrich_share_request_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(data or {})
    media_type = str(payload.get('media_type') or '').strip().lower()
    target_type = _share_request_target_type(payload.get('target_type'), media_type)
    payload['target_type'] = target_type
    payload['media_type'] = 'movie' if target_type == 'movie' else 'tv'
    payload['item_type'] = 'Movie' if target_type == 'movie' else 'Season'
    if target_type == 'movie':
        payload['season_number'] = None
    elif target_type == 'series':
        payload['season_number'] = None
    else:
        payload['season_number'] = _safe_int(payload.get('season_number'), 1) or 1
    payload.pop('episode_number', None)
    payload.pop('episode_numbers', None)
    payload['video_count'] = _share_request_count_from_tmdb(payload)
    if target_type == 'season':
        payload['expected_episode_count'] = payload['video_count']
    return payload


def _tmdb_search_item(row: Dict[str, Any], media_type: str) -> Dict[str, Any]:
    row = dict(row or {})
    if media_type == 'movie':
        release = row.get('release_date') or ''
        title = row.get('title') or row.get('name') or row.get('original_title') or ''
    else:
        release = row.get('first_air_date') or ''
        title = row.get('name') or row.get('title') or row.get('original_name') or ''
    year = None
    if release and len(str(release)) >= 4:
        year = _safe_int(str(release)[:4], 0) or None
    return {
        'tmdb_id': str(row.get('id') or ''),
        'media_type': 'movie' if media_type == 'movie' else 'tv',
        'title': title,
        'release_year': year,
        'poster_path': row.get('poster_path') or '',
        'overview': row.get('overview') or '',
    }


@shared_resource_bp.route('/share-requests/param-options', methods=['GET'])
@admin_required
def api_share_request_param_options():
    return jsonify({'success': True, 'data': {
        'resolution': [{'label': x, 'value': x} for x in ['4K', '1080p', '720p', '480p']],
        'codec': [{'label': x, 'value': x} for x in ['HEVC', 'H.264', 'AV1', 'VP9']],
        'effect': [{'label': x, 'value': x} for x in ['DoVi P8', 'DoVi P7', 'DoVi P5', 'DoVi', 'HDR10+', 'HDR', 'SDR']],
        'frame_rate': [{'label': x, 'value': v} for x, v in [('≥ 60 fps', '60'), ('≥ 50 fps', '50'), ('≥ 30 fps', '30'), ('24 fps', '24')]],
        'audio': [{'label': x, 'value': x} for x in ['国语', '粤语', '英语', '日语', '韩语']],
        'subtitle': [{'label': x, 'value': x} for x in ['简体', '繁体', '英文', '日文', '韩文', '无']],
    }})


@shared_resource_bp.route('/share-requests/quote', methods=['POST'])
@admin_required
def api_share_request_quote():
    try:
        payload = _enrich_share_request_payload(_request_json())
        resp = SharedCenterClient().quote_share_request(payload)
        return jsonify({'success': True, 'ok': True, 'data': resp, **({'message': resp.get('message')} if resp.get('message') else {})})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@shared_resource_bp.route('/share-requests/tmdb/search', methods=['GET'])
@admin_required
def api_share_request_tmdb_search():
    keyword = (request.args.get('keyword') or request.args.get('q') or '').strip()
    media_type = (request.args.get('media_type') or 'movie').strip().lower()
    if not keyword:
        return jsonify({'success': True, 'items': []})
    api_key = _tmdb_api_key()
    if not api_key:
        return jsonify({'success': False, 'message': '未配置 TMDb API Key'}), 400
    try:
        if media_type in ('tv', 'series'):
            rows = tmdb_handler.search_media(query=keyword, api_key=api_key, item_type='tv') or []
            items = [_tmdb_search_item(x, 'tv') for x in rows[:20]]
        else:
            rows = tmdb_handler.search_media(query=keyword, api_key=api_key, item_type='movie') or []
            items = [_tmdb_search_item(x, 'movie') for x in rows[:20]]
        return jsonify({'success': True, 'items': items})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@shared_resource_bp.route('/share-requests', methods=['GET', 'POST'])
@admin_required
def api_share_requests():
    client = SharedCenterClient()
    if request.method == 'GET':
        try:
            resp = client.list_share_requests(
                keyword=request.args.get('keyword') or request.args.get('q') or '',
                status=request.args.get('status') or 'open',
                media_type=request.args.get('media_type') or '',
                target_type=request.args.get('target_type') or '',
                limit=int(request.args.get('limit') or 100),
                offset=int(request.args.get('offset') or 0),
            )
            return jsonify({'success': True, **resp})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e), 'items': [], 'total': 0}), 500
    try:
        payload = _enrich_share_request_payload(_request_json())
        resp = client.create_share_request(payload)
        return jsonify({'success': True, 'message': resp.get('message') or '求共享已发布', 'data': resp})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 400


@shared_resource_bp.route('/share-requests/<group_id>/co-request', methods=['POST'])
@admin_required
def api_share_request_co(group_id):
    try:
        resp = SharedCenterClient().co_request_share_request(group_id, _request_json())
        return jsonify({'success': True, 'message': resp.get('message') or '同求成功', 'data': resp})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 400


@shared_resource_bp.route('/share-requests/<group_id>/cancel', methods=['POST'])
@admin_required
def api_share_request_cancel(group_id):
    try:
        resp = SharedCenterClient().cancel_share_request(group_id, _request_json())
        return jsonify({'success': True, 'message': resp.get('message') or '已取消求共享', 'data': resp})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 400
