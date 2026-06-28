# routes/shared_resource.py
# Rapid v2 共享资源 API：不再创建/管理 115 分享；只登记秒传资源索引与消费中心事件。
import json
import logging
import copy
import os
import re
import socket
import threading
import time
import uuid
from typing import Any, Dict, List

import requests
from flask import Blueprint, jsonify, request

import constants
import config_manager
from extensions import admin_required
from database import shared_credit_db, shared_share_db, shared_virtual_db, settings_db
from database.connection import get_db_connection
from handler.shared_center_client import SharedCenterClient, _current_server_id_hash
from handler.shared_subscription_service import (
    consume_device_event,
    create_virtual_strm_files,
    prepare_center_source_files_for_virtual,
)
from handler import emby
from handler import tmdb as tmdb_handler
import tasks.shared_resource_tasks as shared_tasks

shared_resource_bp = Blueprint('shared_resource_bp', __name__, url_prefix='/api/shared/resources')
logger = logging.getLogger(__name__)
_CENTER_HOME_PROXY_CACHE: Dict[Any, Dict[str, Any]] = {}
_CENTER_HOME_PROXY_CACHE_LOCK = threading.RLock()
_CENTER_HOME_PROXY_CACHE_TTL_SECONDS = 300
_CENTER_DETAIL_PROXY_CACHE: Dict[Any, Dict[str, Any]] = {}


def _center_proxy_cache_get(cache_store: Dict[Any, Dict[str, Any]], cache_key) -> Dict[str, Any] | None:
    now = time.time()
    with _CENTER_HOME_PROXY_CACHE_LOCK:
        entry = cache_store.get(cache_key)
        if not entry:
            return None
        if float(entry.get('expires_at') or 0) < now:
            cache_store.pop(cache_key, None)
            return None
        payload = copy.deepcopy(entry.get('payload') or {})
        payload['local_cache_hit'] = True
        payload['local_cache_ttl_seconds'] = max(0, int(float(entry.get('expires_at') or now) - now))
        return payload


def _center_proxy_cache_set(cache_store: Dict[Any, Dict[str, Any]], cache_key, payload: Dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        return
    now = time.time()
    with _CENTER_HOME_PROXY_CACHE_LOCK:
        if len(cache_store) >= 256:
            oldest_key = min(cache_store, key=lambda k: cache_store[k].get('expires_at', 0))
            cache_store.pop(oldest_key, None)
        cache_store[cache_key] = {
            'expires_at': now + _CENTER_HOME_PROXY_CACHE_TTL_SECONDS,
            'payload': copy.deepcopy(payload),
        }


def _center_home_proxy_cache_get(cache_key) -> Dict[str, Any] | None:
    return _center_proxy_cache_get(_CENTER_HOME_PROXY_CACHE, cache_key)


def _center_home_proxy_cache_set(cache_key, payload: Dict[str, Any]) -> None:
    _center_proxy_cache_set(_CENTER_HOME_PROXY_CACHE, cache_key, payload)


def _center_home_proxy_cache_clear() -> None:
    with _CENTER_HOME_PROXY_CACHE_LOCK:
        _CENTER_HOME_PROXY_CACHE.clear()


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
    payload['p115_shared_resource_mode'] = 'rapid'
    payload.setdefault('p115_shared_disable_episode_transfer', False)
    payload.setdefault('p115_shared_block_clean_version_transfer', False)
    payload.setdefault('p115_shared_block_short_drama_transfer', False)
    payload.setdefault('p115_shared_intro_enabled', False)
    payload.setdefault('p115_shared_auto_share_requests_enabled', False)
    payload.setdefault('p115_shared_virtual_import_enabled', False)
    payload.setdefault('p115_shared_virtual_auto_promote_episodes', 0)
    payload.setdefault('p115_shared_virtual_auto_promote_movie_percent', 0)
    payload.setdefault('p115_shared_center_home_sections', [])
    return payload


def _save_shared_config(data: Dict[str, Any]) -> Dict[str, Any]:
    data = dict(data or {})
    data['p115_shared_resource_enabled'] = _boolish(data.get('p115_shared_resource_enabled'), False)
    data['p115_shared_center_url'] = str(data.get('p115_shared_center_url') or 'https://shared.55565576.xyz').rstrip('/')
    data['p115_shared_resource_mode'] = 'rapid'
    data.pop('p115_shared_max_active_shares', None)
    data['p115_shared_disable_episode_transfer'] = _boolish(data.get('p115_shared_disable_episode_transfer'), False)
    data['p115_shared_block_clean_version_transfer'] = _boolish(data.get('p115_shared_block_clean_version_transfer'), False)
    data['p115_shared_block_short_drama_transfer'] = _boolish(data.get('p115_shared_block_short_drama_transfer'), False)
    data['p115_shared_intro_enabled'] = _boolish(data.get('p115_shared_intro_enabled'), False)
    data['p115_shared_auto_share_requests_enabled'] = _boolish(data.get('p115_shared_auto_share_requests_enabled'), False)
    data['p115_shared_virtual_import_enabled'] = _boolish(data.get('p115_shared_virtual_import_enabled'), False)
    data['p115_shared_virtual_auto_promote_episodes'] = max(0, _safe_int(data.get('p115_shared_virtual_auto_promote_episodes'), 0))
    data['p115_shared_virtual_auto_promote_movie_percent'] = max(0, min(_safe_int(data.get('p115_shared_virtual_auto_promote_movie_percent'), 0), 100))
    sections = data.get('p115_shared_center_home_sections')
    data['p115_shared_center_home_sections'] = sections if isinstance(sections, list) else []
    return settings_db.save_shared_resource_config(data)


def _fetch_center_credit() -> Dict[str, Any]:
    client = SharedCenterClient()
    pro_report = {}
    try:
        pro_report = client.report_current_pro_quota_auth()
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] Pro 额度认证上报失败，继续同步贡献点: {e}")
    me = client.me()
    stats = client.stats()
    display_series = {}
    try:
        display_series = {'total': stats.get('display_series_count') or (stats.get('media_stats') or {}).get('series_count')}
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 拉取中心剧集展示统计失败: {e}")
    ledger = {}
    try:
        ledger = client.credit_ledger(limit=500)
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 拉取中心贡献点流水失败: {e}")
    movie_source_count = int(stats.get('movie_sources') or 0)
    episode_source_count = int(stats.get('episode_sources') or 0)
    logical_group_count = int(stats.get('logical_season_groups') or 0)
    video_count = movie_source_count + episode_source_count
    raw_media_stats = (stats.get('media_stats') or {}) if isinstance(stats.get('media_stats'), dict) else {}
    display_movie_count = int(stats.get('display_movie_count') or raw_media_stats.get('movie_count') or movie_source_count)
    display_series_count = int(
        display_series.get('total')
        or stats.get('display_series_count')
        or raw_media_stats.get('series_count')
        or stats.get('display_season_count')
        or raw_media_stats.get('season_count')
        or logical_group_count
        or 0
    )
    display_season_count = int(stats.get('display_season_count') or raw_media_stats.get('season_count') or display_series_count or logical_group_count)
    media_stats = {
        **raw_media_stats,
        'movie_count': display_movie_count,
        'series_count': display_series_count,
        'season_count': display_season_count,
        'video_count': video_count,
    }
    enriched_stats = {
        **stats,
        'display_movie_count': display_movie_count,
        'display_series_count': display_series_count,
        'display_season_count': display_season_count,
        'video_count': video_count,
        'media_stats': media_stats,
    }
    snapshot = {
        'device_id': me.get('id'),
        'credit': int(me.get('credit') or 0),
        # Rapid v2 入库即共享后，普通缺口数量不再作为首页统计；首页展示主动“求共享”数量。
        'wanted_gaps': int(
            stats.get('share_requests')
            if stats.get('share_requests') is not None
            else (stats.get('active_share_requests') if stats.get('active_share_requests') is not None else stats.get('active_gap_devices') or 0)
        ),
        'share_requests': int(
            stats.get('share_requests')
            if stats.get('share_requests') is not None
            else (stats.get('active_share_requests') if stats.get('active_share_requests') is not None else stats.get('active_gap_devices') or 0)
        ),
        'shared_sources': video_count,
        'raw_ffprobe': int(stats.get('raw_ffprobe') or 0),
        'display_movie_count': display_movie_count,
        'display_series_count': display_series_count,
        'display_season_count': display_season_count,
        'video_count': video_count,
        'media_stats': media_stats,
        'pro_quota': (pro_report.get('pro_quota') or pro_report.get('quota') or stats.get('pro_quota') or me.get('pro_quota') or {}),
        'remote_devices': int(stats.get('online_devices') if stats.get('online_devices') is not None else stats.get('devices') or 0),
        'raw_json': {
            'me': me,
            'stats': {
                **enriched_stats,
                'online_devices': int(stats.get('online_devices') if stats.get('online_devices') is not None else stats.get('devices') or 0),
                'devices': int(stats.get('devices') or 0),
            },
        },
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
    row['root_is_dir'] = row.get('source_kind') == 'logical_season' or row.get('is_aggregated_season')
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
                    'center_source_ids': [],
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
            center_source_id = str(row.get('center_source_id') or '').strip()
            if center_source_id and center_source_id not in g['center_source_ids']:
                g['center_source_ids'].append(center_source_id)
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
    if season_number in (None, ''):
        return {}
    season_no = _safe_int(season_number, -1)
    # season_number=0 是 TMDb 特别篇，不能按缺失季号跳过。
    if not tmdb_id or season_no < 0:
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


def _batch_lookup_local_season_meta(rows: List[Dict[str, Any]]) -> Dict[tuple, Dict[str, Any]]:
    """批量从本机 media_metadata 补齐季总集数，避免中心资源库首屏 N+1。"""
    pairs = []

    def visit(value):
        if not isinstance(value, dict):
            return
        item_type = str(value.get('item_type') or value.get('display_type') or '').strip().lower()
        source_kind = str(value.get('source_kind') or '').strip().lower()
        if item_type in ('season', 'pack') or source_kind in ('season_hub', 'logical_season'):
            tmdb_id = str(value.get('tmdb_id') or '').strip()
            raw_season = value.get('season_number')
            season_no = _safe_int(raw_season, -1)
            if tmdb_id and raw_season not in (None, '') and season_no >= 0 and (tmdb_id, season_no) not in pairs:
                pairs.append((tmdb_id, season_no))
        for key in ('versions', 'children', 'pack_items'):
            children = value.get(key)
            if isinstance(children, list):
                for child in children:
                    visit(child)

    for row in rows or []:
        visit(row)
    if not pairs:
        return {}

    tmdb_ids = sorted({x[0] for x in pairs})
    season_numbers = sorted({x[1] for x in pairs})
    wanted = set(pairs)
    out: Dict[tuple, Dict[str, Any]] = {}
    try:
        from database.connection import get_db_connection
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT tmdb_id, parent_series_tmdb_id, season_number, total_episodes, watching_status, last_updated_at
                    FROM media_metadata
                    WHERE item_type='Season'
                      AND season_number = ANY(%s)
                      AND (tmdb_id = ANY(%s) OR parent_series_tmdb_id = ANY(%s))
                    ORDER BY last_updated_at DESC NULLS LAST
                    """,
                    (season_numbers, tmdb_ids, tmdb_ids),
                )
                for raw in cur.fetchall() or []:
                    meta = dict(raw)
                    season_no = _safe_int(meta.get('season_number'), 0)
                    keys = [
                        (str(meta.get('parent_series_tmdb_id') or '').strip(), season_no),
                        (str(meta.get('tmdb_id') or '').strip(), season_no),
                    ]
                    for key in keys:
                        if key in wanted and key not in out:
                            out[key] = meta
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 批量查询本机季元数据失败: {e}")
    return out


def _apply_local_season_meta(row: Dict[str, Any], meta_map: Dict[tuple, Dict[str, Any]] | None = None) -> Dict[str, Any]:
    row = dict(row or {})
    item_type = str(row.get('item_type') or row.get('display_type') or '').strip().lower()
    source_kind = str(row.get('source_kind') or '').strip().lower()
    if item_type not in ('season', 'pack') and source_kind not in ('season_hub', 'logical_season'):
        return row
    tmdb_id = str(row.get('tmdb_id') or '').strip()
    raw_season = row.get('season_number')
    if raw_season in (None, ''):
        return row
    season_no = _safe_int(raw_season, -1)
    if season_no < 0:
        return row
    if meta_map is not None:
        meta = meta_map.get((tmdb_id, season_no)) or {}
    else:
        meta = _lookup_local_season_meta(row.get('tmdb_id'), raw_season)
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



def _share_channel_raw_json(row: Dict[str, Any]) -> Dict[str, Any]:
    raw = (row or {}).get('raw_json') if isinstance(row, dict) else {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _share_channel_is_logical(row: Dict[str, Any] | None = None, source_id: str = '') -> bool:
    """判断本地 channel/source 行是否属于逻辑完结季文件列表分享。

    本地表名还沿用历史 completed_season 命名，但业务主线只允许 logical_season：
    - center_source_id/group_id 以 svg_ 开头；
    - raw_json.share_kind == logical_season；
    - raw_json.event/command 是 create_logical_season_filelist_share。
    """
    row = row if isinstance(row, dict) else {}
    raw = _share_channel_raw_json(row)
    sid = str(source_id or row.get('center_source_id') or row.get('source_id') or row.get('group_id') or '').strip()
    event_text = ' '.join(
        str(raw.get(k) or '')
        for k in ('event', 'event_type', 'command', 'share_kind', 'source_kind')
    )
    nested_event = raw.get('event') if isinstance(raw.get('event'), dict) else {}
    if nested_event:
        event_text += ' ' + ' '.join(
            str(nested_event.get(k) or '')
            for k in ('event_type', 'command', 'share_kind', 'source_kind')
        )
    return (
        sid.startswith('svg_')
        or str(row.get('source_kind') or '').strip().lower() == 'logical_season'
        or str(raw.get('share_kind') or '').strip() == 'logical_season'
        or 'create_logical_season_filelist_share' in event_text
        or 'logical_season' in event_text
    )


def _share_channel_tmdb_season_key(row: Dict[str, Any]) -> tuple[str, int] | None:
    """从本地逻辑季分享通道缓存里取 tmdb_id + season_number 兜底关联键。"""
    row = row if isinstance(row, dict) else {}
    raw = _share_channel_raw_json(row)
    event = raw.get('event') if isinstance(raw.get('event'), dict) else {}
    payload = raw.get('payload') if isinstance(raw.get('payload'), dict) else {}
    tmdb_id = str(
        row.get('tmdb_id')
        or event.get('tmdb_id')
        or payload.get('tmdb_id')
        or raw.get('tmdb_id')
        or ''
    ).strip()
    season = (
        row.get('season_number')
        if row.get('season_number') not in (None, '')
        else event.get('season_number')
        if event.get('season_number') not in (None, '')
        else payload.get('season_number')
        if payload.get('season_number') not in (None, '')
        else raw.get('season_number')
    )
    season_no = _safe_int(season, -1)
    if not tmdb_id or season_no < 0:
        return None
    return (tmdb_id, season_no)


def _share_channel_hub_id(row: Dict[str, Any]) -> str:
    row = row if isinstance(row, dict) else {}
    raw = _share_channel_raw_json(row)
    event = raw.get('event') if isinstance(raw.get('event'), dict) else {}
    payload = raw.get('payload') if isinstance(raw.get('payload'), dict) else {}
    center_item = raw.get('center_response') if isinstance(raw.get('center_response'), dict) else {}
    center_item = center_item.get('item') if isinstance(center_item.get('item'), dict) else {}
    return str(
        row.get('hub_id')
        or center_item.get('hub_id')
        or event.get('hub_id')
        or payload.get('hub_id')
        or raw.get('hub_id')
        or ''
    ).strip()


def _local_completed_share_public(channel: Dict[str, Any]) -> Dict[str, Any]:
    channel = dict(channel or {})
    if not channel:
        return {}
    out = {}
    for key in (
        'channel_id', 'center_source_id', 'hub_id', 'manifest_hash', 'share_code', 'receive_code',
        'share_url', 'share_title', 'root_fid', 'root_cid', 'root_name', 'file_count', 'total_size',
        'status', 'review_status', 'status_message', 'fail_count', 'last_checked_at', 'last_reported_at',
        'created_at', 'updated_at'
    ):
        value = channel.get(key)
        if value not in (None, '', [], {}):
            out[key] = value
    return out


def _attach_completed_share_channels_to_local_rows(rows: List[Dict[str, Any]]) -> None:
    """我的共享源只对账 ETK 本地托管的完结季分享通道，不扫描 115 账号全量分享。"""
    rows = [r for r in (rows or []) if isinstance(r, dict)]
    local_ids = []
    center_ids = []
    hub_ids = []
    tmdb_ids = []
    row_pairs = {}
    for row in rows:
        kind = str(row.get('source_kind') or '').strip().lower()
        is_logical = _share_channel_is_logical(row)
        source_ids = row.get('source_ids') if isinstance(row.get('source_ids'), list) else []
        if not is_logical and not source_ids and kind not in {'episode_group', 'completed_season'}:
            continue
        try:
            rid = int(row.get('id') or 0)
            if rid > 0 and rid not in local_ids:
                local_ids.append(rid)
        except Exception:
            pass
        for sid in source_ids:
            try:
                sid_int = int(sid or 0)
            except Exception:
                sid_int = 0
            if sid_int > 0 and sid_int not in local_ids:
                local_ids.append(sid_int)
        center_candidates = [row.get('center_source_id')]
        if isinstance(row.get('center_source_ids'), list):
            center_candidates.extend(row.get('center_source_ids') or [])
        for cid_value in center_candidates:
            cid = str(cid_value or '').strip()
            if cid and cid not in center_ids:
                center_ids.append(cid)
        hub_id = _share_channel_hub_id(row)
        if hub_id and hub_id not in hub_ids:
            hub_ids.append(hub_id)
        pair = _share_channel_tmdb_season_key(row)
        if pair:
            row_pairs[id(row)] = pair
            if pair[0] not in tmdb_ids:
                tmdb_ids.append(pair[0])
    if not local_ids and not center_ids and not hub_ids and not tmdb_ids:
        return
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                clauses, args = [], []
                if local_ids:
                    clauses.append('local_source_id = ANY(%s)')
                    args.append(local_ids)
                if center_ids:
                    clauses.append('center_source_id = ANY(%s)')
                    args.append(center_ids)
                if hub_ids:
                    clauses.append('hub_id = ANY(%s)')
                    args.append(hub_ids)
                if tmdb_ids:
                    clauses.append(
                        """
                        (
                            raw_json->'event'->>'tmdb_id' = ANY(%s)
                            OR raw_json->'payload'->>'tmdb_id' = ANY(%s)
                            OR raw_json->>'tmdb_id' = ANY(%s)
                        )
                        """
                    )
                    args.extend([tmdb_ids, tmdb_ids, tmdb_ids])
                cur.execute(
                    f"""
                    SELECT *
                    FROM shared_completed_season_share_channels
                    WHERE {' OR '.join(clauses)}
                    ORDER BY CASE status WHEN 'valid' THEN 0 WHEN 'pending_review' THEN 1 WHEN 'creating' THEN 2 ELSE 9 END,
                             updated_at DESC NULLS LAST, id DESC
                    """,
                    args,
                )
                channels = [dict(r) for r in cur.fetchall() or []]
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 读取本地完结季分享通道失败: {e}")
        return
    by_local = {}
    by_center = {}
    by_hub = {}
    by_pair = {}
    for ch in channels:
        lid = ch.get('local_source_id')
        if lid not in (None, '') and int(lid) not in by_local:
            by_local[int(lid)] = ch
        cid = str(ch.get('center_source_id') or '').strip()
        if cid and cid not in by_center:
            by_center[cid] = ch
        hub_id = _share_channel_hub_id(ch)
        if hub_id and hub_id not in by_hub:
            by_hub[hub_id] = ch
        pair = _share_channel_tmdb_season_key(ch)
        if pair and pair not in by_pair:
            by_pair[pair] = ch
    for row in rows:
        channel = None
        try:
            channel = by_local.get(int(row.get('id') or 0))
        except Exception:
            channel = None
        if not channel and isinstance(row.get('source_ids'), list):
            for sid in row.get('source_ids') or []:
                try:
                    channel = by_local.get(int(sid or 0))
                except Exception:
                    channel = None
                if channel:
                    break
        if not channel:
            channel = by_center.get(str(row.get('center_source_id') or '').strip())
        if not channel and isinstance(row.get('center_source_ids'), list):
            for cid in row.get('center_source_ids') or []:
                channel = by_center.get(str(cid or '').strip())
                if channel:
                    break
        if not channel:
            channel = by_hub.get(_share_channel_hub_id(row))
        if not channel:
            channel = by_pair.get(row_pairs.get(id(row)) or _share_channel_tmdb_season_key(row))
        if not channel:
            row['has_share_channel'] = False
            row['share_channel_status'] = 'none'
            continue
        public = _local_completed_share_public(channel)
        status = str(channel.get('status') or '').strip().lower()
        row['completed_share_channel'] = public
        row['has_share_channel'] = True
        row['share_channel_status'] = status
        row['share_review_status'] = channel.get('review_status') or ''
        row['share_status_message'] = channel.get('status_message') or ''
        row['has_valid_share_channel'] = status == 'valid'
        row['share_transfer_available'] = status == 'valid'


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
    failed = status in {'failed', 'error', 'dead', 'expired', 'rejected', 'inconsistent', 'incomplete', 'raw_missing', 'dirty_raw', 'dirty_summary', 'dirty_meta'} or center_status in {'failed', 'error', 'dead', 'expired', 'rejected', 'raw_missing', 'dirty_raw', 'dirty_summary', 'dirty_meta'}
    reported = center_status in {'reported', 'partial'} or has_center_id
    local_only = not has_center_id and center_status in {'', 'local', 'pending', 'not_reported'}

    share_channel_status = str(row.get('share_channel_status') or '').strip().lower()
    has_share_channel = bool(row.get('has_share_channel'))

    for token in tokens:
        if token in {'usable', 'active', 'alive', 'valid', 'valid_share', '有效', '有效共享'}:
            if live and not disabled and not failed:
                return True
        elif token in {'with_share', 'has_share', 'share', '已创建分享', '有分享'}:
            if has_share_channel:
                return True
        elif token in {'share_valid', 'valid_channel', '可转存', '分享可用'}:
            if share_channel_status == 'valid':
                return True
        elif token in {'share_pending', 'pending_review', '分享审核中', '待审核分享'}:
            if share_channel_status in {'creating', 'pending_review'}:
                return True
        elif token in {'share_abnormal', 'share_failed', '分享异常'}:
            if share_channel_status in {'review_failed', 'expired', 'import_failed', 'disabled', 'source_unavailable', 'failed'}:
                return True
        elif token in {'without_share', 'no_share', 'none_share', '无分享'}:
            if not has_share_channel:
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
    _center_home_proxy_cache_clear()
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
                    file_count, title, file_name, source_provider,
                    COALESCE(
                        raw_json->'center_response'->'item'->>'hub_id',
                        raw_json->'event'->>'hub_id',
                        raw_json->'payload'->>'hub_id',
                        raw_json->>'hub_id'
                    ) AS hub_id
                FROM shared_rapid_sources 
                {where_sql} 
                ORDER BY {order_sql} 
                LIMIT %s
            """, args + [raw_limit])
            light_rows = _rows(cur.fetchall())

    # 2. 在内存中进行聚合和过滤（因为没有庞大的 JSON，这一步只需 1-2 毫秒）
    aggregated = _aggregate_local_sources(light_rows)
    _attach_completed_share_channels_to_local_rows(aggregated)
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
    if center_status in {'disabled', 'cancelled', 'canceled', 'deleted', 'dirty_raw', 'dirty_summary', 'dirty_meta'}:
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



def _p115_response_ok(resp: Any) -> bool:
    if not isinstance(resp, dict):
        return False
    if resp.get('state') is True or resp.get('success') is True:
        return True
    code = str(resp.get('errno') if resp.get('errno') is not None else resp.get('code') if resp.get('code') is not None else '')
    return code in {'0', '200'} and not (resp.get('error') or resp.get('error_msg'))


def _p115_response_text(resp: Any) -> str:
    try:
        return json.dumps(resp, ensure_ascii=False, default=str)
    except Exception:
        return str(resp or '')


def _p115_share_deleted_ok(resp: Any) -> bool:
    if _p115_response_ok(resp):
        return True
    text = _p115_response_text(resp).lower()
    return any(x in text for x in (
        '已删除', '删除成功', '已取消', '取消成功', '不存在', '失效', '过期',
        'not found', 'deleted', 'delete success', 'cancelled', 'canceled', 'expired', 'success'
    ))


def _delete_p115_share_record(p115, share_code: str) -> Dict[str, Any]:
    """删除 115 链接分享列表里的记录；失败时才退回取消分享。

    取消分享只会让 Web 列表显示“已取消”，时间久了就是垃圾。
    所以这里优先 share_delete；如果旧账号/接口要求先取消，再 cancel 后补一次 delete。
    """
    share_code = str(share_code or '').strip()
    if not share_code:
        return {'state': True, 'skipped': True, 'message': '无 share_code，无需删除 115 分享记录'}
    if not p115:
        return {'state': False, 'error_msg': '115 客户端未初始化'}

    attempts = []

    delete_method = getattr(p115, 'share_delete', None)
    cancel_method = getattr(p115, 'share_cancel', None)

    if callable(delete_method):
        try:
            resp = delete_method(share_code)
            attempts.append({'method': 'share_delete', 'response': resp})
            if _p115_share_deleted_ok(resp):
                return {'state': True, 'deleted': True, 'method': 'share_delete', 'attempts': attempts}
        except Exception as e:
            attempts.append({'method': 'share_delete', 'error': str(e)})

    if callable(cancel_method):
        try:
            resp = cancel_method(share_code)
            attempts.append({'method': 'share_cancel', 'response': resp})
        except Exception as e:
            attempts.append({'method': 'share_cancel', 'error': str(e)})

    if callable(delete_method):
        try:
            resp = delete_method(share_code)
            attempts.append({'method': 'share_delete_after_cancel', 'response': resp})
            if _p115_share_deleted_ok(resp):
                return {'state': True, 'deleted': True, 'method': 'share_delete_after_cancel', 'attempts': attempts}
        except Exception as e:
            attempts.append({'method': 'share_delete_after_cancel', 'error': str(e)})

    # 如果至少取消成功，但删除记录失败，也返回非阻断结果，避免本地/中心一直保留可转存。
    cancel_ok = any(_p115_share_deleted_ok(a.get('response')) for a in attempts if a.get('method') == 'share_cancel')
    return {
        'state': bool(cancel_ok),
        'deleted': False,
        'cancelled_only': bool(cancel_ok),
        'error_msg': '' if cancel_ok else '删除/取消 115 分享均失败',
        'attempts': attempts,
    }


@shared_resource_bp.route('/shares/<int:source_id>/share/cancel', methods=['POST'])
@admin_required
def api_cancel_completed_season_share_channel(source_id: int):
    return jsonify({'success': False, 'message': '旧 completed_season 分享取消接口已停用；逻辑季分享由中心 logical_season_share_channels 管理。'}), 410


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
        message = '共享源已停用、异常或无需取消中心登记，已直接删除本地数据'
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
    # 客户端只登记视频资源；Season 候选只是本地批量登记入口，最终仍会拆成分集上传中心。
    rows = shared_share_db.search_shareable_media(keyword, search_limit=max(limit * 6, 200), result_limit=max(limit * 4, 100))
    items = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        row = _apply_local_season_meta(row)
        item_type = str(row.get('share_item_type') or row.get('item_type') or '').strip().lower()
        share_type = str(row.get('share_type') or '').strip().lower()
        if item_type in ('movie', 'season', 'episode', 'series') or share_type in ('movie_file', 'movie_folder', 'season_pack', 'episode_file', 'series_pack'):
            items.append(row)
        if len(items) >= limit:
            break
    return jsonify({'success': True, 'items': items})


@shared_resource_bp.route('/shares/manual-validate', methods=['POST'])
@admin_required
def api_manual_validate():
    """Rapid v2 手动登记前预校验。

    新方案不再由客户端做完结季一致性校验，也不再登记 completed_season_source。
    这里只确认本地能定位到可秒传文件，并检查 RAW/summary_json 是否可上传中心；
    Season 会拆成分集资产，后续由中心逻辑季包统一凑整季。
    """
    data = shared_tasks._normalize_series_candidate_identity(_request_json())
    data['_skip_fingerprint_repair'] = True
    files = shared_share_db.collect_files_for_candidate(data)
    root = shared_share_db.candidate_root_from_files(files)
    missing_raw = []
    for f in files:
        sha1 = str(f.get('sha1') or '').upper()
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
        message = f'找到 {len(files)} 个可登记视频文件，可登记为 Rapid v2 逻辑季资产'
        valid = True

    data_payload = {
        'valid': valid,
        'message': message,
        'file_count': len(files),
        'missing_raw': missing_raw,
        'files': files,
        'root': root,
        'root_fid': root.get('root_fid') or '',
        'reason': 'raw_missing' if missing_raw else '',
        'center_managed_logical_season': True,
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


def _center_source_is_completed_certified(row: Dict[str, Any]) -> bool:
    """中心资源库“已完结认证”只认中心逻辑季包的 pool_complete。

    分享通道是否 valid 只影响“转存/秒传”按钮；只要逻辑季包已经通过中心
    一致性校验，就可以显示已完结标签和缎带。
    """
    row = row if isinstance(row, dict) else {}
    source_kind = str(row.get('source_kind') or '').strip().lower()
    status = str(row.get('status') or '').strip().lower()
    logical_complete = bool(row.get('logical_pool_complete') or row.get('pool_complete') or status == 'pool_complete')
    if source_kind == 'season_hub':
        return False
    if source_kind == 'logical_season':
        return logical_complete
    if row.get('is_ongoing_hub') and not logical_complete:
        return False
    if logical_complete:
        return True
    return bool(_center_flag_meta(row, 'is_completed_certified', 'completed_certified_meta_json'))


def _center_source_is_clean_version(row: Dict[str, Any]) -> bool:
    return bool(_center_flag_meta(row, 'is_clean_version', 'clean_version_meta_json'))


def _center_source_is_short_drama(row: Dict[str, Any]) -> bool:
    direct = _center_direct_flag_state(row, 'is_short_drama', 'short_drama_meta_json')
    if direct is not None:
        return bool(direct)
    return bool(_center_flag_meta(row, 'is_short_drama', 'short_drama_meta_json'))


def _center_import_looks_like_logical_group_id(value: Any) -> bool:
    text = str(value or '').strip().lower()
    return bool(text and re.match(r'^(svg_|lsg_|logical_season_)', text))


def _center_import_logical_group_id(source: Dict[str, Any], fallback: Any = '') -> str:
    source = source if isinstance(source, dict) else {}
    logical_group = source.get('logical_group') if isinstance(source.get('logical_group'), dict) else {}
    for value in (
        source.get('logical_group_id'),
        source.get('group_id'),
        logical_group.get('group_id'),
        logical_group.get('source_id'),
        source.get('logical_season_group_id'),
        source.get('source_id'),
        source.get('source_ref_id'),
        fallback,
    ):
        text = str(value or '').strip()
        if _center_import_looks_like_logical_group_id(text):
            return text
    for value in (source.get('logical_group_id'), source.get('group_id'), logical_group.get('group_id')):
        text = str(value or '').strip()
        if text:
            return text
    return ''


def _center_import_normalize_source(source: Dict[str, Any]) -> Dict[str, Any]:
    source = dict(source or {})
    source_kind = str(source.get('source_kind') or source.get('kind') or '').strip().lower().replace('-', '_')
    if source_kind == 'completed_season':
        source['_legacy_completed_season_rejected'] = True
    return source


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


def _virtual_source_metadata(source: Dict[str, Any], files: List[Dict[str, Any]]) -> Dict[str, Any]:
    source = source if isinstance(source, dict) else {}
    first = next((f for f in (files or []) if isinstance(f, dict)), {}) or {}
    item_type = source.get('item_type') or first.get('item_type') or ''
    tmdb_id = source.get('tmdb_id') or first.get('tmdb_id') or ''
    parent_tmdb = source.get('parent_series_tmdb_id') or source.get('series_tmdb_id') or first.get('parent_series_tmdb_id') or first.get('series_tmdb_id') or ''
    if item_type == 'Episode' or str(source.get('source_kind') or '').lower() in {'logical_season', 'season_hub'}:
        tmdb_id = parent_tmdb or tmdb_id
    total_size = 0
    for f in files or []:
        if isinstance(f, dict):
            total_size += _safe_int(f.get('size') or f.get('file_size'), 0)
    return {
        'tmdb_id': tmdb_id,
        'item_type': item_type or ('Movie' if str(source.get('source_kind') or '').lower() == 'movie' else 'Episode'),
        'parent_series_tmdb_id': parent_tmdb,
        'season_number': source.get('season_number') if source.get('season_number') not in (None, '') else first.get('season_number'),
        'episode_number': source.get('episode_number') if source.get('episode_number') not in (None, '') else first.get('episode_number'),
        'title': source.get('title') or first.get('title') or first.get('file_name') or '',
        'release_year': source.get('release_year') or source.get('year') or first.get('release_year'),
        'file_count': len(files or []),
        'total_size': total_size,
    }


def _delete_virtual_files(row: Dict[str, Any]) -> int:
    deleted = 0
    paths = row.get('strm_paths_json') if isinstance(row.get('strm_paths_json'), list) else []
    for path in paths:
        text = str(path or '').strip()
        if not text:
            continue
        for candidate in (text, re.sub(r'\.strm$', '-mediainfo.json', text, flags=re.I)):
            try:
                if candidate and os.path.exists(candidate):
                    os.remove(candidate)
                    deleted += 1
            except Exception as e:
                logger.debug(f"  ➜ [虚拟入库] 删除本地文件失败：{candidate} -> {e}")
    return deleted


def _virtual_strm_paths(row: Dict[str, Any]) -> List[str]:
    paths = row.get('strm_paths_json') if isinstance(row.get('strm_paths_json'), list) else []
    out, seen = [], set()
    for path in paths:
        text = str(path or '').strip()
        if text and text not in seen:
            seen.add(text)
            out.append(text)
    return out


def _virtual_emby_delete_path(row: Dict[str, Any]) -> str:
    paths = _virtual_strm_paths(row)
    if not paths:
        return ''
    item_type = str(row.get('item_type') or '').strip().lower()
    if item_type in {'tv', 'series', 'season', 'episode'}:
        return os.path.dirname(paths[0])
    return paths[0]


def _find_emby_item_by_path(path: str) -> Dict[str, Any]:
    path = str(path or '').strip()
    base_url = str(config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_SERVER_URL) or '').rstrip('/')
    api_key = str(config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_KEY) or '').strip()
    if not path or not base_url or not api_key:
        return {}
    try:
        resp = requests.get(
            f"{base_url}/Items",
            params={
                "api_key": api_key,
                "Recursive": "true",
                "Path": path,
                "Fields": "Id,Path,Name,Type",
                "Limit": 5,
            },
            timeout=10,
        )
        if resp.status_code != 200:
            return {}
        wanted = os.path.normcase(os.path.normpath(path))
        items = resp.json().get("Items") or []
        for item in items:
            item_path = str(item.get("Path") or "")
            if item_path and os.path.normcase(os.path.normpath(item_path)) == wanted:
                return item
        return items[0] if items else {}
    except Exception as e:
        logger.debug(f"  ➜ [虚拟入库] 查询 Emby 虚拟项失败：{path} -> {e}")
    return {}


def _delete_virtual_emby_item(row: Dict[str, Any]) -> Dict[str, Any]:
    delete_path = _virtual_emby_delete_path(row)
    item = _find_emby_item_by_path(delete_path)
    item_id = str(item.get('Id') or '').strip()
    if not item_id:
        return {'ok': True, 'found': False, 'path': delete_path}
    base_url = str(config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_SERVER_URL) or '').rstrip('/')
    api_key = str(config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_KEY) or '').strip()
    user_id = str(config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_USER_ID) or '').strip()
    ok = emby.delete_item(item_id, base_url, api_key, user_id)
    return {'ok': bool(ok), 'found': True, 'id': item_id, 'name': item.get('Name') or '', 'path': delete_path}


def _delete_virtual_p115_cache(row: Dict[str, Any]) -> int:
    try:
        virtual_id = int(row.get('id') or 0)
    except Exception:
        virtual_id = 0
    if virtual_id <= 0:
        return 0
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM p115_filesystem_cache WHERE parent_id = %s OR id LIKE %s",
                    (f"virtual:{virtual_id}", f"virtual:{virtual_id}:%"),
                )
                deleted = cursor.rowcount or 0
            conn.commit()
        return deleted
    except Exception as e:
        logger.debug(f"  ➜ [虚拟入库] 清理 115 虚拟缓存失败: virtual_id={virtual_id} -> {e}")
        return 0


def _remove_virtual_import_record(row: Dict[str, Any]) -> Dict[str, Any]:
    deleted_files = _delete_virtual_files(row)
    deleted_cache = _delete_virtual_p115_cache(row)
    deleted_row = shared_virtual_db.delete_virtual_import(int(row.get('id')))
    return {'item': deleted_row or row, 'deleted_files': deleted_files, 'deleted_cache': deleted_cache}


def _delete_virtual_import_record_only(row: Dict[str, Any]) -> Dict[str, Any]:
    deleted_cache = _delete_virtual_p115_cache(row)
    deleted_row = shared_virtual_db.delete_virtual_import(int(row.get('id')))
    return {'item': deleted_row or row, 'deleted_files': 0, 'deleted_cache': deleted_cache}


def _track_list_value(value: Any) -> List[Any]:
    if value in (None, '', [], {}):
        return []
    if isinstance(value, list):
        return [x for x in value if x not in (None, '', [], {})]
    return [value]


def _merge_track_values(*values: Any) -> List[Any]:
    out: List[Any] = []
    seen = set()
    for value in values:
        for item in _track_list_value(value):
            try:
                key = json.dumps(item, ensure_ascii=False, sort_keys=True, default=str) if isinstance(item, (dict, list)) else str(item)
            except Exception:
                key = str(item)
            if key in seen:
                continue
            seen.add(key)
            out.append(item)
    return out


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
    # audio_list/subtitle_list 是展示字符串；audios/subtitles 只补充 title。
    # 两者都要下发给前端，否则 Title 里的“特效”会被 display 吃掉。
    audio_list = _merge_track_values(out.get('audio_list'), out.get('audios'), out.get('audio'))
    subtitle_list = _merge_track_values(out.get('subtitle_list'), out.get('subtitles'), out.get('subtitle'))
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
            row['lazy_children_kind'] = 'season_hub' if kind == 'season_hub' else 'logical_season'
    return row

@shared_resource_bp.route('/center/sources', methods=['GET'])
@admin_required
def api_center_sources():
    try:
        client = SharedCenterClient()
        q = request.args.get('q') or request.args.get('keyword') or ''
        tmdb_id = request.args.get('tmdb_id') or ''
        params = {
            'q': q,
            'status': request.args.get('status') or 'alive,available',
            'item_type': request.args.get('item_type') or '',
            'tmdb_id': tmdb_id,
            'order_by': request.args.get('order_by') or 'latest',
            'limit': int(request.args.get('limit') or request.args.get('page_size') or 200),
            'offset': int(request.args.get('offset') or 0),
        }
        resp = client.list_display_sources(
            **params,
            force_refresh=False,
        )

        raw_items = [row for row in (resp.get('items') or []) if isinstance(row, dict)]
        # 中心资源库首屏必须保持“中心端已聚合壳”直出。
        # 本地 media_metadata 补总集数/追剧状态会递归扫 seasons/versions，
        # 对海报墙首屏没有必要；需要本地补充时由详情/children 接口按需处理。
        local_season_meta_map = {}
        if _boolish(request.args.get('local_enrich'), False):
            local_season_meta_map = _batch_lookup_local_season_meta(raw_items)

        def _decorate_center_row(row):
            if not isinstance(row, dict):
                return {}
            row = dict(row)
            # 列表页只保留壳；children/pack_items 会被瘦身丢弃，不能先递归装饰再丢，
            # 否则旧中心或筛选结果一旦带子项，就会把首屏又拖回 N+1。
            for key in ('versions',):
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
                    'certified_by': 'logical_season_pool',
                    'status': row.get('status'),
                }
            else:
                row['is_completed_certified'] = False
                row['is_completed'] = False
                row.pop('completed_certified_meta_json', None)
            row['version_summary'] = _center_version_summary(row)
            if not row.get('size') and row.get('total_size'):
                row['size'] = row.get('total_size')
            if local_season_meta_map:
                row = _apply_local_season_meta(row, local_season_meta_map)
            return row

        resp['items'] = [
            _strip_center_display_children(_decorate_center_row(row))
            for row in raw_items
        ]
        return jsonify({'success': True, **resp})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e), 'items': [], 'total': 0}), 500


@shared_resource_bp.route('/center/sources/tags', methods=['GET'])
@admin_required
def api_center_source_tags():
    fallback_items = [
        {'label': '已完结', 'value': 'completed_certified'},
        {'label': '连载中', 'value': 'ongoing'},
        {'label': '短剧', 'value': 'short_drama'},
        {'label': '纯净版', 'value': 'clean_version'},
        {'label': '片头', 'value': 'intro'},
        {'label': '原盘', 'value': 'original_disc'},
        {'label': '国语', 'value': 'mandarin_audio'},
        {'label': '中字', 'value': 'chinese_subtitle'},
        {'label': '特效', 'value': 'effect_subtitle'},
    ]
    try:
        resp = SharedCenterClient().list_display_tags()
        items = [row for row in (resp.get('items') or []) if isinstance(row, dict)]
        return jsonify({'success': True, 'items': items or fallback_items})
    except Exception as e:
        return jsonify({'success': True, 'items': fallback_items, 'message': str(e)})


@shared_resource_bp.route('/center/sources/home', methods=['GET'])
@admin_required
def api_center_sources_home():
    try:
        client = SharedCenterClient()
        limit_per_section = max(1, min(int(request.args.get('limit_per_section') or 10), 20))
        force_refresh = _boolish(
            request.args.get('force_refresh') or request.args.get('refresh') or request.args.get('no_cache'),
            False,
        )
        home_sections = _shared_resource_config_payload().get('p115_shared_center_home_sections') or []
        identity_key = _current_server_id_hash()
        cache_key = (client.base_url, identity_key, limit_per_section, json.dumps(home_sections, sort_keys=True, ensure_ascii=False))
        if not force_refresh:
            cached = _center_home_proxy_cache_get(cache_key)
            if cached:
                return jsonify(cached)

        resp = client.list_display_home(
            limit_per_section=limit_per_section,
            force_refresh=False,
            sections=home_sections,
        )

        def _decorate_center_row(row):
            if not isinstance(row, dict):
                return {}
            row = dict(row)
            completed_certified = _center_source_is_completed_certified(row)
            if completed_certified:
                row['is_completed_certified'] = True
                row['is_completed'] = True
                row['completed_certified_meta_json'] = _center_flag_meta(row, 'is_completed_certified', 'completed_certified_meta_json') or {
                    'is_completed_certified': True,
                    'certified_by': 'logical_season_pool',
                    'status': row.get('status'),
                }
            row['version_summary'] = _center_version_summary(row)
            if not row.get('size') and row.get('total_size'):
                row['size'] = row.get('total_size')
            return _strip_center_display_children(row)

        sections = []
        for section in resp.get('sections') or []:
            if not isinstance(section, dict):
                continue
            items = [_decorate_center_row(row) for row in section.get('items') or [] if isinstance(row, dict)]
            sections.append({**section, 'items': items})
        resp['sections'] = sections
        resp['items'] = []
        center_cache_hit = bool(resp.get('cache_hit'))
        if 'cache_hit' in resp:
            resp['center_cache_hit'] = center_cache_hit
            resp['cache_hit'] = center_cache_hit
        payload = {'success': True, **resp}
        has_center_cache_miss = any(
            section.get('center_public_rank_cache_miss')
            or section.get('public_rank_cache_pending')
            or section.get('source_schema') == 'display_home_section_cache_miss'
            or any(
                isinstance(item, dict) and (
                    item.get('center_public_rank_cache_miss')
                    or item.get('public_rank_cache_pending')
                    or item.get('source_schema') == 'display_public_rank_cache_miss'
                )
                for item in (section.get('items') or [])
            )
            for section in sections
        )
        if int(resp.get('total') or 0) > 0 and not has_center_cache_miss:
            _center_home_proxy_cache_set(cache_key, payload)
        return jsonify(payload)
    except Exception as e:
        return jsonify({'success': False, 'message': str(e), 'items': [], 'sections': []}), 500


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
                    'certified_by': 'logical_season_pool',
                    'status': row.get('status'),
                }
            else:
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

        for key in ('items', 'children', 'pack_items', 'parents', 'seasons', 'resources', 'versions'):
            if isinstance(resp.get(key), list):
                resp[key] = [_decorate_center_row(row) for row in resp.get(key) if isinstance(row, dict)]
        return jsonify({'success': True, **resp})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e), 'items': [], 'children': [], 'pack_items': [], 'total': 0}), 500



@shared_resource_bp.route('/center/sources/detail', methods=['GET'])
@admin_required
def api_center_source_detail():
    """中心资源库卡片详情：代理中心 display-detail，供详情模态框按需加载。"""
    try:
        client = SharedCenterClient()
        include_people = str(request.args.get('include_people') or '0').strip().lower() not in {'0', 'false', 'no', 'off'}
        limit = int(request.args.get('limit') or 200)

        resp = client.display_detail(
            source_kind=request.args.get('source_kind') or '',
            source_id=request.args.get('source_id') or '',
            hub_id=request.args.get('hub_id') or '',
            tmdb_id=request.args.get('tmdb_id') or '',
            item_type=request.args.get('item_type') or '',
            season_number=request.args.get('season_number') or None,
            limit=limit,
            include_people=include_people,
        )

        def _is_pack_detail_row(row):
            row = row if isinstance(row, dict) else {}
            kind = str(row.get('source_kind') or row.get('lazy_children_kind') or '').strip().lower()
            typ = str(row.get('display_type') or row.get('item_type') or '').strip().lower()
            return bool(kind in {'season_hub', 'logical_season'} or typ in {'pack', 'season', 'series'})

        def _decorate_detail_row(row):
            if not isinstance(row, dict):
                return {}
            row = dict(row)
            # 详情页资源列表只展示“电影源/季包源”这一层。
            # children / pack_items 是季包包内单集，避免详情弹窗把整季几十集全部铺出来。
            if _is_pack_detail_row(row):
                row.pop('children', None)
                row.pop('pack_items', None)
            else:
                for key in ('versions', 'children', 'pack_items', 'resources'):
                    if isinstance(row.get(key), list):
                        row[key] = [_decorate_detail_row(x) for x in row.get(key) if isinstance(x, dict)]
            row['version_summary'] = _center_version_summary(row)
            if not row.get('size') and row.get('total_size'):
                row['size'] = row.get('total_size')
            completed_certified = _center_source_is_completed_certified(row)
            if completed_certified:
                row['is_completed_certified'] = True
                row['is_completed'] = True
                row['completed_certified_meta_json'] = row.get('completed_certified_meta_json') or {
                    'is_completed_certified': True,
                    'certified_by': 'logical_season_pool',
                    'status': row.get('status'),
                }
            row = _apply_local_season_meta(row)
            return row

        for key in ('resources', 'versions', 'items', 'seasons'):
            if isinstance(resp.get(key), list):
                resp[key] = [_decorate_detail_row(x) for x in resp.get(key) if isinstance(x, dict)]
        # 顶层 children / pack_items 不再给详情弹窗使用；展开集详情另走 children 接口。
        resp['children'] = []
        resp['pack_items'] = []
        payload = {'success': True, 'data': resp, **resp}
        return jsonify(payload)
    except Exception as e:
        return jsonify({'success': False, 'message': str(e), 'data': {}, 'resources': [], 'versions': [], 'children': []}), 500


@shared_resource_bp.route('/center/import', methods=['POST'])
@admin_required
def api_center_import():
    data = _request_json()
    source = data.get('source') if isinstance(data.get('source'), dict) else data

    # 旧前端曾只提交 source_ids/context，没有提交完整中心源行；Rapid v2 需要 source_kind/source_id/sha1 等字段。
    # 这里给出明确错误，避免前端继续显示“秒传完成 0/0”。
    if not isinstance(source, dict):
        source = {}
    source = _center_import_normalize_source(source)
    if source.get('_legacy_completed_season_rejected'):
        return jsonify({'success': False, 'message': '旧 completed_season 已停用，请刷新中心资源库后使用 logical_season。', 'data': {'ok': False}}), 400
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

    cfg = _shared_resource_config_payload()
    if _boolish(cfg.get('p115_shared_virtual_import_enabled'), False) and not _boolish(data.get('force_real'), False):
        prepared = prepare_center_source_files_for_virtual(source)
        if not prepared.get('ok'):
            return jsonify({'success': False, 'message': prepared.get('message') or '虚拟入库失败', 'data': prepared}), 400
        files = prepared.get('files') or []
        meta = _virtual_source_metadata(
            {
                **source,
                'source_kind': prepared.get('source_kind') or source_kind,
                'source_id': prepared.get('source_id') or source_id,
            },
            files,
        )
        row = shared_virtual_db.create_virtual_import({
            **meta,
            'source_kind': prepared.get('source_kind') or source_kind,
            'source_id': prepared.get('source_id') or source_id,
            'source_payload_json': source,
            'files_json': files,
        })
        strm_paths = create_virtual_strm_files(source, files, row['id'])
        row = shared_virtual_db.update_virtual_import(row['id'], strm_paths_json=strm_paths)
        message = f"虚拟入库完成：{len(strm_paths)}/{len(files)}"
        return jsonify({'success': True, 'message': message, 'data': {'ok': True, 'virtual': True, 'item': row, 'preflight': prepared.get('preflight') or {}}})

    event = {'event_id': '', 'source_kind': source_kind, 'source_ref_id': source_id, 'payload_json': source}
    # 前端手动秒传不经过后台长轮询 poll_and_consume_once，不能直接调用
    # handler.shared_subscription_service.consume_device_event；否则会绕过秒传许可 lease。
    result = shared_tasks.consume_device_event_with_transfer_gate(event, ack=False)
    status = 200 if result.get('ok') else 400
    message = result.get('message') or f"秒传完成：{result.get('success_count', 0)}/{result.get('total', 0)}"
    return jsonify({'success': bool(result.get('ok')), 'message': message, 'data': result}), status


@shared_resource_bp.route('/virtual-imports', methods=['GET'])
@admin_required
def api_virtual_imports():
    data = shared_virtual_db.list_virtual_imports(
        status=request.args.get('status') or 'virtual',
        keyword=request.args.get('keyword') or request.args.get('q') or '',
        item_type=request.args.get('item_type') or request.args.get('type') or 'all',
        page=int(request.args.get('page') or 1),
        page_size=int(request.args.get('page_size') or 30),
    )
    return jsonify({'success': True, **data})


@shared_resource_bp.route('/virtual-imports/<int:virtual_id>', methods=['DELETE'])
@admin_required
def api_delete_virtual_import(virtual_id: int):
    row = shared_virtual_db.get_virtual_import(virtual_id)
    if not row:
        return jsonify({'success': False, 'message': '虚拟入库记录不存在'}), 404
    emby_result = _delete_virtual_emby_item(row)
    if emby_result.get('found') and not emby_result.get('ok'):
        return jsonify({'success': False, 'message': 'Emby 虚拟项删除失败，已停止辞退', 'data': {'emby_delete': emby_result}}), 500
    return jsonify({
        'success': True,
        'message': '已提交辞退，等待 Emby 删除 webhook 善后清理虚拟入库记录',
        'data': {'item': row, 'emby_delete': emby_result},
    })


@shared_resource_bp.route('/virtual-imports/<int:virtual_id>/promote', methods=['POST'])
@admin_required
def api_promote_virtual_import(virtual_id: int):
    row = shared_virtual_db.get_virtual_import(virtual_id)
    if not row:
        return jsonify({'success': False, 'message': '虚拟入库记录不存在'}), 404
    source = row.get('source_payload_json') if isinstance(row.get('source_payload_json'), dict) else {}
    if not source:
        return jsonify({'success': False, 'message': '虚拟记录缺少中心源 payload，无法正式入库'}), 400
    source_kind = source.get('source_kind') or row.get('source_kind') or ''
    source_id = source.get('source_id') or source.get('source_ref_id') or row.get('source_id') or ''
    marked = shared_virtual_db.mark_active_washing_for_virtual_import(virtual_id, True)
    logger.info(f"  ➜ [虚拟入库] 手动转正已开启 active_washing 特权：virtual_id={virtual_id}, rows={marked}")
    event = {'event_id': '', 'source_kind': source_kind, 'source_ref_id': source_id, 'payload_json': {**source, '_virtual_auto_promote': True, '_virtual_id': virtual_id}}
    result = shared_tasks.consume_device_event_with_transfer_gate(event, ack=False)
    status = 200 if result.get('ok') else 400
    if result.get('ok'):
        item = shared_virtual_db.update_virtual_import(virtual_id, status='promoting', promoted_at='NOW()')
        return jsonify({
            'success': True,
            'message': result.get('message') or '已提交转正，等待正式入库完成后清理虚拟记录',
            'data': {'result': result, 'item': item},
        }), status
    return jsonify({'success': False, 'message': result.get('message') or '正式入库失败', 'data': result}), status


@shared_resource_bp.route('/center/device/register', methods=['POST'])
@admin_required
def api_register_center_device():
    cfg = _shared_resource_config_payload()
    name = socket.gethostname() or 'ETK Device'
    try:
        client = SharedCenterClient()
        resp = client.register_device(name=name)
        cfg.update({
            'p115_shared_resource_enabled': True,
        })
        saved = _save_shared_config(cfg)
        try:
            shared_tasks.ensure_shared_device_event_listener()
        except Exception:
            pass
        return jsonify({'success': True, 'message': '共享资源中心已按 ServerID 重新连接，监听已刷新', 'data': saved, 'device': resp})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@shared_resource_bp.route('/center/device/status', methods=['GET'])
@admin_required
def api_center_device_status():
    try:
        resp = SharedCenterClient().device_status()
        data = {
            **resp,
            'local_server_id_hash': _current_server_id_hash(),
        }
        return jsonify({'success': True, 'data': data, **data})
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e),
            'data': {'local_server_id_hash': _current_server_id_hash()},
        }), 500


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
    'center_share_source_served': '115分享被转存',
    'center_share_source_served_group': '115分享被转存',
    'center_share_source_consumed': '转存115分享资源',
    'center_share_source_consumed_group': '转存115分享资源',
    'share_source_served': '115分享被转存',
    'share_source_consumed': '转存115分享资源',
    'center_rapid_sign_success': '秒传签名成功',
    'center_rapid_sign_failed': '秒传签名失败',
    'center_rapid_sign_timeout': '秒传签名超时',
    'center_rapid_sign_job_success': '秒传签名成功',
    'center_rapid_sign_job_failed': '秒传签名失败',
    'center_rapid_raw_uploaded': '上传媒体信息',
    'center_rapid_raw_ffprobe_uploaded': '上传媒体信息',
    'center_intro_chapters_uploaded': '上传片头',
    'center_intro_chapters_batch_fetched': '使用共享片头',
    'center_intro_chapters_served': '共享片头被使用',
    'center_intro_chapters_consumed': '使用共享片头',
    'intro_chapters_uploaded': '上传片头',
    'intro_chapters_batch_fetched': '使用共享片头',
    'intro_chapters_served': '共享片头被使用',
    'intro_chapters_consumed': '使用共享片头',
    'center_daily_grant': 'Pro每日赠送额度',
    'center_rapid_quota_consumed': 'Pro额度抵扣',
    'virtual_play': '虚拟播放',
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
    'share_source_consumed': '从共享中心转存 115 分享资源',
    'share_source_served': '本机 115 分享被他人转存',
    'source_registered': '共享资源登记入池',
    'center_initial_credit': '基础贡献点',
    'backup_source_registered': '备份共享入池',
    'shared_source_served': '共享资源被他人秒传',
    'shared_source_consumed': '从共享中心秒传资源',
    'intro_chapters_served': '共享片头被使用',
    'intro_chapters_consumed': '使用共享片头',
    'daily_grant': 'Pro每日赠送额度',
    'rapid_quota_consumed': 'Pro额度抵扣',
    'virtual_play': '虚拟播放',
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
    if 'share_source' in low and 'consume' in low:
        return '转存115分享资源'
    if 'share_source' in low and 'serv' in low:
        return '115分享被转存'
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


def _ledger_title_is_identity(value: Any) -> bool:
    text = str(value or '').strip()
    if not text:
        return True
    if text.lower().startswith('rapid_sign:'):
        return True
    return bool(re.fullmatch(r'(?:rapid_sign:)?[A-Fa-f0-9]{40}(?::.*)?', text))


def _ledger_local_source_context(row: Dict[str, Any]) -> Dict[str, Any]:
    row = row if isinstance(row, dict) else {}
    raw = _ledger_json(row.get('raw_json'))
    center_ledger = raw.get('center_ledger') if isinstance(raw.get('center_ledger'), dict) else {}
    nested = [raw.get(k) for k in ('media', 'source', 'shared_source', 'job') if isinstance(raw.get(k), dict)]
    ids: List[str] = []
    kinds: List[str] = []
    for source_obj in (row, raw, center_ledger, *nested):
        kind = str(source_obj.get('source_kind') or '').strip()
        if kind and kind not in kinds:
            kinds.append(kind)
        for key in ('source_id', 'source_ref_id', 'center_source_id', 'ref_id'):
            value = str(source_obj.get(key) or '').strip()
            if not value or value in ids or _ledger_title_is_identity(value):
                continue
            ids.append(value)
    if not ids:
        return {}

    cache = getattr(_ledger_local_source_context, '_cache', None)
    if not isinstance(cache, dict):
        cache = {}
        setattr(_ledger_local_source_context, '_cache', cache)
    cache_key = (tuple(ids), tuple(kinds))
    if cache_key in cache:
        return cache[cache_key]

    candidates: List[Dict[str, Any]] = []
    for value in ids:
        try:
            candidates.append(shared_share_db.get_local_source(int(value)) or {})
        except Exception:
            pass
        for kind in kinds or ('movie', 'episode', 'completed_season'):
            try:
                candidates.append(shared_share_db.get_local_source_by_center(kind, value) or {})
            except Exception:
                pass
    cache[cache_key] = next((x for x in candidates if x and not _ledger_title_is_identity(x.get('title') or x.get('file_name'))), {})
    return cache[cache_key]


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
    media = raw.get('media') if isinstance(raw.get('media'), dict) else {}
    source = raw.get('source') if isinstance(raw.get('source'), dict) else {}
    shared_source = raw.get('shared_source') if isinstance(raw.get('shared_source'), dict) else {}
    job = raw.get('job') if isinstance(raw.get('job'), dict) else {}
    out: Dict[str, Any] = {}

    # 贡献点明细的媒体名应优先来自中心 /credit/ledger 的 join 结果。
    # 本地库只做最后兜底，避免本地重组/重命名导致“标题 S03 S03E08”这种重复拼接。
    for source_obj in (center_ledger, row, media, source, shared_source, job):
        for key in (
            'tmdb_id', 'item_type', 'season_number', 'episode_number', 'source_kind',
            'title', 'file_name', 'release_year', 'file_count',
            'series_title', 'series_original_title', 'series_release_year',
        ):
            if source_obj.get(key) not in (None, ''):
                out[key] = source_obj.get(key)

    for key in ('title', 'file_name', 'name'):
        if out.get('title') in (None, '') and raw.get(key):
            out['title'] = raw.get(key)

    source_ctx = _ledger_local_source_context(row)
    if source_ctx:
        title_is_identity = _ledger_title_is_identity(out.get('title'))
        for key in (
            'tmdb_id', 'item_type', 'season_number', 'episode_number', 'source_kind',
            'title', 'file_name', 'release_year',
            'series_title', 'series_original_title', 'series_release_year',
        ):
            if source_ctx.get(key) in (None, ''):
                continue
            if key == 'title':
                if title_is_identity:
                    out[key] = source_ctx.get(key)
                continue
            if out.get(key) in (None, ''):
                out[key] = source_ctx.get(key)

    if (out.get('title') in (None, '') or _ledger_title_is_identity(out.get('title'))) and sha1:
        local = _ledger_local_media_by_sha1(sha1)
        for key in (
            'tmdb_id', 'item_type', 'season_number', 'episode_number', 'source_kind',
            'title', 'file_name', 'release_year',
            'series_title', 'series_original_title', 'series_release_year',
        ):
            if local.get(key) in (None, ''):
                continue
            if key == 'title' and not _ledger_title_is_identity(out.get('title')):
                continue
            if out.get(key) in (None, '') or key == 'title':
                out[key] = local.get(key)
    elif sha1:
        local = _ledger_local_media_by_sha1(sha1)
        for key in ('series_title', 'series_original_title', 'series_release_year'):
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
    return (
        'rapid_source_consumed' in code or reason == 'rapid_source_consumed'
        or 'shared_source_consumed' in code or reason == 'shared_source_consumed'
        or 'share_source_consumed' in code or reason == 'share_source_consumed'
    )


def _ledger_is_served_row(row: Dict[str, Any]) -> bool:
    code = _ledger_event_code(row)
    reason = _ledger_reason_code(row)
    return (
        'rapid_source_served' in code or reason == 'rapid_source_served'
        or 'shared_source_served' in code or reason == 'shared_source_served'
        or 'share_source_served' in code or reason == 'share_source_served'
    )


def _ledger_is_pro_quota_row(row: Dict[str, Any]) -> bool:
    row = row if isinstance(row, dict) else {}
    code = _ledger_event_code(row)
    reason = _ledger_reason_code(row)
    if reason == 'virtual_play':
        return False
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
