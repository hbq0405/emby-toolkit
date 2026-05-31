# tasks/shared_resource_tasks.py
# 共享资源自动维护任务：缺口登记、分享审核同步、中心登记、失效清理、中心缺口自动分享。
import json
import logging
import os
import time
import threading
from typing import Dict, Any, List

import config_manager
import constants
import task_manager
from database import shared_share_db, shared_virtual_db, settings_db
from database.connection import get_db_connection
from handler.p115_service import P115Service
from handler.shared_center_client import SharedCenterClient, shared_center_enabled, shared_resource_mode

logger = logging.getLogger(__name__)


def _enabled() -> bool:
    return shared_center_enabled()

def _is_network_error(e: Any) -> bool:
    """判断异常是否属于网络超时或连接失败"""
    text = str(e).lower()
    return any(k in text for k in (
        'timeout', 'connection', 'read timed out', 
        'max retries exceeded', 'name or service not known', 
        'host is unreachable', 'socket'
    ))


def _safe_int(v, default=0):
    try:
        if v in (None, ''):
            return default
        return int(float(v))
    except Exception:
        return default


def _safe_bool(v, default=False) -> bool:
    if v is None:
        return bool(default)
    if isinstance(v, str):
        return v.strip().lower() in ('1', 'true', 'yes', 'on', '启用', '开启')
    return bool(v)


def _jsonb_non_empty_sql_expr(column: str) -> str:
    """生成 JSONB 标识字段非空判断，兼容数组/对象/字符串。"""
    return f"""
    (
        CASE jsonb_typeof({column})
            WHEN 'array' THEN jsonb_array_length({column}) > 0
            WHEN 'object' THEN {column} <> '{{}}'::jsonb
            WHEN 'string' THEN btrim({column}::text, '"') <> ''
            ELSE FALSE
        END
    )
    """


def _series_has_physical_episode_identity(parent_tmdb_id: str) -> bool:
    return shared_share_db.check_series_has_physical_episode(parent_tmdb_id)


def _consume_mode_for_watching_row(default_mode: str, row: Dict[str, Any], physical_parent_cache: Dict[str, bool] = None) -> str:
    default_mode = str(default_mode or 'permanent').strip().lower()
    if default_mode != 'virtual':
        return default_mode
    parent = str((row or {}).get('parent_series_tmdb_id') or '').strip()
    if not parent:
        return default_mode

    if physical_parent_cache is not None:
        if parent not in physical_parent_cache:
            physical_parent_cache[parent] = _series_has_physical_episode_identity(parent)
        has_physical = bool(physical_parent_cache.get(parent))
    else:
        has_physical = _series_has_physical_episode_identity(parent)

    if has_physical:
        return 'permanent'
    return default_mode


def _remove_file_quietly(path: str) -> bool:
    if not path:
        return False
    try:
        if os.path.exists(path) and os.path.isfile(path):
            os.remove(path)
            return True
    except Exception as e:
        logger.debug(f"  ➜ [共享资源维护] 删除本地投影文件失败: {path} -> {e}")
    return False


def _row_raw_json(row: Dict[str, Any]) -> Dict[str, Any]:
    raw = (row or {}).get('raw_json')
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            val = json.loads(raw)
            return val if isinstance(val, dict) else {}
        except Exception:
            return {}
    return {}


def _delete_emby_item_for_virtual(row: Dict[str, Any]) -> Dict[str, Any]:
    raw = _row_raw_json(row)
    emby_item_id = str(row.get('emby_item_id') or raw.get('last_play_emby_item_id') or raw.get('emby_item_id') or '').strip()
    if not emby_item_id:
        return {'ok': False, 'skipped': True, 'message': 'missing_emby_item_id'}
    emby_url = (config_manager.APP_CONFIG or {}).get(getattr(constants, 'CONFIG_OPTION_EMBY_SERVER_URL', 'emby_server_url'))
    emby_api_key = (config_manager.APP_CONFIG or {}).get(getattr(constants, 'CONFIG_OPTION_EMBY_API_KEY', 'emby_api_key'))
    emby_user_id = (config_manager.APP_CONFIG or {}).get(getattr(constants, 'CONFIG_OPTION_EMBY_USER_ID', 'emby_user_id'))
    if not emby_url or not emby_api_key:
        return {'ok': False, 'skipped': True, 'message': 'missing_emby_config', 'emby_item_id': emby_item_id}
    try:
        from handler import emby
        ok = emby.delete_item(emby_item_id, emby_url, emby_api_key, emby_user_id or '')
        return {'ok': bool(ok), 'emby_item_id': emby_item_id}
    except Exception as e:
        logger.warning(f"  ➜ [共享资源维护] 删除过期虚拟 Emby 媒体项失败: item={emby_item_id}, err={e}")
        return {'ok': False, 'emby_item_id': emby_item_id, 'message': str(e)}


def _gap_item(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'tmdb_id': str(row.get('parent_series_tmdb_id') or row.get('tmdb_id') or ''),
        'item_type': row.get('item_type') or '',
        'season_number': row.get('season_number'),
        'episode_number': row.get('episode_number'),
        'title': row.get('title') or None,
        'release_year': row.get('release_year'),
    }


def _report_local_wanted_gaps(client: SharedCenterClient, limit: int = 200) -> int:
    rows = shared_share_db.get_local_wanted_gaps(limit)
    items = [_gap_item(r) for r in rows]
    items = [x for x in items if x.get('tmdb_id') and x.get('item_type')]
    if not items: return 0
    try:
        resp = client.report_gaps(items)
        return int(resp.get('count') or len(items))
    except Exception as e:
        logger.warning(f"  ➜ [共享资源维护] 自动登记缺口失败: {e}")
        return 0


def _parse_share_ok(resp: Dict[str, Any]) -> bool:
    if not isinstance(resp, dict):
        return False
    if resp.get('state') is True or str(resp.get('state')).lower() in ('1', 'true'):
        return True
    if resp.get('errno') in (0, '0') or resp.get('code') in (0, '0', 200, '200'):
        return True
    return False


def _share_resp_text(resp: Any) -> str:
    try:
        return json.dumps(resp, ensure_ascii=False).lower()
    except Exception:
        return str(resp or '').lower()


def _looks_share_blocked(resp: Any) -> bool:
    """115 审核违规/风控类状态。

    这类分享不能当作普通失效处理；普通失效可重建，但违规分享如果被维护任务
    反复重建，就会出现同一资源被自动分享多次。
    """
    text = _share_resp_text(resp)
    return any(k in text for k in (
        '违规', '违法', '侵权', '敏感', '禁止分享', '禁止访问',
        '审核失败', '审核不通过', '分享状态异常',
        'violation', 'illegal', 'blocked', 'forbidden', 'risk'
    ))

def _looks_share_source_missing(resp: Any) -> bool:
    """115 返回源文件/目录已不存在，属于确定性失败，应加入自动分享黑名单。"""
    if isinstance(resp, dict):
        if resp.get('errno') in (4100005, '4100005'):
            return True

    text = _share_resp_text(resp)
    return any(k in text for k in (
        '4100005',
        '分享的文件(夹)已被移动或删除',
        '文件(夹)已被移动或删除',
        '已被移动或删除',
        'file has been moved or deleted',
        'folder has been moved or deleted',
    ))

def _looks_share_alive(resp: Dict[str, Any]) -> bool:
    if not _parse_share_ok(resp):
        return False
    text = _share_resp_text(resp)
    if _looks_share_blocked(resp):
        return False
    return not any(k in text for k in ['已取消', '已失效', '不存在', '取消分享', 'expired', 'cancelled', 'not found'])


def _extract_115_history_items(resp: Any) -> List[Dict[str, Any]]:
    """兼容不同 115 最近接收列表返回结构。"""
    if not isinstance(resp, dict):
        return []
    candidates = []
    data = resp.get('data')
    if isinstance(data, list):
        candidates = data
    elif isinstance(data, dict):
        for key in ('list', 'items', 'data', 'rows'):
            if isinstance(data.get(key), list):
                candidates = data.get(key)
                break
    if not candidates:
        for key in ('list', 'items', 'rows'):
            if isinstance(resp.get(key), list):
                candidates = resp.get(key)
                break
    return [x for x in candidates if isinstance(x, dict)]


def _history_item_id(row: Dict[str, Any]) -> str:
    for key in ('id', 'hid', 'history_id', 'record_id'):
        val = row.get(key)
        if val not in (None, ''):
            return str(val).strip()
    return ''


def _cleanup_recent_receive_history(p115, virtual_rows: List[Dict[str, Any]], max_pages: int = 3, page_size: int = 100) -> int:
    """清理 115 最近接收记录中与已释放虚拟缓存相关的条目。

    注意：history/delete 删除的是“历史记录/最近接收”展示项，不保证解除 115
    后端对 share_import 的 4100024 已转存限制，所以这里是 best-effort。
    """
    if not p115 or not hasattr(p115, 'history_receive_list') or not hasattr(p115, 'history_delete'):
        return 0

    terms = set()
    share_codes = set()
    for row in virtual_rows or []:
        share_code = str(row.get('share_code') or '').strip().lower()
        if share_code:
            share_codes.add(share_code)
            terms.add(share_code)
        raw = row.get('raw_json') if isinstance(row.get('raw_json'), dict) else {}
        for val in (
            row.get('title'),
            row.get('file_name'),
            raw.get('last_import_root_name') if isinstance(raw, dict) else '',
        ):
            val = str(val or '').strip().lower()
            if not val:
                continue
            terms.add(val)
            stem = val.rsplit('.', 1)[0] if '.' in val else val
            if len(stem) >= 4:
                terms.add(stem)

    # 太短的词容易误删历史记录。
    terms = {t for t in terms if len(t) >= 4}
    if not terms:
        return 0

    ids = []
    seen_ids = set()
    for page in range(max(1, int(max_pages or 1))):
        try:
            resp = p115.history_receive_list(offset=page * page_size, limit=page_size)
            items = _extract_115_history_items(resp)
        except Exception as e:
            logger.warning(f"  ➜ [共享资源维护] 查询 115 最近接收记录失败: {e}")
            break
        if not items:
            break
        for item in items:
            hid = _history_item_id(item)
            if not hid or hid in seen_ids:
                continue
            text = json.dumps(item, ensure_ascii=False).lower()
            if any(code and code in text for code in share_codes) or any(term in text for term in terms):
                seen_ids.add(hid)
                ids.append(hid)
        if len(items) < page_size:
            break

    deleted = 0
    for i in range(0, len(ids), 50):
        batch = ids[i:i + 50]
        try:
            resp = p115.history_delete(batch)
            if isinstance(resp, dict) and (resp.get('state') or resp.get('success')):
                deleted += len(batch)
            else:
                logger.warning(f"  ➜ [共享资源维护] 删除 115 最近接收记录未确认成功: {resp}")
        except Exception as e:
            logger.warning(f"  ➜ [共享资源维护] 删除 115 最近接收记录失败: {e}")
    return deleted


def _record_reportable(record: Dict[str, Any]) -> bool:
    return (record.get('status') in ('alive', 'reported') or record.get('review_status') == 'alive') and record.get('center_status') not in ('reported', 'partial')


def _max_active_shares_limit() -> int:
    """本机 115 分享数量上限；0 表示不限制。"""
    return max(0, _safe_int(settings_db.get_shared_resource_config().get('p115_shared_max_active_shares', 0), 0))


def _auto_share_requests_enabled() -> bool:
    """是否自动响应别人发布的求分享。"""
    try:
        cfg = settings_db.get_shared_resource_config() or {}
        return _safe_bool(cfg.get('p115_shared_auto_share_requests_enabled'), False)
    except Exception:
        return False


def _share_low_watermark(max_active_shares: int) -> int:
    """低水位硬编码为上限的 80%，不暴露额外配置。"""
    max_active_shares = max(0, int(max_active_shares or 0))
    if max_active_shares <= 0:
        return 0
    return max(0, int(max_active_shares * 0.8))


def _active_share_statuses() -> List[str]:
    """本地仍应视为占用 115 分享名额的状态。"""
    return shared_share_db.active_share_statuses()


def _invalid_share_statuses() -> List[str]:
    """115 已判定违规/风控或上次删除失败的分享，维护任务应直接删除/重试。"""
    return shared_share_db.invalid_share_statuses()


def _count_active_local_shares() -> int:
    return shared_share_db.count_active_share_records(_active_share_statuses())


def _share_cancel_success(resp: Any) -> bool:
    if isinstance(resp, dict):
        if resp.get('state') is True or str(resp.get('state')).lower() in ('1', 'true'):
            return True
        if resp.get('errno') in (0, '0') or resp.get('code') in (0, '0', 200, '200'):
            return True
    text = _share_resp_text(resp)
    return any(k in text for k in (
        '已取消', '已删除', '取消成功', '删除成功', '不存在', 'not found',
        'cancelled', 'canceled', 'deleted', 'success'
    ))


def _delete_115_share(p115, share_code: str) -> tuple[bool, Any]:
    """删除/取消 115 分享。优先删除分享记录，失败再回退为取消分享。"""
    if not p115:
        return False, '115 客户端未初始化'
    share_code = str(share_code or '').strip()
    if not share_code:
        return True, '缺少 share_code，视为无需删除 115 分享'

    last_resp = None
    for method_name in ('share_delete', 'share_cancel'):
        method = getattr(p115, method_name, None)
        if not callable(method):
            continue
        try:
            last_resp = method(share_code)
            if _share_cancel_success(last_resp):
                return True, last_resp
        except Exception as e:
            last_resp = str(e)
            continue
    return False, last_resp or '当前 115 客户端不支持 share_delete/share_cancel'


def _share_items_identity(record_id: int) -> tuple[List[str], List[str]]:
    items = shared_share_db.list_share_items(record_id) or []
    source_ids = [str(i.get('center_source_id') or '').strip() for i in items if str(i.get('center_source_id') or '').strip()]
    sha1s = [str(i.get('sha1') or '').strip().upper() for i in items if str(i.get('sha1') or '').strip()]
    return source_ids, sha1s


def _cancel_center_sources_for_record(client: SharedCenterClient, record_id: int, share_code: str, reason: str) -> tuple[bool, Any]:
    try:
        source_ids, sha1s = _share_items_identity(record_id)
        resp = client.cancel_sources(
            share_code=share_code,
            source_ids=source_ids,
            sha1_list=sha1s,
            reason=reason,
            delete_raw_ffprobe=True,
        )
        return True, resp
    except Exception as e:
        return False, str(e)




def _center_status_after_cancel_response(center_ok: bool, center_resp: Any) -> str:
    if not center_ok:
        return 'cancel_failed'
    if isinstance(center_resp, dict):
        if int(center_resp.get('replenish_count') or 0) > 0 or str(center_resp.get('status') or '').lower() == 'replenish':
            return 'replenish'
    return 'cancelled'

def _mark_share_deleted(record: Dict[str, Any], *, p115_resp: Any, center_resp: Any,
                        center_ok: bool, reason: str, last_error: str,
                        status: str = 'cancelled', review_status: str = 'cancelled'):
    record_id = record.get('id')
    raw_json = record.get('raw_json') if isinstance(record.get('raw_json'), dict) else {}
    raw_json = dict(raw_json or {})
    raw_json.setdefault('share_maintenance_delete', {})
    raw_json['share_maintenance_delete'].update({
        'reason': reason,
        'p115_response': p115_resp,
        'center_response': center_resp,
        'deleted_at': time.strftime('%Y-%m-%d %H:%M:%S'),
    })
    shared_share_db.update_share_record(
        record_id,
        status=status,
        review_status=review_status,
        center_status=_center_status_after_cancel_response(center_ok, center_resp),
        cancelled_at='NOW()',
        last_error=last_error if center_ok else f'{last_error}；中心撤销失败：{center_resp}',
        raw_json=raw_json,
    )


def _cleanup_invalid_local_shares(client: SharedCenterClient, max_rows: int = 100) -> Dict[str, int]:
    """违规/风控/上次删除失败的分享不参与水位评分，直接删除。"""
    p115 = P115Service.get_client()
    if not p115:
        return {'share_invalid_deleted': 0, 'share_invalid_failed': 0}

    rows = shared_share_db.list_invalid_share_records(
        limit=max_rows,
        invalid_statuses=_invalid_share_statuses(),
        review_statuses=['blocked', 'violation'],
    )

    deleted = failed = 0
    consecutive_errors = 0
    for record in rows:
        record_id = record.get('id')
        share_code = str(record.get('share_code') or '').strip()
        title = record.get('title') or record.get('root_name') or share_code or str(record_id)
        p115_ok, p115_resp = _delete_115_share(p115, share_code)
        if not p115_ok:
            shared_share_db.update_share_record(
                record_id,
                status='cancel_failed',
                last_error=f'违规/风控分享自动删除失败：{p115_resp}',
            )
            failed += 1
            continue

        center_ok, center_resp = _cancel_center_sources_for_record(client, record_id, share_code, 'share_invalid_or_blocked')
        if not center_ok and _is_network_error(center_resp):
            consecutive_errors += 1
            if consecutive_errors >= 3:
                logger.error("  ➜ [共享资源维护] 连续 3 次网络请求失败，触发熔断，提前结束违规清理。")
                break
        else:
            consecutive_errors = 0
        _mark_share_deleted(
            record,
            p115_resp=p115_resp,
            center_resp=center_resp,
            center_ok=center_ok,
            reason='share_invalid_or_blocked',
            last_error='115 返回违规/风控/删除失败重试，维护任务已直接删除分享',
            status='cancelled',
            review_status='violation' if record.get('review_status') == 'violation' or record.get('status') in ('blocked', 'violation') else 'cancelled',
        )
        shared_virtual_db.add_credit_ledger(
            'share_invalid_deleted', 0,
            f'删除违规/风控分享：{title}',
            ref_id=str(record_id),
            title=title,
            raw_json={'share_code': share_code, 'p115_response': p115_resp, 'center_ok': center_ok},
        )
        deleted += 1
        time.sleep(0.25)

    if deleted or failed:
        logger.info(f"  ➜ [共享资源维护] 违规/风控分享清理完成：删除 {deleted}，失败 {failed}。")
    return {'share_invalid_deleted': deleted, 'share_invalid_failed': failed}



def _cleanup_missing_raw_local_shares(client: SharedCenterClient, max_rows: int = 100) -> Dict[str, int]:
    """自清洁：本地分享项缺少 raw_ffprobe_json 的分享直接删除。

    缺 raw 的源无法在中心展示清晰度、编码、音轨、字幕，也不能被可靠消费；
    因此不参与水位评分，发现后直接撤销 115 分享和中心登记。
    """
    p115 = P115Service.get_client()
    if not p115:
        return {'share_raw_missing_deleted': 0, 'share_raw_missing_failed': 0}

    try:
        from routes import shared_resource as sr
    except Exception as e:
        logger.warning(f"  ➜ [共享资源维护] 无法加载 raw_ffprobe 检查辅助函数，跳过缺 raw 自清洁: {e}")
        return {'share_raw_missing_deleted': 0, 'share_raw_missing_failed': 0}

    rows = shared_share_db.list_active_share_records(
        limit=max_rows,
        statuses=_active_share_statuses(),
        order_by='created_asc',
    )

    deleted = failed = 0
    consecutive_errors = 0
    for record in rows:
        record_id = record.get('id')
        share_code = str(record.get('share_code') or '').strip()
        title = record.get('title') or record.get('root_name') or share_code or str(record_id)
        try:
            items = shared_share_db.list_share_items(record_id) or []
            if not items:
                continue
            missing = sr._files_missing_raw_ffprobe(items)
            if not missing:
                continue

            p115_ok, p115_resp = _delete_115_share(p115, share_code)
            if not p115_ok:
                shared_share_db.update_share_record(
                    record_id,
                    status='cancel_failed',
                    last_error=f'缺少 raw_ffprobe_json 自动清理失败，115 删除/取消分享失败：{p115_resp}',
                    raw_json={**(record.get('raw_json') if isinstance(record.get('raw_json'), dict) else {}), 'missing_raw_ffprobe': missing},
                )
                failed += 1
                continue

            center_ok, center_resp = _cancel_center_sources_for_record(client, record_id, share_code, 'raw_ffprobe_missing')
            if not center_ok and _is_network_error(center_resp):
                consecutive_errors += 1
                if consecutive_errors >= 3:
                    logger.error("  ➜ [共享资源维护] 连续 3 次网络请求失败，触发熔断，提前结束缺 raw 清理。")
                    break
            else:
                consecutive_errors = 0

            _mark_share_deleted(
                record,
                p115_resp=p115_resp,
                center_resp=center_resp,
                center_ok=center_ok,
                reason='raw_ffprobe_missing',
                last_error=f'分享文件缺少 raw_ffprobe_json，维护任务已直接删除分享：{sr._raw_missing_message(missing)}',
                status='cancelled',
                review_status='raw_missing',
            )
            shared_virtual_db.add_credit_ledger(
                'share_raw_missing_deleted', 0,
                f'删除缺少 raw_ffprobe_json 的分享：{title}',
                ref_id=str(record_id),
                title=title,
                raw_json={'share_code': share_code, 'missing_raw': missing, 'center_ok': center_ok},
            )
            deleted += 1
            time.sleep(0.25)
        except Exception as e:
            logger.warning(f"  ➜ [共享资源维护] 缺 raw 分享清理异常: record={record_id}, share={share_code}, err={e}", exc_info=True)
            failed += 1
            if _is_network_error(e):
                consecutive_errors += 1
                if consecutive_errors >= 3:
                    logger.error("  ➜ [共享资源维护] 连续 3 次网络请求失败，触发熔断，提前结束缺 raw 清理。")
                    break

    if deleted or failed:
        logger.info(f"  ➜ [共享资源维护] 缺 raw_ffprobe_json 分享清理完成：删除 {deleted}，失败 {failed}。")
    return {'share_raw_missing_deleted': deleted, 'share_raw_missing_failed': failed}



def _load_share_waterline_candidates(target_active: int) -> tuple[int, List[Dict[str, Any]]]:
    """按“转存热度 + 创建时间保护”综合评分，取出应清理的超额分享。"""
    return shared_share_db.load_share_waterline_candidates(target_active, _active_share_statuses())


def _cleanup_excess_local_shares(client: SharedCenterClient, max_active_shares: int = 0) -> Dict[str, int]:
    """超过上限时删除到低水位；低水位硬编码为上限的 80%。"""
    max_active_shares = max(0, int(max_active_shares or _max_active_shares_limit()))
    if max_active_shares <= 0:
        return {'share_limit': 0, 'share_low_watermark': 0, 'share_active': _count_active_local_shares(), 'share_pruned': 0, 'share_prune_failed': 0}

    low_watermark = _share_low_watermark(max_active_shares)
    total_active = _count_active_local_shares()
    if total_active <= max_active_shares:
        return {'share_limit': max_active_shares, 'share_low_watermark': low_watermark, 'share_active': total_active, 'share_pruned': 0, 'share_prune_failed': 0}

    # 一旦超过高水位，就清到低水位，给后续自动分享留出空间。
    total_active, candidates = _load_share_waterline_candidates(low_watermark)
    if not candidates:
        return {'share_limit': max_active_shares, 'share_low_watermark': low_watermark, 'share_active': total_active, 'share_pruned': 0, 'share_prune_failed': 0}

    p115 = P115Service.get_client()
    pruned = failed = 0
    consecutive_errors = 0
    for record in candidates:
        record_id = record.get('id')
        share_code = str(record.get('share_code') or '').strip()
        title = record.get('title') or record.get('root_name') or share_code or str(record_id)
        served_count = _safe_int(record.get('served_count'), 0)
        age_days = float(record.get('age_days') or 0)
        retention_score = float(record.get('retention_score') or 0)
        try:
            p115_ok, p115_resp = _delete_115_share(p115, share_code)
            if not p115_ok:
                shared_share_db.update_share_record(
                    record_id,
                    status='cancel_failed',
                    last_error=f'超过最大分享数自动清理失败，115 删除/取消分享失败：{p115_resp}',
                )
                failed += 1
                continue

            center_ok, center_resp = _cancel_center_sources_for_record(client, record_id, share_code, 'max_active_shares_waterline')
            if not center_ok and _is_network_error(center_resp):
                consecutive_errors += 1
                if consecutive_errors >= 3:
                    logger.error("  ➜ [共享资源维护] 连续 3 次网络请求失败，触发熔断，提前结束水位清理。")
                    break
            else:
                consecutive_errors = 0
            raw_json = record.get('raw_json') if isinstance(record.get('raw_json'), dict) else {}
            raw_json = dict(raw_json or {})
            raw_json['share_waterline_prune'] = {
                'max_active_shares': max_active_shares,
                'low_watermark': low_watermark,
                'active_before': total_active,
                'served_count': served_count,
                'age_days': round(age_days, 3),
                'retention_score': round(retention_score, 3),
                'p115_response': p115_resp,
                'center_response': center_resp,
                'pruned_at': time.strftime('%Y-%m-%d %H:%M:%S'),
            }
            shared_share_db.update_share_record(
                record_id,
                status='cancelled',
                review_status='cancelled',
                center_status=_center_status_after_cancel_response(center_ok, center_resp),
                cancelled_at='NOW()',
                last_error=(
                    f'活跃分享超过上限 {max_active_shares}，维护任务按 80% 低水位 {low_watermark} 自动清理；'
                    f'转存次数 {served_count}，创建约 {age_days:.1f} 天，保留分 {retention_score:.1f}'
                ) if center_ok else f'115 分享已删除，但中心撤销失败：{center_resp}',
                raw_json=raw_json,
            )
            shared_virtual_db.add_credit_ledger(
                'share_waterline_pruned', 0,
                f'超过分享水位自动清理分享：{title}',
                ref_id=str(record_id),
                title=title,
                raw_json={
                    'share_code': share_code,
                    'max_active_shares': max_active_shares,
                    'low_watermark': low_watermark,
                    'served_count': served_count,
                    'age_days': round(age_days, 3),
                    'retention_score': round(retention_score, 3),
                    'center_ok': center_ok,
                },
            )
            pruned += 1
        except Exception as e:
            logger.warning(f"  ➜ [共享资源维护] 分享水位清理异常: record={record_id}, share={share_code}, err={e}", exc_info=True)
            failed += 1
            if _is_network_error(e):
                consecutive_errors += 1
                if consecutive_errors >= 3:
                    logger.error("  ➜ [共享资源维护] 连续 3 次网络请求失败，触发熔断，提前结束水位清理。")
                    break
        time.sleep(0.25)

    if pruned or failed:
        logger.info(f"  ➜ [共享资源维护] 分享上限 {max_active_shares}，低水位 {low_watermark}，当前 {total_active}，已清理 {pruned}，失败 {failed}。")
    return {
        'share_limit': max_active_shares,
        'share_low_watermark': low_watermark,
        'share_active': total_active,
        'share_pruned': pruned,
        'share_prune_failed': failed,
    }


def _enforce_local_share_waterline(client: SharedCenterClient) -> Dict[str, int]:
    """先直接删违规/风控分享，再在超过高水位时清到 80% 低水位。"""
    result = {
        'share_invalid_deleted': 0,
        'share_invalid_failed': 0,
        'share_raw_missing_deleted': 0,
        'share_raw_missing_failed': 0,
        'share_pruned': 0,
        'share_prune_failed': 0,
        'share_limit': _max_active_shares_limit(),
        'share_low_watermark': _share_low_watermark(_max_active_shares_limit()),
        'share_active': _count_active_local_shares(),
    }
    invalid = _cleanup_invalid_local_shares(client)
    result.update(invalid)
    raw_missing = _cleanup_missing_raw_local_shares(client)
    result.update(raw_missing)
    excess = _cleanup_excess_local_shares(client)
    result.update(excess)
    return result


def _merge_maintenance_counts(total: Dict[str, Any], update: Dict[str, Any]):
    """维护任务里多次执行水位检查时，计数项累加，状态项取最新。"""
    for k, v in (update or {}).items():
        if isinstance(v, (int, float)) and k not in ('share_limit', 'share_low_watermark', 'share_active'):
            total[k] = total.get(k, 0) + v
        else:
            total[k] = v


def _load_active_local_share_code_set() -> set[str]:
    return shared_share_db.get_active_local_share_code_set(_active_share_statuses())


_EXTERNAL_CENTER_SOURCE_PROVIDERS = {'hdhive', 'tg_channel', 'tg_channel_hdhive'}


def _is_external_center_source(item: Dict[str, Any]) -> bool:
    """中心源是否来自外部资源入口。

    影巢 / TG 频道外部分享并不会写入 shared_share_records，
    不能参与“中心源 vs 本地我的分享记录”的孤儿对账；
    否则会把正常外部转存登记误判为 local_record_missing 并撤销。
    """
    provider = str((item or {}).get('source_provider') or '').strip().lower()
    return provider in _EXTERNAL_CENTER_SOURCE_PROVIDERS


def _cleanup_orphan_center_sources(client: SharedCenterClient, page_size: int = 200, max_pages: int = 20) -> Dict[str, int]:
    """对账中心登记源与本地活动分享，自动撤销已经不在本地的中心孤儿源。

    只清理本机真实创建的分享源。影巢、TG 频道这类外部分享源本来就没有
    shared_share_records 本地记录，必须排除，否则会被误撤销。
    """
    local_active_codes = _load_active_local_share_code_set()
    orphan_groups: Dict[str, Dict[str, Any]] = {}
    checked = 0
    skipped_external = 0
    consecutive_errors = 0

    for page in range(max(1, int(max_pages or 1))):
        try:
            resp = client.list_sources(
                status='alive,pending,dead',
                mine_only=True,
                include_raw=False,
                order_by='latest',
                limit=page_size,
                offset=page * page_size,
            )
        except Exception as e:
            logger.warning(f"  ➜ [共享资源维护] 拉取中心自有共享源失败: {e}")
            if _is_network_error(e):
                consecutive_errors += 1
                if consecutive_errors >= 3:
                    logger.error("  ➜ [共享资源维护] 连续 3 次网络请求失败，触发熔断，提前结束中心孤儿源对账。")
            return {
                'center_orphan_checked': checked,
                'center_orphan_skipped_external': skipped_external,
                'center_orphan_cancelled': 0,
                'center_orphan_failed': 1,
            }

        consecutive_errors = 0
        items = resp.get('items') or []
        total = _safe_int(resp.get('total'), len(items))
        if not items:
            break

        for item in items:
            if not bool(item.get('is_mine')):
                continue
            checked += 1

            # 影巢 / TG 频道等外部来源只是在本机消费后登记到中心，
            # 不会生成 shared_share_records，因此不能按本地 share_code 对账清理。
            if _is_external_center_source(item):
                skipped_external += 1
                continue

            share_code = str(item.get('share_code') or '').strip()
            if not share_code or share_code in local_active_codes:
                continue
            group = orphan_groups.setdefault(share_code, {'source_ids': set(), 'sha1_list': set()})
            source_id = str(item.get('source_id') or '').strip()
            sha1 = str(item.get('sha1') or '').strip().upper()
            if source_id:
                group['source_ids'].add(source_id)
            if sha1:
                group['sha1_list'].add(sha1)

        if (page + 1) * page_size >= total:
            break

    cancelled = failed = 0
    for share_code, group in orphan_groups.items():
        try:
            client.cancel_sources(
                share_code=share_code,
                source_ids=sorted(group['source_ids']),
                sha1_list=sorted(group['sha1_list']),
                reason='local_record_missing',
                delete_raw_ffprobe=True,
            )
            cancelled += 1
        except Exception as e:
            failed += 1
            logger.warning(f"  ➜ [共享资源维护] 撤销中心孤儿共享源失败: share={share_code}, err={e}")
        time.sleep(0.2)

    if cancelled or failed or skipped_external:
        logger.info(
            "  ➜ [共享资源维护] 中心孤儿共享源对账完成：检查 %s，跳过外部来源 %s，撤销 %s，失败 %s。",
            checked, skipped_external, cancelled, failed,
        )
    return {
        'center_orphan_checked': checked,
        'center_orphan_skipped_external': skipped_external,
        'center_orphan_cancelled': cancelled,
        'center_orphan_failed': failed,
    }


def _load_center_own_share_snapshot(client: SharedCenterClient, page_size: int = 500, max_pages: int = 10) -> Dict[str, Dict[str, Any]] | None:
    """拉取中心端当前设备共享源快照，用于修复“本地分享可用但中心源被误删”的情况。

    返回 None 表示中心查询失败。调用方不能把 None 当作“中心没有数据”，
    否则网络抖动时会误触发大批量补登。
    """
    snapshot: Dict[str, Dict[str, Any]] = {}
    for page in range(max(1, int(max_pages or 1))):
        try:
            resp = client.list_sources(
                status='alive,pending,replenish,dead,reported,cancelled,expired,rejected',
                mine_only=True,
                include_raw=False,
                order_by='latest',
                limit=page_size,
                offset=page * page_size,
            )
        except TypeError:
            # 兼容旧中心 / 旧客户端：不认识扩展状态时退回已知状态。
            try:
                resp = client.list_sources(
                    status='alive,pending,dead',
                    mine_only=True,
                    include_raw=False,
                    order_by='latest',
                    limit=page_size,
                    offset=page * page_size,
                )
            except Exception as e:
                logger.warning(f"  ➜ [共享资源维护] 拉取中心共享源快照失败: {e}")
                return None
        except Exception as e:
            logger.warning(f"  ➜ [共享资源维护] 拉取中心共享源快照失败: {e}")
            return None

        items = resp.get('items') or []
        total = _safe_int(resp.get('total'), len(items))
        for item in items:
            if not bool(item.get('is_mine')):
                continue
            share_code = str(item.get('share_code') or '').strip()
            if not share_code:
                continue
            group = snapshot.setdefault(share_code, {
                'source_ids': set(),
                'sha1s': set(),
                'healthy_sha1s': set(),
                'summary_missing_sha1s': set(),
                'replenish_sha1s': set(),
                'dead_sha1s': set(),
                'statuses': set(),
            })
            source_id = str(item.get('source_id') or '').strip()
            sha1 = str(item.get('sha1') or '').strip().upper()
            status = str(item.get('status') or '').strip().lower()
            if source_id:
                group['source_ids'].add(source_id)
            if sha1:
                group['sha1s'].add(sha1)
            if status:
                group['statuses'].add(status)
            # 中心源必须同时满足：源状态可消费 + raw_ffprobe 仍存在，才算健康。
            has_raw = bool(item.get('has_raw_ffprobe'))
            object_key = str(item.get('object_key') or '').strip()
            if sha1 and status in ('alive', 'pending', 'reported') and has_raw and object_key:
                group['healthy_sha1s'].add(sha1)
                summary_json = item.get('summary_json')
                if not isinstance(summary_json, dict) or not summary_json:
                    group['summary_missing_sha1s'].add(sha1)
            if sha1 and status == 'replenish':
                group['replenish_sha1s'].add(sha1)
            if sha1 and status in ('dead', 'cancelled', 'expired', 'rejected'):
                group['dead_sha1s'].add(sha1)

        if not items or (page + 1) * page_size >= total:
            break
    return snapshot


def _center_share_sync_reason(center_snapshot: Dict[str, Dict[str, Any]] | None, share_code: str, items: List[Dict[str, Any]]) -> str:
    """判断本地活跃分享是否需要重新同步到中心。"""
    if center_snapshot is None:
        return ''
    share_code = str(share_code or '').strip()
    if not share_code:
        return ''
    local_sha1s = {
        str(item.get('sha1') or '').strip().upper()
        for item in (items or [])
        if str(item.get('sha1') or '').strip()
    }
    center = center_snapshot.get(share_code)
    if not center:
        return 'center_missing'
    if not local_sha1s:
        return ''

    center_sha1s = set(center.get('sha1s') or set())
    healthy_sha1s = set(center.get('healthy_sha1s') or set())
    summary_missing_sha1s = set(center.get('summary_missing_sha1s') or set())
    replenish_sha1s = set(center.get('replenish_sha1s') or set())
    dead_sha1s = set(center.get('dead_sha1s') or set())

    if local_sha1s & replenish_sha1s:
        return 'center_replenish'
    if local_sha1s & dead_sha1s:
        return 'center_dead'
    if local_sha1s - center_sha1s:
        return 'center_missing_items'
    if local_sha1s - healthy_sha1s:
        return 'center_raw_missing'
    if local_sha1s & summary_missing_sha1s:
        return 'center_summary_missing'
    return ''


def _center_share_sync_reason_text(reason: str) -> str:
    return {
        'center_missing': '中心服务器已没有该分享码',
        'center_missing_items': '中心服务器缺少该分享码的部分文件',
        'center_raw_missing': '中心服务器该分享码的媒体信息不完整',
        'center_summary_missing': '中心服务器该分享码缺少轻量媒体摘要',
        'center_replenish': '中心服务器该分享码处于待补充',
        'center_dead': '中心服务器该分享码被标记为失效',
    }.get(str(reason or ''), str(reason or ''))


def _auto_check_and_report_local_shares(client: SharedCenterClient, max_records: int = 80) -> Dict[str, int]:
    """自动同步 115 分享状态；可用后上传 raw 并登记中心；失效时撤销中心源。

    增加中心反向对账：如果 115 本地分享仍可用，但中心端 share_code 已缺失、缺文件、
    raw 缺失或被标记 dead，则自动重新上传 raw 并登记中心，修复旧版本误删中心源造成的断档。
    """
    p115 = P115Service.get_client()
    if not p115:
        logger.warning("  ➜ [共享资源维护] 115 客户端未初始化，跳过分享状态同步。")
        return {'checked': 0, 'reported': 0, 'cancelled': 0, 'resynced': 0}

    records, _ = shared_share_db.list_share_records(status='all', keyword='', page=1, page_size=max_records)
    checked = reported = cancelled = resynced = 0
    consecutive_errors = 0

    # 先拉取一次中心端“我的共享源”快照，后面逐个本地分享对账，避免每条分享都请求中心。
    center_snapshot = _load_center_own_share_snapshot(client)

    # 延迟导入 routes.shared_resource，复用现有检查/上传/登记逻辑，避免两套实现分叉。
    try:
        from routes import shared_resource as sr
    except Exception as e:
        logger.warning(f"  ➜ [共享资源维护] 无法加载共享资源路由辅助函数: {e}")
        sr = None

    for record in records:
        status = str(record.get('status') or '')
        if status in ('cancelled', 'deleted', 'dead', 'blocked', 'violation'):
            continue
        share_code = str(record.get('share_code') or '').strip()
        if not share_code:
            continue
        try:
            snap = p115.share_info(share_code, record.get('receive_code'), cid=0, limit=1)
            checked += 1
            review = {}
            if sr is not None and hasattr(sr, '_parse_share_status'):
                try:
                    review = sr._parse_share_status(snap) or {}
                except Exception:
                    review = {}
            alive = (review.get('status') == 'alive') or _looks_share_alive(snap)
            if not alive and review.get('status') == 'pending_review':
                # 115 审核中不等于死链。维护任务只更新本地状态，不能撤销中心源/删除分享。
                old_raw_json = record.get('raw_json') if isinstance(record.get('raw_json'), dict) else {}
                shared_share_db.update_share_record(
                    record['id'],
                    status='pending_review',
                    review_status='pending_review',
                    last_checked_at='NOW()',
                    last_error=review.get('message') or '115 分享仍在审核中，等待下次维护任务复查',
                    raw_json={**old_raw_json, 'last_snap': snap},
                )
                continue
            if alive:
                old_raw_json = record.get('raw_json') if isinstance(record.get('raw_json'), dict) else {}
                update = {
                    'status': 'alive',
                    'review_status': 'alive',
                    'last_checked_at': 'NOW()',
                    # 正常可用不再写入 last_error，避免前端“备注”误把成功状态当原因。
                    'last_error': '',
                    # 保留 auto_gap / manual_payload 等来源标记，只追加 last_snap。
                    'raw_json': {**old_raw_json, 'last_snap': snap},
                }
                shared_share_db.update_share_record(record['id'], **update)
                record = shared_share_db.get_share_record(record['id']) or record
                items = shared_share_db.list_share_items(record['id']) or []
                sync_reason = _center_share_sync_reason(center_snapshot, share_code, items)
                need_report = _record_reportable(record) or bool(sync_reason)

                if need_report and sr is not None:
                    if sync_reason:
                        reason_text = _center_share_sync_reason_text(sync_reason)
                        logger.info(f"  ➜ [共享资源维护] 本地分享仍可用但中心登记异常，准备重新登记: share={share_code}, reason={reason_text}")
                        shared_share_db.update_share_record(
                            record['id'],
                            center_status='not_reported',
                            last_error=f'{reason_text}，维护任务将重新登记中心',
                        )
                        # 中心源如果是 dead，register_source 当前不会把 dead 自动拉回 pending；
                        # 先删除中心旧源但保留 raw，再重新登记，避免继续不可见。
                        if sync_reason == 'center_dead':
                            try:
                                sha1s = [str(i.get('sha1') or '').strip().upper() for i in items if str(i.get('sha1') or '').strip()]
                                client.cancel_sources(
                                    share_code=share_code,
                                    sha1_list=sha1s,
                                    reason='local_alive_center_resync',
                                    delete_raw_ffprobe=False,
                                )
                            except Exception as e:
                                logger.debug(f"  ➜ [共享资源维护] 删除中心 dead 源失败，继续尝试重新登记: share={share_code}, err={e}")

                    # 自动补 RAW。新版上传逻辑会同步生成 summary_json，维护任务也能顺手回填旧中心数据缺失的轻量媒体摘要。
                    try:
                        cfg, headers = sr._center_headers()
                        sr._upload_share_raw_ffprobe_to_center(record['id'], cfg, headers, force=True)
                    except Exception as e:
                        logger.debug(f"  ➜ [共享资源维护] 自动上传 raw 失败，继续尝试登记中心: {e}")
                    try:
                        # 直接复用 route 的核心注册逻辑不方便调用带 Flask request 的视图，这里手动按 shared_share_items 注册。
                        items = shared_share_db.list_share_items(record['id']) or []
                        missing_raw = sr._files_missing_raw_ffprobe(items) if sr is not None and hasattr(sr, '_files_missing_raw_ffprobe') else []
                        not_uploaded = [i for i in items if str(i.get('sha1') or '').strip() and not i.get('raw_ffprobe_uploaded')]
                        if missing_raw or not_uploaded:
                            shared_share_db.update_share_record(
                                record['id'],
                                center_status='failed',
                                last_error=(sr._raw_missing_message(missing_raw) if missing_raw and hasattr(sr, '_raw_missing_message') else '存在 raw_ffprobe_json 未上传的分享项，禁止自动登记中心'),
                            )
                            continue
                        record_share_type_for_check = str(record.get('share_type') or '').strip().lower()
                        if record_share_type_for_check in ('season_pack', 'series_pack', 'season', 'tv_pack') and hasattr(sr, '_validate_season_pack_consistency'):
                            consistency = sr._validate_season_pack_consistency(items, record)
                            if not consistency.get('ok'):
                                shared_share_db.update_share_record(
                                    record['id'],
                                    center_status='failed',
                                    last_error=consistency.get('message') or '季包媒体参数不一致，禁止自动登记中心',
                                )
                                shared_virtual_db.add_credit_ledger(
                                    'share_season_pack_inconsistent_blocked', 0,
                                    '季包分辨率或 HDR/杜比不一致，已阻止自动登记中心',
                                    ref_id=str(record['id']), title=record.get('title') or '',
                                    raw_json={'season_pack_consistency': consistency},
                                )
                                continue
                        raw_meta = record.get('raw_json') if isinstance(record.get('raw_json'), dict) else {}
                        if raw_meta.get('auto_backup_share') or raw_meta.get('backup_share') or raw_meta.get('backup_mirror') or raw_meta.get('backup_instruction'):
                            source_provider = 'backup_mirror'
                        elif raw_meta.get('auto_replenish') or raw_meta.get('replenish_share') or raw_meta.get('replenish_payload'):
                            source_provider = 'replenish_share'
                        elif raw_meta.get('auto_share_request') or raw_meta.get('share_request_group_id') or raw_meta.get('share_request_payload'):
                            source_provider = 'request_share'
                        elif raw_meta.get('auto_gap'):
                            source_provider = 'auto_gap_share'
                        else:
                            source_provider = 'user_share'
                        if sr is not None and hasattr(sr, '_register_share_items_to_center'):
                            cfg, headers = sr._center_headers()
                            register_result = sr._register_share_items_to_center(
                                record, items, cfg, headers, source_provider=source_provider
                            )
                            ok = int(register_result.get('reported') or 0)
                            errors = list(register_result.get('errors') or [])
                        else:
                            ok = 0
                            errors = ['共享资源登记辅助函数不可用，无法自动登记中心']

                        if ok > 0:
                            center_status = 'reported' if ok == len(items) and not errors else 'partial'
                            last_error = '自动登记中心成功' if not errors else '；'.join(errors[:5])
                            if sync_reason and not errors:
                                last_error = f'中心同步补登成功：{_center_share_sync_reason_text(sync_reason)}'
                            shared_share_db.update_share_record(
                                record['id'],
                                center_status=center_status,
                                status='reported' if center_status == 'reported' else record.get('status'),
                                reported_count=ok,
                                reported_at='NOW()',
                                last_error=last_error,
                            )
                            reported += 1
                            if sync_reason:
                                resynced += 1
                                # 更新内存快照，避免同一个分享码后续同轮被误判仍缺失。
                                local_sha1s = {str(i.get('sha1') or '').strip().upper() for i in items if str(i.get('sha1') or '').strip()}
                                if center_snapshot is not None:
                                    group = center_snapshot.setdefault(share_code, {
                                        'source_ids': set(),
                                        'sha1s': set(),
                                        'healthy_sha1s': set(),
                                        'summary_missing_sha1s': set(),
                                        'replenish_sha1s': set(),
                                        'dead_sha1s': set(),
                                        'statuses': set(),
                                    })
                                    group['sha1s'].update(local_sha1s)
                                    group['healthy_sha1s'].update(local_sha1s)
                                    group.setdefault('summary_missing_sha1s', set()).difference_update(local_sha1s)
                                    group.setdefault('replenish_sha1s', set()).difference_update(local_sha1s)
                                    group['dead_sha1s'].difference_update(local_sha1s)
                                    group['statuses'].add('pending')
                        elif errors:
                            shared_share_db.update_share_record(
                                record['id'],
                                center_status='failed',
                                last_error='自动登记中心失败：' + '；'.join(errors[:5])
                            )
                    except Exception as e:
                        logger.warning(f"  ➜ [共享资源维护] 自动登记中心过程发生异常: share={share_code}, err={e}")
            else:
                sha1s = [i.get('sha1') for i in (shared_share_db.list_share_items(record['id']) or []) if i.get('sha1')]
                if _looks_share_blocked(snap):
                    # 115 审核违规/风控：直接删除 115 分享并撤销中心源，避免占用分享名额。
                    p115_ok, p115_resp = _delete_115_share(p115, share_code)
                    center_ok = True
                    center_resp = None
                    try:
                        center_resp = client.cancel_sources(share_code=share_code, sha1_list=sha1s, reason='auto_share_violation', delete_raw_ffprobe=True)
                    except Exception as e:
                        center_ok = False
                        center_resp = str(e)
                        logger.debug(f"  ➜ [共享资源维护] 撤销违规中心源失败: {e}")
                    raw_json = record.get('raw_json') if isinstance(record.get('raw_json'), dict) else {}
                    raw_json = dict(raw_json or {})
                    raw_json.update({'last_snap': snap, 'auto_share_blocked': True, 'share_delete_response': p115_resp, 'center_cancel_response': center_resp})
                    if p115_ok:
                        shared_share_db.update_share_record(
                            record['id'],
                            status='cancelled',
                            review_status='violation',
                            center_status=_center_status_after_cancel_response(center_ok, center_resp),
                            last_checked_at='NOW()',
                            cancelled_at='NOW()',
                            last_error='115 审核违规/风控，维护任务已直接删除分享，避免占用分享名额',
                            raw_json=raw_json,
                        )
                    else:
                        shared_share_db.update_share_record(
                            record['id'],
                            status='cancel_failed',
                            review_status='violation',
                            center_status=_center_status_after_cancel_response(center_ok, center_resp),
                            last_checked_at='NOW()',
                            last_error=f'115 审核违规/风控，但自动删除分享失败：{p115_resp}',
                            raw_json=raw_json,
                        )
                    cancelled += 1
                else:
                    # 115 分享已不可用，撤销中心源并本地标记。中心可能按规则转为待补充。
                    center_ok = True
                    center_resp = {}
                    try:
                        center_resp = client.cancel_sources(share_code=share_code, sha1_list=sha1s, reason='auto_share_dead', delete_raw_ffprobe=True)
                    except Exception as e:
                        center_ok = False
                        center_resp = str(e)
                        logger.debug(f"  ➜ [共享资源维护] 撤销中心源失败: {e}")
                    shared_share_db.update_share_record(
                        record['id'],
                        status='dead',
                        review_status='dead',
                        center_status=_center_status_after_cancel_response(center_ok, center_resp),
                        last_checked_at='NOW()',
                        cancelled_at='NOW()',
                        last_error='自动检测到115分享失效，已撤销中心源' if center_ok else f'自动检测到115分享失效，但中心撤销失败：{center_resp}',
                    )
                    cancelled += 1
        except Exception as e:
            logger.warning(f"  ➜ [共享资源维护] 同步分享状态异常: share={share_code}, err={e}")
            if _is_network_error(e):
                consecutive_errors += 1
                if consecutive_errors >= 3:
                    logger.error("  ➜ [共享资源维护] 连续 3 次网络请求失败，触发熔断，提前结束分享状态同步。")
                    break
        time.sleep(0.2)

    return {'checked': checked, 'reported': reported, 'cancelled': cancelled, 'resynced': resynced}


def _find_local_media_for_gap(gap: Dict[str, Any]) -> Dict[str, Any] | None:
    tmdb_id = str(gap.get('tmdb_id') or '').strip()
    item_type = str(gap.get('item_type') or '').strip()
    season = gap.get('season_number')
    episode = gap.get('episode_number')
    if not tmdb_id or not item_type: return None
    return shared_share_db.find_local_media_for_gap(tmdb_id, item_type, season, episode)


def _dedupe_values(*vals) -> List[str]:
    result = []
    seen = set()
    for v in vals:
        if isinstance(v, (list, tuple, set)):
            seq = v
        else:
            seq = (v,)
        for x in seq:
            s = str(x or '').strip()
            if not s or s in seen:
                continue
            seen.add(s)
            result.append(s)
    return result


def _hard_block_review_statuses() -> List[str]:
    """确定性失败才作为自动分享硬黑名单。

    cancelled 本身不等于黑名单：水位清理、用户取消、完结汇总取消都可能是 cancelled，
    这些资源后续仍应允许再次响应中心缺口。只有 115 违规/风控、源文件已不存在、
    自动失败黑名单这类确定性失败，才阻止重复创建。
    """
    return [
        'violation',
        'blocked',
        'share_blocked',
        'source_missing',
        'source_deleted',
        'share_invalid_or_blocked',
    ]


def _has_existing_share_for_gap(gap: Dict[str, Any], candidate: Dict[str, Any] | None = None, files: List[Dict[str, Any]] | None = None) -> bool:
    """判断中心缺口是否已有活动分享，或命中确定性失败黑名单。"""
    # 1) 活动态直接拦截，避免重复创建。cancel_failed 已包含在 _active_share_statuses()。
    if shared_share_db.has_existing_share_for_gap(
        gap or {},
        candidate or {},
        files or [],
        statuses=_active_share_statuses(),
    ):
        return True

    # 2) 非活动态只拦截确定性失败。普通 cancelled/deleted 不再永久拦截。
    return shared_share_db.has_hard_blocked_share_for_gap(
        gap or {},
        candidate or {},
        files or [],
        statuses=['cancelled', 'deleted'],
        review_statuses=_hard_block_review_statuses(),
    )

def _blacklist_auto_gap_candidate(
    gap: Dict[str, Any],
    candidate: Dict[str, Any],
    files: List[Dict[str, Any]],
    *,
    root_fid: str,
    root_name: str,
    share_resp: Any,
    reason: str = 'source_missing',
) -> None:
    """把自动分享失败的候选落成本地黑名单，避免高频维护任务反复重试。"""
    try:
        standard_identity = {
            'tmdb_id': candidate.get('share_tmdb_id') or candidate.get('tmdb_id') or gap.get('tmdb_id'),
            'item_type': candidate.get('share_item_type') or candidate.get('item_type') or gap.get('item_type'),
            'parent_series_tmdb_id': candidate.get('parent_series_tmdb_id') or gap.get('parent_series_tmdb_id'),
            'season_number': candidate.get('season_number', gap.get('season_number')),
            'episode_number': candidate.get('episode_number', gap.get('episode_number')),
            'title': candidate.get('standard_title') or candidate.get('display_title') or candidate.get('title') or root_name,
            'release_year': candidate.get('release_year') or gap.get('release_year'),
        }

        record = shared_share_db.create_share_record({
            # 没有真正 share_code，留空即可；如果你的表对 share_code 有唯一约束，见下面“如果空 share_code 报错”。
            'share_code': f"AUTOFAIL_{standard_identity.get('item_type')}_{standard_identity.get('tmdb_id')}_S{standard_identity.get('season_number') or 0}_E{standard_identity.get('episode_number') or 0}_{root_fid}",
            'receive_code': '',
            'share_url': '',
            'share_type': candidate.get('share_type') or 'auto_gap_failed',
            'root_fid': str(root_fid or ''),
            'root_name': root_name or '',
            'root_is_dir': candidate.get('root_is_dir') is not False,
            'tmdb_id': str(standard_identity.get('tmdb_id') or ''),
            'item_type': standard_identity.get('item_type') or 'Movie',
            'parent_series_tmdb_id': standard_identity.get('parent_series_tmdb_id'),
            'season_number': standard_identity.get('season_number'),
            'episode_number': standard_identity.get('episode_number'),
            'title': standard_identity.get('title') or root_name or '',
            'release_year': standard_identity.get('release_year'),
            'status': 'deleted',
            'review_status': reason,
            'center_status': 'cancelled',
            'last_error': f'自动创建115分享失败，已加入黑名单：{share_resp}',
            'raw_json': {
                'auto_gap_blacklist': True,
                'blacklist_reason': reason,
                'gap': gap,
                'candidate': candidate,
                'root_fid': str(root_fid or ''),
                'root_name': root_name or '',
                'share_create_response': share_resp,
                'blacklisted_at': time.strftime('%Y-%m-%d %H:%M:%S'),
            },
        })

        if files:
            shared_share_db.replace_share_items(record['id'], files)

        shared_virtual_db.add_credit_ledger(
            'share_auto_gap_blacklisted',
            0,
            f"自动分享失败加入黑名单：{standard_identity.get('title') or root_name}",
            ref_id=str(record['id']),
            title=standard_identity.get('title') or root_name or '',
            raw_json={
                'reason': reason,
                'root_fid': str(root_fid or ''),
                'share_response': share_resp,
                'gap': gap,
            },
        )
    except Exception as e:
        logger.warning(
            f"  ➜ [共享资源维护] 自动分享失败候选加入黑名单失败: "
            f"root_fid={root_fid}, root_name={root_name}, err={e}",
            exc_info=True,
        )

def _auto_share_center_open_gaps(client: SharedCenterClient, limit: int = 80) -> int:
    """中心有缺口而本机已入库时，自动创建 115 分享。可用后由下一轮维护自动登记中心。

    注意：Season 缺口不能直接 _build_media_candidate 后放弃。
    _expand_share_candidates 会复用手动分享的策略：Completed 且齐集才给 season_pack；
    未完结/未齐集季会展开为已有 Episode，逐集自动分享。
    """
    try:
        gaps = (client.list_open_gaps(limit=limit).get('items') or [])
    except Exception as e:
        logger.warning(f"  ➜ [共享资源维护] 拉取中心缺口失败: {e}")
        return 0
    if not gaps:
        return 0

    p115 = P115Service.get_client()
    if not p115:
        return 0

    try:
        from routes import shared_resource as sr
    except Exception as e:
        logger.warning(f"  ➜ [共享资源维护] 无法加载分享辅助函数，跳过自动分享缺口: {e}")
        return 0

    created = 0
    for gap in gaps:
        try:
            gap_item_type = str(gap.get('item_type') or '').strip()
            # Movie/Episode 可以先用缺口口径粗略去重；Season 要等展开成候选后逐个去重，
            # 否则同季已有 1 个单集分享就会误判整季无需继续分享。
            if gap_item_type in ('Movie', 'Episode') and _has_existing_share_for_gap(gap):
                continue

            row = _find_local_media_for_gap(gap)
            if not row:
                continue

            if hasattr(sr, '_expand_share_candidates'):
                candidates = sr._expand_share_candidates(row)
            else:
                candidates = [sr._build_media_candidate(row)]

            for candidate in candidates:
                if not candidate.get('resolvable') or not candidate.get('root_fid'):
                    continue

                candidate_gap = {
                    **gap,
                    'tmdb_id': candidate.get('share_tmdb_id') or candidate.get('tmdb_id') or gap.get('tmdb_id'),
                    'item_type': candidate.get('share_item_type') or candidate.get('item_type') or gap.get('item_type'),
                    'season_number': candidate.get('season_number', gap.get('season_number')),
                    'episode_number': candidate.get('episode_number', gap.get('episode_number')),
                }
                if _has_existing_share_for_gap(candidate_gap, candidate=candidate):
                    logger.info(
                        "  ➜ [共享资源维护] 自动分享跳过已有活动分享/硬失败黑名单: %s S%sE%s root=%s",
                        candidate.get('display_title') or candidate.get('title') or candidate_gap.get('tmdb_id'),
                        candidate_gap.get('season_number'),
                        candidate_gap.get('episode_number'),
                        candidate.get('root_fid'),
                    )
                    continue

                # 二次遵守季包策略，避免未完结季整包分享。
                if candidate.get('share_type') == 'season_pack':
                    policy = sr._share_policy_for_media({
                        'item_type': 'Season',
                        'tmdb_id': candidate.get('parent_series_tmdb_id') or candidate.get('tmdb_id'),
                        'parent_series_tmdb_id': candidate.get('parent_series_tmdb_id') or candidate.get('tmdb_id'),
                        'season_number': candidate.get('season_number'),
                    })
                    if not policy.get('allowed'):
                        continue

                root_fid = str(candidate.get('root_fid'))
                standard_identity = sr._standard_media_identity_for_share({
                    'tmdb_id': candidate.get('share_tmdb_id') or candidate.get('tmdb_id'),
                    'item_type': candidate.get('share_item_type') or candidate.get('item_type'),
                    'parent_series_tmdb_id': candidate.get('parent_series_tmdb_id'),
                    'season_number': candidate.get('season_number'),
                    'episode_number': candidate.get('episode_number'),
                    'title': candidate.get('standard_title') or candidate.get('title'),
                    'release_year': candidate.get('release_year'),
                    'share_type': candidate.get('share_type'),
                })
                root_name = candidate.get('root_name') or standard_identity.get('title') or root_fid
                root_is_dir = candidate.get('root_is_dir') is not False

                # 先收集文件并做 sha1 级去重，避免重复创建 115 分享。
                files = sr._collect_files_from_115(p115, root_fid, root_name=root_name, max_depth=6, assume_dir=root_is_dir)
                if not files:
                    payload = {
                        'tmdb_id': candidate.get('share_tmdb_id') or candidate.get('tmdb_id'),
                        'item_type': candidate.get('share_item_type') or candidate.get('item_type'),
                        'parent_series_tmdb_id': candidate.get('parent_series_tmdb_id'),
                        'season_number': candidate.get('season_number'),
                        'episode_number': candidate.get('episode_number'),
                        'title': candidate.get('display_title') or candidate.get('title'),
                        'root_name': root_name,
                    }
                    files = sr._collect_files_from_media_payload(payload)
                for item in files:
                    item.setdefault('tmdb_id', str(candidate.get('share_tmdb_id') or candidate.get('tmdb_id') or ''))
                    share_type_now = str(candidate.get('share_type') or '').strip().lower()
                    if share_type_now == 'episode_file':
                        # 自动补缺展开出来的单集必须强制写 Episode，不能沿用 Season 缺口行。
                        item['item_type'] = 'Episode'
                        if not item.get('episode_number'):
                            item['episode_number'] = candidate.get('episode_number')
                    else:
                        item.setdefault('item_type', 'Episode' if share_type_now in ('season_pack','series_pack') and item.get('episode_number') else candidate.get('share_item_type') or candidate.get('item_type'))
                    item.setdefault('season_number', candidate.get('season_number'))
                    item.setdefault('episode_number', candidate.get('episode_number'))

                if not files:
                    continue
                if hasattr(sr, '_files_missing_raw_ffprobe'):
                    missing_raw = sr._files_missing_raw_ffprobe(files)
                    if missing_raw:
                        logger.info(f"  ➜ [共享资源维护] 自动分享跳过缺 raw_ffprobe_json 的资源：{candidate.get('display_title')} -> {sr._raw_missing_message(missing_raw) if hasattr(sr, '_raw_missing_message') else missing_raw}")
                        continue

                if str(candidate.get('share_type') or '').strip().lower() == 'season_pack' and hasattr(sr, '_validate_season_pack_consistency'):
                    consistency = sr._validate_season_pack_consistency(files, {**candidate, **candidate_gap})
                    if not consistency.get('ok'):
                        logger.info(f"  ➜ [共享资源维护] 自动分享跳过媒体参数不一致的季包：{candidate.get('display_title')} -> {consistency.get('message')}")
                        continue

                if _has_existing_share_for_gap(candidate_gap, candidate=candidate, files=files):
                    logger.info(
                        "  ➜ [共享资源维护] 自动分享跳过已有活动分享/硬失败黑名单: %s S%sE%s root=%s",
                        candidate.get('display_title') or candidate.get('title') or candidate_gap.get('tmdb_id'),
                        candidate_gap.get('season_number'),
                        candidate_gap.get('episode_number'),
                        candidate.get('root_fid'),
                    )
                    continue

                share_resp = p115.share_create([root_fid], share_duration=-1, receive_code=None)
                if not share_resp or not share_resp.get('state'):
                    if _looks_share_source_missing(share_resp):
                        _blacklist_auto_gap_candidate(
                            gap,
                            candidate,
                            files,
                            root_fid=root_fid,
                            root_name=root_name,
                            share_resp=share_resp,
                            reason='source_missing',
                        )
                        logger.warning(
                            f"  ➜ [共享资源维护] 自动创建分享失败且源文件已失效，已加入黑名单: "
                            f"{candidate.get('display_title')} root_fid={root_fid} -> {share_resp}"
                        )
                    elif _looks_share_blocked(share_resp):
                        _blacklist_auto_gap_candidate(
                            gap,
                            candidate,
                            files,
                            root_fid=root_fid,
                            root_name=root_name,
                            share_resp=share_resp,
                            reason='share_blocked',
                        )
                        logger.warning(
                            f"  ➜ [共享资源维护] 自动创建分享失败且疑似违规/风控，已加入黑名单: "
                            f"{candidate.get('display_title')} root_fid={root_fid} -> {share_resp}"
                        )
                    else:
                        logger.warning(
                            f"  ➜ [共享资源维护] 自动创建分享失败: "
                            f"{candidate.get('display_title')} root_fid={root_fid} -> {share_resp}"
                        )
                    continue

                data = share_resp.get('data') or {}
                share_code = data.get('share_code') or share_resp.get('share_code')
                receive_code = data.get('receive_code') or ''
                share_url = data.get('share_url') or (f"https://115.com/s/{share_code}" if share_code else '')

                record = shared_share_db.create_share_record({
                    'share_code': share_code,
                    'receive_code': receive_code,
                    'share_url': share_url,
                    'share_type': candidate.get('share_type') or 'movie_folder',
                    'root_fid': root_fid,
                    'root_name': root_name,
                    'root_is_dir': root_is_dir,
                    'tmdb_id': str(standard_identity.get('tmdb_id') or candidate.get('share_tmdb_id') or candidate.get('tmdb_id') or ''),
                    'item_type': candidate.get('share_item_type') or candidate.get('item_type') or 'Movie',
                    'parent_series_tmdb_id': standard_identity.get('parent_series_tmdb_id') or candidate.get('parent_series_tmdb_id'),
                    'season_number': candidate.get('season_number'),
                    'episode_number': candidate.get('episode_number'),
                    'title': standard_identity.get('title') or candidate.get('standard_title') or candidate.get('title') or root_name,
                    'release_year': standard_identity.get('release_year') or candidate.get('release_year'),
                    'status': 'pending_review',
                    'review_status': 'pending_review',
                    'center_status': 'not_reported',
                    'raw_json': {'auto_gap': gap, 'share_response': share_resp, 'candidate': candidate, 'standard_identity': standard_identity},
                })
                shared_share_db.replace_share_items(record['id'], files)
                shared_virtual_db.add_credit_ledger('share_auto_created_for_gap', 0, '命中中心缺口并自动创建115分享，等待审核', ref_id=str(record['id']), title=record.get('title') or '', raw_json={'gap': gap, 'share_code': share_code})
                created += 1
                time.sleep(0.3)
        except Exception as e:
            logger.warning(f"  ➜ [共享资源维护] 自动分享中心缺口失败: {gap} -> {e}", exc_info=True)
        time.sleep(0.1)
    return created



def _json_dict(value) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            data = json.loads(value)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}
    return {}


def _share_request_target_label(req: Dict[str, Any]) -> str:
    target = str((req or {}).get('target_type') or '').strip().lower()
    media = str((req or {}).get('media_type') or '').strip().lower()
    title = (req or {}).get('title') or (req or {}).get('tmdb_id') or '-'
    season = _safe_int((req or {}).get('season_number'), 0)
    episode = _safe_int((req or {}).get('episode_number'), 0)
    if target == 'episode':
        return f"{title} S{season:02d}E{episode:02d}"
    if target == 'season':
        return f"{title} S{season:02d}"
    if target in ('series', 'tv'):
        return f"{title} 全剧"
    if media == 'movie' or target == 'movie':
        return str(title)
    return str(title)


def _request_filter_from_center_row(req: Dict[str, Any]) -> Dict[str, Any]:
    params = _json_dict((req or {}).get('params_json'))
    price = _json_dict((req or {}).get('price_breakdown'))
    season_count = _safe_int((req or {}).get('season_count'), 0) or _safe_int(price.get('season_count'), 0)
    episode_numbers = (req or {}).get('episode_numbers') or []
    if isinstance(episode_numbers, str):
        try:
            episode_numbers = json.loads(episode_numbers)
        except Exception:
            episode_numbers = []
    if not isinstance(episode_numbers, list):
        episode_numbers = []
    return {
        'tmdb_id': str((req or {}).get('tmdb_id') or '').strip(),
        'media_type': str((req or {}).get('media_type') or '').strip().lower(),
        'target_type': str((req or {}).get('target_type') or '').strip().lower(),
        'season_number': _safe_int((req or {}).get('season_number'), None),
        'season_count': season_count or None,
        'episode_number': _safe_int((req or {}).get('episode_number'), None),
        'episode_numbers': [_safe_int(x, None) for x in episode_numbers if _safe_int(x, None) is not None],
        'params': params,
    }


def _load_seed_media_row_for_share_request(req_filter: Dict[str, Any], sr) -> Dict[str, Any]:
    target = str((req_filter or {}).get('target_type') or '').strip().lower()
    media = str((req_filter or {}).get('media_type') or '').strip().lower()
    tmdb_id = str((req_filter or {}).get('tmdb_id') or '').strip()
    season = req_filter.get('season_number')
    if not tmdb_id: return {}
    if target in ('series', 'tv'):
        try: return sr._load_series_row_for_share_request(req_filter, {}) or {}
        except Exception: return {}
    if target == 'episode':
        try: return sr._load_exact_episode_row_for_share_request(req_filter) or {}
        except Exception: return {}
    try:
        return shared_share_db.get_seed_media_row_for_share_request(target, media, tmdb_id, season)
    except Exception as e:
        logger.debug(f"  ➜ [共享资源维护] 求分享本地种子媒体查询失败: {req_filter} -> {e}")
        return {}


def _candidate_share_type_allowed_for_request(candidate: Dict[str, Any], req_filter: Dict[str, Any]) -> bool:
    target = str((req_filter or {}).get('target_type') or '').strip().lower()
    media = str((req_filter or {}).get('media_type') or '').strip().lower()
    share_type = str((candidate or {}).get('share_type') or '').strip().lower()
    item_type = str((candidate or {}).get('share_item_type') or (candidate or {}).get('item_type') or '').strip()
    if target in ('series', 'tv'):
        return share_type == 'series_pack' or item_type == 'Series'
    if target == 'season':
        return share_type == 'season_pack' or (item_type == 'Season' and (candidate or {}).get('episode_number') in (None, ''))
    if target == 'episode':
        return share_type == 'episode_file' or item_type == 'Episode'
    if media == 'movie' or target == 'movie':
        return item_type == 'Movie' or share_type in ('movie_file', 'movie_folder')
    return True


def _has_existing_share_for_request(req: Dict[str, Any], candidate: Dict[str, Any], files: List[Dict[str, Any]]) -> bool:
    req_filter = _request_filter_from_center_row(req)
    gap_like = {
        'tmdb_id': req_filter.get('tmdb_id'),
        'item_type': 'Movie' if req_filter.get('media_type') == 'movie' else (
            'Series' if req_filter.get('target_type') in ('series', 'tv') else (
                'Season' if req_filter.get('target_type') == 'season' else 'Episode'
            )
        ),
        'season_number': req_filter.get('season_number'),
        'episode_number': req_filter.get('episode_number'),
    }
    if shared_share_db.has_existing_share_for_gap(
        gap_like,
        candidate or {},
        files or [],
        statuses=_active_share_statuses(),
    ):
        return True

    return shared_share_db.has_hard_blocked_share_for_gap(
        gap_like,
        candidate or {},
        files or [],
        statuses=['cancelled', 'deleted'],
        review_statuses=_hard_block_review_statuses(),
    )


def _prepare_request_share_files(sr, p115, candidate: Dict[str, Any], req_filter: Dict[str, Any]) -> tuple[List[Dict[str, Any]], str]:
    root_fid = str((candidate or {}).get('root_fid') or '').strip()
    root_name = (candidate or {}).get('root_name') or (candidate or {}).get('display_title') or root_fid
    root_is_dir = (candidate or {}).get('root_is_dir') is not False
    if not root_fid:
        return [], '候选资源缺少可分享 FID/CID'
    try:
        files = sr._collect_files_from_115(p115, root_fid, root_name=root_name, max_depth=8, assume_dir=root_is_dir)
    except Exception as e:
        logger.debug(f"  ➜ [共享资源维护] 收集求分享候选文件失败，尝试 media_payload 兜底: {e}")
        files = []
    if not files:
        payload = {
            'tmdb_id': candidate.get('share_tmdb_id') or candidate.get('tmdb_id') or req_filter.get('tmdb_id'),
            'item_type': candidate.get('share_item_type') or candidate.get('item_type') or 'Movie',
            'parent_series_tmdb_id': candidate.get('parent_series_tmdb_id') or req_filter.get('tmdb_id'),
            'season_number': candidate.get('season_number') or req_filter.get('season_number'),
            'episode_number': candidate.get('episode_number') or req_filter.get('episode_number'),
            'title': candidate.get('display_title') or candidate.get('title') or root_name,
            'root_name': root_name,
        }
        files = sr._collect_files_from_media_payload(payload)
    if not files:
        return [], '未能定位到可分享的视频文件'

    share_type_now = str(candidate.get('share_type') or '').strip().lower()
    for item in files:
        item.setdefault('tmdb_id', str(candidate.get('share_tmdb_id') or candidate.get('tmdb_id') or req_filter.get('tmdb_id') or ''))
        if share_type_now == 'episode_file':
            item['item_type'] = 'Episode'
            item.setdefault('season_number', req_filter.get('season_number') or candidate.get('season_number'))
            item.setdefault('episode_number', req_filter.get('episode_number') or candidate.get('episode_number'))
        elif share_type_now == 'season_pack':
            item.setdefault('item_type', 'Episode' if item.get('episode_number') else 'Season')
            item.setdefault('season_number', req_filter.get('season_number') or candidate.get('season_number'))
        elif share_type_now == 'series_pack':
            item.setdefault('item_type', 'Episode' if item.get('episode_number') else 'Series')
        else:
            item.setdefault('item_type', candidate.get('share_item_type') or candidate.get('item_type') or 'Movie')

    if hasattr(sr, '_files_missing_raw_ffprobe'):
        missing_raw = sr._files_missing_raw_ffprobe(files)
        if missing_raw:
            return [], sr._raw_missing_message(missing_raw) if hasattr(sr, '_raw_missing_message') else f'缺少 raw_ffprobe_json：{missing_raw}'

    if share_type_now in ('season_pack', 'series_pack') and hasattr(sr, '_validate_season_pack_consistency'):
        consistency = sr._validate_season_pack_consistency(files, {**candidate, **req_filter})
        if not consistency.get('ok'):
            return [], consistency.get('message') or '包内媒体参数不一致'
    return files, ''


def _auto_share_center_share_requests(client: SharedCenterClient, limit: int = 80) -> Dict[str, int]:
    """自动响应别人发布的求分享：拉取中心求分享列表，匹配本地库，命中后创建 115 分享。"""
    result = {'checked': 0, 'matched': 0, 'created': 0, 'skipped': 0, 'failed': 0}
    if not _auto_share_requests_enabled():
        return result
    try:
        reqs = client.list_share_requests(status='open', limit=limit, offset=0).get('items') or []
    except Exception as e:
        logger.warning(f"  ➜ [共享资源维护] 拉取中心求分享列表失败: {e}")
        result['failed'] += 1
        return result
    if not reqs:
        return result
    result['checked'] = len(reqs)

    p115 = P115Service.get_client()
    if not p115:
        result['skipped'] += len(reqs)
        return result
    try:
        from routes import shared_resource as sr
    except Exception as e:
        logger.warning(f"  ➜ [共享资源维护] 无法加载分享辅助函数，跳过自动响应求分享: {e}")
        result['failed'] += len(reqs)
        return result

    for req in reqs:
        try:
            if str(req.get('status') or '').strip().lower() != 'open':
                continue
            # 只自动分享别人所求：自己发起/自己同求的需求均跳过，避免自己给自己发悬赏。
            if req.get('joined_by_me') or str(req.get('my_role') or '').strip().lower() in ('owner', 'co_requester'):
                result['skipped'] += 1
                continue
            req_filter = _request_filter_from_center_row(req)
            if not req_filter.get('tmdb_id'):
                result['skipped'] += 1
                continue
            seed = _load_seed_media_row_for_share_request(req_filter, sr)
            if not seed:
                continue
            candidates = sr._expand_share_candidates_for_share_request(seed, req_filter) if hasattr(sr, '_expand_share_candidates_for_share_request') else sr._expand_share_candidates(seed)
            candidates = [c for c in (candidates or []) if c and c.get('resolvable') and c.get('root_fid')]
            if hasattr(sr, '_filter_candidates_for_share_request'):
                candidates = sr._filter_candidates_for_share_request(candidates, req_filter)
            candidates = [c for c in candidates if _candidate_share_type_allowed_for_request(c, req_filter)]
            if not candidates:
                continue
            result['matched'] += 1

            created_this_request = False
            for candidate in candidates[:3]:
                files, file_error = _prepare_request_share_files(sr, p115, candidate, req_filter)
                if file_error:
                    logger.info(
                        "  ➜ [共享资源维护] 求分享命中但候选不可分享，跳过: %s -> %s",
                        _share_request_target_label(req), file_error,
                    )
                    result['skipped'] += 1
                    continue
                if _has_existing_share_for_request(req, candidate, files):
                    result['skipped'] += 1
                    continue

                root_fid = str(candidate.get('root_fid') or '').strip()
                root_name = candidate.get('root_name') or candidate.get('display_title') or root_fid
                root_is_dir = candidate.get('root_is_dir') is not False
                share_resp = p115.share_create([root_fid], share_duration=-1, receive_code=None)
                if not share_resp or not share_resp.get('state'):
                    logger.warning(f"  ➜ [共享资源维护] 自动响应求分享创建 115 分享失败: {_share_request_target_label(req)} -> {share_resp}")
                    result['failed'] += 1
                    continue
                data = share_resp.get('data') or {}
                share_code = data.get('share_code') or share_resp.get('share_code')
                receive_code = data.get('receive_code') or ''
                share_url = data.get('share_url') or (f"https://115.com/s/{share_code}" if share_code else '')
                standard_identity = sr._standard_media_identity_for_share({
                    'tmdb_id': candidate.get('share_tmdb_id') or candidate.get('tmdb_id') or req_filter.get('tmdb_id'),
                    'item_type': candidate.get('share_item_type') or candidate.get('item_type') or ('Movie' if req_filter.get('media_type') == 'movie' else 'Series'),
                    'parent_series_tmdb_id': candidate.get('parent_series_tmdb_id') or (req_filter.get('tmdb_id') if req_filter.get('media_type') != 'movie' else ''),
                    'season_number': candidate.get('season_number') or req_filter.get('season_number'),
                    'episode_number': candidate.get('episode_number') or req_filter.get('episode_number'),
                    'title': candidate.get('standard_title') or candidate.get('title') or req.get('title') or root_name,
                    'release_year': candidate.get('release_year') or req.get('release_year'),
                    'share_type': candidate.get('share_type'),
                }) if hasattr(sr, '_standard_media_identity_for_share') else {}
                record = shared_share_db.create_share_record({
                    'share_code': share_code,
                    'receive_code': receive_code,
                    'share_url': share_url,
                    'share_type': candidate.get('share_type') or 'movie_file',
                    'root_fid': root_fid,
                    'root_name': root_name,
                    'root_is_dir': root_is_dir,
                    'tmdb_id': str(standard_identity.get('tmdb_id') or candidate.get('share_tmdb_id') or candidate.get('tmdb_id') or req_filter.get('tmdb_id') or ''),
                    'item_type': candidate.get('share_item_type') or candidate.get('item_type') or ('Movie' if req_filter.get('media_type') == 'movie' else 'Series'),
                    'parent_series_tmdb_id': standard_identity.get('parent_series_tmdb_id') or candidate.get('parent_series_tmdb_id'),
                    'season_number': candidate.get('season_number') or req_filter.get('season_number'),
                    'episode_number': candidate.get('episode_number') or req_filter.get('episode_number'),
                    'title': standard_identity.get('title') or candidate.get('standard_title') or candidate.get('title') or req.get('title') or root_name,
                    'release_year': standard_identity.get('release_year') or candidate.get('release_year') or req.get('release_year'),
                    'status': 'pending_review',
                    'review_status': 'pending_review',
                    'center_status': 'not_reported',
                    'raw_json': {
                        'auto_share_request': True,
                        'share_request_group_id': req.get('group_id'),
                        'share_request_payload': req,
                        'share_request_filter': req_filter,
                        'share_response': share_resp,
                        'candidate': candidate,
                        'standard_identity': standard_identity,
                    },
                })
                shared_share_db.replace_share_items(record['id'], files)
                shared_virtual_db.add_credit_ledger(
                    'share_auto_created_for_request',
                    0,
                    f"命中别人求分享并自动创建115分享：{record.get('title') or root_name}",
                    ref_id=str(record['id']),
                    title=record.get('title') or root_name,
                    raw_json={'request': req, 'share_code': share_code},
                )
                logger.info(f"  ➜ [共享资源维护] 命中别人求分享并自动创建分享: {_share_request_target_label(req)} share={share_code}")
                result['created'] += 1
                created_this_request = True
                time.sleep(0.3)
                break
            if not created_this_request:
                # 已命中但可能因为重复/缺 raw/115 创建失败而没有创建成功。
                pass
        except Exception as e:
            logger.warning(f"  ➜ [共享资源维护] 自动响应求分享失败: {req} -> {e}", exc_info=True)
            result['failed'] += 1
        time.sleep(0.1)
    return result


def _replenish_group_is_pack(rows: List[Dict[str, Any]]) -> bool:
    if not rows:
        return False
    if len({str(r.get('sha1') or '').strip().upper() for r in rows if str(r.get('sha1') or '').strip()}) > 1:
        item_type = str(rows[0].get('item_type') or '').strip().lower()
        if item_type in ('season', 'season_pack', 'series', 'series_pack', 'tv', 'show'):
            return True
    item_type = str(rows[0].get('item_type') or '').strip().lower()
    return item_type in ('season', 'season_pack', 'series', 'series_pack', 'tv', 'show') and len(rows) > 1


def _load_center_replenish_groups(client: SharedCenterClient, limit: int = 200, max_pages: int = 5) -> List[Dict[str, Any]]:
    """拉取中心“待补充”队列，并按资源粒度分组。

    普通缺口是 TMDb 级；待补充是 SHA1 / 季包完整 SHA1 集合级。
    待补充只处理 Movie 和 Season/Pack；单集在完结汇总后会被季包替代，不能再补源。
    """
    rows: List[Dict[str, Any]] = []
    for page in range(max(1, int(max_pages or 1))):
        try:
            resp = client.list_sources(
                status='replenish',
                mine_only=False,
                include_raw=False,
                order_by='latest',
                limit=limit,
                offset=page * limit,
            )
        except Exception as e:
            logger.warning(f"  ➜ [共享资源维护] 拉取中心待补充队列失败: {e}")
            break
        items = resp.get('items') or []
        for item in items:
            if str(item.get('status') or '').strip().lower() == 'replenish':
                rows.append(dict(item))
        total = _safe_int(resp.get('total'), len(items))
        if not items or page * limit + len(items) >= total:
            break

    groups: Dict[tuple, Dict[str, Any]] = {}
    for item in rows:
        sha1 = str(item.get('sha1') or '').strip().upper()
        if not sha1:
            continue
        item_type = str(item.get('item_type') or '').strip().lower()
        # 单集不参与待补充。完结季汇总会删除单集分享，历史脏数据不能再被自动补回来。
        if item_type in ('episode', 'episode_file', 'single'):
            continue
        share_code = str(item.get('share_code') or '').strip()
        contributor_id = str(item.get('contributor_id') or '').strip()
        if item_type in ('season', 'season_pack', 'series', 'series_pack', 'tv', 'show') and share_code:
            key = ('pack', contributor_id, share_code, str(item.get('tmdb_id') or ''), str(item.get('season_number') or ''))
        else:
            key = ('single', str(item.get('tmdb_id') or ''), item_type, str(item.get('season_number') or ''), str(item.get('episode_number') or ''), sha1)
        group = groups.setdefault(key, {'kind': key[0], 'items': [], 'sha1s': set(), 'sample': item})
        group['items'].append(item)
        group['sha1s'].add(sha1)

    out = []
    for group in groups.values():
        group['sha1s'] = sorted(group.get('sha1s') or [])
        out.append(group)
    return out


def _find_local_cache_rows_by_sha1s(sha1s: List[str]) -> Dict[str, Dict[str, Any]]:
    sha1s = [str(s or '').strip().upper() for s in (sha1s or []) if str(s or '').strip()]
    sha1s = list(dict.fromkeys(sha1s))
    if not sha1s: return {}
    try:
        rows = shared_share_db.find_local_cache_rows_by_sha1s(sha1s)
    except Exception as e:
        logger.warning(f"  ➜ [共享资源维护] 按 SHA1 查询本地 115 缓存失败: {e}")
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        sha1 = str(row.get('sha1') or '').strip().upper()
        name = str(row.get('name') or '')
        if not sha1 or sha1 in out: continue
        if not os.path.splitext(name.lower())[1] in getattr(__import__('routes.shared_resource', fromlist=['VIDEO_EXTENSIONS']), 'VIDEO_EXTENSIONS', {'.mp4', '.mkv', '.avi', '.ts', '.mov', '.m2ts', '.iso', '.wmv', '.flv'}):
            continue
        out[sha1] = row
    return out


def _share_root_from_cache_rows(cache_rows: List[Dict[str, Any]], sr) -> Dict[str, Any]:
    cache_rows = [r for r in (cache_rows or []) if r]
    if not cache_rows:
        return {}
    if len(cache_rows) == 1:
        row = cache_rows[0]
        return {
            'root_fid': str(row.get('id') or ''),
            'root_name': str(row.get('name') or row.get('id') or ''),
            'root_is_dir': False,
        }

    parent_ids = [str(r.get('parent_id') or '').strip() for r in cache_rows if str(r.get('parent_id') or '').strip()]
    root_id = ''
    if parent_ids:
        chains = []
        for pid in parent_ids:
            try:
                chains.append(sr._ancestor_chain(pid))
            except Exception:
                chains.append([pid])
        if chains:
            for node_id in chains[0]:
                if all(node_id in ch for ch in chains[1:]):
                    root_id = node_id
                    break
        root_id = root_id or parent_ids[0]

    if not root_id:
        return {}
    root_node = {}
    try:
        root_node = sr._get_p115_node(root_id) or {}
    except Exception:
        root_node = {}
    return {
        'root_fid': str(root_id),
        'root_name': str(root_node.get('name') or root_id),
        'root_is_dir': True,
    }


def _replenish_group_to_share_payload(group: Dict[str, Any], local_rows_by_sha1: Dict[str, Dict[str, Any]], sr) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
    sample = dict(group.get('sample') or {})
    center_items = [dict(x) for x in (group.get('items') or [])]
    sha1s = [str(x or '').strip().upper() for x in (group.get('sha1s') or []) if str(x or '').strip()]
    cache_rows = [local_rows_by_sha1.get(sha1) for sha1 in sha1s]
    if not sha1s or any(not r for r in cache_rows):
        return {}, []

    root = _share_root_from_cache_rows(cache_rows, sr)
    if not root.get('root_fid'):
        return {}, []

    by_sha1_center = {str(item.get('sha1') or '').strip().upper(): item for item in center_items}
    is_pack = _replenish_group_is_pack(center_items)
    files = []
    for row in cache_rows:
        sha1 = str(row.get('sha1') or '').strip().upper()
        src = by_sha1_center.get(sha1) or sample
        files.append({
            'fid': str(row.get('id') or ''),
            'sha1': sha1,
            'size': sr._safe_size_bytes(row.get('size')) if hasattr(sr, '_safe_size_bytes') else _safe_int(row.get('size'), 0),
            'file_name': str(row.get('name') or src.get('file_name') or sha1),
            'relative_path': row.get('local_path') or row.get('name') or src.get('file_name') or sha1,
            'tmdb_id': str(src.get('tmdb_id') or sample.get('tmdb_id') or ''),
            'item_type': 'Episode' if (not is_pack and src.get('episode_number') not in (None, '')) else ('Season' if is_pack else (src.get('item_type') or sample.get('item_type') or 'Movie')),
            'season_number': src.get('season_number', sample.get('season_number')),
            'episode_number': None if is_pack else src.get('episode_number', sample.get('episode_number')),
            'raw_json': {'source': 'center_replenish', 'center_source': src, 'cache_row': row},
        })

    item_type = 'Season' if is_pack else (sample.get('item_type') or files[0].get('item_type') or 'Movie')
    share_type = 'season_pack' if is_pack else ('episode_file' if str(item_type).lower() == 'episode' or sample.get('episode_number') not in (None, '') else 'movie_file')
    identity = {}
    try:
        identity = sr._standard_media_identity_for_share({
            'tmdb_id': sample.get('tmdb_id'),
            'item_type': item_type,
            'parent_series_tmdb_id': sample.get('parent_series_tmdb_id'),
            'season_number': sample.get('season_number'),
            'episode_number': None if is_pack else sample.get('episode_number'),
            'title': sample.get('title') or sample.get('file_name'),
            'release_year': sample.get('release_year'),
            'share_type': share_type,
        })
    except Exception:
        identity = {}

    payload = {
        **root,
        'share_type': share_type,
        'item_type': item_type,
        'tmdb_id': str(identity.get('tmdb_id') or sample.get('tmdb_id') or ''),
        'parent_series_tmdb_id': identity.get('parent_series_tmdb_id') or sample.get('parent_series_tmdb_id'),
        'season_number': sample.get('season_number'),
        'episode_number': None if is_pack else sample.get('episode_number'),
        'title': identity.get('title') or sample.get('title') or root.get('root_name') or '',
        'release_year': identity.get('release_year') or sample.get('release_year'),
        'standard_identity': identity,
        'center_items': center_items,
        'center_sha1s': sha1s,
    }
    return payload, files


def _has_existing_share_for_replenish(payload: Dict[str, Any], files: List[Dict[str, Any]]) -> bool:
    gap_like = {
        'tmdb_id': payload.get('tmdb_id'),
        'item_type': payload.get('item_type'),
        'season_number': payload.get('season_number'),
        'episode_number': payload.get('episode_number'),
    }
    candidate = {
        'share_type': payload.get('share_type'),
        'root_fid': payload.get('root_fid'),
        'share_tmdb_id': payload.get('tmdb_id'),
        'share_item_type': payload.get('item_type'),
        'season_number': payload.get('season_number'),
        'episode_number': payload.get('episode_number'),
    }
    # 待补充补源不能被历史取消/删除记录拦截。
    # 场景：用户主动取消原分享 -> 中心转为 replenish -> 本地还留着 cancelled 记录。
    # 如果这里把 cancelled/deleted 也当“已有分享”，维护任务会命中 SHA1 但永远跳过创建新补源分享。
    return shared_share_db.has_existing_share_for_gap(
        gap_like,
        candidate,
        files or [],
        statuses=_active_share_statuses(),
    )


def _auto_share_center_replenish_sources(client: SharedCenterClient, limit: int = 120) -> Dict[str, int]:
    """中心“待补充”资源命中本机精确 SHA1 时，自动创建补源分享。

    这和普通缺口不同：普通缺口按 TMDb 补，待补充必须精确匹配原 SHA1；
    季包必须命中原包内全部 SHA1，不能用代表文件冒充。
    """
    result = {'checked': 0, 'matched': 0, 'created': 0, 'skipped': 0, 'failed': 0}
    groups = _load_center_replenish_groups(client, limit=limit)
    if not groups:
        return result

    result['checked'] = len(groups)
    p115 = P115Service.get_client()
    if not p115:
        result['skipped'] = len(groups)
        return result

    try:
        from routes import shared_resource as sr
    except Exception as e:
        logger.warning(f"  ➜ [共享资源维护] 无法加载分享辅助函数，跳过待补充自动补源: {e}")
        result['failed'] = len(groups)
        return result

    all_sha1s = sorted({sha1 for group in groups for sha1 in (group.get('sha1s') or [])})
    local_rows_by_sha1 = _find_local_cache_rows_by_sha1s(all_sha1s)
    if not local_rows_by_sha1:
        return result

    for group in groups:
        try:
            sha1s = list(group.get('sha1s') or [])
            if not sha1s or any(sha1 not in local_rows_by_sha1 for sha1 in sha1s):
                continue
            result['matched'] += 1

            payload, files = _replenish_group_to_share_payload(group, local_rows_by_sha1, sr)
            if not payload or not files:
                logger.info(
                    "  ➜ [共享资源维护] 待补充命中但无法构造分享载荷，跳过: sha1=%s, group=%s",
                    ','.join(sha1s)[:80],
                    {k: group.get(k) for k in ('kind', 'sha1s')}
                )
                result['skipped'] += 1
                continue

            if hasattr(sr, '_files_missing_raw_ffprobe'):
                missing_raw = sr._files_missing_raw_ffprobe(files)
                if missing_raw:
                    logger.info(
                        f"  ➜ [共享资源维护] 待补充命中但缺 raw_ffprobe_json，跳过: "
                        f"{payload.get('title') or payload.get('root_name')} -> "
                        f"{sr._raw_missing_message(missing_raw) if hasattr(sr, '_raw_missing_message') else missing_raw}"
                    )
                    result['skipped'] += 1
                    continue

            if str(payload.get('share_type') or '').strip().lower() == 'season_pack' and hasattr(sr, '_validate_season_pack_consistency'):
                consistency = sr._validate_season_pack_consistency(files, payload)
                if not consistency.get('ok'):
                    logger.info(f"  ➜ [共享资源维护] 待补充季包媒体参数不一致，跳过: {payload.get('title')} -> {consistency.get('message')}")
                    result['skipped'] += 1
                    continue

            if _has_existing_share_for_replenish(payload, files):
                logger.info(
                    "  ➜ [共享资源维护] 待补充命中但本地已有活跃分享记录，跳过重复创建: %s sha1=%s",
                    payload.get('title') or payload.get('root_name') or '-',
                    ','.join(payload.get('center_sha1s') or [])[:80],
                )
                result['skipped'] += 1
                continue

            share_resp = p115.share_create([payload['root_fid']], share_duration=-1, receive_code=None)
            if not share_resp or not share_resp.get('state'):
                logger.warning(f"  ➜ [共享资源维护] 待补充自动创建分享失败: {payload.get('title') or payload.get('root_name')} -> {share_resp}")
                result['failed'] += 1
                continue

            data = share_resp.get('data') or {}
            share_code = data.get('share_code') or share_resp.get('share_code')
            receive_code = data.get('receive_code') or ''
            share_url = data.get('share_url') or (f"https://115.com/s/{share_code}" if share_code else '')
            identity = payload.get('standard_identity') or {}
            record = shared_share_db.create_share_record({
                'share_code': share_code,
                'receive_code': receive_code,
                'share_url': share_url,
                'share_type': payload.get('share_type') or 'movie_file',
                'root_fid': payload.get('root_fid') or '',
                'root_name': payload.get('root_name') or '',
                'root_is_dir': payload.get('root_is_dir') is not False,
                'tmdb_id': str(payload.get('tmdb_id') or ''),
                'item_type': payload.get('item_type') or 'Movie',
                'parent_series_tmdb_id': payload.get('parent_series_tmdb_id'),
                'season_number': payload.get('season_number'),
                'episode_number': payload.get('episode_number'),
                'title': identity.get('title') or payload.get('title') or payload.get('root_name') or '',
                'release_year': identity.get('release_year') or payload.get('release_year'),
                'status': 'pending_review',
                'review_status': 'pending_review',
                'center_status': 'not_reported',
                'raw_json': {
                    'auto_replenish': True,
                    'replenish_share': True,
                    'replenish_payload': payload,
                    'share_response': share_resp,
                    'standard_identity': identity,
                },
            })
            shared_share_db.replace_share_items(record['id'], files)
            shared_virtual_db.add_credit_ledger(
                'share_auto_created_for_replenish',
                0,
                f"命中中心待补充资源并自动创建115分享：{record.get('title') or payload.get('root_name')}",
                ref_id=str(record['id']),
                title=record.get('title') or payload.get('root_name') or '',
                raw_json={'replenish': payload, 'share_code': share_code},
            )
            logger.info(f"  ➜ [共享资源维护] 命中待补充并自动创建补源分享: {record.get('title') or payload.get('root_name')} sha1={','.join(payload.get('center_sha1s') or [])[:80]}")
            result['created'] += 1
            time.sleep(0.3)
        except Exception as e:
            logger.warning(f"  ➜ [共享资源维护] 处理中心待补充资源失败: {group} -> {e}", exc_info=True)
            result['failed'] += 1
        time.sleep(0.1)
    return result



def _season_rollup_parent_id(record: Dict[str, Any]) -> str:
    """从单集分享记录里取父剧 TMDb ID。"""
    return str((record or {}).get('parent_series_tmdb_id') or (record or {}).get('tmdb_id') or '').strip()


def _active_episode_share_statuses_for_rollup() -> List[str]:
    """完结汇总只处理仍然占用分享名额/中心源的活动单集分享。"""
    return _active_share_statuses()


def _watchlist_auto_resub_ended_enabled() -> bool:
    """读取追剧策略里的完结洗版开关。

    这里复用 watchlist_config.auto_resub_ended：
    - 开启：完结季汇总一致性失败后，下次维护仍然继续尝试；
    - 关闭：一致性失败的季标记为跳过，避免每轮维护重复发起低质量季包汇总。
    """
    try:
        cfg = settings_db.get_setting('watchlist_config') or {}
        if isinstance(cfg, str) and cfg.strip():
            cfg = json.loads(cfg)
        if not isinstance(cfg, dict):
            return False
        return _safe_bool(cfg.get('auto_resub_ended'), False)
    except Exception as e:
        logger.debug(f"  ➜ [共享资源维护] 读取 watchlist_config.auto_resub_ended 失败，按关闭处理: {e}")
        return False


def _is_season_pack_consistency_failure(create_result: Dict[str, Any]) -> bool:
    if not isinstance(create_result, dict):
        return False
    reason = str(create_result.get('reason') or '').strip().lower()
    if reason == 'season_pack_consistency_failed':
        return True
    msg = str(create_result.get('message') or '').lower()
    return '季包一致性校验失败' in msg or '季包媒体参数不一致' in msg


_SEASON_ROLLUP_QUALITY_BLOCKLIST_KEY = 'shared_season_rollup_quality_blocklist'


def _season_rollup_quality_key(parent_series_tmdb_id: str, season_number) -> str:
    try:
        season_text = f"{int(season_number):02d}"
    except Exception:
        season_text = str(season_number or '').strip()
    return f"{str(parent_series_tmdb_id or '').strip()}|S{season_text}"


def _load_season_rollup_quality_blocklist() -> Dict[str, Any]:
    try:
        data = settings_db.get_setting(_SEASON_ROLLUP_QUALITY_BLOCKLIST_KEY) or {}
        if isinstance(data, str) and data.strip():
            data = json.loads(data)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _season_rollup_quality_blocked(parent_series_tmdb_id: str, season_number) -> bool:
    key = _season_rollup_quality_key(parent_series_tmdb_id, season_number)
    if not key or key.startswith('|'):
        return False
    entry = _load_season_rollup_quality_blocklist().get(key)
    return bool(isinstance(entry, dict) and entry.get('blocked'))


def _remember_season_rollup_quality_block(parent_series_tmdb_id: str, season_number, message: str, create_result: Dict[str, Any] | None = None) -> None:
    key = _season_rollup_quality_key(parent_series_tmdb_id, season_number)
    if not key or key.startswith('|'):
        return
    data = _load_season_rollup_quality_blocklist()
    data[key] = {
        'blocked': True,
        'reason': 'season_pack_consistency_failed',
        'message': str(message or '')[:1000],
        'parent_series_tmdb_id': str(parent_series_tmdb_id or '').strip(),
        'season_number': _safe_int(season_number, 0),
        'blocked_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        'retry_when': 'watchlist_config.auto_resub_ended=true',
        'error_meta': (create_result or {}).get('error_meta') or {},
    }
    try:
        settings_db.save_setting(_SEASON_ROLLUP_QUALITY_BLOCKLIST_KEY, data)
    except Exception as e:
        logger.debug(f"  ➜ [共享资源维护] 写入完结季汇总跳过记录失败: {key} -> {e}")


def _mark_season_rollup_quality_blocked(
    episode_records: List[Dict[str, Any]],
    *,
    parent_series_tmdb_id: str,
    season_number,
    message: str,
    create_result: Dict[str, Any] | None = None,
) -> int:
    """一致性不达标时标记本季不再自动汇总。

    不取消现有单集分享，只在 raw_json 上写一个跳过标记。
    以后用户打开 auto_resub_ended 后，维护任务会忽略这个标记重新尝试。
    """
    record_ids = []
    for record in episode_records or []:
        rid = record.get('id')
        if rid is None or rid in record_ids:
            continue
        record_ids.append(rid)
    if not record_ids:
        return 0

    payload = {
        'season_completed_rollup_skip': {
            'blocked': True,
            'reason': 'season_pack_consistency_failed',
            'message': str(message or '')[:1000],
            'parent_series_tmdb_id': str(parent_series_tmdb_id or ''),
            'season_number': _safe_int(season_number, 0),
            'blocked_at': time.strftime('%Y-%m-%d %H:%M:%S'),
            'retry_when': 'watchlist_config.auto_resub_ended=true',
            'error_meta': (create_result or {}).get('error_meta') or {},
        }
    }
    _remember_season_rollup_quality_block(parent_series_tmdb_id, season_number, message, create_result)
    try:
        updated = shared_share_db.mark_season_rollup_skipped_for_records(
            record_ids,
            reason='season_pack_consistency_failed',
            message=str(message or '季包一致性校验失败，已停止自动汇总该季'),
            raw_json_patch=payload,
        )
        return int(updated or 0)
    except Exception as e:
        logger.warning(
            "  ➜ [共享资源维护] 标记完结季汇总跳过失败: parent=%s S%s -> %s",
            parent_series_tmdb_id, season_number, e, exc_info=True,
        )
        return 0


def _load_completed_season_episode_share_groups(limit: int = 30, include_rollup_blocked: bool = False) -> List[Dict[str, Any]]:
    statuses = _active_episode_share_statuses_for_rollup()
    if not statuses: return []
    max_rows = max(50, int(limit or 30) * 20)
    rows = shared_share_db.get_completed_season_episode_share_groups(statuses, max_rows, include_rollup_blocked=include_rollup_blocked)

    groups: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        parent = str(row.get('_season_parent_series_tmdb_id') or row.get('parent_series_tmdb_id') or row.get('tmdb_id') or '').strip()
        season_number = row.get('_season_number') if row.get('_season_number') not in (None, '') else row.get('season_number')
        try: season_number_int = int(season_number)
        except Exception: continue
        if not parent: continue

        key = f'{parent}|{season_number_int}'
        group = groups.setdefault(key, {
            'parent_series_tmdb_id': parent,
            'season_number': season_number_int,
            'season_row': {
                'tmdb_id': str(row.get('_season_tmdb_id') or ''),
                'item_type': 'Season',
                'title': row.get('_season_title'),
                'original_title': row.get('_season_original_title'),
                'parent_series_tmdb_id': parent,
                'season_number': season_number_int,
                'release_year': row.get('_season_release_year'),
                'release_date': row.get('_season_release_date'),
                'last_air_date': row.get('_season_last_air_date'),
                'file_sha1_json': row.get('_season_file_sha1_json'),
                'file_pickcode_json': row.get('_season_file_pickcode_json'),
                'in_library': row.get('_season_in_library'),
                'subscription_status': row.get('_season_subscription_status'),
                'total_episodes': row.get('_season_total_episodes'),
                'watching_status': row.get('_season_watching_status'),
                'watchlist_tmdb_status': row.get('_season_watchlist_tmdb_status'),
            },
            'episode_records': [],
        })

        record = {k: v for k, v in row.items() if not str(k).startswith('_season_')}
        if not any(str(x.get('id')) == str(record.get('id')) for x in group['episode_records']):
            group['episode_records'].append(record)

    result_groups = list(groups.values())
    if not include_rollup_blocked:
        result_groups = [
            g for g in result_groups
            if not _season_rollup_quality_blocked(g.get('parent_series_tmdb_id'), g.get('season_number'))
        ]
    return result_groups[:max(1, int(limit or 30))]


def _has_active_season_pack_share(parent_series_tmdb_id: str, season_number) -> bool:
    parent_series_tmdb_id = str(parent_series_tmdb_id or '').strip()
    if not parent_series_tmdb_id or season_number in (None, ''): return False
    try: season_number = int(season_number)
    except Exception: return False
    return shared_share_db.check_active_season_pack_share(parent_series_tmdb_id, season_number, _active_share_statuses())


def _select_season_pack_candidate(sr, season_row: Dict[str, Any]) -> Dict[str, Any]:
    """复用手动分享候选构造，只接受真正的 season_pack 候选。"""
    candidates = []
    if hasattr(sr, '_expand_share_candidates'):
        candidates = sr._expand_share_candidates(season_row) or []
    elif hasattr(sr, '_build_media_candidate'):
        candidates = [sr._build_media_candidate(season_row)]

    for candidate in candidates:
        if str(candidate.get('share_type') or '').strip().lower() == 'season_pack' and candidate.get('resolvable') and candidate.get('root_fid'):
            return candidate
    return {}


def _prepare_season_pack_files(sr, p115, candidate: Dict[str, Any], standard_identity: Dict[str, Any]) -> tuple[List[Dict[str, Any]], str, Dict[str, Any]]:
    """收集季包文件，并做 raw / 一致性校验。"""
    root_fid = str(candidate.get('root_fid') or '').strip()
    root_name = candidate.get('root_name') or standard_identity.get('title') or root_fid
    root_is_dir = candidate.get('root_is_dir') is not False

    files = sr._collect_files_from_115(p115, root_fid, root_name=root_name, max_depth=8, assume_dir=root_is_dir)
    if not files:
        payload = {
            'tmdb_id': candidate.get('share_tmdb_id') or candidate.get('tmdb_id'),
            'item_type': candidate.get('share_item_type') or candidate.get('item_type') or 'Season',
            'parent_series_tmdb_id': candidate.get('parent_series_tmdb_id'),
            'season_number': candidate.get('season_number'),
            'title': candidate.get('display_title') or candidate.get('title') or standard_identity.get('title'),
            'root_name': root_name,
        }
        files = sr._collect_files_from_media_payload(payload)

    for item in files or []:
        item.setdefault('tmdb_id', str(candidate.get('share_tmdb_id') or candidate.get('tmdb_id') or standard_identity.get('tmdb_id') or ''))
        # 季包里的每个文件仍保留 Episode 明细，中心登记时会由 record.share_type=season_pack 聚合成季包源。
        if not item.get('item_type'):
            item['item_type'] = 'Episode' if item.get('episode_number') else 'Season'
        item.setdefault('season_number', candidate.get('season_number'))
        if not item.get('episode_number') and candidate.get('episode_number'):
            item['episode_number'] = candidate.get('episode_number')

    if not files:
        return [], '未能定位到可分享的视频文件，跳过完结季汇总', {'reason': 'season_pack_files_missing'}

    if hasattr(sr, '_files_missing_raw_ffprobe'):
        missing_raw = sr._files_missing_raw_ffprobe(files)
        if missing_raw:
            if hasattr(sr, '_raw_missing_message'):
                return [], sr._raw_missing_message(missing_raw), {'reason': 'missing_raw_ffprobe', 'missing_raw': missing_raw}
            return [], f'缺少 raw_ffprobe_json：{missing_raw}', {'reason': 'missing_raw_ffprobe', 'missing_raw': missing_raw}

    if hasattr(sr, '_validate_season_pack_consistency'):
        consistency = sr._validate_season_pack_consistency(files, {**candidate, **standard_identity})
        if not consistency.get('ok'):
            return [], consistency.get('message') or '季包媒体参数不一致，跳过完结季汇总', {'reason': 'season_pack_consistency_failed', 'consistency': consistency}

    return files, '', {}


def _create_completed_season_pack_share(
    client: SharedCenterClient,
    p115,
    season_row: Dict[str, Any],
    episode_records: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """创建完结季季包分享；成功后返回新分享记录。"""
    try:
        from routes import shared_resource as sr
    except Exception as e:
        return {'ok': False, 'message': f'无法加载共享资源辅助函数：{e}'}

    if hasattr(sr, '_share_policy_for_media'):
        policy = sr._share_policy_for_media(season_row)
        if not policy.get('allowed') or str(policy.get('share_type') or '').lower() != 'season_pack':
            return {'ok': False, 'message': policy.get('message') or '当前季不符合季包分享策略'}

    candidate = _select_season_pack_candidate(sr, season_row)
    if not candidate:
        return {'ok': False, 'message': '未找到可创建季包的分享根目录'}

    standard_identity = sr._standard_media_identity_for_share({
        'tmdb_id': candidate.get('share_tmdb_id') or candidate.get('tmdb_id') or season_row.get('parent_series_tmdb_id') or season_row.get('tmdb_id'),
        'item_type': 'Season',
        'parent_series_tmdb_id': candidate.get('parent_series_tmdb_id') or season_row.get('parent_series_tmdb_id'),
        'season_number': candidate.get('season_number') or season_row.get('season_number'),
        'title': candidate.get('standard_title') or candidate.get('title') or season_row.get('title'),
        'release_year': candidate.get('release_year') or season_row.get('release_year'),
        'share_type': 'season_pack',
    })

    files, file_error, file_error_meta = _prepare_season_pack_files(sr, p115, candidate, standard_identity)
    if file_error:
        return {
            'ok': False,
            'message': file_error,
            'reason': file_error_meta.get('reason') or 'season_pack_file_error',
            'error_meta': file_error_meta,
        }

    root_fid = str(candidate.get('root_fid') or '').strip()
    root_name = candidate.get('root_name') or standard_identity.get('title') or root_fid
    root_is_dir = candidate.get('root_is_dir') is not False

    share_resp = p115.share_create([root_fid], share_duration=-1, receive_code=None)
    if not share_resp or not share_resp.get('state'):
        return {'ok': False, 'message': f'创建完结季季包分享失败：{share_resp}', 'share_response': share_resp}

    data = share_resp.get('data') or {}
    share_code = data.get('share_code') or share_resp.get('share_code')
    receive_code = data.get('receive_code') or ''
    share_url = data.get('share_url') or (f"https://115.com/s/{share_code}" if share_code else '')

    source_record_ids = [r.get('id') for r in episode_records if r.get('id') is not None]
    season_number = candidate.get('season_number') or season_row.get('season_number')
    parent_series_id = standard_identity.get('parent_series_tmdb_id') or candidate.get('parent_series_tmdb_id') or season_row.get('parent_series_tmdb_id')

    record = shared_share_db.create_share_record({
        'share_code': share_code,
        'receive_code': receive_code,
        'share_url': share_url,
        'share_type': 'season_pack',
        'root_fid': root_fid,
        'root_name': root_name,
        'root_is_dir': root_is_dir,
        'tmdb_id': str(standard_identity.get('tmdb_id') or parent_series_id or ''),
        'item_type': 'Season',
        'parent_series_tmdb_id': parent_series_id,
        'season_number': season_number,
        'episode_number': None,
        'title': standard_identity.get('title') or candidate.get('standard_title') or candidate.get('title') or root_name,
        'release_year': standard_identity.get('release_year') or candidate.get('release_year') or season_row.get('release_year'),
        'status': 'pending_review',
        'review_status': 'pending_review',
        'center_status': 'not_reported',
        # 继续写 auto_gap，复用现有“自动分享”识别与中心 source_provider=auto_gap_share 逻辑。
        'raw_json': {
            'auto_gap': {
                'type': 'season_completed_rollup',
                'parent_series_tmdb_id': parent_series_id,
                'season_number': season_number,
                'source_record_ids': source_record_ids,
                'reason': 'Season.watching_status=Completed',
            },
            'auto_completed_season_pack': True,
            'season_completed_rollup': {
                'source_record_ids': source_record_ids,
                'source_share_codes': [r.get('share_code') for r in episode_records if r.get('share_code')],
                'created_at': time.strftime('%Y-%m-%d %H:%M:%S'),
            },
            'share_response': share_resp,
            'candidate': candidate,
            'standard_identity': standard_identity,
        },
    })
    count = shared_share_db.replace_share_items(record['id'], files)
    record = shared_share_db.update_share_record(record['id'], item_count=count) or record

    shared_virtual_db.add_credit_ledger(
        'share_completed_season_pack_created',
        0,
        f"完结季汇总创建季包分享：{record.get('title') or root_name} S{_safe_int(season_number, 0):02d}",
        ref_id=str(record.get('id')),
        title=record.get('title') or root_name,
        raw_json={
            'share_code': share_code,
            'source_record_ids': source_record_ids,
            'parent_series_tmdb_id': parent_series_id,
            'season_number': season_number,
            'item_count': count,
        },
    )

    return {'ok': True, 'record': record, 'items': files, 'share_response': share_resp}


def _cancel_episode_records_after_season_rollup(
    client: SharedCenterClient,
    p115,
    episode_records: List[Dict[str, Any]],
    *,
    new_pack_record: Dict[str, Any] | None = None,
    reason: str = 'season_completed_rollup',
) -> Dict[str, int]:
    """取消同季单集分享，并撤销它们在中心的源。"""
    cancelled = 0
    failed = 0
    new_pack_id = str((new_pack_record or {}).get('id') or '')
    new_pack_share_code = str((new_pack_record or {}).get('share_code') or '').strip()

    seen_ids = set()
    for record in episode_records or []:
        record_id = record.get('id')
        if record_id in seen_ids:
            continue
        seen_ids.add(record_id)
        if new_pack_id and str(record_id) == new_pack_id:
            continue

        share_code = str(record.get('share_code') or '').strip()
        title = record.get('title') or record.get('root_name') or share_code or str(record_id)
        p115_ok, p115_resp = _delete_115_share(p115, share_code)
        if not p115_ok:
            old_raw = record.get('raw_json') if isinstance(record.get('raw_json'), dict) else {}
            shared_share_db.update_share_record(
                record_id,
                status='cancel_failed',
                last_error=f'完结季已汇总为季包，但取消单集分享失败：{p115_resp}',
                raw_json={
                    **dict(old_raw or {}),
                    'season_completed_rollup_cancel_failed': {
                        'reason': reason,
                        'new_pack_record_id': (new_pack_record or {}).get('id'),
                        'new_pack_share_code': new_pack_share_code,
                        'p115_response': p115_resp,
                        'failed_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                    },
                },
            )
            failed += 1
            continue

        center_ok, center_resp = _cancel_center_sources_for_record(client, record_id, share_code, reason)
        _mark_share_deleted(
            record,
            p115_resp=p115_resp,
            center_resp=center_resp,
            center_ok=center_ok,
            reason=reason,
            last_error=(
                f"所属季已完结并汇总为季包分享"
                f"{f'（新分享码 {new_pack_share_code}）' if new_pack_share_code else ''}，维护任务已取消该单集分享"
            ),
            status='cancelled',
            review_status='cancelled',
        )
        shared_virtual_db.add_credit_ledger(
            'share_episode_cancelled_after_season_rollup',
            0,
            f'完结季汇总后取消单集分享：{title}',
            ref_id=str(record_id),
            title=title,
            raw_json={
                'share_code': share_code,
                'new_pack_record_id': (new_pack_record or {}).get('id'),
                'new_pack_share_code': new_pack_share_code,
                'center_ok': center_ok,
                'center_response': center_resp,
            },
        )
        cancelled += 1
        time.sleep(0.2)

    return {'cancelled': cancelled, 'failed': failed}


def _rollup_completed_season_episode_shares(client: SharedCenterClient, max_groups: int = 20) -> Dict[str, int]:
    """完结汇总：Season.watching_status=Completed 后，用季包替换同季单集分享。"""
    result = {
        'season_rollup_groups': 0,
        'season_rollup_created': 0,
        'season_rollup_existing_pack': 0,
        'season_rollup_cancelled': 0,
        'season_rollup_failed': 0,
        'season_rollup_skipped': 0,
        'season_rollup_quality_blocked': 0,
    }

    auto_resub_ended = _watchlist_auto_resub_ended_enabled()

    p115 = P115Service.get_client()
    if not p115:
        result['season_rollup_skipped'] += 1
        return result

    groups = _load_completed_season_episode_share_groups(
        limit=max_groups,
        include_rollup_blocked=auto_resub_ended,
    )
    if not groups:
        return result

    for group in groups:
        parent = group.get('parent_series_tmdb_id')
        season_number = group.get('season_number')
        episode_records = group.get('episode_records') or []
        season_row = group.get('season_row') or {}
        if not parent or season_number in (None, '') or not episode_records:
            result['season_rollup_skipped'] += 1
            continue

        result['season_rollup_groups'] += 1
        try:
            if _has_active_season_pack_share(parent, season_number):
                cancel_result = _cancel_episode_records_after_season_rollup(
                    client,
                    p115,
                    episode_records,
                    new_pack_record=None,
                    reason='season_completed_rollup_existing_pack',
                )
                result['season_rollup_existing_pack'] += 1
                result['season_rollup_cancelled'] += cancel_result.get('cancelled', 0)
                result['season_rollup_failed'] += cancel_result.get('failed', 0)
                logger.info(
                    "  ➜ [共享资源维护] 已存在完结季季包，已清理同季单集分享: parent=%s S%02d, cancelled=%s, failed=%s",
                    parent, _safe_int(season_number, 0), cancel_result.get('cancelled', 0), cancel_result.get('failed', 0),
                )
                continue

            create_result = _create_completed_season_pack_share(client, p115, season_row, episode_records)
            if not create_result.get('ok'):
                result['season_rollup_skipped'] += 1
                msg = create_result.get('message') or 'unknown'
                if _is_season_pack_consistency_failure(create_result):
                    if auto_resub_ended:
                        logger.info(
                            "  ➜ [共享资源维护] 完结季汇总一致性不通过，下次维护继续尝试: parent=%s S%02d -> %s",
                            parent, _safe_int(season_number, 0), msg,
                        )
                    else:
                        blocked = _mark_season_rollup_quality_blocked(
                            episode_records,
                            parent_series_tmdb_id=parent,
                            season_number=season_number,
                            message=msg,
                            create_result=create_result,
                        )
                        result['season_rollup_quality_blocked'] += blocked
                        logger.info(
                            "  ➜ [共享资源维护] 完结季汇总一致性不通过，已停止该季后续自动汇总: parent=%s S%02d, marked=%s -> %s",
                            parent, _safe_int(season_number, 0), blocked, msg,
                        )
                    continue

                logger.info(
                    "  ➜ [共享资源维护] 完结季汇总跳过: parent=%s S%02d -> %s",
                    parent, _safe_int(season_number, 0), msg,
                )
                continue

            new_record = create_result.get('record') or {}
            result['season_rollup_created'] += 1
            cancel_result = _cancel_episode_records_after_season_rollup(
                client,
                p115,
                episode_records,
                new_pack_record=new_record,
                reason='season_completed_rollup',
            )
            result['season_rollup_cancelled'] += cancel_result.get('cancelled', 0)
            result['season_rollup_failed'] += cancel_result.get('failed', 0)
            logger.info(
                "  ➜ [共享资源维护] 完结季汇总完成: parent=%s S%02d, pack_share=%s, cancelled=%s, failed=%s",
                parent,
                _safe_int(season_number, 0),
                new_record.get('share_code') or '-',
                cancel_result.get('cancelled', 0),
                cancel_result.get('failed', 0),
            )
        except Exception as e:
            result['season_rollup_failed'] += 1
            logger.warning(
                "  ➜ [共享资源维护] 完结季汇总异常: parent=%s S%s -> %s",
                parent, season_number, e, exc_info=True,
            )
        time.sleep(0.3)

    return result



def _cleanup_expired_virtual_cache(max_rows: int = 80) -> int:
    p115 = P115Service.get_client()
    if not p115:
        logger.warning("  ➜ [共享资源维护] 115 客户端未初始化，跳过过期临时转存清理。")
        return 0

    rows = shared_share_db.get_expired_virtual_cache_rows(max_rows)

    cleaned = 0
    cleaned_rows_for_history = []
    for row in rows:
        virtual_id = str(row.get('virtual_id') or '').strip()
        real_fid = str(row.get('real_fid') or '').strip()
        title = row.get('title') or row.get('file_name') or virtual_id
        if not virtual_id or not real_fid: continue

        resp = None
        delete_ok = False
        try:
            resp = p115.fs_delete([real_fid])
            text = json.dumps(resp, ensure_ascii=False) if isinstance(resp, dict) else str(resp)
            delete_ok = bool(isinstance(resp, dict) and resp.get('state')) or any(k in text for k in ['不存在', '已删除', 'not found', 'delete success'])
        except Exception as e:
            text = str(e)
            delete_ok = any(k in text for k in ['不存在', 'not found'])
            logger.debug(f"  ➜ [共享资源维护] 删除过期临时转存异常: {virtual_id}/{real_fid} -> {e}")

        if delete_ok or resp is not None:
            emby_delete = _delete_emby_item_for_virtual(row)
            removed_projection = 0
            for key in ('strm_path', 'mediainfo_path', 'nfo_path'):
                if _remove_file_quietly(str(row.get(key) or '')): removed_projection += 1
            try:
                shared_virtual_db.mark_virtual_deleted(
                    virtual_id,
                    message='临时转存已过期且未转正，维护任务已清理临时缓存并删除 Emby 媒体项',
                    raw_json={
                        'expired_cache_cleaned_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                        'real_fid': real_fid,
                        'delete_response': resp,
                        'emby_delete': emby_delete,
                        'removed_projection_files': removed_projection,
                    }
                )
                shared_share_db.delete_p115_cache_node(real_fid)
                
                shared_virtual_db.add_credit_ledger(
                    'virtual_cache_expired_cleaned', 0,
                    f'清理过期虚拟临时转存并删除 Emby 媒体项：{title}',
                    ref_id=virtual_id, virtual_id=virtual_id, title=title,
                    raw_json={'real_fid': real_fid, 'delete_response': resp, 'emby_delete': emby_delete, 'removed_projection_files': removed_projection},
                )
                cleaned += 1
                cleaned_rows_for_history.append(row)
            except Exception as e:
                logger.warning(f"  ➜ [共享资源维护] 标记虚拟资源已删除失败: {virtual_id} -> {e}")
        time.sleep(0.15)

    if cleaned_rows_for_history:
        try: _cleanup_recent_receive_history(p115, cleaned_rows_for_history)
        except Exception as e: logger.warning(f"  ➜ [共享资源维护] 清理 115 最近接收记录异常: {e}")

    return cleaned


def _watching_missing_episodes(limit: int = 120) -> List[Dict[str, Any]]:
    return shared_share_db.get_watching_missing_episodes(limit)


# ======================================================================
# 虚拟入库分享健康检查：当前分享失效时切换到备份/完结季包
# ======================================================================
_CENTER_CONSUMABLE_STATUSES = {'alive', 'pending', 'reported'}
_SINGLE_EPISODE_TYPES = {'episode', 'episode_file', 'single'}
_SEASON_PACK_TYPES = {'season', 'season_pack', 'series', 'series_pack', 'tv', 'show'}


def _source_id_list(rows: List[Dict[str, Any]]) -> List[str]:
    out = []
    seen = set()
    for row in rows or []:
        sid = str((row or {}).get('source_id') or '').strip()
        if sid and sid not in seen:
            seen.add(sid)
            out.append(sid)
    return out


def _load_center_sources_by_id(client: SharedCenterClient, source_ids: List[str], batch_size: int = 80) -> Dict[str, Dict[str, Any]] | None:
    """按 source_id 批量获取中心源。None 表示中心查询失败，调用方不得据此删除虚拟项。"""
    source_ids = [str(x or '').strip() for x in (source_ids or []) if str(x or '').strip()]
    if not source_ids:
        return {}
    result: Dict[str, Dict[str, Any]] = {}
    statuses = 'alive,pending,replenish,dead,cancelled,expired,rejected'
    for i in range(0, len(source_ids), max(1, int(batch_size or 80))):
        batch = source_ids[i:i + batch_size]
        try:
            resp = client.list_sources(
                source_ids=batch,
                status=statuses,
                mine_only=False,
                include_raw=False,
                limit=max(len(batch), 1),
                offset=0,
            )
        except TypeError as e:
            logger.warning(f"  ➜ [共享资源维护] 当前共享中心客户端不支持按 source_id 校验虚拟入库源，跳过本轮检查: {e}")
            return None
        except Exception as e:
            logger.warning(f"  ➜ [共享资源维护] 批量查询虚拟入库中心源失败，跳过本轮检查: {e}")
            return None
        for item in resp.get('items') or []:
            sid = str((item or {}).get('source_id') or '').strip()
            if sid:
                result[sid] = dict(item)
    return result


def _center_source_consumable(source: Dict[str, Any]) -> bool:
    if not source:
        return False
    status = str(source.get('status') or '').strip().lower()
    if status not in _CENTER_CONSUMABLE_STATUSES:
        return False
    if str(source.get('share_code') or '').strip() == '':
        return False
    # list_sources 非 mine_only 时中心已经会过滤 raw；这里仍保守兜底。
    if source.get('has_raw_ffprobe') is False:
        return False
    return True


def _share_alive_for_virtual(p115, share_code: str, receive_code: str = '', cache: Dict[tuple, Any] = None) -> Any:
    """校验 115 分享是否仍可访问。None 表示网络/接口异常，不应据此删除虚拟项。"""
    share_code = str(share_code or '').strip()
    receive_code = str(receive_code or '').strip()
    if not share_code or not p115:
        return True
    cache = cache if cache is not None else {}
    key = (share_code, receive_code)
    if key in cache:
        return cache[key]
    try:
        snap = p115.share_info(share_code, receive_code, cid=0, limit=1)
        alive = _looks_share_alive(snap)
        cache[key] = alive
        return alive
    except Exception as e:
        if _is_network_error(e):
            logger.debug(f"  ➜ [共享资源维护] 校验 115 分享遇到网络异常，暂不判定失效: share={share_code}, err={e}")
            cache[key] = None
            return None
        logger.debug(f"  ➜ [共享资源维护] 校验 115 分享失败，按不可用处理: share={share_code}, err={e}")
        cache[key] = False
        return False


def _virtual_current_source_unhealthy_reason(
    row: Dict[str, Any],
    center_sources_by_id: Dict[str, Dict[str, Any]],
    p115,
    share_alive_cache: Dict[tuple, Any],
) -> str:
    source_id = str((row or {}).get('source_id') or '').strip()
    current_source = center_sources_by_id.get(source_id) if source_id else None
    if source_id:
        if not current_source:
            return 'center_source_missing'
        if not _center_source_consumable(current_source):
            return f"center_source_{str(current_source.get('status') or 'unusable').lower()}"

    share_code = str((current_source or {}).get('share_code') or (row or {}).get('share_code') or '').strip()
    receive_code = str((current_source or {}).get('receive_code') or (row or {}).get('receive_code') or '').strip()
    if not share_code:
        return 'missing_share_code'
    alive = _share_alive_for_virtual(p115, share_code, receive_code, share_alive_cache)
    if alive is None:
        return ''
    if not alive:
        return 'p115_share_dead'
    return ''


def _virtual_raw_context(row: Dict[str, Any]) -> tuple[Dict[str, Any], Dict[str, Any]]:
    raw = _json_dict((row or {}).get('raw_json'))
    context = raw.get('context') if isinstance(raw.get('context'), dict) else {}
    source = raw.get('center_source') if isinstance(raw.get('center_source'), dict) else {}
    replacement = raw.get('replacement_center_source') if isinstance(raw.get('replacement_center_source'), dict) else {}
    if replacement:
        source = {**source, **replacement}
    return context, source


def _virtual_identity(row: Dict[str, Any]) -> Dict[str, Any]:
    row = row or {}
    context, source = _virtual_raw_context(row)
    item_type = str(row.get('item_type') or context.get('item_type') or source.get('item_type') or '').strip()

    def first_text(*values):
        for value in values:
            text = str(value or '').strip()
            if text:
                return text
        return ''

    def first_int(*values):
        for value in values:
            n = _safe_int(value, None)
            if n is not None:
                return n
        return None

    parent = first_text(
        row.get('parent_series_tmdb_id'),
        context.get('parent_series_tmdb_id'), context.get('series_tmdb_id'), context.get('parent_tmdb_id'),
        source.get('parent_series_tmdb_id'), source.get('series_tmdb_id'),
    )
    tmdb_id = first_text(row.get('tmdb_id'), context.get('tmdb_id'), source.get('tmdb_id'))
    if not parent and item_type.lower() in _SEASON_PACK_TYPES:
        parent = tmdb_id
    # 早期中心单集/季包都可能把 tmdb_id 写成父剧 ID；Episode 搜索优先用 parent，没有 parent 再用 tmdb_id。
    season = first_int(row.get('season_number'), context.get('season_number'), source.get('season_number'))
    episode = first_int(row.get('episode_number'), context.get('episode_number'), source.get('episode_number'))
    return {
        'tmdb_id': tmdb_id,
        'parent_series_tmdb_id': parent,
        'season_number': season,
        'episode_number': episode,
        'sha1': str(row.get('sha1') or source.get('sha1') or '').strip().upper(),
        'item_type': item_type or 'Episode',
    }


def _search_center_sources_cached(client: SharedCenterClient, query: Dict[str, Any], cache: Dict[str, List[Dict[str, Any]]], limit: int = 200) -> List[Dict[str, Any]]:
    key = json.dumps(query or {}, ensure_ascii=False, sort_keys=True)
    if key in cache:
        return cache[key]
    try:
        resp = client.search_sources([query], limit_per_item=limit)
        results = resp.get('results') or []
        items = []
        if results:
            items = [dict(x) for x in (results[0].get('items') or []) if isinstance(x, dict)]
        cache[key] = items
        return items
    except Exception as e:
        logger.warning(f"  ➜ [共享资源维护] 查询虚拟入库备用共享源失败: query={query}, err={e}")
        cache[key] = []
        return []


def _candidate_source_rank(source: Dict[str, Any]) -> tuple:
    status = str((source or {}).get('status') or '').strip().lower()
    return (
        0 if status == 'alive' else 1,
        _safe_int((source or {}).get('success_count'), 0),
        _safe_int((source or {}).get('fail_count'), 0),
        str((source or {}).get('last_verified_at') or (source or {}).get('created_at') or ''),
        str((source or {}).get('source_id') or ''),
    )


def _source_can_replace_virtual(
    source: Dict[str, Any],
    row: Dict[str, Any],
    *,
    require_pack: bool = False,
    p115=None,
    share_alive_cache: Dict[tuple, Any] = None,
) -> bool:
    if not _center_source_consumable(source):
        return False
    old_source_id = str((row or {}).get('source_id') or '').strip()
    if old_source_id and str(source.get('source_id') or '').strip() == old_source_id:
        return False
    expected_sha1 = str((row or {}).get('sha1') or '').strip().upper()
    if expected_sha1 and str(source.get('sha1') or '').strip().upper() != expected_sha1:
        return False
    item_type = str(source.get('item_type') or '').strip().lower()
    if require_pack and item_type not in _SEASON_PACK_TYPES:
        return False
    alive = _share_alive_for_virtual(p115, source.get('share_code') or '', source.get('receive_code') or '', share_alive_cache)
    return alive is not False and alive is not None


def _find_replacement_source_for_virtual(
    client: SharedCenterClient,
    row: Dict[str, Any],
    *,
    p115=None,
    search_cache: Dict[str, List[Dict[str, Any]]] = None,
    share_alive_cache: Dict[tuple, Any] = None,
) -> tuple[Dict[str, Any], str]:
    """优先完结季包/季包，其次同集备份，返回新的中心源。"""
    search_cache = search_cache if search_cache is not None else {}
    share_alive_cache = share_alive_cache if share_alive_cache is not None else {}
    ident = _virtual_identity(row)
    parent = ident.get('parent_series_tmdb_id') or ident.get('tmdb_id')
    season = ident.get('season_number')
    episode = ident.get('episode_number')
    sha1 = ident.get('sha1')

    # 1. 剧集优先找同季季包。完结季汇总删除单集后，这里会直接切到季包里的同 SHA1 文件。
    if parent and season is not None:
        season_query = {'tmdb_id': parent, 'item_type': 'Season', 'season_number': season, 'episode_number': None}
        season_items = _search_center_sources_cached(client, season_query, search_cache, limit=300)
        pack_candidates = [
            x for x in season_items
            if str(x.get('sha1') or '').strip().upper() == sha1
            and _source_can_replace_virtual(x, row, require_pack=True, p115=p115, share_alive_cache=share_alive_cache)
        ]
        if pack_candidates:
            return sorted(pack_candidates, key=_candidate_source_rank)[0], 'season_pack'

    # 2. 没有季包时，再找同集备份共享。
    episode_queries = []
    if parent and season is not None and episode is not None:
        episode_queries.append({'tmdb_id': parent, 'item_type': 'Episode', 'season_number': season, 'episode_number': episode})
    tmdb_id = ident.get('tmdb_id')
    if tmdb_id and tmdb_id != parent and season is not None and episode is not None:
        episode_queries.append({'tmdb_id': tmdb_id, 'item_type': 'Episode', 'season_number': season, 'episode_number': episode})
    if not episode_queries and tmdb_id:
        episode_queries.append({'tmdb_id': tmdb_id, 'item_type': 'Episode', 'season_number': season, 'episode_number': episode})

    single_candidates = []
    for query in episode_queries:
        for src in _search_center_sources_cached(client, query, search_cache, limit=120):
            if _source_can_replace_virtual(src, row, require_pack=False, p115=p115, share_alive_cache=share_alive_cache):
                # Episode 查询理论上只返回单集；这里兜底排除季包，确保优先级语义清晰。
                if str(src.get('item_type') or '').strip().lower() in _SINGLE_EPISODE_TYPES:
                    single_candidates.append(src)
    if single_candidates:
        return sorted(single_candidates, key=_candidate_source_rank)[0], 'episode_backup'

    return {}, ''


def _cleanup_virtual_projection_files(row: Dict[str, Any]) -> int:
    removed = 0
    for key in ('strm_path', 'mediainfo_path', 'nfo_path'):
        if _remove_file_quietly(str((row or {}).get(key) or '')):
            removed += 1
    return removed


def _delete_virtual_item_for_dead_share(row: Dict[str, Any], reason: str, p115=None) -> bool:
    virtual_id = str((row or {}).get('virtual_id') or '').strip()
    if not virtual_id:
        return False
    title = (row or {}).get('title') or (row or {}).get('file_name') or virtual_id
    real_fid = str((row or {}).get('real_fid') or '').strip()
    delete_resp = None
    if p115 and real_fid:
        try:
            delete_resp = p115.fs_delete([real_fid])
            shared_share_db.delete_p115_cache_node(real_fid)
        except Exception as e:
            delete_resp = {'error': str(e)}
            logger.debug(f"  ➜ [共享资源维护] 删除失效虚拟入库临时文件失败: virtual={virtual_id}, fid={real_fid}, err={e}")
    emby_delete = _delete_emby_item_for_virtual(row)
    removed_projection = _cleanup_virtual_projection_files(row)
    try:
        shared_virtual_db.mark_virtual_deleted(
            virtual_id,
            message=f'虚拟入库原分享已失效且没有可用季包/单集备份，维护任务已删除投影：{reason}',
            raw_json={
                'virtual_source_health_deleted_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                'reason': reason,
                'real_fid': real_fid,
                'delete_response': delete_resp,
                'emby_delete': emby_delete,
                'removed_projection_files': removed_projection,
            },
        )
        shared_virtual_db.add_credit_ledger(
            'virtual_source_dead_deleted', 0,
            f'虚拟入库源失效且无备份，已删除：{title}',
            ref_id=virtual_id,
            virtual_id=virtual_id,
            title=title,
            raw_json={'reason': reason, 'real_fid': real_fid, 'delete_response': delete_resp, 'emby_delete': emby_delete},
        )
        return True
    except TypeError:
        # 兼容旧 shared_virtual_db.mark_virtual_deleted(message) 签名。
        shared_virtual_db.mark_virtual_deleted(
            virtual_id,
            message=f'虚拟入库原分享已失效且没有可用季包/单集备份，维护任务已删除投影：{reason}',
        )
        return True
    except Exception as e:
        logger.warning(f"  ➜ [共享资源维护] 标记失效虚拟入库删除失败: virtual={virtual_id}, err={e}", exc_info=True)
        return False


def _check_and_repair_virtual_item_sources(client: SharedCenterClient, limit: int = 300) -> Dict[str, int]:
    """维护任务虚拟入库分享有效性检查。失效时优先切换完结季包，其次单集备份，最后删除投影。"""
    result = {'checked': 0, 'invalid': 0, 'switched_pack': 0, 'switched_episode': 0, 'deleted': 0, 'skipped': 0, 'failed': 0}
    try:
        rows = shared_share_db.get_virtual_items_for_share_health(limit)
    except Exception as e:
        logger.warning(f"  ➜ [共享资源维护] 查询虚拟入库健康检查列表失败: {e}")
        result['failed'] += 1
        return result
    if not rows:
        return result

    result['checked'] = len(rows)
    center_sources = _load_center_sources_by_id(client, _source_id_list(rows))
    if center_sources is None:
        result['skipped'] = len(rows)
        return result

    p115 = P115Service.get_client()
    share_alive_cache: Dict[tuple, Any] = {}
    search_cache: Dict[str, List[Dict[str, Any]]] = {}

    for row in rows:
        virtual_id = str((row or {}).get('virtual_id') or '').strip()
        title = (row or {}).get('title') or (row or {}).get('file_name') or virtual_id
        try:
            reason = _virtual_current_source_unhealthy_reason(row, center_sources, p115, share_alive_cache)
            if not reason:
                continue
            result['invalid'] += 1
            replacement, mode = _find_replacement_source_for_virtual(
                client,
                row,
                p115=p115,
                search_cache=search_cache,
                share_alive_cache=share_alive_cache,
            )
            if replacement:
                message = (
                    f"虚拟入库原分享不可用({reason})，已切换到"
                    f"{'完结季包/季包' if mode == 'season_pack' else '单集备份'}：{replacement.get('share_code') or replacement.get('source_id')}"
                )
                shared_share_db.update_virtual_item_center_source(virtual_id, replacement, message=message)
                shared_virtual_db.add_credit_ledger(
                    'virtual_source_replaced', 0,
                    f"虚拟入库源失效自动切换备份：{title}",
                    ref_id=str(replacement.get('source_id') or virtual_id),
                    source_id=str(replacement.get('source_id') or ''),
                    virtual_id=virtual_id,
                    tmdb_id=str((row or {}).get('tmdb_id') or ''),
                    item_type=str((row or {}).get('item_type') or ''),
                    title=title,
                    raw_json={'reason': reason, 'mode': mode, 'replacement': replacement, 'old_share_code': row.get('share_code'), 'old_source_id': row.get('source_id')},
                )
                if mode == 'season_pack':
                    result['switched_pack'] += 1
                else:
                    result['switched_episode'] += 1
                logger.info(
                    "  ➜ [共享资源维护] 虚拟入库源失效，已切换到%s: %s old=%s new=%s",
                    '季包' if mode == 'season_pack' else '单集备份',
                    title,
                    row.get('share_code') or row.get('source_id'),
                    replacement.get('share_code') or replacement.get('source_id'),
                )
            else:
                if _delete_virtual_item_for_dead_share(row, reason, p115=p115):
                    result['deleted'] += 1
                else:
                    result['failed'] += 1
        except Exception as e:
            logger.warning(f"  ➜ [共享资源维护] 虚拟入库源健康检查失败: virtual={virtual_id}, title={title}, err={e}", exc_info=True)
            result['failed'] += 1
        time.sleep(0.1)

    if result['invalid'] or result['deleted'] or result['switched_pack'] or result['switched_episode']:
        logger.info(
            "  ➜ [共享资源维护] 虚拟入库源健康检查完成：检查 %s，异常 %s，切季包 %s，切单集备份 %s，删除 %s，失败 %s。",
            result['checked'], result['invalid'], result['switched_pack'], result['switched_episode'], result['deleted'], result['failed'],
        )
    return result


def _has_local_virtual_projection_for_episode(row: Dict[str, Any]) -> bool:
    parent = str(row.get('parent_series_tmdb_id') or '')
    season = _safe_int(row.get('season_number'), -1)
    episode = _safe_int(row.get('episode_number'), -1)
    if not parent or season < 0 or episode < 0: return False
    try:
        return shared_share_db.check_local_virtual_projection_exists(parent, season, episode)
    except Exception:
        return False


def _episode_guard_key(row: Dict[str, Any]) -> str:
    parent = str(row.get('parent_series_tmdb_id') or '').strip()
    season = _safe_int(row.get('season_number'), -1)
    episode = _safe_int(row.get('episode_number'), -1)
    if not parent or season < 0 or episode < 0:
        return ''
    return f'{parent}|{season}|{episode}'


def _auto_follow_watching_series_from_center(max_items: int = 80) -> Dict[str, int]:
    """把 Watching / Paused 季的缺失分集纳入共享中心消费链路。"""
    if not _enabled():
        return {'missing': 0, 'consumed': 0, 'gaps': 0, 'skipped': 0}

    try:
        from handler.shared_center_client import shared_resource_mode
        from handler.shared_subscription_service import try_consume_shared_resource
    except Exception as e:
        logger.warning(f"  ➜ [共享资源维护] 无法加载共享消费入口，跳过剧集追更: {e}")
        return {'missing': 0, 'consumed': 0, 'gaps': 0, 'skipped': 0}

    rows = _watching_missing_episodes(limit=max_items)
    consumed = gaps = skipped = 0
    mode = shared_resource_mode()
    consecutive_errors = 0 
    consumed_share_codes = set()
    covered_episode_keys = set()
    physical_parent_cache = {}

    for row in rows:
        try:
            row_key = _episode_guard_key(row)
            if row_key and row_key in covered_episode_keys:
                skipped += 1
                logger.info(
                    "  ➜ [共享资源维护] 追更缺集已被本轮前序季包覆盖，跳过重复消费：%s S%02dE%02d",
                    row.get('season_title') or row.get('parent_series_tmdb_id'),
                    _safe_int(row.get('season_number'), 0),
                    _safe_int(row.get('episode_number'), 0),
                )
                continue

            if _has_local_virtual_projection_for_episode(row):
                skipped += 1
                continue

            parent_tmdb = row.get('parent_series_tmdb_id')
            title = row.get('title') or row.get('season_title') or f"S{_safe_int(row.get('season_number'), 1):02d}E{_safe_int(row.get('episode_number'), 0):02d}"
            consume_mode = _consume_mode_for_watching_row(mode, row, physical_parent_cache)
            forced_permanent = consume_mode != mode
            result = try_consume_shared_resource(
                row,
                title=title,
                tmdb_id=row.get('tmdb_id') or parent_tmdb,
                item_type='Episode',
                parent_tmdb_id=parent_tmdb,
                season_number=row.get('season_number'),
                year=row.get('release_year') or '',
                exclude_share_codes=sorted(consumed_share_codes),
                force_mode=consume_mode,
            )
            for share_code in result.get('matched_share_codes') or []:
                share_code = str(share_code or '').strip()
                if share_code:
                    consumed_share_codes.add(share_code)
            for key in result.get('covered_episode_keys') or []:
                key = str(key or '').strip()
                if key:
                    covered_episode_keys.add(key)
            if result.get('success'):
                if result.get('skipped_existing') and not _safe_int(result.get('count'), 0):
                    skipped += 1
                    logger.info(
                        "  ➜ [共享资源维护] 追更缺集命中中心资源，但本地 SHA1 已存在，跳过重复转存：%s S%02dE%02d",
                        row.get('season_title') or parent_tmdb,
                        _safe_int(row.get('season_number'), 0),
                        _safe_int(row.get('episode_number'), 0),
                    )
                else:
                    consumed += 1
                    action_label = '虚拟入库' if result.get('mode') == 'virtual' else '永久转存'
                    if forced_permanent and result.get('mode') == 'permanent':
                        logger.info(
                            "  ➜ [共享资源维护] 追更缺集命中中心资源；本剧已有物理入库分集，已按永久转存处理：%s S%02dE%02d",
                            row.get('season_title') or parent_tmdb,
                            _safe_int(row.get('season_number'), 0),
                            _safe_int(row.get('episode_number'), 0),
                        )
                    else:
                        logger.info(
                            "  ➜ [共享资源维护] 追更缺集命中中心资源并已%s：%s S%02dE%02d",
                            action_label,
                            row.get('season_title') or parent_tmdb,
                            _safe_int(row.get('season_number'), 0),
                            _safe_int(row.get('episode_number'), 0),
                        )
            elif result.get('reported_gap'):
                gaps += 1
        except Exception as e:
            logger.warning(f"  ➜ [共享资源维护] 剧集追更共享消费失败: {row} -> {e}", exc_info=True)
            if _is_network_error(e):
                consecutive_errors += 1
                if consecutive_errors >= 3:
                    logger.error("  ➜ [共享资源维护] 连续 3 次网络请求失败，触发熔断，提前结束剧集追更。")
                    break
        time.sleep(0.2)

    return {'missing': len(rows), 'consumed': consumed, 'gaps': gaps, 'skipped': skipped}


# ======================================================================
# 求分享命中定向转存：客户端仅在自己有 open 求分享/同求时启动长轮询。
# ======================================================================
_share_request_event_listener_lock = threading.Lock()
_share_request_event_listener_thread = None
_share_request_event_listener_stop = threading.Event()


def _my_active_share_request_count(client: SharedCenterClient) -> int:
    try:
        resp = client.list_share_requests(status='open', limit=100, offset=0)
        items = resp.get('items') or []
        return sum(1 for item in items if bool(item.get('joined_by_me')) and str(item.get('status') or 'open') == 'open')
    except Exception as e:
        logger.debug(f"  ➜ [求分享监听] 检查我的开放求分享失败: {e}")
        return 0


def _notify_share_request_push(event: Dict[str, Any], result: Dict[str, Any], success: bool):
    try:
        from handler.telegram import send_share_request_push_notification
        send_share_request_push_notification(event, result=result, success=success)
    except Exception as e:
        logger.debug(f"  ➜ [求分享监听] 发送自动转存通知失败: {e}")


def _handle_share_request_event(client: SharedCenterClient, event: Dict[str, Any]) -> Dict[str, Any]:
    event = dict(event or {})
    event_id = str(event.get('event_id') or '').strip()
    source_id = str(event.get('source_id') or '').strip()
    title = event.get('title') or (event.get('payload') or {}).get('title') or source_id
    if not event_id or not source_id:
        return {'success': False, 'message': '事件缺少 event_id/source_id'}

    try:
        from handler.shared_subscription_service import consume_center_sources
    except Exception as e:
        msg = f'共享资源消费入口不可用：{e}'
        try:
            client.ack_share_request_event(event_id, result='failed', message=msg)
        except Exception:
            pass
        _notify_share_request_push(event, {'message': msg}, False)
        return {'success': False, 'message': msg}

    try:
        mode = shared_resource_mode()
        context = {
            'source': 'share_request_push',
            'share_request_event_id': event_id,
            'share_request_group_id': event.get('group_id'),
            'title': title,
            'target_type': event.get('target_type'),
            'season_number': event.get('season_number'),
            'episode_number': event.get('episode_number'),
        }
        logger.info("  ➜ [求分享监听] 收到命中事件，开始自动转存: %s source=%s", title, source_id)
        result = consume_center_sources([source_id], mode=mode, context=context) or {}
        ok = bool(result.get('success'))
        message = result.get('message') or result.get('action_type') or ('自动转存成功' if ok else '自动转存失败')
        try:
            client.ack_share_request_event(event_id, result='success' if ok else 'failed', message=message)
        except Exception as e:
            logger.debug(f"  ➜ [求分享监听] 回执中心事件失败: event={event_id}, err={e}")
        if ok:
            shared_virtual_db.add_credit_ledger(
                'share_request_push_import_success', 0,
                f'求分享命中后自动转存：{title}',
                ref_id=str(event.get('group_id') or event_id),
                title=title,
                raw_json={'event': event, 'result': result},
            )
        else:
            shared_virtual_db.add_credit_ledger(
                'share_request_push_import_failed', 0,
                f'求分享命中但自动转存失败：{title}',
                ref_id=str(event.get('group_id') or event_id),
                title=title,
                raw_json={'event': event, 'result': result},
            )
        _notify_share_request_push(event, result, ok)
        return result
    except Exception as e:
        msg = f'自动转存异常：{e}'
        logger.warning(f"  ➜ [求分享监听] 处理命中事件失败: {event} -> {e}", exc_info=True)
        try:
            client.ack_share_request_event(event_id, result='failed', message=msg)
        except Exception:
            pass
        _notify_share_request_push(event, {'message': msg}, False)
        return {'success': False, 'message': msg}


def _share_request_event_listener_worker():
    logger.info("  ➜ [求分享监听] 长轮询监听已启动。")
    client = SharedCenterClient()
    idle_errors = 0
    try:
        while not _share_request_event_listener_stop.is_set():
            if not _enabled() or not client.ready:
                break
            if _my_active_share_request_count(client) <= 0:
                logger.info("  ➜ [求分享监听] 当前没有开放的本人求分享/同求，停止长轮询。")
                break
            try:
                resp = client.poll_share_request_events(timeout=25, limit=5)
                idle_errors = 0
            except Exception as e:
                idle_errors += 1
                logger.debug(f"  ➜ [求分享监听] 长轮询失败，将重试: {e}")
                if idle_errors >= 12:
                    logger.warning("  ➜ [求分享监听] 连续长轮询失败过多，停止监听，等待下次求分享/维护任务重新启动。")
                    break
                time.sleep(min(30, 3 * idle_errors))
                continue

            for event in resp.get('items') or []:
                if _share_request_event_listener_stop.is_set():
                    break
                _handle_share_request_event(client, event)
    finally:
        logger.info("  ➜ [求分享监听] 长轮询监听已停止。")


def ensure_share_request_event_listener() -> bool:
    """启动求分享长轮询监听。没有本人 open 求分享时不会启动。"""
    if not _enabled():
        return False
    client = SharedCenterClient()
    if not client.ready:
        return False
    if _my_active_share_request_count(client) <= 0:
        return False
    global _share_request_event_listener_thread
    with _share_request_event_listener_lock:
        if _share_request_event_listener_thread and _share_request_event_listener_thread.is_alive():
            return True
        _share_request_event_listener_stop.clear()
        _share_request_event_listener_thread = threading.Thread(
            target=_share_request_event_listener_worker,
            name='ShareRequestEventListener',
            daemon=True,
        )
        _share_request_event_listener_thread.start()
        return True


def stop_share_request_event_listener():
    _share_request_event_listener_stop.set()


def task_shared_resource_maintenance(processor=None, maintenance_silent: bool = False):
    """共享资源维护总任务。可由前端手动触发，也由调度器硬编码定时执行。

    maintenance_silent=True 时用于调度器后台静默执行：压制中间过程日志，但保留开始和结束摘要；
    未捕获异常仍会由 task_manager 以 ERROR 记录。
    """
    if maintenance_silent:
        logger.info("  ➜ [共享资源维护] 后台自动维护任务开始执行...")

    old_logger_level = None
    if maintenance_silent:
        # 调度器静默运行时，压制本模块的 INFO/WARNING 成功与进度日志；
        # 真正未捕获的异常仍会由 task_manager 以 ERROR 记录。
        old_logger_level = logger.level
        logger.setLevel(logging.ERROR)

    def _status(progress: int, message: str):
        if maintenance_silent:
            return
        task_manager.update_status_from_thread(progress, message)

    try:
        _status(0, '正在初始化共享资源维护任务...')
        if not _enabled():
            _status(100, '共享资源未启用，跳过。')
            return
        client = SharedCenterClient()
        if not client.ready:
            _status(100, '共享中心地址或 device_token 未配置，跳过。')
            return

        _status(5, '正在测试中心服务器连通性...')
        try:
            from routes.shared_resource import _fetch_center_credit
            credit_test = _fetch_center_credit()
            if not credit_test.get('ok'):
                msg = f"中心服务器连接失败 ({credit_test.get('message')})，为避免任务卡死，本次维护取消。"
                # 连通性失败属于重要异常，无论是否静默都以 ERROR 级别输出
                logger.error(f"  ➜ [共享资源维护] {msg}")
                _status(100, msg)
                return
        except Exception as e:
            msg = f"中心服务器连接超时或异常，为避免任务卡死，本次维护取消。"
            logger.error(f"  ➜ [共享资源维护] {msg} ({e})")
            _status(100, msg)
            return

        total = {}
        _status(10, '正在自动登记本地缺口...')
        total['reported_gaps'] = _report_local_wanted_gaps(client)

        _status(25, '正在清理过期虚拟临时转存...')
        total['expired_virtual_cache_cleaned'] = _cleanup_expired_virtual_cache()

        _status(35, '正在检查分享水位并清理违规分享...')
        _merge_maintenance_counts(total, _enforce_local_share_waterline(client))

        _status(45, '正在为中心缺口自动创建本机分享...')
        total['auto_created_shares'] = _auto_share_center_open_gaps(client)

        _status(47, '正在自动响应别人发布的求分享...')
        request_auto_result = _auto_share_center_share_requests(client)
        total.update({f'request_auto_{k}': v for k, v in request_auto_result.items()})

        _status(49, '正在匹配中心待补充资源...')
        repl_result = _auto_share_center_replenish_sources(client)
        total.update({f'replenish_{k}': v for k, v in repl_result.items()})

        _status(52, '正在汇总已完结季的单集分享...')
        total.update(_rollup_completed_season_episode_shares(client))

        _status(60, '正在从中心资源库处理追更缺集...')
        follow_result = _auto_follow_watching_series_from_center()
        total.update({f'follow_{k}': v for k, v in follow_result.items()})

        _status(72, '正在启动求分享命中长轮询监听...')
        total['share_request_listener'] = ensure_share_request_event_listener()

        _status(74, '正在同步分享审核状态并自动登记中心...')
        total.update(_auto_check_and_report_local_shares(client))

        _status(78, '正在检查虚拟入库分享有效性并切换备份...')
        virtual_health = _check_and_repair_virtual_item_sources(client)
        total.update({f'virtual_source_{k}': v for k, v in virtual_health.items()})

        _status(82, '正在对账中心残留共享源...')
        total.update(_cleanup_orphan_center_sources(client))

        _status(86, '正在复查分享水位...')
        _merge_maintenance_counts(total, _enforce_local_share_waterline(client))

        _status(92, '正在同步贡献值快照...')
        try:
            # 复用路由层已有的中心贡献值同步逻辑。
            from routes.shared_resource import _fetch_center_credit
            total['credit'] = _fetch_center_credit().get('ok', False)
        except Exception as e:
            if maintenance_silent:
                logger.error(f"  ➜ [共享资源维护] 同步贡献值失败: {e}")
            else:
                logger.warning(f"  ➜ [共享资源维护] 同步贡献值失败: {e}")
            total['credit'] = False

        if old_logger_level is not None:
            logger.setLevel(old_logger_level)
            old_logger_level = None

        # 1. 给控制台日志用的多行列表（一目了然）
        log_msg = (
            "\n=== 共享资源维护完成 ===\n"
            f"  ➜ 登记缺口: {total.get('reported_gaps', 0)}\n"
            f"  ➜ 清理临时转存: {total.get('expired_virtual_cache_cleaned', 0)}\n"
            f"  ➜ 自动创建分享: {total.get('auto_created_shares', 0)}\n"
            f"  ➜ 自动响应求分享: 检查 {total.get('request_auto_checked', 0)}，命中 {total.get('request_auto_matched', 0)}，创建 {total.get('request_auto_created', 0)}，跳过 {total.get('request_auto_skipped', 0)}，失败 {total.get('request_auto_failed', 0)}\n"
            f"  ➜ 待补充补源: 检查 {total.get('replenish_checked', 0)}，命中 {total.get('replenish_matched', 0)}，创建 {total.get('replenish_created', 0)}，跳过 {total.get('replenish_skipped', 0)}，失败 {total.get('replenish_failed', 0)}\n"
            f"  ➜ 完结季包汇总: 创建季包 {total.get('season_rollup_created', 0)}，清理单集 {total.get('season_rollup_cancelled', 0)}/{total.get('season_rollup_failed', 0)}\n"
            f"  ➜ 虚拟入库源检查: 检查 {total.get('virtual_source_checked', 0)}，异常 {total.get('virtual_source_invalid', 0)}，切季包 {total.get('virtual_source_switched_pack', 0)}，切单集 {total.get('virtual_source_switched_episode', 0)}，删除 {total.get('virtual_source_deleted', 0)}，失败 {total.get('virtual_source_failed', 0)}\n"
            f"  ➜ 违规分享清理: {total.get('share_invalid_deleted', 0)}/{total.get('share_invalid_failed', 0)}\n"
            f"  ➜ 缺 raw 清理: {total.get('share_raw_missing_deleted', 0)}/{total.get('share_raw_missing_failed', 0)}\n"
            f"  ➜ 分享水位清理: {total.get('share_pruned', 0)}/{total.get('share_prune_failed', 0)}\n"
            f"  ➜ 中心残留清理: {total.get('center_orphan_cancelled', 0)}/{total.get('center_orphan_failed', 0)}\n"
            f"  ➜ 剧集追更命中: {total.get('follow_consumed', 0)}/{total.get('follow_missing', 0)}\n"
            f"  ➜ 登记追更缺口: {total.get('follow_gaps', 0)}\n"
            f"  ➜ 分享状态同步: 检查 {total.get('checked', 0)}，自动登记 {total.get('reported', 0)}，中心补登 {total.get('resynced', 0)}，清理失效 {total.get('cancelled', 0)}\n"
            "========================"
        )
        logger.info(log_msg)
        
        # 2. 给前端任务面板用的单行简报（防止撑破 UI）
        status_msg = (
            f"维护完成：登记缺口 {total.get('reported_gaps', 0)}，创建分享 {total.get('auto_created_shares', 0)}，"
            f"响应求分享 {total.get('request_auto_created', 0)}，待补充创建 {total.get('replenish_created', 0)}，"
            f"虚拟源切换 {total.get('virtual_source_switched_pack', 0) + total.get('virtual_source_switched_episode', 0)}，"
            f"追更命中 {total.get('follow_consumed', 0)}，自动登记 {total.get('reported', 0)}。详细摘要请查看日志。"
        )
        _status(100, status_msg)
    finally:
        # 确保发生异常时也能恢复日志级别
        if old_logger_level is not None:
            logger.setLevel(old_logger_level)

def trigger_shared_resource_maintenance_task() -> bool:
    """供路由/调度器调用的统一入口。"""
    return task_manager.submit_task(
        task_shared_resource_maintenance,
        '共享资源自动维护',
        processor_type='media',
    )
