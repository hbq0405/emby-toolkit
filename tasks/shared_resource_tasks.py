# tasks/shared_resource_tasks.py
# 共享资源自动维护任务：缺口登记、分享审核同步、中心登记、失效清理、中心缺口自动分享。
import json
import logging
import time
from typing import Dict, Any, List

import config_manager
import constants
import task_manager
from database import shared_share_db, shared_virtual_db
from database.connection import get_db_connection
from handler.p115_service import P115Service
from handler.shared_center_client import SharedCenterClient, shared_center_enabled

logger = logging.getLogger(__name__)


def _cfg(name: str, fallback: str, default=None):
    key = getattr(constants, name, fallback)
    return (config_manager.APP_CONFIG or {}).get(key, default)


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
    """把本机订阅/想看但尚未入库的项目登记到中心缺口池。"""
    rows = []
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT tmdb_id, item_type, parent_series_tmdb_id, season_number, episode_number,
                       title, release_year
                FROM media_metadata
                WHERE COALESCE(in_library, FALSE) = FALSE
                  AND item_type IN ('Movie','Series','Season','Episode')
                  AND subscription_status IN ('WANTED','REQUESTED','SUBSCRIBED','PENDING_RELEASE')
                ORDER BY last_updated_at DESC NULLS LAST, created_at DESC NULLS LAST
                LIMIT %s
                """,
                (int(limit),),
            )
            rows = [dict(r) for r in cur.fetchall()]

    items = [_gap_item(r) for r in rows]
    items = [x for x in items if x.get('tmdb_id') and x.get('item_type')]
    if not items:
        return 0
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
    return max(0, _safe_int(_cfg('CONFIG_OPTION_115_SHARED_MAX_ACTIVE_SHARES', 'p115_shared_max_active_shares', 0), 0))


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
        center_status='cancelled' if center_ok else 'cancel_failed',
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
                center_status='cancelled' if center_ok else 'cancel_failed',
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


def _auto_check_and_report_local_shares(client: SharedCenterClient, max_records: int = 80) -> Dict[str, int]:
    """自动同步 115 分享状态；可用后上传 raw 并登记中心；失效时撤销中心源。"""
    p115 = P115Service.get_client()
    if not p115:
        logger.warning("  ➜ [共享资源维护] 115 客户端未初始化，跳过分享状态同步。")
        return {'checked': 0, 'reported': 0, 'cancelled': 0}

    records, _ = shared_share_db.list_share_records(status='all', keyword='', page=1, page_size=max_records)
    checked = reported = cancelled = 0
    consecutive_errors = 0

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
            alive = _looks_share_alive(snap)
            if alive:
                update = {'status': 'alive', 'review_status': 'alive', 'last_checked_at': 'NOW()', 'last_error': '分享可用', 'raw_json': {'last_snap': snap}}
                shared_share_db.update_share_record(record['id'], **update)
                record = shared_share_db.get_share_record(record['id']) or record
                if _record_reportable(record) and sr is not None:
                    # 自动补 raw + 登记中心。
                    try:
                        cfg, headers = sr._center_headers()
                        sr._upload_share_raw_ffprobe_to_center(record['id'], cfg, headers, force=True)
                    except Exception as e:
                        logger.debug(f"  ➜ [共享资源维护] 自动上传 raw 失败，继续尝试登记中心: {e}")
                    try:
                        # 直接复用 route 的核心注册逻辑不方便调用带 Flask request 的视图，这里手动按 shared_share_items 注册。
                        items = shared_share_db.list_share_items(record['id'])
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
                            consistency = sr._validate_season_pack_consistency(items)
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
                        ok = 0
                        for item in items:
                            sha1 = str(item.get('sha1') or '').strip().upper()
                            if not sha1:
                                continue
                            record_share_type = str(record.get('share_type') or '').strip().lower()
                            is_season_pack = record_share_type in ('season_pack', 'series_pack', 'season', 'tv_pack')
                            center_item_type = 'Season' if is_season_pack else (item.get('item_type') or record.get('item_type') or 'Movie')
                            if record_share_type == 'episode_file':
                                center_item_type = 'Episode'
                            standard_identity = sr._standard_share_identity(record, item, center_item_type=center_item_type) if sr is not None else {}
                            resp = client.register_source(
                                tmdb_id=standard_identity.get('tmdb_id') or item.get('tmdb_id') or record.get('tmdb_id'),
                                item_type=center_item_type,
                                season_number=item.get('season_number') or record.get('season_number'),
                                episode_number=None if is_season_pack else item.get('episode_number'),
                                title=standard_identity.get('title') or record.get('title') or '',
                                release_year=standard_identity.get('release_year') or record.get('release_year'),
                                sha1=sha1,
                                size=_safe_int(item.get('size'), 0),
                                file_name=item.get('file_name') or '',
                                quality='',
                                source_provider='auto_gap_share' if ((record.get('raw_json') or {}).get('auto_gap')) else 'user_share',
                                share_code=record.get('share_code'),
                                receive_code=record.get('receive_code') or '',
                                has_raw_ffprobe=bool(item.get('raw_ffprobe_uploaded')),
                            )
                            if resp.get('source_id'):
                                shared_share_db.mark_item_reported(item['id'], resp.get('source_id'))
                                ok += 1
                        if ok:
                            shared_share_db.update_share_record(record['id'], center_status='reported', status='reported', reported_count=ok, reported_at='NOW()', last_error='自动登记中心成功')
                            reported += 1
                    except Exception as e:
                        logger.warning(f"  ➜ [共享资源维护] 自动登记中心失败: share={share_code}, err={e}")
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
                            center_status='cancelled' if center_ok else 'cancel_failed',
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
                            center_status='cancelled' if center_ok else 'cancel_failed',
                            last_checked_at='NOW()',
                            last_error=f'115 审核违规/风控，但自动删除分享失败：{p115_resp}',
                            raw_json=raw_json,
                        )
                    cancelled += 1
                else:
                    # 115 分享已不可用，撤销中心源并本地标记。
                    try:
                        client.cancel_sources(share_code=share_code, sha1_list=sha1s, reason='auto_share_dead', delete_raw_ffprobe=True)
                    except Exception as e:
                        logger.debug(f"  ➜ [共享资源维护] 撤销中心源失败: {e}")
                    shared_share_db.update_share_record(record['id'], status='dead', review_status='dead', center_status='cancelled', last_checked_at='NOW()', cancelled_at='NOW()', last_error='自动检测到115分享失效，已撤销中心源')
                    cancelled += 1
        except Exception as e:
            logger.warning(f"  ➜ [共享资源维护] 同步分享状态异常: share={share_code}, err={e}")
            if _is_network_error(e):
                consecutive_errors += 1
                if consecutive_errors >= 3:
                    logger.error("  ➜ [共享资源维护] 连续 3 次网络请求失败，触发熔断，提前结束分享状态同步。")
                    break
        time.sleep(0.2)

    return {'checked': checked, 'reported': reported, 'cancelled': cancelled}


def _find_local_media_for_gap(gap: Dict[str, Any]) -> Dict[str, Any] | None:
    """中心缺口命中本地媒体库后返回 media_metadata 行。"""
    tmdb_id = str(gap.get('tmdb_id') or '').strip()
    item_type = str(gap.get('item_type') or '').strip()
    season = gap.get('season_number')
    episode = gap.get('episode_number')
    if not tmdb_id or not item_type:
        return None
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if item_type == 'Movie':
                cur.execute("SELECT * FROM media_metadata WHERE item_type='Movie' AND tmdb_id=%s AND in_library=TRUE LIMIT 1", (tmdb_id,))
            elif item_type in ('Season', 'Series'):
                if season not in (None, ''):
                    cur.execute(
                        """
                        SELECT * FROM media_metadata
                        WHERE item_type='Season' AND parent_series_tmdb_id=%s AND season_number=%s AND in_library=TRUE
                        LIMIT 1
                        """,
                        (tmdb_id, int(season)),
                    )
                else:
                    cur.execute("SELECT * FROM media_metadata WHERE item_type='Series' AND tmdb_id=%s AND in_library=TRUE LIMIT 1", (tmdb_id,))
            elif item_type == 'Episode':
                cur.execute(
                    """
                    SELECT * FROM media_metadata
                    WHERE item_type='Episode'
                      AND (parent_series_tmdb_id=%s OR tmdb_id=%s)
                      AND COALESCE(season_number, -1)=COALESCE(%s, -1)
                      AND COALESCE(episode_number, -1)=COALESCE(%s, -1)
                      AND in_library=TRUE
                    LIMIT 1
                    """,
                    (tmdb_id, tmdb_id, int(season or -1), int(episode or -1)),
                )
            else:
                return None
            row = cur.fetchone()
            return dict(row) if row else None


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


def _has_existing_share_for_gap(gap: Dict[str, Any], candidate: Dict[str, Any] | None = None, files: List[Dict[str, Any]] | None = None) -> bool:
    """判断中心缺口是否已经有本机分享在处理（包含已取消的记录，作为黑名单防止死循环）。"""
    return shared_share_db.has_existing_share_for_gap(
        gap or {},
        candidate or {},
        files or [],
        # 👇 【修改】加上 cancelled，只要曾经分享过并被淘汰了，就不再自动分享
        statuses=_active_share_statuses() + ['cancelled', 'cancel_failed', 'deleted'],
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
                    consistency = sr._validate_season_pack_consistency(files)
                    if not consistency.get('ok'):
                        logger.info(f"  ➜ [共享资源维护] 自动分享跳过媒体参数不一致的季包：{candidate.get('display_title')} -> {consistency.get('message')}")
                        continue

                if _has_existing_share_for_gap(candidate_gap, candidate=candidate, files=files):
                    continue

                share_resp = p115.share_create([root_fid], share_duration=-1, receive_code=None)
                if not share_resp or not share_resp.get('state'):
                    logger.warning(f"  ➜ [共享资源维护] 自动创建分享失败: {candidate.get('display_title')} -> {share_resp}")
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



def _cleanup_expired_virtual_cache(max_rows: int = 80) -> int:
    """删除已过期的虚拟入库临时转存文件，但保留虚拟 STRM/记录，后续播放可再次临时转存。"""
    p115 = P115Service.get_client()
    if not p115:
        logger.warning("  ➜ [共享资源维护] 115 客户端未初始化，跳过过期临时转存清理。")
        return 0

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT virtual_id, title, file_name, share_code, raw_json, real_fid, real_pick_code, real_parent_id, expires_at
                FROM shared_virtual_items
                WHERE status IN ('cached','watched')
                  AND COALESCE(real_fid, '') <> ''
                  AND expires_at IS NOT NULL
                  AND expires_at < NOW()
                ORDER BY expires_at ASC
                LIMIT %s
                """,
                (int(max_rows),),
            )
            rows = [dict(r) for r in cur.fetchall()]

    cleaned = 0
    cleaned_rows_for_history = []
    for row in rows:
        virtual_id = str(row.get('virtual_id') or '').strip()
        real_fid = str(row.get('real_fid') or '').strip()
        title = row.get('title') or row.get('file_name') or virtual_id
        if not virtual_id or not real_fid:
            continue

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

        # 无论远端是否已经被手动删除，只要确认过期，就清空本地 real_*，防止继续使用过期 pickcode。
        if delete_ok or resp is not None:
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE shared_virtual_items
                            SET status='virtual_ready',
                                real_fid='', real_pick_code='', real_parent_id='', expires_at=NULL,
                                last_error=%s,
                                raw_json = COALESCE(raw_json, '{}'::jsonb) || %s::jsonb,
                                updated_at=NOW()
                            WHERE virtual_id=%s
                            """,
                            (
                                '临时转存已过期，维护任务已清理；下次播放将重新转存',
                                json.dumps({'expired_cache_cleaned_at': time.strftime('%Y-%m-%d %H:%M:%S'), 'real_fid': real_fid, 'delete_response': resp}, ensure_ascii=False),
                                virtual_id,
                            ),
                        )
                        cur.execute("DELETE FROM p115_filesystem_cache WHERE id=%s", (real_fid,))
                    conn.commit()
                shared_virtual_db.add_credit_ledger(
                    'virtual_cache_expired_cleaned', 0,
                    f'清理过期虚拟临时转存：{title}',
                    ref_id=virtual_id,
                    virtual_id=virtual_id,
                    title=title,
                    raw_json={'real_fid': real_fid, 'delete_response': resp},
                )
                cleaned += 1
                cleaned_rows_for_history.append(row)
            except Exception as e:
                logger.warning(f"  ➜ [共享资源维护] 清空虚拟临时转存状态失败: {virtual_id} -> {e}")
        time.sleep(0.15)

    if cleaned_rows_for_history:
        try:
            _cleanup_recent_receive_history(p115, cleaned_rows_for_history)
        except Exception as e:
            logger.warning(f"  ➜ [共享资源维护] 清理 115 最近接收记录异常: {e}")

    if cleaned:
        logger.info(f"  ➜ [共享资源维护] 已清理 {cleaned} 个过期虚拟临时转存文件。")
    return cleaned


def _watching_missing_episodes(limit: int = 120) -> List[Dict[str, Any]]:
    """查询正在追更/暂停追更季下尚未入库的分集。"""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH watch_seasons AS (
                    SELECT
                        tmdb_id AS season_tmdb_id,
                        parent_series_tmdb_id,
                        season_number,
                        title AS season_title,
                        release_year,
                        watching_status,
                        last_updated_at
                    FROM media_metadata
                    WHERE item_type='Season'
                      AND watching_status IN ('Watching','Paused')
                      AND parent_series_tmdb_id IS NOT NULL
                      AND season_number IS NOT NULL
                )
                SELECT
                    e.tmdb_id,
                    e.item_type,
                    e.parent_series_tmdb_id,
                    e.season_number,
                    e.episode_number,
                    e.title,
                    e.release_year,
                    e.release_date,
                    ws.season_tmdb_id,
                    ws.season_title,
                    ws.watching_status
                FROM media_metadata e
                JOIN watch_seasons ws
                  ON e.item_type='Episode'
                 AND e.parent_series_tmdb_id = ws.parent_series_tmdb_id
                 AND e.season_number = ws.season_number
                WHERE COALESCE(e.in_library, FALSE) = FALSE
                  AND e.episode_number IS NOT NULL
                  AND COALESCE(e.subscription_status, 'NONE') NOT IN ('IGNORED')
                  AND (e.release_date IS NULL OR e.release_date <= CURRENT_DATE)
                ORDER BY ws.last_updated_at DESC NULLS LAST,
                         e.parent_series_tmdb_id, e.season_number, e.episode_number
                LIMIT %s
                """,
                (int(limit),),
            )
            return [dict(r) for r in cur.fetchall()]


def _has_local_virtual_projection_for_episode(row: Dict[str, Any]) -> bool:
    """避免维护任务反复为同一缺失分集创建虚拟 STRM。"""
    parent = str(row.get('parent_series_tmdb_id') or '')
    season = _safe_int(row.get('season_number'), -1)
    episode = _safe_int(row.get('episode_number'), -1)
    if not parent or season < 0 or episode < 0:
        return False
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1
                    FROM shared_virtual_items
                    WHERE status NOT IN ('deleted','promoted')
                      AND (parent_series_tmdb_id=%s OR tmdb_id=%s)
                      AND COALESCE(season_number, -1)=COALESCE(%s, -1)
                      AND COALESCE(episode_number, -1)=COALESCE(%s, -1)
                    LIMIT 1
                    """,
                    (parent, parent, season, episode),
                )
                return cur.fetchone() is not None
    except Exception:
        return False


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

    for row in rows:
        try:
            if mode == 'virtual' and _has_local_virtual_projection_for_episode(row):
                skipped += 1
                continue

            parent_tmdb = row.get('parent_series_tmdb_id')
            title = row.get('title') or row.get('season_title') or f"S{_safe_int(row.get('season_number'), 1):02d}E{_safe_int(row.get('episode_number'), 0):02d}"
            result = try_consume_shared_resource(
                row,
                title=title,
                tmdb_id=row.get('tmdb_id') or parent_tmdb,
                item_type='Episode',
                parent_tmdb_id=parent_tmdb,
                season_number=row.get('season_number'),
                year=row.get('release_year') or '',
            )
            if result.get('success'):
                consumed += 1
                logger.info(
                    "  ➜ [共享资源维护] 追更缺集命中中心资源并已%s：%s S%02dE%02d",
                    '虚拟入库' if result.get('mode') == 'virtual' else '永久转存',
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

def task_shared_resource_maintenance(processor=None, maintenance_silent: bool = False):
    """共享资源维护总任务。可由前端手动触发，也由调度器硬编码定时执行。

    maintenance_silent=True 时用于调度器后台静默执行：不输出成功/进度/摘要日志；
    未捕获异常仍会由 task_manager 以 ERROR 记录。
    """
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
                if not maintenance_silent:
                    logger.warning(f"  ➜ [共享资源维护] {msg}")
                _status(100, msg)
                return
        except Exception as e:
            msg = f"中心服务器连接超时或异常，为避免任务卡死，本次维护取消。"
            if not maintenance_silent:
                logger.warning(f"  ➜ [共享资源维护] {msg} ({e})")
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

        _status(60, '正在从中心资源库处理追更缺集...')
        follow_result = _auto_follow_watching_series_from_center()
        total.update({f'follow_{k}': v for k, v in follow_result.items()})

        _status(74, '正在同步分享审核状态并自动登记中心...')
        total.update(_auto_check_and_report_local_shares(client))

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

        msg = (
            f"共享资源维护完成：登记缺口 {total.get('reported_gaps', 0)}，"
            f"清理临时转存 {total.get('expired_virtual_cache_cleaned', 0)}，"
            f"自动创建分享 {total.get('auto_created_shares', 0)}，"
            f"违规分享清理 {total.get('share_invalid_deleted', 0)}/{total.get('share_invalid_failed', 0)}，"
            f"缺raw清理 {total.get('share_raw_missing_deleted', 0)}/{total.get('share_raw_missing_failed', 0)}，"
            f"水位清理 {total.get('share_pruned', 0)}/{total.get('share_prune_failed', 0)}，"
            f"追更命中 {total.get('follow_consumed', 0)}/{total.get('follow_missing', 0)}，"
            f"登记追更缺口 {total.get('follow_gaps', 0)}，"
            f"检查分享 {total.get('checked', 0)}，自动登记 {total.get('reported', 0)}，"
            f"清理失效 {total.get('cancelled', 0)}。"
        )
        if not maintenance_silent:
            logger.info(f"=== {msg} ===")
        _status(100, msg)
    finally:
        if old_logger_level is not None:
            logger.setLevel(old_logger_level)

def trigger_shared_resource_maintenance_task() -> bool:
    """供路由/调度器调用的统一入口。"""
    return task_manager.submit_task(
        task_shared_resource_maintenance,
        '共享资源自动维护',
        processor_type='media',
    )
