# tasks/shared_resource_tasks.py
# 共享资源自动维护任务：缺口登记、分享审核同步、中心登记、失效清理、中心缺口自动分享。
import json
import logging
import os
import re
import time
import threading
from typing import Dict, Any, List

import config_manager
import constants
import task_manager
from database import shared_share_db, shared_credit_db, settings_db
from database.connection import get_db_connection
from handler.p115_service import P115Service
from handler.shared_center_client import SharedCenterClient, shared_center_enabled, shared_resource_mode

logger = logging.getLogger(__name__)

def _shared_resource_switch_enabled() -> bool:
    """共享资源总开关。

    shared_center_enabled() 可能只代表中心地址/token 可用；自动创建 115 分享
    必须额外尊重独立配置 p115_shared_resource_enabled，用户关闭共享资源时
    不允许 webhook / watchlist / 维护任务主动创建任何分享。
    """
    try:
        cfg = settings_db.get_shared_resource_config() or {}
        value = cfg.get('p115_shared_resource_enabled', False)
        if isinstance(value, str):
            return value.strip().lower() in ('1', 'true', 'yes', 'on', '启用', '开启')
        return bool(value)
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 读取共享资源总开关失败，按未启用处理: {e}")
        return False

def _enabled() -> bool:
    return _shared_resource_switch_enabled() and shared_center_enabled()


def _cfg_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in ('1', 'true', 'yes', 'y', 'on', '启用', '开启'):
        return True
    if text in ('0', 'false', 'no', 'n', 'off', '停用', '关闭'):
        return False
    return bool(default)


def _shared_auto_share_requests_enabled() -> bool:
    """是否允许本机自动响应中心“求分享”。

    兼容两种配置名：
    - p115_shared_auto_share_requests_enabled：当前配置页保存的字段；
    - shared_auto_share_requests_enabled：早期/前端可能使用的短字段。
    """
    try:
        cfg = settings_db.get_shared_resource_config() or {}
        value = cfg.get('p115_shared_auto_share_requests_enabled')
        if value is None:
            value = cfg.get('shared_auto_share_requests_enabled')
        return _cfg_bool(value, False)
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 读取自动响应求分享开关失败，按未启用处理: {e}")
        return False

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
    """宽松兜底判断分享是否可用。

    只能在 _parse_share_status 不可用或解析失败时兜底使用。
    注意：115 的 share_info 在“审核中/处理中”时也可能 state=True，
    所以这里必须显式排除审核态，不能把 state=True 直接视为 alive。
    """
    if not _parse_share_ok(resp):
        return False

    text = _share_resp_text(resp)

    if _looks_share_blocked(resp):
        return False

    if any(k in text for k in (
        '处理中',
        '审核中',
        '待审核',
        '等待审核',
        '审核',
        'pending_review',
        'pending review',
        'pending',
        'processing',
        'reviewing',
        'under review',
    )):
        return False

    return not any(k in text for k in (
        '已取消',
        '已失效',
        '不存在',
        '取消分享',
        'expired',
        'cancelled',
        'canceled',
        'not found',
    ))

def _record_reportable(record: Dict[str, Any]) -> bool:
    return (record.get('status') in ('alive', 'reported') or record.get('review_status') == 'alive') and record.get('center_status') not in ('reported', 'partial')

def _record_raw_etk_dirty(record: Dict[str, Any]) -> bool:
    raw_json = record.get('raw_json') if isinstance(record.get('raw_json'), dict) else {}
    dirty = raw_json.get('raw_etk_dirty') if isinstance(raw_json, dict) else None
    return bool(isinstance(dirty, dict) and dirty.get('pending'))

def _clear_record_raw_etk_dirty(record: Dict[str, Any]) -> Dict[str, Any]:
    raw_json = dict(record.get('raw_json') if isinstance(record.get('raw_json'), dict) else {})
    dirty = raw_json.pop('raw_etk_dirty', None)
    if isinstance(dirty, dict):
        dirty['cleared_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
        raw_json['raw_etk_last_resync'] = dirty
    return raw_json

def _max_active_shares_limit() -> int:
    """本机 115 分享数量上限；0 表示不限制。"""
    return max(0, _safe_int(settings_db.get_shared_resource_config().get('p115_shared_max_active_shares', 0), 0))

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
        shared_credit_db.add_credit_ledger(
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
            shared_credit_db.add_credit_ledger(
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
            shared_credit_db.add_credit_ledger(
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
    """对账中心登记源与本地活动分享，自动撤销已经不在本地的中心残留源。

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
                    logger.error("  ➜ [共享资源维护] 连续 3 次网络请求失败，触发熔断，提前结束中心残留源对账。")
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
            logger.warning(f"  ➜ [共享资源维护] 撤销中心残留共享源失败: share={share_code}, err={e}")
        time.sleep(0.2)

    # 汇总日志由 task_shared_resource_maintenance 统一输出。这里不要再单独 logger.info，
    # 否则前端实时日志会出现一条“中心残留源对账完成”夹在总汇总外面。
    if cancelled or failed:
        logger.debug(
            "  ➜ [共享资源维护] 中心残留源对账阶段统计：检查 %s，跳过外部来源 %s，撤销 %s，失败 %s。",
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
                status='alive,pending,replenish,superseded,dead,reported,cancelled,expired,rejected',
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
                'superseded_sha1s': set(),
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
            if sha1 and status == 'superseded':
                group['superseded_sha1s'].add(sha1)
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
    superseded_sha1s = set(center.get('superseded_sha1s') or set())
    dead_sha1s = set(center.get('dead_sha1s') or set())

    if local_sha1s & superseded_sha1s:
        return 'center_superseded'
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
        'center_superseded': '中心服务器已由同季季包接管该单集分享',
    }.get(str(reason or ''), str(reason or ''))


def _record_episode_like(record: Dict[str, Any]) -> bool:
    share_type = str((record or {}).get('share_type') or '').strip().lower()
    item_type = str((record or {}).get('item_type') or '').strip().lower()
    return share_type in ('episode_file', 'episode', 'single') or item_type in ('episode', 'episode_file', 'single')


def _event_payload_value(event: Dict[str, Any], key: str, default=None):
    event = event or {}
    payload = event.get('payload') if isinstance(event.get('payload'), dict) else {}
    return event.get(key) if event.get(key) not in (None, '') else payload.get(key, default)


def _local_share_record_matches_superseded_event(record: Dict[str, Any], items: List[Dict[str, Any]], event: Dict[str, Any]) -> bool:
    record = record or {}
    items = items or []
    source_id = str(_event_payload_value(event, 'source_id', '') or '').strip()
    share_code = str(_event_payload_value(event, 'share_code', '') or '').strip()
    sha1 = str(_event_payload_value(event, 'sha1', '') or '').strip().upper()
    tmdb_id = str(_event_payload_value(event, 'tmdb_id', '') or '').strip()
    season = _safe_int(_event_payload_value(event, 'season_number', None), None)
    episode = _safe_int(_event_payload_value(event, 'episode_number', None), None)

    record_source_id = str(record.get('center_source_id') or '').strip()
    if source_id and record_source_id == source_id:
        return True
    for item in items:
        if source_id and str((item or {}).get('center_source_id') or '').strip() == source_id:
            return True

    record_share_code = str(record.get('share_code') or '').strip()
    if share_code and record_share_code == share_code:
        if sha1:
            for item in items:
                if str((item or {}).get('sha1') or '').strip().upper() == sha1:
                    return True
        if _record_episode_like(record):
            r_season = _safe_int(record.get('season_number'), None)
            r_episode = _safe_int(record.get('episode_number'), None)
            if season is None or r_season is None or r_season == season:
                if episode is None or r_episode is None or r_episode == episode:
                    return True
        # source_superseded 事件只会下发给源设备；share_code 命中时保守认为是同一个单集分享。
        return True

    if sha1:
        for item in items:
            if str((item or {}).get('sha1') or '').strip().upper() == sha1:
                if _record_episode_like(record):
                    return True

    if tmdb_id and _record_episode_like(record):
        record_parent = str(record.get('parent_series_tmdb_id') or record.get('tmdb_id') or '').strip()
        record_season = _safe_int(record.get('season_number'), None)
        record_episode = _safe_int(record.get('episode_number'), None)
        if record_parent == tmdb_id and record_season == season and (episode is None or record_episode == episode):
            return True
    return False


def _find_local_share_records_for_superseded_event(event: Dict[str, Any], max_pages: int = 20) -> List[Dict[str, Any]]:
    """根据中心 source_superseded 事件定位本机需要删除的单集分享记录。"""
    matches = []
    seen = set()
    active_statuses = set(_active_share_statuses())
    page_size = 200
    for page in range(1, max(1, int(max_pages or 20)) + 1):
        try:
            records, total = shared_share_db.list_share_records(status='all', keyword='', page=page, page_size=page_size)
        except Exception as e:
            logger.warning(f"  ➜ [共享事件监听] 查询本地分享记录失败，无法处理 source_superseded: {e}")
            break
        records = records or []
        if not records:
            break
        for record in records:
            record_id = record.get('id')
            if record_id in seen:
                continue
            status = str(record.get('status') or '').strip()
            review_status = str(record.get('review_status') or '').strip()
            if active_statuses and status not in active_statuses and review_status not in ('alive', 'pending_review'):
                continue
            try:
                items = shared_share_db.list_share_items(record_id) or []
            except Exception:
                items = []
            if _local_share_record_matches_superseded_event(record, items, event):
                seen.add(record_id)
                matches.append(record)
        try:
            if page * page_size >= int(total or 0):
                break
        except Exception:
            if len(records) < page_size:
                break
    return matches


def _cancel_superseded_local_share_record(
    client: SharedCenterClient,
    p115,
    record: Dict[str, Any],
    *,
    event: Dict[str, Any] | None = None,
    reason: str = 'source_superseded_by_season_pack',
) -> tuple[bool, str]:
    """删除已被中心季包接管的本机单集分享，并撤销中心历史源。"""
    record = record or {}
    record_id = record.get('id')
    share_code = str(record.get('share_code') or '').strip()
    title = record.get('title') or record.get('root_name') or share_code or str(record_id)
    p115_ok, p115_resp = _delete_115_share(p115, share_code)
    if not p115_ok:
        old_raw = record.get('raw_json') if isinstance(record.get('raw_json'), dict) else {}
        try:
            shared_share_db.update_share_record(
                record_id,
                status='cancel_failed',
                last_error=f'中心同季季包已接管该单集，但取消 115 分享失败：{p115_resp}',
                raw_json={
                    **dict(old_raw or {}),
                    'source_superseded_cancel_failed': {
                        'reason': reason,
                        'event': event or {},
                        'p115_response': p115_resp,
                        'failed_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                    },
                },
            )
        except Exception:
            pass
        return False, f'取消 115 分享失败：{p115_resp}'

    center_ok, center_resp = _cancel_center_sources_for_record(client, record_id, share_code, reason)
    _mark_share_deleted(
        record,
        p115_resp=p115_resp,
        center_resp=center_resp,
        center_ok=center_ok,
        reason=reason,
        last_error='中心已有同剧同季季包源，单集分享已被接管并自动清理',
        status='cancelled',
        review_status='superseded',
    )
    try:
        shared_credit_db.add_credit_ledger(
            'share_episode_cancelled_after_season_superseded',
            0,
            f'中心季包接管后取消单集分享：{title}',
            ref_id=str(record_id),
            title=title,
            raw_json={
                'share_code': share_code,
                'event': event or {},
                'center_ok': center_ok,
                'center_response': center_resp,
                'p115_response': p115_resp,
            },
        )
    except Exception:
        pass
    return True, '已删除本机 115 单集分享并撤销中心历史源' if center_ok else f'115 分享已删除，但中心撤销失败：{center_resp}'

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
            review_parsed = False
            if sr is not None and hasattr(sr, '_parse_share_status'):
                try:
                    review = sr._parse_share_status(snap) or {}
                    review_parsed = bool(review)
                except Exception:
                    review = {}
                    review_parsed = False

            review_status = str(review.get('status') or '').strip()

            if review_status == 'pending_review':
                # 115 审核中不等于可用，也不等于死链。
                # 不能再让 _looks_share_alive 用 state=True 把它兜底成 alive。
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

            if review_status in ('rejected', 'blocked', 'violation'):
                alive = False
            elif review_status == 'alive':
                alive = True
            else:
                # 只有 _parse_share_status 不可用/解析失败/没有明确状态时，才允许宽松兜底。
                alive = _looks_share_alive(snap) if not review_parsed else False
            if alive:
                old_raw_json = record.get('raw_json') if isinstance(record.get('raw_json'), dict) else {}
                raw_etk_dirty = _record_raw_etk_dirty(record)
                update = {
                    'status': 'alive',
                    'review_status': 'alive',
                    'last_checked_at': 'NOW()',
                    # 正常可用不再写入 last_error，避免前端“备注”误把成功状态当原因。
                    # 但 RAW 修复重传标记不能被这里清空，否则维护任务日志看不出正在处理什么。
                    'last_error': '本地 RAW _etk 已修复，等待覆盖中心 RAW' if raw_etk_dirty else '',
                    # 保留 auto_gap / manual_payload 等来源标记，只追加 last_snap。
                    'raw_json': {**old_raw_json, 'last_snap': snap},
                }
                shared_share_db.update_share_record(record['id'], **update)
                record = shared_share_db.get_share_record(record['id']) or record
                items = shared_share_db.list_share_items(record['id']) or []
                raw_etk_dirty = _record_raw_etk_dirty(record)
                sync_reason = _center_share_sync_reason(center_snapshot, share_code, items)
                if sync_reason == 'center_superseded':
                    ok, msg = _cancel_superseded_local_share_record(
                        client,
                        p115,
                        record,
                        event={'source': 'center_snapshot', 'sync_reason': sync_reason, 'share_code': share_code},
                        reason='center_snapshot_source_superseded',
                    )
                    if ok:
                        cancelled += 1
                        logger.info(f"  ➜ [共享资源维护] 中心季包已接管，已清理本地单集分享: share={share_code}")
                    else:
                        logger.warning(f"  ➜ [共享资源维护] 中心季包已接管，但清理本地单集分享失败: share={share_code}, {msg}")
                    continue

                need_report = _record_reportable(record) or bool(sync_reason) or raw_etk_dirty

                if need_report and sr is not None:
                    if sync_reason or raw_etk_dirty:
                        reason_text = _center_share_sync_reason_text(sync_reason) if sync_reason else '本地 RAW _etk 已修复'
                        logger.debug(f"  ➜ [共享资源维护] 本地分享需要重新同步中心: share={share_code}, reason={reason_text}")
                        shared_share_db.update_share_record(
                            record['id'],
                            center_status='not_reported',
                            last_error=(
                                f'{reason_text}，维护任务将重新上传 RAW 并登记中心'
                                if raw_etk_dirty else
                                f'{reason_text}，维护任务将重新登记中心'
                            ),
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
                                shared_credit_db.add_credit_ledger(
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
                            if raw_etk_dirty and not errors:
                                last_error = '本地 RAW _etk 修复后已覆盖中心并重新登记成功'
                            elif sync_reason and not errors:
                                last_error = f'中心同步补登成功：{_center_share_sync_reason_text(sync_reason)}'
                            update_fields = {
                                'center_status': center_status,
                                'status': 'reported' if center_status == 'reported' else record.get('status'),
                                'reported_count': ok,
                                'reported_at': 'NOW()',
                                'last_error': last_error,
                            }
                            if raw_etk_dirty and not errors:
                                update_fields['raw_json'] = _clear_record_raw_etk_dirty(record)
                            shared_share_db.update_share_record(record['id'], **update_fields)
                            reported += 1
                            if sync_reason or raw_etk_dirty:
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
                                    group.setdefault('superseded_sha1s', set()).difference_update(local_sha1s)
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

        shared_credit_db.add_credit_ledger(
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


def _center_blacklist_item_from_identity(identity: Dict[str, Any], fallback: Dict[str, Any] = None) -> Dict[str, Any]:
    identity = dict(identity or {})
    fallback = dict(fallback or {})
    item_type = identity.get('item_type') or fallback.get('share_item_type') or fallback.get('item_type') or fallback.get('share_type') or ''
    share_type = str(fallback.get('share_type') or identity.get('share_type') or '').strip().lower()
    if share_type in ('season_pack', 'tv_pack'):
        item_type = 'Season'
    elif share_type == 'series_pack':
        item_type = 'Series'
    elif share_type == 'episode_file':
        item_type = 'Episode'
    return {
        'tmdb_id': str(identity.get('parent_series_tmdb_id') or identity.get('tmdb_id') or fallback.get('parent_series_tmdb_id') or fallback.get('tmdb_id') or fallback.get('share_tmdb_id') or '').strip(),
        'item_type': item_type or ('Movie' if str(fallback.get('media_type') or '').lower() == 'movie' else 'Season'),
        'season_number': identity.get('season_number') if identity.get('season_number') not in (None, '') else fallback.get('season_number'),
        'episode_number': identity.get('episode_number') if identity.get('episode_number') not in (None, '') else fallback.get('episode_number'),
        'title': identity.get('title') or identity.get('standard_title') or fallback.get('standard_title') or fallback.get('title') or fallback.get('display_title') or '',
        'release_year': identity.get('release_year') or fallback.get('release_year'),
    }


def _center_resource_blacklisted(client: SharedCenterClient, item: Dict[str, Any]) -> Dict[str, Any]:
    try:
        resp = client.check_resource_blacklist(item=item)
        if resp.get('blacklisted'):
            return resp.get('first_match') or {'blacklisted': True, 'message': '命中中心黑名单'}
    except Exception as e:
        logger.warning(f"  ➜ [共享资源] 中心黑名单检查失败，为安全起见跳过自动分享: {e}")
        return {'blacklisted': True, 'message': f'中心黑名单检查失败：{e}'}
    return {}


def _report_center_resource_blacklist(client: SharedCenterClient, item: Dict[str, Any], resp: Any, reason: str = 'share_blocked') -> None:
    try:
        message = _share_resp_text(resp)
        client.report_resource_blacklist(item, reason=reason, message=message, source='auto_report')
        logger.info(f"  ➜ [共享资源] 已上报中心资源黑名单：{item.get('title') or item.get('tmdb_id')} reason={reason}")
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 上报中心资源黑名单失败: {e}")

def _flatten_center_search_items(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = []
    for block in (data or {}).get('results') or []:
        for row in block.get('items') or []:
            if isinstance(row, dict):
                items.append(row)
    return items

def _center_item_id_candidates(item: Dict[str, Any]) -> List[str]:
    return _dedupe_values(
        (item or {}).get('tmdb_id'),
        (item or {}).get('parent_series_tmdb_id'),
        (item or {}).get('series_tmdb_id'),
        (item or {}).get('parent_tmdb_id'),
    )

def _center_gap_matches_library_item(gap: Dict[str, Any], item: Dict[str, Any]) -> tuple[bool, str]:
    """判断中心 open gap 是否命中本次刚入库的 Movie/Episode。"""
    gap = gap or {}
    item = item or {}
    item_type = str(item.get('item_type') or '').strip()
    gap_type = str(gap.get('item_type') or '').strip()
    ids = set(_center_item_id_candidates(item))
    gap_tmdb = str(gap.get('tmdb_id') or '').strip()
    if not gap_tmdb or gap_tmdb not in ids:
        return False, ''

    if item_type == 'Movie':
        return gap_type == 'Movie', 'movie_gap_open' if gap_type == 'Movie' else ''

    if item_type == 'Episode':
        season = _safe_int(item.get('season_number'), None)
        episode = _safe_int(item.get('episode_number'), None)
        gap_season = _safe_int(gap.get('season_number'), None)
        gap_episode = _safe_int(gap.get('episode_number'), None)
        if season is None or episode is None:
            return False, ''
        if gap_type == 'Episode' and gap_season == season and gap_episode == episode:
            return True, 'episode_gap_open'
        if gap_type == 'Season' and gap_season == season:
            return True, 'season_gap_open'
    return False, ''

def _center_source_covers_library_item(src: Dict[str, Any], item: Dict[str, Any]) -> bool:
    src = src or {}
    item = item or {}
    item_type = str(item.get('item_type') or '').strip()
    src_type = str(src.get('item_type') or '').strip()
    ids = set(_center_item_id_candidates(item))
    src_tmdb = str(src.get('tmdb_id') or src.get('parent_series_tmdb_id') or '').strip()

    if item_type == 'Movie':
        return src_type == 'Movie' and (not ids or src_tmdb in ids)

    if item_type == 'Season':
        season = _safe_int(item.get('season_number'), None)
        src_season = _safe_int(src.get('season_number'), None)
        if season is None:
            return False
        if ids and src_tmdb and src_tmdb not in ids:
            return False
        # 主动备份季包只统计中心返回的季包源，不能把同季单集当作已有季包备份。
        return src_type == 'Season' and src_season == season

    if item_type == 'Episode':
        season = _safe_int(item.get('season_number'), None)
        episode = _safe_int(item.get('episode_number'), None)
        src_season = _safe_int(src.get('season_number'), None)
        src_episode = _safe_int(src.get('episode_number'), None)
        if season is None or episode is None:
            return False
        if ids and src_tmdb and src_tmdb not in ids:
            return False
        # 同季季包已经覆盖本集；精确单集源也覆盖本集。
        if src_type == 'Season' and src_season == season:
            return True
        if src_type == 'Episode' and src_season == season and src_episode == episode:
            return True
    return False

_CENTER_USABLE_SOURCE_STATUSES = {'', 'alive', 'pending', 'reported'}
_CENTER_UNUSABLE_SOURCE_STATUSES = {'dead', 'cancelled', 'canceled', 'expired', 'rejected', 'blocked', 'violation', 'deleted'}

def _center_source_is_usable(src: Dict[str, Any]) -> bool:
    """中心返回源是否仍可作为可用分享计数。"""
    status = str((src or {}).get('status') or '').strip().lower()
    if status in _CENTER_UNUSABLE_SOURCE_STATUSES:
        return False
    if status in _CENTER_USABLE_SOURCE_STATUSES:
        return True
    # 兼容旧中心：search_sources 一般只返回可消费源，未知状态不要直接当死链。
    return True

def _center_source_share_identity(src: Dict[str, Any]) -> str:
    src = src or {}
    return str(src.get('share_code') or src.get('source_id') or '').strip()

def _center_usable_share_count_for_item(client: SharedCenterClient, item: Dict[str, Any], *, limit: int = 200) -> Dict[str, Any]:
    """按 Movie/Season 精确查询中心可用分享数。

    这里统计“分享码”数量而不是文件行数量：季包在中心可能按包内文件展开多行，
    同一个 share_code 只能算一个可用分享。
    """
    if not hasattr(client, 'search_sources'):
        return {'ok': False, 'count': 0, 'reason': 'client_search_sources_missing'}

    query = _build_center_probe_query(item)
    if not query.get('tmdb_id') or not query.get('item_type'):
        return {'ok': False, 'count': 0, 'reason': 'invalid_center_query', 'query': query}

    try:
        data = client.search_sources([query], limit_per_item=limit)
        sources = _flatten_center_search_items(data)
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 查询中心可用分享数失败: item={item}, err={e}")
        return {'ok': False, 'count': 0, 'reason': 'search_sources_failed', 'message': str(e), 'query': query}

    usable = []
    seen = set()
    for src in sources or []:
        if not _center_source_is_usable(src):
            continue
        if not _center_source_covers_library_item(src, item):
            continue
        ident = _center_source_share_identity(src)
        if not ident:
            ident = f"source:{src.get('source_id') or len(seen)}:{src.get('sha1') or ''}"
        if ident in seen:
            continue
        seen.add(ident)
        usable.append(src)

    return {'ok': True, 'count': len(usable), 'sources': usable, 'query': query}

def _probe_backup_share_needed_for_library_item(client: SharedCenterClient, item: Dict[str, Any], *, target_count: int = 1) -> Dict[str, Any]:
    """主动备份探测：只有中心已有且仅有 target_count 个可用分享时才创建备份。

    count=0 表示中心没有可用分享，应交给“命中缺口”的正常分享逻辑；
    count=1 才补一个备份，尽量把分享池维持在至少两个可用分享；
    count>1 已有冗余，不再创建。
    """
    item = dict(item or {})
    item_type = str(item.get('item_type') or '').strip()
    if item_type not in ('Movie', 'Season'):
        return {'need_share': False, 'reason': 'backup_only_movie_or_season'}

    probe = _center_usable_share_count_for_item(client, item)
    if not probe.get('ok'):
        return {'need_share': False, **probe}

    target_count = int(target_count)
    count = _safe_int(probe.get('count'), 0)
    need = count == target_count
    if need:
        reason = 'center_available_share_eq_backup_target'
    elif count < target_count:
        reason = 'center_available_share_lt_backup_target'
    else:
        reason = 'center_available_share_gt_backup_target'
    return {
        **probe,
        'need_share': need,
        'reason': reason,
        'target_count': target_count,
        # 兼容旧日志字段，语义改为“备份目标计数”而不是 <= 阈值。
        'threshold': target_count,
        'available_share_count': count,
    }

def _build_center_probe_query(item: Dict[str, Any]) -> Dict[str, Any]:
    item = item or {}
    item_type = str(item.get('item_type') or '').strip()
    if item_type == 'Movie':
        return {
            'tmdb_id': str(item.get('tmdb_id') or ''),
            'item_type': 'Movie',
            'title': item.get('title') or None,
            'release_year': _safe_int(item.get('release_year'), None),
        }
    if item_type == 'Season':
        return {
            'tmdb_id': str(item.get('parent_series_tmdb_id') or item.get('series_tmdb_id') or item.get('tmdb_id') or ''),
            'item_type': 'Season',
            'season_number': _safe_int(item.get('season_number'), None),
            'episode_number': None,
            'title': item.get('title') or None,
            'release_year': _safe_int(item.get('release_year'), None),
        }
    if item_type == 'Episode':
        return {
            'tmdb_id': str(item.get('parent_series_tmdb_id') or item.get('series_tmdb_id') or item.get('tmdb_id') or ''),
            # 入库实时探测也按季查询中心源；中心是否需要“某一集”由 probe-needed
            # 或本地回退逻辑结合 Season gap + 同季已有源判断。
            'item_type': 'Season',
            'season_number': _safe_int(item.get('season_number'), None),
            'episode_number': None,
            'title': item.get('title') or None,
            'release_year': _safe_int(item.get('release_year'), None),
        }
    return {}

def _probe_share_needed_for_library_item(client: SharedCenterClient, item: Dict[str, Any]) -> Dict[str, Any]:
    """入库事件触发时询问中心是否需要本机分享。

    优先使用新版中心 probe 接口；旧中心回退到 open gaps + sources/search：
    - Movie：中心有 Movie 缺口且尚无 Movie 源。
    - Episode：中心有精确 Episode 缺口，或同剧同季 Season 缺口，且尚无同集源/同季季包。
    """
    item = dict(item or {})
    if not item.get('item_type'):
        return {'need_share': False, 'reason': 'missing_item_type'}

    if hasattr(client, 'probe_share_needed'):
        try:
            resp = client.probe_share_needed(item)
            if resp.get('supported', True):
                return resp
        except Exception as e:
            logger.debug(f"  ➜ [共享资源] 中心 probe-needed 不可用，回退 open gaps 判断: {e}")

    try:
        gaps = (client.list_open_gaps(limit=500).get('items') or [])
    except Exception as e:
        return {'need_share': False, 'reason': 'list_open_gaps_failed', 'message': str(e)}

    matched_gap = None
    matched_reason = ''
    for gap in gaps:
        ok, reason = _center_gap_matches_library_item(gap, item)
        if ok:
            matched_gap = gap
            matched_reason = reason
            break
    if not matched_gap:
        return {'need_share': False, 'reason': 'no_matching_open_gap'}

    query = _build_center_probe_query(item)
    if query.get('tmdb_id'):
        try:
            data = client.search_sources([query], limit_per_item=200)
            sources = _flatten_center_search_items(data)
            for src in sources:
                if _center_source_covers_library_item(src, item):
                    return {
                        'need_share': False,
                        'reason': 'center_source_already_covers_item',
                        'matched_gap': matched_gap,
                        'source_id': src.get('source_id'),
                    }
        except Exception as e:
            logger.debug(f"  ➜ [共享资源] 查询中心已有源失败，仍按 open gap 尝试创建分享: {e}")

    return {'need_share': True, 'reason': matched_reason or 'open_gap', 'matched_gap': matched_gap}

def _collect_auto_share_files_for_candidate(sr, p115, gap: Dict[str, Any], candidate: Dict[str, Any], standard_identity: Dict[str, Any]) -> tuple[List[Dict[str, Any]], str]:
    root_fid = str(candidate.get('root_fid') or '').strip()
    root_name = candidate.get('root_name') or standard_identity.get('title') or root_fid
    root_is_dir = candidate.get('root_is_dir') is not False

    files = sr._collect_files_from_115(p115, root_fid, root_name=root_name, max_depth=8, assume_dir=root_is_dir)
    if not files:
        payload = {
            'tmdb_id': candidate.get('share_tmdb_id') or candidate.get('tmdb_id') or gap.get('tmdb_id'),
            'item_type': candidate.get('share_item_type') or candidate.get('item_type') or gap.get('item_type'),
            'parent_series_tmdb_id': candidate.get('parent_series_tmdb_id') or gap.get('parent_series_tmdb_id'),
            'season_number': candidate.get('season_number', gap.get('season_number')),
            'episode_number': candidate.get('episode_number', gap.get('episode_number')),
            'title': candidate.get('display_title') or candidate.get('title') or root_name,
            'root_name': root_name,
        }
        files = sr._collect_files_from_media_payload(payload)

    share_type_now = str(candidate.get('share_type') or '').strip().lower()
    for item in files or []:
        item.setdefault('tmdb_id', str(candidate.get('share_tmdb_id') or candidate.get('tmdb_id') or gap.get('tmdb_id') or ''))
        if share_type_now == 'episode_file':
            item['item_type'] = 'Episode'
            item.setdefault('season_number', candidate.get('season_number', gap.get('season_number')))
            item.setdefault('episode_number', candidate.get('episode_number', gap.get('episode_number')))
        elif share_type_now == 'season_pack':
            item.setdefault('item_type', 'Episode' if item.get('episode_number') else 'Season')
            item.setdefault('season_number', candidate.get('season_number', gap.get('season_number')))
        else:
            item.setdefault('item_type', candidate.get('share_item_type') or candidate.get('item_type') or gap.get('item_type') or 'Movie')
            item.setdefault('season_number', candidate.get('season_number', gap.get('season_number')))
            item.setdefault('episode_number', candidate.get('episode_number', gap.get('episode_number')))

    if not files:
        return [], '未能定位到可分享的视频文件'

    if hasattr(sr, '_files_missing_raw_ffprobe'):
        missing_raw = sr._files_missing_raw_ffprobe(files)
        if missing_raw:
            return [], sr._raw_missing_message(missing_raw) if hasattr(sr, '_raw_missing_message') else f'缺少 raw_ffprobe_json：{missing_raw}'

    if share_type_now in ('season_pack', 'series_pack') and hasattr(sr, '_validate_season_pack_consistency'):
        consistency = sr._validate_season_pack_consistency(files, {**candidate, **gap, **standard_identity})
        if not consistency.get('ok'):
            return [], consistency.get('message') or '包内媒体参数不一致'
    return files, ''

def _create_auto_share_for_single_gap(client: SharedCenterClient, gap: Dict[str, Any], *, trigger: str = 'center_gap') -> Dict[str, Any]:
    """对单个中心需求创建本地 115 分享，后续由高频状态同步登记中心。"""
    result = {'created': 0, 'skipped': 0, 'failed': 0, 'message': ''}
    gap = dict(gap or {})

    if not _enabled():
        result['skipped'] += 1
        result['message'] = '共享资源未启用，跳过自动创建分享'
        return result

    p115 = P115Service.get_client()
    if not p115:
        result['message'] = '115 客户端未初始化'
        return result

    try:
        from routes import shared_resource as sr
    except Exception as e:
        result['message'] = f'无法加载分享辅助函数：{e}'
        result['failed'] += 1
        return result

    if str(gap.get('item_type') or '').strip() in ('Movie', 'Episode') and _has_existing_share_for_gap(gap):
        result['skipped'] += 1
        result['message'] = '本地已有活动分享或硬失败黑名单'
        return result

    row = _find_local_media_for_gap(gap)
    if not row:
        result['skipped'] += 1
        result['message'] = '本地未找到已入库媒体'
        return result

    candidates = sr._expand_share_candidates(row) if hasattr(sr, '_expand_share_candidates') else [sr._build_media_candidate(row)]
    for candidate in candidates or []:
        try:
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
                result['skipped'] += 1
                continue

            # Webhook 触发的 Episode 只创建单集分享，不能因为中心有季缺口就创建季包。
            if trigger == 'library_webhook' and str(gap.get('item_type') or '') == 'Episode':
                if str(candidate.get('share_type') or '').strip().lower() != 'episode_file':
                    continue

            if candidate.get('share_type') == 'season_pack' and hasattr(sr, '_share_policy_for_media'):
                policy = sr._share_policy_for_media({
                    'item_type': 'Season',
                    'tmdb_id': candidate.get('parent_series_tmdb_id') or candidate.get('tmdb_id'),
                    'parent_series_tmdb_id': candidate.get('parent_series_tmdb_id') or candidate.get('tmdb_id'),
                    'season_number': candidate.get('season_number'),
                })
                if not policy.get('allowed'):
                    continue

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

            root_fid = str(candidate.get('root_fid'))
            root_name = candidate.get('root_name') or standard_identity.get('title') or root_fid
            root_is_dir = candidate.get('root_is_dir') is not False
            files, file_error = _collect_auto_share_files_for_candidate(sr, p115, candidate_gap, candidate, standard_identity)
            if file_error:
                result['skipped'] += 1
                result['message'] = file_error
                logger.info(f"  ➜ [共享资源] 自动分享跳过：{candidate.get('display_title') or root_name} -> {file_error}")
                continue

            if _has_existing_share_for_gap(candidate_gap, candidate=candidate, files=files):
                result['skipped'] += 1
                continue

            blacklist_item = _center_blacklist_item_from_identity(standard_identity, candidate)
            blacklist_hit = _center_resource_blacklisted(client, blacklist_item)
            if blacklist_hit:
                result['skipped'] += 1
                result['message'] = blacklist_hit.get('message') or '命中中心黑名单，跳过自动分享'
                logger.info(f"  ➜ [共享资源] 命中中心黑名单，跳过自动分享：{blacklist_item.get('title') or blacklist_item.get('tmdb_id')}")
                continue

            share_resp = p115.share_create([root_fid], share_duration=-1, receive_code=None)
            if not share_resp or not share_resp.get('state'):
                if _looks_share_source_missing(share_resp):
                    _blacklist_auto_gap_candidate(candidate_gap, candidate, files, root_fid=root_fid, root_name=root_name, share_resp=share_resp, reason='source_missing')
                elif _looks_share_blocked(share_resp):
                    _blacklist_auto_gap_candidate(candidate_gap, candidate, files, root_fid=root_fid, root_name=root_name, share_resp=share_resp, reason='share_blocked')
                    _report_center_resource_blacklist(client, blacklist_item, share_resp, reason='share_blocked')
                result['failed'] += 1
                result['message'] = f'创建 115 分享失败：{share_resp}'
                continue

            data = share_resp.get('data') or {}
            share_code = data.get('share_code') or share_resp.get('share_code')
            receive_code = data.get('receive_code') or ''
            share_url = data.get('share_url') or (f"https://115.com/s/{share_code}" if share_code else '')
            is_backup_share = str(trigger or '').strip().lower() in ('library_backup', 'season_backup', 'completed_season_backup')
            raw_json = {
                'auto_gap': gap if not is_backup_share else None,
                'auto_share_trigger': trigger,
                'share_response': share_resp,
                'candidate': candidate,
                'standard_identity': standard_identity,
            }
            if is_backup_share:
                raw_json.update({
                    'auto_gap': None,
                    'auto_backup_share': True,
                    'backup_share': True,
                    'backup_mirror': True,
                    'source_provider': 'backup_mirror',
                    'share_source': 'backup_mirror',
                    'source_provider_label': '备份分享',
                    'source_label': '备份分享',
                    'backup_reason': gap.get('backup_reason') or gap.get('reason') or 'center_available_share_eq_backup_target',
                    'center_available_share_count': gap.get('center_available_share_count'),
                    'center_backup_probe': gap.get('center_backup_probe'),
                })
            record = shared_share_db.create_share_record({
                'share_code': share_code,
                'receive_code': receive_code,
                'share_url': share_url,
                'share_type': candidate.get('share_type') or 'movie_folder',
                'root_fid': root_fid,
                'root_name': root_name,
                'root_is_dir': root_is_dir,
                'tmdb_id': str(standard_identity.get('tmdb_id') or candidate.get('share_tmdb_id') or candidate.get('tmdb_id') or ''),
                'item_type': candidate.get('share_item_type') or candidate.get('item_type') or gap.get('item_type') or 'Movie',
                'parent_series_tmdb_id': standard_identity.get('parent_series_tmdb_id') or candidate.get('parent_series_tmdb_id') or gap.get('parent_series_tmdb_id'),
                'season_number': candidate.get('season_number', gap.get('season_number')),
                'episode_number': candidate.get('episode_number', gap.get('episode_number')),
                'title': standard_identity.get('title') or candidate.get('standard_title') or candidate.get('title') or root_name,
                'release_year': standard_identity.get('release_year') or candidate.get('release_year') or gap.get('release_year'),
                'status': 'pending_review',
                'review_status': 'pending_review',
                'center_status': 'not_reported',
                'raw_json': raw_json,
            })
            shared_share_db.replace_share_items(record['id'], files)
            shared_credit_db.add_credit_ledger(
                'share_backup_mirror_created' if is_backup_share else 'share_auto_created_for_gap', 0,
                '主动创建备份分享，等待115审核' if is_backup_share else '命中中心需求并自动创建115分享，等待审核',
                ref_id=str(record['id']), title=record.get('title') or '',
                raw_json={'gap': gap, 'share_code': share_code, 'trigger': trigger, 'backup_share': is_backup_share},
            )
            logger.info(
                "  ➜ [共享资源] 已%s，等待 115 审核：%s S%sE%s share=%s trigger=%s",
                '主动创建备份分享' if is_backup_share else '自动创建分享',
                record.get('title') or root_name,
                record.get('season_number'),
                record.get('episode_number'),
                share_code,
                trigger,
            )
            result['created'] += 1
            result['record_id'] = record.get('id')
            result['share_code'] = share_code
            return result
        except Exception as e:
            logger.warning(f"  ➜ [共享资源] 自动创建单个分享失败: gap={gap}, candidate={candidate} -> {e}", exc_info=True)
            result['failed'] += 1
            result['message'] = str(e)
    return result

def trigger_shared_auto_share_for_library_item(
    processor=None,
    *,
    item_type: str = '',
    tmdb_id: str = '',
    emby_item_id: str = '',
    parent_series_tmdb_id: str = '',
    season_number=None,
    episode_number=None,
    title: str = '',
    year='',
) -> Dict[str, Any]:
    """Movie/Episode 入库成功后的供给侧实时分享入口。原正常分享逻辑不变，Movie 额外补备份。"""
    if not _enabled():
        return {'enabled': False, 'created': 0}
    client = SharedCenterClient()
    if not client.ready:
        return {'enabled': True, 'created': 0, 'message': '共享中心未配置'}

    row = {}
    if emby_item_id:
        try:
            row = shared_share_db.find_media_by_emby_item_id(str(emby_item_id), item_type=item_type) or {}
        except Exception as e:
            logger.debug(f"  ➜ [共享资源] 按 Emby ID 查询入库媒体失败: {emby_item_id} -> {e}")
    if not row and tmdb_id and item_type:
        row = {
            'item_type': item_type,
            'tmdb_id': str(tmdb_id),
            'parent_series_tmdb_id': str(parent_series_tmdb_id or ''),
            'season_number': season_number,
            'episode_number': episode_number,
            'title': title,
            'release_year': year,
        }

    item_type = str(row.get('item_type') or item_type or '').strip()
    if item_type not in ('Movie', 'Episode'):
        return {'enabled': True, 'created': 0, 'message': f'只处理 Movie/Episode，当前 {item_type or "-"}'}

    item = {
        'item_type': item_type,
        'tmdb_id': str(row.get('tmdb_id') or tmdb_id or ''),
        'parent_series_tmdb_id': str(row.get('parent_series_tmdb_id') or parent_series_tmdb_id or ''),
        'season_number': row.get('season_number') if row.get('season_number') not in (None, '') else season_number,
        'episode_number': row.get('episode_number') if row.get('episode_number') not in (None, '') else episode_number,
        'title': row.get('title') or title,
        'release_year': row.get('release_year') or year,
    }
    if item_type == 'Episode':
        parent_id = item.get('parent_series_tmdb_id') or item.get('tmdb_id')
        if not parent_id or item.get('season_number') in (None, '') or item.get('episode_number') in (None, ''):
            return {'enabled': True, 'created': 0, 'message': 'Episode 缺少父剧/季/集标识'}
    elif not item.get('tmdb_id'):
        return {'enabled': True, 'created': 0, 'message': 'Movie 缺少 TMDb ID'}

    # 第一段：原有逻辑不变。只有命中中心缺口时，才创建正常分享；Episode 追更分享也走这里。
    probe = _probe_share_needed_for_library_item(client, item)
    if probe.get('need_share'):
        gap = probe.get('matched_gap') or {}
        # 即使命中的是 Season 缺口，也只对本次入库的 Episode 创建单集分享。
        if item_type == 'Episode':
            gap = {
                **gap,
                'tmdb_id': item.get('parent_series_tmdb_id') or item.get('tmdb_id'),
                'item_type': 'Episode',
                'season_number': item.get('season_number'),
                'episode_number': item.get('episode_number'),
                'title': item.get('title'),
                'release_year': item.get('release_year'),
            }
        else:
            gap = {
                **gap,
                'tmdb_id': item.get('tmdb_id'),
                'item_type': 'Movie',
                'title': item.get('title'),
                'release_year': item.get('release_year'),
            }
        return _create_auto_share_for_single_gap(client, gap, trigger='library_webhook')

    logger.debug(
        "  ➜ [共享资源] 入库实时正常分享跳过：%s S%sE%s reason=%s",
        item.get('title') or item.get('tmdb_id'), item.get('season_number'), item.get('episode_number'), probe.get('reason')
    )

    # 第二段：只给 Movie 增量补备份。Episode 不创建备份分享，避免短寿命单集备份泛滥。
    if item_type != 'Movie':
        return {'enabled': True, 'created': 0, 'probe': probe}

    backup_probe = _probe_backup_share_needed_for_library_item(client, {
        'item_type': 'Movie',
        'tmdb_id': item.get('tmdb_id'),
        'title': item.get('title'),
        'release_year': item.get('release_year'),
    })
    if not backup_probe.get('need_share'):
        logger.debug(
            "  ➜ [共享资源] 主动备份电影跳过：%s reason=%s available=%s target=%s",
            item.get('title') or item.get('tmdb_id'),
            backup_probe.get('reason'),
            backup_probe.get('available_share_count', backup_probe.get('count')),
            backup_probe.get('target_count', backup_probe.get('threshold')),
        )
        return {'enabled': True, 'created': 0, 'probe': probe, 'backup_probe': backup_probe}

    gap = {
        'tmdb_id': item.get('tmdb_id'),
        'item_type': 'Movie',
        'title': item.get('title'),
        'release_year': item.get('release_year'),
        'backup_reason': backup_probe.get('reason'),
        'center_available_share_count': backup_probe.get('available_share_count', backup_probe.get('count')),
        'center_backup_probe': {
            'reason': backup_probe.get('reason'),
            'available_share_count': backup_probe.get('available_share_count', backup_probe.get('count')),
            'target_count': backup_probe.get('target_count'),
            'query': backup_probe.get('query'),
        },
    }
    return _create_auto_share_for_single_gap(client, gap, trigger='library_backup')

def _active_episode_share_statuses_for_rollup() -> List[str]:
    """完结汇总只处理仍然占用分享名额/中心源的活动单集分享。"""
    return _active_share_statuses()

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

def _load_active_episode_share_records_for_season(parent_series_tmdb_id: str, season_number) -> List[Dict[str, Any]]:
    """加载同剧同季仍活动的单集分享，用于季包创建/已存在季包后的清理。"""
    parent_series_tmdb_id = str(parent_series_tmdb_id or '').strip()
    try:
        season_number = int(season_number)
    except Exception:
        return []
    if not parent_series_tmdb_id:
        return []

    statuses = _active_episode_share_statuses_for_rollup()
    try:
        return shared_share_db.get_active_episode_share_records_for_season(
            parent_series_tmdb_id,
            season_number,
            statuses,
            include_rollup_blocked=True,
        ) or []
    except AttributeError:
        # 兼容未更新 DB 层的临时环境，退回旧的完结季分组查询。
        groups = _load_completed_season_episode_share_groups(limit=80, include_rollup_blocked=True)
        for group in groups:
            if str(group.get('parent_series_tmdb_id') or '').strip() == parent_series_tmdb_id and _safe_int(group.get('season_number'), None) == season_number:
                return group.get('episode_records') or []
    except Exception as e:
        logger.warning(
            "  ➜ [共享资源] 查询完结季同季单集分享失败: parent=%s S%s -> %s",
            parent_series_tmdb_id, season_number, e, exc_info=True,
        )
    return []

def _prepare_season_pack_files(sr, p115, candidate: Dict[str, Any], standard_identity: Dict[str, Any]) -> tuple[List[Dict[str, Any]], str, Dict[str, Any]]:
    """完结季包文件定位：直接查库定位季目录。

    路线固定为：media_metadata 的 Episode 行 -> PC/SHA1 -> p115_filesystem_cache -> parent_id。
    parent_id 就是 115 季目录，创建分享时直接分享这个目录，字幕/NFO 等非视频文件自然跟随。
    """
    candidate = dict(candidate or {})
    standard_identity = dict(standard_identity or {})
    target_season = _safe_int(candidate.get('season_number') or standard_identity.get('season_number'), None)
    parent_series_id = str(
        standard_identity.get('parent_series_tmdb_id') or
        candidate.get('parent_series_tmdb_id') or
        candidate.get('share_tmdb_id') or
        candidate.get('tmdb_id') or ''
    ).strip()
    if not parent_series_id or target_season is None:
        return [], '季包文件定位失败：缺少父剧 TMDb ID 或季号', {
            'reason': 'season_pack_identity_missing',
            'parent_series_tmdb_id': parent_series_id,
            'season_number': target_season,
        }

    json_values = getattr(sr, '_json_array_values', lambda v: v if isinstance(v, list) else ([v] if v else []))
    norm_pc = getattr(sr, '_norm_pc_list', lambda values: [str(x).strip() for x in values or [] if str(x or '').strip()])
    norm_sha1 = getattr(sr, '_norm_sha1_list', lambda values: [str(x).strip().upper() for x in values or [] if str(x or '').strip()])
    safe_size = getattr(sr, '_safe_size_bytes', _safe_int)
    looks_video = getattr(sr, '_looks_like_video_name', lambda name: os.path.splitext(str(name or '').lower())[1] in {'.mp4', '.mkv', '.avi', '.ts', '.mov', '.m2ts', '.iso', '.wmv', '.flv'})

    try:
        episode_rows = shared_share_db.get_episode_rows_by_season(parent_series_id, target_season) or []
    except Exception as e:
        return [], f'季包文件定位失败：查询 Episode 行失败：{e}', {
            'reason': 'episode_rows_query_failed',
            'parent_series_tmdb_id': parent_series_id,
            'season_number': target_season,
        }

    pc_to_episode: Dict[str, Dict[str, Any]] = {}
    sha1_to_episode: Dict[str, Dict[str, Any]] = {}
    pickcodes: List[str] = []
    sha1s: List[str] = []
    for ep in episode_rows:
        if not ep or not ep.get('in_library'):
            continue
        for pc in norm_pc(json_values(ep.get('file_pickcode_json'))):
            pc_to_episode.setdefault(pc, dict(ep))
            if pc not in pickcodes:
                pickcodes.append(pc)
        for sha1 in norm_sha1(json_values(ep.get('file_sha1_json'))):
            sha1_to_episode.setdefault(sha1, dict(ep))
            if sha1 not in sha1s:
                sha1s.append(sha1)

    if not pickcodes and not sha1s:
        return [], f'季包文件定位失败：S{target_season:02d} 的 Episode 行没有 PC/SHA1', {
            'reason': 'episode_identifiers_missing',
            'parent_series_tmdb_id': parent_series_id,
            'season_number': target_season,
            'episode_rows': len(episode_rows),
        }

    try:
        cache_rows = shared_share_db.get_p115_file_rows_by_pc_sha1(pickcodes, sha1s) or []
    except Exception as e:
        return [], f'季包文件定位失败：查询 p115_filesystem_cache 失败：{e}', {
            'reason': 'p115_cache_query_failed',
            'parent_series_tmdb_id': parent_series_id,
            'season_number': target_season,
            'pickcodes': len(pickcodes),
            'sha1s': len(sha1s),
        }

    matched_rows: List[Dict[str, Any]] = []
    for row in cache_rows:
        row = dict(row or {})
        name = str(row.get('name') or '')
        if not looks_video(name):
            continue
        pc = str(row.get('pick_code') or '').strip()
        sha1 = str(row.get('sha1') or '').strip().upper()
        ep = pc_to_episode.get(pc) or sha1_to_episode.get(sha1) or {}
        if not ep:
            continue
        parent_id = str(row.get('parent_id') or '').strip()
        fid = str(row.get('id') or '').strip()
        if not parent_id or not fid:
            continue
        matched_rows.append({'row': row, 'episode': ep, 'parent_id': parent_id, 'fid': fid})

    if not matched_rows:
        return [], f'季包文件定位失败：S{target_season:02d} 未能通过 PC/SHA1 在 p115_filesystem_cache 反查到视频文件', {
            'reason': 'p115_cache_no_video_match',
            'parent_series_tmdb_id': parent_series_id,
            'season_number': target_season,
            'pickcodes': len(pickcodes),
            'sha1s': len(sha1s),
            'cache_rows': len(cache_rows),
        }

    def _season_dir_matches(name: str) -> bool:
        try:
            if hasattr(sr, '_season_dir_name_matches') and sr._season_dir_name_matches(name, target_season):
                return True
        except Exception:
            pass
        text = str(name or '').strip().lower().replace('_', ' ').replace('-', ' ')
        compact = re.sub(r'\s+', '', text)
        return compact in {
            f's{target_season}', f's{target_season:02d}',
            f'season{target_season}', f'season{target_season:02d}',
            f'第{target_season}季',
        }

    groups: Dict[str, Dict[str, Any]] = {}
    for item in matched_rows:
        parent_id = item['parent_id']
        group = groups.setdefault(parent_id, {'parent_id': parent_id, 'rows': [], 'episodes': set(), 'parent_name': parent_id, 'season_dir': False})
        group['rows'].append(item)
        ep = item.get('episode') or {}
        ep_no = _safe_int(ep.get('episode_number'), None)
        if ep_no is not None:
            group['episodes'].add(ep_no)

    for group in groups.values():
        node = shared_share_db.get_p115_node_by_id(group['parent_id']) or {}
        group['parent_name'] = str(node.get('name') or group['parent_id'])
        group['season_dir'] = _season_dir_matches(group['parent_name'])

    # 同一 PC/SHA1 在 p115_filesystem_cache 里可能有旧路径残留，不能先按集去重。
    # 必须先按 parent_id 分组，优先选择目录名明确匹配 Sxx/Season xx/第x季 的父目录。
    selected = sorted(
        groups.values(),
        key=lambda g: (1 if g.get('season_dir') else 0, len(g.get('episodes') or set()), len(g.get('rows') or [])),
        reverse=True,
    )[0]
    parent_id = str(selected.get('parent_id') or '').strip()
    parent_name = str(selected.get('parent_name') or parent_id)
    parent_candidates = [
        {
            'parent_id': g.get('parent_id'),
            'parent_name': g.get('parent_name'),
            'season_dir': bool(g.get('season_dir')),
            'episode_count': len(g.get('episodes') or set()),
            'row_count': len(g.get('rows') or []),
        }
        for g in sorted(groups.values(), key=lambda x: len(x.get('episodes') or set()), reverse=True)
    ]
    remote_season_dir = None
    cache_file_parent_fix_count = 0
    cache_file_parent_fix_error = ''

    def _p115_node_name(node: Dict[str, Any]) -> str:
        return str(
            (node or {}).get('fn') or
            (node or {}).get('n') or
            (node or {}).get('file_name') or
            (node or {}).get('name') or
            (node or {}).get('title') or
            ''
        ).strip()

    def _p115_node_id(node: Dict[str, Any]) -> str:
        return str(
            (node or {}).get('fid') or
            (node or {}).get('file_id') or
            (node or {}).get('id') or
            (node or {}).get('cid') or
            ''
        ).strip()

    def _p115_node_is_dir(node: Dict[str, Any]) -> bool:
        node = node or {}
        fc = node.get('fc') if node.get('fc') is not None else node.get('type') if node.get('type') is not None else node.get('file_category')
        if str(fc) == '0':
            return True
        if str(fc) == '1':
            return False
        if node.get('is_dir') or node.get('is_folder') or node.get('is_directory'):
            return True
        # 兼容 115 Cookie 目录项：目录经常只有 cid/name/pid，没有 sha1/pc/size。
        return bool(node.get('cid') and not (node.get('fid') or node.get('pc') or node.get('pick_code') or node.get('sha1') or node.get('size') or node.get('fs')))

    def _remote_find_child_season_dir(base_cid: str) -> Dict[str, Any]:
        base_cid = str(base_cid or '').strip()
        if not base_cid or not p115:
            return {}

        search_names = list(dict.fromkeys([
            f'Season {target_season:02d}',
            f'Season {target_season}',
            f'S{target_season:02d}',
            f'S{target_season}',
            f'第{target_season}季',
        ]))

        def _scan(resp):
            for node in (resp or {}).get('data') or []:
                if not _p115_node_is_dir(node):
                    continue
                name = _p115_node_name(node)
                cid = _p115_node_id(node)
                if cid and _season_dir_matches(name):
                    return {'id': cid, 'name': name, 'raw': node}
            return {}

        # 先精准 search_value，避免父目录条目过多时扫不到。
        for name in search_names:
            try:
                found = _scan(p115.fs_files({
                    'cid': base_cid,
                    'search_value': name,
                    'limit': 100,
                    'show_dir': 1,
                    'record_open_time': 0,
                    'count_folders': 0,
                }))
                if found:
                    return found
            except Exception as e:
                logger.debug(
                    "  ➜ [共享资源] 远程回查季目录失败: base=%s, name=%s, err=%s",
                    base_cid, name, e,
                )

        # search_value 失效时，兜底扫一级子目录。
        try:
            return _scan(p115.fs_files({
                'cid': base_cid,
                'limit': 1000,
                'show_dir': 1,
                'record_open_time': 0,
                'count_folders': 0,
            }))
        except Exception as e:
            logger.debug("  ➜ [共享资源] 远程扫描季目录失败: base=%s, err=%s", base_cid, e)
            return {}

    if not selected.get('season_dir'):
        # p115_filesystem_cache 可能出现脏数据：文件 parent_id 仍指向剧目录，
        # 但 local_path/115 实际已经在 Season 03 子目录中。此时现场查 115 一级子目录，
        # 找到明确的季目录后直接分享该目录。
        search_base_ids = []
        for value in (parent_id, candidate.get('root_fid')):
            value = str(value or '').strip()
            if value and value not in search_base_ids:
                search_base_ids.append(value)

        for base_id in search_base_ids:
            remote_season_dir = _remote_find_child_season_dir(base_id)
            if remote_season_dir:
                old_parent_id, old_parent_name = parent_id, parent_name
                parent_id = str(remote_season_dir.get('id') or '').strip()
                parent_name = str(remote_season_dir.get('name') or parent_id)
                selected['season_dir'] = True
                parent_candidates.insert(0, {
                    'parent_id': parent_id,
                    'parent_name': parent_name,
                    'season_dir': True,
                    'episode_count': len(selected.get('episodes') or set()),
                    'row_count': len(selected.get('rows') or []),
                    'source': 'remote_115_child_dir_fallback',
                    'fallback_from_parent_id': old_parent_id,
                    'fallback_from_parent_name': old_parent_name,
                })
                try:
                    from handler.p115_service import P115CacheManager

                    # 回填季目录节点本身；如果父目录有 local_path，也顺手补齐 Season 目录路径。
                    P115CacheManager.save_cid(parent_id, base_id, parent_name)
                    season_local_path = None
                    try:
                        base_local_path = P115CacheManager.get_local_path(base_id)
                        if base_local_path:
                            season_local_path = f"{str(base_local_path).strip('/').rstrip('/')}/{parent_name}".replace('\\', '/')
                            P115CacheManager.update_local_path(parent_id, season_local_path)
                    except Exception:
                        season_local_path = None

                    # 关键兜底：不只回填 Season 03 目录，还要把本季视频文件的 parent_id
                    # 从脏的剧目录修正为真正的季目录。否则下次查库仍会把视频当作剧目录直属文件。
                    for dirty in selected.get('rows') or []:
                        cache_row = dict((dirty or {}).get('row') or {})
                        fid = str(cache_row.get('id') or '').strip()
                        name = str(cache_row.get('name') or '').strip()
                        if not fid or not name:
                            continue
                        if str(cache_row.get('parent_id') or '').strip() == parent_id:
                            continue
                        local_path = cache_row.get('local_path')
                        if not local_path and season_local_path:
                            local_path = f"{season_local_path.rstrip('/')}/{name}"

                        P115CacheManager.save_file_cache(
                            fid,
                            parent_id,
                            name,
                            sha1=cache_row.get('sha1'),
                            pick_code=cache_row.get('pick_code'),
                            local_path=local_path,
                            size=_safe_int(cache_row.get('size'), 0),
                        )
                        cache_row['parent_id'] = parent_id
                        if local_path:
                            cache_row['local_path'] = local_path
                        dirty['row'] = cache_row
                        dirty['parent_id'] = parent_id
                        cache_file_parent_fix_count += 1

                    if cache_file_parent_fix_count:
                        parent_candidates[0]['fixed_cache_file_parent_count'] = cache_file_parent_fix_count
                except Exception as e:
                    cache_file_parent_fix_error = str(e)
                    logger.debug(
                        "  ➜ [共享资源] 回填 p115_filesystem_cache 季目录/文件父级失败: "
                        "parent=%s S%02d season_dir=%s -> %s",
                        parent_series_id, target_season, parent_id, e,
                    )
                logger.debug(
                    "  ➜ [共享资源] 父目录疑似脏数据，重新查找季目录: "
                    "%s S%02d %s(%s) -> %s(%s), fixed_files=%s",
                    parent_series_id, target_season, old_parent_name, old_parent_id, parent_name, parent_id,
                    cache_file_parent_fix_count,
                )
                break

    if not selected.get('season_dir'):
        return [], (
            f'季包文件定位失败：已按 PC/SHA1 命中 S{target_season:02d} 视频，但最佳父目录不是季目录：{parent_name}。'
            '已现场回查 115，但未找到明确的季目录；已拒绝分享。'
        ), {
            'reason': 'season_parent_not_season_dir',
            'parent_series_tmdb_id': parent_series_id,
            'season_number': target_season,
            'parent_candidates': parent_candidates,
        }

    files: List[Dict[str, Any]] = []
    seen_episode_keys = set()
    seen_fids = set()
    for item in selected.get('rows') or []:
        row = dict(item.get('row') or {})
        ep = dict(item.get('episode') or {})
        fid = str(row.get('id') or '').strip()
        pc = str(row.get('pick_code') or '').strip()
        sha1 = str(row.get('sha1') or '').strip().upper()
        ep_key = str(ep.get('tmdb_id') or '') or f"S{target_season}E{_safe_int(ep.get('episode_number'), 0)}"
        if not fid or fid in seen_fids or ep_key in seen_episode_keys:
            continue
        seen_fids.add(fid)
        seen_episode_keys.add(ep_key)
        files.append({
            'fid': fid,
            'sha1': sha1 or None,
            'size': safe_size(row.get('size')),
            'file_name': str(row.get('name') or ''),
            'relative_path': row.get('local_path') or row.get('name') or '',
            'tmdb_id': str(ep.get('tmdb_id') or ''),
            'item_type': 'Episode',
            'season_number': target_season,
            'episode_number': ep.get('episode_number'),
            'raw_json': {
                'source': 'media_metadata_episode_pc_sha1+p115_filesystem_cache',
                'cache_row': row,
                'episode_meta': ep,
                'selected_parent_id': parent_id,
                'selected_parent_name': parent_name,
            },
        })

    if not files:
        return [], f'季包文件定位失败：S{target_season:02d} 选中的季目录下没有可登记的视频文件', {
            'reason': 'season_selected_parent_no_files',
            'parent_series_tmdb_id': parent_series_id,
            'season_number': target_season,
            'parent_candidates': parent_candidates,
        }

    # parent_id / parent_name 已经来自上面按 p115_filesystem_cache.parent_id 分组后选中的季目录，
    # 这里不能再引用旧变量 parent_ids，否则会把目录选择逻辑打断。
    files.sort(key=lambda x: (_safe_int(x.get('episode_number'), 999999), str(x.get('file_name') or '')))

    if hasattr(sr, '_files_missing_raw_ffprobe'):
        missing_raw = sr._files_missing_raw_ffprobe(files)
        if missing_raw:
            if hasattr(sr, '_raw_missing_message'):
                return [], sr._raw_missing_message(missing_raw), {'reason': 'missing_raw_ffprobe', 'missing_raw': missing_raw}
            return [], f'缺少 raw_ffprobe_json：{missing_raw}', {'reason': 'missing_raw_ffprobe', 'missing_raw': missing_raw}

    return files, '', {
        'reason': 'season_directory_from_remote_115_fallback' if remote_season_dir else 'season_directory_from_db_parent_id',
        'root_fid': parent_id,
        'root_name': parent_name,
        'root_is_dir': True,
        'share_fids': [parent_id],
        'share_mode': 'directory',
        'remote_season_dir': remote_season_dir or None,
        'cache_file_parent_fix_count': cache_file_parent_fix_count,
        'cache_file_parent_fix_error': cache_file_parent_fix_error or None,
        'parent_series_tmdb_id': parent_series_id,
        'season_number': target_season,
        'file_count': len(files),
        'parent_candidates': parent_candidates,
        'episode_rows': len(episode_rows),
        'pickcodes': len(pickcodes),
        'sha1s': len(sha1s),
    }

def _create_completed_season_pack_share(
    client: SharedCenterClient,
    p115,
    season_row: Dict[str, Any],
    episode_records: List[Dict[str, Any]],
    backup_probe: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """创建完结季季包分享；成功后返回新分享记录。backup_probe 有值时标记为备份分享。"""
    if not _enabled():
        return {'ok': False, 'skipped': True, 'message': '共享资源未启用，禁止创建季包分享'}

    try:
        from routes import shared_resource as sr
    except Exception as e:
        return {'ok': False, 'message': f'无法加载共享资源辅助函数：{e}'}

    if hasattr(sr, '_share_policy_for_media'):
        policy = sr._share_policy_for_media(season_row)
        if not policy.get('allowed') or str(policy.get('share_type') or '').lower() != 'season_pack':
            return {'ok': False, 'message': policy.get('message') or '当前季不符合季包分享策略'}

    # 完结季包不再走候选目录定位；直接从 Season 行进入 DB 精确定位。
    candidate = dict(season_row or {})
    candidate.setdefault('share_type', 'season_pack')
    candidate.setdefault('share_item_type', 'Season')
    candidate.setdefault('item_type', 'Season')
    candidate.setdefault('parent_series_tmdb_id', season_row.get('parent_series_tmdb_id') or season_row.get('tmdb_id'))
    candidate.setdefault('season_number', season_row.get('season_number'))

    standard_identity = sr._standard_media_identity_for_share({
        'tmdb_id': candidate.get('share_tmdb_id') or candidate.get('tmdb_id') or season_row.get('parent_series_tmdb_id') or season_row.get('tmdb_id'),
        'item_type': 'Season',
        'parent_series_tmdb_id': candidate.get('parent_series_tmdb_id') or season_row.get('parent_series_tmdb_id') or season_row.get('tmdb_id'),
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

    root_fid = str(file_error_meta.get('root_fid') or candidate.get('root_fid') or '').strip()
    root_name = file_error_meta.get('root_name') or candidate.get('root_name') or standard_identity.get('title') or root_fid
    root_is_dir = bool(file_error_meta.get('root_is_dir', candidate.get('root_is_dir') is not False))

    share_fids = [str(x).strip() for x in (file_error_meta.get('share_fids') or []) if str(x or '').strip()]
    if not share_fids:
        return {'ok': False, 'message': '创建完结季季包分享失败：缺少可分享 FID', 'reason': 'season_pack_share_fids_missing'}

    blacklist_item = _center_blacklist_item_from_identity(standard_identity, candidate)
    blacklist_hit = _center_resource_blacklisted(client, blacklist_item)
    if blacklist_hit:
        return {'ok': False, 'skipped': True, 'reason': 'RESOURCE_BLACKLISTED', 'message': blacklist_hit.get('message') or '命中中心黑名单，跳过完结季季包分享', 'blacklist': blacklist_hit}

    share_resp = p115.share_create(share_fids, share_duration=-1, receive_code=None)
    if not share_resp or not share_resp.get('state'):
        if _looks_share_blocked(share_resp):
            _report_center_resource_blacklist(client, blacklist_item, share_resp, reason='share_blocked')
        return {'ok': False, 'message': f'创建完结季季包分享失败：{share_resp}', 'share_response': share_resp, 'share_fids': share_fids}

    data = share_resp.get('data') or {}
    share_code = data.get('share_code') or share_resp.get('share_code')
    receive_code = data.get('receive_code') or ''
    share_url = data.get('share_url') or (f"https://115.com/s/{share_code}" if share_code else '')

    source_record_ids = [r.get('id') for r in episode_records if r.get('id') is not None]
    season_number = candidate.get('season_number') or season_row.get('season_number')
    parent_series_id = standard_identity.get('parent_series_tmdb_id') or candidate.get('parent_series_tmdb_id') or season_row.get('parent_series_tmdb_id')
    is_backup_share = bool(backup_probe)

    raw_json = {
        'auto_completed_season_pack': True,
        'season_completed_rollup': {
            'source_record_ids': source_record_ids,
            'source_share_codes': [r.get('share_code') for r in episode_records if r.get('share_code')],
            'created_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        },
        'share_response': share_resp,
        'candidate': candidate,
        'standard_identity': standard_identity,
        'season_exact_files': {
            k: v for k, v in (file_error_meta or {}).items()
            if k in (
                'parent_series_tmdb_id', 'season_number', 'file_count', 'share_mode',
                'episode_rows', 'pickcodes', 'sha1s', 'reason'
            )
        },
        'share_fids': share_fids,
    }
    if is_backup_share:
        raw_json.update({
            'auto_backup_share': True,
            'backup_share': True,
            'backup_mirror': True,
            'source_provider': 'backup_mirror',
            'share_source': 'backup_mirror',
            'source_provider_label': '备份分享',
            'source_label': '备份分享',
            'backup_reason': 'completed_season_available_share_eq_1',
            'center_available_share_count': (backup_probe or {}).get('available_share_count', (backup_probe or {}).get('count')),
            'center_backup_probe': {
                'reason': (backup_probe or {}).get('reason'),
                'available_share_count': (backup_probe or {}).get('available_share_count', (backup_probe or {}).get('count')),
                'target_count': (backup_probe or {}).get('target_count'),
                'query': (backup_probe or {}).get('query'),
            },
        })
    else:
        # 正常完结季包分享仍按原来的 auto_gap 数据给中心登记，不改变计分/登记口径。
        raw_json['auto_gap'] = {
            'type': 'season_completed_rollup',
            'parent_series_tmdb_id': parent_series_id,
            'season_number': season_number,
            'source_record_ids': source_record_ids,
            'reason': 'Season.watching_status=Completed',
        }

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
        'raw_json': raw_json,
    })
    count = shared_share_db.replace_share_items(record['id'], files)
    record = shared_share_db.update_share_record(record['id'], item_count=count) or record

    shared_credit_db.add_credit_ledger(
        'share_backup_mirror_created' if is_backup_share else 'share_completed_season_pack_created',
        0,
        (
            f"完结季主动创建备份季包分享：{record.get('title') or root_name} S{_safe_int(season_number, 0):02d}"
            if is_backup_share else
            f"完结季汇总创建季包分享：{record.get('title') or root_name} S{_safe_int(season_number, 0):02d}"
        ),
        ref_id=str(record.get('id')),
        title=record.get('title') or root_name,
        raw_json={
            'share_code': share_code,
            'source_record_ids': source_record_ids,
            'parent_series_tmdb_id': parent_series_id,
            'season_number': season_number,
            'item_count': count,
            'backup_share': is_backup_share,
        },
    )

    return {'ok': True, 'record': record, 'items': files, 'share_response': share_resp}

def _center_has_open_season_gap(client: SharedCenterClient, parent_series_tmdb_id: str, season_number) -> bool:
    parent_series_tmdb_id = str(parent_series_tmdb_id or '').strip()
    season = _safe_int(season_number, None)
    if not parent_series_tmdb_id or season is None:
        return False
    try:
        gaps = (client.list_open_gaps(limit=500).get('items') or [])
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 检查中心季缺口失败: {e}")
        return False
    for gap in gaps:
        if str(gap.get('item_type') or '').strip() != 'Season':
            continue
        if str(gap.get('tmdb_id') or '').strip() != parent_series_tmdb_id:
            continue
        if _safe_int(gap.get('season_number'), None) == season:
            return True
    return False

def trigger_completed_season_pack_share_task(processor=None, *, parent_series_tmdb_id: str = '', season_number=None) -> Dict[str, Any]:
    """智能追剧确认完美完结后触发季包分享；创建/发现季包后同步清理同季单集分享。"""
    if not _enabled():
        return {'enabled': False, 'created': 0}
    parent_series_tmdb_id = str(parent_series_tmdb_id or '').strip()
    season_number = _safe_int(season_number, None)
    if not parent_series_tmdb_id or season_number is None:
        return {'enabled': True, 'created': 0, 'message': '缺少父剧 TMDb 或季号'}

    client = SharedCenterClient()
    if not client.ready:
        return {'enabled': True, 'created': 0, 'message': '共享中心未配置'}

    # 完结一致性通过后，单集分享必须跟随季包生命周期清理。
    # 之前这里直接创建季包但传入 []，导致新季包建成后旧单集分享继续留在 115 和中心。
    episode_records = _load_active_episode_share_records_for_season(parent_series_tmdb_id, season_number)
    has_active_pack = shared_share_db.check_active_season_pack_share(
        parent_series_tmdb_id,
        season_number,
        _active_share_statuses(),
    )

    p115 = None
    if episode_records or not has_active_pack:
        p115 = P115Service.get_client()
        if not p115:
            return {
                'enabled': True,
                'created': 0,
                'episode_cancelled': 0,
                'episode_cancel_failed': 0,
                'message': '115 客户端未初始化，无法创建季包或清理单集分享',
            }

    if has_active_pack:
        cancel_result = {'cancelled': 0, 'failed': 0}
        if episode_records:
            cancel_result = _cancel_episode_records_after_season_rollup(
                client,
                p115,
                episode_records,
                new_pack_record=None,
                reason='season_completed_rollup_existing_pack',
            )
        logger.info(
            "  ➜ [共享资源] 完结季包已存在，已清理同季单集分享：%s S%02d, checked=%s, cancelled=%s, failed=%s",
            parent_series_tmdb_id,
            int(season_number),
            len(episode_records),
            cancel_result.get('cancelled', 0),
            cancel_result.get('failed', 0),
        )
        return {
            'enabled': True,
            'created': 0,
            'message': '本地已有活动季包分享，已执行同季单集清理',
            'episode_checked': len(episode_records),
            'episode_cancelled': cancel_result.get('cancelled', 0),
            'episode_cancel_failed': cancel_result.get('failed', 0),
        }

    # 完结季包同样按三档处理：
    # count=0：只有中心存在 Season 缺口时，按原逻辑创建正常季包分享；
    # count=1：创建备份季包分享；
    # count>1：已有冗余，跳过。
    backup_probe = _probe_backup_share_needed_for_library_item(client, {
        'item_type': 'Season',
        'tmdb_id': parent_series_tmdb_id,
        'parent_series_tmdb_id': parent_series_tmdb_id,
        'season_number': season_number,
    })
    backup_needed = bool(backup_probe.get('need_share'))
    backup_probe_ok = bool(backup_probe.get('ok'))
    available_count = _safe_int(backup_probe.get('available_share_count', backup_probe.get('count')), None) if backup_probe_ok else None

    normal_gap_needed = False
    if backup_needed:
        normal_gap_needed = False
    elif (available_count is None or available_count < 1):
        normal_gap_needed = _center_has_open_season_gap(client, parent_series_tmdb_id, season_number)

    if not backup_needed and not normal_gap_needed:
        logger.debug(
            "  ➜ [共享资源] 完结季包分享跳过：%s S%02d reason=%s available=%s target=%s",
            parent_series_tmdb_id,
            int(season_number),
            backup_probe.get('reason') or 'center_season_gap_not_open',
            backup_probe.get('available_share_count', backup_probe.get('count')),
            backup_probe.get('target_count', backup_probe.get('threshold')),
        )
        return {
            'enabled': True,
            'created': 0,
            'episode_checked': len(episode_records),
            'message': backup_probe.get('reason') or 'center_season_gap_not_open',
            'probe': backup_probe,
        }

    season_row = shared_share_db.find_local_media_for_gap(parent_series_tmdb_id, 'Season', season_number, None)
    if not season_row:
        return {'enabled': True, 'created': 0, 'message': '本地未找到已入库季'}

    result = _create_completed_season_pack_share(
        client,
        p115,
        season_row,
        episode_records,
        backup_probe=backup_probe if backup_needed else None,
    )
    if result.get('ok'):
        record = result.get('record') or {}
        cancel_result = {'cancelled': 0, 'failed': 0}
        if episode_records:
            cancel_result = _cancel_episode_records_after_season_rollup(
                client,
                p115,
                episode_records,
                new_pack_record=record,
                reason='season_completed_rollup',
            )
        logger.info(
            "  ➜ [共享资源] 完结一致性通过后已创建季包分享并清理同季单集：%s S%02d share=%s, checked=%s, cancelled=%s, failed=%s",
            parent_series_tmdb_id,
            int(season_number),
            record.get('share_code') or '-',
            len(episode_records),
            cancel_result.get('cancelled', 0),
            cancel_result.get('failed', 0),
        )
        return {
            'enabled': True,
            'created': 1,
            'record_id': record.get('id'),
            'share_code': record.get('share_code'),
            'episode_checked': len(episode_records),
            'episode_cancelled': cancel_result.get('cancelled', 0),
            'episode_cancel_failed': cancel_result.get('failed', 0),
        }
    logger.info(
        "  ➜ [共享资源] 完结一致性通过但创建季包分享失败：%s S%02d -> %s",
        parent_series_tmdb_id, int(season_number), result.get('message') or result
    )
    return {'enabled': True, 'created': 0, 'message': result.get('message') or 'create_failed', 'result': result}

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
        shared_credit_db.add_credit_ledger(
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

# ======================================================================
# 中心通用设备事件监听：中心按 device_id 推送资源可用/求分享命中等事件。
# ======================================================================
_shared_device_event_listener_lock = threading.Lock()
_shared_device_event_listener_thread = None
_shared_device_event_listener_stop = threading.Event()


def _notify_shared_device_event(event: Dict[str, Any], result: Dict[str, Any], success: bool):
    """事件消费后的通知。当前复用求分享通知入口；其它事件只写日志，不强依赖 TG。"""
    event_type = str((event or {}).get('event_type') or '').strip()
    try:
        if event_type == 'request_matched':
            from handler.telegram import send_share_request_push_notification
            send_share_request_push_notification(event, result=result, success=success)
    except Exception as e:
        logger.debug(f"  ➜ [共享事件监听] 发送事件通知失败: {e}")


def _ack_shared_device_event(client: SharedCenterClient, event: Dict[str, Any], result: str, message: str):
    event_id = str((event or {}).get('event_id') or '').strip()
    if not event_id:
        return
    try:
        # 新中心通用事件回执。
        if hasattr(client, 'ack_device_event'):
            client.ack_device_event(event_id, result=result, message=message)
            return
    except Exception as e:
        logger.debug(f"  ➜ [共享事件监听] 通用事件回执失败: event={event_id}, err={e}")

    # 旧中心兼容：求分享事件仍走老接口。
    try:
        client.ack_share_request_event(event_id, result=result, message=message)
    except Exception as e:
        logger.debug(f"  ➜ [共享事件监听] 旧求分享事件回执失败: event={event_id}, err={e}")



def _shared_event_library_hit(event: Dict[str, Any]) -> Dict[str, Any]:
    """事件消费前按中心事件自带的媒体身份做一次本地入库门禁。"""
    event = dict(event or {})
    payload = event.get('payload') if isinstance(event.get('payload'), dict) else {}
    item_type = str(event.get('item_type') or payload.get('item_type') or payload.get('target_type') or '').strip()
    item_type_l = item_type.lower()
    tmdb_id = str(event.get('tmdb_id') or payload.get('tmdb_id') or '').strip()
    parent_id = str(event.get('parent_series_tmdb_id') or payload.get('parent_series_tmdb_id') or payload.get('series_tmdb_id') or tmdb_id).strip()
    season = _safe_int(event.get('season_number') if event.get('season_number') is not None else payload.get('season_number'), None)
    episode = _safe_int(event.get('episode_number') if event.get('episode_number') is not None else payload.get('episode_number'), None)

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                if item_type_l in ('movie', 'movie_file', 'movie_folder') and tmdb_id:
                    cur.execute("""
                        SELECT tmdb_id, item_type, title
                        FROM media_metadata
                        WHERE item_type='Movie' AND tmdb_id=%s AND COALESCE(in_library, FALSE)=TRUE
                        LIMIT 1
                    """, (tmdb_id,))
                    row = cur.fetchone()
                    if row:
                        return {'hit': True, 'reason': 'movie_in_library', 'row': dict(row)}

                if item_type_l in ('episode', 'episode_file', 'single') and parent_id and season is not None and episode is not None:
                    cur.execute("""
                        SELECT tmdb_id, item_type, title, parent_series_tmdb_id, season_number, episode_number
                        FROM media_metadata
                        WHERE item_type='Episode'
                          AND (parent_series_tmdb_id=%s OR tmdb_id=%s)
                          AND season_number=%s AND episode_number=%s
                          AND COALESCE(in_library, FALSE)=TRUE
                        LIMIT 1
                    """, (parent_id, parent_id, season, episode))
                    row = cur.fetchone()
                    if row:
                        return {'hit': True, 'reason': 'episode_in_library', 'row': dict(row)}

                if item_type_l in ('season', 'season_pack', 'tv_pack') and parent_id and season is not None:
                    # 季包事件没有具体集号时，只以 Season 行已入库作为跳过依据；不能拿代表 SHA1 误判整包。
                    cur.execute("""
                        SELECT tmdb_id, item_type, title, parent_series_tmdb_id, season_number
                        FROM media_metadata
                        WHERE item_type='Season'
                          AND (parent_series_tmdb_id=%s OR tmdb_id=%s)
                          AND season_number=%s
                          AND COALESCE(in_library, FALSE)=TRUE
                        LIMIT 1
                    """, (parent_id, parent_id, season))
                    row = cur.fetchone()
                    if row:
                        return {'hit': True, 'reason': 'season_in_library', 'row': dict(row)}
    except Exception as e:
        logger.debug(f"  ➜ [共享事件监听] 本地入库门禁查询失败，继续转存: {e}")
    return {'hit': False, 'reason': 'not_in_library'}

def _json_obj(value) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            obj = json.loads(value)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}


def _share_request_filter_from_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """把中心求分享事件/列表行整理成 shared_resource 候选筛选参数。"""
    event = dict(event or {})
    payload = event.get('payload') if isinstance(event.get('payload'), dict) else {}
    params = payload.get('params')
    if params in (None, {}, ''):
        params = payload.get('params_json')
    params = _json_obj(params)

    def _first(*keys, default=None):
        for key in keys:
            value = event.get(key)
            if value not in (None, '', [], {}):
                return value
            value = payload.get(key)
            if value not in (None, '', [], {}):
                return value
        return default

    return {
        'group_id': str(_first('group_id', 'share_request_group_id', default='') or '').strip(),
        'tmdb_id': str(_first('tmdb_id', default='') or '').strip(),
        'media_type': str(_first('media_type', default='') or '').strip().lower(),
        'target_type': str(_first('target_type', 'item_type', default='') or '').strip().lower(),
        'season_number': _safe_int(_first('season_number', default=None), None),
        'episode_number': _safe_int(_first('episode_number', default=None), None),
        'episode_numbers': payload.get('episode_numbers') or event.get('episode_numbers') or [],
        'season_count': _safe_int(_first('season_count', default=0), 0),
        'title': _first('title', default='') or '',
        'release_year': _safe_int(_first('release_year', default=None), None),
        'current_bounty': _safe_int(_first('current_bounty', 'bounty_total', default=0), 0),
        'params': params,
        'raw_event': event,
        'payload': payload,
    }


def _local_rows_for_share_request(sr, request_filter: Dict[str, Any]) -> List[Dict[str, Any]]:
    """按求分享目标从本地 media_metadata 精确取可分享候选基础行。"""
    tmdb_id = str((request_filter or {}).get('tmdb_id') or '').strip()
    if not tmdb_id:
        return []

    target_type = str((request_filter or {}).get('target_type') or '').strip().lower()
    media_type = str((request_filter or {}).get('media_type') or '').strip().lower()
    season = request_filter.get('season_number')

    if target_type in ('series', 'tv'):
        row = sr._load_series_row_for_share_request(request_filter)
        return [row] if row else []

    if target_type == 'episode':
        row = sr._load_exact_episode_row_for_share_request(request_filter)
        return [row] if row else []

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                if media_type == 'movie' or target_type == 'movie':
                    cur.execute("""
                        SELECT *
                        FROM media_metadata
                        WHERE item_type='Movie'
                          AND tmdb_id=%s
                          AND COALESCE(in_library, FALSE)=TRUE
                        LIMIT 1
                    """, (tmdb_id,))
                    row = cur.fetchone()
                    return [dict(row)] if row else []

                if target_type == 'season' and season is not None:
                    cur.execute("""
                        SELECT *
                        FROM media_metadata
                        WHERE item_type='Season'
                          AND (parent_series_tmdb_id=%s OR tmdb_id=%s)
                          AND season_number=%s
                          AND COALESCE(in_library, FALSE)=TRUE
                        ORDER BY tmdb_id
                        LIMIT 1
                    """, (tmdb_id, tmdb_id, season))
                    row = cur.fetchone()
                    if row:
                        return [dict(row)]

                    cur.execute("""
                        SELECT *
                        FROM media_metadata
                        WHERE item_type='Episode'
                          AND (parent_series_tmdb_id=%s OR tmdb_id=%s)
                          AND season_number=%s
                          AND COALESCE(in_library, FALSE)=TRUE
                        ORDER BY episode_number NULLS LAST, tmdb_id
                        LIMIT 50
                    """, (tmdb_id, tmdb_id, season))
                    return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        logger.debug(f"  ➜ [共享事件监听] 查询本地可响应求分享资源失败: tmdb={tmdb_id}, err={e}")
    return []


def _find_auto_share_request_candidate(sr, request_filter: Dict[str, Any]) -> Dict[str, Any]:
    rows = _local_rows_for_share_request(sr, request_filter)
    candidates = []
    seen = set()
    for row in rows:
        try:
            for cand in sr._expand_share_candidates_for_share_request(row, request_filter):
                key = (
                    cand.get('share_tmdb_id') or cand.get('tmdb_id'),
                    cand.get('share_item_type') or cand.get('item_type'),
                    cand.get('season_number'),
                    cand.get('episode_number'),
                    cand.get('root_fid'),
                )
                if key in seen:
                    continue
                seen.add(key)
                candidates.append(cand)
        except Exception as e:
            logger.debug(f"  ➜ [共享事件监听] 展开求分享候选失败: row={row.get('tmdb_id')}, err={e}")

    candidates = sr._filter_candidates_for_share_request(candidates, request_filter)
    candidates = [
        c for c in candidates
        if c.get('resolvable') and str(c.get('root_fid') or '').strip()
    ]
    if not candidates:
        return {}

    def _score(c: Dict[str, Any]) -> tuple:
        share_type = str(c.get('share_type') or '').strip().lower()
        item_type = str(c.get('share_item_type') or c.get('item_type') or '').strip()
        file_count = _safe_int(c.get('file_count'), 0)
        pack_score = 0
        target_type = str(request_filter.get('target_type') or '').lower()
        if target_type in ('season', 'series', 'tv') and share_type in ('season_pack', 'series_pack'):
            pack_score = 2
        elif item_type in ('Season', 'Series'):
            pack_score = 1
        return (pack_score, file_count)

    return sorted(candidates, key=_score, reverse=True)[0]


def _local_share_request_response_exists(group_id: str) -> bool:
    group_id = str(group_id or '').strip()
    if not group_id:
        return False
    try:
        records, _ = shared_share_db.list_share_records(status='all', keyword='', page=1, page_size=200)
    except Exception:
        return False

    for record in records or []:
        status = str(record.get('status') or '').strip()
        if status in ('cancelled', 'deleted', 'dead', 'blocked', 'violation'):
            continue
        raw = record.get('raw_json') if isinstance(record.get('raw_json'), dict) else {}
        manual = raw.get('manual_payload') if isinstance(raw.get('manual_payload'), dict) else {}
        payload = raw.get('share_request_payload') if isinstance(raw.get('share_request_payload'), dict) else {}
        found = (
            record.get('share_request_group_id') or
            raw.get('share_request_group_id') or
            manual.get('share_request_group_id') or
            payload.get('group_id') or
            payload.get('share_request_group_id')
        )
        if str(found or '').strip() == group_id:
            return True
    return False


def _build_auto_share_payload_from_candidate(candidate: Dict[str, Any], request_filter: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(candidate or {})
    share_item_type = payload.get('share_item_type') or payload.get('item_type')
    share_tmdb_id = payload.get('share_tmdb_id') or payload.get('tmdb_id') or request_filter.get('tmdb_id')

    payload.update({
        'tmdb_id': str(share_tmdb_id or ''),
        'item_type': share_item_type,
        'share_type': payload.get('share_type') or (
            'season_pack' if share_item_type == 'Season' else
            'series_pack' if share_item_type == 'Series' else
            'episode_file' if share_item_type == 'Episode' else
            'movie_file'
        ),
        'root_fid': str(payload.get('root_fid') or '').strip(),
        'root_name': payload.get('root_name') or payload.get('display_title') or payload.get('title') or '',
        'root_is_dir': payload.get('root_is_dir'),
        'parent_series_tmdb_id': payload.get('parent_series_tmdb_id') or (request_filter.get('tmdb_id') if share_item_type in ('Season', 'Episode', 'Series') else ''),
        'season_number': payload.get('season_number') if payload.get('season_number') not in (None, '') else request_filter.get('season_number'),
        'episode_number': payload.get('episode_number') if payload.get('episode_number') not in (None, '') else request_filter.get('episode_number'),
        'title': payload.get('standard_title') or payload.get('title') or request_filter.get('title') or '',
        'release_year': payload.get('release_year') or request_filter.get('release_year'),
        'share_request_group_id': request_filter.get('group_id'),
        'share_request_payload': {
            'group_id': request_filter.get('group_id'),
            'tmdb_id': request_filter.get('tmdb_id'),
            'media_type': request_filter.get('media_type'),
            'target_type': request_filter.get('target_type'),
            'season_number': request_filter.get('season_number'),
            'episode_number': request_filter.get('episode_number'),
            'episode_numbers': request_filter.get('episode_numbers') or [],
            'season_count': request_filter.get('season_count') or 0,
            'title': request_filter.get('title') or '',
            'release_year': request_filter.get('release_year'),
            'params': request_filter.get('params') or {},
            'current_bounty': request_filter.get('current_bounty') or 0,
        },
    })
    return payload


def _create_auto_share_request_response(sr, request_filter: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, Any]:
    """为求分享自动创建 115 分享记录；审核/登记继续走现有高频同步链路。"""
    group_id = str(request_filter.get('group_id') or '').strip()
    if group_id and _local_share_request_response_exists(group_id):
        return {'success': True, 'skipped': True, 'message': '该求分享本机已创建过响应分享，跳过重复创建'}

    p115 = P115Service.get_client()
    if not p115:
        return {'success': False, 'message': '未配置可用的 115 Cookie 客户端，无法自动响应求分享'}

    data = _build_auto_share_payload_from_candidate(candidate, request_filter)
    prepared = sr._collect_manual_share_files_for_payload(data, client=p115)
    if not prepared.get('ok'):
        return {'success': False, 'skipped': True, 'message': prepared.get('message') or '自动响应求分享预处理失败'}

    share_data = prepared.get('data') or data
    files = prepared.get('files') or []
    missing_raw = sr._files_missing_raw_ffprobe(files)
    if missing_raw:
        return {
            'success': False,
            'skipped': True,
            'message': sr._raw_missing_message(missing_raw) if hasattr(sr, '_raw_missing_message') else '缺少 raw_ffprobe_json，跳过自动响应求分享',
            'missing_raw': missing_raw,
        }

    if str(share_data.get('share_type') or '').strip().lower() == 'season_pack':
        consistency = sr._validate_season_pack_consistency(files, share_data)
        if not consistency.get('ok'):
            return {
                'success': False,
                'skipped': True,
                'message': consistency.get('message') or '季包媒体参数不一致，跳过自动响应求分享',
                'season_pack_consistency': consistency,
            }

    root_fid = str(share_data.get('root_fid') or '').strip()
    receive_code = str(share_data.get('receive_code') or '').strip() or None

    standard_identity_for_check = sr._standard_media_identity_for_share({
        **share_data,
        'item_type': share_data.get('item_type') or 'Season',
        'share_type': share_data.get('share_type') or ('season_pack' if share_data.get('season_number') else 'movie_folder'),
    })
    blacklist_item = _center_blacklist_item_from_identity(standard_identity_for_check, share_data)
    sync_client = SharedCenterClient()
    if sync_client.ready:
        blacklist_hit = _center_resource_blacklisted(sync_client, blacklist_item)
        if blacklist_hit:
            return {'success': False, 'skipped': True, 'reason': 'RESOURCE_BLACKLISTED', 'message': blacklist_hit.get('message') or '命中中心黑名单，跳过自动响应求分享', 'blacklist': blacklist_hit}

    share_resp = p115.share_create([root_fid], share_duration=-1, receive_code=receive_code)
    if not share_resp or not share_resp.get('state'):
        if sync_client.ready and _looks_share_blocked(share_resp):
            _report_center_resource_blacklist(sync_client, blacklist_item, share_resp, reason='share_blocked')
        return {'success': False, 'message': f"创建 115 分享失败: {share_resp}"}

    share_resp_data = share_resp.get('data') or {}
    share_code = share_resp_data.get('share_code') or share_resp.get('share_code')
    share_url = share_resp_data.get('share_url') or (f"https://115.com/s/{share_code}" if share_code else '')
    receive_code = receive_code or share_resp_data.get('receive_code') or ''

    standard_identity = sr._standard_media_identity_for_share({
        **share_data,
        'item_type': share_data.get('item_type') or 'Season',
        'share_type': share_data.get('share_type') or ('season_pack' if share_data.get('season_number') else 'movie_folder'),
    })
    standard_title = standard_identity.get('title') or str(share_data.get('title') or '').strip() or prepared.get('root_name') or root_fid
    standard_year = standard_identity.get('release_year') or share_data.get('release_year')

    record = shared_share_db.create_share_record({
        'share_code': share_code,
        'receive_code': receive_code,
        'share_url': share_url,
        'share_type': share_data.get('share_type') or ('season_pack' if share_data.get('season_number') else 'movie_folder'),
        'root_fid': root_fid,
        'root_name': prepared.get('root_name') or share_data.get('root_name') or root_fid,
        'root_is_dir': prepared.get('root_is_dir'),
        'tmdb_id': str(standard_identity.get('tmdb_id') or share_data.get('tmdb_id') or ''),
        'item_type': share_data.get('item_type') or 'Season',
        'parent_series_tmdb_id': standard_identity.get('parent_series_tmdb_id') or share_data.get('parent_series_tmdb_id'),
        'season_number': share_data.get('season_number'),
        'episode_number': share_data.get('episode_number'),
        'title': standard_title,
        'release_year': standard_year,
        'status': 'pending_review',
        'review_status': 'pending_review',
        'center_status': 'not_reported',
        'raw_json': {
            'share_response': share_resp,
            'root_info': prepared.get('info_resp') or {},
            'auto_share_request': True,
            'share_request_group_id': group_id or None,
            'share_request_payload': share_data.get('share_request_payload') or request_filter,
            'auto_payload': share_data,
            'standard_identity': standard_identity,
        },
    })
    count = shared_share_db.replace_share_items(record['id'], files)
    record = shared_share_db.update_share_record(record['id'], item_count=count)
    shared_credit_db.add_credit_ledger(
        'share_request_auto_share_created', 0,
        f"自动响应求分享，已创建115分享：{standard_title}",
        ref_id=str(record.get('id')),
        title=standard_title,
        raw_json={'share_code': share_code, 'group_id': group_id, 'item_count': count},
    )

    try:
        sync_client = SharedCenterClient()
        if sync_client.ready:
            _auto_check_and_report_local_shares(sync_client, max_records=20)
    except Exception as e:
        logger.debug(f"  ➜ [共享事件监听] 自动响应求分享后同步分享状态失败，等待下轮高频任务: {e}")

    return {
        'success': True,
        'message': f"已自动响应求分享并创建 115 分享：{standard_title}",
        'record_id': record.get('id'),
        'share_code': share_code,
        'item_count': count,
        'title': standard_title,
    }


def _auto_respond_to_share_request_event(event: Dict[str, Any]) -> Dict[str, Any]:
    if not _shared_auto_share_requests_enabled():
        return {'success': False, 'skipped': True, 'message': '自动响应求分享未启用'}

    try:
        from routes import shared_resource as sr
    except Exception as e:
        return {'success': False, 'message': f'共享资源路由辅助函数不可用，无法自动响应求分享: {e}'}

    request_filter = _share_request_filter_from_event(event)
    group_id = request_filter.get('group_id')
    if not group_id or not request_filter.get('tmdb_id'):
        return {'success': False, 'skipped': True, 'message': '求分享事件缺少 group_id 或 tmdb_id'}

    candidate = _find_auto_share_request_candidate(sr, request_filter)
    if not candidate:
        title = request_filter.get('title') or request_filter.get('tmdb_id') or group_id
        return {'success': False, 'skipped': True, 'message': f'本机没有可自动响应的资源：{title}'}

    return _create_auto_share_request_response(sr, request_filter, candidate)


def _handle_share_request_auto_response_event(client: SharedCenterClient, event: Dict[str, Any]) -> Dict[str, Any]:
    event = dict(event or {})
    event_id = str(event.get('event_id') or '').strip()
    payload = event.get('payload') if isinstance(event.get('payload'), dict) else {}
    title = event.get('title') or payload.get('title') or payload.get('group_id') or event_id or '求分享'

    result = _auto_respond_to_share_request_event(event)
    ok = bool(result.get('success'))
    skipped = bool(result.get('skipped'))
    message = result.get('message') or ('已自动响应求分享' if ok else '自动响应求分享失败')

    if event_id:
        _ack_shared_device_event(client, event, 'success' if ok else ('skipped' if skipped else 'failed'), message)

    try:
        shared_credit_db.add_credit_ledger(
            'share_request_auto_response_success' if ok else 'share_request_auto_response_skipped' if skipped else 'share_request_auto_response_failed',
            0,
            f"自动响应求分享：{title}，{message}",
            ref_id=str(payload.get('group_id') or event.get('group_id') or event_id),
            title=title,
            raw_json={'event': event, 'result': result},
        )
    except Exception:
        pass

    if ok:
        logger.info("  ➜ [共享事件监听] 自动响应求分享成功: %s", message)
    elif skipped:
        logger.info("  ➜ [共享事件监听] 自动响应求分享跳过: %s", message)
    else:
        logger.warning("  ➜ [共享事件监听] 自动响应求分享失败: %s", message)
    return result


def _auto_respond_open_share_requests(client: SharedCenterClient, max_requests: int = 20) -> Dict[str, int]:
    """高频任务兜底扫描 open 求分享，防止离线期间错过长轮询事件。"""
    if not _shared_auto_share_requests_enabled():
        return {'share_request_auto_checked': 0, 'share_request_auto_created': 0, 'share_request_auto_skipped': 0, 'share_request_auto_failed': 0}
    try:
        from routes import shared_resource as sr
        data = sr._center_json_request(
            'GET',
            '/api/v1/share-requests',
            params={'status': 'open', 'limit': max(1, min(int(max_requests or 20), 100)), 'offset': 0},
            timeout=25,
        )
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 拉取中心 open 求分享用于自动响应失败: {e}")
        return {'share_request_auto_checked': 0, 'share_request_auto_created': 0, 'share_request_auto_skipped': 0, 'share_request_auto_failed': 1}

    checked = created = skipped = failed = 0
    for item in data.get('items') or []:
        if item.get('joined_by_me'):
            continue
        checked += 1
        payload = dict(item)
        if 'params' not in payload:
            payload['params'] = _json_obj(payload.get('params_json'))
        event = {
            'event_id': '',
            'event_type': 'share_request_created',
            'title': payload.get('title') or '',
            'payload': payload,
        }
        result = _auto_respond_to_share_request_event(event)
        if result.get('success') and not result.get('skipped'):
            created += 1
        elif result.get('skipped'):
            skipped += 1
        else:
            failed += 1
        if created > 0:
            break
    return {
        'share_request_auto_checked': checked,
        'share_request_auto_created': created,
        'share_request_auto_skipped': skipped,
        'share_request_auto_failed': failed,
    }




def _handle_source_superseded_event(client: SharedCenterClient, event: Dict[str, Any]) -> Dict[str, Any]:
    """中心通知：本机贡献的单集源已被同季季包接管，需要删除真实 115 分享。"""
    event = dict(event or {})
    payload = event.get('payload') if isinstance(event.get('payload'), dict) else {}
    event_id = str(event.get('event_id') or '').strip()
    title = event.get('title') or payload.get('title') or payload.get('file_name') or payload.get('source_id') or event_id
    if not event_id:
        return {'success': False, 'message': '事件缺少 event_id'}

    records = _find_local_share_records_for_superseded_event(event)
    if not records:
        msg = f'本地未找到需清理的单集分享，可能已删除：{title}'
        _ack_shared_device_event(client, event, 'skipped', msg)
        logger.info(f"  ➜ [共享事件监听] {msg}")
        return {'success': False, 'skipped': True, 'message': msg}

    p115 = P115Service.get_client()
    if not p115:
        msg = '115 客户端未初始化，无法删除被季包接管的单集分享'
        _ack_shared_device_event(client, event, 'failed', msg)
        return {'success': False, 'message': msg, 'matched': len(records)}

    cancelled = failed = 0
    messages = []
    for record in records:
        ok, msg = _cancel_superseded_local_share_record(
            client,
            p115,
            record,
            event=event,
            reason='source_superseded_by_season_pack',
        )
        messages.append(msg)
        if ok:
            cancelled += 1
        else:
            failed += 1
        time.sleep(0.2)

    if failed:
        message = f'单集分享被季包接管，已清理 {cancelled} 个，失败 {failed} 个：' + '；'.join(messages[:3])
        _ack_shared_device_event(client, event, 'failed', message)
        logger.warning(f"  ➜ [共享事件监听] {message}")
        return {'success': False, 'message': message, 'cancelled': cancelled, 'failed': failed}

    message = f'同季季包已入池，已清理本机单集分享 {cancelled} 个：{title}'
    _ack_shared_device_event(client, event, 'success', message)
    logger.info(f"  ➜ [共享事件监听] {message}")
    return {'success': True, 'message': message, 'cancelled': cancelled, 'failed': 0}

def _handle_shared_device_event(client: SharedCenterClient, event: Dict[str, Any]) -> Dict[str, Any]:
    event = dict(event or {})
    payload = event.get('payload') if isinstance(event.get('payload'), dict) else {}
    event_id = str(event.get('event_id') or '').strip()
    event_type = str(event.get('event_type') or payload.get('event_type') or 'resource_available').strip()
    source_id = str(event.get('source_id') or payload.get('source_id') or '').strip()
    title = event.get('title') or payload.get('title') or source_id or event_id

    if not event_id:
        return {'success': False, 'message': '事件缺少 event_id'}

    if event_type in ('share_request_created', 'share_request_updated', 'share_request_opened'):
        return _handle_share_request_auto_response_event(client, event)

    if event_type == 'source_superseded':
        return _handle_source_superseded_event(client, event)

    if event_type not in ('resource_available', 'request_matched'):
        msg = f'暂不支持的事件类型：{event_type}'
        _ack_shared_device_event(client, event, 'skipped', msg)
        logger.debug(f"  ➜ [共享事件监听] {msg} event={event_id}")
        return {'success': False, 'skipped': True, 'message': msg}

    if not source_id:
        msg = '事件缺少 source_id'
        _ack_shared_device_event(client, event, 'failed', msg)
        return {'success': False, 'message': msg}

    local_hit = _shared_event_library_hit(event)
    if local_hit.get('hit'):
        msg = f'本地已入库，跳过中心推送事件：{title}'
        _ack_shared_device_event(client, event, 'skipped', msg)
        logger.debug("  ➜ [共享事件监听] %s event=%s source=%s reason=%s", msg, event_id, source_id, local_hit.get('reason'))
        result = {'success': False, 'skipped': True, 'message': msg, 'local_hit': local_hit}
        try:
            shared_credit_db.add_credit_ledger(
                'shared_device_event_skipped_existing', 0, msg,
                ref_id=str(event.get('gap_key') or payload.get('gap_key') or event.get('group_id') or payload.get('group_id') or event_id),
                title=title,
                raw_json={'event': event, 'local_hit': local_hit},
            )
        except Exception:
            pass
        _notify_shared_device_event(event, result, True)
        return result

    try:
        from handler.shared_subscription_service import consume_center_sources
    except Exception as e:
        msg = f'共享资源消费入口不可用：{e}'
        _ack_shared_device_event(client, event, 'failed', msg)
        _notify_shared_device_event(event, {'message': msg}, False)
        return {'success': False, 'message': msg}

    try:
        context = {
            'source': 'device_event_push',
            'device_event_id': event_id,
            'event_type': event_type,
            'gap_key': event.get('gap_key') or payload.get('gap_key'),
            'share_request_group_id': event.get('group_id') or payload.get('group_id'),
            'title': title,
            'target_type': event.get('target_type') or payload.get('target_type'),
            'season_number': event.get('season_number') if event.get('season_number') is not None else payload.get('season_number'),
            'episode_number': event.get('episode_number') if event.get('episode_number') is not None else payload.get('episode_number'),
        }
        label = '求分享命中' if event_type == 'request_matched' else '订阅资源入池'
        logger.info("  ➜ [共享事件监听] 收到%s事件，开始自动转存: %s source=%s", label, title, source_id)
        result = consume_center_sources([source_id], mode='permanent', context=context) or {}
        ok = bool(result.get('success'))
        message = result.get('message') or result.get('action_type') or ('自动转存成功' if ok else '自动转存失败')
        _ack_shared_device_event(client, event, 'success' if ok else 'failed', message)

        ledger_reason = 'shared_device_event_import_success' if ok else 'shared_device_event_import_failed'
        ledger_title = f'{label}后自动转存：{title}' if ok else f'{label}但自动转存失败：{title}'
        try:
            shared_credit_db.add_credit_ledger(
                ledger_reason, 0, ledger_title,
                ref_id=str(event.get('gap_key') or payload.get('gap_key') or event.get('group_id') or payload.get('group_id') or event_id),
                title=title,
                raw_json={'event': event, 'result': result},
            )
        except Exception:
            pass
        _notify_shared_device_event(event, result, ok)
        return result
    except Exception as e:
        msg = f'自动转存异常：{e}'
        logger.warning(f"  ➜ [共享事件监听] 处理中心事件失败: {event} -> {e}", exc_info=True)
        _ack_shared_device_event(client, event, 'failed', msg)
        _notify_shared_device_event(event, {'message': msg}, False)
        return {'success': False, 'message': msg}


def _shared_device_event_listener_worker():
    # logger.info("  ➜ [共享事件监听] 长轮询监听已启动。")
    client = SharedCenterClient()
    idle_errors = 0
    use_legacy_share_request_poll = False
    try:
        while not _shared_device_event_listener_stop.is_set():
            if not _enabled() or not client.ready:
                break
            try:
                if use_legacy_share_request_poll or not hasattr(client, 'poll_device_events'):
                    resp = client.poll_share_request_events(timeout=25, limit=5)
                else:
                    resp = client.poll_device_events(timeout=25, limit=5)
                    if resp.get('supported') is False:
                        use_legacy_share_request_poll = True
                        logger.info("  ➜ [共享事件监听] 中心暂不支持通用 device_events，回退旧求分享事件轮询。")
                        continue
                idle_errors = 0
            except Exception as e:
                idle_errors += 1
                logger.debug(f"  ➜ [共享事件监听] 长轮询失败，将重试: {e}")
                if idle_errors >= 12:
                    logger.warning("  ➜ [共享事件监听] 连续长轮询失败过多，停止监听，等待下次状态同步任务重新启动。")
                    break
                time.sleep(min(30, 3 * idle_errors))
                continue

            for event in resp.get('items') or []:
                if _shared_device_event_listener_stop.is_set():
                    break
                # 旧中心求分享事件没有 event_type，统一补成 request_matched。
                if use_legacy_share_request_poll and not event.get('event_type'):
                    event = dict(event)
                    event['event_type'] = 'request_matched'
                _handle_shared_device_event(client, event)
    finally:
        logger.info("  ➜ [共享事件监听] 长轮询监听已停止。")


def ensure_shared_device_event_listener() -> bool:
    """启动中心通用事件长轮询。共享资源启用后常驻，用于消费中心按 device_id 下发的事件。"""
    if not _enabled():
        return False
    client = SharedCenterClient()
    if not client.ready:
        return False
    global _shared_device_event_listener_thread
    with _shared_device_event_listener_lock:
        if _shared_device_event_listener_thread and _shared_device_event_listener_thread.is_alive():
            return True
        _shared_device_event_listener_stop.clear()
        _shared_device_event_listener_thread = threading.Thread(
            target=_shared_device_event_listener_worker,
            name='SharedDeviceEventListener',
            daemon=True,
        )
        _shared_device_event_listener_thread.start()
        return True


# 兼容旧调用名：外部若仍调用 ensure_share_request_event_listener，实际启动的是全局事件监听。
def ensure_share_request_event_listener() -> bool:
    return ensure_shared_device_event_listener()


def stop_shared_device_event_listener():
    _shared_device_event_listener_stop.set()


def stop_share_request_event_listener():
    stop_shared_device_event_listener()


def task_shared_share_status_sync_high_freq(processor=None, maintenance_silent: bool = False):
    """硬编码高频任务：只处理 115 分享审核黑洞 -> RAW 上传 -> 中心登记/撤销。"""
    def _status(progress: int, message: str):
        if not maintenance_silent:
            task_manager.update_status_from_thread(progress, message)

    _status(0, '正在同步共享分享审核状态...')
    if not _enabled():
        _status(100, '共享资源未启用，跳过。')
        return
    client = SharedCenterClient()
    if not client.ready:
        _status(100, '共享中心地址或 device_token 未配置，跳过。')
        return

    total = {}
    try:
        if _shared_auto_share_requests_enabled():
            _status(15, '正在自动响应中心求分享...')
            _merge_maintenance_counts(total, _auto_respond_open_share_requests(client, max_requests=20))
        _status(45, '正在检查 115 分享审核状态并登记中心...')
        total.update(_auto_check_and_report_local_shares(client, max_records=80))
        _status(80, '正在确保共享中心事件监听...')
        total['device_event_listener'] = ensure_shared_device_event_listener()
        logger.debug(
            "\n=== 共享分享状态同步完成 ===\n"
            f"  ➜ 自动响应求分享: 检查 {total.get('share_request_auto_checked', 0)}，创建 {total.get('share_request_auto_created', 0)}，跳过 {total.get('share_request_auto_skipped', 0)}，失败 {total.get('share_request_auto_failed', 0)}\n"
            f"  ➜ 分享状态同步: 检查 {total.get('checked', 0)}，自动登记 {total.get('reported', 0)}，中心补登 {total.get('resynced', 0)}，清理失效 {total.get('cancelled', 0)}\n"
            f"  ➜ 共享事件监听: {'已启动/运行中' if total.get('device_event_listener') else '未启动'}\n"
            "========================"
        )
        _status(
            100,
            f"同步完成：自动响应求分享创建 {total.get('share_request_auto_created', 0)}；检查 {total.get('checked', 0)}，自动登记 {total.get('reported', 0)}，中心补登 {total.get('resynced', 0)}，清理失效 {total.get('cancelled', 0)}。"
        )
    except Exception as e:
        logger.error(f"  ➜ [共享资源] 高频分享状态同步失败: {e}", exc_info=True)
        _status(100, f'共享分享状态同步失败：{e}')
        raise


def task_shared_resource_maintenance(processor=None, maintenance_silent: bool = False):
    """共享资源基础维护任务。

    重构后不再承担业务触发：
    - 不再扫描中心缺口自动创建分享；Movie/Episode 改由 webhook 入库事件触发。
    - 不再处理追更消费/登记缺口；统一订阅负责共享池优先消费与缺口登记。
    - 不再汇总完结季包；智能追剧确认完美完结后触发。
    这里只保留低频清理/健康检查/对账/贡献值同步。
    """
    def _status(progress: int, message: str):
        if maintenance_silent:
            return
        task_manager.update_status_from_thread(progress, message)

    try:
        _status(0, '正在初始化共享资源基础维护任务...')
        if not _enabled():
            _status(100, '共享资源未启用，跳过。')
            return
        client = SharedCenterClient()
        if not client.ready:
            _status(100, '共享中心地址或 device_token 未配置，跳过。')
            return

        total = {}
        _status(25, '正在检查分享水位并清理违规分享...')
        _merge_maintenance_counts(total, _enforce_local_share_waterline(client))

        _status(55, '正在对账中心残留共享源...')
        total.update(_cleanup_orphan_center_sources(client))

        _status(75, '正在复查分享水位...')
        _merge_maintenance_counts(total, _enforce_local_share_waterline(client))

        _status(88, '正在同步贡献值快照...')
        try:
            from routes.shared_resource import _fetch_center_credit
            total['credit'] = _fetch_center_credit().get('ok', False)
        except Exception as e:
            logger.warning(f"  ➜ [共享资源维护] 同步贡献值失败: {e}")
            total['credit'] = False

        log_msg = (
            "\n=== 共享资源基础维护完成 ===\n"
            f"  ➜ 违规分享清理: {total.get('share_invalid_deleted', 0)}/{total.get('share_invalid_failed', 0)}\n"
            f"  ➜ 缺 raw 清理: {total.get('share_raw_missing_deleted', 0)}/{total.get('share_raw_missing_failed', 0)}\n"
            f"  ➜ 分享水位清理: {total.get('share_pruned', 0)}/{total.get('share_prune_failed', 0)}\n"
            f"  ➜ 中心残留对账: 检查 {total.get('center_orphan_checked', 0)}，跳过外部 {total.get('center_orphan_skipped_external', 0)}，撤销 {total.get('center_orphan_cancelled', 0)}，失败 {total.get('center_orphan_failed', 0)}\n"
            f"  ➜ 贡献值同步: {'成功' if total.get('credit') else '失败/跳过'}\n"
            "========================"
        )
        logger.info(log_msg)
        _status(
            100,
            f"基础维护完成：水位清理 {total.get('share_pruned', 0)}，中心残留撤销 {total.get('center_orphan_cancelled', 0)}。"
        )
    except Exception as e:
        logger.error(f"  ➜ [共享资源维护] 基础维护失败: {e}", exc_info=True)
        _status(100, f'共享资源基础维护失败：{e}')
        raise


def trigger_shared_resource_maintenance_task() -> bool:
    """供路由/调度器调用的统一入口。"""
    return task_manager.submit_task(
        task_shared_resource_maintenance,
        '共享资源自动维护',
        processor_type='media',
    )
