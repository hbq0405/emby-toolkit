# database/shared_credit_db.py
# 共享资源贡献值快照、贡献值流水与首页统计。
#
# 虚拟入库已移除。本模块只负责共享中心贡献值、我的分享统计，
# 不再访问 shared_virtual_items，也不再提供虚拟入库/临时转存/自动转正相关函数。

import json
import logging
import re
from datetime import datetime
from typing import Any, Dict, List

from database.connection import get_db_connection

logger = logging.getLogger(__name__)


def _json_default(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return str(obj)


def _as_jsonb(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, default=_json_default)


def _row_to_dict(row):
    if row is None:
        return None
    if isinstance(row, dict):
        return dict(row)
    try:
        return dict(row)
    except Exception:
        return row


def get_shared_resource_summary() -> Dict[str, Any]:
    """共享资源首页摘要。

    只统计我的分享与共享中心贡献值快照。
    虚拟入库已移除，这里不再返回 local/virtual 统计，也不访问 shared_virtual_items。
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE status IN ('pending_review','creating')) AS pending,
                    COUNT(*) FILTER (WHERE status IN ('alive','reported')) AS alive,
                    COUNT(*) FILTER (WHERE center_status='reported') AS reported,
                    COUNT(*) FILTER (WHERE status IN ('rejected','dead','error')) AS failed
                FROM shared_share_records
                """
            )
            shares = _row_to_dict(cur.fetchone()) or {}

            cur.execute("SELECT * FROM shared_credit_snapshot WHERE id=1")
            credit = _row_to_dict(cur.fetchone()) or {}

    return {
        "shares": shares,
        "credit": credit,
    }


def upsert_credit_snapshot(data: Dict[str, Any]):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO shared_credit_snapshot(
                    id, device_id, credit, contributed_sources, consumed_sources,
                    transfer_success, transfer_failed, wanted_gaps, shared_sources,
                    raw_ffprobe, remote_devices, raw_json, updated_at
                ) VALUES(1,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,NOW())
                ON CONFLICT(id) DO UPDATE SET
                    device_id=EXCLUDED.device_id,
                    credit=EXCLUDED.credit,
                    contributed_sources=EXCLUDED.contributed_sources,
                    consumed_sources=EXCLUDED.consumed_sources,
                    transfer_success=EXCLUDED.transfer_success,
                    transfer_failed=EXCLUDED.transfer_failed,
                    wanted_gaps=EXCLUDED.wanted_gaps,
                    shared_sources=EXCLUDED.shared_sources,
                    raw_ffprobe=EXCLUDED.raw_ffprobe,
                    remote_devices=EXCLUDED.remote_devices,
                    raw_json=EXCLUDED.raw_json,
                    updated_at=NOW()
                RETURNING *
                """,
                (
                    data.get('device_id'), int(data.get('credit') or 0),
                    int(data.get('contributed_sources') or 0), int(data.get('consumed_sources') or 0),
                    int(data.get('transfer_success') or 0), int(data.get('transfer_failed') or 0),
                    int(data.get('wanted_gaps') or 0), int(data.get('shared_sources') or 0),
                    int(data.get('raw_ffprobe') or 0), int(data.get('remote_devices') or 0),
                    _as_jsonb(data.get('raw_json') or data),
                ),
            )
            row = _row_to_dict(cur.fetchone())
            conn.commit()
            return row


def get_credit_snapshot():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM shared_credit_snapshot WHERE id=1")
            return _row_to_dict(cur.fetchone()) or {}


def add_credit_ledger(
    event_type,
    delta=0,
    reason='',
    ref_id='',
    source_id='',
    virtual_id='',  # 兼容旧调用签名；虚拟入库已移除，不再写入数据库。
    tmdb_id='',
    item_type='',
    title='',
    raw_json=None,
):
    """写入本地贡献值/操作流水。

    virtual_id 参数仅为兼容旧调用点保留；SQL 不再引用 virtual_id 列，
    这样即使你后续把 shared_credit_ledger_local.virtual_id 也 DROP 掉，
    这里仍然不会炸。
    """
    payload = raw_json if isinstance(raw_json, dict) else {}
    if virtual_id:
        payload = dict(payload)
        payload.setdefault('legacy_virtual_id', str(virtual_id))

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO shared_credit_ledger_local(
                    event_type, delta, reason, ref_id, source_id,
                    tmdb_id, item_type, title, raw_json
                ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                RETURNING *
                """,
                (event_type, int(delta or 0), reason, ref_id, source_id, tmdb_id, item_type, title, _as_jsonb(payload)),
            )
            row = _row_to_dict(cur.fetchone())
            conn.commit()
            return row


def _normalize_center_reason_code(reason: str) -> str:
    """中心 reason 归一化。

    本地展示表的 event_type 会自动加 center_ 前缀；如果中心或历史数据已经
    带了 center_，这里先剥掉，避免出现 center_center_xxx。
    """
    reason = str(reason or '').strip()
    if reason.startswith('center_'):
        reason = reason[len('center_'):]
    return reason


def _center_source_provider(item: Dict[str, Any]) -> str:
    item = item or {}
    provider = str(item.get('source_provider') or '').strip().lower().replace('-', '_').replace(' ', '_')
    if provider:
        return provider
    raw = item.get('raw_json') if isinstance(item.get('raw_json'), dict) else {}
    center_item = raw.get('center_item') if isinstance(raw.get('center_item'), dict) else {}
    return str(center_item.get('source_provider') or '').strip().lower().replace('-', '_').replace(' ', '_')


def _is_backup_center_ledger_item(item: Dict[str, Any], reason: str = '') -> bool:
    """判断中心流水是否为备份分享入池。

    新中心会带 source_provider=backup_mirror；少数历史补丁可能直接把 reason
    写成 backup_source_registered，也一并兼容。
    """
    reason = _normalize_center_reason_code(reason)
    if reason in {'backup_source_registered', 'backup_share_registered'}:
        return True
    provider = _center_source_provider(item)
    if provider in {'backup_mirror', 'backup_share', 'auto_backup_share'}:
        return True
    return False


def _center_credit_event_label(reason: str, item: Dict[str, Any] = None) -> str:
    reason = _normalize_center_reason_code(reason)
    if _is_backup_center_ledger_item(item or {}, reason):
        return '备份分享入池'
    mapping = {
        'initial_credit': '设备注册基础贡献值',
        'source_registered': '成功分享视频，中心首次登记',
        'shared_source_served': '共享资源被其他设备成功转存',
        'shared_source_consumed': '从共享中心成功转存资源',
    }
    return mapping.get(reason, reason or '中心贡献值变化')


def _safe_int(value, default=0) -> int:
    try:
        if value is None or value == '':
            return default
        return int(value)
    except Exception:
        return default


def _extract_episode_code(item: Dict[str, Any]) -> str:
    """从中心流水附带的季/集字段或文件名里提取 SxxExx。"""
    season = _safe_int(item.get('season_number'))
    episode = _safe_int(item.get('episode_number'))
    if season > 0 and episode > 0:
        return f"S{season:02d}E{episode:02d}"
    if season > 0:
        return f"S{season:02d}"

    text = ' '.join([
        str(item.get('title') or ''),
        str(item.get('file_name') or ''),
    ])
    match = re.search(r'(?i)\bS(\d{1,2})\s*[._ -]*E(\d{1,3})\b', text)
    if match:
        return f"S{int(match.group(1)):02d}E{int(match.group(2)):02d}"

    match = re.search(r'第\s*(\d{1,2})\s*季.*?第\s*(\d{1,3})\s*[集话話]', text)
    if match:
        return f"S{int(match.group(1)):02d}E{int(match.group(2)):02d}"

    return ''


def _center_credit_display_title(item: Dict[str, Any]) -> str:
    base_title = str(item.get('title') or item.get('file_name') or item.get('ref_id') or '').strip()
    code = _extract_episode_code(item)
    if not code:
        return base_title
    if re.search(re.escape(code), base_title, re.IGNORECASE):
        return base_title
    return f"{base_title} {code}" if base_title else code


def sync_center_credit_ledger(items: List[Dict[str, Any]], device_snapshot: Dict[str, Any] = None) -> int:
    """同步中心服务器真实贡献值流水到本地展示表。"""
    items = list(items or [])
    device_snapshot = device_snapshot or {}
    device_id = device_snapshot.get('device_id') or device_snapshot.get('id') or ''
    initial_credit_created_at = device_snapshot.get('created_at') or device_snapshot.get('updated_at')

    if device_id and not initial_credit_created_at:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT created_at
                    FROM shared_credit_ledger_local
                    WHERE event_type = 'center_initial_credit'
                      AND ref_id = %s
                    ORDER BY created_at ASC
                    LIMIT 1
                    """,
                    (device_id,),
                )
                existing = _row_to_dict(cur.fetchone()) or {}
                initial_credit_created_at = existing.get('created_at')

    # 中心基础 20 分来自 devices.credit 默认值，不在 credit_ledger 里；本地展示补一条虚拟流水。
    if device_id:
        items.append({
            'id': f'base:{device_id}',
            'device_id': device_id,
            'delta': 20,
            'reason': 'initial_credit',
            'ref_id': device_id,
            'source_id': '',
            'tmdb_id': '',
            'item_type': '',
            'title': '基础贡献值',
            'file_name': '',
            'created_at': initial_credit_created_at,
        })

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # 只清理中心同步记录，保留本地操作审计记录。
            cur.execute("DELETE FROM shared_credit_ledger_local WHERE event_type LIKE 'center_%'")
            count = 0
            for item in items:
                reason_code = _normalize_center_reason_code(item.get('reason') or '')
                delta = int(item.get('delta') or 0)
                ref_id = str(item.get('ref_id') or item.get('source_id') or item.get('id') or '')
                title = item.get('title') or item.get('file_name') or ''
                file_name = item.get('file_name') or ''
                tmdb_id = item.get('tmdb_id') or ''
                item_type = item.get('item_type') or ''
                event_reason_code = 'backup_source_registered' if _is_backup_center_ledger_item(item, reason_code) else reason_code
                source_related_reasons = {'source_registered', 'backup_source_registered', 'shared_source_served', 'shared_source_consumed'}
                source_id = str(item.get('source_id') or '').strip()
                has_source_title = bool(str(title or '').strip() or str(file_name or '').strip())
                display = _center_credit_display_title(item) if has_source_title else ''
                # 中心源被删除时 JOIN 不回标题；这类源相关流水在本地展示直接丢弃。
                if reason_code in source_related_reasons and (not source_id or not has_source_title):
                    continue
                if reason_code in source_related_reasons and re.match(r'^src_[A-Za-z0-9]+$', str(display or ref_id)):
                    continue
                if not display:
                    display = title or file_name or ref_id
                label = _center_credit_event_label(reason_code, item)
                sign = '+' if delta > 0 else ''
                reason_text = f"{label}：{display}，贡献值 {sign}{delta}"
                if reason_code == 'initial_credit':
                    reason_text = f"{label}，贡献值 +20"
                created_at = item.get('created_at')

                columns = "event_type, delta, reason, ref_id, source_id, tmdb_id, item_type, title, raw_json"
                values = [
                    f"center_{event_reason_code or 'credit'}", delta, reason_text, ref_id,
                    item.get('source_id') or ref_id, tmdb_id, item_type, display,
                    _as_jsonb({'origin': 'center', 'center_item': item}),
                ]
                placeholders = "%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb"
                if created_at:
                    columns += ", created_at"
                    placeholders += ",%s"
                    values.append(created_at)

                cur.execute(
                    f"INSERT INTO shared_credit_ledger_local({columns}) VALUES({placeholders})",
                    values,
                )
                count += 1
        conn.commit()
    return count


def list_credit_ledger(limit=50, actual_only: bool = False):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            safe_limit = min(500, int(limit or 50))
            if actual_only:
                cur.execute(
                    """
                    SELECT *
                    FROM shared_credit_ledger_local
                    WHERE delta <> 0 OR event_type LIKE %s
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    ("center_%", safe_limit),
                )
            else:
                cur.execute(
                    """
                    SELECT *
                    FROM shared_credit_ledger_local
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (safe_limit,),
                )
            return [_row_to_dict(r) for r in cur.fetchall()]
