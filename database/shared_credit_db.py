# database/shared_credit_db.py
# 共享资源贡献值快照、流水与首页统计（Rapid v2）。
import json
from datetime import datetime
from typing import Any, Dict, List

from database.connection import get_db_connection


def _json_default(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return str(obj)


def _as_jsonb(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, default=_json_default)


def _row(row):
    return dict(row) if row is not None else None


def get_shared_resource_summary() -> Dict[str, Any]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE status IN ('active','available')) AS alive,
                    COUNT(*) FILTER (WHERE status IN ('updating','pending')) AS pending,
                    COUNT(*) FILTER (WHERE center_status='reported') AS reported,
                    COUNT(*) FILTER (WHERE status IN ('inconsistent','incomplete','error')) AS failed,
                    COUNT(*) FILTER (WHERE source_kind='movie') AS movies,
                    COUNT(*) FILTER (WHERE source_kind='episode') AS episodes,
                    COUNT(*) FILTER (WHERE source_kind='completed_season') AS completed_seasons
                FROM shared_rapid_sources
                """
            )
            shares = _row(cur.fetchone()) or {}
            cur.execute("SELECT * FROM shared_credit_snapshot WHERE id=1")
            credit = _row(cur.fetchone()) or {}
    return {"shares": shares, "credit": credit}


def upsert_credit_snapshot(data: Dict[str, Any]):
    data = dict(data or {})
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
                    data.get('device_id') or data.get('id'), int(data.get('credit') or 0),
                    int(data.get('contributed_sources') or data.get('movie_sources') or data.get('episode_sources') or 0),
                    int(data.get('consumed_sources') or 0), int(data.get('transfer_success') or 0), int(data.get('transfer_failed') or 0),
                    int(data.get('wanted_gaps') or data.get('active_gap_devices') or 0), int(data.get('shared_sources') or 0),
                    int(data.get('raw_ffprobe') or 0), int(data.get('remote_devices') or data.get('devices') or 0), _as_jsonb(data.get('raw_json') or data),
                ),
            )
            row = _row(cur.fetchone())
            conn.commit()
            return row


def get_credit_snapshot():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM shared_credit_snapshot WHERE id=1")
            return _row(cur.fetchone()) or {}


def add_credit_ledger(event_type, delta=0, reason='', ref_id='', source_id='', virtual_id='', tmdb_id='', item_type='', title='', raw_json=None):
    payload = raw_json if isinstance(raw_json, dict) else {}
    if virtual_id:
        payload = dict(payload)
        payload.setdefault('legacy_virtual_id', str(virtual_id))
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO shared_credit_ledger_local(event_type, delta, reason, ref_id, source_id, tmdb_id, item_type, title, raw_json)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                RETURNING *
                """,
                (event_type, int(delta or 0), reason, ref_id, source_id, tmdb_id, item_type, title, _as_jsonb(payload)),
            )
            row = _row(cur.fetchone())
            conn.commit()
            return row


def _center_reason_label(reason: str) -> str:
    mapping = {
        'initial_credit': '设备注册基础贡献值',
        'center_initial_credit': '基础贡献点',
        'source_registered': '共享资源首次被验证入池',
        'rapid_source_served': '共享资源被其他设备秒传',
        'rapid_source_consumed': '从共享中心秒传资源',
        'share_source_served': '115 分享被其他设备转存',
        'share_source_consumed': '从共享中心转存 115 分享资源',
        'share_request_escrow': '求资源冻结',
        'share_request_refund': '求资源退款',
    }
    return mapping.get(str(reason or '').strip(), str(reason or '').strip() or '中心贡献值变化')


def sync_center_credit_ledger(items: List[Dict[str, Any]], device_snapshot: Dict[str, Any] = None) -> int:
    items = list(items or [])
    device_snapshot = device_snapshot or {}
    device_id = device_snapshot.get('device_id') or device_snapshot.get('id') or ''
    # 不再本地伪造“基础贡献点 +20”。
    # 中心端 /credit/ledger 会返回真实 center_initial_credit 流水；
    # 本地额外 append 会让“未真正发放/已被幂等拦截”的场景也显示 +20，造成刷点错觉。
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM shared_credit_ledger_local WHERE event_type LIKE 'center_%'")
            count = 0
            for item in items:
                reason = str(item.get('reason') or '').strip()
                event_type = 'center_' + reason if not reason.startswith('center_') else reason
                title = item.get('title') or item.get('file_name') or item.get('ref_id') or _center_reason_label(reason)
                cur.execute(
                    """
                    INSERT INTO shared_credit_ledger_local(event_type, delta, reason, ref_id, source_id, tmdb_id, item_type, title, raw_json, created_at)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,COALESCE(%s::timestamptz,NOW()))
                    """,
                    (
                        event_type, int(item.get('delta') or 0), _center_reason_label(reason), item.get('ref_id'),
                        item.get('source_id') or item.get('ref_id'), item.get('tmdb_id'), item.get('item_type'), title,
                        _as_jsonb({'center_item': item}), item.get('created_at'),
                    ),
                )
                count += 1
            conn.commit()
            return count


def list_credit_ledger(limit=200, event_type='', actual_only=False):
    limit = max(1, min(int(limit or 200), 1000))
    where, args = [], []
    if event_type:
        where.append('event_type=%s')
        args.append(event_type)
    if actual_only:
        where.append("event_type <> 'center_initial_credit'")
    where_sql = 'WHERE ' + ' AND '.join(where) if where else ''
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM shared_credit_ledger_local {where_sql} ORDER BY created_at DESC, id DESC LIMIT %s", args + [limit])
            return [dict(r) for r in cur.fetchall()]
