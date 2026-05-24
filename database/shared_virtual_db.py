# database/shared_virtual_db.py
# 共享资源虚拟入库管理：本地虚拟项、贡献值快照与流水
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Tuple

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


def get_local_summary() -> Dict[str, Any]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE status='virtual_ready') AS virtual_ready,
                    COUNT(*) FILTER (WHERE status IN ('cached','watched')) AS cached,
                    COUNT(*) FILTER (WHERE status='promoted') AS promoted,
                    COUNT(*) FILTER (WHERE status='deleted') AS deleted,
                    COALESCE(SUM(size) FILTER (WHERE status IN ('cached','watched')), 0) AS cached_size
                FROM shared_virtual_items
            """)
            local = _row_to_dict(cur.fetchone()) or {}

            cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE status IN ('pending_review','creating')) AS pending,
                    COUNT(*) FILTER (WHERE status IN ('alive','reported')) AS alive,
                    COUNT(*) FILTER (WHERE center_status='reported') AS reported,
                    COUNT(*) FILTER (WHERE status IN ('rejected','dead','error')) AS failed
                FROM shared_share_records
            """)
            shares = _row_to_dict(cur.fetchone()) or {}

            cur.execute("SELECT * FROM shared_credit_snapshot WHERE id=1")
            credit = _row_to_dict(cur.fetchone()) or {}

    return {"local": local, "shares": shares, "credit": credit}


def list_virtual_items(status='all', item_type='all', keyword='', page=1, page_size=30) -> Tuple[List[Dict[str, Any]], int]:
    page = max(1, int(page or 1))
    page_size = min(100, max(1, int(page_size or 30)))
    where = []
    args = []
    if status and status != 'all':
        where.append('status = %s')
        args.append(status)
    if item_type and item_type != 'all':
        where.append('item_type = %s')
        args.append(item_type)
    if keyword:
        where.append('(title ILIKE %s OR file_name ILIKE %s OR tmdb_id ILIKE %s OR sha1 ILIKE %s)')
        kw = f'%{keyword}%'
        args.extend([kw, kw, kw, kw])
    where_sql = 'WHERE ' + ' AND '.join(where) if where else ''
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS n FROM shared_virtual_items {where_sql}", args)
            total = int((_row_to_dict(cur.fetchone()) or {}).get('n') or 0)
            cur.execute(
                f"""
                SELECT * FROM shared_virtual_items
                {where_sql}
                ORDER BY updated_at DESC
                LIMIT %s OFFSET %s
                """,
                args + [page_size, (page - 1) * page_size]
            )
            rows = [_row_to_dict(r) for r in cur.fetchall()]
    return rows, total


def get_virtual_item(virtual_id: str):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM shared_virtual_items WHERE virtual_id=%s", (virtual_id,))
            return _row_to_dict(cur.fetchone())


def mark_virtual_deleted(virtual_id: str, message=''):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE shared_virtual_items
                SET status='deleted', deleted_at=NOW(), updated_at=NOW(), last_error=%s
                WHERE virtual_id=%s
                RETURNING *
            """, (message, virtual_id))
            row = _row_to_dict(cur.fetchone())
            conn.commit()
            return row


def mark_virtual_promoted(virtual_id: str, promoted_fid='', promoted_pick_code='', message=''):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE shared_virtual_items
                SET status='promoted', promoted_at=NOW(), updated_at=NOW(),
                    promoted_fid=%s, promoted_pick_code=%s, last_error=%s
                WHERE virtual_id=%s
                RETURNING *
            """, (promoted_fid, promoted_pick_code, message, virtual_id))
            row = _row_to_dict(cur.fetchone())
            conn.commit()
            return row


def upsert_credit_snapshot(data: Dict[str, Any]):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
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
            """, (
                data.get('device_id'), int(data.get('credit') or 0),
                int(data.get('contributed_sources') or 0), int(data.get('consumed_sources') or 0),
                int(data.get('transfer_success') or 0), int(data.get('transfer_failed') or 0),
                int(data.get('wanted_gaps') or 0), int(data.get('shared_sources') or 0),
                int(data.get('raw_ffprobe') or 0), int(data.get('remote_devices') or 0),
                _as_jsonb(data.get('raw_json') or data),
            ))
            row = _row_to_dict(cur.fetchone())
            conn.commit()
            return row


def add_credit_ledger(event_type, delta=0, reason='', ref_id='', source_id='', virtual_id='', tmdb_id='', item_type='', title='', raw_json=None):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO shared_credit_ledger_local(
                    event_type, delta, reason, ref_id, source_id, virtual_id,
                    tmdb_id, item_type, title, raw_json
                ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                RETURNING *
            """, (event_type, int(delta or 0), reason, ref_id, source_id, virtual_id, tmdb_id, item_type, title, _as_jsonb(raw_json)))
            row = _row_to_dict(cur.fetchone())
            conn.commit()
            return row


def list_credit_ledger(limit=50):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM shared_credit_ledger_local ORDER BY created_at DESC LIMIT %s", (min(200, int(limit or 50)),))
            return [_row_to_dict(r) for r in cur.fetchall()]
