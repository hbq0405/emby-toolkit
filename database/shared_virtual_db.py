# database/shared_virtual_db.py
# 共享资源虚拟入库管理：本地虚拟项、贡献值快照与流水
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

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
    return dict(row)


def list_virtual_items(
    *,
    status: str = "all",
    item_type: str = "all",
    keyword: str = "",
    page: int = 1,
    page_size: int = 30,
) -> Tuple[List[Dict[str, Any]], int]:
    page = max(1, int(page or 1))
    page_size = max(1, min(int(page_size or 30), 200))
    offset = (page - 1) * page_size

    where = []
    params: List[Any] = []

    if status and status != "all":
        where.append("status = %s")
        params.append(status)

    if item_type and item_type != "all":
        where.append("item_type = %s")
        params.append(item_type)

    if keyword:
        where.append("(title ILIKE %s OR file_name ILIKE %s OR tmdb_id = %s OR sha1 ILIKE %s)")
        kw = f"%{keyword.strip()}%"
        params.extend([kw, kw, keyword.strip(), f"{keyword.strip()}%"])

    where_sql = "WHERE " + " AND ".join(where) if where else ""

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) AS total FROM shared_virtual_items {where_sql}", params)
            total = int(cursor.fetchone()["total"] or 0)

            cursor.execute(
                f"""
                SELECT *
                FROM shared_virtual_items
                {where_sql}
                ORDER BY updated_at DESC, created_at DESC
                LIMIT %s OFFSET %s
                """,
                params + [page_size, offset],
            )
            rows = [_row_to_dict(r) for r in cursor.fetchall()]

    return rows, total


def get_virtual_item(virtual_id: str) -> Optional[Dict[str, Any]]:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM shared_virtual_items WHERE virtual_id = %s", (virtual_id,))
            return _row_to_dict(cursor.fetchone())


def mark_virtual_deleted(virtual_id: str, *, message: str = "手动删除") -> Optional[Dict[str, Any]]:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE shared_virtual_items
                SET status = 'deleted', deleted_at = NOW(), updated_at = NOW(), last_error = %s
                WHERE virtual_id = %s
                RETURNING *
                """,
                (message, virtual_id),
            )
            row = _row_to_dict(cursor.fetchone())
            conn.commit()
            return row


def mark_virtual_promoted(
    virtual_id: str,
    *,
    promoted_fid: Optional[str] = None,
    promoted_pick_code: Optional[str] = None,
    message: str = "手动转正",
) -> Optional[Dict[str, Any]]:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE shared_virtual_items
                SET status = 'promoted',
                    promoted_at = NOW(),
                    updated_at = NOW(),
                    promoted_fid = COALESCE(%s, promoted_fid, real_fid),
                    promoted_pick_code = COALESCE(%s, promoted_pick_code, real_pick_code),
                    last_error = %s
                WHERE virtual_id = %s
                RETURNING *
                """,
                (promoted_fid, promoted_pick_code, message, virtual_id),
            )
            row = _row_to_dict(cursor.fetchone())
            conn.commit()
            return row


def add_credit_ledger(
    *,
    event_type: str,
    delta: int = 0,
    reason: str = "",
    ref_id: str = "",
    source_id: str = "",
    virtual_id: str = "",
    tmdb_id: str = "",
    item_type: str = "",
    title: str = "",
    raw_json: Optional[Dict[str, Any]] = None,
) -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO shared_credit_ledger_local(
                    event_type, delta, reason, ref_id, source_id, virtual_id,
                    tmdb_id, item_type, title, raw_json
                )
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                """,
                (event_type, int(delta or 0), reason, ref_id, source_id, virtual_id, tmdb_id, item_type, title, _as_jsonb(raw_json)),
            )
            conn.commit()


def list_credit_ledger(limit: int = 50) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit or 50), 200))
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM shared_credit_ledger_local
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [_row_to_dict(r) for r in cursor.fetchall()]


def upsert_credit_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    snapshot = snapshot or {}
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO shared_credit_snapshot(
                    id, device_id, credit, contributed_sources, consumed_sources,
                    transfer_success, transfer_failed, wanted_gaps, shared_sources,
                    raw_ffprobe, remote_devices, raw_json, updated_at
                )
                VALUES(1,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,NOW())
                ON CONFLICT(id)
                DO UPDATE SET
                    device_id = EXCLUDED.device_id,
                    credit = EXCLUDED.credit,
                    contributed_sources = EXCLUDED.contributed_sources,
                    consumed_sources = EXCLUDED.consumed_sources,
                    transfer_success = EXCLUDED.transfer_success,
                    transfer_failed = EXCLUDED.transfer_failed,
                    wanted_gaps = EXCLUDED.wanted_gaps,
                    shared_sources = EXCLUDED.shared_sources,
                    raw_ffprobe = EXCLUDED.raw_ffprobe,
                    remote_devices = EXCLUDED.remote_devices,
                    raw_json = EXCLUDED.raw_json,
                    updated_at = NOW()
                RETURNING *
                """,
                (
                    snapshot.get("device_id"),
                    int(snapshot.get("credit") or 0),
                    int(snapshot.get("contributed_sources") or 0),
                    int(snapshot.get("consumed_sources") or 0),
                    int(snapshot.get("transfer_success") or 0),
                    int(snapshot.get("transfer_failed") or 0),
                    int(snapshot.get("wanted_gaps") or 0),
                    int(snapshot.get("shared_sources") or 0),
                    int(snapshot.get("raw_ffprobe") or 0),
                    int(snapshot.get("remote_devices") or 0),
                    _as_jsonb(snapshot.get("raw_json") or snapshot),
                ),
            )
            row = _row_to_dict(cursor.fetchone())
            conn.commit()
            return row


def get_credit_snapshot() -> Dict[str, Any]:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM shared_credit_snapshot WHERE id = 1")
            row = cursor.fetchone()
            return _row_to_dict(row) if row else {
                "device_id": None,
                "credit": 0,
                "contributed_sources": 0,
                "consumed_sources": 0,
                "transfer_success": 0,
                "transfer_failed": 0,
                "wanted_gaps": 0,
                "shared_sources": 0,
                "raw_ffprobe": 0,
                "remote_devices": 0,
                "raw_json": {},
                "updated_at": None,
            }


def get_local_summary() -> Dict[str, Any]:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE status = 'virtual_ready') AS virtual_ready,
                    COUNT(*) FILTER (WHERE status = 'cached') AS cached,
                    COUNT(*) FILTER (WHERE status = 'watched') AS watched,
                    COUNT(*) FILTER (WHERE status = 'promoted') AS promoted,
                    COUNT(*) FILTER (WHERE status = 'deleted') AS deleted,
                    COUNT(*) FILTER (WHERE status = 'error') AS error,
                    COALESCE(SUM(size) FILTER (WHERE status IN ('cached','watched')), 0) AS cached_size
                FROM shared_virtual_items
                """
            )
            local = _row_to_dict(cursor.fetchone()) or {}

    credit = get_credit_snapshot()
    return {"local": local, "credit": credit}
