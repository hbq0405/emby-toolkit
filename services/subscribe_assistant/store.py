import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from database.connection import get_db_connection


def read_state(key: str) -> Dict[str, Any]:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT state_json FROM subscribe_assistant_state WHERE state_key = %s",
                (key,),
            )
            row = cursor.fetchone()
    if not row:
        return {}
    value = row.get("state_json") if isinstance(row, dict) else row[0]
    return value if isinstance(value, dict) else {}


def write_state(key: str, value: Dict[str, Any]) -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO subscribe_assistant_state (state_key, state_json, updated_at)
                VALUES (%s, %s::jsonb, NOW())
                ON CONFLICT (state_key) DO UPDATE SET
                    state_json = EXCLUDED.state_json,
                    updated_at = NOW()
                """,
                (key, json.dumps(value or {}, ensure_ascii=False)),
            )
        conn.commit()


def update_state(key: str, updater) -> Dict[str, Any]:
    current = read_state(key)
    updated = updater(dict(current or {})) or {}
    write_state(key, updated)
    return updated


def upsert_snapshot(
    *,
    tmdb_id: str,
    item_type: str = "Series",
    season_number: Optional[int] = None,
    subscribe_id: Optional[int] = None,
    scope_total: int = 0,
    scope: Dict[str, Any] = None,
    subscribe: Dict[str, Any] = None,
) -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO subscribe_assistant_snapshots (
                    tmdb_id, item_type, season_number, subscribe_id,
                    scope_total, scope_json, subscribe_json, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, NOW())
                """,
                (
                    str(tmdb_id),
                    item_type,
                    season_number,
                    subscribe_id,
                    int(scope_total or 0),
                    json.dumps(scope or {}, ensure_ascii=False),
                    json.dumps(subscribe or {}, ensure_ascii=False),
                ),
            )
        conn.commit()


def get_snapshots_due(hours: int, limit: int = 100) -> List[Dict[str, Any]]:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT *
                FROM subscribe_assistant_snapshots
                WHERE last_checked_at IS NULL
                   OR last_checked_at <= NOW() - (%s || ' hours')::interval
                ORDER BY COALESCE(last_checked_at, created_at) ASC
                LIMIT %s
                """,
                (max(1, int(hours or 1)), max(1, int(limit or 100))),
            )
            return [dict(row) for row in cursor.fetchall()]


def mark_snapshot_checked(snapshot_id: int) -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE subscribe_assistant_snapshots SET last_checked_at = NOW() WHERE id = %s",
                (snapshot_id,),
            )
        conn.commit()


def get_latest_snapshot(
    *,
    tmdb_id: str,
    season_number: Optional[int] = None,
    subscribe_id: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    conditions = ["tmdb_id = %s"]
    params = [str(tmdb_id)]
    if season_number is not None:
        conditions.append("season_number = %s")
        params.append(int(season_number))
    if subscribe_id is not None:
        conditions.append("subscribe_id = %s")
        params.append(int(subscribe_id))

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT *
                FROM subscribe_assistant_snapshots
                WHERE {' AND '.join(conditions)}
                ORDER BY created_at DESC
                LIMIT 1
                """,
                tuple(params),
            )
            row = cursor.fetchone()
            return dict(row) if row else None


def cleanup_snapshots(retention_days: int) -> int:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM subscribe_assistant_snapshots
                WHERE created_at < NOW() - (%s || ' days')::interval
                """,
                (max(1, int(retention_days or 1)),),
            )
            count = cursor.rowcount
        conn.commit()
    return count


def record_deleted_resource(
    fingerprint: str,
    *,
    tmdb_id: str = None,
    season_number: int = None,
    episodes: List[int] = None,
    reason: str = "",
    retention_hours: int = 24,
) -> None:
    expires_at = datetime.now(timezone.utc) + timedelta(hours=max(1, int(retention_hours or 1)))
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO subscribe_assistant_delete_records (
                    fingerprint, tmdb_id, season_number, episode_json, reason, created_at, expires_at
                )
                VALUES (%s, %s, %s, %s::jsonb, %s, NOW(), %s)
                ON CONFLICT (fingerprint) DO UPDATE SET
                    reason = EXCLUDED.reason,
                    expires_at = EXCLUDED.expires_at,
                    created_at = NOW()
                """,
                (
                    fingerprint,
                    tmdb_id,
                    season_number,
                    json.dumps(episodes or [], ensure_ascii=False),
                    reason,
                    expires_at,
                ),
            )
        conn.commit()


def cleanup_delete_records() -> int:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM subscribe_assistant_delete_records WHERE expires_at <= NOW()")
            count = cursor.rowcount
        conn.commit()
    return count
