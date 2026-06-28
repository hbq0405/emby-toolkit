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


def _rows(rows):
    return [dict(r) for r in rows or []]


def _safe_int(value, default=0):
    try:
        if value in (None, ''):
            return default
        return int(float(value))
    except Exception:
        return default


def _safe_float(value, default=0.0):
    try:
        if value in (None, ''):
            return default
        return float(value)
    except Exception:
        return default


def create_virtual_import(data: Dict[str, Any]) -> Dict[str, Any]:
    data = dict(data or {})
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO shared_virtual_imports(
                    source_kind, source_id, tmdb_id, item_type, parent_series_tmdb_id,
                    season_number, episode_number, title, release_year, file_count, total_size,
                    status, strm_paths_json, source_payload_json, files_json, updated_at
                ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb,NOW())
                RETURNING *
                """,
                (
                    data.get('source_kind') or '',
                    data.get('source_id') or '',
                    data.get('tmdb_id') or '',
                    data.get('item_type') or '',
                    data.get('parent_series_tmdb_id') or '',
                    data.get('season_number'),
                    data.get('episode_number'),
                    data.get('title') or '',
                    data.get('release_year'),
                    _safe_int(data.get('file_count'), 0),
                    _safe_int(data.get('total_size'), 0),
                    data.get('status') or 'virtual',
                    _as_jsonb(data.get('strm_paths_json') or []),
                    _as_jsonb(data.get('source_payload_json') or {}),
                    _as_jsonb(data.get('files_json') or []),
                ),
            )
            row = _row(cur.fetchone())
            conn.commit()
            return row


def get_virtual_import(virtual_id: int) -> Dict[str, Any]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM shared_virtual_imports WHERE id=%s", (int(virtual_id),))
            return _row(cur.fetchone()) or {}


def list_virtual_imports(status: str = '', keyword: str = '', item_type: str = '', page: int = 1, page_size: int = 30) -> Dict[str, Any]:
    page = max(1, _safe_int(page, 1))
    page_size = max(1, min(_safe_int(page_size, 30), 200))
    where, args = [], []
    if status and status != 'all':
        where.append("status=%s")
        args.append(status)
    item_type = str(item_type or '').strip().lower()
    if item_type and item_type != 'all':
        if item_type in {'movie', 'film'}:
            where.append("LOWER(item_type)='movie'")
        elif item_type in {'tv', 'series', 'season', 'episode'}:
            where.append("LOWER(item_type) IN ('series','season','episode','tv')")
    if keyword:
        kw = f"%{keyword}%"
        where.append("(title ILIKE %s OR source_id ILIKE %s OR tmdb_id ILIKE %s)")
        args.extend([kw, kw, kw])
    where_sql = "WHERE " + " AND ".join(where) if where else ""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS n FROM shared_virtual_imports {where_sql}", args)
            total = _safe_int((_row(cur.fetchone()) or {}).get('n'), 0)
            cur.execute(
                f"""
                SELECT *
                FROM shared_virtual_imports
                {where_sql}
                ORDER BY updated_at DESC NULLS LAST, id DESC
                LIMIT %s OFFSET %s
                """,
                args + [page_size, (page - 1) * page_size],
            )
            return {'items': _rows(cur.fetchall()), 'total': total, 'page': page, 'page_size': page_size}


def update_virtual_import(virtual_id: int, **fields) -> Dict[str, Any]:
    allowed_json = {'strm_paths_json', 'source_payload_json', 'files_json'}
    allowed = {
        'status', 'watched_count', 'played_percent', 'last_played_at', 'promoted_at',
        'strm_paths_json', 'source_payload_json', 'files_json',
    }
    sets, args = [], []
    for key, value in fields.items():
        if key not in allowed:
            continue
        if key in allowed_json:
            sets.append(f"{key}=%s::jsonb")
            args.append(_as_jsonb(value))
        elif key in {'last_played_at', 'promoted_at'} and value == 'NOW()':
            sets.append(f"{key}=NOW()")
        else:
            sets.append(f"{key}=%s")
            args.append(value)
    if not sets:
        return get_virtual_import(virtual_id)
    sets.append("updated_at=NOW()")
    args.append(int(virtual_id))
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE shared_virtual_imports SET {', '.join(sets)} WHERE id=%s RETURNING *", args)
            row = _row(cur.fetchone())
            conn.commit()
            return row or {}


def delete_virtual_import(virtual_id: int) -> Dict[str, Any]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM shared_virtual_imports WHERE id=%s RETURNING *", (int(virtual_id),))
            row = _row(cur.fetchone())
            conn.commit()
            return row or {}


def mark_active_washing_for_virtual_import(virtual_id: int, enabled: bool = True) -> int:
    row = get_virtual_import(virtual_id)
    if not row:
        return 0

    item_type = str(row.get('item_type') or '').strip().lower()
    season_number = row.get('season_number')
    episode_number = row.get('episode_number')
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if item_type == 'movie':
                tmdb_id = str(row.get('tmdb_id') or '').strip()
                if not tmdb_id:
                    return 0
                cur.execute(
                    """
                    UPDATE media_metadata
                    SET active_washing = %s
                    WHERE tmdb_id = %s AND item_type = 'Movie'
                    """,
                    (bool(enabled), tmdb_id),
                )
            else:
                parent_tmdb_id = str(row.get('parent_series_tmdb_id') or row.get('tmdb_id') or '').strip()
                if not parent_tmdb_id:
                    return 0
                args = [bool(enabled), parent_tmdb_id]
                where = "parent_series_tmdb_id = %s AND item_type = 'Episode'"
                if season_number is not None:
                    where += " AND season_number = %s"
                    args.append(season_number)
                if episode_number is not None:
                    where += " AND episode_number = %s"
                    args.append(episode_number)
                cur.execute(f"UPDATE media_metadata SET active_washing = %s WHERE {where}", args)
            count = cur.rowcount or 0
        conn.commit()
    return count


def record_virtual_play(virtual_id: int, percent: float = 0.0) -> Dict[str, Any]:
    current = get_virtual_import(virtual_id)
    played_percent = max(_safe_float(current.get('played_percent'), 0.0), _safe_float(percent, 0.0))
    watched_count = _safe_int(current.get('watched_count'), 0) + 1
    return update_virtual_import(
        virtual_id,
        watched_count=watched_count,
        played_percent=played_percent,
        last_played_at='NOW()',
    )
