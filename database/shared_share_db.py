# database/shared_share_db.py
# 我的共享资源：本地分享记录与分享包明细
import json
from datetime import datetime
from typing import Any, Dict, List, Tuple

from database.connection import get_db_connection


def _json_default(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return str(obj)


def _as_jsonb(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, default=_json_default)


def _row_to_dict(row):
    if row is None:
        return None
    return dict(row) if isinstance(row, dict) else dict(row)


def create_share_record(data: Dict[str, Any]) -> Dict[str, Any]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO shared_share_records(
                    share_code, receive_code, share_url, share_type, root_fid, root_name,
                    root_is_dir, tmdb_id, item_type, parent_series_tmdb_id, season_number,
                    title, release_year, status, review_status, center_status, raw_json
                ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                ON CONFLICT(share_code) DO UPDATE SET
                    receive_code=EXCLUDED.receive_code,
                    share_url=EXCLUDED.share_url,
                    share_type=EXCLUDED.share_type,
                    root_fid=EXCLUDED.root_fid,
                    root_name=EXCLUDED.root_name,
                    root_is_dir=EXCLUDED.root_is_dir,
                    tmdb_id=EXCLUDED.tmdb_id,
                    item_type=EXCLUDED.item_type,
                    parent_series_tmdb_id=EXCLUDED.parent_series_tmdb_id,
                    season_number=EXCLUDED.season_number,
                    title=EXCLUDED.title,
                    release_year=EXCLUDED.release_year,
                    status=EXCLUDED.status,
                    review_status=EXCLUDED.review_status,
                    center_status=EXCLUDED.center_status,
                    raw_json=EXCLUDED.raw_json,
                    updated_at=NOW()
                RETURNING *
            """, (
                data.get('share_code'), data.get('receive_code'), data.get('share_url'),
                data.get('share_type') or 'season_pack', str(data.get('root_fid') or ''), data.get('root_name'),
                bool(data.get('root_is_dir', True)), data.get('tmdb_id'), data.get('item_type'),
                data.get('parent_series_tmdb_id'), data.get('season_number'), data.get('title'), data.get('release_year'),
                data.get('status') or 'pending_review', data.get('review_status') or 'pending_review',
                data.get('center_status') or 'not_reported', _as_jsonb(data.get('raw_json') or data),
            ))
            row = _row_to_dict(cur.fetchone())
            conn.commit()
            return row


def replace_share_items(record_id: int, items: List[Dict[str, Any]]) -> int:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM shared_share_items WHERE share_record_id=%s", (record_id,))
            count = 0
            for item in items:
                cur.execute("""
                    INSERT INTO shared_share_items(
                        share_record_id, fid, sha1, size, file_name, relative_path,
                        tmdb_id, item_type, season_number, episode_number, raw_json
                    ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                """, (
                    record_id, item.get('fid'), item.get('sha1'), int(item.get('size') or 0),
                    item.get('file_name') or item.get('name') or '', item.get('relative_path') or '',
                    item.get('tmdb_id'), item.get('item_type'), item.get('season_number'), item.get('episode_number'),
                    _as_jsonb(item.get('raw_json') or item),
                ))
                count += 1
            cur.execute("UPDATE shared_share_records SET item_count=%s, updated_at=NOW() WHERE id=%s", (count, record_id))
            conn.commit()
            return count


def list_share_records(status='all', keyword='', page=1, page_size=30) -> Tuple[List[Dict[str, Any]], int]:
    page = max(1, int(page or 1))
    page_size = min(100, max(1, int(page_size or 30)))
    where, args = [], []
    if status and status != 'all':
        where.append('(r.status=%s OR r.review_status=%s OR r.center_status=%s)')
        args.extend([status, status, status])
    if keyword:
        where.append('(r.title ILIKE %s OR r.root_name ILIKE %s OR r.share_code ILIKE %s OR r.tmdb_id ILIKE %s)')
        kw = f'%{keyword}%'
        args.extend([kw, kw, kw, kw])
    where_sql = 'WHERE ' + ' AND '.join(where) if where else ''
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS n FROM shared_share_records r {where_sql}", args)
            total = int((_row_to_dict(cur.fetchone()) or {}).get('n') or 0)
            cur.execute(f"""
                SELECT
                    r.*,
                    COALESCE(s.raw_uploaded_count, 0) AS raw_uploaded_count,
                    COALESCE(s.center_reported_count, 0) AS center_reported_count,
                    COALESCE(s.size_missing_count, 0) AS size_missing_count
                FROM shared_share_records r
                LEFT JOIN (
                    SELECT
                        share_record_id,
                        COUNT(*) FILTER (WHERE raw_ffprobe_uploaded = TRUE) AS raw_uploaded_count,
                        COUNT(*) FILTER (WHERE center_reported = TRUE) AS center_reported_count,
                        COUNT(*) FILTER (WHERE COALESCE(size, 0) <= 0) AS size_missing_count
                    FROM shared_share_items
                    GROUP BY share_record_id
                ) s ON s.share_record_id = r.id
                {where_sql}
                ORDER BY r.updated_at DESC
                LIMIT %s OFFSET %s
            """, args + [page_size, (page - 1) * page_size])
            rows = [_row_to_dict(r) for r in cur.fetchall()]
    return rows, total


def get_share_record(record_id: int):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM shared_share_records WHERE id=%s", (record_id,))
            return _row_to_dict(cur.fetchone())


def list_share_items(record_id: int):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM shared_share_items WHERE share_record_id=%s ORDER BY id", (record_id,))
            return [_row_to_dict(r) for r in cur.fetchall()]


def update_share_record(record_id: int, **fields):
    allowed = {
        'status', 'review_status', 'center_status', 'center_source_id', 'item_count', 'reported_count',
        'last_checked_at', 'reported_at', 'cancelled_at', 'last_error', 'raw_json'
    }
    sets, args = [], []
    for k, v in fields.items():
        if k not in allowed:
            continue
        if k == 'raw_json':
            sets.append(f"{k}=%s::jsonb")
            args.append(_as_jsonb(v))
        elif k in {'last_checked_at', 'reported_at', 'cancelled_at'} and v == 'NOW()':
            sets.append(f"{k}=NOW()")
        else:
            sets.append(f"{k}=%s")
            args.append(v)
    if not sets:
        return get_share_record(record_id)
    sets.append('updated_at=NOW()')
    args.append(record_id)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE shared_share_records SET {', '.join(sets)} WHERE id=%s RETURNING *", args)
            row = _row_to_dict(cur.fetchone())
            conn.commit()
            return row


def mark_item_reported(item_id: int, center_source_id: str):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE shared_share_items
                SET center_reported=TRUE, center_source_id=%s, updated_at=NOW()
                WHERE id=%s
            """, (center_source_id, item_id))
            conn.commit()


def mark_item_raw_uploaded(item_id: int, uploaded: bool = True):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE shared_share_items
                SET raw_ffprobe_uploaded=%s, updated_at=NOW()
                WHERE id=%s
            """, (bool(uploaded), item_id))
            conn.commit()


def update_share_item_size(item_id: int, size: int):
    try:
        size = int(size or 0)
    except Exception:
        size = 0
    if size <= 0:
        return
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE shared_share_items
                SET size=%s, updated_at=NOW()
                WHERE id=%s AND COALESCE(size, 0) <= 0
            """, (size, item_id))
            conn.commit()
