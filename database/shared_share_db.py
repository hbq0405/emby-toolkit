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
                    root_is_dir, tmdb_id, item_type, parent_series_tmdb_id, season_number, episode_number,
                    title, release_year, status, review_status, center_status, raw_json
                ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
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
                    episode_number=EXCLUDED.episode_number,  -- 👈 【新增】冲突更新
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
                data.get('parent_series_tmdb_id'), data.get('season_number'), 
                data.get('episode_number'),  # 👈 【新增】参数映射
                data.get('title'), data.get('release_year'),
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
    if status == 'active':
        where.append("r.status NOT IN ('cancelled', 'deleted', 'cancel_failed')")
    elif status and status != 'all':
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
        'last_checked_at', 'reported_at', 'cancelled_at', 'last_error', 'raw_json', 'episode_number'
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


# ======================================================================
# 统一查询入口：手动分享与自动维护共同复用
# ======================================================================

def active_share_statuses() -> List[str]:
    """本地仍应视为占用 115 分享名额/仍在处理中的分享状态。"""
    return [
        'pending_review', 'alive', 'reported', 'partial', 'not_reported', 'review_failed',
        'blocked', 'violation', 'cancel_failed',
    ]


def invalid_share_statuses() -> List[str]:
    """115 已判定违规/风控或上次删除失败，维护任务应优先删除/重试的状态。"""
    return ['blocked', 'violation', 'cancel_failed']


def _status_list(statuses=None) -> List[str]:
    values = statuses if statuses is not None else active_share_statuses()
    if isinstance(values, (str, bytes)):
        values = [str(values)]
    result = []
    seen = set()
    for value in values or []:
        status = str(value or '').strip()
        if not status or status in seen:
            continue
        seen.add(status)
        result.append(status)
    return result or active_share_statuses()


def _safe_int(v, default=0):
    try:
        if v in (None, ''):
            return default
        return int(float(v))
    except Exception:
        return default


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


def count_active_share_records(statuses=None) -> int:
    """统计仍占用 115 分享名额的本地分享记录数量。"""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) AS n
                FROM shared_share_records
                WHERE COALESCE(share_code, '') <> ''
                  AND status = ANY(%s)
                """,
                (_status_list(statuses),),
            )
            row = _row_to_dict(cur.fetchone()) or {}
            return int(row.get('n') or 0)


def list_active_share_records(limit: int = 100, statuses=None, order_by: str = 'created_asc') -> List[Dict[str, Any]]:
    """查询仍在处理/占用名额的本地分享记录。"""
    limit = max(1, int(limit or 100))
    order_sql = 'created_at ASC NULLS FIRST, id ASC'
    if order_by == 'updated_desc':
        order_sql = 'updated_at DESC NULLS LAST, id DESC'
    elif order_by == 'created_desc':
        order_sql = 'created_at DESC NULLS LAST, id DESC'

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT *
                FROM shared_share_records
                WHERE COALESCE(share_code, '') <> ''
                  AND status = ANY(%s)
                ORDER BY {order_sql}
                LIMIT %s
                """,
                (_status_list(statuses), limit),
            )
            return [_row_to_dict(r) for r in cur.fetchall()]


def list_invalid_share_records(limit: int = 100, invalid_statuses=None, review_statuses=None) -> List[Dict[str, Any]]:
    """查询违规/风控/删除失败的本地分享记录，供维护任务优先清理。"""
    limit = max(1, int(limit or 100))
    invalid_statuses = _status_list(invalid_statuses or invalid_share_statuses())
    review_statuses = _status_list(review_statuses or ['blocked', 'violation'])
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM shared_share_records
                WHERE COALESCE(share_code, '') <> ''
                  AND (
                        status = ANY(%s)
                     OR review_status = ANY(%s)
                     OR center_status = 'cancel_failed'
                  )
                ORDER BY created_at ASC NULLS FIRST, id ASC
                LIMIT %s
                """,
                (invalid_statuses, review_statuses, limit),
            )
            return [_row_to_dict(r) for r in cur.fetchall()]


def load_share_waterline_candidates(target_active: int, statuses=None) -> Tuple[int, List[Dict[str, Any]]]:
    """按“转存热度 + 创建时间保护”综合评分，取出应清理的超额分享。"""
    target_active = max(0, int(target_active or 0))
    statuses = _status_list(statuses)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH active_records AS (
                    SELECT r.*
                    FROM shared_share_records r
                    WHERE COALESCE(r.share_code, '') <> ''
                      AND r.status = ANY(%s)
                ), stats AS (
                    SELECT
                        r.id AS record_id,
                        COUNT(DISTINCT l.id) FILTER (WHERE l.id IS NOT NULL) AS served_count,
                        MAX(l.created_at) FILTER (WHERE l.id IS NOT NULL) AS last_served_at
                    FROM active_records r
                    LEFT JOIN shared_share_items i ON i.share_record_id = r.id
                    LEFT JOIN shared_credit_ledger_local l
                      ON l.event_type = 'center_shared_source_served'
                     AND COALESCE(l.source_id, '') <> ''
                     AND l.source_id IN (COALESCE(i.center_source_id, ''), COALESCE(r.center_source_id, ''))
                    GROUP BY r.id
                ), scored AS (
                    SELECT
                        r.*,
                        COALESCE(s.served_count, 0) AS served_count,
                        s.last_served_at,
                        EXTRACT(EPOCH FROM (NOW() - COALESCE(r.created_at, NOW()))) / 86400.0 AS age_days,
                        COUNT(*) OVER() AS total_active,
                        (
                            COALESCE(s.served_count, 0) * 100.0
                            + CASE
                                WHEN EXTRACT(EPOCH FROM (NOW() - COALESCE(r.created_at, NOW()))) / 86400.0 < 3
                                  THEN (3 - EXTRACT(EPOCH FROM (NOW() - COALESCE(r.created_at, NOW()))) / 86400.0) * 50.0
                                ELSE 0
                              END
                            - LEAST(EXTRACT(EPOCH FROM (NOW() - COALESCE(r.created_at, NOW()))) / 86400.0, 365.0)
                            - CASE
                                WHEN r.status IN ('blocked','violation','cancel_failed') THEN 1000.0
                                ELSE 0
                              END
                        ) AS retention_score
                    FROM active_records r
                    LEFT JOIN stats s ON s.record_id = r.id
                )
                SELECT *
                FROM scored
                WHERE total_active > %s
                ORDER BY retention_score ASC, served_count ASC, created_at ASC NULLS FIRST, id ASC
                LIMIT GREATEST((SELECT MAX(total_active) FROM scored) - %s, 0)
                """,
                (statuses, target_active, target_active),
            )
            rows = [_row_to_dict(r) for r in cur.fetchall()]
            total = int(rows[0].get('total_active') or 0) if rows else count_active_share_records(statuses)
            return total, rows


def get_existing_share_code_set(items_or_codes) -> set:
    """返回传入 share_code 中已经存在于本地 shared_share_records 的集合。"""
    if not items_or_codes:
        return set()
    codes = set()
    for item in items_or_codes:
        if isinstance(item, dict):
            code = item.get('share_code')
        else:
            code = item
        code = str(code or '').strip()
        if code:
            codes.add(code)
    if not codes:
        return set()
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT share_code FROM shared_share_records WHERE share_code = ANY(%s)", (sorted(codes),))
            return {str((_row_to_dict(r) or {}).get('share_code') or '').strip() for r in cur.fetchall()}


def has_existing_share_for_gap(gap: Dict[str, Any], candidate: Dict[str, Any] = None,
                               files: List[Dict[str, Any]] = None, statuses=None) -> bool:
    """判断中心缺口是否已经有本机分享在处理，供手动和自动共用去重口径。"""
    gap = gap or {}
    candidate = candidate or {}
    files = files or []
    item_type = str(gap.get('item_type') or candidate.get('share_item_type') or candidate.get('item_type') or '').strip()
    season = _safe_int(gap.get('season_number', candidate.get('season_number')), -1)
    episode = _safe_int(gap.get('episode_number', candidate.get('episode_number')), -1)
    root_fid = str(candidate.get('root_fid') or '').strip()
    tmdb_ids = _dedupe_values(
        gap.get('tmdb_id'),
        candidate.get('share_tmdb_id'),
        candidate.get('tmdb_id'),
        candidate.get('parent_series_tmdb_id'),
    )
    sha1s = _dedupe_values([str(x.get('sha1') or '').strip().upper() for x in files or [] if x.get('sha1')])
    statuses = _status_list(statuses)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if root_fid:
                cur.execute(
                    """
                    SELECT 1 FROM shared_share_records
                    WHERE root_fid=%s
                      AND status = ANY(%s)
                    LIMIT 1
                    """,
                    (root_fid, statuses),
                )
                if cur.fetchone() is not None:
                    return True

            if sha1s:
                cur.execute(
                    """
                    SELECT 1
                    FROM shared_share_records r
                    JOIN shared_share_items i ON i.share_record_id = r.id
                    WHERE UPPER(COALESCE(i.sha1, '')) = ANY(%s)
                      AND r.status = ANY(%s)
                    LIMIT 1
                    """,
                    (sha1s, statuses),
                )
                if cur.fetchone() is not None:
                    return True

            if not tmdb_ids:
                return False

            if item_type == 'Movie':
                cur.execute(
                    """
                    SELECT 1
                    FROM shared_share_records r
                    LEFT JOIN shared_share_items i ON i.share_record_id = r.id
                    WHERE (r.tmdb_id = ANY(%s) OR i.tmdb_id = ANY(%s))
                      AND (r.item_type IN ('Movie','movie','movie_file','movie_folder') OR i.item_type IN ('Movie','movie','movie_file'))
                      AND r.status = ANY(%s)
                    LIMIT 1
                    """,
                    (tmdb_ids, tmdb_ids, statuses),
                )
                return cur.fetchone() is not None

            if item_type == 'Episode':
                cur.execute(
                    """
                    SELECT 1
                    FROM shared_share_records r
                    LEFT JOIN shared_share_items i ON i.share_record_id = r.id
                    WHERE (r.tmdb_id = ANY(%s) OR r.parent_series_tmdb_id = ANY(%s) OR i.tmdb_id = ANY(%s))
                      AND COALESCE(i.season_number, r.season_number, -1)=COALESCE(%s, -1)
                      AND COALESCE(i.episode_number, -1)=COALESCE(%s, -1)
                      AND r.status = ANY(%s)
                    LIMIT 1
                    """,
                    (tmdb_ids, tmdb_ids, tmdb_ids, season, episode, statuses),
                )
                return cur.fetchone() is not None

            # Season / Series 都按“剧集包”处理，精确到季。
            cur.execute(
                """
                SELECT 1
                FROM shared_share_records r
                LEFT JOIN shared_share_items i ON i.share_record_id = r.id
                WHERE (r.tmdb_id = ANY(%s) OR r.parent_series_tmdb_id = ANY(%s) OR i.tmdb_id = ANY(%s))
                  AND COALESCE(i.season_number, r.season_number, -1)=COALESCE(%s, -1)
                  AND r.status = ANY(%s)
                LIMIT 1
                """,
                (tmdb_ids, tmdb_ids, tmdb_ids, season, statuses),
            )
            return cur.fetchone() is not None
