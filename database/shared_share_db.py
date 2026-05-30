# database/shared_share_db.py
# 我的共享资源：本地分享记录与分享包明细
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Tuple

from database.connection import get_db_connection


def _json_default(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return str(obj)


def _as_jsonb(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, default=_json_default)


def _nullable_int(value):
    """把前端/中心链路里常见的空字符串统一转成 NULL，避免写入 integer 列时报错。"""
    if value in (None, ''):
        return None
    try:
        return int(float(value))
    except Exception:
        return None


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
                data.get('parent_series_tmdb_id'), _nullable_int(data.get('season_number')), 
                _nullable_int(data.get('episode_number')),  # 👈 【新增】参数映射
                data.get('title'), _nullable_int(data.get('release_year')),
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
                    item.get('tmdb_id'), item.get('item_type'), _nullable_int(item.get('season_number')), _nullable_int(item.get('episode_number')),
                    _as_jsonb(item.get('raw_json') or item),
                ))
                count += 1
            cur.execute("UPDATE shared_share_records SET item_count=%s, updated_at=NOW() WHERE id=%s", (count, record_id))
            conn.commit()
            return count


def _share_records_order_sql(order_by: str = 'created_desc') -> str:
    """我的分享列表排序口径。默认按创建时间倒序，避免 updated_at 被状态同步反复改写后看起来乱跳。"""
    order_by = str(order_by or 'created_desc').strip().lower()
    if order_by == 'created_asc':
        return 'r.created_at ASC NULLS LAST, r.id ASC'
    if order_by == 'updated_desc':
        return 'r.updated_at DESC NULLS LAST, r.id DESC'
    if order_by == 'updated_asc':
        return 'r.updated_at ASC NULLS LAST, r.id ASC'
    # created_desc / 默认：只按创建时间展示，状态同步、RAW补传、中心补登不会打乱列表顺序。
    return 'r.created_at DESC NULLS LAST, r.id DESC'


def list_share_records(status='all', keyword='', page=1, page_size=30, order_by='created_desc') -> Tuple[List[Dict[str, Any]], int]:
    page = max(1, int(page or 1))
    page_size = min(100, max(1, int(page_size or 30)))
    order_sql = _share_records_order_sql(order_by)
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
                ORDER BY {order_sql}
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

def _center_norm_sha1(value: str) -> str:
    text = str(value or '').strip().upper()
    return text if re.fullmatch(r'[A-F0-9]{40}', text) else ''

def load_local_library_sha1_index(sha1s: List[str]) -> Dict[str, Dict[str, Any]]:
    """按 SHA1 查询本地是否已有该文件。media_metadata 严格代表媒体库，p115 缓存作为兜底。"""
    sha1s = list(dict.fromkeys([_center_norm_sha1(x) for x in (sha1s or []) if _center_norm_sha1(x)]))
    if not sha1s:
        return {}

    index = {sha1: {'media_metadata': [], 'p115_filesystem_cache': []} for sha1 in sha1s}
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT
                        matched.sha1 AS sha1,
                        m.tmdb_id,
                        m.item_type,
                        m.parent_series_tmdb_id,
                        m.season_number,
                        m.episode_number,
                        m.title,
                        m.in_library
                    FROM media_metadata m
                    JOIN LATERAL (
                        SELECT UPPER(v) AS sha1
                        FROM jsonb_array_elements_text(
                            CASE
                                WHEN jsonb_typeof(m.file_sha1_json) = 'array' THEN m.file_sha1_json
                                WHEN jsonb_typeof(m.file_sha1_json) = 'string' THEN jsonb_build_array(m.file_sha1_json)
                                ELSE '[]'::jsonb
                            END
                        ) AS arr(v)
                        UNION
                        SELECT UPPER(e.key) AS sha1
                        FROM jsonb_each_text(
                            CASE WHEN jsonb_typeof(m.file_sha1_json) = 'object' THEN m.file_sha1_json ELSE '{}'::jsonb END
                        ) AS e(key, value)
                        UNION
                        SELECT UPPER(e.value) AS sha1
                        FROM jsonb_each_text(
                            CASE WHEN jsonb_typeof(m.file_sha1_json) = 'object' THEN m.file_sha1_json ELSE '{}'::jsonb END
                        ) AS e(key, value)
                    ) matched ON matched.sha1 = ANY(%s)
                    WHERE COALESCE(m.in_library, FALSE) = TRUE
                    """,
                    (sha1s,),
                )
                for row in cur.fetchall():
                    d = dict(row)
                    sha1 = _center_norm_sha1(d.get('sha1'))
                    if sha1 in index:
                        index[sha1]['media_metadata'].append(d)

                cur.execute(
                    """
                    SELECT UPPER(sha1) AS sha1, id, parent_id, name, local_path, pick_code, size
                    FROM p115_filesystem_cache
                    WHERE sha1 IS NOT NULL AND sha1 <> '' AND UPPER(sha1) = ANY(%s)
                    ORDER BY updated_at DESC NULLS LAST
                    """,
                    (sha1s,),
                )
                for row in cur.fetchall():
                    d = dict(row)
                    sha1 = _center_norm_sha1(d.get('sha1'))
                    if sha1 in index:
                        index[sha1]['p115_filesystem_cache'].append(d)
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"  ➜ [共享资源DB] 查询本地入库状态失败: {e}")
    return index

# ======================================================================
# 媒体库与 115 缓存查询下沉 (Media & P115 Cache Queries)
# ======================================================================

def _jsonb_non_empty_sql_expr(column: str) -> str:
    return f"""
    (
        CASE jsonb_typeof({column})
            WHEN 'array' THEN jsonb_array_length({column}) > 0
            WHEN 'object' THEN {column} <> '{{}}'::jsonb
            WHEN 'string' THEN btrim({column}::text, '"') <> ''
            ELSE FALSE
        END
    )
    """

def check_series_has_physical_episode(parent_tmdb_id: str) -> bool:
    if not parent_tmdb_id:
        return False
    has_sha1 = _jsonb_non_empty_sql_expr('file_sha1_json')
    has_pc = _jsonb_non_empty_sql_expr('file_pickcode_json')
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT 1 FROM media_metadata
                WHERE item_type='Episode' AND parent_series_tmdb_id = %s
                  AND COALESCE(in_library, FALSE) = TRUE
                  AND ({has_sha1} OR {has_pc})
                LIMIT 1
                """, (parent_tmdb_id,)
            )
            return cur.fetchone() is not None

def get_p115_files_from_cache_tree(root_fid: str, max_depth: int) -> List[Dict[str, Any]]:
    if not root_fid:
        return []
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH RECURSIVE tree AS (
                    SELECT id, parent_id, name, local_path, sha1, pick_code, size,
                           0 AS depth, CAST('' AS text) AS rel_path
                    FROM p115_filesystem_cache WHERE id = %s
                    UNION ALL
                    SELECT c.id, c.parent_id, c.name, c.local_path, c.sha1, c.pick_code, c.size,
                           t.depth + 1 AS depth,
                           CASE WHEN t.rel_path = '' THEN c.name ELSE t.rel_path || '/' || c.name END AS rel_path
                    FROM p115_filesystem_cache c
                    JOIN tree t ON c.parent_id = t.id
                    WHERE t.depth < %s
                )
                SELECT * FROM tree ORDER BY depth, rel_path, name
                """, (str(root_fid), int(max_depth))
            )
            return [_row_to_dict(r) for r in cur.fetchall()]

def get_media_metadata_row(tmdb_id: str, item_type: str) -> Dict[str, Any]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tmdb_id, item_type, title, original_title, parent_series_tmdb_id,
                       season_number, episode_number, release_year, release_date, last_air_date,
                       file_sha1_json, file_pickcode_json, in_library, subscription_status,
                       total_episodes, watching_status, watchlist_tmdb_status
                FROM media_metadata WHERE tmdb_id=%s AND item_type=%s LIMIT 1
            """, (str(tmdb_id), str(item_type)))
            return _row_to_dict(cur.fetchone())

def get_series_identity(series_tmdb_id: str) -> Dict[str, Any]:
    if not series_tmdb_id:
        return {}
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tmdb_id, item_type, title, original_title, release_year, release_date, last_air_date
                FROM media_metadata WHERE tmdb_id=%s AND item_type='Series' LIMIT 1
            """, (str(series_tmdb_id),))
            return _row_to_dict(cur.fetchone())

def get_media_metadata_row_loose(tmdb_id: str, item_type: str = '') -> Dict[str, Any]:
    if not tmdb_id:
        return None
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if item_type:
                cur.execute("""
                    SELECT tmdb_id, item_type, title, original_title, parent_series_tmdb_id,
                           season_number, episode_number, release_year, release_date, last_air_date, watching_status
                    FROM media_metadata WHERE tmdb_id=%s AND item_type=%s LIMIT 1
                """, (tmdb_id, item_type))
                row = cur.fetchone()
                if row: return _row_to_dict(row)
            cur.execute("""
                SELECT tmdb_id, item_type, title, original_title, parent_series_tmdb_id,
                       season_number, episode_number, release_year, release_date, last_air_date, watching_status
                FROM media_metadata WHERE tmdb_id=%s
                ORDER BY CASE item_type WHEN 'Series' THEN 0 WHEN 'Movie' THEN 1 WHEN 'Season' THEN 2 WHEN 'Episode' THEN 3 ELSE 9 END LIMIT 1
            """, (tmdb_id,))
            return _row_to_dict(cur.fetchone())

def get_episode_rows_by_season(series_id: str, season_number: int) -> List[Dict[str, Any]]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tmdb_id, item_type, title, original_title, parent_series_tmdb_id,
                       season_number, episode_number, release_year, release_date, last_air_date,
                       file_sha1_json, file_pickcode_json, in_library, subscription_status,
                       total_episodes, watching_status, watchlist_tmdb_status,
                       CASE WHEN LOWER(COALESCE(to_jsonb(media_metadata)->>'force_ended', '')) IN ('1','true','yes','on','t','y') THEN TRUE ELSE FALSE END AS force_ended
                FROM media_metadata
                WHERE item_type='Episode' AND parent_series_tmdb_id=%s AND season_number=%s
                ORDER BY episode_number NULLS LAST, tmdb_id
            """, (str(series_id), int(season_number)))
            return [_row_to_dict(r) for r in cur.fetchall()]

def get_episode_rows_by_series_filter(series_id: str, season_filter: List[int], positive_only: bool) -> List[Dict[str, Any]]:
    extra_where = ''
    args = [str(series_id)]
    if positive_only:
        extra_where += ' AND COALESCE(season_number, 0) > 0'
    if season_filter:
        extra_where += ' AND season_number = ANY(%s)'
        args.append(season_filter)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT tmdb_id, item_type, title, original_title, parent_series_tmdb_id,
                       season_number, episode_number, release_year, release_date, last_air_date,
                       file_sha1_json, file_pickcode_json, in_library, subscription_status
                FROM media_metadata
                WHERE item_type='Episode' AND parent_series_tmdb_id=%s {extra_where}
                ORDER BY season_number NULLS LAST, episode_number NULLS LAST, tmdb_id
            """, args)
            return [_row_to_dict(r) for r in cur.fetchall()]

def get_p115_file_rows_by_pc_sha1(pickcodes: List[str], sha1s: List[str]) -> List[Dict[str, Any]]:
    if not pickcodes and not sha1s:
        return []
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, parent_id, name, local_path, sha1, pick_code, size
                FROM p115_filesystem_cache
                WHERE (%s::text[] <> '{}'::text[] AND pick_code = ANY(%s))
                   OR (%s::text[] <> '{}'::text[] AND UPPER(sha1) = ANY(%s))
                ORDER BY parent_id, name
            """, (pickcodes, pickcodes, sha1s, sha1s))
            return [_row_to_dict(r) for r in cur.fetchall()]

def get_p115_node_by_id(node_id: str) -> Dict[str, Any]:
    if not node_id: return None
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, parent_id, name, local_path, sha1, pick_code, size FROM p115_filesystem_cache WHERE id=%s LIMIT 1", (str(node_id),))
            return _row_to_dict(cur.fetchone())

def get_asset_details_for_candidate(share_item_type: str, tmdb_id: str, parent_series_id: str, season_number: Any, episode_number: Any, share_type: str) -> List[Dict[str, Any]]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if share_item_type == 'Movie':
                cur.execute("SELECT asset_details_json FROM media_metadata WHERE item_type='Movie' AND tmdb_id=%s LIMIT 1", (tmdb_id,))
            elif share_item_type == 'Episode' and parent_series_id and season_number not in (None, '') and episode_number not in (None, ''):
                cur.execute("SELECT asset_details_json FROM media_metadata WHERE item_type='Episode' AND parent_series_tmdb_id=%s AND season_number=%s AND episode_number=%s LIMIT 5", (str(parent_series_id), int(season_number), int(episode_number)))
            elif share_item_type == 'Series' or share_type in ('series_pack', 'tv_pack'):
                series_id = parent_series_id or tmdb_id
                if not series_id: return []
                cur.execute("SELECT asset_details_json FROM media_metadata WHERE item_type='Episode' AND parent_series_tmdb_id=%s AND in_library=TRUE ORDER BY season_number NULLS LAST, episode_number NULLS LAST, tmdb_id LIMIT 2000", (str(series_id),))
            elif (share_item_type == 'Season' or share_type in ('season_pack',)) and (parent_series_id or tmdb_id) and season_number not in (None, ''):
                cur.execute("SELECT asset_details_json FROM media_metadata WHERE item_type='Episode' AND parent_series_tmdb_id=%s AND season_number=%s AND in_library=TRUE ORDER BY episode_number NULLS LAST, tmdb_id LIMIT 300", (str(parent_series_id or tmdb_id), int(season_number)))
            else:
                return []
            return [_row_to_dict(r) for r in cur.fetchall()]

def get_series_row_for_share_request(series_tmdb_id: str) -> Dict[str, Any]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tmdb_id, item_type, title, original_title, parent_series_tmdb_id,
                       season_number, episode_number, release_year, release_date, last_air_date,
                       file_sha1_json, file_pickcode_json, in_library, subscription_status,
                       total_episodes, watching_status, watchlist_tmdb_status, asset_details_json
                FROM media_metadata WHERE item_type='Series' AND tmdb_id=%s ORDER BY in_library DESC, tmdb_id LIMIT 1
            """, (series_tmdb_id,))
            return _row_to_dict(cur.fetchone())

def get_exact_episode_row_for_share_request(parent_tmdb_id: str, season_number: int, episode_number: int) -> Dict[str, Any]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tmdb_id, item_type, title, original_title, parent_series_tmdb_id,
                       season_number, episode_number, release_year, release_date, last_air_date,
                       file_sha1_json, file_pickcode_json, in_library, subscription_status,
                       total_episodes, watching_status, watchlist_tmdb_status, asset_details_json
                FROM media_metadata
                WHERE item_type='Episode' AND parent_series_tmdb_id=%s AND season_number=%s AND episode_number=%s AND in_library=TRUE
                ORDER BY tmdb_id LIMIT 1
            """, (parent_tmdb_id, season_number, episode_number))
            return _row_to_dict(cur.fetchone())

def get_real_completed_season_info(series_id: str) -> List[Dict[str, Any]]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tmdb_id, title, season_number, total_episodes, in_library, watching_status, 
                       CASE WHEN LOWER(COALESCE(to_jsonb(media_metadata)->>'force_ended', '')) IN ('1','true','yes','on','t','y') THEN TRUE ELSE FALSE END AS force_ended
                FROM media_metadata
                WHERE item_type='Season' AND parent_series_tmdb_id=%s AND COALESCE(season_number, 0) > 0
                ORDER BY season_number NULLS LAST, tmdb_id
            """, (series_id,))
            return [_row_to_dict(r) for r in cur.fetchall()]

def get_season_completion_status(parent_series_id: str, season_number: int) -> Dict[str, Any]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tmdb_id, item_type, title, original_title, parent_series_tmdb_id,
                       season_number, episode_number, release_year, release_date, last_air_date,
                       file_sha1_json, file_pickcode_json, in_library, subscription_status,
                       total_episodes, watching_status, watchlist_tmdb_status,
                       CASE WHEN LOWER(COALESCE(to_jsonb(media_metadata)->>'force_ended', '')) IN ('1','true','yes','on','t','y') THEN TRUE ELSE FALSE END AS force_ended
                FROM media_metadata
                WHERE item_type='Season' AND parent_series_tmdb_id=%s AND season_number=%s
                ORDER BY tmdb_id LIMIT 1
            """, (str(parent_series_id), season_number))
            return _row_to_dict(cur.fetchone())

def search_shareable_media(keyword: str, search_limit: int, result_limit: int) -> List[Dict[str, Any]]:
    kw = f'%{keyword}%'
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                WITH matched AS (
                    SELECT tmdb_id, item_type, title, original_title, parent_series_tmdb_id,
                           season_number, episode_number, release_year, release_date, last_air_date,
                           file_sha1_json, file_pickcode_json, in_library, subscription_status,
                           total_episodes, watching_status, watchlist_tmdb_status,
                           CASE WHEN LOWER(COALESCE(to_jsonb(media_metadata)->>'force_ended', '')) IN ('1','true','yes','on','t','y') THEN TRUE ELSE FALSE END AS force_ended
                    FROM media_metadata
                    WHERE item_type IN ('Movie','Series','Season','Episode') AND in_library = TRUE
                      AND (title ILIKE %s OR original_title ILIKE %s OR tmdb_id ILIKE %s)
                    ORDER BY CASE item_type WHEN 'Movie' THEN 0 WHEN 'Series' THEN 1 WHEN 'Season' THEN 2 ELSE 3 END,
                             in_library DESC, COALESCE(release_year, 0) DESC, title NULLS LAST
                    LIMIT %s
                ), related_series AS (
                    SELECT DISTINCT CASE WHEN item_type='Series' THEN tmdb_id WHEN item_type IN ('Season','Episode') THEN parent_series_tmdb_id ELSE NULL END AS series_id
                    FROM matched
                ), expanded AS (
                    SELECT * FROM matched
                    UNION ALL
                    SELECT s.tmdb_id, s.item_type, s.title, s.original_title, s.parent_series_tmdb_id,
                           s.season_number, s.episode_number, s.release_year, s.release_date, s.last_air_date,
                           s.file_sha1_json, s.file_pickcode_json, s.in_library, s.subscription_status,
                           s.total_episodes, s.watching_status, s.watchlist_tmdb_status,
                           CASE WHEN LOWER(COALESCE(to_jsonb(s)->>'force_ended', '')) IN ('1','true','yes','on','t','y') THEN TRUE ELSE FALSE END AS force_ended
                    FROM media_metadata s
                    JOIN related_series rs ON rs.series_id IS NOT NULL AND s.item_type='Season' AND s.parent_series_tmdb_id=rs.series_id
                    WHERE s.in_library = TRUE
                )
                SELECT * FROM expanded
                ORDER BY CASE item_type WHEN 'Movie' THEN 0 WHEN 'Season' THEN 1 WHEN 'Series' THEN 2 ELSE 3 END,
                         season_number NULLS LAST, episode_number NULLS LAST, in_library DESC, COALESCE(release_year, 0) DESC, title NULLS LAST
                LIMIT %s
            """, (kw, kw, kw, search_limit, result_limit))
            return [_row_to_dict(r) for r in cur.fetchall()]

def find_existing_p115_file_in_target(target_cid: str, expected_sha1: str, expected_pc: str, target_name: str) -> Dict[str, Any]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            clauses = ['parent_id = %s']
            args = [target_cid]
            sub = []
            if expected_sha1:
                sub.append('UPPER(sha1) = %s')
                args.append(expected_sha1)
            if expected_pc:
                sub.append('pick_code = %s')
                args.append(expected_pc)
            if target_name:
                sub.append('name = %s')
                args.append(target_name)
            if not sub: return None
            cur.execute(f"""
                SELECT id, parent_id, name, sha1, pick_code, size, local_path
                FROM p115_filesystem_cache
                WHERE {' AND '.join(clauses)} AND ({' OR '.join(sub)})
                ORDER BY CASE WHEN UPPER(COALESCE(sha1,'')) = %s THEN 0 ELSE 1 END,
                         CASE WHEN COALESCE(pick_code,'') = %s THEN 0 ELSE 1 END,
                         CASE WHEN name = %s THEN 0 ELSE 1 END
                LIMIT 1
            """, args + [expected_sha1, expected_pc, target_name])
            return _row_to_dict(cur.fetchone())

def get_local_wanted_gaps(limit: int) -> List[Dict[str, Any]]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tmdb_id, item_type, parent_series_tmdb_id, season_number, episode_number, title, release_year
                FROM media_metadata
                WHERE COALESCE(in_library, FALSE) = FALSE AND item_type IN ('Movie','Series','Season','Episode')
                  AND subscription_status IN ('WANTED','REQUESTED','SUBSCRIBED','PENDING_RELEASE')
                ORDER BY last_updated_at DESC NULLS LAST, created_at DESC NULLS LAST LIMIT %s
            """, (int(limit),))
            return [_row_to_dict(r) for r in cur.fetchall()]

def get_active_local_share_code_set(active_statuses: List[str]) -> set:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT share_code FROM shared_share_records WHERE COALESCE(share_code, '') <> '' AND status = ANY(%s)", (active_statuses,))
            return {str((r or {}).get('share_code') or '').strip() for r in cur.fetchall() if str((r or {}).get('share_code') or '').strip()}

def get_watching_missing_episodes(limit: int) -> List[Dict[str, Any]]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                WITH watch_seasons AS (
                    SELECT DISTINCT ON (parent_series_tmdb_id, season_number)
                        tmdb_id AS season_tmdb_id, parent_series_tmdb_id, season_number, title AS season_title,
                        release_year, watching_status, last_updated_at
                    FROM media_metadata
                    WHERE item_type='Season' AND watching_status IN ('Watching','Paused') AND parent_series_tmdb_id IS NOT NULL AND season_number IS NOT NULL
                    ORDER BY parent_series_tmdb_id, season_number, CASE watching_status WHEN 'Watching' THEN 0 WHEN 'Paused' THEN 1 ELSE 2 END, last_updated_at DESC NULLS LAST
                ), all_episodes AS (
                    SELECT DISTINCT ON (e.parent_series_tmdb_id, e.season_number, e.episode_number)
                        e.tmdb_id, e.item_type, e.parent_series_tmdb_id, e.season_number, e.episode_number, e.title, e.release_year, e.release_date,
                        COALESCE(e.in_library, FALSE) AS in_library, ws.season_tmdb_id, ws.season_title, ws.watching_status, ws.last_updated_at AS season_last_updated_at
                    FROM media_metadata e
                    JOIN watch_seasons ws ON e.item_type='Episode' AND e.parent_series_tmdb_id = ws.parent_series_tmdb_id AND e.season_number = ws.season_number
                    WHERE e.episode_number IS NOT NULL AND COALESCE(e.subscription_status, 'NONE') NOT IN ('IGNORED') AND (e.release_date IS NULL OR e.release_date <= CURRENT_DATE)
                    ORDER BY e.parent_series_tmdb_id, e.season_number, e.episode_number, COALESCE(e.in_library, FALSE) DESC, e.last_updated_at DESC NULLS LAST, e.release_date DESC NULLS LAST
                )
                SELECT * FROM all_episodes WHERE in_library = FALSE
                ORDER BY season_last_updated_at DESC NULLS LAST, parent_series_tmdb_id, season_number, episode_number LIMIT %s
            """, (int(limit),))
            return [_row_to_dict(r) for r in cur.fetchall()]

def check_local_virtual_projection_exists(parent: str, season: int, episode: int) -> bool:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM shared_virtual_items
                WHERE status <> 'deleted' AND (parent_series_tmdb_id=%s OR tmdb_id=%s)
                  AND COALESCE(season_number, -1)=COALESCE(%s, -1) AND COALESCE(episode_number, -1)=COALESCE(%s, -1)
                LIMIT 1
            """, (parent, parent, season, episode))
            return cur.fetchone() is not None

def find_local_media_for_gap(tmdb_id: str, item_type: str, season: Any, episode: Any) -> Dict[str, Any]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if item_type == 'Movie':
                cur.execute("SELECT * FROM media_metadata WHERE item_type='Movie' AND tmdb_id=%s AND in_library=TRUE LIMIT 1", (tmdb_id,))
            elif item_type in ('Season', 'Series'):
                if season not in (None, ''):
                    cur.execute("SELECT * FROM media_metadata WHERE item_type='Season' AND parent_series_tmdb_id=%s AND season_number=%s AND in_library=TRUE LIMIT 1", (tmdb_id, int(season)))
                else:
                    cur.execute("SELECT * FROM media_metadata WHERE item_type='Series' AND tmdb_id=%s AND in_library=TRUE LIMIT 1", (tmdb_id,))
            elif item_type == 'Episode':
                cur.execute("""
                    SELECT * FROM media_metadata
                    WHERE item_type='Episode' AND (parent_series_tmdb_id=%s OR tmdb_id=%s)
                      AND COALESCE(season_number, -1)=COALESCE(%s, -1) AND COALESCE(episode_number, -1)=COALESCE(%s, -1) AND in_library=TRUE
                    LIMIT 1
                """, (tmdb_id, tmdb_id, int(season or -1), int(episode or -1)))
            else:
                return None
            return _row_to_dict(cur.fetchone())

def get_completed_season_episode_share_groups(statuses: List[str], max_rows: int) -> List[Dict[str, Any]]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT r.*, s.tmdb_id AS _season_tmdb_id, s.title AS _season_title, s.original_title AS _season_original_title,
                       s.parent_series_tmdb_id AS _season_parent_series_tmdb_id, s.season_number AS _season_number,
                       s.release_year AS _season_release_year, s.release_date AS _season_release_date, s.last_air_date AS _season_last_air_date,
                       s.file_sha1_json AS _season_file_sha1_json, s.file_pickcode_json AS _season_file_pickcode_json,
                       s.in_library AS _season_in_library, s.subscription_status AS _season_subscription_status,
                       s.total_episodes AS _season_total_episodes, s.watching_status AS _season_watching_status,
                       s.watchlist_tmdb_status AS _season_watchlist_tmdb_status, s.last_updated_at AS _season_last_updated_at
                FROM shared_share_records r
                JOIN media_metadata s ON s.item_type = 'Season' AND s.parent_series_tmdb_id = COALESCE(NULLIF(r.parent_series_tmdb_id, ''), NULLIF(r.tmdb_id, '')) AND s.season_number = r.season_number
                WHERE r.status = ANY(%s) AND COALESCE(s.watching_status, '') = 'Completed' AND COALESCE(r.share_code, '') <> '' AND r.season_number IS NOT NULL
                  AND (LOWER(COALESCE(r.share_type, '')) = 'episode_file' OR COALESCE(r.item_type, '') = 'Episode' OR r.episode_number IS NOT NULL)
                  AND LOWER(COALESCE(r.share_type, '')) NOT IN ('season_pack', 'series_pack', 'tv_pack', 'season')
                ORDER BY s.last_updated_at DESC NULLS LAST, r.parent_series_tmdb_id, r.season_number, r.episode_number NULLS LAST, r.created_at ASC
                LIMIT %s
            """, (statuses, max_rows))
            return [_row_to_dict(r) for r in cur.fetchall()]

def check_active_season_pack_share(parent_series_tmdb_id: str, season_number: int, statuses: List[str]) -> bool:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM shared_share_records
                WHERE status = ANY(%s) AND COALESCE(season_number, -1) = %s AND (COALESCE(parent_series_tmdb_id, '') = %s OR COALESCE(tmdb_id, '') = %s)
                  AND (LOWER(COALESCE(share_type, '')) IN ('season_pack', 'series_pack', 'tv_pack', 'season') OR (COALESCE(item_type, '') = 'Season' AND episode_number IS NULL))
                LIMIT 1
            """, (statuses, season_number, parent_series_tmdb_id, parent_series_tmdb_id))
            return cur.fetchone() is not None

def get_expired_virtual_cache_rows(limit: int) -> List[Dict[str, Any]]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT virtual_id, title, file_name, share_code, raw_json, real_fid, real_pick_code, real_parent_id, expires_at, strm_path, mediainfo_path, nfo_path, emby_item_id
                FROM shared_virtual_items
                WHERE status IN ('cached','watched') AND COALESCE(real_fid, '') <> '' AND expires_at IS NOT NULL AND expires_at < NOW()
                ORDER BY expires_at ASC LIMIT %s
            """, (int(limit),))
            return [_row_to_dict(r) for r in cur.fetchall()]

def get_virtual_items_for_share_health(limit: int = 300) -> List[Dict[str, Any]]:
    """查询仍保留虚拟投影、需要校验中心分享有效性的虚拟入库记录。"""
    limit = max(1, min(int(limit or 300), 1000))
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM shared_virtual_items
                WHERE COALESCE(status, '') NOT IN ('deleted','promoted','promote_pending')
                  AND (
                        COALESCE(source_id, '') <> ''
                     OR COALESCE(share_code, '') <> ''
                  )
                ORDER BY
                    CASE
                        WHEN COALESCE(real_pick_code, '') = '' THEN 0
                        ELSE 1
                    END,
                    updated_at ASC NULLS FIRST,
                    created_at ASC NULLS FIRST
                LIMIT %s
            """, (limit,))
            return [_row_to_dict(r) for r in cur.fetchall()]


def update_virtual_item_center_source(virtual_id: str, source: Dict[str, Any], message: str = '') -> Dict[str, Any]:
    """把虚拟入库记录切换到新的中心共享源。媒体身份保持原虚拟项不变，只替换分享来源。"""
    if not virtual_id or not source:
        return None
    raw_patch = {
        'virtual_source_replaced_at': datetime.utcnow().isoformat(),
        'virtual_source_replace_message': message or '',
        'replacement_center_source': source,
    }
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE shared_virtual_items
                SET source_id = COALESCE(NULLIF(%s, ''), source_id),
                    source_key = COALESCE(NULLIF(%s, ''), source_key),
                    source_provider = COALESCE(NULLIF(%s, ''), source_provider),
                    share_code = COALESCE(NULLIF(%s, ''), share_code),
                    receive_code = COALESCE(%s, receive_code),
                    contributor_id = COALESCE(NULLIF(%s, ''), contributor_id),
                    sha1 = COALESCE(NULLIF(%s, ''), sha1),
                    size = CASE WHEN COALESCE(%s, 0) > 0 THEN %s ELSE size END,
                    file_name = COALESCE(NULLIF(%s, ''), file_name),
                    quality = COALESCE(NULLIF(%s, ''), quality),
                    status = CASE WHEN status='error' THEN 'virtual_ready' ELSE status END,
                    last_error = %s,
                    raw_json = COALESCE(raw_json, '{}'::jsonb) || %s::jsonb,
                    updated_at = NOW()
                WHERE virtual_id=%s
                RETURNING *
            """, (
                str(source.get('source_id') or ''),
                str(source.get('source_key') or ''),
                str(source.get('source_provider') or 'shared_center'),
                str(source.get('share_code') or ''),
                str(source.get('receive_code') or ''),
                str(source.get('contributor_id') or source.get('provider_id') or ''),
                _center_norm_sha1(source.get('sha1')),
                _safe_int(source.get('size'), 0), _safe_int(source.get('size'), 0),
                str(source.get('file_name') or ''),
                str(source.get('quality') or ''),
                message or '',
                _as_jsonb(raw_patch),
                str(virtual_id),
            ))
            row = _row_to_dict(cur.fetchone())
            conn.commit()
            return row

def find_local_cache_rows_by_sha1s(sha1s: List[str]) -> List[Dict[str, Any]]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, parent_id, name, local_path, sha1, pick_code, size
                FROM p115_filesystem_cache
                WHERE UPPER(sha1) = ANY(%s)
                ORDER BY COALESCE(size, 0) DESC, updated_at DESC NULLS LAST, name ASC
            """, (sha1s,))
            return [_row_to_dict(r) for r in cur.fetchall()]

def get_seed_media_row_for_share_request(target: str, media: str, tmdb_id: str, season: Any) -> Dict[str, Any]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if media == 'movie' or target == 'movie':
                cur.execute("SELECT * FROM media_metadata WHERE item_type='Movie' AND tmdb_id=%s AND COALESCE(in_library, FALSE)=TRUE ORDER BY last_updated_at DESC NULLS LAST, created_at DESC NULLS LAST LIMIT 1", (tmdb_id,))
            elif target == 'season' and season not in (None, ''):
                cur.execute("SELECT * FROM media_metadata WHERE item_type='Season' AND parent_series_tmdb_id=%s AND season_number=%s AND COALESCE(in_library, FALSE)=TRUE ORDER BY last_updated_at DESC NULLS LAST, created_at DESC NULLS LAST LIMIT 1", (tmdb_id, int(season)))
            else:
                return {}
            return _row_to_dict(cur.fetchone())

def update_virtual_target_parent(virtual_id: str, target_cid: str, target_name: str):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE shared_virtual_items SET target_parent_id=%s, target_parent_name=%s, updated_at=NOW() WHERE virtual_id=%s", (str(target_cid), target_name or '', virtual_id))
            conn.commit()

def update_p115_cache_parent(fid: str, target_cid: str, new_name: str = None):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if new_name:
                cur.execute("UPDATE p115_filesystem_cache SET name=%s, parent_id=%s, updated_at=NOW() WHERE id=%s", (new_name, target_cid, fid))
            else:
                cur.execute("UPDATE p115_filesystem_cache SET parent_id=%s, updated_at=NOW() WHERE id=%s", (target_cid, fid))
            conn.commit()

def delete_p115_cache_node(fid: str):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM p115_filesystem_cache WHERE id=%s", (fid,))
            conn.commit()