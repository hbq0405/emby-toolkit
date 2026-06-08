# database/shared_share_db.py
# Rapid v2 本地共享索引：不再创建 115 分享，只登记可秒传资源与 manifest。
import json
import re
import hashlib
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple, Callable

from database.connection import get_db_connection

logger = logging.getLogger(__name__)

VIDEO_EXTS = {'.mkv', '.mp4', '.ts', '.m2ts', '.avi', '.mov', '.wmv', '.flv', '.rmvb', '.webm', '.iso'}


def _json_default(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return str(obj)


def _as_jsonb(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, default=_json_default)


def _as_array(value) -> list:
    if value in (None, ''):
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            value = [value]
    if isinstance(value, dict):
        value = list(value.values())
    if not isinstance(value, (list, tuple, set)):
        value = [value]
    out = []
    for v in value:
        if isinstance(v, dict):
            for key in ('sha1', 'pick_code', 'pickcode', 'pc', 'value'):
                if v.get(key):
                    out.append(str(v.get(key)).strip())
                    break
        else:
            s = str(v or '').strip()
            if s:
                out.append(s)
    return [x for i, x in enumerate(out) if x and x not in out[:i]]


def _row(row):
    return dict(row) if row is not None else None


def _rows(rows):
    return [dict(r) for r in rows or []]


def _safe_int(v, default=0):
    try:
        if v in (None, ''):
            return default
        return int(float(v))
    except Exception:
        return default


def _nullable_int(v):
    try:
        if v in (None, ''):
            return None
        return int(float(v))
    except Exception:
        return None


def _norm_sha1(value: str) -> str:
    text = str(value or '').strip().upper()
    return text if re.fullmatch(r'[A-F0-9]{40}', text) else ''


def _is_video_name(name: str) -> bool:
    import os
    return os.path.splitext(str(name or ''))[1].lower() in VIDEO_EXTS


def _guess_episode_number(name: str):
    text = str(name or '')
    for pat in (r'[Ss]\d{1,3}[. _-]*[Ee](\d{1,4})', r'第\s*(\d{1,4})\s*[集话話]', r'\bE(\d{1,4})\b'):
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None
    return None


def _source_local_key(kind: str, tmdb_id: str, season=None, episode=None, sha1: str = '', provider: str = 'local') -> str:
    raw = f"{kind}|{provider}|{tmdb_id}|{season if season is not None else ''}|{episode if episode is not None else ''}|{sha1 or ''}"
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


def manifest_hash(files: List[Dict[str, Any]]) -> str:
    parts = []
    for f in sorted(files or [], key=lambda x: (_safe_int(x.get('episode_number'), 0), str(x.get('sha1') or ''), str(x.get('file_name') or ''))):
        sha1 = _norm_sha1(f.get('sha1'))
        if not sha1:
            continue
        parts.append(f"{_safe_int(f.get('episode_number'), 0)}:{sha1}:{_safe_int(f.get('size'), 0)}:{f.get('file_name') or ''}")
    return hashlib.sha256('\n'.join(parts).encode('utf-8')).hexdigest()


def upsert_local_source(data: Dict[str, Any]) -> Dict[str, Any]:
    data = dict(data or {})
    kind = str(data.get('source_kind') or data.get('kind') or '').strip()
    tmdb_id = str(data.get('tmdb_id') or '').strip()
    sha1 = _norm_sha1(data.get('sha1'))
    season = _nullable_int(data.get('season_number'))
    episode = _nullable_int(data.get('episode_number'))
    provider = str(data.get('source_provider') or 'local').strip() or 'local'
    source_key = data.get('source_key') or _source_local_key(kind, tmdb_id, season, episode, sha1, provider)
    center_source_id = str(data.get('center_source_id') or '').strip() or None
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO shared_rapid_sources(
                    source_key, source_kind, center_source_id, tmdb_id, item_type, parent_series_tmdb_id,
                    season_number, episode_number, title, release_year, sha1, preid, size, file_name, root_fid, root_name,
                    source_provider, status, center_status, manifest_hash, manifest_version, file_count, total_size,
                    is_clean_version, clean_version_confidence, clean_version_meta_json, media_signature_json,
                    rapid_meta_json, raw_json, reported_at, updated_at
                ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb,%s::jsonb,CASE WHEN %s IS NOT NULL THEN NOW() ELSE NULL END,NOW())
                ON CONFLICT(source_key)
                DO UPDATE SET
                    center_source_id=COALESCE(EXCLUDED.center_source_id, shared_rapid_sources.center_source_id),
                    title=COALESCE(EXCLUDED.title, shared_rapid_sources.title),
                    release_year=COALESCE(EXCLUDED.release_year, shared_rapid_sources.release_year),
                    sha1=COALESCE(EXCLUDED.sha1, shared_rapid_sources.sha1),
                    preid=COALESCE(EXCLUDED.preid, shared_rapid_sources.preid),
                    size=COALESCE(EXCLUDED.size, shared_rapid_sources.size),
                    file_name=COALESCE(EXCLUDED.file_name, shared_rapid_sources.file_name),
                    root_fid=COALESCE(EXCLUDED.root_fid, shared_rapid_sources.root_fid),
                    root_name=COALESCE(EXCLUDED.root_name, shared_rapid_sources.root_name),
                    status=EXCLUDED.status,
                    center_status=EXCLUDED.center_status,
                    manifest_hash=COALESCE(EXCLUDED.manifest_hash, shared_rapid_sources.manifest_hash),
                    manifest_version=GREATEST(shared_rapid_sources.manifest_version, EXCLUDED.manifest_version),
                    file_count=EXCLUDED.file_count,
                    total_size=EXCLUDED.total_size,
                    is_clean_version=EXCLUDED.is_clean_version,
                    clean_version_confidence=EXCLUDED.clean_version_confidence,
                    clean_version_meta_json=EXCLUDED.clean_version_meta_json,
                    media_signature_json=EXCLUDED.media_signature_json,
                    rapid_meta_json=EXCLUDED.rapid_meta_json,
                    raw_json=EXCLUDED.raw_json,
                    reported_at=CASE WHEN EXCLUDED.center_source_id IS NOT NULL THEN NOW() ELSE shared_rapid_sources.reported_at END,
                    updated_at=NOW()
                RETURNING *
                """,
                (
                    source_key, kind, center_source_id, tmdb_id, data.get('item_type'), data.get('parent_series_tmdb_id'),
                    season, episode, data.get('title'), _nullable_int(data.get('release_year')), sha1 or None,
                    _norm_sha1(data.get('preid')) or None, _safe_int(data.get('size'), 0) or None, data.get('file_name'), data.get('root_fid'), data.get('root_name'),
                    provider, data.get('status') or 'active', data.get('center_status') or ('reported' if center_source_id else 'local'),
                    data.get('manifest_hash'), _safe_int(data.get('manifest_version'), 1), _safe_int(data.get('file_count'), 0),
                    _safe_int(data.get('total_size'), 0), bool(data.get('is_clean_version', False)), data.get('clean_version_confidence'),
                    _as_jsonb(data.get('clean_version_meta_json') or {}), _as_jsonb(data.get('media_signature_json') or {}),
                    _as_jsonb(data.get('rapid_meta_json') or {}), _as_jsonb(data.get('raw_json') or data), center_source_id,
                ),
            )
            row = _row(cur.fetchone())
            conn.commit()
            return row


def replace_source_files(local_source_id: int, files: List[Dict[str, Any]]) -> int:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM shared_rapid_source_files WHERE local_source_id=%s", (local_source_id,))
            count = 0
            for item in files or []:
                sha1 = _norm_sha1(item.get('sha1'))
                if not sha1:
                    continue
                cur.execute(
                    """
                    INSERT INTO shared_rapid_source_files(
                        local_source_id, fid, pick_code, sha1, preid, size, file_name, relative_path,
                        tmdb_id, item_type, season_number, episode_number, center_file_id,
                        raw_ffprobe_uploaded, media_signature_json, rapid_meta_json, raw_json
                    ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb)
                    """,
                    (
                        local_source_id, item.get('fid') or item.get('file_id'), item.get('pick_code') or item.get('pickcode') or item.get('pc'),
                        sha1, _norm_sha1(item.get('preid') or (item.get('rapid_meta_json') or {}).get('preid') if isinstance(item.get('rapid_meta_json'), dict) else item.get('preid')) or None,
                        _safe_int(item.get('size'), 0), item.get('file_name') or item.get('name') or '', item.get('relative_path') or '',
                        item.get('tmdb_id'), item.get('item_type'), _nullable_int(item.get('season_number')), _nullable_int(item.get('episode_number')),
                        item.get('center_file_id'), bool(item.get('raw_ffprobe_uploaded', False)), _as_jsonb(item.get('media_signature_json') or {}),
                        _as_jsonb(item.get('rapid_meta_json') or {}), _as_jsonb(item.get('raw_json') or item),
                    ),
                )
                count += 1
            cur.execute("UPDATE shared_rapid_sources SET file_count=%s, updated_at=NOW() WHERE id=%s", (count, local_source_id))
            conn.commit()
            return count


def update_local_source(local_source_id: int, **fields):
    allowed = {
        'status', 'center_status', 'center_source_id', 'last_error', 'preid', 'manifest_hash', 'manifest_version',
        'file_count', 'total_size', 'is_clean_version', 'clean_version_confidence', 'clean_version_meta_json',
        'media_signature_json', 'rapid_meta_json', 'reported_at', 'disabled_at', 'raw_json'
    }
    sets, args = [], []
    for k, v in fields.items():
        if k not in allowed:
            continue
        if k in {'clean_version_meta_json', 'media_signature_json', 'rapid_meta_json', 'raw_json'}:
            sets.append(f"{k}=%s::jsonb")
            args.append(_as_jsonb(v))
        elif k in {'reported_at', 'disabled_at'} and v == 'NOW()':
            sets.append(f"{k}=NOW()")
        else:
            sets.append(f"{k}=%s")
            args.append(v)
    if not sets:
        return get_local_source(local_source_id)
    sets.append('updated_at=NOW()')
    args.append(local_source_id)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE shared_rapid_sources SET {', '.join(sets)} WHERE id=%s RETURNING *", args)
            row = _row(cur.fetchone())
            conn.commit()
            return row


def get_local_source(local_source_id: int):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM shared_rapid_sources WHERE id=%s", (int(local_source_id),))
            return _row(cur.fetchone())


def delete_local_source(local_source_id: int) -> Dict[str, Any]:
    """彻底删除本地 Rapid 共享索引。

    只处理本机数据库：先删 shared_rapid_source_files，再删 shared_rapid_sources。
    中心端取消登记由 routes/shared_resource.py 在调用本函数前完成，避免这里耦合网络请求。
    """
    local_source_id = int(local_source_id)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM shared_rapid_sources WHERE id=%s FOR UPDATE", (local_source_id,))
            row = _row(cur.fetchone())
            if not row:
                conn.commit()
                return {}
            cur.execute("DELETE FROM shared_rapid_source_files WHERE local_source_id=%s", (local_source_id,))
            files_deleted = cur.rowcount or 0
            cur.execute("DELETE FROM shared_rapid_sources WHERE id=%s", (local_source_id,))
            sources_deleted = cur.rowcount or 0
            conn.commit()
            row['_deleted_files'] = files_deleted
            row['_deleted_sources'] = sources_deleted
            return row


def delete_local_sources(local_source_ids: List[int]) -> Dict[str, Any]:
    deleted = []
    missing = []
    for value in local_source_ids or []:
        try:
            sid = int(value)
        except Exception:
            continue
        row = delete_local_source(sid)
        if row:
            deleted.append(row)
        else:
            missing.append(sid)
    return {'deleted': deleted, 'missing': missing, 'count': len(deleted)}


def list_local_sources(status='all', keyword='', page=1, page_size=30, order_by='created_desc') -> Tuple[List[Dict[str, Any]], int]:
    page = max(1, int(page or 1))
    page_size = min(500, max(1, int(page_size or 30)))
    if '_local_sources_where_sql' in globals() and '_local_sources_order_sql' in globals():
        where_sql, args = _local_sources_where_sql(status=status, keyword=keyword)
        order_sql = _local_sources_order_sql(order_by=order_by)
    else:
        where, args = [], []
        if status and status != 'all':
            statuses = [s.strip() for s in str(status).split(',') if s.strip()]
            where.append('status = ANY(%s)')
            args.append(statuses)
        if keyword:
            kw = f"%{keyword}%"
            where.append('(title ILIKE %s OR file_name ILIKE %s OR tmdb_id ILIKE %s OR sha1 ILIKE %s OR center_source_id ILIKE %s)')
            args.extend([kw, kw, kw, kw, kw])
        where_sql = 'WHERE ' + ' AND '.join(where) if where else ''
        order_sql = 'created_at DESC NULLS LAST, id DESC'
        if order_by == 'updated_desc':
            order_sql = 'updated_at DESC NULLS LAST, id DESC'
        elif order_by == 'created_asc':
            order_sql = 'created_at ASC NULLS LAST, id ASC'
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS n FROM shared_rapid_sources {where_sql}", args)
            total = int((_row(cur.fetchone()) or {}).get('n') or 0)
            cur.execute(f"SELECT * FROM shared_rapid_sources {where_sql} ORDER BY {order_sql} LIMIT %s OFFSET %s", args + [page_size, (page - 1) * page_size])
            return _rows(cur.fetchall()), total


def _local_sources_where_sql(status='all', keyword='') -> Tuple[str, List[Any]]:
    """构造本地共享源列表查询条件。我的共享源需要先全量取出再按季聚合，
    所以普通分页查询和管理页全量查询共用同一套 where，避免口径漂移。
    """
    where, args = [], []
    if status and status != 'all':
        statuses = [s.strip() for s in str(status).split(',') if s.strip()]
        where.append('status = ANY(%s)')
        args.append(statuses)
    if keyword:
        kw = f"%{keyword}%"
        where.append('(title ILIKE %s OR file_name ILIKE %s OR tmdb_id ILIKE %s OR sha1 ILIKE %s OR center_source_id ILIKE %s)')
        args.extend([kw, kw, kw, kw, kw])
    where_sql = 'WHERE ' + ' AND '.join(where) if where else ''
    return where_sql, args


def _local_sources_order_sql(order_by='created_desc') -> str:
    order_sql = 'created_at DESC NULLS LAST, id DESC'
    if order_by == 'updated_desc':
        order_sql = 'updated_at DESC NULLS LAST, id DESC'
    elif order_by == 'created_asc':
        order_sql = 'created_at ASC NULLS LAST, id ASC'
    return order_sql


def list_all_local_sources(status='all', keyword='', order_by='created_desc', limit: int = 200000) -> Tuple[List[Dict[str, Any]], int]:
    """我的共享源管理页专用：拉取完整候选集用于聚合后分页。

    list_local_sources 为通用小查询保留了 page_size<=500 的保护；但“我的共享源”
    需要先把分集源按季聚合、再按 Rapid 状态筛选、最后分页。若仍复用
    list_local_sources(page_size=100000)，会被内部 500 上限截断，导致用户实际有
    几千条共享源时前端只看到一页。
    """
    limit = max(1, min(int(limit or 200000), 200000))
    where_sql, args = _local_sources_where_sql(status=status, keyword=keyword)
    order_sql = _local_sources_order_sql(order_by=order_by)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS n FROM shared_rapid_sources {where_sql}", args)
            total = int((_row(cur.fetchone()) or {}).get('n') or 0)
            cur.execute(
                f"SELECT * FROM shared_rapid_sources {where_sql} ORDER BY {order_sql} LIMIT %s",
                args + [limit],
            )
            return _rows(cur.fetchall()), total


def list_source_files(local_source_id: int) -> List[Dict[str, Any]]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM shared_rapid_source_files WHERE local_source_id=%s ORDER BY episode_number NULLS LAST, file_name ASC", (int(local_source_id),))
            return _rows(cur.fetchall())


def get_local_source_by_center(source_kind: str, center_source_id: str) -> Dict[str, Any]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM shared_rapid_sources WHERE source_kind=%s AND center_source_id=%s LIMIT 1", (source_kind, center_source_id))
            return _row(cur.fetchone()) or {}


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
    return {'shares': shares, 'credit': credit}


def raw_ffprobe_for_sha1(sha1: str) -> Dict[str, Any]:
    sha1 = _norm_sha1(sha1)
    if not sha1:
        return {}
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT raw_ffprobe_json, mediainfo_json FROM p115_mediainfo_cache WHERE sha1=%s", (sha1,))
            row = cur.fetchone()
            return _row(row) or {}


def p115_file_rows_by_sha1_or_pc(sha1s: List[str] = None, pickcodes: List[str] = None) -> List[Dict[str, Any]]:
    sha1s = [_norm_sha1(x) for x in (sha1s or [])]
    sha1s = [x for x in sha1s if x]
    pcs = [str(x or '').strip() for x in (pickcodes or []) if str(x or '').strip()]
    if not sha1s and not pcs:
        return []
    clauses, args = [], []
    if sha1s:
        clauses.append('UPPER(sha1)=ANY(%s)')
        args.append(sha1s)
    if pcs:
        clauses.append('pick_code=ANY(%s)')
        args.append(pcs)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM p115_filesystem_cache WHERE {' OR '.join(clauses)} ORDER BY updated_at DESC NULLS LAST", args)
            return _rows(cur.fetchall())


def _media_rows_for_search(keyword: str = '', limit: int = 200) -> List[Dict[str, Any]]:
    """搜索可手动登记的本地媒体。

    Rapid v2 不需要 115 分享码，真正需要的是：media_metadata 里能定位到
    已入库文件的 PC/SHA1，并能反查到 p115_filesystem_cache。Season 自身
    经常是占位行/未入库，真实文件在 Episode 行里，所以这里不能只看
    Season.in_library；只要该父剧该季存在已入库 Episode，就允许返回季候选。
    """
    keyword = str(keyword or '').strip()
    args = []
    where = ["m.item_type IN ('Movie','Series','Season','Episode')"]
    if keyword:
        kw = f"%{keyword}%"
        where.append("""
            (
                m.title ILIKE %s OR m.original_title ILIKE %s OR m.tmdb_id ILIKE %s OR m.parent_series_tmdb_id ILIKE %s
             OR p.title ILIKE %s OR p.original_title ILIKE %s OR p.tmdb_id ILIKE %s
            )
        """)
        args.extend([kw, kw, kw, kw, kw, kw, kw])

    # 电影/单集必须自身入库；Series/Season 可以是未入库占位，只要旗下有已入库 Episode。
    where.append("""
        (
            COALESCE(m.in_library, FALSE) = TRUE
         OR (
                m.item_type = 'Series'
            AND EXISTS (
                SELECT 1 FROM media_metadata e
                WHERE e.item_type = 'Episode'
                  AND COALESCE(e.in_library, FALSE) = TRUE
                  AND e.parent_series_tmdb_id = m.tmdb_id
            )
         )
         OR (
                m.item_type = 'Season'
            AND EXISTS (
                SELECT 1 FROM media_metadata e
                WHERE e.item_type = 'Episode'
                  AND COALESCE(e.in_library, FALSE) = TRUE
                  AND e.parent_series_tmdb_id = COALESCE(NULLIF(m.parent_series_tmdb_id, ''), m.tmdb_id)
                  AND (m.season_number IS NULL OR e.season_number = m.season_number)
            )
         )
        )
    """)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT
                    m.*,
                    p.title AS series_title,
                    p.original_title AS series_original_title,
                    p.release_year AS series_release_year,
                    se.watching_status AS season_watching_status,
                    se.total_episodes AS season_total_episodes
                FROM media_metadata m
                LEFT JOIN media_metadata p
                  ON p.item_type = 'Series'
                 AND p.tmdb_id = COALESCE(NULLIF(m.parent_series_tmdb_id, ''), CASE WHEN m.item_type='Series' THEN m.tmdb_id ELSE NULL END)
                LEFT JOIN media_metadata se
                  ON se.item_type = 'Season'
                 AND se.season_number = m.season_number
                 AND COALESCE(NULLIF(se.parent_series_tmdb_id, ''), se.tmdb_id) = COALESCE(NULLIF(m.parent_series_tmdb_id, ''), CASE WHEN m.item_type='Season' THEN m.tmdb_id ELSE NULL END)
                WHERE {' AND '.join(where)}
                ORDER BY
                    CASE m.item_type WHEN 'Movie' THEN 0 WHEN 'Series' THEN 1 WHEN 'Season' THEN 2 ELSE 3 END,
                    COALESCE(m.date_added, m.created_at, m.last_updated_at) DESC NULLS LAST
                LIMIT %s
            """, args + [max(1, min(int(limit or 200), 2000))])
            return _rows(cur.fetchall())


def _episode_rows(parent_tmdb_id: str, season_number=None) -> List[Dict[str, Any]]:
    parent_tmdb_id = str(parent_tmdb_id or '').strip()
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM media_metadata
                WHERE item_type='Episode' AND COALESCE(in_library, FALSE)=TRUE
                  AND parent_series_tmdb_id=%s
                  AND (%s IS NULL OR season_number=%s)
                ORDER BY season_number ASC, episode_number ASC
                """,
                (parent_tmdb_id, _nullable_int(season_number), _nullable_int(season_number)),
            )
            return _rows(cur.fetchall())


def _files_for_media_row(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    row = dict(row or {})
    item_type = row.get('item_type')
    sha1s = [_norm_sha1(x) for x in _as_array(row.get('file_sha1_json'))]
    sha1s = [x for x in sha1s if x]
    pcs = _as_array(row.get('file_pickcode_json'))
    file_rows = p115_file_rows_by_sha1_or_pc(sha1s, pcs)
    by_key = {}
    for f in file_rows:
        if not _is_video_name(f.get('name')):
            continue
        sha1 = _norm_sha1(f.get('sha1')) or next((x for x in sha1s if x), '')
        if not sha1:
            continue
        key = f.get('id') or sha1
        by_key[key] = {
            'fid': str(f.get('id') or ''),
            'parent_id': str(f.get('parent_id') or ''),
            'pick_code': f.get('pick_code') or '',
            'sha1': sha1,
            'preid': _norm_sha1(f.get('preid')) or '',
            'size': _safe_int(f.get('size'), 0),
            'file_name': f.get('name') or '',
            'relative_path': f.get('local_path') or f.get('name') or '',
            'tmdb_id': row.get('parent_series_tmdb_id') if item_type == 'Episode' else row.get('tmdb_id'),
            'item_type': item_type,
            'season_number': row.get('season_number'),
            'episode_number': row.get('episode_number') if item_type == 'Episode' else _guess_episode_number(f.get('name')),
            'raw_json': {'media_row': row, 'p115_cache': f},
        }
    return list(by_key.values())



def _get_cache_node(fid: str) -> Dict[str, Any]:
    fid = str(fid or '').strip()
    if not fid:
        return {}
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, parent_id, name, local_path
                    FROM p115_filesystem_cache
                    WHERE id=%s
                    LIMIT 1
                    """,
                    (fid,),
                )
                return _row(cur.fetchone()) or {}
    except Exception:
        return {}


def _ancestor_chain(fid: str, max_depth: int = 30) -> List[Dict[str, Any]]:
    """返回从当前节点到根的祖先链，第一项是当前节点。"""
    chain = []
    seen = set()
    current = str(fid or '').strip()
    for _ in range(max_depth):
        if not current or current in seen or current == '0':
            break
        seen.add(current)
        node = _get_cache_node(current)
        if not node:
            # 没有缓存行时至少保留 id，避免调用方完全丢失 root_fid。
            chain.append({'id': current, 'parent_id': '', 'name': current})
            break
        node_id = str(node.get('id') or current)
        node['id'] = node_id
        chain.append(node)
        parent_id = str(node.get('parent_id') or '').strip()
        if not parent_id or parent_id == current:
            break
        current = parent_id
    return chain


def _common_ancestor_for_parents(parent_ids: List[str]) -> Dict[str, Any]:
    """多个文件父目录不同时，推导最深公共祖先，尽量回到季目录 root_fid。"""
    parents = [str(x or '').strip() for x in parent_ids if str(x or '').strip()]
    if not parents:
        return {}
    if len(set(parents)) == 1:
        node = _get_cache_node(parents[0])
        return node or {'id': parents[0], 'name': parents[0]}

    chains = []
    for pid in parents:
        chain = _ancestor_chain(pid)
        if not chain:
            return {}
        # 从根到叶子比较。
        chains.append(list(reversed(chain)))

    common = None
    min_len = min(len(c) for c in chains)
    for idx in range(min_len):
        ids = {str(c[idx].get('id') or '') for c in chains}
        if len(ids) != 1:
            break
        common = chains[0][idx]
    return common or {}


def _series_title_for_consistency(parent_tmdb_id: str) -> str:
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT title
                    FROM media_metadata
                    WHERE tmdb_id=%s AND item_type='Series'
                    LIMIT 1
                    """,
                    (str(parent_tmdb_id or ''),),
                )
                row = cur.fetchone()
                return str((row or {}).get('title') or '').strip() if row else ''
    except Exception:
        return ''


def repair_candidate_fingerprints(data: Dict[str, Any], *, log_result: bool = True) -> Dict[str, Any]:
    """手动登记/登记中心前的季级指纹体检。

    旧分享模式在季包分享前会走 helpers.check_season_consistency，顺手补齐
    file_pickcode_json / file_sha1_json / p115_filesystem_cache。Rapid v2 手动登记如果
    只按现有 PC/SHA1 反查，就会在旧数据缺缓存时显示 0 个文件。这里把同一套体检逻辑
    接回手动登记链路：不因为一致性失败而阻止追更分集入池，只负责尽量补齐 root_fid
    所需的文件缓存。
    """
    data = dict(data or {})
    item_type = str(data.get('item_type') or data.get('share_item_type') or '').strip()
    if item_type not in ('Season', 'Episode'):
        return {'ok': True, 'skipped': True, 'reason': 'not_season'}

    parent_tmdb_id = str(data.get('parent_series_tmdb_id') or data.get('series_tmdb_id') or data.get('tmdb_id') or '').strip()
    season_number = _nullable_int(data.get('season_number'))
    if not parent_tmdb_id or season_number is None:
        return {'ok': False, 'reason': 'missing_identity', 'message': '缺少父剧 TMDb ID 或季号，无法执行季级指纹体检'}

    is_completed_season = _is_completed_season_candidate(data)
    expected = _safe_int(data.get('expected_episode_count') or data.get('total_episodes'), 0)
    if is_completed_season and expected <= 0:
        expected = _strict_expected_episode_count_for_season(parent_tmdb_id, season_number)
        if expected > 0:
            data['expected_episode_count'] = expected
            data['total_episodes'] = expected
    require_expected = bool(data.get('_require_expected_episode_count') or is_completed_season)
    series_name = str(data.get('series_title') or data.get('title') or '').strip()
    if not series_name or re.search(r'\bS\d{1,3}(?:E\d{1,4})?\b|第\s*\d+\s*季', series_name, re.IGNORECASE):
        series_name = _series_title_for_consistency(parent_tmdb_id) or series_name

    try:
        from tasks import helpers
        return helpers.check_season_consistency(
            parent_tmdb_id,
            season_number,
            expected_episode_count=expected,
            series_name=series_name,
            rows=None,
            log_result=log_result,
            processor=None,
            repair_missing_fingerprints=True,
            require_expected_episode_count=require_expected,
        )
    except Exception as e:
        logger.warning(
            "  ➜ [共享资源] 手动登记前执行季级指纹体检失败: tmdb=%s, season=%s, err=%s",
            parent_tmdb_id,
            season_number,
            e,
            exc_info=True,
        )
        return {'ok': False, 'reason': 'repair_error', 'message': str(e)}

def _candidate_root_from_files(files: List[Dict[str, Any]]) -> Dict[str, Any]:
    files = [f for f in (files or []) if isinstance(f, dict)]
    if not files:
        return {'root_fid': '', 'root_name': '', 'root_is_dir': True}
    if len(files) == 1:
        f = files[0]
        parent_id = str(f.get('parent_id') or '').strip()
        # 单文件 Movie/Episode 的 root_fid 用文件自身；同时把父目录放进 raw，后续追踪可用。
        return {
            'root_fid': str(f.get('fid') or ''),
            'root_name': f.get('file_name') or f.get('relative_path') or str(f.get('fid') or ''),
            'root_is_dir': False,
            'parent_fid': parent_id,
        }

    parents = [str(f.get('parent_id') or '').strip() for f in files if str(f.get('parent_id') or '').strip()]
    common = _common_ancestor_for_parents(parents)
    if common and common.get('id'):
        root_id = str(common.get('id'))
        root_name = str(common.get('name') or '').strip()
        if not root_name:
            rel = str(files[0].get('relative_path') or '').replace('\\', '/')
            root_name = rel.split('/')[-2] if '/' in rel else f'{len(files)} 个文件公共目录'
        return {'root_fid': root_id, 'root_name': root_name, 'root_is_dir': True}

    return {'root_fid': '', 'root_name': f'{len(files)} 个已定位文件', 'root_is_dir': True}


def candidate_root_from_files(files: List[Dict[str, Any]]) -> Dict[str, Any]:
    return _candidate_root_from_files(files)


def _candidate_title(row: Dict[str, Any], item_type: str, tmdb_id: str, season=None, episode=None) -> str:
    row = row or {}
    series_title = row.get('series_title') or row.get('series_original_title')
    base = series_title if item_type in ('Season', 'Episode') and series_title else (row.get('title') or row.get('original_title') or tmdb_id)
    if item_type == 'Season' and season not in (None, ''):
        try:
            return f"{base} S{int(season):02d}"
        except Exception:
            return f"{base} S{season}"
    if item_type == 'Episode' and season not in (None, '') and episode not in (None, ''):
        try:
            return f"{base} S{int(season):02d}E{int(episode):02d}"
        except Exception:
            return base
    return base


def build_shareable_candidate(row: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(row or {})
    item_type = str(row.get('item_type') or '')
    season = row.get('season_number')
    episode = row.get('episode_number')

    if item_type in ('Season', 'Episode'):
        tmdb_id = str(row.get('parent_series_tmdb_id') or row.get('tmdb_id') or '')
    else:
        tmdb_id = str(row.get('tmdb_id') or '')

    files: List[Dict[str, Any]] = []
    consistency = None
    share_item_type = item_type
    share_type = 'movie_file'
    if item_type == 'Movie':
        files = _files_for_media_row(row)
        share_type = 'movie_file' if len(files) <= 1 else 'movie_folder'
    elif item_type == 'Episode':
        files = _files_for_media_row(row)
        share_type = 'episode_file'
    elif item_type == 'Season':
        files = []
        ep_rows = _episode_rows(tmdb_id, season)
        for ep_row in ep_rows:
            files.extend(_files_for_media_row(ep_row))
        consistency = None
        if not files and ep_rows:
            consistency = repair_candidate_fingerprints({**row, 'parent_series_tmdb_id': tmdb_id, 'item_type': 'Season', 'season_number': season}, log_result=True)
            files = []
            for ep_row in _episode_rows(tmdb_id, season):
                files.extend(_files_for_media_row(ep_row))
        share_type = 'season_pack'
        share_item_type = 'Season'
    elif item_type == 'Series':
        # Series 行只用于展开季候选；不直接登记整剧。
        files = []
        share_type = 'series_pack'

    root = _candidate_root_from_files(files)
    title = _candidate_title(row, share_item_type, tmdb_id, season, episode)
    resolvable = bool(files)
    message = f'已定位 {len(files)} 个可登记视频文件' if resolvable else '未定位到已入库视频文件；需要 media_metadata 中有 PC/SHA1 且 p115_filesystem_cache 能反查到文件'
    if consistency and isinstance(consistency, dict) and consistency.get('message'):
        message = f"{message}；{consistency.get('message')}"
    in_library = bool(resolvable or row.get('in_library'))

    return {
        'tmdb_id': tmdb_id,
        'share_tmdb_id': tmdb_id,
        'item_type': item_type,
        'share_item_type': share_item_type,
        'parent_series_tmdb_id': row.get('parent_series_tmdb_id') or (tmdb_id if share_item_type in ('Season', 'Episode') else ''),
        'season_number': season,
        'episode_number': episode,
        'title': title,
        'standard_title': title,
        'display_title': title,
        'release_year': row.get('release_year') or row.get('series_release_year'),
        'watching_status': row.get('watching_status') or '',
        'season_status': row.get('watching_status') or '',
        'total_episodes': _safe_int(row.get('total_episodes'), 0) or None,
        'expected_episode_count': _safe_int(row.get('total_episodes'), 0) or None,
        'in_library': in_library,
        'source_in_library': bool(row.get('in_library')),
        'share_type': share_type,
        'root_fid': root.get('root_fid') or '',
        'root_name': root.get('root_name') or title,
        'root_is_dir': root.get('root_is_dir') is not False,
        'file_count': len(files),
        'resolvable': resolvable,
        'message': message,
        'consistency': consistency or {},
        'source_provider': 'manual_rapid',
        'raw_json': row,
    }


def season_metadata_row(parent_tmdb_id: str, season_number=None) -> Dict[str, Any]:
    """读取某父剧某季的 Season 元数据行，用于手动共享候选补齐 watching_status / total_episodes。"""
    parent_tmdb_id = str(parent_tmdb_id or '').strip()
    season = _nullable_int(season_number)
    if not parent_tmdb_id or season is None:
        return {}
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM media_metadata
                    WHERE item_type='Season'
                      AND season_number=%s
                      AND COALESCE(NULLIF(parent_series_tmdb_id, ''), tmdb_id)=%s
                    ORDER BY last_updated_at DESC NULLS LAST, created_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (season, parent_tmdb_id),
                )
                return _row(cur.fetchone()) or {}
    except Exception as e:
        logger.debug(f"  ➜ [共享资源] 查询季元数据失败: tmdb={parent_tmdb_id}, season={season}, err={e}")
        return {}


def search_shareable_media(keyword='', search_limit=300, result_limit=500) -> List[Dict[str, Any]]:
    rows = _media_rows_for_search(keyword, search_limit)
    result = []
    seen = set()
    for row in rows:
        item_type = str(row.get('item_type') or '')
        # Series 本身不直接共享；转成已有季候选。
        if item_type == 'Series':
            eps = _episode_rows(row.get('tmdb_id'))
            seasons = sorted({e.get('season_number') for e in eps if e.get('season_number') is not None})
            for sn in seasons:
                key = (row.get('tmdb_id'), 'Season', sn, None)
                if key in seen:
                    continue
                season_row = season_metadata_row(row.get('tmdb_id'), sn)
                season_candidate_row = {
                    **(season_row or {}),
                    'item_type': 'Season',
                    'season_number': sn,
                    'parent_series_tmdb_id': row.get('tmdb_id'),
                    'tmdb_id': (season_row or {}).get('tmdb_id') or row.get('tmdb_id'),
                    'series_title': row.get('title') or row.get('original_title'),
                    'series_original_title': row.get('original_title'),
                    'series_release_year': row.get('release_year'),
                }
                cand = build_shareable_candidate(season_candidate_row)
                if not cand.get('resolvable'):
                    continue
                seen.add(key)
                result.append(cand)
        else:
            cand = build_shareable_candidate(row)
            key = (cand.get('share_tmdb_id') or cand.get('tmdb_id'), cand.get('share_item_type') or cand.get('item_type'), cand.get('season_number'), cand.get('episode_number'))
            if key in seen:
                continue
            seen.add(key)
            result.append(cand)
        if len(result) >= int(result_limit or 500):
            break
    return result


def collect_files_for_candidate(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = dict(data or {})
    item_type = str(data.get('item_type') or '')
    tmdb_id = str(data.get('parent_series_tmdb_id') or data.get('tmdb_id') or '') if item_type in ('Season', 'Episode') else str(data.get('tmdb_id') or '')
    season = _nullable_int(data.get('season_number'))
    episode = _nullable_int(data.get('episode_number'))
    files = []
    # 只有“明确完结季”才允许走 helpers.check_season_consistency。
    # 连载季 / 单集只做本地文件定位，不做季级一致性校验，避免维护任务或一键登记把连载季拖去体检。
    watching_status = str(data.get('watching_status') or data.get('season_status') or '').strip().lower()
    provider = str(data.get('source_provider') or data.get('_original_source_provider') or '').strip().lower()
    is_completed_season = (
        item_type == 'Season'
        and (
            bool(data.get('_force_completed_season'))
            or watching_status in {'Completed', 'complete', 'ended', 'end', '完结', '已完结'}
            or provider == 'rapid_completed_season'
        )
    )
    if is_completed_season and not data.get('_raw_repair_only') and not data.get('_skip_fingerprint_repair'):
        # 完结季必须严格体检；这里只负责补齐/校验所需指纹，是否允许登记由任务层统一拦截。
        repair_candidate_fingerprints(data, log_result=True)

    if item_type == 'Movie':
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM media_metadata WHERE tmdb_id=%s AND item_type='Movie' LIMIT 1", (tmdb_id,))
                row = _row(cur.fetchone())
        files = _files_for_media_row(row or {})
    elif item_type == 'Episode':
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM media_metadata
                    WHERE item_type='Episode' AND in_library=TRUE
                      AND (tmdb_id=%s OR parent_series_tmdb_id=%s)
                      AND (%s IS NULL OR season_number=%s)
                      AND (%s IS NULL OR episode_number=%s)
                    ORDER BY date_added DESC NULLS LAST LIMIT 5
                    """,
                    (data.get('tmdb_id'), tmdb_id, season, season, episode, episode),
                )
                rows = _rows(cur.fetchall())
        for row in rows:
            files.extend(_files_for_media_row(row))
    elif item_type == 'Season':
        for row in _episode_rows(tmdb_id, season):
            files.extend(_files_for_media_row(row))
    # 去重
    out, seen = [], set()
    for f in files:
        key = (f.get('fid'), f.get('sha1'), f.get('episode_number'))
        if key in seen:
            continue
        seen.add(key)
        out.append(f)
    return out



_EFFECTIVE_SHARE_STATUSES = {'active', 'available', 'updating'}


def _load_effective_local_share_index(limit: int = 500000) -> Dict[str, Any]:
    """读取本机已经有效登记到中心的共享索引，用于一键登记增量过滤。"""
    try:
        limit = max(1, min(int(limit or 500000), 500000))
    except Exception:
        limit = 500000

    index = {
        'source_ids': set(),
        'movie_identities': set(),
        'movie_sha1s': set(),
        'episode_identities': set(),
        'episode_sha1s': set(),
        'completed_seasons': {},
    }
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    s.id,
                    s.source_kind,
                    s.tmdb_id,
                    s.season_number,
                    s.episode_number AS source_episode_number,
                    s.center_source_id,
                    s.manifest_hash,
                    NULLIF(UPPER(COALESCE(s.sha1, '')), '') AS source_sha1,
                    f.episode_number AS file_episode_number,
                    NULLIF(UPPER(COALESCE(f.sha1, '')), '') AS file_sha1
                FROM shared_rapid_sources s
                LEFT JOIN shared_rapid_source_files f ON f.local_source_id = s.id
                WHERE COALESCE(s.status, '') = ANY(%s)
                  AND COALESCE(s.center_status, '') <> 'disabled'
                  AND (
                        COALESCE(s.center_source_id, '') <> ''
                     OR COALESCE(s.center_status, '') = 'reported'
                  )
                ORDER BY s.updated_at DESC NULLS LAST, s.id DESC
                LIMIT %s
                """,
                (list(_EFFECTIVE_SHARE_STATUSES), limit),
            )
            rows = _rows(cur.fetchall())

    for row in rows:
        source_id = row.get('id')
        if source_id:
            index['source_ids'].add(int(source_id))
        kind = str(row.get('source_kind') or '').strip().lower()
        tmdb_id = str(row.get('tmdb_id') or '').strip()
        season = _nullable_int(row.get('season_number'))
        source_ep = _nullable_int(row.get('source_episode_number'))
        file_ep = _nullable_int(row.get('file_episode_number'))
        ep_no = file_ep if file_ep is not None else source_ep
        sha1 = _norm_sha1(row.get('file_sha1')) or _norm_sha1(row.get('source_sha1'))
        if not tmdb_id:
            continue

        if kind == 'movie':
            index['movie_identities'].add(tmdb_id)
            if sha1:
                index['movie_sha1s'].add((tmdb_id, sha1))
        elif kind == 'episode':
            if season is not None and ep_no is not None:
                index['episode_identities'].add((tmdb_id, season, ep_no))
                if sha1:
                    index['episode_sha1s'].add((tmdb_id, season, ep_no, sha1))
        elif kind == 'completed_season':
            if season is not None:
                index['completed_seasons'][(tmdb_id, season)] = row.get('manifest_hash') or True
    index['source_count'] = len(index['source_ids'])
    return index


def _row_sha1s(row: Dict[str, Any]) -> List[str]:
    return [_norm_sha1(x) for x in _as_array((row or {}).get('file_sha1_json')) if _norm_sha1(x)]


def _is_completed_status(value: Any) -> bool:
    return str(value or '').strip().lower() in {'completed', 'complete', 'ended', 'end', '完结', '已完结'}


def _is_completed_season_candidate(data: Dict[str, Any]) -> bool:
    data = dict(data or {})
    item_type = str(data.get('item_type') or data.get('share_item_type') or '').strip()
    if item_type != 'Season':
        return False
    provider = str(data.get('source_provider') or data.get('_original_source_provider') or '').strip().lower()
    watching_status = str(data.get('watching_status') or data.get('season_status') or '').strip()
    return bool(
        data.get('_force_completed_season')
        or provider == 'rapid_completed_season'
        or _is_completed_status(watching_status)
    )


def _strict_expected_episode_count_for_season(parent_tmdb_id: str, season_number) -> int:
    """读取指定季的官方总集数，绝不回退成本地已入库集数。

    完结季质量门禁必须使用 Season.total_episodes 或 Episode.total_episodes
    这类元数据字段；如果字段缺失，就返回 0，让调用方按“缺少官方总集数”拦截。
    不能用 COUNT(已入库集) 兜底，否则历史脏数据 9/27 会被误判为 9/9 达标。
    """
    parent = str(parent_tmdb_id or '').strip()
    season = _nullable_int(season_number)
    if not parent or season is None:
        return 0
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT MAX(NULLIF(total_episodes, 0))::integer AS total
                    FROM media_metadata
                    WHERE item_type='Season'
                      AND season_number=%s
                      AND COALESCE(NULLIF(parent_series_tmdb_id, ''), tmdb_id)=%s
                    """,
                    (season, parent),
                )
                row = _row(cur.fetchone()) or {}
                value = _safe_int(row.get('total'), 0)
                if value > 0:
                    return value

                cur.execute(
                    """
                    SELECT MAX(NULLIF(total_episodes, 0))::integer AS total
                    FROM media_metadata
                    WHERE item_type='Episode'
                      AND parent_series_tmdb_id=%s
                      AND season_number=%s
                    """,
                    (parent, season),
                )
                row = _row(cur.fetchone()) or {}
                value = _safe_int(row.get('total'), 0)
                if value > 0:
                    return value
    except Exception as e:
        logger.debug(
            "  ➜ [共享资源] 读取完结季官方总集数失败: tmdb=%s, season=%s, err=%s",
            parent, season, e,
        )
    return 0


def _movie_row_already_registered(row: Dict[str, Any], index: Dict[str, Any]) -> bool:
    tmdb_id = str((row or {}).get('tmdb_id') or '').strip()
    if not tmdb_id:
        return False
    sha1s = _row_sha1s(row)
    if sha1s:
        return all((tmdb_id, sha1) in index.get('movie_sha1s', set()) for sha1 in sha1s)
    return tmdb_id in index.get('movie_identities', set())


def _episode_row_already_registered(row: Dict[str, Any], index: Dict[str, Any]) -> bool:
    parent = str((row or {}).get('parent_series_tmdb_id') or (row or {}).get('tmdb_id') or '').strip()
    season = _nullable_int((row or {}).get('season_number'))
    episode = _nullable_int((row or {}).get('episode_number'))
    if not parent or season is None or episode is None:
        return False
    sha1s = _row_sha1s(row)
    if sha1s:
        return all((parent, season, episode, sha1) in index.get('episode_sha1s', set()) for sha1 in sha1s)
    return (parent, season, episode) in index.get('episode_identities', set())


def _season_episode_rows_all_registered(parent_tmdb_id: str, season_number, index: Dict[str, Any]) -> bool:
    parent = str(parent_tmdb_id or '').strip()
    season = _nullable_int(season_number)
    if not parent or season is None:
        return False
    rows = _episode_rows(parent, season)
    if not rows:
        return False
    has_file = False
    for ep_row in rows:
        ep_no = _nullable_int(ep_row.get('episode_number'))
        if ep_no is None:
            return False
        sha1s = _row_sha1s(ep_row)
        if sha1s:
            has_file = True
            if not all((parent, season, ep_no, sha1) in index.get('episode_sha1s', set()) for sha1 in sha1s):
                return False
        elif (parent, season, ep_no) not in index.get('episode_identities', set()):
            return False
    return has_file


def _season_row_already_registered(row: Dict[str, Any], index: Dict[str, Any]) -> bool:
    parent = str((row or {}).get('parent_series_tmdb_id') or (row or {}).get('tmdb_id') or '').strip()
    season = _nullable_int((row or {}).get('season_number'))
    if not parent or season is None:
        return False
    if _is_completed_status((row or {}).get('watching_status')) and (parent, season) in index.get('completed_seasons', {}):
        return True
    return _season_episode_rows_all_registered(parent, season, index)


def _media_row_already_registered(row: Dict[str, Any], index: Dict[str, Any]) -> bool:
    if not index:
        return False
    item_type = str((row or {}).get('item_type') or '').strip()
    if item_type == 'Movie':
        return _movie_row_already_registered(row, index)
    if item_type == 'Episode':
        return _episode_row_already_registered(row, index)
    if item_type == 'Season':
        return _season_row_already_registered(row, index)
    return False


def _existing_share_index_summary(index: Dict[str, Any]) -> Dict[str, int]:
    index = index or {}
    return {
        'source_count': int(index.get('source_count') or 0),
        'movie_files': len(index.get('movie_sha1s', set()) or []),
        'episode_files': len(index.get('episode_sha1s', set()) or []),
        'completed_seasons': len(index.get('completed_seasons', {}) or {}),
    }


def _emit_scan_progress(progress_callback: Callable[[int, str], None] = None, progress: int = -1, message: str = '') -> None:
    """一键登记扫描阶段的轻量进度回调。这里不能依赖 task_manager，避免数据库层反向导入任务层。"""
    if not callable(progress_callback):
        return
    try:
        progress_callback(int(progress), str(message or ''))
    except Exception:
        pass


def _build_lightweight_share_candidate(row: Dict[str, Any]) -> Dict[str, Any]:
    """全库扫描专用：只构建登记身份，不在扫描阶段解析 115 文件。"""
    row = dict(row or {})
    item_type = str(row.get('item_type') or '').strip()
    season = row.get('season_number')
    episode = row.get('episode_number')
    if item_type in ('Season', 'Episode'):
        tmdb_id = str(row.get('parent_series_tmdb_id') or row.get('tmdb_id') or '')
    else:
        tmdb_id = str(row.get('tmdb_id') or '')

    share_item_type = item_type
    if item_type == 'Movie':
        share_type = 'movie_file'
    elif item_type == 'Episode':
        share_type = 'episode_file'
    elif item_type == 'Season':
        share_type = 'season_pack'
        share_item_type = 'Season'
    else:
        share_type = 'series_pack'

    title = _candidate_title(row, share_item_type, tmdb_id, season, episode)
    sha1s = _row_sha1s(row)
    episode_file_sha1s = []
    if item_type == 'Episode':
        ep_no = _nullable_int(episode)
        for sha1 in sha1s:
            episode_file_sha1s.append({'episode_number': ep_no, 'sha1': sha1})

    strict_total = _safe_int(row.get('season_total_episodes'), 0) if item_type in ('Season', 'Episode') else 0
    if strict_total <= 0:
        strict_total = _safe_int(row.get('total_episodes'), 0)

    return {
        'tmdb_id': tmdb_id,
        'share_tmdb_id': tmdb_id,
        'item_type': item_type,
        'share_item_type': share_item_type,
        'parent_series_tmdb_id': row.get('parent_series_tmdb_id') or (tmdb_id if share_item_type in ('Season', 'Episode') else ''),
        'season_number': season,
        'episode_number': episode,
        'title': title,
        'standard_title': title,
        'display_title': title,
        'release_year': row.get('release_year') or row.get('series_release_year'),
        'watching_status': row.get('watching_status') or '',
        'season_status': row.get('watching_status') or '',
        'total_episodes': strict_total or None,
        'expected_episode_count': strict_total or None,
        'in_library': bool(row.get('in_library')),
        'source_in_library': bool(row.get('in_library')),
        'share_type': share_type,
        'root_fid': '',
        'root_name': title,
        'root_is_dir': item_type == 'Season',
        'file_count': 0,
        'file_sha1s': sha1s,
        'episode_file_sha1s': episode_file_sha1s,
        'manifest_hash': '',
        'resolvable': True,
        'message': '已加入登记队列，登记时再定位本地视频文件',
        'consistency': {},
        'source_provider': 'manual_rapid',
        'raw_json': row,
        '_lazy_collect_files': True,
    }


def all_library_share_candidates(
    limit: int = 100000,
    *,
    exclude_existing: bool = True,
    return_stats: bool = False,
    progress_callback: Callable[[int, str], None] = None,
):
    """一键登记媒体库候选。

    扫描阶段只做“身份枚举 + 增量过滤”，不再解析每个候选的 115 文件；
    真正登记时 register_candidate_to_center 会重新 collect_files_for_candidate。
    """
    timings: Dict[str, float] = {}
    t0 = time.perf_counter()

    _emit_scan_progress(progress_callback, 1, '正在读取本机已有有效共享索引...')
    existing_index = _load_effective_local_share_index() if exclude_existing else {}
    timings['load_existing_index_sec'] = round(time.perf_counter() - t0, 3)
    existing_summary = _existing_share_index_summary(existing_index)

    t_media = time.perf_counter()
    _emit_scan_progress(
        progress_callback,
        2,
        f"已有有效共享：资源 {existing_summary.get('source_count', 0)}，电影 {existing_summary.get('movie_files', 0)}，分集 {existing_summary.get('episode_files', 0)}，完结季 {existing_summary.get('completed_seasons', 0)}。正在读取媒体库候选..."
    )
    rows = _media_rows_for_search('', limit)
    timings['load_media_rows_sec'] = round(time.perf_counter() - t_media, 3)

    result = []
    seen = set()
    skipped_existing = 0
    skipped_duplicate = 0
    skipped_completed_episode = 0
    scanned = len(rows)
    t_filter = time.perf_counter()

    _emit_scan_progress(progress_callback, 3, f'已读取媒体候选 {scanned} 个，正在做增量排除...')

    for idx, row in enumerate(rows, 1):
        item_type = str(row.get('item_type') or '')
        if idx == 1 or idx % 50 == 0 or idx == scanned:
            progress = 3 + int((idx / max(scanned, 1)) * 7)
            _emit_scan_progress(
                progress_callback,
                min(10, progress),
                f'正在筛选媒体候选 {idx}/{scanned}，已排除有效共享 {skipped_existing}，已屏蔽完结季分集 {skipped_completed_episode}，待登记 {len(result)}...'
            )

        if exclude_existing and _media_row_already_registered(row, existing_index):
            skipped_existing += 1
            continue

        if item_type == 'Movie':
            cand = _build_lightweight_share_candidate(row)
            key = ('Movie', cand.get('tmdb_id'), tuple(cand.get('file_sha1s') or _as_array(row.get('file_sha1_json'))))
        elif item_type == 'Episode':
            # 完结季必须以 Season 候选走严格一致性门禁；
            # 一键登记不能绕过季门禁，把历史脏数据的零散分集登记进中心 season_hub。
            if _is_completed_status(row.get('season_watching_status')):
                skipped_completed_episode += 1
                continue
            cand = _build_lightweight_share_candidate(row)
            key = ('Episode', cand.get('tmdb_id'), cand.get('season_number'), cand.get('episode_number'), tuple(cand.get('file_sha1s') or _as_array(row.get('file_sha1_json'))))
        elif item_type == 'Season':
            cand = _build_lightweight_share_candidate(row)
            key = ('Season', cand.get('tmdb_id'), cand.get('season_number'))
        else:
            continue

        if key in seen:
            skipped_duplicate += 1
            continue
        seen.add(key)
        result.append(cand)

    timings['filter_candidates_sec'] = round(time.perf_counter() - t_filter, 3)
    timings['total_scan_sec'] = round(time.perf_counter() - t0, 3)
    _emit_scan_progress(progress_callback, 10, f'候选扫描完成：扫描 {scanned}，排除有效共享 {skipped_existing}，屏蔽完结季分集 {skipped_completed_episode}，重复 {skipped_duplicate}，待登记 {len(result)}。')

    if return_stats:
        return {
            'items': result,
            'scanned': scanned,
            'total': len(result),
            'skipped_existing': skipped_existing,
            'skipped_duplicate': skipped_duplicate,
            'skipped_completed_episode': skipped_completed_episode,
            'existing_index': existing_summary,
            'timings': timings,
        }
    return result



def list_non_effective_local_sources(limit: int = 300) -> List[Dict[str, Any]]:
    """维护任务专用：找出需要重新登记的“非有效”本地共享源。

    有效口径与一键登记增量过滤保持一致：
    - status 属于 active / available / updating；
    - center_status 未被 disabled；
    - 已有 center_source_id 或 center_status=reported。

    disabled / cancelled / deleted 属于用户/清理任务明确停用的源，不自动复活。
    这里不访问 115、不检查中心 RAW、不收集分集文件，只返回本地索引行，
    真正重登记交给 register_candidate_to_center 走统一登记链路。
    """
    try:
        limit = max(1, min(int(limit or 300), 5000))
    except Exception:
        limit = 300

    effective_statuses = list(_EFFECTIVE_SHARE_STATUSES)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM shared_rapid_sources
                WHERE COALESCE(status, '') NOT IN ('disabled', 'cancelled', 'canceled', 'deleted')
                  AND COALESCE(center_status, '') <> 'disabled'
                  AND NOT (
                        COALESCE(status, '') = ANY(%s)
                    AND (
                            COALESCE(center_source_id, '') <> ''
                         OR COALESCE(center_status, '') = 'reported'
                    )
                  )
                ORDER BY updated_at ASC NULLS LAST, id ASC
                LIMIT %s
                """,
                (effective_statuses, limit),
            )
            return _rows(cur.fetchall())


def list_unregistered_airing_episode_candidates(limit: int = 500) -> List[Dict[str, Any]]:
    """维护任务专用：找出“季条目明确处于追更中”的新入库分集。

    只做本地数据库比对，不访问 115，不触发一致性校验：
    - 候选来自 media_metadata.Episode 且 in_library=true；
    - 只信同一父剧同一季的 Season 行 watching_status；
    - 只有 Season.watching_status IN ('Watching', 'Paused') 才视为追更季；
    - 不再参考 Series.watching_status / Episode.watching_status / watchlist_is_airing，
      避免某一季连载时把同剧已完结旧季重新拉出来“鞭尸”；
    - 排除已经有效登记到中心的 episode 源。
    """
    try:
        limit = max(1, min(int(limit or 500), 5000))
    except Exception:
        limit = 500

    existing_index = _load_effective_local_share_index()
    # 先多取一批，后面还要用共享索引做精确去重过滤。
    scan_limit = min(max(limit * 5, limit), 20000)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    e.*,
                    p.title AS series_title,
                    p.original_title AS series_original_title,
                    p.release_year AS series_release_year,
                    se.watching_status AS effective_watching_status,
                    COALESCE(NULLIF(se.total_episodes, 0), NULLIF(e.total_episodes, 0), NULLIF(p.total_episodes, 0), 0) AS effective_total_episodes
                FROM media_metadata e
                INNER JOIN media_metadata se
                  ON se.item_type='Season'
                 AND se.parent_series_tmdb_id=e.parent_series_tmdb_id
                 AND se.season_number=e.season_number
                LEFT JOIN media_metadata p
                  ON p.item_type='Series'
                 AND p.tmdb_id=e.parent_series_tmdb_id
                WHERE e.item_type='Episode'
                  AND COALESCE(e.in_library, FALSE)=TRUE
                  AND NULLIF(e.parent_series_tmdb_id, '') IS NOT NULL
                  AND e.season_number IS NOT NULL
                  AND e.episode_number IS NOT NULL
                  AND LOWER(COALESCE(se.watching_status, '')) IN ('watching', 'paused')
                ORDER BY COALESCE(e.date_added, e.last_updated_at, e.created_at) DESC NULLS LAST,
                         e.parent_series_tmdb_id ASC, e.season_number ASC, e.episode_number ASC
                LIMIT %s
                """,
                (scan_limit,),
            )
            rows = _rows(cur.fetchall())

    out: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        row = dict(row or {})
        if _episode_row_already_registered(row, existing_index):
            continue
        effective_status = str(row.get('effective_watching_status') or '').strip()
        if effective_status:
            row['watching_status'] = effective_status
        total = _safe_int(row.get('effective_total_episodes'), 0)
        if total > 0:
            row['total_episodes'] = total
        cand = _build_lightweight_share_candidate(row)
        cand['item_type'] = 'Episode'
        cand['share_item_type'] = 'Episode'
        cand['_skip_fingerprint_repair'] = True
        cand['_raw_repair_only'] = True
        cand['source_provider'] = 'rapid_followup_backfill'
        key = (
            cand.get('tmdb_id') or cand.get('parent_series_tmdb_id'),
            _nullable_int(cand.get('season_number')),
            _nullable_int(cand.get('episode_number')),
            tuple(cand.get('file_sha1s') or []),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(cand)
        if len(out) >= limit:
            break
    return out

def list_offline_local_sources(limit: int = 300) -> List[Dict[str, Any]]:
    """找出本机仍标记为共享、但本地媒体库已经完全没有对应文件的 Rapid 源。

    这里不能只用 media_metadata.file_sha1_json 判断：很多旧数据/第三方 STRM
    只有 PC 码，SHA1 是通过 p115_filesystem_cache 反查后补齐的。登记中心能成功，
    但 file_sha1_json 可能仍为空；如果维护任务只看 file_sha1_json，就会把所有
    completed_season 误判为“离线”并下架。

    判断口径：
    - movie / episode：登记文件在同一媒体行中通过 SHA1 或 PC 任一命中，视为仍在库；
    - completed_season：包内至少还有一个文件在同季 Episode 行中通过 SHA1 或 PC 命中，就不下架；
      完结包是一组文件，离线清理只负责“整包已不存在”的善后，缺单集/洗版不在这里处理。

    不访问 115，不触发指纹体检，不重新收集文件。这里只负责清理“媒体库已删除/换版后仍在共享”的本地索引。
    """
    try:
        limit = max(1, min(int(limit or 300), 2000))
    except Exception:
        limit = 300

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH candidate_sources AS (
                    SELECT *
                    FROM shared_rapid_sources
                    WHERE COALESCE(status, '') NOT IN ('disabled', 'cancelled')
                      AND COALESCE(center_status, '') <> 'disabled'
                    ORDER BY updated_at ASC NULLS LAST, id ASC
                    LIMIT %s
                ),
                candidate_files AS (
                    SELECT
                        s.id AS local_source_id,
                        s.source_kind,
                        s.tmdb_id,
                        s.season_number,
                        s.episode_number AS source_episode_number,
                        f.id AS file_row_id,
                        NULLIF(UPPER(COALESCE(f.sha1, '')), '') AS file_sha1,
                        NULLIF(COALESCE(f.pick_code, ''), '') AS file_pick_code,
                        f.episode_number AS file_episode_number
                    FROM candidate_sources s
                    JOIN shared_rapid_source_files f
                      ON f.local_source_id = s.id
                     AND (
                            COALESCE(f.sha1, '') <> ''
                         OR COALESCE(f.pick_code, '') <> ''
                     )
                ),
                file_match AS (
                    SELECT
                        cf.local_source_id,
                        COUNT(cf.file_row_id)::integer AS total_files,
                        COUNT(cf.file_row_id) FILTER (
                            WHERE EXISTS (
                                SELECT 1
                                FROM media_metadata m
                                WHERE COALESCE(m.in_library, FALSE) = TRUE
                                  AND (
                                        (
                                            cf.source_kind = 'movie'
                                            AND m.item_type = 'Movie'
                                            AND m.tmdb_id = cf.tmdb_id
                                        )
                                     OR (
                                            cf.source_kind = 'episode'
                                            AND m.item_type = 'Episode'
                                            AND (m.tmdb_id = cf.tmdb_id OR m.parent_series_tmdb_id = cf.tmdb_id)
                                            AND (cf.season_number IS NULL OR m.season_number = cf.season_number)
                                            AND (cf.source_episode_number IS NULL OR m.episode_number = cf.source_episode_number)
                                        )
                                     OR (
                                            cf.source_kind = 'completed_season'
                                            AND m.item_type = 'Episode'
                                            AND m.parent_series_tmdb_id = cf.tmdb_id
                                            AND (cf.season_number IS NULL OR m.season_number = cf.season_number)
                                            AND (cf.file_episode_number IS NULL OR m.episode_number = cf.file_episode_number)
                                        )
                                  )
                                  AND (
                                        (
                                            cf.file_sha1 IS NOT NULL
                                            AND (
                                                   m.file_sha1_json ? cf.file_sha1
                                                OR m.file_sha1_json ? LOWER(cf.file_sha1)
                                                OR COALESCE(m.file_sha1_json::text, '') ILIKE ('%%' || cf.file_sha1 || '%%')
                                                OR EXISTS (
                                                    SELECT 1
                                                    FROM p115_filesystem_cache p
                                                    WHERE UPPER(COALESCE(p.sha1, '')) = cf.file_sha1
                                                      AND COALESCE(p.pick_code, '') <> ''
                                                      AND (
                                                            m.file_pickcode_json ? p.pick_code
                                                         OR COALESCE(m.file_pickcode_json::text, '') LIKE ('%%' || p.pick_code || '%%')
                                                      )
                                                )
                                            )
                                        )
                                     OR (
                                            cf.file_pick_code IS NOT NULL
                                            AND (
                                                   m.file_pickcode_json ? cf.file_pick_code
                                                OR COALESCE(m.file_pickcode_json::text, '') LIKE ('%%' || cf.file_pick_code || '%%')
                                            )
                                        )
                                  )
                            )
                        )::integer AS live_files
                    FROM candidate_files cf
                    GROUP BY cf.local_source_id
                ),
                source_match AS (
                    SELECT
                        s.id AS local_source_id,
                        EXISTS (
                            SELECT 1
                            FROM media_metadata m
                            WHERE COALESCE(m.in_library, FALSE) = TRUE
                              AND (
                                    (
                                        s.source_kind = 'movie'
                                        AND m.item_type = 'Movie'
                                        AND m.tmdb_id = s.tmdb_id
                                    )
                                 OR (
                                        s.source_kind = 'episode'
                                        AND m.item_type = 'Episode'
                                        AND (m.tmdb_id = s.tmdb_id OR m.parent_series_tmdb_id = s.tmdb_id)
                                        AND (s.season_number IS NULL OR m.season_number = s.season_number)
                                        AND (s.episode_number IS NULL OR m.episode_number = s.episode_number)
                                    )
                              )
                              AND (
                                    (
                                        COALESCE(s.sha1, '') <> ''
                                        AND (
                                               m.file_sha1_json ? UPPER(s.sha1)
                                            OR m.file_sha1_json ? LOWER(s.sha1)
                                            OR COALESCE(m.file_sha1_json::text, '') ILIKE ('%%' || UPPER(s.sha1) || '%%')
                                            OR EXISTS (
                                                SELECT 1
                                                FROM p115_filesystem_cache p
                                                WHERE UPPER(COALESCE(p.sha1, '')) = UPPER(s.sha1)
                                                  AND COALESCE(p.pick_code, '') <> ''
                                                  AND (
                                                        m.file_pickcode_json ? p.pick_code
                                                     OR COALESCE(m.file_pickcode_json::text, '') LIKE ('%%' || p.pick_code || '%%')
                                                  )
                                            )
                                        )
                                    )
                                 OR (
                                        COALESCE(s.rapid_meta_json->>'pick_code', '') <> ''
                                        AND (
                                               m.file_pickcode_json ? (s.rapid_meta_json->>'pick_code')
                                            OR COALESCE(m.file_pickcode_json::text, '') LIKE ('%%' || (s.rapid_meta_json->>'pick_code') || '%%')
                                        )
                                    )
                              )
                        ) AS source_live
                    FROM candidate_sources s
                )
                SELECT
                    s.*,
                    COALESCE(f.total_files, 0) AS total_files,
                    COALESCE(f.live_files, 0) AS live_files,
                    CASE
                        WHEN COALESCE(f.total_files, 0) > 0 THEN 'source_file_not_in_library'
                        ELSE 'source_not_in_library'
                    END AS offline_reason
                FROM candidate_sources s
                LEFT JOIN file_match f ON f.local_source_id = s.id
                LEFT JOIN source_match sm ON sm.local_source_id = s.id
                WHERE
                    (
                        COALESCE(f.total_files, 0) > 0
                        AND COALESCE(f.live_files, 0) = 0
                    )
                    OR (
                        COALESCE(f.total_files, 0) = 0
                        AND COALESCE(s.sha1, '') <> ''
                        AND COALESCE(sm.source_live, FALSE) = FALSE
                    )
                ORDER BY s.updated_at ASC NULLS LAST, s.id ASC
                """,
                (limit,),
            )
            return _rows(cur.fetchall())

def disable_local_source(local_source_id: int, *, reason: str = '', center_response: Dict[str, Any] = None) -> Dict[str, Any]:
    """把本地 Rapid 源标记为 disabled，保留原 raw_json，并追加停用原因。"""
    source = get_local_source(local_source_id) or {}
    raw = source.get('raw_json') if isinstance(source.get('raw_json'), dict) else {}
    raw = dict(raw or {})
    raw['disabled_reason'] = reason or 'disabled'
    raw['disabled_at_source'] = 'local_maintenance'
    if center_response is not None:
        raw['center_disable_response'] = center_response
    return update_local_source(
        int(local_source_id),
        status='disabled',
        center_status='disabled',
        disabled_at='NOW()',
        last_error=reason or None,
        raw_json=raw,
    )


# 下面保留少量旧函数名为空实现，避免未改到的调用点抛 AttributeError；不会再创建 115 分享。
def active_share_statuses(): return ['active', 'available', 'updating', 'inconsistent', 'incomplete', 'error']
def invalid_share_statuses(): return ['inconsistent', 'error']
def count_active_share_records(statuses=None):
    rows, total = list_local_sources(status=','.join(statuses or ['active','available']))
    return total
def list_active_share_records(limit: int = 100, statuses=None, order_by: str = 'created_asc'):
    rows, _ = list_local_sources(status=','.join(statuses or ['active','available']), page=1, page_size=limit, order_by=order_by)
    return rows
def list_invalid_share_records(limit: int = 100, invalid_statuses=None, review_statuses=None):
    rows, _ = list_local_sources(status='inconsistent,error', page=1, page_size=limit)
    return rows
def load_share_waterline_candidates(target_active: int, statuses=None): return 0, []
def get_active_local_share_code_set(statuses=None): return set()
def get_p115_files_from_cache_tree(root_fid: str, max_depth: int = 6):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT *, name AS rel_path FROM p115_filesystem_cache WHERE parent_id=%s OR id=%s LIMIT 5000", (str(root_fid), str(root_fid)))
            return _rows(cur.fetchall())
