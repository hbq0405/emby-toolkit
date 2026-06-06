# database/shared_share_db.py
# Rapid v2 本地共享索引：不再创建 115 分享，只登记可秒传资源与 manifest。
import json
import re
import hashlib
from datetime import datetime
from typing import Any, Dict, List, Tuple

from database.connection import get_db_connection

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
                    season_number, episode_number, title, release_year, sha1, size, file_name, root_fid, root_name,
                    source_provider, status, center_status, manifest_hash, manifest_version, file_count, total_size,
                    is_clean_version, clean_version_confidence, clean_version_meta_json, media_signature_json,
                    rapid_meta_json, raw_json, reported_at, updated_at
                ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb,%s::jsonb,CASE WHEN %s IS NOT NULL THEN NOW() ELSE NULL END,NOW())
                ON CONFLICT(source_key)
                DO UPDATE SET
                    center_source_id=COALESCE(EXCLUDED.center_source_id, shared_rapid_sources.center_source_id),
                    title=COALESCE(EXCLUDED.title, shared_rapid_sources.title),
                    release_year=COALESCE(EXCLUDED.release_year, shared_rapid_sources.release_year),
                    sha1=COALESCE(EXCLUDED.sha1, shared_rapid_sources.sha1),
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
                    _safe_int(data.get('size'), 0) or None, data.get('file_name'), data.get('root_fid'), data.get('root_name'),
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
                        local_source_id, fid, pick_code, sha1, size, file_name, relative_path,
                        tmdb_id, item_type, season_number, episode_number, center_file_id,
                        raw_ffprobe_uploaded, media_signature_json, rapid_meta_json, raw_json
                    ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb)
                    """,
                    (
                        local_source_id, item.get('fid') or item.get('file_id'), item.get('pick_code') or item.get('pickcode') or item.get('pc'),
                        sha1, _safe_int(item.get('size'), 0), item.get('file_name') or item.get('name') or '', item.get('relative_path') or '',
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
        'status', 'center_status', 'center_source_id', 'last_error', 'manifest_hash', 'manifest_version',
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


def list_local_sources(status='all', keyword='', page=1, page_size=30, order_by='created_desc') -> Tuple[List[Dict[str, Any]], int]:
    page = max(1, int(page or 1))
    page_size = min(500, max(1, int(page_size or 30)))
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
    where = ["in_library=TRUE", "item_type IN ('Movie','Series','Season','Episode')"]
    args = []
    if keyword:
        kw = f"%{keyword}%"
        where.append('(title ILIKE %s OR tmdb_id ILIKE %s OR parent_series_tmdb_id ILIKE %s)')
        args.extend([kw, kw, kw])
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT * FROM media_metadata
                WHERE {' AND '.join(where)}
                ORDER BY CASE item_type WHEN 'Movie' THEN 0 WHEN 'Series' THEN 1 WHEN 'Season' THEN 2 ELSE 3 END,
                         COALESCE(date_added, created_at, last_updated_at) DESC NULLS LAST
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
                WHERE item_type='Episode' AND in_library=TRUE
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
            'pick_code': f.get('pick_code') or '',
            'sha1': sha1,
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


def build_shareable_candidate(row: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(row or {})
    item_type = str(row.get('item_type') or '')
    if item_type == 'Episode':
        tmdb_id = str(row.get('parent_series_tmdb_id') or row.get('tmdb_id') or '')
    else:
        tmdb_id = str(row.get('tmdb_id') or '')
    return {
        'tmdb_id': tmdb_id,
        'item_type': item_type,
        'parent_series_tmdb_id': row.get('parent_series_tmdb_id'),
        'season_number': row.get('season_number'),
        'episode_number': row.get('episode_number'),
        'title': row.get('title') or row.get('original_title') or tmdb_id,
        'release_year': row.get('release_year'),
        'root_fid': '',
        'root_name': row.get('title') or row.get('original_title') or tmdb_id,
        'source_provider': 'manual_rapid',
        'raw_json': row,
    }


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
                key = (row.get('tmdb_id'), 'Season', sn)
                if key in seen:
                    continue
                seen.add(key)
                cand = build_shareable_candidate({**row, 'item_type': 'Season', 'season_number': sn, 'parent_series_tmdb_id': row.get('tmdb_id')})
                cand['title'] = f"{row.get('title') or row.get('tmdb_id')} S{int(sn):02d}"
                result.append(cand)
        else:
            cand = build_shareable_candidate(row)
            key = (cand.get('tmdb_id'), cand.get('item_type'), cand.get('season_number'), cand.get('episode_number'))
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


def all_library_share_candidates(limit: int = 100000) -> List[Dict[str, Any]]:
    rows = _media_rows_for_search('', limit)
    result = []
    seen = set()
    for row in rows:
        item_type = str(row.get('item_type') or '')
        if item_type == 'Movie':
            cand = build_shareable_candidate(row)
            key = ('Movie', cand.get('tmdb_id'))
            if key not in seen:
                seen.add(key); result.append(cand)
        elif item_type == 'Episode':
            # 追更池按分集共享，谁先有谁服务。
            cand = build_shareable_candidate(row)
            key = ('Episode', cand.get('tmdb_id'), cand.get('season_number'), cand.get('episode_number'), tuple(_as_array(row.get('file_sha1_json'))))
            if key not in seen:
                seen.add(key); result.append(cand)
        elif item_type == 'Season':
            cand = build_shareable_candidate(row)
            key = ('Season', cand.get('tmdb_id'), cand.get('season_number'))
            if key not in seen:
                seen.add(key); result.append(cand)
    return result


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
