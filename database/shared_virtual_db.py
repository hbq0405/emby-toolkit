# database/shared_virtual_db.py
# 共享资源虚拟入库管理：本地虚拟项、贡献值快照与流水
import json
import logging
import re
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
                    COUNT(*) FILTER (WHERE status='promote_pending') AS promote_pending,
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


def _extract_virtual_id_from_text(value: str) -> str:
    text = str(value or '').strip()
    if not text:
        return ''
    # 支持虚拟 STRM 内容：etk-shared://<virtual_id> 或 etk-shared://play/<virtual_id>
    m = re.search(r'etk-shared://(?:play/)?([A-Za-z0-9_:\-]+)', text, re.IGNORECASE)
    if m:
        return m.group(1)
    # 支持 URL 参数：?virtual_id=xxx
    m = re.search(r'(?:virtual_id|vid)=([A-Za-z0-9_:\-]+)', text, re.IGNORECASE)
    if m:
        return m.group(1)
    return ''


def get_virtual_item_for_playback(emby_item_id: str = '', strm_path: str = '', media_source_id: str = ''):
    """反代播放入口使用：用 Emby ItemId / STRM 路径 / 虚拟协议定位虚拟入库记录。"""
    emby_item_id = str(emby_item_id or '').strip()
    strm_path = str(strm_path or '').strip()
    media_source_id = str(media_source_id or '').strip()

    virtual_id = _extract_virtual_id_from_text(strm_path)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if virtual_id:
                cur.execute(
                    "SELECT * FROM shared_virtual_items WHERE virtual_id=%s AND status NOT IN ('deleted','promoted','promote_pending') LIMIT 1",
                    (virtual_id,),
                )
                row = _row_to_dict(cur.fetchone())
                if row:
                    return row

            clauses = []
            args = []
            if strm_path:
                clauses.extend([
                    "strm_path = %s",
                    "raw_json->>'strm_path' = %s",
                    "raw_json->>'source_path' = %s",
                    "raw_json->>'emby_path' = %s",
                ])
                args.extend([strm_path, strm_path, strm_path, strm_path])
                base = strm_path.split('/')[-1].split('\\')[-1]
                if base:
                    clauses.append("file_name = %s")
                    args.append(base.replace('.strm', ''))
            if emby_item_id:
                clauses.extend([
                    "raw_json->>'emby_item_id' = %s",
                    "raw_json->>'emby_id' = %s",
                    "raw_json->>'item_id' = %s",
                ])
                args.extend([emby_item_id, emby_item_id, emby_item_id])
            if media_source_id:
                clauses.append("raw_json->>'media_source_id' = %s")
                args.append(media_source_id)

            if not clauses:
                return None

            cur.execute(
                f"""
                SELECT *
                FROM shared_virtual_items
                WHERE status NOT IN ('deleted','promoted','promote_pending')
                  AND ({' OR '.join(clauses)})
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                args,
            )
            return _row_to_dict(cur.fetchone())


def mark_virtual_transferring(virtual_id: str, message: str = ''):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE shared_virtual_items
                SET status='transferring', updated_at=NOW(), last_error=%s
                WHERE virtual_id=%s AND status <> 'deleted'
                RETURNING *
                """,
                (message, virtual_id),
            )
            row = _row_to_dict(cur.fetchone())
            conn.commit()
            return row


def mark_virtual_cached(
    virtual_id: str,
    real_fid: str = '',
    real_pick_code: str = '',
    real_parent_id: str = '',
    cache_parent_id: str = '',
    cache_parent_name: str = '',
    expires_at=None,
    message: str = '',
    raw_json: Dict[str, Any] = None,
):
    raw_json = raw_json or {}
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if expires_at:
                cur.execute(
                    """
                    UPDATE shared_virtual_items
                    SET status='cached', updated_at=NOW(),
                        first_transferred_at=COALESCE(first_transferred_at, NOW()),
                        last_transferred_at=NOW(), expires_at=%s,
                        real_fid=%s, real_pick_code=%s, real_parent_id=%s,
                        cache_parent_id=COALESCE(NULLIF(%s,''), cache_parent_id),
                        cache_parent_name=COALESCE(NULLIF(%s,''), cache_parent_name),
                        last_error=%s,
                        raw_json = COALESCE(raw_json, '{}'::jsonb) || %s::jsonb
                    WHERE virtual_id=%s
                    RETURNING *
                    """,
                    (expires_at, real_fid, real_pick_code, real_parent_id, cache_parent_id, cache_parent_name, message, _as_jsonb(raw_json), virtual_id),
                )
            else:
                cur.execute(
                    """
                    UPDATE shared_virtual_items
                    SET status='cached', updated_at=NOW(),
                        first_transferred_at=COALESCE(first_transferred_at, NOW()),
                        last_transferred_at=NOW(),
                        real_fid=%s, real_pick_code=%s, real_parent_id=%s,
                        cache_parent_id=COALESCE(NULLIF(%s,''), cache_parent_id),
                        cache_parent_name=COALESCE(NULLIF(%s,''), cache_parent_name),
                        last_error=%s,
                        raw_json = COALESCE(raw_json, '{}'::jsonb) || %s::jsonb
                    WHERE virtual_id=%s
                    RETURNING *
                    """,
                    (real_fid, real_pick_code, real_parent_id, cache_parent_id, cache_parent_name, message, _as_jsonb(raw_json), virtual_id),
                )
            row = _row_to_dict(cur.fetchone())
            conn.commit()
            return row


def mark_virtual_played(virtual_id: str):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE shared_virtual_items
                SET play_count=COALESCE(play_count,0)+1,
                    last_played_at=NOW(),
                    status=CASE WHEN status='cached' THEN 'watched' ELSE status END,
                    updated_at=NOW()
                WHERE virtual_id=%s
                RETURNING *
                """,
                (virtual_id,),
            )
            row = _row_to_dict(cur.fetchone())
            conn.commit()
            return row


def mark_virtual_error(virtual_id: str, message: str):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE shared_virtual_items
                SET status='error', last_error=%s, updated_at=NOW()
                WHERE virtual_id=%s
                RETURNING *
                """,
                (message, virtual_id),
            )
            row = _row_to_dict(cur.fetchone())
            conn.commit()
            return row


def mark_virtual_promote_pending(virtual_id: str, message: str = '', raw_json: Dict[str, Any] = None):
    """转正已提交但正式整理尚未完成。

    典型场景：未播放虚拟资源直接转存到“待整理”目录后，等待
    task_scan_and_organize_115 移动/重命名并生成正式 STRM。
    """
    raw_json = raw_json or {}
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE shared_virtual_items
                SET status='promote_pending', last_error=%s, updated_at=NOW(),
                    raw_json = COALESCE(raw_json, '{}'::jsonb) || %s::jsonb
                WHERE virtual_id=%s
                RETURNING *
                """,
                (message, _as_jsonb(raw_json), virtual_id),
            )
            row = _row_to_dict(cur.fetchone())
            conn.commit()
            return row


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




def _center_credit_event_label(reason: str) -> str:
    reason = str(reason or '')
    mapping = {
        'initial_credit': '设备注册基础贡献值',
        'source_registered': '成功分享视频，中心首次登记',
        'shared_source_served': '共享资源被其他设备成功转存',
        'shared_source_consumed': '从共享中心成功转存资源',
    }
    return mapping.get(reason, reason or '中心贡献值变化')


def _safe_int(value, default=0) -> int:
    try:
        if value is None or value == '':
            return default
        return int(value)
    except Exception:
        return default


def _extract_episode_code(item: Dict[str, Any]) -> str:
    """从中心流水附带的季/集字段或文件名里提取 SxxExx。

    中心 credit_ledger 关联 shared_sources 时会返回 season_number / episode_number，
    但旧数据或部分登记逻辑可能没有写 episode_number；此时再从 file_name/title
    里兜底识别 S01E02 这类命名，保证贡献值明细里剧集不再全挤成同一个剧名。
    """
    season = _safe_int(item.get('season_number'))
    episode = _safe_int(item.get('episode_number'))
    if season > 0 and episode > 0:
        return f"S{season:02d}E{episode:02d}"
    if season > 0:
        return f"S{season:02d}"

    text = ' '.join([
        str(item.get('title') or ''),
        str(item.get('file_name') or ''),
    ])
    match = re.search(r'(?i)\bS(\d{1,2})\s*[._ -]*E(\d{1,3})\b', text)
    if match:
        return f"S{int(match.group(1)):02d}E{int(match.group(2)):02d}"

    # 兜底识别中文/简单格式，例如 第1季第2集。
    match = re.search(r'第\s*(\d{1,2})\s*季.*?第\s*(\d{1,3})\s*[集话話]', text)
    if match:
        return f"S{int(match.group(1)):02d}E{int(match.group(2)):02d}"

    return ''


def _center_credit_display_title(item: Dict[str, Any]) -> str:
    base_title = str(item.get('title') or item.get('file_name') or item.get('ref_id') or '').strip()
    code = _extract_episode_code(item)
    if not code:
        return base_title
    if re.search(re.escape(code), base_title, re.IGNORECASE):
        return base_title
    return f"{base_title} {code}" if base_title else code


def sync_center_credit_ledger(items: List[Dict[str, Any]], device_snapshot: Dict[str, Any] = None) -> int:
    """同步中心服务器真实贡献值流水到本地展示表。

    本地 shared_credit_ledger_local 原本记录的是操作审计，delta 经常是 0；
    中心 credit_ledger 才是真正的积分变化来源。这里用 center_* 事件覆盖同步，
    避免每次刷新重复插入。
    """
    items = list(items or [])
    device_snapshot = device_snapshot or {}
    device_id = device_snapshot.get('device_id') or device_snapshot.get('id') or ''

    # 中心的基础 20 分是 devices.credit 默认值，不在 credit_ledger 里。
    # 为了让前端能解释“总分 = 基础分 + 贡献分”，本地展示时补一条虚拟流水。
    if device_id:
        items.append({
            'id': f'base:{device_id}',
            'device_id': device_id,
            'delta': 20,
            'reason': 'initial_credit',
            'ref_id': device_id,
            'source_id': '',
            'tmdb_id': '',
            'item_type': '',
            'title': '基础贡献值',
            'file_name': '',
            'created_at': device_snapshot.get('created_at') or device_snapshot.get('updated_at'),
        })

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # 只清理中心同步记录，保留本地操作审计记录。
            cur.execute("DELETE FROM shared_credit_ledger_local WHERE event_type LIKE 'center_%'")
            count = 0
            for item in items:
                reason_code = str(item.get('reason') or '')
                delta = int(item.get('delta') or 0)
                ref_id = str(item.get('ref_id') or item.get('source_id') or item.get('id') or '')
                title = item.get('title') or item.get('file_name') or ''
                file_name = item.get('file_name') or ''
                tmdb_id = item.get('tmdb_id') or ''
                item_type = item.get('item_type') or ''
                display = _center_credit_display_title(item) or title or file_name or ref_id
                label = _center_credit_event_label(reason_code)
                sign = '+' if delta > 0 else ''
                reason_text = f"{label}：{display}，贡献值 {sign}{delta}"
                if reason_code == 'initial_credit':
                    reason_text = f"{label}，贡献值 +20"
                created_at = item.get('created_at')

                if created_at:
                    cur.execute("""
                        INSERT INTO shared_credit_ledger_local(
                            event_type, delta, reason, ref_id, source_id, virtual_id,
                            tmdb_id, item_type, title, raw_json, created_at
                        ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)
                    """, (
                        f"center_{reason_code or 'credit'}", delta, reason_text, ref_id,
                        item.get('source_id') or ref_id, '', tmdb_id, item_type, display,
                        _as_jsonb({'origin': 'center', 'center_item': item}), created_at,
                    ))
                else:
                    cur.execute("""
                        INSERT INTO shared_credit_ledger_local(
                            event_type, delta, reason, ref_id, source_id, virtual_id,
                            tmdb_id, item_type, title, raw_json
                        ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                    """, (
                        f"center_{reason_code or 'credit'}", delta, reason_text, ref_id,
                        item.get('source_id') or ref_id, '', tmdb_id, item_type, display,
                        _as_jsonb({'origin': 'center', 'center_item': item}),
                    ))
                count += 1
        conn.commit()
    return count


def list_credit_ledger(limit=50, actual_only: bool = False):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            safe_limit = min(500, int(limit or 50))
            if actual_only:
                cur.execute(
                    """
                    SELECT *
                    FROM shared_credit_ledger_local
                    WHERE delta <> 0 OR event_type LIKE %s
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    ("center_%", safe_limit),
                )
            else:
                cur.execute(
                    """
                    SELECT *
                    FROM shared_credit_ledger_local
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (safe_limit,),
                )
            return [_row_to_dict(r) for r in cur.fetchall()]
