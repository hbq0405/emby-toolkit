# tasks/shared_resource_tasks.py
# 共享资源自动维护任务：缺口登记、分享审核同步、中心登记、失效清理、中心缺口自动分享。
import json
import logging
import time
from typing import Dict, Any, List

import config_manager
import constants
import task_manager
from database import shared_share_db, shared_virtual_db
from database.connection import get_db_connection
from handler.p115_service import P115Service
from handler.shared_center_client import SharedCenterClient, shared_center_enabled

logger = logging.getLogger(__name__)


def _cfg(name: str, fallback: str, default=None):
    key = getattr(constants, name, fallback)
    return (config_manager.APP_CONFIG or {}).get(key, default)


def _enabled() -> bool:
    return shared_center_enabled()


def _safe_int(v, default=0):
    try:
        if v in (None, ''):
            return default
        return int(float(v))
    except Exception:
        return default


def _gap_item(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'tmdb_id': str(row.get('parent_series_tmdb_id') or row.get('tmdb_id') or ''),
        'item_type': row.get('item_type') or '',
        'season_number': row.get('season_number'),
        'episode_number': row.get('episode_number'),
        'title': row.get('title') or None,
        'release_year': row.get('release_year'),
    }


def _report_local_wanted_gaps(client: SharedCenterClient, limit: int = 200) -> int:
    """把本机订阅/想看但尚未入库的项目登记到中心缺口池。"""
    rows = []
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT tmdb_id, item_type, parent_series_tmdb_id, season_number, episode_number,
                       title, release_year
                FROM media_metadata
                WHERE COALESCE(in_library, FALSE) = FALSE
                  AND item_type IN ('Movie','Series','Season','Episode')
                  AND subscription_status IN ('WANTED','REQUESTED','SUBSCRIBED','PENDING_RELEASE')
                ORDER BY last_updated_at DESC NULLS LAST, created_at DESC NULLS LAST
                LIMIT %s
                """,
                (int(limit),),
            )
            rows = [dict(r) for r in cur.fetchall()]

    items = [_gap_item(r) for r in rows]
    items = [x for x in items if x.get('tmdb_id') and x.get('item_type')]
    if not items:
        return 0
    try:
        resp = client.report_gaps(items)
        return int(resp.get('count') or len(items))
    except Exception as e:
        logger.warning(f"  ➜ [共享资源维护] 自动登记缺口失败: {e}")
        return 0


def _parse_share_ok(resp: Dict[str, Any]) -> bool:
    if not isinstance(resp, dict):
        return False
    if resp.get('state') is True or str(resp.get('state')).lower() in ('1', 'true'):
        return True
    if resp.get('errno') in (0, '0') or resp.get('code') in (0, '0', 200, '200'):
        return True
    return False


def _share_resp_text(resp: Any) -> str:
    try:
        return json.dumps(resp, ensure_ascii=False).lower()
    except Exception:
        return str(resp or '').lower()


def _looks_share_blocked(resp: Any) -> bool:
    """115 审核违规/风控类状态。

    这类分享不能当作普通失效处理；普通失效可重建，但违规分享如果被维护任务
    反复重建，就会出现同一资源被自动分享多次。
    """
    text = _share_resp_text(resp)
    return any(k in text for k in (
        '违规', '违法', '侵权', '敏感', '禁止分享', '禁止访问',
        '审核失败', '审核不通过', '分享状态异常',
        'violation', 'illegal', 'blocked', 'forbidden', 'risk'
    ))


def _looks_share_alive(resp: Dict[str, Any]) -> bool:
    if not _parse_share_ok(resp):
        return False
    text = _share_resp_text(resp)
    if _looks_share_blocked(resp):
        return False
    return not any(k in text for k in ['已取消', '已失效', '不存在', '取消分享', 'expired', 'cancelled', 'not found'])


def _extract_115_history_items(resp: Any) -> List[Dict[str, Any]]:
    """兼容不同 115 最近接收列表返回结构。"""
    if not isinstance(resp, dict):
        return []
    candidates = []
    data = resp.get('data')
    if isinstance(data, list):
        candidates = data
    elif isinstance(data, dict):
        for key in ('list', 'items', 'data', 'rows'):
            if isinstance(data.get(key), list):
                candidates = data.get(key)
                break
    if not candidates:
        for key in ('list', 'items', 'rows'):
            if isinstance(resp.get(key), list):
                candidates = resp.get(key)
                break
    return [x for x in candidates if isinstance(x, dict)]


def _history_item_id(row: Dict[str, Any]) -> str:
    for key in ('id', 'hid', 'history_id', 'record_id'):
        val = row.get(key)
        if val not in (None, ''):
            return str(val).strip()
    return ''


def _cleanup_recent_receive_history(p115, virtual_rows: List[Dict[str, Any]], max_pages: int = 3, page_size: int = 100) -> int:
    """清理 115 最近接收记录中与已释放虚拟缓存相关的条目。

    注意：history/delete 删除的是“历史记录/最近接收”展示项，不保证解除 115
    后端对 share_import 的 4100024 已转存限制，所以这里是 best-effort。
    """
    if not p115 or not hasattr(p115, 'history_receive_list') or not hasattr(p115, 'history_delete'):
        return 0

    terms = set()
    share_codes = set()
    for row in virtual_rows or []:
        share_code = str(row.get('share_code') or '').strip().lower()
        if share_code:
            share_codes.add(share_code)
            terms.add(share_code)
        raw = row.get('raw_json') if isinstance(row.get('raw_json'), dict) else {}
        for val in (
            row.get('title'),
            row.get('file_name'),
            raw.get('last_import_root_name') if isinstance(raw, dict) else '',
        ):
            val = str(val or '').strip().lower()
            if not val:
                continue
            terms.add(val)
            stem = val.rsplit('.', 1)[0] if '.' in val else val
            if len(stem) >= 4:
                terms.add(stem)

    # 太短的词容易误删历史记录。
    terms = {t for t in terms if len(t) >= 4}
    if not terms:
        return 0

    ids = []
    seen_ids = set()
    for page in range(max(1, int(max_pages or 1))):
        try:
            resp = p115.history_receive_list(offset=page * page_size, limit=page_size)
            items = _extract_115_history_items(resp)
        except Exception as e:
            logger.warning(f"  ➜ [共享资源维护] 查询 115 最近接收记录失败: {e}")
            break
        if not items:
            break
        for item in items:
            hid = _history_item_id(item)
            if not hid or hid in seen_ids:
                continue
            text = json.dumps(item, ensure_ascii=False).lower()
            if any(code and code in text for code in share_codes) or any(term in text for term in terms):
                seen_ids.add(hid)
                ids.append(hid)
        if len(items) < page_size:
            break

    deleted = 0
    for i in range(0, len(ids), 50):
        batch = ids[i:i + 50]
        try:
            resp = p115.history_delete(batch)
            if isinstance(resp, dict) and (resp.get('state') or resp.get('success')):
                deleted += len(batch)
            else:
                logger.warning(f"  ➜ [共享资源维护] 删除 115 最近接收记录未确认成功: {resp}")
        except Exception as e:
            logger.warning(f"  ➜ [共享资源维护] 删除 115 最近接收记录失败: {e}")
    return deleted


def _record_reportable(record: Dict[str, Any]) -> bool:
    return (record.get('status') in ('alive', 'reported') or record.get('review_status') == 'alive') and record.get('center_status') not in ('reported', 'partial')


def _auto_check_and_report_local_shares(client: SharedCenterClient, max_records: int = 80) -> Dict[str, int]:
    """自动同步 115 分享状态；可用后上传 raw 并登记中心；失效时撤销中心源。"""
    p115 = P115Service.get_client()
    if not p115:
        logger.warning("  ➜ [共享资源维护] 115 客户端未初始化，跳过分享状态同步。")
        return {'checked': 0, 'reported': 0, 'cancelled': 0}

    records, _ = shared_share_db.list_share_records(status='all', keyword='', page=1, page_size=max_records)
    checked = reported = cancelled = 0

    # 延迟导入 routes.shared_resource，复用现有检查/上传/登记逻辑，避免两套实现分叉。
    try:
        from routes import shared_resource as sr
    except Exception as e:
        logger.warning(f"  ➜ [共享资源维护] 无法加载共享资源路由辅助函数: {e}")
        sr = None

    for record in records:
        status = str(record.get('status') or '')
        if status in ('cancelled', 'deleted', 'dead', 'blocked', 'violation'):
            continue
        share_code = str(record.get('share_code') or '').strip()
        if not share_code:
            continue
        try:
            snap = p115.share_info(share_code, record.get('receive_code'), cid=0, limit=1)
            checked += 1
            alive = _looks_share_alive(snap)
            if alive:
                update = {'status': 'alive', 'review_status': 'alive', 'last_checked_at': 'NOW()', 'last_error': '分享可用', 'raw_json': {'last_snap': snap}}
                shared_share_db.update_share_record(record['id'], **update)
                record = shared_share_db.get_share_record(record['id']) or record
                if _record_reportable(record) and sr is not None:
                    # 自动补 raw + 登记中心。
                    try:
                        cfg, headers = sr._center_headers()
                        sr._upload_share_raw_ffprobe_to_center(record['id'], cfg, headers, force=False)
                    except Exception as e:
                        logger.debug(f"  ➜ [共享资源维护] 自动上传 raw 失败，继续尝试登记中心: {e}")
                    try:
                        # 直接复用 route 的核心注册逻辑不方便调用带 Flask request 的视图，这里手动按 shared_share_items 注册。
                        items = shared_share_db.list_share_items(record['id'])
                        ok = 0
                        for item in items:
                            sha1 = str(item.get('sha1') or '').strip().upper()
                            if not sha1:
                                continue
                            is_season_pack = str(record.get('share_type') or '').lower() in ('season_pack', 'season', 'tv_pack') or (record.get('root_is_dir') and str(record.get('item_type') or '').lower() in ('season', 'series', 'tv'))
                            resp = client.register_source(
                                tmdb_id=item.get('tmdb_id') or record.get('tmdb_id'),
                                item_type='Season' if is_season_pack else (item.get('item_type') or record.get('item_type') or 'Movie'),
                                season_number=item.get('season_number') or record.get('season_number'),
                                episode_number=None if is_season_pack else item.get('episode_number'),
                                title=record.get('title') or item.get('file_name'),
                                release_year=record.get('release_year'),
                                sha1=sha1,
                                size=_safe_int(item.get('size'), 0),
                                file_name=item.get('file_name') or '',
                                quality='',
                                source_provider='auto_gap_share' if ((record.get('raw_json') or {}).get('auto_gap')) else 'user_share',
                                share_code=record.get('share_code'),
                                receive_code=record.get('receive_code') or '',
                                has_raw_ffprobe=bool(item.get('raw_ffprobe_uploaded')),
                            )
                            if resp.get('source_id'):
                                shared_share_db.mark_item_reported(item['id'], resp.get('source_id'))
                                ok += 1
                        if ok:
                            shared_share_db.update_share_record(record['id'], center_status='reported', status='reported', reported_count=ok, reported_at='NOW()', last_error='自动登记中心成功')
                            reported += 1
                    except Exception as e:
                        logger.warning(f"  ➜ [共享资源维护] 自动登记中心失败: share={share_code}, err={e}")
            else:
                sha1s = [i.get('sha1') for i in (shared_share_db.list_share_items(record['id']) or []) if i.get('sha1')]
                if _looks_share_blocked(snap):
                    # 115 审核违规/风控：熔断这个自动分享，不再把它当普通 dead 缺口反复重建。
                    try:
                        client.cancel_sources(share_code=share_code, sha1_list=sha1s, reason='auto_share_violation', delete_raw_ffprobe=True)
                    except Exception as e:
                        logger.debug(f"  ➜ [共享资源维护] 撤销违规中心源失败: {e}")
                    shared_share_db.update_share_record(
                        record['id'],
                        status='blocked',
                        review_status='violation',
                        center_status='cancelled',
                        last_checked_at='NOW()',
                        cancelled_at='NOW()',
                        last_error='115 审核违规/风控，已熔断自动补缺分享，避免重复创建分享',
                        raw_json={'last_snap': snap, 'auto_share_blocked': True},
                    )
                    cancelled += 1
                else:
                    # 115 分享已不可用，撤销中心源并本地标记。
                    try:
                        client.cancel_sources(share_code=share_code, sha1_list=sha1s, reason='auto_share_dead', delete_raw_ffprobe=True)
                    except Exception as e:
                        logger.debug(f"  ➜ [共享资源维护] 撤销中心源失败: {e}")
                    shared_share_db.update_share_record(record['id'], status='dead', review_status='dead', center_status='cancelled', last_checked_at='NOW()', cancelled_at='NOW()', last_error='自动检测到115分享失效，已撤销中心源')
                    cancelled += 1
        except Exception as e:
            logger.warning(f"  ➜ [共享资源维护] 同步分享状态异常: share={share_code}, err={e}")
        time.sleep(0.2)

    return {'checked': checked, 'reported': reported, 'cancelled': cancelled}


def _find_local_media_for_gap(gap: Dict[str, Any]) -> Dict[str, Any] | None:
    """中心缺口命中本地媒体库后返回 media_metadata 行。"""
    tmdb_id = str(gap.get('tmdb_id') or '').strip()
    item_type = str(gap.get('item_type') or '').strip()
    season = gap.get('season_number')
    episode = gap.get('episode_number')
    if not tmdb_id or not item_type:
        return None
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if item_type == 'Movie':
                cur.execute("SELECT * FROM media_metadata WHERE item_type='Movie' AND tmdb_id=%s AND in_library=TRUE LIMIT 1", (tmdb_id,))
            elif item_type in ('Season', 'Series'):
                if season not in (None, ''):
                    cur.execute(
                        """
                        SELECT * FROM media_metadata
                        WHERE item_type='Season' AND parent_series_tmdb_id=%s AND season_number=%s AND in_library=TRUE
                        LIMIT 1
                        """,
                        (tmdb_id, int(season)),
                    )
                else:
                    cur.execute("SELECT * FROM media_metadata WHERE item_type='Series' AND tmdb_id=%s AND in_library=TRUE LIMIT 1", (tmdb_id,))
            elif item_type == 'Episode':
                cur.execute(
                    """
                    SELECT * FROM media_metadata
                    WHERE item_type='Episode'
                      AND (parent_series_tmdb_id=%s OR tmdb_id=%s)
                      AND COALESCE(season_number, -1)=COALESCE(%s, -1)
                      AND COALESCE(episode_number, -1)=COALESCE(%s, -1)
                      AND in_library=TRUE
                    LIMIT 1
                    """,
                    (tmdb_id, tmdb_id, int(season or -1), int(episode or -1)),
                )
            else:
                return None
            row = cur.fetchone()
            return dict(row) if row else None


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


def _has_existing_share_for_gap(gap: Dict[str, Any], candidate: Dict[str, Any] | None = None, files: List[Dict[str, Any]] | None = None) -> bool:
    """判断中心缺口是否已经有本机分享在处理。返回 True 就绝对不再自动 share_create。

    旧逻辑只按 tmdb_id + item_type + season_number 粗略判断，遇到单集缺口时容易因为
    gap tmdb / candidate tmdb / parent_series_tmdb_id 口径不一致而漏判，维护任务就会每轮
    都创建一个新 115 分享。这里改为三层去重：root_fid、sha1、媒体口径。
    """
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

    # blocked/violation 也必须算“已有处理记录”，这样违规资源不会被下一轮自动重建。
    active_statuses = (
        'pending_review', 'alive', 'reported', 'partial', 'not_reported',
        'blocked', 'violation', 'review_failed'
    )

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
                    (root_fid, list(active_statuses)),
                )
                if cur.fetchone() is not None:
                    return True

            if sha1s:
                cur.execute(
                    """
                    SELECT 1
                    FROM shared_share_records r
                    JOIN shared_share_items i ON i.record_id = r.id
                    WHERE UPPER(COALESCE(i.sha1, '')) = ANY(%s)
                      AND r.status = ANY(%s)
                    LIMIT 1
                    """,
                    (sha1s, list(active_statuses)),
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
                    LEFT JOIN shared_share_items i ON i.record_id = r.id
                    WHERE (r.tmdb_id = ANY(%s) OR i.tmdb_id = ANY(%s))
                      AND (r.item_type IN ('Movie','movie','movie_file','movie_folder') OR i.item_type IN ('Movie','movie','movie_file'))
                      AND r.status = ANY(%s)
                    LIMIT 1
                    """,
                    (tmdb_ids, tmdb_ids, list(active_statuses)),
                )
                return cur.fetchone() is not None

            if item_type == 'Episode':
                cur.execute(
                    """
                    SELECT 1
                    FROM shared_share_records r
                    LEFT JOIN shared_share_items i ON i.record_id = r.id
                    WHERE (r.tmdb_id = ANY(%s) OR r.parent_series_tmdb_id = ANY(%s) OR i.tmdb_id = ANY(%s))
                      AND COALESCE(i.season_number, r.season_number, -1)=COALESCE(%s, -1)
                      AND COALESCE(i.episode_number, -1)=COALESCE(%s, -1)
                      AND r.status = ANY(%s)
                    LIMIT 1
                    """,
                    (tmdb_ids, tmdb_ids, tmdb_ids, season, episode, list(active_statuses)),
                )
                return cur.fetchone() is not None

            # Season / Series 都按“剧集包”处理，精确到季。
            cur.execute(
                """
                SELECT 1
                FROM shared_share_records r
                LEFT JOIN shared_share_items i ON i.record_id = r.id
                WHERE (r.tmdb_id = ANY(%s) OR r.parent_series_tmdb_id = ANY(%s) OR i.tmdb_id = ANY(%s))
                  AND COALESCE(i.season_number, r.season_number, -1)=COALESCE(%s, -1)
                  AND r.status = ANY(%s)
                LIMIT 1
                """,
                (tmdb_ids, tmdb_ids, tmdb_ids, season, list(active_statuses)),
            )
            return cur.fetchone() is not None



def _auto_share_center_open_gaps(client: SharedCenterClient, limit: int = 80) -> int:
    """中心有缺口而本机已入库时，自动创建 115 分享。可用后由下一轮维护自动登记中心。"""
    try:
        gaps = (client.list_open_gaps(limit=limit).get('items') or [])
    except Exception as e:
        logger.warning(f"  ➜ [共享资源维护] 拉取中心缺口失败: {e}")
        return 0
    if not gaps:
        return 0

    p115 = P115Service.get_client()
    if not p115:
        return 0

    try:
        from routes import shared_resource as sr
    except Exception as e:
        logger.warning(f"  ➜ [共享资源维护] 无法加载分享辅助函数，跳过自动分享缺口: {e}")
        return 0

    created = 0
    for gap in gaps:
        try:
            if _has_existing_share_for_gap(gap):
                continue
            row = _find_local_media_for_gap(gap)
            if not row:
                continue
            candidate = sr._build_media_candidate(row)
            if not candidate.get('resolvable') or not candidate.get('root_fid'):
                continue
            if _has_existing_share_for_gap(gap, candidate=candidate):
                continue

            # 遵守已有季包分享策略，避免未完结季整包分享。
            if candidate.get('share_type') == 'season_pack':
                policy = sr._share_policy_for_media({
                    'item_type': 'Season',
                    'tmdb_id': candidate.get('parent_series_tmdb_id') or candidate.get('tmdb_id'),
                    'parent_series_tmdb_id': candidate.get('parent_series_tmdb_id') or candidate.get('tmdb_id'),
                    'season_number': candidate.get('season_number'),
                })
                if not policy.get('allowed'):
                    continue

            share_resp = p115.share_create([str(candidate.get('root_fid'))], share_duration=-1, receive_code=None)
            if not share_resp or not share_resp.get('state'):
                logger.warning(f"  ➜ [共享资源维护] 自动创建分享失败: {candidate.get('display_title')} -> {share_resp}")
                continue
            data = share_resp.get('data') or {}
            share_code = data.get('share_code') or share_resp.get('share_code')
            receive_code = data.get('receive_code') or ''
            share_url = data.get('share_url') or (f"https://115.com/s/{share_code}" if share_code else '')
            root_fid = str(candidate.get('root_fid'))
            root_name = candidate.get('root_name') or candidate.get('title') or root_fid
            root_is_dir = candidate.get('root_is_dir') is not False

            files = sr._collect_files_from_115(p115, root_fid, root_name=root_name, max_depth=6, assume_dir=root_is_dir)
            if not files:
                payload = {
                    'tmdb_id': candidate.get('share_tmdb_id') or candidate.get('tmdb_id'),
                    'item_type': candidate.get('share_item_type') or candidate.get('item_type'),
                    'parent_series_tmdb_id': candidate.get('parent_series_tmdb_id'),
                    'season_number': candidate.get('season_number'),
                    'episode_number': candidate.get('episode_number'),
                    'title': candidate.get('display_title') or candidate.get('title'),
                    'root_name': root_name,
                }
                files = sr._collect_files_from_media_payload(payload)
            for item in files:
                item.setdefault('tmdb_id', str(candidate.get('share_tmdb_id') or candidate.get('tmdb_id') or ''))
                item.setdefault('item_type', 'Episode' if candidate.get('share_type') in ('season_pack','series_pack') and item.get('episode_number') else candidate.get('share_item_type') or candidate.get('item_type'))
                item.setdefault('season_number', candidate.get('season_number'))
                item.setdefault('episode_number', candidate.get('episode_number'))
            record = shared_share_db.create_share_record({
                'share_code': share_code,
                'receive_code': receive_code,
                'share_url': share_url,
                'share_type': candidate.get('share_type') or 'movie_folder',
                'root_fid': root_fid,
                'root_name': root_name,
                'root_is_dir': root_is_dir,
                'tmdb_id': str(candidate.get('share_tmdb_id') or candidate.get('tmdb_id') or ''),
                'item_type': candidate.get('share_item_type') or candidate.get('item_type') or 'Movie',
                'parent_series_tmdb_id': candidate.get('parent_series_tmdb_id'),
                'season_number': candidate.get('season_number'),
                'title': candidate.get('display_title') or candidate.get('title') or root_name,
                'release_year': candidate.get('release_year'),
                'status': 'pending_review',
                'review_status': 'pending_review',
                'center_status': 'not_reported',
                'raw_json': {'auto_gap': gap, 'share_response': share_resp, 'candidate': candidate},
            })
            shared_share_db.replace_share_items(record['id'], files)
            shared_virtual_db.add_credit_ledger('share_auto_created_for_gap', 0, '命中中心缺口并自动创建115分享，等待审核', ref_id=str(record['id']), title=record.get('title') or '', raw_json={'gap': gap, 'share_code': share_code})
            created += 1
        except Exception as e:
            logger.warning(f"  ➜ [共享资源维护] 自动分享中心缺口失败: {gap} -> {e}", exc_info=True)
        time.sleep(0.3)
    return created



def _cleanup_expired_virtual_cache(max_rows: int = 80) -> int:
    """删除已过期的虚拟入库临时转存文件，但保留虚拟 STRM/记录，后续播放可再次临时转存。"""
    p115 = P115Service.get_client()
    if not p115:
        logger.warning("  ➜ [共享资源维护] 115 客户端未初始化，跳过过期临时转存清理。")
        return 0

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT virtual_id, title, file_name, share_code, raw_json, real_fid, real_pick_code, real_parent_id, expires_at
                FROM shared_virtual_items
                WHERE status IN ('cached','watched')
                  AND COALESCE(real_fid, '') <> ''
                  AND expires_at IS NOT NULL
                  AND expires_at < NOW()
                ORDER BY expires_at ASC
                LIMIT %s
                """,
                (int(max_rows),),
            )
            rows = [dict(r) for r in cur.fetchall()]

    cleaned = 0
    cleaned_rows_for_history = []
    for row in rows:
        virtual_id = str(row.get('virtual_id') or '').strip()
        real_fid = str(row.get('real_fid') or '').strip()
        title = row.get('title') or row.get('file_name') or virtual_id
        if not virtual_id or not real_fid:
            continue

        resp = None
        delete_ok = False
        try:
            resp = p115.fs_delete([real_fid])
            text = json.dumps(resp, ensure_ascii=False) if isinstance(resp, dict) else str(resp)
            delete_ok = bool(isinstance(resp, dict) and resp.get('state')) or any(k in text for k in ['不存在', '已删除', 'not found', 'delete success'])
        except Exception as e:
            text = str(e)
            delete_ok = any(k in text for k in ['不存在', 'not found'])
            logger.debug(f"  ➜ [共享资源维护] 删除过期临时转存异常: {virtual_id}/{real_fid} -> {e}")

        # 无论远端是否已经被手动删除，只要确认过期，就清空本地 real_*，防止继续使用过期 pickcode。
        if delete_ok or resp is not None:
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE shared_virtual_items
                            SET status='virtual_ready',
                                real_fid='', real_pick_code='', real_parent_id='', expires_at=NULL,
                                last_error=%s,
                                raw_json = COALESCE(raw_json, '{}'::jsonb) || %s::jsonb,
                                updated_at=NOW()
                            WHERE virtual_id=%s
                            """,
                            (
                                '临时转存已过期，维护任务已清理；下次播放将重新转存',
                                json.dumps({'expired_cache_cleaned_at': time.strftime('%Y-%m-%d %H:%M:%S'), 'real_fid': real_fid, 'delete_response': resp}, ensure_ascii=False),
                                virtual_id,
                            ),
                        )
                        cur.execute("DELETE FROM p115_filesystem_cache WHERE id=%s", (real_fid,))
                    conn.commit()
                shared_virtual_db.add_credit_ledger(
                    'virtual_cache_expired_cleaned', 0,
                    f'清理过期虚拟临时转存：{title}',
                    ref_id=virtual_id,
                    virtual_id=virtual_id,
                    title=title,
                    raw_json={'real_fid': real_fid, 'delete_response': resp},
                )
                cleaned += 1
                cleaned_rows_for_history.append(row)
            except Exception as e:
                logger.warning(f"  ➜ [共享资源维护] 清空虚拟临时转存状态失败: {virtual_id} -> {e}")
        time.sleep(0.15)

    if cleaned_rows_for_history:
        try:
            _cleanup_recent_receive_history(p115, cleaned_rows_for_history)
        except Exception as e:
            logger.warning(f"  ➜ [共享资源维护] 清理 115 最近接收记录异常: {e}")

    if cleaned:
        logger.info(f"  ➜ [共享资源维护] 已清理 {cleaned} 个过期虚拟临时转存文件。")
    return cleaned


def _watching_missing_episodes(limit: int = 120) -> List[Dict[str, Any]]:
    """查询正在追更/暂停追更季下尚未入库的分集。"""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH watch_seasons AS (
                    SELECT
                        tmdb_id AS season_tmdb_id,
                        parent_series_tmdb_id,
                        season_number,
                        title AS season_title,
                        release_year,
                        watching_status,
                        last_updated_at
                    FROM media_metadata
                    WHERE item_type='Season'
                      AND watching_status IN ('Watching','Paused')
                      AND parent_series_tmdb_id IS NOT NULL
                      AND season_number IS NOT NULL
                )
                SELECT
                    e.tmdb_id,
                    e.item_type,
                    e.parent_series_tmdb_id,
                    e.season_number,
                    e.episode_number,
                    e.title,
                    e.release_year,
                    e.release_date,
                    ws.season_tmdb_id,
                    ws.season_title,
                    ws.watching_status
                FROM media_metadata e
                JOIN watch_seasons ws
                  ON e.item_type='Episode'
                 AND e.parent_series_tmdb_id = ws.parent_series_tmdb_id
                 AND e.season_number = ws.season_number
                WHERE COALESCE(e.in_library, FALSE) = FALSE
                  AND e.episode_number IS NOT NULL
                  AND COALESCE(e.subscription_status, 'NONE') NOT IN ('IGNORED')
                  AND (e.release_date IS NULL OR e.release_date <= CURRENT_DATE)
                ORDER BY ws.last_updated_at DESC NULLS LAST,
                         e.parent_series_tmdb_id, e.season_number, e.episode_number
                LIMIT %s
                """,
                (int(limit),),
            )
            return [dict(r) for r in cur.fetchall()]


def _has_local_virtual_projection_for_episode(row: Dict[str, Any]) -> bool:
    """避免维护任务反复为同一缺失分集创建虚拟 STRM。"""
    parent = str(row.get('parent_series_tmdb_id') or '')
    season = _safe_int(row.get('season_number'), -1)
    episode = _safe_int(row.get('episode_number'), -1)
    if not parent or season < 0 or episode < 0:
        return False
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1
                    FROM shared_virtual_items
                    WHERE status NOT IN ('deleted','promoted')
                      AND (parent_series_tmdb_id=%s OR tmdb_id=%s)
                      AND COALESCE(season_number, -1)=COALESCE(%s, -1)
                      AND COALESCE(episode_number, -1)=COALESCE(%s, -1)
                    LIMIT 1
                    """,
                    (parent, parent, season, episode),
                )
                return cur.fetchone() is not None
    except Exception:
        return False


def _auto_follow_watching_series_from_center(max_items: int = 80) -> Dict[str, int]:
    """把 Watching / Paused 季的缺失分集纳入共享中心消费链路。"""
    if not _enabled():
        return {'missing': 0, 'consumed': 0, 'gaps': 0, 'skipped': 0}

    try:
        from handler.shared_center_client import shared_resource_mode
        from handler.shared_subscription_service import try_consume_shared_resource
    except Exception as e:
        logger.warning(f"  ➜ [共享资源维护] 无法加载共享消费入口，跳过剧集追更: {e}")
        return {'missing': 0, 'consumed': 0, 'gaps': 0, 'skipped': 0}

    rows = _watching_missing_episodes(limit=max_items)
    consumed = gaps = skipped = 0
    mode = shared_resource_mode()

    for row in rows:
        try:
            if mode == 'virtual' and _has_local_virtual_projection_for_episode(row):
                skipped += 1
                continue

            parent_tmdb = row.get('parent_series_tmdb_id')
            title = row.get('title') or row.get('season_title') or f"S{_safe_int(row.get('season_number'), 1):02d}E{_safe_int(row.get('episode_number'), 0):02d}"
            result = try_consume_shared_resource(
                row,
                title=title,
                tmdb_id=row.get('tmdb_id') or parent_tmdb,
                item_type='Episode',
                parent_tmdb_id=parent_tmdb,
                season_number=row.get('season_number'),
                year=row.get('release_year') or '',
            )
            if result.get('success'):
                consumed += 1
                logger.info(
                    "  ➜ [共享资源维护] 追更缺集命中中心资源并已%s：%s S%02dE%02d",
                    '虚拟入库' if result.get('mode') == 'virtual' else '永久转存',
                    row.get('season_title') or parent_tmdb,
                    _safe_int(row.get('season_number'), 0),
                    _safe_int(row.get('episode_number'), 0),
                )
            elif result.get('reported_gap'):
                gaps += 1
        except Exception as e:
            logger.warning(f"  ➜ [共享资源维护] 剧集追更共享消费失败: {row} -> {e}", exc_info=True)
        time.sleep(0.2)

    return {'missing': len(rows), 'consumed': consumed, 'gaps': gaps, 'skipped': skipped}

def task_shared_resource_maintenance(processor=None, maintenance_silent: bool = False):
    """共享资源维护总任务。可由前端手动触发，也由调度器硬编码定时执行。

    maintenance_silent=True 时用于调度器后台静默执行：不输出成功/进度/摘要日志；
    未捕获异常仍会由 task_manager 以 ERROR 记录。
    """
    old_logger_level = None
    if maintenance_silent:
        # 调度器静默运行时，压制本模块的 INFO/WARNING 成功与进度日志；
        # 真正未捕获的异常仍会由 task_manager 以 ERROR 记录。
        old_logger_level = logger.level
        logger.setLevel(logging.ERROR)

    def _status(progress: int, message: str):
        # 静默调度时仍允许后台状态更新，但不依赖实时日志展示。
        task_manager.update_status_from_thread(progress, message)

    try:
        _status(0, '正在初始化共享资源维护任务...')
        if not _enabled():
            _status(100, '共享资源未启用，跳过。')
            return
        client = SharedCenterClient()
        if not client.ready:
            _status(100, '共享中心地址或 device_token 未配置，跳过。')
            return

        total = {}
        _status(10, '正在自动登记本地缺口...')
        total['reported_gaps'] = _report_local_wanted_gaps(client)

        _status(25, '正在清理过期虚拟临时转存...')
        total['expired_virtual_cache_cleaned'] = _cleanup_expired_virtual_cache()

        _status(40, '正在为中心缺口自动创建本机分享...')
        total['auto_created_shares'] = _auto_share_center_open_gaps(client)

        _status(58, '正在从中心资源库处理追更缺集...')
        follow_result = _auto_follow_watching_series_from_center()
        total.update({f'follow_{k}': v for k, v in follow_result.items()})

        _status(72, '正在同步分享审核状态并自动登记中心...')
        total.update(_auto_check_and_report_local_shares(client))

        _status(92, '正在同步贡献值快照...')
        try:
            # 复用路由层已有的中心贡献值同步逻辑。
            from routes.shared_resource import _fetch_center_credit
            total['credit'] = _fetch_center_credit().get('ok', False)
        except Exception as e:
            if maintenance_silent:
                logger.error(f"  ➜ [共享资源维护] 同步贡献值失败: {e}")
            else:
                logger.warning(f"  ➜ [共享资源维护] 同步贡献值失败: {e}")
            total['credit'] = False

        msg = (
            f"共享资源维护完成：登记缺口 {total.get('reported_gaps', 0)}，"
            f"清理临时转存 {total.get('expired_virtual_cache_cleaned', 0)}，"
            f"自动创建分享 {total.get('auto_created_shares', 0)}，"
            f"追更命中 {total.get('follow_consumed', 0)}/{total.get('follow_missing', 0)}，"
            f"登记追更缺口 {total.get('follow_gaps', 0)}，"
            f"检查分享 {total.get('checked', 0)}，自动登记 {total.get('reported', 0)}，"
            f"清理失效 {total.get('cancelled', 0)}。"
        )
        if not maintenance_silent:
            logger.info(f"=== {msg} ===")
        _status(100, msg)
    finally:
        if old_logger_level is not None:
            logger.setLevel(old_logger_level)

def trigger_shared_resource_maintenance_task() -> bool:
    """供路由/调度器调用的统一入口。"""
    return task_manager.submit_task(
        task_shared_resource_maintenance,
        '共享资源自动维护',
        processor_type='media',
    )
