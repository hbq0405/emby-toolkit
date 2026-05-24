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


def _looks_share_alive(resp: Dict[str, Any]) -> bool:
    if not _parse_share_ok(resp):
        return False
    text = json.dumps(resp, ensure_ascii=False).lower()
    return not any(k in text for k in ['已取消', '已失效', '不存在', '取消分享', 'expired', 'cancelled', 'not found'])


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
        if status in ('cancelled', 'deleted', 'dead'):
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
                            resp = client.register_source(
                                tmdb_id=item.get('tmdb_id') or record.get('tmdb_id'),
                                item_type=item.get('item_type') or record.get('item_type') or 'Movie',
                                season_number=item.get('season_number') or record.get('season_number'),
                                episode_number=item.get('episode_number'),
                                title=record.get('title') or item.get('file_name'),
                                release_year=record.get('release_year'),
                                sha1=sha1,
                                size=_safe_int(item.get('size'), 0),
                                file_name=item.get('file_name') or '',
                                quality='',
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
                # 115 分享已不可用，撤销中心源并本地标记。
                sha1s = [i.get('sha1') for i in (shared_share_db.list_share_items(record['id']) or []) if i.get('sha1')]
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


def _has_existing_share_for_gap(gap: Dict[str, Any]) -> bool:
    tmdb_id = str(gap.get('tmdb_id') or '')
    item_type = str(gap.get('item_type') or '')
    season = gap.get('season_number')
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM shared_share_records
                WHERE tmdb_id=%s
                  AND (item_type=%s OR %s IN ('Series','Season','Episode'))
                  AND COALESCE(season_number, -1)=COALESCE(%s, -1)
                  AND status NOT IN ('cancelled','dead','deleted','cancel_failed')
                LIMIT 1
                """,
                (tmdb_id, item_type, item_type, int(season) if season not in (None, '') else -1),
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


def task_shared_resource_maintenance(processor=None):
    """共享资源维护总任务。可由前端手动触发，也由调度器硬编码定时执行。"""
    task_manager.update_status_from_thread(0, '正在初始化共享资源维护任务...')
    if not _enabled():
        task_manager.update_status_from_thread(100, '共享资源未启用，跳过。')
        return
    client = SharedCenterClient()
    if not client.ready:
        task_manager.update_status_from_thread(100, '共享中心地址或 device_token 未配置，跳过。')
        return

    total = {}
    task_manager.update_status_from_thread(10, '正在自动登记本地缺口...')
    total['reported_gaps'] = _report_local_wanted_gaps(client)

    task_manager.update_status_from_thread(35, '正在为中心缺口自动创建本机分享...')
    total['auto_created_shares'] = _auto_share_center_open_gaps(client)

    task_manager.update_status_from_thread(65, '正在同步分享审核状态并自动登记中心...')
    total.update(_auto_check_and_report_local_shares(client))

    task_manager.update_status_from_thread(90, '正在同步贡献值快照...')
    try:
        # 复用路由层已有的中心贡献值同步逻辑。
        from routes.shared_resource import _fetch_center_credit
        total['credit'] = _fetch_center_credit().get('ok', False)
    except Exception as e:
        logger.warning(f"  ➜ [共享资源维护] 同步贡献值失败: {e}")
        total['credit'] = False

    msg = (
        f"共享资源维护完成：登记缺口 {total.get('reported_gaps', 0)}，"
        f"自动创建分享 {total.get('auto_created_shares', 0)}，"
        f"检查分享 {total.get('checked', 0)}，自动登记 {total.get('reported', 0)}，"
        f"清理失效 {total.get('cancelled', 0)}。"
    )
    logger.info(f"=== {msg} ===")
    task_manager.update_status_from_thread(100, msg)


def trigger_shared_resource_maintenance_task() -> bool:
    """供路由/调度器调用的统一入口。"""
    return task_manager.submit_task(
        task_shared_resource_maintenance,
        '共享资源自动维护',
        processor_type='media',
    )
