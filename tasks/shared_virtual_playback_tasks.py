# tasks/shared_virtual_playback_tasks.py
# 共享虚拟入库播放事件任务：Webhook 只负责转发，自动续期/自动转正都在这里处理。
import json
import logging
from datetime import datetime, timezone
from typing import List

from gevent import spawn

from database import settings_db
from database.connection import get_db_connection

logger = logging.getLogger(__name__)


def _cfg_bool(name: str, fallback: str, default=False) -> bool:
    # name 参数保留函数签名占位；共享资源配置已独立保存到 app_settings.shared_resource_config。
    value = settings_db.get_shared_resource_config().get(fallback, default)
    if isinstance(value, str):
        return value.strip().lower() in ('1', 'true', 'yes', 'on', '启用', '开启')
    return bool(value)


def _cfg_int(name: str, fallback: str, default=0, minimum: int = None, maximum: int = None) -> int:
    try:
        value = settings_db.get_shared_resource_config().get(fallback, default)
        n = int(float(value)) if value not in (None, '') else int(default)
    except Exception:
        n = int(default)
    if minimum is not None:
        n = max(int(minimum), n)
    if maximum is not None:
        n = min(int(maximum), n)
    return n


def _raw_json_dict(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            obj = json.loads(value)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}


def _find_virtual_rows_for_emby_item(emby_item_id: str, item_path: str = '') -> List[dict]:
    emby_item_id = str(emby_item_id or '').strip()
    item_path = str(item_path or '').strip()
    if not emby_item_id and not item_path:
        return []
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM shared_virtual_items
                    WHERE status NOT IN ('deleted','promoted','promote_pending')
                      AND (
                            (%s <> '' AND COALESCE(emby_item_id, '') = %s)
                         OR (%s <> '' AND COALESCE(raw_json->>'last_play_emby_item_id', '') = %s)
                         OR (%s <> '' AND COALESCE(strm_path, '') = %s)
                      )
                    ORDER BY updated_at DESC
                    LIMIT 20
                    """,
                    (emby_item_id, emby_item_id, emby_item_id, emby_item_id, item_path, item_path),
                )
                return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        logger.debug(f"  ➜ [共享虚拟转正] 查找 Emby 对应虚拟资源失败: emby={emby_item_id}, err={e}")
    return []


def _mark_virtual_completed_for_auto(row: dict, user_id: str, event_type: str, playback_info: dict):
    vid = str((row or {}).get('virtual_id') or '').strip()
    if not vid:
        return
    payload = {
        'auto_promote_completed': True,
        'auto_promote_completed_at': datetime.now(timezone.utc).isoformat(),
        'auto_promote_user_id': str(user_id or ''),
        'auto_promote_event_type': str(event_type or ''),
        'auto_promote_playback_info': playback_info or {},
    }
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE shared_virtual_items
                SET auto_promote_completed=TRUE,
                    raw_json = COALESCE(raw_json, '{}'::jsonb) || %s::jsonb,
                    updated_at=NOW()
                WHERE virtual_id=%s
                """,
                (json.dumps(payload, ensure_ascii=False), vid),
            )
        conn.commit()


def _completed_virtual_episode_count(row: dict) -> int:
    parent = str((row or {}).get('parent_series_tmdb_id') or (row or {}).get('tmdb_id') or '').strip()
    season = (row or {}).get('season_number')
    if not parent:
        return 0
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(DISTINCT COALESCE(season_number, -1)::text || '-' || COALESCE(episode_number, -1)::text) AS cnt
                    FROM shared_virtual_items
                    WHERE status NOT IN ('deleted')
                      AND auto_promote_completed IS TRUE
                      AND (parent_series_tmdb_id=%s OR tmdb_id=%s)
                      AND COALESCE(season_number, -1)=COALESCE(%s, -1)
                    """,
                    (parent, parent, season),
                )
                r = cur.fetchone()
                return int((r or {}).get('cnt') or 0)
    except Exception as e:
        logger.debug(f"  ➜ [共享虚拟转正] 统计已看虚拟剧集失败: parent={parent}, err={e}")
    return 0


def _candidate_rows_for_auto_promote(row: dict) -> List[dict]:
    parent = str((row or {}).get('parent_series_tmdb_id') or (row or {}).get('tmdb_id') or '').strip()
    season = (row or {}).get('season_number')
    vid = str((row or {}).get('virtual_id') or '').strip()
    rows = []
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                if parent:
                    cur.execute(
                        """
                        SELECT *
                        FROM shared_virtual_items
                        WHERE status NOT IN ('deleted','promoted','promote_pending')
                          AND auto_promote_completed IS TRUE
                          AND (parent_series_tmdb_id=%s OR tmdb_id=%s)
                          AND COALESCE(season_number, -1)=COALESCE(%s, -1)
                        ORDER BY COALESCE(episode_number, 999999), updated_at DESC
                        LIMIT 20
                        """,
                        (parent, parent, season),
                    )
                    rows = [dict(r) for r in cur.fetchall()]
                if not rows and vid:
                    cur.execute("SELECT * FROM shared_virtual_items WHERE virtual_id=%s", (vid,))
                    r = cur.fetchone()
                    if r:
                        rows = [dict(r)]
    except Exception as e:
        logger.debug(f"  ➜ [共享虚拟转正] 获取自动转正候选失败: {e}")
    return rows


def _playback_progress_percent(data: dict, playback_info: dict) -> float:
    if playback_info.get('PlayedToCompletion') is True:
        return 100.0
    pos = playback_info.get('PositionTicks') or playback_info.get('PlaybackPositionTicks')
    item = (data or {}).get('Item') or {}
    runtime = playback_info.get('RunTimeTicks') or item.get('RunTimeTicks') or item.get('RunTime')
    try:
        pos = float(pos or 0)
        runtime = float(runtime or 0)
        if pos > 0 and runtime > 0:
            return max(0.0, min(100.0, pos * 100.0 / runtime))
    except Exception:
        pass
    return 0.0


def _promote_virtual_rows_async(rows: List[dict], reason: str):
    rows = [r for r in (rows or []) if r and r.get('virtual_id')]
    if not rows:
        return

    def _runner():
        try:
            from routes.shared_resource import promote_virtual_item_internal
        except Exception as e:
            logger.warning(f"  ➜ [共享虚拟转正] 加载转正入口失败: {e}")
            return
        for row in rows:
            vid = str(row.get('virtual_id') or '').strip()
            if not vid:
                continue
            try:
                result = promote_virtual_item_internal(vid, data={}, reason=reason)
                if result.get('success'):
                    with get_db_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute("UPDATE shared_virtual_items SET auto_promoted_at=NOW() WHERE virtual_id=%s", (vid,))
                        conn.commit()
                    logger.info(f"  ➜ [共享虚拟转正] 自动转正成功: virtual_id={vid}, reason={reason}")
                else:
                    logger.warning(f"  ➜ [共享虚拟转正] 自动转正失败: virtual_id={vid}, msg={result.get('message')}")
            except Exception as e:
                logger.warning(f"  ➜ [共享虚拟转正] 自动转正异常: virtual_id={vid}, err={e}", exc_info=True)

    spawn(_runner)


def handle_shared_virtual_playback_event(data: dict, event_type: str, item_id: str, item_type: str, user_id: str):
    """处理共享虚拟入库的播放事件。

    负责：
    1. 根据 Emby 播放完成/进度事件更新自动转正标记；
    2. 满足电视剧已看集数或电影播放进度阈值时，异步调用转正；
    3. 所有异常在任务内吞掉，避免反向影响 Emby Webhook 主流程。
    """
    try:
        _handle_shared_virtual_playback_event(data, event_type, item_id, item_type, user_id)
    except Exception as e:
        logger.warning(f"  ➜ [共享虚拟转正] 播放事件任务失败: {e}", exc_info=True)


def dispatch_shared_virtual_playback_event(data: dict, event_type: str, item_id: str, item_type: str, user_id: str) -> bool:
    """Webhook 调用的轻量分发入口。

    返回 True 只表示任务已投递，不代表发生了自动转正。
    """
    if event_type not in ('playback.pause', 'playback.stop'):
        return False
    spawn(handle_shared_virtual_playback_event, data or {}, event_type, item_id or '', item_type or '', user_id or '')
    return True


def _handle_shared_virtual_playback_event(data: dict, event_type: str, item_id: str, item_type: str, user_id: str):
    if not _cfg_bool('', 'p115_shared_auto_promote_enabled', False):
        return
    if event_type not in ('playback.pause', 'playback.stop'):
        return

    item = (data or {}).get('Item') or {}
    playback_info = (data or {}).get('PlaybackInfo') or {}
    rows = _find_virtual_rows_for_emby_item(item_id, item.get('Path') or item.get('FileName') or '')
    if not rows:
        return

    item_type_l = str(item_type or item.get('Type') or '').lower()
    if item_type_l == 'episode':
        if event_type != 'playback.stop' or playback_info.get('PlayedToCompletion') is not True:
            return
        for row in rows:
            _mark_virtual_completed_for_auto(row, user_id, event_type, playback_info)
        threshold = _cfg_int('', 'p115_shared_auto_promote_tv_episodes', 2, 1, 99)
        base_row = rows[0]
        watched_count = _completed_virtual_episode_count(base_row)
        if watched_count >= threshold:
            _promote_virtual_rows_async(
                _candidate_rows_for_auto_promote(base_row),
                reason=f'auto_tv_{watched_count}_episodes',
            )
    elif item_type_l == 'movie':
        progress = _playback_progress_percent(data, playback_info)
        threshold = _cfg_int('', 'p115_shared_auto_promote_movie_progress', 80, 1, 100)
        if progress >= threshold:
            _promote_virtual_rows_async(rows[:1], reason=f'auto_movie_{int(progress)}pct')
