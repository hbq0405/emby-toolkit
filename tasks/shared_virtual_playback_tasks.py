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


def _safe_int(value, default=None):
    try:
        if value in (None, ''):
            return default
        return int(float(value))
    except Exception:
        return default


def _virtual_raw_parts(row: dict) -> tuple[dict, dict]:
    raw = _raw_json_dict((row or {}).get('raw_json'))
    context = raw.get('context') if isinstance(raw.get('context'), dict) else {}
    source = raw.get('center_source') if isinstance(raw.get('center_source'), dict) else {}
    return context, source


def _virtual_series_key(row: dict) -> str:
    """同剧统计键。优先使用 raw_json.context 里的父剧 ID。

    v7 之前有些虚拟单集会把每一集自己的 tmdb_id 写进
    parent_series_tmdb_id，导致自动转正按“每集一个父剧”统计，日志永远
    watched=1/阈值。这里优先读 context/source 的 parent_series_tmdb_id /
    parent_tmdb_id，并兼容旧行。
    """
    row = row or {}
    context, source = _virtual_raw_parts(row)
    for value in (
        context.get('parent_series_tmdb_id'),
        context.get('series_tmdb_id'),
        context.get('parent_tmdb_id'),
        source.get('parent_series_tmdb_id'),
        source.get('series_tmdb_id'),
        row.get('parent_series_tmdb_id'),
    ):
        value = str(value or '').strip()
        if value:
            return value

    item_type = str(row.get('item_type') or source.get('item_type') or context.get('item_type') or '').strip()
    if item_type in ('Series', 'Season'):
        return str(row.get('tmdb_id') or context.get('tmdb_id') or source.get('tmdb_id') or '').strip()
    return ''


def _virtual_season_number(row: dict):
    context, source = _virtual_raw_parts(row)
    for value in (row.get('season_number'), context.get('season_number'), source.get('season_number')):
        n = _safe_int(value, None)
        if n is not None:
            return n
    return None


def _virtual_episode_number(row: dict):
    context, source = _virtual_raw_parts(row)
    for value in (row.get('episode_number'), context.get('episode_number'), source.get('episode_number')):
        n = _safe_int(value, None)
        if n is not None:
            return n
    return None


def _virtual_identity_values(row: dict) -> tuple[str, int | None, int | None]:
    return _virtual_series_key(row), _virtual_season_number(row), _virtual_episode_number(row)


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
    parent, season, episode = _virtual_identity_values(row or {})
    payload = {
        'auto_promote_completed': True,
        'auto_promote_completed_at': datetime.now(timezone.utc).isoformat(),
        'auto_promote_user_id': str(user_id or ''),
        'auto_promote_event_type': str(event_type or ''),
        'auto_promote_playback_info': playback_info or {},
        'auto_promote_parent_series_tmdb_id': parent,
        'auto_promote_season_number': season,
        'auto_promote_episode_number': episode,
    }
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE shared_virtual_items
                SET auto_promote_completed=TRUE,
                    parent_series_tmdb_id=COALESCE(NULLIF(%s, ''), parent_series_tmdb_id),
                    season_number=COALESCE(%s, season_number),
                    episode_number=COALESCE(%s, episode_number),
                    raw_json = COALESCE(raw_json, '{}'::jsonb) || %s::jsonb,
                    updated_at=NOW()
                WHERE virtual_id=%s
                """,
                (parent, season, episode, json.dumps(payload, ensure_ascii=False), vid),
            )
        conn.commit()


def _candidate_completed_rows(row: dict, include_promote_pending: bool = True) -> List[dict]:
    parent, season, _ = _virtual_identity_values(row or {})
    if not parent:
        return []
    season_key = _safe_int(season, -1)
    status_filter = "status NOT IN ('deleted')" if include_promote_pending else "status NOT IN ('deleted','promoted','promote_pending')"
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT *
                    FROM shared_virtual_items
                    WHERE {status_filter}
                      AND auto_promote_completed IS TRUE
                      AND COALESCE(season_number, -1)=COALESCE(%s, -1)
                      AND (
                            parent_series_tmdb_id=%s
                         OR tmdb_id=%s
                         OR COALESCE(raw_json->'context'->>'parent_series_tmdb_id', '')=%s
                         OR COALESCE(raw_json->'context'->>'series_tmdb_id', '')=%s
                         OR COALESCE(raw_json->'context'->>'parent_tmdb_id', '')=%s
                         OR COALESCE(raw_json->'center_source'->>'parent_series_tmdb_id', '')=%s
                         OR COALESCE(raw_json->'center_source'->>'series_tmdb_id', '')=%s
                      )
                    ORDER BY COALESCE(episode_number, 999999), updated_at DESC
                    LIMIT 80
                    """,
                    (season_key, parent, parent, parent, parent, parent, parent, parent),
                )
                rows = [dict(r) for r in cur.fetchall()]
    except Exception as e:
        logger.debug(f"  ➜ [共享虚拟转正] 查询已看虚拟剧集失败: parent={parent}, season={season}, err={e}")
        return []

    # 再用 Python 按统一身份过滤一次，兼容已写脏的旧行。
    filtered = []
    seen_vid = set()
    for r in rows:
        r_parent, r_season, _ = _virtual_identity_values(r)
        if r_parent == parent and _safe_int(r_season, -1) == season_key:
            vid = str(r.get('virtual_id') or '')
            if vid and vid not in seen_vid:
                seen_vid.add(vid)
                filtered.append(r)
    return filtered


def _completed_virtual_episode_count(row: dict) -> int:
    rows = _candidate_completed_rows(row, include_promote_pending=True)
    keys = set()
    for r in rows:
        _, season, episode = _virtual_identity_values(r)
        if episode is None:
            continue
        keys.add((season if season is not None else -1, episode))
    return len(keys)


def _candidate_rows_for_auto_promote(row: dict) -> List[dict]:
    parent, season, _ = _virtual_identity_values(row or {})
    rows = _candidate_completed_rows(row, include_promote_pending=False)
    if rows:
        logger.info(
            "  ➜ [共享虚拟转正] 自动转正候选: series=%s, season=%s, rows=%s",
            parent or '-', season if season is not None else '-', len(rows)
        )
        return rows
    vid = str((row or {}).get('virtual_id') or '').strip()
    if not vid:
        return []
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM shared_virtual_items WHERE virtual_id=%s", (vid,))
                r = cur.fetchone()
                return [dict(r)] if r else []
    except Exception as e:
        logger.debug(f"  ➜ [共享虚拟转正] 获取自动转正兜底候选失败: {e}")
    return []


def _truthy(value) -> bool:
    if value is True:
        return True
    if isinstance(value, str):
        return value.strip().lower() in ('1', 'true', 'yes', 'on', 'played', 'complete', 'completed', '已播放')
    if isinstance(value, (int, float)):
        return value == 1
    return False


def _pick_nested_bool(*values) -> bool:
    return any(_truthy(v) for v in values)


def _playback_progress_percent(data: dict, playback_info: dict) -> float:
    item = (data or {}).get('Item') or {}
    user_data = (data or {}).get('UserData') or item.get('UserData') or {}
    if _pick_nested_bool(
        playback_info.get('PlayedToCompletion'),
        playback_info.get('IsPlayedToCompletion'),
        user_data.get('Played'),
        item.get('Played'),
    ):
        return 100.0
    pos = (
        playback_info.get('PositionTicks')
        or playback_info.get('PlaybackPositionTicks')
        or user_data.get('PlaybackPositionTicks')
        or user_data.get('PlaybackPosition')
    )
    runtime = playback_info.get('RunTimeTicks') or item.get('RunTimeTicks') or item.get('RunTime')
    try:
        pos = float(pos or 0)
        runtime = float(runtime or 0)
        if pos > 0 and runtime > 0:
            return max(0.0, min(100.0, pos * 100.0 / runtime))
    except Exception:
        pass
    return 0.0


def _episode_played_to_completion(data: dict, playback_info: dict) -> bool:
    item = (data or {}).get('Item') or {}
    user_data = (data or {}).get('UserData') or item.get('UserData') or {}
    if _pick_nested_bool(
        playback_info.get('PlayedToCompletion'),
        playback_info.get('IsPlayedToCompletion'),
        user_data.get('Played'),
        item.get('Played'),
    ):
        return True
    # Emby 某些客户端的 stop 事件不带 PlayedToCompletion，但会带接近片尾的 PositionTicks。
    # 电视剧自动转正按“看完几集”计数，95% 以上按完播兜底，避免事件字段差异导致不计数。
    return _playback_progress_percent(data, playback_info) >= 95.0


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
        if event_type != 'playback.stop':
            return
        if not _episode_played_to_completion(data, playback_info):
            logger.debug(
                "  ➜ [共享虚拟转正] 剧集播放停止但未达到完播条件，跳过计数: item=%s, progress=%.1f%%",
                item_id, _playback_progress_percent(data, playback_info)
            )
            return
        for row in rows:
            _mark_virtual_completed_for_auto(row, user_id, event_type, playback_info)
        threshold = _cfg_int('', 'p115_shared_auto_promote_tv_episodes', 2, 1, 99)
        base_row = rows[0]
        watched_count = _completed_virtual_episode_count(base_row)
        parent, season, _ = _virtual_identity_values(base_row)
        logger.info(
            "  ➜ [共享虚拟转正] 虚拟剧集完播计数: watched=%s/%s, item=%s, series=%s, season=%s",
            watched_count, threshold, item_id, parent or '-', season if season is not None else '-'
        )
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
