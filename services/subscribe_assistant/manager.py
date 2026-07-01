import hashlib
import json
import logging
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional
from gevent import spawn

from database import connection
from database import settings_db, watchlist_db
import handler.moviepilot as moviepilot
import tasks.helpers as helpers
from tasks.p115_fingerprint_helpers import p115_fp_is_virtual_strm_target, p115_fp_read_strm_target

from .config import AssistantConfig, from_watchlist_config
from .engine import (
    CompletionSignal,
    build_scope,
    check_airing_gap_pause,
    check_pre_air_pause,
    evaluate_completion,
    parse_date,
    should_enter_pending,
)
from . import store

logger = logging.getLogger(__name__)


STATE_SUBSCRIBES = "subscribes"
STATE_TORRENTS = "torrents"
SOURCE_PENDING_JUDGE = "pending_judge"
SOURCE_GUARD_VETO = "guard_veto"
SOURCE_DOWNLOAD_PENDING = "download_pending"
SOURCE_PRE_AIR = "pre_air"
SOURCE_AIRING_GAP = "airing_gap"
SOURCE_NO_DOWNLOAD = "no_download"
SOURCE_MANUAL_MP = "manual_mp"
MANUAL_CHANGE_GRACE_SECONDS = 3600


def get_config() -> AssistantConfig:
    cfg = from_watchlist_config(settings_db.get_setting("watchlist_config") or {})
    strategy = settings_db.get_setting("subscription_strategy_config") or {}
    sources = strategy.get("subscription_sources")
    mp_config = settings_db.get_setting("mp_config") or {}
    if not mp_config.get("moviepilot_url") or (isinstance(sources, list) and "mp" not in sources):
        cfg.enabled = False
    return cfg


class SubscribeAssistantManager:
    def __init__(self, app_config: Dict[str, Any] = None, assistant_config: AssistantConfig = None):
        self.app_config = app_config or {}
        self.cfg = assistant_config or get_config()
        self._title_cache: Dict[str, str] = {}

    def sync_series(
        self,
        *,
        tmdb_id: str,
        series_name: str,
        series_details: Dict[str, Any],
        final_status: str,
        old_status: str = None,
        all_tmdb_episodes: List[Dict[str, Any]] = None,
        real_next_episode: Dict[str, Any] = None,
        triggering_episode_ids: List[str] = None,
    ) -> None:
        if not self.cfg.enabled:
            logger.debug("  ➜ [订阅助手] 已关闭，跳过 MoviePilot 同步。")
            return

        all_tmdb_episodes = all_tmdb_episodes or []
        valid_seasons = [
            s for s in (series_details.get("seasons") or [])
            if _safe_int(s.get("season_number")) > 0
        ]
        if not valid_seasons:
            return
        latest_season = max(valid_seasons, key=lambda s: _safe_int(s.get("season_number")))
        latest_season_num = _safe_int(latest_season.get("season_number"))

        existing = moviepilot.find_subscriptions(tmdb_id, config=self.app_config)
        existing_by_season = {
            _safe_int(sub.get("season")): sub
            for sub in existing
            if _safe_int(sub.get("season")) > 0
        }

        target_seasons = self._target_seasons_for_sync(
            valid_seasons=valid_seasons,
            existing_by_season=existing_by_season,
            latest_season_num=latest_season_num,
            final_status=final_status,
        )
        if not target_seasons:
            return

        for season_info in target_seasons:
            season_num = _safe_int(season_info.get("season_number"))
            season_episodes = [
                ep for ep in all_tmdb_episodes
                if _safe_int(ep.get("season_number")) == season_num
            ]
            signal = self._completion_signal(
                tmdb_id=tmdb_id,
                season=season_num,
                series_details=series_details,
                episodes=all_tmdb_episodes,
                season_info=season_info,
            )
            decision = self._decide_subscription_state(
                final_status=final_status,
                series_details=series_details,
                season=season_num,
                season_episodes=season_episodes,
                signal=signal,
                real_next_episode=real_next_episode,
            )
            sub = existing_by_season.get(season_num)
            if not sub and season_num == latest_season_num and final_status in ("Watching", "Paused", "Pending"):
                if self._triggering_episodes_are_virtual(triggering_episode_ids) or self._season_has_virtual_import(tmdb_id, season_num):
                    logger.info(
                        "  ➜ [订阅助手] 《%s》第 %s 季 仍处于虚拟入库状态，跳过自动创建 MP 订阅，等待正式入库。",
                        series_name,
                        season_num,
                    )
                    continue
                sub = self._create_subscription(tmdb_id, series_name, season_num, decision)
                if sub:
                    existing_by_season[season_num] = sub
            if not sub:
                continue

            subscribe_id = _safe_int(sub.get("id"))
            if self._has_recent_manual_mp_change(subscribe_id, "state"):
                logger.info(
                    "  ➜ [订阅助手] 《%s》S%s 检测到 MP 近期人工改状态，本轮暂不覆盖。",
                    series_name,
                    season_num,
                )
                continue

            self._update_source_state(subscribe_id, decision)
            total = self._target_total(decision, season_info, signal)
            self._remember_expected_mp_update(
                subscribe_id,
                fields=["state", "total_episode"] if total else ["state"],
                expected_state=decision["mp_state"],
                expected_total=total,
            )
            if moviepilot.update_subscription_status(
                int(tmdb_id),
                season_num,
                decision["mp_state"],
                self.app_config,
                total_episodes=total,
            ):
                logger.info(
                    "  ➜ [订阅助手] 《%s》第 %s 季 已同步 MP 状态=%s，总集数=%s，原因=%s",
                    series_name,
                    season_num,
                    decision["mp_state"],
                    total or "不改",
                    decision.get("reason") or "状态同步",
                )

            if decision.get("snapshot"):
                self._sync_completed_full_washing(
                    tmdb_id=tmdb_id,
                    series_name=series_name,
                    season=season_num,
                    subscribe=sub,
                    season_info=season_info,
                    signal=signal,
                )
                scope = build_scope(tmdb_id, season_num, all_tmdb_episodes)
                store.upsert_snapshot(
                    tmdb_id=str(tmdb_id),
                    season_number=season_num,
                    subscribe_id=subscribe_id or None,
                    scope_total=scope.total,
                    scope={
                        "season": season_num,
                        "total": scope.total,
                        "high_risk": scope.high_risk,
                        "signals": signal.signals,
                    },
                    subscribe=sub,
                )

    def run_periodic_checks(self, limit: int = 100) -> Dict[str, int]:
        stats = {
            "released_pending": 0,
            "download_checked": 0,
            "snapshots_checked": 0,
            "snapshots_cleaned": 0,
            "delete_records_cleaned": 0,
        }
        stats["delete_records_cleaned"] = store.cleanup_delete_records()
        stats["snapshots_cleaned"] = store.cleanup_snapshots(self.cfg.snapshot_retention_days)
        if self.cfg.download_monitor_enabled:
            stats["download_checked"] = self.run_download_check()
        if self.cfg.verify_enabled:
            stats["snapshots_checked"] = self.run_snapshot_verify(limit=limit)
        return stats

    def run_download_check(self) -> int:
        torrents = store.read_state(STATE_TORRENTS)
        if not torrents:
            return 0
        live = moviepilot.get_downloading_tasks(self.app_config)
        live_by_hash = {
            str(task.get("hash") or task.get("hashString") or task.get("id") or "").lower(): task
            for task in live or []
        }
        changed = 0
        now = time.time()

        def updater(data):
            nonlocal changed
            for torrent_hash, task in list(data.items()):
                info = live_by_hash.get(str(torrent_hash).lower())
                if not info:
                    if self.cfg.manual_delete_listen:
                        self._clear_download_pending(task.get("subscribe_id"), torrent_hash, "下载任务已不存在")
                        data.pop(torrent_hash, None)
                        changed += 1
                    continue
                progress = _progress_value(info)
                baseline = float(task.get("baseline_progress") or 0)
                baseline_at = float(task.get("baseline_at") or task.get("time") or now)
                if progress >= 100 or info.get("state") in ("已完成", "completed", "COMPLETE"):
                    self._clear_download_pending(task.get("subscribe_id"), torrent_hash, "下载已完成")
                    data.pop(torrent_hash, None)
                    changed += 1
                    continue
                if now - baseline_at < self.cfg.download_timeout_minutes * 60:
                    continue
                if progress - baseline >= self.cfg.download_progress_threshold:
                    task["baseline_progress"] = progress
                    task["baseline_at"] = now
                    task["retry_count"] = 0
                    data[torrent_hash] = task
                    continue
                retry_count = _safe_int(task.get("retry_count")) + 1
                task["retry_count"] = retry_count
                task["baseline_at"] = now
                data[torrent_hash] = task
                if retry_count >= self.cfg.download_retry_limit:
                    logger.warning("  ➜ [订阅助手] 下载任务 %s 连续停滞，已达到人工保护阈值。", str(torrent_hash)[:8])
                    continue
                if moviepilot.delete_download_tasks("", self.app_config, hashes=[torrent_hash]):
                    fingerprint = self._delete_fingerprint(task)
                    store.record_deleted_resource(
                        fingerprint,
                        tmdb_id=str(task.get("tmdb_id") or ""),
                        season_number=task.get("season"),
                        episodes=task.get("episodes") or [],
                        reason="timeout",
                        retention_hours=self.cfg.delete_record_retention_hours,
                    )
                    if self.cfg.auto_search_when_delete and task.get("subscribe_id"):
                        moviepilot.search_subscription(_safe_int(task.get("subscribe_id")), self.app_config)
                    self._clear_download_pending(task.get("subscribe_id"), torrent_hash, "下载超时删种")
                    data.pop(torrent_hash, None)
                    changed += 1
            return data

        store.update_state(STATE_TORRENTS, updater)
        return changed

    def run_snapshot_verify(self, limit: int = 100) -> int:
        checked = 0
        for snapshot in store.get_snapshots_due(self.cfg.verify_interval_hours, limit=limit):
            tmdb_id = str(snapshot.get("tmdb_id") or "")
            season = _safe_int(snapshot.get("season_number"))
            old_total = _safe_int(snapshot.get("scope_total"))
            if not tmdb_id or season <= 0:
                store.mark_snapshot_checked(_safe_int(snapshot.get("id")))
                checked += 1
                continue

            locked = self._season_total_locked(tmdb_id, season)
            if locked:
                title = self._series_title(tmdb_id, snapshot.get("subscribe_json"))
                logger.info(
                    "  ➜ [订阅助手] 《%s》第 %s 季 已由豆瓣/手动锁定为 %s 集，自动纠错跳过 MP 恢复动作。",
                    title,
                    season,
                    locked.get("count") or "未知",
                )
                store.mark_snapshot_checked(_safe_int(snapshot.get("id")))
                checked += 1
                continue

            fixed = self._repair_snapshot_subscription(snapshot, tmdb_id, season, old_total)
            if fixed:
                logger.info(
                    "  ➜ [订阅助手] 已根据完成快照纠正《%s》第 %s 季的 MP 订阅。",
                    self._series_title(tmdb_id, snapshot.get("subscribe_json")),
                    season,
                )
            store.mark_snapshot_checked(_safe_int(snapshot.get("id")))
            checked += 1
        return checked

    def handle_moviepilot_event(self, event_type: str, payload: Dict[str, Any]) -> bool:
        if not self.cfg.enabled:
            return False
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        try:
            if event_type == "download.added":
                return self._handle_download_added(data)
            if event_type == "subscribe.added":
                return self._handle_subscribe_added(data)
            if event_type == "subscribe.modified":
                return self._handle_subscribe_modified(data)
            if event_type == "subscribe.deleted":
                return self._handle_subscribe_deleted(data)
            if event_type == "subscribe.complete":
                return self._handle_subscribe_complete(data)
        except Exception as e:
            logger.warning("  ➜ [订阅助手] 处理 MP Webhook 事件失败：%s -> %s", event_type, e, exc_info=True)
            return False
        return False

    def _handle_download_added(self, data: Dict[str, Any]) -> bool:
        torrent_hash = str(data.get("hash") or "").lower().strip()
        if not torrent_hash:
            return False
        source_info = self._parse_subscribe_source(data.get("source"))
        context = data.get("context") if isinstance(data.get("context"), dict) else {}
        meta = context.get("meta_info") if isinstance(context.get("meta_info"), dict) else {}
        torrent = context.get("torrent_info") if isinstance(context.get("torrent_info"), dict) else {}
        media = context.get("media_info") if isinstance(context.get("media_info"), dict) else {}
        subscribe_id = _safe_int(source_info.get("id") or data.get("subscribe_id"))
        if subscribe_id <= 0:
            logger.debug("  ➜ [订阅助手] download.added 未携带订阅 ID，跳过下载状态登记：%s", torrent_hash[:8])
            return False
        season = _safe_int(source_info.get("season") or meta.get("begin_season") or media.get("season"))
        episodes = data.get("episodes") or meta.get("episode_list") or []
        if not isinstance(episodes, list):
            episodes = [episodes]
        metadata = {
            "tmdb_id": str(source_info.get("tmdbid") or media.get("tmdb_id") or ""),
            "season": season or None,
            "episodes": [_safe_int(x) for x in episodes if _safe_int(x) > 0],
            "title": torrent.get("title") or meta.get("title") or media.get("title") or source_info.get("name") or "",
            "page_url": torrent.get("page_url") or "",
            "enclosure": torrent.get("enclosure") or "",
            "site_name": torrent.get("site_name") or "",
            "size": torrent.get("size"),
            "username": data.get("username") or "",
            "source": "moviepilot_webhook",
        }
        self.mark_download_started(subscribe_id, torrent_hash, **metadata)
        self._remember_subscription(subscribe_id, source_info or {
            "id": subscribe_id,
            "tmdbid": metadata["tmdb_id"],
            "season": season,
            "name": metadata["title"],
        }, reason="download.added")
        logger.info(
            "  ➜ [订阅助手] 已接收 MP 下载事件：订阅 %s，%s 第 %s 季，hash=%s。",
            subscribe_id,
            self._series_title(metadata["tmdb_id"], source_info or metadata),
            season or "-",
            torrent_hash[:8],
        )
        return True

    def _handle_subscribe_added(self, data: Dict[str, Any]) -> bool:
        subscribe_id = _safe_int(data.get("subscribe_id"))
        info = data.get("subscribe_info") if isinstance(data.get("subscribe_info"), dict) else {}
        media = data.get("mediainfo") if isinstance(data.get("mediainfo"), dict) else {}
        if not info:
            info = {
                "id": subscribe_id,
                "name": media.get("title"),
                "type": media.get("type"),
                "tmdbid": media.get("tmdb_id"),
                "imdbid": media.get("imdb_id"),
                "season": media.get("season"),
                "year": media.get("year"),
            }
        if subscribe_id <= 0:
            subscribe_id = _safe_int(info.get("id"))
        if subscribe_id <= 0:
            return False
        info = self._enrich_subscribe_info(info, media, subscribe_id)
        self._remember_subscription(subscribe_id, info, reason="subscribe.added")
        logger.info("  ➜ [订阅助手] 已接管 MP 新增订阅：%s。", self._format_subscribe_info(info))
        return True

    def _handle_subscribe_modified(self, data: Dict[str, Any]) -> bool:
        info = data.get("subscribe_info") if isinstance(data.get("subscribe_info"), dict) else {}
        subscribe_id = _safe_int(data.get("subscribe_id") or info.get("id"))
        if subscribe_id <= 0:
            return False
        old_info = data.get("old_subscribe_info") if isinstance(data.get("old_subscribe_info"), dict) else {}
        fields = data.get("fields") if isinstance(data.get("fields"), list) else []
        scene = str(data.get("scene") or "")
        if self._consume_expected_mp_update(subscribe_id, info, fields):
            self._remember_subscription(subscribe_id, info, reason="subscribe.modified.expected")
            logger.debug("  ➜ [订阅助手] 已确认 ETK 预期内的 MP 订阅修改：%s。", self._format_subscribe_info(info))
            return True

        self._remember_subscription(
            subscribe_id,
            info,
            reason="subscribe.modified",
            extra={
                "last_manual_change": {
                    "scene": scene,
                    "fields": fields,
                    "old_state": old_info.get("state"),
                    "new_state": info.get("state"),
                    "updated_at": time.time(),
                }
            },
        )
        if fields:
            self._mark_active_source(subscribe_id, SOURCE_MANUAL_MP, f"MP 手动修改：{','.join(str(x) for x in fields)}")
        logger.debug(
            "  ➜ [订阅助手] 已记录 MP 订阅修改：%s，scene=%s，fields=%s。",
            self._format_subscribe_info(info),
            scene or "-",
            ",".join(str(x) for x in fields) or "-",
        )
        return True

    def _handle_subscribe_deleted(self, data: Dict[str, Any]) -> bool:
        info = data.get("subscribe_info") if isinstance(data.get("subscribe_info"), dict) else {}
        subscribe_id = _safe_int(data.get("subscribe_id") or info.get("id"))
        if subscribe_id <= 0:
            return False
        self._remove_subscription_state(subscribe_id, info, reason="subscribe.deleted")
        self._clear_torrents_for_subscription(subscribe_id, "订阅已删除")
        tmdb_id = str(info.get("tmdbid") or info.get("tmdb_id") or "").strip()
        season = _safe_int(info.get("season"))
        if self.cfg.verify_enabled and tmdb_id and season > 0:
            snapshot = store.get_latest_snapshot(
                tmdb_id=tmdb_id,
                season_number=season or None,
                subscribe_id=subscribe_id,
            ) or store.get_latest_snapshot(
                tmdb_id=tmdb_id,
                season_number=season or None,
            )
            if snapshot:
                fixed = self._repair_snapshot_subscription(snapshot, tmdb_id, season, _safe_int(snapshot.get("scope_total")))
                if fixed:
                    logger.info("  ➜ [订阅助手] MP 订阅删除已由完成快照实时纠正：%s。", self._format_subscribe_info(info))
        logger.info("  ➜ [订阅助手] 已记录 MP 订阅删除：%s。", self._format_subscribe_info(info))
        return True

    def _handle_subscribe_complete(self, data: Dict[str, Any]) -> bool:
        info = data.get("subscribe_info") if isinstance(data.get("subscribe_info"), dict) else {}
        media = data.get("mediainfo") if isinstance(data.get("mediainfo"), dict) else {}
        subscribe_id = _safe_int(data.get("subscribe_id") or info.get("id"))
        tmdb_id = str(info.get("tmdbid") or media.get("tmdb_id") or "").strip()
        season = _safe_int(info.get("season") or media.get("season"))
        if subscribe_id <= 0 or not tmdb_id:
            return False
        total = _safe_int(info.get("total_episode") or media.get("number_of_episodes"))
        self._remember_subscription(subscribe_id, info, reason="subscribe.complete")
        store.upsert_snapshot(
            tmdb_id=tmdb_id,
            item_type="Series" if str(info.get("type") or media.get("type") or "") == "电视剧" else "Movie",
            season_number=season or None,
            subscribe_id=subscribe_id,
            scope_total=total,
            scope={
                "season": season,
                "total": total,
                "source": "subscribe.complete",
                "completed_at": time.time(),
            },
            subscribe=info,
        )
        self._clear_download_pending(subscribe_id, "", "订阅已完成")
        self._clear_torrents_for_subscription(subscribe_id, "订阅已完成")
        logger.info("  ➜ [订阅助手] 已根据 MP 完成事件写入快照：%s，总集数=%s。", self._format_subscribe_info(info), total or "-")
        self._trigger_subscription_cleanup_on_complete(tmdb_id, season, info)
        return True

    def _trigger_subscription_cleanup_on_complete(self, tmdb_id: str, season: int, info: Dict[str, Any]) -> None:
        cleanup_type = str(self.cfg.subscription_cleanup_history_type or "none").strip().lower()
        scenes = {str(x).strip().lower() for x in (self.cfg.subscription_cleanup_history_scenes or []) if str(x).strip()}
        if cleanup_type in ("", "none") or "completed" not in scenes:
            logger.debug(
                "  ➜ [订阅清理] 《%s》S%s 配置为保留历史或未启用订阅完成场景，跳过。",
                self._series_title(tmdb_id, info),
                season or "-",
            )
            return
        if _safe_int(info.get("best_version_full")) != 1:
            logger.debug(
                "  ➜ [订阅清理] 《%s》S%s 不是分集转全集洗版订阅完成，跳过。",
                self._series_title(tmdb_id, info),
                season or "-",
            )
            return

        seasons = []
        if cleanup_type == "current":
            if season <= 0:
                logger.info("  ➜ [订阅清理] 《%s》订阅完成事件缺少季号，无法清理当前季。", self._series_title(tmdb_id, info))
                return
            seasons = [int(season)]
        elif cleanup_type == "tmdb":
            seasons = self._local_seasons_for_tmdb(tmdb_id)
            if not seasons and season > 0:
                seasons = [int(season)]
        else:
            logger.warning("  ➜ [订阅清理] 未识别的清理范围 %s，跳过。", cleanup_type)
            return

        title = self._series_title(tmdb_id, info)
        logger.info(
            "  ➜ [订阅清理] 《%s》订阅完成，按配置触发分集残留清理：范围=%s，季=%s。",
            title,
            cleanup_type,
            ",".join(f"S{s}" for s in seasons) if seasons else "全部",
        )
        spawn(
            moviepilot.smart_cleanup_mp_episode_residue,
            str(tmdb_id),
            seasons,
            title,
            self.app_config,
            True,
            True,
        )

    def _season_total_locked(self, tmdb_id: str, season: int) -> Optional[Dict[str, Any]]:
        try:
            lock_info = watchlist_db.get_series_seasons_lock_info(str(tmdb_id)).get(int(season)) or {}
            if lock_info.get("locked"):
                return lock_info
        except Exception as e:
            logger.warning("  ➜ [订阅助手] 读取《%s》S%s 集数锁定状态失败，按未锁定处理: %s", self._series_title(tmdb_id), season, e)
        return None

    def _repair_snapshot_subscription(self, snapshot: Dict[str, Any], tmdb_id: str, season: int, snapshot_total: int) -> bool:
        if snapshot_total <= 0:
            return False

        subscriptions = moviepilot.find_subscriptions(tmdb_id, season, self.app_config)
        sub = subscriptions[0] if subscriptions else None
        if not sub:
            snap_sub = snapshot.get("subscribe_json") or {}
            title = snap_sub.get("name") or snap_sub.get("title") or snap_sub.get("keyword") or tmdb_id
            logger.warning(
                "  ➜ [订阅助手] 完成快照对应的 MP 订阅已消失：《%s》S%s，正在按快照恢复订阅。",
                self._series_title(tmdb_id, snap_sub),
                season,
            )
            created = self._create_subscription(
                str(tmdb_id),
                str(title),
                season,
                {"mp_state": "R", "sources": {}, "reason": "自动纠错恢复订阅"},
            )
            if not created:
                return False
            sub = created

        current_total = _safe_int(sub.get("total_episode") or sub.get("total") or sub.get("total_episodes"))
        changed = False
        if current_total and current_total < snapshot_total:
            if moviepilot.update_subscription_status(
                int(tmdb_id),
                season,
                str(sub.get("state") or "R"),
                self.app_config,
                total_episodes=snapshot_total,
            ):
                logger.info(
                    "  ➜ [订阅助手] MP 订阅总集数低于快照，已修正：《%s》S%s %s -> %s。",
                    self._series_title(tmdb_id, sub),
                    season,
                    current_total,
                    snapshot_total,
                )
                changed = True

        subscribe_id = _safe_int(sub.get("id") or snapshot.get("subscribe_id"))
        if subscribe_id and self.cfg.auto_search_when_delete:
            if moviepilot.search_subscription(subscribe_id, self.app_config):
                changed = True
        return changed

    def mark_download_started(self, subscribe_id: int, torrent_hash: str, **metadata) -> None:
        if not subscribe_id or not torrent_hash:
            return
        now = time.time()

        def updater(data):
            data[str(torrent_hash).lower()] = {
                "hash": str(torrent_hash).lower(),
                "subscribe_id": subscribe_id,
                "baseline_progress": float(metadata.get("progress") or 0),
                "baseline_at": now,
                "retry_count": 0,
                "time": now,
                **metadata,
            }
            return data

        store.update_state(STATE_TORRENTS, updater)
        self._mark_active_source(subscribe_id, SOURCE_DOWNLOAD_PENDING, "下载已发起，等待整理入库")

    def _remember_subscription(self, subscribe_id: int, info: Dict[str, Any], reason: str = "", extra: Dict[str, Any] = None) -> None:
        if not subscribe_id:
            return

        def updater(data):
            task = data.get(str(subscribe_id), {})
            task["subscribe_id"] = subscribe_id
            task["subscribe_info"] = info or {}
            task["tmdb_id"] = str((info or {}).get("tmdbid") or (info or {}).get("tmdb_id") or task.get("tmdb_id") or "")
            task["season"] = _safe_int((info or {}).get("season") or task.get("season")) or None
            task["mp_state"] = (info or {}).get("state", task.get("mp_state"))
            task["last_event"] = reason
            task["updated_at"] = time.time()
            if extra:
                task.update(extra)
            data[str(subscribe_id)] = task
            return data

        store.update_state(STATE_SUBSCRIBES, updater)

    def _remove_subscription_state(self, subscribe_id: int, info: Dict[str, Any], reason: str = "") -> None:
        def updater(data):
            task = data.get(str(subscribe_id), {})
            task["subscribe_id"] = subscribe_id
            task["subscribe_info"] = info or task.get("subscribe_info") or {}
            task["deleted"] = True
            task["last_event"] = reason
            task["active_sources"] = {}
            task["updated_at"] = time.time()
            data[str(subscribe_id)] = task
            return data

        store.update_state(STATE_SUBSCRIBES, updater)

    def _has_recent_manual_mp_change(self, subscribe_id: int, field: str) -> bool:
        if not subscribe_id:
            return False
        data = store.read_state(STATE_SUBSCRIBES)
        task = data.get(str(subscribe_id)) if isinstance(data, dict) else {}
        change = task.get("last_manual_change") if isinstance(task, dict) else {}
        if not isinstance(change, dict):
            return False
        fields = change.get("fields") if isinstance(change.get("fields"), list) else []
        updated_at = float(change.get("updated_at") or 0)
        if field not in fields or updated_at <= 0:
            return False
        return time.time() - updated_at <= MANUAL_CHANGE_GRACE_SECONDS

    def _remember_expected_mp_update(
        self,
        subscribe_id: int,
        *,
        fields: List[str],
        expected_state: str = None,
        expected_total: int = None,
    ) -> None:
        if not subscribe_id:
            return

        def updater(data):
            task = data.get(str(subscribe_id), {})
            task["expected_mp_update"] = {
                "fields": fields or [],
                "state": expected_state,
                "total_episode": expected_total,
                "updated_at": time.time(),
            }
            data[str(subscribe_id)] = task
            return data

        store.update_state(STATE_SUBSCRIBES, updater)

    def _consume_expected_mp_update(self, subscribe_id: int, info: Dict[str, Any], fields: List[str]) -> bool:
        data = store.read_state(STATE_SUBSCRIBES)
        task = data.get(str(subscribe_id)) if isinstance(data, dict) else {}
        expected = task.get("expected_mp_update") if isinstance(task, dict) else {}
        if not isinstance(expected, dict):
            return False
        updated_at = float(expected.get("updated_at") or 0)
        if updated_at <= 0 or time.time() - updated_at > 300:
            return False
        expected_fields = set(str(x) for x in (expected.get("fields") or []))
        changed_fields = set(str(x) for x in (fields or []))
        if changed_fields and expected_fields and not changed_fields.issubset(expected_fields):
            return False
        expected_state = expected.get("state")
        if expected_state and info.get("state") != expected_state:
            return False
        expected_total = _safe_int(expected.get("total_episode"))
        if expected_total and _safe_int(info.get("total_episode")) not in (0, expected_total):
            return False

        def updater(current):
            item = current.get(str(subscribe_id), {})
            item.pop("expected_mp_update", None)
            current[str(subscribe_id)] = item
            return current

        store.update_state(STATE_SUBSCRIBES, updater)
        return True

    def _clear_torrents_for_subscription(self, subscribe_id: int, reason: str) -> int:
        if not subscribe_id:
            return 0
        removed = 0

        def updater(data):
            nonlocal removed
            for torrent_hash, task in list(data.items()):
                if _safe_int((task or {}).get("subscribe_id")) == subscribe_id:
                    data.pop(torrent_hash, None)
                    removed += 1
            return data

        store.update_state(STATE_TORRENTS, updater)
        if removed:
            logger.info("  ➜ [订阅助手] 已清理订阅 %s 的下载监控：%s，数量=%s。", subscribe_id, reason, removed)
        return removed

    def _enrich_subscribe_info(self, info: Dict[str, Any], media: Dict[str, Any], subscribe_id: int) -> Dict[str, Any]:
        enriched = dict(info or {})
        tmdb_id = str(enriched.get("tmdbid") or media.get("tmdb_id") or "").strip()
        if not tmdb_id:
            return enriched
        if _safe_int(enriched.get("season")) > 0 and enriched.get("state"):
            return enriched
        try:
            for sub in moviepilot.find_subscriptions(tmdb_id, config=self.app_config) or []:
                if _safe_int(sub.get("id")) != subscribe_id:
                    continue
                for key, value in sub.items():
                    if enriched.get(key) in (None, "", 0):
                        enriched[key] = value
                break
        except Exception as e:
            logger.debug("  ➜ [订阅助手] 反查 MP 订阅详情失败：%s -> %s", subscribe_id, e)
        return enriched

    def _completion_signal(self, *, tmdb_id, season, series_details, episodes, season_info) -> CompletionSignal:
        return evaluate_completion(
            tmdb_id=tmdb_id,
            season=season,
            series_details=series_details,
            episodes=episodes,
            season_cooldown_days=self.cfg.season_cooldown_days,
            volatility_stable=True,
        )

    def _decide_subscription_state(
        self,
        *,
        final_status: str,
        series_details: Dict[str, Any],
        season: int,
        season_episodes: List[Dict[str, Any]],
        signal: CompletionSignal,
        real_next_episode: Dict[str, Any],
    ) -> Dict[str, Any]:
        decision = {
            "mp_state": "R",
            "sources": {},
            "reason": "",
            "snapshot": False,
            "best_version": None,
            "best_version_full": None,
        }
        if final_status == "Completed":
            guard_mode = str(self.cfg.guard_mode or "balanced").lower()
            low_confidence_needs_observe = (
                guard_mode == "strict"
                or (guard_mode == "balanced" and signal.scope_total <= 3)
            )
            if guard_mode != "off" and signal.completed and signal.confidence == "low" and low_confidence_needs_observe:
                decision["mp_state"] = "P"
                decision["sources"][SOURCE_GUARD_VETO] = "低置信完结，进入完成前观察"
                decision["reason"] = decision["sources"][SOURCE_GUARD_VETO]
            else:
                decision["snapshot"] = True
                decision["reason"] = "订阅目标已完成，保存完成快照"
            return decision

        if self.cfg.pause_enabled:
            paused, reason = check_pre_air_pause(
                series_details=series_details,
                season=season,
                episodes=season_episodes,
                tv_air_days=self.cfg.tv_air_pause_days,
            )
            if paused:
                decision["mp_state"] = "S"
                decision["sources"][SOURCE_PRE_AIR] = reason
                decision["reason"] = reason
                return decision

            paused, reason = check_airing_gap_pause(
                next_episode=real_next_episode,
                pause_days=self.cfg.airing_pause_days,
                signal=signal,
            )
            if paused:
                decision["mp_state"] = "S"
                decision["sources"][SOURCE_AIRING_GAP] = reason
                decision["reason"] = reason
                return decision

        if final_status == "Pending" or self.cfg.auto_pending_enabled:
            pending, reason = should_enter_pending(
                series_details=series_details,
                season=season,
                episodes=season_episodes,
                pending_days=self.cfg.auto_pending_days,
                pending_episodes=self.cfg.auto_pending_episodes,
                use_volatility=self.cfg.pending_use_volatility,
                signal=signal,
            )
            if final_status == "Pending" or pending:
                decision["mp_state"] = "P"
                decision["sources"][SOURCE_PENDING_JUDGE] = reason or "ETK 追剧状态为待定"
                decision["reason"] = decision["sources"][SOURCE_PENDING_JUDGE]
                return decision

        if final_status == "Paused":
            decision["mp_state"] = "S"
            decision["sources"][SOURCE_AIRING_GAP] = "ETK 追剧状态为暂停"
            decision["reason"] = "ETK 追剧状态为暂停"
            return decision

        decision["mp_state"] = "R"
        decision["reason"] = "订阅可运行"
        return decision

    def _target_seasons_for_sync(self, *, valid_seasons, existing_by_season, latest_season_num, final_status):
        seasons = []
        for season in valid_seasons:
            s_num = _safe_int(season.get("season_number"))
            if s_num in existing_by_season or s_num == latest_season_num:
                seasons.append(season)
        return seasons

    def _triggering_episodes_are_virtual(self, episode_ids: List[str] = None) -> bool:
        ids = [str(x or '').strip() for x in (episode_ids or []) if str(x or '').strip()]
        if not ids:
            return False
        try:
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT asset_details_json
                        FROM media_metadata
                        WHERE emby_item_ids_json ?| %s
                        """,
                        (ids,),
                    )
                    rows = cursor.fetchall() or []
            if not rows:
                return False
            virtual_count = 0
            for row in rows:
                assets = row.get("asset_details_json") or []
                if isinstance(assets, str):
                    assets = json.loads(assets or "[]")
                if isinstance(assets, dict):
                    assets = [assets]
                is_virtual = False
                for asset in assets or []:
                    if not isinstance(asset, dict):
                        continue
                    path = asset.get("path") or asset.get("Path")
                    if p115_fp_is_virtual_strm_target(path) or p115_fp_is_virtual_strm_target(p115_fp_read_strm_target(path)):
                        is_virtual = True
                        break
                if is_virtual:
                    virtual_count += 1
            return virtual_count > 0 and virtual_count == len(rows)
        except Exception as e:
            logger.debug("  ➜ [订阅助手] 判断触发分集是否虚拟入库失败: %s", e)
            return False

    def _season_has_virtual_import(self, tmdb_id: str, season: int) -> bool:
        tmdb_id = str(tmdb_id or '').strip()
        season = _safe_int(season)
        if not tmdb_id or season <= 0:
            return False
        try:
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT 1
                        FROM shared_virtual_imports
                        WHERE status IN ('virtual', 'promoting')
                          AND LOWER(item_type) IN ('series', 'season', 'episode', 'tv')
                          AND (tmdb_id = %s OR parent_series_tmdb_id = %s)
                          AND (season_number = %s OR season_number IS NULL)
                        LIMIT 1
                        """,
                        (tmdb_id, tmdb_id, season),
                    )
                    if cursor.fetchone():
                        return True

                    cursor.execute(
                        """
                        SELECT asset_details_json
                        FROM media_metadata
                        WHERE parent_series_tmdb_id = %s
                          AND season_number = %s
                          AND item_type = 'Episode'
                          AND in_library = TRUE
                        """,
                        (tmdb_id, season),
                    )
                    rows = cursor.fetchall() or []
            for row in rows:
                assets = row.get("asset_details_json") or []
                if isinstance(assets, str):
                    try:
                        assets = json.loads(assets or "[]")
                    except Exception:
                        assets = []
                if isinstance(assets, dict):
                    assets = [assets]
                for asset in assets or []:
                    if not isinstance(asset, dict):
                        continue
                    path = asset.get("path") or asset.get("Path")
                    if p115_fp_is_virtual_strm_target(path) or p115_fp_is_virtual_strm_target(p115_fp_read_strm_target(path)):
                        return True
        except Exception as e:
            logger.debug("  ➜ [订阅助手] 判断季是否仍为虚拟入库失败：tmdb=%s, season=%s, err=%s", tmdb_id, season, e)
        return False

    def _create_subscription(self, tmdb_id: str, series_name: str, season: int, decision: Dict[str, Any]) -> Optional[dict]:
        payload_kwargs = self._subscription_wash_kwargs(decision)
        if not moviepilot.subscribe_series_to_moviepilot(
            {"title": series_name, "tmdb_id": tmdb_id},
            season,
            self.app_config,
            **payload_kwargs,
        ):
            logger.warning("  ➜ [订阅助手] 《%s》S%s 自动补订失败。", series_name, season)
            return None
        subscriptions = moviepilot.find_subscriptions(tmdb_id, season, self.app_config)
        return subscriptions[0] if subscriptions else {"tmdbid": tmdb_id, "season": season}

    def _subscription_wash_kwargs(self, decision: Dict[str, Any]) -> Dict[str, Optional[int]]:
        if decision.get("completed_full_washing"):
            return {"best_version": 1, "best_version_full": 1}
        if self.cfg.best_version_type in ("tv", "all"):
            return {"best_version": 1, "best_version_full": 1}
        if self.cfg.best_version_type == "tv_episode":
            return {"best_version": 1, "best_version_full": None}
        return {"best_version": None, "best_version_full": None}

    def _sync_completed_full_washing(
        self,
        *,
        tmdb_id: str,
        series_name: str,
        season: int,
        subscribe: Dict[str, Any],
        season_info: Dict[str, Any],
        signal: CompletionSignal,
    ) -> None:
        if not self.cfg.best_version_episode_to_full:
            return
        if self.cfg.best_version_type != "tv_episode":
            return

        expected_count = _safe_int(season_info.get("episode_count")) or _safe_int(signal.scope_total)
        if expected_count <= 0:
            logger.info("  ➜ [订阅助手] 《%s》S%s 总集数未知，跳过全集洗版门禁。", series_name, season)
            return

        if self.cfg.best_version_full_consistency_check_enabled:
            if self._season_consistency_ok(tmdb_id, season, expected_count, series_name):
                self._set_season_active_washing(tmdb_id, season, False, "一致性通过，不提交全集洗版。")
                logger.info(
                    "  ➜ [订阅助手] 《%s》S%s 一致性已通过，跳过分集转全集洗版。",
                    series_name,
                    season,
                )
                return
            self._set_season_active_washing(tmdb_id, season, True, "一致性不通过，提交全集洗版并等待收口。")

        if _safe_int((subscribe or {}).get("best_version_full")) == 1:
            logger.debug("  ➜ [订阅助手] 《%s》S%s 已是全集洗版订阅，跳过重复更新。", series_name, season)
            return

        payload = dict(subscribe or {})
        if not payload.get("id"):
            logger.debug("  ➜ [订阅助手] 《%s》S%s 未找到可更新的 MP 订阅，跳过全集洗版。", series_name, season)
            return
        payload["tmdbid"] = int(tmdb_id)
        payload["season"] = int(season)
        payload["name"] = payload.get("name") or series_name
        payload["type"] = payload.get("type") or "电视剧"
        payload["best_version"] = 1
        payload["best_version_full"] = 1

        if moviepilot.update_subscription(payload, self.app_config):
            logger.info("  ➜ [订阅助手] 《%s》S%s 已提交分集转全集洗版订阅。", series_name, season)
        else:
            logger.warning("  ➜ [订阅助手] 《%s》S%s 分集转全集洗版订阅更新失败。", series_name, season)

    def _season_consistency_ok(self, tmdb_id: str, season: int, expected_count: int, series_name: str) -> bool:
        try:
            result = helpers.check_season_consistency(
                tmdb_id=str(tmdb_id),
                season_number=int(season),
                expected_episode_count=int(expected_count),
                series_name=series_name,
            )
            return bool(result.get("ok"))
        except Exception as e:
            logger.warning("  ➜ [订阅助手] 《%s》S%s 一致性校验失败，按不通过处理：%s", series_name, season, e)
            return False

    def _set_season_active_washing(self, tmdb_id: str, season: int, enabled: bool, reason: str = "") -> None:
        title = self._series_title(tmdb_id)
        try:
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE media_metadata
                        SET active_washing = %s
                        WHERE parent_series_tmdb_id = %s
                          AND season_number = %s
                          AND item_type IN ('Season', 'Episode')
                        """,
                        (bool(enabled), str(tmdb_id), int(season)),
                    )
                    conn.commit()
            action = "开启" if enabled else "清理"
            logger.info("  ➜ [订阅助手] 已%s《%s》S%s active_washing：%s", action, title, season, reason or "-")
        except Exception as e:
            logger.warning("  ➜ [订阅助手] 设置 active_washing 失败：《%s》S%s -> %s", title, season, e)

    def _local_seasons_for_tmdb(self, tmdb_id: str) -> List[int]:
        try:
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT DISTINCT season_number
                        FROM media_metadata
                        WHERE parent_series_tmdb_id = %s
                          AND season_number IS NOT NULL
                          AND season_number > 0
                          AND item_type IN ('Season', 'Episode')
                        ORDER BY season_number ASC
                        """,
                        (str(tmdb_id),),
                    )
                    rows = cursor.fetchall() or []
            return [int(row.get("season_number")) for row in rows if _safe_int(row.get("season_number")) > 0]
        except Exception as e:
            logger.warning("  ➜ [订阅清理] 查询《%s》本地季号失败：%s", self._series_title(tmdb_id), e)
            return []

    def _target_total(self, decision: Dict[str, Any], season_info: Dict[str, Any], signal: CompletionSignal) -> Optional[int]:
        if decision["mp_state"] == "P":
            return self.cfg.pending_fake_total_episodes
        total = _safe_int(season_info.get("episode_count"))
        if total > 0:
            return total
        if signal.scope_total > 0:
            return signal.scope_total
        return None

    def _update_source_state(self, subscribe_id: int, decision: Dict[str, Any]) -> None:
        if not subscribe_id:
            return
        active_sources = decision.get("sources") or {}

        def updater(data):
            task = data.get(str(subscribe_id), {})
            task["active_sources"] = active_sources
            task["last_reason"] = decision.get("reason")
            task["updated_at"] = time.time()
            data[str(subscribe_id)] = task
            return data

        store.update_state(STATE_SUBSCRIBES, updater)

    def _mark_active_source(self, subscribe_id: int, source: str, reason: str) -> None:
        def updater(data):
            task = data.get(str(subscribe_id), {})
            sources = task.get("active_sources") or {}
            sources[source] = reason
            task["active_sources"] = sources
            task["updated_at"] = time.time()
            data[str(subscribe_id)] = task
            return data

        store.update_state(STATE_SUBSCRIBES, updater)

    def _clear_download_pending(self, subscribe_id: int, torrent_hash: str, reason: str) -> None:
        if not subscribe_id:
            return

        def updater(data):
            task = data.get(str(subscribe_id), {})
            sources = task.get("active_sources") or {}
            sources.pop(SOURCE_DOWNLOAD_PENDING, None)
            task["active_sources"] = sources
            task["last_reason"] = reason
            task["updated_at"] = time.time()
            data[str(subscribe_id)] = task
            return data

        store.update_state(STATE_SUBSCRIBES, updater)

    def _parse_subscribe_source(self, source: Any) -> Dict[str, Any]:
        text = str(source or "")
        if "|" not in text:
            return {}
        prefix, raw = text.split("|", 1)
        if prefix != "Subscribe":
            return {}
        try:
            value = json.loads(raw)
            return value if isinstance(value, dict) else {}
        except Exception:
            return {}

    def _format_subscribe_info(self, info: Dict[str, Any]) -> str:
        if not isinstance(info, dict):
            return "-"
        tmdb_id = info.get("tmdbid") or info.get("tmdb_id") or "-"
        title = self._series_title(str(tmdb_id), info)
        season = info.get("season")
        state = info.get("state")
        season_text = f"S{season}" if season not in (None, "", 0) else "全局"
        state_text = f"，状态={state}" if state else ""
        return f"{title}({tmdb_id}) {season_text}{state_text}"

    def _series_title(self, tmdb_id: Any, info: Dict[str, Any] = None) -> str:
        info = info if isinstance(info, dict) else {}
        tmdb_id = str(tmdb_id or info.get("tmdbid") or info.get("tmdb_id") or "").strip()
        for key in ("name", "title", "keyword"):
            title = str(info.get(key) or "").strip()
            if title and title.lower() not in ("none", "null") and title != tmdb_id:
                return title

        if not tmdb_id:
            return "-"
        if tmdb_id in self._title_cache:
            return self._title_cache[tmdb_id] or tmdb_id

        title = ""
        try:
            title = watchlist_db.get_watchlist_item_name(tmdb_id) or ""
        except Exception:
            title = ""
        if not title:
            try:
                with connection.get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            """
                            SELECT title
                            FROM media_metadata
                            WHERE tmdb_id = %s AND item_type IN ('Series', 'Movie')
                            ORDER BY CASE WHEN item_type = 'Series' THEN 0 ELSE 1 END
                            LIMIT 1
                            """,
                            (tmdb_id,),
                        )
                        row = cursor.fetchone() or {}
                        title = str(row.get("title") or "").strip()
            except Exception:
                title = ""

        self._title_cache[tmdb_id] = title or tmdb_id
        return self._title_cache[tmdb_id]

    def _delete_fingerprint(self, task: Dict[str, Any]) -> str:
        raw = "|".join([
            str(task.get("tmdb_id") or ""),
            str(task.get("season") or ""),
            ",".join(str(x) for x in (task.get("episodes") or [])),
            str(task.get("title") or ""),
            str(task.get("enclosure") or ""),
            str(task.get("page_url") or ""),
        ])
        return hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()


def _progress_value(info: Dict[str, Any]) -> float:
    for key in ("progress", "percent", "completed"):
        value = info.get(key)
        try:
            number = float(value)
            return number * 100 if 0 <= number <= 1 else number
        except (TypeError, ValueError):
            pass
    return 0.0


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
