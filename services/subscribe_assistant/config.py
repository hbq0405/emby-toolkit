from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class AssistantConfig:
    enabled: bool = True
    notify: bool = True
    guard_mode: str = "balanced"
    season_cooldown_days: int = 14
    volatility_enabled: bool = True
    volatility_window_days: int = 14
    auto_pending_enabled: bool = True
    auto_pending_days: int = 7
    auto_pending_episodes: int = 5
    pending_use_volatility: bool = False
    pending_fake_total_episodes: int = 99
    pause_enabled: bool = True
    airing_pause_days: int = 30
    tv_air_pause_days: int = 14
    tv_no_download_days: int = 0
    no_download_actions: List[str] = None
    download_monitor_enabled: bool = False
    manual_delete_listen: bool = True
    tracker_response_listen: bool = True
    auto_search_when_delete: bool = True
    skip_deletion: bool = True
    download_timeout_minutes: int = 120
    download_progress_threshold: int = 10
    download_retry_limit: int = 3
    delete_record_retention_hours: int = 24
    delete_exclude_tags: List[str] = None
    tracker_keywords: List[str] = None
    best_version_type: str = "tv"
    best_version_backfill_enabled: bool = False
    best_version_episode_to_full: bool = True
    best_version_full_consistency_check_enabled: bool = True
    subscription_cleanup_history_type: str = "none"
    subscription_cleanup_history_scenes: List[str] = None
    verify_enabled: bool = True
    verify_interval_hours: int = 12
    snapshot_retention_days: int = 180
    recognition_guard_enabled: bool = False
    recognition_guard_mode: str = "audit"

    def __post_init__(self):
        if self.no_download_actions is None:
            self.no_download_actions = []
        if self.delete_exclude_tags is None:
            self.delete_exclude_tags = ["H&R"]
        if self.tracker_keywords is None:
            self.tracker_keywords = [
                "torrent not registered with this tracker",
                "torrent banned",
            ]
        if self.subscription_cleanup_history_scenes is None:
            self.subscription_cleanup_history_scenes = ["completed"]


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "启用", "开启"}
    return bool(value)


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    if isinstance(value, str):
        return [x.strip() for x in value.replace("\n", ",").split(",") if x.strip()]
    return [str(value).strip()] if str(value).strip() else []


def from_watchlist_config(raw: Dict[str, Any] = None) -> AssistantConfig:
    raw = raw or {}
    assistant = raw.get("subscribe_assistant") if isinstance(raw.get("subscribe_assistant"), dict) else {}
    auto_pending = raw.get("auto_pending") if isinstance(raw.get("auto_pending"), dict) else {}

    cfg = AssistantConfig()
    cfg.enabled = _as_bool(assistant.get("enabled"), True)
    cfg.guard_mode = str(assistant.get("guard_mode", "balanced") or "balanced")
    cfg.season_cooldown_days = _as_int(assistant.get("season_cooldown_days"), cfg.season_cooldown_days)
    cfg.volatility_enabled = _as_bool(assistant.get("volatility_enabled"), cfg.volatility_enabled)
    cfg.volatility_window_days = _as_int(assistant.get("volatility_window_days"), cfg.volatility_window_days)

    cfg.auto_pending_enabled = _as_bool(
        assistant.get("pending_enhanced_enabled", auto_pending.get("enabled")),
        _as_bool(auto_pending.get("enabled"), cfg.auto_pending_enabled),
    )
    cfg.auto_pending_days = _as_int(
        assistant.get("auto_tv_pending_days", auto_pending.get("days")),
        cfg.auto_pending_days,
    )
    cfg.auto_pending_episodes = _as_int(
        assistant.get("auto_tv_pending_episodes", auto_pending.get("episodes")),
        cfg.auto_pending_episodes,
    )
    cfg.pending_use_volatility = _as_bool(assistant.get("pending_use_volatility"), cfg.pending_use_volatility)
    cfg.pending_fake_total_episodes = _as_int(
        assistant.get("pending_fake_total_episodes", auto_pending.get("default_total_episodes")),
        cfg.pending_fake_total_episodes,
    )

    cfg.pause_enabled = _as_bool(assistant.get("pause_enhanced_enabled"), True)
    cfg.airing_pause_days = _as_int(assistant.get("airing_pause_days", raw.get("auto_pause")), cfg.airing_pause_days)
    cfg.tv_air_pause_days = _as_int(assistant.get("tv_air_pause_days"), cfg.tv_air_pause_days)
    cfg.tv_no_download_days = _as_int(assistant.get("tv_no_download_days"), cfg.tv_no_download_days)
    cfg.no_download_actions = _as_list(assistant.get("no_download_actions"))

    cfg.download_monitor_enabled = _as_bool(assistant.get("download_monitor_enabled"), cfg.download_monitor_enabled)
    cfg.manual_delete_listen = _as_bool(assistant.get("manual_delete_listen"), cfg.manual_delete_listen)
    cfg.tracker_response_listen = _as_bool(assistant.get("tracker_response_listen"), cfg.tracker_response_listen)
    cfg.auto_search_when_delete = _as_bool(assistant.get("auto_search_when_delete"), cfg.auto_search_when_delete)
    cfg.skip_deletion = _as_bool(assistant.get("skip_deletion"), cfg.skip_deletion)
    cfg.download_timeout_minutes = _as_int(assistant.get("download_timeout_minutes"), cfg.download_timeout_minutes)
    cfg.download_progress_threshold = _as_int(assistant.get("download_progress_threshold"), cfg.download_progress_threshold)
    cfg.download_retry_limit = _as_int(assistant.get("download_retry_limit"), cfg.download_retry_limit)
    cfg.delete_record_retention_hours = _as_int(assistant.get("delete_record_retention_hours"), cfg.delete_record_retention_hours)
    cfg.delete_exclude_tags = _as_list(assistant.get("delete_exclude_tags")) or cfg.delete_exclude_tags
    cfg.tracker_keywords = _as_list(assistant.get("tracker_keywords")) or cfg.tracker_keywords

    cfg.best_version_type = str(assistant.get("best_version_type") or cfg.best_version_type)
    cfg.best_version_backfill_enabled = _as_bool(assistant.get("best_version_backfill_enabled"), cfg.best_version_backfill_enabled)
    cfg.best_version_episode_to_full = _as_bool(assistant.get("best_version_episode_to_full"), cfg.best_version_episode_to_full)
    cfg.best_version_full_consistency_check_enabled = _as_bool(
        assistant.get("best_version_full_consistency_check_enabled"),
        cfg.best_version_full_consistency_check_enabled,
    )
    cfg.subscription_cleanup_history_type = str(
        assistant.get("subscription_cleanup_history_type") or cfg.subscription_cleanup_history_type
    )
    cfg.subscription_cleanup_history_scenes = (
        _as_list(assistant.get("subscription_cleanup_history_scenes"))
        or cfg.subscription_cleanup_history_scenes
    )
    cfg.verify_enabled = _as_bool(assistant.get("verify_enabled"), cfg.verify_enabled)
    cfg.verify_interval_hours = _as_int(assistant.get("verify_interval_hours"), cfg.verify_interval_hours)
    cfg.snapshot_retention_days = _as_int(assistant.get("snapshot_retention_days"), cfg.snapshot_retention_days)
    cfg.recognition_guard_enabled = _as_bool(assistant.get("recognition_guard_enabled"), cfg.recognition_guard_enabled)
    cfg.recognition_guard_mode = str(assistant.get("recognition_guard_mode") or cfg.recognition_guard_mode)
    return cfg
