from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional


@dataclass
class CompletionSignal:
    completed: bool = False
    confidence: str = "none"
    stable: bool = True
    cadence_expired: bool = False
    signals: List[str] = field(default_factory=list)
    reason: str = ""
    scope_total: int = 0
    scope_high_risk: bool = False
    volatility_direction: Optional[str] = None
    volatility_detail: Optional[str] = None


@dataclass
class SeasonScope:
    tmdb_id: str = ""
    season: int = 0
    episodes: List[Dict[str, Any]] = field(default_factory=list)
    total: int = 0
    high_risk: bool = False


def parse_date(value: Any) -> Optional[date]:
    if not value:
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def field(data: Any, name: str, default=None):
    if isinstance(data, dict):
        return data.get(name, default)
    return getattr(data, name, default)


def episode_number(episode: Any) -> Optional[int]:
    try:
        return int(field(episode, "episode_number"))
    except (TypeError, ValueError):
        return None


def episode_air_date(episode: Any) -> Optional[date]:
    return parse_date(field(episode, "air_date"))


def build_scope(tmdb_id: str, season: int, episodes: List[Dict[str, Any]]) -> SeasonScope:
    scope_episodes = [
        ep for ep in episodes or []
        if _safe_int(ep.get("season_number")) == int(season or 0)
    ]
    scope_episodes.sort(key=lambda ep: (_safe_int(ep.get("episode_number")), ep.get("air_date") or ""))
    scope = SeasonScope(
        tmdb_id=str(tmdb_id),
        season=int(season or 0),
        episodes=scope_episodes,
        total=len(scope_episodes),
    )
    scope.high_risk = detect_high_risk(scope)
    return scope


def detect_high_risk(scope: SeasonScope) -> bool:
    if len(scope.episodes) >= 40:
        return True
    for ep in scope.episodes[:-1]:
        if field(ep, "episode_type") == "mid_season":
            return True
    finale = [ep for ep in scope.episodes if field(ep, "episode_type") == "finale"]
    return bool(len(finale) == 1 and finale[0] is not scope.episodes[-1])


def last_aired_episode(episodes: List[Dict[str, Any]], as_of: Optional[date] = None):
    today = as_of or date.today()
    aired = []
    for ep in episodes or []:
        air = episode_air_date(ep)
        if air and air <= today:
            aired.append((air, episode_number(ep) or 0, ep))
    if not aired:
        return None
    return max(aired, key=lambda item: (item[0], item[1]))[2]


def all_scope_episodes_aired(scope: SeasonScope, as_of: Optional[date] = None) -> bool:
    if not scope.episodes:
        return False
    today = as_of or date.today()
    for ep in scope.episodes:
        air = episode_air_date(ep)
        if not air or air > today:
            return False
    return True


def scope_future_episode(scope: SeasonScope, as_of: Optional[date] = None):
    today = as_of or date.today()
    last = last_aired_episode(scope.episodes, today)
    last_num = episode_number(last) if last else None
    candidates = []
    for ep in scope.episodes or []:
        num = episode_number(ep)
        air = episode_air_date(ep)
        if air and air > today:
            candidates.append(ep)
        elif air is None and num is not None and last_num is not None and num > last_num:
            candidates.append(ep)
    if not candidates:
        return None
    return min(candidates, key=lambda ep: (episode_air_date(ep) or date.max, episode_number(ep) or 0))


def has_scope_future_episode(scope: SeasonScope, as_of: Optional[date] = None) -> bool:
    return scope_future_episode(scope, as_of) is not None


def has_scope_finale(scope: SeasonScope, as_of: Optional[date] = None) -> bool:
    finale = [ep for ep in scope.episodes if field(ep, "episode_type") == "finale"]
    if len(finale) != 1 or finale[0] is not scope.episodes[-1]:
        return False
    air = episode_air_date(finale[0])
    return bool(air and air <= (as_of or date.today()))


def evaluate_completion(
    *,
    tmdb_id: str,
    season: int,
    series_details: Dict[str, Any],
    episodes: List[Dict[str, Any]],
    season_cooldown_days: int = 14,
    volatility_stable: bool = True,
    volatility_detail: str = "",
    volatility_direction: str = None,
    as_of: Optional[date] = None,
) -> CompletionSignal:
    today = as_of or date.today()
    scope = build_scope(tmdb_id, season, episodes)

    m_sig = _check_m_signal(scope, today)
    if m_sig:
        return _attach_scope(m_sig, scope)

    e_sig = _check_e_signal(series_details, scope, today)
    if not volatility_stable:
        if e_sig and e_sig.confidence == "high" and "E:finale" in e_sig.signals and not has_scope_future_episode(scope, today):
            return _attach_scope(e_sig, scope)
        return _attach_scope(CompletionSignal(
            completed=False,
            stable=False,
            signals=["F:unstable"],
            reason=volatility_detail or "目标总集数近期发生变化",
            volatility_direction=volatility_direction,
            volatility_detail=volatility_detail,
        ), scope)

    if e_sig:
        return _attach_scope(e_sig, scope)

    i_sig = _check_i_signal(series_details, scope, season_cooldown_days, today)
    if i_sig:
        return _attach_scope(i_sig, scope)

    cadence_expired = False
    if scope.high_risk:
        cadence_expired = all_scope_episodes_aired(scope, today) and not has_scope_future_episode(scope, today)

    return _attach_scope(CompletionSignal(
        completed=False,
        stable=True,
        cadence_expired=cadence_expired,
        signals=["none"],
        reason="无信号确认当前目标范围已播完",
    ), scope)


def should_enter_pending(
    *,
    series_details: Dict[str, Any],
    season: int,
    episodes: List[Dict[str, Any]],
    pending_days: int,
    pending_episodes: int,
    use_volatility: bool,
    signal: CompletionSignal = None,
    as_of: Optional[date] = None,
) -> tuple[bool, str]:
    today = as_of or date.today()
    air_date = _season_air_date(series_details, season) or parse_date(series_details.get("first_air_date"))
    if pending_days and air_date and air_date + timedelta(days=int(pending_days)) > today:
        return True, f"开播日期 {air_date.isoformat()}，仍在开播待定窗口内"

    ep_count = len(episodes or [])
    if pending_episodes and ep_count <= int(pending_episodes):
        return True, f"集数不足（{ep_count} <= {pending_episodes}）"

    if use_volatility and signal and not signal.stable:
        return True, f"目标总集数近期变化{f'（{signal.volatility_detail}）' if signal.volatility_detail else ''}"

    if episodes and not any(ep.get("air_date") for ep in episodes):
        return True, "本季无任何 air_date 信息"

    return False, ""


def check_pre_air_pause(
    *,
    series_details: Dict[str, Any],
    season: int,
    episodes: List[Dict[str, Any]],
    tv_air_days: int,
    as_of: Optional[date] = None,
) -> tuple[bool, str]:
    if not tv_air_days:
        return False, ""
    today = as_of or date.today()
    air = _season_air_date(series_details, season)
    if air is None:
        air = _first_episode_air_date(episodes)
    if air is None:
        return True, "开播日期未知"
    if today < air - timedelta(days=int(tv_air_days)):
        return True, f"开播日期 {air.isoformat()}，暂未到订阅窗口"
    return False, ""


def check_airing_gap_pause(
    *,
    next_episode: Dict[str, Any],
    pause_days: int,
    signal: CompletionSignal,
    as_of: Optional[date] = None,
) -> tuple[bool, str]:
    if not pause_days or (signal and signal.completed):
        return False, ""
    air = parse_date((next_episode or {}).get("air_date"))
    if not air:
        return False, ""
    today = as_of or date.today()
    days_until = (air - today).days
    if days_until > int(pause_days):
        return True, f"下一集日期 {air.isoformat()}，距离 {days_until} 天"
    return False, ""


def _check_m_signal(scope: SeasonScope, today: date) -> Optional[CompletionSignal]:
    last = last_aired_episode(scope.episodes, today)
    if last and field(last, "episode_type") == "mid_season":
        return CompletionSignal(completed=False, stable=True, signals=["M:mid_season"], reason="最后已播集为 mid_season")
    return None


def _check_e_signal(series_details: Dict[str, Any], scope: SeasonScope, today: date) -> Optional[CompletionSignal]:
    status = series_details.get("status") or ""
    if status in {"Ended", "Canceled"}:
        return CompletionSignal(completed=True, confidence="high", signals=[f"E:{status.lower()}"], reason=f"status={status}")
    if has_scope_finale(scope, today) and not has_scope_future_episode(scope, today):
        return CompletionSignal(completed=True, confidence="high", signals=["E:finale"], reason="目标范围末集有 finale 标记")
    return None


def _check_i_signal(series_details: Dict[str, Any], scope: SeasonScope, cooldown_days: int, today: date) -> Optional[CompletionSignal]:
    if has_scope_future_episode(scope, today):
        return None
    for season in series_details.get("seasons") or []:
        if _safe_int(season.get("season_number")) > scope.season:
            return CompletionSignal(completed=True, confidence="medium", signals=["I:next_season"], reason=f"TMDB 存在 S{season.get('season_number')}")
    last_ep = series_details.get("last_episode_to_air") or {}
    last_season = _safe_int(last_ep.get("season_number"))
    if last_season > scope.season:
        return CompletionSignal(completed=True, confidence="medium", signals=["I:last_ep_beyond"], reason=f"last_episode_to_air 属于 S{last_season}")
    if scope.high_risk:
        return None
    if all_scope_episodes_aired(scope, today):
        return CompletionSignal(completed=True, confidence="low", signals=["I:all_aired"], reason="目标范围内所有集已播且无后续集反证")
    last = last_aired_episode(scope.episodes, today)
    air = episode_air_date(last) if last else None
    if air and (today - air).days > int(cooldown_days):
        return CompletionSignal(completed=True, confidence="low", signals=["I:cooldown"], reason=f"最后集播出超 {cooldown_days} 天")
    return None


def _attach_scope(signal: CompletionSignal, scope: SeasonScope) -> CompletionSignal:
    signal.scope_total = scope.total
    signal.scope_high_risk = scope.high_risk
    return signal


def _season_air_date(series_details: Dict[str, Any], season_number: int) -> Optional[date]:
    for season in series_details.get("seasons") or []:
        if _safe_int(season.get("season_number")) == int(season_number or 0):
            return parse_date(season.get("air_date"))
    return None


def _first_episode_air_date(episodes: List[Dict[str, Any]]) -> Optional[date]:
    dates = [episode_air_date(ep) for ep in episodes or []]
    dates = [d for d in dates if d]
    return min(dates) if dates else None


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

