import copy
import logging
import re
import threading
import time
from typing import Any, Dict, Iterable, List, Optional

from utils import DEFAULT_TG_REGEX

logger = logging.getLogger(__name__)

TG_CANDIDATE_PARSE_VERSION = "v1"
_CANDIDATE_HINT_TTL_SECONDS = 6 * 60 * 60
_CANDIDATE_HINT_REGISTRY: List[Dict[str, Any]] = []
_CANDIDATE_HINT_LOCK = threading.Lock()
_LOOKUP_KEYS_BLOCKED = object()

_QUALITY_WORDS = (
    "WEB-DL", "WEBRIP", "BLURAY", "REMUX", "HDR", "DV", "DDP",
    "HEVC", "H265", "H.265", "X265", "X264", "内嵌", "外挂",
    "中字", "简中", "繁中", "ATMOS", "TRUEHD", "DTS", "AAC",
)
_PLATFORM_PATTERNS = (
    ("NETFLIX", r"\b(?:NETFLIX|NF)\b"),
    ("AMZN", r"\b(?:AMZN|AMAZON)\b"),
    ("DSNP", r"\b(?:DSNP|DISNEY\+?)\b"),
    ("HULU", r"\bHULU\b"),
    ("MAX", r"\b(?:HMAX|MAX)\b"),
    ("ATVP", r"\b(?:ATVP|APPLE\s*TV\+?)\b"),
    ("WEB", r"\bWEB(?:-DL|RIP)?\b"),
)
_TITLE_NOISE_PATTERN = re.compile(
    r"(?i)\b("
    r"WEB[- ]?DL|WEBRIP|BLURAY|REMUX|HDR10\+?|HDR|DV|DOVI|ATMOS|"
    r"HEVC|H\.?265|X265|H\.?264|X264|DDP\d?(?:\.\d)?|TRUEHD|DTS(?:-HD)?|AAC\d?(?:\.\d)?|"
    r"2160P|1080P|720P|480P|4K|8K|10BIT|8BIT|中字|内嵌|外挂|简中|繁中|双语|国粤|"
    r"COMPLETE|SEASON|PACK|EPISODE|OVA|OAD|SP|SPECIALS?|番外|特别篇|特別篇"
    r")\b"
)


def channel_rule_matches(target_channel, chat_username="", chat_id=""):
    target_channel = str(target_channel or "").strip().lower()
    if not target_channel:
        return True

    chat_username = str(chat_username or "").strip().lower().lstrip("@")
    chat_id = str(chat_id or "").strip()
    target_clean = target_channel.lstrip("@")
    target_id_clean = target_clean.replace("-100", "") if target_clean.startswith("-100") else target_clean
    curr_id_clean = chat_id.replace("-100", "") if chat_id.startswith("-100") else chat_id

    return (
        chat_username == target_clean
        or chat_id == target_channel
        or curr_id_clean == target_id_clean
    )


def apply_channel_regex(text, custom_rules, default_rules, chat_username="", chat_id="", flags=re.IGNORECASE):
    text = str(text or "")
    applicable_rules = []

    for rule_obj in (custom_rules or []):
        if isinstance(rule_obj, str):
            applicable_rules.append(rule_obj)
            continue

        pattern = str((rule_obj or {}).get("pattern", "")).strip()
        target_channel = str((rule_obj or {}).get("channel", "")).strip().lower()
        if not pattern:
            continue
        if channel_rule_matches(target_channel, chat_username=chat_username, chat_id=chat_id):
            applicable_rules.append(pattern)

    rules = applicable_rules + list(default_rules or [])
    for rule in rules:
        if not rule or not str(rule).strip():
            continue
        try:
            match = re.search(rule, text, flags)
            if match:
                return match
        except Exception as e:
            logger.error(f"  ➜ [TG Candidate] 正则执行错误: {rule} -> {e}")
    return None


def normalize_text(text):
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _normalize_url_key(value):
    value = str(value or "").strip()
    if not value:
        return ""
    return re.sub(r"(?<!:)/{2,}", "//", value.rstrip("/")).lower()


def _extract_share_code_from_text(text):
    match = re.search(r"115(?:cdn)?\.com/s/([a-zA-Z0-9]+)", str(text or ""), re.IGNORECASE)
    return match.group(1).lower() if match else ""


def _extract_message_lookup_key(source_chat_id="", source_username="", message_id=None):
    message_id = str(message_id or "").strip()
    if not message_id:
        return ""
    source_chat_id = str(source_chat_id or "").strip()
    if source_chat_id:
        return f"msg:{source_chat_id}:{message_id}"
    source_username = str(source_username or "").strip().lower().lstrip("@")
    if source_username:
        return f"msg:{source_username}:{message_id}"
    return ""


def _collect_lookup_keys(target_link="", magnet_url="", receive_code="", source_chat_id="", source_username="", message_id=None):
    keys = []

    def _append(value):
        value = str(value or "").strip()
        if value and value not in keys:
            keys.append(value)

    target_link = str(target_link or "").strip()
    if target_link:
        _append(f"target:{_normalize_url_key(target_link)}")
        share_code = _extract_share_code_from_text(target_link)
        if share_code:
            _append(f"share:{share_code}")
            receive_code = str(receive_code or "").strip().lower()
            if receive_code:
                _append(f"sharepwd:{share_code}:{receive_code}")

    magnet_url = str(magnet_url or "").strip()
    if magnet_url:
        _append(f"magnet:{magnet_url.lower()}")

    msg_key = _extract_message_lookup_key(
        source_chat_id=source_chat_id,
        source_username=source_username,
        message_id=message_id,
    )
    if msg_key:
        _append(msg_key)

    return keys


def _expand_lookup_keys(*values):
    keys = []
    for value in values:
        if isinstance(value, (list, tuple, set)):
            for item in value:
                for expanded in _expand_lookup_keys(item):
                    if expanded not in keys:
                        keys.append(expanded)
            continue

        value = str(value or "").strip()
        if not value:
            continue

        if value.startswith(("target:", "share:", "sharepwd:", "magnet:", "msg:")):
            normalized = value
            if value.startswith("target:"):
                normalized = f"target:{_normalize_url_key(value.split(':', 1)[1])}"
            else:
                normalized = value.lower()
            if normalized not in keys:
                keys.append(normalized)
            continue

        expanded = _collect_lookup_keys(target_link=value)
        if expanded:
            for item in expanded:
                if item not in keys:
                    keys.append(item)
            continue

        normalized = value.lower()
        if normalized not in keys:
            keys.append(normalized)
    return keys


def extract_message_urls(message) -> List[str]:
    urls = []
    entities = getattr(getattr(message, "message", message), "entities", None)
    if entities:
        for entity in entities:
            url = getattr(entity, "url", None)
            if url:
                urls.append(url)

    reply_markup = getattr(getattr(message, "message", message), "reply_markup", None)
    if reply_markup and hasattr(reply_markup, "rows"):
        for row in reply_markup.rows:
            for button in getattr(row, "buttons", []) or []:
                url = getattr(button, "url", None)
                if url:
                    urls.append(url)

    seen = set()
    deduped = []
    for url in urls:
        if not url or url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped


def guess_size_text(text):
    match = re.search(r"(\d+(?:\.\d+)?)\s*(TB|GB|G|MB|M)\b", str(text or ""), re.IGNORECASE)
    if not match:
        return ""
    value, unit = match.group(1), match.group(2).upper()
    if unit == "G":
        unit = "GB"
    elif unit == "M":
        unit = "MB"
    return f"{value}{unit}"


def guess_resolution(text):
    upper = str(text or "").upper()
    for token in ("8K", "4K", "2160P", "1080P", "720P"):
        if token in upper:
            return "4K" if token == "2160P" else token
    return ""


def guess_quality_text(text):
    lines = [re.sub(r"\s+", " ", line).strip() for line in str(text or "").splitlines() if line.strip()]
    for line in lines:
        upper = line.upper()
        if any(word.upper() in upper for word in _QUALITY_WORDS):
            return line[:120]
    return lines[0][:120] if lines else ""


def guess_title_from_text(text):
    for line in str(text or "").splitlines():
        line = re.sub(r"[#*_`>\[\]【】]+", " ", line).strip()
        line = re.sub(r"^[^\w\u4e00-\u9fa5]+", "", line).strip()
        if line:
            return line[:80]
    return ""


def normalize_title_for_match(value):
    text = str(value or "").lower()
    return re.sub(r"[\s\-_·.．・:：,，;；!！?？()\[\]【】{}<>《》\"“”'’‘`~～/\\|]+", "", text)


def extract_explicit_tmdb_id(text):
    text = str(text or "")
    patterns = [
        r"\bTMDB\s*(?:ID|Id|id)?\s*[:：#-]?\s*(\d{2,10})\b",
        r"\{\s*tmdb-(\d{2,10})\s*\}",
        r"\btmdb-(\d{2,10})\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def extract_year_candidates(text):
    years = set()
    for match in re.finditer(r"(?<!\d)((?:19|20)\d{2})(?!\d)", str(text or "")):
        try:
            years.add(int(match.group(1)))
        except Exception:
            pass
    return years


def channel_text_matches_year(text, expected_year):
    expected_year = str(expected_year or "").strip()
    if not expected_year:
        return True
    try:
        expected = int(expected_year[:4])
    except Exception:
        return True
    return expected in extract_year_candidates(text)


def channel_title_candidate_lines(text):
    candidates = []
    lines = [re.sub(r"\s+", " ", line).strip() for line in str(text or "").splitlines() if line.strip()]
    title_patterns = [
        r"(?:^|[\[【📺🎬🎥🎞️ ]+)(?:电影|影片|剧集|电视剧|番剧|动漫|片名|标题|名称)\s*[:：]\s*(.+)$",
        r"^[\[【]([^\]】]{2,80})[\]】]",
    ]
    for line in lines[:12]:
        clean = line.strip()
        for pattern in title_patterns:
            match = re.search(pattern, clean, re.IGNORECASE)
            if match:
                value = match.group(1).strip()
                value = re.split(r"\s+(?:TMDB|评分|类型|分类|大小|质量|主演|标签)\s*[:：]", value, 1, flags=re.IGNORECASE)[0].strip()
                if value:
                    candidates.append(value)

    meta_prefix = re.compile(
        r"^(?:⭐|🌟|🏷|📁|💾|🎬|👥|🎭|🌏|🗣|📺|🔥|📎|🔗|简介|分享|标签|分类|类型|评分|主演|大小|质量|版本|语言|地区|字幕|链接|公映|投稿|搜索|机场)\s*[:：]",
        re.IGNORECASE,
    )
    for line in lines[:6]:
        clean = re.sub(r"[#*_`>]+", " ", line).strip()
        if not clean or meta_prefix.search(clean):
            continue
        if re.search(r"https?://|TMDB\s*ID|#\w+", clean, re.IGNORECASE):
            continue
        if len(clean) <= 100:
            candidates.append(clean)

    seen = set()
    result = []
    for item in candidates:
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def channel_text_matches_query_title(text, query):
    query = str(query or "").strip()
    if not query:
        return True

    normalized_query = normalize_title_for_match(query)
    if not normalized_query:
        return True

    candidates = channel_title_candidate_lines(text)
    if not candidates:
        return False

    if len(normalized_query) >= 3:
        for cand in candidates:
            normalized_cand = normalize_title_for_match(cand)
            if normalized_query in normalized_cand or normalized_cand in normalized_query:
                return True

    words = [
        w.lower()
        for w in re.findall(r"[A-Za-z0-9]+|[\u4e00-\u9fa5]{2,}", query)
        if len(w.strip()) >= 2
    ]
    if not words:
        return False

    candidate_blob = " ".join(candidates).lower()
    normalized_blob = normalize_title_for_match(candidate_blob)
    hit = 0
    for word in words:
        if normalize_title_for_match(word) in normalized_blob or word in candidate_blob:
            hit += 1

    required = len(words) if len(words) <= 2 else max(2, int(len(words) * 0.7))
    return hit >= required


def _guess_year_from_title(title):
    title = str(title or "")
    match = re.search(r"(?<!\d)((?:19|20)\d{2})(?!\d)", title)
    return int(match.group(1)) if match else None


def _extract_season_episode(text):
    season_number = None
    episode_number = None
    is_pack = False
    is_completed_pack = False
    evidence = []
    raw = str(text or "")

    if re.search(r"(完结|全\d+集|\d+集全)", raw, re.IGNORECASE):
        is_completed_pack = True
        is_pack = True
        evidence.append("completed_pack")

    range_match = re.search(r"S(\d{1,2})\s*E(?:P)?\s*(\d{1,4})\s*(?:-|~|至)\s*(?:E|EP)?\s*(\d{1,4})", raw, re.IGNORECASE)
    if range_match:
        season_number = int(range_match.group(1))
        episode_number = int(range_match.group(3))
        is_pack = True
        evidence.append("episode_range")
    else:
        se_match = re.search(r"S(\d{1,2})\s*E(?:P)?\s*(\d{1,4})", raw, re.IGNORECASE)
        if se_match:
            season_number = int(se_match.group(1))
            episode_number = int(se_match.group(2))
            evidence.append("sxe")
        else:
            s_match = re.search(r"(?:S|Season|第)\s*(\d{1,2})\s*(?:季)?", raw, re.IGNORECASE)
            e_match = re.search(r"(?:E|EP|Episode|第)\s*(\d{1,4})\s*(?:集|话|話|回)", raw, re.IGNORECASE)
            if s_match:
                season_number = int(s_match.group(1))
                evidence.append("season")
            if e_match:
                episode_number = int(e_match.group(1))
                evidence.append("episode")

            if episode_number is None:
                bulk_match = re.search(r"(?:更新至|全|至)(?:第)?\s*(\d{1,4})\s*(?:集|话|話|回)|(?:^|\s)\d{1,3}-(\d{1,4})(?:集|话|話|回)?", raw)
                if bulk_match:
                    ep_str = bulk_match.group(1) or bulk_match.group(2)
                    if ep_str:
                        episode_number = int(ep_str)
                        is_pack = True
                        evidence.append("bulk_episode")

    date_match = re.search(r"\b((?:19|20)\d{2})[.\-_/](\d{1,2})[.\-_/](\d{1,2})\b", raw)
    if date_match and episode_number is None:
        episode_number = int(f"{int(date_match.group(2)):02d}{int(date_match.group(3)):02d}")
        season_number = season_number if season_number is not None else 1
        evidence.append("date_episode")

    if episode_number is not None and season_number is None:
        season_number = 1

    return season_number, episode_number, is_pack, is_completed_pack, evidence


def _detect_special(text):
    return bool(re.search(r"(?i)\b(?:SP|OVA|OAD|SPECIALS?|番外|特别篇|特別篇)\b", str(text or "")))


def _extract_platform_tag(text):
    raw = str(text or "")
    for label, pattern in _PLATFORM_PATTERNS:
        if re.search(pattern, raw, re.IGNORECASE):
            return label
    return ""


def _extract_release_group(text):
    raw = str(text or "")
    prefix_match = re.match(r"^\[(.{2,32}?)\]", raw)
    if prefix_match:
        group = prefix_match.group(1).strip()
        if group and not re.search(r"\d{3,4}p", group, re.IGNORECASE):
            return group[:32]

    suffix_match = re.search(r"(?:-|_)([A-Za-z0-9][A-Za-z0-9.\-_]{1,24})$", raw)
    if suffix_match:
        group = suffix_match.group(1).strip("._- ")
        if group and not re.search(r"(?i)WEB|BLURAY|REMUX|2160P|1080P|720P|AAC|HEVC|H264|H265", group):
            return group[:32]
    return ""


def _clean_candidate_title(title):
    title = str(title or "").strip()
    if not title:
        return ""

    title = re.sub(r"^\[[^\]]{1,32}\]\s*", "", title).strip()
    title = re.sub(r"^[【\[][^\]】]{1,32}[】\]]\s*", "", title).strip()
    title = re.sub(r"(?i)^(?:group|team|字幕组)\s+", "", title).strip()
    title = re.sub(r"^[^\w\u4e00-\u9fa5]+", "", title).strip()
    title = re.sub(r"\{[^{}]*tmdb[-:=_ ]*\d+[^{}]*\}", "", title, flags=re.IGNORECASE).strip()
    title = re.sub(r"\bTMDB\s*(?:ID|Id|id)?\s*[:：#-]?\s*\d{2,10}\b", "", title, flags=re.IGNORECASE).strip()
    title = re.sub(r"[\[\(（【](?:19|20)\d{2}[\]\)）】]", "", title).strip()
    title = re.sub(r"(?i)[\.\s\-_]*S\d{1,4}(?:E\d{1,4})?\b.*$", "", title).strip()
    title = re.sub(r"(?i)[\.\s\-_]*EP?\d{1,4}\b.*$", "", title).strip()
    title = re.sub(r"(?i)[\.\s\-_]*Season\s*\d{1,4}\b.*$", "", title).strip()
    title = re.sub(r"(?i)[\.\s\-_]*第\s*\d{1,4}\s*[季集话話回].*$", "", title).strip()
    title = re.sub(r"(?<!\d)(?:19|20)\d{2}(?:[ ._-](?:0?[1-9]|1[0-2]))(?:[ ._-](?:0?[1-9]|[12]\d|3[01]))?.*$", "", title).strip()
    title = _TITLE_NOISE_PATTERN.sub(" ", title)
    title = title.replace(".", " ").replace("_", " ")
    title = re.sub(r"\s+", " ", title).strip(" -._")
    return title[:120]


def _derive_identify_title(clean_title, query=""):
    title = _clean_candidate_title(clean_title or query or "")
    title = re.sub(r"(?i)\b(?:OVA|OAD|SP|SPECIALS?|番外|特别篇|特別篇)\b", " ", title)
    title = re.sub(r"\s+", " ", title).strip(" -._")
    return title[:120]


def _infer_media_type(text, expected_media_type=None, season_number=None, episode_number=None):
    if expected_media_type in ("movie", "tv"):
        return expected_media_type
    raw = str(text or "")
    if re.search(r"(?:\[|【)(?:电视剧|剧集|动漫|番剧)(?:\]|】)|(?:电视剧|剧集|动漫|番剧)[:：]", raw, re.IGNORECASE):
        return "tv"
    if re.search(r"(?:\[|【)电影(?:\]|】)|电影[:：]", raw, re.IGNORECASE):
        return "movie"
    if season_number is not None or episode_number is not None or _detect_special(raw):
        return "tv"
    tags = " ".join(re.findall(r"#\w+", raw))
    if re.search(r"#(?:电视剧|日剧|韩剧|美剧|英剧|台剧|港剧|泰剧|短剧|动漫|番剧|剧集|动画)", tags, re.IGNORECASE):
        return "tv"
    return "movie"


def build_tg_media_candidate(
    text,
    *,
    urls: Optional[Iterable[str]] = None,
    chat_username: str = "",
    chat_id: str = "",
    chat_title: str = "",
    message_id: Any = None,
    message_date: str = "",
    message_link: str = "",
    custom_regex: Optional[Dict[str, Any]] = None,
    query: str = "",
    expected_tmdb_id: Any = None,
    expected_year: Any = None,
    expected_media_type: Optional[str] = None,
    strict_title_match: bool = False,
):
    text = str(text or "")
    if not text:
        return None

    custom_regex = custom_regex or {}
    urls = [str(u).strip() for u in (urls or []) if str(u or "").strip()]
    target_link = None
    receive_code = ""

    inline_link_match = re.search(r"(https?://(?:115cdn|115)\.com/s/[a-zA-Z0-9]+(?:[?&]password=[a-zA-Z0-9]+)?)", text, re.IGNORECASE)
    if inline_link_match:
        inline_link = inline_link_match.group(1)
        if inline_link not in urls:
            urls = [inline_link] + urls

    for url in urls:
        if "115.com/s/" in url or "115cdn.com/s/" in url or "hdhive.com/resource/" in url:
            target_link = url
            pwd_in_url = apply_channel_regex(url, custom_regex.get("password", []), DEFAULT_TG_REGEX.get("password_url", []), chat_username, chat_id)
            if pwd_in_url:
                receive_code = pwd_in_url.group(1)
            break

    if not receive_code:
        pwd_match = apply_channel_regex(text, custom_regex.get("password", []), DEFAULT_TG_REGEX.get("password_text", []), chat_username, chat_id)
        if pwd_match:
            receive_code = pwd_match.group(1)

    magnet_match = re.search(r"(magnet:\?xt=urn:btih:[a-zA-Z0-9]+.*?|ed2k://\|file\|.*?\|/)", text, re.IGNORECASE | re.DOTALL)
    magnet_url = magnet_match.group(1).strip() if magnet_match else None
    if not target_link and not magnet_url:
        return None

    tmdb_id = None
    tmdb_match = apply_channel_regex(text, custom_regex.get("tmdb", []), DEFAULT_TG_REGEX.get("tmdb", []), chat_username, chat_id)
    if tmdb_match:
        tmdb_id = tmdb_match.group(1)
    if not tmdb_id:
        tmdb_id = extract_explicit_tmdb_id(text)

    if expected_tmdb_id and tmdb_id and str(tmdb_id) != str(expected_tmdb_id):
        return None

    if strict_title_match and query:
        if not channel_text_matches_query_title(text, query):
            return None
        if expected_year and not channel_text_matches_year(text, expected_year):
            return None

    title = None
    year = None
    title_match = apply_channel_regex(text, custom_regex.get("title_year", []), DEFAULT_TG_REGEX.get("title_year", []), chat_username, chat_id, flags=0)
    if title_match:
        title = title_match.group(1).strip()
        year = title_match.group(2)

    original_title = title or guess_title_from_text(text) or query
    clean_title = _clean_candidate_title(original_title)
    identify_title = _derive_identify_title(clean_title, query=query)

    if not year:
        guessed_year = _guess_year_from_title(original_title)
        if guessed_year is None:
            year_candidates = sorted(extract_year_candidates(text))
            guessed_year = year_candidates[0] if year_candidates else None
        year = guessed_year

    season_number, episode_number, is_pack, is_completed_pack, season_evidence = _extract_season_episode(text)
    is_special = _detect_special(text)
    media_type = _infer_media_type(text, expected_media_type=expected_media_type, season_number=season_number, episode_number=episode_number)

    quality_text = guess_quality_text(text)
    resolution = guess_resolution(text)
    share_size = guess_size_text(text)
    release_group = _extract_release_group(original_title or text)
    platform_tag = _extract_platform_tag(text)

    evidence = []
    if tmdb_id:
        evidence.append("explicit_tmdb")
    if identify_title:
        evidence.append("identify_title")
    if year:
        evidence.append("year")
    if media_type == "tv":
        evidence.append("media_type_tv")
    evidence.extend(season_evidence)
    if is_special:
        evidence.append("special")
    if platform_tag:
        evidence.append("platform")
    if release_group:
        evidence.append("release_group")

    confidence = "low"
    if tmdb_id:
        confidence = "high"
    elif identify_title and (year or media_type == "tv" or episode_number is not None):
        confidence = "medium"

    snippet = normalize_text(text)
    if len(snippet) > 180:
        snippet = snippet[:179] + "…"

    title_for_display = identify_title or clean_title or original_title or query or "频道资源"
    return {
        "_tg_source": "channel",
        "source": "channel",
        "source_kind": "channel",
        "title": title_for_display,
        "name": title_for_display,
        "original_title": original_title or title_for_display,
        "clean_title": clean_title or title_for_display,
        "identify_title": identify_title or clean_title or title_for_display,
        "year": year,
        "remark": snippet,
        "quality": quality_text,
        "quality_text": quality_text,
        "resolution": resolution or "未知",
        "share_size": share_size,
        "pan_type": "115" if target_link else "离线",
        "unlock_points": 0,
        "source_channel": chat_title or chat_username or chat_id,
        "source_username": chat_username,
        "source_chat_id": str(chat_id or ""),
        "message_id": message_id,
        "message_date": message_date,
        "message_link": message_link,
        "text": text,
        "raw_text": text,
        "tmdb_id": tmdb_id or expected_tmdb_id,
        "item_type": media_type,
        "media_type": media_type,
        "target_link": target_link,
        "magnet_url": magnet_url,
        "receive_code": receive_code,
        "season_number": season_number,
        "episode_number": episode_number,
        "is_special": is_special,
        "is_pack": is_pack,
        "is_completed_pack": is_completed_pack,
        "release_group": release_group,
        "platform_tag": platform_tag,
        "confidence": confidence,
        "evidence": evidence,
        "matched_rules": list(evidence),
        "conflict_reason": "",
        "parse_version": TG_CANDIDATE_PARSE_VERSION,
    }


def build_channel_task_payload(candidate, *, is_brainless=False, is_keyword_matched=False, is_subscribe=True, title_override=None, tmdb_id_override=None, media_type_override=None, year_override=None):
    candidate = candidate or {}
    media_type = media_type_override or candidate.get("media_type") or candidate.get("item_type") or "movie"
    title = title_override or candidate.get("identify_title") or candidate.get("clean_title") or candidate.get("title") or candidate.get("name")
    payload = {
        "type": "channel_resource_complex",
        "tmdb_id": str(tmdb_id_override) if tmdb_id_override is not None else candidate.get("tmdb_id"),
        "title": title,
        "year": year_override if year_override is not None else candidate.get("year"),
        "item_type": media_type,
        "target_link": candidate.get("target_link"),
        "magnet_url": candidate.get("magnet_url"),
        "receive_code": candidate.get("receive_code") or "",
        "season_number": candidate.get("season_number"),
        "episode_number": candidate.get("episode_number"),
        "is_special": bool(candidate.get("is_special")),
        "is_pack": bool(candidate.get("is_pack")),
        "is_completed_pack": bool(candidate.get("is_completed_pack")),
        "is_brainless": bool(is_brainless),
        "is_keyword_matched": bool(is_keyword_matched),
        "is_subscribe": bool(is_subscribe),
        "candidate": copy.deepcopy(candidate),
    }
    return payload


def candidate_to_recognition_hints(candidate):
    candidate = copy.deepcopy(candidate or {})
    return {
        "tmdb_id": candidate.get("tmdb_id"),
        "title": candidate.get("title") or candidate.get("identify_title") or candidate.get("clean_title"),
        "clean_title": candidate.get("clean_title") or candidate.get("title"),
        "identify_title": candidate.get("identify_title") or candidate.get("clean_title") or candidate.get("title"),
        "year": candidate.get("year"),
        "media_type": candidate.get("media_type") or candidate.get("item_type"),
        "season_number": candidate.get("season_number"),
        "episode_number": candidate.get("episode_number"),
        "is_special": bool(candidate.get("is_special")),
        "quality_text": candidate.get("quality_text") or candidate.get("quality"),
        "resolution": candidate.get("resolution"),
        "release_group": candidate.get("release_group"),
        "platform_tag": candidate.get("platform_tag"),
        "raw_text": candidate.get("raw_text") or candidate.get("text"),
        "target_link": candidate.get("target_link"),
        "magnet_url": candidate.get("magnet_url"),
        "receive_code": candidate.get("receive_code"),
        "source_channel": candidate.get("source_channel"),
        "source_username": candidate.get("source_username"),
        "source_chat_id": candidate.get("source_chat_id"),
        "message_id": candidate.get("message_id"),
        "message_link": candidate.get("message_link"),
        "confidence": candidate.get("confidence") or "low",
        "evidence": list(candidate.get("evidence") or []),
        "matched_rules": list(candidate.get("matched_rules") or []),
        "conflict_reason": candidate.get("conflict_reason") or "",
        "parse_version": candidate.get("parse_version") or TG_CANDIDATE_PARSE_VERSION,
        "source": "tg_candidate",
    }


def is_recognition_hint_eligible(candidate_or_hints):
    hints = candidate_to_recognition_hints(candidate_or_hints)
    if hints.get("conflict_reason"):
        return False
    return hints.get("confidence") in ("medium", "high")


def remember_candidate_hint(candidate_or_hints, ttl_seconds=_CANDIDATE_HINT_TTL_SECONDS):
    hints = candidate_to_recognition_hints(candidate_or_hints)
    if not hints.get("identify_title") and not hints.get("clean_title") and not hints.get("title") and not hints.get("tmdb_id"):
        return

    expiry = time.time() + max(int(ttl_seconds or 0), 60)
    hints["_expiry_at"] = expiry
    hints["_normalized_titles"] = [
        normalize_title_for_match(hints.get("identify_title")),
        normalize_title_for_match(hints.get("clean_title")),
        normalize_title_for_match(hints.get("title")),
    ]
    hints["_normalized_titles"] = [x for x in hints["_normalized_titles"] if x]
    hints["_lookup_keys"] = _collect_lookup_keys(
        target_link=hints.get("target_link"),
        magnet_url=hints.get("magnet_url"),
        receive_code=hints.get("receive_code"),
        source_chat_id=hints.get("source_chat_id"),
        source_username=hints.get("source_username"),
        message_id=hints.get("message_id"),
    )

    with _CANDIDATE_HINT_LOCK:
        prune_candidate_hints_locked()
        _CANDIDATE_HINT_REGISTRY.append(hints)
        if len(_CANDIDATE_HINT_REGISTRY) > 200:
            del _CANDIDATE_HINT_REGISTRY[:-200]


def prune_candidate_hints_locked():
    now = time.time()
    _CANDIDATE_HINT_REGISTRY[:] = [item for item in _CANDIDATE_HINT_REGISTRY if item.get("_expiry_at", 0) > now]


def lookup_candidate_hint(primary_text, *, alt_texts=None, media_type=None, season_number=None, lookup_key=None, lookup_keys=None):
    texts = [str(primary_text or "").strip()]
    texts.extend([str(x or "").strip() for x in (alt_texts or []) if str(x or "").strip()])
    normalized_texts = [normalize_title_for_match(t) for t in texts if t]
    provided_lookup_keys = _expand_lookup_keys(lookup_key, lookup_keys)
    if not normalized_texts and not provided_lookup_keys:
        return None

    best_hint = None
    best_score = 0
    tied_hints = []
    now = time.time()
    with _CANDIDATE_HINT_LOCK:
        prune_candidate_hints_locked()

        if provided_lookup_keys:
            strong_match = None
            strong_score = -1
            for hint in _CANDIDATE_HINT_REGISTRY:
                if hint.get("_expiry_at", 0) <= now:
                    continue
                hint_type = hint.get("media_type")
                if media_type and hint_type and media_type != hint_type:
                    continue

                hint_keys = hint.get("_lookup_keys", [])
                if not hint_keys or not any(key in hint_keys for key in provided_lookup_keys):
                    continue

                score = 10
                if season_number is not None and hint.get("season_number") not in (None, "", season_number):
                    score -= 1
                if hint.get("confidence") == "high":
                    score += 1
                if score > strong_score:
                    strong_score = score
                    strong_match = copy.deepcopy(hint)

            if strong_match:
                strong_match.pop("_expiry_at", None)
                strong_match.pop("_normalized_titles", None)
                strong_match.pop("_lookup_keys", None)
                return strong_match

            return None

        for hint in _CANDIDATE_HINT_REGISTRY:
            if hint.get("_expiry_at", 0) <= now:
                continue
            hint_type = hint.get("media_type")
            if media_type and hint_type and media_type != hint_type:
                continue

            score = 0
            for norm_text in normalized_texts:
                for hint_title in hint.get("_normalized_titles", []):
                    if not norm_text or not hint_title:
                        continue
                    if hint_title == norm_text:
                        score = max(score, 5)
                    elif hint_title in norm_text or norm_text in hint_title:
                        score = max(score, 4)
            if score and season_number is not None and hint.get("season_number") not in (None, "", season_number):
                score -= 1
            if hint.get("confidence") == "high":
                score += 1
            if score > best_score:
                best_score = score
                best_hint = copy.deepcopy(hint)
                tied_hints = [hint]
            elif score and score == best_score:
                tied_hints.append(hint)

    if best_score < 4:
        return None

    if len(tied_hints) > 1:
        tied_key_sets = {
            tuple(sorted(hint.get("_lookup_keys", [])))
            for hint in tied_hints
            if hint.get("_lookup_keys")
        }
        if len(tied_key_sets) > 1:
            return None

    best_hint.pop("_expiry_at", None)
    best_hint.pop("_normalized_titles", None)
    best_hint.pop("_lookup_keys", None)
    return best_hint


def _resolve_lookup_keys_for_name(primary_text, *, alt_texts=None, media_type=None, season_number=None):
    texts = [str(primary_text or "").strip()]
    texts.extend([str(x or "").strip() for x in (alt_texts or []) if str(x or "").strip()])
    normalized_texts = [normalize_title_for_match(t) for t in texts if t]
    if not normalized_texts:
        return None

    best_score = 0
    matched_hints = []
    now = time.time()
    with _CANDIDATE_HINT_LOCK:
        prune_candidate_hints_locked()
        for hint in _CANDIDATE_HINT_REGISTRY:
            if hint.get("_expiry_at", 0) <= now:
                continue
            hint_type = hint.get("media_type")
            if media_type and hint_type and media_type != hint_type:
                continue

            score = 0
            for norm_text in normalized_texts:
                for hint_title in hint.get("_normalized_titles", []):
                    if not norm_text or not hint_title:
                        continue
                    if hint_title == norm_text:
                        score = max(score, 5)
                    elif hint_title in norm_text or norm_text in hint_title:
                        score = max(score, 4)
            if score and season_number is not None and hint.get("season_number") not in (None, "", season_number):
                score -= 1
            if hint.get("confidence") == "high":
                score += 1

            if score > best_score:
                best_score = score
                matched_hints = [hint]
            elif score and score == best_score:
                matched_hints.append(hint)

    if best_score < 4 or not matched_hints:
        return None

    strong_key_sets = {
        tuple(sorted(hint.get("_lookup_keys", [])))
        for hint in matched_hints
        if hint.get("_lookup_keys")
    }
    if not strong_key_sets:
        return None
    if len(strong_key_sets) > 1:
        return _LOOKUP_KEYS_BLOCKED
    return list(next(iter(strong_key_sets)))


def lookup_candidate_hint_for_name(primary_text, *, alt_texts=None, media_type=None, season_number=None):
    lookup_keys = _resolve_lookup_keys_for_name(
        primary_text,
        alt_texts=alt_texts,
        media_type=media_type,
        season_number=season_number,
    )
    if lookup_keys is _LOOKUP_KEYS_BLOCKED:
        return None
    if lookup_keys:
        return lookup_candidate_hint(
            primary_text,
            alt_texts=alt_texts,
            media_type=media_type,
            season_number=season_number,
            lookup_keys=lookup_keys,
        )
    return lookup_candidate_hint(
        primary_text,
        alt_texts=alt_texts,
        media_type=media_type,
        season_number=season_number,
    )
