# tasks/subscriptions.py
# 智能订阅模块
import time
import re
import json
import inspect
from datetime import datetime, timedelta
import logging
from typing import Any, List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

# 导入需要的底层模块和共享实例
import config_manager
import constants
import handler.tmdb as tmdb
import handler.moviepilot as moviepilot
import task_manager
from handler import telegram
from database import settings_db, request_db, user_db, media_db, watchlist_db
from .helpers import is_movie_subscribable, check_series_completion, parse_series_title_and_season, should_mark_as_pending
from handler.hdhive_client import HDHiveClient
from tasks.hdhive import task_download_from_hdhive, filter_hdhive_resources
try:
    from handler.tg_userbot import TGUserBotManager, tg_task_queue
except Exception:
    TGUserBotManager = None
    tg_task_queue = None
from handler.tg_media_candidate import build_channel_task_payload

try:
    from handler.shared_subscription_service import (
        try_consume_shared_resource,
        try_consume_preprobed_shared_resource,
        batch_probe_shared_resources,
    )
except Exception:
    try_consume_shared_resource = None
    try_consume_preprobed_shared_resource = None
    batch_probe_shared_resources = None



logger = logging.getLogger(__name__)


def _try_consume_shared_resource_no_gap(func, **kwargs):
    """调用共享中心即时消费，但禁止客户端登记普通缺口。

    现在中心端已经“有效资源入池即广播”，客户端不再需要在未命中时
    写 wanted_gaps / wanted_gap_devices。旧版 shared_subscription_service
    如果没有提供禁用缺口登记的开关，就直接跳过即时兜底查询，避免
    try_consume_shared_resource 在 miss 分支偷偷 report_shared_gap。
    """
    if not callable(func):
        return {}

    gap_switches = {
        'report_gap': False,
        'enable_gap_report': False,
        'auto_report_gap': False,
        'register_gap': False,
        'skip_report_gap': True,
        'disable_gap_report': True,
    }

    try:
        sig = inspect.signature(func)
        params = sig.parameters
        has_var_kwargs = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())
        supported_switches = {k: v for k, v in gap_switches.items() if has_var_kwargs or k in params}
    except Exception:
        supported_switches = {}

    if not supported_switches:
        logger.debug(
            '  ➜ [共享秒传] 当前 shared_subscription_service 不支持禁用缺口登记，'
            '已跳过即时共享查询，等待中心入池广播事件。'
        )
        return {
            'enabled': True,
            'success': False,
            'skipped': True,
            'reason': 'gap_registration_removed',
            'message': '客户端普通缺口登记已移除，等待中心广播候选。',
        }

    call_kwargs = dict(kwargs)
    call_kwargs.update(supported_switches)
    return func(**call_kwargs)


EFFECT_KEYWORD_MAP = {
    "杜比视界": ["dolby vision", "dovi"],
    "HDR": ["hdr", "hdr10", "hdr10+", "hlg"]
}

AUDIO_SUBTITLE_KEYWORD_MAP = {
    # --- 音轨关键词 ---
    "chi": ["Mandarin", "CHI", "ZHO", "国语", "国配", "国英双语", "公映", "台配", "京译", "上译", "央译"],
    "yue": ["Cantonese", "YUE", "粤语"],
    "eng": ["English", "ENG", "英语"],
    "jpn": ["Japanese", "JPN", "日语"],
    "kor": ["Korean", "KOR", "韩语"],

    # --- 字幕关键词 ---
    # 注意：resubscribe.py 会通过 "sub_" + 语言代码 来查找这里
    "sub_chi": ["CHS", "CHT", "中字", "简中", "繁中", "简", "繁", "Chinese"],
    "sub_eng": ["ENG", "英字", "English"],
    "sub_jpn": ["JPN", "日字", "日文", "Japanese"],
    "sub_kor": ["KOR", "韩字", "韩文", "Korean"],
    "sub_yue": ["CHT", "繁中", "繁体", "Cantonese"],
}

def _try_download_from_hdhive_first(tmdb_id, media_type, title, item_label="媒体", target_season=None, require_complete=False):
    """
    统一的影巢优先处理：
    - Movie 使用电影 TMDb ID + media_type=movie
    - Series 使用剧集 TMDb ID + media_type=tv
    - Season 使用父剧集 TMDb ID + media_type=tv；影巢请求不带季号，本地按 target_season 过滤/排序
    - require_complete=True 时，仅保留全集/全结/完结包，避免已完结剧集转存残缺分段包
    - 只负责检索、筛选、选择最优资源并转存，失败返回 False 交给 MP 兜底
    """
    season_suffix = ""
    if media_type == "tv" and target_season is not None:
        try:
            season_suffix = f" S{int(target_season):02d}"
        except Exception:
            season_suffix = f" S{target_season}"

    logger.info(
        f"  ➜ [策略] {item_label}《{title}》{season_suffix} 启用影巢优先，正在检索并筛选资源..."
    )

    try:
        hd_client = HDHiveClient()
        if not hd_client.ping():
            logger.warning("  ➜ 影巢尚未完成授权或授权已失效，自动降级到 MoviePilot...")
            return False

        resources = hd_client.get_resources(tmdb_id, media_type, target_season=target_season)

        if not resources:
            logger.info(f"  ➜ 影巢未找到{item_label}《{title}》{season_suffix} 的资源，准备降级到 MoviePilot 兜底...")
            return False

        before_count = len(resources)
        valid_resources = filter_hdhive_resources(
            resources,
            target_season=target_season,
            media_type=media_type,
            require_complete=require_complete
        )

        if not valid_resources:
            if media_type == "tv" and require_complete:
                logger.info(
                    f"  ➜ 影巢返回 {before_count} 个资源，但没有符合条件的完结包，准备降级到 MoviePilot 兜底..."
                )
            else:
                logger.info(
                    f"  ➜ 影巢返回 {before_count} 个资源，但全部被影巢配置/季号规则拦截，准备降级到 MoviePilot 兜底..."
                )
            return False

        logger.info(
            f"  ➜ 影巢资源筛选完成: {before_count} -> {len(valid_resources)}，正在选择最优资源..."
        )

        ignore_season_priority = False
        if media_type == "tv" and require_complete and target_season is not None:
            try:
                ignore_season_priority = int(target_season) == 1
            except Exception:
                ignore_season_priority = False

        if ignore_season_priority:
            logger.info(
                "  ➜ [影巢策略] 已完结第一季：忽略季号优先级，只排除明确错季；"
                "完结包之间按积分 / 115优先 / 体积排序。"
            )

        def _resource_score(r):
            completion_level = int(r.get('_completion_level') or 0)
            season_level = int(r.get('_season_match_level') or 0)
            effective_points = int(r.get('_effective_points') or 0)
            size_gb = float(r.get('_size_gb') or 0)
            pan_type = str(r.get('pan_type') or '115').lower()
            base_score = (
                effective_points,
                0 if pan_type == '115' else 1,
                -size_gb
            )

            if media_type == "tv":
                if require_complete:
                    # 已完结剧集/季：完结包已经被硬过滤。
                    # 国产单季剧第一季的完结包经常只写“全集 / 全结 / 38集全”，不写 S01。
                    # 因此 S01 已完结时不再让“明确 S01”天然压过“未标季完结包”，但前面的季号过滤仍会排除明确 S02/S03 等错季资源。
                    if ignore_season_priority:
                        return (-completion_level, *base_score)
                    return (-completion_level, -season_level, *base_score)

                # 未完结剧集/季：不强制完结包，优先避免错季；后续由智能追剧处理追更。
                return (-season_level, *base_score)

            return base_score

        valid_resources.sort(key=_resource_score)
        target_resource = valid_resources[0]
        slug = target_resource.get('slug')

        season_match_info = ""
        if media_type == "tv":
            policy_label = "只收完结包" if require_complete else "不强制完结包"
            if target_season is not None:
                season_match_info = (
                    f", 季匹配: {target_resource.get('_season_match_label') or '未知'}"
                    f", 完整度: {target_resource.get('_completion_label') or '未知'}"
                    f", 策略: {policy_label}"
                )
            else:
                season_match_info = (
                    f", 完整度: {target_resource.get('_completion_label') or '未知'}"
                    f", 策略: {policy_label}"
                )

        logger.info(
            f"  ➜ 最终选定影巢资源: {target_resource.get('title') or slug} "
            f"(类型: {target_resource.get('pan_type') or '115'}, "
            f"积分: {target_resource.get('unlock_points')}, "
            f"体积: {target_resource.get('share_size') or '未知'}{season_match_info})"
        )

        if not slug:
            logger.warning("  ➜ 影巢资源缺少 slug，准备降级到 MoviePilot 兜底...")
            return False

        success = task_download_from_hdhive(
            None,
            slug,
            tmdb_id,
            media_type,
            title
        )

        if success:
            logger.info("  ➜ 影巢处理成功！已跳过 MoviePilot 订阅。")
            return True

        logger.warning("  ➜ 影巢处理失败，准备降级到 MoviePilot 兜底...")
        return False

    except Exception as e:
        logger.error(f"  ➜ 影巢优先处理异常，准备降级到 MoviePilot: {e}", exc_info=True)
        return False


def _cloud_size_to_gb(value):
    """把频道资源里提取到的 3.5GB / 940MB / 1.2TB 等文本转成 GB，便于排序。"""
    if value is None:
        return 0.0
    try:
        if isinstance(value, (int, float)):
            return float(value) / 1024 / 1024 / 1024 if float(value) > 10000 else float(value)
        text = str(value).strip().upper().replace(',', '')
        match = re.search(r'(\d+(?:\.\d+)?)\s*(TB|GB|G|MB|M|KB|K|B)?', text)
        if not match:
            return 0.0
        number = float(match.group(1))
        unit = match.group(2) or 'GB'
        if unit == 'TB':
            return number * 1024
        if unit in ('GB', 'G'):
            return number
        if unit in ('MB', 'M'):
            return number / 1024
        if unit in ('KB', 'K'):
            return number / 1024 / 1024
        if unit == 'B':
            return number / 1024 / 1024 / 1024
    except Exception:
        return 0.0
    return 0.0


def _channel_resource_text(resource: Dict) -> str:
    parts = []
    for key in ('title', 'name', 'quality', 'remark', 'text', 'source_channel'):
        value = resource.get(key)
        if isinstance(value, (list, tuple, set)):
            value = ' '.join(str(v) for v in value if v)
        if value:
            parts.append(str(value))
    return '\n'.join(parts)


def _normalize_title_for_channel_match(value: str) -> str:
    text = str(value or '').lower()
    text = re.sub(r'[\s\-_·.．・:：,，;；!！?？()\[\]【】{}<>《》"“”\'’‘`~～/\\|]+', '', text)
    return text


def _extract_channel_resource_years(resource: Dict) -> set[int]:
    text = _channel_resource_text(resource)
    years = set()
    for match in re.finditer(r'(?<!\d)((?:19|20)\d{2})(?!\d)', text):
        try:
            years.add(int(match.group(1)))
        except Exception:
            pass
    return years


def _channel_resource_matches_year(resource: Dict, year=None) -> bool:
    year = str(year or '').strip()
    if not year:
        return True
    try:
        expected = int(year[:4])
    except Exception:
        return True
    years = _extract_channel_resource_years(resource)
    # 自动流程的无 TMDb ID 兜底必须“片名 + 年份”同时命中；无年份也不放行。
    return expected in years


def _channel_resource_matches_title(resource: Dict, title: str) -> bool:
    title = str(title or '').strip()
    if not title:
        return True

    # 优先使用 tg_userbot 已经抽取出的标题字段；不要用全文匹配，避免演员/简介/标签串台。
    candidate_parts = []
    for key in ('title', 'name'):
        value = resource.get(key)
        if value:
            candidate_parts.append(str(value))

    # 如果 title/name 缺失，再退一步从正文前几行找标题样式字段。
    if not candidate_parts:
        text = str(resource.get('text') or resource.get('remark') or '')
        for line in text.splitlines()[:8]:
            line = re.sub(r'\s+', ' ', line).strip()
            if not line:
                continue
            match = re.search(r'(?:电影|影片|剧集|电视剧|番剧|动漫|片名|标题|名称)\s*[:：]\s*(.+)$', line, re.IGNORECASE)
            if match:
                candidate_parts.append(match.group(1).strip())
                break

    if not candidate_parts:
        return False

    normalized_title = _normalize_title_for_channel_match(title)
    normalized_text = _normalize_title_for_channel_match(' '.join(candidate_parts))

    if normalized_title and len(normalized_title) >= 3 and (normalized_title in normalized_text or normalized_text in normalized_title):
        return True

    words = [
        w.lower()
        for w in re.findall(r'[A-Za-z0-9]+|[\u4e00-\u9fa5]{2,}', title)
        if len(w.strip()) >= 2
    ]
    if not words:
        return False

    raw_text_lower = ' '.join(candidate_parts).lower()
    hit = 0
    for word in words:
        if _normalize_title_for_channel_match(word) in normalized_text or word in raw_text_lower:
            hit += 1

    required = len(words) if len(words) <= 2 else max(2, int(len(words) * 0.7))
    return hit >= required


def _channel_resource_matches_identity(resource: Dict, tmdb_id=None, title: str = '', year=None) -> bool:
    """频道资源身份校验。统一订阅自动流程使用。

    规则：
    1. 资源正文/字段里有 TMDb ID：优先按 TMDb ID 判断，相等即通过，不等即丢弃。
    2. 没有 TMDb ID：才走片名 + 年份兜底；年份不对或缺失则丢弃。
    """
    resource = resource or {}
    expected_tmdb = str(tmdb_id or '').strip()
    resource_tmdb = str(resource.get('tmdb_id') or '').strip()

    if expected_tmdb and resource_tmdb:
        return resource_tmdb == expected_tmdb

    if title and not _channel_resource_matches_title(resource, title):
        return False

    if year and not _channel_resource_matches_year(resource, year):
        return False

    return bool(title)


def _fallback_channel_rule_matches(target_channel, chat_username, chat_id):
    target_channel = str(target_channel or '').strip().lower()
    if not target_channel:
        return True

    chat_username = str(chat_username or '').strip().lower().lstrip('@')
    chat_id = str(chat_id or '').strip()
    target_clean = target_channel.lstrip('@')
    target_id_clean = target_clean.replace('-100', '') if target_clean.startswith('-100') else target_clean
    curr_id_clean = chat_id.replace('-100', '') if chat_id.startswith('-100') else chat_id

    return (
        chat_username == target_clean
        or chat_id == target_channel
        or curr_id_clean == target_id_clean
    )


def _channel_resource_block_rule(resource: Dict):
    """统一订阅自动流程专用：复用 TG 频道监听的拦截规则做资源初检。

    手动 TG 搜索和云下载模态框不调用本函数，因此不会被拦截规则影响；
    自动订阅无人值守选择频道资源时必须调用，避免初筛阶段转入明显不想要的资源。
    """
    resource = resource or {}

    if TGUserBotManager is not None:
        try:
            manager = TGUserBotManager.get_instance()
            if hasattr(manager, 'is_resource_blocked_by_rules'):
                return manager.is_resource_blocked_by_rules(resource)
        except Exception as e:
            logger.warning(f"  ➜ [频道搜索] 调用 UserBot 拦截规则检查失败，将使用本地兜底检查: {e}")

    cfg = settings_db.get_setting('tg_userbot_config') or {}
    rules = cfg.get('block_keywords') or []
    if not rules:
        return None

    text = resource.get('text') or _channel_resource_text(resource)
    chat_username = resource.get('source_username') or ''
    chat_id = resource.get('source_chat_id') or resource.get('chat_id') or ''

    for rule_obj in rules:
        if isinstance(rule_obj, str):
            pattern = rule_obj.strip()
            target_channel = ''
        else:
            pattern = str((rule_obj or {}).get('pattern', '')).strip()
            target_channel = str((rule_obj or {}).get('channel', '')).strip().lower()

        if not pattern:
            continue
        if not _fallback_channel_rule_matches(target_channel, chat_username, chat_id):
            continue

        try:
            if re.search(pattern, text or '', re.IGNORECASE):
                return pattern
        except Exception as e:
            logger.error(f"  ➜ [频道搜索] 拦截规则正则解析错误 '{pattern}': {e}")

    return None


def _extract_explicit_seasons(text: str) -> set[int]:
    """从频道消息中提取明确季号；自动订阅用，避免把 S02 当 S01 转存。"""
    text = str(text or '')
    seasons = set()

    for pattern in [
        r'\bS\s*(\d{1,2})\b',
        r'\bSeason\s*(\d{1,2})\b',
        r'第\s*(\d{1,2})\s*季',
        r'\{tmdb-\d+\}\s*(\d{1,2})\s*-\s*\d+\s*集',
    ]:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            try:
                seasons.add(int(match.group(1)))
            except Exception:
                pass
    return seasons


def _is_channel_resource_complete(resource: Dict) -> bool:
    text = _channel_resource_text(resource)
    if resource.get('is_completed_pack'):
        return True
    return bool(re.search(r'(完结|全集|全\s*\d+\s*[集话]|\d+\s*[集话]\s*全|Complete|Completed|Finale)', text, re.IGNORECASE))


def _channel_resource_season_level(resource: Dict, target_season=None) -> int:
    """
    返回频道资源的季匹配等级：
    2 = 明确命中目标季；1 = 未写季号但目标季为 S01，可视作单季/第一季候选；0 = 不适用；-1 = 明确错季。
    """
    if target_season is None:
        return 0

    try:
        target = int(target_season)
    except Exception:
        return 0

    explicit = set()
    try:
        if resource.get('season_number') is not None:
            explicit.add(int(resource.get('season_number')))
    except Exception:
        pass

    explicit.update(_extract_explicit_seasons(_channel_resource_text(resource)))

    if explicit:
        return 2 if target in explicit else -1

    # 自动流程保守过滤：没有明确季号时，只允许 S01 候选，避免 S02/S03 误转。
    return 1 if target == 1 else -1


def _filter_channel_resources_for_auto(resources: List[Dict], media_type: str, target_season=None, require_complete: bool = False, title: str = '', tmdb_id=None, year=None) -> List[Dict]:
    filtered = []
    for resource in resources or []:
        block_rule = _channel_resource_block_rule(resource)
        if block_rule:
            logger.info(
                "  ➜ [频道搜索] 自动流程初检拦截频道资源：命中规则 '%s'，标题=%s，频道=%s",
                block_rule,
                resource.get('title') or resource.get('name') or title or '未知',
                resource.get('source_channel') or resource.get('source_username') or '未知'
            )
            continue

        if not _channel_resource_matches_identity(resource, tmdb_id=tmdb_id, title=title, year=year):
            continue
        if media_type == 'tv':
            season_level = _channel_resource_season_level(resource, target_season)
            if target_season is not None and season_level < 0:
                continue
            resource['_season_match_level'] = season_level
            if require_complete and not _is_channel_resource_complete(resource):
                continue
            resource['_completion_level'] = 2 if _is_channel_resource_complete(resource) else 0
        filtered.append(resource)
    return filtered


def _channel_resource_score(resource: Dict, media_type: str, target_season=None, require_complete: bool = False):
    text = _channel_resource_text(resource).upper()
    size_gb = _cloud_size_to_gb(resource.get('share_size') or resource.get('size'))
    pan_type = str(resource.get('pan_type') or '').lower()

    resolution_score = 0
    if '8K' in text:
        resolution_score = 4
    elif '4K' in text or '2160P' in text:
        resolution_score = 3
    elif '1080P' in text:
        resolution_score = 2
    elif '720P' in text:
        resolution_score = 1

    quality_score = 0
    for keyword, weight in [('REMUX', 5), ('BLURAY', 4), ('WEB-DL', 3), ('WEBRIP', 2), ('HDR', 1), ('DV', 1), ('DOVI', 1)]:
        if keyword in text:
            quality_score += weight

    season_level = int(resource.get('_season_match_level') or 0)
    completion_level = int(resource.get('_completion_level') or 0)

    # sort 默认升序：负值越小越靠前。
    base = (
        0 if pan_type in ('115', '115网盘') or resource.get('target_link') else 1,
        -resolution_score,
        -quality_score,
        -size_gb,
    )
    if media_type == 'tv':
        if require_complete:
            return (-completion_level, -season_level, *base)
        return (-season_level, *base)
    return base


def _build_channel_extra_queries(title: str, year=None, target_season=None) -> List[str]:
    title = str(title or '').strip()
    year = str(year or '').strip()
    queries = []
    if title and year:
        queries.append(f'{title} {year}')
    if title and target_season is not None:
        try:
            s_num = int(target_season)
            queries.extend([f'{title} S{s_num:02d}', f'{title} 第{s_num}季'])
        except Exception:
            pass
    return queries


def _enqueue_channel_resource_download(resource: Dict, tmdb_id, media_type: str, title: str, target_season=None) -> bool:
    if tg_task_queue is None:
        logger.warning('  ➜ [频道搜索] tg_task_queue 不可用，无法推送转存任务。')
        return False

    target_link = resource.get('target_link')
    magnet_url = resource.get('magnet_url')
    if not target_link and not magnet_url:
        logger.warning('  ➜ [频道搜索] 候选资源缺少 target_link / magnet_url，无法转存。')
        return False

    season_number = resource.get('season_number')
    if season_number is None and target_season is not None:
        try:
            season_number = int(target_season)
        except Exception:
            season_number = target_season

    tg_task_queue.put(
        build_channel_task_payload(
            resource,
            is_brainless=True,
            is_keyword_matched=True,
            is_subscribe=False,
            title_override=title or resource.get('title') or resource.get('name'),
            tmdb_id_override=str(tmdb_id) if tmdb_id is not None else resource.get('tmdb_id'),
            media_type_override=media_type,
            year_override=resource.get('year'),
        ) | {'season_number': season_number}
    )
    return True


def _try_download_from_channel_first(tmdb_id, media_type, title, item_label='媒体', target_season=None, require_complete=False, year=None):
    """统一订阅自动流程的频道历史搜索兜底；剧集/季必须启用季过滤。"""
    if TGUserBotManager is None:
        logger.info('  ➜ [频道搜索] 当前环境未加载 TGUserBotManager，跳过频道搜索。')
        return False

    if not title:
        logger.info('  ➜ [频道搜索] 缺少标题，跳过频道搜索。')
        return False

    season_suffix = ''
    if media_type == 'tv' and target_season is not None:
        try:
            season_suffix = f' S{int(target_season):02d}'
        except Exception:
            season_suffix = f' S{target_season}'

    try:
        manager = TGUserBotManager.get_instance()
        if not hasattr(manager, 'search_channel_resources'):
            logger.info('  ➜ [频道搜索] tg_userbot.py 尚未支持频道历史搜索，跳过。')
            return False

        logger.info(
            f'  ➜ [策略] {item_label}《{title}》{season_suffix} 启用频道历史搜索兜底；'
            f'{"已启用季过滤，" if media_type == "tv" and target_season is not None else ""}'
            f'{"只收完结包" if require_complete else "不强制完结包"}。'
        )

        search_result = manager.search_channel_resources(
            query=title,
            media_type=media_type,
            tmdb_id=tmdb_id,
            limit=50,
            extra_queries=_build_channel_extra_queries(title, year=year, target_season=target_season),
            timeout=35,
            include_tmdb_query=False,
            strict_title_match=True,
        )

        if not search_result.get('ok'):
            logger.info(f"  ➜ [频道搜索] 未能执行频道搜索：{search_result.get('error') or '未知原因'}")
            return False

        candidates = search_result.get('results') or []
        if not candidates:
            logger.info(f'  ➜ [频道搜索] 未找到《{title}》{season_suffix} 的频道资源。')
            return False

        before_count = len(candidates)
        candidates = _filter_channel_resources_for_auto(
            candidates,
            media_type=media_type,
            target_season=target_season,
            require_complete=require_complete,
            title=title,
            tmdb_id=tmdb_id,
            year=year,
        )

        if not candidates:
            logger.info(
                f'  ➜ [频道搜索] 返回 {before_count} 条频道资源，但经季号/完结包规则过滤后无可用候选，准备 MP 兜底。'
            )
            return False

        candidates.sort(key=lambda r: _channel_resource_score(r, media_type, target_season, require_complete))
        target_resource = candidates[0]

        logger.info(
            f"  ➜ 最终选定频道资源: {target_resource.get('title') or target_resource.get('name') or title} "
            f"(频道: {target_resource.get('source_channel') or '未知'}, "
            f"体积: {target_resource.get('share_size') or '未知'}, "
            f"清晰度: {target_resource.get('resolution') or '未知'}, "
            f"季匹配等级: {target_resource.get('_season_match_level', 0)})"
        )

        if _enqueue_channel_resource_download(target_resource, tmdb_id, media_type, title, target_season=target_season):
            logger.info('  ➜ 频道资源已推入转存队列，跳过 MoviePilot 订阅。')
            return True

        return False

    except Exception as e:
        logger.error(f'  ➜ 频道搜索处理异常，准备降级到 MoviePilot: {e}', exc_info=True)
        return False


def _try_download_from_cloud_first(tmdb_id, media_type, title, item_label='媒体', target_season=None, require_complete=False, year=None):
    """云资源优先：先影巢，失败后频道历史搜索；自动流程对剧集启用季过滤。"""
    if _try_download_from_hdhive_first(
        tmdb_id,
        media_type,
        title,
        item_label=item_label,
        target_season=target_season,
        require_complete=require_complete,
    ):
        return '影巢'

    if _try_download_from_channel_first(
        tmdb_id,
        media_type,
        title,
        item_label=item_label,
        target_season=target_season,
        require_complete=require_complete,
        year=year,
    ):
        return '频道'

    return None



def _normalize_missing_episode_numbers(value) -> List[int]:
    """统一订阅 SUBSCRIBED 补库用：把数据库 JSONB/字符串里的缺集号整理成 int 列表。"""
    if value in (None, ''):
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            value = re.split(r'[，,\s]+', value.strip()) if value.strip() else []
    if not isinstance(value, (list, tuple, set)):
        value = [value]

    out = []
    for v in value:
        try:
            n = int(float(v))
            if n > 0 and n not in out:
                out.append(n)
        except Exception:
            pass
    return sorted(out)


def _apply_watchlist_mp_wash_flags(
    mp_payload: Dict[str, Any],
    watchlist_config: Dict[str, Any],
    *,
    force_full: bool = False
) -> str:
    """
    根据追剧配置决定向 MoviePilot 提交的洗版参数。

    - `force_full=True`: 强制全集洗版，忽略开关。
    - 两个开关都关: 不携带 best_version
    - 分集洗版: 携带 best_version
    - 全集洗版: 携带 best_version + best_version_full
    """
    episode_wash_enabled = bool(
        watchlist_config.get(
            'series_subscription_best_version',
            watchlist_config.get('sync_mp_subscription_episode_wash', False)
        )
    )
    full_wash_enabled = bool(
        watchlist_config.get(
            'series_subscription_best_version_full',
            watchlist_config.get('sync_mp_subscription_full_wash', False)
        )
    )

    mp_payload.pop('best_version', None)
    mp_payload.pop('best_version_full', None)

    if force_full or full_wash_enabled:
        mp_payload['best_version'] = 1
        mp_payload['best_version_full'] = 1
        return '全集洗版'

    if episode_wash_enabled:
        mp_payload['best_version'] = 1
        return '分集洗版'

    return '补缺模式'



def _extract_item_year_for_subscription(item: Dict[str, Any]) -> str:
    item = item or {}
    for _year_key in ('release_date', 'first_air_date', 'air_date', 'year'):
        _year_value = item.get(_year_key)
        if _year_value:
            _match = re.search(r'((?:19|20)\d{2})', str(_year_value))
            if _match:
                return _match.group(1)
    _match = re.search(r'\(((?:19|20)\d{2})\)', str(item.get('title') or ''))
    return _match.group(1) if _match else ''


def _prepare_shared_subscription_context(item: Dict[str, Any], tmdb_api_key=None) -> Dict[str, Any]:
    """把统一订阅 item 整理成共享中心批量探测/消费上下文。"""
    item = item or {}
    tmdb_id = item.get('tmdb_id')
    item_type = item.get('item_type')
    title = item.get('title')
    season_number = item.get('season_number')
    parent_tmdb_id = None

    if item_type in ['Series', 'Season', 'Episode']:
        if item_type == 'Season':
            parent_tmdb_id = item.get('parent_series_tmdb_id') or item.get('series_tmdb_id')
            if not parent_tmdb_id and '_' in str(tmdb_id):
                parent_tmdb_id = str(tmdb_id).split('_')[0]
            if not parent_tmdb_id:
                parent_tmdb_id = tmdb_id
        elif item_type == 'Episode':
            parent_tmdb_id = item.get('parent_series_tmdb_id') or item.get('series_tmdb_id')
            if not parent_tmdb_id and '_' in str(tmdb_id):
                parent_tmdb_id = str(tmdb_id).split('_')[0]
        else:
            parent_tmdb_id = tmdb_id

        series_name = media_db.get_series_title_by_tmdb_id(parent_tmdb_id) if parent_tmdb_id else None
        if not series_name:
            raw_title = item.get('title', '')
            parsed_name, _ = parse_series_title_and_season(raw_title, tmdb_api_key)
            series_name = parsed_name if parsed_name else raw_title
        if series_name:
            if item_type == 'Episode' and item.get('episode_number') is not None:
                try:
                    title = f"{series_name} S{int(season_number or item.get('season_number') or 0):02d}E{int(item.get('episode_number')):02d}"
                except Exception:
                    title = series_name
            else:
                title = series_name

    return {
        'item': item,
        'tmdb_id': tmdb_id,
        'item_type': item_type,
        'title': title,
        'season_number': season_number,
        'parent_tmdb_id': parent_tmdb_id,
        'year': _extract_item_year_for_subscription(item),
    }


def _shared_subscription_probe_key(prepared: Dict[str, Any]) -> str:
    item = prepared.get('item') if isinstance(prepared.get('item'), dict) else {}
    item_type = str(prepared.get('item_type') or item.get('item_type') or '').strip()
    tmdb_id = str(prepared.get('tmdb_id') or item.get('tmdb_id') or '').strip()
    season = prepared.get('season_number') if prepared.get('season_number') not in (None, '') else item.get('season_number')
    episode = item.get('episode_number')
    if item_type == 'Movie':
        return f'Movie|{tmdb_id}||'
    if item_type == 'Season':
        sid = str(prepared.get('parent_tmdb_id') or item.get('parent_series_tmdb_id') or item.get('series_tmdb_id') or tmdb_id or '').strip()
        return f'Season|{sid}|{season if season is not None else ""}|'
    if item_type == 'Series':
        return f'Series|{tmdb_id}||'
    if item_type == 'Episode':
        sid = str(prepared.get('parent_tmdb_id') or item.get('parent_series_tmdb_id') or item.get('series_tmdb_id') or tmdb_id or '').strip()
        return f'Season|{sid}|{season if season is not None else ""}|'
    return ''


def _is_shared_rapid_action(action_type: str) -> bool:
    """统一判断共享中心 Rapid v2 秒传动作。

    旧分享模式里 action_type 可能是“共享资源/共享永久转存/共享虚拟”；
    Rapid v2 统一使用“共享资源秒传”，这里集中兼容展示与状态更新判断，
    防止订阅成功后因为 action_type 不在旧白名单里而漏更新数据库。
    """
    text = str(action_type or '').strip()
    return text in {
        '共享资源',
        '共享资源秒传',
        '共享秒传',
        'Rapid秒传',
        '中心秒传',
    }


def _shared_rapid_action_tag(action_type: str) -> str:
    return '共享秒传' if _is_shared_rapid_action(action_type) else str(action_type or '')

# ★★★ 内部辅助函数：处理整部剧集的精细化订阅 ★★★
# ==============================================================================
def _subscribe_full_series_with_logic(tmdb_id: int, series_name: str, config: Dict, tmdb_api_key: str, source: Dict = None) -> bool:
    """
    处理整部剧集的订阅：
    1. 查询 TMDb 获取所有季。
    2. 遍历所有季。
    3. 检查是否未上映 -> 设为 PENDING_RELEASE。
    5. 检查是否完结/配置开启 -> 决定 best_version。
    6. 逐季提交订阅并更新本地数据库。
    """
    watchlist_config = settings_db.get_setting('watchlist_config') or {}
    tg_channel_tracking = watchlist_config.get('tg_channel_tracking', False)

    try:
        # 1. 获取剧集详情
        series_details = tmdb.get_tv_details(tmdb_id, tmdb_api_key)
        if not series_details:
            logger.error(f"  ➜ 无法获取剧集 ID {tmdb_id} 的详情，跳过订阅。")
            return False

        # 规范化名称
        final_series_name = series_details.get('name', series_name)
        series_poster = series_details.get('poster_path')
        series_backdrop = series_details.get('backdrop_path')

        # 2. 获取所有有效季 (Season > 0)
        seasons = series_details.get('seasons', [])
        valid_seasons = sorted([s for s in seasons if s.get('season_number', 0) > 0], key=lambda x: x['season_number'])

        if not valid_seasons:
            logger.warning(f"  ➜ 剧集《{final_series_name}》没有有效的季信息，尝试直接订阅整剧。")
            # 兜底：直接订阅 ID
            mp_payload = {"name": final_series_name, "tmdbid": tmdb_id, "type": "电视剧"}
            wash_mode = _apply_watchlist_mp_wash_flags(mp_payload, watchlist_config)
            logger.info(f"  ➜ 《{final_series_name}》整剧兜底订阅使用 {wash_mode}。")
            return moviepilot.subscribe_with_custom_payload(mp_payload, config)

        # 3. 确定最后一季的季号
        last_season_num = valid_seasons[-1]['season_number']
        any_success = False

        # ★★★ 关键步骤 1：先激活父剧集 ★★★
        watchlist_db.add_item_to_watchlist(str(tmdb_id), final_series_name)

        logger.info(f"  ➜ 正在处理《{final_series_name}》的 {len(valid_seasons)} 个季 (S{valid_seasons[0]['season_number']} - S{last_season_num})...")

        # 4. 遍历逐个订阅
        for season in valid_seasons:
            s_num = season['season_number']
            s_id = season.get('id') # 季的 TMDb ID
            air_date_str = season.get('air_date')

            # 优先使用季海报，没有则使用剧集海报
            season_poster = season.get('poster_path')
            # 如果概要中缺失日期，强制获取季详情
            if not air_date_str:
                logger.debug(f"  ➜ S{s_num} 概要信息缺失发行日期，正在获取详细信息...")
                season_details_deep = tmdb.get_tv_season_details(tmdb_id, s_num, tmdb_api_key)

                if season_details_deep:
                    # 1. 尝试直接获取季日期
                    air_date_str = season_details_deep.get('air_date')

                    # 2. ★★★ 新增：如果季日期仍为空，遍历分集找最早的日期 ★★★
                    if not air_date_str and 'episodes' in season_details_deep:
                        episodes = season_details_deep['episodes']
                        # 提取所有有效的 air_date
                        valid_dates = [e.get('air_date') for e in episodes if e.get('air_date')]
                        if valid_dates:
                            # 取最早的一个日期
                            air_date_str = min(valid_dates)
                            logger.debug(f"  ➜ 从分集数据中推断出 S{s_num} 发行日期: {air_date_str}")

                    # 补全海报和简介
                    if not season_poster: season_poster = season_details_deep.get('poster_path')
                    if not season.get('overview'): season['overview'] = season_details_deep.get('overview')
            final_poster = season_poster if season_poster else series_poster

            # ==============================================================
            # 逻辑 A: 检查是否未上映 (Pending Release)
            # ==============================================================
            is_future_season = False
            # 如果有日期且大于今天，或者干脆没有日期(视为待定/未上映)，都标记为未上映
            if air_date_str:
                try:
                    air_date = datetime.strptime(air_date_str, "%Y-%m-%d").date()
                    if air_date > datetime.now().date():
                        is_future_season = True
                except ValueError:
                    pass
            else:
                # 如果深挖了详情还是没有日期，通常意味着 TBD (To Be Determined)，也应视为未上映，防止错误订阅
                is_future_season = True
                logger.info(f"  ➜ 季《{final_series_name}》S{s_num} 无发行日期，视为 '待上映'。")

            if is_future_season:
                logger.info(f"  ➜ 《{final_series_name}》第 {s_num} 季 尚未播出 ({air_date_str})，已加入待上映列表。")

                media_info = {
                    'tmdb_id': str(s_id) if s_id else f"{tmdb_id}_S{s_num}",
                    'title': season.get('name', f"第 {s_num} 季"),
                    'season_number': s_num,
                    'parent_series_tmdb_id': str(tmdb_id),
                    'release_date': air_date_str,
                    'poster_path': final_poster,
                    'backdrop_path': series_backdrop,
                    'overview': season.get('overview')
                }

                request_db.set_media_status_pending_release(
                    tmdb_ids=media_info['tmdb_id'],
                    item_type='Season',
                    source=source,
                    media_info_list=[media_info]

                )
                any_success = True
                continue

            # ==============================================================
            # 逻辑 B: 自动待定检查 (Auto Pending)
            # ==============================================================
            # 针对刚上映但集数信息不全的剧集，我们需要将其在 MP 中标记为 'P' (待定)
            # 并设置一个虚假的总集数，防止 MP 下载完现有集数后直接完结订阅。
            is_pending_logic, fake_total_episodes = should_mark_as_pending(tmdb_id, s_num, tmdb_api_key)

            if is_pending_logic:
                logger.info(f"  ➜ 季《{final_series_name}》S{s_num} 满足自动待定条件，将执行 [订阅 -> 转待定] 流程。")

            # ==============================================================
            # 逻辑 C: 准备订阅 Payload
            # ==============================================================
            mp_payload = {
                "name": final_series_name,
                "tmdbid": tmdb_id,
                "type": "电视剧",
                "season": s_num
            }

            # ==============================================================
            # 逻辑 D: 决定 Best Version (洗版/完结检测)
            # ==============================================================
            # 只有在【不满足】待定条件时，才去检查完结状态。
            # 如果已经是待定状态，说明肯定没完结，不需要检查，也不应该开启洗版。
            is_completed = False
            if not is_pending_logic:
                is_completed = check_series_completion(
                    tmdb_id,
                    tmdb_api_key,
                    season_number=s_num,
                    series_name=final_series_name
                )
                wash_mode = _apply_watchlist_mp_wash_flags(
                    mp_payload,
                    watchlist_config,
                    force_full=is_completed
                )
                if is_completed:
                    logger.info(f"  ➜ S{s_num} 已完结，强制使用 {wash_mode} 订阅。")
                else:
                    logger.info(f"  ➜ S{s_num} 未完结，向 MoviePilot 提交 {wash_mode} 订阅。")
            else:
                logger.info(f"  ➜ S{s_num} 处于待定模式，向 MoviePilot 提交补缺订阅。")

            # ==============================================================
            # 逻辑 E: 提交订阅 & 后置状态修正
            # ==============================================================
            # ★★★ 修改开始：拦截 TG 频道追更 ★★★
            if tg_channel_tracking and not is_completed:
                logger.info(f"  ➜ [策略] TG频道追更已开启，跳过向 MoviePilot 提交未完结季 S{s_num} 的订阅。")
                mp_submit_success = True # 模拟成功，以便更新本地数据库状态为已订阅
                is_pending_logic = False # 既然没提交给MP，就不需要去MP改待定状态了
            else:
                mp_submit_success = moviepilot.subscribe_with_custom_payload(mp_payload, config)

            if mp_submit_success:
                any_success = True

                # ★★★ 核心修复：如果是待定逻辑，订阅成功后立即修改 MP 状态 ★★★
                if is_pending_logic:
                    logger.info(f"  ➜ [后置操作] 正在将 S{s_num} 的状态修改为 'P' (待定)，并将总集数修正为 {fake_total_episodes}...")
                    # 调用 moviepilot.py 中的 update_subscription_status
                    # 注意：这里传入 fake_total_episodes 以防止 MP 自动完结
                    mp_update_success = moviepilot.update_subscription_status(
                        tmdb_id=tmdb_id,
                        season=s_num,
                        status='P', # P = Pending
                        config=config,
                        total_episodes=fake_total_episodes
                    )
                    if mp_update_success:
                        logger.info(f"  ➜ S{s_num} 已成功转为待定状态。")
                    else:
                        logger.warning(f"  ➜ S{s_num} 订阅成功，但转待定状态失败。")

                # 订阅成功后，更新本地数据库状态为 SUBSCRIBED
                # (即使 MP 是 Pending，对于本地请求队列来说，它也算是“已处理/已订阅”)
                target_s_id = str(s_id) if s_id else f"{tmdb_id}_S{s_num}"
                media_info = {
                    'tmdb_id': target_s_id,
                    'parent_series_tmdb_id': str(tmdb_id),
                    'season_number': s_num,
                    'title': season.get('name'),
                    'poster_path': final_poster,
                    'backdrop_path': series_backdrop,
                    'release_date': air_date_str
                }
                request_db.set_media_status_subscribed(
                    tmdb_ids=[target_s_id],
                    item_type='Season',
                    source=source,
                    media_info_list=[media_info]
                )

        return any_success

    except Exception as e:
        logger.error(f"处理整剧订阅逻辑时出错: {e}", exc_info=True)
        return False

# ★★★ 手动动订阅任务 ★★★
def task_manual_subscribe_batch(processor, subscribe_requests: List[Dict]):
    """
    手动订阅任务
    """
    total_items = len(subscribe_requests)
    task_name = f"手动订阅 {total_items} 个项目"
    logger.trace(f"--- 开始执行 '{task_name}' 任务 ---")

    task_manager.update_status_from_thread(0, "正在准备手动订阅任务...")

    if not subscribe_requests:
        task_manager.update_status_from_thread(100, "任务完成：没有需要处理的项目。")
        return

    try:
        config = config_manager.APP_CONFIG
        tmdb_api_key = config.get(constants.CONFIG_OPTION_TMDB_API_KEY)
        watchlist_config = settings_db.get_setting('watchlist_config') or {}
        tg_channel_tracking = watchlist_config.get('tg_channel_tracking', False)

        processed_count = 0

        for i, req in enumerate(subscribe_requests):
            tmdb_id = req.get('tmdb_id')
            item_type = req.get('item_type')
            item_title_for_log = req.get('title', f"ID: {tmdb_id}")
            season_number = req.get('season_number')
            user_id = req.get('user_id')

            # 构建来源信息 (用于后续通知)
            source = None
            if user_id:
                source = {'type': 'user_request', 'user_id': user_id}

            if not tmdb_id or not item_type:
                logger.warning(f"跳过一个无效的订阅请求: {req}")
                continue

            task_manager.update_status_from_thread(
                int((i / total_items) * 100),
                f"({i+1}/{total_items}) 正在处理: {item_title_for_log}"
            )

            # 检查配额
            if settings_db.get_subscription_quota() <= 0:
                logger.warning("  ➜ 每日订阅配额已用尽，任务提前结束。")
                break

            success = False

            # ==================================================================
            # 逻辑分支 1: 剧集 / 季
            # ==================================================================
            if item_type == 'Series' or item_type == 'Season':
                # 1. ★★★ 核心修复：ID 和 季号 修正 ★★★
                if item_type == 'Season':
                    # 尝试从请求中获取父剧集 ID (统一订阅页面传过来的是 series_tmdb_id 或 parent_series_tmdb_id)
                    parent_id = req.get('series_tmdb_id') or req.get('parent_series_tmdb_id')

                    # 如果请求里没有，去数据库查 (说明传入的 tmdb_id 可能是季 ID)
                    if not parent_id:
                        season_info = media_db.get_media_details(str(tmdb_id), 'Season')
                        if season_info:
                            parent_id = season_info.get('parent_series_tmdb_id')
                            if season_number is None:
                                season_number = season_info.get('season_number')

                    # 如果找到了父剧集 ID，且与当前 tmdb_id 不同，说明传入的是季 ID
                    # 必须将其替换为父剧集 ID，因为后续的 check_series_completion 和 MP 订阅都需要剧集 ID
                    if parent_id and str(parent_id) != str(tmdb_id):
                        logger.debug(f"  ➜ [ID修正] 将季 ID {tmdb_id} 替换为父剧集 ID {parent_id}")
                        tmdb_id = parent_id

                # 2. 处理单季订阅 (最常见情况)
                if season_number is not None:
                    series_name = media_db.get_series_title_by_tmdb_id(str(tmdb_id))
                    if not series_name: series_name = item_title_for_log

                    mp_payload = {
                        "name": series_name,
                        "tmdbid": int(tmdb_id),
                        "type": "电视剧",
                        "season": int(season_number)
                    }

                    # B. ★★★ 核心：完结状态检查 ★★★
                    is_completed = check_series_completion(
                        int(tmdb_id),
                        tmdb_api_key,
                        season_number=season_number,
                        series_name=series_name
                    )
                    wash_mode = _apply_watchlist_mp_wash_flags(
                        mp_payload,
                        watchlist_config,
                        force_full=is_completed
                    )

                    if is_completed:
                        logger.info(f"  ➜ [手动订阅] 第{season_number}季 已完结，强制使用 {wash_mode}。")
                    else:
                        logger.info(f"  ➜ [手动订阅] 第{season_number}季 尚未完结，使用 {wash_mode}。")

                    # ★★★ 拦截 TG 频道追更 ★★★
                    if tg_channel_tracking and not is_completed:
                        logger.info(f"  ➜ [策略] TG频道追更已开启，跳过向 MoviePilot 提交未完结季 S{season_number} 的订阅。")
                        success = True # 模拟成功
                    else:
                        success = moviepilot.subscribe_with_custom_payload(mp_payload, config)

                # 3. 处理整剧订阅 (Series)
                elif item_type == 'Series':
                    # 调用整剧处理逻辑 (内部会遍历所有季)
                    success = _subscribe_full_series_with_logic(
                        tmdb_id=int(tmdb_id),
                        series_name=item_title_for_log,
                        config=config,
                        tmdb_api_key=tmdb_api_key,
                        source=source
                    )
                    if success:
                        # 整剧订阅成功只代表订阅任务已提交，不代表整剧已完成。
                        # Series/Season/Episode 的订阅状态由统一订阅与智能追剧接管；
                        # 只有本地完美完结后才清 NONE。
                        logger.info(f"  ➜ [订阅状态] 整剧订阅已提交，保留《{item_title_for_log}》的订阅状态，等待智能追剧完美完结后清理。")

                else:
                    logger.error(f"  ➜ 订阅失败：季《{item_title_for_log}》缺少季号信息。")
                    continue

            # ==================================================================
            # 逻辑分支 2: 电影
            # ==================================================================
            elif item_type == 'Movie':
                if not is_movie_subscribable(int(tmdb_id), tmdb_api_key, config):
                    logger.warning(f"  ➜ 电影《{item_title_for_log}》不满足发行日期条件，跳过订阅。")
                    continue

                mp_payload = {"name": item_title_for_log, "tmdbid": int(tmdb_id), "type": "电影"}
                # 电影手动订阅，通常意味着用户现在就想看，且电影一般没有“连载”概念
                # 可以默认开启 best_version=1 以获取更好质量，或者保持默认 0
                # 这里保持默认 0 比较稳妥，除非用户明确是洗版操作，但为了简化，这里不设 best_version
                success = moviepilot.subscribe_with_custom_payload(mp_payload, config)

            # ==================================================================
            # 结果处理
            # ==================================================================
            if success:
                logger.info(f"  ➜ 《{item_title_for_log}》订阅成功！")
                settings_db.decrement_subscription_quota()

                # 更新数据库状态 (Series 类型在 _subscribe_full_series_with_logic 里处理了)
                if item_type != 'Series':
                    # 如果是季，需要构建正确的 ID (例如 tmdbid_S1)
                    # 这里的 tmdb_id 已经被修正为 Series ID，所以需要重新构建 Season ID
                    target_id_for_update = str(tmdb_id)
                    if item_type == 'Season' and season_number is not None:
                         # 尝试查询真实的季 ID，查不到则用拼接 ID
                         real_season_id = request_db.get_season_tmdb_id(str(tmdb_id), season_number)
                         target_id_for_update = real_season_id if real_season_id else f"{tmdb_id}_S{season_number}"

                    request_db.set_media_status_subscribed(
                        tmdb_ids=[target_id_for_update],
                        item_type=item_type,
                    )

                processed_count += 1
            else:
                logger.error(f"  ➜ 订阅《{item_title_for_log}》失败，请检查 MoviePilot 日志。")

        final_message = f"  ➜ 手动订阅任务完成，成功处理 {processed_count}/{total_items} 个项目。"
        task_manager.update_status_from_thread(100, final_message)
        logger.info(f"--- '{task_name}' 任务执行完毕 ---")

    except Exception as e:
        logger.error(f"  ➜ {task_name} 任务失败: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"错误: {e}")

# ★★★ 自动订阅任务 ★★★
def task_auto_subscribe(processor):
    """
    【V2 - 统一订阅处理器】
    """
    task_name = "统一订阅处理"
    logger.trace(f"--- 开始执行 '{task_name}' 任务 ---")

    task_manager.update_status_from_thread(0, "正在加载订阅策略...")
    config = config_manager.APP_CONFIG

    # 1. 加载策略配置 (优先从数据库读取，如果没有则使用默认值)
    strategy_config = settings_db.get_setting('subscription_strategy_config') or {}
    mp_config = settings_db.get_setting('mp_config') or {}

    # 默认策略参数
    movie_protection_days = int(mp_config.get('movie_protection_days', 180))    # 默认半年新片保护
    movie_search_window = int(mp_config.get('movie_search_window_days', 1))     # 默认搜索1天
    movie_pause_days = int(mp_config.get('movie_pause_days', 7))                # 默认暂停7天
    timeout_revive_days = int(mp_config.get('timeout_revive_days', 0))          # 默认不复活超时订阅

    # 2. 读取请求延迟配置
    try:
        mp_config = settings_db.get_setting('mp_config') or {}
        request_delay = int(mp_config.get('resubscribe_delay_seconds', 0))
    except:
        request_delay = 0

    try:
        # ======================================================================
        # 阶段 1 - 清理超时订阅
        # ======================================================================
        if movie_search_window > 0:
            logger.info(f"  ➜ 正在检查超过 {movie_search_window} 天仍未入库的订阅...")
            task_manager.update_status_from_thread(2, "正在清理超时订阅...")

            stale_items = request_db.get_stale_subscribed_media(movie_search_window, movie_protection_days)

            if stale_items:
                logger.warning(f"  ➜ 发现 {len(stale_items)} 个超时订阅，准备处理。")
                cancelled_ids_map = {}
                cancelled_for_report = []

                for item in stale_items:
                    tmdb_id_to_cancel = item['tmdb_id']
                    item_type = item['item_type']
                    title = item['title']
                    season_to_cancel = None

                    if item_type == 'Season':
                        if item['parent_series_tmdb_id']:
                            tmdb_id_to_cancel = item['parent_series_tmdb_id']
                            season_to_cancel = item['season_number']
                        else:
                            logger.error(f"  ➜ 无法取消季《{item['title']}》，因为它缺少父剧集ID。")
                            continue

                    # --- 取消 MP 订阅 ---
                    success = moviepilot.cancel_subscription(
                        tmdb_id=tmdb_id_to_cancel,
                        item_type=item_type,
                        config=config,
                        season=season_to_cancel
                    )

                    if success:
                        if item_type not in cancelled_ids_map:
                            cancelled_ids_map[item_type] = []
                        cancelled_ids_map[item_type].append(item['tmdb_id'])

                        display_title = title
                        if item_type == 'Season':
                            parent_id = item.get('parent_series_tmdb_id')
                            s_num = item.get('season_number')
                            if parent_id:
                                series_title = media_db.get_series_title_by_tmdb_id(str(parent_id))
                                if series_title and s_num is not None:
                                    display_title = f"{series_title} 第 {s_num} 季"

                        cancelled_for_report.append(f"《{display_title}》")

                # 1. 批量更新数据库状态
                for item_type, tmdb_ids in cancelled_ids_map.items():
                    if tmdb_ids:
                        request_db.set_media_status_ignored(
                            tmdb_ids=tmdb_ids,
                            item_type=item_type,
                            source={"type": "auto_ignored", "reason": "stale_subscription"},
                            ignore_reason="订阅超时"
                        )

                # 2. 发送取消通知
                if cancelled_for_report:
                    admin_chat_ids = user_db.get_admin_telegram_chat_ids()
                    if admin_chat_ids:
                        items_list_str = "\n".join([f"· `{item}`" for item in cancelled_for_report])
                        message_text = (f"➜ *自动取消了 {len(cancelled_for_report)} 个超时订阅*\n\n"
                                        f"下列项目因超过 {movie_search_window} 天未入库而被自动取消：\n{items_list_str}")
                        for admin_id in admin_chat_ids:
                            telegram.send_telegram_message(admin_id, message_text, disable_notification=True)

            else:
                logger.info("  ➜ 未发现超时订阅。")

        # ======================================================================
        # 阶段 1.5 - 清理下载超时并重新订阅
        # ======================================================================
        download_timeout_hours = int(mp_config.get('download_timeout_hours', 0))
        if download_timeout_hours > 0:
            logger.info(f"  ➜ [策略] 检查下载超时超过 {download_timeout_hours} 小时的任务...")
            task_manager.update_status_from_thread(5, "正在检查下载超时任务...")

            downloading_tasks = moviepilot.get_downloading_tasks(config)
            if downloading_tasks:
                all_subs = media_db.get_all_subscriptions()

                # 获取带本地时区的当前时间
                now = datetime.now().astimezone()
                timeout_threshold = now - timedelta(hours=download_timeout_hours)

                for item in all_subs:
                    if item.get('subscription_status') != 'SUBSCRIBED':
                        continue

                    last_sub_str = item.get('last_subscribed_at')
                    if not last_sub_str:
                        continue

                    # 健壮的时间解析：处理带毫秒和时区的字符串 (如 2026-03-21 17:51:17.554 +0800)
                    if isinstance(last_sub_str, datetime):
                        last_sub_time = last_sub_str
                        if last_sub_time.tzinfo is None:
                            last_sub_time = last_sub_time.astimezone()
                    else:
                        try:
                            # 尝试标准化 ISO 格式
                            clean_str = str(last_sub_str).replace(" ", "T", 1)
                            if " +" in clean_str or " -" in clean_str:
                                clean_str = clean_str.replace(" +", "+").replace(" -", "-")
                            if re.search(r'[+-]\d{4}$', clean_str):
                                clean_str = clean_str[:-2] + ":" + clean_str[-2:]
                            last_sub_time = datetime.fromisoformat(clean_str)
                        except Exception:
                            try:
                                # 降级处理：去掉毫秒和时区，当做本地时间
                                last_sub_time = datetime.strptime(str(last_sub_str).split('.')[0], "%Y-%m-%d %H:%M:%S").astimezone()
                            except Exception:
                                continue

                    # 如果订阅时间早于超时阈值，说明超时了
                    if last_sub_time < timeout_threshold:
                        tmdb_id = item.get('tmdb_id')
                        item_type = item.get('item_type')
                        season_num = item.get('season_number')

                        # 确定要比对的真实 TMDb ID
                        target_tmdb_id = int(item.get('parent_series_tmdb_id') or tmdb_id)

                        for task in downloading_tasks:
                            task_media = task.get('media', {})
                            if not task_media:
                                continue

                            task_tmdbid = task_media.get('tmdb_id') or task_media.get('tmdbid')
                            task_season = task_media.get('season')

                            # 匹配 TMDb ID 和 季号
                            if str(task_tmdbid) == str(target_tmdb_id):
                                if item_type == 'Season' and str(task_season) != str(season_num):
                                    continue

                                task_hash = task.get('hash')

                                # MP的下载列表中，'title' 是原始种子名，'name' 是洗白后的媒体名
                                raw_title = task.get('title', '')
                                clean_media_name = task.get('name', '')

                                # 优先使用 title 作为种子名来精准排除
                                torrent_name = raw_title if raw_title else clean_media_name

                                logger.warning(f"  ➜ 发现超时下载任务: 《{clean_media_name}》 (已订阅超过 {download_timeout_hours} 小时)")

                                # 1. 提取要排除的关键词（去除容易引起正则错误的括号，保留核心文件名）
                                exclude_keywords = set()
                                # 去除扩展名
                                clean_torrent_name = re.sub(r'\.(mkv|mp4|ts|avi|torrent)$', '', torrent_name, flags=re.IGNORECASE).strip()
                                # 去除开头的 [xxx] 或 【xxx】 这种容易让 MP 正则引擎懵逼的符号
                                clean_torrent_name = re.sub(r'^\[[^\]]+\]|^【[^】]+】', '', clean_torrent_name).strip()
                                # 去除开头可能残留的点或空格 (例如 "[狂怒].Fury" 变成 "Fury")
                                clean_torrent_name = clean_torrent_name.lstrip('. ')

                                if clean_torrent_name:
                                    exclude_keywords.add(clean_torrent_name)

                                # 2. 删除下载器中的任务
                                if moviepilot.delete_download_tasks("dummy", config, hashes=[task_hash]):
                                    logger.info(f"    - 已删除超时下载任务: {task_hash[:8]}...")

                                    # 3. 更新 MP 订阅规则，排除该死种
                                    sub_info = moviepilot.get_subscription_by_tmdbid(target_tmdb_id, season_num if item_type == 'Season' else None, config)

                                    if sub_info and sub_info.get('id'):
                                        # 剧集未完结时，订阅通常还在，直接更新现有订阅
                                        if exclude_keywords:
                                            current_exclude = sub_info.get('exclude') or ""
                                            exclude_list = [e.strip() for e in current_exclude.split(',') if e.strip()]
                                            added_any = False
                                            for kw in exclude_keywords:
                                                if kw not in exclude_list:
                                                    exclude_list.append(kw)
                                                    added_any = True

                                            if added_any:
                                                sub_info['exclude'] = ",".join(exclude_list)
                                                if moviepilot.update_subscription(sub_info, config):
                                                    logger.info(f"    - 已更新现有订阅规则，排除死种: {', '.join(exclude_keywords)}")

                                        # 4. 触发重新搜索
                                        moviepilot.search_subscription(sub_info['id'], config)
                                        logger.info(f"    - 已触发重新搜索")
                                    else:
                                        # 电影或已完结剧集，MP 会在下载开始后删除订阅，因此需要重新提交
                                        logger.info(f"    - MP 中订阅已自动移除(正常现象)，正在重新提交订阅并追加排除规则...")

                                        payload = {
                                            "tmdbid": int(target_tmdb_id),
                                            "type": "电影" if item_type == 'Movie' else "电视剧"
                                        }

                                        if item_type == 'Season' and season_num is not None:
                                            payload['season'] = int(season_num)
                                            series_name = media_db.get_series_title_by_tmdb_id(str(target_tmdb_id))
                                            if series_name:
                                                payload['name'] = series_name
                                        elif item_type == 'Movie':
                                            payload['name'] = item.get('title', '')

                                        if exclude_keywords:
                                            payload['exclude'] = ",".join(exclude_keywords)

                                        if moviepilot.subscribe_with_custom_payload(payload, config):
                                            logger.info(f"    - 重新订阅成功，并已排除死种: {', '.join(exclude_keywords)}")
                                        else:
                                            logger.error(f"    - 重新订阅失败！")

                                    # 5. 更新本地订阅时间，防止无限循环
                                    request_db.set_media_status_subscribed(
                                        tmdb_ids=[tmdb_id],
                                        item_type=item_type
                                    )
                                break # 跳出内层循环，处理下一个 item

        # ======================================================================
        # 阶段 2 - 电影间歇性订阅搜索
        # ======================================================================
        # 仅当配置有效时执行
        if movie_protection_days > 0 and movie_pause_days > 0:
            logger.info(f"  ➜ [策略] 执行电影间歇性订阅搜索维护...")

            # 2.1 复活 (Revive: PAUSED -> SUBSCRIBED)
            # 对应 MP 状态: 'S' -> 'R'
            movies_to_revive = request_db.get_movies_to_revive()
            if movies_to_revive:
                revived_ids = []
                for movie in movies_to_revive:
                    tmdb_id = movie['tmdb_id']
                    title = movie['title']

                    # ★★★ 修改：直接更新状态为 'R' (Run) ★★★
                    # season=None 表示电影
                    if moviepilot.update_subscription_status(int(tmdb_id), None, 'R', config):
                        revived_ids.append(tmdb_id)
                    else:
                        # 如果更新失败（比如MP里订阅丢了），尝试重新订阅兜底
                        logger.warning(f"    - 《{title}》状态切换失败，尝试重新提交订阅...")
                        if moviepilot.subscribe_with_custom_payload({"tmdbid": int(tmdb_id), "type": "电影"}, config):
                            revived_ids.append(tmdb_id)

                if revived_ids:
                    request_db.update_movie_status_revived(revived_ids)
                    logger.info(f"  ➜ 成功复活 {len(revived_ids)} 部电影 (MP状态->R)。")

            # 2.2 暂停 (Pause: SUBSCRIBED -> PAUSED)
            # 对应 MP 状态: 'R' -> 'S'
            movies_to_pause = request_db.get_movies_to_pause(search_window_days=movie_search_window, protection_days=movie_protection_days)
            if movies_to_pause:
                paused_ids = []
                for movie in movies_to_pause:
                    tmdb_id = movie['tmdb_id']
                    title = movie['title']

                    # ★★★ 修改开始：尝试暂停，失败则补订后再次暂停 ★★★
                    if moviepilot.update_subscription_status(int(tmdb_id), None, 'S', config):
                        paused_ids.append(tmdb_id)
                    else:
                        logger.warning(f"    - 《{title}》暂停失败 (MP中可能不存在)，尝试重新订阅并同步状态...")

                        # 1. 尝试补订 (默认状态通常为 R)
                        mp_payload = {"name": title, "tmdbid": int(tmdb_id), "type": "电影"}
                        if moviepilot.subscribe_with_custom_payload(mp_payload, config):
                            # 2. 补订成功后，再次尝试将其状态更新为 'S'
                            if moviepilot.update_subscription_status(int(tmdb_id), None, 'S', config):
                                paused_ids.append(tmdb_id)
                                logger.info(f"    - ➜ 《{title}》补订并暂停成功。")
                            else:
                                logger.warning(f"    - ➜ 《{title}》补订成功，但暂停状态同步失败。")
                        else:
                            logger.error(f"    - ➜ 《{title}》补订失败，无法执行暂停操作。")

                if paused_ids:
                    request_db.update_movie_status_paused(paused_ids, pause_days=movie_pause_days)
                    logger.info(f"  ➜ 成功暂停 {len(paused_ids)} 部暂无资源的新片 (MP状态->S)。")

        # ======================================================================
        # 阶段 3 - 超时订阅复活 (轮回机制)
        # ======================================================================
        if timeout_revive_days > 0:
            logger.info(f"  ➜ [策略] 检查是否有被'订阅超时'清理的项目满足复活条件 (>{timeout_revive_days}天)...")

            items_to_revive = media_db.get_timed_out_items_to_revive(timeout_revive_days)

            if items_to_revive:
                logger.info(f"  🧟 发现 {len(items_to_revive)} 个超时项目满足复活条件，正在重置为 '待订阅'...")

                revived_count = 0
                for item in items_to_revive:
                    # 将状态重置为 WANTED，且 force_unignore=True 以允许从 IGNORED 状态流转
                    # source 设为 auto_revive 以便追踪
                    request_db.set_media_status_wanted(
                        tmdb_ids=[item['tmdb_id']],
                        item_type=item['item_type'],
                        source={"type": "revive_from_timeout", "reason": "auto_revive_from_timeout"}, # 使用 manual_add 类型确保能被 set_media_status_wanted 处理
                        force_unignore=True
                    )
                    revived_count += 1
                    logger.debug(f"    - 《{item['title']}》已复活。")

                logger.info(f"  ➜ 成功复活了 {revived_count} 个项目，它们将在本次或下次任务中被重新处理。")
            else:
                logger.debug("  ➜ 没有满足复活条件的项目。")

        # ======================================================================
        # 阶段 4 - 执行订阅
        # ======================================================================
        logger.info("  ➜ 正在检查未上映...")
        promoted_count = media_db.promote_pending_to_wanted()
        if promoted_count > 0:
            logger.info(f"  ➜ 成功将 {promoted_count} 个项目从“未上映”更新为“待订阅”。")
        else:
            logger.trace("  ➜ 没有需要晋升状态的媒体项。")

        wanted_items = media_db.get_all_wanted_media()
        if not wanted_items:
            logger.info("  ➜ 待订阅列表为空，无需处理。")
            task_manager.update_status_from_thread(100, "待订阅列表为空。")
            return

        logger.info(f"  ➜ 发现 {len(wanted_items)} 个待处理的订阅请求。")
        task_manager.update_status_from_thread(10, f"发现 {len(wanted_items)} 个待处理请求...")

        tmdb_api_key = config.get(constants.CONFIG_OPTION_TMDB_API_KEY)
        subscription_details = []
        rejected_details = []
        notifications_to_send = {}
        failed_notifications_to_send = {}
        quota_exhausted = False

        configured_sources = strategy_config.get('subscription_sources')
        configured_sources = configured_sources if isinstance(configured_sources, list) else []
        shared_probe_by_key = {}
        if 'shared_pool' in configured_sources and batch_probe_shared_resources and try_consume_preprobed_shared_resource:
            shared_probe_contexts = []
            quota_for_shared_probe = settings_db.get_subscription_quota()
            for _item in wanted_items:
                try:
                    _item_type = _item.get('item_type')
                    if _item_type == 'Episode':
                        continue
                    _status = str(_item.get('subscription_status') or 'NONE').strip().upper()
                    # 只有真正待订阅(WANTED)的新请求才需要做发行日期保护；
                    # SUBSCRIBED / NONE 等补库项只查询共享中心/云资源，不再触发电影发行日期检查日志。
                    if _status == 'WANTED' and _item_type == 'Movie' and not is_movie_subscribable(int(_item.get('tmdb_id')), tmdb_api_key, config):
                        continue
                    # 只有真正待订阅(WANTED)的新请求才受订阅配额限制；
                    # SUBSCRIBED / NONE 等追更补库项仍允许查询共享中心，但绝不应因此流入 MP。
                    if _status == 'WANTED' and quota_for_shared_probe <= 0:
                        continue
                    if _item_type in ['Movie', 'Series', 'Season']:
                        shared_probe_contexts.append(_prepare_shared_subscription_context(_item, tmdb_api_key))
                except Exception as e:
                    logger.debug(f"  ➜ [共享资源] 准备批量探测项失败，已跳过: {_item.get('title')} -> {e}")
            if shared_probe_contexts:
                task_manager.update_status_from_thread(10, f"正在批量查询共享中心：{len(shared_probe_contexts)} 个订阅...")
                probe_batch = batch_probe_shared_resources(shared_probe_contexts, limit_per_item=200) or {}
                if probe_batch.get('supported'):
                    shared_probe_by_key = probe_batch.get('by_key') or {}

        # 2. 遍历待办列表，逐一处理
        for i, item in enumerate(wanted_items):
            if processor.is_stop_requested(): break

            task_manager.update_status_from_thread(
                int(10 + (i / len(wanted_items)) * 85),
                f"({i+1}/{len(wanted_items)}) 正在处理: {item['title']}"
            )

            # ★★★ 1. 准备基础信息 (提前获取剧集标题，用于日志和搜索) ★★★
            subscription_status = str(item.get('subscription_status') or 'NONE').strip().upper()
            is_wanted_subscription = subscription_status == 'WANTED'
            is_subscribed_recheck = subscription_status == 'SUBSCRIBED'
            missing_episode_numbers = _normalize_missing_episode_numbers(item.get('missing_episode_numbers'))

            tmdb_id = item['tmdb_id']
            item_type = item['item_type']
            title = item['title'] # 默认为 item 标题
            season_number = item.get('season_number')

            # 2.1 发行日期保护只属于 WANTED 新订阅。
            # 非 WANTED 的电影/追更补库项不再做发行日期检查，避免统一订阅日志被补库项刷屏。
            if is_wanted_subscription and item_type == 'Movie' and not is_movie_subscribable(int(tmdb_id), tmdb_api_key, config):
                logger.info(f"  ➜ 电影《{title}》未到发行日期，本次跳过。")
                rejected_details.append({'item': f"电影《{title}》", 'reason': '未发行'})
                # ★★★ 新增：解析来源并记录失败通知 ★★★
                sources = item.get('subscription_sources_json', [])
                for source in sources:
                    if source.get('type') == 'user_request' and (user_id := source.get('user_id')):
                        if user_id not in failed_notifications_to_send:
                            failed_notifications_to_send[user_id] = []
                        failed_notifications_to_send[user_id].append(f"《{title}》(原因: 不满足发行日期延迟订阅)")
                continue

            # 统一订阅任务不再处理 Episode 队列项。
            # 单集只作为 Season.missing_episode_numbers 参与本地精确过滤；
            # 共享中心已经改为入池广播，不再登记客户端缺口；统一订阅只按 Season 粒度查询/消费已有资源。
            if item_type == 'Episode':
                logger.warning(
                    f"  ➜ [队列保护] 《{title}》仍以 Episode 进入统一订阅队列，已跳过；"
                    f"请检查 database.media_db.get_all_wanted_media 是否已应用季粒度补丁。"
                )
                continue

            item_year = ''
            for _year_key in ('release_date', 'first_air_date', 'air_date', 'year'):
                _year_value = item.get(_year_key)
                if _year_value:
                    _match = re.search(r'((?:19|20)\d{2})', str(_year_value))
                    if _match:
                        item_year = _match.group(1)
                        break
            if not item_year:
                _match = re.search(r'\(((?:19|20)\d{2})\)', str(item.get('title') or ''))
                if _match:
                    item_year = _match.group(1)
            parent_tmdb_id = None

            # 如果是剧/季/集，统一修正父剧 ID 与标题。
            # Episode 也必须走这里：共享中心的单集消费以“父剧 TMDb + SxxEyy”为主键。
            if item_type in ['Series', 'Season', 'Episode']:
                if item_type == 'Season':
                    parent_tmdb_id = item.get('parent_series_tmdb_id') or item.get('series_tmdb_id')
                    # 尝试解析 ID
                    if not parent_tmdb_id and '_' in str(tmdb_id):
                        parent_tmdb_id = str(tmdb_id).split('_')[0]
                    if not parent_tmdb_id:
                        parent_tmdb_id = tmdb_id
                elif item_type == 'Episode':
                    parent_tmdb_id = item.get('parent_series_tmdb_id') or item.get('series_tmdb_id')
                    # 极少数历史占位 ID 可能是 124364_S4E6 / 124364_E6 这类，兜底拆出父剧 ID。
                    if not parent_tmdb_id and '_' in str(tmdb_id):
                        parent_tmdb_id = str(tmdb_id).split('_')[0]
                    # Episode 的 tmdb_id 可能是“集自身 ID”，不能盲目当父剧 ID。
                else:
                    parent_tmdb_id = tmdb_id

                # 获取剧集名称
                series_name = media_db.get_series_title_by_tmdb_id(parent_tmdb_id) if parent_tmdb_id else None
                if not series_name:
                     # 尝试从 item title 解析 (例如 "Breaking Bad - S1")
                     raw_title = item.get('title', '')
                     parsed_name, _ = parse_series_title_and_season(raw_title, tmdb_api_key)
                     series_name = parsed_name if parsed_name else raw_title

                # 更新 title 变量为剧集标题；Episode 日志补上 SxxEyy，避免只显示剧名看不出目标集。
                if series_name:
                    if item_type == 'Episode' and item.get('episode_number') is not None:
                        try:
                            title = f"{series_name} S{int(season_number or item.get('season_number') or 0):02d}E{int(item.get('episode_number')):02d}"
                        except Exception:
                            title = series_name
                    else:
                        title = series_name

            if (not is_wanted_subscription) and item_type == 'Season':
                logger.info(
                    f"  ➜ [追更模式] 《{title}》 第 {int(season_number or 0):02d} 季 "
                    f"缺失集: {missing_episode_numbers or '未知/按季处理'}"
                )

            # --- MoviePilot 订阅保护 ---
            # 只有 subscription_status=WANTED 的新请求才允许进入 MP 订阅链路。
            # SUBSCRIBED / NONE / PAUSED 等由追更或补库带进队列的项目，只能走共享池/云资源补库，
            # 不能因为不等于 SUBSCRIBED 就被误判为新订阅。
            if is_wanted_subscription and settings_db.get_subscription_quota() <= 0:
                quota_exhausted = True
                logger.warning(f"  ➜ 每日订阅配额已用尽，跳过待订阅项目《{title}》，继续处理非 MP 补库项。")
                continue

            # 提交 MP 订阅
            success = False
            action_type = "MP"
            watchlist_config = settings_db.get_setting('watchlist_config') or {}
            tg_channel_tracking = watchlist_config.get('tg_channel_tracking', False)

            def try_shared_pool_source():
                nonlocal success, action_type
                if item_type not in ['Movie', 'Series', 'Season', 'Episode']:
                    return None
                try:
                    shared_result = None
                    prepared_ctx = {
                        'item': item,
                        'tmdb_id': tmdb_id,
                        'item_type': item_type,
                        'title': title,
                        'season_number': season_number,
                        'parent_tmdb_id': parent_tmdb_id,
                        'year': item_year,
                    }
                    probe_key = _shared_subscription_probe_key(prepared_ctx)
                    probe_row = shared_probe_by_key.get(probe_key) if probe_key else None
                    if probe_row and try_consume_preprobed_shared_resource:
                        shared_result = try_consume_preprobed_shared_resource(
                            probe_row=probe_row,
                            item=item,
                            title=title,
                            tmdb_id=tmdb_id,
                            item_type=item_type,
                            parent_tmdb_id=parent_tmdb_id,
                            season_number=season_number,
                            year=item_year,
                            missing_episode_numbers=missing_episode_numbers,
                        )
                    elif try_consume_shared_resource:
                        shared_result = _try_consume_shared_resource_no_gap(
                            try_consume_shared_resource,
                            item=item,
                            title=title,
                            tmdb_id=tmdb_id,
                            item_type=item_type,
                            parent_tmdb_id=parent_tmdb_id,
                            season_number=season_number,
                            year=item_year,
                            missing_episode_numbers=missing_episode_numbers,
                        )
                    else:
                        shared_result = {}

                    if shared_result.get('success'):
                        success = True
                        action_type = shared_result.get('action_type') or '共享资源秒传'
                        logger.info(
                            f"  ➜ [共享秒传] 《{title}》命中中心 Rapid 资源池，"
                            f"处理方式: {shared_result.get('mode') or 'rapid'}, 数量: {shared_result.get('count', 0)}"
                        )
                    elif shared_result.get('enabled') and shared_result.get('skipped') and shared_result.get('reason') == 'gap_registration_removed':
                        logger.debug(
                            f"  ➜ [共享秒传] 《{title}》未执行缺口登记；"
                            "共享池候选由中心入池广播推送。"
                        )
                except Exception as e:
                    logger.error(f"  ➜ [共享资源] 处理《{title}》时异常，自动降级原有订阅链路: {e}", exc_info=True)
                return None

            # ==========================================
            # 动态订阅源处理 (云资源 / MP)
            # ==========================================
            raw_sources = strategy_config.get('subscription_sources')
            subscription_sources = list(raw_sources) if isinstance(raw_sources, list) else []

            if not is_wanted_subscription:
                # 只有 WANTED 能进入 MP；其他任何状态都只允许共享池/云资源补库，避免追更季/已订阅项重复投递 MP。
                if 'mp' in subscription_sources:
                    subscription_sources.remove('mp')
                    logger.debug(
                            f"  ➜ [MP保护] 跳过《{title}》的 MoviePilot 订阅："
                        )
                # 移除强制添加 hdhive 的逻辑，完全尊重前端用户的勾选；如果用户勾选了 hdhive 就走，没勾选就算追更也不走。

            for source_type in subscription_sources:
                if success:
                    break

                if source_type == 'shared_pool':
                    try_shared_pool_source()

                elif source_type == 'hdhive':
                    if item_type in ['Movie', 'Series', 'Season']:
                        hdhive_tmdb_id = tmdb_id
                        hdhive_media_type = 'movie'
                        hdhive_item_label = '电影'
                        hdhive_target_season = None
                        hdhive_require_complete = False

                        if item_type in ['Series', 'Season']:
                            hdhive_tmdb_id = parent_tmdb_id or tmdb_id
                            hdhive_media_type = 'tv'
                            hdhive_item_label = '剧集'

                            if item_type == 'Season' and season_number is not None:
                                hdhive_target_season = int(season_number)
                                logger.info(
                                    f"  ➜ [策略] 季《{title}》S{int(season_number):02d} 走云资源时请求不带季号，"
                                    f"仅使用父剧集 TMDb ID {hdhive_tmdb_id} 检索；返回后本地按季号排序。"
                                )

                            if is_wanted_subscription:
                                try:
                                    hdhive_require_complete = check_series_completion(
                                        int(hdhive_tmdb_id),
                                        tmdb_api_key,
                                        season_number=hdhive_target_season,
                                        series_name=title
                                    )
                                except Exception as e:
                                    hdhive_require_complete = False
                                    logger.warning(f"  ➜ [策略] 检查剧集《{title}》完结状态失败，影巢不强制完结包: {e}")
                            else:
                                hdhive_require_complete = False
                                logger.info(
                                    f"  ➜ [追更模式] 剧集《{title}》{f'第 {int(hdhive_target_season):02d} 季' if hdhive_target_season is not None else ''} "
                                    f"subscription_status={subscription_status or 'NONE'}，跳过完结状态检查。"
                                )

                            if hdhive_require_complete:
                                first_season_note = ""
                                try:
                                    if hdhive_target_season is not None and int(hdhive_target_season) == 1:
                                        first_season_note = "第一季完结包允许不写季号；"
                                except Exception:
                                    first_season_note = ""

                                logger.info(
                                    f"  ➜ [策略] 剧集《{title}》{f'第 {int(hdhive_target_season):02d} 季' if hdhive_target_season is not None else ''} 已判定完结，"
                                    f"影巢仅允许转存全集/全结/完结包，分段资源不转存；{first_season_note}明确错季仍排除。"
                                )
                            else:
                                logger.info(
                                    f"  ➜ [策略] 剧集《{title}》{f'S{int(hdhive_target_season):02d}' if hdhive_target_season is not None else ''} 未判定完结，"
                                    f"影巢不强制完结包，转存后由智能追剧处理追更。"
                                )

                        if hdhive_tmdb_id:
                            if _try_download_from_hdhive_first(
                                int(hdhive_tmdb_id),
                                hdhive_media_type,
                                title,
                                item_label=hdhive_item_label,
                                target_season=hdhive_target_season,
                                require_complete=hdhive_require_complete,
                            ):
                                success = True
                                action_type = "影巢"

                elif source_type == 'tg_channel':
                    if item_type in ['Movie', 'Series', 'Season']:
                        channel_tmdb_id = (parent_tmdb_id or tmdb_id) if item_type in ['Series', 'Season'] else tmdb_id
                        channel_media_type = 'tv' if item_type in ['Series', 'Season'] else 'movie'
                        channel_item_label = '剧集' if channel_media_type == 'tv' else '电影'
                        channel_target_season = int(season_number) if item_type == 'Season' and season_number is not None else None
                        channel_require_complete = False
                        if is_wanted_subscription and channel_media_type == 'tv':
                            try:
                                channel_require_complete = check_series_completion(
                                    int(channel_tmdb_id),
                                    tmdb_api_key,
                                    season_number=channel_target_season,
                                    series_name=title
                                )
                            except Exception as e:
                                logger.warning(f"  ➜ [策略] 检查剧集《{title}》完结状态失败，TG频道不强制完结包: {e}")
                        if _try_download_from_channel_first(
                            int(channel_tmdb_id),
                            channel_media_type,
                            title,
                            item_label=channel_item_label,
                            target_season=channel_target_season,
                            require_complete=channel_require_complete,
                            year=item_year
                        ):
                            success = True
                            action_type = "频道"

                elif source_type == 'mp':
                    # 双保险：即使上面的订阅源列表未来被改坏，这里也只允许 WANTED 进入 MP。
                    if not is_wanted_subscription:
                        logger.debug(
                            f"  ➜ [追更模式] 跳过《{title}》的 MoviePilot 订阅："
                        )
                        continue

                    if item_type == 'Movie':
                        logger.info(f"  ➜ 正在向 MoviePilot 提交电影《{title}》的订阅...")
                        mp_payload = {"name": title, "tmdbid": int(tmdb_id), "type": "电影"}
                        success = moviepilot.subscribe_with_custom_payload(mp_payload, config)
                    elif item_type == 'Series':
                        success = _subscribe_full_series_with_logic(int(tmdb_id), title, config, tmdb_api_key)
                    elif item_type == 'Episode':
                        logger.info(f"  ➜ [共享资源] 单集《{title}》未命中可消费资源，已跳过 MP 兜底。")
                    elif item_type == 'Season' and parent_tmdb_id and season_number is not None:
                        mp_payload = {"name": title, "tmdbid": int(parent_tmdb_id), "type": "电视剧", "season": int(season_number)}

                        is_pending, fake_eps = should_mark_as_pending(int(parent_tmdb_id), int(season_number), tmdb_api_key)
                        is_completed = False

                        if not is_pending:
                            is_completed = check_series_completion(
                                int(parent_tmdb_id),
                                tmdb_api_key,
                                season_number=int(season_number),
                                series_name=title
                            )
                            wash_mode = _apply_watchlist_mp_wash_flags(
                                mp_payload,
                                watchlist_config,
                                force_full=is_completed
                            )
                            if is_completed:
                                logger.info(f"  ➜ 《{title}》S{season_number} 已完结，强制使用 {wash_mode} 订阅。")
                            else:
                                logger.info(f"  ➜ 《{title}》S{season_number} 未完结，向 MoviePilot 提交 {wash_mode} 订阅。")
                        else:
                            logger.info(f"  ➜ 《{title}》S{season_number} 处于待定模式，向 MoviePilot 提交补缺订阅。")

                        if tg_channel_tracking and not is_completed:
                            logger.info(f"  ➜ [策略] TG频道追更已开启，跳过向 MoviePilot 提交未完结季 S{season_number} 的订阅。")
                            success = True 
                        else:
                            success = moviepilot.subscribe_with_custom_payload(mp_payload, config)
                            if success and is_pending:
                                moviepilot.update_subscription_status(int(parent_tmdb_id), int(season_number), 'P', config, total_episodes=fake_eps)

            # 处理订阅结果
            if success:
                if is_wanted_subscription:
                    logger.info(f"  ➜ 《{title}》订阅成功！")
                else:
                    logger.info(f"  ➜ 《{title}》补库处理成功！")

                # WANTED 成功后更新为 SUBSCRIBED；SUBSCRIBED 补库成功只保持原状态，不能重复消耗订阅配额。
                # Series 走 MP 整剧逻辑时仍由 _subscribe_full_series_with_logic 内部逐季处理；
                # Series 走云资源时没有逐季订阅流程，需要直接更新当前 Series，避免下次任务重复处理。
                if is_wanted_subscription and (item_type != 'Series' or action_type in ["影巢", "频道", "云资源"] or _is_shared_rapid_action(action_type)):
                    request_db.set_media_status_subscribed(
                        tmdb_ids=item['tmdb_id'],
                        item_type=item_type,
                    )

                # 只有真正从 WANTED 发起的新订阅才扣除配额；SUBSCRIBED 补库不扣。
                if is_wanted_subscription:
                    settings_db.decrement_subscription_quota()

                # 准备通知 (智能拼接通知标题)
                item_display_name = ""
                if item_type == 'Season':
                    season_num = item.get('season_number')
                    if season_num is not None:
                        item_display_name = f"剧集《{series_name} 第 {season_num} 季》"
                    else:
                        item_display_name = f"剧集《{series_name}》"
                elif item_type == 'Episode':
                    try:
                        item_display_name = f"剧集《{series_name or title} S{int(item.get('season_number') or 0):02d}E{int(item.get('episode_number') or 0):02d}》"
                    except Exception:
                        item_display_name = f"单集《{item['title']}》"
                else:
                    item_display_name = f"{item_type}《{item['title']}》"

                # 解析订阅来源，找出需要通知的用户
                sources = item.get('subscription_sources_json', [])
                source_display_parts = []
                for source in sources:
                    source_type = source.get('type')
                    if source_type == 'resubscribe':
                        rule_name = telegram.escape_markdown(source.get('rule_name', '未知规则'))
                        source_display_parts.append(f"`[自动洗版]` \\({rule_name}\\)")
                    elif source_type == 'user_request' and (user_id := source.get('user_id')):
                        if user_id not in notifications_to_send:
                            notifications_to_send[user_id] = []

                        # 为用户通知构建完整的标题
                        user_notify_title = item['title']
                        if item_type == 'Season':
                            season_num = item.get('season_number')
                            if season_num is not None:
                                user_notify_title = f"{series_name} 第 {season_num} 季"

                        notifications_to_send[user_id].append(user_notify_title)
                        user_name = telegram.escape_markdown(user_db.get_username_by_id(user_id) or user_id)
                        source_display_parts.append(f"`[用户请求]` \\({user_name}\\)")
                    elif source_type == 'actor_subscription':
                        actor_name = telegram.escape_markdown(source.get('name', '未知'))
                        source_display_parts.append(f"`[演员订阅]` \\({actor_name}\\)")
                    elif source_type in ['custom_collection', 'native_collection']:
                        coll_name = telegram.escape_markdown(source.get('name', '未知'))
                        source_display_parts.append(f"`[合集]` \\({coll_name}\\)")
                    elif source_type == 'telegram_search':
                        tg_name = telegram.escape_markdown(source.get('name', '未知'))
                        source_display_parts.append(f"`[TG搜索]` \\({tg_name}\\)")
                    elif source_type == 'watchlist':
                        source_display_parts.append("`[追剧补全]`")

                source_display = " ".join(set(source_display_parts)) or "`[未知来源]`"
                subscription_details.append({'source': source_display, 'item': item_display_name, 'action': action_type})

            else:
                if is_wanted_subscription:
                    logger.error(f"  ➜ 订阅《{title}》失败，请检查 MoviePilot 连接或日志。")
                else:
                    logger.info(f"  ➜ [追更模式] 《{title}》 本轮未补到资源。")

            # 如果配置了延时，且不是列表中的最后一个项目，则进行休眠
            if request_delay > 0 and i < len(wanted_items) - 1:
                logger.debug(f"  ➜ 根据配置暂停 {request_delay} 秒...")
                time.sleep(request_delay)

        # 发送用户通知
        logger.info(f"  ➜ 准备为 {len(notifications_to_send)} 位用户发送合并的成功通知...")
        for user_id, subscribed_items in notifications_to_send.items():
            try:
                user_chat_id = user_db.get_user_telegram_chat_id(user_id)
                if user_chat_id:
                    items_list_str = "\n".join([f"· `{telegram._markdown_code_text(item)}`" for item in subscribed_items])
                    message_text = (f"🎉 *您的 {len(subscribed_items)} 个订阅已成功处理*\n\n您之前想看的下列内容现已加入下载队列：\n{items_list_str}")
                    telegram.send_telegram_message(user_chat_id, message_text)
            except Exception as e:
                logger.error(f"  ➜ 为用户 {user_id} 发送自动订阅的合并通知时出错: {e}")

        # 失败的通知
        logger.info(f"  ➜ 准备为 {len(failed_notifications_to_send)} 位用户发送合并的失败通知...")
        for user_id, failed_items in failed_notifications_to_send.items():
            try:
                user_chat_id = user_db.get_user_telegram_chat_id(user_id)
                if user_chat_id:
                    items_list_str = "\n".join([f"· `{telegram._markdown_code_text(item)}`" for item in failed_items])
                    message_text = (f"➜ *您的部分订阅请求未被处理*\n\n下列内容因不满足条件而被跳过：\n{items_list_str}")
                    telegram.send_telegram_message(user_chat_id, message_text)
            except Exception as e:
                logger.error(f"为用户 {user_id} 发送自动订阅的合并失败通知时出错: {e}")

        if subscription_details:
            header = f"  ✅ *统一订阅任务完成，成功处理 {len(subscription_details)} 项:*"

            item_lines = []
            for detail in subscription_details:
                source = detail.get('source', '`[未知来源]`')
                item = telegram.escape_markdown(detail['item'])

                if _is_shared_rapid_action(detail.get('action')):
                    action_tag = _shared_rapid_action_tag(detail.get('action'))
                elif detail.get('action') == '影巢':
                    action_tag = "影巢转存"
                elif detail.get('action') == '频道':
                    action_tag = "频道转存"
                elif detail.get('action') == 'TG搜索':
                    action_tag = "TG搜索"
                else:
                    action_tag = "MP订阅"

                # 直接拼接 source，因为上面已经提前做好了 Markdown 格式化和转义
                item_lines.append(f"├─ `[{action_tag}]` {source} {item}")

            summary_message = header + "\n" + "\n".join(item_lines)
        else:
            summary_message = "ℹ️ *统一订阅任务完成，无成功处理的订阅项。*"

        if rejected_details:
            rejected_header = f"\n\n➜ *下列 {len(rejected_details)} 项因不满足订阅条件而被跳过:*"

            rejected_lines = []
            for detail in rejected_details:
                reason = telegram.escape_markdown(detail.get('reason', '未知原因'))
                item = telegram.escape_markdown(detail['item'])
                rejected_lines.append(f"├─ `{reason}` {item}")

            summary_message += rejected_header + "\n" + "\n".join(rejected_lines)

        if quota_exhausted:
            content = "(每日订阅配额已用尽，部分项目可能未处理)"
            escaped_content = telegram.escape_markdown(content)
            summary_message += f"\n\n*{escaped_content}*"

        # 打印日志和发送通知的逻辑保持不变
        logger.info(summary_message.replace('*', '').replace('`', ''))
        admin_chat_ids = user_db.get_admin_telegram_chat_ids()
        if admin_chat_ids:
            logger.info(f"  ➜ 准备向 {len(admin_chat_ids)} 位管理员发送任务总结...")
            for chat_id in admin_chat_ids:
                # 发送通知，静默模式，避免打扰
                telegram.send_telegram_message(chat_id, summary_message, disable_notification=True)

        task_manager.update_status_from_thread(100, "统一订阅任务处理完成。")
        logger.info(f"--- '{task_name}' 任务执行完毕 ---")

    except Exception as e:
        logger.error(f"  ➜ {task_name} 任务失败: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"错误: {e}")
