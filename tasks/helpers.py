# tasks/helpers.py
# 跨模块共享的辅助函数

import os
import re
from typing import Optional, Dict, Any, List
import logging
from datetime import datetime, timedelta

from handler.tmdb import get_movie_details
import constants

logger = logging.getLogger(__name__)

AUDIO_SUBTITLE_KEYWORD_MAP = {
    "chi": ["Mandarin", "CHI", "ZHO", "国语", "国配", "国英双语", "公映", "台配", "京译", "上译", "央译"],
    "yue": ["Cantonese", "YUE", "粤语"],
    "eng": ["English", "ENG", "英语"],
    "jpn": ["Japanese", "JPN", "日语"],
    "sub_chi": ["CHS", "CHT", "中字", "简中", "繁中", "简", "繁"],
    "sub_eng": ["ENG", "英字"],
}

def _get_standardized_effect(path_lower: str, video_stream: Optional[Dict]) -> List[str]:
    """
    【V10 - 最终权威版】
    - 优先、贪婪地从文件名中解析所有特效，因为文件名信息最全。
    - 如果文件名没有信息，再从 API 的视频流中补充。
    - 返回一个特效列表，例如 ["Dolby Vision", "HDR"]。
    """
    effects = set()

    # 1. 文件名优先，贪婪模式
    if "dovi" in path_lower or "dolbyvision" in path_lower or "dv" in path_lower:
        effects.add("Dolby Vision")
    if "hdr10+" in path_lower or "hdr10plus" in path_lower:
        effects.add("HDR10+")
    # 确保文件名里有hdr，但又不是hdr10+
    if "hdr" in path_lower and "hdr10+" not in path_lower and "hdr10plus" not in path_lower:
        effects.add("HDR")

    # 2. 如果文件名没信息，再从 API 补充
    if not effects and video_stream:
        if video_stream.get("BitDepth") == 10:
            effects.add("HDR")
        if (video_range := video_stream.get("VideoRange")):
            if "hdr" in video_range.lower() or "pq" in video_range.lower():
                effects.add("HDR")

    # 3. 如果最终什么都没有，才默认为 SDR
    if not effects:
        effects.add("SDR")
        
    return sorted(list(effects))

def _extract_quality_tag_from_filename(filename_lower: str) -> str:
    """
    【V2 - 修正版】
    从文件名中提取质量标签，如果找不到，则返回 'Unknown'。
    """
    QUALITY_HIERARCHY = [
        ('remux', 'Remux'),
        ('bluray', 'BluRay'),
        ('blu-ray', 'BluRay'),
        ('web-dl', 'WEB-DL'),
        ('webdl', 'WEB-DL'),
        ('webrip', 'WEBrip'),
        ('hdtv', 'HDTV'),
        ('dvdrip', 'DVDrip')
    ]
    
    for tag, display in QUALITY_HIERARCHY:
        # 使用更宽松的匹配，避免因为点、空格等问题匹配失败
        if tag in filename_lower:
            return display
            
    return "Unknown"

def _get_resolution_tier(width: int, height: int) -> tuple[int, str]:
    if width >= 3800 or height >= 2100: return 4, "2160p"
    if width >= 1900 or height >= 1000: return 3, "1080p"
    if width >= 1200 or height >= 700: return 2, "720p"
    if height > 0: return 1, f"{height}p"
    return 0, "Unknown"

def _get_detected_languages_from_streams(
    media_streams: List[dict], 
    stream_type: str
) -> set:
    detected_langs = set()
    standard_codes = {
        'chi': {'chi', 'zho', 'chs', 'cht', 'zh-cn', 'zh-hans', 'zh-sg', 'cmn', 'yue'},
        'eng': {'eng'},
        'jpn': {'jpn'}
    }
    
    for stream in media_streams:
        if stream.get('Type') == stream_type:
            # 检查 Language 字段
            if lang_code := str(stream.get('Language', '')).lower():
                for key, codes in standard_codes.items():
                    if lang_code in codes:
                        detected_langs.add(key)
            
            # 检查标题字段
            title_string = (stream.get('Title', '') + stream.get('DisplayTitle', '')).lower()
            if not title_string: continue
            for lang_key, keywords in AUDIO_SUBTITLE_KEYWORD_MAP.items():
                normalized_lang_key = lang_key.replace('sub_', '')
                if any(keyword.lower() in title_string for keyword in keywords):
                    detected_langs.add(normalized_lang_key)
    return detected_langs

def analyze_media_asset(item_details: dict) -> dict:
    """
    【权威分析引擎 V4 - 最终正确版】
    正确调用所有辅助函数，生成最权威的分析结果。
    """
    if not item_details: return {}

    media_streams = item_details.get('MediaStreams', [])
    file_path = item_details.get('Path', '')
    file_name_lower = os.path.basename(file_path).lower() if file_path else ""
    video_stream = next((s for s in media_streams if s.get('Type') == 'Video'), None)

    # --- 1. 分辨率 (API优先, 文件名保底) ---
    resolution_str = "Unknown"
    if video_stream and video_stream.get("Width"):
        _, resolution_str = _get_resolution_tier(video_stream["Width"], video_stream.get("Height", 0))
    if resolution_str == "Unknown":
        if "2160p" in file_name_lower or "4k" in file_name_lower: resolution_str = "2160p"
        elif "1080p" in file_name_lower: resolution_str = "1080p"
        elif "1080i" in file_name_lower: resolution_str = "1080i"
        elif "720p" in file_name_lower: resolution_str = "720p"

    # --- 2. 质量 ---
    quality_str = _extract_quality_tag_from_filename(file_name_lower)

    # --- 3. 特效 ---
    effect_list = _get_standardized_effect(file_name_lower, video_stream)

    # --- 4. 音轨 ---
    detected_audio_langs = _get_detected_languages_from_streams(media_streams, 'Audio')
    AUDIO_DISPLAY_MAP = {'chi': '国语', 'yue': '粤语', 'eng': '英语', 'jpn': '日语'}
    audio_str = ', '.join(sorted([AUDIO_DISPLAY_MAP.get(lang, lang) for lang in detected_audio_langs])) or '无'

    # --- 5. 字幕 ---
    detected_sub_langs = _get_detected_languages_from_streams(media_streams, 'Subtitle')
    if 'chi' not in detected_sub_langs and 'yue' not in detected_sub_langs and any(s.get('IsExternal') for s in media_streams if s.get('Type') == 'Subtitle'):
        detected_sub_langs.add('chi')
    SUB_DISPLAY_MAP = {'chi': '中字', 'yue': '粤字', 'eng': '英文', 'jpn': '日文'}
    subtitle_str = ', '.join(sorted([SUB_DISPLAY_MAP.get(lang, lang) for lang in detected_sub_langs])) or '无'

    return {
        "resolution_display": resolution_str,
        "quality_display": quality_str,
        "effect_display": effect_list, # 返回列表
        "audio_display": audio_str,
        "subtitle_display": subtitle_str,
        "audio_languages_raw": list(detected_audio_langs),
        "subtitle_languages_raw": list(detected_sub_langs),
    }

def parse_full_asset_details(item_details: dict) -> dict:
    """
    【V2 - 权威资产解析器】
    这是跨模块共享的唯一入口函数，负责将一个媒体项的详情解析成
    一个包含“原始流数据”和“前端展示标签”的完整资产字典。
    """
    # 1. 安全检查：如果输入无效，返回一个标准的空结构
    if not item_details or "MediaStreams" not in item_details:
        logger.warning(f"  ➜ 无法为媒体项 (ID: {item_details.get('Id')}) 找到可供分析的媒体流信息。")
        return {
            "emby_item_id": item_details.get("Id"), "path": item_details.get("Path", ""),
            "size_bytes": None, "container": None, "video_codec": None,
            "audio_tracks": [], "subtitles": [],
            "resolution_display": "Unknown", "quality_display": "Unknown",
            "effect_display": ["SDR"], "audio_display": "无", "subtitle_display": "无",
            "audio_languages_raw": [], "subtitle_languages_raw": []
        }

    # 2. 提取原始数据 (复现 _analyze_media_item_streams 的第一部分)
    asset = {
        "emby_item_id": item_details.get("Id"),
        "path": item_details.get("Path", ""),
        "size_bytes": item_details.get("Size"),
        "container": item_details.get("Container"),
        "video_codec": None,
        "audio_tracks": [],
        "subtitles": []
    }
    media_streams = item_details.get("MediaStreams", [])
    for stream in media_streams:
        stream_type = stream.get("Type")
        if stream_type == "Video":
            asset["video_codec"] = stream.get("Codec")
        elif stream_type == "Audio":
            asset["audio_tracks"].append({
                "language": stream.get("Language"), "codec": stream.get("Codec"),
                "channels": stream.get("Channels"), "display_title": stream.get("DisplayTitle")
            })
        elif stream_type == "Subtitle":
            asset["subtitles"].append({
                "language": stream.get("Language"), "display_title": stream.get("DisplayTitle")
            })

    # 3. 调用现有的分析引擎生成展示标签
    display_tags = analyze_media_asset(item_details)
    
    # 4. 将两部分结果合并成最终的完整字典
    asset.update(display_tags)
    
    return asset

# +++ 判断电影是否满足订阅条件 +++
def is_movie_subscribable(movie_id: int, api_key: str, config: dict) -> bool:
    """
    检查一部电影是否适合订阅。
    """
    if not api_key:
        logger.error("TMDb API Key 未提供，无法检查电影是否可订阅。")
        return False

    delay_days = config.get(constants.CONFIG_OPTION_MOVIE_SUBSCRIPTION_DELAY_DAYS, 30)

    # 初始日志仍然使用ID，因为此时我们还没有片名
    logger.debug(f"检查电影 (ID: {movie_id}) 是否适合订阅 (延迟天数: {delay_days})...")

    details = get_movie_details(
        movie_id=movie_id,
        api_key=api_key,
        append_to_response="release_dates"
    )

    # ★★★ 获取片名用于后续日志，如果获取失败则回退到使用ID ★★★
    log_identifier = f"《{details.get('title')}》" if details and details.get('title') else f"(ID: {movie_id})"

    if not details:
        logger.warning(f"无法获取电影 {log_identifier} 的详情，默认其不适合订阅。")
        return False

    release_info = details.get("release_dates", {}).get("results", [])
    if not release_info:
        logger.warning(f"电影 {log_identifier} 未找到任何地区的发行日期信息，默认其不适合订阅。")
        return False

    earliest_theatrical_date = None
    today = datetime.now().date()

    for country_releases in release_info:
        for release in country_releases.get("release_dates", []):
            if release.get("type") == 4:
                logger.info(f"  ➜ 成功: 电影 {log_identifier} 已有数字版发行记录，适合订阅。")
                return True
            if release.get("type") == 3:
                try:
                    release_date_str = release.get("release_date", "").split("T")[0]
                    if release_date_str:
                        current_release_date = datetime.strptime(release_date_str, "%Y-%m-%d").date()
                        if earliest_theatrical_date is None or current_release_date < earliest_theatrical_date:
                            earliest_theatrical_date = current_release_date
                except (ValueError, TypeError):
                    logger.warning(f"解析电影 {log_identifier} 的上映日期 '{release.get('release_date')}' 时出错。")
                    continue

    if earliest_theatrical_date:
        days_since_release = (today - earliest_theatrical_date).days
        if days_since_release >= delay_days:
            logger.info(f"  ➜ 成功: 电影 {log_identifier} 最早于 {days_since_release} 天前在影院上映，已超过配置的 {delay_days} 天，适合订阅。")
            return True
        else:
            logger.info(f"  ➜ 失败: 电影 {log_identifier} 最早于 {days_since_release} 天前在影院上映，未满配置的 {delay_days} 天，不适合订阅。")
            return False

    logger.warning(f"电影 {log_identifier} 未找到数字版或任何有效的影院上映日期，默认其不适合订阅。")
    return False