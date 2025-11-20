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

RELEASE_GROUPS: Dict[str, List[str]] = {
    "0ff": ['FF(?:(?:A|WE)B|CD|E(?:DU|B)|TV)'],
    "1pt": [],
    "52pt": [],
    "观众": ['Audies', 'AD(?:Audio|E(?:book|)|Music|Web)'],
    "azusa": [],
    "备胎": ['BeiTai'],
    "学校": ['Bts(?:CHOOL|HD|PAD|TV)', 'Zone'],
    "carpt": ['CarPT'],
    "彩虹岛": ['CHD(?:Bits|PAD|(?:|HK)TV|WEB|)', 'StBOX', 'OneHD', 'Lee', 'xiaopie'],
    "碟粉": ['discfan'],
    "dragonhd": [],
    "eastgame": ['(?:(?:iNT|(?:HALFC|Mini(?:S|H|FH)D))-|)TLF'],
    "filelist": [],
    "gainbound": ['(?:DG|GBWE)B'],
    "hares": ['Hares(?:(?:M|T)V|Web|)'],
    "hd4fans": [],
    "高清视界": ['HDA(?:pad|rea|TV)', 'EPiC'],
    "阿童木": ['hdatmos'],
    "hdbd": [],
    "hdchina": ['HDC(?:hina|TV|)', 'k9611', 'tudou', 'iHD'],
    "杜比": ['D(?:ream|BTV)', '(?:HD|QHstudI)o'],
    "红豆饭": ['beAst(?:TV|)'],
    "家园": ['HDH(?:ome|Pad|TV|WEB|)'],
    "hdpt": ['HDPT(?:Web|)'],
    "天空": ['HDS(?:ky|TV|Pad|WEB|)', 'AQLJ'],
    "高清时间": ['hdtime'],
    "HDU": [],
    "hdvideo": [],
    "hdzone": ['HDZ(?:one|)'],
    "憨憨": ['HHWEB'],
    "hitpt": [],
    "htpt": ['HTPT'],
    "iptorrents": [],
    "joyhd": [],
    "朋友": ['FRDS', 'Yumi', 'cXcY'],
    "柠檬": ['L(?:eague(?:(?:C|H)D|(?:M|T)V|NF|WEB)|HD)', 'i18n', 'CiNT'],
    "馒头": ['MTeam(?:TV|)', 'MPAD', 'MWeb'],
    "nanyangpt": [],
    "老师": ['nicept'],
    "oshen": [],
    "我堡": ['Our(?:Bits|TV)', 'FLTTH', 'Ao', 'PbK', 'MGs', 'iLove(?:HD|TV)'],
    "猪猪": ['PiGo(?:NF|(?:H|WE)B)'],
    "ptchina": [],
    "猫站": ['PTer(?:DIY|Game|(?:M|T)V|WEB|)'],
    "pthome": ['PTH(?:Audio|eBook|music|ome|tv|WEB|)'],
    "ptmsg": [],
    "烧包": ['PTsbao', 'OPS', 'F(?:Fans(?:AIeNcE|BD|D(?:VD|IY)|TV|WEB)|HDMv)', 'SGXT'],
    "pttime": [],
    "putao": ['PuTao'],
    "聆音": ['lingyin'],
    "春天": [r"CMCT(?:A|V)?", "Oldboys", "GTR", "CLV", "CatEDU", "Telesto", "iFree"],
    "鲨鱼": ['Shark(?:WEB|DIY|TV|MV|)'],
    "tccf": [],
    "北洋园": ['TJUPT'],
    "totheglory": ['TTG', 'WiKi', 'NGB', 'DoA', '(?:ARi|ExRE)N'],
    "U2": [],
    "ultrahd": [],
    "others": ['B(?:MDru|eyondHD|TN)', 'C(?:fandora|trlhd|MRG)', 'DON', 'EVO', 'FLUX', 'HONE(?:yG|)',
               'N(?:oGroup|T(?:b|G))', 'PandaMoon', 'SMURF', 'T(?:EPES|aengoo|rollHD )'],
    "anime": ['ANi', 'HYSUB', 'KTXP', 'LoliHouse', 'MCE', 'Nekomoe kissaten', 'SweetSub', 'MingY',
              '(?:Lilith|NC)-Raws', '织梦字幕组', '枫叶字幕组', '猎户手抄部', '喵萌奶茶屋', '漫猫字幕社',
              '霜庭云花Sub', '北宇治字幕组', '氢气烤肉架', '云歌字幕组', '萌樱字幕组', '极影字幕社',
              '悠哈璃羽字幕社',
              '❀拨雪寻春❀', '沸羊羊(?:制作|字幕组)', '(?:桜|樱)都字幕组'],
    "forge": ['FROG(?:E|Web|)'],
    "ubits": ['UB(?:its|WEB|TV)'],
}

def _extract_exclusion_keywords_from_filename(filename: str) -> List[str]:
    """
    基于 RELEASE_GROUPS 字典中的别名匹配文件名，找到发布组名（中文），大小写保留。
    """
    if not filename:
        return []
    name_part = os.path.splitext(filename)[0]
    filename_upper = name_part.upper()

    for group_name, alias_list in RELEASE_GROUPS.items():
        for alias in alias_list:
            # 去除常见正则符号，方便转大写进行简单包含匹配
            alias_plain = re.sub(r'[\?\:\(\)\[\]\{\}\|\*\+\^\\\$\.]', '', alias).upper()
            if alias_plain and alias_plain in filename_upper:
                return [group_name]
        # 额外判断组名本身（如含英文）是否出现在文件名中
        group_name_upper = group_name.upper()
        if group_name_upper in filename_upper:
            return [group_name]

    return []

def _get_standardized_effect(path_lower: str, video_stream: Optional[Dict]) -> List[str]:
    """
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
    """视频流分析引擎"""
    if not item_details:
        return {}

    media_streams = item_details.get('MediaStreams', [])
    file_path = item_details.get('Path', '')
    file_name = os.path.basename(file_path) if file_path else ""
    file_name_lower = file_name.lower()

    video_stream = next((s for s in media_streams if s.get('Type') == 'Video'), None)
    resolution_str = "Unknown"
    if video_stream and video_stream.get("Width"):
        _, resolution_str = _get_resolution_tier(video_stream["Width"], video_stream.get("Height", 0))
    if resolution_str == "Unknown":
        if "2160p" in file_name_lower or "4k" in file_name_lower:
            resolution_str = "2160p"
        elif "1080p" in file_name_lower:
            resolution_str = "1080p"
        elif "720p" in file_name_lower:
            resolution_str = "720p"

    quality_str = _extract_quality_tag_from_filename(file_name_lower)
    effect_list = _get_standardized_effect(file_name_lower, video_stream)

    detected_audio_langs = _get_detected_languages_from_streams(media_streams, 'Audio')
    AUDIO_DISPLAY_MAP = {'chi': '国语', 'yue': '粤语', 'eng': '英语', 'jpn': '日语'}
    audio_str = ', '.join(sorted([AUDIO_DISPLAY_MAP.get(lang, lang) for lang in detected_audio_langs])) or '无'

    detected_sub_langs = _get_detected_languages_from_streams(media_streams, 'Subtitle')
    if 'chi' not in detected_sub_langs and 'yue' not in detected_sub_langs and any(
        s.get('IsExternal') for s in media_streams if s.get('Type') == 'Subtitle'):
        detected_sub_langs.add('chi')
    SUB_DISPLAY_MAP = {'chi': '中字', 'yue': '粤字', 'eng': '英文', 'jpn': '日文'}
    subtitle_str = ', '.join(sorted([SUB_DISPLAY_MAP.get(lang, lang) for lang in detected_sub_langs])) or '无'

    # 保持发布组大小写
    release_group_list = _extract_exclusion_keywords_from_filename(file_name)

    return {
        "resolution_display": resolution_str,
        "quality_display": quality_str,
        "effect_display": effect_list,
        "audio_display": audio_str,
        "subtitle_display": subtitle_str,
        "audio_languages_raw": list(detected_audio_langs),
        "subtitle_languages_raw": list(detected_sub_langs),
        "release_group_raw": release_group_list,
    }

def parse_full_asset_details(item_details: dict) -> dict:
    """视频流分析主函数"""
    if not item_details or "MediaStreams" not in item_details:
        return {
            "emby_item_id": item_details.get("Id"), "path": item_details.get("Path", ""),
            "size_bytes": None, "container": None, "video_codec": None,
            "audio_tracks": [], "subtitles": [],
            "resolution_display": "Unknown", "quality_display": "Unknown",
            "effect_display": ["SDR"], "audio_display": "无", "subtitle_display": "无",
            "audio_languages_raw": [], "subtitle_languages_raw": [],
            "release_group_raw": [], # 在空结构中也包含新字段
        }

    asset = {
        "emby_item_id": item_details.get("Id"), "path": item_details.get("Path", ""),
        "size_bytes": item_details.get("Size"), "container": item_details.get("Container"),
        "video_codec": None, "audio_tracks": [], "subtitles": []
    }
    media_streams = item_details.get("MediaStreams", [])
    for stream in media_streams:
        stream_type = stream.get("Type")
        if stream_type == "Video":
            asset["video_codec"] = stream.get("Codec")
            # 顺便把宽高也存进去
            asset["width"] = stream.get("Width")
            asset["height"] = stream.get("Height")
        elif stream_type == "Audio":
            asset["audio_tracks"].append({"language": stream.get("Language"), "codec": stream.get("Codec"), "channels": stream.get("Channels"), "display_title": stream.get("DisplayTitle")})
        elif stream_type == "Subtitle":
            asset["subtitles"].append({"language": stream.get("Language"), "display_title": stream.get("DisplayTitle")})

    display_tags = analyze_media_asset(item_details)
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