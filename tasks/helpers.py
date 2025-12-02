# tasks/helpers.py
# 跨模块共享的辅助函数

import os
import re
from typing import Optional, Dict, Any, List
import logging
from datetime import datetime, timedelta

from handler.tmdb import get_movie_details, get_tv_details, get_tv_season_details
import constants

logger = logging.getLogger(__name__)

AUDIO_SUBTITLE_KEYWORD_MAP = {
    "chi": ["Mandarin", "CHI", "ZHO", "国语", "国配", "国英双语", "公映", "台配", "京译", "上译", "央译"],
    "yue": ["Cantonese", "YUE", "粤语"],
    "eng": ["English", "ENG", "英语"],
    "jpn": ["Japanese", "JPN", "日语"],
    "kor": ["Korean", "KOR", "韩语"],
    "sub_chi": ["CHS", "SC", "GB", "简体", "简中", "简", "中字"], 
    "sub_yue": ["CHT", "TC", "BIG5", "繁體", "繁体", "繁"], 
    "sub_eng": ["ENG", "英字"],
    "sub_jpn": ["JPN", "日字", "日文"],
    "sub_kor": ["KOR", "韩字", "韩文"],
}

AUDIO_DISPLAY_MAP = {'chi': '国语', 'yue': '粤语', 'eng': '英语', 'jpn': '日语', 'kor': '韩语'}
SUB_DISPLAY_MAP = {'chi': '简体', 'yue': '繁体', 'eng': '英文', 'jpn': '日文', 'kor': '韩文'}

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
    "红豆饭": ['beAst(?:TV|)', 'HDFans'],
    "家园": ['HDH(?:ome|Pad|TV|WEB|)'],
    "hdpt": ['HDPT(?:Web|)'],
    "天空": ['HDS(?:ky|TV|Pad|WEB|)', 'AQLJ'],
    "高清时间": ['hdtime'],
    "HDU": [],
    "hdvideo": [],
    "hdzone": ['HDZ(?:one|)'],
    "憨憨": ['HHWEB'],
    "末日": ['AGSV(PT|WEB|MUS)'],
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
    "铂金学院": ['ptchina'],
    "猫站": ['PTer(?:DIY|Game|(?:M|T)V|WEB|)'],
    "pthome": ['PTH(?:Audio|eBook|music|ome|tv|WEB|)'],
    "ptmsg": [],
    "烧包": ['PTsbao', 'OPS', 'F(?:Fans(?:AIeNcE|BD|D(?:VD|IY)|TV|WEB)|HDMv)', 'SGXT'],
    "pttime": [],
    "葡萄": ['PuTao'],
    "聆音": ['lingyin'],
    "春天": [r"CMCT(?:A|V)?", "Oldboys", "GTR", "CLV", "CatEDU", "Telesto", "iFree"],
    "鲨鱼": ['Shark(?:WEB|DIY|TV|MV|)'],
    "他吹吹风": ['tccf'],
    "北洋园": ['TJUPT'],
    "听听歌": ['TTG', 'WiKi', 'NGB', 'DoA', '(?:ARi|ExRE)N'],
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
    【V2 - 正则修复版】
    基于 RELEASE_GROUPS 字典中的别名匹配文件名，找到发布组名（中文）。
    此版本能正确处理正则表达式别名。
    """
    if not filename:
        return []
    # 我们需要原始大小写的文件名（不含扩展名）来进行正则匹配
    name_part = os.path.splitext(filename)[0]

    for group_name, alias_list in RELEASE_GROUPS.items():
        for alias in alias_list:
            try:
                # 核心修复：使用 re.search 来正确评估正则表达式
                # re.IGNORECASE 可以在匹配时忽略大小写
                if re.search(alias, name_part, re.IGNORECASE):
                    return [group_name]
            except re.error as e:
                # 如果正则表达式本身有语法错误，记录日志并跳过
                logger.warning(f"RELEASE_GROUPS 中存在无效的正则表达式: '{alias}' for group '{group_name}'. Error: {e}")
                continue
        
        # 保留对组名本身的检查（例如 "MTeam"）
        if group_name.upper() in name_part.upper():
            return [group_name]

    return []

def get_keywords_by_group_name(group_name: str) -> List[str]:
    """
    根据发布组的中文名（或其他键名），反查其在 RELEASE_GROUPS 中对应的所有关键词/别名。
    
    :param group_name: 发布组的键名，例如 "朋友"
    :return: 对应的关键词列表，例如 ['FRDS', 'Yumi', 'cXcY']。如果找不到则返回空列表。
    """
    if not group_name:
        return []
    # 使用 .get() 方法安全地获取值，如果找不到键，则返回一个空列表
    return RELEASE_GROUPS.get(group_name, [])

def build_exclusion_regex_from_groups(group_names: List[str]) -> str:
    """
    接收一个发布组名称的列表，查询它们所有的关键词，并构建一个单一的、
    用于排除的 OR 正则表达式。
    
    :param group_names: 发布组名称列表，例如 ["朋友", "春天"]
    :return: 一个正则表达式字符串，例如 "(?:FRDS|Yumi|cXcY|CMCT(?:A|V)?|Oldboys|...)"
             如果列表为空或未找到任何关键词，则返回空字符串。
    """
    if not group_names:
        return ""

    all_keywords = []
    # 遍历传入的每一个组名
    for group_name in group_names:
        # 调用我们之前的反查函数，获取该组的所有关键词
        keywords = get_keywords_by_group_name(group_name)
        if keywords:
            all_keywords.extend(keywords)

    if not all_keywords:
        return ""

    # 使用 | (OR) 将所有关键词连接起来，并用一个非捕获组 (?:...) 包裹
    # 这意味着“只要标题中包含任意一个关键词，就匹配成功”
    return f"(?:{'|'.join(all_keywords)})"

def _get_standardized_effect(path_lower: str, video_stream: Optional[Dict]) -> str:
    """
    【V9 - 全局·智能文件名识别增强版】
    - 这是一个全局函数，可被项目中所有需要特效识别的地方共享调用。
    - 增强了文件名识别逻辑：当文件名同时包含 "dovi" 和 "hdr" 时，智能判断为 davi_p8。
    - 调整了判断顺序，确保更精确的规则优先执行。
    """
    
    # 1. 优先从文件名判断 (逻辑增强)
    if ("dovi" in path_lower or "dolbyvision" in path_lower or "dv" in path_lower) and "hdr" in path_lower:
        return "dovi_p8"
    if any(s in path_lower for s in ["dovi p7", "dovi.p7", "dv.p7", "profile 7", "profile7"]):
        return "dovi_p7"
    if any(s in path_lower for s in ["dovi p5", "dovi.p5", "dv.p5", "profile 5", "profile5"]):
        return "dovi_p5"
    if ("dovi" in path_lower or "dolbyvision" in path_lower) and "hdr" in path_lower:
        return "dovi_p8"
    if "dovi" in path_lower or "dolbyvision" in path_lower:
        return "dovi_other"
    if "hdr10+" in path_lower or "hdr10plus" in path_lower:
        return "hdr10+"
    if "hdr" in path_lower:
        return "hdr"

    # 2. 如果文件名没有信息，再对视频流进行精确分析
    if video_stream and isinstance(video_stream, dict):
        all_stream_info = []
        for key, value in video_stream.items():
            all_stream_info.append(str(key).lower())
            if isinstance(value, str):
                all_stream_info.append(value.lower())
        combined_info = " ".join(all_stream_info)

        if "doviprofile81" in combined_info: return "DoVi_P8"
        if "doviprofile76" in combined_info: return "DoVi_P7"
        if "doviprofile5" in combined_info: return "DoVi_P5"
        if any(s in combined_info for s in ["dvhe.08", "dvh1.08"]): return "DoVi_P8"
        if any(s in combined_info for s in ["dvhe.07", "dvh1.07"]): return "DoVi_P7"
        if any(s in combined_info for s in ["dvhe.05", "dvh1.05"]): return "DoVi_P5"
        if "dovi" in combined_info or "dolby" in combined_info or "dolbyvision" in combined_info: return "DoVi"
        if "hdr10+" in combined_info or "hdr10plus" in combined_info: return "HDR10+"
        if "hdr" in combined_info: return "HDR"

    # 3. 默认是SDR
    return "SDR"

def _extract_quality_tag_from_filename(filename_lower: str) -> str:
    """
    从文件名中提取质量标签，如果找不到，则返回 '未知'。
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
            
    return "未知"

def _get_resolution_tier(width: int, height: int) -> tuple[int, str]:
    if width >= 3800: return 4, "4k"
    if width >= 1900: return 3, "1080p"
    if width >= 1200: return 2, "720p"
    if width >= 700: return 1, "480p"  # 常见480p宽度为720或854
    return 0, "未知"

def _get_detected_languages_from_streams(
    media_streams: List[dict], 
    stream_type: str
) -> set:
    detected_langs = set()
    standard_codes = {
        'chi': {'chi', 'zho', 'chs', 'zh-cn', 'zh-hans', 'zh-sg', 'cmn'}, 
        'yue': {'yue', 'cht'}, 
        'eng': {'eng'},
        'jpn': {'jpn'},
        'kor': {'kor'},
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
    resolution_str = "未知"
    if video_stream and video_stream.get("Width"):
        _, resolution_str = _get_resolution_tier(video_stream["Width"], video_stream.get("Height", 0))
    if resolution_str == "未知":
        if "2160p" in file_name_lower or "4K" in file_name_lower:
            resolution_str = "4k"
        elif "1080p" in file_name_lower:
            resolution_str = "1080p"
        elif "720p" in file_name_lower:
            resolution_str = "720p"
        elif "480p" in file_name_lower: 
            resolution_str = "480p"

    quality_str = _extract_quality_tag_from_filename(file_name_lower)
    
    # 1. 获取权威的、细分的特效标签 (例如 'dovi_p8')
    effect_tag = _get_standardized_effect(file_name_lower, video_stream)
    
    # 2. 将其转换为您期望的、标准化的显示格式
    EFFECT_DISPLAY_MAP = {
        "dovi_p8": "DoVi_P8", "dovi_p7": "DoVi_P7", "dovi_p5": "DoVi_P5",
        "dovi_other": "DoVi", "hdr10+": "HDR10+", "hdr": "HDR", "sdr": "SDR"
    }
    effect_display_str = EFFECT_DISPLAY_MAP.get(effect_tag, effect_tag) # 如果没匹配到，显示原始tag

    # 3. 获取原始编码，并将其转换为标准显示格式
    codec_str = '未知'
    CODEC_DISPLAY_MAP = {
        'hevc': 'HEVC', 'h265': 'HEVC', 'x265': 'HEVC',
        'h264': 'H.264', 'avc': 'H.264', 'x264': 'H.264',
        'vp9': 'VP9', 'av1': 'AV1'
    }
    
    # 1. 优先从流获取
    if video_stream and video_stream.get('Codec'):
        raw_codec = video_stream.get('Codec').lower()
        codec_str = CODEC_DISPLAY_MAP.get(raw_codec, raw_codec.upper())
    # 2. 流获取失败，从文件名猜测
    else:
        for key, val in CODEC_DISPLAY_MAP.items():
            # 简单的包含判断，比如 "x265"
            if key in file_name_lower:
                codec_str = val
                break

    detected_audio_langs = _get_detected_languages_from_streams(media_streams, 'Audio')
    audio_str = ', '.join(sorted([AUDIO_DISPLAY_MAP.get(lang, lang) for lang in detected_audio_langs]))
    
    # ★★★ 核心修改：增强音频 (Audio) 的文件名兜底 ★★★
    # 如果 Emby 没分析出音轨，尝试从文件名提取常见音频格式作为展示
    if not audio_str:
        audio_keywords = {
            'truehd': 'TrueHD', 'atmos': 'Atmos', 
            'dts-hd': 'DTS-HD', 'dts': 'DTS', 
            'ac3': 'AC3', 'eac3': 'EAC3', 'dd+': 'Dolby Digital+',
            'aac': 'AAC', 'flac': 'FLAC'
        }
        found_audios = []
        for k, v in audio_keywords.items():
            if k in file_name_lower:
                found_audios.append(v)
        if found_audios:
            audio_str = " | ".join(found_audios) # 用竖线分隔，表示这是文件名猜的
        else:
            audio_str = '无' # 真的猜不到了

    detected_audio_langs = _get_detected_languages_from_streams(media_streams, 'Audio')
    audio_str = ', '.join(sorted([AUDIO_DISPLAY_MAP.get(lang, lang) for lang in detected_audio_langs])) or '无'

    detected_sub_langs = _get_detected_languages_from_streams(media_streams, 'Subtitle')
    if 'chi' not in detected_sub_langs and 'yue' not in detected_sub_langs and any(
        s.get('IsExternal') for s in media_streams if s.get('Type') == 'Subtitle'):
        detected_sub_langs.add('chi')
    subtitle_str = ', '.join(sorted([SUB_DISPLAY_MAP.get(lang, lang) for lang in detected_sub_langs])) or '无'

    release_group_list = _extract_exclusion_keywords_from_filename(file_name)

    return {
        "resolution_display": resolution_str,
        "quality_display": quality_str,
        "effect_display": effect_display_str, # ★★★ 核心修复 2/2: 使用新的标准大写字符串
        "codec_display": codec_str,          # ★★★ 使用新的标准大写字符串
        "audio_display": audio_str,
        "subtitle_display": subtitle_str,
        "audio_languages_raw": list(detected_audio_langs),
        "subtitle_languages_raw": list(detected_sub_langs),
        "release_group_raw": release_group_list,
    }

def parse_full_asset_details(item_details: dict) -> dict:
    """视频流分析主函数"""
    # 提取并计算时长 (分钟)
    runtime_ticks = item_details.get('RunTimeTicks')
    runtime_min = round(runtime_ticks / 600000000) if runtime_ticks else None

    if not item_details or "MediaStreams" not in item_details:
        return {
            "emby_item_id": item_details.get("Id"), "path": item_details.get("Path", ""),
            "size_bytes": None, "container": None, "video_codec": None,
            "audio_tracks": [], "subtitles": [],
            "resolution_display": "未知", "quality_display": "未知",
            "effect_display": ["SDR"], "audio_display": "无", "subtitle_display": "无",
            "audio_languages_raw": [], "subtitle_languages_raw": [],
            "release_group_raw": [],
            "runtime_minutes": runtime_min,
        }

    date_added_to_library = item_details.get("DateCreated")

    asset = {
        "emby_item_id": item_details.get("Id"), 
        "path": item_details.get("Path", ""),
        "size_bytes": item_details.get("Size"), 
        "container": item_details.get("Container"),
        "video_codec": None, 
        "video_bitrate_mbps": None, # 视频码率 (Mbps)
        "bit_depth": None,          # 色深 (8/10/12)
        "frame_rate": None,         # 帧率
        "audio_tracks": [], 
        "subtitles": [],
        "date_added_to_library": date_added_to_library,
        "runtime_minutes": runtime_min 
    }
    media_streams = item_details.get("MediaStreams", [])
    for stream in media_streams:
        stream_type = stream.get("Type")
        if stream_type == "Video":
            asset["video_codec"] = stream.get("Codec")
            asset["width"] = stream.get("Width")
            asset["height"] = stream.get("Height")
            if stream.get("BitRate"):
                asset["video_bitrate_mbps"] = round(stream.get("BitRate") / 1000000, 1)
            asset["bit_depth"] = stream.get("BitDepth")
            asset["frame_rate"] = stream.get("AverageFrameRate") or stream.get("RealFrameRate")
        elif stream_type == "Audio":
            asset["audio_tracks"].append({
                "language": stream.get("Language"), 
                "codec": stream.get("Codec"), 
                "channels": stream.get("Channels"), 
                "display_title": stream.get("DisplayTitle"),
                "is_default": stream.get("IsDefault")
            })
        elif stream_type == "Subtitle":
            asset["subtitles"].append({
                "language": stream.get("Language"), 
                "display_title": stream.get("DisplayTitle"),
                "is_forced": stream.get("IsForced"),  
                "format": stream.get("Codec") 
            })
            

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
            release_type = release.get("type")
            if release_type in [4, 5]:
                logger.info(f"  ➜ 成功: 电影 {log_identifier} 已有数字版/光盘发行记录 (Type {release_type})，适合订阅。")
                return True
            if release_type in [1, 2, 3]:
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

# +++ 剧集完结状态检查 (共享逻辑) +++
def check_series_completion(tmdb_id: int, api_key: str, season_number: Optional[int] = None, series_name: str = "未知剧集") -> bool:
    """
    检查剧集或特定季是否已完结。
    用于判断是否开启洗版模式 (best_version=1)。
    
    逻辑：
    1. 剧集状态为 Ended/Canceled -> 视为完结
    2. 最后一集播出日期已过 -> 视为完结
    3. 最后一集播出超过30天 (防止数据缺失) -> 视为完结
    4. 获取不到数据 -> 为了防止漏洗版，默认视为完结
    """
    if not api_key:
        return False

    today = datetime.now().date()
    
    # ★★★ 定义缓冲天数 ★★★
    BUFFER_DAYS = 7 
    
    try:
        # 1. 优先检查剧集整体状态
        show_details = get_tv_details(tmdb_id, api_key)

        if show_details:
            status = show_details.get('status', '')
            # 只有明确标记为 Ended 或 Canceled 才直接算完结
            if status in ['Ended', 'Canceled']:
                logger.info(f"  ➜ 剧集《{series_name}》TMDb状态为 '{status}'，判定第 {season_number if season_number else 'All'} 季已完结。")
                return True

        # 2. 如果是查询特定季
        if season_number is not None:
            season_details = get_tv_season_details(tmdb_id, season_number, api_key)
            
            if not season_details:
                logger.warning(f"  ➜ 无法获取《{series_name}》第 {season_number} 季详情，为安全起见，判定为未完结 (不洗版)。")
                return False
            
            episodes = season_details.get('episodes')
            if not episodes:
                logger.warning(f"  ➜ 《{series_name}》第 {season_number} 季暂无集数信息，判定为未完结 (不洗版)。")
                return False

            # A. 检查最后一集播出时间 (增加缓冲期)
            last_episode = episodes[-1]
            last_air_date_str = last_episode.get('air_date')

            if last_air_date_str:
                try:
                    last_air_date = datetime.strptime(last_air_date_str, '%Y-%m-%d').date()
                    
                    # ★★★ 核心修改：加上缓冲期判断 ★★★
                    # 只有当 (最后一集日期 + 7天) 仍然早于或等于今天，才算完结。
                    # 例子：11月27日首播，today是27日。 27 <= 27-7 (20日) -> False。判定为未完结。正确！
                    if last_air_date <= today - timedelta(days=BUFFER_DAYS):
                        logger.info(f"  ➜ 《{series_name}》第 {season_number} 季最后一集于 {last_air_date} 播出 (已过缓冲期)，判定已完结。")
                        return True
                    else:
                        # 即使播出了，如果没过缓冲期，也视为未完结，方便追更或等待Pack
                        status_desc = "已播出但未过缓冲期" if last_air_date <= today else "尚未播出"
                        logger.info(f"  ➜ 《{series_name}》第 {season_number} 季最后一集日期为 {last_air_date} ({status_desc})，判定未完结。")
                        return False
                except ValueError:
                    pass

            # B. 30天规则 (倒序检查) - 针对数据缺失严重的“僵尸剧”
            # 如果最后一集没有日期，或者上面的判断没通过，我们再看看是不是所有有日期的集都播完很久了
            for ep in reversed(episodes):
                air_date_str = ep.get('air_date')
                if air_date_str:
                    try:
                        air_date = datetime.strptime(air_date_str, '%Y-%m-%d').date()
                        # 只要有一集是未来播出的，那肯定没完结
                        if air_date > today: return False 
                        
                        # 如果最近的一集都播出超过30天了，那大概率是完结了（或者断更了）
                        if (today - air_date).days > 30:
                            logger.info(f"  ➜ 《{series_name}》第 {season_number} 季最近一集播出 ({air_date}) 已超30天，判定已完结。")
                            return True
                        # 如果最近一集在30天内，说明可能还在更，或者刚更完，走普通订阅更稳妥
                        else:
                            return False
                    except ValueError:
                        continue
            
            return False 

        else:
            # 3. 查询整剧 (Series类型)
            if show_details and (last_episode_to_air := show_details.get('last_episode_to_air')):
                last_air_date_str = last_episode_to_air.get('air_date')
                if last_air_date_str:
                    last_air_date = datetime.strptime(last_air_date_str, '%Y-%m-%d').date()
                    # 整剧同样增加缓冲期
                    if last_air_date <= today - timedelta(days=BUFFER_DAYS):
                        logger.info(f"  ➜ 剧集《{series_name}》的最新一集已播出并过缓冲期，判定为可洗版状态。")
                        return True
                        
    except Exception as e:
        logger.warning(f"  ➜ 检查《{series_name}》完结状态失败: {e}，为安全起见，默认判定为未完结。")
        return False
    
    return False