# tasks/helpers.py
# 跨模块共享的辅助函数

import os
import re
import json
from typing import Optional, Dict, Tuple, List, Set, Any
import logging
from datetime import datetime, timedelta, timezone

from handler.tmdb import get_movie_details, get_tv_details, get_tv_season_details, search_tv_shows, get_tv_season_details
from database import settings_db, connection, request_db
import utils

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

def normalize_full_width_chars(text: str) -> str:
    """将字符串中的全角字符（数字、字母、冒号）转换为半角。"""
    if not text:
        return ""
    # 全角空格
    text = text.replace('\u3000', ' ')
    # 全角数字、字母、冒号的转换表
    full_width = "０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ： "
    half_width = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz: "
    translation_table = str.maketrans(full_width, half_width)
    return text.translate(translation_table)

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
        "effect_display": effect_display_str, 
        "codec_display": codec_str,          
        "audio_display": audio_str,
        "subtitle_display": subtitle_str,
        "audio_languages_raw": list(detected_audio_langs),
        "subtitle_languages_raw": list(detected_sub_langs),
        "release_group_raw": release_group_list,
    }

def parse_full_asset_details(item_details: dict, id_to_parent_map: dict = None, library_guid: str = None) -> dict:
    """视频流分析主函数"""
    # 提取并计算时长 (分钟)
    runtime_ticks = item_details.get('RunTimeTicks')
    runtime_min = round(runtime_ticks / 600000000) if runtime_ticks else None

    item_id = str(item_details.get("Id"))
    ancestors = []
    if id_to_parent_map and item_id:
        ancestors = calculate_ancestor_ids(item_id, id_to_parent_map, library_guid)

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
            "ancestor_ids": ancestors,
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
        "ancestor_ids": ancestors,
        "runtime_minutes": runtime_min 
    }
    item_id = str(item_details.get("Id"))
    if id_to_parent_map and item_id:
        # 调用下面定义的 calculate_ancestor_ids
        asset["ancestor_ids"] = calculate_ancestor_ids(item_id, id_to_parent_map, library_guid)
    else:
        asset["ancestor_ids"] = []
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

    strategy = settings_db.get_setting('subscription_strategy_config') or {}
    # 优先使用数据库配置，没有则使用默认值
    delay_days = int(strategy.get('delay_subscription_days', 0))

    # 初始日志仍然使用ID，因为此时我们还没有片名
    logger.debug(f"  ➜ 检查电影 (ID: {movie_id}) 是否适合订阅 (延迟天数: {delay_days})...")

    details = get_movie_details(
        movie_id=movie_id,
        api_key=api_key,
        append_to_response="release_dates"
    )

    # ★★★ 获取片名用于后续日志，如果获取失败则回退到使用ID ★★★
    log_identifier = f"《{details.get('title')}》" if details and details.get('title') else f"(ID: {movie_id})"

    if not details:
        logger.warning(f"  ➜ 无法获取电影 {log_identifier} 的详情，默认其不适合订阅。")
        return False

    release_info = details.get("release_dates", {}).get("results", [])
    if not release_info:
        logger.warning(f"  ➜ 电影 {log_identifier} 未找到任何地区的发行日期信息，默认其不适合订阅。")
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
                    logger.warning(f"  ➜ 解析电影 {log_identifier} 的上映日期 '{release.get('release_date')}' 时出错。")
                    continue

    if earliest_theatrical_date:
        days_since_release = (today - earliest_theatrical_date).days
        if days_since_release >= delay_days:
            logger.info(f"  ➜ 成功: 电影 {log_identifier} 最早于 {days_since_release} 天前在影院上映，已超过配置的 {delay_days} 天，适合订阅。")
            return True
        else:
            logger.info(f"  ➜ 失败: 电影 {log_identifier} 最早于 {days_since_release} 天前在影院上映，未满配置的 {delay_days} 天，不适合订阅。")
            return False

    logger.warning(f"  ➜ 电影 {log_identifier} 未找到数字版或任何有效的影院上映日期，默认其不适合订阅。")
    return False

# +++ 剧集完结状态检查 (共享逻辑) +++
def check_series_completion(tmdb_id: int, api_key: str, season_number: Optional[int] = None, series_name: str = "未知剧集") -> bool:
    """
    检查剧集或特定季是否已完结。
    用于判断是否开启洗版模式 (best_version=1)。
    
    逻辑：
    1. 剧集状态为 Ended/Canceled -> 视为完结
    2. 最后一集播出日期已过 (<= Today) -> 视为完结
    3. 最后一集播出超过30天 (防止数据缺失) -> 视为完结
    4. 获取不到数据 -> 为了防止漏洗版，默认视为完结
    """
    if not api_key:
        return False

    today = datetime.now().date()
    
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

            # A. 检查最后一集播出时间 (无缓冲期，播出即完结)
            last_episode = episodes[-1]
            last_air_date_str = last_episode.get('air_date')

            if last_air_date_str:
                try:
                    last_air_date = datetime.strptime(last_air_date_str, '%Y-%m-%d').date()
                    
                    # ★★★ 修改：移除缓冲期，只要日期 <= 今天，即视为完结 ★★★
                    if last_air_date <= today:
                        logger.info(f"  ➜ 《{series_name}》第 {season_number} 季最后一集于 {last_air_date} 播出，判定已完结。")
                        return True
                    else:
                        logger.info(f"  ➜ 《{series_name}》第 {season_number} 季最后一集将于 {last_air_date} 播出，判定未完结。")
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
                    # 整剧同样移除缓冲期
                    if last_air_date <= today:
                        logger.info(f"  ➜ 剧集《{series_name}》的最新一集已播出 ({last_air_date})，判定为可洗版状态。")
                        return True
                        
    except Exception as e:
        logger.warning(f"  ➜ 检查《{series_name}》完结状态失败: {e}，为安全起见，默认判定为未完结。")
        return False
    
    return False

def parse_series_title_and_season(title: str, api_key: str = None) -> Tuple[Optional[str], Optional[int]]:
    """
    从一个可能包含季号的剧集标题中，解析出基础剧名和季号。
    
    【V2 - 严格校验版】
    针对 "唐朝诡事录之长安" 这种 "主标题之副标题" 格式：
    1. 尝试拆分。
    2. 必须通过 TMDb API 验证：主标题能搜到剧，且副标题能匹配到该剧的某一季。
    3. 验证失败则视为普通剧名，不进行截断。
    """
    if not title:
        return None, None
        
    normalized_title = normalize_full_width_chars(title)

    # --- 1. 优先处理 "主标题之副标题" 格式 (严格校验逻辑) ---
    # 仅当提供了 API Key 时才尝试这种高风险解析
    if '之' in normalized_title and api_key:
        parts = normalized_title.split('之', 1)
        if len(parts) == 2:
            parent_candidate = parts[0].strip()
            subtitle_candidate = parts[1].strip()
            
            # 只有当主标题长度大于1时才处理（避免误伤《云之羽》等）
            if len(parent_candidate) > 1 and subtitle_candidate:
                try:
                    # A. 搜索主标题 (例如 "唐朝诡事录")
                    search_results = search_tv_shows(parent_candidate, api_key)
                    
                    # 只有搜到了结果，才继续验证
                    if search_results:
                        # 假设第一个结果就是我们要找的剧
                        tv_id = search_results[0]['id']
                        # B. 获取该剧的所有季信息
                        tv_details = get_tv_details(tv_id, api_key, append_to_response="seasons")
                        
                        if tv_details and 'seasons' in tv_details:
                            for season in tv_details['seasons']:
                                season_name = season.get('name', '')
                                season_num = season.get('season_number')
                                
                                # C. 严格比对：副标题必须包含在季名中
                                # 例如：季名 "唐朝诡事录之西行"，副标题 "西行" -> 匹配成功
                                if season_num and season_num > 0:
                                    if subtitle_candidate in season_name:
                                        logger.info(f"  ➜ [智能解析] 成功将 '{title}' 解析为《{parent_candidate}》第 {season_num} 季 (匹配季名: {season_name})")
                                        return parent_candidate, season_num
                                        
                    # 如果代码走到这里，说明虽然有'之'，但没匹配到任何季信息
                    # 此时记录日志，并放弃拆分，防止将 "亦舞之城" 错误拆分为 "亦舞"
                    logger.debug(f"  ➜ [智能解析] '{title}' 包含'之'字，但未匹配到TMDb季信息，将作为完整剧名处理。")
                    
                except Exception as e:
                    logger.warning(f"  ➜ 解析 '之' 字标题时 TMDb 查询出错: {e}，将回退到普通模式。")

    # --- 2. 标准正则匹配 (原有逻辑) ---
    # 如果上面的逻辑没返回，说明它不是 "主标题之副标题" 格式，或者校验失败。
    # 此时 normalized_title 依然是完整的 "亦舞之城"，我们继续检查它是否包含 "S2", "第2季" 等标准标记。
    
    roman_map = {'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'V': 5, 'VI': 6, 'VII': 7, 'VIII': 8, 'IX': 9, 'X': 10}
    chinese_map = {'一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9, '十': 10}

    patterns = [
        # 模式1: 最优先匹配 "第X季" 或 "Season X"
        re.compile(r'^(.*?)\s*(?:第([一二三四五六七八九十\d]+)季|Season\s*(\d+))', re.IGNORECASE),
        
        # 模式2: 匹配年份 (如 "2024")
        re.compile(r'^(.*?)\s+((?:19|20)\d{2})$'),
        
        # 模式3: 中文数字(带前缀) 或 罗马/阿拉伯数字
        re.compile(r'^(.*?)\s*(?:[第部]\s*([一二三四五六七八九十])|([IVX\d]+))(?:[:\s-]|$)')
    ]

    for pattern in patterns:
        match = pattern.match(normalized_title)
        if not match: continue
        
        groups = [g for g in match.groups() if g is not None]
        if len(groups) < 2: continue
        
        base_name, season_str = groups[0].strip(), groups[1].strip()

        # 健壮性检查
        if (not base_name and len(normalized_title) < 8) or (len(base_name) <= 1 and season_str.isdigit()):
            continue

        season_num = 0
        if season_str.isdigit(): season_num = int(season_str)
        elif season_str.upper() in roman_map: season_num = roman_map[season_str.upper()]
        elif season_str in chinese_map: season_num = chinese_map[season_str]

        if season_num > 0:
            for suffix in ["系列", "合集"]:
                if base_name.endswith(suffix): base_name = base_name[:-len(suffix)]
            return base_name, season_num

    # --- 3. 最终返回 ---
    # 如果所有尝试都失败（既不是"之"字季播剧，也没有"S2"标记）
    # 返回 None, None。调用方会因此使用原始的完整标题进行搜索。
    # 对于 "亦舞之城"，这里返回 (None, None)，于是系统会搜索 "亦舞之城"，这是正确的。
    return None, None

def should_mark_as_pending(tmdb_id: int, season_number: int, api_key: str) -> tuple[bool, int]:
    """
    检查指定季是否满足“自动待定”条件。
    修复版：改用 get_tv_details 获取整剧信息中的 episode_count 字段，而非计算单季详情的列表长度。
    返回: (是否待定, 虚标总集数)
    """
    try:
        # 1. 读取配置
        watchlist_cfg = settings_db.get_setting('watchlist_config') or {}
        auto_pending_cfg = watchlist_cfg.get('auto_pending', {})
        
        if not auto_pending_cfg.get('enabled', False):
            return False, 0

        threshold_days = int(auto_pending_cfg.get('days', 30))
        threshold_episodes = int(auto_pending_cfg.get('episodes', 1))
        fake_total = int(auto_pending_cfg.get('default_total_episodes', 99))
        
        # 2. 获取 TMDb 整剧详情 (比获取单季详情更稳，因为包含明确的 episode_count 字段)
        show_details = get_tv_details(tmdb_id, api_key)
        if not show_details:
            return False, 0

        # 3. 在整剧详情的 seasons 列表中找到目标季
        target_season = None
        seasons = show_details.get('seasons', [])
        for season in seasons:
            if season.get('season_number') == season_number:
                target_season = season
                break
        
        if not target_season:
            # 如果没找到该季信息，无法判断，默认不待定
            return False, 0

        # 4. 获取核心数据
        air_date_str = target_season.get('air_date')
        # 直接读取官方提供的该季总集数，而不是计算列表长度
        episode_count = target_season.get('episode_count', 0)
        
        if air_date_str:
            try:
                air_date = datetime.strptime(air_date_str, '%Y-%m-%d').date()
                # 使用 UTC 时间避免时区导致的日期差异
                today = datetime.now(timezone.utc).date()
                days_diff = (today - air_date).days
                
                # 逻辑：上线时间在阈值内 (例如30天内) AND 集数很少 (例如只有1集)
                # 这种情况通常意味着是刚出的剧，或者数据还没更新全，或者是试播集
                if (0 <= days_diff <= threshold_days) and (episode_count <= threshold_episodes):
                    logger.info(f"  ➜ 触发自动待定: 第{season_number}季 上线{days_diff}天, TMDb记录集数{episode_count} (阈值: {threshold_episodes})")
                    return True, fake_total
            except ValueError:
                pass
                
        return False, 0

    except Exception as e:
        logger.warning(f"检查待定条件失败: {e}")
        return False, 0
    
def calculate_ancestor_ids(item_id: str, id_to_parent_map: dict, library_guid: str) -> List[str]:
    if not item_id or not id_to_parent_map:
        return []

    ancestors = set()
    curr_id = id_to_parent_map.get(item_id)
    
    while curr_id and curr_id != "1":
        ancestors.add(curr_id)
        # ★★★ 核心修改：增加严格的 None 字符串过滤 ★★★
        if library_guid and str(library_guid).lower() != "none":
            ancestors.add(f"{library_guid}_{curr_id}")
        
        curr_id = id_to_parent_map.get(curr_id)
    
    if library_guid and str(library_guid).lower() != "none":
        ancestors.add(library_guid)
        
    return [str(fid) for fid in ancestors if fid and str(fid).lower() != "none"]

# --- 通用订阅处理函数 ---
def process_subscription_items_and_update_db(
    tmdb_items: List[Dict[str, Any]], 
    tmdb_to_emby_item_map: Dict[str, Any], 
    subscription_source: Dict[str, Any], 
    tmdb_api_key: str
) -> Set[str]:
    """
    通用订阅处理器：接收一组 TMDb 条目，自动处理元数据、父剧集占位、在库检查，并更新 request_db。
    
    :param tmdb_items: 待处理列表，格式 [{'tmdb_id': '...', 'media_type': 'Movie'/'Series', 'season': 1, ...}]
    :param tmdb_to_emby_item_map: 全量本地媒体映射表 (用于判断是否在库)
    :param subscription_source: 订阅源对象 (用于写入数据库 source 字段)
    :param tmdb_api_key: TMDb API Key
    :return: processed_active_ids (Set[str]) - 本次处理中确认活跃的 ID 集合 (用于调用方做清理/Diff)
    """
    if not tmdb_items:
        return set()

    logger.info(f"  ➜ [通用订阅] 开始处理 {len(tmdb_items)} 个媒体条目...")

    # 1. 提前加载所有在库的“季”的信息 (用于精准判断季是否存在)
    in_library_seasons_set = set()
    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT parent_series_tmdb_id, season_number FROM media_metadata WHERE item_type = 'Season' AND in_library = TRUE")
            for row in cursor.fetchall():
                in_library_seasons_set.add((str(row['parent_series_tmdb_id']), row['season_number']))
    except Exception as e_db:
        logger.error(f"  -> [通用订阅] 获取在库季列表失败: {e_db}")

    # 2. 获取所有在库的 Key 集合 (Movie/Series)
    in_library_keys = set(tmdb_to_emby_item_map.keys())

    # 3. 获取已订阅/暂停的 Key 集合 (防止重复请求 API)
    subscribed_or_paused_keys = set()
    try:
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT tmdb_id, item_type FROM media_metadata WHERE subscription_status IN ('SUBSCRIBED', 'PAUSED', 'WANTED', 'IGNORED', 'PENDING_RELEASE')")
            for row in cursor.fetchall():
                subscribed_or_paused_keys.add(f"{row['tmdb_id']}_{row['item_type']}")
    except Exception as e_sub:
        logger.error(f"  -> [通用订阅] 获取订阅状态失败: {e_sub}")
    
    missing_released_items = []
    missing_unreleased_items = []
    parent_series_to_ensure_exist = {} 
    today_str = datetime.now().strftime('%Y-%m-%d')
    parent_series_cache = {} 

    # 用于记录本次真正处理过的 ID (返回给调用方用于清理)
    processed_active_ids = set()

    for item_def in tmdb_items:
        # 这里的 tmdb_id 必须保持为 剧集 ID (Series ID) 或 电影 ID
        tmdb_id = str(item_def.get('tmdb_id')) 
        if not tmdb_id or tmdb_id.lower() == 'none': continue

        media_type = item_def.get('media_type')
        season_num = item_def.get('season')

        # 将原始 ID (剧ID/影ID) 加入活跃列表
        processed_active_ids.add(tmdb_id)

        # --- A. 在库检查 ---
        is_in_library = False
        
        # 1. 显式 Emby ID
        if item_def.get('emby_id'):
            is_in_library = True
        # 2. 季的在库检查
        elif media_type == 'Series' and season_num is not None:
            if (tmdb_id, season_num) in in_library_seasons_set:
                is_in_library = True
        
        # 3. 通用 Key 检查
        if not is_in_library:
            current_key = f"{tmdb_id}_{media_type}"
            if current_key in in_library_keys:
                is_in_library = True
        
        if is_in_library: continue

        # --- B. 获取详情并构建请求 ---
        try:
            details = None
            item_type_for_db = media_type
            
            # 用于写入 media_metadata 的 ID (如果是季，这里会变成季ID)
            target_db_id = tmdb_id 
            
            # ★★★ 分支 1: 带季号的剧集 (视为季) ★★★
            if media_type == 'Series' and season_num is not None:
                parent_id = tmdb_id 
                item_type_for_db = 'Season'

                # 1. 获取/缓存父剧集信息
                if parent_id not in parent_series_cache:
                    p_details = get_tv_details(parent_id, tmdb_api_key)
                    if p_details:
                        parent_series_cache[parent_id] = p_details
                
                parent_details = parent_series_cache.get(parent_id)
                if not parent_details: continue

                # 2. 加入父剧集占位 (确保父剧集存在于 media_metadata，状态为 NONE)
                parent_series_to_ensure_exist[parent_id] = {
                    'tmdb_id': str(parent_id),
                    'item_type': 'Series',
                    'title': parent_details.get('name'),
                    'original_title': parent_details.get('original_name'),
                    'release_date': parent_details.get('first_air_date'),
                    'poster_path': parent_details.get('poster_path'),
                    'overview': parent_details.get('overview')
                }

                # 3. 获取季详情
                details = get_tv_season_details(parent_id, season_num, tmdb_api_key)
                if details:
                    details['parent_series_tmdb_id'] = str(parent_id)
                    details['parent_title'] = parent_details.get('name')
                    details['parent_poster_path'] = parent_details.get('poster_path')
                    
                    # 获取真实的季 ID
                    real_season_id = str(details.get('id'))
                    target_db_id = real_season_id
                    
                    # ★★★ 关键：将季 ID 也加入活跃列表，防止被误清理 ★★★
                    processed_active_ids.add(real_season_id)
                    
                    # 二次检查订阅状态 (检查季ID是否已订阅)
                    s_key = f"{real_season_id}_Season"
                    if s_key in subscribed_or_paused_keys: continue
            
            # 分支 2: 电影
            elif media_type == 'Movie':
                if f"{tmdb_id}_Movie" in subscribed_or_paused_keys: continue
                details = get_movie_details(tmdb_id, tmdb_api_key)
                if details:
                    target_db_id = str(details.get('id'))
                    processed_active_ids.add(target_db_id)

            if not details: continue
            
            # --- C. 构建数据库记录 (用于订阅) ---
            release_date = details.get("air_date") or details.get("release_date") or details.get("first_air_date", '')
            release_year = int(release_date.split('-')[0]) if (release_date and '-' in release_date) else None

            item_details_for_db = {
                'tmdb_id': target_db_id, # 这里存入的是 季ID 或 电影ID
                'item_type': item_type_for_db, # 这里是 'Season' 或 'Movie'
                'title': details.get('name') or details.get('title'),
                'release_date': release_date,
                'release_year': release_year, 
                'overview': details.get('overview'),
                'poster_path': details.get('poster_path') or details.get('parent_poster_path'),
                'parent_series_tmdb_id': details.get('parent_series_tmdb_id'),
                'season_number': details.get('season_number'),
                'source': subscription_source # 直接使用传入的 source
            }
            
            if item_type_for_db == 'Season':
                item_details_for_db['title'] = details.get('name') or f"第 {season_num} 季"

            # --- D. 分流 ---
            if release_date and release_date > today_str:
                missing_unreleased_items.append(item_details_for_db)
            else:
                missing_released_items.append(item_details_for_db)

        except Exception as e:
            logger.error(f"  -> [通用订阅] 处理条目 {tmdb_id} ({media_type}) 时出错: {e}")

    # 4. 执行数据库操作 (批量写入)
    if parent_series_to_ensure_exist:
        logger.info(f"  -> [通用订阅] 正在确保 {len(parent_series_to_ensure_exist)} 个父剧集元数据存在...")
        request_db.set_media_status_none(
            tmdb_ids=list(parent_series_to_ensure_exist.keys()),
            item_type='Series',
            media_info_list=list(parent_series_to_ensure_exist.values())
        )

    def group_and_update(items_list, status):
        if not items_list: return
        logger.info(f"  -> [通用订阅] 将 {len(items_list)} 个缺失媒体设为 '{status}'...")
        requests_by_type = {}
        for item in items_list:
            itype = item['item_type']
            if itype not in requests_by_type: requests_by_type[itype] = []
            requests_by_type[itype].append(item)
            
        for itype, requests in requests_by_type.items():
            ids = [req['tmdb_id'] for req in requests]
            if status == 'WANTED':
                request_db.set_media_status_wanted(ids, itype, media_info_list=requests, source=subscription_source)
            elif status == 'PENDING_RELEASE':
                request_db.set_media_status_pending_release(ids, itype, media_info_list=requests, source=subscription_source)

    group_and_update(missing_released_items, 'WANTED')
    group_and_update(missing_unreleased_items, 'PENDING_RELEASE')
    
    return processed_active_ids

def apply_rating_logic(metadata_skeleton: Dict[str, Any], tmdb_data: Dict[str, Any], item_type: str):
    """
    将 TMDb 的原始分级数据，经过配置的映射规则处理后，注入到元数据骨架中。
    """
    from database import settings_db
    
    final_rating_str = ""
    
    # 加载配置
    rating_mapping = settings_db.get_setting('rating_mapping') or utils.DEFAULT_RATING_MAPPING
    priority_list = settings_db.get_setting('rating_priority') or utils.DEFAULT_RATING_PRIORITY
    
    # 获取原产国
    origin_country = None
    if item_type == "Movie":
        _countries = tmdb_data.get('production_countries')
        origin_country = _countries[0].get('iso_3166_1') if _countries else None
    else:
        _countries = tmdb_data.get('origin_country', [])
        origin_country = _countries[0] if _countries else None

    # 准备数据源
    available_ratings = {}
    target_list_node = [] # 指向骨架中的列表节点
    
    if item_type == "Movie":
        # 电影数据源解析
        if 'release_dates' in tmdb_data:
            metadata_skeleton['release_dates'] = tmdb_data['release_dates']
            # 构建列表和字典
            countries_list = []
            for r in tmdb_data['release_dates'].get('results', []):
                country_code = r.get('iso_3166_1')
                cert = ""
                release_date = ""
                for rel in r.get('release_dates', []):
                    if rel.get('certification'):
                        cert = rel.get('certification')
                        release_date = rel.get('release_date')
                        break
                if cert:
                    available_ratings[country_code] = cert
                    countries_list.append({
                        "iso_3166_1": country_code,
                        "certification": cert,
                        "release_date": release_date,
                        "primary": (country_code == origin_country)
                    })
            metadata_skeleton['releases']['countries'] = countries_list
            target_list_node = metadata_skeleton['releases']['countries']
            
    elif item_type == "Series":
        # 剧集数据源解析
        if 'content_ratings' in tmdb_data:
            metadata_skeleton['content_ratings'] = tmdb_data['content_ratings']
            for r in tmdb_data['content_ratings'].get('results', []):
                available_ratings[r.get('iso_3166_1')] = r.get('rating')
            target_list_node = metadata_skeleton['content_ratings']['results']

    # --- 核心映射逻辑 ---
    target_us_code = None
    
    # 1. 成人强制修正
    if tmdb_data.get('adult') is True:
        logger.warning(f"  ⚠️ 发现成人内容，忽略任何国家分级强制设为 'XXX'.")
        target_us_code = 'XXX'
    # 2. 只有当不是成人内容时，才走常规映射逻辑
    elif 'US' in available_ratings:
        final_rating_str = available_ratings['US']
    else:
        # 3. 按优先级查找
        for p_country in priority_list:
            search_country = origin_country if p_country == 'ORIGIN' else p_country
            if not search_country: continue
            
            if search_country in available_ratings:
                source_rating = available_ratings[search_country]
                
                # 尝试映射
                if isinstance(rating_mapping, dict) and search_country in rating_mapping and 'US' in rating_mapping:
                    current_val = None
                    for rule in rating_mapping[search_country]:
                        if str(rule['code']).strip().upper() == str(source_rating).strip().upper():
                            current_val = rule.get('emby_value')
                            break
                    
                    if current_val is not None:
                        valid_us_rules = []
                        for rule in rating_mapping['US']:
                            r_code = rule.get('code', '')
                            if item_type == "Movie" and r_code.startswith('TV-'): continue
                            valid_us_rules.append(rule)
                        
                        # 精确匹配
                        for rule in valid_us_rules:
                            try:
                                if int(rule.get('emby_value')) == int(current_val):
                                    target_us_code = rule['code']
                                    break
                            except: pass
                        
                        # 向上兼容
                        if not target_us_code:
                            for rule in valid_us_rules:
                                try:
                                    if int(rule.get('emby_value')) == int(current_val) + 1:
                                        target_us_code = rule['code']
                                        break
                                except: pass
                
                if target_us_code:
                    logger.info(f"  ➜ [分级映射] 将 {search_country}:{source_rating} 映射为 US:{target_us_code}")
                    final_rating_str = target_us_code
                    break
                elif not final_rating_str:
                    final_rating_str = source_rating

    # 4. 补全 US 分级到列表
    if target_us_code:
        # 移除旧 US
        if item_type == "Movie":
            target_list_node[:] = [c for c in target_list_node if c.get('iso_3166_1') != 'US']
            target_list_node.append({
                "iso_3166_1": "US",
                "certification": target_us_code,
                "release_date": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z'),
                "primary": False
            })
        else:
            target_list_node[:] = [r for r in target_list_node if r.get('iso_3166_1') != 'US']
            target_list_node.append({
                "iso_3166_1": "US",
                "rating": target_us_code
            })

    # 5. 写入根节点兜底
    if final_rating_str:
        metadata_skeleton['mpaa'] = final_rating_str
        metadata_skeleton['certification'] = final_rating_str

def construct_metadata_payload(item_type: str, tmdb_data: Dict[str, Any], 
                                  aggregated_tmdb_data: Optional[Dict[str, Any]] = None,
                                  emby_data_fallback: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        统一封装：将 TMDb 原始数据转换为符合本地 override 格式的标准元数据骨架。
        包含：基础字段映射、复杂字段处理(Genres/Keywords/Videos)、分级逻辑、剧集子项结构化等。
        """
        # 1. 初始化骨架
        if item_type == "Movie":
            payload = json.loads(json.dumps(utils.MOVIE_SKELETON_TEMPLATE))
        else:
            payload = json.loads(json.dumps(utils.SERIES_SKELETON_TEMPLATE))

        if not tmdb_data:
            return payload

        # 2. 基础字段直接覆盖 (排除特殊字段)
        exclude_keys = [
            'casts', 'releases', 'release_dates', 'keywords', 'trailers', 
            'content_ratings', 'videos', 'credits', 'genres', 
            'episodes_details', 'seasons_details', 'created_by', 'networks',
            'production_companies'
        ]
        for key in payload.keys():
            if key in tmdb_data and key not in exclude_keys:
                payload[key] = tmdb_data[key]

        # 3. 通用复杂字段处理
        # Genres: 优先 TMDb，Emby 兜底
        if 'genres' in tmdb_data and tmdb_data['genres']:
            payload['genres'] = tmdb_data['genres']
        elif emby_data_fallback and emby_data_fallback.get('Genres'):
            payload['genres'] = [{'id': 0, 'name': g} for g in emby_data_fallback['Genres']]

        # Keywords
        if 'keywords' in tmdb_data:
            kw_data = tmdb_data['keywords']
            if item_type == "Movie":
                if isinstance(kw_data, dict): payload['keywords']['keywords'] = kw_data.get('keywords', [])
                elif isinstance(kw_data, list): payload['keywords']['keywords'] = kw_data
            else:
                if isinstance(kw_data, dict): payload['keywords']['results'] = kw_data.get('results', [])
                elif isinstance(kw_data, list): payload['keywords']['results'] = kw_data

        # Videos / Trailers
        if 'videos' in tmdb_data:
            if item_type == "Movie":
                youtube_list = []
                for v in tmdb_data['videos'].get('results', []):
                    if v.get('site') == 'YouTube' and v.get('type') == 'Trailer':
                        youtube_list.append({
                            "name": v.get('name'), "size": str(v.get('size', 'HD')), 
                            "source": v.get('key'), "type": "Trailer"
                        })
                payload['trailers']['youtube'] = youtube_list
            else:
                payload['videos'] = tmdb_data['videos']

        # 4. 类型特定处理
        if item_type == "Movie":
            # 演员表
            credits_source = tmdb_data.get('credits') or tmdb_data.get('casts') or {}
            if credits_source:
                payload['casts']['cast'] = credits_source.get('cast', [])
                payload['casts']['crew'] = credits_source.get('crew', [])
            
            # 分级
            apply_rating_logic(payload, tmdb_data, "Movie")

        elif item_type == "Series":
            # 演员表
            credits_source = tmdb_data.get('aggregate_credits') or tmdb_data.get('credits') or {}
            if credits_source:
                payload['credits']['cast'] = credits_source.get('cast', [])
                payload['credits']['crew'] = credits_source.get('crew', [])
            
            if 'created_by' in tmdb_data: payload['created_by'] = tmdb_data['created_by']
            if 'networks' in tmdb_data: payload['networks'] = tmdb_data['networks']
            
            # 外部ID
            if 'external_ids' in tmdb_data:
                ext_ids = tmdb_data['external_ids']
                if 'imdb_id' in ext_ids: payload['external_ids']['imdb_id'] = ext_ids['imdb_id']
                if 'tvdb_id' in ext_ids: payload['external_ids']['tvdb_id'] = ext_ids['tvdb_id']
                if 'tvrage_id' in ext_ids: payload['external_ids']['tvrage_id'] = ext_ids['tvrage_id']

            # 分级
            apply_rating_logic(payload, tmdb_data, "Series")

            # 挂载子项数据 (Seasons / Episodes)
            if aggregated_tmdb_data:
                payload['seasons_details'] = aggregated_tmdb_data.get('seasons_details', [])
                
                raw_episodes = aggregated_tmdb_data.get('episodes_details', {})
                formatted_episodes = {}
                
                # 统一处理分集骨架
                for key, ep_data in raw_episodes.items():
                    ep_skeleton = json.loads(json.dumps(utils.EPISODE_SKELETON_TEMPLATE))
                    
                    # 关键字段
                    ep_skeleton['id'] = ep_data.get('id') 
                    ep_skeleton['season_number'] = ep_data.get('season_number')
                    ep_skeleton['episode_number'] = ep_data.get('episode_number')
                    ep_skeleton['name'] = ep_data.get('name')
                    ep_skeleton['overview'] = ep_data.get('overview')
                    ep_skeleton['air_date'] = ep_data.get('air_date')
                    ep_skeleton['vote_average'] = ep_data.get('vote_average')
                    
                    # 演员
                    ep_credits = ep_data.get('credits', {})
                    ep_skeleton['credits']['cast'] = ep_credits.get('cast', []) 
                    ep_skeleton['credits']['guest_stars'] = ep_credits.get('guest_stars', [])
                    ep_skeleton['credits']['crew'] = ep_credits.get('crew', [])
                    
                    formatted_episodes[key] = ep_skeleton
                
                payload['episodes_details'] = formatted_episodes

        return payload

def reconstruct_metadata_from_db(db_row: Dict[str, Any], actors_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    【新增】将数据库记录还原为符合本地 override 格式的标准元数据骨架。
    用于：当数据库有记录但本地文件丢失时，从数据库生成文件。
    :param db_row: media_metadata 表的一行记录 (字典)
    :param actors_list: 关联的完整演员列表 (包含 name, profile_path 等)
    """
    item_type = db_row.get('item_type')
    
    # 1. 初始化骨架
    if item_type == "Movie":
        payload = json.loads(json.dumps(utils.MOVIE_SKELETON_TEMPLATE))
    else:
        payload = json.loads(json.dumps(utils.SERIES_SKELETON_TEMPLATE))

    # 2. 基础字段映射
    payload['id'] = int(db_row.get('tmdb_id') or 0)
    payload['overview'] = db_row.get('overview')
    
    # 标题
    if item_type == "Movie":
        payload['title'] = db_row.get('title')
        payload['original_title'] = db_row.get('original_title')
        # 转换日期格式 datetime -> str
        r_date = db_row.get('release_date')
        payload['release_date'] = str(r_date) if r_date else ''
        payload['runtime'] = db_row.get('runtime_minutes')
    else:
        payload['name'] = db_row.get('title')
        payload['original_name'] = db_row.get('original_title')
        r_date = db_row.get('release_date')
        payload['first_air_date'] = str(r_date) if r_date else ''

    payload['vote_average'] = db_row.get('rating')
    payload['poster_path'] = db_row.get('poster_path')
    
    # 3. 复杂 JSON 字段还原
    # Genres
    if db_row.get('genres_json'):
        try:
            raw_genres = db_row['genres_json']
            # 兼容 list 和 str
            genres_list = json.loads(raw_genres) if isinstance(raw_genres, str) else raw_genres
            if genres_list:
                # 数据库存的是 ["Action", "Drama"]，JSON需要 [{"id":0, "name":"Action"}]
                payload['genres'] = [{"id": 0, "name": g} for g in genres_list]
        except Exception as e:
            logger.warning(f"还原 Genres 失败: {e}")

    # Studios (production_companies)
    if db_row.get('studios_json'):
        try:
            raw_studios = db_row['studios_json']
            studios_list = json.loads(raw_studios) if isinstance(raw_studios, str) else raw_studios
            if studios_list:
                # 数据库存的是 [{"id":1, "name":"HBO"}]，格式基本一致
                payload['production_companies'] = studios_list
                if item_type == 'Series':
                    payload['networks'] = studios_list 
        except Exception as e:
            logger.warning(f"还原 Studios 失败: {e}")
        
    # Keywords (Tags)
    if db_row.get('keywords_json'):
        try:
            raw_kw = db_row['keywords_json']
            kw_list = json.loads(raw_kw) if isinstance(raw_kw, str) else raw_kw
            if kw_list:
                # 数据库存的是 [{"id":1, "name":"keyword"}]
                if item_type == "Movie":
                    payload['keywords']['keywords'] = kw_list
                else:
                    payload['keywords']['results'] = kw_list
        except Exception as e:
            logger.warning(f"还原 Keywords 失败: {e}")

    # 4. 演员表 (Cast)
    if actors_list:
        formatted_cast = []
        for i, actor in enumerate(actors_list):
            # 确保 name 存在，如果数据库里 name 是空的，尝试用 original_name
            final_name = actor.get('name') or actor.get('original_name')
            
            formatted_cast.append({
                "id": actor.get('tmdb_id'),
                "name": final_name,
                "original_name": actor.get('original_name'),
                "character": actor.get('character') or actor.get('role'),
                "profile_path": actor.get('profile_path'),
                "order": actor.get('order', i),
                "known_for_department": "Acting"
            })
        
        if item_type == "Movie":
            payload['casts']['cast'] = formatted_cast
        else:
            payload['credits']['cast'] = formatted_cast

    # 5. 分级 (Official Rating)
    if db_row.get('official_rating_json'):
        try:
            raw_rating = db_row['official_rating_json']
            ratings_map = json.loads(raw_rating) if isinstance(raw_rating, str) else raw_rating
            
            # 优先取 US，没有则取第一个
            rating_val = ratings_map.get('US')
            if not rating_val and ratings_map:
                rating_val = list(ratings_map.values())[0]
            
            if rating_val:
                payload['mpaa'] = rating_val
                payload['certification'] = rating_val
                
                # 简单构建一个 releases 结构以防万一
                if item_type == "Movie":
                    payload['releases']['countries'] = [{
                        "iso_3166_1": "US", "certification": rating_val, "release_date": "", "primary": True
                    }]
                else:
                    payload['content_ratings']['results'] = [{
                        "iso_3166_1": "US", "rating": rating_val
                    }]
        except Exception as e:
            logger.warning(f"还原 Rating 失败: {e}")

    return payload