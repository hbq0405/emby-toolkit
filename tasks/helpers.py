# tasks/helpers.py
# 跨模块共享的辅助函数

import os
import re
import json
from typing import Optional, Dict, Tuple, List, Set, Any
import logging
from datetime import datetime, timedelta, timezone

from handler.tmdb import get_movie_details, get_tv_details, get_tv_season_details, search_tv_shows, get_tv_season_details
from database import settings_db, connection, request_db, media_db
from ai_translator import AITranslator
import utils
import constants

logger = logging.getLogger(__name__)

AUDIO_SUBTITLE_KEYWORD_MAP = {
    # 音轨：chi=国语, yue=粤语
    "chi": ["Mandarin", "CHI", "ZHO", "国语", "國語", "普通话", "国配", "國配", "国英双语", "公映", "台配", "京译", "上译", "央译", "guoyu", "guo"],
    "yue": ["Cantonese", "YUE", "粤语", "粵語", "粤配", "粵配", "粤英双语", "港配", "粤语配音", "广东话", "廣東話", "yueyu", "yue"],
    "eng": ["English", "ENG", "英语"],
    "jpn": ["Japanese", "JPN", "日语"],
    "kor": ["Korean", "KOR", "韩语"],

    # 字幕：chi=简体, yue=繁体
    "sub_chi": ["CHS", "SC", "GB", "简体", "簡體", "简中", "簡中", "Simplified"],
    "sub_yue": ["CHT", "TC", "BIG5", "繁體", "繁体", "Traditional"],
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
    "观众": ['Audies', r'\bAD(?:Audio|E(?:book|)|Music|Web)\b'],
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
    "我堡": ['Our(?:Bits|TV)', 'FLTTH', 'PbK', 'MGs', 'iLove(?:HD|TV)'],
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
    "anime": [r'\bANi\b', r'\bHYSUB\b', r'\bKTXP\b', 'LoliHouse', r'\bMCE\b', 'Nekomoe kissaten', 'SweetSub', 'MingY',
              '(?:Lilith|NC|AI)-Raws', '织梦字幕组', '枫叶字幕组', '猎户手抄部', '喵萌奶茶屋', '漫猫字幕社',
              '霜庭云花Sub', '北宇治字幕组', '氢气烤肉架', '云歌字幕组', '萌樱字幕组', '极影字幕社',
              '悠哈璃羽字幕社',
              '❀拨雪寻春❀', '沸羊羊(?:制作|字幕组)', '(?:桜|樱)都字幕组'],
    "青蛙": ['FROG(?:E|Web|)'],
    "ubits": ['UB(?:its|WEB|TV)'],
    "影巢": ['HiveWeb'],
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
    统一返回规则值：
    dovi_p8 / dovi_p7 / dovi_p5 / dovi_other / hdr10+ / hdr / sdr

    只认原始视频流，不做资产显示值兼容。
    """
    path_lower = str(path_lower or "").lower()

    if video_stream and isinstance(video_stream, dict):
        ext_subtype = str(video_stream.get("ExtendedVideoSubType") or "").lower()
        ext_subtype_desc = str(video_stream.get("ExtendedVideoSubTypeDescription") or "").lower()
        ext_type = str(video_stream.get("ExtendedVideoType") or "").lower()
        video_range = str(video_stream.get("VideoRange") or "").lower()
        display_title = str(video_stream.get("DisplayTitle") or "").lower()
        profile = str(video_stream.get("Profile") or "").lower()
        codec = str(video_stream.get("Codec") or "").lower()
        color_transfer = str(video_stream.get("ColorTransfer") or "").lower()

        # 1. 最高优先级：直接识别 ExtendedVideoSubType
        if ext_subtype in ["doviprofile81", "doviprofile8", "dvhe.08", "dvh1.08"]:
            return "dovi_p8"
        if ext_subtype in ["doviprofile76", "doviprofile7", "dvhe.07", "dvh1.07"]:
            return "dovi_p7"
        if ext_subtype in ["doviprofile5", "dvhe.05", "dvh1.05"]:
            return "dovi_p5"

        combined_info = " ".join([
            ext_subtype,
            ext_subtype_desc,
            ext_type,
            video_range,
            display_title,
            profile,
            codec,
            color_transfer,
        ])

        # 2. 描述字段补充判断
        if "profile 8.1" in combined_info or "hdr10 compatible" in combined_info:
            return "dovi_p8"
        if "profile 7" in combined_info:
            return "dovi_p7"
        if "profile 5" in combined_info:
            return "dovi_p5"

        has_dv = any(x in combined_info for x in ["dovi", "dolbyvision", "dolby vision"])
        has_hdr10_plus = any(x in combined_info for x in ["hdr10+", "hdr10plus"])
        has_hdr = has_hdr10_plus or any(x in combined_info for x in ["hdr10", "hdr", "smpte2084"])

        if has_dv and has_hdr:
            return "dovi_p8"
        if has_dv:
            return "dovi_other"
        if has_hdr10_plus:
            return "hdr10+"
        if has_hdr:
            return "hdr"

    # 文件名兜底
    if ("dovi" in path_lower or "dolbyvision" in path_lower or "dv" in path_lower) and "hdr" in path_lower:
        return "dovi_p8"
    if any(s in path_lower for s in ["dovi p7", "dovi.p7", "dv.p7", "profile 7", "profile7"]):
        return "dovi_p7"
    if any(s in path_lower for s in ["dovi p5", "dovi.p5", "dv.p5", "profile 5", "profile5"]):
        return "dovi_p5"
    if "dovi" in path_lower or "dolbyvision" in path_lower:
        return "dovi_other"
    if "hdr10+" in path_lower or "hdr10plus" in path_lower:
        return "hdr10+"
    if "hdr" in path_lower:
        return "hdr"

    return "sdr"

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
    """
    根据视频流的宽高判断分辨率。
    考虑到电影经常切除上下黑边（如 1920x800）或左右黑边（如 1804x1080），必须综合判断 width 和 height。
    """
    # 4K: 标准 3840x2160。切黑边可能 3840x1600 等。只要宽>=3800 或 高>=2100 就算
    if width >= 3800 or height >= 2100: return 4, "4k"
    
    # 1080p: 标准 1920x1080。切黑边可能 1920x800，或者左右切边 1804x1080
    if width >= 1800 or height >= 1000: return 3, "1080p"
    
    # 720p: 标准 1280x720。切黑边可能 1280x536
    if width >= 1200 or height >= 700: return 2, "720p"
    
    # 480p: 标准 720x480, 854x480
    if width >= 700 or height >= 480: return 1, "480p"
    
    return 0, "未知"

def normalize_lang_code(lang_str: str) -> str:
    """
    统一语言代码标准化 (动态映射版)：
    读取用户配置的语言映射表，将各种奇葩的输入统一归一化为 3 位 ISO 代码。
    如果匹配不到，返回原字符串的小写。
    """
    if not lang_str:
        return ""

    lang_str = str(lang_str).lower().strip()

    # 1. 优先处理硬编码的常见中文别名 (防止用户把映射表删空导致核心逻辑崩溃)
    if lang_str in ['guo', 'guoyu', 'chs', 'zh-cn', 'zh-sg', 'zh-hans', 'cmn', 'mandarin', '国语', '普通话', '中文', '简体', '简中']:
        return 'chi'
    if lang_str in ['cht', 'zh-hk', 'zh-tw', 'hk', 'tw', 'cantonese', '粤语', '繁体', '繁中', '粤配', '粤英双语', '港配', '粤语配音', '广东话']:
        return 'yue'

    # 2. 动态读取用户配置的语言映射表
    from database import settings_db
    import utils
    lang_mapping = settings_db.get_setting('language_mapping')
    if not lang_mapping:
        lang_mapping = utils.DEFAULT_LANGUAGE_MAPPING

    # 3. 遍历映射表进行匹配
    for item in lang_mapping:
        val = (item.get('value') or '').lower() # 2位代码
        aliases = item.get('aliases', [])       # 3位代码/别名
        if isinstance(aliases, str):
            aliases = [a.strip().lower() for a in aliases.split(',')]
        else:
            aliases = [str(a).lower() for a in aliases]

        # 如果匹配到了 2位代码、3位别名，或者是中文标签本身
        if lang_str == val or lang_str in aliases or lang_str == item.get('label', '').lower():
            # 优先返回 3 位代码 (从别名里找长度为 3 的)
            three_letter_aliases = [a for a in aliases if len(a) == 3]
            if three_letter_aliases:
                return three_letter_aliases[0]
            # 如果没有 3 位代码，返回 2 位代码
            if val:
                return val

    # 兜底：如果都没匹配上，返回原字符串
    return lang_str

def get_lang_display_label(lang_code: str) -> str:
    """
    根据标准化的语言代码，反查其对应的中文显示标签。
    """
    if not lang_code:
        return "未知"
        
    lang_code = lang_code.lower().strip()
    
    from database import settings_db
    import utils
    lang_mapping = settings_db.get_setting('language_mapping')
    if not lang_mapping:
        lang_mapping = utils.DEFAULT_LANGUAGE_MAPPING
        
    for item in lang_mapping:
        val = (item.get('value') or '').lower()
        aliases = item.get('aliases', [])
        if isinstance(aliases, str):
            aliases = [a.strip().lower() for a in aliases.split(',')]
        else:
            aliases = [str(a).lower() for a in aliases]
            
        if lang_code == val or lang_code in aliases:
            return item.get('label', '未知')
            
    return lang_code.upper()

def _get_detected_languages_from_streams(
    media_streams: List[dict],
    stream_type: str
) -> set:
    """
    返回统一规则代码：
    音轨：chi/yue/eng/jpn/kor
    字幕：chi(简体)/yue(繁体)/eng/jpn/kor
    """
    detected_langs = set()

    for stream in media_streams:
        if stream.get('Type') != stream_type:
            continue
            
        stream_langs = set()

        # 1. 先看标题和显示标题 (优先级最高)
        raw_title = stream.get('Title') or ''
        raw_display = stream.get('DisplayTitle') or ''
        title_string = f"{raw_title} {raw_display}".lower().strip()

        if title_string:
            for lang_key, keywords in AUDIO_SUBTITLE_KEYWORD_MAP.items():
                normalized_lang_key = lang_key.replace('sub_', '')
                if any(keyword.lower() in title_string for keyword in keywords):
                    stream_langs.add(normalized_lang_key)

        # 2. 再看 Language 字段
        lang_code = str(stream.get('Language', '')).lower().strip()
        if lang_code:
            norm_lang = normalize_lang_code(lang_code)
            if norm_lang:
                stream_langs.add(norm_lang)

        # 3. ★★★ 核心冲突解决 ★★★
        # 如果一条音轨同时被识别为 yue 和 chi (例如 Title="yue", Language="chi")
        if 'yue' in stream_langs and 'chi' in stream_langs:
            # 只要标题里没有明确写国语/普通话，我们就认为它是纯粤语，剔除被 Language 误导的 chi
            if not any(k in title_string for k in ['guo', '国', 'mandarin']):
                stream_langs.remove('chi')

        detected_langs.update(stream_langs)

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

    # 恢复语言标签提取 (保证洗版和筛选规则正常工作)
    detected_audio_langs = _get_detected_languages_from_streams(media_streams, 'Audio')
    audio_str = ', '.join(sorted([AUDIO_DISPLAY_MAP.get(lang, lang) for lang in detected_audio_langs]))
    
    # ★★★ 增强音频 (Audio) 的文件名兜底 ★★★
    # 如果 Emby 没分析出音轨语言，尝试从文件名提取常见音频格式作为展示
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
            audio_str = " | ".join(found_audios) 
        else:
            audio_str = '无' 

    # 提取字幕语言
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

def parse_full_asset_details(item_details: dict, id_to_parent_map: dict = None, library_guid: str = None, local_mediainfo_path: str = None) -> dict:
    """
    视频流分析主函数 (神医融合版)
    优先读取神医插件生成的 -mediainfo.json，原文照搬并提取展示标签。
    """
    # 提取并计算时长 (分钟)
    runtime_ticks = item_details.get('RunTimeTicks')
    runtime_min = round(runtime_ticks / 600000000) if runtime_ticks else None

    item_id = str(item_details.get("Id"))
    ancestors = []
    if id_to_parent_map and item_id:
        ancestors = calculate_ancestor_ids(item_id, id_to_parent_map, library_guid)

    # ★★★ 核心修复 1：如果没有传入路径，主动去同级目录寻找 JSON ★★★
    if not local_mediainfo_path:
        file_path = item_details.get('Path', '')
        if file_path and not file_path.startswith('http'):
            guessed_path = os.path.splitext(file_path)[0] + "-mediainfo.json"
            if os.path.exists(guessed_path):
                local_mediainfo_path = guessed_path

    raw_shenyi_data = None
    if local_mediainfo_path and os.path.exists(local_mediainfo_path):
        try:
            with open(local_mediainfo_path, 'r', encoding='utf-8') as f:
                raw_shenyi_data = json.load(f)
        except Exception as e:
            logger.error(f"读取神医媒体信息文件失败 {local_mediainfo_path}: {e}")

    primary_source = None
    media_streams = []
    
    # ★★★ 提取 Emby 原生的流信息 (用于提取外挂字幕) ★★★
    emby_media_sources = item_details.get("MediaSources", [])
    emby_primary_source = emby_media_sources[0] if emby_media_sources and len(emby_media_sources) > 0 else None
    emby_streams = (emby_primary_source.get("MediaStreams") if emby_primary_source else None) or item_details.get("MediaStreams", [])

    # ★★★ 核心修复 2：兼容两种不同的 JSON 嵌套格式，并融合外挂字幕 ★★★
    if raw_shenyi_data and isinstance(raw_shenyi_data, list) and len(raw_shenyi_data) > 0:
        first_item = raw_shenyi_data[0]
        if "MediaSourceInfo" in first_item:
            primary_source = first_item.get("MediaSourceInfo", {})
        else:
            primary_source = first_item
            
        # 1. 先拿视频文件内嵌的流 (视频、音频、内嵌字幕)
        media_streams = primary_source.get("MediaStreams", [])
        
        # 2. ★★★ 核心修补：从 Emby 数据中把“外挂字幕”揪出来，塞进我们的流列表里 ★★★
        if emby_streams:
            for stream in emby_streams:
                # 只要是字幕，且被 Emby 标记为外挂 (IsExternal)，就加进来
                if stream.get("Type") == "Subtitle" and stream.get("IsExternal"):
                    media_streams.append(stream)
                    
    else:
        # 兜底：如果没有神医 JSON，完全使用 Emby 原始数据
        primary_source = emby_primary_source
        media_streams = emby_streams

    container = (primary_source.get("Container") if primary_source else None) or item_details.get("Container")
    size_bytes = (primary_source.get("Size") if primary_source else None) or item_details.get("Size")

    date_added_to_library = item_details.get("DateCreated")

    asset = {
        "emby_item_id": item_details.get("Id"), 
        "path": item_details.get("Path", ""),
        "size_bytes": size_bytes,   
        "container": container,     
        "video_codec": None, 
        "video_bitrate_mbps": None, 
        "bit_depth": None,          
        "frame_rate": None,         
        "audio_tracks": [], 
        "subtitles": [],
        "date_added_to_library": date_added_to_library,
        "ancestor_ids": ancestors,
        "runtime_minutes": runtime_min
    }
    
    # 遍历流信息提取基础数据
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
            
    # 生成前端展示用的 display 标签
    fake_details_for_analysis = item_details.copy()
    fake_details_for_analysis['MediaStreams'] = media_streams 
    
    display_tags = analyze_media_asset(fake_details_for_analysis)
    asset.update(display_tags)
    
    return asset

# --- 判断电影是否满足订阅条件 ---
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

# --- 剧集完结状态检查 (共享逻辑) ---
def check_series_completion(tmdb_id: int, api_key: str, season_number: Optional[int] = None, series_name: str = "未知剧集") -> bool:
    """
    检查剧集或特定季是否已完结。
    用于判断是否开启洗版模式 (best_version=1)。
    
    逻辑：
    1. 剧集状态为 Ended/Canceled -> 视为完结
    2. 最后一集播出日期已过 (<= Today) 且 总集数 > 5 -> 视为完结 (防止只有1-2集的占位数据误判)
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
                        # ★★★ 新增：集数阈值检查，防止只有1集的条目被误判完结 ★★★
                        if len(episodes) > 5:
                            logger.info(f"  ➜ 《{series_name}》第 {season_number} 季最后一集于 {last_air_date} 播出 (共{len(episodes)}集)，判定已完结。")
                            return True
                        else:
                            logger.info(f"  ➜ 《{series_name}》第 {season_number} 季最后一集虽已播出，但集数过少 ({len(episodes)}集 <= 5集)，为防止误判(如新剧占位)，判定未完结。")
                            return False
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
                        # 这里通常保留不做集数限制，因为如果断更30天以上，通常意味着该季暂时也就这样了
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
    
# --- 计算祖先 ID 集合 ---
def calculate_ancestor_ids(item_id: str, id_to_parent_map: dict, library_guid: str) -> List[str]:
    """
    计算一个条目的祖先 ID 集合，包含其直接父级、祖父级等所有上层 ID，直到根节点
    """
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
                    'backdrop_path': parent_details.get('backdrop_path'),
                    'overview': parent_details.get('overview')
                }

                # 3. 获取季详情
                details = get_tv_season_details(parent_id, season_num, tmdb_api_key)
                if details:
                    details['parent_series_tmdb_id'] = str(parent_id)
                    details['parent_title'] = parent_details.get('name')
                    details['parent_poster_path'] = parent_details.get('poster_path')
                    details['parent_backdrop_path'] = parent_details.get('backdrop_path')
                    
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
                'backdrop_path': details.get('backdrop_path') or details.get('parent_backdrop_path'),
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

# --- 分级映射逻辑 ---
def apply_rating_logic(payload: Dict[str, Any], tmdb_data: Dict[str, Any], item_type: str):
    """
    将 TMDb 的原始分级数据，经过配置的映射规则处理后，直接提取出最终的分级字符串。
    不再维护复杂的嵌套结构，直接写入 payload 的 mpaa 字段供 NFO 使用。
    ★ 修复：同时将映射结果存入 _official_rating_map 供数据库写入使用。
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

    # 准备数据源 (扁平化提取)
    available_ratings = {}
    if item_type == "Movie":
        for r in tmdb_data.get('release_dates', {}).get('results', []):
            country_code = r.get('iso_3166_1')
            for rel in r.get('release_dates', []):
                if rel.get('certification'):
                    available_ratings[country_code] = rel.get('certification')
                    break
    elif item_type == "Series":
        for r in tmdb_data.get('content_ratings', {}).get('results', []):
            available_ratings[r.get('iso_3166_1')] = r.get('rating')

    # --- 核心映射逻辑 ---
    target_us_code = None
    
    # 1. 成人强制修正
    if tmdb_data.get('adult') is True:
        logger.warning(f"  ➜ 发现成人内容，忽略任何国家分级强制设为 'XXX'.")
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
                            if item_type == "Series" and r_code in ['G', 'PG', 'PG-13', 'R', 'NC-17']: continue
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

    if target_us_code:
        final_rating_str = target_us_code
        available_ratings['US'] = target_us_code # 确保 US 分级在字典中

    # 4. 直接写入 payload 根节点供 NFO 使用
    if final_rating_str:
        payload['mpaa'] = final_rating_str
        payload['certification'] = final_rating_str
        
    # ★★★ 核心修复：将映射后的完整分级字典存入隐藏字段，供数据库写入使用 ★★★
    payload['_official_rating_map'] = available_ratings

def construct_metadata_payload(item_type: str, tmdb_data: Dict[str, Any], 
                                  aggregated_tmdb_data: Optional[Dict[str, Any]] = None,
                                  emby_data_fallback: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    【纯净 NFO 版】将 TMDb 原始数据转换为扁平化的字典，直接供 NFO Builder 使用。
    彻底移除了臃肿的 JSON 骨架模板。
    """
    payload = {}
    if not tmdb_data:
        return payload

    # 1. 基础字段直接拷贝
    exclude_keys = [
        'casts', 'releases', 'release_dates', 'keywords', 'trailers', 
        'content_ratings', 'videos', 'credits', 'genres', 
        'episodes_details', 'seasons_details', 'created_by', 'networks',
        'production_companies'
    ]
    for key, value in tmdb_data.items():
        if key not in exclude_keys:
            payload[key] = value

    # 2. 通用复杂字段处理
    # Genres: 优先 TMDb，Emby 兜底
    if 'genres' in tmdb_data and tmdb_data['genres']:
        payload['genres'] = tmdb_data['genres']
    elif emby_data_fallback and emby_data_fallback.get('Genres'):
        payload['genres'] = [{'id': 0, 'name': g} for g in emby_data_fallback['Genres']]

    # Keywords (统一转为列表)
    if 'keywords' in tmdb_data:
        kw_data = tmdb_data['keywords']
        if item_type == "Movie":
            payload['keywords'] = kw_data.get('keywords', []) if isinstance(kw_data, dict) else (kw_data if isinstance(kw_data, list) else [])
        else:
            payload['keywords'] = kw_data.get('results', []) if isinstance(kw_data, dict) else (kw_data if isinstance(kw_data, list) else [])

    # Studios / Networks
    if 'production_companies' in tmdb_data:
        payload['production_companies'] = tmdb_data['production_companies']
    if 'networks' in tmdb_data:
        payload['networks'] = tmdb_data['networks']

    # 3. 类型特定处理
    if item_type == "Movie":
        apply_rating_logic(payload, tmdb_data, "Movie")

    elif item_type == "Series":
        if 'created_by' in tmdb_data: 
            payload['created_by'] = tmdb_data['created_by']
            
        apply_rating_logic(payload, tmdb_data, "Series")

        # 挂载子项数据 (Seasons / Episodes)
        if aggregated_tmdb_data:
            payload['seasons_details'] = aggregated_tmdb_data.get('seasons_details', [])
            
            raw_episodes = aggregated_tmdb_data.get('episodes_details', {})
            formatted_episodes = {}
            
            # 提取分集所需字段
            for key, ep_data in raw_episodes.items():
                formatted_episodes[key] = {
                    'id': ep_data.get('id'),
                    'season_number': ep_data.get('season_number'),
                    'episode_number': ep_data.get('episode_number'),
                    'name': ep_data.get('name'),
                    'overview': ep_data.get('overview'),
                    'air_date': ep_data.get('air_date'),
                    'vote_average': ep_data.get('vote_average'),
                    'still_path': ep_data.get('still_path'),
                    'credits': {
                        'cast': ep_data.get('credits', {}).get('cast', []),
                        'guest_stars': ep_data.get('credits', {}).get('guest_stars', []),
                        'crew': ep_data.get('credits', {}).get('crew', [])
                    }
                }
            payload['episodes_details'] = formatted_episodes

    # 4. 提取外部 ID (IMDb / TVDb) 和入库时间
    if emby_data_fallback and emby_data_fallback.get('ProviderIds'):
        providers = emby_data_fallback['ProviderIds']
        if 'Imdb' in providers: payload['imdb_id'] = providers['Imdb']
        
    if 'external_ids' in tmdb_data:
        ext = tmdb_data['external_ids']
        if 'imdb_id' in ext and ext['imdb_id']: payload['imdb_id'] = ext['imdb_id']

    if emby_data_fallback and emby_data_fallback.get('DateCreated'):
        payload['date_added'] = emby_data_fallback['DateCreated']

    # 5. 提取导演 (修复原版过滤掉 credits 导致导演丢失的问题)
    credits_data = tmdb_data.get('credits') or tmdb_data.get('casts') or {}
    crew = credits_data.get('crew', [])
    directors = [d for d in crew if d.get('job') == 'Director']
    if directors:
        payload.setdefault('casts', {})['crew'] = directors

    return payload

def reconstruct_metadata_from_db(db_row: Dict[str, Any], actors_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    【纯净 NFO 版】将数据库记录还原为扁平化的字典，直接供 NFO Builder 使用。
    彻底移除了臃肿的 JSON 骨架模板。
    """
    item_type = db_row.get('item_type')
    payload = {}

    # 1. 基础字段映射
    payload['id'] = int(db_row.get('tmdb_id') or 0)
    payload['overview'] = db_row.get('overview')
    payload['original_language'] = db_row.get('original_language')
    payload['status'] = db_row.get('watchlist_tmdb_status')
    payload['backdrop_path'] = db_row.get('backdrop_path')
    payload['homepage'] = db_row.get('homepage')
    payload['vote_average'] = db_row.get('rating')
    payload['poster_path'] = db_row.get('poster_path')
    payload['imdb_id'] = db_row.get('imdb_id')
    payload['tagline'] = db_row.get('tagline')
    
    date_added = db_row.get('date_added')
    if date_added:
        payload['date_added'] = str(date_added)

    # 标题与日期
    if item_type == "Movie":
        payload['title'] = db_row.get('title')
        payload['original_title'] = db_row.get('original_title')
        r_date = db_row.get('release_date')
        payload['release_date'] = str(r_date) if r_date else ''
        payload['runtime'] = db_row.get('runtime_minutes')
    else:
        payload['name'] = db_row.get('title')
        payload['original_name'] = db_row.get('original_title')
        r_date = db_row.get('release_date')
        payload['first_air_date'] = str(r_date) if r_date else ''
        l_date = db_row.get('last_air_date')
        payload['last_air_date'] = str(l_date) if l_date else ''
        payload['number_of_episodes'] = db_row.get('total_episodes', 0)
        payload['number_of_seasons'] = 1 

    # 2. 复杂 JSON 字段还原
    if db_row.get('genres_json'):
        try:
            raw_genres = db_row['genres_json']
            genres_data = json.loads(raw_genres) if isinstance(raw_genres, str) else raw_genres
            if genres_data:
                if isinstance(genres_data[0], str):
                    payload['genres'] = [{"id": 0, "name": g} for g in genres_data]
                else:
                    payload['genres'] = genres_data
        except Exception: pass

    if item_type == 'Movie':
        if db_row.get('production_companies_json'):
            try:
                raw = db_row['production_companies_json']
                data = json.loads(raw) if isinstance(raw, str) else raw
                if data: payload['production_companies'] = data
            except Exception: pass
    elif item_type == 'Series':
        merged_list = []
        seen_ids = set()
        if db_row.get('networks_json'):
            try:
                raw = db_row['networks_json']
                nets = json.loads(raw) if isinstance(raw, str) else raw
                if nets:
                    for n in nets:
                        nid = n.get('id')
                        if nid and nid not in seen_ids:
                            merged_list.append(n)
                            seen_ids.add(nid)
            except Exception: pass
        if db_row.get('production_companies_json'):
            try:
                raw = db_row['production_companies_json']
                comps = json.loads(raw) if isinstance(raw, str) else raw
                if comps:
                    for c in comps:
                        cid = c.get('id')
                        if cid and cid not in seen_ids:
                            merged_list.append(c)
                            seen_ids.add(cid)
            except Exception: pass
        if merged_list:
            payload['networks'] = merged_list

    if db_row.get('directors_json'):
        try:
            raw_directors = db_row['directors_json']
            directors_list = json.loads(raw_directors) if isinstance(raw_directors, str) else raw_directors
            if directors_list:
                if item_type == 'Series':
                    payload['created_by'] = directors_list
                else:
                    crew_list = [{"id": d.get('id'), "name": d.get('name'), "job": "Director", "department": "Directing"} for d in directors_list]
                    payload.setdefault('casts', {})['crew'] = crew_list
        except Exception: pass

    if db_row.get('countries_json'):
        try:
            raw_countries = db_row['countries_json']
            countries_list = json.loads(raw_countries) if isinstance(raw_countries, str) else raw_countries
            if countries_list:
                if item_type == 'Series':
                    payload['origin_country'] = countries_list
                else:
                    payload['production_countries'] = [{"iso_3166_1": c, "name": ""} for c in countries_list]
        except Exception: pass
        
    if db_row.get('keywords_json'):
        try:
            raw_kw = db_row['keywords_json']
            kw_list = json.loads(raw_kw) if isinstance(raw_kw, str) else raw_kw
            if kw_list:
                payload['keywords'] = kw_list
        except Exception: pass

    # 3. 分级 (Official Rating)
    if db_row.get('official_rating_json'):
        try:
            raw_rating = db_row['official_rating_json']
            ratings_map = json.loads(raw_rating) if isinstance(raw_rating, str) else raw_rating
            
            # ★★★ 核心修复：严格只取映射后的 US 分级。绝不拿其他国家的原始分级兜底 ★★★
            rating_val = ratings_map.get('US')
            
            if rating_val:
                payload['mpaa'] = rating_val
                payload['certification'] = rating_val
        except Exception: pass

    return payload

def translate_tmdb_metadata_recursively(
    item_type: str,
    tmdb_data: Dict[str, Any],
    ai_translator: Any,
    item_name: str = "",
    tmdb_api_key: str = None,
    config: dict = None
):
    """
    【终极大一统翻译引擎】
    递归翻译 TMDb 数据的标题、简介、标语。
    地毯式翻译所有主创、导演、演员、客串明星的【姓名】和【角色名】。
    """
    if not ai_translator or not tmdb_data or not config:
        return

    pending_items = {}
    pending_persons = set()
    pending_roles = set()
    translated_count = 0

    # ★ 统计计数器
    stats = {
        'original_cast_count': 0,
        'truncated_cast_count': 0,

        # 待处理词条总数：缓存命中 + 实际提交
        'title_pending_count': 0,
        'overview_pending_count': 0,
        'tagline_pending_count': 0,
        'person_pending_count': 0,
        'role_pending_count': 0,

        # 实际提交 AI 的数量
        'title_needs_translation': 0,
        'overview_needs_translation': 0,
        'tagline_needs_translation': 0,
        'person_ai_calls': 0,
        'role_ai_calls': 0,

        # 缓存命中
        'title_cache_hits': 0,
        'overview_cache_hits': 0,
        'tagline_cache_hits': 0,
        'person_cache_hits': 0,
        'role_cache_hits': 0,
    }

    translate_title_enabled = config.get(constants.CONFIG_OPTION_AI_TRANSLATE_TITLE, False)
    translate_overview_enabled = config.get(constants.CONFIG_OPTION_AI_TRANSLATE_OVERVIEW, False)
    translate_ep_overview_enabled = config.get(constants.CONFIG_OPTION_AI_TRANSLATE_EPISODE_OVERVIEW, False)
    translate_actor_enabled = config.get(constants.CONFIG_OPTION_AI_TRANSLATE_ACTOR_ROLE, False)
    remove_no_avatar = config.get(constants.CONFIG_OPTION_REMOVE_ACTORS_WITHOUT_AVATARS, True)

    # --- 1. 收集与缓存检查阶段 ---
    def _collect_single_item(data_dict: Dict, specific_item_type: str):
        current_tmdb_id = data_dict.get('id')
        if not current_tmdb_id:
            return

        tmdb_id_str = str(current_tmdb_id)
        title_key = 'title' if specific_item_type == 'Movie' else 'name'

        local_info = media_db.get_local_translation_info(tmdb_id_str, specific_item_type)

        needs_title = False
        needs_overview = False
        needs_tagline = False

        # A. 检查简介 Overview
        is_ep = specific_item_type == 'Episode'
        if (not is_ep and translate_overview_enabled) or (is_ep and translate_ep_overview_enabled):
            overview = data_dict.get('overview')
            if not overview or not utils.contains_chinese(overview):
                if local_info and local_info.get('overview') and utils.contains_chinese(local_info['overview']):
                    data_dict['overview'] = local_info['overview']
                    stats['overview_pending_count'] += 1
                    stats['overview_cache_hits'] += 1
                else:
                    if not overview and tmdb_api_key:
                        try:
                            if specific_item_type == 'Movie':
                                en_data = get_movie_details(int(tmdb_id_str), tmdb_api_key, language="en-US")
                                data_dict['overview'] = en_data.get('overview', '')
                            elif specific_item_type == 'Series':
                                en_data = get_tv_details(int(tmdb_id_str), tmdb_api_key, language="en-US")
                                data_dict['overview'] = en_data.get('overview', '')
                        except Exception:
                            pass

                    if data_dict.get('overview'):
                        needs_overview = True
                        stats['overview_pending_count'] += 1
                        stats['overview_needs_translation'] += 1

        # B. 检查标题 Title
        if translate_title_enabled:
            current_title = data_dict.get(title_key)
            if current_title and not utils.contains_chinese(current_title):
                if local_info and local_info.get('title') and utils.contains_chinese(local_info['title']):
                    data_dict[title_key] = local_info['title']
                    stats['title_pending_count'] += 1
                    stats['title_cache_hits'] += 1
                else:
                    needs_title = True
                    stats['title_pending_count'] += 1
                    stats['title_needs_translation'] += 1

        # C. 检查标语 Tagline
        if translate_title_enabled and specific_item_type in ['Movie', 'Series']:
            tagline = data_dict.get('tagline')
            if not tagline or not utils.contains_chinese(tagline):
                # 先用本地缓存回填，避免重复翻译
                if local_info and local_info.get('tagline') and utils.contains_chinese(local_info['tagline']):
                    data_dict['tagline'] = local_info['tagline']
                    stats['tagline_pending_count'] += 1
                    stats['tagline_cache_hits'] += 1
                else:
                    # 本地没有中文标语，再去补英文原文，准备送翻译
                    if not tagline and tmdb_api_key:
                        try:
                            if specific_item_type == 'Movie':
                                en_data = get_movie_details(int(tmdb_id_str), tmdb_api_key, language="en-US")
                                data_dict['tagline'] = en_data.get('tagline', '')
                            elif specific_item_type == 'Series':
                                en_data = get_tv_details(int(tmdb_id_str), tmdb_api_key, language="en-US")
                                data_dict['tagline'] = en_data.get('tagline', '')
                        except Exception:
                            pass

                    if data_dict.get('tagline'):
                        needs_tagline = True
                        stats['tagline_pending_count'] += 1
                        stats['tagline_needs_translation'] += 1

        if needs_title or needs_overview or needs_tagline:
            pending_items[tmdb_id_str] = {
                "type": specific_item_type,
                "title_key": title_key,
                "title": data_dict.get(title_key) if needs_title else None,
                "overview": data_dict.get('overview') if needs_overview else None,
                "tagline": data_dict.get('tagline') if needs_tagline else None,
                "ref": data_dict
            }

        # D. 收集人物和角色
        if translate_actor_enabled:
            credits_data = data_dict.get('credits') or data_dict.get('aggregate_credits') or data_dict.get('casts') or {}

            for crew_member in credits_data.get('crew', []):
                if crew_member.get('job') in ['Director', 'Series Director']:
                    name = crew_member.get('name')
                    if name and not utils.contains_chinese(name):
                        pending_persons.add(name)

            max_actors = config.get(constants.CONFIG_OPTION_MAX_ACTORS_TO_PROCESS, 30)
            max_ep_actors = config.get(constants.CONFIG_OPTION_MAX_EPISODE_ACTORS_TO_PROCESS, 0) # 读取新配置
            
            try:
                limit = int(max_actors)
                if limit <= 0: limit = 30
            except Exception:
                limit = 30
                
            try:
                ep_limit = int(max_ep_actors)
            except Exception:
                ep_limit = 0

            def _smart_truncate(actor_list, max_limit):
                if not actor_list: return []
                stats['original_cast_count'] += len(actor_list)
                valid_actors = [a for a in actor_list if a.get('profile_path')] if remove_no_avatar else actor_list
                valid_actors.sort(key=lambda x: x.get('order') if x.get('order') is not None else 999)
                truncated = valid_actors[:max_limit]
                stats['truncated_cast_count'] += len(truncated)
                return truncated

            # ★★★ 核心优化：如果是分集，且配置为 0，直接清空演员表，不送去翻译 ★★★
            if specific_item_type == 'Episode' and ep_limit == 0:
                if 'cast' in credits_data: credits_data['cast'] = []
                if 'guest_stars' in credits_data: credits_data['guest_stars'] = []
            else:
                # 动态决定当前层级的限制人数
                current_limit = ep_limit if specific_item_type == 'Episode' else limit
                guest_limit = ep_limit if specific_item_type == 'Episode' else 10
                
                if 'cast' in credits_data:
                    credits_data['cast'] = _smart_truncate(credits_data['cast'], current_limit)
                if 'guest_stars' in credits_data:
                    credits_data['guest_stars'] = _smart_truncate(credits_data['guest_stars'], guest_limit)

            all_actors = credits_data.get('cast', []) + credits_data.get('guest_stars', [])
            for actor in all_actors:
                name = actor.get('name')
                if name and not utils.contains_chinese(name):
                    pending_persons.add(name)

                character = actor.get('character')
                if character:
                    cleaned_char = utils.clean_character_name_static(character)
                    if cleaned_char and not utils.contains_chinese(cleaned_char):
                        pending_roles.add(cleaned_char)

    # --- 遍历收集 ---
    if item_type == 'Movie':
        _collect_single_item(tmdb_data, 'Movie')

    elif item_type == 'Series':
        series_details = tmdb_data.get('series_details', tmdb_data)
        _collect_single_item(series_details, 'Series')

        for season in tmdb_data.get("seasons_details", []):
            _collect_single_item(season, 'Season')

        episodes_container = tmdb_data.get("episodes_details", {})
        episodes_list = episodes_container.values() if isinstance(episodes_container, dict) else episodes_container
        for ep in episodes_list:
            _collect_single_item(ep, 'Episode')

    # ★ 收集完成后，记录人物/角色待翻词条总数
    stats['person_pending_count'] = len(pending_persons)
    stats['role_pending_count'] = len(pending_roles)

    # --- 2. 批量翻译阶段 ---
    BATCH_SIZE = 20

    if pending_items:
        logger.info("  ➜ [AI翻译引擎] 开始进行翻译...")

        # 1. 翻译简介
        overviews_to_translate = {k: v["overview"] for k, v in pending_items.items() if v["overview"]}
        if overviews_to_translate:
            items_list = list(overviews_to_translate.items())
            for i in range(0, len(items_list), BATCH_SIZE):
                batch_dict = dict(items_list[i:i + BATCH_SIZE])
                trans_results = ai_translator.batch_translate_overviews(batch_dict, context_title=item_name)

                for tid, trans_text in trans_results.items():
                    if trans_text and utils.contains_chinese(trans_text) and tid in pending_items:
                        pending_items[tid]["ref"]['overview'] = trans_text
                        translated_count += 1

                import time
                time.sleep(1)

        # 2. 翻译标语
        taglines_to_translate = {k: v["tagline"] for k, v in pending_items.items() if v["tagline"]}
        if taglines_to_translate:
            items_list = list(taglines_to_translate.items())
            for i in range(0, len(items_list), BATCH_SIZE):
                batch_dict = dict(items_list[i:i + BATCH_SIZE])
                trans_results = ai_translator.batch_translate_overviews(batch_dict, context_title=item_name)

                for tid, trans_text in trans_results.items():
                    if trans_text and utils.contains_chinese(trans_text) and tid in pending_items:
                        pending_items[tid]["ref"]['tagline'] = trans_text
                        translated_count += 1

                import time
                time.sleep(1)

        # 3. 翻译标题
        titles_to_translate = {k: v["title"] for k, v in pending_items.items() if v["title"]}
        if titles_to_translate:
            items_list = list(titles_to_translate.items())
            for i in range(0, len(items_list), BATCH_SIZE):
                batch_dict = dict(items_list[i:i + BATCH_SIZE])
                trans_results = ai_translator.batch_translate_titles(batch_dict, media_type="Episode")

                for tid, trans_text in trans_results.items():
                    if trans_text and utils.contains_chinese(trans_text) and tid in pending_items:
                        title_key = pending_items[tid]["title_key"]
                        pending_items[tid]["ref"][title_key] = trans_text
                        translated_count += 1

                import time
                time.sleep(1)

    # --- 3. 翻译人物姓名和角色名 ---
    if pending_persons or pending_roles:
        person_trans_map = {}
        role_trans_map = {}

        from database import actor_db
        db_manager = actor_db.ActorDBManager()

        role_translation_mode = config.get(constants.CONFIG_OPTION_AI_TRANSLATION_MODE, 'fast')

        item_title = tmdb_data.get('title') or tmdb_data.get('name') or item_name
        item_year = None
        release_date = tmdb_data.get('release_date') or tmdb_data.get('first_air_date')
        if release_date and len(release_date) >= 4:
            item_year = release_date[:4]

        with connection.get_db_connection() as conn:
            with conn.cursor() as cursor:

                # 人名：固定音译模式 + 强制缓存
                if pending_persons:
                    api_list = []

                    for name in pending_persons:
                        cached = db_manager.get_translation_from_db(cursor, name)
                        if cached and cached.get('translated_text'):
                            person_trans_map[name] = cached['translated_text']
                            stats['person_cache_hits'] += 1
                        else:
                            api_list.append(name)

                    stats['person_ai_calls'] = len(api_list)

                    if api_list:
                        logger.info(
                            f"  ➜ [AI翻译引擎] 提交 {len(api_list)} 个人物姓名进行翻译 "
                            f"(模式: transliterate, 缓存命中: {stats['person_cache_hits']})..."
                        )

                        for i in range(0, len(api_list), BATCH_SIZE):
                            batch_names = api_list[i:i + BATCH_SIZE]
                            trans_results = ai_translator.batch_translate(batch_names, mode='transliterate')

                            if isinstance(trans_results, list) and len(trans_results) == len(batch_names):
                                trans_results = {
                                    batch_names[j]: trans_results[j]
                                    for j in range(len(batch_names))
                                }
                            elif not isinstance(trans_results, dict):
                                trans_results = {}

                            for k, v in trans_results.items():
                                if isinstance(v, (list, tuple, set)):
                                    v = next((x for x in v if isinstance(x, str) and x.strip()), None)

                                if not v:
                                    continue

                                v = str(v).strip()

                                if v and utils.contains_chinese(v):
                                    person_trans_map[k] = v
                                    db_manager.save_translation_to_db(cursor, k, v, ai_translator.provider)

                            import time
                            time.sleep(1)

                # 角色名：根据配置模式，顾问模式跳过缓存
                if pending_roles:
                    api_list = []

                    if role_translation_mode == 'quality':
                        api_list = list(pending_roles)
                    else:
                        for role in pending_roles:
                            cached = db_manager.get_translation_from_db(cursor, role)
                            if cached and cached.get('translated_text'):
                                role_trans_map[role] = cached['translated_text']
                                stats['role_cache_hits'] += 1
                            else:
                                api_list.append(role)

                    stats['role_ai_calls'] = len(api_list)

                    if api_list:
                        logger.info(
                            f"  ➜ [AI翻译引擎] 提交 {len(api_list)} 个角色名进行翻译 "
                            f"(模式: {role_translation_mode}, 缓存命中: {stats['role_cache_hits']})..."
                        )

                        for i in range(0, len(api_list), BATCH_SIZE):
                            batch_roles = api_list[i:i + BATCH_SIZE]
                            trans_results = ai_translator.batch_translate(
                                batch_roles,
                                mode=role_translation_mode,
                                title=item_title,
                                year=item_year
                            )

                            if isinstance(trans_results, list) and len(trans_results) == len(batch_roles):
                                trans_results = {
                                    batch_roles[j]: trans_results[j]
                                    for j in range(len(batch_roles))
                                }
                            elif not isinstance(trans_results, dict):
                                trans_results = {}

                            for k, v in trans_results.items():
                                if isinstance(v, (list, tuple, set)):
                                    v = next((x for x in v if isinstance(x, str) and x.strip()), None)

                                if not v:
                                    continue

                                v = str(v).strip()

                                if not utils.contains_chinese(v):
                                    continue

                                cleaned_v = utils.clean_character_name_static(v)
                                if not cleaned_v:
                                    continue

                                role_trans_map[k] = cleaned_v

                                if role_translation_mode != 'quality':
                                    db_manager.save_translation_to_db(cursor, k, cleaned_v, ai_translator.provider)

                            import time
                            time.sleep(1)

        # 回填翻译结果到 JSON 树
        if person_trans_map or role_trans_map:

            def _apply_person_trans(data_dict):
                credits_data = data_dict.get('credits') or data_dict.get('aggregate_credits') or data_dict.get('casts') or {}

                # 替换导演
                for crew_member in credits_data.get('crew', []):
                    if crew_member.get('job') in ['Director', 'Series Director']:
                        name = crew_member.get('name')
                        if name in person_trans_map:
                            crew_member['original_name'] = name
                            crew_member['name'] = person_trans_map[name]

                # 替换主创
                for creator in data_dict.get('created_by', []):
                    name = creator.get('name')
                    if name in person_trans_map:
                        creator['original_name'] = name
                        creator['name'] = person_trans_map[name]

                # 替换演员和客串
                all_actors = credits_data.get('cast', []) + credits_data.get('guest_stars', [])
                for actor in all_actors:
                    name = actor.get('name')
                    if name in person_trans_map:
                        actor['original_name'] = name
                        actor['name'] = person_trans_map[name]

                    character = actor.get('character')
                    if character:
                        cleaned_char = utils.clean_character_name_static(character)
                        if cleaned_char in role_trans_map:
                            actor['character'] = utils.clean_character_name_static(role_trans_map[cleaned_char])

            if item_type == 'Movie':
                _apply_person_trans(tmdb_data)

            elif item_type == 'Series':
                _apply_person_trans(tmdb_data.get('series_details', tmdb_data))

                for season in tmdb_data.get("seasons_details", []):
                    _apply_person_trans(season)

                episodes_container = tmdb_data.get("episodes_details", {})
                episodes_list = episodes_container.values() if isinstance(episodes_container, dict) else episodes_container
                for ep in episodes_list:
                    _apply_person_trans(ep)

            translated_count += len(person_trans_map) + len(role_trans_map)

    # --- 4. 统计汇总日志 ---
    total_pending = (
        stats['title_pending_count'] +
        stats['overview_pending_count'] +
        stats['tagline_pending_count'] +
        stats['person_pending_count'] +
        stats['role_pending_count']
    )

    total_cache = (
        stats['title_cache_hits'] +
        stats['overview_cache_hits'] +
        stats['tagline_cache_hits'] +
        stats['person_cache_hits'] +
        stats['role_cache_hits']
    )

    total_submit = (
        stats['title_needs_translation'] +
        stats['overview_needs_translation'] +
        stats['tagline_needs_translation'] +
        stats['person_ai_calls'] +
        stats['role_ai_calls']
    )

    logger.info("  ➜ [AI翻译引擎] 翻译统计汇总")
    logger.info(
        f"  ➜ 演员节点: 原始 {stats['original_cast_count']} 人 → "
        f"最终保留 {stats['truncated_cast_count']} 人（含剧/季/集）"
    )
    logger.info(
        f"  ➜ 待翻词条: 标题 {stats['title_pending_count']} | "
        f"简介 {stats['overview_pending_count']} | "
        f"标语 {stats['tagline_pending_count']} | "
        f"人名 {stats['person_pending_count']} | "
        f"角色 {stats['role_pending_count']}"
    )
    logger.info(
        f"  ➜ 缓存命中: 标题 {stats['title_cache_hits']} | "
        f"简介 {stats['overview_cache_hits']} | "
        f"标语 {stats['tagline_cache_hits']} | "
        f"人名 {stats['person_cache_hits']} | "
        f"角色 {stats['role_cache_hits']}"
    )
    logger.info(
        f"  ➜ 实际提交: 标题 {stats['title_needs_translation']} | "
        f"简介 {stats['overview_needs_translation']} | "
        f"标语 {stats['tagline_needs_translation']} | "
        f"人名 {stats['person_ai_calls']} | "
        f"角色 {stats['role_ai_calls']}"
    )

def evaluate_season_airing_status(tmdb_id: str, season_number: int, api_key: str) -> bool:
    """
    【主动连载判定 - 严丝合缝版】
    实时向 TMDb 查询指定季是否正在连载/待播。
    逻辑严格对齐 watchlist_processor._process_one_series，拒绝缓冲期，防止目录反复横跳。
    """
    if not api_key or not tmdb_id or season_number is None:
        return False

    try:
        # 1. 获取该季详情
        season_details = get_tv_season_details(tmdb_id, season_number, api_key)
        if not season_details:
            return False

        episodes = season_details.get('episodes', [])
        tmdb_episode_count = len(episodes) # 预检直接用列表长度最稳
        
        if tmdb_episode_count == 0:
            return False

        today = datetime.now(timezone.utc).date()
        last_aired_ep_num = 0
        
        # 2. 规则一：是否有明确的未来待播集？
        for ep in episodes:
            air_date_str = ep.get('air_date')
            if air_date_str:
                try:
                    air_date = datetime.strptime(air_date_str, '%Y-%m-%d').date()
                    # 严格大于今天才算待播。今天播出的，在核心逻辑里算作已播(<= today)
                    if air_date > today:
                        return True
                    else:
                        # 记录已播出的最大集号
                        last_aired_ep_num = max(last_aired_ep_num, ep.get('episode_number', 0))
                except ValueError:
                    continue

        # 3. 规则二：是否未播完？(TMDb 数据滞后判定)
        # 官方声明该季有 N 集，但目前已播出的集号小于 N，说明还在连载
        official_count = season_details.get('episode_count', tmdb_episode_count)
        if 0 < last_aired_ep_num < official_count:
            return True

        # 4. 规则三：新剧保护 (模拟 Auto Pending 逻辑)
        # 如果总集数很少(<=3)，且最后一集刚播不久(<=30天)，核心逻辑会将其判为 Pending(活跃)
        if official_count <= 3:
            last_ep = episodes[-1]
            last_air_date_str = last_ep.get('air_date')
            if last_air_date_str:
                try:
                    last_air_date = datetime.strptime(last_air_date_str, '%Y-%m-%d').date()
                    if 0 <= (today - last_air_date).days <= 30:
                        return True # 满足新剧待定条件，算作活跃，放进连载目录
                except ValueError:
                    pass

        # 5. 都不满足，说明彻彻底底完结了
        return False

    except Exception as e:
        logger.warning(f"  ➜ 预检连载状态失败 (TMDb:{tmdb_id} S{season_number}): {e}")
        return False
    
def extract_top_directors(tmdb_data: dict, max_count: int = 3) -> list:
    """
    综合提取剧集/电影的导演，并按权重排序截断。
    """
    dir_map = {}
    
    # 1. 提取 created_by
    for c in tmdb_data.get('created_by', []):
        d_id = c.get('id')
        if d_id:
            dir_map[d_id] = {
                'id': d_id, 'name': c.get('name'), 'original_name': c.get('original_name'),
                'is_creator': True, 'ep_count': 9999,
                'profile_path': c.get('profile_path')
            }
            
    # 2. 提取 crew 中的 Director
    credits_data = tmdb_data.get('aggregate_credits') or tmdb_data.get('credits') or tmdb_data.get('casts') or {}
    for c in credits_data.get('crew', []):
        d_id = c.get('id')
        if not d_id: continue
        
        ep_count = 0
        is_director = False
        
        if c.get('job') in ['Director', 'Series Director']:
            is_director = True
            ep_count = 1
            
        for j in c.get('jobs', []):
            if j.get('job') in ['Director', 'Series Director']:
                is_director = True
                ep_count += j.get('episode_count', 1)
                
        if is_director:
            if d_id not in dir_map:
                dir_map[d_id] = {
                    'id': d_id, 'name': c.get('name'), 'original_name': c.get('original_name'),
                    'is_creator': False, 'ep_count': ep_count,
                    'profile_path': c.get('profile_path')
                }
            else:
                dir_map[d_id]['ep_count'] += ep_count
                if not dir_map[d_id]['profile_path'] and c.get('profile_path'):
                    dir_map[d_id]['profile_path'] = c.get('profile_path')

    # 3. 排序并截断
    sorted_dirs = sorted(
        dir_map.values(),
        key=lambda x: (x['is_creator'], x['ep_count'], bool(x['profile_path'])),
        reverse=True
    )[:max_count]
    
    # ★ 返回时带上 original_name
    return [{'id': d['id'], 'name': d['name'], 'original_name': d.get('original_name'), 'job': 'Director', 'profile_path': d['profile_path']} for d in sorted_dirs]
