# utils.py (最终智能匹配版)

import re
import os
import psycopg2
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from urllib.parse import quote_plus
import unicodedata
import logging
logger = logging.getLogger(__name__)
# 尝试导入 pypinyin，如果失败则创建一个模拟函数
try:
    from pypinyin import pinyin, Style
    PYPINYIN_AVAILABLE = True
except ImportError:
    PYPINYIN_AVAILABLE = False
    def pinyin(*args, **kwargs):
        # 如果库不存在，这个模拟函数将导致中文名无法转换为拼音进行匹配
        return []

def contains_chinese(text: Optional[str]) -> bool:
    """检查字符串是否包含中文字符。"""
    if not text:
        return False
    for char in text:
        if '\u4e00' <= char <= '\u9fff' or \
           '\u3400' <= char <= '\u4dbf' or \
           '\uf900' <= char <= '\ufaff':
            return True
    return False

def clean_character_name_static(character_name: Optional[str]) -> str:
    """
    统一格式化角色名：
    - 去除括号内容、前后缀如“饰、配、配音、as”
    - 中外对照时仅保留中文部分
    - 如果仅为“饰 Kevin”这种格式，清理前缀后保留英文，待后续翻译
    """
    if not character_name:
        return ""

    name = str(character_name).strip()

    # 移除括号和中括号的内容
    name = re.sub(r'\(.*?\)|\[.*?\]|（.*?）|【.*?】', '', name).strip()

    # 移除 as 前缀（如 "as Kevin"）
    name = re.sub(r'^(as\s+)', '', name, flags=re.IGNORECASE).strip()

    # 清理前缀中的“饰演/饰/配音/配”（不加判断，直接清理）
    prefix_pattern = r'^((?:饰演|饰|扮演|扮|配音|配|as\b)\s*)+'
    name = re.sub(prefix_pattern, '', name, flags=re.IGNORECASE).strip()

    # 清理后缀中的“饰演/饰/配音/配”
    suffix_pattern = r'(\s*(?:饰演|饰|配音|配))+$'
    name = re.sub(suffix_pattern, '', name).strip()

    # 处理中外对照：“中文 + 英文”形式，只保留中文部分
    match = re.search(r'[a-zA-Z]', name)
    if match:
        # 如果找到了英文字母，取它之前的所有内容
        first_letter_index = match.start()
        chinese_part = name[:first_letter_index].strip()
        
        # 只有当截取出来的部分确实包含中文时，才进行截断。
        # 这可以防止 "Kevin" 这种纯英文名字被错误地清空。
        if re.search(r'[\u4e00-\u9fa5]', chinese_part):
            return chinese_part

    # 如果只有外文，或清理后是英文，保留原值，等待后续翻译流程
    return name.strip()

def generate_search_url(provider: str, title: str, year: Optional[int] = None) -> str:
    """
    【V5 - 语法修复最终版】
    - 修复了 UnboundLocalError，确保变量在使用前被正确定义。
    - 统一了搜索词的生成逻辑，使其更加健壮。
    """
    if not title:
        return ""
    
    # 1. 统一准备搜索词和编码，确保变量在所有分支中都可用
    # 对于网页搜索，带上年份有助于消除歧义
    search_term = f"{title} {year}" if year else title
    encoded_term = quote_plus(search_term)
    
    # 2. 现在，可以安全地根据 provider 选择返回不同的 URL 格式
    if provider == 'baike':
        # 使用百度网页搜索
        return f"https://www.baidu.com/s?wd={encoded_term}"
    
    elif provider == 'wikipedia':
        # 使用 Google 站内搜索维基百科
        return f"https://www.google.com/search?q={encoded_term}+site%3Azh.wikipedia.org"
        
    else:
        # 默认回退到 Google 网页搜索
        return f"https://www.google.com/search?q={encoded_term}"

# --- ★★★ 全新的智能名字匹配核心逻辑 ★★★ ---
def normalize_name_for_matching(name: Optional[str]) -> str:
    """
    将名字极度标准化，用于模糊比较。
    转小写、移除所有非字母数字字符、处理 Unicode 兼容性。
    例如 "Chloë Grace Moretz" -> "chloegracemoretz"
    """
    if not name:
        return ""
    # NFKD 分解可以将 'ë' 分解为 'e' 和 '̈'
    nfkd_form = unicodedata.normalize('NFKD', str(name))
    # 只保留基本字符，去除重音等组合标记
    ascii_name = u"".join([c for c in nfkd_form if not unicodedata.combining(c)])
    # 转小写并只保留字母和数字
    return ''.join(filter(str.isalnum, ascii_name.lower()))

# --- ★★★ 统一分级映射功能 (V2 - 健壮版) ★★★ ---
# 1. 定义我们自己的、统一的、友好的分级体系
UNIFIED_RATING_CATEGORIES = [
    '全年龄', '家长辅导', '青少年', '成人', '限制级', '未知'
]

# 2. 创建从 Emby 原始分级到我们统一体系的映射字典
RATING_MAP = {
    # --- 全年龄 ---
    'g': '全年龄', 'tv-g': '全年龄', 'approved': '全年龄', 'e': '全年龄',
    'u': '全年龄', 'uc': '全年龄',
    '0': '全年龄', '6': '全年龄', '6+': '全年龄',
    'all': '全年龄', 'unrated': '全年龄', 'nr': '全年龄',
    'y': '全年龄', 'tv-y': '全年龄', 'ec': '全年龄',

    # --- 家长辅导 ---
    'pg': '家长辅导', 'tv-pg': '家长辅导',
    '7': '家长辅导', 'tv-y7': '家长辅导', 'tv-y7-fv': '家长辅导',
    '10': '家长辅导',

    # --- 青少年 ---
    'pg-13': '青少年', 'SG-PG13': '青少年', 't': '青少年',
    '12': '青少年', '13': '青少年', '14': '青少年', 'tv-14': '青少年',
    '15': '青少年', '16': '青少年',

    # --- 成人 ---
    'r': '成人', 'm': '成人', 'ma': '成人', 'tv-ma': '成人',
    '17': '成人', '18': '成人', '19': '成人',

    # --- 限制级 ---
    'nc-17': '限制级', 'x': '限制级', 'xxx': '限制级',
    'ao': '限制级', 'rp': '限制级', 'ur': '限制级',
}

# --- ★★★ 新增：关键词中英翻译词典 ★★★ ---
# 一个权威的、可扩展的关键词翻译映射表。
# 格式: "中文": "英文"
KEYWORD_TRANSLATION_MAP = {
    # --- 类型/题材 ---
    "科幻": "science fiction",
    "奇幻": "fantasy",
    "恐怖": "horror",
    "悬疑": "suspense",
    "惊悚": "thriller",
    "犯罪": "crime",
    "动作": "action",
    "冒险": "adventure",
    "喜剧": "comedy",
    "爱情": "romance",
    "战争": "war",
    "西部": "western",
    "历史": "history",
    "传记": "biography",
    "动画": "animation",
    "纪录片": "documentary",
    "音乐": "music",
    "歌舞": "musical",
    "家庭": "family",
    "运动": "sports",
    "灾难": "disaster",
    "武侠": "wuxia",
    "黑色电影": "film noir",
    "古装剧": "costume drama",
    "仙侠": "xianxia",

    # --- 科幻/奇幻元素 ---
    "赛博朋克": "cyberpunk",
    "太空": "space",
    "外星人": "alien",
    "时间旅行": "time travel",
    "机器人": "robot",
    "人工智能": "artificial intelligence (a.i.)",
    "后末日": "post-apocalyptic future",
    "反乌托邦": "dystopia",
    "魔法": "magic",
    "龙": "dragon",
    "怪兽": "monster",
    "神话": "mythology",
    "超级英雄": "superhero",
    "平行宇宙": "parallel universe",
    "漫改": "based on comic",

    # --- 恐怖/惊悚元素 ---
    "丧尸": "zombie",
    "吸血鬼": "vampire",
    "狼人": "werewolf",
    "鬼": "ghost",
    "恶魔": "demon",
    "心理惊悚": "psychological thriller",
    "连环杀手": "serial killer",
    "生存": "survival",
    "血腥": "gore",

    # --- 病毒/感染/瘟疫 ---
    "病毒": "virus",
    "感染": "infection",
    "瘟疫": "plague",
    "变种人": "mutant",

    # --- 犯罪/悬疑元素 ---
    "侦探": "detective",
    "警察": "police",
    "黑帮": "gangster",
    "卧底": "undercover",
    "抢劫": "heist",
    "阴谋": "conspiracy",
    "法庭": "courtroom",

    # --- 其他常见概念 ---
    "青春": "teen",
    "成长": "coming of age",
    "政治": "politics",
    "法律": "legal",
    "医疗": "medical",
    "间谍": "spy",
    "武术": "martial arts",
    "寻宝": "treasure hunt",
    "公路旅行": "road trip",
    "独立电影": "independent film"
}

KEYWORD_ID_MAP = {
    # --- 热门主题 ---
    "丧尸": 12377,
    "吸血鬼": 3133,
    "外星人": 9951,
    "漫改": 9717,
    "漫威电影宇宙": 180547,
    "超级英雄": 9715,
    "机器人": 14544,
    "怪兽": 161791,
    "恐龙": 12616,
    "灾难": 10617,
    "人工智能": 310,
    "时间旅行": 4379,
    "赛博朋克": 12190,
    "后末日": 4458,
    "反乌托邦": 4565,
    "太空": 9882,
    "魔法": 2343,
    "鬼": 10292,
    "连环杀手": 10714,
    "有组织犯罪": 10291,
    "复仇": 9748,
    "间谍": 470,
    "武术": 779,
    "功夫": 780,
    "古装": 195013,
    "仙侠": 234890

    # --- 核心类型 (作为关键词使用) ---
    # "科幻": 878,
    # "奇幻": 170362,
    # "恐怖": 27,
    # "悬疑": 9648,
    # "惊悚": 53,
    # "犯罪": 80,
    # "动作": 28,
    # "冒险": 12,
    # "喜剧": 322268,
    # "爱情": 10749,
    # "战争": 10752,
    # "西部": 37,
    # "历史": 36,
    # "动画": 16,
    # "纪录片": 99,
    # "音乐": 10402,
    # "家庭": 10751
}

DAILY_THEME = {
    # --- 每日主题 ---
    "时间旅行": 4379,
    "外星人": 9951,
    "漫改": 9717,
    "超级英雄": 9715,
    "机器人": 14544,
    "怪兽": 161791,
    "丧尸": 12377,
    "灾难": 10617,
    "人工智能": 310,
    "赛博朋克": 12190,
    "漫威电影宇宙": 180547,
    "后末日": 4458,
    "吸血鬼": 3133,
    "反乌托邦": 4565,
    "太空": 9882,
    "魔法": 2343,
    "鬼": 10292,
    "连环杀手": 10714,
    "有组织犯罪": 10291,
    "复仇": 9748,
    "间谍": 470,
    "功夫": 780
}

def get_unified_rating(official_rating_str: str) -> str:
    """
    【V2 - 健壮版】
    根据 Emby 的 OfficialRating 字符串，返回统一后的分级。
    能正确处理带国家前缀 (us-R) 和不带前缀 (R) 的各种情况。
    """
    if not official_rating_str:
        return '未知'

    # 先转为小写，方便匹配
    rating_value = str(official_rating_str).lower()

    # 如果包含国家代码 (e.g., "us-r"), 则提取后面的部分
    if '-' in rating_value:
        # 这是一个小技巧，可以安全地处理 "us-r" 和 "pg-13"
        # 对于 "us-r", parts[-1] 是 "r"
        # 对于 "pg-13", parts[-1] 是 "13"
        # 但为了更准确，我们直接检查整个分割后的部分
        parts = rating_value.split('-', 1)
        if len(parts) > 1:
            rating_value = parts[1]

    # 直接在字典中查找处理后的值
    return RATING_MAP.get(rating_value, '未知')
# --- ★★★ 新增结束 ★★★ ---

_COUNTRY_SOURCE_DATA = {
    "China": {"chinese_name": "中国大陆", "abbr": "CN"},
    "Taiwan": {"chinese_name": "中国台湾", "abbr": "TW"},
    "Hong Kong": {"chinese_name": "中国香港", "abbr": "HK"},
    "United States of America": {"chinese_name": "美国", "abbr": "US"},
    "Japan": {"chinese_name": "日本", "abbr": "JP"},
    "South Korea": {"chinese_name": "韩国", "abbr": "KR"},
    "United Kingdom": {"chinese_name": "英国", "abbr": "GB"},
    "France": {"chinese_name": "法国", "abbr": "FR"},
    "Germany": {"chinese_name": "德国", "abbr": "DE"},
    "Canada": {"chinese_name": "加拿大", "abbr": "CA"},
    "India": {"chinese_name": "印度", "abbr": "IN"},
    "Italy": {"chinese_name": "意大利", "abbr": "IT"},
    "Spain": {"chinese_name": "西班牙", "abbr": "ES"},
    "Australia": {"chinese_name": "澳大利亚", "abbr": "AU"},
    "Russia": {"chinese_name": "俄罗斯", "abbr": "RU"},
    "Thailand": {"chinese_name": "泰国", "abbr": "TH"},
    "Sweden": {"chinese_name": "瑞典", "abbr": "SE"},
    "Denmark": {"chinese_name": "丹麦", "abbr": "DK"},
    "Mexico": {"chinese_name": "墨西哥", "abbr": "MX"},
    "Brazil": {"chinese_name": "巴西", "abbr": "BR"},
    "Argentina": {"chinese_name": "阿根廷", "abbr": "AR"},
    "Ireland": {"chinese_name": "爱尔兰", "abbr": "IE"},
    "New Zealand": {"chinese_name": "新西兰", "abbr": "NZ"},
    "Netherlands": {"chinese_name": "荷兰", "abbr": "NL"},
    "Singapore": {"chinese_name": "新加坡", "abbr": "SG"},
    "Belgium": {"chinese_name": "比利时", "abbr": "BE"}
}

# --- 国家/地区名称映射功能 (已重构) ---
_country_map_cache = None
def get_country_translation_map() -> dict:
    """
    从 _COUNTRY_SOURCE_DATA 构建并缓存国家/地区反向映射表。
    """
    global _country_map_cache
    if _country_map_cache is not None:
        return _country_map_cache

    try:
        reverse_map = {}
        for english_name, details in _COUNTRY_SOURCE_DATA.items():
            chinese_name = details.get('chinese_name')
            abbr = details.get('abbr')
            if chinese_name:
                reverse_map[english_name] = chinese_name
                if abbr:
                    reverse_map[abbr.lower()] = chinese_name
        
        _country_map_cache = reverse_map
        logger.trace(f"成功从代码中加载并缓存了 {len(reverse_map)} 条国家/地区映射。")
        return _country_map_cache

    except Exception as e:
        logger.error(f"从硬编码数据构建国家映射时出错: {e}。")
        _country_map_cache = {}
        return {}

def get_tmdb_country_options():
    """
    从 _COUNTRY_SOURCE_DATA 生成前端需要的国家/地区选项。
    """
    options = []
    # ★ 现在从常量读取数据
    for details in _COUNTRY_SOURCE_DATA.values():
        if details.get('chinese_name') and details.get('abbr'):
            options.append({
                "label": details['chinese_name'],
                "value": details['abbr']
            })
    
    return options

def translate_country_list(country_names_or_codes: list) -> list:
    """
    接收一个包含国家英文名或代码的列表，返回一个翻译后的中文名列表。
    """
    if not country_names_or_codes:
        return []
    
    translation_map = get_country_translation_map()
    
    if not translation_map:
        return country_names_or_codes

    translated_list = []
    for item in country_names_or_codes:
        translated = translation_map.get(item.lower(), translation_map.get(item, item))
        translated_list.append(translated)
        
    return list(dict.fromkeys(translated_list))

# --- 语言名称映射 ---
LANGUAGE_TRANSLATION_MAP = {
    "zh": "国语",
    "cn": "粤语",
    "en": "英语",
    "ja": "日语",
    "ko": "韩语",
    "fr": "法语",
    "de": "德语",
    "es": "西班牙语",
    "it": "意大利语",
    "ru": "俄语",
    "th": "泰语",
    "hi": "印地语",
    "pt": "葡萄牙语",
    "sv": "瑞典语",
    "da": "丹麦语",
    "nl": "荷兰语",
    "no": "挪威语",
    "fi": "芬兰语",
    "pl": "波兰语",
    "tr": "土耳其语",
    "ar": "阿拉伯语",
    "he": "希伯来语",
    "id": "印尼语",
    "ms": "马来语",
    "vi": "越南语",
    "cs": "捷克语",
    "hu": "匈牙利语",
    "ro": "罗马尼亚语",
    "el": "希腊语",
    "xx": "无语言"
}

def get_tmdb_language_options():
    """
    从硬编码的语言映射表中，生成前端需要的 [{label: '中文', value: '代码'}, ...] 格式。
    严格保持与 LANGUAGE_TRANSLATION_MAP 字典中定义一致的顺序。
    """
    options = [
        {"label": chinese_name, "value": code}
        for code, chinese_name in LANGUAGE_TRANSLATION_MAP.items()
    ]
    return options