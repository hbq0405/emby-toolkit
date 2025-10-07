# utils.py (最终智能匹配版)

import re
import os
import psycopg2
from datetime import datetime
from typing import Optional, List, Dict
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

# 尝试导入 translators
try:
    from translators import translate_text as translators_translate_text
    TRANSLATORS_LIB_AVAILABLE = True
except ImportError:
    TRANSLATORS_LIB_AVAILABLE = False
    def translators_translate_text(*args, **kwargs):
        raise NotImplementedError("translators 库未安装")

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
    """根据提供者、标题和年份生成搜索URL。"""
    if not title:
        return ""
    
    search_term = f"{title} {year}" if year else title
    encoded_term = quote_plus(search_term)

    if provider == 'baike':
        # ▼▼▼ 核心修改：返回百度百科的搜索链接 ▼▼▼
        return f"https://baike.baidu.com/search/word?word={encoded_term}"
    elif provider == 'wikipedia':
        return f"https://www.google.com/search?q={encoded_term}+site%3Azh.wikipedia.org"
    else:
        # 默认回退到 Google
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

# --- ★★★ 获取覆盖缓存路径 ★★★ ---
def get_override_path_for_item(item_type: str, tmdb_id: str, config: dict) -> str | None:
    """
    【修复版】根据类型和ID返回 override 目录的路径。
    此函数现在依赖于传入的 config 字典，而不是全局变量。
    """
    # 1. ★★★ 从传入的 config 中获取 local_data_path ★★★
    local_data_path = config.get("local_data_path")

    # 2. ★★★ 使用 local_data_path 进行检查 ★★★
    if not local_data_path or not tmdb_id:
        # 如果 local_data_path 没有在配置中提供，打印一条警告
        if not local_data_path:
            logger.warning("get_override_path_for_item: 配置中缺少 'local_data_path'。")
        return None

    # 3. ★★★ 使用 local_data_path 构建基础路径 ★★★
    base_path = os.path.join(local_data_path, "override")

    # 确保 item_type 是字符串，以防万一
    item_type_str = str(item_type or '').lower()

    if "movie" in item_type_str:
        # 假设你的电影目录是 tmdb-movies2
        return os.path.join(base_path, "tmdb-movies2", str(tmdb_id))
    elif "series" in item_type_str:
        return os.path.join(base_path, "tmdb-tv", str(tmdb_id))

    logger.warning(f"未知的媒体类型 '{item_type}'，无法确定 override 路径。")
    return None


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

# --- 国家/地区名称映射功能 ---
_country_map_cache = None
def get_country_translation_map() -> dict:
    """
    【V-Hardcoded - 硬编码最终版】
    直接在代码中定义并缓存一个权威的国家/地区反向映射表。
    """
    global _country_map_cache
    if _country_map_cache is not None:
        return _country_map_cache

    try:
        # 直接在代码中定义数据源
        source_data = {
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

        reverse_map = {}
        for english_name, details in source_data.items():
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

_country_reverse_map_cache = None

def get_country_reverse_lookup_map() -> dict:
    """
    【新增】从硬编码数据中，创建一个 "英文全称 -> 两字母代码" 的反向查找表。
    """
    global _country_reverse_map_cache
    if _country_reverse_map_cache is not None:
        return _country_reverse_map_cache

    # 复用 get_country_translation_map 中定义的源数据
    source_data = {
        "Hong Kong": {"chinese_name": "香港", "abbr": "HK"},
        "United States of America": {"chinese_name": "美国", "abbr": "US"},
        "Japan": {"chinese_name": "日本", "abbr": "JP"},
        "United Kingdom": {"chinese_name": "英国", "abbr": "GB"},
        "France": {"chinese_name": "法国", "abbr": "FR"},
        "South Korea": {"chinese_name": "韩国", "abbr": "KR"},
        "Germany": {"chinese_name": "德国", "abbr": "DE"},
        "Canada": {"chinese_name": "加拿大", "abbr": "CA"},
        "India": {"chinese_name": "印度", "abbr": "IN"},
        "Italy": {"chinese_name": "意大利", "abbr": "IT"},
        "Spain": {"chinese_name": "西班牙", "abbr": "ES"},
        "Australia": {"chinese_name": "澳大利亚", "abbr": "AU"},
        "China": {"chinese_name": "中国大陆", "abbr": "CN"},
        "Taiwan": {"chinese_name": "中国台湾", "abbr": "TW"},
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
        "Belgium": {"chinese_name": "比利时", "abbr": "BE"}
    }
    
    reverse_map = {
        english_name.lower(): details.get('abbr')
        for english_name, details in source_data.items()
        if details.get('abbr')
    }
    
    _country_reverse_map_cache = reverse_map
    logger.trace(f"成功构建了 {len(reverse_map)} 条国家英文名到代码的反向映射。")
    return _country_reverse_map_cache

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

def get_tmdb_country_options():
    """
    【复用版】
    复用 get_country_translation_map 中的硬编码数据源，
    生成前端需要的 [{label: '中文', value: '代码'}, ...] 格式。
    """
    # 直接从你的函数中复制这份权威的数据源
    source_data = {
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

    options = []
    for details in source_data.values():
        # 确保中文名和缩写都存在
        if details.get('chinese_name') and details.get('abbr'):
            options.append({
                "label": details['chinese_name'],
                "value": details['abbr']
            })
    
    # 按中文标签的拼音排序，对用户更友好
    return sorted(options, key=lambda x: x['label'])
