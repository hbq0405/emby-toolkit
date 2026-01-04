# utils.py (最终智能匹配版)

import re
from typing import Optional
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
# 1. 统一的分级选项 (前端下拉框用)
UNIFIED_RATING_CATEGORIES = [
    '全年龄', '家长辅导', '青少年', '成人', '限制级', '未知'
]

# 2. 默认优先级策略 (如果数据库没配置，就用这个)
# ORIGIN 代表原产国，如果原产国没数据，按顺序找后面的
DEFAULT_RATING_PRIORITY = ["ORIGIN", "US", "HK", "TW", "JP", "KR", "GB"]

# 3. 默认分级映射表 (如果数据库没配置，就用这个)
# 格式: { 国家代码: [ { code: 原分级, label: 映射中文 }, ... ] }
DEFAULT_RATING_MAPPING = {
    "US": [
        {"code": "G", "label": "全年龄", "emby_value": 1},
        {"code": "TV-Y", "label": "全年龄", "emby_value": 1},
        {"code": "TV-G", "label": "全年龄", "emby_value": 1},
        {"code": "TV-Y7", "label": "家长辅导", "emby_value": 4},
        {"code": "PG", "label": "家长辅导", "emby_value": 5},
        {"code": "TV-PG", "label": "家长辅导", "emby_value": 5},
        {"code": "PG-13", "label": "青少年", "emby_value": 8},
        {"code": "TV-14", "label": "青少年", "emby_value": 8},
        {"code": "R", "label": "成人", "emby_value": 9},
        {"code": "TV-MA", "label": "成人", "emby_value": 9},
        {"code": "NC-17", "label": "限制级", "emby_value": 10},
        {"code": "NR", "label": "未知", "emby_value": 0},
        {"code": "Unrated", "label": "未知", "emby_value": 0}
    ],
    "JP": [
        {"code": "G", "label": "全年龄", "emby_value": 1},
        {"code": "PG12", "label": "家长辅导", "emby_value": 5},
        {"code": "R15+", "label": "成人", "emby_value": 9},
        {"code": "R18+", "label": "限制级", "emby_value": 10}
    ],
    "HK": [
        {"code": "I", "label": "全年龄", "emby_value": 1},
        {"code": "IIA", "label": "家长辅导", "emby_value": 5},
        {"code": "IIB", "label": "成人", "emby_value": 9}, 
        {"code": "III", "label": "限制级", "emby_value": 10}
    ],
    "TW": [
        {"code": "0+", "label": "全年龄", "emby_value": 1},
        {"code": "6+", "label": "家长辅导", "emby_value": 4},
        {"code": "12+", "label": "青少年", "emby_value": 8},
        {"code": "15+", "label": "成人", "emby_value": 9},
        {"code": "18+", "label": "限制级", "emby_value": 10}
    ],
    "KR": [
        {"code": "All", "label": "全年龄", "emby_value": 1},
        {"code": "12", "label": "家长辅导", "emby_value": 5},
        {"code": "15", "label": "青少年", "emby_value": 8},
        {"code": "18", "label": "成人", "emby_value": 9},
        {"code": "Restricted", "label": "限制级", "emby_value": 10}
    ],
    "GB": [
        {"code": "U", "label": "全年龄", "emby_value": 1},
        {"code": "PG", "label": "家长辅导", "emby_value": 5},
        {"code": "12", "label": "青少年", "emby_value": 8},
        {"code": "12A", "label": "青少年", "emby_value": 8},
        {"code": "15", "label": "成人", "emby_value": 9},
        {"code": "18", "label": "成人", "emby_value": 9},
        {"code": "R18", "label": "限制级", "emby_value": 10}
    ]
}

# --- 关键词预设表 ---
DEFAULT_KEYWORD_MAPPING = [
    {"label": "丧尸", "en": ["zombie"], "ids": [12377]},
    {"label": "二战", "en": ["world war ii"], "ids": [1956]},
    {"label": "吸血鬼", "en": ["vampire"], "ids": [3133]},
    {"label": "外星人", "en": ["alien"], "ids": [9951]},
    {"label": "漫改", "en": ["based on comic"], "ids": [9717]},
    {"label": "超级英雄", "en": ["superhero"], "ids": [9715]},
    {"label": "机器人", "en": ["robot"], "ids": [14544]},
    {"label": "怪兽", "en": ["monster"], "ids": [161791]},
    {"label": "恐龙", "en": ["dinosaur"], "ids": [12616]},
    {"label": "灾难", "en": ["disaster"], "ids": [10617]},
    {"label": "人工智能", "en": ["artificial intelligence (a.i.)"], "ids": [310]},
    {"label": "时间旅行", "en": ["time travel"], "ids": [4379]},
    {"label": "赛博朋克", "en": ["cyberpunk"], "ids": [12190]},
    {"label": "后末日", "en": ["post-apocalyptic future"], "ids": [4458]},
    {"label": "反乌托邦", "en": ["dystopia"], "ids": [4565]},
    {"label": "太空", "en": ["space"], "ids": [9882]},
    {"label": "魔法", "en": ["magic"], "ids": [2343]},
    {"label": "鬼", "en": ["ghost"], "ids": [10292]},
    {"label": "连环杀手", "en": ["serial killer"], "ids": [10714]},
    {"label": "复仇", "en": ["revenge"], "ids": [9748]},
    {"label": "间谍", "en": ["spy"], "ids": [470]},
    {"label": "武术", "en": ["martial arts"], "ids": [779]},
    {"label": "功夫", "en": ["kung fu"], "ids": [780]},
    {"label": "古装", "en": ["costume drama"], "ids": [195013]},
    {"label": "仙侠", "en": ["xianxia"], "ids": [234890]},
    {"label": "恐怖", "en": ["horror", "clown", "macabre"], "ids": ["315058", "3199", "162810"]},
    {"label": "惊悚", "en": ["thriller", "gruesome"], "ids": ["10526", "186416"]},
]

# --- 工作室预设表 ---
DEFAULT_STUDIO_MAPPING = [
    # --- 全球流媒体 (Network IDs) ---
    {"label": "网飞", "en": ["Netflix"], "ids": [213]},
    {"label": "HBO", "en": ["HBO"], "ids": [49, 3268]},
    {"label": "Disney+", "en": ["Disney+"], "ids": [2739]},
    {"label": "Apple TV+", "en": ["Apple TV+"], "ids": [2552]},
    {"label": "Amazon", "en": ["Amazon Prime Video"], "ids": [1024]},
    {"label": "Hulu", "en": ["Hulu"], "ids": [453]},

    # --- 国内平台 (Network IDs) ---
    {"label": "腾讯", "en": ["Tencent Video"], "ids": [2007]},
    {"label": "爱奇艺", "en": ["iQiyi"], "ids": [1330]},
    {"label": "优酷", "en": ["Youku"], "ids": [1419]},
    {"label": "芒果", "en": ["Mango TV", "Hunan TV"], "ids": [1631, 952]},
    {"label": "央视", "en": ["CCTV-8", "CCTV-1"], "ids": [521, 1363]}, 
    {"label": "浙江卫视", "en": ["Zhejiang Television"], "ids": [989]},
    {"label": "江苏卫视", "en": ["Jiangsu Television"], "ids": [1055]},
    {"label": "TVB", "en": ["TVB Jade", "Television Broadcasts Limited"], "ids": [48, 79261]},
    # --- 传统制作公司 (Company IDs) ---
    # 这些通常用于电影，或者作为电视剧的制作方（非播出平台）
    {"label": "漫威", "en": ["Marvel Studios"], "ids": [420]},
    {"label": "DC", "en": ["DC"], "ids": [128064, 9993]},
    {"label": "正午阳光", "en": ["Daylight Entertainment"], "ids": [74209]},
    {"label": "A24", "en": ["A24"], "ids": [41077]},
]

# --- 国家预设表 ---
DEFAULT_COUNTRY_MAPPING = [
    {"label": "中国大陆", "value": "CN", "aliases": ["China", "PRC"]},
    {"label": "中国香港", "value": "HK", "aliases": ["Hong Kong"]},
    {"label": "中国台湾", "value": "TW", "aliases": ["Taiwan"]},
    {"label": "美国", "value": "US", "aliases": ["United States of America", "USA"]},
    {"label": "英国", "value": "GB", "aliases": ["United Kingdom", "UK"]},
    {"label": "日本", "value": "JP", "aliases": ["Japan"]},
    {"label": "韩国", "value": "KR", "aliases": ["South Korea", "Korea, Republic of"]},
    {"label": "法国", "value": "FR", "aliases": ["France"]},
    {"label": "德国", "value": "DE", "aliases": ["Germany"]},
    {"label": "意大利", "value": "IT", "aliases": ["Italy"]},
    {"label": "西班牙", "value": "ES", "aliases": ["Spain"]},
    {"label": "加拿大", "value": "CA", "aliases": ["Canada"]},
    {"label": "澳大利亚", "value": "AU", "aliases": ["Australia"]},
    {"label": "印度", "value": "IN", "aliases": ["India"]},
    {"label": "俄罗斯", "value": "RU", "aliases": ["Russia"]},
    {"label": "泰国", "value": "TH", "aliases": ["Thailand"]},
    {"label": "瑞典", "value": "SE", "aliases": ["Sweden"]},
    {"label": "丹麦", "value": "DK", "aliases": ["Denmark"]},
    {"label": "挪威", "value": "NO", "aliases": ["Norway"]},
    {"label": "荷兰", "value": "NL", "aliases": ["Netherlands"]},
    {"label": "巴西", "value": "BR", "aliases": ["Brazil"]},
    {"label": "墨西哥", "value": "MX", "aliases": ["Mexico"]},
    {"label": "阿根廷", "value": "AR", "aliases": ["Argentina"]},
    {"label": "新西兰", "value": "NZ", "aliases": ["New Zealand"]},
    {"label": "爱尔兰", "value": "IE", "aliases": ["Ireland"]},
    {"label": "新加坡", "value": "SG", "aliases": ["Singapore"]},
    {"label": "比利时", "value": "BE", "aliases": ["Belgium"]},
    {"label": "芬兰", "value": "FI", "aliases": ["Finland"]},
    {"label": "波兰", "value": "PL", "aliases": ["Poland"]},
]

# --- 语言预设表 ---
DEFAULT_LANGUAGE_MAPPING = [
    {"label": "国语", "value": "zh"},
    {"label": "粤语", "value": "cn"}, 
    {"label": "英语", "value": "en"},
    {"label": "日语", "value": "ja"},
    {"label": "韩语", "value": "ko"},
    {"label": "法语", "value": "fr"},
    {"label": "德语", "value": "de"},
    {"label": "西班牙语", "value": "es"},
    {"label": "意大利语", "value": "it"},
    {"label": "俄语", "value": "ru"},
    {"label": "泰语", "value": "th"},
    {"label": "印地语", "value": "hi"},
    {"label": "葡萄牙语", "value": "pt"},
    {"label": "阿拉伯语", "value": "ar"},
    {"label": "拉丁语", "value": "la"},
    {"label": "无语言", "value": "xx"},
]

# --- ★★★ Emby 兼容 JSON 骨架模板 (V1 - 电影版) ★★★ ---
# 用于生成 all.json (电影)
MOVIE_SKELETON_TEMPLATE = {
  "adult": False,
  "backdrop_path": "",
  "belongs_to_collection": None, # { "id": 0, "name": "", "poster_path": "", "backdrop_path": "" }
  "budget": 0,
  "mpaa": "",          # Emby/Kodi 常用兼容字段
  "certification": "", # TMDb 常用字段
  "genres": [], # [ { "id": 0, "name": "" } ]
  "homepage": "",
  "id": 0,
  "imdb_id": "",
  "original_language": "",
  "original_title": "",
  "overview": "",
  "popularity": 0.0,
  "poster_path": "",
  "production_companies": [], # [ { "id": 0, "name": "", "origin_country": "", "logo_path": "" } ]
  "production_countries": [], # [ { "iso_3166_1": "", "name": "" } ]
  "release_date": "",
  "revenue": 0,
  "runtime": 0,
  "spoken_languages": [], # [ { "iso_639_1": "", "name": "", "english_name": "" } ]
  "status": "",
  "tagline": "",
  "title": "",
  "video": False,
  "vote_average": 0.0,
  "vote_count": 0,
  # ★ Emby 特有结构：演员表
  "casts": {
    "cast": [], # [ { "id": 0, "name": "", "character": "", "profile_path": "", "order": 0, ... } ]
    "crew": []  # [ { "id": 0, "name": "", "job": "", "department": "", ... } ]
  },
  # ★ Emby 特有结构：分级信息
  "releases": {
    "countries": [] # [ { "iso_3166_1": "US", "certification": "PG-13", "release_date": "" } ]
  },
  # ★ Emby 特有结构：关键词
  "keywords": {
    "keywords": [] # [ { "id": 0, "name": "" } ]
  },
  # ★ 属于合集
  "belongs_to_collection": None,
  # ★ Emby 特有结构：预告片
  "trailers": {
    "quicktime": [],
    "youtube": [] # [ { "name": "", "size": "", "source": "", "type": "" } ]
  }
}

# 用于生成 series.json (电视剧)
SERIES_SKELETON_TEMPLATE = {
  "backdrop_path": "",
  "created_by": [], # [ { "id": 0, "name": "", "profile_path": "" } ]
  "episode_run_time": [], # [ 60 ]
  "first_air_date": "",
  "genres": [], # [ { "id": 0, "name": "" } ]
  "homepage": "",
  "id": 0,
  "in_production": False,
  "languages": [], # [ "en" ]
  "last_air_date": "",
  "name": "",
  "networks": [], # [ { "id": 0, "name": "" } ]
  "number_of_episodes": 0,
  "number_of_seasons": 0,
  "origin_country": [], # [ "US" ]
  "original_language": "",
  "original_name": "",
  "overview": "",
  "popularity": 0.0,
  "poster_path": "",
  "production_companies": [], # [ { "id": 0, "name": "", "origin_country": "", "logo_path": "" } ]
  "status": "",
  "tagline": "",
  "type": "",
  "vote_average": 0.0,
  "vote_count": 0,
  # ★ Emby 特有结构：演员表 (电视剧层级通常只包含常驻演员)
  "casts": {
    "cast": [], 
    "crew": []
  },
  # ★ Emby 特有结构：分级信息
  "content_ratings": {
    "results": [] # [ { "iso_3166_1": "US", "rating": "TV-MA" } ]
  },
  # ★ Emby 特有结构：关键词
  "keywords": {
    "results": [] # [ { "id": 0, "name": "" } ] (注意：剧集关键词通常在 results 里，不同于电影的 keywords)
  },
  # ★ Emby 特有结构：外部ID
  "external_ids": {
    "imdb_id": "",
    "tvdb_id": 0
  },
  # ★ Emby 特有结构：预告片
  "videos": {
    "results": [] 
  }
}

# 用于生成 season-X.json (季)
SEASON_SKELETON_TEMPLATE = {
  "air_date": "",
  "name": "",
  "overview": "",
  "id": 0,
  "poster_path": "",
  "season_number": 0,
  "vote_average": 0.0,
  "credits": {
    "cast": [],
    "crew": []
  },
  "external_ids": {
    "tvdb_id": 0
  },
  "videos": {
    "results": []
  }
}

# 用于生成 season-X-episode-Y.json (分集)
EPISODE_SKELETON_TEMPLATE = {
  "air_date": "",
  "episode_number": 0,
  "name": "",
  "overview": "",
  "id": 0,
  "production_code": "",
  "season_number": 0,
  "still_path": "",
  "vote_average": 0.0,
  "vote_count": 0,
  "external_ids": {
    "imdb_id": "",
    "tvdb_id": 0
  },
  "credits": {
    "cast": [], # 分集特约演员 (Guest Stars) 通常也混在这里或单独处理
    "crew": [],
    "guest_stars": []
  },
  "videos": {
    "results": []
  }
}