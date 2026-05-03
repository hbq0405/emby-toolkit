# utils.py (最终智能匹配版)

import re
from typing import Optional, Tuple, Any
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

    # 处理中外对照：“中文+英文” 或 “英文+中文” 形式，只保留中文部分
    if re.search(r'[\u4e00-\u9fa5]', name) and re.search(r'[a-zA-Z]', name):
        # 1. 优先尝试按常见分隔符 (/, |) 拆分 (例如 "ShenWang/王忱")
        if '/' in name or '|' in name:
            parts = re.split(r'[/|]', name)
            for part in parts:
                # 找到包含中文的那一部分
                if re.search(r'[\u4e00-\u9fa5]', part):
                    # 提取出中文部分后，剔除可能残留的英文字母，并清理首尾空格
                    return re.sub(r'[a-zA-Z]', '', part).strip()
        
        # 2. 如果没有明显分隔符 (例如 "ShenWang王忱" 或 "王忱 ShenWang")
        # 直接暴力剔除所有英文字母，并压缩多余的空格
        clean_name = re.sub(r'[a-zA-Z]', '', name)
        return re.sub(r'\s+', ' ', clean_name).strip()

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

# 类型映射
GENRE_TRANSLATION_PATCH = {
    "Sci-Fi & Fantasy": "科幻奇幻",
    "War & Politics": "战争政治",
    # 以后如果发现其他未翻译的，也可以加在这里
}

# --- ★★★ 统一分级映射功能 (V2 - 健壮版) ★★★ ---
# 1. 统一的分级选项 (前端下拉框用)
UNIFIED_RATING_CATEGORIES = [
    '全年龄', '家长辅导', '青少年', '限制级', '18禁', '成人', '未知'
]

# 2. 默认优先级策略 (如果数据库没配置，就用这个)
# ORIGIN 代表原产国，如果原产国没数据，按顺序找后面的
DEFAULT_RATING_PRIORITY = ["ORIGIN", "US", "HK", "TW", "JP", "KR", "GB", "ES", "DE"]

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
        {"code": "R", "label": "限制级", "emby_value": 9},
        {"code": "TV-MA", "label": "限制级", "emby_value": 9},
        {"code": "NC-17", "label": "18禁", "emby_value": 10},
        {"code": "XXX", "label": "成人", "emby_value": 15},
        {"code": "NR", "label": "未知", "emby_value": 0},
        {"code": "Unrated", "label": "未知", "emby_value": 0}
    ],
    "JP": [
        {"code": "G", "label": "全年龄", "emby_value": 1},
        {"code": "PG12", "label": "家长辅导", "emby_value": 5},
        {"code": "R15+", "label": "限制级", "emby_value": 9},
        {"code": "R18+", "label": "18禁", "emby_value": 10},
        # --- 兼容旧数据/数字录入 ---
        {"code": "12", "label": "家长辅导", "emby_value": 5},
        {"code": "15", "label": "限制级", "emby_value": 9},
        {"code": "18", "label": "18禁", "emby_value": 10}
    ],
    "HK": [
        {"code": "I", "label": "全年龄", "emby_value": 1},
        {"code": "IIA", "label": "家长辅导", "emby_value": 5},
        {"code": "IIB", "label": "限制级", "emby_value": 9}, 
        {"code": "III", "label": "18禁", "emby_value": 10},
        # --- 兼容 TMDb 历史遗留数字录入 ---
        {"code": "15", "label": "限制级", "emby_value": 9}, # 对应 IIB
        {"code": "18", "label": "18禁", "emby_value": 10}  # 对应 III
    ],
    "TW": [
        {"code": "0+", "label": "全年龄", "emby_value": 1},
        {"code": "6+", "label": "家长辅导", "emby_value": 5},
        {"code": "12+", "label": "青少年", "emby_value": 8},
        {"code": "15+", "label": "限制级", "emby_value": 9},
        {"code": "18+", "label": "18禁", "emby_value": 10},
        # --- 兼容无“+”号的数字录入 ---
        {"code": "0", "label": "全年龄", "emby_value": 1},
        {"code": "6", "label": "家长辅导", "emby_value": 5},
        {"code": "12", "label": "青少年", "emby_value": 8},
        {"code": "15", "label": "限制级", "emby_value": 9},
        {"code": "18", "label": "18禁", "emby_value": 10}
    ],
    "KR": [
        {"code": "All", "label": "全年龄", "emby_value": 1},
        {"code": "12", "label": "家长辅导", "emby_value": 5},
        {"code": "15", "label": "青少年", "emby_value": 8},
        {"code": "19", "label": "限制级", "emby_value": 9},
        {"code": "Restricted Screening", "label": "18禁", "emby_value": 10},
        # --- 兼容韩国有时会录入 18 而非 19 的情况 ---
        {"code": "18", "label": "限制级", "emby_value": 9}
    ],
    "GB": [
        {"code": "U", "label": "全年龄", "emby_value": 1},
        {"code": "PG", "label": "家长辅导", "emby_value": 5},
        {"code": "12", "label": "青少年", "emby_value": 8},
        {"code": "12A", "label": "青少年", "emby_value": 8},
        {"code": "15", "label": "限制级", "emby_value": 9},
        {"code": "18", "label": "限制级", "emby_value": 9},
        {"code": "R18", "label": "18禁", "emby_value": 10}
    ],
    "ES": [
        {"code": "TP", "label": "全年龄", "emby_value": 1},
        {"code": "7", "label": "家长辅导", "emby_value": 5},
        {"code": "12", "label": "青少年", "emby_value": 8},
        {"code": "16", "label": "限制级", "emby_value": 9},
        {"code": "18", "label": "18禁", "emby_value": 10}
    ],
    "DE": [
        {"code": "0", "label": "全年龄", "emby_value": 1},
        {"code": "6", "label": "家长辅导", "emby_value": 5},
        {"code": "12", "label": "青少年", "emby_value": 8},
        {"code": "16", "label": "限制级", "emby_value": 9},
        {"code": "18", "label": "18禁", "emby_value": 10}   
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
    {"label": "恐怖", "en": ["horror", "clown", "macabre"], "ids": [315058, 3199, 162810]},
    {"label": "惊悚", "en": ["thriller", "gruesome"], "ids": [10526, 186416]},
    {"label": "赛车", "en": ["car race", "street-race"], "ids": [830, 9666]},
    {"label": "怪物", "en": ["cmonster"], "ids": [1299]},
    {"label": "特工", "en": ["secret agent"], "ids": [4289]},
]

# --- 工作室预设表 ---
DEFAULT_STUDIO_MAPPING = [
    # --- 国内平台 (纯 Network) ---
    {"label": "CCTV-1", "en": ["CCTV-1"], "network_ids": [1363]}, 
    {"label": "CCTV-8", "en": ["CCTV-8"], "network_ids": [521]},
    {"label": "湖南卫视", "en": ["Hunan TV"], "network_ids": [952]},
    {"label": "浙江卫视", "en": ["Zhejiang Television"], "network_ids": [989]},
    {"label": "江苏卫视", "en": ["Jiangsu Television"], "network_ids": [1055]},
    {"label": "北京卫视", "en": ["Beijing Television"], "network_ids": [455]},
    {"label": "东方卫视", "en": ["Dragon Television"], "network_ids": [1056]},
    {"label": "腾讯视频", "en": ["Tencent Video"], "network_ids": [2007]},
    {"label": "爱奇艺", "en": ["iQiyi"], "network_ids": [1330]},
    {"label": "优酷", "en": ["Youku"], "network_ids": [1419]},
    {"label": "芒果TV", "en": ["Mango TV"], "network_ids": [1631]},
    {"label": "哔哩哔哩", "en": ["Bilibili"], "network_ids": [1605]},
    {"label": "TVB", "en": ["TVB Jade", "Television Broadcasts Limited"], "network_ids": [48, 79261]},

    # --- 全球流媒体/电视网 (Network + Company) ---
    # 这些巨头通常既作为播出平台(Network)，也作为制作公司(Company)存在
    {"label": "网飞", "en": ["Netflix"], "network_ids": [213], "company_ids": [178464]},
    {"label": "HBO", "en": ["HBO"], "network_ids": [49], "company_ids": [3268]},
    {"label": "迪士尼", "en": ["Disney+", "Walt Disney Pictures"], "network_ids": [2739], "company_ids": [2]},
    {"label": "苹果TV", "en": ["Apple TV+"], "network_ids": [2552], "company_ids": [108568]},
    {"label": "亚马逊", "en": ["Amazon Prime Video"], "network_ids": [1024], "company_ids": [20555]},
    {"label": "Hulu", "en": ["Hulu"], "network_ids": [453], "company_ids": [15365]},
    {"label": "正午阳光", "en": ["Daylight Entertainment"], "network_ids": [148869], "company_ids": [148869]},

    # --- 传统制作公司 (纯 Company) ---
    {"label": "二十世纪影业", "en": ["20th century fox"], "company_ids": [25]},
    {"label": "康斯坦丁影业", "en": ["Constantin Film"], "company_ids": [47]},
    {"label": "派拉蒙", "en": ["Paramount Pictures"], "company_ids": [4]},
    {"label": "华纳兄弟", "en": ["Warner Bros. Pictures"], "company_ids": [174]},
    {"label": "环球影业", "en": ["Universal Pictures"], "company_ids": [33]},
    {"label": "哥伦比亚影业", "en": ["Columbia Pictures"], "company_ids": [5]},
    {"label": "米高梅", "en": ["Metro-Goldwyn-Mayer"], "company_ids": [21]},
    {"label": "狮门影业", "en": ["Lionsgate"], "company_ids": [1632]}, 
    {"label": "传奇影业", "en": ["Legendary Pictures", "Legendary Entertainment"], "company_ids": [923]},
    {"label": "试金石影业", "en": ["Touchstone Pictures"], "company_ids": [9195]},
    {"label": "漫威", "en": ["Marvel Studios", "Marvel Entertainment"], "company_ids": [420, 7505]},
    {"label": "DC", "en": ["DC"], "company_ids": [128064, 9993]},
    {"label": "皮克斯", "en": ["Pixar"], "company_ids": [3]},
    {"label": "梦工厂", "en": ["DreamWorks Animation", "DreamWorks"], "company_ids": [521]},
    {"label": "吉卜力", "en": ["Studio Ghibli"], "company_ids": [10342]},
    {"label": "中国电影集团", "en": ["China Film Group"], "company_ids": [14714]},
    {"label": "登峰国际", "en": ["DF Pictures"], "company_ids": [65442]},
    {"label": "光线影业", "en": ["Beijing Enlight Pictures"], "company_ids": [17818]},
    {"label": "万达影业", "en": ["Wanda Pictures"], "company_ids": [78952]},
    {"label": "博纳影业", "en": ["Bonanza Pictures"], "company_ids": [30148]},
    {"label": "阿里影业", "en": ["Alibaba Pictures Group"], "company_ids": [69484]},
    {"label": "上影", "en": ["Shanghai Film Group"], "company_ids": [3407]},
    {"label": "华谊兄弟", "en": ["Huayi Brothers"], "company_ids": [76634]},
    {"label": "寰亚电影", "en": ["Media Asia Films"], "company_ids": [5552]},
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

# --- 音视频流/字幕流特色标签映射 ---
# 用于识别并标准化 DYSY、CCTV、上译、公映 等特色标签
DEFAULT_STREAM_FEATURE_MAPPING = [
    {
        "label": "上译",  # 统一标准化为“上译”
        "types": ["Audio", "Subtitle"],
        "patterns": [
            r"(?<![A-Za-z0-9])SY(?![A-Za-z0-9])",
            r"(?<![A-Za-z0-9])CYSY(?![A-Za-z0-9])",
            r"(?<![A-Za-z0-9])DYSY(?![A-Za-z0-9])",
            r"(?<![A-Za-z0-9])GYSY(?![A-Za-z0-9])",
            r"(?<![A-Za-z0-9])CH-DYSY(?![A-Za-z0-9])",
            r"(?<![A-Za-z0-9])GP-DYSY(?![A-Za-z0-9])",
            r"上译",
            r"东影上译",
            r"泰盛上译",
            r"上海电影译制",
            r"上海电影配音",
            r"上海译制",
            r"上海配音",
            r"公映上译",
            r"上譯"
        ],
    },
    {
        "label": "公映",
        "types": ["Audio", "Subtitle"],
        "patterns": [
            r"(?<![A-Za-z0-9])GY(?![A-Za-z0-9])",
            r"公映",
            r"院线配音",
            r"影院版"
        ],
    },
    {
        "label": "长译",
        "types": ["Audio", "Subtitle"],
        "patterns": [
            r"(?<![A-Za-z0-9])CY(?![A-Za-z0-9])",
            r"(?<![A-Za-z0-9])GYCY(?![A-Za-z0-9])",
            r"长译",
            r"长春电影"
        ],
    },
    {
        "label": "京译",
        "types": ["Audio", "Subtitle"],
        "patterns": [
            r"(?<![A-Za-z0-9])JY(?![A-Za-z0-9])",
            r"(?<![A-Za-z0-9])GYJY(?![A-Za-z0-9])",
            r"京译",
            r"中影配音",
            r"北京电影"
        ],
    },
    {
        "label": "八一",
        "types": ["Audio", "Subtitle"],
        "patterns": [
            r"八一",  
        ],
    },
    {
        "label": "央视",
        "types": ["Audio", "Subtitle"],
        "patterns": [
            r"(?<![A-Za-z0-9])CCTV(?![A-Za-z0-9])",
            r"(?<![A-Za-z0-9])GP-CCTV(?![A-Za-z0-9])",
            r"央视",
            r"CCTV6"
        ],
    },
    {
        "label": "台配",
        "types": ["Audio", "Subtitle"],
        "patterns": [
            r"台配",
            r"台湾配音"
        ],
    },
    {
    "label": "台湾",
    "types": ["Subtitle"],
    "patterns": [
        "(?<![A-Za-z0-9])TW(?![A-Za-z0-9])",
        "台灣",
        "臺灣",
        "台湾",
        "台配",
        "台灣配音",
        "臺灣配音",
        "台湾配音"
    ]
    },
    {
    "label": "香港",
    "types": ["Subtitle"],
    "patterns": [
        "(?<![A-Za-z0-9])HK(?![A-Za-z0-9])",
        "港配",
        "香港配音"
    ]
    },
    {
        "label": "特效",
        "types": ["Subtitle"],
        "patterns": [
            r"(?<![A-Za-z0-9])TX(?![A-Za-z0-9])",
            r"特效",
        ],
    },
    {
        "label": "拉美",
        "types": ["Subtitle", "Audio"],
        "patterns": [
            r"\bLatin\s*America\b",
            r"\bLATAM\b",
            r"拉美",
        ],
    },
    {
        "label": "巴西",
        "types": ["Subtitle", "Audio"],
        "patterns": [
            r"\bBrazil\b",
            r"\bBrasil\b",
            r"\bBrazilian\b",
            r"\bBrasilian\b",
            r"巴西",
        ],
    },
    {
        "label": "听障",
        "types": ["Subtitle"],
        "patterns": [
            r"(?<![A-Za-z0-9])SDH(?![A-Za-z0-9])",
            r"(?<![A-Za-z0-9])CC(?![A-Za-z0-9])",
            r"hearing impaired",
            r"hard of hearing",
            r"听障",
        ],
    },
    {
        "label": "导评",
        "types": ["Audio", "Subtitle"],
        "patterns": [
            r"Director'?s Commentary",
            r"Audio Commentary",
            r"Commentary",
            r"导评",
        ],
    },
]

# --- 音轨/字幕无意义压制组/字幕组过滤名单 ---
# 只要出现在这里的词，都会从音轨和字幕的标题中被无情抹除
STREAM_TITLE_GARBAGE_FILTER = [
    "麦哈", "说一不二", "人人字幕组", "人人影视", "远鉴字幕组", "衣柜字幕组", 
    "霸王龙压制组", "字幕组", "压制组", "手抄", "调轴", "精校", "原创", "校对", 
    "后期", "翻译", "制作", "发布", "团队", "组", "字幕", "配音", "合金弹头",
    "山茶树", "木木"
]

def clean_stream_garbage_words(text: str) -> str:
    """
    清理音轨/字幕标题中的无意义压制组、字幕组等干扰词汇。
    """
    if not text:
        return ""
    for garbage in STREAM_TITLE_GARBAGE_FILTER:
        text = text.replace(garbage, "")
    return text.strip()

# --- 语言预设表 ---
DEFAULT_LANGUAGE_MAPPING = [
    {"label": "国语", "value": "zh", "aliases": ["chi", "zho", "zh", "chs", "zh-cn", "zh-sg", "zh-hans", "cmn", "mandarin", "guo", "guoyu", "国语", "普通话", "中文", "简体", "简中"]},
    {"label": "粤语", "value": "cn", "aliases": ["yue", "cht", "cn", "zh-hk", "zh-tw", "zh-hant", "hk", "tw", "cantonese", "粤语", "繁体", "繁中", "粤配", "港配", "粤语配音", "广东话"]},

    {"label": "英语", "value": "en", "aliases": ["eng", "en", "english", "英语", "英文"]},
    {"label": "日语", "value": "ja", "aliases": ["jpn", "ja", "japanese", "日语", "日文"]},
    {"label": "韩语", "value": "ko", "aliases": ["kor", "ko", "korean", "韩语", "韩文"]},
    {"label": "法语", "value": "fr", "aliases": ["fre", "fra", "fr", "french", "法语", "法文"]},
    {"label": "德语", "value": "de", "aliases": ["ger", "deu", "de", "german", "Deutsch", "德语", "德文"]},
    {"label": "西班牙语", "value": "es", "aliases": ["spa", "es", "spanish", "西班牙语", "西班牙文"]},
    {"label": "意大利语", "value": "it", "aliases": ["ita", "it", "italian", "意大利语", "意大利文"]},
    {"label": "俄语", "value": "ru", "aliases": ["rus", "ru", "russian", "俄语", "俄文"]},
    {"label": "泰语", "value": "th", "aliases": ["tha", "th", "thai", "泰语", "泰文"]},
    {"label": "印地语", "value": "hi", "aliases": ["hin", "hi", "hindi", "印地语", "印度语"]},
    {"label": "葡萄牙语", "value": "pt", "aliases": ["por", "pt", "portuguese", "葡萄牙语", "葡萄牙文"]},
    {"label": "阿拉伯语", "value": "ar", "aliases": ["ara", "ar", "arabic", "阿拉伯语", "阿拉伯文"]},

    # --- Netflix 等多语字幕常见补充 ---
    {"label": "加泰罗尼亚语", "value": "ca", "aliases": ["cat", "CAT", "ca", "catalan", "català", "加泰罗尼亚语", "加泰隆语"]},
    {"label": "捷克语", "value": "cs", "aliases": ["cze", "CZE", "ces", "cs", "czech", "捷克语", "捷克文"]},
    {"label": "丹麦语", "value": "da", "aliases": ["dan", "DAN", "da", "danish", "丹麦语", "丹麦文"]},
    {"label": "希腊语", "value": "el", "aliases": ["gre", "GRE", "ell", "el", "greek", "希腊语", "希腊文"]},
    {"label": "巴斯克语", "value": "eu", "aliases": ["baq", "BAQ", "eus", "eu", "basque", "巴斯克语", "巴斯克文"]},
    {"label": "芬兰语", "value": "fi", "aliases": ["fin", "FIN", "fi", "finnish", "芬兰语", "芬兰文"]},
    {"label": "菲律宾语", "value": "fil", "aliases": ["fil", "FIL", "tl", "tag", "tagalog", "filipino", "菲律宾语", "他加禄语"]},
    {"label": "加利西亚语", "value": "gl", "aliases": ["glg", "GLG", "gl", "galician", "加利西亚语", "加里西亚语"]},
    {"label": "希伯来语", "value": "he", "aliases": ["heb", "HEB", "he", "iw", "hebrew", "希伯来语", "希伯来文"]},
    {"label": "克罗地亚语", "value": "hr", "aliases": ["hrv", "HRV", "hr", "croatian", "克罗地亚语", "克罗地亚文"]},
    {"label": "匈牙利语", "value": "hu", "aliases": ["hun", "HUN", "hu", "hungarian", "匈牙利语", "匈牙利文"]},
    {"label": "印度尼西亚语", "value": "id", "aliases": ["ind", "IND", "id", "in", "indonesian", "印度尼西亚语", "印尼语"]},
    {"label": "马来语", "value": "ms", "aliases": ["may", "MAY", "msa", "ms", "malay", "马来语", "马来文"]},
    {"label": "挪威语", "value": "nb", "aliases": ["nob", "NOB", "nb", "no", "nor", "norwegian bokmal", "norwegian bokmål", "bokmal", "bokmål", "挪威语", "挪威文", "书面挪威语"]},
    {"label": "荷兰语", "value": "nl", "aliases": ["dut", "DUT", "nld", "nl", "dutch", "flemish", "荷兰语", "荷兰文", "弗拉芒语"]},
    {"label": "波兰语", "value": "pl", "aliases": ["pol", "POL", "pl", "polish", "波兰语", "波兰文"]},
    {"label": "罗马尼亚语", "value": "ro", "aliases": ["rum", "RUM", "ron", "ro", "romanian", "罗马尼亚语", "罗马尼亚文"]},
    {"label": "瑞典语", "value": "sv", "aliases": ["swe", "SWE", "sv", "swedish", "瑞典语", "瑞典文"]},
    {"label": "土耳其语", "value": "tr", "aliases": ["tur", "TUR", "tr", "turkish", "土耳其语", "土耳其文"]},
    {"label": "乌克兰语", "value": "uk", "aliases": ["ukr", "UKR", "uk", "ukrainian", "乌克兰语", "乌克兰文"]},
    {"label": "越南语", "value": "vi", "aliases": ["vie", "VIE", "vi", "vietnamese", "越南语", "越南文"]},
    {"label": "保加利亚语", "value": "bg", "aliases": ["bul", "BUL", "bg", "bulgarian", "保加利亚语", "保加利亚文"]},
    {"label": "爱沙尼亚语", "value": "et", "aliases": ["est", "EST", "et", "estonian", "爱沙尼亚语", "爱沙尼亚文"]},
    {"label": "立陶宛语", "value": "lt", "aliases": ["lit", "LIT", "lt", "lithuanian", "立陶宛语", "立陶宛文"]},
    {"label": "拉脱维亚语", "value": "lv", "aliases": ["lav", "LAV", "lv", "latvian", "拉脱维亚语", "拉脱维亚文"]},
    {"label": "斯洛伐克语", "value": "sk", "aliases": ["slo", "SLO", "slk", "sk", "slovak", "斯洛伐克语", "斯洛伐克文"]},
    {"label": "斯洛文尼亚语", "value": "sl", "aliases": ["slv", "SLV", "sl", "slovenian", "斯洛文尼亚语", "斯洛文尼亚文"]},
    {"label": "泰米尔语", "value": "ta", "aliases": ["tam", "TAM", "ta", "tamil", "泰米尔语", "泰米尔文"]},
    {"label": "泰卢固语", "value": "te", "aliases": ["tel", "TEL", "te", "telugu", "泰卢固语", "泰卢固文"]},
    {"label": "拉丁语", "value": "la", "aliases": ["lat", "la", "latin", "拉丁语", "拉丁文"]},
    {"label": "卡纳达语", "value": "kn", "aliases": ["kan", "KAN", "kn", "kannada", "卡纳达语", "卡纳达文", "康纳达语"]},
    {"label": "马拉雅拉姆语", "value": "ml", "aliases": ["mal", "MAL", "ml", "malayalam", "马拉雅拉姆语", "马拉雅拉姆文"]},
    {"label": "无语言", "value": "xx", "aliases": ["und", "undefined", "unknown", "none", "无语言", "未知"]},
]

# --- ★★★ AI 默认提示词 (中文优化版) ★★★ ---
DEFAULT_AI_PROMPTS = {
    "fast_mode": """你是一个只返回 JSON 格式的翻译 API。
你的任务是将一系列人名（如演员、演职人员）从各种语言翻译成 **简体中文**。

**必须** 返回一个有效的 JSON 对象，将原始名称映射到其中文翻译。
- 源语言可能是任何语言（如英语、日语、韩语、拼音）。
- 目标语言 **必须永远是** 简体中文。
- 如果名字无法翻译或已经是中文，请使用原始名字作为值。
- **某些名字可能不完整或包含首字母（如 "Peter J."）；请根据现有部分提供最可能的标准音译。**
- 不要添加任何解释或 JSON 对象以外的文本。""",

    "transliterate_mode": """你是一个只返回 JSON 格式的影视人名中文化 API。
你的任务是将一系列影视相关的人名（演员、导演、编剧、制作人等）转换为适合中文媒体库展示的 **简体中文姓名**。

规则：
1. 优先使用该人物在中文世界最常见、最通用的译名。
2. 如果没有公认译名，再根据发音进行自然、常见的中文音译。
3. 目标语言必须永远是简体中文。
4. 如果名字已经是中文，保持原样。
5. 如果名字包含首字母、缩写或不完整部分，请尽力翻译可识别部分。
6. 如果实在无法处理，使用原始名字作为值。
7. 必须返回合法 JSON 对象，键为原文，值为中文结果。
8. 不要输出任何解释、注释、Markdown 或额外文本。""",

    "quality_mode": """你是一位世界级的影视专家，扮演一个只返回 JSON 的 API。
你的任务是利用提供的影视上下文，准确地将外语或拼音的演员名和角色名翻译成 **简体中文**。

**输入格式：**
你将收到一个包含 `context`（含 `title` 和 `year`）和 `terms`（待翻译字符串列表）的 JSON 对象。

**你的策略：**
1. **利用上下文：** 使用 `title` 和 `year` 来确定具体的剧集/电影。在该特定作品的背景下，找到 `terms` 的官方或最受认可的中文译名。这对角色名至关重要。
2. **翻译拼音：** 如果词条是拼音（如 "Zhang San"），请将其翻译成汉字（"张三"）。
3. **【核心指令】**
   **目标语言永远是简体中文：** 无论作品或名字的原始语言是什么（如韩语、日语、英语），你的最终输出翻译 **必须** 是 **简体中文**。不要翻译成该剧的原始语言。
4. **兜底：** 如果一个词条无法或不应被翻译，你 **必须** 使用原始字符串作为其值。

**输出格式（强制）：**
你 **必须** 返回一个有效的 JSON 对象，将每个原始词条映射到其中文翻译。严禁包含其他文本或 markdown 标记。""",

    "overview_translation": """你是一位专门从事影视剧情简介翻译的专业译者。
你的任务是将提供的英文简介翻译成 **流畅、引人入胜的简体中文**。

**指南：**
1. **语调：** 专业、吸引人，适合作为媒体库的介绍。避免机器翻译的生硬感。
2. **准确性：** 保留原意、关键情节和基调（如喜剧与恐怖）。
3. **人名：** 如果简介中包含演员或角色的名字，如果知道其标准中文译名，请进行翻译；如果不确定，请保留英文。
4. **输出：** 返回一个有效的 JSON 对象，包含一个键 "translation"，值为翻译后的文本。

**输入：**
标题: {title}
简介: {overview}

**输出格式：**
{{
  "translation": "..."
}}""",

    "title_translation": """你是一位影视数据库的专业编辑。
你的任务是将提供的标题翻译成 **简体中文**。

**规则：**
1. **电影/剧集：** 如果类型是 'Movie' 或 'Series'，优先使用现有的中国大陆官方译名。如果没有，使用标准音译或意译。
2. **分集 (关键)：** 如果类型是 'Episode'，**直接翻译标题的含义（意译）**。不要保留英文，除非它是无法翻译的专有名词。
   * 例如: "The Weekend in Paris Job" -> "巴黎周末行动" 或 "巴黎周末任务"
   * 例如: "Pilot" -> "试播集"
3. **风格：** 保持简洁、专业。
4. **无额外文本：** 不要包含年份或解释。
5. **输出：** 返回一个有效的 JSON 对象。

**输入：**
类型: {media_type}
原标题: {title}
年份: {year}

**输出格式：**
{{
  "translation": "..."
}}""",

    "filename_parsing": """你是一个影视文件名解析专家。
你的任务是从不规范的影视文件或文件夹名称中，提取出用于搜索的【核心片名】、【年份】和【类型】。

规则：
1. 移除所有广告词、分辨率(1080p/4k)、压制组、视频格式(mp4/mkv)、音视频编码(H265/AAC)等无关信息。
2. 如果包含中英文双语标题，优先提取【中文标题】。
3. 类型(type)只能是 "movie" (电影) 或 "tv" (剧集)。如果包含 S01, E01, 第x季, 完结 等字眼，则是 tv。
4. 年份(year)提取4位数字，如果没有则返回空字符串。
5. 必须返回严格的 JSON 格式。

输入文件名：{filename}

输出格式：
{{
  "title": "提取的纯净片名",
  "year": "2023",
  "type": "movie"
}}""",

    "batch_overview_translation": """你是一个专业的影视翻译专家。
请将以下 JSON 格式的影视剧情简介翻译成流畅、自然的简体中文。
上下文影视名称：{context_title}

**【最高指令 / 严格要求】**：
1. **绝对禁止修改键名**：必须 100% 保持原有的 JSON 键（ID）不变！绝对不允许新增、删除或篡改任何键名！
2. **只翻译值**：只翻译简介内容，遇到人名如果不确定中文译名请保留英文。
3. **符合中文习惯**：翻译要流畅自然，拒绝生硬的机翻腔调。
4. **纯 JSON 输出**：你必须且只能输出一个合法的 JSON 对象。**绝对不要**包含任何解释性文字，**绝对不要**使用 ```json 这样的 Markdown 代码块标记包裹！

**输入示例：**
{{
  "123": "This is an overview.",
  "456": "Another overview here."
}}

**输出示例：**
{{
  "123": "这是一个简介。",
  "456": "这里是另一个简介。"
}}""",

    "batch_title_translation": """你是一个专业的影视翻译专家。
请将以下 JSON 格式的影视标题（类型：{media_type}）翻译成流畅、自然的简体中文。

**【最高指令 / 严格要求】**：
1. **绝对禁止修改键名**：必须 100% 保持原有的 JSON 键（ID）不变！绝对不允许新增、删除或篡改任何键名！
2. **只翻译值**：只翻译标题内容。如果标题已经是中文，请保持原样返回。
3. **专有名词**：如果是人名或专有名词，请提供通用的中文译名或音译。
4. **纯 JSON 输出**：你必须且只能输出一个合法的 JSON 对象。**绝对不要**包含任何解释性文字，**绝对不要**使用 ```json 这样的 Markdown 代码块标记包裹！

**输入示例：**
{{
  "123": "The Beginning",
  "456": "A New Hope"
}}

**输出示例：**
{{
  "123": "开端",
  "456": "新希望"
}}""",

    "batch_joke_fallback": """你是一个幽默、嘴碎的影视解说员“老六”。
以下影视项目（或分集）目前缺少官方剧情简介，请你发挥想象力，为它们分别编一个简短、幽默的冷笑话、吐槽或段子来占位。

**【最高指令 / 严格要求】**：
1. **必须带有前缀**：每个笑话必须以“【老六占位简介】”开头！
2. **简短有趣**：50字以内，如果是剧集分集，可以结合“第X集”调侃一下追剧人的日常（比如催更、水剧情、主角光环等）。
3. **纯 JSON 输出**：必须且只能输出一个合法的 JSON 对象，键名为提供的ID，键值为生成的笑话。绝对不要包含任何解释性文字或 Markdown 代码块标记！

输入示例：
{
    "S1E1": "权力的游戏 S1E1",
    "movie_123": "阿凡达3"
}
输出示例：
{
    "S1E1": "【老六占位简介】凛冬将至，但我连秋裤都还没买，这集主要讲大家怎么凑钱买煤。",
    "movie_123": "【老六占位简介】导演还在水里憋气呢，简介等他浮上来再写吧。"
}"""
}

# --- 分级计算通用逻辑 (含 Adult 强匹配) ---
def get_rating_label(details: dict, media_type: str, rating_map: Optional[dict] = None, priority: Optional[list] = None) -> str:
    """
    根据 TMDb 详情、媒体类型和配置，计算统一的分级标签 (Label)。
    
    逻辑：
    1. 【Adult 强匹配】如果 TMDb 标记为 adult=True，且配置中有 emby_value=15 的项，直接返回该标签。
    2. 【优先级遍历】按照 priority 配置的国家顺序查找分级。
    3. 【映射转换】将找到的国家分级代码转换为统一的中文 Label。
    """
    if rating_map is None: rating_map = DEFAULT_RATING_MAPPING
    if priority is None: priority = DEFAULT_RATING_PRIORITY

    # 1. ★★★ Adult 强匹配 (最高优先级) ★★★
    # 如果 TMDb 明确标记为成人内容
    if details.get('adult') is True:
        # 遍历所有国家的配置，寻找任意一个定义了 emby_value=15 (成人) 的标签
        # 通常在 US 里配置了 XXX -> 成人 -> 15
        for country_rules in rating_map.values():
            for rule in country_rules:
                if rule.get('emby_value') == 15:
                    return rule['label']

    # 2. 准备源数据
    rating_code = None
    rating_country = None
    
    # 获取原产国 (用于处理 'ORIGIN' 优先级)
    origin_countries = details.get('origin_country', [])
    if not origin_countries and 'production_countries' in details:
        origin_countries = [c.get('iso_3166_1') for c in details['production_countries']]
    
    # 3. 遍历优先级
    for country in priority:
        target_countries = []
        if country == 'ORIGIN':
            target_countries = origin_countries
        else:
            target_countries = [country]
        
        if not target_countries: continue

        for target_c in target_countries:
            found_code = None
            
            if media_type == 'tv':
                # TV 逻辑: content_ratings.results
                results = details.get('content_ratings', {}).get('results', [])
                found = next((r for r in results if r['iso_3166_1'] == target_c), None)
                if found: found_code = found.get('rating')
            else:
                # Movie 逻辑: release_dates.results
                results = details.get('release_dates', {}).get('results', [])
                country_data = next((r for r in results if r['iso_3166_1'] == target_c), None)
                if country_data:
                    # 电影可能有多个分级 (不同版本)，优先取第一个非空的 certification
                    for rel in country_data.get('release_dates', []):
                        if rel.get('certification'):
                            found_code = rel.get('certification')
                            break
            
            if found_code:
                rating_code = found_code
                rating_country = target_c
                break
        
        if rating_code: break

    # 4. 映射到 Label
    if rating_code and rating_country:
        # 查找对应的 Label
        country_rules = rating_map.get(rating_country, [])
        
        # 尝试完全匹配
        for rule in country_rules:
            if rule['code'] == rating_code:
                return rule['label']
        
        # 如果没找到完全匹配，尝试不区分大小写
        for rule in country_rules:
            if rule['code'].lower() == rating_code.lower():
                return rule['label']

    return '未知'

# --- ★★★ 万能 STRM 提取器 (支持自定义正则) ★★★ ---
def extract_pickcode_from_strm_url(url: str) -> Optional[str]:
    """万能 PC 码提取器：支持 ETK, MP, CMS, MH 等，以及用户自定义正则"""
    if not url or not isinstance(url, str):
        return None
        
    # 1. ETK 官方格式 (最高优先级，最快，绝对精准)
    if '/p115/play/' in url:
        return url.split('/p115/play/')[-1].split('/')[0].split('?')[0].strip()
        
    # 2. 用户自定义正则 (高优先级，赋予用户最高控制权)
    try:
        from database import settings_db
        custom_rules = settings_db.get_setting("custom_strm_regex") or []
        for rule in custom_rules:
            if not rule: continue
            match = re.search(rule, url, re.IGNORECASE)
            # 必须使用 () 捕获组，且提取第一组
            if match and len(match.groups()) > 0:
                return match.group(1)
    except Exception as e:
        logger.error(f"执行自定义 STRM 正则时出错: {e}")

    # 3. 内置常见第三方格式 (作为最后的兜底)
    # MP 格式 (pickcode=xxx)
    match = re.search(r'pick_?code=([a-zA-Z0-9]+)', url, re.IGNORECASE)
    if match: return match.group(1)
    # CMS 格式 (/d/xxx)
    match = re.search(r'/d/([a-zA-Z0-9]+)[.?/]', url)
    if not match: match = re.search(r'/d/([a-zA-Z0-9]+)$', url)
    if match: return match.group(1)
    # MH 格式 (fileid=xxx)
    match = re.search(r'fileid=([a-zA-Z0-9]+)', url, re.IGNORECASE)
    if match: return match.group(1)
        
    return None

def get_pinyin_initials(text: str) -> str:
    """获取中文拼音首字母大写，用于 Emby 的 sorttitle"""
    if not text:
        return ""
    if not PYPINYIN_AVAILABLE:
        return text
    try:
        initials = pinyin(text, style=Style.FIRST_LETTER, strict=False)
        result = ""
        for i in initials:
            char = i[0]
            if char.isalnum():
                result += char.upper()
        return result if result else text
    except Exception:
        return text
    
def is_spam_title(title: str) -> bool:
    """
    检测标题是否包含卖片、博彩等恶意广告信息。
    """
    if not title:
        return False
    
    # 1. 恶意关键词黑名单 (可根据需要自行添加)
    spam_keywords = [
        '看黄', '片网', '色网', '澳门', '赌场', '真人发牌', 
        '加微', '微信', '网址', '在线观看', '免费看', 'AV'
    ]
    for kw in spam_keywords:
        if kw in title:
            return True
            
    # 2. 正则匹配：检测是否包含域名后缀 (如 .com, .net, .xyz 等) 或连续的长串数字(QQ号)
    # 匹配类似 4488469.com 或 www.xxx.vip
    if re.search(r'[a-zA-Z0-9-]+\.(com|net|org|xyz|cc|tv|vip|top|me)\b', title, re.IGNORECASE):
        return True
        
    # 匹配连续6位以上的数字 (正常电影名很少有连续6位数字，年份最多4位)
    if re.search(r'\d{6,}', title):
        return True
        
    return False

def clean_invisible_chars(text: str) -> str:
    """
    终极字符串清洗：去除所有不可见的零宽字符、特殊排版符号，并将特殊空格转换为普通空格。
    """
    if not text:
        return ""
    
    # 1. 替换特殊空格为普通空格
    # \xa0: 不换行空格 (NBSP)
    # \u3000: 全角空格 (中文输入法下的空格)
    text = text.replace('\xa0', ' ').replace('\u3000', ' ')
    
    # 2. 使用正则剔除所有“零宽字符”和“排版控制字符”
    # \u200b-\u200f: 零宽空格、零宽连字、从左至右标记等
    # \u202a-\u202e: 文本方向控制符
    # \u2060-\u206f: 词语连接符等不可见数学符号
    # \ufeff: 字节顺序标记 (BOM)
    text = re.sub(r'[\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff]', '', text)
    
    # 3. 将连续的多个空格压缩为一个，并去除首尾空格
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()

# --- TG 频道监听默认正则预设 ---
DEFAULT_TG_REGEX = {
    "tmdb": [
        r'TMDB(?:\s*ID)?[:：\s]*(\d+)'
    ],
    "title_year": [
        r'(?:电视剧 | 电影 | 名称)[:：\s]*([^\n]+?)\s*\((\d{4})\)',
        r'^([^\n]+?)\s*\((\d{4})\)'
    ],
    "password_url": [
        r'(?:password|pwd)=([a-zA-Z0-9]{4})'
    ],
    "password_text": [
        r'(?:password=|访问码 | 提取码 | 密码)[:：=\s]*([a-zA-Z0-9]{4})'
    ]
}

def clean_non_chinese_chars(text: Optional[str]) -> str:
    """
    清理字符串，只保留中文字符和中文括号。
    
    移除所有非中文字符，包括：
    - 英文字母、数字
    - 标点符号（除中文括号外）
    - 特殊符号、表情符号
    - 空白字符
    - 其他非汉字字符
    
    保留的字符范围：
    - 基本汉字：\u4e00-\u9fff
    - 扩展 A 区：\u3400-\u4dbf
    - 扩展 B-F 区：\u20000-\u2a6df, \u2a700-\u2b73f, \u2b740-\u2b81f, \u2b820-\u2ceaf, \u2ceb0-\u2ebef
    - 兼容汉字：\uf900-\ufaff
    - 中文括号：（）
    
    Args:
        text: 输入字符串
        
    Returns:
        只包含中文字符和中文括号的字符串
    """
    if not text:
        return ""
    
    result = []
    for char in str(text):
        code_point = ord(char)
        # 基本汉字
        if 0x4e00 <= code_point <= 0x9fff:
            result.append(char)
        # 扩展 A 区
        elif 0x3400 <= code_point <= 0x4dbf:
            result.append(char)
        # 兼容汉字
        elif 0xf900 <= code_point <= 0xfaff:
            result.append(char)
        # 扩展 B-F 区（代理对）
        elif 0x20000 <= code_point <= 0x2ebef:
            result.append(char)
        # 中文括号
        elif char in '（）':
            result.append(char)
    
    return ''.join(result)
