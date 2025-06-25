# utils.py (最终智能匹配版)

import re
import os
from typing import Optional, List, Dict, Any
from urllib.parse import quote_plus
from packaging.version import parse as parse_version
import unicodedata
import logging
import requests
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
    name = re.sub(r'\(.*?\)|\[.*?\]', '', name).strip()

    # 移除 as 前缀（如 "as Kevin"）
    name = re.sub(r'^(as\s+)', '', name, flags=re.IGNORECASE).strip()

    # 清理前缀中的“饰演/饰/配音/配”（不加判断，直接清理）
    name = re.sub(r'^(饰演|饰|配音|配)\s*', '', name).strip()

    # 清理后缀中的“饰演/饰/配音/配”
    name = re.sub(r'\s*(饰演|饰|配音|配)$', '', name).strip()

    # 处理中外对照：“中文 + 英文”形式，只保留中文部分
    match = re.match(r'^([\u4e00-\u9fa5·]{1,})([^a-zA-Z]*)[a-zA-Z]+.*$', name)
    if match:
        chinese_part = match.group(1).strip()
        return chinese_part

    # 如果只有外文，或清理后是英文，保留原值，等待后续翻译流程
    return name.strip()
def translate_text_with_translators(
    query_text: str,
    to_language: str = 'zh',
    engine_order: Optional[List[str]] = None,
    from_language: str = 'auto'
) -> Optional[Dict[str, str]]:
    """使用指定的翻译引擎顺序尝试翻译文本。"""
    if not query_text or not query_text.strip() or not TRANSLATORS_LIB_AVAILABLE:
        return None
    if engine_order is None:
        engine_order = ['bing', 'google', 'baidu']
    for engine_name in engine_order:
        try:
            translated_text = translators_translate_text(
                query_text, translator=engine_name, to_language=to_language,
                from_language=from_language, timeout=10.0
            )
            if translated_text and translated_text.strip().lower() != query_text.strip().lower():
                return {"text": translated_text.strip(), "engine": engine_name}
        except Exception:
            continue
    return None

def generate_search_url(site: str, title: str, year: Optional[int] = None) -> str:
    """为指定网站生成搜索链接。"""
    query = f'"{title}"'
    if year: query += f' {year}'
    final_query = f'site:zh.wikipedia.org {query} 演员表' if site == 'wikipedia' else query + " 演员表 cast"
    return f"https://www.google.com/search?q={quote_plus(final_query)}"

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
# ★★★ 获取 override 路径的辅助函数 ★★★
def get_name_variants(name: Optional[str]) -> set:
    """
    根据一个名字生成所有可能的变体集合，用于匹配。
    处理中文转拼音、英文名姓/名顺序。
    """
    if not name:
        return set()
    
    name_str = str(name).strip()
    
    # 检查是否包含中文字符
    if contains_chinese(name_str):
        if PYPINYIN_AVAILABLE:
            # 如果是中文，转换为无音调拼音
            pinyin_list = pinyin(name_str, style=Style.NORMAL)
            pinyin_flat = "".join([item[0] for item in pinyin_list if item])
            return {pinyin_flat.lower()}
        else:
            # 如果 pypinyin 不可用，无法处理中文名，返回空集合
            return set()

    # 如果是英文/拼音，处理姓和名顺序
    parts = name_str.split()
    if not parts:
        return set()
        
    # 标准化并移除所有空格和特殊字符
    normalized_direct = normalize_name_for_matching(name_str)
    variants = {normalized_direct}
    
    # 如果有多于一个部分，尝试颠倒顺序
    if len(parts) > 1:
        reversed_name = " ".join(parts[::-1])
        normalized_reversed = normalize_name_for_matching(reversed_name)
        variants.add(normalized_reversed)
        
    return variants

def are_names_match(name1_a: Optional[str], name1_b: Optional[str], name2_a: Optional[str], name2_b: Optional[str]) -> bool:
    """
    【智能版】比较两组名字是否可能指向同一个人。
    """
    # 为第一组名字（通常是 TMDb）生成变体集合
    variants1 = get_name_variants(name1_a)
    if name1_b:
        variants1.update(get_name_variants(name1_b))
    
    # 为第二组名字（通常是豆瓣）生成变体集合
    variants2 = get_name_variants(name2_a)
    if name2_b:
        variants2.update(get_name_variants(name2_b))

    # 移除可能产生的空字符串，避免错误匹配
    if "" in variants1: variants1.remove("")
    if "" in variants2: variants2.remove("")
        
    # 如果任何一个集合为空，则无法匹配
    if not variants1 or not variants2:
        return False
            
    # 检查两个集合是否有任何共同的元素（交集不为空）
    return not variants1.isdisjoint(variants2)

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
# ★★★ 版本检查函数 ★★★
def check_for_updates(current_version: str, github_repo: str) -> tuple[bool, str | None]:
    """
    通过 GitHub API 检查应用是否有新版本。

    :param current_version: 当前应用的版本号 (例如 "2.2.7")
    :param github_repo: GitHub 仓库路径 (例如 "your_username/your_repo_name")
    :return: 一个元组 (has_update: bool, latest_version: str | None)
    """
    # GitHub API 获取最新 release 的 URL
    api_url = f"https://api.github.com/repos/{github_repo}/releases/latest"
    
    logger.debug(f"正在从 {api_url} 检查更新...")
    
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # 从 API 响应中获取最新的版本标签名
        latest_version_tag = data.get("tag_name")
        if not latest_version_tag:
            logger.warning("GitHub API 响应中未找到 'tag_name'。")
            return False, None

        # 使用 packaging.version 库来正确比较版本号
        # 它可以处理 "v2.3.0" 和 "2.3.0" 这样的情况
        if parse_version(latest_version_tag) > parse_version(current_version):
            logger.info(f"发现新版本！当前版本: {current_version}, 最新版本: {latest_version_tag}")
            return True, latest_version_tag
        else:
            logger.info(f"当前已是最新版本。当前: {current_version}, 最新: {latest_version_tag}")
            return False, latest_version_tag

    except requests.exceptions.RequestException as e:
        logger.error(f"检查更新时发生网络错误: {e}")
        return False, None
    except Exception as e:
        logger.error(f"检查更新时发生未知错误: {e}", exc_info=True)
        return False, None

if __name__ == '__main__':
    # 测试新的 are_names_match
    print("\n--- Testing are_names_match ---")
    # 测试1: 张子枫
    print(f"张子枫 vs Zhang Zifeng: {are_names_match('Zhang Zifeng', 'Zhang Zifeng', '张子枫', 'Zifeng Zhang')}") # 应该为 True
    # 测试2: 姓/名顺序
    print(f"Jon Hamm vs Hamm Jon: {are_names_match('Jon Hamm', None, 'Hamm Jon', None)}") # 应该为 True
    # 测试3: 特殊字符和大小写
    print(f"Chloë Moretz vs chloe moretz: {are_names_match('Chloë Moretz', None, 'chloe moretz', None)}") # 应该为 True
    # 测试4: 中文 vs 拼音
    print(f"张三 vs zhang san: {are_names_match('zhang san', None, '张三', None)}") # 应该为 True
    # 测试5: 不匹配
    print(f"Zhang San vs Li Si: {are_names_match('Zhang San', None, 'Li Si', None)}") # 应该为 False



