# maoyan_fetcher.py (V4.0 - 精确匹配版)
import logging
import requests
import argparse
import json
import random
from typing import List, Dict, Tuple, Optional
import sys
import os
import time
import re

# -- 关键：确保可以导入项目中的其他模块 --
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import tmdb_handler
except ImportError as e:
    print(f"错误：缺少 tmdb_handler 模块。请确保路径正确。详细信息: {e}")
    sys.exit(1)

# --- 日志记录设置 ---
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] - %(message)s')
logger = logging.getLogger(__name__)

# ★★★ 新增 1/2: 从主处理器中引入完整的标题解析工具 ★★★
SEASON_PATTERN = re.compile(r'(.*?)\s*[（(]?\s*(第?[一二三四五六七八九十百]+)\s*季\s*[)）]?')
CHINESE_NUM_MAP = {
    '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9, '十': 10,
    '十一': 11, '十二': 12, '十三': 13, '十四': 14, '十五': 15, '十六': 16, '十七': 17, '十八': 18, '十九': 19, '二十': 20,
    '第一': 1, '第二': 2, '第三': 3, '第四': 4, '第五': 5, '第六': 6, '第七': 7, '第八': 8, '第九': 9, '第十': 10,
    '第十一': 11, '第十二': 12, '第十三': 13, '第十四': 14, '第十五': 15, '第十六': 16, '第十七': 17, '第十八': 18, '第十九': 19, '第二十': 20
}

def parse_series_title(title: str) -> Tuple[str, Optional[int]]:
    """
    (V4 - 兼容末尾数字版) 能够处理中英文季号混合及末尾数字的复杂标题。
    """
    show_name = title.strip()
    season_number = None
    SEASON_PATTERN_EN = re.compile(r'(.*?)\s+Season\s+(\d+)', re.IGNORECASE)
    SEASON_PATTERN_CN = SEASON_PATTERN
    SEASON_PATTERN_NUM = re.compile(r'^(.*?)\s*(\d+)$')

    match_en = SEASON_PATTERN_EN.search(show_name)
    if match_en:
        show_name = match_en.group(1).strip()
        season_number = int(match_en.group(2))

    match_cn = SEASON_PATTERN_CN.search(show_name)
    if match_cn:
        show_name = match_cn.group(1).strip()
        if season_number is None:
            season_word = match_cn.group(2)
            season_number_from_cn = CHINESE_NUM_MAP.get(season_word)
            if season_number_from_cn:
                season_number = season_number_from_cn

    if season_number is None:
        if not re.search(r'\b(19|20)\d{2}$', show_name):
            match_num = SEASON_PATTERN_NUM.search(show_name)
            if match_num:
                potential_name = match_num.group(1).strip()
                if potential_name:
                    show_name = potential_name
                    season_number = int(match_num.group(2))

    return show_name, season_number

def get_random_user_agent() -> str:
    user_agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    ]
    return random.choice(user_agents)

def get_cookies() -> Dict[str, str]:
    logger.debug("当前 API 无需 Cookie，跳过浏览器操作。")
    return {}

def get_maoyan_rank_titles(types_to_fetch: List[str], platform: str, num: int) -> Tuple[List[Dict], List[Dict]]:
    # ... 此函数保持不变 ...
    movies_list = []
    tv_list = []
    
    headers = {'User-Agent': get_random_user_agent()}
    cookies = get_cookies()

    maoyan_url = 'https://piaofang.maoyan.com'

    MAX_RETRIES = 3
    RETRY_DELAY_SECONDS = 3

    if 'movie' in types_to_fetch:
        url = f'{maoyan_url}/dashboard-ajax/movie'
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"正在获取电影票房榜 (第 {attempt + 1}/{MAX_RETRIES} 次尝试)...")
                response = requests.get(url, headers=headers, cookies=cookies, timeout=30)
                response.raise_for_status()
                data = response.json().get('movieList', {}).get('list', [])
                movies_list.extend([
                    {"title": movie.get('movieInfo', {}).get('movieName')}
                    for movie in data if movie.get('movieInfo', {}).get('movieName')
                ][:num])
                logger.info("电影票房榜获取成功。")
                break
            except Exception as e:
                logger.warning(f"获取电影票房榜失败 (第 {attempt + 1} 次尝试): {e}")
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAY_SECONDS * (attempt + 1)
                    logger.info(f"将在 {delay} 秒后重试...")
                    time.sleep(delay)
                else:
                    logger.error("获取电影票房榜在多次重试后彻底失败。")

    tv_heat_map = {'web-heat': '0', 'web-tv': '1', 'zongyi': '2'}
    platform_code_map = {'all': '', 'tencent': '3', 'iqiyi': '2', 'youku': '1', 'mango': '7'}
    platform_code = platform_code_map.get(platform, '')
    
    tv_types_to_fetch = [t for t in types_to_fetch if t in tv_heat_map]
    if tv_types_to_fetch:
        for tv_type in tv_types_to_fetch:
            series_type_code = tv_heat_map[tv_type]
            url = f'{maoyan_url}/dashboard/webHeatData?seriesType={series_type_code}&platformType={platform_code}&showDate=2'
            for attempt in range(MAX_RETRIES):
                try:
                    logger.info(f"正在获取热度榜 (类型: {tv_type}, 第 {attempt + 1}/{MAX_RETRIES} 次尝试)...")
                    response = requests.get(url, headers=headers, cookies=cookies, timeout=30)
                    response.raise_for_status()
                    data = response.json().get('dataList', {}).get('list', [])
                    tv_list.extend([
                        {"title": item.get('seriesInfo', {}).get('name')}
                        for item in data if item.get('seriesInfo', {}).get('name')
                    ][:num])
                    logger.info(f"热度榜 '{tv_type}' 获取成功。")
                    break
                except Exception as e:
                    logger.warning(f"获取 {tv_type} 热度榜失败 (第 {attempt + 1} 次尝试): {e}")
                    if attempt < MAX_RETRIES - 1:
                        delay = RETRY_DELAY_SECONDS * (attempt + 1)
                        logger.info(f"将在 {delay} 秒后重试...")
                        time.sleep(delay)
                    else:
                        logger.error(f"获取 {tv_type} 热度榜在多次重试后彻底失败。")

    unique_tv_list = list({item['title']: item for item in tv_list}.values())
    return movies_list, unique_tv_list

# ★★★ 新增 2/2: 重构核心匹配函数，使其变得智能 ★★★
def match_titles_to_tmdb(titles: List[Dict], item_type: str, tmdb_api_key: str) -> List[Dict[str, str]]:
    matched_items = []
    
    def normalize_string(s: str) -> str:
        if not s: return ""
        return re.sub(r'[\s:：·\-*\'!,?.。]+', '', s).lower()

    for item in titles:
        title = item.get('title')
        if not title:
            continue
        
        if item_type == 'Movie':
            logger.info(f"正在为 Movie '{title}' 搜索TMDb匹配...")
            results = tmdb_handler.search_media(title, tmdb_api_key, 'Movie')
            if results:
                best_match = results[0]
                tmdb_id = str(best_match.get('id'))
                match_name = best_match.get('title')
                logger.info(f"  -> 匹配成功: {match_name} (ID: {tmdb_id})")
                matched_items.append({'id': tmdb_id, 'type': 'Movie'})
            else:
                logger.warning(f"  -> 未能为 '{title}' 找到任何TMDb匹配项。")
        
        elif item_type == 'Series':
            logger.info(f"正在为 Series '{title}' 搜索TMDb匹配...")
            
            show_name, season_number = parse_series_title(title)
            logger.debug(f"  -> 标题 '{title}' 解析为: 剧名='{show_name}', 季号='{season_number}'")
            
            results = tmdb_handler.search_media(show_name, tmdb_api_key, 'Series')
            
            if not results:
                logger.warning(f"  -> 使用搜索词 '{show_name}' 未能找到任何TMDb匹配项。")
                continue

            series_result = None
            norm_show_name = normalize_string(show_name)
            
            for result in results:
                result_name = result.get('name', '')
                if normalize_string(result_name) == norm_show_name:
                    series_result = result
                    logger.info(f"  -> 通过【精确匹配】找到了基础剧集: {result.get('name')} (ID: {result.get('id')})")
                    break
            
            if not series_result:
                series_result = results[0]
                logger.warning(f"  -> 未找到精确匹配，【回退使用】最相关的结果: {series_result.get('name')} (ID: {series_result.get('id')})")
            
            tmdb_id = str(series_result.get('id'))
            
            # ★★★ 核心修正：在这里构建包含季号的结果 ★★★
            item_to_add = {'id': tmdb_id, 'type': 'Series'}
            if season_number is not None:
                item_to_add['season'] = season_number
                logger.info(f"  -> 已为剧集 '{show_name}' 附加季号: {season_number}")
            
            matched_items.append(item_to_add)
            
    return matched_items

def main():
    parser = argparse.ArgumentParser(description="独立的猫眼榜单获取和TMDb匹配器。")
    parser.add_argument('--api-key', required=True, help="TMDb API Key。")
    parser.add_argument('--output-file', required=True, help="用于存储结果的JSON文件路径。")
    parser.add_argument('--num', type=int, default=10, help="每个榜单获取的项目数量。")
    parser.add_argument('--types', nargs='+', default=['movie'], help="要获取的榜单类型 (例如: movie web-heat zongyi)。")
    parser.add_argument('--platform', default='all', help="平台来源 (all, tencent, iqiyi, youku, mango)。")
    args = parser.parse_args()

    logger.info(f"开始执行猫眼榜单数据抓取和匹配任务 (平台: {args.platform})...")
    
    movie_titles, tv_titles = get_maoyan_rank_titles(args.types, args.platform, args.num)
    
    matched_movies = match_titles_to_tmdb(movie_titles, 'Movie', args.api_key)
    matched_series = match_titles_to_tmdb(tv_titles, 'Series', args.api_key)
    
    all_items = matched_movies + matched_series
    unique_items = list({f"{item['type']}-{item['id']}": item for item in all_items}.values())
    
    try:
        with open(args.output_file, 'w', encoding='utf-8') as f:
            json.dump(unique_items, f, ensure_ascii=False, indent=4)
        logger.info(f"成功将 {len(unique_items)} 个项目写入到缓存文件: {args.output_file}")
    except Exception as e:
        logger.error(f"写入JSON结果文件时出错: {e}")
        sys.exit(1)
        
    logger.info("任务执行完毕。")

if __name__ == "__main__":
    main()