# custom_collection_handler.py
import logging
import requests
import xml.etree.ElementTree as ET
import re
import os
import sys
import gevent
from typing import List, Dict, Any, Optional, Tuple
import json
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

# ★★★ 核心修正：再次回归 gevent.subprocess ★★★
from gevent import subprocess, Timeout

import tmdb_handler
import emby_handler
import config_manager
from database import collection_db, watchlist_db, connection 
from douban import DoubanApi
from tmdb_handler import search_media, get_tv_details

logger = logging.getLogger(__name__)


class ListImporter:
    """
    (V9.1 - 最终异步版)
    使用 gevent.subprocess，并确保在独立的 greenlet 中运行，
    从而实现真正的非阻塞异步执行。
    """
    
    SEASON_PATTERN = re.compile(r'(.*?)\s*[（(]?\s*(第?[一二三四五六七八九十百]+)\s*季\s*[)）]?')
    
    # ▼▼▼ 优化：扩展数字映射，支持到二十季，增强兼容性 ▼▼▼
    CHINESE_NUM_MAP = {
        '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9, '十': 10,
        '十一': 11, '十二': 12, '十三': 13, '十四': 14, '十五': 15, '十六': 16, '十七': 17, '十八': 18, '十九': 19, '二十': 20,
        '第一': 1, '第二': 2, '第三': 3, '第四': 4, '第五': 5, '第六': 6, '第七': 7, '第八': 8, '第九': 9, '第十': 10,
        '第十一': 11, '第十二': 12, '第十三': 13, '第十四': 14, '第十五': 15, '第十六': 16, '第十七': 17, '第十八': 18, '第十九': 19, '第二十': 20
    }
    VALID_MAOYAN_PLATFORMS = {'tencent', 'iqiyi', 'youku', 'mango'}

    def __init__(self, tmdb_api_key: str):
        self.tmdb_api_key = tmdb_api_key
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})

    # ★★★ 核心修改：这个函数现在是纯粹的阻塞执行逻辑 ★★★
    def _execute_maoyan_fetch(self, definition: Dict) -> List[Dict[str, str]]:
        maoyan_url = definition.get('url', '')
        temp_output_file = os.path.join(config_manager.PERSISTENT_DATA_PATH, f"maoyan_temp_output_{hash(maoyan_url)}.json")
        
        content_key = maoyan_url.replace('maoyan://', '')
        parts = content_key.split('-')
        
        platform = 'all'
        if len(parts) > 1 and parts[-1] in self.VALID_MAOYAN_PLATFORMS:
            platform = parts[-1]
            type_part = '-'.join(parts[:-1])
        else:
            type_part = content_key

        types_to_fetch = [t.strip() for t in type_part.split(',') if t.strip()]
        
        if not types_to_fetch:
            logger.error(f"  ➜ 无法从猫眼URL '{maoyan_url}' 中解析出有效的类型。")
            return []
            
        limit = definition.get('limit')
        if not limit:
            limit = 50

        fetcher_script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'maoyan_fetcher.py')
        if not os.path.exists(fetcher_script_path):
            logger.error(f"  ➜ 严重错误：无法找到猫眼获取脚本 '{fetcher_script_path}'。")
            return []

        command = [
            sys.executable,
            fetcher_script_path,
            '--api-key', self.tmdb_api_key,
            '--output-file', temp_output_file,
            '--num', str(limit),
            '--platform', platform,
            '--types', *types_to_fetch
        ]
        
        try:
            logger.debug(f"  ➜ (在一个独立的 Greenlet 中) 执行命令: {' '.join(command)}")
            
            result_bytes = subprocess.check_output(
                command, 
                stderr=subprocess.STDOUT, 
                timeout=600
            )
            
            result_output = result_bytes.decode('utf-8', errors='ignore')
            logger.info("  ➜ 猫眼获取脚本成功完成。")
            if result_output:
                logger.debug(f"  ➜ 脚本输出:\n{result_output}")
            
            with open(temp_output_file, 'r', encoding='utf-8') as f:
                results = json.load(f)
            
            return results

        except Timeout:
            logger.error("  ➜ 执行猫眼获取脚本超时（超过10分钟）。")
            return []
        except subprocess.CalledProcessError as e:
            error_output = e.output.decode('utf-8', errors='ignore') if e.output else "No output captured."
            logger.error(f"  ➜ 执行猫眼获取脚本失败。返回码: {e.returncode}")
            logger.error(f"  ➜ 脚本的完整错误输出:\n{error_output}")
            return []
        except Exception as e:
            logger.error(f"  ➜ 处理猫眼榜单时发生未知错误: {e}", exc_info=True)
            return []
        finally:
            if os.path.exists(temp_output_file):
                os.remove(temp_output_file)

    # ... 其他所有方法 (_match_by_ids, process, FilterEngine等) 保持完全不变 ...
    def _match_by_ids(self, imdb_id: Optional[str], tmdb_id: Optional[str], item_type: str) -> Optional[str]:
        if tmdb_id:
            logger.debug(f"  ➜ 通过TMDb ID直接匹配：{tmdb_id}")
            return tmdb_id
        if imdb_id:
            logger.debug(f"  ➜ 通过IMDb ID查找TMDb ID：{imdb_id}")
            try:
                tmdb_id_from_imdb = tmdb_handler.get_tmdb_id_by_imdb_id(imdb_id, self.tmdb_api_key, item_type)
                if tmdb_id_from_imdb:
                    logger.debug(f"  ➜ IMDb ID {imdb_id} 对应 TMDb ID: {tmdb_id_from_imdb}")
                    return str(tmdb_id_from_imdb)
                else:
                    logger.warning(f"  ➜ 无法通过IMDb ID {imdb_id} 查找到对应的TMDb ID。")
            except Exception as e:
                logger.error(f"  ➜ 通过IMDb ID查找TMDb ID时出错: {e}")
        return None
    
    def _extract_ids_from_title_or_line(self, title_line: str) -> Tuple[Optional[str], Optional[str]]:
        imdb_id = None
        tmdb_id = None
        imdb_match = re.search(r'(tt\d{7,8})', title_line, re.I)
        if imdb_match:
            imdb_id = imdb_match.group(1)
        tmdb_match = re.search(r'tmdb://(\d+)', title_line, re.I)
        if tmdb_match:
            tmdb_id = tmdb_match.group(1)
        return imdb_id, tmdb_id
    
    def _get_items_from_douban_doulist(self, url: str) -> List[Dict[str, str]]:
        """专门用于解析和分页获取豆瓣豆列内容的函数"""
        all_items = []
        # 从URL中移除分页参数，得到基础URL
        base_url = url.split('?')[0]
        page_start = 0
        # 设置一个最大页数限制，防止意外的无限循环
        max_pages = 50 
        items_per_page = 25

        logger.info(f"  ➜ 检测到豆瓣豆列链接，开始分页获取: {base_url}")

        for page in range(max_pages):
            current_start = page * items_per_page
            paginated_url = f"{base_url}?start={current_start}&sort=seq&playable=0&sub_type="
            
            try:
                logger.debug(f"    ➜ 正在获取第 {page + 1} 页: {paginated_url}")
                response = self.session.get(paginated_url, timeout=20)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'lxml')
                
                # 查找页面上所有的条目容器
                doulist_items = soup.find_all('div', class_='doulist-item')

                # 如果当前页没有找到任何条目，说明到达了最后一页
                if not doulist_items:
                    logger.info(f"  ➜ 在第 {page + 1} 页未发现更多项目，获取结束。")
                    break

                for item in doulist_items:
                    title_div = item.find('div', class_='title')
                    if not title_div: continue
                    
                    link_tag = title_div.find('a')
                    if not link_tag: continue
                    
                    # 提取标题
                    title = link_tag.get_text(strip=True)
                    # 提取豆瓣链接
                    douban_link = link_tag.get('href')
                    
                    # 尝试提取年份
                    year = None
                    abstract_div = item.find('div', class_='abstract')
                    if abstract_div:
                        # 年份通常在 abstract 内容中以 (YYYY) 或 YYYY-MM-DD 的形式出现
                        year_match = re.search(r'\b(19\d{2}|20\d{2})\b', abstract_div.get_text())
                        if year_match:
                            year = year_match.group(1)
                    
                    if title:
                        all_items.append({
                            'title': title,
                            'imdb_id': None, # 豆列页面不直接提供IMDb ID
                            'year': year,
                            'douban_link': douban_link # ✨ 关键信息：我们拿到了每个条目的豆瓣链接
                        })

            except Exception as e:
                logger.error(f"  ➜ 获取或解析豆瓣豆列页面 '{paginated_url}' 时出错: {e}")
                # 出现错误时，中断后续所有页面的获取
                break
        
        logger.info(f"  ➜ 豆瓣豆列获取完成，从 {page} 个页面中总共解析出 {len(all_items)} 个项目。")
        return all_items
    
    def _get_items_from_tmdb_list(self, url: str) -> List[Dict[str, str]]:
        """【新】专门用于解析和分页获取TMDb片单内容的函数"""
        match = re.search(r'themoviedb\.org/list/(\d+)', url)
        if not match:
            logger.error(f"  ➜ 无法从URL '{url}' 中解析出TMDb片单ID。")
            return []

        list_id = int(match.group(1))
        all_items = []
        current_page = 1
        total_pages = 1 # 先假设只有一页

        logger.info(f"  ➜ 检测到TMDb片单链接，开始分页获取: {url}")

        while current_page <= total_pages:
            try:
                logger.debug(f"    ➜ 正在获取第 {current_page} / {total_pages} 页...")
                list_data = tmdb_handler.get_list_details_tmdb(list_id, self.tmdb_api_key, page=current_page)

                if not list_data or not list_data.get('items'):
                    logger.warning(f"  ➜ 在第 {current_page} 页未发现更多项目，获取结束。")
                    break

                # 从第一页的返回结果中更新总页数
                if current_page == 1:
                    total_pages = list_data.get('total_pages', 1)

                for item in list_data['items']:
                    media_type = item.get('media_type')
                    tmdb_id = item.get('id')
                    
                    # 将TMDb的 'tv' 映射为我们系统内部的 'Series'
                    item_type_mapped = 'Series' if media_type == 'tv' else 'Movie'

                    if tmdb_id:
                        # ★★★ 直接生成包含精确ID和类型的字典，无需后续匹配 ★★★
                        all_items.append({'id': str(tmdb_id), 'type': item_type_mapped})

                current_page += 1

            except Exception as e:
                logger.error(f"  ➜ 获取或解析TMDb片单页面 {current_page} 时出错: {e}")
                break
        
        logger.info(f"  ➜ TMDb片单获取完成，从 {total_pages} 个页面中总共解析出 {len(all_items)} 个项目。")
        return all_items
    
    def _get_items_from_tmdb_discover(self, url: str) -> List[Dict[str, str]]:
        """【V4.1 - 最终确认版】专门用于解析TMDb Discover URL并获取结果的函数，支持自动分页获取所有项目"""
        from urllib.parse import urlparse, parse_qs
        from datetime import datetime, timedelta
        import re

        logger.info(f"  ➜ 检测到TMDb Discover链接，开始动态获取 (支持分页): {url}")
        
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        params = {k: v[0] for k, v in query_params.items()}

        today = datetime.now()
        date_pattern = re.compile(r'{today([+-]\d+)?}')

        for key, value in params.items():
            match = date_pattern.search(value)
            if match:
                offset_str = match.group(1) 
                target_date = today
                if offset_str:
                    days = int(offset_str)
                    target_date = today + timedelta(days=days)
                params[key] = value.replace(match.group(0), target_date.strftime('%Y-%m-%d'))

        all_items = []
        current_page = 1
        total_pages = 1
        MAX_PAGES_TO_FETCH = 10

        while current_page <= total_pages and current_page <= MAX_PAGES_TO_FETCH:
            try:
                params['page'] = current_page
                logger.debug(f"    ➜ 正在获取第 {current_page} / {total_pages} 页...")

                discover_data = None
                item_type_for_result = None

                # ★★★ 使用最健壮的判断逻辑 ★★★
                if '/discover/movie' in url:
                    discover_data = tmdb_handler.discover_movie_tmdb(self.tmdb_api_key, params)
                    item_type_for_result = 'Movie'
                elif '/discover/tv' in url:
                    discover_data = tmdb_handler.discover_tv_tmdb(self.tmdb_api_key, params)
                    item_type_for_result = 'Series'
                else:
                    # 如果URL格式意外，直接跳出循环
                    logger.warning(f"  ➜ 无法从URL '{url}' 判断是电影还是电视剧，discover任务中止。")
                    break

                if not discover_data or not discover_data.get('results'):
                    logger.info("    ➜ 在当前页未发现更多项目，获取结束。")
                    break

                if current_page == 1:
                    total_pages = discover_data.get('total_pages', 1)

                for item in discover_data['results']:
                    tmdb_id = item.get('id')
                    if tmdb_id and item_type_for_result:
                        all_items.append({'id': str(tmdb_id), 'type': item_type_for_result})
                
                current_page += 1

            except Exception as e:
                logger.error(f"  ➜ 获取或解析TMDb Discover链接的第 {current_page} 页时出错: {e}")
                break

        logger.info(f"  ➜ TMDb Discover 获取完成，从 {total_pages} 个页面中总共解析出 {len(all_items)} 个项目。")
        return all_items
    
    def _get_titles_and_imdbids_from_url(self, url: str) -> Tuple[List[Dict[str, str]], str]:
        source_type = 'list_rss' 
        items = []

        if 'themoviedb.org/discover/' in url:
            source_type = 'list_discover'
            items = self._get_items_from_tmdb_discover(url)
        elif 'themoviedb.org/list/' in url:
            source_type = 'list_tmdb'
            items = self._get_items_from_tmdb_list(url)
        elif 'douban.com/doulist' in url:
            source_type = 'list_douban'
            items = self._get_items_from_douban_doulist(url)
        else:
            logger.info(f"  ➜ 开始获取标准RSS榜单: {url}")
            try:
                response = self.session.get(url, timeout=20)
                response.raise_for_status()
                content = response.text
                if 'encoding="gb2312"' in content.lower():
                     content = response.content.decode('gb2312', errors='ignore')
                
                root = ET.fromstring(content)
                channel = root.find('channel')
                if channel is None: return [], source_type

                for item in channel.findall('item'):
                    title_elem = item.find('title')
                    guid_elem = item.find('guid')
                    link_elem = item.find('link')
                    description_elem = item.find('description')
                    
                    title = title_elem.text if title_elem is not None else None
                    description = description_elem.text if description_elem is not None else ''
                    
                    douban_link = None
                    if link_elem is not None and link_elem.text and 'douban.com' in link_elem.text:
                        douban_link = link_elem.text
                    elif guid_elem is not None and guid_elem.text and 'douban.com' in guid_elem.text:
                        douban_link = guid_elem.text

                    year = None
                    year_match = re.search(r'\b(20\d{2})\b', description)
                    if year_match: year = year_match.group(1)

                    imdb_id = None
                    if guid_elem is not None and guid_elem.text:
                        match = re.search(r'tt\d{7,8}', guid_elem.text)
                        if match: imdb_id = match.group(0)
                    if not imdb_id and link_elem is not None and link_elem.text:
                        match = re.search(r'tt\d{7,8}', link_elem.text)
                        if match: imdb_id = match.group(0)
                    
                    if title:
                        items.append({'title': title.strip(), 'imdb_id': imdb_id, 'year': year, 'douban_link': douban_link})
            except Exception as e:
                logger.error(f"从RSS URL '{url}' 获取榜单时出错: {e}")
        
        return items, source_type

    def _parse_series_title(self, title: str) -> Tuple[str, Optional[int]]:
        """
        (V4 - 兼容末尾数字版) 能够处理中英文季号混合及末尾数字的复杂标题。
        采用分步清理和回退匹配的策略，确保各种常见格式都能正确提取剧集名和季号。
        """
        show_name = title.strip()
        season_number = None

        # 定义英文、中文和纯数字的季号模式
        # 英文模式: "Name Season 2"
        SEASON_PATTERN_EN = re.compile(r'(.*?)\s+Season\s+(\d+)', re.IGNORECASE)
        # 中文模式: "名字 第一季" (使用类里已有的)
        SEASON_PATTERN_CN = self.SEASON_PATTERN
        # ★★★ 新增：纯数字结尾模式: "名字 2" 或 "名字2" ★★★
        SEASON_PATTERN_NUM = re.compile(r'^(.*?)\s*(\d+)$')

        # --- 步骤 1: 尝试解析并清理英文季号 ---
        match_en = SEASON_PATTERN_EN.search(show_name)
        if match_en:
            show_name = match_en.group(1).strip()
            season_number = int(match_en.group(2))
            logger.debug(f"  ➜ 标题解析 (英文部分): '{title}' -> 初步解析为名称='{show_name}', 季号='{season_number}'")

        # --- 步骤 2: 在上一步的结果上，继续尝试解析并清理中文季号 ---
        match_cn = SEASON_PATTERN_CN.search(show_name)
        if match_cn:
            show_name = match_cn.group(1).strip()
            if season_number is None:
                season_word = match_cn.group(2)
                season_number_from_cn = self.CHINESE_NUM_MAP.get(season_word)
                if season_number_from_cn:
                    season_number = season_number_from_cn
            logger.debug(f"  ➜ 标题解析 (中文部分): 清理后名称='{show_name}', 最终季号='{season_number}'")

        # --- 步骤 3: 如果以上都没有匹配到季号，则尝试匹配末尾的纯数字 ---
        if season_number is None:
            # 排除年份结尾的情况，例如 "Series Title 2023"
            if not re.search(r'\b(19|20)\d{2}$', show_name):
                match_num = SEASON_PATTERN_NUM.search(show_name)
                if match_num:
                    potential_name = match_num.group(1).strip()
                    potential_season = int(match_num.group(2))
                    
                    # 增加一个判断，避免把 "1984" 这样的标题错误解析
                    # 如果标题去掉数字后仍然有意义，我们才采纳它
                    if potential_name:
                        show_name = potential_name
                        season_number = potential_season
                        logger.debug(f"  ➜ 标题解析 (回退数字部分): '{title}' -> 解析为名称='{show_name}', 季号='{season_number}'")

        # 如果没有任何匹配，show_name就是原始标题, season_number是None
        if show_name == title.strip() and season_number is None:
            return title.strip(), None
            
        logger.debug(f"  ➜ 标题解析 (最终结果): '{title}' -> 名称='{show_name}', 季号='{season_number}'")
        return show_name, season_number

    def _match_title_to_tmdb(self, title: str, item_type: str, year: Optional[str] = None) -> Optional[Tuple[str, str]]:
        """
        【V3 - 剧集精确匹配最终版】
        - 增加了对剧集搜索结果的精确验证逻辑，避免因TMDb存在不规范条目（如 "剧名2"）时，错误地匹配到非基础剧集。
        - 统一了返回值格式为元组 (tmdb_id, item_type)。
        """
        def normalize_string(s: str) -> str:
            if not s: return ""
            # 移除了中文句号，增加了对更多分隔符的兼容
            return re.sub(r'[\s:：·\-*\'!,?.。]+', '', s).lower()

        if item_type == 'Movie':
            # --- 电影匹配逻辑保持不变 ---
            titles_to_try = set([title.strip()])
            match = re.match(r'([\u4e00-\u9fa5\s·0-9]+)[\s:：*]*(.*)', title.strip())
            if match:
                part1 = match.group(1).strip()
                part2 = match.group(2).strip()
                if part1: titles_to_try.add(part1)
                if part2: titles_to_try.add(part2)

            num_map = {'1': '一', '2': '二', '3': '三', '4': '四', '5': '五', '6': '六', '7': '七', '8': '八', '9': '九'}
            current_titles = list(titles_to_try) 
            for t in current_titles:
                if any(num in t for num in num_map.keys()):
                    new_title = t
                    for num, char in num_map.items():
                        new_title = new_title.replace(num, char)
                    titles_to_try.add(new_title)
            
            final_titles = list(titles_to_try)
            logger.debug(f"  ➜ 为 '{title}' 生成的最终候选搜索标题: {final_titles}")

            first_search_results = None
            year_info = f" (年份: {year})" if year else ""

            for title_variation in final_titles:
                if not title_variation: continue
                
                results = search_media(title_variation, self.tmdb_api_key, 'Movie', year=year)
                
                if first_search_results is None:
                    first_search_results = results

                if not results:
                    continue

                norm_variation = normalize_string(title_variation)

                for result in results:
                    norm_title = normalize_string(result.get('title'))
                    norm_original_title = normalize_string(result.get('original_title'))

                    if norm_variation == norm_title or norm_variation == norm_original_title:
                        tmdb_id = str(result.get('id'))
                        logger.info(f"  ➜ 电影标题 '{title}'{year_info} 通过【精确规范匹配】(使用'{title_variation}') 成功匹配到: {result.get('title')} (ID: {tmdb_id})")
                        return tmdb_id, 'Movie'
                
                for result in results:
                    norm_title = normalize_string(result.get('title'))
                    norm_original_title = normalize_string(result.get('original_title'))

                    if norm_variation in norm_title or norm_variation in norm_original_title:
                        tmdb_id = str(result.get('id'))
                        logger.info(f"  ➜ 电影标题 '{title}'{year_info} 通过【包含匹配】(使用'{title_variation}') 成功匹配到: {result.get('title')} (ID: {tmdb_id})")
                        return tmdb_id, 'Movie'

            if first_search_results:
                first_result = first_search_results[0]
                tmdb_id = str(first_result.get('id'))
                logger.warning(f"  ➜ 电影标题 '{title}'{year_info} 所有精确匹配和包含匹配均失败。将【回退使用】最相关的搜索结果: {first_result.get('title')} (ID: {tmdb_id})")
                return tmdb_id, 'Movie'

            logger.error(f"  ➜ 电影标题 '{title}'{year_info} 未能在TMDb上找到任何搜索结果。")
            return None
        
        elif item_type == 'Series':
            # ★★★ 核心修正区域开始 ★★★
            # 1. 无论如何，先解析标题，分离出基础剧名和可能的季号
            show_name, season_number_to_validate = self._parse_series_title(title)
            
            # 2. 使用解析出的干净剧名进行搜索
            results = search_media(show_name, self.tmdb_api_key, 'Series', year=year)

            if not results and year and season_number_to_validate is not None:
                logger.debug(f"  ➜ 带年份 '{year}' 搜索剧集 '{show_name}' 未找到结果，可能是后续季。尝试不带年份进行回退搜索...")
                results = search_media(show_name, self.tmdb_api_key, 'Series', year=None)

            if not results:
                year_info = f" (年份: {year})" if year else ""
                logger.warning(f"  ➜ 剧集标题 '{title}' (搜索词: '{show_name}'){year_info} 未能在TMDb上找到匹配项。")
                return None
            
            # 3. 【关键】遍历搜索结果，寻找精确匹配项，而不是直接用第一个
            series_result = None
            norm_show_name = normalize_string(show_name)
            
            for result in results:
                result_name = result.get('name', '')
                # 优先寻找规范化后完全一致的
                if normalize_string(result_name) == norm_show_name:
                    series_result = result
                    logger.debug(f"  ➜ 剧集 '{show_name}' 通过【精确规范匹配】找到了基础剧集: {result.get('name')} (ID: {result.get('id')})")
                    break # 找到了最完美的匹配，停止遍历
            
            # 4. 如果没有找到精确匹配，则回退到使用第一个（最相关）的结果
            if not series_result:
                series_result = results[0]
                logger.warning(f"  ➜ 剧集 '{show_name}' 未找到精确匹配项，将【回退使用】最相关的搜索结果: {series_result.get('name')} (ID: {series_result.get('id')})")

            series_id = str(series_result.get('id'))
            
            # 5. 后续逻辑保持不变：验证季号
            if season_number_to_validate is None:
                logger.debug(f"  ➜ 剧集标题 '{title}' 成功匹配到: {series_result.get('name')} (ID: {series_id})")
                return series_id, 'Series'
            
            logger.debug(f"  ➜ 剧集 '{show_name}' (ID: {series_id}) 已找到，正在验证是否存在第 {season_number_to_validate} 季...")
            series_details = get_tv_details(int(series_id), self.tmdb_api_key, append_to_response="seasons")
            if series_details and 'seasons' in series_details:
                for season in series_details['seasons']:
                    if season.get('season_number') == season_number_to_validate:
                        logger.info(f"  ➜ 剧集 '{show_name}' 存在第 {season_number_to_validate} 季。最终匹配ID为 {series_id}。")
                        return series_id, 'Series'
            
            logger.warning(f"  ➜ 验证失败！剧集 '{show_name}' (ID: {series_id}) 存在，但未找到第 {season_number_to_validate} 季。")
            return None
            # ★★★ 核心修正区域结束 ★★★
            
        return None

    def process(self, definition: Dict) -> Tuple[List[Dict[str, str]], str]:
        url = definition.get('url')
        source_type = 'list_rss'
        
        if not url:
            return [], source_type

        # ★★★ 核心修正：在这里直接处理猫眼逻辑 ★★★
        if url.startswith('maoyan://'):
            source_type = 'list_maoyan'
            logger.info(f"  ➜ 检测到猫眼榜单，将启动异步后台脚本...")
            # 使用 gevent 异步执行耗时的子进程调用
            greenlet = gevent.spawn(self._execute_maoyan_fetch, definition)
            # .get() 会等待 greenlet 执行完毕并返回结果
            tmdb_items = greenlet.get()
            return tmdb_items, source_type

        # --- 对于非猫眼榜单，保持原有逻辑不变 ---
        item_types = definition.get('item_type', ['Movie'])
        if isinstance(item_types, str): item_types = [item_types]
        limit = definition.get('limit')
        
        # ★★★ 核心修改 2/2: 接收 _get_titles_and_imdbids_from_url 返回的 source_type ★★★
        items, source_type = self._get_titles_and_imdbids_from_url(url)
        
        if not items: return [], source_type
        
        if items and 'id' in items[0] and 'type' in items[0]:
            logger.info(f"  ➜ 检测到来自TMDb源 ({source_type}) 的预匹配ID，将跳过标题匹配。")
            if limit and isinstance(limit, int) and limit > 0:
                items = items[:limit]
            return items, source_type # 直接返回结果和类型

        if limit and isinstance(limit, int) and limit > 0:
            items = items[:limit]
        
        tmdb_items = []
        douban_api = DoubanApi()

        with ThreadPoolExecutor(max_workers=5) as executor:
            def find_first_match(item: Dict[str, str], types_to_check):
                title = item.get('title')
                year = item.get('year')
                rss_imdb_id = item.get('imdb_id')
                douban_link = item.get('douban_link')

                cleaned_title_for_parsing = re.sub(r'^\s*\d+\.\s*', '', title)
                cleaned_title_for_parsing = re.sub(r'\s*\(\d{4}\)$', '', cleaned_title_for_parsing).strip()
                _, season_number = self._parse_series_title(cleaned_title_for_parsing)

                def create_result(tmdb_id, item_type):
                    result = {'id': tmdb_id, 'type': item_type}
                    if item_type == 'Series' and season_number is not None:
                        logger.debug(f"  ➜ 为剧集 '{title}' 附加季号: {season_number}")
                        result['season'] = season_number
                    return result

                if rss_imdb_id:
                    for item_type in types_to_check:
                        # _match_by_ids 只返回 tmdb_id，这部分逻辑正确
                        tmdb_id = self._match_by_ids(rss_imdb_id, None, item_type)
                        if tmdb_id:
                            logger.info(f"  ➜ 成功通过RSS自带的IMDb ID '{rss_imdb_id}' 匹配到 '{title}'。")
                            return create_result(tmdb_id, item_type)

                cleaned_title = re.sub(r'^\s*\d+\.\s*', '', title)
                cleaned_title = re.sub(r'\s*\(\d{4}\)$', '', cleaned_title).strip()
                for item_type in types_to_check:
                    # _match_title_to_tmdb 现在返回 (tmdb_id, item_type) 或 None
                    match_result = self._match_title_to_tmdb(cleaned_title, item_type, year=year)
                    if match_result:
                        tmdb_id, matched_type = match_result
                        return create_result(tmdb_id, matched_type)
                
                if douban_link:
                    logger.info(f"  ➜ 片名+年份匹配 '{title}' 失败，启动备用方案：通过豆瓣链接获取更多信息...")
                    douban_details = douban_api.get_details_from_douban_link(douban_link, mtype=types_to_check[0] if types_to_check else None)
                    
                    if douban_details:
                        imdb_id_from_douban = douban_details.get("imdb_id")
                        if not imdb_id_from_douban and douban_details.get("attrs", {}).get("imdb"):
                            imdb_ids = douban_details["attrs"]["imdb"]
                            if isinstance(imdb_ids, list) and len(imdb_ids) > 0:
                                imdb_id_from_douban = imdb_ids[0]

                        if imdb_id_from_douban:
                            logger.info(f"  ➜ 豆瓣备用方案(3a)成功！拿到IMDb ID: {imdb_id_from_douban}，现在用它匹配TMDb...")
                            for item_type in types_to_check:
                                tmdb_id = self._match_by_ids(imdb_id_from_douban, None, item_type)
                                if tmdb_id:
                                    return create_result(tmdb_id, item_type)
                        
                        logger.info(f"  ➜ 豆瓣备用方案(3a)失败，尝试方案(3b): 使用 original_title...")
                        original_title = douban_details.get("original_title")
                        if original_title:
                            for item_type in types_to_check:
                                match_result = self._match_title_to_tmdb(original_title, item_type, year=year)
                                if match_result:
                                    tmdb_id, matched_type = match_result
                                    logger.info(f"  ➜ 豆瓣备用方案(3b)成功！通过 original_title '{original_title}' 匹配成功。")
                                    return create_result(tmdb_id, matched_type)

                logger.debug(f"  ➜ 所有优先方案均失败，尝试不带年份进行最后的回退搜索: '{title}'")
                for item_type in types_to_check:
                    match_result = self._match_title_to_tmdb(cleaned_title, item_type, year=None)
                    if match_result:
                        tmdb_id, matched_type = match_result
                        logger.warning(f"  ➜ 注意：'{title}' 在最后的回退搜索中匹配成功，但年份可能不准。")
                        return create_result(tmdb_id, matched_type)

                logger.error(f"  ➜ 彻底失败：所有方案都无法为 '{title}' 找到匹配项。")
                return None

            results_in_order = executor.map(lambda item: find_first_match(item, item_types), items)
            tmdb_items = [result for result in results_in_order if result is not None]
        
        douban_api.close()
        logger.info(f"  ➜ RSS匹配完成，成功获得 {len(tmdb_items)} 个TMDb项目。")
        
        unique_items = list({f"{item['type']}-{item['id']}-{item.get('season')}": item for item in tmdb_items}.values())
        return unique_items, source_type

class FilterEngine:
    """
    【V4 - PG JSON 兼容最终版】
    - 修复了 _item_matches_rules 方法中因 psycopg2 自动解析 JSON 字段而导致的 TypeError。
    - 移除了所有对 _json 字段的多余 json.loads() 调用，解决了筛选规则静默失效的问题。
    """
    def __init__(self):
        self.airing_series_ids = None

    def _get_airing_ids(self): # ◀◀◀ 函数名也改了
        """辅助函数，带缓存地获取连载中ID"""
        if self.airing_series_ids is None:
            logger.debug("  ➜ 筛选引擎：首次需要“连载中”数据，正在从数据库查询...")
            self.airing_series_ids = watchlist_db.get_airing_series_tmdb_ids()
            logger.debug(f"  ➜ 缓存了 {len(self.airing_series_ids)} 个“连载中”剧集ID。")
        return self.airing_series_ids

    def _item_matches_rules(self, item_metadata: Dict[str, Any], rules: List[Dict[str, Any]], logic: str) -> bool:
        if not rules: return True
        
        results = []
        for rule in rules:
            field, op, value = rule.get("field"), rule.get("operator"), rule.get("value")
            match = False
            
            if field in ['actors', 'directors']:
                # ★★★ 核心修改 ★★★
                if not isinstance(value, list):
                    # 如果 value 格式不对（比如是旧的字符串格式），直接跳过
                    results.append(False)
                    continue

                # 1. 直接从规则的 value 中提取出 TMDB ID 列表
                person_tmdb_ids_from_rule = [str(p['id']) for p in value if isinstance(p, dict) and 'id' in p]

                if not person_tmdb_ids_from_rule:
                    results.append(False)
                    continue

                # 2. 获取当前媒体项中所有演员/导演的 TMDB ID 集合 
                item_person_list = item_metadata.get(f"{field}_json")
                if item_person_list:
                    try:
                        # ★★★ 核心修改：只取前三位演员/导演进行匹配 ★★★
                        top_persons = item_person_list[:3]
                        
                        # 如果是演员且总数超过3，可以加个日志方便调试
                        if field == 'actors' and len(item_person_list) > 3:
                            logger.trace(f"  ➜ 演员筛选优化：媒体项《{item_metadata.get('title')}》有 {len(item_person_list)} 位演员，仅匹配前 3 位。")

                        item_person_tmdb_ids = set()
                        # 使用截断后的 top_persons 列表进行遍历
                        for p in top_persons:
                            person_id = p.get('tmdb_id') or p.get('id')
                            if person_id is not None:
                                item_person_tmdb_ids.add(str(person_id))
                        
                        if op == 'is_one_of' or op == 'contains':
                            if any(rule_id in item_person_tmdb_ids for rule_id in person_tmdb_ids_from_rule):
                                match = True
                        elif op == 'is_none_of':
                            if not any(rule_id in item_person_tmdb_ids for rule_id in person_tmdb_ids_from_rule):
                                match = True
                    except (TypeError, KeyError):
                        logger.warning(f"  ➜ 处理 {field}_json 时遇到意外的格式错误，内容: {item_person_list}")

            # 2. 检查字段是否为“字符串列表”（类型/国家/工作室/标签）
            elif field in ['genres', 'countries', 'studios', 'tags']:
                # ★★★ 核心修复 2/2：直接使用已经是列表的 _json 字段 ★★★
                item_value_list = item_metadata.get(f"{field}_json")
                if item_value_list:
                    try:
                        # 不再需要 json.loads()
                        if op == 'is_one_of':
                            if isinstance(value, list) and any(v in item_value_list for v in value):
                                match = True
                        elif op == 'is_none_of':
                            if isinstance(value, list) and not any(v in item_value_list for v in value):
                                match = True
                        elif op == 'contains':
                            if value in item_value_list:
                                match = True
                    except TypeError:
                        logger.warning(f"处理 {field}_json 时遇到意外的类型错误，内容: {item_value_list}")

            # 3. 处理其他所有非列表字段 (这部分逻辑是正确的，无需修改)
            elif field in ['release_date', 'date_added']:
                item_date_val = item_metadata.get(field)
                if item_date_val and str(value).isdigit():
                    try:
                        # 兼容处理 datetime 和 date 类型
                        if isinstance(item_date_val, datetime):
                            item_date = item_date_val.date()
                        elif isinstance(item_date_val, date):
                            item_date = item_date_val
                        else:
                            # 兼容字符串格式
                            item_date = datetime.strptime(str(item_date_val), '%Y-%m-%d').date()

                        today = datetime.now().date()
                        days = int(value)
                        cutoff_date = today - timedelta(days=days)

                        if op == 'in_last_days':
                            if cutoff_date <= item_date <= today:
                                match = True
                        elif op == 'not_in_last_days':
                            if item_date < cutoff_date:
                                match = True
                    except (ValueError, TypeError):
                        pass

            # 4. 处理分级字段
            elif field == 'unified_rating':
                item_unified_rating = item_metadata.get('unified_rating')
                if item_unified_rating:
                    if op == 'is_one_of':
                        if isinstance(value, list) and item_unified_rating in value:
                            match = True
                    elif op == 'is_none_of':
                        if isinstance(value, list) and item_unified_rating not in value:
                            match = True
                    elif op == 'eq':
                        if str(value) == item_unified_rating:
                            match = True

            # 5. 处理连载剧集
            elif field == 'is_in_progress':
                if item_metadata.get('item_type') == 'Series':
                    airing_ids = self._get_airing_ids()
                    is_item_airing = str(item_metadata.get('tmdb_id')) in airing_ids

                    if (op == 'is' and value is True) or (op == 'is_not' and value is False):
                        if is_item_airing:
                            match = True
                    elif (op == 'is' and value is False) or (op == 'is_not' and value is True):
                        if not is_item_airing:
                            match = True

            elif field == 'title':
                item_title = item_metadata.get('title')
                if item_title and isinstance(value, str):
                    item_title_lower = item_title.lower()
                    value_lower = value.lower()
                    if op == 'contains':
                        if value_lower in item_title_lower: match = True
                    elif op == 'does_not_contain':
                        if value_lower not in item_title_lower: match = True
                    elif op == 'starts_with':
                        if item_title_lower.startswith(value_lower): match = True
                    elif op == 'ends_with':
                        if item_title_lower.endswith(value_lower): match = True
            
            else:
                actual_item_value = item_metadata.get(field)
                if actual_item_value is not None:
                    try:
                        if op == 'gte' and float(actual_item_value) >= float(value): match = True
                        elif op == 'lte' and float(actual_item_value) <= float(value): match = True
                        elif op == 'eq' and str(actual_item_value) == str(value): match = True
                    except (ValueError, TypeError): pass

            results.append(match)

        if logic.upper() == 'AND': return all(results)
        else: return any(results)

    def execute_filter(self, definition: Dict[str, Any]) -> List[Dict[str, str]]:
        logger.info("  ➜ 筛选引擎：开始执行合集生成...")
        rules = definition.get('rules', [])
        logic = definition.get('logic', 'AND')
        item_types_to_process = definition.get('item_type', ['Movie'])
        if isinstance(item_types_to_process, str):
            item_types_to_process = [item_types_to_process]
        if not rules:
            logger.warning("  ➜ 合集定义中没有任何规则，将返回空列表。")
            return []

        # ★★★ 核心修改：根据定义判断数据源 ★★★
        library_ids = definition.get('library_ids') # 在新版UI中，这个字段叫 target_library_ids，但我们兼容旧的
        if not library_ids:
            library_ids = definition.get('target_library_ids')

        all_media_metadata = []

        if library_ids and isinstance(library_ids, list) and len(library_ids) > 0:
            # --- 分支1：从指定的媒体库加载数据 ---
            logger.info(f"  ➜ 已指定 {len(library_ids)} 个媒体库作为筛选范围。")
            
            cfg = config_manager.APP_CONFIG
            emby_url = cfg.get('emby_server_url')
            emby_key = cfg.get('emby_api_key')
            emby_user_id = cfg.get('emby_user_id')

            if not all([emby_url, emby_key, emby_user_id]):
                logger.error("  ➜ Emby服务器配置不完整，无法从指定媒体库筛选。")
                return []

            emby_items = emby_handler.get_emby_library_items(
                base_url=emby_url, api_key=emby_key, user_id=emby_user_id,
                library_ids=library_ids, media_type_filter=",".join(item_types_to_process)
            )

            if not emby_items:
                logger.warning("  ➜ 从指定的媒体库中未能获取到任何媒体项。")
                return []

            tmdb_ids_from_libs = [
                item['ProviderIds']['Tmdb']
                for item in emby_items
                if item.get('ProviderIds', {}).get('Tmdb')
            ]

            if not tmdb_ids_from_libs:
                logger.warning("  ➜ 指定媒体库中的项目均缺少TMDb ID，无法进行筛选。")
                return []
            
            logger.info(f"  ➜ 正在从本地缓存中查询这 {len(tmdb_ids_from_libs)} 个项目的元数据...")
            for item_type in item_types_to_process:
                metadata_for_type = collection_db.get_media_metadata_by_tmdb_ids(tmdb_ids_from_libs, item_type)
                all_media_metadata.extend(metadata_for_type)

        else:
            # --- 分支2：保持原有逻辑，扫描全库 ---
            logger.info("  ➜ 未指定媒体库，将扫描所有媒体库的元数据缓存...")
            for item_type in item_types_to_process:
                all_media_metadata.extend(collection_db.get_all_media_metadata(item_type=item_type))

        # --- 后续的筛选逻辑保持不变 ---
        matched_items = []
        if not all_media_metadata:
            logger.warning("  ➜ 未能加载任何媒体元数据进行筛选。")
            return []
        
        logger.info(f"  ➜ 已加载 {len(all_media_metadata)} 条元数据，开始应用筛选规则...")
        for media_metadata in all_media_metadata:
            if self._item_matches_rules(media_metadata, rules, logic):
                tmdb_id = media_metadata.get('tmdb_id')
                item_type = media_metadata.get('item_type')
                if tmdb_id and item_type:
                    matched_items.append({'id': str(tmdb_id), 'type': item_type})
                    
        unique_items = list({f"{item['type']}-{item['id']}": item for item in matched_items}.values())
        logger.info(f"  ➜ 筛选完成！共找到 {len(unique_items)} 部匹配的媒体项目。")
        return unique_items
    
    def find_matching_collections(self, item_metadata: Dict[str, Any], media_library_id: Optional[str] = None) -> List[Dict[str, Any]]:
        media_item_type = item_metadata.get('item_type')
        media_type_cn = "剧集" if media_item_type == "Series" else "影片"
        logger.info(f"  ➜ 正在为{media_type_cn}《{item_metadata.get('title')}》实时匹配自定义合集...")
        matched_collections = []
        all_filter_collections = [
            c for c in collection_db.get_all_custom_collections() 
            if c['type'] == 'filter' and c['status'] == 'active' and c['emby_collection_id']
        ]
        if not all_filter_collections:
            logger.debug("  ➜ 没有发现任何已启用的筛选类合集，跳过匹配。")
            return []
        for collection_def in all_filter_collections:
            try:
                definition = collection_def['definition_json']
                defined_library_ids = definition.get('library_ids')
                if defined_library_ids and media_library_id and media_library_id not in defined_library_ids:
                    logger.debug(f"  ➜ 跳过合集《{collection_def['name']}》，因为媒体库不匹配 (合集要求: {defined_library_ids}, 实际来自: '{media_library_id}')。")
                    continue 
                collection_item_types = definition.get('item_type', ['Movie'])
                if isinstance(collection_item_types, str):
                    collection_item_types = [collection_item_types]
                if media_item_type not in collection_item_types:
                    logger.debug(f"  ➜ 跳过合集《{collection_def['name']}》，因为内容类型不匹配 (合集需要: {collection_item_types}, 实际是: '{media_item_type}')。")
                    continue
                rules = definition.get('rules', [])
                logic = definition.get('logic', 'AND')
                if self._item_matches_rules(item_metadata, rules, logic):
                    logger.info(f"  ➜ 匹配成功！{media_type_cn}《{item_metadata.get('title')}》属于合集《{collection_def['name']}》。")
                    matched_collections.append({
                        'id': collection_def['id'],
                        'name': collection_def['name'],
                        'emby_collection_id': collection_def['emby_collection_id']
                    })
            except TypeError as e:
                logger.warning(f"  ➜ 解析合集《{collection_def['name']}》的定义时出错: {e}，跳过。")
                continue
        return matched_collections
