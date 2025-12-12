# handler/custom_collection.py
import logging
import requests
import xml.etree.ElementTree as ET
import re
import os
import time
import numpy as np
import sys
import gevent
from typing import List, Dict, Any, Optional, Tuple
import json
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
from gevent import subprocess, Timeout

import handler.tmdb as tmdb
import handler.emby as emby
import utils
import config_manager
from tasks.helpers import parse_series_title_and_season
from database import collection_db, watchlist_db, media_db, connection
from handler.douban import DoubanApi
from handler.tmdb import search_media, get_tv_details
from ai_translator import AITranslator

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
                tmdb_id_from_imdb = tmdb.get_tmdb_id_by_imdb_id(imdb_id, self.tmdb_api_key, item_type)
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
        """专门用于解析和分页获取TMDb片单内容的函数"""
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
                list_data = tmdb.get_list_details_tmdb(list_id, self.tmdb_api_key, page=current_page)

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

                    title = item.get('title') if item_type_mapped == 'Movie' else item.get('name')

                    if tmdb_id:
                        all_items.append({
                            'id': str(tmdb_id), 
                            'type': item_type_mapped,
                            'title': title # 新增字段
                        })

                current_page += 1

            except Exception as e:
                logger.error(f"  ➜ 获取或解析TMDb片单页面 {current_page} 时出错: {e}")
                break
        
        logger.info(f"  ➜ TMDb片单获取完成，从 {total_pages} 个页面中总共解析出 {len(all_items)} 个项目。")
        return all_items
    
    def _get_items_from_tmdb_discover(self, url: str) -> List[Dict[str, str]]:
        """专门用于解析TMDb Discover URL并获取结果的函数，支持自动分页并过滤无海报/无中文元数据的项目"""
        from urllib.parse import urlparse, parse_qs
        from datetime import datetime, timedelta
        import re

        logger.info(f"  ➜ 检测到TMDb Discover链接，开始动态获取 (支持分页和过滤): {url}")
        
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

                if '/discover/movie' in url:
                    discover_data = tmdb.discover_movie_tmdb(self.tmdb_api_key, params)
                    item_type_for_result = 'Movie'
                elif '/discover/tv' in url:
                    discover_data = tmdb.discover_tv_tmdb(self.tmdb_api_key, params)
                    item_type_for_result = 'Series'
                else:
                    logger.warning(f"  🚫 无法从URL '{url}' 判断是电影还是电视剧，discover任务中止。")
                    break

                if not discover_data or not discover_data.get('results'):
                    logger.info("    ➜ 在当前页未发现更多项目，获取结束。")
                    break

                if current_page == 1:
                    total_pages = discover_data.get('total_pages', 1)

                for item in discover_data['results']:
                    # 筛选条件 1: 必须有海报 (poster_path不为None或空)
                    if not item.get('poster_path'):
                        logger.debug(f"  ➜ 筛选TMDb Discover结果：跳过项目 '{item.get('title') or item.get('name')}' (ID: {item.get('id')})，因为它没有海报。")
                        continue

                    # 筛选条件 2: 必须有中文元数据 (以overview字段不为空作为判断依据)
                    # TMDB API在指定language=zh-CN时，若无中文简介，此字段通常为空
                    if not item.get('overview'):
                        logger.debug(f"  ➜ 筛选TMDb Discover结果：跳过项目 '{item.get('title') or item.get('name')}' (ID: {item.get('id')})，因为它没有中文简介。")
                        continue
                    
                    tmdb_id = item.get('id')
                    if tmdb_id and item_type_for_result:
                        title = item.get('title') if item_type_for_result == 'Movie' else item.get('name')
                        date_str = item.get('release_date') if item_type_for_result == 'Movie' else item.get('first_air_date')
                        year = date_str[:4] if date_str else None
                        all_items.append({
                            'id': str(tmdb_id), 
                            'type': item_type_for_result,
                            'title': title,
                            'release_date': date_str, 
                            'year': year
                        })
                
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

    def _match_title_to_tmdb(self, title: str, item_type: str, year: Optional[str] = None) -> Optional[Tuple[str, str, Optional[int]]]:
        """
        - 增加了对剧集搜索结果的精确验证逻辑，避免因TMDb存在不规范条目（如 "剧名2"）时，错误地匹配到非基础剧集。
        - 统一了返回值格式为元组 (tmdb_id, item_type, season_number)。
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
                        return tmdb_id, 'Movie', None
                
                for result in results:
                    norm_title = normalize_string(result.get('title'))
                    norm_original_title = normalize_string(result.get('original_title'))

                    if norm_variation in norm_title or norm_variation in norm_original_title:
                        tmdb_id = str(result.get('id'))
                        logger.info(f"  ➜ 电影标题 '{title}'{year_info} 通过【包含匹配】(使用'{title_variation}') 成功匹配到: {result.get('title')} (ID: {tmdb_id})")
                        return tmdb_id, 'Movie', None

            if first_search_results:
                first_result = first_search_results[0]
                tmdb_id = str(first_result.get('id'))
                logger.warning(f"  ➜ 电影标题 '{title}'{year_info} 所有精确匹配和包含匹配均失败。将【回退使用】最相关的搜索结果: {first_result.get('title')} (ID: {tmdb_id})")
                return tmdb_id, 'Movie', None

            logger.error(f"  ➜ 电影标题 '{title}'{year_info} 未能在TMDb上找到任何搜索结果。")
            return None
        
        elif item_type == 'Series':
            # 1. 解析标题
            show_name_parsed, season_number_to_validate = parse_series_title_and_season(title, api_key=self.tmdb_api_key)
            show_name = show_name_parsed if show_name_parsed else title
            
            # 2. 搜索
            results = search_media(show_name, self.tmdb_api_key, 'Series', year=year)

            # 回退搜索逻辑 
            if not results and year and season_number_to_validate is not None:
                logger.debug(f"  ➜ 带年份 '{year}' 搜索剧集 '{show_name}' 未找到结果，可能是后续季。尝试不带年份进行回退搜索...")
                results = search_media(show_name, self.tmdb_api_key, 'Series', year=None)

            if not results:
                year_info = f" (年份: {year})" if year else ""
                logger.warning(f"  ➜ 剧集标题 '{title}' (搜索词: '{show_name}'){year_info} 未能在TMDb上找到匹配项。")
                return None
            
            # 情况 A: 不需要验证季号 (直接找最像的)
            if season_number_to_validate is None:
                series_result = None
                norm_show_name = normalize_string(show_name)
                
                # 优先找精确匹配
                for result in results:
                    if normalize_string(result.get('name', '')) == norm_show_name:
                        series_result = result
                        logger.debug(f"  ➜ 剧集 '{show_name}' 通过【精确规范匹配】找到了: {result.get('name')} (ID: {result.get('id')})")
                        break 
                
                # 没找到精确的就用第一个
                if not series_result:
                    series_result = results[0]
                    logger.warning(f"  ➜ 剧集 '{show_name}' 未找到精确匹配，使用首个结果: {series_result.get('name')} (ID: {series_result.get('id')})")

                return str(series_result.get('id')), 'Series', None

            # 情况 B: 需要验证季号 (遍历前 5 个结果，谁有这一季就算谁的)
            else:
                # 定义一个内部函数来执行验证逻辑，避免代码重复
                def verify_season_in_results(candidates_list, source_desc=""):
                    if not candidates_list:
                        return None
                    
                    # 排序：名字完全匹配的排在最前面
                    norm_show_name = normalize_string(show_name)
                    candidates_list.sort(key=lambda x: 0 if normalize_string(x.get('name', '')) == norm_show_name else 1)
                    
                    logger.info(f"  ➜ 剧集 '{show_name}'{source_desc} 需要验证第 {season_number_to_validate} 季，正在扫描 {len(candidates_list)} 个候选结果...")

                    for candidate in candidates_list:
                        candidate_id = str(candidate.get('id'))
                        candidate_name = candidate.get('name')
                        
                        # 获取该剧集的详情（包含季信息）
                        series_details = get_tv_details(int(candidate_id), self.tmdb_api_key, append_to_response="seasons")
                        
                        if series_details and 'seasons' in series_details:
                            # 检查是否有目标季
                            has_season = False
                            for season in series_details['seasons']:
                                if season.get('season_number') == season_number_to_validate:
                                    has_season = True
                                    break
                            
                            if has_season:
                                logger.info(f"  ➜ 匹配成功！在候选结果 '{candidate_name}' (ID: {candidate_id}) 中找到了第 {season_number_to_validate} 季。")
                                return candidate_id
                            else:
                                logger.debug(f"    - 候选 '{candidate_name}' (ID: {candidate_id}) 没有第 {season_number_to_validate} 季，跳过。")
                    return None

                # 1. 第一次尝试：使用当前的搜索结果（可能带年份）
                matched_id = verify_season_in_results(results[:5])
                if matched_id:
                    return matched_id, 'Series', season_number_to_validate

                # 2. 如果带年份验证失败，尝试去掉年份重搜 
                if year:
                    logger.info(f"  ➜ 剧集 '{show_name}' 带年份 ({year}) 搜索结果中未找到第 {season_number_to_validate} 季，尝试移除年份重搜...")
                    results_no_year = search_media(show_name, self.tmdb_api_key, 'Series', year=None)
                    
                    if results_no_year:
                        # 排除掉已经在第一次搜索中验证过的ID，避免重复API请求
                        checked_ids = set(str(r.get('id')) for r in results[:5])
                        candidates_no_year = [r for r in results_no_year if str(r.get('id')) not in checked_ids][:5]
                        
                        if candidates_no_year:
                            matched_id = verify_season_in_results(candidates_no_year, source_desc=" (无年份重搜)")
                            if matched_id:
                                return matched_id, 'Series', season_number_to_validate

                # 3. 如果循环完了都没找到
                logger.warning(f"  ➜ 验证失败！在 '{show_name}' 的所有搜索结果中，均未找到第 {season_number_to_validate} 季。")
                    
                # ==============================================================================
                # ★★★ 兜底回退机制 ★★★
                # 如果解析后的搜索+验证失败了，尝试用“原始标题”直接搜一次
                # ==============================================================================
                if show_name != title:
                    logger.info(f"  ➜ [兜底机制] 尝试使用原始标题 '{title}' 进行回退搜索...")
                    fallback_results = search_media(title, self.tmdb_api_key, 'Series', year=None)
                    
                    if fallback_results:
                        # 如果原始标题能搜到结果，通常第一个就是最匹配的
                        # 这种情况通常发生在：解析器错误地把剧名的一部分当成了季号
                        # 例如："Love 101" 被解析成了 "Love" 第 101 季，验证失败 -> 回退搜 "Love 101" -> 成功
                        best_match = fallback_results[0]
                        logger.info(f"  ➜ [兜底成功] 原始标题 '{title}' 匹配到了: {best_match.get('name')} (ID: {best_match.get('id')})")
                        return str(best_match.get('id')), 'Series', None
            
            return None
                
        return None

    def process(self, definition: Dict) -> Tuple[List[Dict[str, str]], str]:
        raw_url = definition.get('url')
        urls = []
        if isinstance(raw_url, list):
            urls = [u for u in raw_url if u]
        elif isinstance(raw_url, str) and raw_url:
            urls = [raw_url]
            
        if not urls: return [], 'empty'
        
        all_items = []
        last_source_type = 'mixed'
        
        # 循环调用旧逻辑
        total_urls = len(urls)
        for i, url in enumerate(urls):
            # 构造临时定义，只包含单个 URL
            temp_def = definition.copy()
            temp_def['url'] = url
            
            # 调用原逻辑
            items, source_type = self._process_single_url(url, temp_def)
            all_items.extend(items)
            last_source_type = source_type
            
            # ★★★ 新增：多榜单间的防封控休眠 ★★★
            # 如果当前是猫眼链接，且不是列表中的最后一个，则强制休眠
            if isinstance(url, str) and url.startswith('maoyan://'):
                if i < total_urls - 1:
                    logger.info(f"  ➜ [防封控] 单个猫眼榜单采集完毕，为安全起见，强制休眠 10 秒后再采集下一个...")
                    time.sleep(10)
            
        # 统一去重
        unique_items = []
        seen_keys = set()
        for item in all_items:
            tmdb_id = item.get('id')
            item_type = item.get('type')
            title = item.get('title')
            season = item.get('season')
            
            if tmdb_id:
                key = f"{item_type}-{tmdb_id}-{season}"
            else:
                key = f"unidentified-{title}"
            
            if key not in seen_keys:
                seen_keys.add(key)
                unique_items.append(item)
        
        # 统一限制数量
        limit = definition.get('limit')
        if limit and isinstance(limit, int) and limit > 0:
            unique_items = unique_items[:limit]
            
        # ==================== ★★★ AI 过滤插件 (最终版) ★★★ ====================
        if definition.get('ai_enabled') and definition.get('ai_prompt'):
            ai_prompt = definition.get('ai_prompt')
            logger.info(f"  ➜ [AI审阅] 检测到 AI 选片指令，正在筛选 {len(unique_items)} 个候选项目...")
            
            try:
                # 1. 实例化 AI (注意这里必须是 APP_CONFIG)
                translator = AITranslator(config_manager.APP_CONFIG)
                
                # 2. 准备精简数据 (关键：一定要带上 release_date 和 year)
                candidates_for_ai = []
                for item in unique_items:
                    candidates_for_ai.append({
                        "id": str(item.get('id')),
                        "title": item.get('title'),
                        "type": item.get('type'),
                        "year": item.get('year'),                 # <--- 别漏了
                        "release_date": item.get('release_date')  # <--- 核心！别漏了
                    })
                
                # 3. 调用 AI 过滤
                filtered_ids = translator.filter_candidates(
                    candidates=candidates_for_ai, 
                    user_instruction=ai_prompt
                )
                
                # 4. 应用过滤结果
                if filtered_ids:
                    original_count = len(unique_items)
                    unique_items = [item for item in unique_items if str(item.get('id')) in filtered_ids]
                    logger.info(f"  ➜ [AI审阅] 完成。从 {original_count} 部中筛选出 {len(unique_items)} 部。")
                else:
                    logger.warning("  ➜ [AI审阅] AI 返回了空列表或过滤失败，将保留原始列表。")
            
            except Exception as e_ai:
                logger.error(f"  ➜ [AI审阅] 执行过程中发生错误，跳过筛选: {e_ai}", exc_info=True)
        # ===================================================================
        
        return unique_items, last_source_type

    def _process_single_url(self, url: str, definition: Dict) -> Tuple[List[Dict[str, str]], str]:
        definition = definition.copy()
        definition['url'] = url
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
                original_source_title = item.get('title', '').strip()
                year = item.get('year')
                rss_imdb_id = item.get('imdb_id')
                douban_link = item.get('douban_link')

                # ★★★ 核心修改 1：定义一个包含原始标题的辅助函数 ★★★
                def create_result(tmdb_id, item_type, confirmed_season=None):
                    result = {
                        'id': tmdb_id, 
                        'type': item_type, 
                        'title': original_source_title,
                        'year': year
                    }
                    # 只有当传入了有效的季号时，才添加
                    if item_type == 'Series' and confirmed_season is not None:
                        result['season'] = confirmed_season
                    return result

                # ★★★ 核心修改 2：默认的 fallback 也要带上原始标题 ★★★
                fallback_result = {
                    'id': None, 
                    'type': types_to_check[0] if types_to_check else 'Movie', 
                    'title': original_source_title,
                    'year': year
                }

                # 1. 尝试 IMDb ID
                if rss_imdb_id:
                    for item_type in types_to_check:
                        tmdb_id = self._match_by_ids(rss_imdb_id, None, item_type)
                        if tmdb_id:
                            _, s_num = parse_series_title_and_season(original_source_title, api_key=self.tmdb_api_key)
                            return create_result(tmdb_id, item_type, s_num)

                # 2. 尝试 标题匹配
                cleaned_title = re.sub(r'^\s*\d+\.\s*', '', original_source_title)
                cleaned_title = re.sub(r'\s*\(\d{4}\)$', '', cleaned_title).strip()
                
                for item_type in types_to_check:
                    match_result = self._match_title_to_tmdb(cleaned_title, item_type, year=year)
                    
                    if match_result:
                        tmdb_id, matched_type, matched_season = match_result
                        return create_result(tmdb_id, matched_type, matched_season)
                
                # 3. 尝试 豆瓣链接
                if douban_link:
                    logger.info(f"  ➜ 片名+年份匹配 '{original_source_title}' 失败，启动备用方案：通过豆瓣链接获取更多信息...")
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
                                    tmdb_id, matched_type, matched_season = match_result
                                    logger.info(f"  ➜ 豆瓣备用方案(3b)成功！通过 original_title '{original_title}' 匹配成功。")
                                    return create_result(tmdb_id, matched_type, matched_season)

                logger.debug(f"  ➜ 所有优先方案均失败，尝试不带年份进行最后的回退搜索: '{original_source_title}'")
                for item_type in types_to_check:
                    match_result = self._match_title_to_tmdb(cleaned_title, item_type, year=None)
                    if match_result:
                        tmdb_id, matched_type, matched_season = match_result
                        logger.warning(f"  ➜ 注意：'{original_source_title}' 在最后的回退搜索中匹配成功，但年份可能不准。")
                        return create_result(tmdb_id, matched_type, matched_season)

                logger.error(f"  ➜ 彻底失败：所有方案都无法为 '{original_source_title}' 找到匹配项。")
                return fallback_result

            results_in_order = executor.map(lambda item: find_first_match(item, item_types), items)
            tmdb_items = [result for result in results_in_order if result is not None]
        
        douban_api.close()
        logger.info(f"  ➜ RSS匹配完成，成功获得 {len(tmdb_items)} 个TMDb项目。")
        
        unique_items = []
        seen_keys = set()
        
        for item in tmdb_items:
            tmdb_id = item.get('id')
            item_type = item.get('type')
            title = item.get('title')
            season = item.get('season')
            
            if tmdb_id:
                # 1. 如果有 ID，优先用 ID + 季号去重 (防止同一剧集不同季被去重)
                # 例如: Series-12345-1, Series-12345-2
                key = f"{item_type}-{tmdb_id}-{season}"
            else:
                # 2. 如果没有 ID，必须用 标题 去重！
                # 例如: unidentified-怪奇物语 第五季
                # 这样《怪奇物语》和《黑袍纠察队》就不会因为都是 None 而打架了
                key = f"unidentified-{title}"
            
            if key not in seen_keys:
                seen_keys.add(key)
                unique_items.append(item)
                
        logger.info(f"  ➜ 去重后剩余 {len(unique_items)} 个有效项目。")

        return unique_items, source_type

class FilterEngine:
    """
    【V4 - PG JSON 兼容最终版】
    - 修复了 _item_matches_rules 方法中因 psycopg2 自动解析 JSON 字段而导致的 TypeError。
    - 移除了所有对 _json 字段的多余 json.loads() 调用，解决了筛选规则静默失效的问题。
    """
    def __init__(self):
        self.airing_series_ids = None
        self.series_runtime_cache = {}

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
            
            # 1. 处理列表字段
            if field in ['actors', 'directors', 'genres', 'countries', 'studios', 'tags', 'keywords']:
                item_value_list = item_metadata.get(f"{field}_json")
                if not item_value_list or not isinstance(item_value_list, list):
                    results.append(False)
                    continue

                values_to_check = item_value_list
                if op == 'is_primary':
                    if field == 'actors':
                        values_to_check = item_value_list[:3] #演员只取前3
                    else:
                        values_to_check = item_value_list[:1]
                
                try:
                    if field in ['actors', 'directors']:
                        if not isinstance(value, list):
                            results.append(False); continue
                        
                        rule_person_ids = set(str(p['id']) for p in value if isinstance(p, dict) and 'id' in p)
                        if not rule_person_ids:
                            results.append(False); continue

                        item_person_ids = set()
                        for p in values_to_check:
                            person_id = p.get('tmdb_id') or p.get('id') # <-- 修正点！
                            if person_id is not None:
                                item_person_ids.add(str(person_id))
                        
                        if op in ['is_one_of', 'contains', 'is_primary']:
                            if not rule_person_ids.isdisjoint(item_person_ids):
                                match = True
                        elif op == 'is_none_of':
                            if rule_person_ids.isdisjoint(item_person_ids):
                                match = True
                    
                    # 处理其他普通列表字段
                    else:
                        if op == 'is_primary':
                            if values_to_check and values_to_check[0] == value:
                                match = True
                        elif op == 'is_one_of':
                            if isinstance(value, list) and any(v in values_to_check for v in value):
                                match = True
                        elif op == 'is_none_of':
                            if isinstance(value, list) and not any(v in values_to_check for v in value):
                                match = True
                        elif op == 'contains':
                            if value in values_to_check:
                                match = True

                except (TypeError, KeyError) as e:
                    logger.warning(f"  ➜ 处理 {field}_json 时遇到意外的格式错误: {e}, 内容: {item_value_list}")

            # 2. 处理其他所有非列表字段
            elif field in ['release_date', 'date_added']:
                item_date_val = item_metadata.get(field)
                if item_date_val and str(value).isdigit():
                    try:
                        if isinstance(item_date_val, datetime):
                            item_date = item_date_val.date()
                        elif isinstance(item_date_val, date):
                            item_date = item_date_val
                        else:
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

            # 3. 处理分级字段
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
            
            # 处理时长筛选 
            elif field == 'runtime':
                try:
                    threshold_minutes = float(value)
                    item_runtime = 0.0
                    
                    # 1. 电影处理逻辑 (不变)
                    if item_metadata.get('item_type') == 'Movie':
                        item_runtime = float(item_metadata.get('runtime_minutes') or 0)
                        if item_runtime <= 0:
                            assets = item_metadata.get('asset_details_json')
                            if assets and isinstance(assets, list) and len(assets) > 0:
                                item_runtime = float(assets[0].get('runtime_minutes') or 0)

                    # 2. 剧集处理逻辑 (★★★ 核心修改 ★★★)
                    elif item_metadata.get('item_type') == 'Series':
                        tmdb_id = str(item_metadata.get('tmdb_id'))
                        
                        # A. 优先尝试从缓存获取 (用于批量生成任务，极速)
                        if self.series_runtime_cache and tmdb_id in self.series_runtime_cache:
                            item_runtime = self.series_runtime_cache[tmdb_id]
                        
                        # B. 如果缓存未命中，说明是实时入库匹配 (单次查询，性能无损)
                        else:
                            # 调用我们在第一步中添加的单项查询函数
                            item_runtime = media_db.get_series_average_runtime(tmdb_id)
                            logger.debug(f"    ➜ [实时筛选] 剧集 {tmdb_id} 实时计算平均时长: {item_runtime} 分钟")
                    
                    # 3. 执行比较 (不变)
                    if op == 'gte': 
                        if item_runtime >= threshold_minutes: match = True
                    elif op == 'lte': 
                        if item_runtime > 0 and item_runtime <= threshold_minutes: match = True
                        
                except (ValueError, TypeError):
                    pass

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

        # 指定媒体库
        if library_ids and isinstance(library_ids, list) and len(library_ids) > 0:
            logger.info(f"  ➜ 已指定 {len(library_ids)} 个媒体库，正在从本地数据库筛选...")
            
            # 1. 获取符合库条件的 TMDB ID 集合
            tmdb_ids_in_libs = collection_db.get_tmdb_ids_by_library_ids(library_ids)
            
            if not tmdb_ids_in_libs:
                logger.warning("  ➜ 指定的媒体库中未找到任何符合条件的媒体项。")
                return []

            # 2. 批量获取这些 ID 的详细元数据
            # 注意：get_media_details_by_tmdb_ids 返回的是 {tmdb_id: metadata} 字典
            media_metadata_map = media_db.get_media_details_by_tmdb_ids(list(tmdb_ids_in_libs))
            
            # 3. 按需要的类型过滤并添加到总列表
            for item_type in item_types_to_process:
                metadata_for_type = [
                    meta for meta in media_metadata_map.values() 
                    if meta.get('item_type') == item_type
                ]
                all_media_metadata.extend(metadata_for_type)
        # 未指定媒体库
        else:
            # --- 分支2：保持原有逻辑，扫描全库 ---
            logger.info("  ➜ 未指定媒体库，将扫描所有媒体库的元数据缓存...")
            for item_type in item_types_to_process:
                all_media_metadata.extend(media_db.get_all_media_metadata(item_type=item_type))

        if all_media_metadata:
            # 1. 挑出所有剧集的 TMDB ID
            series_ids_to_fetch = [
                str(m['tmdb_id']) 
                for m in all_media_metadata 
                if m.get('item_type') == 'Series' and m.get('tmdb_id')
            ]
            
            # 2. 如果本次筛选包含剧集，且规则里有时长筛选，则进行精准预取
            # (为了简单稳健，只要有剧集就预取，开销极小)
            if series_ids_to_fetch:
                logger.info(f"  ➜ 正在为本次筛选范围内的 {len(series_ids_to_fetch)} 部剧集精准计算平均时长...")
                start_time = datetime.now()
                self.series_runtime_cache = media_db.get_runtimes_for_series_list(series_ids_to_fetch)
                duration = (datetime.now() - start_time).total_seconds()
                logger.info(f"  ➜ 时长计算完成，耗时 {duration:.3f}秒。")
        
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
                    emby_ids = media_metadata.get('emby_item_ids_json')
                    first_emby_id = emby_ids[0] if emby_ids and isinstance(emby_ids, list) else None
                    
                    matched_items.append({
                        'id': str(tmdb_id), 
                        'type': item_type,
                        'emby_id': first_emby_id 
                    })
                    
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
    
class RecommendationEngine:
    """
    【AI 推荐引擎 (双模版)】
    模式 A (LLM): 基于大模型知识库推荐 (适合发现新片)。
    模式 B (Vector): 基于本地数据库向量相似度推荐 (适合精准匹配口味)。
    """
    def __init__(self, tmdb_api_key: str):
        self.tmdb_api_key = tmdb_api_key
        self.list_importer = ListImporter(tmdb_api_key) # 复用搜索匹配逻辑

    def _vector_search(self, user_history_items: List[Dict], limit: int = 10) -> List[Dict]:
        """
        【内部方法】基于向量相似度搜索本地数据库。
        改进版：优先使用 TMDb ID 进行精确匹配，而非不靠谱的标题匹配。
        """
        logger.info("  ➜ [向量搜索] 开始加载向量数据并计算相似度...")
        
        # 1. 提取历史记录中的 ID 集合 (用于快速查找)
        history_tmdb_ids = set()
        history_titles = set()
        
        # 兼容处理：user_history_items 可能是字典列表，也可能是纯标题列表(旧逻辑)
        for item in user_history_items:
            if isinstance(item, dict):
                if item.get('tmdb_id'):
                    history_tmdb_ids.add(str(item.get('tmdb_id')))
                if item.get('title'):
                    history_titles.add(item.get('title'))
            elif isinstance(item, str):
                history_titles.add(item)

        if not history_tmdb_ids and not history_titles:
            logger.warning("  ➜ [向量搜索] 用户历史记录为空或格式无法解析，跳过。")
            return []

        try:
            with connection.get_db_connection() as conn:
                cursor = conn.cursor()
                # 获取所有已生成向量的媒体
                cursor.execute("""
                    SELECT tmdb_id, title, item_type, overview_embedding 
                    FROM media_metadata 
                    WHERE overview_embedding IS NOT NULL
                """)
                all_data = cursor.fetchall()
                
            if not all_data:
                logger.warning("  ➜ [向量搜索] 数据库中没有向量数据。请先运行“生成媒体向量”任务。")
                return []

            # 2. 构建矩阵
            ids = []
            vectors = []
            titles = []
            types = []
            
            for row in all_data:
                vec = row.get('overview_embedding')
                if vec and len(vec) > 0:
                    ids.append(str(row['tmdb_id']))
                    titles.append(row['title'])
                    types.append(row['item_type'])
                    vectors.append(np.array(vec, dtype=np.float32))
            
            if not vectors:
                return []

            matrix = np.stack(vectors)
            # 归一化
            norm = np.linalg.norm(matrix, axis=1, keepdims=True)
            matrix = matrix / (norm + 1e-10)

            # 3. 定位用户口味 (User Profile)
            user_vectors = []
            matched_count = 0
            
            # ★★★ 改进的核心：优先匹配 ID ★★★
            for idx, db_tmdb_id in enumerate(ids):
                is_match = False
                
                # A. ID 精确匹配
                if db_tmdb_id in history_tmdb_ids:
                    is_match = True
                
                # B. 标题兜底匹配 (如果 ID 没对上，再试标题)
                elif titles[idx] and any(h_t in titles[idx] for h_t in history_titles):
                    is_match = True
                
                if is_match:
                    user_vectors.append(matrix[idx])
                    matched_count += 1
            
            if not user_vectors:
                logger.warning(f"  ➜ [向量搜索] 匹配失败：用户的 {len(user_history_items)} 条历史记录均未在本地向量库中找到对应数据。")
                logger.warning(f"    (提示：请检查这些影片是否已入库，且是否已运行'生成媒体向量'任务)")
                return []
            
            logger.info(f"  ➜ [向量搜索] 成功匹配到 {matched_count} 部历史影片的向量，正在计算推荐...")

            # 计算用户平均向量
            user_profile_vector = np.mean(user_vectors, axis=0)
            user_profile_vector = user_profile_vector / (np.linalg.norm(user_profile_vector) + 1e-10)

            # 4. 计算相似度
            scores = np.dot(matrix, user_profile_vector)
            top_indices = np.argsort(scores)[::-1]
            
            results = []
            count = 0
            for idx in top_indices:
                if count >= limit: break
                
                score = float(scores[idx])
                if score < 0.45: break 
                if score > 0.999: continue # 排除自己
                
                # 排除已看过的 (ID 或 标题)
                if ids[idx] in history_tmdb_ids: continue
                if any(h_t in titles[idx] for h_t in history_titles): continue

                results.append({
                    'id': ids[idx],
                    'type': types[idx],
                    'title': titles[idx],
                    'score': score
                })
                count += 1
                
            logger.info(f"  ➜ [向量搜索] 计算完成，贡献了 {len(results)} 部相似影片。")
            return results

        except Exception as e:
            logger.error(f"  ➜ [向量搜索] 计算过程发生错误: {e}", exc_info=True)
            return []
        
    def generate(self, definition: Dict) -> List[Dict[str, str]]:
        """
        生成推荐列表的主入口 (配合 media_db 返回字典列表的优化版)。
        """
        target_user_id = definition.get('target_user_id')
        ai_prompt = definition.get('ai_prompt')
        limit = definition.get('limit', 20)

        if not target_user_id:
            logger.error("  ➜ [AI推荐] 未指定目标用户，无法生成推荐。")
            return []

        # 1. 获取用户历史 (现在直接返回包含 tmdb_id 的字典列表)
        history_items = media_db.get_user_positive_history(target_user_id, limit=20)
        
        if not history_items:
            logger.warning(f"  ➜ [AI推荐] 用户 {target_user_id} 没有足够的观看历史。")
            return []

        # 准备给 LLM 看的纯文本标题列表
        history_titles_for_llm = []
        for item in history_items:
            title = item.get('title')
            year = item.get('release_year')
            if year:
                history_titles_for_llm.append(f"{title} ({year})")
            else:
                history_titles_for_llm.append(title)

        final_items_map = {} 

        # ==================================================
        # 策略 A: LLM 推荐
        # ==================================================
        logger.info(f"  ➜ [AI推荐] 正在调用 LLM 进行推理...")
        try:
            translator = AITranslator(config_manager.APP_CONFIG)
            request_limit = min(int(limit * 1.5), 50) 
            instruction_with_limit = f"{ai_prompt or ''} (Please recommend at least {request_limit} items)"
            
            # 传给 LLM 纯文本标题
            llm_recommendations = translator.get_recommendations(history_titles_for_llm, instruction_with_limit)
            
            if llm_recommendations:
                logger.info(f"  ➜ [AI推荐] LLM 返回了 {len(llm_recommendations)} 部作品，正在匹配 TMDb ID...")
                
                with ThreadPoolExecutor(max_workers=5) as executor:
                    def resolve_item(rec_item):
                        try:
                            # 1. 数据标准化
                            title = ""
                            original_title = ""
                            year = None
                            # 默认先信 AI 的，AI 没说就默认为 Movie
                            primary_type = 'Movie' 
                            
                            if isinstance(rec_item, dict):
                                title = rec_item.get('title')
                                original_title = rec_item.get('original_title')
                                year = str(rec_item.get('year')) if rec_item.get('year') else None
                                if rec_item.get('type'):
                                    primary_type = rec_item.get('type')
                            elif isinstance(rec_item, str):
                                title = rec_item
                            
                            if not title: return None

                            # 定义反向类型（如果这次搜Movie失败，下次就搜Series）
                            secondary_type = 'Series' if primary_type == 'Movie' else 'Movie'

                            # 简单的中文判断
                            def has_chinese(text):
                                return any('\u4e00' <= char <= '\u9fff' for char in str(text))

                            # 2. 确定搜索关键词
                            # 默认用原名搜
                            search_query = original_title if original_title else title
                            
                            # 特殊优化：如果是国产剧（标题含中文），强行用中文名搜，准确率最高
                            if has_chinese(title):
                                search_query = title

                            # --- 核心修改：四重搜索策略 ---
                            
                            # 第 1 试：用【首选类型】+【首选关键词】搜
                            match_result = self.list_importer._match_title_to_tmdb(search_query, primary_type, year)
                            
                            # 第 2 试：如果没搜到，尝试【反向类型】+【首选关键词】
                            # (解决 AI 把电视剧标成电影，或者没标类型的情况)
                            if not match_result:
                                # logger.debug(f"  ➜ [AI推荐] '{search_query}' 按 {primary_type} 未找到，尝试按 {secondary_type} 搜索...")
                                match_result = self.list_importer._match_title_to_tmdb(search_query, secondary_type, year)

                            # 第 3 试：如果还没搜到，且关键词不是中文标题，尝试用【中文标题】+【首选类型】搜
                            # (解决英文原名搜不到的情况)
                            if not match_result and search_query != title:
                                match_result = self.list_importer._match_title_to_tmdb(title, primary_type, year)

                            # 第 4 试：最后试一次【中文标题】+【反向类型】
                            if not match_result and search_query != title:
                                match_result = self.list_importer._match_title_to_tmdb(title, secondary_type, year)

                            # --- 结果处理 ---
                            if match_result:
                                tmdb_id, matched_type, season_num = match_result
                                return {
                                    'id': tmdb_id,
                                    'type': matched_type, # 注意：这里返回的是实际匹配到的类型
                                    'title': title, 
                                    'season': season_num,
                                    'release_date': None 
                                }
                            else:
                                logger.debug(f"  ➜ [AI推荐] 未能找到 '{title}' (搜: {search_query}) 的 TMDb ID (已尝试电影/剧集跨类型搜索)。")
                                return None
                        except Exception as e:
                            logger.error(f"  ➜ [AI推荐] 处理单项 '{rec_item}' 时出错: {e}")
                            return None

                    results = executor.map(resolve_item, llm_recommendations)
                    for res in results:
                        if res:
                            final_items_map[res['id']] = res
            else:
                logger.warning("  ➜ [AI推荐] LLM 未返回任何有效结果。")

        except Exception as e:
            logger.error(f"  ➜ [AI推荐] LLM 调用失败: {e}")

        # ==================================================
        # 策略 B: 向量推荐
        # ==================================================
        if len(final_items_map) < limit:
            try:
                needed = limit - len(final_items_map)
                # ★★★ 关键：直接把包含 tmdb_id 的字典列表传给向量搜索 ★★★
                # (前提是你已经应用了我上一条回复中对 _vector_search 的修改)
                vector_results = self._vector_search(history_items, limit=needed + 5)
                
                if vector_results:
                    logger.info(f"  ➜ [AI推荐] 启用向量引擎补充了 {len(vector_results)} 部相似影片。")
                    for v in vector_results:
                        if v['id'] not in final_items_map:
                            final_items_map[v['id']] = {
                                'id': v['id'],
                                'type': v['type'],
                                'title': v['title'],
                                'release_date': None
                            }
            except Exception as e:
                logger.error(f"  ➜ [AI推荐] 向量推荐失败: {e}", exc_info=True)

        # ==================================================
        # 最终汇总
        # ==================================================
        final_items = list(final_items_map.values())
        if limit and isinstance(limit, int):
            final_items = final_items[:limit]

        logger.info(f"  ➜ [AI推荐] 全部完成。最终生成 {len(final_items)} 部影片。")
        return final_items