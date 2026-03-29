# handler/custom_collection.py
import logging
import requests
import xml.etree.ElementTree as ET
import re
import os
import time
import gevent
import numpy as np
import sys
from typing import List, Dict, Any, Optional, Tuple
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from gevent import subprocess, Timeout
from urllib.parse import urlparse, parse_qs, unquote
import handler.tmdb as tmdb
import config_manager
from tasks.helpers import parse_series_title_and_season
from database import media_db, connection
from handler.douban import DoubanApi
from handler.tmdb import search_media
from ai_translator import AITranslator

logger = logging.getLogger(__name__)


class ListImporter:
    """
    (V9.1 - 最终异步版)
    负责处理外部榜单源 (RSS, TMDb List, Douban Doulist, Maoyan, etc.)
    """
    
    SEASON_PATTERN = re.compile(r'(.*?)\s*[（(]?\s*(第?[一二三四五六七八九十百]+)\s*季\s*[)）]?')
    
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
    
    def _process_dynamic_date_placeholders(self, url: str) -> str:
        """
        解析包含动态日期占位符的 TMDb URL。
        支持格式: 
        {today}
        {today+N} (例如 {today+7})
        {tomorrow}
        {tomorrow+N} (例如 {tomorrow+7})
        """
        url = unquote(url)
        if '{' not in url:
            return url

        current_date = datetime.now().date()
        
        # 辅助函数：替换占位符
        def replace_date_placeholder(match):
            base_type = match.group(1) # 'today' 或 'tomorrow'
            offset_str = match.group(2) # '+N' 部分 (可选)

            base_date = current_date
            if base_type == 'tomorrow':
                base_date += timedelta(days=1) # 如果是 {tomorrow}，则从明天开始计算

            target_date = base_date
            if offset_str:
                try:
                    days_to_add = int(offset_str)
                    target_date += timedelta(days=days_to_add)
                except ValueError:
                    logger.warning(f"  ➜ 无法解析日期偏移量: {offset_str}。")
                    return match.group(0) # 如果解析失败，返回原始占位符
            
            return target_date.isoformat()

        # 正则表达式匹配 {today} {today+N} {tomorrow} {tomorrow+N}
        # 第1组: 'today' 或 'tomorrow'
        # 第2组: '+N' 部分 (可选)
        url = re.sub(r'\{(today|tomorrow)(\+\d+)?\}', replace_date_placeholder, url)
        
        return url

    def _get_items_from_douban_doulist(self, url: str) -> List[Dict[str, str]]:
        """专门用于解析和分页获取豆瓣豆列内容的函数"""
        all_items = []
        base_url = url.split('?')[0]
        page_start = 0
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
                doulist_items = soup.find_all('div', class_='doulist-item')

                if not doulist_items:
                    logger.info(f"  ➜ 在第 {page + 1} 页未发现更多项目，获取结束。")
                    break

                for item in doulist_items:
                    title_div = item.find('div', class_='title')
                    if not title_div: continue
                    
                    link_tag = title_div.find('a')
                    if not link_tag: continue
                    
                    title = link_tag.get_text(strip=True)
                    douban_link = link_tag.get('href')
                    
                    year = None
                    abstract_div = item.find('div', class_='abstract')
                    if abstract_div:
                        year_match = re.search(r'\b(19\d{2}|20\d{2})\b', abstract_div.get_text())
                        if year_match:
                            year = year_match.group(1)
                    
                    if title:
                        all_items.append({
                            'title': title,
                            'imdb_id': None,
                            'year': year,
                            'douban_link': douban_link
                        })

            except Exception as e:
                logger.error(f"  ➜ 获取或解析豆瓣豆列页面 '{paginated_url}' 时出错: {e}")
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
        total_pages = 1

        logger.info(f"  ➜ 检测到TMDb片单链接，开始分页获取: {url}")

        while current_page <= total_pages:
            try:
                logger.debug(f"    ➜ 正在获取第 {current_page} / {total_pages} 页...")
                list_data = tmdb.get_list_details_tmdb(list_id, self.tmdb_api_key, page=current_page)

                if not list_data or not list_data.get('items'):
                    logger.warning(f"  ➜ 在第 {current_page} 页未发现更多项目，获取结束。")
                    break

                if current_page == 1:
                    total_pages = list_data.get('total_pages', 1)

                for item in list_data['items']:
                    media_type = item.get('media_type')
                    tmdb_id = item.get('id')
                    
                    item_type_mapped = 'Series' if media_type == 'tv' else 'Movie'
                    title = item.get('title') if item_type_mapped == 'Movie' else item.get('name')

                    if tmdb_id:
                        all_items.append({
                            'id': str(tmdb_id), 
                            'type': item_type_mapped,
                            'title': title
                        })

                current_page += 1

            except Exception as e:
                logger.error(f"  ➜ 获取或解析TMDb片单页面 {current_page} 时出错: {e}")
                break
        
        logger.info(f"  ➜ TMDb片单获取完成，从 {total_pages} 个页面中总共解析出 {len(all_items)} 个项目。")
        return all_items
    
    def _get_items_from_tmdb_discover(self, url: str) -> List[Dict[str, str]]:
        """专门用于解析TMDb Discover URL并获取结果的函数"""
        
        # 在解析前处理动态日期占位符 
        processed_url = self._process_dynamic_date_placeholders(url) 
        logger.info(f"  ➜ 检测到TMDb Discover链接，开始动态获取 (支持分页和过滤): {processed_url}")
        parsed_url = urlparse(processed_url) # 使用处理后的 URL
        query_params = parse_qs(parsed_url.query)
        params = {k: v[0] for k, v in query_params.items()}

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

                if '/discover/movie' in url: # 注意这里依然使用原始url判断类型
                    discover_data = tmdb.discover_movie_tmdb(self.tmdb_api_key, params)
                    item_type_for_result = 'Movie'
                elif '/discover/tv' in url: # 注意这里依然使用原始url判断类型
                    discover_data = tmdb.discover_tv_tmdb(self.tmdb_api_key, params)
                    item_type_for_result = 'Series'
                else:
                    logger.warning(f"  ➜ 无法从URL '{url}' 判断是电影还是电视剧，discover任务中止。")
                    break

                if not discover_data or not discover_data.get('results'):
                    logger.info("  ➜ 在当前页未发现更多项目，获取结束。")
                    break

                if current_page == 1:
                    total_pages = discover_data.get('total_pages', 1)

                for item in discover_data['results']:
                    if not item.get('poster_path'):
                        continue
                    if not item.get('overview'):
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
        def normalize_string(s: str) -> str:
            if not s: return ""
            return re.sub(r'[\s:：·\-*\'!,?.。]+', '', s).lower()

        if item_type == 'Movie':
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
            show_name_parsed, season_number_to_validate = parse_series_title_and_season(title, api_key=self.tmdb_api_key)
            show_name = show_name_parsed if show_name_parsed else title
            
            results = search_media(show_name, self.tmdb_api_key, 'Series', year=year)

            if not results and year and season_number_to_validate is not None:
                logger.debug(f"  ➜ 带年份 '{year}' 搜索剧集 '{show_name}' 未找到结果，可能是后续季。尝试不带年份进行回退搜索...")
                results = search_media(show_name, self.tmdb_api_key, 'Series', year=None)

            if not results:
                year_info = f" (年份: {year})" if year else ""
                logger.warning(f"  ➜ 剧集标题 '{title}' (搜索词: '{show_name}'){year_info} 未能在TMDb上找到匹配项。")
                return None
            
            if season_number_to_validate is None:
                series_result = None
                norm_show_name = normalize_string(show_name)
                
                for result in results:
                    if normalize_string(result.get('name', '')) == norm_show_name:
                        series_result = result
                        logger.debug(f"  ➜ 剧集 '{show_name}' 通过【精确规范匹配】找到了: {result.get('name')} (ID: {result.get('id')})")
                        break 
                
                if not series_result:
                    series_result = results[0]
                    logger.warning(f"  ➜ 剧集 '{show_name}' 未找到精确匹配，使用首个结果: {series_result.get('name')} (ID: {series_result.get('id')})")

                return str(series_result.get('id')), 'Series', None

            else:
                def verify_season_in_results(candidates_list, source_desc=""):
                    if not candidates_list:
                        return None
                    
                    norm_show_name = normalize_string(show_name)
                    candidates_list.sort(key=lambda x: 0 if normalize_string(x.get('name', '')) == norm_show_name else 1)
                    
                    logger.info(f"  ➜ 剧集 '{show_name}'{source_desc} 需要验证第 {season_number_to_validate} 季，正在扫描 {len(candidates_list)} 个候选结果...")

                    for candidate in candidates_list:
                        candidate_id = str(candidate.get('id'))
                        candidate_name = candidate.get('name')
                        
                        series_details = tmdb.get_tv_details(int(candidate_id), self.tmdb_api_key, append_to_response="seasons")
                        
                        if series_details and 'seasons' in series_details:
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

                matched_id = verify_season_in_results(results[:5])
                if matched_id:
                    return matched_id, 'Series', season_number_to_validate

                if year:
                    logger.info(f"  ➜ 剧集 '{show_name}' 带年份 ({year}) 搜索结果中未找到第 {season_number_to_validate} 季，尝试移除年份重搜...")
                    results_no_year = search_media(show_name, self.tmdb_api_key, 'Series', year=None)
                    
                    if results_no_year:
                        checked_ids = set(str(r.get('id')) for r in results[:5])
                        candidates_no_year = [r for r in results_no_year if str(r.get('id')) not in checked_ids][:5]
                        
                        if candidates_no_year:
                            matched_id = verify_season_in_results(candidates_no_year, source_desc=" (无年份重搜)")
                            if matched_id:
                                return matched_id, 'Series', season_number_to_validate

                logger.warning(f"  ➜ 验证失败！在 '{show_name}' 的所有搜索结果中，均未找到第 {season_number_to_validate} 季。")
                    
                if show_name != title:
                    logger.info(f"  ➜ [兜底机制] 尝试使用原始标题 '{title}' 进行回退搜索...")
                    fallback_results = search_media(title, self.tmdb_api_key, 'Series', year=None)
                    
                    if fallback_results:
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
        
        collected_lists = [] 
        last_source_type = 'mixed'
        
        total_urls = len(urls)
        for i, url in enumerate(urls):
            temp_def = definition.copy()
            temp_def['url'] = url
            
            items, source_type = self._process_single_url(url, temp_def)
            
            collected_lists.append(items)
            last_source_type = source_type
            
            if isinstance(url, str) and url.startswith('maoyan://'):
                if i < total_urls - 1:
                    logger.info(f"  ➜ [防封控] 单个猫眼榜单采集完毕，为安全起见，强制休眠 10 秒后再采集下一个...")
                    time.sleep(10)
        
        all_items = []
        if collected_lists:
            max_length = max(len(l) for l in collected_lists) if collected_lists else 0
            for i in range(max_length):
                for sublist in collected_lists:
                    if i < len(sublist):
                        all_items.append(sublist[i])
            
            logger.info(f"  ➜ 已将 {len(collected_lists)} 个榜单源交叉合并，总计 {len(all_items)} 个候选项。")
            
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
        
        limit = definition.get('limit')
        if limit and isinstance(limit, int) and limit > 0:
            unique_items = unique_items[:limit]
            
        return unique_items, last_source_type

    def _process_single_url(self, url: str, definition: Dict) -> Tuple[List[Dict[str, str]], str]:
        definition = definition.copy()
        definition['url'] = url
        source_type = 'list_rss'
        
        if not url:
            return [], source_type
            
        if url.startswith('maoyan://'):
            source_type = 'list_maoyan'
            logger.info(f"  ➜ 检测到猫眼榜单，将启动异步后台脚本...")
            greenlet = gevent.spawn(self._execute_maoyan_fetch, definition)
            tmdb_items = greenlet.get()
            return tmdb_items, source_type

        item_types = definition.get('item_type', ['Movie'])
        if isinstance(item_types, str): item_types = [item_types]
        limit = definition.get('limit')
        
        items, source_type = self._get_titles_and_imdbids_from_url(url)
        
        if not items: return [], source_type
        
        if items and 'id' in items[0] and 'type' in items[0]:
            logger.info(f"  ➜ 检测到来自TMDb源 ({source_type}) 的预匹配ID，将跳过标题匹配。")
            if limit and isinstance(limit, int) and limit > 0:
                items = items[:limit]
            return items, source_type

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

                def create_result(tmdb_id, item_type, confirmed_season=None):
                    result = {
                        'id': tmdb_id, 
                        'type': item_type, 
                        'title': original_source_title,
                        'year': year
                    }
                    if item_type == 'Series' and confirmed_season is not None:
                        result['season'] = confirmed_season
                    return result

                fallback_result = {
                    'id': None, 
                    'type': types_to_check[0] if types_to_check else 'Movie', 
                    'title': original_source_title,
                    'year': year
                }

                if rss_imdb_id:
                    for item_type in types_to_check:
                        tmdb_id = self._match_by_ids(rss_imdb_id, None, item_type)
                        if tmdb_id:
                            _, s_num = parse_series_title_and_season(original_source_title, api_key=self.tmdb_api_key)
                            return create_result(tmdb_id, item_type, s_num)

                cleaned_title = re.sub(r'^\s*\d+\.\s*', '', original_source_title)
                cleaned_title = re.sub(r'\s*\(\d{4}\)$', '', cleaned_title).strip()
                
                for item_type in types_to_check:
                    match_result = self._match_title_to_tmdb(cleaned_title, item_type, year=year)
                    
                    if match_result:
                        tmdb_id, matched_type, matched_season = match_result
                        return create_result(tmdb_id, matched_type, matched_season)
                
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
                key = f"{item_type}-{tmdb_id}-{season}"
            else:
                key = f"unidentified-{title}"
            
            if key not in seen_keys:
                seen_keys.add(key)
                unique_items.append(item)
                
        logger.info(f"  ➜ 去重后剩余 {len(unique_items)} 个有效项目。")

        return unique_items, source_type


class RecommendationEngine:
    """
    【AI 推荐引擎 (双模版)】
    模式 A (LLM): 基于大模型知识库推荐 (适合发现新片)。
    模式 B (Vector): 基于本地数据库向量相似度推荐 (适合精准匹配口味)。
    """
    _cache_matrix = None
    _cache_ids = None
    _cache_titles = None
    _cache_types = None
    _REFRESH_INTERVAL = 14400
    _is_refreshing_loop_running = False 

    def __init__(self, tmdb_api_key: str):
        self.tmdb_api_key = tmdb_api_key
        self.list_importer = ListImporter(tmdb_api_key) 

    @classmethod
    def refresh_cache(cls):
        """
        【类方法】强制刷新缓存 (执行数据库读取和矩阵构建)
        """
        logger.info("  🔄 [向量引擎] 开始后台刷新向量缓存...")
        start_t = time.time()
        
        try:
            with connection.get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT tmdb_id, title, item_type, overview_embedding 
                    FROM media_metadata 
                    WHERE overview_embedding IS NOT NULL
                      AND item_type IN ('Movie', 'Series')
                      AND in_library = TRUE
                """)
                all_data = cursor.fetchall()
            
            if not all_data:
                logger.warning("  ➜ [向量引擎] 数据库为空，无法刷新缓存。")
                return

            ids, vectors, titles, types = [], [], [], []
            for row in all_data:
                vec = row.get('overview_embedding')
                if vec and len(vec) > 0:
                    ids.append(str(row['tmdb_id']))
                    titles.append(row['title'])
                    types.append(row['item_type'])
                    vectors.append(np.array(vec, dtype=np.float32))
            
            if not vectors:
                return

            matrix = np.stack(vectors)
            norm = np.linalg.norm(matrix, axis=1, keepdims=True)
            matrix = matrix / (norm + 1e-10)

            cls._cache_matrix = matrix
            cls._cache_ids = ids
            cls._cache_titles = titles
            cls._cache_types = types
            
            logger.info(f"  ➜ [向量引擎] 缓存刷新完成。共 {len(ids)} 条，耗时 {time.time() - start_t:.2f}s。")

        except Exception as e:
            logger.error(f"  ➜ [向量引擎] 刷新缓存失败: {e}", exc_info=True)

    @classmethod
    def start_auto_refresh_loop(cls):
        """
        【类方法】启动自动刷新循环
        """
        if cls._is_refreshing_loop_running:
            return
        
        cls._is_refreshing_loop_running = True
        
        def loop():
            logger.info("  ➜ [向量引擎] 自动刷新守护线程已启动。")
            cls.refresh_cache()
            
            while True:
                gevent.sleep(cls._REFRESH_INTERVAL)
                cls.refresh_cache()
        
        gevent.spawn(loop)

    def _get_vector_data(self):
        """
        【内部方法】获取向量数据 (极速版)
        """
        if RecommendationEngine._cache_matrix is None:
            RecommendationEngine.refresh_cache()
            
        return (RecommendationEngine._cache_matrix, 
                RecommendationEngine._cache_ids, 
                RecommendationEngine._cache_titles, 
                RecommendationEngine._cache_types)

    def _vector_search(self, user_history_items: List[Dict], exclusion_ids: set = None, limit: int = 10, allowed_types: List[str] = None) -> List[Dict]:
        """
        【内部方法】基于向量相似度搜索本地数据库。
        """
        if exclusion_ids is None: exclusion_ids = set()
        if not allowed_types: allowed_types = ['Movie', 'Series']
        if exclusion_ids is None:
            exclusion_ids = set()

        history_tmdb_ids = set()
        history_titles = set()
        for item in user_history_items:
            if isinstance(item, dict):
                if item.get('tmdb_id'): history_tmdb_ids.add(str(item.get('tmdb_id')))
                if item.get('title'): history_titles.add(item.get('title'))
            elif isinstance(item, str):
                history_titles.add(item)

        if not history_tmdb_ids and not history_titles:
            return []

        matrix, ids, titles, types = self._get_vector_data()
        
        if matrix is None:
            logger.warning("  ➜ [向量搜索] 无法获取向量数据 (数据库为空或加载失败)。")
            return []

        try:
            user_vectors = []
            
            for idx, db_tmdb_id in enumerate(ids):
                is_match = False
                if db_tmdb_id in history_tmdb_ids:
                    is_match = True
                elif titles[idx] and any(h_t in titles[idx] for h_t in history_titles):
                    is_match = True
                
                if is_match:
                    user_vectors.append(matrix[idx])
            
            if not user_vectors:
                logger.warning(f"  ➜ [向量搜索] 匹配失败：用户的历史记录未在向量库中找到对应数据。")
                return []
            
            user_profile_vector = np.mean(user_vectors, axis=0)
            user_profile_vector = user_profile_vector / (np.linalg.norm(user_profile_vector) + 1e-10)

            scores = np.dot(matrix, user_profile_vector)
            top_indices = np.argsort(scores)[::-1]
            
            results = []
            count = 0
            
            for idx in top_indices:
                if count >= max(limit, 200): break
                
                if types[idx] not in allowed_types:
                    continue

                score = float(scores[idx])
                if score < 0.45: break 
                if score > 0.999: continue 
                
                current_id = ids[idx]
                
                if current_id in exclusion_ids: continue
                if current_id in history_tmdb_ids: continue
                if any(h_t in titles[idx] for h_t in history_titles): continue

                results.append({
                    'id': current_id,
                    'type': types[idx],
                    'title': titles[idx],
                    'score': score
                })
                count += 1
                
            return results

        except Exception as e:
            logger.error(f"  ➜ [向量搜索] 计算过程发生错误: {e}", exc_info=True)
            return []
        
    def generate_user_vector(self, user_id: str, limit: int = 50, allowed_types: List[str] = None) -> List[Dict]:
        """
        只使用向量搜索，速度快，适合实时生成。
        """
        logger.debug(f"  ➜ [个人向量推荐] 正在为用户 {user_id} 实时计算...")
        
        context_history_items = media_db.get_user_positive_history(user_id, limit=50)
        if not context_history_items:
            logger.warning(f"  ➜ 用户 {user_id} 历史记录不足，无法生成向量推荐。")
            return []

        watched_tmdb_ids = set()
        full_interaction = media_db.get_user_all_interacted_history(user_id)
        for item in full_interaction:
            if item.get('tmdb_id'):
                watched_tmdb_ids.add(str(item.get('tmdb_id')))

        results = self._vector_search(
            user_history_items=context_history_items,
            exclusion_ids=watched_tmdb_ids,
            limit=limit,
            allowed_types=allowed_types 
        )
        
        return results

    def generate_global_vector(self, limit: int = 300, allowed_types: List[str] = None) -> List[Dict]:
        """
        【全局向量推荐】
        逻辑：全站热门记录 -> 向量引擎 -> 库内 300 个相似项。
        """
        logger.debug("  ➜ [全局向量推荐] 正在基于全站热度计算候选池...")
        
        # 1. 获取全站热门作为“种子”
        context_history_items = media_db.get_global_popular_items(limit=20)
        if not context_history_items:
            return []

        # 2. 直接调用现有的向量搜索逻辑，在库内捞 300 个
        # 这样出来的全是“在库媒体”，且数量管饱
        results = self._vector_search(
            user_history_items=context_history_items,
            exclusion_ids=set(), # 全局推荐不需要排除已看
            limit=limit,
            allowed_types=allowed_types 
        )
        return results

    def generate(self, definition: Dict) -> List[Dict[str, str]]:
        """
        推荐生成器。
        """
        ai_prompt = definition.get('ai_prompt')
        limit = definition.get('limit', 20)
        discovery_ratio = float(definition.get('ai_discovery_ratio', 0.2))
        allowed_types = definition.get('item_type', ['Movie', 'Series'])

        logger.debug("  ➜ [智能推荐] 启动 (LLM + 向量混合模式)...")

        context_history_items = media_db.get_global_popular_items(limit=20)
        if not context_history_items:
            logger.warning("  ➜ 全站播放数据不足，无法生成全局推荐。")
            return []

        watched_tmdb_ids = set()
        for item in context_history_items:
            if item.get('tmdb_id'):
                watched_tmdb_ids.add(str(item.get('tmdb_id')))

        final_items_map = {}

        logger.info(f"  ➜ [智能推荐] 正在调用 LLM 分析全站口味...")
        history_titles_for_llm = []
        for item in context_history_items:
            title = item.get('title')
            year = item.get('release_year')
            if year:
                history_titles_for_llm.append(f"{title} ({year})")
            else:
                history_titles_for_llm.append(title)

        try:
            translator = AITranslator(config_manager.APP_CONFIG)
            request_limit = int(limit * discovery_ratio)
            request_limit = max(request_limit, 2) 

            system_prompt = "你是资深选片人。基于以下大众喜欢的影片，推荐同类高分作品。不要推荐列表中已有的。"
            if ai_prompt:
                system_prompt += f" 额外要求: {ai_prompt}"

            llm_recommendations = translator.get_recommendations(
                user_history=history_titles_for_llm, 
                user_instruction=system_prompt,
                allowed_types=allowed_types 
            )
                    
            if llm_recommendations:
                logger.info(f"  ➜ [智能推荐] LLM 返回了 {len(llm_recommendations)} 部作品，正在匹配 TMDb ID...")
                with ThreadPoolExecutor(max_workers=5) as executor:
                    def resolve_item(rec_item):
                        try:
                            title = ""
                            original_title = ""
                            year = None
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

                            search_types = []
                            if 'Movie' in allowed_types and 'Series' in allowed_types:
                                search_types = [primary_type, 'Series' if primary_type == 'Movie' else 'Movie']
                            elif 'Movie' in allowed_types:
                                search_types = ['Movie']
                            elif 'Series' in allowed_types:
                                search_types = ['Series']
                            
                            match_result = None
                            
                            def has_chinese(text):
                                return any('\u4e00' <= char <= '\u9fff' for char in str(text))
                            search_query = original_title if original_title else title
                            if has_chinese(title): search_query = title

                            for try_type in search_types:
                                match_result = self.list_importer._match_title_to_tmdb(search_query, try_type, year)
                                if match_result: break
                                
                                if search_query != title:
                                    match_result = self.list_importer._match_title_to_tmdb(title, try_type, year)
                                    if match_result: break

                            if match_result:
                                tmdb_id, matched_type, season_num = match_result
                                tmdb_id = str(tmdb_id)
                                
                                if matched_type not in allowed_types:
                                    return None

                                if tmdb_id in watched_tmdb_ids:
                                    return None
                                return {
                                    'id': tmdb_id,
                                    'type': matched_type,
                                    'title': title, 
                                    'season': season_num,
                                    'release_date': None 
                                }
                            return None
                        except Exception:
                            return None

                    results = executor.map(resolve_item, llm_recommendations)
                    for res in results:
                        if res:
                            final_items_map[res['id']] = res
            else:
                logger.info("  ➜ [智能推荐] 用户设置探索比例为 0%...")
        except Exception as e:
            logger.error(f"  ➜ [智能推荐] LLM 调用失败: {e}")

        if len(final_items_map) < limit:
            needed = limit - len(final_items_map)
            logger.info(f"  ➜ [智能推荐] 启用向量引擎补充 {needed} 部相似影片...")
            vector_results = self._vector_search(
                user_history_items=context_history_items, 
                exclusion_ids=watched_tmdb_ids, 
                limit=needed + 10
            )
            for v in vector_results:
                if v['type'] in allowed_types and v['id'] not in final_items_map:
                    final_items_map[v['id']] = {
                        'id': v['id'], 'type': v['type'], 'title': v['title']
                    }

        final_items = list(final_items_map.values())[:limit]
        logger.info(f"  ➜ [智能推荐] 完成，生成 {len(final_items)} 部影片。")
        return final_items