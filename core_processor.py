# core_processor.py

import os
import json
import time
import re
import copy
import random
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from collections import defaultdict
import threading
from datetime import datetime, timezone
import time as time_module
import psycopg2
# 确保所有依赖都已正确导入
from handler.custom_collection import RecommendationEngine
import config_manager
from database.connection import get_db_connection
from database import media_db, settings_db
import handler.emby as emby
import handler.tmdb as tmdb
from tasks.helpers import parse_full_asset_details, calculate_ancestor_ids, construct_metadata_payload, extract_top_directors, translate_tmdb_metadata_recursively
import utils
import constants
import logging
import actor_utils
from database.actor_db import ActorDBManager
from database.log_db import LogDBManager
from database.connection import get_db_connection as get_central_db_connection
from cachetools import TTLCache
from ai_translator import AITranslator
from watchlist_processor import WatchlistProcessor
from handler.douban import DoubanApi
import nfo_builder
# --- P115Center 依赖 ---
try:
    from p115center import P115Center
    P115_AVAILABLE = True
except ImportError:
    P115_AVAILABLE = False

logger = logging.getLogger(__name__)
try:
    from handler.douban import DoubanApi
    DOUBAN_API_AVAILABLE = True
except ImportError:
    DOUBAN_API_AVAILABLE = False
    class DoubanApi:
        def __init__(self, *args, **kwargs): pass
        def get_acting(self, *args, **kwargs): return {}
        def close(self): pass

def extract_tag_names(item_data):
    """
    兼容新旧版 Emby API 提取标签名。
    """
    tags_set = set()
    # 1. TagItems
    tag_items = item_data.get('TagItems')
    if isinstance(tag_items, list):
        for t in tag_items:
            if isinstance(t, dict):
                name = t.get('Name')
                if name: tags_set.add(name)
            elif isinstance(t, str) and t:
                tags_set.add(t)
    # 2. Tags
    tags = item_data.get('Tags')
    if isinstance(tags, list):
        for t in tags:
            if t: tags_set.add(str(t))
    return list(tags_set)

def is_valid_tmdb_id(tmdb_id) -> bool:
    """
    严格校验 TMDb ID 是否有效。
    拦截 None, '', '0', 'None', 'null' 以及非纯数字。
    """
    if not tmdb_id:
        return False
    id_str = str(tmdb_id).strip()
    if id_str in ['0', 'None', 'null', '']:
        return False
    if not id_str.isdigit():
        return False
    if int(id_str) <= 0:
        return False
    return True

def _aggregate_series_cast_from_tmdb_data(series_data: Dict[str, Any], all_episodes_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    【新】从内存中的TMDB数据聚合一个剧集的所有演员。
    """
    logger.debug(f"  ➜ 【演员聚合】开始为 '{series_data.get('name')}' 从内存中的TMDB数据聚合演员...")
    aggregated_cast_map = {}

    # 1. 优先处理主剧集的演员列表
    main_cast = series_data.get("credits", {}).get("cast", [])
    for actor in main_cast:
        actor_id = actor.get("id")
        if actor_id:
            aggregated_cast_map[actor_id] = actor
    logger.debug(f"  ➜ 从主剧集数据中加载了 {len(aggregated_cast_map)} 位主演员。")

    # 2. 聚合所有分集的演员和客串演员
    for episode_data in all_episodes_data:
        credits_data = episode_data.get("credits", {})
        actors_to_process = credits_data.get("cast", []) + credits_data.get("guest_stars", [])
        
        for actor in actors_to_process:
            actor_id = actor.get("id")
            if actor_id and actor_id not in aggregated_cast_map:
                if 'order' not in actor:
                    actor['order'] = 999  # 为客串演员设置高order值
                aggregated_cast_map[actor_id] = actor

    full_aggregated_cast = list(aggregated_cast_map.values())
    full_aggregated_cast.sort(key=lambda x: x.get('order', 999))
    
    logger.info(f"  ➜ 共为 '{series_data.get('name')}' 聚合了 {len(full_aggregated_cast)} 位独立演员。")
    return full_aggregated_cast

class MediaProcessor:
    def __init__(self, config: Dict[str, Any], ai_translator=None, douban_api=None):
        # ★★★ 然后，从这个 config 字典里，解析出所有需要的属性 ★★★
        self.config = config

        # 初始化我们的数据库管理员
        self.actor_db_manager = ActorDBManager()
        self.log_db_manager = LogDBManager()

        self.douban_api = douban_api
        self.emby_url = self.config.get("emby_server_url")
        self.emby_api_key = self.config.get("emby_api_key")
        self.emby_user_id = self.config.get("emby_user_id")
        self.tmdb_api_key = self.config.get("tmdb_api_key", "")
        self.ai_translator = ai_translator
        self._stop_event = threading.Event()
        self.processed_items_cache = self._load_processed_log_from_db()
        self.manual_edit_cache = TTLCache(maxsize=10, ttl=600)
        self._global_lib_guid_map = {}
        self._last_lib_map_update = 0
        # =========================================================
        # P115Center 初始化 (由 p115_mediainfo_center 开关控制)
        # =========================================================
        if P115_AVAILABLE and self.config.get("p115_mediainfo_center", True):
            machine_id = self.config.get("p115_machine_id", "")
            auth_file_path = str(Path(__file__).resolve().parent / "extensions.py")
            license_key = "650ad55de8fc0a81868754d39a2390c498ace7625f4d88d653ba0827132a02b3"
            
            try:
                self.p115_center = P115Center(
                    machine_id=machine_id,
                    license=license_key,
                    file_path=auth_file_path
                )
                logger.info("  ➜ P115Center SDK 初始化成功，已启用神医媒体信息中心化同步功能。")
            except Exception as e:
                logger.error(f"P115Center SDK 初始化失败: {e}")
                self.p115_center = None
        else:
            # ➜ 如果开关关闭或依赖缺失，直接设为 None
            self.p115_center = None
            
        logger.trace("核心处理器初始化完成。")

    # --- [优化版] 实时监控文件逻辑 (增加缓存跳过 & 支持批量延迟刷新) ---
    def process_file_actively(self, file_path: str, skip_refresh: bool = False) -> Optional[str]:
        """
        实时监控（优化版）：
        1. 识别 TMDb ID。
        2. 双向检查数据库和本地缓存，互补缺失数据。
        3. 生成本地覆盖缓存文件 (Override Cache)。
        4. (可选) 通知 Emby 刷新。
        
        Args:
            file_path: 文件路径
            skip_refresh: 是否跳过 Emby 刷新步骤 (用于批量处理时最后统一刷新)
            
        Returns:
            str: 该文件所属的父目录路径 (如果处理成功)，否则返回 None
        """
        folder_path = os.path.dirname(file_path)
        try:
            filename = os.path.basename(file_path)
            folder_name = os.path.basename(folder_path)
            grandparent_path = os.path.dirname(folder_path)
            grandparent_name = os.path.basename(grandparent_path)
            
            # =========================================================
            # 步骤 1: 识别信息
            # =========================================================
            tmdb_id = None
            search_query = None
            search_year = None

            tmdb_regex = r'(?:tmdb|tmdbid)[-_=\s]*(\d+)'
            match = re.search(tmdb_regex, folder_name, re.IGNORECASE)
            if not match:
                match = re.search(tmdb_regex, grandparent_name, re.IGNORECASE)
            if not match:
                match = re.search(tmdb_regex, filename, re.IGNORECASE)

            if match:
                temp_id = match.group(1)
                if is_valid_tmdb_id(temp_id):
                    tmdb_id = temp_id
                    logger.info(f"  ➜ [实时监控] 成功提取 TMDb ID: {tmdb_id}")
            if not tmdb_id:
                # 优化：先尝试从目录名提取搜索信息
                def is_season_folder(name: str) -> bool:
                    return bool(re.match(r'^(Season|S)\s*\d+|Specials', name, re.IGNORECASE))
                def extract_title_year(text: str):
                    year_regex = r'\b(19|20)\d{2}\b'
                    season_episode_regex = r'[sS](\d{1,2})[eE](\d{1,2})'
                    year_matches = list(re.finditer(year_regex, text))
                    se_match = re.search(season_episode_regex, text)
                    if year_matches:
                        last_year_match = year_matches[-1]
                        year = last_year_match.group(0)
                        raw_title = text[:last_year_match.start()]
                    elif se_match:
                        year = None
                        raw_title = text[:se_match.start()]
                    else:
                        year = None
                        raw_title = text
                    query = raw_title.replace('.', ' ').replace('_', ' ').strip(' -[]()')
                    return query, year

                # 首先尝试folder_name，但如果是季目录名，则换用grandparent_name
                if is_season_folder(folder_name):
                    search_query, search_year = extract_title_year(grandparent_name)
                else:
                    search_query, search_year = extract_title_year(folder_name)

                # 如果目录名都没提取到有效标题，再用filename
                if not search_query or search_query == '':
                    search_query, search_year = extract_title_year(os.path.splitext(filename)[0])

                logger.info(f"  ➜ [实时监控] 未找到ID，提取搜索信息: 标题='{search_query}', 年份='{search_year}'")

            # =========================================================
            # 步骤 2: 获取 TMDb 数据 (如果只有标题则搜索)
            # =========================================================
            if not tmdb_id and search_query:
                is_series_guess = bool(re.search(r'S\d+E\d+', filename, re.IGNORECASE))
                search_type = 'tv' if is_series_guess else 'movie'
                results = tmdb.search_media(search_query, self.tmdb_api_key, item_type=search_type, year=search_year)
                if results:
                    tmdb_id = str(results[0].get('id'))
                    logger.info(f"  ➜ [实时监控] 搜索匹配成功: {results[0].get('title') or results[0].get('name')} (ID: {tmdb_id})")
                else:
                    logger.warning(f"  ➜ [实时监控] 搜索失败，无法处理: {search_query}")
                    return None

            if not is_valid_tmdb_id(tmdb_id): 
                return None

            # 确定类型
            is_series = bool(re.search(r'S\d+E\d+', filename, re.IGNORECASE))
            item_type = "Series" if is_series else "Movie"

            # =========================================================
            # 极速查重 (利用文件名比对)
            # =========================================================
            try:
                # 获取该 TMDb ID 下所有已入库的文件名 (含电影和所有分集)
                known_files = media_db.get_known_filenames_by_tmdb_id(tmdb_id)
                current_filename = os.path.basename(file_path)
                
                if current_filename in known_files:
                    logger.info(f"  ➜ [实时监控] 文件已完美入库 ({current_filename})，直接跳过。")
                    return folder_path # 即使跳过处理，也返回路径以便后续刷新检查
            except Exception as e:
                logger.warning(f"  ➜ [实时监控] 查重失败，将继续常规流程: {e}")

            # =========================================================
            # ★★★ 核心升级：纯数据库缓存极速检查 (NFO 模式) ★★★
            # =========================================================
            should_skip_full_processing = False
            
            # 1. 数据库查询 (获取完整元数据 + 演员表)
            db_record = None
            db_actors = []
            
            with get_central_db_connection() as conn:
                cursor = conn.cursor()
                # A. 查主表
                cursor.execute(f"SELECT * FROM media_metadata WHERE tmdb_id = %s AND item_type = %s", (str(tmdb_id), item_type))
                row = cursor.fetchone()
                if row:
                    db_record = dict(row)
                    # B. 查演员 (如果主表存在)
                    if db_record.get('actors_json'):
                        try:
                            raw_actors = db_record['actors_json']
                            if isinstance(raw_actors, str):
                                actors_link = json.loads(raw_actors)
                            else:
                                actors_link = raw_actors

                            actor_tmdb_ids = [a['tmdb_id'] for a in actors_link if 'tmdb_id' in a]
                            if actor_tmdb_ids:
                                placeholders = ','.join(['%s'] * len(actor_tmdb_ids))
                                sql = f"""
                                    SELECT *, primary_name AS name, tmdb_person_id AS tmdb_id
                                    FROM person_metadata
                                    WHERE tmdb_person_id IN ({placeholders})
                                """
                                cursor.execute(sql, tuple(actor_tmdb_ids))
                                actor_rows = cursor.fetchall()
                                actor_map = {r['tmdb_id']: dict(r) for r in actor_rows}
                                
                                for link in actors_link:
                                    tid = link.get('tmdb_id')
                                    if tid in actor_map:
                                        full_actor = actor_map[tid].copy()
                                        full_actor['character'] = link.get('character')
                                        full_actor['order'] = link.get('order')
                                        db_actors.append(full_actor)
                                        
                                db_actors.sort(key=lambda x: x.get('order', 999))
                        except Exception as e:
                            logger.warning(f"  ➜ [实时监控] 从数据库解析演员失败: {e}")

            # 2. 决策逻辑分支 (只要数据库有数据且有演员，就是完美命中)
            if db_record and db_actors:
                logger.info(f"  ➜ [实时监控] 命中数据库缓存 (ID:{tmdb_id})。正在从数据库恢复元数据并生成 NFO...")
                try:
                    # 1. 生成主 payload
                    from tasks.helpers import reconstruct_metadata_from_db
                    payload = reconstruct_metadata_from_db(db_record, db_actors)

                    # 如果是剧集，需要查询并注入分季/分集数据
                    if item_type == "Series":
                        with get_central_db_connection() as conn:
                            cursor = conn.cursor()
                            
                            # A. 查分季
                            cursor.execute("SELECT * FROM media_metadata WHERE parent_series_tmdb_id = %s AND item_type = 'Season'", (str(tmdb_id),))
                            seasons_rows = cursor.fetchall()
                            seasons_data = []
                            for s_row in seasons_rows:
                                if not str(s_row['tmdb_id']).isdigit(): continue
                                s_data = {
                                    "id": int(s_row['tmdb_id']),
                                    "name": s_row['title'],
                                    "overview": s_row['overview'],
                                    "season_number": s_row['season_number'],
                                    "air_date": str(s_row['release_date']) if s_row['release_date'] else None,
                                    "poster_path": s_row['poster_path']
                                }
                                seasons_data.append(s_data)
                            
                            # B. 查分集
                            cursor.execute("SELECT * FROM media_metadata WHERE parent_series_tmdb_id = %s AND item_type = 'Episode'", (str(tmdb_id),))
                            episodes_rows = cursor.fetchall()
                            episodes_data = {}
                            
                            for e_row in episodes_rows:
                                if not str(e_row['tmdb_id']).isdigit(): continue
                                s_num = e_row['season_number']
                                e_num = e_row['episode_number']
                                key = f"S{s_num}E{e_num}"
                                e_data = {
                                    "id": int(e_row['tmdb_id']),
                                    "name": e_row['title'],
                                    "overview": e_row['overview'],
                                    "season_number": s_num,
                                    "episode_number": e_num,
                                    "air_date": str(e_row['release_date']) if e_row['release_date'] else None,
                                    "vote_average": e_row['rating'],
                                    "still_path": e_row['poster_path']
                                }
                                episodes_data[key] = e_data

                            if seasons_data: payload['seasons_details'] = seasons_data
                            if episodes_data: payload['episodes_details'] = episodes_data
                                
                            logger.info(f"  ➜ [实时监控] 已从数据库恢复 {len(seasons_data)} 个季和 {len(episodes_data)} 个分集的数据。")
                    
                    # =========================================================
                    # ★★★ 电影合集补全逻辑 (防止洗版丢失合集 NFO) ★★★
                    # =========================================================
                    if item_type == "Movie" and self.config.get(constants.CONFIG_OPTION_GENERATE_COLLECTION_NFO, False):
                        logger.info(f"  ➜ [实时监控] 电影合集 NFO 开关已开启，正在从 TMDb 获取合集信息以补全缓存...")
                        try:
                            movie_details = tmdb.get_movie_details(int(tmdb_id), self.tmdb_api_key)
                            if movie_details and movie_details.get("belongs_to_collection"):
                                enriched_collection = self._enrich_collection_info(movie_details.get("belongs_to_collection"))
                                payload["belongs_to_collection"] = enriched_collection
                                logger.info(f"  ➜ [实时监控] 成功补全合集信息: {enriched_collection.get('name')}")
                        except Exception as e:
                            logger.warning(f"  ➜ [实时监控] 补全合集信息失败: {e}")
                    # =========================================================

                    # 2. 构造上下文对象
                    fake_item_details = {
                        "Id": "pending", 
                        "Name": db_record.get('title'), 
                        "Type": item_type, 
                        "ProviderIds": {"Tmdb": tmdb_id},
                        "Path": file_path
                    }

                    # =========================================================
                    # ★★★ 新增：离线状态检测 (检查本地是否缺失图片) ★★★
                    # =========================================================
                    target_dir = folder_path
                    # 如果是剧集且当前在 Season 文件夹内，需要退回上一级根目录检查
                    if item_type == "Series" and re.match(r'^(Season|S)\s*\d+|Specials', os.path.basename(folder_path), re.IGNORECASE):
                        target_dir = os.path.dirname(folder_path)
                    
                    poster_path = os.path.join(target_dir, "poster.jpg")
                    if not os.path.exists(poster_path):
                        logger.info(f"  ➜ [实时监控] 检测到本地缺失海报 (离线恢复状态)，准备重新下载图片资产...")
                        self.download_images_from_tmdb(
                            tmdb_id=str(tmdb_id),
                            item_type=item_type,
                            aggregated_tmdb_data=payload if item_type == "Series" else None,
                            item_details=fake_item_details
                        )
                    # =========================================================
                    
                    # 3. 写入 NFO 文件 (传入 db_actors 确保演员表不丢失)
                    self.sync_item_metadata(
                        item_details=fake_item_details,
                        tmdb_id=str(tmdb_id),
                        final_cast_override=db_actors,
                        metadata_override=payload
                    )
                    should_skip_full_processing = True
                except Exception as e:
                    logger.error(f"  ➜ [实时监控] 从数据库恢复 NFO 失败: {e}，将回退到在线刮削。")
            else:
                logger.info(f"  ➜ [实时监控] 数据库无有效缓存 (ID:{tmdb_id})，准备执行 TMDb 在线刮削...")

            # =========================================================
            # 步骤 3: 获取完整详情 & 准备核心处理
            # =========================================================
            details = None
            aggregated_tmdb_data = None
            final_processed_cast = None

            if not should_skip_full_processing:
                time.sleep(random.uniform(0.5, 2.0))
                logger.info(f"  ➜ [实时监控] 正在获取 TMDb 详情并执行核心处理 (ID: {tmdb_id})...")
                
                if item_type == "Movie":
                    details = tmdb.get_movie_details(int(tmdb_id), self.tmdb_api_key)
                else:
                    aggregated_tmdb_data = tmdb.aggregate_full_series_data_from_tmdb(int(tmdb_id), self.tmdb_api_key)
                    details = aggregated_tmdb_data.get('series_details') if aggregated_tmdb_data else None
                    
                if not details:
                    logger.error("  ➜ [实时监控] 无法获取 TMDb 详情，中止处理。")
                    return None
                
                # 提取 TMDb 官方中文别名 & 卖片哥广告拦截
                raw_title = details.get("title") if item_type == "Movie" else details.get("name")
                # ★ 核心：第一时间清洗掉所有零宽字符和隐身符！
                current_title = utils.clean_invisible_chars(raw_title)
                
                # 1. 广告拦截：如果是垃圾标题，直接清空，强制进入后续的别名/翻译流程
                if utils.is_spam_title(current_title):
                    logger.warning(f"  ➜ [拦截] 拦截到恶意广告片名: '{current_title}'，准备寻找干净的别名或进行翻译...")
                    current_title = "" 

                # 2. 如果标题为空（被拦截）或不包含中文，则寻找别名
                if not current_title or not utils.contains_chinese(current_title):
                    chinese_alias = None
                    alt_titles_data = details.get("alternative_titles", {})
                    alt_list = alt_titles_data.get("titles") or alt_titles_data.get("results") or []
                    
                    priority_map = {"CN": 1, "SG": 2, "TW": 3, "HK": 4}
                    best_priority = 99
                    
                    for alt in alt_list:
                        # ★ 核心：对别名也必须进行隐身符清洗！
                        alt_title = utils.clean_invisible_chars(alt.get("title", ""))
                        
                        if utils.contains_chinese(alt_title) and not utils.is_spam_title(alt_title):
                            iso_country = alt.get("iso_3166_1", "").upper()
                            current_priority = priority_map.get(iso_country, 5)
                            
                            if current_priority < best_priority:
                                chinese_alias = alt_title
                                best_priority = current_priority
                                
                            if best_priority == 1:
                                break
                    
                    if chinese_alias:
                        logger.info(f"  ➜ 发现干净的 TMDb 官方中文别名: '{chinese_alias}'")
                        if item_type == "Movie":
                            details["title"] = chinese_alias
                        else:
                            details["name"] = chinese_alias
                            if aggregated_tmdb_data and "series_details" in aggregated_tmdb_data:
                                aggregated_tmdb_data["series_details"]["name"] = chinese_alias
                    else:
                        # ★ 核心：如果没有干净的中文别名，回退到原名，原名也要清洗！
                        raw_original = details.get("original_title") if item_type == "Movie" else details.get("original_name")
                        original_title = utils.clean_invisible_chars(raw_original)
                        
                        logger.info(f"  ➜ 未找到干净的中文别名，回退到原名: '{original_title}'，等待 AI 翻译。")
                        if item_type == "Movie":
                            details["title"] = original_title
                        else:
                            details["name"] = original_title
                            if aggregated_tmdb_data and "series_details" in aggregated_tmdb_data:
                                aggregated_tmdb_data["series_details"]["name"] = original_title
                
                # --- 标题与简介 AI 翻译 ---
                if self.ai_translator:
                    
                    # ====== 1. 简介翻译模块 ======
                    if self.config.get(constants.CONFIG_OPTION_AI_TRANSLATE_OVERVIEW, False):
                        current_overview = details.get("overview", "")
                        item_title = details.get("title") or details.get("name")

                        # 优先检查本地数据库缓存 (简介)
                        local_trans = media_db.get_local_translation_info(str(tmdb_id), item_type)
                        if local_trans and local_trans.get('overview') and utils.contains_chinese(local_trans['overview']):
                            details["overview"] = local_trans['overview']
                            if aggregated_tmdb_data and "series_details" in aggregated_tmdb_data:
                                aggregated_tmdb_data["series_details"]["overview"] = local_trans['overview']
                            logger.info(f"  ➜ [实时监控] 命中本地中文简介缓存，跳过AI翻译。")
                        
                        else:
                            # 判断是否需要翻译：简介为空 或 不包含中文
                            needs_translation = False
                            if not current_overview:
                                needs_translation = True
                            elif not utils.contains_chinese(current_overview):
                                needs_translation = True
                            
                            if needs_translation:
                                logger.info(f"  ➜ [实时监控] 检测到简介缺失或非中文，准备进行 AI 翻译...")
                                english_overview = ""
                                
                                # 1. 尝试使用现有的英文简介
                                if current_overview and len(current_overview) > 10:
                                    english_overview = current_overview
                                
                                # 2. 如果现有简介为空，尝试请求英文版数据
                                else:
                                    try:
                                        if item_type == "Movie":
                                            en_data = tmdb.get_movie_details(int(tmdb_id), self.tmdb_api_key, language="en-US")
                                            english_overview = en_data.get("overview")
                                        elif item_type == "Series":
                                            en_data = tmdb.get_tv_details(int(tmdb_id), self.tmdb_api_key, language="en-US")
                                            english_overview = en_data.get("overview")
                                    except Exception as e_en:
                                        logger.warning(f"  ➜ [实时监控] 获取英文源数据失败: {e_en}")

                                # 3. 调用 AI 翻译
                                if english_overview:
                                    translated_overview = self.ai_translator.translate_overview(english_overview, title=item_title)
                                    if translated_overview:
                                        details["overview"] = translated_overview
                                        logger.info(f"  ➜ [实时监控] 简介翻译成功，已更新内存数据。")
                                        if aggregated_tmdb_data and "series_details" in aggregated_tmdb_data:
                                            aggregated_tmdb_data["series_details"]["overview"] = translated_overview
                                    else:
                                        logger.warning(f"  ➜ [实时监控] AI 翻译未返回结果。")
                                else:
                                    logger.info(f"  ➜ [实时监控] 未能获取到有效的英文简介，跳过翻译。")

                    # ====== 2. 标题翻译模块 ======
                    if self.config.get(constants.CONFIG_OPTION_AI_TRANSLATE_TITLE, False):
                        # 获取当前标题
                        current_title = details.get("title") if item_type == "Movie" else details.get("name")
                        
                        # 优先检查本地数据库缓存 (标题)
                        local_trans = media_db.get_local_translation_info(str(tmdb_id), item_type)
                        if local_trans and local_trans.get('title') and utils.contains_chinese(local_trans['title']):
                            current_title = local_trans['title']
                            if item_type == "Movie":
                                details["title"] = current_title
                            else:
                                details["name"] = current_title
                                if aggregated_tmdb_data and "series_details" in aggregated_tmdb_data:
                                    aggregated_tmdb_data["series_details"]["name"] = current_title
                            logger.info(f"  ➜ [实时监控] 命中本地中文标题缓存，跳过AI翻译。")
                        
                        # 如果标题存在且不包含中文，则尝试翻译
                        elif current_title and not utils.contains_chinese(current_title):
                            logger.info(f"  ➜ [实时监控] 检测到标题为纯外文 ('{current_title}')，准备进行 AI 翻译...")
                            
                            release_date = details.get("release_date") if item_type == "Movie" else details.get("first_air_date")
                            year_str = release_date[:4] if release_date else ""
                            
                            translated_title = self.ai_translator.translate_title(
                                current_title, 
                                media_type=item_type, 
                                year=year_str
                            )
                            
                            if translated_title and utils.contains_chinese(translated_title):
                                logger.info(f"  ➜ [实时监控] 标题翻译成功: '{current_title}' -> '{translated_title}'")
                                if item_type == "Movie":
                                    details["title"] = translated_title
                                else:
                                    details["name"] = translated_title
                                    if aggregated_tmdb_data and "series_details" in aggregated_tmdb_data:
                                        aggregated_tmdb_data["series_details"]["name"] = translated_title
                            else:
                                logger.warning(f"  ➜ [实时监控] 标题翻译结果仍为外文或为空，丢弃: {translated_title}")
                    # ====== 3. 标语(Tagline)翻译模块 (跟随标题翻译开关) ======
                    if self.config.get(constants.CONFIG_OPTION_AI_TRANSLATE_TITLE, False):
                        current_tagline = details.get("tagline", "")
                        
                        # 如果标语为空，或者不包含中文，则尝试翻译
                        if not current_tagline or not utils.contains_chinese(current_tagline):
                            english_tagline = current_tagline if current_tagline else ""
                            
                            # 如果当前标语为空，主动请求英文版数据获取标语
                            if not english_tagline and self.tmdb_api_key:
                                try:
                                    if item_type == "Movie":
                                        en_data = tmdb.get_movie_details(int(tmdb_id), self.tmdb_api_key, language="en-US")
                                        english_tagline = en_data.get("tagline", "")
                                    elif item_type == "Series":
                                        en_data = tmdb.get_tv_details(int(tmdb_id), self.tmdb_api_key, language="en-US")
                                        english_tagline = en_data.get("tagline", "")
                                except Exception as e_en:
                                    logger.warning(f"  ➜ [实时监控] 获取英文标语失败: {e_en}")

                            if english_tagline:
                                current_title = details.get("title") if item_type == "Movie" else details.get("name")
                                logger.info(f"  ➜ [实时监控] 准备进行 AI 翻译标语: '{english_tagline}'")
                                
                                translated_tagline = self.ai_translator.translate_overview(english_tagline, title=current_title)
                                
                                if translated_tagline and utils.contains_chinese(translated_tagline):
                                    logger.info(f"  ➜ [实时监控] 标语翻译成功: '{translated_tagline}'")
                                    details["tagline"] = translated_tagline
                                    if aggregated_tmdb_data and "series_details" in aggregated_tmdb_data:
                                        aggregated_tmdb_data["series_details"]["tagline"] = translated_tagline

                # 准备演员源数据
                authoritative_cast_source = []
                if item_type == "Movie":
                    credits_source = details.get('credits') or details.get('casts') or {}
                    authoritative_cast_source = credits_source.get('cast', [])
                elif item_type == "Series":
                    if aggregated_tmdb_data:
                        all_episodes = list(aggregated_tmdb_data.get("episodes_details", {}).values())
                        authoritative_cast_source = _aggregate_series_cast_from_tmdb_data(details, all_episodes)
                    else:
                        credits_source = details.get('aggregate_credits') or details.get('credits') or {}
                        authoritative_cast_source = credits_source.get('cast', [])

                with get_db_connection() as conn:
                    cursor = conn.cursor()
                    dummy_emby_item = {
                        "Id": "pending",
                        "Name": details.get('title') or details.get('name'),
                        "OriginalTitle": details.get('original_title') or details.get('original_name'),
                        "People": [],
                        "Genres": details.get('genres', [])
                    }
                    logger.info(f"  ➜ [实时监控] 启动演员表核心处理 (AI翻译/去重/头像检查)...")
                    final_processed_cast = self._process_cast_list(
                        tmdb_cast_people=authoritative_cast_source,
                        emby_cast_people=[],
                        douban_cast_list=[],
                        item_details_from_emby=dummy_emby_item,
                        cursor=cursor,
                        tmdb_api_key=self.tmdb_api_key,
                        stop_event=None
                    )
                    
                    # ★★★ 新增：将核心导演也写入 person_metadata 单表 (实时监控模式) ★★★
                    try:
                        from tasks.helpers import extract_top_directors
                        top_directors = extract_top_directors(details, max_count=3)
                        emby_config_for_upsert = {"url": self.emby_url, "api_key": self.emby_api_key, "user_id": self.emby_user_id}
                        for director in top_directors:
                            if director.get('id'):
                                director_data = {
                                    "id": director.get("id"),
                                    "name": director.get("name"),
                                    "profile_path": director.get("profile_path"),
                                }
                                self.actor_db_manager.upsert_person(cursor, director_data, emby_config_for_upsert)
                        logger.debug(f"  ➜ [实时监控] 成功将 {len(top_directors)} 位导演信息同步至人员元数据库。")
                    except Exception as e_dir:
                        logger.warning(f"  ➜ [实时监控] 同步导演信息至数据库时失败: {e_dir}")
                        
                    conn.commit()

                if not final_processed_cast:
                    logger.warning("  ➜ [实时监控] 演员处理未能返回结果，将使用原始数据。")
                    final_processed_cast = authoritative_cast_source
            
            # =========================================================
            # 步骤 4 & 5: 生成本地 NFO 元数据文件 & 写入数据库
            # =========================================================
            if not should_skip_full_processing:
                # 1. 准备伪造的 Emby 对象
                fake_item_details = {
                    "Id": "pending",
                    "Name": details.get('title') or details.get('name'),
                    "Type": item_type,
                    "ProviderIds": {"Tmdb": tmdb_id},
                    "Path": file_path
                }

                logger.info(f"  ➜ [实时监控] 正在按照骨架模板格式化元数据...")

                # 2. 初始化骨架
                formatted_metadata = construct_metadata_payload(
                    item_type=item_type,
                    tmdb_data=details,
                    aggregated_tmdb_data=aggregated_tmdb_data
                )

                # 将提取到的导演强行塞入 formatted_metadata 供 NFO 使用 
                if item_type == "Series":
                    top_directors = extract_top_directors(details, max_count=3)
                    
                    if 'credits' not in formatted_metadata:
                        formatted_metadata['credits'] = {'crew': []}
                    elif 'crew' not in formatted_metadata['credits']:
                        formatted_metadata['credits']['crew'] = []
                        
                    existing_crew_ids = {c.get('id') for c in formatted_metadata['credits']['crew'] if c.get('job') in ['Director', 'Series Director']}
                    for d in top_directors:
                        if d['id'] not in existing_crew_ids:
                            formatted_metadata['credits']['crew'].append(d)

                # 3. 写入本地文件
                logger.info(f"  ➜ [实时监控] 正在写入本地元数据文件...")
                self.sync_item_metadata(
                    item_details=fake_item_details,
                    tmdb_id=tmdb_id,
                    final_cast_override=final_processed_cast,
                    metadata_override=formatted_metadata 
                )

                # 4. 写入数据库 (占位记录)
                logger.info(f"  ➜ [实时监控] 正在将元数据写入数据库 ...")
                with get_db_connection() as conn:
                    cursor = conn.cursor()
                    self._upsert_media_metadata(
                        cursor=cursor,
                        item_type=item_type,
                        final_processed_cast=final_processed_cast,
                        source_data_package=formatted_metadata, # 使用格式化后的数据
                        item_details_from_emby=fake_item_details # Id="pending"
                    )
                    conn.commit()

                # 5: 下载图片
                self.download_images_from_tmdb(
                    tmdb_id=tmdb_id,
                    item_type=item_type,
                    aggregated_tmdb_data=aggregated_tmdb_data,
                    item_details=fake_item_details
                )

            else:
                logger.info(f"  ➜ [实时监控] 已跳过在线刮削和元数据写入 (数据已通过缓存恢复)。")

            # =========================================================
            # 步骤 6: 通知 Emby 刷新 (可选)
            # =========================================================
            
            if not skip_refresh:
                logger.info(f"  ➜ [实时监控] 极速通知 Emby 单文件入库: {os.path.basename(file_path)}")
                emby.notify_emby_file_changes([file_path], self.emby_url, self.emby_api_key)
                logger.info(f"  ➜ [实时监控] 预处理完成，Emby 将进行秒级精准入库...")
            else:
                pass
                # logger.info(f"  ➜ [实时监控] 缓存已生成，等待批量极速通知...")
            
            # ★★★ 核心修改：直接返回具体的文件路径，不再返回父目录 ★★★
            return file_path

        except Exception as e:
            logger.error(f"  ➜ [实时监控] 处理文件 {file_path} 时发生错误: {e}", exc_info=True)
            return None

    # --- 批量实时监控处理 ---
    def process_file_actively_batch(self, file_paths: List[str]):
        """
        实时监控（批量版 - 极速优化）：
        针对短时间内涌入的多个文件，先逐个生成覆盖缓存，最后统一通过轻量级接口通知 Emby。
        彻底解决大剧集库刷新卡死、重复补图的问题。
        """
        if not file_paths:
            return

        logger.info(f"  ➜ [实时监控] 收到 {len(file_paths)} 个新任务，开始批量预处理...")
        
        valid_files_to_notify = set()
        
        # 1. 循环处理每个文件 (只生成缓存，不刷新)
        for i, file_path in enumerate(file_paths):
            try:
                logger.info(f"  ➜ [实时监控] ({i+1}/{len(file_paths)}) 正在处理: {os.path.basename(file_path)}")
                # 现在返回的是具体的文件路径
                processed_file = self.process_file_actively(file_path, skip_refresh=True)
                if processed_file:
                    valid_files_to_notify.add(processed_file)
            except Exception as e:
                logger.error(f"  ➜ [实时监控] 处理文件 '{file_path}' 失败: {e}")

        # 2. ★★★ 极速批量通知 Emby ★★★
        if valid_files_to_notify:
            #logger.info(f"  ➜ [实时监控] 预处理完成，正在向 Emby 发送 {len(valid_files_to_notify)} 个文件的极速入库通知...")
            
            # 直接把所有文件路径打包发给 Emby 的轻量级接口
            emby.notify_emby_file_changes(list(valid_files_to_notify), self.emby_url, self.emby_api_key)
            
            logger.info(f"  ➜ [实时监控] 预处理完成，等待视频流数据...")
        else:
            logger.warning(f"  ➜ [实时监控] 未收集到有效的文件路径，任务结束。")

    # --- 媒体库 GUID 映射维护 ---
    def _refresh_lib_guid_map(self):
        """从 Emby 实时获取所有媒体库的 ID 到 GUID 映射"""
        try:
            # 调用 emby.py 中的函数 (现在自带极速缓存)
            libs_data = emby.get_all_libraries_with_paths(self.emby_url, self.emby_api_key)
            new_map = {}
            for lib in libs_data:
                info = lib.get('info', {})
                l_id = str(info.get('Id'))
                l_guid = str(info.get('Guid'))
                if l_id and l_guid:
                    new_map[l_id] = l_guid
            
            self._global_lib_guid_map = new_map
            self._last_lib_map_update = time.time()
            # logger.debug(f"  ➜ 已刷新媒体库 GUID 映射表，共加载 {len(new_map)} 个库。") # ★ 注释掉啰嗦日志
        except Exception as e:
            logger.error(f"刷新媒体库 GUID 映射失败: {e}")

    # --- 实时获取项目的祖先地图和库 GUID ---
    def _get_realtime_ancestor_context(self, item_id: str, source_lib_id: str) -> Tuple[Dict[str, str], Optional[str]]:
        """
        实时获取项目的祖先地图和库 GUID。
        """
        id_to_parent_map = {}
        # 1. 获取 GUID 映射 (保持不变)
        if not self._global_lib_guid_map or (time.time() - self._last_lib_map_update > 3600):
            self._refresh_lib_guid_map()
        lib_guid = self._global_lib_guid_map.get(str(source_lib_id))

        # 3. 向上爬树构建父子关系（用于计算 ancestor_ids）
        try:
            curr_id = item_id
            for _ in range(10):
                # 实时入库只需要 ParentId 即可，不需要再请求 Guid 字段
                details = emby.get_emby_item_details(
                    curr_id, 
                    self.emby_url, 
                    self.emby_api_key, 
                    self.emby_user_id,
                    fields="ParentId",
                    silent_404=True
                )
                if not details: break
                
                p_id = details.get('ParentId')
                if p_id == str(source_lib_id) and lib_guid:
                    # 构造 Emby 特有的复合 ID: GUID_数字ID
                    composite_id = f"{lib_guid}_{p_id}"
                    id_to_parent_map[curr_id] = composite_id
                    # 复合 ID 的父级是系统根节点 "1"
                    id_to_parent_map[composite_id] = "1"
                    break 
                
                if p_id and p_id != '1':
                    id_to_parent_map[str(curr_id)] = p_id
                    curr_id = p_id
                else:
                    break
        except Exception as e:
            logger.error(f"实时构建爬树地图失败: {e}")

        return id_to_parent_map, lib_guid

    # --- 115 提取码和 SHA1 相关的核心函数 ---
    def _get_115_info_by_local_path(self, file_path: str) -> Tuple[Optional[str], Optional[str]]:
        """
        【挂载模式核心】通过 local_path 精准匹配 115 缓存表。
        返回 (pick_code, sha1)。
        """
        if not self.config.get("monitor_sha1_pc_search", True):
            return None, None
        
        if not file_path: return None, None
        
        # 统一路径分隔符
        normalized_path = file_path.replace('\\', '/')
        filename = os.path.basename(normalized_path)
        
        try:
            with get_central_db_connection() as conn:
                cursor = conn.cursor()
                
                # 1. 终极绝杀：利用 local_path 进行后缀匹配
                # 数据库存的是 "电影/科幻/阿凡达/阿凡达.mkv"
                # Emby 传的是 "/mnt/115/电影/科幻/阿凡达/阿凡达.mkv"
                cursor.execute("SELECT pick_code, sha1, local_path FROM p115_filesystem_cache WHERE name = %s AND local_path IS NOT NULL", (filename,))
                rows = cursor.fetchall()
                for row in rows:
                    db_local_path = row['local_path']
                    # 只要 Emby 的绝对路径以数据库的相对路径结尾，就是 100% 命中！
                    if normalized_path.endswith(db_local_path):
                        logger.debug(f"  ➜ [挂载模式] 路径命中: {db_local_path}")
                        return row['pick_code'], row['sha1']
                        
                # 2. 降级方案：三级目录联合匹配 (兼容以前没有写入 local_path 的老数据)
                path_parts = normalized_path.split('/')
                if len(path_parts) >= 3:
                    parent_name = path_parts[-2]
                    grandparent_name = path_parts[-3]
                    sql = """
                        SELECT c.pick_code, c.sha1 
                        FROM p115_filesystem_cache c
                        JOIN p115_filesystem_cache p ON c.parent_id = p.id
                        JOIN p115_filesystem_cache gp ON p.parent_id = gp.id
                        WHERE c.name = %s AND p.name = %s AND gp.name = %s
                        LIMIT 1
                    """
                    cursor.execute(sql, (filename, parent_name, grandparent_name))
                    row = cursor.fetchone()
                    if row: return row['pick_code'], row['sha1']
                    
        except Exception as e:
            logger.warning(f"通过路径查询 115 信息失败: {e}")
            
        return None, None

    # --- 直接从 STRM 文件、HTTP 链接 或 挂载路径中抠出 115 提取码 (PC) 和 SHA1 ---
    def _extract_115_fingerprints(self, file_path: str) -> Tuple[Optional[str], Optional[str]]:
        if not self.config.get("monitor_sha1_pc_search", True):
            return None, None
        
        if not file_path: return None, None
        
        pc = None
        sha1 = None
        target_path_for_db = file_path # 默认用传入的路径去查库

        # =========================================================
        # 🥇 优先级 1：嫡子 (STRM 模式) - 调用万能解析器
        # =========================================================
        try:
            if file_path.startswith('http'):
                pc = utils.extract_pickcode_from_strm_url(file_path)
            elif file_path.lower().endswith('.strm') and os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    # 尝试按 HTTP 链接解析
                    pc = utils.extract_pickcode_from_strm_url(content)
                    
                    # ★★★ 核心修复：如果 STRM 里面存的是挂载路径，把这个路径提取出来供后续查库 ★★★
                    if not pc and content and not content.startswith('http'):
                        target_path_for_db = content
        except Exception as e:
            logger.warning(f"读取 STRM 文件失败: {e}")
            
        # =========================================================
        # 🥈 优先级 2：庶子 (挂载模式) - 调用 local_path 精准匹配
        # =========================================================
        # 拿着真实的视频路径 (可能是直接传进来的，也可能是从 STRM 里读出来的) 去查库
        db_pc, db_sha1 = self._get_115_info_by_local_path(target_path_for_db)
        
        # 合并结果 (如果正则提取到了 PC 就用正则的，否则用数据库的)
        pc = pc or db_pc
        sha1 = db_sha1

        return pc, sha1

    # --- 通过 PC 码反查 SHA1 (自带 115 API 兜底) ---
    def _get_sha1_by_pickcode(self, pick_code: str) -> Optional[str]:
        if not self.config.get("monitor_sha1_pc_search", True):
            return None
        
        if not pick_code: return None
        
        # 1. 优先查本地数据库
        try:
            with get_central_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT sha1 FROM p115_filesystem_cache WHERE pick_code = %s AND sha1 IS NOT NULL LIMIT 1", (pick_code,))
                row = cursor.fetchone()
                if row and row['sha1']: 
                    return row['sha1']
        except Exception: pass

        # 2. 查不到？现场算 FID 调 API 查！(专治第三方 STRM)
        logger.trace(f"  ➜ 未在本地数据库找到 SHA1，尝试通过 115api 获取...")
        try:
            to_id_func = None
            try:
                from p115pickcode import to_id
                to_id_func = to_id
            except ImportError:
                try:
                    from p115client.tool.iterdir import to_id
                    to_id_func = to_id
                except ImportError:
                    pass

            if to_id_func:
                fid = str(to_id_func(pick_code))
                from handler.p115_service import P115Service
                client = P115Service.get_client()
                if client and fid:
                    info_res = client.fs_get_info(fid)
                    if info_res and info_res.get('state'):
                        sha1 = info_res['data'].get('sha1')
                        if sha1:
                            logger.info(f"  ➜ 成功通过 115 API 实时获取到 SHA1: {sha1}")
                            return sha1
        except Exception as e:
            logger.trace(f"  ➜ 实时获取 SHA1 失败: {e}")

        return None

    # --- 通过 PC 码反查 local_path (专治第三方 STRM 和挂载路径) ---
    def _get_local_path_by_pickcode(self, pick_code: str) -> Optional[str]:
        """
        通过 pick_code 反查 115 缓存表获取 local_path (相对路径)
        用于在 HTTP/STRM 模式下精准还原多版本文件的真实物理路径。
        """
        if not pick_code: return None
        try:
            with get_central_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT local_path FROM p115_filesystem_cache WHERE pick_code = %s AND local_path IS NOT NULL LIMIT 1", (pick_code,))
                row = cursor.fetchone()
                if row and row['local_path']: 
                    return row['local_path']
        except Exception as e:
            logger.warning(f"通过 pick_code 查询 local_path 失败: {e}")
        return None

    # --- 从数据库逆向重建完整元数据 ---
    def _reconstruct_full_data_from_db(self, tmdb_id: str, item_type: str) -> Tuple[Optional[Dict[str, Any]], Optional[List[Dict[str, Any]]]]:
        """
        从数据库逆向重建完整的元数据 Payload 和演员表。
        """
        try:
            with get_central_db_connection() as conn:
                cursor = conn.cursor()
                # 1. 查主表
                cursor.execute("SELECT * FROM media_metadata WHERE tmdb_id = %s AND item_type = %s", (str(tmdb_id), item_type))
                row = cursor.fetchone()
                if not row: return None, None
                
                db_record = dict(row)
                db_actors = []
                
                # 2. 查演员
                if db_record.get('actors_json'):
                    raw_actors = db_record['actors_json']
                    actors_link = json.loads(raw_actors) if isinstance(raw_actors, str) else raw_actors
                    actor_tmdb_ids = [a['tmdb_id'] for a in actors_link if 'tmdb_id' in a]
                    if actor_tmdb_ids:
                        placeholders = ','.join(['%s'] * len(actor_tmdb_ids))
                        sql = f"""
                            SELECT *, primary_name AS name, tmdb_person_id AS tmdb_id
                            FROM person_metadata
                            WHERE tmdb_person_id IN ({placeholders})
                        """
                        cursor.execute(sql, tuple(actor_tmdb_ids))
                        actor_map = {r['tmdb_id']: dict(r) for r in cursor.fetchall()}
                        for link in actors_link:
                            tid = link.get('tmdb_id')
                            if tid in actor_map:
                                full_actor = actor_map[tid].copy()
                                full_actor['id'] = tid 
                                full_actor['character'] = link.get('character')
                                full_actor['order'] = link.get('order')
                                db_actors.append(full_actor)
                        db_actors.sort(key=lambda x: x.get('order', 999))
                
                if not db_actors: return None, None

                # 3. 组装 Payload
                from tasks.helpers import reconstruct_metadata_from_db
                payload = reconstruct_metadata_from_db(db_record, db_actors)

                # 恢复标语
                if db_record.get('tagline'):
                    payload['tagline'] = db_record.get('tagline')

                # 4. 如果是剧集，补充季和集
                if item_type == "Series":
                    # A. 查分季
                    cursor.execute("SELECT * FROM media_metadata WHERE parent_series_tmdb_id = %s AND item_type = 'Season'", (str(tmdb_id),))
                    seasons_data = []
                    for s_row in cursor.fetchall():
                        seasons_data.append({
                            "id": int(s_row['tmdb_id']),
                            "name": s_row['title'],
                            "overview": s_row['overview'],
                            "season_number": s_row['season_number'],
                            "air_date": str(s_row['release_date']) if s_row['release_date'] else None,
                            "poster_path": s_row['poster_path']
                        })
                    
                    # B. 查分集
                    cursor.execute("SELECT * FROM media_metadata WHERE parent_series_tmdb_id = %s AND item_type = 'Episode'", (str(tmdb_id),))
                    episodes_data = {}
                    for e_row in cursor.fetchall():
                        s_num = e_row['season_number']
                        e_num = e_row['episode_number']
                        key = f"S{s_num}E{e_num}"
                        episodes_data[key] = {
                            "id": int(e_row['tmdb_id']) if str(e_row['tmdb_id']).isdigit() else e_row['tmdb_id'],
                            "name": e_row['title'],
                            "overview": e_row['overview'],
                            "season_number": s_num,
                            "episode_number": e_num,
                            "air_date": str(e_row['release_date']) if e_row['release_date'] else None,
                            "vote_average": e_row['rating'],
                            "still_path": e_row['poster_path']
                        }

                    if seasons_data: payload['seasons_details'] = seasons_data
                    if episodes_data: payload['episodes_details'] = episodes_data

                return payload, db_actors
        except Exception as e:
            logger.error(f"从数据库重建元数据失败: {e}")
            return None, None

    # --- 更新媒体元数据缓存 ---
    def _upsert_media_metadata(
        self,
        cursor: psycopg2.extensions.cursor,
        item_type: str,
        final_processed_cast: List[Dict[str, Any]],
        source_data_package: Optional[Dict[str, Any]],
        item_details_from_emby: Optional[Dict[str, Any]] = None,
        specific_episode_ids: Optional[List[str]] = None
    ):
        """
        - 实时元数据写入 (终极稳健版)。
        - 兼容 'pending' 预处理模式和 'webhook' 回流模式。
        - 修复了 ID=0 的脏数据问题。
        - 修复了回流时因类型不匹配导致无法标记入库的问题。
        - 【修复】多版本支持：现在会遍历并保存电影和分集的所有版本(MediaSources/Versions)的资产、SHA1和提取码。
        """
        if not item_details_from_emby:
            logger.error("  ➜ 写入元数据缓存失败：缺少 Emby 详情数据。")
            return
            
        item_id = str(item_details_from_emby.get('Id'))
        # 核心判断：是否为预处理/主动监控模式
        is_pending = (item_id == 'pending')

        # 初始化变量
        source_lib_id = ""
        id_to_parent_map = {}
        lib_guid = None
        
        # 只有在不是 pending 状态下，才去计算祖先链和库信息
        if not is_pending:
            source_lib_id = str(item_details_from_emby.get('_SourceLibraryId') or "")
            id_to_parent_map, lib_guid = self._get_realtime_ancestor_context(item_id, source_lib_id)

        def get_representative_runtime(emby_items, tmdb_runtime):
            if not emby_items: return tmdb_runtime
            runtimes = [round(item['RunTimeTicks'] / 600000000) for item in emby_items if item.get('RunTimeTicks')]
            return max(runtimes) if runtimes else tmdb_runtime
        
        def _extract_common_json_fields(details: Dict[str, Any], m_type: str):
            # 1. Genres (类型)
            genres_raw = details.get('genres', [])
            genres_list = []
            for g in genres_raw:
                if isinstance(g, dict): 
                    # TMDb 数据，有 ID
                    name = g.get('name')
                    if name in utils.GENRE_TRANSLATION_PATCH:
                        name = utils.GENRE_TRANSLATION_PATCH[name]
                    genres_list.append({"id": g.get('id', 0), "name": name})
                elif isinstance(g, str): 
                    # Emby 数据，无 ID，默认为 0
                    name = g
                    if name in utils.GENRE_TRANSLATION_PATCH:
                        name = utils.GENRE_TRANSLATION_PATCH[name]
                    genres_list.append({"id": 0, "name": name})
            
            genres_json = json.dumps(genres_list, ensure_ascii=False)

            # A. 制作公司 (Production Companies)
            raw_companies = details.get('production_companies') or []
            companies_list = []
            if isinstance(raw_companies, list):
                for c in raw_companies:
                    if isinstance(c, dict) and c.get('name'):
                        companies_list.append({'id': c.get('id'), 'name': c.get('name')})
            companies_json = json.dumps(companies_list, ensure_ascii=False)

            # B. 电视网 (Networks - 仅限剧集)
            raw_networks = details.get('networks') or []
            networks_list = []
            if isinstance(raw_networks, list):
                for n in raw_networks:
                    if isinstance(n, dict) and n.get('name'):
                        networks_list.append({'id': n.get('id'), 'name': n.get('name')})
            networks_json = json.dumps(networks_list, ensure_ascii=False)

            # 3. Keywords (关键词)
            keywords_data = details.get('keywords') or details.get('tags') or []
            raw_k_list = []
            if isinstance(keywords_data, dict):
                if m_type == 'Series': raw_k_list = keywords_data.get('results')
                else: raw_k_list = keywords_data.get('keywords')
                if not raw_k_list: raw_k_list = keywords_data.get('results') or keywords_data.get('keywords') or []
            elif isinstance(keywords_data, list):
                raw_k_list = keywords_data
            
            keywords = []
            for k in raw_k_list:
                if isinstance(k, dict) and k.get('name'): keywords.append({'id': k.get('id'), 'name': k.get('name')})
                elif isinstance(k, str) and k: keywords.append({'id': None, 'name': k})
            keywords_json = json.dumps(keywords, ensure_ascii=False)

            # 4. Countries (国家)
            countries_raw = details.get('production_countries') or details.get('origin_country') or []
            country_codes = []
            for c in countries_raw:
                if isinstance(c, dict): 
                    code = c.get('iso_3166_1')
                    if code: country_codes.append(code)
                elif isinstance(c, str) and c: country_codes.append(c)
            countries_json = json.dumps(country_codes, ensure_ascii=False)
            return genres_json, companies_json, networks_json, keywords_json, countries_json

        try:
            from psycopg2.extras import execute_batch
            
            if not source_data_package:
                logger.warning("  ➜ 元数据写入跳过：未提供源数据包。")
                return

            records_to_upsert = []

            # 生成向量逻辑
            overview_embedding_json = None
            if item_type in ["Movie", "Series"] and self.ai_translator and self.config.get(constants.CONFIG_OPTION_AI_VECTOR, False):
                overview_text = source_data_package.get('overview') or item_details_from_emby.get('Overview')
                if overview_text:
                    try:
                        embedding = self.ai_translator.generate_embedding(overview_text)
                        if embedding: overview_embedding_json = json.dumps(embedding)
                    except Exception as e_embed:
                        logger.warning(f"  ➜ 生成向量失败: {e_embed}")
            
            # ==================================================================
            # 处理电影 (Movie)
            # ==================================================================
            if item_type == "Movie":
                movie_record = source_data_package.copy()
                movie_record['item_type'] = 'Movie'
                movie_id = movie_record.get('id')
                movie_record['tmdb_id'] = str(movie_id) if movie_id else ""
                movie_record['runtime_minutes'] = get_representative_runtime([item_details_from_emby], movie_record.get('runtime'))
                movie_record['rating'] = movie_record.get('vote_average')
                
                # ★ 资产信息处理 (支持多版本)
                if is_pending:
                    movie_record['asset_details_json'] = '[]'
                    movie_record['emby_item_ids_json'] = '[]'
                    movie_record['file_sha1_json'] = '[]'
                    movie_record['file_pickcode_json'] = '[]'
                    movie_record['in_library'] = False
                else:
                    all_assets = []
                    all_ids = []  # ★ 修复 1：初始化为空列表
                    all_sha1s = []
                    all_pcs = []
                    
                    media_sources = item_details_from_emby.get('MediaSources', [])
                    
                    # 如果有多个媒体源（多版本）
                    if media_sources and len(media_sources) > 0:
                        for source in media_sources:
                            raw_path = source.get('Path', '')
                            if not raw_path: continue
                            
                            # 先提取 PC 码 (支持直接从 HTTP 链接提取)
                            file_pc, file_sha1 = self._extract_115_fingerprints(raw_path)
                            if not file_sha1 and file_pc:
                                file_sha1 = self._get_sha1_by_pickcode(file_pc)
                            
                            # ★ 提取当前版本的真实 ID，并强制剥离 mediasource_ 前缀
                            raw_source_id = str(source.get('Id') or item_id)
                            source_id = raw_source_id.replace("mediasource_", "")
                            
                            # ★★★ 终极修复：利用 local_strm_root + local_path 完美还原物理路径 ★★★
                            emby_path = raw_path
                            if emby_path.startswith('http'):
                                main_emby_path = item_details_from_emby.get('Path', '')
                                if source_id == item_id:
                                    # 主版本：直接使用顶层 Path
                                    emby_path = main_emby_path
                                else:
                                    # 辅版本：查库获取相对路径，与 local_strm_root 拼接
                                    db_local_path = self._get_local_path_by_pickcode(file_pc)
                                    if db_local_path:
                                        local_strm_root = self.config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT, "")
                                        if local_strm_root:
                                            # 完美拼接：/strm/媒体库 + 电影/.../xxx.mkv
                                            emby_path = os.path.join(local_strm_root, db_local_path.lstrip('/\\'))
                                        elif main_emby_path:
                                            # 兜底：如果没配 root，用主版本的目录 + 真实文件名
                                            real_filename = os.path.basename(db_local_path.replace('\\', '/'))
                                            base_dir = os.path.dirname(main_emby_path)
                                            emby_path = os.path.join(base_dir, real_filename)
                                        else:
                                            emby_path = ''
                                    else:
                                        emby_path = ''
                                
                            mediainfo_path = os.path.splitext(emby_path)[0] + "-mediainfo.json" if emby_path and not emby_path.startswith('http') else None
                            file_sha1 = self._get_sha1_by_pickcode(file_pc)
                            
                            # 构造临时 item 传递给 parse_full_asset_details，确保解析的是当前版本的属性
                            temp_item = item_details_from_emby.copy()
                            
                            temp_item['Id'] = source_id 
                            temp_item['Path'] = emby_path
                            if 'Container' in source: temp_item['Container'] = source['Container']
                            if 'Size' in source: temp_item['Size'] = source['Size']
                            if 'RunTimeTicks' in source: temp_item['RunTimeTicks'] = source['RunTimeTicks']
                            
                            # ★ 清除顶层分辨率污染，防止主版本的 Width/Height 污染辅版本
                            temp_item.pop('Width', None)
                            temp_item.pop('Height', None)
                            
                            # ★ 将当前版本的专属流信息强行覆盖到顶层
                            if 'MediaStreams' in source:
                                temp_item['MediaStreams'] = source['MediaStreams']
                            
                            # ★ 隔离污染，让底层函数只看到当前这一个 source
                            temp_item['MediaSources'] = [source]
                            
                            asset_details = parse_full_asset_details(
                                temp_item, 
                                id_to_parent_map=id_to_parent_map, 
                                library_guid=lib_guid,
                                local_mediainfo_path=mediainfo_path 
                            )
                            asset_details['source_library_id'] = source_lib_id

                            all_assets.append(asset_details)
                            all_ids.append(source_id)
                            if file_pc: all_pcs.append(file_pc)
                            if file_sha1: all_sha1s.append(file_sha1)
                    else:
                        # 兜底逻辑：如果没有 MediaSources，使用主 Path
                        emby_path = item_details_from_emby.get('Path', '')
                        mediainfo_path = os.path.splitext(emby_path)[0] + "-mediainfo.json"
                        
                        file_pc, file_sha1 = self._extract_115_fingerprints(raw_path)
                        if not file_sha1 and file_pc:
                            file_sha1 = self._get_sha1_by_pickcode(file_pc)

                        asset_details = parse_full_asset_details(
                            item_details_from_emby, 
                            id_to_parent_map=id_to_parent_map, 
                            library_guid=lib_guid,
                            local_mediainfo_path=mediainfo_path 
                        )
                        asset_details['source_library_id'] = source_lib_id

                        all_assets.append(asset_details)
                        all_ids.append(item_id) # ★ 修复 4：兜底时收集主 ID
                        if file_pc: all_pcs.append(file_pc)
                        if file_sha1: all_sha1s.append(file_sha1)
                    
                    # 使用 dict.fromkeys 去重并保持顺序
                    movie_record['asset_details_json'] = json.dumps(all_assets, ensure_ascii=False)
                    movie_record['emby_item_ids_json'] = json.dumps(list(dict.fromkeys(all_ids)))
                    movie_record['file_sha1_json'] = json.dumps(list(dict.fromkeys(all_sha1s)))
                    movie_record['file_pickcode_json'] = json.dumps(list(dict.fromkeys(all_pcs)))
                    movie_record['in_library'] = True

                movie_record['actors_json'] = json.dumps([{"tmdb_id": int(p.get("id")), "character": p.get("character"), "order": p.get("order")} for p in final_processed_cast if p.get("id")], ensure_ascii=False)
                movie_record['subscription_status'] = 'NONE'
                movie_record['date_added'] = item_details_from_emby.get("DateCreated") or datetime.now(timezone.utc)
                movie_record['overview_embedding'] = overview_embedding_json

                # 通用字段
                g_json, comp_json, net_json, k_json, c_json = _extract_common_json_fields(source_data_package, 'Movie')
                movie_record['genres_json'] = g_json
                movie_record['production_companies_json'] = comp_json 
                movie_record['networks_json'] = net_json
                movie_record['keywords_json'] = k_json
                movie_record['countries_json'] = c_json

                # 分级处理 
                raw_ratings_map = source_data_package.get('_official_rating_map', {})
                movie_record['official_rating_json'] = json.dumps(raw_ratings_map, ensure_ascii=False)
                
                releases = source_data_package.get('releases', {}).get('countries', [])
                for r in releases:
                    country = r.get('iso_3166_1')
                    cert = r.get('certification')
                    if country and cert: raw_ratings_map[country] = cert
                
                movie_record['official_rating_json'] = json.dumps(raw_ratings_map, ensure_ascii=False)
                
                credits_data = source_data_package.get("credits") or source_data_package.get("casts") or {}
                crew = credits_data.get('crew', [])
                movie_record['directors_json'] = json.dumps([{'id': p.get('id'), 'name': p.get('name')} for p in crew if p.get('job') == 'Director'], ensure_ascii=False)

                records_to_upsert.append(movie_record)

            # ==================================================================
            # 处理剧集 (Series)
            # ==================================================================
            elif item_type == "Series":
                series_details = source_data_package.get("series_details", source_data_package)
                seasons_details = source_data_package.get("seasons_details", series_details.get("seasons", []))
                
                series_asset_details = []
                # ★ Pending 模式下不处理资产路径
                if not is_pending:
                    series_path = item_details_from_emby.get('Path')
                    if series_path:
                        series_asset = {
                            "path": series_path,
                            "source_library_id": source_lib_id,
                            "ancestor_ids": calculate_ancestor_ids(item_id, id_to_parent_map, lib_guid)
                        }
                        series_asset_details.append(series_asset)

                # 构建 Series 记录
                series_record = {
                    "item_type": "Series", "tmdb_id": str(series_details.get('id')) if series_details.get('id') else "", "title": series_details.get('name'),
                    "original_title": series_details.get('original_name'), "overview": series_details.get('overview'),
                    "tagline": series_details.get('tagline'),
                    "release_date": series_details.get('first_air_date'), 
                    "last_air_date": series_details.get('last_air_date'),
                    "poster_path": series_details.get('poster_path'),
                    "backdrop_path": series_details.get('backdrop_path'), 
                    "homepage": series_details.get('homepage'),
                    "rating": series_details.get('vote_average'),
                    "total_episodes": series_details.get('number_of_episodes', 0),
                    "watchlist_tmdb_status": series_details.get('status'),
                    "asset_details_json": json.dumps(series_asset_details, ensure_ascii=False),
                    "overview_embedding": overview_embedding_json
                }
                
                # ★ 状态标记
                if is_pending:
                    series_record['in_library'] = False
                    series_record['emby_item_ids_json'] = '[]'
                    series_record['file_sha1_json'] = '[]'
                else:
                    series_record['in_library'] = True
                    series_record['emby_item_ids_json'] = json.dumps([item_id])
                    series_record['file_sha1_json'] = '[]'

                actors_relation = [{"tmdb_id": int(p.get("id")), "character": p.get("character"), "order": p.get("order")} for p in final_processed_cast if p.get("id")]
                series_record['actors_json'] = json.dumps(actors_relation, ensure_ascii=False)
                
                # 分级处理
                raw_ratings_map = source_data_package.get('_official_rating_map', {})
                series_record['official_rating_json'] = json.dumps(raw_ratings_map, ensure_ascii=False)

                # 通用字段
                g_json, comp_json, net_json, k_json, c_json = _extract_common_json_fields(series_details, 'Series')
                series_record['genres_json'] = g_json
                series_record['production_companies_json'] = comp_json
                series_record['networks_json'] = net_json
                series_record['keywords_json'] = k_json
                series_record['countries_json'] = c_json
                
                # ★★★ 综合提取剧集导演 (created_by + crew) ★★★
                top_directors = extract_top_directors(series_details, max_count=3)
                series_record['directors_json'] = json.dumps([{'id': d['id'], 'name': d['name']} for d in top_directors], ensure_ascii=False)
                
                languages_list = series_details.get('languages', [])
                series_record['original_language'] = series_details.get('original_language') or (languages_list[0] if languages_list else None)
                series_record['subscription_status'] = 'NONE'
                series_record['date_added'] = item_details_from_emby.get("DateCreated") or datetime.now(timezone.utc)
                series_record['ignore_reason'] = None
                records_to_upsert.append(series_record)

                # ★★★ 3. 处理季 (Season) ★★★
                emby_season_versions = []
                # ★ Pending 模式下跳过 Emby 查询
                if not is_pending:
                    emby_season_versions = emby.get_series_seasons(
                        series_id=item_details_from_emby.get('Id'),
                        base_url=self.emby_url,
                        api_key=self.emby_api_key,
                        user_id=self.emby_user_id,
                        series_name_for_log=series_details.get('name')
                    ) or []
                
                seasons_grouped_by_number = defaultdict(list)
                for s_ver in emby_season_versions:
                    # 强制转 int，防止类型不匹配
                    idx = s_ver.get("IndexNumber")
                    if idx is not None:
                        try: seasons_grouped_by_number[int(idx)].append(s_ver)
                        except: pass

                for season in seasons_details:
                    if not isinstance(season, dict): continue
                    
                    # ★★★ 核心修复：严防死守 ID=0 ★★★
                    s_tmdb_id = season.get('id')
                    if not s_tmdb_id or str(s_tmdb_id) in ['0', 'None', '']:
                        continue

                    s_num = season.get('season_number')
                    if s_num is None: continue 
                    try: s_num_int = int(s_num)
                    except ValueError: continue

                    season_poster = season.get('poster_path') or series_details.get('poster_path')
                    matched_emby_seasons = seasons_grouped_by_number.get(s_num_int, [])

                    # ★ 提取所有匹配到的季文件夹 ID
                    season_ids = [s['Id'] for s in matched_emby_seasons] if matched_emby_seasons else []
                    
                    records_to_upsert.append({
                        "tmdb_id": str(s_tmdb_id), "item_type": "Season", 
                        "parent_series_tmdb_id": str(series_details.get('id')), 
                        "title": season.get('name'), "overview": season.get('overview'), 
                        "release_date": season.get('air_date'), "poster_path": season_poster, 
                        "season_number": s_num,
                        "total_episodes": season.get('episode_count', 0),
                        "in_library": bool(matched_emby_seasons) if not is_pending else False,
                        "emby_item_ids_json": json.dumps(season_ids),
                        "file_sha1_json": '[]'
                    })
                
                # ★★★ 4. 处理分集 (Episode) ★★★
                raw_episodes = source_data_package.get("episodes_details", {})
                # 兼容字典(S1E1: data)和列表两种格式
                episodes_details = list(raw_episodes.values()) if isinstance(raw_episodes, dict) else (raw_episodes if isinstance(raw_episodes, list) else [])
                
                emby_episode_versions = []
                if not is_pending:
                    emby_episode_versions = emby.get_all_library_versions(
                        base_url=self.emby_url, api_key=self.emby_api_key, user_id=self.emby_user_id,
                        media_type_filter="Episode", parent_id=item_details_from_emby.get('Id'),
                        fields="Id,Type,ParentIndexNumber,IndexNumber,MediaStreams,Container,Size,Path,ProviderIds,RunTimeTicks,DateCreated,_SourceLibraryId"
                    ) or []
                
                episodes_grouped_by_number = defaultdict(list)
                for ep_version in emby_episode_versions:
                    s_num = ep_version.get("ParentIndexNumber")
                    e_num = ep_version.get("IndexNumber")
                    if s_num is not None and e_num is not None:
                        try: episodes_grouped_by_number[(int(s_num), int(e_num))].append(ep_version)
                        except: pass

                processed_emby_episodes = set() # ★ 新增：记录已处理的 Emby 分集

                for episode in episodes_details:
                    # 1. 必须有季号和集号 (提前解析，用于生成内部ID)
                    if episode.get('episode_number') is None: continue
                    try:
                        s_num = int(episode.get('season_number'))
                        e_num = int(episode.get('episode_number'))
                    except (ValueError, TypeError): continue

                    # ★★★ 核心修改：允许缺失 TMDb ID 的分集使用内部兜底 ID 入库 ★★★
                    e_tmdb_id = episode.get('id')
                    e_tmdb_id_str = str(e_tmdb_id) if e_tmdb_id else ""
                    
                    if e_tmdb_id_str in ['0', 'None', ''] or not e_tmdb_id_str.isdigit():
                        e_tmdb_id_str = f"{series_details.get('id')}-S{s_num}E{e_num}"

                    versions_of_episode = episodes_grouped_by_number.get((s_num, e_num))

                    if versions_of_episode:
                        processed_emby_episodes.add((s_num, e_num)) # ★ 记录已处理

                    # 追更模式下，跳过非目标分集，避免全量读写 
                    if specific_episode_ids and not is_pending:
                        is_target = False
                        if versions_of_episode:
                            for v in versions_of_episode:
                                if str(v.get('Id')) in specific_episode_ids:
                                    is_target = True
                                    break
                        if not is_target:
                            continue # 不是本次入库的分集，直接跳过！

                    final_runtime = get_representative_runtime(versions_of_episode, episode.get('runtime'))

                    # ★★★ 提取分集专属导演 ★★★
                    ep_crew = episode.get('crew', [])
                    ep_directors = [{'id': p.get('id'), 'name': p.get('name')} for p in ep_crew if p.get('job') == 'Director']

                    episode_record = {
                        "tmdb_id": e_tmdb_id_str, 
                        "item_type": "Episode", 
                        "parent_series_tmdb_id": str(series_details.get('id')), 
                        "title": episode.get('name'), "overview": episode.get('overview'), 
                        "release_date": episode.get('air_date'), 
                        "season_number": s_num, "episode_number": e_num,
                        "runtime_minutes": final_runtime,
                        "poster_path": episode.get('still_path'),
                        "backdrop_path": episode.get('still_path'),
                        "directors_json": json.dumps(ep_directors, ensure_ascii=False) # 新增写入
                    }
                    
                    # ★ 资产信息处理 (支持多版本)
                    if not is_pending and versions_of_episode:
                        all_assets = []
                        all_ids = []
                        all_sha1s = []
                        all_pcs = []
                        
                        # 遍历该集的所有版本
                        for version in versions_of_episode:
                            raw_path = version.get('Path', '')
                            
                            # ★ 强制剥离 mediasource_ 前缀
                            clean_v_id = str(version.get('Id')).replace("mediasource_", "")
                            
                            file_pc, file_sha1 = self._extract_115_fingerprints(raw_path)
                            if not file_sha1 and file_pc:
                                file_sha1 = self._get_sha1_by_pickcode(file_pc)
                            
                            # ★★★ 终极修复：利用 local_strm_root + local_path 完美还原物理路径 ★★★
                            emby_path = raw_path
                            if emby_path.startswith('http'):
                                main_emby_path = item_details_from_emby.get('Path', '')
                                if clean_v_id == item_id:
                                    # 主版本：直接使用顶层 Path
                                    emby_path = main_emby_path
                                else:
                                    # 辅版本：查库获取相对路径，与 local_strm_root 拼接
                                    db_local_path = self._get_local_path_by_pickcode(file_pc)
                                    if db_local_path:
                                        local_strm_root = self.config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT, "")
                                        if local_strm_root:
                                            emby_path = os.path.join(local_strm_root, db_local_path.lstrip('/\\'))
                                        elif main_emby_path:
                                            real_filename = os.path.basename(db_local_path.replace('\\', '/'))
                                            base_dir = os.path.dirname(main_emby_path)
                                            emby_path = os.path.join(base_dir, real_filename)
                                        else:
                                            emby_path = ''
                                    else:
                                        emby_path = ''
                                
                            mediainfo_path = os.path.splitext(emby_path)[0] + "-mediainfo.json" if emby_path and not emby_path.startswith('http') else None
                            file_sha1 = self._get_sha1_by_pickcode(file_pc)
                            
                            temp_version = version.copy()
                            temp_version['Path'] = emby_path
                            
                            details = parse_full_asset_details(
                                temp_version,
                                local_mediainfo_path=mediainfo_path
                            )
                            details['source_library_id'] = item_details_from_emby.get('_SourceLibraryId')

                            all_assets.append(details)
                            all_ids.append(clean_v_id)
                            
                            if file_sha1: all_sha1s.append(file_sha1)
                            if file_pc: all_pcs.append(file_pc)
                            
                        episode_record['asset_details_json'] = json.dumps(all_assets, ensure_ascii=False)
                        # 使用 dict.fromkeys 去重并保持顺序
                        episode_record['emby_item_ids_json'] = json.dumps(list(dict.fromkeys(all_ids)))
                        episode_record['file_sha1_json'] = json.dumps(list(dict.fromkeys(all_sha1s)))
                        episode_record['file_pickcode_json'] = json.dumps(list(dict.fromkeys(all_pcs)))
                        episode_record['in_library'] = True
                    else:
                        episode_record['in_library'] = False
                        episode_record['emby_item_ids_json'] = '[]'
                        episode_record['asset_details_json'] = '[]'
                        episode_record['file_sha1_json'] = '[]'
                        episode_record['file_pickcode_json'] = '[]'
                        
                    records_to_upsert.append(episode_record)

                # ★★★ 新增：兜底处理 Emby 中存在，但 TMDb 中完全没有的分集 ★★★
                for (s_num, e_num), versions in episodes_grouped_by_number.items():
                    if (s_num, e_num) in processed_emby_episodes:
                        continue

                    # 追更模式下，跳过非目标分集
                    if specific_episode_ids and not is_pending:
                        is_target = False
                        for v in versions:
                            if str(v.get('Id')) in specific_episode_ids:
                                is_target = True
                                break
                        if not is_target:
                            continue

                    fallback_e_tmdb_id = f"{series_details.get('id')}-S{s_num}E{e_num}"
                    logger.debug(f"  ➜ [入库兜底] 发现 Emby 本地分集 S{s_num}E{e_num} 在 TMDb 中不存在，生成内部 ID: {fallback_e_tmdb_id}")

                    emby_ep = versions[0]
                    final_runtime = round(emby_ep['RunTimeTicks'] / 600000000) if emby_ep.get('RunTimeTicks') else None

                    episode_record = {
                        "tmdb_id": fallback_e_tmdb_id, 
                        "item_type": "Episode", 
                        "parent_series_tmdb_id": str(series_details.get('id')), 
                        "title": emby_ep.get('Name') or f"Episode {e_num}", 
                        "overview": emby_ep.get('Overview'), 
                        "release_date": emby_ep.get('PremiereDate'), 
                        "season_number": s_num, "episode_number": e_num,
                        "runtime_minutes": final_runtime,
                        "poster_path": None,
                        "backdrop_path": None,
                        "directors_json": "[]"   
                    }

                    all_assets = []
                    all_ids = []
                    all_sha1s = []
                    all_pcs = []
                    
                    for version in versions:
                        raw_path = version.get('Path', '')
                        clean_v_id = str(version.get('Id')).replace("mediasource_", "")
                        
                        file_pc, file_sha1 = self._extract_115_fingerprints(raw_path)
                        if not file_sha1 and file_pc:
                            file_sha1 = self._get_sha1_by_pickcode(file_pc)
                        
                        emby_path = raw_path
                        if emby_path.startswith('http'):
                            main_emby_path = item_details_from_emby.get('Path', '')
                            if clean_v_id == item_id:
                                emby_path = main_emby_path
                            else:
                                db_local_path = self._get_local_path_by_pickcode(file_pc)
                                if db_local_path:
                                    local_strm_root = self.config.get(constants.CONFIG_OPTION_LOCAL_STRM_ROOT, "")
                                    if local_strm_root:
                                        emby_path = os.path.join(local_strm_root, db_local_path.lstrip('/\\'))
                                    elif main_emby_path:
                                        real_filename = os.path.basename(db_local_path.replace('\\', '/'))
                                        base_dir = os.path.dirname(main_emby_path)
                                        emby_path = os.path.join(base_dir, real_filename)
                                    else:
                                        emby_path = ''
                                else:
                                    emby_path = ''
                            
                        mediainfo_path = os.path.splitext(emby_path)[0] + "-mediainfo.json" if emby_path and not emby_path.startswith('http') else None
                        file_sha1 = self._get_sha1_by_pickcode(file_pc)
                        
                        temp_version = version.copy()
                        temp_version['Path'] = emby_path
                        
                        details = parse_full_asset_details(
                            temp_version,
                            local_mediainfo_path=mediainfo_path
                        )
                        details['source_library_id'] = item_details_from_emby.get('_SourceLibraryId')

                        all_assets.append(details)
                        all_ids.append(clean_v_id)
                        
                        if file_sha1: all_sha1s.append(file_sha1)
                        if file_pc: all_pcs.append(file_pc)
                        
                    episode_record['asset_details_json'] = json.dumps(all_assets, ensure_ascii=False)
                    episode_record['emby_item_ids_json'] = json.dumps(list(dict.fromkeys(all_ids)))
                    episode_record['file_sha1_json'] = json.dumps(list(dict.fromkeys(all_sha1s)))
                    episode_record['file_pickcode_json'] = json.dumps(list(dict.fromkeys(all_pcs)))
                    episode_record['in_library'] = True

                    records_to_upsert.append(episode_record)

            if not records_to_upsert:
                return
            
            # ==================================================================
            # 批量写入数据库 (带指纹保护机制)
            # ==================================================================
            all_possible_columns = [
                "tmdb_id", "item_type", "title", "original_title", "overview", "release_date", "release_year",
                "last_air_date", "backdrop_path", "homepage", "original_language", "poster_path", "rating", 
                "actors_json", "parent_series_tmdb_id", "season_number", "episode_number", "in_library", 
                "subscription_status", "subscription_sources_json", "emby_item_ids_json", 
                "file_sha1_json", "file_pickcode_json", 
                "date_added", "official_rating_json", "genres_json", "directors_json", "production_companies_json", 
                "networks_json", "countries_json", "keywords_json", "ignore_reason", "asset_details_json",
                "runtime_minutes", "overview_embedding", "total_episodes", "watchlist_tmdb_status",
                "imdb_id", "tagline"
            ]
            data_for_batch = []
            for record in records_to_upsert:
                # 再次检查 ID，防止漏网之鱼
                rec_id = record.get('tmdb_id')
                rec_type = record.get('item_type')
                
                # 放宽对 Season 和 Episode 的校验，允许内部兜底 ID (包含 '-') 入库 ★★★
                is_valid = False
                if rec_type in ['Movie', 'Series']:
                    is_valid = is_valid_tmdb_id(rec_id) # 顶层项目必须是纯数字的真实 TMDb ID
                elif rec_type in ['Season', 'Episode']:
                    # 子项目允许是纯数字，也允许是带 '-' 的内部兜底 ID
                    if rec_id and (is_valid_tmdb_id(rec_id) or '-' in str(rec_id)):
                        is_valid = True

                if not is_valid:
                    logger.warning(f"  ➜ [入库拦截] 发现无效的 TMDb ID: '{rec_id}' (类型: {rec_type})，已丢弃该条记录。")
                    continue
                # 确保继承顶层的 IMDb/TVDb ID
                if not record.get('imdb_id'): record['imdb_id'] = source_data_package.get('imdb_id')
                db_row_complete = {col: record.get(col) for col in all_possible_columns}
                
                if db_row_complete['in_library'] is None: db_row_complete['in_library'] = False
                if db_row_complete['subscription_status'] is None: db_row_complete['subscription_status'] = 'NONE'
                if db_row_complete['subscription_sources_json'] is None: db_row_complete['subscription_sources_json'] = '[]'
                if db_row_complete['emby_item_ids_json'] is None: db_row_complete['emby_item_ids_json'] = '[]'
                if db_row_complete['file_sha1_json'] is None: db_row_complete['file_sha1_json'] = '[]'
                if db_row_complete['file_pickcode_json'] is None: db_row_complete['file_pickcode_json'] = '[]'

                r_date = db_row_complete.get('release_date')
                if not r_date: db_row_complete['release_date'] = None
                
                l_date = db_row_complete.get('last_air_date')
                if not l_date: db_row_complete['last_air_date'] = None

                final_date_val = db_row_complete.get('release_date')
                if final_date_val and isinstance(final_date_val, str) and len(final_date_val) >= 4:
                    try: db_row_complete['release_year'] = int(final_date_val[:4])
                    except (ValueError, TypeError): pass
                
                data_for_batch.append(db_row_complete)

            if not data_for_batch:
                return

            cols_str = ", ".join(all_possible_columns)
            placeholders_str = ", ".join([f"%({col})s" for col in all_possible_columns])
            cols_to_update = [col for col in all_possible_columns if col not in ['tmdb_id', 'item_type', 'custom_rating']]
            
            cols_to_protect = ['subscription_sources_json']
            timestamp_field = "last_synced_at"
            
            for col in cols_to_protect:
                if col in cols_to_update: cols_to_update.remove(col)

            update_clauses = []
            for col in cols_to_update:
                # 针对 total_episodes 字段，检查锁定状态
                # 逻辑：如果 total_episodes_locked 为 TRUE，则保持原值；否则使用新值 (EXCLUDED.total_episodes)
                if col == 'total_episodes':
                    update_clauses.append(
                        "total_episodes = CASE WHEN media_metadata.total_episodes_locked IS TRUE THEN media_metadata.total_episodes ELSE EXCLUDED.total_episodes END"
                    )
                else:
                    # 其他字段正常更新
                    update_clauses.append(f"{col} = EXCLUDED.{col}")

            update_clauses.append(f"{timestamp_field} = NOW()")

            sql = f"""
                INSERT INTO media_metadata ({cols_str})
                VALUES ({placeholders_str})
                ON CONFLICT (tmdb_id, item_type) DO UPDATE SET {', '.join(update_clauses)};
            """
            
            execute_batch(cursor, sql, data_for_batch)
            logger.info(f"  ➜ 成功将 {len(data_for_batch)} 条层级元数据记录批量写入数据库。")

        except Exception as e:
            logger.error(f"批量写入层级元数据到数据库时失败: {e}", exc_info=True)
            raise

    # --- 标记为已处理 ---
    def _mark_item_as_processed(self, cursor: psycopg2.extensions.cursor, item_id: str, item_name: str, score: float = 10.0):
        """
        【重构】将一个项目标记为“已处理”的唯一官方方法。
        它会同时更新数据库和内存缓存，确保数据一致性。
        """
        # 1. 更新数据库
        self.log_db_manager.save_to_processed_log(cursor, item_id, item_name, score=score)
        
        # 2. 实时更新内存缓存
        self.processed_items_cache[item_id] = item_name
        
        logger.debug(f"  ➜ 已将 '{item_name}' 标记为已处理 (数据库 & 内存)。")

    # --- 清除已处理记录 ---
    def clear_processed_log(self):
        """
        【已改造】清除数据库和内存中的已处理记录。
        使用中央数据库连接函数。
        """
        try:
            # 1. ★★★ 调用中央函数 ★★★
            with get_central_db_connection() as conn:
                cursor = conn.cursor()
                
                logger.debug("正在从数据库删除 processed_log 表中的所有记录...")
                cursor.execute("DELETE FROM processed_log")
                # with 语句会自动处理 conn.commit()
            
            logger.info("  ➜ 数据库中的已处理记录已清除。")

            # 2. 清空内存缓存
            self.processed_items_cache.clear()
            logger.info("  ➜ 内存中的已处理记录缓存已清除。")

        except Exception as e:
            logger.error(f"清除数据库或内存已处理记录时失败: {e}", exc_info=True)
            # 3. ★★★ 重新抛出异常，通知上游调用者操作失败 ★★★
            raise
    
    # --- 公开的、独立的追剧判断方法 ---
    def check_and_add_to_watchlist(self, item_details: Dict[str, Any]):
        """
        检查一个媒体项目是否为剧集，如果是，则执行智能追剧判断并添加到待看列表。
        此方法被设计为由外部事件（如Webhook）显式调用。
        """
        item_name_for_log = item_details.get("Name", f"未知项目(ID:{item_details.get('Id')})")
        
        if item_details.get("Type") != "Series":
            # 如果不是剧集，直接返回，不打印非必要的日志
            return

        logger.info(f"  ➜ 开始为新入库剧集 '{item_name_for_log}' 进行追剧状态判断...")
        try:
            # 实例化 WatchlistProcessor 并执行添加操作
            watchlist_proc = WatchlistProcessor(self.config, ai_translator=self.ai_translator)
            watchlist_proc.add_series_to_watchlist(item_details)
        except Exception as e_watchlist:
            logger.error(f"  ➜ 在自动添加 '{item_name_for_log}' 到追剧列表时发生错误: {e_watchlist}", exc_info=True)
    
    # --- 停止信号机制 ---
    def signal_stop(self):
        self._stop_event.set()

    # --- 公开一个方法来重置停止信号，允许在同一实例上重复使用 ---
    def clear_stop_signal(self):
        self._stop_event.clear()

    # --- 公开一个方法来检查是否已请求停止，供长时间运行的函数调用 ---
    def get_stop_event(self) -> threading.Event:
        """返回内部的停止事件对象，以便传递给其他函数。"""
        return self._stop_event

    # --- 公开一个方法来检查是否已请求停止，供长时间运行的函数调用 ---
    def is_stop_requested(self) -> bool:
        return self._stop_event.is_set()

    # --- 加载已处理记录 ---
    def _load_processed_log_from_db(self) -> Dict[str, str]:
        log_dict = {}
        try:
            # 1. ★★★ 使用 with 语句和中央函数 ★★★
            with get_central_db_connection() as conn:
                cursor = conn.cursor()
                
                # 2. 执行查询
                cursor.execute("SELECT item_id, item_name FROM processed_log")
                rows = cursor.fetchall()
                
                # 3. 处理结果
                for row in rows:
                    if row['item_id'] and row['item_name']:
                        log_dict[row['item_id']] = row['item_name']
            
            # 4. with 语句会自动处理所有事情，代码干净利落！

        except Exception as e:
            # 5. ★★★ 记录更详细的异常信息 ★★★
            logger.error(f"从数据库读取已处理记录失败: {e}", exc_info=True)
        return log_dict

    # --- 获取豆瓣数据（演员+评分）---
    def _get_douban_data_with_local_cache(self, media_info: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Optional[float]]:
        """
        获取豆瓣数据（演员+评分）。直接使用在线 API。
        """
        provider_ids = media_info.get("ProviderIds", {})
        item_name = media_info.get("Name", "")
        imdb_id = provider_ids.get("Imdb")
        item_type = media_info.get("Type")
        item_year = str(media_info.get("ProductionYear", ""))

        if not self.config.get(constants.CONFIG_OPTION_DOUBAN_ENABLE_ONLINE_API, True):
            return [], None
        
        logger.info("  ➜ 准备通过豆瓣在线 API 获取演员信息...")

        match_info_result = self.douban_api.match_info(
            name=item_name, imdbid=imdb_id, mtype=item_type, year=item_year
        )

        if match_info_result.get("error") or not match_info_result.get("id"):
            logger.warning(f"  ➜ 在线匹配豆瓣ID失败 for '{item_name}': {match_info_result.get('message', '未找到ID')}")
            return [], None

        douban_id = match_info_result["id"]
        douban_type = match_info_result.get("type")

        if not douban_type:
            return [], None

        cast_data = self.douban_api.get_acting(
            name=item_name, 
            douban_id_override=douban_id, 
            mtype=douban_type
        )
        return cast_data.get("cast", []), None
    
    # --- 通过 API 更新 Emby 中演员名字 ---
    def _update_emby_person_names_from_final_cast(self, final_cast: List[Dict[str, Any]], item_name_for_log: str):
        """
        根据最终处理好的演员列表，通过 API 更新 Emby 中“演员”项目的名字。
        """
        actors_to_update = [
            actor for actor in final_cast 
            if actor.get("emby_person_id") and utils.contains_chinese(actor.get("name"))
        ]

        if not actors_to_update:
            logger.info(f"  ➜ 无需通过 API 更新演员名字 (没有找到需要翻译的 Emby 演员)。")
            return

        logger.info(f"  ➜ 开始为《{item_name_for_log}》的 {len(actors_to_update)} 位演员通过 API 更新名字...")
        
        # 批量获取这些演员在 Emby 中的当前信息，以减少 API 请求
        person_ids = [actor["emby_person_id"] for actor in actors_to_update]
        current_person_details = emby.get_emby_items_by_id(
            base_url=self.emby_url,
            api_key=self.emby_api_key,
            user_id=self.emby_user_id,
            item_ids=person_ids,
            fields="Name"
        )
        
        current_names_map = {p["Id"]: p.get("Name") for p in current_person_details} if current_person_details else {}

        updated_count = 0
        for actor in actors_to_update:
            person_id = actor["emby_person_id"]
            new_name = actor["name"]
            current_name = current_names_map.get(person_id)

            # 只有当新名字和当前名字不同时，才执行更新
            if new_name != current_name:
                emby.update_person_details(
                    person_id=person_id,
                    new_data={"Name": new_name},
                    emby_server_url=self.emby_url,
                    emby_api_key=self.emby_api_key,
                    user_id=self.emby_user_id
                )
                updated_count += 1
                # 加个小延迟避免请求过快
                time.sleep(0.2) 

        logger.info(f"  ➜ 成功通过 API 更新了 {updated_count} 位演员的名字。")
    
    # --- 全量处理的入口 ---
    def process_full_library(self, update_status_callback: Optional[callable] = None, force_full_update: bool = False):
        """
        全量处理的入口。
        """
        self.clear_stop_signal()
        
        logger.trace(f"进入核心执行层: process_full_library, 接收到的 force_full_update = {force_full_update}")

        if force_full_update:
            logger.info("  ➜ 检测到“深度更新”模式，正在清空已处理日志...")
            try:
                self.clear_processed_log()
            except Exception as e:
                logger.error(f"在 process_full_library 中清空日志失败: {e}", exc_info=True)
                if update_status_callback: update_status_callback(-1, "清空日志失败")
                return

        libs_to_process_ids = self.config.get("libraries_to_process", [])
        if not libs_to_process_ids:
            logger.warning("  ➜ 未在配置中指定要处理的媒体库。")
            return

        logger.info("  ➜ 正在尝试从Emby获取媒体项目...")
        all_emby_libraries = emby.get_emby_libraries(self.emby_url, self.emby_api_key, self.emby_user_id) or []
        library_name_map = {lib.get('Id'): lib.get('Name', '未知库名') for lib in all_emby_libraries}
        
        movies = emby.get_emby_library_items(self.emby_url, self.emby_api_key, "Movie", self.emby_user_id, libs_to_process_ids, library_name_map=library_name_map) or []
        series = emby.get_emby_library_items(self.emby_url, self.emby_api_key, "Series", self.emby_user_id, libs_to_process_ids, library_name_map=library_name_map) or []
        
        if movies:
            source_movie_lib_names = sorted(list({library_name_map.get(item.get('_SourceLibraryId')) for item in movies if item.get('_SourceLibraryId')}))
            logger.info(f"  ➜ 从媒体库【{', '.join(source_movie_lib_names)}】获取到 {len(movies)} 个电影项目。")

        if series:
            source_series_lib_names = sorted(list({library_name_map.get(item.get('_SourceLibraryId')) for item in series if item.get('_SourceLibraryId')}))
            logger.info(f"  ➜ 从媒体库【{', '.join(source_series_lib_names)}】获取到 {len(series)} 个电视剧项目。")

        all_items = movies + series
        total = len(all_items)
        
        if total == 0:
            logger.info("  ➜ 在所有选定的库中未找到任何可处理的项目。")
            if update_status_callback: update_status_callback(100, "未找到可处理的项目。")
            return

        # --- 清理已删除的媒体项 ---
        if update_status_callback: update_status_callback(20, "正在检查并清理已删除的媒体项...")
        
        with get_central_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT item_id, item_name FROM processed_log")
            processed_log_entries = cursor.fetchall()
            
            processed_ids_in_db = {entry['item_id'] for entry in processed_log_entries}
            # ★★★ 穿透获取所有多版本 ID，防止被误判为已删除 ★★★
            emby_ids_in_library = set()
            for item in all_items:
                if item.get('Id'):
                    emby_ids_in_library.add(str(item['Id']))
                
                # 如果是电影，且包含多个媒体源，把它们的独立 ID 也加进存活名单
                if item.get('Type') == 'Movie' and item.get('MediaSources'):
                    for source in item['MediaSources']:
                        source_id = str(source.get('Id', '')).replace('mediasource_', '')
                        if source_id:
                            emby_ids_in_library.add(source_id)
            
            # 找出在 processed_log 中但不在 Emby 媒体库中的项目
            deleted_items_to_clean = processed_ids_in_db - emby_ids_in_library
            
            if deleted_items_to_clean:
                logger.info(f"  ➜ 发现 {len(deleted_items_to_clean)} 个已从 Emby 媒体库删除的项目，正在从 '已处理' 中移除...")
                for deleted_item_id in deleted_items_to_clean:
                    self.log_db_manager.remove_from_processed_log(cursor, deleted_item_id)
                    self.log_db_manager.remove_from_failed_log(cursor, deleted_item_id)
                    # 同时从内存缓存中移除
                    if deleted_item_id in self.processed_items_cache:
                        del self.processed_items_cache[deleted_item_id]
                    logger.debug(f"  ➜ 已从 '已处理' 中移除 ItemID: {deleted_item_id}")
                conn.commit()
                logger.info("  ➜ 已删除媒体项的清理工作完成。")
            else:
                logger.info("  ➜ 未发现需要从 '已处理' 中清理的已删除媒体项。")
        
        if update_status_callback: update_status_callback(30, "已删除媒体项清理完成，开始处理现有媒体...")

        # --- 现有媒体项处理循环 ---
        for i, item in enumerate(all_items):
            if self.is_stop_requested():
                logger.warning("  ➜ 全库扫描任务已被用户中止。")
                break # 使用 break 优雅地退出循环
            
            item_id = item.get('Id')
            item_name = item.get('Name', f"ID:{item_id}")

            if not force_full_update and item_id in self.processed_items_cache:
                logger.info(f"  ➜ 正在跳过已处理的项目: {item_name}")
                if update_status_callback:
                    # 调整进度条的起始点，使其在清理后从 30% 开始
                    progress_after_cleanup = 30
                    current_progress = progress_after_cleanup + int(((i + 1) / total) * (100 - progress_after_cleanup))
                    update_status_callback(current_progress, f"跳过: {item_name}")
                continue

            if update_status_callback:
                progress_after_cleanup = 30
                current_progress = progress_after_cleanup + int(((i + 1) / total) * (100 - progress_after_cleanup))
                update_status_callback(current_progress, f"处理中 ({i+1}/{total}): {item_name}")
            
            self.process_single_item(
                item_id, 
                force_full_update=force_full_update
            )
            
            time_module.sleep(float(self.config.get("delay_between_items_sec", 0.5)))
        
        if not self.is_stop_requested() and update_status_callback:
            update_status_callback(100, "全量处理完成")
    
    # --- 核心处理总管 ---
    def process_single_item(self, emby_item_id: str, force_full_update: bool = False, specific_episode_ids: Optional[List[str]] = None):
        """
        入口函数，它会先检查是否需要跳过已处理的项目。
        """
        # 1. 除非强制，否则跳过已处理的
        if not force_full_update and not specific_episode_ids and emby_item_id in self.processed_items_cache:
            item_name_from_cache = self.processed_items_cache.get(emby_item_id, f"ID:{emby_item_id}")
            logger.info(f"媒体 '{item_name_from_cache}' 跳过已处理记录。")
            return True

        # 2. 检查停止信号
        if self.is_stop_requested():
            return False

        # 3. 获取Emby详情，这是后续所有操作的基础
        item_details = emby.get_emby_item_details(
            emby_item_id, self.emby_url, self.emby_api_key, self.emby_user_id
        )
        
        if not item_details:
            logger.error(f"process_single_item: 无法获取 Emby 项目 {emby_item_id} 的详情。")
            return False
        
        # 补全 _SourceLibraryId：因为单项获取接口不包含此字段，需通过路径反查
        if not item_details.get('_SourceLibraryId'):
            lib_info = emby.get_library_root_for_item(
                item_id=emby_item_id,
                base_url=self.emby_url,
                api_key=self.emby_api_key,
                user_id=self.emby_user_id,
                item_path=item_details.get("Path") # ★★★ 核心优化：直接把刚查到的 Path 喂进去
            )
            if lib_info and lib_info.get('Id'):
                item_details['_SourceLibraryId'] = lib_info['Id']
                logger.debug(f"  ➜ 已为 '{item_details.get('Name')}' 补全媒体库ID: {lib_info['Id']}")
            else:
                logger.warning(f"  ➜ 无法确定 '{item_details.get('Name')}' 所属的媒体库ID。")

        # 4. 将任务交给核心处理函数
        return self._process_item_core_logic(
            item_details_from_emby=item_details,
            force_full_update=force_full_update,
            specific_episode_ids=specific_episode_ids
        )

    # --- 辅助函数：丰富合集信息（名称、简介） ---
    def _enrich_collection_info(self, collection_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        辅助函数：获取并翻译电影合集信息 (名称、简介)。
        """
        if not collection_info or not isinstance(collection_info, dict) or not collection_info.get("id"):
            return collection_info

        col_id = collection_info.get("id")
        col_name = collection_info.get("name", "")
        col_overview = ""

        try:
            import requests
            base_url = self.config.get(constants.CONFIG_OPTION_TMDB_API_BASE_URL, 'https://api.themoviedb.org/3')
            
            # 尝试获取中文合集简介
            url_zh = f"{base_url}/collection/{col_id}?api_key={self.tmdb_api_key}&language=zh-CN"
            resp_zh = requests.get(url_zh, timeout=10, proxies=config_manager.get_proxies_for_requests())
            if resp_zh.status_code == 200:
                col_details_zh = resp_zh.json()
                col_overview = col_details_zh.get("overview", "")
                if not col_name: col_name = col_details_zh.get("name", "")
            
            # 兜底获取英文合集简介
            if not col_overview:
                url_en = f"{base_url}/collection/{col_id}?api_key={self.tmdb_api_key}&language=en-US"
                resp_en = requests.get(url_en, timeout=10, proxies=config_manager.get_proxies_for_requests())
                if resp_en.status_code == 200:
                    col_details_en = resp_en.json()
                    col_overview = col_details_en.get("overview", "")
                    if not col_name: col_name = col_details_en.get("name", "")
        except Exception as e:
            logger.warning(f"  ➜ 获取合集详细信息失败: {e}")

        # AI 翻译逻辑
        if self.ai_translator:
            # 翻译标题
            if self.config.get(constants.CONFIG_OPTION_AI_TRANSLATE_TITLE, False) and col_name and not utils.contains_chinese(col_name):
                trans_col_name = self.ai_translator.translate_title(col_name, media_type="Movie")
                if trans_col_name and utils.contains_chinese(trans_col_name):
                    if trans_col_name.endswith("合集"): trans_col_name = trans_col_name[:-2] + "（系列）"
                    collection_info["name"] = trans_col_name
                    col_name = trans_col_name 
            else: 
                collection_info["name"] = col_name

            # 翻译简介
            if self.config.get(constants.CONFIG_OPTION_AI_TRANSLATE_OVERVIEW, False) and col_overview and not utils.contains_chinese(col_overview):
                trans_col_overview = self.ai_translator.translate_overview(col_overview, title=col_name)
                if trans_col_overview: collection_info["overview"] = trans_col_overview
            elif col_overview: 
                collection_info["overview"] = col_overview
        else:
            collection_info["name"] = col_name
            if col_overview: collection_info["overview"] = col_overview

        return collection_info

    # ---核心处理流程 ---
    def _process_item_core_logic(self, item_details_from_emby: Dict[str, Any], force_full_update: bool = False, specific_episode_ids: Optional[List[str]] = None):
        """
        【V3 极简架构版】
        彻底分离“预处理”和“Webhook回流”逻辑。
        - 预处理/强制刷新：执行完整的 TMDb -> AI翻译 -> 演员处理 -> NFO生成。
        - Webhook回流：秒级命中缓存，仅提取 Emby ID 和视频流信息更新数据库，拒绝一切冗余操作。
        """
        item_id = item_details_from_emby.get("Id")
        item_name_for_log = item_details_from_emby.get("Name", f"未知项目(ID:{item_id})")
        tmdb_id = item_details_from_emby.get("ProviderIds", {}).get("Tmdb")
        item_type = item_details_from_emby.get("Type")

        logger.trace(f"--- 开始处理 '{item_name_for_log}' (TMDb ID: {tmdb_id}) ---")

        all_emby_people_for_count = item_details_from_emby.get("People", [])
        original_emby_actor_count = len([p for p in all_emby_people_for_count if p.get("Type") == "Actor"])

        if not is_valid_tmdb_id(tmdb_id):
            logger.error(f"  ➜ '{item_name_for_log}' 缺少有效的 TMDb ID (当前值: {tmdb_id})，跳过处理。")
            return False

        try:
            # ======================================================================
            # ★★★ 核心重构：极速回流模式拦截 (Webhook Feedback) ★★★
            # ======================================================================
            is_webhook_feedback = False
            formatted_metadata = None
            final_processed_cast = None

            # 只要不是强制刷新，就尝试从数据库捞取预处理时存入的完整元数据
            if not force_full_update:
                payload, cast = self._reconstruct_full_data_from_db(tmdb_id, item_type)
                if payload and cast:
                    formatted_metadata = payload
                    final_processed_cast = cast
                    is_webhook_feedback = True
                    logger.info(f"  ➜ [webhook回流] 跳过 TMDb/AI/演员处理/NFO生成！")

            # ======================================================================
            # 传统重型处理流程 (手动入库 / 强制刷新 / 预处理遗漏)
            # ======================================================================
            if not is_webhook_feedback:
                logger.info(f"  ➜ [完整模式] 未命中缓存或强制重处理，开始执行核心刮削流程...")

                # 1. 获取 TMDb 数据
                fresh_data = None
                aggregated_tmdb_data = None
                if self.tmdb_api_key:
                    try:
                        if item_type == "Movie":
                            fresh_data = tmdb.get_movie_details(tmdb_id, self.tmdb_api_key)
                        elif item_type == "Series":
                            aggregated_tmdb_data = tmdb.aggregate_full_series_data_from_tmdb(int(tmdb_id), self.tmdb_api_key)
                            if aggregated_tmdb_data:
                                fresh_data = aggregated_tmdb_data.get("series_details")
                    except Exception as e:
                        logger.warning(f"  ➜ 从 TMDb API 获取数据失败: {e}")

                if not fresh_data:
                    logger.error("  ➜ 无法获取 TMDb 详情，中止处理。")
                    return False

                # 提取 TMDb 官方中文别名 & 卖片哥广告拦截
                raw_title = fresh_data.get("title") if item_type == "Movie" else fresh_data.get("name")
                current_title = utils.clean_invisible_chars(raw_title)
                
                if utils.is_spam_title(current_title):
                    logger.warning(f"  ➜ [拦截] 检测到恶意广告片名: '{current_title}'，准备寻找替代片名...")
                    current_title = ""
                
                if not current_title or not utils.contains_chinese(current_title):
                    chinese_alias = None
                    alt_titles_data = fresh_data.get("alternative_titles", {})
                    alt_list = alt_titles_data.get("titles") or alt_titles_data.get("results") or []
                    priority_map = {"CN": 1, "SG": 2, "TW": 3, "HK": 4}
                    best_priority = 99
                    for alt in alt_list:
                        alt_title = utils.clean_invisible_chars(alt.get("title", ""))
                        # 别名也必须经过广告过滤
                        if utils.contains_chinese(alt_title) and not utils.is_spam_title(alt_title):
                            iso_country = alt.get("iso_3166_1", "").upper()
                            current_priority = priority_map.get(iso_country, 5)
                            if current_priority < best_priority:
                                chinese_alias = alt_title
                                best_priority = current_priority
                            if best_priority == 1: break
                    
                    if chinese_alias:
                        logger.info(f"  ➜ 发现干净的 TMDb 官方中文别名: '{chinese_alias}'")
                        if item_type == "Movie": fresh_data["title"] = chinese_alias
                        else:
                            fresh_data["name"] = chinese_alias
                            if aggregated_tmdb_data and "series_details" in aggregated_tmdb_data:
                                aggregated_tmdb_data["series_details"]["name"] = chinese_alias
                    else:
                        # 回退到原名，交给 AI 翻译
                        original_title = fresh_data.get("original_title") if item_type == "Movie" else fresh_data.get("original_name")
                        logger.info(f"  ➜ 未找到干净的中文别名，回退到原名: '{original_title}'，等待 AI 翻译。")
                        if item_type == "Movie": fresh_data["title"] = original_title
                        else:
                            fresh_data["name"] = original_title
                            if aggregated_tmdb_data and "series_details" in aggregated_tmdb_data:
                                aggregated_tmdb_data["series_details"]["name"] = original_title

                # 2. 填充骨架
                formatted_metadata = construct_metadata_payload(
                    item_type=item_type,
                    tmdb_data=fresh_data or {},
                    aggregated_tmdb_data=aggregated_tmdb_data,
                    emby_data_fallback=item_details_from_emby
                )

                if not item_details_from_emby.get("Genres") and fresh_data.get("genres"):
                    item_details_from_emby["Genres"] = fresh_data.get("genres")

                # 提取演员源数据
                authoritative_cast_source = []
                if item_type == "Movie":
                    credits_source = fresh_data.get('credits') or fresh_data.get('casts') or {}
                    authoritative_cast_source = credits_source.get('cast', [])
                elif item_type == "Series":
                    all_episodes = list(aggregated_tmdb_data.get("episodes_details", {}).values()) if aggregated_tmdb_data else []
                    authoritative_cast_source = _aggregate_series_cast_from_tmdb_data(fresh_data, all_episodes)

                if self.config.get(constants.CONFIG_OPTION_REMOVE_ACTORS_WITHOUT_AVATARS, True) and authoritative_cast_source:
                    authoritative_cast_source = [actor for actor in authoritative_cast_source if actor.get("profile_path")]

                # 3. AI 翻译 (大一统引擎：标题、简介、标语)
                if self.ai_translator:
                    from tasks.helpers import translate_tmdb_metadata_recursively
                    translate_tmdb_metadata_recursively(
                        item_type=item_type,
                        tmdb_data=formatted_metadata, # 传入已构建好的骨架
                        ai_translator=self.ai_translator,
                        item_name=item_name_for_log,
                        tmdb_api_key=self.tmdb_api_key,
                        config=self.config
                    )
                    
                    # 合集翻译 (电影专属)
                    if item_type == "Movie" and self.config.get(constants.CONFIG_OPTION_GENERATE_COLLECTION_NFO, False):
                        collection_info = formatted_metadata.get("belongs_to_collection")
                        if collection_info and isinstance(collection_info, dict) and collection_info.get("id"):
                            formatted_metadata["belongs_to_collection"] = self._enrich_collection_info(collection_info)

                # 4. 演员表处理
                with get_central_db_connection() as conn:
                    cursor = conn.cursor()
                    all_emby_people = item_details_from_emby.get("People", [])
                    current_emby_cast_raw = [p for p in all_emby_people if p.get("Type") == "Actor"]
                    emby_config = {"url": self.emby_url, "api_key": self.emby_api_key, "user_id": self.emby_user_id}
                    enriched_emby_cast = self.actor_db_manager.enrich_actors_with_provider_ids(cursor, current_emby_cast_raw, emby_config)
                    douban_cast_raw, _ = self._get_douban_data_with_local_cache(item_details_from_emby)

                    final_processed_cast = self._process_cast_list(
                        tmdb_cast_people=authoritative_cast_source,
                        emby_cast_people=enriched_emby_cast,
                        douban_cast_list=douban_cast_raw,
                        item_details_from_emby=item_details_from_emby,
                        cursor=cursor,
                        tmdb_api_key=self.tmdb_api_key,
                        stop_event=self.get_stop_event()
                    )
                    
                    # ★★★ 新增：将核心导演也写入 person_metadata 单表 ★★★
                    try:
                        from tasks.helpers import extract_top_directors
                        top_directors = extract_top_directors(fresh_data, max_count=3)
                        for director in top_directors:
                            if director.get('id'):
                                director_data = {
                                    "id": director.get("id"),
                                    "name": director.get("name"),
                                    "profile_path": director.get("profile_path"),
                                    # Emby ID 留空，后续的 actor_sync 任务会自动通过 Emby 扫描补全绑定
                                }
                                self.actor_db_manager.upsert_person(cursor, director_data, emby_config)
                        logger.debug(f"  ➜ 成功将 {len(top_directors)} 位导演信息同步至人员元数据库。")
                    except Exception as e_dir:
                        logger.warning(f"  ➜ 同步导演信息至数据库时失败: {e_dir}")

                if final_processed_cast is None: raise ValueError("未能生成有效的最终演员列表。")

                # ======================================================================
                # ★★★ 老六专属：无简介笑话占位功能 (入库预处理) ★★★
                # ======================================================================
                if self.config.get("ai_joke_fallback", False) and self.ai_translator:
                    jokes_to_generate = {}
                    
                    # 1. 检查主干 (电影/剧集主简介)
                    if not formatted_metadata.get("overview"):
                        jokes_to_generate["main"] = formatted_metadata.get("title") or formatted_metadata.get("name")
                        
                    # 2. 检查分集
                    ep_list = []
                    if item_type == "Series" and aggregated_tmdb_data and "episodes_details" in aggregated_tmdb_data:
                        episodes = aggregated_tmdb_data["episodes_details"]
                        ep_list = episodes.values() if isinstance(episodes, dict) else (episodes if isinstance(episodes, list) else [])
                        
                        # 尝试从数据库读取旧数据，继承已有笑话，省 Token！
                        old_payload, _ = self._reconstruct_full_data_from_db(tmdb_id, item_type)
                        old_episodes = {}
                        if old_payload and "episodes_details" in old_payload:
                            old_eps = old_payload["episodes_details"]
                            old_episodes = old_eps if isinstance(old_eps, dict) else {f"S{e.get('season_number')}E{e.get('episode_number')}": e for e in old_eps}

                        for ep in ep_list:
                            if not ep.get("overview"):
                                ep_key = f"S{ep.get('season_number')}E{ep.get('episode_number')}"
                                old_overview = old_episodes.get(ep_key, {}).get("overview") or ""
                                if "【老六占位简介】" in old_overview:
                                    ep["overview"] = old_overview # 继承老笑话，不花冤枉钱
                                else:
                                    jokes_to_generate[ep_key] = f"{formatted_metadata.get('name')} {ep_key}"

                    # 3. 批量生成并回填
                    if jokes_to_generate:
                        logger.info(f"  ➜ [老六模式] 发现 {len(jokes_to_generate)} 处缺失简介，正在呼叫 AI 编段子...")
                        generated_jokes = self.ai_translator.batch_generate_jokes(jokes_to_generate)
                        
                        if "main" in generated_jokes:
                            formatted_metadata["overview"] = generated_jokes["main"]
                            if aggregated_tmdb_data and "series_details" in aggregated_tmdb_data:
                                aggregated_tmdb_data["series_details"]["overview"] = generated_jokes["main"]
                        
                        for ep in ep_list:
                            ep_key = f"S{ep.get('season_number')}E{ep.get('episode_number')}"
                            if ep_key in generated_jokes:
                                ep["overview"] = generated_jokes[ep_key]

                # ★★★ 首次入库时，将提取到的导演强行塞入 formatted_metadata 供 NFO 使用 ★★★
                if item_type == "Series":
                    from tasks.helpers import extract_top_directors
                    top_directors = extract_top_directors(fresh_data, max_count=3)
                    
                    if 'credits' not in formatted_metadata:
                        formatted_metadata['credits'] = {'crew': []}
                    elif 'crew' not in formatted_metadata['credits']:
                        formatted_metadata['credits']['crew'] = []
                        
                    existing_crew_ids = {c.get('id') for c in formatted_metadata['credits']['crew'] if c.get('job') in ['Director', 'Series Director']}
                    for d in top_directors:
                        if d['id'] not in existing_crew_ids:
                            formatted_metadata['credits']['crew'].append(d)

                # 5. 生成 NFO 和 图片
                logger.info(f"  ➜ 正在生成(NFO文件 & 图片)...")
                self.sync_item_metadata(
                    item_details=item_details_from_emby,
                    tmdb_id=tmdb_id,
                    final_cast_override=final_processed_cast,
                    episode_ids_to_sync=specific_episode_ids,
                    metadata_override=formatted_metadata
                )
                self.download_images_from_tmdb(
                    tmdb_id=tmdb_id,
                    item_type=item_type,
                    aggregated_tmdb_data=formatted_metadata, 
                    item_details=item_details_from_emby
                )

                # 6. 更新 Emby 演员名并通知刷新
                self._update_emby_person_names_from_final_cast(final_processed_cast, item_name_for_log)
                logger.info(f"  ➜ 处理完成，正在通知 Emby 刷新...")
                emby.refresh_emby_item_metadata(
                    item_emby_id=item_id,
                    emby_server_url=self.emby_url,
                    emby_api_key=self.emby_api_key,
                    user_id_for_ops=self.emby_user_id,
                    replace_all_metadata_param=True, 
                    item_name_for_log=item_name_for_log
                )
            else:
                logger.debug(f"  ➜ [webhook回流] 开始质检...")

            # ======================================================================
            # 统一收尾流程 (更新数据库、质检、合集、通知)
            # ======================================================================
            with get_central_db_connection() as conn:
                cursor = conn.cursor()

                # 1. 更新数据库缓存 (绑定 Emby ID, 提取视频流资产)
                self._upsert_media_metadata(
                    cursor=cursor,
                    item_type=item_type,
                    item_details_from_emby=item_details_from_emby,
                    final_processed_cast=final_processed_cast,
                    source_data_package=formatted_metadata,
                    specific_episode_ids=specific_episode_ids
                )
                
                # 2. 综合质检 (视频流检查 + 演员匹配度评分)
                stream_check_passed = True
                stream_fail_reason = ""
                
                def _check_stream_validity(file_path, label_prefix="", emby_item=None, db_assets=None):
                    if not file_path: return False, f"{label_prefix} 缺失文件路径"
                    
                    # 1. 检查物理文件
                    mediainfo_path = os.path.splitext(file_path)[0] + "-mediainfo.json"
                    if os.path.exists(mediainfo_path): return True, ""
                    
                    # 2. 检查 Emby 的 MediaSources (针对 Movie/Episode)
                    if emby_item and emby_item.get("MediaSources"):
                        for source in emby_item["MediaSources"]:
                            for stream in source.get("MediaStreams", []):
                                if stream.get("Type") == "Video" and stream.get("Width") and stream.get("Height"):
                                    return True, ""
                                    
                    # 3. 检查数据库的 asset_details_json (针对 Series 中的 Episode)
                    if db_assets and isinstance(db_assets, list):
                        for asset in db_assets:
                            for stream in asset.get("video_streams", []):
                                if stream.get("width") and stream.get("height"):
                                    return True, ""
                                    
                    return False, f"{label_prefix}缺失媒体信息: 文件 (-mediainfo.json) 且无有效视频流数据"

                if item_type in ['Movie', 'Episode']:
                    emby_path = item_details_from_emby.get("Path")
                    passed, reason = _check_stream_validity(emby_path, "", emby_item=item_details_from_emby)
                    if not passed:
                        stream_check_passed = False
                        stream_fail_reason = reason
                elif item_type == 'Series':
                    try:
                        cursor.execute("""
                            SELECT season_number, episode_number, asset_details_json 
                            FROM media_metadata 
                            WHERE parent_series_tmdb_id = %s AND item_type = 'Episode' AND in_library = TRUE
                            ORDER BY season_number ASC, episode_number ASC
                        """, (tmdb_id,))
                        for db_ep in cursor.fetchall():
                            s_idx, e_idx = db_ep['season_number'], db_ep['episode_number']
                            raw_assets = db_ep['asset_details_json']
                            assets = json.loads(raw_assets) if isinstance(raw_assets, str) else (raw_assets if isinstance(raw_assets, list) else [])
                            ep_path = assets[0].get('path') if assets and len(assets) > 0 else None
                            passed, reason = _check_stream_validity(ep_path, f"[S{s_idx}E{e_idx}]", db_assets=assets)
                            if not passed:
                                stream_check_passed = False
                                stream_fail_reason = reason
                                logger.warning(f"  ➜ [质检] 剧集《{item_name_for_log}》检测到坏分集: {reason}")
                                break 
                    except Exception as e_db_check:
                        logger.warning(f"  ➜ [质检] 数据库验证分集流信息时出错: {e_db_check}")

                raw_genres = item_details_from_emby.get("Genres", [])
                genres = [g.get('name') for g in raw_genres if g.get('name')] if raw_genres and isinstance(raw_genres[0], dict) else raw_genres
                is_animation = "Animation" in genres or "动画" in genres or "Documentary" in genres or "纪录" in genres or "记录" in genres
                
                processing_score = actor_utils.evaluate_cast_processing_quality(
                    final_cast=final_processed_cast, 
                    original_cast_count=original_emby_actor_count,
                    expected_final_count=len(final_processed_cast), 
                    is_animation=is_animation
                )

                if is_webhook_feedback:
                    logger.info(f"  ➜ [webhook回流] 基于缓存数据的实时复核评分: {processing_score:.2f}")
                
                raw_min_score = self.config.get("min_score_for_review", constants.DEFAULT_MIN_SCORE_FOR_REVIEW)
                min_score_for_review = float(raw_min_score)
                
                target_log_id = item_id
                target_log_name = item_name_for_log
                target_log_type = item_type

                if item_type == 'Episode':
                    series_id = item_details_from_emby.get('SeriesId')
                    if series_id:
                        target_log_id = str(series_id)
                        target_log_name = item_details_from_emby.get('SeriesName') or f"剧集(ID:{series_id})"
                        target_log_type = 'Series'
                        if not stream_check_passed:
                            s_idx, e_idx = item_details_from_emby.get('ParentIndexNumber'), item_details_from_emby.get('IndexNumber')
                            stream_fail_reason = f"[S{s_idx}E{e_idx}] {stream_fail_reason}"

                # 3. 最终判定与日志写入
                if not stream_check_passed:
                    logger.warning(f"  ➜ [质检]《{item_name_for_log}》因缺失视频流数据，需重新处理。")
                    self.log_db_manager.save_to_failed_log(cursor, target_log_id, target_log_name, stream_fail_reason, target_log_type, score=0.0)
                    self._mark_item_as_processed(cursor, target_log_id, target_log_name, score=0.0)
                elif processing_score < min_score_for_review:
                    reason = f"处理评分 ({processing_score:.2f}) 低于阈值 ({min_score_for_review})。"
                    if is_webhook_feedback: logger.warning(f"  ➜ [质检]《{item_name_for_log}》本地缓存数据质量不佳 (评分: {processing_score:.2f})，已重新标记为【待复核】。")
                    else: logger.warning(f"  ➜ [质检]《{item_name_for_log}》处理质量不佳，已标记为【待复核】。原因: {reason}")
                    self.log_db_manager.save_to_failed_log(cursor, target_log_id, target_log_name, reason, target_log_type, score=processing_score)
                    self._mark_item_as_processed(cursor, target_log_id, target_log_name, score=processing_score)
                else:
                    logger.info(f"  ➜ 《{item_name_for_log}》质检通过 (评分: {processing_score:.2f})，标记为已处理。")
                    self._mark_item_as_processed(cursor, target_log_id, target_log_name, score=processing_score)
                    self.log_db_manager.remove_from_failed_log(cursor, target_log_id)
                
                conn.commit()

            # 4. 刷新向量库
            is_pure_episode_update = (item_type == 'Series' and specific_episode_ids)
            if item_type in ['Movie', 'Series'] and not is_pure_episode_update and self.config.get(constants.CONFIG_OPTION_PROXY_ENABLED) and self.config.get(constants.CONFIG_OPTION_AI_VECTOR):
                try:
                    threading.Thread(target=RecommendationEngine.refresh_cache).start()
                    logger.debug(f"  ➜ [智能推荐] 已触发向量缓存刷新，'{item_name_for_log}' 将即刻加入推荐池。")
                except Exception as e:
                    logger.warning(f"  ➜ [智能推荐] 触发缓存刷新失败: {e}")

            logger.trace(f"--- 处理完成 '{item_name_for_log}' ---")
            return True

        except (ValueError, InterruptedError) as e:
            logger.warning(f"处理 '{item_name_for_log}' 的过程中断: {e}")
            return False
        except Exception as outer_e:
            logger.error(f"核心处理流程中发生未知严重错误 for '{item_name_for_log}': {outer_e}", exc_info=True)
            try:
                with get_central_db_connection() as conn_fail:
                    self.log_db_manager.save_to_failed_log(conn_fail.cursor(), item_id, item_name_for_log, f"核心处理异常: {str(outer_e)}", item_type)
            except Exception as log_e:
                logger.error(f"写入待复核日志时再次发生错误: {log_e}")
            return False

    # --- 核心处理器 ---
    def _process_cast_list(self, tmdb_cast_people: List[Dict[str, Any]],
                                    emby_cast_people: List[Dict[str, Any]],
                                    douban_cast_list: List[Dict[str, Any]],
                                    item_details_from_emby: Dict[str, Any],
                                    cursor: psycopg2.extensions.cursor,
                                    tmdb_api_key: Optional[str],
                                    stop_event: Optional[threading.Event]) -> List[Dict[str, Any]]:
        """
        【V-Final with Truncation - Full Code】
        - 在步骤4的开头，重新加入了对最终演员列表进行截断的逻辑。
        - 确保在进行AI翻译等耗时操作前，将演员数量限制在配置的上限内。
        """
        # --- 在所有处理开始前，从源头清洗同名异人演员 ---
        logger.debug("  ➜ 预处理：清洗源数据中的同名演员，只保留order最小的一个。")
        cleaned_tmdb_cast = []
        seen_names = {} # 使用字典来存储见过的名字及其order
        
        # 首先按 order 排序，确保第一个遇到的是 order 最小的
        tmdb_cast_people.sort(key=lambda x: x.get('order', 999))

        for actor in tmdb_cast_people:
            name = actor.get("name")
            if not name or not isinstance(name, str):
                continue
            
            cleaned_name = name.strip()
            
            if cleaned_name not in seen_names:
                cleaned_tmdb_cast.append(actor)
                seen_names[cleaned_name] = actor.get('order', 999)
            else:
                # 记录被丢弃的演员
                role = actor.get("character", "未知角色")
                logger.info(f"  ➜ 为避免张冠李戴，删除同名异人演员: '{cleaned_name}' (角色: {role}, order: {actor.get('order', 999)})")

        # 使用清洗后的列表进行后续所有操作
        tmdb_cast_people = cleaned_tmdb_cast

        # ★★★ 在流程开始时，记录下来自TMDb的原始演员ID ★★★
        original_tmdb_ids = {str(actor.get("id")) for actor in tmdb_cast_people if actor.get("id")}
        # ======================================================================
        # 步骤 1: ★★★ 数据适配 ★★★
        # ======================================================================
        logger.debug("  ➜ 开始演员数据适配 (反查缓存模式)...")
        
        tmdb_actor_map_by_id = {str(actor.get("id")): actor for actor in tmdb_cast_people}
        tmdb_actor_map_by_en_name = {str(actor.get("name") or "").lower().strip(): actor for actor in tmdb_cast_people}

        final_cast_list = []
        used_tmdb_ids = set()

        for emby_actor in emby_cast_people:
            emby_person_id = emby_actor.get("Id")
            emby_tmdb_id = emby_actor.get("ProviderIds", {}).get("Tmdb")
            emby_name_lower = str(emby_actor.get("Name") or "").lower().strip()

            tmdb_match = None

            if emby_tmdb_id and str(emby_tmdb_id) in tmdb_actor_map_by_id:
                tmdb_match = tmdb_actor_map_by_id[str(emby_tmdb_id)]
            else:
                if emby_name_lower in tmdb_actor_map_by_en_name:
                    tmdb_match = tmdb_actor_map_by_en_name[emby_name_lower]
                else:
                    cache_entry = self.actor_db_manager.get_translation_from_db(cursor, emby_actor.get("Name"), by_translated_text=True)
                    if cache_entry and cache_entry.get('original_text'):
                        original_en_name = str(cache_entry['original_text']).lower().strip()
                        if original_en_name in tmdb_actor_map_by_en_name:
                            tmdb_match = tmdb_actor_map_by_en_name[original_en_name]

            if tmdb_match:
                tmdb_id_str = str(tmdb_match.get("id"))
                merged_actor = tmdb_match.copy()
                merged_actor["emby_person_id"] = emby_person_id
                if utils.contains_chinese(emby_actor.get("Name")):
                    merged_actor["name"] = emby_actor.get("Name")
                else:
                    merged_actor["name"] = tmdb_match.get("name")
                merged_actor["character"] = emby_actor.get("Role")
                final_cast_list.append(merged_actor)
                used_tmdb_ids.add(tmdb_id_str)

        for tmdb_id, tmdb_actor_data in tmdb_actor_map_by_id.items():
            if tmdb_id not in used_tmdb_ids:
                new_actor = tmdb_actor_data.copy()
                new_actor["emby_person_id"] = None
                final_cast_list.append(new_actor)

        logger.debug(f"  ➜ 数据适配完成，生成了 {len(final_cast_list)} 条基准演员数据。")
        
        # ======================================================================
        # 步骤 2: ★★★ “一对一匹配”逻辑 ★★★
        # ======================================================================
        douban_candidates = actor_utils.format_douban_cast(douban_cast_list)
        unmatched_local_actors = list(final_cast_list)
        merged_actors = []
        unmatched_douban_actors = []
        logger.info(f"  ➜ 匹配阶段 1: 对号入座")
        for d_actor in douban_candidates:
            douban_name_zh = d_actor.get("Name", "").lower().strip()
            douban_name_en = d_actor.get("OriginalName", "").lower().strip()
            match_found_for_this_douban_actor = False
            for i, l_actor in enumerate(unmatched_local_actors):
                local_name = str(l_actor.get("name") or "").lower().strip()
                local_original_name = str(l_actor.get("original_name") or "").lower().strip()
                is_match = False
                if douban_name_zh and (douban_name_zh == local_name or douban_name_zh == local_original_name):
                    is_match = True
                elif douban_name_en and (douban_name_en == local_name or douban_name_en == local_original_name):
                    is_match = True
                if is_match:
                    l_actor["name"] = d_actor.get("Name")
                    cleaned_douban_character = utils.clean_character_name_static(d_actor.get("Role"))
                    l_actor["character"] = actor_utils.select_best_role(l_actor.get("character"), cleaned_douban_character)
                    
                    douban_id_to_add = d_actor.get("DoubanCelebrityId")
                    if douban_id_to_add:
                        l_actor["douban_id"] = douban_id_to_add
                    
                    merged_actors.append(unmatched_local_actors.pop(i))
                    match_found_for_this_douban_actor = True
                    break
            if not match_found_for_this_douban_actor:
                unmatched_douban_actors.append(d_actor)

        current_cast_list = merged_actors + unmatched_local_actors
        final_cast_map = {str(actor['id']): actor for actor in current_cast_list if actor.get('id') and str(actor.get('id')) != 'None'}

        # ======================================================================
        # 步骤 3: ★★★ 处理豆瓣补充演员（带丢弃逻辑 和 数量上限逻辑） ★★★
        # ======================================================================
        if not unmatched_douban_actors:
            logger.info("  ➜ 豆瓣API未返回演员或所有演员已匹配，跳过补充演员流程。")
        else:
            logger.info(f"  ➜ 发现 {len(unmatched_douban_actors)} 位潜在的豆瓣补充演员，开始执行匹配与筛选...")
            
            limit = self.config.get(constants.CONFIG_OPTION_MAX_ACTORS_TO_PROCESS, 30)
            try:
                limit = int(limit)
                if limit <= 0: limit = 30
            except (ValueError, TypeError):
                limit = 30

            current_actor_count = len(final_cast_map)
            if current_actor_count >= limit:
                logger.info(f"  ➜ 当前演员数 ({current_actor_count}) 已达上限 ({limit})，将跳过所有豆瓣补充演员的流程。")
                still_unmatched_final = unmatched_douban_actors
            else:
                logger.info(f"  ➜ 当前演员数 ({current_actor_count}) 低于上限 ({limit})，进入补充模式。")
                
                logger.info(f"  ➜ 匹配阶段 2: 用豆瓣ID查'演员映射表' ({len(unmatched_douban_actors)} 位演员)")
                still_unmatched = []
                for d_actor in unmatched_douban_actors:
                    if self.is_stop_requested(): raise InterruptedError("任务中止")
                    d_douban_id = d_actor.get("DoubanCelebrityId")
                    match_found = False
                    if d_douban_id:
                        entry = self.actor_db_manager.find_person_by_any_id(cursor, douban_id=d_douban_id)
                        if entry and entry.get("tmdb_person_id") and entry.get("emby_person_id"):
                            tmdb_id_from_map = str(entry.get("tmdb_person_id"))
                            if tmdb_id_from_map not in final_cast_map:
                                logger.info(f"    ├─ 匹配成功 (通过 豆瓣ID映射): 豆瓣演员 '{d_actor.get('Name')}' -> 加入最终演员表")
                                cached_metadata_map = self.actor_db_manager.get_full_actor_details_by_tmdb_ids(cursor, [int(tmdb_id_from_map)])
                                cached_metadata = cached_metadata_map.get(int(tmdb_id_from_map), {})
                                new_actor_entry = {
                                    "id": tmdb_id_from_map, "name": d_actor.get("Name"),
                                    "original_name": cached_metadata.get("original_name") or d_actor.get("OriginalName"),
                                    "character": d_actor.get("Role"), "order": 999,
                                    "imdb_id": entry.get("imdb_id"), "douban_id": d_douban_id,
                                    "emby_person_id": entry.get("emby_person_id")
                                }
                                final_cast_map[tmdb_id_from_map] = new_actor_entry
                            match_found = True
                    if not match_found:
                        still_unmatched.append(d_actor)
                unmatched_douban_actors = still_unmatched

                if self.config.get(constants.CONFIG_OPTION_DOUBAN_ENABLE_ONLINE_API, False):
                    logger.info(f"  ➜ 匹配阶段 3: 用IMDb ID进行最终匹配和新增 ({len(unmatched_douban_actors)} 位演员)")
                    still_unmatched_final = []
                    for i, d_actor in enumerate(unmatched_douban_actors):
                        if self.is_stop_requested(): raise InterruptedError("任务中止")
                        
                        if len(final_cast_map) >= limit:
                            logger.info(f"  ➜ 演员数已达上限 ({limit})，跳过剩余 {len(unmatched_douban_actors) - i} 位演员的API查询。")
                            still_unmatched_final.extend(unmatched_douban_actors[i:])
                            break

                        d_douban_id = d_actor.get("DoubanCelebrityId")
                        match_found = False
                        if d_douban_id and self.douban_api and self.tmdb_api_key:
                            if self.is_stop_requested(): raise InterruptedError("任务中止")
                            details = self.douban_api.celebrity_details(d_douban_id)
                            time_module.sleep(0.3)
                            d_imdb_id = None
                            if details and not details.get("error"):
                                try:
                                    info_list = details.get("extra", {}).get("info", [])
                                    if isinstance(info_list, list):
                                        for item in info_list:
                                            if isinstance(item, list) and len(item) == 2 and item[0] == 'IMDb编号':
                                                d_imdb_id = item[1]
                                                break
                                except Exception as e_parse:
                                    logger.warning(f"  ➜ 解析 IMDb ID 时发生意外错误: {e_parse}")
                            
                            if d_imdb_id:
                                logger.debug(f"  ➜ 为 '{d_actor.get('Name')}' 获取到 IMDb ID: {d_imdb_id}，开始匹配...")
                                
                                entry_from_map = self.actor_db_manager.find_person_by_any_id(cursor, imdb_id=d_imdb_id)
                                if entry_from_map and entry_from_map.get("tmdb_person_id") and entry_from_map.get("emby_person_id"):
                                    tmdb_id_from_map = str(entry_from_map.get("tmdb_person_id"))
                                    if tmdb_id_from_map not in final_cast_map:
                                        logger.debug(f"    ├─ 匹配成功 (通过 IMDb映射): 豆瓣演员 '{d_actor.get('Name')}' -> 加入最终演员表")
                                        new_actor_entry = {
                                            "id": tmdb_id_from_map, "name": d_actor.get("Name"),
                                            "character": d_actor.get("Role"), "order": 999, "imdb_id": d_imdb_id,
                                            "douban_id": d_douban_id, "emby_person_id": entry_from_map.get("emby_person_id")
                                        }
                                        final_cast_map[tmdb_id_from_map] = new_actor_entry
                                    match_found = True
                                
                                if not match_found:
                                    logger.debug(f"  ➜ 数据库未找到 {d_imdb_id} 的映射，开始通过 TMDb API 反查...")
                                    if self.is_stop_requested(): raise InterruptedError("任务中止")
                                    person_from_tmdb = tmdb.find_person_by_external_id(
                                        external_id=d_imdb_id, api_key=self.tmdb_api_key, source="imdb_id"
                                    )
                                    if person_from_tmdb and person_from_tmdb.get("id"):
                                        tmdb_id_from_find = str(person_from_tmdb.get("id"))
                                        
                                        d_actor['tmdb_id_from_api'] = tmdb_id_from_find
                                        d_actor['imdb_id_from_api'] = d_imdb_id

                                        final_check_row = self.actor_db_manager.find_person_by_any_id(cursor, tmdb_id=tmdb_id_from_find)
                                        if final_check_row and dict(final_check_row).get("emby_person_id"):
                                            emby_pid_from_final_check = dict(final_check_row).get("emby_person_id")
                                            if tmdb_id_from_find not in final_cast_map:
                                                logger.info(f"    ├─ 匹配成功 (通过 TMDb反查): 豆瓣演员 '{d_actor.get('Name')}' -> 加入最终演员表")
                                                new_actor_entry = {
                                                    "id": tmdb_id_from_find, "name": d_actor.get("Name"),
                                                    "character": d_actor.get("Role"), "order": 999,
                                                    "imdb_id": d_imdb_id, "douban_id": d_douban_id,
                                                    "emby_person_id": emby_pid_from_final_check
                                                }
                                                final_cast_map[tmdb_id_from_find] = new_actor_entry
                                            match_found = True
                        
                        if not match_found:
                            still_unmatched_final.append(d_actor)

                    # --- 处理新增 ---
                    if still_unmatched_final:
                        logger.info(f"  ➜ 检查 {len(still_unmatched_final)} 位未匹配演员，尝试合并或加入最终列表...")
                        added_count = 0
                        merged_count = 0
                        
                        for d_actor in still_unmatched_final:
                            tmdb_id_to_process = d_actor.get('tmdb_id_from_api')
                            if tmdb_id_to_process:
                                # 情况一：演员已存在，执行合并/更新
                                if tmdb_id_to_process in final_cast_map:
                                    existing_actor = final_cast_map[tmdb_id_to_process]
                                    original_name = existing_actor.get("name")
                                    new_name = d_actor.get("Name")
                                    
                                    # 仅当豆瓣提供了更优的名字（如中文名）时才更新
                                    if new_name and new_name != original_name and utils.contains_chinese(new_name):
                                        existing_actor["name"] = new_name
                                        logger.debug(f"    ➜ [合并] 已将演员 (TMDb ID: {tmdb_id_to_process}) 的名字从 '{original_name}' 更新为 '{new_name}'")
                                        merged_count += 1
                                
                                # 情况二：演员不存在，执行新增
                                else:
                                    new_actor_entry = {
                                        "id": tmdb_id_to_process,
                                        "name": d_actor.get("Name"),
                                        "character": d_actor.get("Role"),
                                        "order": 999,
                                        "imdb_id": d_actor.get("imdb_id_from_api"),
                                        "douban_id": d_actor.get("DoubanCelebrityId"),
                                        "emby_person_id": None
                                    }
                                    final_cast_map[tmdb_id_to_process] = new_actor_entry
                                    added_count += 1
                        
                        if merged_count > 0:
                            logger.info(f"  ➜ 成功合并了 {merged_count} 位现有演员的豆瓣信息。")
                        if added_count > 0:
                            logger.info(f"  ➜ 成功新增了 {added_count} 位演员到最终列表。")
        
        # ======================================================================
        # 步骤 4: ★★★ 从TMDb补全头像 ★★★
        # ======================================================================
        current_cast_list = list(final_cast_map.values())
        
        # ★★★ 核心修改 2/3: 筛选需要补全的演员时，排除掉原始TMDb列表中的演员 ★★★
        actors_to_supplement = [
            actor for actor in current_cast_list 
            if str(actor.get("id")) not in original_tmdb_ids and actor.get("id")
        ]
        
        if actors_to_supplement:
            total_to_supplement = len(actors_to_supplement)
            logger.info(f"  ➜ 开始为 {total_to_supplement} 位新增演员检查并补全头像信息...")

            ids_to_fetch = [actor.get("id") for actor in actors_to_supplement if actor.get("id")]
            all_cached_metadata = self.actor_db_manager.get_full_actor_details_by_tmdb_ids(cursor, ids_to_fetch)
            
            supplemented_count = 0
            for actor in actors_to_supplement:
                if stop_event and stop_event.is_set(): raise InterruptedError("任务中止")
                
                tmdb_id = actor.get("id")
                profile_path = None
                cached_meta = all_cached_metadata.get(tmdb_id)
                if cached_meta and cached_meta.get("profile_path"):
                    profile_path = cached_meta["profile_path"]
                
                elif tmdb_api_key:
                    person_details = tmdb.get_person_details_tmdb(tmdb_id, tmdb_api_key)
                    if person_details:
                        if person_details.get("profile_path"):
                            profile_path = person_details["profile_path"]
                
                if profile_path:
                    actor["profile_path"] = profile_path
                    supplemented_count += 1

            logger.info(f"  ➜ 新增演员头像信息补全完成，成功为 {supplemented_count}/{total_to_supplement} 位演员补充了头像。")
        else:
            logger.info("  ➜ 没有需要补充头像的新增演员。")

        # ======================================================================
        # 步骤 5: ★★★ 从演员表移除无头像演员 ★★★
        # ======================================================================
        if self.config.get(constants.CONFIG_OPTION_REMOVE_ACTORS_WITHOUT_AVATARS, True):
            actors_with_avatars = [actor for actor in current_cast_list if actor.get("profile_path")]
            actors_without_avatars = [actor for actor in current_cast_list if not actor.get("profile_path")]

            if actors_without_avatars:
                removed_names = [a.get('name', f"TMDbID:{a.get('id')}") for a in actors_without_avatars]
                logger.info(f"  ➜ 将移除 {len(actors_without_avatars)} 位无头像的演员: {removed_names}")
                current_cast_list = actors_with_avatars
        else:
            logger.info("  ➜ 未启用移除无头像演员。")

        # ======================================================================
        # 步骤 6：智能截断逻辑 (Smart Truncation) ★★★
        # ======================================================================
        max_actors = self.config.get(constants.CONFIG_OPTION_MAX_ACTORS_TO_PROCESS, 30)
        try:
            limit = int(max_actors)
            if limit <= 0: limit = 30
        except (ValueError, TypeError):
            limit = 30

        original_count = len(current_cast_list)
        
        if original_count > limit:
            logger.info(f"  ➜ 演员列表总数 ({original_count}) 超过上限 ({limit})，将优先保留有头像的演员后进行截断。")
            sort_key = lambda x: x.get('order') if x.get('order') is not None and x.get('order') >= 0 else 999
            with_profile = [actor for actor in current_cast_list if actor.get("profile_path")]
            without_profile = [actor for actor in current_cast_list if not actor.get("profile_path")]
            with_profile.sort(key=sort_key)
            without_profile.sort(key=sort_key)
            prioritized_list = with_profile + without_profile
            current_cast_list = prioritized_list[:limit]
            logger.debug(f"  ➜ 截断后，保留了 {len(with_profile)} 位有头像演员中的 {len([a for a in current_cast_list if a.get('profile_path')])} 位。")
        else:
            # ▼▼▼ 核心修改：直接在 current_cast_list 上排序 ▼▼▼
            current_cast_list.sort(key=lambda x: x.get('order') if x.get('order') is not None and x.get('order') >= 0 else 999)

        # ======================================================================
        # 步骤 7: ★★★ 翻译和格式化 ★★★
        # ======================================================================
        logger.info(f"  ➜ 将对 {len(current_cast_list)} 位演员进行最终的翻译和格式化处理...")

        if not (self.ai_translator and self.config.get(constants.CONFIG_OPTION_AI_TRANSLATE_ACTOR_ROLE, False)):
            logger.info("  ➜ 翻译未启用，将保留演员和角色名原文。")
        else:
            final_translation_map = {}
            terms_to_translate = set()
            for actor in current_cast_list:
                character = actor.get('character')
                if character:
                    cleaned_character = utils.clean_character_name_static(character)
                    if cleaned_character and not utils.contains_chinese(cleaned_character):
                        terms_to_translate.add(cleaned_character)
                name = actor.get('name')
                if name and not utils.contains_chinese(name):
                    terms_to_translate.add(name)
            
            total_terms_count = len(terms_to_translate)
            logger.info(f"  ➜ [翻译统计] 1. 任务概览: 共收集到 {total_terms_count} 个独立词条需要翻译。")
            if total_terms_count > 0:
                logger.debug(f"    ➜ 待处理词条列表: {list(terms_to_translate)}")

            remaining_terms = list(terms_to_translate)
            if remaining_terms:
                cached_results = {}
                terms_for_api = []
                for term in remaining_terms:
                    cached = self.actor_db_manager.get_translation_from_db(cursor, term)
                    if cached and cached.get('translated_text'):
                        cached_results[term] = cached['translated_text']
                    else:
                        terms_for_api.append(term)
                
                cached_count = len(cached_results)
                logger.info(f"  ➜ [翻译统计] 2. 缓存检查: 命中数据库缓存 {cached_count} 条。")
                if cached_count > 0:
                    logger.debug("    ➜ 命中缓存的词条与译文:")
                    for k, v in sorted(cached_results.items()):
                        logger.debug(f"    ├─ {k} ➜ {v}")

                if cached_results:
                    final_translation_map.update(cached_results)
                if terms_for_api:
                    logger.info(f"  ➜ [翻译统计] 3. AI处理 (快速模式): 提交 {len(terms_for_api)} 条。")
                    if terms_for_api:
                        logger.debug(f"    ➜ 提交给[快速模式]的词条: {terms_for_api}")
                    fast_api_results = self.ai_translator.batch_translate(terms_for_api, mode='fast')
                    for term, translation in fast_api_results.items():
                        final_translation_map[term] = translation
                        self.actor_db_manager.save_translation_to_db(cursor, term, translation, self.ai_translator.provider)
                failed_terms = []
                for term in remaining_terms:
                    if not utils.contains_chinese(final_translation_map.get(term, term)):
                        failed_terms.append(term)
                remaining_terms = failed_terms
            if remaining_terms:
                logger.info(f"  ➜ [翻译统计] 4. AI处理 (音译模式): 提交 {len(remaining_terms)} 条。")
                if remaining_terms:
                    logger.debug(f"    ➜ 提交给[音译模式]的词条: {remaining_terms}")
                transliterate_results = self.ai_translator.batch_translate(remaining_terms, mode='transliterate')
                
                if isinstance(transliterate_results, dict):
                    final_translation_map.update(transliterate_results)
                elif isinstance(transliterate_results, list) and len(transliterate_results) == len(remaining_terms):
                    for i, term in enumerate(remaining_terms):
                        final_translation_map[term] = transliterate_results[i]
                
                still_failed_terms = []
                for term in remaining_terms:
                    if not utils.contains_chinese(final_translation_map.get(term, term)):
                        still_failed_terms.append(term)
                remaining_terms = still_failed_terms
                
            if remaining_terms:
                item_title = item_details_from_emby.get("Name")
                item_year = item_details_from_emby.get("ProductionYear")
                logger.info(f"  ➜ [翻译统计] 5. AI处理 (顾问模式): 提交 {len(remaining_terms)} 条。")
                if remaining_terms:
                    logger.debug(f"  ➜ 提交给[顾问模式]的词条: {remaining_terms}")
                quality_results = self.ai_translator.batch_translate(remaining_terms, mode='quality', title=item_title, year=item_year)
                
                # ★ 修复：防御 AI 翻译器返回非字典类型
                if isinstance(quality_results, dict):
                    final_translation_map.update(quality_results)
                elif isinstance(quality_results, list) and len(quality_results) == len(remaining_terms):
                    for i, term in enumerate(remaining_terms):
                        final_translation_map[term] = quality_results[i]
            
            successfully_translated_terms = {term for term in terms_to_translate if utils.contains_chinese(final_translation_map.get(term, ''))}
            failed_to_translate_terms = terms_to_translate - successfully_translated_terms
            
            logger.info(f"  ➜ [翻译统计] 6. 结果总结: 成功翻译 {len(successfully_translated_terms)}/{total_terms_count} 个词条。")
            if successfully_translated_terms:
                logger.debug("  ➜ 翻译成功列表 (原文 ➜ 译文):")
                for term in sorted(list(successfully_translated_terms)):
                    translation = final_translation_map.get(term)
                    logger.debug(f"    ├─ {term} ➜ {translation}")
            if failed_to_translate_terms:
                logger.warning(f"  ➜ 翻译失败列表 ({len(failed_to_translate_terms)}条): {list(failed_to_translate_terms)}")

            for actor in current_cast_list:
                original_name = actor.get('name')
                if original_name and original_name in final_translation_map:
                    actor['name'] = final_translation_map[original_name]
                original_character = actor.get('character')
                if original_character:
                    cleaned_character = utils.clean_character_name_static(original_character)
                    actor['character'] = final_translation_map.get(cleaned_character, cleaned_character)
                else:
                    actor['character'] = ''

        tmdb_to_emby_id_map = {
            str(actor.get('id')): actor.get('emby_person_id')
            for actor in current_cast_list if actor.get('id') and actor.get('emby_person_id')
        }
        # 获取原始数据
        raw_genres = item_details_from_emby.get("Genres", [])

        # 如果数据本身就是字符串列表（兼容旧数据），则保持不变
        if raw_genres and isinstance(raw_genres[0], dict):
            genres = [g.get('name') for g in raw_genres if g.get('name')]
        else:
            genres = raw_genres

        is_animation = "Animation" in genres or "动画" in genres or "Documentary" in genres or "纪录" in genres or "记录" in genres
        final_cast_perfect = actor_utils.format_and_complete_cast_list(
            current_cast_list, is_animation, self.config, mode='auto'
        )
        for actor in final_cast_perfect:
            tmdb_id_str = str(actor.get("id"))
            if tmdb_id_str in tmdb_to_emby_id_map:
                actor["emby_person_id"] = tmdb_to_emby_id_map[tmdb_id_str]
        for actor in final_cast_perfect:
            actor["provider_ids"] = {
                "Tmdb": str(actor.get("id")),
                "Imdb": actor.get("imdb_id"),
                "Douban": actor.get("douban_id")
            }

        # ======================================================================
        # 步骤 8: ★★★ 最终数据回写/反哺 ★★★ 
        # ======================================================================
        logger.info(f"  ➜ 开始将 {len(final_cast_perfect)} 位最终演员的完整信息同步回数据库...")
        processed_count = 0
        
        # 在循环外准备 emby_config，避免重复创建
        emby_config_for_upsert = {"url": self.emby_url, "api_key": self.emby_api_key, "user_id": self.emby_user_id}

        for actor in final_cast_perfect:
            # 直接将 actor 字典和 emby_config 传递给 upsert_person 函数
            map_id, action = self.actor_db_manager.upsert_person(cursor, actor, emby_config_for_upsert)
            
            if action not in ["ERROR", "SKIPPED", "CONFLICT_ERROR", "UNKNOWN_ERROR"]:
                processed_count += 1
            else:
                # 如果发生错误，回滚当前演员的操作，并为下一个演员开启新事务
                # 这是为了防止一个演员的错误导致整个批次失败
                cursor.connection.rollback()
                cursor.execute("BEGIN")

        logger.info(f"  ➜ 成功处理了 {processed_count} 位演员的数据库回写/更新。")

        return final_cast_perfect
    
    # --- 一键翻译 ---
    def translate_cast_list_for_editing(self, 
                                    cast_list: List[Dict[str, Any]], 
                                    title: Optional[str] = None, 
                                    year: Optional[int] = None,
                                    tmdb_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        【V14 - 纯AI翻译版】为手动编辑页面提供的一键翻译功能。
        - 彻底移除传统翻译引擎的降级逻辑。
        - 如果AI翻译未启用或失败，则直接放弃翻译。
        """
        if not cast_list:
            return []

        # ★★★ 核心修改 1: 检查AI翻译是否启用，如果未启用则直接返回 ★★★
        if not self.ai_translator or not self.config.get(constants.CONFIG_OPTION_AI_TRANSLATE_ACTOR_ROLE, False):
            logger.info("手动编辑-一键翻译：AI翻译未启用，任务跳过。")
            # 可以在这里返回一个提示给前端，或者直接返回原始列表
            # 为了前端体验，我们可以在第一个需要翻译的演员上加一个状态
            translated_cast_for_status = [dict(actor) for actor in cast_list]
            for actor in translated_cast_for_status:
                name_needs_translation = actor.get('name') and not utils.contains_chinese(actor.get('name'))
                role_needs_translation = actor.get('role') and not utils.contains_chinese(actor.get('role'))
                if name_needs_translation or role_needs_translation:
                    actor['matchStatus'] = 'AI未启用'
                    break # 只标记第一个即可
            return translated_cast_for_status

        # 从配置中读取模式
        translation_mode = self.config.get(constants.CONFIG_OPTION_AI_TRANSLATION_MODE, "fast")
        
        context_log = f" (上下文: {title} {year})" if title and translation_mode == 'quality' else ""
        logger.info(f"手动编辑-一键翻译：开始批量处理 {len(cast_list)} 位演员 (模式: {translation_mode}){context_log}。")
        
        translated_cast = [dict(actor) for actor in cast_list]
        
        # --- 纯AI批量翻译逻辑 ---
        try:
            with get_central_db_connection() as conn:
                cursor = conn.cursor()
                
                translation_cache = {} # 本次运行的内存缓存
                texts_to_translate = set()

                # 1. 收集所有需要翻译的词条
                texts_to_collect = set()
                for actor in translated_cast:
                    for field_key in ['name', 'role']:
                        text = actor.get(field_key, '').strip()
                        if field_key == 'role':
                            text = utils.clean_character_name_static(text)
                        if text and not utils.contains_chinese(text):
                            texts_to_collect.add(text)

                # 2. 根据模式决定是否使用缓存
                if translation_mode == 'fast':
                    logger.debug("[快速模式] 正在检查全局翻译缓存...")
                    for text in texts_to_collect:
                        cached_entry = self.actor_db_manager.get_translation_from_db(cursor=cursor, text=text)
                        if cached_entry:
                            translation_cache[text] = cached_entry.get("translated_text")
                        else:
                            texts_to_translate.add(text)
                else: # 'quality' mode
                    logger.debug("[顾问模式] 跳过缓存检查，直接翻译所有词条。")
                    texts_to_translate = texts_to_collect

                # 3. 如果有需要翻译的词条，调用AI
                if texts_to_translate:
                    logger.info(f"手动编辑-翻译：将 {len(texts_to_translate)} 个词条提交给AI (模式: {translation_mode})。")
                    translation_map_from_api = self.ai_translator.batch_translate(
                        texts=list(texts_to_translate),
                        mode=translation_mode,
                        title=title,
                        year=year
                    )
                    if translation_map_from_api:
                        translation_cache.update(translation_map_from_api)
                        
                        if translation_mode == 'fast':
                            for original, translated in translation_map_from_api.items():
                                self.actor_db_manager.save_translation_to_db(
                                    cursor=cursor,
                                    original_text=original, 
                                    translated_text=translated, 
                                    engine_used=self.ai_translator.provider
                                )
                    else:
                        logger.warning("手动编辑-翻译：AI批量翻译未返回任何结果。")
                else:
                    logger.info("手动编辑-翻译：所有词条均在缓存中找到，无需调用API。")

                # 4. 回填所有翻译结果
                if translation_cache:
                    for i, actor in enumerate(translated_cast):
                        original_name = actor.get('name', '').strip()
                        if original_name in translation_cache:
                            translated_cast[i]['name'] = translation_cache[original_name]
                        
                        original_role_raw = actor.get('role', '').strip()
                        cleaned_original_role = utils.clean_character_name_static(original_role_raw)
                        
                        if cleaned_original_role in translation_cache:
                            translated_cast[i]['role'] = translation_cache[cleaned_original_role]
                        
                        if translated_cast[i].get('name') != actor.get('name') or translated_cast[i].get('role') != actor.get('role'):
                            translated_cast[i]['matchStatus'] = '已翻译'
        
        except Exception as e:
            logger.error(f"一键翻译时发生错误: {e}", exc_info=True)
            # 可以在这里给出一个错误提示
            for actor in translated_cast:
                actor['matchStatus'] = '翻译出错'
                break
            return translated_cast

        logger.info("手动编辑-翻译完成。")
        return translated_cast
    
    # --- 手动处理 ---
    def process_item_with_manual_cast(self, item_id: str, manual_cast_list: List[Dict[str, Any]], item_name: str) -> bool:
        """
        【V2.5 - 终极修复版】
        1. 增加了完整的日志记录，让每一步操作都清晰可见。
        2. 修复并强化了“翻译缓存反哺”功能。
        3. 增加了在写入文件前的强制“最终格式化”步骤，确保前缀永远正确。
        """
        logger.info(f"  ➜ 手动处理流程启动：ItemID: {item_id} ('{item_name}')")
        
        try:
            item_details = emby.get_emby_item_details(item_id, self.emby_url, self.emby_api_key, self.emby_user_id)
            if not item_details: raise ValueError(f"无法获取项目 {item_id} 的详情。")
            
            raw_emby_actors = [p for p in item_details.get("People", []) if p.get("Type") == "Actor"]
            emby_config = {"url": self.emby_url, "api_key": self.emby_api_key, "user_id": self.emby_user_id}

            # ★★★ 核心修改: 在所有操作开始前，一次性获取所有 enriched_actors ★★★
            with get_central_db_connection() as conn:
                cursor = conn.cursor()
                enriched_actors = self.actor_db_manager.enrich_actors_with_provider_ids(cursor, raw_emby_actors, emby_config)

            # ======================================================================
            # 步骤 1: 数据准备与定位 (现在只负责构建映射)
            # ======================================================================
            logger.info(f"  ➜ 手动处理：步骤 1/6: 构建TMDb与Emby演员的ID映射...")
            tmdb_to_emby_map = {}
            for person in enriched_actors:
                person_tmdb_id = (person.get("ProviderIds") or {}).get("Tmdb")
                if person_tmdb_id:
                    tmdb_to_emby_map[str(person_tmdb_id)] = person.get("Id")
            logger.info(f"  ➜ 成功构建了 {len(tmdb_to_emby_map)} 条ID映射。")
            
            item_type = item_details.get("Type")
            tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
            if not tmdb_id: raise ValueError(f"项目 {item_id} 缺少 TMDb ID。")

            # --- 获取 TMDb 详情用于分级数据提取 ---
            tmdb_details_for_manual_extra = None
            aggregated_tmdb_data_manual = None
            if self.tmdb_api_key:
                if item_type == "Movie":
                    tmdb_details_for_manual_extra = tmdb.get_movie_details(tmdb_id, self.tmdb_api_key)
                    if not tmdb_details_for_manual_extra:
                        logger.warning(f"  ➜ 手动处理：无法从 TMDb 获取电影 '{item_name}' ({tmdb_id}) 的详情。")
                elif item_type == "Series":
                    aggregated_tmdb_data_manual = tmdb.aggregate_full_series_data_from_tmdb(int(tmdb_id), self.tmdb_api_key)
                    if aggregated_tmdb_data_manual:
                        tmdb_details_for_manual_extra = aggregated_tmdb_data_manual.get("series_details")
                    else:
                        logger.warning(f"  ➜ 手动处理：无法从 TMDb 获取剧集 '{item_name}' ({tmdb_id}) 的详情。")
            else:
                logger.warning("  ➜ 手动处理：未配置 TMDb API Key，无法获取 TMDb 详情用于分级数据。")

            # ======================================================================
            # 步骤 2: 更新AI翻译缓存
            # ======================================================================
            logger.info(f"  ➜ 手动处理：步骤 2/5: 检查并更新AI翻译缓存...")
            try:
                # ★★★ 核心修复 ①: 从缓存获取的是 tmdbId -> 原始角色名 的字典 ★★★
                original_roles_map = self.manual_edit_cache.get(item_id)
                if original_roles_map:
                    with get_central_db_connection() as conn:
                        cursor = conn.cursor()
                        updated_count = 0
                        
                        # ★★★ 核心修复 ②: 遍历前端提交的列表 ★★★
                        for actor_from_frontend in manual_cast_list:
                            tmdb_id_str = str(actor_from_frontend.get("tmdbId"))
                            if not tmdb_id_str: continue
                            
                            # ★★★ 核心修复 ③: 用 tmdbId 精准找到修改前的角色名 ★★★
                            original_role = original_roles_map.get(tmdb_id_str)
                            if original_role is None: # 如果原始记录里就没有这个演员，就跳过
                                continue

                            new_role = actor_from_frontend.get('role', '')
                            
                            cleaned_new_role = utils.clean_character_name_static(new_role)
                            cleaned_original_role = utils.clean_character_name_static(original_role)

                            if cleaned_new_role and cleaned_new_role != cleaned_original_role:
                                cache_entry = self.actor_db_manager.get_translation_from_db(text=cleaned_original_role, by_translated_text=True, cursor=cursor)
                                if cache_entry and 'original_text' in cache_entry:
                                    original_text_key = cache_entry['original_text']
                                    self.actor_db_manager.save_translation_to_db(
                                        cursor=cursor, original_text=original_text_key,
                                        translated_text=cleaned_new_role, engine_used="manual"
                                    )
                                    logger.debug(f"  ➜ AI翻译缓存已更新: '{original_text_key}' ('{cleaned_original_role}' -> '{cleaned_new_role}')")
                                    updated_count += 1
                        if updated_count > 0:
                            logger.info(f"  ➜ 成功更新了 {updated_count} 条翻译缓存。")
                        else:
                            logger.info(f"  ➜ 无需更新翻译缓存 (角色名未发生有效变更)。")
                        conn.commit()
                else:
                    logger.warning(f"  ➜ 无法更新翻译缓存：内存中找不到 ItemID {item_id} 的原始演员数据会话。")
            except Exception as e:
                logger.error(f"  ➜ 手动处理期间更新翻译缓存时发生顶层错误: {e}", exc_info=True)
            
            # ======================================================================
            # 步骤 3: API前置操作 (更新演员名)
            # ======================================================================
            logger.info(f"  ➜ 手动处理：步骤 3/6: 通过API更新现有演员的名字...")
            # 构建 TMDb ID -> Emby Person ID 和 Emby Person ID -> 当前名字的映射
            emby_id_to_name_map = {}
            for person in enriched_actors: # ★★★ 直接使用 enriched_actors
                person_emby_id = person.get("Id")
                if person_emby_id:
                    emby_id_to_name_map[person_emby_id] = person.get("Name")
            
            tmdb_to_emby_map = {}
            emby_id_to_name_map = {}
            for person in enriched_actors:
                person_tmdb_id = (person.get("ProviderIds") or {}).get("Tmdb")
                person_emby_id = person.get("Id")
                if person_tmdb_id and person_emby_id:
                    tmdb_to_emby_map[str(person_tmdb_id)] = person_emby_id
                    emby_id_to_name_map[person_emby_id] = person.get("Name")

            updated_names_count = 0
            for actor_from_frontend in manual_cast_list:
                tmdb_id_str = str(actor_from_frontend.get("tmdbId"))
                
                # 只处理在映射中能找到的、已存在的演员
                actor_emby_id = tmdb_to_emby_map.get(tmdb_id_str)
                if not actor_emby_id: continue

                new_name = actor_from_frontend.get("name")
                original_name = emby_id_to_name_map.get(actor_emby_id)
                
                if new_name and original_name and new_name != original_name:
                    emby.update_person_details(
                        person_id=actor_emby_id, new_data={"Name": new_name},
                        emby_server_url=self.emby_url, emby_api_key=self.emby_api_key, user_id=self.emby_user_id
                    )
                    updated_names_count += 1
            
            if updated_names_count > 0:
                logger.info(f"  ➜ 成功通过 API 更新了 {updated_names_count} 位演员的名字。")

            # ======================================================================
            # 步骤 4: 从数据库重建原始数据，并融合前端修改
            # ======================================================================
            logger.info(f"  ➜ 手动处理：步骤 4/6: 从数据库重建原始数据，识别并补全新增演员...")
            
            payload, db_actors = self._reconstruct_full_data_from_db(tmdb_id, item_type)
            original_cast_map = {str(actor.get('id') or actor.get('tmdb_id')): actor for actor in db_actors}

            new_actor_tmdb_ids = [
                int(actor.get("tmdbId")) for actor in manual_cast_list 
                if str(actor.get("tmdbId")) not in original_cast_map
            ]

            all_new_actors_metadata = {}
            if new_actor_tmdb_ids:
                with get_central_db_connection() as conn_new:
                    cursor_new = conn_new.cursor()
                    all_new_actors_metadata = self.actor_db_manager.get_full_actor_details_by_tmdb_ids(cursor_new, new_actor_tmdb_ids)

            new_cast_built = []
            
            with get_central_db_connection() as conn:
                cursor = conn.cursor()
                for actor_from_frontend in manual_cast_list:
                    tmdb_id_str = str(actor_from_frontend.get("tmdbId"))
                    if not tmdb_id_str: continue
                    
                    # --- A. 处理现有演员 ---
                    if tmdb_id_str in original_cast_map:
                        updated_actor_entry = original_cast_map[tmdb_id_str].copy()
                        updated_actor_entry['name'] = actor_from_frontend.get('name')
                        updated_actor_entry['character'] = actor_from_frontend.get('role')
                        new_cast_built.append(updated_actor_entry)
                    
                    # --- B. 处理新增演员 ---
                    else:
                        logger.info(f"    ├─ 发现新演员: '{actor_from_frontend.get('name')}' (TMDb ID: {tmdb_id_str})，开始补全元数据...")
                        person_details = all_new_actors_metadata.get(int(tmdb_id_str))
                        if not person_details:
                            person_details_from_api = tmdb.get_person_details_tmdb(tmdb_id_str, self.tmdb_api_key)
                            if person_details_from_api:
                                self.actor_db_manager.update_actor_metadata_from_tmdb(cursor, tmdb_id_str, person_details_from_api)
                                person_details = person_details_from_api
                            else:
                                person_details = {} 

                        new_actor_entry = {
                            "id": int(tmdb_id_str),
                            "name": actor_from_frontend.get('name'),
                            "character": actor_from_frontend.get('role'),
                            "original_name": person_details.get("original_name"),
                            "profile_path": person_details.get("profile_path"),
                            "adult": person_details.get("adult", False),
                            "gender": person_details.get("gender", 0),
                            "known_for_department": person_details.get("known_for_department", "Acting"),
                            "popularity": person_details.get("popularity", 0.0),
                            "order": 999 
                        }
                        new_cast_built.append(new_actor_entry)

            # ======================================================================
            # 步骤 5: 最终格式化并写入 NFO
            # ======================================================================
            logger.info(f"  ➜ 手动处理：步骤 5/6: 重建演员列表并生成物理 NFO...")
            genres = item_details.get("Genres", [])
            is_animation = "Animation" in genres or "动画" in genres or "Documentary" in genres or "纪录" in genres
            final_formatted_cast = actor_utils.format_and_complete_cast_list(
                new_cast_built, is_animation, self.config, mode='manual'
            )
            
            # 直接调用 sync_item_metadata 写入 NFO
            self.sync_item_metadata(
                item_details=item_details,
                tmdb_id=tmdb_id,
                final_cast_override=final_formatted_cast,
                metadata_override=payload
            )

            # ======================================================================
            # 步骤 6: 触发刷新并更新日志
            # ======================================================================
            logger.info("  ➜ 手动处理：步骤 6/6: 触发 Emby 刷新并更新内部日志...")
            
            emby.refresh_emby_item_metadata(
                item_emby_id=item_id,
                emby_server_url=self.emby_url,
                emby_api_key=self.emby_api_key,
                user_id_for_ops=self.emby_user_id,
                replace_all_metadata_param=True,
                item_name_for_log=item_name
            )

            # 更新我们自己的数据库日志和缓存
            with get_central_db_connection() as conn:
                cursor = conn.cursor()
                formatted_manual_metadata = None
                if tmdb_details_for_manual_extra:
                    formatted_manual_metadata = construct_metadata_payload(
                        item_type=item_type,
                        tmdb_data=tmdb_details_for_manual_extra,
                        aggregated_tmdb_data=aggregated_tmdb_data_manual,
                        emby_data_fallback=item_details
                    )
                # 写入数据库
                self._upsert_media_metadata(
                    cursor=cursor,
                    item_type=item_type,
                    item_details_from_emby=item_details,
                    final_processed_cast=final_formatted_cast, 
                    source_data_package=formatted_manual_metadata, 
                )
                
                logger.info(f"  ➜ 正在将手动处理完成的《{item_name}》写入已处理日志...")
                self.log_db_manager.save_to_processed_log(cursor, item_id, item_name, score=10.0)
                self.log_db_manager.remove_from_failed_log(cursor, item_id)
                conn.commit()

            logger.info(f"  ➜ 手动处理 '{item_name}' 流程完成。")
            return True

        except Exception as e:
            logger.error(f"  ➜ 手动处理 '{item_name}' 时发生严重错误: {e}", exc_info=True)
            return False
        finally:
            if item_id in self.manual_edit_cache:
                del self.manual_edit_cache[item_id]
                logger.trace(f"已清理 ItemID {item_id} 的手动编辑会话缓存。")
    
    # --- 为前端准备演员列表用于编辑 ---
    def get_cast_for_editing(self, item_id: str) -> Optional[Dict[str, Any]]:
        """
        【纯净 DB 模式】为前端编辑页面准备演员数据。
        直接从数据库重建权威演员表，不再依赖本地 JSON 文件。
        """
        logger.info(f"  ➜ 为编辑页面准备数据：ItemID {item_id}")
        
        try:
            # 1. 获取 Emby 基础详情
            emby_details = emby.get_emby_item_details(item_id, self.emby_url, self.emby_api_key, self.emby_user_id)
            if not emby_details:
                raise ValueError(f"在Emby中未找到项目 {item_id}")

            item_name_for_log = emby_details.get("Name", f"未知(ID:{item_id})")
            tmdb_id = emby_details.get("ProviderIds", {}).get("Tmdb")
            item_type = emby_details.get("Type")
            if not tmdb_id:
                raise ValueError(f"项目 '{item_name_for_log}' 缺少 TMDb ID。")

            # 2. 直接从数据库重建完整数据 (复用现成的神级方法)
            payload, db_actors = self._reconstruct_full_data_from_db(tmdb_id, item_type)
            if not db_actors:
                raise ValueError(f"数据库中未找到 '{item_name_for_log}' 的演员表缓存。请先对其进行一次完整处理。")

            logger.debug(f"  ➜ 成功从数据库为 '{item_name_for_log}' 恢复了 {len(db_actors)} 位演员。")

            # 3. 构建 TMDb ID -> emby_person_id 的映射
            tmdb_to_emby_map = {}
            for person in emby_details.get("People", []):
                person_tmdb_id = (person.get("ProviderIds") or {}).get("Tmdb")
                if person_tmdb_id:
                    tmdb_to_emby_map[str(person_tmdb_id)] = person.get("Id")
            
            # 4. 组装最终数据
            cast_for_frontend = []
            session_cache_map = {}
            
            for actor_data in db_actors:
                actor_tmdb_id = actor_data.get('tmdb_id') or actor_data.get('id')
                if not actor_tmdb_id: continue
                
                emby_person_id = tmdb_to_emby_map.get(str(actor_tmdb_id))
                
                image_url = None
                profile_path = actor_data.get("profile_path")
                if profile_path:
                    if profile_path.startswith('http'):
                        image_url = profile_path
                    else:
                        image_url = f"https://image.tmdb.org/t/p/w185{profile_path}"
                
                original_role = actor_data.get('character', '')
                session_cache_map[str(actor_tmdb_id)] = original_role
                cleaned_role_for_display = utils.clean_character_name_static(original_role)

                cast_for_frontend.append({
                    "tmdbId": actor_tmdb_id,
                    "name": actor_data.get('name'),
                    "role": cleaned_role_for_display,
                    "imageUrl": image_url,
                    "emby_person_id": emby_person_id
                })
                    
            # 5. 缓存会话数据并准备最终响应
            self.manual_edit_cache[item_id] = session_cache_map

            failed_log_info = {}
            with get_central_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT error_message, score FROM failed_log WHERE item_id = %s", (item_id,))
                row = cursor.fetchone()
                if row: failed_log_info = dict(row)

            response_data = {
                "item_id": item_id,
                "item_name": emby_details.get("Name"),
                "item_type": emby_details.get("Type"),
                "image_tag": emby_details.get('ImageTags', {}).get('Primary'),
                "original_score": failed_log_info.get("score"),
                "review_reason": failed_log_info.get("error_message"),
                "current_emby_cast": cast_for_frontend,
                "search_links": {
                    "baidu": utils.generate_search_url('baike', emby_details.get("Name"), emby_details.get("ProductionYear")),
                    "wikipedia": utils.generate_search_url('wikipedia', emby_details.get("Name"), emby_details.get("ProductionYear")),
                    "google": utils.generate_search_url('google', emby_details.get("Name"), emby_details.get("ProductionYear"))
                }
            }
            return response_data

        except Exception as e:
            logger.error(f"  ➜ 获取编辑数据失败 for ItemID {item_id}: {e}", exc_info=True)
            return None
    
    # --- 从 TMDb 直接下载图片 (用于实时监控/预处理/追剧刷新) ---
    def download_images_from_tmdb(self, tmdb_id: str, item_type: str, aggregated_tmdb_data: Optional[Dict[str, Any]] = None, item_details: Optional[Dict[str, Any]] = None, force_overwrite_episodes: Optional[List[str]] = None) -> bool:
        if not tmdb_id: return False

        try:
            log_prefix = "[图片下载]"
            # ======================================================================
            # ★★★ 核心修复：精准区分“剧集根目录”和“分集目录” ★★★
            # ======================================================================
            series_root_dir = ""
            episode_dir = ""
            
            if not item_details or not item_details.get("Path"):
                logger.warning(f"  ➜ {log_prefix} 缺少物理路径，无法下载图片。")
                return False
            media_path = item_details.get("Path")
            episode_dir = os.path.dirname(media_path) if os.path.isfile(media_path) else media_path
            
            import re
            series_root_dir = episode_dir
            # 如果当前目录名是 Season XX 或 Specials，说明在季文件夹内，根目录需要往上一级
            if re.match(r'^(Season|S)\s*\d+|Specials', os.path.basename(episode_dir), re.IGNORECASE):
                series_root_dir = os.path.dirname(episode_dir)

            # 1. 获取基础信息确定语言 (略...)
            orig_lang = "en"
            try:
                if item_type == "Movie": base_info = tmdb.get_movie_details(int(tmdb_id), self.tmdb_api_key, append_to_response="")
                elif item_type == "Series": base_info = tmdb.get_tv_details(int(tmdb_id), self.tmdb_api_key, append_to_response="")
                if base_info: orig_lang = base_info.get("original_language", "en")
            except: pass

            # 2. 确定搜索策略 (略...)
            lang_pref = self.config.get(constants.CONFIG_OPTION_TMDB_IMAGE_LANGUAGE_PREFERENCE, 'zh')
            search_strategies = []
            if lang_pref == 'zh':
                search_strategies.extend([("zh-CN", "简体中文"), ("zh-TW,zh,zh-HK,zh-SG", "繁体/通用中文"), ("en,null", "英文/无文字")])
                if orig_lang not in ['zh', 'cn', 'tw', 'hk', 'en']: search_strategies.append((f"{orig_lang}", f"原语言({orig_lang})"))
            else:
                if orig_lang in ['zh', 'cn', 'tw', 'hk']: search_strategies.append(("zh-CN,zh-HK,zh-TW,zh,cn", f"原语言(中文系/{orig_lang})"))
                elif orig_lang != 'en': search_strategies.append((orig_lang, f"原语言({orig_lang})"))
                search_strategies.append(("en,null", "英文/无文字"))
                if orig_lang not in ['zh', 'cn', 'tw', 'hk']: search_strategies.append(("zh-CN,zh-HK,zh-TW,zh", "中文兜底"))

            tmdb_data = None
            for lang_param, desc in search_strategies:
                try:
                    if item_type == "Movie": data = tmdb.get_movie_details(int(tmdb_id), self.tmdb_api_key, append_to_response="images", include_image_language=lang_param)
                    elif item_type == "Series": data = tmdb.get_tv_details(int(tmdb_id), self.tmdb_api_key, append_to_response="images,seasons", include_image_language=lang_param)
                    if data and data.get("images", {}).get("posters"):
                        tmdb_data = data
                        break 
                except: pass

            if not tmdb_data: return False

            # =========================================================
            # 4. 图片选择与命名逻辑 (存入绝对路径)
            # =========================================================
            downloads = [] # 存储 (url, 绝对保存路径, 是否强制覆盖)
            images_node = tmdb_data.get("images", {})

            # --- A. 海报 (Poster) -> 根目录 ---
            if images_node.get("posters"):
                downloads.append((images_node["posters"][0]["file_path"], os.path.join(series_root_dir, "poster.jpg"), False))
            
            # --- B. 背景 (Backdrop) -> 根目录 ---
            backdrops_list = images_node.get("backdrops", [])
            selected_backdrop = backdrops_list[0]["file_path"] if backdrops_list else tmdb_data.get("backdrop_path")
            if selected_backdrop:
                downloads.append((selected_backdrop, os.path.join(series_root_dir, "fanart.jpg"), False))
                downloads.append((selected_backdrop, os.path.join(series_root_dir, "landscape.jpg"), False))

            # --- C. Logo -> 根目录 ---
            if images_node.get("logos"):
                downloads.append((images_node["logos"][0]["file_path"], os.path.join(series_root_dir, "clearlogo.png"), False))

            # --- D. 剧集季海报 & 分集图 ---
            if item_type == "Series":
                # 季海报 -> 根目录
                seasons_source = aggregated_tmdb_data.get("seasons_details", []) if aggregated_tmdb_data else tmdb_data.get("seasons", [])
                for season in seasons_source:
                    s_num = season.get("season_number")
                    s_poster = season.get("poster_path")
                    if s_num is not None and s_poster:
                        downloads.append((s_poster, os.path.join(series_root_dir, f"season{s_num:02d}-poster.jpg"), False))
                # 分集图 -> 深度遍历寻找视频文件
                if aggregated_tmdb_data and "episodes_details" in aggregated_tmdb_data:
                    episodes = aggregated_tmdb_data["episodes_details"]
                    ep_list = episodes.values() if isinstance(episodes, dict) else (episodes if isinstance(episodes, list) else [])
                    
                    if item_details and item_details.get("Path") and os.path.isdir(series_root_dir):
                        import re
                        valid_exts = {'.mp4', '.mkv', '.avi', '.ts', '.iso', '.rmvb', '.strm'}
                        
                        # ★★★ 核心修复：使用 os.walk 深度遍历，钻进 Season 文件夹找视频 ★★★
                        for root, dirs, files in os.walk(series_root_dir):
                            for filename in files:
                                if os.path.splitext(filename)[1].lower() not in valid_exts: continue
                                match = re.search(r'[sS](\d{1,4})[eE](\d{1,4})', filename)
                                if match:
                                    target_s, target_e = int(match.group(1)), int(match.group(2))
                                    for ep in ep_list:
                                        if ep.get("season_number") == target_s and ep.get("episode_number") == target_e:
                                            e_still = ep.get("still_path")
                                            if e_still:
                                                thumb_name = os.path.splitext(filename)[0] + "-thumb.jpg"
                                                # 检查是否需要强制覆盖
                                                ep_key = f"S{target_s}E{target_e}"
                                                force_overwrite = force_overwrite_episodes and ep_key in force_overwrite_episodes
                                                downloads.append((e_still, os.path.join(root, thumb_name), force_overwrite))
                                            break

            # 5. 执行下载
            base_image_url = "https://image.tmdb.org/t/p/original"
            import requests
            import concurrent.futures
            proxies = config_manager.get_proxies_for_requests()
            
            def _download_single_image(tmdb_path, save_path, force_overwrite=False): 
                if not tmdb_path: return 0
                full_url = f"{base_image_url}{tmdb_path}"
                # ★ 如果没有开启强制覆盖，且文件已存在，则跳过
                if not force_overwrite and os.path.exists(save_path) and os.path.getsize(save_path) > 0: return 0
                try:
                    resp = requests.get(full_url, timeout=15, proxies=proxies)
                    if resp.status_code == 200:
                        with open(save_path, 'wb') as f: f.write(resp.content)
                        return 1
                except Exception as e:
                    logger.warning(f"  ➜ 下载图片失败 {os.path.basename(save_path)}: {e}")
                return 0

            success_count = 0
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                # ★★★ 核心修复：这里必须用 3 个变量解包！ ★★★
                futures = [executor.submit(_download_single_image, path, save_path, force_overwrite) for path, save_path, force_overwrite in downloads]
                for future in concurrent.futures.as_completed(futures):
                    success_count += future.result()

            logger.info(f"  ➜ {log_prefix} 共下载 {success_count} 张图片。")
            return True

        except Exception as e:
            logger.error(f"{log_prefix} 发生未知错误: {e}", exc_info=True)
            return False

    # --- 手动替换媒体图片 ---
    def update_media_image_manually(self, item_id: str, image_type: str, image_url: Optional[str] = None, image_bytes: Optional[bytes] = None) -> Tuple[bool, str]:
        """
        手动更新媒体图片。直接覆盖物理文件，并通知 Emby 刷新。
        支持传入图片直链 (image_url) 或 二进制文件流 (image_bytes)。
        """
        # 1. 校验图片类型和对应的文件名
        valid_types = {
            'poster': 'poster.jpg',
            'clearlogo': 'clearlogo.png',
            'fanart': 'fanart.jpg',
            'landscape': 'landscape.jpg'
        }
        if image_type not in valid_types:
            return False, f"不支持的图片类型: {image_type}"

        try:
            # 2. 获取媒体的物理路径
            item_details = emby.get_emby_item_details(item_id, self.emby_url, self.emby_api_key, self.emby_user_id)
            if not item_details or not item_details.get("Path"):
                return False, "无法获取该媒体的物理路径，请确保文件存在。"

            media_path = item_details.get("Path")
            target_dir = os.path.dirname(media_path) if os.path.isfile(media_path) else media_path
            
            # 智能判断：如果是剧集，且当前在 Season 文件夹内，需要退回上一级根目录
            import re
            if re.match(r'^(Season|S)\s*\d+|Specials', os.path.basename(target_dir), re.IGNORECASE):
                target_dir = os.path.dirname(target_dir)

            target_file_path = os.path.join(target_dir, valid_types[image_type])
            logger.info(f"  ➜ [手动换图] 准备覆盖物理文件: {target_file_path}")

            # 3. 保存图片
            if image_bytes:
                # 模式 A: 直接保存上传的文件流
                with open(target_file_path, 'wb') as f:
                    f.write(image_bytes)
                logger.info(f"  ➜ [手动换图] 成功保存上传的图片流。")
                
            elif image_url:
                # 模式 B: 下载网络图片
                import requests
                proxies = config_manager.get_proxies_for_requests()
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
                resp = requests.get(image_url, timeout=15, proxies=proxies, headers=headers)
                resp.raise_for_status()
                with open(target_file_path, 'wb') as f:
                    f.write(resp.content)
                logger.info(f"  ➜ [手动换图] 成功从 URL 下载并保存图片。")
            else:
                return False, "未提供图片 URL 或文件数据。"

            # 4. 通知 Emby 刷新 (局部刷新，仅让 Emby 重新读取本地文件)
            emby.refresh_emby_item_metadata(
                item_emby_id=item_id,
                emby_server_url=self.emby_url,
                emby_api_key=self.emby_api_key,
                user_id_for_ops=self.emby_user_id,
                replace_all_metadata_param=False, # ★ 设为 False，防止覆盖其他元数据，只扫本地图
                item_name_for_log=item_details.get("Name", "未知项目")
            )
            
            return True, f"{image_type} 替换成功！"

        except Exception as e:
            logger.error(f"  ➜ [手动换图] 失败: {e}", exc_info=True)
            return False, f"替换失败: {str(e)}"

    # --- 备份元数据 ---
    def sync_item_metadata(self, item_details: Dict[str, Any], tmdb_id: str,
                       final_cast_override: Optional[List[Dict[str, Any]]] = None,
                       episode_ids_to_sync: Optional[List[str]] = None,
                       metadata_override: Optional[Dict[str, Any]] = None,
                       is_series_refresh: bool = False):
        """
        【纯净 NFO 模式】基于模板和现有数据构建元数据文件，生成 XML 写入物理目录。
        """
        item_type = item_details.get("Type")
        log_prefix = "[元数据写入]"

        data_to_write = copy.deepcopy(metadata_override) if metadata_override else {}
        cast_to_write = final_cast_override or []

        # 是否写入合集元数据（仅电影且开关未开启时剔除合集信息）
        if item_type == 'Movie':
            if not self.config.get(constants.CONFIG_OPTION_GENERATE_COLLECTION_NFO, False):
                # 如果开关未开启，从写入数据中剔除合集信息
                data_to_write.pop('belongs_to_collection', None)

        # =========================================================
        # ★★★ 通用数据净化阶段 ★★★
        # =========================================================
        if data_to_write:
            # A. 工作室/电视网中文化处理 (过滤并翻译)
            if self.config.get(constants.CONFIG_OPTION_STUDIO_TO_CHINESE, False):
                try:
                    studio_mapping_data = settings_db.get_setting('studio_mapping') or utils.DEFAULT_STUDIO_MAPPING
                    company_id_map, network_id_map, name_map = {}, {}, {}
                    for entry in studio_mapping_data:
                        label = entry.get('label')
                        if not label: continue
                        for cid in entry.get('company_ids', []): company_id_map[int(cid)] = label
                        for nid in entry.get('network_ids', []): network_id_map[int(nid)] = label
                        for en_name in entry.get('en', []): name_map[en_name.lower().strip()] = label

                    def filter_and_translate_studios(source_list, is_network_field=False):
                        if not source_list: return []
                        filtered = []
                        for item in source_list:
                            s_id, s_name = item.get('id'), item.get('name', '').strip()
                            mapped_label = None
                            if s_id is not None:
                                try: mapped_label = network_id_map.get(int(s_id)) if is_network_field else company_id_map.get(int(s_id))
                                except: pass
                            if not mapped_label and s_name: mapped_label = name_map.get(s_name.lower())
                            
                            # ★★★ 核心修复：有映射则改名并保留，无映射则直接丢弃！ ★★★
                            if mapped_label:
                                item['name'] = mapped_label
                                filtered.append(item)
                        return filtered

                    if item_type == 'Movie' and 'production_companies' in data_to_write:
                        data_to_write['production_companies'] = filter_and_translate_studios(data_to_write['production_companies'], False)
                    elif item_type == 'Series':
                        if 'networks' in data_to_write: data_to_write['networks'] = filter_and_translate_studios(data_to_write['networks'], True)
                        if 'production_companies' in data_to_write: data_to_write['production_companies'] = filter_and_translate_studios(data_to_write['production_companies'], False)
                except Exception as e_studio:
                    logger.warning(f"  ➜ {log_prefix} 处理工作室中文化时发生错误: {e_studio}")

            # B. 剧集专属：合并 Networks 和 Production Companies
            if item_type == 'Series':
                merged_list = data_to_write.get('networks', []) + data_to_write.get('production_companies', [])
                unique_networks, seen_ids, seen_names = [], set(), set()
                for item in merged_list:
                    if not isinstance(item, dict): continue
                    i_id, i_name = item.get('id'), item.get('name')
                    is_duplicate = False
                    if i_id:
                        if i_id in seen_ids: is_duplicate = True
                        else: seen_ids.add(i_id)
                    if i_name:
                        if i_name in seen_names: is_duplicate = True
                        else: seen_names.add(i_name)
                    if not i_id and not i_name: continue
                    if not is_duplicate: unique_networks.append(item)
                data_to_write['networks'] = unique_networks
                if 'production_companies' in data_to_write: del data_to_write['production_companies']

            # C. 关键词映射处理
            if self.config.get(constants.CONFIG_OPTION_KEYWORD_TO_TAGS, False):
                try:
                    mapping_data = settings_db.get_setting('keyword_mapping') or utils.DEFAULT_KEYWORD_MAPPING
                    keyword_map = {str(kid): entry.get('label') for entry in mapping_data if entry.get('label') for kid in entry.get('ids', [])}
                    
                    # ★ 修复：同时兼容 TMDb 的字典格式和本地数据库的列表格式
                    kw_data = data_to_write.get('keywords', [])
                    source_keywords = []
                    if isinstance(kw_data, dict):
                        source_keywords = kw_data.get('keywords') or kw_data.get('results') or []
                    elif isinstance(kw_data, list):
                        source_keywords = kw_data
                        
                    final_tags = {keyword_map[str(k.get('id', ''))] for k in source_keywords if isinstance(k, dict) and str(k.get('id', '')) in keyword_map}
                    
                    # 将映射后的中文标签存入一个特殊字段
                    data_to_write['_mapped_chinese_tags'] = list(final_tags)
                except Exception as e_tags:
                    logger.warning(f"  ➜ {log_prefix} 处理关键词映射时发生错误: {e_tags}")

        # ======================================================================
        # ★★★ 写入 NFO 文件 ★★★
        # ======================================================================
        logger.info(f"  ➜ 正在生成并写入 NFO 文件...")
        media_path = item_details.get("Path")
        if not media_path:
            logger.warning(f"  ➜ 无法获取物理路径，跳过 NFO 生成。")
            return

        episode_dir = os.path.dirname(media_path) if os.path.isfile(media_path) else media_path
        import re
        series_root_dir = episode_dir
        if re.match(r'^(Season|S)\s*\d+|Specials', os.path.basename(episode_dir), re.IGNORECASE):
            series_root_dir = os.path.dirname(episode_dir)

        # --- 智能比对写入函数 ---
        def _write_nfo_if_changed(file_path: str, content: str) -> bool:
            if os.path.exists(file_path):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        old_content = f.read()
                    
                    import re
                    def _get_tag_text(xml_str, tag):
                        # 提取指定标签的内容，忽略大小写和换行
                        match = re.search(f'<{tag}[^>]*>(.*?)</{tag}>', xml_str, re.IGNORECASE | re.DOTALL)
                        return match.group(1).strip() if match else ""

                    # 只比对最核心的两个字段：标题和简介
                    old_title = _get_tag_text(old_content, 'title')
                    old_plot = _get_tag_text(old_content, 'plot')
                    
                    new_title = _get_tag_text(content, 'title')
                    new_plot = _get_tag_text(content, 'plot')

                    # 只要标题和简介都没变，就认为核心数据没变，坚决不写硬盘！
                    if old_title == new_title and old_plot == new_plot:
                        return False 
                except Exception:
                    pass # 读取或解析失败则走正常覆盖流程
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                return True
            except Exception as e:
                logger.error(f"  ➜ 写入 NFO 失败 {file_path}: {e}")
                return False

        try:
            if item_type == "Movie":
                nfo_content = nfo_builder.build_movie_nfo(data_to_write, cast_to_write)
                nfo_path = os.path.splitext(media_path)[0] + ".nfo"
                if _write_nfo_if_changed(nfo_path, nfo_content):
                    logger.info(f"  ➜ 成功写入电影 NFO: {nfo_path}")
                else:
                    logger.debug(f"  ➜ 电影 NFO 内容未变，跳过写入: {nfo_path}")

            elif item_type == "Series":
                nfo_path = os.path.join(series_root_dir, "tvshow.nfo")
                
                # 追剧刷新时，读取旧 NFO 锁定标题和演员表
                if is_series_refresh and os.path.exists(nfo_path):
                    try:
                        import xml.etree.ElementTree as ET
                        tree = ET.parse(nfo_path)
                        root = tree.getroot()
                        
                        ext_title = root.findtext('title')
                        ext_orig = root.findtext('originaltitle')
                        ext_sort = root.findtext('sorttitle')
                        
                        if ext_title: data_to_write['name'] = ext_title
                        if ext_orig: data_to_write['original_name'] = ext_orig
                        if ext_sort: data_to_write['sorttitle'] = ext_sort
                        
                        if not cast_to_write:
                            old_actors = []
                            for actor_elem in root.findall('actor'):
                                old_actors.append({
                                    'name': actor_elem.findtext('name'),
                                    'character': actor_elem.findtext('role'),
                                    'order': actor_elem.findtext('order'),
                                    'profile_path': actor_elem.findtext('thumb'),
                                    'tmdb_id': actor_elem.findtext('tmdbid')
                                })
                            if old_actors:
                                cast_to_write = old_actors
                                logger.info(f"  ➜ 追剧刷新：已从旧 NFO 恢复 {len(old_actors)} 位演员。")
                                
                        logger.info("  ➜ 追剧刷新：已锁定并继承原有剧集标题与演员表。")
                    except Exception as e:
                        logger.warning(f"  ➜ 读取原有 NFO 失败: {e}")

                nfo_content = nfo_builder.build_tvshow_nfo(data_to_write, cast_to_write)
                if _write_nfo_if_changed(nfo_path, nfo_content):
                    logger.info(f"  ➜ 成功写入剧 NFO: {nfo_path}")
                
                episodes_data = data_to_write.get("episodes_details", {})
                seasons_data = data_to_write.get("seasons_details", [])
                
                if episodes_data and os.path.isdir(series_root_dir):
                    valid_exts = {'.mp4', '.mkv', '.avi', '.ts', '.iso', '.rmvb', '.strm'}
                    generated_count = 0
                    skipped_count = 0
                    season_dirs_processed = set()
                    
                    for root_dir, dirs, files in os.walk(series_root_dir):
                        for filename in files:
                            if os.path.splitext(filename)[1].lower() not in valid_exts: continue
                            match = re.search(r'[sS](\d{1,4})[eE](\d{1,4})', filename)
                            if match:
                                target_s, target_e = int(match.group(1)), int(match.group(2))
                                
                                if root_dir not in season_dirs_processed:
                                    season_info = next((s for s in seasons_data if s.get('season_number') == target_s), None)
                                    if season_info:
                                        season_nfo_content = nfo_builder.build_season_nfo(season_info)
                                        season_nfo_path = os.path.join(root_dir, "season.nfo")
                                        if _write_nfo_if_changed(season_nfo_path, season_nfo_content):
                                            logger.info(f"  ➜ 成功写入季 NFO: {season_nfo_path}")
                                    season_dirs_processed.add(root_dir)

                                ep_list = episodes_data.values() if isinstance(episodes_data, dict) else (episodes_data if isinstance(episodes_data, list) else [])
                                for ep in ep_list:
                                    if ep.get("season_number") == target_s and ep.get("episode_number") == target_e:
                                        ep_cast = ep.get('credits', {}).get('cast', [])
                                        if not ep_cast: ep_cast = cast_to_write 
                                        ep_nfo_content = nfo_builder.build_episode_nfo(ep, ep_cast)
                                        ep_nfo_path = os.path.join(root_dir, os.path.splitext(filename)[0] + ".nfo")
                                        
                                        # ★★★ 核心比对逻辑 ★★★
                                        if _write_nfo_if_changed(ep_nfo_path, ep_nfo_content):
                                            generated_count += 1
                                        else:
                                            skipped_count += 1
                                        break
                    logger.info(f"  ➜ 生成NFO完成，实际更新了 {generated_count} 个 NFO (跳过了 {skipped_count} 个未变更的)。")

            elif item_type == "Episode":
                nfo_content = nfo_builder.build_episode_nfo(data_to_write, cast_to_write)
                nfo_path = os.path.splitext(media_path)[0] + ".nfo"
                if _write_nfo_if_changed(nfo_path, nfo_content):
                    logger.info(f"  ➜ 成功写入分集 NFO: {nfo_path}")
                else:
                    logger.debug(f"  ➜ 分集 NFO 内容未变，跳过写入: {nfo_path}")

        except Exception as e:
            logger.error(f"  ➜ 写入 NFO 文件失败: {e}")

    # --- 提取标签 ---
    def extract_tag_names(item_data):
        """
        兼容新旧版 Emby API 提取标签名。
        """
        tags_set = set()

        # 1. 尝试提取 TagItems (新版/详细版)
        tag_items = item_data.get('TagItems')
        if isinstance(tag_items, list):
            for t in tag_items:
                if isinstance(t, dict):
                    name = t.get('Name')
                    if name:
                        tags_set.add(name)
                elif isinstance(t, str) and t:
                    tags_set.add(t)
        
        # 2. 尝试提取 Tags (旧版/简略版)
        tags = item_data.get('Tags')
        if isinstance(tags, list):
            for t in tags:
                if t:
                    tags_set.add(str(t))
        
        return list(tags_set)

    # --- 为一个媒体项同步元数据缓存 ---
    def sync_single_item_to_metadata_cache(self, item_id: str, item_name: Optional[str] = None):
        """
        【V12 - 极简版】
        仅用于响应 'metadata.update' 事件。
        将 Emby 中的最新元数据（标题、简介、标签等）快速镜像到本地数据库。
        
        注意：'追更/新分集入库' 不再使用此函数，而是走 process_single_item -> _upsert_media_metadata 流程。
        """
        log_prefix = f"实时同步媒体元数据 '{item_name}'"
        # logger.trace(f"  ➜ {log_prefix} 开始执行...")
        
        try:
            # 1. 获取 Emby 最新详情
            # 不需要请求 MediaSources 等重型字段，只需要元数据
            fields_to_get = "ProviderIds,Type,Name,OriginalTitle,Overview,Tags,TagItems,OfficialRating,CustomRating,Path,_SourceLibraryId,PremiereDate,ProductionYear"
            item_details = emby.get_emby_item_details(item_id, self.emby_url, self.emby_api_key, self.emby_user_id, fields=fields_to_get)
            
            if not item_details:
                logger.warning(f"  ➜ {log_prefix} 无法获取详情，跳过。")
                return
            
            tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
            item_type = item_details.get("Type")
            
            if not tmdb_id or item_type not in ['Movie', 'Series', 'Season', 'Episode']:
                # 仅同步我们关心的类型
                return
            
            # 补全 Library ID
            if not item_details.get('_SourceLibraryId'):
                lib_info = emby.get_library_root_for_item(item_id, self.emby_url, self.emby_api_key, self.emby_user_id)
                if lib_info: item_details['_SourceLibraryId'] = lib_info.get('Id')

            # 2. 直接更新数据库
            with get_central_db_connection() as conn:
                with conn.cursor() as cursor:
                    final_tags = extract_tag_names(item_details)
                    
                    # 基础字段更新
                    updates = {
                        "title": item_details.get('Name'),
                        "original_title": item_details.get('OriginalTitle'),
                        "overview": item_details.get('Overview'),
                        "tags_json": json.dumps(final_tags, ensure_ascii=False),
                        "last_synced_at": datetime.now(timezone.utc)
                    }
                    
                    # 日期字段处理
                    if item_details.get('PremiereDate'):
                        updates["release_date"] = item_details['PremiereDate']
                    if item_details.get('ProductionYear'):
                        updates["release_year"] = item_details['ProductionYear']

                    # 分级同步逻辑 
                    new_official_rating = item_details.get('OfficialRating')
                    if new_official_rating is not None: # 允许空字符串更新，代表清空
                        # 先查旧数据
                        cursor.execute("SELECT official_rating_json FROM media_metadata WHERE tmdb_id = %s AND item_type = %s", (tmdb_id, item_type))
                        row = cursor.fetchone()
                        current_rating_json = row['official_rating_json'] if row and row['official_rating_json'] else {}
                        
                        # 更新 US 字段
                        current_rating_json['US'] = new_official_rating
                        updates["official_rating_json"] = json.dumps(current_rating_json, ensure_ascii=False)
                    
                    # B. 同步自定义分级 (Emby 的 CustomRating -> 数据库 custom_rating)
                    # 直接赋值，Emby 传什么就是什么
                    updates["custom_rating"] = item_details.get('CustomRating')
                    
                    # 构建 SQL
                    set_clauses = [f"{key} = %s" for key in updates.keys()]
                    sql = f"UPDATE media_metadata SET {', '.join(set_clauses)} WHERE tmdb_id = %s AND item_type = %s"
                    
                    cursor.execute(sql, tuple(updates.values()) + (tmdb_id, item_type))
                    
                    # 如果是剧集，且 Emby 改了名字，可能需要级联更新分集吗？
                    # 通常不需要，分集有自己的记录。如果需要，那是全量刷新的事了。
                    
                    conn.commit()
            
            logger.info(f"  ➜ {log_prefix} 数据库同步完成。")

        except Exception as e:
            logger.error(f"{log_prefix} 执行时发生错误: {e}", exc_info=True)

    def close(self):
        if self.douban_api: self.douban_api.close()
