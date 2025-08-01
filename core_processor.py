# core_processor_sa.py

import os
import json
import sqlite3
import concurrent.futures
from typing import Dict, List, Optional, Any, Tuple
import shutil
import threading
import time
import requests
import copy
import random
# 确保所有依赖都已正确导入
import emby_handler
import tmdb_handler
import utils
import constants
import logging
import actor_utils
from cachetools import TTLCache
from db_handler import ActorDBManager
from db_handler import get_db_connection as get_central_db_connection
from ai_translator import AITranslator
from utils import LogDBManager, get_override_path_for_item
from watchlist_processor import WatchlistProcessor
from douban import DoubanApi
logger = logging.getLogger(__name__)
try:
    from douban import DoubanApi
    DOUBAN_API_AVAILABLE = True
except ImportError:
    DOUBAN_API_AVAILABLE = False
    class DoubanApi:
        def __init__(self, *args, **kwargs): pass
        def get_acting(self, *args, **kwargs): return {}
        def close(self): pass

def _read_local_json(file_path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(file_path):
        logger.warning(f"本地元数据文件不存在: {file_path}")
        return None
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"读取本地JSON文件失败: {file_path}, 错误: {e}")
        return None
def _save_metadata_to_cache(
    cursor: sqlite3.Cursor,
    tmdb_id: str,
    item_type: str,
    item_details: Dict[str, Any],
    processed_cast: List[Dict[str, Any]],
    raw_tmdb_json: Dict[str, Any]
):
    """
    将媒体项的关键元数据保存到 media_metadata 表中，用于后续的筛选。
    """
    try:
        # 提取导演信息
        directors = []
        crew = []
        if item_type == "Movie":
            crew = raw_tmdb_json.get("casts", {}).get("crew", [])
        elif item_type == "Series":
            crew = raw_tmdb_json.get("credits", {}).get("crew", [])
        
        for member in crew:
            if member.get("job") == "Director":
                directors.append({"id": member.get("id"), "name": member.get("name")})

        # ★★★ 新增：从原始TMDb JSON中提取国家/地区信息 ★★★
        countries = [
            country.get("name") 
            for country in raw_tmdb_json.get("production_countries", []) 
            if country.get("name")
        ]
        # 准备要存入数据库的数据
        metadata = {
            "tmdb_id": tmdb_id,
            "item_type": item_type,
            "title": item_details.get("Name"),
            "original_title": item_details.get("OriginalTitle"),
            "release_year": item_details.get("ProductionYear"),
            "rating": item_details.get("CommunityRating"),
            "genres_json": json.dumps(item_details.get("Genres", [])),
            "actors_json": json.dumps([{"id": p.get("id"), "name": p.get("name")} for p in processed_cast]),
            "directors_json": json.dumps(directors),
            "studios_json": json.dumps([s.get("Name") for s in item_details.get("Studios", [])]),
            "countries_json": json.dumps(countries),
        }

        # 使用 INSERT OR REPLACE 插入或更新数据
        columns = ', '.join(metadata.keys())
        placeholders = ', '.join('?' for _ in metadata)
        sql = f"INSERT OR REPLACE INTO media_metadata ({columns}) VALUES ({placeholders})"
        
        cursor.execute(sql, tuple(metadata.values()))
        logger.debug(f"成功将《{metadata['title']}》的元数据缓存到数据库。")

    except Exception as e:
        logger.error(f"保存元数据到缓存表时失败: {e}", exc_info=True)
# ==========================================================================================
# +++ 新增：JSON 构建器函数 (JSON Builders) +++
# 这些函数严格按照您提供的最小化模板来构建新的JSON对象，确保结构纯净。
# ==========================================================================================

def _build_movie_json(source_data: Dict[str, Any], processed_cast: List[Dict[str, Any]], processed_crew: List[Dict[str, Any]]) -> Dict[str, Any]:
    """根据模板构建电影 all.json"""
    return {
      "id": source_data.get("id", 0),
      "imdb_id": source_data.get("imdb_id", ""),
      "title": source_data.get("title", ""),
      "original_title": source_data.get("original_title", ""),
      "overview": source_data.get("overview", ""),
      "tagline": source_data.get("tagline", ""),
      "release_date": source_data.get("release_date", ""),
      "vote_average": source_data.get("vote_average", 0.0),
      "production_countries": source_data.get("production_countries", []),
      "production_companies": source_data.get("production_companies", []),
      "genres": source_data.get("genres", []),
      "casts": {
        "cast": processed_cast,
        "crew": processed_crew
      },
      "releases": source_data.get("releases", {"countries": []}),
      "belongs_to_collection": source_data.get("belongs_to_collection", None),
      "trailers": source_data.get("trailers", {"youtube": []}) # 假设神医插件需要这个结构
    }

def _build_series_json(source_data: Dict[str, Any], processed_cast: List[Dict[str, Any]]) -> Dict[str, Any]:
    """根据模板构建电视剧 series.json"""
    return {
      "id": source_data.get("id", 0),
      "name": source_data.get("name", ""),
      "original_name": source_data.get("original_name", ""),
      "overview": source_data.get("overview", ""),
      "vote_average": source_data.get("vote_average", 0.0),
      "episode_run_time": source_data.get("episode_run_time", []),
      "first_air_date": source_data.get("first_air_date", "1970-01-01T00:00:00.000Z"),
      "last_air_date": source_data.get("last_air_date", "1970-01-01T00:00:00.000Z"),
      "status": source_data.get("status", ""),
      "networks": source_data.get("networks", []),
      "genres": source_data.get("genres", []),
      "external_ids": source_data.get("external_ids", {"imdb_id": "", "tvrage_id": None, "tvdb_id": None}),
      "videos": source_data.get("videos", {"results": []}),
      "content_ratings": source_data.get("content_ratings", {"results": []}),
      "credits": {
        "cast": processed_cast
        # 注意：主剧集文件通常不包含 crew，除非API返回
      }
    }

def _build_season_json(source_data: Dict[str, Any], processed_cast: List[Dict[str, Any]], processed_crew: List[Dict[str, Any]]) -> Dict[str, Any]:
    """根据模板构建季 season-X.json"""
    return {
      "name": source_data.get("name", ""),
      "overview": source_data.get("overview", ""),
      "air_date": source_data.get("air_date", "1970-01-01T00:00:00.000Z"),
      "external_ids": source_data.get("external_ids", {"tvdb_id": None}),
      "credits": {
        "cast": processed_cast,
        "crew": processed_crew
      }
    }

def _build_episode_json(source_data: Dict[str, Any], processed_cast: List[Dict[str, Any]], processed_crew: List[Dict[str, Any]]) -> Dict[str, Any]:
    """根据模板构建集 season-X-episode-Y.json"""
    return {
      "name": source_data.get("name", ""),
      "overview": source_data.get("overview", ""),
      "videos": source_data.get("videos", {"results": []}),
      "external_ids": source_data.get("external_ids", {"tvdb_id": None, "tvrage_id": None, "imdb_id": ""}),
      "air_date": source_data.get("air_date", "1970-01-01T00:00:00.000Z"),
      "vote_average": source_data.get("vote_average", 0.0),
      "credits": {
        "cast": processed_cast,
        "guest_stars": [], # 按照统一注入的原则，这里也使用主演员表，并将 guest_stars 留空
        "crew": processed_crew
      }
    }

# ==========================================================================================
# +++ 新增：演员聚合函数 +++
# 从内存中的TMDB数据聚合演员，而不是从文件
# ==========================================================================================
def _aggregate_series_cast_from_tmdb_data(series_data: Dict[str, Any], all_episodes_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    【新】从内存中的TMDB数据聚合一个剧集的所有演员。
    """
    logger.debug(f"【演员聚合】开始为 '{series_data.get('name')}' 从内存中的TMDB数据聚合演员...")
    aggregated_cast_map = {}

    # 1. 优先处理主剧集的演员列表
    main_cast = series_data.get("credits", {}).get("cast", [])
    for actor in main_cast:
        actor_id = actor.get("id")
        if actor_id:
            aggregated_cast_map[actor_id] = actor
    logger.debug(f"  -> 从主剧集数据中加载了 {len(aggregated_cast_map)} 位主演员。")

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
    
    logger.info(f"【演员聚合】完成。共为 '{series_data.get('name')}' 聚合了 {len(full_aggregated_cast)} 位独立演员。")
    return full_aggregated_cast
class MediaProcessor:
    def __init__(self, config: Dict[str, Any]):
        # ★★★ 然后，从这个 config 字典里，解析出所有需要的属性 ★★★
        self.config = config
        self.db_path = config.get('db_path')
        if not self.db_path:
            raise ValueError("数据库路径 (db_path) 未在配置中提供。")

        # 初始化我们的数据库管理员
        self.actor_db_manager = ActorDBManager(self.db_path)
        self.log_db_manager = LogDBManager(self.db_path)

        # 从 config 中获取所有其他配置
        self.douban_api = None
        if getattr(constants, 'DOUBAN_API_AVAILABLE', False):
            try:
                # --- ✨✨✨ 核心修改区域 START ✨✨✨ ---

                # 1. 从配置中获取冷却时间 
                douban_cooldown = self.config.get(constants.CONFIG_OPTION_DOUBAN_DEFAULT_COOLDOWN, 2.0)
                
                # 2. 从配置中获取 Cookie，使用我们刚刚在 constants.py 中定义的常量
                douban_cookie = self.config.get(constants.CONFIG_OPTION_DOUBAN_COOKIE, "")
                
                # 3. 添加一个日志，方便调试
                if not douban_cookie:
                    logger.debug(f"配置文件中未找到或未设置 '{constants.CONFIG_OPTION_DOUBAN_COOKIE}'。如果豆瓣API返回'need_login'错误，请配置豆瓣cookie。")
                else:
                    logger.debug("已从配置中加载豆瓣 Cookie。")

                # 4. 将所有参数传递给 DoubanApi 的构造函数
                self.douban_api = DoubanApi(
                    cooldown_seconds=douban_cooldown,
                    user_cookie=douban_cookie  # <--- 将 cookie 传进去
                )
                logger.trace("DoubanApi 实例已在 MediaProcessorAPI 中创建。")
                
                # --- ✨✨✨ 核心修改区域 END ✨✨✨ ---

            except Exception as e:
                logger.error(f"MediaProcessorAPI 初始化 DoubanApi 失败: {e}", exc_info=True)
        else:
            logger.warning("DoubanApi 常量指示不可用，将不使用豆瓣功能。")
        self.emby_url = self.config.get("emby_server_url")
        self.emby_api_key = self.config.get("emby_api_key")
        self.emby_user_id = self.config.get("emby_user_id")
        self.tmdb_api_key = self.config.get("tmdb_api_key", "")
        self.local_data_path = self.config.get("local_data_path", "").strip()
        self.sync_images_enabled = self.config.get(constants.CONFIG_OPTION_SYNC_IMAGES, False)
        self.translator_engines = self.config.get(constants.CONFIG_OPTION_TRANSLATOR_ENGINES, constants.DEFAULT_TRANSLATOR_ENGINES_ORDER)
        
        self.ai_enabled = self.config.get("ai_translation_enabled", False)
        self.ai_translator = AITranslator(self.config) if self.ai_enabled else None
        
        self._stop_event = threading.Event()
        self.processed_items_cache = self._load_processed_log_from_db()
        self.manual_edit_cache = TTLCache(maxsize=10, ttl=600)
        logger.trace("核心处理器初始化完成。")
    # --- 清除已处理记录 ---
    def clear_processed_log(self):
        """
        【已改造】清除数据库和内存中的已处理记录。
        使用中央数据库连接函数。
        """
        try:
            # 1. ★★★ 调用中央函数，并传入 self.db_path ★★★
            with get_central_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                
                logger.debug("正在从数据库删除 processed_log 表中的所有记录...")
                cursor.execute("DELETE FROM processed_log")
                # with 语句会自动处理 conn.commit()
            
            logger.info("数据库中的已处理记录已清除。")

            # 2. 清空内存缓存
            self.processed_items_cache.clear()
            logger.info("内存中的已处理记录缓存已清除。")

        except Exception as e:
            logger.error(f"清除数据库或内存已处理记录时失败: {e}", exc_info=True)
            # 3. ★★★ 重新抛出异常，通知上游调用者操作失败 ★★★
            raise
    # ★★★ 公开的、独立的追剧判断方法 ★★★
    def check_and_add_to_watchlist(self, item_details: Dict[str, Any]):
        """
        检查一个媒体项目是否为剧集，如果是，则执行智能追剧判断并添加到待看列表。
        此方法被设计为由外部事件（如Webhook）显式调用。
        """
        item_name_for_log = item_details.get("Name", f"未知项目(ID:{item_details.get('Id')})")
        
        if item_details.get("Type") != "Series":
            # 如果不是剧集，直接返回，不打印非必要的日志
            return

        logger.info(f"Webhook触发：开始为新入库剧集 '{item_name_for_log}' 进行追剧状态判断...")
        try:
            # 实例化 WatchlistProcessor 并执行添加操作
            watchlist_proc = WatchlistProcessor(self.config)
            watchlist_proc.add_series_to_watchlist(item_details)
        except Exception as e_watchlist:
            logger.error(f"在自动添加 '{item_name_for_log}' 到追剧列表时发生错误: {e_watchlist}", exc_info=True)

    def signal_stop(self):
        self._stop_event.set()

    def clear_stop_signal(self):
        self._stop_event.clear()

    def get_stop_event(self) -> threading.Event:
        """返回内部的停止事件对象，以便传递给其他函数。"""
        return self._stop_event

    def is_stop_requested(self) -> bool:
        return self._stop_event.is_set()

    def _load_processed_log_from_db(self) -> Dict[str, str]:
        log_dict = {}
        try:
            # 1. ★★★ 使用 with 语句和中央函数 ★★★
            with get_central_db_connection(self.db_path) as conn:
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

    # ✨ 从 SyncHandler 迁移并改造，用于在本地缓存中查找豆瓣JSON文件
    def _find_local_douban_json(self, imdb_id: Optional[str], douban_id: Optional[str], douban_cache_dir: str) -> Optional[str]:
        """根据 IMDb ID 或 豆瓣 ID 在本地缓存目录中查找对应的豆瓣JSON文件。"""
        if not os.path.exists(douban_cache_dir):
            return None
        
        # 优先使用 IMDb ID 匹配，更准确
        if imdb_id:
            for dirname in os.listdir(douban_cache_dir):
                if dirname.startswith('0_'): continue
                if imdb_id in dirname:
                    dir_path = os.path.join(douban_cache_dir, dirname)
                    for filename in os.listdir(dir_path):
                        if filename.endswith('.json'):
                            return os.path.join(dir_path, filename)
                            
        # 其次使用豆瓣 ID 匹配
        if douban_id:
            for dirname in os.listdir(douban_cache_dir):
                if dirname.startswith(f"{douban_id}_"):
                    dir_path = os.path.join(douban_cache_dir, dirname)
                    for filename in os.listdir(dir_path):
                        if filename.endswith('.json'):
                            return os.path.join(dir_path, filename)
        return None

    # ✨ 封装了“优先本地缓存，失败则在线获取”的逻辑
    def _get_douban_data_with_local_cache(self, media_info: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Optional[float]]:
        """
        【V3 - 最终版】获取豆瓣数据（演员+评分）。优先本地缓存，失败则回退到功能完整的在线API路径。
        返回: (演员列表, 豆瓣评分) 的元组。
        """
        # 1. 准备查找所需的信息
        provider_ids = media_info.get("ProviderIds", {})
        item_name = media_info.get("Name", "")
        imdb_id = provider_ids.get("Imdb")
        douban_id_from_provider = provider_ids.get("Douban")
        item_type = media_info.get("Type")
        item_year = str(media_info.get("ProductionYear", ""))

        # 2. 尝试从本地缓存查找
        douban_cache_dir_name = "douban-movies" if item_type == "Movie" else "douban-tv"
        douban_cache_path = os.path.join(self.local_data_path, "cache", douban_cache_dir_name)
        local_json_path = self._find_local_douban_json(imdb_id, douban_id_from_provider, douban_cache_path)

        if local_json_path:
            logger.debug(f"发现本地豆瓣缓存文件，将直接使用: {local_json_path}")
            douban_data = _read_local_json(local_json_path)
            if douban_data:
                cast = douban_data.get('actors', [])
                rating_str = douban_data.get("rating", {}).get("value")
                rating_float = None
                if rating_str:
                    try: rating_float = float(rating_str)
                    except (ValueError, TypeError): pass
                return cast, rating_float
            else:
                logger.warning(f"本地豆瓣缓存文件 '{local_json_path}' 无效，将回退到在线API。")
        
        # 3. 如果本地未找到，回退到功能完整的在线API路径
        logger.info("未找到本地豆瓣缓存，将通过在线API获取演员和评分信息。")

        # 3.1 匹配豆瓣ID和类型。现在 match_info 返回的结果是完全可信的。
        match_info_result = self.douban_api.match_info(
            name=item_name, imdbid=imdb_id, mtype=item_type, year=item_year
        )

        if match_info_result.get("error") or not match_info_result.get("id"):
            logger.warning(f"在线匹配豆瓣ID失败 for '{item_name}': {match_info_result.get('message', '未找到ID')}")
            return [], None

        douban_id = match_info_result["id"]
        # ✨✨✨ 直接信任从 douban.py 返回的类型 ✨✨✨
        douban_type = match_info_result.get("type")

        if not douban_type:
            logger.error(f"从豆瓣匹配结果中未能获取到媒体类型 for ID {douban_id}。处理中止。")
            return [], None

        # 3.2 获取演职员 (使用完全可信的类型)
        cast_data = self.douban_api.get_acting(
            name=item_name, 
            douban_id_override=douban_id, 
            mtype=douban_type
        )
        douban_cast_raw = cast_data.get("cast", [])

        # 3.3 获取详情（为了评分），同样使用可信的类型
        details_data = self.douban_api._get_subject_details(douban_id, douban_type)
        douban_rating = None
        if details_data and not details_data.get("error"):
            rating_str = details_data.get("rating", {}).get("value")
            if rating_str:
                try:
                    douban_rating = float(rating_str)
                    logger.info(f"在线获取到豆瓣评分 for '{item_name}': {douban_rating}")
                except (ValueError, TypeError):
                    pass

        return douban_cast_raw, douban_rating
    # --- 通过豆瓣ID查找映射表 ---
    def _find_person_in_map_by_douban_id(self, douban_id: str, cursor: sqlite3.Cursor) -> Optional[sqlite3.Row]:
        """
        根据豆瓣名人ID在 person_identity_map 表中查找对应的记录。
        """
        if not douban_id:
            return None
        try:
            cursor.execute(
                "SELECT * FROM person_identity_map WHERE douban_celebrity_id = ?",
                (douban_id,)
            )
            return cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"通过豆瓣ID '{douban_id}' 查询 person_identity_map 时出错: {e}")
            return None
    # --- 通过ImbdID查找映射表 ---
    def _find_person_in_map_by_imdb_id(self, imdb_id: str, cursor: sqlite3.Cursor) -> Optional[sqlite3.Row]:
        """
        根据 IMDb ID 在 person_identity_map 表中查找对应的记录。
        """
        if not imdb_id:
            return None
        try:
            # 核心改动：将查询字段从 douban_celebrity_id 改为 imdb_id
            cursor.execute(
                "SELECT * FROM person_identity_map WHERE imdb_id = ?",
                (imdb_id,)
            )
            return cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"通过 IMDb ID '{imdb_id}' 查询 person_identity_map 时出错: {e}")
            return None
    # --- 补充新增演员额外数据 ---
    def _get_actor_metadata_from_cache(self, tmdb_id: int, cursor: sqlite3.Cursor) -> Optional[Dict]:
        """根据TMDb ID从ActorMetadata缓存表中获取演员的元数据。"""
        if not tmdb_id:
            return None
        cursor.execute("SELECT * FROM ActorMetadata WHERE tmdb_id = ?", (tmdb_id,))
        metadata_row = cursor.fetchone()  # fetchone() 返回一个 sqlite3.Row 对象或 None
        if metadata_row:
            return dict(metadata_row)  # 将其转换为字典，方便使用
        return None
    # --- 核心处理流程 ---
    def _process_cast_list_from_local(self, local_cast_list: List[Dict[str, Any]], douban_cast_list: List[Dict[str, Any]], item_details_from_emby: Dict[str, Any], cursor: sqlite3.Cursor, tmdb_api_key: Optional[str], stop_event: Optional[threading.Event]) -> List[Dict[str, Any]]:
        douban_candidates = actor_utils.format_douban_cast(douban_cast_list)
        
        # --- 步骤 1: 执行一对一匹配 ---
        logger.debug("--- 匹配阶段 1: 执行一对一匹配 ---")

        # 1. 创建一个可被消耗的本地演员列表副本
        unmatched_local_actors = list(local_cast_list)
        # 2. 创建用于存放结果的列表
        merged_actors = []
        unmatched_douban_actors = []

        # 3. 遍历豆瓣演员，尝试在“未匹配”的本地演员中寻找配对
        for d_actor in douban_candidates:
            douban_name_zh = d_actor.get("Name", "").lower().strip()
            douban_name_en = d_actor.get("OriginalName", "").lower().strip()
            
            match_found_for_this_douban_actor = False
            
            # 在【未匹配】的本地演员中寻找第一个同名者
            for i, l_actor in enumerate(unmatched_local_actors):
                local_name = str(l_actor.get("name") or "").lower().strip()
                local_original_name = str(l_actor.get("original_name") or "").lower().strip()

                is_match, match_reason = False, ""
                if douban_name_zh and (douban_name_zh == local_name or douban_name_zh == local_original_name):
                    is_match, match_reason = True, "精确匹配 (豆瓣中文名)"
                elif douban_name_en and (douban_name_en == local_name or douban_name_en == local_original_name):
                    is_match, match_reason = True, "精确匹配 (豆瓣外文名)"

                if is_match:
                    logger.info(f"  匹配成功 (对号入座): 豆瓣演员 '{d_actor.get('Name')}' -> 本地演员 '{l_actor.get('name')}' (ID: {l_actor.get('id')})")
                    
                    # 合并信息
                    l_actor["name"] = d_actor.get("Name")
                    cleaned_douban_character = utils.clean_character_name_static(d_actor.get("Role"))
                    l_actor["character"] = actor_utils.select_best_role(l_actor.get("character"), cleaned_douban_character)
                    if d_actor.get("DoubanCelebrityId"):
                        l_actor["douban_id"] = d_actor.get("DoubanCelebrityId")
                    
                    # 4. 从“未匹配”列表中【移除】这个本地演员，并加入到“已合并”列表
                    merged_actors.append(unmatched_local_actors.pop(i))
                    
                    match_found_for_this_douban_actor = True
                    # 5. 立即中断内层循环，处理下一个豆瓣演员
                    break
            
            # 如果这个豆瓣演员没找到任何匹配，则加入到“未匹配豆瓣演员”列表
            if not match_found_for_this_douban_actor:
                unmatched_douban_actors.append(d_actor)

        # 此时，我们得到三个列表：
        # - merged_actors: 已成功与豆瓣匹配并合并信息的演员
        # - unmatched_local_actors: TMDB有，但豆瓣没有的演员
        # - unmatched_douban_actors: 豆瓣有，但TMDB没有的演员
        
        # 将前两个列表合并，作为我们处理的基础
        current_cast_list = merged_actors + unmatched_local_actors
        # 为了后续方便，我们再创建一个 map
        final_cast_map = {actor['id']: actor for actor in current_cast_list if actor.get('id')}

        # --- 后续处理流程（新增、翻译等）基于新的、干净的数据进行 ---
        
        # (这部分代码与你原有的逻辑基本一致，只是现在它工作在一个正确的数据基础上)
        limit = self.config.get(constants.CONFIG_OPTION_MAX_ACTORS_TO_PROCESS, 30)
        try:
            limit = int(limit)
            if limit <= 0: limit = 30
        except (ValueError, TypeError):
            limit = 30

        current_actor_count = len(final_cast_map)

        if current_actor_count >= limit:
            logger.info(f"当前演员数 ({current_actor_count}) 已达上限 ({limit})，跳过所有新增演员的流程。")
        else:
            logger.info(f"当前演员数 ({current_actor_count}) 低于上限 ({limit})，进入补充模式（处理来自豆瓣的新增演员）。")
            
            logger.debug(f"--- 匹配阶段 2: 用豆瓣ID查 person_identity_map ({len(unmatched_douban_actors)} 位演员) ---")
            still_unmatched = []
            for d_actor in unmatched_douban_actors:
                if self.is_stop_requested(): raise InterruptedError("任务中止")
                d_douban_id = d_actor.get("DoubanCelebrityId")
                match_found = False
                if d_douban_id:
                    entry = self._find_person_in_map_by_douban_id(d_douban_id, cursor)
                    if entry and entry["tmdb_person_id"]:
                        tmdb_id_from_map = entry["tmdb_person_id"]
                        if tmdb_id_from_map not in final_cast_map:
                            logger.info(f"  匹配成功 (通过 豆瓣ID映射): 豆瓣演员 '{d_actor.get('Name')}' -> 加入最终演员表")
                            # ★★★ 1. 查询预先缓存的元数据 ★★★
                            cached_metadata = self._get_actor_metadata_from_cache(tmdb_id_from_map, cursor) or {}

                            # ★★★ 2. 使用缓存数据或默认值来创建新演员条目 ★★★
                            new_actor_entry = {
                                "id": tmdb_id_from_map,
                                "name": d_actor.get("Name"),
                                "original_name": cached_metadata.get("original_name") or d_actor.get("OriginalName"),
                                "character": d_actor.get("Role"),
                                "adult": cached_metadata.get("adult", False),
                                "gender": cached_metadata.get("gender", 0),
                                "known_for_department": "Acting",
                                "popularity": cached_metadata.get("popularity", 0.0),
                                "profile_path": cached_metadata.get("profile_path"), # <-- 关键！
                                "cast_id": None, "credit_id": None, "order": 999,
                                "imdb_id": entry["imdb_id"],
                                "douban_id": d_douban_id,
                                "_is_newly_added": True
                            }
                            final_cast_map[tmdb_id_from_map] = new_actor_entry
                        match_found = True
                if not match_found:
                    still_unmatched.append(d_actor)
            unmatched_douban_actors = still_unmatched

            # --- 步骤 3 & 4: 查询IMDbID -> TMDb反查 -> 新增 ---
            logger.debug(f"--- 匹配阶段 3 & 4: 用IMDb ID进行最终匹配和新增 ({len(unmatched_douban_actors)} 位演员) ---")
            still_unmatched_final = []
            for i, d_actor in enumerate(unmatched_douban_actors):
                if self.is_stop_requested(): raise InterruptedError("任务中止")
                
                # ✨ 核心修改：在每次循环开始时检查上限
                if len(final_cast_map) >= limit:
                    # 现在 'i' 是一个有效的、已定义的变量
                    logger.info(f"演员数已达上限 ({limit})，跳过剩余 {len(unmatched_douban_actors) - i} 位演员的API查询。")
                    # 将所有剩下的演员直接加入 still_unmatched_final
                    still_unmatched_final.extend(unmatched_douban_actors[i:])
                    break # 彻底结束新增流程
                d_douban_id = d_actor.get("DoubanCelebrityId")
                match_found = False
                if d_douban_id and self.douban_api and self.tmdb_api_key:
                    if self.is_stop_requested():
                        logger.info("任务在处理豆瓣演员时被中止 (豆瓣API调用前)。")
                        raise InterruptedError("任务中止")
                    details = self.douban_api.celebrity_details(d_douban_id)
                    time.sleep(0.3)
                    
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
                            logger.warning(f"    -> 解析 IMDb ID 时发生意外错误: {e_parse}")
                    
                    if d_imdb_id:
                        logger.debug(f"    -> 为 '{d_actor.get('Name')}' 获取到 IMDb ID: {d_imdb_id}，开始匹配...")
                        
                        # ✨✨✨ 1. 优先调用新的辅助函数查询本地数据库 ✨✨✨
                        entry_from_map = self._find_person_in_map_by_imdb_id(d_imdb_id, cursor)
                        
                        if entry_from_map and entry_from_map["tmdb_person_id"]:
                            tmdb_id_from_map = str(entry_from_map["tmdb_person_id"])
                            
                            if tmdb_id_from_map not in final_cast_map:
                                logger.info(f"  匹配成功 (通过 IMDb映射): 豆瓣演员 '{d_actor.get('Name')}' -> 加入最终演员表")
                                # ★★★ 查询预先缓存的元数据 ★★★
                                cached_metadata = self._get_actor_metadata_from_cache(tmdb_id_from_map, cursor) or {}

                                # ★★★ 使用缓存数据或默认值来创建 ★★★
                                new_actor_entry = {
                                    "id": tmdb_id_from_map,
                                    "name": d_actor.get("Name"),
                                    "original_name": cached_metadata.get("original_name") or d_actor.get("OriginalName"),
                                    "character": d_actor.get("Role"),
                                    "adult": cached_metadata.get("adult", False),
                                    "gender": cached_metadata.get("gender", 0),
                                    "known_for_department": "Acting",
                                    "popularity": cached_metadata.get("popularity", 0.0),
                                    "profile_path": cached_metadata.get("profile_path"), # <-- 关键！
                                    "cast_id": None, "credit_id": None, "order": 999,
                                    "imdb_id": d_imdb_id,
                                    "douban_id": d_douban_id,
                                    "_is_newly_added": True
                                }
                                final_cast_map[tmdb_id_from_map] = new_actor_entry
                            # ✨✨✨ 立刻反哺这个新发现的映射关系！ ✨✨✨
                            logger.debug(f"    -> [实时反哺] 将新发现的映射关系 (Douban ID: {d_douban_id}) 保存回数据库...")
                            self.actor_db_manager.upsert_person(
                                cursor,
                                {
                                    "tmdb_id": tmdb_id_from_map,
                                    "imdb_id": d_imdb_id,
                                    "douban_id": d_douban_id,
                                    # 传递一个名字以防万一，但ID是关键
                                    "name": d_actor.get("Name") or entry_from_map.get("primary_name")
                                }
                            )
                            # ✨✨✨ 修复结束 ✨✨✨
                            match_found = True

                        # ✨✨✨ 2. 如果数据库未命中，才去调用在线 API ✨✨✨
                        if not match_found:
                            logger.debug(f"    -> 数据库未找到 {d_imdb_id} 的映射，开始通过 TMDb API 反查...")
                            if self.is_stop_requested():
                                logger.info("任务在处理豆瓣演员时被中止 (TMDb API调用前)。")
                                raise InterruptedError("任务中止")
                            
                            person_from_tmdb = tmdb_handler.find_person_by_external_id(d_imdb_id, self.tmdb_api_key, "imdb_id")
                            if person_from_tmdb and person_from_tmdb.get("id"):
                                tmdb_id_from_find = str(person_from_tmdb.get("id"))
                                
                                if tmdb_id_from_find not in final_cast_map:
                                    logger.info(f"  匹配成功 (通过 TMDb反查): 豆瓣演员 '{d_actor.get('Name')}' -> 加入最终演员表")
                                    # ★★★ 1. 再次查询预先缓存的元数据 ★★★
                                    cached_metadata = self._get_actor_metadata_from_cache(tmdb_id_from_find, cursor) or {}

                                    # ★★★ 2. 同样使用缓存数据或默认值来创建 ★★★
                                    new_actor_entry = {
                                        "id": tmdb_id_from_find,
                                        "name": d_actor.get("Name"),
                                        "original_name": cached_metadata.get("original_name") or d_actor.get("OriginalName"),
                                        "character": d_actor.get("Role"),
                                        "adult": cached_metadata.get("adult", False),
                                        "gender": cached_metadata.get("gender", 0),
                                        "known_for_department": "Acting",
                                        "popularity": cached_metadata.get("popularity", 0.0),
                                        "profile_path": cached_metadata.get("profile_path"), # <-- 关键！
                                        "cast_id": None, "credit_id": None, "order": 999,
                                        "imdb_id": d_imdb_id,
                                        "douban_id": d_douban_id,
                                        "_is_newly_added": True
                                    }
                                    final_cast_map[tmdb_id_from_find] = new_actor_entry
                                    
                                    # ✨✨✨ 3. 重要：将新获取的映射关系存回数据库！✨✨✨
                                    self.actor_db_manager.upsert_person(
                                        cursor,
                                        {
                                            "tmdb_id": tmdb_id_from_find,
                                            "imdb_id": d_imdb_id,
                                            "douban_id": d_douban_id,
                                            "name": d_actor.get("Name")
                                        }
                                    )
                                match_found = True
                if not match_found:
                    still_unmatched_final.append(d_actor)

            if still_unmatched_final:
                discarded_names = [d.get('Name') for d in still_unmatched_final]
                logger.info(f"--- 最终丢弃 {len(still_unmatched_final)} 位无匹配的豆瓣演员 ---")


        # ★★★ 在截断前进行一次全量反哺 ★★★
        intermediate_cast_list = list(final_cast_map.values())
        logger.debug(f"截断前：将 {len(intermediate_cast_list)} 位演员的完整映射关系反哺到数据库...")
        for actor_data in intermediate_cast_list:
            self.actor_db_manager.upsert_person(
                cursor,
                {
                    "tmdb_id": actor_data.get("id"),
                    "name": actor_data.get("name"),
                    "imdb_id": actor_data.get("imdb_id"),
                    "douban_id": actor_data.get("douban_id"),
                },
            )
        logger.trace("所有演员的ID映射关系已保存。")
        # 步骤 演员列表截断 (先截断！)
        # ======================================================================
        max_actors = self.config.get(constants.CONFIG_OPTION_MAX_ACTORS_TO_PROCESS, 30)
        try:
            limit = int(max_actors)
            if limit <= 0: limit = 30
        except (ValueError, TypeError):
            limit = 30
        
        original_count = len(intermediate_cast_list)
        if original_count > limit:
            logger.info(f"演员列表总数 ({original_count}) 超过上限 ({limit})，将在翻译前进行截断。")
            intermediate_cast_list.sort(key=lambda x: x.get('order') if x.get('order') is not None and x.get('order') >= 0 else 999)
            cast_to_process = intermediate_cast_list[:limit]
        else:
            cast_to_process = intermediate_cast_list
        
        logger.info(f"将对 {len(cast_to_process)} 位演员进行最终的翻译和格式化处理...")

        # ======================================================================
        # 步骤 B: 翻译准备与执行 (后收集，并检查缓存！)
        # ======================================================================
        ai_translation_succeeded = False
        translation_cache = {} # ★★★ 核心修正1：将缓存初始化在最外面

        if self.ai_translator and self.config.get(constants.CONFIG_OPTION_AI_TRANSLATION_ENABLED, False):
            logger.info("AI翻译已启用，优先尝试批量翻译模式。")
            
            try:
                translation_mode = self.config.get(constants.CONFIG_OPTION_AI_TRANSLATION_MODE, "fast")
                
                texts_to_collect = set()
                for actor in cast_to_process:
                    name = actor.get('name')
                    if name and not utils.contains_chinese(name):
                        texts_to_collect.add(name)
                    
                    character = actor.get('character')
                    if character:
                        cleaned_character = utils.clean_character_name_static(character)
                        if cleaned_character and not utils.contains_chinese(cleaned_character):
                            texts_to_collect.add(cleaned_character)
                
                texts_to_send_to_api = set()

                if translation_mode == 'fast':
                    logger.debug("[翻译模式] 正在检查全局翻译缓存...")
                    for text in texts_to_collect:
                        cached_entry = self.actor_db_manager.get_translation_from_db(cursor=cursor, text=text)
                        if cached_entry:
                            translation_cache[text] = cached_entry.get("translated_text")
                        else:
                            texts_to_send_to_api.add(text)
                else:
                    logger.debug("[顾问模式] 跳过缓存检查，直接翻译所有词条。")
                    texts_to_send_to_api = texts_to_collect

                if texts_to_send_to_api:
                    item_title = item_details_from_emby.get("Name")
                    item_year = item_details_from_emby.get("ProductionYear")
                    
                    logger.info(f"将 {len(texts_to_send_to_api)} 个词条提交给AI (模式: {translation_mode})。")
                    
                    translation_map_from_api = self.ai_translator.batch_translate(
                        texts=list(texts_to_send_to_api),
                        mode=translation_mode,
                        title=item_title,
                        year=item_year
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
                
                # 无论API是否被调用，只要这个流程没出错，就认为AI部分成功了
                # （即使只是成功使用了缓存或确认了无需翻译）
                ai_translation_succeeded = True

            except Exception as e:
                logger.error(f"调用AI批量翻译时发生严重错误: {e}", exc_info=True)
                ai_translation_succeeded = False

        # --- ★★★ 核心修正2：无论AI是否成功，都执行清理与回填，降级逻辑只在AI失败时触发 ★★★
        
        if ai_translation_succeeded:
            logger.info("------------ AI翻译流程成功，开始应用结果 ------------")
            
            # 优化日志：根据实际情况报告翻译来源
            if not texts_to_collect:
                logger.info("  所有演员名和角色名均已是中文，无需翻译。")
            elif not texts_to_send_to_api:
                logger.info(f"  所有 {len(texts_to_collect)} 个待翻译词条均从数据库缓存中获取，无需调用AI。")
            else:
                logger.info(f"  AI翻译完成，共处理 {len(translation_cache)} 个词条。")

            # 【修正核心】: 无条件执行回填，因为只要ai_translation_succeeded=True，
            # translation_cache 就包含了所有需要的数据（来自缓存或API）。
            for actor in cast_to_process:
                # 1. 处理演员名
                original_name = actor.get('name')
                # 使用 .get(key, default_value) 来安全地处理，如果没在缓存里，就用原文
                translated_name = translation_cache.get(original_name, original_name)
                if original_name != translated_name:
                    logger.debug(f"  演员名翻译: '{original_name}' -> '{translated_name}'")
                actor['name'] = translated_name

                # 2. 处理角色名
                original_character = actor.get('character')
                if original_character:
                    # 角色名总是先清理，再从缓存中查找翻译
                    cleaned_character = utils.clean_character_name_static(original_character)
                    translated_character = translation_cache.get(cleaned_character, cleaned_character)
                    
                    if translated_character != original_character:
                        actor_name_for_log = actor.get('name', '未知演员')
                        logger.debug(f"  角色名翻译: '{original_character}' -> '{translated_character}' (演员: {actor_name_for_log})")
                    
                    actor['character'] = translated_character
                    
            logger.info("----------------------------------------------------")
        else:
            # AI翻译未启用或执行失败，启动降级程序
            if self.config.get(constants.CONFIG_OPTION_AI_TRANSLATION_ENABLED, False):
                logger.warning("AI批量翻译失败，启动降级程序，将使用传统翻译引擎逐个翻译...")
            else:
                logger.info("AI翻译未启用，使用传统翻译引擎（如果配置了）。")

            translator_engines_order = self.config.get("translator_engines_order", [])
            
            for actor in cast_to_process:
                if self.is_stop_requested():
                    raise InterruptedError("任务在翻译演员列表时被中止")
                
                # 翻译演员名
                cleaned_name = actor.get('name')
                actor['name'] = actor_utils.translate_actor_field(
                    text=cleaned_name,
                    db_manager=self.actor_db_manager,
                    db_cursor=cursor,
                    translator_engines=translator_engines_order,
                    # ★★★ 核心修正：在降级路径中，明确禁用AI ★★★
                    ai_translator=None,  # 传递None来关闭AI
                    ai_enabled=False     # 传递False来关闭AI
                )
                
                # 翻译角色名
                cleaned_character = utils.clean_character_name_static(actor.get('character'))
                if cleaned_character:
                    actor['character'] = actor_utils.translate_actor_field(
                        text=cleaned_character,
                        db_cursor=cursor,
                        db_manager=self.actor_db_manager,
                        translator_engines=translator_engines_order,
                        # ★★★ 核心修正：同样在此处禁用AI ★★★
                        ai_translator=None,
                        ai_enabled=False
                    )

        # 返回处理完的、已经截断和翻译的列表
        return cast_to_process
        
    def _process_item_core_logic(self, item_details_from_emby: Dict[str, Any], force_reprocess_this_item: bool, should_process_episodes_this_run: bool, force_fetch_from_tmdb: bool):
        """
        【V-Rebuild - 重建版】
        在一个统一的数据库事务中，执行所有处理。
        核心逻辑已改为：获取数据 -> 处理演员 -> 从零构建JSON。
        """
        item_id = item_details_from_emby.get("Id")
        item_name_for_log = item_details_from_emby.get("Name", f"未知项目(ID:{item_id})")
        tmdb_id = item_details_from_emby.get("ProviderIds", {}).get("Tmdb")
        item_type = item_details_from_emby.get("Type")
        
        if not tmdb_id:
            logger.error(f"项目 '{item_name_for_log}' 缺少 TMDb ID，无法处理。")
            return False
        if not self.tmdb_api_key:
            logger.error("未配置TMDb API Key，无法从在线获取数据。")
            return False

        # ★★★ 1. 提前定义评分逻辑所需的变量 ★★★
        genres = item_details_from_emby.get("Genres", [])
        is_animation = "Animation" in genres or "动画" in genres or "Documentary" in genres or "纪录" in genres
        original_emby_actor_count = len(item_details_from_emby.get("People", []))

        log_prefix = f"[{'在线模式' if force_fetch_from_tmdb else '本地优先'}]"
        logger.info(f"{log_prefix} 开始处理 '{item_name_for_log}' (TMDb ID: {tmdb_id})")

        # --- 路径定义 ---
        cache_folder_name = "tmdb-movies2" if item_type == "Movie" else "tmdb-tv"
        base_cache_dir = os.path.join(self.local_data_path, "cache", cache_folder_name, tmdb_id)
        base_override_dir = os.path.join(self.local_data_path, "override", cache_folder_name, tmdb_id)
        os.makedirs(base_override_dir, exist_ok=True)

        try:
            # ======================================================================
            # 阶段 1: 数据采集 (Data Acquisition)
            # ======================================================================
            main_tmdb_data = {}
            seasons_data = []
            episodes_data = []

            # 检查是否需要从TMDB API强制获取
            is_fetch_needed = force_fetch_from_tmdb
            if not is_fetch_needed:
                # 如果不是强制获取，检查本地缓存是否存在
                main_json_filename = "all.json" if item_type == "Movie" else "series.json"
                if not os.path.exists(os.path.join(base_cache_dir, main_json_filename)):
                    logger.info(f"本地主缓存文件不存在，将触发在线获取。")
                    is_fetch_needed = True
            
            if is_fetch_needed:
                logger.info(f"正在从 TMDb API 获取 '{item_name_for_log}' 的完整元数据...")
                if item_type == "Movie":
                    main_tmdb_data = tmdb_handler.get_movie_details(tmdb_id, self.tmdb_api_key)
                    if not main_tmdb_data:
                        raise ValueError(f"从TMDb获取电影 {tmdb_id} 详情失败。")
                
                elif item_type == "Series":
                    main_tmdb_data = tmdb_handler.get_tv_details_tmdb(tmdb_id, self.tmdb_api_key)
                    if not main_tmdb_data:
                        raise ValueError(f"从TMDb获取剧集 {tmdb_id} 详情失败。")

                    # ======================================================================
                    # ★★★★★★★★★★★★★★★★★ 核心修正区域 V2 START ★★★★★★★★★★★★★★★★★
                    # ======================================================================
                    
                    # --- 定义请求间隔和并发数 ---
                    REQUEST_DELAY = 0.05  # 每次API请求提交后的延时（秒）
                    MAX_WORKERS = 5       # 最大并发线程数

                    # --- 阶段 A: 并发获取所有季的数据 ---
                    seasons_to_fetch = main_tmdb_data.get("seasons", [])
                    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                        future_to_season_num = {}
                        for s in seasons_to_fetch:
                            season_number = s.get("season_number")
                            if season_number is not None:
                                future = executor.submit(tmdb_handler.get_season_details_tmdb, tmdb_id, season_number, self.tmdb_api_key)
                                future_to_season_num[future] = season_number
                                time.sleep(REQUEST_DELAY) # 【【【 新增：请求延时 】】】
                        
                        logger.info(f"已提交 {len(future_to_season_num)} 个季获取任务，等待完成...")
                        for future in concurrent.futures.as_completed(future_to_season_num):
                            season_number = future_to_season_num[future]
                            try:
                                data = future.result()
                                if data:
                                    seasons_data.append(data)
                            except Exception as exc:
                                logger.error(f"获取季 {season_number} 详情时出错: {exc}", exc_info=False)
                    
                    # --- 阶段 B: 并发获取所有集的数据 ---
                    if seasons_data:
                        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                            future_to_episode_info = {}
                            for season_detail in seasons_data:
                                for ep_stub in season_detail.get('episodes', []):
                                    season_number = season_detail.get("season_number")
                                    episode_number = ep_stub.get("episode_number")
                                    if season_number is not None and episode_number is not None:
                                        future = executor.submit(
                                            tmdb_handler.get_episode_details_tmdb,
                                            tmdb_id, season_number, episode_number, self.tmdb_api_key
                                        )
                                        future_to_episode_info[future] = (season_number, episode_number)
                                        time.sleep(REQUEST_DELAY) # 【【【 新增：请求延时 】】】
                            
                            if future_to_episode_info:
                                logger.info(f"已提交 {len(future_to_episode_info)} 个分集获取任务，等待完成...")
                                for future in concurrent.futures.as_completed(future_to_episode_info):
                                    s_num, e_num = future_to_episode_info[future]
                                    try:
                                        ep_data = future.result()
                                        if ep_data:
                                            episodes_data.append(ep_data)
                                    except Exception as exc:
                                        logger.error(f'获取 S{s_num}E{e_num} 详情时出错: {exc}', exc_info=False)

            else: # 使用本地缓存
                logger.info(f"正在从本地缓存加载 '{item_name_for_log}' 的元数据...")
                main_json_filename = "all.json" if item_type == "Movie" else "series.json"
                main_tmdb_data = _read_local_json(os.path.join(base_cache_dir, main_json_filename))
                if not main_tmdb_data:
                    raise ValueError(f"读取本地主缓存文件失败: {os.path.join(base_cache_dir, main_json_filename)}")

                if item_type == "Series":
                    for filename in os.listdir(base_cache_dir):
                        if filename.startswith("season-") and filename.endswith(".json"):
                            if "episode" in filename:
                                episodes_data.append(_read_local_json(os.path.join(base_cache_dir, filename)))
                            else:
                                seasons_data.append(_read_local_json(os.path.join(base_cache_dir, filename)))

            # ======================================================================
            # 阶段 2: 演员处理 (Actor Processing)
            # ======================================================================
            with get_central_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                
                # --- 获取豆瓣数据 ---
                douban_cast_raw, douban_rating = self._get_douban_data_with_local_cache(item_details_from_emby)
                if douban_rating is not None:
                    logger.info(f"✅ 发现豆瓣评分: {douban_rating}，将替换TMDb评分。")
                    main_tmdb_data['vote_average'] = douban_rating

                # --- 聚合原始演员表 ---
                initial_cast_list = []
                if item_type == "Movie":
                    initial_cast_list = main_tmdb_data.get("casts", {}).get("cast", [])
                elif item_type == "Series":
                    initial_cast_list = _aggregate_series_cast_from_tmdb_data(main_tmdb_data, episodes_data)

                # --- 执行核心的演员处理、匹配和翻译逻辑 ---
                intermediate_cast = self._process_cast_list_from_local(
                    local_cast_list=initial_cast_list,
                    douban_cast_list=douban_cast_raw,
                    item_details_from_emby=item_details_from_emby,
                    cursor=cursor,
                    tmdb_api_key=self.tmdb_api_key,
                    stop_event=self.get_stop_event()
                )

                # --- 最终格式化 ---
                genres = item_details_from_emby.get("Genres", [])
                is_animation = "Animation" in genres or "动画" in genres or "Documentary" in genres or "纪录" in genres
                final_cast_perfect = actor_utils.format_and_complete_cast_list(
                    intermediate_cast, is_animation, self.config, mode='auto'
                )
                
                # 提取演职员表
                final_crew = []
                if item_type == "Movie":
                    final_crew = main_tmdb_data.get("casts", {}).get("crew", [])
                elif item_type == "Series":
                    # 剧集的crew通常在主文件里，分集文件里的crew意义不大
                    final_crew = main_tmdb_data.get("credits", {}).get("crew", [])


                # ======================================================================
                # 阶段 3: JSON 构建与写入 (Build & Write)
                # ======================================================================
                logger.info(f"处理完成，开始根据模板生成覆盖JSON文件...")

                # --- 安全写入函数 ---
                def safe_write_json(data: Dict, path: str):
                    temp_path = f"{path}.{random.randint(1000, 9999)}.tmp"
                    try:
                        with open(temp_path, 'w', encoding='utf-8') as f:
                            json.dump(data, f, ensure_ascii=False, indent=4)
                        os.replace(temp_path, path)
                        logger.trace(f"✅ 成功生成覆盖文件: {path}")
                    except Exception as e:
                        logger.error(f"写入文件 {path} 时发生错误: {e}", exc_info=True)
                        if os.path.exists(temp_path): os.remove(temp_path)
                        raise

                if item_type == "Movie":
                    movie_override_json = _build_movie_json(main_tmdb_data, final_cast_perfect, final_crew)
                    override_path = os.path.join(base_override_dir, "all.json")
                    safe_write_json(movie_override_json, override_path)
                
                elif item_type == "Series":
                    # 写入主剧集文件
                    series_override_json = _build_series_json(main_tmdb_data, final_cast_perfect)
                    override_path = os.path.join(base_override_dir, "series.json")
                    safe_write_json(series_override_json, override_path)

                    # 写入季文件
                    for season_data in seasons_data:
                        season_number = season_data.get("season_number")
                        season_crew = season_data.get("credits", {}).get("crew", [])
                        season_override_json = _build_season_json(season_data, final_cast_perfect, season_crew)
                        override_path = os.path.join(base_override_dir, f"season-{season_number}.json")
                        safe_write_json(season_override_json, override_path)

                    # 写入分集文件
                    if should_process_episodes_this_run:
                        for episode_data in episodes_data:
                            season_number = episode_data.get("season_number")
                            episode_number = episode_data.get("episode_number")
                            episode_crew = episode_data.get("credits", {}).get("crew", [])
                            episode_override_json = _build_episode_json(episode_data, final_cast_perfect, episode_crew)
                            override_path = os.path.join(base_override_dir, f"season-{season_number}-episode-{episode_number}.json")
                            safe_write_json(episode_override_json, override_path)

                # ======================================================================
                # 阶段 4: 后续处理 (Post-processing)
                # ======================================================================
                _save_metadata_to_cache(
                    cursor=cursor,
                    tmdb_id=tmdb_id,
                    item_type=item_type,
                    item_details=item_details_from_emby,
                    processed_cast=final_cast_perfect,
                    raw_tmdb_json=main_tmdb_data
                )
                
                if self.sync_images_enabled:
                    if self.is_stop_requested(): raise InterruptedError("任务被中止")
                    self.sync_item_images(item_details_from_emby)
                    
                # --- 刷新与日志 ---
                final_actor_count = len(final_cast_perfect)
                logger.info(f"✨✨✨处理统计 '{item_name_for_log}'✨✨✨")
                logger.info(f"  - Emby原有演员: {original_emby_actor_count} 位")
                newly_added_count = final_actor_count - original_emby_actor_count
                if newly_added_count > 0:
                    logger.info(f"  - 新增演员: {newly_added_count} 位") 
                logger.info(f"  - 最终演员: {final_actor_count} 位")

                if self.is_stop_requested(): raise InterruptedError("任务被中止")

                processing_score = actor_utils.evaluate_cast_processing_quality(
                    final_cast=final_cast_perfect,
                    original_cast_count=original_emby_actor_count,
                    expected_final_count=final_actor_count,
                    is_animation=is_animation
                )
                min_score_for_review = float(self.config.get("min_score_for_review", constants.DEFAULT_MIN_SCORE_FOR_REVIEW))
                refresh_success = emby_handler.refresh_emby_item_metadata(
                    item_emby_id=item_id,
                    emby_server_url=self.emby_url,
                    emby_api_key=self.emby_api_key,
                    replace_all_metadata_param=True, 
                    item_name_for_log=item_name_for_log,
                    user_id_for_unlock=self.emby_user_id
                )
                if not refresh_success:
                    raise RuntimeError("触发Emby刷新失败")
                
                self.log_db_manager.save_to_processed_log(cursor, item_id, item_name_for_log, score=10.0) # 重建模式默认给高分
                self.log_db_manager.remove_from_failed_log(cursor, item_id)
                
                logger.info(f"✨✨✨处理完成 '{item_name_for_log}'✨✨✨")
                return True

        except (ValueError, InterruptedError) as e:
            logger.warning(f"处理 '{item_name_for_log}' 的过程中断: {e}")
            return False
        except Exception as outer_e:
            logger.error(f"核心处理流程中发生未知严重错误 for '{item_name_for_log}': {outer_e}", exc_info=True)
            try:
                with get_central_db_connection(self.db_path) as conn_fail:
                    self.log_db_manager.save_to_failed_log(conn_fail.cursor(), item_id, item_name_for_log, f"核心处理异常: {str(outer_e)}", item_type)
            except Exception as log_e:
                logger.error(f"写入失败日志时再次发生错误: {log_e}")
            return False

    def process_single_item(self, emby_item_id: str, force_reprocess_this_item: bool = False, process_episodes: Optional[bool] = None, force_fetch_from_tmdb: bool = False):
        """
        【已升级】此函数现在能接收 force_fetch_from_tmdb 标志。
        它负责准备所有信息，并传递给核心处理函数。
        """
        if self.is_stop_requested(): 
            return False

        item_details = emby_handler.get_emby_item_details(emby_item_id, self.emby_url, self.emby_api_key, self.emby_user_id)
        if not item_details:
            # 注意：这里不能直接调用 self.save_to_failed_log，因为它需要一个 cursor
            # 更好的做法是在调用者那里处理这个失败
            logger.error(f"process_single_item: 无法获取 Emby 项目 {emby_item_id} 的详情。")
            return False
        
        # 1. 决定本次运行是否应该处理分集
        if process_episodes is None:
            # 如果调用者没有明确指定，则遵循全局配置
            should_process_episodes_this_run = self.config.get(constants.CONFIG_OPTION_PROCESS_EPISODES, False)
        else:
            # 如果调用者明确指定了，就听从本次调用的指令
            should_process_episodes_this_run = process_episodes

        # 2. 将所有信息和决策，作为参数传递给核心处理函数
        return self._process_item_core_logic(
            item_details, 
            force_reprocess_this_item,
            should_process_episodes_this_run,
            force_fetch_from_tmdb=force_fetch_from_tmdb  # <--- 把新命令传递下去
        )

    def process_full_library(self, update_status_callback: Optional[callable] = None, process_episodes: bool = True, force_reprocess_all: bool = False, force_fetch_from_tmdb: bool = False):
        """
        【V3 - 最终完整版】
        这是所有全量扫描的唯一入口，它自己处理所有与“强制”相关的逻辑。
        """
        self.clear_stop_signal()
        
        logger.info(f"进入核心执行层: process_full_library, 接收到的 force_reprocess_all = {force_reprocess_all}, force_fetch_from_tmdb = {force_fetch_from_tmdb}")

        if force_reprocess_all:
            logger.info("检测到“强制重处理”选项，正在清空已处理日志...")
            try:
                self.clear_processed_log()
            except Exception as e:
                logger.error(f"在 process_full_library 中清空日志失败: {e}", exc_info=True)
                if update_status_callback: update_status_callback(-1, "清空日志失败")
                return

        # --- ★★★ 补全了这部分代码 ★★★ ---
        libs_to_process_ids = self.config.get("libraries_to_process", [])
        if not libs_to_process_ids:
            logger.warning("未在配置中指定要处理的媒体库。")
            return

        logger.info("正在尝试从Emby获取媒体项目...")
        all_emby_libraries = emby_handler.get_emby_libraries(self.emby_url, self.emby_api_key, self.emby_user_id) or []
        library_name_map = {lib.get('Id'): lib.get('Name', '未知库名') for lib in all_emby_libraries}
        
        movies = emby_handler.get_emby_library_items(self.emby_url, self.emby_api_key, "Movie", self.emby_user_id, libs_to_process_ids, library_name_map=library_name_map) or []
        series = emby_handler.get_emby_library_items(self.emby_url, self.emby_api_key, "Series", self.emby_user_id, libs_to_process_ids, library_name_map=library_name_map) or []
        
        if movies:
            source_movie_lib_names = sorted(list({library_name_map.get(item.get('_SourceLibraryId')) for item in movies if item.get('_SourceLibraryId')}))
            logger.info(f"从媒体库【{', '.join(source_movie_lib_names)}】获取到 {len(movies)} 个电影项目。")

        if series:
            source_series_lib_names = sorted(list({library_name_map.get(item.get('_SourceLibraryId')) for item in series if item.get('_SourceLibraryId')}))
            logger.info(f"从媒体库【{', '.join(source_series_lib_names)}】获取到 {len(series)} 个电视剧项目。")

        all_items = movies + series
        total = len(all_items)
        # --- ★★★ 补全结束 ★★★ ---
        
        if total == 0:
            logger.info("在所有选定的库中未找到任何可处理的项目。")
            if update_status_callback: update_status_callback(100, "未找到可处理的项目。")
            return

        for i, item in enumerate(all_items):
            if self.is_stop_requested(): break
            
            item_id = item.get('Id')
            item_name = item.get('Name', f"ID:{item_id}")

            if not force_reprocess_all and item_id in self.processed_items_cache:
                logger.info(f"正在跳过已处理的项目: {item_name}")
                if update_status_callback:
                    update_status_callback(int(((i + 1) / total) * 100), f"跳过: {item_name}")
                continue

            if update_status_callback:
                update_status_callback(int(((i + 1) / total) * 100), f"处理中 ({i+1}/{total}): {item_name}")
            
            self.process_single_item(
                item_id, 
                process_episodes=process_episodes,
                force_reprocess_this_item=force_reprocess_all,
                force_fetch_from_tmdb=force_fetch_from_tmdb
            )
            
            time.sleep(float(self.config.get("delay_between_items_sec", 0.5)))
        
        if not self.is_stop_requested() and update_status_callback:
            update_status_callback(100, "全量处理完成")
    # --- 一键翻译 ---
    def translate_cast_list_for_editing(self, 
                                    cast_list: List[Dict[str, Any]], 
                                    title: Optional[str] = None, 
                                    year: Optional[int] = None,
                                    tmdb_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        【V13 - 返璞归真双核版】为手动编辑页面提供的一键翻译功能。
        根据用户配置，智能选择带全局缓存的翻译模式，或无缓存的顾问模式。
        """
        if not cast_list:
            return []
            
        # 从配置中读取模式，这是决定后续所有行为的总开关
        translation_mode = self.config.get(constants.CONFIG_OPTION_AI_TRANSLATION_MODE, "fast")
        
        context_log = f" (上下文: {title} {year})" if title and translation_mode == 'quality' else ""
        logger.info(f"手动编辑-一键翻译：开始批量处理 {len(cast_list)} 位演员 (模式: {translation_mode}){context_log}。")
        
        translated_cast = [dict(actor) for actor in cast_list]
        
        # --- 批量翻译逻辑 ---
        ai_translation_succeeded = False
        
        if self.ai_translator and self.config.get(constants.CONFIG_OPTION_AI_TRANSLATION_ENABLED, False):
            with get_central_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                
                translation_cache = {} # 本次运行的内存缓存
                texts_to_translate = set()

                # 1. 收集所有需要翻译的词条
                texts_to_collect = set()
                for actor in translated_cast:
                    for field_key in ['name', 'role']:
                        text = actor.get(field_key, '').strip()
                        if field_key == 'role':
                            # 无论是演员名还是角色名，都先清洗一遍，确保拿到的是核心文本
                            # 对于演员名，这个清洗通常无影响，但对于角色名至关重要
                            text = utils.clean_character_name_static(text)
                        if text and not utils.contains_chinese(text):
                            texts_to_collect.add(text)

                # 2. 根据模式决定是否使用缓存
                if translation_mode == 'fast':
                    logger.debug("[翻译模式] 正在检查全局翻译缓存...")
                    for text in texts_to_collect:
                        # 翻译模式只读写全局缓存
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
                    try:
                        translation_map_from_api = self.ai_translator.batch_translate(
                            texts=list(texts_to_translate),
                            mode=translation_mode,
                            title=title,
                            year=year
                        )
                        if translation_map_from_api:
                            translation_cache.update(translation_map_from_api)
                            
                            # 只有在翻译模式下，才将结果写入全局缓存
                            if translation_mode == 'fast':
                                for original, translated in translation_map_from_api.items():
                                    self.actor_db_manager.save_translation_to_db(
                                        cursor=cursor,
                                        original_text=original, 
                                        translated_text=translated, 
                                        engine_used=self.ai_translator.provider
                                    )
                            
                            ai_translation_succeeded = True
                        else:
                            logger.warning("手动编辑-翻译：AI批量翻译未返回结果。")
                    except Exception as e:
                        logger.error(f"手动编辑-翻译：调用AI批量翻译时出错: {e}", exc_info=True)
                else:
                    logger.info("手动编辑-翻译：所有词条均在缓存中找到，无需调用API。")
                    ai_translation_succeeded = True

                # 4. 回填所有翻译结果
                if translation_cache:
                    for i, actor in enumerate(translated_cast):
                        original_name = actor.get('name', '').strip()
                        if original_name in translation_cache:
                            translated_cast[i]['name'] = translation_cache[original_name]
                        
                        original_role_raw = actor.get('role', '').strip()
                        # 使用与收集时完全相同的清理逻辑
                        cleaned_original_role = utils.clean_character_name_static(original_role_raw)
                        
                        # 用清理后的名字作为key去查找
                        if cleaned_original_role in translation_cache:
                            translated_cast[i]['role'] = translation_cache[cleaned_original_role]
                        
                        # 如果发生了翻译，更新状态以便前端高亮
                        if translated_cast[i].get('name') != actor.get('name') or translated_cast[i].get('role') != actor.get('role'):
                            translated_cast[i]['matchStatus'] = '已翻译'
        
        # 如果AI翻译未启用或失败，则降级到传统引擎
        if not ai_translation_succeeded:
            if self.config.get("ai_translation_enabled", False):
                logger.info("手动编辑-翻译：AI翻译失败，降级到传统引擎逐个翻译。")
            else:
                logger.info("手动编辑-翻译：AI未启用，使用传统引擎逐个翻译。")
                
            try:
                with get_central_db_connection(self.db_path) as conn:
                    cursor = conn.cursor()

                    for i, actor in enumerate(translated_cast):
                        if self.is_stop_requested():
                            logger.warning(f"一键翻译（降级模式）被用户中止。")
                            break # 这里使用 break 更安全，可以直接跳出循环
                        # 【【【 修复点 3：使用正确的参数调用 translate_actor_field 】】】
                        
                        # 翻译演员名
                        name_to_translate = actor.get('name', '').strip()
                        if name_to_translate and not utils.contains_chinese(name_to_translate):
                            translated_name = actor_utils.translate_actor_field(
                                text=name_to_translate,
                                db_manager=self.actor_db_manager,
                                db_cursor=cursor,
                                ai_translator=self.ai_translator,
                                translator_engines=self.translator_engines,
                                ai_enabled=self.ai_enabled
                            )
                            if translated_name and translated_name != name_to_translate:
                                translated_cast[i]['name'] = translated_name

                        # 翻译角色名
                        role_to_translate = actor.get('role', '').strip()
                        if role_to_translate and not utils.contains_chinese(role_to_translate):
                            translated_role = actor_utils.translate_actor_field(
                                text=role_to_translate,
                                db_manager=self.actor_db_manager,
                                db_cursor=cursor,
                                ai_translator=self.ai_translator,
                                translator_engines=self.translator_engines,
                                ai_enabled=self.ai_enabled
                            )
                            if translated_role and translated_role != role_to_translate:
                                translated_cast[i]['role'] = translated_role

                        if translated_cast[i].get('name') != actor.get('name') or translated_cast[i].get('role') != actor.get('role'):
                            translated_cast[i]['matchStatus'] = '已翻译'
            
            except Exception as e:
                logger.error(f"一键翻译（降级模式）时发生错误: {e}", exc_info=True)

        logger.info("手动编辑-翻译完成。")
        return translated_cast
    # ✨✨✨手动处理✨✨✨
    def process_item_with_manual_cast(self, item_id: str, manual_cast_list: List[Dict[str, Any]], item_name: str) -> bool:
        """
        【V5 - 格式化增强版】使用前端提交的轻量级修改，与内存中的完整数据合并，并应用最终的格式化步骤。
        """
        logger.info(f"手动处理流程启动 (后端缓存模式)：ItemID: {item_id} ('{item_name}')")
        try:
            # ✨✨✨ 1. 使用 with 语句，在所有操作开始前获取数据库连接 ✨✨✨
            with get_central_db_connection(self.db_path) as conn:
                cursor = conn.cursor()

                # ✨✨✨ 2. 手动开启一个事务 ✨✨✨
                cursor.execute("BEGIN TRANSACTION;")
                logger.debug(f"手动处理 (ItemID: {item_id}) 的数据库事务已开启。")
            
                try:
                    # ★★★ 1. 从内存缓存中获取这个会话的完整原始演员列表 ★★★
                    original_full_cast = self.manual_edit_cache.get(item_id)
                    if not original_full_cast:
                        raise ValueError(f"在内存缓存中找不到 ItemID {item_id} 的原始演员数据。请重新进入编辑页面。")

                    # 2. 获取基础信息
                    item_details = emby_handler.get_emby_item_details(item_id, self.emby_url, self.emby_api_key, self.emby_user_id)
                    if not item_details: raise ValueError(f"无法获取项目 {item_id} 的详情。")
                    tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
                    item_type = item_details.get("Type")
                    if not tmdb_id: raise ValueError(f"项目 {item_id} 缺少 TMDb ID。")

                    # 3. 构建一个以 TMDb ID 为键的原始数据映射表，方便查找
                    reliable_cast_map = {str(actor['id']): actor for actor in original_full_cast if actor.get('id')}

                    # 4. 遍历前端传来的轻量级列表，安全地合并修改，生成一个中间状态的列表
                    intermediate_cast = []
                    for actor_from_frontend in manual_cast_list:
                        frontend_tmdb_id = actor_from_frontend.get("tmdbId")
                        if not frontend_tmdb_id: continue

                        original_actor_data = reliable_cast_map.get(str(frontend_tmdb_id))
                        if not original_actor_data:
                            logger.warning(f"在原始缓存中找不到 TMDb ID {frontend_tmdb_id}，跳过此演员。")
                            continue
                        # 3.1 获取并清理原始角色名和新角色名
                        new_role = actor_from_frontend.get('role', '')      # 从前端数据中获取新角色名
                        original_role = original_actor_data.get('character', '') # 从原始数据中获取旧角色名
                        if new_role != original_role:
                            # 清洗新旧角色名
                            cleaned_original_role = utils.clean_character_name_static(original_role)
                            cleaned_new_role = utils.clean_character_name_static(new_role)

                            # 只有在清洗后的新角色名有效，且与清洗后的旧角色名确实不同时，才进行操作
                            if cleaned_new_role and cleaned_new_role != cleaned_original_role:
                                try:
                                    # 使用“修改前的中文名”（例如 "杰克萨利"）进行反向查找
                                    cache_entry = self.actor_db_manager.get_translation_from_db(
                                        text=cleaned_original_role,
                                        by_translated_text=True,  # <--- 关键！开启反查模式
                                        cursor=cursor
                                    )

                                    # 函数会返回一个包含原文和译文的字典，或者 None
                                    if cache_entry and 'original_text' in cache_entry:
                                        # 如果成功找到了缓存记录，就从中提取出原始的英文 Key
                                        original_text_key = cache_entry['original_text']

                                        # 现在，我们用正确的 Key ("Jake Sully") 和新的 Value ("杰克") 去更新缓存
                                        self.actor_db_manager.save_translation_to_db(
                                            cursor=cursor, # <--- ★★★ 将 cursor 放在正确的位置 ★★★
                                            original_text=original_text_key,
                                            translated_text=cleaned_new_role,
                                            engine_used="manual"
                                        )
                                        logger.debug(f"  AI缓存通过反查更新: '{original_text_key}' -> '{cleaned_new_role}'")
                                        cache_update_succeeded = True
                                    else:
                                        # 如果反查失败（未找到或有其他问题），记录日志并继续
                                        logger.warning(f"无法为修改 '{cleaned_original_role}' -> '{cleaned_new_role}' 更新缓存，因为在缓存中未找到其对应的原文。")

                                except Exception as e_cache:
                                    logger.warning(f"更新AI翻译缓存失败: {e_cache}")
                        
                        updated_actor_data = copy.deepcopy(original_actor_data)
                        role_from_frontend = actor_from_frontend.get('role')
                        cleaned_role = utils.clean_character_name_static(role_from_frontend)
                        
                        updated_actor_data['name'] = actor_from_frontend.get('name')
                        updated_actor_data['character'] = cleaned_role
                        
                        intermediate_cast.append(updated_actor_data)

                    # =================================================================
                    # ★★★ 4.5.【【【 新增核心步骤 】】】★★★
                    #      应用与自动处理流程完全相同的最终格式化和补全逻辑
                    # =================================================================
                    genres = item_details.get("Genres", [])
                    is_animation = "Animation" in genres or "动画" in genres or "Documentary" in genres or "纪录" in genres
                    
                    logger.debug("正在对合并后的演员列表应用最终的格式化、补全和排序...")
                    final_cast_perfect = actor_utils.format_and_complete_cast_list(
                        intermediate_cast, 
                        is_animation, 
                        self.config,
                        mode='manual'
                    )
                    logger.info(f"手动格式化与补全完成，最终演员数量: {len(final_cast_perfect)}。")


                    # =================================================================
                    # ★★★ 5. 【【【 核心逻辑简化：直接修改，不再重建 】】】 ★★★
                    # =================================================================
                    cache_folder_name = "tmdb-movies2" if item_type == "Movie" else "tmdb-tv"
                    base_cache_dir = os.path.join(self.local_data_path, "cache", cache_folder_name, tmdb_id)
                    base_override_dir = os.path.join(self.local_data_path, "override", cache_folder_name, tmdb_id)
                    image_override_dir = os.path.join(base_override_dir, "images")
                    os.makedirs(base_override_dir, exist_ok=True)
                    base_json_filename = "all.json" if item_type == "Movie" else "series.json"
                    
                    # 5.1 读取神医插件生成的原始JSON文件
                    json_file_path = os.path.join(base_cache_dir, base_json_filename)
                    if not os.path.exists(json_file_path):
                        raise FileNotFoundError(f"手动处理失败，因为神医插件的缓存文件不存在: {json_file_path}")
                    
                    base_json_data_original = _read_local_json(json_file_path)
                    if not base_json_data_original:
                        raise ValueError(f"无法读取或解析JSON文件: {json_file_path}")

                    # 5.2 直接在原始数据上替换演员表部分
                    logger.trace("将直接修改缓存元数据中的演员表，并保留所有其他字段。")
                    base_json_data_for_override = base_json_data_original # 直接引用，不再重建

                    if item_type == "Movie":
                        base_json_data_for_override.setdefault("casts", {})["cast"] = final_cast_perfect
                    else: # Series
                        base_json_data_for_override.setdefault("credits", {})["cast"] = final_cast_perfect
                    
                    # 5.3 将修改后的完整JSON写入覆盖文件
                    override_json_path = os.path.join(base_override_dir, base_json_filename)
                    
                    temp_json_path = f"{override_json_path}.{random.randint(1000, 9999)}.tmp"
                    with open(temp_json_path, 'w', encoding='utf-8') as f:
                        json.dump(base_json_data_for_override, f, ensure_ascii=False, indent=4)
                    os.replace(temp_json_path, override_json_path)
                    logger.debug(f"手动处理：成功生成覆盖元数据文件: {override_json_path}")

                    #---深度处理剧集
                    process_episodes_config = self.config.get(constants.CONFIG_OPTION_PROCESS_EPISODES, False)
                    if item_type == "Series" and self.config.get(constants.CONFIG_OPTION_PROCESS_EPISODES, False):
                        logger.info(f"手动处理：开始为所有分集注入手动编辑后的演员表...")
                        # (这部分逻辑已经很精简了，直接复用即可)
                        for filename in os.listdir(base_cache_dir):
                            if self.is_stop_requested():
                                logger.warning(f"手动保存的分集处理循环被用户中止。")
                                raise InterruptedError("任务中止")
                            if filename.startswith("season-") and filename.lower().endswith('.json'):
                                child_json_original = _read_local_json(os.path.join(base_cache_dir, filename))
                                if child_json_original:
                                    child_json_for_override = child_json_original # 直接引用
                                    child_json_for_override.setdefault("credits", {})["cast"] = final_cast_perfect
                                    child_json_for_override["guest_stars"] = []
                                    override_child_path = os.path.join(base_override_dir, filename)
                                    try:
                                        # 使用原子写入，防止意外中断
                                        temp_child_path = f"{override_child_path}.{random.randint(1000, 9999)}.tmp"
                                        with open(temp_child_path, 'w', encoding='utf-8') as f:
                                            json.dump(child_json_for_override, f, ensure_ascii=False, indent=4)
                                        os.replace(temp_child_path, override_child_path)
                                    except Exception as e:
                                        logger.error(f"手动处理：写入子项目JSON失败: {override_child_path}, {e}")
                                        if os.path.exists(temp_child_path): os.remove(temp_child_path)

                    #---同步图片 
                    if self.sync_images_enabled:
                        if self.is_stop_requested(): raise InterruptedError("任务被中止")
                        
                        # 直接调用我们新的、可复用的图片同步方法
                        # 注意：item_details_from_emby 就是它需要的参数
                        self.sync_item_images(item_details)

                    logger.info(f"手动处理：准备刷新 Emby 项目 {item_name}...")
                    refresh_success = emby_handler.refresh_emby_item_metadata(
                        item_emby_id=item_id,
                        emby_server_url=self.emby_url,
                        emby_api_key=self.emby_api_key,
                        replace_all_metadata_param=True,
                        item_name_for_log=item_name,
                        user_id_for_unlock=self.emby_user_id
                    )
                    if not refresh_success:
                        logger.warning(f"手动处理：文件已生成，但触发 Emby 刷新失败。你可能需要稍后在 Emby 中手动刷新。")

                    # 更新处理日志
                    self.log_db_manager.save_to_processed_log(cursor, item_id, item_name, score=10.0) # 手动处理直接给满分
                    self.log_db_manager.remove_from_failed_log(cursor, item_id)
                    
                    # ✨✨✨ 提交事务 ✨✨✨
                    conn.commit()
                    logger.info(f"✅ 手动处理 '{item_name}' 流程完成，数据库事务已提交。")
                    return True
                
                except Exception as inner_e:
                    # 如果在事务中发生任何错误，回滚
                    logger.error(f"手动处理事务中发生错误 for {item_name}: {inner_e}", exc_info=True)
                    conn.rollback()
                    logger.warning(f"由于发生错误，针对 '{item_name}' 的数据库更改已回滚。")
                    # 重新抛出，让外层捕获
                    raise

        except Exception as outer_e:
            logger.error(f"手动处理 '{item_name}' 时发生顶层错误: {outer_e}", exc_info=True)
            return False
        finally:
            # ★★★ 清理本次编辑会话的缓存 ★★★
            if item_id in self.manual_edit_cache:
                del self.manual_edit_cache[item_id]
                logger.debug(f"已清理 ItemID {item_id} 的内存缓存。")
        if cache_update_succeeded:
            updated_cache_count += 1
    # --- 从本地 cache 文件获取演员列表用于编辑 ---
    def get_cast_for_editing(self, item_id: str) -> Optional[Dict[str, Any]]:
        """
        【V5 - 最终版】为手动编辑准备数据。
        1. 根据类型，从本地 cache 加载最完整的演员列表（电影直接加载，电视剧智能聚合）。
        2. 将完整列表缓存在内存中，供后续保存时使用。
        3. 只向前端发送轻量级数据 (ID, name, role, profile_path)。
        """
        logger.info(f"为编辑页面准备数据 (后端缓存模式)：ItemID {item_id}")
        
        try:
            # 1. 获取基础信息
            emby_details = emby_handler.get_emby_item_details(item_id, self.emby_url, self.emby_api_key, self.emby_user_id)
            if not emby_details: raise ValueError(f"在Emby中未找到项目 {item_id}")

            tmdb_id = emby_details.get("ProviderIds", {}).get("Tmdb")
            item_type = emby_details.get("Type")
            item_name_for_log = emby_details.get("Name", f"未知(ID:{item_id})")
            if not tmdb_id: raise ValueError(f"项目 {item_id} 缺少 TMDb ID")

            # 2. 从本地 cache 文件读取最可靠的演员列表
            cache_folder_name = "tmdb-movies2" if item_type == "Movie" else "tmdb-tv"
            base_cache_dir = os.path.join(self.local_data_path, "cache", cache_folder_name, tmdb_id)
            base_json_filename = "all.json" if item_type == "Movie" else "series.json"
            full_cast_from_cache = []
            
            if item_type == "Movie":
                tmdb_data = _read_local_json(os.path.join(base_cache_dir, base_json_filename))
                if not tmdb_data: raise ValueError("未找到本地 TMDb 缓存文件")
                full_cast_from_cache = tmdb_data.get("casts", {}).get("cast", [])
            
            elif item_type == "Series":
                # ✨✨✨ 同样直接调用新的聚合函数 ✨✨✨
                full_cast_from_cache = self._aggregate_series_cast_from_cache(
                    base_cache_dir=base_cache_dir,
                    item_name_for_log=item_name_for_log
                )

            # 3. 将完整的演员列表存入内存缓存
            self.manual_edit_cache[item_id] = full_cast_from_cache
            logger.debug(f"已为 ItemID {item_id} 缓存了 {len(full_cast_from_cache)} 条完整演员数据。")

            # 4. 构建并发送“轻量级”数据给前端
            cast_for_frontend = []
            for actor_data in full_cast_from_cache:
                actor_tmdb_id = actor_data.get('id')
                if not actor_tmdb_id: continue
                
                profile_path = actor_data.get('profile_path')
                image_url = f"https://image.tmdb.org/t/p/w185{profile_path}" if profile_path else None

                # ✨✨✨ 1. 获取从缓存文件中读出的、可能带有前缀的角色名
                role_from_cache = actor_data.get('character', '')
                
                # ✨✨✨ 2. 调用清理函数，得到干净的角色名
                cleaned_role_for_display = utils.clean_character_name_static(role_from_cache)

                cast_for_frontend.append({
                    "tmdbId": actor_tmdb_id,
                    "name": actor_data.get('name'),
                    "role": cleaned_role_for_display,
                    "imageUrl": image_url,
                })
            
            # 5. 获取失败日志信息和组合 response_data
            failed_log_info = {}
            with get_central_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT error_message, score FROM failed_log WHERE item_id = ?", (item_id,))
                row = cursor.fetchone()
                if row: failed_log_info = dict(row)

            response_data = {
                "item_id": item_id,
                "item_name": emby_details.get("Name"),
                "item_type": item_type,
                "image_tag": emby_details.get('ImageTags', {}).get('Primary'),
                "original_score": failed_log_info.get("score"),
                "review_reason": failed_log_info.get("error_message"),
                "current_emby_cast": cast_for_frontend,
                "search_links": {
                    "google_search_wiki": utils.generate_search_url('wikipedia', emby_details.get("Name"), emby_details.get("ProductionYear"))
                }
            }
            return response_data

        except Exception as e:
            logger.error(f"获取编辑数据失败 for ItemID {item_id}: {e}", exc_info=True)
            return None
    # ★★★ 全量图片同步的核心逻辑 ★★★
    def sync_all_images(self, update_status_callback: Optional[callable] = None):
        """
        【最终正确版】遍历所有已处理的媒体项，将它们在 Emby 中的当前图片下载到本地 override 目录。
        """
        logger.info("--- 开始执行全量海报同步任务 ---")
        
        try:
            with get_central_db_connection(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT item_id, item_name FROM processed_log")
                items_to_process = cursor.fetchall()
        except Exception as e:
            logger.error(f"获取已处理项目列表时发生数据库错误: {e}", exc_info=True)
            if update_status_callback:
                update_status_callback(-1, "数据库错误")
            return

        total = len(items_to_process)
        if total == 0:
            logger.info("没有已处理的项目，无需同步图片。")
            if update_status_callback:
                update_status_callback(100, "没有项目")
            return

        logger.info(f"共找到 {total} 个已处理项目需要同步图片。")

        for i, db_row in enumerate(items_to_process):
            if self.is_stop_requested():
                logger.info("全量图片同步任务被中止。")
                break

            item_id = db_row['item_id']
            item_name_from_db = db_row['item_name']
            
            if not item_id:
                logger.warning(f"数据库中发现一条没有 item_id 的记录，跳过。Name: {item_name_from_db}")
                continue

            if update_status_callback:
                update_status_callback(int((i / total) * 100), f"同步图片 ({i+1}/{total}): {item_name_from_db}")

            try:
                item_details = emby_handler.get_emby_item_details(item_id, self.emby_url, self.emby_api_key, self.emby_user_id)
                
                if not item_details:
                    logger.warning(f"跳过 {item_name_from_db} (ID: {item_id})，无法从 Emby 获取其详情。")
                    continue

                tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
                item_type = item_details.get("Type")
                
                if not tmdb_id:
                    logger.warning(f"跳过 '{item_name_from_db}'，因为它缺少 TMDb ID。")
                    continue
                override_path = utils.get_override_path_for_item(item_type, tmdb_id, self.config)

                if not override_path:
                    logger.warning(f"跳过 '{item_name_from_db}'，无法为其生成有效的 override 路径 (可能是未知类型或配置问题)。")
                    continue

                image_override_dir = os.path.join(override_path, "images")
                os.makedirs(image_override_dir, exist_ok=True)

                image_map = {"Primary": "poster.jpg", "Backdrop": "fanart.jpg", "Logo": "clearlogo.png"}
                if item_type == "Movie":
                    image_map["Thumb"] = "landscape.jpg"
                
                logger.debug(f"项目 '{item_name_from_db}': 准备下载图片集到 '{image_override_dir}'")

                for image_type, filename in image_map.items():
                    emby_handler.download_emby_image(
                        item_id, 
                        image_type, 
                        os.path.join(image_override_dir, filename), 
                        self.emby_url, 
                        self.emby_api_key
                    )
                
                if item_type == "Series":
                    logger.info(f"开始为剧集 '{item_name_from_db}' 同步季海报...")
                    children = emby_handler.get_series_children(item_id, self.emby_url, self.emby_api_key, self.emby_user_id) or []
                    
                    for child in children:
                        # 只处理类型为 "Season" 的子项目，完全忽略 "Episode"
                        if child.get("Type") == "Season":
                            season_number = child.get("IndexNumber")
                            if season_number is not None:
                                logger.info(f"  正在同步第 {season_number} 季的海报...")
                                emby_handler.download_emby_image(
                                    child.get("Id"), 
                                    "Primary", # 季项目通常只有 Primary 图片
                                    os.path.join(image_override_dir, f"season-{season_number}.jpg"),
                                    self.emby_url, 
                                    self.emby_api_key
                                )
                
                logger.info(f"成功同步了 '{item_name_from_db}' 的图片。")

            except Exception as e:
                logger.error(f"同步项目 '{item_name_from_db}' (ID: {item_id}) 的图片时发生错误: {e}", exc_info=True)
            
            time.sleep(0.2)

        logger.info("--- 全量海报同步任务结束 ---")
    # --- 一键删除本地TMDB缓存 ---
    def clear_tmdb_caches(self) -> Dict[str, Any]:
        """
        【V2 - 精准清除版】一键清除所有TMDb相关的 .json 缓存和覆盖文件。
        此操作会强制所有项目在下次处理时重新从在线获取元数据，但会保留图片等非JSON文件。
        """
        if not self.local_data_path:
            msg = "未配置本地数据路径 (local_data_path)，无法执行清除操作。"
            logger.error(msg)
            return {"success": False, "message": msg, "details": {}}

        # 定义需要被扫描的目标目录
        target_subdirs = {
            "cache": ["tmdb-movies2", "tmdb-tv"],
            "override": ["tmdb-movies2", "tmdb-tv"]
        }

        report = {"success": True, "message": "TMDb .json 缓存清除成功！", "details": {}}
        base_path = self.local_data_path
        
        logger.warning("!!! 开始执行操作：清除所有TMDb .json缓存文件 !!!")

        for dir_type, subdirs in target_subdirs.items():
            report["details"][dir_type] = []
            for subdir_name in subdirs:
                full_path = os.path.join(base_path, dir_type, subdir_name)
                
                if os.path.isdir(full_path):
                    # ★★★ 核心修改：不再直接删除，而是遍历并筛选 .json 文件 ★★★
                    files_deleted_count = 0
                    errors_encountered = False
                    
                    try:
                        # 使用 os.walk 遍历目标目录及其所有子目录
                        for dirpath, _, filenames in os.walk(full_path):
                            if self.is_stop_requested():
                                logger.warning("缓存清除任务被中止 (遍历目录时)。")
                                break # 跳出 for dirpath... 循环
                            for filename in filenames:
                                if self.is_stop_requested():
                                    logger.warning("缓存清除任务被中止 (处理文件时)。")
                                    break # 跳出 for filename... 循环
                                # 检查文件是否以 .json 结尾 (不区分大小写)
                                if filename.lower().endswith('.json'):
                                    file_to_delete = os.path.join(dirpath, filename)
                                    try:
                                        os.remove(file_to_delete)
                                        files_deleted_count += 1
                                    except OSError as e_remove:
                                        # 如果单个文件删除失败，记录错误并继续
                                        msg = f"删除文件 {file_to_delete} 时失败: {e_remove}"
                                        logger.error(msg)
                                        report["details"][dir_type].append(msg)
                                        errors_encountered = True
                        
                        # 根据遍历结果生成报告信息
                        if errors_encountered:
                            msg = f"从 {full_path} 清理 .json 文件时遇到错误。共删除了 {files_deleted_count} 个文件。"
                            report["success"] = False
                            report["message"] = "清除过程中发生错误，部分缓存可能未被清除。"
                        else:
                            msg = f"成功从 {full_path} 及其子目录中删除了 {files_deleted_count} 个 .json 文件。"
                        
                        logger.info(msg)
                        report["details"][dir_type].append(msg)

                    except Exception as e_walk:
                        msg = f"遍历目录 {full_path} 时发生严重错误: {e_walk}"
                        logger.error(msg, exc_info=True)
                        report["details"][dir_type].append(msg)
                        report["success"] = False
                        report["message"] = "清除过程中发生错误，部分缓存可能未被清除。"
                else:
                    msg = f"目录不存在，跳过: {full_path}"
                    logger.info(msg)
                    report["details"][dir_type].append(msg)
        
        if report["success"]:
            logger.info("✅ 所有指定的TMDb .json 缓存文件已成功清除。")
        else:
            logger.error("❌ 清除TMDb .json 缓存操作未完全成功，请检查日志。")
            
        return report
    # --- 图片同步 ---
    def sync_item_images(self, item_details: Dict[str, Any], update_description: Optional[str] = None) -> bool:
        """
        【新增-重构】这个方法负责同步一个媒体项目的所有相关图片。
        它从 _process_item_core_logic 中提取出来，以便复用。
        """
        item_id = item_details.get("Id")
        item_type = item_details.get("Type")
        item_name_for_log = item_details.get("Name", f"未知项目(ID:{item_id})")
        
        if not all([item_id, item_type, self.local_data_path]):
            logger.error(f"[图片同步] 跳过 '{item_name_for_log}'，因为缺少ID、类型或未配置本地数据路径。")
            return False

        try:
            # --- 准备工作 (目录、TMDb ID等) ---
            log_prefix = "[图片同步]"
            tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
            if not tmdb_id:
                logger.warning(f"{log_prefix} 项目 '{item_name_for_log}' 缺少TMDb ID，无法确定覆盖目录，跳过。")
                return False
            
            cache_folder_name = "tmdb-movies2" if item_type == "Movie" else "tmdb-tv"
            base_override_dir = os.path.join(self.local_data_path, "override", cache_folder_name, tmdb_id)
            image_override_dir = os.path.join(base_override_dir, "images")
            os.makedirs(image_override_dir, exist_ok=True)

            # --- 定义所有可能的图片映射 ---
            full_image_map = {"Primary": "poster.jpg", "Backdrop": "fanart.jpg", "Logo": "clearlogo.png"}
            if item_type == "Movie":
                full_image_map["Thumb"] = "landscape.jpg"

            # ★★★ 全新逻辑分发 ★★★
            images_to_sync = {}
            
            # 模式一：精准同步 (当描述存在时)
            if update_description:
                log_prefix = "[精准图片同步]"
                logger.debug(f"{log_prefix} 正在解析描述: '{update_description}'")
                
                # 定义关键词到Emby图片类型的映射 (使用小写以方便匹配)
                keyword_map = {
                    "primary": "Primary",
                    "backdrop": "Backdrop",
                    "logo": "Logo",
                    "thumb": "Thumb", # 电影缩略图
                    "banner": "Banner" # 剧集横幅 (如果需要可以添加)
                }
                
                desc_lower = update_description.lower()
                found_specific_image = False
                for keyword, image_type_api in keyword_map.items():
                    if keyword in desc_lower and image_type_api in full_image_map:
                        images_to_sync[image_type_api] = full_image_map[image_type_api]
                        logger.debug(f"{log_prefix} 匹配到关键词 '{keyword}'，将只同步 {image_type_api} 图片。")
                        found_specific_image = True
                        break # 找到第一个匹配就停止，避免重复
                
                if not found_specific_image:
                    logger.warning(f"{log_prefix} 未能在描述中找到可识别的图片关键词，将回退到完全同步。")
                    images_to_sync = full_image_map # 回退
            
            # 模式二：完全同步 (默认或回退)
            else:
                log_prefix = "[完整图片同步]"
                logger.debug(f"{log_prefix} 未提供更新描述，将同步所有类型的图片。")
                images_to_sync = full_image_map

            # --- 执行下载 ---
            logger.info(f"{log_prefix} 开始为 '{item_name_for_log}' 下载 {len(images_to_sync)} 张图片至 {image_override_dir}...")
            for image_type, filename in images_to_sync.items():
                if self.is_stop_requested():
                    logger.warning(f"{log_prefix} 收到停止信号，中止图片下载。")
                    return False
                emby_handler.download_emby_image(item_id, image_type, os.path.join(image_override_dir, filename), self.emby_url, self.emby_api_key)
            
            # --- 分集图片逻辑 (只有在完全同步时才考虑执行) ---
            if images_to_sync == full_image_map and item_type == "Series":
            
                # 决定是否处理分集图片，这里可以根据全局配置
                should_process_episodes_this_run = self.config.get(constants.CONFIG_OPTION_PROCESS_EPISODES, False)
                if should_process_episodes_this_run:
                    children = emby_handler.get_series_children(item_id, self.emby_url, self.emby_api_key, self.emby_user_id, series_name_for_log=item_name_for_log) or []
                    for child in children:
                        if self.is_stop_requested():
                            logger.warning(f"{log_prefix} 收到停止信号，中止子项目图片下载。")
                            return False
                        child_type, child_id = child.get("Type"), child.get("Id")
                        if child_type == "Season":
                            season_number = child.get("IndexNumber")
                            if season_number is not None:
                                emby_handler.download_emby_image(child_id, "Primary", os.path.join(image_override_dir, f"season-{season_number}.jpg"), self.emby_url, self.emby_api_key)
                        elif child_type == "Episode":
                            season_number, episode_number = child.get("ParentIndexNumber"), child.get("IndexNumber")
                            if season_number is not None and episode_number is not None:
                                emby_handler.download_emby_image(child_id, "Primary", os.path.join(image_override_dir, f"season-{season_number}-episode-{episode_number}.jpg"), self.emby_url, self.emby_api_key)
            
            logger.info(f"{log_prefix} ✅ 成功完成 '{item_name_for_log}' 的图片同步。")
            return True
        except Exception as e:
            logger.error(f"{log_prefix} 为 '{item_name_for_log}' 同步图片时发生未知错误: {e}", exc_info=True)
            return False
    # --- 聚合演员表 ---
    def _aggregate_series_cast_from_cache(self, base_cache_dir: str, item_name_for_log: str) -> List[Dict[str, Any]]:
        """
        【V3 - 最终修复版】聚合一个剧集所有本地缓存JSON文件中的演员列表。

        此函数会扫描指定TMDb缓存目录，读取series.json、所有season-*.json和
        season-*-episode-*.json文件，提取其中的演员和客串演员，
        然后去重并形成一个完整的演员列表。

        Args:
            base_cache_dir (str): 剧集的TMDb缓存根目录路径。
            item_name_for_log (str): 用于日志记录的媒体项目名称。

        Returns:
            List[Dict[str, Any]]: 聚合、去重并排序后的完整演员列表。
        """
        logger.info(f"【演员聚合】开始为 '{item_name_for_log}' 聚合所有JSON文件中的演员...")
        
        aggregated_cast_map = {}
        
        # 1. 优先处理主文件
        base_json_filename = "series.json"
        main_series_json_path = os.path.join(base_cache_dir, base_json_filename)
        
        main_data = _read_local_json(main_series_json_path)
        if main_data:
            # 主演列表的优先级最高
            main_cast = main_data.get("credits", {}).get("cast", [])
            for actor in main_cast:
                actor_id = actor.get("id")
                if actor_id:
                    aggregated_cast_map[actor_id] = actor
            logger.debug(f"  -> 从 {base_json_filename} 中加载了 {len(aggregated_cast_map)} 位主演员。")
        else:
            logger.warning(f"  -> 未找到主剧集文件: {main_series_json_path}，将只处理子文件。")

        # 2. 扫描并聚合所有子文件（分季、分集）
        try:
            # 获取所有需要处理的子文件名
            child_json_files = [
                f for f in os.listdir(base_cache_dir) 
                if f != base_json_filename and f.startswith("season-") and f.lower().endswith(".json")
            ]
            
            if child_json_files:
                logger.debug(f"  -> 发现 {len(child_json_files)} 个额外的季/集JSON文件需要处理。")

                for json_filename in sorted(child_json_files):
                    file_path = os.path.join(base_cache_dir, json_filename)
                    child_data = _read_local_json(file_path)
                    if not child_data:
                        continue

                    # ✨✨✨ 核心修复：从 "credits" 对象中安全地获取 cast 和 guest_stars ✨✨✨
                    credits_data = child_data.get("credits", {})
                    
                    # 将两个列表安全地合并成一个待处理列表
                    actors_to_process = credits_data.get("cast", []) + credits_data.get("guest_stars", [])
                    
                    if not actors_to_process:
                        continue

                    # 遍历并添加新演员
                    for actor in actors_to_process:
                        actor_id = actor.get("id")
                        # 确保演员有ID
                        if not actor_id:
                            continue
                        
                        # 如果演员ID还未记录，就添加他/她
                        if actor_id not in aggregated_cast_map:
                            # ✨ 新增调试日志，用于追踪
                            logger.trace(f"    -> 新增演员 (ID: {actor_id}): {actor.get('name')}")
                            
                            # 为客串演员设置一个默认的高 'order' 值，确保他们排在主演后面
                            if 'order' not in actor:
                                actor['order'] = 999 
                            aggregated_cast_map[actor_id] = actor

        except FileNotFoundError:
            logger.warning(f"  -> 缓存目录 {base_cache_dir} 不存在，无法聚合子项目演员。")
        
        # 3. 将最终结果从字典转为列表并排序
        full_aggregated_cast = list(aggregated_cast_map.values())
        full_aggregated_cast.sort(key=lambda x: x.get('order', 999))
        
        logger.info(f"【演员聚合】完成。共为 '{item_name_for_log}' 聚合了 {len(full_aggregated_cast)} 位独立演员。")
        
        return full_aggregated_cast
    
    def close(self):
        if self.douban_api: self.douban_api.close()
