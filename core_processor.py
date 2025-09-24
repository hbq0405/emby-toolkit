# core_processor.py

import os
import json
import concurrent.futures
from typing import Dict, List, Optional, Any, Tuple, Set
import shutil
import threading
from datetime import datetime, timezone
import time as time_module
import psycopg2
import requests
# 确保所有依赖都已正确导入
import emby_handler
import tmdb_handler
import utils
import constants
import logging
import actor_utils
from database.actor_db import ActorDBManager
from database.log_db import LogDBManager
from database.connection import get_db_connection as get_central_db_connection
from cachetools import TTLCache
from ai_translator import AITranslator
from utils import get_override_path_for_item, translate_country_list, get_unified_rating
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
    cursor: psycopg2.extensions.cursor,
    tmdb_id: str,
    emby_item_id: str,
    item_type: str,
    item_details_from_emby: Dict[str, Any],
    final_processed_cast: List[Dict[str, Any]],
    tmdb_details_for_extra: Optional[Dict[str, Any]]
):
    """
    【V-API-Native - PG 兼容版】
    修复了 SQLite 特有的 INSERT OR REPLACE 语法。
    """
    try:
        logger.trace(f"【实时缓存】正在为 '{item_details_from_emby.get('Name')}' 组装元数据...")
        
        actors_for_cache = []
        for p in final_processed_cast:
            actors_for_cache.append({
                "id": p.get("id"),
                "name": p.get("name"),
                "character": p.get("character"), # <--- 关键！保存角色名
                "original_name": p.get("original_name"),
                "profile_path": p.get("profile_path"),
                "gender": p.get("gender"),
                "popularity": p.get("popularity")
            })

        directors, countries = [], []
        if tmdb_details_for_extra:
            if item_type == 'Movie':
                credits_data = tmdb_details_for_extra.get("credits", {}) or tmdb_details_for_extra.get("casts", {})
                if credits_data:
                    directors = [{'id': p.get('id'), 'name': p.get('name')} for p in credits_data.get('crew', []) if p.get('job') == 'Director']
                country_names = [c['name'] for c in tmdb_details_for_extra.get('production_countries', [])]
                countries = translate_country_list(country_names)
            elif item_type == 'Series':
                credits_data = tmdb_details_for_extra.get("credits", {})
                if credits_data:
                    directors = [{'id': p.get('id'), 'name': p.get('name')} for p in credits_data.get('crew', []) if p.get('job') == 'Director']
                if not directors:
                    directors = [{'id': c.get('id'), 'name': c.get('name')} for c in tmdb_details_for_extra.get('created_by', [])]
                country_codes = tmdb_details_for_extra.get('origin_country', [])
                countries = translate_country_list(country_codes)
        
        studios = [s['Name'] for s in item_details_from_emby.get('Studios', [])]
        genres = item_details_from_emby.get('Genres', [])
        release_date_str = (item_details_from_emby.get('PremiereDate') or '0000-01-01T00:00:00.000Z').split('T')[0]
        
        metadata = {
            "tmdb_id": tmdb_id,
            "emby_item_id": emby_item_id,
            "item_type": item_type,
            "title": item_details_from_emby.get('Name'),
            "original_title": item_details_from_emby.get('OriginalTitle'),
            "release_year": item_details_from_emby.get('ProductionYear'),
            "rating": item_details_from_emby.get('CommunityRating'),
            "genres_json": json.dumps(genres, ensure_ascii=False),
            "actors_json": json.dumps(actors_for_cache, ensure_ascii=False),
            "directors_json": json.dumps(directors, ensure_ascii=False),
            "studios_json": json.dumps(studios, ensure_ascii=False),
            "countries_json": json.dumps(countries, ensure_ascii=False),
            "date_added": (item_details_from_emby.get("DateCreated") or '').split('T')[0] or None,
            "release_date": release_date_str,
        }
        
        columns = list(metadata.keys())
        # ▼▼▼ 核心修复 1/2：在列清单中手动加入 last_synced_at ▼▼▼
        columns.append("last_synced_at")
        
        columns_str = ', '.join(columns)
        # 占位符比 metadata 字典多一个，用于 NOW()
        placeholders_str = ', '.join(['%s'] * len(metadata)) + ', NOW()'
        
        update_clauses = [f"{col} = EXCLUDED.{col}" for col in metadata.keys()]
        # ▼▼▼ 核心修复 2/2：在 UPDATE 语句中也确保更新 last_synced_at ▼▼▼
        update_clauses.append("last_synced_at = NOW()")
        update_str = ', '.join(update_clauses)

        sql = f"""
            INSERT INTO media_metadata ({columns_str})
            VALUES ({placeholders_str})
            ON CONFLICT (tmdb_id, item_type) DO UPDATE SET {update_str}
        """
        
        # 执行时，值的元组不需要包含 NOW()，因为它已经在 SQL 字符串里了
        cursor.execute(sql, tuple(metadata.values()))
        logger.debug(f"  -> 成功将《{metadata.get('title')}》的元数据缓存到数据库（并更新了同步时间）。")

    except Exception as e:
        logger.error(f"保存元数据到缓存表时失败: {e}", exc_info=True)
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
    
    logger.info(f"  -> 共为 '{series_data.get('name')}' 聚合了 {len(full_aggregated_cast)} 位独立演员。")
    return full_aggregated_cast
class MediaProcessor:
    def __init__(self, config: Dict[str, Any]):
        # ★★★ 然后，从这个 config 字典里，解析出所有需要的属性 ★★★
        self.config = config

        # 初始化我们的数据库管理员
        self.actor_db_manager = ActorDBManager()
        self.log_db_manager = LogDBManager()

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
        self.auto_lock_cast_enabled = self.config.get(constants.CONFIG_OPTION_AUTO_LOCK_CAST, True)
        
        self.ai_enabled = self.config.get("ai_translation_enabled", False)
        self.ai_translator = AITranslator(self.config) if self.ai_enabled else None
        
        self._stop_event = threading.Event()
        self.processed_items_cache = self._load_processed_log_from_db()
        self.manual_edit_cache = TTLCache(maxsize=10, ttl=600)
        logger.trace("核心处理器初始化完成。")
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
        
        logger.debug(f"  -> 已将 '{item_name}' 标记为已处理 (数据库 & 内存)。")
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
            
            logger.info("数据库中的已处理记录已清除。")

            # 2. 清空内存缓存
            self.processed_items_cache.clear()
            logger.info("内存中的已处理记录缓存已清除。")

        except Exception as e:
            logger.error(f"清除数据库或内存已处理记录时失败: {e}", exc_info=True)
            # 3. ★★★ 重新抛出异常，通知上游调用者操作失败 ★★★
            raise
    # --- 演员数据查询、反哺 ---
    def _enrich_cast_from_db_and_api(self, cast_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        演员数据查询、反哺
        """
        if not cast_list:
            return []
        
        logger.info(f"  -> 正在为 {len(cast_list)} 位演员丰富数据...")

        original_actor_map = {str(actor.get("Id")): actor for actor in cast_list if actor.get("Id")}
        
        # --- 阶段一：从本地数据库获取数据 ---
        enriched_actors_map = {}
        ids_found_in_db = set()
        
        try:
            db_results = []
            
            with get_central_db_connection() as conn:
                cursor = conn.cursor()
                person_ids = list(original_actor_map.keys())
                
                if person_ids:
                    query = "SELECT * FROM person_identity_map WHERE emby_person_id = ANY(%s)"
                    cursor.execute(query, (person_ids,))
                    db_results = cursor.fetchall()

            for row in db_results:
                db_data = dict(row)
                actor_id = str(db_data["emby_person_id"])
                ids_found_in_db.add(actor_id)
                
                provider_ids = {}
                if db_data.get("tmdb_person_id"):
                    provider_ids["Tmdb"] = str(db_data.get("tmdb_person_id"))
                if db_data.get("imdb_id"):
                    provider_ids["Imdb"] = db_data.get("imdb_id")
                if db_data.get("douban_celebrity_id"):
                    provider_ids["Douban"] = str(db_data.get("douban_celebrity_id"))
                
                enriched_actor = original_actor_map[actor_id].copy()
                enriched_actor["ProviderIds"] = provider_ids
                enriched_actors_map[actor_id] = enriched_actor
                
        except Exception as e:
            logger.error(f"  -> 数据库查询阶段失败: {e}", exc_info=True)

        logger.info(f"  -> 从演员映射表找到了 {len(ids_found_in_db)} 位演员的信息。")

        # --- 阶段二：为未找到的演员实时查询 Emby API ---
        ids_to_fetch_from_api = [pid for pid in original_actor_map.keys() if pid not in ids_found_in_db]
        
        if ids_to_fetch_from_api:
            logger.trace(f"  -> 开始为 {len(ids_to_fetch_from_api)} 位新演员从Emby获取信息...")
            
        else:
            logger.trace("  -> (API查询) 跳过：所有演员均在本地数据库中找到。")

        # --- 阶段三：合并最终结果 ---
        final_enriched_cast = []
        for original_actor in cast_list:
            actor_id = str(original_actor.get("Id"))
            final_enriched_cast.append(enriched_actors_map.get(actor_id, original_actor))

        return final_enriched_cast
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

        logger.info(f"  -> 开始为新入库剧集 '{item_name_for_log}' 进行追剧状态判断...")
        try:
            # 实例化 WatchlistProcessor 并执行添加操作
            watchlist_proc = WatchlistProcessor(self.config)
            watchlist_proc.add_series_to_watchlist(item_details)
        except Exception as e_watchlist:
            logger.error(f"  -> 在自动添加 '{item_name_for_log}' 到追剧列表时发生错误: {e_watchlist}", exc_info=True)

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
            logger.debug(f"  -> 发现本地豆瓣缓存文件，将直接使用: {local_json_path}")
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
        logger.info("  -> 未找到本地豆瓣缓存，将通过在线API获取演员和评分信息。")

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
                    logger.info(f"  -> 在线获取到豆瓣评分 for '{item_name}': {douban_rating}")
                except (ValueError, TypeError):
                    pass

        return douban_cast_raw, douban_rating
    
    # --- 通过豆瓣ID查找映射表 ---
    def _find_person_in_map_by_douban_id(self, douban_id: str, cursor: psycopg2.extensions.cursor) -> Optional[Dict[str, Any]]:
        """
        根据豆瓣名人ID在 person_identity_map 表中查找对应的记录。
        """
        if not douban_id:
            return None
        try:
            cursor.execute(
                "SELECT * FROM person_identity_map WHERE douban_celebrity_id = %s",
                (douban_id,)
            )
            return cursor.fetchone()
        except psycopg2.Error as e:
            logger.error(f"通过豆瓣ID '{douban_id}' 查询 person_identity_map 时出错: {e}")
            return None
    
    # --- 通过TmdbID查找映射表 ---
    def _find_person_in_map_by_tmdb_id(self, tmdb_id: str, cursor: psycopg2.extensions.cursor) -> Optional[Dict[str, Any]]:
        """
        根据 TMDB ID 在 person_identity_map 表中查找对应的记录。
        """
        if not tmdb_id:
            return None
        try:
            cursor.execute(
                "SELECT * FROM person_identity_map WHERE tmdb_person_id = %s",
                (tmdb_id,)
            )
            return cursor.fetchone()
        except psycopg2.Error as e:
            logger.error(f"通过 TMDB ID '{tmdb_id}' 查询 person_identity_map 时出错: {e}")
            return None
    
    # --- 通过ImbdID查找映射表 ---
    def _find_person_in_map_by_imdb_id(self, imdb_id: str, cursor: psycopg2.extensions.cursor) -> Optional[Dict[str, Any]]:
        """
        根据 IMDb ID 在 person_identity_map 表中查找对应的记录。
        """
        if not imdb_id:
            return None
        try:
            # 核心改动：将查询字段从 douban_celebrity_id 改为 imdb_id
            cursor.execute(
                "SELECT * FROM person_identity_map WHERE imdb_id = %s",
                (imdb_id,)
            )
            return cursor.fetchone()
        except psycopg2.Error as e:
            logger.error(f"通过 IMDb ID '{imdb_id}' 查询 person_identity_map 时出错: {e}")
            return None
    
    # --- 补充新增演员额外数据 ---
    def _get_actor_metadata_from_cache(self, tmdb_id: int, cursor: psycopg2.extensions.cursor) -> Optional[Dict]:
        """根据TMDb ID从ActorMetadata缓存表中获取演员的元数据。"""
        if not tmdb_id:
            return None
        cursor.execute("SELECT * FROM actor_metadata WHERE tmdb_id = %s", (tmdb_id,))
        metadata_row = cursor.fetchone()  # fetchone() 返回一个 Dict[str, Any] 对象或 None
        if metadata_row:
            return dict(metadata_row)  # 将其转换为字典，方便使用
        return None
    
    # --- 批量注入分集演员表 ---
    def _batch_update_episodes_cast(self, series_id: str, series_name: str, final_cast_list: List[Dict[str, Any]]):
        """
        【V1 - 批量写入模块】
        将一个最终处理好的演员列表，高效地写入指定剧集下的所有分集。
        """
        logger.info(f"  -> 开始为剧集 '{series_name}' (ID: {series_id}) 批量更新所有分集的演员表...")
        
        # 1. 获取所有分集的 ID
        # 我们只需要 ID，所以可以请求更少的字段以提高效率
        episodes = emby_handler.get_series_children(
            series_id=series_id,
            base_url=self.emby_url,
            api_key=self.emby_api_key,
            user_id=self.emby_user_id,
            series_name_for_log=series_name,
            include_item_types="Episode" # ★★★ 明确指定只获取分集
        )
        
        if not episodes:
            logger.info("  -> 未找到任何分集，批量更新结束。")
            return

        total_episodes = len(episodes)
        logger.info(f"  -> 共找到 {total_episodes} 个分集需要更新。")
        
        # 2. 准备好要写入的数据 (所有分集都用同一份演员表)
        cast_for_emby_handler = []
        for actor in final_cast_list:
            cast_for_emby_handler.append({
                "name": actor.get("name"),
                "character": actor.get("character"),
                "emby_person_id": actor.get("emby_person_id"),
                "provider_ids": actor.get("provider_ids")
            })

        # 3. 遍历并逐个更新分集
        # 这里仍然需要逐个更新，因为 Emby API 不支持一次性更新多个项目的演员表
        # 但我们已经把最耗时的数据处理放在了循环外面
        for i, episode in enumerate(episodes):
            if self.is_stop_requested():
                logger.warning("分集批量更新任务被中止。")
                break
            
            episode_id = episode.get("Id")
            episode_name = episode.get("Name", f"分集 {i+1}")
            logger.debug(f"  ({i+1}/{total_episodes}) 正在更新分集 '{episode_name}' (ID: {episode_id})...")
            
            emby_handler.update_emby_item_cast(
                item_id=episode_id,
                new_cast_list_for_handler=cast_for_emby_handler,
                emby_server_url=self.emby_url,
                emby_api_key=self.emby_api_key,
                user_id=self.emby_user_id
            )
            # 加入一个微小的延迟，避免请求过于密集
            time_module.sleep(0.2)

        logger.info(f"  -> 剧集 '{series_name}' 的分集批量更新完成。")
    
    # --- 负责将最终处理好的数据写回Emby、更新日志、备份缓存等 ---
    def _write_data_and_finalize(self,
                                 item_details_from_emby: Dict[str, Any],
                                 final_processed_cast: List[Dict[str, Any]],
                                 douban_rating: Optional[float],
                                 tmdb_details_for_extra: Optional[Dict[str, Any]]):
        """
        【辅助函数】负责将最终处理好的数据写回Emby、更新日志、备份缓存等。
        """
        item_id = item_details_from_emby.get("Id")
        item_name_for_log = item_details_from_emby.get("Name", f"未知项目(ID:{item_id})")
        item_type = item_details_from_emby.get("Type")
        tmdb_id = item_details_from_emby.get("ProviderIds", {}).get("Tmdb")
        original_emby_actor_count = len([p for p in item_details_from_emby.get("People", []) if p.get("Type") == "Actor"])

        with get_central_db_connection() as conn:
            cursor = conn.cursor()
            
            # ======================================================================
            # 阶段 4: 数据写回 (Data Write-back)
            # ======================================================================
            # --- 步骤 4.0: [实时反哺] 更新演员映射表 ---
            logger.info("  -> [实时反哺] 开始使用最权威的数据更新演员映射表...")
            emby_config_for_upsert = {"url": self.emby_url, "api_key": self.emby_api_key, "user_id": self.emby_user_id}
            for actor in final_processed_cast:
                if actor.get("emby_person_id") and actor.get("id"):
                    provider_ids = actor.get("provider_ids", {})
                    person_data_for_db = {
                        "emby_id": actor.get("emby_person_id"), "name": actor.get("name"),
                        "tmdb_id": provider_ids.get("Tmdb"), "imdb_id": provider_ids.get("Imdb"),
                        "douban_id": provider_ids.get("Douban")
                    }
                    self.actor_db_manager.upsert_person(cursor=cursor, person_data=person_data_for_db, emby_config=emby_config_for_upsert)
            logger.info("  -> [实时反哺] 演员映射表更新完成。")

            # --- 步骤 4.1: 更新演员(Person)自身的元数据 ---
            logger.info("  -> 写回步骤 1/2: 检查并更新演员的元数据...")
            for actor in final_processed_cast:
                if self.is_stop_requested(): raise InterruptedError("任务在演员元数据更新阶段被中止。")
                emby_pid = actor.get("emby_person_id")
                if not emby_pid: continue
                data_to_update = {"Name": actor.get("name"), "ProviderIds": actor.get("provider_ids", {})}
                emby_handler.update_person_details(
                    person_id=emby_pid, new_data=data_to_update,
                    emby_server_url=self.emby_url, emby_api_key=self.emby_api_key, user_id=self.emby_user_id
                )
            logger.info("  -> 演员元数据更新完成。")

            # --- 步骤 4.2: 更新媒体项目自身的演员列表 ---
            logger.info("  -> 写回步骤 2/2: 准备将最终演员列表更新到媒体项目...")
            cast_for_emby_handler = [
                {"name": a.get("name"), "character": a.get("character"), "emby_person_id": a.get("emby_person_id"), "provider_ids": a.get("provider_ids")}
                for a in final_processed_cast
            ]
            update_success = emby_handler.update_emby_item_cast(
                item_id=item_id, new_cast_list_for_handler=cast_for_emby_handler,
                emby_server_url=self.emby_url, emby_api_key=self.emby_api_key,
                user_id=self.emby_user_id, new_rating=douban_rating
            )

            if item_type == "Series" and update_success:
                self._batch_update_episodes_cast(series_id=item_id, series_name=item_name_for_log, final_cast_list=final_processed_cast)

            # ======================================================================
            # 阶段 5: 通知Emby刷新完成收尾
            # ======================================================================
            auto_refresh_enabled = self.config.get(constants.CONFIG_OPTION_REFRESH_AFTER_UPDATE, True)
            if auto_refresh_enabled:
                auto_lock_enabled = self.config.get(constants.CONFIG_OPTION_AUTO_LOCK_CAST, True)
                fields_to_lock = ["Cast"] if auto_lock_enabled else None
                emby_handler.refresh_emby_item_metadata(
                    item_emby_id=item_id, emby_server_url=self.emby_url, emby_api_key=self.emby_api_key,
                    user_id_for_ops=self.emby_user_id, lock_fields=fields_to_lock,
                    replace_all_metadata_param=False, item_name_for_log=item_name_for_log
                )
            else:
                logger.info(f"  -> 没有启用自动刷新，跳过刷新和锁定步骤。")

            # ======================================================================
            # 阶段 6: 实时元数据缓存
            # ======================================================================
            _save_metadata_to_cache(
                cursor=cursor, tmdb_id=tmdb_id, emby_item_id=item_id, item_type=item_type,
                item_details_from_emby=item_details_from_emby,
                final_processed_cast=final_processed_cast,
                tmdb_details_for_extra=tmdb_details_for_extra
            )

            # ======================================================================
            # 阶段 7: 后续处理 (Post-processing)
            # ======================================================================
            genres = item_details_from_emby.get("Genres", [])
            is_animation = "Animation" in genres or "动画" in genres or "Documentary" in genres or "纪录" in genres
            processing_score = actor_utils.evaluate_cast_processing_quality(
                final_cast=final_processed_cast, original_cast_count=original_emby_actor_count,
                expected_final_count=len(final_processed_cast), is_animation=is_animation
            )
            min_score_for_review = float(self.config.get("min_score_for_review", constants.DEFAULT_MIN_SCORE_FOR_REVIEW))
            if processing_score < min_score_for_review:
                reason = f"处理评分 ({processing_score:.2f}) 低于阈值 ({min_score_for_review})。"
                self.log_db_manager.save_to_failed_log(cursor, item_id, item_name_for_log, reason, item_type, score=processing_score)
            else:
                self._mark_item_as_processed(cursor, item_id, item_name_for_log, score=processing_score)
                self.log_db_manager.remove_from_failed_log(cursor, item_id)
            
            conn.commit()

            # ======================================================================
            # 阶段 8: 覆盖缓存备份
            # ======================================================================
            self.sync_single_item_assets(item_id=item_id, final_cast_override=final_processed_cast)

    # --- 核心处理总管 ---
    def process_single_item(self, emby_item_id: str,
                            force_reprocess_this_item: bool = False,
                            force_fetch_from_tmdb: bool = False):
        """
        【V-API-Ready 最终版 - 带跳过功能】
        这个函数是API模式的入口，它会先检查是否需要跳过已处理的项目。
        """
        # 1. 除非强制，否则跳过已处理的
        if not force_reprocess_this_item and emby_item_id in self.processed_items_cache:
            item_name_from_cache = self.processed_items_cache.get(emby_item_id, f"ID:{emby_item_id}")
            logger.info(f"媒体 '{item_name_from_cache}' 跳过已处理记录。")
            return True

        # 2. 检查停止信号
        if self.is_stop_requested():
            return False

        # 3. 获取Emby详情，这是后续所有操作的基础
        item_details_precheck = emby_handler.get_emby_item_details(emby_item_id, self.emby_url, self.emby_api_key, self.emby_user_id, fields="Type")
        if not item_details_precheck:
            logger.error(f"process_single_item: 无法获取 Emby 项目 {emby_item_id} 的基础详情。")
            return False

        item_type = item_details_precheck.get("Type")
        item_details = None

        if item_type == "Series":
            # 如果是剧集，调用我们新的聚合函数
            item_details = emby_handler.get_emby_series_details_with_full_cast(
                series_id=emby_item_id,
                emby_server_url=self.emby_url,
                emby_api_key=self.emby_api_key,
                user_id=self.emby_user_id
            )
        else:
            # 如果是电影或其他类型，使用原来的函数
            item_details = emby_handler.get_emby_item_details(
                emby_item_id, self.emby_url, self.emby_api_key, self.emby_user_id
            )
        if not item_details:
            logger.error(f"process_single_item: 无法获取 Emby 项目 {emby_item_id} 的详情。")
            return False

        # 4. 将任务交给核心处理函数
        return self._process_item_core_logic_api_version(
            item_details_from_emby=item_details,
            force_reprocess_this_item=force_reprocess_this_item,
            force_fetch_from_tmdb=force_fetch_from_tmdb
        )

        # --- 核心处理流程 ---
    
    # ---核心处理流程 ---
    def _process_item_core_logic_api_version(self, item_details_from_emby: Dict[str, Any], force_reprocess_this_item: bool, force_fetch_from_tmdb: bool = False):
        """
        【V-Final with Fast Path】
        引入“快速通道”逻辑。如果非强制刷新，则优先从黄金缓存中直接加载最终结果，
        极大提升“重新处理”任务的执行效率。
        """
        item_id = item_details_from_emby.get("Id")
        item_name_for_log = item_details_from_emby.get("Name", f"未知项目(ID:{item_id})")
        tmdb_id = item_details_from_emby.get("ProviderIds", {}).get("Tmdb")
        item_type = item_details_from_emby.get("Type")

        if not tmdb_id:
            logger.error(f"项目 '{item_name_for_log}' 缺少 TMDb ID，无法处理。")
            return False

        try:
            final_processed_cast = None
            douban_rating = None
            tmdb_details_for_extra = None

            # ======================================================================
            # 阶段 0: 快速通道检查 (Fast Path)
            # ======================================================================
            if not force_fetch_from_tmdb:
                logger.info(f"  -> [快速通道] 尝试从黄金缓存直接加载 '{item_name_for_log}' 的最终结果...")
                with get_central_db_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT actors_json, rating FROM media_metadata WHERE tmdb_id = %s AND item_type = %s", (tmdb_id, item_type))
                    cache_row = cursor.fetchone()
                    if cache_row and cache_row.get("actors_json"):
                        logger.info(f"  -> [快速通道] 成功命中缓存！将跳过所有数据采集和处理步骤。")
                        cached_actors = cache_row["actors_json"]
                        douban_rating = cache_row.get("rating") # 使用缓存中的评分
                        
                        # ★★★ 反查数据库，补全 emby_person_id ★★★
                        logger.debug("  -> [快速通道] 正在反查数据库以补全 emby_person_id...")
                        tmdb_ids_from_cache = [actor['id'] for actor in cached_actors if actor.get('id')]
                        
                        # 构建一个从 TMDb ID 到 Emby Person ID 的映射
                        tmdb_to_emby_map = {}
                        if tmdb_ids_from_cache:
                            # 使用 ANY(%s) 进行高效的批量查询
                            query = "SELECT tmdb_person_id, emby_person_id FROM person_identity_map WHERE tmdb_person_id = ANY(%s)"
                            cursor.execute(query, (tmdb_ids_from_cache,))
                            for row in cursor.fetchall():
                                tmdb_to_emby_map[str(row['tmdb_person_id'])] = row['emby_person_id']
                        
                        # 构建一个完美的 final_processed_cast
                        final_processed_cast = []
                        for actor_data in cached_actors:
                            actor_tmdb_id = actor_data.get("id")
                            if not actor_tmdb_id: continue
                            
                            # 补全 emby_person_id 和 provider_ids
                            actor_data['emby_person_id'] = tmdb_to_emby_map.get(str(actor_tmdb_id))
                            actor_data['provider_ids'] = {
                                "Tmdb": actor_tmdb_id,
                                # 注意：这里无法还原 Imdb 和 Douban ID，但这在写回逻辑中影响不大，
                                # 因为反哺数据库时会用 NULL 覆盖，而更新 Person 时主要依赖 TMDb ID。
                            }
                            final_processed_cast.append(actor_data)
                        
                        logger.debug(f"  -> [快速通道] emby_person_id 补全完成，共找到 {len(tmdb_to_emby_map)} 个映射。")

            # ======================================================================
            # 如果快速通道失败，则执行完整的“慢速通道”
            # ======================================================================
            if final_processed_cast is None:
                logger.info(f"  -> [慢速通道] 未命中缓存或强制刷新，开始执行完整处理流程...")
                # ======================================================================
                # 阶段 1: Emby 现状数据准备
                # ======================================================================
                all_emby_people = item_details_from_emby.get("People", [])
                non_actor_types = {"Director", "Writer", "Producer"}
                current_emby_cast_raw = [p for p in all_emby_people if p.get("Type") == "Actor" or (p.get("Type") not in non_actor_types and p.get("Role"))]
                enriched_emby_cast = self._enrich_cast_from_db_and_api(current_emby_cast_raw)

                # ======================================================================
                # 阶段 2: 权威数据源采集
                # ======================================================================
                authoritative_cast_source = []
                if self.tmdb_api_key:
                    if item_type == "Movie":
                        movie_details = tmdb_handler.get_movie_details(tmdb_id, self.tmdb_api_key)
                        if movie_details:
                            tmdb_details_for_extra = movie_details
                            credits_data = movie_details.get("credits") or movie_details.get("casts")
                            if credits_data: authoritative_cast_source = credits_data.get("cast", [])
                    elif item_type == "Series":
                        aggregated_tmdb_data = tmdb_handler.aggregate_full_series_data_from_tmdb(int(tmdb_id), self.tmdb_api_key)
                        if aggregated_tmdb_data:
                            tmdb_details_for_extra = aggregated_tmdb_data.get("series_details")
                            all_episodes = list(aggregated_tmdb_data.get("episodes_details", {}).values())
                            authoritative_cast_source = _aggregate_series_cast_from_tmdb_data(aggregated_tmdb_data["series_details"], all_episodes)
                
                authoritative_cast_map = {str(a.get("id")): a for a in authoritative_cast_source if a.get("id")}
                for emby_actor in enriched_emby_cast:
                    emby_tmdb_id = emby_actor.get("ProviderIds", {}).get("Tmdb")
                    if emby_tmdb_id and str(emby_tmdb_id) not in authoritative_cast_map:
                        converted_actor = {"id": emby_tmdb_id, "name": emby_actor.get("Name"), "character": emby_actor.get("Role"), "order": 999}
                        authoritative_cast_map[str(emby_tmdb_id)] = converted_actor
                final_authoritative_cast = list(authoritative_cast_map.values())

                # ======================================================================
                # 阶段 3: 豆瓣及后续处理
                # ======================================================================
                douban_cast_raw, douban_rating = self._get_douban_data_with_local_cache(item_details_from_emby)
                with get_central_db_connection() as conn:
                    cursor = conn.cursor()
                    final_processed_cast = self._process_cast_list_from_api(
                        tmdb_cast_people=final_authoritative_cast, emby_cast_people=enriched_emby_cast,
                        douban_cast_list=douban_cast_raw, item_details_from_emby=item_details_from_emby,
                        cursor=cursor, tmdb_api_key=self.tmdb_api_key, stop_event=self.get_stop_event()
                    )

            # ======================================================================
            # 最终收尾：调用封装好的写回函数
            # ======================================================================
            if final_processed_cast is not None:
                self._write_data_and_finalize(
                    item_details_from_emby=item_details_from_emby,
                    final_processed_cast=final_processed_cast,
                    douban_rating=douban_rating,
                    tmdb_details_for_extra=tmdb_details_for_extra
                )
            else:
                raise ValueError("处理流程结束，但未能生成有效的最终演员列表。")

        except (ValueError, InterruptedError) as e:
            logger.warning(f"处理 '{item_name_for_log}' 的过程中断: {e}")
            return False
        except Exception as outer_e:
            logger.error(f"API模式核心处理流程中发生未知严重错误 for '{item_name_for_log}': {outer_e}", exc_info=True)
            try:
                with get_central_db_connection() as conn_fail:
                    self.log_db_manager.save_to_failed_log(conn_fail.cursor(), item_id, item_name_for_log, f"核心处理异常: {str(outer_e)}", item_type)
            except Exception as log_e:
                logger.error(f"写入失败日志时再次发生错误: {log_e}")
            return False

        logger.info(f"✨✨✨ 处理完成 '{item_name_for_log}' ✨✨✨")
        return True

    # --- 核心处理器（暂时放弃新增演员） ---
    def _process_cast_list_from_api(self, tmdb_cast_people: List[Dict[str, Any]],
                                    emby_cast_people: List[Dict[str, Any]],
                                    douban_cast_list: List[Dict[str, Any]],
                                    item_details_from_emby: Dict[str, Any],
                                    cursor: psycopg2.extensions.cursor,
                                    tmdb_api_key: Optional[str],
                                    stop_event: Optional[threading.Event]) -> List[Dict[str, Any]]:
        """
        【V-Final with Discard Logic - Full Code】
        在函数开头增加一个“数据适配层”，将API数据转换为你现有逻辑期望的格式，
        并根据新规则丢弃无法在Emby中找到匹配的“真·新增”演员。
        """
        # ======================================================================
        # 步骤 1: ★★★ 数据适配 ★★★
        # ======================================================================
        logger.debug("  -> 开始演员数据适配 (反查缓存模式)...")
        
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

        logger.debug(f"  -> 数据适配完成，生成了 {len(final_cast_list)} 条基准演员数据。")
        
        # ======================================================================
        # 步骤 2: ★★★ “一对一匹配”逻辑 ★★★
        # ======================================================================
        douban_candidates = actor_utils.format_douban_cast(douban_cast_list)
        unmatched_local_actors = list(final_cast_list)
        merged_actors = []
        unmatched_douban_actors = []
        logger.debug(f"  -> 匹配阶段 1: 对号入座")
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
                    if d_actor.get("DoubanCelebrityId"):
                        l_actor["douban_id"] = d_actor.get("DoubanCelebrityId")
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
            logger.info("  -> 豆瓣API未返回演员或所有演员已匹配，跳过补充演员流程。")
        else:
            logger.info(f"  -> 发现 {len(unmatched_douban_actors)} 位潜在的豆瓣补充演员，开始执行匹配与筛选...")
            
            # ★★★ 新增逻辑 1/3: 从配置中读取上限值 ★★★
            limit = self.config.get(constants.CONFIG_OPTION_MAX_ACTORS_TO_PROCESS, 30)
            try:
                limit = int(limit)
                if limit <= 0: limit = 30
            except (ValueError, TypeError):
                limit = 30

            # ★★★ 新增逻辑 2/3: 在处理前进行前置检查 ★★★
            current_actor_count = len(final_cast_map)
            if current_actor_count >= limit:
                logger.info(f"  -> 当前演员数 ({current_actor_count}) 已达上限 ({limit})，将跳过所有豆瓣补充演员的流程。")
                # 由于已达上限，直接将所有未匹配的豆瓣演员标记为最终未匹配
                still_unmatched_final = unmatched_douban_actors
            else:
                logger.info(f"  -> 当前演员数 ({current_actor_count}) 低于上限 ({limit})，进入补充模式。")
                
                # --- 阶段 3.1: 用豆瓣ID查'演员映射表' ---
                logger.debug(f"  -> 匹配阶段 2: 用豆瓣ID查'演员映射表' ({len(unmatched_douban_actors)} 位演员)")
                still_unmatched = []
                for d_actor in unmatched_douban_actors:
                    if self.is_stop_requested(): raise InterruptedError("任务中止")
                    d_douban_id = d_actor.get("DoubanCelebrityId")
                    match_found = False
                    if d_douban_id:
                        entry_row = self._find_person_in_map_by_douban_id(d_douban_id, cursor)
                        entry = dict(entry_row) if entry_row else None
                        if entry and entry.get("tmdb_person_id") and entry.get("emby_person_id"):
                            tmdb_id_from_map = str(entry.get("tmdb_person_id"))
                            if tmdb_id_from_map not in final_cast_map:
                                logger.debug(f"  -> 匹配成功 (通过 豆瓣ID映射): 豆瓣演员 '{d_actor.get('Name')}' -> 加入最终演员表")
                                cached_metadata = self._get_actor_metadata_from_cache(tmdb_id_from_map, cursor) or {}
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

                # --- 阶段 3.2: 用IMDb ID进行最终匹配 ---
                logger.debug(f"  -> 匹配阶段 3: 用IMDb ID进行最终匹配和新增 ({len(unmatched_douban_actors)} 位演员)")
                still_unmatched_final = []
                # ★★★ 逻辑修改: 增加了 enumerate 以便记录日志 ★★★
                for i, d_actor in enumerate(unmatched_douban_actors):
                    if self.is_stop_requested(): raise InterruptedError("任务中止")
                    
                    # ★★★ 新增逻辑 3/3: 在循环内部再次检查上限 ★★★
                    if len(final_cast_map) >= limit:
                        logger.info(f"  -> 演员数已达上限 ({limit})，跳过剩余 {len(unmatched_douban_actors) - i} 位演员的API查询。")
                        still_unmatched_final.extend(unmatched_douban_actors[i:])
                        break # 跳出循环

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
                                logger.warning(f"  -> 解析 IMDb ID 时发生意外错误: {e_parse}")
                        
                        if d_imdb_id:
                            logger.debug(f"  -> 为 '{d_actor.get('Name')}' 获取到 IMDb ID: {d_imdb_id}，开始匹配...")
                            
                            # 路径A: 通过IMDb ID在本地映射表查找
                            entry_row_from_map = self._find_person_in_map_by_imdb_id(d_imdb_id, cursor)
                            entry_from_map = dict(entry_row_from_map) if entry_row_from_map else None
                            if entry_from_map and entry_from_map.get("tmdb_person_id") and entry_from_map.get("emby_person_id"):
                                tmdb_id_from_map = str(entry_from_map.get("tmdb_person_id"))
                                if tmdb_id_from_map not in final_cast_map:
                                    logger.debug(f"  -> 匹配成功 (通过 IMDb映射): 豆瓣演员 '{d_actor.get('Name')}' -> 加入最终演员表")
                                    new_actor_entry = {
                                        "id": tmdb_id_from_map, "name": d_actor.get("Name"),
                                        "character": d_actor.get("Role"), "order": 999, "imdb_id": d_imdb_id,
                                        "douban_id": d_douban_id, "emby_person_id": entry_from_map.get("emby_person_id")
                                    }
                                    final_cast_map[tmdb_id_from_map] = new_actor_entry
                                match_found = True
                            
                            # 路径B: 本地找不到，去TMDb API反查
                            if not match_found:
                                logger.debug(f"  -> 数据库未找到 {d_imdb_id} 的映射，开始通过 TMDb API 反查...")
                                if self.is_stop_requested(): raise InterruptedError("任务中止")
                                person_from_tmdb = tmdb_handler.find_person_by_external_id(
                                    external_id=d_imdb_id, api_key=self.tmdb_api_key, source="imdb_id"
                                )
                                if person_from_tmdb and person_from_tmdb.get("id"):
                                    tmdb_id_from_find = str(person_from_tmdb.get("id"))
                                    # 反查成功后，最关键的一步：用TMDb ID再查一次本地映射表
                                    final_check_row = self._find_person_in_map_by_tmdb_id(tmdb_id_from_find, cursor)
                                    if final_check_row and dict(final_check_row).get("emby_person_id"):
                                        emby_pid_from_final_check = dict(final_check_row).get("emby_person_id")
                                        if tmdb_id_from_find not in final_cast_map:
                                            logger.debug(f"  -> 匹配成功 (通过 TMDb反查): 豆瓣演员 '{d_actor.get('Name')}' -> 加入最终演员表")
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

                if still_unmatched_final:
                    discarded_names = [d.get('Name') for d in still_unmatched_final]
                    logger.info(f"  -> [丢弃新增] 以下 {len(still_unmatched_final)} 位豆瓣演员因在Emby中无匹配或已达数量上限而被丢弃: {', '.join(discarded_names)}")
        
        # ======================================================================
        # 步骤 4: ★★★ 整合、排序、翻译和格式化 ★★★
        # ======================================================================
        current_cast_list = list(final_cast_map.values())
        cast_to_process = current_cast_list
        cast_to_process.sort(key=lambda x: x.get('order') if x.get('order') is not None and x.get('order') >= 0 else 999)

        logger.info(f"  -> 将对 {len(cast_to_process)} 位演员进行最终的翻译和格式化处理...")

        if not (self.ai_translator and self.config.get(constants.CONFIG_OPTION_AI_TRANSLATION_ENABLED, False)):
            logger.info("  -> AI翻译未启用，将保留演员和角色名原文。")
        else:
            final_translation_map = {}
            terms_to_translate = set()
            for actor in cast_to_process:
                character = actor.get('character')
                if character:
                    cleaned_character = utils.clean_character_name_static(character)
                    if cleaned_character and not utils.contains_chinese(cleaned_character):
                        terms_to_translate.add(cleaned_character)
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
                if cached_results:
                    final_translation_map.update(cached_results)
                if terms_for_api:
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
                transliterate_results = self.ai_translator.batch_translate(remaining_terms, mode='transliterate')
                final_translation_map.update(transliterate_results)
                still_failed_terms = []
                for term in remaining_terms:
                    if not utils.contains_chinese(final_translation_map.get(term, term)):
                        still_failed_terms.append(term)
                remaining_terms = still_failed_terms
            if remaining_terms:
                item_title = item_details_from_emby.get("Name")
                item_year = item_details_from_emby.get("ProductionYear")
                quality_results = self.ai_translator.batch_translate(remaining_terms, mode='quality', title=item_title, year=item_year)
                final_translation_map.update(quality_results)
            for actor in cast_to_process:
                original_character = actor.get('character')
                if original_character:
                    cleaned_character = utils.clean_character_name_static(original_character)
                    actor['character'] = final_translation_map.get(cleaned_character, cleaned_character)
                else:
                    actor['character'] = ''

        tmdb_to_emby_id_map = {
            str(actor.get('id')): actor.get('emby_person_id')
            for actor in cast_to_process if actor.get('id') and actor.get('emby_person_id')
        }
        genres = item_details_from_emby.get("Genres", [])
        is_animation = "Animation" in genres or "动画" in genres or "Documentary" in genres or "纪录" in genres
        final_cast_perfect = actor_utils.format_and_complete_cast_list(
            cast_to_process, is_animation, self.config, mode='auto'
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

        return final_cast_perfect

    # --- 核心处理器 (备用代码，等4.9正式版恢复)---
    # def _process_cast_list_from_api(self, tmdb_cast_people: List[Dict[str, Any]],
    #                                 emby_cast_people: List[Dict[str, Any]],
    #                                 douban_cast_list: List[Dict[str, Any]],
    #                                 item_details_from_emby: Dict[str, Any],
    #                                 cursor: psycopg2.extensions.cursor,
    #                                 tmdb_api_key: Optional[str],
    #                                 stop_event: Optional[threading.Event]) -> List[Dict[str, Any]]:
    #     """
    #     在函数开头增加一个“数据适配层”，将API数据转换为你现有逻辑期望的格式，
    #     """
    #     # ======================================================================
    #     # 步骤 1: ★★★ 数据适配 ★★★
    #     # ======================================================================
    #     logger.debug("  -> 开始演员数据适配 (反查缓存模式)...")
        
    #     # 1. 为TMDb演员列表创建高效的查找地图
    #     tmdb_actor_map_by_id = {str(actor.get("id")): actor for actor in tmdb_cast_people}
    #     tmdb_actor_map_by_en_name = {str(actor.get("name") or "").lower().strip(): actor for actor in tmdb_cast_people}

    #     # 2. 遍历Emby演员列表，为每个人找到其在TMDb中的“真身”
    #     final_cast_list = []
    #     used_tmdb_ids = set()

    #     for emby_actor in emby_cast_people:
    #         emby_person_id = emby_actor.get("Id")
    #         emby_tmdb_id = emby_actor.get("ProviderIds", {}).get("Tmdb")
    #         emby_name_lower = str(emby_actor.get("Name") or "").lower().strip()

    #         tmdb_match = None

    #         # 2.1 优先使用ID精确匹配
    #         if emby_tmdb_id and emby_tmdb_id in tmdb_actor_map_by_id:
    #             tmdb_match = tmdb_actor_map_by_id[emby_tmdb_id]
    #             logger.trace(f"  -> [适配] Emby演员 '{emby_actor.get('Name')}' 通过TMDb ID '{emby_tmdb_id}' 精确匹配。")
            
    #         # 2.2 ID匹配失败（黑户场景），启动“反查缓存”+“直接匹配”双重后备方案
    #         else:
    #             # 后备方案 A: 直接用Emby的名字（可能是中文）去TMDb的英文名里碰一下运气
    #             if emby_name_lower in tmdb_actor_map_by_en_name:
    #                 tmdb_match = tmdb_actor_map_by_en_name[emby_name_lower]
    #                 logger.info(f"  -> [适配] Emby演员 '{emby_actor.get('Name')}' 通过直接名字匹配到TMDb演员。")
                
    #             # 后备方案 B: 如果直接匹配不上（名字是中文），则执行您提出的“反查缓存”
    #             else:
    #                 # get_translation_from_db 需要一个 by_translated_text=True 的参数
    #                 cache_entry = self.actor_db_manager.get_translation_from_db(cursor, emby_actor.get("Name"), by_translated_text=True)
    #                 if cache_entry and cache_entry.get('original_text'):
    #                     original_en_name = str(cache_entry['original_text']).lower().strip()
    #                     if original_en_name in tmdb_actor_map_by_en_name:
    #                         tmdb_match = tmdb_actor_map_by_en_name[original_en_name]
    #                         logger.info(f"  -> [适配] Emby演员 '{emby_actor.get('Name')}' 通过【反查缓存】(原名: '{original_en_name}') 成功匹配到TMDb演员。")

    #         # 2.3 如果找到匹配，则合并信息
    #         if tmdb_match:
    #             tmdb_id_str = str(tmdb_match.get("id"))
                
    #             merged_actor = tmdb_match.copy()
    #             merged_actor["emby_person_id"] = emby_person_id
    #             # 优先使用Emby中可能已翻译好的名字，但如果Emby是英文而TMDb是中文，则用TMDb的
    #             # 这是一个小优化，确保名字总是最优的
    #             if utils.contains_chinese(emby_actor.get("Name")):
    #                 merged_actor["name"] = emby_actor.get("Name")
    #             else:
    #                 merged_actor["name"] = tmdb_match.get("name") # 如果Emby是英文，回退到TMDb的名字

    #             merged_actor["character"] = emby_actor.get("Role")
                
    #             final_cast_list.append(merged_actor)
    #             used_tmdb_ids.add(tmdb_id_str)
    #         else:
    #             logger.warning(f"  -> [适配] Emby演员 '{emby_actor.get('Name')}' (ID: {emby_person_id}) 在TMDb列表中未找到任何匹配，将被丢弃。")

    #     # 2.4 处理在TMDb中有，但在Emby中完全没有的演员
    #     for tmdb_id, tmdb_actor_data in tmdb_actor_map_by_id.items():
    #         if tmdb_id not in used_tmdb_ids:
    #             new_actor = tmdb_actor_data.copy()
    #             new_actor["emby_person_id"] = None
    #             final_cast_list.append(new_actor)
    #             logger.info(f"  -> [适配] 发现TMDb演员 '{new_actor.get('name')}' 在Emby中完全不存在，将尝试新增。")

    #     logger.debug(f"  -> 数据适配完成，生成了 {len(final_cast_list)} 条基准演员数据。")
    #     # ======================================================================
    #     # 步骤 2: ★★★ “一对一匹配”逻辑 ★★★
    #     # ======================================================================

    #     douban_candidates = actor_utils.format_douban_cast(douban_cast_list)

    #     unmatched_local_actors = list(final_cast_list)  # ★★★ 使用我们适配好的数据源 ★★★
    #     merged_actors = []
    #     unmatched_douban_actors = []
    #     #  遍历豆瓣演员，尝试在“未匹配”的本地演员中寻找配对
    #     logger.debug(f" --- 匹配阶段 1: 对号入座 ---")
    #     for d_actor in douban_candidates:
    #         douban_name_zh = d_actor.get("Name", "").lower().strip()
    #         douban_name_en = d_actor.get("OriginalName", "").lower().strip()

    #         match_found_for_this_douban_actor = False
            
    #         for i, l_actor in enumerate(unmatched_local_actors):
    #             local_name = str(l_actor.get("name") or "").lower().strip()
    #             local_original_name = str(l_actor.get("original_name") or "").lower().strip()
    #             is_match, match_reason = False, ""
    #             if douban_name_zh and (douban_name_zh == local_name or douban_name_zh == local_original_name):
    #                 is_match, match_reason = True, "精确匹配 (豆瓣中文名)"
    #             elif douban_name_en and (douban_name_en == local_name or douban_name_en == local_original_name):
    #                 is_match, match_reason = True, "精确匹配 (豆瓣外文名)"
                
    #             if is_match:
    #                 logger.debug(f"  -> 匹配成功： (对号入座): 豆瓣演员 '{d_actor.get('Name')}' -> 本地演员 '{l_actor.get('name')}' (ID: {l_actor.get('id')})")

    #                 l_actor["name"] = d_actor.get("Name")
    #                 cleaned_douban_character = utils.clean_character_name_static(d_actor.get("Role"))
    #                 l_actor["character"] = actor_utils.select_best_role(l_actor.get("character"), cleaned_douban_character)
    #                 if d_actor.get("DoubanCelebrityId"):
    #                     l_actor["douban_id"] = d_actor.get("DoubanCelebrityId")

    #                 merged_actors.append(unmatched_local_actors.pop(i))
    #                 match_found_for_this_douban_actor = True
    #                 break

    #         if not match_found_for_this_douban_actor:
    #             unmatched_douban_actors.append(d_actor)

    #     # 1. 先将已有的演员（匹配合并的 + 未匹配的本地演员）构成当前的演员列表基础
    #     current_cast_list = merged_actors + unmatched_local_actors
    #     final_cast_map = {str(actor['id']): actor for actor in current_cast_list if actor.get('id') and str(actor.get('id')) != 'None'}

    #     # 2. 检查是否还有未匹配的豆瓣演员需要处理（即是否需要进入“新增演员”流程）
    #     if not unmatched_douban_actors:
    #         # 如果豆瓣API失败，unmatched_douban_actors 列表会是空的，直接进入这里
    #         logger.info("  -> 豆瓣API未返回演员或所有演员已匹配，跳过新增演员流程，直接进入翻译阶段。")
    #     else:
    #         # 只有在有未匹配的豆瓣演员时，才执行复杂的新增逻辑
    #         logger.info(f"  -> 发现 {len(unmatched_douban_actors)} 位潜在的新增演员，开始执行新增流程...")
            
    #         # (将原有的新增逻辑整体放入这个 else 块中)
    #         limit = self.config.get(constants.CONFIG_OPTION_MAX_ACTORS_TO_PROCESS, 30)
    #         try:
    #             limit = int(limit)
    #             if limit <= 0: limit = 30
    #         except (ValueError, TypeError):
    #             limit = 30

    #         current_actor_count = len(final_cast_map)
    #         if current_actor_count >= limit:
    #             logger.info(f"  -> 当前演员数 ({current_actor_count}) 已达上限 ({limit})，跳过所有新增演员的流程。")
    #         else:
    #             logger.info(f"  -> 当前演员数 ({current_actor_count}) 低于上限 ({limit})，进入补充模式（处理来自豆瓣的新增演员）。")
    #             logger.debug(f" --- 匹配阶段 2: 用豆瓣ID查'演员映射表' ({len(unmatched_douban_actors)} 位演员) ---")
    #             still_unmatched = []
    #             for d_actor in unmatched_douban_actors:
    #                 if self.is_stop_requested(): raise InterruptedError("任务中止")
    #                 d_douban_id = d_actor.get("DoubanCelebrityId")
    #                 match_found = False
    #                 if d_douban_id:
    #                     entry_row = self._find_person_in_map_by_douban_id(d_douban_id, cursor)
    #                     entry = dict(entry_row) if entry_row else None
    #                     if entry and entry.get("tmdb_person_id"):
    #                         tmdb_id_from_map = str(entry.get("tmdb_person_id"))
    #                         if tmdb_id_from_map not in final_cast_map:
    #                             logger.debug(f"  -> 匹配成功 (通过 豆瓣ID映射): 豆瓣演员 '{d_actor.get('Name')}' -> 加入最终演员表")
    #                             cached_metadata = self._get_actor_metadata_from_cache(tmdb_id_from_map, cursor) or {}
    #                             new_actor_entry = {
    #                                 "id": tmdb_id_from_map, "name": d_actor.get("Name"),
    #                                 "original_name": cached_metadata.get("original_name") or d_actor.get("OriginalName"),
    #                                 "character": d_actor.get("Role"), "adult": cached_metadata.get("adult", False),
    #                                 "gender": cached_metadata.get("gender", 0), "known_for_department": "Acting",
    #                                 "popularity": cached_metadata.get("popularity", 0.0), "profile_path": cached_metadata.get("profile_path"),
    #                                 "cast_id": None, "credit_id": None, "order": 999,
    #                                 "imdb_id": entry.get("imdb_id"), "douban_id": d_douban_id,
    #                                 "emby_person_id": entry.get("emby_person_id"), "_is_newly_added": True
    #                             }
    #                             final_cast_map[tmdb_id_from_map] = new_actor_entry
    #                         match_found = True
    #                 if not match_found:
    #                     still_unmatched.append(d_actor)
    #             unmatched_douban_actors = still_unmatched

    #             logger.debug(f" --- 匹配阶段 3: 用IMDb ID进行最终匹配和新增 ({len(unmatched_douban_actors)} 位演员) ---")
    #             still_unmatched_final = []
    #             for i, d_actor in enumerate(unmatched_douban_actors):
    #                 if self.is_stop_requested(): raise InterruptedError("任务中止")
    #                 if len(final_cast_map) >= limit:
    #                     logger.info(f"  -> 演员数已达上限 ({limit})，跳过剩余 {len(unmatched_douban_actors) - i} 位演员的API查询。")
    #                     still_unmatched_final.extend(unmatched_douban_actors[i:])
    #                     break
    #                 d_douban_id = d_actor.get("DoubanCelebrityId")
    #                 match_found = False
    #                 if d_douban_id and self.douban_api and self.tmdb_api_key:
    #                     if self.is_stop_requested(): raise InterruptedError("任务中止")
    #                     details = self.douban_api.celebrity_details(d_douban_id)
    #                     time_module.sleep(0.3)
    #                     d_imdb_id = None
    #                     if details and not details.get("error"):
    #                         try:
    #                             info_list = details.get("extra", {}).get("info", [])
    #                             if isinstance(info_list, list):
    #                                 for item in info_list:
    #                                     if isinstance(item, list) and len(item) == 2 and item[0] == 'IMDb编号':
    #                                         d_imdb_id = item[1]
    #                                         break
    #                         except Exception as e_parse:
    #                             logger.warning(f"  -> 解析 IMDb ID 时发生意外错误: {e_parse}")
    #                     if d_imdb_id:
    #                         logger.debug(f"  -> 为 '{d_actor.get('Name')}' 获取到 IMDb ID: {d_imdb_id}，开始匹配...")
    #                         entry_row_from_map = self._find_person_in_map_by_imdb_id(d_imdb_id, cursor)
    #                         entry_from_map = dict(entry_row_from_map) if entry_row_from_map else None
    #                         if entry_from_map and entry_from_map.get("tmdb_person_id"):
    #                             tmdb_id_from_map = str(entry_from_map.get("tmdb_person_id"))
    #                             if tmdb_id_from_map not in final_cast_map:
    #                                 logger.debug(f"  -> 匹配成功 (通过 IMDb映射): 豆瓣演员 '{d_actor.get('Name')}' -> 加入最终演员表")
    #                                 cached_metadata = self._get_actor_metadata_from_cache(tmdb_id_from_map, cursor) or {}
    #                                 new_actor_entry = {
    #                                     "id": tmdb_id_from_map, "name": d_actor.get("Name"),
    #                                     "original_name": cached_metadata.get("original_name") or d_actor.get("OriginalName"),
    #                                     "character": d_actor.get("Role"), "order": 999, "imdb_id": d_imdb_id,
    #                                     "douban_id": d_douban_id, "emby_person_id": entry_from_map.get("emby_person_id"),
    #                                     "_is_newly_added": True
    #                                 }
    #                                 final_cast_map[tmdb_id_from_map] = new_actor_entry
    #                             match_found = True
    #                         if not match_found:
    #                             logger.debug(f"  -> 数据库未找到 {d_imdb_id} 的映射，开始通过 TMDb API 反查...")
    #                             if self.is_stop_requested(): raise InterruptedError("任务中止")
    #                             name_for_verification = d_actor.get("OriginalName")
    #                             log_source = "豆瓣"
    #                             if entry_from_map and entry_from_map.get("tmdb_person_id"):
    #                                 tmdb_id_from_map = str(entry_from_map.get("tmdb_person_id"))
    #                                 cached_metadata = self._get_actor_metadata_from_cache(tmdb_id_from_map, cursor)
    #                                 if cached_metadata and cached_metadata.get("original_name"):
    #                                     name_for_verification = cached_metadata.get("original_name")
    #                                     log_source = "本地数据库"
    #                                     logger.debug(f"  -> [验证准备] 成功从本地数据库为 TMDb ID {tmdb_id_from_map} 找到用于验证的 original_name: '{name_for_verification}'")
    #                             logger.debug(f"  -> 将使用来自 [{log_source}] 的外文名 '{name_for_verification}' 进行 TMDb API 匹配验证。")
    #                             names_to_verify = {"chinese_name": d_actor.get("Name"), "original_name": name_for_verification}
    #                             person_from_tmdb = tmdb_handler.find_person_by_external_id(
    #                                 external_id=d_imdb_id, api_key=self.tmdb_api_key, source="imdb_id",
    #                                 names_for_verification=names_to_verify
    #                             )
    #                             if person_from_tmdb and person_from_tmdb.get("id"):
    #                                 tmdb_id_from_find = str(person_from_tmdb.get("id"))
    #                                 if tmdb_id_from_find not in final_cast_map:
    #                                     logger.debug(f"  -> 匹配成功 (通过 TMDb反查): 豆瓣演员 '{d_actor.get('Name')}' -> 加入最终演员表")
    #                                     emby_pid_from_final_check = None
    #                                     final_check_row = self._find_person_in_map_by_tmdb_id(tmdb_id_from_find, cursor)
    #                                     if final_check_row:
    #                                         final_check_entry = dict(final_check_row)
    #                                         emby_pid_from_final_check = final_check_entry.get("emby_person_id")
    #                                         if emby_pid_from_final_check:
    #                                             logger.trace(f"  -> [最终检查] 发现该TMDB ID已关联Emby Person ID: {emby_pid_from_final_check}")
    #                                     cached_metadata = self._get_actor_metadata_from_cache(tmdb_id_from_find, cursor) or {}
    #                                     new_actor_entry = {
    #                                         "id": tmdb_id_from_find, "name": d_actor.get("Name"),
    #                                         "original_name": cached_metadata.get("original_name") or d_actor.get("OriginalName"),
    #                                         "character": d_actor.get("Role"), "adult": cached_metadata.get("adult", False),
    #                                         "gender": cached_metadata.get("gender", 0), "known_for_department": "Acting",
    #                                         "popularity": cached_metadata.get("popularity", 0.0), "profile_path": cached_metadata.get("profile_path"),
    #                                         "cast_id": None, "credit_id": None, "order": 999,
    #                                         "imdb_id": d_imdb_id, "douban_id": d_douban_id,
    #                                         "emby_person_id": emby_pid_from_final_check, "_is_newly_added": True
    #                                     }
    #                                     final_cast_map[tmdb_id_from_find] = new_actor_entry
                                        
    #                                 match_found = True
    #                 if not match_found:
    #                     still_unmatched_final.append(d_actor)
    #             if still_unmatched_final:
    #                 discarded_names = [d.get('Name') for d in still_unmatched_final]
    #                 logger.info(f"  -> 最终丢弃 {len(still_unmatched_final)} 位豆瓣演员 ---")
    #             unmatched_douban_actors = still_unmatched_final
        
    #     # 3. 无论是否执行了新增，都从 final_cast_map 中获取最终的演员列表
    #     current_cast_list = list(final_cast_map.values())

    #     # 【修改后】不再截断最终列表，直接处理所有演员
    #     cast_to_process = current_cast_list
    #     # 按 order 排序，确保重要演员在前
    #     cast_to_process.sort(key=lambda x: x.get('order') if x.get('order') is not None and x.get('order') >= 0 else 999)

    #     logger.info(f"  -> 将对 {len(cast_to_process)} 位演员进行最终的翻译和格式化处理...")

    #     # ======================================================================
    #     # 步骤 4: ★★★ 三级翻译流程 ★★★
    #     # ======================================================================
    #     if not (self.ai_translator and self.config.get(constants.CONFIG_OPTION_AI_TRANSLATION_ENABLED, False)):
    #         logger.info("  -> AI翻译未启用，将保留演员和角色名原文。")
    #     else:
    #         # --- 数据准备 ---
    #         final_translation_map = {} # 存储所有最终的翻译结果
            
    #         # 1. 收集所有需要翻译的词条
    #         terms_to_translate = set()
    #         for actor in cast_to_process:
    #             name = actor.get('name')
    #             if name and not utils.contains_chinese(name):
    #                 terms_to_translate.add(name)
    #             character = actor.get('character')
    #             if character:
    #                 cleaned_character = utils.clean_character_name_static(character)
    #                 if cleaned_character and not utils.contains_chinese(cleaned_character):
    #                     terms_to_translate.add(cleaned_character)
            
    #         remaining_terms = list(terms_to_translate)

    #         # --- 🚀 第一级: 翻译官模式 (带全局缓存) ---
    #         if remaining_terms:
    #             logger.info(f"--- 第一级翻译开始: 快速模式处理 {len(remaining_terms)} 个词条 ---")
                
    #             # 1.1 查缓存
    #             cached_results = {}
    #             terms_for_api = []
    #             for term in remaining_terms:
    #                 cached = self.actor_db_manager.get_translation_from_db(cursor, term)
    #                 if cached and cached.get('translated_text'):
    #                     cached_results[term] = cached['translated_text']
    #                 else:
    #                     terms_for_api.append(term)
                
    #             if cached_results:
    #                 final_translation_map.update(cached_results)
    #                 logger.info(f"  -> 从数据库缓存命中 {len(cached_results)} 个词条。")

    #             # 1.2 调API
    #             if terms_for_api:
    #                 logger.info(f"  -> 将 {len(terms_for_api)} 个词条提交给AI (模式: fast)...")
    #                 fast_api_results = self.ai_translator.batch_translate(terms_for_api, mode='fast')
                    
    #                 # 1.3 处理API结果并回写缓存
    #                 for term, translation in fast_api_results.items():
    #                     final_translation_map[term] = translation
    #                     self.actor_db_manager.save_translation_to_db(cursor, term, translation, self.ai_translator.provider)

    #             # 1.4 筛选失败者
    #             failed_terms = []
    #             for term in remaining_terms:
    #                 if not utils.contains_chinese(final_translation_map.get(term, term)):
    #                     failed_terms.append(term)
                
    #             remaining_terms = failed_terms
    #             if remaining_terms:
    #                 logger.warning(f"快速模式后，仍有 {len(remaining_terms)} 个词条未翻译成中文，进入二级翻译流程。")

    #         # --- 🚀 第二级: 强制音译模式 ---
    #         if remaining_terms:
    #             logger.info(f"--- 第二级翻译开始: 强制音译模式处理 {len(remaining_terms)} 个专有名词 ---")
    #             transliterate_results = self.ai_translator.batch_translate(remaining_terms, mode='transliterate')
                
    #             final_translation_map.update(transliterate_results) # 直接更新最终结果
                
    #             still_failed_terms = []
    #             for term in remaining_terms:
    #                 if not utils.contains_chinese(final_translation_map.get(term, term)):
    #                     still_failed_terms.append(term)
                
    #             remaining_terms = still_failed_terms
    #             if remaining_terms:
    #                 logger.warning(f"音译模式后，仍有 {len(remaining_terms)} 个顽固词条，将启动三级最终的顾问模式。")

    #         # --- 🚀 第三级翻译: 全上下文顾问模式 ---
    #         if remaining_terms:
    #             logger.info(f"--- 第三级翻译开始: 顾问模式处理 {len(remaining_terms)} 个最棘手的词条 ---")
    #             item_title = item_details_from_emby.get("Name")
    #             item_year = item_details_from_emby.get("ProductionYear")
    #             quality_results = self.ai_translator.batch_translate(remaining_terms, mode='quality', title=item_title, year=item_year)
    #             final_translation_map.update(quality_results) # 最终信任顾问的结果
            
    #         # --- 应用所有翻译结果 ---
    #         logger.info("------------ AI翻译流程成功，开始应用结果 ------------")
    #         for actor in cast_to_process:
    #             original_name = actor.get('name')
    #             actor['name'] = final_translation_map.get(original_name, original_name)
                
    #             original_character = actor.get('character')
    #             if original_character:
    #                 cleaned_character = utils.clean_character_name_static(original_character)
    #                 actor['character'] = final_translation_map.get(cleaned_character, cleaned_character)
    #             else:
    #                 actor['character'] = ''
    #         logger.info("----------------------------------------------------")

    #     # ======================================================================
    #     # 步骤 5: 格式化最终演员表
    #     # ======================================================================
    #     # 5.1: 在调用黑盒函数前，备份所有 emby_person_id
    #     logger.trace("格式化前：备份 emby_person_id...")
    #     tmdb_to_emby_id_map = {
    #         str(actor.get('id')): actor.get('emby_person_id')
    #         for actor in cast_to_process if actor.get('id') and actor.get('emby_person_id')
    #     }
    #     logger.trace(f"已备份 {len(tmdb_to_emby_id_map)} 个 Emby Person ID 映射。")
        
    #     # 5.2: 正常调用格式化函数 (黑盒)
    #     logger.trace("调用 actor_utils.format_and_complete_cast_list 进行格式化...")
        
    #     genres = item_details_from_emby.get("Genres", [])
    #     is_animation = "Animation" in genres or "动画" in genres or "Documentary" in genres or "纪录" in genres
        
    #     final_cast_perfect = actor_utils.format_and_complete_cast_list(
    #         cast_to_process, is_animation, self.config, mode='auto'
    #     )

    #     # 5.3: 格式化后，将备份的 emby_person_id 重新注入
    #     logger.trace("格式化后：恢复 emby_person_id...")
    #     restored_count = 0
    #     for actor in final_cast_perfect:
    #         tmdb_id_str = str(actor.get("id"))
    #         if tmdb_id_str in tmdb_to_emby_id_map:
    #             actor["emby_person_id"] = tmdb_to_emby_id_map[tmdb_id_str]
    #             restored_count += 1
    #     logger.trace(f"已为 {restored_count} 位演员恢复了 Emby Person ID。")

    #     # 5.4: 准备最终的 provider_ids
    #     logger.trace("准备最终的 provider_ids...")
    #     for actor in final_cast_perfect:
    #         actor["provider_ids"] = {
    #             "Tmdb": str(actor.get("id")),
    #             "Imdb": actor.get("imdb_id"),
    #             "Douban": actor.get("douban_id")
    #         }
    #         if actor.get("emby_person_id"):
    #             logger.trace(f"  演员 '{actor.get('name')}' 最终保留了 Emby Person ID: {actor.get('emby_person_id')}")

    #     return final_cast_perfect

    def process_full_library(self, update_status_callback: Optional[callable] = None, force_reprocess_all: bool = False, force_fetch_from_tmdb: bool = False):
        """
        【V3 - 最终完整版】
        这是所有全量处理的唯一入口，它自己处理所有与“强制”相关的逻辑。
        """
        self.clear_stop_signal()
        
        logger.trace(f"进入核心执行层: process_full_library, 接收到的 force_reprocess_all = {force_reprocess_all}, force_fetch_from_tmdb = {force_fetch_from_tmdb}")

        if force_reprocess_all:
            logger.info("  -> 检测到“强制重处理”选项，正在清空已处理日志...")
            try:
                self.clear_processed_log()
            except Exception as e:
                logger.error(f"在 process_full_library 中清空日志失败: {e}", exc_info=True)
                if update_status_callback: update_status_callback(-1, "清空日志失败")
                return

        # --- ★★★ 补全了这部分代码 ★★★ ---
        libs_to_process_ids = self.config.get("libraries_to_process", [])
        if not libs_to_process_ids:
            logger.warning("  -> 未在配置中指定要处理的媒体库。")
            return

        logger.info("  -> 正在尝试从Emby获取媒体项目...")
        all_emby_libraries = emby_handler.get_emby_libraries(self.emby_url, self.emby_api_key, self.emby_user_id) or []
        library_name_map = {lib.get('Id'): lib.get('Name', '未知库名') for lib in all_emby_libraries}
        
        movies = emby_handler.get_emby_library_items(self.emby_url, self.emby_api_key, "Movie", self.emby_user_id, libs_to_process_ids, library_name_map=library_name_map) or []
        series = emby_handler.get_emby_library_items(self.emby_url, self.emby_api_key, "Series", self.emby_user_id, libs_to_process_ids, library_name_map=library_name_map) or []
        
        if movies:
            source_movie_lib_names = sorted(list({library_name_map.get(item.get('_SourceLibraryId')) for item in movies if item.get('_SourceLibraryId')}))
            logger.info(f"  -> 从媒体库【{', '.join(source_movie_lib_names)}】获取到 {len(movies)} 个电影项目。")

        if series:
            source_series_lib_names = sorted(list({library_name_map.get(item.get('_SourceLibraryId')) for item in series if item.get('_SourceLibraryId')}))
            logger.info(f"  -> 从媒体库【{', '.join(source_series_lib_names)}】获取到 {len(series)} 个电视剧项目。")

        all_items = movies + series
        total = len(all_items)
        # --- ★★★ 补全结束 ★★★ ---
        
        if total == 0:
            logger.info("  -> 在所有选定的库中未找到任何可处理的项目。")
            if update_status_callback: update_status_callback(100, "未找到可处理的项目。")
            return

        # --- 新增：清理已删除的媒体项 ---
        if update_status_callback: update_status_callback(20, "正在检查并清理已删除的媒体项...")
        
        with get_central_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT item_id, item_name FROM processed_log")
            processed_log_entries = cursor.fetchall()
            
            processed_ids_in_db = {entry['item_id'] for entry in processed_log_entries}
            emby_ids_in_library = {item.get('Id') for item in all_items if item.get('Id')}
            
            # 找出在 processed_log 中但不在 Emby 媒体库中的项目
            deleted_items_to_clean = processed_ids_in_db - emby_ids_in_library
            
            if deleted_items_to_clean:
                logger.info(f"  -> 发现 {len(deleted_items_to_clean)} 个已从 Emby 媒体库删除的项目，正在从 '已处理' 中移除...")
                for deleted_item_id in deleted_items_to_clean:
                    self.log_db_manager.remove_from_processed_log(cursor, deleted_item_id)
                    # 同时从内存缓存中移除
                    if deleted_item_id in self.processed_items_cache:
                        del self.processed_items_cache[deleted_item_id]
                    logger.debug(f"  -> 已从 '已处理' 中移除 ItemID: {deleted_item_id}")
                conn.commit()
                logger.info("  -> 已删除媒体项的清理工作完成。")
            else:
                logger.info("  -> 未发现需要从 '已处理' 中清理的已删除媒体项。")
        
        if update_status_callback: update_status_callback(30, "已删除媒体项清理完成，开始处理现有媒体...")

        # --- 现有媒体项处理循环 ---
        for i, item in enumerate(all_items):
            if self.is_stop_requested():
                logger.warning("全库扫描任务已被用户中止。")
                break # 使用 break 优雅地退出循环
            
            item_id = item.get('Id')
            item_name = item.get('Name', f"ID:{item_id}")

            if not force_reprocess_all and item_id in self.processed_items_cache:
                logger.info(f"  -> 正在跳过已处理的项目: {item_name}")
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
                force_reprocess_this_item=force_reprocess_all,
                force_fetch_from_tmdb=force_fetch_from_tmdb
            )
            
            time_module.sleep(float(self.config.get("delay_between_items_sec", 0.5)))
        
        if not self.is_stop_requested() and update_status_callback:
            update_status_callback(100, "全量处理完成")
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
        if not self.ai_translator or not self.config.get(constants.CONFIG_OPTION_AI_TRANSLATION_ENABLED, False):
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

        # ★★★ 核心修改 2: 彻底删除降级逻辑 ★★★
        # 原有的 if not ai_translation_succeeded: ... else ... 代码块已全部移除。

        logger.info("手动编辑-翻译完成。")
        return translated_cast
    
    # ✨✨✨手动处理✨✨✨
    def process_item_with_manual_cast(self, item_id: str, manual_cast_list: List[Dict[str, Any]], item_name: str) -> bool:
        """
        【V-API-Direct - 功能增强最终版】
        完全信任前端提交的演员列表，直接写入Emby，并集成了翻译缓存更新和剧集分集处理功能。
        """
        logger.info(f"  -> 手动处理流程启动：ItemID: {item_id} ('{item_name}')")
        
        try:
            # ======================================================================
            # 步骤 1: 数据准备与转换
            # ======================================================================
            logger.debug(f"接收到前端提交的 {len(manual_cast_list)} 位演员数据，开始转换...")
            
            # 1.1 获取媒体详情，这是后续所有操作的基础
            item_details = emby_handler.get_emby_item_details(item_id, self.emby_url, self.emby_api_key, self.emby_user_id)
            if not item_details:
                raise ValueError(f"无法获取项目 {item_id} 的详情。")
            item_type = item_details.get("Type")

            # 1.2 将前端数据转换为 Emby Handler 需要的格式
            cast_for_emby_handler = []
            for actor in manual_cast_list:
                emby_pid = actor.get("emby_person_id")
                if not emby_pid:
                    logger.warning(f"跳过演员 '{actor.get('name')}'，因为缺少 emby_person_id。")
                    continue
                cast_for_emby_handler.append({
                    "name": actor.get("name"),
                    "character": actor.get("role"),
                    "emby_person_id": emby_pid,
                    "provider_ids": {"Tmdb": actor.get("tmdbId")}
                })

            # ======================================================================
            # ★★★ 新增功能 1: 更新翻译缓存 ★★★
            # ======================================================================
            try:
                # 1. 从内存缓存中获取这个会话的完整原始演员列表
                original_full_cast = self.manual_edit_cache.get(item_id)
                if original_full_cast:
                    # 构建一个以 emby_person_id 为键的原始数据映射表
                    original_cast_map = {str(actor.get('emby_person_id')): actor for actor in original_full_cast}
                    
                    with get_central_db_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute("BEGIN TRANSACTION;")
                        try:
                            for actor_from_frontend in manual_cast_list:
                                emby_pid = actor_from_frontend.get("emby_person_id")
                                if not emby_pid: continue
                                
                                original_actor_data = original_cast_map.get(str(emby_pid))
                                if not original_actor_data: continue

                                new_role = actor_from_frontend.get('role', '')
                                original_role = original_actor_data.get('character', '')
                                
                                # 只有当角色名发生变化时才尝试更新缓存
                                if new_role != original_role:
                                    cleaned_original_role = utils.clean_character_name_static(original_role)
                                    cleaned_new_role = utils.clean_character_name_static(new_role)
                                    
                                    if cleaned_new_role and cleaned_new_role != cleaned_original_role:
                                        cache_entry = self.actor_db_manager.get_translation_from_db(
                                            text=cleaned_original_role, by_translated_text=True, cursor=cursor
                                        )
                                        if cache_entry and 'original_text' in cache_entry:
                                            original_text_key = cache_entry['original_text']
                                            self.actor_db_manager.save_translation_to_db(
                                                cursor=cursor,
                                                original_text=original_text_key,
                                                translated_text=cleaned_new_role,
                                                engine_used="manual"
                                            )
                                            logger.debug(f"  -> AI缓存通过反查更新: '{original_text_key}' -> '{cleaned_new_role}'")
                            conn.commit()
                        except Exception as e_cache:
                            logger.error(f"更新翻译缓存事务中发生错误: {e_cache}", exc_info=True)
                            conn.rollback()
                else:
                    logger.warning(f"无法更新翻译缓存，因为在内存中找不到 ItemID {item_id} 的原始演员数据。")
            except Exception as e:
                logger.error(f"手动处理期间更新翻译缓存时发生顶层错误: {e}", exc_info=True)


            # ======================================================================
            # 步骤 2: 执行“两步更新”到 Emby
            # ======================================================================
            
            # 2.1: 前置更新演员名
            logger.info("  -> 手动处理：步骤 1/2: 检查并更新演员名字...")
            original_names_map = {p.get("Id"): p.get("Name") for p in item_details.get("People", []) if p.get("Id")}
            for actor in cast_for_emby_handler:
                actor_id = actor.get("emby_person_id")
                new_name = actor.get("name")
                original_name = original_names_map.get(actor_id)
                if actor_id and new_name and original_name and new_name != original_name:
                    logger.info(f"  -> 检测到手动名字变更，正在更新 Person: '{original_name}' -> '{new_name}' (ID: {actor_id})")
                    emby_handler.update_person_details(
                        person_id=actor_id, new_data={"Name": new_name},
                        emby_server_url=self.emby_url, emby_api_key=self.emby_api_key, user_id=self.emby_user_id
                    )
            logger.info("  -> 手动处理：演员名字前置更新完成。")

            # 2.2: 更新媒体主项目的演员列表
            logger.info(f"  -> 手动处理：步骤 2/2: 准备将 {len(cast_for_emby_handler)} 位演员更新到媒体主项目...")
            update_success = emby_handler.update_emby_item_cast(
                item_id=item_id, new_cast_list_for_handler=cast_for_emby_handler,
                emby_server_url=self.emby_url, emby_api_key=self.emby_api_key, user_id=self.emby_user_id
            )

            if not update_success:
                logger.error(f"  -> 手动处理失败：更新 Emby 项目 '{item_name}' 演员信息时失败。")
                with get_central_db_connection() as conn:
                    self.log_db_manager.save_to_failed_log(conn.cursor(), item_id, item_name, "手动API更新演员信息失败", item_type)
                return False

            # ======================================================================
            # ★★★ 新增功能 2: 如果是剧集，则批量处理所有分集 ★★★
            # ======================================================================
            if item_type == "Series":
                self._batch_update_episodes_cast(
                    series_id=item_id,
                    series_name=item_name,
                    final_cast_list=cast_for_emby_handler # 直接使用我们准备好的列表
                )

            # ======================================================================
            # 步骤 3: 完成上锁和刷新
            # ======================================================================
            logger.info("  -> 手动更新成功")
            fields_to_lock = ["Cast"] if self.auto_lock_cast_enabled else None
            emby_handler.refresh_emby_item_metadata(
                item_emby_id=item_id, emby_server_url=self.emby_url, emby_api_key=self.emby_api_key,
                user_id_for_ops=self.emby_user_id, lock_fields=fields_to_lock,
                replace_all_metadata_param=False, item_name_for_log=item_name
            )

            # ======================================================================
            # 步骤 4: 更新处理日志
            # ======================================================================
            with get_central_db_connection() as conn:
                cursor = conn.cursor()
                self.log_db_manager.save_to_processed_log(cursor, item_id, item_name, score=10.0)
                self.log_db_manager.remove_from_failed_log(cursor, item_id)

            logger.info(f"  -> 手动处理 '{item_name}' 流程完成。")
            return True

        except Exception as e:
            logger.error(f"  -> 手动处理 '{item_name}' 时发生严重错误: {e}", exc_info=True)
            return False
        finally:
            if item_id in self.manual_edit_cache:
                del self.manual_edit_cache[item_id]
                logger.trace(f"已清理 ItemID {item_id} 的手动编辑会话缓存。")
    
    # --- 为前端准备演员列表用于编辑 ---
    def get_cast_for_editing(self, item_id: str) -> Optional[Dict[str, Any]]:
        """
        【V-API-Optimized - 性能与展示优化最终版】
        1. 演员头像直接从本地数据库缓存的 TMDB 路径拼接，加载速度极快。
        2. 角色名在返回给前端前进行清理，去除“饰 ”等前缀。
        """
        logger.info(f"  -> 为编辑页面准备数据：ItemID {item_id}")
        
        try:
            # 步骤 1: 获取 Emby 基础详情 (保持不变)
            emby_details = emby_handler.get_emby_item_details(item_id, self.emby_url, self.emby_api_key, self.emby_user_id)
            if not emby_details:
                raise ValueError(f"在Emby中未找到项目 {item_id}")

            item_name_for_log = emby_details.get("Name", f"未知(ID:{item_id})")
            
            # 步骤 2: 获取演员列表 (保持不变)
            logger.debug(f"  -> 正在为 '{item_name_for_log}' 获取演员列表...")
            raw_emby_people = emby_details.get("People", [])
            full_cast_enhanced = self._enrich_cast_from_db_and_api(raw_emby_people)
            
            if not full_cast_enhanced:
                logger.warning(f"项目 '{item_name_for_log}' 没有演员信息失败。")

            # 步骤 3: 缓存完整数据 (保持不变)
            cast_for_cache = []
            for actor in full_cast_enhanced:
                actor_copy = actor.copy()
                actor_copy['id'] = actor.get("ProviderIds", {}).get("Tmdb")
                actor_copy['emby_person_id'] = actor.get("Id")
                actor_copy['name'] = actor.get("Name")
                actor_copy['character'] = actor.get("Role")
                cast_for_cache.append(actor_copy)
            self.manual_edit_cache[item_id] = cast_for_cache
            logger.debug(f"已为 ItemID {item_id} 缓存了 {len(cast_for_cache)} 条完整演员数据。")

            # ★★★★★★★★★★★★★★★ 步骤 4: 构建前端数据 (全新优化) ★★★★★★★★★★★★★★★
            cast_for_frontend = []
            
            # 为了从数据库获取头像路径，我们需要一个数据库连接
            with get_central_db_connection() as conn:
                cursor = conn.cursor()
                
                for actor_data in cast_for_cache:
                    tmdb_id = actor_data.get('id')
                    image_url = None
                    
                    # --- 核心优化 1: 从数据库获取头像路径 ---
                    if tmdb_id:
                        # 调用我们已有的辅助函数
                        actor_metadata = self._get_actor_metadata_from_cache(tmdb_id, cursor)
                        if actor_metadata and actor_metadata.get("profile_path"):
                            profile_path = actor_metadata["profile_path"]
                            # 拼接 TMDB 小尺寸头像 URL，加载速度飞快
                            image_url = f"https://image.tmdb.org/t/p/w185{profile_path}"
                    
                    # --- 核心优化 2: 清理角色名 ---
                    original_role = actor_data.get('character', '')
                    cleaned_role_for_display = utils.clean_character_name_static(original_role)

                    cast_for_frontend.append({
                        "tmdbId": tmdb_id,
                        "name": actor_data.get('name'),
                        "role": cleaned_role_for_display, # ★ 使用清理后的角色名
                        "imageUrl": image_url,             # ★ 使用拼接的 TMDB 头像 URL
                        "emby_person_id": actor_data.get('emby_person_id')
                    })
            
            # 步骤 5: 准备并返回最终的响应数据 (保持不变)
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
                    "google_search_wiki": utils.generate_search_url('wikipedia', emby_details.get("Name"), emby_details.get("ProductionYear"))
                }
            }
            return response_data

        except Exception as e:
            logger.error(f"  -> 获取编辑数据失败 for ItemID {item_id}: {e}", exc_info=True)
            return None
    
    # ★★★ 全量备份到覆盖缓存 ★★★
    def sync_all_media_assets(self, update_status_callback: Optional[callable] = None, force_full_update: bool = False):
        """
        【V4 - 增量与全量融合版】
        - 快速模式 (默认): 高效找出并并发处理 Emby 中的新增媒体项。
        - 深度模式 (force_full_update=True): 强制并发处理 Emby 中的所有媒体项。
        - 两种模式均采用并发处理，大幅提升执行效率。
        """
        sync_mode = "(全量)" if force_full_update else "(增量)"
        task_name = f"覆盖缓存备份 ({sync_mode})"
        logger.info(f"--- 开始执行 '{task_name}' 任务 ---")

        if not self.local_data_path:
            logger.error(f"'{task_name}' 失败：未在配置中设置“本地数据源路径”。")
            if update_status_callback: update_status_callback(-1, "未配置本地数据源路径")
            return

        try:
            # --- 步骤 1: 获取 Emby 媒体库中的所有项目 ---
            if update_status_callback: update_status_callback(5, "正在获取 Emby 媒体库项目...")
            
            all_emby_items = emby_handler.get_emby_library_items_old(
                base_url=self.emby_url,
                api_key=self.emby_api_key,
                user_id=self.emby_user_id,
                library_ids=self.config.get('libraries_to_process', []),
                fields="ProviderIds,Type,DateModified,Name"
            )
            if all_emby_items is None:
                raise RuntimeError("从 Emby 获取媒体项列表失败。")

            emby_item_map = {item['Id']: item for item in all_emby_items}
            all_emby_ids = set(emby_item_map.keys())

            # --- 步骤 2: 根据模式确定需要处理的项目列表 ---
            items_to_process_ids: Set[str]
            
            if force_full_update:
                # 深度模式：处理所有 Emby 项目
                logger.info(f"  -> 全量模式已激活，将处理所有 {len(all_emby_ids)} 个 Emby 项目。")
                items_to_process_ids = all_emby_ids
            else:
                # 快速模式：计算差集，只处理新项目
                if update_status_callback: update_status_callback(15, "正在获取本地已处理日志...")
                with get_central_db_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT item_id FROM processed_log")
                    processed_ids = {row['item_id'] for row in cursor.fetchall()}
                
                items_to_process_ids = all_emby_ids - processed_ids
                logger.info(f"  -> 增量模式：从 {len(all_emby_ids)} 个 Emby 项目中发现 {len(items_to_process_ids)} 个新项目。")

            total_to_process = len(items_to_process_ids)
            if total_to_process == 0:
                message = "  -> 全量模式检查完成，媒体库为空。" if force_full_update else "  -> 增量模式检查完成，没有发现新项目。"
                logger.info(message)
                if update_status_callback: update_status_callback(100, message)
                return

            if update_status_callback: update_status_callback(30, f"准备处理 {total_to_process} 个项目...")

            # --- 步骤 3: 并发处理目标项目 (无论来源是全量还是增量) ---
            stats = {"success": 0, "skipped": 0, "failed": 0}
            lock = threading.Lock()

            def worker_process_item(item_id: str):
                """线程工作单元：处理单个项目"""
                if self.is_stop_requested():
                    return "stopped"
                try:
                    item_details = emby_item_map.get(item_id)
                    if not item_details:
                        raise ValueError("无法在 Emby 映射中找到项目详情")

                    tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
                    if not tmdb_id:
                        logger.warning(f"项目 '{item_details.get('Name')}' (ID: {item_id}) 缺少 TMDb ID，跳过。")
                        return "skipped"

                    self.sync_item_images(item_details)
                    self.sync_item_metadata(item_details, tmdb_id)

                    with get_central_db_connection() as conn_thread:
                        cursor_thread = conn_thread.cursor()
                        self.log_db_manager.mark_assets_as_synced(
                            cursor_thread, item_id, item_details.get("DateModified")
                        )
                        conn_thread.commit()
                    
                    return "success"
                except Exception as e:
                    logger.error(f"处理项目 (ID: {item_id}) 时发生错误: {e}", exc_info=True)
                    return "failed"

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                future_to_id = {executor.submit(worker_process_item, item_id): item_id for item_id in items_to_process_ids}

                for future in concurrent.futures.as_completed(future_to_id):
                    if self.is_stop_requested():
                        executor.shutdown(wait=False, cancel_futures=True)
                        break
                    
                    result = future.result()
                    with lock:
                        if result == "success":
                            stats["success"] += 1
                        elif result == "skipped":
                            stats["skipped"] += 1
                        elif result == "failed":
                            stats["failed"] += 1
                        
                        processed_count = sum(stats.values())
                        progress = 30 + int((processed_count / total_to_process) * 70)
                        if update_status_callback:
                            update_status_callback(progress, f"进度: {processed_count}/{total_to_process}")

        except Exception as e:
            logger.error(f"执行 '{task_name}' 时发生严重错误: {e}", exc_info=True)
            if update_status_callback: update_status_callback(-1, f"任务失败: {e}")
            return

        final_message = f"✅ 成功: {stats['success']}, 跳过: {stats['skipped']}, 失败: {stats['failed']}。"
        logger.info(f"'{task_name}' 完成。{final_message}")
        if update_status_callback:
            update_status_callback(100, final_message)
   
    # --- 备份图片 ---
    def sync_item_images(self, item_details: Dict[str, Any], update_description: Optional[str] = None) -> bool:
        """
        【新增-重构】这个方法负责同步一个媒体项目的所有相关图片。
        它从 _process_item_core_logic 中提取出来，以便复用。
        """
        item_id = item_details.get("Id")
        item_type = item_details.get("Type")
        item_name_for_log = item_details.get("Name", f"未知项目(ID:{item_id})")
        
        if not all([item_id, item_type, self.local_data_path]):
            logger.error(f"  -> 跳过 '{item_name_for_log}'，因为缺少ID、类型或未配置本地数据路径。")
            return False

        try:
            # --- 准备工作 (目录、TMDb ID等) ---
            log_prefix = "覆盖缓存-图片备份："
            tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
            if not tmdb_id:
                logger.warning(f"  -> {log_prefix} 项目 '{item_name_for_log}' 缺少TMDb ID，无法确定覆盖目录，跳过。")
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
                log_prefix = "[覆盖缓存-图片备份]"
                logger.trace(f"{log_prefix} 正在解析描述: '{update_description}'")
                
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
                        logger.trace(f"{log_prefix} 匹配到关键词 '{keyword}'，将只同步 {image_type_api} 图片。")
                        found_specific_image = True
                        break # 找到第一个匹配就停止，避免重复
                
                if not found_specific_image:
                    logger.trace(f"{log_prefix} 未能在描述中找到可识别的图片关键词，将回退到完全同步。")
                    images_to_sync = full_image_map # 回退
            
            # 模式二：完全同步 (默认或回退)
            else:
                log_prefix = "[覆盖缓存-图片备份]"
                logger.trace(f"  -> {log_prefix} 未提供更新描述，将同步所有类型的图片。")
                images_to_sync = full_image_map

            # --- 执行下载 ---
            logger.info(f"  -> {log_prefix} 开始为 '{item_name_for_log}' 下载 {len(images_to_sync)} 张图片至覆盖缓存")
            for image_type, filename in images_to_sync.items():
                if self.is_stop_requested():
                    logger.warning(f"  -> {log_prefix} 收到停止信号，中止图片下载。")
                    return False
                emby_handler.download_emby_image(item_id, image_type, os.path.join(image_override_dir, filename), self.emby_url, self.emby_api_key)
            
            # --- 分集图片逻辑 (只有在完全同步时才考虑执行) ---
            if images_to_sync == full_image_map and item_type == "Series":
            
                children = emby_handler.get_series_children(item_id, self.emby_url, self.emby_api_key, self.emby_user_id, series_name_for_log=item_name_for_log) or []
                for child in children:
                    if self.is_stop_requested():
                        logger.warning(f"  -> {log_prefix} 收到停止信号，中止子项目图片下载。")
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
            
            logger.info(f"  -> {log_prefix} 成功完成 '{item_name_for_log}' 的覆盖缓存-图片备份。")
            return True
        except Exception as e:
            logger.error(f"{log_prefix} 为 '{item_name_for_log}' 备份图片时发生未知错误: {e}", exc_info=True)
            return False
    
    # --- 备份元数据 ---
    def sync_item_metadata(self, item_details: Dict[str, Any], tmdb_id: str, final_cast_override: Optional[List[Dict[str, Any]]] = None):
        """
        备份元数据到覆盖缓存
        """
        item_id = item_details.get("Id")
        item_name_for_log = item_details.get("Name", f"未知项目(ID:{item_id})")
        log_prefix = "[覆盖缓存-元数据备份]"
        
        # --- 核心：两级优先级获取演员数据，失败则直接中止 ---
        new_perfect_cast = []
        data_source_mode = "无"

        with get_central_db_connection() as conn:
            cursor = conn.cursor()

            # 优先级 1: 直通模式
            if final_cast_override:
                data_source_mode = "模式1: 直通"
                new_perfect_cast = self._build_cast_from_final_data(final_cast_override)
            else:
                # 优先级 2: 元数据缓存模式
                cursor.execute("SELECT actors_json FROM media_metadata WHERE tmdb_id = %s AND item_type = %s", (tmdb_id, item_details.get("Type")))
                cache_row = cursor.fetchone()
                if cache_row and cache_row.get("actors_json") and isinstance(cache_row["actors_json"], list):
                    data_source_mode = "模式2: 缓存"
                    actors_from_cache = cache_row["actors_json"]
                    new_perfect_cast = self._build_cast_from_golden_cache(actors_from_cache)

        # ★★★ 核心修改：如果没有任何黄金数据源，则中止备份 ★★★
        if not new_perfect_cast:
            logger.info(f"  -> {log_prefix} 跳过 '{item_name_for_log}'，因为它尚未被主流程处理，无元数据缓存数据可供备份。")
            return

        logger.info(f"  -> {log_prefix} [{data_source_mode}] 开始为 '{item_name_for_log}' 执行元数据备份...")

        # --- 路径定义和基础文件复制 ---
        item_type = item_details.get("Type")
        cache_folder_name = "tmdb-movies2" if item_type == "Movie" else "tmdb-tv"
        source_cache_dir = os.path.join(self.local_data_path, "cache", cache_folder_name, tmdb_id)
        target_override_dir = os.path.join(self.local_data_path, "override", cache_folder_name, tmdb_id)

        if not os.path.exists(source_cache_dir):
            logger.warning(f"  -> {log_prefix} 跳过，因为源缓存目录不存在: {source_cache_dir}")
            return
        try:
            shutil.copytree(source_cache_dir, target_override_dir, dirs_exist_ok=True)
        except Exception as e:
            logger.error(f"  -> {log_prefix} 复制元数据时失败: {e}", exc_info=True)
            return

        # --- 将重建好的新列表写回主 JSON 文件 ---
        main_json_filename = "all.json" if item_type == "Movie" else "series.json"
        json_path = os.path.join(target_override_dir, main_json_filename)
        if not os.path.exists(json_path): return

        try:
            with open(json_path, 'r+', encoding='utf-8') as f:
                data = json.load(f)
                if 'casts' in data and 'cast' in data['casts']:
                    data['casts']['cast'] = new_perfect_cast
                elif 'credits' in data and 'cast' in data['credits']:
                    data['credits']['cast'] = new_perfect_cast
                else:
                    return
                
                f.seek(0)
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.truncate()
                logger.info(f"  -> {log_prefix} 成功将 {len(new_perfect_cast)} 位演员信息写入覆盖缓存文件。")
        except Exception as e:
            logger.error(f"  -> {log_prefix} 重建并写入 '{main_json_filename}' 时失败: {e}", exc_info=True)
            return

        # --- 注入演员表到所有季/集文件 ---
        if item_type == "Series":
            self._inject_cast_to_series_files(target_override_dir, new_perfect_cast, item_details)

    # --- 辅助函数：从不同数据源构建演员列表 ---
    def _build_cast_from_final_data(self, final_cast_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """辅助函数：从主流程的最终结果构建演员列表"""
        cast_list = []
        for i, actor_info in enumerate(final_cast_data):
            if not actor_info.get("id"): continue
            cast_list.append({
                "id": actor_info.get("id"), "name": actor_info.get("name"), "character": actor_info.get("character"),
                "original_name": actor_info.get("original_name"), "profile_path": actor_info.get("profile_path"),
                "adult": actor_info.get("adult", False), "gender": actor_info.get("gender", 0),
                "known_for_department": actor_info.get("known_for_department", "Acting"),
                "popularity": actor_info.get("popularity", 0.0), "cast_id": actor_info.get("cast_id"),
                "credit_id": actor_info.get("credit_id"), "order": actor_info.get("order", i)
            })
        return cast_list

    def _build_cast_from_golden_cache(self, cached_actors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """辅助函数：从数据库的元数据缓存构建演员列表"""
        cast_list = []
        for i, actor_info in enumerate(cached_actors):
            if not actor_info.get("id"): continue
            cast_list.append({
                "id": actor_info.get("id"), "name": actor_info.get("name"), "character": actor_info.get("character"),
                "original_name": actor_info.get("original_name"), "profile_path": actor_info.get("profile_path"),
                "adult": False, "gender": actor_info.get("gender", 0), "known_for_department": "Acting",
                "popularity": actor_info.get("popularity", 0.0), "cast_id": None, "credit_id": None, "order": i
            })
        return cast_list

    def _inject_cast_to_series_files(self, target_dir: str, cast_list: List[Dict[str, Any]], series_details: Dict[str, Any]):
        """辅助函数：将演员表注入剧集的季/集JSON文件"""
        log_prefix = "[覆盖缓存-元数据备份]"
        logger.info(f"  -> {log_prefix} 开始将元数据注入所有季/集备份文件...")
        
        children_from_emby = emby_handler.get_series_children(
            series_id=series_details.get("Id"), base_url=self.emby_url,
            api_key=self.emby_api_key, user_id=self.emby_user_id,
            series_name_for_log=series_details.get("Name")
        ) or []

        child_data_map = {}
        for child in children_from_emby:
            key = None
            if child.get("Type") == "Season": key = f"season-{child.get('IndexNumber')}"
            elif child.get("Type") == "Episode": key = f"season-{child.get('ParentIndexNumber')}-episode-{child.get('IndexNumber')}"
            if key: child_data_map[key] = child

        updated_children_count = 0
        try:
            for filename in os.listdir(target_dir):
                if filename.startswith("season-") and filename.endswith(".json") and filename != "series.json":
                    child_json_path = os.path.join(target_dir, filename)
                    try:
                        with open(child_json_path, 'r+', encoding='utf-8') as f_child:
                            child_data = json.load(f_child)
                            if 'credits' in child_data and 'cast' in child_data['credits']:
                                child_data['credits']['cast'] = cast_list
                            
                            file_key = os.path.splitext(filename)[0]
                            fresh_data = child_data_map.get(file_key)
                            if fresh_data:
                                child_data['name'] = fresh_data.get('Name', child_data.get('name'))
                                child_data['overview'] = fresh_data.get('Overview', child_data.get('overview'))
                            
                            f_child.seek(0)
                            json.dump(child_data, f_child, ensure_ascii=False, indent=2)
                            f_child.truncate()
                            updated_children_count += 1
                    except Exception as e_child:
                        logger.warning(f"  -> 更新子文件 '{filename}' 时失败: {e_child}")
            logger.info(f"  -> {log_prefix} 成功将元数据注入了 {updated_children_count} 个季/集文件。")
        except Exception as e_list:
            logger.error(f"  -> {log_prefix} 遍历并更新季/集文件时发生错误: {e_list}", exc_info=True)

    def sync_single_item_assets(self, item_id: str, 
                                update_description: Optional[str] = None, 
                                sync_timestamp_iso: Optional[str] = None,
                                final_cast_override: Optional[List[Dict[str, Any]]] = None):
        """
        【V1.5 - 任务队列版】为单个媒体项同步图片和元数据文件。
        - 职责单一化：只负责执行备份。并发控制和冷却逻辑已移交至任务管理器和调用方。
        """
        log_prefix = f"实时覆盖缓存备份"
        logger.trace(f"--- {log_prefix} 开始执行 ---")

        if not self.local_data_path:
            logger.warning(f"{log_prefix} 任务跳过，因为未配置本地数据源路径。")
            return

        try:
            item_details = emby_handler.get_emby_item_details(
                item_id, self.emby_url, self.emby_api_key, self.emby_user_id,
                fields="ProviderIds,Type,Name,People,ImageTags,IndexNumber,ParentIndexNumber"
            )
            if not item_details:
                raise ValueError("在Emby中找不到该项目。")

            tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
            if not tmdb_id:
                logger.warning(f"{log_prefix} 项目 '{item_details.get('Name')}' 缺少TMDb ID，无法备份。")
                return

            # 1. 同步图片
            self.sync_item_images(item_details, update_description)
            
            # 2. 同步元数据文件
            self.sync_item_metadata(item_details, tmdb_id, final_cast_override=final_cast_override)

            # 3. 记录本次同步的时间戳
            timestamp_to_log = sync_timestamp_iso or datetime.now(timezone.utc).isoformat()
            with get_central_db_connection() as conn:
                cursor = conn.cursor()
                self.log_db_manager.mark_assets_as_synced(
                    cursor, 
                    item_id, 
                    timestamp_to_log
                )
                conn.commit()
            
            logger.trace(f"--- {log_prefix} 成功完成 ---")

        except Exception as e:
            logger.error(f"{log_prefix} 执行时发生错误: {e}", exc_info=True)

    def sync_single_item_to_metadata_cache(self, item_id: str, item_name: Optional[str] = None):
        """
        为单独媒体项同步元数据缓存
        """
        log_prefix = f"实时同步媒体数据 '{item_name}'"
        logger.info(f"  -> {log_prefix} 开始执行")
        
        try:
            fields_to_get = "ProviderIds,Type,DateCreated,Name,ProductionYear,OriginalTitle,PremiereDate,CommunityRating,Genres,Studios,ProductionLocations,Tags,DateModified,OfficialRating"
            
            full_details_emby = emby_handler.get_emby_item_details(
                item_id, self.emby_url, self.emby_api_key, self.emby_user_id,
                fields=fields_to_get
            )
            if not full_details_emby:
                raise ValueError("在Emby中找不到该项目。")

            item_type = full_details_emby.get("Type")
            
            if item_type == "Episode":
                series_id = emby_handler.get_series_id_from_child_id(
                    item_id, self.emby_url, self.emby_api_key, self.emby_user_id, item_name=item_name
                )
                if series_id:
                    logger.debug(f"  -> {log_prefix} 检测到剧集，将使用其父剧集信息进行缓存。")
                    # 同样使用精简的字段列表获取父剧集信息
                    full_details_emby = emby_handler.get_emby_item_details(
                        series_id, self.emby_url, self.emby_api_key, self.emby_user_id,
                        fields=fields_to_get
                    )
                    if not full_details_emby:
                        logger.warning(f"  -> {log_prefix} 无法获取所属剧集 (ID: {series_id}) 的详情，跳过缓存。")
                        return
                else:
                    logger.warning(f"  -> {log_prefix} 无法获取剧集 '{full_details_emby.get('Name', item_id)}' 的所属剧集ID，跳过。")
                    return
            
            tmdb_id = full_details_emby.get("ProviderIds", {}).get("Tmdb")
            if not tmdb_id:
                logger.warning(f"{log_prefix} 项目 '{full_details_emby.get('Name')}' 缺少TMDb ID，无法缓存。")
                return

            # 3. 获取 TMDB 补充信息 (导演/国家)
            tmdb_details = None
            item_type = full_details_emby.get("Type")
            if item_type == 'Movie':
                tmdb_details = tmdb_handler.get_movie_details(tmdb_id, self.tmdb_api_key)
            elif item_type == 'Series':
                tmdb_details = tmdb_handler.get_tv_details(tmdb_id, self.tmdb_api_key)

            directors, countries = [], []
            if tmdb_details:
                if item_type == 'Movie':
                    credits = tmdb_details.get("credits", {}) or tmdb_details.get("casts", {})
                    if credits:
                        directors = [{'id': p.get('id'), 'name': p.get('name')} for p in credits.get('crew', []) if p.get('job') == 'Director']
                    countries = translate_country_list([c['name'] for c in tmdb_details.get('production_countries', [])])
                elif item_type == 'Series':
                    credits = tmdb_details.get("credits", {})
                    if credits:
                        directors = [{'id': p.get('id'), 'name': p.get('name')} for p in credits.get('crew', []) if p.get('job') == 'Director']
                    if not directors:
                        directors = [{'id': c.get('id'), 'name': c.get('name')} for c in tmdb_details.get('created_by', [])]
                    countries = translate_country_list(tmdb_details.get('origin_country', []))

            # 4. 组装元数据 (不含 actors_json)
            studios = [s['Name'] for s in full_details_emby.get('Studios', []) if s.get('Name')]
            tags = [tag['Name'] for tag in full_details_emby.get('TagItems', []) if tag.get('Name')]
            release_date_str = (full_details_emby.get('PremiereDate') or '0000-01-01T00:00:00.000Z').split('T')[0]
            official_rating = full_details_emby.get('OfficialRating')
            unified_rating = get_unified_rating(official_rating)

            metadata = {
                "tmdb_id": tmdb_id,
                "emby_item_id": full_details_emby.get('Id'), 
                "item_type": item_type,
                "title": full_details_emby.get('Name'), "original_title": full_details_emby.get('OriginalTitle'),
                "release_year": full_details_emby.get('ProductionYear'), "rating": full_details_emby.get('CommunityRating'),
                "official_rating": official_rating,
                "unified_rating": unified_rating,
                "release_date": release_date_str, "date_added": (full_details_emby.get("DateCreated") or '').split('T')[0] or None,
                "genres_json": json.dumps(full_details_emby.get('Genres', []), ensure_ascii=False),
                "directors_json": json.dumps(directors, ensure_ascii=False),
                "studios_json": json.dumps(studios, ensure_ascii=False),
                "countries_json": json.dumps(countries, ensure_ascii=False),
                "tags_json": json.dumps(tags, ensure_ascii=False),
            }

            # 5. 写入数据库
            with get_central_db_connection() as conn:
                cursor = conn.cursor()
                cols = list(metadata.keys())
                update_clauses = [f"{col} = EXCLUDED.{col}" for col in cols]
                update_clauses.append("last_synced_at = EXCLUDED.last_synced_at")
                
                sql = f"""
                    INSERT INTO media_metadata ({', '.join(cols)}, last_synced_at)
                    VALUES ({', '.join(['%s'] * len(cols))}, %s)
                    ON CONFLICT (tmdb_id, item_type) DO UPDATE SET {', '.join(update_clauses)}
                """
                sync_time = datetime.now(timezone.utc).isoformat()
                cursor.execute(sql, tuple(metadata.values()) + (sync_time,))
                conn.commit()
            
            logger.info(f"  -> {log_prefix} 成功完成。")

        except Exception as e:
            logger.error(f"{log_prefix} 执行时发生错误: {e}", exc_info=True)

    def close(self):
        if self.douban_api: self.douban_api.close()
