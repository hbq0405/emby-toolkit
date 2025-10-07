# core_processor.py

import os
import json
import time
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
    tmdb_details_for_extra: Optional[Dict[str, Any]],
    emby_children_details: Optional[List[Dict[str, Any]]] = None
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
                "popularity": p.get("popularity"),
                "order": p.get("order")
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
        
        # ▼▼▼ 核心修复 ▼▼▼
        # 检查是否存在首映日期，如果不存在，则使用 None (会被数据库正确处理为 NULL)
        premiere_date = item_details_from_emby.get('PremiereDate')
        release_date_str = premiere_date.split('T')[0] if premiere_date else None
        # ▲▲▲ 修复结束 ▲▲▲
        
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
            "release_date": release_date_str, # <--- 这里现在会接收到 None 或者一个有效的日期
            "in_library": True
        }
        if item_type == 'Series' and emby_children_details is not None:
            metadata["emby_children_details_json"] = json.dumps(emby_children_details, ensure_ascii=False)
        
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
        logger.debug(f"  ➜ 成功将《{metadata.get('title')}》的元数据缓存到数据库（并更新了同步时间）。")

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
        
        logger.info(f"  ➜ 正在为 {len(cast_list)} 位演员丰富数据...")

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
            logger.error(f"  ➜ 数据库查询阶段失败: {e}", exc_info=True)

        logger.info(f"  ➜ 从演员映射表找到了 {len(ids_found_in_db)} 位演员的信息。")

        # --- 阶段二：为未找到的演员实时查询 Emby API ---
        ids_to_fetch_from_api = [pid for pid in original_actor_map.keys() if pid not in ids_found_in_db]

        if ids_to_fetch_from_api:
            logger.trace(f"  ➜ 开始为 {len(ids_to_fetch_from_api)} 位新演员从Emby获取信息...")
            
            for person_id in ids_to_fetch_from_api:
                if self.is_stop_requested():
                    break
                
                person_details = emby_handler.get_emby_item_details(
                    item_id=person_id, 
                    emby_server_url=self.emby_url, 
                    emby_api_key=self.emby_api_key, 
                    user_id=self.emby_user_id,
                    fields="ProviderIds,Name" # 我们只需要这两个字段
                )
                
                if person_details and person_details.get("ProviderIds"):
                    enriched_actor = original_actor_map[person_id].copy()
                    enriched_actor["ProviderIds"] = person_details.get("ProviderIds")
                    enriched_actors_map[person_id] = enriched_actor
                    time_module.sleep(0.1) # 加个小延迟避免请求过快
        else:
            logger.trace("  ➜ (API查询) 跳过：所有演员均在本地数据库中找到。")

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

        logger.info(f"  ➜ 开始为新入库剧集 '{item_name_for_log}' 进行追剧状态判断...")
        try:
            # 实例化 WatchlistProcessor 并执行添加操作
            watchlist_proc = WatchlistProcessor(self.config)
            watchlist_proc.add_series_to_watchlist(item_details)
        except Exception as e_watchlist:
            logger.error(f"  ➜ 在自动添加 '{item_name_for_log}' 到追剧列表时发生错误: {e_watchlist}", exc_info=True)

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
            logger.debug(f"  ➜ 发现本地豆瓣缓存文件，将直接使用: {local_json_path}")
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
        logger.info("  ➜ 未找到本地豆瓣缓存，将通过在线API获取演员和评分信息。")

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
                    logger.info(f"  ➜ 在线获取到豆瓣评分 for '{item_name}': {douban_rating}")
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
        current_person_details = emby_handler.get_emby_items_by_id(
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
                emby_handler.update_person_details(
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
        return self._process_item_core_logic(
            item_details_from_emby=item_details,
            force_reprocess_this_item=force_reprocess_this_item,
            force_fetch_from_tmdb=force_fetch_from_tmdb
        )

    # ---核心处理流程 ---
    def _process_item_core_logic(self, item_details_from_emby: Dict[str, Any], force_reprocess_this_item: bool, force_fetch_from_tmdb: bool = False):
        """
        【V-Final-Architecture-Pro - “设计师”最终版 + 评分机制】
        本函数作为“设计师”，只负责计算和思考，产出“设计图”和“物料清单”，然后全权委托给施工队。
        """
        # ======================================================================
        # 阶段 1: 准备工作
        # ======================================================================
        item_id = item_details_from_emby.get("Id")
        item_name_for_log = item_details_from_emby.get("Name", f"未知项目(ID:{item_id})")
        tmdb_id = item_details_from_emby.get("ProviderIds", {}).get("Tmdb")
        item_type = item_details_from_emby.get("Type")

        logger.info(f"--- 开始处理 '{item_name_for_log}' (TMDb ID: {tmdb_id}) ---")

        all_emby_people_for_count = item_details_from_emby.get("People", [])
        original_emby_actor_count = len([p for p in all_emby_people_for_count if p.get("Type") == "Actor"])

        if not tmdb_id:
            logger.error(f"项目 '{item_name_for_log}' 缺少 TMDb ID，无法处理。")
            return False
        if not self.local_data_path:
            logger.error(f"项目 '{item_name_for_log}' 处理失败：未在配置中设置“本地数据源路径”。")
            return False
        
        try:
            authoritative_cast_source = []
            tmdb_details_for_extra = None # 用于内部缓存

            # 步骤1:检查json是否缺失
            cache_folder_name = "tmdb-movies2" if item_type == "Movie" else "tmdb-tv"
            source_cache_dir = os.path.join(self.local_data_path, "cache", cache_folder_name, tmdb_id)
            main_json_filename = "all.json" if item_type == "Movie" else "series.json"
            source_json_path = os.path.join(source_cache_dir, main_json_filename)

            if not os.path.exists(source_json_path):
                logger.warning(f"  ➜ 核心处理前置检查：本地元数据文件 '{source_json_path}' 不存在。启动备用方案...")
                logger.info(f"  ➜ 正在通知 Emby 为 '{item_name_for_log}' 刷新元数据以生成缓存文件...")
                
                emby_handler.refresh_emby_item_metadata(
                    item_emby_id=item_id,
                    emby_server_url=self.emby_url,
                    emby_api_key=self.emby_api_key,
                    user_id_for_ops=self.emby_user_id,
                    replace_all_metadata_param=True,
                    item_name_for_log=item_name_for_log
                )

                # --- 根据媒体类型选择不同的等待策略 ---
                if item_type == "Series":
                    # 电视剧：智能等待模式
                    logger.info("  ➜ 检测到为电视剧，启动智能等待模式...")
                    total_wait_time = 0
                    idle_time = 0
                    last_file_count = 0
                    CHECK_INTERVAL = 10  # 每10秒检查一次
                    MAX_IDLE_TIME = 60   # 连续60秒没动静则超时
                    MAX_TOTAL_WAIT_MINUTES = 15 # 总最长等待时间15分钟

                    while total_wait_time < MAX_TOTAL_WAIT_MINUTES * 60:
                        time_module.sleep(CHECK_INTERVAL)
                        total_wait_time += CHECK_INTERVAL

                        # 检查主文件是否已生成
                        if os.path.exists(source_json_path):
                            logger.info(f"  ➜ 主文件 '{main_json_filename}' 已生成！等待结束。")
                            break
                        
                        # 检查目录内文件数量变化
                        try:
                            current_file_count = len(os.listdir(source_cache_dir))
                        except FileNotFoundError:
                            current_file_count = 0

                        if current_file_count > last_file_count:
                            logger.info(f"  ➜ 缓存目录有活动，检测到 {current_file_count - last_file_count} 个新文件。重置空闲计时器。")
                            idle_time = 0 # 有新文件，重置空闲计时
                            last_file_count = current_file_count
                        else:
                            idle_time += CHECK_INTERVAL
                            logger.info(f"  ➜ 缓存目录无新文件，空闲时间累计: {idle_time}/{MAX_IDLE_TIME}秒。")

                        if idle_time >= MAX_IDLE_TIME:
                            logger.warning(f"  ➜ 缓存目录连续 {MAX_IDLE_TIME} 秒无活动，判定任务完成或超时。")
                            break
                    else: # while循环正常结束（达到总时长）
                        logger.warning(f"  ➜ 已达到总最长等待时间 {MAX_TOTAL_WAIT_MINUTES} 分钟，停止等待。")

                else:
                    # 电影：简单定时等待
                    logger.info("  ➜ 检测到为电影，启动简单等待模式...")
                    for attempt in range(10):
                        logger.info(f"  ➜ 等待3秒后检查文件... (第 {attempt + 1}/10 次尝试)")
                        time_module.sleep(3)
                        if os.path.exists(source_json_path):
                            logger.info(f"  ➜ 文件已成功生成！")
                            break
            
            # 在所有尝试后，最终确认文件是否存在
            if not os.path.exists(source_json_path):
                logger.error(f"  ➜ 等待超时，元数据文件仍未生成。无法继续处理 '{item_name_for_log}'，已跳过。")
                return False


            # 步骤3：如果是强制重处理就从TMDb拉取最新元数据，否则直接用本地的元数据。
            if force_fetch_from_tmdb and self.tmdb_api_key:
                logger.info(f"  ➜ 正在从 TMDB 获取最新演员表...")
                if item_type == "Movie":
                    movie_details = tmdb_handler.get_movie_details(tmdb_id, self.tmdb_api_key)
                    if movie_details:
                        tmdb_details_for_extra = movie_details
                        authoritative_cast_source = (movie_details.get("credits") or movie_details.get("casts", {})).get("cast", [])
                elif item_type == "Series":
                    aggregated_tmdb_data = tmdb_handler.aggregate_full_series_data_from_tmdb(int(tmdb_id), self.tmdb_api_key)
                    if aggregated_tmdb_data:
                        tmdb_details_for_extra = aggregated_tmdb_data.get("series_details")
                        all_episodes = list(aggregated_tmdb_data.get("episodes_details", {}).values())
                        authoritative_cast_source = _aggregate_series_cast_from_tmdb_data(aggregated_tmdb_data["series_details"], all_episodes)
            else:
                # 在文件模式下，直接读取我们已经确认存在的文件
                logger.info(f"  ➜ 正在从 cache 文件中预读演员表...")
                source_json_data = _read_local_json(source_json_path)
                if source_json_data:
                    tmdb_details_for_extra = source_json_data
                    authoritative_cast_source = (source_json_data.get("casts", {}) or source_json_data.get("credits", {})).get("cast", [])
                else:
                    logger.error(f"  ➜ 元数据文件 '{source_json_path}' 无效或为空，无法处理 '{item_name_for_log}'。")
                    return False
                
            # ======================================================================
            # 阶段 2: 数据来源二选一
            # ======================================================================
            final_processed_cast = None
            douban_rating = None
            tmdb_details_for_extra = None
            authoritative_cast_source = [] # 预定义
            cache_row = None # 用于后续判断是否走了快速模式

            # 1.尝试快速模式
            if not force_fetch_from_tmdb:
                logger.info(f"  ➜ 尝试从元数据缓存加载 '{item_name_for_log}'...")
                try:
                    with get_central_db_connection() as conn:
                        cursor = conn.cursor()
                        # 只有当 actors_json 存在 (NOT NULL) 且不是一个空的JSON数组时，才认为缓存有效
                        cursor.execute("""
                            SELECT actors_json, rating 
                            FROM media_metadata 
                            WHERE tmdb_id = %s AND item_type = %s
                              AND actors_json IS NOT NULL AND actors_json::text != '[]'
                        """, (tmdb_id, item_type))
                        cache_row = cursor.fetchone()
                        if cache_row:
                            logger.info(f"  ➜ 成功命中有效缓存！将跳过演员表深度处理。")
                            final_processed_cast = cache_row["actors_json"]
                            douban_rating = cache_row.get("rating")
                except Exception as e_cache:
                    logger.warning(f"  ➜ 加载缓存失败: {e_cache}。将回退到深度模式。")
                    final_processed_cast = None

            # 2.完整模式
            if final_processed_cast is None:
                logger.info(f"  ➜ 未命中缓存或强制重处理，开始处理演员表...")
                
                # 预读本地JSON文件以获取原始TMDb演员表
                source_json_data = _read_local_json(source_json_path)
                if source_json_data:
                    tmdb_details_for_extra = source_json_data
                    authoritative_cast_source = (source_json_data.get("casts", {}) or source_json_data.get("credits", {})).get("cast", [])

                with get_central_db_connection() as conn:
                    cursor = conn.cursor()
                    
                    all_emby_people = item_details_from_emby.get("People", [])
                    current_emby_cast_raw = [p for p in all_emby_people if p.get("Type") == "Actor"]
                    enriched_emby_cast = self._enrich_cast_from_db_and_api(current_emby_cast_raw)
                    douban_cast_raw, douban_rating_deep = self._get_douban_data_with_local_cache(item_details_from_emby)
                    douban_rating = douban_rating_deep # 覆盖评分

                    # 调用核心处理器处理演员表
                    final_processed_cast = self._process_cast_list(
                        tmdb_cast_people=authoritative_cast_source,
                        emby_cast_people=enriched_emby_cast,
                        douban_cast_list=douban_cast_raw,
                        item_details_from_emby=item_details_from_emby,
                        cursor=cursor,
                        tmdb_api_key=self.tmdb_api_key,
                        stop_event=self.get_stop_event()
                    )

            # ======================================================================
            # 阶段 3: 统一的收尾流程 (无论来源，必须执行)
            # ======================================================================
            if final_processed_cast is None:
                raise ValueError("未能生成有效的最终演员列表。")

            with get_central_db_connection() as conn:
                cursor = conn.cursor()

                # 步骤 3.1: 写入 override 文件
                self.sync_single_item_assets(
                    item_id=item_id,
                    update_description="主流程处理完成",
                    final_cast_override=final_processed_cast,
                    douban_rating_override=douban_rating
                )

                # 步骤 3.2: 通过 API 实时更新 Emby 演员库中的名字
                self._update_emby_person_names_from_final_cast(final_processed_cast, item_name_for_log)

                # 步骤 3.3: 通知 Emby 刷新
                logger.info(f"  ➜ 处理完成，正在通知 Emby 刷新...")
                emby_handler.refresh_emby_item_metadata(
                    item_emby_id=item_id,
                    emby_server_url=self.emby_url,
                    emby_api_key=self.emby_api_key,
                    user_id_for_ops=self.emby_user_id,
                    replace_all_metadata_param=True, 
                    item_name_for_log=item_name_for_log
                )

                # 步骤 3.4: 更新我们自己的数据库缓存
                _save_metadata_to_cache(
                    cursor=cursor, tmdb_id=tmdb_id, emby_item_id=item_id, item_type=item_type,
                    item_details_from_emby=item_details_from_emby,
                    final_processed_cast=final_processed_cast,
                    tmdb_details_for_extra=tmdb_details_for_extra
                )
                
                # 步骤 3.5: 根据处理质量评分，决定写入“已处理”或“失败”日志
                logger.info(f"  ➜ 正在评估《{item_name_for_log}》的处理质量...")
                genres = item_details_from_emby.get("Genres", [])
                is_animation = "Animation" in genres or "动画" in genres or "Documentary" in genres or "纪录" in genres
                
                # 如果走了快速模式，我们认为处理质量是完美的
                if cache_row:
                    processing_score = 10.0
                    logger.info(f"  ➜ [快速模式] 处理质量评分为 10.0 (完美)")
                else:
                    # 否则，调用工具函数进行实际评估
                    processing_score = actor_utils.evaluate_cast_processing_quality(
                        final_cast=final_processed_cast, 
                        original_cast_count=original_emby_actor_count,
                        expected_final_count=len(final_processed_cast), 
                        is_animation=is_animation
                    )
                
                min_score_for_review = float(self.config.get("min_score_for_review", constants.DEFAULT_MIN_SCORE_FOR_REVIEW))
                
                if processing_score < min_score_for_review:
                    reason = f"处理评分 ({processing_score:.2f}) 低于阈值 ({min_score_for_review})。"
                    logger.warning(f"  ➜ 《{item_name_for_log}》处理质量不佳，已标记待复核。原因: {reason}")
                    self.log_db_manager.save_to_failed_log(cursor, item_id, item_name_for_log, reason, item_type, score=processing_score)
                else:
                    logger.info(f"  ➜ 《{item_name_for_log}》处理质量良好 (评分: {processing_score:.2f})，已标记已处理。")
                    self._mark_item_as_processed(cursor, item_id, item_name_for_log, score=processing_score)
                    self.log_db_manager.remove_from_failed_log(cursor, item_id)
                
                conn.commit()

            logger.info(f"--- 处理完成 '{item_name_for_log}' ---")

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

        logger.trace(f"  ✅ 处理完成 '{item_name_for_log}'")
        return True

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
        logger.debug(f"  ➜ 匹配阶段 1: 对号入座")
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
                    
                    douban_avatar = d_actor.get("DoubanAvatarUrl")
                    if not l_actor.get("profile_path") and douban_avatar:
                        # 1. 更新内存对象，供本次运行使用
                        l_actor["profile_path"] = douban_avatar
                        logger.debug(f"    ➜ 演员 '{l_actor.get('name')}' 缺少TMDb头像，已从豆瓣缓存补充。")
                        
                        # 2. 立刻将这个发现同步回 actor_metadata 表
                        try:
                            actor_tmdb_id = l_actor.get("id")
                            if actor_tmdb_id:
                                # 使用 UPSERT 语句，确保记录存在并更新 profile_path
                                sql_upsert_avatar = """
                                    INSERT INTO actor_metadata (tmdb_id, profile_path)
                                    VALUES (%s, %s)
                                    ON CONFLICT (tmdb_id) DO UPDATE SET
                                        profile_path = COALESCE(actor_metadata.profile_path, EXCLUDED.profile_path);
                                """
                                cursor.execute(sql_upsert_avatar, (actor_tmdb_id, douban_avatar))
                                logger.debug(f"      ➜ 已将演员 '{l_actor.get('name')}' 的豆瓣头像链接持久化到数据库。")
                        except Exception as e_db_sync:
                            logger.error(f"      ➜ 持久化演员 '{l_actor.get('name')}' 的豆瓣头像到数据库时失败: {e_db_sync}")
                        
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
                
                logger.debug(f"  ➜ 匹配阶段 2: 用豆瓣ID查'演员映射表' ({len(unmatched_douban_actors)} 位演员)")
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
                                logger.debug(f"  ➜ 匹配成功 (通过 豆瓣ID映射): 豆瓣演员 '{d_actor.get('Name')}' -> 加入最终演员表")
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

                logger.debug(f"  ➜ 匹配阶段 3: 用IMDb ID进行最终匹配和新增 ({len(unmatched_douban_actors)} 位演员)")
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
                            
                            entry_row_from_map = self._find_person_in_map_by_imdb_id(d_imdb_id, cursor)
                            entry_from_map = dict(entry_row_from_map) if entry_row_from_map else None
                            if entry_from_map and entry_from_map.get("tmdb_person_id") and entry_from_map.get("emby_person_id"):
                                tmdb_id_from_map = str(entry_from_map.get("tmdb_person_id"))
                                if tmdb_id_from_map not in final_cast_map:
                                    logger.debug(f"  ➜ 匹配成功 (通过 IMDb映射): 豆瓣演员 '{d_actor.get('Name')}' -> 加入最终演员表")
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
                                person_from_tmdb = tmdb_handler.find_person_by_external_id(
                                    external_id=d_imdb_id, api_key=self.tmdb_api_key, source="imdb_id"
                                )
                                if person_from_tmdb and person_from_tmdb.get("id"):
                                    tmdb_id_from_find = str(person_from_tmdb.get("id"))
                                    
                                    d_actor['tmdb_id_from_api'] = tmdb_id_from_find
                                    d_actor['imdb_id_from_api'] = d_imdb_id

                                    final_check_row = self._find_person_in_map_by_tmdb_id(tmdb_id_from_find, cursor)
                                    if final_check_row and dict(final_check_row).get("emby_person_id"):
                                        emby_pid_from_final_check = dict(final_check_row).get("emby_person_id")
                                        if tmdb_id_from_find not in final_cast_map:
                                            logger.debug(f"  ➜ 匹配成功 (通过 TMDb反查): 豆瓣演员 '{d_actor.get('Name')}' -> 加入最终演员表")
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

                # --- 归档+新增 ---
                if still_unmatched_final:
                    logger.info(f"  ➜ 检查 {len(still_unmatched_final)} 位未匹配演员，尝试加入最终列表...")
                    emby_config_for_upsert = {"url": self.emby_url, "api_key": self.emby_api_key, "user_id": self.emby_user_id}
                    added_count = 0
                    
                    for d_actor in still_unmatched_final:
                        tmdb_id_to_save = d_actor.get('tmdb_id_from_api')
                        if tmdb_id_to_save and tmdb_id_to_save not in final_cast_map:
                            
                            new_actor_entry = {
                                "id": tmdb_id_to_save,
                                "name": d_actor.get("Name"),
                                "character": d_actor.get("Role"),
                                "order": 999,
                                "imdb_id": d_actor.get("imdb_id_from_api"),
                                "douban_id": d_actor.get("DoubanCelebrityId"),
                                "emby_person_id": None
                            }
                            final_cast_map[tmdb_id_to_save] = new_actor_entry
                            added_count += 1
                    
                    if added_count > 0:
                        logger.info(f"  ➜ 成功新增了 {added_count} 位演员到最终列表。")
        
        # ======================================================================
        # 步骤 4: ★★★ 补全头像 ★★★
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
            
            supplemented_count = 0
            for actor in actors_to_supplement:
                if stop_event and stop_event.is_set(): raise InterruptedError("任务中止")
                
                tmdb_id = actor.get("id")
                profile_path = None
                
                cached_meta = self._get_actor_metadata_from_cache(tmdb_id, cursor)
                if cached_meta and cached_meta.get("profile_path"):
                    profile_path = cached_meta["profile_path"]
                
                elif tmdb_api_key:
                    person_details = tmdb_handler.get_person_details_tmdb(tmdb_id, tmdb_api_key)
                    if person_details:
                        self.actor_db_manager.update_actor_metadata_from_tmdb(cursor, tmdb_id, person_details)
                        if person_details.get("profile_path"):
                            profile_path = person_details["profile_path"]
                
                if profile_path:
                    actor["profile_path"] = profile_path
                    supplemented_count += 1

            logger.info(f"  ➜ 新增演员头像信息补全完成，成功为 {supplemented_count}/{total_to_supplement} 位演员补充了头像。")
        else:
            logger.info("  ➜ 没有需要补充头像的新增演员。")

        # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
        # 步骤 5：智能截断逻辑 (Smart Truncation) ★★★
        # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
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
            cast_to_process = prioritized_list[:limit]
            logger.debug(f"  ➜ 截断后，保留了 {len(with_profile)} 位有头像演员中的 {len([a for a in cast_to_process if a.get('profile_path')])} 位。")
        else:
            cast_to_process = current_cast_list
            cast_to_process.sort(key=lambda x: x.get('order') if x.get('order') is not None and x.get('order') >= 0 else 999)

        # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
        # 步骤 6: ★★★ 豆瓣头像备用方案 (带数据库缓存) ★★★
        # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
        if self.douban_api:
            actors_needing_douban_avatar = [
                actor for actor in cast_to_process
                if not actor.get("profile_path") and actor.get("douban_id") and actor.get("id") # 确保有TMDb ID用于关联
            ]
            
            if actors_needing_douban_avatar:
                logger.info(f"  ➜ 发现 {len(actors_needing_douban_avatar)} 位无头像演员有关联的豆瓣ID，尝试获取豆瓣头像作为备用...")
                douban_avatars_found = 0
                for actor in actors_needing_douban_avatar:
                    if stop_event and stop_event.is_set(): raise InterruptedError("任务中止")
                    
                    douban_id = actor.get("douban_id")
                    tmdb_id = actor.get("id") # 获取TMDb ID用于数据库操作
                    
                    try:
                        # 调用豆瓣API获取名人详情
                        details = self.douban_api.celebrity_details(douban_id)
                        time_module.sleep(0.3) # 尊重API冷却时间
                        
                        if details and not details.get("error"):
                            # 从详情中提取头像链接，优先大图
                            avatar_url = (details.get("avatars", {}) or {}).get("large")
                            if avatar_url:
                                # 1. 更新内存中的演员对象
                                actor["profile_path"] = avatar_url
                                
                                # ▼▼▼ 新增：将头像链接同步到数据库 ▼▼▼
                                try:
                                    # 使用 UPSERT 语句，如果演员已存在则更新，不存在则插入
                                    upsert_sql = """
                                        INSERT INTO actor_metadata (tmdb_id, profile_path)
                                        VALUES (%s, %s)
                                        ON CONFLICT (tmdb_id) DO UPDATE SET
                                            profile_path = EXCLUDED.profile_path;
                                    """
                                    cursor.execute(upsert_sql, (tmdb_id, avatar_url))
                                    logger.debug(f"    ➜ 成功将演员 '{actor.get('name')}' (TMDb ID: {tmdb_id}) 的豆瓣头像链接缓存至数据库。")
                                except Exception as e_db:
                                    logger.error(f"    ➜ 缓存演员 (TMDb ID: {tmdb_id}) 的豆瓣头像到数据库时失败: {e_db}")
                                # ▲▲▲ 新增结束 ▲▲▲

                                douban_avatars_found += 1
                                
                    except Exception as e_douban_avatar:
                        logger.warning(f"    ➜ 为演员 (豆瓣ID: {douban_id}) 获取豆瓣头像时发生错误: {e_douban_avatar}")

                if douban_avatars_found > 0:
                    logger.info(f"  ➜ 成功为 {douban_avatars_found} 位演员补充并缓存了豆瓣头像。")
            else:
                logger.info("  ➜ 无需从豆瓣补充备用头像。")

        # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
        # 步骤 7: ★★★ 翻译和格式化 ★★★
        # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
        logger.info(f"  ➜ 将对 {len(cast_to_process)} 位演员进行最终的翻译和格式化处理...")

        if not (self.ai_translator and self.config.get(constants.CONFIG_OPTION_AI_TRANSLATION_ENABLED, False)):
            logger.info("  ➜ AI翻译未启用，将保留演员和角色名原文。")
        else:
            final_translation_map = {}
            terms_to_translate = set()
            for actor in cast_to_process:
                character = actor.get('character')
                if character:
                    cleaned_character = utils.clean_character_name_static(character)
                    if cleaned_character and not utils.contains_chinese(cleaned_character):
                        terms_to_translate.add(cleaned_character)
                name = actor.get('name')
                if name and not utils.contains_chinese(name):
                    terms_to_translate.add(name)

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

        # ▼▼▼ 步骤 8: ★★★ 最终数据回写/反哺 ★★★ ▼▼▼
        logger.info(f"  ➜ 开始将 {len(final_cast_perfect)} 位最终演员的完整信息同步回数据库...")
        upserted_count = 0
        for actor in final_cast_perfect:
            try:
                actor_data_for_db = {
                    'primary_name': actor.get('name'),
                    'tmdb_person_id': actor.get('id'),
                    'emby_person_id': actor.get('emby_person_id'),
                    'douban_celebrity_id': actor.get('douban_id'),
                    'imdb_id': actor.get('imdb_id')
                }
                
                # 过滤掉没有有效ID的条目
                if not actor_data_for_db['tmdb_person_id'] and not actor_data_for_db['douban_celebrity_id']:
                    continue

                # 优先使用 TMDb ID 作为冲突键
                conflict_key = "tmdb_person_id"
                conflict_value = actor_data_for_db[conflict_key]
                
                if not conflict_value:
                    conflict_key = "douban_celebrity_id"
                
                # 构建要更新的字段，使用正确的 EXCLUDED 语法
                update_clauses = []
                for key in actor_data_for_db.keys():
                    if key != conflict_key:
                        # 正确语法：COALESCE(table.column, EXCLUDED.column)
                        # 意思是：将此字段更新为它现在的值，但如果现在的值是NULL，就用新插入行的值
                        update_clauses.append(f"{key} = COALESCE(person_identity_map.{key}, EXCLUDED.{key})")
                
                update_str = ", ".join(update_clauses)
                
                # 构建最终的 UPSERT SQL
                sql = f"""
                    INSERT INTO person_identity_map ({', '.join(actor_data_for_db.keys())})
                    VALUES ({', '.join(['%s'] * len(actor_data_for_db))})
                    ON CONFLICT ({conflict_key}) DO UPDATE SET {update_str}
                """
                
                # 参数只需要 INSERT 部分的值
                params = tuple(actor_data_for_db.values())
                
                cursor.execute(sql, params)
                upserted_count += 1

            except Exception as e_backfill:
                logger.error(f"  ➜ 回写演员 '{actor.get('name')}' 的数据到数据库时失败: {e_backfill}")
                # 发生错误时，必须回滚事务，否则后续所有操作都会失败
                cursor.execute("ROLLBACK")
                # 重新开始一个新的事务
                cursor.execute("BEGIN")

        if upserted_count > 0:
            logger.info(f"  ➜ 成功回写/更新了 {upserted_count} 位演员的数据库记录。")

        return final_cast_perfect


    def process_full_library(self, update_status_callback: Optional[callable] = None, force_reprocess_all: bool = False, force_fetch_from_tmdb: bool = False):
        """
        【V3 - 最终完整版】
        这是所有全量处理的唯一入口，它自己处理所有与“强制”相关的逻辑。
        """
        self.clear_stop_signal()
        
        logger.trace(f"进入核心执行层: process_full_library, 接收到的 force_reprocess_all = {force_reprocess_all}, force_fetch_from_tmdb = {force_fetch_from_tmdb}")

        if force_reprocess_all:
            logger.info("  ➜ 检测到“强制重处理”选项，正在清空已处理日志...")
            try:
                self.clear_processed_log()
            except Exception as e:
                logger.error(f"在 process_full_library 中清空日志失败: {e}", exc_info=True)
                if update_status_callback: update_status_callback(-1, "清空日志失败")
                return

        # --- ★★★ 补全了这部分代码 ★★★ ---
        libs_to_process_ids = self.config.get("libraries_to_process", [])
        if not libs_to_process_ids:
            logger.warning("  ➜ 未在配置中指定要处理的媒体库。")
            return

        logger.info("  ➜ 正在尝试从Emby获取媒体项目...")
        all_emby_libraries = emby_handler.get_emby_libraries(self.emby_url, self.emby_api_key, self.emby_user_id) or []
        library_name_map = {lib.get('Id'): lib.get('Name', '未知库名') for lib in all_emby_libraries}
        
        movies = emby_handler.get_emby_library_items(self.emby_url, self.emby_api_key, "Movie", self.emby_user_id, libs_to_process_ids, library_name_map=library_name_map) or []
        series = emby_handler.get_emby_library_items(self.emby_url, self.emby_api_key, "Series", self.emby_user_id, libs_to_process_ids, library_name_map=library_name_map) or []
        
        if movies:
            source_movie_lib_names = sorted(list({library_name_map.get(item.get('_SourceLibraryId')) for item in movies if item.get('_SourceLibraryId')}))
            logger.info(f"  ➜ 从媒体库【{', '.join(source_movie_lib_names)}】获取到 {len(movies)} 个电影项目。")

        if series:
            source_series_lib_names = sorted(list({library_name_map.get(item.get('_SourceLibraryId')) for item in series if item.get('_SourceLibraryId')}))
            logger.info(f"  ➜ 从媒体库【{', '.join(source_series_lib_names)}】获取到 {len(series)} 个电视剧项目。")

        all_items = movies + series
        total = len(all_items)
        # --- ★★★ 补全结束 ★★★ ---
        
        if total == 0:
            logger.info("  ➜ 在所有选定的库中未找到任何可处理的项目。")
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
                logger.info(f"  ➜ 发现 {len(deleted_items_to_clean)} 个已从 Emby 媒体库删除的项目，正在从 '已处理' 中移除...")
                for deleted_item_id in deleted_items_to_clean:
                    self.log_db_manager.remove_from_processed_log(cursor, deleted_item_id)
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
                logger.warning("全库扫描任务已被用户中止。")
                break # 使用 break 优雅地退出循环
            
            item_id = item.get('Id')
            item_name = item.get('Name', f"ID:{item_id}")

            if not force_reprocess_all and item_id in self.processed_items_cache:
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
        【V2.5 - 终极修复版】
        1. 增加了完整的日志记录，让每一步操作都清晰可见。
        2. 修复并强化了“翻译缓存反哺”功能。
        3. 增加了在写入文件前的强制“最终格式化”步骤，确保前缀永远正确。
        """
        logger.info(f"  ➜ 手动处理流程启动：ItemID: {item_id} ('{item_name}')")
        
        try:
            # ======================================================================
            # 步骤 1: 数据准备与定位
            # ======================================================================
            item_details = emby_handler.get_emby_item_details(item_id, self.emby_url, self.emby_api_key, self.emby_user_id)
            if not item_details: raise ValueError(f"无法获取项目 {item_id} 的详情。")
            
            logger.info(f"  ➜ 手动处理：步骤 1/6: 构建TMDb与Emby演员的ID映射...")
            raw_emby_actors = [p for p in item_details.get("People", []) if p.get("Type") == "Actor"]
            enriched_actors = self._enrich_cast_from_db_and_api(raw_emby_actors)
            
            tmdb_to_emby_map = {}
            for person in enriched_actors:
                person_tmdb_id = (person.get("ProviderIds") or {}).get("Tmdb")
                if person_tmdb_id:
                    tmdb_to_emby_map[str(person_tmdb_id)] = person.get("Id")
            logger.info(f"  ➜ 成功构建了 {len(tmdb_to_emby_map)} 条ID映射。")
            
            item_type = item_details.get("Type")
            tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
            if not tmdb_id: raise ValueError(f"项目 {item_id} 缺少 TMDb ID。")

            cache_folder_name = "tmdb-movies2" if item_type == "Movie" else "tmdb-tv"
            target_override_dir = os.path.join(self.local_data_path, "override", cache_folder_name, tmdb_id)
            main_json_filename = "all.json" if item_type == "Movie" else "series.json"
            main_json_path = os.path.join(target_override_dir, main_json_filename)

            if not os.path.exists(main_json_path):
                raise FileNotFoundError(f"手动处理失败：找不到主元数据文件 '{main_json_path}'。")

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
            raw_emby_actors = [p for p in item_details.get("People", []) if p.get("Type") == "Actor"]
            enriched_actors = self._enrich_cast_from_db_and_api(raw_emby_actors)
            
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
                    emby_handler.update_person_details(
                        person_id=actor_emby_id, new_data={"Name": new_name},
                        emby_server_url=self.emby_url, emby_api_key=self.emby_api_key, user_id=self.emby_user_id
                    )
                    updated_names_count += 1
            
            if updated_names_count > 0:
                logger.info(f"    ➜ 成功通过 API 更新了 {updated_names_count} 位演员的名字。")

            # ======================================================================
            # 步骤 4: 文件读、改、写 (包含最终格式化)
            # ======================================================================
            logger.info(f"  ➜ 手动处理：步骤 4/6: 读取原始数据，识别并补全新增演员的元数据...")
            with open(main_json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            original_cast_data = (data.get('casts', {}) or data.get('credits', {})).get('cast', [])
            original_cast_map = {str(actor.get('id')): actor for actor in original_cast_data if actor.get('id')}

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
                        logger.info(f"    ➜ 发现新演员: '{actor_from_frontend.get('name')}' (TMDb ID: {tmdb_id_str})，开始补全元数据...")
                        
                        # B1: 优先从 actor_metadata 缓存获取
                        person_details = self._get_actor_metadata_from_cache(tmdb_id_str, cursor)
                        
                        # B2: 如果缓存没有，则从 TMDb API 获取并反哺
                        if not person_details:
                            logger.debug(f"      ➜ 缓存未命中，从 TMDb API 获取详情...")
                            person_details_from_api = tmdb_handler.get_person_details_tmdb(tmdb_id_str, self.tmdb_api_key)
                            if person_details_from_api:
                                self.actor_db_manager.update_actor_metadata_from_tmdb(cursor, tmdb_id_str, person_details_from_api)
                                person_details = person_details_from_api # 使用API返回的数据
                            else:
                                logger.warning(f"      ➜ 无法获取TMDb ID {tmdb_id_str} 的详情，将使用基础信息跳过。")
                                # 即使失败，也创建一个基础对象，避免丢失
                                person_details = {} 
                        else:
                            logger.debug(f"      ➜ 成功从数据库缓存命中元数据。")

                        # B3: 构建一个与 override 文件格式一致的新演员对象
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
                            # 新增演员没有这些电影特定的ID，设为None
                            "cast_id": None, 
                            "credit_id": None,
                            "order": 999 # 放到最后，后续格式化步骤会重新排序
                        }
                        new_cast_built.append(new_actor_entry)

            # ======================================================================
            # 步骤 5: 最终格式化并写入文件 (逻辑不变)
            # ======================================================================
            logger.info(f"  ➜ 手动处理：步骤 5/6: 重建演员列表并执行最终格式化...")
            genres = item_details.get("Genres", [])
            is_animation = "Animation" in genres or "动画" in genres or "Documentary" in genres or "纪录" in genres
            final_formatted_cast = actor_utils.format_and_complete_cast_list(
                new_cast_built, is_animation, self.config, mode='manual'
            )
            # _build_cast_from_final_data 确保了所有字段都存在，即使是None
            final_cast_for_json = self._build_cast_from_final_data(final_formatted_cast)

            if 'casts' in data:
                data['casts']['cast'] = final_cast_for_json
            elif 'credits' in data:
                data['credits']['cast'] = final_cast_for_json
            else:
                data.setdefault('credits', {})['cast'] = final_cast_for_json
            
            with open(main_json_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            if item_type == "Series":
                self._inject_cast_to_series_files(
                    target_dir=target_override_dir, cast_list=final_cast_for_json,
                    series_details=item_details, source_dir=os.path.join(self.local_data_path, "cache", cache_folder_name, tmdb_id)
                )

            # ======================================================================
            # 步骤 6: 触发刷新并更新日志
            # ======================================================================
            logger.info("  ➜ 手动处理：步骤 6/6: 触发 Emby 刷新并更新内部日志...")
            
            emby_handler.refresh_emby_item_metadata(
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
                # 更新 media_metadata 缓存，使其与手动修改保持一致
                actors_for_cache = [{"id": actor.get("id"), "name": actor.get("name"), "character": actor.get("character")} for actor in new_cast_built]
                new_actors_json = json.dumps(actors_for_cache, ensure_ascii=False)
                cursor.execute(
                    "UPDATE media_metadata SET actors_json = %s, last_synced_at = NOW() WHERE tmdb_id = %s AND item_type = %s",
                    (new_actors_json, tmdb_id, item_type)
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
        【V2 - Override文件中心化版】
        重构数据源，确保前端获取和编辑的演员列表，与 override 文件中的“真理之源”完全一致。
        - 演员表主体(名字, 角色, 顺序) 来自 override 主JSON文件。
        - 通过一次 Emby API 调用来获取 emby_person_id 并进行映射。
        """
        logger.info(f"  ➜ 为编辑页面准备数据：ItemID {item_id}")
        
        try:
            # 步骤 1: 获取 Emby 基础详情 和 用于ID映射的People列表
            emby_details = emby_handler.get_emby_item_details(item_id, self.emby_url, self.emby_api_key, self.emby_user_id)
            if not emby_details:
                raise ValueError(f"在Emby中未找到项目 {item_id}")

            item_name_for_log = emby_details.get("Name", f"未知(ID:{item_id})")
            tmdb_id = emby_details.get("ProviderIds", {}).get("Tmdb")
            item_type = emby_details.get("Type")
            if not tmdb_id:
                raise ValueError(f"项目 '{item_name_for_log}' 缺少 TMDb ID，无法定位元数据文件。")

            # 步骤 2: 读取 override 文件，获取权威演员表
            cache_folder_name = "tmdb-movies2" if item_type == "Movie" else "tmdb-tv"
            target_override_dir = os.path.join(self.local_data_path, "override", cache_folder_name, tmdb_id)
            main_json_filename = "all.json" if item_type == "Movie" else "series.json"
            main_json_path = os.path.join(target_override_dir, main_json_filename)

            if not os.path.exists(main_json_path):
                raise FileNotFoundError(f"无法为 '{item_name_for_log}' 准备编辑数据：找不到主元数据文件 '{main_json_path}'。请确保该项目已被至少处理过一次。")

            with open(main_json_path, 'r', encoding='utf-8') as f:
                override_data = json.load(f)
            
            cast_from_override = (override_data.get('casts', {}) or override_data.get('credits', {})).get('cast', [])
            logger.debug(f"  ➜ 成功从 override 文件为 '{item_name_for_log}' 加载了 {len(cast_from_override)} 位演员。")

            # 步骤 3: 构建 TMDb ID -> emby_person_id 的映射
            tmdb_to_emby_map = {}
            for person in emby_details.get("People", []):
                person_tmdb_id = (person.get("ProviderIds") or {}).get("Tmdb")
                if person_tmdb_id:
                    tmdb_to_emby_map[str(person_tmdb_id)] = person.get("Id")
            
            # 步骤 4: 组装最终数据 (合并 override 内容 和 emby_person_id)
            cast_for_frontend = []
            session_cache_map = {}
            
            with get_central_db_connection() as conn:
                cursor = conn.cursor()
                for actor_data in cast_from_override:
                    actor_tmdb_id = actor_data.get('id')
                    if not actor_tmdb_id: continue
                    
                    emby_person_id = tmdb_to_emby_map.get(str(actor_tmdb_id))
                    
                    # 从本地数据库获取头像
                    image_url = None
                    # actor_data 就是从 override 文件里读出的那条记录，它包含了最准确的 profile_path
                    profile_path = actor_data.get("profile_path")
                    if profile_path:
                        # 如果是完整的 URL (来自豆瓣)，则直接使用
                        if profile_path.startswith('http'):
                            image_url = profile_path
                        # 否则，认为是 TMDb 的相对路径，进行拼接
                        else:
                            image_url = f"https://image.tmdb.org/t/p/w185{profile_path}"
                    
                    # 清理角色名
                    original_role = actor_data.get('character', '')
                    session_cache_map[str(actor_tmdb_id)] = original_role
                    cleaned_role_for_display = utils.clean_character_name_static(original_role)

                    # 为前端准备的数据
                    cast_for_frontend.append({
                        "tmdbId": actor_tmdb_id,
                        "name": actor_data.get('name'),
                        "role": cleaned_role_for_display,
                        "imageUrl": image_url,
                        "emby_person_id": emby_person_id
                    })
                    
            # 步骤 5: 缓存会话数据并准备最终响应
            self.manual_edit_cache[item_id] = session_cache_map
            logger.debug(f"已为 ItemID {item_id} 缓存了 {len(session_cache_map)} 条用于手动编辑会话的演员数据。")

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
            logger.error(f"  ➜ 获取编辑数据失败 for ItemID {item_id}: {e}", exc_info=True)
            return None
    
    # --- 实时覆盖缓存同步 ---
    def sync_single_item_assets(self, item_id: str, 
                                update_description: Optional[str] = None, 
                                sync_timestamp_iso: Optional[str] = None,
                                final_cast_override: Optional[List[Dict[str, Any]]] = None,
                                episode_ids_to_sync: Optional[List[str]] = None,
                                douban_rating_override: Optional[float] = None):
        """
        纯粹的项目经理，负责接收设计师的所有材料，并分发给施工队。
        """
        log_prefix = f"实时覆盖缓存同步"
        logger.trace(f"--- {log_prefix} 开始执行 (ItemID: {item_id}) ---")

        if not self.local_data_path:
            logger.warning(f"  ➜ {log_prefix} 任务跳过，因为未配置本地数据源路径。")
            return

        try:
            item_details = emby_handler.get_emby_item_details(
                item_id, self.emby_url, self.emby_api_key, self.emby_user_id,
                fields="ProviderIds,Type,Name,IndexNumber,ParentIndexNumber"
            )
            if not item_details:
                raise ValueError("在Emby中找不到该项目。")

            tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
            if not tmdb_id:
                logger.warning(f"{log_prefix} 项目 '{item_details.get('Name')}' 缺少TMDb ID，无法同步。")
                return

            # 1. 调度外墙施工队
            self.sync_item_images(item_details, update_description, episode_ids_to_sync=episode_ids_to_sync)
            
            # 2. 调度精装修施工队，并把所有图纸和材料都给他
            self.sync_item_metadata(
                item_details, 
                tmdb_id, 
                final_cast_override=final_cast_override, 
                episode_ids_to_sync=episode_ids_to_sync,
                douban_rating_override=douban_rating_override
            )

            # 3. 记录工时
            timestamp_to_log = sync_timestamp_iso or datetime.now(timezone.utc).isoformat()
            with get_central_db_connection() as conn:
                cursor = conn.cursor()
                self.log_db_manager.mark_assets_as_synced(
                    cursor, 
                    item_id, 
                    timestamp_to_log
                )
                conn.commit()
            
            logger.trace(f"--- {log_prefix} 成功完成 (ItemID: {item_id}) ---")

        except Exception as e:
            logger.error(f"{log_prefix} 执行时发生错误 (ItemID: {item_id}): {e}", exc_info=True)

    # --- 备份图片 ---
    def sync_item_images(self, item_details: Dict[str, Any], update_description: Optional[str] = None, episode_ids_to_sync: Optional[List[str]] = None) -> bool:
        """
        【新增-重构】这个方法负责同步一个媒体项目的所有相关图片。
        它从 _process_item_core_logic 中提取出来，以便复用。
        """
        item_id = item_details.get("Id")
        item_type = item_details.get("Type")
        item_name_for_log = item_details.get("Name", f"未知项目(ID:{item_id})")
        
        if not all([item_id, item_type, self.local_data_path]):
            logger.error(f"  ➜ 跳过 '{item_name_for_log}'，因为缺少ID、类型或未配置本地数据路径。")
            return False

        try:
            # --- 准备工作 (目录、TMDb ID等) ---
            log_prefix = "覆盖缓存-图片备份："
            tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
            if not tmdb_id:
                logger.warning(f"  ➜ {log_prefix} 项目 '{item_name_for_log}' 缺少TMDb ID，无法确定覆盖目录，跳过。")
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
                logger.trace(f"  ➜ {log_prefix} 未提供更新描述，将同步所有类型的图片。")
                images_to_sync = full_image_map

            # --- 执行下载 ---
            if not episode_ids_to_sync:
                logger.info(f"  ➜ {log_prefix} 开始为 '{item_name_for_log}' 下载 {len(images_to_sync)} 张主图片至覆盖缓存")
                for image_type, filename in images_to_sync.items():
                    if self.is_stop_requested():
                        logger.warning(f"  ➜ {log_prefix} 收到停止信号，中止图片下载。")
                        return False
                    emby_handler.download_emby_image(item_id, image_type, os.path.join(image_override_dir, filename), self.emby_url, self.emby_api_key)
            
            # --- 分集图片逻辑 ---
            if item_type == "Series":
                children_to_process = []
                # 获取所有子项信息，用于查找
                all_children = emby_handler.get_series_children(item_id, self.emby_url, self.emby_api_key, self.emby_user_id, series_name_for_log=item_name_for_log) or []
                
                if episode_ids_to_sync:
                    # 模式一：只处理指定的分集
                    logger.info(f"  ➜ {log_prefix} 将只同步 {len(episode_ids_to_sync)} 个指定分集的图片。")
                    id_set = set(episode_ids_to_sync)
                    children_to_process = [child for child in all_children if child.get("Id") in id_set]
                elif images_to_sync == full_image_map:
                    # 模式二：处理所有子项（原逻辑）
                    children_to_process = all_children

                for child in children_to_process:
                    if self.is_stop_requested():
                        logger.warning(f"  ➜ {log_prefix} 收到停止信号，中止子项目图片下载。")
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
            
            logger.trace(f"  ➜ {log_prefix} 成功完成 '{item_name_for_log}' 的覆盖缓存-图片备份。")
            return True
        except Exception as e:
            logger.error(f"{log_prefix} 为 '{item_name_for_log}' 备份图片时发生未知错误: {e}", exc_info=True)
            return False
    
    # --- 备份元数据 ---
    def sync_item_metadata(self, item_details: Dict[str, Any], tmdb_id: str,
                       final_cast_override: Optional[List[Dict[str, Any]]] = None,
                       episode_ids_to_sync: Optional[List[str]] = None,
                       douban_rating_override: Optional[float] = None):
        """
        【V4 - 精装修施工队最终版】
        本函数是唯一的施工队，负责所有 override 文件的读写操作。
        """
        item_id = item_details.get("Id")
        item_name_for_log = item_details.get("Name", f"未知项目(ID:{item_id})")
        item_type = item_details.get("Type")
        log_prefix = "[覆盖缓存-元数据写入]"

        # 定义核心路径
        cache_folder_name = "tmdb-movies2" if item_type == "Movie" else "tmdb-tv"
        source_cache_dir = os.path.join(self.local_data_path, "cache", cache_folder_name, tmdb_id)
        target_override_dir = os.path.join(self.local_data_path, "override", cache_folder_name, tmdb_id)
        main_json_filename = "all.json" if item_type == "Movie" else "series.json"
        main_json_path = os.path.join(target_override_dir, main_json_filename)

        # 步骤 1: 进场施工，打好基础 (复制毛坯房)
        # 只有在需要进行主体装修时（主流程调用），才需要复制。追更等零活不需要。
        if final_cast_override is not None:
            logger.info(f"  ➜ {log_prefix} 开始为 '{item_name_for_log}' 写入覆盖缓存...")
            if not os.path.exists(source_cache_dir):
                logger.error(f"  ➜ {log_prefix} 找不到源缓存目录！路径: {source_cache_dir}")
                return
            try:
                shutil.copytree(source_cache_dir, target_override_dir, dirs_exist_ok=True)
            except Exception as e:
                logger.error(f"  ➜ {log_prefix} 复制元数据时失败: {e}", exc_info=True)
                return

        perfect_cast_for_injection = []
        if final_cast_override is not None:
            # --- 角色一：主体精装修 ---
            new_cast_for_json = self._build_cast_from_final_data(final_cast_override)
            
            perfect_cast_for_injection = new_cast_for_json

            # 步骤 2: 修改主文件
            with open(main_json_path, 'r+', encoding='utf-8') as f:
                data = json.load(f)
                if douban_rating_override is not None: data['vote_average'] = douban_rating_override
                if 'casts' in data: data['casts']['cast'] = perfect_cast_for_injection
                else: data.setdefault('credits', {})['cast'] = perfect_cast_for_injection
                
                f.seek(0)
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.truncate()
        else:
            # --- 角色二：零活处理 (追更) ---
            logger.info(f"  ➜ {log_prefix} [追更] 开始为 '{item_name_for_log}' 的新分集写入覆盖缓存...")
            if not os.path.exists(main_json_path):
                logger.error(f"  ➜ {log_prefix} 追更任务失败：找不到主元数据文件 '{main_json_path}'。")
                return
            try:
                with open(main_json_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    perfect_cast_for_injection = (data.get('casts', {}) or data.get('credits', {})).get('cast', [])
            except Exception as e:
                logger.error(f"  ➜ {log_prefix} 读取主元数据文件 '{main_json_path}' 时失败: {e}", exc_info=True)
                return

        # 步骤 3: 公共施工 - 注入分集文件
        if item_type == "Series" and perfect_cast_for_injection:
            self._inject_cast_to_series_files(
                target_dir=target_override_dir, 
                cast_list=perfect_cast_for_injection, 
                series_details=item_details, 
                source_dir=source_cache_dir,  
                episode_ids_to_sync=episode_ids_to_sync
            )

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

    def _inject_cast_to_series_files(self, target_dir: str, cast_list: List[Dict[str, Any]], series_details: Dict[str, Any], 
                                     # ▼▼▼ 核心修改 1/3: 增加 source_dir 参数 ▼▼▼
                                     source_dir: str, 
                                     episode_ids_to_sync: Optional[List[str]] = None):
        """
        【修复版】辅助函数：将演员表注入剧集的季/集JSON文件。
        - 新增逻辑：在修改文件前，如果文件在目标目录不存在，则从源目录复制。
        """
        log_prefix = "[覆盖缓存-元数据写入]"
        if cast_list is not None:
            logger.info(f"  ➜ {log_prefix} 开始将演员表注入所有季/集备份文件...")
        else:
            logger.info(f"  ➜ {log_prefix} 开始将实时元数据（标题/简介）同步到所有季/集备份文件...")
        
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
            files_to_process = []
            if episode_ids_to_sync:
                id_set = set(episode_ids_to_sync)
                for child in children_from_emby:
                    if child.get("Id") in id_set and child.get("Type") == "Episode":
                        s_num = child.get('ParentIndexNumber')
                        e_num = child.get('IndexNumber')
                        if s_num is not None and e_num is not None:
                            files_to_process.append(f"season-{s_num}-episode-{e_num}.json")
            else:
                for filename in os.listdir(target_dir):
                    if filename.startswith("season-") and filename.endswith(".json") and filename != "series.json":
                        files_to_process.append(filename)

            for filename in files_to_process:
                child_json_path = os.path.join(target_dir, filename)
                
                # ▼▼▼ 核心修改 2/3: 检查-复制-修改 逻辑 ▼▼▼
                if not os.path.exists(child_json_path):
                    source_json_path = os.path.join(source_dir, filename)
                    if os.path.exists(source_json_path):
                        logger.debug(f"  ➜ 正在复制元数据文件 '{filename}'")
                        # 确保目标目录存在
                        os.makedirs(os.path.dirname(child_json_path), exist_ok=True)
                        shutil.copy2(source_json_path, child_json_path)
                    else:
                        logger.warning(f"  ➜ 跳过注入 '{filename}'，因为它在源缓存和覆盖缓存中都不存在。")
                        continue
                
                try:
                    with open(child_json_path, 'r+', encoding='utf-8') as f_child:
                        child_data = json.load(f_child)
                        
                        # ★★★ 核心修改：条件性地更新演员表 ★★★
                        if cast_list is not None and 'credits' in child_data and 'cast' in child_data['credits']:
                            child_data['credits']['cast'] = cast_list
                        
                        # 无论如何都更新元数据
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
                    logger.warning(f"  ➜ 更新子文件 '{filename}' 时失败: {e_child}")
            logger.info(f"  ➜ {log_prefix} 成功将元数据注入了 {updated_children_count} 个季/集文件。")
        except Exception as e_list:
            logger.error(f"  ➜ {log_prefix} 遍历并更新季/集文件时发生错误: {e_list}", exc_info=True)

    def sync_single_item_to_metadata_cache(self, item_id: str, item_name: Optional[str] = None, episode_ids_to_add: Optional[List[str]] = None):
        """
        【V6 - 职责分离最终版】
        为一个媒体项同步元数据缓存。
        - 增量模式 (当提供了 episode_ids_to_add): 只将新增的分集详情追加到现有记录中，然后任务结束。
        - 常规模式 (默认): 对媒体项（电影或剧集）进行一次轻量级的全量元数据刷新（不含演员和子项目），然后任务结束。
        """
        log_prefix = f"实时同步媒体数据 '{item_name}'"
        sync_mode = "精准分集追加" if episode_ids_to_add else "常规元数据刷新"
        logger.info(f"  ➜ {log_prefix} 开始执行 ({sync_mode}模式)")
        
        try:
            # ▼▼▼ 核心修正：根据模式执行完全独立的逻辑分支 ▼▼▼
            if episode_ids_to_add:
                # --- 模式一：精准分集追加 ---
                if not item_id:
                    logger.error(f"  ➜ {log_prefix} [增量模式] 缺少剧集ID，无法执行。")
                    return

                # 1. 批量获取新分集的详情
                new_episodes_details = emby_handler.get_emby_items_by_id(
                    base_url=self.emby_url, api_key=self.emby_api_key, user_id=self.emby_user_id,
                    item_ids=episode_ids_to_add, fields="Id,Name,Type,Overview,ParentIndexNumber,IndexNumber"
                )
                
                new_children_to_append = []
                if new_episodes_details:
                    for child in new_episodes_details:
                        detail = {"Id": child.get("Id"), "Type": "Episode", "Name": child.get("Name"),
                                  "SeasonNumber": child.get("ParentIndexNumber"), "EpisodeNumber": child.get("IndexNumber"),
                                  "Overview": child.get("Overview")}
                        new_children_to_append.append(detail)
                
                if not new_children_to_append:
                    logger.warning(f"  ➜ {log_prefix} [增量模式] 无法从Emby获取新分集的详情，任务中止。")
                    return

                # 2. 使用 JSONB || 操作符将新分集追加到数据库
                with get_central_db_connection() as conn:
                    with conn.cursor() as cursor:
                        update_query = """
                            UPDATE media_metadata
                            SET emby_children_details_json = COALESCE(emby_children_details_json, '[]'::jsonb) || %s::jsonb,
                                last_synced_at = %s
                            WHERE emby_item_id = %s
                        """
                        current_utc_time = datetime.now(timezone.utc)
                        cursor.execute(update_query, (json.dumps(new_children_to_append, ensure_ascii=False), current_utc_time, item_id))
                        conn.commit()
                logger.info(f"  ➜ {log_prefix} [增量模式] 成功追加 {len(new_children_to_append)} 个新分集详情到数据库。")
                # 任务完成，直接返回
                return

            # --- 模式二：常规元数据刷新 ---
            # (如果执行到这里，说明 episode_ids_to_add 为 None)
            fields_to_get = "ProviderIds,Type,DateCreated,Name,ProductionYear,OriginalTitle,PremiereDate,CommunityRating,Genres,Studios,ProductionLocations,Tags,DateModified,OfficialRating"
            full_details_emby = emby_handler.get_emby_item_details(item_id, self.emby_url, self.emby_api_key, self.emby_user_id, fields=fields_to_get)
            if not full_details_emby: raise ValueError("在Emby中找不到该项目。")
            
            item_type = full_details_emby.get("Type")
            if item_type == "Episode":
                series_id = emby_handler.get_series_id_from_child_id(item_id, self.emby_url, self.emby_api_key, self.emby_user_id, item_name=item_name)
                if series_id:
                    full_details_emby = emby_handler.get_emby_item_details(series_id, self.emby_url, self.emby_api_key, self.emby_user_id, fields=fields_to_get)
                    if not full_details_emby:
                        logger.warning(f"  ➜ {log_prefix} 无法获取所属剧集 (ID: {series_id}) 的详情，跳过缓存。")
                        return
                else:
                    logger.warning(f"  ➜ {log_prefix} 无法获取剧集 '{full_details_emby.get('Name', item_id)}' 的所属剧集ID，跳过。")
                    return
            
            tmdb_id = full_details_emby.get("ProviderIds", {}).get("Tmdb")
            if not tmdb_id:
                logger.warning(f"{log_prefix} 项目 '{full_details_emby.get('Name')}' 缺少TMDb ID，无法缓存。")
                return

            # ... (获取导演/国家/组装metadata字典的逻辑保持不变) ...
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
            studios = [s['Name'] for s in full_details_emby.get('Studios', []) if s.get('Name')]
            tags = [tag['Name'] for tag in full_details_emby.get('TagItems', []) if tag.get('Name')]
            release_date_str = (full_details_emby.get('PremiereDate') or '0000-01-01T00:00:00.000Z').split('T')[0]
            official_rating = full_details_emby.get('OfficialRating')
            unified_rating = get_unified_rating(official_rating)
            metadata = {
                "tmdb_id": tmdb_id, "emby_item_id": full_details_emby.get('Id'), "item_type": item_type,
                "title": full_details_emby.get('Name'), "original_title": full_details_emby.get('OriginalTitle'),
                "release_year": full_details_emby.get('ProductionYear'), "rating": full_details_emby.get('CommunityRating'),
                "official_rating": official_rating, "unified_rating": unified_rating,
                "release_date": release_date_str, "date_added": (full_details_emby.get("DateCreated") or '').split('T')[0] or None,
                "genres_json": json.dumps(full_details_emby.get('Genres', []), ensure_ascii=False),
                "directors_json": json.dumps(directors, ensure_ascii=False),
                "studios_json": json.dumps(studios, ensure_ascii=False),
                "countries_json": json.dumps(countries, ensure_ascii=False),
                "tags_json": json.dumps(tags, ensure_ascii=False),
            }
            
            with get_central_db_connection() as conn:
                with conn.cursor() as cursor:
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
            
            logger.info(f"  ➜ {log_prefix} 成功完成。")

        except Exception as e:
            logger.error(f"{log_prefix} 执行时发生错误: {e}", exc_info=True)

    def sync_emby_updates_to_override_files(self, item_details: Dict[str, Any]):
        """
        将来自 Emby 的实时元数据更新同步到 override 缓存文件。
        这是一个 "读-改-写" 操作，用于持久化用户在 Emby UI 上的修改。
        """
        item_id = item_details.get("Id")
        item_name_for_log = item_details.get("Name", f"未知项目(ID:{item_id})")
        item_type = item_details.get("Type")
        tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
        log_prefix = "[覆盖缓存-元数据持久化]"

        if not all([item_id, item_type, tmdb_id, self.local_data_path]):
            logger.warning(f"  ➜ {log_prefix} 跳过 '{item_name_for_log}'，缺少关键ID或路径配置。")
            return

        logger.info(f"  ➜ {log_prefix} 开始为 '{item_name_for_log}' 更新覆盖缓存文件...")

        # --- 定位主文件 ---
        cache_folder_name = "tmdb-movies2" if item_type == "Movie" else "tmdb-tv"
        target_override_dir = os.path.join(self.local_data_path, "override", cache_folder_name, tmdb_id)
        main_json_filename = "all.json" if item_type == "Movie" else "series.json"
        main_json_path = os.path.join(target_override_dir, main_json_filename)

        # --- 安全检查：如果 override 文件不存在，说明从未被完整处理过，不应继续 ---
        if not os.path.exists(main_json_path):
            logger.warning(f"  ➜ {log_prefix} 无法持久化修改：主覆盖文件 '{main_json_path}' 不存在。请先对该项目进行一次完整处理。")
            return

        try:
            # --- 核心的 "读-改-写" 逻辑 ---
            with open(main_json_path, 'r+', encoding='utf-8') as f:
                data = json.load(f)

                # 定义要从 Emby 同步的字段
                fields_to_update = {
                    "Name": "title",
                    "OriginalTitle": "original_title",
                    "Overview": "overview",
                    "Tagline": "tagline",
                    "CommunityRating": "vote_average", # 用户评分
                    "OfficialRating": "official_rating",
                    "Genres": "genres",
                    "Studios": "production_companies",
                    "Tags": "keywords"
                }
                
                updated_count = 0
                for emby_key, json_key in fields_to_update.items():
                    if emby_key in item_details:
                        new_value = item_details[emby_key]
                        # 特殊处理 Studios 和 Genres
                        if emby_key in ["Studios", "Genres"]:
                            # 假设源数据是 [{ "Name": "Studio A" }] 或 ["Action"]
                            if isinstance(new_value, list):
                                if emby_key == "Studios":
                                     data[json_key] = [{"name": s.get("Name")} for s in new_value if s.get("Name")]
                                else: # Genres
                                     data[json_key] = new_value
                                updated_count += 1
                        else:
                            data[json_key] = new_value
                            updated_count += 1
                
                # 处理日期
                if 'PremiereDate' in item_details:
                    data['release_date'] = (item_details['PremiereDate'] or '').split('T')[0]
                    updated_count += 1

                logger.info(f"  ➜ {log_prefix} 准备将 {updated_count} 项更新写入 '{main_json_filename}'。")

                f.seek(0)
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.truncate()

            # 如果是剧集，还需要更新所有子文件的 name 和 overview
            if item_type == "Series":
                logger.info(f"  ➜ {log_prefix} 检测到为剧集，开始同步更新子项（季/集）的元数据...")
                self._inject_cast_to_series_files(
                    target_dir=target_override_dir,
                    cast_list=None, # ★★★ 关键：传入 None 表示我们只更新元数据，不碰演员表 ★★★
                    series_details=item_details,
                    source_dir=os.path.join(self.local_data_path, "cache", cache_folder_name, tmdb_id)
                )

            logger.info(f"  ➜ {log_prefix} 成功为 '{item_name_for_log}' 持久化了元数据修改。")

        except Exception as e:
            logger.error(f"  ➜ {log_prefix} 为 '{item_name_for_log}' 更新覆盖缓存文件时发生错误: {e}", exc_info=True)

    def close(self):
        if self.douban_api: self.douban_api.close()
