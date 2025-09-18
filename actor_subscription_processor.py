# actor_subscription_processor.py

import time
import re
from datetime import datetime, timedelta
import logging
from typing import Optional, Dict, Any, List, Set, Callable
import threading
from enum import Enum

import tmdb_handler
import emby_handler
from db_handler import get_db_connection # ★★★ 核心修改：导入新的数据库连接函数
import moviepilot_handler
import db_handler
import constants

logger = logging.getLogger(__name__)

class MediaStatus(Enum):
    IN_LIBRARY = 'IN_LIBRARY'
    PENDING_RELEASE = 'PENDING_RELEASE'
    SUBSCRIBED = 'SUBSCRIBED'
    MISSING = 'MISSING'

class MediaType(Enum):
    MOVIE = 'Movie'
    SERIES = 'Series'

class ActorSubscriptionProcessor:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.tmdb_api_key = config.get('tmdb_api_key')
        self.emby_url = config.get('emby_server_url')
        self.emby_api_key = config.get('emby_api_key')
        self.emby_user_id = config.get('emby_user_id')
        self.subscribe_delay_sec = config.get(constants.CONFIG_OPTION_RESUBSCRIBE_DELAY_SECONDS, 1.5)
        self._stop_event = threading.Event()
        self._quota_warning_logged = False

    def signal_stop(self):
        self._stop_event.set()

    def is_stop_requested(self) -> bool:
        return self._stop_event.is_set()

    def clear_stop_signal(self):
        self._stop_event.clear()

    def close(self):
        logger.trace("ActorSubscriptionProcessor closed.")

    def run_scheduled_task(self, update_status_callback: Optional[Callable] = None):
        def _update_status(progress, message):
            if update_status_callback:
                safe_progress = max(0, min(100, int(progress)))
                update_status_callback(safe_progress, message)

        logger.trace("--- 开始执行定时刷新演员订阅任务 ---")
        _update_status(0, "正在准备订阅列表...")

        self._quota_warning_logged = False
        
        try:
            # ★★★ 核心修改：使用新的 get_db_connection，不再需要 db_path
            with get_db_connection() as conn:
                # ★★★ 核心修改：不再需要设置 row_factory，因为 db_handler 已配置 RealDictCursor
                cursor = conn.cursor()
                cursor.execute("SELECT id, actor_name FROM actor_subscriptions WHERE status = 'active'")
                # fetchall() 在 RealDictCursor 下返回字典列表，行为一致
                subs_to_process = cursor.fetchall()
        except Exception as e:
            logger.error(f"定时任务：获取启用的订阅列表时失败: {e}", exc_info=True)
            _update_status(-1, "错误：获取订阅列表失败。")
            return
            
        if not subs_to_process:
            logger.info("  -> 没有找到需要处理的演员订阅，任务结束。")
            _update_status(100, "没有需要处理的演员订阅。")
            return
            
        total_subs = len(subs_to_process)
        logger.info(f"  -> 共找到 {total_subs} 个启用的订阅需要处理。")
        
        _update_status(5, "  -> 正在从 Emby 获取媒体库信息...")
        logger.info("  -> 正在从 Emby 一次性获取全量媒体库数据...")
        emby_tmdb_ids: Set[str] = set()
        try:
            all_libraries = emby_handler.get_emby_libraries(self.emby_url, self.emby_api_key, self.emby_user_id)
            library_ids_to_scan = [lib['Id'] for lib in all_libraries if lib.get('CollectionType') in ['movies', 'tvshows']]
            emby_items = emby_handler.get_emby_library_items(base_url=self.emby_url, api_key=self.emby_api_key, user_id=self.emby_user_id, library_ids=library_ids_to_scan, media_type_filter="Movie,Series")
            
            if self.is_stop_requested():
                logger.info("任务在获取Emby媒体库后被用户中断。")
                return

            emby_tmdb_ids = {item['ProviderIds'].get('Tmdb') for item in emby_items if item.get('ProviderIds', {}).get('Tmdb')}
            logger.debug(f"  -> 已从 Emby 获取 {len(emby_tmdb_ids)} 个已入库媒体的 TMDb ID 用于后续对比。")
        except Exception as e:
            logger.error(f"  -> 从 Emby 获取媒体库信息时发生严重错误: {e}", exc_info=True)
            _update_status(-1, "错误：连接 Emby 或获取数据失败。")
            return

        session_subscribed_ids: Set[str] = set()

        for i, sub in enumerate(subs_to_process):
            if self.is_stop_requested():
                logger.info("定时刷新演员订阅任务被用户中断。")
                break
            
            progress = int(5 + ((i + 1) / total_subs) * 95)
            message = f"  -> ({i+1}/{total_subs}) 正在扫描演员: {sub['actor_name']}"
            _update_status(progress, message)
            logger.info(message)
            
            self.run_full_scan_for_actor(sub['id'], emby_tmdb_ids, session_subscribed_ids)
            
            if not self.is_stop_requested() and i < total_subs - 1:
                time.sleep(1) 
                
        if not self.is_stop_requested():
            logger.trace("--- 定时刷新演员订阅任务执行完毕 ---")
            _update_status(100, "  -> 所有订阅扫描完成。")


    def run_full_scan_for_actor(self, subscription_id: int, emby_tmdb_ids: Set[str], session_subscribed_ids: Optional[Set[str]] = None):
        if session_subscribed_ids is None:
            session_subscribed_ids = set()

        logger.trace(f"--- 开始为订阅ID {subscription_id} 执行全量作品扫描 ---")
        try:
            # ★★★ 核心修改：使用新的 get_db_connection
            with get_db_connection() as conn:
                cursor = conn.cursor()

                # ★★★ 核心修改：SQL占位符从 ? 改为 %s
                cursor.execute("SELECT * FROM actor_subscriptions WHERE id = %s", (subscription_id,))
                sub = cursor.fetchone()
                if not sub: return
                
                logger.trace(f"  -> 正在处理演员: {sub['actor_name']} (TMDb ID: {sub['tmdb_person_id']})")

                old_tracked_media = self._get_existing_tracked_media(cursor, subscription_id)
                
                credits = tmdb_handler.get_person_credits_tmdb(sub['tmdb_person_id'], self.tmdb_api_key)
                if self.is_stop_requested() or not credits: return
                
                all_works = credits.get('movie_credits', {}).get('cast', []) + credits.get('tv_credits', {}).get('cast', [])
                logger.info(f"  -> 从TMDb获取到演员 {sub['actor_name']} 的 {len(all_works)} 部原始作品记录。")

                filtered_works = self._filter_works(all_works, sub)
                logger.info(f"  -> 根据规则筛选后，有 {len(filtered_works)} 部作品需要处理。")

                media_to_insert = []
                media_to_update = []
                today_str = datetime.now().strftime('%Y-%m-%d')

                for work in filtered_works:
                    if self.is_stop_requested(): break

                    media_id = work.get('id')
                    old_status = old_tracked_media.get(media_id)

                    current_status = self._determine_media_status(work, emby_tmdb_ids, today_str, old_status, session_subscribed_ids)
                    if not current_status: continue

                    if old_status is None:
                        media_to_insert.append(self._prepare_media_dict(work, subscription_id, current_status))
                    elif old_status != current_status.value:
                        media_to_update.append({'status': current_status.value, 'subscription_id': subscription_id, 'tmdb_media_id': media_id})
                    
                    old_tracked_media.pop(media_id, None)

                if self.is_stop_requested():
                    logger.info(f"任务在处理作品时被中断 (订阅ID: {subscription_id})。")
                    return

                media_ids_to_delete = list(old_tracked_media.keys())

                self._update_database_records(cursor, subscription_id, media_to_insert, media_to_update, media_ids_to_delete)
                
                conn.commit()
                logger.info(f"  -> ✅ {sub['actor_name']} 的全量处理成功完成 ---")

        except Exception as e:
            logger.error(f"为订阅ID {subscription_id} 执行扫描时发生严重错误: {e}", exc_info=True)

    def _get_existing_tracked_media(self, cursor, subscription_id: int) -> Dict[int, str]:
        """从数据库获取当前已追踪的媒体及其状态。"""
        # ★★★ 核心修改：SQL占位符从 ? 改为 %s
        cursor.execute("SELECT tmdb_media_id, status FROM tracked_actor_media WHERE subscription_id = %s", (subscription_id,))
        return {row['tmdb_media_id']: row['status'] for row in cursor.fetchall()}

    def _filter_works(self, works: List[Dict], sub_config) -> List[Dict]:
        """根据订阅配置过滤从TMDb获取的作品列表。"""
        # ... (此函数无数据库交互，无需修改) ...
        filtered = []
        handled_media_ids = set()
        
        config_start_year = sub_config['config_start_year']
        
        raw_types_from_db = sub_config['config_media_types'].split(',')
        config_media_types = {
            'Series' if t.strip().lower() == 'tv' else t.strip().capitalize()
            for t in raw_types_from_db if t.strip()
        }

        config_genres_include = set(sub_config['config_genres_include_json'] or [])
        config_genres_exclude = set(sub_config['config_genres_exclude_json'] or [])
        config_min_rating = sub_config['config_min_rating']
        grace_period_months = 6
        six_months_ago = datetime.now() - timedelta(days=grace_period_months * 30)
        grace_period_end_date_str = six_months_ago.strftime('%Y-%m-%d')
        chinese_char_regex = re.compile(r'[\u4e00-\u9fff]')

        for work in works:
            media_id = work.get('id')
            if not media_id or media_id in handled_media_ids:
                continue
            
            release_date_str = work.get('release_date') or work.get('first_air_date', '')
            if not release_date_str: continue
            
            try:
                if int(release_date_str.split('-')[0]) < config_start_year: continue
            except (ValueError, IndexError): pass

            media_type_raw = work.get('media_type', 'movie' if 'title' in work else 'tv')
            media_type = MediaType.MOVIE.value if media_type_raw == 'movie' else MediaType.SERIES.value
            if media_type not in config_media_types:
                continue

            genre_ids = set(work.get('genre_ids', []))
            if config_genres_exclude and not genre_ids.isdisjoint(config_genres_exclude): continue
            if config_genres_include and genre_ids.isdisjoint(config_genres_include): continue

            if config_min_rating > 0:
                vote_average = work.get('vote_average', 0.0)
                # vote_count = work.get('vote_count', 0) # 移除投票数条件
                
                is_new_movie = release_date_str >= grace_period_end_date_str
                
                if not is_new_movie:
                    # 对于老片，只根据评分进行过滤，不再考虑投票数
                    if vote_average < config_min_rating:
                        logger.trace(f"  -> 过滤老片: '{work.get('title') or work.get('name')}' (评分 {vote_average} < {config_min_rating})")
                        continue
            
            title = work.get('title') or work.get('name', '')
            if not chinese_char_regex.search(title):
                logger.trace(f"  -> 过滤作品: '{title}' (排除无中文片名)。")
                continue
            
            handled_media_ids.add(media_id)
            filtered.append(work)
            
        return filtered

    def _determine_media_status(self, work: Dict, emby_tmdb_ids: Set[str], today_str: str, old_status: Optional[str], session_subscribed_ids: Set[str]) -> Optional[MediaStatus]:
        """
        【V2 - 逻辑简化版】
        判断单个作品的当前状态。不再触发订阅，只负责标记状态。
        """
        media_id_str = str(work.get('id'))
        release_date_str = work.get('release_date') or work.get('first_air_date', '')

        # 1. 如果在 Emby 库中，状态就是 IN_LIBRARY
        if media_id_str in emby_tmdb_ids:
            return MediaStatus.IN_LIBRARY
        
        # 2. 如果之前已被标记为 SUBSCRIBED，则保持此状态，直到它入库
        if old_status == MediaStatus.SUBSCRIBED.value:
            return MediaStatus.SUBSCRIBED

        # 3. 如果还未上映，状态为 PENDING_RELEASE
        if release_date_str > today_str:
            return MediaStatus.PENDING_RELEASE
        
        # 4. 对于其他所有情况（已上映、未入库、未订阅），状态均为 MISSING
        #    实际的订阅操作将由“智能订阅”任务或用户手动触发
        return MediaStatus.MISSING

    def _prepare_media_dict(self, work: Dict, subscription_id: int, status: MediaStatus) -> Dict:
        """根据作品信息和状态，准备用于插入数据库的字典。"""
        media_type_raw = work.get('media_type', 'movie' if 'title' in work else 'tv')
        media_type = MediaType.SERIES if media_type_raw == 'tv' else MediaType.MOVIE
        
        return {
            'subscription_id': subscription_id,
            'tmdb_media_id': work.get('id'),
            'media_type': media_type.value,
            'title': work.get('title') or work.get('name'),
            'release_date': work.get('release_date') or work.get('first_air_date', ''),
            'poster_path': work.get('poster_path'),
            'status': status.value,
            'emby_item_id': None
        }

    def _update_database_records(self, cursor, subscription_id: int, to_insert: List[Dict], to_update: List[Dict], to_delete_ids: List[int]):
        """执行数据库的增、删、改操作。"""
        if to_insert:
            logger.info(f"  -> 新增 {len(to_insert)} 条作品记录。")
            # ★★★ 核心修改：SQL占位符从 :name 改为 %s
            sql_insert = (
                "INSERT INTO tracked_actor_media (subscription_id, tmdb_media_id, media_type, title, release_date, poster_path, status, emby_item_id, last_updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)"
            )
            # ★★★ 核心修改：将字典列表转换为元组列表以适配 psycopg2
            insert_data = [
                (d['subscription_id'], d['tmdb_media_id'], d['media_type'], d['title'], d['release_date'], d['poster_path'], d['status'], d['emby_item_id'])
                for d in to_insert
            ]
            cursor.executemany(sql_insert, insert_data)
        
        if to_update:
            logger.info(f"  -> 更新 {len(to_update)} 条作品记录的状态。")
            # ★★★ 核心修改：SQL占位符从 :name 改为 %s
            sql_update = (
                "UPDATE tracked_actor_media SET status = %s, last_updated_at = CURRENT_TIMESTAMP "
                "WHERE subscription_id = %s AND tmdb_media_id = %s"
            )
            # ★★★ 核心修改：将字典列表转换为元组列表，并注意顺序
            update_data = [
                (d['status'], d['subscription_id'], d['tmdb_media_id'])
                for d in to_update
            ]
            cursor.executemany(sql_update, update_data)

        if to_delete_ids:
            logger.info(f"  -> 删除 {len(to_delete_ids)} 条过时的作品记录。")
            delete_params = [(subscription_id, media_id) for media_id in to_delete_ids]
            # ★★★ 核心修改：SQL占位符从 ? 改为 %s
            cursor.executemany(
                "DELETE FROM tracked_actor_media WHERE subscription_id = %s AND tmdb_media_id = %s",
                delete_params
            )
        
        # ★★★ 核心修改：SQL占位符从 ? 改为 %s
        cursor.execute("UPDATE actor_subscriptions SET last_checked_at = CURRENT_TIMESTAMP WHERE id = %s", (subscription_id,))