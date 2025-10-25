# actor_subscription_processor.py

import time
import re
from datetime import datetime, timedelta
import logging
from typing import Optional, Dict, Any, List, Set, Callable
import threading
from enum import Enum
import concurrent.futures # 新增：导入 concurrent.futures

import tmdb_handler
import emby_handler
from database.connection import get_db_connection
import moviepilot_handler
import constants

logger = logging.getLogger(__name__)

class MediaStatus(Enum):
    IN_LIBRARY = 'IN_LIBRARY'
    PENDING_RELEASE = 'PENDING_RELEASE'
    SUBSCRIBED = 'SUBSCRIBED'
    MISSING = 'MISSING'
    IGNORED = 'IGNORED'

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
            logger.info("  ➜ 没有找到需要处理的演员订阅，任务结束。")
            _update_status(100, "没有需要处理的演员订阅。")
            return
            
        total_subs = len(subs_to_process)
        logger.info(f"  ➜ 共找到 {total_subs} 个启用的订阅需要处理。")
        
        _update_status(5, "  ➜ 正在从 Emby 获取媒体库信息...")
        logger.info("  ➜ 正在从 Emby 一次性获取全量媒体库数据...")
        emby_media_map: Dict[str, str] = {}
        try:
            all_libraries = emby_handler.get_emby_libraries(self.emby_url, self.emby_api_key, self.emby_user_id)
            library_ids_to_scan = [lib['Id'] for lib in all_libraries if lib.get('CollectionType') in ['movies', 'tvshows']]
            emby_items = emby_handler.get_emby_library_items(base_url=self.emby_url, api_key=self.emby_api_key, user_id=self.emby_user_id, library_ids=library_ids_to_scan, media_type_filter="Movie,Series")
            
            if self.is_stop_requested():
                logger.info("任务在获取Emby媒体库后被用户中断。")
                return

            emby_media_map = {
                item['ProviderIds']['Tmdb']: item['Id']
                for item in emby_items
                if item.get('ProviderIds', {}).get('Tmdb')
            }
            logger.debug(f"  ➜ 已从 Emby 获取 {len(emby_media_map)} 个已入库媒体的 TMDb ID 与 Emby ID 映射。")
        except Exception as e:
            logger.error(f"  ➜ 从 Emby 获取媒体库信息时发生严重错误: {e}", exc_info=True)
            _update_status(-1, "错误：连接 Emby 或获取数据失败。")
            return

        session_subscribed_ids: Set[str] = set()

        for i, sub in enumerate(subs_to_process):
            if self.is_stop_requested():
                logger.info("定时刷新演员订阅任务被用户中断。")
                break
            
            progress = int(5 + ((i + 1) / total_subs) * 95)
            message = f"  ➜ ({i+1}/{total_subs}) 正在扫描演员: {sub['actor_name']}"
            _update_status(progress, message)
            logger.info(message)
            
            self.run_full_scan_for_actor(sub['id'], emby_media_map, session_subscribed_ids)
            
            if not self.is_stop_requested() and i < total_subs - 1:
                time.sleep(1) 
                
        if not self.is_stop_requested():
            logger.trace("--- 定时刷新演员订阅任务执行完毕 ---")
            _update_status(100, "  ➜ 所有订阅扫描完成。")


    def run_full_scan_for_actor(self, subscription_id: int, emby_media_map: Dict[str, str], session_subscribed_ids: Optional[Set[str]] = None):
        if session_subscribed_ids is None:
            session_subscribed_ids = set()

        logger.trace(f"--- 开始为订阅ID {subscription_id} 执行全量作品扫描 ---")
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM actor_subscriptions WHERE id = %s", (subscription_id,))
                sub = cursor.fetchone()
                if not sub: return
                
                logger.trace(f"  ➜ 正在处理演员: {sub['actor_name']} (TMDb ID: {sub['tmdb_person_id']})")

                old_tracked_media = self._get_existing_tracked_media(cursor, subscription_id)
                
                credits = tmdb_handler.get_person_credits_tmdb(sub['tmdb_person_id'], self.tmdb_api_key)
                if self.is_stop_requested() or not credits: return
                
                movie_works = credits.get('movie_credits', {}).get('cast', [])
                tv_works = credits.get('tv_credits', {}).get('cast', [])
                for work in movie_works: work['media_type'] = 'movie'
                for work in tv_works: work['media_type'] = 'tv'
                all_works_raw = movie_works + tv_works
                unique_works = []
                seen_media_ids = set()
                for work in all_works_raw:
                    media_id = work.get('id')
                    if media_id and media_id not in seen_media_ids:
                        unique_works.append(work)
                        seen_media_ids.add(media_id)
                
                # 使用去重后的列表进行后续所有操作
                all_works_raw = unique_works
                logger.info(f"  ➜ 从TMDb获取到演员 {sub['actor_name']} 的 {len(all_works_raw)} 部原始作品记录。")

                emby_tmdb_ids_str = {str(id) for id in emby_media_map.keys() if id}
                
                media_to_insert = []
                media_to_update = []
                today_str = datetime.now().strftime('%Y-%m-%d')

                # --- 核心逻辑 V4：分离“已追踪”和“全新”的作品 ---
                
                works_for_status_update = []
                new_candidate_works = []
                for work in all_works_raw:
                    if work.get('id') in old_tracked_media:
                        works_for_status_update.append(work)
                    else:
                        new_candidate_works.append(work)

                # 1. 处理已追踪的作品 (极快)
                # 只检查状态变更，例如从 MISSING -> IN_LIBRARY
                logger.info(f"  ➜ {len(works_for_status_update)} 部是已追踪作品，仅检查状态更新。")
                for work in works_for_status_update:
                    media_id = work.get('id')
                    old_status = old_tracked_media.get(media_id)
                    if old_status == MediaStatus.IGNORED.value:
                        continue
                    
                    current_status = self._determine_media_status(work, emby_tmdb_ids_str, today_str, old_status, session_subscribed_ids)
                    if old_status != current_status.value:
                        # ★★★ 核心修改：如果状态变为 IN_LIBRARY，则查找并添加 emby_item_id ★★★
                        update_dict = {'status': current_status.value, 'subscription_id': subscription_id, 'tmdb_media_id': media_id}
                        if current_status == MediaStatus.IN_LIBRARY:
                            emby_id = emby_media_map.get(str(media_id))
                            update_dict['emby_item_id'] = emby_id
                        else:
                            # 如果状态从 IN_LIBRARY 变为其他（例如用户删除了文件），则清空 emby_item_id
                            update_dict['emby_item_id'] = None
                        media_to_update.append(update_dict)

                # 2. 处理全新的作品 (需要严格筛选)
                if new_candidate_works:
                    logger.info(f"  ➜ 发现 {len(new_candidate_works)} 部全新作品，将进行首次严格筛选...")
                    
                    # 执行完整的筛选流程
                    pre_filtered_new = self._pre_filter_works(new_candidate_works, sub)
                    enriched_new = self._enrich_works_with_order(pre_filtered_new, sub['tmdb_person_id'], self.tmdb_api_key)
                    final_new_works = self._post_filter_works_by_role(enriched_new, sub)
                    
                    final_new_ids = {w['id'] for w in final_new_works}
                    ignored_new_works = [w for w in new_candidate_works if w.get('id') not in final_new_ids]

                    logger.info(f"  ➜ 筛选后，有 {len(final_new_works)} 部新作品符合条件。")
                    logger.info(f"  ➜ 其余 {len(ignored_new_works)} 部新作品将被标记为'忽略'，未来不再处理。")

                    # 为符合条件的新作品确定初始状态 (MISSING/PENDING/IN_LIBRARY)
                    for work in final_new_works:
                        status = self._determine_media_status(work, emby_tmdb_ids_str, today_str, None, session_subscribed_ids)
                        # ★★★ 核心修改：为新作品准备字典时，如果已在库，则直接传入 emby_item_id ★★★
                        emby_id = emby_media_map.get(str(work.get('id'))) if status == MediaStatus.IN_LIBRARY else None
                        media_to_insert.append(self._prepare_media_dict(work, subscription_id, status, emby_id))

                    # 为被忽略的新作品直接标记为 IGNORED
                    for work in ignored_new_works:
                        media_to_insert.append(self._prepare_media_dict(work, subscription_id, MediaStatus.IGNORED))
                
                # --- 统一的数据库更新 ---
                # 在这个逻辑下，old_tracked_media 不再需要 pop，因为我们是基于新旧分离来处理的
                # 删除逻辑也需要调整：只删除那些在 TMDB 列表中已不存在的旧记录
                tmdb_ids_set = {work.get('id') for work in all_works_raw}
                media_ids_to_delete = [media_id for media_id in old_tracked_media.keys() if media_id not in tmdb_ids_set]

                self._update_database_records(cursor, subscription_id, media_to_insert, media_to_update, media_ids_to_delete)
                conn.commit()
                logger.info(f"  ✅ {sub['actor_name']} 的处理成功完成 ---")

        except Exception as e:
            logger.error(f"为订阅ID {subscription_id} 执行扫描时发生严重错误: {e}", exc_info=True)

    def _get_existing_tracked_media(self, cursor, subscription_id: int) -> Dict[int, str]:
        """从数据库获取当前已追踪的媒体及其状态。"""
        # ★★★ 核心修改：SQL占位符从 ? 改为 %s
        cursor.execute("SELECT tmdb_media_id, status FROM tracked_actor_media WHERE subscription_id = %s", (subscription_id,))
        return {row['tmdb_media_id']: row['status'] for row in cursor.fetchall()}

    def _pre_filter_works(self, works: List[Dict], sub_config) -> List[Dict]:
        """【新增】根据订阅配置对作品进行初步筛选（不依赖番位）。"""
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

            # 筛选1：上映日期年份
            release_date_str = work.get('release_date') or work.get('first_air_date', '')
            if not release_date_str: continue
            try:
                if int(release_date_str.split('-')[0]) < config_start_year: continue
            except (ValueError, IndexError): pass

            # 筛选2：媒体类型
            media_type_raw = work.get('media_type', 'movie' if 'title' in work else 'tv')
            media_type = MediaType.MOVIE.value if media_type_raw == 'movie' else MediaType.SERIES.value
            if media_type not in config_media_types:
                continue

            # 筛选3：题材
            genre_ids = set(work.get('genre_ids', []))
            if config_genres_exclude and not genre_ids.isdisjoint(config_genres_exclude): continue
            if config_genres_include and genre_ids.isdisjoint(config_genres_include): continue

            # 筛选4：评分
            if config_min_rating > 0:
                vote_average = work.get('vote_average', 0.0)
                is_new_movie = release_date_str >= grace_period_end_date_str
                if not is_new_movie and vote_average < config_min_rating:
                    logger.trace(f"  ➜ 过滤老片: '{work.get('title') or work.get('name')}' (评分 {vote_average} < {config_min_rating})")
                    continue
            
            # 筛选5：中文片名
            title = work.get('title') or work.get('name', '')
            if not chinese_char_regex.search(title):
                logger.trace(f"  ➜ 过滤作品: '{title}' (排除无中文片名)。")
                continue
            
            handled_media_ids.add(media_id)
            filtered.append(work)
            
        return filtered
    
    def _post_filter_works_by_role(self, works: List[Dict], sub_config) -> List[Dict]:
        """ 在获取番位后，根据主演配置对作品进行最终筛选。"""
        config_main_role_only = sub_config.get('config_main_role_only', False)
        
        # 如果用户没有勾选“只看主演”，直接返回所有作品，不做任何操作
        if not config_main_role_only:
            return works

        filtered = []
        for work in works:
            cast_order = work.get('order', 999) # 此时的 work 已经由 _enrich_works_with_order 处理过
            if cast_order >= 3:
                logger.trace(f"  ➜ 过滤非主演作品: '{work.get('title') or work.get('name')}' (番位: {cast_order} >= 3)")
                continue
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

    def _prepare_media_dict(self, work: Dict, subscription_id: int, status: MediaStatus, emby_item_id: Optional[str] = None) -> Dict:
        """根据作品信息和状态，准备用于插入数据库的字典。"""
        media_type_raw = work.get('media_type', 'movie' if 'title' in work else 'tv')
        media_type = MediaType.SERIES if media_type_raw == 'tv' else MediaType.MOVIE
        
        release_date = work.get('release_date') or work.get('first_air_date')
        if not release_date:
            release_date = None

        return {
            'subscription_id': subscription_id,
            'tmdb_media_id': work.get('id'),
            'media_type': media_type.value,
            'title': work.get('title') or work.get('name'),
            'release_date': release_date, 
            'poster_path': work.get('poster_path'),
            'status': status.value,
            'emby_item_id': emby_item_id
        }

    def _update_database_records(self, cursor, subscription_id: int, to_insert: List[Dict], to_update: List[Dict], to_delete_ids: List[int]):
        """执行数据库的增、删、改操作。"""
        if to_insert:
            logger.info(f"  ➜ 新增 {len(to_insert)} 条作品记录。")
            sql_insert = (
                "INSERT INTO tracked_actor_media (subscription_id, tmdb_media_id, media_type, title, release_date, poster_path, status, emby_item_id, last_updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)"
            )
            insert_data = [
                (d['subscription_id'], d['tmdb_media_id'], d['media_type'], d['title'], d['release_date'], d['poster_path'], d['status'], d['emby_item_id'])
                for d in to_insert
            ]
            cursor.executemany(sql_insert, insert_data)
        
        if to_update:
            logger.info(f"  ➜ 更新 {len(to_update)} 条作品记录的状态。")
            sql_update = (
                "UPDATE tracked_actor_media SET status = %s, emby_item_id = %s, last_updated_at = CURRENT_TIMESTAMP "
                "WHERE subscription_id = %s AND tmdb_media_id = %s"
            )
            update_data = [
                (d['status'], d.get('emby_item_id'), d['subscription_id'], d['tmdb_media_id'])
                for d in to_update
            ]
            cursor.executemany(sql_update, update_data)

        if to_delete_ids:
            logger.info(f"  ➜ 删除 {len(to_delete_ids)} 条过时的作品记录。")
            delete_params = [(subscription_id, media_id) for media_id in to_delete_ids]
            cursor.executemany(
                "DELETE FROM tracked_actor_media WHERE subscription_id = %s AND tmdb_media_id = %s",
                delete_params
            )
        
        cursor.execute("UPDATE actor_subscriptions SET last_checked_at = CURRENT_TIMESTAMP WHERE id = %s", (subscription_id,))

    def _enrich_works_with_order(self, works: List[Dict], tmdb_person_id: int, api_key: str) -> List[Dict]:
        """
        【新增】通过并发请求，为演员的作品列表补充其在作品中的 'order' 字段。
        """
        if not works:
            return []

        logger.info(f"  ➜ 正在为 {len(works)} 部作品并发获取演员番位信息...")
        
        enriched_works = []
        
        # 分离需要额外查询的作品和不需要查询的作品
        works_to_fetch_credits = []
        works_already_have_order = []

        for work in works:
            # 电影作品的 order 字段通常在 get_person_credits_tmdb 返回的原始数据中
            # 电视剧作品的 order 字段则需要额外查询
            if work.get('media_type') == 'movie':
                # 电影作品直接使用原始数据中的 order，如果不存在则为 999
                work['order'] = work.get('order', 999)
                works_already_have_order.append(work)
            elif work.get('media_type') == 'tv':
                works_to_fetch_credits.append(work)
            else:
                # 未知类型或无 media_type 的作品，也给默认 order
                work['order'] = 999
                works_already_have_order.append(work)

        # 处理不需要额外查询的作品
        enriched_works.extend(works_already_have_order)

        if not works_to_fetch_credits:
            logger.info("  ➜ 没有需要额外获取番位信息的电视剧作品。")
            return enriched_works

        logger.info(f"  ➜ 正在为 {len(works_to_fetch_credits)} 部电视剧作品并发获取演员番位信息...")
        
        # 使用线程池并发获取电视剧作品的详细信息
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_work = {
                executor.submit(self._fetch_tv_work_credits, work, api_key): work
                for work in works_to_fetch_credits
            }

            for i, future in enumerate(concurrent.futures.as_completed(future_to_work)):
                original_work = future_to_work[future]
                if self.is_stop_requested():
                    logger.info("任务在获取作品番位信息时被中断。")
                    break
                try:
                    detailed_credits = future.result()
                    if detailed_credits:
                        cast_list = detailed_credits.get('cast', [])
                        actor_order = 999
                        for cast_member in cast_list:
                            if cast_member.get('id') == tmdb_person_id:
                                actor_order = cast_member.get('order', 999)
                                break
                        original_work['order'] = actor_order
                    else:
                        original_work['order'] = 999
                except Exception as exc:
                    logger.error(f"  ➜ 获取电视剧作品 '{original_work.get('title') or original_work.get('name')}' 的番位信息时发生错误: {exc}", exc_info=True)
                    original_work['order'] = 999
                
                enriched_works.append(original_work)
        
        logger.info(f"  ➜ 已为 {len(works_to_fetch_credits)} 部电视剧作品补充番位信息。")
        return enriched_works

    def _fetch_tv_work_credits(self, work: Dict, api_key: str) -> Optional[Dict[str, Any]]:
        """
        【新增】辅助函数：专门用于获取电视剧作品的详细 credits 信息。
        """
        media_id = work.get('id')
        if not media_id:
            return None
        
        details = tmdb_handler.get_tv_details(media_id, api_key, append_to_response="credits")
        
        if details:
            logger.debug(f"  ➜ 获取电视剧 '{work.get('title') or work.get('name')}' (ID: {media_id}) 详情成功。")
            # 调试：打印完整的 details 和 credits 部分
            # logger.debug(f"    完整详情: {details}")
            # logger.debug(f"    Credits 部分: {details.get('credits')}")
            return details.get('credits')
        else:
            logger.warning(f"  ➜ 获取电视剧 '{work.get('title') or work.get('name')}' (ID: {media_id}) 详情失败。")
            return None
