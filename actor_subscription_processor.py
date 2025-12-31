# actor_subscription_processor.py

import time
import re
import json
from datetime import datetime
import logging
from typing import Optional, Dict, Any, List, Set, Callable, Tuple
import threading
from enum import Enum
import concurrent.futures 

import handler.tmdb as tmdb
from tasks.helpers import parse_series_title_and_season, process_subscription_items_and_update_db
from database.connection import get_db_connection
from database import media_db, request_db, actor_db
import constants
import utils

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
        """
        - 演员订阅扫描任务。
        - 并发为所有已启用的订阅执行完整扫描，大幅提升二次扫描和首次扫描的速度。
        """
        def _update_status(progress, message):
            if update_status_callback:
                safe_progress = max(0, min(100, int(progress)))
                update_status_callback(safe_progress, message)

        logger.info("--- 开始执行演员订阅任务 ---")
        _update_status(0, "正在获取所有已启用的订阅...")

        # --- 步骤 1: 获取所有需要处理的订阅 ---
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # 我们只需要 id 即可，因为子任务会自己去查详情
                    cursor.execute("SELECT id FROM actor_subscriptions WHERE status = 'active' ORDER BY actor_name")
                    subs_to_process = cursor.fetchall()
        except Exception as e:
            logger.error(f"任务调度：获取已启用的订阅列表时失败: {e}", exc_info=True)
            _update_status(-1, "错误：获取订阅列表失败。")
            return
            
        if not subs_to_process:
            logger.info("  ➜ 没有找到任何已启用的演员订阅，任务结束。")
            _update_status(100, "没有已启用的演员订阅。")
            return
            
        total_subs = len(subs_to_process)
        logger.info(f"  ➜ 共找到 {total_subs} 个已启用的订阅需要并发处理。")
        
        # --- 步骤 2: 一次性预加载所有任务共享的媒体库缓存 ---
        _update_status(5, "正在从本地数据库缓存媒体信息...")
        logger.info("  ➜ 正在从 media_metadata 表一次性获取全量在库媒体及剧集结构数据...")
        try:
            # 注意：process_subscription_items_and_update_db 主要依赖 emby_media_map (tmdb_id -> emby_id)
            # 来判断是否在库。
            (emby_media_map, 
             emby_series_seasons_map, 
             emby_series_name_to_tmdb_id_map) = actor_db.get_all_in_library_media_for_actor_sync()
            logger.info(f"  ➜ 从数据库成功加载 {len(emby_media_map)} 个媒体映射。")
        except Exception as e:
            logger.error(f"  ➜ 从 media_metadata 获取媒体库信息时发生严重错误: {e}", exc_info=True)
            _update_status(-1, "错误：读取本地数据库失败。")
            return

        # --- 步骤 3: ★★★ 使用线程池并发执行所有演员的扫描任务 ★★★ ---
        processed_count = 0
        # 使用较少的 workers (如5) 可以避免因并发过高而触发 TMDb 的 API 速率限制
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            
            # 提交所有任务到线程池
            future_to_sub_id = {
                executor.submit(
                    self.run_full_scan_for_actor, 
                    sub['id'], 
                    emby_media_map
                ): sub['id'] 
                for sub in subs_to_process
            }

            # 实时处理已完成的任务
            for future in concurrent.futures.as_completed(future_to_sub_id):
                if self.is_stop_requested():
                    logger.info("演员订阅任务被用户中断。")
                    # 尝试取消尚未开始的任务
                    for f in future_to_sub_id: f.cancel()
                    break
                
                processed_count += 1
                progress = int(10 + (processed_count / total_subs) * 90)
                message = f"({processed_count}/{total_subs}) 已完成一个演员的扫描..."
                _update_status(progress, message)
                
                try:
                    future.result() 
                except Exception as exc:
                    sub_id = future_to_sub_id[future]
                    logger.error(f"  ➜ 订阅ID {sub_id} 的扫描任务在线程内发生异常: {exc}", exc_info=True)
                
        # --- 步骤 4: 任务结束 ---
        if not self.is_stop_requested():
            logger.info("--- 演员订阅任务 (并发调度模式) 执行完毕 ---")
            _update_status(100, "所有订阅扫描完成。")


    def _process_single_work(self, work: Dict, sub_config: Dict) -> List[Dict[str, Any]]:
        """
        【重构】处理单个作品。
        1. 补充详情（番位、季信息）。
        2. 执行过滤（年份、题材、番位）。
        3. 如果通过，返回符合 helper 函数要求的条目定义列表。
           格式: [{'tmdb_id': '...', 'media_type': '...', 'season': ...}]
        """
        valid_items = []
        try:
            tmdb_id = str(work.get('id'))
            media_type = MediaType.SERIES.value if work.get('media_type') == 'tv' else MediaType.MOVIE.value
            
            # 1. 基础过滤 (无需详情即可判断的，如年份、中文名)
            is_kept, reason = self._filter_work_and_get_reason(work, sub_config, check_order=False)
            if not is_kept:
                # 记录日志可选
                return []

            # 2. 获取详情 (番位 + 季信息)
            # 注意：这里传入 list 返回 list[0]
            enriched_work = self._enrich_works_with_order([work], sub_config['tmdb_person_id'], self.tmdb_api_key)[0]
            
            # 3. 最终过滤 (含番位、题材等需要详情的检查)
            is_kept_final, reason_final = self._filter_work_and_get_reason(enriched_work, sub_config, check_order=True)
            if not is_kept_final:
                return []

            # 4. 构建返回条目
            if media_type == MediaType.SERIES.value:
                # 检查是否为不规范的单季条目 (如 "xx之xx")
                title = enriched_work.get('name', '')
                base_name, season_num_parsed = parse_series_title_and_season(title, self.tmdb_api_key)
                if base_name and season_num_parsed:
                    logger.info(f"  ➜ 作品 '{title}' 被识别为不规范的分季条目，跳过。")
                    return []

                seasons = enriched_work.get('seasons', [])
                for season in seasons:
                    s_num = season.get('season_number')
                    # 跳过特别篇 (第0季)
                    if s_num is None or s_num == 0:
                        continue
                    
                    valid_items.append({
                        'tmdb_id': tmdb_id,
                        'media_type': 'Series',
                        'season': s_num
                    })
            else:
                # 电影
                valid_items.append({
                    'tmdb_id': tmdb_id,
                    'media_type': 'Movie',
                    'season': None
                })
            
            return valid_items

        except Exception as e:
            item_name = work.get('title') or work.get('name', '未知作品')
            logger.error(f"  ➜ (线程内) 处理作品 '{item_name}' 时发生错误: {e}", exc_info=True)
            return []

    def run_full_scan_for_actor(self, subscription_id: int, emby_media_map: Dict[str, str]):
        """
        - 采用并发模型处理所有新增作品。
        - 筛选出满足条件的条目后，统一调用 helpers.process_subscription_items_and_update_db 处理。
        """
        actor_name_for_log = f"订阅ID {subscription_id}"
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # --- 步骤 1: 获取订阅规则 ---
                    cursor.execute("SELECT *, last_scanned_tmdb_ids_json FROM actor_subscriptions WHERE id = %s FOR UPDATE", (subscription_id,))
                    sub = cursor.fetchone()
                    if not sub: return
                    
                    actor_name_for_log = sub.get('actor_name', actor_name_for_log)
                    logger.info(f"--- 开始为演员 '{actor_name_for_log}' 执行作品扫描 ---")
                    
                    last_scanned_ids = set(sub.get('last_scanned_tmdb_ids_json') or [])
                    subscription_source = {
                        "type": "actor_subscription", 
                        "id": subscription_id, 
                        "name": sub['actor_name'], 
                        "person_id": sub['tmdb_person_id']
                    }

                    # --- 步骤 2: 获取TMDb全量作品 ---
                    logger.info(f"  ➜ [阶段 1/3] 正在从 TMDb 获取演员 '{sub['actor_name']}' 的所有作品...")
                    all_works = self._get_and_clean_actor_works(sub['tmdb_person_id'], self.tmdb_api_key)
                    if self.is_stop_requested(): return
                    if not all_works:
                        cursor.execute("UPDATE actor_subscriptions SET last_scanned_tmdb_ids_json = '[]' WHERE id = %s", (subscription_id,))
                        return

                    # --- 步骤 3: 计算差量 ---
                    current_work_ids = {str(w.get('id')) for w in all_works if w.get('id')}
                    new_work_ids = current_work_ids - last_scanned_ids
                    removed_work_ids = last_scanned_ids - current_work_ids
                    
                    works_to_process = [w for w in all_works if str(w.get('id')) in new_work_ids]
                    
                    logger.info(f"  ➜ [阶段 1/3] 差量计算完成：发现 {len(new_work_ids)} 部新作品，{len(removed_work_ids)} 部作品已从TMDb移除。")

                    if not works_to_process and not removed_work_ids:
                        logger.info(f"  ➜ 演员 '{sub['actor_name']}' 的作品列表无变化，跳过。")
                        return

                    # --- 步骤 4: 并发筛选作品 ---
                    tmdb_items_to_subscribe = []
                    
                    if works_to_process:
                        logger.info(f"  ➜ [阶段 2/3] 正在并发筛选 {len(works_to_process)} 部新作品 (检查题材、番位等)...")
                        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                            future_to_work = {
                                executor.submit(self._process_single_work, work, sub): work 
                                for work in works_to_process
                            }
                            for future in concurrent.futures.as_completed(future_to_work):
                                if self.is_stop_requested(): break
                                try:
                                    # 获取该作品下所有满足条件的条目 (例如剧集的多个季)
                                    valid_items = future.result() 
                                    if valid_items:
                                        tmdb_items_to_subscribe.extend(valid_items)
                                except Exception as e:
                                    logger.error(f"任务异常: {e}")

                    # --- 步骤 5: 调用通用 Helper 进行处理 ---
                    if tmdb_items_to_subscribe:
                        logger.info(f"  ➜ [阶段 3/3] 筛选完成，将 {len(tmdb_items_to_subscribe)} 个有效条目提交给通用订阅处理器...")
                        
                        # ★★★ 核心调用：使用 helpers.py 中的通用逻辑 ★★★
                        # 这将处理：在库检查、父剧集元数据、状态判断(Wanted/Pending)、写入 request_db
                        process_subscription_items_and_update_db(
                            tmdb_items=tmdb_items_to_subscribe,
                            tmdb_to_emby_item_map=emby_media_map,
                            subscription_source=subscription_source,
                            tmdb_api_key=self.tmdb_api_key
                        )
                    else:
                        logger.info(f"  ➜ [阶段 3/3] 没有符合订阅条件的新条目。")

                    # --- 步骤 6: 清理过时记录 ---
                    if removed_work_ids:
                        logger.info(f"  ➜ 发现 {len(removed_work_ids)} 个过时的追踪记录，将为其解绑...")
                        old_items_details = media_db.get_media_details_by_tmdb_ids(list(removed_work_ids))
                        for tmdb_id_to_clean in removed_work_ids:
                            item_info = old_items_details.get(tmdb_id_to_clean)
                            if item_info:
                                request_db.remove_subscription_source(tmdb_id_to_clean, item_info['item_type'], subscription_source)

                    # --- 步骤 7: 更新扫描记录 ---
                    cursor.execute(
                        "UPDATE actor_subscriptions SET last_scanned_tmdb_ids_json = %s WHERE id = %s",
                        (json.dumps(list(current_work_ids)), subscription_id)
                    )
                    
                    conn.commit()
                    logger.info(f"  ✅ 演员 '{actor_name_for_log}' 的扫描更新成功完成 ---")

        except Exception as e:
            logger.error(f"为订阅ID {actor_name_for_log} 执行扫描时发生严重错误: {e}", exc_info=True)

    
    def _filter_work_and_get_reason(self, work: Dict, sub_config, check_order: bool = True) -> Tuple[bool, Optional[str]]:
        """
        【V2 - 终极修复版】
        对单个作品进行完整筛选。新增 check_order 参数，以支持高效的“分阶段过滤”策略。
        """
        # --- 筛选 1: 上映日期年份 ---
        # 检查作品的发行年份是否晚于用户在订阅规则中设置的“起始年份”。
        config_start_year = sub_config['config_start_year']
        release_date_str = work.get('release_date') or work.get('first_air_date', '')
        try:
            # 只取年份部分进行比较
            if int(release_date_str.split('-')[0]) < config_start_year:
                return False, f"发行年份早于 {config_start_year}"
        except (ValueError, IndexError):
            # 如果日期格式不正确或为空，则跳过此检查
            pass

        # --- 筛选 2: 媒体类型 (电影/剧集) ---
        # 检查作品的类型是否在用户允许的类型列表中。
        media_type_raw = work.get('media_type', 'movie' if 'title' in work else 'tv')
        media_type = MediaType.MOVIE.value if media_type_raw == 'movie' else MediaType.SERIES.value
        raw_types_from_db = sub_config['config_media_types'].split(',')
        config_media_types = {
            'Series' if t.strip().lower() == 'tv' else t.strip().capitalize()
            for t in raw_types_from_db if t.strip()
        }
        if media_type not in config_media_types:
            return False, "排除的媒体类型"

        # --- 筛选 3: 题材 (包含/排除) ---
        # 检查作品的题材是否命中了“排除列表”，或者是否未能满足“包含列表”。
        config_genres_include = set(sub_config['config_genres_include_json'] or [])
        config_genres_exclude = set(sub_config['config_genres_exclude_json'] or [])
        genre_ids = set(work.get('genre_ids', []))
        if config_genres_exclude and not genre_ids.isdisjoint(config_genres_exclude):
            return False, "排除的题材"
        if config_genres_include and genre_ids.isdisjoint(config_genres_include):
            return False, "不包含指定的题材"

        # --- 筛选 4: 评分和评价人数 ---
        # 检查作品的评分是否高于用户设置的阈值，同时考虑评价人数是否过少。
        config_min_rating = sub_config['config_min_rating']
        if config_min_rating > 0:
            tmdb_rating = work.get('vote_average', 0.0)
            vote_count = work.get('vote_count', 0)
            min_vote_count_threshold = sub_config.get('config_min_vote_count', 10)
            
            # 如果评价人数不足或评分为0，则豁免评分检查
            is_exempted = (vote_count < min_vote_count_threshold) or (tmdb_rating == 0.0)
            
            if not is_exempted and tmdb_rating < config_min_rating:
                return False, f"评分过低 ({tmdb_rating:.1f}, {vote_count}人评价)"

        # --- 筛选 5: 中文片名 ---
        # 检查作品的标题是否至少包含一个中文字符。
        chinese_char_regex = re.compile(r'[\u4e00-\u9fff]')
        title = work.get('title') or work.get('name', '')
        if not chinese_char_regex.search(title):
            return False, "缺少中文标题"

        # --- 筛选 6: 主演番位 (昂贵操作) ---
        # ★★★ 核心修复：只有在被明确要求时，才执行这个检查 ★★★
        if check_order:
            config_main_role_only = sub_config.get('config_main_role_only', False)
            if config_main_role_only:
                # 'order' 字段由 _enrich_works_with_order 函数补充
                cast_order = work.get('order', 999) 
                # 通常前三位算主演 (order 0, 1, 2)
                if cast_order >= 3:
                    return False, f"非主演 (番位: {cast_order+1})"

        # 如果所有检查都通过了，就返回 True
        return True, None

    def _enrich_works_with_order(self, works: List[Dict], tmdb_person_id: int, api_key: str) -> List[Dict]:
        """
        【新增】通过并发请求，为演员的作品列表补充其在作品中的 'order' 字段。
        """
        if not works:
            return []

        # 分离需要额外查询的作品和不需要查询的作品
        works_to_fetch_credits = []
        works_already_have_order = []
        enriched_works = []

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
            return enriched_works

        # 使用线程池并发获取电视剧作品的详细信息
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_work = {
                executor.submit(self._fetch_tv_work_credits, work, api_key): work
                for work in works_to_fetch_credits
            }

            for i, future in enumerate(concurrent.futures.as_completed(future_to_work)):
                original_work = future_to_work[future]
                if self.is_stop_requested():
                    break
                try:
                    details = future.result() 
                    if details:
                        # 1. 提取番位：注意现在 credits 在 details['credits'] 里
                        credits_data = details.get('credits', {})
                        cast_list = credits_data.get('cast', [])
                        actor_order = 999
                        for cast_member in cast_list:
                            if cast_member.get('id') == tmdb_person_id:
                                actor_order = cast_member.get('order', 999)
                                break
                        original_work['order'] = actor_order
                        
                        # 2. 提取季信息
                        original_work['seasons'] = details.get('seasons', [])
                    else:
                        original_work['order'] = 999
                        original_work['seasons'] = []
                except Exception as exc:
                    logger.error(f"  ➜ 获取电视剧作品 '{original_work.get('title') or original_work.get('name')}' 的番位信息时发生错误: {exc}", exc_info=True)
                    original_work['order'] = 999
                
                enriched_works.append(original_work)
        
        return enriched_works

    def _fetch_tv_work_credits(self, work: Dict, api_key: str) -> Optional[Dict[str, Any]]:
        """
        辅助函数：专门用于获取电视剧作品的详细 credits 信息。
        """
        media_id = work.get('id')
        if not media_id:
            return None
        
        details = tmdb.get_tv_details(media_id, api_key, append_to_response="credits")
        return details
        
    def _get_and_clean_actor_works(self, tmdb_person_id: int, api_key: str) -> List[Dict[str, Any]]:
        """
        一个集成的函数，负责从TMDb获取演员作品，并立即进行垃圾过滤和去重。
        """
        credits = tmdb.get_person_credits_tmdb(tmdb_person_id, api_key)
        if not credits:
            return []

        movie_works = credits.get('movie_credits', {}).get('cast', [])
        tv_works = credits.get('tv_credits', {}).get('cast', [])
        
        for work in movie_works: work['media_type'] = 'movie'
        for work in tv_works: work['media_type'] = 'tv'
        
        all_works_raw = movie_works + tv_works

        work_groups = {}
        for work in all_works_raw:
            # 步骤 1: 垃圾过滤
            if not work.get('poster_path') or not (work.get('release_date') or work.get('first_air_date')):
                continue

            # 步骤 2: 去重
            if not work.get('id'): continue
            
            title = work.get('title') or work.get('name', '')
            normalized_title = utils.normalize_name_for_matching(title)
            if not normalized_title: continue
                
            if normalized_title not in work_groups:
                work_groups[normalized_title] = []
            work_groups[normalized_title].append(work)

        unique_works = []
        for title, group in work_groups.items():
            if len(group) == 1:
                unique_works.append(group[0])
            else:
                best_work = max(group, key=lambda x: x.get('popularity', 0))
                unique_works.append(best_work)

        return unique_works