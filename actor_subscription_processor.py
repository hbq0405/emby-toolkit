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
import handler.emby as emby
from database.connection import get_db_connection
from database import media_db, request_db
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
        """
        def _update_status(progress, message):
            if update_status_callback:
                safe_progress = max(0, min(100, int(progress)))
                update_status_callback(safe_progress, message)

        logger.info("--- 开始执行演员订阅任务 ---")
        _update_status(0, "正在获取所有已启用的订阅...")

        # --- 步骤 1: 获取所有需要处理的订阅 (逻辑不变) ---
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT id, actor_name FROM actor_subscriptions WHERE status = 'active' ORDER BY actor_name")
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
        logger.info(f"  ➜ 共找到 {total_subs} 个已启用的订阅需要处理。")
        
        # --- 步骤 2: ★★★ 核心重构：从 media_metadata 表构建所有需要的映射 ★★★ ---
        _update_status(5, "正在从本地数据库缓存媒体信息...")
        logger.info("  ➜ 正在从 media_metadata 表一次性获取全量在库媒体及剧集结构数据...")
        
        emby_media_map: Dict[str, str] = {}
        emby_series_seasons_map: Dict[str, Set[int]] = {}
        emby_series_name_to_tmdb_id_map: Dict[str, str] = {}
        
        try:
            # 直接调用一个专门的数据库函数来完成所有数据准备工作
            (emby_media_map, 
             emby_series_seasons_map, 
             emby_series_name_to_tmdb_id_map) = request_db.get_all_in_library_media_for_actor_sync()

            logger.info(f"  ➜ 从数据库成功加载 {len(emby_media_map)} 个媒体映射，{len(emby_series_seasons_map)} 个剧集季结构。")

        except Exception as e:
            logger.error(f"  ➜ 从 media_metadata 获取媒体库信息时发生严重错误: {e}", exc_info=True)
            _update_status(-1, "错误：读取本地数据库失败。")
            return

        # --- 步骤 3: 循环处理每一个订阅 (逻辑完全不变) ---
        for i, sub in enumerate(subs_to_process):
            if self.is_stop_requested():
                logger.info("演员订阅任务被用户中断。")
                break
            
            progress = int(10 + ((i + 1) / total_subs) * 90)
            message = f"({i+1}/{total_subs}) 正在扫描演员: {sub['actor_name']}"
            _update_status(progress, message)
            logger.info(message)
            
            self.run_full_scan_for_actor(
                sub['id'], 
                emby_media_map, 
                emby_series_seasons_map, 
                emby_series_name_to_tmdb_id_map
            )
            
            if not self.is_stop_requested() and i < total_subs - 1:
                time.sleep(1) 
                
        # --- 步骤 4: 任务结束 (逻辑不变) ---
        if not self.is_stop_requested():
            logger.info("--- 演员订阅任务 (数据中台模式) 执行完毕 ---")
            _update_status(100, "所有订阅扫描完成。")


    def run_full_scan_for_actor(self, subscription_id: int, emby_media_map: Dict[str, str], emby_series_seasons_map: Dict[str, Set[int]], emby_series_name_to_tmdb_id_map: Dict[str, str]):
        """
        - 采用“层层过滤”的高效策略，数据源完全来自预加载的 media_metadata 缓存。
        - 实现了极致的“差量更新”逻辑，每次只处理演员的新增或已移除作品。
        """
        actor_name_for_log = f"订阅ID {subscription_id}" # 设置一个备用名
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # --- 步骤 1: 获取订阅规则、演员名和上次扫描的缓存 ---
                    cursor.execute("SELECT *, last_scanned_tmdb_ids_json FROM actor_subscriptions WHERE id = %s FOR UPDATE", (subscription_id,))
                    sub = cursor.fetchone()
                    if not sub:
                        logger.warning(f"扫描跳过：在数据库中未找到订阅ID {subscription_id}。")
                        return
                    
                    # 获取到演员名后，立即更新日志备用名
                    actor_name_for_log = sub.get('actor_name', actor_name_for_log)
                    
                    # ★★★ 核心修改 2/3: 在主日志中使用演员名 ★★★
                    logger.info(f"--- 开始为演员 '{actor_name_for_log}' 执行作品扫描 ---")
                    
                    # 将上次扫描的ID列表（JSON）解析为一个集合(Set)，方便后续进行高效的差集运算。
                    last_scanned_ids = set(sub.get('last_scanned_tmdb_ids_json') or [])
                    
                    actor_name = sub['actor_name']
                    tmdb_person_id = sub['tmdb_person_id']
                    subscription_source = {
                        "type": "actor_subscription", "id": subscription_id, 
                        "name": actor_name, "person_id": tmdb_person_id
                    }

                    # --- 步骤 2: 从 TMDb 获取演员当前的全量作品，并进行基础清洗 ---
                    logger.info(f"  ➜ [阶段 1/5] 正在从 TMDb 获取演员 '{actor_name}' 的所有作品...")
                    all_works = self._get_and_clean_actor_works(tmdb_person_id, self.tmdb_api_key)
                    if self.is_stop_requested(): return
                    
                    # 如果从TMDb没有获取到任何作品，就没必要继续了。
                    if not all_works:
                        logger.warning(f"  ➜ 未能从TMDb获取到演员 '{actor_name}' 的任何有效作品。")
                        # 即使没作品，也更新一下缓存，防止下次还扫
                        cursor.execute("UPDATE actor_subscriptions SET last_scanned_tmdb_ids_json = '[]' WHERE id = %s", (subscription_id,))
                        return

                    # --- 步骤 3: 计算“差量”：找出新增的和已移除的作品 ---
                    # 将当前获取到的所有作品ID转换成一个集合。
                    current_work_ids = {str(w.get('id')) for w in all_works if w.get('id')}
                    
                    # 新增作品 = 当前作品集 - 上次扫描的作品集
                    new_work_ids = current_work_ids - last_scanned_ids
                    # 移除作品 = 上次扫描的作品集 - 当前作品集
                    removed_work_ids = last_scanned_ids - current_work_ids

                    logger.info(f"  ➜ [阶段 1/5] 差量计算完成：发现 {len(new_work_ids)} 部新作品，{len(removed_work_ids)} 部作品已从TMDb移除。")

                    # 筛选出真正需要处理的新作品对象列表。
                    works_to_process = [w for w in all_works if str(w.get('id')) in new_work_ids]
                    
                    # 如果没有任何新增或移除的作品，说明一切都没变，任务提前结束。
                    if not works_to_process and not removed_work_ids:
                        logger.info(f"  ➜ 演员 '{actor_name}' 的作品列表无变化，跳过。")
                        return

                    # --- 步骤 4: 对“新增作品”进行层层过滤 ---
                    # 4.1 首先处理在库的作品
                    logger.info(f"  ➜ [阶段 2/5] 正在批量检查 {len(works_to_process)} 部新作品的在库状态...")
                    works_not_in_library = []
                    in_library_count = 0
                    for work in works_to_process:
                        tmdb_id = str(work.get('id'))
                        # 使用预加载的 emby_media_map 进行O(1)复杂度的快速查找。
                        if tmdb_id in emby_media_map:
                            # 如果在库，立即处理：确保数据库记录存在，并关联上本次订阅源。
                            media_type = MediaType.SERIES.value if work.get('media_type') == 'tv' else MediaType.MOVIE.value
                            media_info = self._prepare_media_dict_for_upsert(work)
                            
                            # 确保这条媒体记录在我们的数据库里存在。
                            media_db.ensure_media_record_exists([media_info])
                            
                            # 只做“追加来源”这一件事。
                            request_db.add_subscription_source(tmdb_id, media_type, subscription_source)
                            
                            in_library_count += 1
                        else:
                            # 如果不在库，则加入下一个待处理列表。
                            works_not_in_library.append(work)
                    logger.info(f"  ➜ [阶段 2/5] {in_library_count} 部新作品已在库并已处理。剩余 {len(works_not_in_library)} 部待检查。")

                    # 4.2 执行廉价的本地规则过滤（不含番位）
                    logger.info(f"  ➜ [阶段 3/5] 正在对剩余作品执行本地规则过滤...")
                    works_passed_local_filters = []
                    for work in works_not_in_library:
                        tmdb_id = str(work.get('id'))
                        media_type = MediaType.SERIES.value if work.get('media_type') == 'tv' else MediaType.MOVIE.value
                        media_info = self._prepare_media_dict_for_upsert(work)
                        
                        # 调用过滤函数，但明确告诉它“不要检查番位”（check_order=False）。
                        is_kept, reason = self._filter_work_and_get_reason(work, sub, check_order=False)
                        
                        if is_kept:
                            works_passed_local_filters.append(work)
                        else:
                            # 不符合规则，直接标记为 IGNORED。
                            request_db.set_media_status_ignored(
                                tmdb_ids=tmdb_id, item_type=media_type,
                                source=subscription_source,
                                media_info_list=[media_info], ignore_reason=reason
                            )
                    logger.info(f"  ➜ [阶段 3/5] 本地规则过滤完成，{len(works_passed_local_filters)} 部作品通过。")

                    # 4.3 对极少数幸存者，执行昂贵的番位检查和最终处理
                    if works_passed_local_filters:
                        logger.info(f"  ➜ [阶段 4/5] 正在为剩余 {len(works_passed_local_filters)} 部作品获取番位信息并进行最终检查...")
                        enriched_works = self._enrich_works_with_order(works_passed_local_filters, tmdb_person_id, self.tmdb_api_key)
                        today_str = datetime.now().strftime('%Y-%m-%d')

                        for work in enriched_works:
                            tmdb_id = str(work.get('id'))
                            media_type = MediaType.SERIES.value if work.get('media_type') == 'tv' else MediaType.MOVIE.value
                            media_info = self._prepare_media_dict_for_upsert(work)
                            
                            # 这次执行包含番位检查的完整过滤（check_order=True）。
                            is_kept, reason = self._filter_work_and_get_reason(work, sub, check_order=True)

                            if is_kept:
                                # 最终通过所有检查，判断是“未上映”还是“待订阅”。
                                release_date = work.get('release_date') or work.get('first_air_date', '')
                                final_status = 'PENDING_RELEASE' if release_date and release_date > today_str else 'WANTED'
                                if final_status == 'WANTED':
                                    request_db.set_media_status_wanted(
                                        tmdb_ids=tmdb_id, item_type=media_type,
                                        source=subscription_source,
                                        media_info_list=[media_info]
                                    )
                                elif final_status == 'PENDING_RELEASE':
                                    request_db.set_media_status_pending_release(
                                        tmdb_ids=tmdb_id, item_type=media_type,
                                        source=subscription_source,
                                        media_info_list=[media_info]
                                    )
                            else:
                                # 因为番位不符被刷掉，标记为 IGNORED。
                                request_db.set_media_status_ignored(
                                    tmdb_ids=[tmdb_id], item_type=media_type,
                                    source=subscription_source,
                                    media_info_list=[media_info], ignore_reason=reason
                                )
                        logger.info(f"  ➜ [阶段 4/5] 最终检查完成。")

                    # --- 步骤 5: 清理已从 TMDb 移除的作品的追踪记录 ---
                    if removed_work_ids:
                        logger.info(f"  ➜ [阶段 5/5] 发现 {len(removed_work_ids)} 个过时的追踪记录，将为其解绑...")
                        # 批量从数据库获取这些旧记录的详情，主要是为了拿到 item_type。
                        old_items_details = media_db.get_media_details_by_tmdb_ids(list(removed_work_ids))
                        for tmdb_id_to_clean in removed_work_ids:
                            item_info = old_items_details.get(tmdb_id_to_clean)
                            if item_info:
                                # 调用函数，从作品的订阅源中移除当前演员订阅。
                                request_db.remove_subscription_source(tmdb_id_to_clean, item_info['item_type'], subscription_source)

                    # --- 步骤 6: 将本次扫描到的所有作品ID，更新回数据库作为新的“记忆” ---
                    # 这样下次运行时，就可以和这个最新的列表进行比对，实现差量更新。
                    cursor.execute(
                        "UPDATE actor_subscriptions SET last_scanned_tmdb_ids_json = %s WHERE id = %s",
                        (json.dumps(list(current_work_ids)), subscription_id)
                    )
                    
                    # 提交本次事务的所有数据库变更。
                    conn.commit()
                    logger.info(f"  ✅ 演员 '{actor_name_for_log}' 的差量更新成功完成 ---")

        except Exception as e:
            logger.error(f"为订阅ID {actor_name_for_log} 执行扫描时发生严重错误: {e}", exc_info=True)

    def _determine_library_status(self, work: Dict, emby_media_map: Dict[str, str], emby_series_seasons_map: Dict[str, Set[int]], emby_series_name_to_tmdb_id_map: Dict[str, str], today_str: str) -> Tuple[MediaStatus, Optional[str]]:
        """仅判断媒体是否在库、是否缺失、是否未发行，返回状态和Emby ID。"""
        media_id_str = str(work.get('id'))
        release_date_str = work.get('release_date') or work.get('first_air_date', '')

        if media_id_str in emby_media_map:
            return MediaStatus.IN_LIBRARY, emby_media_map.get(media_id_str)
        
        if work.get('media_type') == 'tv':
            title = work.get('name', '')
            base_name, season_num = utils.parse_series_title_and_season(title)
            
            if base_name and season_num:
                parent_tmdb_id = self._find_parent_series_tmdb_id_from_emby_cache(base_name, emby_series_name_to_tmdb_id_map)
                if parent_tmdb_id and str(parent_tmdb_id) in emby_series_seasons_map:
                    if season_num in emby_series_seasons_map[str(parent_tmdb_id)]:
                        parent_emby_id = emby_media_map.get(str(parent_tmdb_id))
                        return MediaStatus.IN_LIBRARY, parent_emby_id

        if release_date_str and release_date_str > today_str:
            return MediaStatus.PENDING_RELEASE, None

        return MediaStatus.MISSING, None
    
    def _prepare_media_dict_for_upsert(self, work: Dict) -> Dict:
        """准备一个标准的 media_info 字典。"""
        media_type = MediaType.SERIES.value if work.get('media_type') == 'tv' else MediaType.MOVIE.value
        release_date = work.get('release_date') or work.get('first_air_date') or None
        
        # 解析季号
        title = work.get('title') or work.get('name', '')
        base_name, season_num = utils.parse_series_title_and_season(title)

        return {
            "tmdb_id": str(work.get('id')),
            "item_type": media_type,
            "title": title,
            "original_title": work.get('original_title') or work.get('original_name'),
            "release_date": release_date,
            "poster_path": work.get('poster_path'),
            "overview": work.get('overview'),
            "season_number": season_num
        }

    def _find_parent_series_tmdb_id_from_emby_cache(self, base_name: str, name_to_id_map: Dict[str, str]) -> Optional[str]:
        """
        【V2 - 本地优先版】根据基础剧名，在Emby本地剧集缓存中查找父剧集TMDb ID。
        """
        normalized_base_name = utils.normalize_name_for_matching(base_name)
        
        # 直接在本地缓存的映射中查找，精准且高效
        parent_id = name_to_id_map.get(normalized_base_name)
        
        if parent_id:
            logger.debug(f"  ➜ 在Emby本地缓存中为 '{base_name}' 匹配到父剧集 (TMDb ID: {parent_id})")
        else:
            logger.debug(f"  ➜ 在Emby本地缓存中未找到名为 '{base_name}' 的父剧集。")
            
        return parent_id
    
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

    def _determine_media_status(self, work: Dict, emby_media_map: Dict[str, str], emby_series_seasons_map: Dict[str, Set[int]], emby_series_name_to_tmdb_id_map: Dict[str, str], today_str: str, old_status: Optional[str], session_subscribed_ids: Set[str]) -> Tuple[Optional[MediaStatus], Optional[str]]:
        """
        智能识别分季剧集，并检查父剧集下的【特定季】是否入库。
        """
        media_id_str = str(work.get('id'))
        release_date_str = work.get('release_date') or work.get('first_air_date', '')
        emby_tmdb_ids = set(emby_media_map.keys())

        # 1. 最高优先级：如果作品本身的TMDb ID就在Emby库中
        if media_id_str in emby_tmdb_ids:
            # ▼▼▼ 返回状态和它自己的 Emby ID ▼▼▼
            return MediaStatus.IN_LIBRARY, emby_media_map.get(media_id_str), None, None, None
        
        # 2. 对电视剧进行特殊处理
        media_type_raw = work.get('media_type', 'movie' if 'title' in work else 'tv')
        if media_type_raw == 'tv':
            title = work.get('name', '')
            base_name, season_num = utils.parse_series_title_and_season(title)
            
            if base_name and season_num:
                parent_tmdb_id = self._find_parent_series_tmdb_id_from_emby_cache(base_name, emby_series_name_to_tmdb_id_map)
                
                if parent_tmdb_id:
                    parent_tmdb_id_str = str(parent_tmdb_id)
                    if parent_tmdb_id_str in emby_series_seasons_map:
                        available_seasons = emby_series_seasons_map[parent_tmdb_id_str]
                        if season_num in available_seasons:
                            # ▼▼▼ 返回状态和父剧集的 Emby ID ▼▼▼
                            parent_emby_id = emby_media_map.get(parent_tmdb_id_str)
                            logger.info(f"  ➜ 父剧集 '{base_name}' (Emby ID: {parent_emby_id}) 已在库且包含第 {season_num} 季，标记为【已入库】。")
                            return MediaStatus.IN_LIBRARY, parent_emby_id, parent_tmdb_id_str, season_num, base_name
        # 3. 如果之前已被标记为 SUBSCRIBED
        if old_status == MediaStatus.SUBSCRIBED.value:
            return MediaStatus.SUBSCRIBED, None, None, None, None

        # 4. 如果还未上映
        if release_date_str and release_date_str > today_str:
            return MediaStatus.PENDING_RELEASE, None, None, None, None

        # 5. 其他所有情况
        return MediaStatus.MISSING, None, None, None, None

    def _prepare_media_dict(self, work: Dict, subscription_id: int, status: MediaStatus, emby_item_id: Optional[str] = None, ignore_reason: Optional[str] = None, parent_tmdb_id: Optional[str] = None, season_number: Optional[int] = None, base_name: Optional[str] = None) -> Dict:
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
            'title': base_name if base_name else (work.get('title') or work.get('name')),
            'release_date': release_date, 
            'poster_path': work.get('poster_path'),
            'status': status.value,
            'emby_item_id': emby_item_id,
            'ignore_reason': ignore_reason,
            'parent_series_tmdb_id': parent_tmdb_id,
            'parsed_season_number': season_number
        }

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
        
        details = tmdb.get_tv_details(media_id, api_key, append_to_response="credits")
        
        if details:
            logger.debug(f"  ➜ 获取电视剧 '{work.get('title') or work.get('name')}' (ID: {media_id}) 详情成功。")
            # 调试：打印完整的 details 和 credits 部分
            # logger.debug(f"    完整详情: {details}")
            # logger.debug(f"    Credits 部分: {details.get('credits')}")
            return details.get('credits')
        else:
            logger.warning(f"  ➜ 获取电视剧 '{work.get('title') or work.get('name')}' (ID: {media_id}) 详情失败。")
            return None
        
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
                logger.debug(f"  ➜ (垃圾过滤) 丢弃作品 '{work.get('title') or work.get('name')}' (ID: {work.get('id')})，缺少海报或发行日期。")
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
                discarded_ids = [w['id'] for w in group if w['id'] != best_work['id']]
                logger.debug(f"  ➜ (去重) 在同名作品组 '{title}' 中，保留热度最高的条目 (ID: {best_work['id']})，忽略了 {len(discarded_ids)} 个重复项。")

        return unique_works

    def _deduplicate_works(self, credits: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        对从TMDb获取的演员作品列表进行去重。
        TMDb有时会为同一部作品返回多个条目（例如不同地区版本），此函数旨在选出唯一的代表。
        策略：按标准化的标题分组，然后选出每组中 'popularity' 最高的一个。
        """
        if not credits:
            return []

        # 1. 合并电影和电视剧作品
        movie_works = credits.get('movie_credits', {}).get('cast', [])
        tv_works = credits.get('tv_credits', {}).get('cast', [])
        
        # 统一添加 'media_type' 标识，以便后续处理
        for work in movie_works: work['media_type'] = 'movie'
        for work in tv_works: work['media_type'] = 'tv'
        
        all_works_raw = movie_works + tv_works

        # 2. 按标准化的标题对所有作品进行分组
        work_groups = {}
        for work in all_works_raw:
            # 确保有ID，否则是无效数据
            if not work.get('id'):
                continue
            
            title = work.get('title') or work.get('name', '')
            # 使用 utils.normalize_name_for_matching 进行标准化，这是关键
            normalized_title = utils.normalize_name_for_matching(title)
            if not normalized_title:
                continue
                
            if normalized_title not in work_groups:
                work_groups[normalized_title] = []
            work_groups[normalized_title].append(work)

        # 3. 在每个分组内，选出 popularity 最高的作为唯一代表
        unique_works = []
        for title, group in work_groups.items():
            if len(group) == 1:
                # 没有重复，直接采纳
                unique_works.append(group[0])
            else:
                # 有重复，选出热度最高的
                best_work = max(group, key=lambda x: x.get('popularity', 0))
                unique_works.append(best_work)
                
                # (可选) 记录日志，方便调试，看看哪些作品被合并了
                discarded_ids = [w['id'] for w in group if w['id'] != best_work['id']]
                logger.debug(f"  ➜ 在同名作品组 '{title}' 中，保留了热度最高的条目 (ID: {best_work['id']})，忽略了其他重复条目 (IDs: {discarded_ids})。")

        return unique_works
