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
from tasks.helpers import parse_series_title_and_season
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

        # --- 步骤 1: 获取所有需要处理的订阅 (逻辑不变) ---
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
            (emby_media_map, 
             emby_series_seasons_map, 
             emby_series_name_to_tmdb_id_map) = actor_db.get_all_in_library_media_for_actor_sync()
            logger.info(f"  ➜ 从数据库成功加载 {len(emby_media_map)} 个媒体映射，{len(emby_series_seasons_map)} 个剧集季结构。")
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
                    emby_media_map, 
                    emby_series_seasons_map, 
                    emby_series_name_to_tmdb_id_map
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
                # 这里的 message 只是一个通用进度，因为我们不知道具体是哪个演员刚完成
                message = f"({processed_count}/{total_subs}) 已完成一个演员的扫描..."
                _update_status(progress, message)
                
                # 检查任务是否成功执行（可选，用于捕获线程内异常）
                try:
                    future.result() 
                except Exception as exc:
                    sub_id = future_to_sub_id[future]
                    logger.error(f"  ➜ 订阅ID {sub_id} 的扫描任务在线程内发生异常: {exc}", exc_info=True)
                
        # --- 步骤 4: 任务结束 (逻辑不变) ---
        if not self.is_stop_requested():
            logger.info("--- 演员订阅任务 (并发调度模式) 执行完毕 ---")
            _update_status(100, "所有订阅扫描完成。")


    def _process_single_work(self, work: Dict, sub_config: Dict, subscription_source: Dict) -> Optional[Dict]:
        """
        处理单个作品的完整流程，用于并发执行。
        """
        try:
            tmdb_id = str(work.get('id'))
            media_type = MediaType.SERIES.value if work.get('media_type') == 'tv' else MediaType.MOVIE.value
            media_info = self._prepare_media_dict_for_upsert(work)

            # 1. 执行廉价的本地规则过滤（不含番位）
            is_kept, reason = self._filter_work_and_get_reason(work, sub_config, check_order=False)
            if not is_kept:
                media_info['reason'] = reason
                media_info['overview'] = None 
                return {"action": "ignore", "tmdb_id": tmdb_id, "item_type": media_type, "media_info": media_info}

            # 2. 获取番位信息
            enriched_work = self._enrich_works_with_order([work], sub_config['tmdb_person_id'], self.tmdb_api_key)[0]
            
            # 3. 执行包含番位的最终过滤
            is_kept_final, reason_final = self._filter_work_and_get_reason(enriched_work, sub_config, check_order=True)
            if not is_kept_final:
                media_info['reason'] = reason_final
                media_info['overview'] = None
                return {"action": "ignore", "tmdb_id": tmdb_id, "item_type": media_type, "media_info": media_info}

            # 4. 决定最终状态
            today_str = datetime.now().strftime('%Y-%m-%d')
            release_date = enriched_work.get('release_date') or enriched_work.get('first_air_date', '')
            final_status = 'PENDING_RELEASE' if release_date and release_date > today_str else 'WANTED'
            
            return {"action": final_status.lower(), "tmdb_id": tmdb_id, "item_type": media_type, "media_info": media_info}

        except Exception as e:
            item_name = work.get('title') or work.get('name', '未知作品')
            logger.error(f"  ➜ (线程内) 处理作品 '{item_name}' 时发生错误: {e}", exc_info=True)
            return None

    def run_full_scan_for_actor(self, subscription_id: int, emby_media_map: Dict[str, str], emby_series_seasons_map: Dict[str, Set[int]], emby_series_name_to_tmdb_id_map: Dict[str, str]):
        """
        - 采用并发模型处理所有新增作品，显著提升扫描速度。
        - 将数据库操作聚合到任务末尾进行批量处理，大幅减少数据库I/O。
        """
        actor_name_for_log = f"订阅ID {subscription_id}"
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # --- 步骤 1 & 2: 获取订阅规则和TMDb全量作品 (逻辑不变) ---
                    cursor.execute("SELECT *, last_scanned_tmdb_ids_json FROM actor_subscriptions WHERE id = %s FOR UPDATE", (subscription_id,))
                    sub = cursor.fetchone()
                    if not sub: return
                    
                    actor_name_for_log = sub.get('actor_name', actor_name_for_log)
                    logger.info(f"--- 开始为演员 '{actor_name_for_log}' 执行作品扫描 ---")
                    
                    last_scanned_ids = set(sub.get('last_scanned_tmdb_ids_json') or [])
                    subscription_source = {"type": "actor_subscription", "id": subscription_id, "name": sub['actor_name'], "person_id": sub['tmdb_person_id']}

                    logger.info(f"  ➜ [阶段 1/4] 正在从 TMDb 获取演员 '{sub['actor_name']}' 的所有作品...")
                    all_works = self._get_and_clean_actor_works(sub['tmdb_person_id'], self.tmdb_api_key)
                    if self.is_stop_requested(): return
                    if not all_works:
                        cursor.execute("UPDATE actor_subscriptions SET last_scanned_tmdb_ids_json = '[]' WHERE id = %s", (subscription_id,))
                        return

                    # --- 步骤 3: 计算差量 (逻辑不变) ---
                    current_work_ids = {str(w.get('id')) for w in all_works if w.get('id')}
                    new_work_ids = current_work_ids - last_scanned_ids
                    removed_work_ids = last_scanned_ids - current_work_ids
                    logger.info(f"  ➜ [阶段 1/4] 差量计算完成：发现 {len(new_work_ids)} 部新作品，{len(removed_work_ids)} 部作品已从TMDb移除。")
                    works_to_process = [w for w in all_works if str(w.get('id')) in new_work_ids]
                    if not works_to_process and not removed_work_ids:
                        logger.info(f"  ➜ 演员 '{sub['actor_name']}' 的作品列表无变化，跳过。")
                        return

                    # --- 步骤 4: ★★★ 核心修改 - 并发处理所有新增作品 ★★★ ---
                    
                    # 4.1 首先处理在库的作品 (这个循环很快，无需并发)
                    logger.info(f"  ➜ [阶段 2/4] 正在批量检查 {len(works_to_process)} 部新作品的在库状态...")
                    works_not_in_library = []
                    in_library_media_to_update = []
                    # ★ 用于存放因条目不规范而被忽略的作品
                    ignored_due_to_bad_entry = []

                    for work in works_to_process:
                        tmdb_id = str(work.get('id'))
                        media_type = MediaType.SERIES.value if work.get('media_type') == 'tv' else MediaType.MOVIE.value
                        
                        # ★★★ V4 核心逻辑：优先识别并忽略所有分季条目 ★★★
                        is_standalone_season_entry = False
                        if media_type == MediaType.SERIES.value:
                            title = work.get('name', '')
                            base_name, season_num = parse_series_title_and_season(title, self.tmdb_api_key)
                            # 只要能解析出季号，就认定为不规范条目
                            if base_name and season_num:
                                is_standalone_season_entry = True

                        # 根据检查结果进行分流
                        if is_standalone_season_entry:
                            logger.info(f"  ➜ 作品 '{work.get('name', '')}' 被识别为不规范的分季条目，将忽略。")
                            media_info = self._prepare_media_dict_for_upsert(work)
                            # 忽略原因
                            media_info['reason'] = "错误的词条"
                            media_info['overview'] = None
                            ignored_due_to_bad_entry.append({
                                'tmdb_id': tmdb_id,
                                'item_type': media_type,
                                'media_info': media_info
                            })
                        elif tmdb_id in emby_media_map:
                            # 原有逻辑：作品本身就在库
                            media_info = self._prepare_media_dict_for_upsert(work)
                            in_library_media_to_update.append({'tmdb_id': tmdb_id, 'item_type': media_type, 'media_info': media_info})
                        else:
                            # 最终漏网之鱼：规范的、且不在库的作品
                            works_not_in_library.append(work)
                    
                    # ★ 批量处理因条目不规范而被忽略的作品
                    if ignored_due_to_bad_entry:
                        logger.info(f"  ➜ [阶段 2/4] {len(ignored_due_to_bad_entry)} 部新作品因条目不规范而被忽略。")

                    if in_library_media_to_update:
                        logger.info(f"  ➜ [阶段 2/4] {len(in_library_media_to_update)} 部新作品已在库，将批量更新其订阅源...")
                        media_db.ensure_media_record_exists([item['media_info'] for item in in_library_media_to_update])
                        for item in in_library_media_to_update:
                            request_db.add_subscription_source(item['tmdb_id'], item['item_type'], subscription_source)
                    
                    # 4.2 并发处理所有不在库的作品 (逻辑不变)
                    logger.info(f"  ➜ [阶段 3/4] 正在并发处理 {len(works_not_in_library)} 部不在库的新作品...")
                    results_to_commit = []
                    if works_not_in_library:
                        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                            future_to_work = {executor.submit(self._process_single_work, work, sub, subscription_source): work for work in works_not_in_library}
                            for future in concurrent.futures.as_completed(future_to_work):
                                if self.is_stop_requested(): break
                                result = future.result()
                                if result:
                                    results_to_commit.append(result)
                    
                    # 4.3 聚合结果并批量更新数据库 (逻辑不变)
                    if results_to_commit:
                        logger.info(f"  ➜ [阶段 3/4] 并发处理完成，聚合 {len(results_to_commit)} 条结果准备批量写入数据库...")
                        
                        actions_by_type = {"wanted": {}, "pending_release": {}, "ignore": {}}
                        for res in results_to_commit:
                            action = res.get("action")
                            item_type = res.get("item_type")
                            if action in actions_by_type and item_type:
                                if item_type not in actions_by_type[action]:
                                    actions_by_type[action][item_type] = []
                                actions_by_type[action][item_type].append(res)

                        for item_type, items in actions_by_type["wanted"].items():
                            if items:
                                request_db.set_media_status_wanted(
                                    tmdb_ids=[r['tmdb_id'] for r in items],
                                    item_type=item_type,
                                    source=subscription_source,
                                    media_info_list=[r['media_info'] for r in items]
                                )
                        
                        for item_type, items in actions_by_type["pending_release"].items():
                            if items:
                                request_db.set_media_status_pending_release(
                                    tmdb_ids=[r['tmdb_id'] for r in items],
                                    item_type=item_type,
                                    source=subscription_source,
                                    media_info_list=[r['media_info'] for r in items]
                                )

                        for item_type, items in actions_by_type["ignore"].items():
                            if items:
                                logger.info(f"  ➜ [阶段 3/4] {len(items)} 部作品因不满足订阅规则被忽略 (类型: {item_type})。")

                    # --- 步骤 5 & 6: 清理和更新缓存 (逻辑不变) ---
                    if removed_work_ids:
                        logger.info(f"  ➜ [阶段 4/4] 发现 {len(removed_work_ids)} 个过时的追踪记录，将为其解绑...")
                        old_items_details = media_db.get_media_details_by_tmdb_ids(list(removed_work_ids))
                        for tmdb_id_to_clean in removed_work_ids:
                            item_info = old_items_details.get(tmdb_id_to_clean)
                            if item_info:
                                request_db.remove_subscription_source(tmdb_id_to_clean, item_info['item_type'], subscription_source)

                    cursor.execute(
                        "UPDATE actor_subscriptions SET last_scanned_tmdb_ids_json = %s WHERE id = %s",
                        (json.dumps(list(current_work_ids)), subscription_id)
                    )
                    
                    conn.commit()
                    logger.info(f"  ✅ 演员 '{actor_name_for_log}' 的差量更新成功完成 ---")

        except Exception as e:
            logger.error(f"为订阅ID {actor_name_for_log} 执行扫描时发生严重错误: {e}", exc_info=True)

    def _determine_library_status(self, work: Dict, emby_media_map: Dict[str, str], emby_series_seasons_map: Dict[str, Set[int]], emby_series_name_to_tmdb_id_map: Dict[str, str], today_str: str) -> Tuple[MediaStatus, Optional[str]]:
        """仅判断媒体是否在库、是否缺失、是否未发行，返回状态和Emby ID。"""
        media_id_str = str(work.get('id'))
        release_date_str = work.get('release_date') or work.get('first_air_date', '')

        # 1. 最高优先级：直接检查作品本身的 TMDb ID 是否在库。
        if media_id_str in emby_media_map:
            return MediaStatus.IN_LIBRARY, emby_media_map.get(media_id_str)
        
        # 2. 检查是否还未上映
        if release_date_str and release_date_str > today_str:
            return MediaStatus.PENDING_RELEASE, None

        # 3. 其他所有情况都视为缺失
        return MediaStatus.MISSING, None
    
    def _prepare_media_dict_for_upsert(self, work: Dict) -> Dict:
        """
        准备一个标准的 media_info 字典。
        """
        media_type = MediaType.SERIES.value if work.get('media_type') == 'tv' else MediaType.MOVIE.value
        release_date = work.get('release_date') or work.get('first_air_date') or None
        
        # 这里的 title 就是原始标题，因为任何需要解析的条目都已被上游逻辑过滤掉了
        title = work.get('title') or work.get('name', '')

        return {
            "tmdb_id": str(work.get('id')),
            "item_type": media_type,
            "title": title,
            "original_title": work.get('original_title') or work.get('original_name'),
            "release_date": release_date,
            "poster_path": work.get('poster_path'),
            "overview": work.get('overview'),
            "season_number": None
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
