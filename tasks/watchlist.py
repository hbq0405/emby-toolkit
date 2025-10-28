# tasks/watchlist.py
# 智能追剧列表任务模块

import time
import logging
from typing import Optional, List, Dict, Any
import concurrent.futures

# 导入需要的底层模块和共享实例
import config_manager
import constants
import emby_handler
import extensions
import task_manager
import moviepilot_handler
from database import connection, watchlist_db, settings_db
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

# --- 追剧 ---    
def task_process_watchlist(processor, item_id: Optional[str] = None, force_full_update: bool = False):
    """
    【V9 - 启动器】
    调用处理器实例来执行追剧任务，并处理UI状态更新。
    现在支持 deep_mode 参数。
    """
    def progress_updater(progress, message):
        task_manager.update_status_from_thread(progress, message)

    try:
        processor.run_regular_processing_task_concurrent(
            progress_callback=progress_updater, 
            item_id=item_id,
            force_full_update=force_full_update
        )

    except Exception as e:
        task_name = "追剧列表更新"
        if force_full_update:
            task_name += " (深度模式)"
        if item_id:
            task_name = f"单项追剧更新 (ID: {item_id})"
        logger.error(f"执行 '{task_name}' 时发生顶层错误: {e}", exc_info=True)
        progress_updater(-1, f"启动任务时发生错误: {e}")

# ★★★ 只更新追剧列表中的一个特定项目 ★★★
def task_refresh_single_watchlist_item(processor, item_id: str):
    """
    【V11 - 新增】后台任务：只刷新追剧列表中的一个特定项目。
    这是一个职责更明确的函数，专门用于手动触发。
    """
    # 定义一个可以传递给处理器的回调函数
    def progress_updater(progress, message):
        task_manager.update_status_from_thread(progress, message)

    try:
        # 直接调用处理器的主方法，并将 item_id 传入
        # 这会执行完整的元数据刷新、状态检查和数据库更新流程
        processor.run_regular_processing_task_concurrent(progress_callback=progress_updater, item_id=item_id)

    except Exception as e:
        task_name = f"单项追剧刷新 (ID: {item_id})"
        logger.error(f"执行 '{task_name}' 时发生顶层错误: {e}", exc_info=True)
        progress_updater(-1, f"启动任务时发生错误: {e}")

# ★★★ 低频任务 - 检查已完结剧集是否复活 ★★★
def task_run_revival_check(processor):
    """
    【低频任务】后台任务入口：检查所有已完结剧集是否“复活”。
    """
    # 定义一个可以传递给处理器的回调函数
    def progress_updater(progress, message):
        task_manager.update_status_from_thread(progress, message)

    try:
        # 直接调用 processor 实例的方法，并将回调函数传入
        processor.run_revival_check_task(progress_callback=progress_updater)

    except Exception as e:
        task_name = "已完结剧集复活检查"
        logger.error(f"执行 '{task_name}' 时发生顶层错误: {e}", exc_info=True)
        progress_updater(-1, f"启动任务时发生错误: {e}")

# ✨✨✨ 一键添加所有剧集到追剧列表的任务 ✨✨✨
def task_add_all_series_to_watchlist(processor):
    """
    【V4 - 精准扫描版】
    - 严格按照用户在设置中选择的媒体库进行扫描，不再扫描全部。
    - 如果用户未选择任何媒体库，任务将直接中止并提示用户。
    - 保留了并发获取和批量写入的高效特性。
    """
    task_name = "一键扫描选定库剧集"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    
    try:
        emby_url = processor.emby_url
        emby_api_key = processor.emby_api_key
        emby_user_id = processor.emby_user_id
        
        # 1. 直接从配置中获取用户选定的媒体库列表
        library_ids_to_process = processor.config.get(constants.CONFIG_OPTION_EMBY_LIBRARIES_TO_PROCESS, [])
        
        if not library_ids_to_process:
            logger.info("  ➜ 未在配置中指定媒体库，将自动扫描所有剧集/混合媒体库...")
            all_libraries = emby_handler.get_emby_libraries(emby_url, emby_api_key, emby_user_id)
            if all_libraries:
                # 筛选出所有电视剧库和混合内容库
                libraries_to_scan = [
                    lib for lib in all_libraries 
                    if lib.get('CollectionType') in ['tvshows', 'mixed']
                ]
                library_ids_to_process = [lib['Id'] for lib in libraries_to_scan]
                
                if libraries_to_scan:
                    library_names = [lib['Name'] for lib in libraries_to_scan]
                    logger.info(f"  ➜ 将自动扫描以下媒体库: {', '.join(library_names)}")
                else:
                    logger.warning("  ➜ 自动扫描模式：未能从 Emby 找到任何电视剧或混合内容媒体库。")
            else:
                logger.error("  ➜ 自动扫描模式：未能从 Emby 获取到任何媒体库信息。")
        else:
             logger.info(f"  ➜ 将根据用户配置，扫描 {len(library_ids_to_process)} 个指定的媒体库。")

        # 如果最终还是没有要处理的库，则任务结束
        if not library_ids_to_process:
            task_manager.update_status_from_thread(100, "任务完成：没有找到可供扫描的剧集媒体库。")
            return

        task_manager.update_status_from_thread(10, f"正在从 {len(library_ids_to_process)} 个选定的媒体库并发获取剧集...")
        all_series = []
        
        def fetch_series_from_library(library_id: str) -> List[Dict[str, Any]]:
            """线程工作函数：从单个媒体库获取剧集"""
            try:
                # ★★★ 关键点：这里的 library_ids 参数现在接收的是精确的用户选择 ★★★
                items = emby_handler.get_emby_library_items(
                    base_url=emby_url, api_key=emby_api_key, user_id=emby_user_id,
                    library_ids=[library_id], media_type_filter="Series"
                )
                return items if items is not None else []
            except Exception as e:
                logger.error(f"从媒体库 {library_id} 获取数据时出错: {e}")
                return []

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_library = {executor.submit(fetch_series_from_library, lib_id): lib_id for lib_id in library_ids_to_process}
            for future in concurrent.futures.as_completed(future_to_library):
                try:
                    result = future.result()
                    all_series.extend(result)
                except Exception as exc:
                    library_id = future_to_library[future]
                    logger.error(f"媒体库 {library_id} 的任务在执行中产生异常: {exc}")

        if not all_series:
            task_manager.update_status_from_thread(100, "任务完成：在所选媒体库中未发现任何剧集。")
            return

        total = len(all_series)
        task_manager.update_status_from_thread(30, f"共找到 {total} 部剧集，正在筛选...")
        
        series_to_insert = []
        for series in all_series:
            tmdb_id = series.get("ProviderIds", {}).get("Tmdb")
            item_name = series.get("Name")
            item_id = series.get("Id")
            if tmdb_id and item_name and item_id:
                series_to_insert.append(
                    (item_id, tmdb_id, item_name, "Series", 'Watching')
                )

        if not series_to_insert:
            task_manager.update_status_from_thread(100, "任务完成：找到的剧集均缺少TMDb ID，无法添加。")
            return

        added_count = 0
        total_to_add = len(series_to_insert)
        task_manager.update_status_from_thread(60, f"筛选出 {total_to_add} 部有效剧集，准备批量写入数据库...")
        
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            try:
                sql_insert = """
                    INSERT INTO watchlist (item_id, tmdb_id, item_name, item_type, status)
                    VALUES %s
                    ON CONFLICT (item_id) DO NOTHING
                    RETURNING item_id
                """
                inserted_ids = execute_values(
                    cursor, sql_insert, series_to_insert, 
                    template=None, page_size=1000, fetch=True
                )
                added_count = len(inserted_ids)
                conn.commit()
            except Exception as e_db:
                conn.rollback()
                raise RuntimeError(f"数据库批量写入时发生错误: {e_db}")

        scan_complete_message = f"扫描完成！共发现 {total} 部剧集，新增 {added_count} 部。"
        logger.info(scan_complete_message)
        
        if added_count > 0:
            logger.info("--- 任务链：即将自动触发【检查所有在追剧集】任务 ---")
            task_manager.update_status_from_thread(99, "扫描完成，正在启动追剧检查...")
            time.sleep(2)
            try:
                watchlist_proc = extensions.watchlist_processor_instance
                if watchlist_proc:
                    watchlist_proc.run_regular_processing_task_concurrent(
                        progress_callback=task_manager.update_status_from_thread,
                        item_id=None
                    )
                    final_message = "自动化流程完成：扫描与追剧检查均已结束。"
                    task_manager.update_status_from_thread(100, final_message)
                else:
                    raise RuntimeError("WatchlistProcessor 未初始化，无法执行链式任务。")
            except Exception as e_chain:
                 logger.error(f"执行链式任务【检查所有在追剧集】时失败: {e_chain}", exc_info=True)
                 task_manager.update_status_from_thread(-1, f"链式任务失败: {e_chain}")
        else:
            final_message = f"任务完成！共扫描到 {total} 部剧集，没有发现可新增的剧集。"
            logger.info(final_message)
            task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

# ★★★ 新增后台任务：批量订阅缺集的季 ★★★
def task_batch_subscribe_gaps(processor, item_ids: List[str]):
    task_name = "批量订阅缺集的季"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    
    total_items = len(item_ids)
    processed_count = 0
    subscribed_seasons_count = 0
    quota_exhausted = False

    config = config_manager.APP_CONFIG
    use_best_version = config.get(constants.CONFIG_OPTION_RESUBSCRIBE_USE_BEST_VERSION, False)
    best_version_param = 1 if use_best_version else None
    log_mode = "洗版模式" if use_best_version else "普通模式"
    logger.info(f"  ➜ 本次任务将以 [{log_mode}] 进行订阅。")

    for i, item_id in enumerate(item_ids):
        if processor.is_stop_requested() or quota_exhausted:
            break
        
        series_info = watchlist_db.get_watchlist_item_details(item_id)
        if not series_info or not series_info.get('missing_info_json'):
            continue
        
        item_name = series_info.get('item_name', '未知剧集')
        task_manager.update_status_from_thread(
            int((i / total_items) * 100),
            f"({i+1}/{total_items}) 正在处理: {item_name}"
        )

        seasons_to_subscribe = series_info['missing_info_json'].get('seasons_with_gaps', [])
        if not seasons_to_subscribe:
            continue

        seasons_successfully_subscribed = []
        for season_num in seasons_to_subscribe:
            if processor.is_stop_requested(): break

            current_quota = settings_db.get_subscription_quota()
            if current_quota <= 0:
                logger.warning("  ➜ 每日订阅配额已用尽，任务提前结束。")
                quota_exhausted = True
                break

            success = moviepilot_handler.subscribe_series_to_moviepilot(
                series_info=series_info,
                season_number=season_num,
                config=config,
                best_version=best_version_param
            )
            
            if success:
                settings_db.decrement_subscription_quota()
                subscribed_seasons_count += 1
                seasons_successfully_subscribed.append(season_num)
            
            time.sleep(1) # 避免请求过快

        if seasons_successfully_subscribed:
            watchlist_db.remove_seasons_from_gaps_list(item_id, seasons_successfully_subscribed)

    final_message = f"任务完成！共成功提交 {subscribed_seasons_count} 个季的订阅。"
    if quota_exhausted:
        final_message += " (因配额用尽提前中止)"
    task_manager.update_status_from_thread(100, final_message)