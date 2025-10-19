# tasks/watchlist.py
# 智能追剧列表任务模块

import time
import logging
from typing import Optional, List, Dict, Any
import concurrent.futures

# 导入需要的底层模块和共享实例
import config_manager
import emby_handler
import extensions
import task_manager
from database import connection
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
    【V3 - 并发获取与批量写入 PG 版】
    - 使用5个并发线程，分别从不同的媒体库获取剧集，提升 Emby 数据拉取速度。
    - 将数据库操作改为单次批量写入（execute_values），大幅提升数据库性能。
    - 使用 RETURNING 子句精确统计实际新增的剧集数量。
    """
    task_name = "一键扫描全库剧集 (并发版)"
    logger.trace(f"--- 开始执行 '{task_name}' 任务 ---")
    
    try:
        emby_url = processor.emby_url
        emby_api_key = processor.emby_api_key
        emby_user_id = processor.emby_user_id
        
        library_ids_to_process = config_manager.APP_CONFIG.get('emby_libraries_to_process', [])
        
        if not library_ids_to_process:
            logger.info("未在配置中指定媒体库，将自动扫描所有媒体库...")
            all_libraries = emby_handler.get_emby_libraries(emby_url, emby_api_key, emby_user_id)
            if all_libraries:
                library_ids_to_process = [
                    lib['Id'] for lib in all_libraries 
                    if lib.get('CollectionType') in ['tvshows', 'mixed']
                ]
                logger.info(f"将扫描以下剧集库: {[lib['Name'] for lib in all_libraries if lib.get('CollectionType') in ['tvshows', 'mixed']]}")
            else:
                logger.warning("未能从 Emby 获取到任何媒体库。")
        
        if not library_ids_to_process:
            task_manager.update_status_from_thread(100, "任务完成：没有找到可供扫描的剧集媒体库。")
            return

        # --- 并发获取 Emby 剧集 ---
        task_manager.update_status_from_thread(10, f"正在从 {len(library_ids_to_process)} 个媒体库并发获取剧集...")
        all_series = []
        
        def fetch_series_from_library(library_id: str) -> List[Dict[str, Any]]:
            """线程工作函数：从单个媒体库获取剧集"""
            try:
                items = emby_handler.get_emby_library_items(
                    base_url=emby_url, api_key=emby_api_key, user_id=emby_user_id,
                    library_ids=[library_id], media_type_filter="Series"
                )
                return items if items is not None else []
            except Exception as e:
                logger.error(f"从媒体库 {library_id} 获取数据时出错: {e}")
                return []

        # 使用线程池并发执行
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # 提交所有任务
            future_to_library = {executor.submit(fetch_series_from_library, lib_id): lib_id for lib_id in library_ids_to_process}
            for future in concurrent.futures.as_completed(future_to_library):
                try:
                    result = future.result()
                    all_series.extend(result)
                except Exception as exc:
                    library_id = future_to_library[future]
                    logger.error(f"媒体库 {library_id} 的任务在执行中产生异常: {exc}")

        if not all_series:
            raise RuntimeError("从 Emby 获取剧集列表失败，请检查网络和配置。")

        total = len(all_series)
        task_manager.update_status_from_thread(30, f"共找到 {total} 部剧集，正在筛选...")
        
        series_to_insert = []
        for series in all_series:
            tmdb_id = series.get("ProviderIds", {}).get("Tmdb")
            item_name = series.get("Name")
            item_id = series.get("Id")
            if tmdb_id and item_name and item_id:
                # 准备元组用于批量插入
                series_to_insert.append(
                    (item_id, tmdb_id, item_name, "Series", 'Watching')
                )

        if not series_to_insert:
            task_manager.update_status_from_thread(100, "任务完成：找到的剧集均缺少TMDb ID，无法添加。")
            return

        added_count = 0
        total_to_add = len(series_to_insert)
        task_manager.update_status_from_thread(60, f"筛选出 {total_to_add} 部有效剧集，准备批量写入数据库...")
        
        # --- 高效批量写入数据库 ---
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            try:
                # ★★★ 核心修复：使用 execute_values 进行高效批量插入 ★★★
                sql_insert = """
                    INSERT INTO watchlist (item_id, tmdb_id, item_name, item_type, status)
                    VALUES %s
                    ON CONFLICT (item_id) DO NOTHING
                    RETURNING item_id
                """
                # execute_values 会自动将数据列表转换成 (v1,v2), (v1,v2) 的形式
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
        
        # --- 后续任务链逻辑 (保持不变) ---
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