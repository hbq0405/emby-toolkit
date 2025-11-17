# tasks/watchlist.py
# 智能追剧列表任务模块
import json
import time
import logging
from typing import Optional, List, Dict, Any
import concurrent.futures
from datetime import datetime, timedelta, timezone

# 导入需要的底层模块和共享实例
import config_manager
import constants
import handler.emby as emby
import handler.tmdb as tmdb
import extensions
import task_manager
from database import connection, watchlist_db, request_db, media_db
from psycopg2.extras import execute_values
from watchlist_processor import STATUS_WATCHING, STATUS_PAUSED, STATUS_COMPLETED

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
            all_libraries = emby.get_emby_libraries(emby_url, emby_api_key, emby_user_id)
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
                items = emby.get_emby_library_items(
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
        
        series_to_upsert = []
        for series in all_series:
            tmdb_id = series.get("ProviderIds", {}).get("Tmdb")
            item_name = series.get("Name")
            item_id = series.get("Id")  
            if tmdb_id and item_name and item_id:
                series_to_upsert.append(
                    (tmdb_id, "Series", item_name, 'Watching', json.dumps([item_id]))
                )

        if not series_to_upsert:
            task_manager.update_status_from_thread(100, "任务完成：找到的剧集均缺少TMDb ID，无法添加。")
            return

        total_to_add = len(series_to_upsert)
        task_manager.update_status_from_thread(60, f"筛选出 {total_to_add} 部有效剧集，准备批量写入数据库...")
        
        with connection.get_db_connection() as conn:
            cursor = conn.cursor()
            try:
                sql_upsert = """
                    INSERT INTO media_metadata (tmdb_id, item_type, title, watching_status, emby_item_ids_json)
                    VALUES %s
                    ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                        watching_status = EXCLUDED.watching_status,
                        paused_until = NULL,
                        force_ended = FALSE,
                        emby_item_ids_json = (
                            SELECT jsonb_agg(DISTINCT elem)
                            FROM (
                                SELECT jsonb_array_elements_text(media_metadata.emby_item_ids_json) AS elem
                                UNION ALL
                                SELECT jsonb_array_elements_text(EXCLUDED.emby_item_ids_json) AS elem
                            ) AS combined
                        );
                """
                execute_values(
                    cursor, sql_upsert, series_to_upsert, 
                    template=None, page_size=1000
                )
                conn.commit()
            except Exception as e_db:
                conn.rollback()
                raise RuntimeError(f"数据库批量写入时发生错误: {e_db}")

        scan_complete_message = f"扫描完成！共发现 {total} 部剧集，已将 {total_to_add} 部有效剧集全部标记为“追剧中”。"
        logger.info(scan_complete_message)
        
        if total_to_add > 0:
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

# ★★★ 全新、独立的媒体库缺集扫描任务 ★★★
def task_scan_library_gaps(processor):
    """
    【V5 - 最终修复版】扫描指定的媒体库，通过即时同步全量元数据来确保数据完整性，
    然后基于本地数据库分析所有剧集的“中间缺失”，并为真正缺失的季提交订阅请求。
    """
    task_name = "媒体库缺集扫描"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    
    def progress_updater(progress, message):
        task_manager.update_status_from_thread(progress, message)

    try:
        # 步骤 1: 获取媒体库中的所有剧集 (此部分已正确)
        progress_updater(5, "正在从 Emby 获取媒体库中的剧集列表...")
        all_series_in_libs = processor._get_series_to_process(
            where_clause="", 
            include_all_series=True
        )
        if not all_series_in_libs:
            progress_updater(100, "任务完成：在指定的媒体库中未找到任何剧集。")
            return
        
        all_series_tmdb_ids = [s['tmdb_id'] for s in all_series_in_libs if s.get('tmdb_id')]
        
        # 增加一个健壮性检查：如果筛选后没有任何有效的 TMDb ID，也直接退出
        if not all_series_tmdb_ids:
            progress_updater(100, "任务完成：媒体库中的剧集均缺少有效的 TMDb ID。")
            return

        total_series = len(all_series_in_libs)
        logger.info(f"  ➜ 发现 {total_series} 部剧集，即将开始同步子项目元数据...")
        progress_updater(10, f"发现 {total_series} 部剧集，准备同步元数据...")

        # 步骤 2: 高性能元数据同步 
        # a. 并发从 TMDb 拉取所有剧集的完整子项信息
        progress_updater(15, f"正在从TMDb并发获取 {total_series} 部剧集的完整数据...")
        tmdb_full_data = tmdb.batch_get_full_series_details_tmdb(all_series_tmdb_ids, processor.tmdb_api_key)

        # b. 从本地数据库批量获取所有已存在的子项ID
        progress_updater(40, "正在从本地数据库查询现有记录...")
        existing_children_ids = media_db.get_all_children_for_series_batch(all_series_tmdb_ids)

        # c. 计算差异：找出 TMDb 上有但本地数据库没有的记录
        progress_updater(45, "正在对比数据，计算缺失的元数据记录...")
        missing_records_to_insert = []
        for series_id, children in tmdb_full_data.items():
            for child_item in children:
                child_tmdb_id = str(child_item.get("id"))
                if child_tmdb_id not in existing_children_ids:
                    # 这是一个新记录，需要添加到待插入列表
                    child_item['parent_series_tmdb_id'] = series_id # 补充父ID
                    missing_records_to_insert.append(child_item)
        
        # d. 将所有缺失的记录一次性批量写入数据库
        if missing_records_to_insert:
            progress_updater(50, f"发现 {len(missing_records_to_insert)} 条缺失记录，正在批量写入数据库...")
            media_db.batch_insert_media_metadata(missing_records_to_insert)
        else:
            progress_updater(50, "元数据完整性检查完成，无需补充新记录。")

        if processor.is_stop_requested():
            progress_updater(100, "任务已中止。")
            return

        # 步骤 3: 执行数据库分析 (现在数据库中保证有完整且正确的数据了)
        logger.info("  ➜ 所有剧集元数据同步完成，开始进行缺集分析...")
        progress_updater(50, "元数据同步完成，开始数据库分析...")
        
        all_series_tmdb_ids = [s['tmdb_id'] for s in all_series_in_libs if s.get('tmdb_id')]
        seasons_with_gaps = watchlist_db.find_season_tmdb_ids_with_gaps(all_series_tmdb_ids)

        # --------------------------------------------------------------------------
        # 步骤 4: 整合缺集数据并批量更新到数据库，供前端显示
        # --------------------------------------------------------------------------
        logger.info("  ➜ 正在整合缺集分析结果，准备更新前端显示数据...")
        progress_updater(55, "正在整合分析结果...")

        # a. 将查询结果从列表转换为更易于处理的字典
        gaps_by_series = {}
        for gap_info in seasons_with_gaps:
            series_id = gap_info['parent_series_tmdb_id']
            season_num = gap_info['season_number']
            gaps_by_series.setdefault(series_id, set()).add(season_num)

        # b. 构建最终的更新数据，包含所有被扫描的剧集
        final_gaps_data_to_update = {}
        for series_id in all_series_tmdb_ids:
            # 如果剧集在 gaps_by_series 中，说明它有缺集，我们用季号列表更新
            # 否则，说明它没有缺集，我们用一个空列表来清空旧的标记
            found_gaps = sorted(list(gaps_by_series.get(series_id, set())))
            final_gaps_data_to_update[series_id] = found_gaps
        
        # c. 调用新的数据库函数进行一次性批量更新
        if final_gaps_data_to_update:
            try:
                watchlist_db.batch_update_gaps_info(final_gaps_data_to_update)
                logger.info("  ➜ 成功将最新的缺集信息同步到所有被扫描的剧集中。")
            except Exception as e_update_gaps:
                logger.error(f"  ➜ 更新缺集显示信息时发生错误: {e_update_gaps}", exc_info=True)
        
        progress_updater(60, "缺集信息已更新，准备提交订阅...")
        
        if not seasons_with_gaps:
            progress_updater(100, "分析完成：媒体库中的所有剧集均无“中间缺失”的季。")
            return

        # ★★★ 5. 一次性批量提交所有订阅请求 ★★★
        total_seasons_to_sub = len(seasons_with_gaps)
        logger.info(f"  ➜ 分析完成！共发现 {total_seasons_to_sub} 个存在中间缺失的季需要订阅。")
        progress_updater(60, f"发现 {total_seasons_to_sub} 个缺集的季，准备提交订阅...")
        
        series_info_map = {s['tmdb_id']: s for s in all_series_in_libs if s.get('tmdb_id')}
        media_info_batch_for_sub = []
        for i, gap_info in enumerate(seasons_with_gaps):
            series_tmdb_id = gap_info['parent_series_tmdb_id']
            season_num = gap_info['season_number']
            season_tmdb_id = gap_info['tmdb_id']
            series_info = series_info_map.get(series_tmdb_id)
            if not series_info: continue
            series_title = series_info.get('item_name', '未知剧集')
            progress = 50 + int(((i + 1) / total_seasons_to_sub) * 50)
            progress_updater(progress, f"({i+1}/{total_seasons_to_sub}) 准备订阅: {series_title} 第 {season_num} 季")
            media_info_batch_for_sub.append({
                'tmdb_id': season_tmdb_id,
                'title': f"{series_title} - 第 {season_num} 季",
                'parent_series_tmdb_id': series_tmdb_id,
                'season_number': season_num
            })

        if media_info_batch_for_sub:
            logger.info(f"  ➜ 准备批量提交 {len(media_info_batch_for_sub)} 个季的订阅请求...")
            request_db.set_media_status_wanted(
                tmdb_ids=[info['tmdb_id'] for info in media_info_batch_for_sub],
                item_type='Season',
                source={"type": "gap_scan", "reason": "library_integrity_check"},
                media_info_list=media_info_batch_for_sub
            )

        final_message = f"任务完成！共为 {len(media_info_batch_for_sub)} 个缺集的季提交了订阅请求。"
        progress_updater(60, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 时发生严重错误: {e}", exc_info=True)
        progress_updater(-1, f"任务失败: {e}")