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
    【V5 - 高效重构版】
    - 核心数据源改为本地数据库，极大提升执行速度和稳定性。
    - 仅在需要按媒体库筛选时，才对 Emby 进行一次性的 ID 批量查询。
    - 保留了批量写入和任务链的高效特性。
    """
    task_name = "一键扫描库内剧集"
    logger.info(f"--- 开始执行 '{task_name}' 任务 (高效模式) ---")
    
    try:
        # ----------------------------------------------------------------------
        # 步骤 1: 确定处理范围 (媒体库过滤)
        # ----------------------------------------------------------------------
        task_manager.update_status_from_thread(5, "正在确定媒体库扫描范围...")
        library_ids_to_process = processor.config.get(constants.CONFIG_OPTION_EMBY_LIBRARIES_TO_PROCESS, [])
        valid_emby_ids = None # None 表示不过滤，处理所有库的剧集

        if library_ids_to_process:
            logger.info(f"  ➜ 已启用媒体库过滤器，将从 {len(library_ids_to_process)} 个指定库中进行筛选...")
            valid_emby_ids = set()
            for lib_id in library_ids_to_process:
                series_ids_in_lib = emby.get_library_series_ids(
                    library_id=lib_id, emby_server_url=processor.emby_url,
                    emby_api_key=processor.emby_api_key, user_id=processor.emby_user_id
                )
                valid_emby_ids.update(series_ids_in_lib)
            logger.info(f"  ➜ 成功从 Emby 获取到 {len(valid_emby_ids)} 个有效的剧集 ID 用于过滤。")
        else:
            logger.info("  ➜ 未指定媒体库，将扫描数据库中所有的剧集。")

        # ----------------------------------------------------------------------
        # 步骤 2: 从本地数据库获取所有剧集
        # ----------------------------------------------------------------------
        task_manager.update_status_from_thread(20, "正在从本地数据库查询所有剧集...")
        all_series_from_db = watchlist_db.get_all_series_for_watchlist_scan()
        if not all_series_from_db:
            task_manager.update_status_from_thread(100, "任务完成：本地数据库中没有任何剧集记录。")
            return

        # ----------------------------------------------------------------------
        # 步骤 3: 根据媒体库ID进行筛选
        # ----------------------------------------------------------------------
        task_manager.update_status_from_thread(40, f"正在从 {len(all_series_from_db)} 条记录中筛选...")
        series_to_process = []
        if valid_emby_ids is not None:
            for series in all_series_from_db:
                emby_ids_in_db = series.get('emby_item_ids_json', [])
                if isinstance(emby_ids_in_db, list) and any(eid in valid_emby_ids for eid in emby_ids_in_db):
                    series_to_process.append(series)
        else:
            # 如果不过滤，则处理所有从数据库查出来的剧集
            series_to_process = all_series_from_db
        
        total_to_add = len(series_to_process)
        if not series_to_process:
            task_manager.update_status_from_thread(100, "任务完成：筛选后没有需要标记为“追剧中”的剧集。")
            return
            
        logger.info(f"  ➜ 筛选完成，共 {total_to_add} 部剧集将被标记为“追剧中”。")

        # ----------------------------------------------------------------------
        # 步骤 4: 准备数据并批量写入数据库
        # ----------------------------------------------------------------------
        task_manager.update_status_from_thread(60, f"准备将 {total_to_add} 部剧集批量更新为“追剧中”...")
        
        # 我们只需要 tmdb_id 列表来进行更新
        tmdb_ids_to_update = [s['tmdb_id'] for s in series_to_process]

        try:
            watchlist_db.batch_set_series_watching(tmdb_ids_to_update)
        except Exception as e_db:
            raise RuntimeError(f"数据库批量更新状态时发生错误: {e_db}")

        scan_complete_message = f"扫描完成！已将 {total_to_add} 部库内剧集全部标记为“追剧中”。"
        logger.info(scan_complete_message)
        
        # ----------------------------------------------------------------------
        # 步骤 5: 触发后续的追剧检查任务链 (逻辑不变)
        # ----------------------------------------------------------------------
        if total_to_add > 0:
            logger.info("--- 任务链：即将自动触发【检查所有在追剧集】任务 ---")
            task_manager.update_status_from_thread(99, "扫描完成，正在启动追剧检查...")
            time.sleep(2) # 给前端一点反应时间
            try:
                # ★★★ 核心修复：从 extensions 获取正确的 WatchlistProcessor 实例 ★★★
                watchlist_proc = extensions.watchlist_processor_instance
                if watchlist_proc:
                    # 直接调用 task_process_watchlist，并把正确的处理器实例传给它
                    task_process_watchlist(
                        processor=watchlist_proc, 
                        item_id=None, 
                        force_full_update=False
                    )
                    final_message = "自动化流程完成：扫描与追剧检查均已结束。"
                    task_manager.update_status_from_thread(100, final_message)
                else:
                    # 这是一个健壮性检查，防止实例未被正确初始化
                    raise RuntimeError("WatchlistProcessor 未初始化，无法执行链式任务。")
            except Exception as e_chain:
                 logger.error(f"执行链式任务【检查所有在追剧集】时失败: {e_chain}", exc_info=True)
                 task_manager.update_status_from_thread(-1, f"链式任务失败: {e_chain}")
        else:
            final_message = "任务完成！没有发现可新增或需要更新状态的剧集。"
            logger.info(final_message)
            task_manager.update_status_from_thread(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 任务时发生严重错误: {e}", exc_info=True)
        task_manager.update_status_from_thread(-1, f"任务失败: {e}")

# ★★★ 全新、独立的媒体库缺集扫描任务 ★★★
def task_scan_library_gaps(processor):
    """
    【V8 - 最终正确版】沿用原版“先同步后分析”的可靠流程。
    修复了分析环节，使其能够精确地找出每个季缺失的具体集数，
    并将详细信息更新到数据库，同时为不完整的季提交订阅。
    """
    task_name = "媒体库缺集扫描"
    logger.info(f"--- 开始执行 '{task_name}' 任务 ---")
    
    def progress_updater(progress, message):
        task_manager.update_status_from_thread(progress, message)

    try:
        # 步骤 1: 获取媒体库中的所有剧集 (逻辑不变)
        progress_updater(5, "正在从 Emby 获取媒体库中的剧集列表...")
        all_series_in_libs = processor._get_series_to_process(
            where_clause="", 
            include_all_series=True
        )
        if not all_series_in_libs:
            progress_updater(100, "任务完成：在指定的媒体库中未找到任何剧集。")
            return
        
        all_series_tmdb_ids = [s['tmdb_id'] for s in all_series_in_libs if s.get('tmdb_id')]
        if not all_series_tmdb_ids:
            progress_updater(100, "任务完成：媒体库中的剧集均缺少有效的 TMDb ID。")
            return

        total_series = len(all_series_in_libs)
        logger.info(f"  ➜ 发现 {total_series} 部剧集，即将开始同步全量元数据...")
        progress_updater(10, f"发现 {total_series} 部剧集，准备同步元数据...")

        # 步骤 2: 高性能元数据同步 (保留原版可靠逻辑)
        tmdb_full_data = tmdb.batch_get_full_series_details_tmdb(
            all_series_tmdb_ids, 
            processor.tmdb_api_key,
            progress_callback=progress_updater  
        )
        existing_children_ids = media_db.get_all_children_for_series_batch(all_series_tmdb_ids)
        missing_records_to_insert = []
        for series_id, children in tmdb_full_data.items():
            for child_item in children:
                child_tmdb_id = str(child_item.get("id"))
                if child_tmdb_id not in existing_children_ids:
                    child_item['parent_series_tmdb_id'] = series_id
                    missing_records_to_insert.append(child_item)
        
        if missing_records_to_insert:
            progress_updater(50, f"发现 {len(missing_records_to_insert)} 条缺失记录，正在批量写入数据库...")
            media_db.batch_insert_media_metadata(missing_records_to_insert)
        else:
            progress_updater(50, "元数据完整性检查完成，无需补充新记录。")

        if processor.is_stop_requested():
            progress_updater(100, "任务已中止。")
            return

        # 步骤 3: 【核心修复】执行全新的、详细的数据库分析
        logger.info("  ➜ 所有剧集元数据同步完成，开始进行详细的缺集分析...")
        progress_updater(50, "元数据同步完成，开始数据库分析...")
        
        seasons_with_gaps = watchlist_db.find_detailed_missing_episodes(all_series_tmdb_ids)

        # 步骤 4: 【核心修复】整合详细的缺集数据并更新到数据库
        logger.info("  ➜ 正在整合详细的缺集分析结果，准备更新前端显示数据...")
        progress_updater(55, "正在整合分析结果...")

        gaps_by_series = {}
        for gap_info in seasons_with_gaps:
            series_id = gap_info['parent_series_tmdb_id']
            season_num = gap_info['season_number']
            missing_eps = sorted(gap_info.get('missing_episodes', []))
            # 构建更详细的JSON结构
            gaps_by_series.setdefault(series_id, []).append({
                "season": season_num,
                "missing": missing_eps
            })
        
        final_gaps_data_to_update = {
            series_id: gaps_by_series.get(series_id, [])
            for series_id in all_series_tmdb_ids
        }
        
        if final_gaps_data_to_update:
            watchlist_db.batch_update_gaps_info(final_gaps_data_to_update)
            logger.info("  ➜ 成功将最新的详细缺集信息同步到所有被扫描的剧集中。")
        
        progress_updater(60, "缺集信息已更新，准备提交订阅...")
        
        if not seasons_with_gaps:
            progress_updater(100, "分析完成：媒体库中所有剧集的季都是完整的。")
            return

        # 步骤 5: 为所有不完整的季，一次性批量提交订阅请求
        total_seasons_to_sub = len(seasons_with_gaps)
        logger.info(f"  ➜ 分析完成！共发现 {total_seasons_to_sub} 个不完整的季需要重新订阅。")
        
        series_info_map = {s['tmdb_id']: s for s in all_series_in_libs if s.get('tmdb_id')}
        media_info_batch_for_sub = []
        for i, gap_info in enumerate(seasons_with_gaps):
            series_tmdb_id = gap_info['parent_series_tmdb_id']
            season_num = gap_info['season_number']
            season_tmdb_id = gap_info['season_tmdb_id']
            
            if not season_tmdb_id: continue # 如果季ID没找到，无法订阅

            series_info = series_info_map.get(series_tmdb_id)
            series_title = series_info.get('item_name', '未知剧集') if series_info else '未知剧集'
            
            progress = 60 + int(((i + 1) / total_seasons_to_sub) * 40)
            progress_updater(progress, f"({i+1}/{total_seasons_to_sub}) 准备订阅: {series_title} 第 {season_num} 季")
            
            media_info_batch_for_sub.append({
                'tmdb_id': season_tmdb_id,
                'title': f"{series_title} - 第 {season_num} 季",
                'parent_series_tmdb_id': series_tmdb_id,
                'season_number': season_num
            })

        if media_info_batch_for_sub:
            request_db.set_media_status_wanted(
                tmdb_ids=[info['tmdb_id'] for info in media_info_batch_for_sub],
                item_type='Season',
                source={"type": "gap_scan", "reason": "incomplete_season"},
                media_info_list=media_info_batch_for_sub
            )

        final_message = f"任务完成！共为 {len(media_info_batch_for_sub)} 个不完整的季提交了订阅请求。"
        progress_updater(100, final_message)

    except Exception as e:
        logger.error(f"执行 '{task_name}' 时发生严重错误: {e}", exc_info=True)
        progress_updater(-1, f"任务失败: {e}")