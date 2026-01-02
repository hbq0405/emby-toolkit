# tasks/core.py
# 任务注册与任务链核心

import time
import threading
import logging

import constants
import extensions
import task_manager

# 导入各个模块的任务函数
from .actors import (task_sync_person_map, task_enrich_aliases, task_actor_translation, 
                     task_process_actor_subscriptions, task_purge_unregistered_actors, task_merge_duplicate_actors,
                     task_purge_ghost_actors)
from .media import task_role_translation, task_populate_metadata_cache 
from .watchlist import task_process_watchlist, task_run_new_season_check, task_scan_library_gaps, task_scan_old_seasons_backfill
from .custom_collections import task_process_all_custom_collections, process_single_custom_collection
from .tmdb_collections import task_refresh_collections
from .subscriptions import task_auto_subscribe, task_manual_subscribe_batch
from .covers import task_generate_all_covers, task_generate_all_custom_collection_covers
from .cleanup import task_scan_for_cleanup_issues 
from .users import task_sync_all_user_data, task_check_expired_users
from .discover import task_update_daily_theme
from .resubscribe import task_update_resubscribe_cache, task_resubscribe_library
from .vector_tasks import task_generate_embeddings

logger = logging.getLogger(__name__)

def _task_run_chain_internal(processor, task_name: str, sequence_config_key: str, max_runtime_config_key: str):
    """
    【V10 - 内部通用任务链执行器】
    - 将任务链的执行逻辑抽象出来，供高频和低频任务链调用。
    - 通过传入不同的配置键来读取对应的任务序列和运行时长。
    """
    task_sequence = processor.config.get(sequence_config_key, [])
    if not task_sequence:
        logger.info(f"--- '{task_name}' 检测到任务序列为空，已自动跳过 ---")
        return

    total_tasks = len(task_sequence)
    logger.info(f"--- '{task_name}' 已启动，共包含 {total_tasks} 个子任务 ---")
    task_manager.update_status_from_thread(0, f"{task_name}启动，共 {total_tasks} 个任务。")

    # --- 准备计时器和停止信号 ---
    max_runtime_minutes = processor.config.get(max_runtime_config_key, 0)
    timeout_seconds = max_runtime_minutes * 60 if max_runtime_minutes > 0 else None
    
    main_processor = extensions.media_processor_instance
    main_processor.clear_stop_signal()
    timeout_triggered = threading.Event()

    def timeout_watcher():
        if timeout_seconds:
            logger.info(f"'{task_name}' 运行时长限制为 {max_runtime_minutes} 分钟，计时器已启动。")
            time.sleep(timeout_seconds)
            
            if not main_processor.is_stop_requested():
                logger.warning(f"'{task_name}' 达到 {max_runtime_minutes} 分钟的运行时长限制，将发送停止信号...")
                timeout_triggered.set()
                main_processor.signal_stop()

    timer_thread = threading.Thread(target=timeout_watcher, daemon=True)
    timer_thread.start()

    try:
        # --- 主任务循环 ---
        registry = get_task_registry(context='all')

        for i, task_key in enumerate(task_sequence):
            if main_processor.is_stop_requested():
                if not timeout_triggered.is_set():
                    logger.warning(f"  🚫 '{task_name}' 被用户手动中止。")
                break

            task_info = registry.get(task_key)
            if not task_info:
                logger.error(f"任务链警告：在注册表中未找到任务 '{task_key}'，已跳过。")
                continue

            try:
                task_function, task_description, processor_type = task_info
            except ValueError:
                logger.error(f"任务链错误：任务 '{task_key}' 的注册信息格式不正确，已跳过。")
                continue

            progress = int((i / total_tasks) * 100)
            status_message = f"({i+1}/{total_tasks}) 正在执行: {task_description}"
            logger.info(f"--- {status_message} ---")
            task_manager.update_status_from_thread(progress, status_message)

            try:
                target_processor = None
                if processor_type == 'media':
                    # ★★★ 核心修复 ★★★
                    # 优先使用传递给任务链的 processor 实例。
                    # 这个 processor 就是主 media_processor_instance。
                    target_processor = processor
                elif processor_type == 'watchlist':
                    target_processor = extensions.watchlist_processor_instance
                elif processor_type == 'actor':
                    target_processor = extensions.actor_subscription_processor_instance
                
                if not target_processor:
                    logger.error(f"任务链错误：无法为任务 '{task_description}' 找到类型为 '{processor_type}' 的处理器实例，已跳过。")
                    continue

                # ★★★ 核心修复：根据任务键，使用正确的关键字参数调用 ★★★
                tasks_requiring_force_flag = [
                    'role-translation', 
                    'enrich-aliases', 
                    'process-watchlist', 
                    'populate-metadata'
                ]
                
                if task_key in tasks_requiring_force_flag:
                    # 所有在列表中的任务，都以增量模式调用
                    task_function(target_processor, force_full_update=False)
                else:
                    # 其他任务，正常调用
                    task_function(target_processor)

                time.sleep(1)

            except Exception as e:
                if isinstance(e, InterruptedError):
                    logger.info(f"子任务 '{task_description}' 响应停止信号，已中断。")
                else:
                    error_message = f"任务链中的子任务 '{task_description}' 执行失败: {e}"
                    logger.error(error_message, exc_info=True)
                    task_manager.update_status_from_thread(progress, f"子任务'{task_description}'失败，继续...")
                    time.sleep(3)
                continue

    finally:
        # --- 任务结束后的清理和状态报告 ---
        final_message = f"'{task_name}' 执行完毕。"
        if main_processor.is_stop_requested():
            if timeout_triggered.is_set():
                final_message = f"'{task_name}' 已达最长运行时限，自动结束。"
            else:
                final_message = f"'{task_name}' 已被用户手动中止。"
        
        logger.info(f"--- {final_message} ---")
        task_manager.update_status_from_thread(100, final_message)
        
        main_processor.clear_stop_signal()


def task_run_chain_high_freq(processor):
    """高频刷新任务链的入口点"""
    _task_run_chain_internal(
        processor,
        task_name="高频刷新任务链",
        sequence_config_key=constants.CONFIG_OPTION_TASK_CHAIN_SEQUENCE,
        max_runtime_config_key=constants.CONFIG_OPTION_TASK_CHAIN_MAX_RUNTIME_MINUTES
    )

def task_run_chain_low_freq(processor):
    """低频维护任务链的入口点"""
    _task_run_chain_internal(
        processor,
        task_name="低频维护任务链",
        sequence_config_key=constants.CONFIG_OPTION_TASK_CHAIN_LOW_FREQ_SEQUENCE,
        max_runtime_config_key=constants.CONFIG_OPTION_TASK_CHAIN_LOW_FREQ_MAX_RUNTIME_MINUTES
    )


def get_task_registry(context: str = 'all'):
    """
    返回一个包含所有可执行任务的字典。
    - 新增 'task-chain-high-freq' 和 'task-chain-low-freq' 两个独立的任务链入口。
    """
    # 完整的任务注册表
    # 格式: 任务Key: (任务函数, 任务描述, 处理器类型, 是否适合在任务链中运行)
    full_registry = {
        # --- 任务链本身，不能嵌套 ---
        'task-chain-high-freq': (task_run_chain_high_freq, "高频刷新任务链", 'media', False),
        'task-chain-low-freq': (task_run_chain_low_freq, "低频维护任务链", 'media', False),

        # --- 适合任务链的常规任务 ---
        'sync-person-map': (task_sync_person_map, "同步演员数据", 'media', True),
        'enrich-aliases': (task_enrich_aliases, "演员数据补充", 'media', True),
        'populate-metadata': (task_populate_metadata_cache, "同步媒体数据", 'media', True),
        'role-translation': (task_role_translation, "中文化角色名", 'media', True),
        'actor-translation': (task_actor_translation, "中文化演员名", 'media', True),
        'process-watchlist': (task_process_watchlist, "刷新智能追剧", 'watchlist', True),
        'actor-tracking': (task_process_actor_subscriptions, "刷新演员订阅", 'actor', True),
        'refresh-collections': (task_refresh_collections, "刷新原生合集", 'media', True),
        'custom-collections': (task_process_all_custom_collections, "刷新自建合集", 'media', True),
        'update-resubscribe-cache': (task_update_resubscribe_cache, "刷新媒体整理", 'media', True),
        'auto-subscribe': (task_auto_subscribe, "统一订阅处理", 'media', True),
        'generate-all-covers': (task_generate_all_covers, "生成原生封面", 'media', True),
        'generate-custom-collection-covers': (task_generate_all_custom_collection_covers, "生成合集封面", 'media', True),
        'merge-duplicate-actors': (task_merge_duplicate_actors, "合并分身演员", 'media', True),
        'purge-unregistered-actors': (task_purge_unregistered_actors, "删除黑户演员", 'media', True),
        'purge-ghost-actors': (task_purge_ghost_actors, "删除幽灵演员", 'media', True),
        'sync-all-user-data': (task_sync_all_user_data, "同步用户数据", 'media', True),
        'check-expired-users': (task_check_expired_users, "检查过期用户", 'media', True),
        'generate_embeddings': (task_generate_embeddings, "生成媒体向量", 'media', True),
        'run_new_season_check': (task_run_new_season_check, "新季上线检查", 'watchlist', True),
        
        # --- 不适合任务链的、需要特定参数的任务 ---
        'process_all_custom_collections': (task_process_all_custom_collections, "生成所有自建合集", 'media', False),
        'process-single-custom-collection': (process_single_custom_collection, "生成单个自建合集", 'media', False),
        'scan-cleanup-issues': (task_scan_for_cleanup_issues, "扫描媒体重复项", 'media', False),
        'run_new_season_check': (task_run_new_season_check, "新季上线检查", 'watchlist', False),
        'resubscribe-library': (task_resubscribe_library, "媒体订阅删除", 'media', False),
        'update-daily-theme': (task_update_daily_theme, "更新每日主题", 'media', False),
        'manual_subscribe_batch': (task_manual_subscribe_batch, "手动订阅处理", 'media', False),
        'scan-library-gaps': (task_scan_library_gaps, "扫描缺集的季", 'watchlist', False),
        'scan_old_seasons_backfill': (task_scan_old_seasons_backfill, "扫描缺季的剧", 'watchlist', False),
    }

    if context == 'chain':
        return {
            key: (info[0], info[1]) 
            for key, info in full_registry.items() 
            if info[3]
        }
    
    return {
        key: (info[0], info[1], info[2]) 
        for key, info in full_registry.items()
    }