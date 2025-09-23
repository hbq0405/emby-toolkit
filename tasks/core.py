# tasks/core.py
# 任务注册与任务链核心

import time
import threading
import logging

import constants
import extensions
import task_manager

# 导入各个模块的任务函数
from .actors import task_sync_person_map, task_enrich_aliases, task_actor_translation_cleanup, task_process_actor_subscriptions, task_purge_ghost_actors
from .media import task_run_full_scan, task_populate_metadata_cache
from .watchlist import task_process_watchlist, task_run_revival_check
from .collections import task_refresh_collections, task_process_all_custom_collections, task_process_custom_collection
from .subscriptions import task_auto_subscribe, task_update_resubscribe_cache, task_resubscribe_library
from .covers import task_full_image_sync, task_generate_all_covers, task_generate_all_custom_collection_covers
from .maintenance import task_scan_for_cleanup_issues, task_apply_main_cast_to_episodes
from .users import task_sync_all_user_data, task_check_expired_users


logger = logging.getLogger(__name__)


def task_run_chain(processor, task_sequence: list):
    """
    【V9 - 参数名修正最终版】
    - 彻底修复了任务链的调用逻辑，能为不同任务传递正确的关键字参数。
    - 确保所有子任务都能被正确调用，解决所有 'unexpected keyword argument' 错误。
    """
    task_name = "自动化任务链"
    total_tasks = len(task_sequence)
    logger.info(f"--- '{task_name}' 已启动，共包含 {total_tasks} 个子任务 ---")
    task_manager.update_status_from_thread(0, f"任务链启动，共 {total_tasks} 个任务。")

    # --- 准备计时器和停止信号 ---
    max_runtime_minutes = processor.config.get(constants.CONFIG_OPTION_TASK_CHAIN_MAX_RUNTIME_MINUTES, 0)
    timeout_seconds = max_runtime_minutes * 60 if max_runtime_minutes > 0 else None
    
    main_processor = extensions.media_processor_instance
    main_processor.clear_stop_signal()
    timeout_triggered = threading.Event()

    def timeout_watcher():
        if timeout_seconds:
            logger.info(f"任务链运行时长限制为 {max_runtime_minutes} 分钟，计时器已启动。")
            time.sleep(timeout_seconds)
            
            if not main_processor.is_stop_requested():
                logger.warning(f"任务链达到 {max_runtime_minutes} 分钟的运行时长限制，将发送停止信号...")
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
                    logger.warning(f"'{task_name}' 被用户手动中止。")
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
                    target_processor = extensions.media_processor_instance
                elif processor_type == 'watchlist':
                    target_processor = extensions.watchlist_processor_instance
                elif processor_type == 'actor':
                    target_processor = extensions.actor_subscription_processor_instance
                
                if not target_processor:
                    logger.error(f"任务链错误：无法为任务 '{task_description}' 找到类型为 '{processor_type}' 的处理器实例，已跳过。")
                    continue

                # ★★★ 核心修复：根据任务键，使用正确的关键字参数调用 ★★★
                if task_key == 'full-scan':
                    # task_run_full_scan 需要 'force_reprocess'
                    task_function(target_processor, force_reprocess=False)
                elif task_key in ['enrich-aliases', 'sync-images-map']:
                    # 这两个任务需要 'force_full_update'
                    task_function(target_processor, force_full_update=False)
                else:
                    # 其他所有任务都不需要额外的布尔参数
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

def get_task_registry(context: str = 'all'):
    """
    【V4 - 最终完整版】
    返回一个包含所有可执行任务的字典。
    每个任务的定义现在是一个四元组：(函数, 描述, 处理器类型, 是否适合任务链)。
    """
    # 完整的任务注册表
    # 格式: 任务Key: (任务函数, 任务描述, 处理器类型, 是否适合在任务链中运行)
    full_registry = {
        'task-chain': (task_run_chain, "自动化任务链", 'media', False), # 任务链本身不能嵌套

        # --- 适合任务链的常规任务 ---
        'sync-person-map': (task_sync_person_map, "同步演员数据", 'media', True),
        'enrich-aliases': (task_enrich_aliases, "演员数据补充", 'media', True),
        'populate-metadata': (task_populate_metadata_cache, "同步媒体数据", 'media', True),
        'full-scan': (task_run_full_scan, "中文化角色名", 'media', True),
        'actor-cleanup': (task_actor_translation_cleanup, "中文化演员名", 'media', True),
        'process-watchlist': (task_process_watchlist, "刷新智能追剧", 'watchlist', True),
        'refresh-collections': (task_refresh_collections, "刷新原生合集", 'media', True),
        'custom-collections': (task_process_all_custom_collections, "刷新自建合集", 'media', True),
        'update-resubscribe-cache': (task_update_resubscribe_cache, "刷新洗版状态", 'media', True),
        'actor-tracking': (task_process_actor_subscriptions, "刷新演员订阅", 'actor', True),
        'auto-subscribe': (task_auto_subscribe, "智能订阅缺失", 'media', True),
        'sync-images-map': (task_full_image_sync, "覆盖缓存备份", 'media', True),
        'resubscribe-library': (task_resubscribe_library, "媒体洗版订阅", 'media', True),
        'generate-all-covers': (task_generate_all_covers, "生成原生封面", 'media', True),
        'generate-custom-collection-covers': (task_generate_all_custom_collection_covers, "生成合集封面", 'media', True),
        'purge-ghost-actors': (task_purge_ghost_actors, "删除幽灵演员", 'media', True),
        'sync-all-user-data': (task_sync_all_user_data, "同步用户数据", 'media', True),
        'check-expired-users': (task_check_expired_users, "检查过期用户", 'media', True),
        

        # --- 不适合任务链的、需要特定参数的任务 ---
        'process_all_custom_collections': (task_process_all_custom_collections, "生成所有自建合集", 'media', False),
        'process-single-custom-collection': (task_process_custom_collection, "生成单个自建合集", 'media', False),
        'scan-cleanup-issues': (task_scan_for_cleanup_issues, "扫描媒体重复项", 'media', False),
        'revival-check': (task_run_revival_check, "检查剧集复活", 'watchlist', False),
        'task_apply_main_cast_to_episodes': (task_apply_main_cast_to_episodes, "轻量化同步分集演员表", 'media', False),
    }

    if context == 'chain':
        # ★★★ 核心修复 1/2：使用第四个元素 (布尔值) 来进行过滤 ★★★
        # 这将完美恢复您原来的功能
        return {
            key: (info[0], info[1]) 
            for key, info in full_registry.items() 
            if info[3]  # info[3] 就是那个 True/False 标志
        }
    
    # ★★★ 核心修复 2/2：默认情况下，返回前三个元素 ★★★
    # 这确保了“万用插座”API (/api/tasks/run) 能够正确解包，无需修改
    return {
        key: (info[0], info[1], info[2]) 
        for key, info in full_registry.items()
    }