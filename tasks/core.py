# tasks/core.py
# 任务注册与任务链核心

import time
import threading
import logging

import constants
import extensions
import task_manager

# 导入各个模块的任务函数
from .actors import (task_enrich_aliases, task_persons_translation, 
                     task_process_actor_subscriptions, task_merge_duplicate_actors,
                     task_purge_ghost_actors)
from .media import task_role_translation, task_populate_metadata_cache, task_execute_auto_tagging_rules, task_scan_monitor_folders, task_backup_mediainfo, task_restore_mediainfo, task_repair_p115_fingerprints, task_restore_nfo_and_images, task_fill_studio_images
from .watchlist import task_process_watchlist, task_refresh_completed_series, task_scan_old_seasons_backfill, task_add_all_series_to_watchlist
from .custom_collections import task_process_all_custom_collections, process_single_custom_collection
from .tmdb_collections import task_refresh_collections
from .subscriptions import task_auto_subscribe, task_manual_subscribe_batch
from .covers import task_generate_all_covers, task_generate_all_custom_collection_covers
from .cleanup import task_scan_for_cleanup_issues 
from .users import task_sync_all_user_data, task_check_expired_users
from .discover import task_update_daily_theme
from .resubscribe import task_update_resubscribe_cache, task_resubscribe_library
from .vector_tasks import task_generate_embeddings
from .system_update import task_check_and_update_container
from .p115 import task_scan_and_organize_115, task_sync_115_directory_tree, task_full_sync_strm_and_subs, task_monitor_115_life_events, task_recalculate_library_washing_priorities, task_manual_correct_organize_records, task_play_pool_daily_speedtest
from .hdhive import task_hdhive_auto_checkin
from .shared_resource_tasks import task_shared_resource_maintenance, share_all_library, task_shared_share_status_sync_high_freq

logger = logging.getLogger(__name__)


# 任务说明：作为任务注册表的补充元数据，供前端悬停提示、TG 菜单说明等场景复用。
# 新增任务时只需要在 full_registry 里登记任务，再在这里补一句 help，不需要再改前端。
TASK_HELP_TEXTS = {
    'task-chain-high-freq': '按已配置的高频刷新任务链顺序执行多个子任务，适合白天定时刷新媒体数据、追剧和订阅等轻量任务。',
    'task-chain-low-freq': '按已配置的低频维护任务链顺序执行多个子任务，适合夜间处理耗时更长、资源占用更高的维护任务。',
    'populate-metadata': '同步 Emby 媒体库基础数据到本地缓存，用于后续追剧、订阅、整理、统计和共享匹配。',
    'role-translation': '为影视条目中的角色名补充中文显示，让演员角色展示更友好。',
    'actor-translation': '为演员、导演等人物信息补充中文名。',
    'process-watchlist': '刷新智能追剧列表，检查连载剧更新、补充集图片和元数据。',
    'actor-tracking': '刷新演员订阅，根据关注演员检查新作品并触发后续订阅处理。',
    'custom-collections': '刷新全部自建合集，重新拉取榜单并匹配、订阅。',
    'auto-subscribe': '统一处理电影、剧集、追更和求资源等订阅需求，按规则搜索、转存或登记缺口。',
    'generate-all-covers': '批量生成原生媒体封面，适合封面缺失或封面风格需要统一时执行。',
    'generate-custom-collection-covers': '批量生成自建合集封面，让合集封面保持统一风格。',
    'check-expired-users': '检查会员或体验卡到期用户，并执行到期后的权限处理。',
    'refresh_completed_series': '刷新完结剧集状态和季集信息，补充图片和元数据，以及订阅新季。',
    'scan-monitor-folders': '扫描配置的监控目录，发现新增媒体文件后进入识别、整理或入库流程，适合查漏补缺。',
    'scan-organize-115': '扫描 115 网盘待整理目录，并按规则识别、整理、生成记录，适合新增资源后手动触发。',
    'full-sync-strm': '全量重建 STRM 与字幕文件，保持网盘和本地一致，适合媒体库重建或迁移时使用。',
    'monitor-115-life-events': '增量处理 115 网盘文件变化，功能较弱，不熟悉不建议使用。',
    'backup-mediainfo': '备份本地媒体信息缓存，避免重建库或迁移后丢失媒体参数。',
    'repair-p115-fingerprints': '扫描在库电影和分集，补齐共享资源必需的 115 PC 与 SHA1 以及缓存；优先从本地缓存恢复，必要时查询 115。',
    'restore_mediainfo': '从备份中还原媒体信息缓存，适合重装Emby 容器或迁移时使用，或修复本地媒体信息缓存丢失、迁移或缓存损坏后恢复数据。',
    'hdhive-auto-checkin': '执行影巢自动签到，获取签到奖励或保持账号活跃。',
    'restore-nfo-and-images': '从备份或缓存中还原 NFO、海报、背景图等媒体附属文件。',
    'shared-resource-maintenance': '维护共享资源池，包含登记缺口、自动分享、状态检查、清理失效分享和共享订阅消费等。',
    'share-all-library': '增量登记本地媒体库到共享中心。启动前会排除已有有效共享，只处理新增或需要修复的媒体。',
    'add-all-series-to-watchlist': '扫描全库剧集并加入智能追剧管理，适合首次处理存量剧集时使用。',
    'process_all_custom_collections': '立即重新生成所有自建合集，通常用于合集规则调整后手动刷新。',
    'process-single-custom-collection': '只刷新指定的单个自建合集，通常由合集详情页触发。',
    'scan-cleanup-issues': '扫描重复媒体、异常文件和可清理项目，帮助发现占空间或重复入库的问题。',
    'resubscribe-library': '执行媒体订阅删除/洗版相关处理，按配置清理并重新订阅需要替换的资源。',
    'update-daily-theme': '更新每日主题推荐内容，用于影视探索页的主题展示。',
    'manual_subscribe_batch': '处理手动批量订阅队列，适合一次性提交多个想看的电影或剧集。',
    'scan_old_seasons_backfill': '扫描缺季老剧并尝试补齐缺失季度，适合老剧季信息不完整时使用。',
    'contribute-mediainfo': '把本地媒体信息贡献到中心。',
    'generate_embeddings': '为媒体生成向量索引，用于语义搜索、相似推荐等智能功能。',
    'refresh-collections': '刷新 TMDb 原生合集信息，让电影系列合集保持最新。',
    'update-resubscribe-cache': '刷新媒体整理/洗版缓存，为后续洗版筛选和订阅判断提供基础数据。',
    'merge-duplicate-actors': '合并重复演员条目，减少同一演员因别名、翻译不同造成的分身。',
    'sync-all-user-data': '同步全部用户数据，例如播放记录、收藏、观看状态等用户维度信息。',
    'execute-auto-tagging-rules': '执行自动打标规则，根据媒体参数、路径、类型等条件批量添加标签。',
    'enrich-aliases': '补充演员别名、译名等元数据资料，提高搜索和人物匹配命中率。',
    'purge-ghost-actors': '删除没有有效关联作品的幽灵演员，清理人物库冗余数据。',
    'sync-115-directory-tree': '同步 115 网盘目录树缓存，适合目录结构变化大或缓存不准时使用。',
    'fill-studio-images': '补全制作公司/工作室图标，让媒体详情页展示更完整。',
    'shared-share-status-sync': '高频同步共享分享状态，检查分享是否仍可用并更新中心状态。',
    'system-auto-update': '检查并执行系统容器自动更新，适合需要保持 ETK 最新版本时使用。',
    'recalculate_library_washing_priorities': '重新计算媒体库中每个媒体的洗版优先级，适合调整洗版规则或新增优先级因素后使用。',
    'manual-correct-organize-records': '将整理记录按用户指定的 TMDb、媒体类型、季号和目标目录重新整理；走媒体任务队列。',
}


# 任务链入口只应该作为页面顶部的专用按钮出现，不放进“临时任务”网格里重复展示。
TASK_KEYS_HIDDEN_FROM_MANUAL_RUN = {
    'task-chain-high-freq',
    'task-chain-low-freq',
}


def get_task_help(task_key: str, fallback_name: str = '') -> str:
    """返回任务说明文案，供前端和其它展示入口复用。"""
    return TASK_HELP_TEXTS.get(task_key) or fallback_name or '暂无任务说明。'


def get_available_task_definitions(context: str = 'chain'):
    """
    返回给前端使用的任务列表。
    - chain：只返回适合任务链编排的子任务。
    - all：返回全部任务，供 TG 菜单等需要完整任务池的地方使用。
    - manual：返回临时任务按钮列表，沿用任务链可编排任务池，只额外排除页面顶部已有专用按钮的任务链入口。
    - 保持 get_task_registry() 的执行侧返回结构不变，避免影响任务调度。
    """
    normalized_context = context if context in ('chain', 'all', 'manual') else 'chain'

    # manual 是页面上的“临时任务”按钮池。
    # 这里不能从 all 取，否则 full_registry 里标记为 False 的后台/参数型任务也会全部暴露到前端。
    # 原页面行为等同于 chain：只展示 info[3] == True 的常规任务。
    registry_context = 'chain' if normalized_context == 'manual' else normalized_context
    registry = get_task_registry(context=registry_context)

    available_tasks = []
    for key, info in registry.items():
        if normalized_context == 'manual' and key in TASK_KEYS_HIDDEN_FROM_MANUAL_RUN:
            continue

        task_name = info[1]
        task_help = get_task_help(key, task_name)
        task_item = {
            'key': key,
            'name': task_name,
            'help': task_help,
            # 兼容前端旧字段命名，避免部分组件仍然读 description 时显示为空。
            'description': task_help,
        }
        if registry_context == 'all' and len(info) >= 3:
            task_item['processor_type'] = info[2]
        available_tasks.append(task_item)

    return available_tasks

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
                logger.warning(f"  ➜ '{task_name}' 达到 {max_runtime_minutes} 分钟的运行时长限制，将发送停止信号...")
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
                    logger.warning(f"  ➜ '{task_name}' 被用户手动中止。")
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
                    'populate-metadata',
                    'restore_mediainfo'
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
        'populate-metadata': (task_populate_metadata_cache, "同步媒体数据", 'media', True),
        'enrich-aliases': (task_enrich_aliases, "演员数据补充", 'media', True),
        'role-translation': (task_role_translation, "中文化角色名", 'media', True),
        'actor-translation': (task_persons_translation, "中文化人物名", 'media', True),
        'process-watchlist': (task_process_watchlist, "刷新智能追剧", 'watchlist', True),
        'actor-tracking': (task_process_actor_subscriptions, "刷新演员订阅", 'actor', True),
        'custom-collections': (task_process_all_custom_collections, "刷新自建合集", 'media', True),
        'auto-subscribe': (task_auto_subscribe, "统一订阅处理", 'media', True),
        'generate-all-covers': (task_generate_all_covers, "生成原生封面", 'media', True),
        'generate-custom-collection-covers': (task_generate_all_custom_collection_covers, "生成合集封面", 'media', True),
        'refresh_completed_series': (task_refresh_completed_series, "全量刷新剧集", 'watchlist', True),
        'scan-monitor-folders': (task_scan_monitor_folders, "扫描监控目录", 'media', True),
        'scan-organize-115': (task_scan_and_organize_115, "网盘文件整理", 'media', True),
        'full-sync-strm': (task_full_sync_strm_and_subs, "全量生成STRM", 'media', True),
        'monitor-115-life-events': (task_monitor_115_life_events, "增量生成STRM", 'media', True),
        'backup-mediainfo': (task_backup_mediainfo, "备份媒体信息", 'media', True),
        'repair-p115-fingerprints': (task_repair_p115_fingerprints, "补齐缓存指纹", 'media', True),
        'restore_mediainfo': (task_restore_mediainfo, "还原媒体信息", 'media', True),
        'hdhive-auto-checkin': (task_hdhive_auto_checkin, "影巢自动签到", 'media', True),
        'restore-nfo-and-images': (task_restore_nfo_and_images, "还原NFO和封面", 'media', True),
        'shared-resource-maintenance': (task_shared_resource_maintenance, "共享资源维护", 'media', True),
        'sync-all-user-data': (task_sync_all_user_data, "同步用户数据", 'media', True),
        'generate_embeddings': (task_generate_embeddings, "生成媒体向量", 'media', True),
        'purge-ghost-actors': (task_purge_ghost_actors, "删除幽灵演员", 'media', True),
        'system-auto-update': (task_check_and_update_container, "系统自动更新", 'media', True),
        
        # --- 不适合任务链的、需要特定参数的任务 ---
        'add-all-series-to-watchlist': (task_add_all_series_to_watchlist, "扫描全库剧集", 'watchlist', False),
        'process_all_custom_collections': (task_process_all_custom_collections, "生成所有自建合集", 'media', False),
        'process-single-custom-collection': (process_single_custom_collection, "生成单个自建合集", 'media', False),
        'scan-cleanup-issues': (task_scan_for_cleanup_issues, "扫描重复媒体", 'media', False),
        'resubscribe-library': (task_resubscribe_library, "媒体订阅删除", 'media', False),
        'update-daily-theme': (task_update_daily_theme, "更新每日主题", 'media', False),
        'manual_subscribe_batch': (task_manual_subscribe_batch, "手动订阅处理", 'media', False),
        'scan_old_seasons_backfill': (task_scan_old_seasons_backfill, "扫描缺季的剧", 'watchlist', False),
        'refresh-collections': (task_refresh_collections, "刷新原生合集", 'media', False),
        'update-resubscribe-cache': (task_update_resubscribe_cache, "刷新媒体整理", 'media', False),
        'merge-duplicate-actors': (task_merge_duplicate_actors, "合并分身演员", 'media', False),
        'execute-auto-tagging-rules': (task_execute_auto_tagging_rules, "自动打标规则", 'media', False),
        'sync-115-directory-tree': (task_sync_115_directory_tree, "同步网盘目录", 'media', False),
        'fill-studio-images': (task_fill_studio_images, "补全工作室图标", 'media', False),
        'check-expired-users': (task_check_expired_users, "检查过期用户", 'media', False),
        'share-all-library': (share_all_library, "一键登记媒体库", 'media', False),
        'recalculate_library_washing_priorities': (task_recalculate_library_washing_priorities, "重新计算洗版优先级", 'media', False),
        'manual-correct-organize-records': (task_manual_correct_organize_records, "手动重组整理记录", 'media', False),
        # 系统硬编码后台任务：False = 前端不可见/不可编排，执行周期由 scheduler_manager.py 固定控制。
        'shared-share-status-sync': (task_shared_share_status_sync_high_freq, "共享分享状态同步", 'media', False),
        'play-pool-daily-speedtest': (task_play_pool_daily_speedtest, "小号池测速", 'media', False),
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
