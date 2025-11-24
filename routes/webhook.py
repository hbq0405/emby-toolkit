# routes/webhook.py

import collections
import threading
import time
import random
from datetime import datetime, timezone
from flask import Blueprint, request, jsonify
from typing import Optional, List
from gevent import spawn_later, spawn, sleep
from gevent.lock import Semaphore

import task_manager
import handler.emby as emby
import config_manager
import handler.telegram as telegram
import extensions
from extensions import SYSTEM_UPDATE_MARKERS, SYSTEM_UPDATE_LOCK, RECURSION_SUPPRESSION_WINDOW
from core_processor import MediaProcessor
from tasks import (
    task_auto_sync_template_on_policy_change, task_sync_metadata_cache,
    task_sync_all_metadata, task_sync_images, task_apply_main_cast_to_episodes
)
from handler.custom_collection import FilterEngine
from services.cover_generator import CoverGeneratorService
from database import collection_db, settings_db, user_db, maintenance_db, media_db
from database.log_db import LogDBManager
from handler.tmdb import get_movie_details, get_tv_details
import logging
logger = logging.getLogger(__name__)

# 创建一个新的蓝图
webhook_bp = Blueprint('webhook_bp', __name__)

# --- 模块级变量 ---
WEBHOOK_BATCH_QUEUE = collections.deque()
WEBHOOK_BATCH_LOCK = threading.Lock()
WEBHOOK_BATCH_DEBOUNCE_TIME = 5
WEBHOOK_BATCH_DEBOUNCER = None

UPDATE_DEBOUNCE_TIMERS = {}
UPDATE_DEBOUNCE_LOCK = threading.Lock()
UPDATE_DEBOUNCE_TIME = 15
# --- 视频流预检常量 ---
STREAM_CHECK_MAX_RETRIES = 60   # 最大重试次数 
STREAM_CHECK_INTERVAL = 10      # 每次轮询间隔(秒)
STREAM_CHECK_SEMAPHORE = Semaphore(5) # 限制并发预检的数量，防止大量入库时查挂 Emby

def _handle_full_processing_flow(processor: 'MediaProcessor', item_id: str, force_full_update: bool, new_episode_ids: Optional[List[str]] = None):
    """
    【Webhook 专用】编排一个新入库媒体项的完整处理流程。
    包括：元数据处理 -> 自定义合集匹配 -> 封面生成。
    """
    if not processor:
        logger.error(f"完整处理流程中止：核心处理器 (MediaProcessor) 未初始化。")
        return

    item_details = emby.get_emby_item_details(item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
    if not item_details:
        logger.error(f"  ➜ 无法获取项目 {item_id} 的详情，任务中止。")
        return
    
    item_name_for_log = item_details.get("Name", f"ID:{item_id}")

    processor.check_and_add_to_watchlist(item_details)

    processed_successfully = processor.process_single_item(item_id, force_full_update=force_full_update)
    
    if not processed_successfully:
        logger.warning(f"  ➜ 项目 '{item_name_for_log}' 的元数据处理未成功完成，跳过自定义合集匹配。")
        return

    try:
        tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
        item_name = item_details.get("Name", f"ID:{item_id}")
        if not tmdb_id:
            logger.debug("  ➜ 媒体项缺少TMDb ID，无法进行自定义合集匹配。")
            return

        media_metadata_map = media_db.get_media_details_by_tmdb_ids([tmdb_id])
        item_metadata = media_metadata_map.get(tmdb_id) # 使用 .get() 安全获取

        # ★★★ 核心修复：如果数据库里没有，就从 Emby 的详情里实时构建一个 ★★★
        if not item_metadata:
            logger.warning(f"  ➜ 无法从本地缓存找到 TMDb ID {tmdb_id} 的元数据，将尝试从 Emby 详情实时构建。")
            item_metadata = {
                "tmdb_id": tmdb_id,
                "title": item_details.get("Name"),
                "item_type": item_details.get("Type"),
                "genres_json": item_details.get("Genres", []),
                # ... 你可以根据需要从 item_details 添加更多字段 ...
            }
        
        # 再次检查，如果连实时构建都失败，才放弃
        if not item_metadata or not item_metadata.get('item_type'):
            logger.error(f"  ➜ 无法确定媒体项 {tmdb_id} 的类型，合集匹配中止。")
            return

        # ▼▼▼ 步骤 1: 将获取媒体库信息的逻辑提前 ▼▼▼
        library_info = emby.get_library_root_for_item(item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
        if not library_info:
            logger.warning(f"  ➜ 无法为项目 '{item_name_for_log}' 定位到其所属的媒体库根，将无法进行基于媒体库的合集匹配。")
            # 注意：这里我们只记录警告，不中止任务，因为可能还有不限制媒体库的合集需要匹配
            media_library_id = None
        else:
            media_library_id = library_info.get("Id")

        # --- 匹配 Filter (筛选) 类型的合集 ---
        engine = FilterEngine()
        
        # 【关键修改】在这里将获取到的 media_library_id 传递给 find_matching_collections
        matching_filter_collections = engine.find_matching_collections(item_metadata, media_library_id=media_library_id)

        if matching_filter_collections:
            logger.info(f"  ➜ 《{item_name}》匹配到 {len(matching_filter_collections)} 个筛选类合集，正在追加...")
            for collection in matching_filter_collections:
                # 步骤 1: 更新 Emby 实体合集
                emby.append_item_to_collection(
                    collection_id=collection['emby_collection_id'],
                    item_emby_id=item_id,
                    base_url=processor.emby_url,
                    api_key=processor.emby_api_key,
                    user_id=processor.emby_user_id
                )
                
                # ★★★ 核心修复：同步更新我们自己的数据库缓存 ★★★
                collection_db.append_item_to_filter_collection_db(
                    collection_id=collection['id'],
                    new_item_tmdb_id=tmdb_id,
                    new_item_emby_id=item_id,
                    collection_name=collection['name'], 
                    item_name=item_name
                )
        else:
            logger.info(f"  ➜ 《{item_name}》没有匹配到任何筛选类合集。")

        # --- 匹配 List (榜单) 类型的合集 ---
        updated_list_collections = collection_db.match_and_update_list_collections_on_item_add(
            new_item_tmdb_id=tmdb_id,
            new_item_emby_id=item_id,
            new_item_name=item_name
        )
        
        if updated_list_collections:
            logger.info(f"  ➜ 《{item_name}》匹配到 {len(updated_list_collections)} 个榜单类合集，正在追加...")
            for collection_info in updated_list_collections:
                emby.append_item_to_collection(
                    collection_id=collection_info['emby_collection_id'],
                    item_emby_id=item_id,
                    base_url=processor.emby_url,
                    api_key=processor.emby_api_key,
                    user_id=processor.emby_user_id
                )
        else:
             logger.info(f"  ➜ 《{item_name}》没有匹配到任何需要更新状态的榜单类合集。")

        all_matching_collection_ids = []
        if matching_filter_collections:
            all_matching_collection_ids.extend([c['id'] for c in matching_filter_collections])
        if updated_list_collections:
            # 注意：match_and_update_list_collections_on_item_add 需要被修改，
            # 让它返回包含数据库 ID 的字典列表
            # 假设它已经修改好了
            all_matching_collection_ids.extend([c['id'] for c in updated_list_collections])

    except Exception as e:
        logger.error(f"  ➜ 为新入库项目 '{item_name_for_log}' 匹配自定义合集时发生意外错误: {e}", exc_info=True)

    # --- 封面生成逻辑 ---
    try:
        cover_config = settings_db.get_setting('cover_generator_config') or {}

        if cover_config.get("enabled") and cover_config.get("transfer_monitor"):
            logger.info(f"  ➜ 检测到 '{item_details.get('Name')}' 入库，将为其所属媒体库生成新封面...")
            
            # ▼▼▼ 步骤 2: 复用已获取的 library_info，无需重复获取 ▼▼▼
            if not library_info:
                logger.warning(f"  ➜ (封面生成) 无法为项目 '{item_name_for_log}' 定位到其所属的媒体库根，跳过封面生成。")
                return

            library_id = library_info.get("Id") # library_id 变量在这里被重新赋值，但不影响上面的逻辑
            library_name = library_info.get("Name", library_id)
            
            if library_info.get('CollectionType') not in ['movies', 'tvshows', 'boxsets', 'mixed', 'music']:
                logger.debug(f"  ➜ 父级 '{library_name}' 不是一个常规媒体库，跳过封面生成。")
                return

            server_id = 'main_emby'
            library_unique_id = f"{server_id}-{library_id}"
            if library_unique_id in cover_config.get("exclude_libraries", []):
                logger.info(f"  ➜ 媒体库 '{library_name}' 在忽略列表中，跳过。")
                return
            
            TYPE_MAP = {'movies': 'Movie', 'tvshows': 'Series', 'music': 'MusicAlbum', 'boxsets': 'BoxSet', 'mixed': 'Movie,Series'}
            collection_type = library_info.get('CollectionType')
            item_type_to_query = TYPE_MAP.get(collection_type)
            
            item_count = 0
            if library_id and item_type_to_query:
                item_count = emby.get_item_count(base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id, parent_id=library_id, item_type=item_type_to_query) or 0
            
            logger.info(f"  ➜ 正在为媒体库 '{library_name}' 生成封面 (当前实时数量: {item_count}) ---")
            cover_service = CoverGeneratorService(config=cover_config)
            cover_service.generate_for_library(emby_server_id=server_id, library=library_info, item_count=item_count)
        else:
            logger.debug("  ➜ 封面生成器或入库监控未启用，跳过封面生成。")

        # ======================================================================
        # ★★★ 核心修改：在所有流程的最后，调用“补票员” ★★★
        # ======================================================================
        if all_matching_collection_ids:
            emby_config = {
                "url": processor.emby_url,
                "api_key": processor.emby_api_key,
            }
            collection_db.update_user_caches_on_item_add(
                new_item_emby_id=item_id,
                new_item_tmdb_id=tmdb_id,
                new_item_name=item_name,
                matching_collection_ids=all_matching_collection_ids,
                emby_config=emby_config
            )

    except Exception as e:
        logger.error(f"  ➜ 在新入库后执行精准封面生成或权限补票时发生错误: {e}", exc_info=True)

    logger.trace(f"  ➜ Webhook 任务及所有后续流程完成: '{item_name_for_log}'")

    # ======================================================================
    # ★★★ TG的入库通知 - START ★★★
    # ======================================================================
    try:
        # 直接调用 telegram_handler 中的新函数，传递所需参数
        telegram.send_media_notification(
            item_details=item_details, 
            notification_type='new', 
            new_episode_ids=new_episode_ids
        )
            
    except Exception as e:
        logger.error(f"触发入库通知时发生错误: {e}", exc_info=True)

    logger.trace(f"  ➜ Webhook 任务及所有后续流程完成: '{item_name_for_log}'")

# --- 辅助函数 ---
def _process_batch_webhook_events():
    global WEBHOOK_BATCH_DEBOUNCER
    with WEBHOOK_BATCH_LOCK:
        items_in_batch = list(set(WEBHOOK_BATCH_QUEUE))
        WEBHOOK_BATCH_QUEUE.clear()
        WEBHOOK_BATCH_DEBOUNCER = None

    if not items_in_batch:
        return

    logger.info(f"  ➜ 防抖计时器到期，开始批量处理 {len(items_in_batch)} 个 Emby Webhook 新增/入库事件。")

    # ★★★ 核心修复：恢复 V5 版本的、能够记录具体分集ID的数据结构 ★★★
    parent_items = collections.defaultdict(lambda: {
        "name": "", "type": "", "episode_ids": set()
    })
    
    for item_id, item_name, item_type in items_in_batch:
        parent_id = item_id
        parent_name = item_name
        parent_type = item_type
        
        if item_type == "Episode":
            series_id = emby.get_series_id_from_child_id(
                item_id, extensions.media_processor_instance.emby_url,
                extensions.media_processor_instance.emby_api_key, extensions.media_processor_instance.emby_user_id, item_name=item_name
            )
            if not series_id:
                logger.warning(f"  ➜ 批量处理中，分集 '{item_name}' 未找到所属剧集，跳过。")
                continue
            
            parent_id = series_id
            parent_type = "Series"
            
            # 将具体的分集ID添加到记录中
            parent_items[parent_id]["episode_ids"].add(item_id)
            
            # 更新父项的名字（只需一次）
            if not parent_items[parent_id]["name"]:
                series_details = emby.get_emby_item_details(parent_id, extensions.media_processor_instance.emby_url, extensions.media_processor_instance.emby_api_key, extensions.media_processor_instance.emby_user_id, fields="Name")
                parent_items[parent_id]["name"] = series_details.get("Name", item_name) if series_details else item_name
        else:
            # 如果事件是电影或剧集容器本身，也记录下来
            parent_items[parent_id]["name"] = parent_name
        
        # 更新父项的类型
        parent_items[parent_id]["type"] = parent_type

    logger.info(f"  ➜ 批量事件去重后，将为 {len(parent_items)} 个独立媒体项分派任务。")

    for parent_id, item_info in parent_items.items():
        parent_name = item_info['name']
        parent_type = item_info['type']
        
        is_already_processed = parent_id in extensions.media_processor_instance.processed_items_cache

        if not is_already_processed:
            
            # 默认情况下，不强制深度更新
            force_full_update_for_new_item = False
            
            # 如果是首次入库的剧集，根据是否有缓存决定传递参数
            if parent_type == 'Series':
                logger.info(f"  ➜ [前置判断] 检测到新入库剧集 '{parent_name}'，正在检查本地缓存...")
                
                # 1. 先获取TMDb ID
                item_details_for_check = emby.get_emby_item_details(parent_id, extensions.media_processor_instance.emby_url, extensions.media_processor_instance.emby_api_key, extensions.media_processor_instance.emby_user_id, fields="ProviderIds")
                tmdb_id_for_check = (item_details_for_check.get("ProviderIds", {}) if item_details_for_check else {}).get("Tmdb")

                if tmdb_id_for_check:
                    try:
                        # ★★★ 核心修改：直接调用新的、干净的数据库函数 ★★★
                        cache_exists = media_db.does_series_have_valid_actor_cache(tmdb_id_for_check)
                        
                        if cache_exists:
                            logger.info(f"  ➜ [前置判断] 发现有效缓存，不传递深度处理参数。")
                            force_full_update_for_new_item = False
                        else:
                            logger.info(f"  ➜ [前置判断] 未发现有效缓存，将传递深度处理参数以获取最全演员表。")
                            force_full_update_for_new_item = True
                            
                    except Exception as e_check:
                        # 这里的异常捕获仍然有用，以防万一数据库层抛出未捕获的异常
                        logger.error(f"  ➜ [前置判断] 检查缓存时发生意外错误: {e_check}，为保险起见，将启用深度处理模式。")
                        force_full_update_for_new_item = True
            
            # 对于电影，永远是 False，走常规流程
            else: 
                force_full_update_for_new_item = False
            
            logger.info(f"  ➜ 为 '{parent_name}' 分派【完整处理】任务 (原因: 首次入库)。")
            task_manager.submit_task(
                _handle_full_processing_flow,
                task_name=f"Webhook完整处理: {parent_name}",
                item_id=parent_id,
                force_full_update=force_full_update_for_new_item,
                new_episode_ids=list(item_info["episode_ids"]) 
            )
        else:
            # ★★★ 核心修复：恢复正确的追更处理逻辑 ★★★
            if parent_type == 'Series':
                episode_ids_to_update = list(item_info["episode_ids"])
                
                # 只有在确实有新分集入库时才执行任务
                if not episode_ids_to_update:
                    logger.info(f"  ➜ 剧集 '{parent_name}' 有更新事件，但未发现具体的新增分集，将触发一次轻量元数据缓存更新。")
                    task_manager.submit_task(
                        task_sync_metadata_cache,
                        task_name=f"Webhook元数据更新: {parent_name}",
                        processor_type='media',
                        item_id=parent_id,
                        item_name=parent_name
                    )
                    continue

                logger.info(f"  ➜ 为 '{parent_name}' 分派【轻量化更新】任务 (原因: 追更)，将处理 {len(episode_ids_to_update)} 个新分集。")
                task_manager.submit_task(
                    task_apply_main_cast_to_episodes,
                    task_name=f"轻量化同步演员表: {parent_name}",
                    processor_type='media',
                    series_id=parent_id,
                    episode_ids=episode_ids_to_update # <-- 现在传递的是具体的分集ID列表
                )
                task_manager.submit_task(
                    task_sync_metadata_cache,
                    task_name=f"Webhook增量元数据更新: {parent_name}",
                    processor_type='media',
                    item_id=parent_id,
                    item_name=parent_name,
                    episode_ids_to_add=episode_ids_to_update 
                )
            else: # 电影等其他类型
                logger.info(f"  ➜ 媒体项 '{parent_name}' 已处理过，将触发一次轻量元数据缓存更新。")
                task_manager.submit_task(
                    task_sync_metadata_cache,
                    task_name=f"Webhook元数据更新: {parent_name}",
                    processor_type='media',
                    item_id=parent_id,
                    item_name=parent_name
                )

    logger.info("  ➜ 所有 Webhook 批量任务已成功分派。")

def _trigger_metadata_update_task(item_id, item_name):
    """触发元数据同步任务"""
    logger.info(f"  ➜ 防抖计时器到期，为 '{item_name}' (ID: {item_id}) 执行元数据缓存同步任务。")
    task_manager.submit_task(
        task_sync_all_metadata,
        task_name=f"元数据同步: {item_name}",
        processor_type='media',
        item_id=item_id,
        item_name=item_name
    )

def _trigger_images_update_task(item_id, item_name, update_description, sync_timestamp_iso):
    """触发图片备份任务"""
    logger.info(f"  ➜ 防抖计时器到期，为 '{item_name}' (ID: {item_id}) 执行图片备份任务。")
    task_manager.submit_task(
        task_sync_images,
        task_name=f"图片备份: {item_name}",
        processor_type='media',
        item_id=item_id,
        update_description=update_description,
        sync_timestamp_iso=sync_timestamp_iso
    )

def _enqueue_webhook_event(item_id, item_name, item_type):
    """
    将事件加入批量处理队列，并管理防抖计时器。
    """
    global WEBHOOK_BATCH_DEBOUNCER
    with WEBHOOK_BATCH_LOCK:
        WEBHOOK_BATCH_QUEUE.append((item_id, item_name, item_type))
        logger.debug(f"  ➜ [队列] 项目 '{item_name}' ({item_type}) 已加入处理队列。当前积压: {len(WEBHOOK_BATCH_QUEUE)}")
        
        if WEBHOOK_BATCH_DEBOUNCER is None or WEBHOOK_BATCH_DEBOUNCER.ready():
            logger.info(f"  ➜ [队列] 启动批量处理计时器，将在 {WEBHOOK_BATCH_DEBOUNCE_TIME} 秒后执行。")
            WEBHOOK_BATCH_DEBOUNCER = spawn_later(WEBHOOK_BATCH_DEBOUNCE_TIME, _process_batch_webhook_events)
        else:
            logger.debug("  ➜ [队列] 批量处理计时器运行中，等待合并。")

def _wait_for_stream_data_and_enqueue(item_id, item_name, item_type):
    """
    预检视频流数据（带并发限制版）。
    """
    # 只有电影和剧集分集需要检查流数据
    if item_type not in ['Movie', 'Episode']:
        _enqueue_webhook_event(item_id, item_name, item_type)
        return

    # ★★★ 核心修改：使用信号量上下文管理器 ★★★
    # 如果当前已有 5 个任务在检查，第 6 个任务会在这里“阻塞”等待，
    # 直到前面有任务完成（无论成功还是超时）释放锁。
    # 注意：这里的阻塞是 Greenlet 级别的，不会阻塞 Flask 主线程。
    with STREAM_CHECK_SEMAPHORE:
        logger.info(f"  ➜ [预检] 开始检查 '{item_name}' (ID:{item_id}) 的视频流数据...")

        app_config = config_manager.APP_CONFIG
        emby_url = app_config.get("emby_server_url")
        emby_key = app_config.get("emby_api_key")
        emby_user_id = extensions.media_processor_instance.emby_user_id

        for i in range(STREAM_CHECK_MAX_RETRIES):
            try:
                item_details = emby.get_emby_item_details(
                    item_id=item_id,
                    emby_server_url=emby_url,
                    emby_api_key=emby_key,
                    user_id=emby_user_id,
                    fields="MediaSources"
                )

                if not item_details:
                    logger.warning(f"  ➜ [预检] 无法获取 '{item_name}' 详情，可能已被删除。停止等待。")
                    return

                media_sources = item_details.get("MediaSources", [])
                has_valid_video_stream = False
                
                if media_sources:
                    for source in media_sources:
                        media_streams = source.get("MediaStreams", [])
                        for stream in media_streams:
                            if stream.get("Type") == "Video":
                                if stream.get("Codec") or stream.get("Width"):
                                    has_valid_video_stream = True
                                    break
                        if has_valid_video_stream:
                            break
                
                if has_valid_video_stream:
                    logger.info(f"  ➜ [预检] 成功检测到 '{item_name}' 的视频流数据 (耗时: {i * STREAM_CHECK_INTERVAL}s)，加入队列。")
                    _enqueue_webhook_event(item_id, item_name, item_type)
                    return
                
                # 还没准备好，释放 CPU 给其他协程，稍后重试
                logger.debug(f"  ➜ [预检] '{item_name}' 暂无视频流数据，等待重试 ({i+1}/{STREAM_CHECK_MAX_RETRIES})...")
                sleep(STREAM_CHECK_INTERVAL + random.uniform(0, 2))

            except Exception as e:
                logger.error(f"  ➜ [预检] 检查 '{item_name}' 时发生错误: {e}")
                sleep(STREAM_CHECK_INTERVAL + random.uniform(0, 2))

        # 超时强制入库
        logger.warning(f"  ➜ [预检] 超时！在 {STREAM_CHECK_MAX_RETRIES * STREAM_CHECK_INTERVAL} 秒内未提取到 '{item_name}' 的视频流数据。强制加入队列。")
        _enqueue_webhook_event(item_id, item_name, item_type)

# --- Webhook 路由 ---
@webhook_bp.route('/webhook/emby', methods=['POST'])
@extensions.processor_ready_required
def emby_webhook():
    data = request.json
    # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
    # ★★★            魔法日志 - START            ★★★
    # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
    # try:
    #     import json
    #     # 使用 WARNING 级别和醒目的 emoji，让它在日志中脱颖而出
    #     logger.warning("✨✨✨ [魔法日志] 收到原始 Emby Webhook 负载，内容如下: ✨✨✨")
    #     # 将整个 JSON 数据格式化后打印出来
    #     logger.warning(json.dumps(data, indent=2, ensure_ascii=False))
    # except Exception as e:
    #     logger.error(f"[魔法日志] 记录原始 Webhook 时出错: {e}")
    # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
    # ★★★             魔法日志 - END             ★★★
    # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
    event_type = data.get("Event") if data else "未知事件"
    logger.debug(f"  ➜ 收到Emby Webhook: {event_type}")

    USER_DATA_EVENTS = [
        "item.markfavorite", "item.unmarkfavorite",
        "item.markplayed", "item.markunplayed",
        "playback.start", "playback.pause", "playback.stop",
        "item.rate"
    ]

    if event_type == "user.policyupdated":
        updated_user = data.get("User", {})
        updated_user_id = updated_user.get("Id")
        updated_user_name = updated_user.get("Name", "未知用户")
        
        if not updated_user_id:
            return jsonify({"status": "event_ignored_no_user_id"}), 200

        # ★★★ 核心逻辑: 在处理前，先检查信号旗 ★★★
        with SYSTEM_UPDATE_LOCK:
            last_update_time = SYSTEM_UPDATE_MARKERS.get(updated_user_id)
            # 如果找到了标记，并且时间戳在我们的抑制窗口期内
            if last_update_time and (time.time() - last_update_time) < RECURSION_SUPPRESSION_WINDOW:
                logger.debug(f"  ➜ 忽略由系统内部同步触发的用户 '{updated_user_name}' 的权限更新 Webhook。")
                # 为了保险起见，用完就删掉这个标记
                del SYSTEM_UPDATE_MARKERS[updated_user_id]
                # 直接返回成功，不再创建任何后台任务
                return jsonify({"status": "event_ignored_system_triggered"}), 200
        
        # 如果上面的检查通过了（即这是一个正常的手动操作），才继续执行原来的逻辑
        logger.info(f"  ➜ 检测到用户 '{updated_user_name}' 的权限策略已更新，将分派后台任务检查模板同步。")
        task_manager.submit_task(
            task_auto_sync_template_on_policy_change,
            task_name=f"自动同步权限 (源: {updated_user_name})",
            processor_type='media',
            updated_user_id=updated_user_id
        )
        return jsonify({"status": "auto_sync_task_submitted"}), 202

    if event_type in USER_DATA_EVENTS:
        user_from_webhook = data.get("User", {})
        user_id = user_from_webhook.get("Id")
        user_name = user_from_webhook.get("Name")
        user_name_for_log = user_name or user_id
        item_from_webhook = data.get("Item", {})
        item_id_from_webhook = item_from_webhook.get("Id")
        item_type_from_webhook = item_from_webhook.get("Type")

        if not user_id or not item_id_from_webhook:
            return jsonify({"status": "event_ignored_missing_data"}), 200

        id_to_update_in_db = None
        if item_type_from_webhook in ['Movie', 'Series']:
            id_to_update_in_db = item_id_from_webhook
        elif item_type_from_webhook == 'Episode':
            series_id = emby.get_series_id_from_child_id(
                item_id=item_id_from_webhook,
                base_url=config_manager.APP_CONFIG.get("emby_server_url"),
                api_key=config_manager.APP_CONFIG.get("emby_api_key"),
                user_id=user_id
            )
            if series_id:
                id_to_update_in_db = series_id
        
        if not id_to_update_in_db:
            return jsonify({"status": "event_ignored_unsupported_type_or_not_found"}), 200

        update_data = {"user_id": user_id, "item_id": id_to_update_in_db}
        
        if event_type in ["item.markfavorite", "item.unmarkfavorite", "item.markplayed", "item.markunplayed", "item.rate"]:
            user_data_from_item = item_from_webhook.get("UserData", {})
            if 'IsFavorite' in user_data_from_item:
                update_data['is_favorite'] = user_data_from_item['IsFavorite']
            if 'Played' in user_data_from_item:
                update_data['played'] = user_data_from_item['Played']
                if user_data_from_item['Played']:
                    update_data['playback_position_ticks'] = 0
                    update_data['last_played_date'] = datetime.now(timezone.utc)

        elif event_type in ["playback.start", "playback.pause", "playback.stop"]:
            playback_info = data.get("PlaybackInfo", {})
            if playback_info:
                position_ticks = playback_info.get('PositionTicks')
                if position_ticks is not None:
                    update_data['playback_position_ticks'] = position_ticks
                
                update_data['last_played_date'] = datetime.now(timezone.utc)
                
                if event_type == "playback.stop":
                    if playback_info.get('PlayedToCompletion') is True:
                        update_data['played'] = True
                        update_data['playback_position_ticks'] = 0
                    else:
                        update_data['played'] = False

        try:
            if len(update_data) > 2:
                user_db.upsert_user_media_data(update_data)
                item_name_for_log = f"ID:{id_to_update_in_db}"
                try:
                    # 为了日志，只请求 Name 字段，提高效率
                    item_details_for_log = emby.get_emby_item_details(
                        item_id=id_to_update_in_db,
                        emby_server_url=config_manager.APP_CONFIG.get("emby_server_url"),
                        emby_api_key=config_manager.APP_CONFIG.get("emby_api_key"),
                        user_id=user_id,
                        fields="Name"
                    )
                    if item_details_for_log and item_details_for_log.get("Name"):
                        item_name_for_log = item_details_for_log.get("Name")
                except Exception:
                    # 如果获取失败，不影响主流程，日志中继续使用ID
                    pass
                logger.trace(f"  ➜ Webhook: 已更新用户 '{user_name_for_log}' 对项目 '{item_name_for_log}' 的状态 ({event_type})。")
                return jsonify({"status": "user_data_updated"}), 200
            else:
                logger.debug(f"  ➜ Webhook '{event_type}' 未包含可更新的用户数据，已忽略。")
                return jsonify({"status": "event_ignored_no_updatable_data"}), 200
        except Exception as e:
            logger.error(f"  ➜ 通过 Webhook 更新用户媒体数据时失败: {e}", exc_info=True)
            return jsonify({"status": "error_updating_user_data"}), 500

    trigger_events = ["item.add", "library.new", "library.deleted", "metadata.update", "image.update"]
    if event_type not in trigger_events:
        logger.debug(f"  ➜ Webhook事件 '{event_type}' 不在触发列表 {trigger_events} 中，将被忽略。")
        return jsonify({"status": "event_ignored_not_in_trigger_list"}), 200

    item_from_webhook = data.get("Item", {}) if data else {}
    original_item_id = item_from_webhook.get("Id")
    original_item_name = item_from_webhook.get("Name", "未知项目")
    original_item_type = item_from_webhook.get("Type")
    
    trigger_types = ["Movie", "Series", "Episode"]
    if not (original_item_id and original_item_type in trigger_types):
        logger.debug(f"  ➜ Webhook事件 '{event_type}' (项目: {original_item_name}, 类型: {original_item_type}) 被忽略。")
        return jsonify({"status": "event_ignored_no_id_or_wrong_type"}), 200

    if event_type == "library.deleted":
            try:
                series_id_from_webhook = item_from_webhook.get("SeriesId") if original_item_type == "Episode" else None
                # 直接调用新的、干净的数据库函数
                maintenance_db.cleanup_deleted_media_item(
                    item_id=original_item_id,
                    item_name=original_item_name,
                    item_type=original_item_type,
                    series_id_from_webhook=series_id_from_webhook
                )
                return jsonify({"status": "delete_event_processed"}), 200
            except Exception as e:
                logger.error(f"处理删除事件 for item {original_item_id} 时发生错误: {e}", exc_info=True)
                return jsonify({"status": "error_processing_remove_event", "error": str(e)}), 500
    
    if event_type in ["item.add", "library.new"]:
        spawn(_wait_for_stream_data_and_enqueue, original_item_id, original_item_name, original_item_type)
        
        logger.info(f"  ➜ Webhook: 收到入库事件 '{original_item_name}'，已启动后台流数据预检任务。")
        return jsonify({"status": "processing_started_with_stream_check", "item_id": original_item_id}), 202

    # --- 为 metadata.update 和 image.update 事件准备通用变量 ---
    id_to_process = original_item_id
    name_for_task = original_item_name
    
    if original_item_type == "Episode":
        series_id = emby.get_series_id_from_child_id(
            original_item_id, extensions.media_processor_instance.emby_url,
            extensions.media_processor_instance.emby_api_key, extensions.media_processor_instance.emby_user_id, item_name=original_item_name
        )
        if not series_id:
            logger.warning(f"  ➜ Webhook '{event_type}': 剧集 '{original_item_name}' 未找到所属剧集，跳过。")
            return jsonify({"status": "event_ignored_episode_no_series_id"}), 200
        id_to_process = series_id
        
        full_series_details = emby.get_emby_item_details(
            item_id=id_to_process, emby_server_url=extensions.media_processor_instance.emby_url,
            emby_api_key=extensions.media_processor_instance.emby_api_key, user_id=extensions.media_processor_instance.emby_user_id
        )
        if full_series_details:
            name_for_task = full_series_details.get("Name", f"未知剧集(ID:{id_to_process})")

    # --- 分离 metadata.update 和 image.update 的处理逻辑 ---
    if event_type == "metadata.update":
        with UPDATE_DEBOUNCE_LOCK:
            if id_to_process in UPDATE_DEBOUNCE_TIMERS:
                old_timer = UPDATE_DEBOUNCE_TIMERS[id_to_process]
                old_timer.kill()
                logger.debug(f"  ➜ 已为 '{name_for_task}' 取消了旧的同步计时器，将以最新的元数据更新事件为准。")

            logger.info(f"  ➜ 为 '{name_for_task}' 设置了 {UPDATE_DEBOUNCE_TIME} 秒的元数据同步延迟，以合并连续的更新事件。")
            new_timer = spawn_later(
                UPDATE_DEBOUNCE_TIME,
                _trigger_metadata_update_task,
                item_id=id_to_process,
                item_name=name_for_task
            )
            UPDATE_DEBOUNCE_TIMERS[id_to_process] = new_timer
        return jsonify({"status": "metadata_update_task_debounced", "item_id": id_to_process}), 202

    elif event_type == "image.update":
        
        # 1. 先获取原始的描述
        original_update_description = data.get("Description", "Webhook Image Update")
        webhook_received_at_iso = datetime.now(timezone.utc).isoformat()

        # 2. 准备一个变量来存放最终要执行的描述
        final_update_description = original_update_description

        with UPDATE_DEBOUNCE_LOCK:
            # 3. 检查是否已有计时器
            if id_to_process in UPDATE_DEBOUNCE_TIMERS:
                old_timer = UPDATE_DEBOUNCE_TIMERS[id_to_process]
                old_timer.kill()
                logger.debug(f"  ➜ 已为 '{name_for_task}' 取消了旧的同步计时器，将以最新的封面更新事件为准。")
                
                # ★★★ 关键逻辑：如果取消了旧的，说明发生了合并，我们不再相信单一描述 ★★★
                logger.info(f"  ➜ 检测到图片更新事件合并，将任务升级为“完全同步”。")
                final_update_description = "Multiple image updates detected" # 给一个通用描述

            logger.info(f"  ➜ 为 '{name_for_task}' 设置了 {UPDATE_DEBOUNCE_TIME} 秒的封面备份延迟...")
            new_timer = spawn_later(
                UPDATE_DEBOUNCE_TIME,
                _trigger_images_update_task,
                item_id=id_to_process,
                item_name=name_for_task,
                update_description=final_update_description, # <-- 使用我们最终决定的描述
                sync_timestamp_iso=webhook_received_at_iso
            )
            UPDATE_DEBOUNCE_TIMERS[id_to_process] = new_timer
        
        return jsonify({"status": "asset_update_task_debounced", "item_id": id_to_process}), 202

    return jsonify({"status": "event_unhandled"}), 500