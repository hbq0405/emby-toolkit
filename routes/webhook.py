# routes/webhook.py

import collections
import threading
import time
import os
import random
from datetime import datetime, timezone
from flask import Blueprint, request, jsonify
from typing import Optional, List
from gevent import spawn_later, spawn, sleep
from gevent.lock import Semaphore

import task_manager
import handler.emby as emby
import config_manager
import constants
import utils
import handler.telegram as telegram
import extensions
from extensions import SYSTEM_UPDATE_MARKERS, SYSTEM_UPDATE_LOCK, RECURSION_SUPPRESSION_WINDOW, DELETING_COLLECTIONS, UPDATING_IMAGES, UPDATING_METADATA
from core_processor import MediaProcessor
from tasks.watchlist import task_process_watchlist
from tasks.users import task_auto_sync_template_on_policy_change
from tasks.media import task_sync_all_metadata, task_sync_images
from handler.custom_collection import RecommendationEngine
from handler import tmdb_collections as collections_handler
from services.cover_generator import CoverGeneratorService
from database import custom_collection_db, tmdb_collection_db, settings_db, user_db, maintenance_db, media_db, queries_db, watchlist_db
from database.log_db import LogDBManager
from handler.tmdb import get_movie_details, get_tv_details
from handler.p115_service import P115Service, SmartOrganizer, get_config
try:
    from p115client import P115Client
except ImportError:
    P115Client = None
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
STREAM_CHECK_MAX_RETRIES = 6   # 最大重试次数 
STREAM_CHECK_INTERVAL = 10      # 每次轮询间隔(秒)
STREAM_CHECK_SEMAPHORE = Semaphore(5) # 限制并发预检的数量，防止大量入库时查挂 Emby

# MP 临时目录延迟清理定时器 ★★★
MP_TEMP_DIR_TIMERS = {}
MP_TEMP_DIR_LOCK = threading.Lock()

def _handle_full_processing_flow(processor: 'MediaProcessor', item_id: str, force_full_update: bool, new_episode_ids: Optional[List[str]] = None, is_new_item: bool = True):
    """
    【Webhook 统一入口】
    统一处理 新入库(New) 和 追更(Update) 两种情况。
    """
    if not processor:
        logger.error(f"  🚫 完整处理流程中止：核心处理器 (MediaProcessor) 未初始化。")
        return

    item_details = emby.get_emby_item_details(item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
    if not item_details:
        logger.error(f"  🚫 无法获取项目 {item_id} 的详情，任务中止。")
        return
    
    item_name_for_log = item_details.get("Name", f"ID:{item_id}")
    item_type = item_details.get("Type")
    tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")

    # 1. 核心调用：优先执行元数据处理 (process_single_item)
    processed_successfully = processor.process_single_item(
        item_id, 
        force_full_update=force_full_update,
        specific_episode_ids=new_episode_ids 
    )
    
    if not processed_successfully:
        logger.warning(f"  ➜ 项目 '{item_name_for_log}' 的元数据处理未成功完成，跳过后续步骤。")
        return

    # 2. 智能追剧判断 - 初始入库
    if is_new_item and item_type == "Series":
        processor.check_and_add_to_watchlist(item_details)

    # 3. 后续处理
    if is_new_item:
        try:
            tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
            item_name = item_details.get("Name", f"ID:{item_id}")
            
            # --- 匹配 List (榜单) 类型的合集 (保持不变) ---
            # 榜单类合集是静态的，需要将新入库的项目加入到 Emby 实体合集中
            if tmdb_id:
                updated_list_collections = custom_collection_db.match_and_update_list_collections_on_item_add(
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

            # ★★★ 移除 Filter 类合集的匹配逻辑 ★★★
            # Filter 类合集现在是基于 SQL 实时查询的，不需要在入库时做任何操作。
            # 只要 media_metadata 表更新了（process_single_item 已完成），SQL 查询自然能查到它。

        except Exception as e:
            logger.error(f"  ➜ 为新入库项目 '{item_name_for_log}' 匹配榜单合集时发生意外错误: {e}", exc_info=True)

        # --- 封面生成逻辑 (保持不变) ---
        try:
            cover_config = settings_db.get_setting('cover_generator_config') or {}

            if cover_config.get("enabled") and cover_config.get("transfer_monitor"):
                # ... (获取 library_info 的逻辑) ...
                library_info = emby.get_library_root_for_item(item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                
                if library_info:
                    library_id = library_info.get("Id")
                    library_name = library_info.get("Name", library_id)
                    
                    if library_info.get('CollectionType') in ['movies', 'tvshows', 'boxsets', 'mixed', 'music']:
                        server_id = 'main_emby'
                        library_unique_id = f"{server_id}-{library_id}"
                        if library_unique_id not in cover_config.get("exclude_libraries", []):
                            # ... (获取 item_count) ...
                            TYPE_MAP = {'movies': 'Movie', 'tvshows': 'Series', 'music': 'MusicAlbum', 'boxsets': 'BoxSet', 'mixed': 'Movie,Series'}
                            collection_type = library_info.get('CollectionType')
                            item_type_to_query = TYPE_MAP.get(collection_type)
                            item_count = 0
                            if library_id and item_type_to_query:
                                item_count = emby.get_item_count(base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id, parent_id=library_id, item_type=item_type_to_query) or 0

                            logger.info(f"  ➜ 正在为媒体库 '{library_name}' 生成封面 (当前实时数量: {item_count}) ---")
                            cover_service = CoverGeneratorService(config=cover_config)
                            cover_service.generate_for_library(emby_server_id=server_id, library=library_info, item_count=item_count)

            # ★★★ 移除 update_user_caches_on_item_add 调用 ★★★
            # 权限现在是实时的，不需要补票了。

        except Exception as e:
            logger.error(f"  ➜ 在新入库后执行封面生成时发生错误: {e}", exc_info=True)

        # ======================================================================
        # ★★★  TMDb 合集自动补全 ★★★
        # ======================================================================
        try:
            # 1. 检查类型 (只处理电影)
            # ★★★ 修复：直接使用 item_details 和 tmdb_id，不再依赖 item_metadata ★★★
            current_type = item_details.get('Type')
            current_tmdb_id = tmdb_id  # 这个变量在函数前面已经定义过了
            current_name = item_name   # 这个变量在函数前面也定义过了

            if current_type == 'Movie' and current_tmdb_id:
                # 2. 检查开关
                config = settings_db.get_setting('native_collections_config') or {}
                is_auto_complete_enabled = config.get('auto_complete_enabled', False)

                if is_auto_complete_enabled:
                    logger.trace(f"  ➜ 正在检查电影 '{current_name}' 所属 TMDb 合集...")
                    # 直接调用 handler
                    collections_handler.check_and_subscribe_collection_from_movie(
                        movie_tmdb_id=str(current_tmdb_id),
                        movie_name=current_name,
                        movie_emby_id=item_id
                    )
        except Exception as e:
            logger.warning(f"  ➜ 检查所属 TMDb 合集时发生错误: {e}")

    logger.trace(f"  ➜ Webhook 任务及所有后续流程完成: '{item_name_for_log}'")

    # 4. ★★★ 通知分流 ★★★
    try:
        # 如果提供了 new_episode_ids，说明是追更通知
        # 如果 is_new_item 为 True，说明是新入库通知
        notif_type = 'update' if (new_episode_ids and not is_new_item) else 'new'
        
        telegram.send_media_notification(
            item_details=item_details, 
            notification_type=notif_type, 
            new_episode_ids=new_episode_ids
        )
    except Exception as e:
        logger.error(f"触发通知失败: {e}")

    logger.trace(f"  ➜ Webhook 任务及所有后续流程完成: '{item_name_for_log}'")

    # 打标
    if is_new_item: 
        try:
            # 1. 从数据库获取最新记录
            db_record = media_db.get_media_details(str(tmdb_id), item_type)
            
            if db_record:
                # 2. 提取 Library ID
                # asset_details_json 是一个列表，取第一个即可
                assets = db_record.get('asset_details_json')
                lib_id = None
                if assets and isinstance(assets, list) and len(assets) > 0:
                    lib_id = assets[0].get('source_library_id')
                
                # 3. 提取修正后的分级 (US)
                # official_rating_json: {"US": "XXX", "DE": "18"}
                ratings = db_record.get('official_rating_json')
                us_rating = None
                if ratings and isinstance(ratings, dict):
                    us_rating = ratings.get('US')
                
                if lib_id:
                    # 既然数据都在手里了，不需要延迟，直接干！
                    logger.info(f"  ➜ [自动打标] 基于数据库最新元数据 (库ID:{lib_id}, 分级:{us_rating}) ...")
                    # 这里的 lib_name 传个占位符即可，不影响逻辑，只影响日志
                    _handle_immediate_tagging_with_lib(item_id, item_name_for_log, lib_id, "DB_Source", known_rating=us_rating)
                else:
                    logger.warning(f"  ➜ [自动打标] 数据库记录中未找到 来源库，跳过打标。")
            else:
                logger.warning(f"  ➜ [自动打标] 无法从数据库读取刚写入的记录，跳过打标。")

        except Exception as e:
            logger.warning(f"  ➜ [自动打标] 触发打标失败: {e}")

    # 刷新智能追剧状态 
    if item_type == "Series" and tmdb_id:
        def _async_trigger_watchlist():
            try:
                watching_ids = watchlist_db.get_watching_tmdb_ids()
                if str(tmdb_id) not in watching_ids:
                    logger.debug(f"  ➜ [智能追剧] 剧集 {tmdb_id} 当前不在追剧列表中 (状态非 Watching)，跳过刷新。")
                    return
                # =======================================================

                logger.info(f"  ➜ [智能追剧] 触发单项刷新...")
                task_manager.submit_task(
                    task_process_watchlist,
                    task_name=f"刷新智能追剧: {item_name_for_log}",
                    processor_type='watchlist', 
                    tmdb_id=str(tmdb_id)
                )
            except Exception as e:
                logger.error(f"  🚫 触发智能追剧任务失败: {e}")

        # 启动协程，不等待结果，直接让当前 Webhook 任务结束
        spawn(_async_trigger_watchlist)

def _handle_immediate_tagging_with_lib(item_id, item_name, lib_id, lib_name, known_rating=None):
    """
    自动打标 (支持分级过滤)。
    增加 known_rating 参数：如果调用方已经知道确切分级（如从数据库查到的），直接使用，不再查询 Emby。
    """
    try:
        processor = extensions.media_processor_instance
        tagging_config = settings_db.get_setting('auto_tagging_rules') or []
        
        # 只有当没有传入 known_rating 时，才需要去 Emby 查
        item_details = None 
        
        for rule in tagging_config:
            target_libs = rule.get('library_ids', [])
            if not target_libs or lib_id in target_libs:
                tags = rule.get('tags', [])
                rating_filters = rule.get('rating_filters', [])
                
                if tags:
                    # ★★★ 核心修改：分级匹配逻辑 ★★★
                    if rating_filters:
                        # 1. 优先使用传入的已知分级 (数据库里的真理)
                        current_rating = known_rating
                        
                        # 2. 如果没传，且还没查过 Emby，则去查 (兜底逻辑)
                        if not current_rating and item_details is None:
                            item_details = emby.get_emby_item_details(
                                item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id,
                                fields="OfficialRating"
                            )
                            if item_details:
                                current_rating = item_details.get('OfficialRating')
                        
                        # 3. 执行匹配
                        if not current_rating:
                            continue # 拿不到分级，跳过
                            
                        target_codes = queries_db._expand_rating_labels(rating_filters)
                        
                        # 兼容 "US: XXX" 和 "XXX" 两种格式
                        rating_code = current_rating.split(':')[-1].strip()
                        
                        if rating_code not in target_codes:
                            logger.debug(f"  🏷️ 媒体项 '{item_name}' 分级 '{current_rating}' 不满足规则限制 {rating_filters}，跳过打标。")
                            continue

                    if rating_filters:
                        rule_desc = f"分级 '{','.join(rating_filters)}'"
                    else:
                        rule_desc = f"库 '{lib_name}'"

                    logger.info(f"  🏷️ 媒体项 '{item_name}' 命中 {rule_desc} 规则，追加标签: {tags}")
                    emby.add_tags_to_item(item_id, tags, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
                
                break 
    except Exception as e:
        logger.error(f"  🚫 [自动打标] 失败: {e}")

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
        episode_ids = list(item_info["episode_ids"])
        
        # 1. 检查是否已处理
        is_already_processed = parent_id in extensions.media_processor_instance.processed_items_cache

        # 2. 检查数据库是否在线 (处理“僵尸数据”)
        if is_already_processed:
            # 这一步很快，只是查一下 media_metadata 表的 in_library 字段
            is_online_in_db = media_db.is_emby_id_in_library(parent_id)
            
            # ★★★ 优化核心：如果不在线，直接踢出缓存，视为新项目重跑 ★★★
            if not is_online_in_db:
                logger.info(f"  ➜ ⚠️ 缓存命中 '{parent_name}'，但数据库标记为离线/缺失。清除缓存，触发重新入库流程。")
                
                # 从内存缓存中移除
                if parent_id in extensions.media_processor_instance.processed_items_cache:
                    del extensions.media_processor_instance.processed_items_cache[parent_id]
                
                # 标记为未处理，后续逻辑会把它当作“新入库”来执行完整的数据库修复
                is_already_processed = False
        # 3. 统一分派任务
        task_name_prefix = "Webhook追更" if is_already_processed and episode_ids else "Webhook入库"
        
        logger.info(f"  ➜ 为 '{parent_name}' 分派任务: {task_name_prefix} (分集数: {len(episode_ids)})")
        
        task_manager.submit_task(
            _handle_full_processing_flow,
            task_name=f"{task_name_prefix}: {parent_name}",
            processor_type='media', # 确保传递 processor 实例
            item_id=parent_id,
            force_full_update=False, # Webhook 触发通常不需要强制深度刷新 TMDb
            new_episode_ids=episode_ids if episode_ids else None,
            is_new_item=not is_already_processed
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
    预检视频流数据 (基于神医 mediainfo.json 物理文件检查)。
    """
    if item_type not in ['Movie', 'Episode']:
        _enqueue_webhook_event(item_id, item_name, item_type)
        return

    logger.info(f"  ➜ [预检] 开始检查 '{item_name}' (ID:{item_id}) 的媒体信息文件...")

    app_config = config_manager.APP_CONFIG
    emby_url = app_config.get("emby_server_url")
    emby_key = app_config.get("emby_api_key")
    emby_user_id = extensions.media_processor_instance.emby_user_id

    for i in range(STREAM_CHECK_MAX_RETRIES):
        try:
            item_details = None
            
            with STREAM_CHECK_SEMAPHORE:
                item_details = emby.get_emby_item_details(
                    item_id=item_id,
                    emby_server_url=emby_url,
                    emby_api_key=emby_key,
                    user_id=emby_user_id,
                    fields="Path,MediaSources" # ★ 请求 Path 字段
                )

            if not item_details:
                logger.warning(f"  ➜ [预检] 无法获取 '{item_name}' 详情，可能已被删除。停止等待。")
                return

            has_valid_video_stream = False
            
            # ★★★ 核心修改：直接查找物理文件 ★★★
            file_path = item_details.get("Path")
            if not file_path:
                # 兜底：尝试从 MediaSources 中获取 Path
                media_sources = item_details.get("MediaSources", [])
                if media_sources:
                    file_path = media_sources[0].get("Path")
            
            if file_path:
                mediainfo_path = os.path.splitext(file_path)[0] + "-mediainfo.json"
                if os.path.exists(mediainfo_path):
                    has_valid_video_stream = True
            
            if has_valid_video_stream:
                logger.info(f"  ➜ [预检] 成功检测到 '{item_name}' 的媒体信息文件 (耗时: {i * STREAM_CHECK_INTERVAL}s)，加入队列。")
                _enqueue_webhook_event(item_id, item_name, item_type)
                return
            
            logger.debug(f"  ➜ [预检] '{item_name}' 暂无媒体信息文件，等待神医提取 ({i+1}/{STREAM_CHECK_MAX_RETRIES})...")
            sleep(STREAM_CHECK_INTERVAL + random.uniform(0, 2))

        except Exception as e:
            logger.error(f"  ➜ [预检] 检查 '{item_name}' 时发生错误: {e}")
            sleep(STREAM_CHECK_INTERVAL + random.uniform(0, 2))

    # 超时强制入库
    logger.warning(f"  ➜ [预检] 超时！在 {STREAM_CHECK_MAX_RETRIES * STREAM_CHECK_INTERVAL} 秒内未提取到 '{item_name}' 的媒体信息文件。强制加入队列。")
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
    event_type = data.get("Event") # Emby
    mp_event_type = data.get("type") # MP
    # ======================================================================
    # ★★★ 处理神医插件的 deep.delete (深度删除) 事件 ★★★
    # ======================================================================
    if event_type == "deep.delete":
        logger.info("  💀 收到神医助手深度删除通知，准备执行清理流程...")
        
        item_from_webhook = data.get("Item", {})
        original_item_id = item_from_webhook.get("Id")
        original_item_type = item_from_webhook.get("Type")
        original_item_name = item_from_webhook.get("Name", "未知项目")
        series_id_from_webhook = item_from_webhook.get("SeriesId") if original_item_type == "Episode" else None

        # --------------------------------------------------------
        # 任务 1: 清理本地数据库 (完全替代原 library.deleted)
        # --------------------------------------------------------
        if original_item_id and original_item_type:
            try:
                logger.info(f"  🧹 [深度删除] 开始清理本地数据库记录: {original_item_name}")
                maintenance_db.cleanup_deleted_media_item(
                    item_id=original_item_id,
                    item_name=original_item_name,
                    item_type=original_item_type,
                    series_id_from_webhook=series_id_from_webhook
                )
                # 刷新向量缓存
                if config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_PROXY_ENABLED) and config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_AI_VECTOR):
                    if original_item_type in ['Movie', 'Series']:
                        spawn(RecommendationEngine.refresh_cache)
                        logger.debug(f"  ➜ [智能推荐] 检测到媒体删除，已触发向量缓存刷新。")
            except Exception as e:
                logger.error(f"  ❌ [深度删除] 清理本地数据库失败: {e}", exc_info=True)

        # --------------------------------------------------------
        # 任务 2: 联动删除 115 网盘文件
        # --------------------------------------------------------
        nb_config = get_config()
        if not nb_config.get(constants.CONFIG_OPTION_115_ENABLE_SYNC_DELETE, False):
            logger.debug("  🚫 联动删除未开启，跳过网盘清理。")
            return jsonify({"status": "processed_db_only"}), 200

        description = data.get("Description", "")
        if not description:
            return jsonify({"status": "ignored_no_description"}), 200

        try:
            import re
            path_match = re.search(r'Item Path:\n(.*?)\n\n', description)
            item_path = path_match.group(1).strip() if path_match else ""

            pickcodes = []
            if "Mount Paths:\n" in description:
                mount_paths_str = description.split("Mount Paths:\n")[-1]
                urls = [line.strip() for line in mount_paths_str.split('\n') if line.strip()]
                
                for url in urls:
                    pc_match = re.search(r'/api/p115/play/([a-zA-Z0-9]+)', url)
                    if pc_match:
                        pickcodes.append(pc_match.group(1))

            if pickcodes and item_path:
                logger.info(f"  🎯 成功提取到 {len(pickcodes)} 个 115 提取码，交由后台执行联动删除。")
                from handler.p115_service import delete_115_files_by_webhook
                spawn(delete_115_files_by_webhook, item_path, pickcodes)
                return jsonify({"status": "deep_delete_task_started"}), 202
            else:
                logger.warning("  ⚠️ 深度删除通知中未找到有效的 ETK 直链或路径，跳过网盘清理。")
                return jsonify({"status": "processed_db_only_no_pickcodes"}), 200

        except Exception as e:
            logger.error(f"  ❌ 解析深度删除通知失败: {e}", exc_info=True)
            return jsonify({"status": "error_parsing_deep_delete"}), 500
    # ======================================================================
    # ★★★ 处理 MoviePilot transfer.complete 事件 ★★★
    # ======================================================================
    if mp_event_type == "transfer.complete":
        # 1. 检查配置是否开启了智能整理
        nb_config = get_config()
        if not nb_config.get(constants.CONFIG_OPTION_115_ENABLE_ORGANIZE, False):
            logger.debug("  🚫 智能整理未开启，忽略 MP 通知。")
            return jsonify({"status": "ignored_smart_organize_disabled"}), 200
        else:
            logger.info("  📥 收到 MoviePilot 上传完成通知，开始接管整理...")

        # 2. 提取关键数据
        try:
            transfer_info = data.get("data", {}).get("transferinfo", {})
            media_info = data.get("data", {}).get("mediainfo", {})
            
            # 115 文件 ID 和 文件名
            target_item = transfer_info.get("target_item", {})
            file_id = target_item.get("fileid")
            pc = target_item.get("pickcode") 
            
            # 115 当前父目录 ID (MP 创建的临时目录)
            target_dir = transfer_info.get("target_diritem", {})
            current_parent_cid = target_dir.get("fileid")
            
            # 元数据
            tmdb_id = media_info.get("tmdb_id")
            media_type_cn = media_info.get("type") 
            title = media_info.get("title")
            
            if not file_id or not tmdb_id:
                logger.warning("  ⚠️ MP 通知缺少 fileid 或 tmdb_id，无法处理。")
                return jsonify({"status": "ignored_missing_data"}), 200

            # 转换媒体类型
            media_type = 'tv' if media_type_cn == '电视剧' else 'movie'
            
            # 3. 获取共享 115 客户端
            client = P115Service.get_client()
            if not client:
                return jsonify({"status": "error_no_p115_client"}), 500
                
            # 4. 初始化智能整理器
            organizer = SmartOrganizer(client, tmdb_id, media_type, title)
            
            # 5. 计算目标分类 CID
            target_cid = organizer.get_target_cid()
            
            if target_cid:
                logger.info(f"  🚀 [MP上传] 新文件: {target_item.get('name')} (文件大小: {int(target_item.get('size', 0))/1024/1024/1024:.2f} GB)")
                
                # 构造真实的文件对象 (兼容 WebAPI 和 OpenAPI 双重结构)
                is_folder = str(target_item.get("type")) == "0"
                
                real_root_item = {
                    'n': target_item.get("name"),
                    'file_name': target_item.get("name"),
                    's': target_item.get("size"),
                    'size': target_item.get("size"),
                    'cid': current_parent_cid,
                    'parent_id': current_parent_cid,
                    'fid': file_id if not is_folder else None,
                    'file_id': file_id if not is_folder else None,
                    'pc': pc,
                    'pick_code': pc,
                    'fc': '0' if is_folder else '1',  # ★ 关键修复：明确告知是文件还是文件夹
                    'type': '0' if is_folder else '1'
                }
                
                # 双重保险：如果 MP 传的是文件夹 (type=0)，则将 cid 指向自身
                if is_folder:
                    logger.warning("  ⚠️ 检测到 MP 上传的是文件夹，这可能会导致递归扫描，请谨慎！")
                    real_root_item['cid'] = file_id
                    real_root_item['parent_id'] = file_id

                # logger.info(f"  🚀 [MP上传] 转交 SmartOrganizer.execute 处理...")
                # 复用 execute 逻辑
                success = organizer.execute(real_root_item, target_cid, delete_source=False)
                
                if success:
                    # 异步延迟删除 MP 临时目录 (带重置防抖机制) 
                    if current_parent_cid and str(current_parent_cid) != '0':
                        delay_seconds = 3600 # 60分钟
                        
                        def _delayed_delete_temp_dir(cid):
                            try:
                                c = P115Service.get_client()
                                if c:
                                    logger.info(f"  🧹 [延迟清理] 倒计时结束，正在清理 MP 临时目录 (CID: {cid})")
                                    c.fs_delete([cid])
                            except Exception as e:
                                logger.warning(f"  ⚠️ 延迟清理临时目录失败: {e}")
                            finally:
                                # 执行完毕后，从字典中移除自己，防止内存泄漏
                                with MP_TEMP_DIR_LOCK:
                                    if cid in MP_TEMP_DIR_TIMERS:
                                        del MP_TEMP_DIR_TIMERS[cid]
                                        
                        # 使用锁来安全地管理定时器
                        with MP_TEMP_DIR_LOCK:
                            # 1. 如果这个目录已经有倒计时了，杀掉旧的倒计时
                            if current_parent_cid in MP_TEMP_DIR_TIMERS:
                                MP_TEMP_DIR_TIMERS[current_parent_cid].kill()
                                logger.debug(f"  ⏳ [MP上传] 目录 (CID: {current_parent_cid}) 正在持续上传，已重置清理倒计时 ({delay_seconds//60}分钟)。")
                            else:
                                logger.info(f"  ⏳ [MP上传] 整理成功，已安排在 {delay_seconds//60} 分钟后静默清理临时目录。")
                            
                            # 2. 创建一个新的倒计时，并存入字典
                            new_timer = spawn_later(delay_seconds, _delayed_delete_temp_dir, current_parent_cid)
                            MP_TEMP_DIR_TIMERS[current_parent_cid] = new_timer

                    return jsonify({"status": "success_organized"}), 200
                else:
                    return jsonify({"status": "failed_organize"}), 500

            else:
                logger.info("  🚫 [MP上传] 未命中任何分类规则，保持原样。")
                return jsonify({"status": "ignored_no_rule_match"}), 200

        except Exception as e:
            logger.error(f"  ❌ [MP上传] 处理失败: {e}", exc_info=True)
            return jsonify({"status": "error", "message": str(e)}), 500
        
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

        # --- 立即反查并更新本地 Policy ---
        try:
            def _update_local_policy_task():
                try:
                    # 获取最新详情
                    user_details = emby.get_user_details(
                        updated_user_id, 
                        config_manager.APP_CONFIG.get("emby_server_url"), 
                        config_manager.APP_CONFIG.get("emby_api_key")
                    )
                    if user_details and 'Policy' in user_details:
                        # 更新数据库
                        user_db.upsert_emby_users_batch([user_details])
                        logger.info(f"  ➜ Webhook: 已更新用户 {updated_user_id} 的本地权限缓存。")
                except Exception as e:
                    logger.error(f"  ➜ Webhook 更新本地 Policy 失败: {e}")

            # 异步执行，不阻塞 Webhook 返回
            spawn(_update_local_policy_task)
        except Exception as e:
            logger.error(f"启动 Policy 更新任务失败: {e}")

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

    trigger_events = ["item.add", "library.new", "metadata.update", "image.update", "collection.items.removed", "deep.delete", "None"]
    if event_type not in trigger_events:
        logger.debug(f"  ➜ Webhook事件 '{event_type}' 不在触发列表 {trigger_events} 中，将被忽略。")
        return jsonify({"status": "event_ignored_not_in_trigger_list"}), 200
    
    item_from_webhook = data.get("Item", {}) if data else {}
    original_item_id = item_from_webhook.get("Id")
    original_item_type = item_from_webhook.get("Type")
    original_item_name = item_from_webhook.get("Name", "未知项目")
    
    # 如果是分集，将名字格式化为 "剧名 - 集名"，方便日志搜索
    raw_name = item_from_webhook.get("Name", "未知项目")
    series_name = item_from_webhook.get("SeriesName")
    
    if original_item_type == "Episode" and series_name:
        original_item_name = f"{series_name} - {raw_name}"
    else:
        original_item_name = raw_name
    
    trigger_types = ["Movie", "Series", "Season", "Episode", "BoxSet"]
    if not (original_item_id and original_item_type in trigger_types):
        logger.debug(f"  ➜ Webhook事件 '{event_type}' (项目: {original_item_name}, 类型: {original_item_type}) 被忽略。")
        return jsonify({"status": "event_ignored_no_id_or_wrong_type"}), 200

    # ======================================================================
    # ★★★ 处理 collection.items.removed (检查是否变空消失) ★★★
    # ======================================================================
    if event_type == "collection.items.removed":
        # Emby 发送此事件时，Item 指的是合集本身
        collection_id = item_from_webhook.get("Id")
        collection_name = item_from_webhook.get("Name")

        if collection_id in DELETING_COLLECTIONS:
            logger.debug(f"  ➜ Webhook: 忽略合集 '{collection_name}' 的移除通知 (正在执行手动删除)。")
            return jsonify({"status": "ignored_manual_deletion"}), 200
        
        if collection_id:
            logger.info(f"  ➜ Webhook: 合集 '{collection_name}' 有成员移除，正在检查合集存活状态...")
            
            def _check_collection_survival_task(processor=None):
                details = emby.get_emby_item_details(
                    item_id=collection_id,
                    emby_server_url=config_manager.APP_CONFIG.get("emby_server_url"),
                    emby_api_key=config_manager.APP_CONFIG.get("emby_api_key"),
                    user_id=config_manager.APP_CONFIG.get("emby_user_id"),
                    fields="Id",
                    silent_404=True
                )
                
                if not details:
                    logger.info(f"  🗑️ 合集 '{collection_name}' (ID: {collection_id}) 已在 Emby 中消失 (可能是变空自动删除)，同步删除本地记录...")
                    tmdb_collection_db.delete_native_collection_by_emby_id(collection_id)
                else:
                    logger.debug(f"  ✅ 合集 '{collection_name}' 依然存在，无需操作。")

            task_manager.submit_task(
                _check_collection_survival_task,
                task_name=f"检查合集存活: {collection_name}",
                processor_type='media'
            )
            return jsonify({"status": "collection_removal_check_started"}), 202

    # 过滤不在处理范围的媒体库
    if event_type in ["item.add", "library.new", "metadata.update", "image.update"]:
        processor = extensions.media_processor_instance
        
        # --- 【拦截 1】如果是系统正在生成的封面，直接拦截，不查库，不报错 ---
        if event_type == "image.update" and original_item_id in UPDATING_IMAGES:
            logger.debug(f"  ➜ Webhook: 忽略项目 '{original_item_name}' 的图片更新通知 (系统生成的封面)。")
            return jsonify({"status": "ignored_self_triggered_update"}), 200
        
        # --- 【拦截 2】如果是系统正在更新元数据，直接拦截 ---
        if event_type == "metadata.update" and original_item_id in UPDATING_METADATA:
            logger.debug(f"  ➜ Webhook: 忽略项目 '{original_item_name}' 的元数据更新通知 (系统触发的更新)。")
            return jsonify({"status": "ignored_self_triggered_metadata_update"}), 200

        # --- 【拦截 3】如果是合集(BoxSet)，它没有物理路径，直接跳过库路径检查 ---
        if original_item_type == "BoxSet":
            logger.trace(f"  ➜ Webhook: 项目 '{original_item_name}' 是合集类型，跳过媒体库路径检查。")
            # 注意：这里不 return，因为后面可能还有合集的处理逻辑
            library_info = None 
        else:
            # 正常的媒体项，才去获取所属库信息
            library_info = emby.get_library_root_for_item(
                original_item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id
            )
        
        if library_info:
            lib_id = library_info.get("Id")
            lib_name = library_info.get("Name", "未知库")
            allowed_libs = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_LIBRARIES_TO_PROCESS) or []

            # 执行打标（全库生效）
            if event_type in ["item.add", "library.new"]:
                spawn(_handle_immediate_tagging_with_lib, original_item_id, original_item_name, lib_id, lib_name)

            # 【关键拦截点】
            if lib_id not in allowed_libs:
                logger.trace(f"  ➜ Webhook: 项目 '{original_item_name}' 所属库 '{lib_name}' (ID: {lib_id}) 不在处理范围内，已跳过。")
                return jsonify({"status": "ignored_library"}), 200

    if event_type in ["item.add", "library.new"]:
        spawn(_wait_for_stream_data_and_enqueue, original_item_id, original_item_name, original_item_type)
        
        logger.info(f"  ➜ Webhook: 收到入库事件 '{original_item_name}'，已分派预检任务。")
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