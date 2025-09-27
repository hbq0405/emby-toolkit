# routes/webhook.py

import collections
import threading
import json
import re
from datetime import datetime, timezone
from flask import Blueprint, request, jsonify
from gevent import spawn_later

# 导入需要的模块
import task_manager

import emby_handler
import config_manager
import constants
import extensions
from core_processor import MediaProcessor
from tasks import (
    task_auto_sync_template_on_policy_change, 
    task_sync_metadata_cache,
    task_sync_assets,
    task_apply_main_cast_to_episodes
)
from custom_collection_handler import FilterEngine
from services.cover_generator import CoverGeneratorService
from database import collection_db, connection, settings_db, user_db
from database.log_db import LogDBManager
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

def _handle_full_processing_flow(processor: 'MediaProcessor', item_id: str, force_reprocess: bool):
    """
    【Webhook 专用】编排一个新入库媒体项的完整处理流程。
    包括：元数据处理 -> 自定义合集匹配 -> 封面生成。
    """
    if not processor:
        logger.error(f"完整处理流程中止：核心处理器 (MediaProcessor) 未初始化。")
        return

    item_details = emby_handler.get_emby_item_details(item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
    if not item_details:
        logger.error(f"  -> 无法获取项目 {item_id} 的详情，任务中止。")
        return
    
    item_name_for_log = item_details.get("Name", f"ID:{item_id}")

    processor.check_and_add_to_watchlist(item_details)

    processed_successfully = processor.process_single_item(item_id, force_reprocess_this_item=force_reprocess)
    
    if not processed_successfully:
        logger.warning(f"  -> 项目 '{item_name_for_log}' 的元数据处理未成功完成，跳过自定义合集匹配。")
        return

    try:
        tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
        item_name = item_details.get("Name", f"ID:{item_id}")
        if not tmdb_id:
            logger.debug("  -> 媒体项缺少TMDb ID，无法进行自定义合集匹配。")
            return

        item_metadata = collection_db.get_media_metadata_by_tmdb_id(tmdb_id)
        if not item_metadata:
            logger.warning(f"  -> 无法从本地缓存中找到TMDb ID为 {tmdb_id} 的元数据，无法匹配合集。")
            return

        # ▼▼▼ 步骤 1: 将获取媒体库信息的逻辑提前 ▼▼▼
        library_info = emby_handler.get_library_root_for_item(item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
        if not library_info:
            logger.warning(f"  -> 无法为项目 '{item_name_for_log}' 定位到其所属的媒体库根，将无法进行基于媒体库的合集匹配。")
            # 注意：这里我们只记录警告，不中止任务，因为可能还有不限制媒体库的合集需要匹配
            media_library_id = None
        else:
            media_library_id = library_info.get("Id")

        # --- 匹配 Filter (筛选) 类型的合集 ---
        engine = FilterEngine()
        
        # 【关键修改】在这里将获取到的 media_library_id 传递给 find_matching_collections
        matching_filter_collections = engine.find_matching_collections(item_metadata, media_library_id=media_library_id)

        if matching_filter_collections:
            logger.info(f"  -> 《{item_name}》匹配到 {len(matching_filter_collections)} 个筛选类合集，正在追加...")
            for collection in matching_filter_collections:
                # 步骤 1: 更新 Emby 实体合集
                emby_handler.append_item_to_collection(
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
                    new_item_emby_id=item_id
                )
        else:
            logger.info(f"  -> 《{item_name}》没有匹配到任何筛选类合集。")

        # --- 匹配 List (榜单) 类型的合集 ---
        updated_list_collections = collection_db.match_and_update_list_collections_on_item_add(
            new_item_tmdb_id=tmdb_id,
            new_item_emby_id=item_id,
            new_item_name=item_name
        )
        
        if updated_list_collections:
            logger.info(f"  -> 《{item_name}》匹配到 {len(updated_list_collections)} 个榜单类合集，正在追加...")
            for collection_info in updated_list_collections:
                emby_handler.append_item_to_collection(
                    collection_id=collection_info['emby_collection_id'],
                    item_emby_id=item_id,
                    base_url=processor.emby_url,
                    api_key=processor.emby_api_key,
                    user_id=processor.emby_user_id
                )
        else:
             logger.info(f"  -> 《{item_name}》没有匹配到任何需要更新状态的榜单类合集。")

    except Exception as e:
        logger.error(f"  -> 为新入库项目 '{item_name_for_log}' 匹配自定义合集时发生意外错误: {e}", exc_info=True)

    # --- 封面生成逻辑 ---
    try:
        cover_config = settings_db.get_setting('cover_generator_config') or {}

        if cover_config.get("enabled") and cover_config.get("transfer_monitor"):
            logger.info(f"  -> 检测到 '{item_details.get('Name')}' 入库，将为其所属媒体库生成新封面...")
            
            # ▼▼▼ 步骤 2: 复用已获取的 library_info，无需重复获取 ▼▼▼
            if not library_info:
                logger.warning(f"  -> (封面生成) 无法为项目 '{item_name_for_log}' 定位到其所属的媒体库根，跳过封面生成。")
                return

            library_id = library_info.get("Id") # library_id 变量在这里被重新赋值，但不影响上面的逻辑
            library_name = library_info.get("Name", library_id)
            
            if library_info.get('CollectionType') not in ['movies', 'tvshows', 'boxsets', 'mixed', 'music']:
                logger.debug(f"  -> 父级 '{library_name}' 不是一个常规媒体库，跳过封面生成。")
                return

            server_id = 'main_emby'
            library_unique_id = f"{server_id}-{library_id}"
            if library_unique_id in cover_config.get("exclude_libraries", []):
                logger.info(f"  -> 媒体库 '{library_name}' 在忽略列表中，跳过。")
                return
            
            TYPE_MAP = {'movies': 'Movie', 'tvshows': 'Series', 'music': 'MusicAlbum', 'boxsets': 'BoxSet', 'mixed': 'Movie,Series'}
            collection_type = library_info.get('CollectionType')
            item_type_to_query = TYPE_MAP.get(collection_type)
            
            item_count = 0
            if library_id and item_type_to_query:
                item_count = emby_handler.get_item_count(base_url=processor.emby_url, api_key=processor.emby_api_key, user_id=processor.emby_user_id, parent_id=library_id, item_type=item_type_to_query) or 0
            
            logger.info(f"  -> 正在为媒体库 '{library_name}' 生成封面 (当前实时数量: {item_count}) ---")
            cover_service = CoverGeneratorService(config=cover_config)
            cover_service.generate_for_library(emby_server_id=server_id, library=library_info, item_count=item_count)
        else:
            logger.debug("  -> 封面生成器或入库监控未启用，跳过封面生成。")

    except Exception as e:
        logger.error(f"  -> 在新入库后执行精准封面生成时发生错误: {e}", exc_info=True)

    logger.trace(f"  -> Webhook 任务及所有后续流程完成: '{item_name_for_log}'")

# --- 辅助函数 ---
def _process_batch_webhook_events():
    global WEBHOOK_BATCH_DEBOUNCER
    with WEBHOOK_BATCH_LOCK:
        items_in_batch = list(set(WEBHOOK_BATCH_QUEUE))
        WEBHOOK_BATCH_QUEUE.clear()
        WEBHOOK_BATCH_DEBOUNCER = None

    if not items_in_batch:
        return

    logger.info(f"  -> 防抖计时器到期，开始批量处理 {len(items_in_batch)} 个 Emby Webhook 新增/入库事件。")

    # ★★★ 核心修复：恢复 V5 版本的、能够记录具体分集ID的数据结构 ★★★
    parent_items = collections.defaultdict(lambda: {
        "name": "", "type": "", "episode_ids": set()
    })
    
    for item_id, item_name, item_type in items_in_batch:
        parent_id = item_id
        parent_name = item_name
        parent_type = item_type
        
        if item_type == "Episode":
            series_id = emby_handler.get_series_id_from_child_id(
                item_id, extensions.media_processor_instance.emby_url,
                extensions.media_processor_instance.emby_api_key, extensions.media_processor_instance.emby_user_id, item_name=item_name
            )
            if not series_id:
                logger.warning(f"  -> 批量处理中，分集 '{item_name}' 未找到所属剧集，跳过。")
                continue
            
            parent_id = series_id
            parent_type = "Series"
            
            # 将具体的分集ID添加到记录中
            parent_items[parent_id]["episode_ids"].add(item_id)
            
            # 更新父项的名字（只需一次）
            if not parent_items[parent_id]["name"]:
                series_details = emby_handler.get_emby_item_details(parent_id, extensions.media_processor_instance.emby_url, extensions.media_processor_instance.emby_api_key, extensions.media_processor_instance.emby_user_id, fields="Name")
                parent_items[parent_id]["name"] = series_details.get("Name", item_name) if series_details else item_name
        else:
            # 如果事件是电影或剧集容器本身，也记录下来
            parent_items[parent_id]["name"] = parent_name
        
        # 更新父项的类型
        parent_items[parent_id]["type"] = parent_type

    logger.info(f"  -> 批量事件去重后，将为 {len(parent_items)} 个独立媒体项分派任务。")

    for parent_id, item_info in parent_items.items():
        parent_name = item_info['name']
        parent_type = item_info['type']
        
        is_already_processed = parent_id in extensions.media_processor_instance.processed_items_cache

        if not is_already_processed:
            logger.info(f"  -> 为 '{parent_name}' 分派【完整处理】任务 (原因: 首次入库)。")
            task_manager.submit_task(
                _handle_full_processing_flow,
                task_name=f"Webhook完整处理: {parent_name}",
                item_id=parent_id,
                force_reprocess=True
            )
        else:
            # ★★★ 核心修复：恢复正确的追更处理逻辑 ★★★
            if parent_type == 'Series':
                episode_ids_to_update = list(item_info["episode_ids"])
                
                # 只有在确实有新分集入库时才执行任务
                if not episode_ids_to_update:
                    logger.info(f"  -> 剧集 '{parent_name}' 有更新事件，但未发现具体的新增分集，将触发一次轻量元数据缓存更新。")
                    task_manager.submit_task(
                        task_sync_metadata_cache,
                        task_name=f"Webhook元数据更新: {parent_name}",
                        processor_type='media',
                        item_id=parent_id,
                        item_name=parent_name
                    )
                    continue

                logger.info(f"  -> 为 '{parent_name}' 分派【轻量化更新】任务 (原因: 追更)，将处理 {len(episode_ids_to_update)} 个新分集。")
                task_manager.submit_task(
                    task_apply_main_cast_to_episodes,
                    task_name=f"轻量化同步演员表: {parent_name}",
                    processor_type='media',
                    series_id=parent_id,
                    episode_ids=episode_ids_to_update # <-- 现在传递的是具体的分集ID列表
                )
            else: # 电影等其他类型
                logger.info(f"  -> 媒体项 '{parent_name}' 已处理过，将触发一次轻量元数据缓存更新。")
                task_manager.submit_task(
                    task_sync_metadata_cache,
                    task_name=f"Webhook元数据更新: {parent_name}",
                    processor_type='media',
                    item_id=parent_id,
                    item_name=parent_name
                )

    logger.info("  -> 所有 Webhook 批量任务已成功分派。")

def _trigger_metadata_update_task(item_id, item_name):
    """触发元数据缓存同步任务"""
    logger.info(f"  -> 防抖计时器到期，为 '{item_name}' (ID: {item_id}) 执行元数据缓存同步任务。")
    task_manager.submit_task(
        task_sync_metadata_cache,
        task_name=f"元数据缓存同步: {item_name}",
        processor_type='media',
        item_id=item_id,
        item_name=item_name
    )

def _trigger_asset_update_task(item_id, item_name, update_description, sync_timestamp_iso):
    """触发覆盖缓存备份任务"""
    logger.info(f"  -> 防抖计时器到期，为 '{item_name}' (ID: {item_id}) 执行覆盖缓存备份任务。")
    task_manager.submit_task(
        task_sync_assets,
        task_name=f"覆盖缓存备份: {item_name}",
        processor_type='media',
        item_id=item_id,
        update_description=update_description,
        sync_timestamp_iso=sync_timestamp_iso
    )

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
    logger.info(f"  -> 收到Emby Webhook: {event_type}")

    USER_DATA_EVENTS = [
        "item.markfavorite", "item.unmarkfavorite",
        "item.markplayed", "item.markunplayed",
        "playback.start", "playback.pause", "playback.stop",
        "item.rate"
    ]

    if event_type == "user.policyupdated":
        updated_user = data.get("User", {})
        updated_user_id = updated_user.get("Id")
        
        if updated_user_id:
            logger.info(f"  -> 检测到用户 '{updated_user.get('Name')}' 的权限策略已更新，将分派后台任务以检查是否需要同步模板。")
            task_manager.submit_task(
                task_auto_sync_template_on_policy_change,
                task_name=f"自动同步权限 (源: {updated_user.get('Name')})",
                processor_type='media',
                updated_user_id=updated_user_id
            )
            return jsonify({"status": "auto_sync_task_submitted"}), 202
        else:
            return jsonify({"status": "event_ignored_no_user_id"}), 200

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
            series_id = emby_handler.get_series_id_from_child_id(
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
                    item_details_for_log = emby_handler.get_emby_item_details(
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
                logger.info(f"  -> Webhook: 已更新用户 '{user_name_for_log}' 对项目 '{item_name_for_log}' 的状态 ({event_type})。")
                return jsonify({"status": "user_data_updated"}), 200
            else:
                logger.debug(f"  -> Webhook '{event_type}' 未包含可更新的用户数据，已忽略。")
                return jsonify({"status": "event_ignored_no_updatable_data"}), 200
        except Exception as e:
            logger.error(f"  -> 通过 Webhook 更新用户媒体数据时失败: {e}", exc_info=True)
            return jsonify({"status": "error_updating_user_data"}), 500

    trigger_events = ["item.add", "library.new", "library.deleted", "metadata.update", "image.update"]
    if event_type not in trigger_events:
        logger.debug(f"  -> Webhook事件 '{event_type}' 不在触发列表 {trigger_events} 中，将被忽略。")
        return jsonify({"status": "event_ignored_not_in_trigger_list"}), 200

    item_from_webhook = data.get("Item", {}) if data else {}
    original_item_id = item_from_webhook.get("Id")
    original_item_name = item_from_webhook.get("Name", "未知项目")
    original_item_type = item_from_webhook.get("Type")
    
    trigger_types = ["Movie", "Series", "Episode"]
    if not (original_item_id and original_item_type in trigger_types):
        logger.debug(f"  -> Webhook事件 '{event_type}' (项目: {original_item_name}, 类型: {original_item_type}) 被忽略。")
        return jsonify({"status": "event_ignored_no_id_or_wrong_type"}), 200

    if event_type == "library.deleted":
        try:
            id_to_lookup_in_db = original_item_id
            
            # ▼▼▼ 核心修正：不再进行API调用，直接从 item_from_webhook 读取 ▼▼▼
            if original_item_type == "Episode":
                series_id_from_payload = item_from_webhook.get("SeriesId")
                if series_id_from_payload:
                    logger.info(f"Webhook: 分集 '{original_item_name}' (ID: {original_item_id}) 被删除。从Webhook负载中直接获取到其所属剧集ID: {series_id_from_payload}")
                    id_to_lookup_in_db = series_id_from_payload
                else:
                    # 这是一个异常情况，现代Emby版本通常总会提供SeriesId
                    logger.warning(f"Webhook: 分集 {original_item_id} 被删除，但在Webhook负载中未找到 SeriesId。无法可靠处理此事件，将仅清理日志。")
                    with connection.get_db_connection() as conn:
                        with conn.cursor() as cursor:
                            log_manager = LogDBManager()
                            log_manager.remove_from_processed_log(cursor, original_item_id)
                            log_manager.remove_from_failed_log(cursor, original_item_id)
                        conn.commit()
                    return jsonify({"status": "event_ignored_episode_no_seriesid_in_payload"}), 200
            
            # 后续逻辑与之前版本相同，但现在 id_to_lookup_in_db 始终是正确的剧集或电影ID
            with connection.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    log_manager = LogDBManager()
                    
                    cursor.execute(
                        "SELECT tmdb_id FROM media_metadata WHERE emby_item_id = %s",
                        (id_to_lookup_in_db,)
                    )
                    result = cursor.fetchone()
                    tmdb_id = result['tmdb_id'] if result else None

                    item_still_exists_in_emby = False
                    if tmdb_id:
                        current_item = emby_handler.find_emby_item_by_provider_id(
                            provider_name='Tmdb',
                            provider_id=tmdb_id,
                            base_url=extensions.media_processor_instance.emby_url,
                            api_key=extensions.media_processor_instance.emby_api_key,
                            user_id=extensions.media_processor_instance.emby_user_id
                        )
                        if current_item:
                            item_still_exists_in_emby = True
                            logger.info(f"Webhook: 检测到洗版操作。旧项目/分集 {original_item_id} 已删除，但新项目 (ID: {current_item.get('Id')}) 仍然存在于 Emby (TMDb ID: {tmdb_id})。")

                    if item_still_exists_in_emby:
                        logger.warning(f"Webhook: 将仅清理旧项目/分集 {original_item_id} 的处理日志，并保持其在库状态。")
                        log_manager.remove_from_processed_log(cursor, original_item_id)
                        log_manager.remove_from_failed_log(cursor, original_item_id)
                    else:
                        logger.warning(f"Webhook: 项目 {id_to_lookup_in_db} (TMDb ID: {tmdb_id or '未知'}) 已从 Emby 彻底删除。将执行完整数据清理。")
                        
                        log_manager.remove_from_processed_log(cursor, original_item_id)
                        log_manager.remove_from_failed_log(cursor, original_item_id)
                        logger.info(f"Webhook: 已从处理/失败日志中移除原始项目 {original_item_id}。")

                        cursor.execute(
                            "UPDATE media_metadata SET in_library = FALSE, emby_item_id = NULL WHERE emby_item_id = %s",
                            (id_to_lookup_in_db,)
                        )
                        if cursor.rowcount > 0:
                            logger.info(f"Webhook: 已在 media_metadata 缓存中将项目 {id_to_lookup_in_db} 标记为“不在库中”。")

                        cursor.execute("DELETE FROM watchlist WHERE item_id = %s", (id_to_lookup_in_db,))
                        if cursor.rowcount > 0:
                            logger.info(f"Webhook: 已从智能追剧列表中移除项目 {id_to_lookup_in_db}。")
                            
                        cursor.execute("DELETE FROM resubscribe_cache WHERE item_id = %s", (id_to_lookup_in_db,))
                        if cursor.rowcount > 0:
                            logger.info(f"Webhook: 已从媒体洗版缓存中移除项目 {id_to_lookup_in_db}。")

                conn.commit()
                
            return jsonify({"status": "delete_event_processed_intelligently", "item_id": original_item_id}), 200
        except Exception as e:
            logger.error(f"处理删除事件 for item {original_item_id} 时发生错误: {e}", exc_info=True)
            return jsonify({"status": "error_processing_remove_event", "error": str(e)}), 500
    
    if event_type in ["item.add", "library.new"]:
        description = data.get("Description", "")
        global WEBHOOK_BATCH_DEBOUNCER
        with WEBHOOK_BATCH_LOCK:
            WEBHOOK_BATCH_QUEUE.append((original_item_id, original_item_name, original_item_type))
            logger.debug(f"  -> Webhook事件 '{event_type}' (项目: {original_item_name}) 已添加到批量队列。当前队列大小: {len(WEBHOOK_BATCH_QUEUE)}")
            
            if WEBHOOK_BATCH_DEBOUNCER is None or WEBHOOK_BATCH_DEBOUNCER.ready():
                logger.info(f"  -> 启动 Webhook 批量处理 debouncer，将在 {WEBHOOK_BATCH_DEBOUNCE_TIME} 秒后执行。")
                WEBHOOK_BATCH_DEBOUNCER = spawn_later(WEBHOOK_BATCH_DEBOUNCE_TIME, _process_batch_webhook_events)
            else:
                logger.debug("  -> Webhook 批量处理 debouncer 正在运行中，事件已加入队列。")
        
        return jsonify({"status": "added_to_batch_queue", "item_id": original_item_id}), 202

    # --- 为 metadata.update 和 image.update 事件准备通用变量 ---
    id_to_process = original_item_id
    name_for_task = original_item_name
    
    if original_item_type == "Episode":
        series_id = emby_handler.get_series_id_from_child_id(
            original_item_id, extensions.media_processor_instance.emby_url,
            extensions.media_processor_instance.emby_api_key, extensions.media_processor_instance.emby_user_id, item_name=original_item_name
        )
        if not series_id:
            logger.warning(f"  -> Webhook '{event_type}': 剧集 '{original_item_name}' 未找到所属剧集，跳过。")
            return jsonify({"status": "event_ignored_episode_no_series_id"}), 200
        id_to_process = series_id
        
        full_series_details = emby_handler.get_emby_item_details(
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
                logger.debug(f"  -> 已为 '{name_for_task}' 取消了旧的同步计时器，将以最新的元数据更新事件为准。")

            logger.info(f"  -> 为 '{name_for_task}' 设置了 {UPDATE_DEBOUNCE_TIME} 秒的元数据同步延迟，以合并连续的更新事件。")
            new_timer = spawn_later(
                UPDATE_DEBOUNCE_TIME,
                _trigger_metadata_update_task,
                item_id=id_to_process,
                item_name=name_for_task
            )
            UPDATE_DEBOUNCE_TIMERS[id_to_process] = new_timer
        return jsonify({"status": "metadata_update_task_debounced", "item_id": id_to_process}), 202

    elif event_type == "image.update":
        update_description = data.get("UpdateInfo", {}).get("Description", "Webhook Image Update")
        webhook_received_at_iso = datetime.now(timezone.utc).isoformat()

        with UPDATE_DEBOUNCE_LOCK:
            # 注意：计时器键仍然使用 item_id，以便元数据和图像更新可以相互覆盖，通常以最后的操作为准
            if id_to_process in UPDATE_DEBOUNCE_TIMERS:
                old_timer = UPDATE_DEBOUNCE_TIMERS[id_to_process]
                old_timer.kill()
                logger.debug(f"  -> 已为 '{name_for_task}' 取消了旧的同步计时器，将以最新的封面更新事件为准。")

            logger.info(f"  -> 为 '{name_for_task}' 设置了 {UPDATE_DEBOUNCE_TIME} 秒的封面备份延迟，以合并连续的更新事件。")
            new_timer = spawn_later(
                UPDATE_DEBOUNCE_TIME,
                _trigger_asset_update_task,
                item_id=id_to_process,
                item_name=name_for_task,
                update_description=update_description,
                sync_timestamp_iso=webhook_received_at_iso
            )
            UPDATE_DEBOUNCE_TIMERS[id_to_process] = new_timer
        return jsonify({"status": "asset_update_task_debounced", "item_id": id_to_process}), 202

    return jsonify({"status": "event_unhandled"}), 500