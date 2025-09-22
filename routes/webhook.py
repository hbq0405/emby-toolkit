# routes/webhook.py

import collections
import threading
import json
from datetime import datetime, timezone
from flask import Blueprint, request, jsonify
from gevent import spawn_later

# 导入需要的模块
import task_manager
import db_handler
import emby_handler
import config_manager
import constants
import extensions
from tasks import (
    task_auto_sync_template_on_policy_change, 
    webhook_processing_task,
    task_sync_metadata_cache,
    task_sync_assets,
    task_apply_main_cast_to_episodes
)
from db_handler import LogDBManager, get_db_connection as get_central_db_connection


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
                webhook_processing_task,
                task_name=f"Webhook完整处理: {parent_name}",
                processor_type='media',
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

def _trigger_update_tasks(item_id, item_name, update_description, sync_timestamp_iso):
    logger.info(f"  -> 防抖计时器到期，为 '{item_name}' (ID: {item_id}) 创建最终的同步任务。")
    
    task_manager.submit_task(
        task_sync_metadata_cache,
        task_name=f"元数据缓存同步: {item_name}",
        processor_type='media',
        item_id=item_id,
        item_name=item_name
    )

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
        user_id = data.get("User", {}).get("Id")
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
                db_handler.upsert_user_media_data(update_data)
                logger.info(f"  -> Webhook: 已更新用户 '{user_id}' 对项目 '{id_to_update_in_db}' 的状态 ({event_type})。")
                return jsonify({"status": "user_data_updated"}), 200
            else:
                logger.debug(f"  -> Webhook '{event_type}' 未包含可更新的用户数据，已忽略。")
                return jsonify({"status": "event_ignored_no_updatable_data"}), 200
        except Exception as e:
            logger.error(f"  -> 通过 Webhook 更新用户媒体数据时失败: {e}", exc_info=True)
            return jsonify({"status": "error_updating_user_data"}), 500

    trigger_events = ["item.add", "library.new", "library.deleted", "metadata.update", "image.update"]
    if event_type not in trigger_events:
        logger.info(f"  -> Webhook事件 '{event_type}' 不在触发列表 {trigger_events} 中，将被忽略。")
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
            with get_central_db_connection() as conn:
                with conn.cursor() as cursor:
                    log_manager = LogDBManager()
                    log_manager.remove_from_processed_log(cursor, original_item_id)
                    logger.info(f"  -> Webhook: 已从 processed_log 中移除项目 {original_item_id}。")

                    cursor.execute("DELETE FROM media_metadata WHERE emby_item_id = %s", (original_item_id,))
                    
                    if cursor.rowcount > 0:
                        logger.info(f"  -> Webhook: 已从 media_metadata 缓存中移除 Emby ID 为 {original_item_id} 的媒体项。")
                    else:
                        logger.debug(f"  -> Webhook: 在 media_metadata 中未找到 Emby ID {original_item_id}，无需删除。")
                
                conn.commit()
                
            return jsonify({"status": "processed_log_and_metadata_entry_removed", "item_id": original_item_id}), 200
        except Exception as e:
            logger.error(f"  -> 处理删除事件 for item {original_item_id} 时发生错误: {e}", exc_info=True)
            return jsonify({"status": "error_processing_remove_event", "error": str(e)}), 500
    
    if event_type in ["item.add", "library.new"]:
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

    if event_type in ["metadata.update", "image.update"]:
        if not config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_LOCAL_DATA_PATH):
            logger.debug(f"  -> Webhook '{event_type}' 收到，但未配置本地数据源，将忽略。")
            return jsonify({"status": "event_ignored_no_local_data_path"}), 200

        update_description = data.get("UpdateInfo", {}).get("Description", "Webhook Update")
        webhook_received_at_iso = datetime.now(timezone.utc).isoformat()

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

        with UPDATE_DEBOUNCE_LOCK:
            if id_to_process in UPDATE_DEBOUNCE_TIMERS:
                old_timer = UPDATE_DEBOUNCE_TIMERS[id_to_process]
                old_timer.kill()
                logger.debug(f"已为 '{name_for_task}' 取消了旧的同步计时器，将以最新事件为准。")

            logger.info(f"为 '{name_for_task}' 设置了 {UPDATE_DEBOUNCE_TIME} 秒的同步延迟，以合并连续的更新事件。")
            new_timer = spawn_later(
                UPDATE_DEBOUNCE_TIME,
                _trigger_update_tasks,
                item_id=id_to_process,
                item_name=name_for_task,
                update_description=update_description,
                sync_timestamp_iso=webhook_received_at_iso
            )
            UPDATE_DEBOUNCE_TIMERS[id_to_process] = new_timer

        return jsonify({"status": "update_task_debounced", "item_id": id_to_process}), 202

    return jsonify({"status": "event_unhandled"}), 500