# web_app.py
from gevent import monkey
monkey.patch_all()
import os
import sys
import shutil
import threading
from datetime import datetime, timezone # Added timezone for image.update
from jinja2 import Environment, FileSystemLoader
from actor_sync_handler import UnifiedSyncHandler
from db_handler import ActorDBManager
import emby_handler
import moviepilot_handler
import utils
from tasks import *
import extensions
from extensions import (
    login_required, 
    task_lock_required, 
    processor_ready_required
)
from utils import LogDBManager
from flask import Flask, render_template, request, redirect, url_for, jsonify, flash, stream_with_context, send_from_directory,Response, abort, session
from werkzeug.utils import safe_join, secure_filename
from utils import get_override_path_for_item
from watchlist_processor import WatchlistProcessor
from datetime import datetime
import requests
import tmdb_handler
import task_manager
from douban import DoubanApi
from tasks import get_task_registry 
from typing import Optional, Dict, Any, List, Tuple, Union # 确保 List 被导入
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz # 用于处理时区
import atexit # 用于应用退出处理
from core_processor import MediaProcessor
from actor_subscription_processor import ActorSubscriptionProcessor
from werkzeug.security import generate_password_hash, check_password_hash
from actor_utils import enrich_all_actor_aliases_task
import db_handler
from db_handler import get_db_connection as get_central_db_connection
from flask import session
from croniter import croniter
from scheduler_manager import scheduler_manager
from reverse_proxy import proxy_app
import logging
import collections # Added for deque
from gevent import spawn_later # Added for debouncing
# --- 导入蓝图 ---
from routes.watchlist import watchlist_bp
from routes.collections import collections_bp
from routes.custom_collections import custom_collections_bp
from routes.actor_subscriptions import actor_subscriptions_bp
from routes.logs import logs_bp
from routes.database_admin import db_admin_bp
from routes.system import system_bp
from routes.media import media_api_bp, media_proxy_bp
from routes.auth import auth_bp, init_auth as init_auth_from_blueprint
from routes.actions import actions_bp
from routes.cover_generator_config import cover_generator_config_bp
from routes.tasks import tasks_bp
from routes.resubscribe import resubscribe_bp
from routes.media_cleanup import media_cleanup_bp
# --- 核心模块导入 ---
import constants # 你的常量定义\
import logging
from logger_setup import frontend_log_queue, add_file_handler # 日志记录器和前端日志队列
import utils       # 例如，用于 /api/search_media
import config_manager

import task_manager
# --- 核心模块导入结束 ---
logger = logging.getLogger(__name__)
logging.getLogger("apscheduler.scheduler").setLevel(logging.WARNING)
logging.getLogger('werkzeug').setLevel(logging.ERROR)
app = Flask(__name__, static_folder='static')
app.secret_key = os.urandom(24)

#过滤底层日志
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("docker").setLevel(logging.WARNING)
logging.getLogger("PIL").setLevel(logging.WARNING)
logging.getLogger("geventwebsocket").setLevel(logging.WARNING)
# --- 全局变量 ---

JOB_ID_FULL_SCAN = "scheduled_full_scan"
JOB_ID_SYNC_PERSON_MAP = "scheduled_sync_person_map"
JOB_ID_PROCESS_WATCHLIST = "scheduled_process_watchlist"
JOB_ID_REVIVAL_CHECK = "scheduled_revival_check"

# Webhook 批量处理相关
WEBHOOK_BATCH_QUEUE = collections.deque()
WEBHOOK_BATCH_LOCK = threading.Lock()
WEBHOOK_BATCH_DEBOUNCE_TIME = 5 # 秒，在此时间内收集事件
WEBHOOK_BATCH_DEBOUNCER = None

# ★★★ 为 metadata/image update 事件增加防抖机制 ★★★
UPDATE_DEBOUNCE_TIMERS = {}
UPDATE_DEBOUNCE_LOCK = threading.Lock()
UPDATE_DEBOUNCE_TIME = 15 # 秒，等待事件风暴结束

# --- 数据库辅助函数 ---
def task_process_single_item(processor: MediaProcessor, item_id: str, force_reprocess: bool):
    """任务：处理单个媒体项"""
    processor.process_single_item(item_id, force_reprocess)

# --- 保存配置并重新加载的函数 ---
def save_config_and_reload(new_config: Dict[str, Any]):
    """
    【新版】调用配置管理器保存配置，并在此处执行所有必要的重新初始化操作。
    """
    try:
        # 步骤 1: 调用 config_manager 来保存文件和更新内存中的 config_manager.APP_CONFIG
        config_manager.save_config(new_config)
        
        # 步骤 2: 执行所有依赖于新配置的重新初始化逻辑
        initialize_processors()
        init_auth_from_blueprint()
        
        scheduler_manager.update_task_chain_job()
        
        logger.info("所有组件已根据新配置重新初始化完毕。")
        
    except Exception as e:
        logger.error(f"保存配置文件或重新初始化时失败: {e}", exc_info=True)
        # 向上抛出异常，让 API 端点可以捕获它并返回错误信息
        raise

# --- 初始化所有需要的处理器实例 ---
def initialize_processors():
    """初始化所有处理器，并将实例赋值给 extensions 模块中的全局变量。"""
    if not config_manager.APP_CONFIG:
        logger.error("无法初始化处理器：全局配置 APP_CONFIG 为空。")
        return

    current_config = config_manager.APP_CONFIG.copy()

    # --- 1. 创建实例并存储在局部变量中 ---
    
    # 初始化 server_id_local
    server_id_local = None
    emby_url = current_config.get("emby_server_url")
    emby_key = current_config.get("emby_api_key")
    if emby_url and emby_key:
        server_info = emby_handler.get_emby_server_info(emby_url, emby_key)
        if server_info and server_info.get("Id"):
            server_id_local = server_info.get("Id")
            logger.trace(f"成功获取到 Emby Server ID: {server_id_local}")
        else:
            logger.warning("未能获取到 Emby Server ID，跳转链接可能不完整。")

    # 初始化 media_processor_instance_local
    try:
        media_processor_instance_local = MediaProcessor(config=current_config)
        logger.info("核心处理器 实例已创建/更新。")
    except Exception as e:
        logger.error(f"创建 MediaProcessor 实例失败: {e}", exc_info=True)
        media_processor_instance_local = None

    # 初始化 watchlist_processor_instance_local
    try:
        watchlist_processor_instance_local = WatchlistProcessor(config=current_config)
        logger.trace("WatchlistProcessor 实例已成功初始化。")
    except Exception as e:
        logger.error(f"创建 WatchlistProcessor 实例失败: {e}", exc_info=True)
        watchlist_processor_instance_local = None

    # 初始化 actor_subscription_processor_instance_local
    try:
        actor_subscription_processor_instance_local = ActorSubscriptionProcessor(config=current_config)
        logger.trace("ActorSubscriptionProcessor 实例已成功初始化。")
    except Exception as e:
        logger.error(f"创建 ActorSubscriptionProcessor 实例失败: {e}", exc_info=True)
        actor_subscription_processor_instance_local = None


    # --- ✨✨✨ 简化为“单一赋值” ✨✨✨ ---
    # 直接赋值给 extensions 模块的全局变量
    extensions.media_processor_instance = media_processor_instance_local
    extensions.watchlist_processor_instance = watchlist_processor_instance_local
    extensions.actor_subscription_processor_instance = actor_subscription_processor_instance_local
    extensions.EMBY_SERVER_ID = server_id_local

# --- 生成Nginx配置 ---
def ensure_nginx_config():
    """
    【Jinja2 容器集成版】使用 Jinja2 模板引擎，生成供容器内 Nginx 使用的配置文件。
    """
    logger.info("正在生成 Nginx 配置文件...")
    
    # ★★★ 核心修改 1: 配置文件路径改为容器内 Nginx 的标准路径 ★★★
    final_config_path = '/etc/nginx/conf.d/default.conf'
    template_dir = os.path.join(os.getcwd(), 'templates', 'nginx')
    template_filename = 'emby_proxy.conf.template'

    try:
        # 1. 设置 Jinja2 环境
        env = Environment(loader=FileSystemLoader(template_dir))
        template = env.get_template(template_filename)

        # 2. 从 APP_CONFIG 获取值
        emby_url = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_SERVER_URL, "")
        nginx_listen_port = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_PROXY_PORT, 8097)
        redirect_url = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_PROXY_302_REDIRECT_URL, "")

        # 3. 准备替换值
        emby_upstream = emby_url.replace("http://", "").replace("https://", "").rstrip('/')
        # ★★★ 核心修改 2: Nginx 和 Python 代理在同一容器内，使用 localhost 通信 ★★★
        proxy_upstream = "127.0.0.1:7758" 
        redirect_upstream = redirect_url.replace("http://", "").replace("https://", "").rstrip('/')

        if not emby_upstream:
            logger.error("config.ini 中未配置 Emby 服务器地址，无法生成 Nginx 配置！")
            sys.exit(1) # 严重错误，直接退出

        # 4. 填充模板
        context = {
            'EMBY_UPSTREAM': emby_upstream,
            'PROXY_UPSTREAM': proxy_upstream,
            'NGINX_LISTEN_PORT': nginx_listen_port,
            'REDIRECT_UPSTREAM': redirect_upstream
        }
        final_config_content = template.render(context)

        # 5. 写入最终的配置文件
        with open(final_config_path, 'w', encoding='utf-8') as f:
            f.write(final_config_content)
        
        logger.info(f"✅ Nginx 配置文件已成功生成于: {final_config_path}")

    except Exception as e:
        logger.error(f"生成 Nginx 配置文件时发生严重错误: {e}", exc_info=True)
        sys.exit(1) # 严重错误，直接退出

# --- 检查字体文件 ---
def ensure_cover_generator_fonts():
    """
    启动时检查 cover_generator/fonts 目录下是否有指定字体文件，
    若缺少则从项目根目录的 fonts 目录拷贝过去。
    """
    cover_fonts_dir = os.path.join(config_manager.PERSISTENT_DATA_PATH, 'cover_generator', 'fonts')
    project_fonts_dir = os.path.join(os.getcwd(), 'fonts')  # 项目根目录fonts

    required_fonts = [
        "en_font.ttf",
        "en_font_multi_1.otf",
        "zh_font.ttf",
        "zh_font_multi_1.ttf",
    ]

    if not os.path.exists(cover_fonts_dir):
        os.makedirs(cover_fonts_dir, exist_ok=True)
        logger.trace(f"已创建字体目录：{cover_fonts_dir}")

    for font_name in required_fonts:
        dest_path = os.path.join(cover_fonts_dir, font_name)
        if not os.path.isfile(dest_path):
            src_path = os.path.join(project_fonts_dir, font_name)
            if os.path.isfile(src_path):
                try:
                    shutil.copy2(src_path, dest_path)
                    logger.trace(f"已拷贝缺失字体文件 {font_name} 到 {cover_fonts_dir}")
                except Exception as e:
                    logger.error(f"拷贝字体文件 {font_name} 失败: {e}", exc_info=True)
            else:
                logger.warning(f"项目根目录缺少字体文件 {font_name}，无法拷贝至 {cover_fonts_dir}")


# --- 应用退出处理 ---
def application_exit_handler():
    # global media_processor_instance, scheduler, task_worker_thread # 不再需要 scheduler
    global media_processor_instance, task_worker_thread # 修正后的
    logger.info("应用程序正在退出 (atexit)，执行清理操作...")

    # 1. 立刻通知当前正在运行的任务停止
    if extensions.media_processor_instance: # 从 extensions 获取
        logger.info("正在发送停止信号给当前任务...")
        extensions.media_processor_instance.signal_stop()

    task_manager.clear_task_queue()
    task_manager.stop_task_worker()

    # 4. 关闭其他资源
    if extensions.media_processor_instance: # 从 extensions 获取
        extensions.media_processor_instance.close()
    
    scheduler_manager.shutdown()
    
    logger.info("atexit 清理操作执行完毕。")
atexit.register(application_exit_handler)

# --- 反代监控 ---
@app.route('/api/health')
def health_check():
    """一个简单的健康检查端点，用于 Docker healthcheck。"""
    return jsonify({"status": "ok"}), 200

# --- webhook通知任务 ---
@app.route('/webhook/emby', methods=['POST'])
@extensions.processor_ready_required
def emby_webhook():
    data = request.json
    # ★★★ 真正的魔法咒语，就加在这一行！ ★★★
    logger.info(f"--- 魔法日志：收到的完整 Webhook Payload ---\n{json.dumps(data, indent=2, ensure_ascii=False)}")
    event_type = data.get("Event") if data else "未知事件"
    logger.info(f"收到Emby Webhook: {event_type}")

    # ★★★ 定义所有需要处理的用户数据事件 ★★★
    USER_DATA_EVENTS = [
        "item.markfavorite", "item.unmarkfavorite",
        "item.markplayed", "item.markunplayed",
        "playback.start", "playback.pause", "playback.stop",
        "item.rate"
    ]

    # --- 1. 优先处理用户数据事件 ---
    if event_type in USER_DATA_EVENTS:
        user_id = data.get("User", {}).get("Id")
        item_from_webhook = data.get("Item", {})
        item_id_from_webhook = item_from_webhook.get("Id")
        item_type_from_webhook = item_from_webhook.get("Type")

        if not user_id or not item_id_from_webhook:
            return jsonify({"status": "event_ignored_missing_data"}), 200

        id_to_update_in_db = None
        if item_type_from_webhook in ['Movie', 'Series']: # 剧集容器的收藏状态也需要记录
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
        
        # --- 分支 A：静态状态变更 ---
        if event_type in ["item.markfavorite", "item.unmarkfavorite", "item.markplayed", "item.markunplayed", "item.rate"]:
            user_data_from_item = item_from_webhook.get("UserData", {})
            if 'IsFavorite' in user_data_from_item:
                update_data['is_favorite'] = user_data_from_item['IsFavorite']
            if 'Played' in user_data_from_item:
                update_data['played'] = user_data_from_item['Played']
                if user_data_from_item['Played']:
                    update_data['playback_position_ticks'] = 0
                    # ★★★ 核心修复 1/2: 标记为已看时，也记录一下时间 ★★★
                    update_data['last_played_date'] = datetime.now(timezone.utc)

        # --- 分支 B：动态播放事件 ---
        elif event_type in ["playback.start", "playback.pause", "playback.stop"]:
            playback_info = data.get("PlaybackInfo", {})
            if playback_info:
                position_ticks = playback_info.get('PositionTicks')
                if position_ticks is not None:
                    update_data['playback_position_ticks'] = position_ticks
                
                # ★★★ 核心修复 2/2: 任何播放事件，都更新最后播放日期 ★★★
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
                logger.debug(f"Webhook '{event_type}' 未包含可更新的用户数据，已忽略。")
                return jsonify({"status": "event_ignored_no_updatable_data"}), 200
        except Exception as e:
            logger.error(f"通过 Webhook 更新用户媒体数据时失败: {e}", exc_info=True)
            return jsonify({"status": "error_updating_user_data"}), 500

    # --- 批量处理函数：处理队列中的所有新增/入库事件 (此函数不变) ---
    def _process_batch_webhook_events():
        # ... (这个函数的内部逻辑保持原样)
        global WEBHOOK_BATCH_DEBOUNCER
        with WEBHOOK_BATCH_LOCK:
            items_to_process = list(set(WEBHOOK_BATCH_QUEUE)) # 去重
            WEBHOOK_BATCH_QUEUE.clear()
            WEBHOOK_BATCH_DEBOUNCER = None # 重置 debouncer

        if not items_to_process:
            logger.debug("批量处理队列为空，无需处理。")
            return

        logger.info(f"  -> 开始批量处理 {len(items_to_process)} 个 Emby Webhook 新增/入库事件。")
        for item_id, item_name, item_type in items_to_process:
            logger.info(f"  -> 批量处理中: '{item_name}'")
            try:
                id_to_process = item_id
                if item_type == "Episode":
                    series_id = emby_handler.get_series_id_from_child_id(
                        item_id, extensions.media_processor_instance.emby_url,
                        extensions.media_processor_instance.emby_api_key, extensions.media_processor_instance.emby_user_id, item_name=item_name
                    )
                    if not series_id:
                        logger.warning(f"  -> 批量处理中，剧集 '{item_name}' 未找到所属剧集，跳过。")
                        continue
                    id_to_process = series_id
                
                full_item_details = emby_handler.get_emby_item_details(
                    item_id=id_to_process, emby_server_url=extensions.media_processor_instance.emby_url,
                    emby_api_key=extensions.media_processor_instance.emby_api_key, user_id=extensions.media_processor_instance.emby_user_id
                )
                if not full_item_details:
                    logger.warning(f"  -> 批量处理中，无法获取 '{item_name}' 的详情，跳过。")
                    continue
                
                final_item_name = full_item_details.get("Name", f"未知(ID:{id_to_process})")
                if not full_item_details.get("ProviderIds", {}).get("Tmdb"):
                    logger.warning(f"  -> 批量处理中，'{final_item_name}' 缺少 Tmdb ID，跳过。")
                    continue
                
                task_manager.submit_task(
                    webhook_processing_task,
                    task_name=f"Webhook任务: {final_item_name}",
                    processor_type='media',
                    item_id=id_to_process,
                    force_reprocess=True
                )
                logger.info(f"  -> 已将 '{final_item_name}' 添加到任务队列进行处理。")

            except Exception as e:
                logger.error(f"  -> 批量处理 '{item_name}' 时发生错误: {e}", exc_info=True)
        logger.info("  -> 批量处理 Webhook任务 已添加到后台任务队列。")

    # ★★★ 核心新增：这是防抖计时器到期后，真正执行任务的函数 ★★★
    def _trigger_update_tasks(item_id, item_name, update_description, sync_timestamp_iso):
        """
        在防抖延迟结束后，将元数据和资源同步任务提交到队列。
        """
        logger.info(f"防抖计时器到期，为 '{item_name}' (ID: {item_id}) 创建最终的同步任务。")
        
        # 任务1: 同步元数据到数据库缓存
        task_manager.submit_task(
            task_sync_metadata_cache,
            task_name=f"元数据缓存同步: {item_name}",
            processor_type='media',
            item_id=item_id,
            item_name=item_name
        )

        # 任务2: 同步媒体项到覆盖缓存
        task_manager.submit_task(
            task_sync_assets,
            task_name=f"覆盖缓存备份: {item_name}",
            processor_type='media',
            item_id=item_id,
            update_description=update_description,
            sync_timestamp_iso=sync_timestamp_iso
        )

    # --- Webhook 事件分发逻辑 ---
    trigger_events = ["item.add", "library.new", "library.deleted", "metadata.update", "image.update"]
    if event_type not in trigger_events:
        logger.info(f"Webhook事件 '{event_type}' 不在触发列表 {trigger_events} 中，将被忽略。")
        return jsonify({"status": "event_ignored_not_in_trigger_list"}), 200

    item_from_webhook = data.get("Item", {}) if data else {}
    original_item_id = item_from_webhook.get("Id")
    original_item_name = item_from_webhook.get("Name", "未知项目")
    original_item_type = item_from_webhook.get("Type")
    
    trigger_types = ["Movie", "Series", "Episode"]
    if not (original_item_id and original_item_type in trigger_types):
        logger.debug(f"Webhook事件 '{event_type}' (项目: {original_item_name}, 类型: {original_item_type}) 被忽略。")
        return jsonify({"status": "event_ignored_no_id_or_wrong_type"}), 200

    # --- 处理删除事件 (逻辑不变) ---
    if event_type == "library.deleted":
        try:
            # 1. 从 webhook 负载中提取 TMDb ID
            provider_ids = item_from_webhook.get("ProviderIds", {})
            tmdb_id_to_delete = provider_ids.get("Tmdb")

            # 2. 在同一个事务中执行所有数据库删除操作
            with get_central_db_connection() as conn:
                with conn.cursor() as cursor:
                    # 2a. 从 processed_log 中删除
                    log_manager = LogDBManager()
                    log_manager.remove_from_processed_log(cursor, original_item_id)
                    logger.info(f"Webhook: 已从 processed_log 中移除项目 {original_item_id}。")

                    # 2b. 如果获取到了 TMDb ID，则从 media_metadata 中删除
                    if tmdb_id_to_delete:
                        cursor.execute("DELETE FROM media_metadata WHERE tmdb_id = %s", (tmdb_id_to_delete,))
                        if cursor.rowcount > 0:
                            logger.info(f"Webhook: 已从 media_metadata 缓存中移除 TMDb ID 为 {tmdb_id_to_delete} 的媒体项。")
                        else:
                            logger.debug(f"Webhook: 在 media_metadata 中未找到 TMDb ID {tmdb_id_to_delete}，无需删除。")
                    else:
                        logger.warning(f"Webhook: 无法从 media_metadata 中删除项目 {original_item_id}，因为在 webhook 负载中未找到 TMDb ID。")
                
                # 3. 提交事务
                conn.commit()
                
            return jsonify({"status": "processed_log_and_metadata_entry_removed", "item_id": original_item_id}), 200
        except Exception as e:
            # 发生异常时，数据库连接的上下文管理器会自动回滚事务
            logger.error(f"处理删除事件 for item {original_item_id} 时发生错误: {e}", exc_info=True)
            return jsonify({"status": "error_processing_remove_event", "error": str(e)}), 500
    
    # --- 处理新增/入库事件 (使用批量处理, 逻辑不变) ---
    if event_type in ["item.add", "library.new"]:
        global WEBHOOK_BATCH_DEBOUNCER
        with WEBHOOK_BATCH_LOCK:
            WEBHOOK_BATCH_QUEUE.append((original_item_id, original_item_name, original_item_type))
            logger.debug(f"Webhook事件 '{event_type}' (项目: {original_item_name}) 已添加到批量队列。当前队列大小: {len(WEBHOOK_BATCH_QUEUE)}")
            
            if WEBHOOK_BATCH_DEBOUNCER is None or WEBHOOK_BATCH_DEBOUNCER.ready():
                logger.info(f"启动 Webhook 批量处理 debouncer，将在 {WEBHOOK_BATCH_DEBOUNCE_TIME} 秒后执行。")
                WEBHOOK_BATCH_DEBOUNCER = spawn_later(WEBHOOK_BATCH_DEBOUNCE_TIME, _process_batch_webhook_events)
            else:
                logger.debug("Webhook 批量处理 debouncer 正在运行中，事件已加入队列。")
        
        return jsonify({"status": "added_to_batch_queue", "item_id": original_item_id}), 202

    # ★★★ 核心修改：将 metadata.update 和 image.update 纳入防抖机制 ★★★
    if event_type in ["metadata.update", "image.update"]:
        if not config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_LOCAL_DATA_PATH):
            logger.debug(f"Webhook '{event_type}' 收到，但未配置本地数据源，将忽略。")
            return jsonify({"status": "event_ignored_no_local_data_path"}), 200

        # 准备通用参数
        update_description = data.get("UpdateInfo", {}).get("Description", "Webhook Update")
        webhook_received_at_iso = datetime.now(timezone.utc).isoformat()

        # 向上追溯到剧集/电影的ID
        id_to_process = original_item_id
        name_for_task = original_item_name
        
        if original_item_type == "Episode":
            series_id = emby_handler.get_series_id_from_child_id(
                original_item_id, extensions.media_processor_instance.emby_url,
                extensions.media_processor_instance.emby_api_key, extensions.media_processor_instance.emby_user_id, item_name=original_item_name
            )
            if not series_id:
                logger.warning(f"Webhook '{event_type}': 剧集 '{original_item_name}' 未找到所属剧集，跳过。")
                return jsonify({"status": "event_ignored_episode_no_series_id"}), 200
            id_to_process = series_id
            
            full_series_details = emby_handler.get_emby_item_details(
                item_id=id_to_process, emby_server_url=extensions.media_processor_instance.emby_url,
                emby_api_key=extensions.media_processor_instance.emby_api_key, user_id=extensions.media_processor_instance.emby_user_id
            )
            if full_series_details:
                name_for_task = full_series_details.get("Name", f"未知剧集(ID:{id_to_process})")

        # --- 防抖逻辑核心 ---
        with UPDATE_DEBOUNCE_LOCK:
            # 检查是否已有正在等待的计时器
            if id_to_process in UPDATE_DEBOUNCE_TIMERS:
                # 如果有，取消它
                old_timer = UPDATE_DEBOUNCE_TIMERS[id_to_process]
                old_timer.kill() # gevent 使用 kill() 来取消
                logger.debug(f"已为 '{name_for_task}' 取消了旧的同步计时器，将以最新事件为准。")

            # 创建一个新的计时器，延迟执行真正的任务提交函数
            logger.info(f"为 '{name_for_task}' 设置了 {UPDATE_DEBOUNCE_TIME} 秒的同步延迟，以合并连续的更新事件。")
            new_timer = spawn_later(
                UPDATE_DEBOUNCE_TIME,
                _trigger_update_tasks,
                item_id=id_to_process,
                item_name=name_for_task,
                update_description=update_description,
                sync_timestamp_iso=webhook_received_at_iso
            )
            # 存储新的计时器
            UPDATE_DEBOUNCE_TIMERS[id_to_process] = new_timer

        return jsonify({"status": "update_task_debounced", "item_id": id_to_process}), 202

    return jsonify({"status": "event_unhandled"}), 500

# --- 兜底路由，必须放最后 ---
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve(path):
    static_folder_path = app.static_folder 

    if path != "" and os.path.exists(os.path.join(static_folder_path, path)):
        return send_from_directory(static_folder_path, path)
    else:
        return send_from_directory(static_folder_path, 'index.html')
    
# +++ 在应用对象上注册所有蓝图 +++
app.register_blueprint(watchlist_bp)
app.register_blueprint(collections_bp)
app.register_blueprint(custom_collections_bp)
app.register_blueprint(actor_subscriptions_bp)
app.register_blueprint(logs_bp)
app.register_blueprint(db_admin_bp)
app.register_blueprint(system_bp)
app.register_blueprint(media_api_bp) 
app.register_blueprint(media_proxy_bp)
app.register_blueprint(auth_bp)
app.register_blueprint(actions_bp)
app.register_blueprint(cover_generator_config_bp)
app.register_blueprint(tasks_bp)
app.register_blueprint(resubscribe_bp)
app.register_blueprint(media_cleanup_bp)

def main_app_start():
    """将主应用启动逻辑封装成一个函数"""
    from gevent.pywsgi import WSGIServer
    from geventwebsocket.handler import WebSocketHandler
    import gevent

    logger.info(f"应用程序启动... 版本: {constants.APP_VERSION}")
    
    config_manager.load_config()
    
    config_manager.LOG_DIRECTORY = os.path.join(config_manager.PERSISTENT_DATA_PATH, 'logs')
    try:
        log_size = int(config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_LOG_ROTATION_SIZE_MB, constants.DEFAULT_LOG_ROTATION_SIZE_MB))
        log_backups = int(config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_LOG_ROTATION_BACKUPS, constants.DEFAULT_LOG_ROTATION_BACKUPS))
    except (ValueError, TypeError):
        log_size = constants.DEFAULT_LOG_ROTATION_SIZE_MB
        log_backups = constants.DEFAULT_LOG_ROTATION_BACKUPS
    add_file_handler(log_directory=config_manager.LOG_DIRECTORY, log_size_mb=log_size, log_backups=log_backups)
    
    db_handler.init_db()

    ensure_cover_generator_fonts()
    init_auth_from_blueprint()
    initialize_processors()
    task_manager.start_task_worker_if_not_running()
    scheduler_manager.start()
    
    def run_proxy_server():
        if config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_PROXY_ENABLED):
            try:
                internal_proxy_port = 7758
                logger.trace(f"🚀 [GEVENT] 反向代理服务即将启动，监听内部端口: {internal_proxy_port}")
                proxy_server = WSGIServer(('0.0.0.0', internal_proxy_port), proxy_app, handler_class=WebSocketHandler)
                proxy_server.serve_forever()
            except Exception as e:
                logger.error(f"启动反向代理服务失败: {e}", exc_info=True)
        else:
            logger.info("反向代理功能未在配置中启用。")

    gevent.spawn(run_proxy_server)

    main_app_port = int(constants.WEB_APP_PORT)
    logger.info(f"🚀 [GEVENT] 主应用服务器即将启动，监听端口: {main_app_port}")
    
    class NullLogger:
        def write(self, data): pass
        def flush(self): pass

    main_server = WSGIServer(('0.0.0.0', main_app_port), app, log=NullLogger())
    main_server.serve_forever()

# ★★★ 核心修改 2: 新增的启动逻辑，用于处理命令行参数 ★★★
if __name__ == '__main__':
    # 检查是否从 entrypoint.sh 传入了 'generate-nginx-config' 参数
    if len(sys.argv) > 1 and sys.argv[1] == 'generate-nginx-config':
        print("Initializing to generate Nginx config...")
        # 只需要加载配置和日志，然后生成即可
        config_manager.load_config()
        # 确保日志目录存在，避免报错
        log_dir = os.path.join(config_manager.PERSISTENT_DATA_PATH, 'logs')
        os.makedirs(log_dir, exist_ok=True)
        add_file_handler(log_directory=log_dir)
        
        ensure_nginx_config()
        print("Nginx config generated successfully.")
        sys.exit(0) # 执行完毕后正常退出
    else:
        # 如果没有特殊参数，则正常启动整个应用
        main_app_start()

# # --- 主程序入口结束 ---
