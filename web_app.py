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
from flask import Flask, render_template, request, redirect, url_for, jsonify, flash, stream_with_context, send_from_directory,Response, abort, session
from werkzeug.utils import safe_join, secure_filename
from utils import get_override_path_for_item, translate_country_list, get_unified_rating
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
from routes.user_management import user_management_bp
from routes.webhook import webhook_bp
# --- 核心模块导入 ---
import constants # 你的常量定义\
import logging
from logger_setup import frontend_log_queue, add_file_handler # 日志记录器和前端日志队列
import utils       # 例如，用于 /api/search_media
import config_manager
from database import connection

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
        
        scheduler_manager.update_all_scheduled_jobs()
        
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
app.register_blueprint(user_management_bp)
app.register_blueprint(webhook_bp)

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
    
    connection.init_db()

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