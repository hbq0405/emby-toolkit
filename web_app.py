# web_app.py
from gevent import monkey
monkey.patch_all()
import os
import sys
import shutil
from jinja2 import Environment, FileSystemLoader
from handler.actor_sync import UnifiedSyncHandler
import extensions
from flask import Flask, render_template, request, redirect, url_for, jsonify, flash, stream_with_context, send_from_directory,Response, abort, session
from werkzeug.utils import safe_join, secure_filename
from watchlist_processor import WatchlistProcessor
from datetime import datetime
from handler.emby import get_emby_server_info 
import task_manager
from tasks.core import get_task_registry 
from typing import Dict, Any
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import atexit # 用于应用退出处理
from ai_translator import AITranslator
from core_processor import MediaProcessor
from actor_subscription_processor import ActorSubscriptionProcessor
from werkzeug.security import generate_password_hash, check_password_hash
from actor_utils import enrich_all_actor_aliases_task
from handler.custom_collection import RecommendationEngine
from flask import session
from croniter import croniter
from scheduler_manager import scheduler_manager
from reverse_proxy import proxy_app
from handler import telegram
import logging
from gevent import spawn_later # Added for debouncing
# --- 导入蓝图 ---
from routes.watchlist import watchlist_bp
from routes.tmdb_collections import collections_bp
from routes.custom_collections import custom_collections_bp
from routes.actor_subscriptions import actor_subscriptions_bp
from routes.logs import logs_bp
from routes.database_admin import db_admin_bp
from routes.system import system_bp
from routes.media import media_api_bp, media_proxy_bp
from routes.actions import actions_bp
from routes.cover_generator_config import cover_generator_config_bp
from routes.tasks import tasks_bp
from routes.resubscribe import resubscribe_bp
from routes.media_cleanup import media_cleanup_bp
from routes.user_management import user_management_bp
from routes.webhook import webhook_bp
from routes.unified_auth import unified_auth_bp
from routes.user_portal import user_portal_bp
from routes.discover import discover_bp
from routes.p115 import p115_bp
from routes.hdhive import hdhive_bp
# --- 核心模块导入 ---
import constants # 你的常量定义\
import logging
from logger_setup import frontend_log_queue, add_file_handler # 日志记录器和前端日志队列
import config_manager
from database import connection, settings_db

import task_manager
# ★★★ 新增：导入监控服务 ★★★
from monitor_service import MonitorService 
# 导入 DoubanApi
try:
    from handler.douban import DoubanApi
    DOUBAN_API_AVAILABLE = True
except ImportError:
    DOUBAN_API_AVAILABLE = False
    class DoubanApi:
        def __init__(self, *args, **kwargs): pass
        def close(self): pass
# --- 核心模块导入结束 ---
logger = logging.getLogger(__name__)
logging.getLogger("apscheduler.scheduler").setLevel(logging.WARNING)
logging.getLogger('werkzeug').setLevel(logging.ERROR)
app = Flask(__name__, static_folder='static')
# --- 优化 Session 密钥持久化 ---
secret_file_path = os.path.join(config_manager.PERSISTENT_DATA_PATH, '.flask_secret')
if os.path.exists(secret_file_path):
    with open(secret_file_path, 'rb') as f:
        app.secret_key = f.read()
else:
    secret_key = os.urandom(24)
    app.secret_key = secret_key
    try:
        with open(secret_file_path, 'wb') as f:
            f.write(secret_key)
    except Exception as e:
        logger.warning(f"无法保存 Session 密钥，重启后用户需重新登录: {e}")

#过滤底层日志
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)
logging.getLogger("docker").setLevel(logging.WARNING)
logging.getLogger("PIL").setLevel(logging.WARNING)
logging.getLogger("geventwebsocket").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("watchdog").setLevel(logging.WARNING)
# --- 全局变量 ---

JOB_ID_FULL_SCAN = "scheduled_full_scan"
JOB_ID_SYNC_PERSON_MAP = "scheduled_sync_person_map"
JOB_ID_PROCESS_WATCHLIST = "scheduled_process_watchlist"
JOB_ID_REVIVAL_CHECK = "scheduled_revival_check"

# ★★★ 新增：全局监控服务实例 ★★★
monitor_service_instance = None

# --- 保存配置并重新加载的函数 ---
def save_config_and_reload(new_config: Dict[str, Any]):
    """
    【新版】调用配置管理器保存配置，并在此处执行所有必要的重新初始化操作。
    """
    global monitor_service_instance
    try:
        # 步骤 1: 调用 config_manager 来保存文件和更新内存中的 config_manager.APP_CONFIG
        config_manager.save_config(new_config)
        
        # 步骤 2: 执行所有依赖于新配置的重新初始化逻辑
        initialize_processors()
        
        scheduler_manager.update_all_scheduled_jobs()
        
        # ★★★ 新增：重启监控服务以应用新配置 ★★★
        if monitor_service_instance:
            monitor_service_instance.stop()
        
        if extensions.media_processor_instance:
            monitor_service_instance = MonitorService(config_manager.APP_CONFIG, extensions.media_processor_instance)
            monitor_service_instance.start()
        
        logger.info("  ✅ 新配置重新初始化完毕。")
        
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

    # --- 初始化共享的 AI 实例 ---
    shared_ai_translator = None
    
    # 检查是否开启了任意 AI 功能
    ai_enabled = any([
        current_config.get(constants.CONFIG_OPTION_AI_TRANSLATE_ACTOR_ROLE, False),
        current_config.get(constants.CONFIG_OPTION_AI_TRANSLATE_TITLE, False),    
        current_config.get(constants.CONFIG_OPTION_AI_TRANSLATE_OVERVIEW, False), 
        current_config.get(constants.CONFIG_OPTION_AI_TRANSLATE_EPISODE_OVERVIEW, False),
        current_config.get(constants.CONFIG_OPTION_AI_VECTOR, False),
    ])

    if ai_enabled:
        try:
            shared_ai_translator = AITranslator(current_config)
            logger.debug("  ✅ AI增强服务实例已初始化。")
        except Exception as e:
            logger.error(f"  ❌ AITranslator 初始化失败: {e}")

    # --- 初始化共享的 Douban 实例 ---
    shared_douban_api = None
    if getattr(constants, 'DOUBAN_API_AVAILABLE', False):
        try:
            # 从配置中获取参数
            douban_cooldown = current_config.get(constants.CONFIG_OPTION_DOUBAN_DEFAULT_COOLDOWN, 2.0)
            douban_cookie = current_config.get(constants.CONFIG_OPTION_DOUBAN_COOKIE, "")
            
            if not douban_cookie:
                logger.debug(f"配置文件中未找到 '{constants.CONFIG_OPTION_DOUBAN_COOKIE}'。豆瓣功能可能受限。")
            
            shared_douban_api = DoubanApi(
                cooldown_seconds=douban_cooldown,
                user_cookie=douban_cookie
            )
            logger.debug("  ✅ DoubanApi 共享实例已初始化。")
        except Exception as e:
            logger.error(f"DoubanApi 初始化失败: {e}", exc_info=True)
    
    # 初始化 server_id_local
    server_id_local = None
    emby_url = current_config.get("emby_server_url")
    emby_key = current_config.get("emby_api_key")
    
    if emby_url and emby_key:
        # --- 优化启动逻辑：优先检查缓存，决定超时策略 ---
        cached_id = settings_db.get_setting("emby_server_id_cache")
        
        # 如果有缓存，我们只给 5 秒钟尝试连接 Emby (快速失败策略)
        # 如果没缓存，我们给 20 秒 (必须获取策略)
        startup_timeout = 5 if cached_id else 20
        
        logger.debug(f"  ➜ 正在尝试连接 Emby 获取 Server ID (超时设定: {startup_timeout}s)...")
        
        # 尝试获取在线信息
        server_info = get_emby_server_info(emby_url, emby_key, timeout=startup_timeout)
        
        if server_info and server_info.get("Id"):
            server_id_local = server_info.get("Id")
            logger.trace(f"成功获取到 Emby Server ID: {server_id_local}")
            # --- 缓存 Server ID ---
            try:
                settings_db.save_setting("emby_server_id_cache", server_id_local)
            except Exception as e:
                logger.warning(f"缓存 Emby Server ID 失败: {e}")
        else:
            # --- 网络获取失败，回退到缓存 ---
            if cached_id:
                server_id_local = cached_id
                logger.warning(f"⚠️ 无法连接 Emby 服务器 (或超时)，已使用缓存的 Server ID: {server_id_local} 继续启动。")
            else:
                logger.error("❌ 无法连接 Emby 且本地无缓存 Server ID，部分功能可能受限。")

        # =========================================================
        # ★★★ Pro 版本在线验证逻辑 ★★★
        # =========================================================
        # 默认设为普通版
        config_manager.APP_CONFIG['is_pro_active'] = False 
        
        # 从数据库读取用户填写的卡密
        license_key = settings_db.get_setting("pro_license_key")
        
        if license_key:
            logger.info("  ➜ 检测到 Pro 激活码，正在连接云端验证...")
            try:
                import requests
                # 替换为你自己的 CF Worker 域名
                verify_url = "https://auth.55565576.xyz/" 
                payload = {
                    "license_key": license_key.strip(),
                    "server_id": server_id_local
                }
                # 设置 5 秒超时，防止 CF 抽风导致启动卡死
                resp = requests.post(verify_url, json=payload, timeout=5).json()
                
                if resp.get("success") and resp.get("is_pro"):
                    config_manager.APP_CONFIG['is_pro_active'] = True
                    logger.info("  💎 Pro 高级版验证通过！已解锁极速 302 直链等高级功能。")
                else:
                    logger.warning(f"  ⚠️ Pro 验证失败: {resp.get('msg')}。已降级为免费基础版。")
            except Exception as e:
                logger.error(f"  ❌ Pro 验证服务器连接失败: {e}。已降级为免费基础版。")
        else:
            logger.info("  ➜ 当前运行版本: 免费基础版 (支持升级 Pro 解锁 0 带宽消耗直链)")

    # 初始化 media_processor_instance_local
    try:
        media_processor_instance_local = MediaProcessor(
            config=current_config, 
            ai_translator=shared_ai_translator,
            douban_api=shared_douban_api 
        )
        logger.trace("  ->核心处理器 实例已创建/更新。")
    except Exception as e:
        logger.error(f"创建 MediaProcessor 实例失败: {e}", exc_info=True)
        media_processor_instance_local = None

    # 初始化 watchlist_processor_instance_local
    try:
        watchlist_processor_instance_local = WatchlistProcessor(
            config=current_config, 
            ai_translator=shared_ai_translator,
            douban_api=shared_douban_api
        )
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
    final_config_path = '/etc/nginx/conf.d/default.conf'
    # 检查开关
    if not config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_PROXY_ENABLED):
        logger.info("反向代理功能未启用，正在清理 Nginx 默认配置以释放端口...")
        try:
            # 写入空文件，相当于禁用了 Nginx 的默认站点
            with open(final_config_path, 'w') as f:
                f.write("# Proxy disabled in config.ini") 
            return
        except Exception as e:
            logger.warning(f"清理 Nginx 默认配置失败: {e}")
            return
    logger.info("正在生成 Nginx 配置文件...")
    
    template_dir = os.path.join(os.getcwd(), 'templates', 'nginx')
    template_filename = 'emby_proxy.conf.template'

    try:
        # 1. 设置 Jinja2 环境
        env = Environment(loader=FileSystemLoader(template_dir))
        template = env.get_template(template_filename)

        # 2. 从 APP_CONFIG 获取值
        emby_url = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_SERVER_URL, "")
        nginx_listen_port = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_PROXY_PORT, 8097)

        # 3. 准备替换值
        emby_upstream = emby_url.replace("http://", "").replace("https://", "").rstrip('/')
        # ★★★ 核心修改 2: Nginx 和 Python 代理在同一容器内，使用 localhost 通信 ★★★
        proxy_upstream = "127.0.0.1:7758" 

        if not emby_upstream:
            logger.error("config.ini 中未配置 Emby 服务器地址，无法生成 Nginx 配置！")
            sys.exit(1) # 严重错误，直接退出

        # 4. 填充模板
        context = {
            'EMBY_UPSTREAM': emby_upstream,
            'PROXY_UPSTREAM': proxy_upstream,
            'NGINX_LISTEN_PORT': nginx_listen_port,
            'NGINX_MAX_BODY_SIZE': '128m'
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
    global media_processor_instance, task_worker_thread, monitor_service_instance # 修正后的
    logger.info("应用程序正在退出 (atexit)，执行清理操作...")

    # 1. 立刻通知当前正在运行的任务停止
    if extensions.media_processor_instance: # 从 extensions 获取
        logger.info("正在发送停止信号给当前任务...")
        extensions.media_processor_instance.signal_stop()

    task_manager.clear_task_queue()
    task_manager.stop_task_worker()

    # ★★★ 新增：停止监控服务 ★★★
    if monitor_service_instance:
        monitor_service_instance.stop()

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
app.register_blueprint(actions_bp)
app.register_blueprint(cover_generator_config_bp)
app.register_blueprint(tasks_bp)
app.register_blueprint(resubscribe_bp)
app.register_blueprint(media_cleanup_bp)
app.register_blueprint(user_management_bp)
app.register_blueprint(webhook_bp)
app.register_blueprint(unified_auth_bp)
app.register_blueprint(user_portal_bp)
app.register_blueprint(discover_bp)
app.register_blueprint(p115_bp)
app.register_blueprint(hdhive_bp)

def main_app_start():
    """将主应用启动逻辑封装成一个函数"""
    global monitor_service_instance # 声明使用全局变量
    from gevent.pywsgi import WSGIServer
    from geventwebsocket.handler import WebSocketHandler
    import gevent

    logger.info(f"  ➜ 应用程序启动... 版本: {constants.APP_VERSION}")
    
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
    initialize_processors()
    task_manager.start_task_worker_if_not_running()
    scheduler_manager.start()

    # 启动实时监控服务
    try:
        if extensions.media_processor_instance:
            monitor_service_instance = MonitorService(config_manager.APP_CONFIG, extensions.media_processor_instance)
            monitor_service_instance.start()
    except Exception as e:
        logger.error(f"启动实时监控服务失败: {e}", exc_info=True)

    # 启动 Telegram 机器人交互监听
    telegram.start_telegram_bot()

    def warmup_vector_cache():
        try:
            logger.debug("  🔥 正在后台预加载向量数据...")
            # 只需要实例化一个引擎并调用 _get_vector_data 即可触发加载
            # 注意：这里不需要 api_key，因为只读库
            engine = RecommendationEngine(tmdb_api_key="dummy")
            engine._get_vector_data()
            logger.debug("  ✅ 向量数据预加载完成。")
        except Exception as e:
            logger.warning(f"  ⚠️ 向量预加载失败 (不影响启动): {e}")

    if config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_AI_VECTOR):
        # 这行代码会启动一个后台死循环，每隔 30 分钟刷新一次数据
        # 且第一次会立即执行，起到“预热”的作用
        RecommendationEngine.start_auto_refresh_loop()
    else:
        logger.debug("  ❌ 向量未启用，跳过向量预加载以节省内存。")
    
    def run_proxy_server():
        if config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_PROXY_ENABLED):
            # ★★★ 不是 Pro 直接不启动反代服务 ★★★
            if not config_manager.APP_CONFIG.get('is_pro_active', False):
                logger.warning("  ⚠️ [免费版限制] 302 反向代理与虚拟库功能为 Pro 高级版专属！反代服务未启动。")
                return

            try:
                internal_proxy_port = 7758
                external_port = config_manager.APP_CONFIG.get(constants.CONFIG_OPTION_PROXY_PORT, 8097)
                logger.info(f"  🚀 [302反代] 服务器已启动 (容器监听端口: {external_port})")
                proxy_server = WSGIServer(('0.0.0.0', internal_proxy_port), proxy_app, handler_class=WebSocketHandler)
                proxy_server.serve_forever()
            except Exception as e:
                logger.error(f"启动虚拟库服务失败: {e}", exc_info=True)
        else:
            logger.info("虚拟库未在配置中启用。")

    gevent.spawn(run_proxy_server)

    main_app_port = int(constants.WEB_APP_PORT)
    logger.info(f"  ✅ [主应用] 服务器已启动 (容器监听端口: {main_app_port})")
    
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
