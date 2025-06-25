# web_app.py
import os
import re
import sys
import inspect
import sqlite3
import shutil
from actor_sync_handler import UnifiedSyncHandler
import emby_handler
import utils
import configparser
from flask import Flask, render_template, request, redirect, url_for, jsonify, flash, stream_with_context, send_from_directory,Response
from werkzeug.utils import safe_join
from queue import Queue
from functools import wraps
from utils import get_override_path_for_item
from watchlist_processor import WatchlistProcessor
import threading
import time
from datetime import datetime
import requests
from douban import DoubanApi
from typing import Optional, Dict, Any, List, Tuple, Union # 确保 List 被导入
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz # 用于处理时区
import atexit # 用于应用退出处理
from core_processor_sa import MediaProcessorSA
from core_processor_api import MediaProcessorAPI
import csv
from io import StringIO
from werkzeug.security import generate_password_hash, check_password_hash
import secrets
from actor_utils import ActorDBManager
from flask import session
from croniter import croniter
import logging
# --- 核心模块导入 ---
import constants # 你的常量定义\
import logging
from logger_setup import frontend_log_queue, add_file_handler # 日志记录器和前端日志队列
import utils       # 例如，用于 /api/search_media
# --- 核心模块导入结束 ---
logger = logging.getLogger(__name__)
logging.getLogger("apscheduler.scheduler").setLevel(logging.WARNING)
logging.getLogger('werkzeug').setLevel(logging.ERROR)
static_folder='static'
app = Flask(__name__, static_folder='static')
app.secret_key = os.urandom(24)
# CORS(app) # 最简单的全局启用 CORS，允许所有源
# app.secret_key = os.urandom(24) # 用于 flash 消息等
# ✨✨✨ 新增：导入我们创建的网页解析器 ✨✨✨
try:
    from web_parser import parse_cast_from_url, ParserError
    WEB_PARSER_AVAILABLE = True
except ImportError:
    logger.error("web_parser.py 未找到或无法导入，从URL提取功能将不可用。")
    WEB_PARSER_AVAILABLE = False
# vue_dev_server_origin = "http://localhost:5173"
# CORS(app, resources={r"/api/*": {"origins": vue_dev_server_origin}})
# 1. 尝试从环境变量获取数据目录
APP_DATA_DIR_ENV = os.environ.get("APP_DATA_DIR")

if APP_DATA_DIR_ENV:
    # --- Docker 环境 ---
    # 如果环境变量存在，我们认为它在 Docker 中运行，并使用该路径
    PERSISTENT_DATA_PATH = APP_DATA_DIR_ENV
    print(f"INFO: 检测到 APP_DATA_DIR 环境变量，将使用持久化数据路径: {PERSISTENT_DATA_PATH}")
else:
    # --- 本地开发环境 ---
    # 如果环境变量不存在，我们回退到本地开发模式
    # 在当前项目根目录下创建一个 'local_data' 文件夹
    # 这种方式可以确保无论 web_app.py 在哪个子目录，路径都是正确的
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    PERSISTENT_DATA_PATH = os.path.join(PROJECT_ROOT, "local_data")
    print(f"DEBUG: 未检测到 APP_DATA_DIR 环境变量，将使用本地开发数据路径: {PERSISTENT_DATA_PATH}")

# 2. 确保这个持久化数据目录存在 (无论是在本地还是在容器内)
try:
    os.makedirs(PERSISTENT_DATA_PATH, exist_ok=True)
except OSError as e:
    print(f"FATAL: 创建持久化数据目录 '{PERSISTENT_DATA_PATH}' 失败: {e}。")
    sys.exit(1)

# 3. ★★★ 关键：将最终确定的路径，也设置回环境变量中 ★★★
#    这样做可以统一后续代码对路径的获取方式，所有模块都可以安全地使用 os.environ.get
#    即使是在本地开发模式下。
os.environ['APP_DATA_DIR'] = PERSISTENT_DATA_PATH

# 4. 基于这个唯一的、正确的路径来定义其他核心路径常量
#    这些常量在整个应用中都应该是只读的
CONFIG_FILE_PATH = os.path.join(PERSISTENT_DATA_PATH, constants.CONFIG_FILE_NAME)
DB_PATH = os.path.join(PERSISTENT_DATA_PATH, constants.DB_NAME)

# 5. 配置日志文件处理器
#    注意：这里我们不再需要 print 日志了，因为 logger 还没完全配置好
#    我们应该在 logger_setup.py 中配置好基础的 StreamHandler 后再调用它
add_file_handler(PERSISTENT_DATA_PATH)

# 6. 现在，我们可以安全地使用 logger 了
logger.info(f"配置文件路径设置为: {CONFIG_FILE_PATH}")
logger.info(f"数据库文件路径设置为: {DB_PATH}")
logging.basicConfig(
    level=logging.INFO,
    # ✨ 关键在这里：设置你想要的格式 ✨
    format='[%(asctime)s] %(message)s',
    datefmt='%H:%M:%S'
)

# --- 全局变量 ---
media_processor_instance: Optional[Union[MediaProcessorSA, MediaProcessorAPI]] = None
background_task_status = {
    "is_running": False,
    "current_action": "无",
    "progress": 0,
    "message": "等待任务"
}
task_lock = threading.Lock() # 用于确保后台任务串行执行
APP_CONFIG: Dict[str, Any] = {} # ✨✨✨ 新增：全局配置字典 ✨✨✨
media_processor_instance: Optional[MediaProcessorSA] = None
watchlist_processor_instance: Optional[WatchlistProcessor] = None
update_info = {
    "has_update": False,
    "latest_version": None,
    "error": None # 增加一个错误字段，便于排查
}
# ✨✨✨ 任务队列 ✨✨✨
task_queue = Queue()
task_worker_thread: Optional[threading.Thread] = None
task_worker_lock = threading.Lock()

scheduler = BackgroundScheduler(timezone=str(pytz.timezone(constants.TIMEZONE)))
JOB_ID_FULL_SCAN = "scheduled_full_scan"
JOB_ID_SYNC_PERSON_MAP = "scheduled_sync_person_map"
JOB_ID_PROCESS_WATCHLIST = "scheduled_process_watchlist"
# --- 全局变量结束 ---

# --- 数据库辅助函数 ---
def task_process_single_item(processor: MediaProcessorSA, item_id: str, force_reprocess: bool, process_episodes: bool):
    """任务：处理单个媒体项"""
    processor.process_single_item(item_id, force_reprocess, process_episodes)

def get_db_connection() -> sqlite3.Connection:
    # 确保 DB_PATH 是有效的，并且目录存在
    # 这个函数本身不应该处理目录创建，那是 init_db 的责任
    if not os.path.exists(os.path.dirname(DB_PATH)): # 检查数据库文件所在的目录是否存在
        logger.warning(f"数据库目录 {os.path.dirname(DB_PATH)} 不存在，get_db_connection 可能失败。")
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn
def initialize_processors():
    """
    【修复版】初始化所有需要的处理器实例，包括 MediaProcessor 和 WatchlistProcessor。
    """
    # ★★★ 1. 声明所有需要修改的全局变量 ★★★
    global media_processor_instance, watchlist_processor_instance
    
    if not APP_CONFIG:
        logger.error("无法初始化处理器：全局配置 APP_CONFIG 为空。")
        return

    current_config = APP_CONFIG.copy()
    current_config['db_path'] = DB_PATH

    # --- 初始化 MediaProcessor (您的原有逻辑) ---
    if media_processor_instance:
        try:
            media_processor_instance.close()
        except Exception as e:
            logger.warning(f"关闭旧的 media_processor_instance 时出错: {e}")

    use_sa_mode = current_config.get(constants.CONFIG_OPTION_USE_SA_MODE, True)
    
    try:
        if use_sa_mode:
            logger.info("【模式切换】当前为：神医模式")
            media_processor_instance = MediaProcessorSA(config=current_config)
        else:
            logger.info("【模式切换】当前为：普通模式")
            media_processor_instance = MediaProcessorAPI(config=current_config)
        
        logger.debug("MediaProcessor 实例已成功创建/更新。")

    except Exception as e:
        logger.error(f"创建 MediaProcessor 实例失败: {e}", exc_info=True)
        media_processor_instance = None

    # --- ★★★ 2. 新增：初始化 WatchlistProcessor ★★★ ---
    if watchlist_processor_instance:
        try:
            watchlist_processor_instance.close()
        except Exception as e:
            logger.warning(f"关闭旧的 watchlist_processor_instance 时出错: {e}")

    # 追剧功能通常依赖于核心配置，我们在这里创建它，让它随时待命
    # 假设 WatchlistProcessor 也需要 Emby URL 和 API Key
    if current_config.get("emby_server_url") and current_config.get("emby_api_key"):
        try:
            # 假设 WatchlistProcessor 的构造函数和 MediaProcessor 类似，接收一个 config 字典
            watchlist_processor_instance = WatchlistProcessor(config=current_config)
            logger.debug("WatchlistProcessor 实例已成功初始化，随时待命。")
        except Exception as e:
            logger.error(f"创建 WatchlistProcessor 实例失败: {e}", exc_info=True)
            watchlist_processor_instance = None # 初始化失败，明确设为 None
    else:
        logger.warning("WatchlistProcessor 未初始化，因为缺少必要的 Emby 配置。")
        watchlist_processor_instance = None
def setup_scheduled_tasks():
    config = APP_CONFIG

    # --- 处理全量扫描的定时任务 ---
    schedule_scan_enabled = config.get("schedule_enabled", False)
    scan_cron_expression = config.get("schedule_cron", "0 3 * * *")
    force_reprocess_scheduled_scan = config.get("schedule_force_reprocess", False)

    if scheduler.get_job(JOB_ID_FULL_SCAN):
        scheduler.remove_job(JOB_ID_FULL_SCAN)
        # logger.info("已移除旧的定时全量扫描任务。") # 可以选择性保留或移除此日志

    if schedule_scan_enabled:
        try:
            def submit_scheduled_scan_to_queue():
                # ... (内部逻辑保持不变)
                logger.info(f"定时任务触发：准备提交全量扫描到任务队列 (强制={force_reprocess_scheduled_scan})。")
                if force_reprocess_scheduled_scan:
                    logger.info("定时任务：检测到“强制重处理”选项，将在任务开始前清空已处理日志。")
                    if media_processor_instance:
                        media_processor_instance.clear_processed_log()
                    else:
                        logger.error("定时任务：无法清空日志，因为处理器未初始化。")
                current_config, _ = load_config()
                process_episodes = current_config.get('process_episodes', True)
                submit_task_to_queue(
                    task_process_full_library,
                    "定时全量扫描",
                    process_episodes=process_episodes
                )

            scheduler.add_job(
                func=submit_scheduled_scan_to_queue,
                trigger=CronTrigger.from_crontab(scan_cron_expression, timezone=str(pytz.timezone(constants.TIMEZONE))),
                id=JOB_ID_FULL_SCAN,
                name="定时全量媒体库扫描",
                replace_existing=True,
            )
            # ✨ 日志优化 ✨
            next_run_str = _get_next_run_time_str(scan_cron_expression)
            force_str = " (强制重处理)" if force_reprocess_scheduled_scan else ""
            logger.info(f"已设置定时任务：全量扫描，将{next_run_str}{force_str}")

        except Exception as e:
            logger.error(f"设置定时全量扫描任务失败: {e}", exc_info=True)
    else:
        logger.info("定时全量扫描任务未启用。")

    # --- 对同步映射表的定时任务也做类似修改 ---
    schedule_sync_map_enabled = config.get("schedule_sync_map_enabled", False)
    sync_map_cron_expression = config.get("schedule_sync_map_cron", "0 1 * * *")

    if scheduler.get_job(JOB_ID_SYNC_PERSON_MAP):
        scheduler.remove_job(JOB_ID_SYNC_PERSON_MAP)

    if schedule_sync_map_enabled:
        try:
            def scheduled_sync_map_task():
                # ... (内部逻辑保持不变)
                logger.info("定时任务触发：演员映射表同步。")
                submit_task_to_queue(
                    task_sync_person_map,
                    "定时同步演员映射表"
                )

            scheduler.add_job(
                func=scheduled_sync_map_task,
                trigger=CronTrigger.from_crontab(sync_map_cron_expression, timezone=str(pytz.timezone(constants.TIMEZONE))),
                id=JOB_ID_SYNC_PERSON_MAP, name="定时同步Emby演员映射表", replace_existing=True
            )
            # ✨ 日志优化 ✨
            next_run_str = _get_next_run_time_str(sync_map_cron_expression)
            logger.info(f"已设置定时任务：同步演员映射表，将{next_run_str}")

        except Exception as e:
            logger.error(f"设置定时同步演员映射表任务失败: {e}", exc_info=True)
    else:
        logger.info("定时同步演员映射表任务未启用。")

    # --- 对智能追剧任务也做类似修改 ---
    if scheduler.get_job(JOB_ID_PROCESS_WATCHLIST):
        scheduler.remove_job(JOB_ID_PROCESS_WATCHLIST)

    if config.get(constants.CONFIG_OPTION_SCHEDULE_WATCHLIST_ENABLED, False):
        if config.get(constants.CONFIG_OPTION_USE_SA_MODE, False):
            cron_expression = config.get(constants.CONFIG_OPTION_SCHEDULE_WATCHLIST_CRON)
            if cron_expression:
                try:
                    def scheduled_watchlist_task():
                        # ... (内部逻辑保持不变)
                        logger.debug("定时任务触发：智能追剧更新。")
                        submit_task_to_queue(task_process_watchlist, "定时智能追剧更新")

                    scheduler.add_job(
                        func=scheduled_watchlist_task,
                        trigger=CronTrigger.from_crontab(cron_expression, timezone=str(pytz.timezone(constants.TIMEZONE))),
                        id=JOB_ID_PROCESS_WATCHLIST,
                        name="定时智能追剧更新",
                        replace_existing=True,
                    )
                    # ✨ 日志优化 ✨
                    next_run_str = _get_next_run_time_str(cron_expression)
                    logger.info(f"已设置定时任务：智能追剧更新，将{next_run_str}")

                except Exception as e:
                    logger.error(f"设置定时智能追剧更新任务失败: {e}", exc_info=True)
    else:
        logger.info("定时智能追剧更新任务未启用。")

    # --- 启动调度器逻辑保持不变 ---
    scan_enabled = config.get("schedule_enabled", False)
    sync_enabled = config.get("schedule_sync_map_enabled", False)
    watchlist_enabled = config.get(constants.CONFIG_OPTION_SCHEDULE_WATCHLIST_ENABLED, False)

    if not scheduler.running and (scan_enabled or sync_enabled or watchlist_enabled):
        try:
            scheduler.start()
            logger.info("APScheduler 已根据任务需求启动。")
        except Exception as e_scheduler_start:
            logger.error(f"APScheduler 启动失败: {e_scheduler_start}", exc_info=True)
def init_db():
    """
    【重建版】初始化数据库，创建面向未来的统一表结构。
    此版本已移除旧的、分离的演员表，并引入了统一的身份管理体系。
    """
    conn: Optional[sqlite3.Connection] = None
    try:
        # --- 1. 准备工作：创建目录并获取连接 ---
        if not os.path.exists(PERSISTENT_DATA_PATH):
            os.makedirs(PERSISTENT_DATA_PATH, exist_ok=True)
            logger.info(f"持久化数据目录已创建: {PERSISTENT_DATA_PATH}")

        conn = get_db_connection()
        cursor = conn.cursor()

        # --- 2. 性能优化：启用 WAL 模式 ---
        # 提高并发读写性能，是现代 SQLite 应用的标配。
        try:
            cursor.execute("PRAGMA journal_mode=WAL;")
            result = cursor.fetchone()
            if result and result[0].lower() == 'wal':
                logger.debug("数据库已成功启用 WAL (Write-Ahead Logging) 模式。")
            else:
                logger.warning(f"尝试启用 WAL 模式失败，当前模式: {result[0] if result else '未知'}。")
        except Exception as e_wal:
            logger.error(f"启用 WAL 模式时出错: {e_wal}")

        # --- 3. 创建基础表 (日志、缓存、用户) ---
        logger.debug("正在确认/创建基础表...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_log (
                item_id TEXT PRIMARY KEY, item_name TEXT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, score REAL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS failed_log (
                item_id TEXT PRIMARY KEY, item_name TEXT,
                failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                error_message TEXT, item_type TEXT, score REAL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS translation_cache (
                original_text TEXT PRIMARY KEY, translated_text TEXT,
                engine_used TEXT, last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        logger.debug("基础表结构已确认。")

        # --- 4. 创建核心功能表 (追剧列表) ---
        logger.debug("正在确认/创建 'watchlist' 表...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS watchlist (
                item_id TEXT PRIMARY KEY,
                tmdb_id TEXT NOT NULL,
                item_name TEXT,
                item_type TEXT DEFAULT 'Series',
                status TEXT DEFAULT 'Watching', -- 'Watching', 'Paused', 'Completed'
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_checked_at TIMESTAMP
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_watchlist_status ON watchlist (status)")
        logger.debug("表 'watchlist' 结构已确认。")


        # 核心表：person_identity_map (单一事实来源)
        # 职责：存储每个演员的唯一身份和跨平台ID映射。
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS person_identity_map (
                -- 中立的内部主键，我们的地盘我们做主！
                map_id INTEGER PRIMARY KEY AUTOINCREMENT,
                
                -- 权威的、用户友好的名字
                primary_name TEXT NOT NULL,
                -- (可选) 使用JSON存储其他平台的名字，如 {"tmdb": "Yan Ni", "douban": "闫妮"}
                other_names TEXT,

                -- 所有外部ID，都应该是 UNIQUE 且允许为 NULL
                emby_person_id TEXT UNIQUE,
                tmdb_person_id INTEGER UNIQUE,
                imdb_id TEXT UNIQUE,
                douban_celebrity_id TEXT UNIQUE,

                -- 时间戳
                last_synced_at TIMESTAMP,
                last_updated_at TIMESTAMP
            )
        """)
        # 为所有外部ID创建索引，加速查找和冲突检测
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pim_emby_id ON person_identity_map (emby_person_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pim_tmdb_id ON person_identity_map (tmdb_person_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pim_imdb_id ON person_identity_map (imdb_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pim_douban_id ON person_identity_map (douban_celebrity_id)")
        logger.debug("  -> [核心] 'person_identity_map' 表已创建。")

        # --- 6. 提交事务 ---
        conn.commit()
        logger.info(f"数据库重建完成！所有表结构已在 '{DB_PATH}' 中创建。")

    except sqlite3.Error as e_sqlite:
        logger.error(f"数据库初始化时发生 SQLite 错误: {e_sqlite}", exc_info=True)
        if conn:
            try: conn.rollback()
            except Exception as e_rb: logger.error(f"SQLite 错误后回滚失败: {e_rb}")
    except Exception as e_global:
        logger.error(f"数据库初始化时发生未知错误: {e_global}", exc_info=True)
        if conn:
            try: conn.rollback()
            except Exception as e_rb: logger.error(f"未知错误后回滚失败: {e_rb}")
    finally:
        if conn:
            conn.close()
            logger.debug("数据库连接已在 init_db 的 finally 块中安全关闭。")

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # ★★★ 核心修复：正确地解包 load_config 返回的元组 ★★★
        if not APP_CONFIG.get(constants.CONFIG_OPTION_AUTH_ENABLED, False) or 'user_id' in session:
            return f(*args, **kwargs)
        
        return jsonify({"error": "未授权，请先登录"}), 401
    return decorated_function
def init_auth():
    """
    【V2 - 使用全局配置版】初始化认证系统。
    """
    # ✨✨✨ 核心修复：不再自己调用 load_config，而是依赖已加载的 APP_CONFIG ✨✨✨
    # load_config() 应该在主程序入口处被调用一次
    
    auth_enabled = APP_CONFIG.get(constants.CONFIG_OPTION_AUTH_ENABLED, False)
    env_username = os.environ.get("AUTH_USERNAME")
    
    if env_username:
        username = env_username.strip()
        logger.info(f"检测到 AUTH_USERNAME 环境变量，将使用用户名: '{username}'")
    else:
        username = APP_CONFIG.get(constants.CONFIG_OPTION_AUTH_USERNAME, constants.DEFAULT_USERNAME).strip()
        logger.debug(f"未检测到 AUTH_USERNAME 环境变量，将使用配置文件中的用户名: '{username}'")

    if not auth_enabled:
        logger.info("用户认证功能未启用。")
        return

    # ... 函数的其余部分保持不变 ...
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM users WHERE username = ?", (username,))
        user = cursor.fetchone()
        
        if user is None:
            logger.warning(f"[AUTH DIAGNOSTIC] User '{username}' not found in DB. Proceeding to create password.")
            # ... (生成密码的逻辑不变) ...
            random_password = secrets.token_urlsafe(12)
            password_hash = generate_password_hash(random_password)
            cursor.execute(
                "INSERT INTO users (username, password_hash) VALUES (?, ?)",
                (username, password_hash)
            )
            conn.commit()
            logger.critical("=" * 60)
            logger.critical(" " * 20 + "!!! 重要提示 !!!")
            logger.critical(f"首次运行，已为用户 '{username}' 自动生成初始密码。")
            logger.critical(f"用户名: {username}")
            logger.critical(f"初始密码: {random_password}")
            logger.critical("请立即使用此密码登录，并在设置页面修改为您自己的密码。")
            logger.critical("=" * 60)
        else:
            logger.debug(f"[AUTH DIAGNOSTIC] User '{username}' found in DB. No action needed.")

    except Exception as e:
        logger.error(f"初始化认证系统时发生错误: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
        logger.info("="*21 + " [基础配置加载完毕] " + "="*21)
# --- 配置加载与保存 ---
def load_config() -> Tuple[Dict[str, Any], bool]:
    """
    【全自动版】从 config.ini 文件加载配置。
    返回一个元组: (配置字典, 是否是首次创建配置的标记)
    """
    global APP_CONFIG # 声明我们要修改全局变量
    config_parser = configparser.ConfigParser()
    is_first_run_creating_config = False # 初始化标记

    if not os.path.exists(CONFIG_FILE_PATH):
        logger.warning(f"配置文件 '{CONFIG_FILE_PATH}' 未找到。将标记为首次运行并使用默认值。")
        is_first_run_creating_config = True
        # 注意：这里我们不再立即创建文件，将创建文件的责任交给 init_auth
    else:
        try:
            config_parser.read(CONFIG_FILE_PATH, encoding='utf-8')
            logger.debug(f"配置已从 '{CONFIG_FILE_PATH}' 加载。")
        except Exception as e:
            logger.error(f"解析配置文件 '{CONFIG_FILE_PATH}' 时发生错误: {e}", exc_info=True)

    # 定义所有期望的节
    expected_sections = [
        constants.CONFIG_SECTION_EMBY, constants.CONFIG_SECTION_TMDB,
        constants.CONFIG_SECTION_API_DOUBAN, constants.CONFIG_SECTION_TRANSLATION,
        # constants.CONFIG_SECTION_DOMESTIC_SOURCE, constants.CONFIG_SECTION_LOCAL_DATA,
        "General", "Scheduler", "Network", "AITranslation",
        constants.CONFIG_SECTION_AUTH
    ]
    for section_name in expected_sections:
        if not config_parser.has_section(section_name):
            config_parser.add_section(section_name)

    app_cfg: Dict[str, Any] = {}

    # Emby Section
    app_cfg["emby_server_url"] = config_parser.get(constants.CONFIG_SECTION_EMBY, "emby_server_url", fallback="")
    app_cfg["emby_api_key"] = config_parser.get(constants.CONFIG_SECTION_EMBY, "emby_api_key", fallback="")
    app_cfg["emby_user_id"] = config_parser.get(constants.CONFIG_SECTION_EMBY, "emby_user_id", fallback="")
    app_cfg["refresh_emby_after_update"] = config_parser.getboolean(constants.CONFIG_SECTION_EMBY, "refresh_emby_after_update", fallback=True)
    libraries_str = config_parser.get(constants.CONFIG_SECTION_EMBY, "libraries_to_process", fallback="")
    app_cfg["libraries_to_process"] = [lib_id.strip() for lib_id in libraries_str.split(',') if lib_id.strip()]

    # TMDB, Douban, Translation, etc.
    app_cfg["tmdb_api_key"] = config_parser.get(constants.CONFIG_SECTION_TMDB, "tmdb_api_key", fallback="")
    app_cfg["api_douban_default_cooldown_seconds"] = config_parser.getfloat(constants.CONFIG_SECTION_API_DOUBAN, "api_douban_default_cooldown_seconds", fallback=1.0)
    engines_str = config_parser.get(
        constants.CONFIG_SECTION_TRANSLATION, 
        constants.CONFIG_OPTION_TRANSLATOR_ENGINES,
        fallback=",".join(constants.DEFAULT_TRANSLATOR_ENGINES_ORDER)
    )
    app_cfg[constants.CONFIG_OPTION_TRANSLATOR_ENGINES] = [eng.strip() for eng in engines_str.split(',') if eng.strip()]
    app_cfg["local_data_path"] = config_parser.get(constants.CONFIG_SECTION_LOCAL_DATA, "local_data_path", fallback="").strip()

    # General Section
    app_cfg["delay_between_items_sec"] = config_parser.getfloat("General", "delay_between_items_sec", fallback=0.5)
    app_cfg["min_score_for_review"] = config_parser.getfloat("General", "min_score_for_review", fallback=6.0)
    app_cfg["process_episodes"] = config_parser.getboolean("General", "process_episodes", fallback=True)
    app_cfg[constants.CONFIG_OPTION_SYNC_IMAGES] = config_parser.getboolean(
        "General",
        constants.CONFIG_OPTION_SYNC_IMAGES,
        fallback=False
    )
    app_cfg[constants.CONFIG_OPTION_MAX_ACTORS_TO_PROCESS] = config_parser.getint(
    constants.CONFIG_SECTION_GENERAL,
    constants.CONFIG_OPTION_MAX_ACTORS_TO_PROCESS,
    fallback=constants.DEFAULT_MAX_ACTORS_TO_PROCESS
    )

    # Network Section
    app_cfg["user_agent"] = config_parser.get("Network", "user_agent", fallback='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36')
    app_cfg["accept_language"] = config_parser.get("Network", "accept_language", fallback='zh-CN,zh;q=0.9,en;q=0.8')

    # AITranslation Section
    app_cfg[constants.CONFIG_OPTION_AI_TRANSLATION_ENABLED] = config_parser.getboolean(constants.CONFIG_SECTION_AI_TRANSLATION, constants.CONFIG_OPTION_AI_TRANSLATION_ENABLED, fallback=False)
    app_cfg[constants.CONFIG_OPTION_AI_PROVIDER] = config_parser.get(constants.CONFIG_SECTION_AI_TRANSLATION, constants.CONFIG_OPTION_AI_PROVIDER, fallback="openai")
    app_cfg[constants.CONFIG_OPTION_AI_API_KEY] = config_parser.get(constants.CONFIG_SECTION_AI_TRANSLATION, constants.CONFIG_OPTION_AI_API_KEY, fallback="")
    app_cfg[constants.CONFIG_OPTION_AI_MODEL_NAME] = config_parser.get(constants.CONFIG_SECTION_AI_TRANSLATION, constants.CONFIG_OPTION_AI_MODEL_NAME, fallback="deepseek-ai/DeepSeek-V2.5")
    app_cfg[constants.CONFIG_OPTION_AI_BASE_URL] = config_parser.get(constants.CONFIG_SECTION_AI_TRANSLATION, constants.CONFIG_OPTION_AI_BASE_URL, fallback="https://api.siliconflow.cn/v1")
    # app_cfg[constants.CONFIG_OPTION_AI_TRANSLATION_PROMPT] = config_parser.get(constants.CONFIG_SECTION_AI_TRANSLATION, constants.CONFIG_OPTION_AI_TRANSLATION_PROMPT, fallback=constants.DEFAULT_AI_TRANSLATION_PROMPT)

    # Scheduler Section
    app_cfg["schedule_enabled"] = config_parser.getboolean("Scheduler", "schedule_enabled", fallback=False)
    app_cfg["schedule_cron"] = config_parser.get("Scheduler", "schedule_cron", fallback="0 3 * * *")
    app_cfg["schedule_force_reprocess"] = config_parser.getboolean("Scheduler", "schedule_force_reprocess", fallback=False)
    app_cfg["schedule_sync_map_enabled"] = config_parser.getboolean("Scheduler", "schedule_sync_map_enabled", fallback=False)
    app_cfg["schedule_sync_map_cron"] = config_parser.get("Scheduler", "schedule_sync_map_cron", fallback="0 1 * * *")

    app_cfg[constants.CONFIG_OPTION_SCHEDULE_WATCHLIST_ENABLED] = config_parser.getboolean(
        constants.CONFIG_SECTION_SCHEDULER,
        constants.CONFIG_OPTION_SCHEDULE_WATCHLIST_ENABLED,
        fallback=False  # 默认不开启
    )
    app_cfg[constants.CONFIG_OPTION_SCHEDULE_WATCHLIST_CRON] = config_parser.get(
        constants.CONFIG_SECTION_SCHEDULER,
        constants.CONFIG_OPTION_SCHEDULE_WATCHLIST_CRON,
        fallback=constants.DEFAULT_SCHEDULE_WATCHLIST_CRON # 使用常量里的默认值
    )

    # ★★★ 新增神医开关 ★★★
    if not config_parser.has_section(constants.CONFIG_SECTION_FEATURES):
        config_parser.add_section(constants.CONFIG_SECTION_FEATURES)
        
    app_cfg[constants.CONFIG_OPTION_USE_SA_MODE] = config_parser.getboolean(
        constants.CONFIG_SECTION_FEATURES,
        constants.CONFIG_OPTION_USE_SA_MODE,
        fallback=True  # 默认开启神医模式，因为它是功能更全的版本
    )
    # ★★★ 新增结束 ★★★

    # Authentication Section
    if is_first_run_creating_config:
        app_cfg[constants.CONFIG_OPTION_AUTH_ENABLED] = True
    else:
        app_cfg[constants.CONFIG_OPTION_AUTH_ENABLED] = config_parser.getboolean(
            constants.CONFIG_SECTION_AUTH, 
            constants.CONFIG_OPTION_AUTH_ENABLED, 
            fallback=False
        )
    
    app_cfg[constants.CONFIG_OPTION_AUTH_USERNAME] = config_parser.get(
        constants.CONFIG_SECTION_AUTH,
        constants.CONFIG_OPTION_AUTH_USERNAME,
        fallback=constants.DEFAULT_USERNAME
    )
    # ...
    APP_CONFIG = app_cfg.copy() # ✨✨✨ 将加载到的配置存入全局变量 ✨✨✨
    logger.debug("全局配置变量 APP_CONFIG 已更新。")

    return app_cfg, is_first_run_creating_config # 返回两个值

def save_config(new_config: Dict[str, Any]): # 移除 trigger_reload 参数，它总是应该触发
    global APP_CONFIG
    config = configparser.ConfigParser()
    
    if os.path.exists(CONFIG_FILE_PATH):
        config.read(CONFIG_FILE_PATH, encoding='utf-8')

    # ✨✨✨ 关键修复：在设置任何值之前，确保所有节都存在 ✨✨✨
    all_sections_to_manage = [
        constants.CONFIG_SECTION_EMBY,
        constants.CONFIG_SECTION_TMDB,
        constants.CONFIG_SECTION_API_DOUBAN,
        constants.CONFIG_SECTION_TRANSLATION,
        # constants.CONFIG_SECTION_DOMESTIC_SOURCE,
        constants.CONFIG_SECTION_LOCAL_DATA,
        "General",
        "Scheduler",
        "Network",
        constants.CONFIG_SECTION_AI_TRANSLATION
    ]
    all_sections_to_manage.append(constants.CONFIG_SECTION_AUTH)

    for section_name in all_sections_to_manage:
        if not config.has_section(section_name):
            logger.info(f"保存配置：配置文件中缺少节 '[{section_name}]'，将自动创建。")
            config.add_section(section_name)
    # ✨✨✨ 修复结束 ✨✨✨

    # --- 现在可以安全地设置每个配置项了 ---
    
    # Emby Section
    config.set(constants.CONFIG_SECTION_EMBY, "emby_server_url", str(new_config.get("emby_server_url", "")))
    config.set(constants.CONFIG_SECTION_EMBY, "emby_api_key", str(new_config.get("emby_api_key", "")))
    config.set(constants.CONFIG_SECTION_EMBY, "emby_user_id", str(new_config.get("emby_user_id", "")))
    config.set(constants.CONFIG_SECTION_EMBY, "refresh_emby_after_update", str(new_config.get("refresh_emby_after_update", True)).lower())
    libraries_list = new_config.get("libraries_to_process", [])
    if not isinstance(libraries_list, list):
        libraries_list = [lib_id.strip() for lib_id in str(libraries_list).split(',') if lib_id.strip()]
    config.set(constants.CONFIG_SECTION_EMBY, "libraries_to_process", ",".join(map(str, libraries_list)))

    # TMDB, Douban, Translation, etc.
    config.set(constants.CONFIG_SECTION_TMDB, "tmdb_api_key", str(new_config.get("tmdb_api_key", "")))
    config.set(constants.CONFIG_SECTION_API_DOUBAN, "api_douban_default_cooldown_seconds", str(new_config.get("api_douban_default_cooldown_seconds", 1.0)))
    engines_list = new_config.get(constants.CONFIG_OPTION_TRANSLATOR_ENGINES, constants.DEFAULT_TRANSLATOR_ENGINES_ORDER)
    if not isinstance(engines_list, list): # 健壮性检查
        engines_list = constants.DEFAULT_TRANSLATOR_ENGINES_ORDER
    config.set(
        constants.CONFIG_SECTION_TRANSLATION, 
        constants.CONFIG_OPTION_TRANSLATOR_ENGINES, # 使用常量，不再是硬编码字符串
        ",".join(engines_list)
    )
    config.set(constants.CONFIG_SECTION_TRANSLATION, "translator_engines_order_str", ",".join(engines_list))
    config.set(constants.CONFIG_SECTION_LOCAL_DATA, "local_data_path", str(new_config.get("local_data_path", "")))

    # General Section
    config.set("General", "delay_between_items_sec", str(new_config.get("delay_between_items_sec", "0.5")))
    config.set("General", "min_score_for_review", str(new_config.get("min_score_for_review", "6.0")))
    config.set("General", "process_episodes", str(new_config.get("process_episodes", True)).lower())
    sync_images_val = new_config.get(constants.CONFIG_OPTION_SYNC_IMAGES, False)
    config.set(
        "General", # 将其归入 [General] 节
        constants.CONFIG_OPTION_SYNC_IMAGES,
        str(sync_images_val).lower() # 保存为 'true' 或 'false'
    )
    config.set(
    constants.CONFIG_SECTION_GENERAL,
    constants.CONFIG_OPTION_MAX_ACTORS_TO_PROCESS,
    str(new_config.get(constants.CONFIG_OPTION_MAX_ACTORS_TO_PROCESS, constants.DEFAULT_MAX_ACTORS_TO_PROCESS))
    )

    # Network Section
    config.set("Network", "user_agent", str(new_config.get("user_agent", "")))
    config.set("Network", "accept_language", str(new_config.get("accept_language", "")))

    # AITranslation Section
    config.set(constants.CONFIG_SECTION_AI_TRANSLATION, constants.CONFIG_OPTION_AI_TRANSLATION_ENABLED, str(new_config.get(constants.CONFIG_OPTION_AI_TRANSLATION_ENABLED, False)).lower())
    config.set(constants.CONFIG_SECTION_AI_TRANSLATION, constants.CONFIG_OPTION_AI_PROVIDER, str(new_config.get(constants.CONFIG_OPTION_AI_PROVIDER, "openai")))
    config.set(constants.CONFIG_SECTION_AI_TRANSLATION, constants.CONFIG_OPTION_AI_API_KEY, str(new_config.get(constants.CONFIG_OPTION_AI_API_KEY, "")))
    config.set(constants.CONFIG_SECTION_AI_TRANSLATION, constants.CONFIG_OPTION_AI_MODEL_NAME, str(new_config.get(constants.CONFIG_OPTION_AI_MODEL_NAME, "gpt-3.5-turbo")))
    config.set(constants.CONFIG_SECTION_AI_TRANSLATION, constants.CONFIG_OPTION_AI_BASE_URL, str(new_config.get(constants.CONFIG_OPTION_AI_BASE_URL, "")))
    # config.set(constants.CONFIG_SECTION_AI_TRANSLATION, constants.CONFIG_OPTION_AI_TRANSLATION_PROMPT, str(new_config.get(constants.CONFIG_OPTION_AI_TRANSLATION_PROMPT, "")))

    # Scheduler Section
    config.set("Scheduler", "schedule_enabled", str(new_config.get("schedule_enabled", False)).lower())
    config.set("Scheduler", "schedule_cron", str(new_config.get("schedule_cron", "0 3 * * *")))
    config.set("Scheduler", "schedule_force_reprocess", str(new_config.get("schedule_force_reprocess", False)).lower())
    config.set("Scheduler", "schedule_sync_map_enabled", str(new_config.get("schedule_sync_map_enabled", False)).lower())
    config.set("Scheduler", "schedule_sync_map_cron", str(new_config.get("schedule_sync_map_cron", "0 1 * * *")))

    config.set(
        constants.CONFIG_SECTION_SCHEDULER,
        constants.CONFIG_OPTION_SCHEDULE_WATCHLIST_ENABLED,
        str(new_config.get(constants.CONFIG_OPTION_SCHEDULE_WATCHLIST_ENABLED, False)).lower()
    )
    config.set(
        constants.CONFIG_SECTION_SCHEDULER,
        constants.CONFIG_OPTION_SCHEDULE_WATCHLIST_CRON,
        str(new_config.get(constants.CONFIG_OPTION_SCHEDULE_WATCHLIST_CRON, constants.DEFAULT_SCHEDULE_WATCHLIST_CRON))
    )

    # ★★★ 新增：写入追剧定时任务的配置 ★★★
    if not config.has_section(constants.CONFIG_SECTION_FEATURES):
        config.add_section(constants.CONFIG_SECTION_FEATURES)
    
    config.set(
        constants.CONFIG_SECTION_FEATURES,
        constants.CONFIG_OPTION_USE_SA_MODE,
        str(new_config.get(constants.CONFIG_OPTION_USE_SA_MODE, True)).lower()
    )
    # ★★★ 新增结束 ★★★

    #user
    config.set(
        constants.CONFIG_SECTION_AUTH,
        constants.CONFIG_OPTION_AUTH_ENABLED,
        str(new_config.get(constants.CONFIG_OPTION_AUTH_ENABLED, False)).lower()
    )
    config.set(
        constants.CONFIG_SECTION_AUTH,
        constants.CONFIG_OPTION_AUTH_USERNAME,
        str(new_config.get(constants.CONFIG_OPTION_AUTH_USERNAME, constants.DEFAULT_USERNAME))
    )

    try:
        if not os.path.exists(PERSISTENT_DATA_PATH):
            os.makedirs(PERSISTENT_DATA_PATH, exist_ok=True)
        with open(CONFIG_FILE_PATH, 'w', encoding='utf-8') as configfile:
            config.write(configfile)
        logger.info(f"配置已成功写入到 {CONFIG_FILE_PATH}。")
        # ✨✨✨ 保存成功后，立即更新全局配置变量 ✨✨✨
        APP_CONFIG = new_config.copy()
        logger.debug("全局配置变量 APP_CONFIG 已更新。")
        
        logger.debug("配置已保存，正在重新初始化所有相关组件...")
        initialize_processors() # 使用新配置创建新的 MediaProcessor 实例
        init_auth()                  # 重新检查认证设置
        setup_scheduled_tasks()      # 根据新配置重新设置定时任务
        logger.info("所有组件已根据新配置重新初始化完毕。")

    except Exception as e:
        logger.error(f"保存配置文件或重新初始化组件时失败: {e}", exc_info=True)
# ======================================================================
#  ★★★ 最终的、统一的应用初始化函数 ★★★
# ======================================================================
def initialize_application():
    # --- 确定路径 (这部分逻辑不变) ---
    APP_DATA_DIR_ENV = os.environ.get("APP_DATA_DIR")
    if APP_DATA_DIR_ENV:
        PERSISTENT_DATA_PATH = APP_DATA_DIR_ENV
    else:
        PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
        PERSISTENT_DATA_PATH = os.path.join(PROJECT_ROOT, "local_data")
    os.makedirs(PERSISTENT_DATA_PATH, exist_ok=True)

    # --- 定义核心路径常量 (这部分逻辑不变) ---
    global CONFIG_FILE_PATH, DB_PATH
    CONFIG_FILE_PATH = os.path.join(PERSISTENT_DATA_PATH, constants.CONFIG_FILE_NAME)
    DB_PATH = os.path.join(PERSISTENT_DATA_PATH, constants.DB_NAME)
    add_file_handler(PERSISTENT_DATA_PATH)
    logger.info(f"配置文件路径设置为: {CONFIG_FILE_PATH}")
    logger.info(f"数据库文件路径设置为: {DB_PATH}")

    # --- ★★★ 关键修改：加载配置并存入 Flask 官方地址簿 ★★★ ---
    
    # 1. 从 config.ini 文件加载配置到一个临时字典
    config_from_file, _ = load_config()
    
    # 2. 把我们手动确定的、绝对正确的路径也加进去
    config_from_file['PERSISTENT_DATA_PATH'] = PERSISTENT_DATA_PATH
    
    # 3. 把这个完整的配置字典，一次性更新到 app.config 中
    app.config.update(config_from_file)
    
    logger.info("所有配置已加载到 Flask app.config。")
    # --- ★★★ 修改结束 ★★★

    # --- 后续的初始化流程，现在都应该依赖 app.config ---
    # (为了最小化改动，我们暂时保持它们不变，它们可能还在用旧的全局变量)
    init_db()
    init_auth()
    initialize_processors()
    start_task_worker_if_not_running()
    setup_scheduled_tasks()
    
    # 启动后台更新检查线程 (不变)
    try:
        update_thread = threading.Thread(target=update_checker_task, daemon=True)
        update_thread.start()
        logger.info("后台更新检查线程已启动。")
    except Exception as e:
        logger.error(f"启动后台更新检查线程失败: {e}", exc_info=True)

    logger.info(f"--- 应用初始化完成 (版本: {constants.APP_VERSION}) ---")

# --- 后台任务回调 ---
def update_status_from_thread(progress: int, message: str):
    global background_task_status
    if progress >= 0:
        background_task_status["progress"] = progress
    background_task_status["message"] = message
# --- 后台任务封装 ---
def _execute_task_with_lock(task_function, task_name: str, processor: Union[MediaProcessorSA, MediaProcessorAPI, WatchlistProcessor], *args, **kwargs):
    """
    【V2 - 工人专用版】通用后台任务执行器。
    第一个参数必须是 MediaProcessor 实例。
    """
    global background_task_status
    # 锁的检查可以移到提交任务的地方，或者保留作为双重保险
    # if task_lock.locked(): ...

    with task_lock:
        # 1. 检查传入的处理器是否有效
        if not processor:
            logger.error(f"任务 '{task_name}' 无法启动：对应的处理器未初始化。")
            # 可以在这里更新状态，但因为没有启动，所以只打日志可能更清晰
            return

        # 2. 清理当前任务处理器的停止信号
        processor.clear_stop_signal()

        # 3. 设置任务状态，准备执行
        background_task_status["is_running"] = True
        background_task_status["current_action"] = task_name
        background_task_status["progress"] = 0
        background_task_status["message"] = f"{task_name} 初始化..."
        logger.info(f"后台任务 '{task_name}' 开始执行。")

        task_completed_normally = False
        try:
            if processor.is_stop_requested():
                raise InterruptedError("任务被取消")

            # 执行核心任务
            task_function(processor, *args, **kwargs)
            
            # ★★★ 核心修复：如果任务能顺利执行到这里，说明它正常完成了 ★★★
            # 我们只在没有被用户中止的情况下，才标记为正常完成
            if not processor.is_stop_requested():
                task_completed_normally = True
        finally:
            final_message_for_status = "未知结束状态"
            current_progress = background_task_status["progress"] # 获取当前进度

            if processor and processor.is_stop_requested():
                final_message_for_status = "任务已成功中断。"
            elif task_completed_normally:
                final_message_for_status = "处理完成。"
                current_progress = 100 # 正常完成则进度100%
            # else: 异常退出时，消息已在except中通过update_status_from_thread设置

            update_status_from_thread(current_progress, final_message_for_status)
            logger.info(f"后台任务 '{task_name}' 结束，最终状态: {final_message_for_status}")

            if processor:
                processor.close()
                logger.debug(f"任务 '{task_name}' 结束 (finally块)，准备调用 media_processor_instance.close() ...")
                try:
                    media_processor_instance.close()
                    logger.debug(f"media_processor_instance.close() 调用完毕 (任务 '{task_name}' finally块)。")
                except Exception as e_close_proc:
                    logger.error(f"调用 media_processor_instance.close() 时发生错误: {e_close_proc}", exc_info=True)

            time.sleep(1)
        background_task_status["is_running"] = False
        background_task_status["current_action"] = "无"
        background_task_status["progress"] = 0
        background_task_status["message"] = "等待任务"
        if processor:
            processor.clear_stop_signal()
        logger.debug(f"后台任务 '{task_name}' 状态已重置。")

#--- 通用队列 ---
def task_worker_function():
    """
    通用工人线程，从队列中获取并处理各种后台任务。
    """
    logger.info("通用任务线程已启动，等待任务...")
    while True:
        try:
            # 从队列中获取任务元组
            task_info = task_queue.get()

            if task_info is None: # 停止信号
                logger.info("工人线程收到停止信号，即将退出。")
                break

            # 解包任务信息
            task_function, task_name, args, kwargs = task_info
            # ★★★ 核心修复：在任务执行前，检查全局实例是否可用 ★★★
            if "追剧" in task_name or "watchlist" in task_function.__name__:
                processor_to_use = watchlist_processor_instance
                logger.debug(f"任务 '{task_name}' 将使用 WatchlistProcessor。")
            else:
                processor_to_use = media_processor_instance
                logger.debug(f"任务 '{task_name}' 将使用 MediaProcessor。")

            if not processor_to_use:
                logger.error(f"任务 '{task_name}' 无法执行：对应的处理器未初始化。")
                task_queue.task_done()
                continue

            _execute_task_with_lock(task_function, task_name, processor_to_use, *args, **kwargs)
            
            task_queue.task_done()
        except Exception as e:
            logger.error(f"通用工人线程发生未知错误: {e}", exc_info=True)
            time.sleep(5)

def start_task_worker_if_not_running():
    """
    安全地启动通用工人线程。
    """
    global task_worker_thread
    with task_worker_lock:
        if task_worker_thread is None or not task_worker_thread.is_alive():
            logger.debug("通用任务线程未运行，正在启动...")
            task_worker_thread = threading.Thread(target=task_worker_function, daemon=True)
            task_worker_thread.start()
        else:
            logger.debug("通用任务线程已在运行。")
#--- 为通用队列添加任务 ---
def submit_task_to_queue(task_function, task_name: str, *args, **kwargs):
    """
    【修复版】将一个任务提交到通用队列中，并在这里清空日志。
    """
    # ★★★ 核心修改：在提交任务到队列之前，就清空旧日志 ★★★
    # 这个操作应该在 task_lock 的保护下进行，以确保原子性
    with task_lock:
        # 检查是否可以启动新任务
        if background_task_status["is_running"]:
            # 这里可以抛出异常或返回一个状态，让调用方知道任务提交失败
            # 为了简单起见，我们先打印日志并直接返回
            logger.warning(f"任务 '{task_name}' 提交失败：已有任务正在运行。")
            # 或者 raise RuntimeError("已有任务在运行")
            return

        # 如果可以启动，我们就在这里清空日志
        frontend_log_queue.clear()
        logger.info(f"任务 '{task_name}' 已提交到队列，并已清空前端日志。")
        
        task_info = (task_function, task_name, args, kwargs)
        task_queue.put(task_info)
        start_task_worker_if_not_running()

def _get_next_run_time_str(cron_expression: str) -> str:
    """
    将 CRON 表达式转换为人类可读的执行计划字符串。
    """
    try:
        tz = pytz.timezone(constants.TIMEZONE)
        now = datetime.now(tz)
        
        # 特殊处理常见的分钟级周期任务
        parts = cron_expression.split()
        if parts[0].startswith('*/') and parts[1:] == ['*', '*', '*', '*']:
            minutes = parts[0][2:]
            # ✨ 优化点：返回一个完整的短语，加上“执行” ✨
            return f"每隔 {minutes} 分钟执行"

        iterator = croniter(cron_expression, now)
        next_run = iterator.get_next(datetime)
        return f"在 {next_run.strftime('%H:%M')} 执行"
    except Exception as e:
        logger.warning(f"无法解析CRON表达式 '{cron_expression}': {e}")
        return f"按计划 '{cron_expression}' 执行"


# --- 定时任务结束 ---
def enrich_and_match_douban_cast_to_emby(
    douban_actors_api_data: List[Dict[str, Any]],
    current_emby_cast_list: List[Dict[str, Any]],
    tmdb_api_key: Optional[str],
    db_cursor: sqlite3.Cursor # 注意：这里接收的是游标，不是连接
) -> List[Dict[str, Any]]:
    logger.info(f"enrich_and_match_douban_cast_to_emby: 开始处理 {len(douban_actors_api_data)} 位豆瓣演员，与 {len(current_emby_cast_list)} 位Emby演员进行匹配增强。")
    results = []
    processed_emby_pids_in_this_run = set()

    for d_actor_data in douban_actors_api_data:
        # ... (这里是详细的匹配、TMDb增强、状态标记逻辑) ...
        # ... (确保这个函数内部调用的 match_douban_actor_to_emby_person 也已定义或导入) ...
        # ... (并且 tmdb_handler 也是可用的) ...

        # 示例：
        d_name = d_actor_data.get("name")
        logger.debug(f"  正在处理豆瓣演员: {d_name}")
        # (此处应有完整的匹配和信息组合逻辑)
        # (如果匹配成功，将结果添加到 results 列表)
        # results.append({ "embyPersonId": ..., "name": ..., ... "matchStatus": ... })

    logger.info(f"enrich_and_match_douban_cast_to_emby: 处理完成，返回 {len(results)} 个匹配/增强的演员信息。")
    return results

# --- 执行全量媒体库扫描 ---
def task_process_full_library(processor: MediaProcessorSA, process_episodes: bool):
    processor.process_full_library(
        update_status_callback=update_status_from_thread,
        process_episodes=process_episodes
    )

def task_sync_person_map(processor):
    """
    【最终兼容版】任务：同步演员映射表。
    接收 processor 和 is_full_sync 以匹配通用任务执行器，
    但内部逻辑已统一，不再使用 is_full_sync。
    """
    task_name = "统一演员映射表同步"
    # 我们不再需要根据 is_full_sync 来改变任务名了，因为逻辑已经统一
    
    logger.info(f"开始执行 '{task_name}'...")
    
    try:
        # ★★★ 从传入的 processor 对象中获取 config 字典 ★★★
        config = processor.config
        
        sync_handler = UnifiedSyncHandler(
            db_path=DB_PATH,
            emby_url=config.get("emby_server_url"),
            emby_api_key=config.get("emby_api_key"),
            emby_user_id=config.get("emby_user_id"),
            tmdb_api_key=config.get("tmdb_api_key", "")
        )
        
        # 调用同步方法，不再需要传递 is_full_sync
        sync_handler.sync_emby_person_map_to_db(
            update_status_callback=update_status_from_thread
        )
        
        logger.info(f"'{task_name}' 成功完成。")

    except Exception as e:
        logger.error(f"'{task_name}' 执行过程中发生严重错误: {e}", exc_info=True)
        update_status_from_thread(-1, f"错误：同步失败 ({str(e)[:50]}...)")

def task_manual_update(processor: MediaProcessorSA, item_id: str, manual_cast_list: list, item_name: str):
    """任务：使用手动编辑的结果处理媒体项"""
    processor.process_item_with_manual_cast(
        item_id=item_id,
        manual_cast_list=manual_cast_list,
        item_name=item_name
    )
# ★★★ 1. 定义一个webhoo专用追剧、用于编排任务的函数 ★★★
# 这个函数将作为提交到任务队列的目标
def webhook_processing_task(processor: MediaProcessorSA, item_id: str, force_reprocess: bool, process_episodes: bool):
    """
    【修复版】这个函数编排了处理新入库项目的完整流程。
    它的第一个参数现在是 MediaProcessor 实例，以匹配任务执行器的调用方式。
    """
    logger.info(f"Webhook 任务启动，处理项目: {item_id}")

    # 步骤 A: 获取完整的项目详情
    # ★★★ 修复：不再使用全局的 media_processor_instance，而是使用传入的 processor ★★★
    item_details = emby_handler.get_emby_item_details(
        item_id, 
        processor.emby_url, 
        processor.emby_api_key, 
        processor.emby_user_id
    )
    if not item_details:
        logger.error(f"Webhook 任务：无法获取项目 {item_id} 的详情，任务中止。")
        return

    # 步骤 B: 调用追剧判断
    # ★★★ 修复：使用传入的 processor ★★★
    processor.check_and_add_to_watchlist(item_details)

    # 步骤 C: 执行通用的元数据处理流程
    # ★★★ 修复：使用传入的 processor ★★★
    processor.process_single_item(
        item_id, 
        force_reprocess_this_item=force_reprocess, 
        process_episodes=process_episodes
    )
    
    logger.info(f"Webhook 任务完成: {item_id}")
# --- 追剧 ---    
def task_process_watchlist(processor: WatchlistProcessor):
    """
    任务：处理追剧列表。
    """
    # 不传递 item_id，执行全量更新
    processor.process_watching_list()

# ★★★ 核心修改：让这个任务函数调用改造后的方法 ★★★
def task_process_single_watchlist_item(processor: WatchlistProcessor, item_id: str):
    """任务：只更新追剧列表中的一个特定项目"""
    # 传递 item_id，执行单项更新
    processor.process_watching_list(item_id=item_id)
# ★★★ 导入映射表 ★★★
def task_import_person_map(processor, file_content: str, **kwargs):
    """
    【后台任务】从一个CSV文件字符串内容中，导入演员映射表。
    """
    task_name = "导入演员映射表"
    logger.info(f"后台任务 '{task_name}' 开始执行...")
    update_status_from_thread(0, "准备开始导入...")

    try:
        stream = StringIO(file_content, newline=None)
        
        # 先计算总行数用于进度条
        # 注意：这里我们不能消耗 stream，所以需要一种更聪明的方法
        # 一个简单的方法是，先完整读取，再创建 stream
        lines = file_content.splitlines()
        total_lines = len(lines) - 1 if len(lines) > 0 else 0
        
        # 重新创建 stream 用于 DictReader
        stream_for_reader = StringIO(file_content, newline=None)
        csv_reader = csv.DictReader(stream_for_reader)
        
        stats = {"total": total_lines, "processed": 0, "skipped": 0, "errors": 0}
        db_manager = ActorDBManager(DB_PATH)

        with db_manager._get_db_connection() as conn:
            cursor = conn.cursor()
            for i, row in enumerate(csv_reader):
                # ... (处理每一行的逻辑，和我们之前讨论的一样) ...
                person_data = {
                    "tmdb_id": row.get('tmdb_person_id'),
                    "emby_id": row.get('emby_person_id'),
                    "name": row.get('primary_name'),
                    # ...
                }
                # ...
                try:
                    db_manager.upsert_person(cursor, person_data)
                    stats["processed"] += 1
                except Exception as e_row:
                    stats["errors"] += 1
                
                # ★★★ 在循环中汇报进度 ★★★
                if i > 0 and i % 100 == 0 and update_status_from_thread and total_lines > 0:
                    progress = int(((i + 1) / total_lines) * 100)
                    update_status_from_thread(progress, f"正在导入... ({i+1}/{total_lines})")

        message = f"导入完成。总行数: {stats['total']}, 成功处理: {stats['processed']}..."
        logger.info(f"导入任务完成: {message}")
        update_status_from_thread(100, "导入完成！")

    except Exception as e:
        logger.error(f"后台导入任务失败: {e}", exc_info=True)
        update_status_from_thread(-1, f"导入失败: {e}")

# ★★★ 新任务函数 1: 重新处理单个项目 ★★★
def task_reprocess_single_item(processor: MediaProcessorSA, item_id: str):
    """
    后台任务：完整地重新处理单个项目。
    """
    item_name_for_log = f"ItemID: {item_id}"
    logger.info(f"--- 开始执行“重新处理单个项目”任务 ({item_name_for_log}) ---")
    
    try:
        # 1. 获取详情
        item_details = emby_handler.get_emby_item_details(item_id, processor.emby_url, processor.emby_api_key, processor.emby_user_id)
        if not item_details:
            logger.error(f"无法获取项目 {item_id} 的详情，任务中止。")
            update_status_from_thread(-1, f"获取 {item_id} 详情失败")
            return

        item_name_for_log = item_details.get("Name", item_name_for_log)
        update_status_from_thread(10, f"正在处理: {item_name_for_log}")

        # 2. 删除缓存
        tmdb_id = item_details.get("ProviderIds", {}).get("Tmdb")
        item_type = item_details.get("Type")
        override_path = get_override_path_for_item(item_type, tmdb_id)
        if override_path and os.path.exists(override_path):
            shutil.rmtree(override_path)
            logger.info(f"已为 '{item_name_for_log}' 删除缓存: {override_path}")
        update_status_from_thread(25, "缓存已清除")

        # 3. 触发刷新
        emby_handler.refresh_emby_item_metadata(item_id, processor.emby_url, processor.emby_api_key, replace_all_metadata_param=True)
        logger.info(f"已向 Emby 发送对 '{item_name_for_log}' 的刷新请求。")
        update_status_from_thread(40, "已触发Emby刷新，等待...")

        # 4. 等待 Emby 完成刷新
        time.sleep(60)
        update_status_from_thread(70, "Emby刷新完成，开始神医处理...")

        # 5. 调用 processor 的核心方法进行“神医”处理
        #    process_single_item 内部已经有完整的日志和状态更新
        processor.process_single_item(item_id, force_reprocess_this_item=True)

    except Exception as e:
        logger.error(f"重新处理 '{item_name_for_log}' 时发生严重错误: {e}", exc_info=True)
        update_status_from_thread(-1, f"重新处理失败: {e}")


# ★★★ 新任务函数 2: 重新处理所有待复核项 ★★★
def task_reprocess_all_review_items(processor: MediaProcessorSA):
    """
    后台任务：遍历所有待复核项并逐一重新处理。
    """
    logger.info("--- 开始执行“重新处理所有待复核项”任务 ---")
    try:
        with processor._get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT item_id FROM failed_log")
            all_item_ids = [row['item_id'] for row in cursor.fetchall()]
        
        total = len(all_item_ids)
        if total == 0:
            logger.info("待复核列表中没有项目，任务结束。")
            update_status_from_thread(100, "待复核列表为空。")
            return

        logger.info(f"共找到 {total} 个待复核项需要重新处理。")

        for i, item_id in enumerate(all_item_ids):
            if processor.is_stop_requested():
                logger.info("任务被中止。")
                break
            
            # 更新总体任务进度
            update_status_from_thread(int((i/total)*100), f"正在重新处理 {i+1}/{total}: {item_id}")
            
            # 直接调用单个重新处理的任务函数，复用逻辑
            # 注意：我们传递了 processor 和 item_id
            task_reprocess_single_item(processor, item_id)
            
            # 每个项目之间稍作停顿，避免请求过于频繁
            time.sleep(2) 

    except Exception as e:
        logger.error(f"重新处理所有待复核项时发生严重错误: {e}", exc_info=True)
        update_status_from_thread(-1, "任务失败")
# ★★★ 全量图片同步的任务函数 ★★★
def task_full_image_sync(processor: MediaProcessorSA):
    """
    后台任务：调用 processor 的方法来同步所有图片。
    """
    # 直接把回调函数传进去
    processor.sync_all_images(update_status_callback=update_status_from_thread)

# ★★★ 后台检查更新的线程目标函数 ★★★
def update_checker_task():
    """
    后台线程，定期检查更新。
    """
    # 应用启动时先等待一小会儿（比如60秒），避免和应用初始化抢占资源
    time.sleep(60) 
    
    while True:
        try:
            has_update, latest_version = utils.check_for_updates(
                constants.APP_VERSION, 
                constants.GITHUB_REPO
            )
            update_info["has_update"] = has_update
            update_info["latest_version"] = latest_version
            update_info["error"] = None # 成功后清空错误信息
        except Exception as e:
            # 如果检查更新时发生异常，记录下来
            logger.error(f"后台更新检查线程发生错误: {e}", exc_info=True)
            update_info["has_update"] = False
            update_info["error"] = str(e)

        # 每隔 6 小时检查一次
        time.sleep(6 * 60 * 60)
# --- 路由区 ---
# --- webhook通知任务 ---
@app.route('/webhook/emby', methods=['POST'])
def emby_webhook():
    data = request.json
    event_type = data.get("Event") if data else "未知事件"
    logger.info(f"收到Emby Webhook: {event_type}")
    
    trigger_events = ["item.add", "library.new"] 

    if event_type not in trigger_events:
        logger.info(f"Webhook事件 '{event_type}' 不在触发列表 {trigger_events} 中，将被忽略。")
        return jsonify({"status": "event_ignored_not_in_trigger_list"}), 200

    item_from_webhook = data.get("Item", {}) if data else {}
    original_item_id = item_from_webhook.get("Id")
    original_item_name = item_from_webhook.get("Name", "未知项目")
    original_item_type = item_from_webhook.get("Type")
    
    # 我们关心的类型是电影、剧集和分集
    trigger_types = ["Movie", "Series", "Episode"]

    if not (original_item_id and original_item_type in trigger_types):
        logger.debug(f"Webhook事件 '{event_type}' (项目: {original_item_name}, 类型: {original_item_type}) 被忽略（缺少ID或类型不匹配）。")
        return jsonify({"status": "event_ignored_no_id_or_wrong_type"}), 200

    if not media_processor_instance:
        logger.error(f"Webhook 任务 '{original_item_name}' 无法处理：MediaProcessor 未初始化。")
        return jsonify({"error": "Core processor not ready"}), 503

    # ★★★ 核心修复逻辑 START ★★★
    
    id_to_process = original_item_id
    type_to_process = original_item_type

    # 1. 如果是分集，向上查找剧集ID
    if original_item_type == "Episode":
        logger.info(f"Webhook 收到分集 '{original_item_name}' (ID: {original_item_id})，正在向上查找其所属剧集...")
        series_id = emby_handler.get_series_id_from_child_id(
            original_item_id,
            media_processor_instance.emby_url,
            media_processor_instance.emby_api_key,
            media_processor_instance.emby_user_id
        )
        if series_id:
            id_to_process = series_id
            type_to_process = "Series" # 明确类型为剧集
            logger.info(f"成功找到所属剧集 ID: {id_to_process}。将处理此剧集。")
        else:
            logger.error(f"无法为分集 '{original_item_name}' 找到所属剧集ID，将跳过处理。")
            return jsonify({"status": "event_ignored_series_not_found"}), 200

    # 2. 无论最初是什么类型，都用最终确定的ID重新获取一次完整的项目详情
    logger.info(f"准备重新获取项目 {id_to_process} 的最新、最完整的元数据...")
    full_item_details = emby_handler.get_emby_item_details(
        item_id=id_to_process,
        emby_server_url=media_processor_instance.emby_url,
        emby_api_key=media_processor_instance.emby_api_key,
        user_id=media_processor_instance.emby_user_id
    )

    if not full_item_details:
        logger.error(f"无法获取项目 {id_to_process} 的完整详情，处理中止。")
        return jsonify({"status": "event_ignored_details_fetch_failed"}), 200

    # 3. 从新获取的详情中提取最终的名称和TMDb ID
    final_item_name = full_item_details.get("Name", f"未知项目(ID:{id_to_process})")
    provider_ids = full_item_details.get("ProviderIds", {})
    tmdb_id = provider_ids.get("Tmdb")

    # 4. 在提交到队列前，做最后一次检查
    if not tmdb_id:
        logger.warning(f"项目 '{final_item_name}' (ID: {id_to_process}) 缺少 TMDb ID，无法进行处理。这可能是因为 Emby 尚未完成对该项目的元数据刮削。将跳过本次 Webhook 请求。")
        return jsonify({"status": "event_ignored_no_tmdb_id"}), 200
        
    # ★★★ 核心修复逻辑 END ★★★

    logger.info(f"Webhook事件触发，最终处理项目 '{final_item_name}' (ID: {id_to_process}, TMDbID: {tmdb_id}) 已提交到任务队列。")
    
    # ★★★ 核心修改点在这里 ★★★
    # 使用最终确定的信息，提交我们新的“编排任务”到队列
    submit_task_to_queue(
        webhook_processing_task,  # <--- 目标函数是这个，而不是 process_single_item
        f"Webhook处理: {final_item_name}",
        # --- 传递给 webhook_processing_task 的参数 ---
        id_to_process,
        force_reprocess=True, 
        process_episodes=True
    )
    
    return jsonify({"status": "task_queued", "item_id": id_to_process}), 202
@app.route('/trigger_sync_person_map', methods=['POST'])
def trigger_sync_person_map(): # WebUI 用的
    # ... (你的 if not media_processor_instance 和 if task_lock.locked() 检查逻辑不变) ...

    task_name = "同步Emby演员映射表 (WebUI)"
    logger.info(f"收到手动触发 '{task_name}' 的请求。")

    submit_task_to_queue(
        task_sync_person_map,
        task_name
    )

    flash(f"'{task_name}' 任务已在后台启动。", "info")
    return redirect(url_for('settings_page'))

@app.route('/trigger_stop_task', methods=['POST'])
def trigger_stop_task():
    global background_task_status
    if media_processor_instance and hasattr(media_processor_instance, 'signal_stop'):
        media_processor_instance.signal_stop()
        flash("已发送停止后台任务的请求。任务将在当前步骤完成后停止。", "info")
        background_task_status["message"] = "正在尝试停止任务..."
    else:
        flash("错误：服务未就绪或不支持停止操作。", "error")
    return redirect(url_for('settings_page'))

@app.route('/status')
def get_status():
    return jsonify(background_task_status)

#--- 日志 ---
@app.route('/api/status', methods=['GET'])
def api_get_task_status():
    global background_task_status, frontend_log_queue
    
    status_data = background_task_status.copy() # 复制一份，避免线程问题
    
    # 将日志队列的内容添加到返回数据中
    status_data['logs'] = list(frontend_log_queue)
    
    return jsonify(status_data)

@app.route('/api/emby_libraries')
def api_get_emby_libraries():
    # 确保 media_processor_instance 已初始化并且 Emby 配置有效
    if not media_processor_instance or \
       not media_processor_instance.emby_url or \
       not media_processor_instance.emby_api_key:
        logger.warning("/api/emby_libraries: Emby配置不完整或服务未就绪。")
        return jsonify({"error": "Emby配置不完整或服务未就绪"}), 500

    libraries = emby_handler.get_emby_libraries(
        media_processor_instance.emby_url,
        media_processor_instance.emby_api_key,
        media_processor_instance.emby_user_id # 传递 user_id
    )

    if libraries is not None: # get_emby_libraries 成功返回列表 (可能为空列表)
        return jsonify(libraries)
    else: # get_emby_libraries 返回了 None，表示获取失败
        logger.error("/api/emby_libraries: 无法获取Emby媒体库列表 (emby_handler返回None)。")
        return jsonify({"error": "无法获取Emby媒体库列表，请检查Emby连接和日志"}), 500

# --- 应用退出处理 ---
def application_exit_handler():
    global media_processor_instance, scheduler, task_worker_thread
    logger.info("应用程序正在退出 (atexit)，执行清理操作...")

    # 1. 立刻通知当前正在运行的任务停止
    if media_processor_instance:
        logger.info("正在发送停止信号给当前任务...")
        media_processor_instance.signal_stop()

    # 2. 清空任务队列，丢弃所有排队中的任务
    if not task_queue.empty():
        logger.info(f"队列中还有 {task_queue.qsize()} 个任务，正在清空...")
        while not task_queue.empty():
            try:
                task_queue.get_nowait()
            except Queue.Empty:
                break
        logger.info("任务队列已清空。")

    # 3. 停止工人线程
    if task_worker_thread and task_worker_thread.is_alive():
        logger.info("正在发送停止信号给任务工人线程...")
        task_queue.put(None) # 发送“毒丸”
        task_worker_thread.join(timeout=5) # 等待线程退出
        if task_worker_thread.is_alive():
            logger.warning("任务工人线程在5秒内未能正常退出。")
        else:
            logger.info("任务工人线程已成功停止。")

    # 4. 关闭其他资源
    if media_processor_instance:
        media_processor_instance.close()
    if scheduler and scheduler.running:
        scheduler.shutdown(wait=False)
    
    logger.info("atexit 清理操作执行完毕。")

atexit.register(application_exit_handler)
# --- 应用退出处理结束 ---

# --- API 端点 搜索媒体库 ---
@app.route('/api/search_emby_library', methods=['GET'])
def api_search_emby_library():
    query = request.args.get('query', '')
    if not query.strip():
        return jsonify({"error": "搜索词不能为空"}), 400

    if not media_processor_instance:
        return jsonify({"error": "核心处理器未就绪"}), 503

    try:
        # ✨✨✨ 调用改造后的函数，并传入 search_term ✨✨✨
        search_results = emby_handler.get_emby_library_items(
            base_url=media_processor_instance.emby_url,
            api_key=media_processor_instance.emby_api_key,
            user_id=media_processor_instance.emby_user_id,
            media_type_filter="Movie,Series", # 搜索时指定类型
            search_term=query # <--- 传递搜索词
        )
        
        if search_results is None:
            return jsonify({"error": "搜索时发生服务器错误"}), 500

        # 将搜索结果转换为前端表格期望的格式 (这部分逻辑不变)
        formatted_results = []
        for item in search_results:
            formatted_results.append({
                "item_id": item.get("Id"),
                "item_name": item.get("Name"),
                "item_type": item.get("Type"),
                "failed_at": None,
                "error_message": f"来自 Emby 库的搜索结果 (年份: {item.get('ProductionYear', 'N/A')})",
                "score": None,
                # ★★★ 核心修复：把 ProviderIds 也传递给前端 ★★★
                "provider_ids": item.get("ProviderIds") 
            })
        
        return jsonify({
            "items": formatted_results,
            "total_items": len(formatted_results)
        })

    except Exception as e:
        logger.error(f"API /api/search_emby_library Error: {e}", exc_info=True)
        return jsonify({"error": "搜索时发生未知服务器错误"}), 500
# --- 认证 API 端点 ---
@app.route('/api/auth/status', methods=['GET'])
def auth_status():
    """检查当前认证状态"""
    config, _ = load_config()
    auth_enabled = config.get(constants.CONFIG_OPTION_AUTH_ENABLED, False)
    
    response = {
        "auth_enabled": auth_enabled,
        "logged_in": 'user_id' in session,
        "username": session.get('username')
    }
    return jsonify(response)

@app.route('/api/auth/login', methods=['POST'])
def login():
    data = request.json
    username_from_req = data.get('username')
    password_from_req = data.get('password')

    if not username_from_req or not password_from_req:
        return jsonify({"error": "缺少用户名或密码"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE username = ?", (username_from_req,))
    user = cursor.fetchone()
    conn.close()

    if user and check_password_hash(user['password_hash'], password_from_req):
        session['user_id'] = user['id']
        session['username'] = user['username']
        logger.info(f"用户 '{user['username']}' 登录成功。")
        return jsonify({"message": "登录成功", "username": user['username']})
    
    logger.warning(f"用户 '{username_from_req}' 登录失败：用户名或密码错误。")
    return jsonify({"error": "用户名或密码错误"}), 401

@app.route('/api/auth/logout', methods=['POST'])
def logout():
    username = session.get('username', '未知用户')
    session.clear()
    logger.info(f"用户 '{username}' 已注销。")
    return jsonify({"message": "注销成功"})

# --- 认证 API 端点结束 ---
@app.route('/api/auth/change_password', methods=['POST'])
@login_required
def change_password():
    data = request.json
    current_password = data.get('current_password')
    new_password = data.get('new_password')

    if not current_password or not new_password:
        return jsonify({"error": "缺少当前密码或新密码"}), 400
    
    if len(new_password) < 6:
        return jsonify({"error": "新密码长度不能少于6位"}), 400

    user_id = session.get('user_id')
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    user = cursor.fetchone()

    if not user or not check_password_hash(user['password_hash'], current_password):
        conn.close()
        logger.warning(f"用户 '{session.get('username')}' 修改密码失败：当前密码不正确。")
        return jsonify({"error": "当前密码不正确"}), 403

    # 更新密码
    new_password_hash = generate_password_hash(new_password)
    cursor.execute("UPDATE users SET password_hash = ? WHERE id = ?", (new_password_hash, user_id))
    conn.commit()
    conn.close()

    logger.info(f"用户 '{user['username']}' 成功修改密码。")
    return jsonify({"message": "密码修改成功"})
# --- API 端点：获取当前配置 ---
@app.route('/api/config', methods=['GET'])
def api_get_config():
    try:
        # ★★★ 确保这里正确解包了元组 ★★★
        current_config, _ = load_config() 
        
        if current_config:
            logger.info(f"API /api/config (GET): 成功加载并返回配置。")
            return jsonify(current_config)
        else:
            logger.error(f"API /api/config (GET): load_config() 返回为空或None。")
            return jsonify({"error": "无法加载配置数据"}), 500
    except Exception as e:
        logger.error(f"API /api/config (GET) 获取配置时发生错误: {e}", exc_info=True)
        return jsonify({"error": "获取配置信息时发生服务器内部错误"}), 500
        logger.error(f"API /api/config (GET) 获取配置时发生错误: {e}", exc_info=True)
        return jsonify({"error": "获取配置信息时发生服务器内部错误"}), 500

# --- API 端点：保存配置 ---
@app.route('/api/config', methods=['POST'])
def api_save_config():
    try:
        new_config_data = request.json
        if not new_config_data:
            return jsonify({"error": "请求体中未包含配置数据"}), 400
        
        # ★★★ 核心修改：在这里进行严格校验并“打回去” ★★★
        user_id_to_save = new_config_data.get("emby_user_id", "").strip()

        # 规则1：检查是否为空
        if not user_id_to_save:
            error_message = "Emby User ID 不能为空！这是获取媒体库列表的必需项。"
            logger.warning(f"API /api/config (POST): 拒绝保存，原因: {error_message}")
            return jsonify({"error": error_message}), 400

        # 规则2：检查格式是否正确
        if not re.match(r'^[a-f0-9]{32}$', user_id_to_save, re.I):
            error_message = "Emby User ID 格式不正确！它应该是一串32位的字母和数字。"
            logger.warning(f"API /api/config (POST): 拒绝保存，原因: {error_message} (输入值: '{user_id_to_save}')")
            return jsonify({"error": error_message}), 400
        # ★★★ 校验结束 ★★★

        logger.info(f"API /api/config (POST): 收到新的配置数据，准备保存...")
        
        # 校验通过后，才调用保存函数
        save_config(new_config_data) 
        
        logger.debug("API /api/config (POST): 配置已成功传递给 save_config 函数。")
        return jsonify({"message": "配置已成功保存并已触发重新加载。"})
        
    except Exception as e:
        logger.error(f"API /api/config (POST) 保存配置时发生错误: {e}", exc_info=True)
        return jsonify({"error": f"保存配置时发生服务器内部错误: {str(e)}"}), 500

# --- API 端点：获取待复核列表 ---
@app.route('/api/review_items', methods=['GET'])
def api_get_review_items():
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    query_filter = request.args.get('query', '', type=str).strip()

    if page < 1: page = 1
    if per_page < 1: per_page = 10
    if per_page > 100: per_page = 100

    offset = (page - 1) * per_page
    
    items_to_review = []
    total_matching_items = 0
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # --- 终极简化：移除所有 error_message 筛选 ---
        # 既然 failed_log 表中的所有记录都需要复核，我们就不需要任何关键词过滤了。
        
        where_clause = ""
        sql_params = []

        # 只在用户输入搜索词时才添加 WHERE 条件
        if query_filter:
            where_clause = "WHERE item_name LIKE ?"
            sql_params.append(f"%{query_filter}%")
        # --- 简化结束 ---

        # 1. 查询总数 (WHERE 条件要么为空，要么是按名称搜索)
        count_sql = f"SELECT COUNT(*) FROM failed_log {where_clause}"
        cursor.execute(count_sql, tuple(sql_params))
        count_row = cursor.fetchone()
        if count_row:
            total_matching_items = count_row[0]

        # --- 在这里加上确认日志 ---
        print(f"DATABASE CHECK: Found a total of {total_matching_items} items.")
        logger.debug(f"DATABASE CHECK: Found a total of {total_matching_items} items.")
        # --- -------------------- ---

        # 2. 查询当前页的数据
        items_sql = f"""
            SELECT item_id, item_name, failed_at, error_message, item_type, score 
            FROM failed_log 
            {where_clause}
            ORDER BY failed_at DESC 
            LIMIT ? OFFSET ?
        """
        # 将分页参数添加到参数列表
        params_for_page_query = sql_params + [per_page, offset]
        cursor.execute(items_sql, tuple(params_for_page_query))
        fetched_rows = cursor.fetchall()
        
        for row in fetched_rows:
            items_to_review.append(dict(row))

    except Exception as e:
        logger.error(f"API /api/review_items 获取数据失败: {e}", exc_info=True)
        return jsonify({"error": "获取待复核列表时发生服务器内部错误"}), 500
    finally:
        if conn:
            conn.close()
            
    total_pages = (total_matching_items + per_page - 1) // per_page if total_matching_items > 0 else 0
    
    logger.debug(f"API /api/review_items: 返回 {len(items_to_review)} 条待复核项目 (总计: {total_matching_items}, 第 {page}/{total_pages} 页)")
    return jsonify({
        "items": items_to_review,
        "total_items": total_matching_items,
        "total_pages": total_pages,
        "current_page": page,
        "per_page": per_page,
        "query": query_filter
    })
@app.route('/api/actions/mark_item_processed/<item_id>', methods=['POST'])
def api_mark_item_processed(item_id):
    if task_lock.locked():
        return jsonify({"error": "后台有长时间任务正在运行，请稍后再试。"}), 409
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 从 failed_log 获取信息，以便存入 processed_log
        cursor.execute("SELECT item_name, item_type, score FROM failed_log WHERE item_id = ?", (item_id,))
        failed_item_info = cursor.fetchone()
        
        cursor.execute("DELETE FROM failed_log WHERE item_id = ?", (item_id,))
        deleted_count = cursor.rowcount
        
        if deleted_count > 0 and failed_item_info:
            # 标记为已处理时，可以给一个特殊的高分或标记
            # 或者，如果原始评分存在，就用那个评分
            score_to_save = failed_item_info["score"] if failed_item_info["score"] is not None else 10.0 # 假设手动处理给10分
            item_name = failed_item_info["item_name"]
            
            # 添加到 processed_log
            # 假设 MediaProcessor 实例在这里不可用，我们直接操作数据库
            # 如果 MediaProcessor 可用，调用其 save_to_processed_log 方法更好
            cursor.execute(
                "REPLACE INTO processed_log (item_id, item_name, processed_at, score) VALUES (?, ?, CURRENT_TIMESTAMP, ?)",
                (item_id, item_name, score_to_save)
            )
            logger.info(f"项目 {item_id} ('{item_name}') 已标记为已处理，并移至已处理日志 (评分: {score_to_save})。")

        conn.commit()
        conn.close()
        
        if deleted_count > 0:
            return jsonify({"message": f"项目 {item_id} 已成功标记为已处理。"}), 200
        else:
            return jsonify({"error": f"未在待复核列表中找到项目 {item_id}。"}), 404
    except Exception as e:
        logger.error(f"标记项目 {item_id} 为已处理时失败: {e}", exc_info=True)
        return jsonify({"error": "服务器内部错误"}), 500
# --- 前端全量扫描接口 ---   
@app.route('/api/trigger_full_scan', methods=['POST'])
def api_handle_trigger_full_scan():
    logger.debug("API Endpoint: Received request to trigger full scan.")
    
    # 检查任务锁
    if task_lock.locked():
        return jsonify({"error": "后台有任务正在运行，请稍后再试。"}), 409

    if not media_processor_instance:
        return jsonify({"error": "核心处理器未就绪"}), 503

    # 从 FormData 获取数据
    # 注意：前端发送的是 FormData，所以我们用 request.form
    force_reprocess = request.form.get('force_reprocess_all') == 'on'
    
    # ★★★ 您的完美逻辑在这里实现 ★★★
    if force_reprocess:
        logger.info("API: 检测到“强制重处理”选项，将在任务开始前清空已处理日志。")
        try:
            media_processor_instance.clear_processed_log()
            logger.info("API: 已处理日志已成功清除。")
        except Exception as e:
            logger.error(f"API: 清空已处理日志时发生错误: {e}")
            return jsonify({"error": f"清空日志失败: {e}"}), 500

    # 准备任务参数
    action_message = "全量媒体库扫描"
    if force_reprocess:
        action_message += " (已清空日志)"

    # 从全局配置获取处理深度
    process_episodes = APP_CONFIG.get('process_episodes', True)
    
    # 提交纯粹的扫描任务
    submit_task_to_queue(
        task_process_full_library, # 调用简化后的任务函数
        action_message,
        process_episodes # 不再需要传递 force_reprocess
    )
    
    return jsonify({"message": f"{action_message} 任务已提交启动。"}), 202
# --- 同步演员映射表 ---
@app.route('/api/trigger_sync_person_map', methods=['POST'])
@login_required
def api_handle_trigger_sync_map():
    logger.debug("API: 收到触发统一演员映射表同步的请求。")
    try:
        # ★★★ 核心修复：不再需要 full_sync，因为同步逻辑已经统一 ★★★
        task_name_for_api = "同步演员映射表"
        
        submit_task_to_queue(
            task_sync_person_map,
            task_name_for_api
        )

        return jsonify({"message": f"'{task_name_for_api}' 任务已提交启动。"}), 202
    except Exception as e:
        logger.error(f"API /api/trigger_sync_person_map error: {e}", exc_info=True)
        return jsonify({"error": "启动同步映射表时发生服务器内部错误"}), 500

@app.route('/api/trigger_stop_task', methods=['POST'])
def api_handle_trigger_stop_task():
    logger.debug("API Endpoint: Received request to stop current task.")
    if media_processor_instance:
        media_processor_instance.signal_stop()
        logger.info("已发送停止信号给当前正在运行的任务。")
        return jsonify({"message": "已发送停止任务请求。"}), 200
    else:
        logger.warning("API: MediaProcessor 未初始化，无法发送停止信号。")
        return jsonify({"error": "核心处理器未就绪"}), 503
    
# ✨✨✨ 神医保存手动编辑结果的 API ✨✨✨
@app.route('/api/update_media_cast_sa/<item_id>', methods=['POST'])
@login_required
def api_update_edited_cast_sa(item_id):
    if not media_processor_instance:
        return jsonify({"error": "核心处理器未就绪"}), 503
    
    data = request.json
    if not data or "cast" not in data or not isinstance(data["cast"], list):
        return jsonify({"error": "请求体中缺少有效的 'cast' 列表"}), 400
    
    edited_cast = data["cast"]
    item_name = data.get("item_name", f"未知项目(ID:{item_id})")

    submit_task_to_queue(
        task_manual_update, # 传递包装函数
        f"手动更新: {item_name}",
        # --- 后面是传递给 task_manual_update 的参数 ---
        item_id,
        edited_cast,
        item_name
    )
    
    return jsonify({"message": "手动更新任务已在后台启动。"}), 202
@app.route('/api/update_media_cast_api/<item_id>', methods=['POST'])
@login_required
def api_update_edited_cast_api(item_id):
    if not media_processor_instance:
        return jsonify({"error": "核心处理器未就绪"}), 503
    
    try:
        data = request.json
        if not data or "cast" not in data or not isinstance(data["cast"], list):
            return jsonify({"error": "请求体中缺少有效的 'cast' 列表"}), 400

        edited_cast_from_frontend = data["cast"]
        logger.info(f"API: 收到为 ItemID {item_id} 更新演员的请求，共 {len(edited_cast_from_frontend)} 位演员。")

        # ======================================================================
        # ✨✨✨ 逻辑变更：不再自己处理，而是交给 core_processor ✨✨✨
        # ======================================================================

        # 1. 更新翻译缓存 (这部分逻辑可以保留在 API 层)
        # (您现有的更新翻译缓存的代码可以放在这里，我暂时省略以保持清晰)
        # ... 您更新翻译缓存的代码 ...
        
        # 2. 准备传递给核心处理器的数据 (emby_handler 期望的格式)
        cast_for_processor = []
        for actor_frontend in edited_cast_from_frontend:
            entry = {
                "emby_person_id": str(actor_frontend.get("embyPersonId", "")).strip(),
                "name": str(actor_frontend.get("name", "")).strip(),
                "character": str(actor_frontend.get("role", "")).strip(),
                "provider_ids": {} # 您可以根据需要填充这个
            }
            cast_for_processor.append(entry)

        # 3. 调用新的核心处理函数
        process_success = media_processor_instance.process_item_with_manual_cast(
            item_id=item_id,
            manual_cast_list=cast_for_processor
        )

        # 4. 根据处理结果返回响应
        if process_success:
            logger.info(f"API: ItemID {item_id} 的手动更新流程已成功执行。")
            # 更新成功后，也应该更新处理日志
            item_name_for_log = data.get("item_name", f"未知项目(ID:{item_id})")
            media_processor_instance._remove_from_failed_log_if_exists(item_id)
            media_processor_instance.save_to_processed_log(item_id, item_name_for_log, score=10.0) # 手动处理给满分
            return jsonify({"message": "演员信息已成功更新，并执行了完整的处理流程。"}), 200
        else:
            logger.error(f"API: 核心处理器未能成功处理 ItemID {item_id} 的手动更新。")
            return jsonify({"error": "核心处理器执行手动更新失败，请检查后端日志。"}), 500

    except Exception as e:
        logger.error(f"API /api/update_media_cast CRITICAL Error for {item_id}: {e}", exc_info=True)
        return jsonify({"error": "保存演员信息时发生服务器内部错误"}), 500
# ★★★ 导出演员映射表 ★★★
@app.route('/api/actors/export', methods=['GET'])
@login_required
def api_export_person_map():
    """
    【统一版】导出演员身份映射表 (person_identity_map)。
    """
    table_name = 'person_identity_map'
    # 定义统一的、最完整的表头顺序
    headers = [
        'map_id', 'primary_name', 'emby_person_id', 
        'tmdb_person_id', 'imdb_id', 'douban_celebrity_id'
    ]
    logger.info(f"API: 收到导出演员映射表 '{table_name}' 的请求。")

    def generate_csv():
        string_io = StringIO()
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                writer = csv.DictWriter(string_io, fieldnames=headers, extrasaction='ignore')
                
                writer.writeheader()
                yield string_io.getvalue()
                string_io.seek(0); string_io.truncate(0)

                cursor.execute(f"SELECT {', '.join(headers)} FROM {table_name}")
                
                for row in cursor:
                    writer.writerow(dict(row))
                    yield string_io.getvalue()
                    string_io.seek(0); string_io.truncate(0)
        except Exception as e:
            logger.error(f"导出映射表时发生错误: {e}", exc_info=True)
            yield f"Error: {e}"

    timestamp = time.strftime("%Y%m%d-%H%M%S")
    filename = f"person_identity_map_backup_{timestamp}.csv"
    
    response = Response(stream_with_context(generate_csv()), mimetype='text/csv; charset=utf-8')
    response.headers.set("Content-Disposition", "attachment", filename=filename)
    return response
# ★★★ 导入演员映射表 ★★★
@app.route('/api/actors/import', methods=['POST'])
@login_required
def api_import_person_map():
    """
    【队列版】接收上传的CSV文件，读取内容，并提交一个后台任务来处理它。
    """
    if task_lock.locked():
        return jsonify({"error": "后台已有任务在运行，请稍后再试。"}), 409

    if 'file' not in request.files:
        return jsonify({"error": "请求中未找到文件部分"}), 400
    
    file = request.files['file']
    if not file.filename or not file.filename.endswith('.csv'):
        return jsonify({"error": "未选择文件或文件类型不正确"}), 400

    try:
        # 1. 直接将文件内容读入内存字符串
        file_content = file.stream.read().decode("utf-8-sig")
        logger.info(f"已接收上传文件 '{file.filename}'，内容长度: {len(file_content)}")

        # 2. 提交一个后台任务，把文件内容和需要的配置传过去
        submit_task_to_queue(
            task_import_person_map,
            "导入演员映射表",
            # ★★★ 把任务需要的所有东西，都作为关键字参数传递 ★★★
            file_content=file_content,
            tmdb_api_key=app.config.get("tmdb_api_key", "")
        )
        
        return jsonify({"message": "文件上传成功，已提交到后台队列进行导入。"}), 202

    except Exception as e:
        logger.error(f"处理导入文件请求时发生错误: {e}", exc_info=True)
        return jsonify({"error": f"处理上传文件时发生服务器错误"}), 500
# ✨✨✨ 神医编辑页面的API接口 ✨✨✨
@app.route('/api/media_for_editing_sa/<item_id>', methods=['GET'])
@login_required
def api_get_media_for_editing_sa(item_id):
    if not media_processor_instance:
        return jsonify({"error": "核心处理器未就绪"}), 503

    # 直接调用 core_processor 的新方法
    data_for_editing = media_processor_instance.get_cast_for_editing(item_id)
    
    if data_for_editing:
        return jsonify(data_for_editing)
    else:
        return jsonify({"error": f"无法获取项目 {item_id} 的编辑数据，请检查日志。"}), 404
# ✨✨✨ 普通编辑页面的API接口 ✨✨✨
@app.route('/api/media_api_edit_details/<item_id>', methods=['GET'])
@login_required # ✨ 2. 确保依赖存在
def api_get_media_for_editing_api(item_id):
    logger.info(f"API: 收到为 ItemID {item_id} 获取编辑详情的请求。")

    if not media_processor_instance:
        return jsonify({"error": "核心处理器未就绪"}), 503

    # 1. 从 Emby 获取详情 (保持不变)
    try:
        emby_details = emby_handler.get_emby_item_details(
            item_id,
            media_processor_instance.emby_url,
            media_processor_instance.emby_api_key,
            media_processor_instance.emby_user_id
        )
        if not emby_details:
            return jsonify({"error": f"在Emby中未找到项目 {item_id}"}), 404
    except Exception as e:
        logger.error(f"API: 获取Emby详情失败 for ItemID {item_id}: {e}", exc_info=True)
        return jsonify({"error": "获取Emby详情时发生服务器错误"}), 500

    # 2. 从 failed_log 获取额外信息 (保持不变)
    conn = None
    failed_log_info = {}
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT item_name, item_type, error_message, score FROM failed_log WHERE item_id = ?", (item_id,))
        row = cursor.fetchone()
        if row:
            failed_log_info = dict(row)
    finally:
        if conn:
            conn.close()

    # ★★★ START: 3. 格式化并丰富演员列表 (关键修改) ★★★
    enriched_cast_for_frontend = []
    if emby_details.get("People"):
        for person in emby_details.get("People", []):
            person_id = person.get("Id")
            if person_id and person.get("Name"):
                provider_ids = person.get("ProviderIds", {})
                
                # 为每个演员单独获取其详细信息以获得 image_tag
                actor_image_tag = person.get('PrimaryImageTag')
                try:
                    # 调用您现有的 emby_handler 方法来获取演员详情
                    fields_to_request = "People,ProviderIds,PrimaryImageAspectRatio,Path,OriginalTitle,DateCreated,PremiereDate,ProductionYear,Overview,CommunityRating,OfficialRating,Genres,Studios,Taglines"
    
                    # 调用 emby_handler 的函数，并把 fields 参数传进去
                    emby_details = emby_handler.get_emby_item_details(
                        item_id,
                        media_processor_instance.emby_url,
                        media_processor_instance.emby_api_key,
                        media_processor_instance.emby_user_id,
                        fields=fields_to_request  # <--- 把我们需要的字段列表传给它
                    )
                    if not emby_details:
                        return jsonify({"error": f"在Emby中未找到项目 {item_id}"}), 404
                except Exception as e:
                    logger.error(f"API: 获取Emby详情失败 for ItemID {item_id}: {e}", exc_info=True)
                    return jsonify({"error": "获取Emby详情时发生服务器错误"}), 500
                
                enriched_cast_for_frontend.append({
                    "embyPersonId": str(person_id),
                    "name": person["Name"],
                    "role": person.get("Role", ""),
                    "imdbId": provider_ids.get("Imdb"),
                    "doubanId": provider_ids.get("Douban"),
                    "tmdbId": provider_ids.get("Tmdb"),
                    "image_tag": actor_image_tag  # 加入演员的 image_tag
                })
    # ★★★ END: 3. ★★★

    # 4. 生成搜索链接 (保持不变)
    google_search_for_wiki_url = None
    title = emby_details.get("Name")
    year = emby_details.get("ProductionYear")
    
    if title:
        logger.info(f"准备为标题 '{title}' (年份: {year}) 生成搜索链接。")
        try:
            google_search_for_wiki_url = utils.generate_search_url('wikipedia', title, year)
            if google_search_for_wiki_url:
                logger.info(f"成功生成搜索链接: {google_search_for_wiki_url}")
            else:
                logger.warning(f"generate_search_url 函数为标题 '{title}' 返回了一个空值或 None。")
        except Exception as e:
            logger.error(f"为 '{title}' 生成维基百科搜索链接时发生异常: {e}", exc_info=True)
    else:
        logger.warning("无法生成搜索链接，因为 Emby 详情中缺少标题 (Name)。")

    # ★★★ START: 5. 组合最终的响应数据 (关键修改) ★★★
    response_data = {
        "item_id": item_id,
        "item_name": emby_details.get("Name"),
        "item_type": emby_details.get("Type"),
        
        # 加入媒体海报的 image_tag
        "image_tag": emby_details.get('ImageTags', {}).get('Primary'),
        
        "original_score": failed_log_info.get("score"),
        "review_reason": failed_log_info.get("error_message"),
        
        # 使用我们新生成的、包含 image_tag 的演员列表
        "current_emby_cast": enriched_cast_for_frontend,
        
        "search_links": {
            "google_search_wiki": google_search_for_wiki_url
        }
    }
    # ★★★ END: 5. ★★★
    
    logger.info(f"API: 成功为项目 {item_id} 构建编辑详情，准备返回。")
    return jsonify(response_data)

# ✨✨✨   生成外部搜索链接 ✨✨✨
@app.route('/api/parse_cast_from_url', methods=['POST'])
def api_parse_cast_from_url():
    # 检查 web_parser 是否可用
    try:
        from web_parser import parse_cast_from_url, ParserError
    except ImportError:
        return jsonify({"error": "网页解析功能在服务器端不可用。"}), 501

    data = request.json
    url_to_parse = data.get('url')
    if not url_to_parse:
        return jsonify({"error": "请求中未提供 'url' 参数"}), 400

    try:
        current_config, _ = load_config()
        headers = {'User-Agent': current_config.get('user_agent', '')}
        parsed_cast = parse_cast_from_url(url_to_parse, custom_headers=headers)
        
        frontend_cast = [{"name": item['actor'], "role": item['character']} for item in parsed_cast]
        return jsonify(frontend_cast)

    except ParserError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"解析 URL '{url_to_parse}' 时发生未知错误: {e}", exc_info=True)
        return jsonify({"error": "解析时发生未知的服务器错误"}), 500
# ✨✨✨ 神医一键翻译 ✨✨✨
@app.route('/api/actions/translate_cast_sa', methods=['POST']) # 注意路径不同
@login_required
def api_translate_cast_sa():
    if not media_processor_instance:
        return jsonify({"error": "核心处理器未就绪"}), 503
        
    data = request.json
    current_cast = data.get('cast')
    if not isinstance(current_cast, list):
        return jsonify({"error": "请求体必须包含 'cast' 列表。"}), 400

    try:
        # 调用 core_processor 的新方法
        translated_list = media_processor_instance.translate_cast_list_for_editing(current_cast)
        return jsonify(translated_list)
    except Exception as e:
        logger.error(f"一键翻译演员列表时发生错误: {e}", exc_info=True)
        return jsonify({"error": "服务器在翻译时发生内部错误。"}), 500
# ✨✨✨ 普通一键翻译 ✨✨✨
@app.route('/api/actions/translate_cast_api', methods=['POST']) # 注意路径不同
@login_required
def api_translate_cast_api():
    if not media_processor_instance:
        return jsonify({"error": "核心处理器未就绪"}), 503
        
    data = request.json
    current_cast = data.get('cast')

    if not isinstance(current_cast, list):
        return jsonify({"error": "请求体必须包含 'cast' 列表。"}), 400

    try:
        # 调用新的、功能更全的翻译方法
        translated_list = media_processor_instance.translate_cast_list(current_cast)
        return jsonify(translated_list)
        
    except Exception as e:
        logger.error(f"一键翻译演员列表时发生错误: {e}", exc_info=True)
        return jsonify({"error": "服务器在翻译时发生内部错误。"}), 500
# ✨✨✨ 预览处理后的演员表 ✨✨✨
@app.route('/api/preview_processed_cast/<item_id>', methods=['POST'])
def api_preview_processed_cast(item_id):
    """
    一个轻量级的API，用于预览单个媒体项经过核心处理器处理后的演员列表。
    它只返回处理结果，不执行任何数据库更新或Emby更新。
    """
    if not media_processor_instance:
        return jsonify({"error": "核心处理器未就绪"}), 503

    logger.info(f"API: 收到为 ItemID {item_id} 预览处理后演员的请求。")

    # 步骤 1: 获取当前媒体的 Emby 详情
    try:
        item_details = emby_handler.get_emby_item_details(
            item_id,
            media_processor_instance.emby_url,
            media_processor_instance.emby_api_key,
            media_processor_instance.emby_user_id
        )
        if not item_details:
            return jsonify({"error": "无法获取当前媒体的Emby详情"}), 404
    except Exception as e:
        logger.error(f"API /preview_processed_cast: 获取Emby详情失败 for ID {item_id}: {e}", exc_info=True)
        return jsonify({"error": f"获取Emby详情时发生错误: {e}"}), 500

    # 步骤 2: 调用核心处理方法
    try:
        current_emby_cast_raw = item_details.get("People", [])
        
        # 直接调用 MediaProcessor 的核心方法
        processed_cast_result = media_processor_instance._process_cast_list(
            current_emby_cast_people=current_emby_cast_raw,
            media_info=item_details
        )
        
        # 步骤 3: 将处理结果转换为前端友好的格式
        # processed_cast_result 的格式是内部格式，我们需要转换为前端期望的格式
        # (embyPersonId, name, role, imdbId, doubanId, tmdbId)
        
        cast_for_frontend = []
        for actor_data in processed_cast_result:
            cast_for_frontend.append({
                "embyPersonId": actor_data.get("EmbyPersonId"),
                "name": actor_data.get("Name"),
                "role": actor_data.get("Role"),
                "imdbId": actor_data.get("ImdbId"),
                "doubanId": actor_data.get("DoubanCelebrityId"),
                "tmdbId": actor_data.get("TmdbPersonId"),
                "matchStatus": "已刷新" # 可以根据 actor_data['_source_comment'] 提供更详细的状态
            })

        logger.info(f"API: 成功为 ItemID {item_id} 预览了处理后的演员列表，返回 {len(cast_for_frontend)} 位演员。")
        return jsonify(cast_for_frontend)

    except Exception as e:
        logger.error(f"API /preview_processed_cast: 调用 _process_cast_list 时发生错误 for ID {item_id}: {e}", exc_info=True)
        return jsonify({"error": "在服务器端处理演员列表时发生内部错误"}), 500    
# ★★★ START: Emby 图片代理路由 ★★★
@app.route('/image_proxy/<path:image_path>')
def proxy_emby_image(image_path):
    """
    一个安全的、动态的 Emby 图片代理。
    """
    # ★★★ START: 关键修复 - 直接检查属性，而不是调用不存在的方法 ★★★
    if not media_processor_instance or not media_processor_instance.emby_url or not media_processor_instance.emby_api_key:
        logger.warning("图片代理请求失败：核心处理器未配置 Emby URL 或 API Key。")
        # 返回一个 1x1 的透明像素作为占位符
        return Response(
            b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82',
            mimetype='image/png'
        )
    # ★★★ END: 关键修复 ★★★

    try:
        # 从已加载的配置中获取 Emby URL 和 API Key
        emby_url = media_processor_instance.emby_url.rstrip('/')
        emby_api_key = media_processor_instance.emby_api_key

        query_string = request.query_string.decode('utf-8')
        target_url = f"{emby_url}/{image_path}"
        if query_string:
            target_url += f"?{query_string}"
        
        headers = {
            'X-Emby-Token': emby_api_key,
            'User-Agent': request.headers.get('User-Agent', 'EmbyActorProcessorProxy/1.0')
        }
        
        logger.debug(f"代理图片请求: {target_url}")

        emby_response = requests.get(target_url, headers=headers, stream=True, timeout=20)
        emby_response.raise_for_status()

        return Response(
            stream_with_context(emby_response.iter_content(chunk_size=8192)),
            content_type=emby_response.headers.get('Content-Type'),
            status=emby_response.status_code
        )
    except Exception as e:
        logger.error(f"代理 Emby 图片时发生严重错误: {e}", exc_info=True)
        # 返回一个占位符图片
        return Response(
            b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82',
            mimetype='image/png'
        )
# ✨✨✨ 清空待复核列表的 API ✨✨✨
@app.route('/api/actions/clear_review_items', methods=['POST'])
def api_clear_review_items():
    logger.info("API: 收到清空所有待复核项目的请求。")
    if task_lock.locked():
        return jsonify({"error": "后台有任务正在运行，请稍后再试。"}), 409
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM failed_log")
        deleted_count = cursor.rowcount
        conn.commit()
        conn.close()
        
        message = f"操作成功！已从待复核列表中清空 {deleted_count} 条记录。"
        logger.info(message)
        return jsonify({"message": message}), 200
        
    except Exception as e:
        logger.error(f"清空待复核列表时失败: {e}", exc_info=True)
        return jsonify({"error": "服务器在清空数据库时发生内部错误"}), 500
# ✨✨✨ END: 新增 API ✨✨✨

# # ★★★ 获取追剧列表的API ★★★
@app.route('/api/watchlist', methods=['GET']) 
@login_required
def api_get_watchlist(): # <-- 函数名改为 api_get_watchlist
    # 模式检查
    if not APP_CONFIG.get(constants.CONFIG_OPTION_USE_SA_MODE, True):
        return jsonify([]) # API模式下返回空列表

    logger.info("API: 收到获取追剧列表的请求。")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM watchlist ORDER BY added_at DESC")
        items = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return jsonify(items)
    except Exception as e:
        logger.error(f"获取追剧列表时发生错误: {e}", exc_info=True)
        return jsonify({"error": "获取追剧列表时发生服务器内部错误"}), 500
# ★★★ 新增：手动添加到追剧列表的API ★★★
@app.route('/api/watchlist/add', methods=['POST'])
@login_required
def api_add_to_watchlist():
    data = request.json
    item_id = data.get('item_id')
    tmdb_id = data.get('tmdb_id')
    item_name = data.get('item_name')
    item_type = data.get('item_type')

    if not all([item_id, tmdb_id, item_name, item_type]):
        return jsonify({"error": "缺少必要的项目信息"}), 400
    
    if item_type != 'Series':
        return jsonify({"error": "只能将'剧集'类型添加到追剧列表"}), 400

    logger.info(f"API: 收到手动添加 '{item_name}' 到追剧列表的请求。")
    try:
        # 我们直接操作数据库，因为 watchlist_processor 主要是为定时任务服务的
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT OR REPLACE INTO watchlist (item_id, tmdb_id, item_name, item_type, status, last_checked_at)
            VALUES (?, ?, ?, ?, 'Watching', NULL)
        """, (item_id, tmdb_id, item_name, item_type))
        # 使用 INSERT OR REPLACE 确保如果已存在，会更新其状态为 Watching

        conn.commit()
        conn.close()
        
        return jsonify({"message": f"《{item_name}》已成功添加到追剧列表！"}), 200
        
    except Exception as e:
        logger.error(f"手动添加项目到追剧列表时发生错误: {e}", exc_info=True)
        return jsonify({"error": "服务器在添加时发生内部错误"}), 500

#★★★ 手动触发追剧列表更新的API ★★★
@app.route('/api/watchlist/trigger_full_update', methods=['POST']) 
@login_required
def api_trigger_watchlist_update(): # <-- 函数名可以不变，因为它和路径无关了
    # 模式检查
    if not APP_CONFIG.get(constants.CONFIG_OPTION_USE_SA_MODE, True):
        return jsonify({"error": "此功能仅在神医模式下可用。"}), 403

    # ... (这个函数的内部逻辑完全不变) ...
    
    logger.info("API: 收到手动触发追剧列表更新的请求。")
    submit_task_to_queue(
        task_process_watchlist,
        "手动追剧更新"
    )
    return jsonify({"message": "追剧列表更新任务已在后台启动！"}), 202
# ★★★ 新增：手动更新追剧状态的API ★★★
@app.route('/api/watchlist/update_status', methods=['POST'])
@login_required
def api_update_watchlist_status():
    # 1. 检查任务锁，防止并发写入
    if task_lock.locked():
        logger.warning("API: 无法更新追剧状态，因为有后台任务正在运行。")
        return jsonify({"error": "后台任务正在运行，请稍后再试"}), 409

    data = request.json
    item_id = data.get('item_id')
    new_status = data.get('new_status')

    if not item_id or new_status not in ['Watching', 'Ended', 'Paused']:
        return jsonify({"error": "请求参数无效"}), 400

    logger.info(f"API: 收到请求，将项目 {item_id} 的追剧状态更新为 '{new_status}'。")
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE watchlist SET status = ? WHERE item_id = ?",
                (new_status, item_id)
            )
            conn.commit()
            if cursor.rowcount == 0:
                logger.warning(f"尝试更新追剧状态，但未在列表中找到项目 {item_id}。")
                return jsonify({"error": "未在追剧列表中找到该项目"}), 404
        
        return jsonify({"message": "状态更新成功"}), 200
        
    except Exception as e:
        logger.error(f"更新追剧状态时发生错误: {e}", exc_info=True)
        return jsonify({"error": "服务器在更新状态时发生内部错误"}), 500


# ★★★ 新增：手动从追剧列表移除的API ★★★
@app.route('/api/watchlist/remove/<item_id>', methods=['POST'])
@login_required
def api_remove_from_watchlist(item_id):
    # 1. 严格检查任务锁
    if task_lock.locked():
        logger.warning(f"API: 无法移除项目 {item_id}，因为有后台任务正在运行。")
        return jsonify({"error": "后台有长时间任务正在运行，请稍后再试"}), 409

    logger.info(f"API: 收到请求，将项目 {item_id} 从追剧列表移除。")
    conn = None # ★★★ 将 conn 移到 try 外部
    try:
        # ★★★ 不再使用 'with' 语句，手动管理连接 ★★★
        conn = get_db_connection()
        cursor = conn.cursor()
        
        logger.debug(f"准备执行 DELETE FROM watchlist WHERE item_id = {item_id}")
        cursor.execute("DELETE FROM watchlist WHERE item_id = ?", (item_id,))
        
        # 检查是否真的有行被影响了
        if cursor.rowcount > 0:
            logger.info(f"成功执行 DELETE 语句，影响行数: {cursor.rowcount}。准备提交...")
            conn.commit() # ★★★ 提交事务 ★★★
            logger.info("数据库事务已提交。")
            return jsonify({"message": "已从追剧列表移除"}), 200
        else:
            # 如果 rowcount 是 0，说明数据库里本来就没有这个 item_id
            logger.warning(f"尝试删除项目 {item_id}，但在数据库中未找到匹配项。")
            return jsonify({"error": "未在追剧列表中找到该项目"}), 404
            
    except sqlite3.OperationalError as e:
        # ★★★ 专门捕获 OperationalError，它通常与锁定有关 ★★★
        if "database is locked" in str(e).lower():
            logger.error(f"从追剧列表移除项目时发生数据库锁定错误: {e}", exc_info=True)
            return jsonify({"error": "数据库当前正忙，请稍后再试。"}), 503 # 503 Service Unavailable
        else:
            logger.error(f"从追剧列表移除项目时发生数据库操作错误: {e}", exc_info=True)
            return jsonify({"error": "移除项目时发生数据库操作错误"}), 500
    except Exception as e:
        logger.error(f"从追剧列表移除项目时发生未知错误: {e}", exc_info=True)
        return jsonify({"error": "移除项目时发生未知的服务器内部错误"}), 500
    finally:
        # ★★★ 确保连接总是被关闭 ★★★
        if conn:
            conn.close()
# ★★★ 新增：手动触发单项追剧更新的API ★★★
@app.route('/api/watchlist/trigger_update/<item_id>/', methods=['POST'])
@login_required
def api_trigger_single_watchlist_update(item_id):
    logger.info(f"API: 收到对单个项目 {item_id} 的追剧更新请求。")
    
    if task_lock.locked():
        return jsonify({"error": "后台有其他任务正在运行，请稍后再试"}), 409

    if not watchlist_processor_instance:
        return jsonify({"error": "追剧处理模块未就绪"}), 503

    # 我们需要一个新的、只处理单个项目的任务函数
    # 我们在下面定义它
    submit_task_to_queue(
        task_process_single_watchlist_item,
        f"手动单项追剧更新: {item_id}",
        item_id # 把 item_id 作为参数传给任务
    )
    
    return jsonify({"message": f"项目 {item_id} 的更新任务已在后台启动！"}), 202
# ★★★ 重新处理单个项目 ★★★
@app.route('/api/actions/reprocess_item/<item_id>', methods=['POST'])
@login_required
def api_reprocess_item(item_id):
    """
    提交一个任务，用于重新处理单个媒体项。
    """
    # ★★★ 优化后的认证方式 ★★★
    # 1. 检查处理器实例是否存在
    if not media_processor_instance:
        return jsonify({"error": "核心处理器未就绪，请检查配置并重启。"}), 503

    # 2. 检查处理器实例的类型
    #    isinstance() 是 Python 中判断对象类型的标准方法
    #    假设您的神医模式处理器类名叫 MediaProcessorSA
    if not isinstance(media_processor_instance, MediaProcessorSA):
        return jsonify({"error": "此功能仅在神医模式下可用。"}), 403
    # 3. 开始处理
    logger.info(f"API: 收到重新处理项目 '{item_id}' 的请求。")
    
    if task_lock.locked():
        return jsonify({"error": "后台有任务正在运行，请稍后再试。"}), 409

    # 我们只提交任务，把所有复杂逻辑都交给后台
    # 注意：我们把 item_id 作为位置参数传递
    submit_task_to_queue(
        task_reprocess_single_item, # ★★★ 调用新的、封装好的任务函数 ★★★
        f"重新处理: {item_id}",
        item_id # <--- 将 item_id 作为参数传递给任务
    )
    
    return jsonify({"message": f"重新处理项目 '{item_id}' 的任务已提交。"}), 202

# ★★★ 重新处理所有待复核项 ★★★
@app.route('/api/actions/reprocess_all_review_items', methods=['POST'])
@login_required
def api_reprocess_all_review_items():
    """
    提交一个任务，用于重新处理所有待复核列表中的项目。
    """
    # ★★★ 优化后的认证方式 ★★★
    # 1. 检查处理器实例是否存在
    if not media_processor_instance:
        return jsonify({"error": "核心处理器未就绪，请检查配置并重启。"}), 503

    # 2. 检查处理器实例的类型
    #    isinstance() 是 Python 中判断对象类型的标准方法
    #    假设您的神医模式处理器类名叫 MediaProcessorSA
    if not isinstance(media_processor_instance, MediaProcessorSA):
        return jsonify({"error": "此功能仅在神医模式下可用。"}), 403
    # 3. 开始处理
    logger.info("API: 收到重新处理所有待复核项的请求。")
    
    if task_lock.locked():
        return jsonify({"error": "后台有任务正在运行，请稍后再试。"}), 409

    # 提交一个宏任务，让后台线程来做这件事
    submit_task_to_queue(
        task_reprocess_all_review_items, # <--- 我们需要创建这个新的任务函数
        "重新处理所有待复核项"
    )
    
    return jsonify({"message": "重新处理所有待复核项的任务已提交。"}), 202
# ★★★ 新增：触发全量图片同步的 API 接口 ★★★
@app.route('/api/actions/trigger_full_image_sync', methods=['POST'])
@login_required
def api_trigger_full_image_sync():
    """
    提交一个任务，用于全量同步所有已处理项目的海报。
    """
    # 1. 检查处理器实例是否存在
    if not media_processor_instance:
        return jsonify({"error": "核心处理器未就绪，请检查配置并重启。"}), 503

    # 2. 检查处理器实例的类型
    #    isinstance() 是 Python 中判断对象类型的标准方法
    #    假设您的神医模式处理器类名叫 MediaProcessorSA
    if not isinstance(media_processor_instance, MediaProcessorSA):
        return jsonify({"error": "此功能仅在神医模式下可用。"}), 403
    # 3. 开始处理
    
    if task_lock.locked():
        return jsonify({"error": "后台有任务正在运行，请稍后再试。"}), 409

    submit_task_to_queue(
        task_full_image_sync,
        "全量同步媒体库海报"
    )
    
    return jsonify({"message": "全量海报同步任务已成功提交。"}), 202
# ★★★ 触发应用更新和重启的 API ★★★
@app.route('/api/actions/trigger_app_update', methods=['POST'])
@login_required
def api_trigger_app_update():
    """
    触发应用更新。
    """
    logger.info("API: 收到应用更新请求...")

    # ★★★ 核心修复：从 app.config 获取路径，不再依赖任何全局变量 ★★★
    data_dir = app.config.get('PERSISTENT_DATA_PATH')
    
    if not data_dir:
        logger.error("更新失败：数据目录未在 app.config 中配置。")
        return jsonify({"error": "数据目录未配置，无法执行更新。"}), 500

    # 现在 data_dir 的值一定是正确的 ("/config" 或 "local_data")
    marker_file_path = os.path.join(data_dir, ".update_in_progress")
    
    try:
        with open(marker_file_path, 'w') as f:
            f.write('update requested by api')
        # 这里的日志现在会显示正确的路径
        logger.info(f"已成功创建更新标记文件: {marker_file_path}") 
    except Exception as e:
        logger.error(f"创建更新标记文件失败: {e}", exc_info=True)
        return jsonify({"error": "创建更新标记失败，请检查目录权限。"}), 500

    # (后续的 delayed_exit 延迟退出逻辑保持不变)
    def delayed_exit():
        time.sleep(3) 
        logger.info("应用即将退出以进行更新...")
        os._exit(0)
    threading.Thread(target=delayed_exit).start()
    
    return jsonify({"message": "更新请求已收到，应用将在几秒后重启以应用更新。"}), 202
# ★★★ 让前端可以查询更新状态 ★★★
@app.route('/api/update_status')
def get_update_status():
    """
    返回当前的应用更新状态。
    """
    # 直接返回我们后台线程维护的全局字典
    return jsonify(update_info)
# ★★★ END: 1. ★★★
#--- 兜底路由，必须放最后 ---
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve(path):
    static_folder_path = app.static_folder 

    if path != "" and os.path.exists(os.path.join(static_folder_path, path)):
        return send_from_directory(static_folder_path, path)
    else:
        return send_from_directory(static_folder_path, 'index.html')
    
if __name__ == '__main__':
    logger.info(f"应用程序启动... 版本: {constants.APP_VERSION}")
    initialize_application()
    app.run(host='0.0.0.0', port=constants.WEB_APP_PORT, debug=True, use_reloader=False)
# # --- 主程序入口结束 ---