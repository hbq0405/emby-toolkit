# config_manager.py
import os
import configparser
import logging
from typing import Dict, Any, Tuple, Optional
import json
from database import settings_db
# --- 核心模块导入 ---
import constants # 你的常量定义


logger = logging.getLogger(__name__)

# --- 路径和配置定义 ---
# 这部分逻辑与配置紧密相关，所以移到这里
APP_DATA_DIR_ENV = os.environ.get("APP_DATA_DIR")

if APP_DATA_DIR_ENV:
    # 如果在 Docker 中，并且设置了 APP_DATA_DIR 环境变量 (例如设置为 "/config")
    PERSISTENT_DATA_PATH = APP_DATA_DIR_ENV
    logger.info(f"检测到 APP_DATA_DIR 环境变量，将使用持久化数据路径: {PERSISTENT_DATA_PATH}")
else:
    # 本地开发环境
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    PERSISTENT_DATA_PATH = os.path.join(PROJECT_ROOT, "local_data")
    logger.debug(f"未检测到 APP_DATA_DIR 环境变量，将使用本地开发数据路径: {PERSISTENT_DATA_PATH}")

# 确保这个持久化数据目录存在
try:
    if not os.path.exists(PERSISTENT_DATA_PATH):
        os.makedirs(PERSISTENT_DATA_PATH, exist_ok=True)
        logger.info(f"持久化数据目录已创建/确认: {PERSISTENT_DATA_PATH}")
except OSError as e:
    logger.error(f"创建持久化数据目录 '{PERSISTENT_DATA_PATH}' 失败: {e}。程序可能无法正常读写配置文件和数据库。")
    raise RuntimeError(f"无法创建必要的数据目录: {PERSISTENT_DATA_PATH}") from e

# 从 constants 模块安全地获取文件名，如果不存在则使用默认值
CONFIG_FILE_NAME = getattr(constants, 'CONFIG_FILE_NAME', "config.ini")

# 最终的配置文件和数据库路径
CONFIG_FILE_PATH = os.path.join(PERSISTENT_DATA_PATH, CONFIG_FILE_NAME)
LOG_DIRECTORY = os.path.join(PERSISTENT_DATA_PATH, 'logs')

# BOOTSTRAP_CONFIG_DEF: 只包含启动所必需的配置
BOOTSTRAP_CONFIG_DEF = {
    # [Database]
    constants.CONFIG_OPTION_DB_HOST: (constants.CONFIG_SECTION_DATABASE, 'string', 'localhost'),
    constants.CONFIG_OPTION_DB_PORT: (constants.CONFIG_SECTION_DATABASE, 'int', 5432),
    constants.CONFIG_OPTION_DB_USER: (constants.CONFIG_SECTION_DATABASE, 'string', 'postgres'),
    constants.CONFIG_OPTION_DB_PASSWORD: (constants.CONFIG_SECTION_DATABASE, 'string', 'your_password'),
    constants.CONFIG_OPTION_DB_NAME: (constants.CONFIG_SECTION_DATABASE, 'string', 'emby_toolkit'),
    # [Authentication]
    constants.CONFIG_OPTION_AUTH_ENABLED: (constants.CONFIG_SECTION_AUTH, 'boolean', False),
    constants.CONFIG_OPTION_AUTH_USERNAME: (constants.CONFIG_SECTION_AUTH, 'string', constants.DEFAULT_USERNAME),
}

# ✨✨✨ “配置清单” - 这是配置模块的核心 ✨✨✨
DYNAMIC_CONFIG_DEF = {
    # [Emby]
    constants.CONFIG_OPTION_EMBY_SERVER_URL: (constants.CONFIG_SECTION_EMBY, 'string', ""),
    constants.CONFIG_OPTION_EMBY_PUBLIC_URL: (constants.CONFIG_SECTION_EMBY, 'string', ""),
    constants.CONFIG_OPTION_EMBY_API_KEY: (constants.CONFIG_SECTION_EMBY, 'string', ""),
    constants.CONFIG_OPTION_EMBY_USER_ID: (constants.CONFIG_SECTION_EMBY, 'string', ""),
    constants.CONFIG_OPTION_EMBY_API_TIMEOUT: (constants.CONFIG_SECTION_EMBY, 'int', 60),
    constants.CONFIG_OPTION_EMBY_LIBRARIES_TO_PROCESS: (constants.CONFIG_SECTION_EMBY, 'list', []),
    constants.CONFIG_OPTION_EMBY_ADMIN_USER: (constants.CONFIG_SECTION_EMBY, 'string', ""),
    constants.CONFIG_OPTION_EMBY_ADMIN_PASS: (constants.CONFIG_SECTION_EMBY, 'password', ""), 

    # [ReverseProxy]
    constants.CONFIG_OPTION_PROXY_ENABLED: (constants.CONFIG_SECTION_REVERSE_PROXY, 'boolean', False),
    constants.CONFIG_OPTION_PROXY_PORT: (constants.CONFIG_SECTION_REVERSE_PROXY, 'int', 8097),
    constants.CONFIG_OPTION_PROXY_MERGE_NATIVE: (constants.CONFIG_SECTION_REVERSE_PROXY, 'boolean', True),
    constants.CONFIG_OPTION_PROXY_NATIVE_VIEW_SELECTION: (constants.CONFIG_SECTION_REVERSE_PROXY, 'list', []),
    constants.CONFIG_OPTION_PROXY_NATIVE_VIEW_ORDER: (constants.CONFIG_SECTION_REVERSE_PROXY, 'str', 'before'),
    constants.CONFIG_OPTION_PROXY_302_REDIRECT_URL: (constants.CONFIG_SECTION_REVERSE_PROXY, 'string', ""),
    constants.CONFIG_OPTION_PROXY_NATIVE_VIEW_ORDER: (constants.CONFIG_SECTION_REVERSE_PROXY, 'str', 'before'),
    constants.CONFIG_OPTION_PROXY_SHOW_MISSING_PLACEHOLDERS: (constants.CONFIG_SECTION_REVERSE_PROXY, 'boolean', False),

    # [TMDB]
    constants.CONFIG_OPTION_TMDB_API_KEY: (constants.CONFIG_SECTION_TMDB, 'string', ""),
    constants.CONFIG_OPTION_TMDB_API_BASE_URL: (constants.CONFIG_SECTION_TMDB, 'string', "https://api.themoviedb.org/3"),
    constants.CONFIG_OPTION_TMDB_INCLUDE_ADULT: (constants.CONFIG_SECTION_TMDB, 'boolean', False),
    constants.CONFIG_OPTION_TMDB_IMAGE_LANGUAGE_PREFERENCE: (constants.CONFIG_SECTION_TMDB, 'string', 'zh'),
    constants.CONFIG_OPTION_GITHUB_TOKEN: (constants.CONFIG_SECTION_GITHUB, 'string', ""),

    # [DoubanAPI]
    constants.CONFIG_OPTION_DOUBAN_DEFAULT_COOLDOWN: (constants.CONFIG_SECTION_API_DOUBAN, 'float', 1.0),
    constants.CONFIG_OPTION_DOUBAN_COOKIE: (constants.CONFIG_SECTION_API_DOUBAN, 'string', ""),
    constants.CONFIG_OPTION_DOUBAN_ENABLE_ONLINE_API: (constants.CONFIG_SECTION_API_DOUBAN, 'boolean', True),

    # [MoviePilot]
    constants.CONFIG_OPTION_MOVIEPILOT_URL: (constants.CONFIG_SECTION_MOVIEPILOT, 'string', ""),
    constants.CONFIG_OPTION_MOVIEPILOT_USERNAME: (constants.CONFIG_SECTION_MOVIEPILOT, 'string', ""),
    constants.CONFIG_OPTION_MOVIEPILOT_PASSWORD: (constants.CONFIG_SECTION_MOVIEPILOT, 'string', ""),
    constants.CONFIG_OPTION_AUTOSUB_ENABLED: (constants.CONFIG_SECTION_MOVIEPILOT, 'boolean', False),
    constants.CONFIG_OPTION_RESUBSCRIBE_DAILY_CAP: (constants.CONFIG_SECTION_MOVIEPILOT, 'int', 200),
    constants.CONFIG_OPTION_RESUBSCRIBE_DELAY_SECONDS: (constants.CONFIG_SECTION_MOVIEPILOT, 'float', 1.5),
    
    # [Monitor] 
    constants.CONFIG_OPTION_MONITOR_ENABLED: (constants.CONFIG_SECTION_MONITOR, 'boolean', False),
    constants.CONFIG_OPTION_MONITOR_PATHS: (constants.CONFIG_SECTION_MONITOR, 'list', []),
    constants.CONFIG_OPTION_MONITOR_EXTENSIONS: (constants.CONFIG_SECTION_MONITOR, 'list', constants.DEFAULT_MONITOR_EXTENSIONS),
    constants.CONFIG_OPTION_MONITOR_SCAN_LOOKBACK_DAYS: (constants.CONFIG_SECTION_MONITOR, 'int', constants.DEFAULT_MONITOR_SCAN_LOOKBACK_DAYS),
    constants.CONFIG_OPTION_MONITOR_EXCLUDE_DIRS: (constants.CONFIG_SECTION_MONITOR, 'list', constants.DEFAULT_MONITOR_EXCLUDE_DIRS),
    constants.CONFIG_OPTION_MONITOR_EXCLUDE_REFRESH_DELAY: (constants.CONFIG_SECTION_MONITOR, 'int', constants.DEFAULT_MONITOR_EXCLUDE_REFRESH_DELAY),

    # [LocalDataSource]
    constants.CONFIG_OPTION_LOCAL_DATA_PATH: (constants.CONFIG_SECTION_LOCAL_DATA, 'string', ""),

    # [General]
    "delay_between_items_sec": ("General", 'float', 0.5),
    constants.CONFIG_OPTION_MIN_SCORE_FOR_REVIEW: ("General", 'float', constants.DEFAULT_MIN_SCORE_FOR_REVIEW),
    constants.CONFIG_OPTION_MAX_ACTORS_TO_PROCESS: ("General", 'int', constants.DEFAULT_MAX_ACTORS_TO_PROCESS),
    constants.CONFIG_OPTION_REMOVE_ACTORS_WITHOUT_AVATARS: ("General", 'boolean', True),
    constants.CONFIG_OPTION_BACKUP_EPISODE_IMAGE: ("General", 'boolean', False),
    constants.CONFIG_OPTION_KEYWORD_TO_TAGS: ("General", 'boolean', False),

    # [Network] 
    constants.CONFIG_OPTION_NETWORK_PROXY_ENABLED: (constants.CONFIG_SECTION_NETWORK, 'boolean', False),
    constants.CONFIG_OPTION_NETWORK_HTTP_PROXY: (constants.CONFIG_SECTION_NETWORK, 'string', ""),
    "user_agent": ("Network", 'string', 'Mozilla/5.0 ...'),
    "accept_language": ("Network", 'string', 'zh-CN,zh;q=0.9,en;q=0.8'),

    # [AITranslation]
    constants.CONFIG_OPTION_AI_PROVIDER: (constants.CONFIG_SECTION_AI_TRANSLATION, 'string', "openai"),
    constants.CONFIG_OPTION_AI_API_KEY: (constants.CONFIG_SECTION_AI_TRANSLATION, 'string', ""),
    constants.CONFIG_OPTION_AI_MODEL_NAME: (constants.CONFIG_SECTION_AI_TRANSLATION, 'string', "Qwen/Qwen2.5-72B-Instruct"),
    constants.CONFIG_OPTION_AI_BASE_URL: (constants.CONFIG_SECTION_AI_TRANSLATION, 'string', "https://api.siliconflow.cn/v1"),
    constants.CONFIG_OPTION_AI_VECTOR: (constants.CONFIG_SECTION_AI_TRANSLATION, 'boolean', False),
    constants.CONFIG_OPTION_AI_TRANSLATION_MODE: (constants.CONFIG_SECTION_AI_TRANSLATION, 'string', 'fast'),
    constants.CONFIG_OPTION_AI_TRANSLATE_ACTOR_ROLE: (constants.CONFIG_SECTION_AI_TRANSLATION, 'boolean', False),
    constants.CONFIG_OPTION_AI_TRANSLATE_TITLE: (constants.CONFIG_SECTION_AI_TRANSLATION, 'boolean', False),
    constants.CONFIG_OPTION_AI_TRANSLATE_OVERVIEW: (constants.CONFIG_SECTION_AI_TRANSLATION, 'boolean', False),
    constants.CONFIG_OPTION_AI_TRANSLATE_EPISODE_OVERVIEW: (constants.CONFIG_SECTION_AI_TRANSLATION, 'boolean', False),

    # [Scheduler]
    # --- 高频任务链 ---
    constants.CONFIG_OPTION_TASK_CHAIN_ENABLED: (constants.CONFIG_SECTION_SCHEDULER, 'boolean', False),
    constants.CONFIG_OPTION_TASK_CHAIN_CRON: (constants.CONFIG_SECTION_SCHEDULER, 'string', "0 2 * * *"),
    constants.CONFIG_OPTION_TASK_CHAIN_SEQUENCE: (constants.CONFIG_SECTION_SCHEDULER, 'list', []),
    constants.CONFIG_OPTION_TASK_CHAIN_MAX_RUNTIME_MINUTES: (constants.CONFIG_SECTION_SCHEDULER, 'int', 0),
    # --- 低频任务链 ---
    constants.CONFIG_OPTION_TASK_CHAIN_LOW_FREQ_ENABLED: (constants.CONFIG_SECTION_SCHEDULER, 'boolean', False),
    constants.CONFIG_OPTION_TASK_CHAIN_LOW_FREQ_CRON: (constants.CONFIG_SECTION_SCHEDULER, 'string', "0 5 * * 0"),
    constants.CONFIG_OPTION_TASK_CHAIN_LOW_FREQ_SEQUENCE: (constants.CONFIG_SECTION_SCHEDULER, 'list', []),
    constants.CONFIG_OPTION_TASK_CHAIN_LOW_FREQ_MAX_RUNTIME_MINUTES: (constants.CONFIG_SECTION_SCHEDULER, 'int', 0),
    
    # [Actor]
    constants.CONFIG_OPTION_ACTOR_ROLE_ADD_PREFIX: (constants.CONFIG_SECTION_ACTOR, 'boolean', False),
    constants.CONFIG_OPTION_ACTOR_MAIN_ROLE_ONLY: (constants.CONFIG_SECTION_ACTOR, 'boolean', False),

    # [Logging]
    constants.CONFIG_OPTION_LOG_ROTATION_SIZE_MB: (constants.CONFIG_SECTION_LOGGING, 'int', constants.DEFAULT_LOG_ROTATION_SIZE_MB),
    constants.CONFIG_OPTION_LOG_ROTATION_BACKUPS: (constants.CONFIG_SECTION_LOGGING, 'int', constants.DEFAULT_LOG_ROTATION_BACKUPS),

    # [Telegram]
    constants.CONFIG_OPTION_TELEGRAM_BOT_TOKEN: (constants.CONFIG_SECTION_TELEGRAM, 'password', ""),
    constants.CONFIG_OPTION_TELEGRAM_CHANNEL_ID: (constants.CONFIG_SECTION_TELEGRAM, 'string', ""),
}

# --- 全局配置字典 ---
# 其他模块可以通过 from config_manager import APP_CONFIG 来访问
APP_CONFIG: Dict[str, Any] = {}

# --- 加载配置 ---
def load_config():
    """
    【V3 - 混合模式最终版】
    1. 从 config.ini 和环境变量加载启动配置。
    2. 使用启动配置连接数据库。
    3. 从数据库 app_settings 表加载动态应用配置。
    """
    global APP_CONFIG
    
    # ======================================================================
    # 阶段 1: 加载启动配置 (从 config.ini 和环境变量)
    # ======================================================================
    bootstrap_config = {}
    config_parser = configparser.ConfigParser()
    is_first_run = not os.path.exists(CONFIG_FILE_PATH)

    if not is_first_run:
        try:
            config_parser.read(CONFIG_FILE_PATH, encoding='utf-8')
        except Exception as e:
            logger.error(f"解析配置文件时出错: {e}", exc_info=True)

    # --- 从 config.ini 文件读取 ---
    for key, (section, type, default) in BOOTSTRAP_CONFIG_DEF.items():
        if not config_parser.has_section(section):
            config_parser.add_section(section)
            
        if type == 'boolean':
            # 特殊逻辑：如果是首次运行，强制开启认证
            if key == constants.CONFIG_OPTION_AUTH_ENABLED and is_first_run:
                bootstrap_config[key] = True
            else:
                bootstrap_config[key] = config_parser.getboolean(section, key, fallback=default)
        elif type == 'int':
            bootstrap_config[key] = config_parser.getint(section, key, fallback=default)
        elif type == 'float':
            bootstrap_config[key] = config_parser.getfloat(section, key, fallback=default)
        elif type == 'list':
            value_str = config_parser.get(section, key, fallback=",".join(map(str, default)))
            bootstrap_config[key] = [item.strip() for item in value_str.split(',') if item.strip()]
        else: # string
            bootstrap_config[key] = config_parser.get(section, key, fallback=default)

    # --- 使用环境变量覆盖数据库连接信息 ---
    logger.info("  ➜ 检查数据库环境变量...")
    env_to_config_map = {
        constants.ENV_VAR_DB_HOST: constants.CONFIG_OPTION_DB_HOST,
        constants.ENV_VAR_DB_PORT: constants.CONFIG_OPTION_DB_PORT,
        constants.ENV_VAR_DB_USER: constants.CONFIG_OPTION_DB_USER,
        constants.ENV_VAR_DB_PASSWORD: constants.CONFIG_OPTION_DB_PASSWORD,
        constants.ENV_VAR_DB_NAME: constants.CONFIG_OPTION_DB_NAME,
        constants.ENV_VAR_TMDB_API_BASE_URL: constants.CONFIG_OPTION_TMDB_API_BASE_URL,
    }

    for env_var, config_key in env_to_config_map.items():
        env_value = os.environ.get(env_var)
        if env_value:
            logger.trace(f"  ➜ 检测到环境变量 '{env_var}'，将覆盖配置 '{config_key}'。")
            if config_key == constants.CONFIG_OPTION_DB_PORT:
                try:
                    bootstrap_config[config_key] = int(env_value)
                except ValueError:
                    logger.error(f"环境变量 '{env_var}' 的值 '{env_value}' 不是一个有效的端口号，已忽略。")
            else:
                bootstrap_config[config_key] = env_value

    # 将加载好的启动配置更新到全局配置中
    APP_CONFIG.update(bootstrap_config)
    logger.trace("  ➜ 启动配置已加载。")

    # ======================================================================
    # 阶段 2: 加载动态应用配置 (从数据库)
    # ======================================================================
    try:
        # 使用 'dynamic_app_config' 作为唯一的键来获取所有动态配置
        dynamic_config_from_db = settings_db.get_setting('dynamic_app_config') or {}
        
        # 将数据库中的配置与 DYNAMIC_CONFIG_DEF 中定义的默认值合并
        # 这样可以确保即使数据库中的配置不完整，或者未来代码中新增了配置项，程序也能正常工作
        final_dynamic_config = {}
        for key, (section, type, default) in DYNAMIC_CONFIG_DEF.items():
            # 优先使用数据库中的值，如果不存在，则使用代码中定义的默认值
            final_dynamic_config[key] = dynamic_config_from_db.get(key, default)

        # 将加载好的动态配置也更新到全局配置中
        APP_CONFIG.update(final_dynamic_config)
        logger.trace("  ➜ 动态应用配置已从数据库加载。")

    except Exception as e:
        # 如果连接数据库或读取setting失败，程序不能崩溃，必须用默认值继续运行
        logger.error(f"从数据库加载动态配置失败: {e}。应用将使用默认的动态配置值。")
        default_dynamic_config = {key: default for key, (section, type, default) in DYNAMIC_CONFIG_DEF.items()}
        APP_CONFIG.update(default_dynamic_config)

    logger.info("  ➜ 所有配置已加载完成。")
    # 函数现在不再需要返回 is_first_run，因为这个状态只在函数内部使用
    # 但为了保持函数签名不变，我们暂时保留它
    return APP_CONFIG, is_first_run

# --- 保存配置 ---
def save_config(new_config: Dict[str, Any]):
    """
    【V4 - 健壮的合并保存模式】
    1. 从数据库加载当前完整的动态配置。
    2. 将前端传入的新配置（可能不完整）合并到加载的配置中。
    3. 将合并后的完整配置对象存回数据库，确保任何标签页的保存都不会丢失其他标签页的设置。
    """
    global APP_CONFIG
    
    try:
        # 步骤 1: 从数据库加载当前完整的动态配置，如果不存在则视为空字典
        full_dynamic_config = settings_db.get_setting('dynamic_app_config') or {}
        
        # 步骤 2: 将前端传入的新配置更新（合并）到这个完整的配置对象中
        # 这确保了只更新变化的键，而不会丢失其他键
        full_dynamic_config.update(new_config)
        
        # 步骤 3: (可选但推荐) 创建一个最终要保存的字典，确保只包含在 DYNAMIC_CONFIG_DEF 中定义的合法键
        # 这样可以防止任何意外的键被存入数据库
        dynamic_config_to_save = {
            key: value
            for key, value in full_dynamic_config.items()
            if key in DYNAMIC_CONFIG_DEF
        }
        
        # 步骤 4: 将这个合并后的、完整的、干净的配置对象作为一个整体存回数据库
        settings_db.save_setting('dynamic_app_config', dynamic_config_to_save)
        
        # 步骤 5: 更新内存中的全局配置以立即生效
        APP_CONFIG.update(dynamic_config_to_save)
        logger.info("  ➜ 动态应用配置已成功合并保存到数据库，内存中的配置已同步。")
        
    except Exception as e:
        logger.error(f"  ➜ 保存动态配置到数据库时失败: {e}", exc_info=True)
        raise

# ★★★ 保存自定义主题 ★★★
def save_custom_theme(theme_data: dict):
    """
    将自定义主题保存到数据库 app_settings 表中。
    """
    try:
        settings_db.save_setting('custom_theme', theme_data)
        logger.info(f"  ➜ 自定义主题已成功保存到数据库。")
    except Exception as e:
        logger.error(f"  ➜ 保存自定义主题到数据库失败: {e}", exc_info=True)
        raise

# ★★★ 加载自定义主题 (数据库版) ★★★
def load_custom_theme() -> dict:
    """
    从数据库加载自定义主题。
    """
    try:
        theme_data = settings_db.get_setting('custom_theme')
        if theme_data:
            return theme_data
        return {}
    except Exception as e:
        logger.error(f"  ➜ 加载自定义主题失败: {e}", exc_info=True)
        return {}

# ★★★ 删除自定义主题 (数据库版) ★★★    
def delete_custom_theme() -> bool:
    """
    从数据库中删除自定义主题。
    """
    return settings_db.delete_setting('custom_theme')
    
# --- 代理小助手 ---
def get_proxies_for_requests() -> Optional[Dict[str, str]]:
    """
    【代理小助手】
    根据全局配置，生成用于 requests 库的代理字典。
    如果代理未启用或 URL 为空，则返回 None。
    """
    if APP_CONFIG.get(constants.CONFIG_OPTION_NETWORK_PROXY_ENABLED) and APP_CONFIG.get(constants.CONFIG_OPTION_NETWORK_HTTP_PROXY):
        proxy_url = APP_CONFIG[constants.CONFIG_OPTION_NETWORK_HTTP_PROXY]
        return {
            "http": proxy_url,
            "https": proxy_url,
        }
    return None

def is_system_configured() -> bool:
    """检查是否已配置 Emby 基础连接信息"""
    url = APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_SERVER_URL)
    key = APP_CONFIG.get(constants.CONFIG_OPTION_EMBY_API_KEY)
    # 简单的判断：URL和Key都不为空
    return bool(url and key and url.strip() and key.strip())