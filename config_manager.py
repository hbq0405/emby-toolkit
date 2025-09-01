# config_manager.py
import os
import configparser
import logging
from typing import Dict, Any, Tuple, Optional
import json

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


# ✨✨✨ “配置清单” - 这是配置模块的核心 ✨✨✨
CONFIG_DEFINITION = {
     # [Database] - ★★★ 新增PostgreSQL配置 ★★★
    constants.CONFIG_OPTION_DB_HOST: (constants.CONFIG_SECTION_DATABASE, 'string', 'localhost'),
    constants.CONFIG_OPTION_DB_PORT: (constants.CONFIG_SECTION_DATABASE, 'int', 5432),
    constants.CONFIG_OPTION_DB_USER: (constants.CONFIG_SECTION_DATABASE, 'string', 'postgres'),
    constants.CONFIG_OPTION_DB_PASSWORD: (constants.CONFIG_SECTION_DATABASE, 'string', 'your_password'),
    constants.CONFIG_OPTION_DB_NAME: (constants.CONFIG_SECTION_DATABASE, 'string', 'emby_toolkit'),

    # [Emby]
    constants.CONFIG_OPTION_EMBY_SERVER_URL: (constants.CONFIG_SECTION_EMBY, 'string', ""),
    constants.CONFIG_OPTION_EMBY_API_KEY: (constants.CONFIG_SECTION_EMBY, 'string', ""),
    constants.CONFIG_OPTION_EMBY_USER_ID: (constants.CONFIG_SECTION_EMBY, 'string', ""),
    constants.CONFIG_OPTION_REFRESH_AFTER_UPDATE: (constants.CONFIG_SECTION_EMBY, 'boolean', True),
    constants.CONFIG_OPTION_EMBY_LIBRARIES_TO_PROCESS: (constants.CONFIG_SECTION_EMBY, 'list', []),

    # [ReverseProxy]
    constants.CONFIG_OPTION_PROXY_ENABLED: (constants.CONFIG_SECTION_REVERSE_PROXY, 'boolean', False),
    constants.CONFIG_OPTION_PROXY_PORT: (constants.CONFIG_SECTION_REVERSE_PROXY, 'int', 8097),
    constants.CONFIG_OPTION_PROXY_MERGE_NATIVE: (constants.CONFIG_SECTION_REVERSE_PROXY, 'boolean', True),
    constants.CONFIG_OPTION_PROXY_NATIVE_VIEW_SELECTION: (constants.CONFIG_SECTION_REVERSE_PROXY, 'list', []),
    constants.CONFIG_OPTION_PROXY_NATIVE_VIEW_ORDER: (constants.CONFIG_SECTION_REVERSE_PROXY, 'str', 'before'),
    constants.CONFIG_OPTION_PROXY_302_REDIRECT_URL: (constants.CONFIG_SECTION_REVERSE_PROXY, 'string', ""),
    constants.CONFIG_OPTION_PROXY_MERGE_NATIVE: (constants.CONFIG_SECTION_REVERSE_PROXY, 'boolean', True),
    constants.CONFIG_OPTION_PROXY_NATIVE_VIEW_SELECTION: (constants.CONFIG_SECTION_REVERSE_PROXY, 'list', []),
    constants.CONFIG_OPTION_PROXY_NATIVE_VIEW_ORDER: (constants.CONFIG_SECTION_REVERSE_PROXY, 'str', 'before'),
    constants.CONFIG_OPTION_PROXY_REDIRECT_PATHS: (constants.CONFIG_SECTION_REVERSE_PROXY, 'list', []),

    # [TMDB]
    constants.CONFIG_OPTION_TMDB_API_KEY: (constants.CONFIG_SECTION_TMDB, 'string', ""),
    constants.CONFIG_OPTION_GITHUB_TOKEN: (constants.CONFIG_SECTION_GITHUB, 'string', ""),

    # [DoubanAPI]
    constants.CONFIG_OPTION_DOUBAN_DEFAULT_COOLDOWN: (constants.CONFIG_SECTION_API_DOUBAN, 'float', 1.0),
    constants.CONFIG_OPTION_DOUBAN_COOKIE: (constants.CONFIG_SECTION_API_DOUBAN, 'string', ""),

    # [MoviePilot]
    constants.CONFIG_OPTION_MOVIEPILOT_URL: (constants.CONFIG_SECTION_MOVIEPILOT, 'string', ""),
    constants.CONFIG_OPTION_MOVIEPILOT_USERNAME: (constants.CONFIG_SECTION_MOVIEPILOT, 'string', ""),
    constants.CONFIG_OPTION_MOVIEPILOT_PASSWORD: (constants.CONFIG_SECTION_MOVIEPILOT, 'string', ""),
    constants.CONFIG_OPTION_AUTOSUB_ENABLED: (constants.CONFIG_SECTION_MOVIEPILOT, 'boolean', False),
    
    # [LocalDataSource]
    constants.CONFIG_OPTION_LOCAL_DATA_PATH: (constants.CONFIG_SECTION_LOCAL_DATA, 'string', ""),

    # [General]
    "delay_between_items_sec": ("General", 'float', 0.5),
    constants.CONFIG_OPTION_MIN_SCORE_FOR_REVIEW: ("General", 'float', constants.DEFAULT_MIN_SCORE_FOR_REVIEW),
    constants.CONFIG_OPTION_AUTO_LOCK_CAST: ("General", 'boolean', True),
    constants.CONFIG_OPTION_MAX_ACTORS_TO_PROCESS: ("General", 'int', constants.DEFAULT_MAX_ACTORS_TO_PROCESS),

    # [Network] 
    constants.CONFIG_OPTION_NETWORK_PROXY_ENABLED: (constants.CONFIG_SECTION_NETWORK, 'boolean', False),
    constants.CONFIG_OPTION_NETWORK_HTTP_PROXY: (constants.CONFIG_SECTION_NETWORK, 'string', ""),
    "user_agent": ("Network", 'string', 'Mozilla/5.0 ...'),
    "accept_language": ("Network", 'string', 'zh-CN,zh;q=0.9,en;q=0.8'),

    # [AITranslation]
    constants.CONFIG_OPTION_AI_TRANSLATION_ENABLED: (constants.CONFIG_SECTION_AI_TRANSLATION, 'boolean', False),
    constants.CONFIG_OPTION_AI_PROVIDER: (constants.CONFIG_SECTION_AI_TRANSLATION, 'string', "openai"),
    constants.CONFIG_OPTION_AI_API_KEY: (constants.CONFIG_SECTION_AI_TRANSLATION, 'string', ""),
    constants.CONFIG_OPTION_AI_MODEL_NAME: (constants.CONFIG_SECTION_AI_TRANSLATION, 'string', "deepseek-ai/DeepSeek-V2.5"),
    constants.CONFIG_OPTION_AI_BASE_URL: (constants.CONFIG_SECTION_AI_TRANSLATION, 'string', "https://api.siliconflow.cn/v1"),
    constants.CONFIG_OPTION_AI_TRANSLATION_MODE: (constants.CONFIG_SECTION_AI_TRANSLATION, 'string', 'fast'),

    # [Scheduler] - ★★★ 现在这里只剩下我们需要的任务链配置 ★★★
    constants.CONFIG_OPTION_TASK_CHAIN_ENABLED: (constants.CONFIG_SECTION_SCHEDULER, 'boolean', False),
    constants.CONFIG_OPTION_TASK_CHAIN_CRON: (constants.CONFIG_SECTION_SCHEDULER, 'string', "0 2 * * *"),
    constants.CONFIG_OPTION_TASK_CHAIN_SEQUENCE: (constants.CONFIG_SECTION_SCHEDULER, 'list', []),
    
    # [Authentication]
    constants.CONFIG_OPTION_AUTH_ENABLED: (constants.CONFIG_SECTION_AUTH, 'boolean', False),
    constants.CONFIG_OPTION_AUTH_USERNAME: (constants.CONFIG_SECTION_AUTH, 'string', constants.DEFAULT_USERNAME),
    
    # [Actor]
    constants.CONFIG_OPTION_ACTOR_ROLE_ADD_PREFIX: (constants.CONFIG_SECTION_ACTOR, 'boolean', False),

    # [Logging]
    constants.CONFIG_OPTION_LOG_ROTATION_SIZE_MB: (constants.CONFIG_SECTION_LOGGING, 'int', constants.DEFAULT_LOG_ROTATION_SIZE_MB),
    constants.CONFIG_OPTION_LOG_ROTATION_BACKUPS: (constants.CONFIG_SECTION_LOGGING, 'int', constants.DEFAULT_LOG_ROTATION_BACKUPS),
}

# --- 全局配置字典 ---
# 其他模块可以通过 from config_manager import APP_CONFIG 来访问
APP_CONFIG: Dict[str, Any] = {}

# --- 加载配置 ---
def load_config() -> Tuple[Dict[str, Any], bool]:
    """
    【清单驱动+环境变量覆盖版】
    从 config.ini 加载配置，然后用环境变量覆盖数据库配置。
    """
    global APP_CONFIG
    config_parser = configparser.ConfigParser()
    is_first_run = not os.path.exists(CONFIG_FILE_PATH)

    if not is_first_run:
        try:
            config_parser.read(CONFIG_FILE_PATH, encoding='utf-8')
        except Exception as e:
            logger.error(f"解析配置文件时出错: {e}", exc_info=True)

    app_cfg = {}
    
    # 步骤 1: 像以前一样，从 config.ini 加载所有配置
    for key, (section, type, default) in CONFIG_DEFINITION.items():
        if not config_parser.has_section(section):
            config_parser.add_section(section)
            
        if type == 'boolean':
            if key == constants.CONFIG_OPTION_AUTH_ENABLED and is_first_run:
                app_cfg[key] = True
            else:
                app_cfg[key] = config_parser.getboolean(section, key, fallback=default)
        elif type == 'int':
            app_cfg[key] = config_parser.getint(section, key, fallback=default)
        elif type == 'float':
            app_cfg[key] = config_parser.getfloat(section, key, fallback=default)
        elif type == 'list':
            value_str = config_parser.get(section, key, fallback=",".join(map(str, default)))
            app_cfg[key] = [item.strip() for item in value_str.split(',') if item.strip()]
        else: # string
            app_cfg[key] = config_parser.get(section, key, fallback=default)

    # ★★★ 步骤 2: 检查并应用环境变量覆盖 ★★★
    logger.info("检查数据库环境变量...")
    
    # 定义环境变量和内部配置键的映射关系
    env_to_config_map = {
        constants.ENV_VAR_DB_HOST: constants.CONFIG_OPTION_DB_HOST,
        constants.ENV_VAR_DB_PORT: constants.CONFIG_OPTION_DB_PORT,
        constants.ENV_VAR_DB_USER: constants.CONFIG_OPTION_DB_USER,
        constants.ENV_VAR_DB_PASSWORD: constants.CONFIG_OPTION_DB_PASSWORD,
        constants.ENV_VAR_DB_NAME: constants.CONFIG_OPTION_DB_NAME,
    }

    for env_var, config_key in env_to_config_map.items():
        env_value = os.environ.get(env_var)
        if env_value:
            # 如果环境变量存在，就覆盖掉从 config.ini 读到的值
            logger.info(f"检测到环境变量 '{env_var}'，将覆盖配置 '{config_key}'。")
            # 特殊处理端口号，需要是整数
            if config_key == constants.CONFIG_OPTION_DB_PORT:
                try:
                    app_cfg[config_key] = int(env_value)
                except ValueError:
                    logger.error(f"环境变量 '{env_var}' 的值 '{env_value}' 不是一个有效的端口号，已忽略。")
            else:
                app_cfg[config_key] = env_value

    APP_CONFIG = app_cfg.copy()
    logger.info("全局配置 APP_CONFIG 已加载/更新。")
    return app_cfg, is_first_run

# --- 保存配置 ---
def save_config(new_config: Dict[str, Any]):
    """【清单驱动版】将配置保存到 config.ini，并更新全局 APP_CONFIG。"""
    global APP_CONFIG
    config_parser = configparser.ConfigParser()
    
    # 遍历配置清单，自动设置所有配置项
    for key, (section, type, _) in CONFIG_DEFINITION.items():
        if not config_parser.has_section(section):
            config_parser.add_section(section)
        
        value = new_config.get(key)
        
        # 将值转换为适合写入ini文件的字符串格式
        if isinstance(value, bool):
            value_to_write = str(value).lower()
        elif isinstance(value, list):
            value_to_write = ",".join(map(str, value))
        else:
            value_to_write = str(value)
        
        # 处理百分号，防止 configparser 报错
        value_to_write = value_to_write.replace('%', '%%')
        config_parser.set(section, key, value_to_write)

    try:
        with open(CONFIG_FILE_PATH, 'w', encoding='utf-8') as configfile:
            config_parser.write(configfile)
        
        # 更新内存中的全局配置
        APP_CONFIG = new_config.copy()
        logger.info(f"配置已成功写入到 {CONFIG_FILE_PATH}，内存中的配置已同步。")
        
    except Exception as e:
        logger.error(f"保存配置文件时失败: {e}", exc_info=True)
        # 抛出异常，让调用者知道保存失败
        raise

# ★★★ 保存自定义主题 ★★★
def save_custom_theme(theme_data: dict):
    """
    将一个字典（自定义主题对象）保存为 config/custom_theme.json 文件。
    """
    # 使用已经定义好的 PERSISTENT_DATA_PATH 来构建路径
    custom_theme_path = os.path.join(PERSISTENT_DATA_PATH, 'custom_theme.json')
    try:
        with open(custom_theme_path, 'w', encoding='utf-8') as f:
            json.dump(theme_data, f, ensure_ascii=False, indent=4)
        logger.info(f"自定义主题已成功写入到: {custom_theme_path}")
    except Exception as e:
        logger.error(f"写入自定义主题文件失败: {e}", exc_info=True)
        raise
# ★★★ 加载自定义主题 ★★★
def load_custom_theme() -> dict:
    """
    从 config/custom_theme.json 文件加载自定义主题。
    如果文件不存在，返回一个空字典。
    """
    # 使用已经定义好的 PERSISTENT_DATA_PATH 来构建路径
    custom_theme_path = os.path.join(PERSISTENT_DATA_PATH, 'custom_theme.json')
    if not os.path.exists(custom_theme_path):
        return {}
    
    try:
        with open(custom_theme_path, 'r', encoding='utf-8') as f:
            theme_data = json.load(f)
            if isinstance(theme_data, dict):
                logger.debug(f"成功从 {custom_theme_path} 加载自定义主题。")
                return theme_data
            else:
                logger.warning(f"自定义主题文件 {custom_theme_path} 内容格式不正确，不是一个JSON对象。")
                return {}
    except json.JSONDecodeError:
        logger.error(f"解析自定义主题文件 {custom_theme_path} 失败，请检查JSON格式。")
        return {}
    except Exception as e:
        logger.error(f"读取自定义主题文件时发生未知错误: {e}", exc_info=True)
        return {}
# ★★★ 删除自定义主题 ★★★    
def delete_custom_theme() -> bool:
    """
    删除 custom_theme.json 文件。
    
    :return: 如果文件被成功删除或文件本就不存在，返回 True。如果删除失败，返回 False。
    """
    # ★★★ 核心修正：使用已经定义好的 PERSISTENT_DATA_PATH ★★★
    theme_file_path = os.path.join(PERSISTENT_DATA_PATH, 'custom_theme.json')
    try:
        if os.path.exists(theme_file_path):
            os.remove(theme_file_path)
            logger.info(f"✅ 成功删除自定义主题文件: {theme_file_path}")
        else:
            logger.info("尝试删除自定义主题文件，但文件本就不存在。")
        return True
    except OSError as e:
        logger.error(f"删除自定义主题文件时发生 I/O 错误: {e}", exc_info=True)
        return False
    
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