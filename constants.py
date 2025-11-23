# constants.py

# ==============================================================================
# ✨ 应用基础信息 (Application Basics)
# ==============================================================================
APP_VERSION = "5.3.0"  # 更新版本号
GITHUB_REPO_OWNER = "hbq0405"  # 您的 GitHub 用户名
GITHUB_REPO_NAME = "emby-toolkit" # 您的 GitHub 仓库名
DEBUG_MODE = True     # 开发模式开关，部署时应设为 False
WEB_APP_PORT = 5257    # Web UI 监听的端口
CONFIG_FILE_NAME = "config.ini" # 主配置文件名
TIMEZONE = "Asia/Shanghai" # 应用使用的时区，用于计划任务等

# ==============================================================================
# ✨ 数据库配置 (Database) - PostgreSQL
# ==============================================================================
CONFIG_SECTION_DATABASE = "Database"
CONFIG_OPTION_DB_HOST = "db_host"
CONFIG_OPTION_DB_PORT = "db_port"
CONFIG_OPTION_DB_USER = "db_user"
CONFIG_OPTION_DB_PASSWORD = "db_password"
CONFIG_OPTION_DB_NAME = "db_name"
ENV_VAR_DB_HOST = "DB_HOST"
ENV_VAR_DB_PORT = "DB_PORT"
ENV_VAR_DB_USER = "DB_USER"
ENV_VAR_DB_PASSWORD = "DB_PASSWORD"
ENV_VAR_DB_NAME = "DB_NAME"

# ==============================================================================
# ✨ 通知服务 (Notification Services)
# ==============================================================================
CONFIG_SECTION_TELEGRAM = "Telegram"
CONFIG_OPTION_TELEGRAM_BOT_TOKEN = "telegram_bot_token"
CONFIG_OPTION_TELEGRAM_CHANNEL_ID = "telegram_channel_id"

# ==============================================================================
# ✨ 反向代理配置 (Reverse Proxy)
# ==============================================================================
CONFIG_SECTION_REVERSE_PROXY = "ReverseProxy"
CONFIG_OPTION_PROXY_ENABLED = "proxy_enabled"
CONFIG_OPTION_PROXY_PORT = "proxy_port"
CONFIG_OPTION_PROXY_MERGE_NATIVE = "proxy_merge_native_libraries"
CONFIG_OPTION_PROXY_NATIVE_VIEW_SELECTION = "proxy_native_view_selection"  # List[str]
CONFIG_OPTION_PROXY_NATIVE_VIEW_ORDER = "proxy_native_view_order"  # str, 'before' or 'after'
CONFIG_OPTION_PROXY_302_REDIRECT_URL = "proxy_302_redirect_url"
CONFIG_OPTION_PROXY_NATIVE_VIEW_ORDER = "proxy_native_view_order"  # str, 'before' or 'after'

# ==============================================================================
# ✨ Emby 服务器连接配置 (Emby Connection)
# ==============================================================================
CONFIG_SECTION_EMBY = "Emby"
CONFIG_OPTION_EMBY_SERVER_URL = "emby_server_url"       # Emby服务器地址
CONFIG_OPTION_EMBY_API_KEY = "emby_api_key"             # Emby API密钥
CONFIG_OPTION_EMBY_USER_ID = "emby_user_id"             # 用于操作的Emby用户ID
CONFIG_OPTION_EMBY_API_TIMEOUT = "emby_api_timeout"     # Emby API 超时时间 
CONFIG_OPTION_EMBY_LIBRARIES_TO_PROCESS = "libraries_to_process" # 需要处理的媒体库名称列表
CONFIG_OPTION_EMBY_ADMIN_USER = "emby_admin_user"       # (可选) 用于自动登录获取令牌的管理员用户名
CONFIG_OPTION_EMBY_ADMIN_PASS = "emby_admin_pass"       # (可选) 用于自动登录获取令牌的管理员密码

# ==============================================================================
# ✨ 数据处理流程配置 (Processing Workflow)
# ==============================================================================
CONFIG_SECTION_PROCESSING = "Processing"
CONFIG_OPTION_MAX_ACTORS_TO_PROCESS = "max_actors_to_process"   # 每个媒体项目处理的演员数量上限
DEFAULT_MAX_ACTORS_TO_PROCESS = 50                              # 默认的演员数量上限
CONFIG_OPTION_MIN_SCORE_FOR_REVIEW = "min_score_for_review"     # 低于此评分的项目将进入手动处理列表
DEFAULT_MIN_SCORE_FOR_REVIEW = 6.0                              # 默认的最低分
CONFIG_OPTION_REMOVE_ACTORS_WITHOUT_AVATARS = "remove_actors_without_avatars" # 是否移除无头像的演员

# ==============================================================================
# ✨ 外部API与数据源配置 (External APIs & Data Sources)
# ==============================================================================
# --- TMDb ---
CONFIG_SECTION_TMDB = "TMDB"
CONFIG_OPTION_TMDB_API_KEY = "tmdb_api_key" # TMDb API密钥
CONFIG_OPTION_TMDB_API_BASE_URL = "tmdb_api_base_url" # TMDb API基础URL
ENV_VAR_TMDB_API_BASE_URL = "TMDB_API_BASE_URL" # TMDb API基础URL环境变量
# --- GitHub (用于版本检查) ---
CONFIG_SECTION_GITHUB = "GitHub"
CONFIG_OPTION_GITHUB_TOKEN = "github_token" # 用于提高API速率限制的个人访问令牌

# --- 豆瓣 API ---
CONFIG_SECTION_API_DOUBAN = "DoubanAPI"
DOUBAN_API_AVAILABLE = True # 一个硬编码的开关，表示豆瓣API功能是可用的
CONFIG_OPTION_DOUBAN_DEFAULT_COOLDOWN = "api_douban_default_cooldown_seconds" # 调用豆瓣API的冷却时间
CONFIG_OPTION_DOUBAN_COOKIE = "douban_cookie" # 用于身份验证的豆瓣登录Cookie

# --- 本地数据源 (神医模式) ---
CONFIG_SECTION_LOCAL_DATA = "LocalDataSource"
CONFIG_OPTION_LOCAL_DATA_PATH = "local_data_path" # 本地JSON元数据（TMDbHelper等生成）的根路径

# --- MoviePilot ---
CONFIG_SECTION_MOVIEPILOT = "MoviePilot"
CONFIG_OPTION_MOVIEPILOT_URL = "moviepilot_url"
CONFIG_OPTION_MOVIEPILOT_USERNAME = "moviepilot_username"
CONFIG_OPTION_MOVIEPILOT_PASSWORD = "moviepilot_password"
# --- 智能订阅相关配置 ---
CONFIG_OPTION_AUTOSUB_ENABLED = "autosub_enabled" # 智能订阅总开关
CONFIG_OPTION_GAP_FILL_RESUBSCRIBE_ENABLED = "gap_fill_resubscribe_enabled"
CONFIG_OPTION_RESUBSCRIBE_DAILY_CAP = "resubscribe_daily_cap"
CONFIG_OPTION_RESUBSCRIBE_DELAY_SECONDS = "resubscribe_delay_seconds"
CONFIG_OPTION_USE_CUSTOM_RESUBSCRIBE = "use_custom_resubscribe"
CONFIG_OPTION_MOVIE_SUBSCRIPTION_DELAY_DAYS = "movie_subscription_delay_days"
CONFIG_OPTION_AUTOCANCEL_SUBSCRIBED_DAYS = "autocancel_subscribed_days"

# --- AI 翻译 ---
CONFIG_SECTION_AI_TRANSLATION = "AITranslation"
CONFIG_OPTION_AI_TRANSLATION_ENABLED = "ai_translation_enabled" # 是否启用AI翻译
CONFIG_OPTION_AI_PROVIDER = "ai_provider"                       # AI服务提供商 (如 'siliconflow', 'openai')
CONFIG_OPTION_AI_API_KEY = "ai_api_key"                         # AI服务的API密钥
CONFIG_OPTION_AI_MODEL_NAME = "ai_model_name"                   # 使用的AI模型名称 (如 'Qwen/Qwen2-7B-Instruct')
CONFIG_OPTION_AI_BASE_URL = "ai_base_url"                       # AI服务的API基础URL
CONFIG_OPTION_AI_TRANSLATION_MODE = "ai_translation_mode"       # AI翻译模式 ('fast' 或 'quality')

# ==============================================================================
# ✨ 网络配置 (Network) - ★★★ 新增部分 ★★★
# ==============================================================================
CONFIG_SECTION_NETWORK = "Network"
CONFIG_OPTION_NETWORK_PROXY_ENABLED = "network_proxy_enabled"
CONFIG_OPTION_NETWORK_HTTP_PROXY = "network_http_proxy_url"

# ==============================================================================
# ✨ 计划任务配置 (Scheduler)
# ==============================================================================
CONFIG_SECTION_SCHEDULER = "Scheduler"

# --- 高频任务链 ---
CONFIG_OPTION_TASK_CHAIN_ENABLED = "task_chain_enabled"
CONFIG_OPTION_TASK_CHAIN_CRON = "task_chain_cron"
CONFIG_OPTION_TASK_CHAIN_SEQUENCE = "task_chain_sequence"
CONFIG_OPTION_TASK_CHAIN_MAX_RUNTIME_MINUTES = "task_chain_max_runtime_minutes"

# --- 低频任务链配置 ---
CONFIG_OPTION_TASK_CHAIN_LOW_FREQ_ENABLED = "task_chain_low_freq_enabled"
CONFIG_OPTION_TASK_CHAIN_LOW_FREQ_CRON = "task_chain_low_freq_cron"
CONFIG_OPTION_TASK_CHAIN_LOW_FREQ_SEQUENCE = "task_chain_low_freq_sequence"
CONFIG_OPTION_TASK_CHAIN_LOW_FREQ_MAX_RUNTIME_MINUTES = "task_chain_low_freq_max_runtime_minutes"



# --- 演员前缀 ---
CONFIG_SECTION_ACTOR = "Actor"
CONFIG_OPTION_ACTOR_ROLE_ADD_PREFIX = "actor_role_add_prefix"
CONFIG_OPTION_ACTOR_MAIN_ROLE_ONLY = "actor_main_role_only"


# --- 日志配置 ---
CONFIG_SECTION_LOGGING = "Logging"
CONFIG_OPTION_LOG_ROTATION_SIZE_MB = "log_rotation_size_mb"
CONFIG_OPTION_LOG_ROTATION_BACKUPS = "log_rotation_backup_count"
DEFAULT_LOG_ROTATION_SIZE_MB = 5
DEFAULT_LOG_ROTATION_BACKUPS = 10
# ==============================================================================
# ✨ 内部常量与映射 (Internal Constants & Mappings)
# ==============================================================================
# --- 用户认证 (如果未来启用) ---
CONFIG_SECTION_AUTH = "Authentication"
CONFIG_OPTION_AUTH_ENABLED = "auth_enabled"
CONFIG_OPTION_AUTH_USERNAME = "username"
DEFAULT_USERNAME = "admin"

# ★★★ Emby：用户管理相关配置 ★★★
CONFIG_SECTION_USER_MANAGEMENT = "UserManagement"
CONFIG_OPTION_REGISTRATION_REDIRECT_URL = "registration_redirect_url"

# --- 语言代码 ---
CHINESE_LANG_CODES = ["zh", "zh-cn", "zh-hans", "cmn", "yue", "cn", "zh-sg", "zh-tw", "zh-hk"]

# --- 状态文本映射 (可能用于UI显示) ---
ACTOR_STATUS_TEXT_MAP = {
    "ok": "已处理",
    "name_untranslated": "演员名未翻译",
    "character_untranslated": "角色名未翻译",
    "name_char_untranslated": "演员名和角色名均未翻译",
    "pending_translation": "待翻译",
    "parent_failed": "媒体项处理失败",
    "unknown": "未知状态"
}

# --- 数据源信息映射 (可能用于动态构建UI或逻辑) ---
SOURCE_API_MAP = {
    "Douban": {
        "name": "豆瓣",
        "search_types": {
            "movie": {"title": "电影", "season": False},
            "tv": {"title": "电视剧", "season": True},
        },
    },
}