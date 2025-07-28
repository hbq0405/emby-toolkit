# constants.py

# ==============================================================================
# ✨ 应用基础信息 (Application Basics)
# ==============================================================================
APP_VERSION = "2.9.24"  # 更新版本号
GITHUB_REPO_OWNER = "hbq0405"  # 您的 GitHub 用户名
GITHUB_REPO_NAME = "emby-actor-processor" # 您的 GitHub 仓库名
DEBUG_MODE = True     # 开发模式开关，部署时应设为 False
WEB_APP_PORT = 5257    # Web UI 监听的端口
CONFIG_FILE_NAME = "config.ini" # 主配置文件名
DB_NAME = "emby_actor_processor.sqlite" # 主数据库文件名
TIMEZONE = "Asia/Shanghai" # 应用使用的时区，用于计划任务等
#数据库
CONFIG_SECTION_DATABASE = "Database"
CONFIG_OPTION_DB_PATH = "db_path"


# ==============================================================================
# ✨ Emby 服务器连接配置 (Emby Connection)
# ==============================================================================
CONFIG_SECTION_EMBY = "Emby"
CONFIG_OPTION_EMBY_SERVER_URL = "emby_server_url"       # Emby服务器地址
CONFIG_OPTION_EMBY_API_KEY = "emby_api_key"             # Emby API密钥
CONFIG_OPTION_EMBY_USER_ID = "emby_user_id"             # 用于操作的Emby用户ID
CONFIG_OPTION_EMBY_LIBRARIES_TO_PROCESS = "libraries_to_process" # 需要处理的媒体库名称列表

# ==============================================================================
# ✨ 数据处理流程配置 (Processing Workflow)
# ==============================================================================
CONFIG_SECTION_PROCESSING = "Processing"
CONFIG_OPTION_REFRESH_AFTER_UPDATE = "refresh_emby_after_update" # 处理完成后是否自动刷新Emby项目
CONFIG_OPTION_PROCESS_EPISODES = "process_episodes"             # 在神医模式下，是否处理剧集的子项目（如季、集）
CONFIG_OPTION_SYNC_IMAGES = "sync_images"                       # 在神医模式下，是否同步封面、海报等图片
CONFIG_OPTION_MAX_ACTORS_TO_PROCESS = "max_actors_to_process"   # 每个媒体项目处理的演员数量上限
DEFAULT_MAX_ACTORS_TO_PROCESS = 50                              # 默认的演员数量上限
CONFIG_OPTION_MIN_SCORE_FOR_REVIEW = "min_score_for_review"     # 低于此评分的项目将进入手动处理列表
DEFAULT_MIN_SCORE_FOR_REVIEW = 6.0                              # 默认的最低分

# ==============================================================================
# ✨ 外部API与数据源配置 (External APIs & Data Sources)
# ==============================================================================
# --- TMDb ---
CONFIG_SECTION_TMDB = "TMDB"
CONFIG_OPTION_TMDB_API_KEY = "tmdb_api_key" # TMDb API密钥

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

# ==============================================================================
# ✨ 翻译功能配置 (Translation)
# ==============================================================================
CONFIG_SECTION_TRANSLATION = "Translation"
CONFIG_OPTION_TRANSLATOR_ENGINES = "translator_engines_order" # 传统翻译引擎的使用顺序
AVAILABLE_TRANSLATOR_ENGINES = ['bing', 'google', 'baidu', 'alibaba', 'youdao', 'tencent'] # 所有可选的翻译引擎
DEFAULT_TRANSLATOR_ENGINES_ORDER = ['bing', 'google', 'baidu'] # 默认的翻译引擎顺序
CONFIG_OPTION_TRANSLATION_FAILURE_RETRY_DAYS = "translation_failure_retry_days" # 翻译失败的词条，多少天后可以重试
DEFAULT_TRANSLATION_FAILURE_RETRY_DAYS = 1 # 默认1天

# --- AI 翻译 ---
CONFIG_SECTION_AI_TRANSLATION = "AITranslation"
CONFIG_OPTION_AI_TRANSLATION_ENABLED = "ai_translation_enabled" # 是否启用AI翻译
CONFIG_OPTION_AI_PROVIDER = "ai_provider"                       # AI服务提供商 (如 'siliconflow', 'openai')
CONFIG_OPTION_AI_API_KEY = "ai_api_key"                         # AI服务的API密钥
CONFIG_OPTION_AI_MODEL_NAME = "ai_model_name"                   # 使用的AI模型名称 (如 'Qwen/Qwen2-7B-Instruct')
CONFIG_OPTION_AI_BASE_URL = "ai_base_url"                       # AI服务的API基础URL
CONFIG_OPTION_AI_TRANSLATION_MODE = "ai_translation_mode"       # AI翻译模式 ('fast' 或 'quality')

# ==============================================================================
# ✨ 计划任务配置 (Scheduler)
# ==============================================================================
CONFIG_SECTION_SCHEDULER = "Scheduler"

# --- 全量扫描任务 ---
CONFIG_OPTION_SCHEDULE_ENABLED = "schedule_enabled"             # 是否启用全量扫描定时任务
CONFIG_OPTION_SCHEDULE_CRON = "schedule_cron"                   # 全量扫描的CRON表达式
CONFIG_OPTION_SCHEDULE_FORCE_REPROCESS = "schedule_force_reprocess" # 定时任务是否强制重处理所有项目

# --- 同步演员映射表任务 ---
CONFIG_OPTION_SCHEDULE_SYNC_MAP_ENABLED = "schedule_sync_map_enabled" # 是否启用同步演员映射表任务
CONFIG_OPTION_SCHEDULE_SYNC_MAP_CRON = "schedule_sync_map_cron"       # 同步映射表的CRON表达式

# --- 智能追剧任务 ---
CONFIG_OPTION_SCHEDULE_WATCHLIST_ENABLED = "schedule_watchlist_enabled" # 是否启用智能追剧任务
CONFIG_OPTION_SCHEDULE_WATCHLIST_CRON = "schedule_watchlist_cron"       # 智能追剧的CRON表达式
DEFAULT_SCHEDULE_WATCHLIST_CRON = "0 */6 * * *" # 默认每6小时执行一次

# --- 演员处理相关配置 ---
CONFIG_SECTION_ACTOR = "Actor"
CONFIG_OPTION_ACTOR_ROLE_ADD_PREFIX = "actor_role_add_prefix"

# --- 演员订阅 ---
CONFIG_OPTION_SCHEDULE_ACTOR_TRACKING_ENABLED = "schedule_actor_tracking_enabled"
CONFIG_OPTION_SCHEDULE_ACTOR_TRACKING_CRON = "schedule_actor_tracking_cron"

# --- 演员元数据增强任务 ---
CONFIG_OPTION_SCHEDULE_ENRICH_ALIASES_ENABLED = "schedule_enrich_aliases_enabled" # 是否启用演员元数据增强任务
CONFIG_OPTION_SCHEDULE_ENRICH_ALIASES_CRON = "schedule_enrich_aliases_cron"       # 演员元数据增强的CRON表达式
CONFIG_OPTION_SCHEDULE_ENRICH_DURATION_MINUTES = "schedule_enrich_run_duration_minutes" # 演员元数据增强任务的运行时长常量
CONFIG_OPTION_SCHEDULE_ENRICH_SYNC_INTERVAL_DAYS = "schedule_enrich_sync_interval_days"
DEFAULT_ENRICH_ALIASES_SYNC_INTERVAL_DAYS = 7 # 默认冷却7天

# --- 演员名翻译查漏补缺任务 ---
CONFIG_OPTION_SCHEDULE_ACTOR_CLEANUP_ENABLED = "schedule_actor_cleanup_enabled" # 是否启用演员名翻译查漏补缺任务
CONFIG_OPTION_SCHEDULE_ACTOR_CLEANUP_CRON = "schedule_actor_cleanup_cron"       # 演员名翻译查漏补缺的CRON表达式
DEFAULT_SCHEDULE_ACTOR_CLEANUP_CRON = "0 4 * * *" # 默认每天凌晨4点执行

# --- 智能订阅任务 ---
CONFIG_OPTION_SCHEDULE_AUTOSUB_ENABLED = "schedule_autosub_enabled" # 是否启用智能订阅任务
CONFIG_OPTION_SCHEDULE_AUTOSUB_CRON = "schedule_autosub_cron"       # 智能订阅的CRON表达式
DEFAULT_SCHEDULE_AUTOSUB_CRON = "0 5 * * *" # 默认每天凌晨5点执行

# --- 电影合集刷新任务 ---
CONFIG_OPTION_SCHEDULE_REFRESH_COLLECTIONS_ENABLED = "schedule_refresh_collections_enabled"
CONFIG_OPTION_SCHEDULE_REFRESH_COLLECTIONS_CRON = "schedule_refresh_collections_cron"
DEFAULT_SCHEDULE_REFRESH_COLLECTIONS_CRON = "0 2 * * *" # 默认每天凌晨2点
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