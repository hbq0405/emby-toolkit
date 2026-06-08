import importlib
import sys
import types
import unittest
from unittest import mock


def _install_stubs():
    for module_name in [
        "config_manager",
        "constants",
        "database",
        "database.settings_db",
        "database.user_db",
        "handler.telegram",
        "tasks.system_update",
    ]:
        sys.modules.pop(module_name, None)

    constants_mod = types.ModuleType("constants")
    constants_mod.CONFIG_FILE_NAME = "config.ini"
    constants_mod.CONFIG_OPTION_DB_HOST = "db_host"
    constants_mod.CONFIG_OPTION_DB_PORT = "db_port"
    constants_mod.CONFIG_OPTION_DB_USER = "db_user"
    constants_mod.CONFIG_OPTION_DB_PASSWORD = "db_password"
    constants_mod.CONFIG_OPTION_DB_NAME = "db_name"
    constants_mod.CONFIG_OPTION_AUTH_ENABLED = "auth_enabled"
    constants_mod.CONFIG_OPTION_AUTH_USERNAME = "username"
    constants_mod.DEFAULT_USERNAME = "admin"
    constants_mod.CONFIG_SECTION_DATABASE = "Database"
    constants_mod.CONFIG_SECTION_AUTH = "Authentication"
    constants_mod.CONFIG_SECTION_EMBY = "Emby"
    constants_mod.CONFIG_SECTION_REVERSE_PROXY = "ReverseProxy"
    constants_mod.CONFIG_SECTION_TMDB = "TMDB"
    constants_mod.CONFIG_SECTION_GITHUB = "GitHub"
    constants_mod.CONFIG_SECTION_API_DOUBAN = "DoubanAPI"
    constants_mod.CONFIG_SECTION_MONITOR = "Monitor"
    constants_mod.CONFIG_SECTION_115 = "115"
    constants_mod.CONFIG_SECTION_NETWORK = "Network"
    constants_mod.CONFIG_SECTION_AI_TRANSLATION = "AITranslation"
    constants_mod.CONFIG_SECTION_SCHEDULER = "Scheduler"
    constants_mod.CONFIG_SECTION_ACTOR = "Actor"
    constants_mod.CONFIG_SECTION_LOGGING = "Logging"
    constants_mod.CONFIG_SECTION_TELEGRAM = "Telegram"
    constants_mod.CONFIG_OPTION_GITHUB_TOKEN = "github_token"
    constants_mod.CONFIG_OPTION_SYSTEM_UPDATE_STRATEGY = "system_update_strategy"
    constants_mod.CONFIG_OPTION_SYSTEM_UPDATE_HELPER_IMAGE = "system_update_helper_image"
    constants_mod.CONFIG_OPTION_TELEGRAM_BOT_TOKEN = "telegram_bot_token"
    constants_mod.CONFIG_OPTION_TELEGRAM_CHANNEL_ID = "telegram_channel_id"
    constants_mod.CONFIG_OPTION_TELEGRAM_MENU_TASKS = "tg_menu_tasks"
    constants_mod.CONFIG_OPTION_TELEGRAM_NOTIFY_TYPES = "telegram_notify_types"
    constants_mod.DEFAULT_TELEGRAM_MENU_TASKS = []
    constants_mod.DEFAULT_TELEGRAM_NOTIFY_TYPES = []
    constants_mod.CONFIG_OPTION_EMBY_SERVER_URL = "emby_server_url"
    constants_mod.CONFIG_OPTION_EMBY_PUBLIC_URL = "emby_public_url"
    constants_mod.CONFIG_OPTION_EMBY_API_KEY = "emby_api_key"
    constants_mod.CONFIG_OPTION_EMBY_USER_ID = "emby_user_id"
    constants_mod.CONFIG_OPTION_EMBY_API_TIMEOUT = "emby_api_timeout"
    constants_mod.CONFIG_OPTION_EMBY_LIBRARIES_TO_PROCESS = "libraries_to_process"
    constants_mod.CONFIG_OPTION_EMBY_ADMIN_USER = "emby_admin_user"
    constants_mod.CONFIG_OPTION_EMBY_ADMIN_PASS = "emby_admin_pass"
    constants_mod.CONFIG_OPTION_PROXY_ENABLED = "proxy_enabled"
    constants_mod.CONFIG_OPTION_PROXY_PORT = "proxy_port"
    constants_mod.CONFIG_OPTION_PROXY_MERGE_NATIVE = "proxy_merge_native_libraries"
    constants_mod.CONFIG_OPTION_PROXY_NATIVE_VIEW_SELECTION = "proxy_native_view_selection"
    constants_mod.CONFIG_OPTION_PROXY_NATIVE_VIEW_ORDER = "proxy_native_view_order"
    constants_mod.CONFIG_OPTION_PROXY_SHOW_MISSING_PLACEHOLDERS = "proxy_show_missing_placeholders"
    constants_mod.CONFIG_OPTION_PROXY_302_REDIRECT_URL = "proxy_302_redirect_url"
    constants_mod.CONFIG_OPTION_TMDB_API_KEY = "tmdb_api_key"
    constants_mod.CONFIG_OPTION_TMDB_API_BASE_URL = "tmdb_api_base_url"
    constants_mod.CONFIG_OPTION_TMDB_INCLUDE_ADULT = "tmdb_include_adult"
    constants_mod.CONFIG_OPTION_TMDB_IMAGE_LANGUAGE_PREFERENCE = "tmdb_image_language_preference"
    constants_mod.CONFIG_OPTION_DOUBAN_DEFAULT_COOLDOWN = "api_douban_default_cooldown_seconds"
    constants_mod.CONFIG_OPTION_DOUBAN_COOKIE = "douban_cookie"
    constants_mod.CONFIG_OPTION_DOUBAN_ENABLE_ONLINE_API = "douban_enable_online_api"
    constants_mod.CONFIG_OPTION_MONITOR_ENABLED = "monitor_enabled"
    constants_mod.CONFIG_OPTION_MONITOR_PATHS = "monitor_paths"
    constants_mod.CONFIG_OPTION_MONITOR_EXTENSIONS = "monitor_extensions"
    constants_mod.DEFAULT_MONITOR_EXTENSIONS = []
    constants_mod.CONFIG_OPTION_MONITOR_SCAN_LOOKBACK_DAYS = "monitor_scan_lookback_days"
    constants_mod.DEFAULT_MONITOR_SCAN_LOOKBACK_DAYS = 1
    constants_mod.CONFIG_OPTION_MONITOR_EXCLUDE_DIRS = "monitor_exclude_dirs"
    constants_mod.DEFAULT_MONITOR_EXCLUDE_DIRS = []
    constants_mod.CONFIG_OPTION_MONITOR_EXCLUDE_REFRESH_DELAY = "monitor_exclude_refresh_delay"
    constants_mod.DEFAULT_MONITOR_EXCLUDE_REFRESH_DELAY = 0
    constants_mod.CONFIG_OPTION_MONITOR_SHA1_PC_SEARCH = "monitor_sha1_pc_search"
    constants_mod.CONFIG_OPTION_115_SAVE_PATH_CID = "p115_save_path_cid"
    constants_mod.CONFIG_OPTION_115_SAVE_PATH_NAME = "p115_save_path_name"
    constants_mod.CONFIG_OPTION_115_UNRECOGNIZED_CID = "p115_unrecognized_cid"
    constants_mod.CONFIG_OPTION_115_UNRECOGNIZED_NAME = "p115_unrecognized_name"
    constants_mod.CONFIG_OPTION_115_MEDIA_ROOT_NAME = "p115_media_root_name"
    constants_mod.CONFIG_OPTION_115_INTERVAL = "p115_request_interval"
    constants_mod.CONFIG_OPTION_115_MAX_WORKERS = "p115_max_workers"
    constants_mod.CONFIG_OPTION_115_API_PRIORITY = "p115_api_priority"
    constants_mod.CONFIG_OPTION_115_ENABLE_ORGANIZE = "p115_enable_organize"
    constants_mod.CONFIG_OPTION_115_MP_CLASSIFY = "p115_mp_classify"
    constants_mod.CONFIG_OPTION_115_MIN_VIDEO_SIZE = "p115_min_video_size"
    constants_mod.CONFIG_OPTION_115_EXTENSIONS = "p115_extensions"
    constants_mod.CONFIG_OPTION_115_MEDIA_ROOT_CID = "p115_media_root_cid"
    constants_mod.CONFIG_OPTION_LOCAL_STRM_ROOT = "local_strm_root"
    constants_mod.CONFIG_OPTION_ETK_SERVER_URL = "etk_server_url"
    constants_mod.CONFIG_OPTION_115_ENABLE_SYNC_DELETE = "p115_enable_sync_delete"
    constants_mod.CONFIG_OPTION_115_GENERATE_MEDIAINFO = "p115_generate_mediainfo"
    constants_mod.CONFIG_OPTION_115_MEDIAINFO_ASSISTED_RECOGNITION = "p115_mediainfo_assisted_recognition"
    constants_mod.CONFIG_OPTION_115_DOWNLOAD_SUBS = "p115_download_subs"
    constants_mod.CONFIG_OPTION_115_LOCAL_CLEANUP = "p115_local_cleanup"
    constants_mod.CONFIG_OPTION_115_MEDIAINFO_CENTER = "p115_mediainfo_center"
    constants_mod.CONFIG_OPTION_115_APP_ID = "p115_app_id"
    constants_mod.CONFIG_OPTION_115_LIFE_MONITOR_ENABLED = "p115_life_monitor_enabled"
    constants_mod.CONFIG_OPTION_115_LIFE_MONITOR_INTERVAL = "p115_life_monitor_interval"
    constants_mod.CONFIG_OPTION_MIN_SCORE_FOR_REVIEW = "min_score_for_review"
    constants_mod.DEFAULT_MIN_SCORE_FOR_REVIEW = 6.0
    constants_mod.CONFIG_OPTION_MAX_ACTORS_TO_PROCESS = "max_actors_to_process"
    constants_mod.DEFAULT_MAX_ACTORS_TO_PROCESS = 50
    constants_mod.CONFIG_OPTION_MAX_EPISODE_ACTORS_TO_PROCESS = "max_episode_actors_to_process"
    constants_mod.DEFAULT_MAX_EPISODE_ACTORS_TO_PROCESS = 0
    constants_mod.CONFIG_OPTION_EXTRACT_THUMB = "extract_thumb"
    constants_mod.CONFIG_OPTION_REMOVE_ACTORS_WITHOUT_AVATARS = "remove_actors_without_avatars"
    constants_mod.CONFIG_OPTION_KEYWORD_TO_TAGS = "keyword_to_tags"
    constants_mod.CONFIG_OPTION_STUDIO_TO_CHINESE = "studio_to_chinese"
    constants_mod.CONFIG_OPTION_GENERATE_COLLECTION_NFO = "generate_collection_nfo"
    constants_mod.CONFIG_OPTION_NETWORK_PROXY_ENABLED = "network_proxy_enabled"
    constants_mod.CONFIG_OPTION_NETWORK_HTTP_PROXY = "network_http_proxy_url"
    constants_mod.CONFIG_OPTION_AI_PROVIDER = "ai_provider"
    constants_mod.CONFIG_OPTION_AI_API_KEY = "ai_api_key"
    constants_mod.CONFIG_OPTION_AI_MODEL_NAME = "ai_model_name"
    constants_mod.CONFIG_OPTION_AI_BASE_URL = "ai_base_url"
    constants_mod.CONFIG_OPTION_AI_VECTOR = "ai_vector"
    constants_mod.CONFIG_OPTION_AI_TRANSLATION_MODE = "ai_translation_mode"
    constants_mod.CONFIG_OPTION_AI_TRANSLATE_ACTOR_ROLE = "ai_translate_actor_role"
    constants_mod.CONFIG_OPTION_AI_TRANSLATE_TITLE = "ai_translate_title"
    constants_mod.CONFIG_OPTION_AI_TRANSLATE_OVERVIEW = "ai_translate_overview"
    constants_mod.CONFIG_OPTION_AI_TRANSLATE_EPISODE_OVERVIEW = "ai_translate_episode_overview"
    constants_mod.CONFIG_OPTION_AI_RECOGNITION = "ai_recognition"
    constants_mod.CONFIG_OPTION_AI_JOKE_FALLBACK = "ai_joke_fallback"
    constants_mod.CONFIG_OPTION_TASK_CHAIN_ENABLED = "task_chain_enabled"
    constants_mod.CONFIG_OPTION_TASK_CHAIN_CRON = "task_chain_cron"
    constants_mod.CONFIG_OPTION_TASK_CHAIN_SEQUENCE = "task_chain_sequence"
    constants_mod.CONFIG_OPTION_TASK_CHAIN_MAX_RUNTIME_MINUTES = "task_chain_max_runtime_minutes"
    constants_mod.CONFIG_OPTION_TASK_CHAIN_LOW_FREQ_ENABLED = "task_chain_low_freq_enabled"
    constants_mod.CONFIG_OPTION_TASK_CHAIN_LOW_FREQ_CRON = "task_chain_low_freq_cron"
    constants_mod.CONFIG_OPTION_TASK_CHAIN_LOW_FREQ_SEQUENCE = "task_chain_low_freq_sequence"
    constants_mod.CONFIG_OPTION_TASK_CHAIN_LOW_FREQ_MAX_RUNTIME_MINUTES = "task_chain_low_freq_max_runtime_minutes"
    constants_mod.CONFIG_OPTION_ACTOR_ROLE_ADD_PREFIX = "actor_role_add_prefix"
    constants_mod.CONFIG_OPTION_ACTOR_MAIN_ROLE_ONLY = "actor_main_role_only"
    constants_mod.CONFIG_OPTION_LOG_ROTATION_SIZE_MB = "log_rotation_size_mb"
    constants_mod.CONFIG_OPTION_LOG_ROTATION_BACKUPS = "log_rotation_backup_count"
    constants_mod.DEFAULT_LOG_ROTATION_SIZE_MB = 5
    constants_mod.DEFAULT_LOG_ROTATION_BACKUPS = 10
    constants_mod.ENV_VAR_DB_HOST = "DB_HOST"
    constants_mod.ENV_VAR_DB_PORT = "DB_PORT"
    constants_mod.ENV_VAR_DB_USER = "DB_USER"
    constants_mod.ENV_VAR_DB_PASSWORD = "DB_PASSWORD"
    constants_mod.ENV_VAR_DB_NAME = "DB_NAME"
    constants_mod.ENV_VAR_TMDB_API_BASE_URL = "TMDB_API_BASE_URL"
    sys.modules["constants"] = constants_mod

    settings_db_mod = types.ModuleType("database.settings_db")
    settings_db_mod.get_setting = lambda key: {}
    settings_db_mod.save_setting = lambda key, value: None
    settings_db_mod.delete_setting = lambda key: True

    user_db_mod = types.ModuleType("database.user_db")
    user_db_mod.get_admin_telegram_chat_ids = lambda: ["10001"]

    database_pkg = types.ModuleType("database")
    database_pkg.settings_db = settings_db_mod
    database_pkg.user_db = user_db_mod
    sys.modules["database"] = database_pkg
    sys.modules["database.settings_db"] = settings_db_mod
    sys.modules["database.user_db"] = user_db_mod

    handler_telegram_mod = types.ModuleType("handler.telegram")
    handler_telegram_mod.send_telegram_message = lambda *args, **kwargs: None
    handler_telegram_mod.escape_markdown = lambda text: text
    sys.modules["handler.telegram"] = handler_telegram_mod

    tasks_system_update = types.ModuleType("tasks.system_update")
    tasks_system_update.consume_post_update_status = lambda: None
    tasks_system_update.peek_post_update_status = lambda: None
    tasks_system_update.clear_post_update_status = lambda: True
    sys.modules["tasks.system_update"] = tasks_system_update


_install_stubs()
sys.modules.pop("config_manager", None)
config_manager = importlib.import_module("config_manager")


class PostUpdateNotifyTests(unittest.TestCase):
    def test_notify_pending_system_update_result_sends_admin_message(self):
        payload = {
            "ok": True,
            "current_version": "10.2.5",
            "target_version": "v10.2.6",
            "message": "容器健康检查通过。",
        }
        user_db_mod = sys.modules["database.user_db"]
        telegram_mod = sys.modules["handler.telegram"]
        with mock.patch("tasks.system_update.peek_post_update_status", return_value=payload):
            with mock.patch("tasks.system_update.clear_post_update_status", return_value=True) as clear_mock:
                with mock.patch.object(user_db_mod, "get_admin_telegram_chat_ids", return_value=["10001"]):
                    with mock.patch.object(telegram_mod, "send_telegram_message") as send_mock:
                        result = config_manager._notify_pending_system_update_result()

        self.assertTrue(result)
        send_mock.assert_called_once()
        clear_mock.assert_called_once()
        message = send_mock.call_args.args[1].replace("\\", "")
        self.assertIn("系统自动更新", message)
        self.assertIn("10.2.5", message)
        self.assertIn("v10.2.6", message)
        self.assertIn("容器健康检查通过。", message)

    def test_notify_pending_system_update_result_keeps_file_when_send_fails(self):
        payload = {
            "ok": True,
            "current_version": "10.2.5",
            "target_version": "v10.2.6",
            "message": "容器健康检查通过。",
        }
        user_db_mod = sys.modules["database.user_db"]
        telegram_mod = sys.modules["handler.telegram"]
        with mock.patch("tasks.system_update.peek_post_update_status", return_value=payload):
            with mock.patch("tasks.system_update.clear_post_update_status", return_value=True) as clear_mock:
                with mock.patch.object(user_db_mod, "get_admin_telegram_chat_ids", return_value=["10001"]):
                    with mock.patch.object(telegram_mod, "send_telegram_message", side_effect=RuntimeError("tg down")):
                        result = config_manager._notify_pending_system_update_result()

        self.assertFalse(result)
        clear_mock.assert_not_called()

    def test_retry_pending_system_update_result_retries_until_success(self):
        with mock.patch.object(config_manager, "_notify_pending_system_update_result", side_effect=[False, False, True]) as notify_mock:
            with mock.patch("time.sleep") as sleep_mock:
                result = config_manager.retry_pending_system_update_result(max_attempts=5, interval_seconds=1.5)

        self.assertTrue(result)
        self.assertEqual(notify_mock.call_count, 3)
        self.assertEqual(sleep_mock.call_count, 2)

    def test_retry_pending_system_update_result_returns_false_after_exhausted_attempts(self):
        with mock.patch.object(config_manager, "_notify_pending_system_update_result", return_value=False) as notify_mock:
            with mock.patch("time.sleep") as sleep_mock:
                result = config_manager.retry_pending_system_update_result(max_attempts=3, interval_seconds=1.5)

        self.assertFalse(result)
        self.assertEqual(notify_mock.call_count, 3)
        self.assertEqual(sleep_mock.call_count, 2)


if __name__ == "__main__":
    unittest.main()
