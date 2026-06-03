import importlib
import sys
import types
import unittest
from unittest import mock


def _install_test_stubs():
    config_manager_mod = sys.modules.get("config_manager") or types.ModuleType("config_manager")
    config_manager_mod.APP_CONFIG = {}
    config_manager_mod.PERSISTENT_DATA_PATH = "/tmp"
    config_manager_mod.get_proxies_for_requests = lambda: None
    sys.modules["config_manager"] = config_manager_mod

    constants_mod = sys.modules.get("constants") or types.ModuleType("constants")
    constants_mod.APP_VERSION = "10.2.3"
    constants_mod.CONFIG_OPTION_TELEGRAM_BOT_TOKEN = "telegram_bot_token"
    constants_mod.CONFIG_OPTION_TELEGRAM_CHANNEL_ID = "telegram_channel_id"
    constants_mod.CONFIG_OPTION_TELEGRAM_NOTIFY_TYPES = "telegram_notify_types"
    constants_mod.DEFAULT_TELEGRAM_NOTIFY_TYPES = []
    constants_mod.CONFIG_OPTION_EMBY_SERVER_URL = "emby_server_url"
    constants_mod.CONFIG_OPTION_EMBY_API_KEY = "emby_api_key"
    constants_mod.CONFIG_OPTION_EMBY_USER_ID = "emby_user_id"
    constants_mod.CONFIG_OPTION_GITHUB_TOKEN = "github_token"
    constants_mod.CONFIG_OPTION_SYSTEM_UPDATE_STRATEGY = "system_update_strategy"
    constants_mod.CONFIG_OPTION_SYSTEM_UPDATE_HELPER_IMAGE = "system_update_helper_image"
    constants_mod.GITHUB_REPO_OWNER = "hbq0405"
    constants_mod.GITHUB_REPO_NAME = "emby-toolkit"
    sys.modules["constants"] = constants_mod

    extensions_mod = sys.modules.get("extensions") or types.ModuleType("extensions")
    extensions_mod.media_processor_instance = types.SimpleNamespace(config={})
    extensions_mod.watchlist_processor_instance = types.SimpleNamespace(config={})
    extensions_mod.actor_subscription_processor_instance = types.SimpleNamespace(config={})
    sys.modules["extensions"] = extensions_mod

    emby_mod = sys.modules.get("handler.emby") or types.ModuleType("handler.emby")
    emby_mod.get_emby_item_details = lambda *args, **kwargs: {}
    sys.modules["handler.emby"] = emby_mod

    tg_candidate_mod = sys.modules.get("handler.tg_media_candidate") or types.ModuleType("handler.tg_media_candidate")
    tg_candidate_mod.build_channel_task_payload = lambda *args, **kwargs: {}
    sys.modules["handler.tg_media_candidate"] = tg_candidate_mod

    p115_service_mod = sys.modules.get("handler.p115_service") or types.ModuleType("handler.p115_service")
    p115_service_mod.P115Service = object
    sys.modules["handler.p115_service"] = p115_service_mod

    github_mod = sys.modules.get("handler.github") or types.ModuleType("handler.github")
    github_mod.get_github_releases = lambda *args, **kwargs: [{"version": "v10.2.4"}]
    sys.modules["handler.github"] = github_mod

    docker_mod = sys.modules.get("docker") or types.ModuleType("docker")
    docker_mod.from_env = lambda: None
    docker_mod.errors = types.SimpleNamespace(NotFound=Exception, ImageNotFound=Exception)
    sys.modules["docker"] = docker_mod

    task_manager_mod = sys.modules.get("task_manager") or types.ModuleType("task_manager")
    task_manager_mod.update_status_from_thread = lambda *args, **kwargs: None
    sys.modules["task_manager"] = task_manager_mod

    tasks_core_mod = sys.modules.get("tasks.core") or types.ModuleType("tasks.core")
    tasks_core_mod.get_task_registry = lambda context="all": {}
    sys.modules["tasks.core"] = tasks_core_mod

    database_pkg = sys.modules.get("database") or types.ModuleType("database")
    for name in ["user_db", "request_db", "media_db"]:
        mod = sys.modules.get(f"database.{name}") or types.ModuleType(f"database.{name}")
        if name == "user_db":
            mod.get_admin_telegram_chat_ids = lambda: []
            mod.get_user_telegram_chat_id = lambda *args, **kwargs: None
        elif name == "request_db":
            mod.get_subscribers_by_tmdb_id = lambda *args, **kwargs: []
        elif name == "media_db":
            mod.get_notification_media_info_by_emby_id = lambda *args, **kwargs: {}
        setattr(database_pkg, name, mod)
        sys.modules[f"database.{name}"] = mod
    sys.modules["database"] = database_pkg

    database_connection_mod = sys.modules.get("database.connection") or types.ModuleType("database.connection")
    database_connection_mod.get_db_connection = lambda: None
    sys.modules["database.connection"] = database_connection_mod


_install_test_stubs()
telegram = importlib.import_module("handler.telegram")
system_update = importlib.import_module("tasks.system_update")


class TelegramSystemUpdateNotificationTests(unittest.TestCase):
    def test_resolve_update_strategy_defaults_to_docker_helper(self):
        resolved = system_update.resolve_update_strategy({})
        self.assertEqual(resolved["strategy"], "docker_helper")
        self.assertEqual(resolved["helper_image"], "hbq0405/emby-toolkit:latest")

    def test_resolve_update_target_falls_back_to_env(self):
        with mock.patch.dict("os.environ", {"CONTAINER_NAME": "etk-prod", "DOCKER_IMAGE_NAME": "hbq0405/emby-toolkit:v10.2.4"}, clear=False):
            resolved = system_update.resolve_update_target({}, docker_client=object())
        self.assertEqual(resolved["container_name"], "etk-prod")
        self.assertEqual(resolved["docker_image_name"], "hbq0405/emby-toolkit:v10.2.4")

    def test_system_update_tg_notification_uses_real_result_and_versions(self):
        def fake_update_task(processor):
            return {
                "ok": False,
                "updated": False,
                "message": "无法连接 Docker 守护进程",
                "current_version": "10.2.3",
                "target_version": "v10.2.4",
            }

        with mock.patch.object(system_update, "get_system_update_version_info", return_value={"current_version": "10.2.3", "target_version": "v10.2.4"}):
            with mock.patch.object(system_update, "resolve_update_target", return_value={"container_name": "etk-prod", "docker_image_name": "hbq0405/emby-toolkit:v10.2.4"}):
                with mock.patch.object(system_update, "resolve_update_strategy", return_value={"strategy": "docker_helper", "helper_image": "hbq0405/emby-toolkit:latest"}):
                    with mock.patch.object(telegram, "send_telegram_message") as send_mock:
                        with mock.patch("handler.telegram.threading.Thread") as thread_mock:
                            thread_mock.return_value.start.side_effect = lambda: thread_mock.call_args.kwargs["target"]()

                            registry = {
                                "system-auto-update": (fake_update_task, "系统自动更新", "media")
                            }
                            with mock.patch("tasks.core.get_task_registry", return_value=registry):
                                telegram._execute_task_from_tg("10001", "system-auto-update")

        self.assertEqual(send_mock.call_count, 2)
        start_message = send_mock.call_args_list[0].args[1]
        finish_message = send_mock.call_args_list[1].args[1]
        normalized_start = start_message.replace("\\", "")
        normalized_finish = finish_message.replace("\\", "")
        self.assertIn("当前版本: `10.2.3`", normalized_start)
        self.assertIn("目标版本: `v10.2.4`", normalized_start)
        self.assertIn("目标容器: `etk-prod`", normalized_start)
        self.assertIn("目标镜像: `hbq0405/emby-toolkit:v10.2.4`", normalized_start)
        self.assertIn("更新策略: `docker_helper`", normalized_start)
        self.assertIn("❌ 任务执行失败", finish_message)
        self.assertIn("当前版本: `10.2.3`", normalized_finish)
        self.assertIn("目标版本: `v10.2.4`", normalized_finish)
        self.assertIn("错误信息: 无法连接 Docker 守护进程", normalized_finish)

    def test_run_docker_helper_overrides_entrypoint(self):
        fake_client = mock.Mock()
        fake_client.containers.run.return_value = b"ok"
        fake_client.images.get.return_value = object()

        with mock.patch.object(system_update, "_get_update_status_path", return_value="/tmp/system_update_result.json"):
            with mock.patch.object(system_update, "_read_json_file", return_value={"ok": True, "message": "done"}):
                with mock.patch("tempfile.NamedTemporaryFile") as tmp_file:
                    tmp_cm = mock.MagicMock()
                    tmp_cm.__enter__.return_value.name = "/tmp/helper.py"
                    tmp_cm.__enter__.return_value.write.return_value = None
                    tmp_cm.__exit__.return_value = False
                    tmp_file.return_value = tmp_cm
                    with mock.patch("os.makedirs"), mock.patch("os.remove", side_effect=FileNotFoundError()):
                        events = list(system_update._run_docker_helper(
                            fake_client,
                            "hbq0405/emby-toolkit:latest",
                            "emby-toolkit",
                            "hbq0405/emby-toolkit:latest",
                            {"current_version": "10.2.8", "target_version": "v10.2.9"},
                        ))

        self.assertTrue(events)
        _, kwargs = fake_client.containers.run.call_args
        self.assertEqual(kwargs["entrypoint"], ["python"])
        self.assertEqual(kwargs["command"], ["/tmp/etk_update_helper.py"])


if __name__ == "__main__":
    unittest.main()
