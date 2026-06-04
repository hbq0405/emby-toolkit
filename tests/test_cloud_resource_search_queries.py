import importlib
import sys
import types
import unittest


def _install_test_stubs():
    flask_mod = sys.modules.get("flask") or types.ModuleType("flask")

    class _Blueprint:
        def __init__(self, *args, **kwargs):
            pass

        def route(self, *args, **kwargs):
            def _decorator(func):
                return func

            return _decorator

    flask_mod.Blueprint = _Blueprint
    flask_mod.jsonify = lambda *args, **kwargs: {"args": args, "kwargs": kwargs}
    flask_mod.request = types.SimpleNamespace(args={})
    sys.modules["flask"] = flask_mod

    config_manager_mod = sys.modules.get("config_manager") or types.ModuleType("config_manager")
    config_manager_mod.APP_CONFIG = {}
    sys.modules["config_manager"] = config_manager_mod

    constants_mod = sys.modules.get("constants") or types.ModuleType("constants")
    sys.modules["constants"] = constants_mod

    hdhive_mod = sys.modules.get("handler.hdhive_client") or types.ModuleType("handler.hdhive_client")
    hdhive_mod.HDHiveClient = object
    sys.modules["handler.hdhive_client"] = hdhive_mod

    tg_userbot_mod = sys.modules.get("handler.tg_userbot") or types.ModuleType("handler.tg_userbot")
    tg_userbot_mod.TGUserBotManager = object
    tg_userbot_mod.tg_task_queue = object()
    sys.modules["handler.tg_userbot"] = tg_userbot_mod

    tg_candidate_mod = sys.modules.get("handler.tg_media_candidate") or types.ModuleType("handler.tg_media_candidate")
    tg_candidate_mod.build_channel_task_payload = lambda *args, **kwargs: {}
    sys.modules["handler.tg_media_candidate"] = tg_candidate_mod

    tasks_mod = sys.modules.get("tasks.hdhive") or types.ModuleType("tasks.hdhive")
    tasks_mod.task_download_from_hdhive = lambda *args, **kwargs: None
    tasks_mod.filter_hdhive_resources = lambda *args, **kwargs: []
    sys.modules["tasks.hdhive"] = tasks_mod

    database_pkg = sys.modules.get("database") or types.ModuleType("database")
    database_pkg.settings_db = types.SimpleNamespace(get_setting=lambda *args, **kwargs: {})
    sys.modules["database"] = database_pkg

    extensions_mod = sys.modules.get("extensions") or types.ModuleType("extensions")
    extensions_mod.admin_required = lambda func: func
    sys.modules["extensions"] = extensions_mod


_install_test_stubs()
subscription = importlib.import_module("routes.subscription")


class CloudResourceSearchQueryTests(unittest.TestCase):
    def test_extra_queries_do_not_include_season(self):
        self.assertEqual(
            subscription._build_cloud_extra_queries("隐身的名字", year="2026"),
            ["隐身的名字 2026"],
        )

    def test_extra_queries_ignore_season_argument_removal(self):
        self.assertEqual(
            subscription._build_cloud_extra_queries("隐身的名字"),
            [],
        )

    def test_strip_season_suffix_removes_manual_input_suffix(self):
        self.assertEqual(subscription._strip_season_suffix("隐身的名字 第 1 季"), "隐身的名字")
        self.assertEqual(subscription._strip_season_suffix("隐身的名字 S01"), "隐身的名字")


if __name__ == "__main__":
    unittest.main()
