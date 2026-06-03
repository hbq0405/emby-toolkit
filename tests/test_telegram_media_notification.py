import importlib
import sys
import types
import unittest
from unittest import mock


def _install_test_stubs():
    config_manager_mod = sys.modules.get("config_manager") or types.ModuleType("config_manager")
    config_manager_mod.APP_CONFIG = {}
    config_manager_mod.get_proxies_for_requests = lambda: None
    sys.modules["config_manager"] = config_manager_mod

    constants_mod = sys.modules.get("constants") or types.ModuleType("constants")
    constants_mod.CONFIG_OPTION_TELEGRAM_BOT_TOKEN = "telegram_bot_token"
    constants_mod.CONFIG_OPTION_TELEGRAM_CHANNEL_ID = "telegram_channel_id"
    constants_mod.CONFIG_OPTION_TELEGRAM_NOTIFY_TYPES = "telegram_notify_types"
    constants_mod.DEFAULT_TELEGRAM_NOTIFY_TYPES = []
    constants_mod.CONFIG_OPTION_EMBY_SERVER_URL = "emby_server_url"
    constants_mod.CONFIG_OPTION_EMBY_API_KEY = "emby_api_key"
    constants_mod.CONFIG_OPTION_EMBY_USER_ID = "emby_user_id"
    sys.modules["constants"] = constants_mod

    extensions_mod = sys.modules.get("extensions") or types.ModuleType("extensions")
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
            mod.get_notification_media_info_by_tmdb_id = lambda *args, **kwargs: {}
        setattr(database_pkg, name, mod)
        sys.modules[f"database.{name}"] = mod
    sys.modules["database"] = database_pkg

    database_connection_mod = sys.modules.get("database.connection") or types.ModuleType("database.connection")
    database_connection_mod.get_db_connection = lambda: None
    sys.modules["database.connection"] = database_connection_mod


_install_test_stubs()
telegram = importlib.import_module("handler.telegram")


class _FakeCursor:
    def __init__(self, row):
        self._row = row

    def execute(self, *args, **kwargs):
        return None

    def fetchone(self):
        return self._row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def __init__(self, row):
        self._row = row

    def cursor(self):
        return _FakeCursor(self._row)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class TelegramMediaNotificationTests(unittest.TestCase):
    def setUp(self):
        telegram.APP_CONFIG.clear()
        telegram.APP_CONFIG.update(
            {
                telegram.constants.CONFIG_OPTION_TELEGRAM_NOTIFY_TYPES: ["library_new"],
                telegram.constants.CONFIG_OPTION_TELEGRAM_CHANNEL_ID: "test-channel",
            }
        )

    def test_series_library_notification_uses_inventory_fallback(self):
        item_details = {
            "Id": "series-2",
            "Name": "炊事班的故事",
            "ProductionYear": 2002,
            "Overview": "剧情简介",
            "Type": "Series",
            "ProviderIds": {"Tmdb": "456"},
        }

        with mock.patch.object(telegram, "get_db_connection", return_value=_FakeConnection(None)):
            with mock.patch.object(telegram, "_load_series_inventory_episode_refs", return_value=[(1, 1), (1, 2), (1, 3)]):
                with mock.patch.object(telegram.media_db, "get_notification_media_info_by_emby_id", return_value={}):
                    with mock.patch.object(telegram.request_db, "get_subscribers_by_tmdb_id", return_value=[]):
                        with mock.patch.object(telegram.user_db, "get_admin_telegram_chat_ids", return_value=[]):
                            with mock.patch.object(telegram, "send_telegram_photo") as send_photo_mock:
                                with mock.patch.object(telegram, "send_telegram_message") as send_message_mock:
                                    telegram.send_media_notification(item_details, notification_type="new", new_episode_ids=None)

        send_photo_mock.assert_not_called()
        send_message_mock.assert_called_once()
        caption = send_message_mock.call_args.args[1]
        self.assertIn("🎞️ *集数*: `S01E01-E03`", caption)

    def test_review_reason_episode_ref_is_rendered_in_caption(self):
        item_details = {
            "Id": "series-1",
            "Name": "猪猪侠",
            "ProductionYear": 2006,
            "Overview": "剧情简介",
            "Type": "Series",
            "ProviderIds": {"Tmdb": "123"},
        }

        with mock.patch.object(telegram, "get_db_connection", return_value=_FakeConnection({"reason": "缺失媒体信息: [S1E1]"})):
            with mock.patch.object(telegram.media_db, "get_notification_media_info_by_emby_id", return_value={}):
                with mock.patch.object(telegram.request_db, "get_subscribers_by_tmdb_id", return_value=[]):
                    with mock.patch.object(telegram.user_db, "get_admin_telegram_chat_ids", return_value=[]):
                        with mock.patch.object(telegram, "send_telegram_photo") as send_photo_mock:
                            with mock.patch.object(telegram, "send_telegram_message") as send_message_mock:
                                telegram.send_media_notification(item_details, notification_type="new", new_episode_ids=None)

        send_photo_mock.assert_not_called()
        send_message_mock.assert_called_once()
        caption = send_message_mock.call_args.args[1]
        self.assertIn("🎞️ *集数*: `S01E01`", caption)
        self.assertIn("🔍 *原因*: 缺失媒体信息: \\[S1E1\\]", caption)

    def test_transfer_notification_uses_candidate_episode_refs(self):
        telegram.APP_CONFIG[telegram.constants.CONFIG_OPTION_TELEGRAM_CHANNEL_ID] = "test-channel"
        task = {
            "title": "赌金",
            "year": 2026,
            "item_type": "tv",
            "tmdb_id": "789",
            "candidate": {
                "raw_text": "赌金 S01E01-E02 1080p WEB-DL",
            },
        }

        with mock.patch.object(telegram.user_db, "get_admin_telegram_chat_ids", return_value=[]):
            with mock.patch.object(telegram.media_db, "get_notification_media_info_by_tmdb_id", return_value={}):
                with mock.patch.object(telegram, "send_telegram_photo") as send_photo_mock:
                    with mock.patch.object(telegram, "send_telegram_message") as send_message_mock:
                        telegram.send_transfer_success_notification(task)

        send_photo_mock.assert_not_called()
        send_message_mock.assert_called_once()
        caption = send_message_mock.call_args.args[1]
        self.assertIn("🎞️ *集数*: `S01E01-E02`", caption)


if __name__ == "__main__":
    unittest.main()
