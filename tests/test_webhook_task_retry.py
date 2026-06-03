import importlib
import sys
import types
import unittest
from unittest import mock


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
    flask_mod.request = types.SimpleNamespace(get_json=lambda silent=False: {})
    flask_mod.jsonify = lambda *args, **kwargs: {"args": args, "kwargs": kwargs}
    sys.modules["flask"] = flask_mod

    gevent_mod = sys.modules.get("gevent") or types.ModuleType("gevent")
    gevent_mod.spawn_later = lambda *args, **kwargs: None
    gevent_mod.spawn = lambda *args, **kwargs: None
    gevent_mod.sleep = lambda *args, **kwargs: None
    sys.modules["gevent"] = gevent_mod

    gevent_event_mod = sys.modules.get("gevent.event") or types.ModuleType("gevent.event")

    class _Event:
        def set(self):
            pass

        def wait(self, timeout=None):
            return True

    gevent_event_mod.Event = _Event
    sys.modules["gevent.event"] = gevent_event_mod

    gevent_lock_mod = sys.modules.get("gevent.lock") or types.ModuleType("gevent.lock")

    class _Semaphore:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    gevent_lock_mod.Semaphore = _Semaphore
    sys.modules["gevent.lock"] = gevent_lock_mod

    task_manager_mod = sys.modules.get("task_manager") or types.ModuleType("task_manager")
    task_manager_mod.submit_task = lambda *args, **kwargs: True
    sys.modules["task_manager"] = task_manager_mod

    emby_mod = sys.modules.get("handler.emby") or types.ModuleType("handler.emby")
    emby_mod.get_series_id_from_child_id = lambda *args, **kwargs: None
    emby_mod.get_emby_item_details = lambda *args, **kwargs: {}
    emby_mod.add_tags_to_item = lambda *args, **kwargs: None
    emby_mod.get_user_details = lambda *args, **kwargs: None
    sys.modules["handler.emby"] = emby_mod

    telegram_mod = sys.modules.get("handler.telegram") or types.ModuleType("handler.telegram")
    telegram_mod.send_playback_notification = lambda *args, **kwargs: None
    sys.modules["handler.telegram"] = telegram_mod

    config_manager_mod = sys.modules.get("config_manager") or types.ModuleType("config_manager")
    config_manager_mod.APP_CONFIG = {}
    sys.modules["config_manager"] = config_manager_mod

    constants_mod = sys.modules.get("constants") or types.ModuleType("constants")
    constants_mod.CONFIG_OPTION_TELEGRAM_NOTIFY_TYPES = "telegram_notify_types"
    constants_mod.DEFAULT_TELEGRAM_NOTIFY_TYPES = []
    constants_mod.CONFIG_OPTION_115_ENABLE_SYNC_DELETE = "enable_sync_delete"
    constants_mod.CONFIG_OPTION_115_MEDIA_ROOT_NAME = "media_root_name"
    sys.modules["constants"] = constants_mod

    extensions_mod = sys.modules.get("extensions") or types.ModuleType("extensions")
    extensions_mod.SYSTEM_UPDATE_MARKERS = {}
    extensions_mod.SYSTEM_UPDATE_LOCK = mock.MagicMock()
    extensions_mod.RECURSION_SUPPRESSION_WINDOW = 60
    extensions_mod.DELETING_COLLECTIONS = set()
    extensions_mod.UPDATING_IMAGES = set()
    extensions_mod.UPDATING_METADATA = set()
    extensions_mod.media_processor_instance = types.SimpleNamespace(
        processed_items_cache={},
        emby_url="http://emby",
        emby_api_key="key",
        emby_user_id="user",
    )
    extensions_mod.processor_ready_required = lambda func: func
    sys.modules["extensions"] = extensions_mod

    core_processor_mod = sys.modules.get("core_processor") or types.ModuleType("core_processor")
    core_processor_mod.MediaProcessor = object
    sys.modules["core_processor"] = core_processor_mod

    watchlist_mod = sys.modules.get("tasks.watchlist") or types.ModuleType("tasks.watchlist")
    watchlist_mod.task_process_watchlist = lambda *args, **kwargs: None
    sys.modules["tasks.watchlist"] = watchlist_mod

    users_mod = sys.modules.get("tasks.users") or types.ModuleType("tasks.users")
    users_mod.task_auto_sync_template_on_policy_change = lambda *args, **kwargs: None
    sys.modules["tasks.users"] = users_mod

    media_mod = sys.modules.get("tasks.media") or types.ModuleType("tasks.media")
    media_mod.task_sync_all_metadata = lambda *args, **kwargs: None
    sys.modules["tasks.media"] = media_mod

    custom_collection_mod = sys.modules.get("handler.custom_collection") or types.ModuleType("handler.custom_collection")
    custom_collection_mod.RecommendationEngine = object
    sys.modules["handler.custom_collection"] = custom_collection_mod

    tmdb_collections_mod = sys.modules.get("handler.tmdb_collections") or types.ModuleType("handler.tmdb_collections")
    sys.modules["handler.tmdb_collections"] = tmdb_collections_mod

    cover_generator_mod = sys.modules.get("services.cover_generator") or types.ModuleType("services.cover_generator")
    cover_generator_mod.CoverGeneratorService = object
    sys.modules["services.cover_generator"] = cover_generator_mod

    database_pkg = sys.modules.get("database") or types.ModuleType("database")
    for name in [
        "custom_collection_db",
        "tmdb_collection_db",
        "settings_db",
        "user_db",
        "maintenance_db",
        "media_db",
        "queries_db",
        "watchlist_db",
    ]:
        mod = sys.modules.get(f"database.{name}") or types.ModuleType(f"database.{name}")
        if name == "media_db":
            mod.is_emby_id_in_library = lambda *args, **kwargs: False
        if name == "watchlist_db":
            mod.get_watching_tmdb_ids = lambda: []
        if name == "queries_db":
            mod._expand_rating_labels = lambda value: value
        if name == "user_db":
            mod.upsert_emby_users_batch = lambda *args, **kwargs: None
            mod.upsert_user_media_data = lambda *args, **kwargs: None
        setattr(database_pkg, name, mod)
        sys.modules[f"database.{name}"] = mod
    sys.modules["database"] = database_pkg

    database_connection_mod = sys.modules.get("database.connection") or types.ModuleType("database.connection")
    database_connection_mod.get_db_connection = lambda: None
    sys.modules["database.connection"] = database_connection_mod

    database_log_mod = sys.modules.get("database.log_db") or types.ModuleType("database.log_db")
    database_log_mod.LogDBManager = object
    sys.modules["database.log_db"] = database_log_mod

    p115_service_mod = sys.modules.get("handler.p115_service") or types.ModuleType("handler.p115_service")
    p115_service_mod.P115Service = object
    p115_service_mod.SmartOrganizer = object
    p115_service_mod.get_config = lambda: {}
    sys.modules["handler.p115_service"] = p115_service_mod

    p115client_mod = sys.modules.get("p115client") or types.ModuleType("p115client")
    p115client_mod.P115Client = object
    sys.modules["p115client"] = p115client_mod


_install_test_stubs()
webhook = importlib.import_module("routes.webhook")


class WebhookTaskRetryTests(unittest.TestCase):
    def setUp(self):
        webhook.WEBHOOK_PENDING_TASKS.clear()
        webhook.WEBHOOK_PENDING_TASKS_DRAINER = None

    def test_busy_worker_enqueues_pending_task(self):
        with mock.patch.object(webhook.task_manager, "submit_task", return_value=False) as submit_mock:
            with mock.patch.object(webhook, "spawn_later") as spawn_later_mock:
                result = webhook._submit_webhook_media_task(
                    "Webhook入库: Test",
                    task_function=lambda **kwargs: None,
                    item_id="123",
                )
        self.assertFalse(result)
        submit_mock.assert_called_once()
        spawn_later_mock.assert_called_once()
        self.assertEqual(spawn_later_mock.call_args.args[0], webhook.WEBHOOK_REQUEUE_DELAY)
        self.assertIs(spawn_later_mock.call_args.args[1], webhook._drain_pending_webhook_tasks)
        self.assertEqual(len(webhook.WEBHOOK_PENDING_TASKS), 1)
        self.assertEqual(webhook.WEBHOOK_PENDING_TASKS[0]["task_name"], "Webhook入库: Test")

    def test_duplicate_pending_task_is_not_enqueued_twice(self):
        task_fn = lambda **kwargs: None
        with mock.patch.object(webhook.task_manager, "submit_task", return_value=False):
            with mock.patch.object(webhook, "spawn_later") as spawn_later_mock:
                first = webhook._submit_webhook_media_task(
                    "Webhook入库: Test",
                    task_function=task_fn,
                    item_id="123",
                )
                second = webhook._submit_webhook_media_task(
                    "Webhook入库: Test",
                    task_function=task_fn,
                    item_id="123",
                )
        self.assertFalse(first)
        self.assertFalse(second)
        self.assertEqual(len(webhook.WEBHOOK_PENDING_TASKS), 1)
        spawn_later_mock.assert_called_once()

    def test_pending_queue_submission_success_drains_head(self):
        task_fn = lambda **kwargs: None
        webhook.WEBHOOK_PENDING_TASKS.append(
            {
                "task_name": "Webhook入库: Test",
                "task_function": task_fn,
                "processor_type": "media",
                "kwargs": {"item_id": "123"},
            }
        )
        with mock.patch.object(webhook.task_manager, "submit_task", return_value=True) as submit_mock:
            with mock.patch.object(webhook.logger, "info") as logger_mock:
                webhook._drain_pending_webhook_tasks()
        submit_mock.assert_called_once()
        logger_mock.assert_called()
        self.assertEqual(len(webhook.WEBHOOK_PENDING_TASKS), 0)

    def test_pending_queue_failure_keeps_task_and_reschedules(self):
        task_fn = lambda **kwargs: None
        webhook.WEBHOOK_PENDING_TASKS.append(
            {
                "task_name": "Webhook入库: Test",
                "task_function": task_fn,
                "processor_type": "media",
                "kwargs": {"item_id": "123"},
            }
        )
        with mock.patch.object(webhook.task_manager, "submit_task", return_value=False):
            with mock.patch.object(webhook, "spawn_later") as spawn_later_mock:
                webhook._drain_pending_webhook_tasks()
        self.assertEqual(len(webhook.WEBHOOK_PENDING_TASKS), 1)
        spawn_later_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
