import sys
import types
import unittest
from unittest.mock import Mock, patch


config_stub = types.ModuleType("config_manager")
config_stub.APP_CONFIG = {}
sys.modules.setdefault("config_manager", config_stub)

from tasks import cleanup


class CleanupScanEmbyFilterTest(unittest.TestCase):
    def test_collect_unique_emby_ids_from_assets_keeps_order(self):
        items = [
            {
                "asset_details_json": [
                    {"emby_item_id": "1"},
                    {"emby_item_id": "2"},
                    {"emby_item_id": "1"},
                    {"path": "/missing-id.strm"},
                ]
            },
            {
                "asset_details_json": [
                    {"emby_item_id": "3"},
                    "bad-row",
                    {"emby_item_id": "2"},
                ]
            },
        ]

        self.assertEqual(["1", "2", "3"], cleanup._collect_unique_emby_ids_from_assets(items))

    def test_get_existing_emby_ids_returns_ids_from_emby(self):
        processor = Mock(emby_url="http://emby", emby_api_key="key", emby_user_id="user")
        items = [{"asset_details_json": [{"emby_item_id": "1"}, {"emby_item_id": "2"}]}]

        with patch.object(cleanup.emby, "get_emby_items_by_id", return_value=[{"Id": "2"}]) as get_items:
            existing_ids = cleanup._get_existing_emby_ids_for_cleanup_scan(processor, items)

        self.assertEqual({"2"}, existing_ids)
        get_items.assert_called_once_with("http://emby", "key", "user", ["1", "2"], fields="Id")

    def test_get_existing_emby_ids_returns_none_on_emby_error(self):
        processor = Mock(emby_url="http://emby", emby_api_key="key", emby_user_id="user")
        items = [{"asset_details_json": [{"emby_item_id": "1"}]}]

        with patch.object(cleanup.emby, "get_emby_items_by_id", side_effect=RuntimeError("boom")):
            existing_ids = cleanup._get_existing_emby_ids_for_cleanup_scan(processor, items)

        self.assertIsNone(existing_ids)


if __name__ == "__main__":
    unittest.main()
