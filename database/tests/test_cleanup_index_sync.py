import json
import sys
import types
import unittest
from unittest.mock import patch


config_stub = types.ModuleType("config_manager")
config_stub.APP_CONFIG = {}
sys.modules.setdefault("config_manager", config_stub)

from database import maintenance_db


class FakeCursor:
    def __init__(self, rowcount=1):
        self.rowcount = rowcount
        self.calls = []

    def execute(self, query, params=None):
        self.calls.append((str(query), params))


class CleanupIndexSyncTest(unittest.TestCase):
    def test_sync_deletes_cleanup_index_when_one_version_remains(self):
        cursor = FakeCursor(rowcount=1)

        result = maintenance_db._sync_cleanup_index_for_media_row(
            cursor,
            tmdb_id="tmdb-1",
            item_type="Episode",
            item_name="Episode 1",
            asset_details_json=[
                {"emby_item_id": "keep-1", "path": "/media/keep.strm"},
            ],
        )

        self.assertEqual({"deleted": 1, "updated": 0, "remaining_versions": 1}, result)
        self.assertIn("DELETE FROM cleanup_index", cursor.calls[0][0])
        self.assertEqual(("tmdb-1", "Episode"), cursor.calls[0][1])

    def test_sync_updates_cleanup_index_when_multiple_versions_remain(self):
        cursor = FakeCursor(rowcount=1)
        versions_payload = [{"id": "keep-1"}, {"id": "keep-2"}]

        with patch.object(maintenance_db, "_build_cleanup_index_versions_payload", return_value=versions_payload), \
             patch.object(maintenance_db, "_determine_cleanup_best_version_payload", return_value="keep-2"):
            result = maintenance_db._sync_cleanup_index_for_media_row(
                cursor,
                tmdb_id="tmdb-2",
                item_type="Movie",
                item_name="Movie 2",
                asset_details_json=[
                    {"emby_item_id": "keep-1", "path": "/media/a.strm"},
                    {"emby_item_id": "keep-2", "path": "/media/b.strm"},
                ],
                original_language="en",
                countries_json=["US"],
            )

        self.assertEqual({"deleted": 0, "updated": 1, "remaining_versions": 2}, result)
        query, params = cursor.calls[0]
        self.assertIn("UPDATE cleanup_index", query)
        self.assertEqual(json.loads(params[0]), versions_payload)
        self.assertEqual(json.loads(params[1]), "keep-2")
        self.assertEqual(params[2:], ("tmdb-2", "Movie"))

    def test_asset_versions_are_deduped_by_emby_id(self):
        versions = maintenance_db._dedupe_asset_versions_by_emby_id([
            {"emby_item_id": "dup", "path": "/old.strm"},
            {"emby_item_id": "dup", "path": "/new.strm"},
            {"path": "/missing-id.strm"},
            "bad-row",
        ])

        self.assertEqual([{"emby_item_id": "dup", "path": "/new.strm"}], versions)


if __name__ == "__main__":
    unittest.main()
