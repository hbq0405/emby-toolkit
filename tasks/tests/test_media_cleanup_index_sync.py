import unittest
import sys


class FakeCursor:
    def __init__(self, rowcount=1):
        self.rowcount = rowcount
        self.calls = []

    def execute(self, query, params=None):
        self.calls.append((str(query), params))


class MediaCleanupIndexSyncTest(unittest.TestCase):
    def setUp(self):
        sys.modules.pop("config_manager", None)

    def test_delete_cleanup_index_for_episode_scope(self):
        from tasks import media

        cursor = FakeCursor(rowcount=2)

        deleted = media._delete_cleanup_index_for_sync(cursor, "episode-tmdb", "Episode")

        self.assertEqual(2, deleted)
        query, params = cursor.calls[0]
        self.assertIn("DELETE FROM cleanup_index", query)
        self.assertNotIn("USING media_metadata", query)
        self.assertEqual(("episode-tmdb", "Episode"), params)

    def test_delete_cleanup_index_for_series_scope_cascades_children(self):
        from tasks import media

        cursor = FakeCursor(rowcount=3)

        deleted = media._delete_cleanup_index_for_sync(cursor, "series-tmdb", "Series")

        self.assertEqual(3, deleted)
        query, params = cursor.calls[0]
        self.assertIn("DELETE FROM cleanup_index ci", query)
        self.assertIn("USING media_metadata mm", query)
        self.assertIn("parent_series_tmdb_id", query)
        self.assertEqual(("series-tmdb", "series-tmdb"), params)


if __name__ == "__main__":
    unittest.main()
