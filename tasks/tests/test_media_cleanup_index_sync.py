import unittest
import sys


class MediaCleanupIndexSyncTest(unittest.TestCase):
    def setUp(self):
        sys.modules.pop("config_manager", None)

    def test_media_sync_reuses_maintenance_cleanup_helper(self):
        from tasks import media
        from database import maintenance_db

        self.assertIs(
            media.maintenance_db._delete_cleanup_index_for_media_scope,
            maintenance_db._delete_cleanup_index_for_media_scope,
        )


if __name__ == "__main__":
    unittest.main()
