import importlib
import sys
import types
import unittest
from unittest import mock


def _install_test_stubs():
    config_manager_mod = types.ModuleType("config_manager")
    config_manager_mod.APP_CONFIG = {}
    sys.modules.setdefault("config_manager", config_manager_mod)

    constants_mod = types.ModuleType("constants")
    constants_mod.CONFIG_OPTION_115_SAVE_PATH_CID = "p115_save_path_cid"
    constants_mod.CONFIG_OPTION_115_SAVE_PATH_NAME = "p115_save_path_name"
    constants_mod.CONFIG_OPTION_115_ENABLE_ORGANIZE = "p115_enable_organize"
    constants_mod.CONFIG_OPTION_AI_RECOGNITION = "ai_recognition"
    constants_mod.CONFIG_OPTION_115_EXTENSIONS = "p115_extensions"
    constants_mod.CONFIG_OPTION_115_UNRECOGNIZED_CID = "p115_unrecognized_cid"
    constants_mod.CONFIG_OPTION_115_UNRECOGNIZED_NAME = "p115_unrecognized_name"
    constants_mod.CONFIG_OPTION_115_MAX_WORKERS = "p115_max_workers"
    sys.modules.setdefault("constants", constants_mod)

    settings_db_mod = types.ModuleType("database.settings_db")
    settings_db_mod.get_setting = lambda key: {}
    settings_db_mod.save_setting = lambda key, value: None
    sys.modules.setdefault("database.settings_db", settings_db_mod)

    database_pkg = types.ModuleType("database")
    database_pkg.settings_db = settings_db_mod
    sys.modules.setdefault("database", database_pkg)

    conn_mod = types.ModuleType("database.connection")
    conn_mod.get_db_connection = lambda: None
    sys.modules.setdefault("database.connection", conn_mod)

    tmdb_mod = types.ModuleType("handler.tmdb")
    sys.modules.setdefault("handler.tmdb", tmdb_mod)

    p115_service_mod = types.ModuleType("handler.p115_service")
    p115_service_mod.P115Service = object
    p115_service_mod.P115CacheManager = object
    p115_service_mod.P115RecordManager = object
    p115_service_mod.P115DeleteBuffer = object
    p115_service_mod.SmartOrganizer = object
    p115_service_mod.get_config = lambda: {}
    p115_service_mod._parse_115_size = lambda value: int(value or 0)
    p115_service_mod._identify_media_enhanced = lambda *args, **kwargs: (None, None, None)
    sys.modules.setdefault("handler.p115_service", p115_service_mod)

    analyzer_mod = types.ModuleType("handler.p115_media_analyzer")
    class _Mixin:
        pass
    analyzer_mod.P115MediaAnalyzerMixin = _Mixin
    sys.modules.setdefault("handler.p115_media_analyzer", analyzer_mod)

    tg_candidate_mod = types.ModuleType("handler.tg_media_candidate")
    tg_candidate_mod.lookup_candidate_hint_for_name = lambda *args, **kwargs: {}
    sys.modules.setdefault("handler.tg_media_candidate", tg_candidate_mod)


_install_test_stubs()
task_p115 = importlib.import_module("tasks.p115")


class P115BigPackageModeTests(unittest.TestCase):
    def test_should_use_filewise_big_package_skips_tv_and_tmdb_cases(self):
        sample_videos = [{"fid": "1"}, {"fid": "2"}]
        self.assertFalse(
            task_p115._should_use_filewise_big_package(
                "Show",
                is_tv_group=True,
                has_season_dir=False,
                has_tmdb=False,
                valid_video_files=sample_videos,
            )
        )
        self.assertFalse(
            task_p115._should_use_filewise_big_package(
                "Movie {tmdb=1}",
                is_tv_group=False,
                has_season_dir=False,
                has_tmdb=True,
                valid_video_files=sample_videos,
            )
        )
        self.assertTrue(
            task_p115._should_use_filewise_big_package(
                "2026新片",
                is_tv_group=False,
                has_season_dir=False,
                has_tmdb=False,
                valid_video_files=sample_videos,
            )
        )

    def test_should_force_nested_package_scan_for_generic_root_even_if_children_are_tv(self):
        self.assertTrue(task_p115._should_force_nested_package_scan("2026"))
        self.assertTrue(task_p115._should_force_nested_package_scan("电视剧"))
        self.assertTrue(task_p115._should_force_nested_package_scan("合集"))
        self.assertFalse(task_p115._should_force_nested_package_scan("罪无可逃 (2026)"))
        self.assertFalse(task_p115._should_force_nested_package_scan("罪无可逃 (2026) {tmdb=123}"))
        self.assertFalse(task_p115._should_force_nested_package_scan("Season 1"))

    def test_choose_big_package_context_name_prefers_last_non_generic_parent(self):
        self.assertEqual(
            task_p115._choose_big_package_context_name("外层合集", "xxx合集/2026/真爱下一位"),
            "真爱下一位",
        )
        self.assertEqual(
            task_p115._choose_big_package_context_name("外层合集", "合集/2026/电影"),
            "外层合集",
        )

    def test_analyze_nested_root_structure_detects_specific_nested_series_context(self):
        gathered_files = [
            {
                "fid": "v1",
                "fn": "罪无可逃.2026.S01E24.mkv",
                "size": str(300 * 1024 * 1024),
                "_etk_rel_dir": "罪无可逃 (2026) {tmdb-321749}/Season 1",
            },
            {
                "fid": "v2",
                "fn": "罪无可逃.2026.S01E23.mkv",
                "size": str(300 * 1024 * 1024),
                "_etk_rel_dir": "罪无可逃 (2026) {tmdb-321749}/Season 1",
            },
        ]

        result = task_p115._analyze_nested_root_structure("2026", gathered_files)
        self.assertTrue(result["has_nested_specific_context"])
        self.assertIn("罪无可逃 (2026) {tmdb-321749}", result["contexts"])
        self.assertTrue(result["should_force_filewise"])

    def test_analyze_nested_root_structure_detects_multiple_media_contexts(self):
        gathered_files = [
            {
                "fid": "v1",
                "fn": "A.Show.S01E01.mkv",
                "size": str(300 * 1024 * 1024),
                "_etk_rel_dir": "A Show (2026)/Season 1",
            },
            {
                "fid": "v2",
                "fn": "B.Show.S01E01.mkv",
                "size": str(300 * 1024 * 1024),
                "_etk_rel_dir": "B Show (2026)/Season 1",
            },
        ]

        result = task_p115._analyze_nested_root_structure("国产剧集", gathered_files)
        self.assertTrue(result["has_multiple_contexts"])
        self.assertTrue(result["should_force_filewise"])

    def test_build_filewise_big_package_groups_keeps_tv_files_together(self):
        gathered_files = [
            {
                "fid": "v1",
                "fn": "Thank.You.Next.2024.S01E01.mkv",
                "size": str(200 * 1024 * 1024),
                "sha1": "sha1-a",
                "_etk_rel_dir": "xxx合集/2026/真爱下一位",
            },
            {
                "fid": "s1",
                "fn": "Thank.You.Next.2024.S01E01.srt",
                "size": "1234",
                "_etk_rel_dir": "xxx合集/2026/真爱下一位",
            },
            {
                "fid": "v2",
                "fn": "Thank.You.Next.2024.S01E02.mkv",
                "size": str(220 * 1024 * 1024),
                "sha1": "sha1-b",
                "_etk_rel_dir": "xxx合集/2026/真爱下一位",
            },
        ]

        identify_results = [
            ("251883", "tv", "真爱下一位"),
            ("251883", "tv", "真爱下一位"),
        ]

        with mock.patch.object(task_p115, "_identify_media_enhanced", side_effect=identify_results):
            with mock.patch.object(task_p115, "lookup_candidate_hint_for_name", return_value={"confidence": "high"}):
                groups, unresolved = task_p115._build_filewise_big_package_groups(
                    gathered_files,
                    top_name="外层合集",
                    ai_translator=None,
                    use_ai=False,
                )

        self.assertEqual(len(groups), 1)
        self.assertEqual(unresolved, [])
        group = groups[0]
        self.assertEqual(group["identified_tmdb_id"], "251883")
        self.assertEqual(group["identified_media_type"], "tv")
        self.assertEqual(group["identified_title"], "真爱下一位")
        self.assertEqual(group["forced_season"], 1)
        self.assertEqual({item["fid"] for item in group["files"]}, {"v1", "s1", "v2"})

    def test_build_filewise_big_package_groups_leaves_unmatched_items_unresolved(self):
        gathered_files = [
            {
                "fid": "v1",
                "fn": "Unknown.Movie.2026.mkv",
                "size": str(200 * 1024 * 1024),
                "sha1": "sha1-a",
                "_etk_rel_dir": "大包/2026/未知电影",
            },
            {
                "fid": "nfo1",
                "fn": "Unknown.Movie.2026.nfo",
                "size": "345",
                "_etk_rel_dir": "大包/2026/未知电影",
            },
        ]

        with mock.patch.object(task_p115, "_identify_media_enhanced", return_value=(None, None, None)):
            groups, unresolved = task_p115._build_filewise_big_package_groups(
                gathered_files,
                top_name="外层合集",
                ai_translator=None,
                use_ai=False,
            )

        self.assertEqual(groups, [])
        self.assertEqual({item["fid"] for item in unresolved}, {"v1", "nfo1"})


if __name__ == "__main__":
    unittest.main()
