import importlib
import json
import sys
import types
import unittest
from pathlib import Path
from unittest import mock


TESTS_DIR = Path(__file__).resolve().parent
ROOT = TESTS_DIR.parent
FIXTURE_PATH = TESTS_DIR / "fixtures" / "p115_media_recognition_cases.json"


def _install_test_stubs():
    gevent_mod = types.ModuleType("gevent")
    gevent_mod.spawn_later = lambda *args, **kwargs: None
    sys.modules.setdefault("gevent", gevent_mod)

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

    helpers_mod = types.ModuleType("tasks.helpers")
    helpers_mod.check_series_completion = lambda *args, **kwargs: False
    sys.modules.setdefault("tasks.helpers", helpers_mod)

    tasks_pkg = types.ModuleType("tasks")
    tasks_pkg.__path__ = [str(ROOT / "tasks")]
    tasks_pkg.helpers = helpers_mod
    sys.modules.setdefault("tasks", tasks_pkg)

    analyzer_mod = types.ModuleType("handler.p115_media_analyzer")
    class _Mixin:
        pass
    analyzer_mod.P115MediaAnalyzerMixin = _Mixin
    sys.modules.setdefault("handler.p115_media_analyzer", analyzer_mod)


_install_test_stubs()
p115_service = importlib.import_module("handler.p115_service")
task_p115 = importlib.import_module("tasks.p115")


class P115RecognitionRuleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture_cases = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))

    def test_rule_parse_fixture_coverage(self):
        for case in self.fixture_cases:
            with self.subTest(case=case["name"]):
                result = p115_service._build_rule_parse_result(
                    filename=case["raw"],
                    main_dir_name=case.get("main_dir_name"),
                    has_season_subdirs=False,
                    forced_media_type=None,
                    is_folder=False,
                )
                expect = case["expect"]
                for key, value in expect.items():
                    if key == "confidence":
                        self.assertEqual(result.get(key), value)
                    else:
                        self.assertEqual(result.get(key), value)

    def test_identify_media_enhanced_prefers_rule_search_input(self):
        with mock.patch.object(
            p115_service.tmdb,
            "search_media",
            return_value=[{"id": 999, "title": "City of God", "original_title": "Cidade de Deus"}],
        ) as search_mock:
            with mock.patch.dict(
                p115_service.config_manager.APP_CONFIG,
                {"tmdb_api_key": "fake"},
                clear=False,
            ):
                tmdb_id, media_type, title = p115_service._identify_media_enhanced(
                    "[Group] City.of.God.2002.1080p.BluRay.x264.DTS-WiKi.mkv",
                    main_dir_name="City.of.God.2002",
                    use_ai=False,
                )
        self.assertEqual(tmdb_id, "999")
        self.assertEqual(media_type, "movie")
        self.assertEqual(title, "City of God")
        self.assertTrue(search_mock.called)
        kwargs = search_mock.call_args.kwargs
        self.assertEqual(kwargs["query"], "City of God")
        self.assertEqual(kwargs["year"], "2002")

    def test_identify_media_enhanced_explicit_tmdbid_rule_branch(self):
        with mock.patch.object(
            p115_service.tmdb,
            "get_movie_details",
            return_value={"title": "Some Movie"},
        ) as details_mock:
            with mock.patch.dict(
                p115_service.config_manager.APP_CONFIG,
                {"tmdb_api_key": "fake"},
                clear=False,
            ):
                tmdb_id, media_type, title = p115_service._identify_media_enhanced(
                    "Some.Movie.tmdbid=123456.1080p.mkv",
                    main_dir_name="Some Movie",
                    use_ai=False,
                )

        self.assertEqual(tmdb_id, "123456")
        self.assertEqual(media_type, "movie")
        self.assertEqual(title, "Some Movie")
        details_mock.assert_called_once_with("123456", "fake")

    def test_identify_media_enhanced_prefers_raw_ffprobe_identity_over_rule_tmdbid(self):
        raw_ffprobe_json = {
            "_etk": {
                "tmdb_id": "777",
                "media_type": "movie",
            }
        }
        with mock.patch.object(
            p115_service.tmdb,
            "get_movie_details",
            side_effect=[
                {"title": "Cached Movie"},
                {"title": "Rule Movie"},
            ],
        ) as details_mock:
            with mock.patch.dict(
                p115_service.config_manager.APP_CONFIG,
                {"tmdb_api_key": "fake"},
                clear=False,
            ):
                tmdb_id, media_type, title = p115_service._identify_media_enhanced(
                    "Other.Title.tmdbid=123456.1080p.mkv",
                    main_dir_name="Other Title",
                    use_ai=False,
                    raw_ffprobe_json=raw_ffprobe_json,
                )

        self.assertEqual(tmdb_id, "777")
        self.assertEqual(media_type, "movie")
        self.assertEqual(title, "Cached Movie")
        details_mock.assert_called_once_with("777", "fake")

    def test_identify_media_enhanced_keeps_folder_guard_without_id(self):
        result = p115_service._identify_media_enhanced(
            "Some Folder",
            main_dir_name="Some Folder",
            is_folder=True,
            use_ai=False,
        )
        self.assertEqual(result, (None, None, None))

    def test_rename_file_node_consumes_rule_episode_result(self):
        organizer = p115_service.SmartOrganizer.__new__(p115_service.SmartOrganizer)
        organizer.rename_config = {
            "season_dir_format": ["season_name_en"],
            "file_format": ["title_zh", "sep_space", "s_e"],
        }
        organizer.details = {"seasons": [], "last_episode_to_air": {}}
        organizer.forced_season = None
        organizer._fetch_and_parse_mediainfo = lambda *args, **kwargs: None
        organizer._extract_video_info = lambda *args, **kwargs: {}
        organizer._parse_season_episode_by_custom_regex = lambda *args, **kwargs: (None, None, None)
        organizer._build_name_from_format = lambda format_array, **kwargs: (
            f"S{int(kwargs.get('season_num') or 0):02d}E{int(kwargs.get('episode_num') or 0):02d}"
            if "s_e" in format_array else f"Season {int(kwargs.get('season_num') or 0):02d}"
        )

        new_name, season_num, episode_num, s_name, _, _, _ = organizer._rename_file_node(
            {"fn": "庆余年 第2季 第03集 4K.mkv", "rel_path": "庆余年 第2季"},
            new_base_name="庆余年",
            is_tv=True,
            original_title="庆余年",
            silent_log=True,
        )

        self.assertEqual(season_num, 2)
        self.assertEqual(episode_num, 3)
        self.assertEqual(s_name, "Season 02")
        self.assertTrue(new_name.endswith(".mkv"))

    def test_rename_file_node_falls_back_when_rules_miss(self):
        organizer = p115_service.SmartOrganizer.__new__(p115_service.SmartOrganizer)
        organizer.rename_config = {
            "season_dir_format": ["season_name_en"],
            "file_format": ["title_zh", "sep_space", "s_e"],
        }
        organizer.details = {"seasons": [], "last_episode_to_air": {}}
        organizer.forced_season = None
        organizer._fetch_and_parse_mediainfo = lambda *args, **kwargs: None
        organizer._extract_video_info = lambda *args, **kwargs: {}
        organizer._parse_season_episode_by_custom_regex = lambda *args, **kwargs: (None, None, None)
        organizer._build_name_from_format = lambda format_array, **kwargs: (
            f"S{int(kwargs.get('season_num') or 0):02d}E{int(kwargs.get('episode_num') or 0):02d}"
            if "s_e" in format_array else f"Season {int(kwargs.get('season_num') or 0):02d}"
        )

        _, season_num, episode_num, _, _, _, _ = organizer._rename_file_node(
            {"fn": "Show.S01E07.mkv", "rel_path": "Show"},
            new_base_name="Show",
            is_tv=True,
            original_title="Show",
            silent_log=True,
        )

        self.assertEqual(season_num, 1)
        self.assertEqual(episode_num, 7)

    def test_rename_file_node_prefers_file_episode_over_group_hint_episode(self):
        organizer = p115_service.SmartOrganizer.__new__(p115_service.SmartOrganizer)
        organizer.rename_config = {
            "season_dir_format": ["season_name_en"],
            "file_format": ["title_zh", "sep_space", "s_e"],
        }
        organizer.details = {"seasons": [], "last_episode_to_air": {}}
        organizer.forced_season = None
        organizer._fetch_and_parse_mediainfo = lambda *args, **kwargs: {
            "season_number": 1,
            "episode_number": 7,
        }
        organizer._extract_video_info = lambda *args, **kwargs: {}
        organizer._parse_season_episode_by_custom_regex = lambda *args, **kwargs: (None, None, None)
        organizer._build_name_from_format = lambda format_array, **kwargs: (
            f"S{int(kwargs.get('season_num') or 0):02d}E{int(kwargs.get('episode_num') or 0):02d}"
            if "s_e" in format_array else f"Season {int(kwargs.get('season_num') or 0):02d}"
        )

        _, season_num, episode_num, _, _, _, _ = organizer._rename_file_node(
            {"fn": "Show.S01E07.mkv", "rel_path": "Show", "sha1": "abc123"},
            new_base_name="Show",
            is_tv=True,
            original_title="Show",
            silent_log=True,
            recognition_hints={
                "media_type": "tv",
                "season_number": 1,
                "episode_number": 8,
                "confidence": "high",
            },
        )

        self.assertEqual(season_num, 1)
        self.assertEqual(episode_num, 7)

    def test_rename_file_node_uses_group_hint_episode_when_local_evidence_missing(self):
        organizer = p115_service.SmartOrganizer.__new__(p115_service.SmartOrganizer)
        organizer.rename_config = {
            "season_dir_format": ["season_name_en"],
            "file_format": ["title_zh", "sep_space", "s_e"],
        }
        organizer.details = {"seasons": [], "last_episode_to_air": {}}
        organizer.forced_season = None
        organizer._fetch_and_parse_mediainfo = lambda *args, **kwargs: None
        organizer._extract_video_info = lambda *args, **kwargs: {}
        organizer._parse_season_episode_by_custom_regex = lambda *args, **kwargs: (None, None, None)
        organizer._build_name_from_format = lambda format_array, **kwargs: (
            f"S{int(kwargs.get('season_num') or 0):02d}E{int(kwargs.get('episode_num') or 0):02d}"
            if "s_e" in format_array else f"Season {int(kwargs.get('season_num') or 0):02d}"
        )

        _, season_num, episode_num, _, _, _, _ = organizer._rename_file_node(
            {"fn": "暗影蜘蛛侠.2026.WEB-DL.mkv", "rel_path": "暗影蜘蛛侠"},
            new_base_name="暗影蜘蛛侠",
            is_tv=True,
            original_title="暗影蜘蛛侠",
            silent_log=True,
            recognition_hints={
                "media_type": "tv",
                "season_number": 1,
                "episode_number": 8,
                "confidence": "high",
            },
        )

        self.assertEqual(season_num, 1)
        self.assertEqual(episode_num, 8)

    def test_rename_file_node_does_not_patch_raw_ffprobe_from_hint_only_episode(self):
        organizer = p115_service.SmartOrganizer.__new__(p115_service.SmartOrganizer)
        organizer.rename_config = {
            "season_dir_format": ["season_name_en"],
            "file_format": ["title_zh", "sep_space", "s_e"],
        }
        organizer.details = {"seasons": [], "last_episode_to_air": {}}
        organizer.forced_season = None
        organizer._fetch_and_parse_mediainfo = lambda *args, **kwargs: None
        organizer._extract_video_info = lambda *args, **kwargs: {}
        organizer._parse_season_episode_by_custom_regex = lambda *args, **kwargs: (None, None, None)
        organizer._build_name_from_format = lambda format_array, **kwargs: (
            f"S{int(kwargs.get('season_num') or 0):02d}E{int(kwargs.get('episode_num') or 0):02d}"
            if "s_e" in format_array else f"Season {int(kwargs.get('season_num') or 0):02d}"
        )

        with mock.patch.object(p115_service.P115CacheManager, "patch_raw_ffprobe_etk_context") as patch_mock:
            _, season_num, episode_num, _, _, _, _ = organizer._rename_file_node(
                {"fn": "暗影蜘蛛侠.2026.WEB-DL.mkv", "rel_path": "暗影蜘蛛侠", "sha1": "abc123"},
                new_base_name="暗影蜘蛛侠",
                is_tv=True,
                original_title="暗影蜘蛛侠",
                silent_log=True,
                recognition_hints={
                    "media_type": "tv",
                    "season_number": 1,
                    "episode_number": 8,
                    "confidence": "high",
                },
            )

        self.assertEqual(season_num, 1)
        self.assertEqual(episode_num, 8)
        patch_mock.assert_not_called()

    def test_rename_file_node_patches_raw_ffprobe_when_episode_comes_from_local_evidence(self):
        organizer = p115_service.SmartOrganizer.__new__(p115_service.SmartOrganizer)
        organizer.rename_config = {
            "season_dir_format": ["season_name_en"],
            "file_format": ["title_zh", "sep_space", "s_e"],
        }
        organizer.details = {"seasons": [], "last_episode_to_air": {}}
        organizer.forced_season = None
        organizer._fetch_and_parse_mediainfo = lambda *args, **kwargs: None
        organizer._extract_video_info = lambda *args, **kwargs: {}
        organizer._parse_season_episode_by_custom_regex = lambda *args, **kwargs: (None, None, None)
        organizer._build_name_from_format = lambda format_array, **kwargs: (
            f"S{int(kwargs.get('season_num') or 0):02d}E{int(kwargs.get('episode_num') or 0):02d}"
            if "s_e" in format_array else f"Season {int(kwargs.get('season_num') or 0):02d}"
        )

        with mock.patch.object(p115_service.P115CacheManager, "patch_raw_ffprobe_etk_context") as patch_mock:
            _, season_num, episode_num, _, _, _, _ = organizer._rename_file_node(
                {"fn": "Show.S01E07.mkv", "rel_path": "Show", "sha1": "abc123"},
                new_base_name="Show",
                is_tv=True,
                original_title="Show",
                silent_log=True,
                recognition_hints={
                    "media_type": "tv",
                    "season_number": 1,
                    "episode_number": 8,
                    "confidence": "high",
                },
            )

        self.assertEqual(season_num, 1)
        self.assertEqual(episode_num, 7)
        patch_mock.assert_called_once_with("abc123", season_number=1, episode_number=7)

    def test_normalize_batch_recognition_hints_drops_group_episode_for_tv(self):
        hints = task_p115._normalize_batch_recognition_hints(
            {
                "tmdb_id": "220102",
                "media_type": "tv",
                "identify_title": "暗影蜘蛛侠",
                "season_number": 1,
                "episode_number": 8,
                "confidence": "high",
            },
            is_tv=True,
        )

        self.assertEqual(hints["tmdb_id"], "220102")
        self.assertEqual(hints["season_number"], 1)
        self.assertNotIn("episode_number", hints)

    def test_normalize_batch_recognition_hints_drops_season_episode_for_movie(self):
        hints = task_p115._normalize_batch_recognition_hints(
            {
                "tmdb_id": "693134",
                "media_type": "movie",
                "identify_title": "Dune Part Two",
                "season_number": 1,
                "episode_number": 8,
                "confidence": "high",
            },
            is_tv=False,
        )

        self.assertEqual(hints["tmdb_id"], "693134")
        self.assertNotIn("season_number", hints)
        self.assertNotIn("episode_number", hints)


if __name__ == "__main__":
    unittest.main()
