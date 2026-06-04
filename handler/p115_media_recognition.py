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
    helpers_mod.normalize_lang_code = lambda value, *args, **kwargs: value
    helpers_mod._get_detected_languages_from_streams = lambda *args, **kwargs: set()
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

    resubscribe_mod = types.ModuleType("handler.resubscribe_service")
    class _WashingService:
        @staticmethod
        def decide_washing_action(*args, **kwargs):
            return "ACCEPT", "stub"
    resubscribe_mod.WashingService = _WashingService
    sys.modules.setdefault("handler.resubscribe_service", resubscribe_mod)


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

    def test_rename_file_node_uses_original_video_basename_for_subtitles_when_keep_original(self):
        organizer = p115_service.SmartOrganizer.__new__(p115_service.SmartOrganizer)
        organizer.rename_config = {}
        organizer.media_type = "tv"
        organizer.forced_season = 1
        organizer.details = {}
        organizer.raw_metadata = {}
        organizer.recognition_hints = {}
        organizer._build_name_from_format = mock.Mock(return_value="Season 01")

        subtitle = {
            "fn": "Predators.(2019).S01E01.WEB-DL.HDR.DV.2160p.HEVC.Atmos.5.1.NF.ass",
            "_forced_base_name": "Predators (2019) - S01E01 - WEB-DL - HDR10 DoVi P8 - 2160p - HEVC 10bit - DDP 5.1 - NF - Sic",
            "_forced_season": 1,
            "_forced_episode": 1,
        }

        new_name, season_num, episode_num, season_dir, _, _, _ = organizer._rename_file_node(
            subtitle,
            "Predators (2019)",
            is_tv=True,
            original_title="Predators",
            silent_log=True,
            recognition_hints={},
        )

        self.assertEqual(
            new_name,
            "Predators (2019) - S01E01 - WEB-DL - HDR10 DoVi P8 - 2160p - HEVC 10bit - DDP 5.1 - NF - Sic.ass",
        )
        self.assertEqual(season_num, 1)
        self.assertEqual(episode_num, 1)
        self.assertEqual(season_dir, "Season 01")

    def test_execute_keeps_video_original_name_but_renames_sidecar_subtitle_to_video_basename(self):
        organizer = p115_service.SmartOrganizer.__new__(p115_service.SmartOrganizer)
        organizer.client = mock.Mock()
        organizer.client.fs_move.return_value = {"state": True}
        organizer.client.fs_rename_batch.return_value = {"state": True, "_rename_failures": {}}
        organizer.client.fs_files.return_value = {"state": True, "data": [], "path": []}
        organizer.tmdb_id = "1"
        organizer.media_type = "tv"
        organizer.original_title = "Predators"
        organizer.ai_translator = None
        organizer.use_ai = False
        organizer.api_key = "fake"
        organizer.forced_season = 1
        organizer.recognition_hints = {}
        organizer.raw_metadata = {"lang_code": "en"}
        organizer.details = {"title": "Predators", "original_title": "Predators", "date": "2019-01-01", "seasons": []}
        organizer.rules = [{"cid": "999", "dir_name": "欧美剧", "category_path": "电视剧/欧美剧"}]
        organizer.rename_config = {
            "keep_original_name": True,
            "season_dir_format": ["season_name_en"],
            "main_dir_format": ["title_zh"],
        }
        organizer._fetch_raw_metadata = mock.Mock(return_value={})
        organizer.get_target_cid = mock.Mock(return_value="999")
        organizer._fetch_and_parse_mediainfo = mock.Mock(return_value=None)
        organizer._extract_video_info = mock.Mock(return_value={})
        organizer._parse_season_episode_by_custom_regex = mock.Mock(return_value=(None, None, None))
        organizer._extract_season_from_path_or_text = mock.Mock(return_value=1)
        organizer._build_name_from_format = mock.Mock(side_effect=lambda format_array, **kwargs: (
            "Season 01" if format_array == ["season_name_en"] else "Predators (2019)"
        ))

        def fake_rename(file_item, *_args, **_kwargs):
            name = file_item["fn"]
            if name.endswith(".mkv"):
                return name, 1, 1, "Season 01", {}, False, None
            forced_base = file_item.get("_forced_base_name")
            return f"{forced_base}.ass", 1, 1, "Season 01", {}, False, None

        organizer._rename_file_node = mock.Mock(side_effect=fake_rename)

        candidates = [
            {
                "fid": "v1",
                "file_id": "v1",
                "fn": "Predators.(2019).S01E01.WEB-DL.HDR10.DoVi.P8.2160p.HEVC.10bit.DDP.5.1.NF-Sic.mkv",
                "fc": "1",
                "pid": "src1",
                "pc": "pcv1",
                "sha1": "sha-v1",
                "fs": str(7 * 1024 * 1024 * 1024),
            },
            {
                "fid": "s1",
                "file_id": "s1",
                "fn": "Predators.(2019).S01E01.WEB-DL.HDR.DV.2160p.HEVC.Atmos.5.1.NF.ass",
                "fc": "1",
                "pid": "src1",
                "pc": "pcs1",
                "fs": "102400",
            },
        ]

        fake_config = {
            p115_service.constants.CONFIG_OPTION_115_MEDIA_ROOT_CID: "1",
            p115_service.constants.CONFIG_OPTION_115_UNRECOGNIZED_CID: "998",
            p115_service.constants.CONFIG_OPTION_115_MIN_VIDEO_SIZE: 10,
            p115_service.constants.CONFIG_OPTION_115_EXTENSIONS: [],
            p115_service.constants.CONFIG_OPTION_LOCAL_STRM_ROOT: "",
            p115_service.constants.CONFIG_OPTION_ETK_SERVER_URL: "http://127.0.0.1:5257",
            p115_service.constants.CONFIG_OPTION_115_DOWNLOAD_SUBS: True,
            p115_service.constants.CONFIG_OPTION_115_GENERATE_MEDIAINFO: False,
        }

        with mock.patch.object(p115_service, "get_config", return_value=fake_config):
            with mock.patch.object(p115_service.P115CacheManager, "get_cid", return_value=None):
                with mock.patch.object(p115_service.P115CacheManager, "save_cid", return_value=None):
                    with mock.patch.object(p115_service.P115CacheManager, "update_local_path", return_value=None):
                        with mock.patch.object(p115_service.P115CacheManager, "save_file_cache", return_value=None):
                            with mock.patch.object(p115_service.P115RecordManager, "add_or_update_record", return_value=None):
                                with mock.patch.object(p115_service, "get_db_connection", side_effect=RuntimeError("db not used")):
                                    ok = organizer.execute(candidates, None, skip_gc=True)

        self.assertTrue(ok)
        organizer.client.fs_rename_batch.assert_called_once()
        rename_pairs = organizer.client.fs_rename_batch.call_args.args[0]
        self.assertIn(
            (
                "s1",
                "Predators.(2019).S01E01.WEB-DL.HDR10.DoVi.P8.2160p.HEVC.10bit.DDP.5.1.NF-Sic.ass",
            ),
            rename_pairs,
        )

    def test_tasks_related_sidecar_name_matches_same_episode_without_full_prefix(self):
        self.assertTrue(
            task_p115._is_related_sidecar_name(
                "Predators.(2019).S01E01.WEB-DL.HDR10.DoVi.P8.2160p.HEVC.10bit.DDP.5.1.NF-Sic.mkv",
                "Predators.(2019).S01E01.WEB-DL.HDR.DV.2160p.HEVC.Atmos.5.1.NF.ass",
            )
        )

    def test_tasks_related_sidecar_name_rejects_different_episode(self):
        self.assertFalse(
            task_p115._is_related_sidecar_name(
                "Predators.(2019).S01E01.WEB-DL.HDR10.DoVi.P8.2160p.HEVC.10bit.DDP.5.1.NF-Sic.mkv",
                "Predators.(2019).S01E02.WEB-DL.HDR.DV.2160p.HEVC.Atmos.5.1.NF.ass",
            )
        )

    def test_handler_related_sidecar_name_matches_same_episode_without_full_prefix(self):
        self.assertTrue(
            p115_service._is_related_sidecar_name(
                "Predators.(2019).S01E01.WEB-DL.HDR10.DoVi.P8.2160p.HEVC.10bit.DDP.5.1.NF-Sic.mkv",
                "Predators.(2019).S01E01.WEB-DL.HDR.DV.2160p.HEVC.Atmos.5.1.NF.ass",
            )
        )

    def test_handler_related_sidecar_name_rejects_different_episode(self):
        self.assertFalse(
            p115_service._is_related_sidecar_name(
                "Predators.(2019).S01E01.WEB-DL.HDR10.DoVi.P8.2160p.HEVC.10bit.DDP.5.1.NF-Sic.mkv",
                "Predators.(2019).S01E02.WEB-DL.HDR.DV.2160p.HEVC.Atmos.5.1.NF.ass",
            )
        )

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

    def test_filewise_big_package_prefers_top_level_explicit_tmdbid_over_file_search(self):
        gathered_files = [
            {
                "fid": "v1",
                "file_id": "v1",
                "fn": "Neymar.The.Perfect.Chaos.S01E01.2022.2160p.NF.WEB-DL.mkv",
                "sha1": "sha-v1",
                "fs": str(7 * 1024 * 1024 * 1024),
                "_etk_rel_dir": "Neymar.The.Perfect.Chaos.S01",
            }
        ]

        def fake_identify(filename, main_dir_name=None, **kwargs):
            self.assertEqual(main_dir_name, "内马尔：完美乱局 (2022) {tmdbid-153519}")
            return "153519", "tv", "Neymar: The Perfect Chaos"

        with mock.patch.object(task_p115, "_identify_media_enhanced", side_effect=fake_identify):
            groups, unresolved = task_p115._build_filewise_big_package_groups(
                gathered_files,
                "内马尔：完美乱局 (2022) {tmdbid-153519}",
                use_ai=False,
            )

        self.assertEqual(unresolved, [])
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0]["identified_tmdb_id"], "153519")
        self.assertEqual(groups[0]["identified_title"], "Neymar: The Perfect Chaos")

    def test_filewise_big_package_keeps_distinct_nested_contexts_separate(self):
        gathered_files = [
            {
                "fid": "v1",
                "file_id": "v1",
                "fn": "Show.A.S01E01.2022.1080p.WEB-DL.mkv",
                "sha1": "sha-a1",
                "fs": str(7 * 1024 * 1024 * 1024),
                "_etk_rel_dir": "Show A (2022) {tmdbid-111111}/Season 01",
            },
            {
                "fid": "v2",
                "file_id": "v2",
                "fn": "Show.B.S01E01.2023.1080p.WEB-DL.mkv",
                "sha1": "sha-b1",
                "fs": str(7 * 1024 * 1024 * 1024),
                "_etk_rel_dir": "Show B (2023) {tmdbid-222222}/Season 01",
            },
        ]

        seen_main_dirs = []

        def fake_identify(filename, main_dir_name=None, **kwargs):
            seen_main_dirs.append(main_dir_name)
            if "Show.A" in filename:
                return "111111", "tv", "Show A"
            return "222222", "tv", "Show B"

        with mock.patch.object(task_p115, "_identify_media_enhanced", side_effect=fake_identify):
            groups, unresolved = task_p115._build_filewise_big_package_groups(
                gathered_files,
                "Mixed Resource Pack",
                use_ai=False,
            )

        self.assertEqual(unresolved, [])
        self.assertEqual(len(groups), 2)
        self.assertIn("Show A (2022) {tmdbid-111111}", seen_main_dirs)
        self.assertIn("Show B (2023) {tmdbid-222222}", seen_main_dirs)
        self.assertEqual({g["identified_tmdb_id"] for g in groups}, {"111111", "222222"})

    def test_filewise_big_package_without_explicit_tmdbid_keeps_context_search(self):
        gathered_files = [
            {
                "fid": "v1",
                "file_id": "v1",
                "fn": "Plain.Show.S01E01.2022.1080p.WEB-DL.mkv",
                "sha1": "sha-v1",
                "fs": str(7 * 1024 * 1024 * 1024),
                "_etk_rel_dir": "Plain Show (2022)/Season 01",
            }
        ]

        def fake_identify(filename, main_dir_name=None, **kwargs):
            self.assertEqual(main_dir_name, "Plain Show (2022)")
            return "333333", "tv", "Plain Show"

        with mock.patch.object(task_p115, "_identify_media_enhanced", side_effect=fake_identify):
            groups, unresolved = task_p115._build_filewise_big_package_groups(
                gathered_files,
                "Generic Pack",
                use_ai=False,
            )

        self.assertEqual(unresolved, [])
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0]["identified_tmdb_id"], "333333")

    def test_identify_media_enhanced_prefers_main_dir_explicit_tmdbid_for_normal_path(self):
        with mock.patch.object(
            p115_service.tmdb,
            "get_tv_details",
            return_value={"name": "Neymar: The Perfect Chaos"},
        ) as details_mock:
            with mock.patch.dict(
                p115_service.config_manager.APP_CONFIG,
                {"tmdb_api_key": "fake"},
                clear=False,
            ):
                tmdb_id, media_type, title = p115_service._identify_media_enhanced(
                    "Neymar.The.Perfect.Chaos.S01E01.2022.2160p.NF.WEB-DL.mkv",
                    main_dir_name="内马尔：完美乱局 (2022) {tmdbid-153519}",
                    use_ai=False,
                )

        self.assertEqual(tmdb_id, "153519")
        self.assertEqual(media_type, "tv")
        self.assertEqual(title, "Neymar: The Perfect Chaos")
        details_mock.assert_called_once_with("153519", "fake")


if __name__ == "__main__":
    unittest.main()
