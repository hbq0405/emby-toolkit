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
    settings_db_mod.get_washing_conflict_mode = lambda default='replace': default
    settings_db_mod.get_washing_priority_config = lambda default_conflict_mode='replace': {'conflict_mode': default_conflict_mode}
    settings_db_mod.save_washing_priority_config = lambda value: value
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
moviepilot = importlib.import_module("handler.moviepilot")
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

    def test_rename_renderer_supports_mp_tmdb_dash_template(self):
        renderer = p115_service.P115RenameRenderer(
            details={"title": "绝命毒师", "date": "2008-01-20"},
            tmdb_id="1396",
            original_title="Breaking Bad",
        )
        name = renderer.build_name(
            "{{title}}{% if year %} ({{year}}){% endif %} {tmdb-{{tmdbid}}}",
            is_tv=True,
            season_num=1,
            episode_num=1,
        )
        self.assertEqual(name, "绝命毒师 (2008) {tmdb-1396}")

    def test_rename_renderer_exposes_original_name_template(self):
        renderer = p115_service.P115RenameRenderer(
            details={"title": "寄生虫", "date": "2019-05-30"},
            tmdb_id="496243",
            original_title="Parasite",
        )
        name = renderer.build_name(
            "{{original_name}}{{fileExt}}",
            original_name="Parasite.2019.REMASTERED.1080p",
            file_ext="mkv",
        )
        self.assertEqual(name, "Parasite.2019.REMASTERED.1080p.mkv")

    def test_rename_renderer_accepts_mp_zfill_shorthand(self):
        renderer = p115_service.P115RenameRenderer(
            details={"title": "绝命毒师", "date": "2008-01-20"},
            tmdb_id="1396",
            original_title="Breaking Bad",
        )
        name = renderer.build_name(
            "Season {{season|string}.zfill(2)}}/{{title}} - {{season_episode}}{{fileExt}}",
            is_tv=True,
            season_num=1,
            episode_num=1,
            file_ext="mkv",
        )
        self.assertEqual(name, "Season 01/绝命毒师 - S01E01.mkv")

    def test_rename_renderer_exposes_chinese_season_episode_tokens(self):
        renderer = p115_service.P115RenameRenderer(
            details={"title": "绝命毒师", "date": "2008-01-20"},
            tmdb_id="1396",
            original_title="Breaking Bad",
        )
        name = renderer.build_name(
            "{{season_name_zh}} {{episode_name_zh}} {{season_episode_zh}}",
            is_tv=True,
            season_num=1,
            episode_num=1,
        )
        self.assertEqual(name, "第 1 季 第 1 集 第 1 季 1 集")

    def test_rename_renderer_exposes_season_directory_tokens(self):
        renderer = p115_service.P115RenameRenderer(
            details={"title": "绝命毒师", "date": "2008-01-20"},
            tmdb_id="1396",
            original_title="Breaking Bad",
        )
        name = renderer.build_name(
            "{{season_name_en}} - {{season_name_en_no0}} - {{season_name_s}} - {{season_name_s_no0}}",
            is_tv=True,
            season_num=1,
            episode_num=1,
        )
        self.assertEqual(name, "Season 01 - Season 1 - S01 - S1")

    def test_rename_renderer_cleans_empty_separators(self):
        renderer = p115_service.P115RenameRenderer(
            details={"title": "寄生虫", "date": "2019-05-30"},
            tmdb_id="496243",
            original_title="Parasite",
        )
        name = renderer.build_name(
            "{{title}}{% if year %} ({{year}}){% endif %} · {{source}} · {{effect}} · {{resolution}} · {{codec | upper}} · {{audio}} · {{audio_count}} · {{stream}} - {{group}}{{fileExt}}",
            file_ext="mkv",
            video_info={
                "source": "WEB-DL",
                "resolution": "1080p",
                "codec": "AVC",
                "audio": "DDP 5.1",
                "stream": "AMZN",
            },
        )
        self.assertEqual(name, "寄生虫 (2019) · WEB-DL · 1080p · AVC · DDP 5.1 · AMZN.mkv")

    def test_moviepilot_export_wraps_optional_rename_variables(self):
        converted, unsupported = moviepilot.convert_etk_rename_template_to_mp(
            "{{title}} {{year_pure}} · {{source}} · {{customization}} · {{resolution}} · {{codec | upper}} · {{audio}} · {{audio_count}} · {{stream}} - {{group}}"
        )

        self.assertEqual(unsupported, [])
        self.assertIn("{{year}}", converted)
        self.assertIn("{% if resourceType %} · {{resourceType}}{% endif %}", converted)
        self.assertIn("{% if customization %} · {{customization}}{% endif %}", converted)
        self.assertIn("{% if videoFormat %} · {{videoFormat}}{% endif %}", converted)
        self.assertIn("{% if videoCodec %} · {{videoCodec | upper}}{% endif %}", converted)
        self.assertIn("{% if audioCodec %} · {{audioCodec}}{% endif %}", converted)
        self.assertEqual(converted.count("audioCodec"), 2)
        self.assertIn("{% if webSource %} · {{webSource}}{% endif %}", converted)
        self.assertIn("{% if releaseGroup %} - {{releaseGroup}}{% endif %}", converted)

    def test_moviepilot_export_maps_internal_clean_title_to_mp_name(self):
        converted, unsupported = moviepilot.convert_etk_rename_template_to_mp(
            "{{clean_title}}{% if identify_title %}.{{identify_title}}{% endif %}{{fileExt}}"
        )

        self.assertEqual(unsupported, [])
        self.assertIn("{{name}}", converted)
        self.assertIn("{% if name %}.{{name}}{% endif %}", converted)

    def test_moviepilot_import_converts_mp_variables_to_etk_aliases(self):
        converted = moviepilot.convert_mp_rename_template_to_etk(
            "{{title}}{% if en_title %}.{{en_title}}{% endif %}.{{season_episode}}"
            "{% if videoFormat %}.{{videoFormat}}{% endif %}"
            "{% if resourceType %}.{{resourceType}}{% endif %}"
            "{% if webSource %}.{{webSource}}{% endif %}"
            "{% if videoCodec %}.{{videoCodec}}{% endif %}"
            "{% if audioCodec %}.{{audioCodec}}{% endif %}"
            "{% if releaseGroup %}-{{releaseGroup}}{% endif %}{{fileExt}}"
        )

        self.assertIn("{% if title_en %}.{{title_en}}{% endif %}", converted)
        self.assertIn("{% if resolution %}.{{resolution}}{% endif %}", converted)
        self.assertIn("{% if source %}.{{source}}{% endif %}", converted)
        self.assertIn("{% if stream %}.{{stream}}{% endif %}", converted)
        self.assertIn("{% if codec %}.{{codec}}{% endif %}", converted)
        self.assertIn("{% if audio %}.{{audio}}{% endif %}", converted)
        self.assertIn("{% if group %}-{{group}}{% endif %}", converted)

    def test_rename_renderer_accepts_moviepilot_english_title_alias(self):
        renderer = p115_service.P115RenameRenderer(
            details={"title": "爱情有烟火", "date": "2026-01-01"},
            tmdb_id="12345",
            original_title="Love Has Fireworks",
        )
        name = renderer.build_name(
            "{{title}}{% if en_title %}.{{en_title}}{% endif %}.{{season_episode}}{{fileExt}}",
            is_tv=True,
            season_num=1,
            episode_num=13,
            file_ext="mkv",
        )
        self.assertEqual(name, "爱情有烟火.Love Has Fireworks.S01E13.mkv")

    def test_rename_renderer_exposes_moviepilot_documented_variables(self):
        renderer = p115_service.P115RenameRenderer(
            details={
                "title": "爱情有烟火",
                "date": "2026-06-20",
                "vote_average": 8.2,
                "poster_path": "/poster.jpg",
                "backdrop_path": "/backdrop.jpg",
                "overview": "简介",
                "actors": [{"name": "演员一"}, {"name": "演员二"}],
                "imdb_id": "tt123",
                "douban_id": "db123",
            },
            tmdb_id="12345",
            original_title="Love Has Fireworks",
        )
        ctx = renderer.build_template_context(
            is_tv=True,
            season_num=1,
            episode_num=13,
            original_name="Love.Has.Fireworks.S01E13.Part.2.mkv",
            video_info={
                "source": "WEB-DL",
                "effect": "HDR10",
                "resolution": "2160p",
                "codec": "HEVC",
                "videoBit": "10bit",
                "audio": "AAC 2.0",
                "fps": "25fps",
                "stream": "HHWEB",
                "group": "ADWeb",
                "category": "国产剧",
                "episode_title": "第十三集",
                "episode_date": "2026-06-20",
            },
            file_ext="mkv",
        )

        expected_keys = {
            "title", "en_title", "original_title", "name", "en_name", "original_name",
            "clean_title", "identify_title",
            "year", "title_year", "type", "category", "vote_average", "poster",
            "backdrop", "actors", "overview", "resourceType", "effect", "edition",
            "videoFormat", "resource_term", "releaseGroup", "videoCodec", "videoBit",
            "audioCodec", "fps", "webSource", "tmdbid", "imdbid", "doubanid", "part",
            "fileExt", "customization", "season", "season_fmt", "season_year", "episode",
            "season_episode", "episode_title", "episode_date",
        }
        self.assertTrue(expected_keys.issubset(ctx.keys()))
        self.assertEqual(ctx["en_title"], "Love Has Fireworks")
        self.assertEqual(ctx["videoFormat"], "2160p")
        self.assertEqual(ctx["resourceType"], "WEB-DL")
        self.assertEqual(ctx["videoBit"], "10bit")
        self.assertEqual(ctx["webSource"], "HHWEB")
        self.assertEqual(ctx["releaseGroup"], "ADWeb")
        self.assertEqual(ctx["season_fmt"], "S01")
        self.assertEqual(ctx["episode_title"], "第十三集")
        self.assertEqual(ctx["part"], "2")

    def test_rename_renderer_applies_display_preferences(self):
        renderer = p115_service.P115RenameRenderer(
            details={"title": "测试片", "date": "2026-01-01"},
            tmdb_id="12345",
            original_title="Test Movie",
            config={"video_codec_style": "h265", "hide_audio_channels": True},
        )
        ctx = renderer.build_template_context(video_info={"codec": "HEVC 10bit", "audio": "DDP 5.1"})
        self.assertEqual(ctx["codec"], "H265 10bit")
        self.assertEqual(ctx["videoCodec"], "H265 10bit")
        self.assertEqual(ctx["audio"], "DDP")
        self.assertEqual(ctx["audioCodec"], "DDP")

        ctx_avc = renderer.build_template_context(video_info={"codec": "AVC", "audio": "AAC 2.0"})
        self.assertEqual(ctx_avc["codec"], "H264")
        self.assertEqual(ctx_avc["videoCodec"], "H264")
        self.assertEqual(ctx_avc["audio"], "AAC")

        name = renderer.build_name(
            "{{videoCodec}} - {{audioCodec}}",
            video_info={"codec": "HEVC 10bit", "audio": "DDP 5.1"},
        )
        self.assertEqual(name, "H265 10bit - DDP")

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

    def test_rename_file_node_uses_media_specific_file_templates(self):
        organizer = p115_service.SmartOrganizer.__new__(p115_service.SmartOrganizer)
        organizer.rename_config = {
            "movie_file_template": "MOVIE {{title}}{% if year %} ({{year}}){% endif %}{% if resolution %} · {{resolution}}{% endif %}{{fileExt}}",
            "tv_file_template": "TV {{title}} - {{season_episode}}{% if resolution %} · {{resolution}}{% endif %}{{fileExt}}",
            "file_template": "LEGACY {{title}} - {{season_episode}} · {{resolution}}{{fileExt}}",
            "season_dir_template": "Season {{season_no}}",
        }
        organizer.tmdb_id = "1"
        organizer.media_type = "movie"
        organizer.original_title = "Parasite"
        organizer.details = {"title": "寄生虫", "original_title": "Parasite", "date": "2019-05-30", "seasons": []}
        organizer.raw_metadata = {}
        organizer.forced_season = None
        organizer._fetch_and_parse_mediainfo = lambda *args, **kwargs: None
        organizer._extract_video_info = lambda *args, **kwargs: {"resolution": "1080p"}
        organizer._parse_season_episode_by_custom_regex = lambda *args, **kwargs: (None, None, None)

        movie_name, *_ = organizer._rename_file_node(
            {"fn": "Parasite.2019.1080p.mkv", "rel_path": "Parasite"},
            new_base_name="寄生虫",
            is_tv=False,
            original_title="Parasite",
            silent_log=True,
        )
        tv_name, tv_season, tv_episode, season_dir, *_ = organizer._rename_file_node(
            {"fn": "Breaking.Bad.S01E01.2160p.mkv", "rel_path": "Breaking Bad"},
            new_base_name="绝命毒师",
            is_tv=True,
            original_title="Breaking Bad",
            silent_log=True,
        )

        self.assertEqual(movie_name, "MOVIE 寄生虫 (2019) · 1080p.mkv")
        self.assertNotIn("LEGACY", movie_name)
        self.assertEqual(tv_name, "TV 绝命毒师 - S01E01 · 1080p.mkv")
        self.assertEqual(tv_season, 1)
        self.assertEqual(tv_episode, 1)
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

    def test_identify_media_enhanced_prefers_explicit_tmdbid_when_raw_ffprobe_identity_is_disabled(self):
        with mock.patch.object(
            p115_service.tmdb,
            "get_movie_details",
            return_value={"title": "Rule Movie"},
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
                    raw_ffprobe_json={"_etk": {"tmdb_id": "777", "media_type": "movie"}},
                )

        self.assertEqual(tmdb_id, "123456")
        self.assertEqual(media_type, "movie")
        self.assertEqual(title, "Rule Movie")
        details_mock.assert_called_once_with("123456", "fake")

    def test_identify_media_enhanced_keeps_folder_guard_without_id(self):
        result = p115_service._identify_media_enhanced(
            "Some Folder",
            main_dir_name="Some Folder",
            is_folder=True,
            use_ai=False,
        )
        self.assertEqual(result, (None, None, None))

    def test_smart_organizer_init_sets_recognition_hints_before_metadata_fetch(self):
        seen = {}

        def fake_fetch(self):
            seen["recognition_hints"] = dict(self.recognition_hints)
            return {}

        with mock.patch.object(p115_service.SmartOrganizer, "_fetch_raw_metadata", autospec=True, side_effect=fake_fetch):
            with mock.patch.object(p115_service.settings_db, "get_setting", return_value={}):
                p115_service.SmartOrganizer(
                    client=mock.Mock(),
                    tmdb_id="36338",
                    media_type="tv",
                    original_title="The Lead",
                    recognition_hints={
                        "title": "The Lead",
                        "identify_title": "The Lead",
                        "tmdb_id": "36338",
                        "media_type": "tv",
                        "source_kind": "tg_rule_library",
                        "authority_role": "expected",
                        "confidence": "high",
                    },
                )

        self.assertEqual(seen["recognition_hints"].get("identify_title"), "The Lead")
        self.assertEqual(seen["recognition_hints"].get("source_kind"), "tg_rule_library")

    def test_fetch_raw_metadata_skips_cached_title_when_authoritative_hint_conflicts(self):
        organizer = p115_service.SmartOrganizer.__new__(p115_service.SmartOrganizer)
        organizer.api_key = "fake"
        organizer.media_type = "tv"
        organizer.tmdb_id = "36338"
        organizer.rating_map = {}
        organizer.rating_priority = []
        organizer.recognition_hints = {
            "title": "The Lead",
            "identify_title": "The Lead",
            "tmdb_id": "36338",
            "media_type": "tv",
            "source_kind": "tg_rule_library",
            "authority_role": "expected",
            "confidence": "high",
        }

        class _Cursor:
            def execute(self, *_args, **_kwargs):
                return None

            def fetchone(self):
                return {"title": "The Lead Sheet", "original_title": "The Lead Sheet"}

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class _Conn:
            def cursor(self):
                return _Cursor()

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        raw_details = {
            "name": "The Lead",
            "original_name": "The Lead",
            "genres": [],
            "production_countries": [],
            "origin_country": [],
            "production_companies": [],
            "networks": [],
            "keywords": {"results": []},
            "credits": {"cast": []},
            "alternative_titles": {"results": []},
            "first_air_date": "2026-01-01",
            "vote_average": 0,
            "episode_run_time": [],
            "seasons": [],
            "last_episode_to_air": {},
        }

        with mock.patch.object(p115_service.tmdb, "get_tv_details", return_value=raw_details):
            with mock.patch.object(p115_service.utils, "get_rating_label", return_value="未知"):
                with mock.patch.object(p115_service, "get_db_connection", return_value=_Conn()):
                    metadata = organizer._fetch_raw_metadata()

        self.assertEqual(metadata.get("title"), "The Lead")
        self.assertEqual(metadata.get("original_title"), "The Lead")

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

    def test_transfer_context_to_recognition_hints_marks_expected_authority(self):
        hints = p115_service._transfer_context_to_recognition_hints(
            {
                "tmdb_id": "153519",
                "media_type": "tv",
                "title": "主角",
                "identify_title": "The Lead",
                "clean_title": "The Lead",
                "season_number": 1,
                "confidence": "high",
                "matched_rules": ["tmdb_id_pattern", "title_rule"],
                "source": "tg_rule_library",
                "authority_role": "expected",
                "keys": ["thelead"],
            }
        )
        self.assertEqual(hints["tmdb_id"], "153519")
        self.assertEqual(hints["source"], "tg_rule_library")
        self.assertEqual(hints["source_kind"], "tg_rule_library")
        self.assertEqual(hints["source_kinds"], ["tg_rule_library"])
        self.assertEqual(hints["authority_role"], "expected")
        self.assertEqual(hints["matched_rules"], ["tmdb_id_pattern", "title_rule"])
        self.assertTrue(p115_service._is_authoritative_recognition_hint(hints))

    def test_plain_tg_candidate_hint_is_not_authority_by_default(self):
        hints = {
            "tmdb_id": "36338",
            "media_type": "tv",
            "identify_title": "The Lead Sheet",
            "confidence": "high",
            "source": "tg_candidate",
            "source_kind": "tg_candidate",
            "authority_role": "advisory",
        }
        self.assertFalse(p115_service._is_authoritative_recognition_hint(hints))

    def test_merge_authority_hints_prefers_expected_context_fields(self):
        merged = task_p115._merge_authority_hints(
            {
                "tmdb_id": "153519",
                "media_type": "tv",
                "title": "主角",
                "identify_title": "The Lead",
                "confidence": "high",
                "source": "tg_rule_library",
                "source_kind": "tg_rule_library",
                "source_kinds": ["tg_rule_library"],
                "authority_role": "expected",
                "matched_rules": ["tmdb_id_pattern"],
            },
            {
                "tmdb_id": "36338",
                "media_type": "tv",
                "identify_title": "The Lead Sheet",
                "confidence": "high",
                "source": "tg_candidate",
                "authority_role": "advisory",
                "matched_rules": ["identify_title"],
            },
            is_tv=True,
        )
        self.assertEqual(merged["tmdb_id"], "153519")
        self.assertEqual(merged["identify_title"], "The Lead")
        self.assertEqual(merged["source"], "tg_rule_library")
        self.assertEqual(merged["source_kind"], "tg_rule_library")
        self.assertEqual(merged["source_kinds"], ["tg_rule_library"])
        self.assertEqual(merged["authority_role"], "expected")
        self.assertEqual(merged["matched_rules"], ["tmdb_id_pattern"])

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

        with mock.patch.object(p115_service.P115CacheManager, "patch_raw_ffprobe_etk_context"):
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
            self.assertEqual(main_dir_name, "Neymar.The.Perfect.Chaos.S01")
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
