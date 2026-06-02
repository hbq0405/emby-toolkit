import importlib
import json
import sys
import types
import unittest
from pathlib import Path
from unittest import mock


TESTS_DIR = Path(__file__).resolve().parent
FIXTURE_PATH = TESTS_DIR / "fixtures" / "tg_media_candidate_cases.json"


def _install_test_stubs():
    gevent_mod = types.ModuleType("gevent")
    gevent_mod.spawn_later = lambda *args, **kwargs: None
    gevent_mod.spawn = lambda *args, **kwargs: None
    sys.modules.setdefault("gevent", gevent_mod)

    telethon_mod = types.ModuleType("telethon")
    telethon_mod.TelegramClient = object
    telethon_mod.events = types.SimpleNamespace(NewMessage=object)
    sys.modules.setdefault("telethon", telethon_mod)

    telethon_errors = types.ModuleType("telethon.errors")
    telethon_errors.SessionPasswordNeededError = Exception
    telethon_errors.AuthKeyUnregisteredError = Exception
    sys.modules.setdefault("telethon.errors", telethon_errors)

    settings_db_mod = types.ModuleType("database.settings_db")
    settings_db_mod.get_setting = lambda key: {}
    settings_db_mod.save_setting = lambda key, value: None
    sys.modules.setdefault("database.settings_db", settings_db_mod)

    media_db_mod = types.ModuleType("database.media_db")
    media_db_mod.get_series_local_children_info = lambda *args, **kwargs: {}
    sys.modules.setdefault("database.media_db", media_db_mod)

    database_pkg = types.ModuleType("database")
    database_pkg.settings_db = settings_db_mod
    database_pkg.media_db = media_db_mod
    sys.modules.setdefault("database", database_pkg)

    conn_mod = types.ModuleType("database.connection")
    conn_mod.get_db_connection = lambda: None
    sys.modules.setdefault("database.connection", conn_mod)

    helpers_mod = types.ModuleType("tasks.helpers")
    helpers_mod.check_series_completion = lambda *args, **kwargs: False
    sys.modules.setdefault("tasks.helpers", helpers_mod)

    tasks_pkg = types.ModuleType("tasks")
    tasks_pkg.helpers = helpers_mod
    sys.modules.setdefault("tasks", tasks_pkg)

    analyzer_mod = types.ModuleType("handler.p115_media_analyzer")
    class _Mixin:
        pass
    analyzer_mod.P115MediaAnalyzerMixin = _Mixin
    sys.modules.setdefault("handler.p115_media_analyzer", analyzer_mod)


_install_test_stubs()
tg_candidate = importlib.import_module("handler.tg_media_candidate")
moviepilot = importlib.import_module("handler.moviepilot")
p115_service = importlib.import_module("handler.p115_service")


class TgMediaCandidateFlowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture_cases = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))

    def test_candidate_fixture_coverage(self):
        for case in self.fixture_cases:
            with self.subTest(case=case["name"]):
                candidate = tg_candidate.build_tg_media_candidate(
                    case["raw_text"],
                    urls=["https://115.com/s/test123"],
                    chat_username="demo",
                    chat_id="10001",
                    chat_title="Demo Channel",
                    message_id=88,
                    message_date="2025-01-01 10:00",
                    message_link="https://t.me/demo/88",
                    custom_regex={},
                    query=case.get("query", ""),
                )
                self.assertIsNotNone(candidate)
                for key, value in case["expected"].items():
                    self.assertEqual(candidate.get(key), value)

    def test_payload_keeps_candidate_and_legacy_fields(self):
        candidate = tg_candidate.build_tg_media_candidate(
            "The.Last.of.Us.S01E03.1080p.NF.WEB-DL\nhttps://115.com/s/abc123",
            urls=["https://115.com/s/abc123"],
            chat_username="demo",
            chat_id="10001",
            chat_title="Demo",
            query="The Last of Us",
        )
        payload = tg_candidate.build_channel_task_payload(candidate, is_keyword_matched=True, is_subscribe=False)
        self.assertIn("candidate", payload)
        self.assertEqual(payload["candidate"]["identify_title"], "The Last of Us")
        self.assertEqual(payload["title"], "The Last of Us")
        self.assertEqual(payload["item_type"], "tv")
        self.assertEqual(payload["season_number"], 1)
        self.assertEqual(payload["episode_number"], 3)

    def test_moviepilot_queries_use_candidate_titles_before_fallback(self):
        candidate = {
            "identify_title": "The Last of Us",
            "clean_title": "The Last of Us",
            "year": 2023,
            "media_type": "tv",
            "season_number": 1,
            "confidence": "medium",
        }
        queries = moviepilot.build_recognition_queries_from_hints(candidate, fallback_title="The.Last.of.Us.S01E03.1080p.NF.WEB-DL")
        self.assertEqual(queries[0], "The Last of Us")
        self.assertIn("The Last of Us (2023)", queries)
        self.assertIn("The Last of Us S01", queries)
        self.assertEqual(queries[-1], "The.Last.of.Us.S01E03.1080p.NF.WEB-DL")

    def test_moviepilot_queries_fall_back_for_low_confidence_hints(self):
        queries = moviepilot.build_recognition_queries_from_hints(
            {
                "identify_title": "The Last of Us",
                "clean_title": "The Last of Us",
                "year": 2023,
                "media_type": "tv",
                "confidence": "low",
            },
            fallback_title="The.Last.of.Us.S01E03.1080p.NF.WEB-DL",
        )
        self.assertEqual(queries, ["The.Last.of.Us.S01E03.1080p.NF.WEB-DL"])

    def test_moviepilot_queries_fall_back_for_conflict_hints(self):
        queries = moviepilot.build_recognition_queries_from_hints(
            {
                "identify_title": "The Last of Us",
                "clean_title": "The Last of Us",
                "year": 2023,
                "media_type": "tv",
                "confidence": "high",
                "conflict_reason": "title_mismatch",
            },
            fallback_title="The.Last.of.Us.S01E03.1080p.NF.WEB-DL",
        )
        self.assertEqual(queries, ["The.Last.of.Us.S01E03.1080p.NF.WEB-DL"])

    def test_identify_media_enhanced_prefers_high_confidence_hint_tmdb(self):
        hints = {
            "tmdb_id": "12345",
            "media_type": "tv",
            "identify_title": "The Last of Us",
            "confidence": "high",
            "evidence": ["explicit_tmdb"],
        }
        with mock.patch.object(
            p115_service.tmdb,
            "get_tv_details",
            return_value={"name": "The Last of Us"},
        ) as details_mock:
            with mock.patch.dict(
                p115_service.config_manager.APP_CONFIG,
                {"tmdb_api_key": "fake"},
                clear=False,
            ):
                tmdb_id, media_type, title = p115_service._identify_media_enhanced(
                    "The.Last.of.Us.S01E03.1080p.NF.WEB-DL.mkv",
                    main_dir_name="The Last of Us",
                    use_ai=False,
                    recognition_hints=hints,
                )

        self.assertEqual(tmdb_id, "12345")
        self.assertEqual(media_type, "tv")
        self.assertEqual(title, "The Last of Us")
        details_mock.assert_called_once_with("12345", "fake")

    def test_moviepilot_recognition_uses_candidate_queries(self):
        with mock.patch.object(
            moviepilot,
            "recognize_media",
            side_effect=[None, ("555", "tv", "The Last of Us")],
        ) as recognize_mock:
            res = moviepilot.recognize_media_from_candidate(
                {
                    "identify_title": "The Last of Us",
                    "clean_title": "The Last of Us",
                    "year": 2023,
                    "media_type": "tv",
                    "confidence": "high",
                },
                fallback_title="The.Last.of.Us.S01E03.1080p.NF.WEB-DL",
            )
        self.assertEqual(res, ("555", "tv", "The Last of Us"))
        self.assertEqual(recognize_mock.call_args_list[0].args[0], "The Last of Us")
        self.assertEqual(recognize_mock.call_args_list[1].args[0], "The Last of Us (2023)")

    def test_identify_media_enhanced_moviepilot_falls_back_for_low_confidence_hint(self):
        with mock.patch.object(
            p115_service.settings_db,
            "get_setting",
            return_value={"moviepilot_recognition": True},
        ):
            with mock.patch.object(
                p115_service.tmdb,
                "search_media",
                return_value=[],
            ):
                with mock.patch("handler.moviepilot.recognize_media_from_candidate") as mp_mock:
                    mp_mock.return_value = None
                    p115_service._identify_media_enhanced(
                        "The.Last.of.Us.S01E03.1080p.NF.WEB-DL.mkv",
                        main_dir_name="The.Last.of.Us.S01E03.1080p.NF.WEB-DL",
                        use_ai=False,
                        recognition_hints={
                            "identify_title": "The Last of Us",
                            "clean_title": "The Last of Us",
                            "year": 2023,
                            "media_type": "tv",
                            "confidence": "low",
                        },
                    )

        args, kwargs = mp_mock.call_args
        self.assertEqual(args[0].get("title"), "The Last of Us")
        self.assertEqual(kwargs["fallback_title"], "The.Last.of.Us.S01E03.1080p.NF.WEB-DL.mkv")

    def test_lookup_and_remember_candidate_hint_round_trip(self):
        tg_candidate.remember_candidate_hint(
            {
                "identify_title": "The Last of Us",
                "clean_title": "The Last of Us",
                "media_type": "tv",
                "season_number": 1,
                "episode_number": 3,
                "confidence": "medium",
                "target_link": "https://115.com/s/shareabc?password=pass1",
                "receive_code": "pass1",
            },
            ttl_seconds=600,
        )
        hit = tg_candidate.lookup_candidate_hint(
            "The.Last.of.Us.S01E03.1080p",
            media_type="tv",
            lookup_key="sharepwd:shareabc:pass1",
        )
        self.assertIsNotNone(hit)
        self.assertEqual(hit["identify_title"], "The Last of Us")
        self.assertEqual(hit["season_number"], 1)
        self.assertEqual(hit["episode_number"], 3)

    def test_lookup_candidate_hint_prefers_strong_key(self):
        tg_candidate.remember_candidate_hint(
            {
                "identify_title": "Dune",
                "clean_title": "Dune",
                "media_type": "movie",
                "confidence": "medium",
                "target_link": "https://115.com/s/shareaaa?password=code1",
                "receive_code": "code1",
            },
            ttl_seconds=600,
        )
        tg_candidate.remember_candidate_hint(
            {
                "identify_title": "Dune: Part Two",
                "clean_title": "Dune Part Two",
                "media_type": "movie",
                "confidence": "high",
                "target_link": "https://115.com/s/sharebbb?password=code2",
                "receive_code": "code2",
            },
            ttl_seconds=600,
        )

        hit = tg_candidate.lookup_candidate_hint(
            "Dune.2024.1080p.WEB-DL",
            media_type="movie",
            lookup_key="sharepwd:sharebbb:code2",
        )
        self.assertIsNotNone(hit)
        self.assertEqual(hit["identify_title"], "Dune: Part Two")

    def test_lookup_candidate_hint_does_not_fallback_to_title_when_strong_key_misses(self):
        tg_candidate.remember_candidate_hint(
            {
                "identify_title": "Dune",
                "clean_title": "Dune",
                "media_type": "movie",
                "confidence": "medium",
                "target_link": "https://115.com/s/shareaaa?password=code1",
                "receive_code": "code1",
            },
            ttl_seconds=600,
        )

        hit = tg_candidate.lookup_candidate_hint(
            "Dune.2021.1080p.WEB-DL",
            media_type="movie",
            lookup_key="sharepwd:sharezzz:code9",
        )
        self.assertIsNone(hit)

    def test_lookup_candidate_hint_title_fallback_without_strong_key(self):
        tg_candidate.remember_candidate_hint(
            {
                "identify_title": "The Last of Us",
                "clean_title": "The Last of Us",
                "media_type": "tv",
                "season_number": 1,
                "episode_number": 3,
                "confidence": "medium",
            },
            ttl_seconds=600,
        )
        hit = tg_candidate.lookup_candidate_hint("The.Last.of.Us.S01E03.1080p", media_type="tv")
        self.assertIsNotNone(hit)
        self.assertEqual(hit["identify_title"], "The Last of Us")

    def test_lookup_candidate_hint_for_name_uses_resolved_strong_key(self):
        tg_candidate.remember_candidate_hint(
            {
                "identify_title": "Dune: Part Two",
                "clean_title": "Dune Part Two",
                "media_type": "movie",
                "confidence": "high",
                "target_link": "https://115.com/s/sharebbb?password=code2",
                "receive_code": "code2",
            },
            ttl_seconds=600,
        )
        hit = tg_candidate.lookup_candidate_hint_for_name("Dune.Part.Two.2024.1080p", media_type="movie")
        self.assertIsNotNone(hit)
        self.assertEqual(hit["identify_title"], "Dune: Part Two")

    def test_lookup_candidate_hint_for_name_blocks_ambiguous_strong_keys(self):
        tg_candidate.remember_candidate_hint(
            {
                "identify_title": "Dune",
                "clean_title": "Dune",
                "media_type": "movie",
                "confidence": "medium",
                "target_link": "https://115.com/s/shareaaa?password=code1",
                "receive_code": "code1",
            },
            ttl_seconds=600,
        )
        tg_candidate.remember_candidate_hint(
            {
                "identify_title": "Dune",
                "clean_title": "Dune",
                "media_type": "movie",
                "confidence": "medium",
                "target_link": "https://115.com/s/sharebbb?password=code2",
                "receive_code": "code2",
            },
            ttl_seconds=600,
        )
        hit = tg_candidate.lookup_candidate_hint_for_name("Dune.2021.1080p", media_type="movie")
        self.assertIsNone(hit)

    def test_lookup_candidate_hint_for_name_can_use_tmdb_alias_titles(self):
        with mock.patch.object(
            tg_candidate,
            "_fetch_tmdb_alias_titles",
            return_value=["When Will Ayumu Make His Move", "即使如此依旧步步逼近"],
        ):
            tg_candidate.remember_candidate_hint(
                {
                    "tmdb_id": "116168",
                    "identify_title": "即使如此依旧步步逼近",
                    "clean_title": "即使如此依旧步步逼近",
                    "media_type": "tv",
                    "season_number": 1,
                    "confidence": "high",
                    "target_link": "https://115.com/s/shareayumu?password=pass1",
                    "receive_code": "pass1",
                },
                ttl_seconds=600,
            )

        hit = tg_candidate.lookup_candidate_hint_for_name(
            "When Will Ayumu Make His Move.2022.S01E12.1080p.BluRay.Remux.mkv",
            media_type="tv",
            season_number=1,
        )
        self.assertIsNotNone(hit)
        self.assertEqual(hit["tmdb_id"], "116168")
        self.assertEqual(hit["identify_title"], "即使如此依旧步步逼近")

    def test_remember_candidate_hint_refreshes_strong_keys_after_hdhive_unlock(self):
        with mock.patch.object(
            tg_candidate,
            "_fetch_tmdb_alias_titles",
            return_value=["When Will Ayumu Make His Move"],
        ):
            tg_candidate.remember_candidate_hint(
                {
                    "tmdb_id": "116168",
                    "identify_title": "即使如此依旧步步逼近",
                    "clean_title": "即使如此依旧步步逼近",
                    "media_type": "tv",
                    "confidence": "high",
                    "target_link": "https://hdhive.com/resource/115/bee53dc13fd94156919ac43eed672087",
                },
                ttl_seconds=600,
            )
            tg_candidate.remember_candidate_hint(
                {
                    "tmdb_id": "116168",
                    "identify_title": "即使如此依旧步步逼近",
                    "clean_title": "即使如此依旧步步逼近",
                    "media_type": "tv",
                    "confidence": "high",
                    "target_link": "https://115.com/s/shareayumu?password=pass1",
                    "receive_code": "pass1",
                },
                ttl_seconds=600,
            )

        hit = tg_candidate.lookup_candidate_hint(
            "When Will Ayumu Make His Move.2022.S01E12.1080p.BluRay.Remux.mkv",
            media_type="tv",
            lookup_key="sharepwd:shareayumu:pass1",
        )
        self.assertIsNotNone(hit)
        self.assertEqual(hit["tmdb_id"], "116168")


if __name__ == "__main__":
    unittest.main()
