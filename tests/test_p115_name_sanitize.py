import sys
import types
import unittest


class _DummyCursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, *args, **kwargs):
        return None

    def fetchone(self):
        return None


class _DummyConnection:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return _DummyCursor()


def _install_test_stubs():
    gevent_mod = sys.modules.get("gevent") or types.ModuleType("gevent")
    gevent_mod.spawn_later = lambda *args, **kwargs: None
    sys.modules["gevent"] = gevent_mod

    config_manager_mod = sys.modules.get("config_manager") or types.ModuleType("config_manager")
    config_manager_mod.APP_CONFIG = {"tmdb_api_key": "dummy"}
    sys.modules["config_manager"] = config_manager_mod

    constants_mod = sys.modules.get("constants") or types.ModuleType("constants")
    constants_mod.CONFIG_OPTION_TMDB_API_KEY = "tmdb_api_key"
    sys.modules["constants"] = constants_mod

    settings_db_mod = sys.modules.get("database.settings_db") or types.ModuleType("database.settings_db")
    settings_db_mod.get_setting = lambda key: {}
    settings_db_mod.save_setting = lambda key, value: None
    sys.modules["database.settings_db"] = settings_db_mod

    database_pkg = sys.modules.get("database") or types.ModuleType("database")
    database_pkg.settings_db = settings_db_mod
    sys.modules["database"] = database_pkg

    conn_mod = sys.modules.get("database.connection") or types.ModuleType("database.connection")
    conn_mod.get_db_connection = lambda: _DummyConnection()
    sys.modules["database.connection"] = conn_mod

    tmdb_mod = sys.modules.get("handler.tmdb") or types.ModuleType("handler.tmdb")
    tmdb_mod.get_movie_details = lambda *args, **kwargs: {
        "title": 'Falco - Forever Number One: 40 Jahre "Rock Me Amadeus"',
        "original_title": 'Falco - Forever Number One: 40 Jahre "Rock Me Amadeus"',
        "alternative_titles": {"titles": [], "results": []},
        "release_date": "2026-01-01",
        "genres": [],
        "production_countries": [],
        "production_companies": [],
        "credits": {"cast": []},
        "keywords": {"keywords": []},
    }
    tmdb_mod.get_tv_details = lambda *args, **kwargs: {}
    sys.modules["handler.tmdb"] = tmdb_mod

    helpers_mod = sys.modules.get("tasks.helpers") or types.ModuleType("tasks.helpers")
    helpers_mod.RELEASE_GROUPS = {}
    sys.modules["tasks.helpers"] = helpers_mod

    tasks_pkg = sys.modules.get("tasks") or types.ModuleType("tasks")
    tasks_pkg.helpers = helpers_mod
    sys.modules["tasks"] = tasks_pkg

    tg_candidate_mod = sys.modules.get("handler.tg_media_candidate") or types.ModuleType("handler.tg_media_candidate")
    tg_candidate_mod.candidate_to_recognition_hints = lambda *args, **kwargs: {}
    tg_candidate_mod.is_recognition_hint_eligible = lambda *args, **kwargs: False
    tg_candidate_mod.lookup_candidate_hint_for_name = lambda *args, **kwargs: {}
    sys.modules["handler.tg_media_candidate"] = tg_candidate_mod

    p115client_mod = sys.modules.get("p115client") or types.ModuleType("p115client")
    p115client_mod.P115Client = object
    sys.modules["p115client"] = p115client_mod


_install_test_stubs()

from handler.p115_service import SmartOrganizer


class P115NameSanitizeTests(unittest.TestCase):
    def _build_organizer(self):
        organizer = SmartOrganizer(
            client=object(),
            tmdb_id="1654963",
            media_type="movie",
            original_title='Falco - Forever Number One: 40 Jahre "Rock Me Amadeus"',
        )
        organizer.details = {
            "title": 'Falco - Forever Number One: 40 Jahre "Rock Me Amadeus"',
            "title_en": 'Falco - Forever Number One: 40 Jahre "Rock Me Amadeus"',
            "original_title": 'Falco - Forever Number One: 40 Jahre "Rock Me Amadeus"',
            "date": "2026-01-01",
        }
        return organizer

    def test_title_en_and_title_orig_are_sanitized(self):
        organizer = self._build_organizer()

        title_en = organizer._build_name_from_format(["title_en"])
        title_orig = organizer._build_name_from_format(["title_orig"])

        self.assertEqual(title_en, "Falco - Forever Number One 40 Jahre Rock Me Amadeus")
        self.assertEqual(title_orig, "Falco - Forever Number One 40 Jahre Rock Me Amadeus")
        self.assertNotIn('"', title_en)
        self.assertNotIn(":", title_en)

    def test_main_dir_format_uses_safe_title_for_title_zh(self):
        organizer = self._build_organizer()

        result = organizer._build_name_from_format(
            ["title_zh", "sep_space", "year", "sep_space", "tmdb_bracket"],
            original_title=organizer.details["original_title"],
            safe_title='Falco - Forever Number One 40 Jahre Rock Me Amadeus',
        )

        self.assertEqual(
            result,
            "Falco - Forever Number One 40 Jahre Rock Me Amadeus (2026) {tmdb=1654963}",
        )


if __name__ == "__main__":
    unittest.main()
