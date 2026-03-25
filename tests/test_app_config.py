from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models import AppSetting, Base
from app.services.app_config import get_brave_settings_config, set_brave_settings
from app.settings import settings


class BraveSettingsConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        db_path = Path(self.tmpdir.name) / "test.db"
        engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False}, future=True)
        Base.metadata.create_all(bind=engine)
        self.Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
        self._original = {
            "discovery_provider": settings.discovery_provider,
            "brave_search_api_key": settings.brave_search_api_key,
            "brave_search_base_url": settings.brave_search_base_url,
            "brave_search_timeout_seconds": settings.brave_search_timeout_seconds,
            "brave_search_max_results_per_query": settings.brave_search_max_results_per_query,
            "brave_search_country": settings.brave_search_country,
            "brave_search_search_lang": settings.brave_search_search_lang,
            "brave_search_freshness": settings.brave_search_freshness,
            "brave_search_max_retries": settings.brave_search_max_retries,
        }

    def tearDown(self) -> None:
        settings.discovery_provider = self._original["discovery_provider"]
        settings.brave_search_api_key = self._original["brave_search_api_key"]
        settings.brave_search_base_url = self._original["brave_search_base_url"]
        settings.brave_search_timeout_seconds = self._original["brave_search_timeout_seconds"]
        settings.brave_search_max_results_per_query = self._original["brave_search_max_results_per_query"]
        settings.brave_search_country = self._original["brave_search_country"]
        settings.brave_search_search_lang = self._original["brave_search_search_lang"]
        settings.brave_search_freshness = self._original["brave_search_freshness"]
        settings.brave_search_max_retries = self._original["brave_search_max_retries"]
        self.tmpdir.cleanup()

    def test_set_and_get_brave_settings(self) -> None:
        with self.Session() as db:
            set_brave_settings(
                db,
                {
                    "discovery_provider": "brave",
                    "brave_search_api_key": "abc123",
                    "brave_search_base_url": "https://api.search.brave.com/res/v1",
                    "brave_search_timeout_seconds": "30",
                    "brave_search_max_results_per_query": "7",
                    "brave_search_country": "ca",
                    "brave_search_search_lang": "fr",
                    "brave_search_freshness": "pw",
                    "brave_search_max_retries": "3",
                },
            )
            cfg = get_brave_settings_config(db)
            self.assertEqual(cfg.brave_search_api_key, "abc123")
            self.assertEqual(cfg.brave_search_timeout_seconds, 30)
            self.assertEqual(cfg.brave_search_max_results_per_query, 7)
            self.assertEqual(cfg.brave_search_country, "ca")
            self.assertEqual(cfg.brave_search_search_lang, "fr")
            self.assertEqual(cfg.brave_search_freshness, "pw")
            self.assertEqual(cfg.brave_search_max_retries, 3)

    def test_invalid_timeout_raises(self) -> None:
        with self.Session() as db:
            with self.assertRaises(ValueError):
                set_brave_settings(
                    db,
                    {
                        "discovery_provider": "brave",
                        "brave_search_api_key": "abc123",
                        "brave_search_base_url": "https://api.search.brave.com/res/v1",
                        "brave_search_timeout_seconds": "0",
                        "brave_search_max_results_per_query": "7",
                        "brave_search_country": "us",
                        "brave_search_search_lang": "en",
                        "brave_search_freshness": "",
                        "brave_search_max_retries": "2",
                    },
                )

    def test_reads_defaults_when_no_db_values(self) -> None:
        with self.Session() as db:
            cfg = get_brave_settings_config(db)
            self.assertEqual(cfg.discovery_provider, settings.discovery_provider)
            self.assertEqual(cfg.brave_search_base_url, settings.brave_search_base_url)
            self.assertEqual(cfg.brave_search_timeout_seconds, settings.brave_search_timeout_seconds)
            self.assertEqual(db.query(AppSetting).count(), 0)


if __name__ == "__main__":
    unittest.main()
