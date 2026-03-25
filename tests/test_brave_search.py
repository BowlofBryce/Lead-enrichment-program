from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from app.services.brave_search import (
    BraveSearchAuthError,
    BraveSearchClient,
    BraveSearchConfigurationError,
    BraveSearchRateLimitError,
)


class BraveSearchClientTests(unittest.TestCase):
    @patch("app.services.brave_search.get_brave_settings_config")
    @patch("app.services.brave_search.settings")
    def test_missing_api_key_raises_configuration_error(self, mocked_settings: Mock, mocked_config: Mock) -> None:
        mocked_settings.discovery_provider = "brave"
        mocked_settings.brave_search_api_key = ""
        mocked_settings.brave_search_base_url = "https://api.search.brave.com/res/v1"
        mocked_settings.brave_search_timeout_seconds = 10
        mocked_settings.brave_search_max_results_per_query = 5
        mocked_settings.brave_search_country = "us"
        mocked_settings.brave_search_search_lang = "en"
        mocked_settings.brave_search_freshness = ""
        mocked_settings.brave_search_max_retries = 0
        mocked_config.return_value = mocked_settings

        with self.assertRaises(BraveSearchConfigurationError):
            BraveSearchClient().search_web("med spa in Provo, UT")

    @patch("app.services.brave_search.get_brave_settings_config")
    @patch("app.services.brave_search.time.sleep", return_value=None)
    @patch("app.services.brave_search.requests.get")
    @patch("app.services.brave_search.settings")
    def test_rate_limit_retries_then_fails(
        self, mocked_settings: Mock, mocked_get: Mock, _mocked_sleep: Mock, mocked_config: Mock
    ) -> None:
        mocked_settings.discovery_provider = "brave"
        mocked_settings.brave_search_api_key = "token"
        mocked_settings.brave_search_base_url = "https://api.search.brave.com/res/v1"
        mocked_settings.brave_search_timeout_seconds = 10
        mocked_settings.brave_search_max_results_per_query = 5
        mocked_settings.brave_search_country = "us"
        mocked_settings.brave_search_search_lang = "en"
        mocked_settings.brave_search_freshness = ""
        mocked_settings.brave_search_max_retries = 2
        mocked_config.return_value = mocked_settings

        mocked_get.return_value = Mock(status_code=429)

        with self.assertRaises(BraveSearchRateLimitError):
            BraveSearchClient().search_web("medical spa near Orem Utah")
        self.assertEqual(mocked_get.call_count, 3)

    @patch("app.services.brave_search.get_brave_settings_config")
    @patch("app.services.brave_search.requests.get")
    @patch("app.services.brave_search.settings")
    def test_auth_failures_raise_explicit_error(self, mocked_settings: Mock, mocked_get: Mock, mocked_config: Mock) -> None:
        mocked_settings.discovery_provider = "brave"
        mocked_settings.brave_search_api_key = "token"
        mocked_settings.brave_search_base_url = "https://api.search.brave.com/res/v1"
        mocked_settings.brave_search_timeout_seconds = 10
        mocked_settings.brave_search_max_results_per_query = 5
        mocked_settings.brave_search_country = "us"
        mocked_settings.brave_search_search_lang = "en"
        mocked_settings.brave_search_freshness = ""
        mocked_settings.brave_search_max_retries = 0
        mocked_config.return_value = mocked_settings

        mocked_get.return_value = Mock(status_code=403)

        with self.assertRaises(BraveSearchAuthError):
            BraveSearchClient().search_web("best medspa in Salt Lake City")


if __name__ == "__main__":
    unittest.main()
