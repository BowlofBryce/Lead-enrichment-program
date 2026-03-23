from __future__ import annotations

import unittest
from unittest.mock import Mock

from app.services.lead_discovery.duckduckgo import DuckDuckGoHTMLSource
from app.services.lead_discovery.types import DiscoveryQuery


class DuckDuckGoDefensiveTests(unittest.TestCase):
    def setUp(self) -> None:
        self.source = DuckDuckGoHTMLSource()
        self.source._wait_between_requests = lambda: None  # type: ignore[method-assign]
        self.source._throttle_backoff = lambda **kwargs: None  # type: ignore[method-assign]

    def test_disables_provider_after_two_consecutive_403(self) -> None:
        blocked = Mock(status_code=403, text="")
        self.source.session.get = Mock(side_effect=[blocked, blocked])  # type: ignore[method-assign]
        query = DiscoveryQuery(query="med spa in Provo, UT", category="MedSpa", keyword_variant="MedSpa", city="Provo", state="UT")

        first = self.source.fetch(query)
        second = self.source.fetch(query)

        self.assertEqual(first, [])
        self.assertEqual(second, [])
        self.assertTrue(self.source.disabled_for_run)

    def test_skips_query_after_403_seen_twice(self) -> None:
        blocked = Mock(status_code=403, text="")
        self.source.session.get = Mock(side_effect=[blocked, blocked])  # type: ignore[method-assign]
        query = DiscoveryQuery(query="medical spa in Orem, UT", category="MedSpa", keyword_variant="MedSpa", city="Orem", state="UT")
        self.source.fetch(query)

        call_count_before = self.source.session.get.call_count
        self.source.fetch(query)
        self.assertEqual(self.source.session.get.call_count, call_count_before)


if __name__ == "__main__":
    unittest.main()
