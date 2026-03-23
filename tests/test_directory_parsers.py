"""Offline tests for HTML parsing (no network)."""

from __future__ import annotations

import unittest

from app.services.lead_discovery.parsers import yelp_html, yellowpages_html


class YelpHtmlParserTests(unittest.TestCase):
    def test_extracts_biz_links(self) -> None:
        html = """
        <html><body>
        <a href="/biz/acme-med-spa-provo">Acme Med Spa</a>
        <a href="/biz/other-spa-provo">Other Spa</a>
        </body></html>
        """
        rows = yelp_html.parse_yelp_search_results(html)
        self.assertGreaterEqual(len(rows), 1)
        urls = [r["listing_url"] for r in rows]
        self.assertTrue(any("/biz/" in u for u in urls))

    def test_json_ld_itemlist(self) -> None:
        html = """
        <script type="application/ld+json">
        {"@type":"ItemList","itemListElement":[{"@type":"ListItem","item":{"@type":"LocalBusiness",
        "name":"Glow Clinic","url":"https://www.yelp.com/biz/glow-clinic-slc"}}]}
        </script>
        """
        rows = yelp_html.parse_yelp_search_results(html)
        self.assertTrue(any("glow" in r.get("business_name", "").lower() or "biz" in r.get("listing_url", "") for r in rows))


class YellowPagesHtmlParserTests(unittest.TestCase):
    def test_extracts_profile_links(self) -> None:
        html = """
        <html><body>
        <a class="business-name" href="/utah/provo/acme-spa-12345.html">Acme Spa</a>
        </body></html>
        """
        rows = yellowpages_html.parse_yellowpages_search_results(html)
        self.assertGreaterEqual(len(rows), 1)
        self.assertIn("acme", rows[0]["business_name"].lower())


if __name__ == "__main__":
    unittest.main()
