from __future__ import annotations

from urllib.parse import quote_plus

from app.services.lead_discovery.http_client import RateLimitedHttpClient, playwright_fetch_html
from app.services.lead_discovery.parsers import yelp_html
from app.services.lead_discovery.types import DiscoveryQuery, RawBusinessRecord
from app.settings import settings


class YelpDirectorySource:
    """
    Scrape Yelp search result pages and optionally business detail pages for phone/website.
    Fetching uses requests + retries; parsing is isolated in parsers/yelp_html.py.
    """

    name = "yelp_directory"

    def __init__(self) -> None:
        self.client = RateLimitedHttpClient(
            min_interval_seconds=settings.discovery_yelp_min_interval_seconds,
            timeout_seconds=settings.request_timeout_seconds,
            max_retries=settings.discovery_http_max_retries,
        )
        self.max_pages = settings.discovery_yelp_max_pages
        self.fetch_detail = settings.discovery_yelp_fetch_detail
        self.detail_max = settings.discovery_yelp_detail_max_per_query

    def fetch(self, query: DiscoveryQuery) -> list[RawBusinessRecord]:
        term = (query.keyword_variant or query.category or "").strip()
        if not term:
            return []

        loc = f"{query.city}, {query.state}"
        records: list[RawBusinessRecord] = []
        base = "https://www.yelp.com"

        for page_idx in range(self.max_pages):
            start = page_idx * 10
            url = (
                f"{base}/search?find_desc={quote_plus(term)}"
                f"&find_loc={quote_plus(loc)}&start={start}"
            )
            pw = playwright_fetch_html if settings.discovery_yelp_use_playwright_fallback else None
            result = self.client.get_text(url, referer=base, playwright_fallback=pw)
            if result.error:
                break
            listings = yelp_html.parse_yelp_search_results(result.text, base_url=base)
            if not listings:
                break

            detail_budget = self.detail_max
            for li in listings:
                payload = {
                    "business_name": li.get("business_name", ""),
                    "listing_url": li.get("listing_url", ""),
                    "address_line": li.get("address_line", ""),
                    "category_snippet": li.get("category_snippet", ""),
                    "phone": "",
                    "website": "",
                    "search_page": page_idx + 1,
                    "detail_fetched": False,
                    "fetch_used_playwright_search": result.used_playwright,
                }
                if self.fetch_detail and detail_budget > 0 and li.get("listing_url"):
                    detail_budget -= 1
                    dres = self.client.get_text(li["listing_url"], referer=url, playwright_fallback=pw)
                    if not dres.error and dres.text:
                        detail = yelp_html.parse_yelp_biz_detail(dres.text)
                        payload["phone"] = detail.get("phone", "")
                        payload["website"] = detail.get("website", "")
                        payload["detail_fetched"] = True
                        payload["detail_used_playwright"] = dres.used_playwright
                records.append(RawBusinessRecord(source=self.name, payload=payload))

            if len(listings) < 8:
                break

        return records
