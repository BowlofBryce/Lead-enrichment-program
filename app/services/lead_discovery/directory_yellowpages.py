from __future__ import annotations

from urllib.parse import quote_plus

from app.services.lead_discovery.http_client import RateLimitedHttpClient, playwright_fetch_html
from app.services.lead_discovery.parsers import yellowpages_html
from app.services.lead_discovery.types import DiscoveryQuery, RawBusinessRecord
from app.settings import settings


class YellowPagesDirectorySource:
    """Scrape Yellow Pages search results; optional detail fetch for missing fields."""

    name = "yellowpages_directory"

    def __init__(self) -> None:
        self.client = RateLimitedHttpClient(
            min_interval_seconds=settings.discovery_yellowpages_min_interval_seconds,
            timeout_seconds=settings.request_timeout_seconds,
            max_retries=settings.discovery_http_max_retries,
        )
        self.max_pages = settings.discovery_yellowpages_max_pages
        self.fetch_detail = settings.discovery_yellowpages_fetch_detail
        self.detail_max = settings.discovery_yellowpages_detail_max_per_query

    def fetch(self, query: DiscoveryQuery) -> list[RawBusinessRecord]:
        term = (query.keyword_variant or query.category or "").strip()
        if not term:
            return []

        geo = f"{query.city} {query.state}"
        base = "https://www.yellowpages.com"
        records: list[RawBusinessRecord] = []

        for page_idx in range(self.max_pages):
            page_param = f"&page={page_idx + 1}" if page_idx else ""
            url = (
                f"{base}/search?search_terms={quote_plus(term)}"
                f"&geo_location_terms={quote_plus(geo)}{page_param}"
            )
            pw = playwright_fetch_html if settings.discovery_yellowpages_use_playwright_fallback else None
            result = self.client.get_text(url, referer=base, playwright_fallback=pw)
            if result.error:
                break

            listings = yellowpages_html.parse_yellowpages_search_results(result.text, base_url=base)
            if not listings:
                break

            detail_budget = self.detail_max
            for li in listings:
                payload = {
                    "business_name": li.get("business_name", ""),
                    "listing_url": li.get("listing_url", ""),
                    "address_line": li.get("address_line", ""),
                    "phone": li.get("phone", ""),
                    "website": "",
                    "search_page": page_idx + 1,
                    "detail_fetched": False,
                    "fetch_used_playwright_search": result.used_playwright,
                }
                need_detail = self.fetch_detail and detail_budget > 0 and (
                    not payload["phone"] or not payload.get("address_line")
                )
                if need_detail and li.get("listing_url"):
                    detail_budget -= 1
                    dres = self.client.get_text(li["listing_url"], referer=url, playwright_fallback=pw)
                    if not dres.error and dres.text:
                        detail = yellowpages_html.parse_yellowpages_detail(dres.text)
                        payload["phone"] = payload["phone"] or detail.get("phone", "")
                        payload["website"] = detail.get("website", "")
                        payload["detail_fetched"] = True
                        payload["detail_used_playwright"] = dres.used_playwright
                records.append(RawBusinessRecord(source=self.name, payload=payload))

            if len(listings) < 8:
                break

        return records
