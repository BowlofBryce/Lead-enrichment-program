from __future__ import annotations

from urllib.parse import parse_qs, unquote, urlparse

import requests
from bs4 import BeautifulSoup

from app.services.lead_discovery.http_client import RateLimitedClient
from app.services.lead_discovery.types import DiscoveryQuery, RawBusinessRecord
from app.settings import settings


class DuckDuckGoHTMLSource:
    """Search DuckDuckGo HTML endpoint and return normalized raw discovery rows."""

    name = "duckduckgo_html"

    def __init__(self) -> None:
        self.client = RateLimitedClient(min_interval_seconds=settings.discovery_duckduckgo_min_interval_seconds)
        self.endpoint = settings.discovery_duckduckgo_html_url
        self.max_results = settings.discovery_duckduckgo_max_results_per_query
        self.user_agent = settings.discovery_duckduckgo_user_agent

    def fetch(self, query: DiscoveryQuery) -> list[RawBusinessRecord]:
        self.client.wait()
        response = requests.get(
            self.endpoint,
            params={"q": query.query},
            headers={"User-Agent": self.user_agent},
            timeout=self.client.timeout,
        )
        response.raise_for_status()

        rows: list[RawBusinessRecord] = []
        for item in _parse_duckduckgo_results(response.text)[: self.max_results]:
            rows.append(
                RawBusinessRecord(
                    source=self.name,
                    payload={
                        "title": item.get("title", ""),
                        "url": item.get("url", ""),
                        "snippet": item.get("snippet", ""),
                        "search_query": query.query,
                        "category": query.category,
                        "city": query.city,
                        "state": query.state,
                    },
                )
            )
        return rows


def _parse_duckduckgo_results(html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    parsed: list[dict[str, str]] = []

    for result in soup.select(".result"):
        link = result.select_one("a.result__a") or result.find("a", href=True)
        if not link:
            continue
        href = _unwrap_duckduckgo_redirect((link.get("href") or "").strip())
        if not href:
            continue
        snippet_node = result.select_one(".result__snippet")
        snippet = snippet_node.get_text(" ", strip=True) if snippet_node else ""
        parsed.append(
            {
                "url": href,
                "title": link.get_text(" ", strip=True),
                "snippet": snippet,
            }
        )

    return parsed


def _unwrap_duckduckgo_redirect(url: str) -> str:
    if not url:
        return ""

    parsed = urlparse(url)
    if "duckduckgo.com" in parsed.netloc and parsed.path.startswith("/l/"):
        uddg = parse_qs(parsed.query).get("uddg", [""])[0]
        return unquote(uddg) if uddg else ""
    return url
