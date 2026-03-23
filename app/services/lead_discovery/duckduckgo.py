from __future__ import annotations

import random
import time
from urllib.parse import parse_qs, unquote, urlparse

import requests
from bs4 import BeautifulSoup

from app.services.lead_discovery.types import DiscoveryQuery, RawBusinessRecord
from app.services.logging_utils import get_logger
from app.settings import settings


logger = get_logger(__name__)


class DuckDuckGoHTMLSource:
    """Search DuckDuckGo HTML endpoint and return normalized raw discovery rows."""

    name = "duckduckgo_html"

    def __init__(self) -> None:
        self.endpoint = settings.discovery_duckduckgo_html_url
        self.max_results = settings.discovery_duckduckgo_max_results_per_query
        self.user_agent = settings.discovery_duckduckgo_user_agent
        self.min_delay = settings.duckduckgo_min_delay_seconds
        self.max_delay = settings.duckduckgo_max_delay_seconds
        self.consecutive_403_threshold = max(1, settings.duckduckgo_consecutive_403_threshold)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": self.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
        )
        self.disabled_for_run = False
        self.consecutive_403 = 0
        self.query_403_counts: dict[str, int] = {}
        self.total_success_results = 0

    def _wait_between_requests(self) -> None:
        time.sleep(random.uniform(self.min_delay, self.max_delay))

    def _throttle_backoff(self, *, status_code: int, attempt: int) -> None:
        base = self.min_delay * (2**attempt)
        cap = self.max_delay * 4
        sleep_seconds = min(cap, base + random.uniform(0.4, 1.8))
        logger.warning(
            "duckduckgo_throttle_backoff",
            extra={"status_code": status_code, "attempt": attempt + 1, "sleep_seconds": round(sleep_seconds, 2)},
        )
        time.sleep(sleep_seconds)

    def _disable_provider(self) -> None:
        if self.disabled_for_run:
            return
        self.disabled_for_run = True
        logger.warning(
            "duckduckgo_provider_disabled_due_to_throttling",
            extra={"consecutive_403": self.consecutive_403, "successful_results_before_block": self.total_success_results},
        )

    def fetch(self, query: DiscoveryQuery) -> list[RawBusinessRecord]:
        if self.disabled_for_run:
            logger.info("duckduckgo_provider_status", extra={"enabled": False, "reason": "provider_disabled_for_run"})
            return []

        query_key = query.query.strip().lower()
        if self.query_403_counts.get(query_key, 0) >= 2:
            logger.info("duckduckgo_query_skipped_blocked", extra={"query": query.query, "reason": "already_received_403_twice"})
            return []

        self._wait_between_requests()
        response = None
        for attempt in range(2):
            try:
                response = self.session.get(
                    self.endpoint,
                    params={"q": query.query},
                    timeout=settings.request_timeout_seconds,
                )
            except requests.RequestException:
                raise

            if response.status_code in (202, 403):
                self._throttle_backoff(status_code=response.status_code, attempt=attempt)
                if response.status_code == 403:
                    self.consecutive_403 += 1
                    self.query_403_counts[query_key] = self.query_403_counts.get(query_key, 0) + 1
                    if self.consecutive_403 >= self.consecutive_403_threshold:
                        self._disable_provider()
                    if self.query_403_counts[query_key] >= 2:
                        logger.warning(
                            "duckduckgo_query_blocked_for_run",
                            extra={"query": query.query, "query_403_count": self.query_403_counts[query_key]},
                        )
                        return []
                if attempt == 0 and not self.disabled_for_run:
                    continue
                return []

            break

        if response is None:
            return []

        if response.status_code >= 400:
            logger.warning(
                "duckduckgo_non_success_status",
                extra={"status_code": response.status_code, "query": query.query},
            )
            return []
        self.consecutive_403 = 0

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
        self.total_success_results += len(rows)
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
