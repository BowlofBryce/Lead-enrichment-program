from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import requests

from app.services.app_config import get_brave_settings_config
from app.services.logging_utils import get_logger
from app.settings import settings


logger = get_logger(__name__)


class BraveSearchError(RuntimeError):
    """Base exception for Brave Search failures."""


class BraveSearchConfigurationError(BraveSearchError):
    pass


class BraveSearchAuthError(BraveSearchError):
    pass


class BraveSearchRateLimitError(BraveSearchError):
    pass


class BraveSearchProviderError(BraveSearchError):
    pass


class BraveSearchResponseError(BraveSearchError):
    pass


@dataclass(slots=True)
class BraveSearchResult:
    title: str
    url: str
    description: str
    provider: str = "brave"


class BraveSearchClient:
    def __init__(self) -> None:
        config = get_brave_settings_config()
        self.discovery_provider = config.discovery_provider
        self.base_url = config.brave_search_base_url.rstrip("/")
        self.api_key = config.brave_search_api_key.strip()
        self.timeout_seconds = config.brave_search_timeout_seconds
        self.max_results = config.brave_search_max_results_per_query
        self.country = config.brave_search_country
        self.search_lang = config.brave_search_search_lang
        self.freshness = config.brave_search_freshness
        self.max_retries = max(0, config.brave_search_max_retries)

    def validate_configuration(self) -> None:
        if self.discovery_provider.lower() == "brave" and not self.api_key:
            raise BraveSearchConfigurationError(
                "BRAVE_SEARCH_API_KEY is required when DISCOVERY_PROVIDER=brave"
            )

    def search_web(self, query: str) -> list[BraveSearchResult]:
        self.validate_configuration()
        query_clean = (query or "").strip()
        if not query_clean:
            raise BraveSearchResponseError("Brave search query cannot be empty")

        started = time.perf_counter()
        logger.info(
            "discovery.query.started",
            extra_fields={"provider": "brave", "query": query_clean},
        )

        for attempt in range(self.max_retries + 1):
            status_code = 0
            try:
                resp = requests.get(
                    f"{self.base_url}/web/search",
                    params=self._request_params(query_clean),
                    headers={
                        "Accept": "application/json",
                        "Accept-Encoding": "gzip",
                        "X-Subscription-Token": self.api_key,
                    },
                    timeout=self.timeout_seconds,
                )
                status_code = resp.status_code
                if status_code in (401, 403):
                    raise BraveSearchAuthError(
                        f"Brave Search authentication failed with HTTP {status_code}"
                    )
                if status_code == 429:
                    if attempt < self.max_retries:
                        delay = self._backoff_delay(attempt)
                        logger.warning(
                            "discovery.query.rate_limited",
                            extra_fields={
                                "provider": "brave",
                                "query": query_clean,
                                "status_code": status_code,
                                "retry_count": attempt + 1,
                                "sleep_seconds": round(delay, 2),
                            },
                        )
                        time.sleep(delay)
                        continue
                    raise BraveSearchRateLimitError(
                        f"Brave Search rate limit exceeded after {self.max_retries + 1} attempts"
                    )
                if status_code >= 500:
                    raise BraveSearchProviderError(
                        f"Brave Search provider error HTTP {status_code}"
                    )
                if status_code >= 400:
                    raise BraveSearchProviderError(
                        f"Brave Search request failed HTTP {status_code}"
                    )

                payload = resp.json()
                normalized = self._normalize_results(payload)
                elapsed = int((time.perf_counter() - started) * 1000)
                logger.info(
                    "discovery.query.completed",
                    extra_fields={
                        "provider": "brave",
                        "query": query_clean,
                        "status_code": status_code,
                        "result_count": len(normalized),
                        "retry_count": attempt,
                        "duration_ms": elapsed,
                    },
                )
                logger.info(
                    "discovery.results.normalized",
                    extra_fields={
                        "provider": "brave",
                        "query": query_clean,
                        "result_count": len(normalized),
                    },
                )
                return normalized
            except (requests.RequestException, ValueError) as exc:
                err = BraveSearchProviderError(f"Brave Search request failed: {exc}")
            except BraveSearchError as exc:
                err = exc

            elapsed = int((time.perf_counter() - started) * 1000)
            logger.error(
                "discovery.query.failed",
                extra_fields={
                    "provider": "brave",
                    "query": query_clean,
                    "status_code": status_code,
                    "retry_count": attempt,
                    "duration_ms": elapsed,
                    "error": str(err),
                },
            )
            raise err

        raise BraveSearchProviderError("Unexpected Brave Search retry state")

    def _request_params(self, query: str) -> dict[str, Any]:
        params: dict[str, Any] = {
            "q": query,
            "count": self.max_results,
            "country": self.country,
            "search_lang": self.search_lang,
        }
        if self.freshness:
            params["freshness"] = self.freshness
        return params

    def _normalize_results(self, payload: dict[str, Any]) -> list[BraveSearchResult]:
        web = payload.get("web")
        if not isinstance(web, dict):
            raise BraveSearchResponseError("Brave response missing web object")
        items = web.get("results")
        if not isinstance(items, list):
            raise BraveSearchResponseError("Brave response missing web.results array")

        logger.info(
            "discovery.results.received",
            extra_fields={"provider": "brave", "result_count": len(items)},
        )

        out: list[BraveSearchResult] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            url = str(item.get("url") or "").strip()
            title = str(item.get("title") or "").strip()
            description = str(item.get("description") or "").strip()
            if not url:
                continue
            out.append(
                BraveSearchResult(
                    title=title,
                    url=url,
                    description=description,
                    provider=str(item.get("profile", {}).get("name") or "brave"),
                )
            )
        return out[: self.max_results]

    @staticmethod
    def _backoff_delay(attempt: int) -> float:
        return min(10.0, 1.0 * (2**attempt))
