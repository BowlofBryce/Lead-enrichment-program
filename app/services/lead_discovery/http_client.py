from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from typing import Callable

import requests

from app.settings import settings


@dataclass
class RateLimitedClient:
    """Simple rate limiter for plain requests (e.g. Nominatim)."""

    min_interval_seconds: float
    timeout: int = settings.request_timeout_seconds
    _last_called_at: float = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_called_at
        if elapsed < self.min_interval_seconds:
            time.sleep(self.min_interval_seconds - elapsed + random.uniform(0, 0.2))
        self._last_called_at = time.monotonic()


@dataclass
class FetchResult:
    url: str
    status_code: int
    text: str
    error: str = ""
    used_playwright: bool = False


def _default_headers() -> dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


@dataclass
class RateLimitedHttpClient:
    """HTTP client with per-host rate limiting, retries, and optional Playwright fallback."""

    min_interval_seconds: float = 1.2
    timeout_seconds: int = settings.request_timeout_seconds
    max_retries: int = 3
    backoff_base: float = 1.5
    jitter_seconds: float = 0.35
    _last_fetch: dict[str, float] = field(default_factory=dict)

    def _wait_for_host(self, host: str) -> None:
        now = time.monotonic()
        last = self._last_fetch.get(host, 0.0)
        elapsed = now - last
        if elapsed < self.min_interval_seconds:
            time.sleep(self.min_interval_seconds - elapsed + random.uniform(0, self.jitter_seconds))
        self._last_fetch[host] = time.monotonic()

    def get_text(
        self,
        url: str,
        *,
        referer: str | None = None,
        playwright_fallback: Callable[[str], str] | None = None,
    ) -> FetchResult:
        from urllib.parse import urlparse

        host = urlparse(url).netloc or "default"
        headers = _default_headers()
        if referer:
            headers["Referer"] = referer

        last_error = ""
        for attempt in range(self.max_retries):
            self._wait_for_host(host)
            try:
                resp = requests.get(url, headers=headers, timeout=self.timeout_seconds)
                text = resp.text or ""
                if resp.status_code >= 400:
                    last_error = f"http_{resp.status_code}"
                    use_pw = playwright_fallback and (
                        resp.status_code in (403, 429, 503) or len(text) < 400
                    )
                    if use_pw:
                        try:
                            pw_text = playwright_fallback(url)
                            if pw_text and len(pw_text) > 400:
                                return FetchResult(url=url, status_code=200, text=pw_text, used_playwright=True)
                        except Exception as exc:
                            last_error = f"{last_error};playwright:{exc}"
                    time.sleep(self.backoff_base * (attempt + 1))
                    continue
                if len(text) < 200 and playwright_fallback:
                    try:
                        pw_text = playwright_fallback(url)
                        if pw_text and len(pw_text) > len(text):
                            return FetchResult(url=url, status_code=200, text=pw_text, used_playwright=True)
                    except Exception:
                        pass
                return FetchResult(url=url, status_code=resp.status_code, text=text)
            except requests.RequestException as exc:
                last_error = str(exc)
                time.sleep(self.backoff_base * (attempt + 1))

        return FetchResult(url=url, status_code=0, text="", error=last_error or "request_failed")


def playwright_fetch_html(url: str) -> str:
    """Sync Playwright fetch for JS-rendered directory pages (optional dependency)."""
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=settings.request_timeout_seconds * 1000)
            return page.content()
        finally:
            browser.close()
