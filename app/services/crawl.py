from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from app.settings import settings


PAGE_HINTS = {
    "contact": ["contact", "contact-us", "get-in-touch"],
    "about": ["about", "about-us", "our-story"],
    "team": ["team", "staff", "leadership", "our-team"],
    "services": ["services", "what-we-do", "solutions"],
}


@dataclass
class CrawledPage:
    page_type: str
    url: str
    title: str
    html: str
    text: str
    fetched_with: str
    fetch_status: str


def _fetch_with_requests(url: str) -> tuple[str, str, str]:
    try:
        resp = requests.get(
            url,
            timeout=settings.request_timeout_seconds,
            headers={"User-Agent": "LeadEnrichmentLocal/1.0"},
        )
        resp.raise_for_status()
        html = resp.text
        soup = BeautifulSoup(html, "html.parser")
        title = soup.title.get_text(strip=True) if soup.title else ""
        text = soup.get_text(" ", strip=True)
        return html, title, text
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc


def _fetch_with_playwright(url: str) -> tuple[str, str, str]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise RuntimeError(f"Playwright unavailable: {exc}") from exc

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.goto(url, wait_until="networkidle", timeout=settings.request_timeout_seconds * 1000)
            html = page.content()
            soup = BeautifulSoup(html, "html.parser")
            title = page.title() or (soup.title.get_text(strip=True) if soup.title else "")
            text = soup.get_text(" ", strip=True)
            return html, title, text
        finally:
            browser.close()


def _needs_js_fallback(text: str, html: str) -> bool:
    if len(text) < 180:
        return True
    lowered = html.lower()
    return "__next" in lowered or "id=\"app\"" in lowered or "react-root" in lowered


def discover_candidate_links(base_url: str, homepage_html: str) -> dict[str, str]:
    soup = BeautifulSoup(homepage_html, "html.parser")
    out: dict[str, str] = {}
    base_domain = urlparse(base_url).netloc
    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "").strip()
        if not href:
            continue
        full = urljoin(base_url, href)
        parsed = urlparse(full)
        if parsed.netloc != base_domain:
            continue
        lower = parsed.path.lower().strip("/")
        for page_type, hints in PAGE_HINTS.items():
            if page_type in out:
                continue
            if any(h in lower for h in hints):
                out[page_type] = full
    return out


def crawl_site(start_url: str) -> list[CrawledPage]:
    pages: list[CrawledPage] = []

    try:
        html, title, text = _fetch_with_requests(start_url)
        fetched_with = "requests"
        if _needs_js_fallback(text, html):
            html, title, text = _fetch_with_playwright(start_url)
            fetched_with = "playwright"
        pages.append(
            CrawledPage(
                page_type="homepage",
                url=start_url,
                title=title,
                html=html,
                text=text,
                fetched_with=fetched_with,
                fetch_status="ok",
            )
        )
    except Exception as exc:
        pages.append(
            CrawledPage(
                page_type="homepage",
                url=start_url,
                title="",
                html="",
                text="",
                fetched_with="requests",
                fetch_status=f"error: {exc}",
            )
        )
        return pages

    links = discover_candidate_links(start_url, pages[0].html)
    for page_type in ("contact", "about", "team", "services"):
        url = links.get(page_type)
        if not url:
            continue
        try:
            html, title, text = _fetch_with_requests(url)
            fetched_with = "requests"
            if _needs_js_fallback(text, html):
                html, title, text = _fetch_with_playwright(url)
                fetched_with = "playwright"
            pages.append(
                CrawledPage(
                    page_type=page_type,
                    url=url,
                    title=title,
                    html=html,
                    text=text,
                    fetched_with=fetched_with,
                    fetch_status="ok",
                )
            )
        except Exception as exc:
            pages.append(
                CrawledPage(
                    page_type=page_type,
                    url=url,
                    title="",
                    html="",
                    text="",
                    fetched_with="requests",
                    fetch_status=f"error: {exc}",
                )
            )
    return pages
