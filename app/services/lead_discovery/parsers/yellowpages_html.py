from __future__ import annotations

import json
import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup


def _clean(s: str | None) -> str:
    if not s:
        return ""
    return " ".join(s.split()).strip()


def parse_yellowpages_search_results(html: str, base_url: str = "https://www.yellowpages.com") -> list[dict]:
    """
    Extract listings from Yellow Pages search HTML.
    Multiple strategies: JSON-LD, vetted business links, generic result cards.
    """
    if not html or len(html.strip()) < 20:
        return []

    results: list[dict] = []
    seen: set[str] = set()

    def add_row(listing_url: str, name: str, phone: str = "", address: str = "") -> None:
        if not listing_url.startswith("http"):
            listing_url = urljoin(base_url, listing_url)
        path = urlparse(listing_url).path
        key = path or listing_url
        if key in seen:
            return
        seen.add(key)
        results.append(
            {
                "listing_url": listing_url,
                "business_name": _clean(name) or _clean(path.strip("/").split("/")[-1].replace("-", " ")),
                "phone": _clean(phone),
                "address_line": _clean(address),
            }
        )

    soup = BeautifulSoup(html, "html.parser")

    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string or script.get_text() or ""
        if not raw.strip():
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        items = _walk_ld(data)
        for item in items:
            if not isinstance(item, dict):
                continue
            url = item.get("url") or ""
            name = item.get("name") or ""
            phone = ""
            if isinstance(item.get("telephone"), str):
                phone = item["telephone"]
            addr = item.get("address")
            addr_text = ""
            if isinstance(addr, dict):
                addr_text = ", ".join(
                    filter(
                        None,
                        [
                            addr.get("streetAddress"),
                            addr.get("addressLocality"),
                            addr.get("addressRegion"),
                        ],
                    )
                )
            if url and "yellowpages.com" in url:
                add_row(url, name, phone, addr_text)

    # Heuristic: business name links (common YP layout)
    for a in soup.select("a.business-name, a[class*='business-name'], h2 a, h3 a"):
        href = a.get("href") or ""
        if not href or href.startswith("#"):
            continue
        if "yellowpages.com" not in href and not href.startswith("/"):
            continue
        if "/search?" in href:
            continue
        text = _clean(a.get_text(" ", strip=True))
        parent = a.find_parent("div") or a.parent
        phone = ""
        addr = ""
        if parent:
            phone_el = parent.find(class_=re.compile(r"phone", re.I))
            if phone_el:
                phone = _clean(phone_el.get_text(" ", strip=True))
            addr_el = parent.find(class_=re.compile(r"adr|street|address", re.I))
            if addr_el:
                addr = _clean(addr_el.get_text(" ", strip=True))
        add_row(href, text, phone, addr)

    # Broad fallback: profile links containing state/city slug pattern
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r"/[a-z]{2}/[^/]+/[^/?]+\.html", href):
            add_row(href, _clean(a.get_text(" ", strip=True)))

    return results


def _walk_ld(data: object) -> list[dict]:
    found: list[dict] = []
    if isinstance(data, dict):
        if data.get("@type") in {"LocalBusiness", "Organization", "Place"} and data.get("name"):
            found.append(data)
        for v in data.values():
            found.extend(_walk_ld(v))
    elif isinstance(data, list):
        for item in data:
            found.extend(_walk_ld(item))
    return found


def parse_yellowpages_detail(html: str) -> dict:
    """Pull phone/website from a YP business detail page if missing from search."""
    out: dict[str, str] = {"phone": "", "website": ""}
    if not html:
        return out
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        if a["href"].startswith("tel:"):
            out["phone"] = _clean(a["href"].replace("tel:", ""))
            break
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("http") and "yellowpages.com" not in href:
            label = (a.get_text() or "").lower()
            if "website" in label or "visit" in label:
                out["website"] = href
                break
    return out
