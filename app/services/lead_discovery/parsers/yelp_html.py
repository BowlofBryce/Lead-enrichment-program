from __future__ import annotations

import json
import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup


def _clean(s: str | None) -> str:
    if not s:
        return ""
    return " ".join(s.split()).strip()


def parse_yelp_search_results(html: str, base_url: str = "https://www.yelp.com") -> list[dict]:
    """
    Extract listing rows from a Yelp search HTML page.
    Uses multiple strategies: JSON-LD, embedded JSON, then link heuristics.
    """
    if not html or len(html.strip()) < 20:
        return []

    listings: list[dict] = []
    seen_paths: set[str] = set()

    def add_listing(biz_url: str, name: str, address: str = "", category: str = "") -> None:
        biz_url = biz_url.split("?")[0].split("#")[0]
        if "/biz/" not in biz_url:
            return
        if not biz_url.startswith("http"):
            biz_url = urljoin(base_url, biz_url)
        path = urlparse(biz_url).path
        if path in seen_paths:
            return
        seen_paths.add(path)
        name = _clean(name) or _clean(path.rsplit("/", 1)[-1].replace("-", " "))
        listings.append(
            {
                "listing_url": biz_url,
                "business_name": name,
                "address_line": _clean(address),
                "category_snippet": _clean(category),
            }
        )

    # Strategy 1: JSON-LD ItemList
    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string or script.get_text() or ""
        if not raw.strip():
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        items = _extract_ld_itemlist(data)
        for item in items:
            url = item.get("url") or ""
            name = item.get("name") or ""
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
            elif isinstance(addr, str):
                addr_text = addr
            if url and "/biz/" in url:
                add_listing(url, name, addr_text)

    # Strategy 2: /biz/ links
    for a in soup.select('a[href*="/biz/"]'):
        href = a.get("href") or ""
        if "redirect" in href.lower() or "/adredir" in href:
            continue
        text = _clean(a.get_text(" ", strip=True))
        parent = a.find_parent(["div", "section", "li"])
        address_guess = ""
        category_guess = ""
        if parent:
            for sel in ("address", '[class*="address"]', "span"):
                addr_el = parent.find(sel)
                if addr_el and addr_el.get_text(strip=True):
                    address_guess = _clean(addr_el.get_text(" ", strip=True))
                    break
        add_listing(href, text, address_guess, category_guess)

    # Strategy 4: regex fallback for bare /biz/ URLs in HTML
    for m in re.finditer(r'href="(/biz/[^"?#]+)', html):
        add_listing(m.group(1), "")

    return listings


def _extract_ld_itemlist(data: object) -> list[dict]:
    out: list[dict] = []
    if isinstance(data, dict):
        if data.get("@type") == "ItemList" and isinstance(data.get("itemListElement"), list):
            for el in data["itemListElement"]:
                if isinstance(el, dict) and isinstance(el.get("item"), dict):
                    out.append(el["item"])
                elif isinstance(el, dict) and el.get("@type") == "ListItem" and isinstance(el.get("item"), dict):
                    out.append(el["item"])
        for v in data.values():
            out.extend(_extract_ld_itemlist(v))
    elif isinstance(data, list):
        for item in data:
            out.extend(_extract_ld_itemlist(item))
    return out


def parse_yelp_biz_detail(html: str) -> dict:
    """Extract phone and website from a Yelp business detail page."""
    out: dict[str, str] = {"phone": "", "website": ""}
    if not html:
        return out
    soup = BeautifulSoup(html, "html.parser")

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("tel:"):
            digits = re.sub(r"\D", "", href)
            if len(digits) >= 10:
                out["phone"] = href.replace("tel:", "").strip()
                break

    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = (a.get_text() or "").lower()
        if "yelp.com/biz_redir" in href or "biz_redir" in href:
            # Yelp wraps outbound links
            from urllib.parse import parse_qs, urlparse

            qs = parse_qs(urlparse(href).query)
            url = (qs.get("url") or [""])[0]
            if url.startswith("http") and "yelp.com" not in url:
                out["website"] = url
                break
        if "website" in text and href.startswith("http") and "yelp." not in href:
            out["website"] = href
            break

    # Fallback: visible phone pattern
    if not out["phone"]:
        m = re.search(r"\(\d{3}\)\s*\d{3}[-\s]?\d{4}", soup.get_text(" ", strip=True))
        if m:
            out["phone"] = m.group(0)

    return out
