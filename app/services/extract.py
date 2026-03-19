from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urlparse

from app.services.crawl import CrawledPage


EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"(?:\+?1[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}")
ADDRESS_RE = re.compile(
    r"\d{1,6}\s+[A-Za-z0-9.\s]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Lane|Ln|Drive|Dr)\b",
    flags=re.IGNORECASE,
)


@dataclass
class ExtractionResult:
    emails: list[str]
    phones: list[str]
    social_links: dict[str, str]
    address_text: str
    contact_page_url: str
    about_page_url: str
    team_page_url: str
    has_contact_form: bool
    booking_signals: list[str]
    financing_signals: list[str]
    chat_widget_signals: list[str]


def _uniq(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        key = item.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def extract_from_pages(pages: list[CrawledPage]) -> ExtractionResult:
    all_text = " ".join([p.text for p in pages if p.text])
    all_html = " ".join([p.html for p in pages if p.html]).lower()

    emails = _uniq(EMAIL_RE.findall(all_text))
    phones = _uniq(PHONE_RE.findall(all_text))

    social: dict[str, str] = {}
    for p in pages:
        if not p.html:
            continue
        lowered = p.html.lower()
        for net in ("facebook.com", "instagram.com", "linkedin.com"):
            if net in social:
                continue
            idx = lowered.find(net)
            if idx != -1:
                start = lowered.rfind("http", 0, idx)
                if start != -1:
                    end = lowered.find('"', idx)
                    social[net] = p.html[start:end] if end > start else ""

    address_match = ADDRESS_RE.search(all_text)
    address_text = address_match.group(0) if address_match else ""

    contact_page_url = ""
    about_page_url = ""
    team_page_url = ""
    for p in pages:
        if p.page_type == "contact":
            contact_page_url = p.url
        elif p.page_type == "about":
            about_page_url = p.url
        elif p.page_type == "team":
            team_page_url = p.url

    has_contact_form = ("<form" in all_html) and ("contact" in all_html)

    booking_signals = _uniq(
        [k for k in ["book now", "schedule", "appointment", "reserve"] if k in all_text.lower()]
    )
    financing_signals = _uniq([k for k in ["financing", "payment plan", "installments"] if k in all_text.lower()])
    chat_widget_signals = _uniq(
        [k for k in ["livechat", "intercom", "drift", "chat widget", "tawk.to"] if k in all_html]
    )

    return ExtractionResult(
        emails=emails,
        phones=phones,
        social_links={
            "facebook_url": _first_social(social, "facebook.com"),
            "instagram_url": _first_social(social, "instagram.com"),
            "linkedin_url": _first_social(social, "linkedin.com"),
        },
        address_text=address_text,
        contact_page_url=contact_page_url,
        about_page_url=about_page_url,
        team_page_url=team_page_url,
        has_contact_form=has_contact_form,
        booking_signals=booking_signals,
        financing_signals=financing_signals,
        chat_widget_signals=chat_widget_signals,
    )


def _first_social(social: dict[str, str], key: str) -> str:
    value = social.get(key, "")
    if value:
        return value
    for v in social.values():
        if key in urlparse(v).netloc:
            return v
    return ""
