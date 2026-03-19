from __future__ import annotations

import re
from urllib.parse import urlparse


def clean_company_name(name: str | None) -> str:
    if not name:
        return ""
    cleaned = re.sub(r"\s+", " ", name).strip()
    cleaned = re.sub(
        r"\b(inc|llc|l\.l\.c\.|corp|corporation|co|company|ltd|limited)\b\.?",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    return re.sub(r"\s+", " ", cleaned).strip(" ,.-")


def normalize_domain(website: str | None) -> str:
    if not website:
        return ""
    raw = website.strip()
    if not raw:
        return ""
    if not raw.startswith(("http://", "https://")):
        raw = f"https://{raw}"
    try:
        parsed = urlparse(raw)
        domain = parsed.netloc.lower().strip()
        if domain.startswith("www."):
            domain = domain[4:]
        return domain
    except Exception:
        return ""


def normalize_url(website: str | None) -> str:
    if not website:
        return ""
    raw = website.strip()
    if not raw:
        return ""
    if not raw.startswith(("http://", "https://")):
        raw = f"https://{raw}"
    return raw


def normalize_phone(phone: str | None) -> str:
    if not phone:
        return ""
    digits = re.sub(r"\D", "", phone)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"+1{digits}"
    return digits


def dedupe_key(company_name: str | None, website: str | None) -> str:
    domain = normalize_domain(website)
    cleaned_name = clean_company_name(company_name).lower()
    return f"{domain}|{cleaned_name}"
