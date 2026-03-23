from __future__ import annotations

import re
from urllib.parse import urlparse


def normalize_text(value: str, *, title_case: bool = False, upper: bool = False) -> str:
    cleaned = " ".join((value or "").strip().split())
    if upper:
        return cleaned.upper()
    if title_case:
        return cleaned.title()
    return cleaned


def clean_phone(phone: str) -> str:
    digits = re.sub(r"\D", "", phone or "")
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"+1{digits}"
    return ""


def clean_url(url: str) -> str:
    token = (url or "").strip()
    if not token:
        return ""
    if not token.startswith(("http://", "https://")):
        token = f"https://{token}"
    parsed = urlparse(token)
    host = parsed.netloc.lower().replace("www.", "")
    path = parsed.path.rstrip("/")
    return f"https://{host}{path}" if host else ""


def website_domain(url: str) -> str:
    parsed = urlparse(url or "")
    return parsed.netloc.lower().replace("www.", "") if parsed.netloc else ""
