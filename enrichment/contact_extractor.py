from __future__ import annotations

import re
from dataclasses import asdict, dataclass

from app.services.crawl import CrawledPage

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"(?:\+?1[\s.\-]?)?\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}")
NAME_RE = re.compile(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\b")
WINDOW = 110
MAX_PAGES = 4
VALID_PAGE_TYPES = {"homepage", "about", "team", "contact"}


@dataclass
class ContactItem:
    value: str
    type: str
    source_page: str
    raw_text: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass
class ContactExtractionResult:
    items: list[ContactItem]

    @property
    def emails(self) -> list[ContactItem]:
        return [item for item in self.items if item.type == "email"]

    @property
    def phones(self) -> list[ContactItem]:
        return [item for item in self.items if item.type == "phone"]

    @property
    def names(self) -> list[ContactItem]:
        return [item for item in self.items if item.type == "name"]

    def to_dict(self) -> dict[str, list[dict[str, str]]]:
        return {"items": [item.to_dict() for item in self.items]}


def _snippet(text: str, start: int, end: int) -> str:
    left = max(0, start - WINDOW)
    right = min(len(text), end + WINDOW)
    return text[left:right].strip()


def _normalized_phone(phone: str) -> str:
    return re.sub(r"\D", "", phone)


def _is_name_like(value: str) -> bool:
    lowered = value.lower()
    if any(token in lowered for token in ("street", "avenue", "road", "boulevard", "contact", "about")):
        return False
    return len(value.split()) == 2


def _normalize_name(value: str) -> str:
    parts = value.split()
    if len(parts) == 3 and parts[0].lower() in {"owner", "founder", "manager", "director", "ceo", "partner"}:
        return " ".join(parts[1:])
    return value


def extract_contacts(pages: list[CrawledPage]) -> ContactExtractionResult:
    items: list[ContactItem] = []
    seen: set[tuple[str, str, str]] = set()

    scoped_pages = [p for p in pages if p.page_type in VALID_PAGE_TYPES][:MAX_PAGES]
    for page in scoped_pages:
        text = page.text or ""
        for regex, item_type in ((EMAIL_RE, "email"), (PHONE_RE, "phone"), (NAME_RE, "name")):
            for match in regex.finditer(text):
                value = match.group(0).strip()
                if item_type == "name":
                    value = _normalize_name(value)
                    if not _is_name_like(value):
                        continue
                if item_type == "phone":
                    value = value.strip()
                    if len(_normalized_phone(value)) < 10:
                        continue
                key_value = _normalized_phone(value) if item_type == "phone" else value.lower()
                key = (item_type, key_value, page.page_type)
                if key in seen:
                    continue
                seen.add(key)
                items.append(
                    ContactItem(
                        value=value,
                        type=item_type,
                        source_page=page.page_type,
                        raw_text=_snippet(text, match.start(), match.end()),
                    )
                )

    return ContactExtractionResult(items=items)
