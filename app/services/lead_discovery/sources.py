from __future__ import annotations

from typing import Protocol

from app.services.lead_discovery.duckduckgo import DuckDuckGoHTMLSource
from app.services.lead_discovery.types import DiscoveryQuery, RawBusinessRecord
from app.settings import settings


class SourceAdapter(Protocol):
    name: str

    def fetch(self, query: DiscoveryQuery) -> list[RawBusinessRecord]:
        ...


# Priority when merging parallel fetch results.
SOURCE_MERGE_ORDER: tuple[str, ...] = (
    "duckduckgo_html",
)


def build_enabled_sources() -> list[SourceAdapter]:
    """DuckDuckGo HTML search is the canonical and only active discovery source."""
    if not settings.discovery_enable_duckduckgo_html:
        return []
    return [DuckDuckGoHTMLSource()]


def merge_order_index(name: str) -> int:
    try:
        return SOURCE_MERGE_ORDER.index(name)
    except ValueError:
        return len(SOURCE_MERGE_ORDER)
