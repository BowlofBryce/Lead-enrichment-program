from __future__ import annotations

from typing import Protocol

from app.services.lead_discovery.brave import BraveSearchSource
from app.services.lead_discovery.types import DiscoveryQuery, RawBusinessRecord
from app.settings import settings


class SourceAdapter(Protocol):
    name: str

    def fetch(self, query: DiscoveryQuery) -> list[RawBusinessRecord]:
        ...


# Priority when merging parallel fetch results.
SOURCE_MERGE_ORDER: tuple[str, ...] = (
    "brave_search",
)


def build_enabled_sources() -> list[SourceAdapter]:
    provider = settings.discovery_provider.lower().strip()
    if provider != "brave":
        return []
    return [BraveSearchSource()]


def merge_order_index(name: str) -> int:
    try:
        return SOURCE_MERGE_ORDER.index(name)
    except ValueError:
        return len(SOURCE_MERGE_ORDER)
