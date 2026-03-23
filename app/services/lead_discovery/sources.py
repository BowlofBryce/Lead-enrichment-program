from __future__ import annotations

from typing import Protocol

import requests

from app.services.lead_discovery.directory_yelp import YelpDirectorySource
from app.services.lead_discovery.directory_yellowpages import YellowPagesDirectorySource
from app.services.lead_discovery.http_client import RateLimitedClient
from app.services.lead_discovery.types import DiscoveryQuery, RawBusinessRecord
from app.services.lead_discovery.osm_nominatim import OpenStreetMapSource
from app.settings import settings


class SourceAdapter(Protocol):
    name: str

    def fetch(self, query: DiscoveryQuery) -> list[RawBusinessRecord]:
        ...


class GooglePlacesSource:
    """Optional paid API — disabled by default."""

    name = "google_places"

    def __init__(self, api_key: str, min_interval_seconds: float = 0.25) -> None:
        self.api_key = api_key
        self.client = RateLimitedClient(min_interval_seconds=min_interval_seconds)

    def fetch(self, query: DiscoveryQuery) -> list[RawBusinessRecord]:
        self.client.wait()
        url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        resp = requests.get(
            url,
            params={"query": query.query, "key": self.api_key},
            timeout=self.client.timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        rows: list[RawBusinessRecord] = []
        for item in data.get("results", []):
            rows.append(RawBusinessRecord(source=self.name, payload=item))
        return rows


class YelpFusionAPISource:
    """Optional Yelp Fusion API — disabled by default (directory scrape is primary)."""

    name = "yelp_api"

    def __init__(self, api_key: str, min_interval_seconds: float = 0.3) -> None:
        self.api_key = api_key
        self.client = RateLimitedClient(min_interval_seconds=min_interval_seconds)

    def fetch(self, query: DiscoveryQuery) -> list[RawBusinessRecord]:
        self.client.wait()
        resp = requests.get(
            "https://api.yelp.com/v3/businesses/search",
            params={
                "term": query.keyword_variant or query.category,
                "location": f"{query.city}, {query.state}",
                "limit": 50,
            },
            headers={"Authorization": f"Bearer {self.api_key}"},
            timeout=self.client.timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        return [RawBusinessRecord(source=self.name, payload=item) for item in data.get("businesses", [])]


# Priority when merging parallel fetch results (higher-quality directory sources first).
SOURCE_MERGE_ORDER: tuple[str, ...] = (
    "yelp_directory",
    "yellowpages_directory",
    "yelp_api",
    "google_places",
    "openstreetmap",
)


def build_enabled_sources() -> list[SourceAdapter]:
    """Directory scrapers first; OSM last; APIs opt-in via settings."""
    sources: list[SourceAdapter] = []

    if settings.discovery_enable_yelp_directory:
        sources.append(YelpDirectorySource())
    if settings.discovery_enable_yellowpages_directory:
        sources.append(YellowPagesDirectorySource())
    if settings.yelp_api_key and settings.discovery_enable_yelp_fusion_api:
        sources.append(YelpFusionAPISource(settings.yelp_api_key, settings.discovery_yelp_min_interval_seconds))
    if settings.google_places_api_key and settings.discovery_enable_google_places:
        sources.append(GooglePlacesSource(settings.google_places_api_key, settings.discovery_google_min_interval_seconds))
    if settings.discovery_enable_osm_fallback:
        sources.append(OpenStreetMapSource(settings.discovery_osm_user_agent, settings.discovery_osm_min_interval_seconds))

    return sources


def merge_order_index(name: str) -> int:
    try:
        return SOURCE_MERGE_ORDER.index(name)
    except ValueError:
        return len(SOURCE_MERGE_ORDER)
