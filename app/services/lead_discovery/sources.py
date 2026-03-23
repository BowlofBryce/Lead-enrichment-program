from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Protocol

import requests

from app.services.lead_discovery.types import DiscoveryQuery, RawBusinessRecord
from app.settings import settings


class SourceAdapter(Protocol):
    name: str

    def fetch(self, query: DiscoveryQuery) -> list[RawBusinessRecord]:
        ...


@dataclass
class RateLimitedClient:
    min_interval_seconds: float
    timeout: int = settings.request_timeout_seconds
    _last_called_at: float = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_called_at
        if elapsed < self.min_interval_seconds:
            time.sleep(self.min_interval_seconds - elapsed)
        self._last_called_at = time.monotonic()


class GooglePlacesSource:
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


class YelpSource:
    name = "yelp"

    def __init__(self, api_key: str, min_interval_seconds: float = 0.3) -> None:
        self.api_key = api_key
        self.client = RateLimitedClient(min_interval_seconds=min_interval_seconds)

    def fetch(self, query: DiscoveryQuery) -> list[RawBusinessRecord]:
        self.client.wait()
        resp = requests.get(
            "https://api.yelp.com/v3/businesses/search",
            params={"term": query.category, "location": f"{query.city}, {query.state}", "limit": 50},
            headers={"Authorization": f"Bearer {self.api_key}"},
            timeout=self.client.timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        return [RawBusinessRecord(source=self.name, payload=item) for item in data.get("businesses", [])]


class OpenStreetMapSource:
    name = "openstreetmap"

    def __init__(self, user_agent: str, min_interval_seconds: float = 1.1) -> None:
        self.user_agent = user_agent
        self.client = RateLimitedClient(min_interval_seconds=min_interval_seconds)

    def fetch(self, query: DiscoveryQuery) -> list[RawBusinessRecord]:
        self.client.wait()
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": query.query, "format": "jsonv2", "addressdetails": 1, "limit": 50},
            headers={"User-Agent": self.user_agent},
            timeout=self.client.timeout,
        )
        resp.raise_for_status()
        rows = resp.json()
        return [RawBusinessRecord(source=self.name, payload=item) for item in rows if isinstance(item, dict)]


def build_enabled_sources() -> list[SourceAdapter]:
    sources: list[SourceAdapter] = []
    if settings.google_places_api_key:
        sources.append(GooglePlacesSource(settings.google_places_api_key, settings.discovery_google_min_interval_seconds))
    if settings.yelp_api_key:
        sources.append(YelpSource(settings.yelp_api_key, settings.discovery_yelp_min_interval_seconds))
    if settings.discovery_enable_osm:
        sources.append(OpenStreetMapSource(settings.discovery_osm_user_agent, settings.discovery_osm_min_interval_seconds))
    return sources
