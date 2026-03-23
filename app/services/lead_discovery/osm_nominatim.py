from __future__ import annotations

import requests

from app.services.lead_discovery.http_client import RateLimitedClient
from app.services.lead_discovery.types import DiscoveryQuery, RawBusinessRecord
from app.settings import settings


class OpenStreetMapSource:
    """Nominatim geocoder search — lower-quality business coverage; use as fallback only."""

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
            timeout=settings.request_timeout_seconds,
        )
        resp.raise_for_status()
        rows = resp.json()
        return [RawBusinessRecord(source=self.name, payload=item) for item in rows if isinstance(item, dict)]
