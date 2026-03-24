from __future__ import annotations

from app.services.brave_search import BraveSearchClient
from app.services.lead_discovery.types import DiscoveryQuery, RawBusinessRecord


class BraveSearchSource:
    """Use Brave Search API and return normalized raw discovery rows."""

    name = "brave_search"

    def __init__(self) -> None:
        self.client = BraveSearchClient()

    def fetch(self, query: DiscoveryQuery) -> list[RawBusinessRecord]:
        rows: list[RawBusinessRecord] = []
        for item in self.client.search_web(query.query):
            rows.append(
                RawBusinessRecord(
                    source=self.name,
                    payload={
                        "title": item.title,
                        "url": item.url,
                        "snippet": item.description,
                        "provider": item.provider,
                        "search_query": query.query,
                        "category": query.category,
                        "city": query.city,
                        "state": query.state,
                    },
                )
            )
        return rows
