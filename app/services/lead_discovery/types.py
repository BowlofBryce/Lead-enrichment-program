from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class DiscoveryQuery:
    """Structured query for directory adapters."""

    query: str
    category: str
    keyword_variant: str
    city: str
    state: str


@dataclass(slots=True)
class RawBusinessRecord:
    source: str
    payload: dict[str, Any]


@dataclass(slots=True)
class ParsedLead:
    company_name: str
    address: str
    city: str
    state: str
    phone: str
    website: str
    category: str
    source: str
    source_ref: str = ""


@dataclass(slots=True)
class NormalizedLead:
    id: str
    company_name: str
    website: str
    phone: str
    city: str
    state: str
    category: str
    source: str
    address: str = ""
    source_ref: str = ""
    created_at: datetime = field(default_factory=datetime.utcnow)
    raw_payload: dict[str, Any] = field(default_factory=dict)

    def completeness_score(self) -> int:
        values = [
            self.company_name,
            self.website,
            self.phone,
            self.city,
            self.state,
            self.address,
        ]
        return sum(1 for value in values if bool((value or "").strip()))
