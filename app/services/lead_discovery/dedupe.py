from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher

from app.services.lead_discovery.normalization import website_domain
from app.services.lead_discovery.types import NormalizedLead


@dataclass
class DedupeResult:
    chosen: NormalizedLead
    duplicate: NormalizedLead | None
    reason: str


def _name_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, (a or "").lower(), (b or "").lower()).ratio()


def choose_best(existing: NormalizedLead, incoming: NormalizedLead) -> DedupeResult:
    if incoming.completeness_score() > existing.completeness_score():
        return DedupeResult(chosen=incoming, duplicate=existing, reason="incoming_more_complete")
    return DedupeResult(chosen=existing, duplicate=incoming, reason="existing_more_complete")


def is_duplicate(existing: NormalizedLead, incoming: NormalizedLead) -> tuple[bool, str]:
    if existing.website and incoming.website and website_domain(existing.website) == website_domain(incoming.website):
        return True, "same_website"
    if existing.phone and incoming.phone and existing.phone == incoming.phone:
        return True, "same_phone"
    if existing.city and incoming.city and existing.city.lower() == incoming.city.lower():
        if _name_similarity(existing.company_name, incoming.company_name) >= 0.9:
            return True, "fuzzy_name_city"
    return False, ""
