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


class DedupeState:
    """
    In-run dedupe indexes (phone, domain, fuzzy name within city).
    Scales to large runs: stores compact keys only, not full lead objects.
    """

    def __init__(self) -> None:
        self._phone_to_lead: dict[str, int] = {}
        self._domain_to_lead: dict[str, int] = {}
        self._fuzzy_by_city: dict[str, list[tuple[int, str]]] = {}

    def find_match(self, normalized: NormalizedLead) -> tuple[int | None, str]:
        if normalized.phone and normalized.phone in self._phone_to_lead:
            return self._phone_to_lead[normalized.phone], "same_phone"
        dom = website_domain(normalized.website)
        if dom and dom in self._domain_to_lead:
            return self._domain_to_lead[dom], "same_website"
        city_key = (normalized.city or "").lower()
        if city_key:
            for lead_id, name in self._fuzzy_by_city.get(city_key, []):
                if _name_similarity(name, normalized.company_name) >= 0.9:
                    return lead_id, "fuzzy_name_city"
        return None, ""

    def remove_keys(self, lead_id: int, n: NormalizedLead) -> None:
        if n.phone and self._phone_to_lead.get(n.phone) == lead_id:
            del self._phone_to_lead[n.phone]
        dom = website_domain(n.website)
        if dom and self._domain_to_lead.get(dom) == lead_id:
            del self._domain_to_lead[dom]
        city_key = (n.city or "").lower()
        if city_key and city_key in self._fuzzy_by_city:
            self._fuzzy_by_city[city_key] = [pair for pair in self._fuzzy_by_city[city_key] if pair[0] != lead_id]
            if not self._fuzzy_by_city[city_key]:
                del self._fuzzy_by_city[city_key]

    def add_keys(self, lead_id: int, n: NormalizedLead) -> None:
        if n.phone:
            self._phone_to_lead[n.phone] = lead_id
        dom = website_domain(n.website)
        if dom:
            self._domain_to_lead[dom] = lead_id
        city_key = (n.city or "").lower()
        if city_key:
            self._fuzzy_by_city.setdefault(city_key, []).append((lead_id, n.company_name))
