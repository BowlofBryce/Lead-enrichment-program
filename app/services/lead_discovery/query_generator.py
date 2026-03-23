from __future__ import annotations

import json
from itertools import product
from typing import Iterable

from app.services.lead_discovery.types import DiscoveryQuery
from app.services.ollama_client import generate_json
from app.settings import settings


NEARBY_STATE_CITIES: dict[str, list[str]] = {
    "UT": ["Salt Lake City", "Provo", "Ogden", "St George", "Lehi", "Orem", "Park City", "West Valley City"],
    "ID": ["Boise", "Idaho Falls", "Nampa", "Meridian"],
    "NV": ["Las Vegas", "Henderson", "Reno", "North Las Vegas"],
    "AZ": ["Phoenix", "Scottsdale", "Mesa", "Tucson", "Chandler"],
    "CO": ["Denver", "Colorado Springs", "Fort Collins", "Aurora"],
    "WY": ["Cheyenne", "Casper", "Laramie"],
}

# Canonical business types → search keyword variants (directory queries, not web search engines).
BUSINESS_TYPE_KEYWORDS: list[tuple[str, list[str]]] = [
    ("MedSpa", ["med spa", "medical spa", "aesthetic clinic", "botox", "laser spa"]),
    ("Remodelers", ["home remodeler", "kitchen remodel", "bathroom remodel", "home renovation"]),
    ("Window Installers", ["window installation", "replacement windows", "window contractor"]),
    ("Luxury Pool Builders", ["luxury pool builder", "custom pool builder", "inground pool contractor"]),
    ("Exotic Car Rentals", ["exotic car rental", "luxury car rental", "supercar rental"]),
]

CATEGORY_SYNONYMS: dict[str, list[str]] = {
    "medspa": ["med spa", "aesthetic clinic", "botox clinic"],
    "remodeler": ["home remodeler", "kitchen remodel", "bath remodel contractor"],
    "window installer": ["window installation", "replacement windows", "window contractor"],
    "luxury pool builder": ["custom pool builder", "luxury swimming pools", "backyard pool design"],
    "exotic car rental": ["exotic car rental", "luxury car rental", "supercar rental"],
}


def _normalize_state(state: str) -> str:
    token = (state or "").strip().upper()
    if len(token) == 2:
        return token
    mapping = {
        "UTAH": "UT",
        "IDAHO": "ID",
        "NEVADA": "NV",
        "ARIZONA": "AZ",
        "COLORADO": "CO",
        "WYOMING": "WY",
    }
    return mapping.get(token, token[:2])


def _keyword_variants_for_category(category: str) -> list[str]:
    """Return [display category] + synonym variants for structured directory queries."""
    raw = (category or "").strip()
    if not raw:
        return []
    lower = raw.lower()
    for canonical, variants in BUSINESS_TYPE_KEYWORDS:
        if canonical.lower() == lower or canonical.lower() in lower or lower in canonical.lower():
            merged = [raw, canonical] + variants
            return list(dict.fromkeys(m.strip() for m in merged if m.strip()))

    key = lower.replace(" ", "")
    for legacy_key, variants in CATEGORY_SYNONYMS.items():
        if legacy_key in key or key in legacy_key:
            return list(dict.fromkeys([raw] + variants))
    return [raw]


def _llm_query_expansion(categories: list[str], states: list[str], model_name: str | None = None) -> list[dict[str, str]]:
    prompt = (
        "Generate up to 30 focused local-business directory-style search phrases for lead discovery. "
        "Return strict JSON with key `queries` where each item has category, city, state, keyword_variant, phrase. "
        "keyword_variant is the short term used with Yelp/Yellow Pages (e.g. 'med spa'). "
        "Use realistic city/state pairs in Utah and nearby states.\n"
        f"categories={json.dumps(categories)}\n"
        f"states={json.dumps(states)}"
    )
    result = generate_json(prompt=prompt, temperature=0.2, retries=1, model=model_name or settings.default_query_generation_model)
    if not result.ok or not isinstance(result.data.get("queries"), list):
        return []
    rows: list[dict[str, str]] = []
    for row in result.data["queries"]:
        if not isinstance(row, dict):
            continue
        category = str(row.get("category", "")).strip()
        city = str(row.get("city", "")).strip()
        state = _normalize_state(str(row.get("state", "")).strip())
        phrase = str(row.get("phrase", "")).strip()
        kw = str(row.get("keyword_variant", "")).strip() or phrase
        if category and city and state and phrase:
            rows.append({"category": category, "city": city, "state": state, "phrase": phrase, "keyword_variant": kw})
    return rows


def generate_discovery_queries(
    categories: Iterable[str],
    locations: Iterable[str],
    *,
    use_llm: bool = True,
    model_name: str | None = None,
) -> list[DiscoveryQuery]:
    clean_categories = [c.strip() for c in categories if c and c.strip()]
    states = [_normalize_state(loc) for loc in locations if loc and loc.strip()]
    if not states:
        states = ["UT", "ID", "NV", "AZ", "CO", "WY"]

    structured: list[DiscoveryQuery] = []
    for category, state in product(clean_categories, states):
        cities = NEARBY_STATE_CITIES.get(state, [])
        terms = _keyword_variants_for_category(category)
        for city, term in product(cities, terms):
            structured.append(
                DiscoveryQuery(
                    query=f"{term} in {city}, {state}",
                    category=category,
                    keyword_variant=term,
                    city=city,
                    state=state,
                )
            )

    if use_llm and clean_categories:
        llm_rows = _llm_query_expansion(clean_categories, states, model_name=model_name)
        for row in llm_rows:
            kw = row.get("keyword_variant") or row["phrase"]
            structured.append(
                DiscoveryQuery(
                    query=f"{row['phrase']} in {row['city']}, {row['state']}",
                    category=row["category"],
                    keyword_variant=kw,
                    city=row["city"],
                    state=row["state"],
                )
            )

    deduped: list[DiscoveryQuery] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    for q in structured:
        key = (q.query.lower(), q.category.lower(), q.keyword_variant.lower(), q.city.lower(), q.state.lower())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(q)
    return deduped
