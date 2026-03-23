from __future__ import annotations

import json
from itertools import product
from typing import Iterable

from app.services.lead_discovery.types import DiscoveryQuery
from app.services.ollama_client import generate_json
from app.settings import settings


NEARBY_STATE_CITIES: dict[str, list[str]] = {
    "UT": ["Salt Lake City", "Provo", "Ogden", "St George", "Lehi", "Orem"],
    "ID": ["Boise", "Idaho Falls", "Nampa"],
    "NV": ["Las Vegas", "Henderson", "Reno"],
    "AZ": ["Phoenix", "Scottsdale", "Mesa"],
    "CO": ["Denver", "Colorado Springs", "Fort Collins"],
    "WY": ["Cheyenne", "Casper", "Laramie"],
}

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


def _expand_keywords(category: str) -> list[str]:
    key = (category or "").strip().lower()
    for canonical, variants in CATEGORY_SYNONYMS.items():
        if canonical in key or key in canonical:
            return [category] + variants
    return [category]


def _llm_query_expansion(categories: list[str], states: list[str], model_name: str | None = None) -> list[dict[str, str]]:
    prompt = (
        "Generate up to 30 focused local-business search intents for lead discovery. "
        "Return strict JSON with key `queries` where each item has category, city, state, phrase. "
        "Use realistic city/state pairs.\n"
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
        if category and city and state and phrase:
            rows.append({"category": category, "city": city, "state": state, "phrase": phrase})
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
        terms = _expand_keywords(category)
        for city, term in product(cities, terms):
            structured.append(
                DiscoveryQuery(
                    query=f"{term} in {city}, {state}",
                    category=category,
                    city=city,
                    state=state,
                )
            )

    if use_llm and clean_categories:
        llm_rows = _llm_query_expansion(clean_categories, states, model_name=model_name)
        for row in llm_rows:
            structured.append(
                DiscoveryQuery(
                    query=f"{row['phrase']} in {row['city']}, {row['state']}",
                    category=row["category"],
                    city=row["city"],
                    state=row["state"],
                )
            )

    deduped: list[DiscoveryQuery] = []
    seen: set[tuple[str, str, str, str]] = set()
    for q in structured:
        key = (q.query.lower(), q.category.lower(), q.city.lower(), q.state.lower())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(q)
    return deduped
