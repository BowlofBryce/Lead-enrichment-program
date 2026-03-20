from __future__ import annotations

import json
import re
from typing import Any

from app.services.ollama_client import generate_json
from app.settings import settings


def _basic_queries(canonical: dict[str, Any], strategy: dict[str, Any], custom_instructions: str | None = None) -> list[str]:
    name = (canonical.get("company_name") or "").strip()
    city = (canonical.get("city") or "").strip()
    state = (canonical.get("state") or "").strip()
    phone = (canonical.get("phone") or "").strip()
    address = (canonical.get("address") or "").strip()
    hints = strategy.get("search_hint_terms", []) if isinstance(strategy, dict) else []
    if isinstance(hints, str):
        hints = [hints]

    queries: list[str] = []
    if name and city and state:
        queries.append(f'"{name}" {city} {state}')
    if name and address:
        queries.append(f'"{name}" "{address}"')
    if name and phone:
        queries.append(f'"{name}" "{phone}"')
    if name:
        queries.append(f'"{name}" official site')
    if name:
        for hint in hints[:2]:
            hint_clean = str(hint).strip()
            if hint_clean:
                queries.append(f'"{name}" {hint_clean}')
    if name and custom_instructions:
        for token in re.split(r"[,;\n]", custom_instructions)[:2]:
            token = re.sub(r"[^a-zA-Z0-9\s-]", " ", token).strip()
            if 2 <= len(token) <= 40:
                queries.append(f'"{name}" {token}')
    return queries


def _filter_queries(queries: list[str], canonical: dict[str, Any], strategy: dict[str, Any]) -> tuple[list[str], list[dict[str, str]]]:
    name = (canonical.get("company_name") or "").strip().lower()
    city = (canonical.get("city") or "").strip().lower()
    state = (canonical.get("state") or "").strip().lower()
    forbidden = set((strategy.get("forbidden_query_patterns") or [])) if isinstance(strategy, dict) else set()

    seen: set[str] = set()
    accepted: list[str] = []
    notes: list[dict[str, str]] = []
    for q in queries:
        cleaned = " ".join(str(q).split())
        lq = cleaned.lower()
        if not cleaned:
            notes.append({"query": cleaned, "status": "rejected", "reason": "empty"})
            continue
        if name and name not in lq:
            notes.append({"query": cleaned, "status": "rejected", "reason": "missing_entity_name"})
            continue
        if city and state and lq in {f"{city} {state}", f"{city}, {state}"}:
            notes.append({"query": cleaned, "status": "rejected", "reason": "location_only"})
            continue
        if "location_only" in forbidden and (lq == city or lq == state):
            notes.append({"query": cleaned, "status": "rejected", "reason": "forbidden_by_strategy"})
            continue
        if lq in seen:
            notes.append({"query": cleaned, "status": "rejected", "reason": "duplicate"})
            continue
        seen.add(lq)
        accepted.append(cleaned)
        notes.append({"query": cleaned, "status": "accepted", "reason": "valid"})
        if len(accepted) >= 5:
            break
    return accepted, notes


def generate_queries_if_needed(
    canonical: dict[str, Any],
    search_strategy: dict[str, Any],
    custom_instructions: str | None = None,
    model_name: str | None = None,
) -> tuple[list[str], str, list[dict[str, str]]]:
    base_queries = _basic_queries(canonical, search_strategy, custom_instructions)
    chosen_model = (model_name or settings.default_query_generation_model or settings.ollama_model).strip()

    llm_queries: list[str] = []
    if len(base_queries) < 3 and (canonical.get("company_name") or "").strip():
        prompt = (
            "Generate 2-5 conservative web search queries to find the official website for one business. "
            "Return strict JSON with keys: queries, reasoning_summary, confidence. "
            "Every query must include the business name. Never return location-only queries.\n"
            f"canonical_row: {json.dumps(canonical)}\n"
            f"search_strategy: {json.dumps(search_strategy)}\n"
            f"custom_instructions: {custom_instructions or ''}\n"
        )
        llm = generate_json(prompt=prompt, retries=1, temperature=0.1, model=chosen_model)
        if llm.ok and isinstance(llm.data.get("queries"), list):
            llm_queries = [str(q) for q in llm.data.get("queries", [])]

    combined = base_queries + llm_queries
    accepted, notes = _filter_queries(combined, canonical, search_strategy)
    reason = f"deterministic={len(base_queries)} llm={len(llm_queries)} accepted={len(accepted)}"
    return accepted, reason, notes
