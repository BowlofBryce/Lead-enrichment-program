from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from app.services.lead_row import normalize_column_name
from app.services.normalize import clean_company_name, normalize_domain, normalize_phone, normalize_url
from app.services.ollama_client import generate_json
from app.settings import settings

SEMANTIC_ROLES = [
    "primary_entity_name",
    "alternate_entity_name",
    "person_name",
    "first_name",
    "last_name",
    "full_name",
    "organization_name",
    "location_city",
    "location_state",
    "location_region",
    "locality",
    "street_address",
    "postal_code",
    "contact_phone",
    "contact_email",
    "website",
    "domain",
    "title",
    "industry_hint",
    "notes_context",
    "secondary_context",
]

TRANSFORMS = {
    "split_full_name",
    "split_city_state",
    "split_location_text",
    "normalize_phone",
    "derive_domain_from_email",
    "derive_website_from_domain",
    "clean_entity_name",
    "clean_company_name",
    "normalize_email",
    "keep_as_is",
}

HEADER_ROLE_HINTS = {
    "primary_entity_name": ["company", "business", "studio", "account", "org", "organization", "shop", "name"],
    "alternate_entity_name": ["alt", "aka", "dba"],
    "full_name": ["full_name", "full name", "contact name", "owner name"],
    "first_name": ["first", "fname"],
    "last_name": ["last", "lname", "surname"],
    "location_city": ["city", "town", "municipality"],
    "location_state": ["state", "province", "region"],
    "street_address": ["address", "street", "addr", "suite"],
    "postal_code": ["zip", "postal"],
    "contact_phone": ["phone", "mobile", "tel"],
    "contact_email": ["email", "mail"],
    "website": ["website", "site", "url"],
    "domain": ["domain"],
    "title": ["title", "role", "position"],
    "industry_hint": ["industry", "category", "specialty", "service"],
    "notes_context": ["notes", "description", "about", "context"],
    "location_region": ["county", "district"],
    "locality": ["location"],
}


@dataclass
class SchemaInferenceResult:
    plan_json: dict[str, Any]
    model_used: str
    source: str


def _heuristic_role_for_header(header: str) -> str:
    n = normalize_column_name(header)
    for role, hints in HEADER_ROLE_HINTS.items():
        if any(h in n for h in hints):
            return role
    return "secondary_context"


def _default_transforms(role: str, header: str) -> list[str]:
    n = normalize_column_name(header)
    transforms = ["keep_as_is"]
    if role == "full_name":
        transforms.append("split_full_name")
    if role in {"locality", "location_city", "location_state"} and ("location" in n or "city" in n):
        transforms.append("split_city_state")
    if role == "locality":
        transforms.append("split_location_text")
    if role == "contact_phone":
        transforms.append("normalize_phone")
    if role == "contact_email":
        transforms.extend(["normalize_email", "derive_domain_from_email"])
    if role == "domain":
        transforms.append("derive_website_from_domain")
    if role in {"primary_entity_name", "organization_name", "alternate_entity_name"}:
        transforms.append("clean_company_name")
    return list(dict.fromkeys(transforms))


def _heuristic_plan(headers: list[str], normalized_headers: list[str], sample_rows: list[dict[str, Any]], custom_instructions: str) -> dict[str, Any]:
    semantic_column_roles: dict[str, dict[str, Any]] = {}
    for header in headers:
        role = _heuristic_role_for_header(header)
        semantic_column_roles[header] = {
            "role": role,
            "confidence": 0.63,
            "transforms": _default_transforms(role, header),
        }

    canonical_field_plan = {
        "company_name": ["primary_entity_name", "organization_name", "alternate_entity_name"],
        "full_name": ["full_name", "person_name"],
        "first_name": ["first_name"],
        "last_name": ["last_name"],
        "title": ["title"],
        "email": ["contact_email"],
        "phone": ["contact_phone"],
        "website": ["website", "domain"],
        "company_domain": ["domain", "contact_email"],
        "city": ["location_city", "locality"],
        "state": ["location_state", "location_region"],
        "location_text": ["locality", "secondary_context"],
        "address": ["street_address"],
        "industry_hint": ["industry_hint", "notes_context"],
        "notes_context": ["notes_context", "secondary_context"],
    }

    return {
        "semantic_column_roles": semantic_column_roles,
        "canonical_field_plan": canonical_field_plan,
        "transformation_plan": [
            {
                "column": col,
                "role": payload["role"],
                "transforms": payload["transforms"],
            }
            for col, payload in semantic_column_roles.items()
        ],
        "confidence_scores": {k: v["confidence"] for k, v in semantic_column_roles.items()},
        "inferred_dataset_type": "local_business_leads",
        "inferred_search_hint_terms": ["official site", "business", "local"],
        "preferred_query_templates": [
            '"{primary_entity_name}" {location_city} {location_state}',
            '"{primary_entity_name}" official site',
            '"{primary_entity_name}" "{street_address}"',
        ],
        "forbidden_query_patterns": ["city_state_only", "location_only", "tourism_lookup"],
        "preferred_candidate_signals": [
            "company_name_similarity",
            "domain_similarity",
            "phone_match",
            "address_match",
            "city_state_match",
        ],
        "rejected_candidate_domains": [
            "wikipedia.org",
            "tripadvisor.com",
            "mapquest.com",
            "zillow.com",
            "homes.com",
            "niche.com",
            "city-data.com",
        ],
        "search_strategy_json": {
            "dataset_type": "local_business_leads",
            "primary_entity_type": "business",
            "search_hint_terms": ["official site", "business"],
            "preferred_query_templates": [
                '"{primary_entity_name}" {location_city} {location_state}',
                '"{primary_entity_name}" official site',
            ],
            "forbidden_query_patterns": ["location_only", "city_state_only"],
            "preferred_candidate_signals": ["name", "phone", "address", "location"],
            "rejected_candidate_domains": ["tripadvisor.com", "mapquest.com", "wikipedia.org", "city-data.com"],
            "anchor_priority_hints": ["website", "domain", "email_domain", "name_plus_location"],
            "advisory_custom_instructions": custom_instructions[:240],
        },
        "notes": "Fallback heuristic schema plan used when strong model output is unavailable.",
    }


def _build_prompt(headers: list[str], normalized_headers: list[str], sample_rows: list[dict[str, Any]], custom_instructions: str) -> str:
    return (
        "Infer CSV semantic schema and run-level search strategy. Return strict JSON only. "
        "Use only listed semantic roles and transformations.\n"
        f"Semantic roles: {json.dumps(SEMANTIC_ROLES)}\n"
        f"Allowed transformations: {json.dumps(sorted(TRANSFORMS))}\n"
        "Output keys required: semantic_column_roles, canonical_field_plan, transformation_plan, confidence_scores, "
        "inferred_dataset_type, inferred_search_hint_terms, preferred_query_templates, forbidden_query_patterns, "
        "preferred_candidate_signals, rejected_candidate_domains, search_strategy_json, notes.\n"
        f"original_headers: {json.dumps(headers)}\n"
        f"normalized_headers: {json.dumps(normalized_headers)}\n"
        f"sample_rows: {json.dumps(sample_rows[:40])}\n"
        f"custom_run_instructions: {custom_instructions or ''}\n"
        "Rules: never allow location-only query patterns. Search queries must include entity name."
    )


def _validate_plan(plan: dict[str, Any], headers: list[str]) -> dict[str, Any]:
    clean = dict(plan)
    sc = clean.get("semantic_column_roles")
    if not isinstance(sc, dict):
        raise ValueError("semantic_column_roles missing")

    fixed_roles: dict[str, dict[str, Any]] = {}
    for header in headers:
        payload = sc.get(header) or {}
        role = payload.get("role") if isinstance(payload, dict) else ""
        role = role if role in SEMANTIC_ROLES else _heuristic_role_for_header(header)
        transforms = payload.get("transforms") if isinstance(payload, dict) else []
        if not isinstance(transforms, list):
            transforms = []
        valid_transforms = [t for t in transforms if t in TRANSFORMS]
        if not valid_transforms:
            valid_transforms = _default_transforms(role, header)
        conf = payload.get("confidence") if isinstance(payload, dict) else 0.6
        try:
            conf_num = float(conf)
        except Exception:
            conf_num = 0.6
        fixed_roles[header] = {"role": role, "confidence": max(0.0, min(1.0, conf_num)), "transforms": valid_transforms}
    clean["semantic_column_roles"] = fixed_roles
    clean.setdefault("canonical_field_plan", {})
    clean.setdefault("transformation_plan", [])
    clean.setdefault("search_strategy_json", {})
    return clean


def infer_schema_plan(
    headers: list[str],
    normalized_headers: list[str],
    sample_rows: list[dict[str, Any]],
    custom_instructions: str | None = None,
    model_name: str | None = None,
) -> SchemaInferenceResult:
    chosen_model = (model_name or settings.default_schema_inference_model or settings.ollama_model).strip()
    prompt = _build_prompt(headers, normalized_headers, sample_rows, custom_instructions or "")
    fallback = _heuristic_plan(headers, normalized_headers, sample_rows, custom_instructions or "")
    llm = generate_json(prompt=prompt, retries=1, temperature=0, model=chosen_model, stage="schema_inference")
    if not llm.ok:
        return SchemaInferenceResult(plan_json=fallback, model_used=chosen_model, source="heuristic_fallback")
    try:
        validated = _validate_plan(llm.data, headers)
        for key, value in fallback.items():
            validated.setdefault(key, value)
        return SchemaInferenceResult(plan_json=validated, model_used=chosen_model, source="strong_model")
    except Exception:
        return SchemaInferenceResult(plan_json=fallback, model_used=chosen_model, source="heuristic_fallback")


def _split_full_name(value: str) -> tuple[str, str]:
    parts = [p for p in (value or "").split() if p]
    if len(parts) >= 2:
        return parts[0], " ".join(parts[1:])
    return (parts[0], "") if parts else ("", "")


def _split_city_state(value: str) -> tuple[str, str]:
    parts = [p.strip() for p in re.split(r",|\|", value or "") if p.strip()]
    if len(parts) >= 2:
        return parts[0], parts[1]
    raw = (value or "").strip()
    m = re.match(r"^(.+?)\s+([A-Z]{2})$", raw)
    if m:
        return m.group(1), m.group(2)
    return raw, ""


def transform_row_with_plan(raw_row: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
    semantic_map = plan.get("semantic_column_roles", {}) if isinstance(plan, dict) else {}
    semantic_values: dict[str, str] = {}
    canonical = {
        "first_name": "",
        "last_name": "",
        "full_name": "",
        "title": "",
        "company_name": "",
        "email": "",
        "phone": "",
        "website": "",
        "company_domain": "",
        "linkedin_url": "",
        "city": "",
        "state": "",
        "location_text": "",
        "address": "",
        "industry_hint": "",
        "notes_context": "",
        "alternate_entity_name": "",
        "postal_code": "",
    }

    for header, payload in semantic_map.items():
        role = payload.get("role", "secondary_context")
        transforms = payload.get("transforms", ["keep_as_is"])
        value = str(raw_row.get(header, "") or "").strip()
        semantic_values[role] = semantic_values.get(role, value) or value

        if "normalize_email" in transforms and value:
            value = value.lower().strip()
        if "normalize_phone" in transforms and value:
            value = normalize_phone(value)
        if role in {"primary_entity_name", "organization_name", "alternate_entity_name"} and "clean_company_name" in transforms:
            value = clean_company_name(value)

        if role in {"primary_entity_name", "organization_name"} and not canonical["company_name"]:
            canonical["company_name"] = value
        if role == "alternate_entity_name":
            canonical["alternate_entity_name"] = value
        if role in {"contact_email"}:
            canonical["email"] = canonical["email"] or value
            if "derive_domain_from_email" in transforms and "@" in value and not canonical["company_domain"]:
                canonical["company_domain"] = normalize_domain(value.split("@", 1)[1])
        if role in {"domain"}:
            canonical["company_domain"] = canonical["company_domain"] or normalize_domain(value)
            if "derive_website_from_domain" in transforms and not canonical["website"]:
                canonical["website"] = normalize_url(value)
        if role == "website":
            canonical["website"] = canonical["website"] or normalize_url(value)
            if canonical["website"] and not canonical["company_domain"]:
                canonical["company_domain"] = normalize_domain(canonical["website"])
        if role == "contact_phone":
            canonical["phone"] = canonical["phone"] or value
        if role in {"location_city"}:
            canonical["city"] = canonical["city"] or value
        if role in {"location_state", "location_region"}:
            canonical["state"] = canonical["state"] or value
        if role in {"street_address"}:
            canonical["address"] = canonical["address"] or value
        if role in {"postal_code"}:
            canonical["postal_code"] = canonical["postal_code"] or value
        if role in {"title"}:
            canonical["title"] = canonical["title"] or value
        if role in {"industry_hint"}:
            canonical["industry_hint"] = canonical["industry_hint"] or value
        if role in {"notes_context", "secondary_context"}:
            canonical["notes_context"] = canonical["notes_context"] or value
        if role in {"full_name", "person_name"}:
            canonical["full_name"] = canonical["full_name"] or value
            if "split_full_name" in transforms:
                fn, ln = _split_full_name(value)
                canonical["first_name"] = canonical["first_name"] or fn
                canonical["last_name"] = canonical["last_name"] or ln
        if role == "first_name":
            canonical["first_name"] = canonical["first_name"] or value
        if role == "last_name":
            canonical["last_name"] = canonical["last_name"] or value
        if role in {"locality"} and ("split_city_state" in transforms or "split_location_text" in transforms):
            c, s = _split_city_state(value)
            canonical["city"] = canonical["city"] or c
            canonical["state"] = canonical["state"] or s

    if not canonical["full_name"] and (canonical["first_name"] or canonical["last_name"]):
        canonical["full_name"] = f"{canonical['first_name']} {canonical['last_name']}".strip()
    if not canonical["location_text"]:
        canonical["location_text"] = " ".join([p for p in [canonical["city"], canonical["state"]] if p]).strip()

    return {"canonical": canonical, "semantic_values": semantic_values}
