from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

from app.services.ollama_client import generate_json
from app.services.normalize import clean_company_name, normalize_domain, normalize_phone, normalize_url

CANONICAL_COLUMNS = [
    "first_name",
    "last_name",
    "full_name",
    "title",
    "company_name",
    "email",
    "phone",
    "website",
    "company_domain",
    "linkedin_url",
    "city",
    "state",
    "location_text",
    "address",
]

CANONICAL_ALIASES: dict[str, list[str]] = {
    "first_name": ["first_name", "firstname", "first"],
    "last_name": ["last_name", "lastname", "last"],
    "full_name": ["full_name", "full name", "name"],
    "company_name": ["company", "company_name", "organization", "account_name"],
    "title": ["title", "job_title", "position"],
    "email": ["email", "work_email"],
    "phone": ["phone", "mobile", "work_phone"],
    "website": ["website", "company_website", "url"],
    "company_domain": ["domain", "company_domain"],
    "linkedin_url": ["linkedin", "linkedin_url", "linkedin profile"],
    "city": ["city"],
    "state": ["state"],
    "location_text": ["location"],
    "address": ["address", "street", "street_address", "address1"],
}

GENERIC_EMAIL_PREFIXES = {"info", "hello", "sales", "support", "contact", "admin"}


@dataclass
class CanonicalLeadRow:
    first_name: str = ""
    last_name: str = ""
    full_name: str = ""
    normalized_full_name: str = ""
    title: str = ""
    normalized_title: str = ""
    company_name: str = ""
    normalized_company_name: str = ""
    email: str = ""
    normalized_email: str = ""
    email_domain: str = ""
    phone: str = ""
    normalized_phone: str = ""
    company_domain: str = ""
    website: str = ""
    linkedin_url: str = ""
    city: str = ""
    state: str = ""
    location_text: str = ""
    address: str = ""
    industry_hint: str = ""
    notes_context: str = ""
    alternate_entity_name: str = ""
    postal_code: str = ""

    def as_dict(self) -> dict[str, str]:
        return self.__dict__.copy()


@dataclass
class RowAnalysis:
    fields_present: list[str] = field(default_factory=list)
    fields_missing: list[str] = field(default_factory=list)
    fields_suspicious: list[str] = field(default_factory=list)
    validation_notes: list[str] = field(default_factory=list)


@dataclass
class AnchorResolution:
    anchor_type: str
    anchor_value: str
    reason: str


def normalize_column_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def alias_lookup() -> dict[str, str]:
    out: dict[str, str] = {}
    for canonical, aliases in CANONICAL_ALIASES.items():
        for alias in aliases:
            out[normalize_column_name(alias)] = canonical
    return out


def _pick_heuristic_mapping(headers: list[str]) -> tuple[dict[str, str], list[str], list[str]]:
    normalized_to_original: dict[str, str] = {}
    for header in headers:
        normalized_to_original[normalize_column_name(header)] = header

    lookup = alias_lookup()
    mapping: dict[str, str] = {k: "" for k in CANONICAL_COLUMNS}
    warnings: list[str] = []

    for canonical in CANONICAL_COLUMNS:
        candidates = []
        for alias in CANONICAL_ALIASES[canonical]:
            key = normalize_column_name(alias)
            if key in normalized_to_original:
                candidates.append(normalized_to_original[key])
        if candidates:
            mapping[canonical] = candidates[0]
            if len(candidates) > 1:
                warnings.append(f"Ambiguous mapping for {canonical}: {candidates}. Using {candidates[0]}")

    normalized_headers = [lookup.get(normalize_column_name(h), normalize_column_name(h)) for h in headers]
    return mapping, normalized_headers, warnings


def _llm_mapping_prompt(headers: list[str]) -> str:
    alias_examples = {k: v for k, v in CANONICAL_ALIASES.items()}
    return (
        "Map CSV headers to canonical lead fields.\n"
        "Return JSON object with key 'mapping' whose value is an object from canonical field to exact CSV header.\n"
        "Rules:\n"
        "- Use only headers from the given list exactly as written.\n"
        "- Use empty string for unmapped fields.\n"
        "- Prefer semantic meaning over literal string match.\n"
        "- Do not invent keys beyond provided canonical fields.\n\n"
        f"Canonical fields: {json.dumps(CANONICAL_COLUMNS)}\n"
        f"Known alias hints: {json.dumps(alias_examples)}\n"
        f"CSV headers: {json.dumps(headers)}\n"
    )


def _coerce_llm_mapping(headers: list[str], raw: dict[str, Any]) -> dict[str, str]:
    header_set = set(headers)
    out = {k: "" for k in CANONICAL_COLUMNS}
    raw_mapping = raw.get("mapping")
    if not isinstance(raw_mapping, dict):
        return out

    for canonical in CANONICAL_COLUMNS:
        picked = raw_mapping.get(canonical, "")
        picked_str = str(picked).strip() if picked is not None else ""
        out[canonical] = picked_str if picked_str in header_set else ""
    return out


def pick_canonical_mapping(headers: list[str]) -> tuple[dict[str, str], list[str], list[str]]:
    heuristic_mapping, normalized_headers, warnings = _pick_heuristic_mapping(headers)
    prompt = _llm_mapping_prompt(headers)
    result = generate_json(prompt=prompt, retries=1, temperature=0, stage="lead_row_enrichment")
    if not result.ok:
        warnings.append(f"LLM mapping unavailable; using heuristic mapping ({result.error or 'unknown_error'}).")
        return heuristic_mapping, normalized_headers, warnings

    llm_mapping = _coerce_llm_mapping(headers, result.data)
    if not any(llm_mapping.values()):
        warnings.append("LLM mapping returned no usable fields; using heuristic mapping.")
        return heuristic_mapping, normalized_headers, warnings

    for canonical, source in heuristic_mapping.items():
        if source and not llm_mapping.get(canonical):
            llm_mapping[canonical] = source

    for canonical, source in llm_mapping.items():
        heuristic_source = heuristic_mapping.get(canonical, "")
        if source and heuristic_source and source != heuristic_source:
            warnings.append(f"LLM mapping override for {canonical}: {heuristic_source} -> {source}")
    return llm_mapping, normalized_headers, warnings


def _title_case(value: str) -> str:
    return " ".join(part.capitalize() for part in value.split())


def _split_name(full_name: str) -> tuple[str, str]:
    parts = [p for p in full_name.split() if p]
    if len(parts) >= 2:
        return parts[0], " ".join(parts[1:])
    if len(parts) == 1:
        return parts[0], ""
    return "", ""


def canonicalize_row(raw_row: dict[str, Any], mapping: dict[str, str]) -> CanonicalLeadRow:
    def getv(name: str) -> str:
        source = mapping.get(name, "")
        return str(raw_row.get(source, "") or "").strip() if source else ""

    row = CanonicalLeadRow(
        first_name=getv("first_name"),
        last_name=getv("last_name"),
        full_name=getv("full_name"),
        title=getv("title"),
        company_name=getv("company_name"),
        email=getv("email"),
        phone=getv("phone"),
        website=getv("website"),
        company_domain=getv("company_domain"),
        linkedin_url=getv("linkedin_url"),
        city=getv("city"),
        state=getv("state"),
        location_text=getv("location_text"),
        address=getv("address"),
    )

    if row.full_name and (not row.first_name or not row.last_name):
        maybe_first, maybe_last = _split_name(row.full_name)
        row.first_name = row.first_name or maybe_first
        row.last_name = row.last_name or maybe_last

    if not row.full_name and (row.first_name or row.last_name):
        row.full_name = f"{row.first_name} {row.last_name}".strip()

    row.first_name = _title_case(row.first_name)
    row.last_name = _title_case(row.last_name)
    row.normalized_full_name = _title_case(row.full_name)
    row.normalized_title = _title_case(row.title)
    row.normalized_company_name = clean_company_name(row.company_name)
    row.normalized_email = row.email.lower().strip()
    row.email_domain = row.normalized_email.split("@", 1)[1] if "@" in row.normalized_email else ""
    row.normalized_phone = normalize_phone(row.phone)
    row.website = normalize_url(row.website)
    website_domain = normalize_domain(row.website)
    row.company_domain = normalize_domain(row.company_domain or website_domain or row.email_domain)

    if not row.location_text:
        row.location_text = " ".join(part for part in [row.city, row.state] if part).strip()

    if not row.company_name and row.company_domain:
        row.company_name = row.company_domain.split(".")[0].replace("-", " ").title()
        row.normalized_company_name = clean_company_name(row.company_name)

    return row


def canonicalize_from_dynamic(dynamic: dict[str, Any]) -> CanonicalLeadRow:
    row = CanonicalLeadRow(
        first_name=str(dynamic.get("first_name", "") or "").strip(),
        last_name=str(dynamic.get("last_name", "") or "").strip(),
        full_name=str(dynamic.get("full_name", "") or "").strip(),
        title=str(dynamic.get("title", "") or "").strip(),
        company_name=str(dynamic.get("company_name", "") or "").strip(),
        email=str(dynamic.get("email", "") or "").strip(),
        phone=str(dynamic.get("phone", "") or "").strip(),
        website=str(dynamic.get("website", "") or "").strip(),
        company_domain=str(dynamic.get("company_domain", "") or "").strip(),
        linkedin_url=str(dynamic.get("linkedin_url", "") or "").strip(),
        city=str(dynamic.get("city", "") or "").strip(),
        state=str(dynamic.get("state", "") or "").strip(),
        location_text=str(dynamic.get("location_text", "") or "").strip(),
        address=str(dynamic.get("address", "") or "").strip(),
        industry_hint=str(dynamic.get("industry_hint", "") or "").strip(),
        notes_context=str(dynamic.get("notes_context", "") or "").strip(),
        alternate_entity_name=str(dynamic.get("alternate_entity_name", "") or "").strip(),
        postal_code=str(dynamic.get("postal_code", "") or "").strip(),
    )

    if row.full_name and (not row.first_name or not row.last_name):
        maybe_first, maybe_last = _split_name(row.full_name)
        row.first_name = row.first_name or maybe_first
        row.last_name = row.last_name or maybe_last
    if not row.full_name and (row.first_name or row.last_name):
        row.full_name = f"{row.first_name} {row.last_name}".strip()

    row.first_name = _title_case(row.first_name)
    row.last_name = _title_case(row.last_name)
    row.normalized_full_name = _title_case(row.full_name)
    row.normalized_title = _title_case(row.title)
    row.normalized_company_name = clean_company_name(row.company_name)
    row.normalized_email = row.email.lower().strip()
    row.email_domain = row.normalized_email.split("@", 1)[1] if "@" in row.normalized_email else ""
    row.normalized_phone = normalize_phone(row.phone)
    row.website = normalize_url(row.website)
    website_domain = normalize_domain(row.website)
    row.company_domain = normalize_domain(row.company_domain or website_domain or row.email_domain)
    if not row.location_text:
        row.location_text = " ".join(part for part in [row.city, row.state] if part).strip()
    if not row.company_name and row.company_domain:
        row.company_name = row.company_domain.split(".")[0].replace("-", " ").title()
        row.normalized_company_name = clean_company_name(row.company_name)
    return row


def analyze_row(canonical: CanonicalLeadRow) -> RowAnalysis:
    analysis = RowAnalysis()
    for field_name, field_value in canonical.as_dict().items():
        if field_name.startswith("normalized_"):
            continue
        if field_value:
            analysis.fields_present.append(field_name)
        elif field_name in {"title", "linkedin_url", "company_domain", "website"}:
            analysis.fields_missing.append(field_name)

    if canonical.email and canonical.email_domain and canonical.company_domain and canonical.email_domain != canonical.company_domain:
        analysis.fields_suspicious.append("email_domain_company_domain_mismatch")
        analysis.validation_notes.append("Email domain mismatches company domain")

    if canonical.website and canonical.company_domain and normalize_domain(canonical.website) != canonical.company_domain:
        analysis.fields_suspicious.append("website_company_domain_mismatch")

    if canonical.email and canonical.email.split("@", 1)[0].lower() in GENERIC_EMAIL_PREFIXES:
        analysis.fields_suspicious.append("generic_email")
        analysis.validation_notes.append("Generic email only")

    if canonical.full_name and (not canonical.first_name or not canonical.last_name):
        analysis.validation_notes.append("Full name provided but split was partial")

    return analysis


def resolve_anchor(canonical: CanonicalLeadRow) -> AnchorResolution:
    if canonical.linkedin_url:
        return AnchorResolution("linkedin_url", canonical.linkedin_url, "LinkedIn URL is strongest person/company anchor")
    if canonical.email_domain:
        return AnchorResolution("email_domain", canonical.email_domain, "Derived from direct email address")
    if canonical.website:
        return AnchorResolution("website", canonical.website, "Company website provided")
    if canonical.company_domain:
        return AnchorResolution("company_domain", canonical.company_domain, "Provided or derived company domain")
    if canonical.company_name and (canonical.city or canonical.state):
        return AnchorResolution(
            "company_name_location",
            f"{canonical.company_name} | {canonical.city} {canonical.state}".strip(),
            "Company and location combination available",
        )
    if canonical.company_name:
        return AnchorResolution("company_name", canonical.company_name, "Only company name available")
    return AnchorResolution("unresolved", "", "No usable anchor fields")


def compute_scores(
    canonical: CanonicalLeadRow,
    analysis: RowAnalysis,
    person_name_found: bool,
    company_site_found: bool,
    resolution_confidence: float = 0.0,
    resolution_status: str = "",
) -> dict[str, float | int]:
    company_conf = 0.2
    person_conf = 0.2

    if canonical.company_domain:
        company_conf += 0.35
    if canonical.website:
        company_conf += 0.2
    if canonical.company_name:
        company_conf += 0.15
    if company_site_found:
        company_conf += 0.1
    if resolution_status == "resolved":
        company_conf += min(0.22, max(0.0, resolution_confidence) * 0.22)
    elif resolution_status in {"ambiguous", "unresolved"}:
        company_conf -= 0.08

    if canonical.full_name:
        person_conf += 0.25
    if canonical.title:
        person_conf += 0.15
    if canonical.linkedin_url:
        person_conf += 0.2
    if person_name_found:
        person_conf += 0.2

    if "generic_email" in analysis.fields_suspicious:
        person_conf -= 0.15
    if "email_domain_company_domain_mismatch" in analysis.fields_suspicious:
        company_conf -= 0.15

    company_conf = round(max(0.0, min(1.0, company_conf)), 3)
    person_conf = round(max(0.0, min(1.0, person_conf)), 3)

    enrichment_confidence = round((company_conf * 0.6 + person_conf * 0.4), 3)
    lead_quality_score = int(max(0, min(100, enrichment_confidence * 100 - len(analysis.fields_missing) * 3)))
    return {
        "company_match_confidence": company_conf,
        "person_match_confidence": person_conf,
        "enrichment_confidence": enrichment_confidence,
        "lead_quality_score": lead_quality_score,
    }


def to_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)
