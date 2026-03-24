from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from difflib import SequenceMatcher
from typing import Any

import requests
from bs4 import BeautifulSoup

from app.services.brave_search import BraveSearchClient, BraveSearchError
from app.services.crawl import _fetch_with_playwright
from app.services.query_generation import generate_queries_if_needed
from app.services.lead_row import CanonicalLeadRow
from app.services.normalize import clean_company_name, normalize_domain, normalize_phone, normalize_url
from app.settings import settings

BLOCKED_FINAL_DOMAINS = {
    "yelp.com",
    "facebook.com",
    "instagram.com",
    "yellowpages.com",
    "mapquest.com",
    "tripadvisor.com",
    "linkedin.com",
    "youtube.com",
    "x.com",
    "twitter.com",
    "tiktok.com",
    "nextdoor.com",
    "wikipedia.org",
    "zillow.com",
    "homes.com",
    "niche.com",
    "city-data.com",
}
SUSPICIOUS_ANCHOR_DOMAINS = {"example.com", "test.com", "localhost"}

GENERIC_EMAIL_DOMAINS = {
    "gmail.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
    "icloud.com",
    "aol.com",
    "protonmail.com",
}

BUSINESS_HINT_KEYWORDS = {"tattoo", "studio", "ink", "salon", "spa", "barber", "clinic", "repair", "plumbing", "electric"}


@dataclass
class ResolutionCandidate:
    website: str
    domain: str
    source: str
    evidence: dict[str, Any] = field(default_factory=dict)
    score: float = 0.0
    confidence: float = 0.0
    accepted: bool = False
    rejection_reason: str = ""


@dataclass
class ResolutionResult:
    resolved_website: str = ""
    resolved_domain: str = ""
    resolution_method: str = ""
    resolution_confidence: float = 0.0
    resolution_notes: str = ""
    candidate_websites_json: str = "[]"
    resolution_status: str = "unresolved"
    used_existing_anchor: bool = False
    search_queries: list[str] = field(default_factory=list)
    trace: list[dict[str, Any]] = field(default_factory=list)


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio() if a and b else 0.0


def _domain_brand(domain: str) -> str:
    host = domain.lower().replace("www.", "")
    brand = host.split(".")[0]
    return re.sub(r"[^a-z0-9]", "", brand)


def _norm_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _norm_phone_digits(value: str) -> str:
    return normalize_phone(value or "").replace("+", "")


def _instruction_hints(custom_instructions: str | None) -> list[str]:
    if not custom_instructions:
        return []
    hints: list[str] = []
    for token in re.split(r"[,\n;]", custom_instructions.lower()):
        cleaned = re.sub(r"[^a-z0-9\s-]", " ", token).strip()
        if 2 <= len(cleaned) <= 40:
            hints.append(cleaned)
    unique: list[str] = []
    seen: set[str] = set()
    for hint in hints:
        if hint not in seen:
            unique.append(hint)
            seen.add(hint)
    return unique[:4]


def _search_queries(
    canonical: CanonicalLeadRow,
    custom_instructions: str | None = None,
    search_strategy: dict[str, Any] | None = None,
    query_generation_model: str | None = None,
) -> tuple[list[str], str, list[dict[str, str]]]:
    canonical_dict = canonical.as_dict()
    canonical_dict["company_name"] = canonical.company_name or canonical.normalized_company_name
    strategy = search_strategy or {}
    if canonical.industry_hint and "search_hint_terms" in strategy and isinstance(strategy["search_hint_terms"], list):
        strategy["search_hint_terms"] = list(dict.fromkeys(strategy["search_hint_terms"] + [canonical.industry_hint]))
    return generate_queries_if_needed(
        canonical=canonical_dict,
        search_strategy=strategy,
        custom_instructions=custom_instructions,
        model_name=query_generation_model,
    )


def _is_location_only_query(query: str) -> bool:
    tokens = re.findall(r"[a-z0-9]+", (query or "").lower())
    return len(tokens) <= 3 and not any(t in {"official", "site"} for t in tokens)


def _legacy_search_queries(canonical: CanonicalLeadRow, custom_instructions: str | None = None) -> list[str]:
    queries: list[str] = []
    base = " ".join(part for part in [canonical.company_name, canonical.city, canonical.state] if part).strip()
    if base:
        queries.append(base)
    if canonical.company_name and canonical.address:
        queries.append(f"{canonical.company_name} {canonical.address}")
    if canonical.company_name:
        queries.append(f'"{canonical.company_name}" {canonical.city} {canonical.state}'.strip())
        queries.append(canonical.company_name)
    for hint in _instruction_hints(custom_instructions):
        if canonical.company_name:
            queries.append(f"{canonical.company_name} {hint}")
    seen: set[str] = set()
    deduped: list[str] = []
    for q in queries:
        cleaned = " ".join(q.split())
        if cleaned and cleaned.lower() not in seen:
            seen.add(cleaned.lower())
            deduped.append(cleaned)
    return deduped[:3]


def _has_suspicious_existing_anchor(canonical: CanonicalLeadRow) -> bool:
    existing_domain = normalize_domain(canonical.website or canonical.company_domain)
    if not existing_domain:
        return False
    if existing_domain in SUSPICIOUS_ANCHOR_DOMAINS:
        return True
    if canonical.company_name and existing_domain.startswith("www"):
        cleaned = re.sub(r"[^a-z0-9]", "", clean_company_name(canonical.company_name).lower())
        domain_brand = _domain_brand(existing_domain)
        if cleaned and _similarity(cleaned, domain_brand) < 0.25:
            return True
    return False


def search_company_candidates(
    canonical: CanonicalLeadRow,
    max_results: int = 8,
    custom_instructions: str | None = None,
    search_strategy: dict[str, Any] | None = None,
    query_generation_model: str | None = None,
) -> tuple[list[ResolutionCandidate], list[str], list[dict[str, Any]], str]:
    queries, query_note, query_notes = _search_queries(
        canonical,
        custom_instructions=custom_instructions,
        search_strategy=search_strategy,
        query_generation_model=query_generation_model,
    )
    trace: list[dict[str, Any]] = []
    candidates: dict[str, ResolutionCandidate] = {}
    trace.append({"stage": "resolution.query_generation", "status": "ok", "message": query_note, "details": query_notes})

    brave = BraveSearchClient()
    for query in queries:
        ql = query.lower()
        if not canonical.company_name or canonical.company_name.lower() not in ql:
            trace.append({"stage": "resolution.search", "provider": "brave", "status": "skipped", "query": query, "reason": "missing_entity_name"})
            continue
        if _is_location_only_query(query):
            trace.append({"stage": "resolution.search", "provider": "brave", "status": "skipped", "query": query, "reason": "location_only_query"})
            continue

        trace.append({"stage": "resolution.search", "provider": "brave", "status": "start", "query": query})
        try:
            parsed = brave.search_web(query)
        except BraveSearchError as exc:
            trace.append(
                {
                    "stage": "resolution.search",
                    "provider": "brave",
                    "status": "failed",
                    "query": query,
                    "error": str(exc),
                }
            )
            continue

        trace.append(
            {
                "stage": "resolution.search",
                "provider": "brave",
                "status": "ok",
                "query": query,
                "result_count": len(parsed),
            }
        )

        for item in parsed[:max_results]:
            website = normalize_url(item.url)
            domain = normalize_domain(website)
            if not domain:
                continue
            existing = candidates.get(domain)
            if not existing:
                candidates[domain] = ResolutionCandidate(
                    website=website,
                    domain=domain,
                    source="search",
                    evidence={"search_titles": [item.title], "queries": [query], "descriptions": [item.description]},
                )
            else:
                existing.evidence.setdefault("search_titles", []).append(item.title)
                existing.evidence.setdefault("queries", []).append(query)
                existing.evidence.setdefault("descriptions", []).append(item.description)

    return list(candidates.values())[:max_results], queries, trace, query_note


def validate_candidate_website(candidate: ResolutionCandidate, canonical: CanonicalLeadRow) -> ResolutionCandidate:
    website = candidate.website
    try:
        resp = requests.get(
            website,
            timeout=settings.request_timeout_seconds,
            headers={"User-Agent": "LeadEnrichmentLocal/1.0 (+candidate-validation)"},
        )
        resp.raise_for_status()
        html = resp.text
        method = "requests"
    except Exception:
        try:
            html, _, _ = _fetch_with_playwright(website)
            method = "playwright"
        except Exception as exc:
            candidate.rejection_reason = f"fetch_failed:{exc}"
            return candidate

    soup = BeautifulSoup(html, "html.parser")
    title = (soup.title.get_text(" ", strip=True) if soup.title else "")[:180]
    h1 = (soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else "")[:180]
    body = soup.get_text(" ", strip=True)[:5000]

    company_clean = clean_company_name(canonical.company_name or "")
    body_lower = _norm_text(body)
    company_lower = _norm_text(company_clean)
    city_lower = _norm_text(canonical.city)
    state_lower = _norm_text(canonical.state)
    address_lower = _norm_text(canonical.address)
    phone_digits = _norm_phone_digits(canonical.phone)

    candidate.evidence.update(
        {
            "title": title,
            "h1": h1,
            "fetch_method": method,
            "title_name_similarity": round(_similarity(title, company_clean), 3),
            "h1_name_similarity": round(_similarity(h1, company_clean), 3),
            "domain_name_similarity": round(_similarity(_domain_brand(candidate.domain), re.sub(r"[^a-z0-9]", "", company_lower)), 3),
            "has_city": bool(city_lower and city_lower in body_lower),
            "has_state": bool(state_lower and state_lower in body_lower),
            "has_company_name": bool(company_lower and company_lower in body_lower),
            "has_address": bool(address_lower and address_lower[:16] in body_lower),
            "phone_match": bool(phone_digits and phone_digits in re.sub(r"\D", "", body)),
            "business_hint_match": bool(any(h in body_lower for h in BUSINESS_HINT_KEYWORDS)),
        }
    )
    return candidate


def score_resolution_candidate(candidate: ResolutionCandidate, canonical: CanonicalLeadRow) -> ResolutionCandidate:
    evidence = candidate.evidence
    score = 0.0

    if evidence.get("phone_match"):
        score += 0.45
    if evidence.get("has_address"):
        score += 0.35
    if evidence.get("has_company_name"):
        score += 0.2
    if evidence.get("has_city") and evidence.get("has_state"):
        score += 0.15
    elif evidence.get("has_city") or evidence.get("has_state"):
        score += 0.08

    score += min(0.12, float(evidence.get("title_name_similarity", 0.0)) * 0.12)
    score += min(0.1, float(evidence.get("h1_name_similarity", 0.0)) * 0.1)
    score += min(0.08, float(evidence.get("domain_name_similarity", 0.0)) * 0.08)

    if evidence.get("business_hint_match"):
        score += 0.03

    domain = candidate.domain.lower()
    if any(domain == blocked or domain.endswith(f".{blocked}") for blocked in BLOCKED_FINAL_DOMAINS):
        score -= 0.45
        candidate.rejection_reason = "directory_or_social_domain"

    candidate.score = round(max(0.0, min(1.0, score)), 3)
    candidate.confidence = candidate.score
    return candidate


def resolve_company_website(
    canonical: CanonicalLeadRow,
    custom_instructions: str | None = None,
    search_strategy: dict[str, Any] | None = None,
    query_generation_model: str | None = None,
) -> ResolutionResult:
    result = ResolutionResult()
    result.trace.append({"stage": "resolution.start", "status": "ok", "message": "Resolution stage started"})

    if (canonical.website or canonical.company_domain) and not _has_suspicious_existing_anchor(canonical):
        result.resolution_status = "skipped_existing_anchor"
        result.used_existing_anchor = True
        result.resolution_method = "existing_anchor"
        result.resolution_notes = "Website/domain already present; resolution skipped"
        return result
    if canonical.website or canonical.company_domain:
        result.trace.append(
            {
                "stage": "resolution.start",
                "status": "warning",
                "message": "Existing anchor looked suspicious; running resolution",
                "existing_website": canonical.website,
                "existing_domain": canonical.company_domain,
            }
        )

    if canonical.email_domain and canonical.email_domain not in GENERIC_EMAIL_DOMAINS:
        candidate = ResolutionCandidate(
            website=normalize_url(canonical.email_domain),
            domain=normalize_domain(canonical.email_domain),
            source="email_domain",
        )
        score_resolution_candidate(candidate, canonical)
        if candidate.domain:
            result.resolved_domain = candidate.domain
            result.resolved_website = normalize_url(candidate.domain)
            result.resolution_method = "email_domain"
            result.resolution_confidence = 0.72
            result.resolution_status = "resolved"
            result.resolution_notes = "Resolved from non-generic email domain"
            candidate.accepted = True
            result.candidate_websites_json = json.dumps([asdict(candidate)])
            result.trace.append({"stage": "resolution.selected", "status": "ok", "method": "email_domain", "domain": candidate.domain})
            return result

    if not (canonical.company_name or canonical.normalized_company_name):
        result.resolution_status = "unresolved"
        result.resolution_notes = "insufficient business identity for web resolution"
        result.trace.append({"stage": "resolution.unresolved", "status": "ok", "reason": result.resolution_notes})
        return result

    candidates, queries, search_trace, query_note = search_company_candidates(
        canonical,
        custom_instructions=custom_instructions,
        search_strategy=search_strategy,
        query_generation_model=query_generation_model,
    )
    result.search_queries = queries
    if query_note:
        result.resolution_notes = query_note
    result.trace.extend(search_trace)

    scored: list[ResolutionCandidate] = []
    for candidate in candidates:
        result.trace.append({"stage": "resolution.candidate_found", "status": "ok", "candidate": asdict(candidate)})
        validated = validate_candidate_website(candidate, canonical)
        scored_candidate = score_resolution_candidate(validated, canonical)
        result.trace.append(
            {
                "stage": "resolution.candidate_scored",
                "status": "ok",
                "domain": scored_candidate.domain,
                "score": scored_candidate.score,
                "rejection_reason": scored_candidate.rejection_reason,
                "evidence": scored_candidate.evidence,
            }
        )
        scored.append(scored_candidate)

    scored.sort(key=lambda c: c.score, reverse=True)
    result.candidate_websites_json = json.dumps([asdict(c) for c in scored], ensure_ascii=False)

    if not scored:
        result.resolution_status = "failed"
        result.resolution_notes = "No candidate URLs found from search strategies"
        result.trace.append({"stage": "resolution.unresolved", "status": "failed", "reason": result.resolution_notes})
        return result

    top = scored[0]
    second = scored[1] if len(scored) > 1 else None
    if top.score >= 0.58 and (not second or (top.score - second.score) >= 0.12):
        top.accepted = True
        result.resolved_domain = top.domain
        result.resolved_website = normalize_url(top.domain)
        result.resolution_method = "search_validated"
        result.resolution_confidence = top.score
        result.resolution_status = "resolved"
        result.resolution_notes = "Selected highest scoring validated candidate"
        result.trace.append({"stage": "resolution.selected", "status": "ok", "domain": top.domain, "score": top.score})
    elif top.score >= 0.45:
        result.resolution_status = "ambiguous"
        result.resolution_notes = "Top candidates are close or weak; keeping unresolved"
        result.resolution_confidence = top.score
        result.trace.append({"stage": "resolution.unresolved", "status": "ambiguous", "top_score": top.score})
    else:
        result.resolution_status = "unresolved"
        result.resolution_notes = "Candidates did not meet minimum confidence threshold"
        result.resolution_confidence = top.score
        result.trace.append({"stage": "resolution.unresolved", "status": "ok", "top_score": top.score})

    return result


def resolve_company_domain(canonical: CanonicalLeadRow) -> ResolutionResult:
    return resolve_company_website(canonical)
