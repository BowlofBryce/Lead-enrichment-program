from __future__ import annotations

import json
from urllib.parse import urlparse

from app.models import DiscoveryLead
from app.services.lead_discovery.types import DiscoveryQuery, NormalizedLead, ParsedLead, RawBusinessRecord


def parse_raw_business(raw: RawBusinessRecord, query: DiscoveryQuery) -> ParsedLead:
    payload = raw.payload

    if raw.source == "stub":
        return ParsedLead(
            company_name=(payload.get("name") or "").strip(),
            address=(payload.get("address") or "").strip(),
            city=(payload.get("city") or query.city or "").strip(),
            state=(payload.get("state") or query.state or "").strip(),
            phone=str(payload.get("phone") or "").strip(),
            website=str(payload.get("website") or "").strip(),
            category=query.category,
            source=raw.source,
            source_ref=str(payload.get("id") or ""),
        )

    if raw.source == "google_places":
        address = payload.get("formatted_address") or ""
        return ParsedLead(
            company_name=(payload.get("name") or "").strip(),
            address=address.strip(),
            city=query.city,
            state=query.state,
            phone=str(payload.get("formatted_phone_number") or "").strip(),
            website=str(payload.get("website") or "").strip(),
            category=query.category,
            source=raw.source,
            source_ref=str(payload.get("place_id") or ""),
        )

    if raw.source in ("yelp_api", "yelp"):
        location = payload.get("location") or {}
        disp = location.get("display_address") or []
        addr_line = disp[0] if isinstance(disp, list) and disp else ""
        return ParsedLead(
            company_name=(payload.get("name") or "").strip(),
            address=(location.get("address1") or addr_line or "").strip(),
            city=(location.get("city") or query.city or "").strip(),
            state=(location.get("state") or query.state or "").strip(),
            phone=str(payload.get("display_phone") or payload.get("phone") or "").strip(),
            website=str(payload.get("url") or "").strip(),
            category=query.category,
            source=raw.source,
            source_ref=str(payload.get("id") or ""),
        )

    if raw.source == "yelp_directory":
        return ParsedLead(
            company_name=(payload.get("business_name") or "").strip(),
            address=(payload.get("address_line") or "").strip(),
            city=query.city,
            state=query.state,
            phone=str(payload.get("phone") or "").strip(),
            website=_clean_external_website(payload.get("website") or ""),
            category=query.category,
            source=raw.source,
            source_ref=str(payload.get("listing_url") or ""),
        )

    if raw.source == "yellowpages_directory":
        return ParsedLead(
            company_name=(payload.get("business_name") or "").strip(),
            address=(payload.get("address_line") or "").strip(),
            city=query.city,
            state=query.state,
            phone=str(payload.get("phone") or "").strip(),
            website=_clean_external_website(payload.get("website") or ""),
            category=query.category,
            source=raw.source,
            source_ref=str(payload.get("listing_url") or ""),
        )

    if raw.source == "brave_search":
        website = str(payload.get("url") or "").strip()
        title = str(payload.get("title") or "").strip()
        domain = (urlparse(website).netloc or "").replace("www.", "")
        return ParsedLead(
            company_name=title or domain,
            address="",
            city=str(payload.get("city") or query.city or "").strip(),
            state=str(payload.get("state") or query.state or "").strip(),
            phone="",
            website=website,
            category=query.category,
            source=raw.source,
            source_ref=website,
        )

    # OpenStreetMap / Nominatim
    address_details = payload.get("address") or {}
    return ParsedLead(
        company_name=(payload.get("name") or payload.get("display_name") or "").split(",")[0].strip(),
        address=(payload.get("display_name") or "").strip(),
        city=str(address_details.get("city") or address_details.get("town") or query.city or "").strip(),
        state=str(address_details.get("state") or query.state or "").strip(),
        phone="",
        website=str(payload.get("website") or "").strip(),
        category=query.category,
        source=raw.source,
        source_ref=str(payload.get("osm_id") or ""),
    )


def _clean_external_website(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    low = u.lower()
    if "yelp.com" in low or "yellowpages.com" in low:
        return ""
    return u


def to_normalized(parsed: ParsedLead, raw_payload: dict) -> NormalizedLead:
    from app.services.lead_discovery.normalization import clean_phone, clean_url, normalize_text

    company_name = normalize_text(parsed.company_name)
    city = normalize_text(parsed.city, title_case=True)
    state = normalize_text(parsed.state, upper=True)
    website = clean_url(parsed.website)
    phone = clean_phone(parsed.phone)

    identity_seed = "|".join([company_name.lower(), website.lower(), phone.lower(), city.lower(), state.lower(), parsed.source_ref])
    lead_id = str(abs(hash(identity_seed)))

    return NormalizedLead(
        id=lead_id,
        company_name=company_name,
        website=website,
        phone=phone,
        city=city,
        state=state,
        category=normalize_text(parsed.category, title_case=True),
        source=parsed.source,
        address=normalize_text(parsed.address),
        source_ref=parsed.source_ref,
        raw_payload=raw_payload,
    )


def normalized_from_discovery_row(lead: DiscoveryLead) -> NormalizedLead:
    """Reconstruct NormalizedLead from persisted discovery row + raw JSON."""
    raw: dict = {}
    if lead.raw_payload_json:
        try:
            raw = json.loads(lead.raw_payload_json)
        except json.JSONDecodeError:
            raw = {}
    return NormalizedLead(
        id=lead.external_id or str(lead.id),
        company_name=lead.company_name,
        website=lead.website or "",
        phone=lead.phone or "",
        city=lead.city or "",
        state=lead.state or "",
        category=lead.category or "",
        source=lead.source,
        address=lead.address or "",
        source_ref=lead.source_ref or "",
        raw_payload=raw,
    )
