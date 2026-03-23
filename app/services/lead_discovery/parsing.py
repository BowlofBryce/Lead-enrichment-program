from __future__ import annotations

from app.services.lead_discovery.types import DiscoveryQuery, NormalizedLead, ParsedLead, RawBusinessRecord


def parse_raw_business(raw: RawBusinessRecord, query: DiscoveryQuery) -> ParsedLead:
    payload = raw.payload
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
    if raw.source == "yelp":
        location = payload.get("location") or {}
        return ParsedLead(
            company_name=(payload.get("name") or "").strip(),
            address=(location.get("address1") or payload.get("location", {}).get("display_address", [""])[0] or "").strip(),
            city=(location.get("city") or query.city or "").strip(),
            state=(location.get("state") or query.state or "").strip(),
            phone=str(payload.get("display_phone") or payload.get("phone") or "").strip(),
            website=str(payload.get("url") or "").strip(),
            category=query.category,
            source=raw.source,
            source_ref=str(payload.get("id") or ""),
        )
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
