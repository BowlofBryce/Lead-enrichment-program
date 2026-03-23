from __future__ import annotations

from app.services.lead_discovery.normalization import website_domain
from app.services.lead_discovery.types import NormalizedLead

JUNK_DOMAINS = {"example.com", "localhost", "test.com", "invalid"}


def validate_lead(lead: NormalizedLead) -> tuple[bool, str]:
    if not lead.company_name:
        return False, "missing_company_name"
    if lead.phone and len(lead.phone) < 12:
        return False, "invalid_phone"
    domain = website_domain(lead.website)
    if domain in JUNK_DOMAINS:
        return False, "junk_domain"
    return True, ""
