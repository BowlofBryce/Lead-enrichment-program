from __future__ import annotations

import csv
import json
from pathlib import Path

import pandas as pd

from app.models import Lead


EXPECTED_COLUMNS = ["company_name", "website", "city", "state", "phone", "email"]
EXPORT_COLUMNS = [
    "original_company_name",
    "original_website",
    "original_city",
    "original_state",
    "original_phone",
    "original_email",
    "cleaned_company_name",
    "normalized_domain",
    "normalized_phone",
    "public_email",
    "public_phone",
    "city",
    "state",
    "address",
    "contact_page_url",
    "about_page_url",
    "team_page_url",
    "facebook_url",
    "instagram_url",
    "linkedin_url",
    "business_type",
    "services",
    "short_summary",
    "has_contact_form",
    "has_online_booking",
    "has_chat_widget",
    "mentions_financing",
    "likely_decision_maker_names",
    "fit_score",
    "fit_reason",
    "extraction_confidence",
    "enrichment_status",
    "enrichment_error",
]


def read_upload_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    renamed = {c.lower().strip(): c for c in df.columns}
    for col in EXPECTED_COLUMNS:
        if col not in renamed:
            df[col] = ""
    return df


def lead_to_export_row(lead: Lead) -> dict:
    extraction = lead.extraction
    social = {}
    if extraction and extraction.social_links_json:
        try:
            social = json.loads(extraction.social_links_json)
        except json.JSONDecodeError:
            social = {}
    return {
        "original_company_name": lead.original_company_name or "",
        "original_website": lead.original_website or "",
        "original_city": lead.original_city or "",
        "original_state": lead.original_state or "",
        "original_phone": lead.original_phone or "",
        "original_email": lead.original_email or "",
        "cleaned_company_name": lead.cleaned_company_name or "",
        "normalized_domain": lead.normalized_domain or "",
        "normalized_phone": lead.normalized_phone or "",
        "public_email": lead.public_email or "",
        "public_phone": lead.public_phone or "",
        "city": lead.city or "",
        "state": lead.state or "",
        "address": lead.address or "",
        "contact_page_url": extraction.contact_page_url if extraction else "",
        "about_page_url": extraction.about_page_url if extraction else "",
        "team_page_url": extraction.team_page_url if extraction else "",
        "facebook_url": social.get("facebook_url", ""),
        "instagram_url": social.get("instagram_url", ""),
        "linkedin_url": social.get("linkedin_url", ""),
        "business_type": lead.business_type or "",
        "services": lead.services_json or "",
        "short_summary": lead.short_summary or "",
        "has_contact_form": lead.has_contact_form,
        "has_online_booking": lead.has_online_booking,
        "has_chat_widget": lead.has_chat_widget,
        "mentions_financing": lead.mentions_financing,
        "likely_decision_maker_names": lead.likely_decision_maker_names_json or "",
        "fit_score": lead.fit_score if lead.fit_score is not None else "",
        "fit_reason": lead.fit_reason or "",
        "extraction_confidence": lead.extraction_confidence if lead.extraction_confidence is not None else "",
        "enrichment_status": lead.enrichment_status,
        "enrichment_error": lead.enrichment_error or "",
    }


def export_leads_to_csv(rows: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=EXPORT_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
