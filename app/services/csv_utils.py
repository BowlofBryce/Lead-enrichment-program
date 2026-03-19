from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from app.models import Lead
from app.services.normalize import clean_company_name, normalize_phone, normalize_url


EXPECTED_COLUMNS = ["company_name", "website", "city", "state", "phone", "email"]
HEADER_ALIASES = {
    "company": "company_name",
    "companyname": "company_name",
    "name": "company_name",
    "url": "website",
    "site": "website",
    "domain": "website",
    "mail": "email",
    "emailaddress": "email",
    "telephone": "phone",
    "phonenumber": "phone",
    "province": "state",
}
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


@dataclass
class CSVInspectionResult:
    dataframe: pd.DataFrame
    original_headers: list[str]
    normalized_headers: list[str]
    header_mapping: dict[str, str]
    preview_rows: list[dict[str, Any]]
    cleaned_preview_rows: list[dict[str, Any]]
    warnings: list[str]
    detected_row_count: int
    found_expected_columns: list[str]
    missing_expected_columns: list[str]


def _normalize_header(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")
    return HEADER_ALIASES.get(cleaned, cleaned)


def inspect_upload_csv(path: Path) -> CSVInspectionResult:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    original_headers = [str(col) for col in df.columns]
    normalized_headers = [_normalize_header(col) for col in original_headers]

    warnings: list[str] = []
    if len(normalized_headers) != len(set(normalized_headers)):
        warnings.append("Duplicate normalized column names detected; later columns may override earlier values.")

    mapping: dict[str, str] = {}
    renamed_data: dict[str, Any] = {}
    for original, normalized in zip(original_headers, normalized_headers):
        mapping[normalized] = original
        renamed_data[normalized] = df[original]

    normalized_df = pd.DataFrame(renamed_data)
    for col in EXPECTED_COLUMNS:
        if col not in normalized_df.columns:
            normalized_df[col] = ""
            mapping.setdefault(col, "")

    found_expected = [col for col in EXPECTED_COLUMNS if mapping.get(col)]
    missing_expected = [col for col in EXPECTED_COLUMNS if not mapping.get(col)]

    if normalized_df.empty:
        warnings.append("CSV has zero rows after parsing.")

    preview_df = normalized_df.head(10).fillna("")
    preview_rows = preview_df.to_dict(orient="records")

    cleaned_preview_rows = []
    for row in preview_rows:
        cleaned_preview_rows.append(
            {
                "company_name": clean_company_name(str(row.get("company_name", ""))),
                "website": normalize_url(str(row.get("website", ""))),
                "city": str(row.get("city", "") or "").strip(),
                "state": str(row.get("state", "") or "").strip(),
                "phone": normalize_phone(str(row.get("phone", ""))),
                "email": str(row.get("email", "") or "").strip().lower(),
            }
        )

    return CSVInspectionResult(
        dataframe=normalized_df,
        original_headers=original_headers,
        normalized_headers=normalized_headers,
        header_mapping={field: mapping.get(field, "") for field in EXPECTED_COLUMNS},
        preview_rows=preview_rows,
        cleaned_preview_rows=cleaned_preview_rows,
        warnings=warnings,
        detected_row_count=int(len(normalized_df)),
        found_expected_columns=found_expected,
        missing_expected_columns=missing_expected,
    )


def read_upload_csv(path: Path) -> pd.DataFrame:
    return inspect_upload_csv(path).dataframe


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
