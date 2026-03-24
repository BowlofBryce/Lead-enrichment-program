from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from app.models import Lead
from app.services.lead_row import CANONICAL_COLUMNS, analyze_row, canonicalize_row, pick_canonical_mapping

EXPECTED_COLUMNS = CANONICAL_COLUMNS

EXPORT_COLUMNS = [
    "company_name",
    "website",
    "decision_maker_name",
    "decision_maker_role",
    "decision_maker_email",
    "decision_maker_phone",
    "general_phone",
    "confidence_score",
    "source",
    "original_row_json",
    "canonical_first_name",
    "canonical_last_name",
    "canonical_full_name",
    "canonical_title",
    "canonical_company_name",
    "canonical_company_domain",
    "canonical_website",
    "canonical_email",
    "canonical_phone",
    "linkedin_url",
    "anchor_type",
    "anchor_value",
    "anchor_source",
    "resolved_website",
    "resolved_domain",
    "resolution_method",
    "resolution_confidence",
    "resolution_status",
    "resolution_notes",
    "generated_queries_json",
    "query_generation_notes",
    "semantic_row_json",
    "fields_present",
    "fields_missing",
    "fields_suspicious",
    "enrichment_confidence",
    "person_match_confidence",
    "company_match_confidence",
    "lead_quality_score",
    "validation_notes",
    "business_type",
    "short_summary",
    "public_company_email",
    "public_company_phone",
    "company_address",
    "contact_page_url",
    "about_page_url",
    "team_page_url",
    "has_contact_form",
    "has_online_booking",
    "has_chat_widget",
    "mentions_financing",
    "facebook_url",
    "instagram_url",
    "linkedin_company_url",
    "outreach_angle",
    "provenance_json",
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


def inspect_upload_csv(path: Path) -> CSVInspectionResult:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    original_headers = [str(col) for col in df.columns]
    mapping, normalized_headers, warnings = pick_canonical_mapping(original_headers)

    recognized_anchor_columns = {
        "company_name",
        "website",
        "company_domain",
        "email",
        "phone",
        "full_name",
        "first_name",
        "last_name",
    }

    should_retry_without_header = (
        bool(original_headers)
        and df.shape[1] > 1
        and not any(mapping.get(col) for col in recognized_anchor_columns)
    )

    if should_retry_without_header:
        df_no_header = pd.read_csv(path, dtype=str, keep_default_na=False, header=None)
        generated_headers = [f"column_{idx + 1}" for idx in range(df_no_header.shape[1])]
        df_no_header.columns = generated_headers
        no_header_mapping, no_header_normalized_headers, _ = pick_canonical_mapping(generated_headers)
        row_count_gain = len(df_no_header) - len(df)
        if row_count_gain > 0:
            df = df_no_header
            original_headers = generated_headers
            normalized_headers = no_header_normalized_headers
            mapping = no_header_mapping
            warnings.append(
                "No recognizable CSV headers detected; treated first row as data and generated synthetic column names."
            )

    if df.empty:
        warnings.append("CSV has zero rows after parsing.")

    preview_df = df.head(10).fillna("")
    preview_rows = preview_df.to_dict(orient="records")

    cleaned_preview_rows = []
    for row in preview_rows:
        canonical = canonicalize_row(row, mapping)
        analysis = analyze_row(canonical)
        cleaned_preview_rows.append(
            {
                "canonical": canonical.as_dict(),
                "analysis": {
                    "present": analysis.fields_present,
                    "missing": analysis.fields_missing,
                    "suspicious": analysis.fields_suspicious,
                    "notes": analysis.validation_notes,
                },
            }
        )

    found_expected = [col for col in EXPECTED_COLUMNS if mapping.get(col)]
    missing_expected = [col for col in EXPECTED_COLUMNS if not mapping.get(col)]

    return CSVInspectionResult(
        dataframe=df,
        original_headers=original_headers,
        normalized_headers=normalized_headers,
        header_mapping=mapping,
        preview_rows=preview_rows,
        cleaned_preview_rows=cleaned_preview_rows,
        warnings=warnings,
        detected_row_count=int(len(df)),
        found_expected_columns=found_expected,
        missing_expected_columns=missing_expected,
    )


def read_upload_csv(path: Path) -> pd.DataFrame:
    return inspect_upload_csv(path).dataframe


def lead_to_export_row(lead: Lead) -> dict[str, Any]:
    structured = {}
    try:
        structured = json.loads(lead.semantic_row_json or "{}")
    except Exception:
        structured = {}
    structured = structured if isinstance(structured, dict) else {}
    return {
        "company_name": structured.get("company_name", lead.normalized_company_name or lead.company_name or ""),
        "website": structured.get("website", lead.website or ""),
        "decision_maker_name": structured.get("decision_maker_name", lead.normalized_full_name or lead.full_name or ""),
        "decision_maker_role": structured.get("decision_maker_role", lead.normalized_title or lead.title or ""),
        "decision_maker_email": structured.get("decision_maker_email", lead.normalized_email or lead.email or ""),
        "decision_maker_phone": structured.get("decision_maker_phone", lead.normalized_phone or lead.phone or ""),
        "general_phone": structured.get("general_phone", lead.public_company_phone or ""),
        "confidence_score": structured.get(
            "confidence_score",
            lead.enrichment_confidence if lead.enrichment_confidence is not None else "",
        ),
        "source": structured.get("source", ""),
        "original_row_json": lead.original_row_json or "",
        "canonical_first_name": lead.first_name or "",
        "canonical_last_name": lead.last_name or "",
        "canonical_full_name": lead.normalized_full_name or lead.full_name or "",
        "canonical_title": lead.normalized_title or lead.title or "",
        "canonical_company_name": lead.normalized_company_name or lead.company_name or "",
        "canonical_company_domain": lead.company_domain or "",
        "canonical_website": lead.website or "",
        "canonical_email": lead.normalized_email or lead.email or "",
        "canonical_phone": lead.normalized_phone or lead.phone or "",
        "linkedin_url": lead.linkedin_url or "",
        "anchor_type": lead.anchor_type or "",
        "anchor_value": lead.anchor_value or "",
        "anchor_source": lead.anchor_source or "",
        "resolved_website": lead.resolved_website or "",
        "resolved_domain": lead.resolved_domain or "",
        "resolution_method": lead.resolution_method or "",
        "resolution_confidence": lead.resolution_confidence if lead.resolution_confidence is not None else "",
        "resolution_status": lead.resolution_status or "",
        "resolution_notes": lead.resolution_notes or "",
        "generated_queries_json": lead.generated_queries_json or "[]",
        "query_generation_notes": lead.query_generation_notes or "",
        "semantic_row_json": lead.semantic_row_json or "{}",
        "fields_present": lead.fields_present_json or "[]",
        "fields_missing": lead.fields_missing_json or "[]",
        "fields_suspicious": lead.fields_suspicious_json or "[]",
        "enrichment_confidence": lead.enrichment_confidence if lead.enrichment_confidence is not None else "",
        "person_match_confidence": lead.person_match_confidence if lead.person_match_confidence is not None else "",
        "company_match_confidence": lead.company_match_confidence if lead.company_match_confidence is not None else "",
        "lead_quality_score": lead.lead_quality_score if lead.lead_quality_score is not None else "",
        "validation_notes": lead.validation_notes or "",
        "business_type": lead.business_type or "",
        "short_summary": lead.short_summary or "",
        "public_company_email": lead.public_company_email or "",
        "public_company_phone": lead.public_company_phone or "",
        "company_address": lead.company_address or "",
        "contact_page_url": lead.contact_page_url or "",
        "about_page_url": lead.about_page_url or "",
        "team_page_url": lead.team_page_url or "",
        "has_contact_form": lead.has_contact_form,
        "has_online_booking": lead.has_online_booking,
        "has_chat_widget": lead.has_chat_widget,
        "mentions_financing": lead.mentions_financing,
        "facebook_url": lead.facebook_url or "",
        "instagram_url": lead.instagram_url or "",
        "linkedin_company_url": lead.linkedin_company_url or "",
        "outreach_angle": lead.outreach_angle or "",
        "provenance_json": lead.provenance_json or "{}",
        "enrichment_status": lead.enrichment_status,
        "enrichment_error": lead.enrichment_error or "",
    }


def export_leads_to_csv(rows: list[dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        if not rows:
            writer = csv.DictWriter(f, fieldnames=EXPORT_COLUMNS)
            writer.writeheader()
            return

        passthrough_columns = []
        first = rows[0]
        for key in first:
            if key not in EXPORT_COLUMNS:
                passthrough_columns.append(key)

        writer = csv.DictWriter(f, fieldnames=passthrough_columns + EXPORT_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
