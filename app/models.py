from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


class EnrichmentRun(Base):
    __tablename__ = "enrichment_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(50), default="pending", nullable=False)
    total_rows: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    processed_rows: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    leads: Mapped[list["Lead"]] = relationship(back_populates="run", cascade="all,delete")
    csv_diagnostic: Mapped["CSVParseDiagnostic | None"] = relationship(
        back_populates="run", uselist=False, cascade="all,delete"
    )


class Lead(Base):
    __tablename__ = "leads"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("enrichment_runs.id"), index=True, nullable=False)

    original_row_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    original_company_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    original_website: Mapped[str | None] = mapped_column(String(500), nullable=True)
    original_city: Mapped[str | None] = mapped_column(String(100), nullable=True)
    original_state: Mapped[str | None] = mapped_column(String(100), nullable=True)
    original_phone: Mapped[str | None] = mapped_column(String(100), nullable=True)
    original_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    original_address: Mapped[str | None] = mapped_column(String(255), nullable=True)

    first_name: Mapped[str | None] = mapped_column(String(120), nullable=True)
    last_name: Mapped[str | None] = mapped_column(String(120), nullable=True)
    full_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    normalized_full_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    title: Mapped[str | None] = mapped_column(String(255), nullable=True)
    normalized_title: Mapped[str | None] = mapped_column(String(255), nullable=True)
    company_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    normalized_company_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    normalized_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    email_domain: Mapped[str | None] = mapped_column(String(255), nullable=True)
    phone: Mapped[str | None] = mapped_column(String(100), nullable=True)
    normalized_phone: Mapped[str | None] = mapped_column(String(50), nullable=True)
    company_domain: Mapped[str | None] = mapped_column(String(255), nullable=True)
    website: Mapped[str | None] = mapped_column(String(500), nullable=True)
    linkedin_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    city: Mapped[str | None] = mapped_column(String(100), nullable=True)
    state: Mapped[str | None] = mapped_column(String(100), nullable=True)
    location_text: Mapped[str | None] = mapped_column(String(255), nullable=True)
    input_address: Mapped[str | None] = mapped_column(String(255), nullable=True)

    anchor_type: Mapped[str | None] = mapped_column(String(80), nullable=True)
    anchor_value: Mapped[str | None] = mapped_column(String(500), nullable=True)
    anchor_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    anchor_source: Mapped[str | None] = mapped_column(String(80), nullable=True)
    fields_present_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    fields_missing_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    fields_suspicious_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    provenance_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    enrichment_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    lead_quality_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    person_match_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    company_match_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    validation_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    outreach_angle: Mapped[str | None] = mapped_column(Text, nullable=True)
    resolved_website: Mapped[str | None] = mapped_column(String(500), nullable=True)
    resolved_domain: Mapped[str | None] = mapped_column(String(255), nullable=True)
    resolution_method: Mapped[str | None] = mapped_column(String(80), nullable=True)
    resolution_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    resolution_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    candidate_websites_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    resolution_status: Mapped[str | None] = mapped_column(String(50), nullable=True)

    # company/site outputs
    public_company_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    public_company_phone: Mapped[str | None] = mapped_column(String(100), nullable=True)
    company_address: Mapped[str | None] = mapped_column(String(255), nullable=True)
    business_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    services_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    short_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    has_contact_form: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    has_online_booking: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    has_chat_widget: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    mentions_financing: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    likely_decision_maker_names_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    contact_page_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    about_page_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    team_page_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    facebook_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    instagram_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    linkedin_company_url: Mapped[str | None] = mapped_column(String(500), nullable=True)

    # legacy fields retained for backward compatibility
    cleaned_company_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    normalized_domain: Mapped[str | None] = mapped_column(String(255), nullable=True)
    public_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    public_phone: Mapped[str | None] = mapped_column(String(100), nullable=True)
    address: Mapped[str | None] = mapped_column(String(255), nullable=True)
    fit_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    fit_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    extraction_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)

    enrichment_status: Mapped[str] = mapped_column(String(50), default="pending", nullable=False)
    enrichment_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    run: Mapped[EnrichmentRun] = relationship(back_populates="leads")
    pages: Mapped[list["LeadPage"]] = relationship(back_populates="lead", cascade="all,delete")
    extraction: Mapped["LeadExtraction | None"] = relationship(
        back_populates="lead", uselist=False, cascade="all,delete"
    )
    classification: Mapped["LeadClassification | None"] = relationship(
        back_populates="lead", uselist=False, cascade="all,delete"
    )
    debug_events: Mapped[list["LeadDebugEvent"]] = relationship(back_populates="lead", cascade="all,delete")


class LeadPage(Base):
    __tablename__ = "lead_pages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    lead_id: Mapped[int] = mapped_column(ForeignKey("leads.id"), index=True, nullable=False)
    page_type: Mapped[str] = mapped_column(String(50), nullable=False)
    url: Mapped[str] = mapped_column(String(500), nullable=False)
    title: Mapped[str | None] = mapped_column(String(255), nullable=True)
    raw_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    html_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    fetched_with: Mapped[str | None] = mapped_column(String(50), nullable=True)
    fetch_status: Mapped[str] = mapped_column(String(50), default="ok", nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    lead: Mapped[Lead] = relationship(back_populates="pages")


class LeadExtraction(Base):
    __tablename__ = "lead_extractions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    lead_id: Mapped[int] = mapped_column(ForeignKey("leads.id"), unique=True, nullable=False)
    emails_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    phones_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    social_links_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    address_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    contact_page_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    about_page_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    team_page_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    booking_signals_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    financing_signals_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    chat_widget_signals_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    lead: Mapped[Lead] = relationship(back_populates="extraction")


class LeadClassification(Base):
    __tablename__ = "lead_classifications"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    lead_id: Mapped[int] = mapped_column(ForeignKey("leads.id"), unique=True, nullable=False)
    model_name: Mapped[str] = mapped_column(String(100), nullable=False)
    prompt_version: Mapped[str] = mapped_column(String(20), default="v1", nullable=False)
    raw_response: Mapped[str | None] = mapped_column(Text, nullable=True)
    business_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    services_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    short_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    likely_decision_maker_names_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    fit_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    ollama_request_payload_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    ollama_raw_response: Mapped[str | None] = mapped_column(Text, nullable=True)
    ollama_parse_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    lead: Mapped[Lead] = relationship(back_populates="classification")


class CSVParseDiagnostic(Base):
    __tablename__ = "csv_parse_diagnostics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("enrichment_runs.id"), unique=True, nullable=False)
    original_headers_json: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_headers_json: Mapped[str] = mapped_column(Text, nullable=False)
    header_mapping_json: Mapped[str] = mapped_column(Text, nullable=False)
    detected_row_count: Mapped[int] = mapped_column(Integer, nullable=False)
    preview_rows_json: Mapped[str] = mapped_column(Text, nullable=False)
    cleaned_preview_rows_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    warnings_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    run: Mapped[EnrichmentRun] = relationship(back_populates="csv_diagnostic")


class LeadDebugEvent(Base):
    __tablename__ = "lead_debug_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    lead_id: Mapped[int] = mapped_column(ForeignKey("leads.id"), index=True, nullable=False)
    run_id: Mapped[int] = mapped_column(ForeignKey("enrichment_runs.id"), index=True, nullable=False)
    stage: Mapped[str] = mapped_column(String(80), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    payload_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    lead: Mapped[Lead] = relationship(back_populates="debug_events")
