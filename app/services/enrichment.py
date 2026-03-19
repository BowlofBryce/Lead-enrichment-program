from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from app.models import EnrichmentRun, Lead, LeadClassification, LeadDebugEvent, LeadExtraction, LeadPage
from app.services.classify import classify_business
from app.services.crawl import crawl_site
from app.services.extract import extract_from_pages
from app.services.logging_utils import get_logger
from app.services.normalize import clean_company_name, dedupe_key, normalize_domain, normalize_phone, normalize_url
from app.services.score import score_lead
from app.settings import settings

logger = get_logger(__name__)


def _add_debug_event(
    db: Session,
    *,
    run_id: int,
    lead_id: int,
    stage: str,
    status: str,
    message: str,
    payload: dict[str, Any] | None = None,
) -> None:
    payload_json = json.dumps(payload) if payload and settings.debug_mode else None
    db.add(
        LeadDebugEvent(
            lead_id=lead_id,
            run_id=run_id,
            stage=stage,
            status=status,
            message=message,
            payload_json=payload_json,
        )
    )


def process_run(db: Session, run_id: int) -> None:
    run = db.get(EnrichmentRun, run_id)
    if not run:
        return
    logger.info("enrichment.run.started", extra_fields={"run_id": run.id, "filename": run.filename})
    run.status = "processing"
    db.commit()

    seen: set[str] = set()
    leads = db.query(Lead).filter(Lead.run_id == run.id).order_by(Lead.id).all()

    for lead in leads:
        logger.info("lead.processing.started", extra_fields={"run_id": run.id, "lead_id": lead.id})
        try:
            lead.enrichment_status = "processing"
            lead.cleaned_company_name = clean_company_name(lead.original_company_name)
            lead.normalized_domain = normalize_domain(lead.original_website)
            lead.normalized_phone = normalize_phone(lead.original_phone)
            lead.city = lead.original_city or ""
            lead.state = lead.original_state or ""
            _add_debug_event(
                db,
                run_id=run.id,
                lead_id=lead.id,
                stage="normalize",
                status="ok",
                message="Normalized lead values",
                payload={
                    "cleaned_company_name": lead.cleaned_company_name,
                    "normalized_domain": lead.normalized_domain,
                    "normalized_phone": lead.normalized_phone,
                },
            )
            db.commit()

            key = dedupe_key(lead.original_company_name, lead.original_website)
            if key in seen and key != "|":
                lead.enrichment_status = "completed"
                lead.enrichment_error = "duplicate_in_run"
                run.processed_rows += 1
                _add_debug_event(db, run_id=run.id, lead_id=lead.id, stage="persist", status="skip", message="Duplicate in run")
                db.commit()
                continue
            seen.add(key)

            start_url = normalize_url(lead.original_website)
            if not start_url:
                lead.enrichment_status = "failed"
                lead.enrichment_error = "missing_website"
                run.processed_rows += 1
                _add_debug_event(db, run_id=run.id, lead_id=lead.id, stage="crawl_homepage", status="failed", message="Missing website")
                db.commit()
                continue

            pages = crawl_site(start_url)
            _add_debug_event(
                db,
                run_id=run.id,
                lead_id=lead.id,
                stage="discover_subpages",
                status="ok",
                message="Crawl completed",
                payload={"page_urls": [p.url for p in pages], "statuses": [p.fetch_status for p in pages]},
            )
            logger.info(
                "lead.crawl.completed",
                extra_fields={"lead_id": lead.id, "urls": [p.url for p in pages], "statuses": [p.fetch_status for p in pages]},
            )
            for page in pages:
                html_path = _save_page_html(run.id, lead.id, page.page_type, page.html)
                db.add(
                    LeadPage(
                        lead_id=lead.id,
                        page_type=page.page_type,
                        url=page.url,
                        title=page.title,
                        raw_text=page.text[:25000] if page.text else "",
                        html_path=str(html_path),
                        fetched_with=page.fetched_with,
                        fetch_status=page.fetch_status,
                    )
                )
            db.commit()

            good_pages = [p for p in pages if p.fetch_status == "ok"]
            if not good_pages:
                lead.enrichment_status = "failed"
                lead.enrichment_error = "no_extractable_data"
                run.processed_rows += 1
                _add_debug_event(db, run_id=run.id, lead_id=lead.id, stage="extract_fields", status="failed", message="No extractable pages")
                db.commit()
                continue

            extraction = extract_from_pages(good_pages)
            db.add(
                LeadExtraction(
                    lead_id=lead.id,
                    emails_json=json.dumps(extraction.emails),
                    phones_json=json.dumps(extraction.phones),
                    social_links_json=json.dumps(extraction.social_links),
                    address_text=extraction.address_text,
                    contact_page_url=extraction.contact_page_url,
                    about_page_url=extraction.about_page_url,
                    team_page_url=extraction.team_page_url,
                    booking_signals_json=json.dumps(extraction.booking_signals),
                    financing_signals_json=json.dumps(extraction.financing_signals),
                    chat_widget_signals_json=json.dumps(extraction.chat_widget_signals),
                )
            )
            lead.public_email = extraction.emails[0] if extraction.emails else ""
            lead.public_phone = extraction.phones[0] if extraction.phones else ""
            lead.address = extraction.address_text or ""
            _add_debug_event(
                db,
                run_id=run.id,
                lead_id=lead.id,
                stage="extract_fields",
                status="ok",
                message="Extracted deterministic signals",
                payload={"emails": extraction.emails, "phones": extraction.phones, "social_links": extraction.social_links},
            )

            full_text = " ".join([p.text for p in good_pages if p.text])[:20000]
            classification = classify_business(full_text, extraction.has_contact_form)
            db.add(
                LeadClassification(
                    lead_id=lead.id,
                    model_name=classification.model_name or settings.ollama_model,
                    prompt_version=classification.prompt_version,
                    raw_response=classification.raw_response,
                    business_type=classification.business_type,
                    services_json=json.dumps(classification.services),
                    short_summary=classification.short_summary,
                    likely_decision_maker_names_json=json.dumps(classification.likely_decision_maker_names),
                    fit_reason=classification.fit_reason,
                    confidence=classification.confidence,
                    ollama_request_payload_json=json.dumps(classification.ollama_request_payload),
                    ollama_raw_response=classification.ollama_raw_response,
                    ollama_parse_error=classification.ollama_parse_error,
                )
            )
            _add_debug_event(
                db,
                run_id=run.id,
                lead_id=lead.id,
                stage="classify",
                status="ok" if not classification.error else "warning",
                message="Classification completed" if not classification.error else f"LLM fallback: {classification.error}",
                payload={
                    "model": classification.model_name,
                    "business_type": classification.business_type,
                    "parse_error": classification.ollama_parse_error,
                },
            )

            lead.business_type = classification.business_type
            lead.services_json = json.dumps(classification.services)
            lead.short_summary = classification.short_summary
            lead.has_online_booking = classification.has_online_booking or bool(extraction.booking_signals)
            lead.has_contact_form = classification.has_contact_form or extraction.has_contact_form
            lead.has_chat_widget = classification.has_chat_widget or bool(extraction.chat_widget_signals)
            lead.mentions_financing = classification.mentions_financing or bool(extraction.financing_signals)
            lead.likely_decision_maker_names_json = json.dumps(classification.likely_decision_maker_names)
            lead.fit_reason = classification.fit_reason

            scored = score_lead(
                has_email=bool(lead.public_email),
                has_phone=bool(lead.public_phone),
                has_address=bool(lead.address),
                has_summary=bool(lead.short_summary),
                classification_confidence=classification.confidence,
                page_count=len(good_pages),
            )
            lead.fit_score = scored.fit_score
            lead.extraction_confidence = scored.extraction_confidence
            _add_debug_event(
                db,
                run_id=run.id,
                lead_id=lead.id,
                stage="score",
                status="ok",
                message="Lead scored",
                payload={"fit_score": lead.fit_score, "extraction_confidence": lead.extraction_confidence},
            )

            lead.enrichment_status = "completed"
            if classification.error:
                lead.enrichment_error = f"llm_fallback: {classification.error}"
            run.processed_rows += 1
            _add_debug_event(db, run_id=run.id, lead_id=lead.id, stage="persist", status="ok", message="Lead saved")
            db.commit()
            logger.info("lead.processing.completed", extra_fields={"run_id": run.id, "lead_id": lead.id})
        except Exception as exc:
            lead.enrichment_status = "failed"
            lead.enrichment_error = str(exc)
            run.processed_rows += 1
            _add_debug_event(db, run_id=run.id, lead_id=lead.id, stage="persist", status="failed", message=str(exc))
            db.commit()
            logger.exception("lead.processing.failed", extra_fields={"run_id": run.id, "lead_id": lead.id})

    run.status = "completed" if run.processed_rows >= run.total_rows else "failed"
    run.completed_at = datetime.utcnow()
    db.commit()
    logger.info("enrichment.run.completed", extra_fields={"run_id": run.id, "status": run.status})


def _save_page_html(run_id: int, lead_id: int, page_type: str, html: str) -> Path:
    page_dir = Path("data/pages")
    page_dir.mkdir(parents=True, exist_ok=True)
    filename = f"run_{run_id}_lead_{lead_id}_{page_type}.html"
    path = page_dir / filename
    path.write_text(html or "", encoding="utf-8")
    return path
