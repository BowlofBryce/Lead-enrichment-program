from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from sqlalchemy.orm import Session

from app.models import EnrichmentRun, Lead, LeadClassification, LeadExtraction, LeadPage
from app.services.classify import classify_business
from app.services.crawl import crawl_site
from app.services.extract import extract_from_pages
from app.services.normalize import clean_company_name, dedupe_key, normalize_domain, normalize_phone, normalize_url
from app.services.score import score_lead
from app.settings import settings


def process_run(db: Session, run_id: int) -> None:
    run = db.get(EnrichmentRun, run_id)
    if not run:
        return
    run.status = "processing"
    db.commit()

    seen: set[str] = set()
    leads = db.query(Lead).filter(Lead.run_id == run.id).order_by(Lead.id).all()

    for lead in leads:
        try:
            lead.enrichment_status = "processing"
            lead.cleaned_company_name = clean_company_name(lead.original_company_name)
            lead.normalized_domain = normalize_domain(lead.original_website)
            lead.normalized_phone = normalize_phone(lead.original_phone)
            lead.city = lead.original_city or ""
            lead.state = lead.original_state or ""
            db.commit()

            key = dedupe_key(lead.original_company_name, lead.original_website)
            if key in seen and key != "|":
                lead.enrichment_status = "completed"
                lead.enrichment_error = "duplicate_in_run"
                run.processed_rows += 1
                db.commit()
                continue
            seen.add(key)

            start_url = normalize_url(lead.original_website)
            if not start_url:
                lead.enrichment_status = "failed"
                lead.enrichment_error = "missing_website"
                run.processed_rows += 1
                db.commit()
                continue

            pages = crawl_site(start_url)
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
                )
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

            lead.enrichment_status = "completed"
            if classification.error:
                lead.enrichment_error = f"llm_fallback: {classification.error}"
            run.processed_rows += 1
            db.commit()
        except Exception as exc:
            lead.enrichment_status = "failed"
            lead.enrichment_error = str(exc)
            run.processed_rows += 1
            db.commit()

    run.status = "completed" if run.processed_rows >= run.total_rows else "failed"
    run.completed_at = datetime.utcnow()
    db.commit()


def _save_page_html(run_id: int, lead_id: int, page_type: str, html: str) -> Path:
    page_dir = Path("data/pages")
    page_dir.mkdir(parents=True, exist_ok=True)
    filename = f"run_{run_id}_lead_{lead_id}_{page_type}.html"
    path = page_dir / filename
    path.write_text(html or "", encoding="utf-8")
    return path
