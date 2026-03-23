from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from sqlalchemy.orm import Session

from enrichment.contact_extractor import ContactExtractionResult, extract_contacts
from enrichment.decision_engine import build_lead_output, run_decision_engine
from app.models import EnrichmentRun, EnrichmentRunEvent, Lead, LeadDebugEvent, LeadExtraction, LeadPage
from app.services.crawl import crawl_site
from app.services.lead_row import analyze_row, canonicalize_from_dynamic, canonicalize_row, resolve_anchor, to_json
from app.services.logging_utils import get_logger
from app.services.normalize import dedupe_key, normalize_domain, normalize_url
from app.services.ollama_client import list_models
from app.services.resolution import resolve_company_website
from app.services.schema_inference import infer_schema_plan, transform_row_with_plan
from app.settings import settings

logger = get_logger(__name__)


MINIMAL_CRAWL_PAGE_TYPES = {"homepage", "contact", "about", "team"}


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


def _emit_run_event(
    db: Session,
    *,
    run: EnrichmentRun,
    event_type: str,
    machine_status: str,
    human_message: str,
    severity: str = "info",
    lead_id: int | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    run.current_action_message = human_message
    payload_json = json.dumps(payload) if payload else None
    db.add(
        EnrichmentRunEvent(
            run_id=run.id,
            lead_id=lead_id,
            event_type=event_type,
            machine_status=machine_status,
            human_message=human_message,
            severity=severity,
            payload_json=payload_json,
        )
    )


def _maybe_person_name_found(lead: Lead, pages: list[LeadPage | Any]) -> bool:
    if not lead.normalized_full_name:
        return False
    targets = {lead.normalized_full_name.lower()}
    if lead.last_name:
        targets.add(lead.last_name.lower())
    for page in pages:
        if page.page_type not in {"team", "about"}:
            continue
        text = (page.text if hasattr(page, "text") else page.raw_text) or ""
        lowered = text.lower()
        if any(name in lowered for name in targets):
            return True
    return False


def _build_outreach_angle(lead: Lead) -> str:
    if lead.business_type and lead.short_summary:
        return f"Mention {lead.business_type.lower()} focus and reference: {lead.short_summary[:140]}"
    if lead.company_name:
        return f"Personalize around {lead.company_name} with a concise problem/solution opener"
    return "Keep outreach generic due to weak anchors"


def _cache_key_for_url(url: str) -> str:
    parsed = urlparse(url)
    return parsed.netloc.lower().strip() or url.lower().strip()


def process_run(db: Session, run_id: int) -> None:
    run = db.get(EnrichmentRun, run_id)
    if not run:
        return
    logger.info("enrichment.run.started", extra_fields={"run_id": run.id, "filename": run.filename})
    selected_model = (run.selected_model or "").strip()
    custom_instructions = (run.custom_instructions or "").strip()
    model_to_use = selected_model or settings.ollama_model
    used_default_model = not bool(selected_model)
    if selected_model:
        try:
            installed_models = {m.name for m in list_models()}
            if selected_model not in installed_models:
                run.status = "failed"
                run.error_message = f"Selected model '{selected_model}' is not installed in local Ollama."
                db.commit()
                logger.warning("enrichment.run.model_missing", extra_fields={"run_id": run.id, "selected_model": selected_model})
                return
        except Exception as exc:
            run.status = "failed"
            run.error_message = f"Unable to verify selected model '{selected_model}': {exc}"
            db.commit()
            logger.exception("enrichment.run.model_check_failed", extra_fields={"run_id": run.id, "selected_model": selected_model})
            return
    logger.info(
        "enrichment.run.config",
        extra_fields={
            "run_id": run.id,
            "selected_model": selected_model,
            "used_default_model": used_default_model,
            "model_in_use": model_to_use,
            "custom_instructions": custom_instructions[:400],
        },
    )
    if run.status in {"completed", "cancelled"}:
        return
    run.status = "running" if run.processed_rows == 0 else "resuming"
    if not run.started_at:
        run.started_at = datetime.utcnow()
    if run.processed_rows > 0:
        run.resumed_at = datetime.utcnow()
    _emit_run_event(
        db,
        run=run,
        event_type="run_state",
        machine_status=run.status,
        human_message="Starting enrichment run." if run.status == "running" else "Resuming enrichment from saved progress.",
    )
    db.commit()

    diagnostic = run.csv_diagnostic
    header_mapping = json.loads(diagnostic.header_mapping_json) if diagnostic else {}
    original_headers = json.loads(diagnostic.original_headers_json) if diagnostic and diagnostic.original_headers_json else []
    normalized_headers = json.loads(diagnostic.normalized_headers_json) if diagnostic and diagnostic.normalized_headers_json else []
    preview_rows = json.loads(diagnostic.preview_rows_json) if diagnostic and diagnostic.preview_rows_json else []

    schema_model = (run.schema_inference_model or settings.default_schema_inference_model or model_to_use).strip()
    query_model = (run.query_generation_model or settings.default_query_generation_model or model_to_use).strip()
    schema_result = infer_schema_plan(
        headers=original_headers,
        normalized_headers=normalized_headers,
        sample_rows=preview_rows[:50],
        custom_instructions=custom_instructions,
        model_name=schema_model,
    )
    search_strategy = schema_result.plan_json.get("search_strategy_json", {})
    run.schema_inference_model = schema_result.model_used
    run.query_generation_model = query_model
    run.schema_inference_json = json.dumps(schema_result.plan_json)
    run.search_strategy_json = json.dumps(search_strategy)
    _emit_run_event(
        db,
        run=run,
        event_type="schema",
        machine_status=run.status,
        human_message="Reading uploaded CSV and mapping columns.",
    )
    db.commit()

    seen: set[str] = set()
    domain_page_cache: dict[str, list[Any]] = {}
    leads = (
        db.query(Lead)
        .filter(Lead.run_id == run.id)
        .filter(Lead.enrichment_status.in_(["pending", "processing"]))
        .order_by(Lead.id)
        .all()
    )

    for lead in leads:
        db.refresh(run)
        if run.pause_requested:
            run.status = "paused"
            _emit_run_event(
                db,
                run=run,
                event_type="run_state",
                machine_status="paused",
                human_message="Paused. Waiting for resume.",
            )
            db.commit()
            return
        logger.info("lead.processing.started", extra_fields={"run_id": run.id, "lead_id": lead.id})
        try:
            lead.enrichment_status = "processing"
            _emit_run_event(
                db,
                run=run,
                lead_id=lead.id,
                event_type="lead_start",
                machine_status=run.status,
                human_message=f"Calling enrichment provider for record {run.processed_rows + 1} of {run.total_rows}.",
            )
            db.commit()

            raw_row = json.loads(lead.original_row_json or "{}")
            transformed = transform_row_with_plan(raw_row, schema_result.plan_json)
            dynamic_canonical = transformed.get("canonical", {}) if isinstance(transformed, dict) else {}
            semantic_values = transformed.get("semantic_values", {}) if isinstance(transformed, dict) else {}
            if isinstance(dynamic_canonical, dict) and any(dynamic_canonical.values()):
                canonical = canonicalize_from_dynamic(dynamic_canonical)
            else:
                canonical = canonicalize_row(raw_row, header_mapping)
            analysis = analyze_row(canonical)
            _emit_run_event(
                db,
                run=run,
                lead_id=lead.id,
                event_type="normalize",
                machine_status=run.status,
                human_message="Normalizing company website URLs and contact fields.",
            )
            resolution = resolve_company_website(
                canonical,
                custom_instructions=custom_instructions,
                search_strategy=search_strategy,
                query_generation_model=query_model,
            )

            for event in resolution.trace:
                _add_debug_event(
                    db,
                    run_id=run.id,
                    lead_id=lead.id,
                    stage=event.get("stage", "resolution.trace"),
                    status=event.get("status", "ok"),
                    message=event.get("message", event.get("reason", "resolution trace")),
                    payload=event,
                )

            if resolution.resolved_website and not canonical.website:
                canonical.website = normalize_url(resolution.resolved_website)
            if resolution.resolved_domain and not canonical.company_domain:
                canonical.company_domain = normalize_domain(resolution.resolved_domain)

            anchor = resolve_anchor(canonical)

            lead.first_name = canonical.first_name
            lead.last_name = canonical.last_name
            lead.full_name = canonical.full_name
            lead.normalized_full_name = canonical.normalized_full_name
            lead.title = canonical.title
            lead.normalized_title = canonical.normalized_title
            lead.company_name = canonical.company_name
            lead.normalized_company_name = canonical.normalized_company_name
            lead.email = canonical.email
            lead.normalized_email = canonical.normalized_email
            lead.email_domain = canonical.email_domain
            lead.phone = canonical.phone
            lead.normalized_phone = canonical.normalized_phone
            lead.company_domain = canonical.company_domain
            lead.website = canonical.website
            lead.linkedin_url = canonical.linkedin_url
            lead.city = canonical.city
            lead.state = canonical.state
            lead.location_text = canonical.location_text
            lead.input_address = canonical.address

            lead.cleaned_company_name = canonical.normalized_company_name
            lead.normalized_domain = canonical.company_domain

            lead.anchor_type = anchor.anchor_type
            lead.anchor_value = anchor.anchor_value
            lead.anchor_reason = anchor.reason
            lead.anchor_source = "resolution" if resolution.resolution_status == "resolved" else "original_or_derived"
            lead.resolved_website = resolution.resolved_website
            lead.resolved_domain = resolution.resolved_domain
            lead.resolution_method = resolution.resolution_method
            lead.resolution_confidence = resolution.resolution_confidence
            lead.resolution_notes = resolution.resolution_notes
            lead.candidate_websites_json = resolution.candidate_websites_json
            lead.generated_queries_json = to_json(resolution.search_queries)
            lead.query_generation_notes = resolution.resolution_notes or ""
            lead.resolution_status = resolution.resolution_status
            lead.fields_present_json = to_json(analysis.fields_present)
            lead.fields_missing_json = to_json(analysis.fields_missing)
            lead.fields_suspicious_json = to_json(analysis.fields_suspicious)
            lead.validation_notes = "; ".join(analysis.validation_notes)
            lead.semantic_row_json = to_json(semantic_values if isinstance(semantic_values, dict) else {})

            provenance = {k: "original_csv" for k, v in canonical.as_dict().items() if v}
            if canonical.normalized_full_name:
                provenance["normalized_full_name"] = "normalized_from_original"
            if canonical.company_domain and not raw_row.get(header_mapping.get("company_domain", ""), ""):
                provenance["company_domain"] = "derived_from_website_or_email"
            if canonical.email_domain:
                provenance["email_domain"] = "derived_from_email_domain"

            _add_debug_event(
                db,
                run_id=run.id,
                lead_id=lead.id,
                stage="row_analysis",
                status="ok",
                message="Canonical row normalized and analyzed",
                payload={
                    "canonical": canonical.as_dict(),
                    "present": analysis.fields_present,
                    "missing": analysis.fields_missing,
                    "suspicious": analysis.fields_suspicious,
                    "anchor": anchor.__dict__,
                    "resolution": {
                        "status": resolution.resolution_status,
                        "method": resolution.resolution_method,
                        "confidence": resolution.resolution_confidence,
                        "notes": resolution.resolution_notes,
                        "search_queries": resolution.search_queries,
                    },
                },
            )
            db.commit()

            key = dedupe_key(lead.company_name, lead.website)
            if key in seen and key != "|":
                lead.enrichment_status = "completed"
                lead.enrichment_error = "duplicate_in_run"
                run.processed_rows += 1
                run.skipped_count += 1
                _add_debug_event(db, run_id=run.id, lead_id=lead.id, stage="persist", status="skip", message="Duplicate in run")
                _emit_run_event(
                    db,
                    run=run,
                    lead_id=lead.id,
                    event_type="lead_skip",
                    machine_status=run.status,
                    human_message="Skipping duplicate record already seen in this run.",
                    severity="warning",
                )
                db.commit()
                continue
            seen.add(key)

            crawl_url = normalize_url(lead.website or lead.company_domain)
            should_crawl = bool(crawl_url and anchor.anchor_type != "unresolved")
            pages: list[Any] = []
            good_pages: list[Any] = []
            contact_result = ContactExtractionResult(items=[])

            if should_crawl:
                _emit_run_event(
                    db,
                    run=run,
                    lead_id=lead.id,
                    event_type="crawl",
                    machine_status=run.status,
                    human_message=f"🌐 Scraping website ({crawl_url}).",
                )
                domain_key = _cache_key_for_url(crawl_url)
                if domain_key in domain_page_cache:
                    pages = domain_page_cache[domain_key]
                else:
                    pages = crawl_site(crawl_url)
                    pages = [p for p in pages if p.page_type in MINIMAL_CRAWL_PAGE_TYPES][:4]
                    domain_page_cache[domain_key] = pages
                _add_debug_event(
                    db,
                    run_id=run.id,
                    lead_id=lead.id,
                    stage="crawl",
                    status="ok",
                    message="Minimal crawl attempted",
                    payload={"page_urls": [p.url for p in pages], "statuses": [p.fetch_status for p in pages]},
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
            else:
                _add_debug_event(
                    db,
                    run_id=run.id,
                    lead_id=lead.id,
                    stage="crawl",
                    status="skip",
                    message="Crawl skipped; weak/no anchor",
                )
                db.commit()

            company_site_found = bool(good_pages)
            person_name_found = False

            if good_pages:
                _emit_run_event(
                    db,
                    run=run,
                    lead_id=lead.id,
                    event_type="extract_contacts",
                    machine_status=run.status,
                    human_message="📄 Extracting contacts.",
                )
                contact_result = extract_contacts(good_pages)
                emails = [item.value for item in contact_result.emails]
                phones = [item.value for item in contact_result.phones]
                person_name_found = bool(contact_result.names)
                db.add(
                    LeadExtraction(
                        lead_id=lead.id,
                        emails_json=json.dumps(emails),
                        phones_json=json.dumps(phones),
                        social_links_json=json.dumps({}),
                        address_text="",
                        contact_page_url=next((p.url for p in good_pages if p.page_type == "contact"), ""),
                        about_page_url=next((p.url for p in good_pages if p.page_type == "about"), ""),
                        team_page_url=next((p.url for p in good_pages if p.page_type == "team"), ""),
                        booking_signals_json=json.dumps([]),
                        financing_signals_json=json.dumps([]),
                        chat_widget_signals_json=json.dumps([]),
                    )
                )
                lead.public_company_email = emails[0] if emails else ""
                lead.public_company_phone = phones[0] if phones else ""
                lead.company_address = ""
                lead.contact_page_url = next((p.url for p in good_pages if p.page_type == "contact"), "")
                lead.about_page_url = next((p.url for p in good_pages if p.page_type == "about"), "")
                lead.team_page_url = next((p.url for p in good_pages if p.page_type == "team"), "")

                lead.public_email = lead.public_company_email
                lead.public_phone = lead.public_company_phone
                lead.address = lead.company_address

                lead.facebook_url = ""
                lead.instagram_url = ""
                lead.linkedin_company_url = ""

                provenance["public_company_email"] = "website_extraction"
                provenance["public_company_phone"] = "website_extraction"
                _add_debug_event(
                    db,
                    run_id=run.id,
                    lead_id=lead.id,
                    stage="contact_extraction",
                    status="ok",
                    message="Deterministic contact extraction completed",
                    payload={
                        **contact_result.to_dict(),
                        "phone_classifications": [
                            {
                                "value": item.value,
                                "source_page": item.source_page,
                                "weight": {"team": 0.9, "about": 0.7, "contact": 0.5, "homepage": 0.4}.get(item.source_page, 0.2),
                            }
                            for item in contact_result.phones
                        ],
                    },
                )
            else:
                if lead.validation_notes:
                    lead.validation_notes += "; "
                lead.validation_notes = (lead.validation_notes or "") + "Company site unavailable or blocked"

            _emit_run_event(
                db,
                run=run,
                lead_id=lead.id,
                event_type="decision_maker",
                machine_status=run.status,
                human_message="🧠 Identifying decision maker.",
            )
            decision_output = run_decision_engine(contact_result, model_name=model_to_use)
            if decision_output.llm_timed_out:
                timeout_message = "Decision-maker LLM timed out; using heuristic fallback."
                _add_debug_event(
                    db,
                    run_id=run.id,
                    lead_id=lead.id,
                    stage="decision_engine",
                    status="timeout",
                    message=timeout_message,
                    payload={"llm_error": decision_output.llm_error, "fallback_source": decision_output.source},
                )
                _emit_run_event(
                    db,
                    run=run,
                    lead_id=lead.id,
                    event_type="decision_maker",
                    machine_status=run.status,
                    human_message=timeout_message,
                    severity="warning",
                    payload={"reason": decision_output.llm_error},
                )
            _emit_run_event(
                db,
                run=run,
                lead_id=lead.id,
                event_type="phone_match",
                machine_status=run.status,
                human_message="📞 Matching phone.",
            )
            scored_output = build_lead_output(
                company_name=lead.company_name or "",
                website=lead.website or "",
                decision_output=decision_output,
                general_phone=lead.public_company_phone or lead.phone or "",
            )
            lead.full_name = scored_output["decision_maker_name"] or lead.full_name
            lead.normalized_full_name = scored_output["decision_maker_name"] or lead.normalized_full_name
            lead.title = scored_output["decision_maker_role"] or lead.title
            lead.normalized_title = scored_output["decision_maker_role"] or lead.normalized_title
            lead.email = scored_output["decision_maker_email"] or lead.email
            lead.normalized_email = scored_output["decision_maker_email"] or lead.normalized_email
            lead.phone = scored_output["decision_maker_phone"] or lead.phone
            lead.normalized_phone = scored_output["decision_maker_phone"] or lead.normalized_phone
            lead.person_match_confidence = float(scored_output["confidence_score"])
            lead.company_match_confidence = float(scored_output["confidence_score"])
            lead.enrichment_confidence = float(scored_output["confidence_score"])
            lead.extraction_confidence = lead.enrichment_confidence
            lead.lead_quality_score = int(float(scored_output["confidence_score"]) * 100)
            lead.fit_score = lead.lead_quality_score
            lead.semantic_row_json = json.dumps(scored_output)
            lead.likely_decision_maker_names_json = json.dumps(
                [scored_output["decision_maker_name"]] if scored_output["decision_maker_name"] else []
            )
            lead.validation_notes = (
                f"{lead.validation_notes}; source={scored_output['source']}"
                if lead.validation_notes
                else f"source={scored_output['source']}"
            )

            _add_debug_event(
                db,
                run_id=run.id,
                lead_id=lead.id,
                stage="decision_engine",
                status="ok",
                message="Decision maker selection completed",
                payload={
                    "llm_input": decision_output.llm_input,
                    "llm_output": decision_output.llm_output,
                    "result": scored_output,
                },
            )

            if person_name_found:
                lead.validation_notes = f"{lead.validation_notes}; person name found on team/about page".strip("; ")
            elif good_pages and lead.full_name:
                lead.validation_notes = f"{lead.validation_notes}; company site found but person not found".strip("; ")

            lead.outreach_angle = _build_outreach_angle(lead)
            if resolution.resolution_status == "resolved":
                provenance["resolved_website"] = "resolution_search_or_email"
                provenance["resolved_domain"] = "resolution_search_or_email"
            lead.provenance_json = json.dumps(provenance)

            if anchor.anchor_type == "unresolved":
                lead.enrichment_status = "unresolved"
                lead.enrichment_error = "no_usable_anchor"
                run.skipped_count += 1
            else:
                lead.enrichment_status = "completed"
                run.success_count += 1

            run.processed_rows += 1
            _emit_run_event(
                db,
                run=run,
                lead_id=lead.id,
                event_type="score",
                machine_status=run.status,
                human_message="✅ Scoring lead.",
            )
            _add_debug_event(
                db,
                run_id=run.id,
                lead_id=lead.id,
                stage="score",
                status="ok",
                message="Decision-maker confidence scoring completed",
                payload={"confidence_score": scored_output["confidence_score"], "output": scored_output},
            )
            db.commit()
            db.refresh(run)
            if run.pause_requested:
                run.status = "paused"
                _emit_run_event(
                    db,
                    run=run,
                    event_type="run_state",
                    machine_status="paused",
                    human_message="Paused. Waiting for resume.",
                )
                db.commit()
                return
            logger.info("lead.processing.completed", extra_fields={"run_id": run.id, "lead_id": lead.id})
        except Exception as exc:
            lead.enrichment_status = "failed"
            lead.enrichment_error = str(exc)
            run.processed_rows += 1
            run.failed_count += 1
            _add_debug_event(db, run_id=run.id, lead_id=lead.id, stage="persist", status="failed", message=str(exc))
            _emit_run_event(
                db,
                run=run,
                lead_id=lead.id,
                event_type="lead_failed",
                machine_status=run.status,
                human_message=f"Record {lead.id} failed: {exc}",
                severity="error",
            )
            db.commit()
            logger.exception("lead.processing.failed", extra_fields={"run_id": run.id, "lead_id": lead.id})

    run.status = "completed" if run.processed_rows >= run.total_rows else "failed"
    run.completed_at = datetime.utcnow()
    _emit_run_event(
        db,
        run=run,
        event_type="run_state",
        machine_status=run.status,
        human_message="Run complete. Finalizing export." if run.status == "completed" else "Run ended early due to errors.",
        severity="error" if run.status == "failed" else "info",
    )
    db.commit()
    logger.info("enrichment.run.completed", extra_fields={"run_id": run.id, "status": run.status})


def _save_page_html(run_id: int, lead_id: int, page_type: str, html: str) -> Path:
    page_dir = Path("data/pages")
    page_dir.mkdir(parents=True, exist_ok=True)
    filename = f"run_{run_id}_lead_{lead_id}_{page_type}.html"
    path = page_dir / filename
    path.write_text(html or "", encoding="utf-8")
    return path
