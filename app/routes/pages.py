from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, text
from sqlalchemy.orm import Session, joinedload

from app.db import SessionLocal, get_db
from app.models import CSVParseDiagnostic, EnrichmentRun, EnrichmentRunEvent, Lead, LeadDebugEvent
from app.services.app_config import get_ollama_timeout_config, set_ollama_timeout_seconds
from app.services.csv_utils import EXPECTED_COLUMNS, export_leads_to_csv, inspect_upload_csv, lead_to_export_row
from app.services.enrichment import process_run
from app.services.logging_utils import get_logger
from app.services.ollama_client import check_ollama_health, create_model_preset, generate, list_models, pull_model
from app.settings import settings


router = APIRouter()
templates = Jinja2Templates(directory="app/templates")
logger = get_logger(__name__)


def _load_models_state() -> dict[str, object]:
    try:
        models = list_models()
        return {
            "models": models,
            "model_names": [m.name for m in models],
            "error": "",
            "reachable": True,
        }
    except Exception as exc:
        logger.warning("ollama.models.list.failed", extra_fields={"error": str(exc)})
        return {"models": [], "model_names": [], "error": str(exc), "reachable": False}


def _run_in_background(run_id: int) -> None:
    db = SessionLocal()
    try:
        process_run(db, run_id)
    finally:
        db.close()


@router.get("/")
def home(request: Request, db: Session = Depends(get_db)):
    runs = db.query(EnrichmentRun).order_by(EnrichmentRun.created_at.desc()).limit(20).all()
    return templates.TemplateResponse("index.html", {"request": request, "runs": runs, "debug_mode": settings.debug_mode})


@router.post("/upload")
async def upload_csv(
    request: Request,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")

    logger.info("csv.upload.started", extra_fields={"filename": file.filename})
    upload_path = Path("data/uploads") / file.filename
    upload_path.parent.mkdir(parents=True, exist_ok=True)
    content = await file.read()
    upload_path.write_bytes(content)

    try:
        inspection = inspect_upload_csv(upload_path)
    except Exception as exc:
        logger.exception("csv.upload.failed", extra_fields={"filename": file.filename})
        raise HTTPException(status_code=400, detail=f"Malformed CSV: {exc}") from exc

    run = EnrichmentRun(filename=file.filename, status="queued", total_rows=inspection.detected_row_count, processed_rows=0)
    db.add(run)
    db.flush()

    db.add(
        CSVParseDiagnostic(
            run_id=run.id,
            original_headers_json=json.dumps(inspection.original_headers),
            normalized_headers_json=json.dumps(inspection.normalized_headers),
            header_mapping_json=json.dumps(inspection.header_mapping),
            detected_row_count=inspection.detected_row_count,
            preview_rows_json=json.dumps(inspection.preview_rows),
            cleaned_preview_rows_json=json.dumps(inspection.cleaned_preview_rows),
            warnings_json=json.dumps(inspection.warnings),
        )
    )

    mapping = inspection.header_mapping
    for _, row in inspection.dataframe.iterrows():
        raw = {k: str(v or "") for k, v in row.to_dict().items()}
        db.add(
            Lead(
                run_id=run.id,
                original_row_json=json.dumps(raw),
                original_company_name=raw.get(mapping.get("company_name", ""), ""),
                original_website=raw.get(mapping.get("website", ""), ""),
                original_city=raw.get(mapping.get("city", ""), ""),
                original_state=raw.get(mapping.get("state", ""), ""),
                original_phone=raw.get(mapping.get("phone", ""), ""),
                original_email=raw.get(mapping.get("email", ""), ""),
                original_address=raw.get(mapping.get("address", ""), ""),
                enrichment_status="pending",
            )
        )
    db.commit()
    logger.info(
        "csv.upload.completed",
        extra_fields={"run_id": run.id, "rows": inspection.detected_row_count, "detected_columns": inspection.normalized_headers},
    )
    return RedirectResponse(url=f"/runs/{run.id}/preview", status_code=303)


@router.get("/runs/{run_id}/preview")
def run_preview(request: Request, run_id: int, db: Session = Depends(get_db)):
    run = db.get(EnrichmentRun, run_id)
    diagnostic = db.query(CSVParseDiagnostic).filter(CSVParseDiagnostic.run_id == run_id).first()
    if not run or not diagnostic:
        raise HTTPException(status_code=404, detail="Run not found")
    mapping = _json_obj(diagnostic.header_mapping_json)
    models_state = _load_models_state()
    return templates.TemplateResponse(
        "csv_preview.html",
        {
            "request": request,
            "run": run,
            "diagnostic": diagnostic,
            "original_headers": _json_list(diagnostic.original_headers_json),
            "normalized_headers": _json_list(diagnostic.normalized_headers_json),
            "header_mapping": mapping,
            "preview_rows": _json_list(diagnostic.preview_rows_json),
            "cleaned_preview_rows": _json_list(diagnostic.cleaned_preview_rows_json),
            "warnings": _json_list(diagnostic.warnings_json),
            "found_columns": [c for c in EXPECTED_COLUMNS if mapping.get(c)],
            "missing_columns": [c for c in EXPECTED_COLUMNS if not mapping.get(c)],
            "installed_models": models_state["models"],
            "ollama_models_error": models_state["error"],
            "ollama_reachable": models_state["reachable"],
            "default_model": settings.ollama_model,
            "default_enrichment_model": settings.default_enrichment_model,
            "default_schema_inference_model": settings.default_schema_inference_model,
            "default_query_generation_model": settings.default_query_generation_model,
            "debug_mode": settings.debug_mode,
        },
    )


@router.post("/runs/{run_id}/start")
def start_run(
    run_id: int,
    background_tasks: BackgroundTasks,
    selected_model: str = Form(default=""),
    schema_inference_model: str = Form(default=""),
    query_generation_model: str = Form(default=""),
    custom_instructions: str = Form(default=""),
    db: Session = Depends(get_db),
):
    run = db.get(EnrichmentRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    if run.status in {"running", "resuming", "completed"}:
        return RedirectResponse(url=f"/runs/{run.id}", status_code=303)
    selected_model = selected_model.strip()
    custom_instructions = custom_instructions.strip()
    if selected_model or schema_inference_model or query_generation_model:
        model_state = _load_models_state()
        model_names = set(model_state["model_names"])
        for model_choice, label in [
            (selected_model.strip(), "enrichment"),
            (schema_inference_model.strip(), "schema_inference"),
            (query_generation_model.strip(), "query_generation"),
        ]:
            if model_choice and model_names and model_choice not in model_names:
                run.status = "failed"
                run.error_message = f"Selected {label} model '{model_choice}' is not installed."
                run.selected_model = selected_model.strip() or None
                run.schema_inference_model = schema_inference_model.strip() or None
                run.query_generation_model = query_generation_model.strip() or None
                run.custom_instructions = custom_instructions
                db.commit()
                return RedirectResponse(url=f"/runs/{run.id}", status_code=303)
    selected_model = selected_model.strip()
    schema_inference_model = schema_inference_model.strip()
    query_generation_model = query_generation_model.strip()
    run.selected_model = selected_model or None
    run.schema_inference_model = schema_inference_model or None
    run.query_generation_model = query_generation_model or None
    run.custom_instructions = custom_instructions or None
    run.error_message = None
    run.status = "queued"
    run.pause_requested = False
    db.commit()
    logger.info("enrichment.run.enqueue", extra_fields={"run_id": run.id})
    background_tasks.add_task(_run_in_background, run.id)
    return RedirectResponse(url=f"/runs/{run.id}", status_code=303)


@router.post("/runs/{run_id}/pause")
def pause_run(run_id: int, db: Session = Depends(get_db)):
    run = db.get(EnrichmentRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    if run.status not in {"running", "resuming"}:
        return RedirectResponse(url=f"/runs/{run.id}", status_code=303)
    run.pause_requested = True
    run.status = "paused"
    run.current_action_message = "Paused requested. Finishing current record safely."
    db.add(
        EnrichmentRunEvent(
            run_id=run.id,
            event_type="run_state",
            machine_status="paused",
            human_message="Paused requested. Finishing current record safely.",
            severity="info",
        )
    )
    db.commit()
    return RedirectResponse(url=f"/runs/{run.id}", status_code=303)


@router.post("/runs/{run_id}/resume")
def resume_run(run_id: int, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    run = db.get(EnrichmentRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    if run.status in {"running", "resuming"}:
        return RedirectResponse(url=f"/runs/{run.id}", status_code=303)
    if run.status != "paused":
        return RedirectResponse(url=f"/runs/{run.id}", status_code=303)
    run.status = "resuming"
    run.pause_requested = False
    run.current_action_message = "Resuming run from last saved progress."
    db.add(
        EnrichmentRunEvent(
            run_id=run.id,
            event_type="run_state",
            machine_status="resuming",
            human_message="Resuming run from last saved progress.",
            severity="info",
        )
    )
    db.commit()
    background_tasks.add_task(_run_in_background, run.id)
    return RedirectResponse(url=f"/runs/{run.id}", status_code=303)


@router.get("/runs/{run_id}")
def run_detail(request: Request, run_id: int, db: Session = Depends(get_db)):
    run = (
        db.query(EnrichmentRun)
        .options(
            joinedload(EnrichmentRun.leads).joinedload(Lead.extraction),
            joinedload(EnrichmentRun.leads).joinedload(Lead.classification),
            joinedload(EnrichmentRun.leads).joinedload(Lead.pages),
        )
        .filter(EnrichmentRun.id == run_id)
        .first()
    )
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    used_default_model = not bool((run.selected_model or "").strip())
    resolved_model = run.selected_model or settings.default_enrichment_model or settings.ollama_model
    models_state = _load_models_state()
    return templates.TemplateResponse(
        "run_detail.html",
        {
            "request": request,
            "run": run,
            "debug_mode": settings.debug_mode,
            "resolved_model": resolved_model,
            "used_default_model": used_default_model,
            "default_model": settings.ollama_model,
            "default_enrichment_model": settings.default_enrichment_model,
            "default_schema_inference_model": settings.default_schema_inference_model,
            "default_query_generation_model": settings.default_query_generation_model,
            "installed_models": models_state["models"],
            "ollama_reachable": models_state["reachable"],
            "ollama_models_error": models_state["error"],
        },
    )


@router.get("/api/runs/{run_id}/progress")
def run_progress_api(run_id: int, db: Session = Depends(get_db)):
    run = db.get(EnrichmentRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    leads = (
        db.query(Lead)
        .filter(Lead.run_id == run.id)
        .order_by(Lead.id.asc())
        .all()
    )
    lead_rows = [
        {
            "id": lead.id,
            "person_company": " | ".join(
                [
                    lead.normalized_full_name or lead.full_name or "",
                    lead.normalized_company_name or lead.company_name or "",
                    lead.normalized_title or lead.title or "",
                ]
            ).strip(" |"),
            "anchor_type": lead.anchor_type or "",
            "anchor_value": lead.anchor_value or "",
            "anchor_source": lead.anchor_source or "",
            "resolution_status": lead.resolution_status or "",
            "resolution_confidence": lead.resolution_confidence,
            "analysis_missing": lead.fields_missing_json or "[]",
            "analysis_suspicious": lead.fields_suspicious_json or "[]",
            "business_type": lead.business_type or "",
            "public_company_email": lead.public_company_email or "",
            "public_company_phone": lead.public_company_phone or "",
            "decision_maker_name": lead.normalized_full_name or lead.full_name or "",
            "decision_maker_role": lead.normalized_title or lead.title or "",
            "decision_maker_email": lead.normalized_email or lead.email or "",
            "decision_maker_phone": lead.normalized_phone or lead.phone or "",
            "confidence_score": lead.enrichment_confidence,
            "enrichment_confidence": lead.enrichment_confidence,
            "person_match_confidence": lead.person_match_confidence,
            "company_match_confidence": lead.company_match_confidence,
            "lead_quality_score": lead.lead_quality_score,
            "enrichment_status": lead.enrichment_status,
            "enrichment_error": lead.enrichment_error or "",
        }
        for lead in leads
    ]
    return JSONResponse(
        {
            "run_id": run.id,
            "status": run.status,
            "processed_rows": run.processed_rows,
            "total_rows": run.total_rows,
            "leads": lead_rows,
        }
    )


@router.get("/api/runs/{run_id}/live")
def run_live_api(run_id: int, db: Session = Depends(get_db)):
    run = db.get(EnrichmentRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    recent_events = (
        db.query(EnrichmentRunEvent)
        .filter(EnrichmentRunEvent.run_id == run.id)
        .order_by(EnrichmentRunEvent.created_at.desc())
        .limit(25)
        .all()
    )
    completed = max(run.processed_rows, 0)
    total = max(run.total_rows, 0)
    remaining = max(total - completed, 0)
    elapsed = 0.0
    if run.started_at:
        end_time = run.completed_at or datetime.utcnow()
        elapsed = max((end_time - run.started_at).total_seconds(), 0.0)
    rate = (completed / elapsed) if elapsed > 0 else 0.0
    eta_seconds = int(remaining / rate) if rate > 0 else None
    in_flight = (
        db.query(Lead)
        .filter(Lead.run_id == run.id, Lead.enrichment_status == "processing")
        .order_by(Lead.id.asc())
        .first()
    )
    return JSONResponse(
        {
            "run_id": run.id,
            "status": run.status,
            "current_action": run.current_action_message or "Waiting to start run.",
            "total_records": total,
            "records_completed": completed,
            "records_remaining": remaining,
            "success_count": run.success_count,
            "failed_count": run.failed_count,
            "skipped_count": run.skipped_count,
            "processing_rate_per_sec": round(rate, 2),
            "eta_seconds": eta_seconds,
            "currently_processing": in_flight.id if in_flight else None,
            "recent_events": [
                {
                    "id": evt.id,
                    "event_type": evt.event_type,
                    "machine_status": evt.machine_status,
                    "human_message": evt.human_message,
                    "severity": evt.severity,
                    "lead_id": evt.lead_id,
                    "timestamp": evt.created_at.isoformat(),
                }
                for evt in recent_events
            ],
        }
    )


@router.get("/runs/{run_id}/export")
def export_run(run_id: int, db: Session = Depends(get_db)):
    logger.info("export.started", extra_fields={"run_id": run_id})
    run = db.get(EnrichmentRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    leads = (
        db.query(Lead)
        .options(joinedload(Lead.extraction))
        .filter(Lead.run_id == run.id)
        .order_by(Lead.id.asc())
        .all()
    )
    rows = []
    for lead in leads:
        row = lead_to_export_row(lead)
        original = _json_obj(lead.original_row_json)
        row.update(original)
        rows.append(row)
    output_path = Path("data/exports") / f"run_{run.id}_enriched.csv"
    try:
        export_leads_to_csv(rows, output_path)
    except Exception as exc:
        logger.exception("export.failed", extra_fields={"run_id": run_id})
        raise HTTPException(status_code=500, detail=f"Export failed: {exc}") from exc
    logger.info("export.completed", extra_fields={"run_id": run_id, "path": str(output_path)})
    return FileResponse(path=output_path, filename=output_path.name, media_type="text/csv")


@router.get("/leads/{lead_id}")
def lead_detail(request: Request, lead_id: int, db: Session = Depends(get_db)):
    lead = (
        db.query(Lead)
        .options(
            joinedload(Lead.pages),
            joinedload(Lead.extraction),
            joinedload(Lead.classification),
            joinedload(Lead.run),
            joinedload(Lead.debug_events),
        )
        .filter(Lead.id == lead_id)
        .first()
    )
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    extraction = lead.extraction
    extraction_dict = {}
    if extraction:
        extraction_dict = {
            "emails": _json_list(extraction.emails_json),
            "phones": _json_list(extraction.phones_json),
            "social_links": _json_obj(extraction.social_links_json),
            "address_text": extraction.address_text or "",
            "contact_page_url": extraction.contact_page_url or "",
            "about_page_url": extraction.about_page_url or "",
            "team_page_url": extraction.team_page_url or "",
            "booking_signals": _json_list(extraction.booking_signals_json),
            "financing_signals": _json_list(extraction.financing_signals_json),
            "chat_widget_signals": _json_list(extraction.chat_widget_signals_json),
        }

    classification = lead.classification
    classification_dict = {}
    if classification:
        classification_dict = {
            "model_name": classification.model_name,
            "business_type": classification.business_type or "",
            "services": _json_list(classification.services_json),
            "short_summary": classification.short_summary or "",
            "likely_decision_maker_names": _json_list(classification.likely_decision_maker_names_json),
            "fit_reason": classification.fit_reason or "",
            "confidence": classification.confidence if classification.confidence is not None else "",
            "raw_response": classification.raw_response or "",
            "ollama_request_payload": _json_obj(classification.ollama_request_payload_json),
            "ollama_raw_response": classification.ollama_raw_response or "",
            "ollama_parse_error": classification.ollama_parse_error or "",
        }
    return templates.TemplateResponse(
        "lead_detail.html",
        {
            "request": request,
            "lead": lead,
            "pages": lead.pages,
            "extraction": extraction_dict,
            "classification": classification_dict,
            "debug_events": sorted(lead.debug_events, key=lambda e: e.created_at),
            "debug_mode": settings.debug_mode,
            "original_row": _json_obj(lead.original_row_json),
            "present": _json_list(lead.fields_present_json),
            "missing": _json_list(lead.fields_missing_json),
            "suspicious": _json_list(lead.fields_suspicious_json),
            "provenance": _json_obj(lead.provenance_json),
        },
    )


@router.get("/leads/completed")
def completed_leads_page(request: Request, db: Session = Depends(get_db)):
    leads = (
        db.query(Lead)
        .options(joinedload(Lead.run))
        .filter(Lead.enrichment_status == "completed")
        .order_by(Lead.updated_at.desc(), Lead.id.desc())
        .all()
    )
    return templates.TemplateResponse(
        "completed_leads.html",
        {
            "request": request,
            "leads": leads,
            "completed_count": len(leads),
            "debug_mode": settings.debug_mode,
        },
    )


@router.get("/leads/completed/export")
def export_completed_leads(db: Session = Depends(get_db)):
    leads = (
        db.query(Lead)
        .options(joinedload(Lead.extraction))
        .filter(Lead.enrichment_status == "completed")
        .order_by(Lead.updated_at.desc(), Lead.id.desc())
        .all()
    )
    rows = []
    for lead in leads:
        row = lead_to_export_row(lead)
        original = _json_obj(lead.original_row_json)
        row.update(original)
        row["run_id"] = lead.run_id
        row["lead_id"] = lead.id
        rows.append(row)
    output_path = Path("data/exports") / "completed_leads_export.csv"
    export_leads_to_csv(rows, output_path)
    return FileResponse(path=output_path, filename=output_path.name, media_type="text/csv")


@router.get("/models")
def models_page(request: Request):
    models_state = _load_models_state()
    timeout_config = get_ollama_timeout_config()
    flash_message = request.query_params.get("message", "")
    flash_error = request.query_params.get("error", "")
    return templates.TemplateResponse(
        "models.html",
        {
            "request": request,
            "installed_models": models_state["models"],
            "ollama_models_error": models_state["error"],
            "ollama_reachable": models_state["reachable"],
            "default_model": settings.ollama_model,
            "flash_message": flash_message,
            "flash_error": flash_error,
            "ollama_timeout_seconds": timeout_config.seconds,
            "ollama_timeout_source": timeout_config.source,
            "debug_mode": settings.debug_mode,
        },
    )


@router.post("/settings/ollama-timeout")
def update_ollama_timeout(
    ollama_timeout_seconds: str = Form(...),
    db: Session = Depends(get_db),
):
    raw_value = ollama_timeout_seconds.strip()
    try:
        timeout_seconds = int(raw_value)
    except ValueError:
        return RedirectResponse(url="/models?error=Ollama+timeout+must+be+a+positive+integer", status_code=303)
    if timeout_seconds <= 0:
        return RedirectResponse(url="/models?error=Ollama+timeout+must+be+greater+than+zero", status_code=303)
    set_ollama_timeout_seconds(db, timeout_seconds)
    logger.info("settings.ollama_timeout.updated", extra_fields={"ollama_timeout_seconds": timeout_seconds, "source": "ui"})
    return RedirectResponse(url=f"/models?message={quote_plus(f'Ollama timeout updated to {timeout_seconds} seconds')}", status_code=303)


@router.post("/models/pull")
def pull_model_route(model_name: str = Form(...)):
    name = model_name.strip()
    if not name:
        return RedirectResponse(url="/models?error=Model+name+is+required", status_code=303)
    try:
        pull_model(name)
        logger.info("ollama.model.pull.success", extra_fields={"model_name": name})
        return RedirectResponse(url=f"/models?message={quote_plus(f'Pull completed for {name}')}", status_code=303)
    except Exception as exc:
        logger.exception("ollama.model.pull.failed", extra_fields={"model_name": name})
        return RedirectResponse(url=f"/models?error={quote_plus(f'Pull failed for {name}: {exc}')}", status_code=303)


@router.post("/models/create-preset")
def create_preset_route(
    base_model: str = Form(...),
    preset_name: str = Form(...),
    system_prompt: str = Form(default=""),
):
    base_model = base_model.strip()
    preset_name = preset_name.strip()
    system_prompt = system_prompt.strip()
    if not base_model or not preset_name or not system_prompt:
        return RedirectResponse(url="/models?error=Base+model%2C+preset+name%2C+and+system+prompt+are+required", status_code=303)
    model_state = _load_models_state()
    model_names = set(model_state["model_names"])
    if model_names and base_model not in model_names:
        return RedirectResponse(url=f"/models?error={quote_plus(f'Base model not installed: {base_model}')}", status_code=303)
    try:
        create_model_preset(base_model=base_model, preset_name=preset_name, system_prompt=system_prompt)
        logger.info("ollama.model.create_preset.success", extra_fields={"base_model": base_model, "preset_name": preset_name})
        return RedirectResponse(url=f"/models?message={quote_plus(f'Preset created: {preset_name}')}", status_code=303)
    except Exception as exc:
        logger.exception("ollama.model.create_preset.failed", extra_fields={"base_model": base_model, "preset_name": preset_name})
        return RedirectResponse(url=f"/models?error={quote_plus(f'Create preset failed: {exc}')}", status_code=303)


@router.get("/debug/llm")
def llm_debug_page(request: Request):
    return templates.TemplateResponse(
        "debug_llm.html",
        {
            "request": request,
            "result": None,
            "health": check_ollama_health(),
            "ollama_base_url": settings.ollama_base_url,
            "ollama_model": settings.ollama_model,
            "debug_mode": settings.debug_mode,
            "form_values": {
                "prompt": "say hello",
                "system": "",
                "temperature": 0.1,
                "max_tokens": 256,
                "expect_json": False,
            },
        },
    )


@router.post("/debug/llm/test")
def llm_debug_test(
    request: Request,
    action: str = Form(...),
    prompt: str = Form(default="say hello"),
    system: str = Form(default=""),
    temperature: float = Form(default=0.1),
    max_tokens: int = Form(default=256),
    expect_json: bool = Form(default=False),
):
    result: dict[str, object] = {}
    if action == "connection":
        result = {"type": "connection", "data": check_ollama_health()}
    else:
        start = time.perf_counter()
        reply = generate(
            prompt=prompt,
            system=system,
            temperature=temperature,
            max_tokens=max_tokens,
            expect_json=expect_json,
            stage="debug_llm",
        )
        duration_ms = int((time.perf_counter() - start) * 1000)
        result = {
            "type": "prompt",
            "ok": reply.ok,
            "duration_ms": duration_ms,
            "error": reply.error,
            "raw_response": reply.raw_text,
            "parsed_response": reply.data,
            "parse_error": reply.parse_error,
            "request_payload": reply.request_payload,
            "raw_payload": reply.raw_payload,
            "expect_json": expect_json,
            "timed_out": reply.error == "ollama_timeout",
        }
    return templates.TemplateResponse(
        "debug_llm.html",
        {
            "request": request,
            "result": result,
            "health": check_ollama_health(),
            "ollama_base_url": settings.ollama_base_url,
            "ollama_model": settings.ollama_model,
            "debug_mode": settings.debug_mode,
            "form_values": {"prompt": prompt, "system": system, "temperature": temperature, "max_tokens": max_tokens, "expect_json": expect_json},
        },
    )


@router.get("/debug/enrichment")
def enrichment_debug_page(
    request: Request,
    run_id: int | None = None,
    lead_id: int | None = None,
    db: Session = Depends(get_db),
):
    query = db.query(Lead).options(joinedload(Lead.extraction), joinedload(Lead.debug_events))
    if lead_id:
        query = query.filter(Lead.id == lead_id)
    elif run_id:
        query = query.filter(Lead.run_id == run_id).order_by(Lead.id.desc())
    else:
        query = query.order_by(Lead.id.desc())
    lead = query.first()
    payload: dict[str, object] = {}
    if lead:
        extraction = lead.extraction
        contact_payload = _debug_events_payload(lead.debug_events, "contact_extraction")
        name_items: list[dict[str, object]] = []
        if isinstance(contact_payload, dict):
            items = contact_payload.get("items", [])
            if isinstance(items, list):
                name_items = [item for item in items if isinstance(item, dict) and item.get("type") == "name"]
        payload = {
            "lead_id": lead.id,
            "run_id": lead.run_id,
            "extracted_raw_contacts": {
                "names": name_items,
                "emails": _json_list(extraction.emails_json) if extraction else [],
                "phones": _json_list(extraction.phones_json) if extraction else [],
            },
            "phone_classifications": contact_payload,
            "llm_input": _debug_events_payload(lead.debug_events, "decision_engine", "llm_input"),
            "llm_output": _debug_events_payload(lead.debug_events, "decision_engine", "llm_output"),
            "final_scored_result": _json_obj(lead.semantic_row_json),
        }
    return templates.TemplateResponse(
        "debug_enrichment.html",
        {"request": request, "lead": lead, "payload": payload, "run_id": run_id or "", "lead_id": lead_id or ""},
    )


@router.get("/debug/health")
def health_page(request: Request, db: Session = Depends(get_db)):
    db_status = "ok"
    db_error = ""
    run_count = 0
    lead_count = 0
    event_count = 0
    try:
        db.execute(text("SELECT 1"))
        run_count = db.query(func.count(EnrichmentRun.id)).scalar() or 0
        lead_count = db.query(func.count(Lead.id)).scalar() or 0
        event_count = db.query(func.count(LeadDebugEvent.id)).scalar() or 0
    except Exception as exc:
        db_status = "failed"
        db_error = str(exc)

    uploads_dir = Path("data/uploads")
    exports_dir = Path("data/exports")
    pages_dir = Path("data/pages")

    try:
        ollama_health = check_ollama_health()
    except Exception as exc:
        ollama_health = {"reachable": False, "model_available": False, "error": str(exc), "models": []}
    timeout_config = get_ollama_timeout_config(db)

    return templates.TemplateResponse(
        "debug_health.html",
        {
            "request": request,
            "app_status": "ok",
            "debug_mode": settings.debug_mode,
            "db_status": db_status,
            "db_error": db_error,
            "run_count": run_count,
            "lead_count": lead_count,
            "event_count": event_count,
            "ollama_health": ollama_health,
            "ollama_model": settings.ollama_model,
            "ollama_timeout_seconds": timeout_config.seconds,
            "ollama_timeout_source": timeout_config.source,
            "uploads_dir": str(uploads_dir.resolve()),
            "uploads_ok": uploads_dir.exists(),
            "exports_dir": str(exports_dir.resolve()),
            "exports_ok": exports_dir.exists(),
            "pages_dir": str(pages_dir.resolve()),
            "pages_ok": pages_dir.exists(),
        },
    )


def _json_list(value: str | None) -> list:
    if not value:
        return []
    try:
        data = json.loads(value)
        return data if isinstance(data, list) else []
    except json.JSONDecodeError:
        return []


def _json_obj(value: str | None) -> dict:
    if not value:
        return {}
    try:
        data = json.loads(value)
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        return {}


def _debug_events_payload(events: list[LeadDebugEvent], stage: str, key: str | None = None) -> object:
    matches = [event for event in events if event.stage == stage]
    if not matches:
        return {}
    payload = _json_obj(matches[-1].payload_json)
    if key and isinstance(payload, dict):
        return payload.get(key, {})
    return payload
