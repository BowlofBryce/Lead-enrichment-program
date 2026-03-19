from __future__ import annotations

import json
import time
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, text
from sqlalchemy.orm import Session, joinedload

from app.db import SessionLocal, get_db
from app.models import CSVParseDiagnostic, EnrichmentRun, Lead, LeadDebugEvent
from app.services.csv_utils import EXPECTED_COLUMNS, export_leads_to_csv, inspect_upload_csv, lead_to_export_row
from app.services.enrichment import process_run
from app.services.logging_utils import get_logger
from app.services.ollama_client import check_ollama_health, generate
from app.settings import settings


router = APIRouter()
templates = Jinja2Templates(directory="app/templates")
logger = get_logger(__name__)


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

    run = EnrichmentRun(filename=file.filename, status="pending", total_rows=inspection.detected_row_count, processed_rows=0)
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

    for _, row in inspection.dataframe.iterrows():
        db.add(
            Lead(
                run_id=run.id,
                original_company_name=str(row.get("company_name", "") or ""),
                original_website=str(row.get("website", "") or ""),
                original_city=str(row.get("city", "") or ""),
                original_state=str(row.get("state", "") or ""),
                original_phone=str(row.get("phone", "") or ""),
                original_email=str(row.get("email", "") or ""),
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
            "debug_mode": settings.debug_mode,
        },
    )


@router.post("/runs/{run_id}/start")
def start_run(run_id: int, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    run = db.get(EnrichmentRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    if run.status in {"processing", "completed"}:
        return RedirectResponse(url=f"/runs/{run.id}", status_code=303)
    logger.info("enrichment.run.enqueue", extra_fields={"run_id": run.id})
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
    return templates.TemplateResponse("run_detail.html", {"request": request, "run": run, "debug_mode": settings.debug_mode})


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
    rows = [lead_to_export_row(lead) for lead in leads]
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
        },
    )


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


@router.get("/debug/health")
def health_page(request: Request, db: Session = Depends(get_db)):
    db_status = "ok"
    db_error = ""
    try:
        db.execute(text("SELECT 1"))
    except Exception as exc:
        db_status = "failed"
        db_error = str(exc)
    run_count = db.query(func.count(EnrichmentRun.id)).scalar() or 0
    uploads_ok = Path("data/uploads").exists() and Path("data/uploads").is_dir()
    exports_ok = Path("data/exports").exists() and Path("data/exports").is_dir()
    ollama_health = check_ollama_health()
    return templates.TemplateResponse(
        "debug_health.html",
        {
            "request": request,
            "app_status": "ok",
            "db_status": db_status,
            "db_error": db_error,
            "ollama_health": ollama_health,
            "ollama_model": settings.ollama_model,
            "uploads_ok": uploads_ok,
            "exports_ok": exports_ok,
            "run_count": run_count,
            "debug_mode": settings.debug_mode,
        },
    )


def _json_list(value: str | None) -> list:
    if not value:
        return []
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, list) else []
    except json.JSONDecodeError:
        return []


def _json_obj(value: str | None) -> dict:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}
