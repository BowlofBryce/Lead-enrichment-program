from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session, joinedload

from app.db import SessionLocal, get_db
from app.models import EnrichmentRun, Lead
from app.services.csv_utils import export_leads_to_csv, lead_to_export_row, read_upload_csv
from app.services.enrichment import process_run


router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


def _run_in_background(run_id: int) -> None:
    db = SessionLocal()
    try:
        process_run(db, run_id)
    finally:
        db.close()


@router.get("/")
def home(request: Request, db: Session = Depends(get_db)):
    runs = db.query(EnrichmentRun).order_by(EnrichmentRun.created_at.desc()).limit(20).all()
    return templates.TemplateResponse("index.html", {"request": request, "runs": runs})


@router.post("/upload")
async def upload_csv(
    request: Request,
    file: UploadFile = File(...),
    auto_start: bool = Form(default=True),
    db: Session = Depends(get_db),
):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")

    upload_path = Path("data/uploads") / file.filename
    upload_path.parent.mkdir(parents=True, exist_ok=True)
    content = await file.read()
    upload_path.write_bytes(content)

    try:
        df = read_upload_csv(upload_path)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Malformed CSV: {exc}") from exc

    run = EnrichmentRun(filename=file.filename, status="pending", total_rows=int(len(df)), processed_rows=0)
    db.add(run)
    db.flush()

    for _, row in df.iterrows():
        lead = Lead(
            run_id=run.id,
            original_company_name=str(row.get("company_name", "") or ""),
            original_website=str(row.get("website", "") or ""),
            original_city=str(row.get("city", "") or ""),
            original_state=str(row.get("state", "") or ""),
            original_phone=str(row.get("phone", "") or ""),
            original_email=str(row.get("email", "") or ""),
            enrichment_status="pending",
        )
        db.add(lead)
    db.commit()

    if auto_start:
        return RedirectResponse(url=f"/runs/{run.id}/start", status_code=303)
    return RedirectResponse(url=f"/runs/{run.id}", status_code=303)


@router.post("/runs/{run_id}/start")
def start_run(run_id: int, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    run = db.get(EnrichmentRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    if run.status in {"processing", "completed"}:
        return RedirectResponse(url=f"/runs/{run.id}", status_code=303)
    background_tasks.add_task(_run_in_background, run.id)
    return RedirectResponse(url=f"/runs/{run.id}", status_code=303)


@router.get("/runs/{run_id}")
def run_detail(request: Request, run_id: int, db: Session = Depends(get_db)):
    run = (
        db.query(EnrichmentRun)
        .options(joinedload(EnrichmentRun.leads).joinedload(Lead.extraction))
        .filter(EnrichmentRun.id == run_id)
        .first()
    )
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return templates.TemplateResponse("run_detail.html", {"request": request, "run": run})


@router.get("/runs/{run_id}/export")
def export_run(run_id: int, db: Session = Depends(get_db)):
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
        raise HTTPException(status_code=500, detail=f"Export failed: {exc}") from exc
    return FileResponse(path=output_path, filename=output_path.name, media_type="text/csv")


@router.get("/leads/{lead_id}")
def lead_detail(request: Request, lead_id: int, db: Session = Depends(get_db)):
    lead = (
        db.query(Lead)
        .options(joinedload(Lead.pages), joinedload(Lead.extraction), joinedload(Lead.classification), joinedload(Lead.run))
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
        }
    return templates.TemplateResponse(
        "lead_detail.html",
        {
            "request": request,
            "lead": lead,
            "pages": lead.pages,
            "extraction": extraction_dict,
            "classification": classification_dict,
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
