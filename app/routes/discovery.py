from __future__ import annotations

import json
from datetime import datetime

from fastapi import APIRouter, BackgroundTasks, Depends, Form, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.db import SessionLocal, get_db
from app.models import DiscoveryEvent, DiscoveryRun
from app.services.lead_discovery.pipeline import process_discovery_run


router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


def _run_discovery_bg(run_id: int) -> None:
    db = SessionLocal()
    try:
        process_discovery_run(db, run_id)
    finally:
        db.close()


@router.get("/discovery")
def discovery_home(request: Request, db: Session = Depends(get_db)):
    runs = db.query(DiscoveryRun).order_by(DiscoveryRun.created_at.desc()).limit(20).all()
    return templates.TemplateResponse("discovery_index.html", {"request": request, "runs": runs})


@router.post("/discovery/start")
def discovery_start(
    background_tasks: BackgroundTasks,
    categories: str = Form(...),
    locations: str = Form(default="UT,ID,NV,AZ,CO,WY"),
    use_llm: bool = Form(default=True),
    max_retries: int = Form(default=2),
    query_model: str = Form(default=""),
    db: Session = Depends(get_db),
):
    category_list = [c.strip() for c in categories.split(",") if c.strip()]
    if not category_list:
        raise HTTPException(status_code=400, detail="At least one category is required")
    location_list = [c.strip() for c in locations.split(",") if c.strip()]
    run = DiscoveryRun(
        status="queued",
        categories_json=json.dumps(category_list),
        locations_json=json.dumps(location_list),
        use_llm_query_expansion=use_llm,
        max_retries=max(0, min(5, max_retries)),
        query_model=query_model.strip() or None,
    )
    db.add(run)
    db.commit()
    background_tasks.add_task(_run_discovery_bg, run.id)
    return RedirectResponse(url=f"/discovery/runs/{run.id}", status_code=303)


@router.get("/discovery/runs/{run_id}")
def discovery_detail(run_id: int, request: Request, db: Session = Depends(get_db)):
    run = db.get(DiscoveryRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Discovery run not found")
    return templates.TemplateResponse("discovery_run_detail.html", {"request": request, "run": run})


@router.post("/discovery/runs/{run_id}/pause")
def discovery_pause(run_id: int, db: Session = Depends(get_db)):
    run = db.get(DiscoveryRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Discovery run not found")
    run.pause_requested = True
    run.status = "paused"
    run.current_action_message = "Paused. Waiting for resume."
    db.add(
        DiscoveryEvent(
            run_id=run.id,
            stage="run_state",
            event_type="paused",
            human_message="Paused. Waiting for resume.",
            severity="info",
        )
    )
    db.commit()
    return RedirectResponse(url=f"/discovery/runs/{run.id}", status_code=303)


@router.post("/discovery/runs/{run_id}/resume")
def discovery_resume(run_id: int, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    run = db.get(DiscoveryRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Discovery run not found")
    run.pause_requested = False
    run.status = "resuming"
    run.current_action_message = "Resuming discovery run from saved progress."
    db.add(
        DiscoveryEvent(
            run_id=run.id,
            stage="run_state",
            event_type="resuming",
            human_message="Resuming discovery run from saved progress.",
            severity="info",
        )
    )
    db.commit()
    background_tasks.add_task(_run_discovery_bg, run.id)
    return RedirectResponse(url=f"/discovery/runs/{run.id}", status_code=303)


@router.get("/api/discovery/runs/{run_id}/live")
def discovery_live(run_id: int, db: Session = Depends(get_db)):
    run = db.get(DiscoveryRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Discovery run not found")

    recent_events = (
        db.query(DiscoveryEvent)
        .filter(DiscoveryEvent.run_id == run.id)
        .order_by(DiscoveryEvent.created_at.desc())
        .limit(50)
        .all()
    )

    total_queries = max(run.total_queries, 0)
    processed = max(run.processed_queries, 0)
    remaining = max(total_queries - processed, 0)

    elapsed = 0.0
    if run.started_at:
        elapsed = max(((run.completed_at or datetime.utcnow()) - run.started_at).total_seconds(), 0.0)
    rate = processed / elapsed if elapsed > 0 else 0.0
    eta = int(remaining / rate) if rate > 0 else None

    return JSONResponse(
        {
            "run_id": run.id,
            "status": run.status,
            "current_action": run.current_action_message or "Waiting to start.",
            "total_queries": total_queries,
            "processed_queries": processed,
            "remaining_queries": remaining,
            "total_raw_leads": run.total_raw_leads,
            "total_leads_found": run.total_raw_leads,
            "deduplicated_count": run.deduplicated_count,
            "duplicates_removed": run.deduplicated_count,
            "valid_count": run.valid_count,
            "filtered_count": run.filtered_count,
            "enrichment_queued_count": run.enrichment_queued_count,
            "leads_per_source": json.loads(run.leads_per_source_json or "{}"),
            "processing_rate_per_sec": round(rate, 2),
            "eta_seconds": eta,
            "enrichment_run_id": run.enrichment_run_id,
            "recent_events": [
                {
                    "id": evt.id,
                    "stage": evt.stage,
                    "event_type": evt.event_type,
                    "human_message": evt.human_message,
                    "severity": evt.severity,
                    "timestamp": evt.created_at.isoformat(),
                }
                for evt in recent_events
            ],
        }
    )
