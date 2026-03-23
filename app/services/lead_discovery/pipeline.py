from __future__ import annotations

import json
import time
from datetime import datetime
from dataclasses import dataclass
from sqlalchemy.orm import Session

from app.models import CSVParseDiagnostic, DiscoveryEvent, DiscoveryLead, DiscoveryRun, EnrichmentRun, Lead
from app.services.enrichment import process_run
from app.services.lead_discovery.dedupe import choose_best, is_duplicate
from app.services.lead_discovery.parsing import parse_raw_business, to_normalized
from app.services.lead_discovery.query_generator import generate_discovery_queries
from app.services.lead_discovery.sources import SourceAdapter, build_enabled_sources
from app.services.lead_discovery.validation import validate_lead
from app.services.logging_utils import get_logger


logger = get_logger(__name__)


def _emit(db: Session, run: DiscoveryRun, *, stage: str, event_type: str, message: str, severity: str = "info", payload: dict | None = None) -> None:
    run.current_action_message = message
    db.add(
        DiscoveryEvent(
            run_id=run.id,
            stage=stage,
            event_type=event_type,
            human_message=message,
            severity=severity,
            payload_json=json.dumps(payload) if payload else None,
        )
    )


def _retry_fetch(source: SourceAdapter, query, retries: int = 2, delay: float = 1.0):
    for attempt in range(retries + 1):
        try:
            return source.fetch(query)
        except Exception:
            if attempt >= retries:
                raise
            time.sleep(delay * (attempt + 1))
    return []


def process_discovery_run(db: Session, run_id: int, *, auto_start_enrichment: bool = True) -> None:
    run = db.get(DiscoveryRun, run_id)
    if not run:
        return
    run.status = "running" if run.processed_queries == 0 else "resuming"
    if not run.started_at:
        run.started_at = datetime.utcnow()
    db.commit()

    categories = json.loads(run.categories_json or "[]")
    locations = json.loads(run.locations_json or "[]")
    _emit(db, run, stage="query_generation", event_type="stage", message=f"Generating search queries for {', '.join(categories)} in {', '.join(locations)}")
    queries = generate_discovery_queries(categories, locations, use_llm=run.use_llm_query_expansion, model_name=run.query_model)
    run.total_queries = len(queries)
    db.commit()

    sources = build_enabled_sources()
    if not sources:
        run.status = "failed"
        run.error_message = "No lead-discovery sources are enabled. Configure API keys or enable OSM."
        _emit(db, run, stage="source_fetching", event_type="error", message=run.error_message, severity="error")
        db.commit()
        return

    winners: list[WinnerRecord] = []

    for query in queries:
        db.refresh(run)
        if run.pause_requested:
            run.status = "paused"
            _emit(db, run, stage="run_state", event_type="paused", message="Paused. Waiting for resume.")
            db.commit()
            return

        for source in sources:
            _emit(db, run, stage="source_fetching", event_type="fetch", message=f"Querying {source.name} for '{query.query}'")
            db.commit()
            try:
                records = _retry_fetch(source, query, retries=run.max_retries)
            except Exception as exc:
                _emit(
                    db,
                    run,
                    stage="source_fetching",
                    event_type="retry_exhausted",
                    message=f"{source.name} failed for '{query.query}': {exc}",
                    severity="error",
                )
                db.commit()
                continue

            source_count = 0
            for raw in records:
                source_count += 1
                run.total_raw_leads += 1
                parsed = parse_raw_business(raw, query)
                normalized = to_normalized(parsed, raw.payload)

                candidate = NormalizedLeadProxy.from_normalized(run.id, normalized)
                is_valid, reason = validate_lead(normalized)
                if not is_valid:
                    candidate.status = "filtered"
                    candidate.filter_reason = reason
                    run.filtered_count += 1
                    db.add(candidate.to_model())
                    continue

                duplicate_idx = -1
                duplicate_reason = ""
                for idx, winner in enumerate(winners):
                    dup, dup_reason = is_duplicate(winner.normalized, normalized)
                    if dup:
                        duplicate_idx = idx
                        duplicate_reason = dup_reason
                        break

                if duplicate_idx >= 0:
                    winner = winners[duplicate_idx]
                    result = choose_best(winner.normalized, normalized)
                    if result.chosen.id == normalized.id:
                        db.query(DiscoveryLead).filter(DiscoveryLead.id == winner.discovery_lead_id).update(
                            {"status": "duplicate", "filter_reason": duplicate_reason}
                        )
                        candidate.status = "valid"
                        lead_model = candidate.to_model()
                        db.add(lead_model)
                        db.flush()
                        winners[duplicate_idx] = WinnerRecord(normalized=normalized, discovery_lead_id=lead_model.id)
                    else:
                        candidate.status = "duplicate"
                        candidate.filter_reason = duplicate_reason
                        db.add(candidate.to_model())
                    run.deduplicated_count += 1
                else:
                    candidate.status = "valid"
                    lead_model = candidate.to_model()
                    db.add(lead_model)
                    db.flush()
                    winners.append(WinnerRecord(normalized=normalized, discovery_lead_id=lead_model.id))
                    run.valid_count += 1

            run.leads_per_source_json = _inc_source_count(run.leads_per_source_json, source.name, source_count)
            _emit(db, run, stage="parsing", event_type="parse", message=f"Parsing {source_count} businesses from {source.name} results")
            db.commit()

        run.processed_queries += 1
        db.commit()

    _emit(db, run, stage="enrichment_handoff", event_type="handoff", message=f"Sending {run.valid_count} leads to enrichment pipeline")
    enrichment_run = _queue_enrichment_from_discovery(db, run)
    run.enrichment_run_id = enrichment_run.id
    run.enrichment_queued_count = run.valid_count
    run.status = "completed"
    run.completed_at = datetime.utcnow()
    db.commit()

    if auto_start_enrichment:
        process_run(db, enrichment_run.id)


class NormalizedLeadProxy:
    def __init__(self, run_id: int, normalized, status: str = "valid", filter_reason: str = ""):
        self.run_id = run_id
        self.normalized = normalized
        self.status = status
        self.filter_reason = filter_reason

    @classmethod
    def from_normalized(cls, run_id: int, normalized):
        return cls(run_id, normalized)

    def to_model(self) -> DiscoveryLead:
        return DiscoveryLead(
            run_id=self.run_id,
            external_id=self.normalized.id,
            company_name=self.normalized.company_name,
            website=self.normalized.website,
            phone=self.normalized.phone,
            city=self.normalized.city,
            state=self.normalized.state,
            address=self.normalized.address,
            category=self.normalized.category,
            source=self.normalized.source,
            source_ref=self.normalized.source_ref,
            raw_payload_json=json.dumps(self.normalized.raw_payload),
            status=self.status,
            filter_reason=self.filter_reason,
        )


@dataclass
class WinnerRecord:
    normalized: object
    discovery_lead_id: int


def _inc_source_count(source_json: str | None, source_name: str, add: int) -> str:
    obj = json.loads(source_json or "{}")
    obj[source_name] = int(obj.get(source_name, 0)) + int(add)
    return json.dumps(obj)


def _queue_enrichment_from_discovery(db: Session, run: DiscoveryRun) -> EnrichmentRun:
    valid_leads = (
        db.query(DiscoveryLead)
        .filter(DiscoveryLead.run_id == run.id, DiscoveryLead.status == "valid")
        .order_by(DiscoveryLead.id.asc())
        .all()
    )
    enrichment_run = EnrichmentRun(
        filename=f"discovery_run_{run.id}.generated.csv",
        status="queued",
        total_rows=len(valid_leads),
        processed_rows=0,
        discovery_run_id=run.id,
    )
    db.add(enrichment_run)
    db.flush()

    db.add(
        CSVParseDiagnostic(
            run_id=enrichment_run.id,
            original_headers_json=json.dumps(["company_name", "website", "phone", "city", "state", "address"]),
            normalized_headers_json=json.dumps(["company_name", "website", "phone", "city", "state", "address"]),
            header_mapping_json=json.dumps(
                {
                    "company_name": "company_name",
                    "website": "website",
                    "phone": "phone",
                    "city": "city",
                    "state": "state",
                    "address": "address",
                }
            ),
            detected_row_count=len(valid_leads),
            preview_rows_json=json.dumps(
                [
                    {
                        "company_name": lead.company_name,
                        "website": lead.website,
                        "phone": lead.phone,
                        "city": lead.city,
                        "state": lead.state,
                        "address": lead.address,
                    }
                    for lead in valid_leads[:20]
                ]
            ),
            cleaned_preview_rows_json=json.dumps([]),
            warnings_json=json.dumps([]),
        )
    )

    for lead in valid_leads:
        row = {
            "company_name": lead.company_name,
            "website": lead.website,
            "phone": lead.phone,
            "city": lead.city,
            "state": lead.state,
            "address": lead.address,
            "discovery_source": lead.source,
            "discovery_category": lead.category,
            "discovery_run_id": run.id,
        }
        db.add(
            Lead(
                run_id=enrichment_run.id,
                original_row_json=json.dumps(row),
                original_company_name=lead.company_name,
                original_website=lead.website,
                original_city=lead.city,
                original_state=lead.state,
                original_phone=lead.phone,
                original_address=lead.address,
                enrichment_status="pending",
            )
        )
    db.commit()
    return enrichment_run
