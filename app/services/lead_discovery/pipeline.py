from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import requests
from sqlalchemy.orm import Session

from app.models import CSVParseDiagnostic, DiscoveryEvent, DiscoveryLead, DiscoveryRun, EnrichmentRun, Lead
from app.services.enrichment import process_run
from app.services.lead_discovery.dedupe import DedupeState, choose_best
from app.services.lead_discovery.normalization import website_domain
from app.services.lead_discovery.parsing import (
    normalized_from_discovery_row,
    parse_raw_business,
    to_normalized,
)
from app.services.lead_discovery.query_generator import generate_discovery_queries_with_stats
from app.services.lead_discovery.sources import SourceAdapter, build_enabled_sources, merge_order_index
from app.services.lead_discovery.types import DiscoveryQuery, NormalizedLead
from app.services.lead_discovery.validation import validate_lead
from app.services.logging_utils import get_logger
from app.settings import settings


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


def _retry_fetch(source: SourceAdapter, query: DiscoveryQuery, retries: int):
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return source.fetch(query)
        except requests.RequestException as exc:
            last_exc = exc
            if attempt >= retries:
                raise
            delay = settings.discovery_retry_backoff_seconds * (attempt + 1)
            time.sleep(delay)
        except Exception:
            raise
    raise last_exc  # pragma: no cover


def _human_message_for_source(source_name: str, query: DiscoveryQuery) -> str:
    kw = query.keyword_variant or query.category
    if source_name == "duckduckgo_html":
        return f"DuckDuckGo HTML search for '{kw} in {query.city}, {query.state}'"
    return f"Fetching {source_name} for '{query.query}'"


def _fetch_sources_parallel(sources: list[SourceAdapter], query: DiscoveryQuery, retries: int) -> tuple[dict[str, list], dict[str, str]]:
    if not sources:
        return {}, {}
    max_workers = max(1, min(len(sources), settings.discovery_parallel_workers))
    results: dict[str, list] = {}
    errors: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_retry_fetch, src, query, retries): src for src in sources}
        for fut in as_completed(futures):
            src = futures[fut]
            try:
                results[src.name] = fut.result()
            except Exception as exc:
                logger.warning("source_fetch_failed", extra={"source": src.name, "error": str(exc)})
                results[src.name] = []
                errors[src.name] = str(exc)
    return results, errors


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
    _emit(
        db,
        run,
        stage="query_generation",
        event_type="stage",
        message=f"Generating structured queries for categories {categories} across locations {locations}",
    )
    queries, query_stats = generate_discovery_queries_with_stats(
        categories,
        locations,
        use_llm=run.use_llm_query_expansion,
        model_name=run.query_model,
    )
    run.total_queries = len(queries)
    logger.info(
        "lead_discovery_queries_planned",
        extra={
            "run_id": run.id,
            "total_queries_planned": query_stats.total_structured_planned,
            "queries_skipped_exact_dedupe": query_stats.skipped_exact_dedupe,
            "queries_skipped_semantic_dedupe": query_stats.skipped_semantic_dedupe,
            "final_queries": query_stats.total_final,
        },
    )
    _emit(
        db,
        run,
        stage="query_generation",
        event_type="query_metrics",
        message=(
            f"Planned {query_stats.total_structured_planned} queries; skipped "
            f"{query_stats.skipped_exact_dedupe + query_stats.skipped_semantic_dedupe} duplicates; "
            f"executing {query_stats.total_final}"
        ),
        payload={
            "planned_total": query_stats.total_structured_planned,
            "skipped_exact_dedupe": query_stats.skipped_exact_dedupe,
            "skipped_semantic_dedupe": query_stats.skipped_semantic_dedupe,
            "final_total": query_stats.total_final,
        },
    )
    db.commit()

    sources = build_enabled_sources()
    if not sources:
        run.status = "failed"
        run.error_message = "No lead-discovery source enabled. Enable DuckDuckGo HTML discovery in settings."
        _emit(db, run, stage="source_fetching", event_type="error", message=run.error_message, severity="error")
        db.commit()
        return
    provider_names = [src.name for src in sources]
    logger.info("lead_discovery_active_sources", extra={"sources": provider_names, "run_id": run.id})
    _emit(
        db,
        run,
        stage="source_fetching",
        event_type="provider",
        message=f"Active discovery provider(s): {', '.join(provider_names)}",
        payload={"sources": provider_names},
    )
    db.commit()

    dedupe_state = DedupeState()
    pending_commits = 0
    full_pipeline_mode = bool(run.full_pipeline_mode)

    def bump_commit() -> None:
        nonlocal pending_commits
        pending_commits += 1
        if pending_commits >= settings.discovery_batch_commit_size:
            db.commit()
            pending_commits = 0

    for query in queries:
        db.refresh(run)
        if run.pause_requested:
            run.status = "paused"
            _emit(db, run, stage="run_state", event_type="paused", message="Paused. Waiting for resume.")
            db.commit()
            return

        for src in sources:
            _emit(db, run, stage="source_fetching", event_type="status", message=_human_message_for_source(src.name, query))
        db.commit()

        source_results, source_errors = _fetch_sources_parallel(sources, query, run.max_retries)
        provider_status: dict[str, dict[str, object]] = {}
        for src in sources:
            provider_status[src.name] = {
                "disabled": bool(getattr(src, "disabled_for_run", False)),
                "success_results_before_block": int(getattr(src, "total_success_results", 0)),
            }
        _emit(
            db,
            run,
            stage="source_fetching",
            event_type="provider_status",
            message=f"Current provider status: {provider_status}",
            payload={"providers": provider_status},
        )
        for sname, err in source_errors.items():
            _emit(
                db,
                run,
                stage="source_fetching",
                event_type="retry_exhausted",
                message=f"{sname} failed: {err}",
                severity="warning",
            )
        db.commit()

        ordered_names = sorted(source_results.keys(), key=merge_order_index)
        for src_name in ordered_names:
            records = source_results.get(src_name) or []
            page_hint = ""
            if records and isinstance(records[0].payload, dict):
                pg = records[0].payload.get("search_page")
                if pg:
                    page_hint = f" — results page {pg}"
            _emit(
                db,
                run,
                stage="parsing",
                event_type="parse",
                message=f"Parsing {len(records)} businesses from {src_name.replace('_', ' ')}{page_hint}",
                payload={"source": src_name, "count": len(records)},
            )
            db.commit()

            for raw in records:
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
                    bump_commit()
                    continue

                dup_id, dup_reason = dedupe_state.find_match(normalized)
                if dup_id is not None:
                    winner_row = db.get(DiscoveryLead, dup_id)
                    if winner_row is None:
                        candidate.status = "valid"
                        lead_model = candidate.to_model()
                        db.add(lead_model)
                        db.flush()
                        dedupe_state.add_keys(lead_model.id, normalized)
                        run.valid_count += 1
                        if full_pipeline_mode:
                            _enqueue_discovery_lead_for_enrichment(db, run, lead_model)
                            if auto_start_enrichment:
                                process_run(db, run.enrichment_run_id)
                        bump_commit()
                        continue

                    winner_norm = normalized_from_discovery_row(winner_row)
                    result = choose_best(winner_norm, normalized)
                    if result.chosen.id == normalized.id:
                        db.query(DiscoveryLead).filter(DiscoveryLead.id == dup_id).update(
                            {"status": "duplicate", "filter_reason": dup_reason}
                        )
                        candidate.status = "valid"
                        lead_model = candidate.to_model()
                        db.add(lead_model)
                        db.flush()
                        dedupe_state.remove_keys(dup_id, winner_norm)
                        dedupe_state.add_keys(lead_model.id, normalized)
                        run.valid_count += 1
                        run.deduplicated_count += 1
                        if full_pipeline_mode:
                            _enqueue_discovery_lead_for_enrichment(db, run, lead_model)
                            if auto_start_enrichment:
                                process_run(db, run.enrichment_run_id)
                        _emit(
                            db,
                            run,
                            stage="deduplication",
                            event_type="dedupe",
                            message=f"Removing duplicate ({dup_reason}) — keeping richer record for {normalized.company_name}",
                            payload={"reason": dup_reason, "kept_lead_id": lead_model.id},
                        )
                        db.commit()
                    else:
                        candidate.status = "duplicate"
                        candidate.filter_reason = dup_reason
                        db.add(candidate.to_model())
                        run.deduplicated_count += 1
                        _emit(
                            db,
                            run,
                            stage="deduplication",
                            event_type="dedupe",
                            message=f"Removing duplicates — skipped {normalized.company_name} ({dup_reason})",
                            payload={"reason": dup_reason},
                        )
                        db.commit()
                    bump_commit()
                    continue

                candidate.status = "valid"
                lead_model = candidate.to_model()
                db.add(lead_model)
                db.flush()
                dedupe_state.add_keys(lead_model.id, normalized)
                run.valid_count += 1
                if full_pipeline_mode:
                    _enqueue_discovery_lead_for_enrichment(db, run, lead_model)
                    if auto_start_enrichment:
                        process_run(db, run.enrichment_run_id)
                bump_commit()

            run.leads_per_source_json = _inc_source_count(run.leads_per_source_json, src_name, len(records))
            _emit(
                db,
                run,
                stage="normalization",
                event_type="normalize",
                message=f"Extracting phone numbers and websites — normalized {len(records)} rows from {src_name}",
            )
            db.commit()

        run.processed_queries += 1
        db.commit()

    if pending_commits:
        db.commit()

    if full_pipeline_mode and run.enrichment_run_id:
        enrichment_run = db.get(EnrichmentRun, run.enrichment_run_id)
        if enrichment_run is not None:
            _reconcile_full_pipeline_queue(db, run, enrichment_run)
    else:
        _emit(
            db,
            run,
            stage="enrichment_handoff",
            event_type="handoff",
            message=f"Sending {run.valid_count} leads to enrichment pipeline",
            payload={"valid_count": run.valid_count},
        )
        enrichment_run = _queue_enrichment_from_discovery(db, run)
        run.enrichment_run_id = enrichment_run.id
        run.enrichment_queued_count = run.valid_count
    run.status = "completed"
    run.completed_at = datetime.utcnow()
    db.commit()

    if auto_start_enrichment and enrichment_run is not None:
        process_run(db, enrichment_run.id)


class NormalizedLeadProxy:
    def __init__(self, run_id: int, normalized: NormalizedLead, status: str = "valid", filter_reason: str = ""):
        self.run_id = run_id
        self.normalized = normalized
        self.status = status
        self.filter_reason = filter_reason

    @classmethod
    def from_normalized(cls, run_id: int, normalized: NormalizedLead) -> NormalizedLeadProxy:
        return cls(run_id, normalized)

    def to_model(self) -> DiscoveryLead:
        phone_val = self.normalized.phone or ""
        domain_val = website_domain(self.normalized.website) if self.normalized.website else ""
        return DiscoveryLead(
            run_id=self.run_id,
            external_id=self.normalized.id,
            company_name=self.normalized.company_name,
            website=self.normalized.website or None,
            phone=phone_val or None,
            norm_phone=phone_val or None,
            norm_domain=domain_val or None,
            city=self.normalized.city or None,
            state=self.normalized.state or None,
            address=self.normalized.address or None,
            category=self.normalized.category or None,
            source=self.normalized.source,
            source_ref=self.normalized.source_ref or None,
            raw_payload_json=json.dumps(self.normalized.raw_payload),
            status=self.status,
            filter_reason=self.filter_reason or None,
        )


def _inc_source_count(source_json: str | None, source_name: str, add: int) -> str:
    obj = json.loads(source_json or "{}")
    obj[source_name] = int(obj.get(source_name, 0)) + int(add)
    return json.dumps(obj)


def _enqueue_discovery_lead_for_enrichment(db: Session, run: DiscoveryRun, lead: DiscoveryLead) -> EnrichmentRun:
    enrichment_run = db.get(EnrichmentRun, run.enrichment_run_id) if run.enrichment_run_id else None
    if enrichment_run is None:
        enrichment_run = EnrichmentRun(
            filename=f"discovery_run_{run.id}.generated.csv",
            status="queued",
            total_rows=0,
            processed_rows=0,
            discovery_run_id=run.id,
        )
        db.add(enrichment_run)
        db.flush()
        run.enrichment_run_id = enrichment_run.id
        run.enrichment_queued_count = 0
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
                detected_row_count=0,
                preview_rows_json=json.dumps([]),
                cleaned_preview_rows_json=json.dumps([]),
                warnings_json=json.dumps([]),
            )
        )

    row = {
        "discovery_lead_id": lead.id,
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
    enrichment_run.total_rows += 1
    run.enrichment_queued_count += 1
    if enrichment_run.csv_diagnostic:
        diag = enrichment_run.csv_diagnostic
        diag.detected_row_count = enrichment_run.total_rows
        preview_rows = json.loads(diag.preview_rows_json or "[]")
        if len(preview_rows) < 20:
            preview_rows.append(
                {
                    "company_name": lead.company_name,
                    "website": lead.website,
                    "phone": lead.phone,
                    "city": lead.city,
                    "state": lead.state,
                    "address": lead.address,
                }
            )
            diag.preview_rows_json = json.dumps(preview_rows)
    if enrichment_run.status == "completed":
        enrichment_run.status = "queued"
        enrichment_run.completed_at = None
    db.commit()
    return enrichment_run


def _queue_enrichment_from_discovery(db: Session, run: DiscoveryRun) -> EnrichmentRun:
    base_q = db.query(DiscoveryLead).filter(DiscoveryLead.run_id == run.id, DiscoveryLead.status == "valid").order_by(DiscoveryLead.id)
    total = base_q.count()

    enrichment_run = EnrichmentRun(
        filename=f"discovery_run_{run.id}.generated.csv",
        status="queued",
        total_rows=total,
        processed_rows=0,
        discovery_run_id=run.id,
    )
    db.add(enrichment_run)
    db.flush()

    preview: list[dict] = []
    for lead in base_q.yield_per(500):
        if len(preview) < 20:
            preview.append(
                {
                    "company_name": lead.company_name,
                    "website": lead.website,
                    "phone": lead.phone,
                    "city": lead.city,
                    "state": lead.state,
                    "address": lead.address,
                }
            )
        row = {
            "discovery_lead_id": lead.id,
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
            detected_row_count=total,
            preview_rows_json=json.dumps(preview),
            cleaned_preview_rows_json=json.dumps([]),
            warnings_json=json.dumps([]),
        )
    )
    db.commit()
    return enrichment_run


def _reconcile_full_pipeline_queue(db: Session, run: DiscoveryRun, enrichment_run: EnrichmentRun) -> None:
    """Backfill missing valid discovery leads into an already-linked enrichment run.

    Full pipeline mode usually enqueues each valid lead as it is discovered. If that incremental
    enqueue path is interrupted, we reconcile at run completion so enrichment never depends on
    that specific step succeeding.
    """
    existing_rows = db.query(Lead).filter(Lead.run_id == enrichment_run.id).all()
    existing_discovery_ids: set[int] = set()
    for row in existing_rows:
        raw = json.loads(row.original_row_json or "{}")
        discovery_lead_id = raw.get("discovery_lead_id")
        if isinstance(discovery_lead_id, int):
            existing_discovery_ids.add(discovery_lead_id)

    valid_leads = (
        db.query(DiscoveryLead)
        .filter(DiscoveryLead.run_id == run.id, DiscoveryLead.status == "valid")
        .order_by(DiscoveryLead.id)
        .all()
    )
    missing = [lead for lead in valid_leads if lead.id not in existing_discovery_ids]
    if not missing:
        return

    for lead in missing:
        row = {
            "discovery_lead_id": lead.id,
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

    enrichment_run.total_rows += len(missing)
    run.enrichment_queued_count = enrichment_run.total_rows
    if enrichment_run.csv_diagnostic:
        diag = enrichment_run.csv_diagnostic
        diag.detected_row_count = enrichment_run.total_rows
        preview_rows = json.loads(diag.preview_rows_json or "[]")
        for lead in missing:
            if len(preview_rows) >= 20:
                break
            preview_rows.append(
                {
                    "company_name": lead.company_name,
                    "website": lead.website,
                    "phone": lead.phone,
                    "city": lead.city,
                    "state": lead.state,
                    "address": lead.address,
                }
            )
        diag.preview_rows_json = json.dumps(preview_rows)

    _emit(
        db,
        run,
        stage="enrichment_handoff",
        event_type="reconcile",
        message=f"Recovered {len(missing)} missed leads for enrichment queue",
        severity="warning",
        payload={"recovered_count": len(missing), "enrichment_run_id": enrichment_run.id},
    )
    db.commit()
