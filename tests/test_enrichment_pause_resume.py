from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models import Base, CSVParseDiagnostic, EnrichmentRun, EnrichmentRunEvent, Lead
from app.routes.pages import run_live_api
from app.services import enrichment as enrichment_service


class EnrichmentPauseResumeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        db_path = Path(self.tmpdir.name) / "test.db"
        engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False}, future=True)
        Base.metadata.create_all(bind=engine)
        self.Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

        self._patches = {
            "infer_schema_plan": enrichment_service.infer_schema_plan,
            "transform_row_with_plan": enrichment_service.transform_row_with_plan,
            "canonicalize_row": enrichment_service.canonicalize_row,
            "analyze_row": enrichment_service.analyze_row,
            "resolve_company_website": enrichment_service.resolve_company_website,
            "resolve_anchor": enrichment_service.resolve_anchor,
            "crawl_site": enrichment_service.crawl_site,
            "extract_from_pages": enrichment_service.extract_from_pages,
            "compute_scores": enrichment_service.compute_scores,
            "dedupe_key": enrichment_service.dedupe_key,
        }
        enrichment_service.infer_schema_plan = lambda **kwargs: SimpleNamespace(plan_json={}, model_used="mock")
        enrichment_service.transform_row_with_plan = lambda raw, plan: {"canonical": {}, "semantic_values": {}}
        enrichment_service.canonicalize_row = lambda raw, mapping: SimpleNamespace(
            first_name="John",
            last_name="Smith",
            full_name="John Smith",
            normalized_full_name="john smith",
            title="Owner",
            normalized_title="owner",
            company_name="Acme",
            normalized_company_name="acme",
            email="john@acme.com",
            normalized_email="john@acme.com",
            email_domain="acme.com",
            phone="123",
            normalized_phone="123",
            company_domain="acme.com",
            website="https://acme.com",
            linkedin_url="",
            city="",
            state="",
            location_text="",
            address="",
            as_dict=lambda: {"company_name": "Acme"},
        )
        enrichment_service.analyze_row = lambda canonical: SimpleNamespace(
            fields_present=["company_name"],
            fields_missing=[],
            fields_suspicious=[],
            validation_notes=[],
        )
        enrichment_service.resolve_company_website = lambda *args, **kwargs: SimpleNamespace(
            trace=[],
            resolved_website="https://acme.com",
            resolved_domain="acme.com",
            resolution_status="resolved",
            resolution_method="mock",
            resolution_confidence=0.9,
            resolution_notes="",
            candidate_websites_json="[]",
            search_queries=[],
        )
        enrichment_service.resolve_anchor = lambda canonical: SimpleNamespace(
            anchor_type="company_domain", anchor_value="acme.com", reason="domain"
        )
        enrichment_service.crawl_site = lambda url: []
        enrichment_service.extract_from_pages = lambda pages: SimpleNamespace()
        enrichment_service.compute_scores = lambda *args, **kwargs: {
            "company_match_confidence": 0.8,
            "person_match_confidence": 0.7,
            "enrichment_confidence": 0.75,
            "lead_quality_score": 77,
        }
        enrichment_service.dedupe_key = lambda company, website: f"{company}|{website}"

    def tearDown(self) -> None:
        for name, fn in self._patches.items():
            setattr(enrichment_service, name, fn)
        self.tmpdir.cleanup()

    def _seed_run(self, status: str = "queued") -> int:
        with self.Session() as db:
            run = EnrichmentRun(filename="sample.csv", status=status, total_rows=1, processed_rows=0)
            db.add(run)
            db.flush()
            db.add(
                CSVParseDiagnostic(
                    run_id=run.id,
                    original_headers_json=json.dumps(["company_name"]),
                    normalized_headers_json=json.dumps(["company_name"]),
                    header_mapping_json=json.dumps({"company_name": "company_name"}),
                    detected_row_count=1,
                    preview_rows_json=json.dumps([{"company_name": "Acme"}]),
                    cleaned_preview_rows_json=json.dumps([{"company_name": "Acme"}]),
                    warnings_json=json.dumps([]),
                )
            )
            db.add(Lead(run_id=run.id, original_row_json=json.dumps({"company_name": "Acme"}), enrichment_status="pending"))
            db.commit()
            return run.id

    def test_pause_requested_transitions_run_to_paused(self) -> None:
        run_id = self._seed_run(status="running")
        with self.Session() as db:
            run = db.get(EnrichmentRun, run_id)
            run.pause_requested = True
            db.commit()
            enrichment_service.process_run(db, run_id)
            db.refresh(run)
            self.assertEqual(run.status, "paused")
            self.assertEqual(run.processed_rows, 0)

    def test_resume_processes_pending_rows(self) -> None:
        run_id = self._seed_run(status="paused")
        with self.Session() as db:
            run = db.get(EnrichmentRun, run_id)
            run.pause_requested = False
            db.commit()
            enrichment_service.process_run(db, run_id)
            db.refresh(run)
            self.assertEqual(run.status, "completed")
            self.assertEqual(run.processed_rows, 1)
            self.assertEqual(run.success_count, 1)

    def test_state_persists_across_session_reload(self) -> None:
        run_id = self._seed_run(status="running")
        with self.Session() as db:
            run = db.get(EnrichmentRun, run_id)
            run.pause_requested = True
            db.commit()
        with self.Session() as db:
            run = db.get(EnrichmentRun, run_id)
            self.assertTrue(run.pause_requested)

    def test_human_readable_activity_events_are_saved(self) -> None:
        run_id = self._seed_run(status="paused")
        with self.Session() as db:
            enrichment_service.process_run(db, run_id)
            events = db.query(EnrichmentRunEvent).filter(EnrichmentRunEvent.run_id == run_id).all()
            self.assertGreater(len(events), 0)
            self.assertTrue(any("record" in evt.human_message.lower() for evt in events))

    def test_live_progress_payload_contains_current_action_and_metrics(self) -> None:
        run_id = self._seed_run(status="queued")
        with self.Session() as db:
            run = db.get(EnrichmentRun, run_id)
            run.current_action_message = "Reading uploaded CSV and mapping columns."
            run.success_count = 1
            run.processed_rows = 1
            run.total_rows = 2
            db.add(
                EnrichmentRunEvent(
                    run_id=run.id,
                    event_type="schema",
                    machine_status="running",
                    human_message="Reading uploaded CSV and mapping columns.",
                    severity="info",
                )
            )
            db.commit()
            payload = json.loads(run_live_api(run_id, db).body)
            self.assertEqual(payload["current_action"], "Reading uploaded CSV and mapping columns.")
            self.assertEqual(payload["records_remaining"], 1)
            self.assertEqual(payload["success_count"], 1)


if __name__ == "__main__":
    unittest.main()
