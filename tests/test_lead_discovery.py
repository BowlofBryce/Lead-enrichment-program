from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models import Base, DiscoveryLead, DiscoveryRun, EnrichmentRun, Lead
from app.services.lead_discovery import dedupe, normalization, query_generator
from app.services.lead_discovery.pipeline import process_discovery_run
from app.services.lead_discovery.types import DiscoveryQuery, NormalizedLead, RawBusinessRecord
import app.services.lead_discovery.pipeline as pipeline


class _StubSource:
    name = "stub"

    def fetch(self, query: DiscoveryQuery):
        return [
            RawBusinessRecord(source="stub", payload={"name": "Alpha Med Spa", "website": "https://alpha.com", "city": query.city}),
            RawBusinessRecord(source="stub", payload={"name": "Alpha Medspa", "website": "https://alpha.com", "city": query.city}),
            RawBusinessRecord(source="stub", payload={"name": "Beta Clinic", "website": "example.com", "city": query.city}),
        ]


class LeadDiscoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        db_path = Path(self.tmpdir.name) / "discovery.db"
        engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False}, future=True)
        Base.metadata.create_all(bind=engine)
        self.Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

        self._orig_sources = pipeline.build_enabled_sources
        self._orig_process_run = pipeline.process_run
        pipeline.build_enabled_sources = lambda: [_StubSource()]
        pipeline.process_run = lambda db, run_id: None

    def tearDown(self) -> None:
        pipeline.build_enabled_sources = self._orig_sources
        pipeline.process_run = self._orig_process_run
        self.tmpdir.cleanup()

    def test_query_generation_produces_city_level_structured_queries(self) -> None:
        queries = query_generator.generate_discovery_queries(["MedSpa"], ["UT"], use_llm=False)
        self.assertGreater(len(queries), 0)
        self.assertTrue(any(q.city == "Provo" for q in queries))
        self.assertTrue(all(bool(q.query) for q in queries))
        self.assertTrue(all(bool(q.keyword_variant) for q in queries))

    def test_normalization_standardizes_phone_and_url(self) -> None:
        self.assertEqual(normalization.clean_phone("(801) 555-1200"), "+18015551200")
        self.assertEqual(normalization.clean_url("www.Example.com/"), "https://example.com")

    def test_deduplication_detects_fuzzy_same_city_name(self) -> None:
        a = NormalizedLead(id="1", company_name="Alpha Med Spa", website="", phone="", city="Provo", state="UT", category="MedSpa", source="x")
        b = NormalizedLead(id="2", company_name="Alpha Medspa", website="", phone="", city="Provo", state="UT", category="MedSpa", source="y")
        is_dup, reason = dedupe.is_duplicate(a, b)
        self.assertTrue(is_dup)
        self.assertEqual(reason, "fuzzy_name_city")

    def test_pipeline_end_to_end_handoff_creates_enrichment_run(self) -> None:
        with self.Session() as db:
            run = DiscoveryRun(
                status="queued",
                categories_json=json.dumps(["MedSpa"]),
                locations_json=json.dumps(["UT"]),
                use_llm_query_expansion=False,
                max_retries=1,
            )
            db.add(run)
            db.commit()
            process_discovery_run(db, run.id, auto_start_enrichment=False)
            db.refresh(run)
            self.assertEqual(run.status, "completed")
            self.assertIsNotNone(run.enrichment_run_id)
            self.assertGreaterEqual(run.total_raw_leads, 1)

            enrichment = db.get(EnrichmentRun, run.enrichment_run_id)
            self.assertIsNotNone(enrichment)
            leads = db.query(Lead).filter(Lead.run_id == enrichment.id).all()
            self.assertGreaterEqual(len(leads), 1)
            valid = db.query(DiscoveryLead).filter(DiscoveryLead.run_id == run.id, DiscoveryLead.status == "valid").count()
            self.assertEqual(valid, enrichment.total_rows)


if __name__ == "__main__":
    unittest.main()
