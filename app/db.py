from __future__ import annotations

from pathlib import Path
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from app.settings import settings

DATABASE_URL = settings.database_url


class Base(DeclarativeBase):
    pass


engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
    future=True,
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def ensure_data_dirs() -> None:
    Path("data/uploads").mkdir(parents=True, exist_ok=True)
    Path("data/exports").mkdir(parents=True, exist_ok=True)
    Path("data/pages").mkdir(parents=True, exist_ok=True)


def run_sqlite_migrations() -> None:
    if not DATABASE_URL.startswith("sqlite"):
        return
    migration_columns = {
        "lead_classifications": {
            "ollama_request_payload_json": "TEXT",
            "ollama_raw_response": "TEXT",
            "ollama_parse_error": "TEXT",
        },
        "leads": {
            "original_row_json": "TEXT",
            "first_name": "VARCHAR(120)",
            "last_name": "VARCHAR(120)",
            "full_name": "VARCHAR(255)",
            "normalized_full_name": "VARCHAR(255)",
            "title": "VARCHAR(255)",
            "normalized_title": "VARCHAR(255)",
            "company_name": "VARCHAR(255)",
            "normalized_company_name": "VARCHAR(255)",
            "email": "VARCHAR(255)",
            "normalized_email": "VARCHAR(255)",
            "email_domain": "VARCHAR(255)",
            "phone": "VARCHAR(100)",
            "company_domain": "VARCHAR(255)",
            "website": "VARCHAR(500)",
            "linkedin_url": "VARCHAR(500)",
            "location_text": "VARCHAR(255)",
            "input_address": "VARCHAR(255)",
            "anchor_type": "VARCHAR(80)",
            "anchor_value": "VARCHAR(500)",
            "anchor_reason": "TEXT",
            "anchor_source": "VARCHAR(80)",
            "fields_present_json": "TEXT",
            "fields_missing_json": "TEXT",
            "fields_suspicious_json": "TEXT",
            "provenance_json": "TEXT",
            "enrichment_confidence": "FLOAT",
            "lead_quality_score": "INTEGER",
            "person_match_confidence": "FLOAT",
            "company_match_confidence": "FLOAT",
            "validation_notes": "TEXT",
            "outreach_angle": "TEXT",
            "resolved_website": "VARCHAR(500)",
            "resolved_domain": "VARCHAR(255)",
            "resolution_method": "VARCHAR(80)",
            "resolution_confidence": "FLOAT",
            "resolution_notes": "TEXT",
            "candidate_websites_json": "TEXT",
            "resolution_status": "VARCHAR(50)",
            "original_address": "VARCHAR(255)",
            "public_company_email": "VARCHAR(255)",
            "public_company_phone": "VARCHAR(100)",
            "company_address": "VARCHAR(255)",
            "contact_page_url": "VARCHAR(500)",
            "about_page_url": "VARCHAR(500)",
            "team_page_url": "VARCHAR(500)",
            "facebook_url": "VARCHAR(500)",
            "instagram_url": "VARCHAR(500)",
            "linkedin_company_url": "VARCHAR(500)",
        },
        "csv_parse_diagnostics": {},
        "lead_debug_events": {},
    }
    with engine.begin() as conn:
        for table_name, columns in migration_columns.items():
            table_exists = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name = :table_name"),
                {"table_name": table_name},
            ).first()
            if not table_exists or not columns:
                continue
            existing_columns = {
                row[1] for row in conn.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
            }
            for column_name, column_type in columns.items():
                if column_name not in existing_columns:
                    conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"))


def get_db() -> Generator:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
