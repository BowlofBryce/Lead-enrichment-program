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
