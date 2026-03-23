from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import AppSetting
from app.services.logging_utils import get_logger
from app.settings import settings


logger = get_logger(__name__)
OLLAMA_TIMEOUT_KEY = "ollama_timeout_seconds"


@dataclass(frozen=True)
class OllamaTimeoutConfig:
    seconds: int
    source: str


def _coerce_positive_timeout(raw: str | None) -> int | None:
    if raw is None:
        return None
    clean = raw.strip()
    if not clean:
        return None
    try:
        parsed = int(clean)
    except ValueError:
        return None
    if parsed <= 0:
        return None
    return parsed


def _get_setting_value(db: Session, key: str) -> str | None:
    row = db.query(AppSetting).filter(AppSetting.setting_key == key).first()
    return row.setting_value if row else None


def get_ollama_timeout_config(db: Session | None = None) -> OllamaTimeoutConfig:
    default_timeout = settings.ollama_timeout_seconds
    managed_db = db
    close_when_done = False
    if managed_db is None:
        managed_db = SessionLocal()
        close_when_done = True

    try:
        persisted = _get_setting_value(managed_db, OLLAMA_TIMEOUT_KEY)
        parsed = _coerce_positive_timeout(persisted)
        if parsed is not None:
            return OllamaTimeoutConfig(seconds=parsed, source="database")
        if persisted is not None:
            logger.warning(
                "settings.ollama_timeout.invalid_db_value",
                extra_fields={"configured_value": persisted, "fallback_timeout": default_timeout},
            )
            return OllamaTimeoutConfig(seconds=default_timeout, source="environment_default")
        return OllamaTimeoutConfig(seconds=default_timeout, source="environment_default")
    finally:
        if close_when_done and managed_db is not None:
            managed_db.close()


def get_ollama_timeout_seconds(db: Session | None = None) -> int:
    return get_ollama_timeout_config(db).seconds


def set_ollama_timeout_seconds(db: Session, seconds: int) -> None:
    if seconds <= 0:
        raise ValueError("ollama_timeout_seconds must be a positive integer")
    row = db.query(AppSetting).filter(AppSetting.setting_key == OLLAMA_TIMEOUT_KEY).first()
    value = str(seconds)
    if row:
        row.setting_value = value
    else:
        db.add(AppSetting(setting_key=OLLAMA_TIMEOUT_KEY, setting_value=value))
    db.commit()
