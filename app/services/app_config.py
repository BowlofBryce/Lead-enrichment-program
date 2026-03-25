from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models import AppSetting
from app.services.logging_utils import get_logger
from app.settings import settings


logger = get_logger(__name__)
OLLAMA_TIMEOUT_KEY = "ollama_timeout_seconds"
DISCOVERY_PROVIDER_KEY = "discovery_provider"
BRAVE_API_KEY_KEY = "brave_search_api_key"
BRAVE_BASE_URL_KEY = "brave_search_base_url"
BRAVE_TIMEOUT_SECONDS_KEY = "brave_search_timeout_seconds"
BRAVE_MAX_RESULTS_KEY = "brave_search_max_results_per_query"
BRAVE_COUNTRY_KEY = "brave_search_country"
BRAVE_SEARCH_LANG_KEY = "brave_search_search_lang"
BRAVE_FRESHNESS_KEY = "brave_search_freshness"
BRAVE_MAX_RETRIES_KEY = "brave_search_max_retries"


@dataclass(frozen=True)
class OllamaTimeoutConfig:
    seconds: int
    source: str


@dataclass(frozen=True)
class BraveSettingsConfig:
    discovery_provider: str
    brave_search_api_key: str
    brave_search_base_url: str
    brave_search_timeout_seconds: int
    brave_search_max_results_per_query: int
    brave_search_country: str
    brave_search_search_lang: str
    brave_search_freshness: str
    brave_search_max_retries: int


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


def _set_setting_value(db: Session, key: str, value: str) -> None:
    row = db.query(AppSetting).filter(AppSetting.setting_key == key).first()
    if row:
        row.setting_value = value
    else:
        db.add(AppSetting(setting_key=key, setting_value=value))


def _coerce_int(raw: str | None, *, minimum: int | None = None, maximum: int | None = None) -> int | None:
    if raw is None:
        return None
    clean = raw.strip()
    if not clean:
        return None
    try:
        parsed = int(clean)
    except ValueError:
        return None
    if minimum is not None and parsed < minimum:
        return None
    if maximum is not None and parsed > maximum:
        return None
    return parsed


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


def get_brave_settings_config(db: Session | None = None) -> BraveSettingsConfig:
    managed_db = db
    close_when_done = False
    if managed_db is None:
        managed_db = SessionLocal()
        close_when_done = True
    assert managed_db is not None
    try:
        provider = (_get_setting_value(managed_db, DISCOVERY_PROVIDER_KEY) or settings.discovery_provider).strip() or "brave"
        api_key = _get_setting_value(managed_db, BRAVE_API_KEY_KEY)
        base_url = _get_setting_value(managed_db, BRAVE_BASE_URL_KEY)
        timeout_raw = _get_setting_value(managed_db, BRAVE_TIMEOUT_SECONDS_KEY)
        max_results_raw = _get_setting_value(managed_db, BRAVE_MAX_RESULTS_KEY)
        country = _get_setting_value(managed_db, BRAVE_COUNTRY_KEY)
        search_lang = _get_setting_value(managed_db, BRAVE_SEARCH_LANG_KEY)
        freshness = _get_setting_value(managed_db, BRAVE_FRESHNESS_KEY)
        max_retries_raw = _get_setting_value(managed_db, BRAVE_MAX_RETRIES_KEY)

        timeout = _coerce_int(timeout_raw, minimum=1)
        if timeout is None:
            timeout = settings.brave_search_timeout_seconds
        max_results = _coerce_int(max_results_raw, minimum=1, maximum=20)
        if max_results is None:
            max_results = settings.brave_search_max_results_per_query
        max_retries = _coerce_int(max_retries_raw, minimum=0, maximum=5)
        if max_retries is None:
            max_retries = settings.brave_search_max_retries

        return BraveSettingsConfig(
            discovery_provider=provider,
            brave_search_api_key=(api_key if api_key is not None else settings.brave_search_api_key).strip(),
            brave_search_base_url=(base_url if base_url is not None else settings.brave_search_base_url).strip()
            or settings.brave_search_base_url,
            brave_search_timeout_seconds=timeout,
            brave_search_max_results_per_query=max_results,
            brave_search_country=(country if country is not None else settings.brave_search_country).strip() or "us",
            brave_search_search_lang=(search_lang if search_lang is not None else settings.brave_search_search_lang).strip() or "en",
            brave_search_freshness=(freshness if freshness is not None else settings.brave_search_freshness).strip(),
            brave_search_max_retries=max_retries,
        )
    finally:
        if close_when_done:
            managed_db.close()


def set_brave_settings(db: Session, values: dict[str, Any]) -> None:
    provider = str(values.get("discovery_provider", settings.discovery_provider)).strip().lower() or "brave"
    if provider not in {"brave"}:
        raise ValueError("Only 'brave' is currently supported as discovery provider")

    api_key = str(values.get("brave_search_api_key", "")).strip()
    base_url = str(values.get("brave_search_base_url", "")).strip() or settings.brave_search_base_url
    country = str(values.get("brave_search_country", "")).strip().lower() or "us"
    search_lang = str(values.get("brave_search_search_lang", "")).strip().lower() or "en"
    freshness = str(values.get("brave_search_freshness", "")).strip()

    timeout = _coerce_int(str(values.get("brave_search_timeout_seconds", "")), minimum=1)
    max_results = _coerce_int(str(values.get("brave_search_max_results_per_query", "")), minimum=1, maximum=20)
    max_retries = _coerce_int(str(values.get("brave_search_max_retries", "")), minimum=0, maximum=5)
    if timeout is None:
        raise ValueError("Brave timeout must be a positive integer")
    if max_results is None:
        raise ValueError("Brave max results per query must be between 1 and 20")
    if max_retries is None:
        raise ValueError("Brave max retries must be between 0 and 5")

    _set_setting_value(db, DISCOVERY_PROVIDER_KEY, provider)
    _set_setting_value(db, BRAVE_API_KEY_KEY, api_key)
    _set_setting_value(db, BRAVE_BASE_URL_KEY, base_url)
    _set_setting_value(db, BRAVE_TIMEOUT_SECONDS_KEY, str(timeout))
    _set_setting_value(db, BRAVE_MAX_RESULTS_KEY, str(max_results))
    _set_setting_value(db, BRAVE_COUNTRY_KEY, country)
    _set_setting_value(db, BRAVE_SEARCH_LANG_KEY, search_lang)
    _set_setting_value(db, BRAVE_FRESHNESS_KEY, freshness)
    _set_setting_value(db, BRAVE_MAX_RETRIES_KEY, str(max_retries))
    db.commit()

    settings.discovery_provider = provider
    settings.brave_search_api_key = api_key
    settings.brave_search_base_url = base_url
    settings.brave_search_timeout_seconds = timeout
    settings.brave_search_max_results_per_query = max_results
    settings.brave_search_country = country
    settings.brave_search_search_lang = search_lang
    settings.brave_search_freshness = freshness
    settings.brave_search_max_retries = max_retries
