from __future__ import annotations

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.db import Base, engine, ensure_data_dirs, run_sqlite_migrations
from app.routes import router
from app.services.app_config import get_ollama_timeout_config
from app.services.brave_search import BraveSearchClient, BraveSearchConfigurationError
from app.services.logging_utils import configure_logging, get_logger
from app.settings import settings


configure_logging(settings.debug_mode)
logger = get_logger(__name__)

app = FastAPI(title=settings.app_name)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(router)


@app.on_event("startup")
def on_startup() -> None:
    ensure_data_dirs()
    Base.metadata.create_all(bind=engine)
    run_sqlite_migrations()
    timeout_config = get_ollama_timeout_config()
    try:
        BraveSearchClient().validate_configuration()
        brave_status = "ok"
        brave_error = ""
    except BraveSearchConfigurationError as exc:
        brave_status = "invalid"
        brave_error = str(exc)
        logger.error("discovery.provider.healthcheck.failed", extra_fields={"provider": "brave", "error": brave_error})

    logger.info(
        "app.startup",
        extra_fields={
            "app_name": settings.app_name,
            "debug_mode": settings.debug_mode,
            "ollama_model": settings.ollama_model,
            "ollama_timeout_seconds": timeout_config.seconds,
            "ollama_timeout_source": timeout_config.source,
            "discovery_provider": settings.discovery_provider,
            "discovery_provider_health": brave_status,
            "discovery_provider_error": brave_error,
        },
    )
