from __future__ import annotations

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.db import Base, engine, ensure_data_dirs, run_sqlite_migrations
from app.routes import router
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
    logger.info(
        "app.startup",
        extra_fields={
            "app_name": settings.app_name,
            "debug_mode": settings.debug_mode,
            "ollama_model": settings.ollama_model,
        },
    )
