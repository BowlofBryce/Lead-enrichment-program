from __future__ import annotations

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.db import Base, engine, ensure_data_dirs
from app.routes import router
from app.settings import settings


app = FastAPI(title=settings.app_name)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(router)


@app.on_event("startup")
def on_startup() -> None:
    ensure_data_dirs()
    Base.metadata.create_all(bind=engine)
