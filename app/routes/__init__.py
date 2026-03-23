from fastapi import APIRouter

from app.routes.discovery import router as discovery_router
from app.routes.pages import router as pages_router

router = APIRouter()
router.include_router(pages_router)
router.include_router(discovery_router)

__all__ = ["router"]
