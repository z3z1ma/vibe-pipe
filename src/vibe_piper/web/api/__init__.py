"""API router for Vibe Piper web server."""

from fastapi import APIRouter

from vibe_piper.web.api.endpoints import auth, health, pipelines

api_router = APIRouter()

# Include routers for different endpoints
api_router.include_router(
    health.router,
    tags=["health"],
    prefix="",
)

api_router.include_router(
    auth.router,
    tags=["auth"],
    prefix="/auth",
)

api_router.include_router(
    pipelines.router,
    tags=["pipelines"],
    prefix="/pipelines",
)
