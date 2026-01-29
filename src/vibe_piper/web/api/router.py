"""API router initialization.

This file creates the main API router and includes all endpoint routers.
"""

from fastapi import APIRouter

from vibe_piper.web.api.endpoints import auth, health, pipelines

# Create main API router
api_router = APIRouter()

# Include health check endpoints
api_router.include_router(
    health.router,
    tags=["health"],
    prefix="",
)

# Include authentication endpoints
api_router.include_router(
    auth.router,
    tags=["auth"],
    prefix="/auth",
)

# Include pipeline endpoints
api_router.include_router(
    pipelines.router,
    tags=["pipelines"],
    prefix="/pipelines",
)

# TODO: Add more routers as needed
# api_router.include_router(
#     assets.router,
#     tags=["assets"],
#     prefix="/assets",
# )
