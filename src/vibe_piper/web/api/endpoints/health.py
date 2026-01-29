"""Health check endpoints."""

from fastapi import APIRouter
from pydantic import BaseModel


class HealthResponse(BaseModel):
    """Health check response."""

    status: str = "ok"
    version: str
    request_id: str | None = None


router = APIRouter()


@router.get("/health", response_model=HealthResponse, tags=["health"])
async def health_check() -> HealthResponse:
    """
    Health check endpoint.

    Returns service status and version information.

    Returns:
        Health response with status and version
    """
    from vibe_piper import __version__

    return HealthResponse(
        status="ok",
        version=__version__,
    )


@router.get("/health/live", tags=["health"])
async def liveness_probe() -> dict[str, str]:
    """
    Liveness probe for Kubernetes/deployment health checks.

    Returns:
        Simple status indicating service is alive
    """
    return {"status": "alive"}


@router.get("/health/ready", tags=["health"])
async def readiness_probe() -> dict[str, str]:
    """
    Readiness probe for Kubernetes/deployment health checks.

    Returns:
        Simple status indicating service is ready to handle requests
    """
    # TODO: Check database connections, external services, etc.
    # For now, just return ready
    return {"status": "ready"}
