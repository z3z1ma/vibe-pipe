"""FastAPI web server for Vibe Piper."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from vibe_piper.web.api.router import api_router
from vibe_piper.web.middleware.error_handler import add_error_handlers
from vibe_piper.web.middleware.rate_limit import RateLimitMiddleware
from vibe_piper.web.middleware.request_id import RequestIDMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events."""
    # Startup
    # Initialize database connections, caches, etc.
    yield
    # Shutdown
    # Cleanup resources, close connections, etc.


def create_app() -> FastAPI:
    """
    Create and configure the FastAPI application.

    Returns:
        Configured FastAPI application instance
    """
    app = FastAPI(
        title="Vibe Piper API",
        description="Declarative data pipeline, integration, quality, transformation, and activation API",
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )

    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173", "http://localhost:3000"],  # Default dev origins
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Add GZip middleware for response compression
    app.add_middleware(GZipMiddleware, minimum_size=1000)

    # Add custom middleware
    app.add_middleware(RequestIDMiddleware)
    app.add_middleware(RateLimitMiddleware)

    # Add error handlers
    add_error_handlers(app)

    # Include API router
    app.include_router(api_router, prefix="/api/v1")

    return app


# Create app instance for uvicorn
app = create_app()
