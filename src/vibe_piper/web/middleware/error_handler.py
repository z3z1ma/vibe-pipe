"""Error handling middleware for FastAPI."""

import logging
from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field


class ErrorResponse(BaseModel):
    """Standard error response format."""

    error: str = Field(description="Error type/category")
    message: str = Field(description="Human-readable error message")
    detail: dict[str, Any] | None = Field(None, description="Additional error details")
    request_id: str | None = Field(None, description="Request identifier for tracing")


class HTTPExceptionWithDetail(Exception):
    """HTTP exception with structured details."""

    def __init__(
        self,
        status_code: int,
        error: str,
        message: str,
        detail: dict[str, Any] | None = None,
    ) -> None:
        self.status_code = status_code
        self.error = error
        self.message = message
        self.detail = detail
        super().__init__(message)


async def http_exception_handler(request: Request, exc: HTTPExceptionWithDetail) -> JSONResponse:
    """
    Handle custom HTTP exceptions.

    Args:
        request: FastAPI request
        exc: HTTPExceptionWithDetail instance

    Returns:
        JSON response with error details
    """
    request_id = getattr(request.state, "request_id", None)
    content = ErrorResponse(
        error=exc.error,
        message=exc.message,
        detail=exc.detail,
        request_id=request_id,
    )
    return JSONResponse(
        status_code=exc.status_code,
        content=content.model_dump(exclude_none=True),
    )


async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    Handle all uncaught exceptions.

    Args:
        request: FastAPI request
        exc: Uncaught exception

    Returns:
        JSON response with generic error details
    """
    logger = logging.getLogger(__name__)
    request_id = getattr(request.state, "request_id", None)

    logger.exception(
        "Unhandled exception on %s %s (request_id=%s): %s",
        request.method,
        request.url.path,
        request_id,
        str(exc),
    )

    content = ErrorResponse(
        error="internal_server_error",
        message="An unexpected error occurred",
        request_id=request_id,
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=content.model_dump(exclude_none=True),
    )


async def validation_exception_handler(request: Request, exc: ValueError) -> JSONResponse:
    """
    Handle validation errors.

    Args:
        request: FastAPI request
        exc: ValueError exception

    Returns:
        JSON response with validation error details
    """
    request_id = getattr(request.state, "request_id", None)
    content = ErrorResponse(
        error="validation_error",
        message=str(exc),
        request_id=request_id,
    )
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content=content.model_dump(exclude_none=True),
    )


def add_error_handlers(app: FastAPI) -> None:
    """
    Add all error handlers to the FastAPI application.

    Args:
        app: FastAPI application instance
    """
    app.add_exception_handler(HTTPExceptionWithDetail, http_exception_handler)
    app.add_exception_handler(ValueError, validation_exception_handler)
    app.add_exception_handler(Exception, generic_exception_handler)
