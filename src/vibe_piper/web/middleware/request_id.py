"""Request ID middleware for tracing."""

import logging
import uuid
from collections.abc import Awaitable, Callable

from fastapi import Request, Response


class RequestIDMiddleware:
    """
    Middleware to add unique request IDs to all requests.

    This helps with tracing and debugging requests through the system.
    """

    def __init__(self, app: Callable) -> None:
        """
        Initialize middleware.

        Args:
            app: ASGI application
        """
        self.app = app

    async def __call__(self, scope: dict, receive: Callable, send: Callable) -> Awaitable:
        """
        Process request through middleware.

        Args:
            scope: ASGI scope dictionary
            receive: ASGI receive callable
            send: ASGI send callable
        """
        if scope["type"] == "http":
            # Generate or extract request ID
            request_id = self._get_request_id(scope)

            # Add request ID to response headers
            async def send_with_request_id(message: dict) -> None:
                if message["type"] == "http.response.start":
                    headers = dict(message.get("headers", []))
                    headers[b"X-Request-ID"] = request_id.encode()
                    message["headers"] = [(k, v) for k, v in headers.items()]
                await send(message)

            # Store request ID in state
            scope["state"] = scope.get("state", {})
            scope["state"]["request_id"] = request_id

            await self.app(scope, receive, send_with_request_id)
        else:
            await self.app(scope, receive, send)

    def _get_request_id(self, scope: dict) -> str:
        """
        Get or generate request ID.

        Args:
            scope: ASGI scope dictionary

        Returns:
            Request ID string
        """
        # Check for X-Request-ID header
        for header_name, header_value in scope.get("headers", []):
            if header_name.lower() == b"x-request-id":
                return header_value.decode()

        # Generate new request ID
        return uuid.uuid4().hex


# Alternative implementation using FastAPI Request object (easier to use)
async def request_id_middleware(request: Request, call_next: Callable) -> Response:
    """
    Add request ID to each request using FastAPI middleware pattern.

    Args:
        request: FastAPI request
        call_next: Next middleware/endpoint

    Returns:
        Response with X-Request-ID header
    """
    # Check for existing request ID
    request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex

    # Store in state for access in endpoints
    request.state.request_id = request_id

    # Log request ID
    logger = logging.getLogger(__name__)
    logger.info("Request %s: %s %s", request_id, request.method, request.url.path)

    # Process request
    response = await call_next(request)

    # Add request ID to response
    response.headers["X-Request-ID"] = request_id

    return response
