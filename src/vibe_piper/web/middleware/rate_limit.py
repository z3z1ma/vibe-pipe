"""Rate limiting middleware."""

import logging
import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from functools import lru_cache

from fastapi import HTTPException, Request, Response


class RateLimitMiddleware:
    """
    Rate limiting middleware using token bucket algorithm.

    Limits requests per client to prevent abuse.
    """

    def __init__(
        self,
        app: Callable,
        requests_per_minute: int = 60,
        requests_per_hour: int = 1000,
    ) -> None:
        """
        Initialize rate limiting middleware.

        Args:
            app: ASGI application
            requests_per_minute: Maximum requests per minute per client
            requests_per_hour: Maximum requests per hour per client
        """
        self.app = app
        self.requests_per_minute = requests_per_minute
        self.requests_per_hour = requests_per_hour
        self.logger = logging.getLogger(__name__)

    async def __call__(self, scope: dict, receive: Callable, send: Callable) -> Awaitable:
        """
        Process request through rate limiting.

        Args:
            scope: ASGI scope dictionary
            receive: ASGI receive callable
            send: ASGI send callable

        Raises:
            HTTPException: If rate limit is exceeded
        """
        if scope["type"] == "http":
            client_id = self._get_client_id(scope)

            if self._is_rate_limited(client_id):
                self.logger.warning("Rate limit exceeded for client: %s", client_id)

                # Send 429 Too Many Requests response
                await send(
                    {
                        "type": "http.response.start",
                        "status": 429,
                        "headers": [
                            [b"content-type", b"application/json"],
                            [b"retry-after", b"60"],
                        ],
                    }
                )
                await send(
                    {
                        "type": "http.response.body",
                        "body": b'{"error":"rate_limit_exceeded","message":"Too many requests","retry_after":60}',
                    }
                )
                return

        await self.app(scope, receive, send)

    def _get_client_id(self, scope: dict) -> str:
        """
        Get client identifier for rate limiting.

        Uses IP address or X-Forwarded-For header.

        Args:
            scope: ASGI scope dictionary

        Returns:
            Client identifier string
        """
        # Check for X-Forwarded-For header (for proxy setups)
        for header_name, header_value in scope.get("headers", []):
            if header_name.lower() == b"x-forwarded-for":
                # Use first IP in chain
                return header_value.decode().split(",")[0].strip()

        # Use client address
        client = scope.get("client", ("", ""))
        return client[0] or "unknown"

    def _is_rate_limited(self, client_id: str) -> bool:
        """
        Check if client has exceeded rate limit.

        Args:
            client_id: Client identifier

        Returns:
            True if rate limited, False otherwise
        """
        # Get rate limiter for client
        limiter = _RateLimiter.get_limiter(client_id)

        # Check if rate limited
        if limiter.check_minute_limit(self.requests_per_minute):
            return True

        if limiter.check_hour_limit(self.requests_per_hour):
            return True

        return False


class _RateLimiter:
    """Rate limiter using token bucket algorithm."""

    def __init__(self) -> None:
        """Initialize rate limiter."""
        self.minute_requests: list[float] = []
        self.hour_requests: list[float] = []

    def check_minute_limit(self, limit: int) -> bool:
        """
        Check if minute limit is exceeded.

        Args:
            limit: Maximum requests per minute

        Returns:
            True if limit exceeded
        """
        now = time.time()
        cutoff = now - 60

        # Clean old requests
        self.minute_requests = [t for t in self.minute_requests if t > cutoff]

        # Check limit
        if len(self.minute_requests) >= limit:
            return True

        # Add current request
        self.minute_requests.append(now)
        return False

    def check_hour_limit(self, limit: int) -> bool:
        """
        Check if hour limit is exceeded.

        Args:
            limit: Maximum requests per hour

        Returns:
            True if limit exceeded
        """
        now = time.time()
        cutoff = now - 3600

        # Clean old requests
        self.hour_requests = [t for t in self.hour_requests if t > cutoff]

        # Check limit
        if len(self.hour_requests) >= limit:
            return True

        # Add current request
        self.hour_requests.append(now)
        return False

    @staticmethod
    @lru_cache(maxsize=1000)
    def get_limiter(client_id: str) -> "_RateLimiter":
        """
        Get or create rate limiter for client.

        Args:
            client_id: Client identifier

        Returns:
            Rate limiter instance
        """
        return _RateLimiter()


# Alternative implementation using FastAPI middleware pattern
async def rate_limit_middleware(request: Request, call_next: Callable) -> Response:
    """
    Apply rate limiting using FastAPI middleware pattern.

    Args:
        request: FastAPI request
        call_next: Next middleware/endpoint

    Returns:
        Response

    Raises:
        HTTPException: If rate limit is exceeded
    """
    from fastapi import Response

    # Get client ID
    forwarded = request.headers.get("X-Forwarded-For")
    client_id = (
        forwarded.split(",")[0].strip()
        if forwarded
        else request.client.host
        if request.client
        else "unknown"
    )

    # Get rate limiter
    limiter = _RateLimiter.get_limiter(client_id)

    # Check limits
    if limiter.check_minute_limit(60):
        raise HTTPException(
            status_code=429,
            detail={
                "error": "rate_limit_exceeded",
                "message": "Too many requests per minute",
                "retry_after": 60,
            },
        )

    if limiter.check_hour_limit(1000):
        raise HTTPException(
            status_code=429,
            detail={
                "error": "rate_limit_exceeded",
                "message": "Too many requests per hour",
                "retry_after": 3600,
            },
        )

    # Process request
    return await call_next(request)
