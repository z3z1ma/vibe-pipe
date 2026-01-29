"""
Base API Client with common functionality for all API integrations.

This module provides the foundational APIClient class that includes:
- Async HTTP session management
- Error handling and retry logic
- Rate limiting
- Response validation
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Final

import httpx

# =============================================================================
# Exceptions
# =============================================================================


class APIError(Exception):
    """Base exception for API-related errors."""

    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        response_body: str | None = None,
    ) -> None:
        self.message = message
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(self.message)


class RateLimitError(APIError):
    """Exception raised when rate limit is exceeded."""

    def __init__(
        self,
        message: str,
        retry_after: int | None = None,
        response_body: str | None = None,
    ) -> None:
        super().__init__(message, status_code=429, response_body=response_body)
        self.retry_after = retry_after


class AuthenticationError(APIError):
    """Exception raised for authentication failures."""

    def __init__(
        self,
        message: str,
        response_body: str | None = None,
    ) -> None:
        super().__init__(message, status_code=401, response_body=response_body)


class ValidationError(APIError):
    """Exception raised when response validation fails."""

    def __init__(
        self,
        message: str,
        errors: list[str] | None = None,
    ) -> None:
        super().__init__(message)
        self.errors = errors or []


# =============================================================================
# Retry Configuration
# =============================================================================


@dataclass
class RetryConfig:
    """Configuration for retry logic with exponential backoff."""

    max_attempts: int = 3
    """Maximum number of retry attempts."""

    initial_delay: float = 1.0
    """Initial delay in seconds before first retry."""

    max_delay: float = 60.0
    """Maximum delay between retries."""

    exponential_base: float = 2.0
    """Base for exponential backoff calculation."""

    jitter: bool = True
    """Add random jitter to prevent thundering herd."""

    retryable_status_codes: set[int] = field(default_factory=lambda: {408, 429, 500, 502, 503, 504})
    """HTTP status codes that trigger a retry."""

    def calculate_delay(self, attempt: int) -> float:
        """
        Calculate delay for given attempt using exponential backoff.

        Args:
            attempt: The attempt number (0-indexed)

        Returns:
            Delay in seconds
        """
        import random

        delay = min(
            self.initial_delay * (self.exponential_base**attempt),
            self.max_delay,
        )

        if self.jitter:
            delay = delay * (0.5 + random.random() * 0.5)

        return delay


# =============================================================================
# Rate Limiter
# =============================================================================


class RateLimiter:
    """
    Token bucket rate limiter to prevent API throttling.

    This implements a token bucket algorithm that allows bursts up to
    a maximum capacity while maintaining a sustained rate.
    """

    def __init__(
        self,
        max_requests: int,
        time_window_seconds: float = 1.0,
    ) -> None:
        """
        Initialize the rate limiter.

        Args:
            max_requests: Maximum number of requests allowed in time window
            time_window_seconds: Time window in seconds
        """
        self.max_requests = max_requests
        self.time_window = timedelta(seconds=time_window_seconds)
        self.tokens = max_requests
        self.last_update = datetime.now(UTC)
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """
        Acquire a token from the bucket.

        If no tokens are available, this will wait until one becomes available.
        """
        async with self._lock:
            now = datetime.now(UTC)
            elapsed = now - self.last_update

            # Refill tokens based on elapsed time
            tokens_to_add = elapsed.total_seconds() / self.time_window.total_seconds()
            self.tokens = min(self.max_requests, self.tokens + tokens_to_add)
            self.last_update = now

            if self.tokens < 1:
                # Calculate wait time needed
                wait_time = (1 - self.tokens) * self.time_window.total_seconds()
                await asyncio.sleep(wait_time)
                self.tokens = 0
            else:
                self.tokens -= 1


# =============================================================================
# Base API Client
# =============================================================================


class APIClient(ABC):
    """
    Abstract base class for all API clients.

    Provides common functionality:
    - Async HTTP session management
    - Retry logic with exponential backoff
    - Rate limiting
    - Error handling
    - Request/response logging
    """

    DEFAULT_TIMEOUT: Final = 30.0
    DEFAULT_MAX_RETRIES: Final = 3
    DEFAULT_RATE_LIMIT: Final = None  # No rate limiting by default

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = DEFAULT_TIMEOUT,
        retry_config: RetryConfig | None = None,
        rate_limiter: RateLimiter | None = None,
        headers: dict[str, str] | None = None,
        verify_ssl: bool = True,
        log_responses: bool = False,
    ) -> None:
        """
        Initialize the API client.

        Args:
            base_url: Base URL for all requests
            timeout: Request timeout in seconds
            retry_config: Retry configuration
            rate_limiter: Optional rate limiter
            headers: Default headers to include with all requests
            verify_ssl: Whether to verify SSL certificates
            log_responses: Whether to log response data
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retry_config = retry_config or RetryConfig(max_attempts=self.DEFAULT_MAX_RETRIES)
        self.rate_limiter = rate_limiter
        self.default_headers = headers or {}
        self.verify_ssl = verify_ssl
        self.log_responses = log_responses

        self._client: httpx.AsyncClient | None = None
        self._logger = logging.getLogger(self.__class__.__name__)

    async def __aenter__(self) -> "APIClient":
        """Enter async context manager and initialize HTTP client."""
        await self.initialize()
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Exit async context manager and close HTTP client."""
        await self.close()

    async def initialize(self) -> None:
        """Initialize the HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=self.timeout,
                verify=self.verify_ssl,
                headers=self.default_headers,
            )

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _get_client(self) -> httpx.AsyncClient:
        """
        Get the HTTP client, initializing if necessary.

        Returns:
            The HTTP client

        Raises:
            RuntimeError: If client is not initialized
        """
        if self._client is None:
            raise RuntimeError(
                f"{self.__class__.__name__} not initialized. "
                "Use async context manager or call initialize() first."
            )
        return self._client

    @abstractmethod
    async def _prepare_request(
        self,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Prepare request parameters. Implemented by subclasses.

        Args:
            **kwargs: Request-specific parameters

        Returns:
            Dictionary of parameters for httpx request
        """

    async def _execute_request(
        self,
        method: str,
        **kwargs: Any,
    ) -> httpx.Response:
        """
        Execute HTTP request with retry logic and rate limiting.

        Args:
            method: HTTP method (GET, POST, etc.)
            **kwargs: Request parameters

        Returns:
            HTTP response

        Raises:
            APIError: If request fails after all retries
            RateLimitError: If rate limit is exceeded
        """
        client = self._get_client()
        last_error: Exception | None = None

        for attempt in range(self.retry_config.max_attempts):
            try:
                # Rate limiting
                if self.rate_limiter is not None:
                    await self.rate_limiter.acquire()

                # Execute request
                response = await client.request(method, **kwargs)

                # Log response if enabled
                if self.log_responses:
                    self._logger.debug(
                        "Response: %s %s -> %d",
                        method,
                        kwargs.get("url", ""),
                        response.status_code,
                    )

                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    retry_after_seconds = int(retry_after) if retry_after else None

                    if attempt < self.retry_config.max_attempts - 1:
                        # Calculate wait time
                        if retry_after_seconds:
                            wait_time = float(retry_after_seconds)
                        else:
                            wait_time = self.retry_config.calculate_delay(attempt)

                        self._logger.warning(
                            "Rate limited. Waiting %.2f seconds before retry %d/%d",
                            wait_time,
                            attempt + 1,
                            self.retry_config.max_attempts,
                        )
                        await asyncio.sleep(wait_time)
                        continue

                    raise RateLimitError(
                        f"Rate limit exceeded after {self.retry_config.max_attempts} attempts",
                        retry_after=retry_after_seconds,
                        response_body=response.text,
                    )

                # Handle other retryable errors
                if response.status_code in self.retry_config.retryable_status_codes:
                    if attempt < self.retry_config.max_attempts - 1:
                        wait_time = self.retry_config.calculate_delay(attempt)
                        self._logger.warning(
                            "Request failed with status %d. Retrying in %.2f seconds (%d/%d)",
                            response.status_code,
                            wait_time,
                            attempt + 1,
                            self.retry_config.max_attempts,
                        )
                        await asyncio.sleep(wait_time)
                        continue

                # Handle authentication errors
                if response.status_code == 401:
                    raise AuthenticationError(
                        "Authentication failed",
                        response_body=response.text,
                    )

                # Raise for other error status codes
                response.raise_for_status()
                return response

            except httpx.HTTPStatusError as e:
                last_error = e
                if attempt >= self.retry_config.max_attempts - 1:
                    raise APIError(
                        f"HTTP error {e.response.status_code}: {e}",
                        status_code=e.response.status_code,
                        response_body=e.response.text,
                    ) from e
            except httpx.RequestError as e:
                last_error = e
                if attempt >= self.retry_config.max_attempts - 1:
                    raise APIError(f"Request error: {e}") from e
            except (RateLimitError, AuthenticationError):
                raise
            except Exception as e:
                last_error = e
                if attempt >= self.retry_config.max_attempts - 1:
                    raise APIError(f"Unexpected error: {e}") from e

        # Should never reach here, but just in case
        raise APIError(
            f"Request failed after {self.retry_config.max_attempts} attempts"
        ) from last_error
