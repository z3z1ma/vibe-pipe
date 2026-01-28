"""
REST API Client implementation.

This module provides a comprehensive REST client with support for:
- All common HTTP methods (GET, POST, PUT, PATCH, DELETE)
- Retry logic with exponential backoff
- Rate limiting
- Authentication
- Response validation
"""

import logging
from dataclasses import dataclass
from typing import Any

import httpx

from vibe_piper.integration.auth import AuthStrategy
from vibe_piper.integration.base import APIClient, APIError, RateLimiter, RetryConfig

# =============================================================================
# Response Wrapper
# =============================================================================


@dataclass
class RESTResponse:
    """
    Wrapper for REST API responses.

    Provides convenient access to response data and metadata.
    """

    status_code: int
    """HTTP status code"""

    data: Any
    """Parsed response data"""

    headers: dict[str, str]
    """Response headers"""

    raw_response: httpx.Response
    """Underlying httpx response object"""

    @property
    def is_success(self) -> bool:
        """Check if response was successful (2xx status code)."""
        return 200 <= self.status_code < 300

    @property
    def is_error(self) -> bool:
        """Check if response was an error (4xx or 5xx status code)."""
        return not self.is_success


# =============================================================================
# REST Client
# =============================================================================


class RESTClient(APIClient):
    """
    REST API client with comprehensive features.

    Features:
    - All HTTP methods (GET, POST, PUT, PATCH, DELETE)
    - Retry logic with exponential backoff
    - Rate limiting
    - Multiple authentication strategies
    - Request/response logging
    - Error handling
    """

    def __init__(
        self,
        base_url: str,
        *,
        auth: AuthStrategy | None = None,
        timeout: float = APIClient.DEFAULT_TIMEOUT,
        retry_config: RetryConfig | None = None,
        rate_limiter: RateLimiter | None = None,
        headers: dict[str, str] | None = None,
        verify_ssl: bool = True,
        log_responses: bool = False,
        default_params: dict[str, Any] | None = None,
    ) -> None:
        """
        Initialize the REST client.

        Args:
            base_url: Base URL for all requests
            auth: Optional authentication strategy
            timeout: Request timeout in seconds
            retry_config: Retry configuration
            rate_limiter: Optional rate limiter
            headers: Default headers to include with all requests
            verify_ssl: Whether to verify SSL certificates
            log_responses: Whether to log response data
            default_params: Default query parameters for all requests
        """
        super().__init__(
            base_url=base_url,
            timeout=timeout,
            retry_config=retry_config,
            rate_limiter=rate_limiter,
            headers=headers,
            verify_ssl=verify_ssl,
            log_responses=log_responses,
        )
        self.auth = auth
        self.default_params = default_params or {}
        self._logger = logging.getLogger(self.__class__.__name__)

    async def _prepare_request(
        self,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Prepare request parameters.

        Args:
            **kwargs: Request-specific parameters

        Returns:
            Dictionary of parameters for httpx request
        """
        # Merge default params with provided params
        if "params" in kwargs and self.default_params:
            merged_params = {**self.default_params, **kwargs["params"]}
            kwargs["params"] = merged_params
        elif self.default_params:
            kwargs["params"] = self.default_params

        # Apply authentication if configured
        if self.auth:
            # Create a dummy request to apply auth
            request = httpx.Request("GET", "http://dummy", **kwargs)
            await self.auth.apply(request)

            # Extract updated headers and params
            if "Authorization" in request.headers:
                if "headers" not in kwargs:
                    kwargs["headers"] = {}
                kwargs["headers"]["Authorization"] = request.headers["Authorization"]

        return kwargs

    async def _make_request(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> httpx.Response:
        """
        Make HTTP request with retry logic.

        Args:
            method: HTTP method
            path: Request path (appended to base_url)
            **kwargs: Additional request parameters

        Returns:
            HTTP response
        """
        # Prepare request parameters
        prepared = await self._prepare_request(**kwargs)

        # Build full URL
        url = f"{path}" if path.startswith("http") else f"{path}"

        # Execute request with retry logic
        return await self._execute_request(method, url=url, **prepared)

    async def _parse_response(
        self,
        response: httpx.Response,
        return_raw: bool = False,
    ) -> RESTResponse:
        """
        Parse HTTP response into RESTResponse wrapper.

        Args:
            response: HTTP response
            return_raw: If True, return raw response text instead of parsing JSON

        Returns:
            Parsed RESTResponse

        Raises:
            APIError: If response parsing fails
        """
        try:
            if return_raw:
                data = response.text
            else:
                # Try to parse as JSON, fall back to text
                try:
                    data = response.json()
                except ValueError:
                    data = response.text

            return RESTResponse(
                status_code=response.status_code,
                data=data,
                headers=dict(response.headers),
                raw_response=response,
            )

        except Exception as e:
            raise APIError(f"Failed to parse response: {e}") from e

    # =========================================================================
    # HTTP Methods
    # =========================================================================

    async def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        return_raw: bool = False,
        **kwargs: Any,
    ) -> RESTResponse:
        """
        Perform GET request.

        Args:
            path: Request path
            params: Query parameters
            headers: Additional headers
            return_raw: If True, return raw response instead of parsed JSON
            **kwargs: Additional request parameters

        Returns:
            RESTResponse wrapper
        """
        merged_params = {**self.default_params, **(params or {})}

        response = await self._make_request(
            "GET",
            path,
            params=merged_params,
            headers=headers,
            **kwargs,
        )

        return await self._parse_response(response, return_raw=return_raw)

    async def post(
        self,
        path: str,
        data: Any = None,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        return_raw: bool = False,
        **kwargs: Any,
    ) -> RESTResponse:
        """
        Perform POST request.

        Args:
            path: Request path
            data: Request body data
            json: JSON request body
            headers: Additional headers
            return_raw: If True, return raw response instead of parsed JSON
            **kwargs: Additional request parameters

        Returns:
            RESTResponse wrapper
        """
        response = await self._make_request(
            "POST",
            path,
            data=data,
            json=json,
            headers=headers,
            **kwargs,
        )

        return await self._parse_response(response, return_raw=return_raw)

    async def put(
        self,
        path: str,
        data: Any = None,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        return_raw: bool = False,
        **kwargs: Any,
    ) -> RESTResponse:
        """
        Perform PUT request.

        Args:
            path: Request path
            data: Request body data
            json: JSON request body
            headers: Additional headers
            return_raw: If True, return raw response instead of parsed JSON
            **kwargs: Additional request parameters

        Returns:
            RESTResponse wrapper
        """
        response = await self._make_request(
            "PUT",
            path,
            data=data,
            json=json,
            headers=headers,
            **kwargs,
        )

        return await self._parse_response(response, return_raw=return_raw)

    async def patch(
        self,
        path: str,
        data: Any = None,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        return_raw: bool = False,
        **kwargs: Any,
    ) -> RESTResponse:
        """
        Perform PATCH request.

        Args:
            path: Request path
            data: Request body data
            json: JSON request body
            headers: Additional headers
            return_raw: If True, return raw response instead of parsed JSON
            **kwargs: Additional request parameters

        Returns:
            RESTResponse wrapper
        """
        response = await self._make_request(
            "PATCH",
            path,
            data=data,
            json=json,
            headers=headers,
            **kwargs,
        )

        return await self._parse_response(response, return_raw=return_raw)

    async def delete(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        return_raw: bool = False,
        **kwargs: Any,
    ) -> RESTResponse:
        """
        Perform DELETE request.

        Args:
            path: Request path
            params: Query parameters
            headers: Additional headers
            return_raw: If True, return raw response instead of parsed JSON
            **kwargs: Additional request parameters

        Returns:
            RESTResponse wrapper
        """
        merged_params = {**self.default_params, **(params or {})}

        response = await self._make_request(
            "DELETE",
            path,
            params=merged_params,
            headers=headers,
            **kwargs,
        )

        return await self._parse_response(response, return_raw=return_raw)

    # =========================================================================
    # Convenience Methods
    # =========================================================================

    async def get_json(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Perform GET request and return parsed JSON data.

        Args:
            path: Request path
            params: Query parameters
            **kwargs: Additional request parameters

        Returns:
            Parsed JSON data
        """
        response = await self.get(path, params=params, **kwargs)
        return response.data

    async def post_json(
        self,
        path: str,
        json: dict[str, Any],
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Perform POST request with JSON data and return parsed JSON response.

        Args:
            path: Request path
            json: JSON request body
            **kwargs: Additional request parameters

        Returns:
            Parsed JSON data
        """
        response = await self.post(path, json=json, **kwargs)
        return response.data
