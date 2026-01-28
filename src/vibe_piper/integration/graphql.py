"""
GraphQL API Client implementation.

This module provides a GraphQL client with support for:
- Queries and mutations
- Error handling
- Response validation
- Retry logic
- Rate limiting
"""

import logging
from dataclasses import dataclass
from typing import Any

import httpx

from vibe_piper.integration.auth import AuthStrategy
from vibe_piper.integration.base import APIClient, APIError, RateLimiter, RetryConfig

# =============================================================================
# GraphQL Response
# =============================================================================


@dataclass
class GraphQLResponse:
    """
    Wrapper for GraphQL API responses.

    Handles the standard GraphQL response format with data and errors.
    """

    data: dict[str, Any] | None
    """Response data"""

    errors: list[dict[str, Any]] | None
    """List of GraphQL errors"""

    extensions: dict[str, Any] | None
    """GraphQL extensions (optional)"""

    raw_response: httpx.Response
    """Underlying httpx response object"""

    @property
    def is_success(self) -> bool:
        """Check if response was successful (no errors)."""
        return self.errors is None or len(self.errors) == 0

    @property
    def is_error(self) -> bool:
        """Check if response has errors."""
        return not self.is_success

    def get_first_error(self) -> dict[str, Any] | None:
        """Get the first error message if any."""
        if self.errors:
            return self.errors[0]
        return None

    def raise_on_error(self) -> None:
        """
        Raise an exception if the response has errors.

        Raises:
            APIError: If response contains GraphQL errors
        """
        if self.is_error:
            first_error = self.get_first_error()
            message = (
                first_error.get("message", "Unknown GraphQL error")
                if first_error
                else "Unknown GraphQL error"
            )
            raise APIError(f"GraphQL error: {message}")


# =============================================================================
# GraphQL Client
# =============================================================================


class GraphQLClient(APIClient):
    """
    GraphQL API client with comprehensive features.

    Features:
    - Queries and mutations
    - Variable substitution
    - Error handling
    - Retry logic with exponential backoff
    - Rate limiting
    - Multiple authentication strategies
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
    ) -> None:
        """
        Initialize the GraphQL client.

        Args:
            base_url: Base URL for the GraphQL endpoint
            auth: Optional authentication strategy
            timeout: Request timeout in seconds
            retry_config: Retry configuration
            rate_limiter: Optional rate limiter
            headers: Default headers to include with all requests
            verify_ssl: Whether to verify SSL certificates
            log_responses: Whether to log response data
        """
        # Set default GraphQL headers
        graphql_headers = {"Content-Type": "application/json"}
        if headers:
            graphql_headers.update(headers)

        super().__init__(
            base_url=base_url,
            timeout=timeout,
            retry_config=retry_config,
            rate_limiter=rate_limiter,
            headers=graphql_headers,
            verify_ssl=verify_ssl,
            log_responses=log_responses,
        )

        self.auth = auth
        self._logger = logging.getLogger(self.__class__.__name__)

    async def _prepare_request(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
        operation_name: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Prepare GraphQL request parameters.

        Args:
            query: GraphQL query or mutation
            variables: Optional variables for the query
            operation_name: Optional operation name for multiple operations
            **kwargs: Additional request parameters

        Returns:
            Dictionary of parameters for httpx request
        """
        # Build GraphQL request body
        body: dict[str, Any] = {"query": query}

        if variables:
            body["variables"] = variables

        if operation_name:
            body["operationName"] = operation_name

        # Set JSON body
        kwargs["json"] = body

        # Apply authentication if configured
        if self.auth:
            # Create a dummy request to apply auth
            request = httpx.Request("POST", "http://dummy", json=body)
            await self.auth.apply(request)

            # Extract updated headers
            if "Authorization" in request.headers:
                if "headers" not in kwargs:
                    kwargs["headers"] = {}
                kwargs["headers"]["Authorization"] = request.headers["Authorization"]

        return kwargs

    async def _parse_graphql_response(
        self,
        response: httpx.Response,
    ) -> GraphQLResponse:
        """
        Parse HTTP response into GraphQLResponse wrapper.

        Args:
            response: HTTP response

        Returns:
            Parsed GraphQLResponse

        Raises:
            APIError: If response parsing fails
        """
        try:
            response_data = response.json()

            return GraphQLResponse(
                data=response_data.get("data"),
                errors=response_data.get("errors"),
                extensions=response_data.get("extensions"),
                raw_response=response,
            )

        except Exception as e:
            raise APIError(f"Failed to parse GraphQL response: {e}") from e

    async def execute(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
        operation_name: str | None = None,
        raise_on_error: bool = False,
        **kwargs: Any,
    ) -> GraphQLResponse:
        """
        Execute GraphQL query or mutation.

        Args:
            query: GraphQL query or mutation string
            variables: Optional variables for the query
            operation_name: Optional operation name for multiple operations
            raise_on_error: If True, raise exception on GraphQL errors
            **kwargs: Additional request parameters

        Returns:
            GraphQLResponse wrapper

        Raises:
            APIError: If raise_on_error is True and response has errors
        """
        # Prepare request
        prepared = await self._prepare_request(
            query=query,
            variables=variables,
            operation_name=operation_name,
            **kwargs,
        )

        # Execute request with retry logic
        response = await self._execute_request("POST", url="", **prepared)

        # Parse response
        graphql_response = await self._parse_graphql_response(response)

        if raise_on_error:
            graphql_response.raise_on_error()

        return graphql_response

    async def query(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
        operation_name: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any] | None:
        """
        Execute GraphQL query and return data.

        This is a convenience method that automatically raises on errors.

        Args:
            query: GraphQL query string
            variables: Optional variables for the query
            operation_name: Optional operation name
            **kwargs: Additional request parameters

        Returns:
            Query data or None if no data returned

        Raises:
            APIError: If query has errors
        """
        response = await self.execute(
            query=query,
            variables=variables,
            operation_name=operation_name,
            raise_on_error=True,
            **kwargs,
        )

        return response.data

    async def mutate(
        self,
        mutation: str,
        variables: dict[str, Any] | None = None,
        operation_name: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any] | None:
        """
        Execute GraphQL mutation and return data.

        This is a convenience method for mutations that automatically raises on errors.

        Args:
            mutation: GraphQL mutation string
            variables: Variables for the mutation
            operation_name: Optional operation name
            **kwargs: Additional request parameters

        Returns:
            Mutation data or None if no data returned

        Raises:
            APIError: If mutation has errors
        """
        return await self.query(
            query=mutation,
            variables=variables,
            operation_name=operation_name,
            **kwargs,
        )

    # =========================================================================
    # Batch Operations
    # =========================================================================

    async def batch(
        self,
        requests: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[GraphQLResponse]:
        """
        Execute multiple GraphQL requests in batch.

        Note: This requires the GraphQL server to support batch requests.
        Most servers don't support this by default.

        Args:
            requests: List of request dicts with 'query' and optional 'variables'
            **kwargs: Additional request parameters

        Returns:
            List of GraphQLResponse objects
        """
        # Build batch request body
        batch_body: list[dict[str, Any]] = []

        for request in requests:
            body: dict[str, Any] = {"query": request["query"]}

            if "variables" in request:
                body["variables"] = request["variables"]

            if "operation_name" in request:
                body["operationName"] = request["operation_name"]

            batch_body.append(body)

        # Execute batch request
        kwargs["json"] = batch_body

        # Apply authentication if configured
        if self.auth:
            # Create a dummy request to apply auth
            request = httpx.Request("POST", "http://dummy", json=batch_body)
            await self.auth.apply(request)

            # Extract updated headers
            if "Authorization" in request.headers:
                if "headers" not in kwargs:
                    kwargs["headers"] = {}
                kwargs["headers"]["Authorization"] = request.headers["Authorization"]

        response = await self._execute_request("POST", url="", **kwargs)

        # Parse batch response
        try:
            response_data = response.json()

            if not isinstance(response_data, list):
                raise APIError("Batch response should be a list")

            return [
                GraphQLResponse(
                    data=item.get("data"),
                    errors=item.get("errors"),
                    extensions=item.get("extensions"),
                    raw_response=response,
                )
                for item in response_data
            ]

        except Exception as e:
            raise APIError(f"Failed to parse batch GraphQL response: {e}") from e
