"""
REST API Source Implementation

Provides declarative API source with:
- Auto-pagination (cursor, offset, link header)
- Auto-retry with exponential backoff
- Auto-rate limiting
- Auto-schema parsing
- Multiple authentication types (Bearer, API key, OAuth2)
"""

import asyncio
import logging
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from vibe_piper.integration.auth import (
    APIKeyAuth,
    BasicAuth,
    BearerTokenAuth,
)
from vibe_piper.integration.base import RateLimiter, RetryConfig
from vibe_piper.integration.pagination import (
    CursorPagination,
    LinkHeaderPagination,
    OffsetPagination,
    PaginationStrategy,
    fetch_all_pages,
    paginate,
)
from vibe_piper.integration.rest import RESTClient, RESTResponse
from vibe_piper.sources.base import Source
from vibe_piper.types import DataRecord, PipelineContext, Schema

# =============================================================================
# Configuration Classes
# =============================================================================


@dataclass
class APIAuthConfig:
    """Authentication configuration for API source."""

    type: Literal["bearer", "api_key", "basic", "oauth2", "none"] = "none"
    """Authentication type"""

    token: str | None = None
    """Bearer token or API key"""

    api_key_header: str | None = None
    """Custom API key header name (e.g., 'X-API-Key')"""

    username: str | None = None
    """Username for basic auth"""

    password: str | None = None
    """Password for basic auth"""

    oauth2_token_url: str | None = None
    """OAuth2 token endpoint URL"""

    oauth2_client_id: str | None = None
    """OAuth2 client ID"""

    oauth2_client_secret: str | None = None
    """OAuth2 client secret"""


@dataclass
class APIPaginationConfig:
    """Pagination configuration for API source."""

    type: Literal["cursor", "offset", "link_header", "none"] = "none"
    """Pagination type"""

    items_path: str = "items"
    """JSON path to items array in response"""

    page_size: int = 100
    """Number of items per page"""

    cursor_path: str = "cursor"
    """Path to cursor field (cursor pagination)"""

    has_next_path: str = "has_next_page"
    """Path to has_next field (cursor pagination)"""

    total_path: str = "total"
    """Path to total count (offset pagination)"""

    offset_param: str = "offset"
    """Offset parameter name (offset pagination)"""

    limit_param: str = "limit"
    """Limit parameter name (offset pagination)"""


@dataclass
class APIConfig:
    """Complete configuration for API source."""

    name: str
    """Source name"""

    base_url: str
    """API base URL"""

    endpoint: str
    """API endpoint to fetch"""

    auth: APIAuthConfig = field(default_factory=APIAuthConfig)
    """Authentication configuration"""

    pagination: APIPaginationConfig = field(default_factory=APIPaginationConfig)
    """Pagination configuration"""

    retry: RetryConfig = field(default_factory=RetryConfig)
    """Retry configuration"""

    rate_limit: tuple[int, float] | None = None  # (max_requests, time_window_seconds)
    """Rate limiting configuration"""

    headers: dict[str, str] = field(default_factory=dict)
    """Default headers"""

    timeout: float = 30.0
    """Request timeout in seconds"""

    verify_ssl: bool = True
    """Whether to verify SSL certificates"""

    log_responses: bool = False
    """Whether to log responses"""

    params: dict[str, Any] = field(default_factory=dict)
    """Default query parameters"""

    items_path: str = "items"
    """JSON path to items array in response"""

    schema: Schema | None = None
    """Optional explicit schema (inferred if None)"""


# =============================================================================
# API Source Implementation
# =============================================================================


class APISource(Source[DataRecord]):
    """
    REST API source with built-in pagination, retry, and rate limiting.

    Provides a declarative interface for fetching data from REST APIs without
    writing manual pagination, retry, or rate limiting code.

    Example:
        Basic API source::

            source = APISource(
                APIConfig(
                    name="users",
                    base_url="https://api.example.com",
                    endpoint="/users",
                    auth=APIAuthConfig(type="bearer", token="abc123"),
                    pagination=APIPaginationConfig(
                        type="offset",
                        page_size=100,
                        offset_param="offset",
                        limit_param="limit"
                    ),
                    retry=RetryConfig(max_attempts=3),
                    rate_limit=(60, 60)  # 60 requests per minute
                )
            )

            data = await source.fetch(context)

        Minimal config (5 lines)::

            source = APISource(
                APIConfig(
                    name="users",
                    base_url="https://api.example.com",
                    endpoint="/users",
                )
            )

            data = await source.fetch(context)
    """

    def __init__(self, config: APIConfig) -> None:
        """
        Initialize API source.

        Args:
            config: API source configuration
        """
        self.config = config
        self._client: RESTClient | None = None
        self._pagination_strategy: PaginationStrategy[DataRecord] | None = None
        self._auth_strategy: Any = None  # AuthStrategy
        self._rate_limiter: RateLimiter | None = None
        self._logger = logging.getLogger(self.__class__.__name__)

    async def _initialize(self) -> None:
        """Initialize REST client and pagination strategy."""
        if self._client is None:
            # Create rate limiter if configured
            if self.config.rate_limit:
                max_requests, time_window = self.config.rate_limit
                self._rate_limiter = RateLimiter(
                    max_requests=max_requests,
                    time_window_seconds=time_window,
                )
            # Create auth strategy
            self._auth_strategy = self._create_auth_strategy()
            # Create REST client
            self._client = RESTClient(
                base_url=self.config.base_url,
                auth=self._auth_strategy,
                timeout=self.config.timeout,
                retry_config=self.config.retry,
                rate_limiter=self._rate_limiter,
                headers=self.config.headers,
                verify_ssl=self.config.verify_ssl,
                log_responses=self.config.log_responses,
                default_params=self.config.params,
            )
            # Create pagination strategy
            self._pagination_strategy = self._create_pagination_strategy()
            # Initialize client
            await self._client.initialize()

    def _create_auth_strategy(self) -> Any:  # AuthStrategy
        """Create authentication strategy from config."""
        cfg = self.config.auth

        if cfg.type == "none":
            return None

        elif cfg.type == "bearer":
            if not cfg.token:
                msg = "Token required for bearer auth"
                raise ValueError(msg)
            return BearerTokenAuth(token=cfg.token)

        elif cfg.type == "api_key":
            if not cfg.token:
                msg = "API key required for api_key auth"
                raise ValueError(msg)
            header_name = cfg.api_key_header or "X-API-Key"
            return APIKeyAuth(api_key=cfg.token, key_name=header_name)

        elif cfg.type == "basic":
            if not cfg.username or not cfg.password:
                msg = "Username and password required for basic auth"
                raise ValueError(msg)
            return BasicAuth(username=cfg.username, password=cfg.password)

        elif cfg.type == "oauth2":
            if not cfg.oauth2_token_url or not cfg.oauth2_client_id or not cfg.oauth2_client_secret:
                msg = "OAuth2 requires token_url, client_id, and client_secret"
                raise ValueError(msg)
            from vibe_piper.integration.auth import OAuth2ClientCredentialsAuth

            return OAuth2ClientCredentialsAuth(
                token_url=cfg.oauth2_token_url,
                client_id=cfg.oauth2_client_id,
                client_secret=cfg.oauth2_client_secret,
            )

        return None

    def _create_pagination_strategy(self) -> PaginationStrategy[DataRecord] | None:
        """Create pagination strategy from config."""
        cfg = self.config.pagination

        if cfg.type == "none":
            return None

        elif cfg.type == "cursor":
            return CursorPagination(
                cursor_path=cfg.cursor_path,
                items_path=cfg.items_path,
                has_next_path=cfg.has_next_path,
                page_size=cfg.page_size,
            )

        elif cfg.type == "offset":
            return OffsetPagination(
                items_path=cfg.items_path,
                total_path=cfg.total_path,
                offset_param=cfg.offset_param,
                limit_param=cfg.limit_param,
                page_size=cfg.page_size,
            )

        elif cfg.type == "link_header":
            return LinkHeaderPagination(items_path=cfg.items_path)

        return None

    async def fetch(self, context: PipelineContext) -> Sequence[DataRecord]:
        """
        Fetch all data from API source.

        Handles pagination, retry, and rate limiting automatically.

        Args:
            context: Pipeline execution context

        Returns:
            Sequence of DataRecord objects

        Raises:
            Exception: If fetch fails
        """
        await self._initialize()

        if self._client is None:
            msg = "Client not initialized"
            raise RuntimeError(msg)

        # If pagination configured, use fetch_all_pages
        if self._pagination_strategy:
            items = await fetch_all_pages(
                client=self._client,
                path=self.config.endpoint,
                strategy=self._pagination_strategy,
                initial_params=self.config.params,
            )
            return self._convert_to_records(items)
        else:
            # Single request
            response = await self._client.get(self.config.endpoint)
            items = self._extract_items(response, self.config.items_path)
            return self._convert_to_records(items)

    async def stream(self, context: PipelineContext) -> AsyncIterator[DataRecord]:
        """
        Stream data from API source.

        Useful for large datasets where loading all data at once is impractical.

        Args:
            context: Pipeline execution context

        Yields:
            Individual DataRecord objects
        """
        await self._initialize()

        if self._client is None:
            msg = "Client not initialized"
            raise RuntimeError(msg)

        if self._pagination_strategy:
            async for item in paginate(
                client=self._client,
                path=self.config.endpoint,
                strategy=self._pagination_strategy,
                initial_params=self.config.params,
            ):
                yield self._dict_to_record(item)
        else:
            response = await self._client.get(self.config.endpoint)
            items = self._extract_items(response, self.config.items_path)
            for item in items:
                yield self._dict_to_record(item)

    def _extract_items(self, response: RESTResponse, items_path: str) -> list[dict[str, Any]]:
        """
        Extract items array from API response.

        Handles nested JSON paths like 'data.items' or 'results'.

        Args:
            response: API response
            items_path: JSON path to items array

        Returns:
            List of item dictionaries
        """
        try:
            data = response.data
            if isinstance(data, dict):
                # Navigate to items path
                parts = items_path.split(".")
                items = data
                for part in parts:
                    if isinstance(items, dict) and part in items:
                        items = items[part]
                    else:
                        return []
                if isinstance(items, list):
                    return items
            elif isinstance(data, list):
                return data
            return []
        except Exception as e:
            self._logger.error("Failed to extract items: %s", e)
            return []

    def _convert_to_records(self, items: list[dict[str, Any]]) -> list[DataRecord]:
        """Convert list of dicts to DataRecord objects."""
        schema = self.config.schema or self.infer_schema()
        return [self._dict_to_record(item, schema) for item in items]

    def _dict_to_record(self, data: dict[str, Any], schema: Schema | None = None) -> DataRecord:
        """Convert dict to DataRecord."""
        if schema is None:
            schema = self.config.schema or self.infer_schema()
        return DataRecord(data=data, schema=schema)

    def infer_schema(self) -> Schema:
        """
        Infer schema from API response.

        Fetches a sample response and infers field types.

        Returns:
            Inferred Schema
        """
        from datetime import datetime

        from vibe_piper.types import DataType

        # Try to fetch a sample
        async def fetch_sample() -> list[dict[str, Any]]:
            await self._initialize()
            if self._client is None:
                msg = "Client not initialized"
                raise RuntimeError(msg)
            response = await self._client.get(self.config.endpoint)
            return self._extract_items(response, self.config.items_path)

        try:
            sample_items = asyncio.run(fetch_sample())
        except Exception:
            # If fetch fails, return empty schema
            return Schema(name=self.config.name)

        if not sample_items:
            return Schema(name=self.config.name)

        # Analyze sample to infer types
        sample = sample_items[0]

        def infer_type(value: Any) -> DataType:
            """Infer DataType from value."""
            if isinstance(value, bool):
                return DataType.BOOLEAN
            elif isinstance(value, int):
                return DataType.INTEGER
            elif isinstance(value, float):
                return DataType.FLOAT
            elif isinstance(value, str):
                # Try to parse as datetime
                try:
                    datetime.fromisoformat(value)
                    return DataType.DATETIME
                except Exception:
                    return DataType.STRING
            elif isinstance(value, list):
                return DataType.ARRAY
            elif isinstance(value, dict):
                return DataType.OBJECT
            else:
                return DataType.ANY

        # Create fields
        from vibe_piper.types import SchemaField

        fields = [
            SchemaField(
                name=key,
                data_type=infer_type(value),
                required=True,
                nullable=value is None,
            )
            for key, value in sample.items()
        ]

        return Schema(name=self.config.name, fields=tuple(fields))

    def get_metadata(self) -> dict[str, Any]:
        """
        Get metadata about API source.

        Returns:
            Dictionary of metadata
        """
        return {
            "source_type": "api",
            "name": self.config.name,
            "base_url": self.config.base_url,
            "endpoint": self.config.endpoint,
            "auth_type": self.config.auth.type,
            "pagination_type": self.config.pagination.type,
            "rate_limit": self.config.rate_limit,
        }
