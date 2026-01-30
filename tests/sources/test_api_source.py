"""
Tests for API Source
"""

import asyncio
from collections.abc import Sequence
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from vibe_piper.integration.pagination import CursorPagination, OffsetPagination
from vibe_piper.sources.api import APIAuthConfig, APIConfig, APISource
from vibe_piper.types import PipelineContext


@pytest.fixture
def sample_api_response():
    """Sample API response data."""
    return {
        "data": {
            "items": [
                {"id": 1, "name": "Alice", "email": "alice@example.com"},
                {"id": 2, "name": "Bob", "email": "bob@example.com"},
            ],
            "total": 2,
        }
    }


@pytest.fixture
def sample_api_response_nested():
    """Sample nested API response."""
    return {
        "results": {
            "users": [
                {"id": 1, "name": "Alice", "email": "alice@example.com"},
            ],
        }
    }


class TestAPIConfig:
    """Tests for API configuration."""

    def test_minimal_config(self):
        """Test minimal API config (5 lines)."""
        config = APIConfig(
            name="users",
            base_url="https://api.example.com",
            endpoint="/users",
        )
        assert config.name == "users"
        assert config.base_url == "https://api.example.com"
        assert config.endpoint == "/users"
        assert config.auth.type == "none"
        assert config.pagination.type == "none"

    def test_bearer_auth_config(self):
        """Test bearer auth configuration."""
        config = APIConfig(
            name="users",
            base_url="https://api.example.com",
            endpoint="/users",
            auth=APIAuthConfig(type="bearer", token="abc123"),
        )
        assert config.auth.type == "bearer"
        assert config.auth.token == "abc123"

    def test_pagination_config(self):
        """Test pagination configuration."""
        config = APIConfig(
            name="users",
            base_url="https://api.example.com",
            endpoint="/users",
            pagination=APIPaginationConfig(
                type="offset",
                page_size=50,
                offset_param="offset",
                limit_param="limit",
            ),
        )
        assert config.pagination.type == "offset"
        assert config.pagination.page_size == 50


class TestAPISource:
    """Tests for API Source."""

    @pytest.mark.asyncio
    async def test_fetch_bearer_auth(self, sample_api_response, mock_rest_client):
        """Test fetching with bearer authentication."""
        config = APIConfig(
            name="users",
            base_url="https://api.example.com",
            endpoint="/users",
            auth=APIAuthConfig(type="bearer", token="test_token"),
        )

        source = APISource(config)

        # Mock the REST client
        source._client = mock_rest_client
        mock_rest_client.get = AsyncMock(return_value=MagicMock(data=sample_api_response))

        context = PipelineContext(pipeline_id="test_pipeline", run_id="test_run_1")

        records = await source.fetch(context)

        assert isinstance(records, Sequence)
        assert len(records) > 0

    @pytest.mark.asyncio
    async def test_fetch_with_pagination(self, sample_api_response, mock_rest_client):
        """Test fetching with offset pagination."""
        config = APIConfig(
            name="users",
            base_url="https://api.example.com",
            endpoint="/users",
            pagination=APIPaginationConfig(
                type="offset",
                page_size=50,
                items_path="data.items",
                offset_param="offset",
                limit_param="limit",
            ),
        )

        source = APISource(config)
        source._pagination_strategy = OffsetPagination(
            items_path="data.items",
            page_size=50,
            offset_param="offset",
            limit_param="limit",
        )

        source._client = mock_rest_client
        mock_rest_client.get = AsyncMock(return_value=MagicMock(data=sample_api_response))

        context = PipelineContext(pipeline_id="test_pipeline", run_id="test_run_1")

        records = await source.fetch(context)

        assert isinstance(records, Sequence)

    @pytest.mark.asyncio
    async def test_stream_with_cursor_pagination(self, sample_api_response, mock_rest_client):
        """Test streaming with cursor pagination."""
        config = APIConfig(
            name="users",
            base_url="https://api.example.com",
            endpoint="/users",
            pagination=APIPaginationConfig(
                type="cursor",
                page_size=50,
                items_path="data.items",
                cursor_path="cursor",
                has_next_path="has_next",
            ),
        )

        source = APISource(config)
        source._pagination_strategy = CursorPagination(
            items_path="data.items",
            page_size=50,
            cursor_path="cursor",
            has_next_path="has_next",
        )

        source._client = mock_rest_client
        mock_rest_client.get = AsyncMock(return_value=MagicMock(data=sample_api_response))

        context = PipelineContext(pipeline_id="test_pipeline", run_id="test_run_1")

        count = 0
        async for _ in source.stream(context):
            count += 1

        assert count > 0

    @pytest.mark.asyncio
    async def test_infer_schema(self, sample_api_response, mock_rest_client):
        """Test schema inference from API response."""
        config = APIConfig(
            name="users",
            base_url="https://api.example.com",
            endpoint="/users",
        )

        source = APISource(config)
        source._client = mock_rest_client
        mock_rest_client.get = AsyncMock(return_value=MagicMock(data=sample_api_response))

        schema = source.infer_schema()

        assert schema is not None
        assert schema.name == "users"
        assert len(schema.fields) > 0

    def test_get_metadata(self):
        """Test getting metadata."""
        config = APIConfig(
            name="users",
            base_url="https://api.example.com",
            endpoint="/users",
            auth=APIAuthConfig(type="bearer", token="abc123"),
            pagination=APIPaginationConfig(type="offset", page_size=100),
            rate_limit=(60, 60),
        )

        source = APISource(config)
        metadata = source.get_metadata()

        assert metadata["source_type"] == "api"
        assert metadata["name"] == "users"
        assert metadata["base_url"] == "https://api.example.com"
        assert metadata["auth_type"] == "bearer"
        assert metadata["pagination_type"] == "offset"
        assert metadata["rate_limit"] == (60, 60)


@pytest.fixture
def mock_rest_client():
    """Create mock REST client."""
    client = MagicMock()
    client.initialize = AsyncMock()
    client.get = AsyncMock()
    return client
