"""
Shared fixtures for integration tests.
"""

from unittest.mock import AsyncMock

import httpx
import pytest


@pytest.fixture
def mock_http_client():
    """Create a mock HTTP client."""
    client = AsyncMock(spec=httpx.AsyncClient)
    return client


@pytest.fixture
def sample_api_response():
    """Sample API response data."""
    return {
        "id": 1,
        "name": "Test User",
        "email": "test@example.com",
    }


@pytest.fixture
def sample_graphql_response():
    """Sample GraphQL response data."""
    return {
        "data": {
            "user": {
                "id": "1",
                "name": "Test User",
                "email": "test@example.com",
            }
        }
    }


@pytest.fixture
def sample_paginated_response():
    """Sample paginated response data."""
    return {
        "items": [
            {"id": 1, "name": "Item 1"},
            {"id": 2, "name": "Item 2"},
        ],
        "has_next_page": True,
        "total": 10,
    }
