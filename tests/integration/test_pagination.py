"""
Tests for pagination strategies.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from vibe_piper.integration.pagination import (
    CursorPagination,
    LinkHeaderPagination,
    OffsetPagination,
    fetch_all_pages,
    paginate,
)
from vibe_piper.integration.rest import RESTClient, RESTResponse


@pytest.mark.asyncio
class TestCursorPagination:
    """Test cursor-based pagination."""

    async def test_extract_items(self):
        """Test extracting items from response."""
        strategy = CursorPagination()

        mock_response = MagicMock(spec=RESTResponse)
        mock_response.data = {
            "items": [
                {"id": 1, "cursor": "abc"},
                {"id": 2, "cursor": "def"},
            ]
        }

        items = await strategy.get_items(mock_response)

        assert len(items) == 2
        assert items[0]["id"] == 1

    async def test_has_next_page(self):
        """Test checking if there's a next page."""
        strategy = CursorPagination()

        mock_response = MagicMock(spec=RESTResponse)
        mock_response.data = {
            "has_next_page": True,
        }

        has_next = await strategy.has_next_page(mock_response)

        assert has_next is True

    async def test_no_next_page(self):
        """Test when there's no next page."""
        strategy = CursorPagination()

        mock_response = MagicMock(spec=RESTResponse)
        mock_response.data = {
            "has_next_page": False,
        }

        has_next = await strategy.has_next_page(mock_response)

        assert has_next is False

    async def test_get_next_page_params(self):
        """Test getting next page parameters."""
        strategy = CursorPagination()

        mock_response = MagicMock(spec=RESTResponse)
        mock_response.data = {
            "items": [
                {"id": 1, "cursor": "abc"},
                {"id": 2, "cursor": "def"},
            ]
        }

        params = await strategy.get_next_page_params(mock_response)

        assert "cursor" in params
        assert params["cursor"] == "def"
        assert params["first"] == 100  # default page_size

    async def test_custom_paths(self):
        """Test custom JSON paths."""
        strategy = CursorPagination(
            cursor_path="pagination.cursor",
            items_path="data.results",
            has_next_path="pagination.more",
        )

        mock_response = MagicMock(spec=RESTResponse)
        mock_response.data = {
            "data": {"results": [{"id": 1, "cursor": "xyz"}]},
            "pagination": {
                "more": True,
            },
        }

        items = await strategy.get_items(mock_response)
        has_next = await strategy.has_next_page(mock_response)

        assert len(items) == 1
        assert has_next is True


@pytest.mark.asyncio
class TestOffsetPagination:
    """Test offset-based pagination."""

    async def test_extract_items(self):
        """Test extracting items from response."""
        strategy = OffsetPagination()

        mock_response = MagicMock(spec=RESTResponse)
        mock_response.data = {
            "items": [
                {"id": 1},
                {"id": 2},
            ]
        }

        items = await strategy.get_items(mock_response)

        assert len(items) == 2

    async def test_has_next_page_by_count(self):
        """Test checking next page by item count."""
        strategy = OffsetPagination(page_size=10)

        mock_response = MagicMock(spec=RESTResponse)
        mock_response.data = {"items": [{"id": i} for i in range(10)]}

        has_next = await strategy.has_next_page(mock_response)

        assert has_next is True

    async def test_no_next_page_when_fewer_items(self):
        """Test no next page when fewer items than page size."""
        strategy = OffsetPagination(page_size=10)

        mock_response = MagicMock(spec=RESTResponse)
        mock_response.data = {"items": [{"id": 1}, {"id": 2}]}

        has_next = await strategy.has_next_page(mock_response)

        assert has_next is False

    async def test_has_next_page_by_total(self):
        """Test checking next page by total count."""
        strategy = OffsetPagination(page_size=10)

        mock_response = MagicMock(spec=RESTResponse)
        mock_response.data = {
            "items": [{"id": i} for i in range(10)],
            "total": 25,
        }

        has_next = await strategy.has_next_page(mock_response)

        assert has_next is True

    async def test_get_next_page_params(self):
        """Test getting next page parameters."""
        strategy = OffsetPagination(
            page_size=10, offset_param="offset", limit_param="limit"
        )

        mock_response = MagicMock(spec=RESTResponse)
        mock_response.data = {"items": [{"id": i} for i in range(10)]}

        params = await strategy.get_next_page_params(mock_response)

        assert params["offset"] == 10
        assert params["limit"] == 10


@pytest.mark.asyncio
class TestLinkHeaderPagination:
    """Test link header pagination."""

    async def test_extract_items(self):
        """Test extracting items from response."""
        strategy = LinkHeaderPagination()

        mock_response = MagicMock(spec=RESTResponse)
        mock_response.data = {"items": [{"id": 1}, {"id": 2}]}

        items = await strategy.get_items(mock_response)

        assert len(items) == 2

    async def test_has_next_page_from_link_header(self):
        """Test checking next page from Link header."""
        strategy = LinkHeaderPagination()

        mock_response = MagicMock(spec=RESTResponse)
        mock_response.headers = {
            "Link": '<https://api.example.com/page=2>; rel="next", <https://api.example.com/page=1>; rel="prev"'
        }

        has_next = await strategy.has_next_page(mock_response)

        assert has_next is True

    async def test_no_next_page_in_link_header(self):
        """Test when there's no next link in header."""
        strategy = LinkHeaderPagination()

        mock_response = MagicMock(spec=RESTResponse)
        mock_response.headers = {"Link": '<https://api.example.com/page=1>; rel="prev"'}

        has_next = await strategy.has_next_page(mock_response)

        assert has_next is False

    async def test_get_next_page_params(self):
        """Test extracting next page URL from Link header."""
        strategy = LinkHeaderPagination()

        mock_response = MagicMock(spec=RESTResponse)
        mock_response.headers = {
            "Link": '<https://api.example.com/items?page=2&per_page=10>; rel="next"'
        }

        params = await strategy.get_next_page_params(mock_response)

        assert "page" in params
        assert params["page"] == "2"
        assert params["per_page"] == "10"


@pytest.mark.asyncio
class TestPaginationHelpers:
    """Test pagination helper functions."""

    async def test_paginate_iterator(self):
        """Test paginate async iterator."""
        strategy = OffsetPagination(page_size=2)

        # Mock client
        client = MagicMock(spec=RESTClient)

        # Create mock responses
        response1 = MagicMock(spec=RESTResponse)
        response1.data = {
            "items": [{"id": 1}, {"id": 2}],
            "total": 4,
        }

        response2 = MagicMock(spec=RESTResponse)
        response2.data = {
            "items": [{"id": 3}, {"id": 4}],
            "total": 4,
        }

        call_count = 0

        async def mock_get(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return response1
            return response2

        client.get = AsyncMock(side_effect=mock_get)

        # Use paginate
        items = []
        async for item in paginate(client, "/items", strategy):
            items.append(item)

        assert len(items) == 4
        assert call_count == 2

    async def test_fetch_all_pages(self):
        """Test fetching all pages at once."""
        strategy = OffsetPagination(page_size=2)

        client = MagicMock(spec=RESTClient)

        response1 = MagicMock(spec=RESTResponse)
        response1.data = {
            "items": [{"id": 1}, {"id": 2}],
            "total": 3,
        }

        response2 = MagicMock(spec=RESTResponse)
        response2.data = {
            "items": [{"id": 3}],
            "total": 3,
        }

        call_count = 0

        async def mock_get(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return response1
            return response2

        client.get = AsyncMock(side_effect=mock_get)

        items = await fetch_all_pages(client, "/items", strategy)

        assert len(items) == 3
        assert call_count == 2
