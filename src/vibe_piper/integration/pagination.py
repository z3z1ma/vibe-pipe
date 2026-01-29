"""
Pagination support for API clients.

This module provides pagination strategies for handling paginated API responses:
- Cursor-based pagination
- Offset-based pagination
- Link header pagination (RFC 5988)
"""

import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from vibe_piper.integration.rest import RESTClient, RESTResponse

T = TypeVar("T")


# =============================================================================
# Pagination Strategy Protocol
# =============================================================================


class PaginationStrategy(ABC, Generic[T]):
    """
    Abstract base class for pagination strategies.

    Each strategy knows how to extract items from a response
    and determine if there are more pages.
    """

    @abstractmethod
    async def get_items(self, response: RESTResponse) -> list[T]:
        """
        Extract items from response.

        Args:
            response: API response

        Returns:
            List of items from current page
        """

    @abstractmethod
    async def has_next_page(self, response: RESTResponse) -> bool:
        """
        Check if there are more pages.

        Args:
            response: API response

        Returns:
            True if more pages available
        """

    @abstractmethod
    async def get_next_page_params(self, response: RESTResponse) -> dict[str, Any]:
        """
        Get parameters for next page request.

        Args:
            response: Current API response

        Returns:
            Parameters for next page request
        """


# =============================================================================
# Cursor-based Pagination
# =============================================================================


@dataclass
class CursorPagination(PaginationStrategy[T]):
    """
    Cursor-based pagination strategy.

    Common in GraphQL APIs and modern REST APIs.
    Uses an opaque cursor to navigate through results.
    """

    cursor_path: str = "cursor"
    """JSON path to cursor field in response"""

    items_path: str = "items"
    """JSON path to items array in response"""

    has_next_path: str = "has_next_page"
    """JSON path to has_next_page flag"""

    page_size: int = 100
    """Number of items per page"""

    def __post_init__(self) -> None:
        """Validate cursor pagination configuration."""
        self._logger = logging.getLogger(self.__class__.__name__)

    async def get_items(self, response: RESTResponse) -> list[T]:
        """
        Extract items from cursor-based paginated response.

        Args:
            response: API response

        Returns:
            List of items from current page
        """
        try:
            data = response.data
            if isinstance(data, dict):
                # Navigate to items path
                path_parts = self.items_path.split(".")
                items = data
                for part in path_parts:
                    items = items.get(part, {})

                if isinstance(items, list):
                    return items

            # Return empty list if path not found
            return []

        except Exception as e:
            self._logger.error("Failed to extract items: %s", e)
            return []

    async def has_next_page(self, response: RESTResponse) -> bool:
        """
        Check if there are more pages in cursor-based pagination.

        Args:
            response: API response

        Returns:
            True if more pages available
        """
        try:
            data = response.data
            if isinstance(data, dict):
                # Navigate to has_next path
                path_parts = self.has_next_path.split(".")
                value = data
                for part in path_parts:
                    value = value.get(part, False)

                return bool(value)

            return False

        except Exception:
            return False

    async def get_next_page_params(self, response: RESTResponse) -> dict[str, Any]:
        """
        Get parameters for next cursor-based page.

        Args:
            response: Current API response

        Returns:
            Parameters for next page request
        """
        # Extract last cursor from items
        items = await self.get_items(response)

        if items and isinstance(items[-1], dict):
            # Get cursor from last item
            last_item = items[-1]
            cursor = last_item.get(self.cursor_path)

            if cursor:
                return {"cursor": cursor, "first": self.page_size}

        # Return empty params to end pagination
        return {}


# =============================================================================
# Offset-based Pagination
# =============================================================================


@dataclass
class OffsetPagination(PaginationStrategy[T]):
    """
    Offset-based pagination strategy.

    Traditional pagination using offset and limit parameters.
    Common in SQL-based APIs.
    """

    items_path: str = "items"
    """JSON path to items array in response"""

    total_path: str = "total"
    """JSON path to total count field"""

    offset_param: str = "offset"
    """Parameter name for offset"""

    limit_param: str = "limit"
    """Parameter name for limit"""

    page_size: int = 100
    """Number of items per page"""

    def __post_init__(self) -> None:
        """Validate offset pagination configuration."""
        self._logger = logging.getLogger(self.__class__.__name__)
        self._current_offset = 0

    async def get_items(self, response: RESTResponse) -> list[T]:
        """
        Extract items from offset-based paginated response.

        Args:
            response: API response

        Returns:
            List of items from current page
        """
        try:
            data = response.data
            if isinstance(data, dict):
                # Navigate to items path
                path_parts = self.items_path.split(".")
                items = data
                for part in path_parts:
                    items = items.get(part, {})

                if isinstance(items, list):
                    return items

            return []

        except Exception as e:
            self._logger.error("Failed to extract items: %s", e)
            return []

    async def has_next_page(self, response: RESTResponse) -> bool:
        """
        Check if there are more pages in offset-based pagination.

        Args:
            response: API response

        Returns:
            True if more pages available
        """
        try:
            items = await self.get_items(response)

            # If we got fewer items than page size, we're likely at the end
            if len(items) < self.page_size:
                return False

            # Check total count if available
            data = response.data
            if isinstance(data, dict):
                path_parts = self.total_path.split(".")
                total = data
                for part in path_parts:
                    total = total.get(part)

                if isinstance(total, int):
                    return self._current_offset + len(items) < total

            return True

        except Exception:
            return False

    async def get_next_page_params(self, response: RESTResponse) -> dict[str, Any]:
        """
        Get parameters for next offset-based page.

        Args:
            response: Current API response

        Returns:
            Parameters for next page request
        """
        items = await self.get_items(response)
        self._current_offset += len(items)

        return {
            self.offset_param: self._current_offset,
            self.limit_param: self.page_size,
        }


# =============================================================================
# Link Header Pagination
# =============================================================================


@dataclass
class LinkHeaderPagination(PaginationStrategy[T]):
    """
    Link header pagination (RFC 5988).

    Uses HTTP Link headers for pagination navigation.
    Common in GitHub, GitLab APIs.
    """

    items_path: str = "items"
    """JSON path to items array in response"""

    def __post_init__(self) -> None:
        """Validate link header pagination configuration."""
        self._logger = logging.getLogger(self.__class__.__name__)

    async def get_items(self, response: RESTResponse) -> list[T]:
        """
        Extract items from response with link header pagination.

        Args:
            response: API response

        Returns:
            List of items from current page
        """
        try:
            data = response.data
            if isinstance(data, dict):
                # Navigate to items path
                path_parts = self.items_path.split(".")
                items = data
                for part in path_parts:
                    items = items.get(part, {})

                if isinstance(items, list):
                    return items

            return []

        except Exception as e:
            self._logger.error("Failed to extract items: %s", e)
            return []

    async def has_next_page(self, response: RESTResponse) -> bool:
        """
        Check if there are more pages using link headers.

        Args:
            response: API response

        Returns:
            True if more pages available
        """
        link_header = response.headers.get("Link", "")
        return 'rel="next"' in link_header

    async def get_next_page_params(self, response: RESTResponse) -> dict[str, Any]:
        """
        Extract next page URL from link headers.

        Args:
            response: Current API response

        Returns:
            Parameters for next page request (extracted from next URL)
        """
        link_header = response.headers.get("Link", "")

        # Parse link header to find next URL
        import re

        pattern = r'<([^>]+)>;\s*rel="next"'
        match = re.search(pattern, link_header)

        if match:
            next_url = match.group(1)

            # Extract query parameters from next URL
            from urllib.parse import parse_qs, urlparse

            parsed = urlparse(next_url)
            params = parse_qs(parsed.query)

            # Flatten parameter values
            return {k: v[0] if len(v) == 1 else v for k, v in params.items()}

        return {}


# =============================================================================
# Paginated Iterator
# =============================================================================


async def paginate(
    client: RESTClient,
    path: str,
    strategy: PaginationStrategy[T],
    method: str = "GET",
    initial_params: dict[str, Any] | None = None,
    **kwargs: Any,
) -> AsyncIterator[T]:
    """
    Iterate through all pages using a pagination strategy.

    Args:
        client: REST client to use
        path: Request path
        strategy: Pagination strategy to use
        method: HTTP method (default: GET)
        initial_params: Initial query parameters
        **kwargs: Additional request parameters

    Yields:
        Individual items from all pages

    Example:
        ```python
        async for item in paginate(client, "/users", OffsetPagination()):
            print(item)
        ```
    """
    params = initial_params or {}

    while True:
        # Make request
        if method.upper() == "GET":
            response = await client.get(path, params=params, **kwargs)
        elif method.upper() == "POST":
            response = await client.post(path, json=params, **kwargs)
        else:
            raise ValueError(f"Unsupported method: {method}")

        # Extract items from current page
        items = await strategy.get_items(response)

        # Yield each item
        for item in items:
            yield item

        # Check if there are more pages
        if not await strategy.has_next_page(response):
            break

        # Get parameters for next page
        params = await strategy.get_next_page_params(response)

        # If params are empty, end pagination
        if not params:
            break


async def fetch_all_pages(
    client: RESTClient,
    path: str,
    strategy: PaginationStrategy[T],
    method: str = "GET",
    initial_params: dict[str, Any] | None = None,
    **kwargs: Any,
) -> list[T]:
    """
    Fetch all items from all pages.

    Args:
        client: REST client to use
        path: Request path
        strategy: Pagination strategy to use
        method: HTTP method (default: GET)
        initial_params: Initial query parameters
        **kwargs: Additional request parameters

    Returns:
        List of all items from all pages
    """
    items = []

    async for item in paginate(client, path, strategy, method, initial_params, **kwargs):
        items.append(item)

    return items
