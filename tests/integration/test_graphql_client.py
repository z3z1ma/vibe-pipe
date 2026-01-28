"""
Tests for GraphQL client.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from vibe_piper.integration.auth import BearerTokenAuth
from vibe_piper.integration.base import APIError
from vibe_piper.integration.graphql import GraphQLClient, GraphQLResponse


@pytest.mark.asyncio
class TestGraphQLClient:
    """Test GraphQL client functionality."""

    async def test_client_initialization(self):
        """Test client initialization."""
        client = GraphQLClient("https://api.example.com/graphql")
        assert client.base_url == "https://api.example.com/graphql"
        assert "Content-Type" in client.default_headers
        assert client.default_headers["Content-Type"] == "application/json"

    async def test_query_execution(self):
        """Test GraphQL query execution."""
        async with GraphQLClient("https://api.example.com/graphql") as client:
            mock_response = MagicMock(spec=httpx.Response)
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "data": {
                    "user": {
                        "id": "1",
                        "name": "Test User",
                    }
                }
            }

            with patch.object(
                client, "_execute_request", new=AsyncMock(return_value=mock_response)
            ):
                response = await client.execute(
                    query="query { user { id name } }",
                )

                assert response.is_success is True
                assert response.data is not None
                assert response.data["user"]["id"] == "1"

    async def test_query_with_variables(self):
        """Test GraphQL query with variables."""
        async with GraphQLClient("https://api.example.com/graphql") as client:
            mock_response = MagicMock(spec=httpx.Response)
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "data": {
                    "user": {
                        "id": "1",
                        "name": "Test User",
                    }
                }
            }

            with patch.object(
                client, "_execute_request", new=AsyncMock(return_value=mock_response)
            ):
                response = await client.execute(
                    query="query GetUser($id: ID!) { user(id: $id) { id name } }",
                    variables={"id": "1"},
                )

                assert response.is_success is True

    async def test_query_convenience_method(self):
        """Test query convenience method."""
        async with GraphQLClient("https://api.example.com/graphql") as client:
            mock_response = MagicMock(spec=httpx.Response)
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "data": {
                    "user": {
                        "id": "1",
                        "name": "Test User",
                    }
                }
            }

            with patch.object(
                client, "_execute_request", new=AsyncMock(return_value=mock_response)
            ):
                data = await client.query("query { user { id } }")

                assert data is not None
                assert data["user"]["id"] == "1"

    async def test_mutation(self):
        """Test GraphQL mutation."""
        async with GraphQLClient("https://api.example.com/graphql") as client:
            mock_response = MagicMock(spec=httpx.Response)
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "data": {
                    "createUser": {
                        "id": "1",
                        "name": "New User",
                    }
                }
            }

            with patch.object(
                client, "_execute_request", new=AsyncMock(return_value=mock_response)
            ):
                data = await client.mutate(
                    mutation="mutation CreateUser($name: String!) { createUser(name: $name) { id name } }",
                    variables={"name": "New User"},
                )

                assert data is not None
                assert data["createUser"]["name"] == "New User"


@pytest.mark.asyncio
class TestGraphQLResponse:
    """Test GraphQL response handling."""

    async def test_successful_response(self):
        """Test successful GraphQL response."""
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200

        response = GraphQLResponse(
            data={"user": {"id": "1"}},
            errors=None,
            extensions=None,
            raw_response=mock_response,
        )

        assert response.is_success is True
        assert response.is_error is False
        assert response.data is not None

    async def test_error_response(self):
        """Test GraphQL error response."""
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200

        response = GraphQLResponse(
            data=None,
            errors=[{"message": "User not found"}],
            extensions=None,
            raw_response=mock_response,
        )

        assert response.is_success is False
        assert response.is_error is True
        assert response.get_first_error() is not None
        assert response.get_first_error()["message"] == "User not found"

    async def test_raise_on_error(self):
        """Test raise_on_error method."""
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200

        response = GraphQLResponse(
            data=None,
            errors=[{"message": "Validation error"}],
            extensions=None,
            raw_response=mock_response,
        )

        with pytest.raises(APIError, match="GraphQL error"):
            response.raise_on_error()

    async def test_response_with_extensions(self):
        """Test GraphQL response with extensions."""
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200

        response = GraphQLResponse(
            data={"user": {"id": "1"}},
            errors=None,
            extensions={"cost": 5},
            raw_response=mock_response,
        )

        assert response.is_success is True
        assert response.extensions is not None
        assert response.extensions["cost"] == 5


@pytest.mark.asyncio
class TestGraphQLClientErrors:
    """Test GraphQL client error handling."""

    async def test_graphql_errors_with_raise_on_error(self):
        """Test that GraphQL errors raise when raise_on_error is True."""
        async with GraphQLClient("https://api.example.com/graphql") as client:
            mock_response = MagicMock(spec=httpx.Response)
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "data": None,
                "errors": [{"message": "Field 'invalid' doesn't exist"}],
            }

            with patch.object(
                client, "_execute_request", new=AsyncMock(return_value=mock_response)
            ):
                with pytest.raises(APIError, match="GraphQL error"):
                    await client.execute(
                        query="query { invalid }",
                        raise_on_error=True,
                    )

    async def test_graphql_errors_without_raise_on_error(self):
        """Test that GraphQL errors don't raise when raise_on_error is False."""
        async with GraphQLClient("https://api.example.com/graphql") as client:
            mock_response = MagicMock(spec=httpx.Response)
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "data": None,
                "errors": [{"message": "Field 'invalid' doesn't exist"}],
            }

            with patch.object(
                client, "_execute_request", new=AsyncMock(return_value=mock_response)
            ):
                response = await client.execute(
                    query="query { invalid }",
                    raise_on_error=False,
                )

                assert response.is_error is True
                assert response.errors is not None

    async def test_query_convenience_raises_on_error(self):
        """Test that query convenience method raises on errors."""
        async with GraphQLClient("https://api.example.com/graphql") as client:
            mock_response = MagicMock(spec=httpx.Response)
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "data": None,
                "errors": [{"message": "Syntax error"}],
            }

            with patch.object(
                client, "_execute_request", new=AsyncMock(return_value=mock_response)
            ):
                with pytest.raises(APIError):
                    await client.query("query { invalid }")


@pytest.mark.asyncio
class TestGraphQLClientAuth:
    """Test GraphQL client authentication."""

    async def test_bearer_token_auth(self):
        """Test Bearer token authentication."""
        auth = BearerTokenAuth("test_token")
        client = GraphQLClient("https://api.example.com/graphql", auth=auth)

        async with client:
            mock_response = MagicMock(spec=httpx.Response)
            mock_response.status_code = 200
            mock_response.json.return_value = {"data": {"user": {"id": "1"}}}

            with patch.object(
                client, "_execute_request", new=AsyncMock(return_value=mock_response)
            ):
                response = await client.query("query { user { id } }")
                assert response is not None
