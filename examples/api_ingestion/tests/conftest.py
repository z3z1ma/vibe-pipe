"""Test fixtures for API ingestion tests."""

from datetime import UTC, datetime
from typing import Any

import pytest

from examples.api_ingestion.pipeline import APIIngestionPipeline
from examples.api_ingestion.schemas import UserResponse


class MockAPIServer:
    """Mock REST API server for testing."""

    def __init__(self) -> None:
        self.users = self._generate_sample_users(count=250)
        self.request_count = 0
        self.rate_limit_threshold = 20

    def _generate_sample_users(self, count: int = 100) -> list[dict[str, Any]]:
        """Generate sample user data."""
        users = []
        companies = ["Tech Corp", "Innovate LLC", "Data Systems"]
        cities = ["New York", "San Francisco", "Austin", "Seattle"]

        for i in range(1, count + 1):
            user = {
                "id": i,
                "name": f"User {i}",
                "email": f"user{i}@example.com",
                "username": f"user{i}",
                "phone": f"555-{i:04d}" if i % 3 == 0 else None,
                "website": f"user{i}.com" if i % 2 == 0 else None,
                "company": {"name": companies[i % len(companies)]},
                "address": {"city": cities[i % len(cities)]},
                "created_at": datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC).isoformat(),
                "updated_at": datetime(
                    2024, 1, i % 28 + 1, 0, 0, 0, tzinfo=UTC
                ).isoformat(),
            }

            if i == 42:
                user["email"] = "invalid-email"
            if i == 87:
                user["name"] = ""

            users.append(user)

        return users

    async def handle_request(
        self, _scope: dict[str, Any], _receive: Any, send: Any
    ) -> None:
        """Handle incoming ASGI requests."""
        if _scope["type"] != "http":
            return

        self.request_count += 1
        if self.request_count > self.rate_limit_threshold:
            await self._send_rate_limit_response(send)
            return

        method = _scope["method"]
        path = _scope["path"]

        if path == "/users" and method == "GET":
            await self._handle_get_users(_scope, send)
        else:
            await self._send_error_response(send, 404, "Not Found")

    async def _handle_get_users(
        self, scope: dict[str, Any], _receive: Any, send: Any
    ) -> None:
        """Handle GET /users with pagination."""
        query_string = scope.get("query_string", b"").decode()
        params = {}
        if query_string:
            for param in query_string.split("&"):
                if "=" in param:
                    key, value = param.split("=", 1)
                    params[key] = value

        limit = int(params.get("limit", "10"))
        offset = int(params.get("offset", "0"))

        total = len(self.users)
        users_page = self.users[offset : offset + limit]
        total_pages = (total + limit - 1) // limit
        current_page = offset // limit + 1

        response = {
            "data": users_page,
            "page": current_page,
            "per_page": limit,
            "total": total,
            "total_pages": total_pages,
            "has_next": current_page < total_pages,
            "has_prev": current_page > 1,
        }

        await self._send_json_response(send, response)

    async def _send_json_response(
        self, send: Any, data: dict[str, Any], status_code: int = 200
    ) -> None:
        """Send a JSON response."""
        import json

        body = json.dumps(data).encode()

        await send(
            {
                "type": "http.response.start",
                "status": status_code,
                "headers": [
                    [b"content-type", b"application/json"],
                    [b"content-length", str(len(body)).encode()],
                ],
            }
        )

        await send({"type": "http.response.body", "body": body})

    async def _send_error_response(
        self, send: Any, status_code: int, message: str
    ) -> None:
        """Send an error response."""
        await self._send_json_response(
            send,
            {"error": message, "status_code": status_code},
            status_code=status_code,
        )

    async def _send_rate_limit_response(self, send: Any) -> None:
        """Send a rate limit response."""
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

        body = b'{"error": "Rate limit exceeded", "status_code": 429}'

        await send({"type": "http.response.body", "body": body})


@pytest.fixture
def mock_api_server():
    """Create a mock API server for testing."""
    return MockAPIServer()


@pytest.fixture
def test_pipeline(mock_api_url: str):
    """Create a test pipeline instance."""
    pipeline = APIIngestionPipeline(
        api_base_url=mock_api_url,
        api_key=None,
        db_config=None,
        rate_limit_per_second=100,
        max_retries=3,
        page_size=10,
    )
    return pipeline


@pytest.fixture
def mock_api_url():
    """Get the mock API server URL."""
    return "http://testserver"


@pytest.fixture
def sample_user_response():
    """Get a sample UserResponse for testing."""
    data = {
        "id": 1,
        "name": "Test User",
        "email": "test@example.com",
        "username": "testuser",
        "phone": "555-1234",
        "website": "test.com",
        "company": {"name": "Test Corp"},
        "address": {"city": "Test City"},
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z",
    }
    return UserResponse.from_dict(data)


@pytest.fixture
async def initialized_pipeline(test_pipeline: APIIngestionPipeline):
    """Create and initialize a test pipeline."""
    await test_pipeline.initialize()
    yield test_pipeline
    await test_pipeline.close()
