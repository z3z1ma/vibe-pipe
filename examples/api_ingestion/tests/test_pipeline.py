"""Integration tests for API Ingestion pipeline."""

from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

from examples.api_ingestion.pipeline import APIIngestionPipeline
from examples.api_ingestion.schemas import QualityReport, UserResponse


@pytest.mark.anyio
async def test_pipeline_initialization() -> None:
    """Test that pipeline can be initialized correctly."""

    # Use httpx.MockTransport for testing without real HTTP calls
    def handler(request: httpx.Request) -> httpx.Response:
        # Mock a successful response
        return httpx.Response(
            200,
            json={"data": [], "total": 0},
            request=request,
        )

    mock_transport = httpx.MockTransport(handler)

    # Create a mock httpx client with the transport
    mock_client = httpx.AsyncClient(transport=mock_transport)

    pipeline = APIIngestionPipeline(
        api_base_url="http://testserver",
        rate_limit_per_second=100,
        page_size=10,
    )

    # Replace the client's internal httpx client with our mock
    pipeline.rest_client._client = mock_client

    await pipeline.initialize()

    assert pipeline.rest_client is not None
    assert pipeline.rest_client._client is not None

    await pipeline.close()


def test_transform_user_valid(sample_user_response: UserResponse) -> None:
    """Test transforming a valid user."""
    pipeline = APIIngestionPipeline(api_base_url="http://testserver")

    user_dict = pipeline.transform_user(sample_user_response)

    assert user_dict is not None
    assert user_dict["user_id"] == 1
    assert user_dict["name"] == "Test User"
    assert user_dict["email"] == "test@example.com"
    assert user_dict["company_name"] == "Test Corp"
    assert user_dict["city"] == "Test City"


def test_transform_user_missing_name() -> None:
    """Test transforming a user with missing name."""
    pipeline = APIIngestionPipeline(api_base_url="http://testserver")

    user = UserResponse(
        id=1,
        name="",
        email="test@example.com",
        username="testuser",
        phone=None,
        website=None,
        company=None,
        address=None,
        created_at=None,
        updated_at=None,
    )

    user_dict = pipeline.transform_user(user)

    assert user_dict is None
    assert len(pipeline._validation_errors) > 0


def test_transform_user_invalid_email() -> None:
    """Test transforming a user with invalid email."""
    pipeline = APIIngestionPipeline(api_base_url="http://testserver")

    user = UserResponse(
        id=1,
        name="Test User",
        email="invalid-email",
        username="testuser",
        phone=None,
        website=None,
        company=None,
        address=None,
        created_at=None,
        updated_at=None,
    )

    user_dict = pipeline.transform_user(user)

    assert user_dict is None
    assert "email" in pipeline._validation_errors[0]["error"].lower()


def test_quality_report_to_dict() -> None:
    """Test converting quality report to dictionary."""
    start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    end_time = datetime(2024, 1, 1, 0, 1, 30, tzinfo=UTC)

    report = QualityReport(
        total_records=100,
        successful_records=95,
        failed_records=5,
        validation_errors=[],
        api_calls=10,
        pages_fetched=10,
        start_time=start_time,
        end_time=end_time,
        rate_limit_hits=0,
        retry_attempts=2,
    )

    report_dict = report.to_dict()

    assert report_dict["total_records"] == 100
    assert report_dict["successful_records"] == 95
    assert report_dict["success_rate"] == 0.95
    assert report_dict["duration_seconds"] == 90.0


@pytest.mark.anyio
async def test_pipeline_run_dry_run() -> None:
    """Test running of pipeline in dry run mode."""

    # Mock handler that returns sample users
    def handler(request: httpx.Request) -> httpx.Response:
        # Return paginated response for /users endpoint
        if "/users" in str(request.url):
            users_data = []
            for i in range(1, 11):
                users_data.append(
                    {
                        "id": i,
                        "name": f"User {i}",
                        "email": f"user{i}@example.com",
                        "username": f"user{i}",
                    }
                )
            return httpx.Response(
                200,
                json={
                    "data": users_data,
                    "total": 10,
                    "page": 1,
                    "per_page": 10,
                    "has_next": False,
                    "has_prev": False,
                },
                request=request,
            )
        return httpx.Response(404, request=request)

    mock_transport = httpx.MockTransport(handler)
    mock_client = httpx.AsyncClient(
        transport=mock_transport,
        base_url="http://testserver",  # Must match pipeline's base_url
    )

    pipeline = APIIngestionPipeline(
        api_base_url="http://testserver",
        db_config=None,
        rate_limit_per_second=100,
        page_size=10,
    )

    # Replace the client's internal httpx client with our mock
    pipeline.rest_client._client = mock_client

    await pipeline.initialize()

    try:
        report = await pipeline.run(dry_run=True)

        assert report.total_records == 10
        # api_calls tracks items processed in fetch_users
        assert report.api_calls == 10

    finally:
        await pipeline.close()


@pytest.mark.anyio
async def test_pipeline_with_empty_response() -> None:
    """Test pipeline with empty API response."""

    # Mock handler that returns empty response
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"data": [], "total": 0},
            request=request,
        )

    mock_transport = httpx.MockTransport(handler)
    mock_client = httpx.AsyncClient(
        transport=mock_transport,
        base_url="http://testserver",
    )

    pipeline = APIIngestionPipeline(
        api_base_url="http://testserver",
        db_config=None,
        rate_limit_per_second=100,
        page_size=10,
    )

    # Replace the client's internal httpx client with our mock
    pipeline.rest_client._client = mock_client

    await pipeline.initialize()

    try:
        users = await pipeline.fetch_users()

        assert len(users) == 0

    finally:
        await pipeline.close()


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
