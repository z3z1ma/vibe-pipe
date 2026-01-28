"""Integration tests for the API Ingestion pipeline."""

from datetime import UTC, datetime
from typing import Any

import pytest

from examples.api_ingestion.pipeline import APIIngestionPipeline
from examples.api_ingestion.schemas import QualityReport, UserResponse

from .conftest import MockAPIServer


@pytest.mark.asyncio
async def test_pipeline_initialization(test_pipeline: APIIngestionPipeline) -> None:
    """Test that the pipeline can be initialized correctly."""
    await test_pipeline.initialize()

    assert test_pipeline.rest_client is not None
    assert test_pipeline.rest_client._client is not None

    await test_pipeline.close()


@pytest.mark.asyncio
async def test_fetch_users_with_pagination(_mock_api_server: MockAPIServer) -> None:
    """Test fetching users with pagination."""
    pipeline = APIIngestionPipeline(
        api_base_url="http://testserver",
        rate_limit_per_second=100,
        page_size=10,
    )

    await pipeline.initialize()

    try:
        users = await pipeline.fetch_users()

        assert len(users) == 250
        assert pipeline._pages_fetched == 25

        user = users[0]
        assert isinstance(user, UserResponse)
        assert user.id == 1
        assert user.name == "User 1"

    finally:
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


@pytest.mark.asyncio
async def test_pipeline_run_dry_run(_mock_api_server: MockAPIServer) -> None:
    """Test running the pipeline in dry run mode."""
    pipeline = APIIngestionPipeline(
        api_base_url="http://testserver",
        db_config=None,
        rate_limit_per_second=100,
        page_size=10,
    )

    await pipeline.initialize()

    try:
        report = await pipeline.run(dry_run=True)

        assert report.total_records == 250
        assert report.api_calls > 0

    finally:
        await pipeline.close()


@pytest.mark.asyncio
async def test_pipeline_handles_rate_limiting() -> None:
    """Test that the pipeline handles rate limiting."""

    class RateLimitedAPIServer:
        def __init__(self) -> None:
            self.request_count = 0

        async def handle_request(self, _scope: dict, _receive: Any, send: Any) -> None:
            self.request_count += 1

            if self.request_count > 2:
                await send(
                    {
                        "type": "http.response.start",
                        "status": 429,
                        "headers": [[b"retry-after", b"1"]],
                    }
                )
                await send(
                    {"type": "http.response.body", "body": b'{"error": "Rate limit"}'}
                )
            else:
                await send(
                    {
                        "type": "http.response.start",
                        "status": 200,
                        "headers": [[b"content-type", b"application/json"]],
                    }
                )
                body = b'{"data": [], "total": 0, "page": 1, "per_page": 10}'
                await send({"type": "http.response.body", "body": body})

    pipeline = APIIngestionPipeline(
        api_base_url="http://testserver",
        db_config=None,
        rate_limit_per_second=100,
        max_retries=1,
        page_size=10,
    )

    await pipeline.initialize()

    try:
        users = await pipeline.fetch_users()

        assert isinstance(users, list)

    finally:
        await pipeline.close()


@pytest.mark.asyncio
async def test_pipeline_with_empty_response() -> None:
    """Test pipeline with empty API response."""

    class EmptyAPIServer:
        async def handle_request(self, _scope: dict, _receive: Any, send: Any) -> None:
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [[b"content-type", b"application/json"]],
                }
            )
            await send(
                {"type": "http.response.body", "body": b'{"data": [], "total": 0}'}
            )

    pipeline = APIIngestionPipeline(
        api_base_url="http://testserver",
        db_config=None,
        rate_limit_per_second=100,
        page_size=10,
    )

    await pipeline.initialize()

    try:
        users = await pipeline.fetch_users()

        assert len(users) == 0

    finally:
        await pipeline.close()
