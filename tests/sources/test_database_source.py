"""
Tests for Database Source
"""

import asyncio
from collections.abc import Sequence
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

import pytest

from vibe_piper.sources.database import (
    DatabaseConfig,
    DatabaseConnectionConfig,
    DatabaseSource,
)
from vibe_piper.types import PipelineContext


@pytest.fixture
def mock_query_result():
    """Sample database query result."""

    class MockQueryResult:
        rows = [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "email": "bob@example.com"},
        ]
        row_count = 2
        columns = ["id", "name", "email"]
        query = "SELECT * FROM users"

    return MockQueryResult()


@pytest.fixture
def mock_db_connector():
    """Create mock database connector."""
    connector = MagicMock()
    connector.connect = MagicMock()
    connector.execute_query = MagicMock()
    connector.execute_query.return_value = mock_query_result()
    return connector


class TestDatabaseConfig:
    """Tests for database configuration."""

    def test_postgres_config(self):
        """Test PostgreSQL configuration."""
        config = DatabaseConfig(
            name="users",
            connection=DatabaseConnectionConfig(
                type="postgres",
                host="localhost",
                port=5432,
                database="mydb",
                user="user",
                password="pass",
            ),
        )
        assert config.name == "users"
        assert config.connection.type == "postgres"
        assert config.connection.host == "localhost"
        assert config.connection.port == 5432

    def test_incremental_config(self):
        """Test incremental loading configuration."""
        config = DatabaseConfig(
            name="users",
            connection=DatabaseConnectionConfig(
                type="postgres",
                host="localhost",
                database="mydb",
                user="user",
                password="pass",
            ),
            table="users",
            incremental=True,
            watermark_column="updated_at",
            watermark_path="/tmp/watermark.json",
        )
        assert config.incremental is True
        assert config.watermark_column == "updated_at"


class TestDatabaseSource:
    """Tests for database source."""

    @pytest.mark.asyncio
    async def test_fetch_with_query(self, mock_db_connector):
        """Test fetching with explicit query."""
        config = DatabaseConfig(
            name="users",
            connection=DatabaseConnectionConfig(
                type="postgres",
                host="localhost",
                database="mydb",
                user="user",
                password="pass",
            ),
            query="SELECT * FROM users WHERE active = true",
        )

        source = DatabaseSource(config)
        source._connector = mock_db_connector

        context = PipelineContext(pipeline_id="test_pipeline", run_id="test_run_1")

        records = await source.fetch(context)

        assert isinstance(records, Sequence)
        assert len(records) > 0

    @pytest.mark.asyncio
    async def test_fetch_with_table(self, mock_db_connector):
        """Test fetching with table name."""
        config = DatabaseConfig(
            name="users",
            connection=DatabaseConnectionConfig(
                type="postgres",
                host="localhost",
                database="mydb",
                user="user",
                password="pass",
            ),
            table="users",
        )

        source = DatabaseSource(config)
        source._connector = mock_db_connector

        context = PipelineContext(pipeline_id="test_pipeline", run_id="test_run_1")

        records = await source.fetch(context)

        assert isinstance(records, Sequence)

    @pytest.mark.asyncio
    async def test_stream_database(self, mock_db_connector):
        """Test streaming database data."""
        config = DatabaseConfig(
            name="users",
            connection=DatabaseConnectionConfig(
                type="postgres",
                host="localhost",
                database="mydb",
                user="user",
                password="pass",
            ),
            table="users",
        )

        source = DatabaseSource(config)
        source._connector = mock_db_connector

        context = PipelineContext(pipeline_id="test_pipeline", run_id="test_run_1")

        count = 0
        async for _ in source.stream(context):
            count += 1

        assert count > 0

    def test_infer_schema(self, mock_db_connector):
        """Test schema inference from database results."""
        config = DatabaseConfig(
            name="users",
            connection=DatabaseConnectionConfig(
                type="postgres",
                host="localhost",
                database="mydb",
                user="user",
                password="pass",
            ),
            table="users",
        )

        source = DatabaseSource(config)
        source._connector = mock_db_connector

        schema = source.infer_schema()

        assert schema is not None
        assert schema.name == "users"
        assert len(schema.fields) > 0

    @pytest.mark.asyncio
    async def test_incremental_loading_with_watermark(self, mock_db_connector, tmp_path):
        """Test incremental loading with watermark tracking."""
        watermark_file = tmp_path / "watermark.json"

        config = DatabaseConfig(
            name="users",
            connection=DatabaseConnectionConfig(
                type="postgres",
                host="localhost",
                database="mydb",
                user="user",
                password="pass",
            ),
            table="users",
            incremental=True,
            watermark_column="updated_at",
            watermark_path=str(watermark_file),
        )

        source = DatabaseSource(config)
        source._connector = mock_db_connector

        # First fetch - no watermark
        context = PipelineContext(pipeline_id="test_pipeline", run_id="test_run_1")
        await source.fetch(context)

        # Check watermark file was created
        assert watermark_file.exists()

    def test_get_metadata(self):
        """Test getting metadata."""
        config = DatabaseConfig(
            name="users",
            connection=DatabaseConnectionConfig(
                type="postgres",
                host="localhost",
                database="mydb",
                user="user",
                password="pass",
            ),
            table="users",
            incremental=True,
            watermark_column="updated_at",
        )

        source = DatabaseSource(config)
        metadata = source.get_metadata()

        assert metadata["source_type"] == "database"
        assert metadata["name"] == "users"
        assert metadata["database_type"] == "postgres"
        assert metadata["incremental"] is True
        assert metadata["watermark_column"] == "updated_at"


@pytest.fixture
def tmp_path():
    """Create temporary directory for tests."""
    with TemporaryDirectory() as tmp:
        yield Path(tmp)
