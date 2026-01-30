"""
Tests for File Source
"""

import asyncio
from collections.abc import Sequence
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from vibe_piper.sources.file import FileConfig, FileSource
from vibe_piper.types import PipelineContext


@pytest.fixture
def sample_csv_data():
    """Sample CSV data."""
    return """id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com
"""


@pytest.fixture
def sample_json_data():
    """Sample JSON data."""
    return """[
    {"id": 1, "name": "Alice", "email": "alice@example.com"},
    {"id": 2, "name": "Bob", "email": "bob@example.com"}
]"""


class TestFileConfig:
    """Tests for file configuration."""

    def test_csv_config(self):
        """Test CSV file configuration."""
        config = FileConfig(
            name="users",
            path="data/users.csv",
            format="csv",
        )
        assert config.name == "users"
        assert config.format == "csv"
        assert str(config.path) == "data/users.csv"

    def test_json_config(self):
        """Test JSON file configuration."""
        config = FileConfig(
            name="users",
            path="data/users.json",
            format="json",
        )
        assert config.name == "users"
        assert config.format == "json"

    def test_glob_pattern_config(self):
        """Test glob pattern configuration."""
        config = FileConfig(
            name="users",
            path="data/",
            pattern="users_*.csv",
        )
        assert config.pattern == "users_*.csv"

    def test_encoding_config(self):
        """Test encoding configuration."""
        config = FileConfig(
            name="users",
            path="data/users.csv",
            format="csv",
            encoding="utf-16",
        )
        assert config.encoding == "utf-16"


class TestFileSource:
    """Tests for file source."""

    @pytest.mark.asyncio
    async def test_fetch_csv_file(self, sample_csv_data, tmp_path):
        """Test fetching CSV file."""
        file_path = tmp_path / "users.csv"
        file_path.write_text(sample_csv_data)

        config = FileConfig(
            name="users",
            path=file_path,
            format="csv",
        )

        source = FileSource(config)
        context = PipelineContext(pipeline_id="test_pipeline", run_id="test_run_1")

        records = await source.fetch(context)

        assert isinstance(records, Sequence)
        assert len(records) > 0

    @pytest.mark.asyncio
    async def test_fetch_json_file(self, sample_json_data, tmp_path):
        """Test fetching JSON file."""
        file_path = tmp_path / "users.json"
        file_path.write_text(sample_json_data)

        config = FileConfig(
            name="users",
            path=file_path,
            format="json",
        )

        source = FileSource(config)
        context = PipelineContext(pipeline_id="test_pipeline", run_id="test_run_1")

        records = await source.fetch(context)

        assert isinstance(records, Sequence)
        assert len(records) > 0

    @pytest.mark.asyncio
    async def test_fetch_multiple_files(self, sample_csv_data, tmp_path):
        """Test fetching multiple files with glob pattern."""
        # Create multiple files
        for i in range(3):
            file_path = tmp_path / f"users_{i}.csv"
            file_path.write_text(sample_csv_data)

        config = FileConfig(
            name="users",
            path=tmp_path,
            pattern="users_*.csv",
        )

        source = FileSource(config)
        context = PipelineContext(pipeline_id="test_pipeline", run_id="test_run_1")

        records = await source.fetch(context)

        # Should get records from all 3 files
        assert isinstance(records, Sequence)
        assert len(records) > 0

    @pytest.mark.asyncio
    async def test_stream_csv_file(self, sample_csv_data, tmp_path):
        """Test streaming CSV file."""
        file_path = tmp_path / "users.csv"
        file_path.write_text(sample_csv_data)

        config = FileConfig(
            name="users",
            path=file_path,
            format="csv",
            chunk_size=10,
        )

        source = FileSource(config)
        context = PipelineContext(pipeline_id="test_pipeline", run_id="test_run_1")

        count = 0
        async for _ in source.stream(context):
            count += 1

        assert count > 0

    def test_infer_schema_csv(self, sample_csv_data, tmp_path):
        """Test schema inference from CSV file."""
        file_path = tmp_path / "users.csv"
        file_path.write_text(sample_csv_data)

        config = FileConfig(
            name="users",
            path=file_path,
            format="csv",
        )

        source = FileSource(config)
        schema = source.infer_schema()

        assert schema is not None
        assert schema.name == "users"
        assert len(schema.fields) > 0

    def test_infer_schema_json(self, sample_json_data, tmp_path):
        """Test schema inference from JSON file."""
        file_path = tmp_path / "users.json"
        file_path.write_text(sample_json_data)

        config = FileConfig(
            name="users",
            path=file_path,
            format="json",
        )

        source = FileSource(config)
        schema = source.infer_schema()

        assert schema is not None
        assert schema.name == "users"
        assert len(schema.fields) > 0

    def test_auto_detect_format(self, sample_csv_data, tmp_path):
        """Test auto format detection from file extension."""
        file_path = tmp_path / "users.csv"
        file_path.write_text(sample_csv_data)

        config = FileConfig(
            name="users",
            path=file_path,
        )

        source = FileSource(config)
        format_type = source._detect_format(file_path)

        assert format_type == "csv"

    def test_get_metadata(self):
        """Test getting metadata."""
        config = FileConfig(
            name="users",
            path="data/",
            pattern="users_*.csv",
            format="csv",
        )

        source = FileSource(config)
        metadata = source.get_metadata()

        assert metadata["source_type"] == "file"
        assert metadata["name"] == "users"
        assert metadata["format"] == "csv"
        assert metadata["pattern"] == "users_*.csv"


@pytest.fixture
def tmp_path():
    """Create temporary directory for tests."""
    with TemporaryDirectory() as tmp:
        yield Path(tmp)
