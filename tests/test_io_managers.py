"""
Unit tests for IO Managers.

Tests all IO manager implementations including memory, file, S3, and database.
"""

import json
import pickle
from pathlib import Path

import pytest

from vibe_piper.io_managers import (
    DatabaseIOManager,
    FileIOManager,
    MemoryIOManager,
    S3IOManager,
)
from vibe_piper.types import PipelineContext


class TestMemoryIOManager:
    """Tests for MemoryIOManager."""

    def test_create_manager(self) -> None:
        """Test creating a memory IO manager."""
        manager = MemoryIOManager()
        assert manager.storage == {}
        assert isinstance(manager.storage, dict)

    def test_handle_output_stores_data(self) -> None:
        """Test storing data in memory."""
        manager = MemoryIOManager()
        context = PipelineContext(pipeline_id="test_asset", run_id="run_1")

        data = {"key": "value"}
        manager.handle_output(context, data)

        assert "test_asset" in manager.storage
        assert manager.storage["test_asset"] == data

    def test_load_input_retrieves_data(self) -> None:
        """Test loading data from memory."""
        manager = MemoryIOManager()
        context = PipelineContext(pipeline_id="test_asset", run_id="run_1")

        data = {"key": "value"}
        manager.handle_output(context, data)

        loaded = manager.load_input(context)
        assert loaded == data

    def test_load_input_returns_none_for_missing(self) -> None:
        """Test loading non-existent data returns None."""
        manager = MemoryIOManager()
        context = PipelineContext(pipeline_id="nonexistent", run_id="run_1")

        loaded = manager.load_input(context)
        assert loaded is None

    def test_get_and_set(self) -> None:
        """Test direct get and set methods."""
        manager = MemoryIOManager()

        manager.set("custom_key", {"data": "test"})
        assert manager.get("custom_key") == {"data": "test"}
        assert manager.get("missing") is None

    def test_has_asset(self) -> None:
        """Test checking if asset exists."""
        manager = MemoryIOManager()

        assert not manager.has_asset("test")
        manager.set("test", {"data": "value"})
        assert manager.has_asset("test")

    def test_clear(self) -> None:
        """Test clearing all stored data."""
        manager = MemoryIOManager()
        context = PipelineContext(pipeline_id="test_asset", run_id="run_1")

        manager.handle_output(context, {"key": "value"})
        assert len(manager.storage) == 1

        manager.clear()
        assert len(manager.storage) == 0

    def test_multiple_assets(self) -> None:
        """Test storing multiple assets."""
        manager = MemoryIOManager()

        context1 = PipelineContext(pipeline_id="asset1", run_id="run_1")
        context2 = PipelineContext(pipeline_id="asset2", run_id="run_1")

        manager.handle_output(context1, {"data": "first"})
        manager.handle_output(context2, {"data": "second"})

        assert manager.storage["asset1"] == {"data": "first"}
        assert manager.storage["asset2"] == {"data": "second"}
        assert len(manager.storage) == 2


class TestFileIOManager:
    """Tests for FileIOManager."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        import tempfile

        self.temp_dir = tempfile.mkdtemp()
        self.manager = FileIOManager(base_path=self.temp_dir, format="json")

    def teardown_method(self) -> None:
        """Clean up test fixtures."""
        import shutil

        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)

    def test_create_manager(self) -> None:
        """Test creating a file IO manager."""
        assert self.manager.base_path == Path(self.temp_dir)
        assert self.manager.format == "json"
        assert self.manager.base_path.exists()

    def test_create_manager_invalid_format(self) -> None:
        """Test creating manager with invalid format raises error."""
        with pytest.raises(ValueError, match="Invalid format"):
            FileIOManager(base_path=self.temp_dir, format="invalid")

    def test_handle_output_creates_file(self) -> None:
        """Test storing data creates a file."""
        context = PipelineContext(pipeline_id="test_asset", run_id="run_1")

        data = {"key": "value"}
        self.manager.handle_output(context, data)

        file_path = self.manager._get_file_path(context)
        assert file_path.exists()

    def test_load_input_retrieves_data(self) -> None:
        """Test loading data from file."""
        context = PipelineContext(pipeline_id="test_asset", run_id="run_1")

        data = {"key": "value"}
        self.manager.handle_output(context, data)

        loaded = self.manager.load_input(context)
        assert loaded == data

    def test_load_input_file_not_found(self) -> None:
        """Test loading non-existent file raises error."""
        context = PipelineContext(pipeline_id="nonexistent", run_id="run_1")

        with pytest.raises(FileNotFoundError):
            self.manager.load_input(context)

    def test_json_format(self) -> None:
        """Test JSON format storage."""
        manager = FileIOManager(base_path=self.temp_dir, format="json")
        context = PipelineContext(pipeline_id="test_asset", run_id="run_1")

        data = {"key": "value", "number": 42}
        manager.handle_output(context, data)

        loaded = manager.load_input(context)
        assert loaded == data

    def test_pickle_format(self) -> None:
        """Test pickle format storage."""
        manager = FileIOManager(base_path=self.temp_dir, format="pickle")
        context = PipelineContext(pipeline_id="test_asset", run_id="run_1")

        data = {"key": "value", "number": 42}
        manager.handle_output(context, data)

        loaded = manager.load_input(context)
        assert loaded == data

    def test_csv_format(self) -> None:
        """Test CSV format storage."""
        manager = FileIOManager(base_path=self.temp_dir, format="csv")
        context = PipelineContext(pipeline_id="test_asset", run_id="run_1")

        data = [
            {"name": "Alice", "age": "30"},
            {"name": "Bob", "age": "25"},
        ]
        manager.handle_output(context, data)

        loaded = manager.load_input(context)
        assert len(loaded) == 2
        assert loaded[0]["name"] == "Alice"

    def test_csv_format_invalid_data(self) -> None:
        """Test CSV format with invalid data raises error."""
        manager = FileIOManager(base_path=self.temp_dir, format="csv")
        context = PipelineContext(pipeline_id="test_asset", run_id="run_1")

        data = "not a list of dicts"
        with pytest.raises(OSError, match="Failed to write file"):
            manager.handle_output(context, data)

    def test_has_asset(self) -> None:
        """Test checking if asset file exists."""
        context = PipelineContext(pipeline_id="test_asset", run_id="run_1")

        assert not self.manager.has_asset(context)

        self.manager.handle_output(context, {"key": "value"})
        assert self.manager.has_asset(context)

    def test_delete_asset(self) -> None:
        """Test deleting an asset file."""
        context = PipelineContext(pipeline_id="test_asset", run_id="run_1")

        self.manager.handle_output(context, {"key": "value"})
        assert self.manager.has_asset(context)

        self.manager.delete_asset(context)
        assert not self.manager.has_asset(context)


class TestS3IOManager:
    """Tests for S3IOManager."""

    def test_create_manager_requires_boto3(self) -> None:
        """Test that creating manager without boto3 raises ImportError."""
        # Check if boto3 is available
        try:
            import boto3  # noqa: F401

            # If boto3 is installed, skip this test
            pytest.skip("boto3 is installed, cannot test ImportError")
        except ImportError:
            # If boto3 is not available, test that ImportError is raised
            with pytest.raises(ImportError, match="boto3 is required"):
                S3IOManager(
                    bucket="test-bucket",
                    prefix="test",
                    format="json",
                )

    def test_s3_manager_with_boto3(self) -> None:
        """Test S3 manager when boto3 is available."""
        try:
            import boto3  # noqa: F401
        except ImportError:
            pytest.skip("boto3 not installed")

        manager = S3IOManager(
            bucket="test-bucket",
            prefix="test",
            format="json",
        )
        assert manager.bucket == "test-bucket"
        assert manager.prefix == "test"
        assert manager.format == "json"

    def test_create_manager_invalid_format(self) -> None:
        """Test creating manager with invalid format raises error."""
        try:
            import boto3  # noqa: F401
        except ImportError:
            pytest.skip("boto3 not installed")

        with pytest.raises(ValueError, match="Invalid format"):
            S3IOManager(
                bucket="test-bucket",
                prefix="test",
                format="invalid",
            )

    def test_serialize_json(self) -> None:
        """Test JSON serialization."""
        try:
            import boto3  # noqa: F401
        except ImportError:
            pytest.skip("boto3 not installed")

        manager = S3IOManager(bucket="test-bucket", format="json")
        data = {"key": "value", "number": 42}

        serialized = manager._serialize_data(data)
        assert isinstance(serialized, bytes)

        # Verify it's valid JSON
        loaded = json.loads(serialized.decode("utf-8"))
        assert loaded == data

    def test_serialize_pickle(self) -> None:
        """Test pickle serialization."""
        try:
            import boto3  # noqa: F401
        except ImportError:
            pytest.skip("boto3 not installed")

        manager = S3IOManager(bucket="test-bucket", format="pickle")
        data = {"key": "value", "number": 42}

        serialized = manager._serialize_data(data)
        assert isinstance(serialized, bytes)

        # Verify it can be unpickled
        loaded = pickle.loads(serialized)
        assert loaded == data

    def test_serialize_csv(self) -> None:
        """Test CSV serialization."""
        try:
            import boto3  # noqa: F401
        except ImportError:
            pytest.skip("boto3 not installed")

        manager = S3IOManager(bucket="test-bucket", format="csv")
        data = [
            {"name": "Alice", "age": "30"},
            {"name": "Bob", "age": "25"},
        ]

        serialized = manager._serialize_data(data)
        assert isinstance(serialized, bytes)

    def test_get_s3_key(self) -> None:
        """Test S3 key generation."""
        try:
            import boto3  # noqa: F401
        except ImportError:
            pytest.skip("boto3 not installed")

        manager = S3IOManager(
            bucket="test-bucket",
            prefix="assets",
            format="json",
        )

        context = PipelineContext(pipeline_id="test_asset", run_id="run_1")

        key = manager._get_s3_key(context)
        assert key == "assets/test_asset_run_1.json"


class TestDatabaseIOManager:
    """Tests for DatabaseIOManager."""

    def test_create_manager_requires_sqlalchemy(self) -> None:
        """Test that creating manager without sqlalchemy raises ImportError."""
        # This test assumes sqlalchemy is not installed in test environment
        # If sqlalchemy is installed, we test that it works correctly
        try:
            import sqlalchemy

            # If sqlalchemy is available, test with SQLite
            manager = DatabaseIOManager(
                connection_string="sqlite:///:memory:",
                table_name="test_assets",
            )
            assert manager.table_name == "test_assets"

        except ImportError:
            # If sqlalchemy is not available, test that ImportError is raised
            with pytest.raises(ImportError, match="sqlalchemy is required"):
                DatabaseIOManager(
                    connection_string="sqlite:///:memory:",
                )

    def test_get_asset_key(self) -> None:
        """Test asset key generation."""
        try:
            import sqlalchemy

            manager = DatabaseIOManager(
                connection_string="sqlite:///:memory:",
            )

            context = PipelineContext(pipeline_id="test_asset", run_id="run_1")

            key = manager._get_asset_key(context)
            assert key == "test_asset_run_1"

        except ImportError:
            pytest.skip("sqlalchemy not installed")

    def test_get_full_table_name(self) -> None:
        """Test full table name generation."""
        try:
            import sqlalchemy  # noqa: F401

            # Without schema
            manager = DatabaseIOManager(
                connection_string="sqlite:///:memory:",
                table_name="assets",
            )
            assert manager._get_full_table_name() == "assets"

            # With schema
            manager_with_schema = DatabaseIOManager(
                connection_string="sqlite:///:memory:",
                table_name="assets",
                schema="public",
            )
            assert manager_with_schema._get_full_table_name() == "public.assets"

        except ImportError:
            pytest.skip("sqlalchemy not installed")


class TestIOManagerIntegration:
    """Integration tests for IO managers."""

    def test_memory_io_manager_end_to_end(self) -> None:
        """Test end-to-end workflow with memory IO manager."""
        manager = MemoryIOManager()
        context = PipelineContext(pipeline_id="test_asset", run_id="run_1")

        # Store data
        data = {"users": [{"name": "Alice"}, {"name": "Bob"}]}
        manager.handle_output(context, data)

        # Load data
        loaded = manager.load_input(context)
        assert loaded == data

    def test_file_io_manager_end_to_end(self) -> None:
        """Test end-to-end workflow with file IO manager."""
        import tempfile

        temp_dir = tempfile.mkdtemp()
        manager = FileIOManager(base_path=temp_dir, format="json")
        context = PipelineContext(pipeline_id="test_asset", run_id="run_1")

        try:
            # Store data
            data = {"users": [{"name": "Alice"}, {"name": "Bob"}]}
            manager.handle_output(context, data)

            # Load data
            loaded = manager.load_input(context)
            assert loaded == data
        finally:
            import shutil

            if Path(temp_dir).exists():
                shutil.rmtree(temp_dir)

    def test_multiple_io_managers_isolated(self) -> None:
        """Test that multiple IO managers maintain isolation."""
        manager1 = MemoryIOManager()
        manager2 = MemoryIOManager()

        context = PipelineContext(pipeline_id="test_asset", run_id="run_1")

        data1 = {"value": "first"}
        data2 = {"value": "second"}

        manager1.handle_output(context, data1)
        manager2.handle_output(context, data2)

        # Each manager should have its own storage
        assert manager1.load_input(context) == data1
        assert manager2.load_input(context) == data2
