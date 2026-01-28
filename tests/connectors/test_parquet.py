"""
Tests for Parquet reader and writer.
"""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from vibe_piper.connectors.parquet import ParquetReader, ParquetWriter
from vibe_piper.types import DataRecord, DataType, Schema, SchemaField

try:
    import pyarrow  # noqa: F401

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_parquet_data():
    """Sample data for Parquet testing."""
    return [
        {"id": 1, "name": "Alice", "age": 30, "score": 95.5},
        {"id": 2, "name": "Bob", "age": 25, "score": 87.3},
        {"id": 3, "name": "Charlie", "age": 35, "score": 92.1},
    ]


@pytest.fixture
def sample_schema():
    """Sample schema for testing."""
    return Schema(
        name="test_schema",
        fields=(
            SchemaField(name="id", data_type=DataType.INTEGER, required=True),
            SchemaField(name="name", data_type=DataType.STRING, required=True),
            SchemaField(name="age", data_type=DataType.INTEGER, required=True),
            SchemaField(name="score", data_type=DataType.FLOAT, required=True),
        ),
    )


# =============================================================================
# Parquet Reader Tests
# =============================================================================


@pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")
class TestParquetReader:
    """Tests for ParquetReader class."""

    def test_read_parquet_file(self, sample_parquet_data):
        """Test reading a Parquet file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a temporary Parquet file
            df = pd.DataFrame(sample_parquet_data)
            temp_path = Path(tmpdir) / "test.parquet"
            df.to_parquet(temp_path, engine="pyarrow")

            # Read it back
            reader = ParquetReader(temp_path)
            records = reader.read()

            assert len(records) == 3
            assert records[0].data["id"] == 1
            assert records[0].data["name"] == "Alice"
            assert records[2].data["score"] == 92.1

    def test_read_with_schema(self, sample_parquet_data, sample_schema):
        """Test reading with schema validation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pd.DataFrame(sample_parquet_data)
            temp_path = Path(tmpdir) / "test.parquet"
            df.to_parquet(temp_path, engine="pyarrow")

            reader = ParquetReader(temp_path)
            records = reader.read(schema=sample_schema)

            assert len(records) == 3
            assert records[0].schema.name == "test_schema"

    def test_infer_schema(self, sample_parquet_data):
        """Test schema inference from Parquet file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pd.DataFrame(sample_parquet_data)
            temp_path = Path(tmpdir) / "test.parquet"
            df.to_parquet(temp_path, engine="pyarrow")

            reader = ParquetReader(temp_path)
            schema = reader.infer_schema()

            assert schema.name == temp_path.stem
            assert len(schema.fields) == 4

            field_names = {f.name for f in schema.fields}
            assert "id" in field_names
            assert "score" in field_names

    def test_get_metadata(self, sample_parquet_data):
        """Test getting Parquet file metadata."""
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pd.DataFrame(sample_parquet_data)
            temp_path = Path(tmpdir) / "test.parquet"
            df.to_parquet(temp_path, engine="pyarrow")

            reader = ParquetReader(temp_path)
            metadata = reader.get_metadata()

            assert metadata["format"] == "parquet"
            assert "rows" in metadata
            assert metadata["rows"] == 3

    def test_read_specific_columns(self, sample_parquet_data):
        """Test reading specific columns from Parquet file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pd.DataFrame(sample_parquet_data)
            temp_path = Path(tmpdir) / "test.parquet"
            df.to_parquet(temp_path, engine="pyarrow")

            reader = ParquetReader(temp_path)
            records = reader.read(columns=["id", "name"])

            assert len(records) == 3
            assert "id" in records[0].data
            assert "name" in records[0].data
            assert "age" not in records[0].data


# =============================================================================
# Parquet Writer Tests
# =============================================================================


@pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")
class TestParquetWriter:
    """Tests for ParquetWriter class."""

    def test_write_parquet_file(self, sample_parquet_data, sample_schema):
        """Test writing data to a Parquet file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.parquet"

            # Create DataRecord objects
            records = [
                DataRecord(data=row, schema=sample_schema)
                for row in sample_parquet_data
            ]

            # Write to Parquet
            writer = ParquetWriter(output_path)
            count = writer.write(records)

            assert count == 3
            assert output_path.exists()

            # Verify written data
            reader = ParquetReader(output_path)
            read_records = reader.read()
            assert len(read_records) == 3

    def test_write_with_compression(self, sample_parquet_data, sample_schema):
        """Test writing with different compression codecs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            for compression in ["snappy", "gzip"]:
                output_path = Path(tmpdir) / f"output_{compression}.parquet"

                # Create DataRecord objects
                records = [
                    DataRecord(data=row, schema=sample_schema)
                    for row in sample_parquet_data
                ]

                # Write with compression
                writer = ParquetWriter(output_path)
                count = writer.write(records, compression=compression)

                assert count == 3
                assert output_path.exists()

                # Verify it can be read back
                reader = ParquetReader(output_path)
                read_records = reader.read()
                assert len(read_records) == 3

    def test_write_with_unsupported_compression(
        self, sample_parquet_data, sample_schema
    ):
        """Test that unsupported compression raises an error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.parquet"

            # Create DataRecord objects
            records = [
                DataRecord(data=row, schema=sample_schema)
                for row in sample_parquet_data
            ]

            writer = ParquetWriter(output_path)

            with pytest.raises(ValueError, match="Unsupported compression"):
                writer.write(records, compression="invalid_codec")

    def test_write_append_mode(self, sample_parquet_data, sample_schema):
        """Test appending to existing Parquet file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.parquet"

            # Create DataRecord objects
            records1 = [
                DataRecord(data=row, schema=sample_schema)
                for row in sample_parquet_data[:1]
            ]
            records2 = [
                DataRecord(data=row, schema=sample_schema)
                for row in sample_parquet_data[1:]
            ]

            # Write initial data
            writer = ParquetWriter(output_path)
            writer.write(records1, mode="w")

            # Append more data
            writer.write(records2, mode="a")

            # Verify
            reader = ParquetReader(output_path)
            all_records = reader.read()
            assert len(all_records) == 3

    def test_write_partitioned(self, sample_parquet_data, sample_schema):
        """Test writing partitioned Parquet dataset."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "partitioned"

            # Add partition column to data
            for i, row in enumerate(sample_parquet_data):
                row["category"] = ["A", "B", "A"][i]

            # Create DataRecord objects
            records = [
                DataRecord(data=row, schema=sample_schema)
                for row in sample_parquet_data
            ]

            # Write partitioned data
            writer = ParquetWriter(output_path)
            paths = writer.write_partitioned(records, partition_cols=["category"])

            assert len(paths) > 0
            assert all(Path(p).exists() for p in paths)

    def test_write_empty_data_raises_error(self, sample_schema):
        """Test that writing empty data raises an error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.parquet"

            writer = ParquetWriter(output_path)

            with pytest.raises(ValueError, match="Cannot write empty data"):
                writer.write([], schema=sample_schema)
