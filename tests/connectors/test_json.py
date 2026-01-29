"""
Tests for JSON reader and writer.
"""

import json
import tempfile
from pathlib import Path

import pytest

from vibe_piper.connectors.json import JSONReader, JSONWriter
from vibe_piper.types import DataRecord, DataType, Schema, SchemaField

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_json_data():
    """Sample data for JSON testing."""
    return [
        {"id": 1, "name": "Alice", "age": 30, "active": True},
        {"id": 2, "name": "Bob", "age": 25, "active": False},
        {"id": 3, "name": "Charlie", "age": 35, "active": True},
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
            SchemaField(name="active", data_type=DataType.BOOLEAN, required=True),
        ),
    )


@pytest.fixture
def temp_json_file(sample_json_data):
    """Create a temporary JSON file with sample data."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(sample_json_data, f)
        temp_path = f.name

    yield Path(temp_path)

    Path(temp_path).unlink(missing_ok=True)


@pytest.fixture
def temp_ndjson_file(sample_json_data):
    """Create a temporary NDJSON file with sample data."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
        for row in sample_json_data:
            f.write(json.dumps(row) + "\n")
        temp_path = f.name

    yield Path(temp_path)

    Path(temp_path).unlink(missing_ok=True)


# =============================================================================
# JSON Reader Tests
# =============================================================================


class TestJSONReader:
    """Tests for JSONReader class."""

    def test_read_json_file(self, temp_json_file):
        """Test reading a JSON file."""
        reader = JSONReader(temp_json_file)
        records = reader.read()

        assert len(records) == 3
        assert records[0].data["id"] == 1
        assert records[0].data["name"] == "Alice"
        assert records[2].data["active"] is True

    def test_read_ndjson_file(self, temp_ndjson_file):
        """Test reading an NDJSON file."""
        reader = JSONReader(temp_ndjson_file)
        records = reader.read(lines=True)

        assert len(records) == 3
        assert records[0].data["name"] == "Alice"

    def test_auto_detect_ndjson(self, temp_ndjson_file):
        """Test auto-detection of NDJSON format."""
        reader = JSONReader(temp_ndjson_file)
        records = reader.read()  # Should auto-detect from .jsonl extension

        assert len(records) == 3

    def test_read_with_schema(self, temp_json_file, sample_schema):
        """Test reading with schema validation."""
        reader = JSONReader(temp_json_file)
        records = reader.read(schema=sample_schema)

        assert len(records) == 3
        assert records[0].schema.name == "test_schema"

    def test_infer_schema(self, temp_json_file):
        """Test schema inference from JSON file."""
        reader = JSONReader(temp_json_file)
        schema = reader.infer_schema()

        assert schema.name == temp_json_file.stem
        assert len(schema.fields) == 4

        field_names = {f.name for f in schema.fields}
        assert "id" in field_names
        assert "name" in field_names

    def test_get_metadata(self, temp_json_file):
        """Test getting JSON file metadata."""
        reader = JSONReader(temp_json_file)
        metadata = reader.get_metadata()

        assert metadata["format"] == "json"
        assert "size" in metadata
        assert "fields" in metadata


# =============================================================================
# JSON Writer Tests
# =============================================================================


class TestJSONWriter:
    """Tests for JSONWriter class."""

    def test_write_json_file(self, sample_json_data, sample_schema):
        """Test writing data to a JSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.json"

            # Create DataRecord objects
            records = [DataRecord(data=row, schema=sample_schema) for row in sample_json_data]

            # Write to JSON
            writer = JSONWriter(output_path)
            count = writer.write(records)

            assert count == 3
            assert output_path.exists()

            # Verify written data
            with open(output_path) as f:
                data = json.load(f)
                assert len(data) == 3

    def test_write_ndjson_file(self, sample_json_data, sample_schema):
        """Test writing to NDJSON format."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.jsonl"

            # Create DataRecord objects
            records = [DataRecord(data=row, schema=sample_schema) for row in sample_json_data]

            # Write to NDJSON
            writer = JSONWriter(output_path)
            count = writer.write(records, lines=True)

            assert count == 3
            assert output_path.exists()

            # Verify format
            with open(output_path) as f:
                lines = f.readlines()
                assert len(lines) == 3
                # Each line should be valid JSON
                for line in lines:
                    json.loads(line)

    def test_write_with_pretty_print(self, sample_json_data, sample_schema):
        """Test writing with indentation for pretty printing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.json"

            # Create DataRecord objects
            records = [DataRecord(data=row, schema=sample_schema) for row in sample_json_data]

            # Write with indentation
            writer = JSONWriter(output_path)
            writer.write(records, indent=2)

            # Verify formatting
            with open(output_path) as f:
                content = f.read()
                assert "  " in content  # Should have indentation

    def test_write_with_compression(self, sample_json_data, sample_schema):
        """Test writing compressed JSON."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.json.gz"

            # Create DataRecord objects
            records = [DataRecord(data=row, schema=sample_schema) for row in sample_json_data]

            # Write to compressed JSON
            writer = JSONWriter(output_path)
            count = writer.write(records, compression="gzip")

            assert count == 3
            assert output_path.exists()

    def test_write_append_mode_ndjson(self, sample_json_data, sample_schema):
        """Test appending to NDJSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.jsonl"

            # Create DataRecord objects
            records1 = [DataRecord(data=row, schema=sample_schema) for row in sample_json_data[:1]]
            records2 = [DataRecord(data=row, schema=sample_schema) for row in sample_json_data[1:]]

            # Write initial data
            writer = JSONWriter(output_path)
            writer.write(records1, lines=True, mode="w")

            # Append more data
            writer.write(records2, lines=True, mode="a")

            # Verify
            reader = JSONReader(output_path)
            all_records = reader.read(lines=True)
            assert len(all_records) == 3

    def test_write_empty_data_raises_error(self, sample_schema):
        """Test that writing empty data raises an error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.json"

            writer = JSONWriter(output_path)

            with pytest.raises(ValueError, match="Cannot write empty data"):
                writer.write([], schema=sample_schema)

    def test_write_partitioned(self, sample_json_data, sample_schema):
        """Test writing partitioned JSON files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "partitioned"

            # Add partition column to data
            for i, row in enumerate(sample_json_data):
                row["category"] = ["A", "B", "A"][i]

            # Create DataRecord objects
            records = [DataRecord(data=row, schema=sample_schema) for row in sample_json_data]

            # Write partitioned data
            writer = JSONWriter(output_path)
            paths = writer.write_partitioned(records, partition_cols=["category"])

            assert len(paths) == 2
            assert all(Path(p).exists() for p in paths)
