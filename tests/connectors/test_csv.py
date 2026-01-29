"""
Tests for CSV reader and writer.
"""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from vibe_piper.connectors.csv import CSVReader, CSVWriter
from vibe_piper.types import DataType, Schema, SchemaField

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_csv_data():
    """Sample data for CSV testing."""
    return [
        {"id": 1, "name": "Alice", "age": 30, "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "age": 25, "email": "bob@example.com"},
        {"id": 3, "name": "Charlie", "age": 35, "email": "charlie@example.com"},
        {"id": 4, "name": "Diana", "age": 28, "email": "diana@example.com"},
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
            SchemaField(name="email", data_type=DataType.STRING, required=True),
        ),
    )


@pytest.fixture
def temp_csv_file(sample_csv_data):
    """Create a temporary CSV file with sample data."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        # Write CSV header
        f.write("id,name,age,email\n")
        # Write data rows
        for row in sample_csv_data:
            f.write(f"{row['id']},{row['name']},{row['age']},{row['email']}\n")
        temp_path = f.name

    yield Path(temp_path)

    # Cleanup
    Path(temp_path).unlink(missing_ok=True)


# =============================================================================
# CSV Reader Tests
# =============================================================================


class TestCSVReader:
    """Tests for CSVReader class."""

    def test_read_csv_file(self, temp_csv_file):
        """Test reading a CSV file."""
        reader = CSVReader(temp_csv_file)
        records = reader.read()

        assert len(records) == 4
        assert records[0].data["id"] == 1
        assert records[0].data["name"] == "Alice"
        assert records[3].data["name"] == "Diana"

    def test_read_csv_with_schema(self, temp_csv_file, sample_schema):
        """Test reading CSV with schema validation."""
        reader = CSVReader(temp_csv_file)
        records = reader.read(schema=sample_schema)

        assert len(records) == 4
        assert records[0].schema.name == "test_schema"

    def test_infer_schema(self, temp_csv_file):
        """Test schema inference from CSV file."""
        reader = CSVReader(temp_csv_file)
        schema = reader.infer_schema()

        assert schema.name == temp_csv_file.stem
        assert len(schema.fields) == 4

        field_names = {f.name for f in schema.fields}
        assert "id" in field_names
        assert "name" in field_names
        assert "age" in field_names
        assert "email" in field_names

    def test_get_metadata(self, temp_csv_file):
        """Test getting CSV file metadata."""
        reader = CSVReader(temp_csv_file)
        metadata = reader.get_metadata()

        assert metadata["format"] == "csv"
        assert "size" in metadata
        assert "columns" in metadata
        assert metadata["columns"] == 4

    def test_detect_delimiter(self):
        """Test auto-detection of delimiters."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id;name;age\n1;Alice;30\n")
            temp_path = f.name

        try:
            reader = CSVReader(temp_path)
            delimiter = reader._detect_delimiter()
            assert delimiter == ";"
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_read_tsv_file(self):
        """Test reading a TSV (tab-separated) file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=False) as f:
            f.write("id\tname\tage\n1\tAlice\t30\n2\tBob\t25\n")
            temp_path = f.name

        try:
            reader = CSVReader(temp_path, delimiter="\t")
            records = reader.read()

            assert len(records) == 2
            assert records[0].data["name"] == "Alice"
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_read_with_nulls(self):
        """Test reading CSV with null values."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,name,age\n1,Alice,30\n2,Bob,\n3,Charlie,35\n")
            temp_path = f.name

        try:
            reader = CSVReader(temp_path)
            records = reader.read()

            assert len(records) == 3
            assert records[1].data["age"] is None
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_read_chunked(self, temp_csv_file):
        """Test chunked reading of CSV file."""
        reader = CSVReader(temp_csv_file)
        chunks = reader.read(chunk_size=2)

        chunk_list = list(chunks)
        assert len(chunk_list) == 2
        assert len(chunk_list[0]) == 2
        assert len(chunk_list[1]) == 2


# =============================================================================
# CSV Writer Tests
# =============================================================================


class TestCSVWriter:
    """Tests for CSVWriter class."""

    def test_write_csv_file(self, sample_csv_data, sample_schema):
        """Test writing data to a CSV file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.csv"

            # Create DataRecord objects
            from vibe_piper.types import DataRecord

            records = [DataRecord(data=row, schema=sample_schema) for row in sample_csv_data]

            # Write to CSV
            writer = CSVWriter(output_path)
            count = writer.write(records)

            assert count == 4
            assert output_path.exists()

            # Verify written data
            reader = CSVReader(output_path)
            read_records = reader.read()
            assert len(read_records) == 4

    def test_write_with_schema(self, sample_csv_data, sample_schema):
        """Test writing with schema for column ordering."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.csv"

            # Create DataRecord objects
            from vibe_piper.types import DataRecord

            records = [DataRecord(data=row, schema=sample_schema) for row in sample_csv_data]

            # Write to CSV
            writer = CSVWriter(output_path)
            writer.write(records, schema=sample_schema)

            # Read and verify column order
            df = pd.read_csv(output_path)
            assert list(df.columns) == ["id", "name", "age", "email"]

    def test_write_with_compression(self, sample_csv_data, sample_schema):
        """Test writing compressed CSV."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.csv.gz"

            # Create DataRecord objects
            from vibe_piper.types import DataRecord

            records = [DataRecord(data=row, schema=sample_schema) for row in sample_csv_data]

            # Write to compressed CSV
            writer = CSVWriter(output_path)
            count = writer.write(records, compression="gzip")

            assert count == 4
            assert output_path.exists()

    def test_write_append_mode(self, sample_csv_data, sample_schema):
        """Test appending to existing CSV file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.csv"

            # Create DataRecord objects
            from vibe_piper.types import DataRecord

            records1 = [DataRecord(data=row, schema=sample_schema) for row in sample_csv_data[:2]]
            records2 = [DataRecord(data=row, schema=sample_schema) for row in sample_csv_data[2:]]

            # Write initial data
            writer = CSVWriter(output_path)
            writer.write(records1, mode="w")

            # Append more data
            writer.write(records2, mode="a")

            # Verify
            reader = CSVReader(output_path)
            all_records = reader.read()
            assert len(all_records) == 4

    def test_write_empty_data_raises_error(self, sample_schema):
        """Test that writing empty data raises an error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.csv"

            writer = CSVWriter(output_path)

            with pytest.raises(ValueError, match="Cannot write empty data"):
                writer.write([], schema=sample_schema)

    def test_write_partitioned(self, sample_csv_data, sample_schema):
        """Test writing partitioned CSV files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "partitioned"

            # Add partition column to data
            for i, row in enumerate(sample_csv_data):
                row["department"] = ["Sales", "IT", "Sales", "IT"][i]

            # Create DataRecord objects
            from vibe_piper.types import DataRecord

            records = [DataRecord(data=row, schema=sample_schema) for row in sample_csv_data]

            # Write partitioned data
            writer = CSVWriter(output_path)
            paths = writer.write_partitioned(records, partition_cols=["department"])

            assert len(paths) == 2
            assert all(Path(p).exists() for p in paths)

    def test_write_custom_delimiter(self, sample_csv_data, sample_schema):
        """Test writing with custom delimiter."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.csv"

            # Create DataRecord objects
            from vibe_piper.types import DataRecord

            records = [DataRecord(data=row, schema=sample_schema) for row in sample_csv_data[:2]]

            # Write with semicolon delimiter
            writer = CSVWriter(output_path)
            writer.write(records, delimiter=";")

            # Verify delimiter
            with open(output_path) as f:
                content = f.read()
                assert ";" in content
