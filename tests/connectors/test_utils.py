"""
Tests for connector utilities.
"""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from vibe_piper.connectors.utils.compression import (
    compress_data,
    compress_file,
    decompress_file,
    detect_compression,
    get_compression_extension,
)
from vibe_piper.connectors.utils.inference import (
    infer_schema_from_data,
    infer_schema_from_file,
    infer_schema_from_pandas,
    refine_schema_with_sample,
)
from vibe_piper.connectors.utils.type_mapping import (
    map_schema_from_vibepiper,
    map_schema_to_vibepiper,
    map_type_from_vibepiper,
    map_type_to_vibepiper,
)
from vibe_piper.types import DataType, Schema, SchemaField

# =============================================================================
# Type Mapping Tests
# =============================================================================


class TestTypeMapping:
    """Tests for type mapping utilities."""

    def test_map_pandas_dtype_to_vibepiper(self):
        """Test mapping pandas dtypes to VibePiper types."""
        assert map_type_to_vibepiper("int64") == DataType.INTEGER
        assert map_type_to_vibepiper("float64") == DataType.FLOAT
        assert map_type_to_vibepiper("boolean") == DataType.BOOLEAN
        assert map_type_to_vibepiper("string") == DataType.STRING
        assert map_type_to_vibepiper("object") == DataType.STRING

    def test_map_json_type_to_vibepiper(self):
        """Test mapping JSON types to VibePiper types."""
        assert map_type_to_vibepiper("str", format="json") == DataType.STRING
        assert map_type_to_vibepiper("int", format="json") == DataType.INTEGER
        assert map_type_to_vibepiper("float", format="json") == DataType.FLOAT
        assert map_type_to_vibepiper("bool", format="json") == DataType.BOOLEAN

    def test_map_parquet_type_to_vibepiper(self):
        """Test mapping Parquet types to VibePiper types."""
        assert map_type_to_vibepiper("int32", format="parquet") == DataType.INTEGER
        assert map_type_to_vibepiper("double", format="parquet") == DataType.FLOAT
        assert map_type_to_vibepiper("timestamp", format="parquet") == DataType.DATETIME

    def test_map_vibepiper_to_pandas(self):
        """Test mapping VibePiper types to pandas dtypes."""
        assert map_type_from_vibepiper(DataType.INTEGER, format="pandas") == "int64"
        assert map_type_from_vibepiper(DataType.FLOAT, format="pandas") == "float64"
        assert map_type_from_vibepiper(DataType.STRING, format="pandas") == "string"

    def test_map_vibepiper_to_json(self):
        """Test mapping VibePiper types to JSON types."""
        assert map_type_from_vibepiper(DataType.INTEGER, format="json") == "integer"
        assert map_type_from_vibepiper(DataType.STRING, format="json") == "string"
        assert map_type_from_vibepiper(DataType.BOOLEAN, format="json") == "boolean"

    def test_map_schema_to_vibepiper(self):
        """Test mapping a schema dict to VibePiper types."""
        schema_dict = {"id": "int64", "name": "string", "age": "int64"}
        result = map_schema_to_vibepiper(schema_dict)

        assert result["id"] == DataType.INTEGER
        assert result["name"] == DataType.STRING
        assert result["age"] == DataType.INTEGER

    def test_map_schema_from_vibepiper(self):
        """Test mapping a VibePiper schema to format types."""
        schema_dict = {"id": DataType.INTEGER, "name": DataType.STRING}
        result = map_schema_from_vibepiper(schema_dict, format="pandas")

        assert result["id"] == "int64"
        assert result["name"] == "string"


# =============================================================================
# Compression Tests
# =============================================================================


class TestCompression:
    """Tests for compression utilities."""

    def test_detect_compression_from_extension(self):
        """Test detecting compression from file extension."""
        assert detect_compression("data.csv.gz") == "gzip"
        assert detect_compression("data.csv.zip") == "zip"
        assert detect_compression("data.csv") is None

    def test_get_compression_extension(self):
        """Test getting compression file extension."""
        assert get_compression_extension("gzip") == ".gz"
        assert get_compression_extension("zip") == ".zip"
        assert get_compression_extension(None) == ""

    def test_compress_data_gzip(self):
        """Test compressing data with gzip."""
        data = b"Hello, World!"
        compressed = compress_data(data, "gzip")

        assert isinstance(compressed, bytes)
        assert len(compressed) > 0

    def test_compress_data_string(self):
        """Test compressing string data."""
        data = "Hello, World!"
        compressed = compress_data(data, "gzip")

        assert isinstance(compressed, bytes)

    def test_unsupported_compression_type(self):
        """Test that unsupported compression raises an error."""
        with pytest.raises(ValueError, match="Unsupported compression type"):
            compress_data(b"data", "invalid")

    def test_compress_file(self):
        """Test compressing a file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a test file
            test_file = Path(tmpdir) / "test.txt"
            test_file.write_text("Hello, World!")

            # Compress it
            compressed_file = compress_file(test_file, compression="gzip")

            assert compressed_file.exists()
            assert compressed_file.suffix == ".gz"

    def test_decompress_file(self):
        """Test decompressing a file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create and compress a test file
            test_file = Path(tmpdir) / "test.txt"
            test_file.write_text("Hello, World!")

            compressed_file = compress_file(test_file, compression="gzip")

            # Decompress it
            decompressed_file = decompress_file(compressed_file)

            assert decompressed_file.exists()
            assert decompressed_file.read_text() == "Hello, World!"


# =============================================================================
# Schema Inference Tests
# =============================================================================


class TestSchemaInference:
    """Tests for schema inference utilities."""

    def test_infer_schema_from_pandas(self):
        """Test inferring schema from pandas DataFrame."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "age": [30, 25, 35],
                "score": [95.5, 87.3, 92.1],
            }
        )

        schema = infer_schema_from_pandas(df, name="test")

        assert schema.name == "test"
        assert len(schema.fields) == 4

        field_dict = {f.name: f for f in schema.fields}
        assert field_dict["id"].data_type == DataType.INTEGER
        assert field_dict["name"].data_type == DataType.STRING
        assert field_dict["score"].data_type == DataType.FLOAT

    def test_infer_schema_with_nulls(self):
        """Test inferring schema with null values."""
        df = pd.DataFrame(
            {
                "id": [1, 2, None],
                "name": ["Alice", None, "Charlie"],
            }
        )

        schema = infer_schema_from_pandas(df, name="test")

        field_dict = {f.name: f for f in schema.fields}
        assert field_dict["id"].nullable is True
        assert field_dict["name"].nullable is True

    def test_infer_schema_from_data(self):
        """Test inferring schema from list of dicts."""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]

        schema = infer_schema_from_data(data, name="test")

        assert schema.name == "test"
        assert len(schema.fields) == 2

    def test_infer_schema_from_empty_data(self):
        """Test inferring schema from empty data."""
        schema = infer_schema_from_data([], name="empty")

        assert schema.name == "empty"
        assert len(schema.fields) == 0

    def test_infer_schema_from_csv_file(self):
        """Test inferring schema from CSV file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a test CSV file
            test_file = Path(tmpdir) / "test.csv"
            test_file.write_text("id,name,age\n1,Alice,30\n2,Bob,25\n")

            schema = infer_schema_from_file(test_file, format="csv")

            assert schema.name == "test"
            assert len(schema.fields) == 3

    def test_refine_schema_with_sample(self):
        """Test refining schema with additional data samples."""
        # Initial schema
        schema = Schema(
            name="test",
            fields=(
                SchemaField(name="id", data_type=DataType.INTEGER, required=True),
                SchemaField(name="name", data_type=DataType.STRING, required=True),
            ),
        )

        # Sample data
        sample_data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": None},  # This should make name nullable
        ]

        refined = refine_schema_with_sample(schema, sample_data)

        # Name should now be nullable
        name_field = [f for f in refined.fields if f.name == "name"][0]
        assert name_field.nullable is True
