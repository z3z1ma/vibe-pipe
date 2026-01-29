"""
Tests for Excel reader and writer.
"""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from vibe_piper.connectors.excel import ExcelReader, ExcelWriter
from vibe_piper.types import DataRecord, DataType, Schema, SchemaField

try:
    import openpyxl  # noqa: F401

    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_excel_data():
    """Sample data for Excel testing."""
    return [
        {"id": 1, "name": "Alice", "department": "Sales", "salary": 50000},
        {"id": 2, "name": "Bob", "department": "IT", "salary": 60000},
        {"id": 3, "name": "Charlie", "department": "Sales", "salary": 55000},
    ]


@pytest.fixture
def sample_schema():
    """Sample schema for testing."""
    return Schema(
        name="test_schema",
        fields=(
            SchemaField(name="id", data_type=DataType.INTEGER, required=True),
            SchemaField(name="name", data_type=DataType.STRING, required=True),
            SchemaField(name="department", data_type=DataType.STRING, required=True),
            SchemaField(name="salary", data_type=DataType.FLOAT, required=True),
        ),
    )


# =============================================================================
# Excel Reader Tests
# =============================================================================


@pytest.mark.skipif(not HAS_OPENPYXL, reason="openpyxl not installed")
class TestExcelReader:
    """Tests for ExcelReader class."""

    def test_read_excel_file(self, sample_excel_data):
        """Test reading an Excel file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a temporary Excel file
            df = pd.DataFrame(sample_excel_data)
            temp_path = Path(tmpdir) / "test.xlsx"
            df.to_excel(temp_path, engine="openpyxl", index=False)

            # Read it back
            reader = ExcelReader(temp_path)
            records = reader.read()

            assert len(records) == 3
            assert records[0].data["id"] == 1
            assert records[0].data["name"] == "Alice"
            assert records[2].data["department"] == "Sales"

    def test_read_with_schema(self, sample_excel_data, sample_schema):
        """Test reading with schema validation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pd.DataFrame(sample_excel_data)
            temp_path = Path(tmpdir) / "test.xlsx"
            df.to_excel(temp_path, engine="openpyxl", index=False)

            reader = ExcelReader(temp_path)
            records = reader.read(schema=sample_schema)

            assert len(records) == 3
            assert records[0].schema.name == "test_schema"

    def test_infer_schema(self, sample_excel_data):
        """Test schema inference from Excel file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pd.DataFrame(sample_excel_data)
            temp_path = Path(tmpdir) / "test.xlsx"
            df.to_excel(temp_path, engine="openpyxl", index=False)

            reader = ExcelReader(temp_path)
            schema = reader.infer_schema()

            assert schema.name == temp_path.stem
            assert len(schema.fields) == 4

            field_names = {f.name for f in schema.fields}
            assert "id" in field_names
            assert "salary" in field_names

    def test_get_sheet_names(self, sample_excel_data):
        """Test getting sheet names from Excel file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pd.DataFrame(sample_excel_data)
            temp_path = Path(tmpdir) / "test.xlsx"
            df.to_excel(temp_path, engine="openpyxl", index=False)

            reader = ExcelReader(temp_path)
            sheet_names = reader.get_sheet_names()

            assert isinstance(sheet_names, list)
            assert len(sheet_names) >= 1
            assert "Sheet1" in sheet_names

    def test_read_all_sheets(self, sample_excel_data):
        """Test reading all sheets from Excel file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create Excel file with multiple sheets
            temp_path = Path(tmpdir) / "test.xlsx"
            with pd.ExcelWriter(temp_path, engine="openpyxl") as writer:
                pd.DataFrame(sample_excel_data).to_excel(writer, sheet_name="Data", index=False)
                pd.DataFrame([{"x": 1, "y": 2}]).to_excel(writer, sheet_name="Other", index=False)

            # Read all sheets
            reader = ExcelReader(temp_path)
            sheets = reader.read_all_sheets()

            assert "Data" in sheets
            assert "Other" in sheets
            assert len(sheets["Data"]) == 3
            assert len(sheets["Other"]) == 1

    def test_infer_schema_all_sheets(self, sample_excel_data):
        """Test inferring schemas for all sheets."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create Excel file with multiple sheets
            temp_path = Path(tmpdir) / "test.xlsx"
            with pd.ExcelWriter(temp_path, engine="openpyxl") as writer:
                pd.DataFrame(sample_excel_data).to_excel(writer, sheet_name="Data", index=False)
                pd.DataFrame([{"x": 1, "y": 2}]).to_excel(writer, sheet_name="Other", index=False)

            # Infer schemas
            reader = ExcelReader(temp_path)
            schemas = reader.infer_schema_all_sheets()

            assert "Data" in schemas
            assert "Other" in schemas
            assert len(schemas["Data"].fields) == 4
            assert len(schemas["Other"].fields) == 2


# =============================================================================
# Excel Writer Tests
# =============================================================================


@pytest.mark.skipif(not HAS_OPENPYXL, reason="openpyxl not installed")
class TestExcelWriter:
    """Tests for ExcelWriter class."""

    def test_write_excel_file(self, sample_excel_data, sample_schema):
        """Test writing data to an Excel file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.xlsx"

            # Create DataRecord objects
            records = [DataRecord(data=row, schema=sample_schema) for row in sample_excel_data]

            # Write to Excel
            writer = ExcelWriter(output_path)
            count = writer.write(records)

            assert count == 3
            assert output_path.exists()

            # Verify written data
            reader = ExcelReader(output_path)
            read_records = reader.read()
            assert len(read_records) == 3

    def test_write_with_schema(self, sample_excel_data, sample_schema):
        """Test writing with schema for column ordering."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.xlsx"

            # Create DataRecord objects
            records = [DataRecord(data=row, schema=sample_schema) for row in sample_excel_data]

            # Write to Excel
            writer = ExcelWriter(output_path)
            writer.write(records, schema=sample_schema)

            # Read and verify column order
            df = pd.read_excel(output_path, engine="openpyxl")
            assert list(df.columns) == ["id", "name", "department", "salary"]

    def test_write_custom_sheet_name(self, sample_excel_data, sample_schema):
        """Test writing to a custom sheet name."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.xlsx"

            # Create DataRecord objects
            records = [DataRecord(data=row, schema=sample_schema) for row in sample_excel_data]

            # Write to custom sheet
            writer = ExcelWriter(output_path)
            writer.write(records, sheet_name="MyData")

            # Verify sheet name
            reader = ExcelReader(output_path)
            sheet_names = reader.get_sheet_names()
            assert "MyData" in sheet_names

    def test_write_multiple_sheets(self, sample_excel_data, sample_schema):
        """Test writing multiple sheets to a workbook."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.xlsx"

            # Create DataRecord objects
            records1 = [DataRecord(data=row, schema=sample_schema) for row in sample_excel_data]
            other_schema = Schema(
                name="other_schema",
                fields=(
                    SchemaField(name="x", data_type=DataType.INTEGER),
                    SchemaField(name="y", data_type=DataType.INTEGER),
                ),
            )
            records2 = [DataRecord(data={"x": 1, "y": 2}, schema=other_schema)]

            # Write multiple sheets
            writer = ExcelWriter(output_path)
            writer.write(records1, sheet_name="Data", mode="w")
            writer.write(records2, sheet_name="Other", mode="a")

            # Verify
            reader = ExcelReader(output_path)
            sheets = reader.read_all_sheets()
            assert "Data" in sheets
            assert "Other" in sheets

    def test_write_all_sheets(self, sample_excel_data, sample_schema):
        """Test writing all sheets at once."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.xlsx"

            # Create DataRecord objects
            records1 = [DataRecord(data=row, schema=sample_schema) for row in sample_excel_data]
            other_schema = Schema(
                name="other_schema",
                fields=(
                    SchemaField(name="x", data_type=DataType.INTEGER),
                    SchemaField(name="y", data_type=DataType.INTEGER),
                ),
            )
            records2 = [DataRecord(data={"x": 1, "y": 2}, schema=other_schema)]

            # Write all sheets
            writer = ExcelWriter(output_path)
            total = writer.write_all_sheets({"Data": records1, "Other": records2})

            assert total == 4  # 3 + 1

            # Verify
            reader = ExcelReader(output_path)
            sheets = reader.read_all_sheets()
            assert len(sheets["Data"]) == 3
            assert len(sheets["Other"]) == 1

    def test_write_with_compression_raises_error(self, sample_excel_data, sample_schema):
        """Test that compression raises an error for Excel."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.xlsx"

            # Create DataRecord objects
            records = [DataRecord(data=row, schema=sample_schema) for row in sample_excel_data]

            writer = ExcelWriter(output_path)

            with pytest.raises(ValueError, match="Compression not supported"):
                writer.write(records, compression="gzip")

    def test_write_empty_data_raises_error(self, sample_schema):
        """Test that writing empty data raises an error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.xlsx"

            writer = ExcelWriter(output_path)

            with pytest.raises(ValueError, match="Cannot write empty data"):
                writer.write([], schema=sample_schema)

    def test_write_partitioned(self, sample_excel_data, sample_schema):
        """Test writing partitioned Excel files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "partitioned"

            # Add partition column to data
            for i, row in enumerate(sample_excel_data):
                row["category"] = ["A", "B", "A"][i]

            # Create DataRecord objects
            records = [DataRecord(data=row, schema=sample_schema) for row in sample_excel_data]

            # Write partitioned data
            writer = ExcelWriter(output_path)
            paths = writer.write_partitioned(records, partition_cols=["category"])

            assert len(paths) == 2
            assert all(Path(p).exists() for p in paths)
