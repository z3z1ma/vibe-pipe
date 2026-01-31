from __future__ import annotations

from datetime import datetime

import pytest

from vibe_piper.schema.field_mapper import map_record_to_schema
from vibe_piper.types import DataType, Schema, SchemaField


def test_map_record_to_schema_extracts_and_converts() -> None:
    schema = Schema(
        name="users",
        fields=(
            SchemaField(name="id", data_type=DataType.INTEGER, source_path="id"),
            SchemaField(
                name="company_name",
                data_type=DataType.STRING,
                required=False,
                nullable=True,
                source_path="company.name",
            ),
            SchemaField(
                name="first_tag",
                data_type=DataType.STRING,
                required=False,
                nullable=True,
                source_path="tags[0]",
            ),
            SchemaField(name="created_at", data_type=DataType.DATETIME, source_path="created_at"),
        ),
    )

    source = {
        "id": "123",
        "company": {"name": "Acme"},
        "tags": ["x", "y"],
        "created_at": "2024-01-01T12:00:00Z",
    }

    result = map_record_to_schema(schema, source)
    assert result.success is True
    assert result.record is not None
    assert result.data["id"] == 123
    assert result.data["company_name"] == "Acme"
    assert result.data["first_tag"] == "x"
    assert isinstance(result.data["created_at"], datetime)


def test_map_record_to_schema_missing_required_produces_errors() -> None:
    schema = Schema(
        name="users",
        fields=(
            SchemaField(name="id", data_type=DataType.INTEGER, source_path="id"),
            SchemaField(name="created_at", data_type=DataType.DATETIME, source_path="created_at"),
        ),
    )

    result = map_record_to_schema(schema, {"id": "1"})
    assert result.success is False
    assert result.record is None
    assert any("created_at" in err for err in result.errors)
    assert any("Missing source_path" in w for w in result.warnings)


def test_map_record_to_schema_conversion_error_is_collected() -> None:
    schema = Schema(
        name="users",
        fields=(SchemaField(name="id", data_type=DataType.INTEGER, source_path="id"),),
    )

    result = map_record_to_schema(schema, {"id": "not_an_int"})
    assert result.success is False
    assert result.record is None
    assert any("conversion to INTEGER" in err for err in result.errors)
