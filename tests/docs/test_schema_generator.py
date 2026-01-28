"""Tests for the schema documentation generator."""

import json
from pathlib import Path

import pytest

from vibe_piper.docs.schema import SchemaDocGenerator
from vibe_piper.types import (
    Asset,
    AssetType,
    DataType,
    MaterializationStrategy,
    Schema,
    SchemaField,
)


@pytest.fixture
def sample_schema() -> Schema:
    """Create a sample schema for testing."""
    return Schema(
        name="test_schema",
        fields=(
            SchemaField(
                name="id",
                data_type=DataType.INTEGER,
                required=True,
                nullable=False,
                description="Unique identifier",
            ),
            SchemaField(
                name="name",
                data_type=DataType.STRING,
                required=True,
                nullable=False,
                description="User name",
            ),
            SchemaField(
                name="email",
                data_type=DataType.STRING,
                required=False,
                nullable=True,
                description="User email address",
            ),
        ),
        description="Test schema for unit tests",
    )


@pytest.fixture
def sample_assets(sample_schema: Schema) -> list[Asset]:
    """Create sample assets for testing."""
    return [
        Asset(
            name="users",
            asset_type=AssetType.TABLE,
            uri="table://users",
            schema=sample_schema,
            description="User table",
            metadata={"owner": "data-team"},
            config={},
            io_manager="memory",
            materialization=MaterializationStrategy.TABLE,
        ),
        Asset(
            name="users_view",
            asset_type=AssetType.VIEW,
            uri="view://users_view",
            schema=sample_schema,
            description="User view",
            metadata={},
            config={},
            io_manager="memory",
            materialization=MaterializationStrategy.VIEW,
        ),
    ]


def test_schema_doc_generator_initialization(tmp_path: Path) -> None:
    """Test that SchemaDocGenerator initializes correctly."""
    generator = SchemaDocGenerator(output_dir=tmp_path)

    assert generator.output_dir == tmp_path
    assert generator.template_dir is None
    assert generator.context == {}


def test_schema_doc_generator_creates_output_dir(
    tmp_path: Path, sample_assets: list[Asset]
) -> None:
    """Test that generator creates output directory."""
    output_dir = tmp_path / "output"
    generator = SchemaDocGenerator(output_dir=output_dir)

    generator.generate(sample_assets)

    assert output_dir.exists()
    assert output_dir.is_dir()


def test_schema_doc_generator_generates_json(
    tmp_path: Path, sample_assets: list[Asset]
) -> None:
    """Test that generator creates JSON files for schemas."""
    generator = SchemaDocGenerator(output_dir=tmp_path)
    generator.generate(sample_assets)

    # Check that schema JSON file was created
    schema_file = tmp_path / "schema_test_schema.json"
    assert schema_file.exists()

    # Load and verify content
    with schema_file.open() as f:
        data = json.load(f)

    assert data["name"] == "test_schema"
    assert data["description"] == "Test schema for unit tests"
    assert len(data["fields"]) == 3
    assert data["field_count"] == 3
    assert len(data["used_by"]) == 2


def test_schema_doc_generator_field_structure(
    tmp_path: Path, sample_assets: list[Asset]
) -> None:
    """Test that field structure is correctly serialized."""
    generator = SchemaDocGenerator(output_dir=tmp_path)
    generator.generate(sample_assets)

    schema_file = tmp_path / "schema_test_schema.json"
    with schema_file.open() as f:
        data = json.load(f)

    fields = data["fields"]
    assert len(fields) == 3

    # Check first field (id)
    id_field = fields[0]
    assert id_field["name"] == "id"
    assert id_field["type"] == "integer"
    assert id_field["required"] is True
    assert id_field["nullable"] is False
    assert id_field["description"] == "Unique identifier"

    # Check third field (email - optional)
    email_field = fields[2]
    assert email_field["name"] == "email"
    assert email_field["required"] is False
    assert email_field["nullable"] is True


def test_schema_doc_generator_with_assets_without_schema(tmp_path: Path) -> None:
    """Test generator with assets that don't have schemas."""
    assets = [
        Asset(
            name="raw_data",
            asset_type=AssetType.FILE,
            uri="file://raw_data",
            schema=None,
            description="Raw data file",
            metadata={},
            config={},
            io_manager="memory",
            materialization=MaterializationStrategy.FILE,
        ),
    ]

    generator = SchemaDocGenerator(output_dir=tmp_path)
    generator.generate(assets)

    # Should not create any schema files
    schema_files = list(tmp_path.glob("schema_*.json"))
    assert len(schema_files) == 0


def test_get_schema_summary(sample_schema: Schema, tmp_path: Path) -> None:
    """Test the get_schema_summary method."""
    generator = SchemaDocGenerator(output_dir=tmp_path)
    summary = generator.get_schema_summary(sample_schema)

    assert "Schema: test_schema" in summary
    assert "Fields: 3" in summary
    assert "Description: Test schema for unit tests" in summary
    assert "- id: integer (required, non-nullable)" in summary
    assert "- name: string (required, non-nullable)" in summary
    assert "- email: string (optional, nullable)" in summary
