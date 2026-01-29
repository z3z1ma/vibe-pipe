"""Tests for the asset catalog generator."""

import json
from pathlib import Path

import pytest

from vibe_piper.docs.catalog import AssetCatalogGenerator
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
        name="user_schema",
        fields=(
            SchemaField(
                name="id",
                data_type=DataType.INTEGER,
                required=True,
                nullable=False,
            ),
        ),
        description="User schema",
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
            metadata={"owner": "data-team", "pii": "true"},
            config={},
            io_manager="postgres",
            materialization=MaterializationStrategy.TABLE,
        ),
        Asset(
            name="sessions",
            asset_type=AssetType.VIEW,
            uri="view://sessions",
            schema=None,
            description="Sessions view",
            metadata={"owner": "analytics"},
            config={},
            io_manager="memory",
            materialization=MaterializationStrategy.VIEW,
        ),
        Asset(
            name="logs",
            asset_type=AssetType.FILE,
            uri="file://logs",
            schema=None,
            description="Log files",
            metadata={},
            config={},
            io_manager="file",
            materialization=MaterializationStrategy.FILE,
        ),
    ]


def test_catalog_generator_initialization(tmp_path: Path) -> None:
    """Test that AssetCatalogGenerator initializes correctly."""
    generator = AssetCatalogGenerator(output_dir=tmp_path)

    assert generator.output_dir == tmp_path
    assert generator.template_dir is None
    assert generator.context == {}


def test_catalog_generator_creates_catalog_file(tmp_path: Path, sample_assets: list[Asset]) -> None:
    """Test that generator creates catalog JSON file."""
    generator = AssetCatalogGenerator(output_dir=tmp_path)
    generator.generate(sample_assets)

    catalog_file = tmp_path / "asset_catalog.json"
    assert catalog_file.exists()


def test_catalog_generator_catalog_structure(tmp_path: Path, sample_assets: list[Asset]) -> None:
    """Test that catalog has correct structure."""
    generator = AssetCatalogGenerator(output_dir=tmp_path)
    generator.generate(sample_assets)

    catalog_file = tmp_path / "asset_catalog.json"
    with catalog_file.open() as f:
        data = json.load(f)

    assert "assets" in data
    assert "summary" in data
    assert len(data["assets"]) == 3


def test_catalog_generator_asset_details(tmp_path: Path, sample_assets: list[Asset]) -> None:
    """Test that asset details are correctly serialized."""
    generator = AssetCatalogGenerator(output_dir=tmp_path)
    generator.generate(sample_assets)

    catalog_file = tmp_path / "asset_catalog.json"
    with catalog_file.open() as f:
        data = json.load(f)

    assets = data["assets"]
    assert len(assets) == 3

    # Check first asset
    users_asset = next(a for a in assets if a["name"] == "users")
    assert users_asset["type"] == "table"
    assert users_asset["uri"] == "table://users"
    assert users_asset["description"] == "User table"
    assert users_asset["materialization"] == "table"
    assert users_asset["io_manager"] == "postgres"
    assert users_asset["metadata"]["owner"] == "data-team"
    assert users_asset["metadata"]["pii"] == "true"
    assert "schema" in users_asset
    assert users_asset["schema"]["name"] == "user_schema"
    assert users_asset["schema"]["field_count"] == 1


def test_catalog_generator_summary(tmp_path: Path, sample_assets: list[Asset]) -> None:
    """Test that catalog summary is correct."""
    generator = AssetCatalogGenerator(output_dir=tmp_path)
    generator.generate(sample_assets)

    catalog_file = tmp_path / "asset_catalog.json"
    with catalog_file.open() as f:
        data = json.load(f)

    summary = data["summary"]
    assert summary["total_assets"] == 3
    assert summary["by_type"]["table"] == 1
    assert summary["by_type"]["view"] == 1
    assert summary["by_type"]["file"] == 1
    assert summary["by_materialization"]["table"] == 1
    assert summary["by_materialization"]["view"] == 1
    assert summary["by_materialization"]["file"] == 1


def test_get_catalog_index(tmp_path: Path, sample_assets: list[Asset]) -> None:
    """Test the get_catalog_index method."""
    generator = AssetCatalogGenerator(output_dir=tmp_path)
    index = generator.get_catalog_index(sample_assets)

    assert len(index) == 3

    # Check that index has simplified structure
    first_item = index[0]
    assert "name" in first_item
    assert "type" in first_item
    assert "description" in first_item
    assert "uri" not in first_item  # Should not have full details
    assert "metadata" not in first_item


def test_catalog_generator_with_empty_list(tmp_path: Path) -> None:
    """Test generator with empty asset list."""
    generator = AssetCatalogGenerator(output_dir=tmp_path)
    generator.generate([])

    catalog_file = tmp_path / "asset_catalog.json"
    with catalog_file.open() as f:
        data = json.load(f)

    assert data["assets"] == []
    assert data["summary"]["total_assets"] == 0
