"""Tests for the HTML site generator."""

from pathlib import Path

import pytest

from vibe_piper.docs.site import HTMLSiteGenerator
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
                description="ID",
            ),
            SchemaField(
                name="name",
                data_type=DataType.STRING,
                required=True,
                nullable=False,
                description="Name",
            ),
        ),
        description="Test schema",
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


def test_site_generator_initialization(tmp_path: Path) -> None:
    """Test that HTMLSiteGenerator initializes correctly."""
    generator = HTMLSiteGenerator(output_dir=tmp_path)

    assert generator.output_dir == tmp_path
    assert generator.env is not None
    assert generator.catalog_gen is not None
    assert generator.schema_gen is not None
    assert generator.lineage_gen is not None


def test_site_generator_creates_directories(tmp_path: Path, sample_assets: list[Asset]) -> None:
    """Test that generator creates all required directories."""
    generator = HTMLSiteGenerator(output_dir=tmp_path)
    generator.generate(sample_assets)

    assert (tmp_path / "assets").exists()
    assert (tmp_path / "schemas").exists()
    assert (tmp_path / "static" / "css").exists()
    assert (tmp_path / "static" / "js").exists()


def test_site_generator_creates_index_page(tmp_path: Path, sample_assets: list[Asset]) -> None:
    """Test that generator creates index.html."""
    generator = HTMLSiteGenerator(output_dir=tmp_path)
    generator.generate(sample_assets, pipeline_name="Test Pipeline")

    index_file = tmp_path / "index.html"
    assert index_file.exists()

    content = index_file.read_text()
    assert "Test Pipeline" in content
    assert "Vibe Piper Documentation" in content


def test_site_generator_creates_catalog_page(tmp_path: Path, sample_assets: list[Asset]) -> None:
    """Test that generator creates catalog.html."""
    generator = HTMLSiteGenerator(output_dir=tmp_path)
    generator.generate(sample_assets)

    catalog_file = tmp_path / "catalog.html"
    assert catalog_file.exists()

    content = catalog_file.read_text()
    assert "Asset Catalog" in content
    assert "users" in content
    assert "logs" in content


def test_site_generator_creates_lineage_page(tmp_path: Path, sample_assets: list[Asset]) -> None:
    """Test that generator creates lineage.html."""
    generator = HTMLSiteGenerator(output_dir=tmp_path)
    generator.generate(sample_assets)

    lineage_file = tmp_path / "lineage.html"
    assert lineage_file.exists()

    content = lineage_file.read_text()
    assert "Data Lineage" in content
    assert "graph TD" in content  # Mermaid syntax


def test_site_generator_creates_asset_pages(tmp_path: Path, sample_assets: list[Asset]) -> None:
    """Test that generator creates individual asset pages."""
    generator = HTMLSiteGenerator(output_dir=tmp_path)
    generator.generate(sample_assets)

    users_file = tmp_path / "assets" / "users.html"
    logs_file = tmp_path / "assets" / "logs.html"

    assert users_file.exists()
    assert logs_file.exists()

    # Check content
    users_content = users_file.read_text()
    assert "users" in users_content
    assert "User table" in users_content


def test_site_generator_creates_schema_pages(tmp_path: Path, sample_assets: list[Asset]) -> None:
    """Test that generator creates schema pages."""
    generator = HTMLSiteGenerator(output_dir=tmp_path)
    generator.generate(sample_assets)

    schema_file = tmp_path / "schemas" / "test_schema.html"
    assert schema_file.exists()

    content = schema_file.read_text()
    assert "test_schema" in content
    assert "Test schema" in content
    assert "id" in content
    assert "name" in content


def test_site_generator_copies_static_files(tmp_path: Path, sample_assets: list[Asset]) -> None:
    """Test that generator copies static CSS and JS files."""
    generator = HTMLSiteGenerator(output_dir=tmp_path)
    generator.generate(sample_assets)

    css_file = tmp_path / "static" / "css" / "style.css"
    js_file = tmp_path / "static" / "js" / "search.js"
    search_index = tmp_path / "static" / "js" / "search-index.json"

    assert css_file.exists()
    assert js_file.exists()
    assert search_index.exists()


def test_site_generator_creates_search_index(tmp_path: Path, sample_assets: list[Asset]) -> None:
    """Test that generator creates search index."""
    import json

    generator = HTMLSiteGenerator(output_dir=tmp_path)
    generator.generate(sample_assets)

    search_index_file = tmp_path / "static" / "js" / "search-index.json"
    with search_index_file.open() as f:
        data = json.load(f)

    assert "assets" in data
    assert "schemas" in data
    assert len(data["assets"]) == 2
    assert len(data["schemas"]) == 1


def test_site_generator_with_custom_parameters(tmp_path: Path, sample_assets: list[Asset]) -> None:
    """Test generator with custom pipeline name and description."""
    generator = HTMLSiteGenerator(output_dir=tmp_path)
    generator.generate(
        sample_assets,
        pipeline_name="My Pipeline",
        description="This is my custom pipeline",
    )

    index_file = tmp_path / "index.html"
    content = index_file.read_text()

    assert "My Pipeline" in content
    assert "This is my custom pipeline" in content


def test_site_generator_empty_assets(tmp_path: Path) -> None:
    """Test generator with no assets."""
    generator = HTMLSiteGenerator(output_dir=tmp_path)
    generator.generate([])

    # Should still create basic structure
    assert (tmp_path / "index.html").exists()
    assert (tmp_path / "catalog.html").exists()
    assert (tmp_path / "lineage.html").exists()
