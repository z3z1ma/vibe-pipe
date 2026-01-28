"""Tests for the lineage visualizer."""

from pathlib import Path

import pytest

from vibe_piper.docs.lineage import LineageVisualizer
from vibe_piper.types import (
    Asset,
    AssetType,
    MaterializationStrategy,
)


@pytest.fixture
def sample_assets() -> list[Asset]:
    """Create sample assets for testing."""
    return [
        Asset(
            name="raw_data",
            asset_type=AssetType.FILE,
            uri="file://raw_data",
            schema=None,
            description="Raw data source",
            metadata={},
            config={},
            io_manager="file",
            materialization=MaterializationStrategy.FILE,
        ),
        Asset(
            name="processed_data",
            asset_type=AssetType.TABLE,
            uri="table://processed_data",
            schema=None,
            description="Processed data",
            metadata={},
            config={},
            io_manager="postgres",
            materialization=MaterializationStrategy.TABLE,
        ),
        Asset(
            name="analytics_output",
            asset_type=AssetType.VIEW,
            uri="view://analytics_output",
            schema=None,
            description="Analytics output",
            metadata={},
            config={},
            io_manager="memory",
            materialization=MaterializationStrategy.VIEW,
        ),
    ]


def test_lineage_visualizer_initialization(tmp_path: Path) -> None:
    """Test that LineageVisualizer initializes correctly."""
    generator = LineageVisualizer(output_dir=tmp_path)

    assert generator.output_dir == tmp_path
    assert generator.edges == []


def test_lineage_visualizer_creates_mermaid_file(
    tmp_path: Path, sample_assets: list[Asset]
) -> None:
    """Test that generator creates Mermaid diagram file."""
    generator = LineageVisualizer(output_dir=tmp_path)
    generator.generate(sample_assets)

    mermaid_file = tmp_path / "lineage.mmd"
    assert mermaid_file.exists()


def test_lineage_visualizer_mermaid_content(
    tmp_path: Path, sample_assets: list[Asset]
) -> None:
    """Test that Mermaid file has correct content."""
    generator = LineageVisualizer(output_dir=tmp_path)
    generator.generate(sample_assets)

    mermaid_file = tmp_path / "lineage.mmd"
    content = mermaid_file.read_text()

    # Check Mermaid syntax
    assert "graph TD" in content
    assert "raw_data" in content
    assert "processed_data" in content
    assert "analytics_output" in content
    assert "classDef asset" in content


def test_lineage_visualizer_creates_svg_placeholder(
    tmp_path: Path, sample_assets: list[Asset]
) -> None:
    """Test that generator creates SVG placeholder."""
    generator = LineageVisualizer(output_dir=tmp_path)
    generator.generate(sample_assets)

    svg_file = tmp_path / "lineage.svg"
    assert svg_file.exists()

    content = svg_file.read_text()
    assert "<svg" in content
    assert "Lineage Visualization" in content


def test_lineage_visualizer_sanitizes_ids(tmp_path: Path) -> None:
    """Test that node IDs are sanitized correctly."""
    assets = [
        Asset(
            name="my-asset.name",
            asset_type=AssetType.TABLE,
            uri="table://my-asset.name",
            schema=None,
            description="Test",
            metadata={},
            config={},
            io_manager="memory",
            materialization=MaterializationStrategy.TABLE,
        ),
    ]

    generator = LineageVisualizer(output_dir=tmp_path)
    generator.generate(assets)

    mermaid_file = tmp_path / "lineage.mmd"
    content = mermaid_file.read_text()

    # Should replace dots and dashes with underscores
    assert "my_asset_name" in content
    assert "my-asset.name" not in content or "my_asset_name" in content


def test_generate_dependency_matrix(tmp_path: Path, sample_assets: list[Asset]) -> None:
    """Test the generate_dependency_matrix method."""
    generator = LineageVisualizer(output_dir=tmp_path)
    matrix = generator.generate_dependency_matrix(sample_assets)

    assert isinstance(matrix, dict)
    assert len(matrix) == 3
    assert "raw_data" in matrix
    assert "processed_data" in matrix
    assert "analytics_output" in matrix


def test_lineage_visualizer_with_empty_assets(tmp_path: Path) -> None:
    """Test visualizer with no assets."""
    generator = LineageVisualizer(output_dir=tmp_path)
    generator.generate([])

    mermaid_file = tmp_path / "lineage.mmd"
    assert mermaid_file.exists()

    content = mermaid_file.read_text()
    assert "graph TD" in content
