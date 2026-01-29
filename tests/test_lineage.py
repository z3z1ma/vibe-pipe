"""
Tests for asset lineage and metadata features.

Tests for lineage query API, Mermaid export, and asset metadata tracking.
"""

import pytest

from vibe_piper import (
    Asset,
    AssetGraph,
    AssetResult,
    AssetType,
    calculate_checksum,
)


class TestAssetMetadata:
    """Tests for Asset metadata fields (created_at, updated_at, checksum)."""

    def test_asset_with_metadata_defaults(self) -> None:
        """Test that Asset has metadata fields with None defaults."""
        asset = Asset(
            name="test_asset",
            asset_type=AssetType.TABLE,
            uri="postgresql://db/test",
        )

        assert asset.created_at is None
        assert asset.updated_at is None
        assert asset.checksum is None

    def test_asset_with_metadata_fields(self) -> None:
        """Test creating an Asset with metadata fields set."""
        from datetime import datetime

        now = datetime.now()
        asset = Asset(
            name="test_asset",
            asset_type=AssetType.TABLE,
            uri="postgresql://db/test",
            created_at=now,
            updated_at=now,
            checksum="abc123",
        )

        assert asset.created_at == now
        assert asset.updated_at == now
        assert asset.checksum == "abc123"


class TestCalculateChecksum:
    """Tests for checksum calculation utility."""

    def test_checksum_for_dict(self) -> None:
        """Test checksum calculation for dict data."""
        data = {"id": 1, "name": "test"}
        checksum = calculate_checksum(data)

        assert checksum is not None
        assert isinstance(checksum, str)
        assert len(checksum) == 64  # SHA256 produces 64 hex characters

    def test_checksum_for_list(self) -> None:
        """Test checksum calculation for list data."""
        data = [{"id": 1}, {"id": 2}]
        checksum = calculate_checksum(data)

        assert checksum is not None
        assert isinstance(checksum, str)

    def test_checksum_for_none(self) -> None:
        """Test checksum calculation for None data."""
        checksum = calculate_checksum(None)
        assert checksum is None

    def test_checksum_consistency(self) -> None:
        """Test that checksum is consistent for same data."""
        data = {"id": 1, "name": "test"}
        checksum1 = calculate_checksum(data)
        checksum2 = calculate_checksum(data)

        assert checksum1 == checksum2

    def test_checksum_different_for_different_data(self) -> None:
        """Test that checksum is different for different data."""
        checksum1 = calculate_checksum({"id": 1})
        checksum2 = calculate_checksum({"id": 2})

        assert checksum1 != checksum2


class TestAssetResultMetadata:
    """Tests for AssetResult metadata fields."""

    def test_asset_result_with_metadata_defaults(self) -> None:
        """Test that AssetResult has metadata fields with None defaults."""
        result = AssetResult(
            asset_name="test_asset",
            success=True,
        )

        assert result.created_at is None
        assert result.updated_at is None
        assert result.checksum is None

    def test_asset_result_with_metadata_fields(self) -> None:
        """Test creating an AssetResult with metadata fields set."""
        from datetime import datetime

        now = datetime.now()
        result = AssetResult(
            asset_name="test_asset",
            success=True,
            created_at=now,
            updated_at=now,
            checksum="abc123",
        )

        assert result.created_at == now
        assert result.updated_at == now
        assert result.checksum == "abc123"


class TestLineageQueryAPI:
    """Tests for lineage query API on AssetGraph."""

    def test_get_upstream_direct_dependencies(self) -> None:
        """Test getting direct upstream dependencies."""
        a = Asset(name="a", asset_type=AssetType.TABLE, uri="db://a")
        b = Asset(name="b", asset_type=AssetType.TABLE, uri="db://b")
        c = Asset(name="c", asset_type=AssetType.TABLE, uri="db://c")

        graph = AssetGraph(
            name="test",
            assets=(a, b, c),
            dependencies={"b": ("a",), "c": ("b",)},
        )

        upstream = graph.get_upstream("c", depth=1)
        assert len(upstream) == 1
        assert upstream[0].name == "b"

    def test_get_upstream_recursive(self) -> None:
        """Test getting all upstream dependencies recursively."""
        a = Asset(name="a", asset_type=AssetType.TABLE, uri="db://a")
        b = Asset(name="b", asset_type=AssetType.TABLE, uri="db://b")
        c = Asset(name="c", asset_type=AssetType.TABLE, uri="db://c")

        graph = AssetGraph(
            name="test",
            assets=(a, b, c),
            dependencies={"b": ("a",), "c": ("b",)},
        )

        upstream = graph.get_upstream("c")
        upstream_names = {asset.name for asset in upstream}
        assert upstream_names == {"a", "b"}

    def test_get_upstream_no_dependencies(self) -> None:
        """Test getting upstream for asset with no dependencies."""
        a = Asset(name="a", asset_type=AssetType.TABLE, uri="db://a")

        graph = AssetGraph(name="test", assets=(a,))

        upstream = graph.get_upstream("a")
        assert len(upstream) == 0

    def test_get_upstream_nonexistent_asset_raises_error(self) -> None:
        """Test that getting upstream for nonexistent asset raises error."""
        a = Asset(name="a", asset_type=AssetType.TABLE, uri="db://a")

        graph = AssetGraph(name="test", assets=(a,))

        with pytest.raises(ValueError, match="not found in graph"):
            graph.get_upstream("nonexistent")

    def test_get_downstream_direct_dependents(self) -> None:
        """Test getting direct downstream dependents."""
        a = Asset(name="a", asset_type=AssetType.TABLE, uri="db://a")
        b = Asset(name="b", asset_type=AssetType.TABLE, uri="db://b")
        c = Asset(name="c", asset_type=AssetType.TABLE, uri="db://c")

        graph = AssetGraph(
            name="test",
            assets=(a, b, c),
            dependencies={"b": ("a",), "c": ("b",)},
        )

        downstream = graph.get_downstream("a", depth=1)
        assert len(downstream) == 1
        assert downstream[0].name == "b"

    def test_get_downstream_recursive(self) -> None:
        """Test getting all downstream dependents recursively."""
        a = Asset(name="a", asset_type=AssetType.TABLE, uri="db://a")
        b = Asset(name="b", asset_type=AssetType.TABLE, uri="db://b")
        c = Asset(name="c", asset_type=AssetType.TABLE, uri="db://c")

        graph = AssetGraph(
            name="test",
            assets=(a, b, c),
            dependencies={"b": ("a",), "c": ("b",)},
        )

        downstream = graph.get_downstream("a")
        downstream_names = {asset.name for asset in downstream}
        assert downstream_names == {"b", "c"}

    def test_get_downstream_no_dependents(self) -> None:
        """Test getting downstream for asset with no dependents."""
        a = Asset(name="a", asset_type=AssetType.TABLE, uri="db://a")

        graph = AssetGraph(name="test", assets=(a,))

        downstream = graph.get_downstream("a")
        assert len(downstream) == 0

    def test_get_downstream_nonexistent_asset_raises_error(self) -> None:
        """Test that getting downstream for nonexistent asset raises error."""
        a = Asset(name="a", asset_type=AssetType.TABLE, uri="db://a")

        graph = AssetGraph(name="test", assets=(a,))

        with pytest.raises(ValueError, match="not found in graph"):
            graph.get_downstream("nonexistent")

    def test_get_lineage_graph(self) -> None:
        """Test getting complete lineage graph."""
        a = Asset(name="a", asset_type=AssetType.TABLE, uri="db://a")
        b = Asset(name="b", asset_type=AssetType.TABLE, uri="db://b")
        c = Asset(name="c", asset_type=AssetType.TABLE, uri="db://c")

        dependencies = {"b": ("a",), "c": ("b",)}
        graph = AssetGraph(
            name="test",
            assets=(a, b, c),
            dependencies=dependencies,
        )

        lineage = graph.get_lineage_graph()

        assert lineage == dependencies
        # Verify it's a copy, not the same object
        assert lineage is not graph.dependencies

    def test_get_upstream_complex_dag(self) -> None:
        """Test getting upstream in a complex DAG structure."""
        # Create diamond DAG:
        #     a
        #    / \
        #   b   c
        #    \ /
        #     d
        a = Asset(name="a", asset_type=AssetType.TABLE, uri="db://a")
        b = Asset(name="b", asset_type=AssetType.TABLE, uri="db://b")
        c = Asset(name="c", asset_type=AssetType.TABLE, uri="db://c")
        d = Asset(name="d", asset_type=AssetType.TABLE, uri="db://d")

        graph = AssetGraph(
            name="diamond",
            assets=(a, b, c, d),
            dependencies={"b": ("a",), "c": ("a",), "d": ("b", "c")},
        )

        # Get upstream for d
        upstream = graph.get_upstream("d")
        upstream_names = {asset.name for asset in upstream}

        # Should include a, b, and c
        assert upstream_names == {"a", "b", "c"}

    def test_get_downstream_complex_dag(self) -> None:
        """Test getting downstream in a complex DAG structure."""
        # Create diamond DAG:
        #     a
        #    / \
        #   b   c
        #    \ /
        #     d
        a = Asset(name="a", asset_type=AssetType.TABLE, uri="db://a")
        b = Asset(name="b", asset_type=AssetType.TABLE, uri="db://b")
        c = Asset(name="c", asset_type=AssetType.TABLE, uri="db://c")
        d = Asset(name="d", asset_type=AssetType.TABLE, uri="db://d")

        graph = AssetGraph(
            name="diamond",
            assets=(a, b, c, d),
            dependencies={"b": ("a",), "c": ("a",), "d": ("b", "c")},
        )

        # Get downstream from a
        downstream = graph.get_downstream("a")
        downstream_names = {asset.name for asset in downstream}

        # Should include b, c, and d
        assert downstream_names == {"b", "c", "d"}


class TestMermaidExport:
    """Tests for Mermaid diagram export."""

    def test_to_mermaid_simple_graph(self) -> None:
        """Test exporting a simple graph as Mermaid."""
        a = Asset(name="raw_data", asset_type=AssetType.TABLE, uri="db://a")
        b = Asset(name="clean_data", asset_type=AssetType.TABLE, uri="db://b")

        graph = AssetGraph(
            name="test",
            assets=(a, b),
            dependencies={"clean_data": ("raw_data",)},
        )

        mermaid = graph.to_mermaid()

        assert "graph TD" in mermaid
        assert "raw_data" in mermaid
        assert "clean_data" in mermaid
        assert "raw_data --> clean_data" in mermaid or "raw_data-->clean_data" in mermaid

    def test_to_mermaid_includes_asset_types(self) -> None:
        """Test that Mermaid export includes asset types."""
        a = Asset(name="data", asset_type=AssetType.FILE, uri="file://data")

        graph = AssetGraph(name="test", assets=(a,))

        mermaid = graph.to_mermaid()

        assert "file" in mermaid.lower()

    def test_to_mermaid_complex_graph(self) -> None:
        """Test exporting a complex graph as Mermaid."""
        a = Asset(name="a", asset_type=AssetType.TABLE, uri="db://a")
        b = Asset(name="b", asset_type=AssetType.TABLE, uri="db://b")
        c = Asset(name="c", asset_type=AssetType.TABLE, uri="db://c")
        d = Asset(name="d", asset_type=AssetType.TABLE, uri="db://d")

        graph = AssetGraph(
            name="diamond",
            assets=(a, b, c, d),
            dependencies={"b": ("a",), "c": ("a",), "d": ("b", "c")},
        )

        mermaid = graph.to_mermaid()

        # Check all nodes are present
        for asset_name in ["a", "b", "c", "d"]:
            assert asset_name in mermaid

        # Check edges are present
        assert "-->" in mermaid
        assert mermaid.count("-->") >= 3  # At least 3 edges

    def test_to_mermaid_sanitizes_names(self) -> None:
        """Test that Mermaid export sanitizes asset names."""
        a = Asset(name="my-asset.test", asset_type=AssetType.TABLE, uri="db://a")
        b = Asset(name="your.asset_v2", asset_type=AssetType.TABLE, uri="db://b")

        graph = AssetGraph(
            name="test",
            assets=(a, b),
            dependencies={"your.asset_v2": ("my-asset.test",)},
        )

        mermaid = graph.to_mermaid()

        # Names should be sanitized (dots and dashes replaced with underscores)
        assert "my_asset_test" in mermaid
        assert "your_asset_v2" in mermaid

    def test_to_mermaid_empty_graph(self) -> None:
        """Test exporting an empty graph as Mermaid."""
        graph = AssetGraph(name="empty", assets=())

        mermaid = graph.to_mermaid()

        assert "graph TD" in mermaid
        # Should only have the header, no nodes or edges
        lines = mermaid.split("\n")
        assert len(lines) == 1  # Only the "graph TD" line
