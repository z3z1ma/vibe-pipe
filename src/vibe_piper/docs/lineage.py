"""
Lineage visualization generator.

Generates Mermaid.js diagrams showing data flow and dependencies.
"""

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from vibe_piper.docs.base import DocumentationGenerator
from vibe_piper.types import Asset, AssetGraph


class LineageVisualizer(DocumentationGenerator):
    """
    Generates lineage visualizations.

    Creates Mermaid.js diagrams showing:
    - Data flow between assets
    - Dependency relationships
    - Upstream and downstream dependencies
    """

    def __init__(
        self,
        output_dir: Path | str,
        template_dir: Path | str | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        """Initialize the lineage visualizer."""
        super().__init__(output_dir, template_dir, context)
        self.edges: list[tuple[str, str]] = []

    def generate(
        self,
        assets: Sequence[Asset],
        asset_graph: AssetGraph | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Generate lineage visualization.

        Args:
            assets: List of assets to visualize
            asset_graph: Optional asset graph with dependency information
            **kwargs: Additional generation options
        """
        self.ensure_output_dir()

        # Generate Mermaid diagram
        mermaid_diagram = self._generate_mermaid_diagram(assets, asset_graph)

        # Write diagram to file
        output_file = self.output_dir / "lineage.mmd"
        with output_file.open("w") as f:
            f.write(mermaid_diagram)

        # Also generate an SVG representation
        self._generate_svg_placeholder(assets)

    def _generate_mermaid_diagram(
        self,
        assets: Sequence[Asset],
        asset_graph: AssetGraph | None = None,
    ) -> str:
        """
        Generate a Mermaid.js diagram string.

        Args:
            assets: List of assets
            asset_graph: Optional asset graph

        Returns:
            Mermaid diagram string
        """
        lines = ["graph TD"]  # TD = top-down diagram

        # Add nodes for each asset
        for asset in assets:
            node_id = self._sanitize_id(asset.name)
            node_label = asset.name

            # Add additional info in label
            if asset.description:
                # Use first line of description
                desc_first_line = asset.description.split("\n")[0][:50]
                node_label = f"{asset.name}\\n{desc_first_line}"

            lines.append(f'    {node_id}["{node_label}"]')

        # Add edges if graph is provided
        if asset_graph:
            # This would require AssetGraph to expose dependencies
            # For now, we'll add a placeholder
            pass

        # Add styling
        lines.append(
            "\n    classDef asset fill:#e1f5fe,stroke:#01579b,stroke-width:2px;"
        )
        lines.append(
            "    class "
            + ",".join(self._sanitize_id(a.name) for a in assets)
            + " asset;"
        )

        return "\n".join(lines)

    def _sanitize_id(self, name: str) -> str:
        """
        Sanitize a name for use as a Mermaid node ID.

        Args:
            name: The name to sanitize

        Returns:
            Sanitized ID
        """
        # Replace special characters with underscores
        return name.replace("-", "_").replace(".", "_").replace(" ", "_")

    def _generate_svg_placeholder(self, assets: Sequence[Asset]) -> None:
        """
        Generate a placeholder SVG file.

        In production, this would use the Mermaid CLI to render
        the diagram to SVG. For now, we create a placeholder.

        Args:
            assets: List of assets
        """
        svg_content = """<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 800 600">
    <text x="400" y="300" text-anchor="middle" font-size="20" font-family="Arial">
        Lineage Visualization
    </text>
    <text x="400" y="340" text-anchor="middle" font-size="14" font-family="Arial" fill="#666">
        Use Mermaid CLI to render lineage.mmd to SVG
    </text>
</svg>
"""

        output_file = self.output_dir / "lineage.svg"
        with output_file.open("w") as f:
            f.write(svg_content)

    def generate_dependency_matrix(
        self,
        assets: Sequence[Asset],
    ) -> dict[str, list[str]]:
        """
        Generate a dependency matrix.

        Args:
            assets: List of assets

        Returns:
            Dictionary mapping asset names to their dependencies
        """
        # This is a placeholder - actual implementation would
        # extract dependency information from the asset graph
        return {asset.name: [] for asset in assets}
