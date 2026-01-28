"""
Asset catalog generator.

Creates a comprehensive catalog of all assets with metadata and relationships.
"""

from collections.abc import Sequence
from typing import Any

from vibe_piper.docs.base import DocumentationGenerator
from vibe_piper.types import Asset


class AssetCatalogGenerator(DocumentationGenerator):
    """
    Generates an asset catalog.

    Creates a comprehensive catalog of all assets including:
    - Asset names and types
    - Descriptions and documentation
    - Metadata (owners, tags, etc.)
    - Dependencies and relationships
    - Materialization strategies
    """

    def generate(self, assets: Sequence[Asset], **kwargs: Any) -> None:
        """
        Generate asset catalog.

        Args:
            assets: List of assets to catalog
            **kwargs: Additional generation options
        """
        self.ensure_output_dir()

        # Create catalog structure
        catalog = {
            "assets": [self._asset_to_dict(asset) for asset in assets],
            "summary": self._generate_summary(assets),
        }

        # Write catalog to JSON
        import json

        catalog_file = self.output_dir / "asset_catalog.json"
        with catalog_file.open("w") as f:
            json.dump(catalog, f, indent=2)

    def _asset_to_dict(self, asset: Asset) -> dict[str, Any]:
        """
        Convert an asset to a dictionary.

        Args:
            asset: The asset to convert

        Returns:
            Dictionary representation of the asset
        """
        asset_dict = {
            "name": asset.name,
            "type": asset.asset_type.name.lower(),
            "uri": asset.uri,
            "description": asset.description or "",
            "metadata": dict(asset.metadata),
            "materialization": asset.materialization.name.lower(),
            "io_manager": asset.io_manager,
        }

        # Add schema information if present
        if asset.schema:
            asset_dict["schema"] = {
                "name": asset.schema.name,
                "field_count": len(asset.schema.fields),
                "description": asset.schema.description or "",
            }

        return asset_dict

    def _generate_summary(self, assets: Sequence[Asset]) -> dict[str, Any]:
        """
        Generate a summary of the asset catalog.

        Args:
            assets: List of assets

        Returns:
            Summary dictionary
        """
        # Count assets by type
        type_counts: dict[str, int] = {}
        for asset in assets:
            asset_type = asset.asset_type.name.lower()
            type_counts[asset_type] = type_counts.get(asset_type, 0) + 1

        # Count assets by materialization
        materialization_counts: dict[str, int] = {}
        for asset in assets:
            mat = asset.materialization.name.lower()
            materialization_counts[mat] = materialization_counts.get(mat, 0) + 1

        return {
            "total_assets": len(assets),
            "by_type": type_counts,
            "by_materialization": materialization_counts,
        }

    def get_catalog_index(self, assets: Sequence[Asset]) -> list[dict[str, Any]]:
        """
        Get a simplified index of assets for navigation.

        Args:
            assets: List of assets

        Returns:
            List of asset summaries
        """
        return [
            {
                "name": asset.name,
                "type": asset.asset_type.name.lower(),
                "description": asset.description or "",
            }
            for asset in assets
        ]
