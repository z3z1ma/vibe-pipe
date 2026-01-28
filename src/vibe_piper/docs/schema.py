"""
Schema documentation generator.

Extracts and generates documentation for data schemas.
"""

from collections.abc import Sequence
from typing import Any

from vibe_piper.docs.base import DocumentationGenerator
from vibe_piper.types import Asset, Schema, SchemaField


class SchemaDocGenerator(DocumentationGenerator):
    """
    Generates documentation for data schemas.

    Extracts schema information from assets and generates
    structured documentation including field types, constraints,
    and descriptions.
    """

    def generate(self, assets: Sequence[Asset], **kwargs: Any) -> None:
        """
        Generate schema documentation.

        Args:
            assets: List of assets to document
            **kwargs: Additional generation options
        """
        self.ensure_output_dir()

        # Group assets by schema
        schemas_by_asset: dict[str, list[Asset]] = {}
        for asset in assets:
            if asset.schema:
                schema_name = asset.schema.name
                if schema_name not in schemas_by_asset:
                    schemas_by_asset[schema_name] = []
                schemas_by_asset[schema_name].append(asset)

        # Generate documentation for each schema
        for schema_name, assets_list in schemas_by_asset.items():
            schema = assets_list[0].schema
            if schema:
                self._generate_schema_doc(schema, assets_list)

    def _generate_schema_doc(self, schema: Schema, assets: list[Asset]) -> None:
        """
        Generate documentation for a single schema.

        Args:
            schema: The schema to document
            assets: List of assets using this schema
        """
        # Create schema documentation structure
        doc_data = {
            "name": schema.name,
            "description": schema.description or "",
            "fields": [self._field_to_dict(field) for field in schema.fields],
            "metadata": dict(schema.metadata),
            "used_by": [asset.name for asset in assets],
            "field_count": len(schema.fields),
        }

        # Write to JSON file for later rendering
        import json

        output_file = self.output_dir / f"schema_{schema.name}.json"
        with output_file.open("w") as f:
            json.dump(doc_data, f, indent=2)

    def _field_to_dict(self, field: SchemaField) -> dict[str, Any]:
        """
        Convert a schema field to a dictionary.

        Args:
            field: The schema field

        Returns:
            Dictionary representation of the field
        """
        return {
            "name": field.name,
            "type": field.data_type.name.lower(),
            "required": field.required,
            "nullable": field.nullable,
            "description": field.description or "",
            "constraints": dict(field.constraints),
        }

    def get_schema_summary(self, schema: Schema) -> str:
        """
        Get a human-readable summary of a schema.

        Args:
            schema: The schema to summarize

        Returns:
            Summary string
        """
        lines = [
            f"Schema: {schema.name}",
            f"Fields: {len(schema.fields)}",
        ]

        if schema.description:
            lines.append(f"Description: {schema.description}")

        lines.append("\nFields:")
        for field in schema.fields:
            required_str = "required" if field.required else "optional"
            nullable_str = "nullable" if field.nullable else "non-nullable"
            lines.append(
                f"  - {field.name}: {field.data_type.name.lower()} "
                f"({required_str}, {nullable_str})"
            )

            if field.description:
                lines.append(f"      {field.description}")

        return "\n".join(lines)
