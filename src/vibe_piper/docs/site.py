"""
HTML documentation site generator.

Combines all generators and templates to produce a static HTML site.
"""

import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from vibe_piper.docs.base import DocumentationGenerator
from vibe_piper.docs.catalog import AssetCatalogGenerator
from vibe_piper.docs.lineage import LineageVisualizer
from vibe_piper.docs.schema import SchemaDocGenerator
from vibe_piper.types import Asset


class HTMLSiteGenerator(DocumentationGenerator):
    """
    Generates a complete HTML documentation site.

    Orchestrates all documentation generators and renders
    Jinja2 templates to create a static HTML site.
    """

    def __init__(
        self,
        output_dir: Path | str,
        template_dir: Path | str | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        """Initialize the HTML site generator."""
        super().__init__(output_dir, template_dir, context)

        # Set up Jinja2 environment
        if template_dir and Path(template_dir).exists():
            template_path = Path(template_dir)
        else:
            # Use default templates - find them relative to this file
            docs_dir = Path(__file__).parent
            template_path = docs_dir / "templates"

            if not template_path.exists():
                msg = f"Default templates directory not found at {template_path}"
                raise FileNotFoundError(msg)

        self.env = Environment(
            loader=FileSystemLoader(template_path),
            autoescape=select_autoescape(["html", "xml"]),
        )

        # Initialize sub-generators
        self.catalog_gen = AssetCatalogGenerator(output_dir, template_dir, context)
        self.schema_gen = SchemaDocGenerator(output_dir, template_dir, context)
        self.lineage_gen = LineageVisualizer(output_dir, template_dir, context)

    def generate(
        self,
        assets: Sequence[Asset],
        pipeline_name: str | None = None,
        description: str | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Generate the complete HTML documentation site.

        Args:
            assets: List of assets to document
            pipeline_name: Optional name for the pipeline
            description: Optional description for the documentation
            **kwargs: Additional generation options
        """
        self.ensure_output_dir()

        # Create subdirectories
        (self.output_dir / "assets").mkdir(exist_ok=True)
        (self.output_dir / "schemas").mkdir(exist_ok=True)
        (self.output_dir / "static" / "css").mkdir(parents=True, exist_ok=True)
        (self.output_dir / "static" / "js").mkdir(parents=True, exist_ok=True)

        # Generate intermediate documentation
        self.catalog_gen.generate(assets)
        self.schema_gen.generate(assets)
        self.lineage_gen.generate(assets)

        # Copy static files
        self._copy_static_files()

        # Load generated data
        catalog_data = self._load_catalog()

        # Generate pages
        self._generate_index_page(assets, catalog_data, pipeline_name, description)
        self._generate_catalog_page(catalog_data)
        self._generate_lineage_page(assets)
        self._generate_schema_pages(assets)
        self._generate_asset_pages(assets)
        self._generate_search_index(assets, catalog_data)

    def _copy_static_files(self) -> None:
        """Copy static CSS and JS files to output directory."""
        import shutil

        # Find templates/static relative to this file
        docs_dir = Path(__file__).parent
        static_source = docs_dir / "templates" / "static"

        if static_source.exists():
            static_dest = self.output_dir / "static"
            if static_dest.exists():
                shutil.rmtree(static_dest)
            shutil.copytree(static_source, static_dest)

    def _load_catalog(self) -> dict[str, Any]:
        """Load the generated catalog data."""
        catalog_file = self.output_dir / "asset_catalog.json"
        if catalog_file.exists():
            with catalog_file.open() as f:
                return json.load(f)
        return {"assets": [], "summary": {}}

    def _generate_index_page(
        self,
        assets: Sequence[Asset],
        catalog_data: dict[str, Any],
        pipeline_name: str | None,
        description: str | None,
    ) -> None:
        """Generate the index/home page."""
        template = self.env.get_template("index.html")

        # Count unique schemas
        schemas = set(a.schema.name for a in assets if a.schema)
        asset_types = set(a.asset_type.name.lower() for a in assets)

        html = template.render(
            pipeline_name=pipeline_name or "Data Pipeline",
            description=description,
            total_assets=len(assets),
            total_schemas=len(schemas),
            asset_types=list(asset_types),
            recent_assets=list(assets)[:5],  # First 5 assets
            root_url=".",
            static_url="./static",
            timestamp=datetime.now().isoformat(),
        )

        output_file = self.output_dir / "index.html"
        with output_file.open("w") as f:
            f.write(html)

    def _generate_catalog_page(self, catalog_data: dict[str, Any]) -> None:
        """Generate the asset catalog page."""
        template = self.env.get_template("catalog.html")

        html = template.render(
            assets=catalog_data.get("assets", []),
            summary=catalog_data.get("summary", {}),
            root_url=".",
            static_url="./static",
            timestamp=datetime.now().isoformat(),
        )

        output_file = self.output_dir / "catalog.html"
        with output_file.open("w") as f:
            f.write(html)

    def _generate_lineage_page(self, assets: Sequence[Asset]) -> None:
        """Generate the lineage visualization page."""
        # Create a simple lineage page
        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Lineage - Vibe Piper Documentation</title>
    <link rel="stylesheet" href="./static/css/style.css">
</head>
<body>
    <div class="container">
        <header>
            <h1>Data Lineage</h1>
            <nav>
                <ul class="nav-links">
                    <li><a href="./index.html">Home</a></li>
                    <li><a href="./catalog.html">Asset Catalog</a></li>
                    <li><a href="./lineage.html">Lineage</a></li>
                </ul>
            </nav>
        </header>
        <main>
            <h2>Data Flow Diagram</h2>
            <div class="lineage-diagram">
                <pre class="mermaid">{self._generate_mermaid_from_assets(assets)}</pre>
            </div>
            <p class="note">
                Note: This diagram shows data flow between assets.
                Use the Mermaid CLI to render this to SVG: <code>mermaid lineage.mmd -o lineage.svg</code>
            </p>
        </main>
        <footer>
            <p>Generated by Vibe Piper Documentation Generator</p>
        </footer>
    </div>
</body>
</html>"""

        output_file = self.output_dir / "lineage.html"
        with output_file.open("w") as f:
            f.write(html)

    def _generate_mermaid_from_assets(self, assets: Sequence[Asset]) -> str:
        """Generate a simple Mermaid diagram from assets."""
        lines = ["graph TD"]
        for asset in assets:
            node_id = asset.name.replace("-", "_").replace(".", "_")
            lines.append(f'    {node_id}["{asset.name}"]')
        lines.append("\n    classDef asset fill:#e1f5fe,stroke:#01579b,stroke-width:2px;")
        lines.append(
            "    class "
            + ",".join(a.name.replace("-", "_").replace(".", "_") for a in assets)
            + " asset;"
        )
        return "\n".join(lines)

    def _generate_schema_pages(self, assets: Sequence[Asset]) -> None:
        """Generate individual schema documentation pages."""
        template = self.env.get_template("schema.html")

        # Group assets by schema
        schemas_by_asset: dict[str, list[Asset]] = {}
        for asset in assets:
            if asset.schema:
                schema_name = asset.schema.name
                if schema_name not in schemas_by_asset:
                    schemas_by_asset[schema_name] = []
                schemas_by_asset[schema_name].append(asset)

        # Generate page for each schema
        for schema_name, assets_list in schemas_by_asset.items():
            schema = assets_list[0].schema
            if schema:
                schema_data = {
                    "name": schema.name,
                    "description": schema.description,
                    "fields": [
                        {
                            "name": f.name,
                            "type": f.data_type.name.lower(),
                            "required": f.required,
                            "nullable": f.nullable,
                            "description": f.description,
                            "constraints": dict(f.constraints),
                        }
                        for f in schema.fields
                    ],
                    "metadata": dict(schema.metadata),
                    "used_by": [a.name for a in assets_list],
                }

                html = template.render(
                    schema=schema_data,
                    root_url="..",
                    static_url="../static",
                    timestamp=datetime.now().isoformat(),
                )

                output_file = self.output_dir / "schemas" / f"{schema.name}.html"
                with output_file.open("w") as f:
                    f.write(html)

    def _generate_asset_pages(self, assets: Sequence[Asset]) -> None:
        """Generate individual asset detail pages."""
        for asset in assets:
            html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{asset.name} - Vibe Piper Documentation</title>
    <link rel="stylesheet" href="../static/css/style.css">
</head>
<body>
    <div class="container">
        <header>
            <h1>{asset.name}</h1>
            <nav>
                <ul class="nav-links">
                    <li><a href="../index.html">Home</a></li>
                    <li><a href="../catalog.html">Asset Catalog</a></li>
                    <li><a href="../lineage.html">Lineage</a></li>
                </ul>
            </nav>
        </header>
        <main>
            <div class="asset-detail">
                <p><strong>Type:</strong> {asset.asset_type.name.lower()}</p>
                <p><strong>Materialization:</strong> {asset.materialization.name.lower()}</p>
                <p><strong>URI:</strong> <code>{asset.uri}</code></p>
                <p><strong>I/O Manager:</strong> {asset.io_manager}</p>

                {f"<p><strong>Description:</strong></p><p>{asset.description}</p>" if asset.description else ""}

                {f'<h2>Schema</h2><p><a href="../schemas/{asset.schema.name}.html">{asset.schema.name}</a></p>' if asset.schema else ""}

                {"<h2>Metadata</h2><dl>" + "".join(f"<dt>{k}</dt><dd>{v}</dd>" for k, v in asset.metadata.items()) + "</dl>" if asset.metadata else ""}
            </div>
        </main>
        <footer>
            <p>Generated by Vibe Piper Documentation Generator</p>
        </footer>
    </div>
</body>
</html>"""

            output_file = self.output_dir / "assets" / f"{asset.name}.html"
            with output_file.open("w") as f:
                f.write(html)

    def _generate_search_index(
        self,
        assets: Sequence[Asset],
        catalog_data: dict[str, Any],
    ) -> None:
        """Generate a search index for client-side search."""
        search_index = {
            "assets": [
                {
                    "name": asset.name,
                    "type": asset.asset_type.name.lower(),
                    "description": asset.description or "",
                    "url": f"assets/{asset.name}.html",
                }
                for asset in assets
            ],
            "schemas": [],
        }

        # Add schemas
        schemas = set(a.schema.name for a in assets if a.schema)
        for schema_name in schemas:
            # Find the actual schema object
            schema = next(a.schema for a in assets if a.schema and a.schema.name == schema_name)
            search_index["schemas"].append(
                {
                    "name": schema.name,
                    "description": schema.description or "",
                    "url": f"schemas/{schema.name}.html",
                }
            )

        output_file = self.output_dir / "static" / "js" / "search-index.json"
        with output_file.open("w") as f:
            json.dump(search_index, f, indent=2)
