"""Asset show command for VibePiper CLI."""

import json
import sys
from pathlib import Path
from typing import Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

console = Console()


def load_toml(config_path: Path) -> dict[str, Any]:
    """Load a TOML file.

    Args:
        config_path: Path to the TOML file

    Returns:
        Parsed TOML data as a dictionary
    """
    try:
        import tomllib

        with open(config_path, "rb") as f:
            return tomllib.load(f)
    except ImportError:
        # Fallback for Python < 3.11
        import toml  # type: ignore[import-untyped]

        return toml.load(config_path)  # type: ignore[no-any-return]


def import_pipeline(project_path: Path) -> Any:
    """Import and return the pipeline definition.

    Args:
        project_path: Path to the project root

    Returns:
        Pipeline object
    """
    sys.path.insert(0, str(project_path / "src"))

    try:
        import pipeline  # noqa: F401

        if not hasattr(pipeline, "create_pipeline"):
            return None

        return pipeline.create_pipeline()
    except ImportError:
        return None
    finally:
        sys.path.pop(0)


def format_metadata(metadata: dict[str, Any] | None) -> str:
    """Format metadata for display.

    Args:
        metadata: Metadata dictionary

    Returns:
        Formatted string representation
    """
    if not metadata:
        return "None"

    return json.dumps(metadata, indent=2, default=str)


def show(
    asset_name: str = typer.Argument(
        ...,
        help="Name of the asset to show",
    ),
    project_path: Path = typer.Argument(
        Path("."),
        help="Path to the VibePiper project",
        exists=True,
    ),
    format_output: str = typer.Option(
        "table",
        "--format",
        "-f",
        help="Output format (table, json)",
    ),
    include_config: bool = typer.Option(
        False,
        "--config",
        "-c",
        help="Include asset configuration",
    ),
    include_metadata: bool = typer.Option(
        False,
        "--metadata",
        "-m",
        help="Include asset metadata",
    ),
) -> None:
    """Show detailed information about a specific asset.

    Displays comprehensive information about an asset including its type,
    description, operator, dependencies, and configuration.

    Example:
        vibepiper asset show customers my-pipeline/
        vibepiper asset show orders . --config --metadata --format=json
    """
    project_path = project_path.resolve()

    console.print(f"\n[bold cyan]Asset Details:[/bold cyan] {asset_name}\n")

    # Import pipeline
    pipeline_obj = import_pipeline(project_path)
    if not pipeline_obj:
        console.print("[bold red]Error:[/bold red] Failed to import pipeline")
        raise typer.Exit(1)

    # Find asset
    assets = getattr(pipeline_obj, "assets", [])
    asset = None
    for a in assets:
        if a.name == asset_name:
            asset = a
            break

    if not asset:
        console.print(f"[bold red]Error:[/bold red] Asset '{asset_name}' not found")
        console.print("\n[dim]Hint: Use 'vibepiper asset list' to see available assets[/dim]")
        raise typer.Exit(1)

    # Load configuration
    config_path = project_path / "config" / "pipeline.toml"
    project_name = "Unknown"
    if config_path.exists():
        config = load_toml(config_path)
        project_name = config.get("project", {}).get("name", "Unknown")

    if format_output == "json":
        # Output as JSON
        asset_data = {
            "name": asset.name,
            "type": asset.asset_type.value if hasattr(asset, "asset_type") else "unknown",
            "description": asset.description,
            "uri": asset.uri if hasattr(asset, "uri") else None,
            "dependencies": list(asset.dependencies)
            if hasattr(asset, "dependencies") and asset.dependencies
            else [],
        }

        if include_config and hasattr(asset, "config"):
            asset_data["config"] = asset.config

        if include_metadata and hasattr(asset, "metadata"):
            asset_data["metadata"] = asset.metadata

        console.print(Syntax(json.dumps(asset_data, indent=2), "json"))
        raise typer.Exit(0)

    # Display as table
    console.print(f"[bold]Pipeline:[/bold] {pipeline_obj.name}")
    console.print(f"[bold]Project:[/bold] {project_name}")
    console.print()

    # Asset details table
    details_table = Table(show_header=False, box=None)
    details_table.add_column("Field", style="cyan")
    details_table.add_column("Value")

    details_table.add_row("Name", asset.name)
    details_table.add_row(
        "Type", asset.asset_type.value if hasattr(asset, "asset_type") else "unknown"
    )
    details_table.add_row("Description", asset.description or "N/A")
    details_table.add_row("URI", asset.uri if hasattr(asset, "uri") else "N/A")

    if hasattr(asset, "dependencies") and asset.dependencies:
        details_table.add_row("Dependencies", ", ".join(asset.dependencies))
    else:
        details_table.add_row("Dependencies", "None")

    # Operator information
    if hasattr(asset, "operator") and asset.operator:
        op = asset.operator
        details_table.add_row("")
        details_table.add_row("[bold]Operator:[/bold]", "")
        details_table.add_row("Name", op.name)
        details_table.add_row(
            "Type", op.operator_type.value if hasattr(op, "operator_type") else "unknown"
        )
        details_table.add_row("Description", op.description or "N/A")

    # Materialization
    if hasattr(asset, "materialization"):
        mat = asset.materialization
        details_table.add_row("")
        details_table.add_row("[bold]Materialization:[/bold]", "")
        if isinstance(mat, str):
            details_table.add_row("Strategy", mat)
        else:
            details_table.add_row("Strategy", mat.value if hasattr(mat, "value") else str(mat))

    # IO Manager
    if hasattr(asset, "io_manager") and asset.io_manager:
        details_table.add_row("")
        details_table.add_row("[bold]I/O Manager:[/bold]", "")
        details_table.add_row("Name", asset.io_manager)

    # Partition Key
    if hasattr(asset, "partition_key") and asset.partition_key:
        details_table.add_row("")
        details_table.add_row("[bold]Partitioning:[/bold]", "")
        details_table.add_row("Key", asset.partition_key)

    console.print(details_table)

    # Configuration
    if include_config and hasattr(asset, "config") and asset.config:
        console.print("\n[bold]Configuration:[/bold]")
        config_json = json.dumps(asset.config, indent=2, default=str)
        console.print(Syntax(config_json, "json"))
        console.print()

    # Metadata
    if include_metadata and hasattr(asset, "metadata") and asset.metadata:
        console.print("[bold]Metadata:[/bold]")
        metadata_json = json.dumps(asset.metadata, indent=2, default=str)
        console.print(Syntax(metadata_json, "json"))
        console.print()

    # Timestamps
    if hasattr(asset, "created_at") and asset.created_at:
        console.print("[bold]Created At:[/bold] ", asset.created_at)
    if hasattr(asset, "updated_at") and asset.updated_at:
        console.print("[bold]Updated At:[/bold] ", asset.updated_at)

    console.print()

    console.print(
        Panel(
            f"[bold cyan]Asset:[/bold cyan] {asset.name}\n"
            f"[bold]Type:[/bold] {asset.asset_type.value if hasattr(asset, 'asset_type') else 'unknown'}\n"
            f"[bold]Pipeline:[/bold] {pipeline_obj.name}",
            title="[bold cyan]Asset Details[/bold cyan]",
            border_style="cyan",
        )
    )
