"""Asset list command for VibePiper CLI."""

import sys
from pathlib import Path
from typing import Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

app = typer.Typer(help="List assets in a pipeline")
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


@app.command()  # type: ignore[misc]
def list_assets(
    project_path: Path = typer.Argument(
        Path("."),
        help="Path to the VibePiper project",
        exists=True,
    ),
    type_filter: str | None = typer.Option(
        None,
        "--type",
        "-t",
        help="Filter by asset type (source, transform, sink)",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Show detailed asset information",
    ),
) -> None:
    """List all assets in the pipeline.

    Shows all assets with their types, descriptions, and dependencies.

    Example:
        vibepiper asset list my-pipeline/
        vibepiper asset list . --type=source --verbose
    """
    project_path = project_path.resolve()

    console.print(f"\n[bold cyan]Pipeline Assets:[/bold cyan] {project_path.name}\n")

    # Load configuration
    config_path = project_path / "config" / "pipeline.toml"
    if not config_path.exists():
        console.print(f"[bold red]Error:[/bold red] Configuration file not found: {config_path}")
        raise typer.Exit(1)

    config = load_toml(config_path)
    project_name = config.get("project", {}).get("name", "Unknown")

    # Import pipeline
    pipeline_obj = import_pipeline(project_path)
    if not pipeline_obj:
        console.print("[bold red]Error:[/bold red] Failed to import pipeline")
        raise typer.Exit(1)

    console.print(f"[bold]Pipeline:[/bold] {pipeline_obj.name}")
    console.print()

    # Get assets
    assets = getattr(pipeline_obj, "assets", [])
    if not assets:
        console.print("[bold yellow]No assets found in pipeline[/bold yellow]")
        raise typer.Exit(0)

    # Filter by type
    if type_filter:
        from vibe_piper.types import AssetType

        try:
            asset_type = AssetType[type_filter.upper()]
            assets = [a for a in assets if a.asset_type == asset_type]
        except KeyError:
            console.print(f"[bold red]Error:[/bold red] Invalid asset type '{type_filter}'")
            console.print(f"\n[dim]Valid types: {[t.value for t in AssetType]}[/dim]")
            raise typer.Exit(1)

    # Create assets table
    assets_table = Table(show_header=True, box=None)
    assets_table.add_column("Name", style="cyan")
    assets_table.add_column("Type")
    assets_table.add_column("Description")

    if verbose:
        assets_table.add_column("URI")
        assets_table.add_column("Dependencies")

    for asset in assets:
        type_str = asset.asset_type.value if hasattr(asset, "asset_type") else "unknown"
        desc = asset.description or "N/A"
        uri = asset.uri if hasattr(asset, "uri") else "N/A"
        deps = (
            ", ".join(asset.dependencies)
            if hasattr(asset, "dependencies") and asset.dependencies
            else "None"
        )

        if verbose:
            assets_table.add_row(asset.name, type_str, desc, uri, deps)
        else:
            assets_table.add_row(asset.name, type_str, desc)

    console.print(assets_table)
    console.print()

    # Display summary
    from vibe_piper.types import AssetType

    source_count = sum(1 for a in assets if a.asset_type == AssetType.SOURCE)
    transform_count = sum(1 for a in assets if a.asset_type == AssetType.TRANSFORM)
    sink_count = sum(1 for a in assets if a.asset_type == AssetType.SINK)

    console.print(f"[bold]Total Assets:[/bold] {len(assets)}")
    console.print(f"[bold]Sources:[/bold] {source_count}")
    console.print(f"[bold]Transforms:[/bold] {transform_count}")
    console.print(f"[bold]Sinks:[/bold] {sink_count}")

    console.print()

    console.print(
        Panel(
            f"[bold cyan]Pipeline:[/bold cyan] {project_name}\n"
            f"[bold]Total Assets:[/bold] {len(assets)}\n"
            f"[bold]Showing:[/bold] {len(assets)} asset(s)",
            title="[bold cyan]Asset List[/bold cyan]",
            border_style="cyan",
        )
    )
