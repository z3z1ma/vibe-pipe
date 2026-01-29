"""Pipeline status command for VibePiper CLI."""

import json
import sys
from pathlib import Path
from typing import Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

app = typer.Typer(help="Show pipeline status")
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


def get_run_history_file(project_path: Path) -> Path:
    """Get the path to the run history file.

    Args:
        project_path: Path to the project root

    Returns:
        Path to the run history file
    """
    return project_path / ".vibepiper" / "run_history.json"


def load_run_history(project_path: Path) -> list[dict[str, Any]]:
    """Load pipeline run history.

    Args:
        project_path: Path to the project root

    Returns:
        List of run history entries
    """
    history_file = get_run_history_file(project_path)
    if not history_file.exists():
        return []

    try:
        with open(history_file) as f:
            return json.load(f)
    except Exception:
        return []


@app.command()  # type: ignore[misc]
def status(
    project_path: Path = typer.Argument(
        Path("."),
        help="Path to the VibePiper project",
        exists=True,
    ),
    asset: str | None = typer.Option(
        None,
        "--asset",
        "-a",
        help="Show status for specific asset only",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Show detailed status information",
    ),
) -> None:
    """Show pipeline status.

    Displays information about the pipeline, assets, and recent runs.

    Example:
        vibepiper pipeline status my-pipeline/
        vibepiper pipeline status . --asset=customers
    """
    project_path = project_path.resolve()

    console.print(f"\n[bold cyan]Pipeline Status:[/bold cyan] {project_path.name}\n")

    # Load configuration
    config_path = project_path / "config" / "pipeline.toml"
    if not config_path.exists():
        console.print(f"[bold red]Error:[/bold red] Configuration file not found: {config_path}")
        raise typer.Exit(1)

    config = load_toml(config_path)
    project_name = config.get("project", {}).get("name", "Unknown")

    # Load pipeline definition
    pipeline_module = None
    try:
        sys.path.insert(0, str(project_path / "src"))
        import pipeline  # noqa: F401

        if hasattr(pipeline, "create_pipeline"):
            pipeline_module = pipeline.create_pipeline()
    except Exception as e:
        console.print(f"[bold yellow]Warning:[/bold yellow] Could not load pipeline: {e}")
    finally:
        if str(project_path / "src") in sys.path:
            sys.path.remove(str(project_path / "src"))

    # Load run history
    run_history = load_run_history(project_path)
    latest_run = run_history[-1] if run_history else None

    # Display pipeline information
    console.print("[bold]Pipeline Information:[/bold]")
    pipeline_table = Table(show_header=False, box=None)
    pipeline_table.add_column("Field", style="cyan")
    pipeline_table.add_column("Value")

    pipeline_table.add_row("Name", project_name)
    pipeline_table.add_row(
        "Description",
        config.get("project", {}).get("description", "No description"),
    )
    pipeline_table.add_row("Version", config.get("project", {}).get("version", "0.1.0"))

    console.print(pipeline_table)
    console.print()

    # Display assets
    if pipeline_module and hasattr(pipeline_module, "assets"):
        assets = pipeline_module.assets

        if asset:
            # Show specific asset
            asset_found = False
            for asset_def in assets:
                if asset_def.name == asset:
                    asset_found = True
                    console.print(f"[bold]Asset:[/bold] {asset_def.name}")
                    console.print(f"[bold]Type:[/bold] {asset_def.asset_type}")
                    console.print(f"[bold]Description:[/bold] {asset_def.description or 'N/A'}")
                    if verbose:
                        console.print(f"[bold]URI:[/bold] {asset_def.uri}")
                        console.print(
                            f"[bold]Dependencies:[/bold] "
                            f"{', '.join(asset_def.dependencies) if asset_def.dependencies else 'None'}"
                        )
                    break

            if not asset_found:
                console.print(f"[bold red]Error:[/bold red] Asset '{asset}' not found")
                raise typer.Exit(1)
        else:
            # Show all assets
            console.print(f"[bold]Assets:[/bold] ({len(assets)} total)")
            assets_table = Table(show_header=True, box=None)
            assets_table.add_column("Name", style="cyan")
            assets_table.add_column("Type")
            assets_table.add_column("Description")

            for asset_def in assets:
                assets_table.add_row(
                    asset_def.name,
                    asset_def.asset_type.value,
                    asset_def.description or "N/A",
                )

            console.print(assets_table)
            console.print()

    # Display latest run status
    if latest_run:
        console.print("[bold]Latest Run:[/bold]")
        latest_table = Table(show_header=False, box=None)
        latest_table.add_column("Field", style="cyan")
        latest_table.add_column("Value")

        status_color = "green" if latest_run.get("success") else "red"
        status_text = "✓ Success" if latest_run.get("success") else "✗ Failed"

        latest_table.add_row("Status", f"[{status_color}]{status_text}[/{status_color}]")
        latest_table.add_row("Run ID", latest_run.get("run_id", "N/A"))
        latest_table.add_row(
            "Timestamp",
            latest_run.get("timestamp", "N/A"),
        )
        latest_table.add_row("Duration", f"{latest_run.get('duration_ms', 0):.0f}ms")
        latest_table.add_row(
            "Assets Executed",
            str(latest_run.get("assets_executed", 0)),
        )
        latest_table.add_row(
            "Assets Succeeded",
            str(latest_run.get("assets_succeeded", 0)),
        )
        latest_table.add_row(
            "Assets Failed",
            str(latest_run.get("assets_failed", 0)),
        )

        console.print(latest_table)
        console.print()

        if verbose and latest_run.get("errors"):
            console.print("[bold]Errors:[/bold]")
            for error in latest_run.get("errors", []):
                console.print(f"  [red]•[/red] {error}")
            console.print()
    else:
        console.print("[bold yellow]No previous runs found[/bold yellow]")
        console.print()

    # Display statistics
    if run_history:
        total_runs = len(run_history)
        successful_runs = sum(1 for run in run_history if run.get("success"))
        success_rate = (successful_runs / total_runs * 100) if total_runs > 0 else 0

        console.print("[bold]Statistics:[/bold]")
        stats_table = Table(show_header=False, box=None)
        stats_table.add_column("Metric", style="cyan")
        stats_table.add_column("Value")

        stats_table.add_row("Total Runs", str(total_runs))
        stats_table.add_row("Successful Runs", str(successful_runs))
        stats_table.add_row("Failed Runs", str(total_runs - successful_runs))
        stats_table.add_row(
            "Success Rate",
            f"{success_rate:.1f}%",
        )

        console.print(stats_table)
        console.print()

    console.print(
        Panel(
            f"[bold cyan]Pipeline:[/bold cyan] {project_name}\n"
            f"[bold]Assets:[/bold] {len(pipeline_module.assets) if pipeline_module and hasattr(pipeline_module, 'assets') else 0}\n"
            f"[bold]Total Runs:[/bold] {len(run_history)}",
            title="[bold cyan]Status Summary[/bold cyan]",
            border_style="cyan",
        )
    )
