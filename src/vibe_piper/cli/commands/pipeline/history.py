"""Pipeline history command for VibePiper CLI."""

import json
from pathlib import Path
from typing import Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

app = typer.Typer(help="Show pipeline run history")
console = Console()


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
def history(
    project_path: Path = typer.Argument(
        Path("."),
        help="Path to the VibePiper project",
        exists=True,
    ),
    limit: int = typer.Option(
        20,
        "--limit",
        "-n",
        help="Maximum number of runs to show",
    ),
    successful_only: bool = typer.Option(
        False,
        "--successful-only",
        "-s",
        help="Show only successful runs",
    ),
    failed_only: bool = typer.Option(
        False,
        "--failed-only",
        "-f",
        help="Show only failed runs",
    ),
    asset: str | None = typer.Option(
        None,
        "--asset",
        "-a",
        help="Filter by specific asset",
    ),
) -> None:
    """Show pipeline run history.

    Displays a history of pipeline runs with filtering options.

    Example:
        vibepiper pipeline history my-pipeline/
        vibepiper pipeline history . --limit=10 --successful-only
    """
    project_path = project_path.resolve()

    console.print(f"\n[bold cyan]Pipeline Run History:[/bold cyan] {project_path.name}\n")

    # Load run history
    run_history = load_run_history(project_path)

    if not run_history:
        console.print("[bold yellow]No run history found[/bold yellow]")
        console.print("\n[dim]Run your pipeline first with: vibepiper run .[/dim]")
        raise typer.Exit(0)

    # Apply filters
    filtered_history = run_history

    if successful_only:
        filtered_history = [run for run in filtered_history if run.get("success")]

    if failed_only:
        filtered_history = [run for run in filtered_history if not run.get("success")]

    if asset:
        filtered_history = [
            run for run in filtered_history if asset in run.get("asset_results", {})
        ]

    # Apply limit
    filtered_history = filtered_history[-limit:]

    if not filtered_history:
        console.print("[bold yellow]No runs match the specified filters[/bold yellow]")
        raise typer.Exit(0)

    # Create history table
    history_table = Table(show_header=True, box=None)
    history_table.add_column("Run ID", style="cyan")
    history_table.add_column("Timestamp")
    history_table.add_column("Status")
    history_table.add_column("Duration")
    history_table.add_column("Assets")
    history_table.add_column("Success")

    for run in filtered_history:
        run_id = run.get("run_id", "N/A")[:8]  # Show first 8 chars of UUID
        timestamp = run.get("timestamp", "N/A")
        status = "✓" if run.get("success") else "✗"
        duration = f"{run.get('duration_ms', 0):.0f}ms"
        assets = f"{run.get('assets_executed', 0)}/{run.get('assets_succeeded', 0)}/{run.get('assets_failed', 0)}"
        success_rate = (
            f"{(run.get('assets_succeeded', 0) / run.get('assets_executed', 1) * 100):.0f}%"
            if run.get("assets_executed", 0) > 0
            else "N/A"
        )

        status_color = "green" if run.get("success") else "red"
        history_table.add_row(
            run_id,
            timestamp,
            f"[{status_color}]{status}[/{status_color}]",
            duration,
            assets,
            success_rate,
        )

    console.print(history_table)
    console.print()

    # Show summary
    total_runs = len(run_history)
    successful_runs = sum(1 for run in run_history if run.get("success"))
    failed_runs = total_runs - successful_runs

    console.print(f"[bold]Total runs in history:[/bold] {total_runs}")
    console.print(f"[bold]Successful:[/bold] {successful_runs}")
    console.print(f"[bold]Failed:[/bold] {failed_runs}")
    console.print(f"[bold]Showing:[/bold] {len(filtered_history)} run(s)")

    if successful_only or failed_only or asset:
        filters = []
        if successful_only:
            filters.append("successful only")
        if failed_only:
            filters.append("failed only")
        if asset:
            filters.append(f"asset={asset}")
        console.print(f"[bold]Filters:[/bold] {', '.join(filters)}")

    console.print()

    console.print(
        Panel(
            f"[bold cyan]Total Runs:[/bold cyan] {total_runs}\n"
            f"[bold]Success Rate:[/bold] {(successful_runs / total_runs * 100):.1f}%\n"
            f"[bold]Recent Runs:[/bold] {len(filtered_history)}",
            title="[bold cyan]History Summary[/bold cyan]",
            border_style="cyan",
        )
    )
