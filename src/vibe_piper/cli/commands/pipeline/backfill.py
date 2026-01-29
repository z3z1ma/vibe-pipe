"""Pipeline backfill command for VibePiper CLI."""

import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

from vibe_piper.execution import ExecutionEngine, PipelineContext


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


app = typer.Typer(help="Backfill pipeline data")
console = Console()


@app.command()  # type: ignore[misc]
def backfill(
    project_path: Path = typer.Argument(
        Path("."),
        help="Path to the VibePiper project",
        exists=True,
    ),
    asset: str | None = typer.Option(
        None,
        "--asset",
        "-a",
        help="Specific asset to backfill (backfills all if not specified)",
    ),
    start_date: str = typer.Option(
        ...,
        "--start-date",
        "-s",
        help="Start date for backfill (YYYY-MM-DD)",
    ),
    end_date: str = typer.Option(
        ...,
        "--end-date",
        "-e",
        help="End date for backfill (YYYY-MM-DD)",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        "-d",
        help="Show what would be backfilled without executing",
    ),
    parallel: int = typer.Option(
        1,
        "--parallel",
        "-p",
        help="Number of parallel backfill jobs (default: 1)",
    ),
    env: str = typer.Option(
        "dev",
        "--env",
        help="Environment to use (dev, prod, etc.)",
    ),
) -> None:
    """Backfill pipeline data for a date range.

    Re-runs pipeline assets for historical dates to fill missing or correct data.

    Example:
        vibepiper pipeline backfill . --start-date=2024-01-01 --end-date=2024-01-31
        vibepiper pipeline backfill . --asset=customers --start-date=2024-01-01 --end-date=2024-01-31 --dry-run
    """
    project_path = project_path.resolve()

    # Validate date format
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] Invalid date format: {e}")
        console.print("\n[dim]Dates must be in YYYY-MM-DD format[/dim]")
        raise typer.Exit(1)

    if start_dt > end_dt:
        console.print("[bold red]Error:[/bold red] Start date must be before end date")
        raise typer.Exit(1)

    console.print(f"\n[bold cyan]Pipeline Backfill:[/bold cyan] {project_path.name}")
    console.print(f"[dim]Environment: {env}[/dim]\n")

    # Load configuration
    console.print("[dim]→ Loading configuration...[/dim]")
    config_path = project_path / "config" / "pipeline.toml"
    if not config_path.exists():
        console.print(f"[bold red]Error:[/bold red] Configuration file not found: {config_path}")
        raise typer.Exit(1)

    config = load_toml(config_path)
    console.print("[bold green]  ✓[/bold green] Configuration loaded")

    # Import pipeline
    console.print("[dim]→ Importing pipeline...[/dim]")
    pipeline_obj = import_pipeline(project_path)
    if not pipeline_obj:
        console.print("[bold red]Error:[/bold red] Failed to import pipeline")
        raise typer.Exit(1)

    console.print(f"[bold green]  ✓[/bold green] Pipeline '{pipeline_obj.name}' imported")

    # Display backfill info
    console.print(f"\n[bold]Pipeline:[/bold] {pipeline_obj.name}")
    console.print(f"[bold]Asset:[/bold] {asset or 'All assets'}")
    console.print(f"[bold]Date Range:[/bold] {start_date} to {end_date}")
    console.print(f"[bold]Parallel Jobs:[/bold] {parallel}")
    console.print(f"[bold]Mode:[/bold] {'Dry run' if dry_run else 'Execute'}")

    # Calculate date range
    from datetime import timedelta

    delta = end_dt - start_dt
    date_range = [
        (start_dt + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(delta.days + 1)
    ]

    console.print(f"\n[bold]Dates to process:[/bold] {len(date_range)} days")

    if dry_run:
        console.print(
            "\n[bold yellow]Dry run mode:[/bold yellow] Showing backfill plan without executing\n"
        )

        console.print("[bold]Backfill plan:[/bold]")
        console.print(f"  • Pipeline: {pipeline_obj.name}")
        console.print(f"  • Asset: {asset or 'All assets'}")
        console.print(f"  • Dates: {start_date} to {end_date} ({len(date_range)} days)")
        console.print(f"  • Parallel jobs: {parallel}")
        console.print(f"  • Environment: {env}")

        console.print(
            Panel(
                "[bold yellow]Dry run complete[/bold yellow]\n\n"
                "Use vibepiper pipeline backfill without --dry-run to execute backfill.",
                title="[bold yellow]Dry Run[/bold yellow]",
                border_style="yellow",
            )
        )
        raise typer.Exit(0)

    # Execute backfill
    console.print("\n[bold cyan]Executing backfill...[/bold cyan]\n")

    try:
        # Set up context with backfill metadata
        context = PipelineContext(
            pipeline_id=pipeline_obj.name,
            run_id=f"backfill_{start_date}_{end_date}",
            config=config.get("environments", {}).get(env, {}),
            metadata={
                "backfill": True,
                "start_date": start_date,
                "end_date": end_date,
                "parallel_jobs": parallel,
            },
        )

        # Create execution engine
        engine = ExecutionEngine()

        # Process each date
        results: list[dict[str, Any]] = []

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Backfilling pipeline...", total=len(date_range))

            for i, run_date in enumerate(date_range):
                # Update context for this run
                context.metadata["run_date"] = run_date

                # Execute pipeline for this date
                result = engine.execute(
                    pipeline_obj,
                    target_assets=(asset,) if asset else None,
                    context=context,
                )

                results.append(
                    {
                        "date": run_date,
                        "success": result.success,
                        "assets_succeeded": result.assets_succeeded,
                        "assets_failed": result.assets_failed,
                        "duration_ms": result.duration_ms,
                        "errors": result.errors,
                    }
                )

                # Update progress
                status = "✓" if result.success else "✗"
                progress.update(
                    task,
                    description=f"Backfilling... {i + 1}/{len(date_range)} {status}",
                    advance=1,
                )

        # Display summary
        successful_dates = sum(1 for r in results if r["success"])
        failed_dates = len(results) - successful_dates
        total_duration = sum(r["duration_ms"] for r in results)

        console.print("\n[bold green]✓[/bold green] Backfill completed!")
        console.print("\n[bold]Summary:[/bold]")
        console.print(f"  • Dates processed: {len(results)}")
        console.print(f"  • Successful: {successful_dates}")
        console.print(f"  • Failed: {failed_dates}")
        console.print(f"  • Total duration: {total_duration / 1000:.1f}s")

        if failed_dates > 0:
            console.print("\n[bold red]Failed dates:[/bold red]")
            for result in results:
                if not result["success"]:
                    console.print(
                        f"  [red]•[/red] {result['date']}: {result['errors'][0] if result['errors'] else 'Unknown error'}"
                    )

        console.print(
            Panel(
                f"[bold green]✓ Backfill completed![/bold green]\n\n"
                f"Dates processed: {len(results)}\n"
                f"Successful: {successful_dates}\n"
                f"Failed: {failed_dates}",
                title="[bold green]Backfill Complete[/bold green]",
                border_style="green",
            )
        )

        if failed_dates > 0:
            raise typer.Exit(1)

    except Exception as e:
        console.print(f"\n[bold red]✗[/bold red] Backfill failed: {e}")
        console.print("\n[dim]Tip: Use --dry-run to preview the backfill plan[/dim]")
        raise typer.Exit(1) from None
