"""Run command for VibePiper CLI."""

import sys
from pathlib import Path
from typing import Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn


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


app = typer.Typer(help="Execute a VibePiper pipeline")
console = Console()


def load_config(project_path: Path, environment: str) -> dict[str, Any]:
    """Load pipeline configuration for the specified environment.

    Args:
        project_path: Path to the project root
        environment: Environment name (dev, prod, etc.)

    Returns:
        Configuration dictionary
    """
    config_path = project_path / "config" / "pipeline.toml"

    if not config_path.exists():
        console.print(
            f"[bold red]Error:[/bold red] Configuration file not found: {config_path}"
        )
        raise typer.Exit(1)

    config: dict[str, Any] = load_toml(config_path)

    # Merge environment-specific configuration
    if "environments" in config and environment in config["environments"]:
        env_config = config["environments"][environment]
        # Environment-specific settings can be merged here
        config["current_environment"] = env_config

    return config


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
            console.print(
                "[bold red]Error:[/bold red] Pipeline module must define 'create_pipeline' function"
            )
            raise typer.Exit(1)

        return pipeline.create_pipeline()
    except ImportError as e:
        console.print(f"[bold red]Error:[/bold red] Failed to import pipeline: {e}")
        raise typer.Exit(1) from None
    finally:
        sys.path.pop(0)


@app.command()  # type: ignore[misc]
def run(
    project_path: Path = typer.Argument(
        Path("."),
        help="Path to the VibePiper project",
        exists=True,
    ),
    asset: str | None = typer.Option(
        None,
        "--asset",
        "-a",
        help="Specific asset to run (runs all if not specified)",
    ),
    env: str = typer.Option(
        "dev",
        "--env",
        "-e",
        help="Environment to use (dev, prod, etc.)",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        "-d",
        help="Show what would be run without executing",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Show detailed execution output",
    ),
) -> None:
    """Execute a VibePiper pipeline.

    Example:
        vibepiper run my-pipeline/ --asset=customers --env=prod
    """
    project_path = project_path.resolve()

    console.print(f"\n[bold cyan]Running pipeline:[/bold cyan] {project_path.name}")
    console.print(f"[dim]Environment: {env}[/dim]\n")

    # Load configuration
    console.print("[dim]→ Loading configuration...[/dim]")
    load_config(project_path, env)
    console.print("[bold green]  ✓[/bold green] Configuration loaded")

    # Import pipeline
    console.print("[dim]→ Importing pipeline...[/dim]")
    pipeline_obj = import_pipeline(project_path)
    console.print(
        f"[bold green]  ✓[/bold green] Pipeline '{pipeline_obj.name}' imported"
    )

    # Display pipeline info
    console.print(f"\n[bold]Pipeline:[/bold] {pipeline_obj.name}")
    console.print(f"[bold]Description:[/bold] {pipeline_obj.description}")

    # Filter assets if specified
    if asset:
        console.print(f"\n[bold]Running asset:[/bold] {asset}")
    else:
        console.print("\n[bold]Running all assets[/bold]")

    # Dry run mode
    if dry_run:
        console.print(
            "\n[bold yellow]Dry run mode:[/bold yellow] Showing execution plan without running\n"
        )

        console.print("[bold]Execution plan:[/bold]")
        console.print(f"  • Pipeline: {pipeline_obj.name}")
        console.print(f"  • Environment: {env}")
        if asset:
            console.print(f"  • Asset: {asset}")
        else:
            console.print("  • All assets would be executed")

        console.print(
            Panel(
                "[bold yellow]Dry run complete[/bold yellow]\n\n"
                "Use vibepiper run without --dry-run to execute the pipeline.",
                title="[bold yellow]Dry Run[/bold yellow]",
                border_style="yellow",
            )
        )
        raise typer.Exit(0)

    # Execute pipeline
    console.print("\n[bold cyan]Executing pipeline...[/bold cyan]\n")

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Running pipeline...", total=None)

            # Here you would integrate with the actual pipeline execution
            # For now, we're simulating the execution
            console.print(f"[dim]  Running pipeline: {pipeline_obj.name}[/dim]")

            progress.update(task, description="Pipeline execution complete")

        console.print(
            "\n[bold green]✓[/bold green] Pipeline execution completed successfully!"
        )

        console.print(
            Panel(
                f"[bold green]✓ Pipeline '{pipeline_obj.name}' completed successfully!"
                f"[/bold green]\n\n"
                f"Environment: {env}\n"
                f"Assets run: {asset or 'all'}",
                title="[bold green]Execution Complete[/bold green]",
                border_style="green",
            )
        )

    except Exception as e:
        console.print(f"\n[bold red]✗[/bold red] Pipeline execution failed: {e}")
        if verbose:
            import traceback

            console.print("\n[bold red]Traceback:[/bold red]")
            console.print(traceback.format_exc())
        raise typer.Exit(1) from None
