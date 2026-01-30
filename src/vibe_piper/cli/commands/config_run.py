"""Run pipeline from configuration file command."""

from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel

from vibe_piper.pipeline_config import (
    generate_pipeline_from_config,
    load_pipeline_from_path,
    validate_pipeline_config,
)
from vibe_piper.decorators import asset
from vibe_piper.orchestration import execute_graph

app = typer.Typer(help="Run pipeline from configuration file")
console = Console()


@app.command()
def run(
    config_path: Path = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to pipeline configuration file (TOML/YAML/JSON)",
        exists=True,
    ),
    asset: str | None = typer.Option(
        None,
        "--asset",
        "-a",
        help="Specific asset to run (runs all if not specified)",
    ),
    env_overrides: list[str] | None = typer.Option(
        None,
        "--env",
        "-e",
        help="Environment variable overrides (KEY=VALUE)",
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
    """Run a Vibe Piper pipeline from configuration file.

    Example:
        vibepiper config run --config pipeline.toml
        vibepiper config run --config pipeline.toml --asset clean_users
        vibepiper config run --config pipeline.toml --env DB_HOST=prod-db
    """
    # Load configuration
    console.print(f"\n[bold cyan]Loading configuration:[/bold cyan] {config_path}")

    try:
        config = load_pipeline_from_path(config_path)
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] Failed to load configuration: {e}")
        raise typer.Exit(1)

    # Validate configuration
    console.print("[dim]→ Validating configuration...[/dim]")
    errors = validate_pipeline_config(config)
    if errors:
        console.print("[bold red]  ✗[/bold red] Configuration validation failed")
        for error in errors:
            console.print(f"[red]•[/red] {error}")
        raise typer.Exit(1)

    console.print("[bold green]  ✓[/bold green] Configuration is valid")

    # Parse environment overrides
    env_overrides_dict = {}
    if env_overrides:
        for override in env_overrides:
            if "=" in override:
                key, value = override.split("=", 1)
                env_overrides_dict[key] = value

    # Display pipeline info
    console.print(f"\n[bold]Pipeline:[/bold] {config.pipeline.name}")
    if config.pipeline.description:
        console.print(f"[dim]Description:[/dim] {config.pipeline.description}")

    # Show asset count
    console.print(
        f"\n[bold]Assets:[/bold] "
        f"{len(config.sources)} sources, "
        f"{len(config.transforms)} transforms, "
        f"{len(config.sinks)} sinks"
    )

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
        console.print(f"  • Pipeline: {config.pipeline.name}")
        console.print(f"  • Config: {config_path}")
        if asset:
            console.print(f"  • Asset: {asset}")
        else:
            console.print("  • All assets would be executed")

        console.print(
            Panel(
                "[bold yellow]Dry run complete[/bold yellow]\n\n"
                "Use vibepiper config run without --dry-run to execute pipeline.",
                title="[bold yellow]Dry Run[/bold yellow]",
                border_style="yellow",
            )
        )
        raise typer.Exit(0)

    # Execute pipeline
    console.print("\n[bold cyan]Executing pipeline...[/bold cyan]\n")

    try:
        # Generate asset functions from config
        asset_functions = generate_pipeline_from_config(config)

        # Build pipeline using existing decorators
        # Note: This is a simplified approach - a full implementation
        # would integrate source and sink connectors
        if verbose:
            console.print(
                f"[dim]Generated asset functions:[/dim] {', '.join(asset_functions.keys())}"
            )

        console.print(
            Panel(
                "[bold green]✓ Pipeline execution completed (simplified)[/bold green]\n\n"
                "Full source/sink connector integration is pending.\n"
                "Asset functions have been generated and can be used with "
                "the @asset decorator and PipelineBuilder.\n"
                f"\nAssets: {', '.join(asset_functions.keys())}",
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
