"""Main CLI entry point for VibePiper."""

from pathlib import Path

import typer
from rich.console import Console

from vibe_piper.cli.commands import (
    config_cmd,
    config_run,
    dashboard,
    docs,
    init,
    run,
    test,
    validate,
    version,
)
from vibe_piper.cli.commands.asset import (
    list as asset_list,
)
from vibe_piper.cli.commands.asset import (
    show as asset_show,
)
from vibe_piper.cli.commands.pipeline import (
    backfill as pipeline_backfill,
)
from vibe_piper.cli.commands.pipeline import (
    history as pipeline_history,
)
from vibe_piper.cli.commands.pipeline import (
    status as pipeline_status,
)

console = Console()


def version_callback(value: bool) -> None:
    """Show the version of VibePiper and exit."""
    if value:
        console.print(
            f"[bold cyan]VibePiper[/bold cyan] version [bold green]{version.VERSION}[/bold green]"
        )
        raise typer.Exit()


def main(
    version: bool | None = typer.Option(
        None,
        "--version",
        "-v",
        help="Show version and exit",
        is_eager=True,
        callback=version_callback,
    ),
) -> None:
    """VibePiper: Declarative data pipeline framework."""
    pass


app = typer.Typer(
    name="vibepiper",
    help="VibePiper: Declarative data pipeline framework",
    add_completion=True,
    no_args_is_help=True,
    rich_markup_mode="rich",
    callback=main,
)

# Register direct commands
app.command()(init.init)
app.command()(validate.validate)
app.command()(run.run)
app.command()(test.test)
app.command()(docs.docs)
app.command()(dashboard.dashboard)


# Register pipeline commands via wrapper functions
def pipeline_status_cmd(
    project_path: Path = typer.Argument(Path("."), help="Path to VibePiper project", exists=True),
    asset: str | None = typer.Option(
        None, "--asset", "-a", help="Show status for specific asset only"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed status information"),
) -> None:
    """Show pipeline status."""
    return pipeline_status.status(project_path, asset, verbose)


def pipeline_history_cmd(
    project_path: Path = typer.Argument(Path("."), help="Path to VibePiper project", exists=True),
    limit: int = typer.Option(20, "--limit", "-n", help="Maximum number of runs to show"),
    successful_only: bool = typer.Option(
        False, "--successful-only", "-s", help="Show only successful runs"
    ),
    failed_only: bool = typer.Option(False, "--failed-only", "-f", help="Show only failed runs"),
    asset: str | None = typer.Option(None, "--asset", "-a", help="Filter by specific asset"),
) -> None:
    """Show pipeline run history."""
    return pipeline_history.history(project_path, limit, successful_only, failed_only, asset)


def pipeline_backfill_cmd(
    project_path: Path = typer.Argument(Path("."), help="Path to VibePiper project", exists=True),
    asset: str | None = typer.Option(None, "--asset", "-a", help="Specific asset to backfill"),
    start_date: str = typer.Option(
        ..., "--start-date", "-s", help="Start date for backfill (YYYY-MM-DD)"
    ),
    end_date: str = typer.Option(
        ..., "--end-date", "-e", help="End date for backfill (YYYY-MM-DD)"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", "-d", help="Show what would be backfilled without executing"
    ),
    parallel: int = typer.Option(1, "--parallel", "-p", help="Number of parallel backfill jobs"),
    env: str = typer.Option("dev", "--env", help="Environment to use (dev, prod, etc.)"),
) -> None:
    """Backfill pipeline data for a date range."""
    return pipeline_backfill.backfill(
        project_path, asset, start_date, end_date, dry_run, parallel, env
    )


def asset_list_cmd(
    project_path: Path = typer.Argument(Path("."), help="Path to VibePiper project", exists=True),
    type_filter: str | None = typer.Option(
        None, "--type", "-t", help="Filter by asset type (source, transform, sink)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed asset information"),
) -> None:
    """List all assets in the pipeline."""
    return asset_list.list_assets(project_path, type_filter, verbose)


def asset_show_cmd(
    asset_name: str = typer.Argument(..., help="Name of asset to show"),
    project_path: Path = typer.Argument(Path("."), help="Path to VibePiper project", exists=True),
    format_output: str = typer.Option(
        "table", "--format", "-f", help="Output format (table, json)"
    ),
    include_config: bool = typer.Option(
        False, "--config", "-c", help="Include asset configuration"
    ),
    include_metadata: bool = typer.Option(False, "--metadata", "-m", help="Include asset metadata"),
) -> None:
    """Show detailed information about a specific asset."""
    return asset_show.show(
        asset_name, project_path, format_output, include_config, include_metadata
    )


def config_run_cmd(
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
    """Run a Vibe Piper pipeline from configuration file."""
    return config_run.run(config_path, asset, env_overrides, dry_run, verbose)


def config_validate_cmd(
    config_path: Path = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to pipeline configuration file (TOML/YAML/JSON)",
        exists=True,
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Show detailed validation output",
    ),
) -> None:
    """Validate a Vibe Piper pipeline configuration file."""
    return config_cmd.validate(config_path, verbose)


def config_describe_cmd(
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
        help="Show details for specific asset only",
    ),
    format_output: str = typer.Option(
        "text",
        "--format",
        "-f",
        help="Output format (text, json, dot)",
    ),
) -> None:
    """Describe a Vibe Piper pipeline configuration."""
    return config_cmd.describe(config_path, asset, format_output)


# Register all wrapper commands
app.command()(pipeline_status_cmd)
app.command()(pipeline_history_cmd)
app.command()(pipeline_backfill_cmd)
app.command()(asset_list_cmd)
app.command()(asset_show_cmd)


# Register config-based pipeline commands
app.command()(config_run_cmd)
app.command()(config_validate_cmd)
app.command()(config_describe_cmd)


def cli() -> None:
    """Main entry point for the CLI."""
    app()


if __name__ == "__main__":
    cli()
