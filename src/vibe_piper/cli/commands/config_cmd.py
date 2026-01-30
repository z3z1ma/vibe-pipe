"""Validate pipeline configuration command."""

from pathlib import Path
from typing import Any

import typer
from rich.console import Console
from rich.panel import Panel

from vibe_piper.pipeline_config import (
    build_asset_graph,
    load_pipeline_from_path,
    validate_pipeline_config,
)

app = typer.Typer(help="Validate pipeline configuration file")
console = Console()


@app.command()
def validate(
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
    """Validate a Vibe Piper pipeline configuration file.

    Checks:
    - Configuration file syntax
    - Required fields present
    - Asset references are valid
    - No circular dependencies
    - Transform steps are valid

    Example:
        vibepiper config validate --config pipeline.toml
        vibepiper config validate --config pipeline.toml --verbose
    """
    # Load configuration
    console.print(f"\n[bold cyan]Validating configuration:[/bold cyan] {config_path}")

    try:
        config = load_pipeline_from_path(config_path)
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] Failed to load configuration: {e}")
        raise typer.Exit(1)

    # Validate configuration
    console.print("[dim]→ Validating pipeline structure...[/dim]")
    errors = validate_pipeline_config(config, check_references=True)

    # Display results
    console.print()

    if not errors:
        console.print(
            Panel(
                "[bold green]✓ Pipeline configuration is valid![/bold green]\n\n"
                f"Pipeline: {config.pipeline.name}\n"
                f"Sources: {len(config.sources)}\n"
                f"Transforms: {len(config.transforms)}\n"
                f"Sinks: {len(config.sinks)}\n"
                f"Expectations: {len(config.expectations)}\n"
                f"Jobs: {len(config.jobs)}",
                title="[bold green]Validation Successful[/bold green]",
                border_style="green",
            )
        )
        raise typer.Exit(0)

    # Validation failed
    console.print("[bold red]✗[/bold red] Validation failed\n")

    if verbose:
        console.print(
            Panel(
                "\n".join(f"[red]•[/red] {error}" for error in errors),
                title="[bold red]Validation Errors[/bold red]",
                border_style="red",
            )
        )
    else:
        # Show first 10 errors
        error_count = len(errors)
        shown_errors = errors[:10]

        console.print(
            Panel(
                "\n".join(f"[red]•[/red] {error}" for error in shown_errors),
                title="[bold red]Validation Errors[/bold red]",
                border_style="red",
            )
        )

        if error_count > 10:
            console.print(
                f"\n[dim]... and {error_count - 10} more errors. Use --verbose for details.[/dim]"
            )

    console.print(
        f"\n[bold red]✗[/bold red] Validation failed with [bold]{error_count}[/bold] error(s)"
    )
    raise typer.Exit(1)


@app.command()
def describe(
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
    """Describe a Vibe Piper pipeline configuration.

    Shows:
    - Pipeline metadata
    - All assets and their types
    - Asset dependencies
    - Execution DAG (text, JSON, or GraphViz dot format)

    Example:
        vibepiper config describe --config pipeline.toml
        vibepiper config describe --config pipeline.toml --asset clean_users
        vibepiper config describe --config pipeline.toml --format json
    """
    # Load configuration
    console.print(f"\n[bold cyan]Describing pipeline:[/bold cyan] {config_path}")

    try:
        config = load_pipeline_from_path(config_path)
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] Failed to load configuration: {e}")
        raise typer.Exit(1)

    # Display pipeline metadata
    console.print()
    console.print(f"[bold]Pipeline:[/bold] {config.pipeline.name}")
    console.print(f"[dim]Version:[/dim] {config.pipeline.version}")
    if config.pipeline.description:
        console.print(f"[dim]Description:[/dim] {config.pipeline.description}")

    # Build asset graph
    graph = build_asset_graph(config)

    # Display assets by type
    if asset:
        _show_asset_details(config, asset, format_output)
    else:
        _show_all_assets(config, format_output)

    # Show dependency graph
    _show_dependency_graph(graph, format_output)


def _show_all_assets(config: Any, format_output: str) -> None:
    """Show all assets grouped by type."""
    console.print("\n[bold]Assets:[/bold]")

    if config.sources:
        console.print("\n[dim]Sources:[/dim]")
        for name, source in config.sources.items():
            tags = f" ({', '.join(source.tags)})" if source.tags else ""
            console.print(f"  [cyan]•[/cyan] {name}{tags}")

    if config.transforms:
        console.print("\n[dim]Transforms:[/dim]")
        for name, transform in config.transforms.items():
            tags = f" ({', '.join(transform.tags)})" if transform.tags else ""
            console.print(f"  [yellow]•[/yellow] {name} → {transform.source}{tags}")

    if config.sinks:
        console.print("\n[dim]Sinks:[/dim]")
        for name, sink in config.sinks.items():
            tags = f" ({', '.join(sink.tags)})" if sink.tags else ""
            console.print(f"  [green]•[/green] {name}{tags}")


def _show_asset_details(config: Any, asset_name: str, format_output: str) -> None:
    """Show details for a specific asset."""
    console.print(f"\n[bold]Asset Details:[/bold] {asset_name}")

    # Check in sources
    if asset_name in config.sources:
        asset = config.sources[asset_name]
        console.print("  Type: [cyan]source[/cyan]")
        console.print(f"  Description: {asset.description or 'N/A'}")
        if asset.tags:
            console.print(f"  Tags: {', '.join(asset.tags)}")
        if asset.pagination:
            console.print(
                f"  Pagination: {asset.pagination.type} (items: {asset.pagination.items_path})"
            )
        if asset.rate_limit:
            console.print(
                f"  Rate Limit: {asset.rate_limit.requests} req/{asset.rate_limit.window_seconds}s"
            )

    # Check in transforms
    elif asset_name in config.transforms:
        asset = config.transforms[asset_name]
        console.print("  Type: [yellow]transform[/yellow]")
        console.print(f"  Source: {asset.source}")
        console.print(f"  Steps: {len(asset.steps)} transformation(s)")
        console.print(f"  Description: {asset.description or 'N/A'}")
        if asset.tags:
            console.print(f"  Tags: {', '.join(asset.tags)}")

    # Check in sinks
    elif asset_name in config.sinks:
        asset = config.sinks[asset_name]
        console.print("  Type: [green]sink[/green]")
        console.print(f"  Description: {asset.description or 'N/A'}")
        if asset.tags:
            console.print(f"  Tags: {', '.join(asset.tags)}")
        if asset.upsert_key:
            console.print(f"  Upsert Key: {asset.upsert_key}")
        if asset.partition_cols:
            console.print(f"  Partitions: {', '.join(asset.partition_cols)}")

    else:
        console.print("[dim]  Asset not found in configuration[/dim]")


def _show_dependency_graph(graph: dict[str, Any], format_output: str) -> None:
    """Show asset dependency graph."""
    console.print("\n[bold]Dependency Graph:[/bold]")

    if format_output == "json":
        import json

        console.print(json.dumps(graph, indent=2))

    elif format_output == "dot":
        _show_dot_graph(graph)

    else:
        # Text format
        edges = graph.get("edges", [])
        if edges:
            console.print("\n[dim]Edges:[/dim]")
            for edge in edges:
                console.print(f"  [dim]→[/dim] {edge['from']} → {edge['to']}")
        else:
            console.print("[dim]  No dependencies[/dim]")


def _show_dot_graph(graph: dict[str, Any]) -> None:
    """Show dependency graph in GraphViz dot format."""
    edges = graph.get("edges", [])

    console.print("\n[bold]Graph (DOT format):[/bold]")
    console.print("digraph G {")
    console.print("  rankdir=LR;")
    console.print("  node [shape=box];")

    for edge in edges:
        console.print(f'  "{edge["from"]}" -> "{edge["to"]}";')

    console.print("}")
