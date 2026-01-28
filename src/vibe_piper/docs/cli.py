"""
CLI interface for documentation generator.

Provides the 'vibepiper docs' command.
"""

import importlib.util
import sys
from pathlib import Path
from typing import Any

import click
from rich.console import Console

from vibe_piper.docs.site import HTMLSiteGenerator
from vibe_piper.types import Asset

console = Console()


@click.group()
def docs() -> None:
    """Generate documentation for Vibe Piper pipelines."""
    pass


@docs.command()
@click.argument(
    "pipeline_path",
    type=click.Path(exists=True, path_type=Path),
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=Path("docs"),
    help="Output directory for documentation",
)
@click.option(
    "--pipeline-name",
    "-n",
    type=str,
    default=None,
    help="Name for the pipeline",
)
@click.option(
    "--description",
    "-d",
    type=str,
    default=None,
    help="Description for the documentation",
)
@click.option(
    "--template-dir",
    "-t",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Custom template directory",
)
def generate(
    pipeline_path: Path,
    output: Path,
    pipeline_name: str | None,
    description: str | None,
    template_dir: Path | None,
) -> None:
    """
    Generate documentation for a pipeline.

    PIPELINE_PATH: Path to the pipeline directory or module
    """
    console.print("[bold blue]Generating documentation...[/bold blue]")
    console.print(f"Pipeline: {pipeline_path}")
    console.print(f"Output: {output}")

    try:
        # Load assets from pipeline
        assets = _load_assets_from_path(pipeline_path)

        if not assets:
            console.print("[yellow]Warning: No assets found in pipeline[/yellow]")
            return

        console.print(f"Found {len(assets)} assets")

        # Generate documentation
        generator = HTMLSiteGenerator(
            output_dir=output,
            template_dir=template_dir,
        )

        generator.generate(
            assets=assets,
            pipeline_name=pipeline_name,
            description=description,
        )

        console.print(
            "[bold green]✓ Documentation generated successfully![/bold green]"
        )
        console.print(f"Open {output / 'index.html'} in your browser to view")

    except Exception as e:
        console.print(f"[bold red]Error generating documentation:[/bold red] {e}")
        raise click.ClickException(str(e))


@docs.command()
@click.argument(
    "pipeline_path",
    type=click.Path(exists=True, path_type=Path),
)
@click.option(
    "--port",
    "-p",
    type=int,
    default=8000,
    help="Port to serve on",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=Path("docs"),
    help="Output directory for documentation",
)
def serve(
    pipeline_path: Path,
    port: int,
    output: Path,
) -> None:
    """
    Generate and serve documentation locally.

    PIPELINE_PATH: Path to the pipeline directory or module
    """
    import webbrowser
    from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

    console.print("[bold blue]Generating and serving documentation...[/bold blue]")

    # Generate documentation first
    assets = _load_assets_from_path(pipeline_path)

    if not assets:
        console.print("[yellow]Warning: No assets found in pipeline[/yellow]")
        return

    generator = HTMLSiteGenerator(output_dir=output)
    generator.generate(assets=assets)

    # Change to output directory
    import os

    os.chdir(output)

    # Start server
    console.print(
        f"[bold green]Documentation server running at http://localhost:{port}[/bold green]"
    )
    console.print("Press Ctrl+C to stop")

    # Open browser
    webbrowser.open(f"http://localhost:{port}")

    # Custom handler to serve from docs directory
    class DocsHandler(SimpleHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:  # type: ignore[misc]
            pass  # Suppress log messages

    try:
        with ThreadingHTTPServer(("", port), DocsHandler) as httpd:
            httpd.serve_forever()
    except KeyboardInterrupt:
        console.print("\n[yellow]Server stopped[/yellow]")


def _load_assets_from_path(pipeline_path: Path) -> list[Asset]:
    """
    Load assets from a pipeline path.

    Args:
        pipeline_path: Path to pipeline directory or module

    Returns:
        List of assets found in the pipeline
    """
    assets: list[Asset] = []

    # If it's a directory, look for Python files
    if pipeline_path.is_dir():
        python_files = list(pipeline_path.rglob("*.py"))
        console.print(f"Searching {len(python_files)} Python files for assets...")

        for py_file in python_files:
            # Skip __pycache__ and __init__.py
            if "__pycache__" in str(py_file) or py_file.name == "__init__.py":
                continue

            try:
                file_assets = _extract_assets_from_file(py_file)
                assets.extend(file_assets)
            except Exception as e:
                console.print(
                    f"[yellow]Warning: Could not load {py_file}: {e}[/yellow]"
                )

    # If it's a single file, load it
    elif pipeline_path.is_file() and pipeline_path.suffix == ".py":
        assets = _extract_assets_from_file(pipeline_path)

    return assets


def _extract_assets_from_file(file_path: Path) -> list[Asset]:
    """
    Extract assets from a Python file.

    Args:
        file_path: Path to Python file

    Returns:
        List of assets defined in the file
    """
    assets: list[Asset] = []

    # Load the module
    module_name = file_path.stem
    spec = importlib.util.spec_from_file_location(module_name, file_path)

    if spec is None or spec.loader is None:
        return assets

    module = importlib.util.module_from_spec(spec)

    # Execute the module
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as e:
        console.print(f"[yellow]Warning: Could not execute {file_path}: {e}[/yellow]")
        return assets

    # Look for Asset instances in module
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if isinstance(attr, Asset):
            assets.append(attr)

    return assets


# Main entry point
def docs_command() -> None:
    """Entry point for the docs CLI."""
    docs()


if __name__ == "__main__":
    docs_command()
