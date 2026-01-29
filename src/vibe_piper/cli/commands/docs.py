"""Docs command for VibePiper CLI."""

import ast
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel

app = typer.Typer(help="Generate documentation for a VibePiper project")
console = Console()


def extract_docstring(file_path: Path) -> str | None:
    """Extract module docstring from a Python file.

    Args:
        file_path: Path to the Python file

    Returns:
        Module docstring or None
    """
    try:
        with open(file_path, encoding="utf-8") as f:
            source = f.read()

        tree = ast.parse(source)
        return ast.get_docstring(tree)
    except Exception:
        return None


def generate_module_docs(module_path: Path, output_path: Path) -> None:
    """Generate documentation for a single module.

    Args:
        module_path: Path to the Python module
        output_path: Path to the output documentation file
    """
    docstring = extract_docstring(module_path)

    if docstring:
        output_path.write_text(
            f"# {module_path.stem}\n\n{docstring}\n\n[Source]({module_path.name})\n"
        )
    else:
        output_path.write_text(
            f"# {module_path.stem}\n\n"
            f"*No documentation available*\n\n"
            f"[Source]({module_path.name})\n"
        )


def generate_readme_docs(project_path: Path, output_path: Path) -> None:
    """Generate documentation from README.md.

    Args:
        project_path: Path to the project root
        output_path: Path to the output documentation file
    """
    readme_path = project_path / "README.md"

    if readme_path.exists():
        output_path.write_text(readme_path.read_text())
    else:
        output_path.write_text("# Project Documentation\n\nNo README.md found.\n")


def generate_config_docs(project_path: Path, output_path: Path) -> None:
    """Generate documentation from configuration file.

    Args:
        project_path: Path to the project root
        output_path: Path to the output documentation file
    """
    config_path = project_path / "config" / "pipeline.toml"

    if config_path.exists():
        config_content = config_path.read_text()
        output_path.write_text(f"# Configuration\n\n```toml\n{config_content}\n```\n")
    else:
        output_path.write_text("# Configuration\n\nNo configuration file found.\n")


@app.command()  # type: ignore[misc]
def docs(
    project_path: Path = typer.Argument(
        Path("."),
        help="Path to the VibePiper project",
        exists=True,
    ),
    output: Path = typer.Option(
        Path("docs"),
        "--output",
        "-o",
        help="Output directory for documentation",
    ),
    format_type: str = typer.Option(
        "markdown",
        "--format",
        "-f",
        help="Documentation format (markdown, html)",
    ),
    open_browser: bool = typer.Option(
        False,
        "--open",
        help="Open documentation in browser after generation",
    ),
) -> None:
    """Generate documentation for a VibePiper project.

    Generates documentation from:
    - README.md
    - Configuration files
    - Python modules in src/

    Example:
        vibepiper docs my-pipeline/ --output=docs/
    """
    project_path = project_path.resolve()
    output_path = project_path / output

    console.print(f"\n[bold cyan]Generating documentation for:[/bold cyan] {project_path.name}\n")

    # Create output directory
    output_path.mkdir(parents=True, exist_ok=True)

    # Generate documentation files
    console.print("[dim]→ Generating README documentation...[/dim]")
    generate_readme_docs(project_path, output_path / "README.md")
    console.print("[bold green]  ✓[/bold green] README.md")

    console.print("[dim]→ Generating configuration documentation...[/dim]")
    generate_config_docs(project_path, output_path / "config.md")
    console.print("[bold green]  ✓[/bold green] config.md")

    # Generate module documentation
    src_path = project_path / "src"
    if src_path.exists():
        console.print("[dim]→ Generating module documentation...[/dim]")
        modules_docs_path = output_path / "modules"
        modules_docs_path.mkdir(exist_ok=True)

        for py_file in src_path.glob("*.py"):
            if py_file.name != "__init__.py":
                generate_module_docs(py_file, modules_docs_path / f"{py_file.stem}.md")
                console.print(f"[bold green]  ✓[/bold green] {py_file.name}")

    # Generate index
    console.print("[dim]→ Generating index...[/dim]")
    index_content = f"""# {project_path.name} Documentation

Generated documentation for {project_path.name}.

## Contents

- [README](README.md)
- [Configuration](config.md)

## Modules

"""

    modules_docs_path = output_path / "modules"
    if modules_docs_path.exists():
        for doc_file in sorted(modules_docs_path.glob("*.md")):
            index_content += f"- [{doc_file.stem}](modules/{doc_file.name})\n"

    (output_path / "index.md").write_text(index_content)
    console.print("[bold green]  ✓[/bold green] index.md")

    # Convert to HTML if requested
    if format_type == "html":
        console.print("\n[dim]→ Converting to HTML...[/dim]")

        # Check if markdown-it or similar is available
        # For now, just mention that HTML conversion would require additional tools
        console.print("[dim]  HTML conversion requires additional tools (e.g., markdown-it)[/dim]")

    # Display results
    console.print("\n[bold green]✓[/bold green] Documentation generated successfully!")

    console.print(
        Panel(
            f"[bold green]✓ Documentation generated[/bold green]\n\n"
            f"Output directory: {output_path}\n\n"
            f"Files generated:\n"
            f"  • index.md\n"
            f"  • README.md\n"
            f"  • config.md\n"
            f"  • modules/\n",
            title="[bold green]Documentation Complete[/bold green]",
            border_style="green",
        )
    )

    if open_browser and format_type == "html":
        console.print("\n[dim]Note: Browser opening requires HTML format[/dim]")
