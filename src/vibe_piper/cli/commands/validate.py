"""Validate command for VibePiper CLI."""

import sys
from pathlib import Path
from typing import Any

import typer
from rich.console import Console
from rich.panel import Panel


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


app = typer.Typer(help="Validate a VibePiper pipeline")
console = Console()


def validate_config_file(config_path: Path) -> tuple[bool, list[str]]:
    """Validate the pipeline configuration file.

    Args:
        config_path: Path to the pipeline.toml file

    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors: list[str] = []

    if not config_path.exists():
        return False, [f"Configuration file not found: {config_path}"]

    try:
        config: dict[str, Any] = load_toml(config_path)
    except Exception as e:
        return False, [f"Failed to parse configuration file: {e}"]

    # Check required sections
    if "project" not in config:
        errors.append("Missing required section [project] in configuration")

    if "project" in config:
        project = config["project"]
        if "name" not in project:
            errors.append("Missing required field 'name' in [project] section")

    return len(errors) == 0, errors


def validate_project_structure(project_path: Path) -> tuple[bool, list[str]]:
    """Validate the project directory structure.

    Args:
        project_path: Path to the project root

    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors: list[str] = []

    # Check required directories
    required_dirs = ["src", "config"]
    for dir_name in required_dirs:
        dir_path = project_path / dir_name
        if not dir_path.exists():
            errors.append(f"Missing required directory: {dir_name}/")

    # Check for pipeline definition
    pipeline_file = project_path / "src" / "pipeline.py"
    if not pipeline_file.exists():
        errors.append("Missing pipeline definition file: src/pipeline.py")

    return len(errors) == 0, errors


def validate_pipeline_definition(project_path: Path) -> tuple[bool, list[str]]:
    """Validate the pipeline definition file.

    Args:
        project_path: Path to the project root

    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors: list[str] = []
    pipeline_file = project_path / "src" / "pipeline.py"

    if not pipeline_file.exists():
        return False, ["Pipeline definition file not found: src/pipeline.py"]

    try:
        # Try to import the pipeline module
        sys.path.insert(0, str(project_path / "src"))
        try:
            import pipeline  # noqa: F401

            # Check if create_pipeline function exists
            if not hasattr(pipeline, "create_pipeline"):
                errors.append("Pipeline module must define 'create_pipeline' function")
        except ImportError as e:
            errors.append(f"Failed to import pipeline module: {e}")
        finally:
            sys.path.pop(0)

    except Exception as e:
        errors.append(f"Error validating pipeline definition: {e}")

    return len(errors) == 0, errors


@app.command()  # type: ignore[misc]
def validate(
    project_path: Path = typer.Argument(
        Path("."),
        help="Path to the VibePiper project",
        exists=True,
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Show detailed validation output",
    ),
) -> None:
    """Validate a VibePiper project.

    Checks:
    - Project structure
    - Configuration file (pipeline.toml)
    - Pipeline definition

    Example:
        vibepiper validate my-pipeline/
    """
    project_path = project_path.resolve()
    config_path = project_path / "config" / "pipeline.toml"

    console.print(f"\n[bold cyan]Validating project:[/bold cyan] {project_path}\n")

    # Run validations
    all_valid = True
    all_errors: list[str] = []

    # Validate project structure
    console.print("[dim]→ Checking project structure...[/dim]")
    structure_valid, structure_errors = validate_project_structure(project_path)
    if structure_valid:
        console.print("[bold green]  ✓[/bold green] Project structure is valid")
    else:
        console.print("[bold red]  ✗[/bold red] Project structure has errors")
        all_errors.extend(structure_errors)
        all_valid = False

    # Validate configuration file
    console.print("[dim]→ Checking configuration file...[/dim]")
    config_valid, config_errors = validate_config_file(config_path)
    if config_valid:
        console.print("[bold green]  ✓[/bold green] Configuration file is valid")
    else:
        console.print("[bold red]  ✗[/bold red] Configuration file has errors")
        all_errors.extend(config_errors)
        all_valid = False

    # Validate pipeline definition
    console.print("[dim]→ Checking pipeline definition...[/dim]")
    pipeline_valid, pipeline_errors = validate_pipeline_definition(project_path)
    if pipeline_valid:
        console.print("[bold green]  ✓[/bold green] Pipeline definition is valid")
    else:
        console.print("[bold red]  ✗[/bold red] Pipeline definition has errors")
        all_errors.extend(pipeline_errors)
        all_valid = False

    # Display results
    console.print()

    if all_valid:
        console.print(
            Panel(
                "[bold green]✓ Pipeline validation passed![/bold green]\n\n"
                "Your VibePiper project is ready to run.",
                title="[bold green]Validation Successful[/bold green]",
                border_style="green",
            )
        )
        raise typer.Exit(0)
    else:
        if verbose:
            console.print(
                Panel(
                    "\n".join(f"[red]•[/red] {error}" for error in all_errors),
                    title="[bold red]Validation Errors[/bold red]",
                    border_style="red",
                )
            )
        else:
            console.print(
                Panel(
                    "\n".join(f"[red]•[/red] {error}" for error in all_errors[:5]),
                    title="[bold red]Validation Errors[/bold red]",
                    border_style="red",
                )
            )
            if len(all_errors) > 5:
                console.print(
                    f"\n[dim]... and {len(all_errors) - 5} more errors. "
                    f"Use --verbose for details.[/dim]"
                )

        console.print(
            f"\n[bold red]✗[/bold red] Validation failed with "
            f"[bold]{len(all_errors)}[/bold] error(s)"
        )
        raise typer.Exit(1)
