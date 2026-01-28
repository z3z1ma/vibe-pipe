"""Init command for VibePiper CLI."""

from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel

console = Console()

TEMPLATE_DIR = Path(__file__).parent.parent / "templates"


def validate_project_name(name: str) -> bool:
    """Validate project name."""
    if not name:
        return False
    # Check if name is a valid Python identifier
    return name.replace("-", "_").replace(" ", "_").isidentifier()


def init(
    project_name: str = typer.Argument(
        ...,
        help="Name of the project to create",
    ),
    template: str = typer.Option(
        "basic",
        "--template",
        "-t",
        help="Template to use (basic, etl)",
    ),
    directory: Path = typer.Option(
        Path("."),
        "--dir",
        "-d",
        help="Directory to create project in (default: current directory)",
    ),
) -> None:
    """Initialize a new VibePiper project.

    Example:
        vibepiper init my-pipeline --template=etl
    """
    if not validate_project_name(project_name):
        console.print("[bold red]Error:[/bold red] Invalid project name")
        console.print(
            "Project name must be a valid identifier (letters, numbers, underscores, hyphens)"
        )
        raise typer.Exit(1)

    project_path = directory / project_name

    if project_path.exists():
        console.print(
            f"[bold red]Error:[/bold red] Directory '{project_path}' already exists"
        )
        raise typer.Exit(1)

    template_path = TEMPLATE_DIR / template

    if not template_path.exists():
        console.print(f"[bold red]Error:[/bold red] Template '{template}' not found")
        console.print(
            f"Available templates: {[d.name for d in TEMPLATE_DIR.iterdir() if d.is_dir()]}"
        )
        raise typer.Exit(1)

    # Create project directory
    project_path.mkdir(parents=True, exist_ok=True)

    # Create basic project structure
    (project_path / "src").mkdir(exist_ok=True)
    (project_path / "tests").mkdir(exist_ok=True)
    (project_path / "data").mkdir(exist_ok=True)
    (project_path / "config").mkdir(exist_ok=True)
    (project_path / "docs").mkdir(exist_ok=True)

    # Create configuration file
    config_content = f"""[project]
name = "{project_name}"
version = "0.1.0"
description = "VibePiper project: {project_name}"

[environments]
dev = {{}}
prod = {{}}

[pipeline]
assets = []

[quality]
enabled = true
strict = false
"""
    (project_path / "config" / "pipeline.toml").write_text(config_content)

    # Create main pipeline file
    pipeline_content = f"""\"\"\"{project_name} pipeline definition.\"\"\"

from vibe_piper import Pipeline, pipeline

# Create pipeline
@pipeline(name="{project_name}")
def create_pipeline() -> Pipeline:
    \"\"\"Create and configure the {project_name} pipeline.\"\"\"
    return Pipeline(
        name="{project_name}",
        description="VibePiper pipeline: {project_name}",
    )
"""
    (project_path / "src" / "pipeline.py").write_text(pipeline_content)

    # Create README
    readme_content = f"""# {project_name}

VibePiper project: {project_name}

## Getting Started

1. Validate your pipeline:
   ```bash
   vibepiper validate .
   ```

2. Run your pipeline:
   ```bash
   vibepiper run . --env=dev
   ```

3. Run tests:
   ```bash
   vibepiper test .
   ```

## Project Structure

- `src/` - Pipeline definitions and transformations
- `tests/` - Test files
- `data/` - Data files and outputs
- `config/` - Configuration files (pipeline.toml)
- `docs/` - Documentation

## Configuration

Edit `config/pipeline.toml` to configure your pipeline.

## Documentation

Generate documentation:
```bash
vibepiper docs . --output=docs/
```
"""
    (project_path / "README.md").write_text(readme_content)

    # Create .gitignore
    gitignore_content = """# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
ENV/
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# VibePiper
data/raw/
data/processed/
*.db
*.parquet
*.csv

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db
"""
    (project_path / ".gitignore").write_text(gitignore_content)

    # Create example test
    test_content = """\"\"\"Example tests for the pipeline.\"\"\"

import pytest


def test_pipeline_exists():
    \"\"\"Test that the pipeline can be imported.\"\"\"
    from src.pipeline import create_pipeline

    pipeline = create_pipeline()
    assert pipeline is not None
    assert pipeline.name == "PROJECT_NAME"
"""
    (project_path / "tests" / "test_pipeline.py").write_text(
        test_content.replace("PROJECT_NAME", project_name)
    )

    console.print(
        Panel.fit(
            f"[bold green]✓[/bold green] Project '[bold cyan]{project_name}[/bold cyan]' "
            f"created successfully!\n\n"
            f"[bold]Next steps:[/bold]\n"
            f"  1. cd {project_name}\n"
            f"  2. Edit config/pipeline.toml\n"
            f"  3. Define your pipeline in src/pipeline.py\n"
            f"  4. Run: vibepiper validate .\n"
            f"  5. Run: vibepiper run . --env=dev",
            title="[bold cyan]VibePiper Project Created[/bold cyan]",
            border_style="cyan",
        )
    )
