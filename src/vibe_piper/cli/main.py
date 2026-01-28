"""Main CLI entry point for VibePiper."""

import typer
from rich.console import Console

from vibe_piper.cli.commands import (
    docs,
    init,
    run,
    test,
    validate,
    version,
)

app = typer.Typer(
    name="vibepiper",
    help="VibePiper: Declarative data pipeline framework",
    add_completion=True,
    no_args_is_help=True,
    rich_markup_mode="rich",
)

console = Console()

# Register subcommands
app.add_typer(init.app, name="init", help="Initialize a new VibePiper project")
app.add_typer(validate.app, name="validate", help="Validate a VibePiper pipeline")
app.add_typer(run.app, name="run", help="Execute a VibePiper pipeline")
app.add_typer(test.app, name="test", help="Run tests for a VibePiper project")
app.add_typer(
    docs.app, name="docs", help="Generate documentation for a VibePiper project"
)
app.command()(version.version_callback)


def main() -> None:
    """Main entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()
