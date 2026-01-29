"""Main CLI entry point for VibePiper."""

import typer
from rich.console import Console

from vibe_piper.cli.commands import (
    asset,
    docs,
    init,
    pipeline,
    run,
    test,
    validate,
    version,
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

# Register commands
app.command()(init.init)
app.command()(validate.validate)
app.command()(run.run)
app.command()(test.test)
app.command()(docs.docs)

# Add sub-commands
app.add_typer(pipeline.app, name="pipeline", help="Pipeline operations")
app.add_typer(asset.app, name="asset", help="Asset operations")


def cli() -> None:
    """Main entry point for the CLI."""
    app()


if __name__ == "__main__":
    cli()
