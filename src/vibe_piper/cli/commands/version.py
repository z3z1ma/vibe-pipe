"""Version command for VibePiper CLI."""

import typer
from rich.console import Console

console = Console()

VERSION = "0.1.0"


def version_callback(
    value: bool = typer.Option(
        False,
        "--version",
        "-v",
        help="Show version and exit",
        is_eager=True,
    ),
) -> None:
    """Display the current version of VibePiper."""
    if value:
        console.print(
            f"[bold cyan]VibePiper[/bold cyan] version [bold green]{VERSION}[/bold green]"
        )
        raise typer.Exit()
