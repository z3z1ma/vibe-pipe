"""Pipeline commands for VibePiper CLI."""

import typer

from vibe_piper.cli.commands.pipeline import (
    backfill,
    history,
    status,
)

# Create a typer app for pipeline commands
app = typer.Typer(help="Pipeline operations")

# Register sub-commands
app.command()(status.status)
app.command()(history.history)
app.command()(backfill.backfill)

__all__ = [
    "app",
    "backfill",
    "history",
    "status",
]
