"""Asset commands for VibePiper CLI."""

import typer

from vibe_piper.cli.commands.asset import (
    list as asset_list,
)
from vibe_piper.cli.commands.asset import (
    show as asset_show,
)

# Create a typer app for asset commands
app = typer.Typer(help="Asset operations")

# Register sub-commands
app.command()(asset_list.list_assets)
app.command()(asset_show.show, name="show")

__all__ = [
    "app",
    "asset_list",
    "asset_show",
]
