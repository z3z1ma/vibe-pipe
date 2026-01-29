"""VibePiper CLI module."""

# Import submodules for test access
from vibe_piper.cli.commands import (
    asset,  # noqa: F401
    pipeline,  # noqa: F401
)
from vibe_piper.cli.main import app

__all__ = ["app", "asset", "pipeline"]
