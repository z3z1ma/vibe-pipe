"""
IO Managers for asset materialization.

This module provides IO managers that handle reading and writing asset data
to different storage backends (memory, file system, S3, databases).

IO managers are Dagster-inspired abstractions that enable type-safe
materialization of assets regardless of the underlying storage.
"""

from vibe_piper.io_managers.base import IOManagerAdapter
from vibe_piper.io_managers.database import DatabaseIOManager
from vibe_piper.io_managers.file import FileIOManager
from vibe_piper.io_managers.memory import MemoryIOManager
from vibe_piper.io_managers.registry import (
    IOManagerRegistry,
    get_global_registry,
    get_io_manager,
    register_io_manager,
)
from vibe_piper.io_managers.s3 import S3IOManager

__all__ = [
    "IOManagerAdapter",
    "MemoryIOManager",
    "FileIOManager",
    "S3IOManager",
    "DatabaseIOManager",
    "IOManagerRegistry",
    "get_global_registry",
    "get_io_manager",
    "register_io_manager",
]
