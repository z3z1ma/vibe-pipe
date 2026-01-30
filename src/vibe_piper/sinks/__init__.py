"""
Sink Abstractions for Vibe Piper

This module provides declarative sink abstractions for writing data to various
destinations (databases, files, S3) with automatic DDL generation,
UPSERT logic, batching, and error handling.
"""

from vibe_piper.sinks.base import BaseSink, SinkResult

# Optional imports for sink implementations
try:
    from vibe_piper.sinks.database import DatabaseSink

    _database_available = True
except ImportError:
    _database_available = False
    DatabaseSink = None  # type: ignore

try:
    from vibe_piper.sinks.file import FileFormat, FileSink

    _file_available = True
except ImportError:
    _file_available = False
    FileSink = None  # type: ignore
    FileFormat = None  # type: ignore

try:
    from vibe_piper.sinks.s3 import S3Sink

    _s3_available = True
except ImportError:
    _s3_available = False
    S3Sink = None  # type: ignore

__all__ = [
    "BaseSink",
    "SinkResult",
    "DatabaseSink",
    "FileSink",
    "FileFormat",
    "S3Sink",
]
