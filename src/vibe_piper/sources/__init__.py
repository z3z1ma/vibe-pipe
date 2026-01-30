"""
Source Abstractions for Vibe Piper

This module provides declarative source abstractions for data ingestion
without manual connector usage, pagination, authentication, or rate limiting.

Sources provide a high-level interface for:
- REST APIs with auto-pagination, retry, rate limiting
- Databases with incremental loading and schema inference
- Files with auto-detection and schema inference
"""

from vibe_piper.sources.api import APIAuthConfig, APIConfig, APISource
from vibe_piper.sources.database import DatabaseConfig, DatabaseSource
from vibe_piper.sources.file import FileConfig, FileSource

__all__ = [
    "APISource",
    "APIConfig",
    "APIAuthConfig",
    "DatabaseSource",
    "DatabaseConfig",
    "FileSource",
    "FileConfig",
]
