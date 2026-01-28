"""
Utility functions and helpers for file connectors.

This module provides shared utilities for schema inference, type mapping,
compression handling, and other common operations.
"""

from vibe_piper.connectors.utils.compression import (
    compress_data,
    decompress_file,
    get_compression_extension,
)
from vibe_piper.connectors.utils.inference import infer_schema_from_file
from vibe_piper.connectors.utils.type_mapping import (
    map_type_from_vibepiper,
    map_type_to_vibepiper,
)

__all__ = [
    # Schema inference
    "infer_schema_from_file",
    # Type mapping
    "map_type_to_vibepiper",
    "map_type_from_vibepiper",
    # Compression
    "compress_data",
    "decompress_file",
    "get_compression_extension",
]
