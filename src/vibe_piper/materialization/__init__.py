"""
Materialization strategies for asset storage.

This module provides strategies for controlling how assets are materialized
(stored) when executed. Strategies work in conjunction with IO managers to
control the HOW of storage, while IO managers control the WHERE.

Available strategies:
- TableStrategy: Full materialization as a physical table
- ViewStrategy: Virtual view (no storage, computed on demand)
- FileStrategy: File-based storage
- IncrementalStrategy: Append/update based on key
"""

from vibe_piper.materialization.strategies import (
    FileStrategy,
    IncrementalStrategy,
    MaterializationStrategyBase,
    TableStrategy,
    ViewStrategy,
)
from vibe_piper.types import MaterializationStrategy

__all__ = [
    "MaterializationStrategyBase",
    "TableStrategy",
    "ViewStrategy",
    "FileStrategy",
    "IncrementalStrategy",
    "MaterializationStrategy",
]
