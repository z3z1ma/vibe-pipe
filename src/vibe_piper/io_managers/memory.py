"""
Memory-based IO Manager.

This module provides an IO manager that stores asset data in memory.
This is the default IO manager and maintains existing behavior.
"""

from typing import Any

from vibe_piper.io_managers.base import IOManagerAdapter
from vibe_piper.types import PipelineContext


class MemoryIOManager(IOManagerAdapter):
    """
    IO manager that stores data in memory.

    This is the default IO manager for assets. Data is stored in a simple
    dictionary keyed by asset name. This provides fast access but data is
    lost when the process exits.

    Attributes:
        storage: In-memory storage dictionary

    Example:
        Use the memory IO manager::

            @asset(io_manager="memory")
            def my_asset():
                return {"data": "value"}
    """

    def __init__(self) -> None:
        """Initialize the memory IO manager with empty storage."""
        self.storage: dict[str, Any] = {}

    def handle_output(self, context: PipelineContext, data: Any) -> None:
        """
        Store data in memory.

        Args:
            context: The pipeline execution context
            data: The data to store
        """
        # Use the asset name from the run_id or pipeline_id
        asset_key = context.pipeline_id
        self.storage[asset_key] = data

    def load_input(self, context: PipelineContext) -> Any:
        """
        Load data from memory.

        Args:
            context: The pipeline execution context

        Returns:
            The loaded data, or None if not found
        """
        asset_key = context.pipeline_id
        return self.storage.get(asset_key)

    def get(self, asset_key: str) -> Any:
        """
        Get data for a specific asset key.

        Args:
            asset_key: The asset key to retrieve

        Returns:
            The stored data, or None if not found
        """
        return self.storage.get(asset_key)

    def set(self, asset_key: str, data: Any) -> None:
        """
        Set data for a specific asset key.

        Args:
            asset_key: The asset key to store under
            data: The data to store
        """
        self.storage[asset_key] = data

    def clear(self) -> None:
        """Clear all stored data."""
        self.storage.clear()

    def has_asset(self, asset_key: str) -> bool:
        """
        Check if an asset exists in storage.

        Args:
            asset_key: The asset key to check

        Returns:
            True if the asset exists, False otherwise
        """
        return asset_key in self.storage
