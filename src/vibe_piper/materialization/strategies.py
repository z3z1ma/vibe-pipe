"""
Materialization strategy implementations.

This module provides concrete implementations of materialization strategies
that control how assets are stored and managed.
"""

from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any

from vibe_piper.types import MaterializationStrategy, PipelineContext


class MaterializationStrategyBase(ABC):
    """
    Base class for materialization strategies.

    Materialization strategies control HOW data is stored when an asset
    is executed. They work in conjunction with IO managers (which control
    WHERE data is stored).

    Strategies can:
    - Control whether data is physically stored or virtual
    - Implement incremental updates vs. full refresh
    - Manage partitioning and versioning
    - Handle upsert logic for incremental loads

    Attributes:
        strategy_type: The MaterializationStrategy enum value
        key: Optional key field for incremental/upsert logic
        config: Additional strategy-specific configuration
    """

    def __init__(
        self,
        strategy_type: MaterializationStrategy,
        key: str | None = None,
        config: Mapping[str, Any] | None = None,
    ):
        """
        Initialize the materialization strategy.

        Args:
            strategy_type: The type of materialization strategy
            key: Optional key field for incremental/upsert logic
            config: Additional strategy-specific configuration
        """
        self.strategy_type = strategy_type
        self.key = key
        self.config = config or {}

    @abstractmethod
    def should_materialize(self, context: PipelineContext) -> bool:
        """
        Determine if data should be materialized.

        Some strategies (like ViewStrategy) may skip materialization entirely.

        Args:
            context: The pipeline execution context

        Returns:
            True if data should be materialized, False otherwise
        """
        ...

    @abstractmethod
    def prepare_for_storage(
        self,
        context: PipelineContext,
        data: Any,
        existing_data: Any = None,
    ) -> Any:
        """
        Prepare data for storage according to the strategy.

        This method transforms or filters data before it's passed to the
        IO manager. For example, incremental strategies might merge new
        data with existing data.

        Args:
            context: The pipeline execution context
            data: The new data to prepare
            existing_data: Any existing data (for incremental updates)

        Returns:
            The prepared data ready for storage
        """
        ...

    def get_storage_metadata(
        self, context: PipelineContext
    ) -> Mapping[str, Any]:  # noqa: ARG002
        """
        Get metadata about how data should be stored.

        This provides additional hints to IO managers about storage requirements.

        Args:
            context: The pipeline execution context

        Returns:
            Mapping of metadata key-value pairs
        """
        return {"strategy": self.strategy_type.name}


class TableStrategy(MaterializationStrategyBase):
    """
    Full materialization as a physical table.

    This strategy fully materializes data on each execution, replacing
    any previously stored data. It's the default for most assets.

    Example:
        Use table strategy for full refresh::

            @asset(materialization="table")
            def my_table():
                return pd.DataFrame(...)
    """

    def __init__(self, config: Mapping[str, Any] | None = None):
        """
        Initialize the table strategy.

        Args:
            config: Optional configuration (e.g., partitioning info)
        """
        super().__init__(MaterializationStrategy.TABLE, config=config)

    def should_materialize(self, context: PipelineContext) -> bool:  # noqa: ARG002
        """Table strategy always materializes data."""
        return True

    def prepare_for_storage(
        self,
        context: PipelineContext,  # noqa: ARG002
        data: Any,
        existing_data: Any = None,  # noqa: ARG002
    ) -> Any:
        """
        Return data as-is for full materialization.

        Args:
            context: The pipeline execution context
            data: The data to store
            existing_data: Ignored for full refresh

        Returns:
            The data unchanged
        """
        return data


class ViewStrategy(MaterializationStrategyBase):
    """
    Virtual view strategy (no physical storage).

    This strategy skips materialization entirely, treating the asset
    as a virtual view that's computed on demand. Useful for derived
    datasets that don't need to be persisted.

    Example:
        Use view strategy for computed data::

            @asset(materialization="view")
            def computed_metric(raw_data):
                return raw_data * 2
    """

    def __init__(self, config: Mapping[str, Any] | None = None):
        """
        Initialize the view strategy.

        Args:
            config: Optional configuration
        """
        super().__init__(MaterializationStrategy.VIEW, config=config)

    def should_materialize(self, context: PipelineContext) -> bool:  # noqa: ARG002
        """View strategy never materializes data."""
        return False

    def prepare_for_storage(
        self,
        context: PipelineContext,  # noqa: ARG002
        data: Any,
        existing_data: Any = None,  # noqa: ARG002
    ) -> Any:
        """
        View strategy doesn't store data.

        Args:
            context: The pipeline execution context
            data: The computed data (not stored)
            existing_data: Ignored

        Returns:
            The data unchanged (but not stored)
        """
        return data


class FileStrategy(MaterializationStrategyBase):
    """
    File-based storage strategy.

    This strategy stores data as files, with support for partitioning
    and file format configuration.

    Example:
        Use file strategy for file-based storage::

            @asset(materialization="file")
            def my_file():
                return [{"id": 1}, {"id": 2}]
    """

    def __init__(
        self,
        partition_key: str | None = None,
        config: Mapping[str, Any] | None = None,
    ):
        """
        Initialize the file strategy.

        Args:
            partition_key: Optional field to partition files by
            config: Optional configuration (format, compression, etc.)
        """
        super().__init__(
            MaterializationStrategy.FILE,
            key=partition_key,
            config=config,
        )

    def should_materialize(self, context: PipelineContext) -> bool:  # noqa: ARG002
        """File strategy always materializes data."""
        return True

    def prepare_for_storage(
        self,
        context: PipelineContext,  # noqa: ARG002
        data: Any,
        existing_data: Any = None,  # noqa: ARG002
    ) -> Any:
        """
        Prepare data for file storage.

        Args:
            context: The pipeline execution context
            data: The data to store
            existing_data: Ignored for file strategy (full overwrite)

        Returns:
            The data unchanged
        """
        return data

    def get_storage_metadata(
        self, context: PipelineContext
    ) -> Mapping[str, Any]:  # noqa: ARG002
        """
        Get file storage metadata.

        Returns:
            Metadata including partition key if configured
        """
        metadata = super().get_storage_metadata(context)
        if self.key:
            metadata["partition_key"] = self.key
        return metadata


class IncrementalStrategy(MaterializationStrategyBase):
    """
    Incremental materialization with upsert logic.

    This strategy incrementally updates existing data by appending new
    records and updating existing ones based on a key field.

    The strategy implements upsert (update or insert) semantics:
    - Records with new keys are appended
    - Records with existing keys replace the old version
    - Records without the key field are appended

    Example:
        Use incremental strategy with upsert logic::

            @asset(materialization="incremental", key="date")
            def daily_sales():
                return pd.DataFrame({
                    "date": ["2024-01-01"],
                    "amount": [100]
                })
    """

    def __init__(
        self,
        key: str,
        config: Mapping[str, Any] | None = None,
    ):
        """
        Initialize the incremental strategy.

        Args:
            key: The field name to use as the upsert key
            config: Optional configuration (e.g., merge behavior)

        Raises:
            ValueError: If key is None or empty
        """
        if not key:
            msg = "IncrementalStrategy requires a 'key' parameter for upsert logic"
            raise ValueError(msg)

        super().__init__(MaterializationStrategy.INCREMENTAL, key=key, config=config)

    def should_materialize(self, context: PipelineContext) -> bool:  # noqa: ARG002
        """Incremental strategy always materializes data."""
        return True

    def prepare_for_storage(
        self,
        context: PipelineContext,  # noqa: ARG002
        data: Any,
        existing_data: Any = None,
    ) -> Any:
        """
        Merge new data with existing data using upsert logic.

        Args:
            context: The pipeline execution context
            data: The new data to merge
            existing_data: Previously stored data (for incremental updates)

        Returns:
            Merged data with new records added and existing ones updated
        """
        # If no existing data, return new data as-is
        if existing_data is None:
            return data

        # If no new data, return existing data
        if data is None:
            return existing_data

        # Perform upsert based on key
        return self._merge_data(data, existing_data)

    def _merge_data(self, new_data: Any, existing_data: Any) -> Any:
        """
        Merge new data with existing data using upsert logic.

        Args:
            new_data: The new data to merge
            existing_data: The existing data to update

        Returns:
            Merged data
        """
        # Handle list-like data (common case)
        if isinstance(new_data, list) and isinstance(existing_data, list):
            return self._merge_lists(new_data, existing_data)

        # Handle dict-like data
        if isinstance(new_data, dict) and isinstance(existing_data, dict):
            return self._merge_dicts(new_data, existing_data)

        # For other types, just return new data (full replacement)
        return new_data

    def _merge_lists(
        self,
        new_data: list,
        existing_data: list,
    ) -> list:
        """
        Merge list data using upsert logic.

        Creates a mapping from existing data based on the key field,
        then updates it with new data.

        Args:
            new_data: List of new records
            existing_data: List of existing records

        Returns:
            Merged list with upserts applied
        """
        # Check if items are dict-like
        if not new_data or not isinstance(new_data[0], dict):
            # Non-dict items: just append
            return existing_data + new_data

        # Build index of existing data by key
        existing_index = {}
        for item in existing_data:
            if isinstance(item, dict) and self.key in item:
                key_value = item[self.key]
                existing_index[key_value] = item
            else:
                # Item without key: keep it
                existing_index[f"__auto_id_{id(item)}"] = item

        # Update with new data
        new_index = {}
        for item in new_data:
            if isinstance(item, dict) and self.key in item:
                key_value = item[self.key]
                new_index[key_value] = item
            else:
                # Item without key: add to new data
                new_index[f"__auto_id_new_{id(item)}"] = item

        # Merge: new data overwrites existing for matching keys
        merged_index = {**existing_index, **new_index}

        # Convert back to list
        return list(merged_index.values())

    def _merge_dicts(self, new_data: dict, existing_data: dict) -> dict:
        """
        Merge dict data using upsert logic.

        For dict data, assumes the structure is {key: record}.
        Updates existing keys and adds new ones.

        Args:
            new_data: Dict of new records
            existing_data: Dict of existing records

        Returns:
            Merged dict
        """
        # Simple dict merge (new values overwrite existing)
        return {**existing_data, **new_data}

    def get_storage_metadata(
        self, context: PipelineContext
    ) -> Mapping[str, Any]:  # noqa: ARG002
        """
        Get incremental storage metadata.

        Returns:
            Metadata including the upsert key field
        """
        metadata = super().get_storage_metadata(context)
        metadata["incremental_key"] = self.key
        return metadata
