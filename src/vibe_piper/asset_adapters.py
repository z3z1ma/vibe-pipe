"""
Source and Sink asset adapters for Vibe Piper.

This module provides adapter helpers that wrap Source and BaseSink implementations
into Asset objects with operators, enabling them to participate in AssetGraph execution.

Note: Dependencies are managed at the AssetGraph level via add_asset().
These adapters return Asset objects that should be added to AssetGraph
with the depends_on parameter when calling graph.add_asset().
"""

import asyncio
import logging
from collections.abc import Mapping, Sequence
from datetime import datetime

# Import Source from base.py directly to avoid pulling in optional connectors
from typing import TYPE_CHECKING, Any

from vibe_piper.asset_factory import create_asset
from vibe_piper.schema.field_mapper import map_record_to_schema
from vibe_piper.sinks.base import BaseSink, SinkResult
from vibe_piper.types import (
    Asset,
    AssetType,
    DataRecord,
    MaterializationStrategy,
    OperatorType,
    PipelineContext,
    Schema,
)

if TYPE_CHECKING:
    from vibe_piper.sources.base import Source
else:
    # Runtime import to avoid TYPE_CHECKING overhead
    from vibe_piper.sources.base import Source

logger = logging.getLogger(__name__)


def source_asset(
    name: str,
    source: Source[Any],
    *,
    schema: Schema | None = None,
    asset_type: AssetType = AssetType.MEMORY,
    io_manager: str | None = None,
    materialization: str | MaterializationStrategy | None = None,
    description: str | None = None,
) -> Asset:
    """
    Wrap a Source into an Asset that can execute in an AssetGraph.

    This adapter creates an Asset with a SOURCE operator that:
    - Calls `asyncio.run(source.fetch(context))` to fetch data
    - Maps results with `map_record_to_schema` when schema is provided
    - Returns a list of DataRecords (or raw data if no schema)

    Args:
        name: The name of the asset
        source: The Source instance to wrap
        schema: Optional schema for mapping source records to DataRecords
        asset_type: The type of asset (defaults to MEMORY)
        io_manager: IO manager name (defaults to "memory")
        materialization: Materialization strategy (defaults to TABLE)
        description: Optional description for the asset

    Returns:
        An Asset configured to execute the Source

    Raises:
        RuntimeError: If called from within an existing event loop (asyncio.run conflict)

    Note:
        Dependency management: To establish dependencies in an AssetGraph,
        use `graph.add_asset(asset, depends_on=(...))` when adding
        this asset. Dependencies are tracked at the AssetGraph level,
        not on individual Asset objects.

    Example:
        Create a source asset with schema mapping::

            from vibe_piper.sources.api import APISource
            from vibe_piper.schema_definitions import String, Integer

            source = APISource(url="https://api.example.com/data")
            schema = define_schema(
                "user_schema",
                String("name", required=True),
                Integer("age", required=True),
            )

            asset = source_asset(
                name="users",
                source=source,
                schema=schema,
            )

            # Add to AssetGraph with dependencies:
            # graph.add_asset(asset, depends_on=("upstream_asset",))
    """

    # Check if we're in a running event loop
    try:
        asyncio.get_running_loop()
        # Event loop is already running
        msg = (
            f"Cannot execute async Source {source.__class__.__name__} from "
            f"within a running event loop. The adapter uses asyncio.run() "
            f"which cannot be called from an existing loop. "
            f"Use the source directly or ensure this runs in a sync context."
        )
        logger.error(msg)
        raise RuntimeError(msg)
    except RuntimeError:
        # No event loop running, safe to use asyncio.run()
        pass

    def _fetch_data(data: Any, context: PipelineContext) -> Sequence[DataRecord] | Sequence[Any]:
        """Operator function that fetches data from the async Source."""
        logger.debug(f"Fetching data from Source {source.__class__.__name__}")

        # Run the async fetch method
        raw_data = asyncio.run(source.fetch(context))

        # Map to DataRecords if schema is provided
        if schema is not None and isinstance(raw_data, list):
            logger.debug(f"Mapping {len(raw_data)} records to schema {schema.name}")
            mapped_records: list[DataRecord] = []
            errors: list[str] = []

            for i, item in enumerate(raw_data):
                # Handle both dict and DataRecord inputs
                if isinstance(item, DataRecord):
                    # Already a DataRecord, just validate against schema
                    try:
                        coerced_record = DataRecord(data=item.data, schema=schema)
                        mapped_records.append(coerced_record)
                    except ValueError as e:
                        msg = f"Record {i} schema validation failed: {e}"
                        errors.append(msg)
                elif isinstance(item, Mapping):
                    # Map to schema
                    result = map_record_to_schema(schema, item)
                    if result.record is not None:
                        mapped_records.append(result.record)
                    else:
                        # Collect mapping errors
                        for err in result.errors:
                            errors.append(f"Record {i}: {err}")
                        for warn in result.warnings:
                            logger.warning(f"Record {i}: {warn}")
                else:
                    msg = f"Source item {i} is not a dict or DataRecord: got {type(item).__name__}"
                    logger.error(msg)
                    raise ValueError(msg)

            return mapped_records

        # Return raw data if no schema provided or not a list
        if isinstance(raw_data, list):
            return raw_data

        # Handle single record case
        return [raw_data]

    # Create the Asset with the operator
    return create_asset(
        name=name,
        fn=_fetch_data,
        asset_type=asset_type,
        io_manager=io_manager,
        materialization=materialization,
        description=description,
        schema=schema,
        create_operator=True,
        operator_type=OperatorType.SOURCE,
    )


def sink_asset(
    name: str,
    sink: BaseSink,
    *,
    schema: Schema | None = None,
    asset_type: AssetType = AssetType.MEMORY,
    io_manager: str | None = None,
    materialization: str | MaterializationStrategy | None = None,
    description: str | None = None,
) -> Asset:
    """
    Wrap a BaseSink into an Asset that can execute in an AssetGraph.

    This adapter creates an Asset with a SINK operator that:
    - Accepts upstream data from previous assets
    - Coerces data to DataRecords if schema is provided
    - Calls `sink.write(data, context)` to write data
    - Returns a SinkResult or summary dict

    Args:
        name: The name of the asset
        sink: The BaseSink instance to wrap
        schema: Optional schema for coercing upstream data to DataRecords
        asset_type: The type of asset (defaults to MEMORY)
        io_manager: IO manager name (defaults to "memory")
        materialization: Materialization strategy (defaults to TABLE)
        description: Optional description for the asset

    Returns:
        An Asset configured to write data to the Sink

    Note:
        Dependency management: To establish dependencies in an AssetGraph,
        use `graph.add_asset(asset, depends_on=(...))` when adding
        this asset. Dependencies are tracked at the AssetGraph level,
        not on individual Asset objects.

    Example:
        Create a sink asset with schema coercion::

            from vibe_piper.sinks.database import DatabaseSink
            from vibe_piper.schema_definitions import String, Integer

            sink = DatabaseSink(
                connection_string="postgresql://...",
                table_name="users",
            )
            schema = define_schema(
                "user_schema",
                String("name", required=True),
                Integer("age", required=True),
            )

            asset = sink_asset(
                name="save_users",
                sink=sink,
                schema=schema,
                depends_on=("transform_users",),
            )
    """

    def _write_data(data: Any, context: PipelineContext) -> dict[str, Any]:
        """Operator function that writes data to the sync Sink."""
        logger.debug(f"Writing data to Sink {sink.__class__.__name__}")

        # Coerce to DataRecords if schema is provided
        records_to_write: Sequence[DataRecord] | Sequence[Any]
        if schema is not None and isinstance(data, list):
            logger.debug(f"Coercing {len(data)} records to schema {schema.name}")
            coerced_records: list[DataRecord] = []
            errors: list[str] = []

            for i, item in enumerate(data):
                # Handle both dict and DataRecord inputs
                source_record: Mapping[str, Any]
                if isinstance(item, DataRecord):
                    # Already a DataRecord, just validate against schema
                    try:
                        coerced_record = DataRecord(data=item.data, schema=schema)
                        coerced_records.append(coerced_record)
                    except ValueError as e:
                        msg = f"Record {i} schema validation failed: {e}"
                        errors.append(msg)
                elif isinstance(item, Mapping):
                    # Map to schema
                    result = map_record_to_schema(schema, item)
                    if result.record is not None:
                        coerced_records.append(result.record)
                    else:
                        # Collect mapping errors
                        for err in result.errors:
                            errors.append(f"Record {i}: {err}")
                        for warn in result.warnings:
                            logger.warning(f"Record {i}: {warn}")
                else:
                    msg = (
                        f"Upstream item {i} is not a dict or DataRecord: got {type(item).__name__}"
                    )
                    logger.error(msg)
                    raise ValueError(msg)

            if errors:
                msg = f"Schema coercion failed for {name}: {'; '.join(errors)}"
                logger.error(msg)
                raise ValueError(msg)

            records_to_write = coerced_records

        # Handle non-list data (should be a list from upstream)
        elif isinstance(data, list):
            records_to_write = data
        else:
            # Single item - wrap in list
            records_to_write = [data]

        # Initialize the sink if it has an initialize method
        if hasattr(sink, "initialize"):
            try:
                sink.initialize(context)
                logger.debug(f"Initialized Sink {sink.__class__.__name__}")
            except Exception as e:
                msg = f"Failed to initialize Sink {sink.__class__.__name__}: {e}"
                logger.error(msg)
                raise RuntimeError(msg) from e

        # Write data to the sink
        try:
            sink_result: SinkResult = sink.write(records_to_write, context)
            logger.info(
                f"Wrote {sink_result.records_written} records to "
                f"{sink.__class__.__name__}: success={sink_result.success}"
            )

            # Return summary dict with SinkResult data
            return {
                "success": sink_result.success,
                "records_written": sink_result.records_written,
                "error": sink_result.error,
                "metrics": sink_result.metrics or {},
                "timestamp": sink_result.timestamp or datetime.utcnow().isoformat(),
                "sink_class": sink.__class__.__name__,
            }

        except Exception as e:
            msg = f"Failed to write data to Sink {sink.__class__.__name__}: {e}"
            logger.error(msg)
            return {
                "success": False,
                "records_written": 0,
                "error": msg,
                "metrics": {},
                "timestamp": datetime.utcnow().isoformat(),
                "sink_class": sink.__class__.__name__,
            }

    # Create the Asset with the operator
    return create_asset(
        name=name,
        fn=_write_data,
        asset_type=asset_type,
        io_manager=io_manager,
        materialization=materialization,
        description=description,
        schema=schema,
        create_operator=True,
        operator_type=OperatorType.SINK,
    )
