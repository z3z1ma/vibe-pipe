"""
Tests for Source and Sink asset adapters.

These tests validate that source_asset and sink_asset correctly
wrap Source and BaseSink implementations into Assets.
"""

from datetime import datetime

import pytest

from vibe_piper.asset_adapters import sink_asset, source_asset
from vibe_piper.schema_definitions import String
from vibe_piper.sinks.base import SinkResult
from vibe_piper.types import DataRecord, DataType, PipelineContext, Schema, SchemaField

# =============================================================================
# Stub Source Implementation
# =============================================================================


class StubSource:
    """A stub async Source for testing."""

    def __init__(self, data: list[dict[str, any]]) -> None:
        self.data = data
        self._fetch_called = False

    async def fetch(self, context: PipelineContext) -> list[dict[str, any]]:
        """Fetch test data."""
        self._fetch_called = True
        return self.data

    def infer_schema(self) -> Schema:
        """Infer schema from test data."""
        fields = []
        for key in self.data[0].keys():
            fields.append(SchemaField(name=key, data_type=DataType.STRING))
        return Schema(name="stub_schema", fields=tuple(fields))

    def get_metadata(self) -> dict[str, any]:
        """Get metadata."""
        return {"source_type": "StubSource"}

    @property
    def fetch_called(self) -> bool:
        """Check if fetch was called."""
        return self._fetch_called


# =============================================================================
# Stub Sink Implementation
# =============================================================================


class StubSink:
    """A stub sync Sink for testing."""

    def __init__(self) -> None:
        # Store data as-is (dicts or DataRecords)
        self.written_data: list[list[any]] = []
        self.initialized = False
        self.cleaned_up = False

    def write(self, data: list[any], context: PipelineContext) -> SinkResult:
        """Write test data."""
        self.written_data.append(data)
        return SinkResult(
            success=True,
            records_written=len(data),
            metrics={},
            timestamp=datetime.utcnow(),
        )

    def initialize(self, context: PipelineContext) -> None:
        """Initialize sink."""
        self.initialized = True

    def cleanup(self, context: PipelineContext) -> None:
        """Clean up sink."""
        self.cleaned_up = True

    def get_metrics(self) -> dict[str, int | float]:
        """Get metrics."""
        return {"writes": len(self.written_data)}


# =============================================================================
# source_asset Tests
# =============================================================================


def test_source_asset_basic() -> None:
    """Test that source_asset creates a valid Asset."""
    source_data = [{"name": "Alice"}, {"name": "Bob"}]
    source = StubSource(source_data)
    schema = Schema(
        name="user_schema",
        fields=(SchemaField(name="name", data_type=DataType.STRING, required=True),),
    )

    asset = source_asset(name="users", source=source, schema=schema)

    assert asset.name == "users"
    assert asset.asset_type.name == "MEMORY"
    assert asset.operator is not None
    assert asset.schema == schema
    assert asset.io_manager == "memory"


def test_source_asset_executes_async_fetch() -> None:
    """Test that source_asset executes async Source.fetch()."""
    source_data = [{"name": "Alice"}, {"name": "Bob"}]
    source = StubSource(source_data)
    schema = Schema(
        name="user_schema",
        fields=(SchemaField(name="name", data_type=DataType.STRING, required=True),),
    )

    asset = source_asset(name="users", source=source, schema=schema)
    context = PipelineContext(pipeline_id="test", run_id="test_run")

    # Execute the operator (SOURCE operators take context only)
    result = asset.operator.fn(context)

    # Verify fetch was called
    assert source.fetch_called

    # Verify results are DataRecords
    assert len(result) == 2
    assert all(isinstance(r, DataRecord) for r in result)
    assert result[0].data["name"] == "Alice"
    assert result[1].data["name"] == "Bob"


def test_source_asset_without_schema() -> None:
    """Test that source_asset returns raw data when no schema is provided."""
    source_data = [{"name": "Alice"}, {"name": "Bob"}]
    source = StubSource(source_data)

    asset = source_asset(name="users", source=source, schema=None)
    context = PipelineContext(pipeline_id="test", run_id="test_run")

    # Execute the operator (SOURCE operators take context only)
    result = asset.operator.fn(context)

    # Verify fetch was called
    assert source.fetch_called

    # Verify raw data is returned (not DataRecords)
    assert result == source_data


def test_source_asset_with_nested_mapping() -> None:
    """Test that source_asset maps nested dicts using SchemaField.source_path."""
    # Create test data with nested structure
    source_data = [
        {"user": {"name": "Alice", "age": 30}},
        {"user": {"name": "Bob", "age": 25}},
    ]
    source = StubSource(source_data)

    # Schema with source_path for mapping
    schema = Schema(
        name="user_schema",
        fields=(
            SchemaField(
                name="name",
                data_type=DataType.STRING,
                required=True,
                source_path="user.name",
            ),
            SchemaField(
                name="age",
                data_type=DataType.INTEGER,
                required=True,
                source_path="user.age",
            ),
        ),
    )

    asset = source_asset(name="users", source=source, schema=schema)
    context = PipelineContext(pipeline_id="test", run_id="test_run")

    # Execute the operator (SOURCE operators take context only)
    result = asset.operator.fn(context)

    # Verify mapping worked
    assert len(result) == 2
    assert result[0].data["name"] == "Alice"
    assert result[0].data["age"] == 30
    assert result[1].data["name"] == "Bob"
    assert result[1].data["age"] == 25


def test_source_asset_propagates_errors() -> None:
    """Test that source_asset propagates schema mapping errors."""
    # Create test data that violates schema
    source_data = [{"name": "Alice", "age": "invalid"}]  # age should be int
    source = StubSource(source_data)

    schema = Schema(
        name="user_schema",
        fields=(
            SchemaField(name="name", data_type=DataType.STRING, required=True),
            SchemaField(name="age", data_type=DataType.INTEGER, required=True),
        ),
    )

    asset = source_asset(name="users", source=source, schema=schema)
    context = PipelineContext(pipeline_id="test", run_id="test_run")

    # Execute should raise ValueError (SOURCE operators take context only)
    with pytest.raises(ValueError, match="Schema mapping failed"):
        asset.operator.fn(context)


def test_source_asset_event_loop_error() -> None:
    """Test that source_asset raises RuntimeError when called from running event loop."""
    source_data = [{"name": "Alice"}]
    source = StubSource(source_data)

    schema = Schema(
        name="user_schema",
        fields=(SchemaField(name="name", data_type=DataType.STRING, required=True),),
    )

    asset = source_asset(name="users", source=source, schema=schema)
    context = PipelineContext(pipeline_id="test", run_id="test_run")

    # Execute within existing event loop should raise RuntimeError
    import asyncio

    async def simulate_event_loop() -> None:
        # Create an event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # Try to execute asset within running event loop
        # Update regex to match actual error message from asyncio.run()
        with pytest.raises(
            RuntimeError, match="asyncio.run\\(\\) cannot be called from a running event loop"
        ):
            asset.operator.fn(context)

    # Run the test in an event loop
    asyncio.run(simulate_event_loop())


# =============================================================================
# sink_asset Tests
# =============================================================================


def test_sink_asset_basic() -> None:
    """Test that sink_asset creates a valid Asset."""
    sink = StubSink()
    schema = Schema(
        name="user_schema",
        fields=(SchemaField(name="name", data_type=DataType.STRING, required=True),),
    )

    asset = sink_asset(name="save_users", sink=sink, schema=schema)

    assert asset.name == "save_users"
    assert asset.asset_type.name == "MEMORY"
    assert asset.operator is not None
    assert asset.schema == schema
    assert asset.io_manager == "memory"


def test_sink_asset_writes_data() -> None:
    """Test that sink_asset writes data to the sink."""
    sink = StubSink()
    schema = Schema(
        name="user_schema",
        fields=(SchemaField(name="name", data_type=DataType.STRING, required=True),),
    )

    asset = sink_asset(name="save_users", sink=sink, schema=schema)
    context = PipelineContext(pipeline_id="test", run_id="test_run")

    # Create test data (list of dicts, simulating upstream)
    input_data = [{"name": "Alice"}, {"name": "Bob"}]

    # Execute the operator
    result = asset.operator.fn(input_data, context)

    # Verify write was called
    assert len(sink.written_data) == 1

    # Note: sink_asset coerces data to DataRecords when schema is provided
    # Verify written data contains DataRecords with the correct values
    assert len(sink.written_data[0]) == 2
    assert all(isinstance(d, DataRecord) for d in sink.written_data[0])
    assert sink.written_data[0][0].data["name"] == "Alice"
    assert sink.written_data[0][1].data["name"] == "Bob"

    # Verify result summary
    assert result["success"] is True
    assert result["records_written"] == 2
    assert result["error"] is None
    assert result["sink_class"] == "StubSink"


def test_sink_asset_without_schema() -> None:
    """Test that sink_asset passes raw data when no schema is provided."""
    sink = StubSink()

    asset = sink_asset(name="save_users", sink=sink, schema=None)
    context = PipelineContext(pipeline_id="test", run_id="test_run")

    # Create test data (list of dicts)
    input_data = [{"name": "Alice"}, {"name": "Bob"}]

    # Execute the operator
    result = asset.operator.fn(input_data, context)

    # Verify write was called
    assert len(sink.written_data) == 1
    assert sink.written_data[0] == input_data

    # Verify result summary
    assert result["success"] is True
    assert result["records_written"] == 2


def test_sink_asset_with_datarecord_coercion() -> None:
    """Test that sink_asset coerces DataRecords when schema is provided."""
    sink = StubSink()
    schema = Schema(
        name="user_schema",
        fields=(SchemaField(name="name", data_type=DataType.STRING, required=True),),
    )

    asset = sink_asset(name="save_users", sink=sink, schema=schema)
    context = PipelineContext(pipeline_id="test", run_id="test_run")

    # Create DataRecords as input
    record1 = DataRecord(data={"name": "Alice"}, schema=schema)
    record2 = DataRecord(data={"name": "Bob"}, schema=schema)
    input_data = [record1, record2]

    # Execute the operator
    result = asset.operator.fn(input_data, context)

    # Verify write was called
    assert len(sink.written_data) == 1

    # Verify result summary
    assert result["success"] is True
    assert result["records_written"] == 2
    assert result["sink_class"] == "StubSink"


def test_sink_asset_initializes_sink() -> None:
    """Test that sink_asset initializes the sink before writing."""
    sink = StubSink()
    schema = Schema(
        name="user_schema",
        fields=(SchemaField(name="name", data_type=DataType.STRING, required=True),),
    )

    asset = sink_asset(name="save_users", sink=sink, schema=schema)
    context = PipelineContext(pipeline_id="test", run_id="test_run")

    # Execute the operator
    asset.operator.fn([], context)

    # Verify sink was initialized
    assert sink.initialized


def test_sink_asset_propagates_write_errors() -> None:
    """Test that sink_asset propagates sink write errors."""

    # Create a sink that fails
    class FailingSink:
        def write(self, data: list[dict[str, any]], context: PipelineContext) -> SinkResult:
            return SinkResult(
                success=False,
                records_written=0,
                error="Write failed!",
            )

        def initialize(self, context: PipelineContext) -> None:
            pass

        def cleanup(self, context: PipelineContext) -> None:
            pass

        def get_metrics(self) -> dict[str, int | float]:
            return {}

    sink = FailingSink()
    schema = Schema(
        name="user_schema",
        fields=(SchemaField(name="name", data_type=DataType.STRING, required=True),),
    )

    asset = sink_asset(name="save_users", sink=sink, schema=schema)
    context = PipelineContext(pipeline_id="test", run_id="test_run")

    # Create test data (list of dicts)
    input_data = [{"name": "Alice"}]

    # Execute the operator
    result = asset.operator.fn(input_data, context)

    # Verify error is propagated
    assert result["success"] is False
    assert result["records_written"] == 0
    assert result["error"] == "Write failed!"


# =============================================================================
# Integration Tests
# =============================================================================


def test_source_sink_pipeline() -> None:
    """Test a complete pipeline from source through sink."""
    source_data = [{"name": "Alice"}, {"name": "Bob"}]
    source = StubSource(source_data)
    sink = StubSink()
    schema = Schema(
        name="user_schema",
        fields=(SchemaField(name="name", data_type=DataType.STRING, required=True),),
    )

    # Create assets
    source_asset_obj = source_asset(name="users", source=source, schema=schema)
    sink_asset_obj = sink_asset(name="save_users", sink=sink, schema=schema)

    context = PipelineContext(pipeline_id="test", run_id="test_run")

    # Execute source asset
    source_result = source_asset_obj.operator.fn([], context)

    assert len(source_result) == 2
    assert all(isinstance(r, DataRecord) for r in source_result)

    # Execute sink asset with source output
    sink_result = sink_asset_obj.operator.fn(source_result, context)

    assert sink_result["success"] is True
    assert sink_result["records_written"] == 2
