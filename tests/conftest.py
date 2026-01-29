"""Pytest configuration and fixtures for vibe_piper tests."""

import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

# Add src directory to Python path
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))


def pytest_collection_modifyitems(config: Any, items: list[pytest.Item]) -> None:
    """Mark tests under tests/integration as integration."""
    for item in items:
        if "/tests/integration/" in str(item.fspath):
            item.add_marker(pytest.mark.integration)


from vibe_piper.types import (
    Asset,
    AssetGraph,
    AssetResult,
    AssetType,
    DataRecord,
    DataType,
    ExecutionResult,
    Operator,
    OperatorType,
    Pipeline,
    PipelineContext,
    Schema,
    SchemaField,
)

# =============================================================================
# Schema Fixtures
# =============================================================================


@pytest.fixture
def basic_schema() -> Schema:
    """A basic schema with common field types."""
    return Schema(
        name="basic_schema",
        fields=(
            SchemaField(name="id", data_type=DataType.INTEGER, required=True),
            SchemaField(name="name", data_type=DataType.STRING, required=True),
            SchemaField(name="email", data_type=DataType.STRING, required=False, nullable=True),
            SchemaField(name="age", data_type=DataType.INTEGER, required=False, nullable=True),
        ),
        description="Basic schema with common field types",
    )


@pytest.fixture
def sample_schema(basic_schema: Schema) -> Schema:
    """Alias fixture used by expectation tests."""
    return basic_schema


@pytest.fixture
def user_schema() -> Schema:
    """Schema representing a user record."""
    return Schema(
        name="user_schema",
        fields=(
            SchemaField(name="user_id", data_type=DataType.INTEGER, required=True),
            SchemaField(name="username", data_type=DataType.STRING, required=True),
            SchemaField(name="email", data_type=DataType.STRING, required=True),
            SchemaField(name="created_at", data_type=DataType.DATETIME, required=True),
            SchemaField(
                name="is_active",
                data_type=DataType.BOOLEAN,
                required=False,
                nullable=True,
            ),
        ),
        description="User account schema",
    )


@pytest.fixture
def product_schema() -> Schema:
    """Schema representing a product record."""
    return Schema(
        name="product_schema",
        fields=(
            SchemaField(name="product_id", data_type=DataType.INTEGER, required=True),
            SchemaField(name="name", data_type=DataType.STRING, required=True),
            SchemaField(name="price", data_type=DataType.FLOAT, required=True),
            SchemaField(name="quantity", data_type=DataType.INTEGER, required=False),
            SchemaField(name="tags", data_type=DataType.ARRAY, required=False, nullable=True),
        ),
        description="Product catalog schema",
    )


@pytest.fixture
def event_schema() -> Schema:
    """Schema representing an event/log record."""
    return Schema(
        name="event_schema",
        fields=(
            SchemaField(name="event_id", data_type=DataType.STRING, required=True),
            SchemaField(name="timestamp", data_type=DataType.DATETIME, required=True),
            SchemaField(name="event_type", data_type=DataType.STRING, required=True),
            SchemaField(name="payload", data_type=DataType.OBJECT, required=False, nullable=True),
        ),
        description="Event tracking schema",
    )


@pytest.fixture
def schema_with_constraints() -> Schema:
    """Schema with various field constraints."""
    return Schema(
        name="constrained_schema",
        fields=(
            SchemaField(
                name="username",
                data_type=DataType.STRING,
                required=True,
                constraints={
                    "min_length": 3,
                    "max_length": 50,
                    "pattern": r"^[a-zA-Z0-9_]+$",
                },
            ),
            SchemaField(
                name="age",
                data_type=DataType.INTEGER,
                required=True,
                constraints={"min_value": 0, "max_value": 150},
            ),
            SchemaField(
                name="score",
                data_type=DataType.FLOAT,
                required=True,
                constraints={"min_value": 0.0, "max_value": 100.0},
            ),
        ),
        description="Schema with validation constraints",
    )


# =============================================================================
# Pipeline Context Fixtures
# =============================================================================


@pytest.fixture
def pipeline_context() -> PipelineContext:
    """A basic pipeline execution context."""
    return PipelineContext(
        pipeline_id="test_pipeline",
        run_id="test_run_001",
        config={"env": "test", "log_level": "INFO"},
        metadata={"test_run": True},
    )


@pytest.fixture
def pipeline_context_with_state() -> PipelineContext:
    """Pipeline context with pre-populated state."""
    ctx = PipelineContext(
        pipeline_id="test_pipeline",
        run_id="test_run_002",
        config={"env": "test"},
    )
    ctx.set_state("counter", 0)
    ctx.set_state("items_processed", [])
    return ctx


@pytest.fixture
def production_context() -> PipelineContext:
    """Pipeline context configured for production environment."""
    return PipelineContext(
        pipeline_id="prod_pipeline",
        run_id="prod_run_001",
        config={
            "env": "production",
            "log_level": "WARNING",
            "max_retries": 3,
            "timeout_seconds": 300,
        },
        metadata={"deployment": "production", "region": "us-east-1"},
    )


# =============================================================================
# Operator Fixtures
# =============================================================================


@pytest.fixture
def source_operator() -> Operator:
    """A simple source operator that returns static data."""

    def source_fn(data: Any, context: PipelineContext) -> list[dict[str, Any]]:
        return [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]

    return Operator(
        name="source_data",
        operator_type=OperatorType.SOURCE,
        fn=source_fn,
        description="Source operator that returns static user data",
    )


@pytest.fixture
def transform_operator() -> Operator:
    """A transform operator that uppercase all string values."""

    def transform_fn(data: Any, context: PipelineContext) -> Any:
        if isinstance(data, list):
            return [
                {k: v.upper() if isinstance(v, str) else v for k, v in item.items()}
                for item in data
            ]
        elif isinstance(data, dict):
            return {k: v.upper() if isinstance(v, str) else v for k, v in data.items()}
        return data

    return Operator(
        name="uppercase_strings",
        operator_type=OperatorType.TRANSFORM,
        fn=transform_fn,
        description="Transform operator that uppercases all string values",
    )


@pytest.fixture
def filter_operator() -> Operator:
    """A filter operator that filters data based on a condition."""

    def filter_fn(data: Any, context: PipelineContext) -> Any:
        if isinstance(data, list):
            return [item for item in data if item.get("age", 0) >= 18]
        return data

    return Operator(
        name="filter_adults",
        operator_type=OperatorType.FILTER,
        fn=filter_fn,
        description="Filter operator that keeps only adult records",
    )


@pytest.fixture
def aggregate_operator() -> Operator:
    """An aggregate operator that counts records."""

    def aggregate_fn(data: Any, context: PipelineContext) -> dict[str, int]:
        if isinstance(data, list):
            return {"count": len(data)}
        return {"count": 1 if data else 0}

    return Operator(
        name="count_records",
        operator_type=OperatorType.AGGREGATE,
        fn=aggregate_fn,
        description="Aggregate operator that counts records",
    )


@pytest.fixture
def validated_operator(basic_schema: Schema) -> Operator:
    """An operator with input and output schemas."""

    def validated_fn(data: Any, context: PipelineContext) -> Any:
        # Simply pass through data (schemas handle validation)
        return data

    return Operator(
        name="validated_transform",
        operator_type=OperatorType.TRANSFORM,
        fn=validated_fn,
        input_schema=basic_schema,
        output_schema=basic_schema,
        description="Transform operator with schema validation",
    )


# =============================================================================
# Pipeline Fixtures
# =============================================================================


@pytest.fixture
def empty_pipeline() -> Pipeline:
    """An empty pipeline with no operators."""
    return Pipeline(name="empty_pipeline", description="An empty pipeline for testing")


@pytest.fixture
def simple_pipeline(source_operator: Operator, transform_operator: Operator) -> Pipeline:
    """A simple pipeline with source and transform operators."""
    return Pipeline(
        name="simple_pipeline",
        operators=(source_operator, transform_operator),
        description="Simple pipeline with source and transform",
    )


@pytest.fixture
def multi_stage_pipeline(
    source_operator: Operator,
    transform_operator: Operator,
    filter_operator: Operator,
    aggregate_operator: Operator,
) -> Pipeline:
    """A pipeline with multiple stages: source -> transform -> filter -> aggregate."""
    return Pipeline(
        name="multi_stage_pipeline",
        operators=(
            source_operator,
            transform_operator,
            filter_operator,
            aggregate_operator,
        ),
        description="Multi-stage pipeline demonstrating full data flow",
    )


@pytest.fixture
def validated_pipeline(validated_operator: Operator, basic_schema: Schema) -> Pipeline:
    """A pipeline with input and output schema validation."""
    return Pipeline(
        name="validated_pipeline",
        operators=(validated_operator,),
        input_schema=basic_schema,
        output_schema=basic_schema,
        description="Pipeline with schema validation",
    )


# =============================================================================
# Asset Fixtures
# =============================================================================


@pytest.fixture
def memory_asset() -> Asset:
    """A simple in-memory asset."""
    return Asset(
        name="test_asset",
        asset_type=AssetType.MEMORY,
        uri="memory://test_asset",
        description="A simple in-memory asset for testing",
    )


@pytest.fixture
def table_asset() -> Asset:
    """A database table asset."""
    return Asset(
        name="users_table",
        asset_type=AssetType.TABLE,
        uri="postgresql://localhost:5432/testdb/public.users",
        description="Users table in PostgreSQL",
        metadata={"database": "testdb", "schema": "public", "table": "users"},
    )


@pytest.fixture
def file_asset() -> Asset:
    """A file-based asset."""
    return Asset(
        name="data_file",
        asset_type=AssetType.FILE,
        uri="file:///data/users.parquet",
        description="Parquet file containing user data",
        metadata={"format": "parquet", "compression": "snappy"},
    )


@pytest.fixture
def api_asset() -> Asset:
    """An API endpoint asset."""
    return Asset(
        name="users_api",
        asset_type=AssetType.API,
        uri="https://api.example.com/v1/users",
        description="REST API endpoint for user data",
        metadata={"method": "GET", "auth": "bearer"},
    )


@pytest.fixture
def asset_with_schema(user_schema: Schema) -> Asset:
    """An asset with an associated schema."""
    return Asset(
        name="users_with_schema",
        asset_type=AssetType.TABLE,
        uri="postgresql://localhost:5432/testdb/public.users",
        schema=user_schema,
        description="Users table with schema definition",
    )


# =============================================================================
# Asset Graph Fixtures
# =============================================================================


@pytest.fixture
def simple_asset_graph(memory_asset: Asset) -> AssetGraph:
    """A simple asset graph with a single asset."""
    return AssetGraph(
        name="simple_graph",
        assets=(memory_asset,),
        description="Single-asset graph for testing",
    )


@pytest.fixture
def linear_asset_graph(table_asset: Asset, file_asset: Asset) -> AssetGraph:
    """A linear asset graph with two assets and one dependency."""
    return AssetGraph(
        name="linear_graph",
        assets=(table_asset, file_asset),
        dependencies={"data_file": ("users_table",)},
        description="Linear dependency graph: table -> file",
    )


@pytest.fixture
def complex_asset_graph() -> AssetGraph:
    """A more complex asset graph with multiple dependencies."""
    source1 = Asset(name="source1", asset_type=AssetType.MEMORY, uri="memory://source1")
    source2 = Asset(name="source2", asset_type=AssetType.MEMORY, uri="memory://source2")
    intermediate = Asset(
        name="intermediate", asset_type=AssetType.MEMORY, uri="memory://intermediate"
    )
    final1 = Asset(name="final1", asset_type=AssetType.MEMORY, uri="memory://final1")
    final2 = Asset(name="final2", asset_type=AssetType.MEMORY, uri="memory://final2")

    return AssetGraph(
        name="complex_graph",
        assets=(source1, source2, intermediate, final1, final2),
        dependencies={
            "intermediate": ("source1", "source2"),
            "final1": ("intermediate",),
            "final2": ("intermediate",),
        },
        description="Complex DAG with branching and merging",
    )


# =============================================================================
# Data Record Fixtures
# =============================================================================


@pytest.fixture
def sample_data_record(basic_schema: Schema) -> DataRecord:
    """A sample data record conforming to the basic schema."""
    return DataRecord(
        data={"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30},
        schema=basic_schema,
        metadata={"source": "test"},
    )


@pytest.fixture
def sample_data_records(basic_schema: Schema) -> list[DataRecord]:
    """Multiple sample data records."""
    return [
        DataRecord(
            data={
                "id": i,
                "name": f"User{i}",
                "email": f"user{i}@example.com",
                "age": 20 + i,
            },
            schema=basic_schema,
        )
        for i in range(1, 4)
    ]


@pytest.fixture
def user_data_record(user_schema: Schema) -> DataRecord:
    """A user data record."""
    return DataRecord(
        data={
            "user_id": 12345,
            "username": "testuser",
            "email": "testuser@example.com",
            "created_at": datetime.now().isoformat(),
            "is_active": True,
        },
        schema=user_schema,
    )


# =============================================================================
# Execution Result Fixtures
# =============================================================================


@pytest.fixture
def successful_asset_result() -> AssetResult:
    """A successful asset execution result."""
    return AssetResult(
        asset_name="test_asset",
        success=True,
        data={"result": "success"},
        metrics={"rows_processed": 100, "duration_ms": 50.0},
        duration_ms=50.0,
    )


@pytest.fixture
def failed_asset_result() -> AssetResult:
    """A failed asset execution result."""
    return AssetResult(
        asset_name="test_asset",
        success=False,
        error="Something went wrong",
        duration_ms=25.0,
    )


@pytest.fixture
def successful_execution_result() -> ExecutionResult:
    """A successful execution result."""
    return ExecutionResult(
        success=True,
        asset_results={
            "asset1": AssetResult(asset_name="asset1", success=True, data={"val": 1}),
            "asset2": AssetResult(asset_name="asset2", success=True, data={"val": 2}),
        },
        assets_executed=2,
        assets_succeeded=2,
        assets_failed=0,
        duration_ms=100.0,
    )


@pytest.fixture
def failed_execution_result() -> ExecutionResult:
    """A failed execution result with some failures."""
    return ExecutionResult(
        success=False,
        asset_results={
            "asset1": AssetResult(asset_name="asset1", success=True, data={"val": 1}),
            "asset2": AssetResult(asset_name="asset2", success=False, error="Failed"),
        },
        errors=("asset2: Failed",),
        assets_executed=2,
        assets_succeeded=1,
        assets_failed=1,
        duration_ms=100.0,
    )


# =============================================================================
# Mock IO Manager Fixtures
# =============================================================================


class MockIOManager:
    """Mock IO manager for testing asset I/O operations."""

    def __init__(self) -> None:
        self.storage: dict[str, Any] = {}
        self.reads: list[str] = []
        self.writes: list[str] = []

    def read(self, uri: str) -> Any:
        """Read data from the mock storage."""
        self.reads.append(uri)
        return self.storage.get(uri)

    def write(self, uri: str, data: Any) -> None:
        """Write data to the mock storage."""
        self.writes.append(uri)
        self.storage[uri] = data

    def exists(self, uri: str) -> bool:
        """Check if data exists at the given URI."""
        return uri in self.storage

    def delete(self, uri: str) -> None:
        """Delete data at the given URI."""
        if uri in self.storage:
            del self.storage[uri]

    def clear(self) -> None:
        """Clear all stored data."""
        self.storage.clear()
        self.reads.clear()
        self.writes.clear()


@pytest.fixture
def mock_io_manager() -> MockIOManager:
    """A mock IO manager for testing."""
    return MockIOManager()


@pytest.fixture
def populated_io_manager(mock_io_manager: MockIOManager) -> MockIOManager:
    """A mock IO manager pre-populated with test data."""
    mock_io_manager.write("memory://test_data", {"id": 1, "name": "Test"})
    mock_io_manager.write(
        "memory://users",
        [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ],
    )
    return mock_io_manager


# =============================================================================
# Integration Test Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def test_database_url() -> str:
    """URL for the test database. Uses in-memory SQLite by default."""
    return "sqlite:///:memory:"


@pytest.fixture(scope="session")
def test_data_dir(tmp_path_factory: Any) -> Path:
    """Temporary directory for test data files."""
    return tmp_path_factory.mktemp("test_data")


# =============================================================================
# Parameterized Test Fixtures
# =============================================================================


@pytest.fixture(params=["memory", "file", "table", "api"])
def all_asset_types(request: Any) -> str:
    """Parameterized fixture for all asset types."""
    return request.param


@pytest.fixture(params=[DataType.STRING, DataType.INTEGER, DataType.FLOAT, DataType.BOOLEAN])
def primitive_data_types(request: Any) -> DataType:
    """Parameterized fixture for primitive data types."""
    return request.param


@pytest.fixture(
    params=[
        OperatorType.SOURCE,
        OperatorType.TRANSFORM,
        OperatorType.FILTER,
        OperatorType.AGGREGATE,
    ]
)
def operator_types(request: Any) -> OperatorType:
    """Parameterized fixture for common operator types."""
    return request.param
