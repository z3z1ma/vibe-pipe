"""Example tests demonstrating the testing infrastructure.

This file shows how to use the testing infrastructure including fixtures,
assertion helpers, fake data generators, and more.
"""

from datetime import datetime
from typing import Any

import pytest

from tests.fixtures.fake_data import FakeDataGenerator, fake_user_data
from tests.helpers import (
    assert_asset_graph_valid,
    assert_asset_valid,
    assert_data_conforms_to_schema,
    assert_lineage,
    assert_matches_snapshot,
    assert_no_circular_dependencies,
    assert_schema_valid,
    assert_topological_order,
    make_asset,
    make_data_record,
    make_diamond_asset_graph,
    make_linear_asset_graph,
    make_pipeline,
    make_product_schema,
    make_schema,
    make_user_schema,
)
from vibe_piper.types import (
    Asset,
    AssetGraph,
    AssetType,
    DataType,
    Operator,
    OperatorType,
    Pipeline,
    PipelineContext,
    Schema,
    SchemaField,
)

# =============================================================================
# Example 1: Using Fixtures
# =============================================================================


def test_using_basic_schema(basic_schema: Schema) -> None:
    """Example: Using the basic_schema fixture."""
    assert basic_schema.name == "basic_schema"
    assert len(basic_schema.fields) == 4

    # Verify field types
    field_names = {f.name for f in basic_schema.fields}
    assert field_names == {"id", "name", "email", "age"}


def test_using_pipeline_context(pipeline_context: PipelineContext) -> None:
    """Example: Using the pipeline_context fixture."""
    assert pipeline_context.pipeline_id == "test_pipeline"
    assert pipeline_context.run_id == "test_run_001"
    assert pipeline_context.config["env"] == "test"


def test_using_memory_asset(memory_asset: Asset) -> None:
    """Example: Using the memory_asset fixture."""
    assert memory_asset.name == "test_asset"
    assert memory_asset.asset_type == AssetType.MEMORY
    assert memory_asset.uri == "memory://test_asset"


def test_using_simple_pipeline(simple_pipeline: Pipeline) -> None:
    """Example: Using the simple_pipeline fixture."""
    from tests.helpers.factories import make_pipeline_context

    assert simple_pipeline.name == "simple_pipeline"
    assert len(simple_pipeline.operators) == 2

    # Execute the pipeline
    ctx = make_pipeline_context()
    result = simple_pipeline.execute([], ctx)
    assert result is not None


# =============================================================================
# Example 2: Using Assertion Helpers
# =============================================================================


def test_assert_schema_valid() -> None:
    """Example: Using assert_schema_valid helper."""
    schema = make_schema(
        name="test",
        fields=(
            SchemaField(name="id", data_type=DataType.INTEGER, required=True),
            SchemaField(name="name", data_type=DataType.STRING, required=True),
        ),
    )

    # This will pass silently
    assert_schema_valid(schema)

    # This would fail (commented out to avoid test failure)
    # invalid_schema = Schema(name="", fields=())  # Empty name
    # assert_schema_valid(invalid_schema)  # Would raise AssertionError


def test_assert_data_conforms_to_schema(basic_schema: Schema) -> None:
    """Example: Using assert_data_conforms_to_schema helper."""
    valid_data = {
        "id": 1,
        "name": "Alice",
        "email": "alice@example.com",
        "age": 30,
    }

    # This will pass silently
    assert_data_conforms_to_schema(valid_data, basic_schema)


def test_assert_asset_valid() -> None:
    """Example: Using assert_asset_valid helper."""
    asset = make_asset(
        name="test_asset",
        asset_type=AssetType.MEMORY,
        uri="memory://test_asset",
    )

    # This will pass silently
    assert_asset_valid(asset)


def test_assert_asset_graph_valid() -> None:
    """Example: Using assert_asset_graph_valid helper."""
    asset1 = make_asset(name="asset1")
    asset2 = make_asset(name="asset2")

    graph = AssetGraph(
        name="test_graph",
        assets=(asset1, asset2),
        dependencies={"asset2": ("asset1",)},
    )

    # This will pass silently
    assert_asset_graph_valid(graph)
    assert_no_circular_dependencies(graph)


def test_assert_topological_order() -> None:
    """Example: Using assert_topological_order helper."""
    asset1 = make_asset(name="source")
    asset2 = make_asset(name="intermediate")
    asset3 = make_asset(name="final")

    graph = AssetGraph(
        name="linear_graph",
        assets=(asset1, asset2, asset3),
        dependencies={
            "intermediate": ("source",),
            "final": ("intermediate",),
        },
    )

    order = graph.topological_order()
    assert_topological_order(graph, order)


def test_assert_lineage() -> None:
    """Example: Using assert_lineage helper."""
    asset1 = make_asset(name="source")
    asset2 = make_asset(name="intermediate")
    asset3 = make_asset(name="final")

    graph = AssetGraph(
        name="linear_graph",
        assets=(asset1, asset2, asset3),
        dependencies={
            "intermediate": ("source",),
            "final": ("intermediate",),
        },
    )

    # final depends on intermediate which depends on source
    assert_lineage(graph, "final", ["source", "intermediate"])


# =============================================================================
# Example 3: Using Factory Functions
# =============================================================================


def test_make_schema() -> None:
    """Example: Using make_schema factory."""
    schema = make_schema(
        name="product",
        fields=(
            SchemaField(name="product_id", data_type=DataType.INTEGER, required=True),
            SchemaField(name="name", data_type=DataType.STRING, required=True),
            SchemaField(name="price", data_type=DataType.FLOAT, required=True),
        ),
    )

    assert schema.name == "product"
    assert len(schema.fields) == 3


def test_make_data_record() -> None:
    """Example: Using make_data_record factory."""
    record = make_data_record(
        data={"id": 1, "name": "test", "value": 100},
    )

    assert record.data["id"] == 1
    assert record.schema.name == "inferred_schema"


def test_make_pipeline() -> None:
    """Example: Using make_pipeline factory."""
    pipeline = make_pipeline(
        name="test_pipeline",
        operators=(
            Operator(
                name="op1",
                operator_type=OperatorType.SOURCE,
                fn=lambda data, ctx: [1, 2, 3],
            ),
        ),
    )

    assert pipeline.name == "test_pipeline"
    assert len(pipeline.operators) == 1


# =============================================================================
# Example 4: Using Fake Data Generators
# =============================================================================


def test_fake_data_generator() -> None:
    """Example: Using FakeDataGenerator class."""
    generator = FakeDataGenerator(seed=42)

    # Generate for a schema
    schema = make_schema(
        name="user",
        fields=(
            SchemaField(name="id", data_type=DataType.INTEGER, required=True),
            SchemaField(name="name", data_type=DataType.STRING, required=True),
            SchemaField(name="email", data_type=DataType.STRING, required=True),
        ),
    )

    data = generator.generate_for_schema(schema, count=5)

    assert len(data) == 5
    assert all("id" in record for record in data)
    assert all("name" in record for record in data)


def test_fake_user_data() -> None:
    """Example: Using fake_user_data convenience function."""
    users = fake_user_data(count=10, seed=42)

    assert len(users) == 10
    assert all("user_id" in user for user in users)
    assert all("username" in user for user in users)
    assert all("email" in user for user in users)


def test_fake_data_reproducibility() -> None:
    """Example: Fake data is reproducible with same seed."""
    data1 = fake_user_data(count=5, seed=42)
    data2 = fake_user_data(count=5, seed=42)

    # Check that all fields except timestamps match exactly
    assert len(data1) == len(data2)
    for i, (record1, record2) in enumerate(zip(data1, data2)):
        # All fields except created_at should match
        for key in record1:
            if key != "created_at":
                assert record1[key] == record2[key], f"Record {i}, field {key} doesn't match"
        # created_at should have the same date (ignoring microseconds)
        if "created_at" in record1 and "created_at" in record2:
            date1 = record1["created_at"].split(".")[0]
            date2 = record2["created_at"].split(".")[0]
            assert date1 == date2, f"Record {i}, created_at date doesn't match"


def test_fake_data_different_seeds() -> None:
    """Example: Different seeds produce different data."""
    data1 = fake_user_data(count=5, seed=42)
    data2 = fake_user_data(count=5, seed=123)

    assert data1 != data2


# =============================================================================
# Example 5: Testing Schema Validation
# =============================================================================


def test_schema_validation_success(user_schema: Schema) -> None:
    """Example: Testing schema validation with valid data."""
    valid_data = {
        "user_id": 123,
        "username": "testuser",
        "email": "test@example.com",
        "created_at": datetime.now().isoformat(),
        "is_active": True,
    }

    # Should not raise any exception
    assert_data_conforms_to_schema(valid_data, user_schema)


def test_schema_validation_missing_required(user_schema: Schema) -> None:
    """Example: Testing schema validation with missing required field."""
    invalid_data = {
        "user_id": 123,
        # Missing required 'username' field
        "email": "test@example.com",
        "created_at": datetime.now().isoformat(),
    }

    with pytest.raises(AssertionError, match="Required field"):
        assert_data_conforms_to_schema(invalid_data, user_schema)


def test_schema_validation_invalid_nullable(user_schema: Schema) -> None:
    """Example: Testing schema validation with invalid null value."""
    invalid_data = {
        "user_id": None,  # Not nullable
        "username": "testuser",
        "email": "test@example.com",
        "created_at": datetime.now().isoformat(),
    }

    with pytest.raises(AssertionError, match="not nullable"):
        assert_data_conforms_to_schema(invalid_data, user_schema)


# =============================================================================
# Example 6: Testing Pipeline Execution
# =============================================================================


def test_simple_pipeline_execution() -> None:
    """Example: Testing a simple pipeline execution."""
    # Create a simple pipeline
    pipeline = make_pipeline(
        name="uppercase_pipeline",
        operators=(
            Operator(
                name="source",
                operator_type=OperatorType.SOURCE,
                fn=lambda data, ctx: [{"name": "alice"}, {"name": "bob"}],
            ),
            Operator(
                name="uppercase",
                operator_type=OperatorType.TRANSFORM,
                fn=lambda data, ctx: [
                    {k: v.upper() if isinstance(v, str) else v for k, v in item.items()}
                    for item in data
                ],
            ),
        ),
    )

    result = pipeline.execute([])

    assert len(result) == 2
    assert result[0]["name"] == "ALICE"
    assert result[1]["name"] == "BOB"


def test_pipeline_with_context() -> None:
    """Example: Testing pipeline with context."""
    # Create a context with state
    ctx = PipelineContext(
        pipeline_id="test",
        run_id="run1",
    )
    ctx.set_state("counter", 0)

    # Create a pipeline that uses context state
    def counting_transform(data, context):
        count = context.get_state("counter", 0)
        context.set_state("counter", count + 1)
        return data

    pipeline = make_pipeline(
        name="counting_pipeline",
        operators=(
            Operator(
                name="counter",
                operator_type=OperatorType.TRANSFORM,
                fn=counting_transform,
            ),
        ),
    )

    pipeline.execute({}, ctx)

    # Verify counter was incremented
    assert ctx.get_state("counter") == 1


# =============================================================================
# Example 7: Testing Asset Graphs
# =============================================================================


def test_linear_asset_graph() -> None:
    """Example: Testing a linear asset graph."""
    assets = (
        make_asset(name="source"),
        make_asset(name="transform"),
        make_asset(name="sink"),
    )

    graph = AssetGraph(
        name="linear",
        assets=assets,
        dependencies={
            "transform": ("source",),
            "sink": ("transform",),
        },
    )

    assert_asset_graph_valid(graph)

    order = graph.topological_order()
    assert order == ("source", "transform", "sink")


def test_diamond_asset_graph() -> None:
    """Example: Testing a diamond-shaped asset graph."""
    assets = (
        make_asset(name="source"),
        make_asset(name="branch1"),
        make_asset(name="branch2"),
        make_asset(name="merge"),
    )

    graph = AssetGraph(
        name="diamond",
        assets=assets,
        dependencies={
            "branch1": ("source",),
            "branch2": ("source",),
            "merge": ("branch1", "branch2"),
        },
    )

    assert_asset_graph_valid(graph)
    assert_no_circular_dependencies(graph)


def test_circular_dependency_detection() -> None:
    """Example: Testing circular dependency detection."""
    assets = (
        make_asset(name="a"),
        make_asset(name="b"),
        make_asset(name="c"),
    )

    # Create a circular dependency: a -> b -> c -> a
    with pytest.raises(ValueError, match="Circular dependency"):
        AssetGraph(
            name="circular",
            assets=assets,
            dependencies={
                "b": ("a",),
                "c": ("b",),
                "a": ("c",),
            },
        )


# =============================================================================
# Example 8: Parameterized Testing
# =============================================================================


@pytest.mark.parametrize(
    "asset_type,expected_prefix",
    [
        (AssetType.MEMORY, "memory://"),
        (AssetType.FILE, "file://"),
        (AssetType.API, "api://"),
    ],
)
def test_asset_uri_prefixes(asset_type: AssetType, expected_prefix: str) -> None:
    """Example: Parameterized test for asset URI prefixes."""
    asset = make_asset(asset_type=asset_type)
    assert asset.uri.startswith(expected_prefix)


@pytest.mark.parametrize(
    "data_type,example_value",
    [
        (DataType.STRING, "test"),
        (DataType.INTEGER, 42),
        (DataType.FLOAT, 3.14),
        (DataType.BOOLEAN, True),
    ],
)
def test_schema_field_types(data_type: DataType, example_value: Any) -> None:
    """Example: Parameterized test for schema field types."""
    field = SchemaField(name="test", data_type=data_type, required=True)
    assert field.data_type == data_type


# =============================================================================
# Example 9: Testing Edge Cases
# =============================================================================


def test_pipeline_with_empty_input() -> None:
    """Example: Testing pipeline behavior with empty input."""
    pipeline = make_pipeline(
        name="identity_pipeline",
        operators=(
            Operator(
                name="identity",
                operator_type=OperatorType.TRANSFORM,
                fn=lambda data, ctx: data,
            ),
        ),
    )

    result = pipeline.execute([])
    assert result == []


def test_schema_with_no_fields() -> None:
    """Example: Testing schema with no fields."""
    schema = Schema(name="empty", fields=())
    assert_schema_valid(schema)
    assert len(schema.fields) == 0


def test_asset_graph_with_no_dependencies() -> None:
    """Example: Testing asset graph with no dependencies."""
    assets = (make_asset(name="a"), make_asset(name="b"))
    graph = AssetGraph(name="no_deps", assets=assets)

    assert_asset_graph_valid(graph)
    order = graph.topological_order()
    assert set(order) == {"a", "b"}


# =============================================================================
# Example 10: Integration with Mock IO
# =============================================================================


def test_mock_io_manager(mock_io_manager) -> None:
    """Example: Using mock IO manager for testing."""
    test_data = {"id": 1, "name": "test"}

    # Write data
    mock_io_manager.write("memory://test", test_data)
    assert mock_io_manager.exists("memory://test")

    # Read data back
    retrieved = mock_io_manager.read("memory://test")
    assert retrieved == test_data

    # Check tracking
    assert "memory://test" in mock_io_manager.writes
    assert "memory://test" in mock_io_manager.reads


def test_populated_io_manager(populated_io_manager) -> None:
    """Example: Using pre-populated mock IO manager."""
    # Data is already populated
    data = populated_io_manager.read("memory://test_data")
    assert data is not None
    assert data["id"] == 1

    users = populated_io_manager.read("memory://users")
    assert len(users) == 2


# =============================================================================
# Example 11: Snapshot Testing
# =============================================================================


def test_snapshot_pipeline_output() -> None:
    """Example: Using snapshot testing to verify pipeline output.

    Snapshot tests are useful for verifying that data transformations
    produce consistent outputs. Run with --update-snapshots to create/update.
    """
    pipeline = make_pipeline(
        name="transform_pipeline",
        operators=(
            Operator(
                name="uppercase",
                operator_type=OperatorType.TRANSFORM,
                fn=lambda data, ctx: [{"name": "ALICE"}, {"name": "BOB"}],
            ),
        ),
    )

    result = pipeline.execute([])

    # Snapshot will be created on first run or updated with --update-snapshots
    assert_matches_snapshot(result, "tests/snapshots/pipeline_output.json")


def test_snapshot_data_transformation() -> None:
    """Example: Snapshot testing for data transformations."""
    # Sample transformation result
    transformed_data = {
        "users": [
            {"id": 1, "name": "Alice", "status": "active"},
            {"id": 2, "name": "Bob", "status": "inactive"},
            {"id": 3, "name": "Charlie", "status": "active"},
        ],
        "summary": {"total": 3, "active": 2, "inactive": 1},
    }

    # Verify transformation output is consistent
    assert_matches_snapshot(transformed_data, "tests/snapshots/transformation.json")


def test_snapshot_api_response() -> None:
    """Example: Snapshot testing for API responses."""
    # Mock API response structure
    api_response = {
        "status": "success",
        "code": 200,
        "data": {
            "items": [
                {"id": "item1", "value": 100},
                {"id": "item2", "value": 200},
            ],
            "pagination": {"page": 1, "per_page": 10, "total": 2},
        },
        "metadata": {
            "timestamp": "2026-01-31T12:00:00Z",
            "version": "1.0",
        },
    }

    # Verify API response structure
    assert_matches_snapshot(api_response, "tests/snapshots/api_response.json")


# =============================================================================
# Example 12: Specialized Factory Helpers
# =============================================================================


def test_specialized_factory_helpers_importable() -> None:
    """Test that specialized factory helpers are available from tests.helpers."""
    # Test make_user_schema
    user_schema = make_user_schema()
    assert user_schema.name == "user_schema"
    assert len(user_schema.fields) == 5
    field_names = {f.name for f in user_schema.fields}
    assert field_names == {"user_id", "username", "email", "created_at", "is_active"}

    # Test make_product_schema
    product_schema = make_product_schema()
    assert product_schema.name == "product_schema"
    assert len(product_schema.fields) == 5
    field_names = {f.name for f in product_schema.fields}
    assert field_names == {"product_id", "name", "price", "quantity", "in_stock"}

    # Test make_linear_asset_graph
    linear_graph = make_linear_asset_graph()
    assert linear_graph.name == "linear_graph"
    assert len(linear_graph.assets) == 3
    assert assert_no_circular_dependencies(linear_graph) is None
    order = linear_graph.topological_order()
    assert order == ("source_asset", "intermediate_asset", "final_asset")

    # Test make_diamond_asset_graph
    diamond_graph = make_diamond_asset_graph()
    assert diamond_graph.name == "diamond_graph"
    assert len(diamond_graph.assets) == 4
    assert assert_no_circular_dependencies(diamond_graph) is None
    assert len(diamond_graph.dependencies["merge"]) == 2
