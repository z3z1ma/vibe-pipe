"""Factory functions for creating test objects."""

from collections.abc import Callable
from datetime import datetime
from typing import Any

from vibe_piper.types import (
    Asset,
    AssetGraph,
    AssetType,
    DataRecord,
    DataType,
    Operator,
    OperatorType,
    Pipeline,
    PipelineContext,
    Schema,
    SchemaField,
)


def make_schema(
    name: str = "test_schema",
    fields: tuple[SchemaField, ...] | None = None,
    description: str | None = None,
) -> Schema:
    """
    Create a test schema with the given parameters.

    Args:
        name: Schema name
        fields: Optional tuple of schema fields
        description: Optional description

    Returns:
        A Schema object for testing
    """
    if fields is None:
        fields = (
            SchemaField(name="id", data_type=DataType.INTEGER, required=True),
            SchemaField(name="name", data_type=DataType.STRING, required=True),
        )

    return Schema(name=name, fields=fields, description=description)


def make_data_record(
    data: dict[str, Any] | None = None,
    schema: Schema | None = None,
    metadata: dict[str, Any] | None = None,
) -> DataRecord:
    """
    Create a test data record.

    Args:
        data: Optional data dictionary
        schema: Optional schema (creates default if None)
        metadata: Optional metadata

    Returns:
        A DataRecord object for testing
    """
    if data is None:
        data = {"id": 1, "name": "test"}

    if schema is None:
        # Infer schema from data
        fields = []
        for key, value in data.items():
            if isinstance(value, int):
                data_type = DataType.INTEGER
            elif isinstance(value, float):
                data_type = DataType.FLOAT
            elif isinstance(value, bool):
                data_type = DataType.BOOLEAN
            elif isinstance(value, str):
                data_type = DataType.STRING
            elif isinstance(value, list):
                data_type = DataType.ARRAY
            elif isinstance(value, dict):
                data_type = DataType.OBJECT
            else:
                data_type = DataType.ANY

            fields.append(SchemaField(name=key, data_type=data_type, required=True))

        schema = Schema(name="inferred_schema", fields=tuple(fields))

    return DataRecord(data=data, schema=schema, metadata=metadata or {})


def make_pipeline_context(
    pipeline_id: str = "test_pipeline",
    run_id: str | None = None,
    config: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> PipelineContext:
    """
    Create a test pipeline context.

    Args:
        pipeline_id: Pipeline identifier
        run_id: Optional run identifier (generates one if None)
        config: Optional configuration dictionary
        metadata: Optional metadata dictionary

    Returns:
        A PipelineContext object for testing
    """
    if run_id is None:
        run_id = f"{pipeline_id}_{datetime.now().isoformat()}"

    return PipelineContext(
        pipeline_id=pipeline_id,
        run_id=run_id,
        config=config or {},
        metadata=metadata or {},
    )


def make_operator(
    name: str = "test_operator",
    operator_type: OperatorType = OperatorType.TRANSFORM,
    fn: Callable[[Any, PipelineContext], Any] | None = None,
    description: str | None = None,
) -> Operator:
    """
    Create a test operator.

    Args:
        name: Operator name
        operator_type: Type of operator
        fn: Optional transformation function
        description: Optional description

    Returns:
        An Operator object for testing
    """
    if fn is None:
        # Default function that returns data unchanged
        def default_fn(data: Any, context: PipelineContext) -> Any:
            return data

        fn = default_fn

    return Operator(
        name=name,
        operator_type=operator_type,
        fn=fn,
        description=description,
    )


def make_asset(
    name: str = "test_asset",
    asset_type: AssetType = AssetType.MEMORY,
    uri: str | None = None,
    schema: Schema | None = None,
    operator: Operator | None = None,
    description: str | None = None,
) -> Asset:
    """
    Create a test asset.

    Args:
        name: Asset name
        asset_type: Type of asset
        uri: Optional URI (generates default if None)
        schema: Optional schema
        operator: Optional operator
        description: Optional description

    Returns:
        An Asset object for testing
    """
    if uri is None:
        uri = f"{asset_type.name.lower()}://{name}"

    return Asset(
        name=name,
        asset_type=asset_type,
        uri=uri,
        schema=schema,
        operator=operator,
        description=description,
    )


def make_asset_graph(
    name: str = "test_graph",
    assets: tuple[Asset, ...] | None = None,
    dependencies: dict[str, tuple[str, ...]] | None = None,
    description: str | None = None,
) -> AssetGraph:
    """
    Create a test asset graph.

    Args:
        name: Graph name
        assets: Optional tuple of assets
        dependencies: Optional dependency mapping
        description: Optional description

    Returns:
        An AssetGraph object for testing
    """
    if assets is None:
        assets = (make_asset(name="asset1"), make_asset(name="asset2"))

    if dependencies is None:
        dependencies = {}

    return AssetGraph(
        name=name,
        assets=assets,
        dependencies=dependencies,
        description=description,
    )


def make_pipeline(
    name: str = "test_pipeline",
    operators: tuple[Operator, ...] | None = None,
    input_schema: Schema | None = None,
    output_schema: Schema | None = None,
    description: str | None = None,
) -> Pipeline:
    """
    Create a test pipeline.

    Args:
        name: Pipeline name
        operators: Optional tuple of operators
        input_schema: Optional input schema
        output_schema: Optional output schema
        description: Optional description

    Returns:
        A Pipeline object for testing
    """
    if operators is None:
        operators = ()

    return Pipeline(
        name=name,
        operators=operators,
        input_schema=input_schema,
        output_schema=output_schema,
        description=description,
    )


# =============================================================================
# Specialized Factories for Common Test Scenarios
# =============================================================================


def make_user_schema() -> Schema:
    """Create a schema for user records."""
    return Schema(
        name="user_schema",
        fields=(
            SchemaField(name="user_id", data_type=DataType.INTEGER, required=True),
            SchemaField(name="username", data_type=DataType.STRING, required=True),
            SchemaField(name="email", data_type=DataType.STRING, required=True),
            SchemaField(name="created_at", data_type=DataType.DATETIME, required=True),
            SchemaField(name="is_active", data_type=DataType.BOOLEAN, required=False),
        ),
    )


def make_product_schema() -> Schema:
    """Create a schema for product records."""
    return Schema(
        name="product_schema",
        fields=(
            SchemaField(name="product_id", data_type=DataType.INTEGER, required=True),
            SchemaField(name="name", data_type=DataType.STRING, required=True),
            SchemaField(name="price", data_type=DataType.FLOAT, required=True),
            SchemaField(name="quantity", data_type=DataType.INTEGER, required=False),
            SchemaField(name="in_stock", data_type=DataType.BOOLEAN, required=False),
        ),
    )


def make_linear_asset_graph() -> AssetGraph:
    """Create a simple linear asset graph: A -> B -> C."""
    asset_a = make_asset(name="source_asset")
    asset_b = make_asset(name="intermediate_asset")
    asset_c = make_asset(name="final_asset")

    return AssetGraph(
        name="linear_graph",
        assets=(asset_a, asset_b, asset_c),
        dependencies={
            "intermediate_asset": ("source_asset",),
            "final_asset": ("intermediate_asset",),
        },
    )


def make_diamond_asset_graph() -> AssetGraph:
    """Create a diamond-shaped asset graph: A -> B, A -> C, B -> D, C -> D."""
    asset_a = make_asset(name="source")
    asset_b = make_asset(name="branch1")
    asset_c = make_asset(name="branch2")
    asset_d = make_asset(name="merge")

    return AssetGraph(
        name="diamond_graph",
        assets=(asset_a, asset_b, asset_c, asset_d),
        dependencies={
            "branch1": ("source",),
            "branch2": ("source",),
            "merge": ("branch1", "branch2"),
        },
    )
