"""Custom assertion helpers for vibe_piper testing."""

from collections.abc import Mapping, Sequence
from typing import Any

from vibe_piper.types import (
    Asset,
    AssetGraph,
    AssetResult,
    DataRecord,
    ExecutionResult,
    Schema,
    SchemaField,
)


def assert_schema_valid(
    schema: Schema,
    check_fields: bool = True,
    check_constraints: bool = True,
) -> None:
    """
    Assert that a schema is valid.

    Args:
        schema: The schema to validate
        check_fields: Whether to validate field definitions
        check_constraints: Whether to validate field constraints

    Raises:
        AssertionError: If the schema is invalid
        ValueError: If the schema has structural issues
    """
    # Check basic schema properties
    assert schema.name, "Schema name must not be empty"
    assert isinstance(schema.name, str), "Schema name must be a string"

    if check_fields:
        # Check for duplicate field names
        field_names = [f.name for f in schema.fields]
        assert len(field_names) == len(
            set(field_names)
        ), f"Schema '{schema.name}' has duplicate field names"

        # Validate each field
        for field in schema.fields:
            assert_field_valid(field, check_constraints)


def assert_field_valid(field: SchemaField, check_constraints: bool = True) -> None:
    """
    Assert that a schema field is valid.

    Args:
        field: The schema field to validate
        check_constraints: Whether to validate constraints

    Raises:
        AssertionError: If the field is invalid
    """
    assert field.name, "Field name must not be empty"
    assert isinstance(field.name, str), "Field name must be a string"
    assert field.name.replace(
        "_", ""
    ).isidentifier(), f"Field name '{field.name}' must be a valid identifier"

    # Check that data_type is a valid DataType enum
    from vibe_piper.types import DataType

    valid_types = {
        DataType.STRING,
        DataType.INTEGER,
        DataType.FLOAT,
        DataType.BOOLEAN,
        DataType.DATETIME,
        DataType.DATE,
        DataType.ARRAY,
        DataType.OBJECT,
        DataType.ANY,
    }
    assert field.data_type in valid_types, f"Field '{field.name}' has invalid data_type"

    if check_constraints and field.constraints:
        assert isinstance(
            field.constraints, Mapping
        ), f"Field '{field.name}' constraints must be a mapping"


def assert_data_conforms_to_schema(
    data: Mapping[str, Any] | DataRecord | Sequence[Mapping[str, Any] | DataRecord],
    schema: Schema,
    strict: bool = False,
) -> None:
    """
    Assert that data conforms to a schema.

    Args:
        data: The data to validate (single record, DataRecord, or sequence)
        schema: The schema to validate against
        strict: If True, require that all data fields are in the schema

    Raises:
        AssertionError: If data doesn't conform to the schema
    """
    # Handle single record
    if isinstance(data, DataRecord):
        records = [data.data]
    elif isinstance(data, Mapping):
        records = [data]
    else:
        # Handle sequence of records
        records = [
            record.data if isinstance(record, DataRecord) else record for record in data
        ]

    # Build a field lookup for efficiency
    field_map = {f.name: f for f in schema.fields}

    for i, record in enumerate(records):
        # Check required fields
        for field in schema.fields:
            if field.required and field.name not in record:
                raise AssertionError(
                    f"Record {i}: Required field '{field.name}' is missing"
                )

            # Check nullability
            if (
                field.name in record
                and record[field.name] is None
                and not field.nullable
            ):
                raise AssertionError(
                    f"Record {i}: Field '{field.name}' is not nullable but has None value"
                )

        # In strict mode, check that all record fields are in schema
        if strict:
            for field_name in record:
                if field_name not in field_map:
                    raise AssertionError(
                        f"Record {i}: Field '{field_name}' not in schema"
                    )


def assert_asset_valid(asset: Asset) -> None:
    """
    Assert that an asset is valid.

    Args:
        asset: The asset to validate

    Raises:
        AssertionError: If the asset is invalid
    """
    assert asset.name, "Asset name must not be empty"
    assert isinstance(asset.name, str), "Asset name must be a string"
    assert asset.uri, f"Asset '{asset.name}' URI must not be empty"
    assert isinstance(asset.uri, str), "Asset URI must be a string"

    # Validate schema if present
    if asset.schema is not None:
        assert_schema_valid(asset.schema)

    # Validate operator if present
    if asset.operator is not None:
        assert asset.operator.name, "Asset operator must have a name"
        assert isinstance(asset.operator.name, str), "Operator name must be a string"


def assert_asset_graph_valid(graph: AssetGraph) -> None:
    """
    Assert that an asset graph is valid.

    Args:
        graph: The asset graph to validate

    Raises:
        AssertionError: If the graph is invalid
    """
    assert graph.name, "AssetGraph name must not be empty"

    # Check for duplicate asset names
    asset_names = [a.name for a in graph.assets]
    assert len(asset_names) == len(
        set(asset_names)
    ), f"AssetGraph '{graph.name}' has duplicate asset names"

    # Validate all assets
    for asset in graph.assets:
        assert_asset_valid(asset)

    # Validate all dependencies exist
    asset_name_set = set(asset_names)
    for asset_name, deps in graph.dependencies.items():
        assert (
            asset_name in asset_name_set
        ), f"Asset '{asset_name}' in dependencies but not in assets"
        for dep in deps:
            assert (
                dep in asset_name_set
            ), f"Dependency '{dep}' of asset '{asset_name}' not found in assets"

    # Check for circular dependencies
    assert_no_circular_dependencies(graph)


def assert_no_circular_dependencies(graph: AssetGraph) -> None:
    """
    Assert that an asset graph has no circular dependencies.

    Args:
        graph: The asset graph to check

    Raises:
        AssertionError: If circular dependencies are found
    """
    visited: set[str] = set()
    rec_stack: set[str] = set()

    def dfs(node: str, path: list[str]) -> None:
        if node in rec_stack:
            cycle_start = path.index(node)
            cycle_path = path[cycle_start:] + [node]
            raise AssertionError(
                f"Circular dependency detected: {' -> '.join(cycle_path)}"
            )
        if node in visited:
            return

        visited.add(node)
        rec_stack.add(node)
        path.append(node)

        for dep in graph.dependencies.get(node, ()):
            dfs(dep, path)

        path.pop()
        rec_stack.remove(node)

    for asset in graph.assets:
        if asset.name not in visited:
            dfs(asset.name, [])


def assert_topological_order(
    graph: AssetGraph,
    order: Sequence[str],
) -> None:
    """
    Assert that a topological ordering is valid for the graph.

    Args:
        graph: The asset graph
        order: The claimed topological order

    Raises:
        AssertionError: If the ordering is invalid
    """
    # Check that all assets are in the order
    asset_names = {a.name for a in graph.assets}
    order_set = set(order)
    assert asset_names == order_set, "Topological order doesn't match graph assets"

    # Check that dependencies come before dependents
    position = {name: i for i, name in enumerate(order)}

    for asset_name, deps in graph.dependencies.items():
        for dep in deps:
            assert position[dep] < position[asset_name], (
                f"Dependency '{dep}' should come before '{asset_name}' "
                f"in topological order (dep at {position[dep]}, "
                f"asset at {position[asset_name]})"
            )


def assert_lineage(
    graph: AssetGraph,
    asset_name: str,
    expected_lineage: Sequence[str],
) -> None:
    """
    Assert that an asset has the expected lineage (upstream dependencies).

    Args:
        graph: The asset graph
        asset_name: The asset to check
        expected_lineage: Expected upstream asset names in any order

    Raises:
        AssertionError: If lineage doesn't match
    """
    # Get actual lineage by traversing dependencies
    actual_lineage: set[str] = set()

    def collect_lineage(name: str) -> None:
        for dep in graph.dependencies.get(name, ()):
            actual_lineage.add(dep)
            collect_lineage(dep)

    collect_lineage(asset_name)

    expected_set = set(expected_lineage)
    assert actual_lineage == expected_set, (
        f"Asset '{asset_name}' has lineage {actual_lineage}, "
        f"expected {expected_set}"
    )


def assert_execution_successful(
    result: ExecutionResult | AssetResult,
    check_data: bool = False,
) -> None:
    """
    Assert that an execution result indicates success.

    Args:
        result: The execution result to check
        check_data: If True, also assert that data is present

    Raises:
        AssertionError: If execution was not successful
    """
    if isinstance(result, AssetResult):
        assert (
            result.success
        ), f"Asset '{result.asset_name}' execution failed: {result.error}"
        if check_data:
            assert (
                result.data is not None
            ), f"Asset '{result.asset_name}' has no data despite success=True"
    else:  # ExecutionResult
        assert (
            result.success
        ), f"Execution failed with {len(result.errors)} errors: {result.errors}"
        assert (
            result.assets_failed == 0
        ), f"Execution had {result.assets_failed} failed assets"
        if check_data:
            for asset_name, asset_result in result.asset_results.items():
                if asset_result.success:
                    assert (
                        asset_result.data is not None
                    ), f"Asset '{asset_name}' has no data despite success=True"


def assert_assets_equal(asset1: Asset, asset2: Asset) -> None:
    """
    Assert that two assets are equal.

    Args:
        asset1: First asset
        asset2: Second asset

    Raises:
        AssertionError: If assets are not equal
    """
    assert asset1.name == asset2.name, "Asset names don't match"
    assert asset1.asset_type == asset2.asset_type, "Asset types don't match"
    assert asset1.uri == asset2.uri, "Asset URIs don't match"

    # Compare schemas if both have them
    if asset1.schema is not None and asset2.schema is not None:
        assert_schemas_equal(asset1.schema, asset2.schema)
    elif (asset1.schema is None) != (asset2.schema is None):
        raise AssertionError("One asset has a schema, the other doesn't")


def assert_schemas_equal(schema1: Schema, schema2: Schema) -> None:
    """
    Assert that two schemas are equal.

    Args:
        schema1: First schema
        schema2: Second schema

    Raises:
        AssertionError: If schemas are not equal
    """
    assert schema1.name == schema2.name, "Schema names don't match"
    assert len(schema1.fields) == len(
        schema2.fields
    ), f"Schema field counts don't match: {len(schema1.fields)} vs {len(schema2.fields)}"

    # Compare fields
    field_map2 = {f.name: f for f in schema2.fields}
    for field1 in schema1.fields:
        assert field1.name in field_map2, f"Field '{field1.name}' not in second schema"
        field2 = field_map2[field1.name]

        assert (
            field1.data_type == field2.data_type
        ), f"Field '{field1.name}' has different data_type"
        assert (
            field1.required == field2.required
        ), f"Field '{field1.name}' has different required flag"
        assert (
            field1.nullable == field2.nullable
        ), f"Field '{field1.name}' has different nullable flag"


def assert_execution_results_equal(
    result1: ExecutionResult,
    result2: ExecutionResult,
) -> None:
    """
    Assert that two execution results are equal.

    Args:
        result1: First execution result
        result2: Second execution result

    Raises:
        AssertionError: If results are not equal
    """
    assert result1.success == result2.success, "Success status doesn't match"
    assert (
        result1.assets_executed == result2.assets_executed
    ), "Assets executed count doesn't match"
    assert (
        result1.assets_succeeded == result2.assets_succeeded
    ), "Assets succeeded count doesn't match"
    assert (
        result1.assets_failed == result2.assets_failed
    ), "Assets failed count doesn't match"

    # Compare asset results
    assert set(result1.asset_results.keys()) == set(
        result2.asset_results.keys()
    ), "Asset result keys don't match"

    for asset_name in result1.asset_results:
        asset_result1 = result1.asset_results[asset_name]
        asset_result2 = result2.asset_results[asset_name]

        assert (
            asset_result1.success == asset_result2.success
        ), f"Asset '{asset_name}' success status doesn't match"
        assert (
            asset_result1.asset_name == asset_result2.asset_name
        ), f"Asset name mismatch for '{asset_name}'"
