"""Testing helpers and utilities for vibe_piper."""

from tests.helpers.assertions import (
    assert_asset_graph_valid,
    assert_asset_valid,
    assert_data_conforms_to_schema,
    assert_execution_successful,
    assert_lineage,
    assert_no_circular_dependencies,
    assert_schema_valid,
    assert_topological_order,
)
from tests.helpers.comparators import (
    assert_assets_equal,
    assert_execution_results_equal,
    assert_schemas_equal,
)
from tests.helpers.factories import (
    make_asset,
    make_asset_graph,
    make_data_record,
    make_diamond_asset_graph,
    make_linear_asset_graph,
    make_operator,
    make_pipeline,
    make_pipeline_context,
    make_product_schema,
    make_schema,
    make_user_schema,
)
from tests.helpers.snapshots import (
    SnapshotMismatchError,
    assert_match_snapshot,
    assert_matches_snapshot,
)

__all__ = [
    # Assertions
    "assert_asset_valid",
    "assert_asset_graph_valid",
    "assert_data_conforms_to_schema",
    "assert_execution_successful",
    "assert_lineage",
    "assert_no_circular_dependencies",
    "assert_schema_valid",
    "assert_topological_order",
    # Comparators
    "assert_assets_equal",
    "assert_schemas_equal",
    "assert_execution_results_equal",
    # Factories
    "make_asset",
    "make_asset_graph",
    "make_data_record",
    "make_diamond_asset_graph",
    "make_linear_asset_graph",
    "make_operator",
    "make_pipeline",
    "make_pipeline_context",
    "make_product_schema",
    "make_schema",
    "make_user_schema",
    # Snapshots
    "assert_match_snapshot",
    "assert_matches_snapshot",
    "SnapshotMismatchError",
]
