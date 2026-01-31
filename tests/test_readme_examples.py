"""
Smoke test for README Quick Start example.
Tests that the example compiles and creates a valid AssetGraph.
"""

from pathlib import Path

import pytest

from vibe_piper import (
    ExecutionEngine,
    PipelineContext,
    PipelineDefinitionContext,
    add_field,
    aggregate_group_by,
    filter_field_equals,
)
from vibe_piper.types import DataRecord, DataType, Schema, SchemaField


def test_readme_example_compiles():
    """Test that README Quick Start example compiles against current API."""
    # Define your data pipeline using PipelineDefinitionContext with @asset decorator
    with PipelineDefinitionContext("user_analytics") as pipeline:

        @pipeline.asset()
        def extract_users(ctx: PipelineContext) -> list[dict]:
            """Extract user data from CSV."""
            # Mock implementation for testing
            return [
                {"id": 1, "name": "Alice", "age": 35, "status": "active"},
                {"id": 2, "name": "Bob", "age": 25, "status": "active"},
                {"id": 3, "name": "Charlie", "age": 28, "status": "inactive"},
            ]

        @pipeline.asset(depends_on=["extract_users"])
        def transform_users(extract_users: list[dict], ctx: PipelineContext) -> list[dict]:
            """Transform and filter users."""
            from vibe_piper.operators import map_transform

            # Add a computed field
            with_category = map_transform(
                extract_users,
                add_field("category", lambda x: "premium" if x.get("age", 0) > 30 else "standard"),
            )

            # Filter only active users
            active_users = filter_field_equals(with_category, "status", "active")

            return list(active_users)

        @pipeline.asset(depends_on=["transform_users"])
        def aggregate_by_category(transform_users: list[dict], ctx: PipelineContext) -> list[dict]:
            """Aggregate users by category."""
            return aggregate_group_by(
                transform_users,
                group_by="category",
                aggregations={"count": "count", "avg_age": "avg"},
            )

        @pipeline.asset(depends_on=["aggregate_by_category"])
        def load_results(aggregate_by_category: list[dict], ctx: PipelineContext) -> str:
            """Load results to output CSV."""
            # Mock implementation for testing
            return "mock_output_path.csv"

        # Build the asset graph
        graph = pipeline.build()

        # Verify graph is built correctly
        assert graph.name == "user_analytics"
        assert len(graph.assets) == 4
        assert set(asset.name for asset in graph.assets) == {
            "extract_users",
            "transform_users",
            "aggregate_by_category",
            "load_results",
        }

        # Verify dependencies are correct
        assert "transform_users" in graph.dependencies
        assert "aggregate_by_category" in graph.dependencies
        assert "load_results" in graph.dependencies
        assert graph.dependencies["transform_users"] == ("extract_users",)
        assert graph.dependencies["aggregate_by_category"] == ("transform_users",)
        assert graph.dependencies["load_results"] == ("aggregate_by_category",)


def test_readme_cli_example():
    """Test that README CLI examples reference correct command name."""
    # Just verify the command name is correct
    import subprocess

    result = subprocess.run(
        ["uv", "run", "--", "vibepiper", "--help"], capture_output=True, text=True
    )
    # vibepiper CLI should exist and show help
    assert "vibepiper" in result.stdout or result.returncode == 0
