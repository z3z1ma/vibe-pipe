"""
Tests for fluent pipeline API.

Tests for the TransformationBuilder fluent API with method chaining
and pipe() pattern.
"""

import pytest

from vibe_piper import DataRecord, DataType, PipelineContext, Schema, SchemaField
from vibe_piper.transformations import (
    Avg,
    Count,
    Sum,
    TransformationBuilder,
    compute_field,
    extract_fields,
    filter_by_field,
    filter_rows,
    transform,
)


@pytest.fixture
def sample_schema() -> Schema:
    """Create sample schema."""
    return Schema(
        name="sample",
        fields=(
            SchemaField(name="id", data_type=DataType.INTEGER),
            SchemaField(name="name", data_type=DataType.STRING),
            SchemaField(name="value", data_type=DataType.FLOAT),
            SchemaField(name="active", data_type=DataType.BOOLEAN),
            SchemaField(name="category", data_type=DataType.STRING),
        ),
    )


@pytest.fixture
def sample_data(sample_schema: Schema) -> list[DataRecord]:
    """Create sample test data."""
    return [
        DataRecord(
            data={"id": 1, "name": "Alice", "value": 100.0, "active": True, "category": "A"},
            schema=sample_schema,
        ),
        DataRecord(
            data={"id": 2, "name": "Bob", "value": 150.0, "active": True, "category": "B"},
            schema=sample_schema,
        ),
        DataRecord(
            data={"id": 3, "name": "Charlie", "value": 200.0, "active": False, "category": "A"},
            schema=sample_schema,
        ),
        DataRecord(
            data={"id": 4, "name": "Diana", "value": 250.0, "active": True, "category": "B"},
            schema=sample_schema,
        ),
    ]


class TestTransformationBuilderBasics:
    """Test basic TransformationBuilder functionality."""

    def test_builder_initialization(self, sample_data: list[DataRecord]) -> None:
        """Test builder initializes correctly."""
        builder = TransformationBuilder(sample_data)
        assert builder.data == sample_data
        assert len(builder.transformations) == 0

    def test_builder_with_context(self, sample_data: list[DataRecord]) -> None:
        """Test builder with custom context."""
        context = PipelineContext(pipeline_id="test", run_id="run1")
        builder = TransformationBuilder(sample_data, context)
        assert builder.context.pipeline_id == "test"

    def test_transform_convenience_function(self, sample_data: list[DataRecord]) -> None:
        """Test transform() convenience function."""
        builder = transform(sample_data)
        assert builder.data == sample_data
        assert isinstance(builder, TransformationBuilder)


class TestFilterTransformations:
    """Test filter transformations."""

    def test_filter_with_callable(self, sample_data: list[DataRecord]) -> None:
        """Test filter with callable predicate."""
        result = TransformationBuilder(sample_data).filter(lambda r: r.get("active")).execute()

        assert len(result) == 3
        assert all(r.get("active") for r in result)

    def test_filter_with_equals_shortcut(self, sample_data: list[DataRecord]) -> None:
        """Test filter with equals shortcut."""
        result = (
            TransformationBuilder(sample_data)
            .filter("equals", field="active", value=True)
            .execute()
        )

        assert len(result) == 3
        assert all(r.get("active") for r in result)

    def test_filter_with_not_null_shortcut(self, sample_data: list[DataRecord]) -> None:
        """Test filter with not_null shortcut."""
        result = TransformationBuilder(sample_data).filter("not_null", field="name").execute()

        assert len(result) == len(sample_data)

    def test_filter_chaining(self, sample_data: list[DataRecord]) -> None:
        """Test chaining multiple filters."""
        result = (
            TransformationBuilder(sample_data)
            .filter(lambda r: r.get("active"))
            .filter(lambda r: r.get("value") > 100.0)
            .execute()
        )

        assert len(result) == 2
        assert all(r.get("active") and r.get("value") > 100.0 for r in result)


class TestMapTransformations:
    """Test map transformations."""

    def test_map_with_transform_function(self, sample_data: list[DataRecord]) -> None:
        """Test map with transform function."""

        def uppercase_name(record: DataRecord) -> DataRecord:
            return DataRecord(
                data={**record.data, "name": record.get("name").upper()},
                schema=record.schema,
                metadata=record.metadata,
            )

        result = TransformationBuilder(sample_data).map(uppercase_name).execute()

        assert all(r.get("name").isupper() for r in result)

    def test_map_chaining(self, sample_data: list[DataRecord]) -> None:
        """Test chaining multiple map operations."""

        def add_suffix(record: DataRecord) -> DataRecord:
            return DataRecord(
                data={**record.data, "name": f"{record.get('name')}_test"},
                schema=record.schema,
                metadata=record.metadata,
            )

        result = (
            TransformationBuilder(sample_data)
            .map(
                lambda r: DataRecord(
                    data={**r.data, "name": r.get("name").upper()},
                    schema=r.schema,
                    metadata=r.metadata,
                )
            )
            .map(add_suffix)
            .execute()
        )

        assert all("_test" in r.get("name") for r in result)


class TestCustomTransformations:
    """Test custom transformations."""

    def test_custom_transformation(self, sample_data: list[DataRecord]) -> None:
        """Test adding custom transformation."""

        def double_values(data: list[DataRecord]) -> list[DataRecord]:
            return [
                DataRecord(
                    data={**r.data, "value": r.get("value") * 2},
                    schema=r.schema,
                    metadata=r.metadata,
                )
                for r in data
            ]

        result = TransformationBuilder(sample_data).custom(double_values).execute()

        assert all(
            r.get("value") == original.get("value") * 2 for r, original in zip(result, sample_data)
        )


class TestPipeTransformation:
    """Test pipe() method for fluent API."""

    def test_pipe_with_extract_fields(self, sample_data: list[DataRecord]) -> None:
        """Test pipe() with extract_fields transformation."""
        nested_data = [
            DataRecord(
                data={"id": 1, "user": {"name": "Alice", "email": "alice@example.com"}},
                schema=sample_data[0].schema,
            ),
            DataRecord(
                data={"id": 2, "user": {"name": "Bob", "email": "bob@example.com"}},
                schema=sample_data[0].schema,
            ),
        ]

        result = (
            transform(nested_data)
            .pipe(extract_fields({"user_name": "user.name", "user_email": "user.email"}))
            .execute()
        )

        assert len(result) == 2
        assert result[0].get("user_name") == "Alice"
        assert result[0].get("user_email") == "alice@example.com"

    def test_pipe_with_filter_rows(self, sample_data: list[DataRecord]) -> None:
        """Test pipe() with filter_rows transformation."""
        result = transform(sample_data).pipe(filter_rows(lambda r: r.get("active"))).execute()

        assert len(result) == 3
        assert all(r.get("active") for r in result)

    def test_pipe_with_compute_field(self, sample_data: list[DataRecord]) -> None:
        """Test pipe() with compute_field transformation."""
        result = (
            transform(sample_data)
            .pipe(
                compute_field(
                    "category", lambda r: "premium" if r.get("value") > 150 else "standard"
                )
            )
            .execute()
        )

        assert "category" in result[0].data
        assert result[0].get("category") == "standard"  # value=100
        assert result[1].get("category") == "premium"  # value=150
        assert result[3].get("category") == "premium"  # value=250

    def test_pipe_with_filter_by_field(self, sample_data: list[DataRecord]) -> None:
        """Test pipe() with filter_by_field transformation."""
        result = transform(sample_data).pipe(filter_by_field("active", True)).execute()

        assert len(result) == 3
        assert all(r.get("active") for r in result)

    def test_pipe_chaining(self, sample_data: list[DataRecord]) -> None:
        """Test chaining multiple pipe() operations."""
        result = (
            transform(sample_data)
            .pipe(filter_rows(lambda r: r.get("active")))
            .pipe(compute_field("doubled", lambda r: r.get("value") * 2))
            .pipe(filter_by_field("doubled", 200.0, operator="gte"))
            .execute()
        )

        assert len(result) == 2
        assert all(r.get("doubled") >= 200 for r in result)


class TestMethodChaining:
    """Test traditional method chaining."""

    def test_method_chain_filter_map(self, sample_data: list[DataRecord]) -> None:
        """Test method chaining with filter and map."""
        result = (
            TransformationBuilder(sample_data)
            .filter(lambda r: r.get("active"))
            .map(
                lambda r: DataRecord(
                    data={**r.data, "name": r.get("name").upper()},
                    schema=r.schema,
                    metadata=r.metadata,
                )
            )
            .execute()
        )

        assert len(result) == 3
        assert all(r.get("name").isupper() for r in result)

    def test_method_chain_multiple_filters(self, sample_data: list[DataRecord]) -> None:
        """Test method chaining with multiple filters."""
        result = (
            TransformationBuilder(sample_data)
            .filter(lambda r: r.get("active"))
            .filter(lambda r: r.get("value") > 100.0)
            .filter(lambda r: r.get("category") == "B")
            .execute()
        )

        assert len(result) == 1
        assert result[0].get("name") == "Diana"


class TestMixedPatterns:
    """Test mixing pipe() and method chaining."""

    def test_pipe_and_method_mix(self, sample_data: list[DataRecord]) -> None:
        """Test mixing pipe() and method chaining."""
        result = (
            transform(sample_data)
            .pipe(filter_rows(lambda r: r.get("active")))
            .filter(lambda r: r.get("value") > 100.0)
            .pipe(
                compute_field(
                    "category", lambda r: "premium" if r.get("value") > 200 else "standard"
                )
            )
            .execute()
        )

        assert len(result) == 2
        assert all(r.get("active") and r.get("value") > 100 for r in result)


class TestExecuteBehavior:
    """Test execute() method behavior."""

    def test_execute_empty_builder(self, sample_data: list[DataRecord]) -> None:
        """Test execute on builder with no transformations."""
        result = TransformationBuilder(sample_data).execute()
        assert result == sample_data

    def test_execute_with_transformations(self, sample_data: list[DataRecord]) -> None:
        """Test execute with transformations."""
        builder = TransformationBuilder(sample_data)
        builder.filter(lambda r: r.get("active"))
        result = builder.execute()

        assert len(result) == 3

    def test_multiple_execute_calls(self, sample_data: list[DataRecord]) -> None:
        """Test multiple execute calls don't re-apply transformations."""
        builder = TransformationBuilder(sample_data)
        builder.filter(lambda r: r.get("active"))

        result1 = builder.execute()
        result2 = builder.execute()

        assert result1 == result2


class TestIntegrationWithExistingTransforms:
    """Test integration with existing transformations."""

    def test_pipe_with_aggregation(self, sample_data: list[DataRecord]) -> None:
        """Test pipe() with groupby aggregation."""
        result = transform(sample_data).groupby(["category"], [Sum("value"), Count("id")]).execute()

        assert len(result) == 2
        categories = {r.get("category") for r in result}
        assert categories == {"A", "B"}

    def test_method_chain_with_aggregation(self, sample_data: list[DataRecord]) -> None:
        """Test method chaining with groupby aggregation."""
        result = (
            TransformationBuilder(sample_data)
            .filter(lambda r: r.get("active"))
            .groupby(["category"], [Avg("value")])
            .execute()
        )

        assert len(result) == 2  # A and B

    def test_pipe_and_method_aggregation(self, sample_data: list[DataRecord]) -> None:
        """Test mixing pipe() and method chaining with aggregation."""
        result = (
            transform(sample_data)
            .pipe(filter_rows(lambda r: r.get("active")))
            .groupby(["category"], [Sum("value"), Count("id")])
            .execute()
        )

        assert len(result) == 2
