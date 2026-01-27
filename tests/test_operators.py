"""
Unit tests for built-in operators.

Tests all transformation operators in the operators module.
"""

import pytest

from vibe_piper import (
    DataRecord,
    DataType,
    OperatorType,
    PipelineContext,
    Schema,
    SchemaField,
    add_field,
    aggregate_count,
    aggregate_group_by,
    aggregate_sum,
    custom_operator,
    filter_field_equals,
    filter_field_not_null,
    filter_operator,
    map_field,
    map_transform,
    validate_schema,
)


class TestMapTransform:
    """Tests for map_transform operator."""

    def test_map_transform_basic(self) -> None:
        """Test basic map transformation."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="value", data_type=DataType.INTEGER),),
        )

        def double_value(record: DataRecord, ctx: PipelineContext) -> DataRecord:
            return DataRecord(
                data={"value": record.get("value") * 2},
                schema=record.schema,
            )

        op = map_transform(
            name="double_values",
            transform_fn=double_value,
            description="Double all values",
        )

        assert op.name == "double_values"
        assert op.operator_type == OperatorType.TRANSFORM

        ctx = PipelineContext(pipeline_id="test", run_id="run_1")
        input_data = [
            DataRecord(data={"value": 1}, schema=schema),
            DataRecord(data={"value": 2}, schema=schema),
        ]

        result = op.fn(input_data, ctx)

        assert len(result) == 2
        assert result[0].get("value") == 2
        assert result[1].get("value") == 4


class TestMapField:
    """Tests for map_field operator."""

    def test_map_field_transform(self) -> None:
        """Test transforming a specific field."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="name", data_type=DataType.STRING),),
        )

        op = map_field(
            name="uppercase_name",
            field_name="name",
            transform_fn=str.upper,
            description="Uppercase name field",
        )

        ctx = PipelineContext(pipeline_id="test", run_id="run_1")
        input_data = [
            DataRecord(data={"name": "alice"}, schema=schema),
            DataRecord(data={"name": "bob"}, schema=schema),
        ]

        result = op.fn(input_data, ctx)

        assert len(result) == 2
        assert result[0].get("name") == "ALICE"
        assert result[1].get("name") == "BOB"

    def test_map_field_missing_field(self) -> None:
        """Test that records without the field are unchanged."""
        schema = Schema(
            name="test",
            fields=(
                SchemaField(name="name", data_type=DataType.STRING, required=False),
            ),
        )

        op = map_field(
            name="uppercase_name",
            field_name="name",
            transform_fn=str.upper,
        )

        ctx = PipelineContext(pipeline_id="test", run_id="run_1")
        input_data = [DataRecord(data={"other": "value"}, schema=schema)]

        result = op.fn(input_data, ctx)

        assert len(result) == 1
        assert result[0].get("name") is None
        assert result[0].get("other") == "value"


class TestAddField:
    """Tests for add_field operator."""

    def test_add_field_computed(self) -> None:
        """Test adding a computed field."""
        schema = Schema(
            name="test",
            fields=(
                SchemaField(name="first_name", data_type=DataType.STRING),
                SchemaField(name="last_name", data_type=DataType.STRING),
            ),
        )

        def compute_full_name(record: DataRecord, ctx: PipelineContext) -> str:
            return f"{record.get('first_name')} {record.get('last_name')}"

        op = add_field(
            name="add_full_name",
            field_name="full_name",
            field_type=DataType.STRING,
            value_fn=compute_full_name,
            description="Add full name field",
        )

        ctx = PipelineContext(pipeline_id="test", run_id="run_1")
        input_data = [
            DataRecord(
                data={"first_name": "John", "last_name": "Doe"},
                schema=schema,
            ),
        ]

        result = op.fn(input_data, ctx)

        assert len(result) == 1
        assert result[0].get("full_name") == "John Doe"
        assert result[0].get("first_name") == "John"


class TestFilterOperator:
    """Tests for filter_operator."""

    def test_filter_by_predicate(self) -> None:
        """Test filtering records with a predicate."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="age", data_type=DataType.INTEGER),),
        )

        def is_adult(record: DataRecord, ctx: PipelineContext) -> bool:
            return record.get("age", 0) >= 18

        op = filter_operator(
            name="filter_adults",
            predicate=is_adult,
            description="Keep only adults",
        )

        ctx = PipelineContext(pipeline_id="test", run_id="run_1")
        input_data = [
            DataRecord(data={"age": 15}, schema=schema),
            DataRecord(data={"age": 18}, schema=schema),
            DataRecord(data={"age": 21}, schema=schema),
        ]

        result = op.fn(input_data, ctx)

        assert len(result) == 2
        assert result[0].get("age") == 18
        assert result[1].get("age") == 21


class TestFilterFieldEquals:
    """Tests for filter_field_equals operator."""

    def test_filter_field_equals_match(self) -> None:
        """Test filtering by field equality."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="status", data_type=DataType.STRING),),
        )

        op = filter_field_equals(
            name="filter_active",
            field_name="status",
            value="active",
        )

        ctx = PipelineContext(pipeline_id="test", run_id="run_1")
        input_data = [
            DataRecord(data={"status": "active"}, schema=schema),
            DataRecord(data={"status": "inactive"}, schema=schema),
            DataRecord(data={"status": "active"}, schema=schema),
        ]

        result = op.fn(input_data, ctx)

        assert len(result) == 2
        assert all(r.get("status") == "active" for r in result)


class TestFilterFieldNotNull:
    """Tests for filter_field_not_null operator."""

    def test_filter_field_not_null(self) -> None:
        """Test filtering out null/missing fields."""
        schema = Schema(
            name="test",
            fields=(
                SchemaField(
                    name="email",
                    data_type=DataType.STRING,
                    required=False,
                    nullable=True,
                ),
            ),
        )

        op = filter_field_not_null(
            name="filter_has_email",
            field_name="email",
        )

        ctx = PipelineContext(pipeline_id="test", run_id="run_1")
        input_data = [
            DataRecord(data={"email": "test@example.com"}, schema=schema),
            DataRecord(data={"email": None}, schema=schema),
            DataRecord(data={}, schema=schema),
        ]

        result = op.fn(input_data, ctx)

        assert len(result) == 1
        assert result[0].get("email") == "test@example.com"


class TestAggregateCount:
    """Tests for aggregate_count operator."""

    def test_aggregate_count(self) -> None:
        """Test counting records."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="value", data_type=DataType.INTEGER),),
        )

        op = aggregate_count(name="count_records")

        ctx = PipelineContext(pipeline_id="test", run_id="run_1")
        input_data = [
            DataRecord(data={"value": 1}, schema=schema),
            DataRecord(data={"value": 2}, schema=schema),
            DataRecord(data={"value": 3}, schema=schema),
        ]

        result = op.fn(input_data, ctx)

        assert result == 3


class TestAggregateSum:
    """Tests for aggregate_sum operator."""

    def test_aggregate_sum(self) -> None:
        """Test summing a field."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="amount", data_type=DataType.INTEGER),),
        )

        op = aggregate_sum(name="sum_amount", field_name="amount")

        ctx = PipelineContext(pipeline_id="test", run_id="run_1")
        input_data = [
            DataRecord(data={"amount": 10}, schema=schema),
            DataRecord(data={"amount": 20}, schema=schema),
            DataRecord(data={"amount": 30}, schema=schema),
        ]

        result = op.fn(input_data, ctx)

        assert result == 60


class TestAggregateGroupBy:
    """Tests for aggregate_group_by operator."""

    def test_aggregate_group_by_count(self) -> None:
        """Test grouping and counting."""
        schema = Schema(
            name="test",
            fields=(
                SchemaField(name="category", data_type=DataType.STRING),
                SchemaField(name="value", data_type=DataType.INTEGER),
            ),
        )

        def count_group(records: list[DataRecord]) -> int:
            return len(records)

        op = aggregate_group_by(
            name="group_by_category",
            group_field="category",
            aggregate_fn=count_group,
        )

        ctx = PipelineContext(pipeline_id="test", run_id="run_1")
        input_data = [
            DataRecord(data={"category": "A", "value": 1}, schema=schema),
            DataRecord(data={"category": "B", "value": 2}, schema=schema),
            DataRecord(data={"category": "A", "value": 3}, schema=schema),
        ]

        result = op.fn(input_data, ctx)

        assert isinstance(result, dict)
        assert result["A"] == 2
        assert result["B"] == 1


class TestValidateSchema:
    """Tests for validate_schema operator."""

    def test_validate_schema_pass(self) -> None:
        """Test validation with valid records."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="id", data_type=DataType.INTEGER, required=True),),
        )

        op = validate_schema(name="validate", schema=schema)

        ctx = PipelineContext(pipeline_id="test", run_id="run_1")
        input_data = [
            DataRecord(data={"id": 1}, schema=schema),
            DataRecord(data={"id": 2}, schema=schema),
        ]

        result = op.fn(input_data, ctx)

        assert len(result) == 2

    def test_validate_schema_fail(self) -> None:
        """Test validation with invalid records."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="id", data_type=DataType.INTEGER, required=True),),
        )

        op = validate_schema(name="validate", schema=schema)

        ctx = PipelineContext(pipeline_id="test", run_id="run_1")
        # Create a record with a different schema first (no validation)
        loose_schema = Schema(name="loose", fields=())
        input_data = [
            DataRecord(data={}, schema=loose_schema),  # Missing required field
        ]

        with pytest.raises(ValueError, match="Required field"):
            op.fn(input_data, ctx)


class TestCustomOperator:
    """Tests for custom_operator."""

    def test_custom_operator(self) -> None:
        """Test creating a custom operator."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="value", data_type=DataType.INTEGER),),
        )

        def custom_fn(
            data: list[DataRecord],
            ctx: PipelineContext,
        ) -> list[DataRecord]:
            # Custom logic: reverse the list
            return list(reversed(data))

        op = custom_operator(
            name="reverse_list",
            fn=custom_fn,
            description="Reverse the list of records",
        )

        ctx = PipelineContext(pipeline_id="test", run_id="run_1")
        input_data = [
            DataRecord(data={"value": 1}, schema=schema),
            DataRecord(data={"value": 2}, schema=schema),
            DataRecord(data={"value": 3}, schema=schema),
        ]

        result = op.fn(input_data, ctx)

        assert len(result) == 3
        assert result[0].get("value") == 3
        assert result[2].get("value") == 1


class TestOperatorIntegration:
    """Integration tests for operators used together."""

    def test_pipeline_with_operators(self) -> None:
        """Test using operators in a pipeline."""
        from vibe_piper import Pipeline

        schema = Schema(
            name="test",
            fields=(
                SchemaField(name="value", data_type=DataType.INTEGER),
                SchemaField(name="category", data_type=DataType.STRING),
            ),
        )

        # Filter: only values > 5
        def greater_than_five(record: DataRecord, ctx: PipelineContext) -> bool:
            return record.get("value", 0) > 5

        filter_op = filter_operator(
            name="filter_gt_5",
            predicate=greater_than_five,
        )

        # Map: double the value
        def double_value(record: DataRecord, ctx: PipelineContext) -> DataRecord:
            return DataRecord(
                data={
                    "value": record.get("value") * 2,
                    "category": record.get("category"),
                },
                schema=record.schema,
            )

        map_op = map_transform(name="double", transform_fn=double_value)

        # Aggregate: count records
        count_op = aggregate_count(name="count")

        pipeline = Pipeline(
            name="test_pipeline",
            operators=(filter_op, map_op, count_op),
        )

        ctx = PipelineContext(pipeline_id="test", run_id="run_1")
        input_data = [
            DataRecord(data={"value": 3, "category": "A"}, schema=schema),
            DataRecord(data={"value": 7, "category": "B"}, schema=schema),
            DataRecord(data={"value": 10, "category": "A"}, schema=schema),
        ]

        result = pipeline.execute(input_data, context=ctx)

        # Filter keeps 7 and 10 (2 records)
        # Map doubles them to 14 and 20
        # Count returns 2
        assert result == 2
