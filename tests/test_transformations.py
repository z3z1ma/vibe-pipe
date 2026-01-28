"""
Tests for transformation framework.

Comprehensive tests covering all transformation operations including edge cases.
"""

import pytest
from vibe_piper import DataRecord, DataType, PipelineContext, Schema, SchemaField
from vibe_piper.transformations import (
    Avg,
    Count,
    Cube,
    GroupBy,
    Join,
    JoinType,
    Max,
    Min,
    Pivot,
    Rollup,
    Sum,
    TransformationBuilder,
    Unpivot,
    Window,
    join,
    transform,
    window_function,
)


@pytest.fixture
def customer_schema() -> Schema:
    """Create customer schema."""
    return Schema(
        name="customers",
        fields=(
            SchemaField(name="customer_id", data_type=DataType.INTEGER),
            SchemaField(name="name", data_type=DataType.STRING),
            SchemaField(name="region", data_type=DataType.STRING),
        ),
    )


@pytest.fixture
def order_schema() -> Schema:
    """Create order schema."""
    return Schema(
        name="orders",
        fields=(
            SchemaField(name="order_id", data_type=DataType.INTEGER),
            SchemaField(name="customer_id", data_type=DataType.INTEGER),
            SchemaField(name="amount", data_type=DataType.FLOAT),
        ),
    )


@pytest.fixture
def customers(customer_schema: Schema) -> list[DataRecord]:
    """Create customer records."""
    return [
        DataRecord(data={"customer_id": 1, "name": "Alice", "region": "North"}, schema=customer_schema),
        DataRecord(data={"customer_id": 2, "name": "Bob", "region": "South"}, schema=customer_schema),
        DataRecord(data={"customer_id": 3, "name": "Charlie", "region": "North"}, schema=customer_schema),
    ]


@pytest.fixture
def orders(order_schema: Schema) -> list[DataRecord]:
    """Create order records."""
    return [
        DataRecord(data={"order_id": 101, "customer_id": 1, "amount": 100.0}, schema=order_schema),
        DataRecord(data={"order_id": 102, "customer_id": 1, "amount": 150.0}, schema=order_schema),
        DataRecord(data={"order_id": 103, "customer_id": 2, "amount": 200.0}, schema=order_schema),
    ]


@pytest.fixture
def sales_schema() -> Schema:
    """Create sales schema."""
    return Schema(
        name="sales",
        fields=(
            SchemaField(name="category", data_type=DataType.STRING),
            SchemaField(name="product", data_type=DataType.STRING),
            SchemaField(name="amount", data_type=DataType.FLOAT),
            SchemaField(name="date", data_type=DataType.STRING),
        ),
    )


@pytest.fixture
def sales_data(sales_schema: Schema) -> list[DataRecord]:
    """Create sales records."""
    return [
        DataRecord(
            data={"category": "A", "product": "P1", "amount": 100.0, "date": "2024-01-01"},
            schema=sales_schema,
        ),
        DataRecord(
            data={"category": "A", "product": "P2", "amount": 150.0, "date": "2024-01-02"},
            schema=sales_schema,
        ),
        DataRecord(
            data={"category": "B", "product": "P1", "amount": 200.0, "date": "2024-01-01"},
            schema=sales_schema,
        ),
        DataRecord(
            data={"category": "B", "product": "P2", "amount": 250.0, "date": "2024-01-03"},
            schema=sales_schema,
        ),
    ]


class TestJoinOperators:
    """Tests for join operations."""

    def test_inner_join(self, customers: list[DataRecord], orders: list[DataRecord]) -> None:
        """Test inner join between customers and orders."""
        join_op = Join(
            name="inner_join",
            right_data=orders,
            on="customer_id",
            how=JoinType.INNER,
        )

        result = join_op.transform(customers, ctx=None)

        # Inner join should only include matching records
        assert len(result) == 3  # Customer 1 has 2 orders, customer 2 has 1

    def test_left_join(self, customers: list[DataRecord], orders: list[DataRecord]) -> None:
        """Test left join (all customers preserved)."""
        join_op = Join(
            name="left_join",
            right_data=orders,
            on="customer_id",
            how=JoinType.LEFT,
        )

        result = join_op.transform(customers, ctx=None)

        # Left join should include all customers
        assert len(result) == 4  # Customer 1 (2), Customer 2 (1), Customer 3 (1 with nulls)

    def test_right_join(self, customers: list[DataRecord], orders: list[DataRecord]) -> None:
        """Test right join (all orders preserved)."""
        join_op = Join(
            name="right_join",
            right_data=orders,
            on="customer_id",
            how=JoinType.RIGHT,
        )

        result = join_op.transform(customers, ctx=None)

        # Right join should include all orders
        assert len(result) == 3

    def test_full_outer_join(self, customers: list[DataRecord], orders: list[DataRecord]) -> None:
        """Test full outer join."""
        join_op = Join(
            name="full_join",
            right_data=orders,
            on="customer_id",
            how=JoinType.FULL,
        )

        result = join_op.transform(customers, ctx=None)

        # Full join should include all records
        assert len(result) >= 3

    def test_join_empty_left(self, orders: list[DataRecord]) -> None:
        """Test join with empty left dataset."""
        join_op = Join(
            name="empty_left",
            right_data=orders,
            on="customer_id",
            how=JoinType.INNER,
        )

        result = join_op.transform([], ctx=None)

        assert len(result) == 0

    def test_join_empty_right(self, customers: list[DataRecord]) -> None:
        """Test join with empty right dataset."""
        join_op = Join(
            name="empty_right",
            right_data=[],
            on="customer_id",
            how=JoinType.LEFT,
        )

        result = join_op.transform(customers, ctx=None)

        # Left join with empty right should return all left records with nulls
        assert len(result) == len(customers)

    def test_join_missing_column(self, customers: list[DataRecord], orders: list[DataRecord]) -> None:
        """Test join with missing column raises error."""
        join_op = Join(
            name="missing_column",
            right_data=orders,
            on="nonexistent_column",
            how=JoinType.INNER,
        )

        with pytest.raises(ValueError, match="Join column.*not found"):
            join_op.transform(customers, ctx=None)

    def test_convenience_join_function(
        self,
        customers: list[DataRecord],
        orders: list[DataRecord],
    ) -> None:
        """Test convenience join function."""
        result = join(customers, orders, on="customer_id", how="left")

        assert len(result) >= 3


class TestAggregationOperators:
    """Tests for aggregation operations."""

    def test_groupby_with_sum(self, sales_data: list[DataRecord]) -> None:
        """Test groupby with sum aggregation."""
        groupby_op = GroupBy(
            name="groupby_category",
            group_by=["category"],
            aggregations=[Sum(column="amount", alias="total")],
        )

        result = groupby_op.transform(sales_data, ctx=None)

        # Should have 2 groups (A and B)
        assert len(result) == 2

        # Check totals
        category_totals = {r.get("category"): r.get("total") for r in result}
        assert category_totals.get("A") == 250.0
        assert category_totals.get("B") == 450.0

    def test_groupby_multiple_aggregations(self, sales_data: list[DataRecord]) -> None:
        """Test groupby with multiple aggregations."""
        groupby_op = GroupBy(
            name="multiple_aggs",
            group_by=["category"],
            aggregations=[
                Sum(column="amount", alias="total"),
                Count(column="product", alias="count"),
                Avg(column="amount", alias="average"),
            ],
        )

        result = groupby_op.transform(sales_data, ctx=None)

        assert len(result) == 2

        # Check first record has all aggregations
        first = result[0]
        assert "total" in first.data
        assert "count" in first.data
        assert "average" in first.data

    def test_groupby_missing_column(self, sales_data: list[DataRecord]) -> None:
        """Test groupby with missing column raises error."""
        groupby_op = GroupBy(
            name="missing_column",
            group_by=["nonexistent"],
            aggregations=[Sum(column="amount", alias="total")],
        )

        with pytest.raises(ValueError, match="Group column.*not found"):
            groupby_op.transform(sales_data, ctx=None)

    def test_rollup(self, sales_data: list[DataRecord]) -> None:
        """Test rollup aggregation."""
        rollup_op = Rollup(
            name="sales_rollup",
            group_by=["category", "product"],
            aggregations=[Sum(column="amount", alias="total")],
        )

        result = rollup_op.transform(sales_data, ctx=None)

        # Should have grand total + category totals + detail rows
        assert len(result) > 0

    def test_cube(self, sales_data: list[DataRecord]) -> None:
        """Test cube aggregation."""
        cube_op = Cube(
            name="sales_cube",
            group_by=["category", "product"],
            aggregations=[Sum(column="amount", alias="total")],
        )

        result = cube_op.transform(sales_data, ctx=None)

        # Cube should create all combinations
        assert len(result) > 0


class TestWindowFunctions:
    """Tests for window function operations."""

    def test_row_number(self, sales_data: list[DataRecord]) -> None:
        """Test row_number window function."""
        row_num = window_function("row_number", alias="row_num")
        window_op = Window(
            name="row_num",
            functions=[row_num],
            order_by=["amount desc"],
        )

        result = window_op.transform(sales_data, ctx=None)

        assert len(result) == len(sales_data)
        assert "row_num" in result[0].data
        assert result[0].get("row_num") == 1

    def test_rank(self, sales_data: list[DataRecord]) -> None:
        """Test rank window function."""
        rank = window_function("rank", alias="rank")
        window_op = Window(
            name="rank_by_amount",
            functions=[rank],
            order_by=["amount desc"],
        )

        result = window_op.transform(sales_data, ctx=None)

        assert len(result) == len(sales_data)
        assert "rank" in result[0].data

    def test_lag(self, sales_data: list[DataRecord]) -> None:
        """Test lag window function."""
        lag = window_function("lag", column="amount", offset=1, alias="prev_amount")
        window_op = Window(
            name="lag_amount",
            functions=[lag],
            order_by=["amount"],
        )

        result = window_op.transform(sales_data, ctx=None)

        assert len(result) == len(sales_data)
        assert "prev_amount" in result[0].data
        # First row should have null/None for lag
        assert result[0].get("prev_amount") is None

    def test_lead(self, sales_data: list[DataRecord]) -> None:
        """Test lead window function."""
        lead = window_function("lead", column="amount", offset=1, alias="next_amount")
        window_op = Window(
            name="lead_amount",
            functions=[lead],
            order_by=["amount"],
        )

        result = window_op.transform(sales_data, ctx=None)

        assert len(result) == len(sales_data)
        assert "next_amount" in result[0].data

    def test_partition_by(self, sales_data: list[DataRecord]) -> None:
        """Test window function with partition by."""
        window_op = Window(
            name="partition_by_category",
            functions=[RowNumber(alias="row_num")],
            partition_by=["category"],
            order_by=["amount desc"],
        )

        result = window_op.transform(sales_data, ctx=None)

        assert len(result) == len(sales_data)
        # Each partition should start at row_num = 1
        category_row_nums = {}
        for r in result:
            cat = r.get("category")
            if cat not in category_row_nums:
                category_row_nums[cat] = r.get("row_num")
                assert r.get("row_num") == 1

    def test_window_function_convenience(self) -> None:
        """Test window_function convenience function."""
        lag_func = window_function("lag", column="value", offset=1)

        assert isinstance(lag_func, Lag)
        assert lag_func.column == "value"
        assert lag_func.offset == 1


class TestPivotUnpivot:
    """Tests for pivot and unpivot operations."""

    def test_pivot(self, sales_data: list[DataRecord]) -> None:
        """Test pivot operation."""
        pivot_op = Pivot(
            name="pivot_by_category",
            index="product",
            columns="category",
            values="amount",
            aggfunc="sum",
        )

        result = pivot_op.transform(sales_data, ctx=None)

        assert len(result) > 0
        # Should have product as index and categories as columns
        assert "product" in result[0].data

    def test_unpivot(self, sales_data: list[DataRecord]) -> None:
        """Test unpivot operation."""
        # First create pivoted data
        pivot_op = Pivot(
            name="pivot",
            index="product",
            columns="category",
            values="amount",
            aggfunc="sum",
        )

        pivoted = pivot_op.transform(sales_data, ctx=None)

        # Now unpivot
        unpivot_op = Unpivot(
            name="unpivot",
            id_vars=["product"],
            value_vars=["A", "B"],
            var_name="category",
            value_name="amount",
        )

        result = unpivot_op.transform(pivoted, ctx=None)

        assert len(result) > 0
        assert "category" in result[0].data
        assert "amount" in result[0].data


class TestTransformationBuilder:
    """Tests for transformation builder API."""

    def test_builder_filter(self, customers: list[DataRecord]) -> None:
        """Test builder with filter."""
        builder = TransformationBuilder(customers)

        result = (
            builder.filter(lambda r: r.get("region") == "North").filter(
                lambda r: r.get("name") != "Charlie"
            )
        ).execute()

        # Should only have Alice
        assert len(result) == 1
        assert result[0].get("name") == "Alice"

    def test_builder_filter_shortcut(self, customers: list[DataRecord]) -> None:
        """Test builder with filter shortcut."""
        builder = TransformationBuilder(customers)

        result = builder.filter("equals", field="region", value="North").execute()

        assert len(result) == 2

    def test_builder_map(self, customers: list[DataRecord]) -> None:
        """Test builder with map."""
        builder = TransformationBuilder(customers)

        def uppercase_name(record: DataRecord) -> DataRecord:
            return DataRecord(
                data={**record.data, "name": record.get("name").upper()},
                schema=record.schema,
            )

        result = builder.map(uppercase_name).execute()

        assert result[0].get("name") == "ALICE"
        assert result[1].get("name") == "BOB"

    def test_builder_groupby(self, sales_data: list[DataRecord]) -> None:
        """Test builder with groupby."""
        builder = TransformationBuilder(sales_data)

        result = builder.groupby(["category"], [Sum("amount", "total")]).execute()

        assert len(result) == 2

    def test_builder_composition(self, sales_data: list[DataRecord]) -> None:
        """Test builder with multiple transformations."""
        builder = TransformationBuilder(sales_data)

        result = (
            builder.filter(lambda r: r.get("amount") > 100)
            .map(lambda r: DataRecord(data={**r.data, "high_value": True}, schema=r.schema))
            .groupby(["category"], [Sum("amount", "total")])
        ).execute()

        assert len(result) > 0

    def test_transform_convenience(self, customers: list[DataRecord]) -> None:
        """Test transform convenience function."""
        result = (
            transform(customers).filter(lambda r: r.get("region") == "North").execute()
        )

        assert len(result) == 2


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_dataset(self, sales_schema: Schema) -> None:
        """Test transformations with empty dataset."""
        empty_data: list[DataRecord] = []

        # GroupBy should handle empty data
        groupby_op = GroupBy(
            name="empty_groupby",
            group_by=["category"],
            aggregations=[Sum(column="amount", alias="total")],
        )

        result = groupby_op.transform(empty_data, ctx=None)

        assert len(result) == 0

    def test_single_record(self, sales_schema: Schema) -> None:
        """Test transformations with single record."""
        single_record = [
            DataRecord(
                data={"category": "A", "product": "P1", "amount": 100.0, "date": "2024-01-01"},
                schema=sales_schema,
            )
        ]

        groupby_op = GroupBy(
            name="single_groupby",
            group_by=["category"],
            aggregations=[Sum(column="amount", alias="total")],
        )

        result = groupby_op.transform(single_record, ctx=None)

        assert len(result) == 1
        assert result[0].get("total") == 100.0

    def test_null_values(self, sales_schema: Schema) -> None:
        """Test transformations with null values."""
        data_with_nulls = [
            DataRecord(
                data={"category": "A", "product": "P1", "amount": None, "date": "2024-01-01"},
                schema=sales_schema,
            ),
            DataRecord(
                data={"category": "A", "product": "P2", "amount": 150.0, "date": "2024-01-02"},
                schema=sales_schema,
            ),
        ]

        groupby_op = GroupBy(
            name="null_groupby",
            group_by=["category"],
            aggregations=[Sum(column="amount", alias="total")],
        )

        result = groupby_op.transform(data_with_nulls, ctx=None)

        assert len(result) == 1
