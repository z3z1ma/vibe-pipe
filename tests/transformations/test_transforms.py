"""
Tests for built-in transformation functions.

Tests for extract_fields, map_field, compute_field, filter_rows,
enrich_from_lookup, rename_fields, drop_fields, select_fields, cast_field.
"""

import pytest

from vibe_piper import DataRecord, DataType, Schema, SchemaField
from vibe_piper.transformations import (
    cast_field,
    compute_field,
    compute_field_from_expression,
    drop_fields,
    enrich_from_lookup,
    extract_fields,
    extract_nested_value,
    filter_by_field,
    filter_rows,
    map_field,
    rename_fields,
    select_fields,
)


@pytest.fixture
def sample_schema() -> Schema:
    """Create sample schema."""
    return Schema(
        name="sample",
        fields=(
            SchemaField(name="id", data_type=DataType.INTEGER),
            SchemaField(name="name", data_type=DataType.STRING),
            SchemaField(name="email", data_type=DataType.STRING),
            SchemaField(name="age", data_type=DataType.INTEGER),
            SchemaField(name="status", data_type=DataType.STRING),
        ),
    )


@pytest.fixture
def nested_schema() -> Schema:
    """Create schema with nested fields."""
    return Schema(
        name="nested",
        fields=(
            SchemaField(name="id", data_type=DataType.INTEGER),
            SchemaField(name="user", data_type=DataType.OBJECT),
            SchemaField(name="address", data_type=DataType.OBJECT),
        ),
    )


@pytest.fixture
def nested_data(nested_schema: Schema) -> list[DataRecord]:
    """Create nested test data."""
    return [
        DataRecord(
            data={
                "id": 1,
                "user": {"name": "Alice", "email": "alice@example.com"},
                "address": {"city": "SF", "state": "CA"},
            },
            schema=nested_schema,
        ),
        DataRecord(
            data={
                "id": 2,
                "user": {"name": "Bob", "email": "bob@example.com"},
                "address": {"city": "NYC", "state": "NY"},
            },
            schema=nested_schema,
        ),
    ]


@pytest.fixture
def sample_data(sample_schema: Schema) -> list[DataRecord]:
    """Create sample test data."""
    return [
        DataRecord(
            data={"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30, "status": "A"},
            schema=sample_schema,
        ),
        DataRecord(
            data={"id": 2, "name": "Bob", "email": "bob@example.com", "age": 25, "status": "I"},
            schema=sample_schema,
        ),
        DataRecord(
            data={
                "id": 3,
                "name": "Charlie",
                "email": "charlie@example.com",
                "age": 35,
                "status": "A",
            },
            schema=sample_schema,
        ),
        DataRecord(
            data={"id": 4, "name": "Diana", "email": "diana@example.com", "age": 28, "status": "P"},
            schema=sample_schema,
        ),
    ]


class TestExtractNestedValue:
    """Test extract_nested_value helper function."""

    def test_extract_single_level(self, nested_schema: Schema) -> None:
        """Test extracting from single level."""
        data = {"name": "Alice"}
        result = extract_nested_value(data, "name")
        assert result == "Alice"

    def test_extract_nested_level(self) -> None:
        """Test extracting from nested structure."""
        data = {"user": {"name": "Alice", "email": "alice@example.com"}}
        result = extract_nested_value(data, "user.name")
        assert result == "Alice"

    def test_extract_deeply_nested(self) -> None:
        """Test extracting from deeply nested structure."""
        data = {"user": {"profile": {"settings": {"theme": "dark"}}}}
        result = extract_nested_value(data, "user.profile.settings.theme")
        assert result == "dark"

    def test_extract_missing_field(self) -> None:
        """Test extracting missing field returns None."""
        data = {"user": {"name": "Alice"}}
        result = extract_nested_value(data, "user.email")
        assert result is None

    def test_extract_from_non_dict(self) -> None:
        """Test extracting from non-dict returns None."""
        data = {"user": "Alice"}
        result = extract_nested_value(data, "user.name")
        assert result is None


class TestExtractFields:
    """Test extract_fields transformation."""

    def test_extract_nested_fields(self, nested_data: list[DataRecord]) -> None:
        """Test extracting nested fields."""
        transform = extract_fields({"user_name": "user.name", "city": "address.city"})
        result = transform(nested_data)

        assert len(result) == 2
        assert result[0].get("user_name") == "Alice"
        assert result[0].get("city") == "SF"
        assert result[1].get("user_name") == "Bob"
        assert result[1].get("city") == "NYC"

    def test_extract_preserves_original_fields(self, nested_data: list[DataRecord]) -> None:
        """Test that extract_fields preserves original fields."""
        transform = extract_fields({"user_name": "user.name"})
        result = transform(nested_data)

        assert "id" in result[0].data
        assert "user" in result[0].data  # Original nested structure preserved
        assert "user_name" in result[0].data  # New extracted field added

    def test_extract_multiple_nested_fields(self, nested_data: list[DataRecord]) -> None:
        """Test extracting multiple nested fields."""
        transform = extract_fields(
            {
                "user_name": "user.name",
                "user_email": "user.email",
                "city": "address.city",
                "state": "address.state",
            }
        )
        result = transform(nested_data)

        assert result[0].get("user_name") == "Alice"
        assert result[0].get("user_email") == "alice@example.com"
        assert result[0].get("city") == "SF"
        assert result[0].get("state") == "CA"


class TestMapField:
    """Test map_field transformation."""

    def test_map_field_values(self, sample_data: list[DataRecord]) -> None:
        """Test mapping field values."""
        transform = map_field("status", {"A": "Active", "I": "Inactive", "P": "Pending"})
        result = transform(sample_data)

        assert result[0].get("status") == "Active"
        assert result[1].get("status") == "Inactive"
        assert result[2].get("status") == "Active"
        assert result[3].get("status") == "Pending"

    def test_map_field_with_default(self, sample_data: list[DataRecord]) -> None:
        """Test mapping field with default value."""
        transform = map_field("status", {"A": "Active"}, default="Unknown")
        result = transform(sample_data)

        assert result[0].get("status") == "Active"
        assert result[1].get("status") == "Unknown"
        assert result[3].get("status") == "Unknown"

    def test_map_field_preserves_unmapped_without_default(
        self, sample_data: list[DataRecord]
    ) -> None:
        """Test that unmapped values are preserved without default."""
        transform = map_field("status", {"A": "Active"})
        result = transform(sample_data)

        assert result[0].get("status") == "Active"
        assert result[1].get("status") == "I"  # Preserved original value


class TestComputeField:
    """Test compute_field transformation."""

    def test_compute_field_simple(self, sample_data: list[DataRecord]) -> None:
        """Test computing a simple field."""
        transform = compute_field("is_adult", lambda r: r.get("age") >= 18)
        result = transform(sample_data)

        assert "is_adult" in result[0].data
        assert all(r.get("is_adult") for r in result)  # All are adults

    def test_compute_field_conditional(self, sample_data: list[DataRecord]) -> None:
        """Test computing a conditional field."""
        transform = compute_field(
            "age_group", lambda r: "senior" if r.get("age") >= 30 else "junior"
        )
        result = transform(sample_data)

        assert result[0].get("age_group") == "senior"
        assert result[1].get("age_group") == "junior"
        assert result[2].get("age_group") == "senior"

    def test_compute_field_derived_value(self, sample_data: list[DataRecord]) -> None:
        """Test computing a field from other fields."""
        transform = compute_field("name_upper", lambda r: r.get("name").upper())
        result = transform(sample_data)

        assert result[0].get("name_upper") == "ALICE"
        assert result[1].get("name_upper") == "BOB"


class TestComputeFieldFromExpression:
    """Test compute_field_from_expression transformation."""

    def test_compute_from_arithmetic_expression(self, sample_data: list[DataRecord]) -> None:
        """Test computing field from arithmetic expression."""
        transform = compute_field_from_expression("age_doubled", "age * 2")
        result = transform(sample_data)

        assert result[0].get("age_doubled") == 60
        assert result[1].get("age_doubled") == 50

    def test_compute_from_complex_expression(self, sample_data: list[DataRecord]) -> None:
        """Test computing field from complex expression."""
        transform = compute_field_from_expression("age_plus_ten", "age + 10")
        result = transform(sample_data)

        assert result[0].get("age_plus_ten") == 40
        assert result[1].get("age_plus_ten") == 35

    def test_compute_from_missing_field(self, sample_data: list[DataRecord]) -> None:
        """Test computing field with missing field results in None."""
        transform = compute_field_from_expression("computed", "missing_field * 2")
        result = transform(sample_data)

        assert result[0].get("computed") is None


class TestFilterRows:
    """Test filter_rows transformation."""

    def test_filter_with_predicate(self, sample_data: list[DataRecord]) -> None:
        """Test filtering with predicate."""
        transform = filter_rows(lambda r: r.get("age") >= 30)
        result = transform(sample_data)

        assert len(result) == 2
        assert all(r.get("age") >= 30 for r in result)

    def test_filter_with_complex_predicate(self, sample_data: list[DataRecord]) -> None:
        """Test filtering with complex predicate."""
        transform = filter_rows(lambda r: r.get("age") >= 25 and r.get("status") == "A")
        result = transform(sample_data)

        assert len(result) == 2
        assert result[0].get("name") == "Alice"
        assert result[1].get("name") == "Charlie"


class TestFilterByField:
    """Test filter_by_field transformation."""

    def test_filter_equals(self, sample_data: list[DataRecord]) -> None:
        """Test filtering with equals operator."""
        transform = filter_by_field("status", "A")
        result = transform(sample_data)

        assert len(result) == 2
        assert all(r.get("status") == "A" for r in result)

    def test_filter_not_equals(self, sample_data: list[DataRecord]) -> None:
        """Test filtering with not equals operator."""
        transform = filter_by_field("status", "A", operator="ne")
        result = transform(sample_data)

        assert len(result) == 2
        assert all(r.get("status") != "A" for r in result)

    def test_filter_greater_than(self, sample_data: list[DataRecord]) -> None:
        """Test filtering with greater than operator."""
        transform = filter_by_field("age", 30, operator="gt")
        result = transform(sample_data)

        assert len(result) == 1
        assert result[0].get("name") == "Charlie"

    def test_filter_less_than(self, sample_data: list[DataRecord]) -> None:
        """Test filtering with less than operator."""
        transform = filter_by_field("age", 30, operator="lt")
        result = transform(sample_data)

        assert len(result) == 3
        assert all(r.get("age") < 30 for r in result)

    def test_filter_in(self, sample_data: list[DataRecord]) -> None:
        """Test filtering with in operator."""
        transform = filter_by_field("status", ["A", "P"], operator="in")
        result = transform(sample_data)

        assert len(result) == 3
        assert all(r.get("status") in ["A", "P"] for r in result)

    def test_filter_is_null(self, sample_data: list[DataRecord]) -> None:
        """Test filtering for null values."""
        sample_with_null = [
            DataRecord(data={"id": 1, "name": "Alice"}, schema=sample_data[0].schema),
            DataRecord(data={"id": 2, "name": None}, schema=sample_data[0].schema),
        ]
        transform = filter_by_field("name", None, operator="is_null")
        result = transform(sample_with_null)

        assert len(result) == 1
        assert result[0].get("id") == 2


class TestEnrichFromLookup:
    """Test enrich_from_lookup transformation."""

    def test_enrich_from_lookup(self) -> None:
        """Test enriching records from lookup."""
        lookup_schema = Schema(
            name="customer",
            fields=(
                SchemaField(name="customer_id", data_type=DataType.INTEGER),
                SchemaField(name="name", data_type=DataType.STRING),
                SchemaField(name="email", data_type=DataType.STRING),
            ),
        )

        lookup_data = [
            DataRecord(
                data={"customer_id": 1, "name": "Alice", "email": "alice@example.com"},
                schema=lookup_schema,
            ),
            DataRecord(
                data={"customer_id": 2, "name": "Bob", "email": "bob@example.com"},
                schema=lookup_schema,
            ),
        ]

        order_schema = Schema(
            name="order",
            fields=(
                SchemaField(name="order_id", data_type=DataType.INTEGER),
                SchemaField(name="customer_id", data_type=DataType.INTEGER),
                SchemaField(name="amount", data_type=DataType.FLOAT),
            ),
        )

        orders = [
            DataRecord(
                data={"order_id": 101, "customer_id": 1, "amount": 100.0}, schema=order_schema
            ),
            DataRecord(
                data={"order_id": 102, "customer_id": 2, "amount": 200.0}, schema=order_schema
            ),
        ]

        transform = enrich_from_lookup(
            lookup_data=lookup_data,
            lookup_key="customer_id",
            target_key="customer_id",
            fields_to_add=["name", "email"],
            prefix="customer_",
        )
        result = transform(orders)

        assert len(result) == 2
        assert result[0].get("customer_name") == "Alice"
        assert result[0].get("customer_email") == "alice@example.com"
        assert result[1].get("customer_name") == "Bob"

    def test_enrich_missing_lookup(self) -> None:
        """Test enrichment with missing lookup returns None for fields."""
        lookup_schema = Schema(
            name="customer",
            fields=(
                SchemaField(name="customer_id", data_type=DataType.INTEGER),
                SchemaField(name="name", data_type=DataType.STRING),
            ),
        )

        lookup_data = [
            DataRecord(data={"customer_id": 1, "name": "Alice"}, schema=lookup_schema),
        ]

        order_schema = Schema(
            name="order",
            fields=(
                SchemaField(name="order_id", data_type=DataType.INTEGER),
                SchemaField(name="customer_id", data_type=DataType.INTEGER),
            ),
        )

        orders = [
            DataRecord(data={"order_id": 101, "customer_id": 1}, schema=order_schema),
            DataRecord(
                data={"order_id": 102, "customer_id": 999}, schema=order_schema
            ),  # Not in lookup
        ]

        transform = enrich_from_lookup(
            lookup_data=lookup_data,
            lookup_key="customer_id",
            target_key="customer_id",
            fields_to_add=["name"],
            prefix="customer_",
        )
        result = transform(orders)

        assert result[0].get("customer_name") == "Alice"
        assert result[1].get("customer_name") is None  # No match


class TestRenameFields:
    """Test rename_fields transformation."""

    def test_rename_single_field(self, sample_data: list[DataRecord]) -> None:
        """Test renaming a single field."""
        transform = rename_fields({"name": "full_name"})
        result = transform(sample_data)

        assert "full_name" in result[0].data
        assert "name" not in result[0].data
        assert result[0].get("full_name") == "Alice"

    def test_rename_multiple_fields(self, sample_data: list[DataRecord]) -> None:
        """Test renaming multiple fields."""
        transform = rename_fields(
            {
                "name": "full_name",
                "email": "email_address",
                "status": "account_status",
            }
        )
        result = transform(sample_data)

        assert "full_name" in result[0].data
        assert "email_address" in result[0].data
        assert "account_status" in result[0].data
        assert "name" not in result[0].data
        assert "email" not in result[0].data


class TestDropFields:
    """Test drop_fields transformation."""

    def test_drop_single_field(self, sample_data: list[DataRecord]) -> None:
        """Test dropping a single field."""
        transform = drop_fields(["email"])
        result = transform(sample_data)

        assert "email" not in result[0].data
        assert "name" in result[0].data
        assert "id" in result[0].data

    def test_drop_multiple_fields(self, sample_data: list[DataRecord]) -> None:
        """Test dropping multiple fields."""
        transform = drop_fields(["email", "status"])
        result = transform(sample_data)

        assert "email" not in result[0].data
        assert "status" not in result[0].data
        assert "name" in result[0].data
        assert "id" in result[0].data


class TestSelectFields:
    """Test select_fields transformation."""

    def test_select_single_field(self, sample_data: list[DataRecord]) -> None:
        """Test selecting a single field."""
        transform = select_fields(["name"])
        result = transform(sample_data)

        assert "name" in result[0].data
        assert "id" not in result[0].data
        assert "email" not in result[0].data

    def test_select_multiple_fields(self, sample_data: list[DataRecord]) -> None:
        """Test selecting multiple fields."""
        transform = select_fields(["id", "name", "email"])
        result = transform(sample_data)

        assert "id" in result[0].data
        assert "name" in result[0].data
        assert "email" in result[0].data
        assert "age" not in result[0].data
        assert "status" not in result[0].data


class TestCastField:
    """Test cast_field transformation."""

    def test_cast_to_int(self) -> None:
        """Test casting field to int."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="value", data_type=DataType.STRING),),
        )
        data = [DataRecord(data={"value": "100"}, schema=schema)]

        transform = cast_field("value", int)
        result = transform(data)

        assert isinstance(result[0].get("value"), int)
        assert result[0].get("value") == 100

    def test_cast_to_float(self) -> None:
        """Test casting field to float."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="value", data_type=DataType.STRING),),
        )
        data = [DataRecord(data={"value": "100.5"}, schema=schema)]

        transform = cast_field("value", float)
        result = transform(data)

        assert isinstance(result[0].get("value"), float)
        assert result[0].get("value") == 100.5

    def test_cast_to_str(self) -> None:
        """Test casting field to str."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="value", data_type=DataType.INTEGER),),
        )
        data = [DataRecord(data={"value": 100}, schema=schema)]

        transform = cast_field("value", str)
        result = transform(data)

        assert isinstance(result[0].get("value"), str)
        assert result[0].get("value") == "100"

    def test_cast_with_default(self) -> None:
        """Test casting with default value on failure."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="value", data_type=DataType.STRING),),
        )
        data = [DataRecord(data={"value": "not_a_number"}, schema=schema)]

        transform = cast_field("value", int, default=0)
        result = transform(data)

        assert result[0].get("value") == 0

    def test_cast_null_value(self) -> None:
        """Test casting null value."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="value", data_type=DataType.STRING),),
        )
        data = [DataRecord(data={"value": None}, schema=schema)]

        transform = cast_field("value", int, default=0)
        result = transform(data)

        assert result[0].get("value") == 0
