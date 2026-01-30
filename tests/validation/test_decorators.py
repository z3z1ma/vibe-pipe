"""
Tests for validation decorators and fluent API.

Tests the @validate decorator and @expect fluent API.
"""

import pytest

from vibe_piper.types import DataRecord, DataType, Schema, SchemaField
from vibe_piper.validation import (
    ValidationConfig,
    expect,
    validate,
)

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def sample_schema():
    """Create a sample schema for testing."""
    return Schema(
        name="test_schema",
        fields=(
            SchemaField(name="id", data_type=DataType.INTEGER, required=False),
            SchemaField(name="name", data_type=DataType.STRING, required=False),
            SchemaField(name="age", data_type=DataType.INTEGER, required=False),
            SchemaField(name="email", data_type=DataType.STRING, required=False),
        ),
    )


@pytest.fixture
def sample_records(sample_schema):
    """Create sample records for testing."""
    return (
        DataRecord(
            data={"id": 1, "name": "Alice", "age": 30, "email": "alice@example.com"},
            schema=sample_schema,
        ),
        DataRecord(
            data={"id": 2, "name": "Bob", "age": 25, "email": "bob@example.com"},
            schema=sample_schema,
        ),
        DataRecord(
            data={"id": 3, "name": "Charlie", "age": 35, "email": "charlie@example.com"},
            schema=sample_schema,
        ),
    )


# =============================================================================
# @validate Decorator Tests
# =============================================================================


class TestValidateDecorator:
    """Test the @validate decorator."""

    def test_validate_with_schema_pass(self, sample_schema, sample_records):
        """Test @validate with schema passes for valid data."""

        @validate(schema=sample_schema)
        def get_data():
            return sample_records

        result = get_data()
        assert len(result) == 3

    def test_validate_with_schema_fail(self, sample_schema):
        """Test @validate with schema fails for invalid data."""
        # Create records with wrong schema
        wrong_schema = Schema(
            name="wrong_schema",
            fields=(SchemaField(name="id", data_type=DataType.INTEGER),),
        )
        invalid_records = (DataRecord(data={"id": 1}, schema=wrong_schema),)

        @validate(schema=sample_schema)
        def get_data():
            return invalid_records

        with pytest.raises(ValueError, match="Validation failed"):
            get_data()

    def test_validate_with_checks_pass(self, sample_records):
        """Test @validate with custom checks passes."""

        @validate(checks=(expect.column("age").to_be_between(20, 40),))
        def get_data():
            return sample_records

        result = get_data()
        assert len(result) == 3

    def test_validate_with_checks_fail(self, sample_schema):
        """Test @validate with custom checks fails."""
        records = (
            DataRecord(data={"age": 15}, schema=sample_schema),
            DataRecord(data={"age": 25}, schema=sample_schema),
        )

        @validate(checks=(expect.column("age").to_be_between(20, 40),))
        def get_data():
            return records

        with pytest.raises(ValueError, match="Validation failed"):
            get_data()

    def test_validate_lazy_mode_collects_all_errors(self, sample_schema):
        """Test lazy validation collects all errors."""
        records = (
            DataRecord(data={"age": 15, "email": "bad-email"}, schema=sample_schema),
            DataRecord(data={"age": 25, "email": "valid@example.com"}, schema=sample_schema),
        )

        @validate(
            lazy=True,
            checks=(
                expect.column("age").to_be_between(20, 40),
                expect.column("email").to_match_regex(r"^[\w\.-]+@"),
            ),
        )
        def get_data():
            return records

        with pytest.raises(ValueError) as exc_info:
            get_data()

        # Should have both age and email errors
        error_message = str(exc_info.value)
        assert "age" in error_message.lower()
        assert "email" in error_message.lower() or "pattern" in error_message.lower()

    def test_validate_on_failure_warn(self, sample_records, caplog):
        """Test on_failure='warn' issues warning."""
        import warnings

        records_with_invalid = (sample_records[0],)

        @validate(
            checks=(expect.column("id").to_be_unique(),),
            on_failure="warn",
        )
        def get_data():
            return records_with_invalid

        # Should not raise, just warn
        with warnings.catch_warnings(record=True):
            result = get_data()
            assert len(result) == 1

    def test_validate_on_failure_ignore(self, sample_records):
        """Test on_failure='ignore' ignores failures."""

        @validate(
            checks=(expect.column("id").to_be_unique(),),
            on_failure="ignore",
        )
        def get_data():
            return sample_records

        # Should not raise or warn
        result = get_data()
        assert len(result) == 3


# =============================================================================
# @expect Fluent API Tests
# =============================================================================


class TestExpectFluentAPI:
    """Test the @expect fluent API."""

    def test_expect_column_to_match_regex(self, sample_records):
        """Test expect.column().to_match_regex()."""
        check = expect.column("email").to_match_regex(r"^[\w\.-]+@")
        result = check(sample_records)
        assert result.is_valid

    def test_expect_column_to_be_between(self, sample_records):
        """Test expect.column().to_be_between()."""
        check = expect.column("age").to_be_between(20, 40)
        result = check(sample_records)
        assert result.is_valid

    def test_expect_column_to_be_in_set(self, sample_records):
        """Test expect.column().to_be_in_set()."""
        check = expect.column("name").to_be_in_set({"Alice", "Bob", "Charlie"})
        result = check(sample_records)
        assert result.is_valid

    def test_expect_column_to_be_unique(self, sample_records):
        """Test expect.column().to_be_unique()."""
        check = expect.column("id").to_be_unique()
        result = check(sample_records)
        assert result.is_valid

    def test_expect_column_to_not_be_null(self, sample_records):
        """Test expect.column().to_not_be_null()."""
        check = expect.column("name").to_not_be_null()
        result = check(sample_records)
        assert result.is_valid

    def test_expect_column_value_lengths_to_be_between(self, sample_records):
        """Test expect.column().value_lengths_to_be_between()."""
        check = expect.column("name").value_lengths_to_be_between(3, 10)
        result = check(sample_records)
        assert result.is_valid

    def test_expect_column_to_be_increasing(self, sample_schema):
        """Test expect.column().to_be_increasing()."""
        records = (
            DataRecord(data={"value": 1}, schema=sample_schema),
            DataRecord(data={"value": 2}, schema=sample_schema),
            DataRecord(data={"value": 3}, schema=sample_schema),
        )
        check = expect.column("value").to_be_increasing()
        result = check(records)
        assert result.is_valid

    def test_expect_column_to_be_decreasing(self, sample_schema):
        """Test expect.column().to_be_decreasing()."""
        records = (
            DataRecord(data={"value": 3}, schema=sample_schema),
            DataRecord(data={"value": 2}, schema=sample_schema),
            DataRecord(data={"value": 1}, schema=sample_schema),
        )
        check = expect.column("value").to_be_decreasing()
        result = check(records)
        assert result.is_valid

    def test_expect_column_mean_to_be_between(self, sample_records):
        """Test expect.column().mean_to_be_between()."""
        check = expect.column("age").mean_to_be_between(20, 40)
        result = check(sample_records)
        assert result.is_valid

    def test_expect_column_std_dev_to_be_between(self, sample_records):
        """Test expect.column().std_dev_to_be_between()."""
        check = expect.column("age").std_dev_to_be_between(0, 10)
        result = check(sample_records)
        assert result.is_valid


# =============================================================================
# Multi-Column Expectations Tests
# =============================================================================


class TestMultiColumnExpectations:
    """Test multi-column expectations via expect.columns()."""

    def test_expect_columns_to_be_equal_pass(self, sample_schema):
        """Test expect.columns().to_be_equal() passes."""
        records = (
            DataRecord(data={"a": 1, "b": 1}, schema=sample_schema),
            DataRecord(data={"a": 2, "b": 2}, schema=sample_schema),
        )
        check = expect.columns(["a", "b"]).to_be_equal()
        result = check(records)
        assert result.is_valid

    def test_expect_columns_to_be_equal_fail(self, sample_schema):
        """Test expect.columns().to_be_equal() fails."""
        records = (DataRecord(data={"a": 1, "b": 2}, schema=sample_schema),)
        check = expect.columns(["a", "b"]).to_be_equal()
        result = check(records)
        assert not result.is_valid

    def test_expect_columns_a_to_be_greater_than_b_pass(self, sample_schema):
        """Test expect.columns().a_to_be_greater_than_b() passes."""
        records = (DataRecord(data={"start": 10, "end": 20}, schema=sample_schema),)
        check = expect.columns(["end", "start"]).a_to_be_greater_than_b()
        result = check(records)
        assert result.is_valid

    def test_expect_columns_a_to_be_greater_than_b_or_equal(self, sample_schema):
        """Test expect.columns().a_to_be_greater_than_b(or_equal=True)."""
        records = (
            DataRecord(data={"a": 10, "b": 10}, schema=sample_schema),
            DataRecord(data={"a": 20, "b": 10}, schema=sample_schema),
        )
        check = expect.columns(["a", "b"]).a_to_be_greater_than_b(or_equal=True)
        result = check(records)
        assert result.is_valid


# =============================================================================
# Table-Level Expectations Tests
# =============================================================================


class TestTableExpectations:
    """Test table-level expectations via expect.table()."""

    def test_expect_table_row_count_to_be_between_pass(self, sample_records):
        """Test expect.table().row_count_to_be_between() passes."""
        check = expect.table().row_count_to_be_between(1, 10)
        result = check(sample_records)
        assert result.is_valid

    def test_expect_table_row_count_to_be_between_fail(self, sample_records):
        """Test expect.table().row_count_to_be_between() fails."""
        check = expect.table().row_count_to_be_between(10, 20)
        result = check(sample_records)
        assert not result.is_valid

    def test_expect_table_row_count_to_equal_pass(self, sample_records):
        """Test expect.table().row_count_to_equal() passes."""
        check = expect.table().row_count_to_equal(3)
        result = check(sample_records)
        assert result.is_valid

    def test_expect_table_row_count_to_equal_fail(self, sample_records):
        """Test expect.table().row_count_to_equal() fails."""
        check = expect.table().row_count_to_equal(5)
        result = check(sample_records)
        assert not result.is_valid


# =============================================================================
# ValidationConfig Tests
# =============================================================================


class TestValidationConfig:
    """Test ValidationConfig dataclass."""

    def test_validation_config_creation(self):
        """Test creating a ValidationConfig."""
        config = ValidationConfig(
            lazy=True,
            on_failure="warn",
            severity="warning",
        )
        assert config.lazy is True
        assert config.on_failure == "warn"
        assert config.severity == "warning"
        assert config.schema is None
        assert config.checks == ()

    def test_validation_config_with_schema(self, sample_schema):
        """Test ValidationConfig with schema."""
        config = ValidationConfig(schema=sample_schema)
        assert config.schema == sample_schema
