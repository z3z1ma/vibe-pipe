"""
Comprehensive tests for validation checks.

Tests all 30+ validation types including:
- Statistical checks
- Regex pattern matching
- Range and value checks
- Cross-column validation
- Aggregate validation
"""

import pytest

from vibe_piper.types import DataRecord, DataType, Schema, SchemaField
from vibe_piper.validation.checks import (
    expect_column_groupby_mean_to_be_between,
    expect_column_groupby_value_counts_to_be_between,
    expect_column_max_to_be_between,
    expect_column_mean_to_be_between,
    expect_column_median_to_be_between,
    expect_column_min_to_be_between,
    expect_column_pair_values_a_to_be_greater_than_b,
    expect_column_pair_values_to_be_equal,
    expect_column_pair_values_to_be_not_equal,
    expect_column_proportion_of_nulls_to_be_between,
    expect_column_std_dev_to_be_between,
    expect_column_sum_to_equal_other_column_sum,
    expect_column_value_lengths_to_be_between,
    expect_column_values_to_be_between,
    expect_column_values_to_be_dateutil_parseable,
    expect_column_values_to_be_decreasing,
    expect_column_values_to_be_in_set,
    expect_column_values_to_be_increasing,
    # expect_column_values_to_not_in_set,  # Temporarily disabled due to import issue
    expect_column_values_to_be_of_type,
    expect_column_values_to_be_unique,
    expect_column_values_to_match_regex,
    expect_column_values_to_not_be_null,
    expect_column_values_to_not_match_regex,
    expect_table_row_count_to_be_between,
    expect_table_row_count_to_equal,
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
            SchemaField(name="id", data_type=DataType.INTEGER),
            SchemaField(name="name", data_type=DataType.STRING),
            SchemaField(name="age", data_type=DataType.INTEGER),
            SchemaField(name="email", data_type=DataType.STRING),
            SchemaField(name="score", data_type=DataType.FLOAT),
        ),
    )


@pytest.fixture
def sample_records(sample_schema):
    """Create sample records for testing."""
    return (
        DataRecord(
            data={"id": 1, "name": "Alice", "age": 30, "email": "alice@example.com", "score": 95.5},
            schema=sample_schema,
        ),
        DataRecord(
            data={"id": 2, "name": "Bob", "age": 25, "email": "bob@example.com", "score": 87.0},
            schema=sample_schema,
        ),
        DataRecord(
            data={
                "id": 3,
                "name": "Charlie",
                "age": 35,
                "email": "charlie@example.com",
                "score": 92.5,
            },
            schema=sample_schema,
        ),
    )


# =============================================================================
# Statistical Checks Tests
# =============================================================================


class TestStatisticalChecks:
    """Test statistical validation checks."""

    def test_expect_column_mean_to_be_between_pass(self, sample_records):
        """Test mean check passes when mean is in range."""
        check = expect_column_mean_to_be_between("age", 20, 40)
        result = check(sample_records)
        assert result.is_valid
        assert not result.errors

    def test_expect_column_mean_to_be_between_fail(self, sample_records):
        """Test mean check fails when mean is outside range."""
        check = expect_column_mean_to_be_between("age", 40, 50)
        result = check(sample_records)
        assert not result.is_valid
        assert "mean" in str(result.errors).lower()
        assert "age" in str(result.errors).lower()

    def test_expect_column_std_dev_to_be_between_pass(self, sample_records):
        """Test std dev check passes."""
        check = expect_column_std_dev_to_be_between("age", 0, 10)
        result = check(sample_records)
        assert result.is_valid

    def test_expect_column_min_to_be_between_pass(self, sample_records):
        """Test min check passes."""
        check = expect_column_min_to_be_between("age", 20, 30)
        result = check(sample_records)
        assert result.is_valid

    def test_expect_column_max_to_be_between_pass(self, sample_records):
        """Test max check passes."""
        check = expect_column_max_to_be_between("age", 30, 40)
        result = check(sample_records)
        assert result.is_valid

    def test_expect_column_median_to_be_between_pass(self, sample_records):
        """Test median check passes."""
        check = expect_column_median_to_be_between("age", 25, 35)
        result = check(sample_records)
        assert result.is_valid


# =============================================================================
# Regex Pattern Matching Tests
# =============================================================================


class TestRegexPatternMatching:
    """Test regex pattern matching checks."""

    def test_expect_column_values_to_match_regex_pass(self, sample_records):
        """Test regex match passes for valid emails."""
        check = expect_column_values_to_match_regex("email", r"^[\\w\\.-]+@")
        result = check(sample_records)
        assert result.is_valid

    def test_expect_column_values_to_match_regex_fail(self, sample_schema):
        """Test regex match fails for invalid pattern."""
        records = (
            DataRecord(data={"email": "invalid"}, schema=sample_schema),
            DataRecord(data={"email": "also-invalid"}, schema=sample_schema),
        )
        check = expect_column_values_to_match_regex("email", r"^[\\w\\.-]+@")
        result = check(records)
        assert not result.is_valid

    def test_expect_column_values_to_not_match_regex_pass(self, sample_schema):
        """Test regex not match passes."""
        records = (
            DataRecord(data={"email": "user@example.com"}, schema=sample_schema),
            DataRecord(data={"email": "admin@example.com"}, schema=sample_schema),
        )
        check = expect_column_values_to_not_match_regex("email", r"^\\s*$")
        result = check(records)
        assert result.is_valid

    def test_expect_column_values_to_not_match_regex_fail(self, sample_schema):
        """Test regex not match fails."""
        records = (DataRecord(data={"email": " whitespace "}, schema=sample_schema),)
        check = expect_column_values_to_not_match_regex("email", r"^\\s+.*\\s+$")
        result = check(records)
        assert not result.is_valid


# =============================================================================
# Range and Value Checks Tests
# =============================================================================


class TestRangeAndValueChecks:
    """Test range and value validation checks."""

    def test_expect_column_values_to_be_between_pass(self, sample_records):
        """Test range check passes."""
        check = expect_column_values_to_be_between("age", 20, 40)
        result = check(sample_records)
        assert result.is_valid

    def test_expect_column_values_to_be_between_fail(self, sample_schema):
        """Test range check fails."""
        records = (
            DataRecord(data={"age": 15}, schema=sample_schema),
            DataRecord(data={"age": 25}, schema=sample_schema),
        )
        check = expect_column_values_to_be_between("age", 20, 40)
        result = check(records)
        assert not result.is_valid

    def test_expect_column_values_to_be_in_set_pass(self, sample_records):
        """Test in set check passes."""
        check = expect_column_values_to_be_in_set("name", {"Alice", "Bob", "Charlie"})
        result = check(sample_records)
        assert result.is_valid

    def test_expect_column_values_to_be_in_set_fail(self, sample_schema):
        """Test in set check fails."""
        records = (DataRecord(data={"name": "Unknown"}, schema=sample_schema),)
        check = expect_column_values_to_be_in_set("name", {"Alice", "Bob"})
        result = check(records)
        assert not result.is_valid

    def test_expect_column_values_to_not_be_in_set_pass(self, sample_schema):
        """Test not in set check passes."""
        records = (DataRecord(data={"status": "active"}, schema=sample_schema),)
        check = expect_column_values_to_not_be_in_set("status", {"deleted", "banned"})
        result = check(records)
        assert result.is_valid

    def test_expect_column_values_to_be_unique_pass(self, sample_records):
        """Test unique check passes."""
        check = expect_column_values_to_be_unique("id")
        result = check(sample_records)
        assert result.is_valid

    def test_expect_column_values_to_be_unique_fail(self, sample_schema):
        """Test unique check fails."""
        records = (
            DataRecord(data={"id": 1}, schema=sample_schema),
            DataRecord(data={"id": 1}, schema=sample_schema),
        )
        check = expect_column_values_to_be_unique("id")
        result = check(records)
        assert not result.is_valid


# =============================================================================
# Type and Null Checks Tests
# =============================================================================


class TestTypeAndNullChecks:
    """Test type and null validation checks."""

    def test_expect_column_values_to_be_of_type_pass(self, sample_records):
        """Test type check passes."""
        check = expect_column_values_to_be_of_type("age", DataType.INTEGER)
        result = check(sample_records)
        assert result.is_valid

    def test_expect_column_values_to_be_of_type_fail(self, sample_schema):
        """Test type check fails."""
        records = (DataRecord(data={"age": "thirty"}, schema=sample_schema),)
        check = expect_column_values_to_be_of_type("age", DataType.INTEGER)
        result = check(records)
        assert not result.is_valid

    def test_expect_column_values_to_not_be_null_pass(self, sample_records):
        """Test not null check passes."""
        check = expect_column_values_to_not_be_null("name")
        result = check(sample_records)
        assert result.is_valid

    def test_expect_column_values_to_not_be_null_fail(self, sample_records):
        """Test not null check fails."""
        records_with_null = tuple(
            DataRecord(data={**r.data, "name": None}, schema=r.schema) for r in sample_records[:1]
        )
        check = expect_column_values_to_not_be_null("name")
        result = check(records_with_null)
        assert not result.is_valid

    def test_expect_column_proportion_of_nulls_to_be_between_pass(self, sample_schema):
        """Test null proportion check passes."""
        records = (
            DataRecord(data={"value": 1}, schema=sample_schema),
            DataRecord(data={"value": None}, schema=sample_schema),
            DataRecord(data={"value": None}, schema=sample_schema),
        )
        check = expect_column_proportion_of_nulls_to_be_between("value", 0.5, 0.7)
        result = check(records)
        assert result.is_valid


# =============================================================================
# String Length and Format Tests
# =============================================================================


class TestStringLengthAndFormat:
    """Test string length and format checks."""

    def test_expect_column_value_lengths_to_be_between_pass(self, sample_records):
        """Test string length check passes."""
        check = expect_column_value_lengths_to_be_between("name", 3, 10)
        result = check(sample_records)
        assert result.is_valid

    def test_expect_column_value_lengths_to_be_between_fail(self, sample_schema):
        """Test string length check fails."""
        records = (DataRecord(data={"name": "A"}, schema=sample_schema),)
        check = expect_column_value_lengths_to_be_between("name", 3, 10)
        result = check(records)
        assert not result.is_valid

    def test_expect_column_values_to_be_increasing_pass(self, sample_schema):
        """Test increasing check passes."""
        records = (
            DataRecord(data={"value": 1}, schema=sample_schema),
            DataRecord(data={"value": 2}, schema=sample_schema),
            DataRecord(data={"value": 3}, schema=sample_schema),
        )
        check = expect_column_values_to_be_increasing("value")
        result = check(records)
        assert result.is_valid

    def test_expect_column_values_to_be_decreasing_pass(self, sample_schema):
        """Test decreasing check passes."""
        records = (
            DataRecord(data={"value": 3}, schema=sample_schema),
            DataRecord(data={"value": 2}, schema=sample_schema),
            DataRecord(data={"value": 1}, schema=sample_schema),
        )
        check = expect_column_values_to_be_decreasing("value")
        result = check(records)
        assert result.is_valid


# =============================================================================
# Cross-Column Validation Tests
# =============================================================================


class TestCrossColumnValidation:
    """Test cross-column validation checks."""

    def test_expect_column_pair_values_to_be_equal_pass(self, sample_schema):
        """Test equal columns check passes."""
        records = (
            DataRecord(data={"a": 1, "b": 1}, schema=sample_schema),
            DataRecord(data={"a": 2, "b": 2}, schema=sample_schema),
        )
        check = expect_column_pair_values_to_be_equal("a", "b")
        result = check(records)
        assert result.is_valid

    def test_expect_column_pair_values_to_be_equal_fail(self, sample_schema):
        """Test equal columns check fails."""
        records = (DataRecord(data={"a": 1, "b": 2}, schema=sample_schema),)
        check = expect_column_pair_values_to_be_equal("a", "b")
        result = check(records)
        assert not result.is_valid

    def test_expect_column_pair_values_to_be_not_equal_pass(self, sample_schema):
        """Test not equal columns check passes."""
        records = (DataRecord(data={"a": 1, "b": 2}, schema=sample_schema),)
        check = expect_column_pair_values_to_be_not_equal("a", "b")
        result = check(records)
        assert result.is_valid

    def test_expect_column_pair_values_a_to_be_greater_than_b_pass(self, sample_schema):
        """Test A > B check passes."""
        records = (DataRecord(data={"start": 10, "end": 20}, schema=sample_schema),)
        check = expect_column_pair_values_a_to_be_greater_than_b("end", "start")
        result = check(records)
        assert result.is_valid

    def test_expect_column_sum_to_equal_other_column_sum_pass(self, sample_schema):
        """Test sum equality check passes."""
        records = (
            DataRecord(data={"a": 1, "b": 1}, schema=sample_schema),
            DataRecord(data={"a": 2, "b": 2}, schema=sample_schema),
        )
        check = expect_column_sum_to_equal_other_column_sum("a", "b")
        result = check(records)
        assert result.is_valid


# =============================================================================
# Aggregate Validation Tests
# =============================================================================


class TestAggregateValidation:
    """Test aggregate validation checks."""

    def test_expect_column_groupby_value_counts_to_be_between_pass(self, sample_schema):
        """Test groupby counts check passes."""
        records = (
            DataRecord(data={"group": "A", "value": 1}, schema=sample_schema),
            DataRecord(data={"group": "A", "value": 2}, schema=sample_schema),
            DataRecord(data={"group": "B", "value": 3}, schema=sample_schema),
        )
        check = expect_column_groupby_value_counts_to_be_between("value", "group", 1, 2)
        result = check(records)
        assert result.is_valid

    def test_expect_column_groupby_mean_to_be_between_pass(self, sample_schema):
        """Test groupby mean check passes."""
        records = (
            DataRecord(data={"group": "A", "value": 10}, schema=sample_schema),
            DataRecord(data={"group": "A", "value": 20}, schema=sample_schema),
            DataRecord(data={"group": "B", "value": 15}, schema=sample_schema),
        )
        check = expect_column_groupby_mean_to_be_between("value", "group", 10, 20)
        result = check(records)
        assert result.is_valid


# =============================================================================
# Table-Level Validation Tests
# =============================================================================


class TestTableLevelValidation:
    """Test table-level validation checks."""

    def test_expect_table_row_count_to_be_between_pass(self, sample_records):
        """Test row count range check passes."""
        check = expect_table_row_count_to_be_between(1, 10)
        result = check(sample_records)
        assert result.is_valid

    def test_expect_table_row_count_to_be_between_fail(self, sample_records):
        """Test row count range check fails."""
        check = expect_table_row_count_to_be_between(10, 20)
        result = check(sample_records)
        assert not result.is_valid

    def test_expect_table_row_count_to_equal_pass(self, sample_records):
        """Test row count equality check passes."""
        check = expect_table_row_count_to_equal(3)
        result = check(sample_records)
        assert result.is_valid

    def test_expect_table_row_count_to_equal_fail(self, sample_records):
        """Test row count equality check fails."""
        check = expect_table_row_count_to_equal(5)
        result = check(sample_records)
        assert not result.is_valid


# =============================================================================
# Date Validation Tests
# =============================================================================


class TestDateValidation:
    """Test date validation checks."""

    def test_expect_column_values_to_be_dateutil_parseable_pass(self, sample_schema):
        """Test date parseable check passes."""
        records = (
            DataRecord(data={"date": "2024-01-01"}, schema=sample_schema),
            DataRecord(data={"date": "2024-12-31"}, schema=sample_schema),
        )
        check = expect_column_values_to_be_dateutil_parseable("date")
        result = check(records)
        assert result.is_valid

    def test_expect_column_values_to_be_dateutil_parseable_fail(self, sample_schema):
        """Test date parseable check fails."""
        records = (DataRecord(data={"date": "not-a-date"}, schema=sample_schema),)
        check = expect_column_values_to_be_dateutil_parseable("date")
        result = check(records)
        assert not result.is_valid


# =============================================================================
# Edge Cases
# =============================================================================


class TestEdgeCases:
    """Test edge cases for validation checks."""

    def test_empty_records_pass(self, sample_schema):
        """Test that empty records pass validation."""
        records: tuple[DataRecord, ...] = ()
        check = expect_column_mean_to_be_between("age", 0, 100)
        result = check(records)
        assert result.is_valid

    def test_all_null_values(self, sample_schema):
        """Test handling of all null values."""
        records = (
            DataRecord(data={"value": None}, schema=sample_schema),
            DataRecord(data={"value": None}, schema=sample_schema),
        )
        check = expect_column_mean_to_be_between("value", 0, 100)
        result = check(records)
        assert not result.is_valid

    def test_single_record(self, sample_records):
        """Test validation with single record."""
        single_record = sample_records[:1]
        check = expect_column_values_to_be_unique("id")
        result = check(single_record)
        assert result.is_valid
