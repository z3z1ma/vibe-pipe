"""
Tests for validation suite framework.

Tests ValidationSuite, different strategies, and result handling.
"""

import pytest

from vibe_piper.types import DataRecord, DataType, Schema, SchemaField
from vibe_piper.validation import (
    ValidationStrategy,
    ValidationSuite,
    create_validation_suite,
    expect,
)
from vibe_piper.validation.suite import (
    FailFastValidationStrategy,
    LazyValidationStrategy,
    ValidationContext,
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
            data={"id": 3, "name": "Charlie", "age": 35, "email": "charlie@example.com", "score": 92.5},
            schema=sample_schema,
        ),
    )


@pytest.fixture
def invalid_records(sample_schema):
    """Create invalid records for testing."""
    return (
        DataRecord(
            data={"id": 1, "name": "Alice", "age": 15, "email": "bad-email", "score": 95.5},
            schema=sample_schema,
        ),
        DataRecord(
            data={"id": 1, "name": "Bob", "age": 25, "email": "bob@example.com", "score": 87.0},
            schema=sample_schema,
        ),
    )


# =============================================================================
# ValidationSuite Tests
# =============================================================================


class TestValidationSuite:
    """Test ValidationSuite functionality."""

    def test_create_suite(self):
        """Test creating a validation suite."""
        suite = ValidationSuite(name="test_suite")
        assert suite.name == "test_suite"
        assert len(suite) == 0
        assert suite.strategy == ValidationStrategy.COLLECT_ALL

    def test_add_check(self):
        """Test adding a check to suite."""
        suite = ValidationSuite(name="test_suite")
        check_fn = expect.column("id").to_be_unique()

        result = suite.add_check("unique_ids", check_fn)
        assert result is suite  # Method chaining
        assert "unique_ids" in suite.list_checks()
        assert len(suite) == 1

    def test_add_duplicate_check_raises(self):
        """Test adding duplicate check raises error."""
        suite = ValidationSuite(name="test_suite")
        check_fn = expect.column("id").to_be_unique()

        suite.add_check("unique_ids", check_fn)
        with pytest.raises(ValueError, match="already exists"):
            suite.add_check("unique_ids", check_fn)

    def test_add_checks(self):
        """Test adding multiple checks."""
        suite = ValidationSuite(name="test_suite")
        checks = {
            "unique_ids": expect.column("id").to_be_unique(),
            "valid_ages": expect.column("age").to_be_between(0, 120),
        }

        suite.add_checks(checks)
        assert len(suite) == 2
        assert "unique_ids" in suite
        assert "valid_ages" in suite

    def test_remove_check(self):
        """Test removing a check."""
        suite = ValidationSuite(name="test_suite")
        check_fn = expect.column("id").to_be_unique()

        suite.add_check("unique_ids", check_fn)
        removed = suite.remove_check("unique_ids")

        assert removed is check_fn
        assert "unique_ids" not in suite
        assert len(suite) == 0

    def test_remove_nonexistent_check_raises(self):
        """Test removing non-existent check raises error."""
        suite = ValidationSuite(name="test_suite")
        with pytest.raises(KeyError, match="not found"):
            suite.remove_check("nonexistent")

    def test_get_check(self):
        """Test getting a check."""
        suite = ValidationSuite(name="test_suite")
        check_fn = expect.column("id").to_be_unique()

        suite.add_check("unique_ids", check_fn)
        retrieved = suite.get_check("unique_ids")

        assert retrieved is check_fn
        assert suite.get_check("nonexistent") is None

    def test_list_checks(self):
        """Test listing all checks."""
        suite = ValidationSuite(name="test_suite")
        checks = {
            "check1": expect.column("id").to_be_unique(),
            "check2": expect.column("age").to_be_between(0, 120),
            "check3": expect.column("email").to_match_regex(r"^[\\w\\.-]+@"),
        }

        suite.add_checks(checks)
        check_names = suite.list_checks()

        assert len(check_names) == 3
        assert "check1" in check_names
        assert "check2" in check_names
        assert "check3" in check_names

    def test_contains(self):
        """Test __contains__ for check membership."""
        suite = ValidationSuite(name="test_suite")
        suite.add_check("check1", expect.column("id").to_be_unique())

        assert "check1" in suite
        assert "check2" not in suite

    def test_repr(self):
        """Test string representation."""
        suite = ValidationSuite(name="test_suite")
        suite.add_check("check1", expect.column("id").to_be_unique())

        repr_str = repr(suite)
        assert "test_suite" in repr_str
        assert "1" in repr_str


# =============================================================================
# Validation Execution Tests
# =============================================================================


class TestValidationExecution:
    """Test validation execution."""

    def test_validate_all_pass(self, sample_records):
        """Test validation where all checks pass."""
        suite = ValidationSuite(name="test_suite")
        suite.add_check("unique_ids", expect.column("id").to_be_unique())
        suite.add_check("valid_ages", expect.column("age").to_be_between(20, 40))

        result = suite.validate(sample_records)

        assert result.success is True
        assert result.total_checks == 2
        assert len(result.failed_checks) == 0
        assert result.total_records == 3

    def test_validate_some_fail(self, invalid_records):
        """Test validation where some checks fail."""
        suite = ValidationSuite(name="test_suite")
        suite.add_check("unique_ids", expect.column("id").to_be_unique())
        suite.add_check("valid_ages", expect.column("age").to_be_between(20, 40))
        suite.add_check("valid_emails", expect.column("email").to_match_regex(r"^[\\w\\.-]+@"))

        result = suite.validate(invalid_records)

        assert result.success is False
        assert result.total_checks == 3
        assert len(result.failed_checks) > 0
        assert result.total_records == 2

    def test_validate_with_context(self, sample_records):
        """Test validation with custom context."""
        suite = ValidationSuite(name="test_suite")
        suite.add_check("unique_ids", expect.column("id").to_be_unique())

        context = ValidationContext(
            validation_suite="test_suite",
            metadata={"run_id": "test-123"},
        )

        result = suite.validate(sample_records, context=context)

        assert result.context is not None
        assert result.context.validation_suite == "test_suite"
        assert result.context.metadata["run_id"] == "test-123"

    def test_validate_empty_records(self, sample_schema):
        """Test validation with empty records."""
        suite = ValidationSuite(name="test_suite")
        suite.add_check("unique_ids", expect.column("id").to_be_unique())

        empty_records: tuple[DataRecord, ...] = ()
        result = suite.validate(empty_records)

        # Should pass with empty records
        assert result.success is True
        assert result.total_records == 0

    def test_validate_with_exception(self, sample_records):
        """Test validation when a check raises an exception."""

        def failing_check(records):
            raise ValueError("Check failed with exception")

        suite = ValidationSuite(name="test_suite")
        suite.add_check("failing_check", failing_check)

        result = suite.validate(sample_records)

        assert result.success is False
        assert len(result.errors) > 0
        assert "exception" in str(result.errors).lower()


# =============================================================================
# Validation Strategy Tests
# =============================================================================


class TestValidationStrategy:
    """Test different validation strategies."""

    def test_collect_all_strategy(self, invalid_records):
        """Test COLLECT_ALL strategy runs all checks."""
        suite = ValidationSuite(
            name="test_suite",
            strategy=ValidationStrategy.COLLECT_ALL,
        )
        suite.add_check("unique_ids", expect.column("id").to_be_unique())
        suite.add_check("valid_ages", expect.column("age").to_be_between(20, 40))
        suite.add_check("valid_emails", expect.column("email").to_match_regex(r"^[\\w\\.-]+@"))

        result = suite.validate(invalid_records)

        # Should run all checks and collect all failures
        assert result.total_checks == 3
        # All 3 checks should fail (duplicate IDs, age 15, bad email)
        assert len(result.failed_checks) == 3

    def test_fail_fast_strategy(self, invalid_records):
        """Test FAIL_FAST strategy stops on first failure."""
        suite = ValidationSuite(
            name="test_suite",
            strategy=ValidationStrategy.FAIL_FAST,
        )
        suite.add_check("unique_ids", expect.column("id").to_be_unique())
        suite.add_check("valid_ages", expect.column("age").to_be_between(20, 40))
        suite.add_check("valid_emails", expect.column("email").to_match_regex(r"^[\\w\\.-]+@"))

        result = suite.validate(invalid_records)

        # Should stop after first failure
        assert len(result.failed_checks) <= len(suite._checks)
        # At least one check should have run
        assert len(result.failed_checks) >= 1

    def test_continue_on_warning_strategy(self, sample_records):
        """Test CONTINUE_ON_WARNING strategy."""
        suite = ValidationSuite(
            name="test_suite",
            strategy=ValidationStrategy.CONTINUE_ON_WARNING,
        )
        suite.add_check("unique_ids", expect.column("id").to_be_unique())

        result = suite.validate(sample_records)
        assert result.success is True


# =============================================================================
# Validation Result Tests
# =============================================================================


class TestSuiteValidationResult:
    """Test SuiteValidationResult."""

    def test_get_failure_details(self, invalid_records):
        """Test getting failure details."""
        suite = ValidationSuite(name="test_suite")
        suite.add_check("unique_ids", expect.column("id").to_be_unique())
        suite.add_check("valid_ages", expect.column("age").to_be_between(20, 40))

        result = suite.validate(invalid_records)

        details = result.get_failure_details()
        assert isinstance(details, dict)
        # Should have details for failed checks
        for check_name in result.failed_checks:
            assert check_name in details

    def test_get_summary_passed(self, sample_records):
        """Test summary for passed validation."""
        suite = ValidationSuite(name="test_suite")
        suite.add_check("unique_ids", expect.column("id").to_be_unique())

        result = suite.validate(sample_records)
        summary = result.get_summary()

        assert "PASSED" in summary
        assert "Total checks: 1" in summary
        assert "Failed: 0" in summary
        assert "Total records: 3" in summary

    def test_get_summary_failed(self, invalid_records):
        """Test summary for failed validation."""
        suite = ValidationSuite(name="test_suite")
        suite.add_check("unique_ids", expect.column("id").to_be_unique())
        suite.add_check("valid_ages", expect.column("age").to_be_between(20, 40))

        result = suite.validate(invalid_records)
        summary = result.get_summary()

        assert "FAILED" in summary
        assert "Failed checks:" in summary
        assert "Errors:" in summary


# =============================================================================
# Lazy and Fail-Fast Strategy Classes Tests
# =============================================================================


class TestStrategyClasses:
    """Test LazyValidationStrategy and FailFastValidationStrategy."""

    def test_lazy_strategy_never_stops(self):
        """Test LazyValidationStrategy never stops."""
        strategy = LazyValidationStrategy()

        from vibe_piper.types import ValidationResult

        # Should never stop, even on failure
        assert strategy.should_stop(ValidationResult(is_valid=True)) is False
        assert strategy.should_stop(ValidationResult(is_valid=False, errors=("error",))) is False

    def test_fail_fast_strategy_stops_on_failure(self):
        """Test FailFastValidationStrategy stops on failure."""
        strategy = FailFastValidationStrategy()

        from vibe_piper.types import ValidationResult

        # Should stop on failure
        assert strategy.should_stop(ValidationResult(is_valid=True)) is False
        assert strategy.should_stop(ValidationResult(is_valid=False, errors=("error",))) is True


# =============================================================================
# Convenience Functions Tests
# =============================================================================


class TestConvenienceFunctions:
    """Test convenience functions."""

    def test_create_validation_suite(self):
        """Test create_validation_suite function."""
        checks = {
            "unique_ids": expect.column("id").to_be_unique(),
            "valid_ages": expect.column("age").to_be_between(0, 120),
        }

        suite = create_validation_suite(
            name="my_suite",
            checks=checks,
            strategy=ValidationStrategy.COLLECT_ALL,
            description="My validation suite",
        )

        assert suite.name == "my_suite"
        assert len(suite) == 2
        assert suite.strategy == ValidationStrategy.COLLECT_ALL
        assert suite.description == "My validation suite"

    def test_create_validation_suite_without_checks(self):
        """Test creating suite without checks."""
        suite = create_validation_suite(name="empty_suite")

        assert suite.name == "empty_suite"
        assert len(suite) == 0


# =============================================================================
# ValidationContext Tests
# =============================================================================


class TestValidationContext:
    """Test ValidationContext."""

    def test_create_context(self):
        """Test creating a validation context."""
        context = ValidationContext(
            validation_suite="test_suite",
            metadata={"key": "value"},
        )

        assert context.validation_suite == "test_suite"
        assert context.metadata["key"] == "value"
        assert context.timestamp is not None

    def test_context_with_default_timestamp(self):
        """Test context has default timestamp."""
        context = ValidationContext(validation_suite="test")

        assert context.timestamp is not None
        from datetime import datetime

        assert isinstance(context.timestamp, datetime)


# =============================================================================
# Integration Tests
# =============================================================================


class TestIntegration:
    """Integration tests for validation suite."""

    def test_comprehensive_validation_suite(self, sample_records, invalid_records):
        """Test a comprehensive validation suite."""
        suite = create_validation_suite(
            name="customer_data_quality",
            checks={
                "unique_ids": expect.column("id").to_be_unique(),
                "valid_ages": expect.column("age").to_be_between(0, 120),
                "valid_emails": expect.column("email").to_match_regex(r"^[\\w\\.-]+@"),
                "name_not_null": expect.column("name").to_not_be_null(),
                "id_not_null": expect.column("id").to_not_be_null(),
            },
            strategy=ValidationStrategy.COLLECT_ALL,
            description="Customer data quality checks",
        )

        # Test with valid data
        valid_result = suite.validate(sample_records)
        assert valid_result.success is True

        # Test with invalid data
        invalid_result = suite.validate(invalid_records)
        assert invalid_result.success is False
        assert len(invalid_result.failed_checks) > 0

        # Get summary
        summary = invalid_result.get_summary()
        assert "FAILED" in summary
        assert "Total checks: 5" in summary
