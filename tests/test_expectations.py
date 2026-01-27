"""
Tests for expectations framework and built-in schema expectations.

This module tests ExpectationLibrary, ExpectationSuite, and related utilities,
as well as the library of pre-built expectations for common schema validations.
"""

from typing import Any

import pytest

from vibe_piper import (
    DataType,
    Expectation,
    ExpectationLibrary,
    ExpectationSuite,
    FailureStrategy,
    Schema,
    ValidationResult,
    compose_expectations,
    create_parameterized_expectation,
    expect,
)
from vibe_piper.expectations import (
    expect_column_constraint_to_equal,
    expect_column_to_be_non_nullable,
    expect_column_to_be_nullable,
    expect_column_to_be_optional,
    expect_column_to_be_required,
    expect_column_to_exist,
    expect_column_to_have_constraint,
    expect_column_to_not_exist,
    expect_column_type_to_be,
    expect_table_column_count_to_be_between,
    expect_table_column_count_to_equal,
    expect_table_columns_to_contain,
    expect_table_columns_to_match_set,
    expect_table_columns_to_not_contain,
)


class TestExpectationLibrary:
    """Tests for ExpectationLibrary."""

    def test_library_creation(self) -> None:
        """Test creating an expectation library."""

        library = ExpectationLibrary(name="test_lib")
        assert library.name == "test_lib"
        assert len(library) == 0

    def test_register_expectation_no_args(self) -> None:
        """Test registering an expectation with @library.register()."""

        library = ExpectationLibrary(name="test_lib")

        @library.register()
        def expect_positive(value: Any) -> bool:
            """Value must be positive."""
            return isinstance(value, (int, float)) and value > 0

        assert "expect_positive" in library
        exp = library.get("expect_positive")
        assert exp is not None
        assert exp.name == "expect_positive"
        assert exp.description == "Value must be positive."

    def test_register_expectation_with_name(self) -> None:
        """Test registering an expectation with custom name."""

        library = ExpectationLibrary(name="test_lib")

        @library.register("custom_name")
        def my_function(value: Any) -> bool:
            return value is not None

        assert "custom_name" in library
        assert "my_function" not in library

    def test_register_expectation_with_parameters(self) -> None:
        """Test registering an expectation with parameters."""

        library = ExpectationLibrary(name="test_lib")

        @library.register(severity="warning", description="Custom description")
        def expect_not_null(value: Any) -> bool:
            return value is not None

        exp = library.get("expect_not_null")
        assert exp is not None
        assert exp.severity == "warning"
        assert exp.description == "Custom description"

    def test_add_expectation(self) -> None:
        """Test adding an expectation to a library."""

        library = ExpectationLibrary(name="test_lib")

        @expect
        def expect_not_null(value: Any) -> bool:
            return value is not None

        library.add(expect_not_null)
        assert "expect_not_null" in library

    def test_add_duplicate_expectation_raises(self) -> None:
        """Test that adding duplicate expectation raises error."""

        library = ExpectationLibrary(name="test_lib")

        @expect
        def expect_not_null(value: Any) -> bool:
            return value is not None

        library.add(expect_not_null)

        with pytest.raises(ValueError, match="already registered"):
            library.add(expect_not_null)

    def test_get_expectation(self) -> None:
        """Test retrieving an expectation from a library."""

        library = ExpectationLibrary(name="test_lib")

        @expect
        def expect_positive(value: Any) -> bool:
            return isinstance(value, (int, float)) and value > 0

        library.add(expect_positive)

        retrieved = library.get("expect_positive")
        assert retrieved is not None
        assert retrieved.name == "expect_positive"

    def test_get_nonexistent_expectation(self) -> None:
        """Test retrieving a non-existent expectation returns None."""

        library = ExpectationLibrary(name="test_lib")
        assert library.get("nonexistent") is None

    def test_list_expectations(self) -> None:
        """Test listing all expectations in a library."""

        library = ExpectationLibrary(name="test_lib")

        @expect
        def exp1(value: Any) -> bool:
            return value is not None

        @expect
        def exp2(value: Any) -> bool:
            return isinstance(value, int)

        library.add(exp1)
        library.add(exp2)

        expectations = library.list_expectations()
        assert len(expectations) == 2
        assert "exp1" in expectations
        assert "exp2" in expectations

    def test_has_expectation(self) -> None:
        """Test checking if an expectation exists."""

        library = ExpectationLibrary(name="test_lib")

        @expect
        def expect_not_null(value: Any) -> bool:
            return value is not None

        assert "expect_not_null" not in library
        library.add(expect_not_null)
        assert "expect_not_null" in library

    def test_remove_expectation(self) -> None:
        """Test removing an expectation from a library."""

        library = ExpectationLibrary(name="test_lib")

        @expect
        def expect_not_null(value: Any) -> bool:
            return value is not None

        library.add(expect_not_null)
        assert "expect_not_null" in library

        removed = library.remove("expect_not_null")
        assert removed.name == "expect_not_null"
        assert "expect_not_null" not in library

    def test_remove_nonexistent_raises(self) -> None:
        """Test removing non-existent expectation raises KeyError."""

        library = ExpectationLibrary(name="test_lib")

        with pytest.raises(KeyError, match="not found"):
            library.remove("nonexistent")

    def test_clear_expectations(self) -> None:
        """Test clearing all expectations from a library."""

        library = ExpectationLibrary(name="test_lib")

        @expect
        def exp1(value: Any) -> bool:
            return value is not None

        @expect
        def exp2(value: Any) -> bool:
            return isinstance(value, int)

        library.add(exp1)
        library.add(exp2)
        assert len(library) == 2

        library.clear()
        assert len(library) == 0

    def test_iterate_expectations(self) -> None:
        """Test iterating over expectations in a library."""

        library = ExpectationLibrary(name="test_lib")

        @expect
        def exp1(value: Any) -> bool:
            return value is not None

        @expect
        def exp2(value: Any) -> bool:
            return isinstance(value, int)

        library.add(exp1)
        library.add(exp2)

        expectations = list(library)
        assert len(expectations) == 2
        assert all(isinstance(exp, Expectation) for exp in expectations)


class TestExpectationSuite:
    """Tests for ExpectationSuite."""

    def test_suite_creation(self) -> None:
        """Test creating an expectation suite."""

        suite = ExpectationSuite(name="test_suite")
        assert suite.name == "test_suite"
        assert len(suite) == 0

    def test_suite_with_failure_strategy(self) -> None:
        """Test creating suite with different failure strategies."""

        suite_fail_fast = ExpectationSuite(
            name="test", failure_strategy=FailureStrategy.FAIL_FAST
        )
        assert suite_fail_fast.failure_strategy == FailureStrategy.FAIL_FAST

        suite_collect = ExpectationSuite(
            name="test", failure_strategy=FailureStrategy.COLLECT_ALL
        )
        assert suite_collect.failure_strategy == FailureStrategy.COLLECT_ALL

    def test_add_expectation(self) -> None:
        """Test adding an expectation to a suite."""

        suite = ExpectationSuite(name="test_suite")

        @expect
        def expect_positive(value: Any) -> bool:
            return isinstance(value, (int, float)) and value > 0

        suite.add_expectation(expect_positive)
        assert len(suite) == 1

    def test_add_expectations(self) -> None:
        """Test adding multiple expectations to a suite."""

        suite = ExpectationSuite(name="test_suite")

        @expect
        def exp1(value: Any) -> bool:
            return value is not None

        @expect
        def exp2(value: Any) -> bool:
            return isinstance(value, int)

        suite.add_expectations([exp1, exp2])
        assert len(suite) == 2

    def test_add_duplicate_expectation_raises(self) -> None:
        """Test that adding duplicate expectation raises error."""

        suite = ExpectationSuite(name="test_suite")

        @expect
        def expect_not_null(value: Any) -> bool:
            return value is not None

        suite.add_expectation(expect_not_null)

        with pytest.raises(ValueError, match="already in suite"):
            suite.add_expectation(expect_not_null)

    def test_validate_success(self) -> None:
        """Test validation with all passing expectations."""

        suite = ExpectationSuite(name="test_suite")

        @expect
        def expect_positive(value: Any) -> bool:
            return isinstance(value, (int, float)) and value > 0

        @expect
        def expect_not_null(value: Any) -> bool:
            return value is not None

        suite.add_expectations([expect_positive, expect_not_null])

        result = suite.validate(42)
        assert result.success
        assert len(result.failed_expectations) == 0

    def test_validate_failure(self) -> None:
        """Test validation with failing expectations."""

        suite = ExpectationSuite(name="test_suite")

        @expect
        def expect_positive(value: Any) -> bool:
            return isinstance(value, (int, float)) and value > 0

        @expect
        def expect_not_null(value: Any) -> bool:
            return value is not None

        suite.add_expectations([expect_positive, expect_not_null])

        result = suite.validate(-5)
        assert not result.success
        assert "expect_positive" in result.failed_expectations
        assert len(result.errors) > 0

    def test_validate_collect_all(self) -> None:
        """Test that COLLECT_ALL strategy runs all expectations."""

        suite = ExpectationSuite(
            name="test_suite", failure_strategy=FailureStrategy.COLLECT_ALL
        )

        @expect
        def expect_positive(value: Any) -> bool:
            return isinstance(value, (int, float)) and value > 0

        @expect
        def expect_not_null(value: Any) -> bool:
            return value is not None

        suite.add_expectations([expect_positive, expect_not_null])

        result = suite.validate(None)
        # Both should fail
        assert not result.success
        assert len(result.failed_expectations) == 2
        assert "expect_positive" in result.failed_expectations
        assert "expect_not_null" in result.failed_expectations

    def test_validate_with_warnings(self) -> None:
        """Test validation that produces warnings."""

        suite = ExpectationSuite(name="test_suite")

        @expect
        def expect_with_warning(value: Any) -> ValidationResult:
            if value is None:
                return ValidationResult(
                    is_valid=True, warnings=("Value is None but allowed",)
                )
            return ValidationResult(is_valid=True)

        suite.add_expectation(expect_with_warning)

        result = suite.validate(None)
        assert result.success
        assert len(result.warnings) > 0
        assert "expect_with_warning" in result.warning_expectations

    def test_get_failure_details(self) -> None:
        """Test getting detailed failure information."""

        suite = ExpectationSuite(name="test_suite")

        @expect
        def expect_positive(value: Any) -> bool:
            return isinstance(value, (int, float)) and value > 0

        suite.add_expectation(expect_positive)

        result = suite.validate(-5)
        details = result.get_failure_details()

        assert "expect_positive" in details
        assert len(details["expect_positive"]) > 0

    def test_remove_expectation(self) -> None:
        """Test removing an expectation from a suite."""

        suite = ExpectationSuite(name="test_suite")

        @expect
        def expect_not_null(value: Any) -> bool:
            return value is not None

        suite.add_expectation(expect_not_null)
        assert "expect_not_null" in suite

        suite.remove_expectation("expect_not_null")
        assert "expect_not_null" not in suite

    def test_get_expectation(self) -> None:
        """Test retrieving an expectation from a suite."""

        suite = ExpectationSuite(name="test_suite")

        @expect
        def expect_not_null(value: Any) -> bool:
            return value is not None

        suite.add_expectation(expect_not_null)

        retrieved = suite.get_expectation("expect_not_null")
        assert retrieved is not None
        assert retrieved.name == "expect_not_null"


class TestComposeExpectations:
    """Tests for compose_expectations utility."""

    def test_compose_with_and_logic(self) -> None:
        """Test composing expectations with AND logic."""

        @expect
        def expect_not_null(value: Any) -> bool:
            return value is not None

        @expect
        def expect_positive(value: Any) -> bool:
            return isinstance(value, (int, float)) and value > 0

        composed = compose_expectations(
            "expect_not_null_and_positive",
            expect_not_null,
            expect_positive,
            logic="and",
        )

        # Both pass
        result = composed.validate(42)
        assert result.is_valid

        # One fails
        result = composed.validate(None)
        assert not result.is_valid

        # Other fails
        result = composed.validate(-5)
        assert not result.is_valid

    def test_compose_with_or_logic(self) -> None:
        """Test composing expectations with OR logic."""

        @expect
        def expect_string(value: Any) -> bool:
            return isinstance(value, str)

        @expect
        def expect_int(value: Any) -> bool:
            return isinstance(value, int)

        composed = compose_expectations(
            "expect_string_or_int", expect_string, expect_int, logic="or"
        )

        # First passes
        result = composed.validate("hello")
        assert result.is_valid

        # Second passes
        result = composed.validate(42)
        assert result.is_valid

        # Neither passes
        result = composed.validate(3.14)
        assert not result.is_valid

    def test_compose_with_invalid_logic(self) -> None:
        """Test composing with invalid logic raises error."""

        @expect
        def expect_not_null(value: Any) -> bool:
            return value is not None

        with pytest.raises(ValueError, match="Invalid composition logic"):
            compose_expectations("test", expect_not_null, logic="invalid")


class TestCreateParameterizedExpectation:
    """Tests for create_parameterized_expectation utility."""

    def test_create_parameterized_expectation(self) -> None:
        """Test creating a parameterized expectation."""

        def is_valid_status(data: Any, status: str) -> bool:
            return data == status

        exp = create_parameterized_expectation(
            name="expect_status_active",
            validation_fn=lambda d: is_valid_status(d, "active"),
            parameter_name="status",
            parameter_values=["active"],
            description="Status must be active",
        )

        assert exp.name == "expect_status_active"

        # Should pass
        result = exp.validate("active")
        assert result.is_valid

        # Should fail
        result = exp.validate("inactive")
        assert not result.is_valid

    def test_parameterized_expectation_metadata(self) -> None:
        """Test that parameterized expectations store parameter info."""

        exp = create_parameterized_expectation(
            name="test",
            validation_fn=lambda d: True,
            parameter_name="test_param",
            parameter_values=["value1", "value2"],
        )

        assert "parameter_name" in exp.metadata
        assert exp.metadata["parameter_name"] == "test_param"
        assert "parameter_values" in exp.config


class TestExpectationIntegration:
    """Integration tests for expectations framework."""

    def test_library_and_suite_integration(self) -> None:
        """Test using library and suite together."""

        library = ExpectationLibrary(name="data_quality")

        @library.register()
        def expect_not_null(value: Any) -> bool:
            """Value must not be null."""
            return value is not None

        @library.register()
        def expect_positive(value: Any) -> bool:
            """Value must be positive."""
            return isinstance(value, (int, float)) and value > 0

        # Create suite with expectations from library
        suite = ExpectationSuite(name="quality_checks")
        # Convert library to list of expectations
        expectations_list = list(library)
        suite.add_expectations(expectations_list)

        result = suite.validate(42)
        assert result.success

    def test_expectation_with_validation_result(self) -> None:
        """Test expectations that return ValidationResult."""

        library = ExpectationLibrary(name="test_lib")

        @library.register()
        def expect_detailed_validation(
            value: Any,
        ) -> ValidationResult:
            """Validate with detailed result."""
            if value is None:
                return ValidationResult(
                    is_valid=False, errors=("Value cannot be None",)
                )
            if isinstance(value, int) and value < 0:
                return ValidationResult(
                    is_valid=False, errors=("Value must be non-negative",)
                )
            return ValidationResult(is_valid=True)

        suite = ExpectationSuite(name="detailed_suite")
        suite.add_expectation(library.get("expect_detailed_validation"))  # type: ignore

        result = suite.validate(None)
        assert not result.success
        assert "Value cannot be None" in result.errors

    def test_suite_with_mixed_severities(self) -> None:
        """Test suite with expectations of different severities."""

        suite = ExpectationSuite(name="mixed_severity")

        @expect(severity="error")
        def expect_critical(value: Any) -> bool:
            return value is not None

        @expect(severity="warning")
        def expect_optional(value: Any) -> bool:
            return isinstance(value, str) and len(value) > 0

        suite.add_expectations([expect_critical, expect_optional])

        # Critical fails
        result = suite.validate(None)
        assert not result.success
        assert "expect_critical" in result.failed_expectations

        # Only optional fails - severity is metadata, failures are failures
        result = suite.validate("")
        # The expectation has severity="warning" but validation still fails
        assert not result.success
        assert "expect_optional" in result.failed_expectations
        # Check that the severity is stored correctly
        exp_optional = suite.get_expectation("expect_optional")
        assert exp_optional is not None
        assert exp_optional.severity == "warning"

        # All pass
        result = suite.validate("valid")
        assert result.success


class TestColumnExistenceExpectations:
    """Tests for column existence expectations."""

    def test_expect_column_to_exist_valid(self, sample_schema: Schema) -> None:
        """Test expecting an existing column."""
        expectation = expect_column_to_exist("email")
        assert isinstance(expectation, Expectation)
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_column_to_exist_invalid(self, sample_schema: Schema) -> None:
        """Test expecting a non-existent column."""
        expectation = expect_column_to_exist("address")
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "does not exist" in result.errors[0]

    def test_expect_column_to_not_exist_valid(self, sample_schema: Schema) -> None:
        """Test expecting a column to not exist."""
        expectation = expect_column_to_not_exist("password")
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_column_to_not_exist_invalid(self, sample_schema: Schema) -> None:
        """Test expecting an existing column to not exist."""
        expectation = expect_column_to_not_exist("email")
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "should not exist" in result.errors[0]


# =============================================================================
# Column Type Tests
# =============================================================================


class TestColumnTypeExpectations:
    """Tests for column type expectations."""

    def test_expect_column_type_to_be_valid(self, sample_schema: Schema) -> None:
        """Test expecting correct column type."""
        expectation = expect_column_type_to_be("id", DataType.INTEGER)
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_column_type_to_be_invalid(self, sample_schema: Schema) -> None:
        """Test expecting incorrect column type."""
        expectation = expect_column_type_to_be("id", DataType.STRING)
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "has type INTEGER" in result.errors[0]
        assert "expected STRING" in result.errors[0]

    def test_expect_column_type_to_be_nonexistent_column(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting type on non-existent column."""
        expectation = expect_column_type_to_be("missing", DataType.STRING)
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "does not exist" in result.errors[0]


# =============================================================================
# Column Count Tests
# =============================================================================


class TestColumnCountExpectations:
    """Tests for column count expectations."""

    def test_expect_table_column_count_to_equal_valid(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting correct column count."""
        expectation = expect_table_column_count_to_equal(4)
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_table_column_count_to_equal_invalid(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting incorrect column count."""
        expectation = expect_table_column_count_to_equal(5)
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "Expected 5 columns, but found 4" in result.errors[0]

    def test_expect_table_column_count_to_be_between_valid(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting column count within range."""
        expectation = expect_table_column_count_to_be_between(3, 5)
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_table_column_count_to_be_between_too_few(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting column count within range (too few)."""
        expectation = expect_table_column_count_to_be_between(5, 10)
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "between 5 and 10" in result.errors[0]
        assert "but found 4" in result.errors[0]

    def test_expect_table_column_count_to_be_between_too_many(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting column count within range (too many)."""
        expectation = expect_table_column_count_to_be_between(1, 3)
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "between 1 and 3" in result.errors[0]
        assert "but found 4" in result.errors[0]


# =============================================================================
# Column Set Tests
# =============================================================================


class TestColumnSetExpectations:
    """Tests for column set expectations."""

    def test_expect_table_columns_to_match_set_valid(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting exact column set match."""
        expectation = expect_table_columns_to_match_set({"id", "email", "age", "name"})
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_table_columns_to_match_set_missing(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting exact column set with missing columns."""
        expectation = expect_table_columns_to_match_set({"id", "email", "password"})
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert any("Missing columns" in err for err in result.errors)
        assert any("Unexpected columns" in err for err in result.errors)

    def test_expect_table_columns_to_contain_valid(self, sample_schema: Schema) -> None:
        """Test expecting schema to contain columns."""
        expectation = expect_table_columns_to_contain({"id", "email"})
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_table_columns_to_contain_invalid(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting schema to contain columns (missing)."""
        expectation = expect_table_columns_to_contain({"id", "password"})
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "Missing required columns" in result.errors[0]
        assert "password" in result.errors[0]

    def test_expect_table_columns_to_not_contain_valid(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting schema to not contain columns."""
        expectation = expect_table_columns_to_not_contain({"password", "ssn"})
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_table_columns_to_not_contain_invalid(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting schema to not contain columns (found)."""
        expectation = expect_table_columns_to_not_contain({"id", "password"})
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "Found forbidden columns" in result.errors[0]
        assert "id" in result.errors[0]


# =============================================================================
# Column Property Tests
# =============================================================================


class TestColumnPropertyExpectations:
    """Tests for column property expectations."""

    def test_expect_column_to_be_required_valid(self, sample_schema: Schema) -> None:
        """Test expecting a required column."""
        expectation = expect_column_to_be_required("id")
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_column_to_be_required_invalid(self, sample_schema: Schema) -> None:
        """Test expecting a required column (optional field)."""
        expectation = expect_column_to_be_required("age")
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "is optional, expected required" in result.errors[0]

    def test_expect_column_to_be_optional_valid(self, sample_schema: Schema) -> None:
        """Test expecting an optional column."""
        expectation = expect_column_to_be_optional("age")
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_column_to_be_optional_invalid(self, sample_schema: Schema) -> None:
        """Test expecting an optional column (required field)."""
        expectation = expect_column_to_be_optional("id")
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "is required, expected optional" in result.errors[0]

    def test_expect_column_to_be_nullable_valid(self, sample_schema: Schema) -> None:
        """Test expecting a nullable column."""
        expectation = expect_column_to_be_nullable("age")
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_column_to_be_nullable_invalid(self, sample_schema: Schema) -> None:
        """Test expecting a nullable column (non-nullable field)."""
        expectation = expect_column_to_be_nullable("id")
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "does not allow null values" in result.errors[0]

    def test_expect_column_to_be_non_nullable_valid(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting a non-nullable column."""
        expectation = expect_column_to_be_non_nullable("id")
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_column_to_be_non_nullable_invalid(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting a non-nullable column (nullable field)."""
        expectation = expect_column_to_be_non_nullable("age")
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "allows null values, expected non-nullable" in result.errors[0]


# =============================================================================
# Constraint Tests
# =============================================================================


class TestConstraintExpectations:
    """Tests for constraint expectations."""

    def test_expect_column_to_have_constraint_valid(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting a column to have a constraint."""
        expectation = expect_column_to_have_constraint("email", "max_length")
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_column_to_have_constraint_invalid(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting a column to have a constraint (missing)."""
        expectation = expect_column_to_have_constraint("id", "max_length")
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "does not have constraint 'max_length'" in result.errors[0]

    def test_expect_column_constraint_to_equal_valid(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting a constraint value to match."""
        expectation = expect_column_constraint_to_equal("email", "max_length", 255)
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_column_constraint_to_equal_invalid_value(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting a constraint value to match (incorrect)."""
        expectation = expect_column_constraint_to_equal("email", "max_length", 100)
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "has value 255" in result.errors[0]
        assert "expected 100" in result.errors[0]

    def test_expect_column_constraint_to_equal_missing_constraint(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting a constraint value to match (constraint missing)."""
        expectation = expect_column_constraint_to_equal("id", "max_length", 255)
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "does not have constraint 'max_length'" in result.errors[0]


# =============================================================================
# Integration Tests
# =============================================================================


class TestIntegrationExpectations:
    """Integration tests for expectations."""

    def test_multiple_expectations_composed(self, sample_schema: Schema) -> None:
        """Test using multiple expectations together."""
        expectations = [
            expect_column_to_exist("id"),
            expect_column_type_to_be("id", DataType.INTEGER),
            expect_column_to_be_required("id"),
            expect_column_to_be_non_nullable("id"),
        ]

        for expectation in expectations:
            result = expectation.validate(sample_schema)
            assert (
                result.is_valid is True
            ), f"{expectation.name} failed: {result.errors}"

    def test_expectation_with_declarative_schema(self) -> None:
        """Test expectations with declarative schema definitions."""
        from vibe_piper import Integer, String, define_schema

        @define_schema
        class UserSchema:
            id: Integer = Integer()
            email: String = String(max_length=255)

        schema = UserSchema.to_schema()  # type: ignore[attr-defined]

        # Test column existence
        expectation = expect_column_to_exist("email")
        result = expectation.validate(schema)
        assert result.is_valid

        # Test column type
        expectation = expect_column_type_to_be("id", DataType.INTEGER)
        result = expectation.validate(schema)
        assert result.is_valid

    def test_expectation_name_and_description(self) -> None:
        """Test that expectations have proper names and descriptions."""
        expectation = expect_column_to_exist("email")

        assert expectation.name == "expect_column_to_exist(email)"
        assert expectation.description is not None
        assert "email" in expectation.description
        assert "exist" in expectation.description
        assert expectation.severity == "error"

    def test_expectation_metadata(self) -> None:
        """Test that expectations can have metadata."""
        expectation = expect_column_to_exist("email")

        # Expectations should have metadata dict
        assert isinstance(expectation.metadata, dict)
        assert isinstance(expectation.config, dict)
