"""
Validation decorators for Vibe Piper assets.

This module provides the @validate decorator and @expect enhancements
for declarative validation of asset data.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from functools import wraps
from typing import Any, ParamSpec, TypeVar

from vibe_piper.types import DataRecord, Schema, ValidationResult

P = ParamSpec("P")
T = TypeVar("T")


# =============================================================================
# Validation Decorator
# =============================================================================


@dataclass(frozen=True)
class ValidationConfig:
    """
    Configuration for asset validation.

    Attributes:
        schema: Optional schema to validate against
        checks: List of validation check functions to run
        lazy: If True, collect all validation errors before failing
        on_failure: Action to take on validation failure ('raise', 'warn', 'ignore')
        severity: Severity level for validation failures
    """

    schema: Schema | None = None
    checks: tuple[Callable[[Sequence[DataRecord]], ValidationResult], ...] = field(
        default_factory=tuple
    )
    lazy: bool = True
    on_failure: str = "raise"  # 'raise', 'warn', 'ignore'
    severity: str = "error"


class ValidateDecorator:
    """
    Decorator class for validating asset data.

    Supports both @validate and @validate(...) patterns.

    Example:
        Use with schema validation::

            @asset
            @validate(schema=MySchema)
            def my_data():
                return records

        Use with custom checks::

            @asset
            @validate(checks=[expect_column_values_to_be_unique("id")])
            def my_data():
                return records

        Use with lazy validation (collect all errors)::

            @asset
            @validate(lazy=True)
            @expect.column_values_match_regex("email", r"^[\\w\\.-]+@")
            @expect.column_values_between("age", 0, 120)
            def customers():
                return records
    """

    def __call__(
        self,
        func_or_schema: Callable[P, T] | Schema | ValidationConfig | None = None,
        **kwargs: Any,
    ) -> Callable[P, T] | Callable[[Callable[P, T]], Callable[P, T]]:
        """
        Decorator to validate data returned by a function.

        Can be used as:
        - @validate (without configuration)
        - @validate(schema=MySchema)
        - @validate(checks=[...])
        - @validate(lazy=True, on_failure='warn')

        Args:
            func_or_schema: Either the function to decorate, a Schema, or ValidationConfig
            **kwargs: Additional configuration parameters

        Returns:
            Decorated function with validation
        """
        # Extract configuration from kwargs
        schema = kwargs.pop("schema", None)
        checks = kwargs.pop("checks", None)
        lazy = kwargs.pop("lazy", True)
        on_failure = kwargs.pop("on_failure", "raise")
        severity = kwargs.pop("severity", "error")

        # Create ValidationConfig
        if isinstance(func_or_schema, ValidationConfig):
            config = func_or_schema
        elif isinstance(func_or_schema, Schema):
            config = ValidationConfig(schema=func_or_schema)
        else:
            config = ValidationConfig(
                schema=schema,
                checks=tuple(checks) if checks else (),
                lazy=lazy,
                on_failure=on_failure,
                severity=severity,
            )

        # Case 1: @validate (no parentheses, no config) - just wrap the function
        if callable(func_or_schema) and not kwargs:
            # Empty validation config
            return self._wrap_function(func_or_schema, ValidationConfig())

        # Case 2: @validate(...) - return a decorator function
        def decorator(func: Callable[P, T]) -> Callable[P, T]:
            return self._wrap_function(func, config)

        return decorator

    def _wrap_function(self, func: Callable[P, T], config: ValidationConfig) -> Callable[P, T]:
        """Wrap a function with validation logic."""

        @wraps(func)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
            # Call the original function
            result = func(*args, **kwargs)

            # Only validate if result is a sequence of DataRecords
            if isinstance(result, Sequence) and len(result) > 0:
                if isinstance(result[0], DataRecord):
                    self._validate_data(result, config, func.__name__)

            return result

        return wrapped

    def _validate_data(
        self,
        records: Sequence[DataRecord],
        config: ValidationConfig,
        func_name: str,
    ) -> None:
        """Validate data against the configuration."""
        errors: list[str] = []
        warnings: list[str] = []

        # Run schema validation if configured
        if config.schema is not None:
            schema_errors: list[str] = []
            for idx, record in enumerate(records):
                try:
                    # Validate record against schema
                    # DataRecord already validates in __post_init__
                    if record.schema != config.schema:
                        schema_errors.append(
                            f"Record {idx}: schema mismatch (expected {config.schema.name}, "
                            f"got {record.schema.name})"
                        )
                except ValueError as e:
                    schema_errors.append(f"Record {idx}: {e}")

            if schema_errors:
                errors.append(f"Schema validation failed: {len(schema_errors)} errors")
                errors.extend(schema_errors[:10])  # First 10 errors
                if len(schema_errors) > 10:
                    warnings.append(f"... and {len(schema_errors) - 10} more schema errors")

                if not config.lazy:
                    self._handle_validation_failure(errors, warnings, config, func_name)
                    return

        # Run custom validation checks if configured
        if config.checks:
            for check_idx, check_fn in enumerate(config.checks):
                try:
                    result = check_fn(records)
                    if not result.is_valid:
                        errors.extend(result.errors)
                        warnings.extend(result.warnings)

                        if not config.lazy:
                            self._handle_validation_failure(errors, warnings, config, func_name)
                            return
                except Exception as e:
                    error_msg = f"Check {check_idx} failed with exception: {e}"
                    errors.append(error_msg)

                    if not config.lazy:
                        self._handle_validation_failure(errors, warnings, config, func_name)
                        return

        # Handle accumulated errors if lazy validation
        if errors:
            self._handle_validation_failure(errors, warnings, config, func_name)

    def _handle_validation_failure(
        self,
        errors: list[str],
        warnings: list[str],
        config: ValidationConfig,
        func_name: str,
    ) -> None:
        """Handle validation failure based on configuration."""

        error_summary = f"Validation failed for function '{func_name}':\n" + "\n".join(errors)

        if warnings:
            error_summary += "\nWarnings:\n" + "\n".join(warnings)

        if config.on_failure == "raise":
            raise ValueError(error_summary)
        elif config.on_failure == "warn":
            import warnings as py_warnings

            py_warnings.warn(error_summary, stacklevel=2)
        elif config.on_failure == "ignore":
            pass
        else:
            msg = f"Invalid on_failure value: {config.on_failure}"
            raise ValueError(msg)


# Create the validate decorator instance
validate = ValidateDecorator()


# =============================================================================
# Expect Decorator Enhancements
# =============================================================================


class ExpectationBuilder:
    """
    Fluent builder for creating expectations.

    Provides a chainable API for creating validation expectations.

    Example:
        >>> expect.column("email").to_match_regex(r"^[\\w\\.-]+@")
        >>> expect.column("age").to_be_between(0, 120)
        >>> expect.columns(["a", "b"]).to_be_equal()
    """

    def __init__(self) -> None:
        """Initialize the expectation builder."""

    def column(self, name: str) -> ColumnExpectationBuilder:
        """
        Create expectations for a single column.

        Args:
            name: Column name

        Returns:
            ColumnExpectationBuilder for chaining
        """
        return ColumnExpectationBuilder(name)

    def columns(self, names: list[str]) -> MultiColumnExpectationBuilder:
        """
        Create expectations for multiple columns.

        Args:
            names: List of column names

        Returns:
            MultiColumnExpectationBuilder for chaining
        """
        return MultiColumnExpectationBuilder(names)

    def table(self) -> TableExpectationBuilder:
        """
        Create table-level expectations.

        Returns:
            TableExpectationBuilder for chaining
        """
        return TableExpectationBuilder()


class ColumnExpectationBuilder:
    """Builder for single-column expectations."""

    def __init__(self, column: str) -> None:
        """Initialize with column name."""
        self.column = column

    def to_match_regex(self, pattern: str, match_percentage: float = 1.0):
        """Expect column values to match regex pattern."""
        from vibe_piper.validation.checks import expect_column_values_to_match_regex

        return expect_column_values_to_match_regex(self.column, pattern, match_percentage)

    def to_not_match_regex(self, pattern: str):
        """Expect column values to NOT match regex pattern."""
        from vibe_piper.validation.checks import expect_column_values_to_not_match_regex

        return expect_column_values_to_not_match_regex(self.column, pattern)

    def to_be_between(self, min_value: float, max_value: float):
        """Expect column values to be within a range."""
        from vibe_piper.validation.checks import expect_column_values_to_be_between

        return expect_column_values_to_be_between(self.column, min_value, max_value)

    def to_be_in_set(self, value_set: set[Any]):
        """Expect column values to be in a set."""
        from vibe_piper.validation.checks import expect_column_values_to_be_in_set

        return expect_column_values_to_be_in_set(self.column, value_set)

    def to_not_be_in_set(self, forbidden_set: set[Any]):
        """Expect column values to NOT be in a set."""
        from vibe_piper.validation.checks import expect_column_values_to_not_be_in_set

        return expect_column_values_to_not_be_in_set(self.column, forbidden_set)

    def to_be_unique(self, ignore_nulls: bool = True):
        """Expect column values to be unique."""
        from vibe_piper.validation.checks import expect_column_values_to_be_unique

        return expect_column_values_to_be_unique(self.column, ignore_nulls)

    def to_be_of_type(self, expected_type):
        """Expect column values to be of a specific type."""
        from vibe_piper.validation.checks import expect_column_values_to_be_of_type

        return expect_column_values_to_be_of_type(self.column, expected_type)

    def to_not_be_null(self, allow_empty_strings: bool = False):
        """Expect column values to not be null."""
        from vibe_piper.validation.checks import expect_column_values_to_not_be_null

        return expect_column_values_to_not_be_null(self.column, allow_empty_strings)

    def value_lengths_to_be_between(self, min_length: int, max_length: int):
        """Expect column string lengths to be within a range."""
        from vibe_piper.validation.checks import expect_column_value_lengths_to_be_between

        return expect_column_value_lengths_to_be_between(self.column, min_length, max_length)

    def to_be_increasing(self, strict: bool = False):
        """Expect column values to be monotonically increasing."""
        from vibe_piper.validation.checks import expect_column_values_to_be_increasing

        return expect_column_values_to_be_increasing(self.column, strict)

    def to_be_decreasing(self, strict: bool = False):
        """Expect column values to be monotonically decreasing."""
        from vibe_piper.validation.checks import expect_column_values_to_be_decreasing

        return expect_column_values_to_be_decreasing(self.column, strict)

    def mean_to_be_between(self, min_value: float, max_value: float):
        """Expect column mean to be within a range."""
        from vibe_piper.validation.checks import expect_column_mean_to_be_between

        return expect_column_mean_to_be_between(self.column, min_value, max_value)

    def std_dev_to_be_between(self, min_value: float, max_value: float):
        """Expect column std dev to be within a range."""
        from vibe_piper.validation.checks import expect_column_std_dev_to_be_between

        return expect_column_std_dev_to_be_between(self.column, min_value, max_value)

    def min_to_be_between(self, min_value: float, max_value: float):
        """Expect column minimum to be within a range."""
        from vibe_piper.validation.checks import expect_column_min_to_be_between

        return expect_column_min_to_be_between(self.column, min_value, max_value)

    def max_to_be_between(self, min_value: float, max_value: float):
        """Expect column maximum to be within a range."""
        from vibe_piper.validation.checks import expect_column_max_to_be_between

        return expect_column_max_to_be_between(self.column, min_value, max_value)

    def median_to_be_between(self, min_value: float, max_value: float):
        """Expect column median to be within a range."""
        from vibe_piper.validation.checks import expect_column_median_to_be_between

        return expect_column_median_to_be_between(self.column, min_value, max_value)

    def proportion_of_nulls_to_be_between(self, min_value: float, max_value: float):
        """Expect proportion of nulls to be within a range."""
        from vibe_piper.validation.checks import (
            expect_column_proportion_of_nulls_to_be_between,
        )

        return expect_column_proportion_of_nulls_to_be_between(self.column, min_value, max_value)

    def to_be_dateutil_parseable(self):
        """Expect column values to be parseable as dates."""
        from vibe_piper.validation.checks import expect_column_values_to_be_dateutil_parseable

        return expect_column_values_to_be_dateutil_parseable(self.column)


class MultiColumnExpectationBuilder:
    """Builder for multi-column expectations."""

    def __init__(self, columns: list[str]) -> None:
        """Initialize with column names."""
        self.columns = columns

    def to_be_equal(self):
        """Expect all columns to have equal values."""
        if len(self.columns) != 2:
            msg = "to_be_equal requires exactly 2 columns"
            raise ValueError(msg)
        from vibe_piper.validation.checks import expect_column_pair_values_to_be_equal

        return expect_column_pair_values_to_be_equal(self.columns[0], self.columns[1])

    def to_not_be_equal(self):
        """Expect all columns to have different values."""
        if len(self.columns) != 2:
            msg = "to_not_be_equal requires exactly 2 columns"
            raise ValueError(msg)
        from vibe_piper.validation.checks import expect_column_pair_values_to_be_not_equal

        return expect_column_pair_values_to_be_not_equal(self.columns[0], self.columns[1])

    def a_to_be_greater_than_b(self, or_equal: bool = False):
        """Expect first column to be greater than second."""
        if len(self.columns) != 2:
            msg = "a_to_be_greater_than_b requires exactly 2 columns"
            raise ValueError(msg)
        from vibe_piper.validation.checks import (
            expect_column_pair_values_a_to_be_greater_than_b,
        )

        return expect_column_pair_values_a_to_be_greater_than_b(
            self.columns[0], self.columns[1], or_equal
        )

    def sum_to_be_equal(self, tolerance: float = 0.0):
        """Expect sum of first column to equal sum of second column."""
        if len(self.columns) != 2:
            msg = "sum_to_be_equal requires exactly 2 columns"
            raise ValueError(msg)
        from vibe_piper.validation.checks import (
            expect_column_sum_to_equal_other_column_sum,
        )

        return expect_column_sum_to_equal_other_column_sum(
            self.columns[0], self.columns[1], tolerance
        )


class TableExpectationBuilder:
    """Builder for table-level expectations."""

    def row_count_to_be_between(self, min_value: int, max_value: int):
        """Expect table row count to be within a range."""
        from vibe_piper.validation.checks import expect_table_row_count_to_be_between

        return expect_table_row_count_to_be_between(min_value, max_value)

    def row_count_to_equal(self, expected_value: int):
        """Expect table row count to equal a specific value."""
        from vibe_piper.validation.checks import expect_table_row_count_to_equal

        return expect_table_row_count_to_equal(expected_value)


# Create the expect builder instance
expect = ExpectationBuilder()


# =============================================================================
# Re-exports
# =============================================================================

__all__ = [
    "validate",
    "expect",
    "ValidationConfig",
    "ValidateDecorator",
    "ExpectationBuilder",
    "ColumnExpectationBuilder",
    "MultiColumnExpectationBuilder",
    "TableExpectationBuilder",
]
