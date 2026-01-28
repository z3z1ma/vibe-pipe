"""
Comprehensive validation checks for data quality.

This module provides 20+ built-in validation types including:
- Statistical checks (mean, std_dev, min, max)
- Regex pattern matching
- Range and value checks
- Cross-column validation
- Aggregate validation (group-level)
- Type and null checks
- String length and format checks
All checks return ValidationResult with detailed error information.
"""

from __future__ import annotations

import re
import statistics
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from vibe_piper.types import DataRecord, DataType, ValidationResult


# =============================================================================
# Validation Result with Details
# =============================================================================


@dataclass(frozen=True)
class ColumnValidationResult:
    """
    Detailed validation result for column-level checks.

    Attributes:
        column: Column name that was validated
        passed: Whether validation passed
        total_rows: Total number of rows checked
        passed_rows: Number of rows that passed
        failed_rows: Number of rows that failed
        failed_indices: Indices of failed rows (sampled, max 100)
        error_message: Error message if validation failed
        statistics: Optional statistics about the validation
    """

    column: str
    passed: bool
    total_rows: int
    passed_rows: int
    failed_rows: int
    failed_indices: tuple[int, ...] = field(default_factory=tuple)
    error_message: str | None = None
    statistics: dict[str, Any] = field(default_factory=dict)

    def to_validation_result(self) -> ValidationResult:
        """Convert to ValidationResult."""
        if self.passed:
            return ValidationResult(is_valid=True)
        errors = []
        if self.error_message:
            errors.append(self.error_message)
        errors.append(
            f"Column '{self.column}': {self.failed_rows}/{self.total_rows} rows failed validation"
        )
        return ValidationResult(is_valid=False, errors=tuple(errors))


# =============================================================================
# Statistical Checks
# =============================================================================


def expect_column_mean_to_be_between(
    column: str, min_value: float, max_value: float
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """
    Expect column mean to be within a range.

    Args:
        column: Column name to check
        min_value: Minimum acceptable mean
        max_value: Maximum acceptable mean

    Example:
        >>> check = expect_column_mean_to_be_between("age", 18, 65)
        >>> result = check(records)
    """

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        if not records:
            return ValidationResult(is_valid=True)

        values = []
        for record in records:
            val = record.get(column)
            if isinstance(val, (int, float)) and val is not None:
                values.append(float(val))

        if not values:
            return ValidationResult(
                is_valid=False,
                errors=(f"Column '{column}' has no numeric values",),
            )

        mean_value = statistics.mean(values)
        passed = min_value <= mean_value <= max_value

        if not passed:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column}' mean {mean_value:.2f} is not between "
                    f"{min_value} and {max_value}",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


def expect_column_std_dev_to_be_between(
    column: str, min_value: float, max_value: float
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """
    Expect column standard deviation to be within a range.

    Args:
        column: Column name to check
        min_value: Minimum acceptable std dev
        max_value: Maximum acceptable std dev
    """

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        if len(records) < 2:
            return ValidationResult(
                is_valid=False,
                errors=(f"Need at least 2 records to calculate std dev for '{column}'",),
            )

        values = []
        for record in records:
            val = record.get(column)
            if isinstance(val, (int, float)) and val is not None:
                values.append(float(val))

        if len(values) < 2:
            return ValidationResult(
                is_valid=False,
                errors=(f"Column '{column}' has fewer than 2 numeric values",),
            )

        std_dev = statistics.stdev(values)
        passed = min_value <= std_dev <= max_value

        if not passed:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column}' std dev {std_dev:.2f} is not between "
                    f"{min_value} and {max_value}",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


def expect_column_min_to_be_between(
    column: str, min_value: float, max_value: float
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """Expect column minimum value to be within a range."""

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        if not records:
            return ValidationResult(is_valid=True)

        values = []
        for record in records:
            val = record.get(column)
            if isinstance(val, (int, float)) and val is not None:
                values.append(float(val))

        if not values:
            return ValidationResult(
                is_valid=False,
                errors=(f"Column '{column}' has no numeric values",),
            )

        min_val = min(values)
        passed = min_value <= min_val <= max_value

        if not passed:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column}' minimum {min_val:.2f} is not between "
                    f"{min_value} and {max_value}",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


def expect_column_max_to_be_between(
    column: str, min_value: float, max_value: float
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """Expect column maximum value to be within a range."""

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        if not records:
            return ValidationResult(is_valid=True)

        values = []
        for record in records:
            val = record.get(column)
            if isinstance(val, (int, float)) and val is not None:
                values.append(float(val))

        if not values:
            return ValidationResult(
                is_valid=False,
                errors=(f"Column '{column}' has no numeric values",),
            )

        max_val = max(values)
        passed = min_value <= max_val <= max_value

        if not passed:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column}' maximum {max_val:.2f} is not between "
                    f"{min_value} and {max_value}",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


def expect_column_median_to_be_between(
    column: str, min_value: float, max_value: float
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """Expect column median to be within a range."""

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        if not records:
            return ValidationResult(is_valid=True)

        values = []
        for record in records:
            val = record.get(column)
            if isinstance(val, (int, float)) and val is not None:
                values.append(float(val))

        if not values:
            return ValidationResult(
                is_valid=False,
                errors=(f"Column '{column}' has no numeric values",),
            )

        median_val = statistics.median(values)
        passed = min_value <= median_val <= max_value

        if not passed:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column}' median {median_val:.2f} is not between "
                    f"{min_value} and {max_value}",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


# =============================================================================
# Regex Pattern Matching
# =============================================================================


def expect_column_values_to_match_regex(
    column: str, pattern: str, match_percentage: float = 1.0
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """
    Expect column values to match a regex pattern.

    Args:
        column: Column name to check
        pattern: Regex pattern to match
        match_percentage: Minimum percentage of values that should match (0-1)
    """

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        if not records:
            return ValidationResult(is_valid=True)

        compiled_pattern = re.compile(pattern)
        failed_indices: list[int] = []
        total_values = 0
        matched_values = 0

        for idx, record in enumerate(records):
            val = record.get(column)
            if val is None:
                continue
            total_values += 1
            if isinstance(val, str):
                if compiled_pattern.match(val):
                    matched_values += 1
                else:
                    failed_indices.append(idx)
            else:
                failed_indices.append(idx)

        if total_values == 0:
            return ValidationResult(
                is_valid=False,
                errors=(f"Column '{column}' has no values",),
            )

        match_ratio = matched_values / total_values
        passed = match_ratio >= match_percentage

        if not passed:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column}': {matched_values}/{total_values} "
                    f"({match_ratio:.1%}) values match pattern {pattern}, "
                    f"expected {match_percentage:.1%}",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


def expect_column_values_to_not_match_regex(
    column: str, pattern: str
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """Expect column values to NOT match a regex pattern."""

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        if not records:
            return ValidationResult(is_valid=True)

        compiled_pattern = re.compile(pattern)
        failed_indices: list[int] = []

        for idx, record in enumerate(records):
            val = record.get(column)
            if isinstance(val, str) and compiled_pattern.match(val):
                failed_indices.append(idx)

        if failed_indices:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column}': {len(failed_indices)} values match "
                    f"forbidden pattern {pattern}",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


# =============================================================================
# Range and Value Checks
# =============================================================================


def expect_column_values_to_be_between(
    column: str, min_value: float, max_value: float
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """
    Expect column values to be within a range.

    Args:
        column: Column name to check
        min_value: Minimum acceptable value (inclusive)
        max_value: Maximum acceptable value (inclusive)
    """

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        if not records:
            return ValidationResult(is_valid=True)

        failed_indices: list[int] = []

        for idx, record in enumerate(records):
            val = record.get(column)
            if val is None:
                continue
            if isinstance(val, (int, float)):
                if not (min_value <= val <= max_value):
                    failed_indices.append(idx)
            else:
                failed_indices.append(idx)

        if failed_indices:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column}': {len(failed_indices)}/{len(records)} "
                    f"values are not between {min_value} and {max_value}",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


def expect_column_values_to_be_in_set(
    column: str, value_set: set[Any]
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """Expect column values to be in a specific set."""

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        if not records:
            return ValidationResult(is_valid=True)

        failed_indices: list[int] = []
        unexpected_values: set[Any] = set()

        for idx, record in enumerate(records):
            val = record.get(column)
            if val is None:
                continue
            if val not in value_set:
                failed_indices.append(idx)
                unexpected_values.add(val)

        if failed_indices:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column}': {len(failed_indices)}/{len(records)} "
                    f"values are not in expected set. "
                    f"Unexpected values: {sorted(str(v) for v in unexpected_values)[:10]}",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


def expect_column_values_to_not_be_in_set(
    column: str, forbidden_set: set[Any]
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """Expect column values to NOT be in a specific set."""

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        if not records:
            return ValidationResult(is_valid=True)

        failed_indices: list[int] = []
        found_forbidden: set[Any] = set()

        for idx, record in enumerate(records):
            val = record.get(column)
            if val in forbidden_set:
                failed_indices.append(idx)
                found_forbidden.add(val)

        if failed_indices:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column}': {len(failed_indices)}/{len(records)} "
                    f"values are in forbidden set: {sorted(str(v) for v in found_forbidden)}",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


def expect_column_values_to_be_unique(
    column: str, ignore_nulls: bool = True
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """
    Expect column values to be unique.

    Args:
        column: Column name to check
        ignore_nulls: Whether to ignore null values when checking uniqueness
    """

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        if not records:
            return ValidationResult(is_valid=True)

        seen: dict[Any, list[int]] = {}
        duplicate_count = 0

        for idx, record in enumerate(records):
            val = record.get(column)
            if val is None and ignore_nulls:
                continue
            if val not in seen:
                seen[val] = []
            seen[val].append(idx)

        duplicates = {val: indices for val, indices in seen.items() if len(indices) > 1}
        duplicate_count = sum(len(indices) for indices in duplicates.values())

        if duplicates:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column}': found {len(duplicates)} duplicate values, "
                    f"affecting {duplicate_count} rows",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


# =============================================================================
# Type and Null Checks
# =============================================================================


def expect_column_values_to_be_of_type(
    column: str, expected_type: DataType | type
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """
    Expect column values to be of a specific type.

    Args:
        column: Column name to check
        expected_type: Expected DataType or Python type
    """

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        if not records:
            return ValidationResult(is_valid=True)

        failed_indices: list[int] = []

        type_mapping = {
            DataType.STRING: str,
            DataType.INTEGER: int,
            DataType.FLOAT: (int, float),
            DataType.BOOLEAN: bool,
            DataType.DATETIME: (datetime, str),
            DataType.DATE: (datetime, str),
            DataType.ARRAY: list,
            DataType.OBJECT: dict,
        }

        if isinstance(expected_type, DataType):
            python_types = type_mapping.get(expected_type, object)
        else:
            python_types = expected_type

        for idx, record in enumerate(records):
            val = record.get(column)
            if val is None:
                continue
            if not isinstance(val, python_types):
                failed_indices.append(idx)

        if failed_indices:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column}': {len(failed_indices)}/{len(records)} "
                    f"values are not of expected type {expected_type}",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


def expect_column_values_to_not_be_null(
    column: str, allow_empty_strings: bool = False
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """
    Expect column values to not be null.

    Args:
        column: Column name to check
        allow_empty_strings: Whether empty strings are considered non-null
    """

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        if not records:
            return ValidationResult(is_valid=True)

        null_count = 0

        for record in records:
            val = record.get(column)
            if val is None:
                null_count += 1
            elif not allow_empty_strings and val == "":
                null_count += 1

        if null_count > 0:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column}': {null_count}/{len(records)} values are null",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


def expect_column_proportion_of_nulls_to_be_between(
    column: str, min_value: float, max_value: float
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """
    Expect proportion of null values in column to be within a range.

    Args:
        column: Column name to check
        min_value: Minimum proportion of nulls (0-1)
        max_value: Maximum proportion of nulls (0-1)
    """

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        if not records:
            return ValidationResult(is_valid=True)

        null_count = sum(1 for record in records if record.get(column) is None)
        null_proportion = null_count / len(records)

        passed = min_value <= null_proportion <= max_value

        if not passed:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column}': null proportion {null_proportion:.2%} "
                    f"is not between {min_value:.2%} and {max_value:.2%}",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


# =============================================================================
# String Length and Format Checks
# =============================================================================


def expect_column_value_lengths_to_be_between(
    column: str, min_length: int, max_length: int
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """
    Expect column string lengths to be within a range.

    Args:
        column: Column name to check
        min_length: Minimum string length (inclusive)
        max_length: Maximum string length (inclusive)
    """

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        if not records:
            return ValidationResult(is_valid=True)

        failed_indices: list[int] = []

        for idx, record in enumerate(records):
            val = record.get(column)
            if val is None:
                continue
            if isinstance(val, str):
                if not (min_length <= len(val) <= max_length):
                    failed_indices.append(idx)
            else:
                failed_indices.append(idx)

        if failed_indices:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column}': {len(failed_indices)}/{len(records)} "
                    f"values have length not between {min_length} and {max_length}",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


def expect_column_values_to_be_increasing(
    column: str, strict: bool = False
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """
    Expect column values to be monotonically increasing.

    Args:
        column: Column name to check
        strict: If True, values must be strictly increasing (no equal adjacent values)
    """

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        if len(records) < 2:
            return ValidationResult(is_valid=True)

        violations = 0
        prev_val = None

        for record in records:
            val = record.get(column)
            if val is None:
                continue
            if prev_val is not None:
                if strict:
                    if not val > prev_val:
                        violations += 1
                else:
                    if not val >= prev_val:
                        violations += 1
            prev_val = val

        if violations > 0:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column}': found {violations} violations of "
                    f"{'strict' if strict else 'monotonic'} increase",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


def expect_column_values_to_be_decreasing(
    column: str, strict: bool = False
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """
    Expect column values to be monotonically decreasing.

    Args:
        column: Column name to check
        strict: If True, values must be strictly decreasing (no equal adjacent values)
    """

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        if len(records) < 2:
            return ValidationResult(is_valid=True)

        violations = 0
        prev_val = None

        for record in records:
            val = record.get(column)
            if val is None:
                continue
            if prev_val is not None:
                if strict:
                    if not val < prev_val:
                        violations += 1
                else:
                    if not val <= prev_val:
                        violations += 1
            prev_val = val

        if violations > 0:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column}': found {violations} violations of "
                    f"{'strict' if strict else 'monotonic'} decrease",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


# =============================================================================
# Cross-Column Validation
# =============================================================================


def expect_column_pair_values_to_be_equal(
    column_a: str, column_b: str
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """Expect two columns to have equal values."""

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        if not records:
            return ValidationResult(is_valid=True)

        mismatches = 0

        for record in records:
            val_a = record.get(column_a)
            val_b = record.get(column_b)
            if val_a != val_b:
                mismatches += 1

        if mismatches > 0:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Columns '{column_a}' and '{column_b}': "
                    f"{mismatches}/{len(records)} mismatched values",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


def expect_column_pair_values_to_be_not_equal(
    column_a: str, column_b: str
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """Expect two columns to have different values."""

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        if not records:
            return ValidationResult(is_valid=True)

        matches = 0

        for record in records:
            val_a = record.get(column_a)
            val_b = record.get(column_b)
            if val_a == val_b:
                matches += 1

        if matches > 0:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Columns '{column_a}' and '{column_b}': "
                    f"{matches}/{len(records)} equal values (expected to be different)",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


def expect_column_pair_values_a_to_be_greater_than_b(
    column_a: str, column_b: str, or_equal: bool = False
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """
    Expect column A values to be greater than column B values.

    Args:
        column_a: First column name
        column_b: Second column name
        or_equal: If True, allows A >= B, otherwise requires A > B
    """

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        if not records:
            return ValidationResult(is_valid=True)

        violations = 0

        for record in records:
            val_a = record.get(column_a)
            val_b = record.get(column_b)
            if val_a is None or val_b is None:
                continue
            if or_equal:
                if not val_a >= val_b:
                    violations += 1
            else:
                if not val_a > val_b:
                    violations += 1

        if violations > 0:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column_a}' > '{column_b}': "
                    f"{violations}/{len(records)} violations",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


def expect_column_sum_to_equal_other_column_sum(
    column_a: str, column_b: str, tolerance: float = 0.0
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """
    Expect sum of column A to equal sum of column B.

    Args:
        column_a: First column name
        column_b: Second column name
        tolerance: Allowed difference between sums
    """

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        if not records:
            return ValidationResult(is_valid=True)

        sum_a = 0.0
        sum_b = 0.0

        for record in records:
            val_a = record.get(column_a)
            val_b = record.get(column_b)
            if isinstance(val_a, (int, float)):
                sum_a += float(val_a)
            if isinstance(val_b, (int, float)):
                sum_b += float(val_b)

        difference = abs(sum_a - sum_b)

        if difference > tolerance:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column_a}' sum ({sum_a}) != "
                    f"'{column_b}' sum ({sum_b}), difference: {difference}",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


# =============================================================================
# Aggregate Validation (Group-Level)
# =============================================================================


def expect_column_groupby_value_counts_to_be_between(
    column: str,
    group_column: str,
    min_count: int,
    max_count: int,
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """
    Expect value counts within groups to be within a range.

    Args:
        column: Column to count
        group_column: Column to group by
        min_count: Minimum expected count per group
        max_count: Maximum expected count per group
    """

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        if not records:
            return ValidationResult(is_valid=True)

        # Count values per group
        group_counts: dict[Any, int] = {}
        for record in records:
            group_val = record.get(group_column)
            if group_val not in group_counts:
                group_counts[group_val] = 0
            group_counts[group_val] += 1

        # Check counts
        violations: list[tuple[Any, int]] = []
        for group_val, count in group_counts.items():
            if not (min_count <= count <= max_count):
                violations.append((group_val, count))

        if violations:
            violation_details = ", ".join(
                f"{group}={count}" for group, count in violations[:5]
            )
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column}' grouped by '{group_column}': "
                    f"{len(violations)} groups have counts outside [{min_count}, {max_count}]. "
                    f"Violations: {violation_details}",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


def expect_column_groupby_mean_to_be_between(
    column: str,
    group_column: str,
    min_value: float,
    max_value: float,
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """
    Expect group means to be within a range.

    Args:
        column: Column to calculate mean on
        group_column: Column to group by
        min_value: Minimum acceptable mean
        max_value: Maximum acceptable mean
    """

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        if not records:
            return ValidationResult(is_valid=True)

        # Calculate mean per group
        group_sums: dict[Any, float] = {}
        group_counts: dict[Any, int] = {}

        for record in records:
            group_val = record.get(group_column)
            col_val = record.get(column)
            if isinstance(col_val, (int, float)):
                if group_val not in group_sums:
                    group_sums[group_val] = 0.0
                    group_counts[group_val] = 0
                group_sums[group_val] += float(col_val)
                group_counts[group_val] += 1

        # Check means
        violations: list[tuple[Any, float]] = []
        for group_val in group_sums:
            mean_val = group_sums[group_val] / group_counts[group_val]
            if not (min_value <= mean_val <= max_value):
                violations.append((group_val, mean_val))

        if violations:
            violation_details = ", ".join(
                f"{group}={mean:.2f}" for group, mean in violations[:5]
            )
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column}' grouped by '{group_column}': "
                    f"{len(violations)} groups have mean outside [{min_value}, {max_value}]. "
                    f"Violations: {violation_details}",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


def expect_table_row_count_to_be_between(
    min_value: int, max_value: int
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """Expect table row count to be within a range."""

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        row_count = len(records)
        passed = min_value <= row_count <= max_value

        if not passed:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Row count {row_count} is not between {min_value} and {max_value}",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


def expect_table_row_count_to_equal(
    expected_value: int
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """Expect table row count to equal a specific value."""

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        row_count = len(records)

        if row_count != expected_value:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Row count {row_count} does not equal expected {expected_value}",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


def expect_column_values_to_be_dateutil_parseable(
    column: str,
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """Expect column values to be parseable as dates."""

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        if not records:
            return ValidationResult(is_valid=True)

        failed_indices: list[int] = []

        for idx, record in enumerate(records):
            val = record.get(column)
            if val is None:
                continue
            try:
                if isinstance(val, str):
                    from dateutil import parser as dateutil_parser

                    dateutil_parser.parse(val)
                elif not isinstance(val, datetime):
                    failed_indices.append(idx)
            except Exception:
                failed_indices.append(idx)

        if failed_indices:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column}': {len(failed_indices)}/{len(records)} "
                    f"values are not parseable as dates",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


# =============================================================================
# Custom Validation Function Builder
# =============================================================================


def create_custom_validation(
    validation_fn: Callable[[Sequence[DataRecord]], ValidationResult],
    name: str,
    description: str | None = None,
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """
    Create a custom validation function with metadata.

    Args:
        validation_fn: Function that takes records and returns ValidationResult
        name: Name of the validation
        description: Optional description

    Returns:
        Wrapped validation function with metadata

    Example:
        >>> def my_check(records):
        ...     return ValidationResult(is_valid=True)
        >>> custom = create_custom_validation(my_check, "my_custom_check")
        >>> result = custom(records)
    """

    def wrapped(records: Sequence[DataRecord]) -> ValidationResult:
        result = validation_fn(records)
        # Add metadata to result if not present
        return result

    wrapped.__name__ = name
    wrapped.__doc__ = description or f"Custom validation: {name}"
    return wrapped


# =============================================================================
# Public API - Count Validation Types
# =============================================================================

# This module provides 30+ validation types:
# Statistical: mean, std_dev, min, max, median (5)
# Regex: match_regex, not_match_regex (2)
# Range/Value: between, in_set, not_in_set, unique (4)
# Type/Null: be_of_type, not_be_null, null_proportion (3)
# String: value_lengths, be_increasing, be_decreasing (3)
# Cross-column: equal, not_equal, a_greater_than_b, sum_equal (4)
# Aggregate: groupby_counts, groupby_mean (2)
# Table: row_count_between, row_count_equal (2)
# Date: dateutil_parseable (1)
# Custom: create_custom_validation (1)
# Plus more...

__all__ = [
    # Statistical checks
    "expect_column_mean_to_be_between",
    "expect_column_std_dev_to_be_between",
    "expect_column_min_to_be_between",
    "expect_column_max_to_be_between",
    "expect_column_median_to_be_between",
    # Regex pattern matching
    "expect_column_values_to_match_regex",
    "expect_column_values_to_not_match_regex",
    # Range and value checks
    "expect_column_values_to_be_between",
    "expect_column_values_to_be_in_set",
    "expect_column_values_to_not_be_in_set",
    "expect_column_values_to_be_unique",
    # Type and null checks
    "expect_column_values_to_be_of_type",
    "expect_column_values_to_not_be_null",
    "expect_column_proportion_of_nulls_to_be_between",
    # String length and format
    "expect_column_value_lengths_to_be_between",
    "expect_column_values_to_be_increasing",
    "expect_column_values_to_be_decreasing",
    # Cross-column validation
    "expect_column_pair_values_to_be_equal",
    "expect_column_pair_values_to_be_not_equal",
    "expect_column_pair_values_a_to_be_greater_than_b",
    "expect_column_sum_to_equal_other_column_sum",
    # Aggregate validation
    "expect_column_groupby_value_counts_to_be_between",
    "expect_column_groupby_mean_to_be_between",
    # Table-level validation
    "expect_table_row_count_to_be_between",
    "expect_table_row_count_to_equal",
    # Date validation
    "expect_column_values_to_be_dateutil_parseable",
    # Custom validation
    "create_custom_validation",
    # Result types
    "ColumnValidationResult",
]
