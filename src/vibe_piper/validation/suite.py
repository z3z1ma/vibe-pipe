"""
Validation suite framework for organizing and running validation checks.

This module provides utilities for organizing validation checks into suites,
with support for different execution strategies (lazy vs. fail-fast).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from vibe_piper.types import DataRecord, ValidationResult

# =============================================================================
# Validation Strategy
# =============================================================================


class ValidationStrategy:
    """Strategy for how to handle validation failures."""

    FAIL_FAST = "fail_fast"  # Stop on first failure
    COLLECT_ALL = "collect_all"  # Run all checks, collect all failures
    CONTINUE_ON_WARNING = "continue_on_warning"  # Stop on error, continue on warning


# =============================================================================
# Validation Context
# =============================================================================


@dataclass(frozen=True)
class ValidationContext:
    """
    Context information for a validation run.

    Attributes:
        validation_suite: Name of the validation suite
        timestamp: When the validation was run
        metadata: Additional context metadata
    """

    validation_suite: str
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: dict[str, Any] = field(default_factory=dict)


# =============================================================================
# Validation Result with Details
# =============================================================================


@dataclass(frozen=True)
class SuiteValidationResult:
    """
    Result of running a validation suite.

    Attributes:
        success: Whether all validations passed
        check_results: Mapping of check name to validation result
        failed_checks: Names of checks that failed
        warning_checks: Names of checks that produced warnings
        total_checks: Total number of checks run
        total_records: Total number of records validated
        errors: All error messages from failed checks
        warnings: All warning messages from checks
        duration_ms: Time taken to run the validation suite
        context: Validation context information
    """

    success: bool
    check_results: dict[str, ValidationResult] = field(default_factory=dict)
    failed_checks: tuple[str, ...] = field(default_factory=tuple)
    warning_checks: tuple[str, ...] = field(default_factory=tuple)
    total_checks: int = 0
    total_records: int = 0
    errors: tuple[str, ...] = field(default_factory=tuple)
    warnings: tuple[str, ...] = field(default_factory=tuple)
    duration_ms: float = 0.0
    context: ValidationContext | None = None

    def get_failure_details(self) -> dict[str, tuple[str, ...]]:
        """
        Get detailed failure information per check.

        Returns:
            Dict mapping check name to its error messages
        """
        details: dict[str, tuple[str, ...]] = {}
        for check_name in self.failed_checks:
            result = self.check_results.get(check_name)
            if result and result.errors:
                details[check_name] = result.errors
        return details

    def get_summary(self) -> str:
        """
        Get a human-readable summary of the validation result.

        Returns:
            Summary string
        """
        status = "✓ PASSED" if self.success else "✗ FAILED"
        lines = [
            f"Validation Result: {status}",
            f"  Total checks: {self.total_checks}",
            f"  Passed: {self.total_checks - len(self.failed_checks)}",
            f"  Failed: {len(self.failed_checks)}",
            f"  Warnings: {len(self.warning_checks)}",
            f"  Total records: {self.total_records}",
            f"  Duration: {self.duration_ms:.2f}ms",
        ]

        if self.failed_checks:
            lines.append("\nFailed checks:")
            for check_name in self.failed_checks:
                lines.append(f"  - {check_name}")

        if self.errors:
            lines.append("\nErrors:")
            for error in self.errors[:5]:
                lines.append(f"  - {error}")
            if len(self.errors) > 5:
                lines.append(f"  ... and {len(self.errors) - 5} more errors")

        if self.warnings:
            lines.append("\nWarnings:")
            for warning in self.warnings[:3]:
                lines.append(f"  - {warning}")
            if len(self.warnings) > 3:
                lines.append(f"  ... and {len(self.warnings) - 3} more warnings")

        return "\n".join(lines)


# =============================================================================
# Validation Suite
# =============================================================================


class LazyValidationStrategy:
    """
    Lazy validation strategy that collects all errors before failing.

    This is the default strategy and is useful for getting a complete
    picture of all data quality issues in a single run.
    """

    def should_stop(self, result: ValidationResult) -> bool:
        """Determine if validation should stop on this result."""
        return False  # Never stop, collect all errors


class FailFastValidationStrategy:
    """
    Fail-fast validation strategy that stops on first failure.

    This is useful for quick validation where you want to fail fast
    on the first error encountered.
    """

    def should_stop(self, result: ValidationResult) -> bool:
        """Determine if validation should stop on this result."""
        return not result.is_valid


class ValidationSuite:
    """
    A collection of validation checks that can be run together.

    ValidationSuite allows you to group multiple validation checks and
    run them against data. Supports different execution strategies
    (lazy vs. fail-fast) and provides detailed results.

    Example:
        Create and run a validation suite::

            suite = ValidationSuite(name="customer_data_quality")

            suite.add_check(
                "unique_emails",
                expect_column_values_to_be_unique("email")
            )
            suite.add_check(
                "valid_ages",
                expect_column_values_to_be_between("age", 0, 120)
            )

            result = suite.validate(customer_records)
            print(result.get_summary())

    Attributes:
        name: Unique name for this suite
        strategy: Execution strategy ('collect_all' or 'fail_fast')
        description: Optional description
        metadata: Additional metadata
    """

    def __init__(
        self,
        name: str,
        strategy: str = ValidationStrategy.COLLECT_ALL,
        description: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Initialize a ValidationSuite.

        Args:
            name: Unique name for this suite
            strategy: Execution strategy ('collect_all' or 'fail_fast')
            description: Optional description
            metadata: Additional metadata
        """
        if strategy not in (
            ValidationStrategy.FAIL_FAST,
            ValidationStrategy.COLLECT_ALL,
            ValidationStrategy.CONTINUE_ON_WARNING,
        ):
            msg = f"Invalid strategy: {strategy!r}"
            raise ValueError(msg)

        self.name = name
        self.strategy = strategy
        self.description = description
        self.metadata = dict(metadata) if metadata else {}
        self._checks: dict[str, Callable[[Sequence[DataRecord]], ValidationResult]] = {}

    def add_check(
        self,
        name: str,
        check_fn: Callable[[Sequence[DataRecord]], ValidationResult],
    ) -> ValidationSuite:
        """
        Add a validation check to this suite.

        Args:
            name: Unique name for the check
            check_fn: Function that validates data and returns ValidationResult

        Returns:
            Self for method chaining

        Raises:
            ValueError: If a check with the same name already exists
        """
        if name in self._checks:
            msg = f"Check '{name}' already exists in suite '{self.name}'"
            raise ValueError(msg)
        self._checks[name] = check_fn
        return self

    def add_checks(
        self,
        checks: dict[str, Callable[[Sequence[DataRecord]], ValidationResult]],
    ) -> ValidationSuite:
        """
        Add multiple checks to this suite.

        Args:
            checks: Dictionary mapping check names to check functions

        Returns:
            Self for method chaining
        """
        for name, check_fn in checks.items():
            self.add_check(name, check_fn)
        return self

    def remove_check(self, name: str) -> Callable[[Sequence[DataRecord]], ValidationResult]:
        """
        Remove a check from this suite.

        Args:
            name: Name of the check to remove

        Returns:
            The removed check function

        Raises:
            KeyError: If the check doesn't exist
        """
        if name not in self._checks:
            msg = f"Check '{name}' not found in suite '{self.name}'"
            raise KeyError(msg)
        return self._checks.pop(name)

    def get_check(self, name: str) -> Callable[[Sequence[DataRecord]], ValidationResult] | None:
        """
        Get a check by name.

        Args:
            name: Name of the check

        Returns:
            The check function if found, None otherwise
        """
        return self._checks.get(name)

    def list_checks(self) -> tuple[str, ...]:
        """
        List all check names in this suite.

        Returns:
            Tuple of check names
        """
        return tuple(self._checks.keys())

    def validate(
        self,
        records: Sequence[DataRecord],
        context: ValidationContext | None = None,
    ) -> SuiteValidationResult:
        """
        Validate data against all checks in this suite.

        Args:
            records: Records to validate
            context: Optional validation context

        Returns:
            SuiteValidationResult with detailed results
        """
        import time

        start_time = time.time()
        check_results: dict[str, ValidationResult] = {}
        failed: list[str] = []
        warning_checks: list[str] = []
        all_errors: list[str] = []
        all_warnings: list[str] = []

        # Create context if not provided
        if context is None:
            context = ValidationContext(validation_suite=self.name)

        # Run all checks
        for check_name, check_fn in self._checks.items():
            try:
                result = check_fn(records)
                check_results[check_name] = result

                # Collect warnings
                if result.warnings:
                    all_warnings.extend(result.warnings)
                    warning_checks.append(check_name)

                # Check if failed
                if not result.is_valid:
                    failed.append(check_name)
                    all_errors.extend(result.errors)

                    # Handle failure strategy
                    if self.strategy == ValidationStrategy.FAIL_FAST:
                        break
                    elif self.strategy == ValidationStrategy.CONTINUE_ON_WARNING:
                        # Stop on error (not warning)
                        if check_name not in warning_checks:
                            break

            except Exception as e:
                error_msg = f"Check '{check_name}' failed with exception: {e}"
                all_errors.append(error_msg)
                failed.append(check_name)
                check_results[check_name] = ValidationResult(is_valid=False, errors=(error_msg,))

                # Stop on exception for fail-fast
                if self.strategy == ValidationStrategy.FAIL_FAST:
                    break

        # Calculate duration
        duration_ms = (time.time() - start_time) * 1000

        # Determine overall success
        success = len(failed) == 0

        return SuiteValidationResult(
            success=success,
            check_results=check_results,
            failed_checks=tuple(failed),
            warning_checks=tuple(warning_checks),
            total_checks=len(self._checks),
            total_records=len(records),
            errors=tuple(all_errors),
            warnings=tuple(all_warnings),
            duration_ms=duration_ms,
            context=context,
        )

    def __len__(self) -> int:
        """Return the number of checks in this suite."""
        return len(self._checks)

    def __contains__(self, name: str) -> bool:
        """Check if a check exists in this suite."""
        return name in self._checks

    def __repr__(self) -> str:
        """Return string representation."""
        return f"ValidationSuite(name='{self.name}', checks={len(self._checks)})"


# =============================================================================
# Convenience Functions
# =============================================================================


def create_validation_suite(
    name: str,
    checks: dict[str, Callable[[Sequence[DataRecord]], ValidationResult]] | None = None,
    strategy: str = ValidationStrategy.COLLECT_ALL,
    description: str | None = None,
) -> ValidationSuite:
    """
    Create a validation suite with the specified checks.

    Args:
        name: Name for the suite
        checks: Dictionary of check names to check functions
        strategy: Execution strategy
        description: Optional description

    Returns:
        ValidationSuite instance

    Example:
        >>> suite = create_validation_suite(
        ...     name="my_checks",
        ...     checks={
        ...         "unique_ids": expect_column_values_to_be_unique("id"),
        ...         "valid_ages": expect_column_values_to_be_between("age", 0, 120),
        ...     }
        ... )
    """
    suite = ValidationSuite(name=name, strategy=strategy, description=description)
    if checks:
        suite.add_checks(checks)
    return suite


# =============================================================================
# Re-exports
# =============================================================================

__all__ = [
    "ValidationSuite",
    "ValidationStrategy",
    "LazyValidationStrategy",
    "FailFastValidationStrategy",
    "ValidationContext",
    "SuiteValidationResult",
    "create_validation_suite",
]
