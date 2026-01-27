"""
Expectation management framework for Vibe Piper.

This module provides utilities for organizing, managing, and executing
custom data quality expectations. Users can create their own expectation
libraries, group them into suites, and apply them to data in pipelines.
"""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from vibe_piper.types import Expectation, ValidationResult

# =============================================================================
# Expectation Library - Registry for Custom Expectations
# =============================================================================


class ExpectationLibrary:
    """
    A registry for organizing and managing custom expectations.

    ExpectationLibrary provides a central place to register, organize,
    and retrieve custom expectations. Expectations can be grouped by
    namespace/category for better organization.

    Example:
        Create and populate a custom expectation library::

            library = ExpectationLibrary(name="my_company_expectations")

            @library.register("email")
            def expect_valid_email(data: Any) -> ValidationResult:
                '''Validate email format.'''
                if isinstance(data, str) and "@" in data:
                    return ValidationResult(is_valid=True)
                return ValidationResult(
                    is_valid=False,
                    errors=(f"Invalid email: {data}",)
                )

            # Later, retrieve the expectation
            email_expectation = library.get("email")
    """

    def __init__(
        self,
        name: str,
        description: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        """
        Initialize an ExpectationLibrary.

        Args:
            name: Unique name for this library
            description: Optional description of the library
            metadata: Additional metadata about the library
        """
        self.name = name
        self.description = description
        self.metadata = dict(metadata) if metadata else {} if metadata else {}
        self._expectations: dict[str, Expectation] = {}

    def register(
        self,
        name: str | None = None,
        severity: str = "error",
        description: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> Callable[[Callable[[Any], ValidationResult | bool]], Expectation]:
        """
        Decorator to register an expectation function in this library.

        Can be used as:
        - @library.register()
        - @library.register("custom_name")
        - @library.register(name="foo", severity="warning", ...)

        Args:
            name: Custom name for the expectation (defaults to function name)
            severity: Severity level ('error', 'warning', 'info')
            description: Optional description (defaults to function docstring)
            metadata: Optional metadata
            config: Optional configuration

        Returns:
            Decorator function that registers the expectation

        Example:
            Register an expectation::

                @library.register()
                def expect_positive(value: Any) -> bool:
                    '''Value must be positive.'''
                    return isinstance(value, (int, float)) and value > 0
        """

        def decorator(func: Callable[[Any], ValidationResult | bool]) -> Expectation:
            from vibe_piper.decorators import _create_expectation_from_function

            expectation_name = name or func.__name__

            # Create the expectation using the helper function
            expectation = _create_expectation_from_function(
                func=func,
                name=expectation_name,
                severity=severity,
                description=description,
                metadata=dict(metadata) if metadata else None,
                config=dict(config) if config else None,
            )

            # Register in the library
            self._expectations[expectation_name] = expectation
            return expectation

        return decorator

    def add(self, expectation: Expectation) -> "ExpectationLibrary":
        """
        Add an expectation to this library.

        Args:
            expectation: The expectation to add

        Returns:
            Self for method chaining

        Raises:
            ValueError: If an expectation with the same name already exists
        """
        if expectation.name in self._expectations:
            msg = f"Expectation '{expectation.name}' already registered in library '{self.name}'"
            raise ValueError(msg)
        self._expectations[expectation.name] = expectation
        return self

    def get(self, name: str) -> Expectation | None:
        """
        Get an expectation by name.

        Args:
            name: Name of the expectation to retrieve

        Returns:
            The expectation if found, None otherwise
        """
        return self._expectations.get(name)

    def list_expectations(self) -> tuple[str, ...]:
        """
        List all expectation names in this library.

        Returns:
            Tuple of expectation names
        """
        return tuple(self._expectations.keys())

    def has(self, name: str) -> bool:
        """
        Check if an expectation exists in this library.

        Args:
            name: Name of the expectation

        Returns:
            True if the expectation exists, False otherwise
        """
        return name in self._expectations

    def remove(self, name: str) -> Expectation:
        """
        Remove an expectation from this library.

        Args:
            name: Name of the expectation to remove

        Returns:
            The removed expectation

        Raises:
            KeyError: If the expectation doesn't exist
        """
        if name not in self._expectations:
            msg = f"Expectation '{name}' not found in library '{self.name}'"
            raise KeyError(msg)
        return self._expectations.pop(name)

    def clear(self) -> None:
        """Remove all expectations from this library."""
        self._expectations.clear()

    def __len__(self) -> int:
        """Return the number of expectations in this library."""
        return len(self._expectations)

    def __contains__(self, name: str) -> bool:
        """Check if an expectation exists in this library."""
        return name in self._expectations

    def __iter__(self):
        """Iterate over all expectations in this library."""
        return iter(self._expectations.values())


# =============================================================================
# Expectation Suite - Group and Run Multiple Expectations
# =============================================================================


class FailureStrategy:
    """Strategy for handling expectation failures in a suite."""

    FAIL_FAST = "fail_fast"  # Stop on first failure
    COLLECT_ALL = "collect_all"  # Run all, collect all failures
    CONTINUE_ON_WARNING = "continue_on_warning"  # Stop on error, continue on warning


@dataclass(frozen=True)
class SuiteResult:
    """
    Result of running an expectation suite.

    Attributes:
        success: Whether all expectations passed
        expectation_results: Mapping of expectation name to its validation result
        failed_expectations: Names of expectations that failed
        warning_expectations: Names of expectations that produced warnings
        total_expectations: Total number of expectations run
        errors: All error messages from failed expectations
        warnings: All warning messages from expectations
    """

    success: bool
    expectation_results: Mapping[str, ValidationResult] = field(default_factory=dict)
    failed_expectations: tuple[str, ...] = field(default_factory=tuple)
    warning_expectations: tuple[str, ...] = field(default_factory=tuple)
    total_expectations: int = 0
    errors: tuple[str, ...] = field(default_factory=tuple)
    warnings: tuple[str, ...] = field(default_factory=tuple)

    def get_failure_details(self) -> dict[str, tuple[str, ...]]:
        """
        Get detailed failure information per expectation.

        Returns:
            Dict mapping expectation name to its error messages
        """
        details: dict[str, tuple[str, ...]] = {}
        for exp_name in self.failed_expectations:
            result = self.expectation_results.get(exp_name)
            if result and result.errors:
                details[exp_name] = result.errors
        return details


class ExpectationSuite:
    """
    A collection of expectations that can be run together.

    ExpectationSuite allows you to group multiple expectations and
    validate data against all of them. Supports configurable failure
    strategies and provides detailed results.

    Example:
        Create and run an expectation suite::

            suite = ExpectationSuite(name="data_quality_checks")

            @expect
            def expect_not_null(data: Any) -> bool:
                return data is not None

            suite.add_expectation(expect_not_null)

            # Run the suite
            result = suite.validate(my_data)
            if result.success:
                print("All checks passed!")
    """

    def __init__(
        self,
        name: str,
        failure_strategy: str = FailureStrategy.COLLECT_ALL,
        description: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        """
        Initialize an ExpectationSuite.

        Args:
            name: Unique name for this suite
            failure_strategy: How to handle failures ('fail_fast', 'collect_all',
                             or 'continue_on_warning')
            description: Optional description
            metadata: Additional metadata
        """
        if failure_strategy not in (
            FailureStrategy.FAIL_FAST,
            FailureStrategy.COLLECT_ALL,
            FailureStrategy.CONTINUE_ON_WARNING,
        ):
            msg = f"Invalid failure strategy: {failure_strategy!r}"
            raise ValueError(msg)

        self.name = name
        self.failure_strategy = failure_strategy
        self.description = description
        self.metadata = dict(metadata) if metadata else {}
        self._expectations: dict[str, Expectation] = {}

    def add_expectation(self, expectation: Expectation) -> "ExpectationSuite":
        """
        Add an expectation to this suite.

        Args:
            expectation: The expectation to add

        Returns:
            Self for method chaining

        Raises:
            ValueError: If an expectation with the same name already exists
        """
        if expectation.name in self._expectations:
            msg = f"Expectation '{expectation.name}' already in suite '{self.name}'"
            raise ValueError(msg)
        self._expectations[expectation.name] = expectation
        return self

    def add_expectations(
        self, expectations: Sequence[Expectation]
    ) -> "ExpectationSuite":
        """
        Add multiple expectations to this suite.

        Args:
            expectations: Sequence of expectations to add

        Returns:
            Self for method chaining
        """
        for exp in expectations:
            self.add_expectation(exp)
        return self

    def remove_expectation(self, name: str) -> Expectation:
        """
        Remove an expectation from this suite.

        Args:
            name: Name of the expectation to remove

        Returns:
            The removed expectation

        Raises:
            KeyError: If the expectation doesn't exist
        """
        if name not in self._expectations:
            msg = f"Expectation '{name}' not found in suite '{self.name}'"
            raise KeyError(msg)
        return self._expectations.pop(name)

    def get_expectation(self, name: str) -> Expectation | None:
        """
        Get an expectation by name.

        Args:
            name: Name of the expectation

        Returns:
            The expectation if found, None otherwise
        """
        return self._expectations.get(name)

    def list_expectations(self) -> tuple[str, ...]:
        """
        List all expectation names in this suite.

        Returns:
            Tuple of expectation names
        """
        return tuple(self._expectations.keys())

    def validate(self, data: Any) -> SuiteResult:
        """
        Validate data against all expectations in this suite.

        Args:
            data: The data to validate

        Returns:
            SuiteResult with detailed results
        """
        results: dict[str, ValidationResult] = {}
        failed: list[str] = []
        warning_expectations: list[str] = []
        all_errors: list[str] = []
        all_warnings: list[str] = []

        for exp_name, expectation in self._expectations.items():
            result = expectation.validate(data)
            results[exp_name] = result

            # Collect warnings
            if result.warnings:
                all_warnings.extend(result.warnings)
                warning_expectations.append(exp_name)

            # Check if failed
            if not result.is_valid:
                failed.append(exp_name)
                all_errors.extend(result.errors)

                # Handle failure strategy
                if self.failure_strategy == FailureStrategy.FAIL_FAST or (
                    self.failure_strategy == FailureStrategy.CONTINUE_ON_WARNING
                    and expectation.severity == "error"
                ):
                    break

        # Determine overall success
        success = len(failed) == 0

        return SuiteResult(
            success=success,
            expectation_results=results,
            failed_expectations=tuple(failed),
            warning_expectations=tuple(warning_expectations),
            total_expectations=len(self._expectations),
            errors=tuple(all_errors),
            warnings=tuple(all_warnings),
        )

    def __len__(self) -> int:
        """Return the number of expectations in this suite."""
        return len(self._expectations)

    def __contains__(self, name: str) -> bool:
        """Check if an expectation exists in this suite."""
        return name in self._expectations


# =============================================================================
# Helper Functions for Creating Custom Expectations
# =============================================================================


def create_parameterized_expectation(
    name: str,
    validation_fn: Callable[[Any], ValidationResult | bool],
    parameter_name: str,
    parameter_values: Sequence[Any],
    severity: str = "error",
    description: str | None = None,
) -> Expectation:
    """
    Create multiple parameterized expectations from a single validation function.

    This is useful when you want to create similar expectations for different
    parameter values (e.g., expecting a column to be one of several valid values).

    Args:
        name: Base name for the expectations (will be appended with parameter value)
        validation_fn: Function that takes (data, parameter) and returns validation result
        parameter_name: Name of the parameter (for documentation)
        parameter_values: List of parameter values to create expectations for
        severity: Severity level for all created expectations
        description: Optional description template

    Returns:
        A single Expectation (note: this is simplified; in practice you might
        want to return multiple expectations)

    Example:
        Create parameterized expectations for valid status values::

            def is_valid_status(data: Any, status: str) -> bool:
                return data == status

            expectations = []
            for status in ["active", "pending", "closed"]:
                exp = create_parameterized_expectation(
                    name=f"expect_status_{status}",
                    validation_fn=lambda d: is_valid_status(d, status),
                    parameter_name="status",
                    parameter_values=[status],
                    description=f"Status must be {status}"
                )
                expectations.append(exp)
    """
    from vibe_piper.decorators import _create_expectation_from_function

    def wrapped_fn(data: Any) -> ValidationResult | bool:
        return validation_fn(data)

    return _create_expectation_from_function(
        func=wrapped_fn,
        name=name,
        severity=severity,
        description=description,
        metadata={"parameter_name": parameter_name},
        config={"parameter_values": list(parameter_values)},
    )


def compose_expectations(
    name: str,
    *expectations: Expectation,
    logic: str = "and",
) -> Expectation:
    """
    Compose multiple expectations into a single expectation.

    Args:
        name: Name for the composed expectation
        *expectations: Expectations to compose
        logic: Composition logic ('and' or 'or')

    Returns:
        A new Expectation that combines the input expectations

    Example:
        Create a composed expectation::

            @expect
            def expect_not_null(data: Any) -> bool:
                return data is not None

            @expect
            def expect_positive(data: Any) -> bool:
                return isinstance(data, (int, float)) and data > 0

            # Both conditions must pass
            expect_not_null_and_positive = compose_expectations(
                "expect_not_null_and_positive",
                expect_not_null,
                expect_positive,
                logic="and"
            )
    """
    if logic not in ("and", "or"):
        msg = f"Invalid composition logic: {logic!r}. Must be 'and' or 'or'"
        raise ValueError(msg)

    def composed_fn(data: Any) -> ValidationResult:
        results = [exp.validate(data) for exp in expectations]

        if logic == "and":
            # All must pass
            all_errors: list[str] = []
            all_warnings: list[str] = []

            for result in results:
                if not result.is_valid:
                    all_errors.extend(result.errors)
                all_warnings.extend(result.warnings)

            is_valid = all(r.is_valid for r in results)
            return ValidationResult(
                is_valid=is_valid,
                errors=tuple(all_errors) if not is_valid else (),
                warnings=tuple(all_warnings),
            )
        else:
            # At least one must pass
            any_valid = any(r.is_valid for r in results)
            if any_valid:
                return ValidationResult(is_valid=True)

            # Collect all errors since none passed
            all_errors: list[str] = []
            for result in results:
                all_errors.extend(result.errors)

            return ValidationResult(
                is_valid=False,
                errors=tuple(all_errors),
            )

    return Expectation(
        name=name,
        fn=composed_fn,
        description=f"Composed expectation using {logic} logic",
        severity="error",
        metadata={"composed_of": [exp.name for exp in expectations]},
    )
