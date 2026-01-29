"""
Great Expectations integration for Vibe Piper.

This module provides integration with Great Expectations (GE) for data quality validation.
Includes the @ge_asset decorator and utilities for loading GE suites from YAML.

Note: This module requires the great_expectations package to be installed.
       It is an optional dependency - install with: pip install vibe-piper[ge]

Example usage:

    @ge_asset(suite_path="ge_suites/customers.yaml")
    def customers():
        return load_customers()

    @ge_asset(suite_path="ge_suites/orders.yaml")
    def orders():
        return load_orders()
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any, ParamSpec, TypeVar

# Optional import - GE is not a hard dependency
try:
    import pandas as pd
except ImportError:
    pd = None  # type: ignore
    yaml = None  # type: ignore

from vibe_piper.external_quality.base import (
    QualityToolAdapter,
    QualityToolResult,
    ToolType,
)
from vibe_piper.types import DataRecord, QualityMetric, QualityMetricType

P = ParamSpec("P")
T = TypeVar("T")


# =============================================================================
# GE Adapter Class
# =============================================================================


class GreatExpectationsAdapter(QualityToolAdapter):
    """
    Adapter for Great Expectations validation.

    Loads GE expectations from YAML files, executes validations,
    and converts results to VibePiper format.
    """

    def __init__(self, suite_path: str) -> None:
        """
        Initialize the adapter with a GE suite path.

        Args:
            suite_path: Path to GE suite YAML file
        """
        self.suite_path = suite_path
        self.config = self.load_config(suite_path)

    def load_config(self, config_path: str) -> dict[str, Any]:
        """
        Load GE suite configuration from YAML.

        Args:
            config_path: Path to GE suite YAML file

        Returns:
            Parsed configuration as dictionary
        """
        if yaml is None:
            msg = "PyYAML is required to load GE suites. Install with: pip install pyyaml"
            raise ImportError(msg)

        path = Path(config_path)
        if not path.exists():
            msg = f"GE suite file not found: {config_path}"
            raise FileNotFoundError(msg)

        with path.open() as f:
            return yaml.safe_load(f)  # type: ignore

    def validate(
        self,
        data: Sequence[DataRecord],
        config_path: str,  # noqa: ARG001
    ) -> QualityToolResult:
        """
        Validate data using Great Expectations.

        Args:
            data: Records to validate
            config_path: Path to GE suite YAML file (uses self.suite_path)

        Returns:
            QualityToolResult with validation outcome
        """
        start_time = datetime.now()
        metrics: list[QualityMetric] = []
        errors: list[str] = []
        warnings: list[str] = []

        if not data:
            return QualityToolResult(
                tool_type=ToolType.GREAT_EXPECTATIONS,
                asset_name=self.suite_path,
                passed=True,
                duration_ms=(datetime.now() - start_time).total_seconds() * 1000,
            )

        # Convert DataRecords to pandas DataFrame for GE
        records_dicts = [record.data for record in data]
        df = pd.DataFrame(records_dicts)  # type: ignore

        # Execute GE validations based on config
        config = self.config

        # Check for table expectations
        if "expectations" in config:
            for expectation in config["expectations"]:
                result = self._run_expectation(df, expectation)
                metrics.append(result["metric"])
                if result["error"]:
                    errors.append(result["error"])
                if result["warning"]:
                    warnings.append(result["warning"])

        # Calculate overall pass/fail
        passed = len(errors) == 0

        return QualityToolResult(
            tool_type=ToolType.GREAT_EXPECTATIONS,
            asset_name=Path(self.suite_path).stem,
            passed=passed,
            metrics=tuple(metrics),
            errors=tuple(errors),
            warnings=tuple(warnings),
            raw_result={"config": config, "df_shape": df.shape},
            duration_ms=(datetime.now() - start_time).total_seconds() * 1000,
            timestamp=start_time,
        )

    def _run_expectation(self, df: Any, expectation: dict[str, Any]) -> dict[str, Any]:
        """
        Run a single GE expectation on DataFrame.

        Args:
            df: Pandas DataFrame with data
            expectation: Expectation configuration

        Returns:
            Dictionary with metric, error, and warning
        """
        metric_name = expectation.get("name", "unnamed_expectation")
        expectation_type = expectation.get("type", "")
        column = expectation.get("column")

        error = None
        warning = None
        metric_value = 0.0
        passed = True

        try:
            # Table-level expectations
            if expectation_type == "expect_table_row_count_to_be_between":
                min_rows = expectation.get("min", 0)
                max_rows = expectation.get("max", float("inf"))
                row_count = len(df)

                passed = min_rows <= row_count <= max_rows
                metric_value = row_count
                if not passed:
                    error = f"Table row count {row_count} not in range [{min_rows}, {max_rows}]"

            elif expectation_type == "expect_table_row_count_to_equal":
                expected_count = expectation.get("value", 0)
                row_count = len(df)

                passed = row_count == expected_count
                metric_value = row_count
                if not passed:
                    error = f"Table row count {row_count} != expected {expected_count}"

            # Column-level expectations
            elif column is not None and column in df.columns:
                col = df[column]

                if expectation_type == "expect_column_to_exist":
                    passed = True
                    metric_value = 1.0

                elif expectation_type == "expect_column_values_to_not_be_null":
                    null_count = col.isna().sum()
                    total_count = len(col)
                    metric_value = 1.0 - (null_count / total_count if total_count > 0 else 0)

                    max_nulls = expectation.get("max_nulls", 0)
                    passed = null_count <= max_nulls
                    if not passed:
                        error = (
                            f"Column '{column}' has {null_count} nulls, max allowed: {max_nulls}"
                        )

                elif expectation_type == "expect_column_values_to_be_unique":
                    unique_count = col.nunique()
                    total_count = len(col)
                    metric_value = unique_count / total_count if total_count > 0 else 1.0

                    passed = unique_count == total_count
                    if not passed:
                        warning = f"Column '{column}' has {total_count - unique_count} duplicates"

                elif expectation_type == "expect_column_values_to_be_in_set":
                    value_set = set(expectation.get("value_set", []))
                    invalid_count = (~col.isin(value_set)).sum()
                    metric_value = 1.0 - (invalid_count / len(col) if len(col) > 0 else 0)

                    passed = invalid_count == 0
                    if not passed:
                        error = f"Column '{column}' has {invalid_count} values not in expected set"

                elif expectation_type == "expect_column_values_to_match_regex":
                    import re

                    pattern = expectation.get("regex", "")
                    regex = re.compile(pattern)
                    invalid_count = (~col.astype(str).str.match(regex, na=False)).sum()
                    metric_value = 1.0 - (invalid_count / len(col) if len(col) > 0 else 0)

                    passed = invalid_count == 0
                    if not passed:
                        error = f"Column '{column}' has {invalid_count} values not matching regex"

                elif expectation_type == "expect_column_values_to_be_of_type":
                    expected_type = expectation.get("type", "string")

                    if expected_type == "integer":
                        passed = pd.api.types.is_integer_dtype(col)
                    elif expected_type == "float":
                        passed = pd.api.types.is_float_dtype(col)
                    elif expected_type == "string":
                        passed = pd.api.types.is_string_dtype(col)
                    elif expected_type == "boolean":
                        passed = pd.api.types.is_bool_dtype(col)
                    elif expected_type == "datetime":
                        passed = pd.api.types.is_datetime64_any_dtype(col)
                    else:
                        passed = True
                        warning = f"Unknown type '{expected_type}' for column '{column}'"

                    metric_value = 1.0 if passed else 0.0
                    if not passed and not warning:
                        error = f"Column '{column}' is not of type '{expected_type}'"

            elif column is not None and column not in df.columns:
                error = f"Column '{column}' not found in DataFrame"
                passed = False

            else:
                warning = f"Unknown expectation type: {expectation_type}"
                passed = True

        except Exception as e:
            error = f"Error running expectation '{metric_name}': {e}"
            passed = False

        metric = QualityMetric(
            name=f"ge_{metric_name}",
            metric_type=QualityMetricType.VALIDITY,
            value=metric_value,
            passed=passed,
            description=f"Great Expectations: {metric_name}",
        )

        return {
            "metric": metric,
            "error": error,
            "warning": warning,
        }


# =============================================================================
# GE Decorator
# =============================================================================


@dataclass(frozen=True)
class GEAssetConfig:
    """
    Configuration for @ge_asset decorator.

    Attributes:
        suite_path: Path to GE suite YAML file
        on_failure: Action to take on validation failure
    """

    suite_path: str
    on_failure: str = "raise"  # 'raise', 'warn', 'ignore'


class GEAssetDecorator:
    """
    Decorator class for Great Expectations asset validation.

    Supports both @ge_asset and @ge_asset(...) patterns.

    Example:
        @ge_asset(suite_path="ge_suites/customers.yaml")
        def customers():
            return load_customers()
    """

    def __call__(
        self,
        func_or_config: Callable[P, T] | str | GEAssetConfig | None = None,
        **kwargs: Any,
    ) -> Callable[P, T] | Callable[[Callable[P, T]], Callable[P, T]]:
        """
        Decorator to validate asset data using Great Expectations.

        Args:
            func_or_config: Function to decorate or suite path
            **kwargs: Additional configuration (suite_path, on_failure)

        Returns:
            Decorated function with GE validation
        """
        # Extract parameters from kwargs
        suite_path = kwargs.pop("suite_path", None)
        on_failure = kwargs.pop("on_failure", "raise")

        # Case 1: @ge_asset("path/to/suite.yaml")
        if isinstance(func_or_config, str):
            return self._wrap_with_config(
                func=None,
                config=GEAssetConfig(
                    suite_path=func_or_config,
                    on_failure=on_failure or "raise",
                ),
            )

        # Case 2: @ge_asset(config=GEAssetConfig(...))
        if isinstance(func_or_config, GEAssetConfig):
            return self._wrap_with_config(func=None, config=func_or_config)

        # Case 3: @ge_asset (no parentheses)
        if callable(func_or_config) and not kwargs:
            # No configuration provided - raise error
            msg = "@ge_asset requires a suite_path parameter"
            raise ValueError(msg)

        # Case 4: @ge_asset(suite_path="...") - return decorator
        config = GEAssetConfig(
            suite_path=suite_path or "",
            on_failure=on_failure or "raise",
        )

        def decorator(func: Callable[P, T]) -> Callable[P, T]:
            return self._wrap_with_config(func, config)

        return decorator

    def _wrap_with_config(
        self, func: Callable[P, T] | None, config: GEAssetConfig
    ) -> Callable[P, T]:
        """Wrap a function with GE validation logic."""

        if func is None:
            # Return decorator when used as @ge_asset("path")
            def decorator(f: Callable[P, T]) -> Callable[P, T]:
                return self._wrap_with_config(f, config)

            return decorator

        @wraps(func)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
            # Call the original function to get data
            result = func(*args, **kwargs)

            # Only validate if result is a sequence of DataRecords
            if isinstance(result, Sequence) and len(result) > 0:
                if isinstance(result[0], DataRecord):
                    # Create adapter and run validation
                    adapter = GreatExpectationsAdapter(config.suite_path)
                    tool_result = adapter.validate(result, config.suite_path)

                    # Handle validation result based on on_failure setting
                    self._handle_validation_result(tool_result, config.on_failure)

            return result

        return wrapped

    def _handle_validation_result(self, result: QualityToolResult, on_failure: str) -> None:
        """Handle GE validation result based on configuration."""
        if on_failure == "raise" and not result.passed:
            error_msg = f"Great Expectations validation failed for '{result.asset_name}'"
            if result.errors:
                error_msg += "\n" + "\n".join(result.errors)
            if result.warnings:
                error_msg += "\nWarnings:\n" + "\n".join(result.warnings)

            raise ValueError(error_msg)

        elif on_failure == "warn" and not result.passed:
            import warnings as py_warnings

            warning_msg = f"Great Expectations validation warning for '{result.asset_name}'"
            if result.errors:
                warning_msg += "\n" + "\n".join(result.errors)
            if result.warnings:
                warning_msg += "\nWarnings:\n" + "\n".join(result.warnings)

            py_warnings.warn(warning_msg, stacklevel=2)

        # on_failure == "ignore": do nothing


# Create the ge_asset decorator instance
ge_asset = GEAssetDecorator()


# =============================================================================
# YAML Loader Helpers
# =============================================================================


def load_ge_suite(suite_path: str) -> dict[str, Any]:
    """
    Load a Great Expectations suite from YAML file.

    Args:
        suite_path: Path to GE suite YAML file

    Returns:
        Parsed configuration as dictionary

    Raises:
        FileNotFoundError: If suite file doesn't exist
        ImportError: If PyYAML is not installed
    """
    adapter = GreatExpectationsAdapter(suite_path)
    return adapter.load_config(suite_path)


def create_ge_suite_config(
    expectations: list[dict[str, Any]],
    data_asset_name: str | None = None,
) -> dict[str, Any]:
    """
    Create a GE suite configuration dictionary.

    Args:
        expectations: List of expectation configurations
        data_asset_name: Optional data asset name

    Returns:
        GE suite configuration dictionary

    Example:
        config = create_ge_suite_config([
            {
                "name": "row_count_check",
                "type": "expect_table_row_count_to_be_between",
                "min": 100,
                "max": 10000,
            },
            {
                "name": "email_format",
                "type": "expect_column_values_to_match_regex",
                "column": "email",
                "regex": "^[\\w\\.-]+@[\\w\\.-]+$",
            },
        ])
    """
    suite: dict[str, Any] = {
        "expectations": expectations,
    }

    if data_asset_name:
        suite["data_asset_name"] = data_asset_name

    return suite


def save_ge_suite(suite: dict[str, Any], output_path: str) -> None:
    """
    Save a GE suite configuration to YAML file.

    Args:
        suite: GE suite configuration dictionary
        output_path: Path to save YAML file

    Raises:
        ImportError: If PyYAML is not installed
    """
    if yaml is None:
        msg = "PyYAML is required to save GE suites. Install with: pip install pyyaml"
        raise ImportError(msg)

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w") as f:
        yaml.dump(suite, f, default_flow_style=False)  # type: ignore
