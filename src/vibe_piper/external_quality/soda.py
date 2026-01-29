"""
Soda integration for Vibe Piper.

This module provides integration with Soda (Soda Core) for data quality validation.
Includes @soda_asset decorator and utilities for loading Soda checks from YAML.

Note: This module requires soda-core package to be installed.
       It is an optional dependency - install with: pip install vibe-piper[soda]

Example usage:

    @soda_asset(checks_path="soda_checks/sales.yaml")
    def sales():
        return load_sales()

    @soda_asset(checks_path="soda_checks/products.yaml")
    def products():
        return load_products()
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any, ParamSpec, TypeVar

# Optional import - Soda is not a hard dependency
try:
    import pandas as pd
    import yaml
except ImportError:
    pd = None  # type: ignore
    yaml = None  # type: ignore

from vibe_piper.types import DataRecord, QualityMetric, QualityMetricType

# Optional import - pandas is not a hard dependency
try:
    import pandas as pd
except ImportError:
    pd = None  # type: ignore

from vibe_piper.external_quality.base import (
    QualityToolAdapter,
    QualityToolResult,
    ToolType,
)

P = ParamSpec("P")
T = TypeVar("T")


# =============================================================================
# Soda Adapter Class
# =============================================================================


class SodaAdapter(QualityToolAdapter):
    """
    Adapter for Soda data quality checks.

    Loads Soda checks from YAML files, executes validations,
    and converts results to VibePiper format.
    """

    def __init__(self, checks_path: str) -> None:
        """
        Initialize adapter with a Soda checks path.

        Args:
            checks_path: Path to Soda checks YAML file
        """
        self.checks_path = checks_path
        self.config = self.load_config(checks_path)

    def load_config(self, config_path: str) -> dict[str, Any]:
        """
        Load Soda checks configuration from YAML.

        Args:
            config_path: Path to Soda checks YAML file

        Returns:
            Parsed configuration as dictionary
        """
        if yaml is None:
            msg = "PyYAML is required to load Soda checks. Install with: pip install pyyaml"
            raise ImportError(msg)

        path = Path(config_path)
        if not path.exists():
            msg = f"Soda checks file not found: {config_path}"
            raise FileNotFoundError(msg)

        with path.open() as f:
            return yaml.safe_load(f)  # type: ignore

    def validate(
        self,
        data: Sequence[DataRecord],
        config_path: str,  # noqa: ARG001
    ) -> QualityToolResult:
        """
        Validate data using Soda checks.

        Args:
            data: Records to validate
            config_path: Path to Soda checks YAML file (uses self.checks_path)

        Returns:
            QualityToolResult with validation outcome
        """
        start_time = datetime.now()
        metrics: list[QualityMetric] = []
        errors: list[str] = []
        warnings: list[str] = []

        if not data:
            return QualityToolResult(
                tool_type=ToolType.SODA,
                asset_name=self.checks_path,
                passed=True,
                duration_ms=(datetime.now() - start_time).total_seconds() * 1000,
            )

        # Convert DataRecords to pandas DataFrame for Soda
        records_dicts = [record.data for record in data]
        df = pd.DataFrame(records_dicts)  # type: ignore

        # Execute Soda checks based on config
        config = self.config

        # Check for checks section
        if "checks" in config:
            for check in config["checks"]:
                result = self._run_check(df, check)
                metrics.append(result["metric"])
                if result["error"]:
                    errors.append(result["error"])
                if result["warning"]:
                    warnings.append(result["warning"])

        # Calculate overall pass/fail
        passed = len(errors) == 0

        return QualityToolResult(
            tool_type=ToolType.SODA,
            asset_name=Path(self.checks_path).stem,
            passed=passed,
            metrics=tuple(metrics),
            errors=tuple(errors),
            warnings=tuple(warnings),
            raw_result={"config": config, "df_shape": df.shape},
            duration_ms=(datetime.now() - start_time).total_seconds() * 1000,
            timestamp=start_time,
        )

    def _run_check(self, df: Any, check: dict[str, Any]) -> dict[str, Any]:
        """
        Run a single Soda check on DataFrame.

        Args:
            df: Pandas DataFrame with data
            check: Check configuration

        Returns:
            Dictionary with metric, error, and warning
        """
        check_name = check.get("name", "unnamed_check")
        check_type = check.get("type", "")
        column = check.get("column")

        error = None
        warning = None
        metric_value = 0.0
        passed = True

        try:
            # Row count checks
            if check_type == "row_count":
                min_count = check.get("min", 0)
                max_count = check.get("max", float("inf"))
                row_count = len(df)

                passed = min_count <= row_count <= max_count
                metric_value = row_count
                if not passed:
                    error = f"Row count {row_count} not in range [{min_count}, {max_count}]"

            elif check_type == "freshness":
                column = check.get("column")
                max_age_hours = check.get("max_age_hours", 24)

                if column is not None and column in df.columns:
                    col = df[column]
                    try:
                        # Try to parse timestamps
                        if pd.api.types.is_datetime64_any_dtype(col):
                            timestamps = col
                        elif col.dtype == "object":
                            timestamps = pd.to_datetime(col, errors="coerce")
                        else:
                            timestamps = None

                        if timestamps is not None:
                            now = pd.Timestamp.now()
                            age_hours = (
                                (now - timestamps.max()).total_seconds() / 3600
                                if not timestamps.isna().all()
                                else 0
                            )

                            passed = age_hours <= max_age_hours
                            metric_value = age_hours
                            if not passed:
                                error = f"Freshness check failed: max age {age_hours:.2f} hours > {max_age_hours} hours"
                            else:
                                warning = f"Freshness warning: max age {age_hours:.2f} hours"

                    except Exception as e:
                        error = f"Error checking freshness: {e}"
                        passed = False

            # Null checks
            elif check_type == "missing_values":
                column = check.get("column")
                max_missing_pct = check.get("max_missing_pct", 0.0)

                if column is not None and column in df.columns:
                    col = df[column]
                    missing_count = col.isna().sum()
                    total_count = len(col)
                    missing_pct = (missing_count / total_count * 100) if total_count > 0 else 0

                    metric_value = missing_pct
                    passed = missing_pct <= max_missing_pct
                    if not passed:
                        error = f"Column '{column}' has {missing_pct:.2f}% missing, max allowed: {max_missing_pct}%"

            # Uniqueness checks
            elif check_type == "duplicate_values":
                column = check.get("column")
                max_duplicate_pct = check.get("max_duplicate_pct", 0.0)

                if column is not None and column in df.columns:
                    col = df[column]
                    duplicate_count = col.duplicated().sum()
                    total_count = len(col)
                    duplicate_pct = (duplicate_count / total_count * 100) if total_count > 0 else 0

                    metric_value = duplicate_pct
                    passed = duplicate_pct <= max_duplicate_pct
                    if not passed:
                        error = f"Column '{column}' has {duplicate_pct:.2f}% duplicates, max allowed: {max_duplicate_pct}%"

            # Value range checks
            elif check_type == "values_in_range":
                column = check.get("column")
                min_val = check.get("min")
                max_val = check.get("max")

                if column is not None and column in df.columns:
                    col = df[column]
                    out_of_range = (
                        ((col < min_val) | (col > max_val)).sum()
                        if pd.api.types.is_numeric_dtype(col)
                        else 0
                    )

                    metric_value = out_of_range
                    passed = out_of_range == 0
                    if not passed:
                        error = f"Column '{column}' has {out_of_range} values outside range [{min_val}, {max_val}]"

            # Value set checks
            elif check_type == "values_in_set":
                column = check.get("column")
                allowed_values = set(check.get("values", []))

                if column is not None and column in df.columns:
                    col = df[column]
                    invalid_count = (~col.isin(allowed_values)).sum()
                    total_count = len(col)

                    metric_value = invalid_count
                    passed = invalid_count == 0
                    if not passed:
                        error = f"Column '{column}' has {invalid_count} values not in allowed set"

            # Reference checks
            elif check_type == "reference":
                source_column = check.get("source_column")
                target_column = check.get("target_column")

                if (
                    source_column is not None
                    and target_column is not None
                    and source_column in df.columns
                    and target_column in df.columns
                ):
                    source_values = set(df[source_column].dropna().unique())
                    target_values = set(df[target_column].dropna().unique())
                    missing_refs = source_values - target_values

                    metric_value = len(missing_refs)
                    passed = len(missing_refs) == 0
                    if not passed:
                        error = f"Reference check failed: {len(missing_refs)} values in '{source_column}' not found in '{target_column}'"

            # Schema checks
            elif check_type == "schema":
                required_columns = check.get("columns", [])

                metric_value = 1.0
                missing_cols = [col for col in required_columns if col not in df.columns]

                if missing_cols:
                    passed = False
                    error = f"Missing required columns: {', '.join(missing_cols)}"
                else:
                    passed = True

            else:
                warning = f"Unknown check type: {check_type}"
                passed = True

        except Exception as e:
            error = f"Error running check '{check_name}': {e}"
            passed = False

        metric = QualityMetric(
            name=f"soda_{check_name}",
            metric_type=QualityMetricType.VALIDITY,
            value=metric_value,
            passed=passed,
            description=f"Soda check: {check_name}",
        )

        return {
            "metric": metric,
            "error": error,
            "warning": warning,
        }


# =============================================================================
# Soda Decorator
# =============================================================================


@dataclass(frozen=True)
class SodaAssetConfig:
    """
    Configuration for @soda_asset decorator.

    Attributes:
        checks_path: Path to Soda checks YAML file
        on_failure: Action to take on validation failure
    """

    checks_path: str
    on_failure: str = "raise"  # 'raise', 'warn', 'ignore'


class SodaAssetDecorator:
    """
    Decorator class for Soda asset validation.

    Supports both @soda_asset and @soda_asset(...) patterns.

    Example:
        @soda_asset(checks_path="soda_checks/sales.yaml")
        def sales():
            return load_sales()
    """

    def __call__(
        self,
        func_or_config: Callable[P, T] | str | SodaAssetConfig | None = None,
        **kwargs: Any,
    ) -> Callable[P, T] | Callable[[Callable[P, T]], Callable[P, T]]:
        """
        Decorator to validate asset data using Soda.

        Args:
            func_or_config: Function to decorate or checks path
            **kwargs: Additional configuration (checks_path, on_failure)

        Returns:
            Decorated function with Soda validation
        """
        # Extract parameters from kwargs
        checks_path = kwargs.pop("checks_path", None)
        on_failure = kwargs.pop("on_failure", "raise")

        # Case 1: @soda_asset("path/to/checks.yaml")
        if isinstance(func_or_config, str):
            return self._wrap_with_config(
                func=None,
                config=SodaAssetConfig(
                    checks_path=func_or_config,
                    on_failure=on_failure or "raise",
                ),
            )

        # Case 2: @soda_asset(config=SodaAssetConfig(...))
        if isinstance(func_or_config, SodaAssetConfig):
            return self._wrap_with_config(func=None, config=func_or_config)

        # Case 3: @soda_asset (no parentheses)
        if callable(func_or_config) and not kwargs:
            # No configuration provided - raise error
            msg = "@soda_asset requires a checks_path parameter"
            raise ValueError(msg)

        # Case 4: @soda_asset(checks_path="...") - return decorator
        config = SodaAssetConfig(
            checks_path=checks_path or "",
            on_failure=on_failure or "raise",
        )

        def decorator(func: Callable[P, T]) -> Callable[P, T]:
            return self._wrap_with_config(func, config)

        return decorator

    def _wrap_with_config(
        self, func: Callable[P, T] | None, config: SodaAssetConfig
    ) -> Callable[P, T]:
        """Wrap a function with Soda validation logic."""

        if func is None:
            # Return decorator when used as @soda_asset("path")
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
                    adapter = SodaAdapter(config.checks_path)
                    tool_result = adapter.validate(result, config.checks_path)

                    # Handle validation result based on on_failure setting
                    self._handle_validation_result(tool_result, config.on_failure)

            return result

        return wrapped

    def _handle_validation_result(self, result: QualityToolResult, on_failure: str) -> None:
        """Handle Soda validation result based on configuration."""
        if on_failure == "raise" and not result.passed:
            error_msg = f"Soda validation failed for '{result.asset_name}'"
            if result.errors:
                error_msg += "\n" + "\n".join(result.errors)
            if result.warnings:
                error_msg += "\nWarnings:\n" + "\n".join(result.warnings)

            raise ValueError(error_msg)

        elif on_failure == "warn" and not result.passed:
            import warnings as py_warnings

            warning_msg = f"Soda validation warning for '{result.asset_name}'"
            if result.errors:
                warning_msg += "\n" + "\n".join(result.errors)
            if result.warnings:
                warning_msg += "\nWarnings:\n" + "\n".join(result.warnings)

            py_warnings.warn(warning_msg, stacklevel=2)

        # on_failure == "ignore": do nothing


# Create the soda_asset decorator instance
soda_asset = SodaAssetDecorator()


# =============================================================================
# YAML Loader Helpers
# =============================================================================


def load_soda_checks(checks_path: str) -> dict[str, Any]:
    """
    Load Soda checks from YAML file.

    Args:
        checks_path: Path to Soda checks YAML file

    Returns:
        Parsed configuration as dictionary

    Raises:
        FileNotFoundError: If checks file doesn't exist
        ImportError: If PyYAML is not installed
    """
    adapter = SodaAdapter(checks_path)
    return adapter.load_config(checks_path)


def create_soda_checks_config(
    checks: list[dict[str, Any]],
    data_source_name: str | None = None,
) -> dict[str, Any]:
    """
    Create a Soda checks configuration dictionary.

    Args:
        checks: List of check configurations
        data_source_name: Optional data source name

    Returns:
        Soda checks configuration dictionary

    Example:
        config = create_soda_checks_config([
            {
                "name": "row_count",
                "type": "row_count",
                "min": 100,
                "max": 10000,
            },
            {
                "name": "email_format",
                "type": "values_in_set",
                "column": "email_domain",
                "values": ["gmail.com", "yahoo.com", "outlook.com"],
            },
        ])
    """
    checks_config: dict[str, Any] = {
        "checks": checks,
    }

    if data_source_name:
        checks_config["data_source_name"] = data_source_name

    return checks_config


def save_soda_checks(checks: dict[str, Any], output_path: str) -> None:
    """
    Save a Soda checks configuration to YAML file.

    Args:
        checks: Soda checks configuration dictionary
        output_path: Path to save YAML file

    Raises:
        ImportError: If PyYAML is not installed
    """
    if yaml is None:
        msg = "PyYAML is required to save Soda checks. Install with: pip install pyyaml"
        raise ImportError(msg)

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w") as f:
        yaml.dump(checks, f, default_flow_style=False)  # type: ignore
