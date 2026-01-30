"""
Data cleaning transformation utilities.

This module provides comprehensive data cleaning capabilities including:
- Deduplication (duplicate detection and removal)
- Null handling (strategies for missing values)
- Outlier detection and treatment
- Type normalization (type conversion and validation)
- Standardization (scaling and normalization)
- Text cleaning (whitespace, format normalization)
- Cleaning reports (tracking what was cleaned)

All functions operate on lists of DataRecord objects and return cleaned data.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any

import pandas as pd

from vibe_piper.types import DataRecord, DataType

# =============================================================================
# Enums and Configuration Types
# =============================================================================


class NullStrategy(Enum):
    """Strategies for handling null/missing values."""

    DROP = auto()  # Drop rows with null values
    FILL_DEFAULT = auto()  # Fill with default value
    FILL_MEAN = auto()  # Fill with mean (numeric only)
    FILL_MEDIAN = auto()  # Fill with median (numeric only)
    FILL_MODE = auto()  # Fill with mode (most frequent)
    FILL_FORWARD = auto()  # Forward fill (last valid value)
    FILL_BACKWARD = auto()  # Backward fill (next valid value)
    INTERPOLATE = auto()  # Interpolate (numeric only)
    KEEP = auto()  # Keep null values as is


class OutlierMethod(Enum):
    """Methods for detecting outliers."""

    IQR = auto()  # Interquartile range method
    ZSCORE = auto()  # Z-score method
    MODIFIED_ZSCORE = auto()  # Modified Z-score with MAD
    ISOLATION_FOREST = auto()  # Isolation Forest (sklearn)
    PERCENTILE = auto()  # Percentile-based method


class OutlierAction(Enum):
    """Actions to take on detected outliers."""

    DROP = auto()  # Drop outlier rows
    CAP = auto()  # Cap at threshold
    FLOOR = auto()  # Floor at threshold
    MEAN_REPLACE = auto()  # Replace with mean
    MEDIAN_REPLACE = auto()  # Replace with median
    FLAG = auto()  # Add flag column


@dataclass(frozen=True)
class CleaningConfig:
    """
    Configuration for data cleaning operations.

    Attributes:
        dedup_columns: Columns to consider for deduplication (None = all columns)
        null_strategy: Strategy for handling null values
        null_fill_value: Default fill value for FILL_DEFAULT strategy
        null_columns: Specific columns to handle (None = all columns)
        outlier_method: Method for outlier detection
        outlier_action: Action to take on outliers
        outlier_threshold: Threshold for outlier detection (e.g., 3 for z-score)
        outlier_columns: Columns to check for outliers (None = numeric only)
        normalize_text: Whether to normalize text columns
        trim_whitespace: Whether to trim whitespace from strings
        case_normalization: Text case normalization ('upper', 'lower', 'title', None)
        standardize_columns: Columns to standardize/normalize
        generate_report: Whether to generate cleaning report
        strict: If True, raise errors on validation failures
    """

    dedup_columns: tuple[str, ...] | None = None
    null_strategy: NullStrategy = NullStrategy.KEEP
    null_fill_value: Any = None
    null_columns: tuple[str, ...] | None = None
    outlier_method: OutlierMethod = OutlierMethod.IQR
    outlier_action: OutlierAction = OutlierAction.CAP
    outlier_threshold: float = 1.5
    outlier_columns: tuple[str, ...] | None = None
    normalize_text: bool = False
    trim_whitespace: bool = True
    case_normalization: str | None = None
    standardize_columns: tuple[str, ...] = ()
    generate_report: bool = True
    strict: bool = False


@dataclass(frozen=True)
class CleaningReport:
    """
    Report of cleaning operations performed.

    Attributes:
        original_count: Number of records before cleaning
        final_count: Number of records after cleaning
        duplicates_removed: Number of duplicate records removed
        nulls_filled: Number of null values filled
        outliers_handled: Number of outliers handled
        text_normalized: Number of text fields normalized
        types_converted: Number of type conversions
        operations: List of operations performed
        duration_ms: Time taken for cleaning in milliseconds
        timestamp: When cleaning was performed
        details: Detailed breakdown by column
    """

    original_count: int
    final_count: int
    duplicates_removed: int = 0
    nulls_filled: int = 0
    outliers_handled: int = 0
    text_normalized: int = 0
    types_converted: int = 0
    operations: tuple[str, ...] = field(default_factory=tuple)
    duration_ms: float = 0.0
    timestamp: datetime = field(default_factory=datetime.utcnow)
    details: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def records_removed(self) -> int:
        """Total number of records removed."""
        return self.original_count - self.final_count

    def to_dict(self) -> dict[str, Any]:
        """Convert report to dictionary."""
        return {
            "original_count": self.original_count,
            "final_count": self.final_count,
            "records_removed": self.records_removed,
            "duplicates_removed": self.duplicates_removed,
            "nulls_filled": self.nulls_filled,
            "outliers_handled": self.outliers_handled,
            "text_normalized": self.text_normalized,
            "types_converted": self.types_converted,
            "operations": list(self.operations),
            "duration_ms": self.duration_ms,
            "timestamp": self.timestamp.isoformat(),
            "details": self.details,
        }


# =============================================================================
# Decorator
# =============================================================================


def clean_data(config: CleaningConfig | None = None):
    """
    Decorator to automatically clean function output data.

    This decorator wraps a function and applies data cleaning to its
    output before returning it. Generates a cleaning report by default.

    Args:
        config: Cleaning configuration. If None, uses default config.

    Returns:
        Decorated function that returns (cleaned_data, report) tuple

    Example:
        Clean data from a function::

            @clean_data(
                config=CleaningConfig(
                    null_strategy=NullStrategy.FILL_MEAN,
                    outlier_method=OutlierMethod.IQR,
                    trim_whitespace=True
                )
            )
            def load_users() -> list[DataRecord]:
                # Load data from somewhere
                return records

            # Usage
            cleaned_data, report = load_users()
            print(f"Cleaned {report.records_removed} records")
    """

    def decorator(func: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> tuple[list[DataRecord], CleaningReport]:
            result = func(*args, **kwargs)
            cleaning_config = config or CleaningConfig()

            if not isinstance(result, list):
                msg = f"Function {func.__name__} must return list[DataRecord]"
                raise TypeError(msg)

            if result and not isinstance(result[0], DataRecord):
                msg = f"Function {func.__name__} must return list[DataRecord]"
                raise TypeError(msg)

            # Apply cleaning
            cleaned_data, report = clean_dataset(result, cleaning_config)

            return cleaned_data, report

        return wrapper

    return decorator


# =============================================================================
# Main Cleaning Function
# =============================================================================


def clean_dataset(
    data: list[DataRecord],
    config: CleaningConfig | None = None,
) -> tuple[list[DataRecord], CleaningReport]:
    """
    Clean a dataset using the provided configuration.

    This is the main entry point for data cleaning. It applies all
    configured cleaning operations in order and generates a report.

    Args:
        data: Input dataset as list of DataRecord objects
        config: Cleaning configuration. If None, uses defaults.

    Returns:
        Tuple of (cleaned_data, report)

    Example:
        Clean a dataset with default config::

            cleaned, report = clean_dataset(data)

        Clean with custom config::

            config = CleaningConfig(
                null_strategy=NullStrategy.FILL_MEAN,
                outlier_method=OutlierMethod.IQR,
                trim_whitespace=True
            )
            cleaned, report = clean_dataset(data, config)
    """
    import time

    cleaning_config = config or CleaningConfig()
    start_time = time.time()

    # Initialize report
    report_data = {
        "original_count": len(data),
        "details": {},
    }

    operations: list[str] = []
    cleaned_data = list(data)

    # Apply cleaning operations in order

    # 1. Deduplication
    if cleaning_config.dedup_columns is not None or len(cleaned_data) > 0:
        cleaned_data, dup_report = remove_duplicates(
            cleaned_data,
            columns=cleaning_config.dedup_columns,
        )
        report_data["duplicates_removed"] = dup_report["removed_count"]
        if dup_report["removed_count"] > 0:
            operations.append("deduplication")

    # 2. Null handling
    if cleaning_config.null_strategy != NullStrategy.KEEP:
        cleaned_data, null_report = handle_nulls(
            cleaned_data,
            strategy=cleaning_config.null_strategy,
            fill_value=cleaning_config.null_fill_value,
            columns=cleaning_config.null_columns,
        )
        report_data["nulls_filled"] = null_report["filled_count"]
        if null_report["filled_count"] > 0:
            operations.append("null_handling")

    # 3. Outlier treatment
    if (
        cleaning_config.outlier_action != OutlierAction.FLAG
        and cleaning_config.outlier_method != OutlierMethod.IQR
    ):
        cleaned_data, outlier_report = handle_outliers(
            cleaned_data,
            method=cleaning_config.outlier_method,
            action=cleaning_config.outlier_action,
            threshold=cleaning_config.outlier_threshold,
            columns=cleaning_config.outlier_columns,
        )
        report_data["outliers_handled"] = outlier_report["handled_count"]
        if outlier_report["handled_count"] > 0:
            operations.append("outlier_treatment")

    # 4. Text cleaning
    if cleaning_config.trim_whitespace or cleaning_config.normalize_text:
        cleaned_data, text_report = clean_text(
            cleaned_data,
            trim=cleaning_config.trim_whitespace,
            normalize=cleaning_config.normalize_text,
            case_normalization=cleaning_config.case_normalization,
        )
        report_data["text_normalized"] = text_report["normalized_count"]
        if text_report["normalized_count"] > 0:
            operations.append("text_cleaning")

    # 5. Standardization
    if cleaning_config.standardize_columns:
        cleaned_data, std_report = standardize_columns(
            cleaned_data,
            columns=cleaning_config.standardize_columns,
        )
        report_data["types_converted"] = std_report["converted_count"]
        if std_report["converted_count"] > 0:
            operations.append("standardization")

    # Calculate duration
    duration_ms = (time.time() - start_time) * 1000

    # Create report
    report = CleaningReport(
        original_count=report_data["original_count"],
        final_count=len(cleaned_data),
        duplicates_removed=report_data.get("duplicates_removed", 0),
        nulls_filled=report_data.get("nulls_filled", 0),
        outliers_handled=report_data.get("outliers_handled", 0),
        text_normalized=report_data.get("text_normalized", 0),
        types_converted=report_data.get("types_converted", 0),
        operations=tuple(operations),
        duration_ms=duration_ms,
        details=report_data.get("details", {}),
    )

    return cleaned_data, report


# =============================================================================
# Deduplication Functions
# =============================================================================


def remove_duplicates(
    data: list[DataRecord],
    columns: tuple[str, ...] | None = None,
    keep: str = "first",
) -> tuple[list[DataRecord], dict[str, int]]:
    """
    Remove duplicate records from dataset.

    Args:
        data: Input dataset
        columns: Columns to consider for deduplication. If None, uses all columns.
        keep: Which duplicate to keep ('first', 'last', 'none')

    Returns:
        Tuple of (cleaned_data, report_dict)

    Example:
        Remove duplicates based on specific columns::

            cleaned, report = remove_duplicates(
                data,
                columns=("email", "user_id"),
                keep="first"
            )
    """
    if not data:
        return data, {"removed_count": 0, "original_count": 0}

    # Convert to DataFrame for efficient deduplication
    df = pd.DataFrame([record.data for record in data])

    # Select columns for deduplication
    subset = list(columns) if columns else None

    original_count = len(df)

    if keep == "none":
        # Remove all duplicates (keep only unique)
        df_cleaned = df.drop_duplicates(subset=subset, keep=False)
    else:
        df_cleaned = df.drop_duplicates(subset=subset, keep=keep)  # type: ignore[arg-type]

    removed_count = original_count - len(df_cleaned)

    # Reconstruct DataRecord objects
    cleaned_data = [
        DataRecord(data=row.to_dict(), schema=data[0].schema) for _, row in df_cleaned.iterrows()
    ]

    report = {
        "removed_count": removed_count,
        "original_count": original_count,
        "final_count": len(cleaned_data),
        "columns": subset,
    }

    return cleaned_data, report


def find_duplicates(
    data: list[DataRecord],
    columns: tuple[str, ...] | None = None,
) -> list[int]:
    """
    Find indices of duplicate records.

    Args:
        data: Input dataset
        columns: Columns to check for duplicates

    Returns:
        List of duplicate indices (excluding first occurrence)

    Example:
        Find duplicate indices::

            dup_indices = find_duplicates(data, columns=("email",))
    """
    if not data:
        return []

    df = pd.DataFrame([record.data for record in data])
    subset = list(columns) if columns else None

    # Boolean mask of duplicates (True for all but first occurrence)
    duplicates = df.duplicated(subset=subset, keep="first")

    return [i for i, is_dup in enumerate(duplicates) if is_dup]


# =============================================================================
# Null Handling Functions
# =============================================================================


def handle_nulls(
    data: list[DataRecord],
    strategy: NullStrategy,
    fill_value: Any = None,
    columns: tuple[str, ...] | None = None,
) -> tuple[list[DataRecord], dict[str, int]]:
    """
    Handle null values according to the specified strategy.

    Args:
        data: Input dataset
        strategy: Null handling strategy
        fill_value: Fill value for FILL_DEFAULT strategy
        columns: Columns to handle. If None, handles all columns.

    Returns:
        Tuple of (cleaned_data, report_dict)

    Example:
        Fill nulls with mean::

            cleaned, report = handle_nulls(
                data,
                strategy=NullStrategy.FILL_MEAN,
                columns=("age", "income")
            )
    """
    if not data:
        return data, {"filled_count": 0, "original_count": 0}

    # Convert to DataFrame
    df = pd.DataFrame([record.data for record in data])

    # Select columns to handle
    target_cols = list(columns) if columns else list(df.columns)

    filled_count = 0

    # Apply strategy
    if strategy == NullStrategy.DROP:
        df_cleaned = df.dropna(subset=target_cols)
        filled_count = len(df) - len(df_cleaned)
    elif strategy == NullStrategy.FILL_DEFAULT:
        for col in target_cols:
            null_count = df[col].isna().sum()
            if null_count > 0:
                df[col] = df[col].fillna(fill_value)
                filled_count += null_count
        df_cleaned = df
    elif strategy == NullStrategy.FILL_MEAN:
        for col in target_cols:
            if df[col].dtype in [int, float]:
                null_count = df[col].isna().sum()
                if null_count > 0:
                    mean_val = df[col].mean()
                    df[col] = df[col].fillna(mean_val)
                    filled_count += null_count
        df_cleaned = df
    elif strategy == NullStrategy.FILL_MEDIAN:
        for col in target_cols:
            if df[col].dtype in [int, float]:
                null_count = df[col].isna().sum()
                if null_count > 0:
                    median_val = df[col].median()
                    df[col] = df[col].fillna(median_val)
                    filled_count += null_count
        df_cleaned = df
    elif strategy == NullStrategy.FILL_MODE:
        for col in target_cols:
            null_count = df[col].isna().sum()
            if null_count > 0:
                mode_val = df[col].mode()
                if len(mode_val) > 0:
                    df[col] = df[col].fillna(mode_val[0])
                    filled_count += null_count
        df_cleaned = df
    elif strategy == NullStrategy.FILL_FORWARD:
        df_cleaned = df[target_cols].fillna(method="ffill").fillna(method="bfill")
        filled_count = df[target_cols].isna().sum().sum()
    elif strategy == NullStrategy.FILL_BACKWARD:
        df_cleaned = df[target_cols].fillna(method="bfill").fillna(method="ffill")
        filled_count = df[target_cols].isna().sum().sum()
    elif strategy == NullStrategy.INTERPOLATE:
        for col in target_cols:
            if df[col].dtype in [int, float]:
                null_count = df[col].isna().sum()
                if null_count > 0:
                    df[col] = df[col].interpolate()
                    filled_count += null_count
        df_cleaned = df
    else:  # KEEP
        return data, {"filled_count": 0, "original_count": len(data)}

    # Reconstruct DataRecord objects
    cleaned_data = [
        DataRecord(data=row.to_dict(), schema=data[0].schema) for _, row in df_cleaned.iterrows()
    ]

    report = {
        "filled_count": filled_count,
        "original_count": len(data),
        "final_count": len(cleaned_data),
        "strategy": strategy.name,
        "columns": target_cols,
    }

    return cleaned_data, report


def drop_nulls(data: list[DataRecord], columns: tuple[str, ...] | None = None) -> list[DataRecord]:
    """
    Drop rows containing null values.

    Args:
        data: Input dataset
        columns: Columns to check. If None, checks all columns.

    Returns:
        Cleaned dataset

    Example:
        Drop rows with null email::

            cleaned = drop_nulls(data, columns=("email",))
    """
    cleaned, _ = handle_nulls(data, NullStrategy.DROP, columns=columns)
    return cleaned


def fill_nulls(
    data: list[DataRecord],
    value: Any,
    columns: tuple[str, ...] | None = None,
) -> list[DataRecord]:
    """
    Fill null values with a specified value.

    Args:
        data: Input dataset
        value: Fill value
        columns: Columns to fill. If None, fills all columns.

    Returns:
        Cleaned dataset

    Example:
        Fill missing ages with 0::

            cleaned = fill_nulls(data, 0, columns=("age",))
    """
    cleaned, _ = handle_nulls(data, NullStrategy.FILL_DEFAULT, fill_value=value, columns=columns)
    return cleaned


# =============================================================================
# Outlier Detection and Treatment Functions
# =============================================================================


def detect_outliers(
    data: list[DataRecord],
    method: OutlierMethod = OutlierMethod.IQR,
    threshold: float = 1.5,
    columns: tuple[str, ...] | None = None,
) -> dict[str, list[int]]:
    """
    Detect outliers in numeric columns.

    Args:
        data: Input dataset
        method: Detection method
        threshold: Threshold for outlier detection
        columns: Columns to check. If None, checks all numeric columns.

    Returns:
        Dictionary mapping column names to lists of outlier indices

    Example:
        Detect outliers using IQR method::

            outliers = detect_outliers(
                data,
                method=OutlierMethod.IQR,
                threshold=1.5
            )
    """
    if not data:
        return {}

    df = pd.DataFrame([record.data for record in data])

    # Select numeric columns
    if columns:
        target_cols = [
            col for col in columns if col in df.columns and df[col].dtype in [int, float]
        ]
    else:
        target_cols = [col for col in df.columns if df[col].dtype in [int, float]]

    outliers: dict[str, list[int]] = {}

    for col in target_cols:
        series = df[col].dropna()

        if method == OutlierMethod.IQR:
            Q1 = series.quantile(0.25)
            Q3 = series.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR
            outlier_mask = (df[col] < lower_bound) | (df[col] > upper_bound)
        elif method == OutlierMethod.ZSCORE:
            mean = series.mean()
            std = series.std()
            z_scores = ((df[col] - mean) / std).abs()
            outlier_mask = z_scores > threshold
        elif method == OutlierMethod.MODIFIED_ZSCORE:
            median = series.median()
            mad = (series - median).abs().median()
            modified_z_scores = 0.6745 * (df[col] - median) / mad
            outlier_mask = modified_z_scores.abs() > threshold
        elif method == OutlierMethod.PERCENTILE:
            lower = series.quantile(threshold / 100)
            upper = series.quantile(1 - threshold / 100)
            outlier_mask = (df[col] < lower) | (df[col] > upper)
        else:
            # ISOLATION_FOREST and others not implemented yet
            continue

        outlier_indices = [i for i, is_outlier in enumerate(outlier_mask) if is_outlier]
        outliers[col] = outlier_indices

    return outliers


def handle_outliers(
    data: list[DataRecord],
    method: OutlierMethod = OutlierMethod.IQR,
    action: OutlierAction = OutlierAction.CAP,
    threshold: float = 1.5,
    columns: tuple[str, ...] | None = None,
) -> tuple[list[DataRecord], dict[str, int]]:
    """
    Handle outliers according to the specified action.

    Args:
        data: Input dataset
        method: Detection method
        action: Action to take on outliers
        threshold: Detection threshold
        columns: Columns to handle

    Returns:
        Tuple of (cleaned_data, report_dict)

    Example:
        Cap outliers at threshold::

            cleaned, report = handle_outliers(
                data,
                method=OutlierMethod.IQR,
                action=OutlierAction.CAP,
                threshold=1.5
            )
    """
    if not data:
        return data, {"handled_count": 0, "original_count": 0}

    outliers = detect_outliers(data, method, threshold, columns)

    if not outliers:
        return data, {"handled_count": 0, "original_count": len(data)}

    df = pd.DataFrame([record.data for record in data])
    handled_count = 0

    for col, indices in outliers.items():
        series = df[col].dropna()
        handled_count += len(indices)

        if action == OutlierAction.DROP:
            df = df.drop(indices)
        elif action == OutlierAction.CAP:
            Q1 = series.quantile(0.25)
            Q3 = series.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR
            df.loc[indices, col] = df.loc[indices, col].clip(lower=lower_bound, upper=upper_bound)
        elif action == OutlierAction.FLOOR:
            Q1 = series.quantile(0.25)
            Q3 = series.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR
            df.loc[indices, col] = df.loc[indices, col].clip(lower=lower_bound)
        elif action == OutlierAction.MEAN_REPLACE:
            mean_val = series.mean()
            df.loc[indices, col] = mean_val
        elif action == OutlierAction.MEDIAN_REPLACE:
            median_val = series.median()
            df.loc[indices, col] = median_val
        elif action == OutlierAction.FLAG:
            df[f"{col}_is_outlier"] = False
            df.loc[indices, f"{col}_is_outlier"] = True

    # Reconstruct DataRecord objects
    cleaned_data = [
        DataRecord(data=row.to_dict(), schema=data[0].schema) for _, row in df.iterrows()
    ]

    report = {
        "handled_count": handled_count,
        "original_count": len(data),
        "final_count": len(cleaned_data),
        "method": method.name,
        "action": action.name,
    }

    return cleaned_data, report


def cap_outliers(
    data: list[DataRecord],
    method: OutlierMethod = OutlierMethod.IQR,
    threshold: float = 1.5,
    columns: tuple[str, ...] | None = None,
) -> list[DataRecord]:
    """
    Cap outliers at threshold boundaries.

    Args:
        data: Input dataset
        method: Detection method
        threshold: Detection threshold
        columns: Columns to cap

    Returns:
        Cleaned dataset

    Example:
        Cap outliers::

            cleaned = cap_outliers(data, method=OutlierMethod.IQR, threshold=1.5)
    """
    cleaned, _ = handle_outliers(
        data, method=method, action=OutlierAction.CAP, threshold=threshold, columns=columns
    )
    return cleaned


# =============================================================================
# Type Normalization Functions
# =============================================================================


def normalize_types(
    data: list[DataRecord],
    type_mapping: dict[str, DataType] | None = None,
    infer: bool = False,
) -> tuple[list[DataRecord], dict[str, int]]:
    """
    Normalize data types across columns.

    Args:
        data: Input dataset
        type_mapping: Mapping of column names to target data types
        infer: If True, infer types from data

    Returns:
        Tuple of (cleaned_data, report_dict)

    Example:
        Normalize specific columns::

            cleaned, report = normalize_types(
                data,
                type_mapping={"age": DataType.INTEGER, "price": DataType.FLOAT}
            )
    """
    if not data:
        return data, {"converted_count": 0, "original_count": 0}

    df = pd.DataFrame([record.data for record in data])
    converted_count = 0

    if type_mapping:
        for col, target_type in type_mapping.items():
            if col not in df.columns:
                continue

            original_nulls = df[col].isna().sum()

            if target_type == DataType.INTEGER:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif target_type == DataType.FLOAT:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif target_type == DataType.STRING:
                df[col] = df[col].astype(str)
            elif target_type == DataType.BOOLEAN:
                df[col] = df[col].astype(bool)

            new_nulls = df[col].isna().sum()
            converted_count += (len(df) - original_nulls) - (len(df) - new_nulls)

    elif infer:
        for col in df.columns:
            original_nulls = df[col].isna().sum()

            # Try to infer type
            try:
                # Try integer
                df[col] = pd.to_numeric(df[col], errors="raise").astype("Int64")
                converted_count += len(df) - original_nulls
            except (ValueError, TypeError):
                try:
                    # Try float
                    df[col] = pd.to_numeric(df[col], errors="raise")
                    converted_count += len(df) - original_nulls
                except (ValueError, TypeError):
                    # Keep as string
                    df[col] = df[col].astype(str)

    # Reconstruct DataRecord objects
    cleaned_data = [
        DataRecord(data=row.to_dict(), schema=data[0].schema) for _, row in df.iterrows()
    ]

    report = {
        "converted_count": converted_count,
        "original_count": len(data),
        "final_count": len(cleaned_data),
    }

    return cleaned_data, report


def convert_column_type(
    data: list[DataRecord],
    column: str,
    target_type: DataType,
) -> list[DataRecord]:
    """
    Convert a single column to target type.

    Args:
        data: Input dataset
        column: Column name to convert
        target_type: Target data type

    Returns:
        Cleaned dataset

    Example:
        Convert age to integer::

            cleaned = convert_column_type(data, "age", DataType.INTEGER)
    """
    type_mapping = {column: target_type}
    cleaned, _ = normalize_types(data, type_mapping=type_mapping)
    return cleaned


# =============================================================================
# Standardization Functions
# =============================================================================


def standardize_columns(
    data: list[DataRecord],
    columns: tuple[str, ...],
    method: str = "zscore",
) -> tuple[list[DataRecord], dict[str, int]]:
    """
    Standardize numeric columns.

    Args:
        data: Input dataset
        columns: Columns to standardize
        method: Standardization method ('zscore', 'minmax', 'robust')

    Returns:
        Tuple of (cleaned_data, report_dict)

    Example:
        Standardize using z-score::

            cleaned, report = standardize_columns(
                data,
                columns=("age", "income"),
                method="zscore"
            )
    """
    if not data or not columns:
        return data, {"converted_count": 0, "original_count": 0}

    df = pd.DataFrame([record.data for record in data])
    converted_count = 0

    for col in columns:
        if col not in df.columns or df[col].dtype not in [int, float]:
            continue

        series = df[col].dropna()
        converted_count += len(series)

        if method == "zscore":
            mean = series.mean()
            std = series.std()
            df[col] = (df[col] - mean) / std
        elif method == "minmax":
            min_val = series.min()
            max_val = series.max()
            df[col] = (df[col] - min_val) / (max_val - min_val)
        elif method == "robust":
            median = series.median()
            iqr = series.quantile(0.75) - series.quantile(0.25)
            df[col] = (df[col] - median) / iqr

    # Reconstruct DataRecord objects
    cleaned_data = [
        DataRecord(data=row.to_dict(), schema=data[0].schema) for _, row in df.iterrows()
    ]

    report = {
        "converted_count": converted_count,
        "original_count": len(data),
        "final_count": len(cleaned_data),
        "method": method,
    }

    return cleaned_data, report


def normalize_minmax(data: list[DataRecord], columns: tuple[str, ...]) -> list[DataRecord]:
    """
    Normalize columns to [0, 1] range using min-max scaling.

    Args:
        data: Input dataset
        columns: Columns to normalize

    Returns:
        Cleaned dataset

    Example:
        Normalize to [0, 1]::

            cleaned = normalize_minmax(data, columns=("age", "income"))
    """
    cleaned, _ = standardize_columns(data, columns, method="minmax")
    return cleaned


def normalize_zscore(data: list[DataRecord], columns: tuple[str, ...]) -> list[DataRecord]:
    """
    Standardize columns using z-score (mean=0, std=1).

    Args:
        data: Input dataset
        columns: Columns to standardize

    Returns:
        Cleaned dataset

    Example:
        Standardize using z-score::

            cleaned = normalize_zscore(data, columns=("age", "income"))
    """
    cleaned, _ = standardize_columns(data, columns, method="zscore")
    return cleaned


# =============================================================================
# Text Cleaning Functions
# =============================================================================


def clean_text(
    data: list[DataRecord],
    trim: bool = True,
    normalize: bool = True,
    case_normalization: str | None = None,
    columns: tuple[str, ...] | None = None,
) -> tuple[list[DataRecord], dict[str, int]]:
    """
    Clean text columns.

    Args:
        data: Input dataset
        trim: Trim whitespace
        normalize: Normalize unicode characters
        case_normalization: Case normalization ('upper', 'lower', 'title')
        columns: Columns to clean. If None, cleans all string columns.

    Returns:
        Tuple of (cleaned_data, report_dict)

    Example:
        Clean text columns::

            cleaned, report = clean_text(
                data,
                trim=True,
                normalize=True,
                case_normalization="lower"
            )
    """
    if not data:
        return data, {"normalized_count": 0, "original_count": 0}

    df = pd.DataFrame([record.data for record in data])
    normalized_count = 0

    # Select columns to clean
    if columns:
        target_cols = [col for col in columns if col in df.columns]
    else:
        target_cols = [col for col in df.columns if df[col].dtype == object]

    for col in target_cols:
        if df[col].dtype != object:
            continue

        original_nulls = df[col].isna().sum()

        if trim:
            df[col] = df[col].str.strip()

        if normalize:
            import unicodedata

            def normalize_text_func(text: Any) -> Any:
                if pd.isna(text):
                    return text
                if not isinstance(text, str):
                    return text
                return unicodedata.normalize("NFKC", text)

            df[col] = df[col].apply(normalize_text_func)

        if case_normalization:
            if case_normalization == "upper":
                df[col] = df[col].str.upper()
            elif case_normalization == "lower":
                df[col] = df[col].str.lower()
            elif case_normalization == "title":
                df[col] = df[col].str.title()

        new_nulls = df[col].isna().sum()
        normalized_count += (len(df) - original_nulls) - (len(df) - new_nulls)

    # Reconstruct DataRecord objects
    cleaned_data = [
        DataRecord(data=row.to_dict(), schema=data[0].schema) for _, row in df.iterrows()
    ]

    report = {
        "normalized_count": normalized_count,
        "original_count": len(data),
        "final_count": len(cleaned_data),
    }

    return cleaned_data, report


def trim_whitespace(
    data: list[DataRecord],
    columns: tuple[str, ...] | None = None,
) -> list[DataRecord]:
    """
    Trim leading/trailing whitespace from text columns.

    Args:
        data: Input dataset
        columns: Columns to trim. If None, trims all string columns.

    Returns:
        Cleaned dataset

    Example:
        Trim whitespace::

            cleaned = trim_whitespace(data, columns=("name", "email"))
    """
    cleaned, _ = clean_text(data, trim=True, normalize=False, columns=columns)
    return cleaned


def normalize_case(
    data: list[DataRecord],
    case: str = "lower",
    columns: tuple[str, ...] | None = None,
) -> list[DataRecord]:
    """
    Normalize case in text columns.

    Args:
        data: Input dataset
        case: Case normalization ('upper', 'lower', 'title')
        columns: Columns to normalize. If None, normalizes all string columns.

    Returns:
        Cleaned dataset

    Example:
        Convert to lowercase::

            cleaned = normalize_case(data, case="lower", columns=("email",))
    """
    cleaned, _ = clean_text(
        data, trim=False, normalize=False, case_normalization=case, columns=columns
    )
    return cleaned


def remove_special_chars(
    data: list[DataRecord],
    columns: tuple[str, ...] | None = None,
    keep_alphanumeric: bool = True,
    keep_spaces: bool = True,
) -> list[DataRecord]:
    """
    Remove special characters from text columns.

    Args:
        data: Input dataset
        columns: Columns to clean
        keep_alphanumeric: Keep alphanumeric characters
        keep_spaces: Keep whitespace characters

    Returns:
        Cleaned dataset

    Example:
        Remove special characters::

            cleaned = remove_special_chars(
                data,
                columns=("name", "description"),
                keep_alphanumeric=True,
                keep_spaces=True
            )
    """
    if not data:
        return data

    import re

    df = pd.DataFrame([record.data for record in data])

    # Select columns to clean
    if columns:
        target_cols = [col for col in columns if col in df.columns]
    else:
        target_cols = [col for col in df.columns if df[col].dtype == object]

    for col in target_cols:
        if df[col].dtype != object:
            continue

        def clean_chars(text: Any) -> Any:
            if pd.isna(text):
                return text
            if not isinstance(text, str):
                return text

            if keep_alphanumeric and keep_spaces:
                return re.sub(r"[^a-zA-Z0-9\s]", "", text)
            elif keep_alphanumeric:
                return re.sub(r"[^a-zA-Z0-9]", "", text)
            else:
                return re.sub(r"\s+", "", text)

        df[col] = df[col].apply(clean_chars)

    # Reconstruct DataRecord objects
    cleaned_data = [
        DataRecord(data=row.to_dict(), schema=data[0].schema) for _, row in df.iterrows()
    ]

    return cleaned_data


# =============================================================================
# Utility Functions
# =============================================================================


def get_null_counts(data: list[DataRecord]) -> dict[str, int]:
    """
    Get count of null values per column.

    Args:
        data: Input dataset

    Returns:
        Dictionary mapping column names to null counts

    Example:
        Get null counts::

            null_counts = get_null_counts(data)
    """
    if not data:
        return {}

    df = pd.DataFrame([record.data for record in data])
    return df.isna().sum().to_dict()


def get_value_counts(
    data: list[DataRecord],
    column: str,
    top_n: int = 10,
) -> dict[Any, int]:
    """
    Get value counts for a column.

    Args:
        data: Input dataset
        column: Column name
        top_n: Return top N values

    Returns:
        Dictionary mapping values to counts

    Example:
        Get top 10 values::

            value_counts = get_value_counts(data, "category", top_n=10)
    """
    if not data:
        return {}

    df = pd.DataFrame([record.data for record in data])

    if column not in df.columns:
        return {}

    return df[column].value_counts().head(top_n).to_dict()


def get_data_profile(data: list[DataRecord]) -> dict[str, Any]:
    """
    Get basic profile of the dataset.

    Args:
        data: Input dataset

    Returns:
        Dictionary with dataset profile information

    Example:
        Get data profile::

            profile = get_data_profile(data)
    """
    if not data:
        return {}

    df = pd.DataFrame([record.data for record in data])

    return {
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": list(df.columns),
        "dtypes": df.dtypes.astype(str).to_dict(),
        "memory_usage_mb": df.memory_usage(deep=True).sum() / (1024 * 1024),
        "null_counts": df.isna().sum().to_dict(),
        "duplicate_rows": df.duplicated().sum(),
    }


def summarize_report(report: CleaningReport) -> str:
    """
    Generate a human-readable summary of the cleaning report.

    Args:
        report: Cleaning report to summarize

    Returns:
        Formatted summary string

    Example:
        Get summary::

            summary = summarize_report(report)
            print(summary)
    """
    lines = [
        "=== Data Cleaning Report ===",
        f"Original records: {report.original_count}",
        f"Final records: {report.final_count}",
        f"Records removed: {report.records_removed}",
        "",
        "Operations performed:",
    ]

    for op in report.operations:
        lines.append(f"  - {op}")

    lines.append("")
    lines.append("Details:")
    lines.append(f"  Duplicates removed: {report.duplicates_removed}")
    lines.append(f"  Nulls filled: {report.nulls_filled}")
    lines.append(f"  Outliers handled: {report.outliers_handled}")
    lines.append(f"  Text fields normalized: {report.text_normalized}")
    lines.append(f"  Type conversions: {report.types_converted}")
    lines.append("")
    lines.append(f"Duration: {report.duration_ms:.2f}ms")
    lines.append(f"Timestamp: {report.timestamp.isoformat()}")

    return "\n".join(lines)
