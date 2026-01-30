"""
Drift detection module for comparing data distributions.

This module provides methods for detecting distribution drift between
historical baseline data and new data:
- KS Test: Kolmogorov-Smirnov test for continuous distributions
- Chi-Square Test: For categorical distribution differences
- PSI: Population Stability Index for monitoring feature drift
- Baseline storage and retrieval for historical comparisons
- Threshold-based alerting for drift monitoring
- Drift history tracking over time

All methods provide statistical significance and actionable recommendations.
"""

from __future__ import annotations

import json
import statistics
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from vibe_piper.types import DataRecord, DataType, ValidationResult

if TYPE_CHECKING:
    from vibe_piper.types import Schema
else:
    # Import Schema at runtime for use in non-type-checked code
    from vibe_piper.types import Schema

# =============================================================================
# Configuration Types
# =============================================================================


@dataclass(frozen=True)
class DriftThresholds:
    """
    Configuration for drift detection thresholds.

    Attributes:
        warning: Threshold for warning level alerts (0-1)
        critical: Threshold for critical level alerts (0-1)
        psi_warning: PSI threshold for warnings (default 0.1)
        psi_critical: PSI threshold for critical alerts (default 0.2)
        ks_significance: Significance level for KS test (default 0.05)
    """

    warning: float = 0.1
    critical: float = 0.25
    psi_warning: float = 0.1
    psi_critical: float = 0.2
    ks_significance: float = 0.05

    def __post_init__(self) -> None:
        """Validate threshold values."""
        if not 0 <= self.warning <= 1:
            msg = f"warning threshold must be between 0 and 1, got {self.warning}"
            raise ValueError(msg)
        if not 0 <= self.critical <= 1:
            msg = f"critical threshold must be between 0 and 1, got {self.critical}"
            raise ValueError(msg)
        if self.warning >= self.critical:
            msg = f"warning threshold ({self.warning}) must be less than critical ({self.critical})"
            raise ValueError(msg)
        if self.psi_warning >= self.psi_critical:
            msg = f"psi_warning ({self.psi_warning}) must be less than psi_critical ({self.psi_critical})"
            raise ValueError(msg)


# =============================================================================
# Drift Detection Result Types
# =============================================================================


@dataclass(frozen=True)
class DriftResult:
    """
    Result of drift detection between two datasets.

    Attributes:
        method: Name of drift detection method used
        drift_score: Overall drift score (0-1, higher = more drift)
        drifted_columns: Columns with significant drift
        p_values: P-values from statistical tests (per column)
        statistics: Additional statistics about the drift analysis
        recommendations: Actionable recommendations based on drift
        timestamp: When drift was detected
    """

    method: str
    drift_score: float  # 0-1 scale, higher = more drift
    drifted_columns: tuple[str, ...]
    p_values: dict[str, float] = field(default_factory=dict)
    statistics: dict[str, Any] = field(default_factory=dict)
    recommendations: list[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass(frozen=True)
class ColumnDriftResult:
    """
    Drift result for a single column.

    Attributes:
        column_name: Name of the column
        drift_score: Drift score (0-1)
        p_value: Statistical significance p-value
        is_significant: Whether drift is statistically significant
        baseline_distribution: Summary of baseline distribution
        new_distribution: Summary of new distribution
        recommendation: Specific recommendation for this column
    """

    column_name: str
    drift_score: float  # 0-1 scale
    p_value: float
    is_significant: bool
    baseline_distribution: dict[str, Any]
    new_distribution: dict[str, Any]
    recommendation: str | None = None


@dataclass
class BaselineMetadata:
    """
    Metadata for a stored baseline.

    Attributes:
        baseline_id: Unique identifier for the baseline
        created_at: When the baseline was created
        sample_size: Number of records in the baseline
        columns: List of columns in the baseline
        description: Optional description of the baseline
    """

    baseline_id: str
    created_at: datetime
    sample_size: int
    columns: tuple[str, ...]
    description: str | None = None


@dataclass
class DriftHistoryEntry:
    """
    Single entry in drift history.

    Attributes:
        timestamp: When the drift check was performed
        baseline_id: ID of the baseline used for comparison
        method: Drift detection method used
        drift_score: Overall drift score
        max_drift_score: Maximum drift score across all columns
        drifted_columns: Columns with significant drift
        alert_level: Alert level (none, warning, critical)
    """

    timestamp: datetime
    baseline_id: str
    method: str
    drift_score: float
    max_drift_score: float
    drifted_columns: tuple[str, ...]
    alert_level: str  # "none", "warning", "critical"


# =============================================================================
# Baseline Storage
# =============================================================================


class BaselineStore:
    """
    Store and retrieve historical baselines for drift detection.

    Baselines are stored in JSON format in a configured directory.
    Supports adding, retrieving, listing, and deleting baselines.

    Example:
        >>> store = BaselineStore(storage_dir="./baselines")
        >>> baseline_id = store.add_baseline("production_baseline", historical_data, description="Production data from 2024-01-01")
        >>> baseline = store.get_baseline("production_baseline")
    """

    def __init__(self, storage_dir: str | Path = ".baselines") -> None:
        """
        Initialize baseline store.

        Args:
            storage_dir: Directory to store baseline files (default: .baselines)
        """
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)

    def _baseline_path(self, baseline_id: str) -> Path:
        """Get filesystem path for a baseline."""
        return self.storage_dir / f"{baseline_id}.json"

    def add_baseline(
        self,
        baseline_id: str,
        data: Sequence[DataRecord],
        description: str | None = None,
    ) -> BaselineMetadata:
        """
        Add a new baseline to the store.

        Args:
            baseline_id: Unique identifier for the baseline
            data: Historical data to use as baseline
            description: Optional description of the baseline

        Returns:
            BaselineMetadata with baseline information

        Raises:
            ValueError: If baseline_id already exists
        """
        baseline_path = self._baseline_path(baseline_id)

        if baseline_path.exists():
            msg = f"Baseline '{baseline_id}' already exists"
            raise ValueError(msg)

        if not data:
            msg = "Cannot create baseline from empty data"
            raise ValueError(msg)

        # Extract columns from data
        columns = tuple(sorted(data[0].data.keys()))

        # Store data as list of dicts (store both data and schema info)
        data_list = [{"data": dict(record.data)} for record in data]

        # Get schema name for reference
        schema_name = data[0].schema.name

        # Create metadata
        metadata = BaselineMetadata(
            baseline_id=baseline_id,
            created_at=datetime.utcnow(),
            sample_size=len(data),
            columns=columns,
            description=description,
        )

        # Prepare storage format
        storage = {
            "metadata": {
                "baseline_id": metadata.baseline_id,
                "created_at": metadata.created_at.isoformat(),
                "sample_size": metadata.sample_size,
                "columns": list(metadata.columns),
                "description": metadata.description,
                "schema_name": schema_name,
            },
            "data": data_list,
        }

        # Write to file
        with baseline_path.open("w") as f:
            json.dump(storage, f, indent=2)

        return metadata

    def get_baseline(
        self,
        baseline_id: str,
        schema: Schema | None = None,
    ) -> Sequence[DataRecord]:
        """
        Retrieve a baseline from storage.

        Args:
            baseline_id: ID of the baseline to retrieve
            schema: Optional schema to use when reconstructing DataRecords.
                     If None, creates a minimal schema.

        Returns:
            Sequence of DataRecords from the baseline

        Raises:
            FileNotFoundError: If baseline doesn't exist
        """
        baseline_path = self._baseline_path(baseline_id)

        if not baseline_path.exists():
            msg = f"Baseline '{baseline_id}' not found"
            raise FileNotFoundError(msg)

        with baseline_path.open("r") as f:
            storage = json.load(f)

        # Get schema name from metadata
        metadata = storage["metadata"]
        schema_name = metadata.get("schema_name", "baseline_schema")

        # Create minimal schema if not provided
        if schema is None:
            from vibe_piper.types import SchemaField

            fields = [
                SchemaField(name=col, data_type=DataType.ANY, required=False, nullable=True)
                for col in metadata["columns"]
            ]
            schema = Schema(name=schema_name, fields=tuple(fields))

        # Convert back to DataRecords
        return tuple(DataRecord(data=record["data"], schema=schema) for record in storage["data"])

    def get_metadata(self, baseline_id: str) -> BaselineMetadata:
        """
        Get metadata for a baseline without loading all data.

        Args:
            baseline_id: ID of the baseline

        Returns:
            BaselineMetadata for the baseline

        Raises:
            FileNotFoundError: If baseline doesn't exist
        """
        baseline_path = self._baseline_path(baseline_id)

        if not baseline_path.exists():
            msg = f"Baseline '{baseline_id}' not found"
            raise FileNotFoundError(msg)

        with baseline_path.open("r") as f:
            storage = json.load(f)

        meta_dict = storage["metadata"]
        return BaselineMetadata(
            baseline_id=meta_dict["baseline_id"],
            created_at=datetime.fromisoformat(meta_dict["created_at"]),
            sample_size=meta_dict["sample_size"],
            columns=tuple(meta_dict["columns"]),
            description=meta_dict.get("description"),
        )

    def list_baselines(self) -> list[BaselineMetadata]:
        """
        List all baselines in storage.

        Returns:
            List of BaselineMetadata for all baselines
        """
        baselines = []

        for path in self.storage_dir.glob("*.json"):
            try:
                metadata = self.get_metadata(path.stem)
                baselines.append(metadata)
            except Exception:
                # Skip invalid files
                continue

        return baselines

    def delete_baseline(self, baseline_id: str) -> None:
        """
        Delete a baseline from storage.

        Args:
            baseline_id: ID of the baseline to delete

        Raises:
            FileNotFoundError: If baseline doesn't exist
        """
        baseline_path = self._baseline_path(baseline_id)

        if not baseline_path.exists():
            msg = f"Baseline '{baseline_id}' not found"
            raise FileNotFoundError(msg)

        baseline_path.unlink()


# =============================================================================
# Drift History Tracking
# =============================================================================


class DriftHistory:
    """
    Track drift detection results over time.

    Stores history of drift checks for trend analysis and alerting.

    Example:
        >>> history = DriftHistory(storage_dir="./drift_history")
        >>> history.add_entry(result, "production_baseline", thresholds)
        >>> recent = history.get_recent_entries("production_baseline", n=10)
    """

    def __init__(self, storage_dir: str | Path = ".drift_history") -> None:
        """
        Initialize drift history tracker.

        Args:
            storage_dir: Directory to store history files (default: .drift_history)
        """
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)

    def _history_path(self, baseline_id: str) -> Path:
        """Get filesystem path for a baseline's history."""
        return self.storage_dir / f"{baseline_id}_history.jsonl"

    def add_entry(
        self,
        result: DriftResult,
        baseline_id: str,
        thresholds: DriftThresholds,
    ) -> DriftHistoryEntry:
        """
        Add a drift result to history.

        Args:
            result: DriftResult from drift detection
            baseline_id: ID of the baseline used for comparison
            thresholds: DriftThresholds used for alerting

        Returns:
            DriftHistoryEntry that was added
        """
        # Determine alert level
        if result.drift_score >= thresholds.critical:
            alert_level = "critical"
        elif result.drift_score >= thresholds.warning:
            alert_level = "warning"
        else:
            alert_level = "none"

        # Calculate max drift score from drifted columns
        max_drift_score = result.drift_score
        if result.drifted_columns and result.statistics:
            # Try to get max from statistics if available
            for col in result.drifted_columns:
                if col in result.p_values:
                    # Use drift score as proxy
                    pass

        entry = DriftHistoryEntry(
            timestamp=datetime.utcnow(),
            baseline_id=baseline_id,
            method=result.method,
            drift_score=result.drift_score,
            max_drift_score=max_drift_score,
            drifted_columns=result.drifted_columns,
            alert_level=alert_level,
        )

        # Append to history file
        history_path = self._history_path(baseline_id)
        with history_path.open("a") as f:
            line = json.dumps(
                {
                    "timestamp": entry.timestamp.isoformat(),
                    "baseline_id": entry.baseline_id,
                    "method": entry.method,
                    "drift_score": entry.drift_score,
                    "max_drift_score": entry.max_drift_score,
                    "drifted_columns": list(entry.drifted_columns),
                    "alert_level": entry.alert_level,
                }
            )
            f.write(line + "\n")

        return entry

    def get_entries(self, baseline_id: str, limit: int | None = None) -> list[DriftHistoryEntry]:
        """
        Get drift history for a baseline.

        Args:
            baseline_id: ID of the baseline
            limit: Maximum number of entries to return (most recent first)

        Returns:
            List of DriftHistoryEntry
        """
        history_path = self._history_path(baseline_id)

        if not history_path.exists():
            return []

        entries = []
        with history_path.open("r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                entry_dict = json.loads(line)
                entry = DriftHistoryEntry(
                    timestamp=datetime.fromisoformat(entry_dict["timestamp"]),
                    baseline_id=entry_dict["baseline_id"],
                    method=entry_dict["method"],
                    drift_score=entry_dict["drift_score"],
                    max_drift_score=entry_dict["max_drift_score"],
                    drifted_columns=tuple(entry_dict["drifted_columns"]),
                    alert_level=entry_dict["alert_level"],
                )
                entries.append(entry)

        # Sort by timestamp descending and apply limit
        entries.sort(key=lambda e: e.timestamp, reverse=True)
        if limit:
            entries = entries[:limit]

        return entries

    def get_trend(self, baseline_id: str, window: int = 10) -> dict[str, Any]:
        """
        Get drift trend statistics for a baseline.

        Args:
            baseline_id: ID of the baseline
            window: Number of recent entries to analyze

        Returns:
            Dictionary with trend statistics
        """
        entries = self.get_entries(baseline_id, limit=window)

        if not entries:
            return {
                "baseline_id": baseline_id,
                "window": window,
                "count": 0,
                "avg_drift_score": 0.0,
                "max_drift_score": 0.0,
                "min_drift_score": 0.0,
                "critical_count": 0,
                "warning_count": 0,
                "none_count": 0,
                "trend": "stable",
            }

        drift_scores = [e.drift_score for e in entries]
        critical_count = sum(1 for e in entries if e.alert_level == "critical")
        warning_count = sum(1 for e in entries if e.alert_level == "warning")
        none_count = sum(1 for e in entries if e.alert_level == "none")

        # Determine trend
        if len(drift_scores) >= 3:
            recent_avg = sum(drift_scores[:3]) / 3
            older_avg = sum(drift_scores[3:]) / len(drift_scores[3:])
            if recent_avg > older_avg * 1.2:
                trend = "increasing"
            elif recent_avg < older_avg * 0.8:
                trend = "decreasing"
            else:
                trend = "stable"
        else:
            trend = "insufficient_data"

        return {
            "baseline_id": baseline_id,
            "window": window,
            "count": len(entries),
            "avg_drift_score": sum(drift_scores) / len(drift_scores),
            "max_drift_score": max(drift_scores),
            "min_drift_score": min(drift_scores),
            "critical_count": critical_count,
            "warning_count": warning_count,
            "none_count": none_count,
            "trend": trend,
        }

    def clear_history(self, baseline_id: str) -> None:
        """
        Clear drift history for a baseline.

        Args:
            baseline_id: ID of the baseline
        """
        history_path = self._history_path(baseline_id)
        if history_path.exists():
            history_path.unlink()


# =============================================================================
# Alerting Logic
# =============================================================================


def check_drift_alert(result: DriftResult, thresholds: DriftThresholds) -> tuple[bool, str]:
    """
    Check if drift exceeds alerting thresholds.

    Args:
        result: DriftResult from drift detection
        thresholds: DriftThresholds configuration

    Returns:
        Tuple of (should_alert, alert_level) where alert_level is 'none', 'warning', or 'critical'
    """
    if result.drift_score >= thresholds.critical:
        return True, "critical"
    elif result.drift_score >= thresholds.warning:
        return True, "warning"
    return False, "none"


# =============================================================================
# Validation Check Wrappers
# =============================================================================


def check_drift_ks(
    column: str,
    baseline: Sequence[DataRecord],
    thresholds: DriftThresholds | None = None,
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """
    Create a drift check function compatible with @validate decorator using KS test.

    This wrapper allows drift detection to be used seamlessly with the @validate decorator.

    Args:
        column: Column name to check for drift
        baseline: Historical baseline data to compare against
        thresholds: Optional drift thresholds for alerting

    Returns:
        Validation check function that takes new data and returns ValidationResult

    Example:
        >>> baseline_data = [...]  # Your historical data
        >>> check = check_drift_ks("price", baseline_data)
        >>> @validate(checks=[check])
        >>> @asset
        >>> def new_price_data():
        ...     return new_records
    """

    if thresholds is None:
        thresholds = DriftThresholds()

    def validate_new_data(new_data: Sequence[DataRecord]) -> ValidationResult:
        detector = detect_drift_ks(column, significance_level=thresholds.ks_significance)
        result = detector((baseline, new_data))

        # Check for alerts
        should_alert, alert_level = check_drift_alert(result, thresholds)

        # Build validation result
        errors: list[str] = []
        warnings: list[str] = []

        if alert_level == "critical":
            errors.append(
                f"CRITICAL DRIFT DETECTED in column '{column}': "
                f"drift_score={result.drift_score:.3f} (threshold={thresholds.critical})"
            )
            errors.extend(result.recommendations)
        elif alert_level == "warning":
            warnings.append(
                f"WARNING: Drift detected in column '{column}': "
                f"drift_score={result.drift_score:.3f} (threshold={thresholds.warning})"
            )
            warnings.extend(result.recommendations)
        else:
            # No alert, add as info
            if result.recommendations:
                msg = f"Drift check passed for column '{column}': {result.recommendations[0]}"
                # We don't have an "info" level, so use warnings
                warnings.append(msg)

        # Include drifted columns in warnings for visibility
        if result.drifted_columns:
            warnings.append(f"Drifted columns: {', '.join(result.drifted_columns)}")

        return ValidationResult(
            is_valid=(alert_level != "critical"),
            errors=tuple(errors),
            warnings=tuple(warnings),
        )

    return validate_new_data


def check_drift_psi(
    column: str,
    baseline: Sequence[DataRecord],
    thresholds: DriftThresholds | None = None,
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """
    Create a drift check function compatible with @validate decorator using PSI.

    This wrapper allows drift detection to be used seamlessly with the @validate decorator.

    Args:
        column: Column name to check for drift
        baseline: Historical baseline data to compare against
        thresholds: Optional drift thresholds for alerting

    Returns:
        Validation check function that takes new data and returns ValidationResult

    Example:
        >>> baseline_data = [...]  # Your historical data
        >>> check = check_drift_psi("income", baseline_data)
        >>> @validate(checks=[check])
        >>> @asset
        >>> def new_income_data():
        ...     return new_records
    """

    if thresholds is None:
        thresholds = DriftThresholds()

    def validate_new_data(new_data: Sequence[DataRecord]) -> ValidationResult:
        detector = detect_drift_psi(column, num_bins=10, psi_threshold=thresholds.psi_critical)
        result = detector((baseline, new_data))

        # Check for alerts
        should_alert, alert_level = check_drift_alert(result, thresholds)

        # Build validation result
        errors: list[str] = []
        warnings: list[str] = []

        if alert_level == "critical":
            errors.append(
                f"CRITICAL DRIFT DETECTED in column '{column}': "
                f"drift_score={result.drift_score:.3f} (threshold={thresholds.critical})"
            )
            errors.extend(result.recommendations)
        elif alert_level == "warning":
            warnings.append(
                f"WARNING: Drift detected in column '{column}': "
                f"drift_score={result.drift_score:.3f} (threshold={thresholds.warning})"
            )
            warnings.extend(result.recommendations)
        else:
            # No alert, add as info
            if result.recommendations:
                msg = f"Drift check passed for column '{column}': {result.recommendations[0]}"
                warnings.append(msg)

        # Include drifted columns in warnings for visibility
        if result.drifted_columns:
            warnings.append(f"Drifted columns: {', '.join(result.drifted_columns)}")

        return ValidationResult(
            is_valid=(alert_level != "critical"),
            errors=tuple(errors),
            warnings=tuple(warnings),
        )

    return validate_new_data


# =============================================================================
# Kolmogorov-Smirnov (KS) Test
# =============================================================================


def detect_drift_ks(
    column: str,
    significance_level: float = 0.05,
    min_samples: int = 50,
) -> Callable[[tuple[Sequence[DataRecord], Sequence[DataRecord]]], DriftResult]:
    """
    Detect drift using Kolmogorov-Smirnov (KS) test.

    KS test compares cumulative distribution functions of two samples.
    Good for detecting shifts in continuous distributions.

    Args:
        column: Column name to check for drift
        significance_level: Alpha for statistical significance (default: 0.05)
        min_samples: Minimum samples required in each dataset

    Returns:
        Function that produces DriftResult when applied to (historical, new) data

    Example:
        >>> detector = detect_drift_ks("revenue", significance_level=0.01)
        >>> result = detector((historical_data, new_data))
    """

    def validate(data: tuple[Sequence[DataRecord], Sequence[DataRecord]]) -> DriftResult:
        historical, new_data = data

        if len(historical) < min_samples or len(new_data) < min_samples:
            return DriftResult(
                method="ks_test",
                drift_score=0.0,
                drifted_columns=(),
                statistics={"error": f"Need at least {min_samples} samples in each dataset"},
                recommendations=["Insufficient sample size for KS test"],
            )

        # Extract column values
        historical_values = sorted(
            [float(r.get(column)) for r in historical if isinstance(r.get(column), (int, float))]
        )
        new_values = sorted(
            [float(r.get(column)) for r in new_data if isinstance(r.get(column), (int, float))]
        )

        if not historical_values or not new_values:
            return DriftResult(
                method="ks_test",
                drift_score=0.0,
                drifted_columns=(),
                statistics={"error": "No valid numeric values"},
            )

        # Calculate KS statistic
        from scipy.stats import ks_2samp

        ks_statistic, p_value = ks_2samp(historical_values, new_values)

        # KS statistic is the maximum difference in CDFs
        drift_score = float(ks_statistic)  # Can be 0-1 range for small shifts
        is_significant = p_value < significance_level

        # Column drift result
        column_result = ColumnDriftResult(
            column_name=column,
            drift_score=min(drift_score, 1.0),  # Cap at 1.0
            p_value=p_value,
            is_significant=is_significant,
            baseline_distribution={
                "count": len(historical_values),
                "min": min(historical_values),
                "max": max(historical_values),
                "mean": statistics.mean(historical_values),
                "median": statistics.median(historical_values),
            },
            new_distribution={
                "count": len(new_values),
                "min": min(new_values),
                "max": max(new_values),
                "mean": statistics.mean(new_values),
                "median": statistics.median(new_values),
            },
            recommendation=(
                f"Re-train model or update thresholds. KS statistic: {drift_score:.3f}, p-value: {p_value:.4f}"
                if is_significant
                else "No significant drift detected"
            ),
        )

        # Generate recommendations
        recommendations = []
        if is_significant:
            recommendations.extend(
                [
                    f"Significant drift detected in column '{column}' (p-value: {p_value:.4f})",
                    "Consider retraining models with recent data",
                    "Check for data pipeline changes or upstream data issues",
                    f"Drift magnitude: {drift_score:.3f} (>0.1 indicates meaningful shift)",
                ]
            )
        else:
            recommendations.append(f"No significant drift in column '{column}'")

        return DriftResult(
            method="ks_test",
            drift_score=column_result.drift_score,
            drifted_columns=(column,) if is_significant else (),
            p_values={column: p_value},
            statistics={
                "ks_statistic": ks_statistic,
                "p_value": p_value,
                "significance_level": significance_level,
                "is_significant": is_significant,
            },
            recommendations=recommendations,
        )

    return validate


# =============================================================================
# Chi-Square Test
# =============================================================================


def detect_drift_chi_square(
    column: str,
    significance_level: float = 0.05,
    min_samples: int = 30,
) -> Callable[[tuple[Sequence[DataRecord], Sequence[DataRecord]]], DriftResult]:
    """
    Detect drift using Chi-Square test for categorical distributions.

    Chi-square test compares observed vs expected frequencies.
    Good for detecting changes in categorical variable distributions.

    Args:
        column: Column name to check for drift
        significance_level: Alpha for statistical significance (default: 0.05)
        min_samples: Minimum samples required in each dataset

    Returns:
        Function that produces DriftResult when applied to (historical, new) data

    Example:
        >>> detector = detect_drift_chi_square("category", significance_level=0.01)
        >>> result = detector((historical_data, new_data))
    """

    def validate(data: tuple[Sequence[DataRecord], Sequence[DataRecord]]) -> DriftResult:
        historical, new_data = data

        if len(historical) < min_samples or len(new_data) < min_samples:
            return DriftResult(
                method="chi_square",
                drift_score=0.0,
                drifted_columns=(),
                statistics={"error": f"Need at least {min_samples} samples in each dataset"},
                recommendations=["Insufficient sample size for Chi-square test"],
            )

        # Extract column values (treat as categorical)
        historical_values = [str(r.get(column)) for r in historical if r.get(column) is not None]
        new_values = [str(r.get(column)) for r in new_data if r.get(column) is not None]

        if not historical_values or not new_values:
            return DriftResult(
                method="chi_square",
                drift_score=0.0,
                drifted_columns=(),
                statistics={"error": "No valid categorical values"},
            )

        # Build contingency table
        historical_counter = Counter(historical_values)
        new_counter = Counter(new_values)
        all_categories = set(historical_counter.keys()) | set(new_counter.keys())

        # Expected frequencies based on combined distribution
        total_historical = len(historical_values)
        total_new = len(new_values)
        total_combined = total_historical + total_new

        # Create observed and expected arrays
        observed = []
        expected = []

        for cat in all_categories:
            hist_count = historical_counter.get(cat, 0)
            new_count = new_counter.get(cat, 0)
            observed.extend([hist_count, new_count])

            # Expected: based on combined proportions
            combined_count = hist_count + new_count
            expected_historical = combined_count * (total_historical / total_combined)
            expected_new = combined_count * (total_new / total_combined)
            expected.extend([expected_historical, expected_new])

        # Perform chi-square test
        from scipy.stats import chisquare

        chi2_statistic, p_value = chisquare(f_observed=observed, f_exp=expected)

        # Calculate drift score (normalized chi2 statistic)
        drift_score = min(float(chi2_statistic) / (len(all_categories) * 2), 1.0)
        is_significant = p_value < significance_level

        # Column drift result
        column_result = ColumnDriftResult(
            column_name=column,
            drift_score=drift_score,
            p_value=p_value,
            is_significant=is_significant,
            baseline_distribution={
                "unique_categories": len(historical_counter),
                "top_categories": dict(historical_counter.most_common(5)),
            },
            new_distribution={
                "unique_categories": len(new_counter),
                "top_categories": dict(new_counter.most_common(5)),
            },
            recommendation=(
                f"Re-train model on new categories. Chi-square: {chi2_statistic:.2f}, p-value: {p_value:.4f}"
                if is_significant
                else "No significant distribution shift detected"
            ),
        )

        # Generate recommendations
        recommendations = []
        if is_significant:
            new_categories = set(new_counter.keys()) - set(historical_counter.keys())
            lost_categories = set(historical_counter.keys()) - set(new_counter.keys())

            recommendations.extend(
                [
                    f"Significant categorical drift in '{column}' (p-value: {p_value:.4f})",
                    f"New categories: {len(new_categories)}, Lost categories: {len(lost_categories)}",
                    "Consider updating schema to handle new categories",
                    "Review data pipeline for changes in categorization logic",
                ]
            )
        else:
            recommendations.append(f"No significant drift in column '{column}'")

        return DriftResult(
            method="chi_square",
            drift_score=column_result.drift_score,
            drifted_columns=(column,) if is_significant else (),
            p_values={column: p_value},
            statistics={
                "chi2_statistic": chi2_statistic,
                "p_value": p_value,
                "significance_level": significance_level,
                "is_significant": is_significant,
            },
            recommendations=recommendations,
        )

    return validate


# =============================================================================
# Population Stability Index (PSI)
# =============================================================================


def detect_drift_psi(
    column: str,
    num_bins: int = 10,
    psi_threshold: float = 0.25,
    min_samples: int = 50,
) -> Callable[[tuple[Sequence[DataRecord], Sequence[DataRecord]]], DriftResult]:
    """
    Detect drift using Population Stability Index (PSI).

    PSI measures how much a variable has shifted in distribution.
    Commonly used in credit scoring and monitoring:
    - PSI < 0.1: No significant change
    - 0.1 <= PSI < 0.2: Moderate change
    - PSI >= 0.2: Significant shift

    Args:
        column: Column name to check for drift
        num_bins: Number of bins for discretization (default: 10)
        psi_threshold: PSI threshold for significance (default: 0.25)
        min_samples: Minimum samples required in each dataset

    Returns:
        Function that produces DriftResult when applied to (historical, new) data

    Example:
        >>> detector = detect_drift_psi("income", num_bins=20, psi_threshold=0.2)
        >>> result = detector((historical_data, new_data))
    """

    def validate(data: tuple[Sequence[DataRecord], Sequence[DataRecord]]) -> DriftResult:
        historical, new_data = data

        if len(historical) < min_samples or len(new_data) < min_samples:
            return DriftResult(
                method="psi",
                drift_score=0.0,
                drifted_columns=(),
                statistics={"error": f"Need at least {min_samples} samples in each dataset"},
                recommendations=["Insufficient sample size for PSI calculation"],
            )

        # Extract column values
        historical_values = [
            float(r.get(column)) for r in historical if isinstance(r.get(column), (int, float))
        ]
        new_values = [
            float(r.get(column)) for r in new_data if isinstance(r.get(column), (int, float))
        ]

        if not historical_values or not new_values:
            return DriftResult(
                method="psi",
                drift_score=0.0,
                drifted_columns=(),
                statistics={"error": "No valid numeric values"},
            )

        # Determine bins based on historical distribution
        min_val = min(historical_values + new_values)
        max_val = max(historical_values + new_values)
        bin_edges = [min_val + i * (max_val - min_val) / num_bins for i in range(num_bins + 1)]

        # Calculate bin counts for historical and new
        hist_bins = [0] * num_bins
        new_bins = [0] * num_bins

        for val in historical_values:
            bin_idx = min(len(bin_edges) - 2, int((val - min_val) / (max_val - min_val) * num_bins))
            hist_bins[bin_idx] += 1

        for val in new_values:
            bin_idx = min(len(bin_edges) - 2, int((val - min_val) / (max_val - min_val) * num_bins))
            new_bins[bin_idx] += 1

        # Normalize to proportions
        total_hist = len(historical_values)
        total_new = len(new_values)
        hist_props = [count / total_hist if total_hist > 0 else 0 for count in hist_bins]
        new_props = [count / total_new if total_new > 0 else 0 for count in new_bins]

        # Calculate PSI per bin
        psi_values = []
        for i, (h_prop, n_prop) in enumerate(zip(hist_props, new_props)):
            if n_prop == 0:
                psi_per_bin = 0
            else:
                psi_per_bin = (n_prop - h_prop) * (0 if h_prop == 0 else (n_prop / h_prop - 1))
            psi_values.append(psi_per_bin)

        total_psi = sum(psi_values)

        # Normalize PSI for interpretability
        psi_score = min(total_psi, 2.0)  # Can exceed 1.0 for large shifts
        is_significant = psi_score >= psi_threshold

        # Column drift result
        column_result = ColumnDriftResult(
            column_name=column,
            drift_score=min(psi_score, 1.0),
            p_value=1.0 if is_significant else 0.0,  # PSI doesn't have p-value
            is_significant=is_significant,
            baseline_distribution={
                "num_bins": num_bins,
                "bin_edges": bin_edges,
                "bin_proportions": hist_props,
            },
            new_distribution={
                "num_bins": num_bins,
                "bin_proportions": new_props,
            },
            recommendation=(
                f"Retrain model. PSI: {psi_score:.3f} (threshold: {psi_threshold})"
                if is_significant
                else "No significant population shift detected"
            ),
        )

        # Generate recommendations
        recommendations = []
        if is_significant:
            recommendations.extend(
                [
                    f"Significant population shift in '{column}' (PSI: {psi_score:.3f})",
                    "Consider retraining prediction models",
                    "Review feature engineering and data preprocessing",
                    "Check for changes in data collection or source systems",
                ]
            )
        else:
            recommendations.append(f"No significant drift in column '{column}'")

        return DriftResult(
            method="psi",
            drift_score=column_result.drift_score,
            drifted_columns=(column,) if is_significant else (),
            p_values={column: column_result.p_value},
            statistics={
                "psi_score": psi_score,
                "psi_threshold": psi_threshold,
                "num_bins": num_bins,
                "is_significant": is_significant,
            },
            recommendations=recommendations,
        )

    return validate


# =============================================================================
# Multi-Method Drift Detection
# =============================================================================


def detect_drift_multi_method(
    columns: list[str],
    methods: list[str] = ["ks_test", "psi"],
    ks_significance_level: float = 0.05,
    psi_num_bins: int = 10,
    psi_threshold: float = 0.25,
) -> Callable[[tuple[Sequence[DataRecord], Sequence[DataRecord]]], dict[str, DriftResult]]:
    """
    Detect drift using multiple methods across multiple columns.

    Useful for comprehensive drift analysis comparing results from
    different detection algorithms.

    Args:
        columns: List of columns to analyze
        methods: List of methods ('ks_test', 'chi_square', 'psi')
        ks_significance_level: Alpha for KS test
        psi_num_bins: Number of bins for PSI discretization
        psi_threshold: PSI threshold for significance

    Returns:
        Function that produces dict mapping column+method to DriftResult

    Example:
        >>> detector = detect_drift_multi_method(['price', 'quantity'], methods=['ks_test', 'psi'])
        >>> results = detector((historical_data, new_data))
        >>> ks_result = results['price']['ks_test']
    """

    def validate(data: tuple[Sequence[DataRecord], Sequence[DataRecord]]) -> dict[str, DriftResult]:
        results = {}

        for col in columns:
            for method in methods:
                key = f"{col}_{method}"
                if method == "ks_test":
                    detector = detect_drift_ks(col, significance_level=ks_significance_level)
                    results[key] = detector(data)
                elif method == "chi_square":
                    detector = detect_drift_chi_square(col)
                    results[key] = detector(data)
                elif method == "psi":
                    detector = detect_drift_psi(
                        col, num_bins=psi_num_bins, psi_threshold=psi_threshold
                    )
                    results[key] = detector(data)

        return results

    return validate


__all__ = [
    # Result types
    "DriftResult",
    "ColumnDriftResult",
    "BaselineMetadata",
    "DriftHistoryEntry",
    # Configuration
    "DriftThresholds",
    # Storage and history
    "BaselineStore",
    "DriftHistory",
    # Drift detection methods
    "detect_drift_ks",
    "detect_drift_chi_square",
    "detect_drift_psi",
    "detect_drift_multi_method",
    # Alerting
    "check_drift_alert",
    # Validation check wrappers (for @validate decorator)
    "check_drift_ks",
    "check_drift_psi",
]
