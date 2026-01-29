"""
Anomaly detection module for identifying outliers in data.

This module provides multiple algorithms for detecting anomalies:
- Z-score: Standard score-based outlier detection
- IQR: Interquartile range-based outlier detection
- Isolation Forest: ML-based anomaly detection using scikit-learn

All methods return structured results with outlier indices and scores.
"""

from __future__ import annotations

import statistics
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from vibe_piper.types import DataRecord, ValidationResult

if TYPE_CHECKING:
    pass

# =============================================================================
# Anomaly Detection Result Types
# =============================================================================


@dataclass(frozen=True)
class AnomalyResult:
    """
    Result of anomaly detection on a dataset.

    Attributes:
        method: Name of detection method used
        outlier_indices: Indices of records identified as anomalies
        outlier_count: Number of anomalies detected
        anomaly_scores: Scores for all records (higher = more anomalous)
        threshold: Threshold used to classify outliers
        statistics: Additional statistics about detection
        timestamp: When detection was performed
    """

    method: str
    outlier_indices: tuple[int, ...]
    outlier_count: int
    anomaly_scores: tuple[float, ...] = field(default_factory=tuple)
    threshold: float = 0.0
    statistics: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def get_anomaly_rate(self) -> float:
        """Calculate the rate of anomalies in the dataset."""
        if not self.anomaly_scores:
            return 0.0
        return self.outlier_count / len(self.anomaly_scores)


# =============================================================================
# Statistical Anomaly Detection Methods
# =============================================================================


def detect_anomalies_zscore(
    column: str,
    threshold: float = 3.0,
    use_modified_zscore: bool = False,
) -> Callable[[Sequence[DataRecord]], AnomalyResult]:
    """
    Detect anomalies using Z-score method.

    Z-score measures how many standard deviations a value is from the mean.
    Values with |z| > threshold are flagged as anomalies.

    Args:
        column: Column name to check for anomalies
        threshold: Z-score threshold (default: 3.0, captures ~99.7% of data)
        use_modified_zscore: Use median/MAD instead of mean/std (more robust)

    Returns:
        Function that produces AnomalyResult when applied to data

    Example:
        >>> detector = detect_anomalies_zscore("price", threshold=2.5)
        >>> result = detector(records)
    """

    def validate(records: Sequence[DataRecord]) -> AnomalyResult:
        if not records:
            return AnomalyResult(
                method="zscore",
                outlier_indices=(),
                outlier_count=0,
                anomaly_scores=(),
                statistics={"total_records": 0},
            )

        # Extract numeric values
        values = []
        for record in records:
            val = record.get(column)
            if isinstance(val, (int, float)):
                values.append(float(val))

        if len(values) < 2:
            return AnomalyResult(
                method="zscore",
                outlier_indices=(),
                outlier_count=0,
                anomaly_scores=(),
                statistics={"error": "Need at least 2 numeric values"},
            )

        if use_modified_zscore:
            # Modified Z-score: uses median and MAD (more robust to outliers)
            median_val = statistics.median(values)
            mad = statistics.median(
                [abs(v - median_val) for v in values]
            )  # Median Absolute Deviation
            # Constant factor for normality approximation
            mad_scaled = 1.4826 * mad if mad != 0 else 1.0
            scores = [abs(v - median_val) / mad_scaled for v in values]
            center = median_val
            scale = mad
        else:
            # Standard Z-score
            mean_val = statistics.mean(values)
            stdev = statistics.stdev(values) if len(values) > 1 else 0.0
            stdev = stdev if stdev != 0 else 1.0
            scores = [abs((v - mean_val) / stdev) for v in values]
            center = mean_val
            scale = stdev

        # Identify outliers
        outlier_indices = []
        current_idx = 0
        for idx, record in enumerate(records):
            val = record.get(column)
            if isinstance(val, (int, float)):
                if scores[current_idx] > threshold:
                    outlier_indices.append(idx)
                current_idx += 1

        return AnomalyResult(
            method="modified_zscore" if use_modified_zscore else "zscore",
            outlier_indices=tuple(outlier_indices),
            outlier_count=len(outlier_indices),
            anomaly_scores=tuple(scores),
            threshold=threshold,
            statistics={
                "mean": center,
                "std_dev": scale,
                "min_score": min(scores),
                "max_score": max(scores),
                "median_score": statistics.median(scores),
            },
        )

    return validate


def detect_anomalies_iqr(
    column: str,
    multiplier: float = 1.5,
    use_quartiles: tuple[float, float] | None = None,
) -> Callable[[Sequence[DataRecord]], AnomalyResult]:
    """
    Detect anomalies using Interquartile Range (IQR) method.

    IQR identifies outliers as values outside Q1 - k*IQR or Q3 + k*IQR.
    Default k=1.5 (Tukey's fences), captures ~99.3% of data for normal.

    Args:
        column: Column name to check for anomalies
        multiplier: IQR multiplier (default: 1.5 for Tukey's fences)
                      Use 3.0 for extreme outliers only
        use_quartiles: Custom Q1 and Q3 values (optional)

    Returns:
        Function that produces AnomalyResult when applied to data
    """

    def validate(records: Sequence[DataRecord]) -> AnomalyResult:
        if not records:
            return AnomalyResult(
                method="iqr",
                outlier_indices=(),
                outlier_count=0,
                anomaly_scores=(),
                statistics={"total_records": 0},
            )

        # Extract numeric values
        values = []
        for record in records:
            val = record.get(column)
            if isinstance(val, (int, float)):
                values.append(float(val))

        if len(values) < 4:
            return AnomalyResult(
                method="iqr",
                outlier_indices=(),
                outlier_count=0,
                anomaly_scores=(),
                statistics={"error": "Need at least 4 values for IQR"},
            )

        # Calculate quartiles
        sorted_vals = sorted(values)
        n = len(sorted_vals)

        if use_quartiles:
            q1, q3 = use_quartiles
        else:
            # Linear interpolation for quartiles
            q1_pos = (n + 1) / 4.0 - 1
            q3_pos = (3 * (n + 1) / 4.0) - 1

            q1_floor = int(q1_pos)
            q3_floor = int(q3_pos)
            q1_ceil = q1_floor + 1
            q3_ceil = q3_floor + 1

            q1 = sorted_vals[q1_floor] + (q1_pos - q1_floor) * (
                sorted_vals[q1_ceil] - sorted_vals[q1_floor]
            )
            q3 = sorted_vals[q3_floor] + (q3_pos - q3_floor) * (
                sorted_vals[q3_ceil] - sorted_vals[q3_floor]
            )

        iqr = q3 - q1
        lower_bound = q1 - multiplier * iqr
        upper_bound = q3 + multiplier * iqr

        # Identify outliers and create anomaly scores
        outlier_indices = []
        scores = []
        for idx, record in enumerate(records):
            val = record.get(column)
            if isinstance(val, (int, float)):
                # Score based on distance from bounds
                if val < lower_bound:
                    distance = lower_bound - val
                    score = distance / (iqr if iqr != 0 else 1.0)
                elif val > upper_bound:
                    distance = val - upper_bound
                    score = distance / (iqr if iqr != 0 else 1.0)
                else:
                    score = 0.0
                scores.append(score)

                if score > 0:
                    outlier_indices.append(idx)

        return AnomalyResult(
            method="iqr",
            outlier_indices=tuple(outlier_indices),
            outlier_count=len(outlier_indices),
            anomaly_scores=tuple(scores),
            statistics={
                "q1": q1,
                "q3": q3,
                "iqr": iqr,
                "lower_bound": lower_bound,
                "upper_bound": upper_bound,
                "multiplier": multiplier,
            },
        )

    return validate


# =============================================================================
# ML-based Anomaly Detection
# =============================================================================


def detect_anomalies_isolation_forest(
    column: str,
    contamination: float = 0.1,
    n_estimators: int = 100,
    max_samples: int | str = "auto",
    random_state: int = 42,
) -> Callable[[Sequence[DataRecord]], AnomalyResult]:
    """
    Detect anomalies using Isolation Forest algorithm.

    Isolation Forest isolates anomalies by randomly selecting features and
    split values. Anomalies are easier to isolate (shorter path length).

    Args:
        column: Column name to check for anomalies
        contamination: Expected proportion of outliers (0-1, default: 0.1)
        n_estimators: Number of trees in the forest (default: 100)
        max_samples: Number of samples to draw for each tree (default: 'auto')
        random_state: Random seed for reproducibility

    Returns:
        Function that produces AnomalyResult when applied to data

    Example:
        >>> detector = detect_anomalies_isolation_forest("amount", contamination=0.05)
        >>> result = detector(records)
    """

    def validate(records: Sequence[DataRecord]) -> AnomalyResult:
        if not records:
            return AnomalyResult(
                method="isolation_forest",
                outlier_indices=(),
                outlier_count=0,
                anomaly_scores=(),
                statistics={"total_records": 0},
            )

        # Extract numeric values and record indices
        values = []
        valid_indices = []
        for idx, record in enumerate(records):
            val = record.get(column)
            if isinstance(val, (int, float)):
                values.append(float(val))
                valid_indices.append(idx)

        if len(values) < 2:
            return AnomalyResult(
                method="isolation_forest",
                outlier_indices=(),
                outlier_count=0,
                anomaly_scores=(),
                statistics={"error": "Need at least 2 numeric values"},
            )

        # Import scikit-learn only when needed (optional dependency)
        try:
            from sklearn.ensemble import IsolationForest  # type: ignore[import-untyped]
        except ImportError as e:
            msg = (
                "scikit-learn is required for Isolation Forest. "
                "Install with: pip install scikit-learn"
            )
            return AnomalyResult(
                method="isolation_forest",
                outlier_indices=(),
                outlier_count=0,
                anomaly_scores=(),
                statistics={"error": str(e), "hint": msg},
            )

        # Convert to numpy array
        try:
            import numpy as np

            X = np.array(values).reshape(-1, 1)
        except ImportError:
            # Fallback without numpy - return empty result
            return AnomalyResult(
                method="isolation_forest",
                outlier_indices=(),
                outlier_count=0,
                anomaly_scores=(),
                statistics={"error": "numpy is required for Isolation Forest"},
            )

        # Fit Isolation Forest
        clf = IsolationForest(
            n_estimators=n_estimators,
            contamination=contamination,
            max_samples=max_samples,
            random_state=random_state,
        )
        clf.fit(X)

        # Predict anomalies (-1 = anomaly, 1 = normal)
        predictions = clf.predict(X)
        # Get anomaly scores (lower = more anomalous)
        scores = clf.score_samples(X)

        # Convert scores to positive (higher = more anomalous)
        max_score = max(scores)
        normalized_scores = [max_score - s for s in scores]

        # Map back to original record indices
        outlier_indices = []
        for i, pred in enumerate(predictions):
            if pred == -1:  # Anomaly
                outlier_indices.append(valid_indices[i])

        return AnomalyResult(
            method="isolation_forest",
            outlier_indices=tuple(outlier_indices),
            outlier_count=len(outlier_indices),
            anomaly_scores=tuple(normalized_scores),
            statistics={
                "contamination": contamination,
                "n_estimators": n_estimators,
                "min_score": min(normalized_scores),
                "max_score": max(normalized_scores),
                "median_score": statistics.median(normalized_scores),
            },
        )

    return validate


# =============================================================================
# Anomaly Ranking and Aggregation
# =============================================================================


@dataclass(frozen=True)
class AnomalyRankingResult:
    """
    Result of ranking anomalies across multiple records.

    Attributes:
        column: Column name analyzed
        ranked_indices: Indices sorted by anomaly score (highest first)
        ranked_scores: Anomaly scores sorted descending
        top_n_indices: Top N most anomalous records
        top_n_scores: Scores of top N anomalies
        total_anomalies: Total number of records flagged
    """

    column: str
    ranked_indices: tuple[int, ...]
    ranked_scores: tuple[float, ...]
    top_n_indices: tuple[int, ...]
    top_n_scores: tuple[float, ...]
    total_anomalies: int


def rank_anomalies(
    column: str,
    top_n: int = 10,
) -> Callable[[Sequence[DataRecord]], AnomalyRankingResult]:
    """
    Rank records by anomaly score using Z-score.

    Higher Z-score = more anomalous. Returns top N anomalies.

    Args:
        column: Column name to analyze
        top_n: Number of top anomalies to return (default: 10)

    Returns:
        Function that produces AnomalyRankingResult when applied to data

    Example:
        >>> ranker = rank_anomalies("price", top_n=5)
        >>> result = ranker(records)
        >>> print(f"Top {len(result.top_n_indices)} anomalies: {result.top_n_scores}")
    """

    def validate(records: Sequence[DataRecord]) -> AnomalyRankingResult:
        if not records:
            return AnomalyRankingResult(
                column=column,
                ranked_indices=(),
                ranked_scores=(),
                top_n_indices=(),
                top_n_scores=(),
                total_anomalies=0,
            )

        # Extract numeric values
        values = []
        valid_indices = []
        for idx, record in enumerate(records):
            val = record.get(column)
            if isinstance(val, (int, float)):
                values.append(float(val))
                valid_indices.append(idx)

        if len(values) < 2:
            return AnomalyRankingResult(
                column=column,
                ranked_indices=(),
                ranked_scores=(),
                top_n_indices=(),
                top_n_scores=(),
                total_anomalies=0,
            )

        # Calculate Z-scores
        mean_val = statistics.mean(values)
        stdev = statistics.stdev(values)
        stdev = stdev if stdev != 0 else 1.0
        scores = [abs((v - mean_val) / stdev) for v in values]

        # Rank by score (descending)
        sorted_pairs = sorted(zip(valid_indices, scores), key=lambda x: x[1], reverse=True)
        ranked_indices = tuple(idx for idx, _ in sorted_pairs)
        ranked_scores = tuple(score for _, score in sorted_pairs)

        # Get top N
        top_indices = tuple(idx for idx, score in sorted_pairs[:top_n])
        top_scores = tuple(score for _, score in sorted_pairs[:top_n])

        # Count anomalies (Z-score > 3)
        anomaly_count = sum(1 for score in scores if score > 3.0)

        return AnomalyRankingResult(
            column=column,
            ranked_indices=ranked_indices,
            ranked_scores=ranked_scores,
            top_n_indices=top_indices,
            top_n_scores=top_scores,
            total_anomalies=anomaly_count,
        )

    return validate


# =============================================================================
# Historical Baseline Comparison
# =============================================================================


@dataclass(frozen=True)
class BaselineComparisonResult:
    """
    Result of comparing current data against historical baseline.

    Attributes:
        column: Column name analyzed
        baseline_stats: Statistics from baseline period
        current_stats: Statistics from current data
        drifted_indices: Indices of records that deviate from baseline
        drift_score: Overall drift magnitude (0-1, higher = more drift)
        baseline_window: Description of baseline window
    """

    column: str
    baseline_stats: dict[str, Any]
    current_stats: dict[str, Any]
    drifted_indices: tuple[int, ...]
    drift_score: float
    baseline_window: str | None = None


def detect_anomalies_against_baseline(
    column: str,
    baseline_records: Sequence[DataRecord],
    baseline_window: str | None = None,
    threshold_std: float = 3.0,
    threshold_iqr_multiplier: float = 1.5,
) -> Callable[[Sequence[DataRecord]], BaselineComparisonResult]:
    """
    Detect anomalies by comparing current data against historical baseline.

    Computes statistics from baseline period and flags current records that
    deviate beyond configured thresholds.

    Args:
        column: Column name to analyze
        baseline_records: Historical records to compute baseline statistics
        baseline_window: Description of baseline period (e.g., "last_30_days")
        threshold_std: Number of std devs from baseline mean to flag as anomaly
        threshold_iqr_multiplier: IQR multiplier for Tukey's fences from baseline

    Returns:
        Function that produces BaselineComparisonResult when applied to current data

    Example:
        >>> baseline = load_historical_data()
        >>> detector = detect_anomalies_against_baseline("revenue", baseline, "last_30_days")
        >>> result = detector(current_records)
        >>> print(f"Drift score: {result.drift_score:.2%}")
    """

    # Pre-compute baseline statistics
    baseline_values = []
    for record in baseline_records:
        val = record.get(column)
        if isinstance(val, (int, float)):
            baseline_values.append(float(val))

    if len(baseline_values) < 4:
        # Not enough data for baseline - return empty result
        def validate_no_baseline(records: Sequence[DataRecord]) -> BaselineComparisonResult:
            return BaselineComparisonResult(
                column=column,
                baseline_stats={"error": "Insufficient baseline data"},
                current_stats={},
                drifted_indices=(),
                drift_score=0.0,
                baseline_window=baseline_window,
            )

        return validate_no_baseline

    # Compute baseline statistics
    baseline_mean = statistics.mean(baseline_values)
    baseline_stdev = statistics.stdev(baseline_values) if len(baseline_values) > 1 else 1.0
    baseline_stdev = baseline_stdev if baseline_stdev != 0 else 1.0

    sorted_baseline = sorted(baseline_values)
    n_baseline = len(sorted_baseline)
    q1_pos = (n_baseline + 1) / 4.0 - 1
    q3_pos = (3 * (n_baseline + 1) / 4.0) - 1

    q1_floor = int(q1_pos)
    q3_floor = int(q3_pos)
    q1_ceil = q1_floor + 1
    q3_ceil = q3_floor + 1

    baseline_q1 = sorted_baseline[q1_floor] + (q1_pos - q1_floor) * (
        sorted_baseline[q1_ceil] - sorted_baseline[q1_floor]
    )
    baseline_q3 = sorted_baseline[q3_floor] + (q3_pos - q3_floor) * (
        sorted_baseline[q3_ceil] - sorted_baseline[q3_floor]
    )
    baseline_iqr = baseline_q3 - baseline_q1

    baseline_stats = {
        "mean": baseline_mean,
        "std_dev": baseline_stdev,
        "q1": baseline_q1,
        "q3": baseline_q3,
        "iqr": baseline_iqr,
        "count": len(baseline_values),
        "min": min(baseline_values),
        "max": max(baseline_values),
    }

    def validate(records: Sequence[DataRecord]) -> BaselineComparisonResult:
        if not records:
            return BaselineComparisonResult(
                column=column,
                baseline_stats=baseline_stats,
                current_stats={"count": 0},
                drifted_indices=(),
                drift_score=0.0,
                baseline_window=baseline_window,
            )

        # Extract current values
        current_values = []
        valid_indices = []
        for idx, record in enumerate(records):
            val = record.get(column)
            if isinstance(val, (int, float)):
                current_values.append(float(val))
                valid_indices.append(idx)

        if not current_values:
            return BaselineComparisonResult(
                column=column,
                baseline_stats=baseline_stats,
                current_stats={"error": "No valid current values"},
                drifted_indices=(),
                drift_score=0.0,
                baseline_window=baseline_window,
            )

        # Compute current statistics
        current_mean = statistics.mean(current_values)
        current_stdev = statistics.stdev(current_values) if len(current_values) > 1 else 0.0

        current_stats = {
            "mean": current_mean,
            "std_dev": current_stdev,
            "count": len(current_values),
            "min": min(current_values),
            "max": max(current_values),
        }

        # Detect anomalies using both std dev and IQR thresholds
        drifted_indices = []
        for idx, val in zip(valid_indices, current_values):
            # Check std dev threshold
            z_score = abs((val - baseline_mean) / baseline_stdev)
            std_anomaly = z_score > threshold_std

            # Check IQR threshold
            lower_bound = baseline_q1 - threshold_iqr_multiplier * baseline_iqr
            upper_bound = baseline_q3 + threshold_iqr_multiplier * baseline_iqr
            iqr_anomaly = val < lower_bound or val > upper_bound

            # Flag if either method detects anomaly
            if std_anomaly or iqr_anomaly:
                drifted_indices.append(idx)

        # Compute overall drift score (distance between means as proportion of std dev)
        drift_magnitude = abs(current_mean - baseline_mean) / baseline_stdev
        drift_score = min(drift_magnitude / 5.0, 1.0)  # Cap at 1.0

        return BaselineComparisonResult(
            column=column,
            baseline_stats=baseline_stats,
            current_stats=current_stats,
            drifted_indices=tuple(drifted_indices),
            drift_score=drift_score,
            baseline_window=baseline_window,
        )

    return validate


# =============================================================================
# Integration with @validate Decorator
# =============================================================================


def expect_column_no_anomalies_zscore(
    column: str,
    threshold: float = 3.0,
    max_anomalies: int = 0,
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """
    Expectation: No anomalies detected using Z-score method.

    Args:
        column: Column name to check for anomalies
        threshold: Z-score threshold (default: 3.0)
        max_anomalies: Maximum number of anomalies allowed (default: 0)

    Returns:
        Function that produces ValidationResult
    """

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        detector = detect_anomalies_zscore(column, threshold=threshold)
        result = detector(records)

        if result.outlier_count > max_anomalies:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column}': Found {result.outlier_count} anomalies "
                    f"(threshold: {threshold}, max allowed: {max_anomalies}) "
                    f"at indices: {result.outlier_indices[:10]}",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


def expect_column_no_anomalies_iqr(
    column: str,
    multiplier: float = 1.5,
    max_anomalies: int = 0,
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """
    Expectation: No anomalies detected using IQR method.

    Args:
        column: Column name to check for anomalies
        multiplier: IQR multiplier (default: 1.5 for Tukey's fences)
        max_anomalies: Maximum number of anomalies allowed (default: 0)

    Returns:
        Function that produces ValidationResult
    """

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        detector = detect_anomalies_iqr(column, multiplier=multiplier)
        result = detector(records)

        if result.outlier_count > max_anomalies:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column}': Found {result.outlier_count} anomalies "
                    f"(IQR multiplier: {multiplier}, max allowed: {max_anomalies}) "
                    f"at indices: {result.outlier_indices[:10]}",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


def expect_column_no_anomalies_isolation_forest(
    column: str,
    contamination: float = 0.1,
    max_anomalies: int = 0,
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """
    Expectation: No anomalies detected using Isolation Forest method.

    Args:
        column: Column name to check for anomalies
        contamination: Expected proportion of outliers (0-1, default: 0.1)
        max_anomalies: Maximum number of anomalies allowed (default: 0)

    Returns:
        Function that produces ValidationResult
    """

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        detector = detect_anomalies_isolation_forest(column, contamination=contamination)
        result = detector(records)

        if result.outlier_count > max_anomalies:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column}': Found {result.outlier_count} anomalies "
                    f"(contamination: {contamination}, max allowed: {max_anomalies}) "
                    f"at indices: {result.outlier_indices[:10]}",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


def expect_column_no_anomalies_one_class_svm(
    column: str,
    nu: float = 0.1,
    max_anomalies: int = 0,
) -> Callable[[Sequence[DataRecord]], ValidationResult]:
    """
    Expectation: No anomalies detected using One-Class SVM method.

    Args:
        column: Column name to check for anomalies
        nu: Upper bound on fraction of training errors (0-1, default: 0.1)
        max_anomalies: Maximum number of anomalies allowed (default: 0)

    Returns:
        Function that produces ValidationResult
    """

    def validate(records: Sequence[DataRecord]) -> ValidationResult:
        detector = detect_anomalies_one_class_svm(column, nu=nu)
        result = detector(records)

        if result.outlier_count > max_anomalies:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column}': Found {result.outlier_count} anomalies "
                    f"(nu: {nu}, max allowed: {max_anomalies}) "
                    f"at indices: {result.outlier_indices[:10]}",
                ),
            )

        return ValidationResult(is_valid=True)

    return validate


# =============================================================================
# Convenience Functions
# =============================================================================


def detect_anomalies_one_class_svm(
    column: str,
    nu: float = 0.1,
    kernel: str = "rbf",
    gamma: str = "scale",
) -> Callable[[Sequence[DataRecord]], AnomalyResult]:
    """
    Detect anomalies using One-Class SVM algorithm.

    One-Class SVM is an unsupervised algorithm that learns a decision
    function for novelty detection: classifying new data as similar or
    different to the training set.

    Args:
        column: Column name to check for anomalies
        nu: Upper bound on the fraction of training errors (0-1, default: 0.1)
        kernel: Specifies the kernel type to be used ('linear', 'poly', 'rbf', 'sigmoid')
        gamma: Kernel coefficient for 'rbf', 'poly' and 'sigmoid' ('scale', 'auto', or float)

    Returns:
        Function that produces AnomalyResult when applied to data

    Example:
        >>> detector = detect_anomalies_one_class_svm("amount", nu=0.05)
        >>> result = detector(records)
    """

    def validate(records: Sequence[DataRecord]) -> AnomalyResult:
        if not records:
            return AnomalyResult(
                method="one_class_svm",
                outlier_indices=(),
                outlier_count=0,
                anomaly_scores=(),
                statistics={"total_records": 0},
            )

        # Extract numeric values and record indices
        values = []
        valid_indices = []
        for idx, record in enumerate(records):
            val = record.get(column)
            if isinstance(val, (int, float)):
                values.append(float(val))
                valid_indices.append(idx)

        if len(values) < 2:
            return AnomalyResult(
                method="one_class_svm",
                outlier_indices=(),
                outlier_count=0,
                anomaly_scores=(),
                statistics={"error": "Need at least 2 numeric values"},
            )

        # Import scikit-learn only when needed (optional dependency)
        try:
            from sklearn.svm import OneClassSVM  # type: ignore[import-untyped]
        except ImportError as e:
            msg = (
                "scikit-learn is required for One-Class SVM. Install with: pip install scikit-learn"
            )
            return AnomalyResult(
                method="one_class_svm",
                outlier_indices=(),
                outlier_count=0,
                anomaly_scores=(),
                statistics={"error": str(e), "hint": msg},
            )

        # Convert to numpy array
        try:
            import numpy as np

            X = np.array(values).reshape(-1, 1)
        except ImportError:
            return AnomalyResult(
                method="one_class_svm",
                outlier_indices=(),
                outlier_count=0,
                anomaly_scores=(),
                statistics={"error": "numpy is required for One-Class SVM"},
            )

        # Fit One-Class SVM
        clf = OneClassSVM(nu=nu, kernel=kernel, gamma=gamma)
        clf.fit(X)

        # Predict anomalies (-1 = anomaly, 1 = normal)
        predictions = clf.predict(X)
        # Get decision function scores (lower = more anomalous)
        scores = clf.decision_function(X)

        # Convert scores to positive (higher = more anomalous)
        max_score = max(scores)
        normalized_scores = [max_score - s for s in scores]

        # Map back to original record indices
        outlier_indices = []
        for i, pred in enumerate(predictions):
            if pred == -1:  # Anomaly
                outlier_indices.append(valid_indices[i])

        return AnomalyResult(
            method="one_class_svm",
            outlier_indices=tuple(outlier_indices),
            outlier_count=len(outlier_indices),
            anomaly_scores=tuple(normalized_scores),
            statistics={
                "nu": nu,
                "kernel": kernel,
                "gamma": gamma,
                "min_score": min(normalized_scores),
                "max_score": max(normalized_scores),
                "median_score": statistics.median(normalized_scores),
            },
        )

    return validate


def detect_anomalies_multi_method(
    column: str,
    methods: list[str] = ["zscore", "iqr"],
    zscore_threshold: float = 3.0,
    iqr_multiplier: float = 1.5,
    isolation_forest_contamination: float = 0.1,
    one_class_svm_nu: float = 0.1,
) -> Callable[[Sequence[DataRecord]], dict[str, AnomalyResult]]:
    """
    Detect anomalies using multiple methods and compare results.

    Useful for understanding consistency across different detection algorithms.
    Results flagged by multiple methods are more likely to be true anomalies.

    Args:
        column: Column name to check for anomalies
        methods: List of methods to run ('zscore', 'iqr', 'isolation_forest', 'one_class_svm')
        zscore_threshold: Threshold for Z-score method
        iqr_multiplier: IQR multiplier for IQR method
        isolation_forest_contamination: Contamination for Isolation Forest
        one_class_svm_nu: Nu parameter for One-Class SVM

    Returns:
        Function that produces dict mapping method name to AnomalyResult

    Example:
        >>> detector = detect_anomalies_multi_method("revenue", methods=['zscore', 'iqr', 'isolation_forest'])
        >>> results = detector(records)
        >>> print(f"Z-score found {results['zscore'].outlier_count} outliers")
    """

    def validate(records: Sequence[DataRecord]) -> dict[str, AnomalyResult]:
        results = {}

        if "zscore" in methods:
            zscore_detector = detect_anomalies_zscore(column, threshold=zscore_threshold)
            results["zscore"] = zscore_detector(records)

        if "iqr" in methods:
            iqr_detector = detect_anomalies_iqr(column, multiplier=iqr_multiplier)
            results["iqr"] = iqr_detector(records)

        if "isolation_forest" in methods:
            iso_detector = detect_anomalies_isolation_forest(
                column, contamination=isolation_forest_contamination
            )
            results["isolation_forest"] = iso_detector(records)

        if "one_class_svm" in methods:
            svm_detector = detect_anomalies_one_class_svm(column, nu=one_class_svm_nu)
            results["one_class_svm"] = svm_detector(records)

        return results

    return validate


__all__ = [
    "AnomalyResult",
    "detect_anomalies_zscore",
    "detect_anomalies_iqr",
    "detect_anomalies_isolation_forest",
    "detect_anomalies_one_class_svm",
    "detect_anomalies_multi_method",
    "AnomalyRankingResult",
    "rank_anomalies",
    "BaselineComparisonResult",
    "detect_anomalies_against_baseline",
    "expect_column_no_anomalies_zscore",
    "expect_column_no_anomalies_iqr",
    "expect_column_no_anomalies_isolation_forest",
    "expect_column_no_anomalies_one_class_svm",
]
