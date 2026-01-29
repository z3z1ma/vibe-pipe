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

from vibe_piper.types import DataRecord

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
    max_samples: int | None = None,
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
            from sklearn.ensemble import IsolationForest
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
# Convenience Functions
# =============================================================================


def detect_anomalies_multi_method(
    column: str,
    methods: list[str] = ["zscore", "iqr"],
    zscore_threshold: float = 3.0,
    iqr_multiplier: float = 1.5,
    isolation_forest_contamination: float = 0.1,
) -> Callable[[Sequence[DataRecord]], dict[str, AnomalyResult]]:
    """
    Detect anomalies using multiple methods and compare results.

    Useful for understanding consistency across different detection algorithms.
    Results flagged by multiple methods are more likely to be true anomalies.

    Args:
        column: Column name to check for anomalies
        methods: List of methods to run ('zscore', 'iqr', 'isolation_forest')
        zscore_threshold: Threshold for Z-score method
        iqr_multiplier: IQR multiplier for IQR method
        isolation_forest_contamination: Contamination for Isolation Forest

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

        return results

    return validate


__all__ = [
    "AnomalyResult",
    "detect_anomalies_zscore",
    "detect_anomalies_iqr",
    "detect_anomalies_isolation_forest",
    "detect_anomalies_multi_method",
]
