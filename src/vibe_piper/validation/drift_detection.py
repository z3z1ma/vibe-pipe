"""
Drift detection module for comparing data distributions.

This module provides methods for detecting distribution drift between
historical baseline data and new data:
- KS Test: Kolmogorov-Smirnov test for continuous distributions
- Chi-Square Test: For categorical distribution differences
- PSI: Population Stability Index for monitoring feature drift

All methods provide statistical significance and actionable recommendations.
"""

from __future__ import annotations

import statistics
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from vibe_piper.types import DataRecord

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
    "DriftResult",
    "ColumnDriftResult",
    "detect_drift_ks",
    "detect_drift_chi_square",
    "detect_drift_psi",
    "detect_drift_multi_method",
]
