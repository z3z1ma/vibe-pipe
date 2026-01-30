"""
Validation framework for Vibe Piper.

This module provides comprehensive data validation capabilities including:
- 30+ built-in validation types
- @validate decorator for assets
- @expect fluent API for expectations
- Validation suites for organizing checks
- Detailed validation results
- Lazy validation mode (collect all errors)
- Advanced validation: anomaly detection, data profiling, drift detection, quality scoring
- Validation history: PostgreSQL-based storage, trend analysis, failure pattern detection, baseline comparison
"""

# Import checks using importlib to avoid circular issues
import importlib

checks = importlib.import_module(".checks", package="vibe_piper.validation")

# TODO: Investigate why expect_column_values_to_not_in_set cannot be imported
# from checks module when accessed through package
# For now, we'll skip this one function
_expect_not_in_set = None

# Re-export all check functions
ColumnValidationResult = checks.ColumnValidationResult
create_custom_validation = checks.create_custom_validation
expect_column_groupby_mean_to_be_between = checks.expect_column_groupby_mean_to_be_between
expect_column_groupby_value_counts_to_be_between = (
    checks.expect_column_groupby_value_counts_to_be_between
)
expect_column_max_to_be_between = checks.expect_column_max_to_be_between
expect_column_mean_to_be_between = checks.expect_column_mean_to_be_between
expect_column_pair_values_a_to_be_greater_than_b = (
    checks.expect_column_pair_values_a_to_be_greater_than_b
)
expect_column_pair_values_to_be_equal = checks.expect_column_pair_values_to_be_equal
expect_column_pair_values_to_be_not_equal = checks.expect_column_pair_values_to_be_not_equal
expect_column_proportion_of_nulls_to_be_between = (
    checks.expect_column_proportion_of_nulls_to_be_between
)
expect_column_std_dev_to_be_between = checks.expect_column_std_dev_to_be_between
expect_column_sum_to_equal_other_column_sum = checks.expect_column_sum_to_equal_other_column_sum
expect_column_value_lengths_to_be_between = checks.expect_column_value_lengths_to_be_between
expect_column_values_to_be_between = checks.expect_column_values_to_be_between
expect_column_values_to_be_dateutil_parseable = checks.expect_column_values_to_be_dateutil_parseable
expect_column_values_to_be_decreasing = checks.expect_column_values_to_be_decreasing
expect_column_values_to_be_in_set = checks.expect_column_values_to_be_in_set
expect_column_values_to_not_be_in_set = _expect_not_in_set  # Workaround: use direct import
expect_column_values_to_be_increasing = checks.expect_column_values_to_be_increasing
expect_column_values_to_not_be_null = checks.expect_column_values_to_not_be_null
expect_column_values_to_not_in_set = _expect_not_in_set  # Workaround: use direct import
expect_column_values_to_be_of_type = checks.expect_column_values_to_be_of_type
expect_column_values_to_be_unique = checks.expect_column_values_to_be_unique
expect_column_values_to_match_regex = checks.expect_column_values_to_match_regex
expect_column_values_to_not_match_regex = checks.expect_column_values_to_not_match_regex
expect_table_row_count_to_be_between = checks.expect_table_row_count_to_be_between
expect_table_row_count_to_equal = checks.expect_table_row_count_to_equal

# Import decorators and suite
# Import advanced validation modules
from vibe_piper.validation.anomaly_detection import (
    AnomalyRankingResult,
    AnomalyResult,
    BaselineComparisonResult,
    detect_anomalies_against_baseline,
    detect_anomalies_iqr,
    detect_anomalies_isolation_forest,
    detect_anomalies_multi_method,
    detect_anomalies_one_class_svm,
    detect_anomalies_zscore,
    expect_column_no_anomalies_iqr,
    expect_column_no_anomalies_isolation_forest,
    expect_column_no_anomalies_one_class_svm,
    expect_column_no_anomalies_zscore,
    rank_anomalies,
)
from vibe_piper.validation.data_profiling import (
    ColumnStatistics,
    DataProfile,
    profile_column,
    profile_data,
)
from vibe_piper.validation.decorators import (
    ColumnExpectationBuilder,
    ExpectationBuilder,
    MultiColumnExpectationBuilder,
    TableExpectationBuilder,
    ValidateDecorator,
    ValidationConfig,
    expect,
    validate,
)
from vibe_piper.validation.drift_detection import (
    BaselineMetadata,
    BaselineStore,
    ColumnDriftResult,
    DriftHistory,
    DriftHistoryEntry,
    DriftResult,
    DriftThresholds,
    check_drift_alert,
    check_drift_ks,
    check_drift_psi,
    detect_drift_chi_square,
    detect_drift_ks,
    detect_drift_multi_method,
    detect_drift_psi,
)

# Validation history storage and analysis
from vibe_piper.validation.history import (
    FailurePattern,
    PostgreSQLValidationHistoryStore,
    TrendAnalysisResult,
    ValidationCheckRecord,
    ValidationHistoryAnalyzer,
    ValidationHistoryStore,
    ValidationMetric,
    ValidationRunMetadata,
)

# Integration utilities
from vibe_piper.validation.integration import (
    extract_metrics_from_suite_result,
    store_validation_result,
    suite_result_to_check_records,
    suite_result_to_run_metadata,
)
from vibe_piper.validation.quality_scoring import (
    ColumnQualityResult,
    QualityAlert,
    QualityDashboard,
    QualityHistory,
    QualityRecommendation,
    QualityScore,
    QualityThresholdConfig,
    QualityTrend,
    calculate_column_quality,
    calculate_completeness,
    calculate_consistency,
    calculate_quality_score,
    calculate_uniqueness,
    calculate_validity,
    create_quality_dashboard,
    generate_quality_alerts,
    generate_quality_recommendations,
    track_quality_history,
)
from vibe_piper.validation.suite import (
    LazyValidationStrategy,
    SuiteValidationResult,
    ValidationContext,
    ValidationStrategy,
    ValidationSuite,
    create_validation_suite,
)

__all__ = [
    # Decorators and builders
    "validate",
    "expect",
    "ValidationConfig",
    # Suite and strategy
    "ValidationSuite",
    "ValidationStrategy",
    "LazyValidationStrategy",
    "ValidationContext",
    "SuiteValidationResult",
    "create_validation_suite",
    # Check functions (30+ validations)
    "expect_column_mean_to_be_between",
    "expect_column_std_dev_to_be_between",
    "expect_column_min_to_be_between",
    "expect_column_max_to_be_between",
    "expect_column_median_to_be_between",
    "expect_column_values_to_match_regex",
    "expect_column_values_to_not_match_regex",
    "expect_column_values_to_be_between",
    "expect_column_values_to_be_in_set",
    "expect_column_values_to_not_be_in_set",
    "expect_column_values_to_be_unique",
    "expect_column_values_to_be_of_type",
    "expect_column_values_to_not_be_null",
    "expect_column_value_lengths_to_be_between",
    "expect_column_values_to_be_increasing",
    "expect_column_values_to_be_decreasing",
    "expect_column_pair_values_to_be_equal",
    "expect_column_pair_values_to_be_not_equal",
    "expect_column_pair_values_a_to_be_greater_than_b",
    "expect_column_sum_to_equal_other_column_sum",
    "expect_column_groupby_value_counts_to_be_between",
    "expect_column_groupby_mean_to_be_between",
    "expect_column_proportion_of_nulls_to_be_between",
    "expect_table_row_count_to_be_between",
    "expect_table_row_count_to_equal",
    "expect_column_values_to_be_dateutil_parseable",
    "create_custom_validation",
    # Result types
    "ColumnValidationResult",
    # Builder classes
    "ExpectationBuilder",
    "ColumnExpectationBuilder",
    "MultiColumnExpectationBuilder",
    "TableExpectationBuilder",
    "ValidateDecorator",
    # Advanced validation: anomaly detection
    "AnomalyResult",
    "AnomalyRankingResult",
    "BaselineComparisonResult",
    "detect_anomalies_zscore",
    "detect_anomalies_iqr",
    "detect_anomalies_isolation_forest",
    "detect_anomalies_one_class_svm",
    "detect_anomalies_multi_method",
    "detect_anomalies_against_baseline",
    "rank_anomalies",
    "expect_column_no_anomalies_zscore",
    "expect_column_no_anomalies_iqr",
    "expect_column_no_anomalies_isolation_forest",
    "expect_column_no_anomalies_one_class_svm",
    # Advanced validation: data profiling
    "ColumnStatistics",
    "DataProfile",
    "profile_data",
    "profile_column",
    # Advanced validation: drift detection
    "ColumnDriftResult",
    "DriftResult",
    "DriftThresholds",
    "BaselineStore",
    "DriftHistory",
    "BaselineMetadata",
    "DriftHistoryEntry",
    "detect_drift_ks",
    "detect_drift_chi_square",
    "detect_drift_psi",
    "detect_drift_multi_method",
    "check_drift_alert",
    "check_drift_ks",
    "check_drift_psi",
    # Advanced validation: quality scoring
    "QualityScore",
    "ColumnQualityResult",
    "QualityThresholdConfig",
    "QualityAlert",
    "QualityRecommendation",
    "QualityTrend",
    "QualityHistory",
    "QualityDashboard",
    "calculate_completeness",
    "calculate_validity",
    "calculate_uniqueness",
    "calculate_consistency",
    "calculate_quality_score",
    "calculate_column_quality",
    "track_quality_history",
    "generate_quality_alerts",
    "generate_quality_recommendations",
    "create_quality_dashboard",
    # Validation history storage and analysis
    "ValidationRunMetadata",
    "ValidationCheckRecord",
    "ValidationMetric",
    "TrendAnalysisResult",
    "FailurePattern",
    "ValidationHistoryStore",
    "PostgreSQLValidationHistoryStore",
    "ValidationHistoryAnalyzer",
    # Integration utilities
    "suite_result_to_run_metadata",
    "suite_result_to_check_records",
    "extract_metrics_from_suite_result",
    "store_validation_result",
]

__version__ = "2.1.0"
