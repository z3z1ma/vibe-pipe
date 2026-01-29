"""
Comprehensive tests for anomaly detection functionality.

Tests include:
- Statistical methods (Z-score, IQR)
- ML-based methods (Isolation Forest, One-Class SVM)
- Anomaly ranking
- Historical baseline comparison
- Integration with @validate decorator
"""

import pytest

from vibe_piper.types import DataRecord, DataType, Schema, SchemaField
from vibe_piper.validation import (
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
    validate,
)

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def sample_schema():
    """Create a sample schema for testing."""
    return Schema(
        name="test_schema",
        fields=(
            SchemaField(name="id", data_type=DataType.INTEGER),
            SchemaField(name="value", data_type=DataType.FLOAT),
            SchemaField(name="name", data_type=DataType.STRING),
        ),
    )


@pytest.fixture
def normal_data(sample_schema):
    """Create normal data with no anomalies."""
    return tuple(
        DataRecord(
            data={"id": i, "value": 100.0 + i * 0.5, "name": f"item_{i}"}, schema=sample_schema
        )
        for i in range(50)
    )


@pytest.fixture
def data_with_anomalies(sample_schema):
    """Create data with intentional anomalies."""
    records = list(
        DataRecord(
            data={"id": i, "value": 100.0 + i * 0.1, "name": f"item_{i}"}, schema=sample_schema
        )
        for i in range(45)
    )
    # Add anomalies
    records.append(
        DataRecord(data={"id": 45, "value": 500.0, "name": "anomaly_high"}, schema=sample_schema)
    )
    records.append(
        DataRecord(data={"id": 46, "value": -200.0, "name": "anomaly_low"}, schema=sample_schema)
    )
    records.append(
        DataRecord(
            data={"id": 47, "value": 1000.0, "name": "anomaly_extreme"}, schema=sample_schema
        )
    )
    records.append(
        DataRecord(data={"id": 48, "value": 100.0, "name": "normal_again"}, schema=sample_schema)
    )
    records.append(
        DataRecord(data={"id": 49, "value": 102.0, "name": "normal_final"}, schema=sample_schema)
    )
    return tuple(records)


@pytest.fixture
def baseline_data(sample_schema):
    """Create historical baseline data."""
    return tuple(
        DataRecord(
            data={"id": i, "value": 100.0 + i * 0.5, "name": f"baseline_{i}"},
            schema=sample_schema,
        )
        for i in range(100)
    )


# =============================================================================
# Z-score Anomaly Detection Tests
# =============================================================================


class TestZScoreAnomalyDetection:
    """Test Z-score anomaly detection."""

    def test_zscore_detects_high_value_anomalies(self, data_with_anomalies):
        """Test Z-score detects high value anomalies."""
        detector = detect_anomalies_zscore("value", threshold=2.0)
        result = detector(data_with_anomalies)

        assert isinstance(result, AnomalyResult)
        assert result.method == "zscore"
        assert result.outlier_count > 0
        # High value anomalies should be detected
        assert any(idx in [45, 47] for idx in result.outlier_indices)
        assert len(result.anomaly_scores) == len(data_with_anomalies)

    def test_zscore_with_modified_zscore(self, data_with_anomalies):
        """Test modified Z-score using median and MAD."""
        detector = detect_anomalies_zscore("value", threshold=3.0, use_modified_zscore=True)
        result = detector(data_with_anomalies)

        assert result.method == "modified_zscore"
        assert result.outlier_count > 0

    def test_zscore_empty_data(self, sample_schema):
        """Test Z-score with empty data."""
        detector = detect_anomalies_zscore("value", threshold=3.0)
        empty_data = ()
        result = detector(empty_data)

        assert result.outlier_count == 0
        assert result.outlier_indices == ()
        assert result.statistics.get("total_records") == 0

    def test_zscore_insufficient_data(self, sample_schema):
        """Test Z-score with insufficient data (less than 2 values)."""
        record = DataRecord(data={"id": 1, "value": 100.0, "name": "test"}, schema=sample_schema)
        detector = detect_anomalies_zscore("value", threshold=3.0)
        result = detector((record,))

        assert result.outlier_count == 0
        assert "error" in result.statistics

    def test_zscore_threshold_configurable(self, normal_data):
        """Test Z-score threshold is configurable."""
        strict_detector = detect_anomalies_zscore("value", threshold=2.0)
        lenient_detector = detect_anomalies_zscore("value", threshold=10.0)

        strict_result = strict_detector(normal_data)
        lenient_result = lenient_detector(normal_data)

        # Strict threshold should catch more or same anomalies
        assert strict_result.outlier_count >= lenient_result.outlier_count


# =============================================================================
# IQR Anomaly Detection Tests
# =============================================================================


class TestIQRAnomalyDetection:
    """Test IQR anomaly detection."""

    def test_iqr_detects_outliers(self, data_with_anomalies):
        """Test IQR detects outliers beyond quartile bounds."""
        detector = detect_anomalies_iqr("value", multiplier=1.5)
        result = detector(data_with_anomalies)

        assert isinstance(result, AnomalyResult)
        assert result.method == "iqr"
        assert result.outlier_count > 0
        assert "q1" in result.statistics
        assert "q3" in result.statistics
        assert "iqr" in result.statistics
        assert "lower_bound" in result.statistics
        assert "upper_bound" in result.statistics

    def test_iqr_custom_multiplier(self, data_with_anomalies):
        """Test IQR with custom multiplier."""
        strict_detector = detect_anomalies_iqr("value", multiplier=1.5)
        lenient_detector = detect_anomalies_iqr("value", multiplier=3.0)

        strict_result = strict_detector(data_with_anomalies)
        lenient_result = lenient_detector(data_with_anomalies)

        # Strict multiplier should catch more anomalies
        assert strict_result.outlier_count >= lenient_result.outlier_count

    def test_iqr_custom_quartiles(self, data_with_anomalies):
        """Test IQR with custom quartile values."""
        custom_q1 = 95.0
        custom_q3 = 105.0
        detector = detect_anomalies_iqr(
            "value", multiplier=1.5, use_quartiles=(custom_q1, custom_q3)
        )
        result = detector(data_with_anomalies)

        assert result.statistics["q1"] == custom_q1
        assert result.statistics["q3"] == custom_q3

    def test_iqr_insufficient_data(self, sample_schema):
        """Test IQR with insufficient data (less than 4 values)."""
        records = tuple(
            DataRecord(
                data={"id": i, "value": 100.0 + i, "name": f"test_{i}"}, schema=sample_schema
            )
            for i in range(3)
        )
        detector = detect_anomalies_iqr("value", multiplier=1.5)
        result = detector(records)

        assert result.outlier_count == 0
        assert "error" in result.statistics


# =============================================================================
# Isolation Forest Anomaly Detection Tests
# =============================================================================


class TestIsolationForestAnomalyDetection:
    """Test Isolation Forest anomaly detection."""

    def test_isolation_forest_detects_anomalies(self, data_with_anomalies):
        """Test Isolation Forest detects anomalies."""
        detector = detect_anomalies_isolation_forest("value", contamination=0.1)
        result = detector(data_with_anomalies)

        assert isinstance(result, AnomalyResult)
        assert result.method == "isolation_forest"
        assert result.outlier_count > 0
        assert len(result.anomaly_scores) == len(data_with_anomalies)
        assert "contamination" in result.statistics

    def test_isolation_forest_contamination_configurable(self, data_with_anomalies):
        """Test Isolation Forest contamination is configurable."""
        low_contamination = detect_anomalies_isolation_forest("value", contamination=0.05)
        high_contamination = detect_anomalies_isolation_forest("value", contamination=0.2)

        low_result = low_contamination(data_with_anomalies)
        high_result = high_contamination(data_with_anomalies)

        # Higher contamination should flag more anomalies
        assert high_result.outlier_count >= low_result.outlier_count

    def test_isolation_forest_custom_estimators(self, data_with_anomalies):
        """Test Isolation Forest with custom n_estimators."""
        detector = detect_anomalies_isolation_forest("value", contamination=0.1, n_estimators=50)
        result = detector(data_with_anomalies)

        assert result.outlier_count > 0
        assert result.statistics["n_estimators"] == 50


# =============================================================================
# One-Class SVM Anomaly Detection Tests
# =============================================================================


class TestOneClassSVMAnomalyDetection:
    """Test One-Class SVM anomaly detection."""

    def test_one_class_svm_detects_anomalies(self, data_with_anomalies):
        """Test One-Class SVM detects anomalies."""
        detector = detect_anomalies_one_class_svm("value", nu=0.1)
        result = detector(data_with_anomalies)

        assert isinstance(result, AnomalyResult)
        assert result.method == "one_class_svm"
        assert result.outlier_count >= 0  # May or may not find anomalies
        assert len(result.anomaly_scores) == len(data_with_anomalies)
        assert "nu" in result.statistics
        assert "kernel" in result.statistics

    def test_one_class_svm_nu_configurable(self, data_with_anomalies):
        """Test One-Class SVM nu parameter is configurable."""
        low_nu = detect_anomalies_one_class_svm("value", nu=0.05)
        high_nu = detect_anomalies_one_class_svm("value", nu=0.2)

        low_result = low_nu(data_with_anomalies)
        high_result = high_nu(data_with_anomalies)

        # Higher nu parameter means more flexibility for anomalies
        # The relationship is not strictly monotonic, but both should be valid
        assert isinstance(low_result.outlier_count, int)
        assert isinstance(high_result.outlier_count, int)

    def test_one_class_svm_different_kernels(self, normal_data):
        """Test One-Class SVM with different kernels."""
        rbf_kernel = detect_anomalies_one_class_svm("value", nu=0.1, kernel="rbf")
        linear_kernel = detect_anomalies_one_class_svm("value", nu=0.1, kernel="linear")

        rbf_result = rbf_kernel(normal_data)
        linear_result = linear_kernel(normal_data)

        # Both should produce valid results
        assert isinstance(rbf_result, AnomalyResult)
        assert isinstance(linear_result, AnomalyResult)


# =============================================================================
# Multi-Method Anomaly Detection Tests
# =============================================================================


class TestMultiMethodAnomalyDetection:
    """Test running multiple anomaly detection methods."""

    def test_multi_method_returns_all_results(self, data_with_anomalies):
        """Test multi-method returns results for all specified methods."""
        detector = detect_anomalies_multi_method(
            "value", methods=["zscore", "iqr", "isolation_forest", "one_class_svm"]
        )
        results = detector(data_with_anomalies)

        assert isinstance(results, dict)
        assert "zscore" in results
        assert "iqr" in results
        assert "isolation_forest" in results
        assert "one_class_svm" in results

        # All results should be AnomalyResult
        for result in results.values():
            assert isinstance(result, AnomalyResult)

    def test_multi_method_selective_methods(self, data_with_anomalies):
        """Test multi-method with selective method list."""
        detector = detect_anomalies_multi_method("value", methods=["zscore", "iqr"])
        results = detector(data_with_anomalies)

        assert len(results) == 2
        assert "zscore" in results
        assert "iqr" in results
        assert "isolation_forest" not in results


# =============================================================================
# Anomaly Ranking Tests
# =============================================================================


class TestAnomalyRanking:
    """Test anomaly ranking functionality."""

    def test_rank_anomalies_returns_ranked_results(self, data_with_anomalies):
        """Test ranking returns sorted anomalies."""
        ranker = rank_anomalies("value", top_n=5)
        result = ranker(data_with_anomalies)

        assert isinstance(result, AnomalyRankingResult)
        assert result.column == "value"
        assert len(result.ranked_indices) == len(data_with_anomalies)
        assert len(result.ranked_scores) == len(data_with_anomalies)
        assert len(result.top_n_indices) == 5
        assert len(result.top_n_scores) == 5

        # Scores should be sorted descending (highest first)
        assert all(
            result.ranked_scores[i] >= result.ranked_scores[i + 1]
            for i in range(len(result.ranked_scores) - 1)
        )

    def test_rank_anomalies_top_n_configurable(self, data_with_anomalies):
        """Test top_n parameter is configurable."""
        ranker_top3 = rank_anomalies("value", top_n=3)
        ranker_top10 = rank_anomalies("value", top_n=10)

        result_top3 = ranker_top3(data_with_anomalies)
        result_top10 = ranker_top10(data_with_anomalies)

        assert len(result_top3.top_n_indices) == 3
        assert len(result_top10.top_n_indices) == 10

    def test_rank_anomalies_high_scores_for_anomalies(self, data_with_anomalies):
        """Test anomalies have high scores."""
        ranker = rank_anomalies("value", top_n=5)
        result = ranker(data_with_anomalies)

        # Known anomaly indices should be in top ranks
        assert 45 in result.top_n_indices or 47 in result.top_n_indices


# =============================================================================
# Historical Baseline Comparison Tests
# =============================================================================


class TestBaselineComparison:
    """Test historical baseline comparison."""

    def test_baseline_comparison_detects_drift(self, baseline_data):
        """Test baseline comparison detects drift."""
        # Create current data with different mean
        drifted_records = tuple(
            DataRecord(
                data={"id": i, "value": 200.0 + i * 0.5, "name": f"current_{i}"},
                schema=baseline_data[0].schema,
            )
            for i in range(20)
        )

        detector = detect_anomalies_against_baseline("value", baseline_data, "last_100_records")
        result = detector(drifted_records)

        assert isinstance(result, BaselineComparisonResult)
        assert result.column == "value"
        assert result.drift_score > 0.5  # Significant drift
        assert len(result.drifted_indices) > 0
        assert "mean" in result.baseline_stats
        assert "mean" in result.current_stats

    def test_baseline_comparison_no_drift(self, baseline_data):
        """Test baseline comparison with no drift."""
        # Create current data similar to baseline
        similar_records = tuple(
            DataRecord(
                data={"id": i, "value": 100.0 + i * 0.5, "name": f"current_{i}"},
                schema=baseline_data[0].schema,
            )
            for i in range(20)
        )

        detector = detect_anomalies_against_baseline("value", baseline_data, "last_100_records")
        result = detector(similar_records)

        assert result.drift_score < 0.5  # Low drift

    def test_baseline_comparison_custom_thresholds(self, baseline_data):
        """Test baseline comparison with custom thresholds."""
        current_records = tuple(
            DataRecord(
                data={"id": i, "value": 150.0 + i * 0.5, "name": f"current_{i}"},
                schema=baseline_data[0].schema,
            )
            for i in range(20)
        )

        strict_detector = detect_anomalies_against_baseline(
            "value", baseline_data, threshold_std=2.0, threshold_iqr_multiplier=1.0
        )
        lenient_detector = detect_anomalies_against_baseline(
            "value", baseline_data, threshold_std=5.0, threshold_iqr_multiplier=3.0
        )

        strict_result = strict_detector(current_records)
        lenient_result = lenient_detector(current_records)

        # Strict should flag more anomalies
        assert len(strict_result.drifted_indices) >= len(lenient_result.drifted_indices)


# =============================================================================
# Integration with @validate Decorator Tests
# =============================================================================


class TestAnomalyValidationIntegration:
    """Test integration of anomaly detection with @validate decorator."""

    def test_expect_no_anomalies_zscore_pass(self, normal_data):
        """Test Z-score expectation passes with normal data."""
        check = expect_column_no_anomalies_zscore("value", threshold=3.0, max_anomalies=0)
        result = check(normal_data)

        assert result.is_valid is True
        assert not result.errors

    def test_expect_no_anomalies_zscore_fail(self, data_with_anomalies):
        """Test Z-score expectation fails with anomalies."""
        check = expect_column_no_anomalies_zscore("value", threshold=2.0, max_anomalies=0)
        result = check(data_with_anomalies)

        assert result.is_valid is False
        assert len(result.errors) > 0
        assert "anomalies" in result.errors[0].lower()

    def test_expect_no_anomalies_iqr_pass(self, normal_data):
        """Test IQR expectation passes with normal data."""
        check = expect_column_no_anomalies_iqr("value", multiplier=1.5, max_anomalies=0)
        result = check(normal_data)

        assert result.is_valid is True
        assert not result.errors

    def test_expect_no_anomalies_iqr_fail(self, data_with_anomalies):
        """Test IQR expectation fails with anomalies."""
        check = expect_column_no_anomalies_iqr("value", multiplier=1.5, max_anomalies=0)
        result = check(data_with_anomalies)

        assert result.is_valid is False
        assert len(result.errors) > 0

    def test_expect_no_anomalies_isolation_forest_pass(self, normal_data):
        """Test Isolation Forest expectation passes with normal data."""
        check = expect_column_no_anomalies_isolation_forest(
            "value", contamination=0.05, max_anomalies=5
        )
        result = check(normal_data)

        assert result.is_valid is True
        assert not result.errors

    def test_expect_no_anomalies_one_class_svm(self, data_with_anomalies):
        """Test One-Class SVM expectation."""
        check = expect_column_no_anomalies_one_class_svm("value", nu=0.1, max_anomalies=5)
        result = check(data_with_anomalies)

        # Should handle gracefully even with anomalies
        assert isinstance(result.is_valid, bool)

    def test_validate_decorator_with_anomaly_checks(self, sample_schema):
        """Test @validate decorator with anomaly checks."""

        @validate(
            checks=[
                expect_column_no_anomalies_zscore("value", threshold=3.0, max_anomalies=2),
                expect_column_no_anomalies_iqr("value", multiplier=1.5, max_anomalies=2),
            ]
        )
        def generate_data():
            # Normal data
            return tuple(
                DataRecord(
                    data={"id": i, "value": 100.0 + i * 0.1, "name": f"item_{i}"},
                    schema=sample_schema,
                )
                for i in range(20)
            )

        # Should not raise with normal data
        result = generate_data()
        assert len(result) == 20


# =============================================================================
# Anomaly Result Methods Tests
# =============================================================================


class TestAnomalyResultMethods:
    """Test AnomalyResult utility methods."""

    def test_get_anomaly_rate(self, data_with_anomalies):
        """Test get_anomaly_rate calculates correct rate."""
        detector = detect_anomalies_zscore("value", threshold=2.0)
        result = detector(data_with_anomalies)

        rate = result.get_anomaly_rate()
        expected_rate = result.outlier_count / len(data_with_anomalies)

        assert rate == expected_rate
        assert 0.0 <= rate <= 1.0


# =============================================================================
# Edge Cases and Error Handling Tests
# =============================================================================


class TestAnomalyDetectionEdgeCases:
    """Test edge cases and error handling."""

    def test_non_numeric_column(self, sample_schema):
        """Test handling of non-numeric column."""
        records = tuple(
            DataRecord(data={"id": i, "name": f"item_{i}", "value": 100.0}, schema=sample_schema)
            for i in range(10)
        )

        detector = detect_anomalies_zscore("name", threshold=3.0)
        result = detector(records)

        # Should handle gracefully
        assert isinstance(result, AnomalyResult)

    def test_all_null_values(self):
        """Test handling of all null values in column."""
        # Create schema with nullable value field
        nullable_schema = Schema(
            name="test_schema_nullable",
            fields=(
                SchemaField(name="id", data_type=DataType.INTEGER),
                SchemaField(name="value", data_type=DataType.FLOAT, nullable=True),
                SchemaField(name="name", data_type=DataType.STRING),
            ),
        )

        records = tuple(
            DataRecord(data={"id": i, "value": None, "name": f"item_{i}"}, schema=nullable_schema)
            for i in range(10)
        )

        detector = detect_anomalies_zscore("value", threshold=3.0)
        result = detector(records)

        # Should handle gracefully - no numeric values means no anomalies detected
        assert isinstance(result, AnomalyResult)
        assert result.outlier_count == 0

    def test_single_value_repeated(self, sample_schema):
        """Test handling of single repeated value."""
        records = tuple(
            DataRecord(data={"id": i, "value": 100.0, "name": f"item_{i}"}, schema=sample_schema)
            for i in range(10)
        )

        detector = detect_anomalies_iqr("value", multiplier=1.5)
        result = detector(records)

        # Should handle zero IQR gracefully
        assert isinstance(result, AnomalyResult)


# =============================================================================
# Performance Tests
# =============================================================================


class TestAnomalyDetectionPerformance:
    """Test performance with larger datasets."""

    def test_large_dataset_performance(self, sample_schema):
        """Test performance with larger dataset."""
        # Generate 1000 records
        records = tuple(
            DataRecord(
                data={"id": i, "value": 100.0 + i * 0.1, "name": f"item_{i}"}, schema=sample_schema
            )
            for i in range(1000)
        )

        # Test all methods complete in reasonable time
        zscore_result = detect_anomalies_zscore("value", threshold=3.0)(records)
        iqr_result = detect_anomalies_iqr("value", multiplier=1.5)(records)
        multi_result = detect_anomalies_multi_method("value", methods=["zscore", "iqr"])(records)

        assert isinstance(zscore_result, AnomalyResult)
        assert isinstance(iqr_result, AnomalyResult)
        assert isinstance(multi_result, dict)
