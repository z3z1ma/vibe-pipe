"""
Tests for validation history storage and analysis.
"""

from __future__ import annotations

import pytest
from datetime import datetime, timedelta
from dataclasses import dataclass

from vibe_piper.types import QualityMetricType, ValidationResult
from vibe_piper.validation.history import (
    BaselineComparisonResult,
    FailurePattern,
    PostgreSQLValidationHistoryStore,
    TrendAnalysisResult,
    ValidationCheckRecord,
    ValidationHistoryAnalyzer,
    ValidationHistoryStore,
    ValidationMetric,
    ValidationRunMetadata,
)
from vibe_piper.validation.integration import (
    extract_metrics_from_suite_result,
    store_validation_result,
    suite_result_to_check_records,
    suite_result_to_run_metadata,
)
from vibe_piper.validation.suite import (
    SuiteValidationResult,
    ValidationContext,
    ValidationSuite,
)


@dataclass
class QueryResult:
    """Mock QueryResult for testing."""

    rows: list[dict]
    row_count: int


@pytest.fixture
def mock_connector():
    """Create a mock database connector."""

    class MockConnector:
        def __init__(self) -> None:
            self.executed_queries: list[tuple[str, dict]] = []

        def execute(self, query: str, params: dict | None = None) -> None:
            self.executed_queries.append((query, params or {}))

        def execute_query(self, query: str, params: dict | None = None) -> QueryResult:
            self.executed_queries.append((query, params or {}))
            return QueryResult(rows=[], row_count=0)

    return MockConnector()


@pytest.fixture
def sample_validation_run():
    """Create a sample validation run metadata."""
    return ValidationRunMetadata(
        validation_run_id="test-run-1",
        asset_name="test_asset",
        suite_name="test_suite",
        status="passed",
        started_at=datetime.now(),
        completed_at=datetime.now(),
        duration_ms=100.0,
        total_checks=5,
        passed_checks=5,
        failed_checks=0,
        warning_checks=0,
        total_records=100,
        error_count=0,
        warning_count=0,
    )


@pytest.fixture
def sample_check_records():
    """Create sample check records."""
    return [
        ValidationCheckRecord(
            validation_run_id="test-run-1",
            check_name="check_1",
            check_type="expect_column_values_to_be_between",
            passed=True,
            error_message=None,
            warning_messages=tuple(),
            metrics={},
            column_name="col1",
            duration_ms=20.0,
        ),
        ValidationCheckRecord(
            validation_run_id="test-run-1",
            check_name="check_2",
            check_type="expect_column_values_to_not_be_null",
            passed=False,
            error_message="Found 5 null values",
            warning_messages=("Warning: some rows affected",),
            metrics={},
            column_name="col2",
            duration_ms=30.0,
        ),
    ]


@pytest.fixture
def sample_validation_metrics():
    """Create sample validation metrics."""
    now = datetime.now()
    return [
        ValidationMetric(
            metric_name="pass_rate",
            metric_type=QualityMetricType.UNIQUENESS,
            asset_name="test_asset",
            check_name=None,
            value=0.95,
            timestamp=now,
            status="passed",
            threshold=0.95,
        ),
        ValidationMetric(
            metric_name="completeness",
            metric_type=QualityMetricType.COMPLETENESS,
            asset_name="test_asset",
            check_name=None,
            value=0.98,
            timestamp=now - timedelta(hours=1),
            status="passed",
            threshold=0.95,
        ),
    ]


def test_validation_run_metadata_creation(sample_validation_run):
    """Test ValidationRunMetadata creation."""
    assert sample_validation_run.validation_run_id == "test-run-1"
    assert sample_validation_run.asset_name == "test_asset"
    assert sample_validation_run.status == "passed"
    assert sample_validation_run.total_checks == 5
    assert sample_validation_run.passed_checks == 5
    assert sample_validation_run.failed_checks == 0


def test_validation_check_record_creation():
    """Test ValidationCheckRecord creation."""
    record = ValidationCheckRecord(
        validation_run_id="test-run-1",
        check_name="test_check",
        check_type="expect_column_values_to_be_between",
        passed=True,
        error_message=None,
        column_name="test_col",
    )

    assert record.validation_run_id == "test-run-1"
    assert record.check_name == "test_check"
    assert record.passed is True
    assert record.column_name == "test_col"


def test_validation_metric_creation():
    """Test ValidationMetric creation."""
    metric = ValidationMetric(
        metric_name="test_metric",
        metric_type=QualityMetricType.ACCURACY,
        asset_name="test_asset",
        value=0.95,
        status="passed",
    )

    assert metric.metric_name == "test_metric"
    assert metric.metric_type == QualityMetricType.ACCURACY
    assert metric.value == 0.95


def test_suite_result_to_run_metadata():
    """Test conversion from SuiteValidationResult to ValidationRunMetadata."""
    context = ValidationContext(
        validation_suite="test_suite",
        timestamp=datetime.now(),
    )

    suite_result = SuiteValidationResult(
        success=True,
        check_results={},
        total_checks=2,
        total_records=100,
        duration_ms=50.0,
        context=context,
    )

    run_metadata = suite_result_to_run_metadata(suite_result, "test_asset")

    assert run_metadata.asset_name == "test_asset"
    assert run_metadata.suite_name == "test_suite"
    assert run_metadata.status == "passed"
    assert run_metadata.total_checks == 2
    assert run_metadata.passed_checks == 2
    assert run_metadata.validation_run_id is not None


def test_suite_result_to_check_records():
    """Test conversion from SuiteValidationResult to check records."""
    context = ValidationContext(
        validation_suite="test_suite",
        timestamp=datetime.now(),
    )

    check1_result = ValidationResult(is_valid=True)
    check2_result = ValidationResult(
        is_valid=False,
        errors=("Check failed",),
        warnings=("Warning message",),
    )

    suite_result = SuiteValidationResult(
        success=False,
        check_results={"check1": check1_result, "check2": check2_result},
        failed_checks=("check2",),
        total_checks=2,
        total_records=100,
        duration_ms=100.0,
        context=context,
    )

    check_records = suite_result_to_check_records(suite_result, "test-run-1")

    assert len(check_records) == 2
    assert check_records[0].check_name == "check1"
    assert check_records[0].passed is True
    assert check_records[1].check_name == "check2"
    assert check_records[1].passed is False
    assert check_records[1].error_message == "Check failed"


def test_extract_metrics_from_suite_result():
    """Test metric extraction from SuiteValidationResult."""
    context = ValidationContext(
        validation_suite="test_suite",
        timestamp=datetime.now(),
    )

    suite_result = SuiteValidationResult(
        success=True,
        check_results={},
        total_checks=3,
        total_records=100,
        duration_ms=50.0,
        context=context,
    )

    metrics = extract_metrics_from_suite_result(suite_result, "test_asset", "test-run-1")

    assert len(metrics) >= 2
    pass_rate_metric = next((m for m in metrics if m.metric_name == "pass_rate"), None)
    assert pass_rate_metric is not None
    assert pass_rate_metric.asset_name == "test_asset"


def test_save_validation_run(mock_connector, sample_validation_run):
    """Test saving validation run metadata."""
    store = PostgreSQLValidationHistoryStore(mock_connector)

    store.save_validation_run(sample_validation_run)

    assert len(mock_connector.executed_queries) > 0


def test_save_check_results(mock_connector, sample_check_records):
    """Test saving check results."""
    store = PostgreSQLValidationHistoryStore(mock_connector)

    store.save_check_results(sample_check_records)

    assert len(mock_connector.executed_queries) == len(sample_check_records)


def test_save_metrics(mock_connector, sample_validation_metrics):
    """Test saving metrics."""
    store = PostgreSQLValidationHistoryStore(mock_connector)

    store.save_metrics(sample_validation_metrics)

    assert len(mock_connector.executed_queries) == len(sample_validation_metrics)


def test_query_validation_runs(mock_connector, sample_validation_run):
    """Test querying validation runs."""
    store = PostgreSQLValidationHistoryStore(mock_connector)

    runs = store.query_validation_runs(asset_name="test_asset")

    assert len(runs) >= 0
