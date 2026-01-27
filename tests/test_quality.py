"""
Tests for data quality metrics collection.
"""

from datetime import datetime, timedelta

import pytest

from vibe_piper import (
    DataQualityReport,
    DataRecord,
    DataType,
    QualityCheckResult,
    QualityMetric,
    QualityMetricType,
    Schema,
    SchemaField,
    check_completeness,
    check_freshness,
    check_uniqueness,
    check_validity,
    generate_quality_report,
)


class TestQualityMetric:
    """Tests for QualityMetric class."""

    def test_create_quality_metric(self) -> None:
        """Test creating a quality metric."""
        metric = QualityMetric(
            name="test_metric",
            metric_type=QualityMetricType.COMPLETENESS,
            value=0.95,
            threshold=0.9,
            passed=True,
            description="Test metric",
        )

        assert metric.name == "test_metric"
        assert metric.metric_type == QualityMetricType.COMPLETENESS
        assert metric.value == 0.95
        assert metric.threshold == 0.9
        assert metric.passed is True

    def test_quality_metric_without_threshold(self) -> None:
        """Test quality metric without threshold."""
        metric = QualityMetric(
            name="test_metric",
            metric_type=QualityMetricType.COMPLETENESS,
            value=100,
        )

        assert metric.passed is None


class TestQualityCheckResult:
    """Tests for QualityCheckResult class."""

    def test_create_passed_check_result(self) -> None:
        """Test creating a passed check result."""
        metrics = (
            QualityMetric(
                name="score",
                metric_type=QualityMetricType.COMPLETENESS,
                value=0.95,
            ),
        )

        result = QualityCheckResult(
            check_name="completeness",
            passed=True,
            metrics=metrics,
            errors=(),
            warnings=(),
        )

        assert result.check_name == "completeness"
        assert result.passed is True
        assert len(result.metrics) == 1
        assert result.errors == ()


class TestCheckCompleteness:
    """Tests for check_completeness function."""

    def test_complete_data(self) -> None:
        """Test completeness check with complete data."""
        schema = Schema(
            name="test",
            fields=(
                SchemaField(name="id", data_type=DataType.INTEGER),  # type: ignore
                SchemaField(name="name", data_type=DataType.STRING),  # type: ignore
            ),
        )

        records = [
            DataRecord(data={"id": 1, "name": "Alice"}, schema=schema),
            DataRecord(data={"id": 2, "name": "Bob"}, schema=schema),
        ]

        result = check_completeness(records)

        assert result.passed is True
        assert result.check_name == "completeness"

        # Find completeness_score metric
        score_metric = next(m for m in result.metrics if m.name == "completeness_score")
        assert score_metric.value == 1.0

    def test_missing_values(self) -> None:
        """Test completeness check with missing values."""
        schema = Schema(
            name="test",
            fields=(
                SchemaField(
                    name="id", data_type=DataType.INTEGER, nullable=True
                ),  # type: ignore
                SchemaField(
                    name="name", data_type=DataType.STRING, nullable=True
                ),  # type: ignore
            ),
        )

        records = [
            DataRecord(data={"id": 1, "name": "Alice"}, schema=schema),
            DataRecord(data={"id": 2, "name": None}, schema=schema),  # Missing name
            DataRecord(data={"id": None, "name": "Charlie"}, schema=schema),  # Missing id
        ]

        result = check_completeness(records)

        # 6 total fields, 4 non-null = 66.67% complete
        score_metric = next(m for m in result.metrics if m.name == "completeness_score")
        assert score_metric.value < 1.0

        missing_metric = next(m for m in result.metrics if m.name == "missing_count")
        assert missing_metric.value == 2

    def test_empty_records(self) -> None:
        """Test completeness check with empty records."""
        result = check_completeness([])

        assert result.passed is True
        assert len(result.metrics) == 0


class TestCheckValidity:
    """Tests for check_validity function."""

    def test_valid_records(self) -> None:
        """Test validity check with valid records."""
        schema = Schema(
            name="test",
            fields=(
                SchemaField(name="id", data_type=DataType.INTEGER),  # type: ignore
                SchemaField(name="name", data_type=DataType.STRING),  # type: ignore
            ),
        )

        records = [
            DataRecord(data={"id": 1, "name": "Alice"}, schema=schema),
            DataRecord(data={"id": 2, "name": "Bob"}, schema=schema),
        ]

        result = check_validity(records)

        assert result.passed is True

        valid_metric = next(m for m in result.metrics if m.name == "validity_score")
        assert valid_metric.value == 1.0

    def test_invalid_records(self) -> None:
        """Test validity check with invalid records."""
        schema = Schema(
            name="test",
            fields=(
                SchemaField(name="id", data_type=DataType.INTEGER),  # type: ignore
                SchemaField(
                    name="name",
                    data_type=DataType.STRING,  # type: ignore
                    required=True,
                    nullable=True,
                ),
            ),
        )

        # This will raise ValueError when creating DataRecord
        # So we test with records that passed validation
        records = [
            DataRecord(data={"id": 1, "name": "Alice"}, schema=schema),
            DataRecord(data={"id": 2, "name": "Bob"}, schema=schema),
        ]

        result = check_validity(records)

        assert result.passed is True
        valid_metric = next(m for m in result.metrics if m.name == "valid_count")
        assert valid_metric.value == 2


class TestCheckUniqueness:
    """Tests for check_uniqueness function."""

    def test_unique_records(self) -> None:
        """Test uniqueness check with unique records."""
        schema = Schema(
            name="test",
            fields=(
                SchemaField(name="id", data_type=DataType.INTEGER),  # type: ignore
                SchemaField(name="email", data_type=DataType.STRING),  # type: ignore
            ),
        )

        records = [
            DataRecord(data={"id": 1, "email": "alice@example.com"}, schema=schema),
            DataRecord(data={"id": 2, "email": "bob@example.com"}, schema=schema),
        ]

        result = check_uniqueness(records, unique_fields=("email",))

        assert result.passed is True

        unique_metric = next(m for m in result.metrics if m.name == "uniqueness_score")
        assert unique_metric.value == 1.0

    def test_duplicate_records(self) -> None:
        """Test uniqueness check with duplicate records."""
        schema = Schema(
            name="test",
            fields=(
                SchemaField(name="id", data_type=DataType.INTEGER),  # type: ignore
                SchemaField(name="email", data_type=DataType.STRING),  # type: ignore
            ),
        )

        records = [
            DataRecord(data={"id": 1, "email": "alice@example.com"}, schema=schema),
            DataRecord(data={"id": 2, "email": "alice@example.com"}, schema=schema),
            DataRecord(data={"id": 3, "email": "bob@example.com"}, schema=schema),
        ]

        result = check_uniqueness(records, unique_fields=("email",))

        assert result.passed is False

        duplicate_metric = next(
            m for m in result.metrics if m.name == "duplicate_count"
        )
        assert duplicate_metric.value == 1

    def test_no_unique_fields_specified(self) -> None:
        """Test uniqueness check without specifying fields."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="id", data_type=DataType.INTEGER),),  # type: ignore
        )

        records = [
            DataRecord(data={"id": 1}, schema=schema),
            DataRecord(data={"id": 2}, schema=schema),
        ]

        result = check_uniqueness(records)

        # Should pass and be informational
        assert result.passed is True
        # Check that uniqueness_score is 1.0 (informational)
        unique_metric = next((m for m in result.metrics if m.name == "uniqueness_score"), None)
        assert unique_metric is not None
        assert unique_metric.value == 1.0


class TestCheckFreshness:
    """Tests for check_freshness function."""

    def test_fresh_data(self) -> None:
        """Test freshness check with fresh data."""
        schema = Schema(
            name="test",
            fields=(
                SchemaField(
                    name="timestamp",
                    data_type=DataType.DATETIME,  # type: ignore
                ),
            ),
        )

        now = datetime.now()
        records = [
            DataRecord(data={"timestamp": now}, schema=schema),
            DataRecord(data={"timestamp": now - timedelta(hours=1)}, schema=schema),
        ]

        result = check_freshness(records, timestamp_field="timestamp", max_age_hours=24)

        assert result.passed is True

    def test_stale_data(self) -> None:
        """Test freshness check with stale data."""
        schema = Schema(
            name="test",
            fields=(
                SchemaField(
                    name="timestamp",
                    data_type=DataType.DATETIME,  # type: ignore
                ),
            ),
        )

        now = datetime.now()
        records = [
            DataRecord(
                data={"timestamp": now - timedelta(hours=48)}, schema=schema  # 2 days old
            ),
        ]

        result = check_freshness(records, timestamp_field="timestamp", max_age_hours=24)

        assert result.passed is False

        stale_metric = next(m for m in result.metrics if m.name == "stale_count")
        assert stale_metric.value == 1

    def test_missing_timestamps(self) -> None:
        """Test freshness check with missing timestamps."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="id", data_type=DataType.INTEGER),),  # type: ignore
        )

        records = [
            DataRecord(data={"id": 1}, schema=schema),
        ]

        result = check_freshness(records, timestamp_field="timestamp", max_age_hours=24)

        # Should have warnings about missing timestamps
        assert len(result.warnings) > 0
        # Should fail since no valid timestamps were found
        assert result.passed is False


class TestGenerateQualityReport:
    """Tests for generate_quality_report function."""

    def test_generate_full_report(self) -> None:
        """Test generating a comprehensive quality report."""
        schema = Schema(
            name="test",
            fields=(
                SchemaField(
                    name="id", data_type=DataType.INTEGER, nullable=True
                ),  # type: ignore
                SchemaField(
                    name="name", data_type=DataType.STRING, nullable=True
                ),  # type: ignore
            ),
        )

        records = [
            DataRecord(data={"id": 1, "name": "Alice"}, schema=schema),
            DataRecord(data={"id": 2, "name": "Bob"}, schema=schema),
            DataRecord(data={"id": 3, "name": None}, schema=schema),
        ]

        report = generate_quality_report(
            records=records,
            schema=schema,
            checks=("completeness", "validity"),
        )

        assert isinstance(report, DataQualityReport)
        assert report.total_records == 3
        assert report.completeness_score < 1.0  # One null name
        assert len(report.checks) == 2

    def test_report_with_uniqueness_check(self) -> None:
        """Test report including uniqueness check."""
        schema = Schema(
            name="test",
            fields=(
                SchemaField(name="id", data_type=DataType.INTEGER),  # type: ignore
                SchemaField(name="email", data_type=DataType.STRING),  # type: ignore
            ),
        )

        records = [
            DataRecord(data={"id": 1, "email": "alice@example.com"}, schema=schema),
            DataRecord(data={"id": 2, "email": "bob@example.com"}, schema=schema),
        ]

        report = generate_quality_report(
            records=records,
            checks=("completeness", "validity", "uniqueness"),
            unique_fields=("email",),
        )

        assert report.total_records == 2
        assert len(report.checks) == 3

        # Find uniqueness check
        uniqueness_check = next(c for c in report.checks if c.check_name == "uniqueness")
        assert uniqueness_check.passed is True


class TestDataQualityReport:
    """Tests for DataQualityReport class."""

    def test_create_quality_report(self) -> None:
        """Test creating a data quality report."""
        checks: tuple[QualityCheckResult, ...] = ()

        report = DataQualityReport(
            total_records=100,
            valid_records=95,
            invalid_records=5,
            completeness_score=0.95,
            validity_score=0.95,
            overall_score=0.95,
            checks=checks,
        )

        assert report.total_records == 100
        assert report.valid_records == 95
        assert report.invalid_records == 5
        assert report.completeness_score == 0.95
        assert report.validity_score == 0.95
        assert report.overall_score == 0.95
