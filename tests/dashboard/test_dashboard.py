"""Tests for quality dashboard module."""

from datetime import datetime, timedelta

import pytest

from vibe_piper.dashboard.anomaly_detection import (
    AnomalyDetectionMethod,
    AnomalyDetector,
    AnomalyDetectorConfig,
)
from vibe_piper.dashboard.models import (
    AlertStatus,
    Anomaly,
    AnomalySeverity,
    QualityAggregation,
    QualityAlert,
    QualityHistory,
    QualitySnapshot,
)

# =============================================================================
# Quality Snapshot Tests
# =============================================================================


def test_quality_snapshot_creation() -> None:
    """Test creating a quality snapshot."""
    snapshot = QualitySnapshot(
        asset_name="test_asset",
        timestamp=datetime.now(),
        total_records=1000,
        valid_records=950,
        invalid_records=50,
        completeness_score=0.95,
        validity_score=0.95,
        overall_score=0.95,
        metrics=(),
    )

    assert snapshot.asset_name == "test_asset"
    assert snapshot.total_records == 1000
    assert snapshot.valid_records == 950
    assert snapshot.invalid_records == 50
    assert snapshot.completeness_score == 0.95
    assert snapshot.validity_score == 0.95
    assert snapshot.overall_score == 0.95


def test_quality_history_add_snapshot() -> None:
    """Test adding snapshots to quality history."""
    history = QualityHistory(asset_name="test_asset")

    now = datetime.now()
    snapshot1 = QualitySnapshot(
        asset_name="test_asset",
        timestamp=now - timedelta(hours=2),
        total_records=1000,
        valid_records=900,
        invalid_records=100,
        completeness_score=0.90,
        validity_score=0.90,
        overall_score=0.90,
        metrics=(),
    )

    snapshot2 = QualitySnapshot(
        asset_name="test_asset",
        timestamp=now - timedelta(hours=1),
        total_records=1000,
        valid_records=950,
        invalid_records=50,
        completeness_score=0.95,
        validity_score=0.95,
        overall_score=0.95,
        metrics=(),
    )

    history.add_snapshot(snapshot1)
    history.add_snapshot(snapshot2)

    assert len(history.snapshots) == 2
    assert history.average_score == 0.925


def test_quality_history_latest_snapshot() -> None:
    """Test getting latest snapshot from history."""
    history = QualityHistory(asset_name="test_asset")

    now = datetime.now()

    older_snapshot = QualitySnapshot(
        asset_name="test_asset",
        timestamp=now - timedelta(hours=2),
        total_records=1000,
        valid_records=900,
        invalid_records=100,
        completeness_score=0.90,
        validity_score=0.90,
        overall_score=0.90,
        metrics=(),
    )

    newer_snapshot = QualitySnapshot(
        asset_name="test_asset",
        timestamp=now - timedelta(hours=1),
        total_records=1000,
        valid_records=950,
        invalid_records=50,
        completeness_score=0.95,
        validity_score=0.95,
        overall_score=0.95,
        metrics=(),
    )

    history.add_snapshot(older_snapshot)
    history.add_snapshot(newer_snapshot)

    assert history.latest_snapshot == newer_snapshot
    assert history.latest_snapshot.overall_score == 0.95


def test_quality_history_get_trend() -> None:
    """Test getting trend snapshots within time window."""
    history = QualityHistory(asset_name="test_asset")

    now = datetime.now()

    # Add snapshots with different timestamps
    for i in range(5):
        snapshot = QualitySnapshot(
            asset_name="test_asset",
            timestamp=now - timedelta(hours=i + 1),
            total_records=1000,
            valid_records=950 - (i * 10),
            invalid_records=50 + (i * 10),
            completeness_score=0.95 - (i * 0.01),
            validity_score=0.95 - (i * 0.01),
            overall_score=0.95 - (i * 0.01),
            metrics=(),
        )
        history.add_snapshot(snapshot)

    # Get trend for last 3 hours (should include first 3 snapshots)
    trend = history.get_trend(hours=3)
    assert len(trend) == 3


def test_quality_history_empty() -> None:
    """Test empty quality history."""
    history = QualityHistory(asset_name="test_asset")

    assert history.latest_snapshot is None
    assert history.average_score == 0.0
    assert len(history.snapshots) == 0


# =============================================================================
# Anomaly Detection Tests
# =============================================================================


def test_anomaly_detector_z_score() -> None:
    """Test z-score based anomaly detection."""
    detector = AnomalyDetector(
        config=AnomalyDetectorConfig(
            method=AnomalyDetectionMethod.Z_SCORE,
            z_score_threshold=2.5,
        )
    )

    # Create snapshots with one outlier
    now = datetime.now()
    snapshots: list[QualitySnapshot] = []

    for i in range(10):
        score = 0.95 + (i * 0.01)
        snapshot = QualitySnapshot(
            asset_name="test_asset",
            timestamp=now - timedelta(hours=10 - i),
            total_records=1000,
            valid_records=int(score * 1000),
            invalid_records=1000 - int(score * 1000),
            completeness_score=score,
            validity_score=score,
            overall_score=score,
            metrics=(),
        )
        snapshots.append(snapshot)

    # Add an outlier (much lower score)
    outlier = QualitySnapshot(
        asset_name="test_asset",
        timestamp=now - timedelta(hours=5),
        total_records=1000,
        valid_records=600,  # Much lower
        invalid_records=400,
        completeness_score=0.60,  # Outlier
        validity_score=0.60,
        overall_score=0.60,
        metrics=(),
    )
    snapshots.insert(5, outlier)  # Insert in the middle

    anomalies = detector.detect_anomalies(snapshots, metric_name="overall_score")

    # Should detect the outlier
    assert len(anomalies) > 0
    assert any(a.actual_value == 0.60 for a in anomalies)


def test_anomaly_detector_threshold() -> None:
    """Test threshold-based anomaly detection."""
    detector = AnomalyDetector(
        config=AnomalyDetectorConfig(
            method=AnomalyDetectionMethod.THRESHOLD,
            threshold_value=0.90,
        )
    )

    # Create snapshots
    now = datetime.now()
    snapshots: list[QualitySnapshot] = []

    for i in range(5):
        snapshot = QualitySnapshot(
            asset_name="test_asset",
            timestamp=now - timedelta(hours=i + 1),
            total_records=1000,
            valid_records=int(0.95 * 1000),
            invalid_records=int(0.05 * 1000),
            completeness_score=0.95,
            validity_score=0.95,
            overall_score=0.95,
            metrics=(),
        )
        snapshots.append(snapshot)

    # Add one below threshold
    below_threshold = QualitySnapshot(
        asset_name="test_asset",
        timestamp=now - timedelta(hours=3),
        total_records=1000,
        valid_records=800,
        invalid_records=200,
        completeness_score=0.80,  # Below threshold
        validity_score=0.80,
        overall_score=0.80,
        metrics=(),
    )
    snapshots.insert(2, below_threshold)

    anomalies = detector.detect_anomalies(snapshots, metric_name="overall_score")

    # Should detect the snapshot below threshold
    assert len(anomalies) == 1
    assert anomalies[0].actual_value == 0.80
    assert anomalies[0].anomaly_type == "threshold_breach"


def test_anomaly_detector_insufficient_data() -> None:
    """Test anomaly detection with insufficient data."""
    detector = AnomalyDetector(
        config=AnomalyDetectorConfig(
            method=AnomalyDetectionMethod.Z_SCORE,
            min_data_points=5,
        )
    )

    # Create only 3 snapshots (below min_data_points)
    now = datetime.now()
    snapshots: list[QualitySnapshot] = []

    for i in range(3):
        snapshot = QualitySnapshot(
            asset_name="test_asset",
            timestamp=now - timedelta(hours=i + 1),
            total_records=1000,
            valid_records=950,
            invalid_records=50,
            completeness_score=0.95,
            validity_score=0.95,
            overall_score=0.95,
            metrics=(),
        )
        snapshots.append(snapshot)

    anomalies = detector.detect_anomalies(snapshots)

    # Should not detect anomalies with insufficient data
    assert len(anomalies) == 0


# =============================================================================
# Quality Alert Tests
# =============================================================================


def test_quality_alert_creation() -> None:
    """Test creating a quality alert."""
    now = datetime.now()

    alert = QualityAlert(
        id="alert-123",
        asset_name="test_asset",
        alert_type="threshold_breach",
        status=AlertStatus.ACTIVE,
        severity=AnomalySeverity.HIGH,
        title="Quality score below threshold",
        message="Overall quality score is 0.80, which is below the threshold of 0.90",
        created_at=now,
    )

    assert alert.id == "alert-123"
    assert alert.asset_name == "test_asset"
    assert alert.alert_type == "threshold_breach"
    assert alert.status == AlertStatus.ACTIVE
    assert alert.severity == AnomalySeverity.HIGH


def test_quality_alert_acknowledge() -> None:
    """Test acknowledging a quality alert."""
    now = datetime.now()

    alert = QualityAlert(
        id="alert-123",
        asset_name="test_asset",
        alert_type="threshold_breach",
        status=AlertStatus.ACTIVE,
        severity=AnomalySeverity.HIGH,
        title="Quality score below threshold",
        message="Quality score below threshold",
        created_at=now,
    )

    assert alert.status == AlertStatus.ACTIVE
    assert alert.acknowledged_at is None

    alert.acknowledge()

    assert alert.status == AlertStatus.ACKNOWLEDGED
    assert alert.acknowledged_at is not None


def test_quality_alert_resolve() -> None:
    """Test resolving a quality alert."""
    now = datetime.now()

    alert = QualityAlert(
        id="alert-123",
        asset_name="test_asset",
        alert_type="threshold_breach",
        status=AlertStatus.ACKNOWLEDGED,
        severity=AnomalySeverity.HIGH,
        title="Quality score below threshold",
        message="Quality score below threshold",
        created_at=now,
        acknowledged_at=now,
    )

    assert alert.status == AlertStatus.ACKNOWLEDGED
    assert alert.resolved_at is None

    alert.resolve()

    assert alert.status == AlertStatus.RESOLVED
    assert alert.resolved_at is not None


# =============================================================================
# Quality Aggregation Tests
# =============================================================================


def test_quality_aggregation_creation() -> None:
    """Test creating a quality aggregation."""
    now = datetime.now()

    aggregation = QualityAggregation(
        period="day",
        start_time=now - timedelta(days=1),
        end_time=now,
        asset_name="test_asset",
        avg_completeness=0.94,
        avg_validity=0.93,
        avg_overall=0.935,
        min_overall=0.90,
        max_overall=0.97,
        total_snapshots=24,
        total_records=24000,
    )

    assert aggregation.period == "day"
    assert aggregation.asset_name == "test_asset"
    assert aggregation.avg_overall == 0.935
    assert aggregation.total_snapshots == 24


# =============================================================================
# Anomaly Model Tests
# =============================================================================


def test_anomaly_creation() -> None:
    """Test creating an anomaly."""
    now = datetime.now()

    anomaly = Anomaly(
        id="anomaly-123",
        asset_name="test_asset",
        timestamp=now,
        severity=AnomalySeverity.HIGH,
        anomaly_type="sudden_drop",
        description="Quality score dropped from 0.95 to 0.70",
        expected_value=0.95,
        actual_value=0.70,
        deviation_percentage=26.3,
        affected_metrics=("overall_score", "completeness_score"),
    )

    assert anomaly.id == "anomaly-123"
    assert anomaly.asset_name == "test_asset"
    assert anomaly.severity == AnomalySeverity.HIGH
    assert anomaly.anomaly_type == "sudden_drop"
    assert anomaly.expected_value == 0.95
    assert anomaly.actual_value == 0.70
    assert anomaly.deviation_percentage == 26.3
    assert len(anomaly.affected_metrics) == 2


def test_anomaly_is_critical() -> None:
    """Test anomaly severity checks."""
    now = datetime.now()

    critical_anomaly = Anomaly(
        id="anomaly-1",
        asset_name="test_asset",
        timestamp=now,
        severity=AnomalySeverity.CRITICAL,
        anomaly_type="critical_failure",
        description="Critical failure",
    )

    high_anomaly = Anomaly(
        id="anomaly-2",
        asset_name="test_asset",
        timestamp=now,
        severity=AnomalySeverity.HIGH,
        anomaly_type="high_severity",
        description="High severity issue",
    )

    medium_anomaly = Anomaly(
        id="anomaly-3",
        asset_name="test_asset",
        timestamp=now,
        severity=AnomalySeverity.MEDIUM,
        anomaly_type="medium_severity",
        description="Medium severity issue",
    )

    assert critical_anomaly.is_critical is True
    assert critical_anomaly.is_high is True

    assert high_anomaly.is_critical is False
    assert high_anomaly.is_high is True

    assert medium_anomaly.is_critical is False
    assert medium_anomaly.is_high is False
