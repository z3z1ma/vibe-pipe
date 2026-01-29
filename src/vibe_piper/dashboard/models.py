"""Data models for the quality dashboard.

These models represent quality metrics stored for historical tracking,
anomaly detection, and alerting.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any

from vibe_piper.types import DataQualityReport, QualityMetric

# =============================================================================
# Severity Levels
# =============================================================================


class AnomalySeverity(Enum):
    """Severity levels for anomalies."""

    INFO = auto()
    LOW = auto()
    MEDIUM = auto()
    HIGH = auto()
    CRITICAL = auto()


class AlertStatus(Enum):
    """Status of quality alerts."""

    ACTIVE = auto()
    ACKNOWLEDGED = auto()
    RESOLVED = auto()
    SUPPRESSED = auto()


# =============================================================================
# Quality Snapshot
# =============================================================================


@dataclass(frozen=True)
class QualitySnapshot:
    """
    A snapshot of quality metrics at a point in time.

    Captures quality scores for a specific asset at a specific timestamp,
    storing individual metrics for detailed analysis.
    """

    asset_name: str
    """Name of the asset this snapshot represents."""

    timestamp: datetime
    """When this snapshot was taken."""

    total_records: int
    """Total number of records in the asset."""

    valid_records: int
    """Number of valid records."""

    invalid_records: int
    """Number of invalid records."""

    completeness_score: float
    """Completeness score (0-1)."""

    validity_score: float
    """Validity score (0-1)."""

    overall_score: float
    """Overall quality score (0-1)."""

    metrics: tuple[QualityMetric, ...] = field(default_factory=tuple)
    """Individual quality metrics."""

    pipeline_id: str | None = None
    """Optional pipeline ID that generated this snapshot."""

    run_id: str | None = None
    """Optional run ID that generated this snapshot."""

    metadata: dict[str, Any] = field(default_factory=dict)
    """Additional metadata."""

    @classmethod
    def from_report(
        cls,
        asset_name: str,
        report: DataQualityReport,
        pipeline_id: str | None = None,
        run_id: str | None = None,
    ) -> "QualitySnapshot":
        """
        Create a snapshot from a DataQualityReport.

        Args:
            asset_name: Name of the asset
            report: Data quality report
            pipeline_id: Optional pipeline ID
            run_id: Optional run ID

        Returns:
            QualitySnapshot instance
        """
        # Extract metrics from all check results
        all_metrics: list[QualityMetric] = []
        for check_result in report.checks:
            all_metrics.extend(check_result.metrics)

        return cls(
            asset_name=asset_name,
            timestamp=report.timestamp,
            total_records=report.total_records,
            valid_records=report.valid_records,
            invalid_records=report.invalid_records,
            completeness_score=report.completeness_score,
            validity_score=report.validity_score,
            overall_score=report.overall_score,
            metrics=tuple(all_metrics),
            pipeline_id=pipeline_id,
            run_id=run_id,
        )


# =============================================================================
# Quality History
# =============================================================================


@dataclass
class QualityHistory:
    """
    Historical quality data for an asset.

    Aggregates quality snapshots over time for trend analysis.
    """

    asset_name: str
    """Name of the asset."""

    snapshots: tuple[QualitySnapshot, ...] = field(default_factory=tuple)
    """Historical snapshots ordered by timestamp."""

    @property
    def latest_snapshot(self) -> QualitySnapshot | None:
        """Get the most recent snapshot."""
        if not self.snapshots:
            return None
        return max(self.snapshots, key=lambda s: s.timestamp)

    @property
    def average_score(self) -> float:
        """Calculate average overall score across all snapshots."""
        if not self.snapshots:
            return 0.0
        return sum(s.overall_score for s in self.snapshots) / len(self.snapshots)

    def get_trend(self, hours: int = 24) -> tuple[QualitySnapshot, ...]:
        """
        Get snapshots from the last N hours.

        Args:
            hours: Number of hours to look back

        Returns:
            Tuple of snapshots within the time window
        """
        cutoff = datetime.now() - __import__("datetime").timedelta(hours=hours)
        return tuple(s for s in self.snapshots if s.timestamp >= cutoff)

    def add_snapshot(self, snapshot: QualitySnapshot) -> None:
        """
        Add a new snapshot to history.

        Args:
            snapshot: Snapshot to add
        """
        snapshots_list = list(self.snapshots)
        snapshots_list.append(snapshot)
        snapshots_list.sort(key=lambda s: s.timestamp)
        self.snapshots = tuple(snapshots_list)


# =============================================================================
# Anomaly Detection
# =============================================================================


@dataclass(frozen=True)
class Anomaly:
    """
    Detected quality anomaly.

    Represents a significant deviation from expected quality patterns,
    such as sudden drops in quality scores or unusual validation failures.
    """

    id: str
    """Unique identifier for this anomaly."""

    asset_name: str
    """Name of the asset where anomaly was detected."""

    timestamp: datetime
    """When the anomaly was detected."""

    severity: AnomalySeverity
    """Severity level of the anomaly."""

    anomaly_type: str
    """Type of anomaly (e.g., 'score_drop', 'validation_failure')."""

    description: str
    """Human-readable description of the anomaly."""

    expected_value: float | None = None
    """Expected value (e.g., normal quality score)."""

    actual_value: float | None = None
    """Actual observed value."""

    deviation_percentage: float | None = None
    """Percentage deviation from expected."""

    affected_metrics: tuple[str, ...] = field(default_factory=tuple)
    """Names of metrics affected by this anomaly."""

    related_snapshots: tuple[QualitySnapshot, ...] = field(default_factory=tuple)
    """Snapshots related to this anomaly."""

    metadata: dict[str, Any] = field(default_factory=dict)
    """Additional metadata."""

    @property
    def is_critical(self) -> bool:
        """Check if anomaly is critical."""
        return self.severity == AnomalySeverity.CRITICAL

    @property
    def is_high(self) -> bool:
        """Check if anomaly is high severity."""
        return self.severity in {AnomalySeverity.HIGH, AnomalySeverity.CRITICAL}


# =============================================================================
# Quality Alerts
# =============================================================================


@dataclass
class QualityAlert:
    """
    Alert triggered by quality issues.

    Represents an alert that has been triggered based on quality thresholds,
    anomaly detection, or manual configuration.
    """

    id: str
    """Unique identifier for this alert."""

    asset_name: str
    """Name of the asset this alert is for."""

    alert_type: str
    """Type of alert (e.g., 'threshold_breach', 'anomaly')."""

    status: AlertStatus
    """Current status of the alert."""

    severity: AnomalySeverity
    """Severity level."""

    title: str
    """Alert title."""

    message: str
    """Alert message."""

    created_at: datetime
    """When the alert was created."""

    acknowledged_at: datetime | None = None
    """When the alert was acknowledged."""

    resolved_at: datetime | None = None
    """When the alert was resolved."""

    anomaly_id: str | None = None
    """ID of related anomaly (if any)."""

    snapshot: QualitySnapshot | None = None
    """Quality snapshot that triggered the alert."""

    threshold_config: dict[str, Any] = field(default_factory=dict)
    """Threshold configuration that triggered this alert."""

    metadata: dict[str, Any] = field(default_factory=dict)
    """Additional metadata."""

    def acknowledge(self) -> None:
        """Mark alert as acknowledged."""
        if self.status == AlertStatus.ACTIVE:
            self.status = AlertStatus.ACKNOWLEDGED
            self.acknowledged_at = datetime.now()

    def resolve(self) -> None:
        """Mark alert as resolved."""
        if self.status in {AlertStatus.ACTIVE, AlertStatus.ACKNOWLEDGED}:
            self.status = AlertStatus.RESOLVED
            self.resolved_at = datetime.now()


# =============================================================================
# Aggregation Results
# =============================================================================


@dataclass(frozen=True)
class QualityAggregation:
    """
    Aggregated quality metrics across assets or time periods.

    Provides summarized quality metrics for dashboard views.
    """

    period: str
    """Time period (e.g., 'day', 'week', 'month')."""

    start_time: datetime
    """Start of aggregation period."""

    end_time: datetime
    """End of aggregation period."""

    asset_name: str | None = None
    """Optional asset name (None for all assets)."""

    avg_completeness: float = 0.0
    """Average completeness score."""

    avg_validity: float = 0.0
    """Average validity score."""

    avg_overall: float = 0.0
    """Average overall score."""

    min_overall: float = 0.0
    """Minimum overall score."""

    max_overall: float = 0.0
    """Maximum overall score."""

    total_snapshots: int = 0
    """Total number of snapshots included."""

    total_records: int = 0
    """Total records processed."""

    failed_snapshots: int = 0
    """Number of snapshots that failed quality checks."""

    metadata: dict[str, Any] = field(default_factory=dict)
    """Additional metadata."""
