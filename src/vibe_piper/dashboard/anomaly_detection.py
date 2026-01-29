"""Anomaly detection for quality metrics.

This module provides anomaly detection algorithms that identify unusual
patterns or significant deviations in quality metrics.
"""

import logging
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum, auto

from vibe_piper.dashboard.models import (
    Anomaly,
    AnomalySeverity,
    QualitySnapshot,
)

# =============================================================================
# Logger
# =============================================================================

logger = logging.getLogger(__name__)

# =============================================================================
# Anomaly Detection Methods
# =============================================================================


class AnomalyDetectionMethod(Enum):
    """Methods for detecting anomalies."""

    Z_SCORE = auto()  # Statistical z-score based detection
    IQR = auto()  # Interquartile range
    THRESHOLD = auto()  # Static threshold breach
    CHANGE_POINT = auto()  # Sudden change detection
    MOVING_AVERAGE = auto()  # Deviation from moving average


# =============================================================================
# Anomaly Detector
# =============================================================================


@dataclass
class AnomalyDetectorConfig:
    """
    Configuration for anomaly detection.

    Attributes:
        method: Detection method to use
        z_score_threshold: Threshold for z-score method (default: 3.0)
        iqr_multiplier: Multiplier for IQR method (default: 1.5)
        threshold_value: Static threshold value
        change_point_threshold: Threshold for change point detection (default: 0.2 = 20%)
        moving_average_window: Window size for moving average (default: 7)
        min_data_points: Minimum data points required for detection (default: 5)
    """

    method: AnomalyDetectionMethod = AnomalyDetectionMethod.Z_SCORE
    z_score_threshold: float = 3.0
    iqr_multiplier: float = 1.5
    threshold_value: float | None = None
    change_point_threshold: float = 0.2
    moving_average_window: int = 7
    min_data_points: int = 5


class AnomalyDetector:
    """
    Detects anomalies in quality metrics.

    Uses statistical methods to identify unusual patterns
    such as sudden drops in quality scores or
    excessive validation failures.
    """

    def __init__(self, config: AnomalyDetectorConfig | None = None) -> None:
        """
        Initialize anomaly detector.

        Args:
            config: Detector configuration
        """
        self.config = config or AnomalyDetectorConfig()

    def detect_anomalies(
        self,
        history: Sequence[QualitySnapshot],
        metric_name: str = "overall_score",
    ) -> Sequence[Anomaly]:
        """
        Detect anomalies in quality history.

        Args:
            history: Historical quality snapshots
            metric_name: Name of metric to analyze

        Returns:
            List of detected anomalies
        """
        if len(history) < self.config.min_data_points:
            logger.debug(f"Not enough data points for anomaly detection: {len(history)}")
            return []

        method = self.config.method
        anomalies: list[Anomaly] = []

        if method == AnomalyDetectionMethod.Z_SCORE:
            anomalies = self._detect_z_score(history, metric_name)
        elif method == AnomalyDetectionMethod.IQR:
            anomalies = self._detect_iqr(history, metric_name)
        elif method == AnomalyDetectionMethod.THRESHOLD:
            anomalies = self._detect_threshold(history, metric_name)
        elif method == AnomalyDetectionMethod.CHANGE_POINT:
            anomalies = self._detect_change_points(history, metric_name)
        elif method == AnomalyDetectionMethod.MOVING_AVERAGE:
            anomalies = self._detect_moving_average(history, metric_name)
        else:
            logger.warning(f"Unknown anomaly detection method: {method}")

        return anomalies

    def _detect_z_score(
        self,
        history: Sequence[QualitySnapshot],
        metric_name: str,
    ) -> Sequence[Anomaly]:
        """
        Detect anomalies using z-score method.

        Values more than N standard deviations from mean are flagged.

        Args:
            history: Historical quality snapshots
            metric_name: Name of metric to analyze

        Returns:
            List of anomalies
        """
        import statistics

        anomalies: list[Anomaly] = []

        # Extract metric values
        values = []
        for snapshot in history:
            if metric_name == "overall_score":
                values.append(snapshot.overall_score)
            elif metric_name == "completeness_score":
                values.append(snapshot.completeness_score)
            elif metric_name == "validity_score":
                values.append(snapshot.validity_score)
            else:
                continue

        if len(values) < self.config.min_data_points:
            return []

        # Calculate statistics
        mean_val = statistics.mean(values)
        std_dev = statistics.stdev(values)

        threshold = self.config.z_score_threshold

        # Detect anomalies
        for i, (snapshot, value) in enumerate(zip(history, values)):
            z_score = abs((value - mean_val) / std_dev) if std_dev > 0 else 0

            if z_score > threshold:
                # Determine severity based on z-score
                if z_score > 4:
                    severity = AnomalySeverity.CRITICAL
                elif z_score > 3:
                    severity = AnomalySeverity.HIGH
                else:
                    severity = AnomalySeverity.MEDIUM

                anomalies.append(
                    Anomaly(
                        id=str(uuid.uuid4()),
                        asset_name=snapshot.asset_name,
                        timestamp=snapshot.timestamp,
                        severity=severity,
                        anomaly_type="z_score_outlier",
                        description=(
                            f"Z-score of {z_score:.2f} detected for {metric_name}. "
                            f"Value: {value:.4f}, Mean: {mean_val:.4f}, StdDev: {std_dev:.4f}"
                        ),
                        expected_value=mean_val,
                        actual_value=value,
                        deviation_percentage=abs((value - mean_val) / mean_val * 100)
                        if mean_val != 0
                        else None,
                        affected_metrics=(metric_name,),
                        related_snapshots=(snapshot,),
                    )
                )

        return anomalies

    def _detect_iqr(
        self,
        history: Sequence[QualitySnapshot],
        metric_name: str,
    ) -> Sequence[Anomaly]:
        """
        Detect anomalies using interquartile range (IQR) method.

        Values outside Q1 - k*IQR or Q3 + k*IQR are flagged.

        Args:
            history: Historical quality snapshots
            metric_name: Name of metric to analyze

        Returns:
            List of anomalies
        """
        import statistics

        anomalies: list[Anomaly] = []

        # Extract metric values
        values = []
        for snapshot in history:
            if metric_name == "overall_score":
                values.append(snapshot.overall_score)
            elif metric_name == "completeness_score":
                values.append(snapshot.completeness_score)
            elif metric_name == "validity_score":
                values.append(snapshot.validity_score)
            else:
                continue

        if len(values) < self.config.min_data_points:
            return []

        # Calculate IQR
        sorted_values = sorted(values)
        q1 = statistics.quantiles(sorted_values, n=4)[0]
        q3 = statistics.quantiles(sorted_values, n=4)[2]
        iqr = q3 - q1

        lower_bound = q1 - (self.config.iqr_multiplier * iqr)
        upper_bound = q3 + (self.config.iqr_multiplier * iqr)

        # Detect anomalies
        for snapshot, value in zip(history, values):
            if value < lower_bound or value > upper_bound:
                # Determine severity based on distance from bounds
                distance = min(abs(value - lower_bound), abs(value - upper_bound)) / iqr

                if distance > 2:
                    severity = AnomalySeverity.CRITICAL
                elif distance > 1:
                    severity = AnomalySeverity.HIGH
                else:
                    severity = AnomalySeverity.MEDIUM

                anomalies.append(
                    Anomaly(
                        id=str(uuid.uuid4()),
                        asset_name=snapshot.asset_name,
                        timestamp=snapshot.timestamp,
                        severity=severity,
                        anomaly_type="iqr_outlier",
                        description=(
                            f"Value {value:.4f} outside IQR bounds [{lower_bound:.4f}, {upper_bound:.4f}]. "
                            f"IQR: {iqr:.4f}"
                        ),
                        expected_value=(q1 + q3) / 2,
                        actual_value=value,
                        deviation_percentage=None,
                        affected_metrics=(metric_name,),
                        related_snapshots=(snapshot,),
                    )
                )

        return anomalies

    def _detect_threshold(
        self,
        history: Sequence[QualitySnapshot],
        metric_name: str,
    ) -> Sequence[Anomaly]:
        """
        Detect anomalies using static threshold.

        Values below (or above) threshold are flagged.

        Args:
            history: Historical quality snapshots
            metric_name: Name of metric to analyze

        Returns:
            List of anomalies
        """
        if self.config.threshold_value is None:
            logger.warning("Threshold value not configured")
            return []

        anomalies: list[Anomaly] = []

        for snapshot in history:
            value: float | None = None

            if metric_name == "overall_score":
                value = snapshot.overall_score
            elif metric_name == "completeness_score":
                value = snapshot.completeness_score
            elif metric_name == "validity_score":
                value = snapshot.validity_score

            if value is not None and value < self.config.threshold_value:
                severity = (
                    AnomalySeverity.CRITICAL
                    if value < self.config.threshold_value * 0.5
                    else (
                        AnomalySeverity.HIGH
                        if value < self.config.threshold_value * 0.75
                        else AnomalySeverity.MEDIUM
                    )
                )

                anomalies.append(
                    Anomaly(
                        id=str(uuid.uuid4()),
                        asset_name=snapshot.asset_name,
                        timestamp=snapshot.timestamp,
                        severity=severity,
                        anomaly_type="threshold_breach",
                        description=(
                            f"Value {value:.4f} below threshold {self.config.threshold_value:.4f}"
                        ),
                        expected_value=self.config.threshold_value,
                        actual_value=value,
                        deviation_percentage=abs(
                            (value - self.config.threshold_value)
                            / self.config.threshold_value
                            * 100
                        ),
                        affected_metrics=(metric_name,),
                        related_snapshots=(snapshot,),
                    )
                )

        return anomalies

    def _detect_change_points(
        self,
        history: Sequence[QualitySnapshot],
        metric_name: str,
    ) -> Sequence[Anomaly]:
        """
        Detect sudden changes in metric values.

        Args:
            history: Historical quality snapshots
            metric_name: Name of metric to analyze

        Returns:
            List of anomalies
        """
        anomalies: list[Anomaly] = []

        # Extract metric values
        values = []
        for snapshot in history:
            if metric_name == "overall_score":
                values.append(snapshot.overall_score)
            elif metric_name == "completeness_score":
                values.append(snapshot.completeness_score)
            elif metric_name == "validity_score":
                values.append(snapshot.validity_score)
            else:
                continue

        if len(values) < 2:
            return []

        threshold = self.config.change_point_threshold

        # Detect changes
        for i in range(1, len(values)):
            prev_value = values[i - 1]
            curr_value = values[i]

            change_pct = abs((curr_value - prev_value) / prev_value) if prev_value != 0 else 0

            if change_pct > threshold:
                snapshot = history[i]
                severity = (
                    AnomalySeverity.CRITICAL
                    if change_pct > threshold * 2
                    else (
                        AnomalySeverity.HIGH
                        if change_pct > threshold * 1.5
                        else AnomalySeverity.MEDIUM
                    )
                )

                anomalies.append(
                    Anomaly(
                        id=str(uuid.uuid4()),
                        asset_name=snapshot.asset_name,
                        timestamp=snapshot.timestamp,
                        severity=severity,
                        anomaly_type="sudden_change",
                        description=(
                            f"Sudden {change_pct:.1%} change in {metric_name}. "
                            f"Previous: {prev_value:.4f}, Current: {curr_value:.4f}"
                        ),
                        expected_value=prev_value,
                        actual_value=curr_value,
                        deviation_percentage=change_pct * 100,
                        affected_metrics=(metric_name,),
                        related_snapshots=(history[i - 1], snapshot),
                    )
                )

        return anomalies

    def _detect_moving_average(
        self,
        history: Sequence[QualitySnapshot],
        metric_name: str,
    ) -> Sequence[Anomaly]:
        """
        Detect anomalies by comparing to moving average.

        Args:
            history: Historical quality snapshots
            metric_name: Name of metric to analyze

        Returns:
            List of anomalies
        """
        import statistics

        anomalies: list[Anomaly] = []

        # Extract metric values
        values = []
        for snapshot in history:
            if metric_name == "overall_score":
                values.append(snapshot.overall_score)
            elif metric_name == "completeness_score":
                values.append(snapshot.completeness_score)
            elif metric_name == "validity_score":
                values.append(snapshot.validity_score)
            else:
                continue

        window = self.config.moving_average_window

        if len(values) < window:
            return []

        # Calculate moving averages
        moving_avg: list[float] = []

        for i in range(window, len(values) + 1):
            window_values = values[i - window : i]
            moving_avg.append(statistics.mean(window_values))

        # Detect deviations
        for i in range(len(moving_avg)):
            actual_value = values[i + window - 1]
            expected_value = moving_avg[i]

            deviation = (
                abs((actual_value - expected_value) / expected_value) if expected_value != 0 else 0
            )

            if deviation > 0.2:  # More than 20% deviation
                snapshot = history[i + window - 1]
                severity = (
                    AnomalySeverity.CRITICAL
                    if deviation > 0.4
                    else (AnomalySeverity.HIGH if deviation > 0.3 else AnomalySeverity.MEDIUM)
                )

                anomalies.append(
                    Anomaly(
                        id=str(uuid.uuid4()),
                        asset_name=snapshot.asset_name,
                        timestamp=snapshot.timestamp,
                        severity=severity,
                        anomaly_type="moving_average_deviation",
                        description=(
                            f"Value {actual_value:.4f} deviates {deviation:.1%} "
                            f"from moving average {expected_value:.4f} (window={window})"
                        ),
                        expected_value=expected_value,
                        actual_value=actual_value,
                        deviation_percentage=deviation * 100,
                        affected_metrics=(metric_name,),
                        related_snapshots=(snapshot,),
                    )
                )

        return anomalies
