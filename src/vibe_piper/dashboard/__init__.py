"""Data quality dashboard for Vibe Piper.

This module provides a web-based dashboard for monitoring data quality,
including real-time metrics, historical trends, anomaly detection, and
drill-down into validation failures.
"""

from vibe_piper.dashboard.anomaly_detection import AnomalyDetector, AnomalyDetectorConfig
from vibe_piper.dashboard.api import create_app
from vibe_piper.dashboard.models import (
    Anomaly,
    AnomalySeverity,
    QualityAggregation,
    QualityAlert,
    QualityHistory,
    QualitySnapshot,
)

__all__ = [
    "create_app",
    "AnomalyDetector",
    "AnomalyDetectorConfig",
    "Anomaly",
    "AnomalySeverity",
    "QualityAlert",
    "QualityAggregation",
    "QualityHistory",
    "QualitySnapshot",
]
