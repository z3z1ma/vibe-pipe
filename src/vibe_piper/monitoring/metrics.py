"""
Metrics collection system for Vibe Piper.

This module provides a comprehensive metrics collection system that tracks:
- Execution time
- Record counts
- Custom metrics
- Resource utilization
"""

import logging
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any

from vibe_piper.types import AssetResult, ExecutionResult

# =============================================================================
# Logger
# =============================================================================

logger = logging.getLogger(__name__)

# =============================================================================
# Metric Types
# =============================================================================


class MetricType(Enum):
    """Types of metrics that can be collected."""

    COUNTER = auto()  # Monotonically increasing value
    GAUGE = auto()  # Current value that can go up or down
    HISTOGRAM = auto()  # Distribution of values
    TIMER = auto()  # Duration measurements
    SUMMARY = auto()  # Statistical summary of values


@dataclass(frozen=True)
class Metric:
    """
    A single metric measurement.

    Attributes:
        name: Metric name (e.g., "pipeline_execution_time")
        value: Metric value
        metric_type: Type of metric
        timestamp: When the metric was recorded
        labels: Optional labels/dimensions for the metric
        unit: Unit of measurement (e.g., "ms", "rows")
    """

    name: str
    value: int | float
    metric_type: MetricType
    timestamp: datetime = field(default_factory=datetime.utcnow)
    labels: Mapping[str, str] = field(default_factory=dict)
    unit: str | None = None


# =============================================================================
# Metrics Snapshot
# =============================================================================


@dataclass(frozen=True)
class MetricsSnapshot:
    """
    A snapshot of metrics at a point in time.

    Attributes:
        metrics: Collection of metrics
        timestamp: When the snapshot was taken
        pipeline_id: Optional pipeline identifier
        run_id: Optional run identifier
    """

    metrics: tuple[Metric, ...]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    pipeline_id: str | None = None
    run_id: str | None = None

    def get_metric(self, name: str, labels: Mapping[str, str] | None = None) -> Metric | None:
        """
        Get a specific metric from the snapshot.

        Args:
            name: Metric name
            labels: Optional labels to match

        Returns:
            Metric if found, None otherwise
        """
        for metric in self.metrics:
            if metric.name == name:
                if labels is None or metric.labels == labels:
                    return metric
        return None

    def filter_by_type(self, metric_type: MetricType) -> "MetricsSnapshot":
        """
        Filter metrics by type.

        Args:
            metric_type: Type of metric to filter

        Returns:
            New MetricsSnapshot with filtered metrics
        """
        filtered = tuple(m for m in self.metrics if m.metric_type == metric_type)
        return MetricsSnapshot(
            metrics=filtered,
            timestamp=self.timestamp,
            pipeline_id=self.pipeline_id,
            run_id=self.run_id,
        )


# =============================================================================
# Metrics Collector
# =============================================================================


class MetricsCollector:
    """
    Collects and aggregates metrics from pipeline executions.

    This class provides a thread-safe mechanism for collecting metrics
    throughout pipeline execution and producing snapshots for analysis.

    Attributes:
        enabled: Whether metrics collection is enabled
        include_system_metrics: Whether to include system-level metrics

    Example:
        Collect metrics from a pipeline execution::

            collector = MetricsCollector()
            result = engine.execute(graph, context, metrics_collector=collector)
            snapshot = collector.get_snapshot()
            print(f"Execution time: {snapshot.get_metric('total_duration_ms')}")
    """

    def __init__(
        self,
        enabled: bool = True,
        include_system_metrics: bool = False,
    ) -> None:
        """
        Initialize the metrics collector.

        Args:
            enabled: Whether to collect metrics
            include_system_metrics: Whether to include system-level metrics (CPU, memory)
        """
        self.enabled = enabled
        self.include_system_metrics = include_system_metrics
        self._metrics: list[Metric] = []
        self._lock = threading.Lock()
        self._start_time: float | None = None
        self._pipeline_id: str | None = None
        self._run_id: str | None = None

    def start_execution(self, pipeline_id: str, run_id: str) -> None:
        """
        Mark the start of pipeline execution.

        Args:
            pipeline_id: Pipeline identifier
            run_id: Run identifier
        """
        if not self.enabled:
            return

        with self._lock:
            self._start_time = time.time()
            self._pipeline_id = pipeline_id
            self._run_id = run_id
            logger.debug(f"Started metrics collection for pipeline {pipeline_id}, run {run_id}")

    def end_execution(self) -> None:
        """
        Mark the end of pipeline execution and calculate duration metrics.
        """
        if not self.enabled or self._start_time is None:
            return

        with self._lock:
            duration_ms = (time.time() - self._start_time) * 1000
            self.record_metric(
                name="total_execution_time_ms",
                value=duration_ms,
                metric_type=MetricType.TIMER,
                unit="ms",
                labels={"pipeline_id": self._pipeline_id or "unknown"},
            )
            logger.debug(
                f"Completed metrics collection: {duration_ms:.2f}ms, "
                f"{len(self._metrics)} metrics collected"
            )

    def record_metric(
        self,
        name: str,
        value: int | float,
        metric_type: MetricType = MetricType.GAUGE,
        labels: Mapping[str, str] | None = None,
        unit: str | None = None,
    ) -> None:
        """
        Record a single metric.

        Args:
            name: Metric name
            value: Metric value
            metric_type: Type of metric
            labels: Optional labels for the metric
            unit: Unit of measurement
        """
        if not self.enabled:
            return

        metric = Metric(
            name=name,
            value=value,
            metric_type=metric_type,
            labels=labels or {},
            unit=unit,
        )

        with self._lock:
            self._metrics.append(metric)

    def record_asset_execution(self, result: AssetResult) -> None:
        """
        Record metrics from an asset execution result.

        Args:
            result: AssetResult from execution
        """
        if not self.enabled:
            return

        labels = {
            "asset_name": result.asset_name,
            "success": str(result.success).lower(),
        }

        # Record execution time
        self.record_metric(
            name="asset_execution_time_ms",
            value=result.duration_ms,
            metric_type=MetricType.TIMER,
            unit="ms",
            labels=labels,
        )

        # Record metrics from the result
        for metric_name, metric_value in result.metrics.items():
            # Determine metric type based on the metric name
            if "count" in metric_name.lower():
                metric_type = MetricType.COUNTER
            elif "time" in metric_name.lower() or "duration" in metric_name.lower():
                metric_type = MetricType.TIMER
            else:
                metric_type = MetricType.GAUGE

            self.record_metric(
                name=f"asset_{metric_name}",
                value=float(metric_value),
                metric_type=metric_type,
                labels=labels,
            )

    def record_execution_result(self, result: ExecutionResult) -> None:
        """
        Record metrics from a complete execution result.

        Args:
            result: ExecutionResult from pipeline execution
        """
        if not self.enabled:
            return

        # Record overall execution metrics
        self.record_metric(
            name="assets_executed",
            value=result.assets_executed,
            metric_type=MetricType.COUNTER,
            labels={"success": str(result.success).lower()},
        )

        self.record_metric(
            name="assets_succeeded",
            value=result.assets_succeeded,
            metric_type=MetricType.COUNTER,
        )

        self.record_metric(
            name="assets_failed",
            value=result.assets_failed,
            metric_type=MetricType.COUNTER,
        )

        # Record aggregated metrics
        for metric_name, metric_value in result.metrics.items():
            if "count" in metric_name.lower():
                metric_type = MetricType.COUNTER
            elif "time" in metric_name.lower() or "duration" in metric_name.lower():
                metric_type = MetricType.TIMER
            else:
                metric_type = MetricType.GAUGE

            self.record_metric(
                name=f"pipeline_{metric_name}",
                value=float(metric_value),
                metric_type=metric_type,
            )

        # Record individual asset results
        for asset_name, asset_result in result.asset_results.items():
            self.record_asset_execution(asset_result)

    def get_snapshot(self) -> MetricsSnapshot:
        """
        Get a snapshot of all collected metrics.

        Returns:
            MetricsSnapshot with all collected metrics
        """
        with self._lock:
            return MetricsSnapshot(
                metrics=tuple(self._metrics),
                pipeline_id=self._pipeline_id,
                run_id=self._run_id,
            )

    def clear(self) -> None:
        """Clear all collected metrics."""
        with self._lock:
            self._metrics.clear()
            self._start_time = None
            self._pipeline_id = None
            self._run_id = None

    def to_dict(self) -> dict[str, Any]:
        """
        Convert all metrics to a dictionary representation.

        Returns:
            Dictionary mapping metric names to their values and metadata
        """
        snapshot = self.get_snapshot()
        result: dict[str, Any] = {
            "pipeline_id": snapshot.pipeline_id,
            "run_id": snapshot.run_id,
            "timestamp": snapshot.timestamp.isoformat(),
            "metrics": [],
        }

        for metric in snapshot.metrics:
            metric_dict = {
                "name": metric.name,
                "value": metric.value,
                "type": metric.metric_type.name,
                "timestamp": metric.timestamp.isoformat(),
                "labels": dict(metric.labels),
            }
            if metric.unit:
                metric_dict["unit"] = metric.unit

            result["metrics"].append(metric_dict)

        return result

    def __len__(self) -> int:
        """Return the number of metrics collected."""
        with self._lock:
            return len(self._metrics)
