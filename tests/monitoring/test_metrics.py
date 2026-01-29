"""
Tests for the metrics collection system.
"""

from datetime import datetime

import pytest

from vibe_piper.monitoring.metrics import (
    Metric,
    MetricsCollector,
    MetricsSnapshot,
    MetricType,
)
from vibe_piper.types import AssetResult, ExecutionResult


def test_metric_creation() -> None:
    """Test creating a metric."""
    metric = Metric(
        name="test_metric",
        value=42,
        metric_type=MetricType.COUNTER,
        labels={"environment": "test"},
        unit="count",
    )

    assert metric.name == "test_metric"
    assert metric.value == 42
    assert metric.metric_type == MetricType.COUNTER
    assert metric.labels == {"environment": "test"}
    assert metric.unit == "count"


def test_metrics_snapshot_creation() -> None:
    """Test creating a metrics snapshot."""
    metric1 = Metric(
        name="metric1",
        value=10,
        metric_type=MetricType.GAUGE,
    )
    metric2 = Metric(
        name="metric2",
        value=20,
        metric_type=MetricType.GAUGE,
    )

    snapshot = MetricsSnapshot(
        metrics=(metric1, metric2),
        pipeline_id="test_pipeline",
        run_id="test_run",
    )

    assert len(snapshot.metrics) == 2
    assert snapshot.pipeline_id == "test_pipeline"
    assert snapshot.run_id == "test_run"


def test_metrics_snapshot_get_metric() -> None:
    """Test retrieving a metric from a snapshot."""
    metric = Metric(
        name="test_metric",
        value=42,
        metric_type=MetricType.COUNTER,
        labels={"env": "test"},
    )

    snapshot = MetricsSnapshot(metrics=(metric,))

    # Test retrieval with matching labels
    result = snapshot.get_metric("test_metric", {"env": "test"})
    assert result is not None
    assert result.value == 42

    # Test retrieval without labels
    result = snapshot.get_metric("test_metric")
    assert result is not None

    # Test retrieval of non-existent metric
    result = snapshot.get_metric("nonexistent")
    assert result is None


def test_metrics_snapshot_filter_by_type() -> None:
    """Test filtering metrics by type."""
    metric1 = Metric(
        name="counter_metric",
        value=10,
        metric_type=MetricType.COUNTER,
    )
    metric2 = Metric(
        name="gauge_metric",
        value=20,
        metric_type=MetricType.GAUGE,
    )

    snapshot = MetricsSnapshot(metrics=(metric1, metric2))

    filtered = snapshot.filter_by_type(MetricType.COUNTER)
    assert len(filtered.metrics) == 1
    assert filtered.metrics[0].name == "counter_metric"


def test_metrics_collector_initialization() -> None:
    """Test metrics collector initialization."""
    collector = MetricsCollector(enabled=True)

    assert collector.enabled is True
    assert len(collector) == 0


def test_metrics_collector_disabled() -> None:
    """Test that disabled collector doesn't collect metrics."""
    collector = MetricsCollector(enabled=False)

    collector.record_metric("test_metric", 42)

    assert len(collector) == 0


def test_metrics_collector_record_metric() -> None:
    """Test recording a single metric."""
    collector = MetricsCollector(enabled=True)

    collector.record_metric(
        name="test_metric",
        value=100,
        metric_type=MetricType.GAUGE,
        unit="ms",
    )

    assert len(collector) == 1

    snapshot = collector.get_snapshot()
    metric = snapshot.metrics[0]
    assert metric.name == "test_metric"
    assert metric.value == 100
    assert metric.unit == "ms"


def test_metrics_collector_start_end_execution() -> None:
    """Test recording execution duration."""
    collector = MetricsCollector(enabled=True)

    collector.start_execution("test_pipeline", "test_run")
    # Simulate some work
    collector.end_execution()

    snapshot = collector.get_snapshot()
    duration_metric = snapshot.get_metric("total_execution_time_ms")
    assert duration_metric is not None
    assert duration_metric.value > 0


def test_metrics_collector_record_asset_execution() -> None:
    """Test recording asset execution results."""
    collector = MetricsCollector(enabled=True)

    asset_result = AssetResult(
        asset_name="test_asset",
        success=True,
        duration_ms=100.0,
        metrics={"row_count": 1000, "field_count": 5},
    )

    collector.record_asset_execution(asset_result)

    snapshot = collector.get_snapshot()
    assert len(snapshot.metrics) >= 2  # execution time + row_count + field_count

    # Check for execution time metric
    exec_time_metric = snapshot.get_metric("asset_execution_time_ms", {"asset_name": "test_asset"})
    assert exec_time_metric is not None
    assert exec_time_metric.value == 100.0


def test_metrics_collector_record_execution_result() -> None:
    """Test recording complete execution results."""
    collector = MetricsCollector(enabled=True)

    execution_result = ExecutionResult(
        success=True,
        asset_results={},
        errors=(),
        metrics={"total_rows": 10000},
        duration_ms=500.0,
        assets_executed=5,
        assets_succeeded=5,
        assets_failed=0,
    )

    collector.record_execution_result(execution_result)

    snapshot = collector.get_snapshot()
    assert (
        len(snapshot.metrics) >= 4
    )  # assets_executed + assets_succeeded + assets_failed + total_rows


def test_metrics_collector_get_snapshot() -> None:
    """Test getting a metrics snapshot."""
    collector = MetricsCollector(enabled=True)

    collector.record_metric("metric1", 10)
    collector.record_metric("metric2", 20)

    snapshot = collector.get_snapshot()
    assert len(snapshot.metrics) == 2
    assert snapshot.metrics[0].name == "metric1"
    assert snapshot.metrics[1].name == "metric2"


def test_metrics_collector_clear() -> None:
    """Test clearing collected metrics."""
    collector = MetricsCollector(enabled=True)

    collector.record_metric("metric1", 10)
    assert len(collector) == 1

    collector.clear()
    assert len(collector) == 0


def test_metrics_collector_to_dict() -> None:
    """Test converting metrics to dictionary format."""
    collector = MetricsCollector(enabled=True)
    collector.start_execution("test_pipeline", "test_run")
    collector.record_metric("test_metric", 42)
    collector.end_execution()

    metrics_dict = collector.to_dict()

    assert "pipeline_id" in metrics_dict
    assert "run_id" in metrics_dict
    assert "metrics" in metrics_dict
    assert "timestamp" in metrics_dict
    assert len(metrics_dict["metrics"]) == 2  # test_metric + total_execution_time_ms


def test_metrics_collector_thread_safety() -> None:
    """Test that metrics collector is thread-safe."""
    import threading

    collector = MetricsCollector(enabled=True)

    def record_metrics() -> None:
        for i in range(10):
            collector.record_metric(f"thread_metric_{i}", i)

    threads = [threading.Thread(target=record_metrics) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(collector) == 50  # 5 threads * 10 metrics each
