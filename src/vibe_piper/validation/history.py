"""
Validation history storage and analysis.

This module provides comprehensive validation history tracking including:
- Validation run history storage (PostgreSQL)
- Per-asset validation history
- Trend analysis and pattern detection
- Historical baseline comparison
- Search and filtering capabilities

Design:
- Follows ScheduleStore pattern but uses PostgreSQL for persistence
- Provides APIs for storing, querying, and analyzing validation history
- Supports trend analysis and failure pattern detection
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Protocol

# Optional dependencies for analysis
try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import numpy as np
except ImportError:
    np = None

from vibe_piper.types import (
    QualityMetric,
    QualityMetricType,
    ValidationResult,
)

if TYPE_CHECKING:
    from vibe_piper.connectors.postgres import PostgreSQLConnector

logger = logging.getLogger(__name__)


# =============================================================================
# Data Models
# =============================================================================


@dataclass(frozen=True)
class ValidationRunMetadata:
    """
    Metadata for a validation run.

    Attributes:
        validation_run_id: Unique identifier for this validation run
        asset_name: Name of the asset being validated
        suite_name: Name of the validation suite
        pipeline_id: Optional pipeline ID (if validation was part of a pipeline)
        status: Status of the validation run ('passed', 'failed', 'warning')
        started_at: When validation started
        completed_at: When validation completed
        duration_ms: Duration of validation in milliseconds
        total_checks: Total number of checks run
        passed_checks: Number of checks that passed
        failed_checks: Number of checks that failed
        warning_checks: Number of checks with warnings
        total_records: Total number of records validated
        error_count: Total number of errors encountered
        warning_count: Total number of warnings
    """

    validation_run_id: str
    asset_name: str
    suite_name: str
    pipeline_id: str | None = None
    status: str = "passed"  # 'passed', 'failed', 'warning'
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: datetime | None = None
    duration_ms: float = 0.0
    total_checks: int = 0
    passed_checks: int = 0
    failed_checks: int = 0
    warning_checks: int = 0
    total_records: int = 0
    error_count: int = 0
    warning_count: int = 0


@dataclass(frozen=True)
class ValidationCheckRecord:
    """
    Record of a single validation check result.

    Attributes:
        validation_run_id: ID of the parent validation run
        check_name: Name of the validation check
        check_type: Type of check (e.g., 'expect_column_values_to_be_between')
        passed: Whether the check passed
        error_message: Error message if check failed
        warning_messages: List of warning messages
        metrics: Additional metrics from the check
        column_name: Optional column name (for column-level checks)
        duration_ms: Duration of this check in milliseconds
    """

    validation_run_id: str
    check_name: str
    check_type: str
    passed: bool
    error_message: str | None = None
    warning_messages: tuple[str, ...] = field(default_factory=tuple)
    metrics: Mapping[str, Any] = field(default_factory=dict)
    column_name: str | None = None
    duration_ms: float = 0.0


@dataclass(frozen=True)
class ValidationMetric:
    """
    A metric measurement from validation history.

    Attributes:
        metric_name: Name of the metric
        metric_type: Type of quality metric
        asset_name: Asset this metric is for
        check_name: Check this metric is from (if applicable)
        value: The metric value
        status: Whether metric meets threshold ('passed', 'failed', 'warning')
        timestamp: When this metric was recorded
        threshold: Optional threshold value
    """

    metric_name: str
    metric_type: QualityMetricType
    asset_name: str
    value: int | float
    status: str  # 'passed', 'failed', 'warning'
    check_name: str | None = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    threshold: int | float | None = None


@dataclass(frozen=True)
class TrendAnalysisResult:
    """
    Result of trend analysis on validation history.

    Attributes:
        metric_name: Name of the metric being analyzed
        asset_name: Asset being analyzed
        direction: Trend direction ('improving', 'declining', 'stable', 'unknown')
        trend_value: Numerical trend value (e.g., slope of linear regression)
        confidence: Confidence level in trend detection (0-1)
        period_start: Start of analysis period
        period_end: End of analysis period
        data_points: Number of data points analyzed
    """

    metric_name: str
    asset_name: str
    direction: str  # 'improving', 'declining', 'stable', 'unknown'
    trend_value: float
    confidence: float
    period_start: datetime
    period_end: datetime
    data_points: int


@dataclass(frozen=True)
class FailurePattern:
    """
    A detected failure pattern in validation history.

    Attributes:
        pattern_type: Type of failure pattern
        asset_name: Asset where pattern was detected
        check_name: Check that is failing (if applicable)
        frequency: How often this pattern occurs
        last_occurrence: Most recent occurrence
        first_occurrence: First occurrence in the analysis period
        affected_runs: List of validation run IDs affected
        description: Human-readable description of the pattern
    """

    pattern_type: str  # 'recurring_failure', 'flaky_check', 'degradation', 'new_failure'
    asset_name: str
    check_name: str | None = None
    frequency: int = 0
    last_occurrence: datetime | None = None
    first_occurrence: datetime | None = None
    affected_runs: tuple[str, ...] = field(default_factory=tuple)
    description: str = ""


@dataclass(frozen=True)
class BaselineComparisonResult:
    """
    Result of comparing validation metrics against a baseline.

    Attributes:
        metric_name: Name of the metric
        asset_name: Asset being compared
        baseline_value: Baseline value (average of historical runs)
        baseline_period: Period over which baseline was calculated
        current_value: Current metric value
        difference: Difference from baseline (current - baseline)
        percent_change: Percent change from baseline
        status: Whether current value is acceptable ('within_tolerance', 'warning', 'critical')
        tolerance_percent: Acceptable tolerance percentage
    """

    metric_name: str
    asset_name: str
    baseline_value: float
    baseline_period: str  # e.g., "last_30_runs", "last_7_days"
    current_value: float
    difference: float
    percent_change: float
    status: str  # 'within_tolerance', 'warning', 'critical'
    tolerance_percent: float = 10.0


# =============================================================================
# Validation History Store Protocol
# =============================================================================


class ValidationHistoryStore(Protocol):
    """
    Protocol for validation history storage backends.

    This protocol defines the interface for storing and querying
    validation history. Implementations can use different storage
    backends (PostgreSQL, file system, etc.).
    """

    def save_validation_run(self, run_metadata: ValidationRunMetadata) -> None: ...

    def save_check_results(self, check_records: Sequence[ValidationCheckRecord]) -> None: ...

    def save_metrics(self, metrics: Sequence[ValidationMetric]) -> None: ...

    def get_validation_run(self, validation_run_id: str) -> ValidationRunMetadata | None: ...

    def query_validation_runs(
        self,
        asset_name: str | None = None,
        status: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        limit: int = 100,
    ) -> Sequence[ValidationRunMetadata]: ...

    def get_asset_history(
        self,
        asset_name: str,
        limit: int = 100,
    ) -> Sequence[ValidationRunMetadata]: ...

    def get_metrics_history(
        self,
        asset_name: str,
        metric_name: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        limit: int = 1000,
    ) -> Sequence[ValidationMetric]: ...

    def search_validation_runs(
        self,
        query: str,
        limit: int = 100,
    ) -> Sequence[ValidationRunMetadata]: ...

    def delete_old_runs(self, before_date: datetime, asset_name: str | None = None) -> int: ...


# =============================================================================
# PostgreSQL Implementation
# =============================================================================


class PostgreSQLValidationHistoryStore:
    """
    PostgreSQL-based validation history storage.

    This implementation stores validation runs, check results, and metrics
    in PostgreSQL tables. It provides efficient querying and analysis
    capabilities for large-scale validation history.

    Example:
        connector = PostgreSQLConnector(config)
        store = PostgreSQLValidationHistoryStore(connector)
        store.initialize_schema()

        # Save validation results
        store.save_validation_run(run_metadata)
        store.save_check_results(check_records)
        store.save_metrics(metrics)

        # Query history
        history = store.get_asset_history("my_asset")
        trends = store.analyze_trends("my_asset", "quality_score")
    """

    def __init__(self, connector: PostgreSQLConnector) -> None:
        """
        Initialize PostgreSQL validation history store.

        Args:
            connector: PostgreSQL connector instance
        """
        self._connector = connector

    def initialize_schema(self) -> None:
        """
        Create database tables for validation history.

        Creates the following tables:
        - validation_runs: Stores validation run metadata
        - validation_check_results: Stores individual check results
        - validation_metrics: Stores metric measurements
        """
        schema_sql = """
        -- Validation runs table
        CREATE TABLE IF NOT EXISTS validation_runs (
            validation_run_id VARCHAR(255) PRIMARY KEY,
            asset_name VARCHAR(255) NOT NULL,
            suite_name VARCHAR(255) NOT NULL,
            pipeline_id VARCHAR(255),
            status VARCHAR(50) NOT NULL,
            started_at TIMESTAMP WITH TIME ZONE NOT NULL,
            completed_at TIMESTAMP WITH TIME ZONE,
            duration_ms FLOAT DEFAULT 0,
            total_checks INTEGER DEFAULT 0,
            passed_checks INTEGER DEFAULT 0,
            failed_checks INTEGER DEFAULT 0,
            warning_checks INTEGER DEFAULT 0,
            total_records INTEGER DEFAULT 0,
            error_count INTEGER DEFAULT 0,
            warning_count INTEGER DEFAULT 0,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        -- Validation check results table
        CREATE TABLE IF NOT EXISTS validation_check_results (
            id SERIAL PRIMARY KEY,
            validation_run_id VARCHAR(255) NOT NULL REFERENCES validation_runs(validation_run_id) ON DELETE CASCADE,
            check_name VARCHAR(255) NOT NULL,
            check_type VARCHAR(255) NOT NULL,
            passed BOOLEAN NOT NULL,
            error_message TEXT,
            warning_messages JSONB,
            metrics JSONB,
            column_name VARCHAR(255),
            duration_ms FLOAT DEFAULT 0,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        -- Validation metrics table
        CREATE TABLE IF NOT EXISTS validation_metrics (
            id SERIAL PRIMARY KEY,
            metric_name VARCHAR(255) NOT NULL,
            metric_type VARCHAR(100) NOT NULL,
            asset_name VARCHAR(255) NOT NULL,
            check_name VARCHAR(255),
            value FLOAT NOT NULL,
            timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
            status VARCHAR(50) NOT NULL,
            threshold FLOAT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        -- Indexes for efficient querying
        CREATE INDEX IF NOT EXISTS idx_validation_runs_asset ON validation_runs(asset_name);
        CREATE INDEX IF NOT EXISTS idx_validation_runs_status ON validation_runs(status);
        CREATE INDEX IF NOT EXISTS idx_validation_runs_started_at ON validation_runs(started_at DESC);
        CREATE INDEX IF NOT EXISTS idx_check_results_run ON validation_check_results(validation_run_id);
        CREATE INDEX IF NOT EXISTS idx_metrics_asset ON validation_metrics(asset_name);
        CREATE INDEX IF NOT EXISTS idx_metrics_metric ON validation_metrics(metric_name);
        CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON validation_metrics(timestamp DESC);
        """

        self._connector.execute(schema_sql)
        logger.info("Initialized validation history schema in PostgreSQL")

    # =========================================================================
    # Save Operations
    # =========================================================================

    def save_validation_run(self, run_metadata: ValidationRunMetadata) -> None:
        """
        Save validation run metadata.

        Args:
            run_metadata: Validation run metadata to save
        """
        sql = """
        INSERT INTO validation_runs (
            validation_run_id, asset_name, suite_name, pipeline_id, status,
            started_at, completed_at, duration_ms, total_checks, passed_checks,
            failed_checks, warning_checks, total_records, error_count, warning_count
        ) VALUES (%(validation_run_id)s, %(asset_name)s, %(suite_name)s, %(pipeline_id)s, %(status)s,
        %(started_at)s, %(completed_at)s, %(duration_ms)s, %(total_checks)s, %(passed_checks)s,
        %(failed_checks)s, %(warning_checks)s, %(total_records)s, %(error_count)s, %(warning_count)s)
        ON CONFLICT (validation_run_id) DO UPDATE SET
            completed_at = EXCLUDED.completed_at,
            duration_ms = EXCLUDED.duration_ms,
            total_checks = EXCLUDED.total_checks,
            passed_checks = EXCLUDED.passed_checks,
            failed_checks = EXCLUDED.failed_checks,
            warning_checks = EXCLUDED.warning_checks,
            error_count = EXCLUDED.error_count,
            warning_count = EXCLUDED.warning_count
        """

        params = {
            "validation_run_id": run_metadata.validation_run_id,
            "asset_name": run_metadata.asset_name,
            "suite_name": run_metadata.suite_name,
            "pipeline_id": run_metadata.pipeline_id,
            "status": run_metadata.status,
            "started_at": run_metadata.started_at,
            "completed_at": run_metadata.completed_at,
            "duration_ms": run_metadata.duration_ms,
            "total_checks": run_metadata.total_checks,
            "passed_checks": run_metadata.passed_checks,
            "failed_checks": run_metadata.failed_checks,
            "warning_checks": run_metadata.warning_checks,
            "total_records": run_metadata.total_records,
            "error_count": run_metadata.error_count,
            "warning_count": run_metadata.warning_count,
        }

        self._connector.execute(sql, params)
        logger.debug(f"Saved validation run {run_metadata.validation_run_id}")

    def save_check_results(self, check_records: Sequence[ValidationCheckRecord]) -> None:
        """
        Save validation check results.

        Args:
            check_records: Check result records to save
        """
        sql = """
        INSERT INTO validation_check_results (
            validation_run_id, check_name, check_type, passed, error_message,
            warning_messages, metrics, column_name, duration_ms
        ) VALUES (%(validation_run_id)s, %(check_name)s, %(check_type)s, %(passed)s, %(error_message)s,
        %(warning_messages)s, %(metrics)s, %(column_name)s, %(duration_ms)s)
        """

        for record in check_records:
            import json

            params = {
                "validation_run_id": record.validation_run_id,
                "check_name": record.check_name,
                "check_type": record.check_type,
                "passed": record.passed,
                "error_message": record.error_message,
                "warning_messages": json.dumps(record.warning_messages),
                "metrics": json.dumps(record.metrics),
                "column_name": record.column_name,
                "duration_ms": record.duration_ms,
            }
            self._connector.execute(sql, params)

        logger.debug(f"Saved {len(check_records)} check results")

    def save_metrics(self, metrics: Sequence[ValidationMetric]) -> None:
        """
        Save validation metrics.

        Args:
            metrics: Metric records to save
        """
        sql = """
        INSERT INTO validation_metrics (
            metric_name, metric_type, asset_name, check_name, value,
            timestamp, status, threshold
        ) VALUES (%(metric_name)s, %(metric_type)s, %(asset_name)s, %(check_name)s, %(value)s,
        %(timestamp)s, %(status)s, %(threshold)s)
        """

        for metric in metrics:
            params = {
                "metric_name": metric.metric_name,
                "metric_type": metric.metric_type.value,
                "asset_name": metric.asset_name,
                "check_name": metric.check_name,
                "value": metric.value,
                "timestamp": metric.timestamp,
                "status": metric.status,
                "threshold": metric.threshold,
            }
            self._connector.execute(sql, params)

        logger.debug(f"Saved {len(metrics)} metrics")

    # =========================================================================
    # Query Operations
    # =========================================================================

    def get_validation_run(self, validation_run_id: str) -> ValidationRunMetadata | None:
        """
        Get a specific validation run by ID.

        Args:
            validation_run_id: ID of the validation run

        Returns:
            ValidationRunMetadata if found, None otherwise
        """
        sql = """
        SELECT
            validation_run_id, asset_name, suite_name, pipeline_id, status,
            started_at, completed_at, duration_ms, total_checks, passed_checks,
            failed_checks, warning_checks, total_records, error_count, warning_count
        FROM validation_runs
        WHERE validation_run_id = %(validation_run_id)s
        """

        result = self._connector.execute_query(sql, {"validation_run_id": validation_run_id})

        if result.row_count == 0:
            return None

        row = result.rows[0]
        return ValidationRunMetadata(**row)

    def query_validation_runs(
        self,
        asset_name: str | None = None,
        status: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        limit: int = 100,
    ) -> Sequence[ValidationRunMetadata]:
        """
        Query validation runs with filters.

        Args:
            asset_name: Filter by asset name
            status: Filter by status
            start_date: Filter to runs after this date
            end_date: Filter to runs before this date
            limit: Maximum number of results

        Returns:
            Sequence of ValidationRunMetadata
        """
        conditions: list[str] = []
        params: dict[str, Any] = {}

        if asset_name:
            conditions.append("asset_name = %(asset_name)s")
            params["asset_name"] = asset_name

        if status:
            conditions.append("status = %(status)s")
            params["status"] = status

        if start_date:
            conditions.append("started_at >= %(start_date)s")
            params["start_date"] = start_date

        if end_date:
            conditions.append("started_at <= %(end_date)s")
            params["end_date"] = end_date

        where_clause = " AND ".join(conditions) if conditions else "TRUE"
        params["limit"] = limit

        sql = f"""
        SELECT
            validation_run_id, asset_name, suite_name, pipeline_id, status,
            started_at, completed_at, duration_ms, total_checks, passed_checks,
            failed_checks, warning_checks, total_records, error_count, warning_count
        FROM validation_runs
        WHERE {where_clause}
        ORDER BY started_at DESC
        LIMIT %(limit)s
        """

        result = self._connector.execute_query(sql, params)
        return [ValidationRunMetadata(**row) for row in result.rows]

    def get_asset_history(
        self, asset_name: str, limit: int = 100
    ) -> Sequence[ValidationRunMetadata]:
        """
        Get validation history for a specific asset.

        Args:
            asset_name: Name of the asset
            limit: Maximum number of runs to return

        Returns:
            Sequence of validation runs for the asset
        """
        return self.query_validation_runs(asset_name=asset_name, limit=limit)

    def get_metrics_history(
        self,
        asset_name: str,
        metric_name: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        limit: int = 1000,
    ) -> Sequence[ValidationMetric]:
        """
        Get metrics history for an asset.

        Args:
            asset_name: Name of the asset
            metric_name: Optional filter by metric name
            start_date: Filter to metrics after this date
            end_date: Filter to metrics before this date
            limit: Maximum number of results

        Returns:
            Sequence of ValidationMetric
        """
        conditions: list[str] = ["asset_name = %(asset_name)s"]
        params: dict[str, Any] = {"asset_name": asset_name}

        if metric_name:
            conditions.append("metric_name = %(metric_name)s")
            params["metric_name"] = metric_name

        if start_date:
            conditions.append("timestamp >= %(start_date)s")
            params["start_date"] = start_date

        if end_date:
            conditions.append("timestamp <= %(end_date)s")
            params["end_date"] = end_date

        where_clause = " AND ".join(conditions)
        params["limit"] = limit

        sql = f"""
        SELECT
            metric_name, metric_type, asset_name, check_name, value,
            timestamp, status, threshold
        FROM validation_metrics
        WHERE {where_clause}
        ORDER BY timestamp DESC
        LIMIT %(limit)s
        """

        result = self._connector.execute_query(sql, params)
        return [
            ValidationMetric(
                metric_name=row["metric_name"],
                metric_type=QualityMetricType(row["metric_type"]),
                asset_name=row["asset_name"],
                check_name=row["check_name"],
                value=row["value"],
                timestamp=row["timestamp"],
                status=row["status"],
                threshold=row["threshold"],
            )
            for row in result.rows
        ]

    def search_validation_runs(
        self, query: str, limit: int = 100
    ) -> Sequence[ValidationRunMetadata]:
        """
        Search validation runs by text.

        Args:
            query: Search query (matches asset_name or suite_name)
            limit: Maximum number of results

        Returns:
            Sequence of matching validation runs
        """
        sql = """
        SELECT
            validation_run_id, asset_name, suite_name, pipeline_id, status,
            started_at, completed_at, duration_ms, total_checks, passed_checks,
            failed_checks, warning_checks, total_records, error_count, warning_count
        FROM validation_runs
        WHERE asset_name ILIKE %(query)s OR suite_name ILIKE %(query)s
        ORDER BY started_at DESC
        LIMIT %(limit)s
        """

        search_pattern = f"%{query}%"
        result = self._connector.execute_query(sql, {"query": search_pattern, "limit": limit})
        return [ValidationRunMetadata(**row) for row in result.rows]

    def delete_old_runs(self, before_date: datetime, asset_name: str | None = None) -> int:
        """
        Delete old validation runs.

        Args:
            before_date: Delete runs before this date
            asset_name: Optional filter by asset name

        Returns:
            Number of runs deleted
        """
        conditions: list[str] = ["started_at < %(before_date)s"]
        params: dict[str, Any] = {"before_date": before_date}

        if asset_name:
            conditions.append("asset_name = %(asset_name)s")
            params["asset_name"] = asset_name

        where_clause = " AND ".join(conditions)

        sql = f"""
        DELETE FROM validation_runs
        WHERE {where_clause}
        """

        self._connector.execute(sql, params)

        # Get count of deleted (query before delete)
        count_sql = f"""
        SELECT COUNT(*) as count FROM validation_runs
        WHERE {where_clause}
        """

        result = self._connector.execute_query(count_sql, params)
        count = result.rows[0]["count"] if result.row_count > 0 else 0

        logger.info(f"Deleted {count} old validation runs")
        return count


# =============================================================================
# Analysis Functions
# =============================================================================


class ValidationHistoryAnalyzer:
    """
    Analyzes validation history for trends, patterns, and comparisons.

    This class provides analysis capabilities on top of the
    validation history store, including trend analysis,
    failure pattern detection, and baseline comparisons.

    Example:
        store = PostgreSQLValidationHistoryStore(connector)
        analyzer = ValidationHistoryAnalyzer(store)

        # Analyze trends
        trend = analyzer.analyze_trends("my_asset", "quality_score")

        # Detect failure patterns
        patterns = analyzer.detect_failure_patterns("my_asset")

        # Compare against baseline
        comparison = analyzer.compare_with_baseline("my_asset", "quality_score")
    """

    def __init__(self, store: ValidationHistoryStore) -> None:
        """
        Initialize validation history analyzer.

        Args:
            store: Validation history store instance
        """
        self._store = store

    def analyze_trends(
        self,
        asset_name: str,
        metric_name: str,
        period_days: int = 30,
        min_data_points: int = 5,
    ) -> TrendAnalysisResult | None:
        """
        Analyze trends in validation metrics.

        Args:
            asset_name: Asset to analyze
            metric_name: Metric to analyze
            period_days: Analysis period in days
            min_data_points: Minimum data points required for analysis

        Returns:
            TrendAnalysisResult if enough data, None otherwise
        """
        if pd is None or np is None:
            logger.warning("pandas or numpy not available for trend analysis")
            return None

        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=period_days)

        # Get metrics history
        metrics = self._store.get_metrics_history(
            asset_name=asset_name,
            metric_name=metric_name,
            start_date=start_date,
            end_date=end_date,
            limit=1000,
        )

        if len(metrics) < min_data_points:
            logger.debug(
                f"Not enough data points for trend analysis: {len(metrics)} < {min_data_points}"
            )
            return None

        # Convert to pandas DataFrame for analysis
        df = pd.DataFrame(
            [
                {
                    "timestamp": m.timestamp,
                    "value": float(m.value),
                }
                for m in metrics
            ]
        )

        df = df.sort_values("timestamp").reset_index(drop=True)

        # Calculate trend using linear regression
        x = np.arange(len(df))
        y = df["value"].values

        # Fit linear regression
        slope, intercept = np.polyfit(x, y, 1)

        # Determine direction
        if abs(slope) < 0.01:  # Threshold for "stable"
            direction = "stable"
        elif slope > 0:
            direction = "improving"
        else:
            direction = "declining"

        # Calculate R-squared for confidence
        y_pred = slope * x + intercept
        ss_res = ((y - y_pred) ** 2).sum()
        ss_tot = ((y - y.mean()) ** 2).sum()
        r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0
        confidence = float(max(0, min(1, r_squared)))

        return TrendAnalysisResult(
            metric_name=metric_name,
            asset_name=asset_name,
            direction=direction,
            trend_value=float(slope),
            confidence=confidence,
            period_start=start_date,
            period_end=end_date,
            data_points=len(metrics),
        )

    def detect_failure_patterns(
        self,
        asset_name: str,
        period_days: int = 30,
        min_occurrences: int = 3,
    ) -> Sequence[FailurePattern]:
        """
        Detect recurring failure patterns in validation history.

        Args:
            asset_name: Asset to analyze
            period_days: Analysis period in days
            min_occurrences: Minimum occurrences to consider a pattern

        Returns:
            Sequence of detected FailurePattern objects
        """
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=period_days)

        # Get validation runs
        runs = self._store.query_validation_runs(
            asset_name=asset_name,
            start_date=start_date,
            end_date=end_date,
            limit=1000,
        )

        patterns: list[FailurePattern] = []

        # Get check results for failed runs
        check_failure_counts: dict[str, list[str]] = {}
        failed_run_ids = [r.validation_run_id for r in runs if r.status == "failed"]

        for run_id in failed_run_ids:
            sql = """
            SELECT
                check_name, check_type, passed
            FROM validation_check_results
            WHERE validation_run_id = %(run_id)s AND passed = FALSE
            """

            result = self._store._connector.execute_query(sql, {"run_id": run_id})

            for row in result.rows:
                check_key = f"{row['check_name']}:{row['check_type']}"
                if check_key not in check_failure_counts:
                    check_failure_counts[check_key] = []
                check_failure_counts[check_key].append(run_id)

        # Detect patterns
        for check_key, run_ids in check_failure_counts.items():
            if len(run_ids) >= min_occurrences:
                check_name = check_key.split(":")[0]

                # Get timestamps
                run_timestamps = {
                    r.validation_run_id: r.started_at
                    for r in runs
                    if r.validation_run_id in run_ids
                }

                timestamps = [run_timestamps[rid] for rid in run_ids if rid in run_timestamps]

                patterns.append(
                    FailurePattern(
                        pattern_type="recurring_failure",
                        asset_name=asset_name,
                        check_name=check_name,
                        frequency=len(run_ids),
                        first_occurrence=min(timestamps) if timestamps else None,
                        last_occurrence=max(timestamps) if timestamps else None,
                        affected_runs=tuple(run_ids),
                        description=f"Check '{check_name}' has failed {len(run_ids)} times in the last {period_days} days",
                    )
                )

        return patterns

    def compare_with_baseline(
        self,
        asset_name: str,
        metric_name: str,
        baseline_period: str = "last_30_runs",
        tolerance_percent: float = 10.0,
    ) -> BaselineComparisonResult | None:
        """
        Compare current metrics against historical baseline.

        Args:
            asset_name: Asset to compare
            metric_name: Metric to compare
            baseline_period: Period for baseline calculation
            tolerance_percent: Acceptable tolerance percentage

        Returns:
            BaselineComparisonResult if enough data, None otherwise
        """
        # Parse baseline period
        if baseline_period.startswith("last_"):
            num_runs = int(baseline_period.replace("last_", "").replace("_runs", ""))
        else:
            num_runs = 30

        # Get metrics history
        metrics = self._store.get_metrics_history(
            asset_name=asset_name,
            metric_name=metric_name,
            limit=num_runs + 1,  # Get one extra for current value
        )

        if len(metrics) < num_runs + 1:
            logger.debug(
                f"Not enough data points for baseline comparison: {len(metrics)} < {num_runs + 1}"
            )
            return None

        # Calculate baseline from historical data (excluding most recent)
        historical_metrics = metrics[1 : num_runs + 1]  # Skip first (most recent)
        baseline_value = sum(m.value for m in historical_metrics) / len(historical_metrics)

        # Get current value
        current_metric = metrics[0]
        current_value = float(current_metric.value)

        # Calculate difference
        difference = current_value - baseline_value
        percent_change = (difference / baseline_value * 100) if baseline_value != 0 else 0

        # Determine status
        abs_change = abs(percent_change)
        if abs_change <= tolerance_percent:
            status = "within_tolerance"
        elif abs_change <= tolerance_percent * 2:
            status = "warning"
        else:
            status = "critical"

        return BaselineComparisonResult(
            metric_name=metric_name,
            asset_name=asset_name,
            baseline_value=baseline_value,
            baseline_period=baseline_period,
            current_value=current_value,
            difference=difference,
            percent_change=percent_change,
            status=status,
            tolerance_percent=tolerance_percent,
        )

    def get_summary_statistics(
        self,
        asset_name: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, Any]:
        """
        Get summary statistics for validation history.

        Args:
            asset_name: Asset to summarize
            start_date: Optional start date for summary
            end_date: Optional end date for summary

        Returns:
            Dictionary with summary statistics
        """
        runs = self._store.query_validation_runs(
            asset_name=asset_name,
            start_date=start_date,
            end_date=end_date,
            limit=10000,
        )

        if not runs:
            return {
                "total_runs": 0,
                "passed_runs": 0,
                "failed_runs": 0,
                "warning_runs": 0,
                "pass_rate": 0.0,
            }

        total_runs = len(runs)
        passed_runs = sum(1 for r in runs if r.status == "passed")
        failed_runs = sum(1 for r in runs if r.status == "failed")
        warning_runs = sum(1 for r in runs if r.status == "warning")

        avg_duration = sum(r.duration_ms for r in runs) / total_runs
        avg_checks = sum(r.total_checks for r in runs) / total_runs

        return {
            "total_runs": total_runs,
            "passed_runs": passed_runs,
            "failed_runs": failed_runs,
            "warning_runs": warning_runs,
            "pass_rate": passed_runs / total_runs if total_runs > 0 else 0.0,
            "avg_duration_ms": avg_duration,
            "avg_checks_per_run": avg_checks,
            "total_records_validated": sum(r.total_records for r in runs),
        }


# =============================================================================
# Re-exports
# =============================================================================

__all__ = [
    # Data models
    "ValidationRunMetadata",
    "ValidationCheckRecord",
    "ValidationMetric",
    "TrendAnalysisResult",
    "FailurePattern",
    "BaselineComparisonResult",
    # Storage
    "ValidationHistoryStore",
    "PostgreSQLValidationHistoryStore",
    # Analysis
    "ValidationHistoryAnalyzer",
]
