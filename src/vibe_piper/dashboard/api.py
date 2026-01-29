"""FastAPI application for quality dashboard.

Provides REST API endpoints for accessing quality metrics,
historical data, anomaly detection, and aggregations.
"""

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

# Optional FastAPI dependency
FASTAPI_AVAILABLE = False

try:
    from fastapi import (  # type: ignore[import-untyped]
        FastAPI,
        HTTPException,
        WebSocket,
        WebSocketDisconnect,
    )
    from fastapi.middleware.cors import CORSMiddleware  # type: ignore[import-untyped]
    from fastapi.responses import JSONResponse  # type: ignore[import-untyped]

    FASTAPI_AVAILABLE = True
except ImportError:  # pragma: no cover
    pass

try:
    import uvicorn  # type: ignore[import-untyped]
except ImportError:  # pragma: no cover
    uvicorn = None  # type: ignore[assignment]

from vibe_piper.dashboard.models import (
    Anomaly,
    AnomalySeverity,
    QualityAggregation,
    QualityAlert,
    QualityHistory,
    QualitySnapshot,
)

# =============================================================================
# Logger
# =============================================================================

logger = logging.getLogger(__name__)

# =============================================================================
# In-Memory Storage (Production: use PostgreSQL)
# =============================================================================


class QualityStore:
    """
    In-memory store for quality data.

    Production implementation should use PostgreSQL with proper indexing.
    """

    def __init__(self) -> None:
        """Initialize the store."""
        self.snapshots: dict[str, list[QualitySnapshot]] = {}
        self.alerts: dict[str, QualityAlert] = {}
        self.anomalies: dict[str, Anomaly] = {}
        self.history: dict[str, QualityHistory] = {}
        self._lock: Any = None  # threading.Lock() would go here

    def add_snapshot(self, snapshot: QualitySnapshot) -> None:
        """
        Add a quality snapshot.

        Args:
            snapshot: Snapshot to add
        """
        key = snapshot.asset_name
        if key not in self.snapshots:
            self.snapshots[key] = []
        self.snapshots[key].append(snapshot)

        # Update history
        if key not in self.history:
            self.history[key] = QualityHistory(asset_name=key)
        self.history[key].add_snapshot(snapshot)

        logger.debug(f"Added snapshot for {key}: score={snapshot.overall_score:.3f}")

    def get_snapshots(
        self,
        asset_name: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int = 100,
    ) -> list[QualitySnapshot]:
        """
        Get quality snapshots.

        Args:
            asset_name: Optional asset name filter
            start_time: Optional start time filter
            end_time: Optional end time filter
            limit: Maximum number of snapshots to return

        Returns:
            List of matching snapshots
        """
        snapshots: list[QualitySnapshot] = []

        if asset_name:
            snapshots.extend(self.snapshots.get(asset_name, []))
        else:
            for snap_list in self.snapshots.values():
                snapshots.extend(snap_list)

        # Filter by time range
        if start_time:
            snapshots = [s for s in snapshots if s.timestamp >= start_time]
        if end_time:
            snapshots = [s for s in snapshots if s.timestamp <= end_time]

        # Sort by timestamp (most recent first) and limit
        snapshots.sort(key=lambda s: s.timestamp, reverse=True)
        return snapshots[:limit]

    def get_history(self, asset_name: str) -> QualityHistory | None:
        """
        Get historical quality data for an asset.

        Args:
            asset_name: Name of asset

        Returns:
            QualityHistory or None if not found
        """
        return self.history.get(asset_name)

    def add_alert(self, alert: QualityAlert) -> None:
        """
        Add a quality alert.

        Args:
            alert: Alert to add
        """
        self.alerts[alert.id] = alert
        logger.warning(f"Alert created: {alert.title} for {alert.asset_name}")

    def get_alerts(
        self,
        asset_name: str | None = None,
        status: str | None = None,
        severity: AnomalySeverity | None = None,
        limit: int = 50,
    ) -> list[QualityAlert]:
        """
        Get quality alerts.

        Args:
            asset_name: Optional asset name filter
            status: Optional status filter
            severity: Optional severity filter
            limit: Maximum number of alerts to return

        Returns:
            List of matching alerts
        """
        alerts = list(self.alerts.values())

        if asset_name:
            alerts = [a for a in alerts if a.asset_name == asset_name]
        if status:
            alerts = [a for a in alerts if a.status.value.lower() == status.lower()]
        if severity:
            alerts = [a for a in alerts if a.severity == severity]

        # Sort by creation time (most recent first) and limit
        alerts.sort(key=lambda a: a.created_at, reverse=True)
        return alerts[:limit]

    def add_anomaly(self, anomaly: Anomaly) -> None:
        """
        Add a detected anomaly.

        Args:
            anomaly: Anomaly to add
        """
        self.anomalies[anomaly.id] = anomaly
        logger.warning(f"Anomaly detected: {anomaly.anomaly_type} for {anomaly.asset_name}")

    def get_anomalies(
        self,
        asset_name: str | None = None,
        severity: AnomalySeverity | None = None,
        start_time: datetime | None = None,
        limit: int = 50,
    ) -> list[Anomaly]:
        """
        Get detected anomalies.

        Args:
            asset_name: Optional asset name filter
            severity: Optional severity filter
            start_time: Optional start time filter
            limit: Maximum number of anomalies to return

        Returns:
            List of matching anomalies
        """
        anomalies = list(self.anomalies.values())

        if asset_name:
            anomalies = [a for a in anomalies if a.asset_name == asset_name]
        if severity:
            anomalies = [a for a in anomalies if a.severity == severity]
        if start_time:
            anomalies = [a for a in anomalies if a.timestamp >= start_time]

        # Sort by timestamp (most recent first) and limit
        anomalies.sort(key=lambda a: a.timestamp, reverse=True)
        return anomalies[:limit]

    def get_aggregations(
        self,
        asset_name: str | None = None,
        period: str = "day",
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> list[QualityAggregation]:
        """
        Get aggregated quality metrics.

        Args:
            asset_name: Optional asset name filter
            period: Aggregation period (day, week, month)
            start_time: Optional start time
            end_time: Optional end time

        Returns:
            List of aggregations
        """
        if not start_time:
            start_time = datetime.now() - timedelta(days=30)
        if not end_time:
            end_time = datetime.now()

        # Get all snapshots for the period
        snapshots = self.get_snapshots(asset_name, start_time, end_time, limit=10000)

        # Group by period (simplified - production should use proper grouping)
        aggregations: list[QualityAggregation] = []

        if period == "day":
            # Group by day
            days = {}
            for snap in snapshots:
                day_key = snap.timestamp.date().isoformat()
                if day_key not in days:
                    days[day_key] = []
                days[day_key].append(snap)

            for day_key, day_snaps in days.items():
                if not day_snaps:
                    continue
                avg_score = sum(s.overall_score for s in day_snaps) / len(day_snaps)
                avg_completeness = sum(s.completeness_score for s in day_snaps) / len(day_snaps)
                avg_validity = sum(s.validity_score for s in day_snaps) / len(day_snaps)
                min_score = min(s.overall_score for s in day_snaps)
                max_score = max(s.overall_score for s in day_snaps)

                aggregations.append(
                    QualityAggregation(
                        period=period,
                        start_time=min(s.timestamp for s in day_snaps),
                        end_time=max(s.timestamp for s in day_snaps),
                        asset_name=asset_name,
                        avg_completeness=round(avg_completeness, 4),
                        avg_validity=round(avg_validity, 4),
                        avg_overall=round(avg_score, 4),
                        min_overall=round(min_score, 4),
                        max_overall=round(max_score, 4),
                        total_snapshots=len(day_snaps),
                        total_records=sum(s.total_records for s in day_snaps),
                    )
                )

        # Sort by start time
        aggregations.sort(key=lambda a: a.start_time)
        return aggregations

    def get_assets(self) -> list[str]:
        """Get list of all assets with quality data."""
        return list(self.snapshots.keys())


# Global store instance
_store = QualityStore()

# =============================================================================
# FastAPI Application
# =============================================================================


@asynccontextmanager
async def lifespan(app: FastAPI) -> None:  # type: ignore[misc]
    """Manage application lifecycle."""
    logger.info("Starting quality dashboard API")
    yield
    logger.info("Shutting down quality dashboard API")


def create_app() -> FastAPI:  # type: ignore[misc]
    """
    Create and configure the FastAPI application.

    Returns:
        Configured FastAPI application
    """
    if not FASTAPI_AVAILABLE:  # pragma: no cover
        msg = "FastAPI is not installed. Install with: uv pip install 'vibe-piper[dashboard]'"
        raise ImportError(msg)

    app = FastAPI(
        title="Vibe Piper Quality Dashboard",
        description="Data quality monitoring and alerting API",
        version="0.1.0",
        lifespan=lifespan,
    )

    # Configure CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Store in app state
    app.state.store = _store

    # Register routes
    register_routes(app)

    return app


def register_routes(app: FastAPI) -> None:  # type: ignore[misc]
    """Register all API routes."""
    app.add_api_route("/", read_root, methods=["GET"])
    app.add_api_route("/api/health", health_check, methods=["GET"])
    app.add_api_route("/api/assets", get_assets, methods=["GET"])
    app.add_api_route("/api/quality/snapshots", get_snapshots, methods=["GET"])
    app.add_api_route("/api/quality/history/{asset_name}", get_history, methods=["GET"])
    app.add_api_route("/api/quality/aggregations", get_aggregations, methods=["GET"])
    app.add_api_route("/api/alerts", get_alerts, methods=["GET"])
    app.add_api_route("/api/alerts/{alert_id}", get_alert, methods=["GET"])
    app.add_api_route("/api/alerts/{alert_id}/acknowledge", acknowledge_alert, methods=["POST"])
    app.add_api_route("/api/alerts/{alert_id}/resolve", resolve_alert, methods=["POST"])
    app.add_api_route("/api/anomalies", get_anomalies, methods=["GET"])
    app.add_api_route("/api/anomalies/{anomaly_id}", get_anomaly, methods=["GET"])
    app.add_api_route("/api/export/{format}", export_data, methods=["GET"])
    app.add_api_route("/ws/quality", websocket_quality, methods=["GET"])


# =============================================================================
# Route Handlers
# =============================================================================


async def read_root() -> dict[str, str]:
    """Root endpoint."""
    return {"message": "Vibe Piper Quality Dashboard API", "version": "0.1.0"}


async def health_check() -> dict[str, Any]:
    """Health check endpoint."""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


async def get_assets(asset_name: str | None = None) -> JSONResponse:  # type: ignore[misc]
    """
    Get list of assets with quality data.

    Args:
        asset_name: Optional asset name filter

    Returns:
        JSON response with assets list
    """
    store: QualityStore = _store  # type: ignore[assignment]

    if asset_name:
        assets = [asset_name] if asset_name in store.get_assets() else []
    else:
        assets = store.get_assets()

    return JSONResponse({"assets": assets, "count": len(assets)})


async def get_snapshots(
    asset_name: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    limit: int = 100,
) -> JSONResponse:  # type: ignore[misc]
    """
    Get quality snapshots.

    Args:
        asset_name: Optional asset name filter
        start_time: Optional start time (ISO format)
        end_time: Optional end time (ISO format)
        limit: Maximum number of snapshots

    Returns:
        JSON response with snapshots
    """
    store: QualityStore = _store  # type: ignore[assignment]

    start_dt = datetime.fromisoformat(start_time) if start_time else None
    end_dt = datetime.fromisoformat(end_time) if end_time else None

    snapshots = store.get_snapshots(asset_name, start_dt, end_dt, limit)

    # Convert to dict for JSON serialization
    snapshots_data = [
        {
            "asset_name": s.asset_name,
            "timestamp": s.timestamp.isoformat(),
            "total_records": s.total_records,
            "valid_records": s.valid_records,
            "invalid_records": s.invalid_records,
            "completeness_score": s.completeness_score,
            "validity_score": s.validity_score,
            "overall_score": s.overall_score,
            "pipeline_id": s.pipeline_id,
            "run_id": s.run_id,
            "metrics_count": len(s.metrics),
        }
        for s in snapshots
    ]

    return JSONResponse({"snapshots": snapshots_data, "count": len(snapshots_data)})


async def get_history(asset_name: str) -> JSONResponse:  # type: ignore[misc]
    """
    Get historical quality data for an asset.

    Args:
        asset_name: Name of asset

    Returns:
        JSON response with historical data
    """
    store: QualityStore = _store  # type: ignore[assignment]

    history = store.get_history(asset_name)

    if not history:
        raise HTTPException(status_code=404, detail=f"No history found for asset {asset_name}")

    return JSONResponse(
        {
            "asset_name": asset_name,
            "total_snapshots": len(history.snapshots),
            "average_score": history.average_score,
            "latest_score": history.latest_snapshot.overall_score
            if history.latest_snapshot
            else 0.0,
        }
    )


async def get_aggregations(
    asset_name: str | None = None,
    period: str = "day",
    start_time: str | None = None,
    end_time: str | None = None,
) -> JSONResponse:  # type: ignore[misc]
    """
    Get aggregated quality metrics.

    Args:
        asset_name: Optional asset name filter
        period: Aggregation period (day, week, month)
        start_time: Optional start time (ISO format)
        end_time: Optional end time (ISO format)

    Returns:
        JSON response with aggregations
    """
    store: QualityStore = _store  # type: ignore[assignment]

    start_dt = datetime.fromisoformat(start_time) if start_time else None
    end_dt = datetime.fromisoformat(end_time) if end_time else None

    aggregations = store.get_aggregations(asset_name, period, start_dt, end_dt)

    aggregations_data = [
        {
            "period": a.period,
            "start_time": a.start_time.isoformat(),
            "end_time": a.end_time.isoformat(),
            "asset_name": a.asset_name,
            "avg_completeness": a.avg_completeness,
            "avg_validity": a.avg_validity,
            "avg_overall": a.avg_overall,
            "min_overall": a.min_overall,
            "max_overall": a.max_overall,
            "total_snapshots": a.total_snapshots,
            "total_records": a.total_records,
        }
        for a in aggregations
    ]

    return JSONResponse({"aggregations": aggregations_data, "count": len(aggregations_data)})


async def get_alerts(
    asset_name: str | None = None,
    status: str | None = None,
    severity: AnomalySeverity | None = None,
    limit: int = 50,
) -> JSONResponse:  # type: ignore[misc]
    """
    Get quality alerts.

    Args:
        asset_name: Optional asset name filter
        status: Optional status filter
        severity: Optional severity filter
        limit: Maximum number of alerts

    Returns:
        JSON response with alerts
    """
    store: QualityStore = _store  # type: ignore[assignment]

    alerts = store.get_alerts(asset_name, status, severity, limit)

    alerts_data = [
        {
            "id": a.id,
            "asset_name": a.asset_name,
            "alert_type": a.alert_type,
            "status": a.status.value,
            "severity": a.severity.value,
            "title": a.title,
            "message": a.message,
            "created_at": a.created_at.isoformat(),
            "acknowledged_at": a.acknowledged_at.isoformat() if a.acknowledged_at else None,
            "resolved_at": a.resolved_at.isoformat() if a.resolved_at else None,
        }
        for a in alerts
    ]

    return JSONResponse({"alerts": alerts_data, "count": len(alerts_data)})


async def get_alert(alert_id: str) -> JSONResponse:  # type: ignore[misc]
    """
    Get a specific alert.

    Args:
        alert_id: Alert ID

    Returns:
        JSON response with alert details
    """
    store: QualityStore = _store  # type: ignore[assignment]

    alert = store.alerts.get(alert_id)

    if not alert:
        raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")

    return JSONResponse(
        {
            "id": alert.id,
            "asset_name": alert.asset_name,
            "alert_type": alert.alert_type,
            "status": alert.status.value,
            "severity": alert.severity.value,
            "title": alert.title,
            "message": alert.message,
            "created_at": alert.created_at.isoformat(),
            "acknowledged_at": alert.acknowledged_at.isoformat() if alert.acknowledged_at else None,
            "resolved_at": alert.resolved_at.isoformat() if alert.resolved_at else None,
            "threshold_config": alert.threshold_config,
            "metadata": alert.metadata,
        }
    )


async def acknowledge_alert(alert_id: str) -> JSONResponse:  # type: ignore[misc]
    """
    Acknowledge an alert.

    Args:
        alert_id: Alert ID

    Returns:
        JSON response
    """
    store: QualityStore = _store  # type: ignore[assignment]

    alert = store.alerts.get(alert_id)

    if not alert:
        raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")

    alert.acknowledge()

    return JSONResponse(
        {
            "id": alert.id,
            "status": alert.status.value,
            "acknowledged_at": alert.acknowledged_at.isoformat(),
        }
    )


async def resolve_alert(alert_id: str) -> JSONResponse:  # type: ignore[misc]
    """
    Resolve an alert.

    Args:
        alert_id: Alert ID

    Returns:
        JSON response
    """
    store: QualityStore = _store  # type: ignore[assignment]

    alert = store.alerts.get(alert_id)

    if not alert:
        raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")

    alert.resolve()

    return JSONResponse(
        {"id": alert.id, "status": alert.status.value, "resolved_at": alert.resolved_at.isoformat()}
    )


async def get_anomalies(
    asset_name: str | None = None,
    severity: AnomalySeverity | None = None,
    start_time: str | None = None,
    limit: int = 50,
) -> JSONResponse:  # type: ignore[misc]
    """
    Get detected anomalies.

    Args:
        asset_name: Optional asset name filter
        severity: Optional severity filter
        start_time: Optional start time (ISO format)
        limit: Maximum number of anomalies

    Returns:
        JSON response with anomalies
    """
    store: QualityStore = _store  # type: ignore[assignment]

    start_dt = datetime.fromisoformat(start_time) if start_time else None

    anomalies = store.get_anomalies(asset_name, severity, start_dt, limit)

    anomalies_data = [
        {
            "id": a.id,
            "asset_name": a.asset_name,
            "timestamp": a.timestamp.isoformat(),
            "severity": a.severity.value,
            "anomaly_type": a.anomaly_type,
            "description": a.description,
            "expected_value": a.expected_value,
            "actual_value": a.actual_value,
            "deviation_percentage": a.deviation_percentage,
            "affected_metrics": list(a.affected_metrics),
        }
        for a in anomalies
    ]

    return JSONResponse({"anomalies": anomalies_data, "count": len(anomalies_data)})


async def get_anomaly(anomaly_id: str) -> JSONResponse:  # type: ignore[misc]
    """
    Get a specific anomaly.

    Args:
        anomaly_id: Anomaly ID

    Returns:
        JSON response with anomaly details
    """
    store: QualityStore = _store  # type: ignore[assignment]

    anomaly = store.anomalies.get(anomaly_id)

    if not anomaly:
        raise HTTPException(status_code=404, detail=f"Anomaly {anomaly_id} not found")

    return JSONResponse(
        {
            "id": anomaly.id,
            "asset_name": anomaly.asset_name,
            "timestamp": anomaly.timestamp.isoformat(),
            "severity": anomaly.severity.value,
            "anomaly_type": anomaly.anomaly_type,
            "description": anomaly.description,
            "expected_value": anomaly.expected_value,
            "actual_value": anomaly.actual_value,
            "deviation_percentage": anomaly.deviation_percentage,
            "affected_metrics": list(anomaly.affected_metrics),
            "metadata": anomaly.metadata,
        }
    )


async def export_data(
    format: str,  # noqa: A002
    asset_name: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
) -> JSONResponse:  # type: ignore[misc]
    """
    Export quality data in specified format.

    Args:
        format: Export format (csv, json)
        asset_name: Optional asset name filter
        start_time: Optional start time (ISO format)
        end_time: Optional end time (ISO format)

    Returns:
        Exported data in requested format
    """
    if format not in ("csv", "json"):
        raise HTTPException(status_code=400, detail=f"Unsupported format: {format}")

    store: QualityStore = _store  # type: ignore[assignment]

    start_dt = datetime.fromisoformat(start_time) if start_time else None
    end_dt = datetime.fromisoformat(end_time) if end_time else None

    snapshots = store.get_snapshots(asset_name, start_dt, end_dt, limit=10000)

    if format == "json":
        data = [s.__dict__ for s in snapshots]
        return JSONResponse({"data": data, "count": len(data)})

    # CSV export
    import csv
    import io

    output = io.StringIO()
    writer = csv.writer(output)

    # Header
    writer.writerow(
        [
            "timestamp",
            "asset_name",
            "total_records",
            "valid_records",
            "invalid_records",
            "completeness_score",
            "validity_score",
            "overall_score",
        ]
    )

    # Data rows
    for s in snapshots:
        writer.writerow(
            [
                s.timestamp.isoformat(),
                s.asset_name,
                s.total_records,
                s.valid_records,
                s.invalid_records,
                s.completeness_score,
                s.validity_score,
                s.overall_score,
            ]
        )

    csv_content = output.getvalue()
    return JSONResponse(
        {"csv": csv_content, "count": len(snapshots)},
        headers={
            "Content-Disposition": f"attachment; filename=quality_export_{datetime.now().date()}.csv"
        },
    )


async def websocket_quality(websocket: WebSocket) -> None:  # type: ignore[misc]
    """
    WebSocket endpoint for real-time quality updates.

    Args:
        websocket: WebSocket connection
    """
    await websocket.accept()

    try:
        # In production, push real-time updates
        while True:
            # For demo, send current snapshots every 5 seconds
            store: QualityStore = _store  # type: ignore[assignment]

            await websocket.send_json(
                {
                    "type": "quality_update",
                    "timestamp": datetime.now().isoformat(),
                    "assets": [
                        {
                            "name": asset,
                            "latest_score": history.latest_snapshot.overall_score
                            if history.latest_snapshot
                            else 0.0,
                        }
                        for asset, history in store.history.items()
                    ],
                }
            )

            await __import__("asyncio").sleep(5)

    except WebSocketDisconnect:  # type: ignore
        logger.info("WebSocket client disconnected")


# =============================================================================
# Server Entry Point
# =============================================================================


def run_server(host: str = "0.0.0.0", port: int = 8000) -> None:
    """
    Run the quality dashboard server.

    Args:
        host: Host to bind to
        port: Port to listen on
    """
    if not FASTAPI_AVAILABLE:  # pragma: no cover
        msg = "FastAPI is not installed. Install with: uv pip install 'vibe-piper[dashboard]'"
        raise ImportError(msg)

    app = create_app()
    uvicorn.run(app, host=host, port=port, log_level="info")
