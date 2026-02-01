# Quality Dashboard

## Overview

The Vibe Piper Quality Dashboard provides real-time monitoring of data quality metrics, historical trend analysis, anomaly detection, and drill-down capabilities into validation failures.

## Installation

Install the dashboard dependencies:

```bash
uv pip install 'vibe-piper[dashboard]'
```

## Quick Start

Start the quality dashboard server:

```bash
vibepiper dashboard
```

By default, the dashboard runs on `http://0.0.0.0:8000`

### Custom Host and Port

```bash
vibepiper dashboard --host 127.0.0.1 --port 8080
```

### Enable Auto-Reload (Development)

```bash
vibepiper dashboard --reload
```

## Features

### Quality Metrics API

The dashboard provides a comprehensive REST API for quality metrics:

- **Snapshots**: Get quality score history for assets
- **History**: Retrieve aggregated historical quality data
- **Alerts**: View and manage quality alerts
- **Anomalies**: Explore detected anomalies with severity
- **Aggregations**: Query aggregated metrics by time period
- **Export**: Download quality data as CSV or JSON

### API Endpoints

#### Health Check
```
GET /api/health
```

#### Assets
```
GET /api/assets
GET /api/assets?asset_name=my_asset
```

#### Quality Snapshots
```
GET /api/quality/snapshots?asset_name=my_asset&limit=100&start_time=2026-01-01T00:00:00Z&end_time=2026-01-31T23:59:59Z
```

#### Quality History
```
GET /api/quality/history/{asset_name}
```

#### Quality Aggregations
```
GET /api/quality/aggregations?period=day&asset_name=my_asset
```

Periods: `day`, `week`, `month`

#### Alerts
```
GET /api/alerts
GET /api/alerts?asset_name=my_asset&status=ACTIVE&severity=HIGH
GET /api/alerts/{alert_id}
POST /api/alerts/{alert_id}/acknowledge
POST /api/alerts/{alert_id}/resolve
```

#### Anomalies
```
GET /api/anomalies
GET /api/anomalies?asset_name=my_asset&severity=CRITICAL
GET /api/anomalies/{anomaly_id}
```

#### Export
```
GET /api/export/csv?asset_name=my_asset&start_time=2026-01-01T00:00:00Z
GET /api/export/json?asset_name=my_asset
```

#### WebSocket (Real-time Updates)
```
WS /ws/quality
```

Connect to receive real-time quality updates every 5 seconds.

### Anomaly Detection Methods

The dashboard includes 5 anomaly detection algorithms:

1. **Z-Score**: Detects outliers based on standard deviations from mean
   - Configurable z-score threshold (default: 3.0)
   - Automatic severity calculation based on z-score magnitude

2. **IQR (Interquartile Range)**: Uses quartiles to detect outliers
   - Configurable IQR multiplier (default: 1.5)
   - Robust to extreme values

3. **Threshold Breach**: Static threshold monitoring
   - Alert when quality falls below configured threshold
   - Useful for SLA monitoring

4. **Change Point Detection**: Detects sudden quality changes
   - Configurable percentage threshold (default: 20%)
   - Identifies rapid degradation or improvement

5. **Moving Average Deviation**: Compares to rolling average
   - Configurable window size (default: 7 periods)
   - Smooths out normal fluctuations

### Data Models

#### QualitySnapshot

Represents quality metrics at a point in time:

```python
from vibe_piper.dashboard.models import QualitySnapshot

snapshot = QualitySnapshot(
    asset_name="my_data_asset",
    timestamp=datetime.now(),
    total_records=1000,
    valid_records=950,
    invalid_records=50,
    completeness_score=0.95,
    validity_score=0.95,
    overall_score=0.95,
    metrics=(),  # Individual quality metrics
    pipeline_id="my_pipeline",
    run_id="run-123",
)
```

#### QualityHistory

Aggregates snapshots for trend analysis:

```python
from vibe_piper.dashboard.models import QualityHistory

history = QualityHistory(asset_name="my_data_asset")
history.add_snapshot(snapshot1)
history.add_snapshot(snapshot2)

print(history.average_score)
print(history.latest_snapshot)

# Get snapshots from last 24 hours
recent = history.get_trend(hours=24)
```

#### Anomaly

Represents a detected anomaly:

```python
from vibe_piper.dashboard.models import Anomaly, AnomalySeverity

anomaly = Anomaly(
    id="anomaly-123",
    asset_name="my_data_asset",
    timestamp=datetime.now(),
    severity=AnomalySeverity.HIGH,
    anomaly_type="sudden_drop",
    description="Quality score dropped from 0.95 to 0.70",
    expected_value=0.95,
    actual_value=0.70,
    deviation_percentage=26.3,
    affected_metrics=("overall_score", "completeness_score"),
)
```

#### QualityAlert

Alert triggered by quality issues:

```python
from vibe_piper.dashboard.models import QualityAlert, AlertStatus, AnomalySeverity

alert = QualityAlert(
    id="alert-123",
    asset_name="my_data_asset",
    alert_type="threshold_breach",
    status=AlertStatus.ACTIVE,
    severity=AnomalySeverity.HIGH,
    title="Quality score below threshold",
    message="Overall quality score is below threshold",
    created_at=datetime.now(),
    threshold_config={"min_score": 0.90},
)

# Acknowledge alert
alert.acknowledge()

# Resolve alert
alert.resolve()
```

### Programmatic API Usage

#### Creating Quality Snapshots

```python
from vibe_piper.quality import generate_quality_report
from vibe_piper.dashboard.models import QualitySnapshot

# Generate quality report from data
report = generate_quality_report(records, schema)

# Create snapshot for dashboard
snapshot = QualitySnapshot.from_report(
    asset_name="my_asset",
    report=report,
    pipeline_id="my_pipeline",
    run_id="run-123",
)

# Store in dashboard (API or direct)
# In production, this goes to database
```

#### Detecting Anomalies

```python
from vibe_piper.dashboard.anomaly_detection import (
    AnomalyDetector,
    AnomalyDetectorConfig,
    AnomalyDetectionMethod,
)

# Configure detector
config = AnomalyDetectorConfig(
    method=AnomalyDetectionMethod.Z_SCORE,
    z_score_threshold=3.0,
    min_data_points=10,
)

detector = AnomalyDetector(config=config)

# Get historical snapshots
snapshots = get_quality_history(asset_name)

# Detect anomalies
anomalies = detector.detect_anomalies(snapshots, metric_name="overall_score")

for anomaly in anomalies:
    print(f"Anomaly: {anomaly.anomaly_type}")
    print(f"Severity: {anomaly.severity}")
    print(f"Deviation: {anomaly.deviation_percentage:.1%}")
```

## Configuration

### Anomaly Detection Configuration

```python
from vibe_piper.dashboard.anomaly_detection import AnomalyDetectorConfig

config = AnomalyDetectorConfig(
    method=AnomalyDetectionMethod.Z_SCORE,  # Detection method
    z_score_threshold=3.0,              # Z-score threshold
    iqr_multiplier=1.5,                 # IQR multiplier
    threshold_value=0.90,                # Static threshold
    change_point_threshold=0.2,           # % change threshold
    moving_average_window=7,               # Window size
    min_data_points=5,                    # Minimum data points
)
```

### Alert Thresholds

Configure quality score thresholds in your pipeline configuration:

```yaml
# vibe_piper_config.yaml
dashboard:
  thresholds:
    overall_score:
      warning: 0.90
      critical: 0.80
    completeness_score:
      warning: 0.95
      critical: 0.85
    validity_score:
      warning: 0.95
      critical: 0.85
```

## Testing

Run dashboard tests:

```bash
# All dashboard tests
uv run pytest tests/dashboard/ -v

# Specific test
uv run pytest tests/dashboard/test_dashboard.py::test_quality_snapshot_creation -v

# With coverage
uv run pytest tests/dashboard/ --cov=src/vibe_piper/dashboard --cov-report=term-missing
```

## Production Deployment

### Using uvicorn

```bash
uvicorn vibe_piper.dashboard.api:create_app \
  --host 0.0.0.0 \
  --port 8000 \
  --workers 4
```

### Using gunicorn

```bash
gunicorn vibe_piper.dashboard.api:create_app \
  --workers 4 \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind 0.0.0.0:8000
```

### Environment Variables

```bash
export VIBE_PIPER_HOST=0.0.0.0
export VIBE_PIPER_PORT=8000
export VIBE_PIPER_LOG_LEVEL=info
export VIBE_PIPER_DB_URL=postgresql://user:pass@localhost:5432/vibepiper
```

### Database Configuration

For production use, configure PostgreSQL for persistent storage:

```python
from vibe_piper.dashboard.api import QualityStore

# In production, use SQLAlchemy with PostgreSQL
# Example (to be implemented):
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine(os.getenv("VIBE_PIPER_DB_URL"))
Session = sessionmaker(bind=engine)

store = QualityStore()  # Would use database instead of in-memory dict
```

## Current Limitations

1. **In-Memory Storage**: Current implementation uses in-memory storage. Production should use PostgreSQL.

2. **Frontend UI**: Basic API and data models are complete. React frontend UI is not yet implemented.

3. **PDF Export**: Only CSV and JSON export are currently available.

4. **Advanced Anomaly Configuration**: Detection methods are fixed per asset. Per-metric configuration will be added.

## Future Enhancements

- [ ] PostgreSQL persistent storage
- [ ] React frontend UI with real-time charts
- [ ] PDF report generation
- [ ] Advanced anomaly configuration per metric
- [ ] Alert notification (email, Slack, PagerDuty)
- [ ] Drill-down into specific validation failures
- [ ] Quality score heatmap visualization
- [ ] Asset comparison views
- [ ] Custom anomaly detection algorithms
- [ ] Multi-asset correlation analysis

## Troubleshooting

### Dashboard Won't Start

```bash
# Check dependencies
uv pip list | grep -E "(fastapi|uvicorn|websockets)"

# Install missing dependencies
uv pip install 'vibe-piper[dashboard]'
```

### Port Already in Use

```bash
# Find process using port 8000
lsof -i :8000

# Kill process
kill -9 <PID>

# Or use different port
vibepiper dashboard --port 8080
```

### No Quality Data Showing

```bash
# Verify quality reports are being generated
# Check pipeline execution logs

# Manually add test snapshot
from vibe_piper.dashboard.api import _store
from vibe_piper.dashboard.models import QualitySnapshot
from datetime import datetime

store = _store
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
store.add_snapshot(snapshot)

# Verify via API
curl http://localhost:8000/api/quality/snapshots
```

## API Examples

### Get All Assets

```bash
curl http://localhost:8000/api/assets
```

Response:
```json
{
  "assets": ["asset1", "asset2", "asset3"],
  "count": 3
}
```

### Get Quality Snapshots

```bash
curl "http://localhost:8000/api/quality/snapshots?limit=10"
```

### Get Aggregated Quality Data

```bash
curl "http://localhost:8000/api/quality/aggregations?period=day&start_time=2026-01-01T00:00:00Z"
```

Response:
```json
{
  "aggregations": [
    {
      "period": "day",
      "start_time": "2026-01-01T00:00:00Z",
      "end_time": "2026-01-01T23:59:59Z",
      "avg_completeness": 0.94,
      "avg_validity": 0.93,
      "avg_overall": 0.935,
      "total_snapshots": 24
    }
  ],
  "count": 1
}
```

### Export Quality Data

```bash
# Export as CSV
curl "http://localhost:8000/api/export/csv?asset_name=my_asset" \
  -o quality_report.csv

# Export as JSON
curl "http://localhost:8000/api/export/json?asset_name=my_asset" \
  -o quality_report.json
```

## Contributing

To extend the dashboard:

1. **Add New Anomaly Detection Method**:
   - Add enum value to `AnomalyDetectionMethod`
   - Implement detection logic in `AnomalyDetector`
   - Add configuration options to `AnomalyDetectorConfig`

2. **Add New API Endpoint**:
   - Add route handler in `api.py`
   - Update `register_routes()`
   - Add tests in `tests/dashboard/test_dashboard.py`

3. **Add New Data Model**:
   - Define in `models.py`
   - Update `__init__.py` exports
   - Add tests

## License

MIT License - see LICENSE file for details.
