---
"id": "vp-acbc"
"status": "open"
"deps": []
"links": []
"created": "2026-01-30T05:03:33Z"
"type": "feature"
"priority": 1
"assignee": "z3z1ma"
"tags":
- "phase4"
- "implementation"
- "quality"
- "monitoring"
"external": {}
---
# Implement Built-in Quality & Monitoring

Implement built-in quality tracking and monitoring features to eliminate manual metrics collection and provide automatic drift detection.

Based on investigation (vp-5299), Phase 4 needs:

Quality & Monitoring to implement:
1. **Auto-Metrics Collection** - Track metrics automatically during pipeline execution:
   - Row count tracking
   - Null count tracking
   - Value distribution tracking (min, max, mean, median, std)
   - Data type inference
   - Write speed tracking
   - Memory usage tracking

2. **Expectation Integration** - Auto-run expectations on assets:
   - Attach expectations to @asset decorator
   - Auto-execute during pipeline runs
   - Store results in validation history
   - FailureStrategy configuration (FAIL_FAST, FAIL_SLOW, CONTINUE)

3. **Drift Detection Integration** - Automatic drift detection:
   - Auto-capture baselines
   - Calculate Population Stability Index (PSI)
   - Calculate Wasserstein distance
   - Alert on threshold breaches
   - Temporal drift tracking (sliding window, trend detection)

4. **Data Quality Dashboard** - Web UI for quality metrics:
   - Real-time metrics display
   - Historical trends visualization
   - Drift alerts dashboard
   - Asset health status
   - Pipeline execution logs

5. **Alerting System** - Configurable alerting:
   - Email alerts on quality failures
   - Slack/Teams notifications
   - Webhook callbacks
   - Threshold-based alerting (absolute thresholds, percentage changes)
   - Alert suppression rules

Expected files:
- src/vibe_piper/quality/__init__.py
- src/vibe_piper/quality/collector.py (metrics collector)
- src/vibe_piper/quality/drift_detection.py (drift detection engine)
- src/vibe_piper/quality/expectations.py (expectation integration wrapper)
- src/vibe_piper/quality/dashboard.py (web dashboard routes)
- src/vibe_piper/quality/alerting.py (alerting system)
- tests/quality/test_collector.py
- tests/quality/test_drift_detection.py
- docs/quality_monitoring.md (API reference)
- web/dashboard.html (quality dashboard UI)
- web/dashboard.js (dashboard JavaScript)

Acceptance Criteria:
- Auto-metrics collection working for all asset executions
- Expectations auto-run and stored in validation history
- Drift detection with PSI and Wasserstein distance
- Web dashboard displays quality metrics
- Alerting system with email/Slack/webhook support
- Comprehensive test coverage (80%+)
- Documentation for all quality features

Success Metrics (from investigation):
- Zero manual metrics code (automatically collected)
- Automatic drift detection
- Real-time quality dashboard
- Configurable alerting
- Integration with existing validation history and expectations

Reference:
- Investigator design in docs/investigation_missing_abstractions.md section 4
- User goal: 'Zero manual metrics code, automatic drift detection'
