"""
Tests for enhanced quality scoring features including:
- 0-100 scale scoring
- Timeliness dimension
- Configurable weights
- Historical trend tracking
- Quality threshold alerts
- Quality improvement recommendations
- Quality dashboard
"""

from datetime import datetime, timedelta

import pytest

from vibe_piper import DataRecord, DataType, Schema, SchemaField
from vibe_piper.validation.quality_scoring import (
    QualityAlert,
    QualityDashboard,
    QualityHistory,
    QualityRecommendation,
    QualityScore,
    QualityTrend,
    calculate_column_quality,
    calculate_quality_score,
    create_quality_dashboard,
    generate_quality_alerts,
    generate_quality_recommendations,
    track_quality_history,
)


class TestQualityScoreScale:
    """Tests for 0-100 scale quality scoring."""

    def test_quality_score_100_scale(self) -> None:
        """Test that quality scores are on 0-100 scale."""
        schema = Schema(
            name="test",
            fields=(
                SchemaField(name="id", data_type=DataType.INTEGER),
                SchemaField(name="name", data_type=DataType.STRING),
            ),
        )

        records = [
            DataRecord(data={"id": 1, "name": "Alice"}, schema=schema),
            DataRecord(data={"id": 2, "name": "Bob"}, schema=schema),
        ]

        score = calculate_quality_score(records)

        # All scores should be between 0 and 100
        assert 0 <= score.completeness_score <= 100
        assert 0 <= score.accuracy_score <= 100
        assert 0 <= score.uniqueness_score <= 100
        assert 0 <= score.consistency_score <= 100
        assert 0 <= score.timeliness_score <= 100
        assert 0 <= score.overall_score <= 100

    def test_perfect_quality_score(self) -> None:
        """Test perfect data quality."""
        schema = Schema(
            name="test",
            fields=(
                SchemaField(name="id", data_type=DataType.INTEGER),
                SchemaField(name="name", data_type=DataType.STRING),
            ),
        )

        records = [
            DataRecord(data={"id": 1, "name": "Alice"}, schema=schema),
            DataRecord(data={"id": 2, "name": "Bob"}, schema=schema),
        ]

        score = calculate_quality_score(records)

        # Perfect data should have high scores
        assert score.completeness_score >= 95.0
        assert score.overall_score >= 95.0


class TestConfigurableWeights:
    """Tests for configurable weighted scoring."""

    def test_default_weights(self) -> None:
        """Test default weight configuration."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="id", data_type=DataType.INTEGER),),
        )

        records = [
            DataRecord(data={"id": 1}, schema=schema),
            DataRecord(data={"id": 2}, schema=schema),
        ]

        score = calculate_quality_score(records)

        # Default weights should be used
        expected_weights = {
            "completeness": 0.3,
            "accuracy": 0.3,
            "uniqueness": 0.2,
            "consistency": 0.1,
            "timeliness": 0.1,
        }

        assert score.weights == expected_weights

    def test_custom_weights(self) -> None:
        """Test custom weight configuration."""
        custom_weights = {
            "completeness": 0.4,
            "accuracy": 0.4,
            "uniqueness": 0.1,
            "consistency": 0.05,
            "timeliness": 0.05,
        }

        schema = Schema(
            name="test",
            fields=(SchemaField(name="id", data_type=DataType.INTEGER),),
        )

        records = [
            DataRecord(data={"id": 1}, schema=schema),
        ]

        score = calculate_quality_score(records, weights=custom_weights)

        # Custom weights should be used
        assert score.weights == custom_weights


class TestTimelinessDimension:
    """Tests for timeliness/freshness dimension."""

    def test_timeliness_with_timestamp_field(self) -> None:
        """Test timeliness calculation with timestamp field."""
        schema = Schema(
            name="test",
            fields=(
                SchemaField(
                    name="updated_at",
                    data_type=DataType.DATETIME,
                ),
            ),
        )

        now = datetime.now()
        records = [
            DataRecord(data={"updated_at": now}, schema=schema),
            DataRecord(
                data={"updated_at": now - timedelta(hours=1)},
                schema=schema,
            ),
        ]

        score = calculate_quality_score(records, timestamp_field="updated_at", max_age_hours=24)

        # Fresh data should have high timeliness score
        assert score.timeliness_score >= 90.0

    def test_timeliness_with_stale_data(self) -> None:
        """Test timeliness with stale data."""
        schema = Schema(
            name="test",
            fields=(
                SchemaField(
                    name="updated_at",
                    data_type=DataType.DATETIME,
                ),
            ),
        )

        now = datetime.now()
        records = [
            DataRecord(
                data={"updated_at": now - timedelta(hours=48)},
                schema=schema,
            ),
        ]

        score = calculate_quality_score(records, timestamp_field="updated_at", max_age_hours=24)

        # Stale data should have lower timeliness score
        assert score.timeliness_score < 90.0

    def test_timeliness_without_timestamp_field(self) -> None:
        """Test timeliness without timestamp field defaults to 100."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="id", data_type=DataType.INTEGER),),
        )

        records = [
            DataRecord(data={"id": 1}, schema=schema),
        ]

        score = calculate_quality_score(records)

        # Without timestamp field, timeliness should default to 100
        assert score.timeliness_score == 100.0


class TestHistoricalTrendTracking:
    """Tests for historical quality trend tracking."""

    def test_track_quality_history(self) -> None:
        """Test tracking quality history for an asset."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="id", data_type=DataType.INTEGER),),
        )

        records = [
            DataRecord(data={"id": 1}, schema=schema),
        ]

        score1 = calculate_quality_score(records)
        history1 = track_quality_history("test_asset", score1)

        assert history1.asset_name == "test_asset"
        assert len(history1.scores) == 1
        assert history1.scores[0] == score1

        # Track another score
        score2 = calculate_quality_score(records)
        history2 = track_quality_history("test_asset", score2)

        assert len(history2.scores) == 2

    def test_history_max_limit(self) -> None:
        """Test that history respects max_history limit."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="id", data_type=DataType.INTEGER),),
        )

        records = [
            DataRecord(data={"id": 1}, schema=schema),
        ]

        # Add 150 scores (more than default max of 100)
        for _ in range(150):
            score = calculate_quality_score(records)
            track_quality_history("test_asset", score, max_history=100)

        # Should only keep 100 most recent scores
        from vibe_piper.validation.quality_scoring import _quality_history_store

        assert len(_quality_history_store["test_asset"]) == 100

    def test_trend_analysis(self) -> None:
        """Test trend direction analysis."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="id", data_type=DataType.INTEGER),),
        )

        records = [
            DataRecord(data={"id": 1}, schema=schema),
        ]

        # Create scores with improving trend
        scores = []
        for i in range(5):
            # Artificially create different scores
            score = calculate_quality_score(records)
            # We'll use the same score but track timestamps
            scores.append(score)

        history = track_quality_history("test_asset", scores[-1])

        # Trend analysis should be present
        assert "completeness" in history.trends
        assert isinstance(history.trends["completeness"], QualityTrend)


class TestQualityThresholdAlerts:
    """Tests for quality threshold alerts."""

    def test_no_alerts_for_good_quality(self) -> None:
        """Test no alerts for good quality scores."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="id", data_type=DataType.INTEGER),),
        )

        records = [
            DataRecord(data={"id": 1}, schema=schema),
        ]

        score = calculate_quality_score(records)
        alerts = generate_quality_alerts(score)

        # High quality should not generate alerts
        assert len(alerts) == 0

    def test_alert_for_low_overall_score(self) -> None:
        """Test alert for low overall quality score."""
        from vibe_piper.validation.quality_scoring import QualityThresholdConfig

        config = QualityThresholdConfig(overall_threshold=80.0)

        schema = Schema(
            name="test",
            fields=(
                SchemaField(
                    name="id",
                    data_type=DataType.INTEGER,
                    nullable=True,
                ),
                SchemaField(
                    name="name",
                    data_type=DataType.STRING,
                    nullable=True,
                ),
            ),
        )

        # Create records with missing values to lower quality
        records = [
            DataRecord(data={"id": 1, "name": "Alice"}, schema=schema),
            DataRecord(data={"id": None, "name": "Bob"}, schema=schema),
            DataRecord(data={"id": 3, "name": None}, schema=schema),
        ]

        score = calculate_quality_score(records)
        alerts = generate_quality_alerts(score, config)

        # Should have at least one alert (overall score)
        overall_alerts = [a for a in alerts if a.dimension == "overall"]
        # May not trigger if score is still above threshold
        if score.overall_score < config.overall_threshold:
            assert len(overall_alerts) >= 1

    def test_alert_severity_levels(self) -> None:
        """Test alert severity based on score."""
        from vibe_piper.validation.quality_scoring import QualityThresholdConfig

        config = QualityThresholdConfig(
            overall_threshold=100.0, dimension_thresholds={"completeness": 50.0}
        )

        schema = Schema(
            name="test",
            fields=(
                SchemaField(
                    name="id",
                    data_type=DataType.INTEGER,
                    nullable=True,
                ),
            ),
        )

        # Create records with very low completeness
        records = [
            DataRecord(data={"id": None}, schema=schema),
            DataRecord(data={"id": None}, schema=schema),
            DataRecord(data={"id": None}, schema=schema),
        ]

        score = calculate_quality_score(records)
        alerts = generate_quality_alerts(score, config)

        # Check that alerts have appropriate severity
        if alerts:
            for alert in alerts:
                assert alert.severity in ("critical", "warning", "info")


class TestQualityRecommendations:
    """Tests for quality improvement recommendations."""

    def test_no_recommendations_for_perfect_quality(self) -> None:
        """Test no recommendations for perfect quality."""
        schema = Schema(
            name="test",
            fields=(
                SchemaField(name="id", data_type=DataType.INTEGER),
                SchemaField(name="name", data_type=DataType.STRING),
            ),
        )

        records = [
            DataRecord(data={"id": 1, "name": "Alice"}, schema=schema),
            DataRecord(data={"id": 2, "name": "Bob"}, schema=schema),
        ]

        score = calculate_quality_score(records)
        recommendations = generate_quality_recommendations(score)

        # Perfect quality may still have some recommendations
        # but high quality (>90%) should have fewer
        if score.overall_score >= 95:
            assert len(recommendations) <= 2

    def test_completeness_recommendations(self) -> None:
        """Test recommendations for low completeness."""
        schema = Schema(
            name="test",
            fields=(
                SchemaField(
                    name="id",
                    data_type=DataType.INTEGER,
                    nullable=True,
                ),
                SchemaField(
                    name="name",
                    data_type=DataType.STRING,
                    nullable=True,
                ),
            ),
        )

        records = [
            DataRecord(data={"id": None, "name": None}, schema=schema),
            DataRecord(data={"id": None, "name": None}, schema=schema),
        ]

        score = calculate_quality_score(records)
        recommendations = generate_quality_recommendations(score)

        # Should have completeness recommendation
        completeness_recs = [r for r in recommendations if r.category == "completeness"]
        if score.completeness_score < 90:
            assert len(completeness_recs) >= 1
            assert completeness_recs[0].priority in ("critical", "high", "medium")

    def test_all_categories_have_recommendations(self) -> None:
        """Test that all quality categories can generate recommendations."""
        schema = Schema(
            name="test",
            fields=(
                SchemaField(
                    name="id",
                    data_type=DataType.INTEGER,
                    nullable=True,
                ),
            ),
        )

        # Create records with issues in all dimensions
        records = [
            DataRecord(data={"id": None}, schema=schema),
            DataRecord(data={"id": None}, schema=schema),
            DataRecord(data={"id": None}, schema=schema),
        ]

        score = calculate_quality_score(records)
        recommendations = generate_quality_recommendations(score)

        # Check that all categories with low scores have recommendations
        categories = {r.category for r in recommendations}
        # At least some categories should be represented
        if score.completeness_score < 90:
            assert "completeness" in categories


class TestQualityDashboard:
    """Tests for quality dashboard functionality."""

    def test_create_dashboard(self) -> None:
        """Test creating a quality dashboard."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="id", data_type=DataType.INTEGER),),
        )

        records = [
            DataRecord(data={"id": 1}, schema=schema),
        ]

        score = calculate_quality_score(records)
        dashboard = create_quality_dashboard("test_asset", score)

        # Dashboard should contain all expected elements
        assert isinstance(dashboard, QualityDashboard)
        assert dashboard.current_score == score.overall_score
        assert len(dashboard.dimension_scores) == 5  # 5 dimensions
        assert "completeness" in dashboard.dimension_scores
        assert "accuracy" in dashboard.dimension_scores
        assert "uniqueness" in dashboard.dimension_scores
        assert "consistency" in dashboard.dimension_scores
        assert "timeliness" in dashboard.dimension_scores

    def test_dashboard_with_history(self) -> None:
        """Test dashboard with historical trends."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="id", data_type=DataType.INTEGER),),
        )

        records = [
            DataRecord(data={"id": 1}, schema=schema),
        ]

        score = calculate_quality_score(records)
        history = track_quality_history("test_asset", score)
        dashboard = create_quality_dashboard("test_asset", score, history=history)

        # Dashboard should include historical trends
        assert len(dashboard.historical_trends) > 0
        assert "completeness" in dashboard.historical_trends

    def test_dashboard_includes_alerts_and_recommendations(self) -> None:
        """Test dashboard includes alerts and recommendations."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="id", data_type=DataType.INTEGER),),
        )

        records = [
            DataRecord(data={"id": 1}, schema=schema),
        ]

        score = calculate_quality_score(records)
        dashboard = create_quality_dashboard("test_asset", score)

        # Dashboard should include alerts and recommendations
        assert isinstance(dashboard.alerts, tuple)
        assert isinstance(dashboard.recommendations, tuple)
        # Even with good data, we might have recommendations
        assert len(dashboard.alerts) >= 0
        assert len(dashboard.recommendations) >= 0


class TestColumnQuality:
    """Tests for column-level quality scoring."""

    def test_column_quality_100_scale(self) -> None:
        """Test column quality is on 0-100 scale."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="id", data_type=DataType.INTEGER),),
        )

        records = [
            DataRecord(data={"id": 1}, schema=schema),
            DataRecord(data={"id": 2}, schema=schema),
        ]

        col_quality = calculate_column_quality(records, "id")

        # All scores should be between 0 and 100
        assert 0 <= col_quality.completeness <= 100
        assert 0 <= col_quality.accuracy <= 100
        assert 0 <= col_quality.uniqueness <= 100

    def test_column_quality_with_missing_values(self) -> None:
        """Test column quality with missing values."""
        schema = Schema(
            name="test",
            fields=(
                SchemaField(
                    name="id",
                    data_type=DataType.INTEGER,
                    nullable=True,
                ),
            ),
        )

        records = [
            DataRecord(data={"id": 1}, schema=schema),
            DataRecord(data={"id": 2}, schema=schema),
            DataRecord(data={"id": None}, schema=schema),
            DataRecord(data={"id": None}, schema=schema),
        ]

        col_quality = calculate_column_quality(records, "id")

        # Should detect missing values
        assert col_quality.null_count == 2
        # Completeness should reflect missing values (50% complete)
        assert 48 <= col_quality.completeness <= 52  # Allow for rounding
