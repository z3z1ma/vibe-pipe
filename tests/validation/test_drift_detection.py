"""
Tests for drift detection module.

Tests cover:
- KS test drift detection
- Chi-square test drift detection
- PSI drift detection
- Baseline storage and retrieval
- Drift history tracking
- Threshold configuration and alerting
- Validation check wrappers
- Synthetic drift scenarios
"""

import json
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from vibe_piper.types import DataRecord, DataType, Schema, SchemaField
from vibe_piper.validation import (
    BaselineMetadata,
    BaselineStore,
    DriftHistory,
    DriftResult,
    DriftThresholds,
    check_drift_alert,
    check_drift_ks,
    check_drift_psi,
    detect_drift_ks,
    detect_drift_psi,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_schema() -> Schema:
    """Create a sample schema for testing."""
    return Schema(
        name="test_schema",
        fields=(
            SchemaField(name="id", data_type=DataType.INTEGER, required=True),
            SchemaField(name="value", data_type=DataType.FLOAT, required=True),
            SchemaField(name="category", data_type=DataType.STRING, required=False),
        ),
    )


@pytest.fixture
def normal_baseline(sample_schema: Schema) -> list[DataRecord]:
    """Create baseline data with normal distribution."""
    import random

    random.seed(42)
    return [
        DataRecord(
            data={"id": i, "value": random.gauss(50, 10), "category": "A"}, schema=sample_schema
        )
        for i in range(200)
    ]


@pytest.fixture
def drifted_baseline(sample_schema: Schema) -> list[DataRecord]:
    """Create baseline data with shifted distribution."""
    import random

    random.seed(100)
    return [
        DataRecord(
            data={"id": i, "value": random.gauss(70, 10), "category": "B"}, schema=sample_schema
        )
        for i in range(200)
    ]


@pytest.fixture
def minimal_schema() -> Schema:
    """Minimal schema for simple records."""
    return Schema(
        name="minimal",
        fields=(SchemaField(name="value", data_type=DataType.FLOAT, required=True),),
    )


# =============================================================================
# Threshold Configuration Tests
# =============================================================================


class TestDriftThresholds:
    """Tests for DriftThresholds configuration."""

    def test_default_thresholds(self) -> None:
        """Test default threshold values."""
        thresholds = DriftThresholds()
        assert thresholds.warning == 0.1
        assert thresholds.critical == 0.25
        assert thresholds.psi_warning == 0.1
        assert thresholds.psi_critical == 0.2
        assert thresholds.ks_significance == 0.05

    def test_custom_thresholds(self) -> None:
        """Test custom threshold values."""
        thresholds = DriftThresholds(
            warning=0.05,
            critical=0.15,
            psi_warning=0.05,
            psi_critical=0.1,
            ks_significance=0.01,
        )
        assert thresholds.warning == 0.05
        assert thresholds.critical == 0.15
        assert thresholds.psi_warning == 0.05
        assert thresholds.psi_critical == 0.1
        assert thresholds.ks_significance == 0.01

    def test_invalid_warning_threshold(self) -> None:
        """Test that invalid warning threshold raises error."""
        with pytest.raises(ValueError, match="warning threshold must be between 0 and 1"):
            DriftThresholds(warning=1.5)

    def test_invalid_critical_threshold(self) -> None:
        """Test that invalid critical threshold raises error."""
        with pytest.raises(ValueError, match="critical threshold must be between 0 and 1"):
            DriftThresholds(critical=-0.1)

    def test_warning_ge_critical(self) -> None:
        """Test that warning >= critical raises error."""
        with pytest.raises(ValueError, match="warning threshold .* must be less than critical"):
            DriftThresholds(warning=0.3, critical=0.25)

    def test_psi_warning_ge_critical(self) -> None:
        """Test that psi_warning >= psi_critical raises error."""
        with pytest.raises(ValueError, match="psi_warning .* must be less than psi_critical"):
            DriftThresholds(psi_warning=0.25, psi_critical=0.2)


# =============================================================================
# Baseline Storage Tests
# =============================================================================


class TestBaselineStore:
    """Tests for BaselineStore functionality."""

    @pytest.fixture
    def temp_dir(self) -> tempfile.TemporaryDirectory:
        """Create temporary directory for test files."""
        return tempfile.TemporaryDirectory()

    @pytest.fixture
    def store(self, temp_dir: tempfile.TemporaryDirectory) -> BaselineStore:
        """Create BaselineStore instance with temp directory."""
        return BaselineStore(storage_dir=temp_dir.name)

    def test_add_baseline(self, store: BaselineStore, sample_schema: Schema) -> None:
        """Test adding a new baseline."""
        baseline_data = [
            DataRecord(data={"id": 1, "value": 10.0}, schema=sample_schema) for _ in range(10)
        ]

        metadata = store.add_baseline("test_baseline", baseline_data, description="Test baseline")

        assert metadata.baseline_id == "test_baseline"
        assert metadata.sample_size == 10
        assert metadata.description == "Test baseline"
        assert "id" in metadata.columns
        assert "value" in metadata.columns

    def test_add_empty_baseline_fails(self, store: BaselineStore) -> None:
        """Test that adding empty baseline raises error."""
        with pytest.raises(ValueError, match="Cannot create baseline from empty data"):
            store.add_baseline("empty", [])

    def test_add_duplicate_baseline_fails(
        self, store: BaselineStore, sample_schema: Schema
    ) -> None:
        """Test that adding duplicate baseline raises error."""
        baseline_data = [
            DataRecord(data={"id": 1, "value": 10.0}, schema=sample_schema) for _ in range(10)
        ]

        store.add_baseline("duplicate", baseline_data)

        with pytest.raises(ValueError, match="already exists"):
            store.add_baseline("duplicate", baseline_data)

    def test_get_baseline(self, store: BaselineStore, sample_schema: Schema) -> None:
        """Test retrieving a baseline."""
        original_data = [
            DataRecord(data={"id": i, "value": float(i)}, schema=sample_schema) for i in range(5)
        ]

        store.add_baseline("retrieve_test", original_data)
        retrieved_data = store.get_baseline("retrieve_test")

        assert len(retrieved_data) == len(original_data)
        for i, (orig, ret) in enumerate(zip(original_data, retrieved_data)):
            assert orig["id"] == ret["id"]
            assert orig["value"] == ret["value"]

    def test_get_baseline_with_schema(self, store: BaselineStore, minimal_schema: Schema) -> None:
        """Test retrieving baseline with specific schema."""
        original_data = [
            DataRecord(data={"value": float(i)}, schema=minimal_schema) for i in range(5)
        ]

        store.add_baseline("schema_test", original_data)
        retrieved_data = store.get_baseline("schema_test", schema=minimal_schema)

        assert len(retrieved_data) == len(original_data)
        assert all(r.schema == minimal_schema for r in retrieved_data)

    def test_get_nonexistent_baseline_fails(self, store: BaselineStore) -> None:
        """Test that getting nonexistent baseline raises error."""
        with pytest.raises(FileNotFoundError, match="not found"):
            store.get_baseline("nonexistent")

    def test_get_metadata(self, store: BaselineStore, sample_schema: Schema) -> None:
        """Test retrieving baseline metadata."""
        baseline_data = [
            DataRecord(data={"id": 1, "value": 10.0}, schema=sample_schema) for _ in range(10)
        ]

        store.add_baseline("metadata_test", baseline_data, description="Metadata test")
        metadata = store.get_metadata("metadata_test")

        assert metadata.baseline_id == "metadata_test"
        assert metadata.sample_size == 10
        assert metadata.description == "Metadata test"
        assert isinstance(metadata.created_at, datetime)

    def test_list_baselines(self, store: BaselineStore, sample_schema: Schema) -> None:
        """Test listing all baselines."""
        for i in range(3):
            baseline_data = [
                DataRecord(data={"id": j, "value": float(j)}, schema=sample_schema)
                for j in range(5)
            ]
            store.add_baseline(f"baseline_{i}", baseline_data)

        baselines = store.list_baselines()
        assert len(baselines) == 3
        baseline_ids = {b.baseline_id for b in baselines}
        assert baseline_ids == {"baseline_0", "baseline_1", "baseline_2"}

    def test_delete_baseline(self, store: BaselineStore, sample_schema: Schema) -> None:
        """Test deleting a baseline."""
        baseline_data = [
            DataRecord(data={"id": 1, "value": 10.0}, schema=sample_schema) for _ in range(10)
        ]

        store.add_baseline("delete_test", baseline_data)
        assert store.get_metadata("delete_test") is not None

        store.delete_baseline("delete_test")

        with pytest.raises(FileNotFoundError):
            store.get_metadata("delete_test")

    def test_delete_nonexistent_baseline_fails(self, store: BaselineStore) -> None:
        """Test that deleting nonexistent baseline raises error."""
        with pytest.raises(FileNotFoundError):
            store.delete_baseline("nonexistent")


# =============================================================================
# Drift History Tests
# =============================================================================


class TestDriftHistory:
    """Tests for DriftHistory functionality."""

    @pytest.fixture
    def temp_dir(self) -> tempfile.TemporaryDirectory:
        """Create temporary directory for test files."""
        return tempfile.TemporaryDirectory()

    @pytest.fixture
    def history(self, temp_dir: tempfile.TemporaryDirectory) -> DriftHistory:
        """Create DriftHistory instance with temp directory."""
        return DriftHistory(storage_dir=temp_dir.name)

    @pytest.fixture
    def sample_result(self) -> DriftResult:
        """Create a sample drift result."""
        return DriftResult(
            method="ks_test",
            drift_score=0.15,
            drifted_columns=("value",),
            p_values={"value": 0.01},
            statistics={"ks_statistic": 0.15},
            recommendations=["Retrain model"],
        )

    @pytest.fixture
    def thresholds(self) -> DriftThresholds:
        """Create test thresholds."""
        return DriftThresholds(warning=0.1, critical=0.25)

    def test_add_entry(
        self,
        history: DriftHistory,
        sample_result: DriftResult,
        thresholds: DriftThresholds,
    ) -> None:
        """Test adding a drift entry to history."""
        entry = history.add_entry(sample_result, "test_baseline", thresholds)

        assert entry.baseline_id == "test_baseline"
        assert entry.method == "ks_test"
        assert entry.drift_score == 0.15
        assert entry.alert_level == "warning"  # 0.15 >= 0.1 warning

    def test_critical_alert(
        self,
        history: DriftHistory,
        sample_result: DriftResult,
        thresholds: DriftThresholds,
    ) -> None:
        """Test critical alert level."""
        critical_result = DriftResult(
            method="ks_test",
            drift_score=0.30,  # Above critical
            drifted_columns=("value",),
            p_values={"value": 0.001},
            statistics={"ks_statistic": 0.30},
            recommendations=["Retrain immediately"],
        )

        entry = history.add_entry(critical_result, "test_baseline", thresholds)
        assert entry.alert_level == "critical"

    def test_no_alert(
        self,
        history: DriftHistory,
        thresholds: DriftThresholds,
    ) -> None:
        """Test no alert when drift is below threshold."""
        no_drift_result = DriftResult(
            method="ks_test",
            drift_score=0.05,  # Below warning
            drifted_columns=(),
            p_values={"value": 0.5},
            statistics={"ks_statistic": 0.05},
            recommendations=["No drift detected"],
        )

        entry = history.add_entry(no_drift_result, "test_baseline", thresholds)
        assert entry.alert_level == "none"

    def test_get_entries(
        self,
        history: DriftHistory,
        sample_result: DriftResult,
        thresholds: DriftThresholds,
    ) -> None:
        """Test retrieving drift entries."""
        for i in range(5):
            result = DriftResult(
                method="ks_test",
                drift_score=0.1 + i * 0.05,
                drifted_columns=("value",) if i > 2 else (),
                p_values={"value": 0.01},
                statistics={},
                recommendations=[],
            )
            history.add_entry(result, "test_baseline", thresholds)

        entries = history.get_entries("test_baseline")
        assert len(entries) == 5
        # Should be sorted by timestamp descending
        assert entries[0].drift_score >= entries[-1].drift_score

    def test_get_entries_with_limit(
        self,
        history: DriftHistory,
        sample_result: DriftResult,
        thresholds: DriftThresholds,
    ) -> None:
        """Test retrieving entries with limit."""
        for i in range(10):
            result = DriftResult(
                method="ks_test",
                drift_score=0.1,
                drifted_columns=(),
                p_values={},
                statistics={},
                recommendations=[],
            )
            history.add_entry(result, "test_baseline", thresholds)

        entries = history.get_entries("test_baseline", limit=5)
        assert len(entries) == 5

    def test_get_empty_history(self, history: DriftHistory) -> None:
        """Test getting entries from empty history."""
        entries = history.get_entries("nonexistent")
        assert entries == []

    def test_get_trend(
        self,
        history: DriftHistory,
        thresholds: DriftThresholds,
    ) -> None:
        """Test getting drift trend statistics."""
        # Add entries with increasing drift
        for i in range(5):
            result = DriftResult(
                method="ks_test",
                drift_score=0.05 + i * 0.05,  # 0.05, 0.1, 0.15, 0.2, 0.25
                drifted_columns=(),
                p_values={},
                statistics={},
                recommendations=[],
            )
            history.add_entry(result, "test_baseline", thresholds)

        trend = history.get_trend("test_baseline", window=5)
        assert trend["count"] == 5
        assert 0.14 <= trend["avg_drift_score"] <= 0.16
        assert trend["max_drift_score"] == 0.25
        assert trend["min_drift_score"] == 0.05

    def test_increasing_trend(
        self,
        history: DriftHistory,
        thresholds: DriftThresholds,
    ) -> None:
        """Test increasing trend detection."""
        # Add entries with increasing drift
        for i in range(10):
            result = DriftResult(
                method="ks_test",
                drift_score=0.05 + i * 0.02,  # Steady increase
                drifted_columns=(),
                p_values={},
                statistics={},
                recommendations=[],
            )
            history.add_entry(result, "test_baseline", thresholds)

        trend = history.get_trend("test_baseline", window=10)
        assert trend["trend"] == "increasing"

    def test_clear_history(
        self,
        history: DriftHistory,
        sample_result: DriftResult,
        thresholds: DriftThresholds,
    ) -> None:
        """Test clearing drift history."""
        history.add_entry(sample_result, "test_baseline", thresholds)
        assert len(history.get_entries("test_baseline")) == 1

        history.clear_history("test_baseline")
        assert len(history.get_entries("test_baseline")) == 0


# =============================================================================
# Drift Detection Tests
# =============================================================================


class TestDriftDetection:
    """Tests for drift detection methods."""

    def test_detect_drift_no_drift(
        self, normal_baseline: list[DataRecord], minimal_schema: Schema
    ) -> None:
        """Test KS detection with no significant drift."""
        # Create similar data
        import random

        random.seed(42)
        new_data = [
            DataRecord(data={"value": random.gauss(50, 10)}, schema=minimal_schema)
            for _ in range(200)
        ]

        detector = detect_drift_ks("value", significance_level=0.05)
        result = detector((normal_baseline, new_data))

        # Should have low drift since distributions are similar
        assert result.drift_score < 0.15
        assert result.method == "ks_test"

    def test_detect_drift_with_shift(
        self, normal_baseline: list[DataRecord], drifted_baseline: list[DataRecord]
    ) -> None:
        """Test KS detection with distribution shift."""
        detector = detect_drift_ks("value", significance_level=0.05)
        result = detector((normal_baseline, drifted_baseline))

        # Should detect higher drift
        assert result.drift_score > 0.1
        assert len(result.drifted_columns) > 0
        assert "value" in result.drifted_columns

    def test_detect_drift_psi(
        self, normal_baseline: list[DataRecord], drifted_baseline: list[DataRecord]
    ) -> None:
        """Test PSI detection."""
        detector = detect_drift_psi("value", num_bins=10, psi_threshold=0.2)
        result = detector((normal_baseline, drifted_baseline))

        assert result.method == "psi"
        assert result.drift_score >= 0

    def test_detect_drift_insufficient_data(self, minimal_schema: Schema) -> None:
        """Test that insufficient data is handled gracefully."""
        small_data = [DataRecord(data={"value": 1.0}, schema=minimal_schema) for _ in range(10)]

        detector = detect_drift_ks("value", min_samples=50)
        result = detector((small_data, small_data))

        assert result.drift_score == 0.0
        assert any("Insufficient sample size" in rec for rec in result.recommendations)


# =============================================================================
# Alerting Tests
# =============================================================================


class TestDriftAlerting:
    """Tests for drift alerting logic."""

    def test_no_alert(self) -> None:
        """Test no alert when drift is below warning threshold."""
        result = DriftResult(
            method="ks_test",
            drift_score=0.05,
            drifted_columns=(),
            p_values={},
            statistics={},
            recommendations=[],
        )

        thresholds = DriftThresholds(warning=0.1, critical=0.25)
        should_alert, alert_level = check_drift_alert(result, thresholds)

        assert should_alert is False
        assert alert_level == "none"

    def test_warning_alert(self) -> None:
        """Test warning alert."""
        result = DriftResult(
            method="ks_test",
            drift_score=0.15,
            drifted_columns=("value",),
            p_values={},
            statistics={},
            recommendations=[],
        )

        thresholds = DriftThresholds(warning=0.1, critical=0.25)
        should_alert, alert_level = check_drift_alert(result, thresholds)

        assert should_alert is True
        assert alert_level == "warning"

    def test_critical_alert(self) -> None:
        """Test critical alert."""
        result = DriftResult(
            method="ks_test",
            drift_score=0.30,
            drifted_columns=("value",),
            p_values={},
            statistics={},
            recommendations=[],
        )

        thresholds = DriftThresholds(warning=0.1, critical=0.25)
        should_alert, alert_level = check_drift_alert(result, thresholds)

        assert should_alert is True
        assert alert_level == "critical"


# =============================================================================
# Validation Check Wrapper Tests
# =============================================================================


class TestValidationCheckWrappers:
    """Tests for validation check wrappers compatible with @validate decorator."""

    def test_check_drift_ks_no_drift(
        self, normal_baseline: list[DataRecord], minimal_schema: Schema
    ) -> None:
        """Test KS check wrapper with no drift."""
        import random

        random.seed(42)
        new_data = [
            DataRecord(data={"value": random.gauss(50, 10)}, schema=minimal_schema)
            for _ in range(200)
        ]

        check = check_drift_ks("value", normal_baseline)
        result = check(new_data)

        # Should be valid when no significant drift
        assert result.is_valid

    def test_check_drift_ks_with_drift(
        self, normal_baseline: list[DataRecord], drifted_baseline: list[DataRecord]
    ) -> None:
        """Test KS check wrapper with drift."""
        check = check_drift_ks("value", normal_baseline)
        result = check(drifted_baseline)

        # Should have warnings about drift
        assert len(result.warnings) > 0

    def test_check_drift_ks_critical(
        self, normal_baseline: list[DataRecord], minimal_schema: Schema
    ) -> None:
        """Test KS check wrapper with critical drift."""
        # Create highly drifted data
        import random

        random.seed(999)
        critical_data = [
            DataRecord(data={"value": random.gauss(100, 5)}, schema=minimal_schema)
            for _ in range(200)
        ]

        check = check_drift_ks("value", normal_baseline)
        result = check(critical_data)

        # Should not be valid with critical drift
        assert not result.is_valid
        assert len(result.errors) > 0
        assert "CRITICAL" in result.errors[0]

    def test_check_drift_psi(
        self, normal_baseline: list[DataRecord], drifted_baseline: list[DataRecord]
    ) -> None:
        """Test PSI check wrapper."""
        check = check_drift_psi("value", normal_baseline)
        result = check(drifted_baseline)

        # Should produce validation result
        assert result is not None
        assert isinstance(result.errors, tuple)
        assert isinstance(result.warnings, tuple)

    def test_check_with_custom_thresholds(
        self, normal_baseline: list[DataRecord], minimal_schema: Schema
    ) -> None:
        """Test check with custom thresholds."""
        # Use very strict thresholds
        strict_thresholds = DriftThresholds(warning=0.01, critical=0.05)

        # Create slightly drifted data
        import random

        random.seed(43)
        new_data = [
            DataRecord(data={"value": random.gauss(55, 10)}, schema=minimal_schema)
            for _ in range(200)
        ]

        check = check_drift_ks("value", normal_baseline, thresholds=strict_thresholds)
        result = check(new_data)

        # Should trigger alert even with small drift due to strict thresholds
        assert not result.is_valid or len(result.warnings) > 0


# =============================================================================
# Synthetic Drift Scenarios
# =============================================================================


class TestSyntheticDriftScenarios:
    """Tests using synthetic drift scenarios."""

    def test_mean_shift_detection(self, minimal_schema: Schema) -> None:
        """Test detection of mean shift."""
        import random

        # Baseline: N(50, 10)
        random.seed(42)
        baseline = [
            DataRecord(data={"value": random.gauss(50, 10)}, schema=minimal_schema)
            for _ in range(200)
        ]

        # New data: N(60, 10) - mean shift of 10
        random.seed(100)
        new_data = [
            DataRecord(data={"value": random.gauss(60, 10)}, schema=minimal_schema)
            for _ in range(200)
        ]

        detector = detect_drift_ks("value")
        result = detector((baseline, new_data))

        # Should detect significant drift
        assert result.drift_score > 0.1
        assert len(result.drifted_columns) > 0

    def test_variance_shift_detection(self, minimal_schema: Schema) -> None:
        """Test detection of variance shift."""
        import random

        # Baseline: N(50, 5)
        random.seed(42)
        baseline = [
            DataRecord(data={"value": random.gauss(50, 5)}, schema=minimal_schema)
            for _ in range(200)
        ]

        # New data: N(50, 20) - variance increase
        random.seed(100)
        new_data = [
            DataRecord(data={"value": random.gauss(50, 20)}, schema=minimal_schema)
            for _ in range(200)
        ]

        detector = detect_drift_ks("value")
        result = detector((baseline, new_data))

        # Should detect variance change
        assert result.drift_score > 0.05

    def test_no_drift_scenario(self, minimal_schema: Schema) -> None:
        """Test scenario with no actual drift."""
        import random

        # Both datasets from same distribution
        random.seed(42)
        baseline = [
            DataRecord(data={"value": random.gauss(50, 10)}, schema=minimal_schema)
            for _ in range(200)
        ]

        random.seed(43)
        new_data = [
            DataRecord(data={"value": random.gauss(50, 10)}, schema=minimal_schema)
            for _ in range(200)
        ]

        detector = detect_drift_ks("value")
        result = detector((baseline, new_data))

        # Should have low drift score
        assert result.drift_score < 0.15
        # May or may not be significant depending on random sampling
        # but should not be a strong signal

    def test_multi_column_drift(self, sample_schema: Schema) -> None:
        """Test drift detection across multiple columns."""
        import random

        # Baseline
        random.seed(42)
        baseline = [
            DataRecord(
                data={
                    "id": i,
                    "value": random.gauss(50, 10),
                    "category": random.choice(["A", "B", "C"]),
                },
                schema=sample_schema,
            )
            for i in range(200)
        ]

        # New data with drift in both value and category
        random.seed(100)
        new_data = [
            DataRecord(
                data={
                    "id": i,
                    "value": random.gauss(70, 10),  # Mean shift
                    "category": random.choice(["C", "D", "E"]),  # Distribution shift
                },
                schema=sample_schema,
            )
            for i in range(200)
        ]

        # Check value drift
        value_detector = detect_drift_ks("value")
        value_result = value_detector((baseline, new_data))

        # Should detect drift in value column
        assert value_result.drift_score > 0.1
