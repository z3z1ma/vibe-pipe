"""Pytest fixtures for dashboard tests."""

from datetime import datetime, timedelta

import pytest

from vibe_piper.dashboard.models import QualitySnapshot


@pytest.fixture
def sample_snapshot() -> QualitySnapshot:
    """Create a sample quality snapshot for testing."""
    return QualitySnapshot(
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


@pytest.fixture
def sample_snapshots(sample_snapshot: QualitySnapshot) -> list[QualitySnapshot]:
    """Create multiple sample snapshots for testing."""
    now = datetime.now()
    snapshots: list[QualitySnapshot] = []

    for i in range(10):
        snapshot = QualitySnapshot(
            asset_name="test_asset",
            timestamp=now - timedelta(hours=i + 1),
            total_records=1000,
            valid_records=950 - (i * 5),
            invalid_records=50 + (i * 5),
            completeness_score=0.95 - (i * 0.005),
            validity_score=0.95 - (i * 0.005),
            overall_score=0.95 - (i * 0.005),
            metrics=(),
        )
        snapshots.append(snapshot)

    return snapshots


@pytest.fixture
def sample_snapshots_with_outlier(sample_snapshots: list[QualitySnapshot]) -> list[QualitySnapshot]:
    """Create sample snapshots with an outlier included."""
    # Add an outlier in the middle
    outlier = QualitySnapshot(
        asset_name="test_asset",
        timestamp=sample_snapshots[4].timestamp,
        total_records=1000,
        valid_records=600,  # Much lower
        invalid_records=400,
        completeness_score=0.60,  # Outlier
        validity_score=0.60,
        overall_score=0.60,
        metrics=(),
    )

    # Insert outlier
    modified = sample_snapshots.copy()
    modified[4] = outlier

    return modified
