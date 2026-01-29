"""Tests for backfill functionality."""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from vibe_piper.scheduling import (
    BackfillConfig,
    BackfillManager,
    BackfillStatus,
    BackfillTask,
    IntervalSchedule,
    Schedule,
    ScheduleStatus,
    ScheduleStore,
    ScheduleType,
)
from vibe_piper.types import AssetGraph


class TestBackfill:
    """Tests for backfill functionality."""

    def test_create_backfill_config(self):
        """Test creating a backfill configuration."""
        start = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
        end = datetime(2024, 1, 31, 23, 59, tzinfo=timezone.utc)

        config = BackfillConfig(
            backfill_id="bf_test",
            schedule_id="test_schedule",
            start_date=start,
            end_date=end,
            timezone="UTC",
        )

        assert config.backfill_id == "bf_test"
        assert config.schedule_id == "test_schedule"
        assert config.parallel is False

    def test_invalid_backfill_dates(self):
        """Test that invalid backfill dates raise errors."""
        start = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
        end = datetime(2023, 12, 31, 23, 59, tzinfo=timezone.utc)

        with pytest.raises(ValueError, match="start_date must be before"):
            BackfillConfig(
                backfill_id="bf_test",
                schedule_id="test_schedule",
                start_date=start,
                end_date=end,
            )

    def test_backfill_task_status(self):
        """Test backfill task status transitions."""
        task = BackfillTask(
            task_id="task_1",
            backfill_id="bf_test",
            schedule_id="test_schedule",
            scheduled_for=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
        )

        assert task.status == BackfillStatus.PENDING

        # Update to running
        task_running = BackfillTask(
            task_id=task.task_id,
            backfill_id=task.backfill_id,
            schedule_id=task.schedule_id,
            scheduled_for=task.scheduled_for,
            status=BackfillStatus.RUNNING,
        )

        assert task_running.status == BackfillStatus.RUNNING

        # Update to completed
        task_completed = BackfillTask(
            task_id=task.task_id,
            backfill_id=task.backfill_id,
            schedule_id=task.schedule_id,
            scheduled_for=task.scheduled_for,
            status=BackfillStatus.COMPLETED,
            started_at=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
            completed_at=datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc),
        )

        assert task_completed.status == BackfillStatus.COMPLETED
