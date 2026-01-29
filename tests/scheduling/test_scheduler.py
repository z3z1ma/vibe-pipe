"""Tests for Scheduler."""

from datetime import timezone
from zoneinfo import ZoneInfo

import pytest

from vibe_piper.scheduling import (
    BackfillConfig,
    BackfillStatus,
    BackfillTask,
    CronSchedule,
    Schedule,
    Scheduler,
    SchedulerConfig,
    ScheduleStatus,
    ScheduleType,
    TriggerEvent,
    TriggerType,
)
from vibe_piper.types import AssetGraph


class TestScheduler:
    """Tests for Scheduler."""

    def test_create_scheduler(self):
        """Test creating a scheduler."""
        config = SchedulerConfig(check_interval_seconds=30)
        scheduler = Scheduler(config=config)
        assert scheduler.config.check_interval_seconds == 30
        assert not scheduler.is_running()

    def test_add_and_get_schedule(self):
        """Test adding and retrieving schedules."""
        scheduler = Scheduler()

        schedule = Schedule(
            schedule_id="test_schedule",
            name="Test Schedule",
            schedule_type=ScheduleType.CRON,
            schedule_definition=CronSchedule(cron_expression="0 0 * * *"),
            asset_graph=AssetGraph(name="test_graph"),
            status=ScheduleStatus.ACTIVE,
        )

        scheduler.add_schedule(schedule)
        loaded = scheduler.get_schedule("test_schedule")
        assert loaded is not None
        assert loaded.name == "Test Schedule"

    def test_pause_and_resume_schedule(self):
        """Test pausing and resuming a schedule."""
        scheduler = Scheduler()

        schedule = Schedule(
            schedule_id="test_schedule",
            name="Test Schedule",
            schedule_type=ScheduleType.CRON,
            schedule_definition=CronSchedule(cron_expression="0 0 * * *"),
            asset_graph=AssetGraph(name="test_graph"),
            status=ScheduleStatus.ACTIVE,
        )

        scheduler.add_schedule(schedule)

        # Pause schedule
        result = scheduler.pause_schedule("test_schedule")
        assert result is True

        loaded = scheduler.get_schedule("test_schedule")
        assert loaded is not None
        assert loaded.status == ScheduleStatus.PAUSED

        # Resume schedule
        result = scheduler.resume_schedule("test_schedule")
        assert result is True

        loaded = scheduler.get_schedule("test_schedule")
        assert loaded is not None
        assert loaded.status == ScheduleStatus.ACTIVE

    def test_delete_schedule(self):
        """Test deleting a schedule."""
        scheduler = Scheduler()

        schedule = Schedule(
            schedule_id="test_schedule",
            name="Test Schedule",
            schedule_type=ScheduleType.CRON,
            schedule_definition=CronSchedule(cron_expression="0 0 * * *"),
            asset_graph=AssetGraph(name="test_graph"),
            status=ScheduleStatus.ACTIVE,
        )

        scheduler.add_schedule(schedule)

        # Delete schedule
        result = scheduler.delete_schedule("test_schedule")
        assert result is True

        # Verify it's gone
        loaded = scheduler.get_schedule("test_schedule")
        assert loaded is None

    def test_trigger_event(self):
        """Test triggering an event."""
        scheduler = Scheduler()

        # Add event-driven schedule
        schedule = Schedule(
            schedule_id="event_schedule",
            name="Event Schedule",
            schedule_type=ScheduleType.EVENT,
            schedule_definition=EventTrigger(event_type="webhook"),
            asset_graph=AssetGraph(name="test_graph"),
            status=ScheduleStatus.ACTIVE,
        )

        scheduler.add_schedule(schedule)

        # Submit event
        event = TriggerEvent(
            event_type="webhook",
            event_data={"source": "external"},
            source="external",
        )

        scheduler.trigger_event(event)

        # Event should be in queue (not processed yet since scheduler isn't running)
