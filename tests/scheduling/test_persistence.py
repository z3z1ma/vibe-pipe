"""Tests for Schedule persistence."""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from vibe_piper.scheduling import (
    CronSchedule,
    Schedule,
    ScheduleEvent,
    ScheduleStatus,
    ScheduleStore,
    ScheduleType,
    TriggerType,
)
from vibe_piper.types import AssetGraph


class TestScheduleStore:
    """Tests for ScheduleStore."""

    def test_save_and_load_schedule(self, tmp_path):
        """Test saving and loading a schedule."""
        store = ScheduleStore(storage_dir=tmp_path)

        schedule = Schedule(
            schedule_id="test_schedule",
            name="Test Schedule",
            schedule_type=ScheduleType.CRON,
            schedule_definition=CronSchedule(cron_expression="0 0 * * *"),
            asset_graph=AssetGraph(name="test_graph"),
            status=ScheduleStatus.ACTIVE,
            timezone="UTC",
        )

        store.save_schedule(schedule)
        loaded = store.load_schedule("test_schedule")

        assert loaded is not None
        assert loaded.schedule_id == "test_schedule"
        assert loaded.name == "Test Schedule"
        assert loaded.status == ScheduleStatus.ACTIVE

    def test_list_schedules(self, tmp_path):
        """Test listing schedules."""
        store = ScheduleStore(storage_dir=tmp_path)

        # Create multiple schedules
        for i in range(3):
            schedule = Schedule(
                schedule_id=f"schedule_{i}",
                name=f"Schedule {i}",
                schedule_type=ScheduleType.CRON,
                schedule_definition=CronSchedule(cron_expression=f"0 {i} * * *"),
                asset_graph=AssetGraph(name=f"test_graph_{i}"),
                status=ScheduleStatus.ACTIVE,
            )
            store.save_schedule(schedule)

        # List all schedules
        schedules = store.list_schedules()
        assert len(schedules) == 3

        # Filter by status
        active_schedules = store.list_schedules(status=ScheduleStatus.ACTIVE)
        assert len(active_schedules) == 3

    def test_delete_schedule(self, tmp_path):
        """Test deleting a schedule."""
        store = ScheduleStore(storage_dir=tmp_path)

        schedule = Schedule(
            schedule_id="test_schedule",
            name="Test Schedule",
            schedule_type=ScheduleType.CRON,
            schedule_definition=CronSchedule(cron_expression="0 0 * * *"),
            asset_graph=AssetGraph(name="test_graph"),
            status=ScheduleStatus.ACTIVE,
        )

        store.save_schedule(schedule)

        # Delete schedule
        result = store.delete_schedule("test_schedule")
        assert result is True

        # Verify it's gone
        loaded = store.load_schedule("test_schedule")
        assert loaded is None

    def test_save_and_load_event(self, tmp_path):
        """Test saving and loading schedule events."""
        store = ScheduleStore(storage_dir=tmp_path)

        event = ScheduleEvent(
            event_id="test_event",
            schedule_id="test_schedule",
            trigger_type=TriggerType.SCHEDULED,
            triggered_at=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
            run_id="test_run",
            status="success",
        )

        store.save_event(event)

        # Load events
        events = store.get_events(schedule_id="test_schedule")
        assert len(events) == 1
        assert events[0].event_id == "test_event"
        assert events[0].status == "success"

    def test_get_last_event(self, tmp_path):
        """Test getting the last event for a schedule."""
        store = ScheduleStore(storage_dir=tmp_path)

        # Create multiple events
        for i in range(3):
            event = ScheduleEvent(
                event_id=f"event_{i}",
                schedule_id="test_schedule",
                trigger_type=TriggerType.SCHEDULED,
                triggered_at=datetime(2024, 1, 1, i, tzinfo=timezone.utc),
                run_id=f"run_{i}",
                status="success",
            )
            store.save_event(event)

        # Get last event
        last_event = store.get_last_event("test_schedule")
        assert last_event is not None
        assert last_event.event_id == "event_2"
