"""Tests for schedule definitions."""

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from vibe_piper.scheduling import CronSchedule, EventTrigger, IntervalSchedule


class TestCronSchedule:
    """Tests for CronSchedule."""

    def test_parse_valid_cron_expression(self):
        """Test parsing a valid cron expression."""
        schedule = CronSchedule(cron_expression="0 0 * * *")
        assert schedule.minute == (0,)
        assert schedule.hour == (0,)
        assert len(schedule.day_of_month) == 31
        assert len(schedule.month) == 12
        assert len(schedule.day_of_week) == 7

    def test_parse_range(self):
        """Test parsing a range in cron expression."""
        schedule = CronSchedule(cron_expression="0 9-17 * * *")
        assert schedule.hour == tuple(range(9, 18))

    def test_parse_list(self):
        """Test parsing a list in cron expression."""
        schedule = CronSchedule(cron_expression="0 9,12,15 * * *")
        assert schedule.hour == (9, 12, 15)

    def test_parse_step(self):
        """Test parsing a step in cron expression."""
        schedule = CronSchedule(cron_expression="*/5 * * * *")
        assert schedule.minute == tuple(range(0, 60, 5))

    def test_invalid_cron_expression(self):
        """Test that invalid cron expressions raise errors."""
        with pytest.raises(ValueError):
            CronSchedule(cron_expression="invalid")

        with pytest.raises(ValueError):
            CronSchedule(cron_expression="0 0 * *")

    def test_should_trigger(self):
        """Test should_trigger logic."""
        schedule = CronSchedule(cron_expression="0 0 * * *")
        matching_time = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
        assert schedule.should_trigger(None, matching_time, ZoneInfo("UTC"))

        non_matching_time = datetime(2024, 1, 1, 1, 0, tzinfo=timezone.utc)
        assert not schedule.should_trigger(None, non_matching_time, ZoneInfo("UTC"))

    def test_get_next_trigger_time(self):
        """Test getting next trigger time."""
        schedule = CronSchedule(cron_expression="0 0 * * *")
        from_time = datetime(2024, 1, 1, 0, 30, tzinfo=timezone.utc)
        next_trigger = schedule.get_next_trigger_time(from_time, ZoneInfo("UTC"))
        assert next_trigger.hour == 0
        assert next_trigger.minute == 0
        assert next_trigger > from_time


class TestIntervalSchedule:
    """Tests for IntervalSchedule."""

    def test_parse_minutes(self):
        """Test parsing minutes interval."""
        schedule = IntervalSchedule(interval="5m")
        assert schedule.interval == "5m"
        assert schedule.interval_seconds == 300.0

    def test_should_trigger(self):
        """Test should_trigger logic."""
        schedule = IntervalSchedule(interval="1h")
        now = datetime(2024, 1, 1, 1, 0, tzinfo=timezone.utc)

        # Never triggered before
        assert schedule.should_trigger(None, now, ZoneInfo("UTC"))

        # Triggered 30 minutes ago
        last_triggered = datetime(2024, 1, 1, 0, 30, tzinfo=timezone.utc)
        assert schedule.should_trigger(last_triggered, now, ZoneInfo("UTC"))

        # Triggered 2 hours ago
        last_triggered = datetime(2023, 12, 31, 23, 0, tzinfo=timezone.utc)
        assert schedule.should_trigger(last_triggered, now, ZoneInfo("UTC"))

        # Triggered 30 minutes ago (too soon)
        last_triggered = datetime(2024, 1, 1, 0, 50, tzinfo=timezone.utc)
        assert not schedule.should_trigger(last_triggered, now, ZoneInfo("UTC"))

    def test_get_next_trigger_time(self):
        """Test getting next trigger time."""
        schedule = IntervalSchedule(interval="30m")
        from_time = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
        next_trigger = schedule.get_next_trigger_time(from_time, ZoneInfo("UTC"))
        expected = datetime(2024, 1, 1, 0, 30, tzinfo=timezone.utc)
        assert next_trigger == expected


class TestEventTrigger:
    """Tests for EventTrigger."""

    def test_create_event_trigger(self):
        """Test creating an event trigger."""
        trigger = EventTrigger(event_type="webhook", debounce_seconds=60)
        assert trigger.event_type == "webhook"
        assert trigger.debounce_seconds == 60

    def test_should_trigger_event(self):
        """Test should_trigger_event logic."""
        trigger = EventTrigger(event_type="data_available", debounce_seconds=60)
        event_data = {"event_type": "data_available", "source": "s3"}

        # No last trigger
        assert trigger.should_trigger_event(event_data, None)

        # Last trigger was 2 minutes ago (debounce passed)
        last_triggered = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
        from datetime import datetime as dt

        now = dt.now(dt.timezone.utc)
        if last_triggered.tzinfo is None:
            last_triggered_utc = last_triggered.replace(tzinfo=dt.timezone.utc)
        else:
            last_triggered_utc = last_triggered

        assert trigger.should_trigger_event(event_data, last_triggered_utc)

        # Last trigger was 30 seconds ago (debounce not passed)
        last_triggered = datetime(2024, 1, 1, 0, 1, 30, tzinfo=timezone.utc)
        assert not trigger.should_trigger_event(event_data, last_triggered)
