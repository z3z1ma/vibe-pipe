"""
Schedule definitions for different trigger types.

This module provides concrete schedule definition classes for:
- Cron-like schedules (e.g., "0 0 * * *" for daily at midnight)
- Interval-based schedules (e.g., "1h" for hourly, "30m" for every 30 minutes)
- Event-driven triggers (e.g., webhooks, data availability)
"""

from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

# =============================================================================
# Abstract Base Class
# =============================================================================


class ScheduleDefinition(ABC):
    """
    Abstract base class for schedule definitions.

    All schedule types (CronSchedule, IntervalSchedule, EventTrigger)
    inherit from this base and implement the required methods.
    """

    @abstractmethod
    def should_trigger(
        self,
        last_triggered: datetime | None,
        now: datetime,
        timezone: ZoneInfo,
    ) -> bool:
        """
        Check if this schedule should trigger at the current time.

        Args:
            last_triggered: The last time this schedule triggered
            now: The current time to check against
            timezone: The timezone for the schedule

        Returns:
            True if the schedule should trigger, False otherwise
        """
        ...

    @abstractmethod
    def get_next_trigger_time(
        self,
        from_time: datetime,
        timezone: ZoneInfo,
    ) -> datetime:
        """
        Get the next time this schedule should trigger.

        Args:
            from_time: The time to calculate the next trigger from
            timezone: The timezone for the schedule

        Returns:
            The next trigger time as a datetime
        """
        ...

    @abstractmethod
    def to_dict(self) -> Mapping[str, Any]:
        """
        Convert this schedule definition to a dictionary for storage.

        Returns:
            Dictionary representation of this schedule
        """
        ...

    @classmethod
    @abstractmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ScheduleDefinition":
        """
        Create a schedule definition from a dictionary.

        Args:
            data: Dictionary containing schedule configuration

        Returns:
            A ScheduleDefinition instance
        """
        ...


# =============================================================================
# Cron Schedule
# =============================================================================


@dataclass(frozen=True)
class CronSchedule(ScheduleDefinition):
    """
    A cron-like schedule definition.

    Supports standard cron syntax with 5 fields:
    - minute: 0-59
    - hour: 0-23
    - day_of_month: 1-31
    - month: 1-12
    - day_of_week: 0-6 (0=Sunday, 6=Saturday)

    Special characters:
    - *: Match any value
    - x-y: Range (e.g., 1-5 for Monday-Friday)
    - x,y: List (e.g., 1,3,5 for specific values)
    - */x: Step (e.g., */5 for every 5)

    Examples:
        - "0 0 * * *" - Daily at midnight
        - "0 */6 * * *" - Every 6 hours
        - "30 9 * * 1-5" - 9:30 AM on weekdays
        - "0 0 1 * *" - Monthly on the 1st at midnight
    """

    cron_expression: str  # e.g., "0 0 * * *"
    description: str | None = None

    # Parsed cron fields
    minute: tuple[int, ...] = field(default_factory=tuple)
    hour: tuple[int, ...] = field(default_factory=tuple)
    day_of_month: tuple[int, ...] = field(default_factory=tuple)
    month: tuple[int, ...] = field(default_factory=tuple)
    day_of_week: tuple[int, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        """Parse and validate the cron expression."""
        if not self.cron_expression:
            msg = "cron_expression cannot be empty"
            raise ValueError(msg)

        # Parse the 5 fields
        parts = self.cron_expression.split()
        if len(parts) != 5:
            msg = f"Invalid cron expression: '{self.cron_expression}'. Expected 5 fields, got {len(parts)}"
            raise ValueError(msg)

        # Parse each field
        self._parse_field(parts[0], 0, 59, "minute")
        self._parse_field(parts[1], 0, 23, "hour")
        self._parse_field(parts[2], 1, 31, "day_of_month")
        self._parse_field(parts[3], 1, 12, "month")
        self._parse_field(parts[4], 0, 6, "day_of_week")

    def _parse_field(self, field_str: str, min_val: int, max_val: int, field_name: str) -> None:
        """Parse a single cron field and set the corresponding attribute."""
        if field_str == "*":
            values = tuple(range(min_val, max_val + 1))
        elif field_str.startswith("*/"):
            # Step value
            try:
                step = int(field_str[2:])
                values = tuple(range(min_val, max_val + 1, step))
            except ValueError as e:
                msg = f"Invalid step value in {field_name}: '{field_str}'"
                raise ValueError(msg) from e
        elif "," in field_str:
            # List of values
            parts_list = field_str.split(",")
            values = self._parse_list_values(parts_list, min_val, max_val, field_name)
        elif "-" in field_str:
            # Range
            parts_range = field_str.split("-")
            if len(parts_range) != 2:
                msg = f"Invalid range in {field_name}: '{field_str}'"
                raise ValueError(msg)
            try:
                start = int(parts_range[0])
                end = int(parts_range[1])
                values = tuple(range(start, end + 1))
            except ValueError as e:
                msg = f"Invalid range values in {field_name}: '{field_str}'"
                raise ValueError(msg) from e
        else:
            # Single value
            try:
                values = (int(field_str),)
            except ValueError as e:
                msg = f"Invalid value in {field_name}: '{field_str}'"
                raise ValueError(msg) from e

        # Validate all values are within range
        for val in values:
            if val < min_val or val > max_val:
                msg = f"Value {val} in {field_name} is out of range [{min_val}, {max_val}]"
                raise ValueError(msg)

        # Set the corresponding attribute
        # We need to use object.__setattr__ because dataclass is frozen
        object.__setattr__(self, field_name, values)

    def _parse_list_values(
        self, parts: list[str], min_val: int, max_val: int, field_name: str
    ) -> tuple[int, ...]:
        """Parse a comma-separated list of values."""
        values = []
        for part in parts:
            if "-" in part:
                # Range within list
                range_parts = part.split("-")
                if len(range_parts) != 2:
                    msg = f"Invalid range in {field_name} list: '{part}'"
                    raise ValueError(msg)
                try:
                    start = int(range_parts[0])
                    end = int(range_parts[1])
                    values.extend(range(start, end + 1))
                except ValueError as e:
                    msg = f"Invalid range values in {field_name} list: '{part}'"
                    raise ValueError(msg) from e
            else:
                try:
                    values.append(int(part))
                except ValueError as e:
                    msg = f"Invalid value in {field_name} list: '{part}'"
                    raise ValueError(msg) from e
        return tuple(sorted(set(values)))

    def should_trigger(
        self,
        last_triggered: datetime | None,
        now: datetime,
        timezone: ZoneInfo,
    ) -> bool:
        """
        Check if the schedule should trigger at the current time.

        A cron schedule should trigger if:
        1. It has never triggered before, AND the current time matches
        2. The last trigger was at a different time period, AND the current time matches
        """
        # Convert to schedule timezone
        now_tz = now.astimezone(timezone)

        # Check if current time matches cron expression
        if not self._matches_time(now_tz):
            return False

        # Check if we already triggered in the current time period
        if last_triggered:
            last_tz = last_triggered.astimezone(timezone)

            # If last triggered at the exact same minute, don't trigger again
            if (
                last_tz.year == now_tz.year
                and last_tz.month == now_tz.month
                and last_tz.day == now_tz.day
                and last_tz.hour == now_tz.hour
                and last_tz.minute == now_tz.minute
            ):
                return False

        return True

    def _matches_time(self, dt: datetime) -> bool:
        """Check if datetime matches the cron expression."""
        return (
            dt.minute in self.minute
            and dt.hour in self.hour
            and dt.day in self.day_of_month
            and dt.month in self.month
            and dt.weekday() in self.day_of_week
        )

    def get_next_trigger_time(
        self,
        from_time: datetime,
        timezone: ZoneInfo,
    ) -> datetime:
        """
        Get the next time this schedule should trigger.

        Brute force approach: Check each minute from from_time onwards
        until we find a match. This is simple but not optimal for long ranges.
        """
        # Convert to schedule timezone
        from_tz = from_time.astimezone(timezone)

        # Start checking from the next minute
        check_time = from_tz + timedelta(minutes=1)

        # Safety limit: Check up to 1 year ahead
        max_time = from_tz + timedelta(days=365)

        while check_time < max_time:
            if self._matches_time(check_time):
                return check_time

            check_time += timedelta(minutes=1)

        # If we didn't find a match within a year, raise an error
        msg = "Could not find next trigger time within 1 year"
        raise ValueError(msg)

    def to_dict(self) -> Mapping[str, Any]:
        """Convert this schedule definition to a dictionary."""
        return {
            "type": "cron",
            "cron_expression": self.cron_expression,
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ScheduleDefinition":
        """Create a cron schedule from a dictionary."""
        return cls(
            cron_expression=data["cron_expression"],
            description=data.get("description"),
        )


# =============================================================================
# Interval Schedule
# =============================================================================


@dataclass(frozen=True)
class IntervalSchedule(ScheduleDefinition):
    """
    An interval-based schedule definition.

    Triggers at regular intervals (e.g., every 5 minutes, every 2 hours).
    Intervals are specified as a duration string with a unit suffix:
    - s: seconds
    - m: minutes
    - h: hours
    - d: days

    Examples:
        - "5s" - Every 5 seconds
        - "30m" - Every 30 minutes
        - "1h" - Every hour
        - "1d" - Every day
    """

    interval: str  # e.g., "5m", "1h", "30s"
    description: str | None = None
    interval_seconds: float = field(init=False)

    def __post_init__(self) -> None:
        """Parse and validate the interval string."""
        if not self.interval:
            msg = "interval cannot be empty"
            raise ValueError(msg)

        # Parse the interval string
        try:
            interval_str = self.interval.strip().lower()
            if interval_str.endswith("s"):
                seconds = float(interval_str[:-1])
            elif interval_str.endswith("m"):
                seconds = float(interval_str[:-1]) * 60
            elif interval_str.endswith("h"):
                seconds = float(interval_str[:-1]) * 3600
            elif interval_str.endswith("d"):
                seconds = float(interval_str[:-1]) * 86400
            else:
                msg = f"Invalid interval unit: '{self.interval}'. Must end with s, m, h, or d"
                raise ValueError(msg)

            if seconds <= 0:
                msg = f"Interval must be positive: '{self.interval}'"
                raise ValueError(msg)

            # Set the parsed value
            object.__setattr__(self, "interval_seconds", seconds)
        except (ValueError, IndexError) as e:
            msg = f"Invalid interval format: '{self.interval}'"
            raise ValueError(msg) from e

    def should_trigger(
        self,
        last_triggered: datetime | None,
        now: datetime,
        timezone: ZoneInfo,
    ) -> bool:
        """
        Check if the schedule should trigger at the current time.

        An interval schedule should trigger if:
        1. It has never triggered before, OR
        2. Enough time has passed since the last trigger
        """
        if last_triggered is None:
            return True

        # Calculate time since last trigger
        elapsed = (now - last_triggered).total_seconds()

        return elapsed >= self.interval_seconds

    def get_next_trigger_time(
        self,
        from_time: datetime,
        timezone: ZoneInfo,
    ) -> datetime:
        """
        Get the next time this schedule should trigger.
        """
        return from_time + timedelta(seconds=self.interval_seconds)

    def to_dict(self) -> Mapping[str, Any]:
        """Convert this schedule definition to a dictionary."""
        return {
            "type": "interval",
            "interval": self.interval,
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ScheduleDefinition":
        """Create an interval schedule from a dictionary."""
        return cls(
            interval=data["interval"],
            description=data.get("description"),
        )


# =============================================================================
# Event Trigger
# =============================================================================


@dataclass
class EventTrigger(ScheduleDefinition):
    """
    An event-driven trigger definition.

    Triggers when a specific event occurs. Events can be:
    - Webhook callbacks
    - Pipeline completion events
    - Data availability events
    - Custom events

    Attributes:
        event_type: The type of event to listen for
        event_filter: Optional filter function to determine if event should trigger
        debounce_seconds: Minimum time between triggers (to avoid rapid-fire triggers)
        description: Human-readable description
    """

    event_type: str
    event_filter: Callable[[Mapping[str, Any]], bool] | None = None
    debounce_seconds: float = 0
    description: str | None = None

    def should_trigger(
        self,
        last_triggered: datetime | None,
        now: datetime,
        timezone: ZoneInfo,
    ) -> bool:
        """
        Event triggers are handled differently - this returns False as they
        don't have a time-based trigger. Use `should_trigger_event` instead.
        """
        return False

    def should_trigger_event(
        self,
        event: Mapping[str, Any],
        last_triggered: datetime | None,
    ) -> bool:
        """
        Check if the given event should trigger this schedule.

        Args:
            event: The event data to check
            last_triggered: The last time this schedule was triggered

        Returns:
            True if the event should trigger this schedule
        """
        # Check event type matches
        if event.get("event_type") != self.event_type:
            return False

        # Apply custom filter if provided
        if self.event_filter and not self.event_filter(event):
            return False

        # Check debounce
        if last_triggered and self.debounce_seconds > 0:
            from datetime import datetime, timezone

            # Make both datetimes timezone-aware
            now = datetime.now(timezone.utc)
            if last_triggered.tzinfo is None:
                last_triggered_utc = last_triggered.replace(tzinfo=timezone.utc)
            else:
                last_triggered_utc = last_triggered

            elapsed = (now - last_triggered_utc).total_seconds()
            if elapsed < self.debounce_seconds:
                return False

        return True

    def get_next_trigger_time(
        self,
        from_time: datetime,
        timezone: ZoneInfo,
    ) -> datetime:
        """
        Event triggers don't have predictable next trigger times.
        Returns far future date.
        """
        return from_time + timedelta(days=365 * 10)

    def to_dict(self) -> Mapping[str, Any]:
        """Convert this schedule definition to a dictionary."""
        return {
            "type": "event",
            "event_type": self.event_type,
            "debounce_seconds": self.debounce_seconds,
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ScheduleDefinition":
        """Create an event trigger from a dictionary."""
        return cls(
            event_type=data["event_type"],
            debounce_seconds=data.get("debounce_seconds", 0),
            description=data.get("description"),
        )
