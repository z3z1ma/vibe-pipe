"""
Base types and enums for scheduling system.

This module defines the core types used throughout the scheduling system,
including schedule types, status enums, and data structures.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from vibe_piper.scheduling.schedules import ScheduleDefinition

# =============================================================================
# Enums
# =============================================================================


class ScheduleType(Enum):
    """Types of schedules."""

    CRON = auto()  # Cron-like schedule
    INTERVAL = auto()  # Interval-based schedule
    EVENT = auto()  # Event-driven trigger


class ScheduleStatus(Enum):
    """Status of a schedule."""

    ACTIVE = auto()  # Schedule is active and will trigger
    PAUSED = auto()  # Schedule is paused
    DISABLED = auto()  # Schedule is disabled
    COMPLETED = auto()  # Schedule has completed (for one-time schedules)


class TriggerType(Enum):
    """Types of triggers."""

    SCHEDULED = auto()  # Triggered by schedule (cron/interval)
    EVENT = auto()  # Triggered by event
    MANUAL = auto()  # Manually triggered
    BACKFILL = auto()  # Triggered by backfill


class BackfillStatus(Enum):
    """Status of a backfill task."""

    PENDING = auto()  # Backfill is pending
    RUNNING = auto()  # Backfill is running
    COMPLETED = auto()  # Backfill completed successfully
    FAILED = auto()  # Backfill failed
    PARTIAL = auto()  # Some backfill runs failed


# =============================================================================
# Data Classes
# =============================================================================


@dataclass(frozen=True)
class ScheduleEvent:
    """
    A single trigger event for a schedule.

    Represents a point in time when a schedule was triggered
    and the associated execution context.

    Attributes:
        event_id: Unique identifier for this event
        schedule_id: ID of the schedule that was triggered
        trigger_type: Type of trigger that caused this event
        triggered_at: When this event was triggered
        run_id: ID of the pipeline run that was created
        status: Status of the pipeline run (success/failure/running)
        metadata: Additional metadata about the event
    """

    event_id: str
    schedule_id: str
    trigger_type: TriggerType
    triggered_at: datetime
    run_id: str | None = None
    status: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TriggerEvent:
    """
    An event that can trigger schedules.

    Events can be external triggers (webhooks, API calls)
    or internal triggers (data changes, pipeline completions).

    Attributes:
        event_type: Type of event (webhook, pipeline_complete, data_available, etc.)
        event_data: Payload data for the event
        source: Source of the event (system, user, external service)
        timestamp: When the event occurred
        event_id: Optional unique identifier for the event
    """

    event_type: str
    event_data: Mapping[str, Any] = field(default_factory=dict)
    source: str = "system"
    timestamp: datetime = field(default_factory=datetime.utcnow)
    event_id: str | None = None


@dataclass
class Schedule:
    """
    A schedule definition for executing pipelines.

    Schedules define when and how pipelines should be executed.
    They can be triggered by time-based schedules (cron/interval)
    or by events (webhooks, data availability).

    Attributes:
        schedule_id: Unique identifier for this schedule
        name: Human-readable name for this schedule
        schedule_type: Type of schedule (cron/interval/event)
        schedule_definition: The schedule definition (CronSchedule, IntervalSchedule, or EventTrigger)
        asset_graph: The asset graph to execute when triggered
        status: Current status of the schedule
        timezone: Timezone for schedule interpretation (default: UTC)
        config: Additional configuration for the schedule
        created_at: When this schedule was created
        updated_at: When this schedule was last updated
    """

    schedule_id: str
    name: str
    schedule_type: ScheduleType
    schedule_definition: "ScheduleDefinition"
    asset_graph: Any  # AssetGraph from types.py - avoid circular import
    status: ScheduleStatus = ScheduleStatus.ACTIVE
    timezone: str = "UTC"
    config: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    def __post_init__(self) -> None:
        """Validate the schedule configuration."""
        if not self.schedule_id:
            msg = "schedule_id cannot be empty"
            raise ValueError(msg)
        if not self.name:
            msg = "name cannot be empty"
            raise ValueError(msg)

        # Validate timezone
        try:
            ZoneInfo(self.timezone)
        except Exception as e:
            msg = f"Invalid timezone '{self.timezone}': {e}"
            raise ValueError(msg) from e

    def get_timezone(self) -> ZoneInfo:
        """Get the timezone as a ZoneInfo object."""
        return ZoneInfo(self.timezone)

    def is_active(self) -> bool:
        """Check if this schedule is currently active."""
        return self.status == ScheduleStatus.ACTIVE


@dataclass
class BackfillConfig:
    """
    Configuration for backfilling historical data.

    Backfill allows you to execute a schedule for historical dates
    that were missed or need to be re-run.

    Attributes:
        backfill_id: Unique identifier for this backfill
        schedule_id: ID of the schedule to backfill
        start_date: Start date for backfill
        end_date: End date for backfill
        timezone: Timezone for backfill execution
        parallel: Whether to run backfill tasks in parallel
        max_parallel: Maximum number of parallel tasks (if parallel=True)
        on_failure: Strategy for handling failures (fail_fast, continue, retry)
        retry_attempts: Number of retry attempts for failed tasks
        config: Additional configuration for backfill
    """

    backfill_id: str
    schedule_id: str
    start_date: datetime
    end_date: datetime
    timezone: str = "UTC"
    parallel: bool = False
    max_parallel: int = 3
    on_failure: str = "fail_fast"  # fail_fast, continue, retry
    retry_attempts: int = 3
    config: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate the backfill configuration."""
        if not self.backfill_id:
            msg = "backfill_id cannot be empty"
            raise ValueError(msg)
        if not self.schedule_id:
            msg = "schedule_id cannot be empty"
            raise ValueError(msg)
        if self.start_date > self.end_date:
            msg = "start_date must be before or equal to end_date"
            raise ValueError(msg)
        if self.parallel and self.max_parallel < 1:
            msg = "max_parallel must be at least 1 when parallel=True"
            raise ValueError(msg)


@dataclass
class BackfillTask:
    """
    A single task in a backfill operation.

    Each backfill consists of one or more tasks, each representing
    a single scheduled execution for a specific point in time.

    Attributes:
        task_id: Unique identifier for this task
        backfill_id: ID of the parent backfill
        schedule_id: ID of the schedule being backfilled
        scheduled_for: When this task was scheduled for (historical date)
        status: Current status of this task
        run_id: ID of the pipeline run (if executed)
        error: Error message if task failed
        started_at: When the task started running
        completed_at: When the task completed (or failed)
        retry_count: Number of retry attempts made
    """

    task_id: str
    backfill_id: str
    schedule_id: str
    scheduled_for: datetime
    status: BackfillStatus = BackfillStatus.PENDING
    run_id: str | None = None
    error: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    retry_count: int = 0

    def __post_init__(self) -> None:
        """Validate the backfill task."""
        if not self.task_id:
            msg = "task_id cannot be empty"
            raise ValueError(msg)
        if not self.backfill_id:
            msg = "backfill_id cannot be empty"
            raise ValueError(msg)
        if not self.schedule_id:
            msg = "schedule_id cannot be empty"
            raise ValueError(msg)
