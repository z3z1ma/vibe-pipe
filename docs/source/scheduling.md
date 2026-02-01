# Scheduling System

The scheduling system provides comprehensive capabilities for automating and managing pipeline executions.

## Overview

The scheduling system consists of:

- **Schedule Definitions**: Cron-like, interval-based, and event-driven triggers
- **Scheduler Engine**: Manages schedules, checks for triggers, and executes pipelines
- **Schedule Store**: Persistent storage for schedules, events, and backfill data
- **Backfill Manager**: Handles historical data backfill operations

## Schedule Types

### Cron Schedules

Cron schedules use standard cron expression syntax with 5 fields:
- minute (0-59)
- hour (0-23)
- day_of_month (1-31)
- month (1-12)
- day_of_week (0-6, where 0=Sunday)

**Syntax Examples:**
- `0 0 * * *` - Daily at midnight
- `0 */6 * * *` - Every 6 hours
- `30 9 * * 1-5` - 9:30 AM on weekdays (Monday-Friday)
- `0 0 1 * *` - Monthly on the 1st at midnight

**Special Characters:**
- `*` - Match any value
- `x-y` - Range (e.g., `1-5` for Monday-Friday)
- `x,y` - List (e.g., `1,3,5` for specific values)
- `*/x` - Step (e.g., `*/5` for every 5)

### Interval Schedules

Interval schedules trigger at regular intervals.

**Format:** `<number><unit>` where unit is:
- `s` - seconds
- `m` - minutes
- `h` - hours
- `d` - days

**Examples:**
- `5s` - Every 5 seconds
- `30m` - Every 30 minutes
- `1h` - Every hour
- `1d` - Every day

### Event Triggers

Event triggers fire when specific events occur, such as:
- Webhook callbacks
- Data availability events
- Pipeline completion events
- Custom events

**Features:**
- Event type filtering
- Optional event filter functions for custom logic
- Debounce support to prevent rapid-fire triggers

## Usage

### Creating a Scheduler

```python
from vibe_piper import Scheduler, SchedulerConfig
from vibe_piper.types import AssetGraph

# Create scheduler with custom config
config = SchedulerConfig(
    check_interval_seconds=30,  # Check every 30 seconds
    max_concurrent_runs=3,      # Max 3 concurrent pipeline runs
    timezone="UTC",              # Default timezone
)

scheduler = Scheduler(config=config)

# Start the scheduler
scheduler.start()
```

### Adding Schedules

#### Cron Schedule

```python
from vibe_piper import Schedule, ScheduleType, CronSchedule
from vibe_piper.types import AssetGraph

# Create a daily schedule
schedule = Schedule(
    schedule_id="daily_report",
    name="Daily Report",
    schedule_type=ScheduleType.CRON,
    schedule_definition=CronSchedule(
        cron_expression="0 9 * * *",  # 9 AM daily
        description="Generate daily report at 9 AM",
    ),
    asset_graph=AssetGraph(name="report_pipeline"),
    timezone="America/New_York",
)

scheduler.add_schedule(schedule)
```

#### Interval Schedule

```python
from vibe_piper import Schedule, ScheduleType, IntervalSchedule

# Create an hourly schedule
schedule = Schedule(
    schedule_id="hourly_sync",
    name="Hourly Data Sync",
    schedule_type=ScheduleType.INTERVAL,
    schedule_definition=IntervalSchedule(
        interval="1h",
        description="Sync data every hour",
    ),
    asset_graph=AssetGraph(name="sync_pipeline"),
    timezone="UTC",
)

scheduler.add_schedule(schedule)
```

#### Event Trigger

```python
from vibe_piper import Schedule, ScheduleType, EventTrigger

# Create an event-driven schedule
schedule = Schedule(
    schedule_id="data_arrival",
    name="Process New Data",
    schedule_type=ScheduleType.EVENT,
    schedule_definition=EventTrigger(
        event_type="data_available",
        debounce_seconds=60,  # Don't trigger more than once per minute
        description="Process data when new data arrives",
    ),
    asset_graph=AssetGraph(name="process_pipeline"),
    timezone="UTC",
)

scheduler.add_schedule(schedule)

# Trigger the event
from vibe_piper import TriggerEvent

event = TriggerEvent(
    event_type="data_available",
    event_data={"source": "s3", "bucket": "my-bucket"},
    source="external",
)

scheduler.trigger_event(event)
```

### Managing Schedules

```python
# List all schedules
schedules = scheduler.list_schedules()

# List only active schedules
active_schedules = scheduler.list_schedules(status=ScheduleStatus.ACTIVE)

# Pause a schedule
scheduler.pause_schedule("daily_report")

# Resume a schedule
scheduler.resume_schedule("daily_report")

# Delete a schedule
scheduler.delete_schedule("daily_report")
```

## Backfill

Backfill allows you to re-execute schedules for historical dates.

```python
from datetime import datetime

# Create a backfill
backfill = scheduler.create_backfill(
    schedule_id="daily_report",
    start_date=datetime(2024, 1, 1),  # January 1, 2024
    end_date=datetime(2024, 1, 31),   # January 31, 2024
    timezone="UTC",
    parallel=True,      # Run tasks in parallel
    max_parallel=5,   # Up to 5 concurrent tasks
)

# Execute the backfill
result = scheduler.execute_backfill(backfill.backfill_id)

print(f"Backfill completed: {result.tasks_succeeded} succeeded, {result.tasks_failed} failed")
```

## Persistence

Schedules, events, and backfill data are persisted to `.vibe_piper/schedules/` by default.

**Structure:**
```
.vibe_piper/schedules/
├── schedules/          # Schedule definitions (JSON)
├── events/            # Schedule events (JSONL)
└── backfills/         # Backfill configs and tasks (JSON)
```

## Timezone Support

All schedules support timezone-aware execution:

```python
schedule = Schedule(
    schedule_id="report",
    schedule_type=ScheduleType.CRON,
    schedule_definition=CronSchedule(cron_expression="0 9 * * *"),
    asset_graph=AssetGraph(name="report_pipeline"),
    timezone="America/New_York",  # Schedule in Eastern Time
)
```

The scheduler automatically converts times to the schedule's timezone before checking triggers.

## Event History

Track all schedule triggers:

```python
# Get events for a specific schedule
events = scheduler.store.get_events(schedule_id="daily_report", limit=100)

# Get the last event for a schedule
last_event = scheduler.store.get_last_event("daily_report")
```

## API Reference

### Scheduler

| Method | Description |
|---------|-------------|
| `start()` | Start the scheduler loop |
| `stop()` | Stop the scheduler loop |
| `is_running()` | Check if scheduler is running |
| `add_schedule(schedule)` | Add a schedule |
| `get_schedule(schedule_id)` | Get a schedule by ID |
| `list_schedules(status)` | List schedules, optionally filtered |
| `pause_schedule(schedule_id)` | Pause a schedule |
| `resume_schedule(schedule_id)` | Resume a paused schedule |
| `delete_schedule(schedule_id)` | Delete a schedule |
| `trigger_event(event)` | Submit an event for event-driven schedules |
| `create_backfill(...)` | Create a backfill configuration |
| `execute_backfill(backfill_id)` | Execute a backfill |

### ScheduleStatus

- `ACTIVE` - Schedule is active and will trigger
- `PAUSED` - Schedule is paused
- `DISABLED` - Schedule is disabled
- `COMPLETED` - Schedule has completed (one-time schedules)

### TriggerType

- `SCHEDULED` - Triggered by schedule (cron/interval)
- `EVENT` - Triggered by event
- `MANUAL` - Manually triggered
- `BACKFILL` - Triggered by backfill

## Best Practices

1. **Use descriptive schedule IDs and names** for easier debugging
2. **Set appropriate intervals** to avoid overloading your system
3. **Use debounce for event triggers** to prevent rapid-fire executions
4. **Monitor event history** to identify patterns and issues
5. **Use timezones correctly** for geographically distributed systems
6. **Limit concurrent runs** to prevent resource exhaustion
7. **Backfill with caution** - historical backfills can be resource-intensive

## Limitations

- Cron expressions support 5 fields (standard cron format)
- Backfill has a safety limit of 10,000 tasks
- Event-driven schedules cannot be backfilled
- Persistence is file-based (JSON) - for production use, consider database backend
