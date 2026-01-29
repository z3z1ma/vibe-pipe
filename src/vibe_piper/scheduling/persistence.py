"""
Persistence layer for schedule storage and history.

This module provides storage capabilities for schedules, events,
and backfill tasks. It uses file-based storage (JSON) for simplicity
but can be extended to use database backends.
"""

import json
import logging
from collections.abc import Mapping
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from vibe_piper.scheduling.base import (
    BackfillConfig,
    BackfillStatus,
    BackfillTask,
    Schedule,
    ScheduleEvent,
    ScheduleStatus,
)

logger = logging.getLogger(__name__)

# =============================================================================
# Schedule Store
# =============================================================================


class ScheduleStore:
    """
    Storage backend for schedules and scheduling history.

    Provides CRUD operations for schedules, schedule events, and backfill tasks.
    Uses file-based storage (JSON) by default.

    Attributes:
        storage_dir: Directory where schedule data is stored
    """

    def __init__(self, storage_dir: str | Path = ".vibe_piper/schedules") -> None:
        """
        Initialize the schedule store.

        Args:
            storage_dir: Directory for storing schedule data
        """
        self.storage_dir = Path(storage_dir)
        self._ensure_storage_dirs()

    def _ensure_storage_dirs(self) -> None:
        """Create storage directories if they don't exist."""
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        (self.storage_dir / "schedules").mkdir(exist_ok=True)
        (self.storage_dir / "events").mkdir(exist_ok=True)
        (self.storage_dir / "backfills").mkdir(exist_ok=True)

    # =============================================================================
    # Schedule CRUD
    # =============================================================================

    def save_schedule(self, schedule: Schedule) -> None:
        """
        Save a schedule to storage.

        Args:
            schedule: The schedule to save
        """
        file_path = self.storage_dir / "schedules" / f"{schedule.schedule_id}.json"

        # Convert schedule to dict (handle non-serializable types)
        schedule_dict = self._schedule_to_dict(schedule)

        with open(file_path, "w") as f:
            json.dump(schedule_dict, f, indent=2, default=str)

        logger.info(f"Saved schedule '{schedule.name}' ({schedule.schedule_id})")

    def load_schedule(self, schedule_id: str) -> Schedule | None:
        """
        Load a schedule from storage.

        Args:
            schedule_id: The ID of the schedule to load

        Returns:
            The loaded Schedule, or None if not found
        """
        file_path = self.storage_dir / "schedules" / f"{schedule_id}.json"

        if not file_path.exists():
            return None

        with open(file_path, "r") as f:
            data = json.load(f)

        return self._dict_to_schedule(data)

    def list_schedules(
        self,
        status: ScheduleStatus | None = None,
    ) -> list[Schedule]:
        """
        List all schedules, optionally filtered by status.

        Args:
            status: Optional status filter

        Returns:
            List of schedules
        """
        schedules_dir = self.storage_dir / "schedules"
        schedules = []

        for file_path in schedules_dir.glob("*.json"):
            with open(file_path, "r") as f:
                data = json.load(f)

            schedule = self._dict_to_schedule(data)

            if status is None or schedule.status == status:
                schedules.append(schedule)

        return schedules

    def delete_schedule(self, schedule_id: str) -> bool:
        """
        Delete a schedule from storage.

        Args:
            schedule_id: The ID of the schedule to delete

        Returns:
            True if deleted, False if not found
        """
        file_path = self.storage_dir / "schedules" / f"{schedule_id}.json"

        if not file_path.exists():
            return False

        file_path.unlink()
        logger.info(f"Deleted schedule {schedule_id}")

        return True

    # =============================================================================
    # Schedule Events
    # =============================================================================

    def save_event(self, event: ScheduleEvent) -> None:
        """
        Save a schedule event to storage.

        Args:
            event: The event to save
        """
        events_file = self.storage_dir / "events" / "events.jsonl"

        # Append event to JSONL file
        event_dict = asdict(event)
        event_dict["triggered_at"] = event.triggered_at.isoformat()

        with open(events_file, "a") as f:
            f.write(json.dumps(event_dict, default=str) + "\n")

        logger.debug(f"Saved event {event.event_id} for schedule {event.schedule_id}")

    def get_events(
        self,
        schedule_id: str | None = None,
        limit: int = 100,
    ) -> list[ScheduleEvent]:
        """
        Get schedule events, optionally filtered by schedule.

        Args:
            schedule_id: Optional schedule ID filter
            limit: Maximum number of events to return

        Returns:
            List of events, sorted by triggered_at descending
        """
        events_file = self.storage_dir / "events" / "events.jsonl"

        if not events_file.exists():
            return []

        events = []

        with open(events_file, "r") as f:
            for line in f:
                data = json.loads(line.strip())
                event = self._dict_to_event(data)

                if schedule_id is None or event.schedule_id == schedule_id:
                    events.append(event)

        # Sort by triggered_at descending and limit
        events.sort(key=lambda e: e.triggered_at, reverse=True)

        return events[:limit]

    def get_last_event(self, schedule_id: str) -> ScheduleEvent | None:
        """
        Get the last event for a schedule.

        Args:
            schedule_id: The schedule ID

        Returns:
            The last event, or None if no events exist
        """
        events = self.get_events(schedule_id=schedule_id, limit=1)
        return events[0] if events else None

    # =============================================================================
    # Backfill Tasks
    # =============================================================================

    def save_backfill_config(self, config: BackfillConfig) -> None:
        """
        Save a backfill configuration.

        Args:
            config: The backfill configuration to save
        """
        file_path = self.storage_dir / "backfills" / f"{config.backfill_id}.json"

        config_dict = asdict(config)
        config_dict["start_date"] = config.start_date.isoformat()
        config_dict["end_date"] = config.end_date.isoformat()

        with open(file_path, "w") as f:
            json.dump(config_dict, f, indent=2)

        logger.info(f"Saved backfill config {config.backfill_id} for schedule {config.schedule_id}")

    def load_backfill_config(self, backfill_id: str) -> BackfillConfig | None:
        """
        Load a backfill configuration.

        Args:
            backfill_id: The backfill ID to load

        Returns:
            The BackfillConfig, or None if not found
        """
        file_path = self.storage_dir / "backfills" / f"{backfill_id}.json"

        if not file_path.exists():
            return None

        with open(file_path, "r") as f:
            data = json.load(f)

        data["start_date"] = datetime.fromisoformat(data["start_date"])
        data["end_date"] = datetime.fromisoformat(data["end_date"])

        return BackfillConfig(**data)

    def save_backfill_task(self, task: BackfillTask) -> None:
        """
        Save a backfill task.

        Args:
            task: The backfill task to save
        """
        file_path = self.storage_dir / "backfills" / f"{task.backfill_id}_{task.task_id}.json"

        task_dict = asdict(task)

        # Convert datetime fields
        task_dict["scheduled_for"] = task.scheduled_for.isoformat()
        if task.started_at:
            task_dict["started_at"] = task.started_at.isoformat()
        if task.completed_at:
            task_dict["completed_at"] = task.completed_at.isoformat()

        with open(file_path, "w") as f:
            json.dump(task_dict, f, indent=2)

        logger.debug(f"Saved backfill task {task.task_id}")

    def load_backfill_task(self, backfill_id: str, task_id: str) -> BackfillTask | None:
        """
        Load a backfill task.

        Args:
            backfill_id: The backfill ID
            task_id: The task ID

        Returns:
            The BackfillTask, or None if not found
        """
        file_path = self.storage_dir / "backfills" / f"{backfill_id}_{task_id}.json"

        if not file_path.exists():
            return None

        with open(file_path, "r") as f:
            data = json.load(f)

        # Convert datetime fields
        data["scheduled_for"] = datetime.fromisoformat(data["scheduled_for"])
        if data.get("started_at"):
            data["started_at"] = datetime.fromisoformat(data["started_at"])
        if data.get("completed_at"):
            data["completed_at"] = datetime.fromisoformat(data["completed_at"])

        return BackfillTask(**data)

    def list_backfill_tasks(
        self,
        backfill_id: str,
        status: BackfillStatus | None = None,
    ) -> list[BackfillTask]:
        """
        List backfill tasks for a backfill.

        Args:
            backfill_id: The backfill ID
            status: Optional status filter

        Returns:
            List of backfill tasks
        """
        tasks = []

        for file_path in (self.storage_dir / "backfills").glob(f"{backfill_id}_*.json"):
            with open(file_path, "r") as f:
                data = json.load(f)

            # Skip config files
            if file_path.stem == backfill_id:
                continue

            # Convert datetime fields
            data["scheduled_for"] = datetime.fromisoformat(data["scheduled_for"])
            if data.get("started_at"):
                data["started_at"] = datetime.fromisoformat(data["started_at"])
            if data.get("completed_at"):
                data["completed_at"] = datetime.fromisoformat(data["completed_at"])

            task = BackfillTask(**data)

            if status is None or task.status == status:
                tasks.append(task)

        return tasks

    # =============================================================================
    # Utility Methods
    # =============================================================================

    def _schedule_to_dict(self, schedule: Schedule) -> Mapping[str, Any]:
        """Convert a Schedule to a dictionary for JSON serialization."""
        data = asdict(schedule)

        # Convert datetime fields
        data["created_at"] = schedule.created_at.isoformat()
        data["updated_at"] = schedule.updated_at.isoformat()

        # Convert schedule definition
        if schedule.schedule_definition:
            data["schedule_definition"] = schedule.schedule_definition.to_dict()

        return data

    def _dict_to_schedule(self, data: Mapping[str, Any]) -> Schedule:
        """Convert a dictionary to a Schedule object."""
        # Import here to avoid circular dependency
        from vibe_piper.scheduling.schedules import (
            CronSchedule,
            EventTrigger,
            IntervalSchedule,
        )

        # Convert datetime fields
        data = dict(data)
        data["created_at"] = datetime.fromisoformat(data["created_at"])
        data["updated_at"] = datetime.fromisoformat(data["updated_at"])

        # Convert schedule definition
        def_data = data.get("schedule_definition", {})
        if def_data.get("type") == "cron":
            data["schedule_definition"] = CronSchedule.from_dict(def_data)
        elif def_data.get("type") == "interval":
            data["schedule_definition"] = IntervalSchedule.from_dict(def_data)
        elif def_data.get("type") == "event":
            data["schedule_definition"] = EventTrigger.from_dict(def_data)
        else:
            msg = f"Unknown schedule definition type: {def_data.get('type')}"
            raise ValueError(msg)

        # Import AssetGraph to avoid circular import

        # AssetGraph is stored as dict, need to reconstruct
        # For now, we'll store the graph definition and reconstruct later
        # This is a simplification - in production, use proper serialization
        if "asset_graph" in data and isinstance(data["asset_graph"], dict):
            # Store the dict for now, will be reconstructed by the scheduler
            pass

        return Schedule(**data)

    def _dict_to_event(self, data: Mapping[str, Any]) -> ScheduleEvent:
        """Convert a dictionary to a ScheduleEvent object."""
        data = dict(data)
        data["triggered_at"] = datetime.fromisoformat(data["triggered_at"])

        return ScheduleEvent(**data)
