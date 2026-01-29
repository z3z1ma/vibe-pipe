"""
Backfill management for historical data processing.

This module provides functionality for backfilling historical data,
allowing you to re-run schedules for dates that were missed
or need to be re-processed.
"""

import logging
import uuid
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from vibe_piper.scheduling.base import (
    BackfillConfig,
    BackfillStatus,
    BackfillTask,
    Schedule,
)
from vibe_piper.scheduling.persistence import ScheduleStore

if TYPE_CHECKING:
    from vibe_piper.scheduling.schedules import CronSchedule, IntervalSchedule

logger = logging.getLogger(__name__)


@dataclass
class BackfillResult:
    """
    Result of a backfill operation.

    Attributes:
        backfill_id: ID of the backfill
        schedule_id: ID of the schedule that was backfilled
        tasks_created: Number of backfill tasks created
        tasks_succeeded: Number of tasks that succeeded
        tasks_failed: Number of tasks that failed
        duration_ms: Total duration of backfill in milliseconds
        started_at: When backfill started
        completed_at: When backfill completed
    """

    backfill_id: str
    schedule_id: str
    tasks_created: int = 0
    tasks_succeeded: int = 0
    tasks_failed: int = 0
    duration_ms: float = 0.0
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: datetime | None = None


class BackfillManager:
    """
    Manager for executing backfill operations.

    Handles creating, executing, and tracking backfill tasks
    for schedules.

    Attributes:
        store: The ScheduleStore for persistence
        executor: Optional custom executor for running backfill tasks
    """

    def __init__(
        self,
        store: ScheduleStore | None = None,
        executor: Any | None = None,  # ExecutionEngine
    ) -> None:
        """
        Initialize the backfill manager.

        Args:
            store: ScheduleStore for persistence (created if None)
            executor: Optional ExecutionEngine for running pipelines
        """
        self.store = store or ScheduleStore()
        self.executor = executor

    def create_backfill(
        self,
        schedule: Schedule,
        start_date: datetime,
        end_date: datetime,
        timezone: str = "UTC",
        parallel: bool = False,
        max_parallel: int = 3,
    ) -> BackfillConfig:
        """
        Create a backfill configuration for a schedule.

        Args:
            schedule: The schedule to backfill
            start_date: Start date for backfill
            end_date: End date for backfill
            timezone: Timezone for backfill execution
            parallel: Whether to run tasks in parallel
            max_parallel: Maximum parallel tasks (if parallel=True)

        Returns:
            BackfillConfig for the backfill
        """
        backfill_id = f"bf_{uuid.uuid4().hex[:12]}"

        config = BackfillConfig(
            backfill_id=backfill_id,
            schedule_id=schedule.schedule_id,
            start_date=start_date,
            end_date=end_date,
            timezone=timezone,
            parallel=parallel,
            max_parallel=max_parallel,
        )

        self.store.save_backfill_config(config)
        logger.info(
            f"Created backfill {backfill_id} for schedule {schedule.schedule_id} "
            f"from {start_date} to {end_date}"
        )

        return config

    def generate_backfill_tasks(
        self,
        config: BackfillConfig,
        schedule: Schedule,
    ) -> Sequence[BackfillTask]:
        """
        Generate backfill tasks for a schedule.

        The tasks are generated based on the schedule type:
        - Cron schedules: Tasks for each matching time slot
        - Interval schedules: Tasks at regular intervals
        - Event triggers: Not applicable (returns empty)

        Args:
            config: The backfill configuration
            schedule: The schedule to generate tasks for

        Returns:
            List of BackfillTask objects
        """
        tasks: list[BackfillTask] = []

        # Get schedule timezone
        from zoneinfo import ZoneInfo

        tz = ZoneInfo(config.timezone)

        # Normalize dates to schedule timezone
        start = config.start_date.astimezone(tz)
        end = config.end_date.astimezone(tz)

        # Generate tasks based on schedule type
        if isinstance(schedule.schedule_definition, CronSchedule):
            # Cron schedule
            tasks = self._generate_cron_backfill_tasks(config, schedule, start, end, tz)
        elif isinstance(schedule.schedule_definition, IntervalSchedule):
            # Interval schedule
            tasks = self._generate_interval_backfill_tasks(config, schedule, start, end, tz)
        else:
            # Event triggers - no backfill possible
            logger.warning(
                f"Schedule {schedule.schedule_id} is event-driven, backfill not applicable"
            )
            return []

        # Save all tasks
        for task in tasks:
            self.store.save_backfill_task(task)

        logger.info(f"Generated {len(tasks)} backfill tasks for schedule {schedule.schedule_id}")

        return tasks

    def _generate_cron_backfill_tasks(
        self,
        config: BackfillConfig,
        schedule: Schedule,
        start: datetime,
        end: datetime,
        tz: Any,
    ) -> list[BackfillTask]:
        """Generate backfill tasks for a cron schedule."""
        tasks = []
        current = start

        # Safety limit: generate max 10,000 tasks
        max_tasks = 10_000
        task_count = 0

        # Get next trigger times
        schedule_def = schedule.schedule_definition

        while current < end and task_count < max_tasks:
            next_trigger = schedule_def.get_next_trigger_time(current, tz)

            if next_trigger > end:
                break

            # Create task
            task_id = f"bf_{config.backfill_id}_{task_count:05d}"
            task = BackfillTask(
                task_id=task_id,
                backfill_id=config.backfill_id,
                schedule_id=schedule.schedule_id,
                scheduled_for=next_trigger,
                status=BackfillStatus.PENDING,
            )

            tasks.append(task)

            # Move to next time slot
            current = next_trigger + timedelta(minutes=1)
            task_count += 1

        if task_count >= max_tasks:
            logger.warning(
                f"Reached maximum task limit ({max_tasks}) for backfill {config.backfill_id}"
            )

        return tasks

    def _generate_interval_backfill_tasks(
        self,
        config: BackfillConfig,
        schedule: Schedule,
        start: datetime,
        end: datetime,
        tz: Any,
    ) -> list[BackfillTask]:
        """Generate backfill tasks for an interval schedule."""
        tasks = []

        # Get interval in seconds
        # Cast to IntervalSchedule for type safety
        schedule_def = schedule.schedule_definition
        if isinstance(schedule_def, IntervalSchedule):
            interval_seconds = schedule_def.interval_seconds
        else:
            interval_seconds = 3600  # Fallback to 1 hour

        # Calculate number of tasks
        total_seconds = (end - start).total_seconds()
        num_tasks = int(total_seconds / interval_seconds)

        # Safety limit
        max_tasks = 10_000
        if num_tasks > max_tasks:
            num_tasks = max_tasks
            logger.warning(
                f"Limiting backfill to {max_tasks} tasks for schedule {schedule.schedule_id}"
            )

        # Generate tasks
        for i in range(num_tasks):
            scheduled_for = start + timedelta(seconds=i * interval_seconds)

            task_id = f"bf_{config.backfill_id}_{i:05d}"
            task = BackfillTask(
                task_id=task_id,
                backfill_id=config.backfill_id,
                schedule_id=schedule.schedule_id,
                scheduled_for=scheduled_for,
                status=BackfillStatus.PENDING,
            )

            tasks.append(task)

        return tasks

    def execute_backfill(
        self,
        config: BackfillConfig,
        schedule: Schedule,
        execute_fn: Callable[[BackfillTask], tuple[bool, str | None]],
    ) -> BackfillResult:
        """
        Execute a backfill operation.

        Args:
            config: The backfill configuration
            schedule: The schedule being backfilled
            execute_fn: Function to execute a single backfill task.
                       Receives BackfillTask, returns (success, error_message)

        Returns:
            BackfillResult with execution statistics
        """
        started_at = datetime.utcnow()
        logger.info(f"Starting backfill {config.backfill_id} for schedule {schedule.schedule_id}")

        # Generate tasks
        tasks = self.generate_backfill_tasks(config, schedule)

        result = BackfillResult(
            backfill_id=config.backfill_id,
            schedule_id=schedule.schedule_id,
            tasks_created=len(tasks),
            started_at=started_at,
        )

        if not tasks:
            result.completed_at = datetime.utcnow()
            result.duration_ms = (result.completed_at - started_at).total_seconds() * 1000
            logger.info(f"Backfill {config.backfill_id} had no tasks to execute")
            return result

        # Execute tasks
        if config.parallel and config.max_parallel > 1:
            # Parallel execution
            result = self._execute_backfill_parallel(config, schedule, tasks, execute_fn, result)
        else:
            # Sequential execution
            result = self._execute_backfill_sequential(config, schedule, tasks, execute_fn, result)

        # Calculate final stats
        result.completed_at = datetime.utcnow()
        result.duration_ms = (result.completed_at - started_at).total_seconds() * 1000

        logger.info(
            f"Completed backfill {config.backfill_id}: "
            f"{result.tasks_succeeded} succeeded, {result.tasks_failed} failed, "
            f"{result.duration_ms:.2f}ms"
        )

        return result

    def _execute_backfill_sequential(
        self,
        config: BackfillConfig,
        schedule: Schedule,
        tasks: Sequence[BackfillTask],
        execute_fn: Callable[[BackfillTask], tuple[bool, str | None]],
        result: BackfillResult,
    ) -> BackfillResult:
        """Execute backfill tasks sequentially."""
        for task in tasks:
            try:
                # Mark as running
                self._update_task_status(task, BackfillStatus.RUNNING)

                # Execute task
                success, error = execute_fn(task)

                # Update task status
                if success:
                    self._update_task_status(task, BackfillStatus.COMPLETED)
                    result.tasks_succeeded += 1
                else:
                    self._update_task_status(task, BackfillStatus.FAILED, error=error)
                    result.tasks_failed += 1

                    # Check failure strategy
                    if config.on_failure == "fail_fast":
                        logger.info(
                            f"Backfill {config.backfill_id} failed fast after task {task.task_id}"
                        )
                        break
                    elif config.on_failure == "retry" and task.retry_count < config.retry_attempts:
                        # Retry logic
                        task.retry_count += 1
                        self.store.save_backfill_task(task)
                        # Re-execute (simplified - just loop again)
                        continue

            except Exception as e:
                logger.error(f"Error executing backfill task {task.task_id}: {e}")
                self._update_task_status(task, BackfillStatus.FAILED, error=str(e))
                result.tasks_failed += 1

                if config.on_failure == "fail_fast":
                    break

        return result

    def _execute_backfill_parallel(
        self,
        config: BackfillConfig,
        schedule: Schedule,
        tasks: Sequence[BackfillTask],
        execute_fn: Callable[[BackfillTask], tuple[bool, str | None]],
        result: BackfillResult,
    ) -> BackfillResult:
        """Execute backfill tasks in parallel."""

        with ThreadPoolExecutor(max_workers=config.max_parallel) as executor:
            # Submit all tasks
            future_to_task = {executor.submit(execute_fn, task): task for task in tasks}

            # Process results as they complete
            for future in as_completed(future_to_task):
                task = future_to_task[future]

                try:
                    # Mark as running
                    self._update_task_status(task, BackfillStatus.RUNNING)

                    # Get result
                    success, error = future.result()

                    # Update task status
                    if success:
                        self._update_task_status(task, BackfillStatus.COMPLETED)
                        result.tasks_succeeded += 1
                    else:
                        self._update_task_status(task, BackfillStatus.FAILED, error=error)
                        result.tasks_failed += 1

                        if config.on_failure == "fail_fast":
                            # Cancel remaining tasks
                            for f in future_to_task:
                                f.cancel()
                            break

                except Exception as e:
                    logger.error(f"Error executing backfill task {task.task_id}: {e}")
                    self._update_task_status(task, BackfillStatus.FAILED, error=str(e))
                    result.tasks_failed += 1

                    if config.on_failure == "fail_fast":
                        break

        return result

    def _update_task_status(
        self,
        task: BackfillTask,
        status: BackfillStatus,
        error: str | None = None,
    ) -> None:
        """Update the status of a backfill task and save it."""
        # Create new task with updated status
        updated_task = BackfillTask(
            task_id=task.task_id,
            backfill_id=task.backfill_id,
            schedule_id=task.schedule_id,
            scheduled_for=task.scheduled_for,
            status=status,
            run_id=task.run_id,
            error=error or task.error,
            started_at=task.started_at if task.started_at else datetime.utcnow(),
            completed_at=datetime.utcnow()
            if status in (BackfillStatus.COMPLETED, BackfillStatus.FAILED)
            else None,
            retry_count=task.retry_count,
        )

        self.store.save_backfill_task(updated_task)

    def get_backfill_status(self, backfill_id: str) -> dict[str, int]:
        """
        Get the status of a backfill operation.

        Args:
            backfill_id: The backfill ID

        Returns:
            Dictionary with status counts (pending, running, completed, failed)
        """
        tasks = self.store.list_backfill_tasks(backfill_id)

        status_counts = {
            BackfillStatus.PENDING: 0,
            BackfillStatus.RUNNING: 0,
            BackfillStatus.COMPLETED: 0,
            BackfillStatus.FAILED: 0,
        }

        for task in tasks:
            status_counts[task.status] += 1

        return {
            "pending": status_counts[BackfillStatus.PENDING],
            "running": status_counts[BackfillStatus.RUNNING],
            "completed": status_counts[BackfillStatus.COMPLETED],
            "failed": status_counts[BackfillStatus.FAILED],
        }
