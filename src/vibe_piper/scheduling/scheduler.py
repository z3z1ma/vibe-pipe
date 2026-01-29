"""
Core scheduler engine for executing scheduled pipelines.

This module provides Scheduler class that manages schedules,
checks for triggers, executes pipelines, and tracks history.
"""

import logging
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime
from queue import Empty, Queue
from typing import TYPE_CHECKING, Any

from vibe_piper.execution import ExecutionEngine
from vibe_piper.scheduling.backfill import BackfillManager
from vibe_piper.scheduling.base import (
    Schedule,
    ScheduleEvent,
    ScheduleStatus,
    TriggerEvent,
    TriggerType,
)
from vibe_piper.scheduling.persistence import ScheduleStore

if TYPE_CHECKING:
    from vibe_piper.scheduling.schedules import EventTrigger

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SchedulerConfig:
    """
    Configuration for the scheduler.

    Attributes:
        check_interval_seconds: How often to check for triggers (default: 60)
        max_concurrent_runs: Maximum number of concurrent pipeline runs
        storage_dir: Directory for schedule storage
        timezone: Default timezone for the scheduler (default: UTC)
        auto_start: Whether to start the scheduler loop automatically (default: False)
    """

    check_interval_seconds: float = 60.0
    max_concurrent_runs: int = 5
    storage_dir: str = ".vibe_piper/schedules"
    timezone: str = "UTC"
    auto_start: bool = False


class Scheduler:
    """
    Engine for managing and executing scheduled pipelines.

    The Scheduler:
    - Manages schedule lifecycle (create, update, pause, delete)
    - Checks schedules for triggers at regular intervals
    - Executes pipelines when triggered
    - Tracks execution history
    - Handles event-driven triggers
    - Integrates with backfill operations

    Attributes:
        config: The scheduler configuration
        store: The ScheduleStore for persistence
        execution_engine: The ExecutionEngine for running pipelines
        backfill_manager: The BackfillManager for backfill operations
    """

    def __init__(
        self,
        config: SchedulerConfig | None = None,
        execution_engine: ExecutionEngine | None = None,
    ) -> None:
        """
        Initialize the scheduler.

        Args:
            config: Optional scheduler configuration
            execution_engine: Optional execution engine (created if None)
        """
        self.config = config or SchedulerConfig()
        self.store = ScheduleStore(storage_dir=self.config.storage_dir)
        self.execution_engine = execution_engine or ExecutionEngine()
        self.backfill_manager = BackfillManager(store=self.store)

        # Internal state
        self._running = False
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._active_runs: dict[str, datetime] = {}
        self._event_queue: Queue[TriggerEvent] = Queue()
        self._lock = threading.Lock()

        logger.info(f"Scheduler initialized with config: {self.config}")

    # =============================================================================
    # Lifecycle Management
    # =============================================================================

    def start(self) -> None:
        """Start the scheduler loop."""
        if self._running:
            logger.warning("Scheduler is already running")
            return

        self._running = True
        self._stop_event.clear()

        # Start scheduler thread
        self._thread = threading.Thread(
            target=self._scheduler_loop,
            name="vibe_piper_scheduler",
            daemon=True,
        )
        self._thread.start()

        logger.info("Scheduler started")

    def stop(self) -> None:
        """Stop the scheduler loop."""
        if not self._running:
            logger.warning("Scheduler is not running")
            return

        self._running = False
        self._stop_event.set()

        # Wait for thread to finish
        if self._thread:
            self._thread.join(timeout=10)

        logger.info("Scheduler stopped")

    def is_running(self) -> bool:
        """Check if the scheduler is running."""
        return self._running

    # =============================================================================
    # Schedule Management
    # =============================================================================

    def add_schedule(
        self,
        schedule: Schedule,
    ) -> Schedule:
        """
        Add a schedule to the scheduler.

        Args:
            schedule: The schedule to add

        Returns:
            The added schedule
        """
        # Validate schedule
        if not schedule.schedule_id:
            msg = "schedule_id is required"
            raise ValueError(msg)

        # Save to store
        self.store.save_schedule(schedule)

        logger.info(
            f"Added schedule '{schedule.name}' ({schedule.schedule_id}) "
            f"of type {schedule.schedule_type}"
        )

        return schedule

    def get_schedule(self, schedule_id: str) -> Schedule | None:
        """
        Get a schedule by ID.

        Args:
            schedule_id: The schedule ID

        Returns:
            The schedule, or None if not found
        """
        return self.store.load_schedule(schedule_id)

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
        return self.store.list_schedules(status=status)

    def pause_schedule(self, schedule_id: str) -> bool:
        """
        Pause a schedule.

        Args:
            schedule_id: The schedule ID

        Returns:
            True if paused, False if not found
        """
        schedule = self.store.load_schedule(schedule_id)
        if not schedule:
            return False

        # Create updated schedule with paused status
        updated = Schedule(
            schedule_id=schedule.schedule_id,
            name=schedule.name,
            schedule_type=schedule.schedule_type,
            schedule_definition=schedule.schedule_definition,
            asset_graph=schedule.asset_graph,
            status=ScheduleStatus.PAUSED,
            timezone=schedule.timezone,
            config=schedule.config,
            created_at=schedule.created_at,
            updated_at=datetime.utcnow(),
        )

        self.store.save_schedule(updated)
        logger.info(f"Paused schedule {schedule_id}")

        return True

    def resume_schedule(self, schedule_id: str) -> bool:
        """
        Resume a paused schedule.

        Args:
            schedule_id: The schedule ID

        Returns:
            True if resumed, False if not found
        """
        schedule = self.store.load_schedule(schedule_id)
        if not schedule:
            return False

        # Create updated schedule with active status
        updated = Schedule(
            schedule_id=schedule.schedule_id,
            name=schedule.name,
            schedule_type=schedule.schedule_type,
            schedule_definition=schedule.schedule_definition,
            asset_graph=schedule.asset_graph,
            status=ScheduleStatus.ACTIVE,
            timezone=schedule.timezone,
            config=schedule.config,
            created_at=schedule.created_at,
            updated_at=datetime.utcnow(),
        )

        self.store.save_schedule(updated)
        logger.info(f"Resumed schedule {schedule_id}")

        return True

    def delete_schedule(self, schedule_id: str) -> bool:
        """
        Delete a schedule.

        Args:
            schedule_id: The schedule ID

        Returns:
            True if deleted, False if not found
        """
        return self.store.delete_schedule(schedule_id)

    # =============================================================================
    # Event Handling
    # =============================================================================

    def trigger_event(self, event: TriggerEvent) -> None:
        """
        Submit an event to the scheduler.

        This is used for event-driven triggers.

        Args:
            event: The event to submit
        """
        self._event_queue.put(event)
        logger.info(f"Submitted event: {event.event_type}")

    # =============================================================================
    # Scheduler Loop
    # =============================================================================

    def _scheduler_loop(self) -> None:
        """Main scheduler loop that runs in a background thread."""
        from zoneinfo import ZoneInfo

        tz = ZoneInfo(self.config.timezone)

        while self._running:
            try:
                # Check for scheduled triggers
                self._check_scheduled_triggers(tz)

                # Check for event triggers
                self._check_event_triggers()

                # Clean up old active runs
                self._cleanup_active_runs()

                # Wait for next check
                self._stop_event.wait(self.config.check_interval_seconds)

            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}", exc_info=True)

    def _check_scheduled_triggers(self, tz: Any) -> None:
        """Check for time-based schedule triggers."""
        schedules = self.store.list_schedules(status=ScheduleStatus.ACTIVE)
        now = datetime.utcnow()

        for schedule in schedules:
            try:
                # Skip event-driven schedules
                if schedule.schedule_type.value == "event":
                    continue

                # Get last trigger time
                last_event = self.store.get_last_event(schedule.schedule_id)
                last_triggered = last_event.triggered_at if last_event else None

                # Check if schedule should trigger
                schedule_tz = schedule.get_timezone()
                if schedule.schedule_definition.should_trigger(
                    last_triggered,
                    now,
                    schedule_tz,
                ):
                    # Trigger the schedule
                    self._execute_schedule(schedule, TriggerType.SCHEDULED)

            except Exception as e:
                logger.error(
                    f"Error checking schedule {schedule.schedule_id}: {e}",
                    exc_info=True,
                )

    def _check_event_triggers(self) -> None:
        """Check for event-based triggers."""
        try:
            # Get events from queue (non-blocking)
            while True:
                try:
                    event = self._event_queue.get_nowait()

                    # Find matching schedules
                    schedules = self.store.list_schedules(status=ScheduleStatus.ACTIVE)

                    for schedule in schedules:
                        # Only check event-driven schedules
                        if schedule.schedule_type.value != "event":
                            continue

                        try:
                            # Get last trigger time
                            last_event = self.store.get_last_event(schedule.schedule_id)
                            last_triggered = last_event.triggered_at if last_event else None

                            # Check if event should trigger
                            schedule_def = schedule.schedule_definition
                            if isinstance(
                                schedule_def, EventTrigger
                            ) and schedule_def.should_trigger_event(
                                dict(event.event_data), last_triggered
                            ):
                                # Trigger the schedule
                                self._execute_schedule(schedule, TriggerType.EVENT, event=event)

                        except Exception as e:
                            logger.error(
                                f"Error processing event for schedule {schedule.schedule_id}: {e}",
                                exc_info=True,
                            )

                except Empty:
                    break

        except Exception as e:
            logger.error(f"Error processing event triggers: {e}", exc_info=True)

    # =============================================================================
    # Execution
    # =============================================================================

    def _execute_schedule(
        self,
        schedule: Schedule,
        trigger_type: TriggerType,
        event: TriggerEvent | None = None,
    ) -> None:
        """
        Execute a schedule.

        Args:
            schedule: The schedule to execute
            trigger_type: The type of trigger
            event: Optional event that triggered this execution
        """
        # Check concurrency limit
        with self._lock:
            if len(self._active_runs) >= self.config.max_concurrent_runs:
                logger.warning(
                    f"Max concurrent runs ({self.config.max_concurrent_runs}) reached, "
                    f"skipping schedule {schedule.schedule_id}"
                )
                return

            run_id = str(uuid.uuid4())
            self._active_runs[run_id] = datetime.utcnow()

        try:
            # Create schedule event
            schedule_event = ScheduleEvent(
                event_id=f"evt_{uuid.uuid4().hex[:12]}",
                schedule_id=schedule.schedule_id,
                trigger_type=trigger_type,
                triggered_at=datetime.utcnow(),
                run_id=run_id,
                status="running",
                metadata={"event_type": event.event_type if event else None},
            )

            self.store.save_event(schedule_event)

            # Execute the pipeline
            logger.info(
                f"Executing schedule '{schedule.name}' ({schedule.schedule_id}) "
                f"triggered by {trigger_type.value}"
            )

            # Import AssetGraph here to avoid circular import

            # Note: asset_graph is currently stored as dict in persistence
            # This is a simplification - in production, reconstruct properly
            if isinstance(schedule.asset_graph, dict):
                # For now, create a simple execution
                # This would need proper reconstruction in production
                logger.warning(
                    f"Asset graph for schedule {schedule.schedule_id} is serialized, "
                    "executing with placeholder"
                )
                result = None
            else:
                # Execute with actual graph
                context = self._create_pipeline_context(schedule, run_id, event)
                result = self.execution_engine.execute(schedule.asset_graph, context=context)

            # Update event with result
            status = "success" if (result and result.success) else "failed"

            # Create updated event
            updated_event = ScheduleEvent(
                event_id=schedule_event.event_id,
                schedule_id=schedule.schedule_id,
                trigger_type=trigger_type,
                triggered_at=schedule_event.triggered_at,
                run_id=run_id,
                status=status,
                metadata=schedule_event.metadata,
            )

            self.store.save_event(updated_event)

            if result:
                logger.info(
                    f"Schedule execution completed: {schedule.schedule_id} - "
                    f"succeeded={result.success}, "
                    f"assets={result.assets_succeeded}/{result.assets_executed}"
                )

        except Exception as e:
            logger.error(
                f"Error executing schedule {schedule.schedule_id}: {e}",
                exc_info=True,
            )

            # Create failed event
            failed_event = ScheduleEvent(
                event_id=f"evt_{uuid.uuid4().hex[:12]}",
                schedule_id=schedule.schedule_id,
                trigger_type=trigger_type,
                triggered_at=datetime.utcnow(),
                run_id=run_id,
                status="failed",
                metadata={"error": str(e)},
            )

            self.store.save_event(failed_event)

        finally:
            # Clean up active run
            with self._lock:
                self._active_runs.pop(run_id, None)

    def _create_pipeline_context(
        self,
        schedule: Schedule,
        run_id: str,
        event: TriggerEvent | None,
    ) -> Any:
        """Create a pipeline context for execution."""
        from vibe_piper.types import PipelineContext

        # Merge schedule config with event metadata
        config = dict(schedule.config)
        if event:
            config["trigger_event"] = event.event_data

        return PipelineContext(
            pipeline_id=schedule.schedule_id,
            run_id=run_id,
            config=config,
            metadata={
                "schedule_name": schedule.name,
                "schedule_type": schedule.schedule_type.value,
                "triggered_at": datetime.utcnow().isoformat(),
            },
        )

    def _cleanup_active_runs(self) -> None:
        """Clean up old active run entries."""
        now = datetime.utcnow()
        timeout = 3600  # 1 hour timeout

        with self._lock:
            expired_runs = [
                run_id
                for run_id, started_at in self._active_runs.items()
                if (now - started_at).total_seconds() > timeout
            ]

            for run_id in expired_runs:
                logger.warning(f"Removing expired run {run_id} from active runs")
                self._active_runs.pop(run_id, None)

    # =============================================================================
    # Backfill Operations
    # =============================================================================

    def create_backfill(
        self,
        schedule_id: str,
        start_date: datetime,
        end_date: datetime,
        timezone: str = "UTC",
        parallel: bool = False,
        max_parallel: int = 3,
    ) -> Any:
        """
        Create a backfill for a schedule.

        Args:
            schedule_id: The schedule to backfill
            start_date: Start date for backfill
            end_date: End date for backfill
            timezone: Timezone for backfill
            parallel: Whether to run tasks in parallel
            max_parallel: Maximum parallel tasks

        Returns:
            BackfillConfig from BackfillManager
        """
        schedule = self.store.load_schedule(schedule_id)
        if not schedule:
            msg = f"Schedule {schedule_id} not found"
            raise ValueError(msg)

        return self.backfill_manager.create_backfill(
            schedule=schedule,
            start_date=start_date,
            end_date=end_date,
            timezone=timezone,
            parallel=parallel,
            max_parallel=max_parallel,
        )

    def execute_backfill(
        self,
        backfill_id: str,
    ) -> Any:
        """
        Execute a backfill operation.

        Args:
            backfill_id: The backfill ID to execute

        Returns:
            BackfillResult from BackfillManager
        """
        config = self.store.load_backfill_config(backfill_id)
        if not config:
            msg = f"Backfill {backfill_id} not found"
            raise ValueError(msg)

        schedule = self.store.load_schedule(config.schedule_id)
        if not schedule:
            msg = f"Schedule {config.schedule_id} not found"
            raise ValueError(msg)

        # Define execution function for backfill tasks
        def execute_backfill_task(task: Any) -> tuple[bool, str | None]:
            try:
                # Create context for backfill task
                from vibe_piper.types import PipelineContext

                context = PipelineContext(
                    pipeline_id=schedule.schedule_id,
                    run_id=f"bf_{task.task_id}",
                    config={
                        "backfill": True,
                        "backfill_id": backfill_id,
                        "scheduled_for": task.scheduled_for.isoformat(),
                    },
                )

                # Execute pipeline
                result = self.execution_engine.execute(schedule.asset_graph, context=context)

                # Update task with run_id
                updated_task = type(task)(
                    task_id=task.task_id,
                    backfill_id=task.backfill_id,
                    schedule_id=task.schedule_id,
                    scheduled_for=task.scheduled_for,
                    status=type(task).status if result and result.success else type(task).FAILED,
                    run_id=context.run_id,
                    started_at=task.started_at,
                    completed_at=datetime.utcnow(),
                    retry_count=task.retry_count,
                )

                self.store.save_backfill_task(updated_task)

                return (result.success if result else False, None)

            except Exception as e:
                logger.error(f"Error in backfill task {task.task_id}: {e}")
                return (False, str(e))

        # Execute backfill
        return self.backfill_manager.execute_backfill(
            config=config,
            schedule=schedule,
            execute_fn=execute_backfill_task,
        )
