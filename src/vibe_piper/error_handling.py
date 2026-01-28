"""
Error handling and recovery utilities for Vibe Piper.

This module provides retry logic, error context capture, checkpointing,
and recovery mechanisms for resilient pipeline execution.
"""

import functools
import logging
import time
import traceback
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, ParamSpec, TypeVar

from vibe_piper.types import AssetResult

# =============================================================================
# Logger
# =============================================================================

logger = logging.getLogger(__name__)

# =============================================================================
# Type Variables
# =============================================================================

P = ParamSpec("P")
T = TypeVar("T")

# =============================================================================
# Retry Configuration
# =============================================================================


class BackoffStrategy(Enum):
    """Strategies for retry backoff."""

    LINEAR = auto()  # Wait the same amount of time between retries
    EXPONENTIAL = auto()  # Double the wait time after each retry
    FIXED = auto()  # No backoff, retry immediately


@dataclass
class RetryConfig:
    """
    Configuration for retry behavior.

    Attributes:
        max_retries: Maximum number of retry attempts
        backoff_strategy: Strategy for calculating backoff delay
        base_delay: Base delay in seconds (for linear/expponential backoff)
        max_delay: Maximum delay in seconds (for exponential backoff)
        retry_on_exceptions: Tuple of exception types to retry on
        retry_on_callback: Optional callback function to determine if retry should occur
    """

    max_retries: int = 3
    backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    base_delay: float = 1.0
    max_delay: float = 60.0
    retry_on_exceptions: tuple[type[Exception], ...] = (Exception,)
    retry_on_callback: Callable[[Exception, int], bool] | None = None

    def calculate_delay(self, attempt: int) -> float:
        """
        Calculate the delay before the next retry attempt.

        Args:
            attempt: The current retry attempt (0-indexed)

        Returns:
            Delay in seconds
        """
        if self.backoff_strategy == BackoffStrategy.LINEAR:
            delay = self.base_delay * (attempt + 1)
        elif self.backoff_strategy == BackoffStrategy.EXPONENTIAL:
            delay = self.base_delay * (2**attempt)
        else:  # FIXED
            delay = 0.0

        return min(delay, self.max_delay)


# =============================================================================
# Error Context
# =============================================================================


@dataclass(frozen=True)
class ErrorContext:
    """
    Detailed context about an error that occurred during execution.

    Attributes:
        error_type: The type of exception that occurred
        error_message: The error message
        stack_trace: Full stack trace as a string
        timestamp: When the error occurred
        asset_name: Name of the asset that failed
        inputs: Inputs to the asset that failed
        attempt_number: Which attempt number this error occurred on
        retryable: Whether this error is retryable
        metadata: Additional error metadata
    """

    error_type: str
    error_message: str
    stack_trace: str
    timestamp: datetime
    asset_name: str
    inputs: Mapping[str, Any] = field(default_factory=dict)
    attempt_number: int = 0
    retryable: bool = False
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert error context to a dictionary for serialization."""
        return {
            "error_type": self.error_type,
            "error_message": self.error_message,
            "stack_trace": self.stack_trace,
            "timestamp": self.timestamp.isoformat(),
            "asset_name": self.asset_name,
            "inputs": dict(self.inputs),
            "attempt_number": self.attempt_number,
            "retryable": self.retryable,
            "metadata": dict(self.metadata),
        }


def capture_error_context(
    error: Exception,
    asset_name: str,
    inputs: Mapping[str, Any] | None = None,
    attempt_number: int = 0,
    retryable: bool = False,
    metadata: Mapping[str, Any] | None = None,
) -> ErrorContext:
    """
    Capture detailed context about an error.

    Args:
        error: The exception that occurred
        asset_name: Name of the asset that failed
        inputs: Inputs to the asset
        attempt_number: Which attempt number this error occurred on
        retryable: Whether this error is retryable
        metadata: Additional error metadata

    Returns:
        ErrorContext with detailed error information
    """
    return ErrorContext(
        error_type=type(error).__name__,
        error_message=str(error),
        stack_trace="".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        ),
        timestamp=datetime.now(),
        asset_name=asset_name,
        inputs=inputs or {},
        attempt_number=attempt_number,
        retryable=retryable,
        metadata=metadata or {},
    )


# =============================================================================
# Retry Decorator
# =============================================================================


def retry_with_backoff(
    max_retries: int = 3,
    backoff: str = "exponential",
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    retry_on_exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """
    Decorator to add retry logic with exponential backoff to a function.

    Args:
        max_retries: Maximum number of retry attempts
        backoff: Backoff strategy - "linear", "exponential", or "fixed"
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
        retry_on_exceptions: Tuple of exception types to retry on

    Returns:
        Decorated function with retry logic

    Example:
        Retry a flaky function with exponential backoff::

            @retry_with_backoff(max_retries=3, backoff="exponential")
            def flaky_api_call():
                return fetch_data()
    """

    # Convert backoff string to enum
    backoff_strategy = BackoffStrategy.EXPONENTIAL
    if backoff == "linear":
        backoff_strategy = BackoffStrategy.LINEAR
    elif backoff == "fixed":
        backoff_strategy = BackoffStrategy.FIXED
    elif backoff != "exponential":
        raise ValueError(
            f"Invalid backoff strategy: {backoff}. "
            "Must be 'linear', 'exponential', or 'fixed'"
        )

    retry_config = RetryConfig(
        max_retries=max_retries,
        backoff_strategy=backoff_strategy,
        base_delay=base_delay,
        max_delay=max_delay,
        retry_on_exceptions=retry_on_exceptions,
    )

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            last_error = None

            for attempt in range(retry_config.max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retry_on_exceptions as e:
                    last_error = e

                    if attempt < retry_config.max_retries:
                        delay = retry_config.calculate_delay(attempt)

                        logger.warning(
                            f"Function {func.__name__} failed on attempt "
                            f"{attempt + 1}/{retry_config.max_retries + 1}: {e}. "
                            f"Retrying in {delay:.2f}s..."
                        )

                        time.sleep(delay)
                    else:
                        logger.error(
                            f"Function {func.__name__} failed after "
                            f"{retry_config.max_retries + 1} attempts: {e}"
                        )

            # All retries exhausted
            raise last_error  # type: ignore

        return wrapper

    return decorator


# =============================================================================
# Checkpoint Types
# =============================================================================


@dataclass(frozen=True)
class Checkpoint:
    """
    A checkpoint representing the state of pipeline execution.

    Attributes:
        run_id: The run ID that created this checkpoint
        asset_name: Name of the asset that was checkpointed
        timestamp: When the checkpoint was created
        asset_result: The result of the asset execution
        upstream_results: Results from upstream assets
        metadata: Additional checkpoint metadata
    """

    run_id: str
    asset_name: str
    timestamp: datetime
    asset_result: AssetResult
    upstream_results: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert checkpoint to a dictionary for serialization."""
        return {
            "run_id": self.run_id,
            "asset_name": self.asset_name,
            "timestamp": self.timestamp.isoformat(),
            "asset_result": {
                "asset_name": self.asset_result.asset_name,
                "success": self.asset_result.success,
                "data": self.asset_result.data,
                "error": self.asset_result.error,
                "metrics": dict(self.asset_result.metrics),
                "duration_ms": self.asset_result.duration_ms,
                "timestamp": self.asset_result.timestamp.isoformat(),
                "lineage": self.asset_result.lineage,
            },
            "upstream_results": dict(self.upstream_results),
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class CheckpointState:
    """
    The overall checkpoint state for a pipeline execution.

    Attributes:
        pipeline_id: The pipeline ID
        run_id: The current run ID
        checkpoints: All checkpoints for this run
        last_checkpoint_asset: The last asset that was successfully checkpointed
        created_at: When the checkpoint state was created
        metadata: Additional metadata
    """

    pipeline_id: str
    run_id: str
    checkpoints: tuple[Checkpoint, ...] = field(default_factory=tuple)
    last_checkpoint_asset: str | None = None
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def get_checkpoint_for_asset(self, asset_name: str) -> Checkpoint | None:
        """Get the checkpoint for a specific asset."""
        for checkpoint in self.checkpoints:
            if checkpoint.asset_name == asset_name:
                return checkpoint
        return None

    def get_results_from_checkpoint(self, asset_name: str) -> AssetResult | None:
        """Get the asset result from a checkpoint."""
        checkpoint = self.get_checkpoint_for_asset(asset_name)
        return checkpoint.asset_result if checkpoint else None


# =============================================================================
# Checkpoint Manager
# =============================================================================


class CheckpointManager:
    """
    Manages checkpoint creation and recovery for pipeline execution.

    The CheckpointManager is responsible for:
    - Creating checkpoints after successful asset execution
    - Storing checkpoint state
    - Recovering execution from checkpoints
    """

    def __init__(self) -> None:
        """Initialize the CheckpointManager."""
        self._checkpoint_state: CheckpointState | None = None

    def initialize(
        self,
        pipeline_id: str,
        run_id: str,
        existing_state: CheckpointState | None = None,
    ) -> None:
        """
        Initialize checkpoint state for a run.

        Args:
            pipeline_id: The pipeline ID
            run_id: The current run ID
            existing_state: Optional existing checkpoint state to recover from
        """
        if existing_state:
            self._checkpoint_state = existing_state
            logger.info(
                f"Resuming from checkpoint state from run {existing_state.run_id}. "
                f"Last checkpoint: {existing_state.last_checkpoint_asset}"
            )
        else:
            self._checkpoint_state = CheckpointState(
                pipeline_id=pipeline_id,
                run_id=run_id,
            )
            logger.info(f"Initialized new checkpoint state for run {run_id}")

    def create_checkpoint(
        self,
        asset_name: str,
        asset_result: AssetResult,
        upstream_results: Mapping[str, Any],
        metadata: Mapping[str, Any] | None = None,
    ) -> Checkpoint:
        """
        Create a checkpoint for an asset execution.

        Args:
            asset_name: Name of the asset
            asset_result: The result of the asset execution
            upstream_results: Results from upstream assets
            metadata: Optional additional metadata

        Returns:
            The created checkpoint

        Raises:
            RuntimeError: If checkpoint state is not initialized
        """
        if self._checkpoint_state is None:
            msg = "Checkpoint state not initialized. Call initialize() first."
            raise RuntimeError(msg)

        checkpoint = Checkpoint(
            run_id=self._checkpoint_state.run_id,
            asset_name=asset_name,
            timestamp=datetime.now(),
            asset_result=asset_result,
            upstream_results=upstream_results,
            metadata=metadata or {},
        )

        # Add checkpoint to state (immutable, so create new state)
        current_checkpoints = list(self._checkpoint_state.checkpoints)
        current_checkpoints.append(checkpoint)

        self._checkpoint_state = CheckpointState(
            pipeline_id=self._checkpoint_state.pipeline_id,
            run_id=self._checkpoint_state.run_id,
            checkpoints=tuple(current_checkpoints),
            last_checkpoint_asset=asset_name,
            created_at=self._checkpoint_state.created_at,
            metadata=self._checkpoint_state.metadata,
        )

        logger.info(f"Created checkpoint for asset '{asset_name}'")
        return checkpoint

    def get_checkpoint_state(self) -> CheckpointState | None:
        """Get the current checkpoint state."""
        return self._checkpoint_state

    def can_resume_from_asset(self, asset_name: str) -> bool:
        """
        Check if execution can resume from a specific asset.

        Args:
            asset_name: Name of the asset to check

        Returns:
            True if a checkpoint exists for the asset
        """
        if self._checkpoint_state is None:
            return False

        return self._checkpoint_state.get_checkpoint_for_asset(asset_name) is not None

    def get_asset_result_from_checkpoint(self, asset_name: str) -> AssetResult | None:
        """
        Get an asset result from checkpoint.

        Args:
            asset_name: Name of the asset

        Returns:
            AssetResult if checkpoint exists, None otherwise
        """
        if self._checkpoint_state is None:
            return None

        return self._checkpoint_state.get_results_from_checkpoint(asset_name)

    def get_execution_plan_from_checkpoint(
        self, execution_order: tuple[str, ...]
    ) -> tuple[str, ...]:
        """
        Determine the execution plan based on checkpoint state.

        Returns only the assets that need to be executed (excluding those
        that have successful checkpoints).

        Args:
            execution_order: Full execution order

        Returns:
            Tuple of asset names that still need to be executed
        """
        if self._checkpoint_state is None:
            return execution_order

        # Find the last checkpointed asset
        last_checkpointed = self._checkpoint_state.last_checkpoint_asset

        if last_checkpointed is None:
            return execution_order

        # Return assets after the last checkpointed asset
        try:
            last_index = execution_order.index(last_checkpointed)
            return execution_order[last_index + 1 :]
        except ValueError:
            # Last checkpointed asset not in execution order
            # This can happen if the graph changed
            logger.warning(
                f"Last checkpointed asset '{last_checkpointed}' not found in "
                f"current execution order. Re-executing all assets."
            )
            return execution_order
