"""
Error handling and recovery utilities for Vibe Piper.

This module provides retry logic, error context capture, checkpointing,
and recovery mechanisms for resilient pipeline execution.
"""

import functools
import logging
import random
import threading
import time
import traceback
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, ParamSpec, TypeVar

from vibe_piper.monitoring.metrics import Metric, MetricType
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


class JitterStrategy(Enum):
    """Strategies for adding jitter to backoff delays to prevent thundering herd."""

    NONE = auto()  # No jitter, use exact backoff delay
    FULL = auto()  # Full jitter: random value between 0 and delay
    EQUAL = auto()  # Equal jitter: random value between delay/2 and delay
    DECORRELATED = auto()  # Decorrelated jitter: random value between base_delay and 3 * last_delay


@dataclass
class RetryConfig:
    """
    Configuration for retry behavior.

    Attributes:
        max_retries: Maximum number of retry attempts
        backoff_strategy: Strategy for calculating backoff delay
        jitter_strategy: Strategy for adding jitter to prevent thundering herd
        base_delay: Base delay in seconds (for linear/exponential backoff)
        max_delay: Maximum delay in seconds (for exponential backoff)
        jitter_amount: Amount of jitter to add (ratio of delay, e.g., 0.5 for 50%)
        retry_on_exceptions: Tuple of exception types to retry on
        retry_on_callback: Optional callback function to determine if retry should occur
        enable_dlq: Whether to send failed tasks to dead letter queue
    """

    max_retries: int = 3
    backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    jitter_strategy: JitterStrategy = JitterStrategy.FULL
    base_delay: float = 1.0
    max_delay: float = 60.0
    jitter_amount: float = 0.5
    retry_on_exceptions: tuple[type[Exception], ...] = (Exception,)
    retry_on_callback: Callable[[Exception, int], bool] | None = None
    enable_dlq: bool = False

    def calculate_delay(self, attempt: int, last_delay: float | None = None) -> float:
        """
        Calculate the delay before the next retry attempt.

        Args:
            attempt: The current retry attempt (0-indexed)
            last_delay: The delay used in the previous attempt (for decorrelated jitter)

        Returns:
            Delay in seconds with jitter applied
        """
        if self.backoff_strategy == BackoffStrategy.LINEAR:
            delay = self.base_delay * (attempt + 1)
        elif self.backoff_strategy == BackoffStrategy.EXPONENTIAL:
            delay = self.base_delay * (2**attempt)
        else:  # FIXED
            delay = 0.0

        # Cap at max_delay
        delay = min(delay, self.max_delay)

        # Apply jitter
        return self._apply_jitter(delay, last_delay)

    def _apply_jitter(self, delay: float, last_delay: float | None) -> float:
        """
        Apply jitter to the delay to prevent thundering herd.

        Args:
            delay: The calculated delay
            last_delay: The delay used in previous attempt (for decorrelated jitter)

        Returns:
            Delay with jitter applied
        """
        if self.jitter_strategy == JitterStrategy.NONE:
            return delay
        elif self.jitter_strategy == JitterStrategy.FULL:
            # Random value between 0 and delay
            return random.random() * delay
        elif self.jitter_strategy == JitterStrategy.EQUAL:
            # Random value between delay/2 and delay
            return (delay / 2) + (random.random() * (delay / 2))
        elif self.jitter_strategy == JitterStrategy.DECORRELATED:
            # Random value between base_delay and 3 * last_delay
            # If no last_delay, use exponential backoff
            if last_delay is None:
                return min(delay, self.max_delay)
            return min(
                random.uniform(self.base_delay, 3 * last_delay),
                self.max_delay,
            )
        return delay


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
        stack_trace="".join(traceback.format_exception(type(error), error, error.__traceback__)),
        timestamp=datetime.now(),
        asset_name=asset_name,
        inputs=inputs or {},
        attempt_number=attempt_number,
        retryable=retryable,
        metadata=metadata or {},
    )


# =============================================================================
# Dead Letter Queue (DLQ)
# =============================================================================


@dataclass(frozen=True)
class DeadLetterItem:
    """
    An item that has been sent to the dead letter queue.

    This represents a failed task that could not be completed after
    all retry attempts and has been moved to the DLQ for manual
    inspection and handling.

    Attributes:
        id: Unique identifier for the dead letter item
        asset_name: Name of the asset that failed
        error_context: Detailed error context
        inputs: Inputs that caused the failure
        timestamp: When the item was sent to DLQ
        retry_count: Number of retry attempts before giving up
        metadata: Additional metadata
    """

    id: str
    asset_name: str
    error_context: ErrorContext
    inputs: Mapping[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    retry_count: int = 0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert dead letter item to dictionary for serialization."""
        return {
            "id": self.id,
            "asset_name": self.asset_name,
            "error_context": self.error_context.to_dict(),
            "inputs": dict(self.inputs),
            "timestamp": self.timestamp.isoformat(),
            "retry_count": self.retry_count,
            "metadata": dict(self.metadata),
        }


class DeadLetterQueue:
    """
    Dead Letter Queue for failed tasks.

    The DLQ stores tasks that have failed after all retry attempts.
    These tasks can be inspected, reprocessed, or analyzed for debugging.

    Example:
        Send a failed task to DLQ::

            dlq = DeadLetterQueue()
            dlq.enqueue(
                asset_name="failing_asset",
                error=error,
                inputs={"param": "value"},
                retry_count=3
            )
    """

    def __init__(self, max_size: int = 1000) -> None:
        """
        Initialize the dead letter queue.

        Args:
            max_size: Maximum number of items to store in the DLQ
        """
        self._items: dict[str, DeadLetterItem] = {}
        self._lock = threading.Lock()
        self._max_size = max_size
        self._counter = 0

    def enqueue(
        self,
        asset_name: str,
        error: Exception,
        inputs: Mapping[str, Any] | None = None,
        retry_count: int = 0,
        metadata: Mapping[str, Any] | None = None,
    ) -> DeadLetterItem:
        """
        Add a failed task to the dead letter queue.

        Args:
            asset_name: Name of the asset that failed
            error: The exception that occurred
            inputs: Inputs that caused the failure
            retry_count: Number of retry attempts
            metadata: Additional metadata

        Returns:
            The created DeadLetterItem

        Raises:
            RuntimeError: If DLQ is at maximum capacity
        """
        with self._lock:
            if len(self._items) >= self._max_size:
                msg = f"DeadLetterQueue is at maximum capacity ({self._max_size})"
                raise RuntimeError(msg)

            error_context = capture_error_context(
                error=error,
                asset_name=asset_name,
                inputs=inputs,
                attempt_number=retry_count,
                retryable=False,
                metadata=metadata,
            )

            self._counter += 1
            item_id = f"dlq-{self._counter:06d}"

            item = DeadLetterItem(
                id=item_id,
                asset_name=asset_name,
                error_context=error_context,
                inputs=inputs or {},
                retry_count=retry_count,
                metadata=metadata or {},
            )

            self._items[item_id] = item

            logger.warning(
                f"Enqueued dead letter item {item_id} for asset '{asset_name}' "
                f"after {retry_count} retries: {error}"
            )

            return item

    def dequeue(self, item_id: str) -> DeadLetterItem | None:
        """
        Remove and return an item from the dead letter queue.

        Args:
            item_id: ID of the item to dequeue

        Returns:
            The DeadLetterItem if found, None otherwise
        """
        with self._lock:
            return self._items.pop(item_id, None)

    def get(self, item_id: str) -> DeadLetterItem | None:
        """
        Get an item from the dead letter queue without removing it.

        Args:
            item_id: ID of the item to get

        Returns:
            The DeadLetterItem if found, None otherwise
        """
        with self._lock:
            return self._items.get(item_id)

    def list_items(
        self,
        asset_name: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[DeadLetterItem, ...]:
        """
        List items in the dead letter queue.

        Args:
            asset_name: Optional filter by asset name
            limit: Maximum number of items to return
            offset: Number of items to skip

        Returns:
            Tuple of DeadLetterItems
        """
        with self._lock:
            items = list(self._items.values())

            if asset_name:
                items = [item for item in items if item.asset_name == asset_name]

            # Sort by timestamp (newest first)
            items.sort(key=lambda x: x.timestamp, reverse=True)

            return tuple(items[offset : offset + limit])

    def requeue(self, item_id: str) -> tuple[Mapping[str, Any], ErrorContext] | None:
        """
        Remove an item from DLQ and return its data for reprocessing.

        Args:
            item_id: ID of the item to requeue

        Returns:
            Tuple of (inputs, error_context) if found, None otherwise
        """
        item = self.dequeue(item_id)

        if item is None:
            return None

        logger.info(f"Requeued dead letter item {item_id} for reprocessing")

        return item.inputs, item.error_context

    def size(self) -> int:
        """Return the number of items in the DLQ."""
        with self._lock:
            return len(self._items)

    def clear(self) -> None:
        """Clear all items from the DLQ."""
        with self._lock:
            count = len(self._items)
            self._items.clear()
            logger.info(f"Cleared {count} items from DeadLetterQueue")

    def to_dict_list(self) -> list[dict[str, Any]]:
        """
        Convert all items to a list of dictionaries.

        Returns:
            List of serialized DeadLetterItems
        """
        with self._lock:
            return [item.to_dict() for item in self._items.values()]


# =============================================================================
# Circuit Breaker Pattern
# =============================================================================


class CircuitState(Enum):
    """States of the circuit breaker."""

    CLOSED = auto()  # Circuit is closed, requests flow normally
    OPEN = auto()  # Circuit is open, requests fail immediately
    HALF_OPEN = auto()  # Circuit is half-open, testing if service has recovered


@dataclass(frozen=True)
class CircuitBreakerConfig:
    """
    Configuration for circuit breaker behavior.

    Attributes:
        failure_threshold: Number of failures before opening the circuit
        success_threshold: Number of successes in half-open state to close circuit
        timeout_seconds: How long to wait in open state before trying half-open
        exception_types: Exception types that count as failures
    """

    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: float = 60.0
    exception_types: tuple[type[Exception], ...] = (Exception,)


class CircuitBreakerError(Exception):
    """Raised when circuit breaker is open."""

    def __init__(self, reason: str) -> None:
        msg = f"Circuit breaker is OPEN: {reason}"
        self.reason: str = reason
        super().__init__(msg)


class CircuitBreaker:
    """
    Circuit breaker pattern implementation.

    The circuit breaker prevents cascading failures by failing fast when
    a service is experiencing problems. It has three states:

    1. CLOSED: Normal operation, requests pass through
    2. OPEN: Requests fail immediately without attempting the operation
    3. HALF_OPEN: Testing if the service has recovered

    Example:
        Use circuit breaker to protect a flaky service::

            breaker = CircuitBreaker(
                config=CircuitBreakerConfig(failure_threshold=3)
            )

            try:
                result = breaker.call(failing_function)
            except CircuitBreakerError:
                # Circuit is open, use fallback
                result = fallback_function()
    """

    def __init__(
        self,
        config: CircuitBreakerConfig | None = None,
        name: str = "circuit_breaker",
    ) -> None:
        """
        Initialize the circuit breaker.

        Args:
            config: Circuit breaker configuration
            name: Name of the circuit breaker (for logging/metrics)
        """
        self._config = config or CircuitBreakerConfig()
        self._name = name
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: datetime | None = None
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        """Get the current state of the circuit breaker."""
        return self._state

    def call(
        self,
        func: Callable[P, T],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        """
        Call a function through the circuit breaker.

        Args:
            func: Function to call
            *args: Positional arguments to pass to function
            **kwargs: Keyword arguments to pass to function

        Returns:
            Result of the function call

        Raises:
            CircuitBreakerError: If circuit is open
            Exception: If the function raises an exception
        """
        with self._lock:
            if self._should_trip_open():
                msg = (
                    f"Circuit breaker '{self._name}' is OPEN. "
                    f"Failures: {self._failure_count}, "
                    f"Threshold: {self._config.failure_threshold}"
                )
                logger.warning(msg)
                raise CircuitBreakerError(msg)

            # If in HALF_OPEN, check if we should transition to CLOSED
            if self._state == CircuitState.HALF_OPEN:
                if self._success_count >= self._config.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._success_count = 0
                    logger.info(f"Circuit breaker '{self._name}' transitioned to CLOSED")

        # Execute the function
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure(e)
            raise

    def _should_trip_open(self) -> bool:
        """Check if circuit should be opened."""
        if self._state == CircuitState.OPEN:
            # Check if timeout has elapsed
            if self._last_failure_time is None:
                return True

            elapsed = (datetime.now() - self._last_failure_time).total_seconds()
            if elapsed >= self._config.timeout_seconds:
                # Transition to HALF_OPEN to test if service has recovered
                self._state = CircuitState.HALF_OPEN
                self._success_count = 0
                logger.info(
                    f"Circuit breaker '{self._name}' transitioned to HALF_OPEN "
                    f"after {elapsed:.2f}s timeout"
                )
                return False
            return True

        return False

    def _on_success(self) -> None:
        """Handle a successful function call."""
        with self._lock:
            if self._state == CircuitState.CLOSED:
                # Reset failure count on success in CLOSED state
                self._failure_count = 0
            elif self._state == CircuitState.HALF_OPEN:
                # Track successes in HALF_OPEN state
                self._success_count += 1
                logger.debug(
                    f"Circuit breaker '{self._name}' success in HALF_OPEN: "
                    f"{self._success_count}/{self._config.success_threshold}"
                )

    def _on_failure(self, exception: Exception) -> None:
        """Handle a failed function call."""
        with self._lock:
            # Check if this exception type counts as a failure
            if not isinstance(exception, self._config.exception_types):
                # Not a failure exception, treat as success
                self._on_success()
                return

            self._failure_count += 1
            self._last_failure_time = datetime.now()

            if self._state == CircuitState.HALF_OPEN:
                # Failed in HALF_OPEN, reopen the circuit
                self._state = CircuitState.OPEN
                self._success_count = 0
                logger.warning(
                    f"Circuit breaker '{self._name}' failed in HALF_OPEN state, "
                    f"reopening circuit. Failures: {self._failure_count}"
                )
            elif self._failure_count >= self._config.failure_threshold:
                # Threshold reached, open the circuit
                self._state = CircuitState.OPEN
                logger.warning(
                    f"Circuit breaker '{self._name}' tripped to OPEN. "
                    f"Failures: {self._failure_count}/{self._config.failure_threshold}"
                )

    def get_metrics(self) -> dict[str, Any]:
        """
        Get circuit breaker metrics.

        Returns:
            Dictionary with current metrics
        """
        with self._lock:
            return {
                "name": self._name,
                "state": self._state.name,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
                "last_failure_time": (
                    self._last_failure_time.isoformat() if self._last_failure_time else None
                ),
                "failure_threshold": self._config.failure_threshold,
                "success_threshold": self._config.success_threshold,
                "timeout_seconds": self._config.timeout_seconds,
            }

    def reset(self) -> None:
        """Reset the circuit breaker to CLOSED state."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_time = None
            logger.info(f"Circuit breaker '{self._name}' reset to CLOSED state")


# =============================================================================
# Retry Metrics
# =============================================================================


@dataclass
class RetryMetrics:
    """
    Metrics collected during retry operations.

    Attributes:
        function_name: Name of the function being retried
        total_attempts: Total number of attempts (initial + retries)
        successful_attempt: Which attempt succeeded (0-indexed)
        total_delay_ms: Total time spent waiting between retries
        backoff_strategy: Backoff strategy used
        jitter_strategy: Jitter strategy used
        dlq_sent: Whether the task was sent to DLQ
        circuit_breaker_opened: Whether circuit breaker was opened
    """

    function_name: str
    total_attempts: int
    successful_attempt: int | None = None
    total_delay_ms: float = 0.0
    backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    jitter_strategy: JitterStrategy = JitterStrategy.NONE
    dlq_sent: bool = False
    circuit_breaker_opened: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            "function_name": self.function_name,
            "total_attempts": self.total_attempts,
            "successful_attempt": self.successful_attempt,
            "total_delay_ms": self.total_delay_ms,
            "backoff_strategy": self.backoff_strategy.name,
            "jitter_strategy": self.jitter_strategy.name,
            "dlq_sent": self.dlq_sent,
            "circuit_breaker_opened": self.circuit_breaker_opened,
        }

    def to_metric(self) -> Metric:
        """Convert to a monitoring Metric."""
        labels = {
            "function_name": self.function_name,
            "backoff_strategy": self.backoff_strategy.name,
            "jitter_strategy": self.jitter_strategy.name,
        }

        return Metric(
            name="retry_attempts",
            value=self.total_attempts,
            metric_type=MetricType.COUNTER,
            labels=labels,
        )


# =============================================================================
# Retry Decorator
# =============================================================================


def retry_with_backoff(
    max_retries: int = 3,
    backoff: str = "exponential",
    jitter: str = "full",
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter_amount: float = 0.5,
    retry_on_exceptions: tuple[type[Exception], ...] = (Exception,),
    circuit_breaker: CircuitBreaker | None = None,
    dead_letter_queue: DeadLetterQueue | None = None,
    metrics_collector: Any = None,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """
    Decorator to add retry logic with backoff, jitter, and circuit breaker.

    Args:
        max_retries: Maximum number of retry attempts
        backoff: Backoff strategy - "linear", "exponential", or "fixed"
        jitter: Jitter strategy - "none", "full", "equal", or "decorrelated"
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
        jitter_amount: Amount of jitter to add (ratio of delay)
        retry_on_exceptions: Tuple of exception types to retry on
        circuit_breaker: Optional circuit breaker instance
        dead_letter_queue: Optional dead letter queue for failed tasks
        metrics_collector: Optional metrics collector for retry metrics

    Returns:
        Decorated function with retry logic

    Example:
        Retry a flaky function with exponential backoff and jitter::

            @retry_with_backoff(max_retries=3, backoff="exponential", jitter="full")
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
            f"Invalid backoff strategy: {backoff}. Must be 'linear', 'exponential', or 'fixed'"
        )

    # Convert jitter string to enum
    jitter_strategy = JitterStrategy.FULL
    if jitter == "none":
        jitter_strategy = JitterStrategy.NONE
    elif jitter == "equal":
        jitter_strategy = JitterStrategy.EQUAL
    elif jitter == "decorrelated":
        jitter_strategy = JitterStrategy.DECORRELATED
    elif jitter != "full":
        raise ValueError(
            f"Invalid jitter strategy: {jitter}. Must be 'none', 'full', 'equal', or 'decorrelated'"
        )

    retry_config = RetryConfig(
        max_retries=max_retries,
        backoff_strategy=backoff_strategy,
        jitter_strategy=jitter_strategy,
        base_delay=base_delay,
        max_delay=max_delay,
        jitter_amount=jitter_amount,
        retry_on_exceptions=retry_on_exceptions,
        enable_dlq=dead_letter_queue is not None,
    )

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            last_error = None
            last_delay: float | None = None
            total_delay_ms = 0.0

            # Initialize retry metrics
            retry_metrics = RetryMetrics(
                function_name=func.__name__,
                total_attempts=0,
                backoff_strategy=backoff_strategy,
                jitter_strategy=jitter_strategy,
            )

            for attempt in range(retry_config.max_retries + 1):
                retry_metrics.total_attempts = attempt + 1

                # Check circuit breaker if provided
                if circuit_breaker is not None:
                    try:
                        # Check if circuit is open before attempting
                        if circuit_breaker.state == CircuitState.OPEN:
                            retry_metrics.circuit_breaker_opened = True
                            msg = (
                                f"Circuit breaker is open for {func.__name__}. "
                                f"Aborting retry attempt {attempt + 1}"
                            )
                            logger.warning(msg)
                            raise CircuitBreakerError(msg)
                    except CircuitBreakerError:
                        if metrics_collector:
                            metrics_collector.record_metric(
                                name="circuit_breaker_open",
                                value=1,
                                metric_type=MetricType.COUNTER,
                                labels={"function_name": func.__name__},
                            )
                        retry_metrics.circuit_breaker_opened = True
                        raise

                try:
                    return func(*args, **kwargs)
                except retry_on_exceptions as e:
                    last_error = e

                    if attempt < retry_config.max_retries:
                        delay = retry_config.calculate_delay(attempt, last_delay)
                        last_delay = delay
                        total_delay_ms += delay * 1000

                        logger.warning(
                            f"Function {func.__name__} failed on attempt "
                            f"{attempt + 1}/{retry_config.max_retries + 1}: {e}. "
                            f"Retrying in {delay:.2f}s (jitter: {jitter_strategy.name})..."
                        )

                        time.sleep(delay)
                    else:
                        logger.error(
                            f"Function {func.__name__} failed after "
                            f"{retry_config.max_retries + 1} attempts: {e}"
                        )

                        # Send to DLQ if enabled
                        if dead_letter_queue is not None and retry_config.enable_dlq:
                            dead_letter_queue.enqueue(
                                asset_name=func.__name__,
                                error=e,
                                retry_count=attempt,
                                metadata=retry_metrics.to_dict(),
                            )
                            retry_metrics.dlq_sent = True
                            logger.warning(
                                f"Sent failed task to dead letter queue: "
                                f"{func.__name__} after {attempt} retries"
                            )

                            # Record DLQ metric
                            if metrics_collector:
                                metrics_collector.record_metric(
                                    name="dead_letter_queue_enqueued",
                                    value=1,
                                    metric_type=MetricType.COUNTER,
                                    labels={"function_name": func.__name__},
                                )

            # All retries exhausted
            if metrics_collector:
                # Record retry metrics
                metrics_collector.record_metric(
                    name="retry_total_attempts",
                    value=retry_metrics.total_attempts,
                    metric_type=MetricType.COUNTER,
                    labels={
                        "function_name": func.__name__,
                        "success": "false",
                    },
                )
                metrics_collector.record_metric(
                    name="retry_total_delay_ms",
                    value=total_delay_ms,
                    metric_type=MetricType.GAUGE,
                    labels={"function_name": func.__name__},
                )

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
