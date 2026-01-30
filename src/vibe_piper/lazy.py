"""
Lazy evaluation for transformations in Vibe Piper.

This module provides:
- Lazy computation of transformations
- Deferred execution until result is needed
- Transparent conversion to eager evaluation
- Memoization of lazy values
"""

import logging
from collections.abc import Callable, Generator, Iterator
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

# =============================================================================
# Logger
# =============================================================================

logger = logging.getLogger(__name__)

# =============================================================================
# Lazy Value Types
# =============================================================================


@dataclass
class LazyValue:
    """
    A value that is computed lazily.

    LazyValue defers computation until the value is actually needed.
    Once computed, the result is memoized for subsequent accesses.

    Attributes:
        fn: Function to compute the value
        args: Arguments to pass to function
        kwargs: Keyword arguments to pass to function
        computed: Whether the value has been computed
        value: The computed value (if computed)
        error: Error that occurred during computation (if any)
        computation_time_ms: Time taken to compute (in milliseconds)
        computed_at: When the value was computed
    """

    fn: Callable[..., Any]
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    computed: bool = False
    value: Any | None = None
    error: Exception | None = None
    computation_time_ms: float | None = None
    computed_at: datetime | None = None

    def __call__(self) -> Any:
        """
        Get the value, computing if necessary.

        Returns:
            The computed value

        Raises:
            Exception: If computation failed
        """
        return self.get()

    def get(self) -> Any:
        """
        Get the value, computing if necessary.

        Returns:
            The computed value

        Raises:
            Exception: If computation failed
        """
        if self.computed:
            return self.value

        return self._compute()

    def _compute(self) -> Any:
        """
        Compute the value.

        Returns:
            The computed value

        Raises:
            Exception: If computation failed
        """
        import time

        logger.debug(f"Computing lazy value for {self.fn.__name__}")

        start_time = time.time()
        computed_at = datetime.utcnow()

        try:
            result = self.fn(*self.args, **self.kwargs)

            self.value = result
            self.computed = True
            self.computed_at = computed_at
            self.computation_time_ms = (time.time() - start_time) * 1000

            logger.debug(f"Lazy value computed in {self.computation_time_ms:.2f}ms")

            return result

        except Exception as e:
            self.error = e
            self.computed = True
            self.computed_at = computed_at
            self.computation_time_ms = (time.time() - start_time) * 1000

            logger.error(f"Lazy value computation failed: {e}")
            raise

    def is_computed(self) -> bool:
        """Check if value has been computed."""
        return self.computed

    def is_failed(self) -> bool:
        """Check if computation failed."""
        return self.computed and self.error is not None

    def reset(self) -> None:
        """Reset to uncomputed state (for re-computation)."""
        self.computed = False
        self.value = None
        self.error = None
        self.computation_time_ms = None
        self.computed_at = None

    def __repr__(self) -> str:
        """String representation."""
        status = "computed" if self.computed else "pending"
        return f"<LazyValue {self.fn.__name__} ({status})>"

    def __iter__(self) -> Iterator[Any]:
        """
        Make lazy values iterable.

        This allows lazy values to be used in for loops,
        automatically computing the value first.
        """
        return iter(self.get())


@dataclass
class LazySequence:
    """
    A sequence that is computed lazily.

    LazySequence defers computation of each element until accessed.
    Useful for large datasets or expensive transformations.

    Attributes:
        generator: Generator function for sequence elements
        length_hint: Optional hint about sequence length
        computed: Whether the sequence has been materialized
        cache: Cached computed elements
    """

    generator: Generator[Any, None, Any] | Callable[[], Generator[Any, None, Any]]
    length_hint: int | None = None
    computed: bool = False
    cache: list[Any] = field(default_factory=list)
    _iterator: Iterator[Any] | None = None

    def __iter__(self) -> Iterator[Any]:
        """
        Iterate over sequence.

        Returns:
            Iterator over sequence elements
        """
        # If already computed, iterate over cache
        if self.computed:
            return iter(self.cache)

        # Otherwise, use the generator
        if self._iterator is None:
            if callable(self.generator):
                self._iterator = iter(self.generator())
            else:
                self._iterator = iter(self.generator)

        return self

    def __next__(self) -> Any:
        """
        Get next element from sequence.

        Returns:
            Next element in sequence

        Raises:
            StopIteration: If no more elements
        """
        iterator = iter(self)
        return next(iterator)

    def __len__(self) -> int:
        """
        Get length of sequence.

        Note: This forces materialization of the entire sequence.

        Returns:
            Length of sequence
        """
        if not self.computed:
            self.materialize()

        return len(self.cache)

    def __getitem__(self, index: int) -> Any:
        """
        Get element at index.

        Note: This forces materialization of elements up to index.

        Args:
            index: Index of element to get

        Returns:
            Element at index
        """
        if not self.computed:
            # Materialize up to the requested index
            iterator = iter(self)
            while len(self.cache) <= index:
                try:
                    self.cache.append(next(iterator))
                except StopIteration:
                    break
            self.computed = self._iterator is None or all(c is not None for c in self.cache)

        return self.cache[index]

    def materialize(self) -> list[Any]:
        """
        Force materialization of entire sequence.

        Returns:
            List of all elements
        """
        if not self.computed:
            iterator = iter(self)
            for item in iterator:
                pass

            self.computed = True

        return self.cache

    def is_materialized(self) -> bool:
        """Check if sequence has been materialized."""
        return self.computed


@dataclass
class LazyTransform:
    """
    A transformation that is applied lazily.

    LazyTransform represents a transformation that will be applied
    to input data only when the result is actually needed.

    Attributes:
        fn: Transformation function
        input_data: Input data (can be LazyValue or LazySequence)
        description: Optional description of transformation
    """

    fn: Callable[[Any], Any]
    input_data: Any
    description: str | None = None

    def apply(self) -> Any:
        """
        Apply the transformation to input data.

        Returns:
            Transformed data

        Raises:
            Exception: If transformation fails
        """
        # First, materialize input if it's lazy
        materialized_input = self._materialize_input(self.input_data)

        # Apply transformation
        result = self.fn(materialized_input)

        return result

    def _materialize_input(self, data: Any) -> Any:
        """
        Materialize input data if it's lazy.

        Args:
            data: Input data to materialize

        Returns:
            Materialized data
        """
        # Handle LazyValue
        if isinstance(data, LazyValue):
            return data.get()

        # Handle LazySequence
        if isinstance(data, LazySequence):
            return data.materialize()

        # Handle nested LazyTransform
        if isinstance(data, LazyTransform):
            return data.apply()

        # Regular data, return as-is
        return data

    def __call__(self) -> Any:
        """
        Apply transformation (alias for apply).

        Returns:
            Transformed data

        Raises:
            Exception: If transformation fails
        """
        return self.apply()

    def __repr__(self) -> str:
        """String representation."""
        name = self.description or self.fn.__name__
        return f"<LazyTransform {name}>"

    def pipe(self, other: Callable[[Any], Any]) -> "LazyTransform":
        """
        Chain this transformation with another.

        Args:
            other: Another transformation to apply after this one

        Returns:
            New LazyTransform representing the chain
        """

        def chained(input_data: Any) -> Any:
            intermediate = self.fn(input_data)
            return other(intermediate)

        return LazyTransform(fn=chained, input_data=self.input_data)


# =============================================================================
# Lazy Operators
# =============================================================================


def lazy(fn):
    """
    Decorator to create lazy values.

    Args:
        fn: Function to make lazy

    Returns:
        Function that returns a LazyValue

    Example:
        Create a lazy function::

            @lazy
            def expensive_computation(data):
                return complex_transform(data)

            result = expensive_computation(raw_data)
            # Computation hasn't happened yet

            value = result()
            # Now computation happens
    """

    def wrapper(*args, **kwargs) -> LazyValue:
        return LazyValue(fn=fn, args=args, kwargs=kwargs)

    return wrapper


def lazy_map(fn):
    """
    Create a lazy map operation.

    Args:
        fn: Mapping function

    Returns:
        Function that creates a LazySequence

    Example:
        Lazy map over a sequence::

            def double(x):
                return x * 2

            data = [1, 2, 3, 4, 5]
            mapped = lazy_map(double)(iter(data))

            # No computation yet
            result = mapped.materialize()
            # Now computation happens: [2, 4, 6, 8, 10]
    """

    def wrapper(iterable: Iterator[Any]) -> LazySequence:
        def generator() -> Generator[Any, None, None]:
            for item in iterable:
                yield fn(item)

        return LazySequence(generator=generator)

    return wrapper


def lazy_filter(predicate):
    """
    Create a lazy filter operation.

    Args:
        predicate: Filter predicate function

    Returns:
        Function that creates a LazySequence

    Example:
        Lazy filter over a sequence::

            def is_even(x):
                return x % 2 == 0

            data = [1, 2, 3, 4, 5, 6]
            filtered = lazy_filter(is_even)(iter(data))

            # No computation yet
            result = filtered.materialize()
            # Now computation happens: [2, 4, 6]
    """

    def wrapper(iterable: Iterator[Any]) -> LazySequence:
        def generator() -> Generator[Any, None, None]:
            for item in iterable:
                if predicate(item):
                    yield item

        return LazySequence(generator=generator)

    return wrapper


def lazy_reduce(fn, initial):
    """
    Create a lazy reduce operation.

    Note: This is only "lazy" in the sense that the reduction
    is wrapped in a LazyValue. The actual reduction
    is eager because reduce requires all elements.

    Args:
        fn: Reduce function
        initial: Initial value

    Returns:
        Function that creates a LazyValue

    Example:
        Lazy reduce over a sequence::

            def add(acc, x):
                return acc + x

            data = [1, 2, 3, 4, 5]
            reduced = lazy_reduce(add, initial=0)(iter(data))

            # No computation yet
            result = reduced()
            # Now computation happens: 15
    """

    def wrapper(iterable: Iterator[Any]) -> LazyValue:
        def reduce_fn() -> Any:
            result = initial
            for item in iterable:
                result = fn(result, item)
            return result

        return LazyValue(fn=reduce_fn)

    return wrapper


def lazy_transform(input_data: Any, transform_fn, description: str | None = None) -> LazyTransform:
    """
    Create a lazy transformation.

    Args:
        input_data: Input data (can be regular or lazy)
        transform_fn: Transformation function
        description: Optional description

    Returns:
        LazyTransform object

    Example:
        Create a lazy transformation::

            data = [1, 2, 3, 4, 5]
            lazy_result = lazy_transform(data, lambda x: x * 2)

            # No computation yet
            result = lazy_result.apply()
            # Now computation happens: [2, 4, 6, 8, 10]
    """
    return LazyTransform(fn=transform_fn, input_data=input_data, description=description)


def materialize(value: Any) -> Any:
    """
    Force materialization of lazy values.

    Recursively materializes LazyValue, LazySequence,
    and LazyTransform objects.

    Args:
        value: Value to materialize

    Returns:
        Materialized value

    Example:
        Materialize nested lazy values::

            lazy_data = lazy_transform(raw_data, transform_fn)
            lazy_result = lazy_transform(lazy_data, another_transform_fn)

            result = materialize(lazy_result)
            # All transformations are applied
    """
    # Handle LazyValue
    if isinstance(value, LazyValue):
        return value.get()

    # Handle LazySequence
    if isinstance(value, LazySequence):
        return value.materialize()

    # Handle LazyTransform
    if isinstance(value, LazyTransform):
        return value.apply()

    # Handle lists/tuples with lazy elements
    if isinstance(value, list):
        return [materialize(item) for item in value]

    if isinstance(value, tuple):
        return tuple(materialize(item) for item in value)

    # Handle dicts with lazy values
    if isinstance(value, dict):
        return {k: materialize(v) for k, v in value.items()}

    # Regular value
    return value


def is_lazy(value: Any) -> bool:
    """
    Check if a value is lazy.

    Args:
        value: Value to check

    Returns:
        True if value is lazy (LazyValue, LazySequence, LazyTransform)
    """
    return isinstance(value, (LazyValue, LazySequence, LazyTransform))


# =============================================================================
# Context Manager
# =============================================================================


class LazyContext:
    """
    Context manager for tracking lazy operations.

    Tracks all lazy values created within the context,
    allowing for batch materialization and statistics.

    Attributes:
        lazy_values: List of lazy values created in this context
        materialize_on_exit: Whether to materialize all on exit
    """

    def __init__(self, materialize_on_exit: bool = False) -> None:
        """
        Initialize lazy context.

        Args:
            materialize_on_exit: Whether to materialize all lazy values on exit
        """
        self.lazy_values: list[LazyValue | LazySequence | LazyTransform] = []
        self.materialize_on_exit = materialize_on_exit
        self._original_lazy = lazy

    def __enter__(self) -> "LazyContext":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager."""
        if self.materialize_on_exit:
            self.materialize_all()

    def materialize_all(self) -> None:
        """Materialize all tracked lazy values."""
        logger.info(f"Materializing {len(self.lazy_values)} lazy values")
        for lazy_value in self.lazy_values:
            try:
                materialize(lazy_value)
            except Exception as e:
                logger.error(f"Failed to materialize lazy value: {e}")

    def get_stats(self) -> dict[str, Any]:
        """
        Get statistics about lazy values in this context.

        Returns:
            Dictionary with statistics
        """
        total_computed = sum(
            1 for v in self.lazy_values if isinstance(v, LazyValue) and v.is_computed()
        )

        total_failed = sum(
            1 for v in self.lazy_values if isinstance(v, LazyValue) and v.is_failed()
        )

        total_time = sum(
            v.computation_time_ms or 0
            for v in self.lazy_values
            if isinstance(v, LazyValue) and v.is_computed()
        )

        return {
            "total_lazy_values": len(self.lazy_values),
            "computed": total_computed,
            "failed": total_failed,
            "pending": len(self.lazy_values) - total_computed,
            "total_time_ms": total_time,
        }
