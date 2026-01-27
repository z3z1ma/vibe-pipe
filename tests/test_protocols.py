"""
Unit tests for protocol interfaces.

Tests protocol implementations and ensures they work correctly
with structural subtyping.
"""

from vibe_piper.types import (
    Executable,
    Observable,
    PipelineContext,
    Schema,
    Sink,
    Source,
    Transformable,
    Validatable,
)


class TestValidatable:
    """Tests for Validatable protocol."""

    def test_custom_validatable_class(self) -> None:
        """Test a custom class implementing Validatable."""

        class CustomData:
            """Custom data class that implements Validatable."""

            def __init__(self, value: int) -> None:
                self.value = value

            def validate(self, schema: Schema) -> bool:
                """Validate against schema."""
                return self.value > 0

            def validation_errors(self, schema: Schema) -> list[str]:
                """Return validation errors."""
                if self.value <= 0:
                    return ["Value must be positive"]
                return []

        # Test that the class can be used as Validatable
        data: Validatable = CustomData(42)
        schema = Schema(name="test")

        assert data.validate(schema) is True
        assert len(data.validation_errors(schema)) == 0

        invalid_data: Validatable = CustomData(-1)
        assert invalid_data.validate(schema) is False
        assert invalid_data.validation_errors(schema) == ["Value must be positive"]


class TestTransformable:
    """Tests for Transformable protocol."""

    def test_custom_transformable_class(self) -> None:
        """Test a custom class implementing Transformable."""

        class CustomData:
            """Custom data class that implements Transformable."""

            def __init__(self, value: int) -> None:
                self.value = value

            def transform(self, operator, context: PipelineContext) -> "CustomData":
                """Transform using an operator."""
                new_value = operator.fn(self.value, context)
                return CustomData(new_value)

        from vibe_piper.types import Operator, OperatorType

        def double_fn(value: int, ctx: PipelineContext) -> int:
            return value * 2

        data: Transformable[CustomData] = CustomData(5)
        operator = Operator(
            name="double", operator_type=OperatorType.TRANSFORM, fn=double_fn
        )
        ctx = PipelineContext(pipeline_id="test", run_id="run1")

        result = data.transform(operator, ctx)
        assert result.value == 10


class TestExecutable:
    """Tests for Executable protocol."""

    def test_custom_executable_class(self) -> None:
        """Test a custom class implementing Executable."""

        class CustomTask:
            """Custom task class that implements Executable."""

            def __init__(self, result: int) -> None:
                self.result = result

            def execute(self, context: PipelineContext) -> int:
                """Execute the task."""
                context.set_state("executed", True)
                return self.result

        task: Executable = CustomTask(42)
        ctx = PipelineContext(pipeline_id="test", run_id="run1")

        result = task.execute(ctx)
        assert result == 42
        assert ctx.get_state("executed") is True


class TestSource:
    """Tests for Source protocol."""

    def test_custom_source_class(self) -> None:
        """Test a custom class implementing Source."""

        class CustomSource:
            """Custom source that implements Source."""

            def __init__(self, data: list[int]) -> None:
                self.data = data

            def read(self, context: PipelineContext) -> list[int]:
                """Read data from source."""
                context.set_state("rows_read", len(self.data))
                return self.data

        source: Source[list[int]] = CustomSource([1, 2, 3, 4, 5])
        ctx = PipelineContext(pipeline_id="test", run_id="run1")

        data = source.read(ctx)
        assert data == [1, 2, 3, 4, 5]
        assert ctx.get_state("rows_read") == 5


class TestSink:
    """Tests for Sink protocol."""

    def test_custom_sink_class(self) -> None:
        """Test a custom class implementing Sink."""

        class CustomSink:
            """Custom sink that implements Sink."""

            def __init__(self) -> None:
                self.written_data: list[int] = []

            def write(self, data: list[int], context: PipelineContext) -> None:
                """Write data to sink."""
                self.written_data.extend(data)
                context.set_state("rows_written", len(data))

        sink: Sink[list[int]] = CustomSink()
        ctx = PipelineContext(pipeline_id="test", run_id="run1")

        sink.write([1, 2, 3], ctx)
        assert sink.written_data == [1, 2, 3]
        assert ctx.get_state("rows_written") == 3


class TestObservable:
    """Tests for Observable protocol."""

    def test_custom_observable_class(self) -> None:
        """Test a custom class implementing Observable."""

        class CustomObservable:
            """Custom observable that tracks metrics."""

            def __init__(self) -> None:
                self.calls = 0

            def do_work(self) -> None:
                """Simulate work."""
                self.calls += 1

            def get_metrics(self) -> dict[str, int | float]:
                """Get current metrics."""
                return {"calls": self.calls, "efficiency": 0.95}

        observable: Observable = CustomObservable()
        observable.do_work()
        observable.do_work()

        metrics = observable.get_metrics()
        assert metrics["calls"] == 2
        assert metrics["efficiency"] == 0.95


class TestProtocolComposition:
    """Tests for combining multiple protocols."""

    def test_multi_protocol_class(self) -> None:
        """Test a class implementing multiple protocols."""

        class MultiProtocolData:
            """Class implementing Validatable, Transformable, and Observable."""

            def __init__(self, value: int) -> None:
                self.value = value
                self.transform_count = 0

            def validate(self, schema: Schema) -> bool:
                """Validate the data."""
                return self.value >= 0

            def validation_errors(self, schema: Schema) -> list[str]:
                """Return validation errors."""
                if self.value < 0:
                    return ["Value must be non-negative"]
                return []

            def transform(
                self, operator, context: PipelineContext
            ) -> "MultiProtocolData":
                """Transform the data."""
                new_value = operator.fn(self.value, context)
                result = MultiProtocolData(new_value)
                result.transform_count = self.transform_count + 1
                return result

            def get_metrics(self) -> dict[str, int | float]:
                """Get metrics."""
                return {"value": self.value, "transforms": self.transform_count}

        from vibe_piper.types import Operator, OperatorType

        def add_fn(value: int, ctx: PipelineContext) -> int:
            return value + 10

        # Can be used as any of the protocols
        data: Validatable = MultiProtocolData(5)
        schema = Schema(name="test")
        assert data.validate(schema) is True

        data2: Transformable[MultiProtocolData] = MultiProtocolData(5)
        operator = Operator(name="add", operator_type=OperatorType.TRANSFORM, fn=add_fn)
        ctx = PipelineContext(pipeline_id="test", run_id="run1")
        result = data2.transform(operator, ctx)
        assert result.value == 15

        obs: Observable = result
        metrics = obs.get_metrics()
        assert metrics["transforms"] == 1
