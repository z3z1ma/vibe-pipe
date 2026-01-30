"""
Tests for error handling and recovery features.
"""

from datetime import datetime
from typing import Any
from unittest.mock import patch

import pytest

from vibe_piper import (
    Asset,
    AssetGraph,
    AssetResult,
    AssetType,
    BackoffStrategy,
    Checkpoint,
    CheckpointManager,
    CheckpointState,
    ErrorStrategy,
    ExecutionEngine,
    JitterStrategy,
    Operator,
    OperatorType,
    PipelineContext,
    RetryConfig,
    capture_error_context,
    retry_with_backoff,
)


class TestRetryConfig:
    """Tests for RetryConfig class."""

    def test_default_config(self) -> None:
        """Test creating a default retry config."""
        config = RetryConfig()

        assert config.max_retries == 3
        assert config.backoff_strategy == BackoffStrategy.EXPONENTIAL
        assert config.base_delay == 1.0
        assert config.max_delay == 60.0

    def test_custom_config(self) -> None:
        """Test creating a custom retry config."""
        config = RetryConfig(
            max_retries=5,
            backoff_strategy=BackoffStrategy.LINEAR,
            base_delay=2.0,
            max_delay=30.0,
        )

        assert config.max_retries == 5
        assert config.backoff_strategy == BackoffStrategy.LINEAR
        assert config.base_delay == 2.0
        assert config.max_delay == 30.0

    def test_calculate_delay_exponential(self) -> None:
        """Test exponential backoff delay calculation."""
        # Test with jitter disabled for exact values
        config = RetryConfig(
            backoff_strategy=BackoffStrategy.EXPONENTIAL,
            base_delay=1.0,
            jitter_strategy=JitterStrategy.NONE,
        )

        assert config.calculate_delay(0) == 1.0
        assert config.calculate_delay(1) == 2.0
        assert config.calculate_delay(2) == 4.0
        assert config.calculate_delay(3) == 8.0

    def test_calculate_delay_linear(self) -> None:
        """Test linear backoff delay calculation."""
        # Test with jitter disabled for exact values
        config = RetryConfig(
            backoff_strategy=BackoffStrategy.LINEAR,
            base_delay=1.0,
            jitter_strategy=JitterStrategy.NONE,
        )

        assert config.calculate_delay(0) == 1.0
        assert config.calculate_delay(1) == 2.0
        assert config.calculate_delay(2) == 3.0
        assert config.calculate_delay(3) == 4.0

    def test_calculate_delay_fixed(self) -> None:
        """Test fixed (no) backoff delay calculation."""
        config = RetryConfig(backoff_strategy=BackoffStrategy.FIXED)

        assert config.calculate_delay(0) == 0.0
        assert config.calculate_delay(1) == 0.0
        assert config.calculate_delay(2) == 0.0

    def test_calculate_delay_max_cap(self) -> None:
        """Test that delay is capped at max_delay."""
        config = RetryConfig(
            backoff_strategy=BackoffStrategy.EXPONENTIAL,
            base_delay=10.0,
            max_delay=15.0,
            jitter_strategy=JitterStrategy.NONE,
        )

        # Without cap, would be 10, 20, 40, etc.
        # With cap at 15.0
        assert config.calculate_delay(0) == 10.0
        assert config.calculate_delay(1) == 15.0  # Capped
        assert config.calculate_delay(2) == 15.0  # Capped


class TestRetryDecorator:
    """Tests for retry_with_backoff decorator."""

    def test_successful_execution_no_retry(self) -> None:
        """Test that successful function doesn't retry."""

        @retry_with_backoff(max_retries=3)
        def successful_func() -> str:
            return "success"

        result = successful_func()
        assert result == "success"

    def test_retry_on_failure_with_success(self) -> None:
        """Test retry logic that eventually succeeds."""
        attempts = [0]

        @retry_with_backoff(max_retries=3, base_delay=0.001)
        def flaky_func() -> str:
            attempts[0] += 1
            if attempts[0] < 3:
                raise ValueError("Not yet!")
            return "success"

        result = flaky_func()
        assert result == "success"
        assert attempts[0] == 3

    def test_retry_exhaustion(self) -> None:
        """Test that retries are exhausted and final error is raised."""

        @retry_with_backoff(max_retries=2, base_delay=0.001)
        def always_failing_func() -> str:
            raise ValueError("Always fails")

        with pytest.raises(ValueError, match="Always fails"):
            always_failing_func()

    def test_exponential_backoff(self) -> None:
        """Test exponential backoff strategy."""
        delays = []

        # Disable jitter for predictable delays
        @retry_with_backoff(max_retries=3, backoff="exponential", base_delay=0.1, jitter="none")
        def failing_func() -> str:
            raise ValueError("Fail")

        with patch("time.sleep") as mock_sleep:
            mock_sleep.side_effect = lambda d: delays.append(d)

            with pytest.raises(ValueError):
                failing_func()

        # Check exponential delays: 0.1, 0.2, 0.4, 0.8
        assert len(delays) == 3  # 3 retries after first failure
        assert delays[0] == 0.1
        assert delays[1] == 0.2
        assert delays[2] == 0.4

    def test_linear_backoff(self) -> None:
        """Test linear backoff strategy."""
        delays = []

        # Disable jitter for predictable delays
        @retry_with_backoff(max_retries=3, backoff="linear", base_delay=0.1, jitter="none")
        def failing_func() -> str:
            raise ValueError("Fail")

        with patch("time.sleep") as mock_sleep:
            mock_sleep.side_effect = lambda d: delays.append(d)

            with pytest.raises(ValueError):
                failing_func()

        # Check linear delays: 0.1, 0.2, 0.3, 0.4
        assert len(delays) == 3
        assert delays[0] == 0.1
        assert delays[1] == 0.2
        assert abs(delays[2] - 0.3) < 0.0001  # Floating point tolerance

    def test_invalid_backoff_strategy(self) -> None:
        """Test that invalid backoff strategy raises error."""

        with pytest.raises(ValueError, match="Invalid backoff strategy"):

            @retry_with_backoff(backoff="invalid_strategy")
            def some_func() -> str:
                return "result"

            some_func()


class TestErrorContext:
    """Tests for ErrorContext class."""

    def test_capture_error_context_basic(self) -> None:
        """Test capturing basic error context."""
        error = ValueError("Test error")

        context = capture_error_context(
            error=error,
            asset_name="test_asset",
        )

        assert context.error_type == "ValueError"
        assert context.error_message == "Test error"
        assert context.asset_name == "test_asset"
        assert context.stack_trace is not None
        assert len(context.stack_trace) > 0
        assert isinstance(context.timestamp, datetime)

    def test_capture_error_context_with_inputs(self) -> None:
        """Test capturing error context with inputs."""
        error = RuntimeError("Processing failed")

        inputs = {"param1": "value1", "param2": 42}

        context = capture_error_context(
            error=error,
            asset_name="failing_asset",
            inputs=inputs,
        )

        assert context.inputs == inputs
        assert context.error_type == "RuntimeError"

    def test_capture_error_context_with_retry_info(self) -> None:
        """Test capturing error context with retry information."""
        error = ConnectionError("Connection lost")

        context = capture_error_context(
            error=error,
            asset_name="api_call",
            attempt_number=2,
            retryable=True,
        )

        assert context.attempt_number == 2
        assert context.retryable is True

    def test_error_context_to_dict(self) -> None:
        """Test converting error context to dictionary."""
        error = ValueError("Test error")

        context = capture_error_context(
            error=error,
            asset_name="test_asset",
            metadata={"key": "value"},
        )

        error_dict = context.to_dict()

        assert isinstance(error_dict, dict)
        assert error_dict["error_type"] == "ValueError"
        assert error_dict["error_message"] == "Test error"
        assert error_dict["asset_name"] == "test_asset"
        assert error_dict["metadata"] == {"key": "value"}
        assert "timestamp" in error_dict


class TestCheckpoint:
    """Tests for Checkpoint and CheckpointState classes."""

    def test_create_checkpoint(self) -> None:
        """Test creating a checkpoint."""
        asset_result = AssetResult(
            asset_name="test_asset",
            success=True,
            data={"key": "value"},
        )

        checkpoint = Checkpoint(
            run_id="run-1",
            asset_name="test_asset",
            timestamp=datetime.now(),
            asset_result=asset_result,
        )

        assert checkpoint.run_id == "run-1"
        assert checkpoint.asset_name == "test_asset"
        assert checkpoint.asset_result.success is True

    def test_checkpoint_state(self) -> None:
        """Test checkpoint state management."""
        state = CheckpointState(
            pipeline_id="test_pipeline",
            run_id="run-1",
        )

        assert state.pipeline_id == "test_pipeline"
        assert state.run_id == "run-1"
        assert len(state.checkpoints) == 0
        assert state.last_checkpoint_asset is None

    def test_checkpoint_to_dict(self) -> None:
        """Test converting checkpoint to dictionary."""
        asset_result = AssetResult(
            asset_name="test_asset",
            success=True,
            data={"result": 42},
        )

        checkpoint = Checkpoint(
            run_id="run-1",
            asset_name="test_asset",
            timestamp=datetime.now(),
            asset_result=asset_result,
        )

        checkpoint_dict = checkpoint.to_dict()

        assert isinstance(checkpoint_dict, dict)
        assert checkpoint_dict["run_id"] == "run-1"
        assert checkpoint_dict["asset_name"] == "test_asset"
        assert "asset_result" in checkpoint_dict


class TestCheckpointManager:
    """Tests for CheckpointManager class."""

    def test_initialize_new_state(self) -> None:
        """Test initializing new checkpoint state."""
        manager = CheckpointManager()

        manager.initialize(
            pipeline_id="test_pipeline",
            run_id="run-1",
        )

        state = manager.get_checkpoint_state()
        assert state is not None
        assert state.pipeline_id == "test_pipeline"
        assert state.run_id == "run-1"

    def test_initialize_with_existing_state(self) -> None:
        """Test initializing with existing checkpoint state."""
        manager = CheckpointManager()

        existing_state = CheckpointState(
            pipeline_id="test_pipeline",
            run_id="run-1",
            last_checkpoint_asset="asset1",
        )

        manager.initialize(
            pipeline_id="test_pipeline",
            run_id="run-2",
            existing_state=existing_state,
        )

        state = manager.get_checkpoint_state()
        assert state is not None
        assert state.run_id == "run-1"  # Should use existing state's run_id

    def test_create_checkpoint(self) -> None:
        """Test creating a checkpoint."""
        manager = CheckpointManager()
        manager.initialize(pipeline_id="test", run_id="run-1")

        asset_result = AssetResult(
            asset_name="asset1",
            success=True,
            data="result",
        )

        checkpoint = manager.create_checkpoint(
            asset_name="asset1",
            asset_result=asset_result,
            upstream_results={},
        )

        assert checkpoint.asset_name == "asset1"
        assert checkpoint.asset_result.success is True

        state = manager.get_checkpoint_state()
        assert state is not None
        assert state.last_checkpoint_asset == "asset1"
        assert len(state.checkpoints) == 1

    def test_can_resume_from_asset(self) -> None:
        """Test checking if can resume from asset."""
        manager = CheckpointManager()
        manager.initialize(pipeline_id="test", run_id="run-1")

        # Before creating checkpoint
        assert not manager.can_resume_from_asset("asset1")

        # After creating checkpoint
        asset_result = AssetResult(asset_name="asset1", success=True)
        manager.create_checkpoint(
            asset_name="asset1",
            asset_result=asset_result,
            upstream_results={},
        )

        assert manager.can_resume_from_asset("asset1")

    def test_get_asset_result_from_checkpoint(self) -> None:
        """Test getting asset result from checkpoint."""
        manager = CheckpointManager()
        manager.initialize(pipeline_id="test", run_id="run-1")

        # Create checkpoint
        asset_result = AssetResult(
            asset_name="asset1",
            success=True,
            data="checkpointed_data",
        )
        manager.create_checkpoint(
            asset_name="asset1",
            asset_result=asset_result,
            upstream_results={},
        )

        # Retrieve result
        retrieved = manager.get_asset_result_from_checkpoint("asset1")
        assert retrieved is not None
        assert retrieved.success is True
        assert retrieved.data == "checkpointed_data"

    def test_get_execution_plan_from_checkpoint(self) -> None:
        """Test getting execution plan from checkpoint."""
        manager = CheckpointManager()
        manager.initialize(pipeline_id="test", run_id="run-1")

        full_order = ("asset1", "asset2", "asset3")

        # No checkpoint - should return full order
        plan = manager.get_execution_plan_from_checkpoint(full_order)
        assert plan == full_order

        # Create checkpoint for asset1
        asset_result = AssetResult(asset_name="asset1", success=True)
        manager.create_checkpoint(
            asset_name="asset1",
            asset_result=asset_result,
            upstream_results={},
        )

        # Should return assets after asset1
        plan = manager.get_execution_plan_from_checkpoint(full_order)
        assert plan == ("asset2", "asset3")


class TestExecutionEngineWithRetry:
    """Tests for ExecutionEngine with retry functionality."""

    @pytest.mark.skip(reason="ExecutionEngine retry integration not yet implemented")
    def test_asset_level_retry_config(self) -> None:
        """Test that asset-level retry config is respected."""
        attempt_count = [0]

        def flaky_op(data: Any, context: PipelineContext) -> Any:
            attempt_count[0] += 1
            if attempt_count[0] < 3:
                raise ValueError("Fail")
            return "success"

        operator = Operator(
            name="flaky",
            operator_type=OperatorType.TRANSFORM,
            fn=flaky_op,
        )

        # Asset with retry config
        asset = Asset(
            name="flaky_asset",
            asset_type=AssetType.MEMORY,
            uri="memory://flaky_asset",
            operator=operator,
            config={"retries": 3, "backoff": "fixed"},
        )

        graph = AssetGraph(name="test", assets=(asset,))

        engine = ExecutionEngine()
        result = engine.execute(graph)

        assert result.success is True
        assert attempt_count[0] == 3  # Initial attempt + 2 retries

    @pytest.mark.skip(reason="ExecutionEngine retry integration not yet implemented")
    def test_engine_level_retry_config(self) -> None:
        """Test that engine-level retry config is used when asset has no config."""
        attempt_count = [0]

        def flaky_op(data: Any, context: PipelineContext) -> Any:
            attempt_count[0] += 1
            if attempt_count[0] < 2:
                raise ValueError("Fail")
            return "success"

        operator = Operator(
            name="flaky",
            operator_type=OperatorType.TRANSFORM,
            fn=flaky_op,
        )

        asset = Asset(
            name="flaky_asset",
            asset_type=AssetType.MEMORY,
            uri="memory://flaky_asset",
            operator=operator,
            # No retry config at asset level
        )

        graph = AssetGraph(name="test", assets=(asset,))

        engine = ExecutionEngine(max_retries=2)
        result = engine.execute(graph)

        assert result.success is True
        assert attempt_count[0] == 2  # Initial attempt + 1 retry


class TestExecutionEngineWithCheckpoints:
    """Tests for ExecutionEngine with checkpoint functionality."""

    @pytest.mark.skip(reason="ExecutionEngine does not support enable_checkpoints parameter yet")
    def test_checkpointing_enabled(self) -> None:
        """Test that checkpoints are created when enabled."""

        def simple_op(data: Any, context: PipelineContext) -> Any:
            return "result"

        operator = Operator(
            name="simple",
            operator_type=OperatorType.TRANSFORM,
            fn=simple_op,
        )

        asset = Asset(
            name="test_asset",
            asset_type=AssetType.MEMORY,
            uri="memory://test_asset",
            operator=operator,
        )

        graph = AssetGraph(name="test", assets=(asset,))

        engine = ExecutionEngine(enable_checkpoints=True)
        result = engine.execute(graph)

        assert result.success is True
        # Check that checkpoint was created
        assert engine.checkpoint_manager.can_resume_from_asset("test_asset")

    @pytest.mark.skip(reason="ExecutionEngine does not support enable_checkpoints parameter yet")
    def test_checkpointing_disabled(self) -> None:
        """Test that checkpoints are not created when disabled."""

        def simple_op(data: Any, context: PipelineContext) -> Any:
            return "result"

        operator = Operator(
            name="simple",
            operator_type=OperatorType.TRANSFORM,
            fn=simple_op,
        )

        asset = Asset(
            name="test_asset",
            asset_type=AssetType.MEMORY,
            uri="memory://test_asset",
            operator=operator,
        )

        graph = AssetGraph(name="test", assets=(asset,))

        engine = ExecutionEngine(enable_checkpoints=False)
        result = engine.execute(graph)

        assert result.success is True
        # Check that checkpoint was NOT created
        assert not engine.checkpoint_manager.can_resume_from_asset("test_asset")

    @pytest.mark.skip(reason="ExecutionEngine does not support enable_checkpoints parameter yet")
    def test_checkpoint_recovery_skip_executed_assets(self) -> None:
        """Test that checkpoint recovery skips already executed assets."""
        execution_count = [0]

        def simple_op(data: Any, context: PipelineContext) -> Any:
            execution_count[0] += 1
            return "result"

        operator = Operator(
            name="simple",
            operator_type=OperatorType.TRANSFORM,
            fn=simple_op,
        )

        asset1 = Asset(
            name="asset1",
            asset_type=AssetType.MEMORY,
            uri="memory://asset1",
            operator=operator,
        )
        asset2 = Asset(
            name="asset2",
            asset_type=AssetType.MEMORY,
            uri="memory://asset2",
            operator=operator,
        )

        graph = AssetGraph(
            name="test",
            assets=(asset1, asset2),
            dependencies={"asset2": ("asset1",)},
        )

        engine = ExecutionEngine(enable_checkpoints=True)

        # First execution - both assets should run
        result1 = engine.execute(graph)
        assert result1.success is True
        assert execution_count[0] == 2

        # Get the checkpoint state from first execution
        checkpoint_state = engine.checkpoint_manager.get_checkpoint_state()
        assert checkpoint_state is not None
        assert checkpoint_state.last_checkpoint_asset == "asset2"

        # Create a new engine instance (simulating a restart)
        engine2 = ExecutionEngine(enable_checkpoints=True)

        # Manually restore checkpoint state (simulating loading from storage)
        engine2.checkpoint_manager.initialize(
            pipeline_id=graph.name,
            run_id="recovery-run",
            existing_state=checkpoint_state,
        )

        # Reset counter
        execution_count[0] = 0

        # Second execution with recovery - both assets should be skipped
        # because we have checkpoints for both
        result2 = engine2.execute(graph, recover_from_checkpoint=True)
        assert result2.success is True
        assert execution_count[0] == 0  # Both assets should be skipped


class TestErrorStrategyBehaviors:
    """Tests for error strategy behaviors."""

    def test_fail_fast_stops_on_first_error(self) -> None:
        """Test that FAIL_FAST stops execution on first error."""
        execution_log = []

        def success_op(data: Any, context: PipelineContext) -> Any:
            execution_log.append("success")
            return "success"

        def failing_op(data: Any, context: PipelineContext) -> Any:
            execution_log.append("fail")
            raise ValueError("Fail")

        success_operator = Operator(
            name="success",
            operator_type=OperatorType.TRANSFORM,
            fn=success_op,
        )
        failing_operator = Operator(
            name="failing",
            operator_type=OperatorType.TRANSFORM,
            fn=failing_op,
        )

        asset1 = Asset(
            name="asset1",
            asset_type=AssetType.MEMORY,
            uri="memory://asset1",
            operator=success_operator,
        )
        asset2 = Asset(
            name="asset2",
            asset_type=AssetType.MEMORY,
            uri="memory://asset2",
            operator=failing_operator,
        )
        asset3 = Asset(
            name="asset3",
            asset_type=AssetType.MEMORY,
            uri="memory://asset3",
            operator=success_operator,
        )

        graph = AssetGraph(
            name="test",
            assets=(asset1, asset2, asset3),
            dependencies={"asset2": ("asset1",), "asset3": ("asset2",)},
        )

        engine = ExecutionEngine(error_strategy=ErrorStrategy.FAIL_FAST)
        result = engine.execute(graph)

        assert result.success is False
        assert result.assets_failed == 1
        # asset3 should not be executed
        assert "asset3" not in result.asset_results

    def test_continue_executes_all_assets(self) -> None:
        """Test that CONTINUE executes all assets despite errors."""
        execution_log = []

        def success_op(data: Any, context: PipelineContext) -> Any:
            execution_log.append("success")
            return "success"

        def failing_op(data: Any, context: PipelineContext) -> Any:
            execution_log.append("fail")
            raise ValueError("Fail")

        success_operator = Operator(
            name="success",
            operator_type=OperatorType.TRANSFORM,
            fn=success_op,
        )
        failing_operator = Operator(
            name="failing",
            operator_type=OperatorType.TRANSFORM,
            fn=failing_op,
        )

        asset1 = Asset(
            name="asset1",
            asset_type=AssetType.MEMORY,
            uri="memory://asset1",
            operator=success_operator,
        )
        asset2 = Asset(
            name="asset2",
            asset_type=AssetType.MEMORY,
            uri="memory://asset2",
            operator=failing_operator,
        )
        asset3 = Asset(
            name="asset3",
            asset_type=AssetType.MEMORY,
            uri="memory://asset3",
            operator=success_operator,
        )

        graph = AssetGraph(
            name="test",
            assets=(asset1, asset2, asset3),
            # No dependencies - assets can run independently
        )

        engine = ExecutionEngine(error_strategy=ErrorStrategy.CONTINUE)
        result = engine.execute(graph)

        assert result.success is False
        assert result.assets_failed == 1
        assert result.assets_succeeded == 2
        # All assets should be in results
        assert "asset1" in result.asset_results
        assert "asset2" in result.asset_results
        assert "asset3" in result.asset_results
