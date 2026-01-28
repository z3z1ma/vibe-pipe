"""
Tests for materialization strategies.

This module tests the materialization strategy implementations including
table, view, file, and incremental strategies.
"""

import pytest

from vibe_piper.materialization import (
    FileStrategy,
    IncrementalStrategy,
    TableStrategy,
    ViewStrategy,
)
from vibe_piper.types import MaterializationStrategy, PipelineContext

# =============================================================================
# Table Strategy Tests
# =============================================================================


class TestTableStrategy:
    """Tests for TableStrategy."""

    def test_table_strategy_initialization(self):
        """Test TableStrategy can be initialized."""
        strategy = TableStrategy()
        assert strategy.strategy_type == MaterializationStrategy.TABLE
        assert strategy.key is None
        assert strategy.config == {}

    def test_table_strategy_with_config(self):
        """Test TableStrategy can be initialized with config."""
        config = {"partitioned": True}
        strategy = TableStrategy(config=config)
        assert strategy.config == config

    def test_table_strategy_should_materialize(self, context):
        """Test TableStrategy always materializes."""
        strategy = TableStrategy()
        assert strategy.should_materialize(context) is True

    def test_table_strategy_prepare_for_storage(self, context):
        """Test TableStrategy returns data unchanged."""
        strategy = TableStrategy()
        data = [{"id": 1}, {"id": 2}]

        result = strategy.prepare_for_storage(context, data)
        assert result == data

    def test_table_strategy_ignores_existing_data(self, context):
        """Test TableStrategy ignores existing data (full refresh)."""
        strategy = TableStrategy()
        new_data = [{"id": 3}]
        existing_data = [{"id": 1}, {"id": 2}]

        result = strategy.prepare_for_storage(context, new_data, existing_data)
        assert result == new_data

    def test_table_strategy_storage_metadata(self, context):
        """Test TableStrategy storage metadata."""
        strategy = TableStrategy()
        metadata = strategy.get_storage_metadata(context)

        assert "strategy" in metadata
        assert metadata["strategy"] == "TABLE"


# =============================================================================
# View Strategy Tests
# =============================================================================


class TestViewStrategy:
    """Tests for ViewStrategy."""

    def test_view_strategy_initialization(self):
        """Test ViewStrategy can be initialized."""
        strategy = ViewStrategy()
        assert strategy.strategy_type == MaterializationStrategy.VIEW
        assert strategy.key is None
        assert strategy.config == {}

    def test_view_strategy_should_not_materialize(self, context):
        """Test ViewStrategy never materializes."""
        strategy = ViewStrategy()
        assert strategy.should_materialize(context) is False

    def test_view_strategy_prepare_for_storage(self, context):
        """Test ViewStrategy returns data unchanged but not stored."""
        strategy = ViewStrategy()
        data = [{"id": 1}, {"id": 2}]

        result = strategy.prepare_for_storage(context, data)
        assert result == data


# =============================================================================
# File Strategy Tests
# =============================================================================


class TestFileStrategy:
    """Tests for FileStrategy."""

    def test_file_strategy_initialization(self):
        """Test FileStrategy can be initialized."""
        strategy = FileStrategy()
        assert strategy.strategy_type == MaterializationStrategy.FILE
        assert strategy.key is None

    def test_file_strategy_with_partition_key(self):
        """Test FileStrategy with partition key."""
        strategy = FileStrategy(partition_key="date")
        assert strategy.key == "date"

    def test_file_strategy_should_materialize(self, context):
        """Test FileStrategy always materializes."""
        strategy = FileStrategy()
        assert strategy.should_materialize(context) is True

    def test_file_strategy_prepare_for_storage(self, context):
        """Test FileStrategy returns data unchanged."""
        strategy = FileStrategy()
        data = [{"id": 1}, {"id": 2}]

        result = strategy.prepare_for_storage(context, data)
        assert result == data

    def test_file_strategy_storage_metadata(self, context):
        """Test FileStrategy storage metadata includes partition key."""
        strategy = FileStrategy(partition_key="date")
        metadata = strategy.get_storage_metadata(context)

        assert "strategy" in metadata
        assert metadata["strategy"] == "FILE"
        assert "partition_key" in metadata
        assert metadata["partition_key"] == "date"

    def test_file_strategy_storage_metadata_without_partition(self, context):
        """Test FileStrategy storage metadata without partition key."""
        strategy = FileStrategy()
        metadata = strategy.get_storage_metadata(context)

        assert "strategy" in metadata
        assert metadata["strategy"] == "FILE"
        assert "partition_key" not in metadata


# =============================================================================
# Incremental Strategy Tests
# =============================================================================


class TestIncrementalStrategy:
    """Tests for IncrementalStrategy."""

    def test_incremental_strategy_initialization(self):
        """Test IncrementalStrategy can be initialized with a key."""
        strategy = IncrementalStrategy(key="id")
        assert strategy.strategy_type == MaterializationStrategy.INCREMENTAL
        assert strategy.key == "id"

    def test_incremental_strategy_requires_key(self):
        """Test IncrementalStrategy requires a key."""
        with pytest.raises(ValueError, match="requires a 'key' parameter"):
            IncrementalStrategy(key="")

    def test_incremental_strategy_should_materialize(self, context):
        """Test IncrementalStrategy always materializes."""
        strategy = IncrementalStrategy(key="id")
        assert strategy.should_materialize(context) is True

    def test_incremental_strategy_with_new_data_only(self, context):
        """Test IncrementalStrategy with only new data (no existing data)."""
        strategy = IncrementalStrategy(key="id")
        new_data = [{"id": 1, "value": "a"}]

        result = strategy.prepare_for_storage(context, new_data, None)
        assert result == new_data

    def test_incremental_strategy_with_existing_data(self, context):
        """Test IncrementalStrategy appends new records to existing data."""
        strategy = IncrementalStrategy(key="id")
        existing_data = [{"id": 1, "value": "a"}]
        new_data = [{"id": 2, "value": "b"}]

        result = strategy.prepare_for_storage(context, new_data, existing_data)

        # Should contain both records
        assert len(result) == 2
        assert {"id": 1, "value": "a"} in result
        assert {"id": 2, "value": "b"} in result

    def test_incremental_strategy_upsert_logic(self, context):
        """Test IncrementalStrategy upsert logic (update existing records)."""
        strategy = IncrementalStrategy(key="id")
        existing_data = [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]
        new_data = [{"id": 2, "value": "b_updated"}, {"id": 3, "value": "c"}]

        result = strategy.prepare_for_storage(context, new_data, existing_data)

        # Should update id=2 and add id=3
        assert len(result) == 3
        assert {"id": 1, "value": "a"} in result
        assert {"id": 2, "value": "b_updated"} in result
        assert {"id": 3, "value": "c"} in result

    def test_incremental_strategy_merges_lists(self, context):
        """Test IncrementalStrategy correctly merges list data."""
        strategy = IncrementalStrategy(key="date")
        existing_data = [
            {"date": "2024-01-01", "sales": 100},
            {"date": "2024-01-02", "sales": 150},
        ]
        new_data = [
            {"date": "2024-01-02", "sales": 200},  # Update
            {"date": "2024-01-03", "sales": 180},  # New
        ]

        result = strategy.prepare_for_storage(context, new_data, existing_data)

        assert len(result) == 3
        # Find updated record
        updated = next(r for r in result if r["date"] == "2024-01-02")
        assert updated["sales"] == 200

    def test_incremental_strategy_handles_dict_data(self, context):
        """Test IncrementalStrategy handles dict-based data."""
        strategy = IncrementalStrategy(key="id")
        existing_data = {1: {"name": "Alice"}, 2: {"name": "Bob"}}
        new_data = {2: {"name": "Robert"}, 3: {"name": "Charlie"}}

        result = strategy.prepare_for_storage(context, new_data, existing_data)

        # Should merge dicts
        assert 1 in result
        assert result[1] == {"name": "Alice"}
        assert result[2] == {"name": "Robert"}
        assert result[3] == {"name": "Charlie"}

    def test_incremental_strategy_handles_non_dict_items(self, context):
        """Test IncrementalStrategy handles non-dict list items."""
        strategy = IncrementalStrategy(key="id")
        existing_data = [{"id": 1, "value": "a"}]
        new_data = [{"id": 2, "value": "b"}]

        result = strategy.prepare_for_storage(context, new_data, existing_data)

        assert len(result) == 2

    def test_incremental_strategy_returns_existing_if_no_new(self, context):
        """Test IncrementalStrategy returns existing data if no new data."""
        strategy = IncrementalStrategy(key="id")
        existing_data = [{"id": 1, "value": "a"}]

        result = strategy.prepare_for_storage(context, None, existing_data)

        assert result == existing_data

    def test_incremental_strategy_storage_metadata(self, context):
        """Test IncrementalStrategy storage metadata includes key."""
        strategy = IncrementalStrategy(key="date")
        metadata = strategy.get_storage_metadata(context)

        assert "strategy" in metadata
        assert metadata["strategy"] == "INCREMENTAL"
        assert "incremental_key" in metadata
        assert metadata["incremental_key"] == "date"


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def context():
    """Provide a PipelineContext for testing."""
    return PipelineContext(pipeline_id="test_pipeline", run_id="test_run")
