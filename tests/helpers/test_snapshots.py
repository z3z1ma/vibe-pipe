"""Tests for snapshot testing framework."""

import json
from pathlib import Path
from typing import Any

import pytest

from tests.helpers.snapshots import (
    SnapshotMismatchError,
    assert_match_snapshot,
    assert_matches_snapshot,
)


class TestSnapshotCreation:
    """Test snapshot file creation on first run."""

    def test_creates_snapshot_file_on_first_run(self, tmp_path: Path) -> None:
        """Test that snapshot is created when it doesn't exist."""
        snapshot_path = tmp_path / "test_snapshot.json"
        test_data = {"name": "Alice", "age": 30, "email": "alice@example.com"}

        # First run should create snapshot and pass
        assert_matches_snapshot(test_data, snapshot_path)

        # Verify snapshot file was created
        assert snapshot_path.exists()

        # Verify content matches
        with snapshot_path.open() as f:
            stored_data = json.load(f)
        assert stored_data == test_data

    def test_creates_directory_if_needed(self, tmp_path: Path) -> None:
        """Test that snapshot directory is created if it doesn't exist."""
        snapshot_path = tmp_path / "nested" / "dir" / "test_snapshot.json"
        test_data = {"value": 123}

        assert_matches_snapshot(test_data, snapshot_path)

        assert snapshot_path.exists()
        assert snapshot_path.parent.is_dir()

    def test_creates_sorted_json_for_reproducibility(self, tmp_path: Path) -> None:
        """Test that JSON keys are sorted for reproducible snapshots."""
        snapshot_path = tmp_path / "test_snapshot.json"
        test_data = {"z": 1, "a": 2, "m": 3, "c": 4}

        assert_matches_snapshot(test_data, snapshot_path)

        with snapshot_path.open() as f:
            content = f.read()

        # Keys should be in sorted order (with indentation)
        expected_lines = ["{", '  "a": 2,', '  "c": 4,', '  "m": 3,', '  "z": 1', "}"]
        actual_lines = content.split("\n")
        assert actual_lines == expected_lines


class TestSnapshotComparison:
    """Test snapshot comparison with expected data."""

    def test_passes_when_data_matches(self, tmp_path: Path) -> None:
        """Test that assertion passes when data matches snapshot."""
        snapshot_path = tmp_path / "test_snapshot.json"
        test_data = {"id": 1, "name": "Alice"}

        # Create snapshot
        assert_matches_snapshot(test_data, snapshot_path)

        # Verify matching data passes
        assert_matches_snapshot(test_data, snapshot_path)

    def test_fails_when_data_differs(self, tmp_path: Path) -> None:
        """Test that assertion fails when data differs from snapshot."""
        snapshot_path = tmp_path / "test_snapshot.json"
        original_data = {"id": 1, "name": "Alice"}

        # Create snapshot
        assert_matches_snapshot(original_data, snapshot_path)

        # Different data should fail
        different_data = {"id": 1, "name": "Bob"}

        with pytest.raises(SnapshotMismatchError) as exc_info:
            assert_matches_snapshot(different_data, snapshot_path)

        assert "Alice" in str(exc_info.value)
        assert "Bob" in str(exc_info.value)

    def test_fails_with_diff_output(self, tmp_path: Path) -> None:
        """Test that failure includes readable diff output."""
        snapshot_path = tmp_path / "test_snapshot.json"
        original_data = {
            "users": [
                {"id": 1, "name": "Alice", "active": True},
                {"id": 2, "name": "Bob", "active": False},
            ]
        }

        # Create snapshot
        assert_matches_snapshot(original_data, snapshot_path)

        # Modify data
        modified_data = {
            "users": [
                {"id": 1, "name": "Alice", "active": True},
                {"id": 2, "name": "Charlie", "active": True},
            ]
        }

        with pytest.raises(SnapshotMismatchError) as exc_info:
            assert_matches_snapshot(modified_data, snapshot_path)

        error_msg = str(exc_info.value)
        assert "expected" in error_msg.lower() or "Expected" in error_msg
        assert "actual" in error_msg.lower() or "Actual" in error_msg
        # Diff should show the differences
        assert "Bob" in error_msg or "Charlie" in error_msg


class TestUpdateSnapshotsFlag:
    """Test --update-snapshots flag functionality."""

    def test_update_flag_overwrites_existing_snapshot(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that --update-snapshots overwrites existing snapshot."""
        snapshot_path = tmp_path / "test_snapshot.json"
        original_data = {"id": 1, "name": "Alice"}

        # Create snapshot
        assert_matches_snapshot(original_data, snapshot_path)

        # Mock update flag
        monkeypatch.setenv("UPDATE_SNAPSHOTS", "1")

        # New data should update snapshot
        new_data = {"id": 1, "name": "Bob", "email": "bob@example.com"}
        assert_matches_snapshot(new_data, snapshot_path)

        # Verify snapshot was updated
        with snapshot_path.open() as f:
            stored_data = json.load(f)
        assert stored_data == new_data

    def test_update_flag_creates_new_snapshot(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that --update-snapshots creates snapshot if it doesn't exist."""
        snapshot_path = tmp_path / "new_snapshot.json"
        test_data = {"value": "test"}

        # Mock update flag
        monkeypatch.setenv("UPDATE_SNAPSHOTS", "1")

        assert_matches_snapshot(test_data, snapshot_path)

        assert snapshot_path.exists()


class TestMaxDepthProtection:
    """Test max depth protection for nested structures."""

    def test_respects_max_depth_parameter(self, tmp_path: Path) -> None:
        """Test that max depth limits how deep we compare nested structures."""
        snapshot_path = tmp_path / "test_snapshot.json"
        nested_data = {"level1": {"level2": {"level3": {"deep": "value"}}}}

        # Create snapshot with max_depth=2
        assert_matches_snapshot(nested_data, snapshot_path, max_depth=2)

        # Modify data beyond max_depth (should pass)
        modified_data = {"level1": {"level2": {"level3": {"deep": "different"}}}}
        assert_matches_snapshot(modified_data, snapshot_path, max_depth=2)

    def test_compares_all_levels_within_max_depth(
        self,
        tmp_path: Path,
    ) -> None:
        """Test that all levels within max_depth are compared."""
        snapshot_path = tmp_path / "test_snapshot.json"
        nested_data = {"level1": {"level2": {"level3": "value"}}}

        # Create snapshot with max_depth=3
        assert_matches_snapshot(nested_data, snapshot_path, max_depth=3)

        # Modify data at level 2 (within max_depth - should fail)
        modified_data = {"level1": {"level2": {"level3": "different"}}}

        with pytest.raises(SnapshotMismatchError):
            assert_matches_snapshot(modified_data, snapshot_path, max_depth=3)


class TestEdgeCases:
    """Test edge cases (empty data, None, nested structures)."""

    def test_empty_dict(self, tmp_path: Path) -> None:
        """Test snapshot with empty dictionary."""
        snapshot_path = tmp_path / "empty.json"

        assert_matches_snapshot({}, snapshot_path)
        assert snapshot_path.exists()

        with snapshot_path.open() as f:
            assert json.load(f) == {}

    def test_empty_list(self, tmp_path: Path) -> None:
        """Test snapshot with empty list."""
        snapshot_path = tmp_path / "empty.json"

        assert_matches_snapshot([], snapshot_path)
        assert snapshot_path.exists()

        with snapshot_path.open() as f:
            assert json.load(f) == []

    def test_none_value(self, tmp_path: Path) -> None:
        """Test snapshot with None value."""
        snapshot_path = tmp_path / "none.json"

        assert_matches_snapshot(None, snapshot_path)
        assert snapshot_path.exists()

        with snapshot_path.open() as f:
            assert json.load(f) is None

    def test_deeply_nested_structure(self, tmp_path: Path) -> None:
        """Test snapshot with deeply nested structure."""
        snapshot_path = tmp_path / "nested.json"
        nested = {"a": {"b": {"c": {"d": {"e": "deep"}}}}}

        assert_matches_snapshot(nested, snapshot_path)

        with snapshot_path.open() as f:
            assert json.load(f) == nested

    def test_list_of_dicts(self, tmp_path: Path) -> None:
        """Test snapshot with list of dictionaries."""
        snapshot_path = tmp_path / "list_dicts.json"
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

        assert_matches_snapshot(data, snapshot_path)

        with snapshot_path.open() as f:
            assert json.load(f) == data

    def test_mixed_types(self, tmp_path: Path) -> None:
        """Test snapshot with mixed data types."""
        snapshot_path = tmp_path / "mixed.json"
        data = {
            "string": "test",
            "int": 42,
            "float": 3.14,
            "bool": True,
            "null": None,
            "list": [1, 2, 3],
            "dict": {"nested": "value"},
        }

        assert_matches_snapshot(data, snapshot_path)

        with snapshot_path.open() as f:
            assert json.load(f) == data


class TestAlternativeFunctionNames:
    """Test that both function names work the same way."""

    def test_assert_match_snapshot_alias(self, tmp_path: Path) -> None:
        """Test that assert_match_snapshot is an alias for assert_matches_snapshot."""
        snapshot_path = tmp_path / "test.json"
        data = {"value": "test"}

        # Both should work identically
        assert_match_snapshot(data, snapshot_path)
        assert_matches_snapshot(data, snapshot_path)
