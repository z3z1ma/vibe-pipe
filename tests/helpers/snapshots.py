"""Snapshot testing framework for vibe_piper.

This module provides snapshot testing functionality to catch regressions in
data transformations and pipeline outputs. Snapshots are stored as JSON files
and compared against actual data during test runs.
"""

import json
import os
from difflib import unified_diff
from pathlib import Path
from typing import Any


class SnapshotMismatchError(AssertionError):
    """Raised when snapshot data doesn't match expected data."""

    def __init__(self, message: str, expected: Any, actual: Any, diff: str) -> None:
        """Initialize the snapshot mismatch error.

        Args:
            message: Error message
            expected: Expected data from snapshot
            actual: Actual data from test
            diff: Unified diff output
        """
        super().__init__(message)
        self.expected = expected
        self.actual = actual
        self.diff = diff


def _serialize_data(data: Any, max_depth: int | None = None) -> Any:
    """Serialize data to JSON-compatible format with depth protection.

    Args:
        data: Data to serialize
        max_depth: Maximum depth to traverse (None = unlimited)

    Returns:
        JSON-serializable data
    """
    if max_depth is not None and max_depth < 0:
        return "<max_depth_exceeded>"

    if isinstance(data, dict):
        if max_depth is not None:
            return {k: _serialize_data(v, max_depth - 1) for k, v in data.items()}
        return {k: _serialize_data(v, None) for k, v in data.items()}
    elif isinstance(data, list):
        if max_depth is not None:
            return [_serialize_data(item, max_depth - 1) for item in data]
        return [_serialize_data(item, None) for item in data]
    elif isinstance(data, tuple):
        if max_depth is not None:
            return [_serialize_data(item, max_depth - 1) for item in data]
        return [_serialize_data(item, None) for item in data]
    elif data is None or isinstance(data, (str, int, float, bool)):
        return data
    else:
        # Convert other types to string for serialization
        return str(data)


def _load_snapshot(snapshot_path: Path) -> Any:
    """Load snapshot data from file.

    Args:
        snapshot_path: Path to snapshot file

    Returns:
        Snapshot data

    Raises:
        FileNotFoundError: If snapshot doesn't exist
        json.JSONDecodeError: If snapshot is invalid JSON
    """
    with snapshot_path.open() as f:
        return json.load(f)


def _save_snapshot(snapshot_path: Path, data: Any) -> None:
    """Save snapshot data to file with sorted keys.

    Args:
        snapshot_path: Path to snapshot file
        data: Data to save
    """
    # Create parent directory if it doesn't exist
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)

    # Write with sorted keys for reproducibility
    with snapshot_path.open("w") as f:
        json.dump(data, f, sort_keys=True, indent=2)


def _should_update_snapshots() -> bool:
    """Check if snapshots should be updated.

    Returns:
        True if --update-snapshots flag is set or UPDATE_SNAPSHOTS env var is set
    """
    # Check environment variable (most reliable approach)
    return os.getenv("UPDATE_SNAPSHOTS") == "1"


def _generate_diff(expected: Any, actual: Any) -> str:
    """Generate unified diff for snapshot comparison.

    Args:
        expected: Expected data
        actual: Actual data

    Returns:
        String representation of the diff
    """
    expected_str = json.dumps(expected, sort_keys=True, indent=2)
    actual_str = json.dumps(actual, sort_keys=True, indent=2)

    diff_lines = list(
        unified_diff(
            expected_str.splitlines(keepends=True),
            actual_str.splitlines(keepends=True),
            fromfile="expected",
            tofile="actual",
            lineterm="",
        )
    )

    return "\n".join(diff_lines) if diff_lines else ""


def assert_matches_snapshot(
    data: Any,
    snapshot_path: Path | str,
    *,
    max_depth: int | None = None,
) -> None:
    """Assert that data matches the snapshot.

    On first run or with --update-snapshots flag, creates/updates the snapshot.
    On subsequent runs, compares data against the stored snapshot.

    Args:
        data: Data to compare against snapshot
        snapshot_path: Path to snapshot file (string or Path)
        max_depth: Maximum depth to compare (None = unlimited)

    Raises:
        SnapshotMismatchError: If data doesn't match snapshot
        FileNotFoundError: If snapshot doesn't exist and not updating
        json.JSONDecodeError: If snapshot is invalid JSON
    """
    if isinstance(snapshot_path, str):
        snapshot_path = Path(snapshot_path)

    # Serialize data with depth protection
    serialized_data = _serialize_data(data, max_depth)

    # Check if we should update snapshots
    if _should_update_snapshots():
        _save_snapshot(snapshot_path, serialized_data)
        return

    # Check if snapshot exists
    if not snapshot_path.exists():
        # Snapshot missing - fail unless updating
        error_message = (
            f"Snapshot file not found: {snapshot_path}\n"
            f"Run tests with --update-snapshots flag to create snapshot.\n"
            f"Example: uv run pytest --update-snapshots"
        )
        raise FileNotFoundError(error_message)

    # Load and compare snapshot
    try:
        expected_data = _load_snapshot(snapshot_path)
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(
            f"Invalid snapshot JSON in {snapshot_path}: {e.msg}", e.doc, e.pos
        ) from e

    # Compare serialized data
    if serialized_data != expected_data:
        diff = _generate_diff(expected_data, serialized_data)

        error_message = (
            f"Snapshot mismatch: {snapshot_path}\n\n"
            f"Expected:\n{json.dumps(expected_data, sort_keys=True, indent=2)}\n\n"
            f"Actual:\n{json.dumps(serialized_data, sort_keys=True, indent=2)}\n\n"
            f"Diff:\n{diff}"
        )

        raise SnapshotMismatchError(
            message=error_message,
            expected=expected_data,
            actual=serialized_data,
            diff=diff,
        )


# Alias for compatibility
assert_match_snapshot = assert_matches_snapshot


__all__ = [
    "SnapshotMismatchError",
    "assert_matches_snapshot",
    "assert_match_snapshot",
]
