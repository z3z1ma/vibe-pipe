"""Tests for CLI pipeline history and backfill commands."""

from pathlib import Path

import pytest
from typer.testing import CliRunner

from vibe_piper.cli.main import app


@pytest.fixture
def runner():
    """Create a CLI runner."""
    return CliRunner()


@pytest.fixture
def test_project_dir(tmp_path: Path) -> Path:
    """Create a test project directory."""
    project_dir = tmp_path / "test_pipeline"
    project_dir.mkdir()
    (project_dir / "src").mkdir()
    (project_dir / "config").mkdir()

    # Create config file
    config_content = """
[project]
name = "test_pipeline"
version = "0.1.0"
description = "Test pipeline for CLI testing"

[environments]
dev = {}
    """
    (project_dir / "config" / "pipeline.toml").write_text(config_content)

    # Create minimal pipeline file
    pipeline_content = """
\"\"\"Test pipeline definition.\"\"\"

from vibe_piper.pipeline import PipelineBuilder

def create_pipeline():
    \"\"\"Create test pipeline.\"\"\"
    builder = PipelineBuilder("test_pipeline", description="Test pipeline")
    return builder.build()
    """
    (project_dir / "src" / "pipeline.py").write_text(pipeline_content)

    return project_dir


def test_pipeline_history_no_runs(runner: CliRunner, test_project_dir: Path):
    """Test pipeline history with no runs."""
    result = runner.invoke(app, ["pipeline", "history", str(test_project_dir)])

    assert result.exit_code == 0
    assert "No run history found" in result.stdout


def test_pipeline_history_limit(runner: CliRunner, test_project_dir: Path):
    """Test pipeline history with limit."""
    result = runner.invoke(app, ["pipeline", "history", str(test_project_dir), "--limit", "5"])

    assert result.exit_code == 0


def test_pipeline_history_successful_only(runner: CliRunner, test_project_dir: Path):
    """Test pipeline history with successful-only filter."""
    result = runner.invoke(app, ["pipeline", "history", str(test_project_dir), "--successful-only"])

    assert result.exit_code == 0


def test_pipeline_history_failed_only(runner: CliRunner, test_project_dir: Path):
    """Test pipeline history with failed-only filter."""
    result = runner.invoke(app, ["pipeline", "history", str(test_project_dir), "--failed-only"])

    assert result.exit_code == 0


def test_backfill_missing_dates(runner: CliRunner, test_project_dir: Path):
    """Test backfill command with missing dates."""
    result = runner.invoke(app, ["pipeline", "backfill", str(test_project_dir)])

    assert result.exit_code == 2  # typer exit code for missing required args


def test_backfill_invalid_date_format(runner: CliRunner, test_project_dir: Path):
    """Test backfill command with invalid date format."""
    result = runner.invoke(
        app,
        [
            "pipeline",
            "backfill",
            str(test_project_dir),
            "--start-date",
            "2024/01/01",
            "--end-date",
            "2024/01/31",
        ],
    )

    assert result.exit_code == 1
    assert "Invalid date format" in result.stdout


def test_backfill_invalid_date_range(runner: CliRunner, test_project_dir: Path):
    """Test backfill command with invalid date range."""
    result = runner.invoke(
        app,
        [
            "pipeline",
            "backfill",
            str(test_project_dir),
            "--start-date",
            "2024-12-31",
            "--end-date",
            "2024-01-01",
        ],
    )

    assert result.exit_code == 1
    assert "must be before end date" in result.stdout


def test_backfill_dry_run(runner: CliRunner, test_project_dir: Path):
    """Test backfill command with dry-run flag."""
    result = runner.invoke(
        app,
        [
            "pipeline",
            "backfill",
            str(test_project_dir),
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-02",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    assert "Dry run mode" in result.stdout
    assert "Dry run complete" in result.stdout
