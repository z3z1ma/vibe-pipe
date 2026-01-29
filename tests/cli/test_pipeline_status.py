"""Tests for CLI pipeline status command."""

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


def test_pipeline_status_basic(runner: CliRunner, test_project_dir: Path):
    """Test basic pipeline status command."""
    result = runner.invoke(app, ["pipeline", "status", str(test_project_dir)])

    assert result.exit_code == 0
    assert "Pipeline Status" in result.stdout
    assert "test_pipeline" in result.stdout
    assert "Pipeline Information" in result.stdout


def test_pipeline_status_with_asset_filter(runner: CliRunner, test_project_dir: Path):
    """Test pipeline status with asset filter."""
    result = runner.invoke(app, ["pipeline", "status", str(test_project_dir), "--asset=test_asset"])

    # Should fail since there's no test_asset
    assert result.exit_code == 1


def test_pipeline_status_verbose(runner: CliRunner, test_project_dir: Path):
    """Test pipeline status with verbose flag."""
    result = runner.invoke(app, ["pipeline", "status", str(test_project_dir), "--verbose"])

    assert result.exit_code == 0
    assert "Pipeline Status" in result.stdout


def test_pipeline_status_invalid_project(runner: CliRunner, tmp_path: Path):
    """Test pipeline status with invalid project."""
    invalid_project = tmp_path / "nonexistent"
    result = runner.invoke(app, ["pipeline", "status", str(invalid_project)])

    assert result.exit_code == 2  # typer exit code for file not found
