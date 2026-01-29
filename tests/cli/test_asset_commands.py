"""Tests for CLI asset commands."""

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


def test_asset_list_basic(runner: CliRunner, test_project_dir: Path):
    """Test basic asset list command."""
    result = runner.invoke(app, ["asset", "list", str(test_project_dir)])

    assert result.exit_code == 0
    assert "Pipeline Assets" in result.stdout


def test_asset_list_with_type_filter(runner: CliRunner, test_project_dir: Path):
    """Test asset list with type filter."""
    result = runner.invoke(app, ["asset", "list", str(test_project_dir), "--type", "source"])

    # Should succeed even if no assets match
    assert result.exit_code == 0


def test_asset_list_invalid_type(runner: CliRunner, test_project_dir: Path):
    """Test asset list with invalid type filter."""
    result = runner.invoke(app, ["asset", "list", str(test_project_dir), "--type", "invalid"])

    assert result.exit_code == 1
    assert "Invalid asset type" in result.stdout


def test_asset_list_verbose(runner: CliRunner, test_project_dir: Path):
    """Test asset list with verbose flag."""
    result = runner.invoke(app, ["asset", "list", str(test_project_dir), "--verbose"])

    assert result.exit_code == 0


def test_asset_show_missing_asset(runner: CliRunner, test_project_dir: Path):
    """Test asset show with non-existent asset."""
    result = runner.invoke(app, ["asset", "show", "nonexistent_asset", str(test_project_dir)])

    assert result.exit_code == 1
    assert "not found" in result.stdout


def test_asset_show_invalid_project(runner: CliRunner, tmp_path: Path):
    """Test asset show with invalid project."""
    invalid_project = tmp_path / "nonexistent"
    result = runner.invoke(app, ["asset", "show", "test_asset", str(invalid_project)])

    assert result.exit_code == 2
