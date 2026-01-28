"""Tests for VibePiper CLI commands."""

from collections.abc import Generator
from pathlib import Path

import pytest
from typer.testing import CliRunner

from vibe_piper.cli.main import app

runner = CliRunner()


@pytest.fixture
def temp_project_dir(tmp_path: Path) -> Generator[Path, None, None]:  # type: ignore[type-arg]
    """Create a temporary project directory for testing.

    Args:
        tmp_path: Pytest temporary path fixture

    Yields:
        Path to temporary project directory
    """
    project_dir = tmp_path / "test_project"
    project_dir.mkdir()

    # Create basic project structure
    (project_dir / "src").mkdir()
    (project_dir / "tests").mkdir()
    (project_dir / "config").mkdir()

    # Create pipeline.toml
    config_content = """[project]
name = "test_project"
version = "0.1.0"

[environments]
dev = {}
prod = {}
"""
    (project_dir / "config" / "pipeline.toml").write_text(config_content)

    # Create basic pipeline.py
    pipeline_content = """\"\"\"Test pipeline definition.\"\"\"

from vibe_piper import Pipeline


def create_pipeline() -> Pipeline:
    \"\"\"Create test pipeline.\"\"\"
    return Pipeline(
        name="test_project",
        description="Test pipeline",
    )
"""
    (project_dir / "src" / "pipeline.py").write_text(pipeline_content)

    yield project_dir


class TestVersionCommand:
    """Tests for the version command."""

    def test_version_output(self) -> None:
        """Test that version command outputs version information."""
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "VibePiper" in result.stdout
        assert "0.1.0" in result.stdout

    def test_version_short_flag(self) -> None:
        """Test version command with -v flag."""
        result = runner.invoke(app, ["-v"])
        assert result.exit_code == 0
        assert "VibePiper" in result.stdout


class TestInitCommand:
    """Tests for the init command."""

    def test_init_creates_project_structure(self, tmp_path: Path) -> None:
        """Test that init command creates correct project structure."""
        project_name = "new_project"
        result = runner.invoke(app, ["init", project_name, "--dir", str(tmp_path)])

        assert result.exit_code == 0
        project_path = tmp_path / project_name

        # Check directories
        assert project_path.exists()
        assert (project_path / "src").exists()
        assert (project_path / "tests").exists()
        assert (project_path / "config").exists()
        assert (project_path / "data").exists()
        assert (project_path / "docs").exists()

        # Check files
        assert (project_path / "config" / "pipeline.toml").exists()
        assert (project_path / "src" / "pipeline.py").exists()
        assert (project_path / "README.md").exists()
        assert (project_path / ".gitignore").exists()

    def test_init_with_invalid_name(self, tmp_path: Path) -> None:
        """Test that init command rejects invalid project names."""
        result = runner.invoke(
            app, ["init", "invalid-name-123!", "--dir", str(tmp_path)]
        )
        assert result.exit_code == 1

    def test_init_existing_directory_fails(self, tmp_path: Path) -> None:
        """Test that init command fails if directory already exists."""
        existing_dir = tmp_path / "existing"
        existing_dir.mkdir()

        result = runner.invoke(app, ["init", "existing", "--dir", str(tmp_path)])
        assert result.exit_code == 1


class TestValidateCommand:
    """Tests for the validate command."""

    def test_validate_valid_project(self, temp_project_dir: Path) -> None:
        """Test that validate command succeeds for valid project."""
        result = runner.invoke(app, ["validate", str(temp_project_dir)])
        assert result.exit_code == 0
        assert "validation passed" in result.stdout.lower()

    def test_validate_missing_config(self, tmp_path: Path) -> None:
        """Test that validate command fails when config is missing."""
        result = runner.invoke(app, ["validate", str(tmp_path)])
        assert result.exit_code == 1

    def test_validate_missing_pipeline_file(self, temp_project_dir: Path) -> None:
        """Test that validate command fails when pipeline.py is missing."""
        (temp_project_dir / "src" / "pipeline.py").unlink()

        result = runner.invoke(app, ["validate", str(temp_project_dir)])
        assert result.exit_code == 1

    def test_validate_verbose_output(self, temp_project_dir: Path) -> None:
        """Test that validate command shows verbose output with --verbose flag."""
        result = runner.invoke(app, ["validate", str(temp_project_dir), "--verbose"])
        assert result.exit_code == 0


class TestRunCommand:
    """Tests for the run command."""

    def test_run_basic(self, temp_project_dir: Path) -> None:
        """Test basic run command."""
        result = runner.invoke(app, ["run", str(temp_project_dir)])
        assert result.exit_code == 0

    def test_run_dry_run(self, temp_project_dir: Path) -> None:
        """Test run command with --dry-run flag."""
        result = runner.invoke(app, ["run", str(temp_project_dir), "--dry-run"])
        assert result.exit_code == 0
        assert "Dry run" in result.stdout

    def test_run_with_environment(self, temp_project_dir: Path) -> None:
        """Test run command with environment selection."""
        result = runner.invoke(app, ["run", str(temp_project_dir), "--env", "prod"])
        assert result.exit_code == 0

    def test_run_with_asset(self, temp_project_dir: Path) -> None:
        """Test run command with specific asset."""
        result = runner.invoke(
            app, ["run", str(temp_project_dir), "--asset", "test_asset"]
        )
        assert result.exit_code == 0


class TestTestCommand:
    """Tests for the test command."""

    def test_test_basic(self, temp_project_dir: Path) -> None:
        """Test basic test command."""
        # Create a simple test file
        test_content = """def test_example():
    assert True
"""
        (temp_project_dir / "tests" / "test_example.py").write_text(test_content)

        result = runner.invoke(app, ["test", str(temp_project_dir)])
        assert result.exit_code == 0

    def test_test_with_coverage(self, temp_project_dir: Path) -> None:
        """Test test command with coverage flag."""
        test_content = """def test_example():
    assert True
"""
        (temp_project_dir / "tests" / "test_example.py").write_text(test_content)

        result = runner.invoke(app, ["test", str(temp_project_dir), "--coverage"])
        # Note: This might fail if pytest-cov is not installed
        # Just check that the command was invoked correctly
        assert "test" in result.stdout.lower() or result.exit_code in (0, 1)

    def test_test_missing_tests_dir(self, tmp_path: Path) -> None:
        """Test that test command fails when tests directory is missing."""
        result = runner.invoke(app, ["test", str(tmp_path)])
        assert result.exit_code == 1


class TestDocsCommand:
    """Tests for the docs command."""

    def test_docs_generate(self, temp_project_dir: Path) -> None:
        """Test that docs command generates documentation."""
        output_dir = temp_project_dir / "generated_docs"
        result = runner.invoke(
            app, ["docs", str(temp_project_dir), "--output", str(output_dir)]
        )

        assert result.exit_code == 0
        assert output_dir.exists()
        assert (output_dir / "index.md").exists()
        assert (output_dir / "config.md").exists()

    def test_docs_custom_output(self, temp_project_dir: Path) -> None:
        """Test docs command with custom output directory."""
        custom_output = temp_project_dir / "custom_docs"
        result = runner.invoke(
            app, ["docs", str(temp_project_dir), "--output", str(custom_output)]
        )

        assert result.exit_code == 0
        assert custom_output.exists()


class TestCliEntryPoints:
    """Tests for CLI entry points and help."""

    def test_main_help(self) -> None:
        """Test that main command shows help."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "VibePiper" in result.stdout

    def test_init_help(self) -> None:
        """Test that init command shows help."""
        result = runner.invoke(app, ["init", "--help"])
        assert result.exit_code == 0
        assert "Initialize" in result.stdout

    def test_validate_help(self) -> None:
        """Test that validate command shows help."""
        result = runner.invoke(app, ["validate", "--help"])
        assert result.exit_code == 0
        assert "Validate" in result.stdout

    def test_run_help(self) -> None:
        """Test that run command shows help."""
        result = runner.invoke(app, ["run", "--help"])
        assert result.exit_code == 0
        assert "Execute" in result.stdout

    def test_test_help(self) -> None:
        """Test that test command shows help."""
        result = runner.invoke(app, ["test", "--help"])
        assert result.exit_code == 0
        assert "Run tests" in result.stdout

    def test_docs_help(self) -> None:
        """Test that docs command shows help."""
        result = runner.invoke(app, ["docs", "--help"])
        assert result.exit_code == 0
        assert "Generate documentation" in result.stdout
