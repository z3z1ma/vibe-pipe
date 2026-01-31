"""Tests for pipeline configuration parser."""

import os
from pathlib import Path

import pytest

from vibe_piper.pipeline_config.parser import (
    AuthConfig,
    CheckType,
    ExpectationCheck,
    ExpectationConfig,
    JobConfig,
    MaterializationType,
    PaginationConfig,
    PipelineConfig,
    PipelineConfigError,
    PipelineMetadata,
    RateLimitConfig,
    SinkConfig,
    SinkType,
    SourceConfig,
    SourceType,
    TransformConfig,
    TransformStep,
    TransformType,
    interpolate_env_vars,
    load_pipeline_config,
)


class TestEnvironmentVariableInterpolation:
    """Tests for environment variable interpolation."""

    def test_interpolate_single_env_var(self) -> None:
        """Test interpolation of single environment variable."""
        os.environ["TEST_VAR"] = "test_value"

        result = interpolate_env_vars({"key": "${TEST_VAR}"})
        assert result == {"key": "test_value"}

    def test_interpolate_with_default(self) -> None:
        """Test interpolation with default value."""
        # Env var not set
        if "TEST_VAR" in os.environ:
            del os.environ["TEST_VAR"]

        result = interpolate_env_vars({"key": "${TEST_VAR:-default}"})
        assert result == {"key": "default"}

    def test_interpolate_nested_dict(self) -> None:
        """Test interpolation in nested dictionary."""
        os.environ["HOST"] = "localhost"
        os.environ["PORT"] = "5432"

        result = interpolate_env_vars(
            {
                "database": {
                    "host": "${HOST}",
                    "port": "${PORT}",
                }
            }
        )
        assert result == {
            "database": {
                "host": "localhost",
                "port": "5432",
            }
        }

    def test_interpolate_in_list(self) -> None:
        """Test interpolation in list."""
        os.environ["ENV"] = "prod"

        result = interpolate_env_vars({"environments": ["dev", "${ENV}", "staging"]})
        assert result == {"environments": ["dev", "prod", "staging"]}

    def test_no_interpolation_needed(self) -> None:
        """Test that non-interpolated values pass through unchanged."""
        result = interpolate_env_vars({"key": "static_value"})
        assert result == {"key": "static_value"}


class TestPipelineConfigParsing:
    """Tests for parsing pipeline configuration."""

    def test_parse_simple_toml(self, tmp_path: Path) -> None:
        """Test parsing a simple TOML pipeline config."""
        config_file = tmp_path / "simple.toml"
        config_file.write_text(
            """
[pipeline]
name = "test_pipeline"
version = "1.0.0"

[[sources]]
name = "users_api"
type = "api"
endpoint = "/users"

[[sinks]]
name = "users_db"
type = "database"
connection = "postgres://localhost/test"
table = "users"
"""
        )

        config = load_pipeline_config(config_file)

        assert config.pipeline.name == "test_pipeline"
        assert config.pipeline.version == "1.0.0"
        assert "users_api" in config.sources
        assert "users_db" in config.sinks

    def test_parse_transform_config(self, tmp_path: Path) -> None:
        """Test parsing transform configuration."""
        config_file = tmp_path / "transform.toml"
        config_file.write_text(
            """
[pipeline]
name = "test_pipeline"
version = "1.0.0"

[[sources]]
name = "raw_data"
type = "api"
endpoint = "/data"

[[transforms]]
name = "clean_data"
source = "raw_data"
steps = [
    { type = "filter", condition = "email is not null" },
    { type = "filter", condition = "age >= 18" },
]
"""
        )

        config = load_pipeline_config(config_file)

        assert "clean_data" in config.transforms
        transform = config.transforms["clean_data"]
        assert transform.source == "raw_data"
        assert len(transform.steps) == 2
        assert transform.steps[0].type == TransformType.FILTER

    def test_parse_expectation_config(self, tmp_path: Path) -> None:
        """Test parsing expectation configuration."""
        config_file = tmp_path / "expectations.toml"
        config_file.write_text(
            """
[pipeline]
name = "test_pipeline"
version = "1.0.0"

[[sources]]
name = "users"
type = "file"
path = "users.csv"

[[expectations]]
name = "email_validity"
asset = "users"
checks = [
    { type = "not_null", column = "email" },
    { type = "regex", column = "email", pattern = "^[^@]+@[^@]+$" },
]
"""
        )

        config = load_pipeline_config(config_file)

        assert "email_validity" in config.expectations
        expectation = config.expectations["email_validity"]
        assert expectation.asset == "users"
        assert len(expectation.checks) == 2

    def test_parse_job_config(self, tmp_path: Path) -> None:
        """Test parsing job configuration."""
        config_file = tmp_path / "jobs.toml"
        config_file.write_text(
            """
[pipeline]
name = "test_pipeline"
version = "1.0.0"

[[sources]]
name = "users_api"
type = "api"
endpoint = "/users"

[[jobs]]
name = "daily_sync"
schedule = "0 0 * * *"
sources = ["users_api"]
"""
        )

        config = load_pipeline_config(config_file)

        assert "daily_sync" in config.jobs
        job = config.jobs["daily_sync"]
        assert job.schedule == "0 0 * * *"
        assert "users_api" in job.sources

    def test_parse_auth_config(self, tmp_path: Path) -> None:
        """Test parsing authentication configuration."""
        config_file = tmp_path / "auth.toml"
        config_file.write_text(
            """
[pipeline]
name = "test_pipeline"
version = "1.0.0"

[[sources]]
name = "api"
type = "api"
endpoint = "/data"

[sources.api.auth]
type = "bearer"
from_env = "API_KEY"
"""
        )

        config = load_pipeline_config(config_file)

        source = config.sources["api"]
        assert source.auth is not None
        assert source.auth.type == "bearer"
        assert source.auth.from_env == "API_KEY"

    def test_parse_pagination_config(self, tmp_path: Path) -> None:
        """Test parsing pagination configuration."""
        config_file = tmp_path / "pagination.toml"
        config_file.write_text(
            """
[pipeline]
name = "test_pipeline"
version = "1.0.0"

[[sources]]
name = "api"
type = "api"
endpoint = "/data"

[sources.api.pagination]
type = "offset"
items_path = "data.items"
limit_param = "limit"
offset_param = "offset"
"""
        )

        config = load_pipeline_config(config_file)

        source = config.sources["api"]
        assert source.pagination is not None
        assert source.pagination.type == "offset"
        assert source.pagination.items_path == "data.items"

    def test_parse_yaml_config(self, tmp_path: Path) -> None:
        """Test parsing YAML configuration."""
        pytest.importorskip("yaml", reason="PyYAML not installed")

        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            """
pipeline:
  name: test_pipeline
  version: "1.0.0"

sources:
  - name: users_api
    type: api
    endpoint: /users
"""
        )

        config = load_pipeline_config(config_file)

        assert config.pipeline.name == "test_pipeline"
        assert "users_api" in config.sources

    def test_missing_pipeline_name_raises_error(self, tmp_path: Path) -> None:
        """Test that missing pipeline name raises error."""
        config_file = tmp_path / "invalid.toml"
        config_file.write_text(
            """
[pipeline]
version = "1.0.0"
"""
        )

        with pytest.raises(PipelineConfigError, match="Pipeline name is required"):
            load_pipeline_config(config_file)

    def test_missing_source_type_raises_error(self, tmp_path: Path) -> None:
        """Test that missing source type raises error."""
        config_file = tmp_path / "invalid.toml"
        config_file.write_text(
            """
[pipeline]
name = "test"
version = "1.0.0"

[[sources]]
name = "api_source"
endpoint = "/data"
"""
        )

        with pytest.raises(
            PipelineConfigError, match="Source 'api_source' must have a 'type' field"
        ):
            load_pipeline_config(config_file)

    def test_duplicate_source_names_raise_error(self, tmp_path: Path) -> None:
        """Test that duplicate source names raise error."""
        config_file = tmp_path / "invalid.toml"
        config_file.write_text(
            """
[pipeline]
name = "test"
version = "1.0.0"

[[sources]]
name = "users"
type = "api"
endpoint = "/users"

[[sources]]
name = "users"
type = "database"
connection = "postgres://localhost/test"
table = "users"
"""
        )

        with pytest.raises(PipelineConfigError, match="Duplicate source name: users"):
            load_pipeline_config(config_file)


class TestPipelineConfigMethods:
    """Tests for PipelineConfig helper methods."""

    def test_get_source(self, sample_config: PipelineConfig) -> None:
        """Test getting a source by name."""
        source = sample_config.get_source("users_api")
        assert source.name == "users_api"

    def test_get_source_raises_for_unknown(self, sample_config: PipelineConfig) -> None:
        """Test that getting unknown source raises KeyError."""
        with pytest.raises(KeyError, match="Source 'unknown' not found"):
            sample_config.get_source("unknown")

    def test_has_source(self, sample_config: PipelineConfig) -> None:
        """Test checking if source exists."""
        assert sample_config.has_source("users_api") is True
        assert sample_config.has_source("unknown") is False

    def test_get_transform(self, sample_config: PipelineConfig) -> None:
        """Test getting a transform by name."""
        transform = sample_config.get_transform("clean_data")
        assert transform.name == "clean_data"

    def test_get_sink(self, sample_config: PipelineConfig) -> None:
        """Test getting a sink by name."""
        sink = sample_config.get_sink("users_db")
        assert sink.name == "users_db"

    def test_get_job(self, sample_config: PipelineConfig) -> None:
        """Test getting a job by name."""
        job = sample_config.get_job("daily_sync")
        assert job.name == "daily_sync"


@pytest.fixture
def sample_config() -> PipelineConfig:
    """Fixture providing a sample pipeline configuration."""
    return PipelineConfig(
        pipeline=PipelineMetadata(name="test", version="1.0.0"),
        sources={
            "users_api": SourceConfig(
                name="users_api",
                type=SourceType.API,
                endpoint="/users",
            )
        },
        sinks={
            "users_db": SinkConfig(
                name="users_db",
                type=SinkType.DATABASE,
                connection="postgres://localhost/test",
                table="users",
            )
        },
        transforms={
            "clean_data": TransformConfig(
                name="clean_data",
                source="users_api",
                steps=[
                    TransformStep(
                        type=TransformType.FILTER,
                        condition="email is not null",
                    )
                ],
            )
        },
        expectations={
            "email_valid": ExpectationConfig(
                name="email_valid",
                asset="clean_data",
                checks=[
                    ExpectationCheck(
                        type=CheckType.NOT_NULL,
                        column="email",
                    )
                ],
            )
        },
        jobs={
            "daily_sync": JobConfig(
                name="daily_sync",
                schedule="0 0 * * *",
            )
        },
    )


@pytest.fixture
def tmp_path(tmp_path: Path) -> Path:
    """Fixture providing a temporary path for config files."""
    return tmp_path
