"""Tests for enhanced runtime configuration overrides."""

import pytest

from vibe_piper.config.loader import load_config
from vibe_piper.config.schema import (
    Config,
    EnvironmentConfig,
    ProjectConfig,
)


class TestRuntimeOverrides:
    """Tests for runtime configuration overrides."""

    def test_override_io_manager(self) -> None:
        """Test overriding io_manager at runtime."""
        import tempfile
        from pathlib import Path

        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(
                """
[project]
name = "test"
version = "1.0"

[environments.dev]
io_manager = "memory"
"""
            )
            f.flush()
            path = Path(f.name)

        try:
            config = load_config(path, cli_overrides={"io_manager": "s3"})
            dev_env = config.get_environment("dev", apply_overrides=True)
            assert dev_env.io_manager == "s3"
        finally:
            path.unlink()

    def test_override_log_level(self) -> None:
        """Test overriding log_level at runtime."""
        import tempfile
        from pathlib import Path

        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(
                """
[project]
name = "test"
version = "1.0"

[environments.dev]
log_level = "info"
"""
            )
            f.flush()
            path = Path(f.name)

        try:
            config = load_config(path, cli_overrides={"log_level": "debug"})
            dev_env = config.get_environment("dev", apply_overrides=True)
            assert dev_env.log_level == "debug"
        finally:
            path.unlink()

    def test_override_parallelism(self) -> None:
        """Test overriding parallelism at runtime."""
        import tempfile
        from pathlib import Path

        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(
                """
[project]
name = "test"
version = "1.0"

[environments.dev]
parallelism = 2
"""
            )
            f.flush()
            path = Path(f.name)

        try:
            config = load_config(path, cli_overrides={"parallelism": 8})
            dev_env = config.get_environment("dev", apply_overrides=True)
            assert dev_env.parallelism == 8
        finally:
            path.unlink()

    def test_override_bucket(self) -> None:
        """Test overriding bucket at runtime."""
        import tempfile
        from pathlib import Path

        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(
                """
[project]
name = "test"
version = "1.0"

[environments.prod]
io_manager = "s3"
bucket = "original-bucket"
"""
            )
            f.flush()
            path = Path(f.name)

        try:
            config = load_config(path, cli_overrides={"bucket": "override-bucket"})
            prod_env = config.get_environment("prod", apply_overrides=True)
            assert prod_env.bucket == "override-bucket"
        finally:
            path.unlink()

    def test_override_region(self) -> None:
        """Test overriding region at runtime."""
        import tempfile
        from pathlib import Path

        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(
                """
[project]
name = "test"
version = "1.0"

[environments.prod]
io_manager = "s3"
bucket = "prod-bucket"
region = "us-west-2"
"""
            )
            f.flush()
            path = Path(f.name)

        try:
            config = load_config(path, cli_overrides={"region": "eu-west-1"})
            prod_env = config.get_environment("prod", apply_overrides=True)
            assert prod_env.region == "eu-west-1"
        finally:
            path.unlink()

    def test_override_additional_config(self) -> None:
        """Test overriding additional configuration fields at runtime."""
        import tempfile
        from pathlib import Path

        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(
                """
[project]
name = "test"
version = "1.0"

[environments.dev]
custom_field = "original"
"""
            )
            f.flush()
            path = Path(f.name)

        try:
            config = load_config(path, cli_overrides={"custom_field": "override"})
            dev_env = config.get_environment("dev", apply_overrides=True)
            assert dev_env.additional_config["custom_field"] == "override"
        finally:
            path.unlink()

    def test_multiple_overrides(self) -> None:
        """Test applying multiple overrides at once."""
        import tempfile
        from pathlib import Path

        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(
                """
[project]
name = "test"
version = "1.0"

[environments.prod]
io_manager = "s3"
bucket = "original"
log_level = "info"
"""
            )
            f.flush()
            path = Path(f.name)

        try:
            config = load_config(
                path,
                cli_overrides={
                    "io_manager": "gcs",
                    "bucket": "override-bucket",
                    "log_level": "debug",
                },
            )
            prod_env = config.get_environment("prod", apply_overrides=True)
            assert prod_env.io_manager == "gcs"
            assert prod_env.bucket == "override-bucket"
            assert prod_env.log_level == "debug"
        finally:
            path.unlink()

    def test_no_overrides_applied_when_false(self) -> None:
        """Test that overrides are not applied when apply_overrides=False."""
        import tempfile
        from pathlib import Path

        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(
                """
[project]
name = "test"
version = "1.0"

[environments.dev]
io_manager = "memory"
log_level = "info"
"""
            )
            f.flush()
            path = Path(f.name)

        try:
            config = load_config(
                path,
                cli_overrides={"io_manager": "s3", "log_level": "debug"},
            )
            dev_env_no_override = config.get_environment("dev", apply_overrides=False)
            dev_env_with_override = config.get_environment("dev", apply_overrides=True)

            # Without overrides
            assert dev_env_no_override.io_manager == "memory"
            assert dev_env_no_override.log_level == "info"

            # With overrides
            assert dev_env_with_override.io_manager == "s3"
            assert dev_env_with_override.log_level == "debug"
        finally:
            path.unlink()

    def test_overrides_with_inheritance(self) -> None:
        """Test that runtime overrides work with config inheritance."""
        import tempfile
        from pathlib import Path

        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(
                """
[project]
name = "test"
version = "1.0"

[environments.base]
io_manager = "s3"
bucket = "base-bucket"

[environments.prod]
inherits = "base"
log_level = "warning"
"""
            )
            f.flush()
            path = Path(f.name)

        try:
            config = load_config(path, cli_overrides={"log_level": "error"})
            prod_env = config.get_environment("prod", apply_overrides=True)

            # Inherits io_manager and bucket from base
            assert prod_env.io_manager == "s3"
            assert prod_env.bucket == "base-bucket"
            # Runtime override overrides inherited value
            assert prod_env.log_level == "error"
        finally:
            path.unlink()
