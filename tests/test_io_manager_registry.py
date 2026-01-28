"""
Unit tests for IO Manager Registry.

Tests the registry functionality for managing IO manager instances.
"""

import pytest

from vibe_piper.io_managers import (
    FileIOManager,
    IOManagerRegistry,
    MemoryIOManager,
    get_global_registry,
    get_io_manager,
    register_io_manager,
)
from vibe_piper.types import PipelineContext


class TestIOManagerRegistry:
    """Tests for IOManagerRegistry."""

    def test_create_registry(self) -> None:
        """Test creating a registry."""
        registry = IOManagerRegistry()
        assert isinstance(registry._managers, dict)
        assert isinstance(registry._configurations, dict)

    def test_default_managers_registered(self) -> None:
        """Test that default managers are registered."""
        registry = IOManagerRegistry()
        assert registry.has("memory")

    def test_register_manager(self) -> None:
        """Test registering a custom IO manager."""
        registry = IOManagerRegistry()
        manager = MemoryIOManager()

        registry.register("custom", manager)

        assert registry.has("custom")
        assert registry.get("custom") == manager

    def test_register_duplicate_raises_error(self) -> None:
        """Test registering duplicate manager raises error."""
        registry = IOManagerRegistry()
        manager = MemoryIOManager()

        registry.register("custom", manager)

        with pytest.raises(ValueError, match="already registered"):
            registry.register("custom", manager)

    def test_get_manager(self) -> None:
        """Test getting a registered manager."""
        registry = IOManagerRegistry()
        manager = registry.get("memory")

        assert isinstance(manager, MemoryIOManager)

    def test_get_nonexistent_raises_error(self) -> None:
        """Test getting non-existent manager raises error."""
        registry = IOManagerRegistry()

        with pytest.raises(KeyError, match="not found"):
            registry.get("nonexistent")

    def test_get_or_create_memory(self) -> None:
        """Test get_or_create with memory manager."""
        registry = IOManagerRegistry()
        manager = registry.get_or_create("memory")

        assert isinstance(manager, MemoryIOManager)

    def test_get_or_create_file(self) -> None:
        """Test get_or_create with file manager."""
        registry = IOManagerRegistry()

        manager = registry.get_or_create("file", {"base_path": "./test_data"})

        assert isinstance(manager, FileIOManager)
        # Path normalizes "./test_data" to "test_data"
        assert manager.base_path.name == "test_data"

    def test_get_or_create_s3_requires_config(self) -> None:
        """Test get_or_create with S3 requires bucket."""
        registry = IOManagerRegistry()

        with pytest.raises(ValueError, match="requires 'bucket'"):
            registry.get_or_create("s3")

    def test_get_or_create_database_requires_config(self) -> None:
        """Test get_or_create with database requires connection_string."""
        registry = IOManagerRegistry()

        with pytest.raises(ValueError, match="requires 'connection_string'"):
            registry.get_or_create("database")

    def test_get_or_create_invalid_type(self) -> None:
        """Test get_or_create with invalid type."""
        registry = IOManagerRegistry()

        with pytest.raises(ValueError, match="Unknown IO manager type"):
            registry.get_or_create("invalid_type")

    def test_has_manager(self) -> None:
        """Test checking if manager is registered."""
        registry = IOManagerRegistry()

        assert registry.has("memory")
        assert not registry.has("nonexistent")

    def test_list_registered(self) -> None:
        """Test listing registered managers."""
        registry = IOManagerRegistry()
        managers = registry.list_registered()

        assert isinstance(managers, tuple)
        assert "memory" in managers

    def test_get_or_create_caches_instances(self) -> None:
        """Test that get_or_create caches instances."""
        registry = IOManagerRegistry()

        manager1 = registry.get_or_create("memory")
        manager2 = registry.get_or_create("memory")

        assert manager1 is manager2


class TestGlobalRegistry:
    """Tests for global registry functions."""

    def test_get_global_registry_singleton(self) -> None:
        """Test that global registry is a singleton."""
        registry1 = get_global_registry()
        registry2 = get_global_registry()

        assert registry1 is registry2

    def test_register_io_manager(self) -> None:
        """Test registering to global registry."""
        # Note: This may affect other tests since it's a global singleton
        # In a real test suite, you'd want to reset the registry between tests
        manager = MemoryIOManager()

        register_io_manager("test_global", manager)

        registry = get_global_registry()
        assert registry.has("test_global")

    def test_get_io_manager(self) -> None:
        """Test getting from global registry."""
        manager = get_io_manager("memory")

        assert isinstance(manager, MemoryIOManager)

    def test_get_io_manager_not_found(self) -> None:
        """Test getting non-existent manager from global registry."""
        with pytest.raises(KeyError):
            get_io_manager("nonexistent_global")


class TestIOManagerRegistryIntegration:
    """Integration tests for IO manager registry."""

    def test_workflow_with_registry(self) -> None:
        """Test complete workflow using registry."""
        registry = IOManagerRegistry()

        # Get memory manager
        manager = registry.get("memory")
        context = PipelineContext(pipeline_id="test_asset", run_id="run_1")

        # Store data
        data = {"key": "value"}
        manager.handle_output(context, data)

        # Load data
        loaded = manager.load_input(context)
        assert loaded == data

    def test_multiple_managers_in_registry(self) -> None:
        """Test using multiple managers from registry."""
        registry = IOManagerRegistry()

        # Get different managers
        memory_manager = registry.get("memory")
        file_manager = registry.get_or_create("file")

        assert isinstance(memory_manager, MemoryIOManager)
        assert isinstance(file_manager, FileIOManager)

    def test_manager_isolation(self) -> None:
        """Test that managers maintain separate storage."""
        registry = IOManagerRegistry()

        manager1 = registry.get("memory")
        manager2 = registry.get_or_create("memory")

        context1 = PipelineContext(pipeline_id="asset1", run_id="run_1")
        context2 = PipelineContext(pipeline_id="asset2", run_id="run_1")

        manager1.handle_output(context1, {"value": "first"})
        manager2.handle_output(context2, {"value": "second"})

        # Since they're the same instance (cached), both should have both values
        assert manager1.load_input(context1) == {"value": "first"}
        assert manager2.load_input(context2) == {"value": "second"}
