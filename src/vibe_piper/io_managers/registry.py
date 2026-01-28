"""
IO Manager Registry.

This module provides a registry for managing IO manager instances.
It allows dynamic registration and retrieval of IO managers by name.
"""

from typing import Any

from vibe_piper.io_managers.base import IOManagerAdapter
from vibe_piper.io_managers.database import DatabaseIOManager
from vibe_piper.io_managers.file import FileIOManager
from vibe_piper.io_managers.memory import MemoryIOManager
from vibe_piper.io_managers.s3 import S3IOManager


class IOManagerRegistry:
    """
    Registry for IO manager instances.

    This class manages IO manager instances and allows retrieval by name.
    It provides a singleton-like interface for accessing IO managers.

    Attributes:
        _managers: Dictionary of registered IO managers

    Example:
        Get an IO manager by name::

            registry = IOManagerRegistry()
            manager = registry.get("memory")
    """

    def __init__(self) -> None:
        """Initialize the registry with default IO managers."""
        self._managers: dict[str, IOManagerAdapter] = {}
        self._configurations: dict[str, dict[str, Any]] = {}

        # Register default IO managers
        self._register_default_managers()

    def _register_default_managers(self) -> None:
        """Register default IO managers."""
        self._managers["memory"] = MemoryIOManager()

    def register(
        self,
        name: str,
        manager: IOManagerAdapter,
        config: dict[str, Any] | None = None,
    ) -> None:
        """
        Register an IO manager.

        Args:
            name: Name to register the IO manager under
            manager: The IO manager instance
            config: Optional configuration for the IO manager

        Raises:
            ValueError: If name is already registered
        """
        if name in self._managers:
            msg = f"IO manager {name!r} is already registered"
            raise ValueError(msg)

        self._managers[name] = manager
        if config:
            self._configurations[name] = config

    def get(self, name: str) -> IOManagerAdapter:
        """
        Get an IO manager by name.

        Args:
            name: Name of the IO manager

        Returns:
            The IO manager instance

        Raises:
            KeyError: If IO manager is not found
        """
        if name not in self._managers:
            # Try to create a default instance for common types
            if name == "file":
                manager = FileIOManager()
                self._managers[name] = manager
                return manager
            elif name == "s3":
                msg = (
                    "S3 IO manager requires configuration. "
                    "Use register() to provide bucket and other settings."
                )
                raise KeyError(msg)
            elif name == "database":
                msg = (
                    "Database IO manager requires configuration. "
                    "Use register() to provide connection string."
                )
                raise KeyError(msg)
            else:
                msg = f"IO manager {name!r} not found in registry"
                raise KeyError(msg)

        return self._managers[name]

    def get_or_create(
        self,
        name: str,
        config: dict[str, Any] | None = None,
    ) -> IOManagerAdapter:
        """
        Get an IO manager by name, creating it if necessary.

        Args:
            name: Name of the IO manager
            config: Optional configuration for creating the IO manager

        Returns:
            The IO manager instance

        Raises:
            ValueError: If IO manager type is unknown
        """
        # Return existing if already registered
        if name in self._managers:
            return self._managers[name]

        # Create new instance based on type
        manager: IOManagerAdapter
        if name == "memory":
            manager = MemoryIOManager()
        elif name == "file":
            file_config = config or {}
            base_path = file_config.get("base_path", "./data")
            format_type = file_config.get("format", "json")
            manager = FileIOManager(base_path=base_path, format=format_type)
        elif name == "s3":
            s3_config = config or {}
            bucket = s3_config.get("bucket", "")
            if not bucket:
                msg = "S3 IO manager requires 'bucket' in configuration"
                raise ValueError(msg)
            prefix = s3_config.get("prefix", "assets")
            format_type = s3_config.get("format", "json")
            region_name = s3_config.get("region_name")
            manager = S3IOManager(
                bucket=bucket,
                prefix=prefix,
                format=format_type,
                region_name=region_name,
            )
        elif name == "database":
            db_config = config or {}
            connection_string = db_config.get("connection_string", "")
            if not connection_string:
                msg = (
                    "Database IO manager requires 'connection_string' in configuration"
                )
                raise ValueError(msg)
            table_name = db_config.get("table_name", "asset_data")
            schema = db_config.get("schema")
            manager = DatabaseIOManager(
                connection_string=connection_string,
                table_name=table_name,
                schema=schema,
            )
        else:
            msg = f"Unknown IO manager type: {name!r}"
            raise ValueError(msg)

        # Register and return
        self._managers[name] = manager
        if config:
            self._configurations[name] = config

        return manager

    def has(self, name: str) -> bool:
        """
        Check if an IO manager is registered.

        Args:
            name: Name of the IO manager

        Returns:
            True if registered, False otherwise
        """
        return name in self._managers

    def list_registered(self) -> tuple[str, ...]:
        """
        List all registered IO manager names.

        Returns:
            Tuple of IO manager names
        """
        return tuple(self._managers.keys())


# Global registry instance
_global_registry: IOManagerRegistry | None = None


def get_global_registry() -> IOManagerRegistry:
    """
    Get the global IO manager registry.

    Returns:
        The global registry instance
    """
    global _global_registry
    if _global_registry is None:
        _global_registry = IOManagerRegistry()
    return _global_registry


def register_io_manager(
    name: str,
    manager: IOManagerAdapter,
    config: dict[str, Any] | None = None,
) -> None:
    """
    Register an IO manager in the global registry.

    Args:
        name: Name to register the IO manager under
        manager: The IO manager instance
        config: Optional configuration for the IO manager
    """
    registry = get_global_registry()
    registry.register(name, manager, config)


def get_io_manager(name: str) -> IOManagerAdapter:
    """
    Get an IO manager from the global registry.

    Args:
        name: Name of the IO manager

    Returns:
        The IO manager instance
    """
    registry = get_global_registry()
    return registry.get(name)
