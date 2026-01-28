"""
File system-based IO Manager.

This module provides an IO manager that stores asset data on the local file system.
Supports multiple file formats including JSON, CSV, and pickle.
"""

import json
import pickle
from pathlib import Path
from typing import Any

from vibe_piper.io_managers.base import IOManagerAdapter
from vibe_piper.types import PipelineContext


class FileIOManager(IOManagerAdapter):
    """
    IO manager that stores data on the local file system.

    This IO manager persists data to disk, making it available across
    pipeline runs. It supports multiple file formats and handles
    directory creation automatically.

    Attributes:
        base_path: Base directory for storing asset files
        format: File format to use (json, csv, pickle)

    Example:
        Use the file IO manager::

            @asset(
                io_manager="file",
                uri="/data/assets/{name}.json"
            )
            def my_asset():
                return {"data": "value"}
    """

    def __init__(self, base_path: str | Path = "./data", format: str = "json") -> None:
        """
        Initialize the file IO manager.

        Args:
            base_path: Base directory for storing files
            format: File format (json, csv, pickle)
        """
        self.base_path = Path(base_path)
        self.format = format.lower()

        # Create base directory if it doesn't exist
        self.base_path.mkdir(parents=True, exist_ok=True)

        # Validate format
        valid_formats = {"json", "csv", "pickle", "pkl"}
        if self.format not in valid_formats:
            msg = f"Invalid format {format!r}. Must be one of {valid_formats}"
            raise ValueError(msg)

    def _get_file_path(self, context: PipelineContext) -> Path:
        """
        Get the file path for an asset.

        Args:
            context: The pipeline execution context

        Returns:
            Path object for the asset file
        """
        # Create a path based on pipeline_id and run_id
        filename = f"{context.pipeline_id}_{context.run_id}.{self.format}"
        return self.base_path / filename

    def handle_output(self, context: PipelineContext, data: Any) -> None:
        """
        Store data to a file.

        Args:
            context: The pipeline execution context
            data: The data to store

        Raises:
            IOError: If file writing fails
        """
        file_path = self._get_file_path(context)

        # Create parent directories if needed
        file_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            if self.format == "json":
                with file_path.open("w") as f:
                    json.dump(data, f, indent=2, default=str)
            elif self.format in {"pickle", "pkl"}:
                with file_path.open("wb") as f:
                    pickle.dump(data, f)
            elif self.format == "csv":
                import csv

                # Handle list of dicts
                if isinstance(data, list) and data and isinstance(data[0], dict):
                    with file_path.open("w", newline="") as f:
                        writer = csv.DictWriter(f, fieldnames=data[0].keys())
                        writer.writeheader()
                        writer.writerows(data)
                else:
                    msg = "CSV format requires data to be a list of dicts"
                    raise ValueError(msg)
        except Exception as e:
            msg = f"Failed to write file {file_path}: {e}"
            raise OSError(msg) from e

    def load_input(self, context: PipelineContext) -> Any:
        """
        Load data from a file.

        Args:
            context: The pipeline execution context

        Returns:
            The loaded data

        Raises:
            FileNotFoundError: If the file doesn't exist
            IOError: If file reading fails
        """
        file_path = self._get_file_path(context)

        if not file_path.exists():
            msg = f"File not found: {file_path}"
            raise FileNotFoundError(msg)

        try:
            if self.format == "json":
                with file_path.open("r") as f:
                    return json.load(f)
            elif self.format in {"pickle", "pkl"}:
                with file_path.open("rb") as f:
                    return pickle.load(f)
            elif self.format == "csv":
                import csv

                with file_path.open("r", newline="") as f:
                    reader = csv.DictReader(f)
                    return list(reader)
        except Exception as e:
            msg = f"Failed to read file {file_path}: {e}"
            raise OSError(msg) from e

    def delete_asset(self, context: PipelineContext) -> None:
        """
        Delete an asset file.

        Args:
            context: The pipeline execution context
        """
        file_path = self._get_file_path(context)
        if file_path.exists():
            file_path.unlink()

    def has_asset(self, context: PipelineContext) -> bool:
        """
        Check if an asset file exists.

        Args:
            context: The pipeline execution context

        Returns:
            True if the asset file exists, False otherwise
        """
        file_path = self._get_file_path(context)
        return file_path.exists()
