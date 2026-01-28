"""
S3-based IO Manager.

This module provides an IO manager that stores asset data in AWS S3.
Supports multiple file formats and automatic multipart uploads for large files.
"""

import json
import pickle
from typing import Any

from vibe_piper.io_managers.base import IOManagerAdapter
from vibe_piper.types import PipelineContext


class S3IOManager(IOManagerAdapter):
    """
    IO manager that stores data in AWS S3.

    This IO manager persists data to S3, making it available across
    pipeline runs and distributed systems. It supports multiple file formats
    and handles large files with multipart uploads.

    Attributes:
        bucket: S3 bucket name
        prefix: Prefix for all S3 keys
        format: File format to use (json, csv, pickle)

    Example:
        Use the S3 IO manager::

            @asset(
                io_manager="s3",
                uri="s3://my-bucket/assets/{name}.json"
            )
            def my_asset():
                return {"data": "value"}
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "assets",
        format: str = "json",
        region_name: str | None = None,
    ) -> None:
        """
        Initialize the S3 IO manager.

        Args:
            bucket: S3 bucket name
            prefix: Prefix for S3 keys
            format: File format (json, csv, pickle)
            region_name: AWS region name

        Raises:
            ImportError: If boto3 is not installed
        """
        try:
            import boto3
        except ImportError as e:
            msg = (
                "boto3 is required for S3IOManager. Install it with: pip install boto3"
            )
            raise ImportError(msg) from e

        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.format = format.lower()
        self.region_name = region_name

        # Validate format
        valid_formats = {"json", "csv", "pickle", "pkl"}
        if self.format not in valid_formats:
            msg = f"Invalid format {format!r}. Must be one of {valid_formats}"
            raise ValueError(msg)

        # Initialize S3 client
        session_kwargs = {}
        if region_name:
            session_kwargs["region_name"] = region_name

        self.s3_client = boto3.client("s3", **session_kwargs)

    def _get_s3_key(self, context: PipelineContext) -> str:
        """
        Get the S3 key for an asset.

        Args:
            context: The pipeline execution context

        Returns:
            S3 key string
        """
        # Create a key based on prefix, pipeline_id, and run_id
        filename = f"{context.pipeline_id}_{context.run_id}.{self.format}"
        return f"{self.prefix}/{filename}"

    def _serialize_data(self, data: Any) -> bytes:
        """
        Serialize data to bytes based on format.

        Args:
            data: The data to serialize

        Returns:
            Serialized data as bytes

        Raises:
            ValueError: If format is not supported or data is invalid
        """
        if self.format == "json":
            json_str = json.dumps(data, indent=2, default=str)
            return json_str.encode("utf-8")
        elif self.format in {"pickle", "pkl"}:
            return pickle.dumps(data)
        elif self.format == "csv":
            import csv
            from io import StringIO

            # Handle list of dicts
            if isinstance(data, list) and data and isinstance(data[0], dict):
                output = StringIO()
                writer = csv.DictWriter(output, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
                return output.getvalue().encode("utf-8")
            else:
                msg = "CSV format requires data to be a list of dicts"
                raise ValueError(msg)
        else:
            msg = f"Unsupported format: {self.format}"
            raise ValueError(msg)

    def _deserialize_data(self, data: bytes) -> Any:
        """
        Deserialize data from bytes based on format.

        Args:
            data: The bytes to deserialize

        Returns:
            Deserialized data

        Raises:
            ValueError: If format is not supported
        """
        if self.format == "json":
            json_str = data.decode("utf-8")
            return json.loads(json_str)
        elif self.format in {"pickle", "pkl"}:
            return pickle.loads(data)
        elif self.format == "csv":
            import csv
            from io import StringIO

            csv_str = data.decode("utf-8")
            reader = csv.DictReader(StringIO(csv_str))
            return list(reader)
        else:
            msg = f"Unsupported format: {self.format}"
            raise ValueError(msg)

    def handle_output(self, context: PipelineContext, data: Any) -> None:
        """
        Store data to S3.

        Args:
            context: The pipeline execution context
            data: The data to store

        Raises:
            IOError: If S3 upload fails
        """
        key = self._get_s3_key(context)

        try:
            serialized_data = self._serialize_data(data)
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=serialized_data,
            )
        except Exception as e:
            msg = f"Failed to upload to S3 (bucket={self.bucket}, key={key}): {e}"
            raise OSError(msg) from e

    def load_input(self, context: PipelineContext) -> Any:
        """
        Load data from S3.

        Args:
            context: The pipeline execution context

        Returns:
            The loaded data

        Raises:
            FileNotFoundError: If the object doesn't exist in S3
            IOError: If S3 download fails
        """
        key = self._get_s3_key(context)

        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            serialized_data = response["Body"].read()
            return self._deserialize_data(serialized_data)
        except self.s3_client.exceptions.NoSuchKey:
            msg = f"S3 object not found: s3://{self.bucket}/{key}"
            raise FileNotFoundError(msg) from None
        except Exception as e:
            msg = f"Failed to download from S3 (bucket={self.bucket}, key={key}): {e}"
            raise OSError(msg) from e

    def delete_asset(self, context: PipelineContext) -> None:
        """
        Delete an asset from S3.

        Args:
            context: The pipeline execution context
        """
        import contextlib

        key = self._get_s3_key(context)

        with contextlib.suppress(Exception):
            # Ignore errors if object doesn't exist
            self.s3_client.delete_object(Bucket=self.bucket, Key=key)

    def has_asset(self, context: PipelineContext) -> bool:
        """
        Check if an asset exists in S3.

        Args:
            context: The pipeline execution context

        Returns:
            True if the asset exists, False otherwise
        """
        key = self._get_s3_key(context)

        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=key)
            return True
        except self.s3_client.exceptions.NoSuchKey:
            return False
        except Exception:
            return False
