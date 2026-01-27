"""
Tests for Vibe Piper decorators.

This module tests the @asset decorator and related decorator functionality.
"""

import pytest

from vibe_piper import Asset, AssetType, Schema, SchemaField, DataType
from vibe_piper.decorators import asset


class TestAssetDecorator:
    """Tests for the @asset decorator."""

    def test_asset_decorator_basic(self) -> None:
        """Test basic @asset decorator without parameters."""

        @asset
        def my_asset() -> None:
            """A simple asset."""

        assert isinstance(my_asset, Asset)
        assert my_asset.name == "my_asset"
        assert my_asset.asset_type == AssetType.MEMORY
        assert my_asset.uri == "memory://my_asset"

    def test_asset_decorator_with_name(self) -> None:
        """Test @asset decorator with custom name."""

        @asset(name="custom_asset")
        def my_function() -> None:
            """A function."""

        assert isinstance(my_function, Asset)
        assert my_function.name == "custom_asset"

    def test_asset_decorator_with_asset_type(self) -> None:
        """Test @asset decorator with custom asset type."""

        @asset(asset_type=AssetType.FILE)
        def data_file() -> None:
            """A file asset."""

        assert isinstance(data_file, Asset)
        assert data_file.asset_type == AssetType.FILE

    def test_asset_decorator_with_uri(self) -> None:
        """Test @asset decorator with custom URI."""

        @asset(uri="s3://my-bucket/data.csv")
        def s3_asset() -> None:
            """An S3 asset."""

        assert isinstance(s3_asset, Asset)
        assert s3_asset.uri == "s3://my-bucket/data.csv"

    def test_asset_decorator_with_schema(self) -> None:
        """Test @asset decorator with schema."""

        schema = Schema(
            name="user_schema",
            fields=(
                SchemaField(name="id", data_type=DataType.INTEGER),
                SchemaField(name="name", data_type=DataType.STRING),
            ),
        )

        @asset(schema=schema)
        def users() -> None:
            """Users asset."""

        assert isinstance(users, Asset)
        assert users.schema == schema

    def test_asset_decorator_with_description(self) -> None:
        """Test @asset decorator with description."""

        @asset(description="This is a test asset")
        def my_asset() -> None:
            """A function."""

        assert isinstance(my_asset, Asset)
        assert my_asset.description == "This is a test asset"

    def test_asset_decorator_with_metadata(self) -> None:
        """Test @asset decorator with metadata."""

        @asset(metadata={"owner": "data-team", "pii": True})
        def sensitive_data() -> None:
            """Sensitive data asset."""

        assert isinstance(sensitive_data, Asset)
        assert sensitive_data.metadata == {"owner": "data-team", "pii": True}

    def test_asset_decorator_with_config(self) -> None:
        """Test @asset decorator with config."""

        @asset(config={"format": "parquet", "compression": "snappy"})
        def data_asset() -> None:
            """Data asset."""

        assert isinstance(data_asset, Asset)
        assert data_asset.config == {"format": "parquet", "compression": "snappy"}

    def test_asset_decorator_combined_parameters(self) -> None:
        """Test @asset decorator with multiple parameters."""

        schema = Schema(
            name="user_schema",
            fields=(
                SchemaField(name="id", data_type=DataType.INTEGER),
                SchemaField(name="name", data_type=DataType.STRING),
            ),
        )

        @asset(
            name="users_table",
            asset_type=AssetType.TABLE,
            uri="postgresql://localhost/db/users",
            schema=schema,
            description="Users table",
            metadata={"owner": "analytics"},
        )
        def users() -> None:
            """Users asset."""

        assert isinstance(users, Asset)
        assert users.name == "users_table"
        assert users.asset_type == AssetType.TABLE
        assert users.uri == "postgresql://localhost/db/users"
        assert users.schema == schema
        assert users.description == "Users table"
        assert users.metadata == {"owner": "analytics"}

    def test_asset_decorator_preserves_function_docstring(self) -> None:
        """Test that function docstring is used as default description."""

        @asset
        def documented_asset() -> None:
            """This is a documented asset."""

        assert isinstance(documented_asset, Asset)
        assert documented_asset.description == "This is a documented asset."

    def test_asset_decorator_explicit_description_overrides_docstring(self) -> None:
        """Test that explicit description parameter overrides docstring."""

        @asset(description="Custom description")
        def documented_asset() -> None:
            """This is a docstring."""

        assert isinstance(documented_asset, Asset)
        assert documented_asset.description == "Custom description"

    def test_asset_decorator_uri_auto_generation(self) -> None:
        """Test that URI is auto-generated based on asset type and name."""

        @asset(asset_type=AssetType.TABLE)
        def users() -> None:
            """Users table."""

        assert isinstance(users, Asset)
        assert users.uri == "table://users"

        @asset(asset_type=AssetType.FILE)
        def data_file() -> None:
            """Data file."""

        assert isinstance(data_file, Asset)
        assert data_file.uri == "file://data_file"
