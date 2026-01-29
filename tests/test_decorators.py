"""
Tests for Vibe Piper decorators.

This module tests the @asset and @expect decorators and related decorator functionality.
"""

from typing import Any

import pytest

from vibe_piper import (
    Asset,
    AssetType,
    DataType,
    Expectation,
    Schema,
    SchemaField,
    ValidationResult,
)
from vibe_piper.decorators import asset, expect


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


class TestExpectationDecorator:
    """Tests for the @expect decorator."""

    def test_expect_decorator_basic(self) -> None:
        """Test basic @expect decorator without parameters."""

        @expect
        def positive_numbers(data: list[int]) -> bool:
            """All numbers should be positive."""
            return all(x > 0 for x in data)

        assert isinstance(positive_numbers, Expectation)
        assert positive_numbers.name == "positive_numbers"
        assert positive_numbers.severity == "error"

    def test_expect_decorator_with_validation_result(self) -> None:
        """Test @expect decorator returning ValidationResult."""

        @expect
        def non_negative_numbers(data: list[int]) -> ValidationResult:
            """All numbers should be non-negative."""
            invalid = [x for x in data if x < 0]
            if invalid:
                return ValidationResult(
                    is_valid=False,
                    errors=(f"Found negative numbers: {invalid}",),
                )
            return ValidationResult(is_valid=True)

        assert isinstance(non_negative_numbers, Expectation)
        result = non_negative_numbers.validate([1, 2, 3])
        assert result.is_valid is True

        result = non_negative_numbers.validate([1, -2, 3])
        assert result.is_valid is False
        assert "negative numbers" in result.errors[0]

    def test_expect_decorator_with_name(self) -> None:
        """Test @expect decorator with custom name."""

        @expect(name="custom_expectation")
        def my_function(data: Any) -> bool:
            """A function."""
            return True

        assert isinstance(my_function, Expectation)
        assert my_function.name == "custom_expectation"

    def test_expect_decorator_with_severity_error(self) -> None:
        """Test @expect decorator with error severity."""

        @expect(severity="error")
        def critical_check(data: Any) -> bool:
            """Critical check."""
            return True

        assert isinstance(critical_check, Expectation)
        assert critical_check.severity == "error"

    def test_expect_decorator_with_severity_warning(self) -> None:
        """Test @expect decorator with warning severity."""

        @expect(severity="warning")
        def warning_check(data: Any) -> bool:
            """Warning check."""
            return True

        assert isinstance(warning_check, Expectation)
        assert warning_check.severity == "warning"

    def test_expect_decorator_with_severity_info(self) -> None:
        """Test @expect decorator with info severity."""

        @expect(severity="info")
        def info_check(data: Any) -> bool:
            """Info check."""
            return True

        assert isinstance(info_check, Expectation)
        assert info_check.severity == "info"

    def test_expect_decorator_with_invalid_severity(self) -> None:
        """Test @expect decorator with invalid severity raises error."""

        with pytest.raises(ValueError, match="Invalid severity"):

            @expect(severity="invalid")
            def my_func(data: Any) -> bool:
                return True

    def test_expect_decorator_with_description(self) -> None:
        """Test @expect decorator with description."""

        @expect(description="This is a custom description")
        def my_expectation(data: Any) -> bool:
            """A function."""
            return True

        assert isinstance(my_expectation, Expectation)
        assert my_expectation.description == "This is a custom description"

    def test_expect_decorator_with_metadata(self) -> None:
        """Test @expect decorator with metadata."""

        @expect(metadata={"category": "data-quality", "owner": "data-team"})
        def my_expectation(data: Any) -> bool:
            """Data quality expectation."""
            return True

        assert isinstance(my_expectation, Expectation)
        assert my_expectation.metadata == {
            "category": "data-quality",
            "owner": "data-team",
        }

    def test_expect_decorator_with_config(self) -> None:
        """Test @expect decorator with config."""

        @expect(config={"threshold": 0.95, "sample_size": 1000})
        def my_expectation(data: Any) -> bool:
            """Configurable expectation."""
            return True

        assert isinstance(my_expectation, Expectation)
        assert my_expectation.config == {"threshold": 0.95, "sample_size": 1000}

    def test_expect_decorator_combined_parameters(self) -> None:
        """Test @expect decorator with multiple parameters."""

        @expect(
            name="custom_name",
            severity="warning",
            description="Custom description",
            metadata={"tag": "important"},
            config={"max_retries": 3},
        )
        def my_expectation(data: Any) -> bool:
            """A function."""
            return True

        assert isinstance(my_expectation, Expectation)
        assert my_expectation.name == "custom_name"
        assert my_expectation.severity == "warning"
        assert my_expectation.description == "Custom description"
        assert my_expectation.metadata == {"tag": "important"}
        assert my_expectation.config == {"max_retries": 3}

    def test_expect_decorator_preserves_function_docstring(self) -> None:
        """Test that function docstring is used as default description."""

        @expect
        def documented_expectation(data: Any) -> bool:
            """This is a documented expectation."""
            return True

        assert isinstance(documented_expectation, Expectation)
        assert documented_expectation.description == "This is a documented expectation."

    def test_expect_decorator_explicit_description_overrides_docstring(
        self,
    ) -> None:
        """Test that explicit description parameter overrides docstring."""

        @expect(description="Custom description")
        def documented_expectation(data: Any) -> bool:
            """This is a docstring."""
            return True

        assert isinstance(documented_expectation, Expectation)
        assert documented_expectation.description == "Custom description"

    def test_expect_decorator_empty_name_raises_error(self) -> None:
        """Test that empty name raises error."""

        from vibe_piper.types import Expectation

        with pytest.raises(ValueError, match="name cannot be empty"):
            Expectation(
                name="",
                fn=lambda _: ValidationResult(is_valid=True),
            )

    def test_expect_decorator_validate_method(self) -> None:
        """Test the validate method of Expectation."""

        @expect
        def even_numbers(data: list[int]) -> bool:
            """All numbers should be even."""
            return all(x % 2 == 0 for x in data)

        # Valid data
        result = even_numbers.validate([2, 4, 6, 8])
        assert result.is_valid is True
        assert len(result.errors) == 0

        # Invalid data
        result = even_numbers.validate([2, 3, 6])
        assert result.is_valid is False
        assert len(result.errors) > 0

    def test_expect_decorator_bool_to_validation_result_conversion(self) -> None:
        """Test that bool return values are converted to ValidationResult."""

        @expect
        def simple_check(data: Any) -> bool:
            """Simple check returning bool."""
            return len(data) > 0

        # True should convert to successful ValidationResult
        result = simple_check.validate([1, 2, 3])
        assert result.is_valid is True

        # False should convert to failed ValidationResult with error
        result = simple_check.validate([])
        assert result.is_valid is False
        assert len(result.errors) == 1
        assert "failed" in result.errors[0]

    def test_expect_decorator_with_dict_data(self) -> None:
        """Test @expect decorator with dictionary data."""

        @expect
        def has_required_fields(data: dict[str, Any]) -> bool:
            """Data should have required fields."""
            required = {"id", "name", "email"}
            return required.issubset(data.keys())

        assert isinstance(has_required_fields, Expectation)

        # Valid data
        result = has_required_fields.validate(
            {"id": 1, "name": "Alice", "email": "alice@example.com"}
        )
        assert result.is_valid is True

        # Invalid data
        result = has_required_fields.validate({"id": 1, "name": "Alice"})
        assert result.is_valid is False

    def test_expect_decorator_with_list_of_records(self) -> None:
        """Test @expect decorator validating list of records."""

        @expect
        def no_null_ids(data: list[dict[str, Any]]) -> ValidationResult:
            """No record should have null ID."""
            null_records = [i for i, r in enumerate(data) if r.get("id") is None]
            if null_records:
                return ValidationResult(
                    is_valid=False,
                    errors=(f"Records with null IDs at indices: {null_records}",),
                )
            return ValidationResult(is_valid=True)

        assert isinstance(no_null_ids, Expectation)

        # Valid data
        result = no_null_ids.validate([{"id": 1}, {"id": 2}, {"id": 3}])
        assert result.is_valid is True

        # Invalid data
        result = no_null_ids.validate([{"id": 1}, {"id": None}, {"id": 3}])
        assert result.is_valid is False
        assert "null IDs" in result.errors[0]

    def test_expect_decorator_with_custom_validation_logic(self) -> None:
        """Test @expect decorator with complex validation logic."""

        @expect
        def email_format(data: list[dict[str, Any]]) -> ValidationResult:
            """Emails should be valid."""
            errors: list[str] = []
            for i, record in enumerate(data):
                email = record.get("email", "")
                if "@" not in email:
                    errors.append(f"Record {i}: Invalid email '{email}'")

            if errors:
                return ValidationResult(is_valid=False, errors=tuple(errors))
            return ValidationResult(is_valid=True)

        assert isinstance(email_format, Expectation)

        # Valid data
        result = email_format.validate(
            [{"email": "user1@example.com"}, {"email": "user2@example.com"}]
        )
        assert result.is_valid is True

        # Invalid data
        result = email_format.validate([{"email": "valid@example.com"}, {"email": "invalid-email"}])
        assert result.is_valid is False
        assert len(result.errors) == 1
        assert "Record 1" in result.errors[0]
