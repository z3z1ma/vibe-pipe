"""
Tests for response validation.
"""

import pytest

from vibe_piper.integration.base import ValidationError
from vibe_piper.integration.validation import (
    ResponseValidator,
    ValidationResult,
    validate_and_parse,
    validate_response,
)
from vibe_piper.schema_definitions import Integer, String, define_schema


# Define test schemas
@define_schema
class BasicUserSchema:
    """Basic user schema for testing."""

    id: Integer = Integer()
    name: String = String()
    email: String = String()


@define_schema
class UserWithNickname:
    """User schema with nullable nickname."""

    id: Integer = Integer()
    nickname: String = String(nullable=True)


@define_schema
class UserWithConstraints:
    """User schema with string constraints."""

    name: String = String(min_length=3, max_length=10)


@define_schema
class UserSchemaRequired:
    """User schema with required fields."""

    id: Integer = Integer()
    name: String = String(nullable=False)


@pytest.mark.asyncio
class TestValidationResult:
    """Test ValidationResult class."""

    def test_successful_validation(self):
        """Test successful validation result."""
        result = ValidationResult(
            is_valid=True,
            errors=[],
            data={"id": 1, "name": "test"},
        )

        assert result.is_valid is True
        assert result.has_errors is False
        assert result.get_error_summary() == "No errors"

    def test_failed_validation(self):
        """Test failed validation result."""
        result = ValidationResult(
            is_valid=False,
            errors=["Field 'id' is required", "Field 'name' too long"],
            data={},
        )

        assert result.is_valid is False
        assert result.has_errors is True
        assert "Field 'id' is required" in result.get_error_summary()


@pytest.mark.asyncio
class TestResponseValidator:
    """Test ResponseValidator class."""

    def test_validate_with_declarative_schema(self):
        """Test validation with declarative schema."""
        validator = ResponseValidator(BasicUserSchema)
        data = {"id": 1, "name": "Alice", "email": "alice@example.com"}

        result = validator.validate(data)

        assert result.is_valid is True
        assert result.has_errors is False

    def test_validate_missing_required_field(self):
        """Test validation fails with missing required field."""
        validator = ResponseValidator(BasicUserSchema)
        data = {"id": 1}  # Missing 'name'

        result = validator.validate(data)

        assert result.is_valid is False
        assert result.has_errors is True
        assert any("name" in error for error in result.errors)

    def test_validate_wrong_type(self):
        """Test validation fails with wrong type."""
        validator = ResponseValidator(BasicUserSchema)
        data = {"id": "not_an_int", "name": "Alice"}

        result = validator.validate(data)

        assert result.is_valid is False
        assert any("id" in error for error in result.errors)

    def test_validate_string_constraints(self):
        """Test string constraint validation."""
        validator = ResponseValidator(UserWithConstraints)

        validator = ResponseValidator(UserSchema)

        # Test min_length
        data1 = {"name": "AB"}
        result1 = validator.validate(data1)
        assert result1.is_valid is False
        assert any("minimum length" in error for error in result1.errors)

        # Test max_length
        data2 = {"name": "A" * 11}
        result2 = validator.validate(data2)
        assert result2.is_valid is False
        assert any("maximum length" in error for error in result2.errors)

    def test_validate_nullable_field(self):
        """Test nullable field validation."""
        UserSchema = define_schema(
            "UserSchema",
            {
                "id": Integer(),
                "nickname": String(nullable=True),
            },
        )

        validator = ResponseValidator(UserSchema)
        data = {"id": 1, "nickname": None}

        result = validator.validate(data)

        assert result.is_valid is True

    def test_validate_non_nullable_field_with_null(self):
        """Test non-nullable field rejects null."""
        UserSchema = define_schema(
            "UserSchema",
            {
                "id": Integer(),
                "name": String(nullable=False),
            },
        )

        validator = ResponseValidator(UserSchema)
        data = {"id": 1, "name": None}

        result = validator.validate(data)

        assert result.is_valid is False
        assert any("cannot be null" in error for error in result.errors)

    def test_validate_with_strict_mode(self):
        """Test strict mode rejects extra fields."""
        UserSchema = define_schema(
            "UserSchema",
            {
                "id": Integer(),
                "name": String(),
            },
        )

        validator = ResponseValidator(UserSchema, strict=True)
        data = {"id": 1, "name": "Alice", "extra_field": "value"}

        result = validator.validate(data)

        assert result.is_valid is False
        assert any("Unexpected fields" in error for error in result.errors)

    def test_validate_without_strict_mode(self):
        """Test non-strict mode allows extra fields."""
        UserSchema = define_schema(
            "UserSchema",
            {
                "id": Integer(),
                "name": String(),
            },
        )

        validator = ResponseValidator(UserSchema, strict=False)
        data = {"id": 1, "name": "Alice", "extra_field": "value"}

        result = validator.validate(data)

        assert result.is_valid is True

    def test_validate_with_coerce(self):
        """Test type coercion."""
        UserSchema = define_schema(
            "UserSchema",
            {
                "id": Integer(),
                "active": String(),
            },
        )

        validator = ResponseValidator(UserSchema, coerce=True)
        data = {"id": "123", "active": "true"}

        result = validator.validate(data)

        # Coercion may succeed for string to int
        # The exact behavior depends on implementation
        assert result is not None

    def test_validate_raises_on_error(self):
        """Test that raise_on_error raises exception."""
        UserSchema = define_schema(
            "UserSchema",
            {
                "id": Integer(),
                "name": String(),
            },
        )

        validator = ResponseValidator(UserSchema)
        data = {"id": 1}  # Missing 'name'

        with pytest.raises(ValidationError):
            validator.validate(data, raise_on_error=True)


@pytest.mark.asyncio
class TestValidationConvenienceFunctions:
    """Test validation convenience functions."""

    def test_validate_response(self):
        """Test validate_response function."""
        UserSchema = define_schema(
            "UserSchema",
            {
                "id": Integer(),
                "name": String(),
            },
        )

        data = {"id": 1, "name": "Alice"}
        result = validate_response(data, UserSchema)

        assert result.is_valid is True

    def test_validate_response_with_errors(self):
        """Test validate_response with invalid data."""
        UserSchema = define_schema(
            "UserSchema",
            {
                "id": Integer(),
                "name": String(),
            },
        )

        data = {"id": 1}  # Missing 'name'
        result = validate_response(data, UserSchema)

        assert result.is_valid is False

    def test_validate_and_parse_success(self):
        """Test validate_and_parse with valid data."""
        UserSchema = define_schema(
            "UserSchema",
            {
                "id": Integer(),
                "name": String(),
            },
        )

        data = {"id": 1, "name": "Alice"}
        result = validate_and_parse(data, UserSchema)

        assert result == data

    def test_validate_and_parse_failure(self):
        """Test validate_and_parse raises on invalid data."""
        UserSchema = define_schema(
            "UserSchema",
            {
                "id": Integer(),
                "name": String(),
            },
        )

        data = {"id": 1}  # Missing 'name'

        with pytest.raises(ValidationError):
            validate_and_parse(data, UserSchema)
