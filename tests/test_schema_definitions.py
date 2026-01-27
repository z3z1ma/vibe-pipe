"""
Tests for declarative schema definitions.

This module tests the declarative schema API, including field types,
schema conversion, validation, and inheritance.
"""

import pytest

from vibe_piper import (
    AnyType,
    Array,
    Boolean,
    DataRecord,
    Date,
    DateTime,
    Float,
    Integer,
    Object,
    Schema,
    String,
    define_schema,
)
from vibe_piper.types import DataType

# =============================================================================
# Field Tests
# =============================================================================


class TestStringField:
    """Tests for String field type."""

    def test_create_basic_string(self) -> None:
        """Test creating a basic String field."""
        field = String()
        assert field.data_type == DataType.STRING
        assert field.nullable is False
        assert field.required is True

    def test_string_with_constraints(self) -> None:
        """Test String field with constraints."""
        field = String(min_length=1, max_length=100)
        assert field.constraints["min_length"] == 1
        assert field.constraints["max_length"] == 100

    def test_string_with_pattern(self) -> None:
        """Test String field with regex pattern."""
        field = String(pattern=r"^[A-Z]{2}-\d{4}$")
        assert field.constraints["pattern"] == r"^[A-Z]{2}-\d{4}$"

    def test_string_nullable(self) -> None:
        """Test nullable String field."""
        field = String(nullable=True)
        assert field.nullable is True

    def test_string_with_default(self) -> None:
        """Test String field with default value."""
        field = String(default="default_value")
        assert field.default == "default_value"

    def test_string_with_description(self) -> None:
        """Test String field with description."""
        field = String(description="User email address")
        assert field.description == "User email address"


class TestIntegerField:
    """Tests for Integer field type."""

    def test_create_basic_integer(self) -> None:
        """Test creating a basic Integer field."""
        field = Integer()
        assert field.data_type == DataType.INTEGER
        assert field.nullable is False
        assert field.required is True

    def test_integer_with_constraints(self) -> None:
        """Test Integer field with value constraints."""
        field = Integer(min_value=0, max_value=100)
        assert field.constraints["min_value"] == 0
        assert field.constraints["max_value"] == 100

    def test_integer_with_aliases(self) -> None:
        """Test Integer field with ge/le aliases."""
        field = Integer(ge=0, le=100)
        assert field.constraints["min_value"] == 0
        assert field.constraints["max_value"] == 100

    def test_integer_conflicting_aliases(self) -> None:
        """Test that min_value and ge cannot both be specified."""
        with pytest.raises(ValueError, match="Cannot specify both min_value and ge"):
            Integer(min_value=0, ge=1)

    def test_integer_nullable(self) -> None:
        """Test nullable Integer field."""
        field = Integer(nullable=True)
        assert field.nullable is True


class TestFloatField:
    """Tests for Float field type."""

    def test_create_basic_float(self) -> None:
        """Test creating a basic Float field."""
        field = Float()
        assert field.data_type == DataType.FLOAT
        assert field.nullable is False
        assert field.required is True

    def test_float_with_constraints(self) -> None:
        """Test Float field with value constraints."""
        field = Float(min_value=0.0, max_value=100.0)
        assert field.constraints["min_value"] == 0.0
        assert field.constraints["max_value"] == 100.0


class TestBooleanField:
    """Tests for Boolean field type."""

    def test_create_basic_boolean(self) -> None:
        """Test creating a basic Boolean field."""
        field = Boolean()
        assert field.data_type == DataType.BOOLEAN

    def test_boolean_with_default(self) -> None:
        """Test Boolean field with default value."""
        field = Boolean(default=True)
        assert field.default is True


class TestDateTimeField:
    """Tests for DateTime field type."""

    def test_create_basic_datetime(self) -> None:
        """Test creating a basic DateTime field."""
        field = DateTime()
        assert field.data_type == DataType.DATETIME


class TestDateField:
    """Tests for Date field type."""

    def test_create_basic_date(self) -> None:
        """Test creating a basic Date field."""
        field = Date()
        assert field.data_type == DataType.DATE


class TestArrayField:
    """Tests for Array field type."""

    def test_create_basic_array(self) -> None:
        """Test creating a basic Array field."""
        field = Array()
        assert field.data_type == DataType.ARRAY


class TestObjectField:
    """Tests for Object field type."""

    def test_create_basic_object(self) -> None:
        """Test creating a basic Object field."""
        field = Object()
        assert field.data_type == DataType.OBJECT


class TestAnyTypeField:
    """Tests for AnyType field type."""

    def test_create_basic_any(self) -> None:
        """Test creating a basic AnyType field."""
        field = AnyType()
        assert field.data_type == DataType.ANY


# =============================================================================
# Schema Definition Tests
# =============================================================================


class TestDefineSchema:
    """Tests for @define_schema decorator."""

    def test_basic_schema_definition(self) -> None:
        """Test defining a basic schema."""

        @define_schema
        class UserSchema:
            id: Integer = Integer()
            email: String = String(max_length=255)

        schema = UserSchema.to_schema()
        assert isinstance(schema, Schema)
        assert schema.name == "UserSchema"
        assert len(schema.fields) == 2

    def test_schema_with_custom_name(self) -> None:
        """Test schema with custom name."""

        @define_schema(name="custom_user_schema")
        class UserSchema:
            id: Integer = Integer()

        schema = UserSchema.to_schema()
        assert schema.name == "custom_user_schema"

    def test_schema_with_description(self) -> None:
        """Test schema with description."""

        @define_schema(description="User account schema")
        class UserSchema:
            id: Integer = Integer()

        schema = UserSchema.to_schema()
        assert schema.description == "User account schema"

    def test_schema_with_metadata(self) -> None:
        """Test schema with metadata."""

        @define_schema(metadata={"owner": "data-team", "pii": True})
        class UserSchema:
            id: Integer = Integer()

        schema = UserSchema.to_schema()
        assert schema.metadata["owner"] == "data-team"
        assert schema.metadata["pii"] is True


# =============================================================================
# Schema Conversion Tests
# =============================================================================


class TestSchemaConversion:
    """Tests for converting declarative schemas to Schema objects."""

    def test_to_schema_returns_schema_object(self) -> None:
        """Test that to_schema() returns a Schema object."""

        @define_schema
        class UserSchema:
            id: Integer = Integer()
            email: String = String(max_length=255)

        schema = UserSchema.to_schema()
        assert isinstance(schema, Schema)

    def test_field_conversion(self) -> None:
        """Test that fields are converted to SchemaField objects."""

        @define_schema
        class UserSchema:
            id: Integer = Integer()
            email: String = String(max_length=255)

        schema = UserSchema.to_schema()
        assert len(schema.fields) == 2

        id_field = schema.get_field("id")
        assert id_field is not None
        assert id_field.data_type == DataType.INTEGER

        email_field = schema.get_field("email")
        assert email_field is not None
        assert email_field.data_type == DataType.STRING
        assert email_field.constraints["max_length"] == 255

    def test_nullable_field_conversion(self) -> None:
        """Test that nullable fields are converted correctly."""

        @define_schema
        class UserSchema:
            age: Integer = Integer(nullable=True)

        schema = UserSchema.to_schema()
        age_field = schema.get_field("age")
        assert age_field is not None
        assert age_field.nullable is True


# =============================================================================
# Validation Tests
# =============================================================================


class TestValidation:
    """Tests for data validation using declarative schemas."""

    def test_validate_valid_data(self) -> None:
        """Test validating valid data."""

        @define_schema
        class UserSchema:
            id: Integer = Integer()
            email: String = String(max_length=255)

        data = {"id": 1, "email": "user@example.com"}
        record = UserSchema.validate(data)

        assert isinstance(record, DataRecord)
        assert record.data == data
        assert record.schema == UserSchema.to_schema()

    def test_validate_missing_required_field(self) -> None:
        """Test that missing required fields raise an error."""

        @define_schema
        class UserSchema:
            id: Integer = Integer()
            email: String = String(max_length=255)

        data = {"id": 1}  # Missing email

        with pytest.raises(ValueError, match="Required field"):
            UserSchema.validate(data)

    def test_validate_with_nullable_field(self) -> None:
        """Test validating data with nullable field."""

        @define_schema
        class UserSchema:
            id: Integer = Integer()
            age: Integer = Integer(nullable=True)

        data = {"id": 1, "age": None}
        record = UserSchema.validate(data)

        assert record.data["age"] is None


# =============================================================================
# Inheritance Tests
# =============================================================================


class TestSchemaInheritance:
    """Tests for schema inheritance."""

    def test_basic_inheritance(self) -> None:
        """Test basic schema inheritance."""

        @define_schema
        class BaseSchema:
            id: Integer = Integer()
            created_at: DateTime = DateTime()

        @define_schema
        class UserSchema(BaseSchema):
            email: String = String(max_length=255)

        schema = UserSchema.to_schema()
        assert len(schema.fields) == 3
        assert schema.has_field("id")
        assert schema.has_field("created_at")
        assert schema.has_field("email")

    def test_override_parent_field(self) -> None:
        """Test overriding parent field."""

        @define_schema
        class BaseSchema:
            id: Integer = Integer()

        @define_schema
        class UserSchema(BaseSchema):
            id: Integer = Integer(description="User ID")

        schema = UserSchema.to_schema()
        id_field = schema.get_field("id")
        assert id_field is not None
        assert id_field.description == "User ID"


# =============================================================================
# Field Descriptor Tests
# =============================================================================


class TestFieldDescriptor:
    """Tests for Field descriptor behavior."""

    def test_field_name_from_class(self) -> None:
        """Test accessing field name from class."""

        @define_schema
        class UserSchema:
            email: String = String(max_length=255)

        # Accessing field from class returns its name
        field_name = UserSchema.email
        assert field_name == "email"

    def test_field_not_accessible_from_instance(self) -> None:
        """Test that fields cannot be accessed from instances."""

        @define_schema
        class UserSchema:
            email: String = String(max_length=255)

        # Create instance (not really supported, but testing behavior)
        with pytest.raises(AttributeError, match="cannot be accessed from instances"):
            # This should fail because we don't support instance creation
            instance = UserSchema()
            _ = instance.email


# =============================================================================
# Error Handling Tests
# =============================================================================


class TestErrorHandling:
    """Tests for error handling."""

    def test_string_field_with_invalid_constraints(self) -> None:
        """Test String field with invalid constraint values."""
        # This should not raise - validation happens at use time
        field = String(min_length=-1, max_length=0)
        assert field.constraints["min_length"] == -1
        assert field.constraints["max_length"] == 0


# =============================================================================
# Integration Tests
# =============================================================================


class TestIntegration:
    """Integration tests for declarative schema API."""

    def test_full_workflow(self) -> None:
        """Test a complete workflow with schema definition and validation."""

        # Define schema
        @define_schema(
            name="user",
            description="User account schema",
            metadata={"owner": "data-team"},
        )
        class UserSchema:
            id: Integer = Integer(description="Unique user identifier")
            email: String = String(max_length=255, nullable=False)
            age: Integer = Integer(min_value=0, max_value=120, nullable=True)
            is_active: Boolean = Boolean(default=True)

        # Convert to Schema
        schema = UserSchema.to_schema()
        assert schema.name == "user"
        assert len(schema.fields) == 4

        # Validate data
        data = {
            "id": 1,
            "email": "user@example.com",
            "age": 25,
            "is_active": True,
        }
        record = UserSchema.validate(data)
        assert record.data == data

    def test_compose_schemas_with_mixins(self) -> None:
        """Test composing schemas with mixin classes."""

        @define_schema
        class TimestampMixin:
            created_at: DateTime = DateTime()
            updated_at: DateTime = DateTime()

        @define_schema
        class SoftDeleteMixin:
            deleted_at: DateTime = DateTime(nullable=True)
            is_deleted: Boolean = Boolean(default=False)

        @define_schema
        class UserSchema(TimestampMixin, SoftDeleteMixin):
            id: Integer = Integer()
            email: String = String(max_length=255)

        schema = UserSchema.to_schema()
        assert len(schema.fields) == 6
        assert schema.has_field("created_at")
        assert schema.has_field("updated_at")
        assert schema.has_field("deleted_at")
        assert schema.has_field("is_deleted")
        assert schema.has_field("id")
        assert schema.has_field("email")
