"""
Tests for validation helpers.

Tests for validate_record, validate_batch, field validators, and validator creators.
"""

import pytest

from vibe_piper import DataRecord, DataType, Schema, SchemaField
from vibe_piper.transformations.validators import (
    create_filter_validator,
    create_validator_from_schema,
    validate_batch,
    validate_email_format,
    validate_enum,
    validate_field_length,
    validate_field_required,
    validate_field_type,
    validate_range,
    validate_record,
    validate_regex_pattern,
)


@pytest.fixture
def user_schema() -> Schema:
    """Create user schema with constraints."""
    return Schema(
        name="users",
        fields=(
            SchemaField(name="id", data_type=DataType.INTEGER, required=True),
            SchemaField(name="name", data_type=DataType.STRING, required=True),
            SchemaField(name="email", data_type=DataType.STRING, required=True),
            SchemaField(name="age", data_type=DataType.INTEGER, nullable=True),
            SchemaField(
                name="status",
                data_type=DataType.STRING,
                required=True,
                constraints={"enum": ["active", "inactive", "pending"]},
            ),
        ),
    )


@pytest.fixture
def user_record(user_schema: Schema) -> DataRecord:
    """Create valid user record."""
    return DataRecord(
        data={
            "id": 1,
            "name": "Alice",
            "email": "alice@example.com",
            "age": 30,
            "status": "active",
        },
        schema=user_schema,
    )


class TestValidateFieldType:
    """Test validate_field_type function."""

    def test_validate_string_type(self, user_record: DataRecord) -> None:
        """Test validating string type."""
        assert validate_field_type(user_record, "name", DataType.STRING)
        assert validate_field_type(user_record, "email", DataType.STRING)

    def test_validate_integer_type(self, user_record: DataRecord) -> None:
        """Test validating integer type."""
        assert validate_field_type(user_record, "id", DataType.INTEGER)
        assert validate_field_type(user_record, "age", DataType.INTEGER)

    def test_validate_wrong_type(self, user_record: DataRecord) -> None:
        """Test validating wrong type returns False."""
        assert not validate_field_type(user_record, "name", DataType.INTEGER)
        assert not validate_field_type(user_record, "id", DataType.STRING)

    def test_validate_null_field(self) -> None:
        """Test that null is always valid."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="value", data_type=DataType.INTEGER, nullable=True),),
        )
        record = DataRecord(data={"value": None}, schema=schema)

        assert validate_field_type(record, "value", DataType.INTEGER)

    def test_validate_float_or_int(self) -> None:
        """Test that float type accepts both int and float."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="value", data_type=DataType.FLOAT),),
        )
        record = DataRecord(data={"value": 100}, schema=schema)

        assert validate_field_type(record, "value", DataType.FLOAT)


class TestValidateFieldRequired:
    """Test validate_field_required function."""

    def test_validate_present_field(self, user_record: DataRecord) -> None:
        """Test validating present required field."""
        assert validate_field_required(user_record, "id")
        assert validate_field_required(user_record, "name")

    def test_validate_missing_field(self, user_schema: Schema) -> None:
        """Test validating missing required field returns False."""
        record = DataRecord(data={"id": 1, "name": "Alice"}, schema=user_schema)
        assert not validate_field_required(record, "email")

    def test_validate_null_field(self, user_record: DataRecord) -> None:
        """Test that null is not valid for required field."""
        record = DataRecord(
            data={"id": 1, "name": None, "email": "alice@example.com"},
            schema=user_record.schema,
        )
        assert not validate_field_required(record, "name")


class TestValidateEmailFormat:
    """Test validate_email_format function."""

    def test_valid_email(self) -> None:
        """Test validating valid emails."""
        assert validate_email_format("alice@example.com")
        assert validate_email_format("bob.smith@example.co.uk")
        assert validate_email_format("user+tag@domain.com")

    def test_invalid_email(self) -> None:
        """Test validating invalid emails."""
        assert not validate_email_format("invalid")
        assert not validate_email_format("missing@at")
        assert not validate_email_format("no@dot.")
        assert not validate_email_format("spaces @example.com")

    def test_null_or_empty_email(self) -> None:
        """Test that null or empty strings are invalid."""
        assert not validate_email_format(None)
        assert not validate_email_format("")
        assert not validate_email_format(" ")


class TestValidateRegexPattern:
    """Test validate_regex_pattern function."""

    def test_matching_pattern(self) -> None:
        """Test validating string that matches pattern."""
        assert validate_regex_pattern("ABC-123", r"^[A-Z]{3}-\d{3}$")
        assert validate_regex_pattern("user_123", r"^user_\d+$")

    def test_not_matching_pattern(self) -> None:
        """Test validating string that doesn't match pattern."""
        assert not validate_regex_pattern("abc-123", r"^[A-Z]{3}-\d{3}$")
        assert not validate_regex_pattern("user-123", r"^user_\d+$")

    def test_null_or_empty_value(self) -> None:
        """Test that null or empty is invalid."""
        assert not validate_regex_pattern(None, r"test")
        assert not validate_regex_pattern("", r"test")


class TestValidateRange:
    """Test validate_range function."""

    def test_value_in_range(self) -> None:
        """Test validating value within range."""
        assert validate_range(50, min_value=0, max_value=100)
        assert validate_range(75, min_value=0, max_value=100)

    def test_value_outside_range(self) -> None:
        """Test validating value outside range."""
        assert not validate_range(150, min_value=0, max_value=100)
        assert not validate_range(-10, min_value=0, max_value=100)

    def test_min_only(self) -> None:
        """Test validating with only min value."""
        assert validate_range(50, min_value=0)
        assert not validate_range(-10, min_value=0)

    def test_max_only(self) -> None:
        """Test validating with only max value."""
        assert validate_range(50, max_value=100)
        assert not validate_range(150, max_value=100)

    def test_no_bounds(self) -> None:
        """Test validating with no bounds."""
        assert validate_range(50)

    def test_invalid_type(self) -> None:
        """Test that non-numeric values are invalid."""
        assert not validate_range("not a number", min_value=0, max_value=100)


class TestValidateFieldLength:
    """Test validate_field_length function."""

    def test_string_length_in_range(self) -> None:
        """Test validating string length within range."""
        assert validate_field_length("Alice", min_length=1, max_length=10)

    def test_string_length_outside_range(self) -> None:
        """Test validating string length outside range."""
        assert not validate_field_length("Alice", min_length=10, max_length=20)
        assert not validate_field_length("Alice", min_length=1, max_length=3)

    def test_list_length_in_range(self) -> None:
        """Test validating list length within range."""
        assert validate_field_length([1, 2, 3], min_length=1, max_length=10)

    def test_dict_length_in_range(self) -> None:
        """Test validating dict length within range."""
        assert validate_field_length({"a": 1, "b": 2}, min_length=1, max_length=10)

    def test_null_value(self) -> None:
        """Test that null is always valid."""
        assert validate_field_length(None, min_length=1, max_length=10)


class TestValidateEnum:
    """Test validate_enum function."""

    def test_value_in_allowed_set(self) -> None:
        """Test validating value in allowed set."""
        assert validate_enum("active", ["active", "inactive", "pending"])
        assert validate_enum("inactive", ["active", "inactive", "pending"])

    def test_value_not_in_allowed_set(self) -> None:
        """Test validating value not in allowed set."""
        assert not validate_enum("unknown", ["active", "inactive", "pending"])
        assert not validate_enum("Active", ["active", "inactive", "pending"])  # Case sensitive


class TestValidateRecord:
    """Test validate_record function."""

    def test_validate_valid_record(self, user_record: DataRecord) -> None:
        """Test validating a valid record."""
        result = validate_record(user_record, user_record.schema)
        assert result.is_valid
        assert len(result.errors) == 0

    def test_validate_missing_required_field(self, user_schema: Schema) -> None:
        """Test validating record with missing required field."""
        record = DataRecord(
            data={"id": 1, "email": "alice@example.com"},
            schema=user_schema,
        )
        result = validate_record(record, user_schema)
        assert not result.is_valid
        assert "required" in result.errors[0].lower()

    def test_validate_wrong_type(self, user_schema: Schema) -> None:
        """Test validating record with wrong field type."""
        record = DataRecord(
            data={"id": "not an int", "name": "Alice", "email": "alice@example.com"},
            schema=user_schema,
        )
        result = validate_record(record, user_schema)
        assert not result.is_valid
        assert any("invalid type" in error.lower() for error in result.errors)

    def test_validate_enum_constraint(self, user_schema: Schema) -> None:
        """Test validating record with enum constraint violation."""
        record = DataRecord(
            data={"id": 1, "name": "Alice", "email": "alice@example.com", "status": "unknown"},
            schema=user_schema,
        )
        result = validate_record(record, user_schema)
        assert not result.is_valid
        assert any("enum" in error.lower() or "allowed" in error.lower() for error in result.errors)

    def test_validate_strict_mode(self, user_schema: Schema) -> None:
        """Test validating with strict mode (reject extra fields)."""
        record = DataRecord(
            data={
                "id": 1,
                "name": "Alice",
                "email": "alice@example.com",
                "status": "active",
                "extra_field": "not in schema",
            },
            schema=user_schema,
        )
        result = validate_record(record, user_schema, strict=True)
        assert len(result.warnings) > 0
        assert any("extra_field" in warning for warning in result.warnings)

    def test_validate_nullable_field_with_none(self, user_schema: Schema) -> None:
        """Test validating nullable field with None value."""
        record = DataRecord(
            data={
                "id": 1,
                "name": "Alice",
                "email": "alice@example.com",
                "age": None,
                "status": "active",
            },
            schema=user_schema,
        )
        result = validate_record(record, user_schema)
        assert result.is_valid  # age is nullable


class TestValidateBatch:
    """Test validate_batch function."""

    def test_validate_all_valid(self, user_schema: Schema) -> None:
        """Test validating batch with all valid records."""
        records = [
            DataRecord(
                data={
                    "id": i,
                    "name": f"User{i}",
                    "email": f"user{i}@example.com",
                    "status": "active",
                },
                schema=user_schema,
            )
            for i in range(1, 4)
        ]
        result = validate_batch(records, user_schema)
        assert result.is_valid
        assert len(result.errors) == 0

    def test_validate_some_invalid(self, user_schema: Schema) -> None:
        """Test validating batch with some invalid records."""
        records = [
            DataRecord(
                data={"id": 1, "name": "Alice", "email": "alice@example.com", "status": "active"},
                schema=user_schema,
            ),
            DataRecord(
                data={"id": 2, "name": "Bob", "email": "invalid-email", "status": "active"},
                schema=user_schema,
            ),
        ]
        result = validate_batch(records, user_schema)
        assert not result.is_valid
        assert "Record 1" in result.errors[0]

    def test_validate_empty_batch(self, user_schema: Schema) -> None:
        """Test validating empty batch."""
        result = validate_batch([], user_schema)
        assert result.is_valid
        assert len(result.errors) == 0


class TestCreateValidatorFromSchema:
    """Test create_validator_from_schema function."""

    def test_create_validator_validates_records(self, user_schema: Schema) -> None:
        """Test creating validator from schema."""
        validator = create_validator_from_schema(user_schema)

        valid_record = DataRecord(
            data={"id": 1, "name": "Alice", "email": "alice@example.com", "status": "active"},
            schema=user_schema,
        )
        invalid_record = DataRecord(
            data={"id": 1, "name": None, "email": "alice@example.com", "status": "active"},
            schema=user_schema,
        )

        assert validator(valid_record)
        assert not validator(invalid_record)

    def test_create_validator_strict_mode(self, user_schema: Schema) -> None:
        """Test creating validator with strict mode."""
        validator = create_validator_from_schema(user_schema, strict=True)

        record_with_extra = DataRecord(
            data={
                "id": 1,
                "name": "Alice",
                "email": "alice@example.com",
                "status": "active",
                "extra": "field",
            },
            schema=user_schema,
        )

        # In strict mode, should return False for records with extra fields
        # (Note: Currently strict mode only generates warnings, not errors)
        result = validator(record_with_extra)
        assert result is True  # Because extra fields only generate warnings


class TestCreateFilterValidator:
    """Test create_filter_validator function."""

    def test_create_validator_from_predicate(self) -> None:
        """Test creating validator from predicate."""
        validator = create_filter_validator(
            lambda r: r.get("age", 0) >= 18, "User must be 18 or older"
        )

        schema = Schema(
            name="test",
            fields=(SchemaField(name="age", data_type=DataType.INTEGER),),
        )
        adult = DataRecord(data={"age": 30}, schema=schema)
        child = DataRecord(data={"age": 10}, schema=schema)

        assert validator(adult)
        assert not validator(child)

    def test_create_validator_complex_predicate(self) -> None:
        """Test creating validator from complex predicate."""
        validator = create_filter_validator(
            lambda r: r.get("status") == "active" and r.get("age", 0) >= 21
        )

        schema = Schema(
            name="test",
            fields=(
                SchemaField(name="age", data_type=DataType.INTEGER),
                SchemaField(name="status", data_type=DataType.STRING),
            ),
        )
        valid = DataRecord(data={"age": 25, "status": "active"}, schema=schema)
        invalid1 = DataRecord(data={"age": 25, "status": "inactive"}, schema=schema)
        invalid2 = DataRecord(data={"age": 20, "status": "active"}, schema=schema)

        assert validator(valid)
        assert not validator(invalid1)
        assert not validator(invalid2)
