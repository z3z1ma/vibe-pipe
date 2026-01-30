"""
Validation helpers for transformations.

Provides schema-aware validation functions that can be used in transformation pipelines.
"""

from collections.abc import Callable
from typing import Any

from vibe_piper.types import (
    DataRecord,
    DataType,
    Schema,
    ValidationResult,
)


def validate_field_type(
    record: DataRecord,
    field_name: str,
    expected_type: DataType,
) -> bool:
    """
    Validate that a field has the expected data type.

    Args:
        record: The record to validate
        field_name: Name of the field to validate
        expected_type: Expected data type

    Returns:
        True if valid, False otherwise

    Example:
        Validate field type::

            if validate_field_type(record, "age", DataType.INTEGER):
                # Field is valid
                pass
    """
    value = record.get(field_name)

    if value is None:
        # None is valid for nullable fields
        return True

    type_map = {
        DataType.STRING: str,
        DataType.INTEGER: int,
        DataType.FLOAT: (int, float),
        DataType.BOOLEAN: bool,
        DataType.ARRAY: list,
        DataType.OBJECT: dict,
    }

    expected_python_types = type_map.get(expected_type)
    if expected_python_types is None:
        # Any type is valid
        return True

    return isinstance(value, expected_python_types)


def validate_field_required(record: DataRecord, field_name: str) -> bool:
    """
    Validate that a required field is not null.

    Args:
        record: The record to validate
        field_name: Name of the required field

    Returns:
        True if field is present and not null, False otherwise

    Example:
        Check required field::

            if validate_field_required(record, "email"):
                # Field is present and not null
                pass
    """
    value = record.get(field_name)
    return value is not None


def validate_email_format(email: str) -> bool:
    """
    Validate email format.

    Args:
        email: Email string to validate

    Returns:
        True if valid email format, False otherwise

    Example:
        Validate email::

            if validate_email_format("user@example.com"):
                # Email is valid
                pass
    """
    if not email or not isinstance(email, str):
        return False
    return "@" in email and "." in email.split("@")[-1]


def validate_regex_pattern(value: str, pattern: str) -> bool:
    r"""
    Validate a string matches a regex pattern.

    Args:
        value: String to validate
        pattern: Regex pattern to match

    Returns:
        True if value matches pattern, False otherwise

    Example:
        Validate pattern::

            if validate_regex_pattern("ABC-123", r"^[A-Z]{3}-\d{3}$"):
                # Value matches pattern
                pass
    """
    import re

    if not value or not isinstance(value, str):
        return False
    return re.match(pattern, value) is not None


def validate_range(
    value: int | float,
    min_value: int | float | None = None,
    max_value: int | float | None = None,
) -> bool:
    """
    Validate that a numeric value is within a range.

    Args:
        value: Numeric value to validate
        min_value: Minimum allowed value (inclusive)
        max_value: Maximum allowed value (inclusive)

    Returns:
        True if value is within range, False otherwise

    Example:
        Validate age range::

            if validate_range(age, min_value=0, max_value=120):
                # Age is valid
                pass
    """
    if not isinstance(value, (int, float)):
        return False

    if min_value is not None and value < min_value:
        return False

    if max_value is not None and value > max_value:
        return False

    return True


def validate_field_length(
    value: str | list | dict,
    min_length: int | None = None,
    max_length: int | None = None,
) -> bool:
    """
    Validate that a field's length is within bounds.

    Args:
        value: Value to check (string, list, or dict)
        min_length: Minimum allowed length (inclusive)
        max_length: Maximum allowed length (inclusive)

    Returns:
        True if length is within bounds, False otherwise

    Example:
        Validate string length::

            if validate_field_length(name, min_length=1, max_length=100):
                # Name length is valid
                pass
    """
    if value is None:
        return True

    if not isinstance(value, (str, list, dict)):
        return False

    length = len(value)

    if min_length is not None and length < min_length:
        return False

    if max_length is not None and length > max_length:
        return False

    return True


def validate_enum(value: Any, allowed_values: list[Any]) -> bool:
    """
    Validate that a value is in the allowed set.

    Args:
        value: Value to validate
        allowed_values: List of allowed values

    Returns:
        True if value is in allowed set, False otherwise

    Example:
        Validate enum::

            if validate_enum(status, ["active", "inactive", "pending"]):
                # Status is valid
                pass
    """
    return value in allowed_values


def validate_record(
    record: DataRecord,
    schema: Schema,
    strict: bool = False,
) -> ValidationResult:
    """
    Validate a record against a schema.

    Args:
        record: The record to validate
        schema: Schema to validate against
        strict: If True, reject records with extra fields not in schema

    Returns:
        ValidationResult with validation status and any errors

    Example:
        Validate record against schema::

            result = validate_record(record, user_schema)
            if not result.is_valid:
                print(result.errors)
    """
    errors: list[str] = []
    warnings: list[str] = []

    # Get schema fields as a dict for efficient lookup
    schema_fields = {field.name: field for field in schema.fields}

    # Check required fields
    for field in schema.fields:
        if field.required and field.name not in record.data:
            errors.append(f"Required field '{field.name}' is missing")

    # Validate field types and constraints
    for field_name, field in schema_fields.items():
        if field_name not in record.data:
            continue

        value = record.get(field_name)

        # Skip null values for nullable fields
        if value is None and field.nullable:
            continue

        # Type validation
        if not validate_field_type(record, field_name, field.data_type):
            errors.append(f"Field '{field_name}' has invalid type (expected {field.data_type})")

        # Constraint validations
        if field.constraints:
            for constraint_name, constraint_value in field.constraints.items():
                if constraint_name == "min_value" and isinstance(value, (int, float)):
                    if value < constraint_value:
                        errors.append(
                            f"Field '{field_name}' is below minimum value {constraint_value}"
                        )

                elif constraint_name == "max_value" and isinstance(value, (int, float)):
                    if value > constraint_value:
                        errors.append(
                            f"Field '{field_name}' exceeds maximum value {constraint_value}"
                        )

                elif constraint_name == "min_length" and isinstance(value, (str, list)):
                    if len(value) < constraint_value:
                        errors.append(
                            f"Field '{field_name}' is below minimum length {constraint_value}"
                        )

                elif constraint_name == "max_length" and isinstance(value, (str, list)):
                    if len(value) > constraint_value:
                        errors.append(
                            f"Field '{field_name}' exceeds maximum length {constraint_value}"
                        )

                elif constraint_name == "pattern" and isinstance(value, str):
                    if not validate_regex_pattern(value, constraint_value):
                        errors.append(
                            f"Field '{field_name}' does not match pattern '{constraint_value}'"
                        )

                elif constraint_name == "enum":
                    if not validate_enum(value, constraint_value):
                        errors.append(
                            f"Field '{field_name}' must be one of {constraint_value}, got '{value}'"
                        )

    # Check for extra fields in strict mode
    if strict:
        for field_name in record.data.keys():
            if field_name not in schema_fields:
                warnings.append(f"Field '{field_name}' is not defined in schema")

    is_valid = len(errors) == 0

    return ValidationResult(
        is_valid=is_valid,
        errors=tuple(errors) if not is_valid else (),
        warnings=tuple(warnings),
    )


def validate_batch(
    records: list[DataRecord],
    schema: Schema,
    strict: bool = False,
) -> ValidationResult:
    """
    Validate a batch of records against a schema.

    Args:
        records: List of records to validate
        schema: Schema to validate against
        strict: If True, reject records with extra fields not in schema

    Returns:
        ValidationResult with validation status and any errors

    Example:
        Validate batch of records::

            result = validate_batch(records, user_schema)
            if not result.is_valid:
                print(f"Found {len(result.errors)} errors")
    """
    all_errors: list[str] = []
    all_warnings: list[str] = []

    for idx, record in enumerate(records):
        result = validate_record(record, schema, strict=strict)
        if not result.is_valid:
            for error in result.errors:
                all_errors.append(f"Record {idx}: {error}")
        for warning in result.warnings:
            all_warnings.append(f"Record {idx}: {warning}")

    is_valid = len(all_errors) == 0

    return ValidationResult(
        is_valid=is_valid,
        errors=tuple(all_errors) if not is_valid else (),
        warnings=tuple(all_warnings),
    )


def create_validator_from_schema(
    schema: Schema,
    strict: bool = False,
) -> Callable[[DataRecord], bool]:
    """
    Create a validator function from a schema.

    Args:
        schema: Schema to validate against
        strict: If True, reject records with extra fields not in schema

    Returns:
        Function that validates a record and returns bool

    Example:
        Create validator and use in filter::

            validator = create_validator_from_schema(user_schema)
            valid_records = [r for r in records if validator(r)]
    """

    def validator(record: DataRecord) -> bool:
        result = validate_record(record, schema, strict=strict)
        return result.is_valid

    return validator


def create_filter_validator(
    predicate: Callable[[DataRecord], bool],
    error_message: str = "Filter validation failed",
) -> Callable[[DataRecord], bool]:
    """
    Create a validator from a predicate function.

    Args:
        predicate: Function that takes a record and returns bool
        error_message: Error message to include in warnings

    Returns:
        Function that validates a record and returns bool

    Example:
        Create filter validator::

            validator = create_filter_validator(
                lambda r: r.get("age") >= 18,
                error_message="User must be 18 or older"
            )
    """

    def validator(record: DataRecord) -> bool:
        return predicate(record)

    return validator


__all__ = [
    # Field validators
    "validate_field_type",
    "validate_field_required",
    "validate_email_format",
    "validate_regex_pattern",
    "validate_range",
    "validate_field_length",
    "validate_enum",
    # Record/batch validators
    "validate_record",
    "validate_batch",
    # Validator creators
    "create_validator_from_schema",
    "create_filter_validator",
]
