"""
Response validation for API clients.

This module provides validation utilities for API responses:
- Integration with schema_definitions
- Type-safe response parsing
- Validation error reporting
"""

import logging
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from vibe_piper.schema_definitions import DeclarativeSchema
from vibe_piper.types import Schema as LegacySchema

# =============================================================================
# Validation Result
# =============================================================================


@dataclass
class ValidationResult:
    """
    Result of validating data against a schema.

    Contains validation status and error details.
    """

    is_valid: bool
    """Whether validation passed"""

    errors: list[str]
    """List of validation error messages"""

    data: Any | None
    """Original data that was validated"""

    @property
    def has_errors(self) -> bool:
        """Check if there are validation errors."""
        return len(self.errors) > 0

    def get_error_summary(self) -> str:
        """
        Get a summary of validation errors.

        Returns:
            Formatted error summary
        """
        if not self.has_errors:
            return "No errors"

        return "\n".join(f"- {error}" for error in self.errors)


# =============================================================================
# Response Validator
# =============================================================================


T = TypeVar("T")


class ResponseValidator(Generic[T]):
    """
    Validates API responses against schemas.

    Supports both declarative schemas and legacy schemas.
    """

    def __init__(
        self,
        schema: type[DeclarativeSchema] | LegacySchema,
        *,
        strict: bool = False,
        coerce: bool = False,
    ) -> None:
        """
        Initialize response validator.

        Args:
            schema: Schema to validate against
            strict: If True, reject data with extra fields
            coerce: If True, attempt to coerce types to match schema
        """
        self.schema = schema
        self.strict = strict
        self.coerce = coerce
        self._logger = logging.getLogger(self.__class__.__name__)

    def validate(
        self,
        data: dict[str, Any],
        *,
        raise_on_error: bool = False,
    ) -> ValidationResult:
        """
        Validate data against schema.

        Args:
            data: Data to validate
            raise_on_error: If True, raise exception on validation failure

        Returns:
            ValidationResult instance

        Raises:
            ValidationError: If raise_on_error is True and validation fails
        """
        errors = []

        # Determine schema type
        if isinstance(self.schema, type) and issubclass(self.schema, DeclarativeSchema):
            # Declarative schema
            schema_obj = self.schema.to_schema()
            errors = self._validate_with_legacy_schema(data, schema_obj)
        elif isinstance(self.schema, LegacySchema):
            # Legacy schema
            errors = self._validate_with_legacy_schema(data, self.schema)
        else:
            errors = [f"Unsupported schema type: {type(self.schema)}"]

        is_valid = len(errors) == 0

        if raise_on_error and not is_valid:
            from vibe_piper.integration.base import ValidationError

            raise ValidationError(
                f"Response validation failed: {errors}",
                errors=errors,
            )

        return ValidationResult(
            is_valid=is_valid,
            errors=errors,
            data=data,
        )

    def _validate_with_legacy_schema(
        self,
        data: dict[str, Any],
        schema: LegacySchema,
    ) -> list[str]:
        """
        Validate data using legacy schema.

        Args:
            data: Data to validate
            schema: Legacy schema instance

        Returns:
            List of validation errors
        """
        errors = []

        # Check required fields
        for field in schema.fields:
            field_name = field.name

            # Check if field is required
            if field.required and field_name not in data:
                errors.append(f"Required field '{field_name}' is missing")
                continue

            # Skip validation if field is missing and not required
            if field_name not in data:
                continue

            value = data[field_name]

            # Check nullable
            if value is None:
                if not field.nullable:
                    errors.append(f"Field '{field_name}' cannot be null")
                continue

            # Type validation
            type_errors = self._validate_field_type(field, value)
            errors.extend(type_errors)

            # Constraint validation
            constraint_errors = self._validate_field_constraints(field, value)
            errors.extend(constraint_errors)

        # Check for extra fields in strict mode
        if self.strict:
            field_names = {f.name for f in schema.fields}
            extra_fields = set(data.keys()) - field_names

            if extra_fields:
                errors.append(f"Unexpected fields: {', '.join(extra_fields)}")

        return errors

    def _validate_field_type(
        self,
        field: Any,
        value: Any,
    ) -> list[str]:
        """
        Validate field type.

        Args:
            field: Schema field
            value: Field value

        Returns:
            List of type validation errors
        """
        from vibe_piper.types import DataType

        errors = []

        # Map DataType to Python types
        type_map = {
            DataType.STRING: str,
            DataType.INTEGER: int,
            DataType.FLOAT: (int, float),
            DataType.BOOLEAN: bool,
            DataType.DATETIME: str,  # Will validate format separately
            DataType.DATE: str,
            DataType.ARRAY: list,
            DataType.OBJECT: dict,
            DataType.ANY: object,
        }

        expected_types = type_map.get(field.data_type, object)

        # Check type
        if not isinstance(value, expected_types):
            if self.coerce:
                # Attempt type coercion
                coerced = self._coerce_value(field.data_type, value)
                if coerced is not None:
                    return []  # Coercion succeeded

            errors.append(
                f"Field '{field.name}' expected type {field.data_type.name}, got {type(value).__name__}"
            )

        return errors

    def _validate_field_constraints(
        self,
        field: Any,
        value: Any,
    ) -> list[str]:
        """
        Validate field constraints.

        Args:
            field: Schema field
            value: Field value

        Returns:
            List of constraint validation errors
        """
        errors = []

        if not field.constraints:
            return errors

        from vibe_piper.types import DataType

        # String constraints
        if field.data_type == DataType.STRING:
            if isinstance(value, str):
                if (
                    "min_length" in field.constraints
                    and len(value) < field.constraints["min_length"]
                ):
                    errors.append(
                        f"Field '{field.name}' minimum length is {field.constraints['min_length']}, got {len(value)}"
                    )

                if (
                    "max_length" in field.constraints
                    and len(value) > field.constraints["max_length"]
                ):
                    errors.append(
                        f"Field '{field.name}' maximum length is {field.constraints['max_length']}, got {len(value)}"
                    )

                if "pattern" in field.constraints:
                    import re

                    pattern = field.constraints["pattern"]
                    if not re.match(pattern, value):
                        errors.append(
                            f"Field '{field.name}' does not match pattern '{pattern}'"
                        )

        # Numeric constraints
        elif field.data_type in (DataType.INTEGER, DataType.FLOAT):
            if isinstance(value, (int, float)):
                if "min" in field.constraints and value < field.constraints["min"]:
                    errors.append(
                        f"Field '{field.name}' minimum value is {field.constraints['min']}, got {value}"
                    )

                if "max" in field.constraints and value > field.constraints["max"]:
                    errors.append(
                        f"Field '{field.name}' maximum value is {field.constraints['max']}, got {value}"
                    )

        return errors

    def _coerce_value(
        self,
        data_type: Any,
        value: Any,
    ) -> Any | None:
        """
        Attempt to coerce value to specified type.

        Args:
            data_type: Target data type
            value: Value to coerce

        Returns:
            Coerced value or None if coercion fails
        """
        from vibe_piper.types import DataType

        try:
            if data_type == DataType.STRING:
                return str(value)
            elif data_type == DataType.INTEGER:
                return int(value)
            elif data_type == DataType.FLOAT:
                return float(value)
            elif data_type == DataType.BOOLEAN:
                if isinstance(value, str):
                    return value.lower() in ("true", "1", "yes")
                return bool(value)
            else:
                return None
        except (ValueError, TypeError):
            return None


# =============================================================================
# Convenience Functions
# =============================================================================


def validate_response(
    data: dict[str, Any],
    schema: type[DeclarativeSchema] | LegacySchema,
    *,
    strict: bool = False,
    raise_on_error: bool = False,
) -> ValidationResult:
    """
    Validate response data against a schema.

    Args:
        data: Response data to validate
        schema: Schema to validate against
        strict: If True, reject data with extra fields
        raise_on_error: If True, raise exception on validation failure

    Returns:
        ValidationResult instance

    Example:
        ```python
        from vibe_piper import define_schema, String, Integer

        UserSchema = define_schema("User", {
            "id": Integer(),
            "name": String(max_length=100),
        })

        result = validate_response(
            {"id": 1, "name": "Alice"},
            UserSchema,
        )
        ```
    """
    validator = ResponseValidator(schema, strict=strict)
    return validator.validate(data, raise_on_error=raise_on_error)


def validate_and_parse(
    data: dict[str, Any],
    schema: type[DeclarativeSchema],
    *,
    strict: bool = False,
) -> dict[str, Any]:
    """
    Validate and parse response data, raising on errors.

    This is a convenience function that validates and raises on failure.

    Args:
        data: Response data to validate
        schema: Schema to validate against
        strict: If True, reject data with extra fields

    Returns:
        Original data (if validation passes)

    Raises:
        ValidationError: If validation fails
    """
    result = validate_response(data, schema, strict=strict, raise_on_error=True)
    return result.data
