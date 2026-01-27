"""
Built-in schema expectations for Vibe Piper.

__all__ = [

This module provides a library of pre-built expectations for common schema
validations. These expectations can be used directly or as examples for
creating custom expectations.

Example:
    >>> from vibe_piper.expectations import expect_column_to_exist
    >>> from vibe_piper import Schema, SchemaField, DataType
    >>>
    >>> schema = Schema(
    ...     name="users",
    ...     fields=(
    ...         SchemaField(name="id", data_type=DataType.INTEGER),
    ...         SchemaField(name="email", data_type=DataType.STRING),
    ...     )
    ... )
    >>>
    >>> expectation = expect_column_to_exist("email")
    >>> result = expectation.validate(schema)
    >>> assert result.is_valid
"""

from __future__ import annotations

from typing import Any

from vibe_piper.types import DataType, Expectation, ValidationResult

# =============================================================================
# Column Existence Expectations
# =============================================================================


def expect_column_to_exist(column: str) -> Expectation:
    """
    Expect a column to exist in the schema.

    Args:
        column: The column name to check for

    Example:
        >>> expectation = expect_column_to_exist("email")
        >>> result = expectation.validate(schema)
    """

    def validate(schema: Any) -> ValidationResult:
        if hasattr(schema, "has_field"):
            if schema.has_field(column):
                return ValidationResult(is_valid=True)
            return ValidationResult(
                is_valid=False,
                errors=(f"Column '{column}' does not exist in schema",),
            )
        return ValidationResult(
            is_valid=False,
            errors=("Expected a Schema object with has_field method",),
        )

    return Expectation(
        name=f"expect_column_to_exist({column})",
        fn=validate,
        description=f"Column '{column}' should exist in the schema",
        severity="error",
    )


def expect_column_to_not_exist(column: str) -> Expectation:
    """
    Expect a column to NOT exist in the schema.

    Args:
        column: The column name that should not exist

    Example:
        >>> expectation = expect_column_to_not_exist("password")
        >>> result = expectation.validate(schema)
    """

    def validate(schema: Any) -> ValidationResult:
        if hasattr(schema, "has_field"):
            if not schema.has_field(column):
                return ValidationResult(is_valid=True)
            return ValidationResult(
                is_valid=False,
                errors=(f"Column '{column}' should not exist in schema",),
            )
        return ValidationResult(
            is_valid=False,
            errors=("Expected a Schema object with has_field method",),
        )

    return Expectation(
        name=f"expect_column_to_not_exist({column})",
        fn=validate,
        description=f"Column '{column}' should not exist in the schema",
        severity="error",
    )


# =============================================================================
# Column Type Expectations
# =============================================================================


def expect_column_type_to_be(column: str, expected_type: DataType) -> Expectation:
    """
    Expect a column to have a specific data type.

    Args:
        column: The column name to check
        expected_type: The expected DataType

    Example:
        >>> from vibe_piper.types import DataType
        >>> expectation = expect_column_type_to_be("id", DataType.INTEGER)
        >>> result = expectation.validate(schema)
    """

    def validate(schema: Any) -> ValidationResult:
        if not hasattr(schema, "get_field"):
            return ValidationResult(
                is_valid=False,
                errors=("Expected a Schema object with get_field method",),
            )

        field = schema.get_field(column)
        if field is None:
            return ValidationResult(
                is_valid=False,
                errors=(f"Column '{column}' does not exist",),
            )

        if field.data_type == expected_type:
            return ValidationResult(is_valid=True)

        return ValidationResult(
            is_valid=False,
            errors=(
                f"Column '{column}' has type {field.data_type.name}, "
                f"expected {expected_type.name}",
            ),
        )

    return Expectation(
        name=f"expect_column_type_to_be({column}, {expected_type.name})",
        fn=validate,
        description=f"Column '{column}' should have type {expected_type.name}",
        severity="error",
    )


# =============================================================================
# Column Count Expectations
# =============================================================================


def expect_table_column_count_to_equal(count: int) -> Expectation:
    """
    Expect the schema to have a specific number of columns.

    Args:
        count: The expected number of columns

    Example:
        >>> expectation = expect_table_column_count_to_equal(3)
        >>> result = expectation.validate(schema)
    """

    def validate(schema: Any) -> ValidationResult:
        if hasattr(schema, "fields"):
            actual_count = len(schema.fields)
            if actual_count == count:
                return ValidationResult(is_valid=True)
            return ValidationResult(
                is_valid=False,
                errors=(f"Expected {count} columns, but found {actual_count}",),
            )
        return ValidationResult(
            is_valid=False,
            errors=("Expected a Schema object with fields attribute",),
        )

    return Expectation(
        name=f"expect_table_column_count_to_equal({count})",
        fn=validate,
        description=f"Schema should have exactly {count} columns",
        severity="error",
    )


def expect_table_column_count_to_be_between(
    min_count: int, max_count: int
) -> Expectation:
    """
    Expect the schema to have a column count within a range.

    Args:
        min_count: Minimum expected number of columns (inclusive)
        max_count: Maximum expected number of columns (inclusive)

    Example:
        >>> expectation = expect_table_column_count_to_be_between(2, 5)
        >>> result = expectation.validate(schema)
    """

    def validate(schema: Any) -> ValidationResult:
        if hasattr(schema, "fields"):
            actual_count = len(schema.fields)
            if min_count <= actual_count <= max_count:
                return ValidationResult(is_valid=True)
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Expected column count between {min_count} and {max_count}, "
                    f"but found {actual_count}",
                ),
            )
        return ValidationResult(
            is_valid=False,
            errors=("Expected a Schema object with fields attribute",),
        )

    return Expectation(
        name=f"expect_table_column_count_to_be_between({min_count}, {max_count})",
        fn=validate,
        description=f"Schema should have between {min_count} and {max_count} columns",
        severity="error",
    )


# =============================================================================
# Column Set Expectations
# =============================================================================


def expect_table_columns_to_match_set(column_set: set[str]) -> Expectation:
    """
    Expect the schema to have exactly the specified columns.

    Args:
        column_set: Set of expected column names

    Example:
        >>> expectation = expect_table_columns_to_match_set({"id", "name", "email"})
        >>> result = expectation.validate(schema)
    """

    def validate(schema: Any) -> ValidationResult:
        if not hasattr(schema, "fields"):
            return ValidationResult(
                is_valid=False,
                errors=("Expected a Schema object with fields attribute",),
            )

        actual_columns = {field.name for field in schema.fields}
        if actual_columns == column_set:
            return ValidationResult(is_valid=True)

        missing = column_set - actual_columns
        unexpected = actual_columns - column_set
        errors = []
        if missing:
            errors.append(f"Missing columns: {sorted(missing)}")
        if unexpected:
            errors.append(f"Unexpected columns: {sorted(unexpected)}")

        return ValidationResult(
            is_valid=False,
            errors=tuple(errors),
        )

    return Expectation(
        name=f"expect_table_columns_to_match_set({sorted(column_set)})",
        fn=validate,
        description=f"Schema should have exactly columns: {sorted(column_set)}",
        severity="error",
    )


def expect_table_columns_to_contain(column_set: set[str]) -> Expectation:
    """
    Expect the schema to contain at least the specified columns.

    Args:
        column_set: Set of required column names

    Example:
        >>> expectation = expect_table_columns_to_contain({"id", "email"})
        >>> result = expectation.validate(schema)
    """

    def validate(schema: Any) -> ValidationResult:
        if not hasattr(schema, "fields"):
            return ValidationResult(
                is_valid=False,
                errors=("Expected a Schema object with fields attribute",),
            )

        actual_columns = {field.name for field in schema.fields}
        missing = column_set - actual_columns

        if not missing:
            return ValidationResult(is_valid=True)

        return ValidationResult(
            is_valid=False,
            errors=(f"Missing required columns: {sorted(missing)}",),
        )

    return Expectation(
        name=f"expect_table_columns_to_contain({sorted(column_set)})",
        fn=validate,
        description=f"Schema should contain columns: {sorted(column_set)}",
        severity="error",
    )


def expect_table_columns_to_not_contain(column_set: set[str]) -> Expectation:
    """
    Expect the schema to NOT contain any of the specified columns.

    Args:
        column_set: Set of column names that should not exist

    Example:
        >>> expectation = expect_table_columns_to_not_contain({"password", "ssn"})
        >>> result = expectation.validate(schema)
    """

    def validate(schema: Any) -> ValidationResult:
        if not hasattr(schema, "fields"):
            return ValidationResult(
                is_valid=False,
                errors=("Expected a Schema object with fields attribute",),
            )

        actual_columns = {field.name for field in schema.fields}
        found = column_set & actual_columns

        if not found:
            return ValidationResult(is_valid=True)

        return ValidationResult(
            is_valid=False,
            errors=(f"Found forbidden columns: {sorted(found)}",),
        )

    return Expectation(
        name=f"expect_table_columns_to_not_contain({sorted(column_set)})",
        fn=validate,
        description=f"Schema should not contain columns: {sorted(column_set)}",
        severity="error",
    )


# =============================================================================
# Column Property Expectations
# =============================================================================


def expect_column_to_be_required(column: str) -> Expectation:
    """
    Expect a column to be required (not optional).

    Args:
        column: The column name to check

    Example:
        >>> expectation = expect_column_to_be_required("id")
        >>> result = expectation.validate(schema)
    """

    def validate(schema: Any) -> ValidationResult:
        if not hasattr(schema, "get_field"):
            return ValidationResult(
                is_valid=False,
                errors=("Expected a Schema object with get_field method",),
            )

        field = schema.get_field(column)
        if field is None:
            return ValidationResult(
                is_valid=False,
                errors=(f"Column '{column}' does not exist",),
            )

        if field.required:
            return ValidationResult(is_valid=True)

        return ValidationResult(
            is_valid=False,
            errors=(f"Column '{column}' is optional, expected required",),
        )

    return Expectation(
        name=f"expect_column_to_be_required({column})",
        fn=validate,
        description=f"Column '{column}' should be required",
        severity="error",
    )


def expect_column_to_be_optional(column: str) -> Expectation:
    """
    Expect a column to be optional (not required).

    Args:
        column: The column name to check

    Example:
        >>> expectation = expect_column_to_be_optional("middle_name")
        >>> result = expectation.validate(schema)
    """

    def validate(schema: Any) -> ValidationResult:
        if not hasattr(schema, "get_field"):
            return ValidationResult(
                is_valid=False,
                errors=("Expected a Schema object with get_field method",),
            )

        field = schema.get_field(column)
        if field is None:
            return ValidationResult(
                is_valid=False,
                errors=(f"Column '{column}' does not exist",),
            )

        if not field.required:
            return ValidationResult(is_valid=True)

        return ValidationResult(
            is_valid=False,
            errors=(f"Column '{column}' is required, expected optional",),
        )

    return Expectation(
        name=f"expect_column_to_be_optional({column})",
        fn=validate,
        description=f"Column '{column}' should be optional",
        severity="error",
    )


def expect_column_to_be_nullable(column: str) -> Expectation:
    """
    Expect a column to allow null values.

    Args:
        column: The column name to check

    Example:
        >>> expectation = expect_column_to_be_nullable("phone")
        >>> result = expectation.validate(schema)
    """

    def validate(schema: Any) -> ValidationResult:
        if not hasattr(schema, "get_field"):
            return ValidationResult(
                is_valid=False,
                errors=("Expected a Schema object with get_field method",),
            )

        field = schema.get_field(column)
        if field is None:
            return ValidationResult(
                is_valid=False,
                errors=(f"Column '{column}' does not exist",),
            )

        if field.nullable:
            return ValidationResult(is_valid=True)

        return ValidationResult(
            is_valid=False,
            errors=(f"Column '{column}' does not allow null values",),
        )

    return Expectation(
        name=f"expect_column_to_be_nullable({column})",
        fn=validate,
        description=f"Column '{column}' should allow null values",
        severity="error",
    )


def expect_column_to_be_non_nullable(column: str) -> Expectation:
    """
    Expect a column to NOT allow null values.

    Args:
        column: The column name to check

    Example:
        >>> expectation = expect_column_to_be_non_nullable("id")
        >>> result = expectation.validate(schema)
    """

    def validate(schema: Any) -> ValidationResult:
        if not hasattr(schema, "get_field"):
            return ValidationResult(
                is_valid=False,
                errors=("Expected a Schema object with get_field method",),
            )

        field = schema.get_field(column)
        if field is None:
            return ValidationResult(
                is_valid=False,
                errors=(f"Column '{column}' does not exist",),
            )

        if not field.nullable:
            return ValidationResult(is_valid=True)

        return ValidationResult(
            is_valid=False,
            errors=(f"Column '{column}' allows null values, expected non-nullable",),
        )

    return Expectation(
        name=f"expect_column_to_be_non_nullable({column})",
        fn=validate,
        description=f"Column '{column}' should not allow null values",
        severity="error",
    )


# =============================================================================
# Constraint Expectations
# =============================================================================


def expect_column_to_have_constraint(column: str, constraint_key: str) -> Expectation:
    """
    Expect a column to have a specific constraint.

    Args:
        column: The column name to check
        constraint_key: The constraint key to look for (e.g., "min_length", "max_value")

    Example:
        >>> expectation = expect_column_to_have_constraint("email", "max_length")
        >>> result = expectation.validate(schema)
    """

    def validate(schema: Any) -> ValidationResult:
        if not hasattr(schema, "get_field"):
            return ValidationResult(
                is_valid=False,
                errors=("Expected a Schema object with get_field method",),
            )

        field = schema.get_field(column)
        if field is None:
            return ValidationResult(
                is_valid=False,
                errors=(f"Column '{column}' does not exist",),
            )

        if constraint_key in field.constraints:
            return ValidationResult(is_valid=True)

        return ValidationResult(
            is_valid=False,
            errors=(f"Column '{column}' does not have constraint '{constraint_key}'",),
        )

    return Expectation(
        name=f"expect_column_to_have_constraint({column}, {constraint_key})",
        fn=validate,
        description=f"Column '{column}' should have constraint '{constraint_key}'",
        severity="error",
    )


def expect_column_constraint_to_equal(
    column: str, constraint_key: str, expected_value: Any
) -> Expectation:
    """
    Expect a column constraint to have a specific value.

    Args:
        column: The column name to check
        constraint_key: The constraint key to check
        expected_value: The expected constraint value

    Example:
        >>> expectation = expect_column_constraint_to_equal("email", "max_length", 255)
        >>> result = expectation.validate(schema)
    """

    def validate(schema: Any) -> ValidationResult:
        if not hasattr(schema, "get_field"):
            return ValidationResult(
                is_valid=False,
                errors=("Expected a Schema object with get_field method",),
            )

        field = schema.get_field(column)
        if field is None:
            return ValidationResult(
                is_valid=False,
                errors=(f"Column '{column}' does not exist",),
            )

        if constraint_key not in field.constraints:
            return ValidationResult(
                is_valid=False,
                errors=(
                    f"Column '{column}' does not have constraint '{constraint_key}'",
                ),
            )

        actual_value = field.constraints[constraint_key]
        if actual_value == expected_value:
            return ValidationResult(is_valid=True)

        return ValidationResult(
            is_valid=False,
            errors=(
                f"Column '{column}' constraint '{constraint_key}' has value {actual_value}, "
                f"expected {expected_value}",
            ),
        )

    return Expectation(
        name=f"expect_column_constraint_to_equal({column}, {constraint_key}, {expected_value})",
        fn=validate,
        description=(
            f"Column '{column}' constraint '{constraint_key}' should equal {expected_value}"
        ),
        severity="error",
    )


# =============================================================================
# Re-exports
# =============================================================================


__all__ = [
    # Column existence
    "expect_column_to_exist",
    "expect_column_to_not_exist",
    # Column type
    "expect_column_type_to_be",
    # Column count
    "expect_table_column_count_to_equal",
    "expect_table_column_count_to_be_between",
    # Column set
    "expect_table_columns_to_match_set",
    "expect_table_columns_to_contain",
    "expect_table_columns_to_not_contain",
    # Column properties
    "expect_column_to_be_required",
    "expect_column_to_be_optional",
    "expect_column_to_be_nullable",
    "expect_column_to_be_non_nullable",
    # Constraints
    "expect_column_to_have_constraint",
    "expect_column_constraint_to_equal",
]
