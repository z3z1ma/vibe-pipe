"""
Tests for built-in schema expectations.

This module tests the library of pre-built expectations for common schema validations.
"""

import pytest

from vibe_piper import DataType, Expectation, Schema, SchemaField
from vibe_piper.expectations import (
    expect_column_constraint_to_equal,
    expect_column_to_be_non_nullable,
    expect_column_to_be_nullable,
    expect_column_to_be_optional,
    expect_column_to_be_required,
    expect_column_to_exist,
    expect_column_to_have_constraint,
    expect_column_to_not_exist,
    expect_column_type_to_be,
    expect_table_column_count_to_be_between,
    expect_table_column_count_to_equal,
    expect_table_columns_to_contain,
    expect_table_columns_to_match_set,
    expect_table_columns_to_not_contain,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture()  # type: ignore[misc]
def sample_schema() -> Schema:
    """Create a sample schema for testing."""
    return Schema(
        name="users",
        fields=(
            SchemaField(
                name="id", data_type=DataType.INTEGER, required=True, nullable=False
            ),
            SchemaField(
                name="email",
                data_type=DataType.STRING,
                required=True,
                nullable=False,
                constraints={"max_length": 255},
            ),
            SchemaField(
                name="age", data_type=DataType.INTEGER, required=False, nullable=True
            ),
            SchemaField(
                name="name",
                data_type=DataType.STRING,
                required=True,
                nullable=True,
                constraints={"min_length": 1, "max_length": 100},
            ),
        ),
    )


# =============================================================================
# Column Existence Tests
# =============================================================================


class TestColumnExistenceExpectations:
    """Tests for column existence expectations."""

    def test_expect_column_to_exist_valid(self, sample_schema: Schema) -> None:
        """Test expecting an existing column."""
        expectation = expect_column_to_exist("email")
        assert isinstance(expectation, Expectation)
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_column_to_exist_invalid(self, sample_schema: Schema) -> None:
        """Test expecting a non-existent column."""
        expectation = expect_column_to_exist("address")
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "does not exist" in result.errors[0]

    def test_expect_column_to_not_exist_valid(self, sample_schema: Schema) -> None:
        """Test expecting a column to not exist."""
        expectation = expect_column_to_not_exist("password")
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_column_to_not_exist_invalid(self, sample_schema: Schema) -> None:
        """Test expecting an existing column to not exist."""
        expectation = expect_column_to_not_exist("email")
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "should not exist" in result.errors[0]


# =============================================================================
# Column Type Tests
# =============================================================================


class TestColumnTypeExpectations:
    """Tests for column type expectations."""

    def test_expect_column_type_to_be_valid(self, sample_schema: Schema) -> None:
        """Test expecting correct column type."""
        expectation = expect_column_type_to_be("id", DataType.INTEGER)
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_column_type_to_be_invalid(self, sample_schema: Schema) -> None:
        """Test expecting incorrect column type."""
        expectation = expect_column_type_to_be("id", DataType.STRING)
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "has type INTEGER" in result.errors[0]
        assert "expected STRING" in result.errors[0]

    def test_expect_column_type_to_be_nonexistent_column(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting type on non-existent column."""
        expectation = expect_column_type_to_be("missing", DataType.STRING)
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "does not exist" in result.errors[0]


# =============================================================================
# Column Count Tests
# =============================================================================


class TestColumnCountExpectations:
    """Tests for column count expectations."""

    def test_expect_table_column_count_to_equal_valid(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting correct column count."""
        expectation = expect_table_column_count_to_equal(4)
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_table_column_count_to_equal_invalid(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting incorrect column count."""
        expectation = expect_table_column_count_to_equal(5)
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "Expected 5 columns, but found 4" in result.errors[0]

    def test_expect_table_column_count_to_be_between_valid(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting column count within range."""
        expectation = expect_table_column_count_to_be_between(3, 5)
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_table_column_count_to_be_between_too_few(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting column count within range (too few)."""
        expectation = expect_table_column_count_to_be_between(5, 10)
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "between 5 and 10" in result.errors[0]
        assert "but found 4" in result.errors[0]

    def test_expect_table_column_count_to_be_between_too_many(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting column count within range (too many)."""
        expectation = expect_table_column_count_to_be_between(1, 3)
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "between 1 and 3" in result.errors[0]
        assert "but found 4" in result.errors[0]


# =============================================================================
# Column Set Tests
# =============================================================================


class TestColumnSetExpectations:
    """Tests for column set expectations."""

    def test_expect_table_columns_to_match_set_valid(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting exact column set match."""
        expectation = expect_table_columns_to_match_set({"id", "email", "age", "name"})
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_table_columns_to_match_set_missing(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting exact column set with missing columns."""
        expectation = expect_table_columns_to_match_set({"id", "email", "password"})
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert any("Missing columns" in err for err in result.errors)
        assert any("Unexpected columns" in err for err in result.errors)

    def test_expect_table_columns_to_contain_valid(self, sample_schema: Schema) -> None:
        """Test expecting schema to contain columns."""
        expectation = expect_table_columns_to_contain({"id", "email"})
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_table_columns_to_contain_invalid(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting schema to contain columns (missing)."""
        expectation = expect_table_columns_to_contain({"id", "password"})
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "Missing required columns" in result.errors[0]
        assert "password" in result.errors[0]

    def test_expect_table_columns_to_not_contain_valid(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting schema to not contain columns."""
        expectation = expect_table_columns_to_not_contain({"password", "ssn"})
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_table_columns_to_not_contain_invalid(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting schema to not contain columns (found)."""
        expectation = expect_table_columns_to_not_contain({"id", "password"})
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "Found forbidden columns" in result.errors[0]
        assert "id" in result.errors[0]


# =============================================================================
# Column Property Tests
# =============================================================================


class TestColumnPropertyExpectations:
    """Tests for column property expectations."""

    def test_expect_column_to_be_required_valid(self, sample_schema: Schema) -> None:
        """Test expecting a required column."""
        expectation = expect_column_to_be_required("id")
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_column_to_be_required_invalid(self, sample_schema: Schema) -> None:
        """Test expecting a required column (optional field)."""
        expectation = expect_column_to_be_required("age")
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "is optional, expected required" in result.errors[0]

    def test_expect_column_to_be_optional_valid(self, sample_schema: Schema) -> None:
        """Test expecting an optional column."""
        expectation = expect_column_to_be_optional("age")
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_column_to_be_optional_invalid(self, sample_schema: Schema) -> None:
        """Test expecting an optional column (required field)."""
        expectation = expect_column_to_be_optional("id")
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "is required, expected optional" in result.errors[0]

    def test_expect_column_to_be_nullable_valid(self, sample_schema: Schema) -> None:
        """Test expecting a nullable column."""
        expectation = expect_column_to_be_nullable("age")
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_column_to_be_nullable_invalid(self, sample_schema: Schema) -> None:
        """Test expecting a nullable column (non-nullable field)."""
        expectation = expect_column_to_be_nullable("id")
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "does not allow null values" in result.errors[0]

    def test_expect_column_to_be_non_nullable_valid(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting a non-nullable column."""
        expectation = expect_column_to_be_non_nullable("id")
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_column_to_be_non_nullable_invalid(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting a non-nullable column (nullable field)."""
        expectation = expect_column_to_be_non_nullable("age")
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "allows null values, expected non-nullable" in result.errors[0]


# =============================================================================
# Constraint Tests
# =============================================================================


class TestConstraintExpectations:
    """Tests for constraint expectations."""

    def test_expect_column_to_have_constraint_valid(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting a column to have a constraint."""
        expectation = expect_column_to_have_constraint("email", "max_length")
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_column_to_have_constraint_invalid(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting a column to have a constraint (missing)."""
        expectation = expect_column_to_have_constraint("id", "max_length")
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "does not have constraint 'max_length'" in result.errors[0]

    def test_expect_column_constraint_to_equal_valid(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting a constraint value to match."""
        expectation = expect_column_constraint_to_equal("email", "max_length", 255)
        result = expectation.validate(sample_schema)
        assert result.is_valid is True

    def test_expect_column_constraint_to_equal_invalid_value(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting a constraint value to match (incorrect)."""
        expectation = expect_column_constraint_to_equal("email", "max_length", 100)
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "has value 255" in result.errors[0]
        assert "expected 100" in result.errors[0]

    def test_expect_column_constraint_to_equal_missing_constraint(
        self, sample_schema: Schema
    ) -> None:
        """Test expecting a constraint value to match (constraint missing)."""
        expectation = expect_column_constraint_to_equal("id", "max_length", 255)
        result = expectation.validate(sample_schema)
        assert result.is_valid is False
        assert "does not have constraint 'max_length'" in result.errors[0]


# =============================================================================
# Integration Tests
# =============================================================================


class TestIntegrationExpectations:
    """Integration tests for expectations."""

    def test_multiple_expectations_composed(self, sample_schema: Schema) -> None:
        """Test using multiple expectations together."""
        expectations = [
            expect_column_to_exist("id"),
            expect_column_type_to_be("id", DataType.INTEGER),
            expect_column_to_be_required("id"),
            expect_column_to_be_non_nullable("id"),
        ]

        for expectation in expectations:
            result = expectation.validate(sample_schema)
            assert (
                result.is_valid is True
            ), f"{expectation.name} failed: {result.errors}"

    def test_expectation_with_declarative_schema(self) -> None:
        """Test expectations with declarative schema definitions."""
        from vibe_piper import Integer, String, define_schema

        @define_schema
        class UserSchema:
            id: Integer = Integer()
            email: String = String(max_length=255)

        schema = UserSchema.to_schema()  # type: ignore[attr-defined]

        # Test column existence
        expectation = expect_column_to_exist("email")
        result = expectation.validate(schema)
        assert result.is_valid

        # Test column type
        expectation = expect_column_type_to_be("id", DataType.INTEGER)
        result = expectation.validate(schema)
        assert result.is_valid

    def test_expectation_name_and_description(self) -> None:
        """Test that expectations have proper names and descriptions."""
        expectation = expect_column_to_exist("email")

        assert expectation.name == "expect_column_to_exist(email)"
        assert expectation.description is not None
        assert "email" in expectation.description
        assert "exist" in expectation.description
        assert expectation.severity == "error"

    def test_expectation_metadata(self) -> None:
        """Test that expectations can have metadata."""
        expectation = expect_column_to_exist("email")

        # Expectations should have metadata dict
        assert isinstance(expectation.metadata, dict)
        assert isinstance(expectation.config, dict)
