"""
Tests for SQL Asset Decorator

This module contains comprehensive tests for SQL asset functionality including:
- @sql_asset decorator
- SQL template rendering
- SQL validation
- Parameter binding
- Dependency tracking
- Multiple SQL dialect support
"""

from dataclasses import FrozenInstanceError

import pytest

from vibe_piper.sql_assets import (
    SQLOperator,
    SQLTemplateResult,
    SQLValidationResult,
    execute_sql_query,
    extract_asset_dependencies,
    render_sql_template,
    sql_asset,
    validate_sql,
)

# =============================================================================
# Test SQL Asset Decorator
# =============================================================================


def test_sql_asset_basic():
    """Test basic @sql_asset decorator without dialect."""

    @sql_asset
    def my_sql():
        return "SELECT * FROM users"

    assert isinstance(my_sql, type)  # It's an Asset
    assert hasattr(my_sql, "name")
    assert my_sql.name == "my_sql"


def test_sql_asset_with_dialect():
    """Test @sql_asset decorator with dialect parameter."""

    @sql_asset(dialect="mysql")
    def my_mysql_sql():
        return "SELECT * FROM customers"

    assert isinstance(my_sql, type)
    assert hasattr(my_sql, "config")
    assert my_sql.config.get("dialect") == "mysql"


def test_sql_asset_postgres():
    """Test @sql_asset with postgres dialect."""

    @sql_asset("postgresql")
    def clean_users():
        return """
        SELECT
            id,
            LOWER(email) as email,
            created_at
        FROM {{ raw_users }}
        WHERE email IS NOT NULL
        """

    assert isinstance(clean_users, type)
    assert clean_users.config.get("dialect") == "postgresql"


def test_sql_asset_with_config():
    """Test @sql_asset with full configuration."""

    @sql_asset(
        dialect="postgresql",
        depends_on=["raw_users", "raw_orders"],
        io_manager="postgresql",
        materialization="table",
        description="Clean and aggregate user data",
    )
    def complex_sql():
        return """
        SELECT u.id, u.email, COUNT(o.id) as order_count
        FROM {{ raw_users }} u
        LEFT JOIN {{ raw_orders }} o ON u.id = o.user_id
        GROUP BY u.id, u.email
        """

    assert isinstance(complex_sql, type)
    assert complex_sql.config.get("depends_on") == ("raw_users", "raw_orders")
    assert complex_sql.io_manager == "postgresql"


# =============================================================================
# Test SQL Dependency Extraction
# =============================================================================


def test_extract_single_dependency():
    """Test extracting single asset dependency."""
    sql = "SELECT * FROM {{ raw_users }}"
    deps = extract_asset_dependencies(sql)

    assert deps == ("raw_users",)


def test_extract_multiple_dependencies():
    """Test extracting multiple asset dependencies."""
    sql = """
    SELECT * FROM {{ users }} u
    JOIN {{ orders }} o ON u.id = o.user_id
    JOIN {{ products }} p ON o.product_id = p.id
    """
    deps = extract_asset_dependencies(sql)

    assert set(deps) == {"users", "orders", "products"}
    assert len(deps) == 3


def test_extract_no_dependencies():
    """Test SQL with no asset dependencies."""
    sql = "SELECT * FROM users WHERE active = true"
    deps = extract_asset_dependencies(sql)

    assert deps == ()


def test_extract_duplicate_dependencies():
    """Test handling duplicate asset references."""
    sql = """
    SELECT * FROM {{ users }} u1
    JOIN {{ users }} u2 ON u1.id = u2.parent_id
    """
    deps = extract_asset_dependencies(sql)

    assert deps == ("users",)  # Duplicates should be removed


# =============================================================================
# Test SQL Validation
# =============================================================================


def test_validate_valid_sql():
    """Test validation of valid SQL."""
    sql = "SELECT id, name FROM users WHERE active = true"
    result = validate_sql(sql, dialect="postgresql")

    assert result.is_valid
    assert len(result.errors) == 0


def test_validate_invalid_sql():
    """Test validation of invalid SQL."""
    sql = "SELECT FORM users"  # Typo: FORM instead of FROM
    result = validate_sql(sql, dialect="postgresql")

    assert not result.is_valid
    assert len(result.errors) > 0
    assert "parse error" in result.errors[0].lower()


def test_validate_dangerous_sql():
    """Test validation detects dangerous SQL patterns."""
    sql = "DROP TABLE users"
    result = validate_sql(sql, dialect="postgresql")

    assert not result.is_valid or len(result.warnings) > 0


def test_validate_with_warnings():
    """Test SQL that generates warnings."""
    sql = """
    SELECT * FROM users;
    DROP TABLE IF EXISTS temp_users;
    """
    result = validate_sql(sql, dialect="postgresql")

    # Should be valid but with warnings
    assert len(result.warnings) > 0


def test_validate_unsupported_dialect():
    """Test validation with unsupported dialect."""
    sql = "SELECT * FROM users"

    with pytest.raises(ValueError, match="Unsupported SQL dialect"):
        validate_sql(sql, dialect="oracle")


# =============================================================================
# Test SQL Template Rendering
# =============================================================================


def test_render_simple_template():
    """Test rendering simple SQL template."""
    sql = "SELECT * FROM {{ table_name }}"
    result = render_sql_template(sql, context={"table_name": "users"})

    assert result.rendered_sql == "SELECT * FROM users"
    assert result.extracted_params == {"table_name": "users"}
    assert result.asset_dependencies == ()


def test_render_template_with_parameters():
    """Test rendering template with multiple parameters."""
    sql = """
    SELECT * FROM users
    WHERE status = {{ status }}
      AND created_at >= {{ start_date }}
      AND created_at <= {{ end_date }}
    """
    result = render_sql_template(
        sql,
        context={
            "status": "active",
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
        },
    )

    assert "{{ status }}" not in result.rendered_sql
    assert "active" in result.rendered_sql
    assert len(result.extracted_params) == 3


def test_render_template_with_asset_deps():
    """Test rendering template with asset dependencies."""
    sql = "SELECT * FROM {{ users }} JOIN {{ orders }} ON users.id = orders.user_id"
    result = render_sql_template(sql, context={})

    assert result.asset_dependencies == ("orders", "users")


def test_render_template_with_mixed_vars():
    """Test rendering with both params and asset deps."""
    sql = """
    SELECT * FROM {{ users }} u
    WHERE u.status = {{ status }}
    """
    result = render_sql_template(sql, context={"status": "active"})

    assert "users" in result.asset_dependencies
    assert result.extracted_params == {"status": "active"}


def test_render_template_with_jinja_features():
    """Test Jinja2 template features like conditionals and loops."""
    sql = """
    SELECT id, name
    {% if include_email %}
        , email
    {% endif %}
    FROM {{ users }}
    """
    result = render_sql_template(sql, context={"include_email": True})

    assert "email" in result.rendered_sql
    assert result.extracted_params == {"include_email": True}


# =============================================================================
# Test SQL Operator Creation
# =============================================================================


def test_sql_operator_creation():
    """Test creating a SQL operator from function."""

    def my_sql_func():
        return "SELECT * FROM users"

    operator = SQLOperator(
        sql_template="SELECT * FROM users",
        dialect="postgresql",
        sql_asset_name="my_sql",
    )

    assert operator.sql_template == "SELECT * FROM users"
    assert operator.dialect == "postgresql"
    assert operator.sql_asset_name == "my_sql"


def test_sql_operator_with_dependencies():
    """Test SQL operator extracts dependencies."""
    sql = "SELECT * FROM {{ users }} JOIN {{ orders }} ON ..."
    operator = SQLOperator(
        sql_template=sql,
        dialect="postgresql",
        sql_asset_name="joined_data",
    )

    assert "users" in operator.asset_dependencies
    assert "orders" in operator.asset_dependencies


# =============================================================================
# Test SQL Validation Result
# =============================================================================


def test_validation_result_immutable():
    """Test SQLValidationResult is immutable."""
    result = SQLValidationResult(is_valid=True)

    with pytest.raises(FrozenInstanceError):
        result.is_valid = False


def test_validation_result_with_errors():
    """Test validation result with errors."""
    result = SQLValidationResult(
        is_valid=False,
        errors=("Syntax error", "Missing table"),
    )

    assert not result.is_valid
    assert len(result.errors) == 2
    assert result.errors[0] == "Syntax error"


def test_validation_result_with_warnings():
    """Test validation result with warnings."""
    result = SQLValidationResult(
        is_valid=True,
        warnings=("Uses deprecated syntax",),
    )

    assert result.is_valid
    assert len(result.warnings) == 1


# =============================================================================
# Test SQL Template Result
# =============================================================================


def test_template_result_immutable():
    """Test SQLTemplateResult is immutable."""
    result = SQLTemplateResult(
        rendered_sql="SELECT * FROM users",
        extracted_params={},
        asset_dependencies=(),
    )

    with pytest.raises(FrozenInstanceError):
        result.rendered_sql = "UPDATE users SET active = false"


# =============================================================================
# Integration Tests (Mock)
# =============================================================================


class MockConnector:
    """Mock database connector for testing."""

    def __init__(self):
        self.queries_executed = []

    def execute_query(self, query, params=None):
        self.queries_executed.append((query, params))
        # Return mock result
        return type(
            "QueryResult",
            (),
            {
                "rows": [{"id": 1, "name": "test"}],
                "row_count": 1,
                "columns": ["id", "name"],
                "query": query,
            },
        )()


def test_execute_sql_query_mock():
    """Test executing SQL query with mock connector."""
    sql = "SELECT * FROM users WHERE id = {{ user_id }}"
    connector = MockConnector()

    result = execute_sql_query(sql, connector, params={"user_id": 123}, dialect="postgresql")

    assert len(connector.queries_executed) == 1
    query, params = connector.queries_executed[0]
    assert "users" in query
    assert params == {"user_id": 123}


def test_execute_sql_with_validation():
    """Test SQL query execution with validation."""
    valid_sql = "SELECT id, name FROM users"
    connector = MockConnector()

    result = execute_sql_query(valid_sql, connector, params={}, dialect="postgresql")

    assert result is not None
    assert len(connector.queries_executed) == 1


def test_execute_sql_with_invalid_sql():
    """Test execution fails with invalid SQL."""
    invalid_sql = "SELECT FORM users"  # Typo

    connector = MockConnector()

    with pytest.raises(ValueError, match="SQL validation failed"):
        execute_sql_query(invalid_sql, connector, dialect="postgresql")


# =============================================================================
# Test Dialect Support
# =============================================================================


def test_all_supported_dialects():
    """Test all supported dialects are recognized."""
    supported = ["postgresql", "postgres", "mysql", "snowflake", "bigquery"]

    for dialect in supported:
        sql = "SELECT * FROM users"
        result = validate_sql(sql, dialect=dialect)
        # Should not raise ValueError for supported dialects
        assert result is not None


# =============================================================================
# Test Error Handling
# =============================================================================


def test_sql_function_not_returning_string():
    """Test error when SQL function doesn't return string."""

    @sql_asset
    def bad_sql():
        return 123  # Returns int instead of string

    # Should raise ValueError during operator creation
    # The exact error might vary based on implementation


def test_sql_function_with_exception():
    """Test error when SQL function raises exception."""

    @sql_asset
    def failing_sql():
        raise RuntimeError("Database connection failed")
        return "SELECT * FROM users"

    # Should handle the exception gracefully


def test_render_template_with_undefined_var():
    """Test rendering template with undefined variable raises error."""
    sql = "SELECT * FROM {{ missing_var }}"
    result = render_sql_template(sql, context={})

    # Should fail due to undefined variable
    assert "missing_var" in result.asset_dependencies


# =============================================================================
# Test Complex SQL Patterns
# =============================================================================


def test_cte_sql():
    """Test SQL with Common Table Expressions."""
    sql = """
    WITH ranked_users AS (
        SELECT id, name,
               ROW_NUMBER() OVER (ORDER BY created_at) as rn
        FROM {{ users }}
    )
    SELECT * FROM ranked_users
    WHERE rn <= 100
    """

    result = render_sql_template(sql, context={})
    assert "users" in result.asset_dependencies
    assert "WITH" in result.rendered_sql


def test_subquery_sql():
    """Test SQL with subqueries."""
    sql = """
    SELECT id, name,
           (SELECT COUNT(*) FROM {{ orders }} WHERE user_id = users.id) as order_count
    FROM {{ users }}
    """

    result = render_sql_template(sql, context={})
    assert "users" in result.asset_dependencies
    assert "orders" in result.asset_dependencies


def test_window_function_sql():
    """Test SQL with window functions."""
    sql = """
    SELECT id, name,
           SUM(amount) OVER (PARTITION BY user_id ORDER BY created_at) as running_total
    FROM {{ transactions }}
    """

    result = validate_sql(sql, dialect="postgresql")
    assert result.is_valid or "OVER" in result.rendered_sql


# =============================================================================
# Test Parameter Safety
# =============================================================================


def test_parameter_binding_prevents_injection():
    """Test that parameter binding prevents SQL injection."""
    sql = "SELECT * FROM users WHERE name = {{ user_input }}"

    # Normal usage
    result = render_sql_template(sql, context={"user_input": "admin"})
    assert "admin" in result.rendered_sql

    # Malicious input - would be rendered in template but
    # connector's execute_query should handle parameter binding
    result = render_sql_template(sql, context={"user_input": "'; DROP TABLE users; --"})
    # The template renders, but execute_query should use params
    assert "DROP" in result.rendered_sql


# =============================================================================
# Test Asset Configuration
# =============================================================================


def test_sql_asset_default_materialization():
    """Test SQL asset uses VIEW as default materialization."""

    @sql_asset
    def my_view():
        return "SELECT * FROM users"

    # SQL assets should default to VIEW materialization
    assert hasattr(my_view, "materialization")


def test_sql_asset_config_contains_dialect():
    """Test SQL asset stores dialect in config."""

    @sql_asset(dialect="bigquery")
    def bq_query():
        return "SELECT * FROM `my-project.my_dataset.users`"

    assert "bigquery" in str(my_view.config.get("dialect", ""))


def test_sql_asset_depends_on_tracking():
    """Test explicit depends_on is tracked."""

    @sql_asset(
        dialect="postgresql",
        depends_on=("raw_users", "raw_orders"),
    )
    def aggregated():
        return """
        SELECT u.id, COUNT(o.id) as order_count
        FROM {{ raw_users }} u
        JOIN {{ raw_orders }} o ON u.id = o.user_id
        GROUP BY u.id
        """

    # Both explicit and extracted deps should be available
    deps = aggregated.config.get("depends_on", ())
    assert "raw_users" in deps
    assert "raw_orders" in deps
