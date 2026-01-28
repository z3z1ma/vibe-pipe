"""
Unit tests for base connector functionality.
"""

import pytest

from vibe_piper.connectors.base import ConnectionConfig, QueryBuilder


class TestConnectionConfig:
    """Test ConnectionConfig dataclass."""

    def test_default_config(self):
        """Test creating default connection config."""
        config = ConnectionConfig(
            host="localhost",
            port=5432,
            database="testdb",
            user="testuser",
            password="testpass",
        )

        assert config.host == "localhost"
        assert config.port == 5432
        assert config.database == "testdb"
        assert config.user == "testuser"
        assert config.password == "testpass"
        assert config.pool_size == 10  # Default
        assert config.max_overflow == 20  # Default

    def test_config_with_pool_settings(self):
        """Test connection config with custom pool settings."""
        config = ConnectionConfig(
            host="localhost",
            port=5432,
            database="testdb",
            user="testuser",
            password="testpass",
            pool_size=20,
            max_overflow=40,
            pool_timeout=60,
        )

        assert config.pool_size == 20
        assert config.max_overflow == 40
        assert config.pool_timeout == 60


class TestQueryBuilder:
    """Test QueryBuilder functionality."""

    def test_basic_select(self):
        """Test building a basic SELECT query."""
        builder = QueryBuilder("users")
        query, params = builder.build_select()

        assert query == "SELECT * FROM users"
        assert params == {}

    def test_select_columns(self):
        """Test SELECT with specific columns."""
        builder = QueryBuilder("users")
        query, params = builder.select("id", "name", "email").build_select()

        assert query == "SELECT id, name, email FROM users"
        assert params == {}

    def test_select_with_where(self):
        """Test SELECT with WHERE clause."""
        builder = QueryBuilder("users")
        query, params = builder.where(
            "status = :status", status="active"
        ).build_select()

        assert query == "SELECT * FROM users WHERE status = :status"
        assert params == {"status": "active"}

    def test_select_with_multiple_where(self):
        """Test SELECT with multiple WHERE clauses."""
        builder = QueryBuilder("users")
        query, params = (
            builder.where("status = :status", status="active")
            .where("age > :age", age=18)
            .build_select()
        )

        assert query == "SELECT * FROM users WHERE status = :status AND age > :age"
        assert params == {"status": "active", "age": 18}

    def test_select_with_order_by(self):
        """Test SELECT with ORDER BY clause."""
        builder = QueryBuilder("users")
        query, params = builder.order_by("created_at DESC", "name ASC").build_select()

        assert query == "SELECT * FROM users ORDER BY created_at DESC, name ASC"
        assert params == {}

    def test_select_with_limit(self):
        """Test SELECT with LIMIT clause."""
        builder = QueryBuilder("users")
        query, params = builder.limit(10).build_select()

        assert query == "SELECT * FROM users LIMIT 10"
        assert params == {}

    def test_select_with_limit_and_offset(self):
        """Test SELECT with LIMIT and OFFSET."""
        builder = QueryBuilder("users")
        query, params = builder.limit(10).offset(20).build_select()

        assert query == "SELECT * FROM users LIMIT 10 OFFSET 20"
        assert params == {}

    def test_complex_query(self):
        """Test building a complex query."""
        builder = QueryBuilder("users")
        query, params = (
            builder.select("id", "name", "email")
            .where("status = :status", status="active")
            .where("age > :age", age=18)
            .order_by("created_at DESC")
            .limit(10)
        ).build_select()

        expected = (
            "SELECT id, name, email FROM users WHERE status = :status "
            "AND age > :age ORDER BY created_at DESC LIMIT 10"
        )
        assert query == expected
        assert params == {"status": "active", "age": 18}

    def test_build_insert_single(self):
        """Test building INSERT query for single row."""
        query, params = QueryBuilder.build_insert("users", {"name": "John", "age": 30})

        assert query == "INSERT INTO users (name, age) VALUES (:name, :age)"
        assert params == {"name": "John", "age": 30}

    def test_build_insert_multiple(self):
        """Test building INSERT query for multiple rows (first row used for structure)."""
        data = [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]
        query, params = QueryBuilder.build_insert("users", data)

        assert query == "INSERT INTO users (name, age) VALUES (:name, :age)"
        assert params == {"name": "John", "age": 30}

    def test_build_insert_empty(self):
        """Test building INSERT query with empty data raises error."""
        with pytest.raises(ValueError, match="Cannot build INSERT query with no data"):
            QueryBuilder.build_insert("users", [])

    def test_build_update(self):
        """Test building UPDATE query."""
        query, params = QueryBuilder.build_update(
            "users", {"name": "John", "age": 31}, "id = :where_id", {"where_id": 1}
        )

        assert (
            query
            == "UPDATE users SET name = :update_name, age = :update_age WHERE id = :where_id"
        )
        assert params == {"update_name": "John", "update_age": 31, "where_id": 1}

    def test_build_update_no_data(self):
        """Test building UPDATE query with no data raises error."""
        with pytest.raises(ValueError, match="Cannot build UPDATE query with no data"):
            QueryBuilder.build_update("users", {}, "id = 1")

    def test_build_delete(self):
        """Test building DELETE query."""
        query, params = QueryBuilder.build_delete("users", "id = :id", {"id": 1})

        assert query == "DELETE FROM users WHERE id = :id"
        assert params == {"id": 1}

    def test_join(self):
        """Test building query with JOIN."""
        builder = QueryBuilder("users")
        query, params = builder.join(
            "orders", "users.id = orders.user_id"
        ).build_select()

        assert (
            query
            == "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"
        )
        assert params == {}

    def test_left_join(self):
        """Test building query with LEFT JOIN."""
        builder = QueryBuilder("users")
        query, params = builder.join(
            "orders", "users.id = orders.user_id", "LEFT"
        ).build_select()

        assert (
            query == "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id"
        )
        assert params == {}

    def test_group_by(self):
        """Test building query with GROUP BY."""
        builder = QueryBuilder("users")
        query, params = builder.group_by("department", "status").build_select()

        assert query == "SELECT * FROM users GROUP BY department, status"
        assert params == {}
