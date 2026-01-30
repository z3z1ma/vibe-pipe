"""
Tests for DDL Generator

Tests the DDLGenerator class for creating DDL statements from Schema objects
across multiple database dialects.
"""

import pytest

from vibe_piper.sinks.ddl_generator import DDLGenerator, Dialect
from vibe_piper.types import DataType, Schema, SchemaField


class TestDDLGenerator:
    """Tests for DDLGenerator class."""

    def test_create_table_postgresql(self) -> None:
        """Test CREATE TABLE generation for PostgreSQL."""
        schema = Schema(
            name="users",
            fields=(
                SchemaField(name="id", data_type=DataType.INTEGER, required=True),
                SchemaField(name="name", data_type=DataType.STRING, nullable=True),
                SchemaField(name="email", data_type=DataType.STRING, required=False),
                SchemaField(name="active", data_type=DataType.BOOLEAN),
            ),
        )

        generator = DDLGenerator(Dialect.POSTGRESQL)
        create_sql = generator.generate_create_table("users", schema)

        assert "CREATE TABLE users" in create_sql
        assert "id INTEGER NOT NULL" in create_sql
        assert "name VARCHAR" in create_sql
        assert "email VARCHAR" in create_sql
        assert "active BOOLEAN" in create_sql

    def test_create_table_mysql(self) -> None:
        """Test CREATE TABLE generation for MySQL."""
        schema = Schema(
            name="users",
            fields=(
                SchemaField(name="id", data_type=DataType.INTEGER, required=True),
                SchemaField(name="name", data_type=DataType.STRING),
            ),
        )

        generator = DDLGenerator(Dialect.MYSQL)
        create_sql = generator.generate_create_table("users", schema)

        assert "CREATE TABLE users" in create_sql
        assert "id INT NOT NULL" in create_sql
        assert "name VARCHAR" in create_sql

    def test_create_table_snowflake(self) -> None:
        """Test CREATE TABLE generation for Snowflake."""
        schema = Schema(
            name="users",
            fields=(
                SchemaField(name="id", data_type=DataType.INTEGER, required=True),
                SchemaField(name="name", data_type=DataType.STRING),
                SchemaField(name="created_at", data_type=DataType.DATETIME),
            ),
        )

        generator = DDLGenerator(Dialect.SNOWFLAKE)
        create_sql = generator.generate_create_table("users", schema)

        assert "CREATE TABLE users" in create_sql
        assert "id NUMBER(38,0) NOT NULL" in create_sql
        assert "name VARCHAR" in create_sql
        assert "created_at TIMESTAMP_NTZ" in create_sql

    def test_create_table_bigquery(self) -> None:
        """Test CREATE TABLE generation for BigQuery."""
        schema = Schema(
            name="users",
            fields=(
                SchemaField(name="id", data_type=DataType.INTEGER, required=True),
                SchemaField(name="name", data_type=DataType.STRING),
            ),
        )

        generator = DDLGenerator(Dialect.BIGQUERY)
        create_sql = generator.generate_create_table("users", schema)

        assert "CREATE TABLE users" in create_sql
        assert "id INT64 NOT NULL" in create_sql
        assert "name STRING" in create_sql

    def test_create_table_with_constraints(self) -> None:
        """Test CREATE TABLE with column constraints."""
        schema = Schema(
            name="users",
            fields=(
                SchemaField(
                    name="id",
                    data_type=DataType.INTEGER,
                    required=True,
                    constraints={"unique": True},
                ),
                SchemaField(
                    name="score",
                    data_type=DataType.FLOAT,
                    nullable=True,
                    constraints={"default": 0.0},
                ),
            ),
        )

        generator = DDLGenerator(Dialect.POSTGRESQL)
        create_sql = generator.generate_create_table("users", schema)

        assert "id INTEGER NOT NULL UNIQUE" in create_sql
        # Check for default value (with double precision for FLOAT)
        assert "DEFAULT 0.0" in create_sql

    def test_create_table_with_schema_name(self) -> None:
        """Test CREATE TABLE with schema name qualification."""
        schema = Schema(
            name="users",
            fields=(SchemaField(name="id", data_type=DataType.INTEGER, required=True),),
        )

        generator = DDLGenerator(Dialect.POSTGRESQL)
        create_sql = generator.generate_create_table("users", schema, schema_name="public")

        assert "CREATE TABLE public.users" in create_sql

    def test_create_table_empty_schema_raises_error(self) -> None:
        """Test that CREATE TABLE with empty schema raises error."""
        schema = Schema(name="users", fields=())

        generator = DDLGenerator(Dialect.POSTGRESQL)

        with pytest.raises(ValueError, match="schema has no fields"):
            generator.generate_create_table("users", schema)

    def test_upsert_postgresql(self) -> None:
        """Test UPSERT generation for PostgreSQL."""
        schema = Schema(
            name="users",
            fields=(
                SchemaField(name="id", data_type=DataType.INTEGER, required=True),
                SchemaField(name="name", data_type=DataType.STRING),
                SchemaField(name="email", data_type=DataType.STRING),
            ),
        )

        generator = DDLGenerator(Dialect.POSTGRESQL)
        upsert_sql = generator.generate_upsert("users", schema, "id")

        assert "INSERT INTO users" in upsert_sql
        assert "ON CONFLICT (id)" in upsert_sql
        assert "DO UPDATE SET" in upsert_sql
        assert "name = EXCLUDED.name" in upsert_sql
        assert "email = EXCLUDED.email" in upsert_sql

    def test_upsert_mysql(self) -> None:
        """Test UPSERT generation for MySQL."""
        schema = Schema(
            name="users",
            fields=(
                SchemaField(name="id", data_type=DataType.INTEGER, required=True),
                SchemaField(name="name", data_type=DataType.STRING),
            ),
        )

        generator = DDLGenerator(Dialect.MYSQL)
        upsert_sql = generator.generate_upsert("users", schema, "id")

        assert "INSERT INTO users" in upsert_sql
        assert "ON DUPLICATE KEY UPDATE" in upsert_sql
        assert "name = VALUES(name)" in upsert_sql

    def test_upsert_snowflake(self) -> None:
        """Test UPSERT generation for Snowflake."""
        schema = Schema(
            name="users",
            fields=(
                SchemaField(name="id", data_type=DataType.INTEGER, required=True),
                SchemaField(name="name", data_type=DataType.STRING),
            ),
        )

        generator = DDLGenerator(Dialect.SNOWFLAKE)
        upsert_sql = generator.generate_upsert("users", schema, "id")

        assert "MERGE INTO users" in upsert_sql
        assert "ON (target.id = source.id)" in upsert_sql
        assert "WHEN MATCHED THEN UPDATE" in upsert_sql
        assert "WHEN NOT MATCHED THEN INSERT" in upsert_sql

    def test_upsert_multiple_keys(self) -> None:
        """Test UPSERT generation with multiple key columns."""
        schema = Schema(
            name="users",
            fields=(
                SchemaField(name="user_id", data_type=DataType.INTEGER, required=True),
                SchemaField(name="account_id", data_type=DataType.INTEGER, required=True),
                SchemaField(name="name", data_type=DataType.STRING),
            ),
        )

        generator = DDLGenerator(Dialect.POSTGRESQL)
        upsert_sql = generator.generate_upsert("users", schema, ["user_id", "account_id"])

        assert "ON CONFLICT (user_id, account_id)" in upsert_sql

    def test_upsert_invalid_key_raises_error(self) -> None:
        """Test that UPSERT with invalid key raises error."""
        schema = Schema(
            name="users",
            fields=(SchemaField(name="id", data_type=DataType.INTEGER, required=True),),
        )

        generator = DDLGenerator(Dialect.POSTGRESQL)

        with pytest.raises(ValueError, match="Upsert key"):
            generator.generate_upsert("users", schema, "invalid_key")

    def test_drop_table(self) -> None:
        """Test DROP TABLE generation."""
        generator = DDLGenerator(Dialect.POSTGRESQL)
        drop_sql = generator.generate_drop_table("users")

        assert "DROP TABLE IF EXISTS users" in drop_sql
        assert drop_sql.endswith(";")

    def test_drop_table_with_cascade(self) -> None:
        """Test DROP TABLE with CASCADE."""
        generator = DDLGenerator(Dialect.POSTGRESQL)
        drop_sql = generator.generate_drop_table("users", cascade=True)

        assert "DROP TABLE IF EXISTS users CASCADE" in drop_sql

    def test_drop_table_with_schema(self) -> None:
        """Test DROP TABLE with schema name."""
        generator = DDLGenerator(Dialect.POSTGRESQL)
        drop_sql = generator.generate_drop_table("users", schema_name="public")

        assert "DROP TABLE IF EXISTS public.users" in drop_sql

    def test_create_index(self) -> None:
        """Test CREATE INDEX generation."""
        generator = DDLGenerator(Dialect.POSTGRESQL)
        index_sql = generator.generate_index("users", "idx_users_id", ["id"])

        assert "CREATE INDEX idx_users_id" in index_sql
        assert "ON users (id)" in index_sql

    def test_create_unique_index(self) -> None:
        """Test CREATE UNIQUE INDEX generation."""
        generator = DDLGenerator(Dialect.POSTGRESQL)
        index_sql = generator.generate_index("users", "idx_users_email", ["email"], unique=True)

        assert "CREATE UNIQUE INDEX idx_users_email" in index_sql

    def test_create_index_multiple_columns(self) -> None:
        """Test CREATE INDEX with multiple columns."""
        generator = DDLGenerator(Dialect.POSTGRESQL)
        index_sql = generator.generate_index("users", "idx_users_name_email", ["name", "email"])

        assert "CREATE INDEX idx_users_name_email" in index_sql
        assert "ON users (name, email)" in index_sql

    def test_create_index_with_schema(self) -> None:
        """Test CREATE INDEX with schema name."""
        generator = DDLGenerator(Dialect.POSTGRESQL)
        index_sql = generator.generate_index("users", "idx_users_id", ["id"], schema_name="public")

        assert "CREATE INDEX public.idx_users_id" in index_sql
        assert "ON public.users (id)" in index_sql
