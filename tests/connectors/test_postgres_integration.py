"""
Integration tests for PostgreSQL connector.

These tests require a running PostgreSQL instance.
Run with: pytest tests/connectors/test_postgres_integration.py
"""

import os

import pytest

from vibe_piper.connectors import PostgreSQLConfig, PostgreSQLConnector


@pytest.mark.integration
class TestPostgreSQLConnector:
    """Integration tests for PostgreSQL connector."""

    @pytest.fixture(autouse=True)
    def setup_connector(self):
        """Setup PostgreSQL connector for tests."""
        # Get connection details from environment or use defaults
        config = PostgreSQLConfig(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DATABASE", "testdb"),
            user=os.getenv("POSTGRES_USER", "testuser"),
            password=os.getenv("POSTGRES_PASSWORD", "testpass"),
            pool_size=2,
        )

        self.connector = PostgreSQLConnector(config)
        self.connector.connect()

        yield

        self.connector.disconnect()

    def test_connection(self):
        """Test that connection is established."""
        assert self.connector.is_connected()

    def test_create_and_drop_table(self):
        """Test creating and dropping a table."""
        # Create table
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS test_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                age INTEGER,
                active BOOLEAN
            )
        """
        self.connector.execute(create_table_sql)

        # Drop table
        drop_table_sql = "DROP TABLE IF EXISTS test_table"
        affected_rows = self.connector.execute(drop_table_sql)

        # Should succeed without error
        assert affected_rows >= 0

    def test_insert_and_select(self):
        """Test inserting and selecting data."""
        # Create table
        self.connector.execute(
            """
            CREATE TABLE IF NOT EXISTS test_users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(255),
                age INTEGER
            )
        """
        )

        # Insert data
        insert_sql = "INSERT INTO test_users (name, email, age) VALUES (:name, :email, :age)"
        affected = self.connector.execute(
            insert_sql, {"name": "John Doe", "email": "john@example.com", "age": 30}
        )
        assert affected == 1

        # Select data
        result = self.connector.query(
            "SELECT * FROM test_users WHERE name = :name", {"name": "John Doe"}
        )

        assert result.row_count == 1
        assert len(result.rows) == 1
        assert result.rows[0]["name"] == "John Doe"
        assert result.rows[0]["email"] == "john@example.com"
        assert result.rows[0]["age"] == 30

        # Cleanup
        self.connector.execute("DROP TABLE test_users")

    def test_batch_insert(self):
        """Test batch insert operations."""
        # Create table
        self.connector.execute(
            """
            CREATE TABLE IF NOT EXISTS test_products (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                price DECIMAL(10, 2)
            )
        """
        )

        # Batch insert
        insert_sql = "INSERT INTO test_products (name, price) VALUES (:name, :price)"
        params_list = [
            {"name": "Product 1", "price": 10.99},
            {"name": "Product 2", "price": 20.99},
            {"name": "Product 3", "price": 30.99},
        ]
        affected = self.connector.execute_batch(insert_sql, params_list)
        assert affected == 3

        # Verify
        result = self.connector.query("SELECT COUNT(*) as count FROM test_products")
        assert result.rows[0]["count"] == 3

        # Cleanup
        self.connector.execute("DROP TABLE test_products")

    def test_transaction_success(self):
        """Test successful transaction."""
        # Create table
        self.connector.execute(
            """
            CREATE TABLE IF NOT EXISTS test_accounts (
                id SERIAL PRIMARY KEY,
                balance DECIMAL(10, 2)
            )
        """
        )

        # Execute transaction
        with self.connector.transaction():
            self.connector.execute("INSERT INTO test_accounts (balance) VALUES (100.00)")
            self.connector.execute("INSERT INTO test_accounts (balance) VALUES (200.00)")

        # Verify both inserts succeeded
        result = self.connector.query("SELECT COUNT(*) as count FROM test_accounts")
        assert result.rows[0]["count"] == 2

        # Cleanup
        self.connector.execute("DROP TABLE test_accounts")

    def test_transaction_rollback(self):
        """Test transaction rollback on error."""
        # Create table
        self.connector.execute(
            """
            CREATE TABLE IF NOT EXISTS test_accounts (
                id SERIAL PRIMARY KEY,
                balance DECIMAL(10, 2)
            )
        """
        )

        # Execute transaction that fails
        with pytest.raises(Exception):
            with self.connector.transaction():
                self.connector.execute("INSERT INTO test_accounts (balance) VALUES (100.00)")
                # This should fail
                self.connector.execute("INVALID SQL")

        # Verify rollback occurred
        result = self.connector.query("SELECT COUNT(*) as count FROM test_accounts")
        assert result.rows[0]["count"] == 0

        # Cleanup
        self.connector.execute("DROP TABLE test_accounts")

    def test_query_builder_integration(self):
        """Test using QueryBuilder with PostgreSQL."""
        from vibe_piper.connectors import QueryBuilder

        # Create table
        self.connector.execute(
            """
            CREATE TABLE IF NOT EXISTS test_employees (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                department VARCHAR(50),
                salary INTEGER,
                active BOOLEAN
            )
        """
        )

        # Insert test data
        self.connector.execute(
            "INSERT INTO test_employees (name, department, salary, active) VALUES "
            "('Alice', 'Engineering', 90000, true), "
            "('Bob', 'Sales', 70000, true), "
            "('Charlie', 'Engineering', 95000, false)"
        )

        # Use QueryBuilder
        builder = QueryBuilder("test_employees")
        query, params = (
            builder.select("name", "salary")
            .where("department = :dept", dept="Engineering")
            .where("active = :active", active=True)
            .order_by("salary DESC")
        ).build_select()

        result = self.connector.query(query, params)

        assert result.row_count == 1
        assert result.rows[0]["name"] == "Alice"
        assert result.rows[0]["salary"] == 90000

        # Cleanup
        self.connector.execute("DROP TABLE test_employees")

    def test_map_to_schema(self):
        """Test mapping query results to Pydantic schema."""
        from pydantic import BaseModel

        # Create table
        self.connector.execute(
            """
            CREATE TABLE IF NOT EXISTS test_customers (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(255)
            )
        """
        )

        # Insert data
        self.connector.execute(
            "INSERT INTO test_customers (name, email) VALUES "
            "('Customer 1', 'customer1@example.com'), "
            "('Customer 2', 'customer2@example.com')"
        )

        # Define schema
        class Customer(BaseModel):
            id: int
            name: str
            email: str

        # Query and map
        result = self.connector.query("SELECT * FROM test_customers ORDER BY id")
        customers = self.connector.map_to_schema(result, Customer)

        assert len(customers) == 2
        assert customers[0].name == "Customer 1"
        assert customers[1].email == "customer2@example.com"

        # Verify Pydantic validation
        assert isinstance(customers[0], Customer)
        assert customers[0].model_dump() == {
            "id": 1,
            "name": "Customer 1",
            "email": "customer1@example.com",
        }

        # Cleanup
        self.connector.execute("DROP TABLE test_customers")
