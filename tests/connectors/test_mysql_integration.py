"""
Integration tests for MySQL connector.

These tests require a running MySQL instance.
Run with: pytest tests/connectors/test_mysql_integration.py
"""

import os

import pytest

from vibe_piper.connectors import MySQLConfig, MySQLConnector


@pytest.mark.integration
class TestMySQLConnector:
    """Integration tests for MySQL connector."""

    @pytest.fixture(autouse=True)
    def setup_connector(self):
        """Setup MySQL connector for tests."""
        config = MySQLConfig(
            host=os.getenv("MYSQL_HOST", "localhost"),
            port=int(os.getenv("MYSQL_PORT", "3306")),
            database=os.getenv("MYSQL_DATABASE", "testdb"),
            user=os.getenv("MYSQL_USER", "testuser"),
            password=os.getenv("MYSQL_PASSWORD", "testpass"),
            pool_size=2,
        )

        self.connector = MySQLConnector(config)
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
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                age INT,
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
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(255),
                age INT
            )
        """
        )

        # Insert data
        insert_sql = (
            "INSERT INTO test_users (name, email, age) VALUES (%(name)s, %(email)s, %(age)s)"
        )
        affected = self.connector.execute(
            insert_sql, {"name": "John Doe", "email": "john@example.com", "age": 30}
        )
        assert affected == 1

        # Select data
        result = self.connector.query(
            "SELECT * FROM test_users WHERE name = %(name)s", {"name": "John Doe"}
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
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                price DECIMAL(10, 2)
            )
        """
        )

        # Batch insert
        insert_sql = "INSERT INTO test_products (name, price) VALUES (%(name)s, %(price)s)"
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

    def test_execute_many(self):
        """Test executemany for better performance."""
        # Create table
        self.connector.execute(
            """
            CREATE TABLE IF NOT EXISTS test_items (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                value INT
            )
        """
        )

        # Use executemany
        insert_sql = "INSERT INTO test_items (name, value) VALUES (%s, %s)"
        params_list = [("Item 1", 100), ("Item 2", 200), ("Item 3", 300)]
        affected = self.connector.execute_many(insert_sql, params_list)
        assert affected == 3

        # Verify
        result = self.connector.query("SELECT COUNT(*) as count FROM test_items")
        assert result.rows[0]["count"] == 3

        # Cleanup
        self.connector.execute("DROP TABLE test_items")

    def test_transaction_success(self):
        """Test successful transaction."""
        # Create table
        self.connector.execute(
            """
            CREATE TABLE IF NOT EXISTS test_accounts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                balance DECIMAL(10, 2)
            )
        """
        )

        # Execute transaction
        with self.connector.transaction() as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO test_accounts (balance) VALUES (100.00)")
            cursor.execute("INSERT INTO test_accounts (balance) VALUES (200.00)")

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
                id INT AUTO_INCREMENT PRIMARY KEY,
                balance DECIMAL(10, 2)
            )
        """
        )

        # Execute transaction that fails
        with pytest.raises(Exception):
            with self.connector.transaction() as conn:
                cursor = conn.cursor()
                cursor.execute("INSERT INTO test_accounts (balance) VALUES (100.00)")
                # This should fail
                cursor.execute("INVALID SQL")

        # Verify rollback occurred
        result = self.connector.query("SELECT COUNT(*) as count FROM test_accounts")
        assert result.rows[0]["count"] == 0

        # Cleanup
        self.connector.execute("DROP TABLE test_accounts")
