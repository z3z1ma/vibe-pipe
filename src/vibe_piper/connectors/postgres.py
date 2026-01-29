"""
PostgreSQL Database Connector

Provides connectivity to PostgreSQL databases with connection pooling support.
"""

from contextlib import contextmanager
from typing import Any, cast

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

from vibe_piper.connectors.base import ConnectionConfig, DatabaseConnector, QueryResult


class PostgreSQLConfig(ConnectionConfig):
    """PostgreSQL-specific connection configuration."""

    sslmode: str | None = None
    connect_timeout: int = 10


class PostgreSQLConnector(DatabaseConnector):
    """
    PostgreSQL database connector with connection pooling.

    Example:
        config = PostgreSQLConfig(
            host="localhost",
            port=5432,
            database="mydb",
            user="user",
            password="password",
            pool_size=10
        )
        connector = PostgreSQLConnector(config)
        with connector:
            result = connector.query("SELECT * FROM users")
    """

    def __init__(self, config: PostgreSQLConfig) -> None:
        """
        Initialize PostgreSQL connector.

        Args:
            config: PostgreSQL connection configuration
        """
        super().__init__(config)
        self._pool: pool.ThreadedConnectionPool | None = None
        self._current_connection: Any = None

    def connect(self) -> None:
        """
        Establish connection to PostgreSQL and create connection pool.

        Raises:
            ConnectionError: If connection fails
        """
        try:
            self._pool = pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=self.config.pool_size + self.config.max_overflow,
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                sslmode=getattr(self.config, "sslmode", None),
                connect_timeout=getattr(self.config, "connect_timeout", 10),
            )
            self._is_connected = True
        except psycopg2.Error as e:
            raise ConnectionError(f"Failed to connect to PostgreSQL: {e}") from e

    def disconnect(self) -> None:
        """Close all connections in the pool and cleanup resources."""
        if self._pool:
            self._pool.closeall()
            self._pool = None
        self._is_connected = False
        self._current_connection = None

    def _get_connection(self) -> Any:
        """
        Get a connection from the pool.

        Returns:
            PostgreSQL connection object

        Raises:
            RuntimeError: If not connected
        """
        if not self._is_connected or not self._pool:
            raise RuntimeError("Not connected to database. Call connect() first.")

        if not self._current_connection:
            self._current_connection = self._pool.getconn()

        return self._current_connection

    def _release_connection(self) -> None:
        """Release the current connection back to the pool."""
        if self._current_connection and self._pool:
            self._pool.putconn(self._current_connection)
            self._current_connection = None

    def query(self, query: str, params: dict[str, Any] | None = None) -> QueryResult:
        """
        Execute a SELECT query and return results.

        Args:
            query: SQL query string
            params: Optional query parameters

        Returns:
            QueryResult containing rows and metadata

        Raises:
            RuntimeError: If not connected
            Exception: For database errors
        """
        conn = self._get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()
                columns = [desc.name for desc in cursor.description] if cursor.description else []

                return QueryResult(
                    rows=[dict(row) for row in rows],
                    row_count=len(rows),
                    columns=columns,
                    query=query,
                )
        except psycopg2.Error as e:
            raise Exception(f"Query failed: {e}") from e
        finally:
            self._release_connection()

    def execute(self, query: str, params: dict[str, Any] | None = None) -> int:
        """
        Execute a statement (INSERT, UPDATE, DELETE) and return affected row count.

        Args:
            query: SQL statement
            params: Optional query parameters

        Returns:
            Number of affected rows

        Raises:
            RuntimeError: If not connected
            Exception: For database errors
        """
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                conn.commit()
                return cast(int, cursor.rowcount)
        except psycopg2.Error as e:
            conn.rollback()
            raise Exception(f"Execute failed: {e}") from e
        finally:
            self._release_connection()

    @contextmanager
    def transaction(self):
        """
        Context manager for transaction handling.

        Yields:
            Transaction context

        Example:
            with connector.transaction():
                connector.execute("INSERT INTO users ...")
                connector.execute("UPDATE stats ...")
        """
        if not self._is_connected or not self._pool:
            raise RuntimeError("Not connected to database. Call connect() first.")

        conn = self._pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)

    def execute_batch(self, query: str, params_list: list[dict[str, Any]]) -> int:
        """
        Execute a query multiple times with different parameters.

        Args:
            query: SQL query with parameter placeholders
            params_list: List of parameter dictionaries

        Returns:
            Total number of affected rows

        Raises:
            RuntimeError: If not connected
            Exception: For database errors
        """
        if not params_list:
            return 0

        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                affected_rows = 0
                for params in params_list:
                    cursor.execute(query, params)
                    affected_rows += cursor.rowcount
                conn.commit()
                return affected_rows
        except psycopg2.Error as e:
            conn.rollback()
            raise Exception(f"Batch execute failed: {e}") from e
        finally:
            self._release_connection()

    def copy_from_csv(
        self,
        table: str,
        csv_file_path: str,
        columns: list[str] | None = None,
        delimiter: str = ",",
    ) -> int:
        """
        Bulk import data from CSV file using PostgreSQL COPY.

        Args:
            table: Target table name
            csv_file_path: Path to CSV file
            columns: List of column names (None for all columns)
            delimiter: CSV delimiter character

        Returns:
            Number of rows imported

        Raises:
            RuntimeError: If not connected
            Exception: For database errors
            FileNotFoundError: If CSV file doesn't exist
        """
        import os

        if not os.path.exists(csv_file_path):
            raise FileNotFoundError(f"CSV file not found: {csv_file_path}")

        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                with open(csv_file_path) as f:
                    column_str = f"({', '.join(columns)})" if columns else ""
                    cursor.copy_expert(
                        f"COPY {table}{column_str} FROM STDIN WITH DELIMITER '{delimiter}'",
                        f,
                    )
                conn.commit()
                # Note: COPY doesn't return row count directly
                # You would need to query afterward for exact count
                return -1  # Indicates success but unknown count
        except psycopg2.Error as e:
            conn.rollback()
            raise Exception(f"COPY from CSV failed: {e}") from e
        finally:
            self._release_connection()
