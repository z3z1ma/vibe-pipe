"""
MySQL Database Connector

Provides connectivity to MySQL databases with connection pooling support.
"""

from contextlib import contextmanager
from typing import Any, cast

from mysql.connector import errors, pooling

from vibe_piper.connectors.base import ConnectionConfig, DatabaseConnector, QueryResult


class MySQLConfig(ConnectionConfig):
    """MySQL-specific connection configuration."""

    charset: str = "utf8mb4"
    collation: str = "utf8mb4_unicode_ci"
    autocommit: bool = False
    connect_timeout: int = 10
    use_unicode: bool = True


class MySQLConnector(DatabaseConnector):
    """
    MySQL database connector with connection pooling.

    Example:
        config = MySQLConfig(
            host="localhost",
            port=3306,
            database="mydb",
            user="user",
            password="password",
            pool_size=10
        )
        connector = MySQLConnector(config)
        with connector:
            result = connector.query("SELECT * FROM users")
    """

    def __init__(self, config: MySQLConfig) -> None:
        """
        Initialize MySQL connector.

        Args:
            config: MySQL connection configuration
        """
        super().__init__(config)
        self._pool: pooling.MySQLConnectionPool | None = None
        self._current_connection: Any = None

    def connect(self) -> None:
        """
        Establish connection to MySQL and create connection pool.

        Raises:
            ConnectionError: If connection fails
        """
        try:
            db_config = {
                "host": self.config.host,
                "port": self.config.port,
                "database": self.config.database,
                "user": self.config.user,
                "password": self.config.password,
                "charset": getattr(self.config, "charset", "utf8mb4"),
                "collation": getattr(self.config, "collation", "utf8mb4_unicode_ci"),
                "autocommit": getattr(self.config, "autocommit", False),
                "connect_timeout": getattr(self.config, "connect_timeout", 10),
                "use_unicode": getattr(self.config, "use_unicode", True),
                "pool_name": "vibepiper_pool",
                "pool_size": self.config.pool_size,
            }

            self._pool = pooling.MySQLConnectionPool(**db_config)
            self._is_connected = True
        except errors.Error as e:
            raise ConnectionError(f"Failed to connect to MySQL: {e}") from e

    def disconnect(self) -> None:
        """Close all connections and cleanup resources."""
        # MySQL connector pool doesn't have explicit close method
        # Connections will be cleaned up when pool is destroyed
        self._pool = None
        self._is_connected = False
        self._current_connection = None

    def _get_connection(self) -> Any:
        """
        Get a connection from the pool.

        Returns:
            MySQL connection object

        Raises:
            RuntimeError: If not connected
        """
        if not self._is_connected or not self._pool:
            raise RuntimeError("Not connected to database. Call connect() first.")

        if not self._current_connection:
            self._current_connection = self._pool.get_connection()

        return self._current_connection

    def _release_connection(self) -> None:
        """Release the current connection back to the pool."""
        if self._current_connection:
            self._current_connection.close()
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
        cursor = None
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(query, params)
            rows = cursor.fetchall()
            columns = list(cursor.column_names) if cursor.description else []

            return QueryResult(
                rows=rows,
                row_count=len(rows),
                columns=columns,
                query=query,
            )
        except errors.Error as e:
            raise Exception(f"Query failed: {e}") from e
        finally:
            if cursor:
                cursor.close()
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
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            return cast(int, cursor.rowcount)
        except errors.Error as e:
            conn.rollback()
            raise Exception(f"Execute failed: {e}") from e
        finally:
            if cursor:
                cursor.close()
            self._release_connection()

    @contextmanager
    def transaction(self):
        """
        Context manager for transaction handling.

        Yields:
            Transaction context (MySQL connection)

        Example:
            with connector.transaction() as conn:
                cursor = conn.cursor()
                cursor.execute("INSERT INTO users ...")
                cursor.execute("UPDATE stats ...")
        """
        conn = self._get_connection()
        try:
            # Start transaction
            conn.start_transaction()
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._release_connection()

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
        cursor = None
        try:
            cursor = conn.cursor()
            affected_rows = 0
            for params in params_list:
                cursor.execute(query, params)
                affected_rows += cursor.rowcount
            conn.commit()
            return affected_rows
        except errors.Error as e:
            conn.rollback()
            raise Exception(f"Batch execute failed: {e}") from e
        finally:
            if cursor:
                cursor.close()
            self._release_connection()

    def execute_many(self, query: str, params_list: list[tuple[Any, ...]]) -> int:
        """
        Execute a query multiple times using executemany for better performance.

        Args:
            query: SQL query with parameter placeholders
            params_list: List of parameter tuples

        Returns:
            Total number of affected rows

        Raises:
            RuntimeError: If not connected
            Exception: For database errors
        """
        if not params_list:
            return 0

        conn = self._get_connection()
        cursor = None
        try:
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            conn.commit()
            return cast(int, cursor.rowcount)
        except errors.Error as e:
            conn.rollback()
            raise Exception(f"Execute many failed: {e}") from e
        finally:
            if cursor:
                cursor.close()
            self._release_connection()

    def load_data_local(
        self,
        table: str,
        data_file_path: str,
        fields: list[str] | None = None,
        lines_terminated_by: str = "\\n",
        fields_terminated_by: str = ",",
        ignore_lines: int = 0,
    ) -> int:
        """
        Load data from local file using MySQL LOAD DATA LOCAL INFILE.

        Args:
            table: Target table name
            data_file_path: Path to data file
            fields: List of field/column names
            lines_terminated_by: Line terminator
            fields_terminated_by: Field delimiter
            ignore_lines: Number of lines to ignore from start

        Returns:
            Number of rows loaded

        Raises:
            RuntimeError: If not connected
            Exception: For database errors
            FileNotFoundError: If file doesn't exist
        """
        import os

        if not os.path.exists(data_file_path):
            raise FileNotFoundError(f"Data file not found: {data_file_path}")

        conn = self._get_connection()
        cursor = None
        try:
            cursor = conn.cursor()

            field_str = f"({', '.join(fields)})" if fields else ""
            ignore_str = f"IGNORE {ignore_lines} LINES" if ignore_lines > 0 else ""

            query = f"""
                LOAD DATA LOCAL INFILE '{data_file_path}'
                INTO TABLE {table}
                FIELDS TERMINATED BY '{fields_terminated_by}'
                LINES TERMINATED BY '{lines_terminated_by}'
                {ignore_str}
                {field_str}
            """

            cursor.execute(query)
            conn.commit()
            return cast(int, cursor.rowcount)
        except errors.Error as e:
            conn.rollback()
            raise Exception(f"LOAD DATA failed: {e}") from e
        finally:
            if cursor:
                cursor.close()
            self._release_connection()
