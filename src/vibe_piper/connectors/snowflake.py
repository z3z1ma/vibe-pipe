"""
Snowflake Database Connector

Provides connectivity to Snowflake data warehouse.
"""

from contextlib import contextmanager
from typing import Any, cast

import snowflake.connector
from snowflake.connector import errors

from vibe_piper.connectors.base import ConnectionConfig, DatabaseConnector, QueryResult


class SnowflakeConfig(ConnectionConfig):
    """Snowflake-specific connection configuration."""

    account: str
    warehouse: str
    schema: str = "PUBLIC"
    role: str | None = None
    region: str | None = None
    autocommit: bool = False


class SnowflakeConnector(DatabaseConnector):
    """
    Snowflake database connector.

    Example:
        config = SnowflakeConfig(
            account="xy12345.us-east-1",
            host="xy12345.us-east-1.snowflakecomputing.com",
            port=443,
            database="mydb",
            warehouse="compute_wh",
            schema="public",
            user="user",
            password="password"
        )
        connector = SnowflakeConnector(config)
        with connector:
            result = connector.query("SELECT * FROM users")
    """

    def __init__(self, config: SnowflakeConfig) -> None:
        """
        Initialize Snowflake connector.

        Args:
            config: Snowflake connection configuration
        """
        super().__init__(config)
        self._connection: Any = None

    def connect(self) -> None:
        """
        Establish connection to Snowflake.

        Raises:
            ConnectionError: If connection fails
        """
        try:
            connection_params = {
                "account": getattr(self.config, "account", ""),
                "user": self.config.user,
                "password": self.config.password,
                "database": self.config.database,
                "warehouse": getattr(self.config, "warehouse", ""),
                "schema": getattr(self.config, "schema", "PUBLIC"),
                "autocommit": getattr(self.config, "autocommit", False),
            }

            # Optional parameters
            if hasattr(self.config, "role") and self.config.role:
                connection_params["role"] = self.config.role

            self._connection = snowflake.connector.connect(**connection_params)
            self._is_connected = True
        except errors.Error as e:
            raise ConnectionError(f"Failed to connect to Snowflake: {e}") from e

    def disconnect(self) -> None:
        """Close the Snowflake connection and cleanup resources."""
        if self._connection:
            self._connection.close()
            self._connection = None
        self._is_connected = False

    def query(self, query: str, params: dict[str, Any] | None = None) -> QueryResult:
        """
        Execute a SELECT query and return results.

        Args:
            query: SQL query string
            params: Optional query parameters (using %s style placeholders)

        Returns:
            QueryResult containing rows and metadata

        Raises:
            RuntimeError: If not connected
            Exception: For database errors
        """
        if not self._is_connected or not self._connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        cursor = None
        try:
            cursor = self._connection.cursor()
            cursor.execute(query, params or {})
            rows = cursor.fetchall()

            # Get column names from description
            columns = []
            if cursor.description:
                columns = [col[0] for col in cursor.description]

            # Convert rows to dictionaries
            row_dicts = []
            for row in rows:
                row_dict = {}
                for i, col_name in enumerate(columns):
                    row_dict[col_name] = row[i]
                row_dicts.append(row_dict)

            return QueryResult(
                rows=row_dicts,
                row_count=len(row_dicts),
                columns=columns,
                query=query,
            )
        except errors.Error as e:
            raise Exception(f"Query failed: {e}") from e
        finally:
            if cursor:
                cursor.close()

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
        if not self._is_connected or not self._connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        cursor = None
        try:
            cursor = self._connection.cursor()
            cursor.execute(query, params or {})
            self._connection.commit()
            # Snowflake returns rowcount in cursor
            return cast(int, cursor.rowcount)
        except errors.Error as e:
            self._connection.rollback()
            raise Exception(f"Execute failed: {e}") from e
        finally:
            if cursor:
                cursor.close()

    @contextmanager
    def transaction(self):
        """
        Context manager for transaction handling.

        Yields:
            Transaction context (Snowflake cursor)

        Example:
            with connector.transaction() as cur:
                cur.execute("INSERT INTO users ...")
                cur.execute("UPDATE stats ...")
        """
        if not self._is_connected or not self._connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        cursor = self._connection.cursor()
        try:
            yield cursor
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise
        finally:
            cursor.close()

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

        if not self._is_connected or not self._connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        cursor = None
        try:
            cursor = self._connection.cursor()
            affected_rows = 0
            for params in params_list:
                cursor.execute(query, params)
                affected_rows += cursor.rowcount
            self._connection.commit()
            return affected_rows
        except errors.Error as e:
            self._connection.rollback()
            raise Exception(f"Batch execute failed: {e}") from e
        finally:
            if cursor:
                cursor.close()

    def put_file(self, stage_name: str, file_path: str, parallel: int = 4) -> None:
        """
        Upload a file to a Snowflake stage.

        Args:
            stage_name: Name of the stage (e.g., '@mystage')
            file_path: Local path to file
            parallel: Number of parallel threads for upload

        Raises:
            RuntimeError: If not connected
            Exception: For upload errors
        """
        if not self._is_connected or not self._connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        cursor = None
        try:
            cursor = self._connection.cursor()
            cursor.execute(f"PUT file://{file_path} {stage_name} PARALLEL={parallel}")
            self._connection.commit()
        except errors.Error as e:
            raise Exception(f"PUT file failed: {e}") from e
        finally:
            if cursor:
                cursor.close()

    def copy_into_table(
        self,
        table: str,
        stage: str,
        file_pattern: str,
        file_format: str = "CSV",
        on_error: str = "continue",
    ) -> int:
        """
        Load data from a stage into a table using COPY INTO.

        Args:
            table: Target table name
            stage: Stage location (e.g., '@mystage')
            file_pattern: File pattern to load (e.g., 'data.csv')
            file_format: File format type (CSV, JSON, PARQUET, etc.)
            on_error: Error handling strategy

        Returns:
            Number of rows loaded

        Raises:
            RuntimeError: If not connected
            Exception: For database errors
        """
        query = f"""
            COPY INTO {table}
            FROM {stage}/{file_pattern}
            FILE_FORMAT = (TYPE = '{file_format}')
            ON_ERROR = '{on_error}'
        """

        result = self.execute(query)
        return result

    def get_query_status(self, query_id: str) -> dict[str, Any]:
        """
        Get status of an async query.

        Args:
            query_id: Query ID returned from async execution

        Returns:
            Dictionary with query status information

        Raises:
            RuntimeError: If not connected
            Exception: For database errors
        """
        if not self._is_connected or not self._connection:
            raise RuntimeError("Not connected to database. Call connect() first.")

        cursor = None
        try:
            cursor = self._connection.cursor()
            cursor.execute(f"SELECT * FROM TABLE(result_scan('{query_id}'))")
            result = cursor.fetchone()
            return result
        except errors.Error as e:
            raise Exception(f"Failed to get query status: {e}") from e
        finally:
            if cursor:
                cursor.close()
