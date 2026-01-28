"""
Base Database Connector Protocol and Query Builder

Defines the interface for all database connectors and provides query building utilities.
"""

from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, TypeVar

from pydantic import BaseModel


@dataclass
class ConnectionConfig:
    """Base configuration for database connections."""

    host: str
    port: int
    database: str
    user: str
    password: str
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 30
    pool_recycle: int = 3600


@dataclass
class QueryResult:
    """Result of a database query."""

    rows: list[dict[str, Any]]
    row_count: int
    columns: list[str]
    query: str


T = TypeVar("T", bound=BaseModel)


class DatabaseConnector(ABC):
    """
    Abstract protocol for database connectors.

    All database connectors must implement this protocol to ensure
    consistent interface across different database backends.
    """

    def __init__(self, config: ConnectionConfig) -> None:
        """
        Initialize the database connector.

        Args:
            config: Connection configuration
        """
        self.config = config
        self._connection: Any = None
        self._is_connected = False

    @abstractmethod
    def connect(self) -> None:
        """
        Establish connection to the database.

        Raises:
            ConnectionError: If connection fails
        """
        ...

    @abstractmethod
    def disconnect(self) -> None:
        """Close the database connection and cleanup resources."""
        ...

    @abstractmethod
    def query(self, query: str, params: dict[str, Any] | None = None) -> QueryResult:
        """
        Execute a query and return results.

        Args:
            query: SQL query string
            params: Optional query parameters for parameterized queries

        Returns:
            QueryResult containing rows, metadata

        Raises:
            RuntimeError: If not connected
            Exception: For database-specific errors
        """
        ...

    @abstractmethod
    def execute(self, query: str, params: dict[str, Any] | None = None) -> int:
        """
        Execute a statement without returning results (INSERT, UPDATE, DELETE).

        Args:
            query: SQL statement
            params: Optional query parameters

        Returns:
            Number of affected rows

        Raises:
            RuntimeError: If not connected
            Exception: For database-specific errors
        """
        ...

    @abstractmethod
    @contextmanager
    def transaction(self):
        """
        Context manager for database transactions.

        Yields:
            Transaction context

        Example:
            with connector.transaction():
                connector.execute(...)
                connector.query(...)
        """
        ...

    def is_connected(self) -> bool:
        """Check if the connector is currently connected."""
        return self._is_connected

    def map_to_schema(self, result: QueryResult, schema: type[T]) -> list[T]:
        """
        Map query results to a Pydantic schema.

        Args:
            result: Query result from database
            schema: Pydantic model class

        Returns:
            List of validated schema instances

        Raises:
            ValidationError: If data doesn't match schema
        """
        return [schema.model_validate(row) for row in result.rows]

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()


class QueryBuilder:
    """
    Query builder for common SQL operations.

    Provides a fluent interface for building SQL queries programmatically
    while preventing SQL injection through parameterization.
    """

    def __init__(self, table: str) -> None:
        """
        Initialize query builder for a table.

        Args:
            table: Table name to query
        """
        self.table = table
        self._select_columns: list[str] = ["*"]
        self._where_clauses: list[str] = []
        self._params: dict[str, Any] = {}
        self._order_by: list[str] = []
        self._limit_value: int | None = None
        self._offset_value: int | None = None
        self._joins: list[str] = []
        self._group_by: list[str] = []

    def select(self, *columns: str) -> "QueryBuilder":
        """
        Specify columns to select.

        Args:
            *columns: Column names

        Returns:
            Self for chaining
        """
        if columns:
            self._select_columns = list(columns)
        return self

    def where(self, clause: str, **params: Any) -> "QueryBuilder":
        """
        Add a WHERE clause.

        Args:
            clause: WHERE clause (use named parameters like :name)
            **params: Parameter values

        Returns:
            Self for chaining

        Example:
            builder.where("status = :status", status="active")
        """
        self._where_clauses.append(clause)
        self._params.update(params)
        return self

    def join(self, table: str, on: str, join_type: str = "INNER") -> "QueryBuilder":
        """
        Add a JOIN clause.

        Args:
            table: Table to join
            on: JOIN condition
            join_type: Type of join (INNER, LEFT, RIGHT, FULL)

        Returns:
            Self for chaining
        """
        self._joins.append(f"{join_type} JOIN {table} ON {on}")
        return self

    def order_by(self, *columns: str) -> "QueryBuilder":
        """
        Add ORDER BY clause.

        Args:
            *columns: Column names (append DESC for descending)

        Returns:
            Self for chaining
        """
        self._order_by.extend(columns)
        return self

    def group_by(self, *columns: str) -> "QueryBuilder":
        """
        Add GROUP BY clause.

        Args:
            *columns: Column names to group by

        Returns:
            Self for chaining
        """
        self._group_by.extend(columns)
        return self

    def limit(self, count: int) -> "QueryBuilder":
        """
        Add LIMIT clause.

        Args:
            count: Maximum number of rows

        Returns:
            Self for chaining
        """
        self._limit_value = count
        return self

    def offset(self, count: int) -> "QueryBuilder":
        """
        Add OFFSET clause.

        Args:
            count: Number of rows to skip

        Returns:
            Self for chaining
        """
        self._offset_value = count
        return self

    def build_select(self) -> tuple[str, dict[str, Any]]:
        """
        Build the SELECT query.

        Returns:
            Tuple of (query_string, parameters)
        """
        query_parts: list[str] = []

        # SELECT clause
        select_clause = ", ".join(self._select_columns)
        query_parts.append(f"SELECT {select_clause}")

        # FROM clause
        query_parts.append(f"FROM {self.table}")

        # JOINs
        if self._joins:
            query_parts.extend(self._joins)

        # WHERE clause
        if self._where_clauses:
            where_clause = " AND ".join(self._where_clauses)
            query_parts.append(f"WHERE {where_clause}")

        # GROUP BY
        if self._group_by:
            query_parts.append(f"GROUP BY {', '.join(self._group_by)}")

        # ORDER BY
        if self._order_by:
            query_parts.append(f"ORDER BY {', '.join(self._order_by)}")

        # LIMIT and OFFSET
        if self._limit_value is not None:
            query_parts.append(f"LIMIT {self._limit_value}")
            if self._offset_value is not None:
                query_parts.append(f"OFFSET {self._offset_value}")

        query = " ".join(query_parts)
        return query, self._params

    @staticmethod
    def build_insert(
        table: str, data: dict[str, Any] | list[dict[str, Any]]
    ) -> tuple[str, dict[str, Any]]:
        """
        Build an INSERT query.

        Args:
            table: Table name
            data: Dictionary or list of dictionaries to insert

        Returns:
            Tuple of (query_string, parameters)
        """
        if isinstance(data, dict):
            data = [data]

        if not data:
            raise ValueError("Cannot build INSERT query with no data")

        columns = list(data[0].keys())
        placeholders = ", ".join([f":{col}" for col in columns])
        column_list = ", ".join(columns)

        query = f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})"
        params = data[0]

        return query, params

    @staticmethod
    def build_update(
        table: str,
        data: dict[str, Any],
        where_clause: str,
        where_params: dict[str, Any] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """
        Build an UPDATE query.

        Args:
            table: Table name
            data: Column values to update
            where_clause: WHERE clause
            where_params: Parameters for WHERE clause

        Returns:
            Tuple of (query_string, parameters)
        """
        if not data:
            raise ValueError("Cannot build UPDATE query with no data")

        set_clauses = [f"{col} = :update_{col}" for col in data]
        params = {f"update_{col}": val for col, val in data.items()}

        if where_params:
            params.update(where_params)

        query = f"UPDATE {table} SET {', '.join(set_clauses)} WHERE {where_clause}"
        return query, params

    @staticmethod
    def build_delete(
        table: str, where_clause: str, params: dict[str, Any]
    ) -> tuple[str, dict[str, Any]]:
        """
        Build a DELETE query.

        Args:
            table: Table name
            where_clause: WHERE clause
            params: Parameters for WHERE clause

        Returns:
            Tuple of (query_string, parameters)
        """
        query = f"DELETE FROM {table} WHERE {where_clause}"
        return query, params
