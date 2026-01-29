"""
Base Connector Protocols for Vibe Piper

This module provides base protocols for both database and file I/O connectors.
"""

from abc import ABC, abstractmethod
from collections.abc import Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, TypeVar, runtime_checkable

from pydantic import BaseModel

from vibe_piper.types import DataRecord, Schema

# =============================================================================
# Database Connector Protocol
# =============================================================================


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

    @abstractmethod
    def connect(self) -> Any:
        """Establish a database connection."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close the database connection."""
        pass

    @abstractmethod
    @contextmanager
    def get_connection(self):
        """Get a database connection context manager."""
        pass

    @abstractmethod
    def execute_query(self, query: str, params: dict[str, Any] | None = None) -> QueryResult:
        """Execute a SQL query and return results."""
        pass

    @abstractmethod
    def execute(self, command: str, params: dict[str, Any] | None = None) -> None:
        """Execute a SQL command (INSERT, UPDATE, DELETE, etc.)."""
        pass

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        pass

    @abstractmethod
    def get_table_schema(self, table_name: str) -> Schema:
        """Get the schema of a database table."""
        pass

    def test_connection(self) -> bool:
        """Test if the database connection is working."""
        try:
            with self.get_connection() as conn:
                return conn is not None
        except Exception:
            return False


class QueryBuilder:
    """
    Query builder for constructing SQL queries programmatically.

    Provides a fluent interface for building SELECT, INSERT, UPDATE,
    and DELETE queries without writing raw SQL.
    """

    def __init__(self, table: str) -> None:
        self._table = table
        self._select: list[str] = []
        self._where: list[str] = []
        self._params: dict[str, Any] = {}
        self._joins: list[tuple[str, str, str]] = []
        self._group_by: list[str] = []
        self._order_by: list[str] = []
        self._limit: int | None = None
        self._offset: int | None = None

    def select(self, *columns: str) -> "QueryBuilder":
        """Set columns to select."""
        self._select = list(columns)
        return self

    def where(self, condition: str, **params: Any) -> "QueryBuilder":
        """Add WHERE clause and bind parameters."""
        self._where.append(condition)
        self._params.update(params)
        return self

    def join(self, table: str, on: str, join_type: str = "INNER") -> "QueryBuilder":
        """Add JOIN clause."""
        self._joins.append((table, on, join_type))
        return self

    def group_by(self, *columns: str) -> "QueryBuilder":
        """Add GROUP BY clause."""
        self._group_by = list(columns)
        return self

    def order_by(self, *clauses: str) -> "QueryBuilder":
        """Add ORDER BY clause."""
        self._order_by.extend(clauses)
        return self

    def limit(self, count: int) -> "QueryBuilder":
        """Set LIMIT clause."""
        self._limit = count
        return self

    def offset(self, count: int) -> "QueryBuilder":
        """Set OFFSET clause."""
        self._offset = count
        return self

    def build_select(self) -> tuple[str, dict[str, Any]]:
        """Build a SELECT query and return (sql, params)."""
        select_cols = ", ".join(self._select) if self._select else "*"
        query_parts = [f"SELECT {select_cols}", f"FROM {self._table}"]

        for table, on, join_type in self._joins:
            query_parts.append(f"{join_type} JOIN {table} ON {on}")

        if self._where:
            query_parts.append(f"WHERE {' AND '.join(self._where)}")

        if self._group_by:
            query_parts.append(f"GROUP BY {', '.join(self._group_by)}")

        if self._order_by:
            query_parts.append(f"ORDER BY {', '.join(self._order_by)}")

        if self._limit is not None:
            query_parts.append(f"LIMIT {self._limit}")

        if self._offset is not None:
            query_parts.append(f"OFFSET {self._offset}")

        return " ".join(query_parts), dict(self._params)

    @staticmethod
    def build_insert(
        table: str, data: dict[str, Any] | list[dict[str, Any]]
    ) -> tuple[str, dict[str, Any]]:
        """Build an INSERT query for a single row and return (sql, params)."""
        if isinstance(data, list):
            if not data:
                msg = "Cannot build INSERT query with no data"
                raise ValueError(msg)
            row = data[0]
        else:
            row = data

        columns = list(row.keys())
        placeholders = [f":{c}" for c in columns]
        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        return query, dict(row)

    @staticmethod
    def build_update(
        table: str,
        data: dict[str, Any],
        where_clause: str,
        where_params: dict[str, Any] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Build an UPDATE query and return (sql, params)."""
        if not data:
            msg = "Cannot build UPDATE query with no data"
            raise ValueError(msg)

        set_parts: list[str] = []
        params: dict[str, Any] = {}
        for col, value in data.items():
            key = f"update_{col}"
            set_parts.append(f"{col} = :{key}")
            params[key] = value

        if where_params:
            params.update(where_params)

        query = f"UPDATE {table} SET {', '.join(set_parts)} WHERE {where_clause}"
        return query, params

    @staticmethod
    def build_delete(
        table: str, where_clause: str, where_params: dict[str, Any] | None = None
    ) -> tuple[str, dict[str, Any]]:
        """Build a DELETE query and return (sql, params)."""
        query = f"DELETE FROM {table} WHERE {where_clause}"
        return query, dict(where_params or {})


# =============================================================================
# File I/O Connector Protocol
# =============================================================================


@runtime_checkable
class FileReader(Protocol):
    """
    Protocol for reading data from files.

    All file readers must implement this protocol to ensure a consistent
    interface across different file formats.
    """

    path: str | Path
    """Path to the file to read."""

    def read(
        self,
        schema: Schema | None = None,
        chunk_size: int | None = None,
        **kwargs: Any,
    ) -> Sequence[DataRecord] | "FileReaderIterator":
        """
        Read data from the file.

        Args:
            schema: Optional schema to validate data against.
                   If None, schema will be inferred from the file.
            chunk_size: If provided, returns an iterator that yields
                       chunks of records. Otherwise, returns all records.
            **kwargs: Format-specific options (e.g., delimiter for CSV).

        Returns:
            Either a sequence of DataRecord objects or an iterator
            that yields chunks of records.

        Raises:
            FileNotFoundError: If the file doesn't exist.
            ValueError: If the file format is invalid.
            SchemaError: If data doesn't match the provided schema.
        """
        ...

    def infer_schema(self, **kwargs: Any) -> Schema:
        """
        Infer the schema from the file.

        Args:
            **kwargs: Format-specific options.

        Returns:
            The inferred schema.

        Raises:
            FileNotFoundError: If the file doesn't exist.
            ValueError: If the schema cannot be inferred.
        """
        ...

    def get_row_count(self, **kwargs: Any) -> int:
        """
        Get the number of rows in the file.

        Args:
            **kwargs: Format-specific options.

        Returns:
            Number of rows in the file.
        """
        ...


@runtime_checkable
class FileWriter(Protocol):
    """
    Protocol for writing data to files.

    All file writers must implement this protocol to ensure a consistent
    interface across different file formats.
    """

    path: str | Path
    """Path to the file to write."""

    def write(
        self,
        data: Sequence[DataRecord],
        schema: Schema | None = None,
        **kwargs: Any,
    ) -> int:
        """
        Write data to the file.

        Args:
            data: Sequence of DataRecord objects to write.
            schema: Optional schema for the output file.
                   If None, schema is inferred from the first record.
            **kwargs: Format-specific options (e.g., compression for Parquet).

        Returns:
            Number of records written.

        Raises:
            ValueError: If the data is invalid or cannot be written.
            IOError: If the file cannot be written.
        """
        ...

    def append(
        self,
        data: Sequence[DataRecord],
        schema: Schema | None = None,
        **kwargs: Any,
    ) -> int:
        """
        Append data to an existing file.

        Args:
            data: Sequence of DataRecord objects to append.
            schema: Optional schema for the data.
            **kwargs: Format-specific options.

        Returns:
            Number of records appended.

        Raises:
            FileNotFoundError: If the file doesn't exist.
            ValueError: If the data cannot be appended (format doesn't support it).
            IOError: If the file cannot be written.
        """
        ...


class FileReaderIterator:
    """
    Iterator for reading large files in chunks.

    This iterator yields sequences of DataRecord objects, allowing
    memory-efficient processing of files that don't fit in memory.
    """

    def __init__(
        self,
        reader: FileReader,
        chunk_size: int,
        schema: Schema | None = None,
        **kwargs: Any,
    ):
        """
        Initialize the iterator.

        Args:
            reader: The file reader to use.
            chunk_size: Number of records per chunk.
            schema: Optional schema for validation.
            **kwargs: Additional reader options.
        """
        self.reader = reader
        self.chunk_size = chunk_size
        self.schema = schema
        self.kwargs = kwargs

    def __iter__(self) -> "FileReaderIterator":
        """Return the iterator object."""
        return self

    def __next__(self) -> Sequence[DataRecord]:
        """
        Get the next chunk of records.

        Returns:
            A sequence of DataRecord objects.

        Raises:
            StopIteration: When there are no more records.
        """
        # Implementation is format-specific
        raise NotImplementedError
