"""Integration test framework for vibe_piper."""

import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from vibe_piper.types import PipelineContext

# =============================================================================
# Database Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def test_db_url() -> str:
    """
    Get the test database URL.

    Uses in-memory SQLite by default. Override this fixture
    in your conftest.py to use a different database.

    Returns:
        Database connection URL string
    """
    return "sqlite:///:memory:"


@pytest.fixture(scope="function")
def test_db_connection(test_db_url: str) -> Generator[Any, None, None]:
    """
    Create a test database connection.

    This fixture sets up a test database and yields a connection object.
    The database is cleaned up after the test.

    Args:
        test_db_url: The database URL to connect to

    Yields:
        Database connection object
    """
    # For SQLite, we can use the built-in sqlite3 module
    if test_db_url.startswith("sqlite://"):
        import sqlite3

        # Extract database path from URL
        if test_db_url == "sqlite:///:memory:":
            # In-memory database
            conn = sqlite3.connect(":memory:")
        else:
            # File-based database
            db_path = test_db_url.replace("sqlite:///", "")
            conn = sqlite3.connect(db_path)

        yield conn

        # Cleanup
        conn.close()
    else:
        # For other databases, you would need to install the appropriate driver
        # and implement connection logic here
        pytest.skip(f"Database URL not supported in tests: {test_db_url}")


@pytest.fixture(scope="function")
def test_db_tables(test_db_connection: Any) -> Any:
    """
    Create test database tables.

    This fixture creates common test tables in the database.
    Override this fixture to create custom tables for your tests.

    Args:
        test_db_connection: Database connection fixture

    Yields:
        The database connection with tables created
    """
    conn = test_db_connection
    cursor = conn.cursor()

    # Create test tables
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS test_users (
            user_id INTEGER PRIMARY KEY,
            username TEXT NOT NULL,
            email TEXT NOT NULL,
            age INTEGER,
            is_active BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS test_products (
            product_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL NOT NULL,
            quantity INTEGER DEFAULT 0,
            in_stock BOOLEAN DEFAULT 1
        )
    """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS test_events (
            event_id TEXT PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            event_type TEXT NOT NULL,
            severity TEXT,
            payload TEXT
        )
    """
    )

    conn.commit()

    yield conn

    # Cleanup - drop tables
    cursor.execute("DROP TABLE IF EXISTS test_users")
    cursor.execute("DROP TABLE IF EXISTS test_products")
    cursor.execute("DROP TABLE IF EXISTS test_events")
    conn.commit()


@pytest.fixture(scope="function")
def populated_test_db(test_db_tables: Any) -> Any:
    """
    Create a test database populated with sample data.

    Args:
        test_db_tables: Database with tables created

    Yields:
        The database connection with sample data
    """
    conn = test_db_tables
    cursor = conn.cursor()

    # Insert sample users
    cursor.execute(
        """
        INSERT INTO test_users (user_id, username, email, age, is_active)
        VALUES (1, 'alice', 'alice@example.com', 30, 1)
    """
    )
    cursor.execute(
        """
        INSERT INTO test_users (user_id, username, email, age, is_active)
        VALUES (2, 'bob', 'bob@example.com', 25, 1)
    """
    )
    cursor.execute(
        """
        INSERT INTO test_users (user_id, username, email, age, is_active)
        VALUES (3, 'charlie', 'charlie@example.com', 35, 0)
    """
    )

    # Insert sample products
    cursor.execute(
        """
        INSERT INTO test_products (product_id, name, price, quantity, in_stock)
        VALUES (1, 'Widget A', 19.99, 100, 1)
    """
    )
    cursor.execute(
        """
        INSERT INTO test_products (product_id, name, price, quantity, in_stock)
        VALUES (2, 'Widget B', 29.99, 50, 1)
    """
    )
    cursor.execute(
        """
        INSERT INTO test_products (product_id, name, price, quantity, in_stock)
        VALUES (3, 'Widget C', 39.99, 0, 0)
    """
    )

    conn.commit()

    yield conn

    # Cleanup - delete all data
    cursor.execute("DELETE FROM test_users")
    cursor.execute("DELETE FROM test_products")
    cursor.execute("DELETE FROM test_events")
    conn.commit()


# =============================================================================
# File System Fixtures
# =============================================================================


@pytest.fixture(scope="function")
def test_data_dir() -> Generator[Path, None, None]:
    """
    Create a temporary directory for test data files.

    Yields:
        Path to temporary directory
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture(scope="function")
def test_csv_file(test_data_dir: Path) -> Path:
    """
    Create a test CSV file with sample data.

    Args:
        test_data_dir: Temporary directory fixture

    Returns:
        Path to the created CSV file
    """
    import csv

    csv_path = test_data_dir / "test_data.csv"

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "value"])
        writer.writeheader()
        writer.writerow({"id": 1, "name": "Alice", "value": 100})
        writer.writerow({"id": 2, "name": "Bob", "value": 200})
        writer.writerow({"id": 3, "name": "Charlie", "value": 300})

    return csv_path


@pytest.fixture(scope="function")
def test_json_file(test_data_dir: Path) -> Path:
    """
    Create a test JSON file with sample data.

    Args:
        test_data_dir: Temporary directory fixture

    Returns:
        Path to the created JSON file
    """
    import json

    json_path = test_data_dir / "test_data.json"

    data = [
        {"id": 1, "name": "Alice", "value": 100},
        {"id": 2, "name": "Bob", "value": 200},
        {"id": 3, "name": "Charlie", "value": 300},
    ]

    with open(json_path, "w") as f:
        json.dump(data, f)

    return json_path


@pytest.fixture(scope="function")
def test_parquet_file(test_data_dir: Path) -> Path:
    """
    Create a test Parquet file with sample data.

    Note: Requires pyarrow or fastparquet to be installed.

    Args:
        test_data_dir: Temporary directory fixture

    Returns:
        Path to the created Parquet file
    """
    try:
        import pandas as pd
    except ImportError:
        pytest.skip("pandas not installed, required for parquet fixture")

    parquet_path = test_data_dir / "test_data.parquet"

    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [100, 200, 300],
        }
    )

    df.to_parquet(parquet_path)

    return parquet_path


# =============================================================================
# Pipeline Context Fixtures for Integration Tests
# =============================================================================


@pytest.fixture(scope="function")
def integration_pipeline_context(
    test_db_url: str, test_data_dir: Path
) -> PipelineContext:
    """
    Create a pipeline context configured for integration testing.

    Args:
        test_db_url: Test database URL
        test_data_dir: Temporary data directory

    Returns:
        PipelineContext with integration test configuration
    """
    return PipelineContext(
        pipeline_id="integration_test",
        run_id="test_run",
        config={
            "env": "test",
            "database_url": test_db_url,
            "data_dir": str(test_data_dir),
        },
        metadata={"test_type": "integration"},
    )


# =============================================================================
# API Test Fixtures
# =============================================================================


@pytest.fixture(scope="function")
def mock_api_server():
    """
    Create a mock API server for testing API assets.

    This fixture uses pytest-httpx or similar to mock HTTP requests.
    You'll need to install the appropriate mocking library.

    Yields:
        Mock server object
    """
    try:
        from unittest.mock import Mock, patch

        # Create a simple mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ]
        }

        with patch("requests.get", return_value=mock_response) as mock_get:
            yield mock_get

    except ImportError:
        pytest.skip("Mocking library not available")


# =============================================================================
# Helper Functions
# =============================================================================


def create_test_table(
    conn: Any,
    table_name: str,
    columns: dict[str, str],
) -> None:
    """
    Create a test table in the database.

    Args:
        conn: Database connection
        table_name: Name of the table to create
        columns: Dictionary of column names to types
    """
    column_defs = ", ".join([f"{name} {type_}" for name, type_ in columns.items()])
    cursor = conn.cursor()
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({column_defs})")
    conn.commit()


def insert_test_data(
    conn: Any,
    table_name: str,
    data: list[dict[str, Any]],
) -> None:
    """
    Insert test data into a table.

    Args:
        conn: Database connection
        table_name: Name of the table
        data: List of row dictionaries
    """
    if not data:
        return

    columns = list(data[0].keys())
    placeholders = ", ".join(["?" for _ in columns])
    column_names = ", ".join(columns)

    sql = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"

    cursor = conn.cursor()
    for row in data:
        values = [row[col] for col in columns]
        cursor.execute(sql, values)

    conn.commit()
