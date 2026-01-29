"""
BigQuery Database Connector

Provides connectivity to Google BigQuery.
"""

from contextlib import contextmanager
from typing import Any, cast

from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery

from vibe_piper.connectors.base import ConnectionConfig, DatabaseConnector, QueryResult


class BigQueryConfig(ConnectionConfig):
    """BigQuery-specific connection configuration."""

    project_id: str
    location: str = "US"
    credentials_path: str | None = None
    dataset_id: str | None = None

    # Override unused fields
    port: int = 443  # BigQuery uses HTTPS, not traditional ports

    def __init__(
        self,
        project_id: str,
        host: str = "bigquery.googleapis.com",
        port: int = 443,
        database: str = "bigquery",  # noqa: ARG002 - Project ID acts as database
        user: str = "",  # Not used for BigQuery
        password: str = "",  # Uses credentials instead
        pool_size: int = 10,
        max_overflow: int = 20,
        pool_timeout: int = 30,
        pool_recycle: int = 3600,
        location: str = "US",
        credentials_path: str | None = None,
        dataset_id: str | None = None,
    ) -> None:
        """
        Initialize BigQuery configuration.

        Args:
            project_id: Google Cloud project ID
            host: BigQuery host (default: bigquery.googleapis.com)
            port: HTTPS port (default: 443)
            database: Not used, kept for compatibility
            user: Not used for BigQuery
            password: Not used for BigQuery
            pool_size: Connection pool size (for job clients)
            location: BigQuery dataset location
            credentials_path: Path to service account credentials JSON
            dataset_id: Default dataset ID
        """
        super().__init__(
            host=host,
            port=port,
            database=project_id,  # Store project_id in database field
            user=user,
            password=password,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle,
        )
        self.project_id = project_id
        self.location = location
        self.credentials_path = credentials_path
        self.dataset_id = dataset_id


class BigQueryConnector(DatabaseConnector):
    """
    BigQuery database connector.

    Example:
        config = BigQueryConfig(
            project_id="my-project",
            credentials_path="/path/to/credentials.json",
            location="US"
        )
        connector = BigQueryConnector(config)
        with connector:
            result = connector.query("SELECT * FROM dataset.users")
    """

    def __init__(self, config: BigQueryConfig) -> None:
        """
        Initialize BigQuery connector.

        Args:
            config: BigQuery connection configuration
        """
        super().__init__(config)
        self._client: bigquery.Client | None = None
        self._job_config: bigquery.QueryJobConfig | None = None

    def connect(self) -> None:
        """
        Establish connection to BigQuery.

        Raises:
            ConnectionError: If connection fails
        """
        try:
            client_kwargs = {"project": self.config.project_id}

            # Add credentials if provided
            if hasattr(self.config, "credentials_path") and self.config.credentials_path:
                from google.oauth2 import service_account

                credentials = service_account.Credentials.from_service_account_file(
                    self.config.credentials_path
                )
                client_kwargs["credentials"] = credentials

            self._client = bigquery.Client(**client_kwargs)
            self._is_connected = True
        except GoogleAPIError as e:
            raise ConnectionError(f"Failed to connect to BigQuery: {e}") from e
        except Exception as e:
            raise ConnectionError(f"Failed to connect to BigQuery: {e}") from e

    def disconnect(self) -> None:
        """Close the BigQuery client and cleanup resources."""
        self._client = None
        self._is_connected = False

    def query(self, query: str, params: dict[str, Any] | None = None) -> QueryResult:
        """
        Execute a SELECT query and return results.

        Args:
            query: SQL query string
            params: Optional query parameters (using @param style)

        Returns:
            QueryResult containing rows and metadata

        Raises:
            RuntimeError: If not connected
            Exception: For database errors
        """
        if not self._is_connected or not self._client:
            raise RuntimeError("Not connected to database. Call connect() first.")

        try:
            job_config = bigquery.QueryJobConfig()

            # Add query parameters if provided
            if params:
                job_config.query_parameters = [
                    bigquery.ScalarQueryParameter(name, "STRING", value)
                    for name, value in params.items()
                ]

            query_job = self._client.query(query, job_config=job_config)

            # Wait for query to complete
            result = query_job.result()

            # Convert to list of dictionaries
            rows = [dict(row) for row in result]

            # Get column names
            columns = list(result.schema) if result.schema else []

            return QueryResult(
                rows=rows,
                row_count=len(rows),
                columns=columns,
                query=query,
            )
        except GoogleAPIError as e:
            raise Exception(f"Query failed: {e}") from e

    def execute(self, query: str, params: dict[str, Any] | None = None) -> int:
        """
        Execute a DDL/DML statement and return affected row count.

        Args:
            query: SQL statement
            params: Optional query parameters

        Returns:
            Number of affected rows

        Raises:
            RuntimeError: If not connected
            Exception: For database errors
        """
        if not self._is_connected or not self._client:
            raise RuntimeError("Not connected to database. Call connect() first.")

        try:
            job_config = bigquery.QueryJobConfig()

            # Add query parameters if provided
            if params:
                job_config.query_parameters = [
                    bigquery.ScalarQueryParameter(name, "STRING", value)
                    for name, value in params.items()
                ]

            query_job = self._client.query(query, job_config=job_config)
            result = query_job.result()

            # Return number of rows affected (for DML statements)
            return cast(
                int,
                result.num_affected_rows if hasattr(result, "num_affected_rows") else 0,
            )
        except GoogleAPIError as e:
            raise Exception(f"Execute failed: {e}") from e

    @contextmanager
    def transaction(self):
        """
        Context manager for transaction handling.

        Note: BigQuery doesn't support traditional transactions.
        This is provided for API compatibility but operates as a pass-through.

        Yields:
            BigQuery client

        Example:
            with connector.transaction() as client:
                client.query("INSERT INTO dataset.users ...")
                client.query("UPDATE dataset.stats ...")
        """
        if not self._is_connected or not self._client:
            raise RuntimeError("Not connected to database. Call connect() first.")

        try:
            yield self._client
        except Exception:
            # BigQuery doesn't support rollback, so just re-raise
            raise

    def execute_batch(self, query: str, params_list: list[dict[str, Any]]) -> int:
        """
        Execute a query multiple times with different parameters.

        Note: BigQuery is optimized for bulk operations, so consider
        using load_table_from_json or similar methods for large datasets.

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

        if not self._is_connected or not self._client:
            raise RuntimeError("Not connected to database. Call connect() first.")

        try:
            total_affected = 0
            for params in params_list:
                job_config = bigquery.QueryJobConfig()
                job_config.query_parameters = [
                    bigquery.ScalarQueryParameter(name, "STRING", value)
                    for name, value in params.items()
                ]

                query_job = self._client.query(query, job_config=job_config)
                result = query_job.result()
                total_affected += (
                    cast(int, result.num_affected_rows)
                    if hasattr(result, "num_affected_rows")
                    else 0
                )

            return total_affected
        except GoogleAPIError as e:
            raise Exception(f"Batch execute failed: {e}") from e

    def load_from_json(
        self,
        table_id: str,
        json_data: list[dict[str, Any]],
        schema: list[bigquery.SchemaField] | None = None,
        write_disposition: str = "WRITE_TRUNCATE",
    ) -> int:
        """
        Load data from JSON into a BigQuery table.

        Args:
            table_id: Full table ID (project.dataset.table)
            json_data: List of dictionaries to load
            schema: Table schema (optional, will autodetect if not provided)
            write_disposition: Write disposition (WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY)

        Returns:
            Number of rows loaded

        Raises:
            RuntimeError: If not connected
            Exception: For load errors
        """
        if not self._is_connected or not self._client:
            raise RuntimeError("Not connected to database. Call connect() first.")

        try:
            import io
            import json

            # Convert JSON data to file-like object
            json_string = json.dumps(json_data)
            json_file = io.StringIO(json_string)

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=write_disposition,
            )

            if schema:
                job_config.schema = schema
            else:
                job_config.autodetect = True

            job = self._client.load_table_from_file(json_file, table_id, job_config=job_config)
            job.result()

            return job.output_rows if job.output_rows else 0
        except GoogleAPIError as e:
            raise Exception(f"Load from JSON failed: {e}") from e

    def load_from_dataframe(
        self,
        table_id: str,
        dataframe: Any,  # pandas.DataFrame
        write_disposition: str = "WRITE_TRUNCATE",
    ) -> int:
        """
        Load data from a pandas DataFrame into a BigQuery table.

        Args:
            table_id: Full table ID (project.dataset.table)
            dataframe: pandas DataFrame to load
            write_disposition: Write disposition (WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY)

        Returns:
            Number of rows loaded

        Raises:
            RuntimeError: If not connected
            Exception: For load errors
        """
        if not self._is_connected or not self._client:
            raise RuntimeError("Not connected to database. Call connect() first.")

        try:
            job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)

            job = self._client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)
            job.result()

            return job.output_rows if job.output_rows else 0
        except GoogleAPIError as e:
            raise Exception(f"Load from DataFrame failed: {e}") from e

    def export_to_dataframe(self, query: str) -> Any:
        """
        Execute query and return results as pandas DataFrame.

        Args:
            query: SQL query string

        Returns:
            pandas DataFrame with results

        Raises:
            RuntimeError: If not connected
            Exception: For database errors
        """
        if not self._is_connected or not self._client:
            raise RuntimeError("Not connected to database. Call connect() first.")

        try:
            return self._client.query(query).to_dataframe()
        except GoogleAPIError as e:
            raise Exception(f"Export to DataFrame failed: {e}") from e
