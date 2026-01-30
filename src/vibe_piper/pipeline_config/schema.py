"""Pipeline configuration schema definitions.

This module defines the data structures for declarative pipeline configuration.
These schemas support TOML, YAML, and JSON configuration files.
"""

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


class SourceType(str, Enum):
    """Source type enumeration."""

    API = "api"
    DATABASE = "database"
    FILE = "file"


class SinkType(str, Enum):
    """Sink type enumeration."""

    DATABASE = "database"
    FILE = "file"
    S3 = "s3"


class MaterializationType(str, Enum):
    """Materialization type enumeration."""

    TABLE = "table"
    VIEW = "view"
    INCREMENTAL = "incremental"
    FILE = "file"


class TransformType(str, Enum):
    """Transform operation type enumeration."""

    EXTRACT_FIELDS = "extract_fields"
    VALIDATE = "validate"
    FILTER = "filter"
    COMPUTE_FIELD = "compute_field"
    AGGREGATE = "aggregate"
    SORT = "sort"
    JOIN = "join"


class CheckType(str, Enum):
    """Expectation check type enumeration."""

    NOT_NULL = "not_null"
    UNIQUE = "unique"
    REGEX = "regex"
    RANGE = "range"
    ROW_COUNT = "row_count"
    VALUE_SET = "value_set"


@dataclass
class PaginationConfig:
    """Pagination configuration for API sources.

    Attributes:
        type: Pagination type (offset, cursor, page, limit_offset)
        items_path: JSON path to items array in response
        limit_param: Query parameter for page size
        offset_param: Query parameter for offset (offset type)
        page_param: Query parameter for page number (page type)
        total_key: JSON path to total count (for total-based pagination)
    """

    type: str
    items_path: str | None = None
    limit_param: str | None = None
    offset_param: str | None = None
    page_param: str | None = None
    total_key: str | None = None


@dataclass
class RateLimitConfig:
    """Rate limiting configuration.

    Attributes:
        requests: Maximum number of requests
        window_seconds: Time window in seconds
    """

    requests: int
    window_seconds: int


@dataclass
class AuthConfig:
    """Authentication configuration.

    Attributes:
        type: Authentication type (bearer, api_key, basic, oauth2)
        from_env: Environment variable name containing the secret
        token: Token value (for direct embedding, not recommended)
        username: Username (for basic auth)
        password: Password (for basic auth, typically from env)
    """

    type: str
    from_env: str | None = None
    token: str | None = None
    username: str | None = None
    password: str | None = None


@dataclass
class SourceConfig:
    """Source configuration.

    Attributes:
        name: Source asset name
        type: Source type (api, database, file)
        endpoint: API endpoint (for API sources)
        base_url: Base URL (for API sources)
        query: SQL query (for database sources)
        path: File path or pattern (for file sources)
        connection: Connection string or reference
        table: Database table name
        schema: Schema reference or inline schema definition
        auth: Authentication configuration (for API sources)
        pagination: Pagination configuration (for API sources)
        rate_limit: Rate limiting configuration (for API sources)
        incremental: Enable incremental loading
        watermark_column: Column for watermark (incremental loading)
        description: Optional description
        tags: Optional tags for organization
        additional_config: Additional configuration fields
    """

    name: str
    type: SourceType
    endpoint: str | None = None
    base_url: str | None = None
    query: str | None = None
    path: str | None = None
    connection: str | None = None
    table: str | None = None
    schema: str | dict[str, Any] | None = None
    auth: AuthConfig | None = None
    pagination: PaginationConfig | None = None
    rate_limit: RateLimitConfig | None = None
    incremental: bool = False
    watermark_column: str | None = None
    description: str | None = None
    tags: list[str] = field(default_factory=list)
    additional_config: dict[str, Any] = field(default_factory=dict)


@dataclass
class SinkConfig:
    """Sink configuration.

    Attributes:
        name: Sink asset name
        type: Sink type (database, file, s3)
        connection: Connection string or reference
        table: Database table name
        schema_name: Database schema name
        path: File path or S3 prefix
        format: File format (parquet, json, csv, jsonl)
        materialization: Materialization strategy
        upsert_key: Column(s) for upsert key
        batch_size: Batch size for writes
        partition_cols: Partition column names
        compression: Compression codec
        schema: Schema reference for target
        description: Optional description
        tags: Optional tags for organization
        additional_config: Additional configuration fields
    """

    name: str
    type: SinkType
    connection: str | None = None
    table: str | None = None
    schema_name: str | None = None
    path: str | None = None
    format: str | None = None
    materialization: MaterializationType = MaterializationType.TABLE
    upsert_key: str | list[str] | None = None
    batch_size: int = 1000
    partition_cols: list[str] = field(default_factory=list)
    compression: str | None = None
    schema: str | dict[str, Any] | None = None
    description: str | None = None
    tags: list[str] = field(default_factory=list)
    additional_config: dict[str, Any] = field(default_factory=dict)


@dataclass
class TransformStep:
    """Single transformation step.

    Attributes:
        type: Transform operation type
        field: Target field name (for compute_field)
        mappings: Field mappings (for extract_fields)
        schema: Schema reference (for validate)
        condition: Filter condition (for filter)
        pattern: Regex pattern (for regex validation)
        value: Static value (for compute_field)
        aggregation: Aggregation type and columns (for aggregate)
        sort_by: Sort columns and direction (for sort)
        join_with: Join source name (for join)
        join_on: Join key columns (for join)
        description: Optional description
    """

    type: TransformType
    field: str | None = None
    mappings: dict[str, str] | None = None
    schema: str | dict[str, Any] | None = None
    condition: str | None = None
    pattern: str | None = None
    value: str | None = None
    aggregation: dict[str, Any] | None = None
    sort_by: list[dict[str, Any]] | None = None
    join_with: str | None = None
    join_on: str | list[str] | None = None
    description: str | None = None


@dataclass
class TransformConfig:
    """Transform configuration.

    Attributes:
        name: Transform asset name
        source: Source asset name or reference
        steps: Array of transformation steps
        description: Optional description
        tags: Optional tags for organization
    """

    name: str
    source: str
    steps: list[TransformStep]
    description: str | None = None
    tags: list[str] = field(default_factory=list)


@dataclass
class ExpectationCheck:
    """Single expectation check.

    Attributes:
        type: Check type
        column: Column name (for column-level checks)
        pattern: Regex pattern (for regex check)
        min_value: Minimum value (for range check)
        max_value: Maximum value (for range check)
        values: Allowed value set (for value_set check)
        min_rows: Minimum row count (for row_count check)
        max_rows: Maximum row count (for row_count check)
        severity: Check severity (error, warning)
        description: Optional description
    """

    type: CheckType
    column: str | None = None
    pattern: str | None = None
    min_value: float | int | None = None
    max_value: float | int | None = None
    values: list[Any] | None = None
    min_rows: int | None = None
    max_rows: int | None = None
    severity: str = "error"
    description: str | None = None


@dataclass
class ExpectationConfig:
    """Expectation configuration.

    Attributes:
        name: Expectation group name
        asset: Asset name to apply expectations to
        checks: Array of expectation checks
        description: Optional description
    """

    name: str
    asset: str
    checks: list[ExpectationCheck]
    description: str | None = None


@dataclass
class JobConfig:
    """Job configuration for scheduled execution.

    Attributes:
        name: Job name
        schedule: Cron expression for scheduling
        sources: Source assets to include
        sinks: Sink assets to include
        transforms: Transform assets to include
        expectations: Expectation groups to include
        environment: Environment to run in
        retry_on_failure: Retry configuration
        timeout: Job timeout in seconds
        description: Optional description
        tags: Optional tags for organization
    """

    name: str
    schedule: str
    sources: list[str] = field(default_factory=list)
    sinks: list[str] = field(default_factory=list)
    transforms: list[str] = field(default_factory=list)
    expectations: list[str] = field(default_factory=list)
    environment: str = "default"
    retry_on_failure: bool = False
    timeout: int | None = None
    description: str | None = None
    tags: list[str] = field(default_factory=list)


@dataclass
class PipelineMetadata:
    """Pipeline metadata section.

    Attributes:
        name: Pipeline name
        version: Pipeline version
        description: Pipeline description
        author: Optional author
        created: Optional creation date
    """

    name: str
    version: str
    description: str | None = None
    author: str | None = None
    created: str | None = None


@dataclass
class PipelineConfig:
    """Complete pipeline configuration.

    Attributes:
        pipeline: Pipeline metadata
        sources: Dictionary of source configurations
        sinks: Dictionary of sink configurations
        transforms: Dictionary of transform configurations
        expectations: Dictionary of expectation configurations
        jobs: Dictionary of job configurations
        config_path: Path to config file (for reference)
    """

    pipeline: PipelineMetadata
    sources: dict[str, SourceConfig] = field(default_factory=dict)
    sinks: dict[str, SinkConfig] = field(default_factory=dict)
    transforms: dict[str, TransformConfig] = field(default_factory=dict)
    expectations: dict[str, ExpectationConfig] = field(default_factory=dict)
    jobs: dict[str, JobConfig] = field(default_factory=dict)
    config_path: Path | None = None

    def get_source(self, name: str) -> SourceConfig:
        """Get source configuration by name.

        Args:
            name: Source name

        Returns:
            Source configuration

        Raises:
            KeyError: If source not found
        """
        if name not in self.sources:
            msg = f"Source '{name}' not found in configuration"
            raise KeyError(msg)
        return self.sources[name]

    def get_sink(self, name: str) -> SinkConfig:
        """Get sink configuration by name.

        Args:
            name: Sink name

        Returns:
            Sink configuration

        Raises:
            KeyError: If sink not found
        """
        if name not in self.sinks:
            msg = f"Sink '{name}' not found in configuration"
            raise KeyError(msg)
        return self.sinks[name]

    def get_transform(self, name: str) -> TransformConfig:
        """Get transform configuration by name.

        Args:
            name: Transform name

        Returns:
            Transform configuration

        Raises:
            KeyError: If transform not found
        """
        if name not in self.transforms:
            msg = f"Transform '{name}' not found in configuration"
            raise KeyError(msg)
        return self.transforms[name]

    def get_expectation(self, name: str) -> ExpectationConfig:
        """Get expectation configuration by name.

        Args:
            name: Expectation name

        Returns:
            Expectation configuration

        Raises:
            KeyError: If expectation not found
        """
        if name not in self.expectations:
            msg = f"Expectation '{name}' not found in configuration"
            raise KeyError(msg)
        return self.expectations[name]

    def get_job(self, name: str) -> JobConfig:
        """Get job configuration by name.

        Args:
            name: Job name

        Returns:
            Job configuration

        Raises:
            KeyError: If job not found
        """
        if name not in self.jobs:
            msg = f"Job '{name}' not found in configuration"
            raise KeyError(msg)
        return self.jobs[name]

    def has_source(self, name: str) -> bool:
        """Check if source exists.

        Args:
            name: Source name

        Returns:
            True if source exists
        """
        return name in self.sources

    def has_sink(self, name: str) -> bool:
        """Check if sink exists.

        Args:
            name: Sink name

        Returns:
            True if sink exists
        """
        return name in self.sinks

    def has_transform(self, name: str) -> bool:
        """Check if transform exists.

        Args:
            name: Transform name

        Returns:
            True if transform exists
        """
        return name in self.transforms

    def has_expectation(self, name: str) -> bool:
        """Check if expectation exists.

        Args:
            name: Expectation name

        Returns:
            True if expectation exists
        """
        return name in self.expectations

    def has_job(self, name: str) -> bool:
        """Check if job exists.

        Args:
            name: Job name

        Returns:
            True if job exists
        """
        return name in self.jobs
