"""
Core type system for Vibe Piper.

This module defines the foundational types that power the entire framework.
All types are strictly typed with mypy in strict mode.

Design principles:
- Types are immutable data structures
- Protocols define extensibility points
- Type aliases provide readability
- Generic types enable composability
"""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import (
    Any,
    Generic,
    Protocol,
    TypeAlias,
    TypeVar,
    final,
)

# =============================================================================
# Type Variables and Generics
# =============================================================================

T = TypeVar("T", covariant=True)
T_input = TypeVar("T_input")
T_output = TypeVar("T_output")
T_ctx = TypeVar("T_ctx", bound="PipelineContext", covariant=True)

# =============================================================================
# Common Type Aliases
# =============================================================================

#: JSON-compatible primitive types
JSONPrimitive: TypeAlias = "str | int | float | bool | None"

#: JSON-compatible values (including nested structures)
JSONValue: TypeAlias = "JSONPrimitive | list['JSONValue'] | dict[str, 'JSONValue']"

#: A mapping of field names to values
RecordData: TypeAlias = Mapping[str, Any]

#: Timestamp in ISO format or datetime object
Timestamp: TypeAlias = str | datetime

#: Operator function signature
OperatorFn: TypeAlias = Callable[[T_input, "PipelineContext"], T_output]

# =============================================================================
# Enums
# =============================================================================


class DataType(Enum):
    """Supported data types for schema fields."""

    STRING = auto()
    INTEGER = auto()
    FLOAT = auto()
    BOOLEAN = auto()
    DATETIME = auto()
    DATE = auto()
    ARRAY = auto()
    OBJECT = auto()
    ANY = auto()


class AssetType(Enum):
    """Types of assets in the system."""

    TABLE = auto()
    VIEW = auto()
    FILE = auto()
    STREAM = auto()
    API = auto()
    MEMORY = auto()


class OperatorType(Enum):
    """Categories of operators."""

    SOURCE = auto()  # Data ingestion
    TRANSFORM = auto()  # Data transformation
    FILTER = auto()  # Data filtering
    AGGREGATE = auto()  # Data aggregation
    JOIN = auto()  # Data joining
    SINK = auto()  # Data output
    VALIDATE = auto()  # Data validation
    CUSTOM = auto()  # Custom user-defined


# =============================================================================
# Core Data Types
# =============================================================================


@final
@dataclass(frozen=True)
class SchemaField:
    """
    Represents a single field in a schema.

    A SchemaField defines the name, type, and constraints for a single
    column or attribute in a data structure.

    Attributes:
        name: The field name
        data_type: The type of data this field holds
        required: Whether the field is required (cannot be None)
        nullable: Whether the field can contain null values
        description: Optional documentation for the field
        constraints: Optional validation constraints (e.g., max_length, min_value)
    """

    name: str
    data_type: DataType
    required: bool = True
    nullable: bool = False
    description: str | None = None
    constraints: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate the schema field configuration."""
        if not self.name:
            msg = "Schema field name cannot be empty"
            raise ValueError(msg)
        if not self.name.replace("_", "").isidentifier():
            msg = f"Invalid field name: {self.name!r}"
            raise ValueError(msg)


@final
@dataclass(frozen=True)
class Schema:
    """
    Represents the structure and constraints of data.

    A Schema defines the expected structure of data, including field names,
    types, and validation rules. Schemas are used to validate data at runtime
    and provide type safety for pipeline operations.

    Attributes:
        name: A unique identifier for this schema
        fields: The fields that make up this schema
        description: Optional documentation
        metadata: Additional metadata (tags, owner, etc.)
    """

    name: str
    fields: tuple[SchemaField, ...] = field(default_factory=tuple)
    description: str | None = None
    metadata: Mapping[str, str | int | float | bool] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate the schema configuration."""
        if not self.name:
            msg = "Schema name cannot be empty"
            raise ValueError(msg)
        field_names = {f.name for f in self.fields}
        if len(field_names) != len(self.fields):
            msg = f"Duplicate field names in schema {self.name!r}"
            raise ValueError(msg)

    def get_field(self, name: str) -> SchemaField | None:
        """Get a field by name."""
        for field_def in self.fields:
            if field_def.name == name:
                return field_def
        return None

    def has_field(self, name: str) -> bool:
        """Check if a field exists in the schema."""
        return self.get_field(name) is not None


@final
@dataclass(frozen=True)
class DataRecord:
    """
    A single record conforming to a Schema.

    DataRecord represents a single row or item of data that conforms to
    a specific Schema. It provides type-safe access to field values.

    Attributes:
        data: The actual data as a mapping
        schema: The schema this record conforms to
        metadata: Optional metadata (source, timestamp, etc.)
    """

    data: RecordData
    schema: Schema
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate that the record conforms to its schema."""
        # Validate required fields are present
        for field_def in self.schema.fields:
            if field_def.required and field_def.name not in self.data:
                msg = f"Required field {field_def.name!r} missing from record"
                raise ValueError(msg)
            # Check nullability
            if (
                field_def.name in self.data
                and self.data[field_def.name] is None
                and not field_def.nullable
            ):
                msg = f"Field {field_def.name!r} is not nullable"
                raise ValueError(msg)

    def get(self, field_name: str, default: T | None = None) -> Any:
        """Get a field value by name."""
        return self.data.get(field_name, default)

    def __getitem__(self, field_name: str) -> Any:
        """Get a field value by name using dictionary syntax."""
        if field_name not in self.data:
            msg = f"Field {field_name!r} not found in record"
            raise KeyError(msg)
        return self.data[field_name]


# =============================================================================
# Pipeline Types
# =============================================================================


@final
@dataclass(frozen=True)
class PipelineContext:
    """
    Execution context for pipeline operations.

    PipelineContext provides runtime information and configuration for
    pipeline execution, including configuration, state, and logging capabilities.

    Attributes:
        pipeline_id: Unique identifier for the pipeline
        run_id: Unique identifier for this execution run
        config: Runtime configuration
        state: Mutable state shared across operators
        metadata: Additional context metadata
    """

    pipeline_id: str
    run_id: str
    config: Mapping[str, Any] = field(default_factory=dict)
    state: dict[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def get_config(self, key: str, default: T | None = None) -> Any:
        """Get a configuration value."""
        return self.config.get(key, default)

    def get_state(self, key: str, default: T | None = None) -> Any:
        """Get a state value."""
        return self.state.get(key, default)

    def set_state(self, key: str, value: Any) -> None:
        """Set a state value."""
        self.state[key] = value


@dataclass(frozen=True)
class Operator:
    """
    A transformation operation in a pipeline.

    Operators are the building blocks of pipelines. Each operator represents
    a single transformation step that takes input data and produces output data.

    Attributes:
        name: Unique identifier for this operator
        operator_type: The category of operator
        fn: The function that implements the operator logic
        input_schema: Optional schema for input validation
        output_schema: Optional schema for output validation
        description: Optional documentation
        config: Operator-specific configuration
    """

    name: str
    operator_type: OperatorType
    fn: OperatorFn[Any, Any]
    input_schema: Schema | None = None
    output_schema: Schema | None = None
    description: str | None = None
    config: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate the operator configuration."""
        if not self.name:
            msg = "Operator name cannot be empty"
            raise ValueError(msg)
        if not self.name.replace("-", "_").replace(".", "_").isidentifier():
            msg = f"Invalid operator name: {self.name!r}"
            raise ValueError(msg)


@dataclass(frozen=True)
class Asset:
    """
    Represents a data source or sink in the system.

    An Asset represents a tangible data entity - a table, file, API endpoint,
    or any other data source/destination that can be used in pipelines.

    Attributes:
        name: Unique identifier for this asset
        asset_type: The type of asset (table, file, etc.)
        uri: Location/identifier for the asset
        schema: The schema of data in this asset
        description: Optional documentation
        metadata: Additional metadata (owner, tags, etc.)
        config: Asset-specific configuration
    """

    name: str
    asset_type: AssetType
    uri: str
    schema: Schema | None = None
    description: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    config: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate the asset configuration."""
        if not self.name:
            msg = "Asset name cannot be empty"
            raise ValueError(msg)
        if not self.uri:
            msg = f"Asset URI cannot be empty for asset {self.name!r}"
            raise ValueError(msg)


@dataclass(frozen=True)
class Pipeline:
    """
    A data pipeline composed of operators.

    A Pipeline defines a directed acyclic graph (DAG) of operations that
    transform data from sources to sinks. Pipelines are composable and can
    be nested within other pipelines.

    Attributes:
        name: Unique identifier for this pipeline
        operators: Ordered sequence of operators to execute
        input_schema: Expected schema for input data
        output_schema: Schema of output data
        description: Optional documentation
        metadata: Additional metadata (tags, owner, etc.)
        config: Pipeline-specific configuration
    """

    name: str
    operators: tuple[Operator, ...] = field(default_factory=tuple)
    input_schema: Schema | None = None
    output_schema: Schema | None = None
    description: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    config: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate the pipeline configuration."""
        if not self.name:
            msg = "Pipeline name cannot be empty"
            raise ValueError(msg)
        # Check for duplicate operator names
        op_names = {op.name for op in self.operators}
        if len(op_names) != len(self.operators):
            msg = f"Duplicate operator names in pipeline {self.name!r}"
            raise ValueError(msg)

    def add_operator(self, operator: Operator) -> "Pipeline":
        """Return a new pipeline with an operator added."""
        new_operators = (*self.operators, operator)
        return Pipeline(
            name=self.name,
            operators=new_operators,
            input_schema=self.input_schema,
            output_schema=self.output_schema,
            description=self.description,
            metadata=self.metadata,
            config=self.config,
        )


# =============================================================================
# Protocols for Extensibility
# =============================================================================


class Transformable(Protocol[T]):
    """
    Protocol for objects that can be transformed.

    This protocol defines the interface for any object that can undergo
    transformations via operators.
    """

    def transform(self, operator: Operator, context: PipelineContext) -> T:
        """
        Apply a transformation to this object.

        Args:
            operator: The operator to apply
            context: The pipeline execution context

        Returns:
            The transformed object
        """
        ...


class Validatable(Protocol):
    """
    Protocol for objects that can be validated.

    This protocol defines the interface for any object that can validate
    itself against a schema or set of rules.
    """

    def validate(self, schema: Schema) -> bool:
        """
        Validate this object against a schema.

        Args:
            schema: The schema to validate against

        Returns:
            True if valid, False otherwise
        """
        ...

    def validation_errors(self, schema: Schema) -> Sequence[str]:
        """
        Get validation errors for this object.

        Args:
            schema: The schema to validate against

        Returns:
            A list of validation error messages
        """
        ...


class Executable(Protocol):
    """
    Protocol for executable pipeline components.

    This protocol defines the interface for any object that can be
    executed within a pipeline context.
    """

    def execute(self, context: PipelineContext) -> Any:
        """
        Execute this component.

        Args:
            context: The pipeline execution context

        Returns:
            The result of execution
        """
        ...


class Source(Protocol[T]):
    """
    Protocol for data sources.

    This protocol defines the interface for objects that can provide
    data to a pipeline.
    """

    def read(self, context: PipelineContext) -> T:
        """
        Read data from this source.

        Args:
            context: The pipeline execution context

        Returns:
            The data read from the source
        """
        ...


class Sink(Protocol):
    """
    Protocol for data sinks.

    This protocol defines the interface for objects that can accept
    data from a pipeline.
    """

    def write(self, data: Any, context: PipelineContext) -> None:
        """
        Write data to this sink.

        Args:
            data: The data to write
            context: The pipeline execution context
        """
        ...


class Observable(Protocol):
    """
    Protocol for observable objects.

    This protocol defines the interface for objects that can emit
    events or be monitored during execution.
    """

    def get_metrics(self) -> Mapping[str, int | float]:
        """
        Get current metrics for this object.

        Returns:
            A mapping of metric names to values
        """
        ...


# =============================================================================
# Utility Types
# =============================================================================


@dataclass(frozen=True)
class ValidationResult:
    """
    Result of a validation operation.

    Attributes:
        is_valid: Whether validation passed
        errors: List of error messages if validation failed
        warnings: List of warning messages
    """

    is_valid: bool
    errors: tuple[str, ...] = field(default_factory=tuple)
    warnings: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class PipelineResult:
    """
    Result of a pipeline execution.

    Attributes:
        success: Whether execution succeeded
        data: The output data (if successful)
        error: Error message (if failed)
        metrics: Execution metrics
        context: The final pipeline context
    """

    success: bool
    data: Any | None = None
    error: str | None = None
    metrics: Mapping[str, int | float] = field(default_factory=dict)
    context: PipelineContext | None = None
