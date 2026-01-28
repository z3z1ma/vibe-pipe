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


class MaterializationStrategy(Enum):
    """Strategies for materializing data assets."""

    IN_MEMORY = auto()  # Keep data in memory
    TABLE = auto()  # Materialize as a physical table
    VIEW = auto()  # Create as a virtual view
    FILE = auto()  # Store as files
    INCREMENTAL = auto()  # Incrementally update existing data


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


@dataclass
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
        operator: Optional operator to transform data for this asset
        io_manager: Name of the IO manager to use for materialization (default: "memory")
        materialization: Materialization strategy for controlling how data is
                         stored (default: "table")
        description: Optional documentation
        metadata: Additional metadata (owner, tags, etc.)
        config: Asset-specific configuration
        version: Version identifier for the asset (default: "1")
        partition_key: Optional partition key for large datasets (default: None)
        created_at: Timestamp when the asset was created (default: None)
        updated_at: Timestamp when the asset was last updated (default: None)
        checksum: Optional checksum for data integrity verification (default: None)
    """

    name: str
    asset_type: AssetType
    uri: str
    schema: Schema | None = None
    operator: "Operator | None" = None
    io_manager: str = "memory"
    materialization: str | MaterializationStrategy = MaterializationStrategy.TABLE
    description: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    config: Mapping[str, Any] = field(default_factory=dict)
    version: str = "1"
    partition_key: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    checksum: str | None = None

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
        checkpoints: Checkpoint data for pipeline recovery (default: empty tuple)
    """

    name: str
    operators: tuple[Operator, ...] = field(default_factory=tuple)
    input_schema: Schema | None = None
    output_schema: Schema | None = None
    description: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    config: Mapping[str, Any] = field(default_factory=dict)
    checkpoints: tuple[str, ...] = field(default_factory=tuple)

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

    def execute(
        self,
        data: Any,
        context: PipelineContext | None = None,
    ) -> Any:
        """
        Execute the pipeline with the given input data.

        Operators are executed in sequence, with the output of each operator
        becoming the input to the next.

        Args:
            data: Input data to process through the pipeline
            context: Optional pipeline execution context. If None, a default
                    context will be created.

        Returns:
            The final output after all operators have been executed.

        Raises:
            Exception: If any operator raises an exception during execution.

        Example:
            Execute a pipeline with data::

                ctx = PipelineContext(pipeline_id="my_pipe", run_id="run_1")
                result = pipeline.execute(input_data, context=ctx)
        """
        # Create default context if not provided
        if context is None:
            context = PipelineContext(
                pipeline_id=self.name,
                run_id=f"{self.name}_{datetime.now().isoformat()}",
            )

        # Validate input schema if present
        if self.input_schema is not None:
            if isinstance(data, DataRecord):
                # DataRecord validates itself in __post_init__
                pass
            elif isinstance(data, list) and data and isinstance(data[0], DataRecord):
                # List of DataRecords - each validates itself
                pass
            elif isinstance(data, dict):
                # Raw dict - create DataRecord to validate
                try:
                    DataRecord(data=data, schema=self.input_schema)
                except ValueError as e:
                    msg = f"Input data validation failed: {e}"
                    raise ValueError(msg) from e

        # Execute operators in sequence
        result = data
        for operator in self.operators:
            result = operator.fn(result, context)

        # Validate output schema if present
        if self.output_schema is not None:
            if isinstance(result, DataRecord):
                # DataRecord validates itself in __post_init__
                pass
            elif (
                isinstance(result, list)
                and result
                and isinstance(result[0], DataRecord)
            ):
                # List of DataRecords - each validates itself
                pass
            elif isinstance(result, dict):
                # Raw dict - create DataRecord to validate
                try:
                    DataRecord(data=result, schema=self.output_schema)
                except ValueError as e:
                    msg = f"Output data validation failed: {e}"
                    raise ValueError(msg) from e

        return result


@dataclass(frozen=True)
class AssetGraph:
    """
    A directed acyclic graph (DAG) of assets with dependencies.

    An AssetGraph manages a collection of assets and tracks their dependencies.
    Each asset can depend on zero or more other assets, forming a DAG that
    represents data lineage and transformation flow.

    The graph validates that:
    - All asset dependencies exist in the graph
    - No circular dependencies exist (it's a true DAG)
    - Asset names are unique

    Attributes:
        name: Unique identifier for this asset graph
        assets: Collection of assets in the graph
        dependencies: Mapping of asset name to its dependencies (asset names)
        description: Optional documentation
        metadata: Additional metadata (tags, owner, etc.)
        config: Graph-specific configuration

    Example:
        Create a simple asset graph with dependencies::

            source_asset = Asset(
                name="raw_users",
                asset_type=AssetType.TABLE,
                uri="postgresql://db/raw_users"
            )

            derived_asset = Asset(
                name="clean_users",
                asset_type=AssetType.TABLE,
                uri="postgresql://db/clean_users"
            )

            graph = AssetGraph(
                name="user_pipeline",
                assets=(source_asset, derived_asset),
                dependencies={"clean_users": ("raw_users",)}
            )

            # Get execution order
            order = graph.topological_order()
            # Returns: ["raw_users", "clean_users"]
    """

    name: str
    assets: tuple[Asset, ...] = field(default_factory=tuple)
    dependencies: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    description: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    config: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate the asset graph configuration."""
        if not self.name:
            msg = "AssetGraph name cannot be empty"
            raise ValueError(msg)

        # Check for duplicate asset names
        asset_names = {asset.name for asset in self.assets}
        if len(asset_names) != len(self.assets):
            msg = f"Duplicate asset names in graph {self.name!r}"
            raise ValueError(msg)

        # Validate all dependencies reference existing assets
        for asset_name, deps in self.dependencies.items():
            if asset_name not in asset_names:
                msg = f"Asset {asset_name!r} in dependencies but not in assets"
                raise ValueError(msg)
            for dep in deps:
                if dep not in asset_names:
                    msg = f"Dependency {dep!r} of {asset_name!r} not found in assets"
                    raise ValueError(msg)

        # Validate no circular dependencies
        self._validate_no_cycles()

    def _validate_no_cycles(self) -> None:
        """Detect circular dependencies using DFS."""
        asset_names = {asset.name for asset in self.assets}
        visited: set[str] = set()
        rec_stack: set[str] = set()

        def dfs(node: str) -> None:
            visited.add(node)
            rec_stack.add(node)

            for neighbor in self.dependencies.get(node, ()):
                if neighbor not in visited:
                    dfs(neighbor)
                elif neighbor in rec_stack:
                    msg = f"Circular dependency detected involving {node!r} and {neighbor!r}"
                    raise ValueError(msg)

            rec_stack.remove(node)

        for asset_name in asset_names:
            if asset_name not in visited:
                dfs(asset_name)

    def get_asset(self, name: str) -> Asset | None:
        """Get an asset by name."""
        for asset in self.assets:
            if asset.name == name:
                return asset
        return None

    def get_dependencies(self, asset_name: str) -> tuple[Asset, ...]:
        """Get all assets that the given asset depends on (upstream)."""
        if asset_name not in self.dependencies:
            return ()
        dep_names = self.dependencies[asset_name]
        deps: list[Asset] = []
        for asset in self.assets:
            if asset.name in dep_names:
                deps.append(asset)
        return tuple(deps)

    def get_dependents(self, asset_name: str) -> tuple[Asset, ...]:
        """Get all assets that depend on the given asset (downstream)."""
        dependents: list[Asset] = []
        for asset in self.assets:
            if asset_name in self.dependencies.get(asset.name, ()):
                dependents.append(asset)
        return tuple(dependents)

    def topological_order(self) -> tuple[str, ...]:
        """
        Compute topological ordering of assets for execution.

        Assets are ordered such that all dependencies appear before
        the assets that depend on them.

        Returns:
            Tuple of asset names in topological order.

        Raises:
            ValueError: If the graph contains cycles (should not happen
                if validation passed).
        """
        # Kahn's algorithm for topological sorting
        asset_names = {asset.name for asset in self.assets}
        in_degree: dict[str, int] = {name: 0 for name in asset_names}

        # Calculate in-degrees
        for asset_name, deps in self.dependencies.items():
            in_degree[asset_name] = len(deps)

        # Initialize queue with assets that have no dependencies
        from collections import deque

        queue: deque[str] = deque()
        for name in asset_names:
            if in_degree[name] == 0:
                queue.append(name)

        result: list[str] = []

        while queue:
            node = queue.popleft()
            result.append(node)

            # Reduce in-degree for all dependents
            for dependent_asset in self.get_dependents(node):
                in_degree[dependent_asset.name] -= 1
                if in_degree[dependent_asset.name] == 0:
                    queue.append(dependent_asset.name)

        if len(result) != len(asset_names):
            msg = "Graph contains cycles - cannot compute topological order"
            raise ValueError(msg)

        return tuple(result)

    def get_upstream(
        self, asset_name: str, depth: int | None = None
    ) -> tuple[Asset, ...]:
        """
        Get all upstream assets (dependencies) recursively.

        This method traverses the dependency graph upstream from the given asset,
        collecting all assets that it depends on, either directly or indirectly.

        Args:
            asset_name: The name of the asset to query
            depth: Maximum depth to traverse. None means unlimited.
                   1 returns only direct dependencies.

        Returns:
            Tuple of upstream Asset objects in dependency order (closest first)

        Raises:
            ValueError: If asset_name is not found in the graph

        Example:
            Get all upstream assets::

                upstream = graph.get_upstream("my_asset")
                # Returns all assets that my_asset depends on
        """
        if self.get_asset(asset_name) is None:
            msg = f"Asset {asset_name!r} not found in graph"
            raise ValueError(msg)

        visited: set[str] = set()
        result: list[Asset] = []

        def traverse(name: str, current_depth: int) -> None:
            """Recursively traverse upstream dependencies."""
            # Check depth limit
            if depth is not None and current_depth > depth:
                return

            # Get direct dependencies
            deps = self.get_dependencies(name)

            for dep in deps:
                if dep.name not in visited:
                    visited.add(dep.name)
                    result.append(dep)
                    # Recursively traverse dependencies
                    traverse(dep.name, current_depth + 1)

        traverse(asset_name, 1)

        return tuple(result)

    def get_downstream(
        self, asset_name: str, depth: int | None = None
    ) -> tuple[Asset, ...]:
        """
        Get all downstream assets (dependents) recursively.

        This method traverses the dependency graph downstream from the given asset,
        collecting all assets that depend on it, either directly or indirectly.

        Args:
            asset_name: The name of the asset to query
            depth: Maximum depth to traverse. None means unlimited.
                   1 returns only direct dependents.

        Returns:
            Tuple of downstream Asset objects in dependency order (closest first)

        Raises:
            ValueError: If asset_name is not found in the graph

        Example:
            Get all downstream assets::

                downstream = graph.get_downstream("source_asset")
                # Returns all assets that depend on source_asset
        """
        if self.get_asset(asset_name) is None:
            msg = f"Asset {asset_name!r} not found in graph"
            raise ValueError(msg)

        visited: set[str] = set()
        result: list[Asset] = []

        def traverse(name: str, current_depth: int) -> None:
            """Recursively traverse downstream dependents."""
            # Check depth limit
            if depth is not None and current_depth > depth:
                return

            # Get direct dependents
            dependents = self.get_dependents(name)

            for dependent in dependents:
                if dependent.name not in visited:
                    visited.add(dependent.name)
                    result.append(dependent)
                    # Recursively traverse dependents
                    traverse(dependent.name, current_depth + 1)

        traverse(asset_name, 1)

        return tuple(result)

    def get_lineage_graph(self) -> dict[str, tuple[str, ...]]:
        """
        Get complete lineage mapping for all assets in the graph.

        Returns a dictionary mapping each asset name to a tuple of its
        direct upstream dependencies.

        Returns:
            Dictionary mapping asset names to tuples of upstream asset names

        Example:
            Get complete lineage graph::

                lineage = graph.get_lineage_graph()
                # Returns: {"asset_b": ("asset_a",), "asset_c": ("asset_b",)}
        """
        # Return a copy to prevent modification
        return dict(self.dependencies)

    def to_mermaid(self) -> str:
        """
        Export the asset graph as a Mermaid DAG diagram.

        Generates a Mermaid flowchart definition that visualizes the
        asset dependency graph. Each asset is represented as a node
        with its dependencies shown as directed edges.

        Returns:
            A string containing the Mermaid diagram definition

        Example:
            Export graph as Mermaid diagram::

                mermaid_diagram = graph.to_mermaid()
                print(mermaid_diagram)
                # Output:
                # graph TD
                #     A[raw_data]
                #     B[processed_data]
                #     A --> B
        """
        lines = ["graph TD"]

        # Define nodes for each asset
        for asset in self.assets:
            # Use asset type in the label for clarity
            type_label = asset.asset_type.name.lower()
            label = f"{asset.name} ({type_label})"
            # Sanitize name for Mermaid (remove special characters)
            node_id = asset.name.replace("-", "_").replace(".", "_")
            lines.append(f"    {node_id}[{label}]")

        # Add edges for dependencies
        for asset_name, deps in self.dependencies.items():
            target_id = asset_name.replace("-", "_").replace(".", "_")
            for dep in deps:
                source_id = dep.replace("-", "_").replace(".", "_")
                lines.append(f"    {source_id} --> {target_id}")

        return "\n".join(lines)

    def add_asset(
        self,
        asset: Asset,
        depends_on: tuple[str, ...] = (),
    ) -> "AssetGraph":
        """
        Return a new asset graph with an asset added.

        Args:
            asset: The asset to add
            depends_on: Names of assets this new asset depends on

        Returns:
            A new AssetGraph with the asset added

        Raises:
            ValueError: If dependency names don't exist in the current graph
        """
        # Validate dependencies exist in current graph
        current_asset_names = {a.name for a in self.assets}
        for dep in depends_on:
            if dep not in current_asset_names:
                msg = f"Cannot add asset: dependency {dep!r} not found in graph"
                raise ValueError(msg)

        new_assets = (*self.assets, asset)
        new_deps = dict(self.dependencies)
        if depends_on:
            new_deps = dict(new_deps)  # Make mutable copy
            new_deps[asset.name] = depends_on

        return AssetGraph(
            name=self.name,
            assets=new_assets,
            dependencies=new_deps,
            description=self.description,
            metadata=self.metadata,
            config=self.config,
        )


# =============================================================================
# IO Manager Protocol
# =============================================================================


class IOManager(Protocol):
    """
    Protocol for IO managers that handle asset materialization.

    IO managers are responsible for storing and loading asset data.
    They provide abstraction over different storage backends (memory,
    file system, S3, databases, etc.) and enable type-safe materialization.

    Example:
        Basic IO manager usage::

            class MyIOManager:
                def handle_output(self, context, data):
                    # Store data to storage
                    pass

                def load_input(self, context):
                    # Load data from storage
                    pass
    """

    def handle_output(self, context: "PipelineContext", data: Any) -> None:
        """
        Store asset output data.

        Args:
            context: The pipeline execution context
            data: The data to store

        Raises:
            Exception: If storage fails
        """
        ...

    def load_input(self, context: "PipelineContext") -> Any:
        """
        Load asset input data.

        Args:
            context: The pipeline execution context

        Returns:
            The loaded data

        Raises:
            Exception: If loading fails
        """
        ...


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


class Executor(Protocol):
    """
    Protocol for executing assets.

    This protocol defines the interface for objects that can execute
    assets within the context of an asset graph.
    """

    def execute(
        self,
        asset: "Asset",
        context: "PipelineContext",
        upstream_results: Mapping[str, Any],
    ) -> "AssetResult":
        """
        Execute an asset and return the result.

        Args:
            asset: The asset to execute
            context: The pipeline execution context
            upstream_results: Results from upstream assets

        Returns:
            AssetResult containing execution outcome
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
class Expectation:
    """
    A data quality rule or expectation.

    Expectations define declarative data quality rules that can be validated
    against data. They encapsulate the logic for checking whether data meets
    certain quality criteria.

    Attributes:
        name: Unique identifier for this expectation
        fn: Function that validates data against this expectation
        description: Optional documentation for the expectation
        severity: How severe a failure is ('error', 'warning', 'info')
        metadata: Additional metadata about the expectation
        config: Expectation-specific configuration
    """

    name: str
    fn: Callable[[Any], ValidationResult]
    description: str | None = None
    severity: str = "error"
    metadata: Mapping[str, Any] = field(default_factory=dict)
    config: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate the expectation configuration."""
        if not self.name:
            msg = "Expectation name cannot be empty"
            raise ValueError(msg)
        valid_severities = {"error", "warning", "info"}
        if self.severity not in valid_severities:
            msg = f"Invalid severity: {self.severity!r}. Must be one of {valid_severities}"
            raise ValueError(msg)

    def validate(self, data: Any) -> ValidationResult:
        """
        Validate data against this expectation.

        Args:
            data: The data to validate

        Returns:
            A ValidationResult indicating whether the data meets this expectation
        """
        return self.fn(data)


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


# =============================================================================
# Execution Engine Types
# =============================================================================


class ErrorStrategy(Enum):
    """Strategies for handling execution errors."""

    FAIL_FAST = auto()  # Stop execution on first error
    CONTINUE = auto()  # Continue execution, collect all errors
    RETRY = auto()  # Retry failed assets (with max_retries)


@dataclass(frozen=True)
class AssetResult:
    """
    Result of executing a single asset.

    Attributes:
        asset_name: Name of the asset that was executed
        success: Whether execution succeeded
        data: The output data (if successful)
        error: Error message (if failed)
        metrics: Execution metrics for this asset
        duration_ms: Execution time in milliseconds
        timestamp: When the asset was executed
        lineage: upstream asset names that contributed to this result
        created_at: Timestamp when the asset data was created
        updated_at: Timestamp when the asset data was last updated
        checksum: Optional checksum for data integrity verification
    """

    asset_name: str
    success: bool
    data: Any | None = None
    error: str | None = None
    metrics: Mapping[str, int | float] = field(default_factory=dict)
    duration_ms: float = 0.0
    timestamp: datetime = field(default_factory=datetime.utcnow)
    lineage: tuple[str, ...] = field(default_factory=tuple)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    checksum: str | None = None


@dataclass(frozen=True)
class ExecutionResult:
    """
    Result of executing an AssetGraph.

    Attributes:
        success: Whether overall execution succeeded
        asset_results: Mapping of asset name to its execution result
        errors: List of all errors encountered during execution
        metrics: Aggregated execution metrics
        duration_ms: Total execution time in milliseconds
        timestamp: When execution started
        assets_executed: Number of assets that were executed
        assets_succeeded: Number of assets that succeeded
        assets_failed: Number of assets that failed
        lineage: Complete lineage mapping of all assets (default: empty dict)
    """

    success: bool
    asset_results: Mapping[str, AssetResult]
    errors: tuple[str, ...] = field(default_factory=tuple)
    metrics: Mapping[str, int | float] = field(default_factory=dict)
    duration_ms: float = 0.0
    timestamp: datetime = field(default_factory=datetime.utcnow)
    assets_executed: int = 0
    assets_succeeded: int = 0
    assets_failed: int = 0
    lineage: Mapping[str, tuple[str, ...]] = field(default_factory=dict)

    def get_asset_result(self, asset_name: str) -> AssetResult | None:
        """
        Get the result for a specific asset.

        Args:
            asset_name: Name of the asset to look up

        Returns:
            AssetResult if found, None otherwise
        """
        return self.asset_results.get(asset_name)

    def get_failed_assets(self) -> tuple[str, ...]:
        """
        Get names of all assets that failed.

        Returns:
            Tuple of asset names that failed execution
        """
        return tuple(
            name for name, result in self.asset_results.items() if not result.success
        )

    def get_succeeded_assets(self) -> tuple[str, ...]:
        """
        Get names of all assets that succeeded.

        Returns:
            Tuple of asset names that succeeded
        """
        return tuple(
            name for name, result in self.asset_results.items() if result.success
        )


# =============================================================================
# Data Quality Types
# =============================================================================


class QualityMetricType(Enum):
    """Types of quality metrics."""

    COMPLETENESS = auto()  # Missing values, null counts
    VALIDITY = auto()  # Schema validation, type violations
    UNIQUENESS = auto()  # Duplicate detection
    FRESHNESS = auto()  # Data age, timestamp checks
    CONSISTENCY = auto()  # Cross-field consistency
    ACCURACY = auto()  # Business rule validation
    CUSTOM = auto()  # User-defined quality metrics


@dataclass(frozen=True)
class QualityMetric:
    """
    A single quality metric measurement.

    Attributes:
        name: Name of the metric (e.g., "null_count", "duplicate_pct")
        metric_type: Type of quality metric
        value: The measured value
        threshold: Optional threshold for the metric
        passed: Whether the metric meets the threshold
        description: Optional description
        metadata: Additional metadata
    """

    name: str
    metric_type: QualityMetricType
    value: int | float
    threshold: int | float | None = None
    passed: bool | None = None  # None if no threshold
    description: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class QualityCheckResult:
    """
    Result of a quality check on data.

    Attributes:
        check_name: Name of the quality check
        passed: Whether the check passed
        metrics: Individual metrics collected
        errors: List of error messages
        warnings: List of warning messages
        timestamp: When the check was performed
        duration_ms: Time taken to perform the check
    """

    check_name: str
    passed: bool
    metrics: tuple[QualityMetric, ...] = field(default_factory=tuple)
    errors: tuple[str, ...] = field(default_factory=tuple)
    warnings: tuple[str, ...] = field(default_factory=tuple)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    duration_ms: float = 0.0


@dataclass(frozen=True)
class DataQualityReport:
    """
    Comprehensive data quality report.

    Attributes:
        total_records: Total number of records checked
        valid_records: Number of records that passed validation
        invalid_records: Number of records that failed validation
        completeness_score: Completeness score (0-1)
        validity_score: Validity score (0-1)
        overall_score: Overall quality score (0-1)
        checks: List of quality check results
        timestamp: When the report was generated
        duration_ms: Time taken to generate the report
    """

    total_records: int
    valid_records: int
    invalid_records: int
    completeness_score: float
    validity_score: float
    overall_score: float
    checks: tuple[QualityCheckResult, ...] = field(default_factory=tuple)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    duration_ms: float = 0.0
