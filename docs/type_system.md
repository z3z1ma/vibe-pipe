# Vibe Piper Type System Documentation

## Overview

The Vibe Piper type system is the foundation of the entire framework. It provides strict type safety, composability, and extensibility through a carefully designed set of core types, protocols, and type aliases.

This document describes the design principles, core types, and how to use them effectively.

## Design Principles

### 1. Types are Immutable

All core types (`Asset`, `Schema`, `Pipeline`, `Operator`, `DataRecord`) are implemented as frozen dataclasses. This ensures:

- **Thread safety**: Immutable objects can be safely shared across threads
- **Predictability**: Once created, objects cannot be modified accidentally
- **Easier reasoning**: You can trust that an object's state won't change

Example:
```python
schema = Schema(name="users", fields=(SchemaField(name="id", data_type=DataType.INTEGER),))
# schema.fields cannot be modified - attempting to do so raises an exception
```

### 2. Protocols Define Extensibility

Instead of abstract base classes, Vibe Piper uses Python's `Protocol` for defining interfaces. This enables **structural subtyping** (duck typing with type hints), making the framework more flexible:

```python
# Any class with a validate() method can be used as a Validatable
class MyValidator:
    def validate(self, schema: Schema) -> bool:
        return True

def check_validation(obj: Validatable) -> None:
    # MyValidator works here because it has the right structure
    pass
```

### 3. Type Aliases Provide Readability

Complex types are aliased to simple names for better documentation and IDE support:

```python
# Instead of writing this complex type everywhere:
def process(data: dict[str, str | int | float | bool | None]) -> None:
    pass

# Use the type alias:
def process(data: JSONValue) -> None:
    pass
```

### 4. Generic Types Enable Composability

Generic type variables enable creating reusable, type-safe components:

```python
T = TypeVar("T", covariant=True)

class Source(Protocol[T]):
    def read(self, context: PipelineContext) -> T:
        ...

# Can specify exact types
int_source: Source[int] = ...
list_source: Source[list[DataRecord]] = ...
```

## Core Types

### Schema

A `Schema` defines the structure and constraints of data. It's used for validation and documentation.

```python
from vibe_piper import Schema, SchemaField, DataType

# Define a schema with multiple fields
user_schema = Schema(
    name="user",
    fields=(
        SchemaField(
            name="id",
            data_type=DataType.INTEGER,
            description="Unique user identifier"
        ),
        SchemaField(
            name="email",
            data_type=DataType.STRING,
            required=True,
            nullable=False,
            constraints={"max_length": 255}
        ),
        SchemaField(
            name="age",
            data_type=DataType.INTEGER,
            required=False,
            nullable=True
        ),
    ),
    description="User account schema",
    metadata={"owner": "data-team", "pii": True}
)

# Query fields
field = user_schema.get_field("email")
has_id = user_schema.has_field("id")
```

**Key Features:**
- Field validation (names must be valid identifiers)
- Duplicate field detection
- Optional metadata for documentation
- Immutable structure

### SchemaField

Represents a single field in a schema:

```python
field = SchemaField(
    name="username",
    data_type=DataType.STRING,
    required=True,
    nullable=False,
    description="User's login name",
    constraints={"min_length": 3, "max_length": 50}
)
```

**Supported Data Types:**
- `STRING`: Text data
- `INTEGER`: Whole numbers
- `FLOAT`: Decimal numbers
- `BOOLEAN`: True/false values
- `DATETIME`: Date and time values
- `DATE`: Date values (no time)
- `ARRAY`: Arrays/lists
- `OBJECT`: Nested objects
- `ANY`: Any type

### DataRecord

A single record that conforms to a schema:

```python
record = DataRecord(
    data={"id": 1, "email": "user@example.com", "age": 25},
    schema=user_schema,
    metadata={"source": "api", "timestamp": "2024-01-27"}
)

# Access fields
value = record.get("email")
value = record["email"]  # Same as above
missing = record.get("missing", "default")
```

**Validation:**
- Required fields must be present
- Non-nullable fields cannot be None
- Schema is validated at creation time

### Asset

Represents a data source or sink:

```python
from vibe_piper import Asset, AssetType

# Database table
users_table = Asset(
    name="users_table",
    asset_type=AssetType.TABLE,
    uri="postgresql://localhost/mydb/users",
    schema=user_schema,
    metadata={"owner": "analytics", "rows": 1000000}
)

# S3 file
data_file = Asset(
    name="user_exports",
    asset_type=AssetType.FILE,
    uri="s3://my-bucket/exports/users.csv",
    metadata={"format": "csv", "size_bytes": 102400}
)
```

**Asset Types:**
- `TABLE`: Database table or view
- `VIEW`: Database view
- `FILE`: File-based data (CSV, JSON, Parquet, etc.)
- `STREAM`: Streaming data source
- `API`: API endpoint
- `MEMORY`: In-memory data

### Operator

A transformation operation in a pipeline:

```python
from vibe_piper import Operator, OperatorType

def uppercase_emails(data: list[DataRecord], ctx: PipelineContext) -> list[DataRecord]:
    """Transform email addresses to uppercase."""
    result = []
    for record in data:
        new_data = dict(record.data)
        if "email" in new_data and new_data["email"]:
            new_data["email"] = new_data["email"].upper()
        result.append(DataRecord(data=new_data, schema=record.schema))
    return result

uppercase_op = Operator(
    name="uppercase_emails",
    operator_type=OperatorType.TRANSFORM,
    fn=uppercase_emails,
    description="Convert all email addresses to uppercase"
)
```

**Operator Types:**
- `SOURCE`: Data ingestion
- `TRANSFORM`: Data transformation
- `FILTER`: Data filtering
- `AGGREGATE`: Data aggregation
- `JOIN`: Data joining
- `SINK`: Data output
- `VALIDATE`: Data validation
- `CUSTOM`: User-defined operations

### Pipeline

A data pipeline composed of operators:

```python
from vibe_piper import Pipeline

# Create a simple pipeline
pipeline = Pipeline(
    name="user_processing",
    operators=(read_op, uppercase_op, validate_op, write_op),
    input_schema=user_schema,
    output_schema=user_schema,
    description="Process user data"
)

# Add an operator to create a new pipeline (immutable)
extended_pipeline = pipeline.add_operator(additional_op)
```

**Key Features:**
- Operators are executed in sequence
- Duplicate operator names are not allowed
- Pipelines are composable (can be nested)
- Immutable structure

#### Pipeline Execution

Pipelines can be executed with the `execute()` method:

```python
from vibe_piper import Pipeline, PipelineContext

# Create a pipeline with operators
pipeline = Pipeline(
    name="data_processing",
    operators=(filter_op, transform_op, aggregate_op),
)

# Execute the pipeline
ctx = PipelineContext(
    pipeline_id="data_processing",
    run_id="run_001",
    config={"batch_size": 100}
)

result = pipeline.execute(input_data, context=ctx)
```

The `execute()` method:
- Accepts input data and an optional `PipelineContext`
- Creates a default context if none is provided
- Executes operators in sequence, passing output to input
- Validates input/output schemas if present
- Returns the final output

### Built-in Operators

Vibe Piper provides a library of common transformation operators that can be used directly in pipelines.

#### Map Operators

**`map_transform`** - Apply a custom function to each record:

```python
from vibe_piper import map_transform

def uppercase_email(record: DataRecord, ctx: PipelineContext) -> DataRecord:
    new_data = dict(record.data)
    if "email" in new_data:
        new_data["email"] = new_data["email"].upper()
    return DataRecord(data=new_data, schema=record.schema)

map_op = map_transform(
    name="uppercase_emails",
    transform_fn=uppercase_email,
    description="Convert email addresses to uppercase"
)
```

**`map_field`** - Transform a specific field:

```python
from vibe_piper import map_field

map_op = map_field(
    name="uppercase_name",
    field_name="name",
    transform_fn=str.upper,
    description="Convert name field to uppercase"
)
```

**`add_field`** - Add a computed field:

```python
from vibe_piper import add_field

def compute_full_name(record: DataRecord, ctx: PipelineContext) -> str:
    return f"{record.get('first_name')} {record.get('last_name')}"

add_op = add_field(
    name="add_full_name",
    field_name="full_name",
    field_type=DataType.STRING,
    value_fn=compute_full_name,
    description="Add full name field"
)
```

#### Filter Operators

**`filter_operator`** - Filter records with a predicate:

```python
from vibe_piper import filter_operator

def is_adult(record: DataRecord, ctx: PipelineContext) -> bool:
    return record.get("age", 0) >= 18

filter_op = filter_operator(
    name="filter_adults",
    predicate=is_adult,
    description="Keep only adult records"
)
```

**`filter_field_equals`** - Filter by field equality:

```python
from vibe_piper import filter_field_equals

filter_op = filter_field_equals(
    name="filter_active",
    field_name="status",
    value="active",
    description="Keep only active records"
)
```

**`filter_field_not_null`** - Remove null/missing fields:

```python
from vibe_piper import filter_field_not_null

filter_op = filter_field_not_null(
    name="filter_has_email",
    field_name="email",
    description="Remove records without email"
)
```

#### Aggregate Operators

**`aggregate_count`** - Count records:

```python
from vibe_piper import aggregate_count

count_op = aggregate_count(
    name="count_records",
    description="Count total records"
)
```

**`aggregate_sum`** - Sum a field:

```python
from vibe_piper import aggregate_sum

sum_op = aggregate_sum(
    name="sum_amount",
    field_name="amount",
    description="Sum all amounts"
)
```

**`aggregate_group_by`** - Group and aggregate:

```python
from vibe_piper import aggregate_group_by

def count_group(records: list[DataRecord]) -> int:
    return len(records)

group_op = aggregate_group_by(
    name="group_by_category",
    group_field="category",
    aggregate_fn=count_group,
    description="Group by category and count"
)
```

#### Validate Operators

**`validate_schema`** - Validate records against a schema:

```python
from vibe_piper import validate_schema

validate_op = validate_schema(
    name="validate_user_schema",
    schema=user_schema,
    description="Validate user records"
)
```

#### Custom Operators

**`custom_operator`** - Create a custom operator:

```python
from vibe_piper import custom_operator

def custom_transform(data: list[DataRecord], ctx: PipelineContext) -> list[DataRecord]:
    # Your custom logic here
    return data

custom_op = custom_operator(
    name="my_custom_transform",
    fn=custom_transform,
    description="My custom transformation"
)
```

### PipelineContext

Execution context for pipeline operations:

```python
from vibe_piper import PipelineContext

ctx = PipelineContext(
    pipeline_id="user_pipeline",
    run_id="run_123",
    config={
        "batch_size": 1000,
        "parallel": True,
        "max_workers": 4
    },
    metadata={"environment": "production"}
)

# Access configuration
batch_size = ctx.get_config("batch_size", 100)

# Mutable state for sharing data across operators
ctx.set_state("records_processed", 0)
count = ctx.get_state("records_processed")
```

## Protocols (Interfaces)

Protocols define the interface that objects must implement to be compatible with Vibe Piper components.

### Validatable

Objects that can validate themselves against a schema:

```python
class MyData:
    def __init__(self, value: int) -> None:
        self.value = value

    def validate(self, schema: Schema) -> bool:
        return self.value > 0

    def validation_errors(self, schema: Schema) -> list[str]:
        if self.value <= 0:
            return ["Value must be positive"]
        return []

data: Validatable = MyData(42)
is_valid = data.validate(schema)
```

### Transformable

Objects that can be transformed by operators:

```python
class MyData:
    def __init__(self, value: int) -> None:
        self.value = value

    def transform(self, operator: Operator, context: PipelineContext) -> "MyData":
        new_value = operator.fn(self.value, context)
        return MyData(new_value)

data: Transformable[MyData] = MyData(5)
result = data.transform(double_op, ctx)
```

### Executable

Objects that can be executed within a pipeline:

```python
class MyTask:
    def execute(self, context: PipelineContext) -> Any:
        # Perform work
        context.set_state("completed", True)
        return result

task: Executable = MyTask()
output = task.execute(ctx)
```

### Source

Objects that can provide data to a pipeline:

```python
class MySource:
    def __init__(self, filepath: str) -> None:
        self.filepath = filepath

    def read(self, context: PipelineContext) -> list[DataRecord]:
        # Read and parse data
        return records

source: Source[list[DataRecord]] = MySource("data.csv")
data = source.read(ctx)
```

### Sink

Objects that can accept data from a pipeline:

```python
class MySink:
    def __init__(self, filepath: str) -> None:
        self.filepath = filepath

    def write(self, data: list[DataRecord], context: PipelineContext) -> None:
        # Write data to destination
        pass

sink: Sink = MySink("output.csv")
sink.write(records, ctx)
```

### Observable

Objects that can emit metrics:

```python
class MyObservable:
    def __init__(self) -> None:
        self.operations = 0

    def do_work(self) -> None:
        self.operations += 1

    def get_metrics(self) -> dict[str, int | float]:
        return {"operations": self.operations, "rate": 1.5}

obj: Observable = MyObservable()
metrics = obj.get_metrics()
```

## Type Aliases

### JSONPrimitive

Primitive JSON-compatible types:
```python
JSONPrimitive = str | int | float | bool | None
```

### JSONValue

JSON-compatible values including nested structures:
```python
JSONValue = JSONPrimitive | list[JSONValue] | dict[str, JSONValue]
```

### RecordData

A mapping of field names to values:
```python
RecordData = Mapping[str, Any]
```

### Timestamp

Timestamp in ISO format or datetime object:
```python
Timestamp = str | datetime
```

### OperatorFn

Operator function signature:
```python
OperatorFn = Callable[[T_input, PipelineContext], T_output]
```

## Best Practices

### 1. Always Define Schemas

Even for simple data, defining a schema provides validation and documentation:

```python
# Good
schema = Schema(
    name="event",
    fields=(
        SchemaField(name="id", data_type=DataType.STRING),
        SchemaField(name="timestamp", data_type=DataType.DATETIME),
    )
)
record = DataRecord(data={"id": "123", "timestamp": "2024-01-27"}, schema=schema)

# Avoid
# Using raw dicts without schemas loses type safety
```

### 2. Use Type Hints

Always use type hints, even for internal functions:

```python
def process_record(record: DataRecord, ctx: PipelineContext) -> DataRecord:
    ...
```

### 3. Leverage Immutability

Since types are immutable, use transformation methods:

```python
# Create new objects instead of modifying
new_pipeline = pipeline.add_operator(new_op)
new_record = DataRecord(data={**record.data, "new_field": value}, schema=new_schema)
```

### 4. Use Built-in Operators

Prefer built-in operators over custom implementations when possible:

```python
from vibe_piper import (
    Pipeline,
    PipelineContext,
    filter_field_equals,
    map_field,
    aggregate_count,
    Schema,
    SchemaField,
    DataType,
)

# Create a schema
schema = Schema(
    name="transactions",
    fields=(
        SchemaField(name="id", data_type=DataType.INTEGER),
        SchemaField(name="amount", data_type=DataType.FLOAT),
        SchemaField(name="status", data_type=DataType.STRING),
    ),
)

# Build a pipeline with built-in operators
pipeline = Pipeline(
    name="process_transactions",
    operators=(
        filter_field_equals(
            name="filter_completed",
            field_name="status",
            value="completed",
            description="Keep only completed transactions"
        ),
        map_field(
            name="normalize_amount",
            field_name="amount",
            transform_fn=lambda x: round(x, 2),
            description="Round amounts to 2 decimal places"
        ),
        aggregate_count(
            name="count_transactions",
            description="Count completed transactions"
        ),
    ),
)

# Execute the pipeline
ctx = PipelineContext(pipeline_id="process_transactions", run_id="run_001")
result = pipeline.execute(input_data, context=ctx)
```

### 5. Implement Protocols for Custom Components

When creating custom components, implement the relevant protocols:

```python
class MyCustomSource:
    """A custom data source."""

    def read(self, context: PipelineContext) -> list[DataRecord]:
        # Implementation
        pass

# Now it can be used anywhere a Source is expected
def process_data(source: Source[list[DataRecord]]) -> None:
    data = source.read(ctx)
```

### 6. Use Metadata for Documentation

Add metadata to schemas, assets, and pipelines:

```python
schema = Schema(
    name="users",
    fields=fields,
    metadata={
        "owner": "data-team",
        "domain": "user-management",
        "sensitivity": "pii",
        "update_frequency": "daily"
    }
)
```

## Type Safety with Mypy

Vibe Piper uses mypy in strict mode. Run type checking:

```bash
mypy src/
```

All code passes strict type checking:
- No implicit any types
- All functions have type hints
- All return types are explicit
- No unused or ignored type checking

## Testing

The type system includes comprehensive unit tests:

```bash
pytest tests/ -v
```

Coverage report:
```bash
pytest tests/ --cov=src --cov-report=html
```

## Summary

The Vibe Piper type system provides:

- **Safety**: Catch errors at type-check time
- **Clarity**: Clear intent through types
- **Composability**: Combine types and protocols
- **Extensibility**: Easy to add custom components
- **Documentation**: Types serve as documentation

This foundation enables building robust, maintainable data pipelines with confidence.
