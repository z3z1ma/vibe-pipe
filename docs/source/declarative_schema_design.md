# Declarative Schema Definitions - Design Document

## Overview

This document describes the design for Vibe Piper's declarative schema definition API. This API provides a class-based, declarative syntax for defining schemas, complementing the existing imperative `Schema` and `SchemaField` classes.

## Design Goals

1. **Declarative**: Describe WHAT, not HOW - users declare schema structure, framework handles implementation
2. **Pythonic**: Natural Python syntax using type annotations and class definitions
3. **Type-Safe**: Full mypy strict mode compliance
4. **Composable**: Support schema inheritance and composition
5. **Backward Compatible**: Work with existing `Schema`/`SchemaField` classes

## API Design

### Approach: Field-Based Class Definitions

Following Pandera's lead (which itself is inspired by Pydantic), we'll use a field-based approach where users define classes with field descriptors.

### Basic Usage

```python
from vibe_piper import define_schema, String, Integer, Float, DateTime

@define_schema
class UserSchema:
    """User account schema"""

    id: Integer = Integer()
    email: String = String(max_length=255)
    age: Integer = Integer(min_value=0, max_value=120, nullable=True)
    created_at: DateTime = DateTime()
```

This creates a class that:
1. Has a `to_schema()` classmethod that returns a `Schema` object
2. Can be used for validation
3. Supports inheritance
4. Is fully type-safe

### Type Aliases

For users who prefer brevity, we support type annotations without field initialization:

```python
@define_schema
class UserSchema:
    id: Integer
    email: String
    created_at: DateTime
```

### Optional Fields

Use `nullable=True` or Python's `Optional` type hint:

```python
from typing import Optional

@define_schema
class UserSchema:
    # Using nullable parameter
    age: Integer = Integer(nullable=True)

    # Using Optional type hint (preferred)
    phone: String = String(nullable=True)

    # Or with default None
    bio: String = String(default=None)
```

### Field Constraints

Each field type supports relevant constraints:

```python
@define_schema
class ProductSchema:
    name: String = String(max_length=200, min_length=1)
    price: Float = Float(min_value=0.0, max_value=1000000.0)
    quantity: Integer = Integer(ge=0, le=10000)  # aliases for min_value/max_value
    sku: String = String(pattern=r'^[A-Z]{2}-\d{4}$')  # regex pattern
    description: String = String(max_length=5000, nullable=True)
```

### Schema Inheritance

Extend schemas by inheritance:

```python
@define_schema
class BaseSchema:
    id: Integer = Integer()
    created_at: DateTime = DateTime()
    updated_at: DateTime = DateTime()

@define_schema
class UserSchema(BaseSchema):
    email: String = String(max_length=255)
    name: String = String(max_length=100)

    # Override parent field
    id: Integer = Integer(description="Unique user ID")
```

### Schema Composition

Combine multiple schemas:

```python
@define_schema
class TimestampMixin:
    created_at: DateTime = DateTime()
    updated_at: DateTime = DateTime()

@define_schema
class SoftDeleteMixin:
    deleted_at: DateTime = DateTime(nullable=True)
    is_deleted: Boolean = Boolean(default=False)

@define_schema
class UserSchema(TimestampMixin, SoftDeleteMixin):
    id: Integer = Integer()
    email: String = String(max_length=255)
```

### Schema Metadata

Add metadata at schema level:

```python
@define_schema(
    name="user_v2",
    description="User account schema v2.0",
    metadata={"owner": "data-team", "pii": True}
)
class UserSchema:
    id: Integer = Integer()
    email: String = String(max_length=255)
```

### Field Metadata

Add metadata at field level:

```python
@define_schema
class UserSchema:
    id: Integer = Integer(description="Unique user identifier")
    email: String = String(
        max_length=255,
        description="User email address",
        metadata={"sensitive": True, " searchable": True}
    )
```

### Converting to Schema

Get the underlying `Schema` object:

```python
schema = UserSchema.to_schema()
# Returns: Schema(name="UserSchema", fields=(SchemaField(...), ...))

# Validate with the schema
from vibe_piper import DataRecord
record = DataRecord(
    data={"id": 1, "email": "user@example.com"},
    schema=schema
)
```

### Validation

Use the schema class directly for validation:

```python
# Validate data
user_data = {"id": 1, "email": "user@example.com", "age": 25}
validated = UserSchema.validate(user_data)

# Or use it as a context manager
with UserSchema.validator() as validator:
    result = validator.validate(raw_data)
```

## Implementation Components

### 1. Field Classes

Create field descriptor classes for each data type:

```python
class String:
    def __init__(
        self,
        *,
        min_length: int | None = None,
        max_length: int | None = None,
        pattern: str | None = None,
        nullable: bool = False,
        default: Any | None = None,
        description: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ):
        ...

class Integer:
    def __init__(
        self,
        *,
        min_value: int | None = None,  # alias: ge
        max_value: int | None = None,  # alias: le
        nullable: bool = False,
        default: Any | None = None,
        description: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ):
        ...
```

### 2. Decorator: @define_schema

The decorator that transforms a class into a schema class:

```python
def define_schema(
    name: str | None = None,
    description: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> Callable[[type[T]], type[T]]:
    """Decorator to define a declarative schema."""
    ...
```

### 3. Metaclass: SchemaMeta

Metaclass that processes the class definition and builds the schema:

```python
class SchemaMeta(type):
    """Metaclass for building schema from class definition."""

    def __new__(mcs, name, bases, namespace, **kwargs):
        # Process field definitions
        # Build Schema object
        # Add to_schema() classmethod
        ...
```

### 4. Protocol: DeclarativeSchema

Protocol that all declarative schemas follow:

```python
class DeclarativeSchema(Protocol):
    """Protocol for declarative schema classes."""

    @classmethod
    def to_schema(cls) -> Schema:
        """Convert to Schema object."""
        ...

    @classmethod
    def validate(cls, data: Mapping[str, Any]) -> DataRecord:
        """Validate data against this schema."""
        ...
```

## Comparison with Existing API

### Imperative (Current)

```python
from vibe_piper import Schema, SchemaField, DataType

schema = Schema(
    name="user",
    fields=(
        SchemaField(name="id", data_type=DataType.INTEGER),
        SchemaField(
            name="email",
            data_type=DataType.STRING,
            constraints={"max_length": 255}
        ),
    )
)
```

### Declarative (New)

```python
from vibe_piper import define_schema, Integer, String

@define_schema
class UserSchema:
    id: Integer = Integer()
    email: String = String(max_length=255)

schema = UserSchema.to_schema()
```

## Migration Guide

Users can gradually migrate:

1. **Keep using imperative API** - No breaking changes
2. **Mix both approaches** - Use declarative for new schemas, imperative for existing
3. **Fully migrate** - Eventually convert all schemas to declarative

## File Structure

```
src/vibe_piper/
├── types.py              # Existing Schema, SchemaField
├── schema_definitions.py  # New: Declarative schema API
└── __init__.py           # Export both APIs

tests/
└── test_schema_definitions.py  # Tests for declarative API
```

## Testing Strategy

1. **Unit tests for each field type** - String, Integer, Float, etc.
2. **Schema conversion tests** - to_schema() produces correct Schema objects
3. **Inheritance tests** - Schema inheritance works correctly
4. **Validation tests** - Data validation against schemas
5. **Error handling tests** - Invalid schema definitions raise errors
6. **Type checking tests** - mypy strict mode compliance

## Future Enhancements

1. **Generic schemas** - `class GenericSchema(Schema[T]): ...`
2. **Custom validators** - Decorator-based custom validation functions
3. **Schema transformations** - `schema.add_field()`, `schema.remove_field()`
4. **Auto-documentation** - Generate docs from schema definitions
5. **IDE integration** - Better autocomplete and type inference

## References

- Pydantic: https://docs.pydantic.dev/latest/concepts/models/
- Pandera: https://pandera.readthedocs.io/en/latest/dataframe_models.html
- Python PEP 484: Type Hints
- Python PEP 560: Core support for typing module and generic types
