"""
Declarative schema definition API for Vibe Piper.

This module provides a class-based, declarative syntax for defining schemas.
It complements the existing imperative Schema/SchemaField classes with a more
Pythonic, type-safe API inspired by Pydantic and Pandera.

Example:
    >>> from vibe_piper import define_schema, String, Integer
    >>>
    >>> @define_schema
    ... class UserSchema:
    ...     id: Integer = Integer()
    ...     email: String = String(max_length=255)
    >>>
    >>> schema = UserSchema.to_schema()
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Protocol,
    TypeVar,
)

from vibe_piper.types import DataType, Schema, SchemaField

if TYPE_CHECKING:
    from vibe_piper.types import DataRecord

# Import DataRecord here to avoid circular imports
# Will be imported at module level for use in validate method


# =============================================================================
# Type Variables
# =============================================================================

T = TypeVar("T")
T_schema = TypeVar("T_schema")  # Remove bound to avoid Protocol covariance issue


# =============================================================================
# Field Base Classes
# =============================================================================


@dataclass(frozen=True)
class Field:
    """
    Base field descriptor for declarative schema definitions.

    Field objects are used as class attributes in schema definitions
    to declare the type and constraints of individual fields.

    Attributes:
        data_type: The DataType enum value for this field
        nullable: Whether the field can contain None values
        required: Whether the field is required (cannot be omitted)
        default: Default value if field is optional
        description: Optional documentation
        metadata: Additional metadata (tags, constraints, etc.)
        constraints: Validation constraints specific to data type
    """

    data_type: DataType
    nullable: bool = False
    required: bool = True
    default: Any | None = None
    description: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    constraints: Mapping[str, Any] = field(default_factory=dict)

    def __set_name__(self, owner: type, name: str) -> None:
        """Set the field name when used as a descriptor."""
        self._name = name

    def __get__(self, obj: object | None, objtype: type | None = None) -> str:
        """
        Get the field name when accessed as a class attribute.

        Returns the actual column/field name, respecting aliases if present.
        """
        if obj is None:
            # Accessed from class, return the name
            return getattr(self, "_name", "")

        raise AttributeError(
            f"Field '{self._name}' cannot be accessed from instances. "
            "Use the schema class to validate data instead."
        )

    def to_schema_field(self, name: str) -> SchemaField:
        """
        Convert this Field to a SchemaField instance.

        Args:
            name: The field name to use

        Returns:
            A SchemaField instance
        """
        return SchemaField(
            name=name,
            data_type=self.data_type,
            required=self.required,
            nullable=self.nullable,
            description=self.description,
            constraints={
                **self.constraints,
                **self.metadata,
            },
        )


# =============================================================================
# Specialized Field Types
# =============================================================================


class String(Field):
    """
    Field descriptor for string data.

    Example:
        >>> @define_schema
        ... class UserSchema:
        ...     email: String = String(max_length=255)
        ...     name: String = String(min_length=1, max_length=100)
    """

    def __init__(
        self,
        *,
        min_length: int | None = None,
        max_length: int | None = None,
        pattern: str | None = None,
        nullable: bool = False,
        required: bool = True,
        default: str | None = None,
        description: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ):
        """
        Initialize a String field.

        Args:
            min_length: Minimum string length
            max_length: Maximum string length
            pattern: Regex pattern the string must match
            nullable: Whether None values are allowed
            required: Whether the field is required
            default: Default value
            description: Field description
            metadata: Additional metadata
        """
        constraints: dict[str, Any] = {}
        if min_length is not None:
            constraints["min_length"] = min_length
        if max_length is not None:
            constraints["max_length"] = max_length
        if pattern is not None:
            constraints["pattern"] = pattern

        super().__init__(
            data_type=DataType.STRING,
            nullable=nullable,
            required=required,
            default=default,
            description=description,
            metadata=metadata or {},
            constraints=constraints,
        )


class Integer(Field):
    """
    Field descriptor for integer data.

    Example:
        >>> @define_schema
        ... class ProductSchema:
        ...     quantity: Integer = Integer(ge=0, le=1000)
    """

    def __init__(
        self,
        *,
        min_value: int | None = None,
        max_value: int | None = None,
        ge: int | None = None,  # alias for min_value
        le: int | None = None,  # alias for max_value
        nullable: bool = False,
        required: bool = True,
        default: int | None = None,
        description: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ):
        """
        Initialize an Integer field.

        Args:
            min_value: Minimum value (inclusive)
            max_value: Maximum value (inclusive)
            ge: Alias for min_value (greater than or equal)
            le: Alias for max_value (less than or equal)
            nullable: Whether None values are allowed
            required: Whether the field is required
            default: Default value
            description: Field description
            metadata: Additional metadata
        """
        # Resolve aliases
        if ge is not None:
            if min_value is not None:
                msg = "Cannot specify both min_value and ge"
                raise ValueError(msg)
            min_value = ge
        if le is not None:
            if max_value is not None:
                msg = "Cannot specify both max_value and le"
                raise ValueError(msg)
            max_value = le

        constraints: dict[str, Any] = {}
        if min_value is not None:
            constraints["min_value"] = min_value
        if max_value is not None:
            constraints["max_value"] = max_value

        super().__init__(
            data_type=DataType.INTEGER,
            nullable=nullable,
            required=required,
            default=default,
            description=description,
            metadata=metadata or {},
            constraints=constraints,
        )


class Float(Field):
    """
    Field descriptor for float/decimal data.

    Example:
        >>> @define_schema
        ... class ProductSchema:
        ...     price: Float = Float(min_value=0.0, max_value=1000000.0)
    """

    def __init__(
        self,
        *,
        min_value: float | None = None,
        max_value: float | None = None,
        ge: float | None = None,  # alias for min_value
        le: float | None = None,  # alias for max_value
        nullable: bool = False,
        required: bool = True,
        default: float | None = None,
        description: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ):
        """
        Initialize a Float field.

        Args:
            min_value: Minimum value (inclusive)
            max_value: Maximum value (inclusive)
            ge: Alias for min_value
            le: Alias for max_value
            nullable: Whether None values are allowed
            required: Whether the field is required
            default: Default value
            description: Field description
            metadata: Additional metadata
        """
        # Resolve aliases
        if ge is not None:
            if min_value is not None:
                msg = "Cannot specify both min_value and ge"
                raise ValueError(msg)
            min_value = ge
        if le is not None:
            if max_value is not None:
                msg = "Cannot specify both max_value and le"
                raise ValueError(msg)
            max_value = le

        constraints: dict[str, Any] = {}
        if min_value is not None:
            constraints["min_value"] = min_value
        if max_value is not None:
            constraints["max_value"] = max_value

        super().__init__(
            data_type=DataType.FLOAT,
            nullable=nullable,
            required=required,
            default=default,
            description=description,
            metadata=metadata or {},
            constraints=constraints,
        )


class Boolean(Field):
    """
    Field descriptor for boolean data.

    Example:
        >>> @define_schema
        ... class UserSchema:
        ...     is_active: Boolean = Boolean(default=True)
    """

    def __init__(
        self,
        *,
        nullable: bool = False,
        required: bool = True,
        default: bool | None = None,
        description: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ):
        """
        Initialize a Boolean field.

        Args:
            nullable: Whether None values are allowed
            required: Whether the field is required
            default: Default value
            description: Field description
            metadata: Additional metadata
        """
        super().__init__(
            data_type=DataType.BOOLEAN,
            nullable=nullable,
            required=required,
            default=default,
            description=description,
            metadata=metadata or {},
            constraints={},
        )


class DateTime(Field):
    """
    Field descriptor for datetime data.

    Example:
        >>> @define_schema
        ... class EventSchema:
        ...     timestamp: DateTime = DateTime()
    """

    def __init__(
        self,
        *,
        nullable: bool = False,
        required: bool = True,
        default: Any | None = None,
        description: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ):
        """
        Initialize a DateTime field.

        Args:
            nullable: Whether None values are allowed
            required: Whether the field is required
            default: Default value
            description: Field description
            metadata: Additional metadata
        """
        super().__init__(
            data_type=DataType.DATETIME,
            nullable=nullable,
            required=required,
            default=default,
            description=description,
            metadata=metadata or {},
            constraints={},
        )


class Date(Field):
    """
    Field descriptor for date data (without time).

    Example:
        >>> @define_schema
    ... class EventSchema:
    ...     birth_date: Date = Date()
    """

    def __init__(
        self,
        *,
        nullable: bool = False,
        required: bool = True,
        default: Any | None = None,
        description: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ):
        """
        Initialize a Date field.

        Args:
            nullable: Whether None values are allowed
            required: Whether the field is required
            default: Default value
            description: Field description
            metadata: Additional metadata
        """
        super().__init__(
            data_type=DataType.DATE,
            nullable=nullable,
            required=required,
            default=default,
            description=description,
            metadata=metadata or {},
            constraints={},
        )


class Array(Field):
    """
    Field descriptor for array/list data.

    Example:
        >>> @define_schema
        ... class PostSchema:
        ...     tags: Array = Array()
    """

    def __init__(
        self,
        *,
        nullable: bool = False,
        required: bool = True,
        default: list[Any] | None = None,
        description: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ):
        """
        Initialize an Array field.

        Args:
            nullable: Whether None values are allowed
            required: Whether the field is required
            default: Default value
            description: Field description
            metadata: Additional metadata
        """
        super().__init__(
            data_type=DataType.ARRAY,
            nullable=nullable,
            required=required,
            default=default,
            description=description,
            metadata=metadata or {},
            constraints={},
        )


class Object(Field):
    """
    Field descriptor for object/dict data.

    Example:
        >>> @define_schema
        ... class UserSchema:
        ...     metadata: Object = Object()
    """

    def __init__(
        self,
        *,
        nullable: bool = False,
        required: bool = True,
        default: dict[str, Any] | None = None,
        description: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ):
        """
        Initialize an Object field.

        Args:
            nullable: Whether None values are allowed
            required: Whether the field is required
            default: Default value
            description: Field description
            metadata: Additional metadata
        """
        super().__init__(
            data_type=DataType.OBJECT,
            nullable=nullable,
            required=required,
            default=default,
            description=description,
            metadata=metadata or {},
            constraints={},
        )


class AnyType(Field):
    """
    Field descriptor for any data type.

    Example:
        >>> @define_schema
        ... class EventSchema:
        ...     payload: AnyType = AnyType()
    """

    def __init__(
        self,
        *,
        nullable: bool = False,
        required: bool = True,
        default: Any | None = None,
        description: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ):
        """
        Initialize an AnyType field.

        Args:
            nullable: Whether None values are allowed
            required: Whether the field is required
            default: Default value
            description: Field description
            metadata: Additional metadata
        """
        super().__init__(
            data_type=DataType.ANY,
            nullable=nullable,
            required=required,
            default=default,
            description=description,
            metadata=metadata or {},
            constraints={},
        )


# =============================================================================
# Schema Metaclass
# =============================================================================


class SchemaMeta(type):
    """
    Metaclass for building declarative schemas.

    This metaclass processes class definitions and builds a Schema object
    from Field descriptors. It adds the `to_schema()` classmethod to the class.
    """

    _schema: ClassVar[Schema]
    __fields__: ClassVar[dict[str, Field]]

    def __new__(
        mcs: type[SchemaMeta],
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> SchemaMeta:
        """Create a new schema class."""
        # Extract schema metadata from kwargs
        schema_name: str = kwargs.pop("name", name)
        description: str | None = kwargs.pop("description", None)
        metadata: dict[str, Any] = dict(kwargs.pop("metadata", {}))

        # Collect fields from namespace
        fields: dict[str, Field] = {}
        for key, value in list(namespace.items()):
            # Skip private attributes and special methods
            if key.startswith("_") or key in ("__annotations__",):
                continue

            # Check if it's a Field instance
            if isinstance(value, Field):
                fields[key] = value

        # Inherit fields from parent classes
        for base in bases:
            if hasattr(base, "__fields__"):
                for field_name, field_obj in base.__fields__.items():
                    # Child fields override parent fields
                    if field_name not in fields:
                        fields[field_name] = field_obj

        # Build the Schema object
        schema_fields: tuple[SchemaField, ...] = tuple(
            field_obj.to_schema_field(field_name)
            for field_name, field_obj in fields.items()
        )

        schema = Schema(
            name=schema_name,
            fields=schema_fields,
            description=description,
            metadata=metadata,
        )

        # Create the class
        cls = type.__new__(mcs, name, bases, namespace)

        # Attach schema and fields as class attributes using setattr
        cls._schema = schema
        cls.__fields__ = fields

        return cls


# =============================================================================
# Protocol for Declarative Schemas
# =============================================================================


class DeclarativeSchema(Protocol):
    """Protocol for declarative schema classes."""

    _schema: ClassVar[Schema]
    __fields__: ClassVar[dict[str, Field]]

    @classmethod
    def to_schema(cls) -> Schema:
        """Convert this declarative schema to a Schema object."""
        ...

    @classmethod
    def validate(cls, data: Mapping[str, Any]) -> DataRecord:
        """Validate data against this schema."""
        ...

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Initialize subclass behavior."""
        ...


# =============================================================================
# Decorator Function
# =============================================================================


def define_schema(
    _cls: type[T] | None = None,
    *,
    name: str | None = None,
    description: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> Callable[[type[T]], type[T]] | type[T]:
    """
    Decorator to define a declarative schema.

    This decorator transforms a class with Field attributes into a schema class
    with methods to convert to Schema objects and validate data.

    Can be used with or without arguments:

        >>> @define_schema
        ... class UserSchema:
        ...     id: Integer = Integer()
        >>>
        >>> @define_schema(description="User account schema")
        ... class UserSchema:
        ...     id: Integer = Integer()

    Args:
        _cls: Class being decorated (when used without arguments)
        name: Optional schema name (defaults to class name)
        description: Optional schema description
        metadata: Optional schema metadata

    Returns:
        A decorator function or the decorated class

    Example:
        >>> @define_schema(description="User account schema")
        ... class UserSchema:
        ...     id: Integer = Integer()
        ...     email: String = String(max_length=255)
        >>>
        >>> schema = UserSchema.to_schema()
    """

    def decorator(cls: type[T]) -> type[T]:
        """Transform the class into a declarative schema."""
        # Create metaclass instance
        schema_name = name or cls.__name__

        # Build Schema using metaclass logic - collect from class dict
        fields: dict[str, Field] = {}

        # Get the class's own attributes (not inherited)
        for key, value in cls.__dict__.items():
            # Skip private attributes and special methods
            if key.startswith("_"):
                continue
            # Check if it's a Field instance
            if isinstance(value, Field):
                fields[key] = value

        # Also collect from parent classes
        for base in cls.__mro__[1:]:  # Skip the class itself
            if hasattr(base, "__fields__"):
                for field_name, field_obj in base.__fields__.items():
                    # Child fields override parent fields
                    if field_name not in fields:
                        fields[field_name] = field_obj

        # Build schema fields
        schema_fields = tuple(
            field_obj.to_schema_field(field_name)
            for field_name, field_obj in fields.items()
        )

        # Create Schema object
        schema = Schema(
            name=schema_name,
            fields=schema_fields,
            description=description,
            metadata=dict(metadata or {}),
        )

        # Attach as class attributes
        cls._schema = schema  # type: ignore[attr-defined]
        cls.__fields__ = fields  # type: ignore[attr-defined]
        cls.to_schema = classmethod(lambda cls: cls._schema)  # type: ignore[attr-defined]

        # Import DataRecord lazily to avoid circular imports
        def validate_method(cls: type, data: Mapping[str, Any]) -> DataRecord:
            from vibe_piper.types import DataRecord

            return DataRecord(data=data, schema=cls._schema)  # type: ignore[attr-defined]

        cls.validate = classmethod(validate_method)  # type: ignore[attr-defined]

        return cls

    # Support using decorator with or without arguments
    if _cls is None:
        # Called with arguments: @define_schema(...)
        return decorator
    else:
        # Called without arguments: @define_schema
        return decorator(_cls)


# =============================================================================
# Re-exports for convenience
# =============================================================================

__all__ = [
    # Field types
    "Field",
    "String",
    "Integer",
    "Float",
    "Boolean",
    "DateTime",
    "Date",
    "Array",
    "Object",
    "AnyType",  # Renamed from Any to avoid conflict with typing.Any
    # Decorator
    "define_schema",
    # Protocol
    "DeclarativeSchema",
]
