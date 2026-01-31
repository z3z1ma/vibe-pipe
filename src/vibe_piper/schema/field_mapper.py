"""Map nested source records into schema-typed records."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from vibe_piper.schema.mapping import ConversionError, convert_value, extract_value
from vibe_piper.types import DataRecord, Schema


@dataclass(frozen=True)
class FieldMapperResult:
    """Result of mapping a source record into a schema."""

    success: bool
    record: DataRecord | None
    data: Mapping[str, Any]
    errors: tuple[str, ...] = field(default_factory=tuple)
    warnings: tuple[str, ...] = field(default_factory=tuple)


def map_record_to_schema(schema: Schema, source_record: Mapping[str, Any]) -> FieldMapperResult:
    """Map a nested `source_record` into a schema-typed `DataRecord`.

    - Uses `SchemaField.source_path` when present; falls back to the field name.
    - Missing paths return None and emit a warning.
    - Performs type conversion for DATETIME/DATE/INTEGER/FLOAT/BOOLEAN.
    - Collects validation and conversion errors.
    """
    mapped: dict[str, Any] = {}
    errors: list[str] = []
    warnings: list[str] = []

    for field_def in schema.fields:
        source_path = field_def.source_path or field_def.name
        value, found = extract_value(source_record, source_path, field_name=field_def.name)
        if not found:
            warnings.append(f"Missing source_path for field {field_def.name!r}: {source_path}")
            if field_def.required:
                errors.append(
                    f"Required field {field_def.name!r} missing at source_path {source_path!r}"
                )

        if found and value is not None:
            try:
                value = convert_value(value, field_def.data_type, field_name=field_def.name)
            except ConversionError as e:
                errors.append(str(e))
                value = None

        if value is None and not field_def.nullable:
            if found or not field_def.required:
                errors.append(f"Field {field_def.name!r} is not nullable")

        mapped[field_def.name] = value

    record: DataRecord | None = None
    if not errors:
        try:
            record = DataRecord(data=mapped, schema=schema)
        except ValueError as e:
            errors.append(f"Schema validation failed: {e}")

    return FieldMapperResult(
        success=not errors,
        record=record,
        data=mapped,
        errors=tuple(errors),
        warnings=tuple(warnings),
    )
