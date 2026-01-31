"""Schema-first mapping utilities.

This package provides schema-first mapping from nested source records into
schema-typed records using `SchemaField.source_path`.

`source_path` syntax supports dot + bracket notation:

- `company.name`
- `tags[0]`
- `data.items[0].name`
"""

from vibe_piper.schema.field_mapper import FieldMapperResult, map_record_to_schema
from vibe_piper.schema.mapping import (
    ConversionError,
    SourcePathSyntaxError,
    convert_value,
    extract_value,
    parse_source_path,
)

__all__ = [
    "ConversionError",
    "FieldMapperResult",
    "SourcePathSyntaxError",
    "convert_value",
    "extract_value",
    "map_record_to_schema",
    "parse_source_path",
]
