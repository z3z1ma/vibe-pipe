"""Source-path extraction and type conversion.

`source_path` is a lightweight path syntax for extracting values from nested
structures (dicts and lists). Supported syntax:

- Dot segments for dict keys: `company.name`
- Bracket segments for list indexes: `tags[0]`
- Mixed: `data.items[0].name`

Missing paths return `None` and emit a `warning` log.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time
from typing import Any, Final, TypeAlias

from vibe_piper.types import DataType

logger = logging.getLogger(__name__)

PathToken: TypeAlias = str | int

_MISSING: Final[object] = object()


class SourcePathSyntaxError(ValueError):
    """Raised when a `source_path` string is not valid."""


class ConversionError(ValueError):
    """Raised when a value cannot be converted to the requested data type."""


def parse_source_path(path: str) -> tuple[PathToken, ...]:
    """Parse a dot/bracket `source_path` into tokens.

    Examples:
        - `company.name` -> ("company", "name")
        - `tags[0]` -> ("tags", 0)
        - `data.items[0].name` -> ("data", "items", 0, "name")
    """
    if not path:
        msg = "source_path cannot be empty"
        raise SourcePathSyntaxError(msg)

    tokens: list[PathToken] = []
    buf: list[str] = []
    i = 0

    def flush_buf() -> None:
        if not buf:
            return
        tokens.append("".join(buf))
        buf.clear()

    while i < len(path):
        ch = path[i]

        if ch == ".":
            if i == 0 or i == len(path) - 1:
                msg = f"Invalid source_path {path!r}: empty segment"
                raise SourcePathSyntaxError(msg)

            if buf:
                flush_buf()
                i += 1
                continue

            # Allow `items[0].name` (dot after a bracket index).
            if tokens and isinstance(tokens[-1], int):
                i += 1
                continue

            msg = f"Invalid source_path {path!r}: empty segment"
            raise SourcePathSyntaxError(msg)

        if ch == "[":
            flush_buf()
            i += 1
            if i >= len(path):
                msg = f"Invalid source_path {path!r}: missing closing ']'"
                raise SourcePathSyntaxError(msg)

            end = path.find("]", i)
            if end == -1:
                msg = f"Invalid source_path {path!r}: missing closing ']'"
                raise SourcePathSyntaxError(msg)

            index_str = path[i:end].strip()
            if not index_str or not index_str.isdigit():
                msg = f"Invalid source_path {path!r}: invalid list index {index_str!r}"
                raise SourcePathSyntaxError(msg)

            tokens.append(int(index_str))
            i = end + 1
            continue

        if ch == "]":
            msg = f"Invalid source_path {path!r}: unexpected ']'"
            raise SourcePathSyntaxError(msg)

        buf.append(ch)
        i += 1

    flush_buf()

    if not tokens:
        msg = f"Invalid source_path {path!r}: no tokens"
        raise SourcePathSyntaxError(msg)

    for tok in tokens:
        if isinstance(tok, str) and not tok:
            msg = f"Invalid source_path {path!r}: empty segment"
            raise SourcePathSyntaxError(msg)

    return tuple(tokens)


def _resolve_tokens(source: Any, tokens: Sequence[PathToken]) -> Any:
    current: Any = source

    for tok in tokens:
        if isinstance(tok, str):
            if isinstance(current, Mapping) and tok in current:
                current = current[tok]
                continue
            return _MISSING

        # tok is int
        if isinstance(current, Sequence) and not isinstance(current, (str, bytes, bytearray)):
            if 0 <= tok < len(current):
                current = current[tok]
                continue
            return _MISSING

        return _MISSING

    return current


def extract_value(
    source: Any,
    source_path: str,
    *,
    field_name: str | None = None,
    warn_on_missing: bool = True,
) -> tuple[Any, bool]:
    """Extract a value from a nested record using `source_path`.

    Returns:
        (value, found)

        If the path is missing, (None, False) is returned and a warning is logged.
        If the path exists but the value is None, (None, True) is returned.
    """
    tokens = parse_source_path(source_path)
    resolved = _resolve_tokens(source, tokens)
    if resolved is _MISSING:
        if warn_on_missing:
            logger.warning(
                "Missing source_path for field %r: %s",
                field_name or "<unknown>",
                source_path,
            )
        return None, False

    return resolved, True


def _parse_datetime_str(value: str) -> datetime:
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


def _parse_date_str(value: str) -> date:
    s = value.strip()
    try:
        return date.fromisoformat(s)
    except ValueError:
        # Allow datetime strings and extract the date component.
        return _parse_datetime_str(s).date()


def convert_value(value: Any, data_type: DataType, *, field_name: str | None = None) -> Any:
    """Convert `value` to the requested `DataType`.

    Conversions are implemented for DATETIME/DATE/INTEGER/FLOAT/BOOLEAN.
    Other types are passed through.
    """
    if value is None:
        return None

    fname = field_name or "<unknown>"

    if data_type is DataType.DATETIME:
        if isinstance(value, datetime):
            return value
        if isinstance(value, date) and not isinstance(value, datetime):
            return datetime.combine(value, time.min)
        if isinstance(value, str):
            try:
                return _parse_datetime_str(value)
            except ValueError as e:
                msg = f"Field {fname!r} conversion to DATETIME failed: {e}"
                raise ConversionError(msg) from e
        msg = f"Field {fname!r} conversion to DATETIME failed: got {type(value).__name__}"
        raise ConversionError(msg)

    if data_type is DataType.DATE:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            try:
                return _parse_date_str(value)
            except ValueError as e:
                msg = f"Field {fname!r} conversion to DATE failed: {e}"
                raise ConversionError(msg) from e
        msg = f"Field {fname!r} conversion to DATE failed: got {type(value).__name__}"
        raise ConversionError(msg)

    if data_type is DataType.INTEGER:
        if isinstance(value, bool):
            msg = f"Field {fname!r} conversion to INTEGER failed: got bool"
            raise ConversionError(msg)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            if value.is_integer():
                return int(value)
            msg = f"Field {fname!r} conversion to INTEGER failed: non-integer float {value!r}"
            raise ConversionError(msg)
        if isinstance(value, str):
            try:
                return int(value.strip())
            except ValueError as e:
                msg = f"Field {fname!r} conversion to INTEGER failed: {e}"
                raise ConversionError(msg) from e
        try:
            converted = int(value)
        except (TypeError, ValueError) as e:
            msg = f"Field {fname!r} conversion to INTEGER failed: {e}"
            raise ConversionError(msg) from e
        return converted

    if data_type is DataType.FLOAT:
        if isinstance(value, bool):
            msg = f"Field {fname!r} conversion to FLOAT failed: got bool"
            raise ConversionError(msg)
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value.strip())
            except ValueError as e:
                msg = f"Field {fname!r} conversion to FLOAT failed: {e}"
                raise ConversionError(msg) from e
        try:
            return float(value)
        except (TypeError, ValueError) as e:
            msg = f"Field {fname!r} conversion to FLOAT failed: {e}"
            raise ConversionError(msg) from e

    if data_type is DataType.BOOLEAN:
        if isinstance(value, bool):
            return value
        if isinstance(value, int) and not isinstance(value, bool):
            if value in (0, 1):
                return bool(value)
            msg = f"Field {fname!r} conversion to BOOLEAN failed: invalid int {value!r}"
            raise ConversionError(msg)
        if isinstance(value, str):
            s = value.strip().lower()
            true_values = {"true", "t", "yes", "y", "1"}
            false_values = {"false", "f", "no", "n", "0"}
            if s in true_values:
                return True
            if s in false_values:
                return False
            msg = f"Field {fname!r} conversion to BOOLEAN failed: invalid string {value!r}"
            raise ConversionError(msg)

        msg = f"Field {fname!r} conversion to BOOLEAN failed: got {type(value).__name__}"
        raise ConversionError(msg)

    return value
