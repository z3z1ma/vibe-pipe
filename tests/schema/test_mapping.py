from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from vibe_piper.schema.mapping import (
    ConversionError,
    SourcePathSyntaxError,
    convert_value,
    extract_value,
    parse_source_path,
)
from vibe_piper.types import DataType


def test_parse_source_path_dot_and_brackets() -> None:
    assert parse_source_path("company.name") == ("company", "name")
    assert parse_source_path("tags[0]") == ("tags", 0)
    assert parse_source_path("data.items[0].name") == ("data", "items", 0, "name")


def test_parse_source_path_rejects_invalid() -> None:
    with pytest.raises(SourcePathSyntaxError):
        parse_source_path("")
    with pytest.raises(SourcePathSyntaxError):
        parse_source_path("a..b")
    with pytest.raises(SourcePathSyntaxError):
        parse_source_path("a[bad]")


def test_extract_value_nested_dict_and_list() -> None:
    source = {
        "company": {"name": "Acme"},
        "tags": ["x", "y"],
        "data": {"items": [{"name": "n0"}, {"name": "n1"}]},
    }

    assert extract_value(source, "company.name") == ("Acme", True)
    assert extract_value(source, "tags[1]") == ("y", True)
    assert extract_value(source, "data.items[0].name") == ("n0", True)


def test_extract_value_missing_logs_warning(caplog: pytest.LogCaptureFixture) -> None:
    source = {"company": {"name": "Acme"}}

    with caplog.at_level("WARNING"):
        value, found = extract_value(source, "company.missing", field_name="company_name")
    assert value is None
    assert found is False
    assert any("Missing source_path" in rec.message for rec in caplog.records)


def test_convert_value_datetime_preserves_timezone() -> None:
    dt = convert_value("2024-01-01T12:00:00Z", DataType.DATETIME, field_name="ts")
    assert isinstance(dt, datetime)
    assert dt.tzinfo is not None
    assert dt.tzinfo.utcoffset(dt) == timezone.utc.utcoffset(dt)


def test_convert_value_date() -> None:
    d = convert_value("2024-01-02", DataType.DATE, field_name="d")
    assert d == date(2024, 1, 2)


def test_convert_value_integer_and_float_and_boolean() -> None:
    assert convert_value("42", DataType.INTEGER, field_name="i") == 42
    assert convert_value(3.0, DataType.INTEGER, field_name="i") == 3

    with pytest.raises(ConversionError):
        convert_value(3.5, DataType.INTEGER, field_name="i")

    assert convert_value("3.14", DataType.FLOAT, field_name="f") == 3.14
    assert convert_value("true", DataType.BOOLEAN, field_name="b") is True
    assert convert_value("0", DataType.BOOLEAN, field_name="b") is False
