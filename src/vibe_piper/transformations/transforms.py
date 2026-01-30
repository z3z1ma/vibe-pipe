"""
Built-in transformations for common data manipulation tasks.

Provides reusable transformation functions for extracting fields, mapping fields,
computing derived fields, filtering, and enriching data from lookups.
"""

from collections.abc import Callable
from typing import Any

from vibe_piper.types import DataRecord, RecordData


def extract_nested_value(data: RecordData, path: str) -> Any:
    """
    Extract a value from a nested dictionary using dot notation.

    Args:
        data: Dictionary to extract from
        path: Dot-separated path (e.g., "company.name")

    Returns:
        Value at the path, or None if not found

    Example:
        Extract nested value::

            extract_nested_value({"company": {"name": "Acme"}}, "company.name")
            # Returns: "Acme"
    """
    keys = path.split(".")
    value = data

    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return None

    return value


def extract_fields(field_mapping: dict[str, str]) -> Callable[[list[DataRecord]], list[DataRecord]]:
    """
    Extract nested fields from records using dot notation.

    Creates a transformation that extracts fields from nested structures
    and creates new top-level fields.

    Args:
        field_mapping: Mapping from target field name to source path
                      (e.g., {"company_name": "company.name", "city": "address.city"})

    Returns:
        Transformation function

    Example:
        Extract nested fields::

            extract = extract_fields({
                "company_name": "company.name",
                "city": "address.city",
                "state": "address.state"
            })
            transformed = extract(records)
    """

    def transform(data: list[DataRecord]) -> list[DataRecord]:
        result = []

        for record in data:
            new_data = dict(record.data)

            for target_field, source_path in field_mapping.items():
                extracted_value = extract_nested_value(record.data, source_path)
                new_data[target_field] = extracted_value

            # Create new record with same schema
            new_record = DataRecord(
                data=new_data,
                schema=record.schema,
                metadata=record.metadata,
            )
            result.append(new_record)

        return result

    return transform


def map_field(
    field_name: str,
    mapping: dict[Any, Any],
    default: Any | None = None,
) -> Callable[[list[DataRecord]], list[DataRecord]]:
    """
    Map/transform individual field values using a mapping dictionary.

    Args:
        field_name: Name of the field to transform
        mapping: Dictionary mapping old values to new values
        default: Default value if no mapping is found

    Returns:
        Transformation function

    Example:
        Map status codes::

            mapper = map_field("status", {
                "A": "Active",
                "I": "Inactive",
                "P": "Pending"
            }, default="Unknown")
            transformed = mapper(records)
    """

    def transform(data: list[DataRecord]) -> list[DataRecord]:
        result = []

        for record in data:
            new_data = dict(record.data)
            current_value = record.get(field_name)

            # Map value if found in mapping, otherwise use default or keep original
            if current_value in mapping:
                new_data[field_name] = mapping[current_value]
            elif default is not None:
                new_data[field_name] = default

            new_record = DataRecord(
                data=new_data,
                schema=record.schema,
                metadata=record.metadata,
            )
            result.append(new_record)

        return result

    return transform


def compute_field(
    field_name: str,
    compute_fn: Callable[[DataRecord], Any],
) -> Callable[[list[DataRecord]], list[DataRecord]]:
    """
    Add a computed/calculated field to each record.

    Args:
        field_name: Name of the new field to add
        compute_fn: Function that takes a DataRecord and returns the computed value

    Returns:
        Transformation function

    Example:
        Add computed field::

            compute = compute_field("category", lambda r: "premium" if r.get("age") > 30 else "standard")
            transformed = compute(records)
    """

    def transform(data: list[DataRecord]) -> list[DataRecord]:
        result = []

        for record in data:
            new_data = dict(record.data)
            computed_value = compute_fn(record)
            new_data[field_name] = computed_value

            new_record = DataRecord(
                data=new_data,
                schema=record.schema,
                metadata=record.metadata,
            )
            result.append(new_record)

        return result

    return transform


def compute_field_from_expression(
    field_name: str,
    expression: str,
) -> Callable[[list[DataRecord]], list[DataRecord]]:
    """
    Add a computed field from a Python expression.

    Args:
        field_name: Name of the new field to add
        expression: Python expression (e.g., "age * 2" or "price * (1 + tax_rate)")

    Returns:
        Transformation function

    Example:
        Compute total from price and tax::

            compute = compute_field_from_expression("total", "price * (1 + tax_rate)")
            transformed = compute(records)

        Note:
            Only simple arithmetic expressions are supported.
    """

    def transform(data: list[DataRecord]) -> list[DataRecord]:
        result = []

        for record in data:
            new_data = dict(record.data)

            # Build a safe namespace for evaluation
            namespace = {
                k: v for k, v in record.data.items() if isinstance(v, (int, float, str, bool))
            }

            try:
                computed_value = eval(expression, {"__builtins__": {}}, namespace)
                new_data[field_name] = computed_value
            except Exception:
                # If evaluation fails, set to None
                new_data[field_name] = None

            new_record = DataRecord(
                data=new_data,
                schema=record.schema,
                metadata=record.metadata,
            )
            result.append(new_record)

        return result

    return transform


def filter_rows(
    predicate: Callable[[DataRecord], bool],
) -> Callable[[list[DataRecord]], list[DataRecord]]:
    """
    Filter records by a predicate function.

    Args:
        predicate: Function that takes a DataRecord and returns bool

    Returns:
        Transformation function

    Example:
        Filter by condition::

            filt = filter_rows(lambda r: r.get("active") and r.get("age") >= 18)
            filtered = filt(records)
    """

    def transform(data: list[DataRecord]) -> list[DataRecord]:
        return [r for r in data if predicate(r)]

    return transform


def filter_by_field(
    field_name: str,
    value: Any,
    operator: str = "eq",
) -> Callable[[list[DataRecord]], list[DataRecord]]:
    """
    Filter records by a field value using a comparison operator.

    Args:
        field_name: Name of the field to filter on
        value: Value to compare against
        operator: Comparison operator ("eq", "ne", "gt", "lt", "gte", "lte", "in", "not_in", "is_null", "is_not_null")

    Returns:
        Transformation function

    Example:
        Filter by field value::

            filt = filter_by_field("status", "active")
            filt = filter_by_field("age", 18, operator="gte")
            filt = filter_by_field("category", ["A", "B"], operator="in")
            filtered = filt(records)
    """

    def transform(data: list[DataRecord]) -> list[DataRecord]:
        result = []

        for record in data:
            field_value = record.get(field_name)

            if operator == "eq":
                keep = field_value == value
            elif operator == "ne":
                keep = field_value != value
            elif operator == "gt":
                keep = field_value is not None and field_value > value
            elif operator == "lt":
                keep = field_value is not None and field_value < value
            elif operator == "gte":
                keep = field_value is not None and field_value >= value
            elif operator == "lte":
                keep = field_value is not None and field_value <= value
            elif operator == "in":
                keep = field_value in value
            elif operator == "not_in":
                keep = field_value not in value
            elif operator == "is_null":
                keep = field_value is None
            elif operator == "is_not_null":
                keep = field_value is not None
            else:
                keep = True

            if keep:
                result.append(record)

        return result

    return transform


def enrich_from_lookup(
    lookup_data: list[DataRecord],
    lookup_key: str,
    target_key: str,
    fields_to_add: list[str],
    prefix: str = "",
) -> Callable[[list[DataRecord]], list[DataRecord]]:
    """
    Enrich records by joining with a lookup table/reference data.

    Performs a left join with the lookup data and adds specified fields.

    Args:
        lookup_data: List of lookup records to join with
        lookup_key: Field in lookup data to join on
        target_key: Field in target data to join on
        fields_to_add: List of field names from lookup to add to target records
        prefix: Optional prefix to add to lookup fields (e.g., "company_")

    Returns:
        Transformation function

    Example:
        Enrich with lookup data::

            # Enrich orders with customer information
            enrich = enrich_from_lookup(
                lookup_data=customers,
                lookup_key="customer_id",
                target_key="customer_id",
                fields_to_add=["name", "email", "city"],
                prefix="customer_"
            )
            enriched = enrich(orders)
    """
    # Build a lookup dictionary for efficient access
    lookup_dict = {}
    for record in lookup_data:
        key_value = record.get(lookup_key)
        if key_value is not None:
            lookup_dict[key_value] = record

    def transform(data: list[DataRecord]) -> list[DataRecord]:
        result = []

        for record in data:
            new_data = dict(record.data)
            key_value = record.get(target_key)

            # Find matching lookup record
            lookup_record = lookup_dict.get(key_value)

            if lookup_record:
                # Add specified fields from lookup
                for field_name in fields_to_add:
                    field_value = lookup_record.get(field_name)
                    target_field_name = f"{prefix}{field_name}" if prefix else field_name
                    new_data[target_field_name] = field_value

            new_record = DataRecord(
                data=new_data,
                schema=record.schema,
                metadata=record.metadata,
            )
            result.append(new_record)

        return result

    return transform


def rename_fields(field_mapping: dict[str, str]) -> Callable[[list[DataRecord]], list[DataRecord]]:
    """
    Rename fields in records.

    Args:
        field_mapping: Mapping from old field names to new field names

    Returns:
        Transformation function

    Example:
        Rename fields::

            renamer = rename_fields({
                "user_id": "id",
                "user_name": "name",
                "user_email": "email"
            })
            transformed = renamer(records)
    """

    def transform(data: list[DataRecord]) -> list[DataRecord]:
        result = []

        for record in data:
            new_data = {}

            for field_name, value in record.data.items():
                # Rename if in mapping, otherwise keep original name
                new_field_name = field_mapping.get(field_name, field_name)
                new_data[new_field_name] = value

            new_record = DataRecord(
                data=new_data,
                schema=record.schema,
                metadata=record.metadata,
            )
            result.append(new_record)

        return result

    return transform


def drop_fields(fields: list[str]) -> Callable[[list[DataRecord]], list[DataRecord]]:
    """
    Drop specified fields from records.

    Args:
        fields: List of field names to drop

    Returns:
        Transformation function

    Example:
        Drop fields::

            dropper = drop_fields(["password", "ssn", "secret_key"])
            transformed = dropper(records)
    """
    fields_set = set(fields)

    def transform(data: list[DataRecord]) -> list[DataRecord]:
        result = []

        for record in data:
            new_data = {k: v for k, v in record.data.items() if k not in fields_set}

            new_record = DataRecord(
                data=new_data,
                schema=record.schema,
                metadata=record.metadata,
            )
            result.append(new_record)

        return result

    return transform


def select_fields(fields: list[str]) -> Callable[[list[DataRecord]], list[DataRecord]]:
    """
    Select only specified fields from records (inverse of drop_fields).

    Args:
        fields: List of field names to keep

    Returns:
        Transformation function

    Example:
        Select fields::

            selector = select_fields(["id", "name", "email"])
            transformed = selector(records)
    """
    fields_set = set(fields)

    def transform(data: list[DataRecord]) -> list[DataRecord]:
        result = []

        for record in data:
            new_data = {k: v for k, v in record.data.items() if k in fields_set}

            new_record = DataRecord(
                data=new_data,
                schema=record.schema,
                metadata=record.metadata,
            )
            result.append(new_record)

        return result

    return transform


def cast_field(
    field_name: str,
    target_type: type,
    default: Any = None,
) -> Callable[[list[DataRecord]], list[DataRecord]]:
    """
    Cast a field to a different type.

    Args:
        field_name: Name of the field to cast
        target_type: Target type (int, float, str, bool)
        default: Default value if casting fails

    Returns:
        Transformation function

    Example:
        Cast field to int::

            caster = cast_field("age", int, default=0)
            transformed = caster(records)
    """

    def transform(data: list[DataRecord]) -> list[DataRecord]:
        result = []

        for record in data:
            new_data = dict(record.data)
            value = record.get(field_name)

            try:
                if value is not None:
                    new_data[field_name] = target_type(value)
                else:
                    new_data[field_name] = default
            except (ValueError, TypeError):
                new_data[field_name] = default

            new_record = DataRecord(
                data=new_data,
                schema=record.schema,
                metadata=record.metadata,
            )
            result.append(new_record)

        return result

    return transform


__all__ = [
    # Field extraction
    "extract_nested_value",
    "extract_fields",
    # Field mapping/transformation
    "map_field",
    # Computed fields
    "compute_field",
    "compute_field_from_expression",
    # Filtering
    "filter_rows",
    "filter_by_field",
    # Enrichment
    "enrich_from_lookup",
    # Field manipulation
    "rename_fields",
    "drop_fields",
    "select_fields",
    "cast_field",
]
