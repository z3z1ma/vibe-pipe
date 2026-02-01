# Transformation Library API Reference

This document provides comprehensive reference and examples for the Vibe Piper transformation library.

## Table of Contents

- [Fluent Pipeline API](#fluent-pipeline-api)
- [Built-in Transformations](#built-in-transformations)
- [Validation Helpers](#validation-helpers)
- [Integration with @asset Decorator](#integration-with-asset-decorator)
- [Examples](#examples)

---

## Fluent Pipeline API

The `TransformationBuilder` provides a fluent, chainable API for composing complex transformations.

### Getting Started

```python
from vibe_piper import DataRecord, transform
from vibe_piper.transformations import extract_fields, filter_rows, compute_field

# Start a transformation pipeline
result = (transform(data)
    .pipe(extract_fields({"company_name": "company.name"}))
    .pipe(filter_rows(lambda r: r.get("active")))
    .pipe(compute_field("category", lambda r: "premium" if r.get("age") > 30 else "standard"))
    .execute()
```

### Core Methods

#### `transform(data)`

Start a new transformation builder.

**Parameters:**
- `data`: List of `DataRecord` objects to transform

**Returns:** `TransformationBuilder` instance

**Example:**

```python
builder = transform(records)
```

---

#### `pipe(transform_fn)`

Apply a transformation function using pipe syntax. This is the preferred fluent API pattern.

**Parameters:**
- `transform_fn`: Function that takes a `list[DataRecord]` and returns `list[DataRecord]`

**Returns:** `self` for method chaining

**Example:**

```python
from vibe_piper.transformations import extract_fields, filter_rows

result = (transform(data)
    .pipe(extract_fields({"name": "user.name"}))
    .pipe(filter_rows(lambda r: r.get("active")))
    .execute()
```

---

#### `filter(predicate, field=None, value=None)`

Filter records by a predicate function or shortcut.

**Parameters:**
- `predicate`: Function or shortcut string ("equals", "not_null")
- `field`: Field name (for shortcut predicates)
- `value`: Value to compare (for shortcut predicates)

**Returns:** `self` for method chaining

**Example:**

```python
# Using function
result = (transform(data)
    .filter(lambda r: r.get("age") >= 18)
    .execute()

# Using shortcut
result = (transform(data)
    .filter("equals", field="status", value="active")
    .execute()
```

---

#### `map(transform_fn)`

Transform each record with a function.

**Parameters:**
- `transform_fn`: Function that transforms a `DataRecord`

**Returns:** `self` for method chaining

**Example:**

```python
def uppercase_name(record):
    return DataRecord(
        data={**record.data, "name": record.get("name").upper()},
        schema=record.schema,
        metadata=record.metadata,
    )

result = (transform(data)
    .map(uppercase_name)
    .execute()
```

---

#### `join(right_data, on, how="inner")`

Join with another dataset.

**Parameters:**
- `right_data`: Right dataset to join with
- `on`: Column to join on
- `how`: Join type ("inner", "left", "right", "full")

**Returns:** `self` for method chaining

**Example:**

```python
from vibe_piper.transformations import join

result = (transform(orders)
    .join(customers, on="customer_id", how="left")
    .execute())
```

---

#### `groupby(group_by, aggregations)`

Group by columns and apply aggregations.

**Parameters:**
- `group_by`: Column name or list of column names
- `aggregations`: List of aggregation functions (Sum, Count, Avg, etc.)

**Returns:** `self` for method chaining

**Example:**

```python
from vibe_piper.transformations import Sum, Count, Avg

result = (transform(sales)
    .groupby(["region"], [Sum("amount"), Count("id"), Avg("amount")])
    .execute())
```

---

#### `window(functions, partition_by=None, order_by=None)`

Apply window functions.

**Parameters:**
- `functions`: List of window function objects
- `partition_by`: Column(s) to partition by
- `order_by`: Column(s) to order by

**Returns:** `self` for method chaining

**Example:**

```python
from vibe_piper.transformations import Rank, Lag

result = (transform(sales)
    .window([Rank("rank")], partition_by="category", order_by="amount desc")
    .execute())
```

---

#### `pivot(index, columns, values, aggfunc="mean")`

Pivot wide-to-long transformation.

**Parameters:**
- `index`: Column(s) for index
- `columns`: Column for pivot columns
- `values`: Column for values
- `aggfunc`: Aggregation function ("sum", "mean", "count", etc.)

**Returns:** `self` for method chaining

**Example:**

```python
result = (transform(sales)
    .pivot(index="category", columns="month", values="amount", aggfunc="sum")
    .execute())
```

---

#### `unpivot(id_vars, value_vars, var_name="variable", value_name="value")`

Unpivot long-to-wide transformation.

**Parameters:**
- `id_vars`: Column(s) to keep as identifiers
- `value_vars`: Column(s) to unpivot
- `var_name`: Name for variable column
- `value_name`: Name for value column

**Returns:** `self` for method chaining

**Example:**

```python
result = (transform(monthly_sales)
    .unpivot(id_vars=["category"], value_vars=["Jan", "Feb", "Mar"], var_name="month", value_name="amount")
    .execute())
```

---

#### `custom(transform_fn)`

Add a custom transformation.

**Parameters:**
- `transform_fn`: Custom transformation function

**Returns:** `self` for method chaining

**Example:**

```python
def my_transform(data):
    # Custom logic here
    return data

result = (transform(data)
    .custom(my_transform)
    .execute())
```

---

#### `execute()`

Execute all transformations and return result.

**Returns:** `list[DataRecord]` - Transformed dataset

**Example:**

```python
result = (transform(data)
    .filter(lambda r: r.get("active"))
    .execute()
```

---

## Built-in Transformations

These transformation functions can be used with `pipe()` for composable operations.

### Field Extraction

#### `extract_nested_value(data, path)`

Extract a value from a nested dictionary using dot notation.

**Parameters:**
- `data`: Dictionary to extract from
- `path`: Dot-separated path (e.g., "company.name")

**Returns:** Value at path, or `None` if not found

**Example:**

```python
from vibe_piper.transformations import extract_nested_value

value = extract_nested_value({"company": {"name": "Acme"}}, "company.name")
# Returns: "Acme"
```

---

#### `extract_fields(field_mapping)`

Extract nested fields from records using dot notation.

**Parameters:**
- `field_mapping`: Mapping from target field name to source path

**Returns:** Transformation function

**Example:**

```python
from vibe_piper.transformations import extract_fields

transform = extract_fields({
    "company_name": "company.name",
    "city": "address.city",
    "state": "address.state"
})

result = transform(data)
# Result has: id, user, address, company_name, city, state fields
```

---

### Field Mapping

#### `map_field(field_name, mapping, default=None)`

Map/transform individual field values using a mapping dictionary.

**Parameters:**
- `field_name`: Name of field to transform
- `mapping`: Dictionary mapping old values to new values
- `default`: Default value if no mapping is found

**Returns:** Transformation function

**Example:**

```python
from vibe_piper.transformations import map_field

transform = map_field("status", {
    "A": "Active",
    "I": "Inactive",
    "P": "Pending"
})

result = transform(data)
# All status values are now "Active", "Inactive", or "Pending"
```

---

#### `rename_fields(field_mapping)`

Rename fields in records.

**Parameters:**
- `field_mapping`: Mapping from old field names to new field names

**Returns:** Transformation function

**Example:**

```python
from vibe_piper.transformations import rename_fields

transform = rename_fields({
    "user_id": "id",
    "user_name": "name",
    "user_email": "email"
})

result = transform(data)
# Fields are renamed to: id, name, email
```

---

#### `drop_fields(fields)`

Drop specified fields from records.

**Parameters:**
- `fields`: List of field names to drop

**Returns:** Transformation function

**Example:**

```python
from vibe_piper.transformations import drop_fields

transform = drop_fields(["password", "ssn", "api_key"])

result = transform(data)
# Sensitive fields are removed
```

---

#### `select_fields(fields)`

Select only specified fields from records (inverse of `drop_fields`).

**Parameters:**
- `fields`: List of field names to keep

**Returns:** Transformation function

**Example:**

```python
from vibe_piper.transformations import select_fields

transform = select_fields(["id", "name", "email"])

result = transform(data)
# Only id, name, email fields are kept
```

---

### Computed Fields

#### `compute_field(field_name, compute_fn)`

Add a computed/calculated field to each record.

**Parameters:**
- `field_name`: Name of new field to add
- `compute_fn`: Function that takes a `DataRecord` and returns computed value

**Returns:** Transformation function

**Example:**

```python
from vibe_piper.transformations import compute_field

transform = compute_field("category", lambda r: "premium" if r.get("age") > 30 else "standard")

result = transform(data)
# Each record now has a "category" field based on age
```

---

#### `compute_field_from_expression(field_name, expression)`

Add a computed field from a Python expression.

**Parameters:**
- `field_name`: Name of new field to add
- `expression`: Python expression (e.g., "age * 2", "price * (1 + tax_rate)")

**Returns:** Transformation function

**Example:**

```python
from vibe_piper.transformations import compute_field_from_expression

transform = compute_field_from_expression("total", "price * (1 + tax_rate)")

result = transform(data)
# Each record now has a "total" field
```

---

### Filtering

#### `filter_rows(predicate)`

Filter records by a predicate function.

**Parameters:**
- `predicate`: Function that takes a `DataRecord` and returns `bool`

**Returns:** Transformation function

**Example:**

```python
from vibe_piper.transformations import filter_rows

transform = filter_rows(lambda r: r.get("active") and r.get("age") >= 18)

result = transform(data)
# Only active adults are kept
```

---

#### `filter_by_field(field_name, value, operator="eq")`

Filter records by a field value using comparison operators.

**Parameters:**
- `field_name`: Name of field to filter on
- `value`: Value to compare against
- `operator`: Comparison operator ("eq", "ne", "gt", "lt", "gte", "lte", "in", "not_in", "is_null", "is_not_null")

**Returns:** Transformation function

**Example:**

```python
from vibe_piper.transformations import filter_by_field

# Equal to
transform = filter_by_field("status", "active")

# Greater than
transform = filter_by_field("age", 18, operator="gte")

# In list
transform = filter_by_field("category", ["A", "B"], operator="in")

# Not null
transform = filter_by_field("email", None, operator="is_not_null")
```

---

### Enrichment

#### `enrich_from_lookup(lookup_data, lookup_key, target_key, fields_to_add, prefix="")`

Enrich records by joining with lookup table/reference data.

**Parameters:**
- `lookup_data`: List of lookup records to join with
- `lookup_key`: Field in lookup data to join on
- `target_key`: Field in target data to join on
- `fields_to_add`: List of field names from lookup to add to target records
- `prefix`: Optional prefix to add to lookup fields

**Returns:** Transformation function

**Example:**

```python
from vibe_piper.transformations import enrich_from_lookup

transform = enrich_from_lookup(
    lookup_data=customers,
    lookup_key="customer_id",
    target_key="customer_id",
    fields_to_add=["name", "email", "city"],
    prefix="customer_"
)

result = transform(orders)
# Each order now has customer_name, customer_email, customer_city fields
```

---

### Type Casting

#### `cast_field(field_name, target_type, default=None)`

Cast a field to a different type.

**Parameters:**
- `field_name`: Name of field to cast
- `target_type`: Target type (`int`, `float`, `str`, `bool`)
- `default`: Default value if casting fails

**Returns:** Transformation function

**Example:**

```python
from vibe_piper.transformations import cast_field

# Cast to int
transform = cast_field("age", int, default=0)

# Cast to float
transform = cast_field("price", float)
```

---

## Validation Helpers

These functions help validate data quality and schema compliance.

### Field Validators

#### `validate_field_type(record, field_name, expected_type)`

Validate that a field has the expected data type.

**Parameters:**
- `record`: The record to validate
- `field_name`: Name of field to validate
- `expected_type`: Expected `DataType`

**Returns:** `True` if valid, `False` otherwise

---

#### `validate_field_required(record, field_name)`

Validate that a required field is not null.

**Parameters:**
- `record`: The record to validate
- `field_name`: Name of required field

**Returns:** `True` if field is present and not null, `False` otherwise

---

#### `validate_email_format(email)`

Validate email format.

**Parameters:**
- `email`: Email string to validate

**Returns:** `True` if valid email format, `False` otherwise

---

#### `validate_regex_pattern(value, pattern)`

Validate a string matches a regex pattern.

**Parameters:**
- `value`: String to validate
- `pattern`: Regex pattern to match

**Returns:** `True` if value matches pattern, `False` otherwise

---

#### `validate_range(value, min_value=None, max_value=None)`

Validate that a numeric value is within a range.

**Parameters:**
- `value`: Numeric value to validate
- `min_value`: Minimum allowed value (inclusive)
- `max_value`: Maximum allowed value (inclusive)

**Returns:** `True` if value is within range, `False` otherwise

---

#### `validate_field_length(value, min_length=None, max_length=None)`

Validate that a field's length is within bounds.

**Parameters:**
- `value`: Value to check (string, list, or dict)
- `min_length`: Minimum allowed length (inclusive)
- `max_length`: Maximum allowed length (inclusive)

**Returns:** `True` if length is within bounds, `False` otherwise

---

#### `validate_enum(value, allowed_values)`

Validate that a value is in the allowed set.

**Parameters:**
- `value`: Value to validate
- `allowed_values`: List of allowed values

**Returns:** `True` if value is in allowed set, `False` otherwise

---

### Record/Batch Validators

#### `validate_record(record, schema, strict=False)`

Validate a record against a schema.

**Parameters:**
- `record`: The record to validate
- `schema`: Schema to validate against
- `strict`: If `True`, reject records with extra fields not in schema

**Returns:** `ValidationResult` with validation status and any errors/warnings

**Example:**

```python
from vibe_piper.transformations import validate_record

result = validate_record(record, user_schema)

if not result.is_valid:
    print(f"Validation errors: {result.errors}")
    print(f"Warnings: {result.warnings}")
```

---

#### `validate_batch(records, schema, strict=False)`

Validate a batch of records against a schema.

**Parameters:**
- `records`: List of records to validate
- `schema`: Schema to validate against
- `strict`: If `True`, reject records with extra fields not in schema

**Returns:** `ValidationResult` with validation status and any errors/warnings

**Example:**

```python
from vibe_piper.transformations import validate_batch

result = validate_batch(records, user_schema)

if not result.is_valid:
    print(f"Found {len(result.errors)} validation errors")
```

---

#### `create_validator_from_schema(schema, strict=False)`

Create a validator function from a schema.

**Parameters:**
- `schema`: Schema to validate against
- `strict`: If `True`, reject records with extra fields not in schema

**Returns:** Function that validates a `DataRecord` and returns `bool`

**Example:**

```python
from vibe_piper.transformations import create_validator_from_schema

validator = create_validator_from_schema(user_schema)
valid_records = [r for r in records if validator(r)]
```

---

#### `create_filter_validator(predicate, error_message)`

Create a validator from a predicate function.

**Parameters:**
- `predicate`: Function that takes a `DataRecord` and returns `bool`
- `error_message`: Error message to include in warnings

**Returns:** Function that validates a `DataRecord` and returns `bool`

**Example:**

```python
from vibe_piper.transformations import create_filter_validator

validator = create_filter_validator(
    lambda r: r.get("age") >= 18,
    error_message="User must be 18 or older"
)
```

---

## Integration with @asset Decorator

The transformation library integrates seamlessly with the `@asset` decorator.

```python
from vibe_piper import asset, Schema, SchemaField, DataType
from vibe_piper.transformations import transform, extract_fields, compute_field

@asset(
    name="clean_users",
    schema=Schema(
        name="users",
        fields=[
            SchemaField("id", DataType.INTEGER),
            SchemaField("name", DataType.STRING),
            SchemaField("email", DataType.STRING),
        ],
    ),
)
def clean_users(data):
    """Clean and transform user data."""
    return (transform(data)
        .pipe(extract_fields({"username": "user.name"}))
        .pipe(compute_field("is_adult", lambda r: r.get("age") >= 18))
        .execute())
```

---

## Examples

### Example 1: Simple Data Cleaning

Clean user data by extracting fields and validating emails.

```python
from vibe_piper import DataRecord, transform
from vibe_piper.transformations import (
    extract_fields,
    filter_rows,
    compute_field,
    validate_email_format,
)

users = [
    DataRecord(
        data={"id": 1, "user": {"name": "Alice", "email": "alice@example.com"}, "age": 30},
        schema=schema,
    ),
    DataRecord(
        data={"id": 2, "user": {"name": "Bob", "email": "invalid"}, "age": 25},
        schema=schema,
    ),
]

result = (transform(users)
    .pipe(extract_fields({"name": "user.name", "email": "user.email"}))
    .pipe(filter_rows(lambda r: validate_email_format(r.get("email", ""))))
    .pipe(compute_field("is_adult", lambda r: r.get("age") >= 18))
    .execute()
```

### Example 2: Sales Aggregation

Group sales data by region and calculate aggregates.

```python
from vibe_piper import transform
from vibe_piper.transformations import Sum, Count, Avg

sales_data = [...]  # List of sales records

result = (transform(sales_data)
    .groupby(["region"], [Sum("amount"), Count("order_id"), Avg("amount")])
    .execute()
```

### Example 3: Enrichment with Lookup

Enrich orders with customer information.

```python
from vibe_piper import DataRecord, transform
from vibe_piper.transformations import enrich_from_lookup

orders = [...]  # List of order records
customers = [...]  # List of customer records

result = (transform(orders)
    .pipe(enrich_from_lookup(
        lookup_data=customers,
        lookup_key="customer_id",
        target_key="customer_id",
        fields_to_add=["name", "email", "city"],
        prefix="customer_",
    ))
    .execute()
```

### Example 4: Type Casting and Filtering

Convert string IDs to integers and filter by range.

```python
from vibe_piper import transform
from vibe_piper.transformations import cast_field, filter_by_field

result = (transform(data)
    .pipe(cast_field("user_id", int, default=0))
    .pipe(filter_by_field("user_id", 100, operator="lte"))
    .execute()
```

---

## Best Practices

1. **Use `pipe()` for readability**: The `pipe()` pattern is more readable than nested method chaining for complex transformations.

2. **Compose transformations**: Combine simple transformations into complex pipelines.

3. **Type safety**: All transformations preserve type information through `DataRecord` and `Schema`.

4. **Lazy evaluation**: Transformations are only executed when `.execute()` is called.

5. **Reusable transformations**: Create transformation functions once and reuse them across multiple pipelines.

6. **Schema-aware validation**: Use `validate_record()` and `validate_batch()` to ensure data quality.

---

## See Also

- [Schema Definitions](schema_definitions.md)
- [Validation Framework](validation.md)
- [Data Quality](quality.md)
- [API Reference](api_reference.md)
