# External Quality Tools Integration

This guide explains how to integrate Great Expectations and Soda with Vibe Piper
for unified quality reporting.

## Overview

Vibe Piper provides integrations with external quality tools including:
- **Great Expectations (GE)**: Industry-standard data testing and documentation library
- **Soda**: Modern data quality testing framework with CI/CD integration

These integrations allow you to:
- Use existing GE suites and Soda checks
- Execute validations via `@ge_asset` and `@soda_asset` decorators
- Get unified quality reports from all tools
- Display consistent quality dashboards

## Installation

### Base Installation
```bash
pip install vibe-piper
```

### With Great Expectations
```bash
pip install vibe-piper[ge]
```

### With Soda
```bash
pip install vibe-piper[soda]
```

### With Both Tools
```bash
pip install vibe-piper[ge,soda]
```

## Great Expectations Integration

### Basic Usage

```python
from vibe_piper import ge_asset, DataRecord, Schema, SchemaField

# Define schema
customer_schema = Schema(
    name="customers",
    fields=(
        SchemaField(name="id", data_type=DataType.INTEGER),
        SchemaField(name="name", data_type=DataType.STRING),
        SchemaField(name="email", data_type=DataType.STRING),
        SchemaField(name="age", data_type=DataType.INTEGER),
    ),
)

# Define GE suite YAML file (ge_suites/customers.yaml):
# expectations:
#   - name: row_count_check
#     type: expect_table_row_count_to_be_between
#     min: 1
#     max: 100000
#   - name: email_format_check
#     type: expect_column_values_to_match_regex
#     column: email
#     regex: "^[\\w\\.-]+@[\\w\\.-]+$"

@ge_asset(suite_path="ge_suites/customers.yaml")
def customers() -> list[DataRecord]:
    """Load customers data with GE validation."""
    records = load_customers_from_source()
    return records
```

### Creating GE Suite Configurations

Use the `create_ge_suite_config()` helper to programmatically create GE suites:

```python
from vibe_piper.external_quality import create_ge_suite_config, save_ge_suite

config = create_ge_suite_config(
    expectations=[
        {
            "name": "row_count_check",
            "type": "expect_table_row_count_to_be_between",
            "min": 100,
            "max": 100000,
        },
        {
            "name": "email_format",
            "type": "expect_column_values_to_match_regex",
            "column": "email",
            "regex": r"^[\\w\\.-]+@[\\w\\.-]+$",
        },
    ],
    data_asset_name="customers",
)

# Save to YAML file
save_ge_suite(config, "ge_suites/customers.yaml")
```

### Loading and Saving GE Suites

```python
from vibe_piper.external_quality import load_ge_suite, save_ge_suite

# Load existing suite
config = load_ge_suite("ge_suites/customers.yaml")

# Modify and save
config["expectations"].append({
    "name": "new_check",
    "type": "expect_column_values_to_not_be_null",
    "column": "name",
})

save_ge_suite(config, "ge_suites/customers.yaml")
```

### Supported GE Expectation Types

Vibe Piper supports the following Great Expectations expectation types:

#### Table-Level Expectations
- `expect_table_row_count_to_be_between`
- `expect_table_row_count_to_equal`
- `expect_table_columns_to_contain`
- `expect_table_columns_to_not_contain`

#### Column-Level Expectations
- `expect_column_to_exist`
- `expect_column_values_to_not_be_null`
- `expect_column_values_to_be_unique`
- `expect_column_values_to_be_in_set`
- `expect_column_values_to_match_regex`
- `expect_column_values_to_be_of_type`
- `expect_column_values_to_be_between`

## Soda Integration

### Basic Usage

```python
from vibe_piper import soda_asset, DataRecord, Schema, SchemaField

# Define schema
sales_schema = Schema(
    name="sales",
    fields=(
        SchemaField(name="id", data_type=DataType.INTEGER),
        SchemaField(name="customer_id", data_type=DataType.INTEGER),
        SchemaField(name="amount", data_type=DataType.FLOAT),
        SchemaField(name="status", data_type=DataType.STRING),
    ),
)

# Define Soda checks YAML file (soda_checks/sales.yaml):
# checks:
#   - name: row_count
#     type: row_count
#     min: 1
#     max: 1000000
#   - name: email_format
#     type: values_in_set
#     column: email_domain
#     values: ["gmail.com", "yahoo.com", "outlook.com"]

@soda_asset(checks_path="soda_checks/sales.yaml")
def sales() -> list[DataRecord]:
    """Load sales data with Soda validation."""
    records = load_sales_from_source()
    return records
```

### Creating Soda Checks Configurations

Use the `create_soda_checks_config()` helper to programmatically create Soda checks:

```python
from vibe_piper.external_quality import create_soda_checks_config, save_soda_checks

config = create_soda_checks_config(
    checks=[
        {
            "name": "row_count",
            "type": "row_count",
            "min": 100,
            "max": 100000,
        },
        {
            "name": "email_format",
            "type": "values_in_set",
            "column": "email_domain",
            "values": ["gmail.com", "yahoo.com", "outlook.com"],
        },
    ],
    data_source_name="sales",
)

# Save to YAML file
save_soda_checks(config, "soda_checks/sales.yaml")
```

### Loading and Saving Soda Checks

```python
from vibe_piper.external_quality import load_soda_checks, save_soda_checks

# Load existing checks
config = load_soda_checks("soda_checks/sales.yaml")

# Modify and save
config["checks"].append({
    "name": "new_check",
    "type": "missing_values",
    "column": "status",
    "max_missing_pct": 5.0,
})

save_soda_checks(config, "soda_checks/sales.yaml")
```

### Supported Soda Check Types

Vibe Piper supports the following Soda check types:

#### Row-Level Checks
- `row_count`: Check row count is within range

#### Freshness Checks
- `freshness`: Check data age against a maximum age

#### Null Checks
- `missing_values`: Check percentage of null values

#### Uniqueness Checks
- `duplicate_values`: Check percentage of duplicate values

#### Value Range Checks
- `values_in_range`: Check values are within a range

#### Value Set Checks
- `values_in_set`: Check values are in an allowed set

#### Reference Checks
- `reference`: Check foreign key integrity

#### Schema Checks
- `schema`: Check required columns exist

## Unified Quality Reporting

### Merging Results from Multiple Tools

```python
from vibe_piper.external_quality import (
    merge_quality_results,
    generate_unified_report,
    display_quality_dashboard,
)

# Run validations with both GE and Soda
ge_result = run_ge_validation(data, "ge_suites/asset.yaml")
soda_result = run_soda_validation(data, "soda_checks/asset.yaml")

# Merge results
unified = merge_quality_results(
    [ge_result, soda_result],
    asset_name="my_asset",
)

# Generate unified report
report = generate_unified_report(
    asset_name="my_asset",
    tool_results=[ge_result, soda_result],
)
```

### Displaying Quality Dashboard

```python
from vibe_piper.external_quality import display_quality_dashboard

# Display dashboard
print(display_quality_dashboard(report, show_details=True))
```

### Output Example

```
╔══════════════════════════════════════════╗
║         Quality Dashboard: customers                  ║
╠══════════════════════════════════════════╣
║ Overall Status: PASSED                             ║
║ Quality Score: 96.5%                              ║
║                                                   ║
║ Tool Results:                                       ║
║ • Great Expectations: PASSED                     ║
║   - Completeness: 98.0%                         ║
║   - Validity: 95.0%                             ║
║ • Soda: PASSED                                     ║
║   - Uniqueness: 100.0%                            ║
║                                                   ║
║ Checks Run: 2                                     ║
║ Passed: 2                                          ║
║ Failed: 0                                          ║
║                                                   ║
║ Duration: 125ms                                    ║
║ Timestamp: 2026-01-29 20:52:03               ║
╚════════════════════════════════════════════╝
```

## Error Handling

### Consistent Error Messages

All tools use a consistent error format:

```python
from vibe_piper.external_quality import format_consistent_error_message

error_message = format_consistent_error_message(
    tool_type=ToolType.GREAT_EXPECTATIONS,
    asset_name="customers",
    errors=["Email format invalid", "Missing required fields"],
)
print(error_message)
```

### Failure Strategies

Both `@ge_asset` and `@soda_asset` support three failure strategies:

```python
# Raise error on failure (default)
@ge_asset(suite_path="ge_suites/asset.yaml", on_failure="raise")
def my_asset():
    return data

# Warn on failure
@ge_asset(suite_path="ge_suites/asset.yaml", on_failure="warn")
def my_asset():
    return data

# Ignore failures
@ge_asset(suite_path="ge_suites/asset.yaml", on_failure="ignore")
def my_asset():
    return data
```

## Best Practices

### 1. Use Separate Configuration Files

Keep GE suites and Soda checks in separate directories:

```
project/
├── ge_suites/
│   ├── customers.yaml
│   └── sales.yaml
└── soda_checks/
    ├── sales.yaml
    └── products.yaml
```

### 2. Validate Data Before External Checks

Let Vibe Piper run basic quality checks before external tools:

```python
from vibe_piper import asset, check_completeness, check_validity

@asset
def my_data():
    data = load_from_source()

    # VibePiper checks first
    # Then GE/Soda validation
    return data
```

### 3. Monitor Quality Trends

Track quality scores over time to identify data quality degradation:

```python
# Store quality reports in your data warehouse
# Create dashboards showing quality trends
# Set up alerts for quality score drops
```

### 4. Use Descriptive Check Names

Choose clear, descriptive names for checks:

```yaml
# Good
- name: customer_email_format_validation
- name: sales_amount_within_range

# Avoid
- name: check1
- name: validation
```

## Troubleshooting

### Great Expectations Not Available

If you see "Great Expectations integration not available", install with:

```bash
pip install vibe-piper[ge]
```

### Soda Not Available

If you see "Soda integration not available", install with:

```bash
pip install vibe-piper[soda]
```

### PyYAML Not Available

Both GE and Soda require PyYAML for YAML loading. Install with:

```bash
pip install pyyaml
```

### pandas Not Available

Both GE and Soda require pandas for data processing. Install with:

```bash
pip install vibe-piper  # pandas is a core dependency
```

## Example: Complete Pipeline

```python
from vibe_piper import (
    asset,
    ge_asset,
    soda_asset,
    merge_quality_results,
    generate_unified_report,
    display_quality_dashboard,
)

# Define schemas
customer_schema = Schema(name="customers", ...)
sales_schema = Schema(name="sales", ...)

# Define assets with external quality validation
@ge_asset(suite_path="ge_suites/customers.yaml")
@asset(name="customers", schema=customer_schema)
def customers():
    return load_customers()

@soda_asset(checks_path="soda_checks/sales.yaml")
@asset(name="sales", schema=sales_schema)
def sales():
    return load_sales()

# Create pipeline with unified quality reporting
from vibe_piper import build_pipeline

pipeline = build_pipeline("quality_pipeline")

# Run with quality reporting
unified_report = generate_unified_report(
    asset_name="quality_pipeline",
    tool_results=[ge_result, soda_result],
)

print(display_quality_dashboard(unified_report))
```

## Next Steps

- Review GE and Soda documentation for advanced expectation types
- Integrate with CI/CD pipelines for automated quality checks
- Set up quality alerting based on unified reports
- Create custom quality dashboards using the reporting API
