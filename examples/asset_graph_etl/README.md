# AssetGraph ETL Pipeline Example

A canonical, fully runnable AssetGraph example that demonstrates Vibe Piper's declarative asset-based pipeline framework with local file I/O and validation.

## Overview

This example showcases a complete ETL pipeline using:
- **PipelineDefinitionContext**: Declarative pipeline building with `@pipeline.asset()` decorators
- **ExecutionEngine**: Orchestration of asset execution respecting dependencies
- **ValidationSuite**: Data quality checks with fail-fast behavior
- **File-based I/O**: CSV input/output (no external services required)
- **Asset dependencies**: Automatic dependency inference from function signatures

## Pipeline Steps

1. **extract** - Read raw user data from `data/users.csv`
2. **transform** - Clean and enrich data (normalize emails, add customer tiers, derived fields)
3. **validate** - Run data quality checks (email format, null checks, row counts)
4. **load** - Write transformed data to `output/users_transformed.csv`
5. **summarize** - Generate statistics and write `output/summary.json`

## Quick Start

### Prerequisites

- Python 3.12+
- `uv` package manager

### Installation

```bash
# From the examples/asset_graph_etl directory
cd examples/asset_graph_etl

# Install dependencies with file I/O extras
uv pip install -e ".[files]"
```

Or from the project root:

```bash
uv pip install -e ".[files]"
```

### Run the Pipeline

```bash
# Run pipeline once
uv run python pipeline.py --once
```

Expected output:
- `output/users_transformed.csv` - Cleaned and enriched user data
- `output/summary.json` - Pipeline statistics

### View Output

```bash
# Check transformed data
cat output/users_transformed.csv

# View summary
cat output/summary.json
```

## Data Transformations

The pipeline applies the following transformations:

| Field | Transformation |
|--------|----------------|
| **email** | Lowercase and strip whitespace |
| **phone_clean** | Extract digits only from phone number |
| **status** | Normalize to lowercase (active, inactive, pending) |
| **signup_year** | Extract year from signup_date |
| **signup_month** | Extract month from signup_date |
| **customer_tier** | Calculate based on total_spent (gold ≥$500, silver ≥$200, bronze >$0, inactive = $0) |
| **days_since_login** | Days since last login date |

## Data Quality Checks

The `validate` asset runs these checks:

| Check | Description | Threshold |
|--------|-------------|-----------|
| **Row Count** | Ensure sufficient data | 5-1000 rows |
| **Email Not Null** | All users have email | 100% |
| **Email Format** | Valid email format | Regex validation |
| **Status Values** | Valid status values | active, inactive, pending |

If validation fails, the pipeline stops immediately with a detailed error message.

## Pipeline Structure

```
extract (CSV read)
    │
    ▼
transform (clean/enrich)
    │
    ▼
validate (quality checks)
    │
    ▼
load (CSV write)
    │
    ▼
summarize (statistics)
```

## Example Input

`data/users.csv`:
```csv
user_id,name,email,phone,country,status,signup_date,last_login,total_orders,total_spent
1,John Smith,john.smith@example.com,+1-555-0101,USA,active,2024-01-15,2024-01-28,5,529.95
2,Jane Doe,jane.doe@example.com,+1-555-0102,USA,active,2024-01-16,2024-01-27,3,239.97
...
```

## Example Output

`output/users_transformed.csv`:
```csv
user_id,name,email,phone_clean,country,status,customer_tier,signup_year,signup_month,days_since_login,total_orders,total_spent
1,John Smith,john.smith@example.com,15550101,USA,active,gold,2024,1,8,5,529.95
2,Jane Doe,jane.doe@example.com,15550102,USA,active,silver,2024,1,9,3,239.97
...
```

`output/summary.json`:
```json
{
  "total_users": 10,
  "status_distribution": {"active": 6, "inactive": 2, "pending": 2},
  "tier_distribution": {"gold": 2, "silver": 3, "bronze": 3, "inactive": 2},
  "total_revenue": 3194.64,
  "total_orders": 36,
  "average_order_value": 88.74,
  "output_file": "output/users_transformed.csv",
  "generated_at": "2024-01-28T10:30:00"
}
```

## Running Tests

```bash
# Run all tests
uv run pytest tests -q

# Run with verbose output
uv run pytest tests -v

# Run with coverage
uv run pytest tests --cov=examples/asset_graph_etl/pipeline.py
```

## Project Structure

```
examples/asset_graph_etl/
├── README.md                # This file
├── pipeline.py              # Main pipeline implementation
├── .gitignore               # Ignore output files
├── data/                    # Input data
│   └── users.csv
├── output/                  # Generated output (not in git)
│   ├── users_transformed.csv
│   └── summary.json
└── tests/                   # Test suite
    ├── __init__.py
    └── test_pipeline.py
```

## Key Concepts

### PipelineDefinitionContext

The `PipelineDefinitionContext` provides a declarative way to define pipelines using Python decorators:

```python
with PipelineDefinitionContext("my_pipeline") as pipeline:
    @pipeline.asset()
    def extract():
        return read_data()

    @pipeline.asset()
    def transform(extract):  # Dependencies inferred from parameter name
        return clean_data(extract)
```

Dependencies are **automatically inferred** from function parameter names that match existing asset names.

### ExecutionEngine

The `ExecutionEngine` orchestrates the execution of assets in the correct order:

```python
engine = ExecutionEngine()
result = engine.execute(graph)

if result.success:
    print(f"Executed {result.assets_executed} assets")
```

### ValidationSuite

Data quality checks are grouped into a suite:

```python
suite = ValidationSuite(name="quality_checks")
suite.add_check("email_not_null", expect_column_values_to_not_be_null("email"))
suite.add_check("email_format", expect_column_values_to_match_regex("email", pattern))

result = suite.validate(records)
if not result.is_valid:
    raise ValueError("Validation failed")
```

## Customization

### Add New Transformations

Edit the `transform` asset in `pipeline.py`:

```python
@pipeline.asset(description="Clean and enrich user data")
def transform(extract: list[dict]) -> list[dict]:
    for row in extract:
        # Add your custom transformation
        row['full_name'] = f"{row['name']}"
        # ...
```

### Add New Validation Checks

Edit the `validate` asset in `pipeline.py`:

```python
suite.add_check(
    "phone_not_null",
    expect_column_values_to_not_be_null("phone")
)
```

### Change Output Format

Replace `CSVWriter` with `ParquetWriter`:

```python
from vibe_piper.connectors.parquet import ParquetWriter

writer = ParquetWriter(output_path)
count = writer.write(records, schema=output_schema)
```

## Troubleshooting

### Issue: `ModuleNotFoundError: No module named 'vibe_piper'`

**Solution**: Install the package in development mode:
```bash
uv pip install -e ".[files]"
```

### Issue: `FileNotFoundError: data/users.csv`

**Solution**: Ensure you're running from the `examples/asset_graph_etl/` directory, or update `ETLConfig.input_path`.

### Issue: Validation fails with row count error

**Solution**: The sample data has 10 rows. Check that the validation threshold (`min_row_count`) is ≤ 10.

### Issue: "Data validation failed" error

**Solution**: Check the logs for specific validation errors. Common issues:
- Missing email addresses
- Invalid email format
- Invalid status values

## Related Examples

- [ETL Pipeline (PostgreSQL to Parquet)](../etl_pipeline/)
- [API Ingestion Pipeline](../api_ingestion/)
