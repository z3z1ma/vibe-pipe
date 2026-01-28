# ETL Pipeline Example: PostgreSQL → Parquet → Analytics

A comprehensive, production-ready ETL (Extract, Transform, Load) pipeline example demonstrating the Vibe Piper library's capabilities for building data pipelines with database connectors, file I/O, data quality checks, error handling, and incremental loading.

## 🎯 Overview

This example showcases a complete ETL pipeline that:

1. **Extracts** customer data from a PostgreSQL database
2. **Transforms** and validates the data with quality checks
3. **Loads** to partitioned Parquet files optimized for analytics
4. **Supports** incremental loading using watermarks
5. **Handles** errors with retry logic and exponential backoff
6. **Generates** quality reports for monitoring
7. **Schedules** automatic pipeline runs

## 📋 Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Pipeline Flow](#pipeline-flow)
- [Configuration](#configuration)
- [Data Quality](#data-quality)
- [Incremental Loading](#incremental-loading)
- [Error Handling](#error-handling)
- [Scheduling](#scheduling)
- [Testing](#testing)
- [Project Structure](#project-structure)
- [Customization](#customization)
- [Best Practices](#best-practices)

## ✨ Features

### Core Capabilities

- ✅ **PostgreSQL Connector**: Connection pooling, batch queries, transactions
- ✅ **Parquet Output**: Partitioned datasets, compression (snappy, gzip, zstd)
- ✅ **Data Quality**: 30+ validation checks, custom validation suites
- ✅ **Incremental Loading**: Watermark-based change data capture
- ✅ **Error Handling**: Retry logic with exponential backoff
- ✅ **Scheduling**: Interval-based and cron-style scheduling
- ✅ **Monitoring**: Quality reports, metrics collection
- ✅ **Docker Support**: Complete development environment

### Advanced Features

- **Partitioned Output**: Data partitioned by year/month for optimal query performance
- **Schema Validation**: Type checking and schema evolution support
- **Transformation Pipeline**: Data cleaning, enrichment, and normalization
- **Dead Letter Queue**: Failed records captured for analysis
- **Parallel Processing**: Configurable parallel load workers
- **Memory Management**: Chunked reading/writing for large datasets

## 🏗️ Architecture

```
┌─────────────────┐
│   PostgreSQL    │
│  (Source Data)  │
└────────┬────────┘
         │
         │ Extract (with watermark)
         ▼
┌─────────────────────────────────────────┐
│         Transformation Layer             │
│  ┌───────────────────────────────────┐  │
│  │ • Data cleaning                   │  │
│  │ • Schema validation               │  │
│  │ • Enrichment (partition cols)     │  │
│  │ • Normalization                   │  │
│  └───────────────────────────────────┘  │
└────────┬────────────────────────────────┘
         │
         │ Validate (quality checks)
         ▼
┌─────────────────────────────────────────┐
│       Data Quality Suite                │
│  • Email format validation              │
│  • Null proportion checks               │
│  • Status value validation              │
│  • Row count validation                 │
└────────┬────────────────────────────────┘
         │
         │ Load (partitioned)
         ▼
┌─────────────────────────────────────────┐
│      Parquet Files                      │
│  output/customers/                      │
│    ├── year=2024/month=01/              │
│    ├── year=2024/month=02/              │
│    └── year=2024/month=03/              │
└────────┬────────────────────────────────┘
         │
         │ Report
         ▼
┌─────────────────────────────────────────┐
│   Quality Report & Metrics              │
└─────────────────────────────────────────┘
```

## 📦 Prerequisites

### Required Software

- **Python**: 3.12 or higher
- **Docker**: For PostgreSQL databases
- **Docker Compose**: For orchestrating services

### Python Dependencies

```bash
pip install vibe-piper[postgres,files]
```

Or install from the project root:

```bash
pip install -e ".[postgres,files,dev]"
```

## 🚀 Quick Start

### 1. Start the Databases

```bash
# Start PostgreSQL databases
docker-compose up -d

# Verify databases are running
docker-compose ps
```

This starts:
- **Source Database**: `localhost:5432` (customers data)
- **Analytics Database**: `localhost:5433` (for reporting)
- **Adminer** (optional): `localhost:8080` (database UI)

### 2. Run the Pipeline

```bash
# Run once (incremental mode)
python pipeline.py --once

# Run full load (ignore watermark)
python pipeline.py --full --once

# Run with scheduling (every 60 minutes)
python pipeline.py --interval 60
```

### 3. Check the Output

```bash
# View output directory
ls -la output/

# View Parquet files (requires pyarrow)
python -c "import pandas as pd; df = pd.read_parquet('output/customers/'); print(df.head())"

# Read quality report
cat output/quality_report.txt
```

## 🔄 Pipeline Flow

### Step 1: Extract

```python
# Reads from PostgreSQL with retry logic
# Supports incremental loading via watermark
@retry_on_failure(max_retries=3, delay=5)
def _extract_data():
    if incremental and watermark_exists:
        query = "SELECT * FROM customers WHERE updated_at > %s"
    else:
        query = "SELECT * FROM customers"
    return connector.query(query)
```

**Features**:
- Connection pooling for efficiency
- Retry on connection failure
- Incremental extraction using watermarks
- Batch processing for large datasets

### Step 2: Transform

```python
# Cleans and enriches data
def _transform_data(raw_data):
    for row in raw_data:
        # Add partition columns
        row['year'] = updated_at.year
        row['month'] = f"{updated_at.month:02d}"

        # Clean email
        row['email'] = row['email'].lower().strip()

        # Clean phone
        row['phone_clean'] = extract_digits(row['phone'])

        # Normalize status
        row['status'] = row['status'].lower()
```

**Transformations**:
- **Partition Columns**: `year`, `month` from `updated_at`
- **Email**: Lowercase, strip whitespace
- **Phone**: Extract digits only
- **Status**: Normalize to lowercase
- **Dates**: Parse and normalize datetime formats

### Step 3: Validate

```python
# Data quality checks
validation_suite = ValidationSuite()
validation_suite.add_check(expect_column_values_to_not_be_null("customer_id"))
validation_suite.add_check(expect_column_values_to_match_regex("email", email_pattern))
validation_suite.add_check(expect_column_proportion_of_nulls_to_be_between("phone", 0.0, 0.1))

result = validation_suite.validate(records)
if not result.is_valid:
    raise DataQualityError(result.errors)
```

**Quality Checks**:
- ✅ Customer ID is never null
- ✅ Email matches regex pattern
- ✅ Status in allowed values
- ✅ Phone nulls ≤ 10%
- ✅ Dates are parseable
- ✅ Minimum row count met

### Step 4: Load

```python
# Write to partitioned Parquet files
writer = ParquetWriter("output/customers")
file_paths = writer.write_partitioned(
    records,
    partition_cols=["year", "month"],
    compression="snappy"
)
```

**Output Structure**:
```
output/customers/
├── year=2024/
│   ├── month=01/
│   │   └── part-00001.parquet
│   └── month=02/
│       └── part-00002.parquet
└── year=2025/
    └── month=01/
        └── part-00003.parquet
```

## ⚙️ Configuration

### Environment Variables

```bash
# PostgreSQL connection
export PG_HOST=localhost
export PG_PORT=5432
export PG_DATABASE=source_db
export PG_USER=etl_user
export PG_PASSWORD=etl_password

# Or use .env file
```

### Configuration File (`vibepiper.toml`)

```toml
[pipeline]
name = "customer_etl"

[source]
type = "postgresql"
host = "localhost"
port = 5432
database = "source_db"
table = "customers"

[source.incremental]
enabled = true
watermark_column = "updated_at"

[output]
type = "parquet"
directory = "output"
compression = "snappy"

[output.partitioning]
enabled = true
columns = ["year", "month"]

[quality]
min_row_count = 100
max_null_proportion = 0.1

[error_handling]
max_retries = 3
retry_delay = 5
```

## 🔍 Data Quality

### Validation Checks

The pipeline includes comprehensive data quality checks:

| Check | Description | Threshold |
|-------|-------------|-----------|
| **Row Count** | Ensure minimum rows extracted | ≥ 100 |
| **Customer ID** | Never null, unique | 100% |
| **Email** | Valid format, not null | 100% |
| **Phone** | Null proportion ≤ 10% | ≤ 10% |
| **Status** | In allowed values | 100% |
| **Dates** | Parseable as datetime | 100% |

### Quality Report

After each run, a quality report is generated:

```
ETL Pipeline Quality Report
==================================================

Start Time: 2024-01-28 10:00:00
End Time: 2024-01-28 10:00:15
Duration: 15.23 seconds

Row Counts:
  Extracted: 1000
  Transformed: 995
  Loaded: 995

Quality Metrics:
  Validation Errors: 0
  Transformation Success Rate: 99.5%

Watermark: 2024-01-28T10:00:00

Validation Checks:
  ✓ expect_table_row_count_to_be_between
  ✓ expect_column_values_to_not_be_null (customer_id)
  ✓ expect_column_values_to_not_be_null (email)
  ✓ expect_column_proportion_of_nulls_to_be_between (phone)
  ✓ expect_column_values_to_match_regex (email)
  ✓ expect_column_values_to_be_in_set (status)
  ✓ expect_column_values_to_be_dateutil_parseable (created_at)
  ✓ expect_column_values_to_be_dateutil_parseable (updated_at)
```

## 🔄 Incremental Loading

### How It Works

1. **First Run**: Full load, watermark = max `updated_at`
2. **Subsequent Runs**: Only load rows where `updated_at` > watermark
3. **Update Watermark**: Set watermark = max `updated_at` from current load

### Watermark File

```python
# Stored in output/watermark.txt
2024-01-28T10:00:00
```

### Enable/Disable

```bash
# Incremental (default)
python pipeline.py --once

# Full load (ignore watermark)
python pipeline.py --full --once
```

## 🛡️ Error Handling

### Retry Logic

```python
@retry_on_failure(max_retries=3, delay=5)
def extract_data():
    # Automatically retries on failure
    # Exponential backoff: 5s, 10s, 20s
    return connector.query(query)
```

### Error Scenarios

| Error Type | Handling | Retry |
|------------|----------|-------|
| Connection Error | Log warning, retry | Yes (3x) |
| Query Timeout | Log warning, retry | Yes (3x) |
| Validation Error | Stop pipeline, log errors | No |
| Transform Error | Skip record, log warning | No |
| Load Error | Retry with backoff | Yes (3x) |

### Dead Letter Queue

```python
# Failed records go to output/errors/
# Can be analyzed and re-processed later
```

## ⏰ Scheduling

### Interval-Based

```python
scheduler = ETLScheduler(pipeline, interval_minutes=60)
scheduler.start()  # Runs every hour
```

### Manual Control

```python
scheduler = ETLScheduler(pipeline)
scheduler.run_once()  # Run immediately
```

### Production Deployment

Use cron, systemd, or your orchestrator:

```cron
# Crontab example
0 * * * * cd /path/to/etl_pipeline && python pipeline.py --once
```

## 🧪 Testing

### Run Tests

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest --cov=examples/etl_pipeline tests/

# Run specific test
pytest tests/test_pipeline.py::test_extract_data
```

### Test Coverage

- ✅ Unit tests for each pipeline step
- ✅ Integration tests with PostgreSQL
- ✅ Validation tests
- ✅ Error handling tests
- ✅ Incremental loading tests

### Mock Database

Tests use pytest fixtures to mock PostgreSQL:

```python
@pytest.fixture
def mock_pg_connector():
    # Returns mocked connector with sample data
    yield MockPostgreSQLConnector()
```

## 📁 Project Structure

```
examples/etl_pipeline/
├── README.md                 # This file
├── vibepiper.toml           # Pipeline configuration
├── pipeline.py              # Main ETL pipeline implementation
├── schemas.py               # Data schema definitions
├── docker-compose.yml       # PostgreSQL databases
├── scripts/
│   ├── init_source_db.sql       # Source database setup
│   └── init_analytics_db.sql    # Analytics database setup
├── tests/
│   └── test_pipeline.py     # Integration tests
└── output/                  # Generated output (not in git)
    ├── customers/           # Partitioned Parquet files
    │   ├── year=2024/month=01/
    │   └── year=2024/month=02/
    ├── watermark.txt        # Incremental loading watermark
    ├── quality_report.txt   # Data quality report
    └── errors/              # Failed records
```

## 🎨 Customization

### Adding New Transformations

```python
def _transform_data(self, raw_data):
    for row in raw_data:
        # Add your custom transformation
        row['full_name'] = f"{row['first_name']} {row['last_name']}"

        # Calculate derived fields
        row['is_high_value'] = row.get('total_spent', 0) > 1000

        transformed.append(row)
    return transformed
```

### Adding New Validation Checks

```python
def _create_validation_suite(self):
    suite = ValidationSuite()

    # Add custom check
    suite.add_check(
        expect_column_values_to_be_between("total_orders", 0, 1000),
        description="Order count must be reasonable"
    )

    # Add custom validation
    def custom_check(records):
        # Your custom validation logic
        return ValidationResult(is_valid=True)

    suite.add_check(custom_check)
    return suite
```

### Changing Partitioning

```python
# In pipeline.py, modify:
self.config.partition_cols = ["year", "month", "country"]

# Or for daily partitions:
self.config.partition_cols = ["year", "month", "day"]
```

### Adding to Analytics Database

```python
def _load_to_analytics_db(self, data):
    # Load to analytics database for reporting
    analytics_config = PostgreSQLConfig(
        host="localhost",
        port=5433,
        database="analytics_db",
        user="analytics_user",
        password="analytics_password"
    )

    with PostgreSQLConnector(analytics_config) as conn:
        conn.execute_batch(
            "INSERT INTO analytics.customers_analytics VALUES (%s, %s, ...)",
            params_list=data
        )
```

## 📚 Best Practices

### Production Tips

1. **Monitor Performance**: Track pipeline duration and row counts
2. **Set Alerts**: Notify on validation failures or long runtimes
3. **Version Schemas**: Use schema versioning for backward compatibility
4. **Backfill Strategy**: Have a plan for reloading historical data
5. **Resource Limits**: Set memory and connection pool limits
6. **Idempotency**: Ensure pipeline can be re-run safely
7. **Audit Logs**: Keep track of all pipeline runs

### Performance Optimization

```python
# Use connection pooling
pg_config.pool_size = 10

# Use batch processing
batch_size = 10000

# Use partitioned output
partition_cols = ["year", "month", "day"]

# Use compression
compression = "zstd"  # Better compression than snappy

# Parallel processing (for multiple tables)
from concurrent.futures import ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [executor.submit(process_table, t) for t in tables]
```

### Data Quality

```python
# Always validate before loading
if not validation_passed:
    raise DataQualityError("Validation failed")

# Log data quality metrics
logger.info(f"Null proportion: {null_proportion:.2%}")
logger.info(f"Valid emails: {valid_email_count}/{total_count}")

# Sample and inspect data
sample_df = pd.read_parquet(output_path)
print(sample_df.describe())
```

## 🤝 Contributing

This is an example project. For contributions to Vibe Piper:

1. Check the main project repository
2. Follow contribution guidelines
3. Add tests for new features
4. Update documentation

## 📄 License

This example is part of Vibe Piper and follows the same license.

## 🔗 Related Examples

- [REST API Integration](../rest_api_integration/)
- [Data Validation](../data_validation/)
- [Streaming Pipeline](../streaming_pipeline/)

## 🆘 Troubleshooting

### Common Issues

**Issue**: `psycopg2.OperationalError: could not connect`
- **Solution**: Ensure Docker containers are running (`docker-compose up -d`)

**Issue**: `ValidationErrors: Data quality validation failed`
- **Solution**: Check `output/quality_report.txt` for details, adjust thresholds in config

**Issue**: `No data extracted`
- **Solution**: Verify watermark, try `--full` flag, check source database

**Issue**: `Permission denied writing to output/`
- **Solution**: Check directory permissions, ensure `output/` is writable

### Debug Mode

```bash
# Enable debug logging
python pipeline.py --once

# Set log level in code
logging.basicConfig(level=logging.DEBUG)
```

## 📞 Support

- **Documentation**: See main Vibe Piper docs
- **Issues**: Report via GitHub issues
- **Discussions**: Use GitHub Discussions for questions

---

**Built with ❤️ using Vibe Piper**
