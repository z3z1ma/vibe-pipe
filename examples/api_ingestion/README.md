# API Ingestion Example

A comprehensive example demonstrating how to build a production-ready data pipeline for ingesting data from a REST API into a PostgreSQL database using Vibe Piper.

## Overview

This example showcases a real-world data pipeline with:

- ✅ **REST API Integration** - Fetch data from any REST API
- ✅ **Automatic Pagination** - Handles multiple pagination strategies
- ✅ **Rate Limiting** - Token bucket algorithm to prevent API throttling
- ✅ **Retry Logic** - Exponential backoff with jitter for failed requests
- ✅ **Data Transformation** - Transform and validate data before loading
- ✅ **Database Loading** - Efficient upserts to PostgreSQL
- ✅ **Quality Reporting** - Comprehensive metrics and error tracking
- ✅ **Error Handling** - Graceful handling of API and database errors

## Architecture

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│  REST API   │ ───> │  Pipeline    │ ───> │  Database   │
│             │      │  (Vibe       │      │  (Postgres) │
│  Paginated  │      │   Piper)     │      │             │
└─────────────┘      └──────────────┘      └─────────────┘
                          │
                          v
                   ┌──────────────┐
                   │  Quality     │
                   │  Report      │
                   └──────────────┘
```

### Pipeline Flow

1. **Fetch** - Retrieve data from REST API with automatic pagination
2. **Transform** - Convert and validate data structure
3. **Load** - Insert/update records in PostgreSQL (upsert)
4. **Report** - Generate quality metrics and error summary

## Project Structure

```
examples/api_ingestion/
├── __init__.py           # Package initialization
├── pipeline.py           # Main pipeline implementation
├── schemas.py            # Data models and validation
├── vibepiper.toml       # Configuration file
├── README.md            # This file
└── tests/
    ├── __init__.py
    ├── conftest.py      # Test fixtures (mock API server)
    └── test_pipeline.py # Integration tests
```

## Quick Start

### 1. Start a PostgreSQL Database

Using Docker:

```bash
docker run -d \
  --name vibe-piper-demo \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=vibe_piper_demo \
  -p 5432:5432 \
  postgres:15
```

### 2. Configure the Pipeline

Edit `vibepiper.toml`:

```toml
[api]
base_url = "https://your-api.com/v1"
api_key = "${API_KEY}"

[database]
host = "localhost"
port = 5432
database = "vibe_piper_demo"
user = "postgres"
password = "${DB_PASSWORD}"

[rate_limiting]
requests_per_second = 10
```

### 3. Run the Pipeline

```bash
cd examples/api_ingestion
python pipeline.py
```

### 4. Check the Results

```bash
psql -h localhost -U postgres -d vibe_piper_demo

SELECT COUNT(*) FROM users;
SELECT * FROM users LIMIT 10;
```

## Usage Examples

### Basic Usage

```python
import asyncio
from vibe_piper.connectors.postgres import PostgreSQLConfig
from examples.api_ingestion.pipeline import APIIngestionPipeline

async def main():
    pipeline = APIIngestionPipeline(
        api_base_url="https://api.example.com/v1",
        api_key="your-api-key",
        rate_limit_per_second=10,
        max_retries=3,
        page_size=100,
    )

    db_config = PostgreSQLConfig(
        host="localhost",
        port=5432,
        database="mydb",
        user="user",
        password="password",
    )
    pipeline.db_config = db_config

    try:
        await pipeline.initialize()
        report = await pipeline.run()
        report.print_summary()
    finally:
        await pipeline.close()

asyncio.run(main())
```

### Dry Run (No Database Insertion)

```python
report = await pipeline.run(dry_run=True)
```

### Limit Pages

```python
report = await pipeline.run(max_pages=5)
```

## Configuration

| Setting | Description | Default |
|---------|-------------|---------|
| `base_url` | API base URL | Required |
| `api_key` | API authentication key | `None` |
| `page_size` | Items per page | `100` |
| `rate_limit_per_second` | Max requests per second | `10` |
| `max_retries` | Maximum retry attempts | `3` |

## Testing

The example includes comprehensive integration tests with a mock API server.

```bash
# Run all tests
pytest examples/api_ingestion/tests/

# Run with coverage
pytest examples/api_ingestion/tests/ --cov=examples/api_ingestion --cov-report=html
```

### Test Features

- ✅ Mock API server with realistic responses
- ✅ Pagination testing
- ✅ Rate limiting verification
- ✅ Error handling scenarios
- ✅ Data transformation validation

## Quality Report

After each run, the pipeline generates a quality report:

```
============================================================
DATA QUALITY REPORT
============================================================
Total Records Processed: 1000
Successful: 987
Failed: 13
Success Rate: 98.70%

API Calls: 10
Pages Fetched: 10
Rate Limit Hits: 0
Retry Attempts: 0

Duration: 15.32 seconds
============================================================
```

## Best Practices

1. **Always configure rate limiting** to match your API's limits
2. **Choose appropriate page sizes** based on your API's performance
3. **Monitor quality reports** for validation errors and rate limit hits
4. **Use dry run mode** for testing without database writes
5. **Implement incremental loads** by tracking update timestamps

## Troubleshooting

### Connection Errors

Check database is running and credentials are correct.

### Rate Limit Errors

Reduce `requests_per_second` in config or increase retry delays.

### Validation Errors

Review `validation_errors` in quality report and adjust validation rules.

## License

MIT License - See LICENSE file in the root directory.
