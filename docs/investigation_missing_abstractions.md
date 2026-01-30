# Investigation: Missing Abstractions in Vibe Piper

**Date:** 2026-01-29
**Ticket:** vp-5299
**Status:** In Progress

---

## Executive Summary

Vibe Piper has strong foundational components (decorators, types, connectors, validation) but provides **little to no abstraction value** in real-world scenarios. The API ingestion and ETL examples demonstrate that users are still writing substantial amounts of boilerplate code:

- Manual SQL DDL generation
- Manual field mapping between sources and sinks
- Manual quality reporting
- Manual retry and error handling logic
- No integration of existing library components (@asset, PipelineBuilder, expectations)

**Result:** 365+ lines of code for a simple API→DB pipeline.

---

## 1. Analysis of Current State

### 1.1 Existing Library Components (Strong Foundation)

The library has excellent building blocks:

| Component | Status | Description |
|-----------|--------|-------------|
| **@asset decorator** | ✅ Implemented | Declarative asset definition with automatic dependency inference |
| **PipelineBuilder** | ✅ Implemented | Fluent API for building asset graphs |
| **AssetGraph** | ✅ Implemented | DAG with topological ordering, lineage tracking |
| **Schema / SchemaField** | ✅ Implemented | Type-safe schema definitions |
| **DataRecord** | ✅ Implemented | Schema-validated records |
| **Expectation** | ✅ Implemented | Declarative quality rules |
| **ValidationSuite** | ✅ Implemented | Composable validation checks |
| **IOManager protocol** | ✅ Implemented | Storage abstraction (memory, file, db, S3) |
| **Materialization strategies** | ✅ Implemented | TABLE, VIEW, FILE, INCREMENTAL |
| **Connectors** | ✅ Implemented | PostgreSQL, MySQL, Snowflake, BigQuery, REST, CSV, JSON, Parquet |
| **Quality checks** | ✅ Implemented | Completeness, validity, uniqueness, etc. |

### 1.2 What Examples Actually Do

#### API Ingestion Example (365 lines)

```python
# Manual SQL DDL
create_table_sql = """
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) NOT NULL UNIQUE,
        ...
    );
"""

# Manual UPSERT SQL
upsert_sql = """
    INSERT INTO users (...) VALUES (...)
    ON CONFLICT (email) DO UPDATE SET ...
"""

# Manual field mapping
user_dict = {
    "user_id": self.id,
    "name": self.name,
    "email": self.email,
    "company_name": self.company.get("name") if self.company else None,
    "city": self.address.get("city") if self.address else None,
}

# Manual validation
if not user_dict.get("name") or not user_dict.get("email"):
    error = {"user_id": user.id, "error": "Missing required field"}
    self._validation_errors.append(error)
    return None

# Manual quality report
@dataclass
class QualityReport:
    total_records: int
    successful_records: int
    failed_records: int
    validation_errors: list[dict[str, Any]]
    api_calls: int
    pages_fetched: int
    ...
```

**Issues:**
1. ❌ Manual SQL DDL - should be auto-generated from Schema
2. ❌ Manual UPSERT SQL - should be auto-generated from schema + conflict key
3. ❌ Manual field mapping - should use declarative mapping rules
4. ❌ Manual validation - should use Expectations/ValidationSuite
5. ❌ Manual quality metrics - should be auto-tracked by execution engine
6. ❌ No use of @asset decorator
7. ❌ No use of PipelineBuilder
8. ❌ No integration with IO managers

---

## 2. Missing Abstractions

### 2.1 High-Priority Missing Abstractions

#### A. Source/Sink Abstractions

**Missing:** Declarative source and sink definitions

**Current State:**
- Users manually instantiate connectors (RESTClient, PostgreSQLConnector)
- Write manual fetch/store logic
- Handle pagination manually
- Handle authentication manually

**What We Need:**

```python
@source(
    type="api",
    endpoint="/users",
    pagination=OffsetPagination(items_path="data", ...),
    authentication="Bearer ${API_KEY}",
    rate_limit=RateLimit(requests=10, window_seconds=1),
    schema=UserSchema,
)
def users_source(ctx):
    """Source users from REST API."""
    pass

@sink(
    type="database",
    connection="postgres://...",
    schema=UserSchema,
    materialization="table",
    upsert_key="email",  # Auto-generate ON CONFLICT DO UPDATE
)
def users_sink(data, ctx):
    """Sink users to PostgreSQL."""
    pass
```

**Benefits:**
- Auto-generate SQL DDL from schema
- Auto-generate INSERT/UPSERT queries
- Built-in retry, rate limiting, pagination
- Declarative authentication configuration

---

#### B. Schema-First Pipeline Definition

**Missing:** Auto-DDL, auto-mapping from schema

**Current State:**
- Schema exists but not used for DDL generation
- Users manually map API fields to DB columns
- No automatic type conversion

**What We Need:**

```python
# Define schema once
UserSchema = Schema(
    name="users",
    fields=[
        SchemaField(name="user_id", data_type=DataType.INTEGER, primary_key=True),
        SchemaField(name="name", data_type=DataType.STRING, required=True),
        SchemaField(name="email", data_type=DataType.STRING, unique=True, required=True),
        SchemaField(name="company_name", data_type=DataType.STRING, source_path="company.name"),
        SchemaField(name="city", data_type=DataType.STRING, source_path="address.city"),
    ]
)

# Use schema for both source and sink
@source(schema=UserSchema, endpoint="/users")
def fetch_users():
    pass

@sink(schema=UserSchema, table="users", upsert_key="email")
def store_users(data):
    pass

# Auto-mapping happens:
# API: {"company": {"name": "Acme Corp"}} → DB: company_name
# API: {"address": {"city": "SF"}} → DB: city
```

**Benefits:**
- Single source of truth for schema
- Automatic field mapping via `source_path` annotations
- Auto-DDL generation for target tables
- Type-safe transformations

---

#### C. Declarative Transformations

**Missing:** Built-in transformation operators

**Current State:**
- Users write manual transformation functions
- Manual field extraction and validation
- No reusable transformation library

**What We Need:**

```python
from vibe_piper.transformations import (
    map_fields,         # Map field names
    extract_fields,      # Extract nested fields
    validate_fields,     # Validate against schema
    filter_rows,        # Filter by condition
    enrich_from_lookup,  # Join with lookup table
    compute_field,       # Add computed fields
)

@transform
def clean_users(users):
    """Clean and validate users."""
    pipeline = (
        users
        .pipe(extract_fields({"company_name": "company.name", "city": "address.city"}))
        .pipe(validate_fields(UserSchema))
        .pipe(filter_rows(lambda row: row["email"] and "@" in row["email"]))
        .pipe(compute_field("category", lambda row: "premium" if row["age"] > 30 else "standard"))
    )
    return pipeline
```

**Benefits:**
- Composable, chainable transformations
- Reusable transformation library
- Type-safe with schema validation

---

#### D. Built-in Quality/Monitoring

**Missing:** Auto-tracking of metrics, drift detection, alerting

**Current State:**
- Users manually track metrics (api_calls, pages_fetched, errors)
- Manual quality report generation
- No drift detection integration
- No alerting/thresholds

**What We Need:**

```python
@asset(
    expectations=[
        expect_column_values_to_not_be_null("email"),
        expect_column_values_to_match_regex("email", pattern="..."),
        expect_table_row_count_to_be_between(min=100, max=1000000),
    ],
    drift_detection={
        "enabled": True,
        "baseline_window": "7d",
        "threshold": 0.1,  # PSI threshold
    },
    monitoring={
        "metrics": ["row_count", "null_count", "avg_age"],
        "alert_on": {
            "row_count": {"condition": "decrease", "threshold": 0.1},
            "null_count": {"condition": "increase", "threshold": 0.05},
        }
    }
)
def validated_users(raw_users):
    """Users with auto-tracking of quality and drift."""
    return raw_users
```

**Benefits:**
- Automatic quality metrics collection
- Built-in drift detection
- Alerting on metric anomalies
- No manual tracking code

---

#### E. Configuration-Driven Pipelines (TOML/YAML)

**Missing:** Define pipelines declaratively via config files

**What We Need:**

```toml
# vibepiper.toml
[project]
name = "user_ingestion"
version = "0.1.0"

[[sources]]
name = "users_api"
type = "api"
endpoint = "/users"
authentication = { type = "bearer", from = "env.API_KEY" }
pagination = { type = "offset", items_path = "data", limit_param = "limit" }
rate_limit = { requests = 10, window_seconds = 1 }
schema = { file = "schemas/users.py::UserSchema" }

[[sinks]]
name = "users_db"
type = "postgres"
connection = "postgres://user:pass@localhost:5432/db"
table = "users"
schema = { file = "schemas/users.py::UserSchema" }
upsert_key = "email"
materialization = "table"

[[transforms]]
name = "clean_users"
source = "users_api"
steps = [
    { type = "extract_fields", mappings = {"company_name": "company.name", "city": "address.city"} },
    { type = "validate", schema = { file = "schemas/users.py::UserSchema" } },
    { type = "filter", condition = "email is not null" },
]

[[expectations]]
asset = "clean_users"
checks = [
    { type = "not_null", column = "email" },
    { type = "regex", column = "email", pattern = "^[^@]+@[^@]+$" },
]

[[jobs]]
name = "ingest_users"
schedule = "0 * * * *"  # Every hour
sources = ["users_api"]
sinks = ["users_db"]
transforms = ["clean_users"]
expectations = ["email_validity"]
```

**Benefits:**
- No code for simple pipelines
- Version-controlled pipeline definitions
- Easy deployment (copy config file)
- Non-technical users can modify pipelines

---

### 2.2 Medium-Priority Missing Abstractions

#### F. Incremental Loading Abstraction

**Missing:** Declarative watermark/incremental loading

**Current State:**
- Users manually track watermarks
- Manual incremental queries
- No library support

**What We Need:**

```python
@source(
    incremental=True,
    watermark_column="updated_at",
    watermark_store="file://watermark.txt",
)
def fetch_users():
    """Auto-incremental fetch based on updated_at."""
    pass  # Library handles watermark automatically
```

**Benefits:**
- Automatic watermark tracking
- Declarative incremental loading
- No manual watermark logic

---

#### G. Asset Versioning & Lineage

**Missing:** Automatic versioning, lineage tracking

**What We Need:**

```python
@asset(version="2.0", checksum=True)
def users_v2():
    """Asset with automatic versioning."""
    pass

# Query lineage
lineage = graph.get_lineage("users_v2")
# Returns: {"users_v2": ["users_v1", "raw_users"]}
```

**Benefits:**
- Track asset versions
- Understand data dependencies
- Rollback capability

---

#### H. Streaming & Real-Time Support

**Missing:** Streaming operators, windowing

**What We Need:**

```python
@stream_source(type="kafka", topic="users")
def user_events():
    """Stream user events from Kafka."""
    pass

@transform(window="5m", aggregation="count")
def user_count(stream):
    """Count users in 5-minute windows."""
    pass
```

**Benefits:**
- Real-time pipeline support
- Windowed aggregations
- Event-driven architecture

---

## 3. Design Proposals

### 3.1 Source Abstraction Design

```python
from vibe_piper.sources import APISource, DatabaseSource, FileSource

# REST API source
users_source = APISource(
    name="users_api",
    base_url="https://api.example.com/v1",
    endpoint="/users",
    authentication=BearerAuth("${API_KEY}"),
    pagination=OffsetPagination(
        items_path="data",
        limit_param="limit",
        offset_param="offset",
    ),
    rate_limit=RateLimit(requests=10, window_seconds=1),
    retry=RetryConfig(max_attempts=3, exponential_backoff=True),
)

# Database source
customers_source = DatabaseSource(
    name="customers_db",
    connection="postgres://...",
    table="customers",
    query="SELECT * FROM customers WHERE updated_at > %s",
    incremental=True,
    watermark_column="updated_at",
)

# File source
logs_source = FileSource(
    name="logs_file",
    path="data/logs/*.jsonl",
    format="jsonl",
    schema=LogSchema,
)
```

**Key Features:**
- Declarative configuration
- Built-in retry, rate limiting, pagination
- Incremental loading support
- Schema-aware parsing

---

### 3.2 Sink Abstraction Design

```python
from vibe_piper.sinks import DatabaseSink, FileSink, S3Sink

# PostgreSQL sink
users_sink = DatabaseSink(
    name="users_db",
    connection="postgres://...",
    table="users",
    schema=UserSchema,
    materialization="table",
    upsert_key="email",  # Auto-generate ON CONFLICT
    batch_size=1000,
)

# Parquet sink
analytics_sink = FileSink(
    name="analytics_parquet",
    path="s3://bucket/analytics/",
    format="parquet",
    partition_cols=["year", "month"],
    compression="snappy",
)

# S3 sink
backup_sink = S3Sink(
    name="backup_s3",
    bucket="backup-bucket",
    prefix="users/",
    format="jsonl",
)
```

**Key Features:**
- Auto-DDL generation from schema
- Auto-INSERT/UPSERT query generation
- Batch processing support
- Multiple materialization strategies

---

### 3.3 Schema-First Mapping Design

```python
from vibe_piper.schema import Schema, SchemaField, FieldMapping

UserSchema = Schema(
    name="users",
    fields=[
        SchemaField(name="id", data_type=DataType.INTEGER, primary_key=True),
        SchemaField(name="name", data_type=DataType.STRING, required=True),
        SchemaField(name="email", data_type=DataType.STRING, required=True, unique=True),
        SchemaField(
            name="company_name",
            data_type=DataType.STRING,
            source_path="company.name",  # API: company.name → DB: company_name
        ),
        SchemaField(
            name="city",
            data_type=DataType.STRING,
            source_path="address.city",  # API: address.city → DB: city
        ),
    ],
)

# Auto-mapping happens automatically:
# Source: {"id": 1, "name": "John", "company": {"name": "Acme"}, "address": {"city": "SF"}}
# Target: {"id": 1, "name": "John", "email": "john@example.com", "company_name": "Acme", "city": "SF"}
```

**Key Features:**
- Declarative field mapping via `source_path`
- Automatic type conversion
- Schema validation at source and sink
- Single source of truth

---

### 3.4 Declarative Transformations Design

```python
from vibe_piper.transform import pipeline as T

@transform
def clean_users(raw_users):
    """Clean and validate user records."""
    return (
        T.from_source(raw_users, schema=UserSchema)
        .pipe(T.extract_fields({
            "company_name": "company.name",
            "city": "address.city",
        }))
        .pipe(T.validate(schema=UserSchema))
        .pipe(T.filter(lambda row: row["email"] and "@" in row["email"]))
        .pipe(T.compute_field("category", lambda row: "premium" if row["age"] > 30 else "standard"))
        .pipe(T.enrich_from_lookup(
            lookup="categories",
            key="category_id",
            fields=["category_name", "discount_rate"]
        ))
    )

# Equivalent to manual code:
# transformed = []
# for user in raw_users:
#     company_name = user["company"]["name"] if user["company"] else None
#     city = user["address"]["city"] if user["address"] else None
#     if "@" not in user["email"]: continue
#     category = "premium" if user["age"] > 30 else "standard"
#     # ... manual enrichment
#     transformed.append({...})
```

**Key Features:**
- Fluent, chainable API
- Schema-aware transformations
- Built-in validation
- Type-safe operations

---

### 3.5 Configuration-Driven Pipeline Design

```toml
# pipeline.toml
[pipeline]
name = "user_ingestion"
version = "1.0.0"

[sources.users_api]
type = "api"
base_url = "https://api.example.com/v1"
endpoint = "/users"

[sources.users_api.auth]
type = "bearer"
from_env = "API_KEY"

[sources.users_api.pagination]
type = "offset"
items_path = "data"
limit_param = "limit"
offset_param = "offset"

[sources.users_api.rate_limit]
requests = 10
window_seconds = 1

[sinks.users_db]
type = "postgres"
connection = "postgres://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:5432/${DB_NAME}"
table = "users"
schema = "schemas::UserSchema"
upsert_key = "email"
batch_size = 1000

[transforms.clean_users]
source = "users_api"
steps = [
    { type = "extract_fields", mappings = { "company_name": "company.name", "city": "address.city" } },
    { type = "validate", schema = "schemas::UserSchema" },
    { type = "filter", condition = "email is not null" },
    { type = "filter", condition = "email contains '@'" },
    { type = "compute_field", name = "ingested_at", value = "now()" },
]

[expectations.quality_checks]
asset = "clean_users"
checks = [
    { type = "not_null", column = "email" },
    { type = "regex", column = "email", pattern = "^[^@]+@[^@]+$" },
    { type = "row_count", min = 100, max = 1000000 },
]

[jobs.ingest_users]
schedule = "0 * * * *"  # Every hour
sources = ["users_api"]
sinks = ["users_db"]
transforms = ["clean_users"]
expectations = ["quality_checks"]
```

**Key Features:**
- Declarative pipeline definition
- No code for simple pipelines
- Environment variable interpolation
- Scheduling support

---

## 4. Integration Architecture

### 4.1 How Components Fit Together

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Configuration Layer                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │  TOML Config │  │ YAML Config  │  │  Python API  │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   Source/Sink Layer                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │   API Source │  │  DB Source   │  │  File Source  │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │  DB Sink     │  │  File Sink    │  │  S3 Sink      │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│              Transformation Layer                             │
│  ┌──────────────────────────────────────────────────┐        │
│  │  Extract Fields │ Validate │ Filter │ Compute │ │
│  └──────────────────────────────────────────────────┘        │
│                    │ (fluent pipeline)                       │
└─────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│              Quality & Monitoring Layer                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │ Expectations │  │  Drift Detect │  │  Monitoring   │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   Execution Layer                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │ AssetGraph   │  │ PipelineBuilder│  │ Executor     │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
│  ┌──────────────┐  ┌──────────────┐                     │
│  │ IO Manager   │  │  Materialization│                     │
│  └──────────────┘  └──────────────┘                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 4.2 Execution Flow

```python
# 1. Load configuration
config = load_config("pipeline.toml")

# 2. Build asset graph
builder = PipelineBuilder("user_ingestion")

# 3. Register sources
users_source = APISource.from_config(config.sources["users_api"])
builder.asset("raw_users", fn=users_source.fetch, schema=UserSchema)

# 4. Register transforms
clean_users = TransformPipeline.from_config(config.transforms["clean_users"])
builder.asset("clean_users", fn=clean_users.execute, depends_on=["raw_users"])

# 5. Register sinks
users_sink = DatabaseSink.from_config(config.sinks["users_db"])
builder.asset("users", fn=users_sink.store, depends_on=["clean_users"])

# 6. Register expectations
for check in config.expectations["quality_checks"].checks:
    builder.add_expectation("clean_users", Expectation.from_config(check))

# 7. Build graph
graph = builder.build()

# 8. Execute
result = execute_graph(graph)

# 9. Auto-DDL, auto-mapping, auto-metrics all happen
```

---

## 5. Before vs After Comparison

### 5.1 Before: Manual API Ingestion (365 lines)

```python
class APIIngestionPipeline:
    def _create_users_table(self):
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL UNIQUE,
                ...
            );
        """
        self.db_connector.execute(create_table_sql)

    async def fetch_users(self):
        raw_users = []
        async for user_data in fetch_all_pages(...):
            raw_users.append(user_data)
        users = [UserResponse.from_dict(user) for user in raw_users]
        return users

    def transform_user(self, user: UserResponse):
        user_dict = user.to_database_dict()
        if not user_dict.get("name") or not user_dict.get("email"):
            error = {"user_id": user.id, "error": "Missing required field"}
            self._validation_errors.append(error)
            return None
        email = user_dict.get("email", "")
        if "@" not in email or "." not in email:
            error = {"user_id": user.id, "error": f"Invalid email: {email}"}
            self._validation_errors.append(error)
            return None
        user_dict["ingested_at"] = datetime.now(UTC)
        return user_dict

    def load_users(self, users: list[dict]):
        for user_dict in users:
            upsert_sql = """
                INSERT INTO users (...) VALUES (...)
                ON CONFLICT (email) DO UPDATE SET ...
            """
            self.db_connector.execute(upsert_sql, user_dict)

    async def run(self) -> QualityReport:
        users = await self.fetch_users()
        transformed = [self.transform_user(u) for u in users if self.transform_user(u) is not None]
        load_results = self.load_users(transformed)
        report = QualityReport(...)
        return report
```

**Problems:**
- ❌ 365+ lines of boilerplate
- ❌ Manual SQL DDL
- ❌ Manual UPSERT SQL
- ❌ Manual field mapping
- ❌ Manual validation
- ❌ Manual retry/pagination/rate limiting logic
- ❌ Manual quality reporting

---

### 5.2 After: Declarative Pipeline (~30 lines)

```python
from vibe_piper import asset, APISource, DatabaseSink, expect
from vibe_piper.transform import pipeline as T

# Define schema (one source of truth)
UserSchema = Schema(
    name="users",
    fields=[
        SchemaField(name="user_id", data_type=DataType.INTEGER, primary_key=True),
        SchemaField(name="name", data_type=DataType.STRING, required=True),
        SchemaField(name="email", data_type=DataType.STRING, unique=True, required=True),
        SchemaField(name="company_name", data_type=DataType.STRING, source_path="company.name"),
        SchemaField(name="city", data_type=DataType.STRING, source_path="address.city"),
    ]
)

# Source: Auto-fetch with pagination, retry, rate limiting
users_source = APISource(
    base_url="https://api.example.com/v1",
    endpoint="/users",
    authentication=BearerAuth("${API_KEY}"),
    pagination=OffsetPagination(items_path="data", limit_param="limit"),
)

# Transform: Auto-mapping + auto-validation
@transform
def clean_users(raw_users):
    return (
        T.from_source(raw_users, schema=UserSchema)
        .pipe(T.extract_fields({
            "company_name": "company.name",
            "city": "address.city",
        }))
        .pipe(T.validate(schema=UserSchema))
        .pipe(T.filter(lambda row: "@" in row["email"]))
    )

# Sink: Auto-DDL + auto-UPSERT
users_sink = DatabaseSink(
    connection="postgres://localhost:5432/vibe_piper_demo",
    table="users",
    schema=UserSchema,
    upsert_key="email",
    batch_size=1000,
)

# Asset: Auto-quality + auto-metrics
@asset(
    expectations=[
        expect_column_values_to_not_be_null("email"),
        expect_column_values_to_match_regex("email", pattern="^[^@]+@[^@]+$"),
    ],
)
def ingest_users(raw_users, clean_users, ctx):
    """Ingest users with full automation."""
    return users_sink.store(clean_users)

# Build and execute
pipeline = build_pipeline("user_ingestion")
    .asset("raw_users", users_source.fetch)
    .asset("clean_users", clean_users)
    .asset("users", ingest_users, depends_on=["clean_users"])

result = pipeline.execute()
```

**Benefits:**
- ✅ ~30 lines (12x less code)
- ✅ Auto-DDL from schema
- ✅ Auto-UPSERT from schema
- ✅ Auto field mapping
- ✅ Built-in retry/pagination/rate limiting
- ✅ Auto-validation via expectations
- ✅ Auto-metrics collection
- ✅ Type-safe throughout
- ✅ Clear intent: what vs how

---

## 6. Implementation Roadmap

### Phase 1: Core Abstractions (Week 1-2)

1. **Source Abstraction** (`vibe_piper/sources/`)
   - `APISource` with REST client integration
   - `DatabaseSource` with connector integration
   - `FileSource` with connector integration
   - Declarative config (authentication, pagination, rate limiting)

2. **Sink Abstraction** (`vibe_piper/sinks/`)
   - `DatabaseSink` with auto-DDL/UPSERT
   - `FileSink` with partitioning
   - `S3Sink` with batching

3. **Schema-First Mapping** (`vibe_piper/schema/mapping.py`)
   - `source_path` field annotation
   - Auto-mapping logic
   - Type conversion

**Success Criteria:**
- Can define API→DB pipeline in ~30 lines
- Auto-DDL generation working
- Auto-UPSERT generation working

---

### Phase 2: Transformations (Week 3)

1. **Transformation Library** (`vibe_piper/transformations/pipeline.py`)
   - Fluent API (`pipe()`, `filter()`, `map()`, `validate()`)
   - Built-in transformations
   - Type-safe with schema

2. **Transformation DSL** (`vibe_piper/transformations/dsl.py`)
   - Config-driven transformations
   - YAML/TOML parsing for transforms

**Success Criteria:**
- Fluent pipeline API working
- 10+ built-in transformations
- Config-driven transforms working

---

### Phase 3: Configuration (Week 4)

1. **Config Loader** (`vibe_piper/config/pipeline.py`)
   - TOML/YAML parser
   - Environment variable interpolation
   - Schema validation of config

2. **Pipeline Generator** (`vibe_piper/config/generator.py`)
   - Generate @asset decorators from config
   - Build asset graphs automatically

**Success Criteria:**
- Can define pipeline via TOML
- No code required for simple pipelines
- Environment variable interpolation working

---

### Phase 4: Quality & Monitoring (Week 5-6)

1. **Auto-Metrics** (`vibe_piper/execution/metrics.py`)
   - Track row counts, null counts, etc.
   - Store in validation history
   - Expose via dashboard

2. **Drift Detection Integration** (`vibe_piper/monitoring/drift.py`)
   - Auto-baseline capture
   - Auto-drift scoring
   - Alerting on thresholds

3. **Expectations Integration** (`vibe_piper/expectations/auto.py`)
   - Attach expectations to assets
   - Auto-run during execution
   - Auto-reporting

**Success Criteria:**
- Auto-quality metrics working
- Drift detection integrated
- Expectations auto-run

---

## 7. Risks & Mitigations

| Risk | Impact | Mitigation |
|-------|---------|------------|
| **Breaking changes** to existing connectors | High | Deprecate old APIs gradually, provide migration guide |
| **Performance overhead** of abstractions | Medium | Benchmark critical paths, optimize hot paths |
| **Complexity** of new abstractions | Medium | Keep APIs simple, provide extensive docs |
| **Config file validation** | Low | Strong validation, helpful error messages |
| **Type inference** limitations | Medium | Fallback to Any when uncertain, user can override |

---

## 8. Success Metrics

### 8.1 Code Reduction

- **Before:** 365 lines (API ingestion example)
- **After:** ~30 lines
- **Reduction:** 92% less code

### 8.2 Developer Experience

- **Time to build pipeline:** 2 hours → 10 minutes
- **Lines of SQL written:** 50+ → 0
- **Lines of validation code:** 50+ → 0
- **Manual tracking code:** 30+ → 0

### 8.3 Test Coverage

- **Before:** Manual tests for each component
- **After:** Test sources/sinks/transforms independently
- **Coverage goal:** 80%+ for new abstractions

---

## 9. Conclusion

Vibe Piper has excellent foundational components but lacks the **integration layer** that makes it useful for real-world pipelines. The missing abstractions are:

1. **Source/Sink abstractions** - declarative data sources and sinks
2. **Schema-first mapping** - single source of truth with auto-mapping
3. **Declarative transformations** - composable, reusable transformation library
4. **Built-in quality/monitoring** - auto-tracking of metrics and drift
5. **Configuration-driven pipelines** - TOML/YAML pipeline definitions

Implementing these abstractions will reduce code by **90%+**, improve developer experience dramatically, and make Vibe Piper a **truly declarative** data pipeline library.

---

**Next Steps:**
1. Review and approve this analysis
2. Prioritize implementation phases
3. Begin Phase 1 implementation (Source/Sink abstractions)
4. Create prototype rewritten example
5. Iterate based on feedback
