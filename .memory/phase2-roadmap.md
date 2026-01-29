# Phase 2 Roadmap: Integration & Transformation Layer

**Date:** 2026-01-27
**Ticket:** vp-5902 - Phase 2 Planning & Roadmap
**Status:** Final Draft
**Version:** VibePiper v0.2.0 → v0.3.0

## Executive Summary

Phase 2 transforms VibePiper from a **foundation** (v0.1.0-v0.2.0) into a **capable data pipeline framework** (v0.3.0). Building on Phase 1's solid architecture (IO managers, materialization strategies, validation, error handling, testing infrastructure), Phase 2 focuses on three critical areas:

1. **Integration Layer** - Connect to real data sources and sinks
2. **Transformation Framework** - Enable sophisticated data transformations
3. **Developer Experience** - CLI, configuration, and examples

**Timeline:** 4-6 weeks (depending on team size)
**Target Release:** v0.3.0
**Primary Goal:** Make VibePiper useful for real-world data pipelines

---

## What Was Accomplished in Phase 1 ✅

Based on the current state analysis and ticket history:

### Core Foundation (v0.1.0 → v0.2.0)
- ✅ **vp-101**: IO Manager abstraction layer (memory, file, S3, database)
- ✅ **vp-102**: Materialization strategies (table, view, file, incremental)
- ✅ **vp-103**: Enhanced validation framework (20+ validation types)
- ✅ **vp-104**: Asset versioning and lineage tracking
- ✅ **vp-105**: Testing infrastructure foundation
- ✅ **vp-106**: Error handling & recovery (retry, checkpointing)

### Architectural Strengths
- Asset-centric model (Dagster-inspired)
- Type-safe throughout (mypy strict mode)
- Automatic dependency inference
- Declarative schema definitions
- Protocol-based extensibility
- Immutable data structures

### Current Gaps (What Phase 2 Must Address)
1. **No real connectors** - Can't connect to databases, APIs, or files
2. **Limited transformations** - Only basic map/filter/aggregate
3. **No CLI** - Everything must be done via Python code
4. **No examples** - Hard for users to get started
5. **No configuration** - Hard to manage environments

---

## Phase 2 Vision

**Strategic Position:** Be the "simple, type-safe Python data pipeline framework"

We are NOT trying to be:
- ❌ Another Dagster (too complex)
- ❌ Another Airflow (too legacy)
- ❌ Another dbt (too SQL-focused)

We ARE trying to be:
- ✅ **Simple**: 5-minute learning curve
- ✅ **Type-safe**: Catch errors at dev time, not runtime
- ✅ **Pythonic**: Pure Python, no config languages
- ✅ **Composable**: Mix and match components
- ✅ **Production-ready**: From day one

---

## Phase 2 Priorities

### Priority 1: Integration Layer (P0 - Blocker)

**Why:** Without connectors, VibePiper can't interact with real data.

**What:**
- Database connectors (PostgreSQL, MySQL, Snowflake, BigQuery)
- File I/O (CSV, JSON, Parquet, Excel)
- API clients (REST, GraphQL)

**Success Criteria:**
- Can read from PostgreSQL and write to S3
- Can ingest data from a REST API
- All connectors have 90%+ test coverage

### Priority 2: Transformation Framework (P0 - Blocker)

**Why:** Real pipelines need joins, aggregations, and complex transformations.

**What:**
- Join operators (inner, left, right, full)
- Advanced aggregations (groupby, rollup, cube)
- Window functions (lag, lead, rank)
- SQL transformation support

**Success Criteria:**
- Can join two assets on a key
- Can perform grouped aggregations
- Can write SQL transformations

### Priority 3: Developer Experience (P1 - High)

**Why:** Low friction adoption is critical.

**What:**
- CLI framework (init, validate, run, test, docs)
- Configuration management (dev/staging/prod)
- Real-world examples (ETL, API ingestion)

**Success Criteria:**
- Can scaffold a new project in 30 seconds
- Can run a pipeline from CLI
- Has 2 working examples

---

## Phase 2 Tickets

### Core Tickets (P0 - Blockers)

1. **vp-201: Database Connectors**
   - PostgreSQL, MySQL, Snowflake, BigQuery
   - Connection pooling
   - Type-safe result mapping
   - **Estimate:** 5 days
   - **Dependencies:** vp-101 (IO managers)

2. **vp-202: File I/O Abstractions**
   - CSV, JSON, Parquet, Excel
   - Schema inference
   - Compression support
   - **Estimate:** 3 days
   - **Dependencies:** vp-101 (IO managers)

3. **vp-203: API Clients**
   - REST and GraphQL clients
   - Authentication (API key, OAuth)
   - Rate limiting
   - **Estimate:** 4 days
   - **Dependencies:** None

4. **vp-204: Transformation Framework**
   - Join operators
   - Advanced aggregations
   - Window functions
   - **Estimate:** 6 days
   - **Dependencies:** vp-103 (validation), vp-201 (databases)

5. **vp-205: SQL Integration**
   - @sql_asset decorator
   - SQL template engine
   - Multi-dialect support
   - **Estimate:** 4 days
   - **Dependencies:** vp-201 (databases), vp-204 (transformations)

6. **vp-206: CLI Framework**
   - init, validate, run, test, docs commands
   - Project scaffolding
   - Configuration file support
   - **Estimate:** 5 days
   - **Dependencies:** vp-101 (IO managers), vp-105 (testing)

### Enhancement Tickets (P1 - High)

7. **vp-207: Configuration Management**
   - Environment-specific configs
   - Secrets management
   - Parameter overrides
   - **Estimate:** 3 days
   - **Dependencies:** vp-206 (CLI)

8. **vp-208: Documentation Generator**
   - Asset catalog
   - Schema documentation
   - Lineage visualization
   - **Estimate:** 4 days
   - **Dependencies:** vp-104 (lineage)

9. **vp-209: Real-World Example - ETL Pipeline**
   - PostgreSQL → Parquet → Dashboard
   - Data quality checks
   - Error handling
   - **Estimate:** 3 days
   - **Dependencies:** vp-201, vp-202, vp-206

10. **vp-210: Real-World Example - API Ingestion**
    - REST API → Database
    - Pagination, rate limiting
    - **Estimate:** 2 days
    - **Dependencies:** vp-203, vp-206

### Optional Tickets (P2 - Medium)

11. **vp-211: Performance Optimizations**
    - Parallel execution
    - Result caching
    - Lazy evaluation
    - **Estimate:** 4 days
    - **Dependencies:** vp-101, vp-204

12. **vp-212: Advanced Validation Patterns**
    - Anomaly detection
    - Data profiling
    - Drift detection
    - **Estimate:** 3 days
    - **Dependencies:** vp-103 (validation)

---

## Dependency Graph

```
Phase 2 Dependencies:

vp-101 (IO Managers) ─────┐
                         ├─> vp-201 (Database Connectors)
vp-103 (Validation) ─────┤  │
                         │  └─> vp-204 (Transformations) ──> vp-205 (SQL)
                         │
vp-104 (Lineage) ─────────┴─────────────────────────────────> vp-208 (Docs)

vp-101 ───────────────────────────────────────> vp-202 (File I/O)
                                                 (no dependencies)
vp-105 (Testing) ──────┐
                       ├─> vp-206 (CLI) ──────> vp-207 (Config)
vp-101 ────────────────┤                     └─> vp-209 (ETL Example)
                       │
vp-201, vp-202 ────────┴─────────────────────────> vp-210 (API Example)

(no dependencies) ─────────────────────────────────> vp-203 (API Clients)

vp-204 ───────────────────────────────────────────> vp-211 (Performance)
vp-103 ───────────────────────────────────────────> vp-212 (Advanced Validation)
```

---

## Architecture Decisions

### 1. Integration Layer Design

**Decision:** Use protocol-based connectors with SQLAlchemy abstraction

**Rationale:**
- SQLAlchemy provides database-agnostic interface
- Protocols allow custom implementations
- Type-safe result mapping to schemas
- Easy to test with mocks

**Implementation:**
```python
class DatabaseConnector(Protocol):
    def connect(self) -> Connection: ...
    def query(self, sql: str, params: dict) -> DataFrame: ...
    def disconnect(self) -> None: ...

@asset(io_manager="postgresql")
def customers(db: DatabaseConnector):
    return db.query("SELECT * FROM customers")
```

### 2. Transformation Framework Design

**Decision:** Python-first with SQL as a first-class citizen

**Rationale:**
- Python transformations are more flexible
- SQL is better for database-backed operations
- Users should choose the right tool for the job
- Type safety maintained in both approaches

**Implementation:**
```python
# Python transformation
@transform
def customer_orders(customers, orders):
    return join(customers, orders, on="customer_id", how="left")

# SQL transformation
@sql_asset(dialect="postgresql")
def clean_customers():
    return "SELECT id, LOWER(email) FROM raw_customers"
```

### 3. CLI Design

**Decision:** Use Typer for CLI, TOML for configuration

**Rationale:**
- Typer is type-safe and Pythonic
- TOML is cleaner than YAML
- Consistent with modern Python tools
- Easy to generate completions

**Implementation:**
```bash
vibepiper init my-pipeline --template=etl
vibepiper run my-pipeline/ --asset=customers --env=prod
```

### 4. Configuration Management

**Decision:** Layered configuration with environment variables

**Rationale:**
- 12-factor app principles
- Secrets never in code
- Easy to override per environment
- Git-friendly (no secrets in repo)

**Implementation:**
```toml
# vibepiper.toml
[project]
name = "my-pipeline"

[environments.dev]
io_manager = "memory"

[environments.prod]
io_manager = "s3"
bucket = "my-bucket"

[secrets]
AWS_SECRET_ACCESS_KEY = { from = "env" }
```

---

## Risk Assessment

### High Risk

1. **vp-204 (Transformation Framework)** - High complexity
   - **Mitigation:** Break into smaller PRs, extensive testing
   - **Contingency:** Start with basic joins, add advanced features later

2. **vp-201 (Database Connectors)** - Many databases to support
   - **Mitigation:** Start with PostgreSQL, add others incrementally
   - **Contingency:** Community contributions for additional databases

### Medium Risk

1. **vp-206 (CLI)** - New interface, UX challenges
   - **Mitigation:** Prototype early, validate with users
   - **Contingency:** Keep Python API as primary interface

2. **vp-205 (SQL Integration)** - Dialect differences
   - **Mitigation:** Start with PostgreSQL, validate SQL generation
   - **Contingency:** Focus on ANSI SQL, document dialect limitations

---

## Success Criteria for Phase 2

### Must Have (P0)
- [ ] Can read from PostgreSQL and write to S3
- [ ] Can join two assets and perform aggregations
- [ ] Can scaffold and run a pipeline from CLI
- [ ] Has working ETL example
- [ ] Test coverage > 80%

### Should Have (P1)
- [ ] Can ingest data from REST API
- [ ] Can write SQL transformations
- [ ] Can generate documentation
- [ ] Has configuration management

### Nice to Have (P2)
- [ ] Performance optimizations
- [ ] Advanced validation patterns
- [ ] Multiple database dialects

---

## Phase 2 Deliverables

### Code Deliverables
1. **Integration Layer** (`src/vibe_piper/connectors/`)
   - `database/` - PostgreSQL, MySQL, Snowflake, BigQuery
   - `files/` - CSV, JSON, Parquet, Excel
   - `api/` - REST, GraphQL

2. **Transformation Framework** (`src/vibe_piper/transformations/`)
   - `joins.py` - All join types
   - `aggregations.py` - Groupby, rollup, cube
   - `windows.py` - Window functions
   - `sql.py` - SQL asset decorator

3. **CLI** (`src/vibe_piper/cli/`)
   - `main.py` - CLI entry point
   - `commands/` - Individual command implementations
   - `templates/` - Project scaffolding templates

4. **Configuration** (`src/vibe_piper/config/`)
   - `loader.py` - Config file parser
   - `secrets.py` - Secrets management
   - `validator.py` - Config validation

5. **Documentation** (`src/vibe_piper/docs/`)
   - `generator.py` - Docs generator
   - `templates/` - HTML templates

### Documentation Deliverables
1. **User Guide**
   - Getting Started Tutorial
   - Integration Guide (databases, files, APIs)
   - Transformation Guide
   - CLI Reference

2. **API Reference**
   - All public APIs documented
   - Type hints throughout
   - Example code for every feature

3. **Examples**
   - `examples/etl_pipeline/` - End-to-end ETL
   - `examples/api_ingestion/` - REST API integration

### Release Deliverables
1. **v0.3.0 Release**
   - Changelog
   - Migration guide (v0.2.0 → v0.3.0)
   - PyPI release
   - Announcement blog post

---

## Timeline

### Week 1-2: Integration Layer
- vp-201: Database Connectors (5 days)
- vp-202: File I/O (3 days)
- vp-203: API Clients (4 days)

**Milestone:** Can connect to real data sources

### Week 3: Transformations
- vp-204: Transformation Framework (6 days)
- Start vp-205: SQL Integration

**Milestone:** Can perform complex transformations

### Week 4: Developer Experience
- Complete vp-205: SQL Integration (4 days)
- vp-206: CLI Framework (5 days)

**Milestone:** Can use VibePiper from CLI

### Week 5-6: Polish & Examples
- vp-207: Configuration (3 days)
- vp-208: Documentation Generator (4 days)
- vp-209: ETL Example (3 days)
- vp-210: API Example (2 days)

**Milestone:** Production-ready v0.3.0

**Optional** (if time permits):
- vp-211: Performance (4 days)
- vp-212: Advanced Validation (3 days)

---

## Next Steps

1. **Review this roadmap** with stakeholders
2. **Create tickets** in the ticket system (vp-201 through vp-212)
3. **Set up sprint planning** for Phase 2
4. **Kick off vp-201** (Database Connectors) as first ticket
5. **Weekly check-ins** to track progress

---

## Conclusion

Phase 2 is the **make-or-break phase** for VibePiper. Phase 1 built a solid foundation, but Phase 2 makes it actually useful. By focusing on integration, transformation, and developer experience, we'll create a framework that users can adopt for real-world data pipelines.

**Key Success Factors:**
- Keep it simple (don't over-engineer)
- Type-safe throughout (maintain our differentiator)
- Test everything (quality > speed)
- Listen to users (validate assumptions)

**Post-Phase 2 (Phase 3 Preview):**
- Scheduling & Orchestration
- Web UI for monitoring
- Advanced features (distributed execution, etc.)

---

**End of Phase 2 Roadmap**
