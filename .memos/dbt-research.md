# dbt Research Memo

**Date:** 2026-01-27
**Framework:** dbt (data build tool)
**Focus:** Analytics Engineering & SQL Transformation

## Executive Summary

dbt revolutionized data transformation by bringing software engineering practices to SQL. It pioneered the "transformation in the warehouse" model (ELT vs ETL), treating SQL models as version-controlled code with testing, documentation, and lineage. While SQL-focused, its patterns are applicable to Python transformation frameworks.

## Core Architecture

### Design Philosophy
- **Transformation in database**: Push computation to data warehouse
- **Select statements only**: Models are `SELECT` statements (no `CREATE/INSERT`)
- **Modular SQL**: Models compose other models
- **Data as code**: Version control, testing, CI/CD for data

### Key Components

1. **Models**
   ```sql
   -- models/customers.sql
   WITH
   source AS (
       SELECT * FROM {{ source('raw', 'users') }}
   ),
   transformed AS (
       SELECT
           id,
           email,
           created_at
       FROM source
       WHERE email IS NOT NULL
   )
   SELECT * FROM transformed
   ```
   - `.sql` files containing SELECT statements
   - Jinja templating for reusability
   - Ref operator to compose models (`{{ ref('other_model') }}`)
   - Materialization strategies (table, view, incremental)

2. **Tests**
   ```yaml
   # models/schema.yml
   models:
     - name: customers
       columns:
         - name: email
           tests:
             - unique
             - not_null
   ```
   - Column-level assertions (unique, not_null, relationships)
   - Custom tests (SQL queries that should return 0 rows)
   - Run separately from transformations
   - Data quality as first-class

3. **Documentation**
   ```yaml
   models:
     - name: customers
       description: "Customer master table"
       columns:
         - name: email
           description: "Customer email address"
   ```
   - Doc blocks in YAML
   - Auto-generated documentation site
   - Data dictionary from code

4. **Seed Data**
   - CSV files for static reference data
   - Version controlled
   - Useful for test data, lookup tables

## Notable Patterns for VibePiper

### Strengths to Emulate

1. **Modular Composition** ⭐
   - Models compose other models (directed acyclic graph)
   - `ref()` function creates dependency graph
   - Automatic lineage from composition
   - Lesson: VibePiper's asset graph should enable composition

2. **Testing Framework**
   - Built-in test types (unique, not_null, relationships)
   - Custom tests are SQL queries
   - Tests are separate from models (decoupled)
   - Lesson: Validation should be pluggable and composable

3. **Documentation Generation**
   - Documentation lives with code (YAML)
   - Auto-generated data dictionary
   - Column-level descriptions
   - Lineage visualization
   - Lesson: Generate docs from code, not separate

4. **Materialization Strategies**
   - View (virtual, no storage)
   - Table (materialized)
   - Incremental (append/update)
   - Lesson: Give users control over when/how assets materialize

5. **Source Definitions**
   ```yaml
   sources:
     - name: raw
       tables:
         - name: users
           freshness:
             warn_after: {count: 24, period: hour}
   ```
   - Declare upstream sources
   - Freshness monitoring
   - Lesson: VibePiper should have first-class source concept

6. **CI/CD Integration**
   - `dbt build` (run + test)
   - `dbt docs generate` (documentation)
   - Run tests in CI pipeline
   - Lesson: Make testing easy to automate

### Anti-Patterns to Avoid

1. **SQL-Only** (historically)
   - Initially SQL-only (Python models added later)
   - Limited programmatic logic
   - Lesson: VibePiper is Python-native, big advantage

2. **Jinja Templating**
   - Breaks IDE tooling
   - Hard to debug
   - No type safety
   - Lesson: Stay pure Python, use functions instead

3. **Monolithic dbt_project.yml**
   - Global configuration can get unwieldy
   - Lesson: Support per-asset config in Python

4. **Tests Separate from Models**
   - YAML files separate from SQL models
   - Can get out of sync
   - Lesson: Co-locate tests with assets when possible

## Key Takeaways for VibePiper Design

1. **Transformation Framework**
   - dbt excels at transformations, not orchestration
   - VibePiper can combine both!
   - Rich Python transformation library
   - SQL support via integration (not core)

2. **Testing Strategy**
   - dbt's test approach is elegant:
     - Declarative tests in YAML
     - Custom tests as queries
     - Tests are assertions about data
   - VibePiper should have similar:
     - Built-in validation types (not_null, unique, range)
     - Custom validation functions
     - Validation as first-class citizen

3. **Documentation**
   - Auto-generated from code/docs
   - Include lineage
   - Column/field descriptions
   - Easy to host static site
   - VibePiper can do better with Python docstrings

4. **Source Management**
   - Explicit source declarations
   - Freshness checks
   - Connection management
   - VibePiper should have similar concept

5. **Materialization**
   - Give users control
   - Support in-memory, file, database
   - Incremental updates
   - Caching strategies

## Python-Specific Advantages for VibePiper

Unlike dbt (SQL-based), VibePiper can leverage Python:

1. **Type Safety**
   ```python
   @asset
   def customers() -> DataFrame[CustomerSchema]:
       # Type-checked at compile time
   ```
   - dbt has no compile-time checking
   - VibePiper can use Python type hints

2. **Programmatic Logic**
   ```python
   @asset
   def process_data(data):
       # Complex Python logic
       result = []
       for item in data:
           if complex_condition(item):
               result.append(transform(item))
       return result
   ```
   - dbt limited to SQL (and limited Python)
   - VibePiper can use full Python

3. **Testing in Python**
   ```python
   @asset
   def customers():
       return get_customers()

   @validate(customers)
   def test_customers_unique(df):
       assert df['id'].is_unique
   ```
   - Use pytest, unittest
   - No custom test runner needed
   - Better IDE integration

4. **Composability**
   ```python
   @asset
   def final_report(sales, inventory, customers):
       # Compose multiple assets
       return merge([sales, inventory, customers])
   ```
   - Function composition
   - No templating language needed
   - Natural Python patterns

## Integration Ideas for VibePiper

1. **SQL Integration Layer**
   ```python
   @asset
   def customers(db: Database):
       return db.query("""
           SELECT id, email FROM raw.users
       """)
   ```
   - Support SQL where it makes sense
   - Don't build a SQL framework, just integrate

2. **dbt-Like Tests**
   ```python
   @asset
   def customers():
       return load_customers()

   @test_asset(customers)
   def test_unique_emails(df):
       assert df['email'].is_unique

   @test_asset(customers)
   def test_not_null_emails(df):
       assert df['email'].notnull().all()
   ```
   - Co-located with assets
   - Use standard pytest
   - Auto-discoverable

3. **Materialization Decorators**
   ```python
   @asset(materialize="table")
   def customers():
       return df

   @asset(materialize="incremental", key="date")
   def daily_sales(date):
       return df
   ```
   - Inspired by dbt
   - More Pythonic

4. **Source Definitions**
   ```python
   @source(
       name="production_db",
       uri="postgresql://...",
       freshness_check="1h"
   )
   class ProductionSource:
       pass
   ```
   - Pythonic source management
   - Type-safe connections

## References

- https://www.getdbt.com/
- https://docs.getdbt.com/docs/build/build-a-project
- https://www.getdbt.com/blog/data-transformation
