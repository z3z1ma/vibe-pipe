---
"id": "vp-1aa6"
"status": "in_progress"
"deps": []
"links": []
"created": "2026-01-28T01:26:41Z"
"type": "feature"
"priority": 2
"assignee": "z3z1ma"
"tags":
- "phase2"
- "sql"
- "transformations"
"external": {}
---
# SQL Integration

Add SQL transformation support for database-backed transformations with @sql_asset decorator.

## Tasks
1. Create @sql_asset decorator
2. Implement SQL template engine (Jinja-like syntax)
3. Add parameter binding for safe queries
4. Add SQL validation before execution
5. Support multiple SQL dialects (PostgreSQL, MySQL, Snowflake, BigQuery)
6. Integrate with database connectors
7. Add SQL dependency tracking

## Example Usage
```python
from vibe_piper import sql_asset

@sql_asset(
    depends_on=["raw_users"],
    dialect="postgresql",
    io_manager="postgresql"
)
def clean_users():
    return '''
    SELECT
        id,
        LOWER(email) as email,
        created_at
    FROM {{ raw_users }}
    WHERE email IS NOT NULL
    '''

@sql_asset(dialect="postgresql")
def aggregated_sales(raw_sales):
    return '''
    SELECT
        date,
        SUM(amount) as total_sales,
        COUNT(*) as order_count
    FROM {{ raw_sales }}
    GROUP BY date
    '''
```

## Dependencies
- vp-201 (Database connectors)
- vp-204 (Transformation framework)

## Technical Notes
- Use Jinja2 for templating
- Parameterize all user inputs
- Parse SQL to validate syntax
- Track dependencies from {{ asset }} references
- Support CTEs and subqueries

## Acceptance Criteria

@sql_asset decorator works
SQL templates support Jinja-like syntax
Parameters bound safely (SQL injection prevention)
SQL validated before execution
Multiple dialects supported (PostgreSQL, MySQL, Snowflake, BigQuery)
Works with database connectors
SQL dependency tracking
Tests and documentation
Test coverage > 85%

## Notes

**2026-01-29T10:14:16Z**

Manager note: This is a P2 ticket. Please prioritize completing and merging the P1 tickets (vp-045b, vp-0862, vp-77b7, vp-e2b5) before working on this ticket. The SQL integration builds on top of database connectors.
