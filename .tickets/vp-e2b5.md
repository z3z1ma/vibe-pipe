---
"id": "vp-e2b5"
"status": "open"
"deps": []
"links": []
"created": "2026-01-28T01:27:18Z"
"type": "example"
"priority": 1
"assignee": "z3z1ma"
"tags":
- "phase2"
- "examples"
- "api"
"external": {}
---
# Real-World Example - API Ingestion

Build example pipeline for ingesting data from REST API into database.

## Tasks
1. Create pipeline: REST API → Database
2. Include pagination handling
3. Include rate limiting
4. Include transformation logic
5. Include error handling and retries
6. Document the example
7. Add integration tests

## Pipeline Flow:
1. Fetch data from REST API with pagination
2. Transform and validate
3. Handle rate limiting
4. Load into database
5. Generate quality report

## Example Structure:
```bash
examples/api_ingestion/
├── README.md
├── vibepiper.toml
├── pipeline.py
├── schemas.py
└── tests/
    └── test_pipeline.py
```

## Dependencies
- vp-203 (API clients)
- vp-206 (CLI)

## Acceptance Criteria:
API ingestion working
Pagination handled correctly
Rate limiting configured
Data transformed and validated
Documented
Integration tests passing
Uses mock API server for tests
