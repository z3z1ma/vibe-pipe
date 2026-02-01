---
"id": "vp-c9f3"
"status": "in_progress"
"deps": []
"links": []
"created": "2026-02-01T14:50:49Z"
"type": "task"
"priority": 2
"assignee": "z3z1ma"
"parent": "vp-875f"
"tags":
- "sprint:Examples-Revitalization"
"external": {}
---
# Example: API ingestion modernization

Objective alignment:
Modernize the API ingestion example so it is runnable, tested, and aligned with current integration and connector APIs.

Scope:
- Update examples/api_ingestion/ pipeline code, config, and README to use uv and current library APIs.
- Keep PostgreSQL as the default sink, but document an optional dry run mode.
- Ensure tests use the mock API server and can run without external services.

Non goals:
- No new connectors beyond existing Postgres integration.
- No production hardening beyond the example scope.

Implementation plan:
1. Review examples/api_ingestion/pipeline.py for outdated patterns, remove type ignore, and align with current integration modules.
2. Add or update a dry run mode that skips DB writes while still exercising the ingestion path.
3. Update examples/api_ingestion/vibepiper.toml and README to use uv run commands and environment variables.
4. Ensure tests in examples/api_ingestion/tests pass using the mock API fixtures without requiring a running database.
5. Add a local .gitignore for any generated artifacts under examples/api_ingestion/output/.

Verification:
- uv run pytest examples/api_ingestion/tests -q
- uv run python examples/api_ingestion/pipeline.py --dry-run

Risks and edge cases:
- Optional postgres extras may be required for full DB runs; document uv pip install -e ".[postgres]".
- Ensure dry run path does not import or require a live database connection.

## Acceptance Criteria

- API ingestion example runs in dry run mode without a database.
- Tests pass with uv run pytest examples/api_ingestion/tests -q.
- README lists uv run commands and required extras for DB mode.
