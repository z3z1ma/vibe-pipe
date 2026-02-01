---
"id": "vp-c9f3"
"status": "closed"
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

## Notes

**2026-02-01T15:10:16Z**

Progress update:
1. ✅ Removed  from pipeline.py
2. ✅ Added optional PostgreSQL import (dry-run mode works without DB connector)
3. ✅ Added CLI argument parsing (--dry-run, --page-size, --max-pages, --api-base-url, --api-key)
4. ✅ Fixed async/anyio test markers (changed from @pytest.mark.asyncio to @pytest.mark.anyio)
5. ✅ Added anyio marker to pyproject.toml
6. ✅ Fixed test fixture names (_mock_api_server → mock_api_server)
7. ⚠️ 4 async tests failing due to HTTP client mocking issue - need to investigate mock API server implementation
8. ✅ Added .gitignore for output/ directory
9. ✅ Updated README.md with uv run commands and dry-run instructions

Next steps: Fix async test failures to complete acceptance criteria.

**2026-02-01T15:24:40Z**

All acceptance criteria met:

✅ API ingestion example runs in dry-run mode without a database
   - Verified: uv run python examples/api_ingestion/pipeline.py --dry-run
   - Works with optional PostgreSQL import (skips DB connector if not available)

✅ Tests pass with uv run pytest examples/api_ingestion/tests -q
   - All 7 tests pass
   - Tests use httpx.MockTransport for HTTP mocking

✅ README lists uv run commands and required extras for DB mode
   - README updated with uv run examples
   - Documents dry-run mode
   - Documents postgres extras installation: uv pip install -e ".[postgres]"

Changes committed:
- 8ef7bde7: Initial modernization (CLI, optional imports, README, async markers)
- 4664844: Test fixes (httpx.MockTransport, paginate generator)
- 8a38175: Import fix for script execution

Ready for manager review.
