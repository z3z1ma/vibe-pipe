---
"id": "vp-1a6a"
"status": "in_progress"
"deps": []
"links": []
"created": "2026-02-01T14:50:19Z"
"type": "task"
"priority": 2
"assignee": "z3z1ma"
"parent": "vp-875f"
"tags":
- "sprint:Examples-Revitalization"
"external": {}
---
# Example: AssetGraph local ETL

Objective alignment:
Create a canonical, fully runnable AssetGraph example that showcases declarative assets, validation, and file outputs without external services.

Scope:
- Add a new example under examples/asset_graph_etl/ with sample data, pipeline code, README, and tests.
- Use file based inputs and outputs only (CSV or Parquet).

Non goals:
- No database connectors or Docker.
- No scheduling or production orchestration.

Implementation plan:
1. Create examples/asset_graph_etl/data/users.csv with a small realistic dataset.
2. Implement examples/asset_graph_etl/pipeline.py using PipelineDefinitionContext, ExecutionEngine, and assets: extract, transform, validate, load, and summarize.
3. Use ValidationSuite checks on transformed records and fail fast on invalid data.
4. Write outputs to examples/asset_graph_etl/output/ and add a local .gitignore for output.
5. Add examples/asset_graph_etl/README.md with prerequisites and uv run commands.
6. Add tests under examples/asset_graph_etl/tests/ to assert row counts and key fields.

Verification:
- uv run pytest examples/asset_graph_etl/tests -q
- uv run python examples/asset_graph_etl/pipeline.py --once

Risks and edge cases:
- Optional file IO extras may be required; document uv pip install -e ".[files]".
- Keep sample data small so tests stay fast.

## Acceptance Criteria

- Example runs end to end and writes output under examples/asset_graph_etl/output/.
- Tests pass with uv run pytest examples/asset_graph_etl/tests -q.
- README lists exact uv commands and prerequisites.

## Notes

**2026-02-01T15:02:08Z**

Investigating pipeline execution issue - extract asset is failing. The issue appears to be related to how AssetGraph assets access configuration. Assets are defined without context parameters but ExecutionEngine expects PipelineContext with config metadata. Working on fix by simplifying to use direct config values in assets.

**2026-02-01T15:02:49Z**

Root cause identified: Asset functions must accept both upstream_data and context parameters. For source assets with no dependencies, upstream_data is empty. Fixing all asset signatures to match the ExecutionEngine's calling convention.
