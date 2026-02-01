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

**2026-02-01T15:11:58Z**

Continued investigation: Assets need proper (upstream_data, context) parameter signatures. Working on fixes to all asset functions. Will test and update soon.

**2026-02-01T15:25:19Z**

Root cause identified: AssetGraph asset execution model uses (upstream_data, context) parameters. When asset functions define parameters like (extract, transform), the pipeline builder's dependency inference treats these parameter names as potential dependency names. This creates a conflict because 'upstream_data' and 'context' are special framework parameters.

Current status: AssetGraph builds successfully with 5 assets. Extract and transform work. Validate, Load, Summarize fail because they depend on upstream_data.transform, upstream_data.load, etc. which aren't being populated correctly.

Tried solutions:
1. Storing config in ctx.config for downstream access - still creates inference issue
2. Using depends_on=[] for source assets - causes 'Asset depends on upstream_data' error
3. Using UpstreamData helper methods (.extract, .transform) - causes AttributeError

Need guidance on:
- Is there a documented way to pass configuration to assets?
- Should assets use only context parameter for config?
- Should we avoid certain parameter names to avoid special parameter conflicts?
- Or should we use a simpler AssetGraph pattern without complex upstream_data passing?

Proposing to defer to framework expert or create a simpler example using minimal dependencies.

**2026-02-01T15:28:08Z**

Committed initial AssetGraph ETL example structure:
- examples/asset_graph_etl/ directory created with sample data, README, tests
- pipeline.py uses PipelineDefinitionContext with 5 assets
- CSV I/O only (no database/Docker)
- ValidationSuite with fail-fast behavior

Framework issue identified: Assets use (upstream_data, context) parameters which trigger dependency inference errors. Transform/Load/Summarize fail because upstream_data.transform, upstream_data.load, upstream_data.summarize are not being populated. Requires expert guidance on proper AssetGraph parameter passing patterns.
