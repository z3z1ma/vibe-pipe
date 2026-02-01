---
"id": "vp-3fcc"
"status": "in_progress"
"deps": []
"links": []
"created": "2026-02-01T00:51:36Z"
"type": "task"
"priority": 2
"assignee": "z3z1ma"
"parent": "vp-ef4a"
"tags":
- "sprint:Phase-3-Core-Abstractions-Cohesiveness"
- "core"
- "assetgraph"
"external": {}
---
# Explicit asset dependencies + build_asset_graph

# Explicit asset dependencies + build_asset_graph

## Objective alignment
Make @asset definitions composable into AssetGraph with an explicit dependency contract, eliminating the current mismatch between decorator usage, builder inference, and docs.

## Scope
- Add a `depends_on` parameter to `@asset` (decorators.py) and asset_factory.create_asset to capture dependencies.
- Represent dependencies on Asset (preferred: new `dependencies: tuple[str, ...]` field) or store in `config`/`metadata` with a typed accessor.
- Add `build_asset_graph(assets: Sequence[Asset]) -> AssetGraph` that reads dependencies from assets and validates them.
- Ensure PipelineBuilder respects explicit dependencies when provided.
- Add tests for dependency capture and graph construction.

## Non-goals
- No async execution changes.
- No global asset registry.
- No changes to execution semantics beyond dependency mapping.

## Implementation plan
1. Update `src/vibe_piper/types.py` Asset dataclass to include a `dependencies` field (or document a config key if avoiding dataclass changes).
2. Update `src/vibe_piper/asset_factory.py` to accept `depends_on` and set the field/config.
3. Update `src/vibe_piper/decorators.py` to accept `depends_on` and pass it through to create_asset.
4. Add `build_asset_graph` in `src/vibe_piper/pipeline.py` (or `types.py`) to build dependencies from the new field/config.
5. Add tests in `tests/test_pipeline.py` and `tests/test_asset_creation_parity.py` for @asset depends_on and graph building.
6. Update docstrings for @asset and build_pipeline/build_asset_graph to reflect explicit dependencies.

## Acceptance criteria
- `@asset(depends_on=("a", "b"))` retains dependencies on the Asset.
- `build_asset_graph([...])` produces an AssetGraph with dependencies mapping and cycle validation.
- Tests cover decorator + builder parity for dependencies.

## Verification
- `uv run pytest tests/test_pipeline.py -q`
- `uv run pytest tests/test_asset_creation_parity.py -q`

## Risks / edge cases
- Adding a new Asset field may require updates in multiple tests; keep default empty tuple to preserve compatibility.
- If dependencies are stored in config/metadata, ensure a single canonical accessor to prevent drift.

## Dependencies
- None (foundational for other tickets).

## Acceptance Criteria

@asset supports depends_on and build_asset_graph constructs AssetGraph with validated dependencies

## Notes

**2026-02-01T01:05:42Z**

Implementation complete:
- Added dependencies field (tuple[str, ...]) to Asset dataclass in types.py
- Updated create_asset in asset_factory.py to accept depends_on parameter and pass to Asset constructor
- Updated @asset decorator in decorators.py to accept depends_on parameter and pass through to create_asset
- Added build_asset_graph function in pipeline.py to build AssetGraph from sequence of Assets with explicit dependencies
- Added build_asset_graph to pipeline module exports in __init__.py
- Updated PipelineBuilder and PipelineDefinitionContext to pass depends_on to create_asset when explicitly provided
- Added tests for depends_on parity in test_asset_creation_parity.py
- Added tests for build_asset_graph in test_pipeline.py

All acceptance criteria met:
- @asset(depends_on=("a", "b")) retains dependencies on the Asset
- build_asset_graph([...]) produces AssetGraph with dependencies mapping and cycle validation
- Tests cover decorator + builder parity for dependencies
- Tests cover build_asset_graph functionality

Verification:
- uv run pytest tests/test_pipeline.py::TestBuildAssetGraph -q: 7 passed
- uv run pytest tests/test_asset_creation_parity.py -k depends_on -q: 2 passed
