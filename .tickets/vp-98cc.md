---
"id": "vp-98cc"
"status": "in_progress"
"deps":
- "vp-786e"
- "vp-a1f7"
- "vp-b2ef"
"links":
- "vp-7686"
"created": "2026-01-31T15:12:19Z"
"type": "task"
"priority": 1
"assignee": "z3z1ma"
"tags":
- "sprint:Cohesive-Core-Abstractions"
- "sprint:Bootstrap-backlog-objective"
- "docs"
- "api"
"external": {}
---
# Public API + docs alignment for core abstractions

Objective
Update public API surface and documentation to reflect the cohesive core abstractions.

Dependencies
- vp-a1f7 (Consolidated PipelineContext/types).
- vp-b2ef (Multi-upstream execution contract).
- vp-786e (Builder/decorator alignment).

Suggested Order
1. After core refactor tickets complete.

Notes
- Touchpoints include `README.md`, docs under `docs/source/`, and `src/vibe_piper/__init__.py` exports.

## Acceptance Criteria

- README and docs examples reflect canonical Pipeline/Asset/Operator/Context APIs.
- Public exports are accurate and avoid exposing deprecated types.
- Deprecation/migration notes are documented.

## Notes

**2026-01-31T17:40:51Z**

Investigated current state:
- README.md: Examples already use correct API (@asset, build_pipeline)
- docs/source/index.rst: OUTDATED - references old Pipeline/Stage API
- docs/source/getting_started.rst: OUTDATED - references old Pipeline/Stage API
- docs/source/api/core.rst: References vibe_piper.core module (doesn't exist)
- src/vibe_piper/__init__.py: Exports are correct (PipelineContext, UpstreamData, PipelineDefinitionContext, etc.)

Plan:
1. Update docs/source/index.rst with current API examples
2. Update docs/source/getting_started.rst with current API examples
3. Update docs/source/api/core.rst to reference correct modules
4. Add migration notes to README

**2026-01-31T17:41:56Z**

Updated docs:
- docs/source/index.rst: Updated Quick Example and Features to use @asset, build_pipeline, PipelineContext
- docs/source/getting_started.rst: Complete rewrite with current API (Asset decorator, PipelineBuilder, PipelineDefinitionContext, UpstreamData, ExecutionEngine)
- docs/source/api/core.rst: Updated to reference vibe_piper.pipeline and vibe_piper.types modules instead of non-existent vibe_piper.core
- README.md: Added Migration Guide section documenting old Pipeline/Stage API → new Asset API

All docs now reflect the canonical core abstractions:
- Runtime context: PipelineContext (from types.py)
- Definition context: PipelineDefinitionContext (from pipeline.py)
- Multi-upstream contract: UpstreamData (from types.py)
- Asset creation parity: @asset / PipelineBuilder.asset / PipelineDefinitionContext.asset all use create_asset factory

**2026-01-31T17:43:03Z**

Documentation updates complete:

Files updated:
1. docs/source/index.rst - Updated Quick Example and Features sections to use @asset, build_pipeline, and current API
2. docs/source/getting_started.rst - Complete rewrite covering:
   - Asset decorator usage
   - PipelineBuilder fluent interface
   - PipelineDefinitionContext declarative syntax
   - Multi-upstream support with UpstreamData
   - Pipeline execution with ExecutionEngine
3. docs/source/api/core.rst - Updated to reference vibe_piper.pipeline and vibe_piper.types (removed non-existent vibe_piper.core)
4. README.md - Added Migration Guide section documenting old Pipeline/Stage → new Asset API transition

Verification:
- All types used in docs are correctly exported in src/vibe_piper/__init__.py
- Ruff format and lint passed for all updated files
- Examples align with canonical core abstractions:
  * Runtime context: PipelineContext (types.py)
  * Definition context: PipelineDefinitionContext (pipeline.py)
  * Multi-upstream contract: UpstreamData (types.py)
  * Asset creation parity: @asset / PipelineBuilder.asset / PipelineDefinitionContext.asset use create_asset factory

Acceptance criteria met:
✅ README and docs examples reflect canonical Pipeline/Asset/Operator/Context APIs
✅ Public exports are accurate (verified no deprecated types exposed)
✅ Deprecation/migration notes documented

Note: Pre-existing test failure in test_diamond_dependency_pattern (multi-upstream bug) - not related to doc changes.

**2026-01-31T17:44:11Z**

Requested manager review via loom team send.
Sent message: 'READY_FOR_REVIEW ticket=vp-98cc worker=w7 branch=team/vp-98cc sha=17f2d01 summary=Aligned docs with canonical core abstractions...'

Waiting for manager review and approval.

**2026-01-31T17:46:19Z**

Manager review: BLOCKED. Fix doc/API mismatches before approval: (1) Remove incorrect claim that top-level @asset auto-registers with PipelineBuilder/build_pipeline; rewrite examples to supported patterns (PipelineBuilder.asset, PipelineDefinitionContext/@pipeline.asset, or manual AssetGraph from Asset objects). (2) Fix multi-upstream docs: multi-upstream assets should use UpstreamData + PipelineContext, not separate users/orders args. (3) Fix Sphinx directives in docs/source/api/core.rst: PipelineBuilder and PipelineDefinitionContext are classes -> use autoclass, not autofunction.

**2026-01-31T17:50:22Z**

Fixed all issues from manager review:

1. Removed incorrect claim about @asset auto-registration
   - docs/source/index.rst now uses PipelineBuilder.asset() pattern
   - Removed statement that @asset automatically adds to builder
   - Updated docs/source/getting_started.rst to remove misleading section

2. Fixed multi-upstream example
   - Changed def merge(users, orders) to def merge(upstream: UpstreamData, context: PipelineContext)
   - Shows accessing upstream['users'], upstream['orders']

3. Fixed Sphinx directives in docs/source/api/core.rst
   - Changed .. autofunction:: to .. autoclass:: for PipelineBuilder
   - Changed .. autofunction:: to .. autoclass:: for PipelineDefinitionContext
   - Kept build_pipeline as .. autofunction::

All examples now use supported API patterns:
- PipelineBuilder.asset(...) (explicit builder)
- PipelineDefinitionContext with @pipeline.asset(...) (collects assets within context)
- Correct multi-upstream signature with UpstreamData parameter

New commit: c93e930

**2026-01-31T17:50:33Z**

Resubmitted READY_FOR_REVIEW request with sha c93e930.

Fixed issues:
1. @asset auto-registration claim removed - docs now correctly show PipelineBuilder.asset() usage
2. Multi-upstream examples updated to use upstream: UpstreamData parameter
3. Sphinx directives fixed - PipelineBuilder/PipelineDefinitionContext now use autoclass

Waiting for manager approval.

**2026-01-31T17:51:40Z**

Manager note: branch includes .opencode/compound/state.json (autolearn metadata). Please drop this file from the PR/commit history before approval (docs-only ticket).

**2026-01-31T17:53:22Z**

Fixed final blocker per manager review:
- Removed .opencode/compound/state.json from git tracking
- Added .opencode/compound/ to .gitignore
- Resubmitted READY_FOR_REVIEW with sha cb1c68b

All previous doc issues fixed in commit c93e930:
1. @asset auto-registration claim removed
2. Multi-upstream example uses UpstreamData
3. Sphinx directives fixed (autoclass for classes)

Waiting for manager approval.
