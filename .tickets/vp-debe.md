---
"id": "vp-debe"
"status": "in_progress"
"deps": []
"links": []
"created": "2026-01-31T19:55:56Z"
"type": "task"
"priority": 1
"assignee": "z3z1ma"
"tags":
- "sprint:vibe-piper-architectural-reboot"
"external": {}
---
# Unify pipeline definition APIs (builder/context/@asset)

Context:
- @asset decorator returns an Asset (no operator), while PipelineBuilder/PipelineDefinitionContext create executable assets.
- build_pipeline signature (name: str) conflicts with README examples (build_pipeline(asset_fn)).
- Parity tests only cover metadata, not execution semantics.

Scope:
- Select a single canonical pipeline definition path per ADR.
- Align @asset, PipelineBuilder, PipelineDefinitionContext, and build_pipeline behavior and docs.
- Preserve backward compatibility via deprecations/aliases where needed.

Dependencies: vp-8783.
Suggested order: 2.


## Acceptance Criteria

- Canonical pipeline definition API documented and enforced.
- @asset semantics aligned with pipeline definition (or renamed) and produce executable assets in the canonical path.
- build_pipeline signature and behavior match docs/examples.
- Tests added/updated for execution semantics + dependency inference.
- Deprecation warnings cover any old usage.

## Notes

**2026-01-31T20:06:38Z**

Investigation findings: Found signature mismatch between README examples and build_pipeline() implementation. README shows build_pipeline(asset_fn) but actual signature is build_pipeline(name: str). Also @asset creates Assets without operators (not executable) while PipelineBuilder/PipelineDefinitionContext create assets WITH operators (executable). Current parity tests only check metadata, not execution semantics.

**2026-01-31T20:21:33Z**

## Investigation Update

### Current Progress

**Completed:**
1. Updated asset decorator to always create operators (create_operator=True)
2. Created build_pipeline_from_assets() in pipeline_builder.py for scanning modules and building graphs
3. Updated build_pipeline() in pipeline.py to support two modes:
   - Canonical mode: build_pipeline(asset_fn) scans module and returns AssetGraph
   - Builder mode: build_pipeline(name) returns PipelineBuilder (deprecated)
4. Exported build_pipeline_from_assets in __init__.py
5. Added execution parity tests in test_pipeline_execution_parity.py

### Current Issue

The module scanning approach has a fundamental problem:
- When build_pipeline(aggregate) is called from a test, it uses inspect.currentframe() to get the calling module
- This returns the test module (test_pipeline_execution_parity), not the global module where assets should be defined
- The decorated assets in the test are local variables in the test function, not accessible at module level

Need to implement a global asset registry in decorators.py that:
1. Automatically registers assets when they're decorated at module level
2. Can be accessed by build_pipeline_from_assets for scanning
3. Works across different modules (tests vs. user code)

Options:
A. Use global dict + module introspection on function definition
B. Use a custom registry class with explicit registration
C. Accept limitation and require explicit asset registration

Recommendation: Use option A (global registry) as it's most ergonomic for users.

Next: Implement global asset registry in decorators.py module
