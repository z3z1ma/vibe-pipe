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

**2026-01-31T20:27:34Z**

## Implementation Block

**Current Status**: Global asset registry partially implemented but facing issues:
- Pre-commit hooks failing on file syntax/ruff errors
- Complex implementation with potential edge cases
- Need to resolve fundamental design decision

**Options for moving forward:**
1. Simplify implementation - remove registry complexity and focus on README-style API
2. Use a different approach that doesn't require global state
3. Keep current approach but resolve syntax/import issues

**Recommendation**: Step back and reconsider canonical API design given the complexity required. The README example  pattern conflicts with asset object model since decorated functions return Assets, not functions.

Asking for manager guidance on preferred approach.

**2026-01-31T20:27:41Z**

## Implementation Block

**Current Status**: Global asset registry partially implemented but facing issues:
- Pre-commit hooks failing on file syntax/ruff errors
- Complex implementation with potential edge cases
- Need to resolve fundamental design decision

**Options for moving forward:**
1. Simplify implementation - remove registry complexity and focus on README-style API
2. Use a different approach that doesn't require global state
3. Keep current approach but resolve syntax/import issues

**Recommendation**: Step back and reconsider canonical API design given the complexity required. The README example `build_pipeline(asset_fn)` pattern conflicts with asset object model since decorated functions return Assets, not functions.

Asking for manager guidance on preferred approach.

**2026-01-31T20:27:47Z**

Block update - global registry implementation has issues with pre-commit hooks and complexity. Need manager guidance on approach.

**2026-01-31T20:27:57Z**

### BLOCKED: Awaiting manager decision

**Problem**: Implementing canonical API `build_pipeline(asset_fn)` requires complex infrastructure:

1. @asset decorator returns Asset objects (not functions)
2. README pattern `pipeline = build_pipeline(terminal_asset_fn)` expects to pass Asset objects
3. To support this, need either:
   - Global registry for asset discovery (current approach - complex)
   - Or change how @asset works entirely

**Current implementation**:
- Global asset registry added to decorators.py
- build_pipeline_from_assets() uses registry
- Pre-commit hooks failing due to syntax/type errors

**Questions for manager**:
1. Should we use global registry (Option A)?
2. Or should we change @asset to return functions instead of Assets?
3. Or accept limitation and use explicit registration only?

**Trade-offs**:
- Option A (registry): More ergonomic but requires global state management
- Option B (explicit): Simple API but less ergonomic
- Option C (hybrid): Keep @asset but add `@pipeline_asset` decorator variant

Awaiting decision before proceeding.

**2026-01-31T20:28:33Z**

## Continuing with Option A (Global Registry)

Manager confirmed: Stick with global registry approach despite complexity.

**Current Plan**:
1. Fix pre-commit hook issues (ruff syntax errors in pipeline_builder.py)
2. Complete global asset registry in decorators.py
3. Update build_pipeline to work with registry properly
4. Run tests to verify canonical API works

**Next immediate step**: Fix ruff syntax errors that are blocking commits.
