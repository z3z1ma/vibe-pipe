---
"id": "vp-a1f7"
"status": "in_progress"
"deps": []
"links": []
"created": "2026-01-31T15:11:54Z"
"type": "task"
"priority": 2
"assignee": "z3z1ma"
"tags":
- "sprint:Cohesive-Core-Abstractions"
"external": {}
---
# Consolidate PipelineContext + remove duplicate Pipeline/Stage types

Objective
Unify duplicate core types so there is a single source of truth for PipelineContext and pipeline primitives.

Dependencies
- vp-ba4d (Core abstraction contract).

Suggested Order
1. After vp-ba4d is done.

Notes
- Targets duplicate definitions in `src/vibe_piper/core.py`, `src/vibe_piper/types.py`, and `src/vibe_piper/pipeline.py`.

## Acceptance Criteria

- There is exactly one canonical `PipelineContext` type and it is used by execution, operators, and builders.
- Duplicate/legacy `Pipeline`/`Stage` types are removed or explicitly deprecated with a documented migration path.
- Public exports in `src/vibe_piper/__init__.py` reflect the canonical types only.
- Tests updated/added to validate the canonical context usage.

## Notes

**2026-01-31T15:30:19Z**

Investigation complete. Found duplicate definitions:

1. core.py - Pipeline and Stage classes (UNUSED - no imports found)
2. types.py - Canonical PipelineContext and Pipeline (these are the ones being used)
3. pipeline.py - PipelineContext (context manager, name conflicts with types.py)

Plan:
1. Remove core.py (unused legacy)
2. Rename PipelineContext in pipeline.py to PipelineDefinitionContext
3. Update __init__.py exports
4. Update tests

**2026-01-31T15:38:31Z**

## Completed Implementation

### Changes Made:
1. Renamed PipelineContext in pipeline.py to PipelineDefinitionContext
   - Resolved naming conflict with runtime PipelineContext from types.py
   - Kept PipelineContext alias for backward compatibility in pipeline.py

2. Removed src/vibe_piper/core.py
   - Contained unused duplicate Pipeline and Stage classes
   - No code imports from this module (verified via grep)

3. Updated src/vibe_piper/__init__.py exports:
   - Exports canonical PipelineContext from types.py (runtime context)
   - Exports PipelineDefinitionContext from pipeline.py (definition builder)
   - Removed legacy PipelineDefContext alias

4. Updated test files:
   - tests/test_pipeline.py - Updated to use PipelineDefinitionContext
   - tests/test_dependency_inference.py - Updated to use PipelineDefinitionContext

### Tests:
- All 65 modified tests pass
- Formatting verified with ruff format
- Pre-existing typecheck issues noted (not related to changes)

Note: Tests in tests/transformations/test_pipeline.py fail but these are pre-existing syntax errors in test code unrelated to this ticket.

**2026-01-31T15:39:32Z**

## Summary
Successfully consolidated PipelineContext and removed duplicate types.

## Changes
1. pipeline.py: Renamed PipelineContext to PipelineDefinitionContext
   - Resolves naming conflict with runtime PipelineContext from types.py
   - Added backward compatibility alias: PipelineContext = PipelineDefinitionContext

2. Removed src/vibe_piper/core.py
   - Contained unused Pipeline and Stage classes
   - No code imports verified via grep

3. __init__.py: Updated exports
   - PipelineContext -> from types.py (runtime context)
   - PipelineDefinitionContext -> from pipeline.py (definition builder)
   - Removed PipelineDefContext alias

4. Tests: Updated all references
   - test_pipeline.py: Uses PipelineDefinitionContext
   - test_dependency_inference.py: Uses PipelineDefinitionContext

## Acceptance Criteria Status
✓ Exactly one canonical PipelineContext type (from types.py)
✓ Duplicate/legacy types removed (core.py deleted)
✓ Public exports reflect canonical types only
✓ Tests updated and pass (65/65)

## Commands Run
- uv run pytest tests/test_pipeline.py tests/test_dependency_inference.py -v (all pass)
- uv run ruff format --check (already formatted)

## Notes
- Pre-existing test failures in tests/transformations/test_pipeline.py are unrelated to this ticket
- Backward compatibility maintained via alias in pipeline.py

**2026-01-31T15:46:34Z**

## Fix Applied (Manager Feedback)

Removed backward compatibility alias from pipeline.py:
- Removed: PipelineContext = PipelineDefinitionContext
- Rationale: Ensures exactly ONE canonical PipelineContext type exists
- Users should now use:
  - PipelineDefinitionContext for pipeline building (from pipeline module)
  - PipelineContext for runtime context (from types.py, via vibe_piper)

All tests still pass (65/65).
