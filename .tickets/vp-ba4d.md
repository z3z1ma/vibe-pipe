---
"id": "vp-ba4d"
"status": "in_progress"
"deps": []
"links": []
"created": "2026-01-31T15:11:44Z"
"type": "task"
"priority": 2
"assignee": "z3z1ma"
"tags":
- "sprint:Cohesive-Core-Abstractions"
"external": {}
---
# Core abstraction contract (pipeline/asset/operator/context)

Objective
Define the canonical core abstractions and their contracts so subsequent refactors are aligned.

Dependencies
- None.

Suggested Order
1. Create this contract before any refactor tickets are started.

Notes
- Capture findings about duplicates across `src/vibe_piper/core.py`, `src/vibe_piper/types.py`, `src/vibe_piper/pipeline.py`, `src/vibe_piper/decorators.py`.
- Output can live in the ticket body or a short linked ADR-style doc.

## Acceptance Criteria

- Identify duplicate/overlapping abstractions and where they live.
- Define canonical versions of Pipeline/Asset/Operator/PipelineContext and what is public vs internal.
- Define the execution data contract for upstream dependencies.
- Provide a deprecation + migration plan for legacy/duplicate types.
- Provide a short TDD-focused test plan for downstream refactors.

## Notes

**2026-01-31T15:23:11Z**

Investigation completed. Created CORE_ABSTRACTION_CONTRACT.md with:

**Key Findings:**
1. **Pipeline duplicate**: core.py (simple, legacy) vs types.py (sophisticated, canonical)
2. **PipelineContext name collision**: types.py (execution context) vs pipeline.py (builder, uses workaround alias PipelineDefContext)
3. **Stage vs Operator**: core.py Stage (unused, no context) vs types.py Operator (canonical, context-aware)
4. **Asset, AssetGraph**: No duplicates found - canonical types

**Recommendations:**
- Deprecate core.py Pipeline and Stage
- Rename pipeline.py PipelineContext to PipelineDefinitionContext
- Keep types.py as canonical source of truth

Contract includes: deprecation plan, migration path, TDD test plan, public vs internal API boundaries.

**2026-01-31T15:27:25Z**

Manager review complete. Contract approved and ready for follow-up tickets.

**Status:**
- Core abstraction contract documented in CORE_ABSTRACTION_CONTRACT.md
- All acceptance criteria met
- Manager review approved (ack e49336aa88dd)

**Deliverables Summary:**
1. Identified all duplicate/overlapping abstractions:
   - Pipeline (core.py vs types.py) - CRITICAL
   - PipelineContext (name collision between types.py and pipeline.py) - CRITICAL
   - Stage vs Operator (core.py vs types.py) - Moderate

2. Defined canonical versions with public contracts:
   - Asset (types.py:314-370)
   - AssetGraph (types.py:499-878)
   - Operator (types.py:278-313)
   - Pipeline (types.py:372-497) - canonical
   - PipelineContext (types.py:243-277) - execution context

3. Defined execution data contract:
   - Operator function signature: (data: Any, context: PipelineContext) -> Any
   - Context provides: pipeline_id, run_id, config (read-only), state (mutable)

4. Provided deprecation + migration plan:
   - v0.1.0: Deprecate core.py Pipeline and Stage (warnings)
   - v0.2.0: Remove from __init__.py exports
   - v0.3.0: Delete core.py entirely
   - Rename pipeline.py PipelineContext to PipelineDefinitionContext

5. TDD-focused test plan:
   - Unit tests for each abstraction (validation, immutability)
   - Integration tests for cross-abstraction workflows
   - Migration tests for deprecation warnings

**Next:**
- Manager will create follow-up tickets for actual refactoring work
- This ticket (vp-ba4d) can be closed as documentation is complete
