---
"id": "vp-a1f7"
"status": "open"
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
