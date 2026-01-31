---
"id": "vp-98cc"
"status": "open"
"deps": []
"links": []
"created": "2026-01-31T15:12:19Z"
"type": "task"
"priority": 2
"assignee": "z3z1ma"
"tags":
- "sprint:Cohesive-Core-Abstractions"
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
