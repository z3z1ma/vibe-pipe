---
"id": "vp-64cd"
"status": "open"
"deps": []
"links": []
"created": "2026-01-31T19:56:05Z"
"type": "task"
"priority": 1
"assignee": "z3z1ma"
"tags":
- "sprint:vibe-piper-architectural-reboot"
"external": {}
---
# Consolidate ExecutionEngine + OrchestrationEngine

Context:
- ExecutionEngine and OrchestrationEngine both execute AssetGraphs with overlapping logic (ordering, metrics, retries), leading to duplication and drift.
- OrchestrationEngine has duplicate execution blocks and unclear layering vs ExecutionEngine.

Scope:
- Define a single execution core and extension points (parallel, incremental, caching, retries).
- Refactor OrchestrationEngine to reuse shared execution core or merge engines as per ADR.
- Remove duplicate ordering/metrics logic.

Dependencies: vp-8783.
Suggested order: 3 (after canonical model).


## Acceptance Criteria

- One execution code path for AssetGraph execution; orchestration features compose cleanly on top.
- Duplicate ordering/metrics logic removed.
- Updated tests cover sequential + parallel + incremental paths.
- Execution docs updated with new layering.
