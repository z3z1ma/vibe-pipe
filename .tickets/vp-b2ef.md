---
"id": "vp-b2ef"
"status": "open"
"deps": []
"links": []
"created": "2026-01-31T15:12:01Z"
"type": "task"
"priority": 2
"assignee": "z3z1ma"
"tags":
- "sprint:Cohesive-Core-Abstractions"
"external": {}
---
# Asset execution data contract for multi-upstream dependencies

Objective
Define and implement a clear data contract for how assets receive data from multiple upstream dependencies.

Dependencies
- vp-ba4d (Core abstraction contract).
- vp-a1f7 (Consolidated PipelineContext/types).

Suggested Order
1. After vp-ba4d; preferably after vp-a1f7 to avoid conflicting type changes.

Notes
- Current execution only passes the first upstream result; this should be addressed.

## Acceptance Criteria

- Execution passes all upstream dependency outputs in a documented structure (e.g., mapping by asset name).
- Operator/asset invocation signatures updated to match the contract.
- Pipeline builder/decorators align with the new contract.
- Tests cover multi-dependency assets and error handling behavior.
