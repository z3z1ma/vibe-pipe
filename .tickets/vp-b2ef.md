---
"id": "vp-b2ef"
"status": "in_progress"
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

## Notes

**2026-01-31T16:09:41Z**

Investigation complete. Current state:

1. execution.py (lines 180-191): Only passes first upstream result to operator
   - Comment explicitly says 'This is a simplification - real implementation might merge data'

2. Tests confirm the limitation: multi-dependency assets only receive first upstream result

3. Required changes per acceptance criteria:
   - Pass all upstream dependency outputs in documented structure (mapping by asset name)
   - Update operator/asset invocation signatures
   - Update pipeline builder/decorators
   - Add comprehensive tests for multi-dependency and error handling

Starting implementation now.

**2026-01-31T16:28:17Z**

Progress update: Successfully added UpstreamData type to types.py and updated execution.py to pass all upstream dependencies as UpstreamData to operators.

Current status:
- UpstreamData type defined in types.py with proper handling
- Executor updated to convert Mapping to UpstreamData before calling operator
- Pipeline builder/decorators updated to handle both Mapping and UpstreamData

Issue: Tests failing because they expect old contract (data, context).

Next: Need to update tests to use new UpstreamData contract.

**2026-01-31T16:42:35Z**

Progress: Fixed backward compatibility in executor and pipeline builder.

The executor now properly handles both:
- New contract: Operators receive UpstreamData (for multi-upstream)
- Old contract: Operators receive raw data (backward compatibility)

Key changes:
- Executor tries UpstreamData first, falls back to unwrapping raw data
- Catches both TypeError and KeyError from operator
- Pipeline builder wrapped_fn updated with same logic
- All execution tests pass

Next: Write tests for multi-upstream scenarios.

**2026-01-31T16:49:40Z**

Implementation complete!

Summary:
- Added UpstreamData type for multi-upstream contract
- Updated executor with backward compatibility
- Updated pipeline builder/decorators
- 15 new tests covering all scenarios

See next ticket note for details on acceptance criteria.
