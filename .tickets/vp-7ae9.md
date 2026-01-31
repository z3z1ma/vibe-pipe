---
"id": "vp-7ae9"
"status": "open"
"deps": []
"links": []
"created": "2026-01-31T20:01:55Z"
"type": "task"
"priority": 3
"assignee": "z3z1ma"
"tags":
- "sprint:vibe-piper-architectural-reboot"
"external": {}
---
# Update API documentation with execution layering examples

## Notes

**2026-01-31T20:02:05Z**

# Update API documentation with execution layering examples

Context:
- ADR vp-8783 defined three execution layers (Operator, Pipeline, AssetGraph)
- Need to document how each layer works and when to use which

Scope:
- Update docs/ with:
  - Execution layering diagram
  - Examples of operator execution
  - Examples of pipeline execution
  - Examples of asset graph execution
  - Comparison of each layer's use cases

Dependencies: vp-8783 (ADR: canonical pipeline abstractions)

Acceptance Criteria:
- Execution layering documented with clear examples
- Diagram showing relationship between layers
- Use cases for each layer documented
- Linked from README and API docs

No code changes beyond documentation.
