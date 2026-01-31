---
"id": "vp-6d4f"
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
# Create migration guide for Pipeline -> AssetGraph

## Notes

**2026-01-31T20:02:05Z**

# Create migration guide for Pipeline -> AssetGraph

Context:
- Users with existing Pipeline code may want to migrate to AssetGraph model
- Need guidance on how to convert linear pipelines to DAG-based assets

Scope:
- Create docs/migration_pipeline_to_assetgraph.md with:
  - When to migrate (production pipelines, materialization needs, orchestration)
  - Step-by-step migration examples
  - Before/after code comparisons
  - Common pitfalls and solutions

Dependencies: vp-8783 (ADR: canonical pipeline abstractions)

Acceptance Criteria:
- Migration guide created in docs/
- Covers common migration scenarios
- Provides working code examples
- Linked from README and API docs

No code changes beyond documentation.
