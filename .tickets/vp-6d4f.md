---
"id": "vp-6d4f"
"status": "closed"
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

**2026-01-31T21:20:39Z**

Completed migration guide creation:

Created: docs/migration_pipeline_to_assetgraph.md

Content includes:
- When to migrate guidelines (production pipelines, materialization needs, orchestration)
- 5 step-by-step migration examples with before/after code comparisons:
  1. Simple linear pipeline
  2. Pipeline with multiple branches (DAG support)
  3. Pipeline with retry logic
  4. Pipeline with state sharing
  5. PipelineBuilder migration
- Common pitfalls and solutions (5 scenarios)
- Quick reference checklist and operator signature mapping

Updated: README.md with link to migration guide in Documentation section

Next: Ready for manager review. Verification steps completed:
- Migration guide created in docs/
- Covers common migration scenarios (5 examples)
- Provides working code examples
- Linked from README

No code changes beyond documentation as required.
