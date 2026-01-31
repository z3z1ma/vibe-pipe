---
"id": "vp-debe"
"status": "open"
"deps": []
"links": []
"created": "2026-01-31T19:55:56Z"
"type": "task"
"priority": 1
"assignee": "z3z1ma"
"tags":
- "sprint:vibe-piper-architectural-reboot"
"external": {}
---
# Unify pipeline definition APIs (builder/context/@asset)

Context:
- @asset decorator returns an Asset (no operator), while PipelineBuilder/PipelineDefinitionContext create executable assets.
- build_pipeline signature (name: str) conflicts with README examples (build_pipeline(asset_fn)).
- Parity tests only cover metadata, not execution semantics.

Scope:
- Select a single canonical pipeline definition path per ADR.
- Align @asset, PipelineBuilder, PipelineDefinitionContext, and build_pipeline behavior and docs.
- Preserve backward compatibility via deprecations/aliases where needed.

Dependencies: vp-8783.
Suggested order: 2.


## Acceptance Criteria

- Canonical pipeline definition API documented and enforced.
- @asset semantics aligned with pipeline definition (or renamed) and produce executable assets in the canonical path.
- build_pipeline signature and behavior match docs/examples.
- Tests added/updated for execution semantics + dependency inference.
- Deprecation warnings cover any old usage.
