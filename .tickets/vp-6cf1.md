---
"id": "vp-6cf1"
"status": "open"
"deps": []
"links": []
"created": "2026-01-29T10:16:41Z"
"type": "feature"
"priority": 1
"assignee": "z3z1ma"
"tags":
- "phase3"
- "cli"
"external": {}
---
# Untitled

## Notes

**2026-01-29T10:16:59Z**

CLI for Pipeline Operations. Tasks: 1) pipeline run command 2) pipeline validate command 3) pipeline status command 4) pipeline history command 5) pipeline backfill command 6) asset list/show commands 7) Config validation. Acceptance: All CLI commands working, helpful error messages, auto-completion, examples, tests, docs.

**2026-01-29T10:18:03Z**

DEPENDENCIES: Can start in parallel with other phase3 tickets but needs pipeline execution model (vp-cf95) to be defined. CLI commands wrap the execution engine. Phase 3 priority: HIGH - critical for UX.
