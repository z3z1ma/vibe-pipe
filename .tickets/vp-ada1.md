---
"id": "vp-ada1"
"status": "open"
"deps": []
"links": []
"created": "2026-01-31T19:56:31Z"
"type": "task"
"priority": 2
"assignee": "z3z1ma"
"tags":
- "sprint:vibe-piper-architectural-reboot"
"external": {}
---
# Align docs/examples + CLI naming with canonical API

Context:
- README examples call build_pipeline(load_results), but build_pipeline currently expects a name string.
- Docs/README reference CLI name "vibe-piper" while script entrypoint is "vibepiper".
- CORE_ABSTRACTION_CONTRACT.md is stale relative to code.

Scope:
- Update README, docs, and examples to match the canonical API and CLI naming.
- Ensure examples run (or add minimal test coverage for examples).

Dependencies: vp-8783, vp-debe, vp-28e9.
Suggested order: 6.


## Acceptance Criteria

- README + docs examples compile/run against current API.
- CLI name is consistent across docs and code.
- CORE_ABSTRACTION_CONTRACT.md references are aligned with ADR decisions.
- Example tests or smoke checks added where feasible.

## Notes

**2026-01-31T21:15:20Z**

Investigating issues: 1) README line 156 uses build_pipeline(load_results) which expects a string, 2) CLI name mismatch (vibe-piper vs vibepiper), 3) Examples already have tests.

**2026-01-31T21:20:40Z**

Completed all tasks:
1. Fixed README Quick Start example to use PipelineDefinitionContext pattern instead of incorrect build_pipeline(load_results)
2. Updated README CLI examples from 'vibe-piper' to 'vibepiper'
3. Verified CORE_ABSTRACTION_CONTRACT.md is aligned with current API (no changes needed)
4. Fixed bug in src/vibe_piper/__init__.py - file I/O connectors were in __all__ but not imported
5. Added smoke test file tests/test_readme_examples.py with 2 tests that both pass
6. All changes committed (b76cbd6)
