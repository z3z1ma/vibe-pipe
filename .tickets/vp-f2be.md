---
"id": "vp-f2be"
"status": "in_progress"
"deps": []
"links": []
"created": "2026-02-02T06:00:30Z"
"type": "task"
"priority": 2
"assignee": "z3z1ma"
"parent": "vp-831b"
"tags":
- "sprint:Codebase-Cleanup-and-De-bloating"
- "docs"
- "cleanup"
"external": {}
---
# Clean up config-driven pipelines doc example references

# Clean up config-driven pipelines doc example references

Objective alignment:
Remove references to deleted example files so the configuration guide is accurate and self-contained.

Scope:
- Update `docs/config_driven_pipelines.md`
- Update `docs/source/config_driven_pipelines.md`
- Remove or replace references to `examples/pipelines/demo_pipeline.toml` and `examples/pipelines/demo_pipeline.yaml`

Non-goals:
- Add new example files or restructure the examples directory
- Change the config schema described in the guide

Plan:
1. Locate the "Complete Example" section in both docs files.
2. Replace the missing examples references with an in-guide pointer (e.g., "See the TOML/YAML examples above") or a pointer to an existing relevant example file.
3. Re-scan to ensure no `examples/pipelines/` references remain.

Verification:
- `uv run pytest tests/docs/test_cli.py -q`

Acceptance criteria:
- No references to `examples/pipelines/demo_pipeline.*` remain.
- The guide still points readers to a concrete example (inline or existing file).

Risks/edge cases:
- Ensure any replacement example actually matches the config schema described.

Dependencies:
- None


## Acceptance Criteria

Removed stale examples/pipelines references and kept config-driven pipelines docs self-contained.

## Notes

**2026-02-02T06:04:57Z**

Updated both docs files (docs/config_driven_pipelines.md and docs/source/config_driven_pipelines.md) to replace stale examples/pipelines/demo_pipeline.* references with inline pointers to existing TOML/YAML format sections. Verified no remaining examples/pipelines/ references.

**2026-02-02T06:06:15Z**

✓ Completed

Changes made:
- Updated docs/config_driven_pipelines.md line 358
- Updated docs/source/config_driven_pipelines.md line 358
- Replaced stale references with inline pointers to TOML/YAML format examples

Verification:
- All 6 tests in tests/docs/test_cli.py passed
- No examples/pipelines/ references remain in docs/
- Committed changes as 28878bf
- Worktree clean

Ready for manager review.
