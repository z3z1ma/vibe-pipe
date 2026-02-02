---
"id": "vp-1d9b"
"status": "closed"
"deps":
- "vp-07c3"
- "vp-1a6a"
- "vp-ac5f"
- "vp-c9f3"
"links": []
"created": "2026-02-01T14:50:59Z"
"type": "task"
"priority": 2
"assignee": "z3z1ma"
"parent": "vp-875f"
"tags":
- "sprint:Examples-Revitalization"
"external": {}
---
# Examples catalog and cleanup

Objective alignment:
Deliver a clean, discoverable examples directory that matches the new example set and removes outdated artifacts.

Scope:
- Add examples/README.md as the canonical index for examples.
- Remove outdated or generated artifacts that are no longer part of the curated examples set.
- Update root README examples list to match.

Non goals:
- Do not implement new example logic beyond documentation and cleanup.

Implementation plan:
1. Inventory references to examples/sample_pipeline and examples/pipelines in README and docs.
2. Remove examples/sample_pipeline/ and examples/pipelines/ (generated docs and placeholder configs).
3. Add examples/README.md with sections for each active example and exact uv run commands.
4. Update README.md examples list to reflect the new example directories only.
5. Ensure each example directory has a brief README and output folders are gitignored.

Verification:
- uv run python -m compileall examples

Risks and edge cases:
- Removing old paths may break docs references; update references in README.md accordingly.

## Acceptance Criteria

- examples/README.md lists the active examples with uv run commands.
- examples/sample_pipeline/ and examples/pipelines/ are removed.
- README.md example list matches the curated set.

## Notes

**2026-02-01T19:31:03Z**

Work completed:

**Implementation completed:**
1. ✅ Created examples/README.md as canonical index
   - Lists all active examples with uv run commands
   - Organized by categories: Production Pipelines, Data Quality, Simple Scripts, Standalone Examples, Configuration Examples
   - Includes AssetGraph vs Pipeline comparison guide

2. ✅ Removed outdated directories:
   - examples/sample_pipeline/ (generated docs)
   - examples/pipelines/ (placeholder configs)

3. ✅ Updated README.md:
   - Removed references to deleted directories
   - Added reference to examples/README.md
   - Updated examples list to match curated set (added asset_graph_etl, pipeline_simple, drift_detection)

4. ✅ Added .gitignore files:
   - examples/pipeline_simple/.gitignore
   - examples/drift_detection/.gitignore
   - Ensures output/ and __pycache__ are ignored

5. ✅ Verification passed:
   - uv run python -m compileall examples - succeeded

**Git commits:**
- b030d0a: Add examples catalog and remove outdated directories
- 3965781: Remove outdated examples/pipelines and examples/sample_pipeline directories

Ready for manager review.
