---
"id": "vp-ac5f"
"status": "closed"
"deps": []
"links": []
"created": "2026-02-01T14:50:37Z"
"type": "task"
"priority": 2
"assignee": "z3z1ma"
"parent": "vp-875f"
"tags":
- "sprint:Examples-Revitalization"
"external": {}
---
# Example: Drift detection refresh

Objective alignment:
Keep a focused drift detection example that demonstrates baselines, scoring, and history using current APIs.

Scope:
- Replace examples/drift_detection_example.py with a new folder examples/drift_detection/ containing run.py and README.
- Store baseline and history outputs under examples/drift_detection/output/ with a local .gitignore.

Non goals:
- No dashboarding or scheduled runs.
- No large datasets or long runtime.

Implementation plan:
1. Move logic into examples/drift_detection/run.py and update imports to current vibe_piper.validation APIs.
2. Add small helper flags like --quick and --clean to control sample size and clean output.
3. Ensure outputs write to examples/drift_detection/output/ with clear filenames.
4. Add examples/drift_detection/README.md with uv run commands and expected output locations.
5. Remove the old examples/drift_detection_example.py.

Verification:
- uv run python examples/drift_detection/run.py --quick --clean

Risks and edge cases:
- API changes in drift utilities; check src/vibe_piper/validation for current signatures and adjust.

## Acceptance Criteria

- New drift example runs with --quick and writes baseline and history files under examples/drift_detection/output/.
- Old examples/drift_detection_example.py is removed.
- README documents how to run and where outputs go.

## Notes

**2026-02-01T14:53:44Z**

Started implementation: creating new examples/drift_detection/ directory with run.py and README.md

**2026-02-01T15:00:04Z**

Implementation complete:

✓ Created examples/drift_detection/ directory with run.py and README.md
✓ Added --quick (100 samples) and --clean flags for configurable execution
✓ Outputs written to examples/drift_detection/output/ with baselines/ and drift_history/ subdirs
✓ Added local .gitignore for output files (JSON/JSONL excluded)
✓ Removed old examples/drift_detection_example.py
✓ All code formatted with ruff and linting passes

Files:
- examples/drift_detection/run.py (main script with argparse)
- examples/drift_detection/README.md (comprehensive documentation)
- examples/drift_detection/output/.gitignore (excludes generated files)
- examples/drift_detection/output/.gitkeep (preserves directory structure)

Verification:
- Tested: uv run python examples/drift_detection/run.py --quick --clean
- Runs successfully with proper baseline and history output
- --clean flag properly removes subdirectories

Ready for manager review.
