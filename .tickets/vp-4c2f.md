---
"id": "vp-4c2f"
"status": "open"
"deps":
- "vp-521f"
- "vp-a0d9"
"links": []
"created": "2026-02-03T02:20:19Z"
"type": "task"
"priority": 2
"assignee": "z3z1ma"
"parent": "vp-aa45"
"tags":
- "sprint:Codebase-Cleanup-and-De-bloating"
"external": {}
---
# Docs and README cleanup pass

# Docs and README cleanup pass

Objective alignment:
Ensure documentation is current, minimal, and aligned with the cleaned codebase and examples.

Scope:
- Top-level markdown files (excluding `README.md`, `LICENSE`, `LOOM_*.md`, `AGENTS.md`)
- `README.md` documentation, including CLI command section
- Links to examples and docs in `README.md`

Non-goals:
- No changes to Loom-managed files (`LOOM_*.md`, `.tickets/`)
- No API changes in `src/` (handled by dead-code ticket if needed)

Plan:
1. Inventory top-level markdown files and classify each as keep, move into `docs/`, or remove.
2. If moving/removing a file, update any references in `README.md` or `docs/`.
3. Update the README CLI commands to match the actual CLI usage (e.g., `vibepiper pipeline status`, `vibepiper asset list`).
4. Reconcile the README examples list with the final `examples/README.md` after example cleanup.

Acceptance criteria:
- Only essential top-level markdown files remain; the rest are moved or removed.
- README links and CLI command snippets are accurate and runnable.
- No dangling references to removed docs or examples.

Verification:
- `uv run vibepiper --help`
- `uv run vibepiper pipeline --help`
- `uv run vibepiper asset --help`

Risks/edge cases:
- Moving docs may break external links; ensure references in README/docs are updated.
- Some top-level docs might still be intentionally public; verify before removal.

## Acceptance Criteria

Top-level docs pruned or relocated; README links accurate; CLI commands match actual usage
