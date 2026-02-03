---
"id": "vp-aa45"
"status": "closed"
"deps": []
"links": []
"created": "2026-02-03T02:15:18Z"
"type": "task"
"priority": 1
"assignee": "z3z1ma"
"tags":
- "sprint:Codebase-Cleanup-and-De-bloating"
- "fanout"
"external": {}
---
# Sprint prep: Codebase Cleanup and De-bloating

Objective:
Clean up the codebase, remove dead code, outdated examples, and so on. Make every bit of bytes left in the codebase relevant and net useful. Distill out the essence of beauty and novelty. Prove the expressiveness and value. Look at non code artifacts too which are outdated or useless.

Sprint prep deliverable (fill this ticket in, then create tickets):

## Sprint Brief

Required sections:
- Objective restatement: Remove stale or unused code, docs, and examples so the repo only contains current, high-signal artifacts.
- Sprint focus (2-5 words): Prune Stale Artifacts
- Why this sprint focus is the best next step: Recent commits already removed outdated examples and fixed doc links; the remaining work is to systematically audit and prune residual dead code and docs so the repo stays lean and accurate.
- Current state:
  - Existing tickets that matter: None open besides this sprint prep ticket.
  - Codebase state that matters (git status/log, key modules): `git status` shows untracked `.venv` file. Recent commits focus on examples cleanup and doc link fixes. Key areas include `examples/` (multiple pipelines + tests), `docs/` (md + Sphinx source), and a broad `src/vibe_piper/` surface (connectors, integration, scheduling, monitoring, validation, docs site generator). Top-level docs include `CORE_ABSTRACTION_CONTRACT.md`, `PUBLIC_API_CURATION_ANALYSIS.md`, and `INVESTIGATION_NOTES.md`.
- Risks + unknowns (and how we'll resolve them):
  - Risk: Removing a module or doc still referenced by examples or docs. Resolve via search for references and updating callers before deletion.
  - Risk: Examples listed in README but removed or changed. Resolve by auditing README/examples index and updating links.
  - Unknown: Which modules are truly unused. Resolve with usage search, ruff checks, and running targeted tests/examples.

## Ticket Set

Created sprint tickets:
- `vp-a0d9` - Examples audit and pruning
- `vp-521f` - Dead code and unused module pruning (depends on `vp-a0d9`)
- `vp-4c2f` - Docs and README cleanup pass (depends on `vp-a0d9`, `vp-521f`)

## Output

- Created/updated ticket IDs: [vp-a0d9, vp-521f, vp-4c2f]
- Suggested ordering + what can run in parallel:
  1) `vp-a0d9` (examples audit)
  2) `vp-521f` (dead code prune) after `vp-a0d9` decisions land
  3) `vp-4c2f` (docs cleanup) after `vp-a0d9` + `vp-521f` to align README/docs with final state
  Parallelization: none recommended due to shared README/docs surfaces.

Sprint name: Codebase Cleanup and De-bloating
Sprint tag: sprint:Codebase-Cleanup-and-De-bloating
