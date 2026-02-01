# INSTINCTS

This is the *fast index* of the current instinct set.
The source of truth is `.opencode/memory/instincts.json`.

<!-- BEGIN:compound:instincts-md -->
## Active instincts (top confidence)

- **memory-store-mass-diff-low-signal** (100%)
  - Trigger: Git diffstat shows large deletions/rewrites in .opencode/memory/instincts.json and/or .opencode/memory/INSTINCTS.md without corresponding product code changes.
  - Action: Assume bookkeeping/cleanup; avoid inferring new behaviors. Prefer small updates to existing instincts/skills, or no-op if evidence is only memory-store churn.
- **ticket-and-doc-churn-low-signal** (100%)
  - Trigger: Git diffstat is dominated by .tickets/*.md and LOOM_*.md/AGENTS.md edits with no corresponding product code changes.
  - Action: Avoid inventing new product behaviors; limit proposals to workflow hygiene (skills/instinct wording tweaks, docs.sync) and keep docs block edits minimal unless a stable always-on principle changed.
- **snapshot-testing-implementation-check** (100%)
  - Trigger: Investigating Phase 3 testing or any testing-related ticket
  - Action: 1. Check if tests/helpers/snapshots.py exists. 2. Check if tests/helpers/test_snapshots.py exists. 3. Check if tests/snapshots/ directory exists. 4. If missing: check if snapshot-testing skill exists …
- **claude-opencode-skill-mirror-artifact** (98%)
  - Trigger: Git diffstat shows the same skill files changed under both .claude/skills/ and .opencode/skills/ in the same session/PR.
  - Action: Assume .claude/skills is a mirror/sync artifact; prefer proposals that target .opencode/skills only and avoid inferring new behavior from duplicated diffs.
- **phase3-ticket-scoping-investigation** (97%)
  - Trigger: Investigating a Phase 3 ticket (vp-cf95, vp-6cf1, vp-7d49, vp-f17e, vp-0429) that has empty body or unclear status
  - Action: 1. Check ROADMAP.md for the ticket's Phase 3 role (Orchestration, CLI, Scheduling, Monitoring, Testing) and component description. 2. Check all related Phase 3 tickets to see overall completion status…
- **ticket-scope-check-implementation-gap** (90%)
  - Trigger: Scoping a ticket where skill definition exists but implementation code is missing
  - Action: 1. Verify skill exists (e.g., .opencode/skills/snapshot-testing/SKILL.md). 2. Check if implementation files exist (e.g., tests/helpers/snapshots.py, tests/snapshots/). 3. If skill exists but implement…
- **ticket-investigation-workflow** (89%)
  - Trigger: Completing ticket investigation/scoping work
  - Action: 1. Document findings in ticket notes with clear status. 2. Create investigation notes file (INVESTIGATION_NOTES.md or similar). 3. Commit investigation documentation with clear message. 4. Notify mana…
- **manager-acknowledgment-confirmation** (80%)
  - Trigger: Receiving manager message acknowledging investigation or providing scheduling decision
  - Action: 1. Acknowledge manager message (loom team inbox ack <id>). 2. Update ticket with note documenting manager's decision/acknowledgment. 3. Document next steps (await scheduling, ready for implementation,…
- **large-module-deletion-safety-sweep** (79%)
  - Trigger: Git diffstat shows a large deletion of a core src module (hundreds of lines removed) with accompanying test churn.
  - Action: Assume a refactor/removal and do a safety sweep: search for import/call-site fallout, remove/replace references, ensure tests cover the new path, and run targeted + full test suites before merging.
- **inst-egg-info-deletions-are-cleanup** (76%)
  - Trigger: git diffstat shows large deletions under src/*.egg-info (PKG-INFO, SOURCES.txt, requires.txt, etc.)
  - Action: Assume this is cleanup of generated packaging artifacts, not a functional product change; avoid inferring new behavior changes from it and keep follow-up focused on untracking/ignoring egg-info rather…
- **execution-types-contract-lockstep** (76%)
  - Trigger: Git diff shows meaningful changes in src/vibe_piper/execution.py and/or new execution-related abstractions.
  - Action: Audit src/vibe_piper/types.py for the corresponding public types/protocols/aliases, keep names consistent, and run uv-driven ruff + mypy + a fast pytest slice to catch contract breaks early.
- **git-unmerged-state-blocker** (75%)
  - Trigger: Git summary/status shows unmerged paths (e.g., diffstat lines labeled 'Unmerged <path>') or merge conflict state.
  - Action: Treat the diff as incomplete/low-signal; do not infer product behavior from it. Resolve conflicts first, then re-run format/lint/tests before making further changes or drafting release notes/PR summar…
- **sql-assets-docs-tests-lockstep** (74%)
  - Trigger: A change touches src/vibe_piper/sql_assets.py and tests/test_sql_assets.py and also rewrites docs/sql_assets.md.
  - Action: Treat docs/sql_assets.md as the public contract: update it alongside code changes, and add/adjust tests/test_sql_assets.py to cover the documented behavior; if a symbol becomes public, ensure it's exp…
- **execution-types-lockstep** (74%)
  - Trigger: Git diffstat shows changes in both src/vibe_piper/execution.py and src/vibe_piper/types.py in the same session/PR.
  - Action: Treat src/vibe_piper/types.py as the stable, user-facing contract: move/define any new public dataclasses/enums there, keep src/vibe_piper/execution.py focused on orchestration, and avoid circular imp…
- **pipeline-execution-parity-as-contract** (70%)
  - Trigger: Changes touch pipeline execution and a parity-focused test file (e.g., tests/*parity*.py).
  - Action: Treat parity tests as the contract: keep them explicit about user-facing vs agent-facing execution paths, and prefer simplifying the contract rather than expanding mocking complexity when refactoring …
- **ruff-f401-unused-import** (70%)
  - Trigger: Ruff/lint reports F401: imported but unused
  - Action: Remove the unused import; if intentionally kept for side effects, add an explicit usage or a narrowly-scoped ignore with a comment explaining why.
- **asset-adapters-factory-refactor-safety-sweep** (70%)
  - Trigger: Git diffstat shows a large deletion-heavy refactor in src/vibe_piper/asset_adapters.py alongside changes in src/vibe_piper/asset_factory.py.
  - Action: Assume API shape churn risk: search for imports/usages of adapter and factory symbols, update call sites to match, and run uv-driven checks (ruff format, ruff check, mypy, pytest -m "not integration")…
- **examples-directory-low-signal-diff** (70%)
  - Trigger: Git diffstat shows changes confined to examples/ (especially examples/**/tests/ fixtures like conftest.py) with no src/ changes.
  - Action: Treat the diff as demo/test harness cleanup; avoid inferring product behavior changes. If reviewing, focus on whether example usage still runs and whether CI/test selection should include or exclude e…
- **git-summary-mismatch-low-signal** (70%)
  - Trigger: Autolearn git summary reports changed_files count that does not match the diffstat entries (or file list is missing/truncated).
  - Action: Assume evidence is incomplete; do not infer product behavior. Prefer emitting no-op memory updates (or only workflow hygiene) unless additional diff evidence is available.
- **git-summary-inconsistency-low-signal** (70%)
  - Trigger: Autolearn context shows a mismatch between changed_files count and the diffstat/file list (or other internal inconsistencies).
  - Action: Treat evidence as incomplete/low-signal; avoid inferring product behavior. Prefer no-op memory updates or only workflow hygiene learnings grounded in stable evidence.
- **autolearn-inconsistent-git-summary-low-signal** (70%)
  - Trigger: Autolearn context shows `changed_files` count that does not match the provided `diffstat` (missing files/paths or obviously incomplete summary).
  - Action: Treat evidence as low-signal: avoid inferring product behavior; prefer emitting minimal/empty memory updates unless other stable evidence is present.
- **execution-public-api-stability-sweep** (68%)
  - Trigger: Execution module grows substantially (large insertions) or introduces new entrypoints.
  - Action: Do a quick sweep for downstream breakage: search for imports/usages of the changed symbols, ensure exports stay stable (or rename via a project-wide rename), and add/adjust tests to pin the contract.
- **examples-diff-low-signal** (65%)
  - Trigger: Git diffstat shows changes only under examples/** with no corresponding src/** or tests/** edits.
  - Action: Assume reference/example churn; do not infer product behavior changes or create skills from it. Prefer emitting no memory updates unless repeated evidence suggests a durable workflow principle.
- **examples-only-diff-low-signal** (65%)
  - Trigger: Git diffstat shows changes only under examples/** (and no src/** or tests/** changes).
  - Action: Treat as low-signal for product behavior; avoid proposing new skills/docs changes unless the change repeats across sessions or is explicitly about docs/workflows.

## Notes

- Instincts are the *pre-skill* layer: small, repeatable heuristics.
- When an instinct proves useful across sessions, promote it into a Skill.
<!-- END:compound:instincts-md -->
