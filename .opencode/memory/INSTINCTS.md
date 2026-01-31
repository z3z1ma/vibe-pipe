# INSTINCTS

This is the *fast index* of the current instinct set.
The source of truth is `.opencode/memory/instincts.json`.

<!-- BEGIN:compound:instincts-md -->
## Active instincts (top confidence)

- **memory-store-mass-diff-low-signal** (98%)
  - Trigger: Git diffstat shows large deletions/rewrites in .opencode/memory/instincts.json and/or .opencode/memory/INSTINCTS.md without corresponding product code changes.
  - Action: Assume bookkeeping/cleanup; avoid inferring new behaviors. Prefer small updates to existing instincts/skills, or no-op if evidence is only memory-store churn.
- **claude-opencode-skill-mirror-artifact** (91%)
  - Trigger: Git diffstat shows the same skill files changed under both .claude/skills/ and .opencode/skills/ in the same session/PR.
  - Action: Assume .claude/skills is a mirror/sync artifact; prefer proposals that target .opencode/skills only and avoid inferring new behavior from duplicated diffs.
- **snapshot-testing-implementation-check** (90%)
  - Trigger: Investigating Phase 3 testing or any testing-related ticket
  - Action: 1. Check if tests/helpers/snapshots.py exists. 2. Check if tests/helpers/test_snapshots.py exists. 3. Check if tests/snapshots/ directory exists. 4. If missing: check if snapshot-testing skill exists …
- **phase3-ticket-scoping-investigation** (85%)
  - Trigger: Investigating a Phase 3 ticket (vp-cf95, vp-6cf1, vp-7d49, vp-f17e, vp-0429) that has empty body or unclear status
  - Action: 1. Check ROADMAP.md for the ticket's Phase 3 role (Orchestration, CLI, Scheduling, Monitoring, Testing) and component description. 2. Check all related Phase 3 tickets to see overall completion status…
- **ticket-scope-check-implementation-gap** (80%)
  - Trigger: Scoping a ticket where skill definition exists but implementation code is missing
  - Action: 1. Verify skill exists (e.g., .opencode/skills/snapshot-testing/SKILL.md). 2. Check if implementation files exist (e.g., tests/helpers/snapshots.py, tests/snapshots/). 3. If skill exists but implement…
- **ticket-and-doc-churn-low-signal** (78%)
  - Trigger: Git diffstat is dominated by .tickets/*.md and LOOM_*.md/AGENTS.md edits with no corresponding product code changes.
  - Action: Avoid inventing new product behaviors; limit proposals to workflow hygiene (skills/instinct wording tweaks, docs.sync) and keep docs block edits minimal unless a stable always-on principle changed.
- **ticket-investigation-workflow** (75%)
  - Trigger: Completing ticket investigation/scoping work
  - Action: 1. Document findings in ticket notes with clear status. 2. Create investigation notes file (INVESTIGATION_NOTES.md or similar). 3. Commit investigation documentation with clear message. 4. Notify mana…
- **manager-acknowledgment-confirmation** (70%)
  - Trigger: Receiving manager message acknowledging investigation or providing scheduling decision
  - Action: 1. Acknowledge manager message (loom team inbox ack <id>). 2. Update ticket with note documenting manager's decision/acknowledgment. 3. Document next steps (await scheduling, ready for implementation,…

## Notes

- Instincts are the *pre-skill* layer: small, repeatable heuristics.
- When an instinct proves useful across sessions, promote it into a Skill.
<!-- END:compound:instincts-md -->
