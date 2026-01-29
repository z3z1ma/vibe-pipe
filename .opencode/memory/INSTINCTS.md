# INSTINCTS

This is the *fast index* of the current instinct set.
The source of truth is `.opencode/memory/instincts.json`.

<!-- BEGIN:compound:instincts-md -->
## Active instincts (top confidence)

- **sync-ticket-updates** (90%)
  - Trigger: After any `loom ticket` command that changes state (close, add-note, update)
  - Action: Run `loom ticket sync` to commit ticket changes to repository
- **phase3-ticket-sequencing** (90%)
  - Trigger: Starting Phase 3 ticket implementation
  - Action: Orchestration Engine (vp-cf95) must complete before CLI (vp-6cf1), Scheduling (vp-7d49), and Monitoring (vp-f17e) can fully integrate. Verify orchestration engine is in codebase before spawning worker…
- **optional-dependency-import-pattern** (90%)
  - Trigger: Need to use external library that may not be installed
  - Action: 1. Wrap import in try/except block 2. Add type: ignore[import-untyped] comment for mypy 3. Set flag variable to track availability 4. Provide fallback behavior when unavailable 5. Document degradation…
- **verify-code-before-ticket-close** (85%)
  - Trigger: Worker marks ticket ready for review/merge
  - Action: Before closing ticket, use glob to verify expected code files exist in codebase (e.g., src/vibe_piper/connectors/*.py for database connector tickets). This prevents closing tickets where implementatio…
- **datetime-none-coalescing** (85%)
  - Trigger: Performing datetime arithmetic with potentially None fields
  - Action: Always use: (field or datetime.utcnow()) to ensure arithmetic operations have datetime, not Optional[datetime]
- **check-loom-permissions-early** (80%)
  - Trigger: Acting as Team Manager for Loom team
  - Action: Verify permission rules allow `loom team *` commands early. If only `loom ticket *` is allowed, document blocker immediately and proceed with ticket-only workflow.
- **loom-manager-add-ticket-notes** (80%)
  - Trigger: Created new ticket via loom ticket create
  - Action: Immediately add a note to the ticket with: 1) Tasks numbered list, 2) Dependencies (if any), 3) Acceptance criteria, 4) Technical notes, 5) Example usage (if feature)
- **merge-worker-workspace-cleanup** (80%)
  - Trigger: Attempting to retire worker after merge
  - Action: When 'loom team retire <worker>' fails with 'modified or untracked files' error, check worktree status with git status and either: 1) Stash uncommitted changes, 2) Use --force flag if safe to delete, …
- **loom-manager-dependency-notes** (75%)
  - Trigger: Starting work on a ticket that depends on others or creating a ticket that other tickets will depend on
  - Action: Add a note starting with 'DEPENDENCIES:' listing all ticket IDs this ticket depends on, with brief explanation of relationship and priority guidance (when to work on this)
- **formatter-type-separation** (75%)
  - Trigger: MyPy complains about incompatible formatter assignments
  - Action: Create distinct variables (json_formatter, colored_formatter, simple_formatter) instead of reusing single 'formatter' variable to avoid type checker confusion
- **verify-code-before-closing-tickets** (70%)
  - Trigger: Ticket marked 'ready to merge' or 'implementation complete'
  - Action: Use glob/read to verify actual code files exist in codebase before closing the ticket. Look for expected file paths (e.g., src/vibe_piper/connectors/*.py for database connector tickets)
- **loom-manager-check-merge-blockage** (70%)
  - Trigger: Listing tickets shows in_progress status but implementation is complete
  - Action: For each in_progress ticket: check if implementation is done (git log shows recent feature commits, ticket notes say 'implementation complete'), if yes but not merged: add URGENT note with commit hash…
- **loom-manager-phase-organization** (70%)
  - Trigger: Creating tickets for sequential project phases
  - Action: Add phase tags (phase1, phase2, phase3) to tickets and ensure lower-priority tickets have dependency notes referencing higher-priority phase tickets that must complete first
- **loom-manager-inspect-before-action** (65%)
  - Trigger: About to merge a ticket branch or assess completion status
  - Action: Run git log <branch> --oneline -5 to see recent commits, git diff main <branch> --stat to see change scope, check if commits look like implementation vs cleanup, verify branch exists and is ahead of m…

## Notes

- Instincts are the *pre-skill* layer: small, repeatable heuristics.
- When an instinct proves useful across sessions, promote it into a Skill.
<!-- END:compound:instincts-md -->
