---
name: compound-workflows
description: Use Plan → Work → Review → Compound loop with loom ticket + loom workspace in a polyrepo workspace (ticket-named branches, worktrees per service).
license: MIT
compatibility: opencode,claude
metadata:
  created_at: "2026-01-27T17:03:09.731823+00:00"
  updated_at: "2026-01-27T17:03:09.731823+00:00"
  version: "1"
  tags: "workflow,compounding"
---
<!-- BEGIN:compound:skill-managed -->
## Purpose

This repository uses a loop:

1. **Plan**: turn an idea into a ticketed plan (loom ticket), informed by recalled notes (loom memory).
2. **Work**: execute in isolated git worktrees (loom workspace), updating ticket status.
3. **Review**: run a multi-angle review before merging.
4. **Compound**: extract reusable patterns into skills (procedural memory) + store memos for future planning.

The point is not vibes. The point is *reusable procedure*.

## Commands

### `/workflows:plan <idea>`

- Recall relevant memos for planning:
  - `compound_memory_recall(query="<idea>", command="workflows:plan")`
- Create/organize tickets:
  - `compound_ticket(argv=["init"])` (if needed)
  - `compound_ticket(argv=["create", "..."])`
  - `compound_ticket(argv=["dep-add", "<id>", "<dep-id>"])` (optional)
- Output a plan with:
  - ticket IDs
  - affected repos/services
  - acceptance criteria and tests
  - risk list

### `/workflows:work <ticket-id>`

- Fetch ticket, set status to `in_progress`.
- Identify affected services via `services/*.md` and `loom workspace deps ...` commands; do not guess.
- Create coordinated branches named **exactly** the ticket ID in each affected repo:
  - `loom workspace branch <TICKET-ID> --repos <service-a> <service-b>`
- Create worktrees (one per service per ticket):
  - `loom workspace worktree add <TICKET-ID> --repos <service-a> <service-b>`
- Implement inside `worktrees/<TICKET-ID>/<service>`.
- Commit in each service repo normally.

### `/workflows:review <ticket-id>`

- Run fast checks (lint/tests/build) appropriate for each service.
- Review with multiple lenses:
  - correctness & maintainability
  - security & foot-guns
  - performance/regressions
  - docs & ergonomics
- Update the ticket with follow-ups.

### `/workflows:compound <ticket-id>`

- Write memory notes that future planning can recall.
- Propose skill/instinct/doc updates as a **CompoundSpec v2** JSON object.
- Apply with `compound_apply()`.

## Operational defaults

- Keep skills small, scoped, and action-oriented.
- Prefer updating an existing skill over creating a near-duplicate.
- If dependencies/interfaces change, update `services/<name>.md` and then run:
  - `loom workspace services refresh-index`
<!-- END:compound:skill-managed -->

## Manual notes

_This section is preserved when the skill is updated. Put human notes, caveats, and exceptions here._
