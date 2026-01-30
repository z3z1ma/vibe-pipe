---
name: compound-workflows
description: Use Plan → Work → Review → Compound to compound skills and maintain project context.
license: MIT
compatibility: opencode,claude
metadata:
  created_at: "2026-01-27T17:03:09.731823+00:00"
  updated_at: "2026-01-27T17:03:09.731823+00:00"
  version: "1"
  tags: "workflow,compounding"
---
<!-- BEGIN:compound:skill-managed -->
# Purpose
Document the Plan → Work → Review → Compound workflow pattern used for testing infrastructure development.

# When To Use
- User needs to understand the workflow pattern for implementing complex features
- Referencing this workflow for future feature development

# Workflow Description

## 1. Plan
- Turn an idea into a ticketed plan (loom ticket)
- Informed by recalled notes (loom memory)
- Create/organize tickets
- Define sequencing/dependencies
- Specify acceptance criteria and tests
- Identify risk list

## 2. Work
- Execute in an isolated git worktree (loom workspace)
- Update ticket status as progress is made
- Implement the feature
- Run tests
- Address issues

## 3. Review
- Run fast checks (lint/tests if applicable)
- Do a review pass with multiple lenses:
  - Correctness & maintainability
  - Security & footguns
  - Performance/regressions
  - Docs & ergonomics
- Update ticket with required follow-ups

## 4. Compound
- Extract reusable patterns into skills (procedural memory)
- Store memos for future planning
- Propose skill changes as a CompoundSpec v2 JSON object
- Call `compound_apply()` to apply the spec
- Sync derived indexes (docs sync)
- Append an agent-optimized CHANGELOG entry

# Example: Testing Infrastructure
- Plan: Created ticket vp-0429 for testing infrastructure
- Work: Implemented snapshot, performance, and data quality frameworks in isolated worktree
- Review: Ran tests, verified functionality, addressed issues
- Compound: Created snapshot-testing, performance-testing, and data-quality-testing skills; updated compound-workflows skill; documented workflow in CHANGELOG

# Commands

### /workflows:plan <idea>
- Recall relevant memos for planning: `compound_memory_recall(query="<idea>", command="workflows:plan")`
- Create/organize tickets:
  - `compound_ticket(argv=["init"])` (if needed)
  - `compound_ticket(argv=["create", "..."])`
  - `compound_ticket(argv=["dep-add", "<id>", "<dep-id>"])` (optional)

### /workflows:work <ticket-id>
- Fetch ticket, set status to `in_progress`
- Create/ensure worktree
- Branch convention: `ticket-<id>-<slug>`
- `compound_workspace(argv=["repo", "worktree", "add", "<branch>"])`
- Do work in that worktree
- Update ticket as you go (`add-note`, `update --status`)

### /workflows:review <ticket-id>
- Run fast checks (lint/tests if applicable)
- Do a review pass with multiple lenses
- Update ticket with required follow-ups

### /workflows:compound <ticket-id>
- Write memory notes (loom memory) that future planning can recall
- Produce a single CompoundSpec v2 JSON object (skills/instincts/docs/changelog)
- Run `compound_apply()` to apply it

# Operational Defaults
- Keep skills small, scoped, and action-oriented
- Prefer updating an existing skill over creating a near-duplicate
- A skill should be applicable in at least 2 future contexts
<!-- END:compound:skill-managed -->

## Manual notes

_This section is preserved when the skill is updated. Put human notes, caveats, and exceptions here._
