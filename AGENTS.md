# AGENTS

## Compound (OpenCode)

These blocks are maintained by the Loom compound OpenCode plugin.
Do not edit inside the BEGIN/END fences.

<!-- BEGIN:compound:agents-ai-behavior -->
# Compound Engineering Baseline

This block is maintained by the compound plugin.

**Core loop:** Plan → Work → Review → Compound → Repeat.

**Memory model:**
- **Observations** are logged automatically from tool calls and session events.
- **Instincts** are small heuristics extracted from observations.
- **Skills** are durable procedural memory (directory + SKILL.md) and are the primary compounding mechanism.

**Non-negotiables:**
- Keep skills small, specific, and triggerable from the `description`.
- Prefer updating an existing skill over creating a near-duplicate.
- Never put secrets into skills, memos, or observations.
- The plugin may auto-create/update skills. Humans should occasionally prune duplicates.

**Where things live:**
- Skills: `.opencode/skills/<name>/SKILL.md`
- Instincts: `.opencode/memory/instincts.json` (index at `.opencode/memory/INSTINCTS.md`)
- Observations: `.opencode/memory/observations.jsonl` (gitignored by default)
 - Constitution: `LOOM_PROJECT.md`
 - Direction: `LOOM_ROADMAP.md`
<!-- END:compound:agents-ai-behavior -->

<!-- BEGIN:compound:workflow-commands -->
- `/workflows:plan` - Create tickets + plan (uses memory recall)
- `/workflows:work` - Create/manage worktree (workspace) and implement
- `/workflows:review` - Review changes and update tickets
- `/workflows:compound` - Extract learnings into skills + memory + docs
<!-- END:compound:workflow-commands -->

<!-- BEGIN:compound:skills-index -->
- **compound-apply-spec** (v1): Write a CompoundSpec v1 JSON payload and apply it via compound_apply to create/update skills and docs.
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.opencode/skills/compound-apply-spec/SKILL.md
- **compound-workflows** (v1): Use Plan → Work → Review → Compound to compound skills and maintain project context.
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.opencode/skills/compound-workflows/SKILL.md
- **loom-ticketing** (v1): Use loom ticket for ticket creation, status updates, deps, and notes.
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.opencode/skills/loom-ticketing/SKILL.md
- **loom-workspace** (v1): Use loom workspace to create/manage worktrees for isolated execution of tickets.
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.opencode/skills/loom-workspace/SKILL.md
- **skill-authoring** (v1): Create high-quality skills: scoped, procedural, and durable. Prefer updates over duplicates.
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.opencode/skills/skill-authoring/SKILL.md
<!-- END:compound:skills-index -->

<!-- BEGIN:compound:instincts-index -->
- _(none yet)_
<!-- END:compound:instincts-index -->

<!-- BEGIN:compound:rules-index -->
- _(none)_
<!-- END:compound:rules-index -->
