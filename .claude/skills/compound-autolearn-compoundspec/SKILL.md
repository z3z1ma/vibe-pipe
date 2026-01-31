---
name: compound-autolearn-compoundspec
description: Generate a CompoundSpec v2 JSON-only autolearn proposal from git diffstat + existing skills, while respecting strict file-scope constraints.
license: MIT
compatibility: opencode,claude
metadata:
  created_at: "2026-01-30T16:42:50.393Z"
  updated_at: "2026-01-30T16:42:50.393Z"
  version: "1"
---
<!-- BEGIN:compound:skill-managed -->
# Purpose
Turn an autolearn prompt (recent activity + constraints) into a valid CompoundSpec v2 JSON object.

# When To Use
- Prompt says "Background Autolearn" or "learning agent".
- Prompt requires JSON-only output and limits edits to memory/docs artifacts.

# Procedure
- Read the constraints and allowed paths; assume anything else is forbidden.
- If a system reminder indicates read-only / Plan Mode:
  - Do not propose product code changes.
  - Keep proposals strictly within skills/instincts/docs/changelog.
- Use the git summary (changed_files + diffstat) as evidence for what to learn.
- Treat generated/derived artifacts as low-signal evidence:
  - If `services/index.json` changes without corresponding `services/*.md` edits, assume it was refreshed and do not infer new dependency learnings.
  - If `.opencode/memory/instincts.json` / `.opencode/memory/INSTINCTS.md` show large rewrites or deletions, assume cleanup and avoid inventing new heuristics from it.
  - If changes appear duplicated under both `.claude/skills/` and `.opencode/skills/`, assume a mirror/sync artifact; prefer learning proposals that target `.opencode/skills/` only.
  - If the diff is primarily additions/deletions under `src/*.egg-info/`, assume packaging metadata cleanup/regeneration noise and avoid learning anything beyond "egg-info is generated" unless the prompt explicitly states an intentional packaging change.
- Prefer:
  - `instincts.update[]` to strengthen an existing heuristic
  - `skills.update[]` to refine an existing skill
  - `skills.create[]` only if there is no close match
- If the diff is primarily ticket/process artifacts (e.g. `.tickets/*.md`):
  - Prefer updating Loom-ticket/workflow-related instincts over creating new skills.
  - Avoid proposing docs block changes unless a stable, always-on principle changed.
- If updating a skill:
  - Re-emit the entire final managed body (no diffs/snippets).
  - Keep it checklist-like.
- Keep instincts crisp:
  - Trigger is a concrete situation.
  - Action is a concrete behavior.
  - Confidence is 0.6-0.85 unless repeated evidence.
- Keep proposals small:
  - Max 3 skills per run.
  - Max 8 instinct updates per run.
- Output hygiene:
  - Output exactly one JSON object.
  - No code fences, no commentary.
  - Use repo-root-relative paths in any markdown content.

# Suggested Minimal Template
- `auto.reason` from prompt (often `session.idle`).
- `auto.sessionID` from prompt.
- `docs.sync: true` if you touched skills/instincts/docs.
- `changelog.note` as a single sentence describing the memory delta.
<!-- END:compound:skill-managed -->

## Manual notes

_This section is preserved when the skill is updated. Put human notes, caveats, and exceptions here._
