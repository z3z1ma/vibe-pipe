---
name: compound-apply-spec
description: Write a CompoundSpec v2 JSON payload and apply it via compound_apply to create/update skills and docs.
license: MIT
compatibility: opencode,claude
metadata:
  created_at: "2026-01-27T17:03:09.731823+00:00"
  updated_at: "2026-01-27T17:03:09.731823+00:00"
  version: "1"
  tags: "skills,compounding,schema"
---
<!-- BEGIN:compound:skill-managed -->
## Why this exists

The plugin is deterministic. The agent is not.
So we separate:

- **Agent**: decides what to learn (writes the spec).
- **Tool** (`compound_apply`): validates and applies changes safely.

## CompoundSpec v2

Top-level shape:

- `schema_version`: must be `2`
- `auto`: `{ reason, sessionID }`
- `instincts`: `{ create[], update[] }`
- `skills`: `{ create[], update[] }`
- `docs`: `{ sync, blocks? }`
- `changelog`: `{ note }`

### `instincts`

- `create[]`: `{ id, title, trigger, action, confidence }`
- `update[]`: `{ id, confidence_delta, evidence_note }`

### `skills`

- `create[]`: `{ name, description, body }`
- `update[]`: `{ name, description?, body }`

Notes:
- `name` should match `^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$`.
- `body` is markdown without frontmatter.
- For `skills.update[]`, `body` must be the entire final managed body (no snippets/diffs).

### `docs`

- Prefer `docs.sync: true` when changing skills/instincts so derived indexes refresh.
- Optionally upsert AI-managed blocks:
  - `docs.blocks.upsert[]`: `{ file, id, content }`

Use repo-root-relative paths when referencing files (e.g., `AGENTS.md`, `LOOM_ROADMAP.md`).

## Output hygiene

- Output exactly one JSON object.
- Do not wrap in code fences.
- Do not include commentary.

## Apply

Call:

- `compound_apply()`
<!-- END:compound:skill-managed -->

## Manual notes

_This section is preserved when the skill is updated. Put human notes, caveats, and exceptions here._
