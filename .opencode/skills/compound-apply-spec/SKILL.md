---
name: compound-apply-spec
description: Write a CompoundSpec v1 JSON payload and apply it via compound_apply to create/update skills and docs.
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

## CompoundSpec v1

Top-level keys (all optional except `version`):

- `version`: must be `1`
- `sessionID`: session identifier (string)
- `summary`: 1-2 sentences

### `skills`

- `create[]`: `{ name, description, body, tags?, metadata?, compatibility? }`
- `update[]`: `{ name, description?, body, tags?, metadata?, compatibility?, bumpVersion? }`
- `deprecate[]`: `{ name, reason, replacement? }`

Notes:
- `name` must match `^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$`.
- `body` is markdown **without** frontmatter.
- The plugin wraps `body` into a SKILL.md with managed markers + preserved manual notes.

### `docs`

These update AI-managed blocks (human-owned text is left alone):

- `agents_ai_behavior`: bullet list text that gets merged/deduped
- `project_ai_constitution`: bullet list text that gets merged/deduped
- `roadmap_ai_notes`: markdown appended with a date heading

### `memos`

Array of:

- `{ title, body, tags?, scopes?, visibility? }`

Scopes are passed through to `loom memory add`.

## Apply

Call:

- `compound_apply(spec_json="<JSON string>")`
<!-- END:compound:skill-managed -->

## Manual notes

_This section is preserved when the skill is updated. Put human notes, caveats, and exceptions here._
