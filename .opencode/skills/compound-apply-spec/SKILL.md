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

Output **one JSON object** matching this schema:

- `schema_version`: must be `2`
- `auto`: `{ reason, sessionID }`
- `instincts`: `{ create[], update[] }`
- `skills`: `{ create[], update[] }`
- `docs`: `{ sync, blocks: { upsert[] } }`
- `changelog`: `{ note }`

### `instincts`

- `create[]`: `{ id, title, trigger, action, confidence }`
- `update[]`: `{ id, confidence_delta, evidence_note }`

Keep instincts small: *trigger -> action*.

### `skills`

- `create[]`: `{ name, description, body }`
- `update[]`: `{ name, description?, body }`

Rules:
- `name` must match `^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$`.
- `body` is markdown **without** frontmatter.
- For `skills.update[]`, `body` must be the **entire final** managed body (no snippets/diffs).

### `docs`

Use this only to update AI-managed blocks:

- `sync`: set `true` to refresh derived indexes.
- `blocks.upsert[]`: `{ file, id, content }`

Path rule:
- When referencing files/dirs in markdown, use **repo-root-relative** paths.

## Apply

After drafting the JSON spec, run:

- `compound_apply()`

It applies skill/doc/memory updates to the repo's memory files.
<!-- END:compound:skill-managed -->

## Manual notes

_This section is preserved when the skill is updated. Put human notes, caveats, and exceptions here._
