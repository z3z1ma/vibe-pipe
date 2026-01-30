---
name: compound-apply-spec
description: Write a CompoundSpec v2 JSON payload and apply it via compound_apply to create/update skills, instincts, and AI-managed docs blocks.
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

Return **one JSON object** (no code fences, no extra text) with:

- `schema_version`: must be `2`
- `auto`: `{ reason, sessionID }`
- `instincts`: `{ create?: [], update?: [] }`
- `skills`: `{ create?: [], update?: [] }`
- `docs`: `{ sync?: boolean, blocks?: { upsert?: [] } }`
- `changelog`: `{ note }`

### `instincts`

- `create[]`: `{ id, title, trigger, action, confidence }`
- `update[]`: `{ id, confidence_delta, evidence_note }`

Notes:
- Keep triggers concrete and action checklists.
- Keep confidence realistic; update confidence with new evidence.

### `skills`

- `create[]`: `{ name, description, body }`
- `update[]`: `{ name, description?, body }`

Notes:
- `name` should match `^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$`.
- `body` is markdown without frontmatter.
- For `skills.update[]`, `body` must be the **entire final managed body** (not a diff).

### `docs`

Use this to keep AI-managed blocks consistent:

- `sync: true` to refresh derived indexes/blocks.
- `blocks.upsert[]`: `{ file, id, content }`
  - Use short, stable bullets.
  - Do not edit human-owned text.

### `changelog`

- `note`: a short AI-first memory delta (what changed and why).

## Apply

Workflow:

1. Produce the CompoundSpec v2 JSON as the assistant output.
2. Run `compound_apply()` to apply it.
<!-- END:compound:skill-managed -->

## Manual notes

_This section is preserved when the skill is updated. Put human notes, caveats, and exceptions here._
