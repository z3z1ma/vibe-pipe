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

Top-level keys:

- `schema_version`: must be `2`
- `auto`: `{ reason, sessionID }`
- `instincts`: `{ create[], update[] }`
- `skills`: `{ create[], update[] }`
- `docs`: `{ sync: true|false }`
- `changelog`: `{ note }`

### `instincts`

- `create[]`: `{ id, title, trigger, action, confidence }`
- `update[]`: `{ id, confidence_delta, evidence_note }`

Notes:
- Keep instincts small: one clear trigger, one concrete action.
- Prefer updates over creating near-duplicates.

### `skills`

- `create[]`: `{ name, description, body }`
- `update[]`: `{ name, description?, body }`

Notes:
- `name` must match `^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$`.
- `body` is markdown without frontmatter.
- For `skills.update[]`, `body` MUST be the entire final managed body (no snippets/diffs).

### `docs`

- `sync: true` refreshes AI-managed blocks and derived indexes (for example `.opencode/memory/INSTINCTS.md`).

### `changelog`

- `note` is a short, AI-first summary of what changed in memory.

## Output rules (especially for autolearn prompts)

- Output exactly one valid JSON object.
- Do not wrap in code fences.
- Do not add commentary outside JSON.

## Path rule

- When referencing repository files/directories in any markdown you emit (skill bodies, docs, changelog), use repo-root-relative paths.
- Avoid absolute paths and URIs like `file://...`.

## Apply

1. Produce a single valid JSON object matching the schema.
2. Run `compound_apply()`.
<!-- END:compound:skill-managed -->

## Manual notes

_This section is preserved when the skill is updated. Put human notes, caveats, and exceptions here._
