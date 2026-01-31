---
"id": "vp-36be"
"status": "in_progress"
"deps":
- "vp-f701"
"links":
- "vp-7686"
"created": "2026-01-31T17:44:25Z"
"type": "feature"
"priority": 1
"assignee": "z3z1ma"
"parent": "vp-7686"
"tags":
- "sprint:Bootstrap-backlog-objective"
- "phase1"
- "schema"
"external": {}
---
# Adopt schema-first source_path parsing in config + transforms

## Context
We currently have multiple, inconsistent implementations of nested path extraction:
- `src/vibe_piper/pipeline_config/generator.py:_get_nested_value()` supports **dot-only** paths and explicitly documents that bracket/array syntax is not supported.
- `src/vibe_piper/transformations/transforms.py:extract_nested_value()` also supports **dot-only** paths.

Ticket `vp-f701` is implementing `SchemaField.source_path` with dot + bracket syntax (e.g., `items[0].name`) plus mapping utilities.

## Objective
Standardize nested path parsing across the codebase by reusing the schema-first mapping path implementation everywhere we support `source_path`/field extraction.

## Scope
- Introduce or reuse a shared path parser / nested-value getter that supports:
  - dot syntax: `a.b.c`
  - bracket indexing: `items[0].name`
  - mixed paths: `data.items[0].name`
- Update config-driven pipeline transforms:
  - `src/vibe_piper/pipeline_config/generator.py` (EXTRACT_FIELDS step)
  - Remove the "dot-only" limitation in the `_get_nested_value` docstring once bracket syntax is supported.
- Update transformation library:
  - `src/vibe_piper/transformations/transforms.py` (`extract_nested_value` + `extract_fields`)
- Add tests that cover (at minimum):
  - dot paths
  - bracket indexing into lists
  - missing path -> returns `None`
  - non-container nodes encountered mid-path -> returns `None`
  - behavior parity with existing dot-only behavior

## Acceptance Criteria
- [ ] Config-driven `extract_fields` transform supports bracket syntax (e.g., `tags[0]`).
- [ ] `vibe_piper.transformations.extract_fields()` supports bracket syntax.
- [ ] Existing dot-only mappings remain unchanged in behavior.
- [ ] Tests exist for both config-driven and transformation use cases.
- [ ] Documentation/comments no longer claim bracket syntax is unsupported in config transforms.

## Dependencies
- `vp-f701` (schema-first mapping utilities).

## Suggested Order
- After `vp-f701` lands, but can be started in parallel by preparing tests and refactor plan.

## Recon Notes
- `src/vibe_piper/pipeline_config/generator.py` currently says: "For array indexing or bracket syntax, use the schema-first mapping system."
- `src/vibe_piper/transformations/transforms.py` uses `path.split(".")` today.

## Notes

**2026-01-31T17:52:42Z**

Starting implementation: add shared nested path parser/getter with dot+bracket syntax (items[0].name), refactor pipeline_config generator + transformations to use it, and update/extend tests for bracket indexing + missing/non-container parity.

**2026-01-31T17:55:18Z**

Implemented shared dot+bracket nested path parsing in new `vibe_piper.schema.mapping` and refactored pipeline_config generator + transformations to use it. Updated existing tests and added coverage for list indexing + non-container mid-path returning None. Next: run ruff+pytest, then commit milestone.
