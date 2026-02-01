---
"id": "vp-c9ca"
"status": "in_progress"
"deps":
- "vp-07cd"
- "vp-d800"
"links": []
"created": "2026-02-01T03:22:04Z"
"type": "task"
"priority": 2
"assignee": "z3z1ma"
"parent": "vp-981a"
"tags":
- "sprint:The-Great-Pruning"
- "docs"
- "cleanup"
"external": {}
---
# Rewrite README for canonical core

## Objective alignment
A coherent README is the entry point for users. It must reflect the final API and avoid internal or obsolete references.

## Scope
- Rewrite `README.md` to emphasize the canonical AssetGraph/Pipeline models from `CORE_ABSTRACTION_CONTRACT.md`.
- Remove internal build notes (e.g., loom team start command) and outdated links.
- Fix CLI command name to `vibepiper` (not `vibe-piper`).
- Ensure examples use actual APIs and match the pruned codebase.

## Non-goals
- Adding new features or examples beyond what exists.
- Expanding documentation beyond README (handled in separate docs ticket).

## Plan
1. Align structure with core concepts: overview, install, quick start (AssetGraph + Pipeline), configuration, CLI.
2. Remove sections that describe removed modules or non-core features.
3. Validate examples against current code (imports and names).
4. Update links to docs and examples that will remain post-pruning.

## Acceptance criteria
- README is accurate for current codebase and uses `vibepiper` CLI commands.
- No references to removed web/dashboard/frontend features.
- Examples import symbols that exist in `src/vibe_piper`.

## Verification
- `uv run python -c "import vibe_piper"`
- `uv run vibepiper --help`

## Risks / edge cases
- Example code may rely on optional dependencies; note optional installs explicitly.

## Dependencies
- Depends on public API pruning and web/dashboard removal tickets.


## Acceptance Criteria

README reflects canonical API, uses vibepiper CLI, no internal/obsolete sections

## Notes

**2026-02-01T03:54:55Z**

# README Rewrite Plan

## Analysis

### Current State
- CLI command:  (correct in pyproject.toml)
- Still has  command and web dependencies in this worktree (awaiting manager merge)
- Examples directory exists with: api_ingestion/, etl_pipeline/, pipelines/, sample_pipeline/
- Core exports properly defined in __init__.py (270 symbols)

### Key Changes Needed
1. **Remove internal build notes** (lines 43-54: loom team start command)
2. **Fix CLI commands**: Use  consistently (README still has  in CLI section)
3. **Emphasize canonical models**: AssetGraph (production) vs Pipeline (simple scripts)
4. **Remove dashboard/web references**: Once merged from vp-d800
5. **Validate examples**: Ensure imports match actual __init__.py exports
6. **Simplify structure**: Focus on core concepts without non-core features

### Structure
1. Overview + Choosing right model (AssetGraph vs Pipeline)
2. Installation (core + optional extras)
3. Quick Start 1: Production Pipeline (AssetGraph) - canonical path
4. Quick Start 2: Simple Script (Pipeline) - lightweight path
5. Configuration
6. CLI Usage
7. Architecture (simplified, canonical models only)
8. Development (testing, linting)
9. Documentation links

Proceeding with rewrite based on current canonical API and CORE_ABSTRACTION_CONTRACT.md.

**2026-02-01T03:57:45Z**

## Work Completed

### Changes Made
1. ✅ Removed internal build notes (loom team start command)
2. ✅ Rewrote README to emphasize canonical AssetGraph/Pipeline models from CORE_ABSTRACTION_CONTRACT.md
3. ✅ All CLI commands use 'vibepiper' (package name 'vibe-piper' for pip is correct)
4. ✅ Removed all dashboard/web references
5. ✅ Streamlined structure: Overview → Features → Quick Start → Installation → Choosing Models → Config → CLI → Architecture → Docs → Development
6. ✅ Validated all example imports against actual __init__.py exports
7. ✅ Updated examples to import from 'vibe_piper' (not 'vibe_piper.connectors')
8. ✅ Simplified features section to focus on core capabilities

### Verification Results
✅ `uv run python -c "import vibe_piper"` - Imports successfully (257 symbols, v0.1.0)
✅ `uv run vibepiper --help` - CLI command works correctly

### Known Issues
- CLI still shows 'dashboard' command in help output (vp-d800 changes not merged to this worktree yet)
- This is expected per manager message: 'I am currently working on merging changes to main'

### Acceptance Criteria Met
✅ README reflects canonical API
✅ Uses vibepiper CLI commands
✅ No internal/obsolete sections
✅ Examples import from vibe_piper
✅ Emphasizes AssetGraph vs Pipeline from CORE_ABSTRACTION_CONTRACT.md

### Commit
SHA: 1d2d38d - docs: rewrite README for canonical core

**2026-02-01T04:14:44Z**

## Status Update

Requested manager review at SHA 1d2d38d with verification details. Waiting for manager approval.

**2026-02-01T04:30:23Z**

## Standing By

Per manager message, Phase 1 and 2 are complete but team is blocked on merge due to integrator spawn issue.

### Ticket Status
All work for vp-c9ca is complete:
- ✅ README rewritten for canonical core API
- ✅ Commit created: 1d2d38d
- ✅ READY_FOR_REVIEW sent to manager
- ✅ All acceptance criteria met

### Waiting For
- Manager to resolve merge/integrator issue
- Potential merge of vp-d800 (web/dashboard removal) to remove CLI dashboard command
- Manager review and approval of my work

No further action required until merge block is resolved.
