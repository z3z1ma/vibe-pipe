# AGENTS

## Repo Commands (Use UV)

This repo is Python (src layout) and is meant to be driven via `uv`.

Hard rules:
- Use `uv` and only `uv` for dependency management and running tools.
- Do not use Poetry (`poetry`, `poetry.lock`) in this repo.
- Do not use Black. Formatting is done with `ruff format`.

### Setup

- Install dev deps (matches CI): `uv sync --dev` then `uv pip install -e .`
- Optional connector extras (as needed): `uv pip install -e ".[postgres,mysql,snowflake,bigquery]"`

### Format / Lint / Typecheck

- Format (preferred): `uv run ruff format src tests`
- Lint: `uv run ruff check src tests`
- Lint + autofix: `uv run ruff check --fix src tests`
- Typecheck (strict): `uv run mypy src`

Notes:
- Ruff config is in `pyproject.toml` (`[tool.ruff]`, `[tool.ruff.lint]`).
- Formatting is `ruff format` only (no Black).

### Tests (pytest)

- All tests: `uv run pytest`
- Fast unit-only (skip integration): `uv run pytest -m "not integration"`
- Single file: `uv run pytest tests/test_pipeline.py -q`
- Single test (node id): `uv run pytest tests/test_pipeline.py::test_pipeline_execution -q`
- By keyword: `uv run pytest -k "pipeline_execution" -q`
- With coverage (local): `uv run pytest --cov=src --cov-report=term-missing`

### Integration tests (Docker)

- Spin up test DBs: `docker-compose -f docker-compose.test.yml up -d`
- Run integration tests: `uv run pytest -m integration`
- Tear down: `docker-compose -f docker-compose.test.yml down`
- Scripted: `./scripts/run_integration_tests.sh`

### Docs

- Build docs: `cd docs && uv run sphinx-build -b html source build/html`
- Sphinx Make targets: `cd docs && make html`

### CLI

- CLI entrypoint is `vibepiper` (see `pyproject.toml` `[project.scripts]`).
- Examples: `uv run vibepiper --help`, `uv run vibepiper validate .`, `uv run vibepiper run . --env=dev`
- Note: Some docs mention `vibe-piper`, but the configured script is `vibepiper`.

## Code Style (Enforced By Ruff + MyPy)

### Formatting

- Keep lines <= 100 chars (see `pyproject.toml` `[tool.ruff] line-length = 100`).
- Prefer Ruff's formatter (`uv run ruff format`) over manual alignment.

### Imports

- Ruff enforces import sorting (`pyproject.toml` selects `I`).
- Group imports in this order: stdlib, third-party, local `vibe_piper.*`.
- Prefer `from collections.abc import ...` over `typing.*` collections.

### Types

- Code targets Python 3.12+ (use `X | Y`, `list[str]`, `dict[str, Any]`).
- MyPy is strict for `src/` (see `pyproject.toml` `[tool.mypy] strict = true`).
- Every public function/method must have type hints; avoid untyped `Any` in APIs.
- Prefer `Protocol` for extensibility points and `TypeAlias` for readability (see `src/vibe_piper/types.py`).

### Naming

- Modules, functions, variables: `snake_case`.
- Classes, protocols, dataclasses: `CamelCase`.
- Exceptions: `SomethingError`.
- Constants: `UPPER_SNAKE_CASE`.
- Type variables: `T`, `R`, or more explicit `T_input`, `T_output`.

### Data structures

- Prefer `@dataclass(frozen=True)` for value objects (schemas, results, records).
- Use `Mapping[...]` for read-only inputs and `dict[...]` only when mutation is intended.

### Error handling

- Prefer explicit exception types and clear messages.
- Follow the pattern used across the repo:
  - `msg = "..."; raise ValueError(msg)` / `raise KeyError(msg)`
  - Wrap with context when re-raising: `raise ValueError(msg) from e`
- Don’t swallow exceptions; either re-raise or convert to a domain error with `from e`.
- For retryable operations, use `vibe_piper.error_handling.retry_with_backoff` or `RetryConfig`.
- Logging: use `logger = logging.getLogger(__name__)` and log at `warning` for retries, `error` on final failure.

### Tests

- Use pytest; prefer fixtures from `tests/conftest.py`.
- Use `@pytest.mark.integration` for DB/network tests (marker declared in `pyproject.toml`).
- Prefer assertion helpers in `tests/helpers/` for schema/graph comparisons.

## Editor / Copilot / Cursor Rules

- No `.cursorrules`, `.cursor/rules/`, or `.github/copilot-instructions.md` found in this repo.

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
- **author-agents-md-uv-python** (v1): Create/update AGENTS.md for a Python repo driven by uv (ruff/mypy/pytest), including single-test commands and editor rule discovery.
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.team/runs/MiyagiDo/worktrees/vp-e6da/.opencode/skills/author-agents-md-uv-python/SKILL.md
- **compound-apply-spec** (v1): Write a CompoundSpec v1 JSON payload and apply it via compound_apply to create/update skills and docs.
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.team/runs/MiyagiDo/worktrees/vp-e6da/.opencode/skills/compound-apply-spec/SKILL.md
- **compound-workflows** (v1): Use Plan → Work → Review → Compound to compound skills and maintain project context.
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.team/runs/MiyagiDo/worktrees/vp-e6da/.opencode/skills/compound-workflows/SKILL.md
- **loom-manager-workflow** (v1): Manage Loom team tickets as a team manager with limited permissions (no git merge/push, no loom team commands). Handle ticket lifecycle: create, update, add notes, track dependencies, identify blockages.
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.team/runs/MiyagiDo/worktrees/vp-e6da/.opencode/skills/loom-manager-workflow/SKILL.md
- **loom-merge-queue-worker** (v1): Process merge queue items as a Loom merge worker - claim, merge, mark done, and handle no-op merges correctly.
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.team/runs/MiyagiDo/worktrees/vp-e6da/.opencode/skills/loom-merge-queue-worker/SKILL.md
- **loom-team-lifecycle-management** (v1): Manage Loom team worker lifecycle from spawn to retire, including ticket assignment, progress tracking, merge operations, and workspace cleanup.
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.team/runs/MiyagiDo/worktrees/vp-e6da/.opencode/skills/loom-team-lifecycle-management/SKILL.md
- **loom-team-manager** (v1): Act as Team Manager for Loom team, handling ticket workflow when team commands may be unavailable
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.team/runs/MiyagiDo/worktrees/vp-e6da/.opencode/skills/loom-team-manager/SKILL.md
- **loom-team-manager-pane-dead-on-spawn** (v1): Diagnose and fix Loom team manager pane dying immediately (tmux pane status 1).
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.team/runs/MiyagiDo/worktrees/vp-e6da/.opencode/skills/loom-team-manager-pane-dead-on-spawn/SKILL.md
- **loom-ticketing** (v1): Use loom ticket for ticket creation, status updates, deps, and notes.
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.team/runs/MiyagiDo/worktrees/vp-e6da/.opencode/skills/loom-ticketing/SKILL.md
- **loom-workspace** (v1): Use loom workspace to create/manage worktrees for isolated execution of tickets.
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.team/runs/MiyagiDo/worktrees/vp-e6da/.opencode/skills/loom-workspace/SKILL.md
- **monitoring-implementation** (v1): Implement comprehensive monitoring and observability features for Vibe Piper pipelines
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.team/runs/MiyagiDo/worktrees/vp-e6da/.opencode/skills/monitoring-implementation/SKILL.md
- **skill-authoring** (v1): Create high-quality skills: scoped, procedural, and durable. Prefer updates over duplicates.
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.team/runs/MiyagiDo/worktrees/vp-e6da/.opencode/skills/skill-authoring/SKILL.md
- **uv-ruff-only-tooling-migration** (v1): Migrate a Python repo from Poetry/Black to UV + Ruff-only (CI, pre-commit, pyproject, docs).
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.team/runs/MiyagiDo/worktrees/vp-e6da/.opencode/skills/uv-ruff-only-tooling-migration/SKILL.md
<!-- END:compound:skills-index -->

<!-- BEGIN:compound:instincts-index -->
- **sync-ticket-updates** (90%)
  - Trigger: After any `loom ticket` command that changes state (close, add-note, update)
  - Action: Run `loom ticket sync` to commit ticket changes to repository
- **phase3-ticket-sequencing** (90%)
  - Trigger: Starting Phase 3 ticket implementation
  - Action: Orchestration Engine (vp-cf95) must complete before CLI (vp-6cf1), Scheduling (vp-7d49), and Monitoring (vp-f17e) can fully integrate. Verify orchestration engine is in codebase before spawning worker…
- **optional-dependency-import-pattern** (90%)
  - Trigger: Need to use external library that may not be installed
  - Action: 1. Wrap import in try/except block 2. Add type: ignore[import-untyped] comment for mypy 3. Set flag variable to track availability 4. Provide fallback behavior when unavailable 5. Document degradation…
- **verify-code-before-ticket-close** (85%)
  - Trigger: Worker marks ticket ready for review/merge
  - Action: Before closing ticket, use glob to verify expected code files exist in codebase (e.g., src/vibe_piper/connectors/*.py for database connector tickets). This prevents closing tickets where implementatio…
- **datetime-none-coalescing** (85%)
  - Trigger: Performing datetime arithmetic with potentially None fields
  - Action: Always use: (field or datetime.utcnow()) to ensure arithmetic operations have datetime, not Optional[datetime]
- **check-loom-permissions-early** (80%)
  - Trigger: Acting as Team Manager for Loom team
  - Action: Verify permission rules allow `loom team *` commands early. If only `loom ticket *` is allowed, document blocker immediately and proceed with ticket-only workflow.
- **loom-manager-add-ticket-notes** (80%)
  - Trigger: Created new ticket via loom ticket create
  - Action: Immediately add a note to the ticket with: 1) Tasks numbered list, 2) Dependencies (if any), 3) Acceptance criteria, 4) Technical notes, 5) Example usage (if feature)
- **merge-worker-workspace-cleanup** (80%)
  - Trigger: Attempting to retire worker after merge
  - Action: When 'loom team retire <worker>' fails with 'modified or untracked files' error, check worktree status with git status and either: 1) Stash uncommitted changes, 2) Use --force flag if safe to delete, …
- **loom-manager-dependency-notes** (75%)
  - Trigger: Starting work on a ticket that depends on others or creating a ticket that other tickets will depend on
  - Action: Add a note starting with 'DEPENDENCIES:' listing all ticket IDs this ticket depends on, with brief explanation of relationship and priority guidance (when to work on this)
- **formatter-type-separation** (75%)
  - Trigger: MyPy complains about incompatible formatter assignments
  - Action: Create distinct variables (json_formatter, colored_formatter, simple_formatter) instead of reusing single 'formatter' variable to avoid type checker confusion
- **verify-code-before-closing-tickets** (70%)
  - Trigger: Ticket marked 'ready to merge' or 'implementation complete'
  - Action: Use glob/read to verify actual code files exist in codebase before closing the ticket. Look for expected file paths (e.g., src/vibe_piper/connectors/*.py for database connector tickets)
- **loom-manager-check-merge-blockage** (70%)
  - Trigger: Listing tickets shows in_progress status but implementation is complete
  - Action: For each in_progress ticket: check if implementation is done (git log shows recent feature commits, ticket notes say 'implementation complete'), if yes but not merged: add URGENT note with commit hash…
- **loom-manager-phase-organization** (70%)
  - Trigger: Creating tickets for sequential project phases
  - Action: Add phase tags (phase1, phase2, phase3) to tickets and ensure lower-priority tickets have dependency notes referencing higher-priority phase tickets that must complete first
- **loom-manager-inspect-before-action** (65%)
  - Trigger: About to merge a ticket branch or assess completion status
  - Action: Run git log <branch> --oneline -5 to see recent commits, git diff main <branch> --stat to see change scope, check if commits look like implementation vs cleanup, verify branch exists and is ahead of m…
<!-- END:compound:instincts-index -->

<!-- BEGIN:compound:rules-index -->
- _(none)_
<!-- END:compound:rules-index -->
