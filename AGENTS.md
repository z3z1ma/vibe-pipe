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
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.team/runs/MiyagiDo/worktrees/vp-6cf1/.opencode/skills/author-agents-md-uv-python/SKILL.md
- **compound-apply-spec** (v1): Write a CompoundSpec v1 JSON payload and apply it via compound_apply to create/update skills and docs.
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.team/runs/MiyagiDo/worktrees/vp-6cf1/.opencode/skills/compound-apply-spec/SKILL.md
- **compound-workflows** (v1): Use Plan → Work → Review → Compound to compound skills and maintain project context.
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.team/runs/MiyagiDo/worktrees/vp-6cf1/.opencode/skills/compound-workflows/SKILL.md
- **loom-team-manager-pane-dead-on-spawn** (v1): Diagnose and fix Loom team manager pane dying immediately (tmux pane status 1).
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.team/runs/MiyagiDo/worktrees/vp-6cf1/.opencode/skills/loom-team-manager-pane-dead-on-spawn/SKILL.md
- **loom-ticketing** (v1): Use loom ticket for ticket creation, status updates, deps, and notes.
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.team/runs/MiyagiDo/worktrees/vp-6cf1/.opencode/skills/loom-ticketing/SKILL.md
- **loom-workspace** (v1): Use loom workspace to create/manage worktrees for isolated execution of tickets.
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.team/runs/MiyagiDo/worktrees/vp-6cf1/.opencode/skills/loom-workspace/SKILL.md
- **skill-authoring** (v1): Create high-quality skills: scoped, procedural, and durable. Prefer updates over duplicates.
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.team/runs/MiyagiDo/worktrees/vp-6cf1/.opencode/skills/skill-authoring/SKILL.md
- **uv-ruff-only-tooling-migration** (v1): Migrate a Python repo from Poetry/Black to UV + Ruff-only (CI, pre-commit, pyproject, docs).
  - /Users/alexanderbutler/code_projects/personal/vibe-piper/.team/runs/MiyagiDo/worktrees/vp-6cf1/.opencode/skills/uv-ruff-only-tooling-migration/SKILL.md
<!-- END:compound:skills-index -->

<!-- BEGIN:compound:instincts-index -->
- _(none yet)_
<!-- END:compound:instincts-index -->

<!-- BEGIN:compound:rules-index -->
- _(none)_
<!-- END:compound:rules-index -->
