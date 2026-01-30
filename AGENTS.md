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
  - .opencode/skills/author-agents-md-uv-python/SKILL.md
- **compound-apply-spec** (v1): Write a CompoundSpec v2 JSON payload and apply it via compound_apply to create/update skills and docs.
  - .opencode/skills/compound-apply-spec/SKILL.md
- **compound-workflows** (v1): Use Plan → Work → Review → Compound to compound skills and maintain project context.
  - .opencode/skills/compound-workflows/SKILL.md
- **data-cleaning-implementation** (v1): Update data-cleaning-implementation skill with critical pandas patterns and testing learnings from vp-e62a
  - .opencode/skills/data-cleaning-implementation/SKILL.md
- **drift-detection-implementation** (v1): Implement drift detection features for data quality monitoring including baseline storage, history tracking, thresholds, and validation wrappers
  - .opencode/skills/drift-detection-implementation/SKILL.md
- **egg-info-hygiene** (v1): Handle `src/*.egg-info` diffs in Python repos (uv-driven) to avoid committing generated metadata unintentionally.
  - .opencode/skills/egg-info-hygiene/SKILL.md
- **fastapi-web-framework** (v1): Create FastAPI web server with standard middleware, JWT authentication, and REST API endpoints
  - .opencode/skills/fastapi-web-framework/SKILL.md
- **implement-schema-evolution** (v1): Implement schema evolution features including semantic versioning, migration planning, breaking change detection, and schema history tracking
  - .opencode/skills/implement-schema-evolution/SKILL.md
- **loom-docs-merge-conflicts** (v1): Resolve git merge conflicts in compound-managed LOOM docs (LOOM_CHANGELOG.md, LOOM_ROADMAP.md) without breaking BEGIN/END fences.
  - .opencode/skills/loom-docs-merge-conflicts/SKILL.md
- **loom-manager-workflow** (v1): Manage Loom team tickets as a team manager with limited permissions (no git merge/push, no loom team commands). Handle ticket lifecycle: create, update, add notes, track dependencies, identify blockages.
  - .opencode/skills/loom-manager-workflow/SKILL.md
- **loom-merge-queue-worker** (v1): Process merge queue items as a Loom merge worker - claim, merge, mark done, handle no-op merges, and resolve compound block conflicts correctly.
  - .opencode/skills/loom-merge-queue-worker/SKILL.md
- **loom-team-lifecycle-management** (v1): Manage Loom team worker lifecycle from spawn to retire, including ticket assignment, progress tracking, merge operations, and workspace cleanup.
  - .opencode/skills/loom-team-lifecycle-management/SKILL.md
- **loom-team-manager** (v1): Act as Team Manager for Loom team, handling ticket workflow when team commands may be unavailable
  - .opencode/skills/loom-team-manager/SKILL.md
- **loom-team-manager-pane-dead-on-spawn** (v1): Diagnose and fix Loom team manager pane dying immediately (tmux pane status 1).
  - .opencode/skills/loom-team-manager-pane-dead-on-spawn/SKILL.md
- **loom-ticketing** (v1): Use loom ticket for ticket creation, status updates, deps, and notes.
  - .opencode/skills/loom-ticketing/SKILL.md
- **loom-workspace** (v1): Use loom workspace to create/manage worktrees for isolated execution of tickets.
  - .opencode/skills/loom-workspace/SKILL.md
- **merge-queue-conflict-resolution** (v1): Resolve common merge conflicts in merge-queue worktree including observations.jsonl, uv.lock, pyproject.toml, and egg-info files
  - .opencode/skills/merge-queue-conflict-resolution/SKILL.md
- **monitoring-implementation** (v1): Implement comprehensive monitoring and observability features for Vibe Piper pipelines
  - .opencode/skills/monitoring-implementation/SKILL.md
- **multi-format-config-management** (v1): Implement configuration management supporting TOML/YAML/JSON formats with inheritance and runtime overrides
  - .opencode/skills/multi-format-config-management/SKILL.md
- **python-egg-info-hygiene** (v1): Handle noisy git diffs from src/*.egg-info generated by editable installs (uv) and decide whether to commit or ignore.
  - .opencode/skills/python-egg-info-hygiene/SKILL.md
- **quality-scoring-implementation** (v1): Update quality-scoring-implementation skill with validation notes from completed implementation of ticket vp-d5ae
  - .opencode/skills/quality-scoring-implementation/SKILL.md
- **skill-authoring** (v1): Create high-quality skills: scoped, procedural, and durable. Prefer updates over duplicates.
  - .opencode/skills/skill-authoring/SKILL.md
- **snapshot-testing** (v1): Snapshot testing framework for asserting data structures don't change unexpectedly. Supports JSON serialization, automatic snapshot creation on first run, diff visualization on mismatches, max depth protection, and --update-snapshots flag.
  - .opencode/skills/snapshot-testing/SKILL.md
- **uv-ruff-only-tooling-migration** (v1): Migrate a Python repo from Poetry/Black to UV + Ruff-only (CI, pre-commit, pyproject, docs).
  - .opencode/skills/uv-ruff-only-tooling-migration/SKILL.md
- **validation-history-integration** (v1): Auto-store validation results using store_validation_result() integration utility
  - .opencode/skills/validation-history-integration/SKILL.md
- **validation-history-schema-initialization** (v1): Initialize PostgreSQL schema for validation history tables
  - .opencode/skills/validation-history-schema-initialization/SKILL.md
<!-- END:compound:skills-index -->

<!-- BEGIN:compound:instincts-index -->
- **pandas-string-accessor-error-pattern** (98%)
  - Trigger: Test failures showing text not cleaned (whitespace not trimmed, case not changed) when using pandas DataFrame operations
  - Action: When applying string operations to DataFrame columns in pandas 2.x, always use Series.str accessor. Pattern is: df[col].str.trim() or df[col].str.lower(). Never apply string methods directly to column…
- **nullable-schema-fields-for-data-with-nones** (92%)
  - Trigger: Creating test fixtures with DataRecord objects that will have None/null values in specific fields
  - Action: When creating SchemaField for any field that might contain None in test data, set nullable=True. DataRecord.__post_init__ raises ValueError if a non-nullable field contains None.
- **inst-20250129-003** (90%)
  - Trigger: validation_suite_completed
  - Action: store_validation_result
- **float-to-int-type-safety-in-outlier-replacement** (88%)
  - Trigger: Replacing outliers (always float mean/median) into DataFrame columns with integer dtype
  - Action: Either convert integer column to float before replacement (df[col] = df[col].astype(float)), or explicitly cast mean/median to int (int(mean_val)) when assigning back. Pandas will raise TypeError sett…
- **inst-20250129-001** (85%)
  - Trigger: validation_suite_completed
  - Action: store_validation_result
- **drift-history-timestamp-tracking** (85%)
  - Trigger: Implementing DriftHistory class for drift monitoring
  - Action: Store each drift check with timestamp, baseline_id, method, drift_score, and alert_level to enable temporal trend analysis.
- **loom-docs-merge-conflict-markers** (85%)
  - Trigger: Git diff or file contents show merge conflict markers (<<<<<<<, =======, >>>>>>>) in LOOM_CHANGELOG.md or LOOM_ROADMAP.md (especially inside/near compound-managed fences).
  - Action: Manually resolve by removing conflict markers, preserving the compound BEGIN/END fences, merging content (often keep both sides but dedupe repeated entries), and ensuring lists remain valid markdown. …
- **optional-type-checking-guard** (82%)
  - Trigger: Importing types only for type checking (Schema, DataType) used only in annotations
  - Action: Import optional types inside TYPE_CHECKING block, import at runtime in else block. This allows module to work without the optional dependency.
- **egg-info-diff-hygiene** (82%)
  - Trigger: git diff shows changes under src/*.egg-info/ (PKG-INFO, SOURCES.txt, requires.txt) after installs/runs
  - Action: Assume generated metadata; avoid committing unless intentionally updating packaging. Prefer adding src/*.egg-info/ to .gitignore and cleaning the working tree by removing the generated directories, th…
- **inst-20250129-002** (80%)
  - Trigger: new_history_store_created
  - Action: initialize_schema
- **inst-20250129-004** (80%)
  - Trigger: new_history_store_created
  - Action: initialize_schema
- **drift-baseline-storage-json** (80%)
  - Trigger: Implementing drift detection with historical baseline comparison
  - Action: Store baseline data as JSON file with metadata (timestamp, sample_size, columns, schema_name). Create BaselineStore class with add_baseline, get_baseline, get_metadata, list_baselines, delete_baseline…
- **inst-20260130-001** (80%)
  - Trigger: Producing a CompoundSpec v2 with skills.update entries
  - Action: Re-emit the entire final managed body for any skills.update[].body (no snippets/diffs), keep paths repo-root-relative, and include auto/sessionID plus docs.sync if indexes should refresh.
- **inst-20260130-002** (78%)
  - Trigger: Git shows changes under src/*.egg-info after running uv editable installs (uv pip install -e .).
  - Action: Treat src/*.egg-info as generated noise: avoid committing unless intentionally updating packaging metadata; use the existing python-egg-info-hygiene skill to decide whether to ignore, clean, or commit…
- **baseline-json-storage-pattern** (75%)
  - Trigger: Implementing historical data storage with timestamp, sample_size, columns for later retrieval
  - Action: Save data to JSON files with metadata dict (created_at, sample_size, columns, schema_name). Store data list efficiently. Create methods for add, get, get_metadata, list, delete.
- **drift-threshold-validation** (75%)
  - Trigger: Creating DriftThresholds configuration class
  - Action: Validate that warning < critical, all thresholds in [0,1] range, and warning < psi_critical, with clear error messages.
- **jsonl-append-only-time-series** (72%)
  - Trigger: Storing time-series drift history that only grows
  - Action: Write each entry as a line to JSONL file (json.dumps(entry) + newline). Efficient append-only, easy to read latest N entries with tail.
- **validation-result-wrapper-mapping** (70%)
  - Trigger: Creating wrapper functions that convert domain types to validation framework ValidationResult
  - Action: Create wrapper function that takes domain-specific result (DriftResult) and returns ValidationResult (is_valid=alert_level != 'critical', errors=list for critical, warnings=list for recommendations). …
- **avoid-committing-egg-info** (70%)
  - Trigger: Git diff shows changes under `src/*.egg-info/` (e.g., `PKG-INFO`, `requires.txt`) without intentional packaging/version work
  - Action: Assume the changes are generated; do not include them in commits/PRs and instead regenerate/clean the working tree via the repo's normal install workflow before re-checking diffs
- **egg-info-churn-is-build-noise** (70%)
  - Trigger: git diff shows only src/vibe_piper.egg-info/* changes (PKG-INFO, SOURCES.txt, requires.txt) with no src/ or tests/ edits
  - Action: Assume these are generated artifacts; avoid proposing learnings from them and avoid including them in commits unless the repo explicitly tracks egg-info as source of truth.
<!-- END:compound:instincts-index -->

<!-- BEGIN:compound:rules-index -->
- _(none)_
<!-- END:compound:rules-index -->
