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
- **compound-apply-spec** (v1): Write a CompoundSpec v1 JSON payload and apply it via compound_apply to create/update skills and docs.
  - .opencode/skills/compound-apply-spec/SKILL.md
- **compound-workflows** (v1): Use Plan → Work → Review → Compound to compound skills and maintain project context.
  - .opencode/skills/compound-workflows/SKILL.md
- **data-cleaning-implementation** (v1): Implement comprehensive data cleaning utilities with deduplication, null handling, outlier detection/treatment, type normalization, standardization, and text cleaning
  - .opencode/skills/data-cleaning-implementation/SKILL.md
- **fastapi-web-framework** (v1): Create FastAPI web server with standard middleware, JWT authentication, and REST API endpoints
  - .opencode/skills/fastapi-web-framework/SKILL.md
- **implement-schema-evolution** (v1): Implement schema evolution features including semantic versioning, migration planning, breaking change detection, and schema history tracking
  - .opencode/skills/implement-schema-evolution/SKILL.md
- **loom-manager-workflow** (v1): Manage Loom team tickets as a team manager with limited permissions (no git merge/push, no loom team commands). Handle ticket lifecycle: create, update, add notes, track dependencies, identify blockages.
  - .opencode/skills/loom-manager-workflow/SKILL.md
- **loom-merge-queue-worker** (v1): Process merge queue items as a Loom merge worker - claim, merge, mark done, and handle no-op merges correctly.
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
- **monitoring-implementation** (v1): Implement comprehensive monitoring and observability features for Vibe Piper pipelines
  - .opencode/skills/monitoring-implementation/SKILL.md
- **multi-format-config-management** (v1): Implement configuration management supporting TOML/YAML/JSON formats with inheritance and runtime overrides
  - .opencode/skills/multi-format-config-management/SKILL.md
- **quality-scoring-implementation** (v1): Implement comprehensive data quality scoring with multi-dimensional assessment, historical tracking, threshold alerts, and improvement recommendations
  - .opencode/skills/quality-scoring-implementation/SKILL.md
- **skill-authoring** (v1): Create high-quality skills: scoped, procedural, and durable. Prefer updates over duplicates.
  - .opencode/skills/skill-authoring/SKILL.md
- **uv-ruff-only-tooling-migration** (v1): Migrate a Python repo from Poetry/Black to UV + Ruff-only (CI, pre-commit, pyproject, docs).
  - .opencode/skills/uv-ruff-only-tooling-migration/SKILL.md
- **validation-history-integration** (v1): Integrate validation history auto-storage with existing validation framework
  - .opencode/skills/validation-history-integration/SKILL.md
<!-- END:compound:skills-index -->

<!-- BEGIN:compound:instincts-index -->
- **nullable-schema-fields-for-tests** (95%)
  - Trigger: Creating test fixtures with DataRecord objects that have null values in specific fields
  - Action: Set nullable=True on SchemaField definitions for any field that may contain None in test data. DataRecord.__post_init__ raises ValueError if a non-nullable field contains None.
- **yaml-dev-deps-for-mypy** (95%)
  - Trigger: Adding YAML format support in strict MyPy project
  - Action: Install pyyaml runtime dependency and types-pyamal as dev dependency to satisfy strict type checking
- **retry-metrics-mutable-not-frozen** (95%)
  - Trigger: During retry decorator implementation, encountered mypy error when trying to update RetryMetrics fields. RetryMetrics was frozen=True which prevented field mutations during retry loop.
  - Action: Change RetryMetrics from @dataclass(frozen=True) to @dataclass to allow mutable updates during retry attempts. Fields like total_attempts, dlq_sent, and circuit_breaker_opened need to be updated.
- **sync-ticket-updates** (90%)
  - Trigger: After any `loom ticket` command that changes state (close, add-note, update)
  - Action: Run `loom ticket sync` to commit ticket changes to repository
- **phase3-ticket-sequencing** (90%)
  - Trigger: Starting Phase 3 ticket implementation
  - Action: Orchestration Engine (vp-cf95) must complete before CLI (vp-6cf1), Scheduling (vp-7d49), and Monitoring (vp-f17e) can fully integrate. Verify orchestration engine is in codebase before spawning worker…
- **optional-dependency-import-pattern** (90%)
  - Trigger: Need to use external library that may not be installed
  - Action: 1. Wrap import in try/except block 2. Add type: ignore[import-untyped] comment for mypy 3. Set flag variable to track availability 4. Provide fallback behavior when unavailable 5. Document degradation…
- **pandas-string-accessor-pattern** (90%)
  - Trigger: Applying string methods (trim, upper, lower, title) to pandas DataFrame columns
  - Action: Always use df[col].str.method() for column operations, never operate on individual strings. The Series.str accessor returns a StringMethods object that operates on the entire series efficiently.
- **export-new-transformation-modules** (90%)
  - Trigger: Creating new transformation modules (aggregations, cleaning, windows, etc.)
  - Action: Immediately update src/vibe_piper/transformations/__init__.py to include __all__ exports from the new module with descriptive docstring categories (e.g., # Cleaning, # Aggregations).
- **sklearn-max-samples-auto** (90%)
  - Trigger: Using sklearn.ensemble.IsolationForest
  - Action: Set max_samples='auto' instead of None (scikit-learn v1.5+ requires explicit value)
- **expectation-validationresult-return** (90%)
  - Trigger: Creating validation expectation for @validate decorator
  - Action: Ensure custom validation functions return ValidationResult(is_valid=..., errors=(...)) for compatibility with ValidationConfig.checks
- **optional-scipy-type-checking** (90%)
  - Trigger: importing scipy.stats for KS/PSI/chi-square tests
  - Action: import scipy inside function body with TYPE_CHECKING guard to prevent ImportError when scipy is not installed, keep import localized
- **two-pass-inheritance-parsing** (90%)
  - Trigger: Implementing environment inheritance
  - Action: Parse all environments first, then merge in second pass to handle forward references (child inherits from parent)
- **catch-as-exception-check-isinstance** (90%)
  - Trigger: Catching multiple parser exception types with MyPy strict mode
  - Action: Catch as Exception, then use isinstance() to distinguish specific parser errors vs generic IO errors
- **cloud-io-bucket-validation** (90%)
  - Trigger: Validating environment configuration
  - Action: When environment uses cloud IO manager (s3/gcs/azure), enforce that bucket parameter is provided
- **inst-20250129-001** (90%)
  - Trigger: validation_completed
  - Action: store_validation_result
- **retry-jitter-default-full** (90%)
  - Trigger: Implemented jitter strategies (NONE, FULL, EQUAL, DECORRELATED) in retry system. Observed that FULL jitter (random 0 to delay) provides best protection against thundering herd when multiple clients re…
  - Action: Set jitter_strategy=JitterStrategy.FULL as default in RetryConfig to prevent coordinated retry storms.
- **fastapi-middleware-stack** (90%)
  - Trigger: Creating new FastAPI application
  - Action: Add CORS middleware (allow_origins=['http://localhost:5173', 'http://localhost:3000']), GZipMiddleware(minimum_size=1000), RequestIDMiddleware (X-Request-ID header), RateLimitMiddleware (token bucket:…
- **tailwind-vite-setup** (90%)
  - Trigger: Setting up new React + Vite + Tailwind project
  - Action: Install @tailwindcss/postcss, configure postcss.config.js with '@tailwindcss/postcss' plugin, add @tailwind directives to src/index.css, configure tsconfig.json path aliases (@/* -> ./src/*), add path…
- **verify-code-before-ticket-close** (85%)
  - Trigger: Worker marks ticket ready for review/merge
  - Action: Before closing ticket, use glob to verify expected code files exist in codebase (e.g., src/vibe_piper/connectors/*.py for database connector tickets). This prevents closing tickets where implementatio…
- **datetime-none-coalescing** (85%)
  - Trigger: Performing datetime arithmetic with potentially None fields
  - Action: Always use: (field or datetime.utcnow()) to ensure arithmetic operations have datetime, not Optional[datetime]
<!-- END:compound:instincts-index -->

<!-- BEGIN:compound:rules-index -->
- _(none)_
<!-- END:compound:rules-index -->
