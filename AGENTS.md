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

<!-- BEGIN:compound:instincts-index -->
- **memory-store-mass-diff-low-signal** (100%)
  - Trigger: Git diffstat shows large deletions/rewrites in .opencode/memory/instincts.json and/or .opencode/memory/INSTINCTS.md without corresponding product code changes.
  - Action: Assume bookkeeping/cleanup; avoid inferring new behaviors. Prefer small updates to existing instincts/skills, or no-op if evidence is only memory-store churn.
- **snapshot-testing-implementation-check** (100%)
  - Trigger: Investigating Phase 3 testing or any testing-related ticket
  - Action: 1. Check if tests/helpers/snapshots.py exists. 2. Check if tests/helpers/test_snapshots.py exists. 3. Check if tests/snapshots/ directory exists. 4. If missing: check if snapshot-testing skill exists …
- **phase3-ticket-scoping-investigation** (95%)
  - Trigger: Investigating a Phase 3 ticket (vp-cf95, vp-6cf1, vp-7d49, vp-f17e, vp-0429) that has empty body or unclear status
  - Action: 1. Check ROADMAP.md for the ticket's Phase 3 role (Orchestration, CLI, Scheduling, Monitoring, Testing) and component description. 2. Check all related Phase 3 tickets to see overall completion status…
- **claude-opencode-skill-mirror-artifact** (91%)
  - Trigger: Git diffstat shows the same skill files changed under both .claude/skills/ and .opencode/skills/ in the same session/PR.
  - Action: Assume .claude/skills is a mirror/sync artifact; prefer proposals that target .opencode/skills only and avoid inferring new behavior from duplicated diffs.
- **ticket-and-doc-churn-low-signal** (90%)
  - Trigger: Git diffstat is dominated by .tickets/*.md and LOOM_*.md/AGENTS.md edits with no corresponding product code changes.
  - Action: Avoid inventing new product behaviors; limit proposals to workflow hygiene (skills/instinct wording tweaks, docs.sync) and keep docs block edits minimal unless a stable always-on principle changed.
- **ticket-scope-check-implementation-gap** (90%)
  - Trigger: Scoping a ticket where skill definition exists but implementation code is missing
  - Action: 1. Verify skill exists (e.g., .opencode/skills/snapshot-testing/SKILL.md). 2. Check if implementation files exist (e.g., tests/helpers/snapshots.py, tests/snapshots/). 3. If skill exists but implement…
- **ticket-investigation-workflow** (85%)
  - Trigger: Completing ticket investigation/scoping work
  - Action: 1. Document findings in ticket notes with clear status. 2. Create investigation notes file (INVESTIGATION_NOTES.md or similar). 3. Commit investigation documentation with clear message. 4. Notify mana…
- **manager-acknowledgment-confirmation** (80%)
  - Trigger: Receiving manager message acknowledging investigation or providing scheduling decision
  - Action: 1. Acknowledge manager message (loom team inbox ack <id>). 2. Update ticket with note documenting manager's decision/acknowledgment. 3. Document next steps (await scheduling, ready for implementation,…
- **inst-egg-info-deletions-are-cleanup** (74%)
  - Trigger: git diffstat shows large deletions under src/*.egg-info (PKG-INFO, SOURCES.txt, requires.txt, etc.)
  - Action: Assume this is cleanup of generated packaging artifacts, not a functional product change; avoid inferring new behavior changes from it and keep follow-up focused on untracking/ignoring egg-info rather…
<!-- END:compound:instincts-index -->

<!-- BEGIN:compound:rules-index -->
- _(none)_
<!-- END:compound:rules-index -->

## Compound (OpenCode)

These blocks are maintained by the Loom compound OpenCode plugin.
Do not edit inside the BEGIN/END fences.

<!-- BEGIN:compound:loom-core-context -->
# Loom always-on context (second-order compression)

This block is intentionally *small and stable*. Only update it when a principle has proven durable.

- First-order: observations → instincts → skills.
- Second-order: compress skills/instincts/patterns into a few fundamentals that are always-on.
- Prefer agent-native primitives: ticket, memory, workspace, team.
- Governance loop: Plan → Work → Review → Compound → Repeat.

@ LOOM_PROJECT.md
@ LOOM_ROADMAP.md
@ LOOM_CHANGELOG.md
<!-- END:compound:loom-core-context -->

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
- **inst-20260130-001** (85%)
  - Trigger: Producing a CompoundSpec v2 with skills.update entries
  - Action: Re-emit the entire final managed body for any skills.update[].body (no snippets/diffs), keep paths repo-root-relative, and include auto/sessionID plus docs.sync if indexes should refresh.
- **inst-20250129-001** (85%)
  - Trigger: validation_suite_completed
  - Action: store_validation_result
- **drift-history-timestamp-tracking** (85%)
  - Trigger: Implementing DriftHistory class for drift monitoring
  - Action: Store each drift check with timestamp, baseline_id, method, drift_score, and alert_level to enable temporal trend analysis.
- **loom-docs-merge-conflict-markers** (85%)
  - Trigger: Git diff or file contents show merge conflict markers (<<<<<<<, >>>>>>>) in LOOM_CHANGELOG.md or LOOM_ROADMAP.md (especially inside/near compound-managed fences).
  - Action: Manually resolve by removing conflict markers, preserving the compound BEGIN/END fences, merging content (often keep both sides but dedupe repeated entries), and ensuring lists remain valid markdown. …
- **inst-20260130-003** (85%)
  - Trigger: System reminder indicates Plan Mode is ACTIVE / READ-ONLY phase with a prohibition on edits or file-modifying commands.
  - Action: Do not modify files or run write-effect shell commands; restrict work to reading/inspection tools (Read/Glob/Grep/webfetch) and read-only bash commands (e.g., git status/diff/log). Defer implementatio…
- **inst-20260130-002** (83%)
  - Trigger: Git shows changes under src/*.egg-info after running uv editable installs (uv pip install -e .).
  - Action: Treat src/*.egg-info as generated noise: avoid committing unless intentionally updating packaging metadata; use the existing python-egg-info-hygiene skill to decide whether to ignore, clean, or commit…
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
- **egg-info-diffs-uv-editable-install** (78%)
  - Trigger: git diff shows changes under src/*.egg-info (PKG-INFO, SOURCES.txt, requires.txt), often after running `uv pip install -e .` or similar editable install steps
  - Action: Assume these are generated artifacts; avoid committing them by default. If they are untracked, add src/*.egg-info/ to .gitignore. If they are tracked, only commit them when the project explicitly want…
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
<!-- END:compound:instincts-index -->

<!-- BEGIN:compound:rules-index -->
- _(none)_
<!-- END:compound:rules-index -->
