# INSTINCTS

This is the *fast index* of the current instinct set.
The source of truth is `.opencode/memory/instincts.json`.

<!-- BEGIN:compound:instincts-md -->
## Active instincts (top confidence)

- **inst-20260130-003** (100%)
  - Trigger: System reminder indicates Plan Mode is ACTIVE / READ-ONLY phase with a prohibition on edits or file-modifying commands.
  - Action: Do not modify files or run write-effect shell commands; restrict work to reading/inspection tools (Read/Glob/Grep/webfetch) and read-only bash commands (e.g., git status/diff/log). Defer implementatio…
- **pandas-string-accessor-error-pattern** (98%)
  - Trigger: Test failures showing text not cleaned (whitespace not trimmed, case not changed) when using pandas DataFrame operations
  - Action: When applying string operations to DataFrame columns in pandas 2.x, always use Series.str accessor. Pattern is: df[col].str.trim() or df[col].str.lower(). Never apply string methods directly to column…
- **inst-autolearn-json-only** (96%)
  - Trigger: Responding to a background autolearn prompt that explicitly requires valid JSON-only output (no code fences, no commentary).
  - Action: Return exactly one JSON object matching the specified schema; include required fields (schema_version, auto.reason, auto.sessionID), and avoid any extra text.
- **nullable-schema-fields-for-data-with-nones** (92%)
  - Trigger: Creating test fixtures with DataRecord objects that will have None/null values in specific fields
  - Action: When creating SchemaField for any field that might contain None in test data, set nullable=True. DataRecord.__post_init__ raises ValueError if a non-nullable field contains None.
- **inst-autolearn-repo-relative-paths** (90%)
  - Trigger: An autolearn/background CompoundSpec prompt includes a Path rule requiring repo-root-relative file references
  - Action: In any markdown emitted inside skills/docs/changelog, reference files using repo-root-relative paths (no absolute paths, no file:// URIs).
- **inst-20250129-003** (90%)
  - Trigger: validation_suite_completed
  - Action: store_validation_result
- **inst-20260130-002** (88%)
  - Trigger: Git shows changes under src/*.egg-info after running uv editable installs (uv pip install -e .).
  - Action: Treat src/*.egg-info as generated noise: avoid committing unless intentionally updating packaging metadata; use the existing python-egg-info-hygiene skill to decide whether to ignore, clean, or commit…
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
  - Trigger: Git diff or file contents show merge conflict markers (<<<<<<<, =======, >>>>>>>) in LOOM_CHANGELOG.md or LOOM_ROADMAP.md (especially inside/near compound-managed fences).
  - Action: Manually resolve by removing conflict markers, preserving the compound BEGIN/END fences, merging content (often keep both sides but dedupe repeated entries), and ensuring lists remain valid markdown. …
- **optional-type-checking-guard** (82%)
  - Trigger: Importing types only for type checking (Schema, DataType) used only in annotations
  - Action: Import optional types inside TYPE_CHECKING block, import at runtime in else block. This allows module to work without the optional dependency.
- **egg-info-diffs-uv-editable-install** (82%)
  - Trigger: git diff shows changes under src/*.egg-info (PKG-INFO, SOURCES.txt, requires.txt), often after running `uv pip install -e .` or similar editable install steps
  - Action: Assume these are generated artifacts; avoid committing them by default. If they are untracked, add src/*.egg-info/ to .gitignore. If they are tracked, only commit them when the project explicitly want…
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
- **baseline-json-storage-pattern** (75%)
  - Trigger: Implementing historical data storage with timestamp, sample_size, columns for later retrieval
  - Action: Save data to JSON files with metadata dict (created_at, sample_size, columns, schema_name). Store data list efficiently. Create methods for add, get, get_metadata, list, delete.
- **drift-threshold-validation** (75%)
  - Trigger: Creating DriftThresholds configuration class
  - Action: Validate that warning < critical, all thresholds in [0,1] range, and warning < psi_critical, with clear error messages.
- **jsonl-append-only-time-series** (72%)
  - Trigger: Storing time-series drift history that only grows
  - Action: Write each entry as a line to JSONL file (json.dumps(entry) + newline). Efficient append-only, easy to read latest N entries with tail.
- **inst-workspace-add-remove-only-via-loom** (72%)
  - Trigger: About to add/remove a service repo, or you notice workspace.json needs changes (new repo, remote, default branch).
  - Action: Use `loom workspace add/remove` (not manual edits). Then run `loom workspace sync --clone` and `loom workspace services refresh-index` so clones and services/index.json stay consistent and derived.
- **inst-compound-autolearn-json-only-v2** (72%)
  - Trigger: Prompt explicitly requests a background autolearn output and specifies 'Output only valid JSON' with 'CompoundSpec v2' schema.
  - Action: Return a single JSON object matching schema_version=2 (no commentary, no code fences). Prefer updating existing skills/docs over creating duplicates; avoid proposing product code changes.
- **validation-result-wrapper-mapping** (70%)
  - Trigger: Creating wrapper functions that convert domain types to validation framework ValidationResult
  - Action: Create wrapper function that takes domain-specific result (DriftResult) and returns ValidationResult (is_valid=alert_level != 'critical', errors=list for critical, warnings=list for recommendations). …
- **avoid-committing-egg-info** (70%)
  - Trigger: Git diff shows changes under `src/*.egg-info/` (e.g., `PKG-INFO`, `requires.txt`) without intentional packaging/version work
  - Action: Assume the changes are generated; do not include them in commits/PRs and instead regenerate/clean the working tree via the repo's normal install workflow before re-checking diffs
- **egg-info-churn-is-build-noise** (70%)
  - Trigger: git diff shows only src/vibe_piper.egg-info/* changes (PKG-INFO, SOURCES.txt, requires.txt) with no src/ or tests/ edits
  - Action: Assume these are generated artifacts; avoid proposing learnings from them and avoid including them in commits unless the repo explicitly tracks egg-info as source of truth.
- **compound-state-json-noop** (70%)
  - Trigger: Git summary shows only .opencode/compound/state.json changed
  - Action: Treat as internal bookkeeping; propose no memory/product changes and avoid suggesting commits or follow-up work based on it.
- **inst-services-index-json-change-audit** (67%)
  - Trigger: `services/index.json` changes (especially without `services/*.md` changes)
  - Action: Re-run `loom workspace services refresh-index`, then verify whether any `services/*.md` should change; commit only a derived index that corresponds to intentional `services/*.md` updates (never hand-e…
- **immutable-result-dataclass-frozen** (65%)
  - Trigger: Creating result types that should never be modified after creation
  - Action: Mark result dataclasses with frozen=True to ensure immutability and enable hashability.

## Notes

- Instincts are the *pre-skill* layer: small, repeatable heuristics.
- When an instinct proves useful across sessions, promote it into a Skill.
<!-- END:compound:instincts-md -->
