# INSTINCTS

This is the *fast index* of the current instinct set.
The source of truth is `.opencode/memory/instincts.json`.

<!-- BEGIN:compound:instincts-md -->
## Active instincts (top confidence)

- **nullable-schema-fields-for-tests** (95%)
  - Trigger: Creating test fixtures with DataRecord objects that have null values in specific fields
  - Action: Set nullable=True on SchemaField definitions for any field that may contain None in test data. DataRecord.__post_init__ raises ValueError if a non-nullable field contains None.
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
- **verify-code-before-ticket-close** (85%)
  - Trigger: Worker marks ticket ready for review/merge
  - Action: Before closing ticket, use glob to verify expected code files exist in codebase (e.g., src/vibe_piper/connectors/*.py for database connector tickets). This prevents closing tickets where implementatio…
- **datetime-none-coalescing** (85%)
  - Trigger: Performing datetime arithmetic with potentially None fields
  - Action: Always use: (field or datetime.utcnow()) to ensure arithmetic operations have datetime, not Optional[datetime]
- **float-to-int-outlier-replacement** (85%)
  - Trigger: Replacing outliers with mean/median (always float) into integer columns
  - Action: Either use .astype(float) on the column before replacement to allow float values, or cast mean_val to int explicitly using int(mean_val) when the target column is integer type.
- **nullable-schema-test-fixture** (85%)
  - Trigger: Testing null handling in DataRecords
  - Action: Define SchemaField with nullable=True when fixture may contain None values to avoid DataRecord validation errors
- **optional-dependency-type-ignore** (85%)
  - Trigger: Importing sklearn or numpy with try/except for optional dependency
  - Action: Include '# type: ignore[import-untyped]' on import statement to satisfy mypy strict mode while allowing graceful degradation
- **drift-history-timestamp-tracking** (85%)
  - Trigger: implementing DriftHistory class for drift monitoring
  - Action: store each drift check with timestamp, baseline_id, method, drift_score, and alert_level to enable temporal trend analysis
- **statistical-test-min-samples** (85%)
  - Trigger: calling detect_drift_ks, detect_drift_psi, or detect_drift_chi_square
  - Action: check that both baseline and new data have at least min_samples (default 50) before running test, return DriftResult with error message if insufficient
- **check-loom-permissions-early** (80%)
  - Trigger: Acting as Team Manager for Loom team
  - Action: Verify permission rules allow `loom team *` commands early. If only `loom ticket *` is allowed, document blocker immediately and proceed with ticket-only workflow.
- **loom-manager-add-ticket-notes** (80%)
  - Trigger: Created new ticket via loom ticket create
  - Action: Immediately add a note to the ticket with: 1) Tasks numbered list, 2) Dependencies (if any), 3) Acceptance criteria, 4) Technical notes, 5) Example usage (if feature)
- **merge-worker-workspace-cleanup** (80%)
  - Trigger: Attempting to retire worker after merge
  - Action: When 'loom team retire <worker>' fails with 'modified or untracked files' error, check worktree status with git status and either: 1) Stash uncommitted changes, 2) Use --force flag if safe to delete, …
- **drift-baseline-storage-json** (80%)
  - Trigger: implementing drift detection with historical comparison
  - Action: save baseline data to JSON file with metadata (timestamp, sample_size, columns, schema_name) for efficient retrieval
- **validation-result-wrapper-pattern** (80%)
  - Trigger: creating check_drift_ks and check_drift_psi functions
  - Action: map drift detection results to ValidationResult: is_valid=(alert_level != 'critical'), errors=list for critical, warnings=list for recommendations and drifted columns
- **loom-manager-dependency-notes** (75%)
  - Trigger: Starting work on a ticket that depends on others or creating a ticket that other tickets will depend on
  - Action: Add a note starting with 'DEPENDENCIES:' listing all ticket IDs this ticket depends on, with brief explanation of relationship and priority guidance (when to work on this)
- **formatter-type-separation** (75%)
  - Trigger: MyPy complains about incompatible formatter assignments
  - Action: Create distinct variables (json_formatter, colored_formatter, simple_formatter) instead of reusing single 'formatter' variable to avoid type checker confusion
- **drift-threshold-validation** (75%)
  - Trigger: creating DriftThresholds configuration class
  - Action: validate that warning < critical, all thresholds in [0,1] range, and warning < psi_critical, with clear error messages
- **datarecord-fixture-schema** (75%)
  - Trigger: writing pytest fixtures for drift detection tests
  - Action: define schema fixtures with proper SchemaFields and DataType enums, use schema parameter when creating DataRecord instances
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

## Notes

- Instincts are the *pre-skill* layer: small, repeatable heuristics.
- When an instinct proves useful across sessions, promote it into a Skill.
<!-- END:compound:instincts-md -->
