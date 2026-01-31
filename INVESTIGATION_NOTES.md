# Phase 3 Testing: Investigation and Scope Clarification

**Date:** 2026-01-31
**Ticket:** vp-0429 - Phase 3 testing: clarify scope
**Status:** Completed investigation, scope defined

## Investigation Summary

### Phase 3 Tickets Status
| Ticket | Component | Status | Notes |
|--------|-----------|--------|-------|
| vp-cf95 | Orchestration Engine | CLOSED | Body empty, status unclear |
| vp-6cf1 | CLI Pipeline Commands | CLOSED ✓ | Fully implemented (7 commands) |
| vp-7d49 | Scheduling System | CLOSED ✓ | 19/25 tests passing |
| vp-f17e | Monitoring & Observability | CLOSED ✓ | 76 tests created |
| **vp-0429** | **Testing Framework** | **OPEN** | **Snapshot testing missing** |

### Testing Infrastructure Status

**✅ Present:**
- Unit tests: Comprehensive (100+ test files)
- Integration tests: Full suite with `@pytest.mark.integration` markers
- Fixtures: Extensive in `tests/conftest.py` (basic_schema, user_schema, pipeline_context, memory_asset, simple_pipeline, etc.)
- Assertion helpers: Complete in `tests/helpers/assertions.py` (assert_schema_valid, assert_asset_valid, assert_data_conforms_to_schema, assert_lineage, etc.)
- Factory functions: Available in `tests/helpers/factories.py` (make_schema, make_asset, make_pipeline, make_data_record)
- Fake data generators: Working in `tests/fixtures/fake_data.py` (FakeDataGenerator, fake_user_data)

**❌ Missing:**
- Snapshot testing framework

### Snapshot Testing Status

**Skill Definition:**
- File: `.opencode/skills/snapshot-testing/SKILL.md`
- API defined:
  - `assert_match_snapshot(data, snapshot_name, update=False, max_depth=10)`
  - `assert_json_snapshot(data, snapshot_name, update=False)`
  - `assert_snapshot_matches_data(actual, snapshot_path, update=False)`

**Implementation Status:**
- ❌ No `tests/helpers/snapshots.py`
- ❌ No `tests/helpers/test_snapshots.py`
- ❌ No `tests/snapshots/` directory

**Dependencies:**
- Only built-in modules needed: `json`, `difflib`, `pathlib`
- Test framework: `pytest`

## Roadmap Context

From `ROADMAP.md` line 170:
```
⏳ Snapshot testing (vp-0429 in progress)
```

This is the **only item** in the Testing Layer marked as "in progress" or incomplete. All other testing infrastructure components are marked with ✅.

## Decision: IMPLEMENT

**Ticket vp-0429 SHOULD EXIST** with clear scope for implementing snapshot testing.

### Reasons to Implement
1. **Roadmap explicitly requires it** - Only incomplete Phase 3 Testing Layer component
2. **Low effort, high value** - Estimated 2-3 hours
3. **Catches regressions** - Prevents accidental changes to pipeline outputs
4. **Well-defined API** - Snapshot testing skill already specifies the interface
5. **Independent** - No dependencies on other tickets or current sprint work

### Reasons Against Defer/Close
- ❌ Current sprint is "Config pipelines verification" - but snapshot testing is independent
- ❌ P2 priority - but it's the only missing Phase 3 component
- ❌ Nice-to-have - but it's called out in the roadmap as required

## Defined Scope

See ticket vp-0429 for the full scope and acceptance criteria.

### Implementation Overview
1. Create `tests/helpers/snapshots.py` with assertion functions
2. Create `tests/helpers/test_snapshots.py` with test suite
3. Add `--update-snapshots` pytest flag support
4. Create `tests/snapshots/` directory structure
5. Update `AGENTS.md` with snapshot testing documentation
6. Add example snapshot tests

### Key Features
- JSON-based snapshot storage
- Automatic snapshot creation on first run
- Diff visualization on mismatches
- Max depth protection (default 10)
- Sorted keys for reproducibility
- Support for update flag

## Related Tickets

**Phase 3:**
- vp-8690: Config-driven pipelines verification (current sprint, independent)
- vp-8d93, vp-a340, vp-973a: Subtickets of vp-8690

**Phase 4 (future):**
- vp-q01: Advanced Validation Framework
- vp-q02: Data Quality Dashboard
- vp-q03: Integration with External Quality Tools

## Next Steps

1. Manager to review the defined scope in vp-0429
2. If approved, spawn worker for implementation
3. Estimated effort: 2-3 hours
4. Can proceed independently of current sprint work
