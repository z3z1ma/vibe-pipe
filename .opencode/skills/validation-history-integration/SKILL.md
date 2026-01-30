---
name: validation-history-integration
description: Integrate validation history auto-storage with existing validation framework
license: MIT
compatibility: opencode,claude
metadata:
  created_at: "2026-01-29T22:58:59.652Z"
  updated_at: "2026-01-29T22:58:59.652Z"
  version: "1"
  tags: "validation,history,integration"
  compatibility: "2.1.0+"
  difficulty: "intermediate"
---
<!-- BEGIN:compound:skill-managed -->
# Validation History Integration

This skill provides guidance on automatically storing validation results in history.

## When To Use

- Run validation suites with auto-storage enabled
- Ticket requires validation history implementation
- Use `store_validation_result()` integration utility

## Procedure

1. Import from vibe_piper.validation.integration: `store_validation_result`
2. Create validation history store (PostgreSQLValidationHistoryStore)
3. Run validation suite on data
4. Call `store_validation_result()` with suite result and asset name

5. The history store will automatically:
   - Convert suite result to ValidationRunMetadata
   - Convert suite check results to ValidationCheckRecord
   - Extract and store quality metrics (pass_rate, duration, total_records)
   - Return validation_run_id for tracking

## Example

```python
from vibe_piper.validation import ValidationSuite, expect_column_values_to_be_unique, expect_column_values_to_not_be_null
from vibe_piper.validation.integration import store_validation_result
from vibe_piper.connectors.postgres import PostgreSQLConnector, PostgreSQLConfig


# Create suite and checks
suite = ValidationSuite(name='data_quality')
suite.add_check('unique_ids', expect_column_values_to_be_unique('user_id'))
suite.add_check('no_null_emails', expect_column_values_to_not_be_null('email'))

# Create history store
config = PostgreSQLConfig(
    host='localhost',
    port=5432,
    database='vibe_piper',
    user='user',
    password='password'
)

connector = PostgreSQLConnector(config)
connector.connect()

store = PostgreSQLValidationHistoryStore(connector)
store.initialize_schema()

# Run validation and auto-store
result = suite.validate(data)
validation_run_id = store_validation_result(
    result,
    asset_name='users',
    history_store=store,
    pipeline_id='daily_pipeline'
)

print(f'Validation run ID: {validation_run_id}')
```

## Integration Points

- Works with existing ValidationSuite API
- Works with all validation check functions
- Integrates with asset metadata (pipeline_id, suite_name)
- Stores comprehensive metrics for trend analysis

## Notes

- Requires PostgreSQL database initialized with validation history schema
- Call `store.initialize_schema()` before first use
- Metrics are automatically extracted: pass_rate, duration_ms, total_records
- Validation run ID can be used for tracking and querying history

- See src/vibe_piper/validation/history.py for full API
<!-- END:compound:skill-managed -->

## Manual notes

_This section is preserved when the skill is updated. Put human notes, caveats, and exceptions here._
