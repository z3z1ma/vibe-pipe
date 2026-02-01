# Simple Pipeline Example

A minimal, fast example that demonstrates the simple Pipeline model and operator composition using in-memory data only.

## Overview

This example shows how to build and execute a simple data pipeline with Vibe Piper:

- **Clean**: Normalize whitespace and case
- **Transform**: Convert text to structured records
- **Filter**: Keep only records matching criteria
- **Summarize**: Compute aggregate statistics

## Pipeline Flow

```
┌─────────────────┐
│  Raw Text      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Clean Text     │ (strip whitespace, lowercase)
└────────┬────────┘
         │
         ▼
┌───────────────────────────────────┐
│  Structured Records             │ (add metadata)
│  - id, text, word_count       │
│  - first_char, last_char       │
│  - is_palindrome              │
└────────┬──────────────────────┘
         │
         ▼
┌─────────────────┐
│  Filter        │ (word_count >= 2)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Summary       │ (statistics)
│  - total      │
│  - avg_words  │
│  - palindromes│
└─────────────────┘
```

## Quick Start

### Run the Pipeline

```bash
# Run from the examples/pipeline_simple directory
cd examples/pipeline_simple

# Run with uv (recommended)
uv run python pipeline.py

# Or run directly (if vibe_piper is installed)
python pipeline.py
```

### Expected Output

```
======================================================================
Simple Pipeline Example
======================================================================

Building pipeline...
✓ Built pipeline with 5 assets

Executing pipeline...

  Executing: raw_text...
  Executing: clean_text...
  Executing: structured_records...
  Executing: filtered_records...
  Executing: summary...

======================================================================
Results
======================================================================

Step 1: Raw Text
----------------------------------------------------------------------
  '  hello world  '
  '  VIBE PIPER  '
  '  data pipeline  '
  '  simple example  '
  '  minimal code  '

Step 2: Clean Text
----------------------------------------------------------------------
  'hello world'
  'vibe piper'
  'data pipeline'
  'simple example'
  'minimal code'

Step 3: Structured Records
----------------------------------------------------------------------
  {'id': 1, 'text': 'hello world', 'word_count': 2, ...}
  {'id': 2, 'text': 'vibe piper', 'word_count': 2, ...}
  {'id': 3, 'text': 'data pipeline', 'word_count': 2, ...}
  {'id': 4, 'text': 'simple example', 'word_count': 2, ...}
  {'id': 5, 'text': 'minimal code', 'word_count': 2, ...}

Step 4: Filtered Records (2+ words)
----------------------------------------------------------------------
  {'id': 1, 'text': 'hello world', 'word_count': 2, ...}
  {'id': 2, 'text': 'vibe piper', 'word_count': 2, ...}
  {'id': 3, 'text': 'data pipeline', 'word_count': 2, ...}
  {'id': 4, 'text': 'simple example', 'word_count': 2, ...}
  {'id': 5, 'text': 'minimal code', 'word_count': 2, ...}

Step 5: Summary
----------------------------------------------------------------------
  Total records: 5
  Total words: 10
  Average words per record: 2.00
  Palindrome count: 0
  Unique first characters: 5

======================================================================
✓ Pipeline executed successfully!
======================================================================
```

## Key Concepts

### Pipeline Builder

```python
builder = PipelineBuilder(
    name="simple_text_pipeline",
    description="Simple text processing pipeline",
)
```

### Adding Assets

Assets are added with functions that process data. Dependencies are inferred from parameter names:

```python
# Source asset (no dependencies)
builder.asset(
    name="raw_text",
    fn=lambda: ["hello world", "vibe piper"],
    description="Raw input text",
)

# Transform asset (depends on "raw_text")
builder.asset(
    name="clean_text",
    fn=lambda raw_text: [t.strip().lower() for t in raw_text],
    depends_on=["raw_text"],
    description="Normalize whitespace and case",
)
```

### Pipeline Context

The execution context provides runtime information:

```python
context = PipelineContext(
    pipeline_id="simple_text_pipeline",
    run_id="demo-run-001",
    config={"log_level": "INFO"},
)
```

### Default Executor

The executor runs the pipeline assets in dependency order:

```python
executor = DefaultExecutor()
for asset in graph.assets:
    result = executor.execute(asset, context, upstream_data=results)
    results[asset.name] = result.data
```

## Testing

### Run Tests

```bash
# Run tests from the examples/pipeline_simple directory
cd examples/pipeline_simple

# Run with uv (recommended)
uv run pytest tests/ -q

# Or run directly
pytest tests/ -q
```

### Test Coverage

- ✅ Pipeline builds without errors
- ✅ All assets execute successfully
- ✅ Output matches expected deterministic results
- ✅ Filtering works correctly
- ✅ Summary statistics are accurate

## Customization

### Modify Input Data

Change the `raw_text` asset to process different data:

```python
builder.asset(
    name="raw_text",
    fn=lambda: [
        "  custom input 1  ",
        "  custom input 2  ",
    ],
    description="Custom raw input text",
)
```

### Change Filter Criteria

Modify the `filter_records` asset to use different criteria:

```python
def filter_records(upstream_data, context):  # noqa: ARG001
    """Filter to keep only records starting with specific character."""
    records = upstream_data["structured_records"]
    return [r for r in records if r["first_char"] == "d"]
```

### Add New Transformations

Add additional processing steps:

```python
def compute_word_length(upstream_data, context):  # noqa: ARG001
    """Compute total character length."""
    records = upstream_data["filtered_records"]
    for r in records:
        r["char_length"] = len(r["text"])
    return records

builder.asset(
    name="length_enriched",
    fn=compute_word_length,
    depends_on=["filtered_records"],
    description="Add character length metadata",
)
```

## Design Principles

### Minimal Code

This example demonstrates:
- **No external dependencies**: Uses only core Vibe Piper APIs
- **No file I/O**: All data is in-memory
- **No databases**: No connectors or external services
- **No validation suites**: Focus on pipeline structure

### Deterministic Output

- **Fixed input data**: Results are predictable
- **No time-based fields**: Tests are stable
- **Pure functions**: No side effects

## Architecture

### Asset Types

1. **Source**: `raw_text` - Provides initial data
2. **Transform**: `clean_text`, `structured_records` - Transform data shape
3. **Filter**: `filtered_records` - Reduce dataset
4. **Aggregate**: `summary` - Compute statistics

### Dependency Management

Dependencies are declared explicitly and validated:
- Cycle detection prevents circular dependencies
- Topological sort ensures correct execution order

## Best Practices

### 1. Use Descriptive Names

```python
builder.asset(
    name="clean_text",  # Clear, descriptive name
    ...
)
```

### 2. Add Descriptions

```python
builder.asset(
    ...
    description="Normalize whitespace and case",  # Documents purpose
)
```

### 3. Keep Functions Pure

```python
def clean_text(upstream_data, context):  # noqa: ARG001
    """No side effects, same input → same output."""
    raw_texts = upstream_data["raw_text"]
    return [text.strip().lower() for text in raw_texts]
```

### 4. Type Hints

```python
def transform_to_dicts(upstream_data, context):  # noqa: ARG001
    """Transform clean text to structured records with metadata."""
    clean_texts = upstream_data["clean_text"]
    records = []
    # ...
    return records  # Returns list[dict]
```

## Related Examples

- [ETL Pipeline](../etl_pipeline/) - Full-featured ETL with PostgreSQL
- [API Integration](../api_ingestion/) - REST API data ingestion
- [Transformation Library](../transformation_example.py) - Using transformation operators

## License

This example is part of Vibe Piper and follows the same license.

---

**Built with ❤️ using Vibe Piper**
