# Testing Patterns for Vibe Piper

This guide describes testing patterns and best practices for Vibe Piper.

## Table of Contents

1. [Overview](#overview)
2. [Test Structure](#test-structure)
3. [Using Fixtures](#using-fixtures)
4. [Assertion Helpers](#assertion-helpers)
5. [Fake Data Generation](#fake-data-generation)
6. [Integration Testing](#integration-testing)
7. [Testing Patterns](#testing-patterns)
8. [Best Practices](#best-practices)

## Overview

Vibe Piper provides comprehensive testing infrastructure including:

- **Pytest fixtures** for common objects (Asset, Pipeline, Context, Schema, etc.)
- **Mock IO managers** for testing asset I/O operations
- **Fake data generators** for creating test data
- **Assertion helpers** for validation (`assert_valid_asset`, `assert_lineage`, etc.)
- **Integration test framework** with database setup

## Test Structure

```
tests/
├── conftest.py                 # Pytest configuration and fixtures
├── helpers/                    # Helper modules
│   ├── assertions.py          # Custom assertion helpers
│   ├── comparators.py         # Object comparison helpers
│   └── factories.py           # Factory functions for test objects
├── fixtures/                   # Test data and generators
│   ├── fake_data.py           # Fake data generators
│   ├── sample.csv             # Sample CSV file
│   ├── sample.json            # Sample JSON file
│   └── sample.parquet         # Sample Parquet file
├── integration/                # Integration test framework
│   └── __init__.py            # Integration test fixtures
└── test_*.py                  # Test files
```

## Using Fixtures

### Available Fixtures

The `conftest.py` file provides 40+ fixtures for common test scenarios:

#### Schema Fixtures
```python
def test_something(basic_schema):
    """Use a basic schema with common field types."""
    assert basic_schema.name == "basic_schema"

def test_user_schema(user_schema):
    """Use a pre-defined user schema."""
    assert user_schema.name == "user_schema"
```

#### Context Fixtures
```python
def test_with_context(pipeline_context):
    """Test with a basic pipeline context."""
    assert pipeline_context.pipeline_id == "test_pipeline"

def test_production(production_context):
    """Test with production-like context."""
    assert production_context.config["env"] == "production"
```

#### Asset Fixtures
```python
def test_asset(memory_asset):
    """Test with a simple in-memory asset."""
    assert memory_asset.asset_type == AssetType.MEMORY

def test_table_asset(table_asset):
    """Test with a database table asset."""
    assert "postgresql://" in table_asset.uri
```

#### Pipeline Fixtures
```python
def test_pipeline(simple_pipeline):
    """Test with a simple source -> transform pipeline."""
    result = simple_pipeline.execute(test_data)
    assert result is not None
```

### Creating Custom Fixtures

You can create your own fixtures in `tests/conftest.py`:

```python
import pytest
from vibe_piper.types import Asset, AssetType

@pytest.fixture
def custom_asset():
    """Custom asset for your tests."""
    return Asset(
        name="my_asset",
        asset_type=AssetType.MEMORY,
        uri="memory://my_asset",
    )
```

## Assertion Helpers

The `tests/helpers/assertions.py` module provides custom assertion helpers:

### Schema Assertions

```python
from tests.helpers import assert_schema_valid, assert_data_conforms_to_schema

def test_schema_validation(basic_schema):
    assert_schema_valid(basic_schema)

def test_data_validation(basic_schema):
    data = {"id": 1, "name": "Alice", "email": "alice@example.com"}
    assert_data_conforms_to_schema(data, basic_schema)
```

### Asset Assertions

```python
from tests.helpers import assert_asset_valid, assert_asset_graph_valid

def test_asset(memory_asset):
    assert_asset_valid(memory_asset)

def test_graph(simple_asset_graph):
    assert_asset_graph_valid(simple_asset_graph)
```

### Lineage and Dependency Assertions

```python
from tests.helpers import assert_lineage, assert_no_circular_dependencies

def test_lineage(complex_asset_graph):
    assert_no_circular_dependencies(complex_asset_graph)
    assert_lineage(complex_asset_graph, "final1", ["source1", "source2", "intermediate"])
```

### Execution Assertions

```python
from tests.helpers import assert_execution_successful

def test_execution(multi_stage_pipeline):
    result = multi_stage_pipeline.execute(test_data, context)
    assert_execution_successful(result)
```

## Fake Data Generation

The `tests/fixtures/fake_data.py` module provides fake data generators:

### Using the Generator Class

```python
from tests.fixtures.fake_data import FakeDataGenerator

def test_with_fake_data():
    generator = FakeDataGenerator(seed=42)  # Seed for reproducibility

    # Generate data for a schema
    data = generator.generate_for_schema(my_schema, count=10)

    # Generate individual field values
    string_val = generator.generate_string({"min_length": 5, "max_length": 20})
    int_val = generator.generate_integer({"min_value": 0, "max_value": 100})
```

### Using Convenience Functions

```python
from tests.fixtures.fake_data import fake_user_data, fake_product_data

def test_with_users():
    users = fake_user_data(count=5, seed=42)
    assert len(users) == 5
    assert "user_id" in users[0]

def test_with_products():
    products = fake_product_data(count=10)
    assert all("price" in p for p in products)
```

## Integration Testing

The `tests/integration/` module provides fixtures for integration testing:

### Database Testing

```python
import pytest
from tests.integration import populated_test_db

def test_database_operations(populated_test_db):
    """Test with a real database populated with test data."""
    conn = populated_test_db
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM test_users")
    count = cursor.fetchone()[0]
    assert count == 3
```

### File System Testing

```python
import pytest
from tests.integration import test_csv_file, test_json_file

def test_csv_reading(test_csv_file):
    """Test reading CSV files."""
    import csv
    with open(test_csv_file) as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert len(rows) == 3

def test_json_operations(test_json_file):
    """Test JSON file operations."""
    import json
    with open(test_json_file) as f:
        data = json.load(f)
        assert len(data) == 3
```

### API Testing

```python
import pytest
from tests.integration import mock_api_server

def test_api_asset(mock_api_server):
    """Test API asset with mocked server."""
    # Make API calls using the mocked server
    response = requests.get("https://api.example.com/data")
    assert response.status_code == 200
```

## Testing Patterns

### Pattern 1: Testing Pipeline Execution

```python
def test_pipeline_execution(simple_pipeline, pipeline_context):
    """Test basic pipeline execution."""
    input_data = {"id": 1, "name": "test"}

    result = simple_pipeline.execute(input_data, context=pipeline_context)

    assert result is not None
    # Add specific assertions based on your pipeline
```

### Pattern 2: Testing Schema Validation

```python
def test_schema_validation_success(user_schema):
    """Test valid data conforms to schema."""
    from tests.helpers import assert_data_conforms_to_schema

    valid_data = {
        "user_id": 123,
        "username": "testuser",
        "email": "test@example.com",
        "created_at": "2024-01-01T00:00:00",
    }

    assert_data_conforms_to_schema(valid_data, user_schema)

def test_schema_validation_failure(user_schema):
    """Test invalid data is rejected."""
    import pytest
    from vibe_piper.types import DataRecord

    invalid_data = {
        "user_id": 123,
        # Missing required 'username' field
        "email": "test@example.com",
    }

    with pytest.raises(ValueError, match="Required field"):
        DataRecord(data=invalid_data, schema=user_schema)
```

### Pattern 3: Testing Asset Graphs

```python
def test_asset_graph_dependencies(complex_asset_graph):
    """Test asset graph dependency resolution."""
    from tests.helpers import (
        assert_no_circular_dependencies,
        assert_topological_order,
    )

    # Check for circular dependencies
    assert_no_circular_dependencies(complex_asset_graph)

    # Get and validate topological order
    order = complex_asset_graph.topological_order()
    assert_topological_order(complex_asset_graph, order)
```

### Pattern 4: Testing with Fake Data

```python
def test_pipeline_with_fake_data(multi_stage_pipeline, user_schema):
    """Test pipeline processing fake user data."""
    from tests.fixtures.fake_data import fake_user_data

    # Generate fake data
    users = fake_user_data(count=100, seed=42)

    # Process through pipeline
    result = multi_stage_pipeline.execute(users)

    # Validate results
    assert len(result) > 0
    # Add specific assertions
```

### Pattern 5: Testing Error Handling

```python
def test_error_handling():
    """Test error handling in pipelines."""
    from vibe_piper.types import Pipeline, Operator, OperatorType, PipelineContext

    def failing_transform(data, context):
        raise ValueError("Intentional error")

    pipeline = Pipeline(
        name="failing_pipeline",
        operators=(Operator(
            name="failing_op",
            operator_type=OperatorType.TRANSFORM,
            fn=failing_transform,
        ),)
    )

    with pytest.raises(ValueError, match="Intentional error"):
        pipeline.execute({"test": "data"})
```

### Pattern 6: Parameterized Testing

```python
@pytest.mark.parametrize("asset_type,expected_uri_prefix", [
    (AssetType.MEMORY, "memory://"),
    (AssetType.FILE, "file://"),
    (AssetType.API, "https://"),
])
def test_asset_uri_prefixes(asset_type, expected_uri_prefix):
    """Test that different asset types have correct URI prefixes."""
    from tests.helpers.factories import make_asset

    asset = make_asset(asset_type=asset_type)
    assert asset.uri.startswith(expected_uri_prefix)
```

### Pattern 7: Testing with Mock IO

```python
def test_with_mock_io_manager(mock_io_manager):
    """Test asset I/O with mocked storage."""
    # Write data
    test_data = {"id": 1, "name": "test"}
    mock_io_manager.write("memory://test", test_data)

    # Read data back
    retrieved = mock_io_manager.read("memory://test")
    assert retrieved == test_data

    # Check writes were tracked
    assert "memory://test" in mock_io_manager.writes
```

## Best Practices

### 1. Use Descriptive Test Names

```python
# Good
def test_pipeline_executes_in_topological_order():
    """Test that assets execute in dependency order."""

# Bad
def test_pipeline():
    """Test pipeline."""
```

### 2. Follow Arrange-Act-Assert Pattern

```python
def test_user_transformation():
    # Arrange
    users = fake_user_data(count=5)
    pipeline = make_user_transform_pipeline()

    # Act
    result = pipeline.execute(users)

    # Assert
    assert len(result) == 5
    assert all("transformed_at" in u for u in result)
```

### 3. Use Fixtures for Shared Setup

```python
# Instead of setting up in each test
def test_1():
    schema = Schema(...)  # Repetitive

def test_2():
    schema = Schema(...)  # Repetitive

# Use fixtures instead
@pytest.fixture
def my_schema():
    return Schema(...)

def test_1(my_schema):
    pass  # Use fixture

def test_2(my_schema):
    pass  # Use fixture
```

### 4. Test Edge Cases

```python
def test_pipeline_with_empty_data():
    """Test pipeline behavior with empty input."""
    result = pipeline.execute([])
    assert result == []

def test_pipeline_with_null_values():
    """Test pipeline handles null values correctly."""
    data = {"id": 1, "name": None}
    result = pipeline.execute(data)
    assert result is not None
```

### 5. Use Seeds for Reproducible Random Data

```python
def test_with_reproducible_data():
    """Use a seed for reproducible fake data."""
    # Always generates the same data
    data1 = fake_user_data(count=5, seed=42)
    data2 = fake_user_data(count=5, seed=42)
    assert data1 == data2
```

### 6. Clean Up in Tests

```python
def test_with_temporary_resources(test_data_dir):
    """test_data_dir fixture automatically cleans up."""
    temp_file = test_data_dir / "temp.txt"
    temp_file.write_text("test data")
    # No need to delete - fixture handles cleanup
```

### 7. Use Type Hints in Tests

```python
def test_schema_fields(schema: Schema) -> None:
    """Use type hints for better test documentation."""
    fields: tuple[SchemaField, ...] = schema.fields
    assert len(fields) > 0
```

### 8. Group Related Tests

```python
class TestPipelineExecution:
    """Group related pipeline execution tests."""

    def test_simple_execution(self, simple_pipeline):
        pass

    def test_multi_stage_execution(self, multi_stage_pipeline):
        pass

    def test_error_handling(self, error_pipeline):
        pass
```

## Running Tests

### Run All Tests
```bash
pytest
```

### Run Specific Test File
```bash
pytest tests/test_pipeline.py
```

### Run Specific Test
```bash
pytest tests/test_pipeline.py::test_pipeline_execution
```

### Run with Coverage
```bash
pytest --cov=vibe_piper --cov-report=html
```

### Run Integration Tests Only
```bash
pytest -m integration
```

### Run with Verbose Output
```bash
pytest -v
```

## Resources

- [Pytest Documentation](https://docs.pytest.org/)
- [Vibe Piper Core Documentation](../../README.md)
- [Testing Best Practices](https://docs.pytest.org/en/stable/best-practices.html)
