# Great Expectations Research Memo

**Date:** 2026-01-27
**Framework:** Great Expectations (GX)
**Focus:** Data Quality Testing & Documentation

## Executive Summary

Great Expectations (GX) is the leading open-source data quality framework. It pioneered "expectations" (assertions about data) as first-class objects, with automatic documentation, profiling, and validation. However, it's complex and heavy; recent GX 2.0/3.0 releases aim to modernize but add migration complexity.

## Core Architecture

### Design Philosophy
- **Expectations as code**: Data quality assertions are version-controlled code
- **Auto-documentation**: Expectations generate human-readable docs
- **Profiling**: Automatically infer expectations from data
- **Data Docs**: Auto-generated documentation sites

### Key Components

1. **Expectations**
   ```python
   import great_expectations as gx

   df.expect_column_values_to_not_be_null('email')
   df.expect_column_values_to_match_regex('email', r'^[\w\.-]+@')
   df.expect_column_values_to_be_between('age', min_value=0, max_value=120)
   df.expect_table_row_count_to_be_between(min_value=100, max_value=10000)
   ```
   - 50+ built-in expectation types
   - Return ValidationResult objects
   - Can save as ExpectationSuite
   - Rich metadata (statistics, examples)

2. **Data Context**
   ```python
   context = gx.get_context()
   suite = context.get_expectation_suite('customers')
   validation = context.validate(df, expectation_suite=suite)
   ```
   - Manages expectations, validations, data docs
   - Stores configuration (YAML)
   - Connects to data stores
   - Can be complex to set up

3. **Data Docs**
   - Auto-generated HTML documentation
   - Shows expectations per table
   - Validation history
   - Sample data
   - Similar to dbt docs

4. **Profiling**
   ```python
   suite = df.profile()
   # Automatically infers expectations:
   # - column types
   # - value ranges
   # - null percentages
   # - unique value counts
   ```
   - One-click expectation generation
   - Good starting point
   - Needs human review

5. **Validation Results**
   ```python
   {
       'success': True,
       'statistics': {
           'evaluated': 10000,
           'success_percent': 100.0
       },
       'results': [
           {
               'expectation_config': {...},
               'success': True,
               'element_count': 10000
           }
       ]
   }
   ```
   - Detailed results
   - Per-experimentation stats
   - Partial success handling
   - Useful for monitoring

## Notable Patterns for VibePiper

### Strengths to Emulate

1. **Expectation Library** ⭐
   - Rich set of built-in validations
   - Clear naming conventions
   - Consistent API
   - Lesson: VibePiper needs similar validation library

2. **Auto-Documentation**
   - Expectations generate human-readable docs
   - "Data Docs" sites
   - Useful for business stakeholders
   - Lesson: Generate docs from validations

3. **Profiling**
   - Auto-generate expectations from data
   - Great for getting started
   - Reduces boilerplate
   - Lesson: Add profiling to VibePiper

4. **Rich Metadata**
   - Validation results include stats
   - Example values
   - Percentiles
   - Lesson: Collect metadata during validation

5. **Validation Stores**
   - Save validation results
   - Historical tracking
   - Trends over time
   - Lesson: Track validation history

### Anti-Patterns to Avoid

1. **Complex Setup** ⚠️
   - Requires data context configuration
   - YAML files, directory structure
   - Stores, backends, connectors
   - Steep learning curve
   - Lesson: Keep VibePiper simple to start

2. **Heavy Abstraction**
   - Multiple layers (DataContext, Suite, Validation)
   - Custom DSL for expectations
   - Can feel over-engineered
   - Lesson: Stay close to Python idioms

3. **Performance**
   - Validation can be slow on large datasets
   - Profiling is expensive
   - No built-in sampling
   - Lesson: Make validation efficient/configurable

4. **Expectation Management**
   - Expectations stored separately from code
   - Can get out of sync
   - Hard to version control
   - Lesson: Keep expectations with code

5. **Fragmentation**
   - GX 2.0/3.0 breaking changes
   - Confusing migration paths
   - Multiple APIs (legacy vs. new)
   - Lesson: Maintain API stability

## Key Takeaways for VibePiper Design

1. **Validation Library Core**
   - GX's biggest strength is expectation library
   - 50+ validation types
   - Consistent API
   - VibePiper should build similar:
     - Core validation functions
     - Consistent naming (expect_* or validate_*)
     - Extensible for custom validations

2. **Expectation vs. Validation**
   - GX: "expectation" = definition, "validation" = execution
   - Good distinction
   - VibePiper: "schema" = definition, "validate" = execution
   - Already similar! Keep this.

3. **Auto-Profiling**
   - Huge productivity booster
   - Reduces boilerplate
   - Good for onboarding
   - VibePiper should add:
     ```python
     @asset
     @profile()  # Auto-generate schema from data
     def customers():
         return df
     ```

4. **Documentation Generation**
   - GX's Data Docs are excellent
   - Auto-generated from expectations
   - Human-readable
   - VibePiper should generate:
     - Schema documentation
     - Validation history
     - Sample data
     - Statistics

5. **Validation Results**
   - Rich result objects
   - Statistics, examples, partial success
   - Useful for monitoring
   - VibePiper should track:
     - Validation history
     - Trends over time
     - Failures and warnings

6. **Keep It Simple**
   - GX is complex
   - VibePiper can be simpler
   - No need for DataContext
   - No need for YAML config
   - Just Python functions

## Comparison with VibePiper (Current State)

### What VibePiper Already Does Well
- ✅ Schema definitions (similar to expectation suites)
- ✅ Validation operators
- ✅ Type system
- ✅ Simpler than GX

### What VibePiper Could Learn
- ❌ Rich validation result objects
- ❌ Auto-profiling
- ❌ Validation history tracking
- ❌ Data docs generation
- ❌ More validation types (50+ vs current limited set)
- ❌ Metadata collection during validation

### VibePiper Differentiation Opportunities
- 🚀 **Simpler setup**: No config files, just Python
- 🚀 **Integrated with assets**: Validation is part of asset execution
- 🚀 **Better performance**: Designed for speed
- 🚀 **Pythonic**: Uses standard Python patterns
- 🚀 **Type-safe**: Leverages Python type hints

## Integration Ideas for VibePiper

1. **Enhanced Validation Library**
   ```python
   from vibe_piper.validation import expect

   @asset
   @expect.column_values_not_null('email')
   @expect.column_values_match_regex('email', r'^[\w\.-]+@')
   @expect.column_values_between('age', 0, 120)
   @expect.table_row_count_between(100, 10000)
   def customers():
       return df
   ```
   - GX-like expectations
   - Pythonic decorator syntax
   - Co-located with assets

2. **Profiling Decorator**
   ```python
   @asset
   @profile(sample_size=10000)  # Profile first N rows
   def customers():
       return df
   # Auto-generates schema based on data
   ```
   - Auto-infer schema
   - Sample-based for performance
   - Human in the loop

3. **Rich Validation Results**
   ```python
   result = validate_asset(customers)
   print(result)
   # ValidationResult(
   #     success=True,
   #     statistics={'evaluated': 10000, 'failed': 0},
   #     details={...}
   # )
   ```
   - Detailed results
   - Statistics
   - Failed rows
   - Warnings

4. **Validation History**
   ```python
   @asset
   @validate(track_history=True)
   def customers():
       return df
   # Stores validation results
   # Can query history:
   # history = get_validation_history('customers')
   ```
   - Track over time
   - Detect degradation
   - Monitor data quality

5. **Data Docs Generator**
   ```bash
   vibe-piper generate-docs --output ./docs
   # Generates HTML site with:
   # - All assets and schemas
   # - Validation results
   # - Statistics
   # - Sample data
   ```
   - Similar to GX Data Docs
   - Similar to dbt docs
   - One command to generate

6. **Expectation Store**
   ```python
   # Define expectations once
   customer_expectations = ExpectationSuite([
       expect_column_not_null('email'),
       expect_column_unique('id'),
       expect_column_values_between('age', 0, 120)
   ])

   # Use on multiple assets
   @asset(expectations=customer_expectations)
   def customers_v1():
       return df

   @asset(expectations=customer_expectations)
   def customers_v2():
       return df
   ```
   - Reusable expectation suites
   - Version-controlled
   - Share across assets

## When to Use GX vs. VibePiper Validation

- **Use Great Expectations when**:
  - You need complex data quality tracking
  - You have dedicated data quality team
  - You want heavy data documentation
  - You're OK with complex setup

- **Use VibePiper Validation when**:
  - You want simple, Pythonic validation
  - Validation is part of pipeline, not separate
  - You want type-safe validation
  - You want integrated asset + validation framework

## References

- https://greatexpectations.io/
- https://docs.greatexpectations.io/
- https://github.com/great-expectations/great_expectations
- https://www.datacamp.com/tutorial/great-expectations-tutorial
