# Pandera Research Memo

**Date:** 2026-01-27
**Framework:** Pandera
**Focus:** Statistical Data Validation for Python

## Executive Summary

Pandera is a lightweight, flexible data validation library for pandas and polars DataFrames. It provides a declarative API for defining schemas, runtime validation, and type checking. It's "pandas-but-with-schemas" and fills a critical gap in Python data tooling.

## Core Architecture

### Design Philosophy
- **Declarative schemas**: Define what data should look like
- **Runtime validation**: Fail fast when data doesn't match schema
- **Type safety**: Use Python type hints for DataFrame schemas
- **Framework-agnostic**: Works with pandas, polars, etc.

### Key Components

1. **DataFrame Schemas**
   ```python
   import pandera as pa

   schema = pa.DataFrameSchema({
       'customer_id': pa.Column(int),
       'email': pa.Column(str),
       'signup_date': pa.Column(datetime),
       'is_active': pa.Column(bool, nullable=True)
   })

   validated_df = schema.validate(df)
   ```
   - Column-level definitions
   - Type checking
   - Nullability constraints
   - Custom validators

2. **Column-Level Checks**
   ```python
   schema = pa.DataFrameSchema({
       'age': pa.Column(
           int,
           checks=[
               pa.Check.ge(0),
               pa.Check.le(120),
               pa.Check(lambda x: x.mean() > 18)
           ]
       ),
       'email': pa.Column(
           str,
           checks=pa.Check.str_matches(r'^[\w\.-]+@[\w\.-]+\.\w+$')
       )
   })
   ```
   - Built-in checks (ge, le, in_range, etc.)
   - Custom functions
   - String validation
   - Statistical checks

3. **Multi-Column Checks**
   ```python
   schema = pa.DataFrameSchema(
       columns={
           'start_date': pa.Column(datetime),
           'end_date': pa.Column(datetime)
       },
       checks=pa.Check(
           lambda df: df['end_date'] >= df['start_date'],
           error="end_date must be >= start_date"
       )
   )
   ```
   - Cross-column validation
   - Custom error messages
   - Aggregate checks

4. **Type Annotations**
   ```python
   from pandera.typing import DataFrame, Series

   class CustomerSchema(pa.DataFrameModel):
       customer_id: Series[int] = pa.Field(ge=1)
       email: Series[str] = pa.Field(str_matches=r'^[\w\.-]+@')
       signup_date: Series[datetime]

   def process_data(df: DataFrame[CustomerSchema]) -> DataFrame[CustomerSchema]:
       return df
   ```
   - Use Python type hints
   - Runtime validation via decorators
   - IDE autocompletion
   - Static type checking with mypy

## Notable Patterns for VibePiper

### Strengths to Emulate

1. **Declarative Schema API** ⭐
   - Clean, readable syntax
   - Composable schemas
   - Reusable across contexts
   - Lesson: VibePiper's schema system is similar! Keep and enhance

2. **Runtime Validation**
   - Fails fast on bad data
   - Clear error messages
   - Validation happens at boundaries
   - Lesson: VibePiper should integrate validation into asset execution

3. **Type System Integration**
   - Leverages Python type hints
   - Works with mypy/pyright
   - Generic types for DataFrame schemas
   - Lesson: VibePiper's type system is on the right track

4. **Separation of Concerns**
   - Schema definition separate from validation
   - Can validate at any point
   - Can disable validation in production (performance)
   - Lesson: Make validation configurable

5. **Flexible Validation Levels**
   ```python
   # Strict: validate everything
   schema.validate(df, lazy=False)

   # Lazy: collect all errors
   schema.validate(df, lazy=True)

   # Disabled: skip validation
   schema.validate(df, validation_depth='none')
   ```
   - Adapt to development/production
   - Performance optimization
   - Lesson: VibePiper should have similar flexibility

### Anti-Patterns to Avoid

1. **Validation Overhead**
   - Runtime validation has performance cost
   - Large datasets can be slow to validate
   - Lesson: Make validation optional/skippable in production

2. **Limited to DataFrames**
   - Focused on tabular data
   - Not for nested/structured data
   - Lesson: VibePiper should support more than just DataFrames

3. **Schema Evolution**
   - No built-in schema migration
   - Breaking changes are hard to manage
   - Lesson: Think about schema versioning from day one

## Key Takeaways for VibePiper Design

1. **Validation is Critical**
   - Data quality is a first-class concern
   - Runtime validation catches bugs early
   - Should be integrated into asset execution

2. **Schema-First Design**
   - Define schemas upfront
   - Use schemas for documentation
   - Generate docs from schemas
   - VibePiper already has this! Enhance it.

3. **Type Safety Matters**
   - Python type hints are powerful
   - Runtime + static type checking
   - Better IDE experience
   - VibePiper should lean into this

4. **Validation as Decorator**
   ```python
   @validate(schema=CustomerSchema)
   def process_data(df):
       return df
   ```
   - Clean syntax
   - Composable with other decorators
   - Easy to enable/disable

5. **Error Messages**
   - Pandera has excellent error messages
   - Shows exactly what failed
   - Suggestions for fixing
   - VibePiper should invest here

## Comparison with VibePiper (Current State)

### What VibePiper Already Does Well
- ✅ Declarative schema definitions (`define_schema()`)
- ✅ Type system (DataType, SchemaField)
- ✅ Validation operators (`validate_schema`)
- ✅ Similar pattern to Pandera

### What VibePiper Could Learn
- ❌ Decorator-based validation (`@validate`)
- ❌ More built-in checks (range, regex, statistical)
- ❌ Multi-column/aggregate checks
- ❌ Lazy validation (collect all errors)
- ❌ Better error messages
- ❌ Type hints integration (`DataFrame[CustomerSchema]`)

### VibePiper Differentiation Opportunities
- 🚀 **Beyond DataFrames**: Support dicts, lists, objects, Pydantic models
- 🚀 **Integrated with Assets**: Validate as part of asset execution
- 🚀 **Pipeline-Aware**: Validation aware of upstream/downstream assets
- 🚀 **Transformation Framework**: Validation + transformation in one

## Integration Ideas for VibePiper

1. **Enhanced Validation Decorator**
   ```python
   @asset
   @validate(schema=CustomerSchema, lazy=True)
   def customers(raw_data):
       return clean_data(raw_data)
   ```
   - Automatic validation on asset output
   - Collects all validation errors
   - Configurable validation modes

2. **Built-In Check Library**
   ```python
   from vibe_piper.validation import Check

   schema = define_schema({
       'age': Integer(check=Check.ge(0) & Check.le(120)),
       'email': String(check=Check.email),
       'score': Float(check=Check.in_range(0.0, 1.0))
   })
   ```
   - Reusable check objects
   - Composable with `&`, `|` operators
   - Rich standard library

3. **Type Hint Integration**
   ```python
   from vibe_piper.typing import Asset

   @asset
   def customers() -> Asset[CustomerSchema]:
       return load_customers()
   ```
   - Asset as generic type
   - Runtime validation
   - Static type checking

4. **Multi-Asset Validation**
   ```python
   @validate_assets([sales, costs, revenue])
   def validate_financials(sales, costs, revenue):
       assert revenue == sales - costs
       return revenue
   ```
   - Cross-asset validation
   - Business logic validation
   - Data reconciliation

5. **Validation Strategies**
   ```python
   @asset(validation="strict")  # Always validate
   @asset(validation="sample", sample_rate=0.1)  # Sample 10%
   @asset(validation="none")  # Skip validation
   def my_asset():
       pass
   ```
   - Adapt to environment
   - Performance optimization
   - Risk-based validation

## References

- https://pandera.readthedocs.io/
- https://github.com/pandera-dev/pandera
- https://towardsdatascience.com/data-validation-with-pandera-in-python-f07b0f845040
