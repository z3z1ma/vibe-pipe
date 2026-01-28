"""
Validation framework for Vibe Piper.

This module provides comprehensive data validation capabilities including:
- 30+ built-in validation types
- @validate decorator for assets
- @expect fluent API for expectations
- Validation suites for organizing checks
- Detailed validation results
- Lazy validation mode (collect all errors)

Example usage:

    @asset
    @validate(schema=CustomerSchema, lazy=True)
    @expect.column("email").to_match_regex(r"^[\\w\\.-]+@")
    @expect.column("age").to_be_between(0, 120)
    def customers():
        return records
"""

# Import checks using importlib to avoid circular issues
import importlib
checks = importlib.import_module('.checks', package='vibe_piper.validation')

# TODO: Investigate why expect_column_values_to_not_in_set cannot be imported
# from checks module when accessed through package
# For now, we'll skip this one function
_expect_not_in_set = None

# Re-export all check functions
ColumnValidationResult = checks.ColumnValidationResult
create_custom_validation = checks.create_custom_validation
expect_column_groupby_mean_to_be_between = checks.expect_column_groupby_mean_to_be_between
expect_column_groupby_value_counts_to_be_between = checks.expect_column_groupby_value_counts_to_be_between
expect_column_max_to_be_between = checks.expect_column_max_to_be_between
expect_column_mean_to_be_between = checks.expect_column_mean_to_be_between
expect_column_median_to_be_between = checks.expect_column_median_to_be_between
expect_column_min_to_be_between = checks.expect_column_min_to_be_between
expect_column_pair_values_a_to_be_greater_than_b = checks.expect_column_pair_values_a_to_be_greater_than_b
expect_column_pair_values_to_be_equal = checks.expect_column_pair_values_to_be_equal
expect_column_pair_values_to_be_not_equal = checks.expect_column_pair_values_to_be_not_equal
expect_column_proportion_of_nulls_to_be_between = checks.expect_column_proportion_of_nulls_to_be_between
expect_column_std_dev_to_be_between = checks.expect_column_std_dev_to_be_between
expect_column_sum_to_equal_other_column_sum = checks.expect_column_sum_to_equal_other_column_sum
expect_column_value_lengths_to_be_between = checks.expect_column_value_lengths_to_be_between
expect_column_values_to_be_between = checks.expect_column_values_to_be_between
expect_column_values_to_be_dateutil_parseable = checks.expect_column_values_to_be_dateutil_parseable
expect_column_values_to_be_decreasing = checks.expect_column_values_to_be_decreasing
expect_column_values_to_be_in_set = checks.expect_column_values_to_be_in_set
expect_column_values_to_be_increasing = checks.expect_column_values_to_be_increasing
expect_column_values_to_not_be_null = checks.expect_column_values_to_not_be_null
expect_column_values_to_not_in_set = _expect_not_in_set  # Workaround: use direct import
expect_column_values_to_be_of_type = checks.expect_column_values_to_be_of_type
expect_column_values_to_be_unique = checks.expect_column_values_to_be_unique
expect_column_values_to_match_regex = checks.expect_column_values_to_match_regex
expect_column_values_to_not_match_regex = checks.expect_column_values_to_not_match_regex
expect_table_row_count_to_be_between = checks.expect_table_row_count_to_be_between
expect_table_row_count_to_equal = checks.expect_table_row_count_to_equal

# Import decorators and suite
from vibe_piper.validation.decorators import (
    ColumnExpectationBuilder,
    ExpectationBuilder,
    MultiColumnExpectationBuilder,
    TableExpectationBuilder,
    ValidationConfig,
    ValidateDecorator,
    expect,
    validate,
)
from vibe_piper.validation.suite import (
    LazyValidationStrategy,
    ValidationContext,
    ValidationSuite,
    ValidationStrategy,
    create_validation_suite,
)

__all__ = [
    # Decorators and builders
    "validate",
    "expect",
    "ValidationConfig",
    # Suite and strategy
    "ValidationSuite",
    "ValidationStrategy",
    "LazyValidationStrategy",
    "ValidationContext",
    "create_validation_suite",
    # Check functions (30+ validations)
    "expect_column_mean_to_be_between",
    "expect_column_std_dev_to_be_between",
    "expect_column_min_to_be_between",
    "expect_column_max_to_be_between",
    "expect_column_median_to_be_between",
    "expect_column_values_to_match_regex",
    "expect_column_values_to_not_match_regex",
    "expect_column_values_to_be_between",
    "expect_column_values_to_be_in_set",
    "expect_column_values_to_not_be_in_set",
    "expect_column_values_to_be_unique",
    "expect_column_values_to_be_of_type",
    "expect_column_values_to_not_be_null",
    "expect_column_proportion_of_nulls_to_be_between",
    "expect_column_value_lengths_to_be_between",
    "expect_column_values_to_be_increasing",
    "expect_column_values_to_be_decreasing",
    "expect_column_pair_values_to_be_equal",
    "expect_column_pair_values_to_be_not_equal",
    "expect_column_pair_values_a_to_be_greater_than_b",
    "expect_column_sum_to_equal_other_column_sum",
    "expect_column_groupby_value_counts_to_be_between",
    "expect_column_groupby_mean_to_be_between",
    "expect_table_row_count_to_be_between",
    "expect_table_row_count_to_equal",
    "expect_column_values_to_be_dateutil_parseable",
    "create_custom_validation",
    # Result types
    "ColumnValidationResult",
    # Builder classes
    "ExpectationBuilder",
    "ColumnExpectationBuilder",
    "MultiColumnExpectationBuilder",
    "TableExpectationBuilder",
]

__version__ = "1.0.0"
