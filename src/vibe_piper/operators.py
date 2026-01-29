"""
Built-in transformation operators for Vibe Piper.

This module provides a library of common transformation operations that can be
used in pipelines. Each operator returns an Operator instance that can be added
to a Pipeline.
"""

from collections.abc import Callable
from typing import Any, TypeVar

from vibe_piper.types import (
    DataRecord,
    DataType,
    Expectation,
    Operator,
    OperatorFn,
    OperatorType,
    PipelineContext,
    Schema,
)

try:
    from vibe_piper.expectations import ExpectationSuite, SuiteResult
except ImportError:
    # expectations module not available yet
    ExpectationSuite = None  # type: ignore
    SuiteResult = None  # type: ignore

T = TypeVar("T")


# =============================================================================
# Map Operators
# =============================================================================


def map_transform(
    name: str,
    transform_fn: Callable[[DataRecord, PipelineContext], DataRecord],
    description: str | None = None,
) -> Operator:
    """
    Create a map operator that applies a function to each DataRecord.

    Args:
        name: Unique identifier for this operator
        transform_fn: Function to apply to each record
        description: Optional description of what this operator does

    Returns:
        An Operator instance that can be added to a Pipeline

    Example:
        Transform each record to uppercase a field::

            def uppercase_email(record: DataRecord, ctx: PipelineContext) -> DataRecord:
                new_data = dict(record.data)
                if "email" in new_data and new_data["email"]:
                    new_data["email"] = new_data["email"].upper()
                return DataRecord(data=new_data, schema=record.schema)

            map_op = map_transform(
                name="uppercase_emails",
                transform_fn=uppercase_email,
                description="Convert email addresses to uppercase"
            )
    """
    return Operator(
        name=name,
        operator_type=OperatorType.TRANSFORM,
        fn=lambda data, ctx: [transform_fn(r, ctx) for r in data],
        description=description,
    )


def map_field(
    name: str,
    field_name: str,
    transform_fn: Callable[[Any], Any],
    description: str | None = None,
) -> Operator:
    """
    Create a map operator that transforms a specific field in each record.

    Args:
        name: Unique identifier for this operator
        field_name: Name of the field to transform
        transform_fn: Function to apply to the field value
        description: Optional description

    Returns:
        An Operator instance that can be added to a Pipeline

    Example:
        Transform a field to uppercase::

            map_op = map_field(
                name="uppercase_name",
                field_name="name",
                transform_fn=str.upper,
                description="Convert name field to uppercase"
            )
    """
    return Operator(
        name=name,
        operator_type=OperatorType.TRANSFORM,
        fn=lambda data, ctx: [
            (
                DataRecord(
                    data={**r.data, field_name: transform_fn(r.get(field_name))},
                    schema=r.schema,
                )
                if field_name in r.data
                else r
            )
            for r in data
        ],
        description=description or f"Transform field '{field_name}'",
    )


def add_field(
    name: str,
    field_name: str,
    field_type: DataType,
    value_fn: Callable[[DataRecord, PipelineContext], Any],
    description: str | None = None,
) -> Operator:
    """
    Create a map operator that adds a new field to each record.

    Args:
        name: Unique identifier for this operator
        field_name: Name of the field to add
        field_type: Data type of the new field
        value_fn: Function that computes the field value
        description: Optional description

    Returns:
        An Operator instance that can be added to a Pipeline

    Example:
        Add a computed field::

            def compute_full_name(record: DataRecord, ctx: PipelineContext) -> str:
                return f"{record.get('first_name')} {record.get('last_name')}"

            add_op = add_field(
                name="add_full_name",
                field_name="full_name",
                field_type=DataType.STRING,
                value_fn=compute_full_name,
                description="Add full name field"
            )
    """
    return Operator(
        name=name,
        operator_type=OperatorType.TRANSFORM,
        fn=lambda data, ctx: [
            DataRecord(
                data={**r.data, field_name: value_fn(r, ctx)},
                schema=r.schema,
            )
            for r in data
        ],
        description=description or f"Add field '{field_name}'",
    )


# =============================================================================
# Filter Operators
# =============================================================================


def filter_operator(
    name: str,
    predicate: Callable[[DataRecord, PipelineContext], bool],
    description: str | None = None,
) -> Operator:
    """
    Create a filter operator that filters records based on a predicate.

    Args:
        name: Unique identifier for this operator
        predicate: Function that returns True to keep a record, False to filter it out
        description: Optional description

    Returns:
        An Operator instance that can be added to a Pipeline

    Example:
        Filter records where age is greater than 18::

            def is_adult(record: DataRecord, ctx: PipelineContext) -> bool:
                return record.get("age", 0) > 18

            filter_op = filter_operator(
                name="filter_adults",
                predicate=is_adult,
                description="Keep only adult records"
            )
    """
    return Operator(
        name=name,
        operator_type=OperatorType.FILTER,
        fn=lambda data, ctx: [r for r in data if predicate(r, ctx)],
        description=description,
    )


def filter_field_equals(
    name: str,
    field_name: str,
    value: Any,
    description: str | None = None,
) -> Operator:
    """
    Create a filter operator that filters records where a field equals a value.

    Args:
        name: Unique identifier for this operator
        field_name: Name of the field to check
        value: Value to compare against
        description: Optional description

    Returns:
        An Operator instance that can be added to a Pipeline

    Example:
        Filter records where status is 'active'::

            filter_op = filter_field_equals(
                name="filter_active",
                field_name="status",
                value="active",
                description="Keep only active records"
            )
    """
    return Operator(
        name=name,
        operator_type=OperatorType.FILTER,
        fn=lambda data, ctx: [r for r in data if r.get(field_name) == value],
        description=description or f"Filter where '{field_name}' equals {value!r}",
    )


def filter_field_not_null(
    name: str,
    field_name: str,
    description: str | None = None,
) -> Operator:
    """
    Create a filter operator that removes records where a field is null or missing.

    Args:
        name: Unique identifier for this operator
        field_name: Name of the field to check
        description: Optional description

    Returns:
        An Operator instance that can be added to a Pipeline

    Example:
        Remove records with null email field::

            filter_op = filter_field_not_null(
                name="filter_has_email",
                field_name="email",
                description="Remove records without email"
            )
    """
    return Operator(
        name=name,
        operator_type=OperatorType.FILTER,
        fn=lambda data, ctx: [
            r for r in data if field_name in r.data and r.get(field_name) is not None
        ],
        description=description or f"Filter where '{field_name}' is not null",
    )


# =============================================================================
# Aggregate Operators
# =============================================================================


def aggregate_count(
    name: str,
    description: str | None = None,
) -> Operator:
    """
    Create an aggregate operator that counts records.

    Args:
        name: Unique identifier for this operator
        description: Optional description

    Returns:
        An Operator instance that can be added to a Pipeline

    Example:
        Count the number of records::

            count_op = aggregate_count(
                name="count_records",
                description="Count total records"
            )
    """
    return Operator(
        name=name,
        operator_type=OperatorType.AGGREGATE,
        fn=lambda data, ctx: len(data),
        description=description or "Count records",
    )


def aggregate_sum(
    name: str,
    field_name: str,
    description: str | None = None,
) -> Operator:
    """
    Create an aggregate operator that sums a field across all records.

    Args:
        name: Unique identifier for this operator
        field_name: Name of the field to sum
        description: Optional description

    Returns:
        An Operator instance that can be added to a Pipeline

    Example:
        Sum the 'amount' field::

            sum_op = aggregate_sum(
                name="sum_amount",
                field_name="amount",
                description="Sum all amounts"
            )
    """
    return Operator(
        name=name,
        operator_type=OperatorType.AGGREGATE,
        fn=lambda data, ctx: sum(r.get(field_name, 0) for r in data),
        description=description or f"Sum field '{field_name}'",
    )


def aggregate_group_by(
    name: str,
    group_field: str,
    aggregate_fn: Callable[[list[DataRecord]], Any],
    description: str | None = None,
) -> Operator:
    """
    Create an aggregate operator that groups records by a field.

    Args:
        name: Unique identifier for this operator
        group_field: Field to group by
        aggregate_fn: Function to apply to each group
        description: Optional description

    Returns:
        An Operator instance that can be added to a Pipeline

    Example:
        Group by 'category' and count items in each group::

            def count_group(records: list[DataRecord]) -> int:
                return len(records)

            group_op = aggregate_group_by(
                name="group_by_category",
                group_field="category",
                aggregate_fn=count_group,
                description="Group by category and count"
            )
    """

    def group_by_fn(
        data: list[DataRecord],
        ctx: PipelineContext,
    ) -> dict[Any, Any]:
        groups: dict[Any, list[DataRecord]] = {}
        for record in data:
            key = record.get(group_field)
            if key not in groups:
                groups[key] = []
            groups[key].append(record)

        return {key: aggregate_fn(group) for key, group in groups.items()}

    return Operator(
        name=name,
        operator_type=OperatorType.AGGREGATE,
        fn=group_by_fn,
        description=description or f"Group by '{group_field}'",
    )


# =============================================================================
# Validate Operators
# =============================================================================


def validate_schema(
    name: str,
    schema: Schema,
    description: str | None = None,
) -> Operator:
    """
    Create a validation operator that validates records against a schema.

    Args:
        name: Unique identifier for this operator
        schema: Schema to validate against
        description: Optional description

    Returns:
        An Operator instance that can be added to a Pipeline

    Example:
        Validate records against a user schema::

            validate_op = validate_schema(
                name="validate_user_schema",
                schema=user_schema,
                description="Validate user records"
            )
    """

    def validate_fn(
        data: list[DataRecord],
        ctx: PipelineContext,
    ) -> list[DataRecord]:
        # Validation happens in DataRecord.__post_init__
        # This operator just re-creates records to trigger validation
        return [DataRecord(data=r.data, schema=schema, metadata=r.metadata) for r in data]

    return Operator(
        name=name,
        operator_type=OperatorType.VALIDATE,
        fn=validate_fn,
        input_schema=schema,
        output_schema=schema,
        description=description or f"Validate against schema '{schema.name}'",
    )


def validate_expectation(
    name: str,
    expectation: Expectation,
    on_failure: str = "fail",
    description: str | None = None,
) -> Operator:
    """
    Create a validation operator that validates data against a single expectation.

    Args:
        name: Unique identifier for this operator
        expectation: The expectation to validate against
        on_failure: Action to take on validation failure ('fail', 'warn', 'ignore')
        description: Optional description

    Returns:
        An Operator instance that can be added to a Pipeline

    Example:
        Validate that all emails contain '@'::

            @expect
            def expect_valid_email(data: Any) -> bool:
                if isinstance(data, str):
                    return "@" in data
                return False

            validate_op = validate_expectation(
                name="validate_email_format",
                expectation=expect_valid_email,
                on_failure="fail",
                description="Ensure email addresses are valid"
            )
    """
    if on_failure not in ("fail", "warn", "ignore"):
        msg = f"Invalid on_failure value: {on_failure!r}"
        raise ValueError(msg)

    def validate_fn(data: Any, ctx: PipelineContext) -> Any:
        result = expectation.validate(data)

        if not result.is_valid:
            error_msg = f"Expectation '{expectation.name}' failed: {result.errors}"
            if on_failure == "fail":
                raise ValueError(error_msg)
            elif on_failure == "warn":
                # Log warning to context metadata
                warnings = ctx.get_state("validation_warnings", [])
                warnings.extend(result.errors)
                ctx.set_state("validation_warnings", warnings)
            # If 'ignore', do nothing

        return data

    return Operator(
        name=name,
        operator_type=OperatorType.VALIDATE,
        fn=validate_fn,
        description=description or f"Validate expectation '{expectation.name}'",
        config={"expectation": expectation.name, "on_failure": on_failure},
    )


def validate_expectation_suite(
    name: str,
    suite: "ExpectationSuite",
    on_failure: str = "fail",
    store_results: bool = True,
    description: str | None = None,
) -> Operator:
    """
    Create a validation operator that validates data against an expectation suite.

    Args:
        name: Unique identifier for this operator
        suite: The ExpectationSuite to validate against
        on_failure: Action to take on validation failure ('fail', 'warn', 'ignore')
        store_results: Whether to store validation results in context state
        description: Optional description

    Returns:
        An Operator instance that can be added to a Pipeline

    Example:
        Validate data against a suite of expectations::

            suite = ExpectationSuite(name="data_quality")
            suite.add_expectation(expect_not_null)
            suite.add_expectation(expect_positive)

            validate_op = validate_expectation_suite(
                name="validate_data_quality",
                suite=suite,
                on_failure="fail",
                description="Run data quality checks"
            )
    """
    if on_failure not in ("fail", "warn", "ignore"):
        msg = f"Invalid on_failure value: {on_failure!r}"
        raise ValueError(msg)

    def validate_fn(data: Any, ctx: PipelineContext) -> Any:
        result: SuiteResult = suite.validate(data)

        if store_results:
            # Store results in context state
            ctx.set_state(f"validation_results_{name}", result)

        if not result.success:
            error_msg = f"Expectation suite '{suite.name}' failed: {result.errors}"
            if on_failure == "fail":
                raise ValueError(error_msg)
            elif on_failure == "warn":
                # Log warning to context metadata
                warnings = ctx.get_state("validation_warnings", [])
                warnings.extend(result.errors)
                ctx.set_state("validation_warnings", warnings)
            # If 'ignore', do nothing

        return data

    return Operator(
        name=name,
        operator_type=OperatorType.VALIDATE,
        fn=validate_fn,
        description=description or f"Validate expectation suite '{suite.name}'",
        config={
            "suite": suite.name,
            "expectation_count": len(suite),
            "on_failure": on_failure,
            "store_results": store_results,
        },
    )


# =============================================================================
# Custom Operators
# =============================================================================


def custom_operator(
    name: str,
    fn: OperatorFn[Any, Any],
    description: str | None = None,
    input_schema: Schema | None = None,
    output_schema: Schema | None = None,
    config: dict[str, Any] | None = None,
) -> Operator:
    """
    Create a custom operator with a user-provided function.

    Args:
        name: Unique identifier for this operator
        fn: Function that implements the operator logic
        description: Optional description
        input_schema: Optional input schema for validation
        output_schema: Optional output schema for validation
        config: Optional operator configuration

    Returns:
        An Operator instance that can be added to a Pipeline

    Example:
        Create a custom transformation::

            def custom_transform(
                data: list[DataRecord],
                ctx: PipelineContext
            ) -> list[DataRecord]:
                # Custom logic here
                return data

            custom_op = custom_operator(
                name="my_custom_transform",
                fn=custom_transform,
                description="My custom transformation"
            )
    """
    return Operator(
        name=name,
        operator_type=OperatorType.CUSTOM,
        fn=fn,
        input_schema=input_schema,
        output_schema=output_schema,
        description=description,
        config=config or {},
    )


# =============================================================================
# Quality Check Operators
# =============================================================================


def check_quality_completeness(
    name: str,
    threshold: float = 1.0,
    description: str | None = None,
) -> Operator:
    """
    Create an operator that checks data completeness.

    Args:
        name: Unique identifier for this operator
        threshold: Minimum completeness score (0-1) to pass
        description: Optional description

    Returns:
        An Operator instance that can be added to a Pipeline

    Example:
        Check data completeness::

            from vibe_piper import check_quality_completeness

            quality_op = check_quality_completeness(
                name="check_completeness",
                threshold=0.95,
                description="Ensure 95% of fields are populated"
            )
    """
    from vibe_piper.quality import check_completeness

    def completeness_fn(
        data: list[DataRecord],
        ctx: PipelineContext,  # noqa: ARG001
    ) -> list[DataRecord]:
        # Run completeness check
        result = check_completeness(data, threshold=threshold)

        # Store result in context for later retrieval
        ctx.set_state(f"{name}_result", result)

        # Return data unchanged (check is non-blocking)
        return data

    return Operator(
        name=name,
        operator_type=OperatorType.VALIDATE,
        fn=completeness_fn,
        description=description or f"Check completeness (threshold: {threshold})",
    )


def check_quality_validity(
    name: str,
    threshold: float = 1.0,
    description: str | None = None,
) -> Operator:
    """
    Create an operator that checks data validity against schema.

    Args:
        name: Unique identifier for this operator
        threshold: Minimum validity score (0-1) to pass
        description: Optional description

    Returns:
        An Operator instance that can be added to a Pipeline

    Example:
        Check data validity::

            from vibe_piper import check_quality_validity

            quality_op = check_quality_validity(
                name="check_validity",
                threshold=0.98,
                description="Ensure 98% of records pass schema validation"
            )
    """
    from vibe_piper.quality import check_validity

    def validity_fn(
        data: list[DataRecord],
        ctx: PipelineContext,  # noqa: ARG001
    ) -> list[DataRecord]:
        # Run validity check
        result = check_validity(data, threshold=threshold)

        # Store result in context for later retrieval
        ctx.set_state(f"{name}_result", result)

        # Return data unchanged (check is non-blocking)
        return data

    return Operator(
        name=name,
        operator_type=OperatorType.VALIDATE,
        fn=validity_fn,
        description=description or f"Check validity (threshold: {threshold})",
    )


def check_quality_uniqueness(
    name: str,
    unique_fields: tuple[str, ...] = (),
    threshold: float = 1.0,
    description: str | None = None,
) -> Operator:
    """
    Create an operator that checks data uniqueness.

    Args:
        name: Unique identifier for this operator
        unique_fields: Fields that should be unique
        threshold: Minimum uniqueness score (0-1) to pass
        description: Optional description

    Returns:
        An Operator instance that can be added to a Pipeline

    Example:
        Check uniqueness of email field::

            from vibe_piper import check_quality_uniqueness

            quality_op = check_quality_uniqueness(
                name="check_unique_emails",
                unique_fields=("email",),
                threshold=1.0,
                description="Ensure all emails are unique"
            )
    """
    from vibe_piper.quality import check_uniqueness

    def uniqueness_fn(
        data: list[DataRecord],
        ctx: PipelineContext,  # noqa: ARG001
    ) -> list[DataRecord]:
        # Run uniqueness check
        result = check_uniqueness(data, unique_fields=unique_fields, threshold=threshold)

        # Store result in context for later retrieval
        ctx.set_state(f"{name}_result", result)

        # Return data unchanged (check is non-blocking)
        return data

    return Operator(
        name=name,
        operator_type=OperatorType.VALIDATE,
        fn=uniqueness_fn,
        description=description
        or f"Check uniqueness of {unique_fields if unique_fields else 'all fields'} "
        f"(threshold: {threshold})",
    )


def check_quality_freshness(
    name: str,
    timestamp_field: str,
    max_age_hours: float = 24.0,
    description: str | None = None,
) -> Operator:
    """
    Create an operator that checks data freshness.

    Args:
        name: Unique identifier for this operator
        timestamp_field: Name of the timestamp field to check
        max_age_hours: Maximum acceptable age in hours
        description: Optional description

    Returns:
        An Operator instance that can be added to a Pipeline

    Example:
        Check data freshness::

            from vibe_piper import check_quality_freshness

            quality_op = check_quality_freshness(
                name="check_freshness",
                timestamp_field="created_at",
                max_age_hours=24.0,
                description="Ensure data is not older than 24 hours"
            )
    """
    from vibe_piper.quality import check_freshness

    def freshness_fn(
        data: list[DataRecord],
        ctx: PipelineContext,  # noqa: ARG001
    ) -> list[DataRecord]:
        # Run freshness check
        result = check_freshness(data, timestamp_field=timestamp_field, max_age_hours=max_age_hours)

        # Store result in context for later retrieval
        ctx.set_state(f"{name}_result", result)

        # Return data unchanged (check is non-blocking)
        return data

    return Operator(
        name=name,
        operator_type=OperatorType.VALIDATE,
        fn=freshness_fn,
        description=description
        or f"Check freshness of '{timestamp_field}' (max age: {max_age_hours}h)",
    )
