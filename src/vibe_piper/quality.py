"""
Data quality metrics collection and analysis.

This module provides functions for collecting and analyzing data quality metrics,
including completeness, validity, uniqueness, freshness, and consistency checks.
"""

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any

from vibe_piper.types import (
    DataQualityReport,
    DataRecord,
    QualityCheckResult,
    QualityMetric,
    QualityMetricType,
    Schema,
)

# =============================================================================
# Completeness Metrics
# =============================================================================


def check_completeness(
    records: Sequence[DataRecord],
    schema: Schema | None = None,  # noqa: ARG001
    threshold: float = 1.0,
) -> QualityCheckResult:
    """
    Check data completeness by measuring missing/null values.

    Args:
        records: Records to check
        schema: Optional schema to validate against
        threshold: Minimum completeness score (0-1) to pass

    Returns:
        QualityCheckResult with completeness metrics
    """
    start_time = datetime.now()
    metrics: list[QualityMetric] = []
    errors: list[str] = []
    warnings: list[str] = []

    if not records:
        return QualityCheckResult(
            check_name="completeness",
            passed=True,
            metrics=tuple(metrics),
            duration_ms=(datetime.now() - start_time).total_seconds() * 1000,
        )

    total_fields = 0
    total_missing = 0
    field_missing_counts: dict[str, int] = {}

    for record in records:
        for schema_field in record.schema.fields:
            total_fields += 1
            field_name = schema_field.name

            # Check if field is missing or null
            if field_name not in record.data or record.data[field_name] is None:
                total_missing += 1
                field_missing_counts[field_name] = field_missing_counts.get(field_name, 0) + 1

    # Calculate overall completeness
    completeness_score = 1.0 - (total_missing / total_fields) if total_fields > 0 else 1.0
    passed = completeness_score >= threshold

    # Add overall completeness metric
    metrics.append(
        QualityMetric(
            name="completeness_score",
            metric_type=QualityMetricType.COMPLETENESS,
            value=round(completeness_score, 4),
            threshold=threshold,
            passed=passed,
            description="Fraction of non-null fields",
        )
    )

    # Add missing count metric
    metrics.append(
        QualityMetric(
            name="missing_count",
            metric_type=QualityMetricType.COMPLETENESS,
            value=total_missing,
            description="Total number of missing/null values",
        )
    )

    # Add per-field completeness metrics
    for field_name, missing_count in field_missing_counts.items():
        field_completeness = 1.0 - (missing_count / len(records))
        metrics.append(
            QualityMetric(
                name=f"completeness_{field_name}",
                metric_type=QualityMetricType.COMPLETENESS,
                value=round(field_completeness, 4),
                description=f"Completeness for field '{field_name}'",
                metadata={
                    "missing_count": missing_count,
                    "total_records": len(records),
                },
            )
        )

        # Warn if field completeness is low
        if field_completeness < 0.9:
            warnings.append(f"Field '{field_name}' has low completeness: {field_completeness:.2%}")

    if not passed:
        errors.append(
            f"Completeness score {completeness_score:.2%} is below threshold {threshold:.2%}"
        )

    return QualityCheckResult(
        check_name="completeness",
        passed=passed,
        metrics=tuple(metrics),
        errors=tuple(errors),
        warnings=tuple(warnings),
        duration_ms=(datetime.now() - start_time).total_seconds() * 1000,
    )


# =============================================================================
# Validity Metrics
# =============================================================================


def check_validity(
    records: Sequence[DataRecord],
    schema: Schema | None = None,  # noqa: ARG001
    threshold: float = 1.0,
) -> QualityCheckResult:
    """
    Check data validity against schema.

    Args:
        records: Records to check
        schema: Schema to validate against
        threshold: Minimum validity score (0-1) to pass

    Returns:
        QualityCheckResult with validity metrics
    """
    start_time = datetime.now()
    metrics: list[QualityMetric] = []
    errors: list[str] = []
    warnings: list[str] = []

    if not records:
        return QualityCheckResult(
            check_name="validity",
            passed=True,
            metrics=tuple(metrics),
            duration_ms=(datetime.now() - start_time).total_seconds() * 1000,
        )

    valid_count = 0
    validation_errors: list[str] = []

    for record in records:
        try:
            # Validate record against its schema
            # DataRecord already validates in __post_init__, but we check again
            for schema_field in record.schema.fields:
                if schema_field.required and schema_field.name not in record.data:
                    validation_errors.append(f"Record missing required field: {schema_field.name}")

                if (
                    not schema_field.nullable
                    and schema_field.name in record.data
                    and record.data[schema_field.name] is None
                ):
                    validation_errors.append(
                        f"Field '{schema_field.name}' is not nullable but has null value"
                    )

            # If we got here without exceptions, record is valid
            if not validation_errors or all(
                f"Record missing required field: {schema_field.name}" not in err
                and f"Field '{schema_field.name}' is not nullable" not in err
                for err in validation_errors[-len(record.schema.fields) :]
            ):
                valid_count += 1

        except ValueError as e:
            validation_errors.append(str(e))

    validity_score = valid_count / len(records) if records else 1.0
    passed = validity_score >= threshold

    metrics.append(
        QualityMetric(
            name="validity_score",
            metric_type=QualityMetricType.VALIDITY,
            value=round(validity_score, 4),
            threshold=threshold,
            passed=passed,
            description="Fraction of records passing schema validation",
        )
    )

    metrics.append(
        QualityMetric(
            name="valid_count",
            metric_type=QualityMetricType.VALIDITY,
            value=valid_count,
            description="Number of valid records",
        )
    )

    metrics.append(
        QualityMetric(
            name="invalid_count",
            metric_type=QualityMetricType.VALIDITY,
            value=len(records) - valid_count,
            description="Number of invalid records",
        )
    )

    if not passed:
        errors.append(f"Validity score {validity_score:.2%} is below threshold {threshold:.2%}")

    # Add first few validation errors
    for error in validation_errors[:10]:
        errors.append(error)

    if len(validation_errors) > 10:
        warnings.append(f"... and {len(validation_errors) - 10} more validation errors")

    return QualityCheckResult(
        check_name="validity",
        passed=passed,
        metrics=tuple(metrics),
        errors=tuple(errors),
        warnings=tuple(warnings),
        duration_ms=(datetime.now() - start_time).total_seconds() * 1000,
    )


# =============================================================================
# Uniqueness Metrics
# =============================================================================


def check_uniqueness(
    records: Sequence[DataRecord],
    unique_fields: tuple[str, ...] = (),
    threshold: float = 1.0,
) -> QualityCheckResult:
    """
    Check data uniqueness by detecting duplicate records.

    Args:
        records: Records to check
        unique_fields: Fields that should be unique (if empty, checks all fields)
        threshold: Minimum uniqueness score (0-1) to pass

    Returns:
        QualityCheckResult with uniqueness metrics
    """
    start_time = datetime.now()
    metrics: list[QualityMetric] = []
    errors: list[str] = []
    warnings: list[str] = []

    if not records:
        return QualityCheckResult(
            check_name="uniqueness",
            passed=True,
            metrics=tuple(metrics),
            duration_ms=(datetime.now() - start_time).total_seconds() * 1000,
        )

    # Determine which fields to use for uniqueness
    fields_to_check = unique_fields if unique_fields else ()

    if fields_to_check:
        # Check uniqueness of specified fields
        seen: set[tuple[Any, ...]] = set()
        duplicate_count = 0

        for record in records:
            # Create a tuple of values for the specified fields
            key = tuple(record.get(field) for field in fields_to_check)

            if key in seen:
                duplicate_count += 1
            else:
                seen.add(key)

        unique_count = len(seen)
        uniqueness_score = unique_count / len(records) if records else 1.0
        passed = uniqueness_score >= threshold

        metrics.append(
            QualityMetric(
                name="uniqueness_score",
                metric_type=QualityMetricType.UNIQUENESS,
                value=round(uniqueness_score, 4),
                threshold=threshold,
                passed=passed,
                description=f"Fraction of unique records by {fields_to_check}",
            )
        )

        metrics.append(
            QualityMetric(
                name="unique_count",
                metric_type=QualityMetricType.UNIQUENESS,
                value=unique_count,
                description="Number of unique records",
            )
        )

        metrics.append(
            QualityMetric(
                name="duplicate_count",
                metric_type=QualityMetricType.UNIQUENESS,
                value=duplicate_count,
                description="Number of duplicate records",
            )
        )

        if not passed:
            errors.append(
                f"Uniqueness score {uniqueness_score:.2%} is below threshold {threshold:.2%}"
            )

        if duplicate_count > 0:
            warnings.append(f"Found {duplicate_count} duplicate records")

    else:
        # No unique fields specified - report as informational
        metrics.append(
            QualityMetric(
                name="uniqueness_score",
                metric_type=QualityMetricType.UNIQUENESS,
                value=1.0,
                description="Uniqueness not checked (no unique fields specified)",
            )
        )

        passed = True

    return QualityCheckResult(
        check_name="uniqueness",
        passed=passed,
        metrics=tuple(metrics),
        errors=tuple(errors),
        warnings=tuple(warnings),
        duration_ms=(datetime.now() - start_time).total_seconds() * 1000,
    )


# =============================================================================
# Freshness Metrics
# =============================================================================


def check_freshness(
    records: Sequence[DataRecord],
    timestamp_field: str,
    max_age_hours: float = 24.0,
) -> QualityCheckResult:
    """
    Check data freshness by analyzing timestamps.

    Args:
        records: Records to check
        timestamp_field: Name of the timestamp field to check
        max_age_hours: Maximum acceptable age in hours

    Returns:
        QualityCheckResult with freshness metrics
    """
    start_time = datetime.now()
    metrics: list[QualityMetric] = []
    errors: list[str] = []
    warnings: list[str] = []

    if not records:
        return QualityCheckResult(
            check_name="freshness",
            passed=True,
            metrics=tuple(metrics),
            duration_ms=(datetime.now() - start_time).total_seconds() * 1000,
        )

    now = datetime.now()
    max_age = timedelta(hours=max_age_hours)
    stale_count = 0
    ages: list[timedelta] = []

    for record in records:
        timestamp_value = record.get(timestamp_field)

        if timestamp_value is None:
            warnings.append(f"Record has missing timestamp field: {timestamp_field}")
            continue

        # Parse timestamp
        if isinstance(timestamp_value, datetime):
            timestamp = timestamp_value
        elif isinstance(timestamp_value, str):
            try:
                timestamp = datetime.fromisoformat(timestamp_value.replace("Z", "+00:00"))
            except ValueError:
                errors.append(f"Invalid timestamp format: {timestamp_value}")
                continue
        else:
            errors.append(f"Unsupported timestamp type: {type(timestamp_value)}")
            continue

        age = now - timestamp
        ages.append(age)

        if age > max_age:
            stale_count += 1

    if ages:
        avg_age = sum(ages, timedelta()) / len(ages)
        max_age_observed = max(ages)

        freshness_score = 1.0 - (stale_count / len(records))
        passed = freshness_score >= 0.95  # Default threshold

        metrics.append(
            QualityMetric(
                name="freshness_score",
                metric_type=QualityMetricType.FRESHNESS,
                value=round(freshness_score, 4),
                threshold=0.95,
                passed=passed,
                description="Fraction of records within max age",
            )
        )

        metrics.append(
            QualityMetric(
                name="stale_count",
                metric_type=QualityMetricType.FRESHNESS,
                value=stale_count,
                description="Number of records exceeding max age",
            )
        )

        metrics.append(
            QualityMetric(
                name="avg_age_hours",
                metric_type=QualityMetricType.FRESHNESS,
                value=round(avg_age.total_seconds() / 3600, 2),
                description="Average age of records in hours",
            )
        )

        metrics.append(
            QualityMetric(
                name="max_age_hours",
                metric_type=QualityMetricType.FRESHNESS,
                value=round(max_age_observed.total_seconds() / 3600, 2),
                description="Maximum age of records in hours",
            )
        )

        if not passed:
            errors.append(f"Found {stale_count} stale records (older than {max_age_hours} hours)")

    else:
        # No valid timestamps found
        passed = False
        errors.append("No valid timestamps found in records")

    return QualityCheckResult(
        check_name="freshness",
        passed=passed,
        metrics=tuple(metrics),
        errors=tuple(errors),
        warnings=tuple(warnings),
        duration_ms=(datetime.now() - start_time).total_seconds() * 1000,
    )


# =============================================================================
# Comprehensive Quality Report
# =============================================================================


def generate_quality_report(
    records: Sequence[DataRecord],
    schema: Schema | None = None,
    checks: tuple[str, ...] = ("completeness", "validity"),
    unique_fields: tuple[str, ...] = (),
    timestamp_field: str | None = None,
    max_age_hours: float = 24.0,
) -> DataQualityReport:
    """
    Generate a comprehensive data quality report.

    Args:
        records: Records to analyze
        schema: Optional schema to validate against
        checks: Quality checks to perform (completeness, validity, uniqueness, freshness)
        unique_fields: Fields for uniqueness check
        timestamp_field: Field for freshness check
        max_age_hours: Max age for freshness check

    Returns:
        DataQualityReport with all quality metrics
    """
    start_time = datetime.now()
    check_results: list[QualityCheckResult] = []

    # Run requested checks
    if "completeness" in checks:
        result = check_completeness(records, schema)
        check_results.append(result)

    if "validity" in checks:
        result = check_validity(records, schema)
        check_results.append(result)

    if "uniqueness" in checks:
        result = check_uniqueness(records, unique_fields)
        check_results.append(result)

    if "freshness" in checks and timestamp_field:
        result = check_freshness(records, timestamp_field, max_age_hours)
        check_results.append(result)

    # Extract scores from check results
    completeness_score = 0.0
    validity_score = 0.0

    for result in check_results:
        for metric in result.metrics:
            if metric.name == "completeness_score":
                completeness_score = metric.value
            elif metric.name == "validity_score":
                validity_score = metric.value

    # Count valid/invalid records
    valid_records = 0
    invalid_records = 0

    for result in check_results:
        if result.check_name == "validity":
            for metric in result.metrics:
                if metric.name == "valid_count":
                    valid_records = int(metric.value)
                elif metric.name == "invalid_count":
                    invalid_records = int(metric.value)

    total_records = len(records)

    # Calculate overall score (average of completeness and validity)
    overall_score = (completeness_score + validity_score) / 2 if check_results else 1.0

    return DataQualityReport(
        total_records=total_records,
        valid_records=valid_records if valid_records > 0 else total_records,
        invalid_records=invalid_records,
        completeness_score=round(completeness_score, 4),
        validity_score=round(validity_score, 4),
        overall_score=round(overall_score, 4),
        checks=tuple(check_results),
        timestamp=datetime.now(),
        duration_ms=(datetime.now() - start_time).total_seconds() * 1000,
    )
