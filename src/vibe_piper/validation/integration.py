"""
Integration utilities for validation history.

This module provides utilities for integrating validation history
with the existing validation framework, including conversion
functions and auto-storage hooks.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import datetime

from vibe_piper.validation.history import (
    ValidationCheckRecord,
    ValidationHistoryStore,
    ValidationMetric,
    ValidationRunMetadata,
)
from vibe_piper.validation.suite import SuiteValidationResult

# =============================================================================
# Conversion Functions
# =============================================================================


def suite_result_to_run_metadata(
    result: SuiteValidationResult,
    asset_name: str,
    pipeline_id: str | None = None,
) -> ValidationRunMetadata:
    """
    Convert SuiteValidationResult to ValidationRunMetadata.

    Args:
        result: The suite validation result to convert
        asset_name: Name of the asset being validated
        pipeline_id: Optional pipeline ID

    Returns:
        ValidationRunMetadata instance
    """
    # Determine overall status
    if result.success:
        status = "passed"
    elif result.failed_checks:
        status = "failed"
    else:
        status = "warning"

    return ValidationRunMetadata(
        validation_run_id=str(uuid.uuid4()),
        asset_name=asset_name,
        suite_name=result.context.validation_suite if result.context else "unknown",
        pipeline_id=pipeline_id,
        status=status,
        started_at=result.context.timestamp if result.context else datetime.utcnow(),
        completed_at=datetime.utcnow(),
        duration_ms=result.duration_ms,
        total_checks=result.total_checks,
        passed_checks=result.total_checks - len(result.failed_checks) - len(result.warning_checks),
        failed_checks=len(result.failed_checks),
        warning_checks=len(result.warning_checks),
        total_records=result.total_records,
        error_count=len(result.errors),
        warning_count=len(result.warnings),
    )


def suite_result_to_check_records(
    result: SuiteValidationResult,
    validation_run_id: str,
) -> Sequence[ValidationCheckRecord]:
    """
    Convert SuiteValidationResult check results to ValidationCheckRecord list.

    Args:
        result: The suite validation result to convert
        validation_run_id: ID of the validation run

    Returns:
        Sequence of ValidationCheckRecord instances
    """
    records: list[ValidationCheckRecord] = []

    for check_name, check_result in result.check_results.items():
        # Extract check type from check name if possible
        check_type = check_name

        # Get error message
        error_message = None
        if not check_result.is_valid and check_result.errors:
            error_message = check_result.errors[0]

        records.append(
            ValidationCheckRecord(
                validation_run_id=validation_run_id,
                check_name=check_name,
                check_type=check_type,
                passed=check_result.is_valid,
                error_message=error_message,
                warning_messages=check_result.warnings,
                metrics={},  # TODO: Extract metrics from check_result if available
                column_name=None,  # TODO: Extract column name if available
                duration_ms=0.0,  # Individual check timing not tracked
            )
        )

    return records


def extract_metrics_from_suite_result(
    result: SuiteValidationResult,
    asset_name: str,
    validation_run_id: str,
) -> Sequence[ValidationMetric]:
    """
    Extract quality metrics from SuiteValidationResult.

    Args:
        result: The suite validation result to extract metrics from
        asset_name: Name of the asset
        validation_run_id: ID of the validation run

    Returns:
        Sequence of ValidationMetric instances
    """
    from vibe_piper.types import QualityMetricType

    metrics: list[ValidationMetric] = []

    # Extract pass rate metric
    if result.total_checks > 0:
        pass_rate = (result.total_checks - len(result.failed_checks)) / result.total_checks

        metrics.append(
            ValidationMetric(
                metric_name="pass_rate",
                metric_type=QualityMetricType.UNIQUENESS,
                asset_name=asset_name,
                check_name=None,
                value=pass_rate,
                status="passed" if pass_rate >= 0.95 else "failed",
                threshold=0.95,
            )
        )

    # Extract duration metric
    metrics.append(
        ValidationMetric(
            metric_name="duration_ms",
            metric_type=QualityMetricType.CUSTOM,
            asset_name=asset_name,
            check_name=None,
            value=result.duration_ms,
            status="passed",  # Duration doesn't have pass/fail
            threshold=None,
        )
    )

    # Extract total records metric
    metrics.append(
        ValidationMetric(
            metric_name="total_records",
            metric_type=QualityMetricType.CUSTOM,
            asset_name=asset_name,
            check_name=None,
            value=result.total_records,
            status="passed",
            threshold=None,
        )
    )

    return metrics


# =============================================================================
# Auto-Storage Hook
# =============================================================================


def store_validation_result(
    result: SuiteValidationResult,
    asset_name: str,
    history_store: ValidationHistoryStore,
    pipeline_id: str | None = None,
) -> str:
    """
    Automatically store a validation result in history.

    This is a convenience function that converts and stores
    a SuiteValidationResult in the validation history.

    Args:
        result: The validation suite result to store
        asset_name: Name of the asset being validated
        history_store: Validation history store instance
        pipeline_id: Optional pipeline ID

    Returns:
        The validation_run_id of the stored run
    """
    # Convert to history format
    run_metadata = suite_result_to_run_metadata(result, asset_name, pipeline_id)
    check_records = suite_result_to_check_records(result, run_metadata.validation_run_id)
    metrics = extract_metrics_from_suite_result(result, asset_name, run_metadata.validation_run_id)

    # Store in history
    history_store.save_validation_run(run_metadata)
    history_store.save_check_results(check_records)
    history_store.save_metrics(metrics)

    return run_metadata.validation_run_id


# =============================================================================
# Re-exports
# =============================================================================

__all__ = [
    "suite_result_to_run_metadata",
    "suite_result_to_check_records",
    "extract_metrics_from_suite_result",
    "store_validation_result",
]
