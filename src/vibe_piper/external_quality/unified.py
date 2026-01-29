"""
Unified quality reporting for external tools.

This module provides functionality to merge results from multiple quality tools
into a single, consistent quality report with a unified dashboard display.
"""

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime

from vibe_piper.external_quality.base import (
    QualityToolResult,
    ToolType,
)
from vibe_piper.types import (
    DataQualityReport,
    QualityCheckResult,
    QualityMetric,
)

# =============================================================================
# Unified Quality Report
# =============================================================================


@dataclass(frozen=True)
class UnifiedQualityReport:
    """
    Unified quality report from multiple tools.

    Attributes:
        asset_name: Name of the asset
        tool_results: Results from each external tool
        overall_passed: Whether all validations passed
        quality_score: Overall quality score (0-1)
        timestamp: When report was generated
        duration_ms: Total time for all validations
    """

    asset_name: str
    tool_results: tuple[QualityToolResult, ...] = field(default_factory=tuple)
    overall_passed: bool = True
    quality_score: float = 1.0
    timestamp: datetime = field(default_factory=datetime.now)
    duration_ms: float = 0.0

    def to_vibe_piper_report(self) -> DataQualityReport:
        """
        Convert to VibePiper DataQualityReport.

        Returns:
            DataQualityReport with merged quality results
        """
        # Collect all metrics from all tools
        all_metrics: list[QualityMetric] = []
        all_errors: list[str] = []
        all_warnings: list[str] = []

        total_duration = self.duration_ms
        total_checks = 0
        passed_checks = 0

        for tool_result in self.tool_results:
            # Convert tool result to VibePiper format
            vp_result = tool_result.to_vibe_piper_result()
            all_metrics.extend(vp_result.metrics)
            all_errors.extend(vp_result.errors)
            all_warnings.extend(vp_result.warnings)
            total_checks += 1
            passed_checks += 1 if tool_result.passed else 0

        # Calculate overall scores
        total_records = 0  # We don't have record count from tool results
        valid_records = passed_checks
        invalid_records = total_checks - passed_checks

        # Calculate completeness, validity scores from metrics
        completeness_score = 1.0
        validity_score = 1.0

        for metric in all_metrics:
            if metric.name.startswith("completeness"):
                completeness_score = metric.value
            elif metric.name.startswith("validity"):
                validity_score = metric.value

        # Overall score is average of completeness and validity
        overall_score = (completeness_score + validity_score) / 2

        return DataQualityReport(
            total_records=total_records,
            valid_records=valid_records,
            invalid_records=invalid_records,
            completeness_score=round(completeness_score, 4),
            validity_score=round(validity_score, 4),
            overall_score=round(overall_score, 4),
            checks=tuple(
                QualityCheckResult(
                    check_name=result.check_name,
                    passed=result.passed,
                    metrics=result.metrics,
                    errors=result.errors,
                    warnings=result.warnings,
                    timestamp=result.timestamp,
                    duration_ms=result.duration_ms,
                )
                for result in [tr.to_vibe_piper_result() for tr in self.tool_results]
            ),
            timestamp=self.timestamp,
            duration_ms=total_duration,
        )


# =============================================================================
# Unified Reporting Functions
# =============================================================================


def merge_quality_results(
    tool_results: Sequence[QualityToolResult],
    asset_name: str = "combined",
) -> UnifiedQualityReport:
    """
    Merge results from multiple quality tools into a unified report.

    Args:
        tool_results: Results from external quality tools
        asset_name: Name of the asset being validated

    Returns:
        UnifiedQualityReport with merged results
    """
    if not tool_results:
        return UnifiedQualityReport(
            asset_name=asset_name,
            overall_passed=True,
            quality_score=1.0,
        )

    # Check if all tools passed
    overall_passed = all(result.passed for result in tool_results)

    # Calculate overall quality score
    # Use minimum score from all tools as conservative estimate
    scores: list[float] = []
    for tool_result in tool_results:
        passed_metrics = [m for m in tool_result.metrics if m.passed]
        if passed_metrics:
            scores.append(min(m.value for m in passed_metrics))

    quality_score = min(scores) if scores else 1.0

    # Calculate total duration
    total_duration = sum(r.duration_ms for r in tool_results)

    return UnifiedQualityReport(
        asset_name=asset_name,
        tool_results=tuple(tool_results),
        overall_passed=overall_passed,
        quality_score=quality_score,
        timestamp=datetime.now(),
        duration_ms=total_duration,
    )


def generate_unified_report(
    asset_name: str,
    tool_results: Sequence[QualityToolResult],
) -> DataQualityReport:
    """
    Generate a unified VibePiper quality report from tool results.

    Args:
        asset_name: Name of the asset
        tool_results: Results from external quality tools

    Returns:
        DataQualityReport in VibePiper format
    """
    unified = merge_quality_results(tool_results, asset_name)
    return unified.to_vibe_piper_report()


def display_quality_dashboard(
    report: DataQualityReport | UnifiedQualityReport,
    show_details: bool = True,
) -> str:
    """
    Generate a formatted quality dashboard display.

    Args:
        report: Quality report to display
        show_details: Whether to show detailed metric information

    Returns:
        Formatted dashboard as string

    Example:
        >>> print(display_quality_dashboard(report))
        ╔══════════════════════════════════════════╗
        ║         Quality Dashboard: customers                  ║
        ╠════════════════════════════════════════════╣
        ║ Overall Status: PASSED                             ║
        ║ Quality Score: 95.5%                              ║
        ║                                                   ║
        ║ Tool Results:                                       ║
        ║ • Great Expectations: PASSED                     ║
        ║   - Completeness: 98.0%                         ║
        ║   - Validity: 95.0%                             ║
        ║ • Soda: PASSED                                     ║
        ║   - Uniqueness: 100.0%                            ║
        ║                                                   ║
        ║ Checks Run: 2                                     ║
        ║ Passed: 2                                          ║
        ║ Failed: 0                                          ║
        ║                                                   ║
        ║ Duration: 125ms                                    ║
        ║ Timestamp: 2026-01-29 20:52:03               ║
        ╚══════════════════════════════════════════════╝
    """
    lines = []
    border_width = 60

    # Header
    lines.append("╔" + "═" * (border_width - 2) + "╗")

    # Handle different report types
    if isinstance(report, UnifiedQualityReport):
        title = f"Quality Dashboard: {report.asset_name}"
    else:
        title = "Quality Dashboard"

    title_line = f"║ {title:^{border_width - 4}} ║"
    lines.append(title_line)
    lines.append("╠" + "═" * (border_width - 2) + "╣")

    # Overall status
    if isinstance(report, UnifiedQualityReport):
        status = "PASSED" if report.overall_passed else "FAILED"
        status_display = f"Overall Status: {status}"
        lines.append(f"║ {status_display:<{border_width - 4}} ║")

        # Quality score
        score_pct = report.quality_score * 100
        score_display = f"Quality Score: {score_pct:.1f}%"
        lines.append(f"║ {score_display:<{border_width - 4}} ║")
        lines.append("║" + " " * (border_width - 4) + "║")

        # Tool results
        lines.append("║ Tool Results:" + " " * (border_width - 14) + "║")

        for tool_result in report.tool_results:
            tool_name = tool_result.tool_type.name.replace("_", " ").title()
            tool_status = "PASSED" if tool_result.passed else "FAILED"
            lines.append(f"║ • {tool_name}: {tool_status} ║")

            # Show metrics if requested
            if show_details and tool_result.metrics:
                for metric in tool_result.metrics:
                    metric_name = metric.name.replace("_", " ").title()
                    metric_value = metric.value

                    if isinstance(metric_value, float):
                        value_display = (
                            f"{metric_value * 100:.2f}%"
                            if metric_value <= 1.0
                            else f"{metric_value:.4f}"
                        )
                    else:
                        value_display = f"{metric_value}"

                    lines.append(f"║   - {metric_name}: {value_display} ║")

        lines.append("║" + " " * (border_width - 4) + "║")

        # Summary stats
        total_checks = len(report.tool_results)
        passed_checks = sum(1 for tr in report.tool_results if tr.passed)
        failed_checks = total_checks - passed_checks

        lines.append("║ Checks Run:" + " " * (border_width - 13) + "║")
        lines.append(f"║ Passed: {passed_checks} ║")
        lines.append(f"║ Failed: {failed_checks} ║")
        lines.append("║" + " " * (border_width - 4) + "║")

        # Duration
        duration_display = f"Duration: {report.duration_ms:.0f}ms"
        lines.append(f"║ {duration_display:<{border_width - 4}} ║")

    elif isinstance(report, DataQualityReport):
        status = "PASSED" if report.overall_score > 0.8 else "FAILED"
        status_display = f"Overall Status: {status}"
        lines.append(f"║ {status_display:<{border_width - 4}} ║")

        # Quality scores
        score_pct = report.overall_score * 100
        score_display = f"Quality Score: {score_pct:.1f}%"
        lines.append(f"║ {score_display:<{border_width - 4}} ║")
        lines.append("║" + " " * (border_width - 4) + "║")

        # Individual scores
        lines.append("║ Scores:" + " " * (border_width - 8) + "║")
        completeness_pct = report.completeness_score * 100
        validity_pct = report.validity_score * 100
        lines.append(f"║ • Completeness: {completeness_pct:.1f}% ║")
        lines.append(f"║ • Validity: {validity_pct:.1f}% ║")
        lines.append("║" + " " * (border_width - 4) + "║")

        # Check results
        if report.checks:
            lines.append("║ Check Results:" + " " * (border_width - 15) + "║")

            for check in report.checks:
                check_name = check.check_name.replace("_", " ").title()
                check_status = "PASSED" if check.passed else "FAILED"
                lines.append(f"║ • {check_name}: {check_status} ║")

                if show_details and check.metrics:
                    for metric in check.metrics:
                        metric_name = metric.name.replace("_", " ").title()

                        if isinstance(metric.value, float):
                            value_display = (
                                f"{metric.value * 100:.2f}%"
                                if metric.value <= 1.0
                                else f"{metric.value:.4f}"
                            )
                        else:
                            value_display = f"{metric.value}"

                        lines.append(f"║   - {metric_name}: {value_display} ║")

            lines.append("║" + " " * (border_width - 4) + "║")

        # Duration
        duration_display = f"Duration: {report.duration_ms:.0f}ms"
        lines.append(f"║ {duration_display:<{border_width - 4}} ║")

    # Footer
    timestamp = report.timestamp.strftime("%Y-%m-%d %H:%M:%S")
    timestamp_display = f"Timestamp: {timestamp}"
    lines.append("║" + " " * (border_width - 4) + "║")
    lines.append(f"║ {timestamp_display:<{border_width - 4}} ║")
    lines.append("╚" + "═" * (border_width - 2) + "╝")

    return "\n".join(lines)


def format_consistent_error_message(
    tool_type: ToolType,
    asset_name: str,
    errors: Sequence[str],
    warnings: Sequence[str] | None = None,
) -> str:
    """
    Format a consistent error message across all tools.

    Args:
        tool_type: Type of quality tool
        asset_name: Name of the asset
        errors: Error messages
        warnings: Optional warning messages

    Returns:
        Formatted error message

    Example:
        >>> msg = format_consistent_error_message(
        ...     ToolType.GREAT_EXPECTATIONS,
        ...     "customers",
        ...     ["Email format invalid", "Missing required fields"],
        ... )
        >>> print(msg)
        [Great Expectations Validation Failed]
        Asset: customers
        Errors:
          - Email format invalid
          - Missing required fields
    """
    lines = []

    tool_name = tool_type.name.replace("_", " ").title()
    lines.append(f"[{tool_name} Validation Failed]")
    lines.append(f"Asset: {asset_name}")

    if errors:
        lines.append("Errors:")
        for error in errors:
            lines.append(f"  - {error}")

    if warnings:
        lines.append("Warnings:")
        for warning in warnings:
            lines.append(f"  - {warning}")

    return "\n".join(lines)
