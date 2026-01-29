"""
Base adapter pattern for external quality tools.

This module defines the interface and types for quality tool adapters.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any

from vibe_piper.types import (
    DataRecord,
    QualityCheckResult,
    QualityMetric,
)


class ToolType(Enum):
    """Types of external quality tools."""

    GREAT_EXPECTATIONS = auto()
    SODA = auto()
    CUSTOM = auto()


@dataclass(frozen=True)
class QualityToolResult:
    """
    Result from an external quality tool execution.

    Attributes:
        tool_type: Which tool generated this result
        asset_name: Name of the asset that was validated
        passed: Whether validation passed
        metrics: Quality metrics collected
        errors: Error messages from validation
        warnings: Warning messages from validation
        raw_result: Original result from the external tool
        duration_ms: Time taken to execute validation
        timestamp: When validation was performed
    """

    tool_type: ToolType
    asset_name: str
    passed: bool
    metrics: tuple[QualityMetric, ...] = field(default_factory=tuple)
    errors: tuple[str, ...] = field(default_factory=tuple)
    warnings: tuple[str, ...] = field(default_factory=tuple)
    raw_result: Any = None
    duration_ms: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)

    def to_vibe_piper_result(self) -> QualityCheckResult:
        """
        Convert to VibePiper QualityCheckResult.

        Returns:
            QualityCheckResult in VibePiper format
        """
        return QualityCheckResult(
            check_name=f"{self.tool_type.name.lower()}_{self.asset_name}",
            passed=self.passed,
            metrics=self.metrics,
            errors=self.errors,
            warnings=self.warnings,
            timestamp=self.timestamp,
            duration_ms=self.duration_ms,
        )


class QualityToolAdapter(ABC):
    """
    Abstract base class for quality tool adapters.

    Adapters convert external tool results to VibePiper's unified format.
    Subclasses must implement the validate method.
    """

    @abstractmethod
    def validate(
        self,
        data: Sequence[DataRecord],
        config_path: str,
    ) -> QualityToolResult:
        """
        Validate data against the external tool's configuration.

        Args:
            data: Records to validate
            config_path: Path to YAML configuration file

        Returns:
            QualityToolResult with validation outcome
        """
        ...

    @abstractmethod
    def load_config(self, config_path: str) -> dict[str, Any]:
        """
        Load configuration from YAML file.

        Args:
            config_path: Path to YAML configuration file

        Returns:
            Parsed configuration as dictionary
        """
        ...

    def _calculate_score(self, metrics: tuple[QualityMetric, ...]) -> float:
        """
        Calculate overall score from metrics.

        Args:
            metrics: Quality metrics to score

        Returns:
            Overall score (0-1)
        """
        if not metrics:
            return 1.0

        # Average of all metrics with passed status
        scored_metrics = [m for m in metrics if m.passed is not None]
        if not scored_metrics:
            return 1.0

        return sum(m.value for m in scored_metrics if m.passed) / len(scored_metrics)  # type: ignore


# Type alias for decorator functions
AssetFunction = Callable[[], Sequence[DataRecord]]
