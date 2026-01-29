"""
External quality tool integrations for Vibe Piper.

This module provides integrations with external data quality tools including:
- Great Expectations
- Soda

These integrations follow an adapter pattern to convert external tool results
to VibePiper's unified quality reporting format.
"""

# Import optional integrations using lazy loading
_ge_available = False
_soda_available = False

# Base adapter
from vibe_piper.external_quality.base import (
    QualityToolAdapter,
    QualityToolResult,
    ToolType,
)

__all__ = [
    "QualityToolAdapter",
    "QualityToolResult",
    "ToolType",
    # Great Expectations (optional)
    "ge_asset",
    "GreatExpectationsAdapter",
    # Soda (optional)
    "soda_asset",
    "SodaAdapter",
    # Unified reporting
    "merge_quality_results",
    "generate_unified_report",
    "display_quality_dashboard",
]

# Try to import Great Expectations
try:
    from vibe_piper.external_quality.great_expectations import (
        GreatExpectationsAdapter,
        ge_asset,
    )

    _ge_available = True
except ImportError:
    ge_asset = None  # type: ignore
    GreatExpectationsAdapter = None  # type: ignore

# Try to import Soda
try:
    from vibe_piper.external_quality.soda import SodaAdapter, soda_asset

    _soda_available = True
except ImportError:
    soda_asset = None  # type: ignore
    SodaAdapter = None  # type: ignore

# Import unified reporting (always available)
from vibe_piper.external_quality.unified import (
    display_quality_dashboard,
    generate_unified_report,
    merge_quality_results,
)
