"""
Integration tests for external quality tools.

Tests Great Expectations and Soda integrations with real checks.
"""

from collections.abc import Sequence
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest

from vibe_piper import DataRecord, DataType, Schema, SchemaField
from vibe_piper.types import QualityMetricType

# Try to import external quality tools
try:
    from vibe_piper import ge_asset, soda_asset
    from vibe_piper.external_quality import (
        ToolType,
        create_ge_suite_config,
        create_soda_checks_config,
        display_quality_dashboard,
        format_consistent_error_message,
        generate_unified_report,
        load_ge_suite,
        load_soda_checks,
        merge_quality_results,
        save_ge_suite,
        save_soda_checks,
    )

    EXTERNAL_QUALITY_AVAILABLE = True
except ImportError as e:
    EXTERNAL_QUALITY_AVAILABLE = False
    pytest.skip(f"External quality tools not available: {e}", allow_module_level=True)


@pytest.fixture
def sample_schema():
    """Create a sample schema for testing."""
    return Schema(
        name="test_schema",
        fields=(
            SchemaField(name="id", data_type=DataType.INTEGER),
            SchemaField(name="name", data_type=DataType.STRING),
            SchemaField(name="email", data_type=DataType.STRING),
            SchemaField(name="age", data_type=DataType.INTEGER),
            SchemaField(name="score", data_type=DataType.FLOAT),
        ),
    )


@pytest.fixture
def sample_records(sample_schema):
    """Create sample records for testing."""
    return (
        DataRecord(
            data={"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30, "score": 95.5},
            schema=sample_schema,
        ),
        DataRecord(
            data={"id": 2, "name": "Bob", "email": "bob@example.com", "age": 25, "score": 87.0},
            schema=sample_schema,
        ),
        DataRecord(
            data={
                "id": 3,
                "name": "Charlie",
                "email": "charlie@example.com",
                "age": 35,
                "score": 92.5,
            },
            schema=sample_schema,
        ),
    )


# =============================================================================
# Great Expectations Tests
# =============================================================================


@pytest.mark.skipif(
    not EXTERNAL_QUALITY_AVAILABLE,
    reason="External quality tools not available",
)
class TestGreatExpectations:
    """Tests for Great Expectations integration."""

    def test_create_ge_suite_config(self) -> None:
        """Test creating GE suite configuration."""
        config = create_ge_suite_config(
            expectations=[
                {
                    "name": "row_count_check",
                    "type": "expect_table_row_count_to_be_between",
                    "min": 1,
                    "max": 1000,
                },
                {
                    "name": "email_format",
                    "type": "expect_column_values_to_match_regex",
                    "column": "email",
                    "regex": r"^[\\w\\.-]+@[\\w\\.-]+$",
                },
            ],
            data_asset_name="test_asset",
        )

        assert "expectations" in config
        assert len(config["expectations"]) == 2
        assert config["data_asset_name"] == "test_asset"

    def test_load_and_save_ge_suite(self) -> None:
        """Test loading and saving GE suite."""
        # Create a temporary file
        with NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            config = create_ge_suite_config(
                expectations=[
                    {
                        "name": "row_count_check",
                        "type": "expect_table_row_count_to_be_between",
                        "min": 1,
                        "max": 100,
                    },
                ],
            )

            # Save the config
            save_ge_suite(config, f.name)

            # Load it back
            loaded_config = load_ge_suite(f.name)

            # Verify it matches
            assert loaded_config["expectations"][0]["name"] == "row_count_check"

        # Clean up
        Path(f.name).unlink(missing_ok=True)

    def test_ge_asset_decorator_with_file(self, sample_records, tmp_path) -> None:
        """Test @ge_asset decorator with YAML file."""
        # Create a GE suite file
        config = create_ge_suite_config(
            expectations=[
                {
                    "name": "row_count_check",
                    "type": "expect_table_row_count_to_be_between",
                    "min": 1,
                    "max": 10,
                },
            ],
        )

        suite_path = str(tmp_path / "test_ge_suite.yaml")
        save_ge_suite(config, suite_path)

        # Define asset with decorator
        @ge_asset(suite_path=suite_path, on_failure="ignore")
        def test_asset():
            return sample_records

        # Execute the asset
        result = test_asset()

        # Verify it returns data
        assert len(result) == 3


# =============================================================================
# Soda Tests
# =============================================================================


@pytest.mark.skipif(
    not EXTERNAL_QUALITY_AVAILABLE,
    reason="External quality tools not available",
)
class TestSoda:
    """Tests for Soda integration."""

    def test_create_soda_checks_config(self) -> None:
        """Test creating Soda checks configuration."""
        config = create_soda_checks_config(
            checks=[
                {
                    "name": "row_count",
                    "type": "row_count",
                    "min": 1,
                    "max": 1000,
                },
                {
                    "name": "email_format",
                    "type": "values_in_set",
                    "column": "email",
                    "values": ["gmail.com", "yahoo.com", "outlook.com"],
                },
            ],
            data_source_name="test_source",
        )

        assert "checks" in config
        assert len(config["checks"]) == 2
        assert config["data_source_name"] == "test_source"

    def test_load_and_save_soda_checks(self) -> None:
        """Test loading and saving Soda checks."""
        # Create a temporary file
        with NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            config = create_soda_checks_config(
                checks=[
                    {
                        "name": "row_count",
                        "type": "row_count",
                        "min": 1,
                        "max": 100,
                    },
                ],
            )

            # Save the config
            save_soda_checks(config, f.name)

            # Load it back
            loaded_config = load_soda_checks(f.name)

            # Verify it matches
            assert loaded_config["checks"][0]["name"] == "row_count"

        # Clean up
        Path(f.name).unlink(missing_ok=True)

    def test_soda_asset_decorator_with_file(self, sample_records, tmp_path) -> None:
        """Test @soda_asset decorator with YAML file."""
        # Create a Soda checks file
        config = create_soda_checks_config(
            checks=[
                {
                    "name": "row_count",
                    "type": "row_count",
                    "min": 1,
                    "max": 10,
                },
            ],
        )

        checks_path = str(tmp_path / "test_soda_checks.yaml")
        save_soda_checks(config, checks_path)

        # Define asset with decorator
        @soda_asset(checks_path=checks_path, on_failure="ignore")
        def test_asset():
            return sample_records

        # Execute the asset
        result = test_asset()

        # Verify it returns data
        assert len(result) == 3


# =============================================================================
# Unified Reporting Tests
# =============================================================================


@pytest.mark.skipif(
    not EXTERNAL_QUALITY_AVAILABLE,
    reason="External quality tools not available",
)
class TestUnifiedReporting:
    """Tests for unified quality reporting."""

    def test_merge_quality_results_empty(self) -> None:
        """Test merging empty quality results."""
        result = merge_quality_results([], "test_asset")

        assert result.overall_passed is True
        assert result.quality_score == 1.0
        assert len(result.tool_results) == 0

    def test_merge_quality_results_single(self) -> None:
        """Test merging single quality result."""
        # Create a mock tool result
        from vibe_piper.external_quality.base import QualityToolResult

        tool_result = QualityToolResult(
            tool_type=ToolType.GREAT_EXPECTATIONS,
            asset_name="test_asset",
            passed=True,
            metrics=(
                QualityMetric(
                    name="test_metric",
                    metric_type=QualityMetricType.VALIDITY,
                    value=1.0,
                    passed=True,
                ),
            ),
        )

        result = merge_quality_results((tool_result,), "test_asset")

        assert result.overall_passed is True
        assert result.quality_score == 1.0
        assert len(result.tool_results) == 1

    def test_merge_quality_results_multiple(self) -> None:
        """Test merging multiple quality results."""
        from vibe_piper.external_quality.base import QualityToolResult

        tool_results = (
            QualityToolResult(
                tool_type=ToolType.GREAT_EXPECTATIONS,
                asset_name="test_asset",
                passed=True,
                metrics=(
                    QualityMetric(
                        name="ge_metric",
                        metric_type=QualityMetricType.VALIDITY,
                        value=0.95,
                        passed=True,
                    ),
                ),
            ),
            QualityToolResult(
                tool_type=ToolType.SODA,
                asset_name="test_asset",
                passed=True,
                metrics=(
                    QualityMetric(
                        name="soda_metric",
                        metric_type=QualityMetricType.VALIDITY,
                        value=0.98,
                        passed=True,
                    ),
                ),
            ),
        )

        result = merge_quality_results(tool_results, "test_asset")

        assert result.overall_passed is True
        assert result.quality_score == 0.95  # min(0.95, 0.98)
        assert len(result.tool_results) == 2

    def test_display_quality_dashboard(self) -> None:
        """Test quality dashboard display."""
        from vibe_piper.external_quality.base import QualityToolResult

        tool_results = (
            QualityToolResult(
                tool_type=ToolType.GREAT_EXPECTATIONS,
                asset_name="customers",
                passed=True,
                metrics=(
                    QualityMetric(
                        name="completeness",
                        metric_type=QualityMetricType.COMPLETENESS,
                        value=0.98,
                        passed=True,
                    ),
                ),
            ),
            QualityToolResult(
                tool_type=ToolType.SODA,
                asset_name="customers",
                passed=True,
                metrics=(
                    QualityMetric(
                        name="uniqueness",
                        metric_type=QualityMetricType.UNIQUENESS,
                        value=1.0,
                        passed=True,
                    ),
                ),
            ),
        )

        unified = merge_quality_results(tool_results, "customers")
        report = unified.to_vibe_piper_report()
        dashboard = display_quality_dashboard(report, show_details=True)

        # Verify dashboard contains expected content
        assert "Quality Dashboard: customers" in dashboard
        assert "Overall Status: PASSED" in dashboard
        assert "Great Expectations: PASSED" in dashboard
        assert "Soda: PASSED" in dashboard

    def test_format_consistent_error_message(self) -> None:
        """Test consistent error message formatting."""
        message = format_consistent_error_message(
            ToolType.GREAT_EXPECTATIONS,
            "test_asset",
            ("Email format invalid", "Missing required fields"),
        )

        assert "[Great Expectations Validation Failed]" in message
        assert "Asset: test_asset" in message
        assert "Email format invalid" in message
        assert "Missing required fields" in message
