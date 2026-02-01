"""
Tests for AssetGraph ETL pipeline example.

Tests cover:
- Pipeline building and validation
- Data extraction from CSV
- Data transformation logic
- Validation checks
- Output file generation
- Summary statistics
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline import ETLConfig, build_pipeline

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_csv_data(tmp_path: Path) -> Path:
    """Create a sample CSV file for testing."""
    csv_content = """user_id,name,email,phone,country,status,signup_date,last_login,total_orders,total_spent
1,John Smith,john.smith@example.com,+1-555-0101,USA,active,2024-01-15,2024-01-28,5,529.95
2,Jane Doe,jane.doe@example.com,+1-555-0102,USA,active,2024-01-16,2024-01-27,3,239.97
3,Bob Johnson,bob.johnson@example.com,+1-555-0103,USA,inactive,2023-12-01,2024-01-20,1,49.99
"""

    csv_path = tmp_path / "test_users.csv"
    csv_path.write_text(csv_content)
    return csv_path


@pytest.fixture
def tmp_path() -> Path:  # type: ignore[misc]
    """Create a temporary directory for tests."""
    with TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def config(tmp_path: Path, sample_csv_data: Path) -> ETLConfig:
    """Create test configuration."""
    return ETLConfig(input_path=str(sample_csv_data), output_dir=str(tmp_path / "output"))


# =============================================================================
# Configuration Tests
# =============================================================================


def test_etl_config_defaults():
    """Test that ETLConfig has correct defaults."""
    config = ETLConfig()

    assert config.input_path == "data/users.csv"
    assert config.output_dir == "output"
    assert config.min_row_count == 5
    assert config.max_row_count == 1000
    assert config.max_null_proportion == 0.1


def test_etl_config_custom():
    """Test that ETLConfig accepts custom values."""
    config = ETLConfig(
        input_path="custom/input.csv",
        output_dir="custom/output",
        min_row_count=10,
        max_row_count=5000,
    )

    assert config.input_path == "custom/input.csv"
    assert config.output_dir == "custom/output"
    assert config.min_row_count == 10
    assert config.max_row_count == 5000


# =============================================================================
# Pipeline Building Tests
# =============================================================================


def test_build_pipeline_creates_graph(config: ETLConfig):
    """Test that build_pipeline creates a valid AssetGraph."""
    graph = build_pipeline(config)

    assert graph.name == "asset_graph_etl"
    assert len(graph.assets) == 5  # extract, transform, validate, load, summarize

    # Check asset names
    asset_names = {asset.name for asset in graph.assets}
    assert "extract" in asset_names
    assert "transform" in asset_names
    assert "validate" in asset_names
    assert "load" in asset_names
    assert "summarize" in asset_names


def test_pipeline_has_correct_dependencies():
    """Test that pipeline has correct dependency structure."""
    config = ETLConfig(input_path="dummy.csv")
    graph = build_pipeline(config)

    # Expected dependencies
    # extract -> transform -> validate -> load -> summarize
    assert "transform" in graph.dependencies
    assert "extract" in graph.dependencies.get("transform", ())
    assert "validate" in graph.dependencies
    assert "transform" in graph.dependencies.get("validate", ())


# =============================================================================
# Transformation Tests
# =============================================================================


def test_email_normalization(config: ETLConfig, sample_csv_data: Path):
    """Test that email is normalized to lowercase."""
    graph = build_pipeline(config)

    # Execute extract and transform
    from vibe_piper.execution import ExecutionEngine

    engine = ExecutionEngine()
    result = engine.execute(graph)

    assert result.success

    # Get transformed data
    transformed_data = result.get_asset_output("transform")

    # Check email normalization
    for row in transformed_data:
        if row.get("email"):
            assert row["email"] == row["email"].lower().strip()


def test_phone_cleaning(config: ETLConfig, sample_csv_data: Path):
    """Test that phone is cleaned to digits only."""
    graph = build_pipeline(config)

    from vibe_piper.execution import ExecutionEngine

    engine = ExecutionEngine()
    result = engine.execute(graph)

    assert result.success

    transformed_data = result.get_asset_output("transform")

    # Check phone cleaning
    assert transformed_data[0]["phone_clean"] == "15550101"
    assert transformed_data[1]["phone_clean"] == "15550102"


def test_status_normalization(config: ETLConfig, sample_csv_data: Path):
    """Test that status is normalized to lowercase."""
    graph = build_pipeline(config)

    from vibe_piper.execution import ExecutionEngine

    engine = ExecutionEngine()
    result = engine.execute(graph)

    assert result.success

    transformed_data = result.get_asset_output("transform")

    # All statuses should be lowercase
    for row in transformed_data:
        if row.get("status"):
            assert row["status"] in {"active", "inactive", "pending"}


def test_customer_tier_calculation(config: ETLConfig, sample_csv_data: Path):
    """Test that customer tier is calculated correctly."""
    graph = build_pipeline(config)

    from vibe_piper.execution import ExecutionEngine

    engine = ExecutionEngine()
    result = engine.execute(graph)

    assert result.success

    transformed_data = result.get_asset_output("transform")

    # Check tier calculation
    # John: $529.95 -> gold
    assert transformed_data[0]["customer_tier"] == "gold"
    # Jane: $239.97 -> silver
    assert transformed_data[1]["customer_tier"] == "silver"
    # Bob: $49.99 -> bronze
    assert transformed_data[2]["customer_tier"] == "bronze"


# =============================================================================
# Output File Tests
# =============================================================================


def test_output_csv_created(config: ETLConfig, sample_csv_data: Path, tmp_path: Path):
    """Test that output CSV is created."""
    output_dir = tmp_path / "output"

    graph = build_pipeline(config)

    from vibe_piper.execution import ExecutionEngine

    engine = ExecutionEngine()
    result = engine.execute(graph)

    assert result.success

    # Check output file exists
    output_file = output_dir / "users_transformed.csv"
    assert output_file.exists()

    # Check file has content
    content = output_file.read_text()
    assert len(content) > 0
    assert "user_id" in content
    assert "email" in content
    assert "customer_tier" in content


def test_summary_json_created(config: ETLConfig, sample_csv_data: Path, tmp_path: Path):
    """Test that summary JSON is created."""
    output_dir = tmp_path / "output"

    graph = build_pipeline(config)

    from vibe_piper.execution import ExecutionEngine

    engine = ExecutionEngine()
    result = engine.execute(graph)

    assert result.success

    # Check summary file exists
    summary_file = output_dir / "summary.json"
    assert summary_file.exists()

    # Check summary structure
    with open(summary_file) as f:
        summary = json.load(f)

    assert "total_users" in summary
    assert "status_distribution" in summary
    assert "tier_distribution" in summary
    assert "total_revenue" in summary
    assert "total_orders" in summary
    assert "average_order_value" in summary


# =============================================================================
# Summary Statistics Tests
# =============================================================================


def test_summary_statistics(config: ETLConfig, sample_csv_data: Path):
    """Test that summary statistics are calculated correctly."""
    graph = build_pipeline(config)

    from vibe_piper.execution import ExecutionEngine

    engine = ExecutionEngine()
    result = engine.execute(graph)

    assert result.success

    summary = result.get_asset_output("summarize")

    # Check statistics
    assert summary["total_users"] == 3
    assert summary["total_orders"] == 9  # 5 + 3 + 1
    assert summary["total_revenue"] == 819.91  # 529.95 + 239.97 + 49.99
    assert abs(summary["average_order_value"] - 91.10) < 0.01

    # Check status distribution
    assert "active" in summary["status_distribution"]
    assert "inactive" in summary["status_distribution"]

    # Check tier distribution
    assert "gold" in summary["tier_distribution"]
    assert "silver" in summary["tier_distribution"]


def test_summary_includes_key_fields(config: ETLConfig, sample_csv_data: Path):
    """Test that summary includes all expected fields."""
    graph = build_pipeline(config)

    from vibe_piper.execution import ExecutionEngine

    engine = ExecutionEngine()
    result = engine.execute(graph)

    assert result.success

    summary = result.get_asset_output("summarize")

    required_fields = [
        "total_users",
        "status_distribution",
        "tier_distribution",
        "total_revenue",
        "total_orders",
        "average_order_value",
        "output_file",
        "generated_at",
    ]

    for field in required_fields:
        assert field in summary, f"Summary missing field: {field}"


# =============================================================================
# End-to-End Tests
# =============================================================================


def test_pipeline_end_to_end(config: ETLConfig, sample_csv_data: Path, tmp_path: Path):
    """Test complete pipeline execution."""
    output_dir = tmp_path / "output"

    graph = build_pipeline(config)

    from vibe_piper.execution import ExecutionEngine

    engine = ExecutionEngine()
    result = engine.execute(graph)

    # Check success
    assert result.success
    assert result.assets_executed == 5

    # Check output files
    output_file = output_dir / "users_transformed.csv"
    summary_file = output_dir / "summary.json"

    assert output_file.exists()
    assert summary_file.exists()

    # Check output content
    with open(summary_file) as f:
        summary = json.load(f)

    assert summary["total_users"] == 3


def test_pipeline_handles_missing_data(tmp_path: Path):
    """Test that pipeline handles empty CSV gracefully."""
    # Create empty CSV
    csv_path = tmp_path / "empty.csv"
    csv_path.write_text("user_id,name,email\n")
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    config = ETLConfig(input_path=str(csv_path), output_dir=str(tmp_path / "output"))
    graph = build_pipeline(config)

    from vibe_piper.execution import ExecutionEngine

    engine = ExecutionEngine()
    result = engine.execute(graph)

    # Should fail due to row count validation
    assert not result.success
    assert len(result.get_failed_assets()) > 0


# =============================================================================
# Output Field Tests
# =============================================================================


def test_output_contains_required_fields(config: ETLConfig, sample_csv_data: Path):
    """Test that output CSV contains all required fields."""
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    graph = build_pipeline(config)

    from vibe_piper.execution import ExecutionEngine

    engine = ExecutionEngine()
    result = engine.execute(graph)

    assert result.success

    output_file = output_dir / "users_transformed.csv"
    content = output_file.read_text()

    required_fields = [
        "user_id",
        "name",
        "email",
        "phone_clean",
        "country",
        "status",
        "customer_tier",
        "signup_year",
        "signup_month",
        "days_since_login",
        "total_orders",
        "total_spent",
    ]

    for field in required_fields:
        assert field in content, f"Output missing field: {field}"


def test_output_row_count_matches_input(config: ETLConfig, sample_csv_data: Path):
    """Test that output has same number of rows as input."""
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    graph = build_pipeline(config)

    from vibe_piper.execution import ExecutionEngine

    engine = ExecutionEngine()
    result = engine.execute(graph)

    assert result.success

    output_file = output_dir / "users_transformed.csv"

    # Count lines in output CSV (minus header)
    output_lines = output_file.read_text().split("\n")
    output_rows = len([line for line in output_lines if line.strip()]) - 1  # Subtract header

    # Input has 3 data rows
    assert output_rows == 3
