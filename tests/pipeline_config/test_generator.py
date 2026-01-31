"""Tests for pipeline configuration generator."""

import pytest

from vibe_piper.pipeline_config.generator import (
    PipelineGeneratorError,
    _compute_field,
    _compute_value,
    _evaluate_condition,
    _extract_fields,
    _filter_rows,
    _generate_sink_function,
    _generate_source_function,
    _generate_transform_function,
    _get_nested_value,
    generate_asset_dependencies,
    generate_expectations,
)
from vibe_piper.pipeline_config.schema import (
    ExpectationCheck,
    ExpectationConfig,
    PipelineConfig,
    PipelineMetadata,
    SinkConfig,
    SinkType,
    SourceConfig,
    SourceType,
    TransformConfig,
    TransformStep,
    TransformType,
)


class TestGenerateSourceFunction:
    """Tests for generating source asset functions."""

    def test_generate_source_function_returns_callable(self) -> None:
        """Test that source generation returns a callable function."""
        config = SourceConfig(
            name="users_api",
            type=SourceType.API,
            endpoint="/users",
        )

        func = _generate_source_function("users_api", config)

        assert callable(func)
        assert func.__name__ == "users_api"

    def test_source_function_raises_not_implemented(self) -> None:
        """Test that source function raises NotImplementedError."""
        config = SourceConfig(
            name="users_api",
            type=SourceType.API,
            endpoint="/users",
        )

        func = _generate_source_function("users_api", config)

        with pytest.raises(
            NotImplementedError,
            match="Source 'users_api' implementation not yet generated",
        ):
            func(None)

    def test_source_function_has_config_metadata(self) -> None:
        """Test that source function has config metadata attached."""
        config = SourceConfig(
            name="users_api",
            type=SourceType.API,
            endpoint="/users",
            description="Fetch users from API",
        )

        func = _generate_source_function("users_api", config)

        assert hasattr(func, "_config_type")
        assert func._config_type == "source"
        assert hasattr(func, "_config")
        assert func._config is config


class TestGenerateTransformFunction:
    """Tests for generating transform asset functions."""

    def test_generate_transform_function_returns_callable(self) -> None:
        """Test that transform generation returns a callable function."""
        config = TransformConfig(
            name="clean_data",
            source="raw_data",
            steps=[TransformStep(type=TransformType.FILTER, condition="email is not null")],
        )

        func = _generate_transform_function("clean_data", config)

        assert callable(func)
        assert func.__name__ == "clean_data"

    def test_transform_function_needs_source_data(self) -> None:
        """Test that transform function requires source data."""
        config = TransformConfig(
            name="clean_data",
            source="raw_data",
            steps=[],
        )

        func = _generate_transform_function("clean_data", config)

        with pytest.raises(ValueError, match="Transform 'clean_data' depends on source"):
            func(None)

    def test_transform_function_has_dependency_metadata(self) -> None:
        """Test that transform function has dependency metadata."""
        config = TransformConfig(
            name="clean_data",
            source="raw_data",
            steps=[],
        )

        func = _generate_transform_function("clean_data", config)

        assert hasattr(func, "_depends_on")
        assert func._depends_on == ["raw_data"]


class TestGenerateSinkFunction:
    """Tests for generating sink asset functions."""

    def test_generate_sink_function_returns_callable(self) -> None:
        """Test that sink generation returns a callable function."""
        config = SinkConfig(
            name="users_db",
            type=SinkType.DATABASE,
            connection="postgres://localhost/test",
            table="users",
        )

        func = _generate_sink_function("users_db", config)

        assert callable(func)
        assert func.__name__ == "users_db"

    def test_sink_function_raises_not_implemented(self) -> None:
        """Test that sink function raises NotImplementedError."""
        config = SinkConfig(
            name="users_db",
            type=SinkType.DATABASE,
            connection="postgres://localhost/test",
            table="users",
        )

        func = _generate_sink_function("users_db", config)

        with pytest.raises(
            NotImplementedError,
            match="Sink 'users_db' implementation not yet generated",
        ):
            func(None)


class TestTransformSteps:
    """Tests for transform step execution."""

    def test_extract_fields_simple_mapping(self) -> None:
        """Test extracting fields with simple mapping."""
        step = TransformStep(
            type=TransformType.EXTRACT_FIELDS,
            mappings={"company_name": "company.name", "city": "address.city"},
        )

        data = [{"id": 1, "company": {"name": "Acme"}, "address": {"city": "SF"}}]

        result = _extract_fields(step, data)

        assert result[0]["company_name"] == "Acme"
        assert result[0]["city"] == "SF"
        assert "id" in result[0]  # Other fields preserved

    def test_extract_fields_nested_dict(self) -> None:
        """Test extracting fields from single dict."""
        step = TransformStep(
            type=TransformType.EXTRACT_FIELDS,
            mappings={"company_name": "company.name"},
        )

        data = {"id": 1, "company": {"name": "Acme"}}

        result = _extract_fields(step, data)

        assert result["company_name"] == "Acme"
        assert "id" in result  # Other fields preserved

    def test_extract_fields_no_duplicate_when_target_matches_source_key(self) -> None:
        """Test that mapping doesn't create duplicate fields when target name matches a source key."""
        step = TransformStep(
            type=TransformType.EXTRACT_FIELDS,
            mappings={"company": "company.name"},  # Map target "company" to source "company.name"
        )

        data = [{"id": 1, "company": {"name": "Acme"}}]

        result = _extract_fields(step, data)

        # The nested value should be extracted, not duplicated by copy loop
        assert result[0]["company"] == "Acme"  # Extracted value
        assert "id" in result[0]  # Other fields preserved

    def test_extract_fields_all_unmapped_fields_preserved(self) -> None:
        """Test that all unmapped top-level fields are preserved correctly."""
        step = TransformStep(
            type=TransformType.EXTRACT_FIELDS,
            mappings={"company_name": "company.name"},
        )

        data = [
            {
                "id": 1,
                "company": {"name": "Acme"},
                "email": "test@example.com",
                "status": "active",
            }
        ]

        result = _extract_fields(step, data)

        assert result[0]["company_name"] == "Acme"  # Mapped field
        assert result[0]["id"] == 1  # Unmapped field preserved
        assert result[0]["email"] == "test@example.com"  # Unmapped field preserved
        assert result[0]["status"] == "active"  # Unmapped field preserved

    def test_filter_rows_not_null(self) -> None:
        """Test filtering rows with 'is not null' condition."""
        step = TransformStep(type=TransformType.FILTER, condition="email is not null")

        data = [
            {"id": 1, "email": "test@example.com"},
            {"id": 2, "email": None},
            {"id": 3, "email": "valid@test.com"},
        ]

        result = _filter_rows(step, data)

        assert len(result) == 2
        assert all(row["email"] is not None for row in result)

    def test_filter_rows_contains(self) -> None:
        """Test filtering rows with 'contains' condition."""
        step = TransformStep(type=TransformType.FILTER, condition='status contains "active"')

        data = [
            {"id": 1, "status": "active"},
            {"id": 2, "status": "inactive"},
            {"id": 3, "status": "active"},
        ]

        result = _filter_rows(step, data)

        assert len(result) == 2
        assert all(row["status"] == "active" for row in result)

    def test_compute_field_adds_new_field(self) -> None:
        """Test computing a new field."""
        step = TransformStep(type=TransformType.COMPUTE_FIELD, field="computed", value="constant")

        data = [{"id": 1, "name": "John"}]

        result = _compute_field(step, data)

        assert result[0]["computed"] == "constant"
        assert "id" in result[0]
        assert "name" in result[0]

    def test_compute_field_now_function(self) -> None:
        """Test computing field with now() function."""
        step = TransformStep(type=TransformType.COMPUTE_FIELD, field="timestamp", value="now()")

        data = [{"id": 1}]

        result = _compute_field(step, data)

        assert "timestamp" in result[0]
        # Should be ISO format datetime string
        assert isinstance(result[0]["timestamp"], str)


class TestHelperFunctions:
    """Tests for helper functions."""

    def test_get_nested_value_simple(self) -> None:
        """Test getting nested value with simple path."""
        data = {"company": {"name": "Acme"}}

        result = _get_nested_value(data, "company.name")

        assert result == "Acme"

    def test_get_nested_value_deep_nesting(self) -> None:
        """Test getting value from deeply nested structure."""
        data = {"level1": {"level2": {"level3": "value"}}}

        result = _get_nested_value(data, "level1.level2.level3")

        assert result == "value"

    def test_get_nested_value_missing_key(self) -> None:
        """Test that missing key returns None."""
        data = {"company": {"name": "Acme"}}

        result = _get_nested_value(data, "company.address")

        assert result is None

    def test_evaluate_condition_is_not_null(self) -> None:
        """Test evaluating 'is not null' condition."""
        record1 = {"email": "test@example.com"}
        record2 = {"email": None}

        assert _evaluate_condition("email is not null", record1) is True
        assert _evaluate_condition("email is not null", record2) is False

    def test_evaluate_condition_contains(self) -> None:
        """Test evaluating 'contains' condition."""
        record1 = {"status": "active"}
        record2 = {"status": "inactive"}

        assert _evaluate_condition('status contains "active"', record1) is True
        assert _evaluate_condition('status contains "active"', record2) is False

    def test_compute_value_now_function(self) -> None:
        """Test computing value with now() function."""
        record = {"id": 1}

        result = _compute_value("now()", record)

        assert isinstance(result, str)
        # Should be ISO format datetime
        assert "T" in result or "-" in result

    def test_compute_value_field_reference(self) -> None:
        """Test computing value with field reference."""
        record = {"id": 1, "name": "John"}

        result = _compute_value("name", record)

        assert result == "John"


class TestGenerateExpectations:
    """Tests for generating expectations from config."""

    def test_generate_expectations_by_asset(self, sample_config: PipelineConfig) -> None:
        """Test generating expectations grouped by asset."""
        expectations = generate_expectations(sample_config)

        assert "users" in expectations
        assert len(expectations["users"]) == 2


class TestGenerateAssetDependencies:
    """Tests for generating asset dependencies."""

    def test_generate_dependencies(self, sample_config: PipelineConfig) -> None:
        """Test generating dependency mapping."""
        deps = generate_asset_dependencies(sample_config)

        # Sources have no dependencies
        assert deps["users_api"] == []

        # Transforms depend on source
        assert deps["clean_data"] == ["users_api"]


@pytest.fixture
def sample_config() -> PipelineConfig:
    """Fixture providing a sample pipeline configuration."""
    return PipelineConfig(
        pipeline=PipelineMetadata(name="test", version="1.0.0"),
        sources={
            "users_api": SourceConfig(
                name="users_api",
                type=SourceType.API,
                endpoint="/users",
            )
        },
        sinks={
            "users_db": SinkConfig(
                name="users_db",
                type=SinkType.DATABASE,
                connection="postgres://localhost/test",
                table="users",
            )
        },
        transforms={
            "clean_data": TransformConfig(
                name="clean_data",
                source="users_api",
                steps=[
                    TransformStep(
                        type=TransformType.FILTER,
                        condition="email is not null",
                    )
                ],
            )
        },
        expectations={
            "email_valid": ExpectationConfig(
                name="email_valid",
                asset="users",
                checks=[ExpectationCheck(type=CheckType.NOT_NULL, column="email")],
            )
        },
        jobs={},
    )
