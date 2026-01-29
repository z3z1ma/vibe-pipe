"""
Tests for data cleaning transformation utilities.
"""

from datetime import datetime

import pytest

from vibe_piper.transformations.cleaning import (
    CleaningConfig,
    CleaningReport,
    NullStrategy,
    OutlierAction,
    OutlierMethod,
    cap_outliers,
    clean_data,
    clean_dataset,
    clean_text,
    convert_column_type,
    drop_nulls,
    fill_nulls,
    find_duplicates,
    get_data_profile,
    get_null_counts,
    get_value_counts,
    handle_nulls,
    handle_outliers,
    normalize_case,
    normalize_minmax,
    normalize_types,
    normalize_zscore,
    remove_duplicates,
    remove_special_chars,
    standardize_columns,
    summarize_report,
    trim_whitespace,
)
from vibe_piper.types import DataRecord, DataType, Schema, SchemaField


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_schema() -> Schema:
    """Create a sample schema for testing."""
    return Schema(
        name="test_schema",
        fields=(
            SchemaField(name="id", data_type=DataType.INTEGER),
            SchemaField(name="name", data_type=DataType.STRING, nullable=True),
            SchemaField(name="age", data_type=DataType.INTEGER, nullable=True),
            SchemaField(name="email", data_type=DataType.STRING, nullable=True),
            SchemaField(name="score", data_type=DataType.FLOAT, nullable=True),
            SchemaField(name="active", data_type=DataType.BOOLEAN),
        ),
    )


@pytest.fixture
def sample_data(sample_schema: Schema) -> list[DataRecord]:
    """Create sample data for testing."""
    return [
        DataRecord(
            data={
                "id": 1,
                "name": "John Doe",
                "age": 25,
                "email": "john@example.com",
                "score": 85.5,
                "active": True,
            },
            schema=sample_schema,
        ),
        DataRecord(
            data={
                "id": 2,
                "name": "Jane Smith",
                "age": 30,
                "email": "jane@example.com",
                "score": 92.0,
                "active": False,
            },
            schema=sample_schema,
        ),
        DataRecord(
            data={
                "id": 3,
                "name": "Bob Johnson",
                "age": 35,
                "email": "bob@example.com",
                "score": 78.5,
                "active": True,
            },
            schema=sample_schema,
        ),
        DataRecord(
            data={
                "id": 4,
                "name": "Alice Brown",
                "age": 28,
                "email": "alice@example.com",
                "score": 95.0,
                "active": True,
            },
            schema=sample_schema,
        ),
        DataRecord(
            data={
                "id": 5,
                "name": "Charlie Wilson",
                "age": 42,
                "email": "charlie@example.com",
                "score": 88.0,
                "active": False,
            },
            schema=sample_schema,
        ),
    ]


@pytest.fixture
def data_with_nulls(sample_schema: Schema) -> list[DataRecord]:
    """Create data with null values for testing."""
    return [
        DataRecord(
            data={
                "id": 1,
                "name": "John Doe",
                "age": 25,
                "email": "john@example.com",
                "score": 85.5,
                "active": True,
            },
            schema=sample_schema,
        ),
        DataRecord(
            data={
                "id": 2,
                "name": None,
                "age": 30,
                "email": "jane@example.com",
                "score": 92.0,
                "active": False,
            },
            schema=sample_schema,
        ),
        DataRecord(
            data={
                "id": 3,
                "name": "Bob Johnson",
                "age": None,
                "email": "bob@example.com",
                "score": None,
                "active": True,
            },
            schema=sample_schema,
        ),
        DataRecord(
            data={
                "id": 4,
                "name": "Alice Brown",
                "age": 28,
                "email": None,
                "score": 95.0,
                "active": True,
            },
            schema=sample_schema,
        ),
    ]


@pytest.fixture
def data_with_duplicates(sample_schema: Schema) -> list[DataRecord]:
    """Create data with duplicates for testing."""
    return [
        DataRecord(
            data={
                "id": 1,
                "name": "John Doe",
                "age": 25,
                "email": "john@example.com",
                "score": 85.5,
                "active": True,
            },
            schema=sample_schema,
        ),
        DataRecord(
            data={
                "id": 2,
                "name": "Jane Smith",
                "age": 30,
                "email": "jane@example.com",
                "score": 92.0,
                "active": False,
            },
            schema=sample_schema,
        ),
        DataRecord(
            data={
                "id": 1,
                "name": "John Doe",
                "age": 25,
                "email": "john@example.com",
                "score": 85.5,
                "active": True,
            },
            schema=sample_schema,
        ),
        DataRecord(
            data={
                "id": 3,
                "name": "Bob Johnson",
                "age": 35,
                "email": "bob@example.com",
                "score": 78.5,
                "active": True,
            },
            schema=sample_schema,
        ),
        DataRecord(
            data={
                "id": 2,
                "name": "Jane Smith",
                "age": 30,
                "email": "jane@example.com",
                "score": 92.0,
                "active": False,
            },
            schema=sample_schema,
        ),
    ]


@pytest.fixture
def data_with_outliers(sample_schema: Schema) -> list[DataRecord]:
    """Create data with outliers for testing."""
    return [
        DataRecord(
            data={
                "id": 1,
                "name": "John",
                "age": 25,
                "email": "john@example.com",
                "score": 85.5,
                "active": True,
            },
            schema=sample_schema,
        ),
        DataRecord(
            data={
                "id": 2,
                "name": "Jane",
                "age": 30,
                "email": "jane@example.com",
                "score": 92.0,
                "active": False,
            },
            schema=sample_schema,
        ),
        DataRecord(
            data={
                "id": 3,
                "name": "Bob",
                "age": 35,
                "email": "bob@example.com",
                "score": 78.5,
                "active": True,
            },
            schema=sample_schema,
        ),
        DataRecord(
            data={
                "id": 4,
                "name": "Alice",
                "age": 200,
                "email": "alice@example.com",
                "score": 500.0,
                "active": True,
            },
            schema=sample_schema,
        ),
        DataRecord(
            data={
                "id": 5,
                "name": "Charlie",
                "age": 42,
                "email": "charlie@example.com",
                "score": 88.0,
                "active": False,
            },
            schema=sample_schema,
        ),
    ]


@pytest.fixture
def data_with_text_issues(sample_schema: Schema) -> list[DataRecord]:
    """Create data with text issues for testing."""
    return [
        DataRecord(
            data={
                "id": 1,
                "name": "  John Doe  ",
                "age": 25,
                "email": "JOHN@EXAMPLE.COM",
                "score": 85.5,
                "active": True,
            },
            schema=sample_schema,
        ),
        DataRecord(
            data={
                "id": 2,
                "name": "\tJane Smith\t",
                "age": 30,
                "email": "JANE@EXAMPLE.COM",
                "score": 92.0,
                "active": False,
            },
            schema=sample_schema,
        ),
        DataRecord(
            data={
                "id": 3,
                "name": "Bob Johnson",
                "age": 35,
                "email": "bob@example.com",
                "score": 78.5,
                "active": True,
            },
            schema=sample_schema,
        ),
    ]


# =============================================================================
# Configuration Tests
# =============================================================================


class TestCleaningConfig:
    """Tests for CleaningConfig class."""

    def test_default_config(self) -> None:
        """Test creating default cleaning configuration."""
        config = CleaningConfig()
        assert config.null_strategy == NullStrategy.KEEP
        assert config.dedup_columns is None
        assert config.null_columns is None
        assert config.trim_whitespace is True
        assert config.normalize_text is False
        assert config.generate_report is True

    def test_custom_config(self) -> None:
        """Test creating custom cleaning configuration."""
        config = CleaningConfig(
            dedup_columns=("email",),
            null_strategy=NullStrategy.FILL_MEAN,
            trim_whitespace=False,
            normalize_text=True,
        )
        assert config.dedup_columns == ("email",)
        assert config.null_strategy == NullStrategy.FILL_MEAN
        assert config.trim_whitespace is False
        assert config.normalize_text is True


# =============================================================================
# CleaningReport Tests
# =============================================================================


class TestCleaningReport:
    """Tests for CleaningReport class."""

    def test_report_creation(self) -> None:
        """Test creating a cleaning report."""
        report = CleaningReport(
            original_count=100,
            final_count=95,
            duplicates_removed=3,
            nulls_filled=5,
            outliers_handled=2,
        )
        assert report.original_count == 100
        assert report.final_count == 95
        assert report.duplicates_removed == 3
        assert report.nulls_filled == 5
        assert report.outliers_handled == 2

    def test_records_removed_property(self) -> None:
        """Test records_removed property."""
        report = CleaningReport(original_count=100, final_count=90)
        assert report.records_removed == 10

    def test_to_dict(self) -> None:
        """Test converting report to dictionary."""
        report = CleaningReport(
            original_count=100,
            final_count=95,
            operations=("deduplication", "null_handling"),
        )
        report_dict = report.to_dict()
        assert report_dict["original_count"] == 100
        assert report_dict["final_count"] == 95
        assert "deduplication" in report_dict["operations"]
        assert "null_handling" in report_dict["operations"]
        assert isinstance(report_dict["timestamp"], str)


# =============================================================================
# Decorator Tests
# =============================================================================


class TestCleanDataDecorator:
    """Tests for @clean_data decorator."""

    def test_decorator_basic(self) -> None:
        """Test basic decorator usage."""

        @clean_data()
        def load_data() -> list[DataRecord]:
            schema = Schema(
                name="test", fields=(SchemaField(name="id", data_type=DataType.INTEGER),)
            )
            return [
                DataRecord(data={"id": 1}, schema=schema),
                DataRecord(data={"id": 2}, schema=schema),
                DataRecord(data={"id": 1}, schema=schema),
            ]

        cleaned, report = load_data()
        assert len(cleaned) == 2  # Duplicate removed
        assert report.duplicates_removed == 1

    def test_decorator_with_config(self) -> None:
        """Test decorator with custom configuration."""
        config = CleaningConfig(null_strategy=NullStrategy.FILL_DEFAULT, null_fill_value=0)

        @clean_data(config=config)
        def load_data() -> list[DataRecord]:
            schema = Schema(
                name="test",
                fields=(
                    SchemaField(name="id", data_type=DataType.INTEGER),
                    SchemaField(name="value", data_type=DataType.INTEGER),
                ),
            )
            return [
                DataRecord(data={"id": 1, "value": 10}, schema=schema),
                DataRecord(data={"id": 2, "value": None}, schema=schema),
                DataRecord(data={"id": 3, "value": 30}, schema=schema),
            ]

        cleaned, report = load_data()
        assert len(cleaned) == 3
        assert report.nulls_filled == 1
        assert cleaned[1].data["value"] == 0

    def test_decorator_non_list_return(self) -> None:
        """Test decorator raises error for non-list return."""

        @clean_data()
        def load_data() -> DataRecord:
            schema = Schema(
                name="test", fields=(SchemaField(name="id", data_type=DataType.INTEGER),)
            )
            return DataRecord(data={"id": 1}, schema=schema)

        with pytest.raises(TypeError):
            load_data()


# =============================================================================
# Main Cleaning Function Tests
# =============================================================================


class TestCleanDataset:
    """Tests for clean_dataset function."""

    def test_clean_dataset_default(self, sample_data: list[DataRecord]) -> None:
        """Test clean_dataset with default config."""
        cleaned, report = clean_dataset(sample_data)
        assert len(cleaned) == len(sample_data)
        assert report.original_count == len(sample_data)
        assert report.final_count == len(cleaned)

    def test_clean_dataset_with_nulls(self, data_with_nulls: list[DataRecord]) -> None:
        """Test clean_dataset with null handling."""
        config = CleaningConfig(null_strategy=NullStrategy.FILL_DEFAULT, null_fill_value="N/A")
        cleaned, report = clean_dataset(data_with_nulls, config)
        assert report.nulls_filled > 0
        assert report.operations

    def test_clean_dataset_with_duplicates(self, data_with_duplicates: list[DataRecord]) -> None:
        """Test clean_dataset with deduplication."""
        config = CleaningConfig(dedup_columns=("id",))
        cleaned, report = clean_dataset(data_with_duplicates, config)
        assert report.duplicates_removed > 0
        assert len(cleaned) < len(data_with_duplicates)

    def test_clean_dataset_empty(self) -> None:
        """Test clean_dataset with empty data."""
        cleaned, report = clean_dataset([])
        assert len(cleaned) == 0
        assert report.original_count == 0
        assert report.final_count == 0


# =============================================================================
# Deduplication Tests
# =============================================================================


class TestRemoveDuplicates:
    """Tests for remove_duplicates function."""

    def test_remove_duplicates_all_columns(self, data_with_duplicates: list[DataRecord]) -> None:
        """Test removing duplicates using all columns."""
        cleaned, report = remove_duplicates(data_with_duplicates)
        assert len(cleaned) == 3  # 5 - 2 duplicates
        assert report["removed_count"] == 2

    def test_remove_duplicates_specific_columns(
        self, data_with_duplicates: list[DataRecord]
    ) -> None:
        """Test removing duplicates using specific columns."""
        cleaned, report = remove_duplicates(data_with_duplicates, columns=("id",))
        assert len(cleaned) == 3
        assert report["columns"] == ["id"]

    def test_remove_duplicates_keep_first(self, data_with_duplicates: list[DataRecord]) -> None:
        """Test keeping first occurrence."""
        cleaned, report = remove_duplicates(data_with_duplicates, keep="first")
        assert len(cleaned) == 3
        assert cleaned[0].data["id"] == 1

    def test_remove_duplicates_keep_last(self, data_with_duplicates: list[DataRecord]) -> None:
        """Test keeping last occurrence."""
        cleaned, report = remove_duplicates(data_with_duplicates, keep="last")
        assert len(cleaned) == 3

    def test_remove_duplicates_empty(self) -> None:
        """Test removing duplicates from empty data."""
        cleaned, report = remove_duplicates([])
        assert len(cleaned) == 0
        assert report["removed_count"] == 0


class TestFindDuplicates:
    """Tests for find_duplicates function."""

    def test_find_duplicates(self, data_with_duplicates: list[DataRecord]) -> None:
        """Test finding duplicate indices."""
        duplicates = find_duplicates(data_with_duplicates)
        assert len(duplicates) == 2
        assert 2 in duplicates  # Third record is duplicate
        assert 4 in duplicates  # Fifth record is duplicate

    def test_find_duplicates_specific_columns(self, data_with_duplicates: list[DataRecord]) -> None:
        """Test finding duplicates by specific columns."""
        duplicates = find_duplicates(data_with_duplicates, columns=("id",))
        assert len(duplicates) == 2

    def test_find_duplicates_no_duplicates(self, sample_data: list[DataRecord]) -> None:
        """Test finding duplicates when none exist."""
        duplicates = find_duplicates(sample_data)
        assert len(duplicates) == 0

    def test_find_duplicates_empty(self) -> None:
        """Test finding duplicates in empty data."""
        duplicates = find_duplicates([])
        assert len(duplicates) == 0


# =============================================================================
# Null Handling Tests
# =============================================================================


class TestHandleNulls:
    """Tests for handle_nulls function."""

    def test_handle_nulls_drop(self, data_with_nulls: list[DataRecord]) -> None:
        """Test dropping null values."""
        cleaned, report = handle_nulls(data_with_nulls, NullStrategy.DROP)
        assert len(cleaned) < len(data_with_nulls)
        assert report["strategy"] == "DROP"

    def test_handle_nulls_fill_default(self, data_with_nulls: list[DataRecord]) -> None:
        """Test filling with default value."""
        cleaned, report = handle_nulls(data_with_nulls, NullStrategy.FILL_DEFAULT, fill_value="N/A")
        assert len(cleaned) == len(data_with_nulls)
        assert report["filled_count"] > 0
        assert cleaned[1].data["name"] == "N/A"

    def test_handle_nulls_fill_mean(self, data_with_nulls: list[DataRecord]) -> None:
        """Test filling with mean."""
        cleaned, report = handle_nulls(
            data_with_nulls, NullStrategy.FILL_MEAN, columns=("age", "score")
        )
        assert len(cleaned) == len(data_with_nulls)
        assert report["filled_count"] > 0

    def test_handle_nulls_fill_median(self, data_with_nulls: list[DataRecord]) -> None:
        """Test filling with median."""
        cleaned, report = handle_nulls(data_with_nulls, NullStrategy.FILL_MEDIAN, columns=("age",))
        assert len(cleaned) == len(data_with_nulls)

    def test_handle_nulls_fill_mode(self, data_with_nulls: list[DataRecord]) -> None:
        """Test filling with mode."""
        cleaned, report = handle_nulls(data_with_nulls, NullStrategy.FILL_MODE, columns=("active",))
        assert len(cleaned) == len(data_with_nulls)

    def test_handle_nulls_keep(self, data_with_nulls: list[DataRecord]) -> None:
        """Test keeping null values."""
        cleaned, report = handle_nulls(data_with_nulls, NullStrategy.KEEP)
        assert len(cleaned) == len(data_with_nulls)
        assert report["filled_count"] == 0

    def test_handle_nulls_empty(self) -> None:
        """Test handling nulls in empty data."""
        cleaned, report = handle_nulls([], NullStrategy.FILL_DEFAULT, fill_value=0)
        assert len(cleaned) == 0
        assert report["filled_count"] == 0

    def test_handle_nulls_specific_columns(self, data_with_nulls: list[DataRecord]) -> None:
        """Test handling nulls in specific columns."""
        cleaned, report = handle_nulls(
            data_with_nulls,
            NullStrategy.FILL_DEFAULT,
            fill_value="unknown",
            columns=("name", "email"),
        )
        assert len(cleaned) == len(data_with_nulls)
        assert cleaned[1].data["name"] == "unknown"
        assert cleaned[3].data["email"] == "unknown"


class TestDropNulls:
    """Tests for drop_nulls function."""

    def test_drop_nulls_all_columns(self, data_with_nulls: list[DataRecord]) -> None:
        """Test dropping nulls from all columns."""
        cleaned = drop_nulls(data_with_nulls)
        assert len(cleaned) < len(data_with_nulls)

    def test_drop_nulls_specific_columns(self, data_with_nulls: list[DataRecord]) -> None:
        """Test dropping nulls from specific columns."""
        cleaned = drop_nulls(data_with_nulls, columns=("name",))
        assert len(cleaned) < len(data_with_nulls)


class TestFillNulls:
    """Tests for fill_nulls function."""

    def test_fill_nulls_with_value(self, data_with_nulls: list[DataRecord]) -> None:
        """Test filling nulls with a value."""
        cleaned = fill_nulls(data_with_nulls, "N/A", columns=("name",))
        assert len(cleaned) == len(data_with_nulls)
        assert cleaned[1].data["name"] == "N/A"

    def test_fill_nulls_zero(self, data_with_nulls: list[DataRecord]) -> None:
        """Test filling numeric nulls with zero."""
        cleaned = fill_nulls(data_with_nulls, 0, columns=("age",))
        assert cleaned[2].data["age"] == 0


# =============================================================================
# Outlier Tests
# =============================================================================


class TestDetectOutliers:
    """Tests for detect_outliers function."""

    def test_detect_outliers_iqr(self, data_with_outliers: list[DataRecord]) -> None:
        """Test detecting outliers with IQR method."""
        outliers = detect_outliers(data_with_outliers, OutlierMethod.IQR, threshold=1.5)
        assert len(outliers) > 0
        assert "age" in outliers or "score" in outliers

    def test_detect_outliers_zscore(self, data_with_outliers: list[DataRecord]) -> None:
        """Test detecting outliers with Z-score method."""
        outliers = detect_outliers(data_with_outliers, OutlierMethod.ZSCORE, threshold=2.0)
        assert len(outliers) > 0

    def test_detect_outliers_specific_columns(self, data_with_outliers: list[DataRecord]) -> None:
        """Test detecting outliers in specific columns."""
        outliers = detect_outliers(
            data_with_outliers, OutlierMethod.IQR, threshold=1.5, columns=("age",)
        )
        assert "age" in outliers
        assert "score" not in outliers

    def test_detect_outliers_no_outliers(self, sample_data: list[DataRecord]) -> None:
        """Test detecting outliers when none exist."""
        outliers = detect_outliers(sample_data, OutlierMethod.IQR)
        assert len(outliers) == 0

    def test_detect_outliers_empty(self) -> None:
        """Test detecting outliers in empty data."""
        outliers = detect_outliers([], OutlierMethod.IQR)
        assert len(outliers) == 0


class TestHandleOutliers:
    """Tests for handle_outliers function."""

    def test_handle_outliers_cap(self, data_with_outliers: list[DataRecord]) -> None:
        """Test capping outliers."""
        cleaned, report = handle_outliers(
            data_with_outliers, OutlierMethod.IQR, OutlierAction.CAP, threshold=1.5
        )
        assert len(cleaned) == len(data_with_outliers)
        assert report["handled_count"] > 0

    def test_handle_outliers_drop(self, data_with_outliers: list[DataRecord]) -> None:
        """Test dropping outliers."""
        cleaned, report = handle_outliers(
            data_with_outliers, OutlierMethod.IQR, OutlierAction.DROP, threshold=1.5
        )
        assert len(cleaned) < len(data_with_outliers)
        assert report["handled_count"] > 0

    def test_handle_outliers_mean_replace(self, data_with_outliers: list[DataRecord]) -> None:
        """Test replacing outliers with mean."""
        cleaned, report = handle_outliers(
            data_with_outliers, OutlierMethod.IQR, OutlierAction.MEAN_REPLACE, threshold=1.5
        )
        assert len(cleaned) == len(data_with_outliers)
        assert report["action"] == "MEAN_REPLACE"

    def test_handle_outliers_empty(self) -> None:
        """Test handling outliers in empty data."""
        cleaned, report = handle_outliers([], OutlierMethod.IQR, OutlierAction.CAP)
        assert len(cleaned) == 0
        assert report["handled_count"] == 0


class TestCapOutliers:
    """Tests for cap_outliers function."""

    def test_cap_outliers(self, data_with_outliers: list[DataRecord]) -> None:
        """Test capping outliers."""
        cleaned = cap_outliers(data_with_outliers, OutlierMethod.IQR, threshold=1.5)
        assert len(cleaned) == len(data_with_outliers)


# =============================================================================
# Type Normalization Tests
# =============================================================================


class TestNormalizeTypes:
    """Tests for normalize_types function."""

    def test_normalize_types_with_mapping(self, sample_data: list[DataRecord]) -> None:
        """Test normalizing types with explicit mapping."""
        type_mapping = {"age": DataType.INTEGER, "score": DataType.FLOAT}
        cleaned, report = normalize_types(sample_data, type_mapping=type_mapping)
        assert len(cleaned) == len(sample_data)
        assert report["converted_count"] >= 0

    def test_normalize_types_infer(self, sample_data: list[DataRecord]) -> None:
        """Test inferring types from data."""
        cleaned, report = normalize_types(sample_data, infer=True)
        assert len(cleaned) == len(sample_data)

    def test_normalize_types_empty(self) -> None:
        """Test normalizing types with empty data."""
        cleaned, report = normalize_types([], type_mapping={})
        assert len(cleaned) == 0
        assert report["converted_count"] == 0


class TestConvertColumnType:
    """Tests for convert_column_type function."""

    def test_convert_column_type_to_integer(self, sample_data: list[DataRecord]) -> None:
        """Test converting column to integer."""
        cleaned = convert_column_type(sample_data, "age", DataType.INTEGER)
        assert len(cleaned) == len(sample_data)

    def test_convert_column_type_to_string(self, sample_data: list[DataRecord]) -> None:
        """Test converting column to string."""
        cleaned = convert_column_type(sample_data, "name", DataType.STRING)
        assert len(cleaned) == len(sample_data)


# =============================================================================
# Standardization Tests
# =============================================================================


class TestStandardizeColumns:
    """Tests for standardize_columns function."""

    def test_standardize_columns_zscore(self, sample_data: list[DataRecord]) -> None:
        """Test standardizing with z-score."""
        cleaned, report = standardize_columns(
            sample_data, columns=("age", "score"), method="zscore"
        )
        assert len(cleaned) == len(sample_data)
        assert report["method"] == "zscore"

    def test_standardize_columns_minmax(self, sample_data: list[DataRecord]) -> None:
        """Test standardizing with min-max."""
        cleaned, report = standardize_columns(
            sample_data, columns=("age", "score"), method="minmax"
        )
        assert len(cleaned) == len(sample_data)
        assert report["method"] == "minmax"

    def test_standardize_columns_empty(self) -> None:
        """Test standardizing empty data."""
        cleaned, report = standardize_columns([], columns=("age",))
        assert len(cleaned) == 0
        assert report["converted_count"] == 0


class TestNormalizeMinMax:
    """Tests for normalize_minmax function."""

    def test_normalize_minmax(self, sample_data: list[DataRecord]) -> None:
        """Test min-max normalization."""
        cleaned = normalize_minmax(sample_data, columns=("age", "score"))
        assert len(cleaned) == len(sample_data)


class TestNormalizeZscore:
    """Tests for normalize_zscore function."""

    def test_normalize_zscore(self, sample_data: list[DataRecord]) -> None:
        """Test z-score normalization."""
        cleaned = normalize_zscore(sample_data, columns=("age", "score"))
        assert len(cleaned) == len(sample_data)


# =============================================================================
# Text Cleaning Tests
# =============================================================================


class TestCleanText:
    """Tests for clean_text function."""

    def test_clean_text_trim(self, data_with_text_issues: list[DataRecord]) -> None:
        """Test trimming whitespace."""
        cleaned, report = clean_text(data_with_text_issues, trim=True, normalize=False)
        assert len(cleaned) == len(data_with_text_issues)
        assert cleaned[0].data["name"] == "John Doe"
        assert cleaned[1].data["name"] == "Jane Smith"
        assert report["normalized_count"] > 0

    def test_clean_text_normalize_case(self, data_with_text_issues: list[DataRecord]) -> None:
        """Test normalizing case."""
        cleaned, report = clean_text(data_with_text_issues, trim=False, case_normalization="lower")
        assert len(cleaned) == len(data_with_text_issues)
        assert cleaned[0].data["email"] == "john@example.com"
        assert cleaned[1].data["email"] == "jane@example.com"

    def test_clean_text_upper_case(self, data_with_text_issues: list[DataRecord]) -> None:
        """Test converting to uppercase."""
        cleaned, report = clean_text(data_with_text_issues, trim=False, case_normalization="upper")
        assert cleaned[2].data["email"] == "BOB@EXAMPLE.COM"

    def test_clean_text_title_case(self, data_with_text_issues: list[DataRecord]) -> None:
        """Test converting to title case."""
        cleaned, report = clean_text(data_with_text_issues, trim=False, case_normalization="title")
        assert cleaned[2].data["email"] == "Bob@Example.Com"

    def test_clean_text_specific_columns(self, data_with_text_issues: list[DataRecord]) -> None:
        """Test cleaning specific columns."""
        cleaned, report = clean_text(data_with_text_issues, trim=True, columns=("name",))
        assert len(cleaned) == len(data_with_text_issues)
        assert cleaned[0].data["name"] == "John Doe"

    def test_clean_text_empty(self) -> None:
        """Test cleaning text with empty data."""
        cleaned, report = clean_text([], trim=True)
        assert len(cleaned) == 0
        assert report["normalized_count"] == 0


class TestTrimWhitespace:
    """Tests for trim_whitespace function."""

    def test_trim_whitespace(self, data_with_text_issues: list[DataRecord]) -> None:
        """Test trimming whitespace."""
        cleaned = trim_whitespace(data_with_text_issues, columns=("name",))
        assert len(cleaned) == len(data_with_text_issues)
        assert cleaned[0].data["name"] == "John Doe"
        assert cleaned[1].data["name"] == "Jane Smith"


class TestNormalizeCase:
    """Tests for normalize_case function."""

    def test_normalize_case_lower(self, data_with_text_issues: list[DataRecord]) -> None:
        """Test normalizing to lowercase."""
        cleaned = normalize_case(data_with_text_issues, case="lower", columns=("email",))
        assert len(cleaned) == len(data_with_text_issues)
        assert cleaned[0].data["email"] == "john@example.com"

    def test_normalize_case_upper(self, data_with_text_issues: list[DataRecord]) -> None:
        """Test normalizing to uppercase."""
        cleaned = normalize_case(data_with_text_issues, case="upper", columns=("name",))
        assert len(cleaned) == len(data_with_text_issues)
        assert cleaned[0].data["name"] == "JOHN DOE"


class TestRemoveSpecialChars:
    """Tests for remove_special_chars function."""

    def test_remove_special_chars(self) -> None:
        """Test removing special characters."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="text", data_type=DataType.STRING),),
        )
        data = [
            DataRecord(data={"text": "Hello, World! 123"}, schema=schema),
            DataRecord(data={"text": "Test@#$%^&*()"}, schema=schema),
        ]
        cleaned = remove_special_chars(data, columns=("text",))
        assert cleaned[0].data["text"] == "Hello World 123"
        assert cleaned[1].data["text"] == "Test"


# =============================================================================
# Utility Function Tests
# =============================================================================


class TestGetNullCounts:
    """Tests for get_null_counts function."""

    def test_get_null_counts(self, data_with_nulls: list[DataRecord]) -> None:
        """Test getting null counts."""
        null_counts = get_null_counts(data_with_nulls)
        assert "name" in null_counts
        assert "age" in null_counts
        assert null_counts["name"] > 0
        assert null_counts["age"] > 0

    def test_get_null_counts_no_nulls(self, sample_data: list[DataRecord]) -> None:
        """Test getting null counts when none exist."""
        null_counts = get_null_counts(sample_data)
        for count in null_counts.values():
            assert count == 0

    def test_get_null_counts_empty(self) -> None:
        """Test getting null counts for empty data."""
        null_counts = get_null_counts([])
        assert len(null_counts) == 0


class TestGetValueCounts:
    """Tests for get_value_counts function."""

    def test_get_value_counts(self, sample_data: list[DataRecord]) -> None:
        """Test getting value counts."""
        value_counts = get_value_counts(sample_data, "active")
        assert True in value_counts
        assert False in value_counts

    def test_get_value_counts_top_n(self, sample_data: list[DataRecord]) -> None:
        """Test getting top N value counts."""
        value_counts = get_value_counts(sample_data, "active", top_n=2)
        assert len(value_counts) <= 2

    def test_get_value_counts_empty(self) -> None:
        """Test getting value counts for empty data."""
        value_counts = get_value_counts([], "column")
        assert len(value_counts) == 0


class TestGetDataProfile:
    """Tests for get_data_profile function."""

    def test_get_data_profile(self, sample_data: list[DataRecord]) -> None:
        """Test getting data profile."""
        profile = get_data_profile(sample_data)
        assert profile["row_count"] == 5
        assert profile["column_count"] == 6
        assert "id" in profile["columns"]
        assert "null_counts" in profile
        assert "duplicate_rows" in profile

    def test_get_data_profile_empty(self) -> None:
        """Test getting profile for empty data."""
        profile = get_data_profile([])
        assert profile == {}


class TestSummarizeReport:
    """Tests for summarize_report function."""

    def test_summarize_report(self) -> None:
        """Test summarizing cleaning report."""
        report = CleaningReport(
            original_count=100,
            final_count=90,
            duplicates_removed=5,
            nulls_filled=3,
            outliers_handled=2,
            operations=("deduplication", "null_handling", "outlier_treatment"),
        )
        summary = summarize_report(report)
        assert "Data Cleaning Report" in summary
        assert "Original records: 100" in summary
        assert "Final records: 90" in summary
        assert "deduplication" in summary
        assert "null_handling" in summary
        assert "outlier_treatment" in summary

    def test_summarize_report_no_operations(self) -> None:
        """Test summarizing report with no operations."""
        report = CleaningReport(original_count=10, final_count=10)
        summary = summarize_report(report)
        assert "Data Cleaning Report" in summary
        assert "Original records: 10" in summary
