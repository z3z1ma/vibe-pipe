"""
Tests for schema evolution module.

Tests cover:
- Semantic versioning
- Schema diff detection
- Breaking change detection
- Migration planning
- Backward compatibility checking
- Schema history tracking
- @schema_version decorator
"""

from datetime import datetime
from typing import Any

import pytest

from vibe_piper import (
    BackwardCompatibilityChecker,
    BreakingChangeDetector,
    BreakingChangeSeverity,
    ChangeType,
    DataType,
    MigrationPlan,
    MigrationPlanner,
    Schema,
    SchemaChange,
    SchemaDiff,
    SchemaField,
    define_schema,
    get_schema_history,
    reset_schema_history,
    schema_version,
)
from vibe_piper.schema_definitions import (
    Boolean as BooleanField,
)
from vibe_piper.schema_definitions import (
    Float as FloatField,
)
from vibe_piper.schema_definitions import (
    Integer as IntegerField,
)
from vibe_piper.schema_definitions import (
    String as StringField,
)
from vibe_piper.schema_evolution import (
    MigrationStep,
    SchemaHistory,
    SchemaHistoryEntry,
    SemanticVersion,
)

# =============================================================================
# Semantic Version Tests
# =============================================================================


class TestSemanticVersion:
    """Tests for SemanticVersion class."""

    def test_parse_basic_version(self) -> None:
        """Test parsing basic version string."""
        version = SemanticVersion.parse("1.2.3")
        assert version.major == 1
        assert version.minor == 2
        assert version.patch == 3
        assert str(version) == "1.2.3"

    def test_parse_version_with_prerelease(self) -> None:
        """Test parsing version with prerelease."""
        version = SemanticVersion.parse("2.0.0-alpha")
        assert version.major == 2
        assert version.minor == 0
        assert version.patch == 0
        assert version.prerelease == "alpha"
        assert str(version) == "2.0.0-alpha"

    def test_parse_version_with_build(self) -> None:
        """Test parsing version with build metadata."""
        version = SemanticVersion.parse("1.0.0+build.123")
        assert version.major == 1
        assert version.minor == 0
        assert version.patch == 0
        assert version.build == "build.123"
        assert str(version) == "1.0.0+build.123"

    def test_version_comparison(self) -> None:
        """Test version comparison."""
        v1 = SemanticVersion(1, 0, 0)
        v2 = SemanticVersion(1, 1, 0)
        v3 = SemanticVersion(2, 0, 0)

        assert v2 > v1
        assert v3 > v2
        assert v1 < v2

    def test_version_equality(self) -> None:
        """Test version equality."""
        v1 = SemanticVersion(1, 2, 3)
        v2 = SemanticVersion(1, 2, 3)

        assert v1 == v2

    def test_version_prerelease_comparison(self) -> None:
        """Test prerelease version comparison."""
        v1 = SemanticVersion(1, 0, 0, "alpha")
        v2 = SemanticVersion(1, 0, 0)
        v3 = SemanticVersion(1, 0, 0, "beta")

        # Release version > prerelease version
        assert v2 > v1
        # Alpha < beta (lexicographic)
        assert v1 < v3

    def test_version_compatibility(self) -> None:
        """Test version compatibility check."""
        v1 = SemanticVersion(1, 2, 0)
        v2 = SemanticVersion(1, 3, 0)
        v3 = SemanticVersion(2, 0, 0)

        # Same major, higher minor is compatible
        assert v2.is_compatible(v1)
        # Different major is not compatible
        assert not v3.is_compatible(v1)

    def test_next_version_methods(self) -> None:
        """Test next version generation methods."""
        v1 = SemanticVersion(1, 2, 3)

        v2_major = v1.next_major()
        assert v2_major.major == 2
        assert v2_major.minor == 0
        assert v2_major.patch == 0

        v2_minor = v1.next_minor()
        assert v2_minor.major == 1
        assert v2_minor.minor == 3
        assert v2_minor.patch == 0

        v2_patch = v1.next_patch()
        assert v2_patch.major == 1
        assert v2_patch.minor == 2
        assert v2_patch.patch == 4

    def test_invalid_version_raises(self) -> None:
        """Test that invalid version strings raise errors."""
        with pytest.raises(ValueError):
            SemanticVersion.parse("invalid")

        with pytest.raises(ValueError):
            SemanticVersion.parse("1.2")

        with pytest.raises(ValueError):
            SemanticVersion(1, -1, 0)


# =============================================================================
# Schema Diff Tests
# =============================================================================


class TestSchemaDiff:
    """Tests for SchemaDiff and SchemaChange."""

    def test_field_added_change(self) -> None:
        """Test detection of added field."""
        old_schema = Schema(name="test", fields=())
        new_schema = Schema(
            name="test",
            fields=(SchemaField(name="new_field", data_type=Integer),),
        )

        detector = BreakingChangeDetector()
        diff = detector.detect(old_schema, new_schema)

        assert diff.has_breaking_changes is False
        assert len(diff.changes) == 1
        assert diff.changes[0].change_type == ChangeType.FIELD_ADDED
        assert diff.changes[0].field_name == "new_field"
        assert diff.changes[0].is_breaking is False

    def test_field_removed_change(self) -> None:
        """Test detection of removed field."""
        old_schema = Schema(
            name="test",
            fields=(SchemaField(name="old_field", data_type=Integer),),
        )
        new_schema = Schema(name="test", fields=())

        detector = BreakingChangeDetector()
        diff = detector.detect(old_schema, new_schema)

        assert diff.has_breaking_changes is True
        assert len(diff.changes) == 1
        assert diff.changes[0].change_type == ChangeType.FIELD_REMOVED
        assert diff.changes[0].field_name == "old_field"
        assert diff.changes[0].is_breaking is True
        assert diff.changes[0].severity == BreakingChangeSeverity.MAJOR

    def test_field_type_changed(self) -> None:
        """Test detection of field type change."""
        old_schema = Schema(
            name="test",
            fields=(SchemaField(name="field", data_type=Integer),),
        )
        new_schema = Schema(
            name="test",
            fields=(SchemaField(name="field", data_type=String),),
        )

        detector = BreakingChangeDetector()
        diff = detector.detect(old_schema, new_schema)

        assert diff.has_breaking_changes is True
        assert len(diff.changes) == 1
        assert diff.changes[0].change_type == ChangeType.FIELD_TYPE_CHANGED
        assert diff.changes[0].is_breaking is True

    def test_field_nullable_changed(self) -> None:
        """Test detection of nullability change."""
        old_schema = Schema(
            name="test",
            fields=(SchemaField(name="field", data_type=Integer, nullable=False),),
        )
        new_schema = Schema(
            name="test",
            fields=(SchemaField(name="field", data_type=Integer, nullable=True),),
        )

        detector = BreakingChangeDetector()
        diff = detector.detect(old_schema, new_schema)

        # non-nullable to nullable is not breaking
        assert diff.has_breaking_changes is False

        # Nullable to non-nullable is breaking
        old_schema_nullable = Schema(
            name="test",
            fields=(SchemaField(name="field", data_type=Integer, nullable=True),),
        )
        new_schema_non_nullable = Schema(
            name="test",
            fields=(SchemaField(name="field", data_type=Integer, nullable=False),),
        )

        diff_breaking = detector.detect(old_schema_nullable, new_schema_non_nullable)
        assert diff_breaking.has_breaking_changes is True

    def test_field_required_changed(self) -> None:
        """Test detection of required field change."""
        old_schema = Schema(
            name="test",
            fields=(SchemaField(name="field", data_type=Integer, required=True),),
        )
        new_schema = Schema(
            name="test",
            fields=(SchemaField(name="field", data_type=Integer, required=False),),
        )

        detector = BreakingChangeDetector()
        diff = detector.detect(old_schema, new_schema)

        # Required to optional is not breaking
        assert diff.has_breaking_changes is False

        # Optional to required is breaking
        old_schema_optional = Schema(
            name="test",
            fields=(SchemaField(name="field", data_type=Integer, required=False),),
        )
        new_schema_required = Schema(
            name="test",
            fields=(SchemaField(name="field", data_type=Integer, required=True),),
        )

        diff_breaking = detector.detect(old_schema_optional, new_schema_required)
        assert diff_breaking.has_breaking_changes is True

    def test_constraint_change(self) -> None:
        """Test detection of constraint changes."""
        old_schema = Schema(
            name="test",
            fields=(
                SchemaField(
                    name="field",
                    data_type=Integer,
                    constraints={"max_value": 100},
                ),
            ),
        )
        new_schema = Schema(
            name="test",
            fields=(
                SchemaField(
                    name="field",
                    data_type=Integer,
                    constraints={"max_value": 200},
                ),
            ),
        )

        detector = BreakingChangeDetector()
        diff = detector.detect(old_schema, new_schema)

        # Relaxing constraint is not breaking
        assert diff.has_breaking_changes is False

        # Tightening constraint is breaking
        new_schema_tight = Schema(
            name="test",
            fields=(
                SchemaField(
                    name="field",
                    data_type=Integer,
                    constraints={"max_value": 50},
                ),
            ),
        )

        diff_breaking = detector.detect(old_schema, new_schema_tight)
        assert diff_breaking.has_breaking_changes is True

    def test_no_changes(self) -> None:
        """Test that identical schemas have no changes."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="field", data_type=Integer),),
        )

        detector = BreakingChangeDetector()
        diff = detector.detect(schema, schema)

        assert len(diff.changes) == 0
        assert diff.has_breaking_changes is False
        assert diff.severity == BreakingChangeSeverity.NONE

    def test_diff_to_dict(self) -> None:
        """Test SchemaDiff to_dict conversion."""
        old_schema = Schema(
            name="old",
            fields=(SchemaField(name="field1", data_type=Integer),),
        )
        new_schema = Schema(
            name="new",
            fields=(SchemaField(name="field2", data_type=String),),
        )

        detector = BreakingChangeDetector()
        diff = detector.detect(old_schema, new_schema)

        diff_dict = diff.to_dict()
        assert diff_dict["old_schema"] == "old"
        assert diff_dict["new_schema"] == "new"
        assert isinstance(diff_dict["changes"], list)
        assert diff_dict["has_breaking_changes"] is True


# =============================================================================
# Migration Plan Tests
# =============================================================================


class TestMigrationPlan:
    """Tests for MigrationPlan and MigrationStep."""

    def test_add_field_migration(self) -> None:
        """Test migration step for adding a field."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="new_field", data_type=Integer),),
        )

        change = SchemaChange(
            change_type=ChangeType.FIELD_ADDED,
            field_name="new_field",
            new_field=schema.fields[0],
            description="Field 'new_field' was added",
        )

        planner = MigrationPlanner()
        step = planner._change_to_step(change)

        assert step is not None
        assert step.step_type == "add_field"
        assert step.field_name == "new_field"
        assert step.new_value == 0  # default for Integer

    def test_remove_field_migration(self) -> None:
        """Test migration step for removing a field."""
        change = SchemaChange(
            change_type=ChangeType.FIELD_REMOVED,
            field_name="old_field",
            description="Field 'old_field' was removed",
        )

        planner = MigrationPlanner()
        step = planner._change_to_step(change)

        assert step is not None
        assert step.step_type == "remove_field"
        assert step.field_name == "old_field"

    def test_migration_plan_execution(self) -> None:
        """Test executing a migration plan."""
        old_schema = Schema(
            name="old",
            fields=(SchemaField(name="id", data_type=Integer),),
        )
        new_schema = Schema(
            name="new",
            fields=(
                SchemaField(name="id", data_type=Integer),
                SchemaField(name="name", data_type=String),
            ),
        )

        detector = BreakingChangeDetector()
        diff = detector.detect(old_schema, new_schema)

        planner = MigrationPlanner()
        plan = planner.generate(diff)

        data = {"id": 1}
        migrated = plan.execute(data)

        assert "id" in migrated
        assert "name" in migrated
        assert migrated["name"] == ""  # default for String

    def test_migration_plan_to_dict(self) -> None:
        """Test MigrationPlan to_dict conversion."""
        old_schema = Schema(name="old", fields=())
        new_schema = Schema(
            name="new",
            fields=(SchemaField(name="field", data_type=Integer),),
        )

        detector = BreakingChangeDetector()
        diff = detector.detect(old_schema, new_schema)

        planner = MigrationPlanner()
        plan = planner.generate(diff)

        plan_dict = plan.to_dict()
        assert plan_dict["from_version"] == "1.0.0"
        assert plan_dict["to_version"] == "1.0.0"
        assert isinstance(plan_dict["steps"], list)
        assert plan_dict["estimated_risk"] == "low"


# =============================================================================
# Backward Compatibility Tests
# =============================================================================


class TestBackwardCompatibilityChecker:
    """Tests for BackwardCompatibilityChecker."""

    def test_backward_compatible_change(self) -> None:
        """Test backward compatible schema change."""
        old_schema = Schema(
            name="test",
            fields=(SchemaField(name="field", data_type=Integer),),
        )
        new_schema = Schema(
            name="test",
            fields=(
                SchemaField(name="field", data_type=Integer),
                SchemaField(name="new_field", data_type=String),
            ),
        )

        checker = BackwardCompatibilityChecker()
        is_compatible = checker.is_backward_compatible(old_schema, new_schema)

        assert is_compatible is True

    def test_backward_incompatible_change(self) -> None:
        """Test backward incompatible schema change."""
        old_schema = Schema(
            name="test",
            fields=(SchemaField(name="field", data_type=Integer),),
        )
        new_schema = Schema(name="test", fields=())

        checker = BackwardCompatibilityChecker()
        is_compatible = checker.is_backward_compatible(old_schema, new_schema)

        assert is_compatible is False

    def test_compatibility_report(self) -> None:
        """Test compatibility report generation."""
        old_schema = Schema(
            name="test",
            fields=(SchemaField(name="field", data_type=Integer),),
        )
        new_schema = Schema(name="test", fields=())

        checker = BackwardCompatibilityChecker()
        report = checker.get_compatibility_report(old_schema, new_schema)

        assert "✗ Not backward compatible" in report


# =============================================================================
# Schema History Tests
# =============================================================================


class TestSchemaHistory:
    """Tests for SchemaHistory."""

    def setup_method(self) -> None:
        """Reset history before each test."""
        reset_schema_history()

    def test_add_entry(self) -> None:
        """Test adding a history entry."""
        schema = Schema(
            name="test_schema",
            fields=(SchemaField(name="field", data_type=Integer),),
        )
        version = SemanticVersion(1, 0, 0)

        history = get_schema_history()
        entry = SchemaHistoryEntry(
            schema_name="test_schema",
            version=version,
            schema=schema,
            author="test",
            description="Initial version",
        )

        history.add_entry(entry)

        retrieved = history.get_version("test_schema", version)
        assert retrieved is not None
        assert retrieved.version == version
        assert retrieved.author == "test"

    def test_get_latest_version(self) -> None:
        """Test getting the latest version."""
        schema = Schema(
            name="test_schema",
            fields=(SchemaField(name="field", data_type=Integer),),
        )

        history = get_schema_history()
        entry1 = SchemaHistoryEntry(
            schema_name="test_schema",
            version=SemanticVersion(1, 0, 0),
            schema=schema,
            description="v1",
        )
        entry2 = SchemaHistoryEntry(
            schema_name="test_schema",
            version=SemanticVersion(1, 1, 0),
            schema=schema,
            description="v2",
        )

        history.add_entry(entry1)
        history.add_entry(entry2)

        latest = history.get_latest_version("test_schema")
        assert latest is not None
        assert latest.version == SemanticVersion(1, 1, 0)

    def test_get_all_versions(self) -> None:
        """Test getting all versions."""
        schema = Schema(
            name="test_schema",
            fields=(SchemaField(name="field", data_type=Integer),),
        )

        history = get_schema_history()
        entry1 = SchemaHistoryEntry(
            schema_name="test_schema",
            version=SemanticVersion(1, 0, 0),
            schema=schema,
        )
        entry2 = SchemaHistoryEntry(
            schema_name="test_schema",
            version=SemanticVersion(2, 0, 0),
            schema=schema,
        )

        history.add_entry(entry1)
        history.add_entry(entry2)

        all_versions = history.get_all_versions("test_schema")
        assert len(all_versions) == 2
        assert all_versions[0].version == SemanticVersion(1, 0, 0)
        assert all_versions[1].version == SemanticVersion(2, 0, 0)

    def test_rollback_to_version(self) -> None:
        """Test rollback to a previous version."""
        schema = Schema(
            name="test_schema",
            fields=(SchemaField(name="field", data_type=Integer),),
        )
        version = SemanticVersion(1, 0, 0)

        history = get_schema_history()
        entry = SchemaHistoryEntry(
            schema_name="test_schema",
            version=version,
            schema=schema,
        )

        history.add_entry(entry)

        rolled_back = history.rollback_to("test_schema", version)
        assert rolled_back is not None
        assert rolled_back.author == "rollback"
        assert "1.0.1-rollback" in str(rolled_back.version)

    def test_history_to_dict(self) -> None:
        """Test SchemaHistory to_dict conversion."""
        history = get_schema_history()
        history_dict = history.to_dict()

        assert history_dict["enabled"] is True
        assert isinstance(history_dict["schemas"], dict)

    def test_history_disabled(self) -> None:
        """Test that disabled history doesn't track entries."""
        history = SchemaHistory(enabled=False)
        schema = Schema(
            name="test_schema",
            fields=(SchemaField(name="field", data_type=Integer),),
        )
        version = SemanticVersion(1, 0, 0)

        entry = SchemaHistoryEntry(
            schema_name="test_schema",
            version=version,
            schema=schema,
        )

        history.add_entry(entry)

        assert history.get_latest_version("test_schema") is None


# =============================================================================
# Schema Version Decorator Tests
# =============================================================================


class TestSchemaVersionDecorator:
    """Tests for @schema_version decorator."""

    def setup_method(self) -> None:
        """Reset history before each test."""
        reset_schema_history()

    def test_schema_version_attribute(self) -> None:
        """Test that decorator sets version attribute."""

        @define_schema
        @schema_version("1.2.0", author="test")
        class TestSchema:
            id: Integer = Integer()

        assert hasattr(TestSchema, "__schema_version__")
        assert str(TestSchema.__schema_version__) == "1.2.0"

    def test_schema_version_in_history(self) -> None:
        """Test that schema version is tracked in history."""

        @define_schema
        @schema_version(
            "1.0.0",
            description="Initial version",
            track_in_history=True,
        )
        class TestSchema:
            id: Integer = Integer()

        history = get_schema_history()
        entry = history.get_latest_version("TestSchema")

        assert entry is not None
        assert str(entry.version) == "1.0.0"
        assert entry.description == "Initial version"

    def test_schema_version_no_history_tracking(self) -> None:
        """Test that history tracking can be disabled."""

        @define_schema
        @schema_version(
            "1.0.0",
            track_in_history=False,
        )
        class TestSchema:
            id: Integer = Integer()

        history = get_schema_history()
        entry = history.get_latest_version("TestSchema")

        assert entry is None

    def test_schema_version_with_prerelease(self) -> None:
        """Test decorator with prerelease version."""

        @define_schema
        @schema_version("2.0.0-beta", author="test")
        class TestSchema:
            id: Integer = Integer()

        assert str(TestSchema.__schema_version__) == "2.0.0-beta"
        assert TestSchema.__schema_version__.prerelease == "beta"


# =============================================================================
# Integration Tests
# =============================================================================


class TestSchemaEvolutionIntegration:
    """Integration tests for schema evolution."""

    def setup_method(self) -> None:
        """Reset history before each test."""
        reset_schema_history()

    def test_full_schema_evolution_workflow(self) -> None:
        """Test complete schema evolution workflow."""

        # Define initial schema
        @define_schema
        @schema_version("1.0.0", description="Initial schema")
        class UserV1:
            id: Integer = Integer()
            name: String = String(max_length=100)

        # Define evolved schema
        @define_schema
        @schema_version(
            "2.0.0",
            description="Add email field",
        )
        class UserV2:
            id: Integer = Integer()
            name: String = String(max_length=100)
            email: String = String(max_length=255)

        # Detect changes
        old_schema = UserV1.to_schema()
        new_schema = UserV2.to_schema()

        detector = BreakingChangeDetector()
        diff = detector.detect(old_schema, new_schema)

        # Check that we detected the added field
        assert len(diff.changes) == 1
        assert diff.changes[0].change_type == ChangeType.FIELD_ADDED
        assert diff.changes[0].field_name == "email"

        # Check backward compatibility
        checker = BackwardCompatibilityChecker()
        is_compatible = checker.is_backward_compatible(old_schema, new_schema)
        assert is_compatible is True

        # Generate migration plan
        planner = MigrationPlanner()
        plan = planner.generate(diff)

        # Execute migration
        old_data = {"id": 1, "name": "Alice"}
        new_data = plan.execute(old_data)

        assert "id" in new_data
        assert "name" in new_data
        assert "email" in new_data
        assert new_data["email"] == ""  # default for String

    def test_breaking_schema_change_workflow(self) -> None:
        """Test breaking schema change workflow."""

        # Define initial schema
        @define_schema
        @schema_version("1.0.0", description="Initial schema")
        class ProductV1:
            id: Integer = Integer()
            price: Float = Float()
            description: String = String()

        # Define breaking schema change
        @define_schema
        @schema_version("2.0.0", description="Remove description field")
        class ProductV2:
            id: Integer = Integer()
            price: Float = Float()

        # Detect changes
        old_schema = ProductV1.to_schema()
        new_schema = ProductV2.to_schema()

        detector = BreakingChangeDetector()
        diff = detector.detect(old_schema, new_schema)

        # Check that we detected the breaking change
        assert diff.has_breaking_changes is True
        assert any(c.change_type == ChangeType.FIELD_REMOVED for c in diff.changes)

        # Check backward compatibility
        checker = BackwardCompatibilityChecker()
        is_compatible = checker.is_backward_compatible(old_schema, new_schema)
        assert is_compatible is False

    def test_multi_step_migration(self) -> None:
        """Test migration with multiple steps."""
        old_schema = Schema(
            name="old",
            fields=(
                SchemaField(name="id", data_type=Integer),
                SchemaField(name="name", data_type=String),
            ),
        )
        new_schema = Schema(
            name="new",
            fields=(
                SchemaField(name="id", data_type=Integer),
                SchemaField(
                    name="name",
                    data_type=String,
                    constraints={"max_length": 50},
                ),
                SchemaField(name="active", data_type=Boolean),
            ),
        )

        detector = BreakingChangeDetector()
        diff = detector.detect(old_schema, new_schema)

        planner = MigrationPlanner()
        plan = planner.generate(diff)

        # Execute migration
        old_data = {"id": 1, "name": "Alice"}
        new_data = plan.execute(old_data)

        assert new_data["id"] == 1
        assert new_data["name"] == "Alice"
        assert new_data["active"] is False  # default for Boolean

    def test_custom_migration_function(self) -> None:
        """Test custom migration function."""
        old_schema = Schema(
            name="old",
            fields=(SchemaField(name="full_name", data_type=String),),
        )
        new_schema = Schema(
            name="new",
            fields=(
                SchemaField(name="first_name", data_type=String),
                SchemaField(name="last_name", data_type=String),
            ),
        )

        detector = BreakingChangeDetector()
        diff = detector.detect(old_schema, new_schema)

        # Custom migration to split full_name into first and last
        def split_name(
            data: dict[str, Any],
            old_schema: Schema | None,
            new_schema: Schema | None,
        ) -> dict[str, Any]:
            full_name = data.get("full_name", "")
            parts = full_name.split(" ", 1)
            data["first_name"] = parts[0] if parts else ""
            data["last_name"] = parts[1] if len(parts) > 1 else ""
            return data

        planner = MigrationPlanner()
        plan = planner.generate(diff, custom_migrations={"full_name": split_name})

        old_data = {"full_name": "Alice Smith"}
        new_data = plan.execute(old_data)

        assert new_data["first_name"] == "Alice"
        assert new_data["last_name"] == "Smith"
