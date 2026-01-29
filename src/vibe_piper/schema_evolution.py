"""
Schema Evolution for Vibe Piper.

This module provides comprehensive schema versioning, migration, and evolution
capabilities. It enables safe schema changes with backward compatibility
checking, breaking change detection, and automatic migration planning.

Features:
- Semantic versioning for schemas
- Backward compatibility validation
- Breaking change detection
- Schema diff generation
- Migration plan generation
- Rollback support
- Schema history tracking
- @schema_version decorator for declarative versioning
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, TypeAlias

from vibe_piper.types import Schema, SchemaField

if TYPE_CHECKING:
    pass

# =============================================================================
# Type Aliases
# =============================================================================

#: Field name to field mapping
FieldMap: TypeAlias = Mapping[str, SchemaField]

#: Schema migration function signature
MigrationFn: TypeAlias = Callable[[dict[str, Any], Schema | None, Schema | None], dict[str, Any]]

# =============================================================================
# Enums
# =============================================================================


class ChangeType(Enum):
    """Types of schema changes."""

    FIELD_ADDED = auto()  # New field added
    FIELD_REMOVED = auto()  # Field removed
    FIELD_RENAMED = auto()  # Field renamed
    FIELD_TYPE_CHANGED = auto()  # Field data type changed
    FIELD_NULLABILITY_CHANGED = auto()  # Field nullable requirement changed
    FIELD_REQUIRED_CHANGED = auto()  # Field required requirement changed
    FIELD_CONSTRAINT_CHANGED = auto()  # Field constraint changed
    SCHEMA_RENAMED = auto()  # Schema renamed
    NO_CHANGE = auto()  # No changes detected


class BreakingChangeSeverity(Enum):
    """Severity levels for breaking changes."""

    MAJOR = auto()  # Breaking change, requires manual intervention
    MINOR = auto()  # Potentially breaking, may need attention
    NONE = auto()  # Non-breaking change


# =============================================================================
# Core Types
# =============================================================================


@dataclass(frozen=True)
class SemanticVersion:
    """
    Semantic version for schema versioning.

    Follows Semantic Versioning 2.0.0: MAJOR.MINOR.PATCH

    - MAJOR: Breaking changes
    - MINOR: Backward-compatible additions
    - PATCH: Backward-compatible bug fixes

    Attributes:
        major: Major version number
        minor: Minor version number
        patch: Patch version number
        prerelease: Optional prerelease identifier (e.g., "alpha", "beta")
        build: Optional build metadata
    """

    major: int
    minor: int
    patch: int = 0
    prerelease: str | None = None
    build: str | None = None

    def __post_init__(self) -> None:
        """Validate version components."""
        if self.major < 0:
            msg = f"Major version must be non-negative, got {self.major}"
            raise ValueError(msg)
        if self.minor < 0:
            msg = f"Minor version must be non-negative, got {self.minor}"
            raise ValueError(msg)
        if self.patch < 0:
            msg = f"Patch version must be non-negative, got {self.patch}"
            raise ValueError(msg)

    def __str__(self) -> str:
        """Return string representation of version."""
        version_str = f"{self.major}.{self.minor}.{self.patch}"
        if self.prerelease:
            version_str += f"-{self.prerelease}"
        if self.build:
            version_str += f"+{self.build}"
        return version_str

    def __lt__(self, other: object) -> bool:
        """Compare versions for ordering."""
        if not isinstance(other, SemanticVersion):
            return NotImplemented
        if self.major != other.major:
            return self.major < other.major
        if self.minor != other.minor:
            return self.minor < other.minor
        if self.patch != other.patch:
            return self.patch < other.patch

        # Compare prerelease (no prerelease > any prerelease)
        if self.prerelease is None and other.prerelease is not None:
            return False
        if self.prerelease is not None and other.prerelease is None:
            return True
        if self.prerelease is not None and other.prerelease is not None:
            return self.prerelease < other.prerelease

        return False

    def __le__(self, other: object) -> bool:
        """Compare versions for ordering."""
        if not isinstance(other, SemanticVersion):
            return NotImplemented
        return self < other or self == other

    def __eq__(self, other: object) -> bool:
        """Compare versions for equality."""
        if not isinstance(other, SemanticVersion):
            return NotImplemented
        return (
            self.major == other.major
            and self.minor == other.minor
            and self.patch == other.patch
            and self.prerelease == other.prerelease
            # Build metadata doesn't affect precedence
        )

    def __hash__(self) -> int:
        """Make version hashable."""
        return hash(
            (self.major, self.minor, self.patch, self.prerelease)
            # Build metadata excluded from hash
        )

    def is_compatible(self, other: SemanticVersion | None = None) -> bool:
        """
        Check if this version is compatible with another.

        A version is compatible if it has the same major version
        and a greater or equal minor/patch version.

        Args:
            other: Other version to compare against (defaults to 1.0.0)

        Returns:
            True if compatible, False otherwise
        """
        target = other or SemanticVersion(1, 0, 0)
        return self.major == target.major and (
            self.minor > target.minor or self.minor == target.minor
        )

    def next_major(self, prerelease: str | None = None) -> "SemanticVersion":
        """Return next major version."""
        return SemanticVersion(
            major=self.major + 1,
            minor=0,
            patch=0,
            prerelease=prerelease,
        )

    def next_minor(self, prerelease: str | None = None) -> "SemanticVersion":
        """Return next minor version."""
        return SemanticVersion(
            major=self.major,
            minor=self.minor + 1,
            patch=0,
            prerelease=prerelease,
        )

    def next_patch(self, prerelease: str | None = None) -> "SemanticVersion":
        """Return next patch version."""
        return SemanticVersion(
            major=self.major,
            minor=self.minor,
            patch=self.patch + 1,
            prerelease=prerelease,
        )

    @classmethod
    def parse(cls, version_str: str) -> "SemanticVersion":
        """
        Parse version string into SemanticVersion.

        Args:
            version_str: Version string (e.g., "1.2.3", "2.0.0-alpha")

        Returns:
            SemanticVersion instance

        Raises:
            ValueError: If version string is invalid
        """
        # Split version and build metadata
        version_part = version_str.split("+")[0]
        build = version_str.split("+")[1] if "+" in version_str else None

        # Split version and prerelease
        prerelease_part = version_part.split("-")[1] if "-" in version_part else None
        version_nums = version_part.split("-")[0].split(".")

        if len(version_nums) < 2:
            msg = f"Invalid version string: {version_str}"
            raise ValueError(msg)

        try:
            major = int(version_nums[0])
            minor = int(version_nums[1])
            patch = int(version_nums[2]) if len(version_nums) > 2 else 0
        except (IndexError, ValueError) as e:
            msg = f"Invalid version numbers in: {version_str}"
            raise ValueError(msg) from e

        return cls(
            major=major,
            minor=minor,
            patch=patch,
            prerelease=prerelease_part,
            build=build,
        )


@dataclass(frozen=True)
class SchemaChange:
    """
    Represents a single change between two schemas.

    Attributes:
        change_type: The type of change detected
        field_name: Name of the field that changed (if applicable)
        old_field: The field in the old schema (if applicable)
        new_field: The field in the new schema (if applicable)
        description: Human-readable description of the change
        is_breaking: Whether this change is backward-incompatible
        severity: Severity of the breaking change
    """

    change_type: ChangeType
    field_name: str | None = None
    old_field: SchemaField | None = None
    new_field: SchemaField | None = None
    description: str = ""
    is_breaking: bool = False
    severity: BreakingChangeSeverity = BreakingChangeSeverity.NONE

    def __str__(self) -> str:
        """Return string representation of the change."""
        if self.field_name:
            return f"{self.change_type.name}: {self.field_name} - {self.description}"
        return f"{self.change_type.name}: {self.description}"


@dataclass(frozen=True)
class SchemaDiff:
    """
    Represents differences between two schemas.

    A SchemaDiff captures all changes between an old and new schema,
    including breaking changes and severity analysis.

    Attributes:
        old_schema: The original schema
        new_schema: The modified schema
        old_version: Version of the old schema
        new_version: Version of the new schema
        changes: List of individual changes detected
        breaking_changes: List of breaking changes only
        non_breaking_changes: List of non-breaking changes
        has_breaking_changes: Whether any breaking changes exist
        severity: Overall severity of changes
    """

    old_schema: Schema
    new_schema: Schema
    old_version: SemanticVersion | None = None
    new_version: SemanticVersion | None = None
    changes: tuple[SchemaChange, ...] = field(default_factory=tuple)
    breaking_changes: tuple[SchemaChange, ...] = field(default_factory=tuple)
    non_breaking_changes: tuple[SchemaChange, ...] = field(default_factory=tuple)
    has_breaking_changes: bool = False
    severity: BreakingChangeSeverity = BreakingChangeSeverity.NONE

    def to_dict(self) -> dict[str, Any]:
        """Convert diff to dictionary representation."""
        return {
            "old_schema": self.old_schema.name,
            "new_schema": self.new_schema.name,
            "old_version": str(self.old_version) if self.old_version else None,
            "new_version": str(self.new_version) if self.new_version else None,
            "changes": [
                {
                    "type": c.change_type.name,
                    "field": c.field_name,
                    "description": c.description,
                    "is_breaking": c.is_breaking,
                    "severity": c.severity.name,
                }
                for c in self.changes
            ],
            "has_breaking_changes": self.has_breaking_changes,
            "severity": self.severity.name,
        }


@dataclass(frozen=True)
class MigrationStep:
    """
    A single step in a migration plan.

    Migration steps define the actions needed to transform data
    from one schema version to another.

    Attributes:
        step_type: Type of migration action
        description: Human-readable description of the step
        field_name: Name of the field to migrate (if applicable)
        old_value: Old value or type (if applicable)
        new_value: New value or type (if applicable)
        migration_fn: Optional custom migration function
    """

    step_type: str
    description: str
    field_name: str | None = None
    old_value: Any | None = None
    new_value: Any | None = None
    migration_fn: MigrationFn | None = None

    def __str__(self) -> str:
        """Return string representation of the step."""
        if self.field_name:
            return f"{self.step_type} {self.field_name}: {self.description}"
        return f"{self.step_type}: {self.description}"


@dataclass(frozen=True)
class MigrationPlan:
    """
    A plan for migrating data from one schema version to another.

    Migration plans are automatically generated from schema diffs and
    can be customized with custom migration functions.

    Attributes:
        from_version: Source schema version
        to_version: Target schema version
        from_schema: Source schema
        to_schema: Target schema
        steps: Ordered list of migration steps
        estimated_risk: Risk assessment of the migration
        rollback_possible: Whether rollback is possible
        custom_migrations: Custom migration functions by field name
    """

    from_version: SemanticVersion
    to_version: SemanticVersion
    from_schema: Schema
    to_schema: Schema
    steps: tuple[MigrationStep, ...] = field(default_factory=tuple)
    estimated_risk: str = "low"
    rollback_possible: bool = True
    custom_migrations: dict[str, MigrationFn] = field(default_factory=dict)

    def execute(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        Execute the migration plan on a data record.

        Args:
            data: The data record to migrate

        Returns:
            Migrated data record
        """
        result = dict(data)

        for step in self.steps:
            if step.migration_fn:
                # Use custom migration function
                result = step.migration_fn(result, self.from_schema, self.to_schema)
            else:
                # Apply default migration logic
                result = self._apply_step(result, step)

        return result

    def _apply_step(self, data: dict[str, Any], step: MigrationStep) -> dict[str, Any]:
        """Apply a single migration step."""
        result = dict(data)

        if step.field_name:
            if step.step_type == "remove_field":
                result.pop(step.field_name, None)
            elif step.step_type == "add_field":
                if step.field_name not in result:
                    result[step.field_name] = step.new_value
            elif step.step_type == "rename_field":
                if step.old_value in result:
                    result[step.field_name] = result.pop(step.old_value)
            elif step.step_type == "default_value":
                if step.field_name not in result or result[step.field_name] is None:
                    result[step.field_name] = step.new_value

        return result

    def to_dict(self) -> dict[str, Any]:
        """Convert plan to dictionary representation."""
        return {
            "from_version": str(self.from_version),
            "to_version": str(self.to_version),
            "steps": [
                {
                    "type": s.step_type,
                    "description": s.description,
                    "field": s.field_name,
                    "old_value": str(s.old_value) if s.old_value is not None else None,
                    "new_value": str(s.new_value) if s.new_value is not None else None,
                }
                for s in self.steps
            ],
            "estimated_risk": self.estimated_risk,
            "rollback_possible": self.rollback_possible,
        }


@dataclass(frozen=True)
class SchemaHistoryEntry:
    """
    An entry in the schema history.

    Tracks the evolution of a schema over time.

    Attributes:
        schema_name: Name of the schema
        version: Version of the schema
        schema: The schema at this version
        timestamp: When this version was created
        author: Who created this version
        description: Description of changes in this version
        checksum: Checksum for integrity verification
    """

    schema_name: str
    version: SemanticVersion
    schema: Schema
    timestamp: datetime = field(default_factory=datetime.utcnow)
    author: str = "unknown"
    description: str = ""
    checksum: str | None = None


@dataclass(frozen=True)
class VersionedSchema:
    """
    A schema with version information.

    Combines a Schema with its version and metadata.

    Attributes:
        schema: The underlying schema
        version: Semantic version of this schema
        history: Version history of this schema
        backward_compatible: List of compatible versions
        deprecation_warning: Optional deprecation message
    """

    schema: Schema
    version: SemanticVersion = field(default_factory=lambda: SemanticVersion(1, 0, 0))
    history: tuple[SchemaHistoryEntry, ...] = field(default_factory=tuple)
    backward_compatible: tuple[SemanticVersion, ...] = field(default_factory=tuple)
    deprecation_warning: str | None = None

    def __post_init__(self) -> None:
        """Validate versioned schema."""
        # Schema name and version name can differ; no validation needed
        # This is handled during schema registration

    @property
    def name(self) -> str:
        """Get the schema name."""
        return self.schema.name


@dataclass
class SchemaHistory:
    """
    Manages the history and evolution of schemas.

    SchemaHistory tracks all versions of schemas, provides
    migration planning, and enables rollback to previous versions.

    Attributes:
        entries: All history entries
        enabled: Whether history tracking is enabled
    """

    entries: dict[str, list[SchemaHistoryEntry]] = field(default_factory=dict)
    enabled: bool = True

    def add_entry(self, entry: SchemaHistoryEntry) -> None:
        """
        Add a history entry.

        Args:
            entry: The history entry to add
        """
        if not self.enabled:
            return

        schema_key = entry.schema_name
        if schema_key not in self.entries:
            self.entries[schema_key] = []

        self.entries[schema_key].append(entry)

    def get_latest_version(self, schema_name: str) -> SchemaHistoryEntry | None:
        """
        Get the latest version of a schema.

        Args:
            schema_name: Name of the schema

        Returns:
            Latest history entry or None if not found
        """
        entries = self.entries.get(schema_name, [])
        if not entries:
            return None

        # Return the most recent entry
        return max(entries, key=lambda e: e.timestamp)

    def get_version(self, schema_name: str, version: SemanticVersion) -> SchemaHistoryEntry | None:
        """
        Get a specific version of a schema.

        Args:
            schema_name: Name of the schema
            version: Version to retrieve

        Returns:
            History entry or None if not found
        """
        entries = self.entries.get(schema_name, [])
        for entry in entries:
            if entry.version == version:
                return entry
        return None

    def get_all_versions(self, schema_name: str) -> list[SchemaHistoryEntry]:
        """
        Get all versions of a schema.

        Args:
            schema_name: Name of the schema

        Returns:
            List of all history entries, oldest first
        """
        entries = self.entries.get(schema_name, [])
        return sorted(entries, key=lambda e: e.timestamp)

    def rollback_to(self, schema_name: str, version: SemanticVersion) -> SchemaHistoryEntry | None:
        """
        Rollback a schema to a previous version.

        Args:
            schema_name: Name of the schema
            version: Version to rollback to

        Returns:
            The rolled-back history entry

        Raises:
            ValueError: If version not found
        """
        entry = self.get_version(schema_name, version)
        if entry is None:
            msg = f"Version {version} not found for schema {schema_name}"
            raise ValueError(msg)

        # Create a new entry for the rollback
        rollback_entry = SchemaHistoryEntry(
            schema_name=schema_name,
            version=version.next_patch(prerelease="rollback"),
            schema=entry.schema,
            author="rollback",
            description=f"Rollback to version {version}",
        )

        self.add_entry(rollback_entry)
        return rollback_entry

    def to_dict(self) -> dict[str, Any]:
        """Convert history to dictionary representation."""
        return {
            "enabled": self.enabled,
            "schemas": {
                name: [
                    {
                        "version": str(entry.version),
                        "timestamp": entry.timestamp.isoformat(),
                        "author": entry.author,
                        "description": entry.description,
                    }
                    for entry in entries
                ]
                for name, entries in self.entries.items()
            },
        }


# =============================================================================
# Schema Evolution Services
# =============================================================================


class BreakingChangeDetector:
    """
    Detects breaking changes in schema evolutions.

    Analyzes schema changes and identifies potential issues with
    backward compatibility.
    """

    def detect(self, old_schema: Schema, new_schema: Schema) -> SchemaDiff:
        """
        Detect all changes between two schemas.

        Args:
            old_schema: The original schema
            new_schema: The modified schema

        Returns:
            SchemaDiff with all detected changes
        """
        changes: list[SchemaChange] = []

        # Build field maps
        old_fields = {f.name: f for f in old_schema.fields}
        new_fields = {f.name: f for f in new_schema.fields}

        # Detect added fields
        for field_name in new_fields:
            if field_name not in old_fields:
                changes.append(
                    SchemaChange(
                        change_type=ChangeType.FIELD_ADDED,
                        field_name=field_name,
                        new_field=new_fields[field_name],
                        description=f"Field '{field_name}' was added",
                        is_breaking=False,
                        severity=BreakingChangeSeverity.NONE,
                    )
                )

        # Detect removed fields
        for field_name in old_fields:
            if field_name not in new_fields:
                changes.append(
                    SchemaChange(
                        change_type=ChangeType.FIELD_REMOVED,
                        field_name=field_name,
                        old_field=old_fields[field_name],
                        description=f"Field '{field_name}' was removed",
                        is_breaking=True,
                        severity=BreakingChangeSeverity.MAJOR,
                    )
                )

        # Detect field modifications
        for field_name in old_fields:
            if field_name in new_fields:
                old_field = old_fields[field_name]
                new_field = new_fields[field_name]
                field_changes = self._detect_field_changes(field_name, old_field, new_field)
                changes.extend(field_changes)

        # Separate breaking and non-breaking changes
        breaking_changes = tuple(c for c in changes if c.is_breaking)
        non_breaking_changes = tuple(c for c in changes if not c.is_breaking)

        # Determine overall severity
        severity = BreakingChangeSeverity.NONE
        if breaking_changes:
            severity = BreakingChangeSeverity.MAJOR

        return SchemaDiff(
            old_schema=old_schema,
            new_schema=new_schema,
            changes=tuple(changes),
            breaking_changes=breaking_changes,
            non_breaking_changes=non_breaking_changes,
            has_breaking_changes=bool(breaking_changes),
            severity=severity,
        )

    def _detect_field_changes(
        self, field_name: str, old_field: SchemaField, new_field: SchemaField
    ) -> list[SchemaChange]:
        """
        Detect changes between two field definitions.

        Args:
            field_name: Name of the field
            old_field: Original field definition
            new_field: New field definition

        Returns:
            List of detected changes
        """
        changes: list[SchemaChange] = []

        # Check data type changes
        if old_field.data_type != new_field.data_type:
            changes.append(
                SchemaChange(
                    change_type=ChangeType.FIELD_TYPE_CHANGED,
                    field_name=field_name,
                    old_field=old_field,
                    new_field=new_field,
                    description=f"Field '{field_name}' type changed from {old_field.data_type.name} to {new_field.data_type.name}",
                    is_breaking=True,
                    severity=BreakingChangeSeverity.MAJOR,
                )
            )

        # Check required changes (required -> optional is safe)
        if old_field.required and not new_field.required:
            changes.append(
                SchemaChange(
                    change_type=ChangeType.FIELD_REQUIRED_CHANGED,
                    field_name=field_name,
                    old_field=old_field,
                    new_field=new_field,
                    description=f"Field '{field_name}' changed from required to optional",
                    is_breaking=False,
                    severity=BreakingChangeSeverity.NONE,
                )
            )
        elif not old_field.required and new_field.required:
            changes.append(
                SchemaChange(
                    change_type=ChangeType.FIELD_REQUIRED_CHANGED,
                    field_name=field_name,
                    old_field=old_field,
                    new_field=new_field,
                    description=f"Field '{field_name}' changed from optional to required",
                    is_breaking=True,
                    severity=BreakingChangeSeverity.MAJOR,
                )
            )

        # Check nullability changes (non-nullable -> nullable is safe)
        if not old_field.nullable and new_field.nullable:
            changes.append(
                SchemaChange(
                    change_type=ChangeType.FIELD_NULLABILITY_CHANGED,
                    field_name=field_name,
                    old_field=old_field,
                    new_field=new_field,
                    description=f"Field '{field_name}' changed from non-nullable to nullable",
                    is_breaking=False,
                    severity=BreakingChangeSeverity.NONE,
                )
            )
        elif old_field.nullable and not new_field.nullable:
            changes.append(
                SchemaChange(
                    change_type=ChangeType.FIELD_NULLABILITY_CHANGED,
                    field_name=field_name,
                    old_field=old_field,
                    new_field=new_field,
                    description=f"Field '{field_name}' changed from nullable to non-nullable",
                    is_breaking=True,
                    severity=BreakingChangeSeverity.MAJOR,
                )
            )

        # Check constraint changes
        if old_field.constraints != new_field.constraints:
            changes.append(
                SchemaChange(
                    change_type=ChangeType.FIELD_CONSTRAINT_CHANGED,
                    field_name=field_name,
                    old_field=old_field,
                    new_field=new_field,
                    description=f"Field '{field_name}' constraints changed",
                    is_breaking=self._is_constraint_breaking(
                        old_field.constraints, new_field.constraints
                    ),
                    severity=BreakingChangeSeverity.MINOR,
                )
            )

        return changes

    def _is_constraint_breaking(
        self, old_constraints: Mapping[str, Any], new_constraints: Mapping[str, Any]
    ) -> bool:
        """
        Determine if constraint changes are breaking.

        Args:
            old_constraints: Original constraints
            new_constraints: New constraints

        Returns:
            True if changes are breaking, False otherwise
        """
        # If min_value increases, it's breaking (old values may violate)
        if "min_value" in old_constraints and "min_value" in new_constraints:
            if new_constraints["min_value"] > old_constraints["min_value"]:
                return True

        # If max_value decreases, it's breaking (old values may violate)
        if "max_value" in old_constraints and "max_value" in new_constraints:
            if new_constraints["max_value"] < old_constraints["max_value"]:
                return True

        # If max_length decreases, it's breaking
        if "max_length" in old_constraints and "max_length" in new_constraints:
            if new_constraints["max_length"] < old_constraints["max_length"]:
                return True

        return False


class MigrationPlanner:
    """
    Generates migration plans from schema diffs.

    Automatically creates step-by-step migration plans to transform
    data from one schema version to another.
    """

    def generate(
        self,
        diff: SchemaDiff,
        custom_migrations: dict[str, MigrationFn] | None = None,
    ) -> MigrationPlan:
        """
        Generate a migration plan from a schema diff.

        Args:
            diff: The schema diff to generate a plan for
            custom_migrations: Optional custom migration functions

        Returns:
            A MigrationPlan with steps to execute
        """
        steps: list[MigrationStep] = []

        for change in diff.changes:
            step = self._change_to_step(change)
            if step:
                steps.append(step)

        # Apply custom migrations if provided
        if custom_migrations:
            steps = self._apply_custom_migrations(steps, custom_migrations)

        # Determine rollback possibility and risk
        rollback_possible = not any(
            c.change_type == ChangeType.FIELD_REMOVED for c in diff.breaking_changes
        )

        estimated_risk = "low"
        if diff.has_breaking_changes:
            estimated_risk = "high"
        elif len(steps) > 5:
            estimated_risk = "medium"

        return MigrationPlan(
            from_version=diff.old_version or SemanticVersion(1, 0, 0),
            to_version=diff.new_version or SemanticVersion(1, 0, 0),
            from_schema=diff.old_schema,
            to_schema=diff.new_schema,
            steps=tuple(steps),
            estimated_risk=estimated_risk,
            rollback_possible=rollback_possible,
            custom_migrations=custom_migrations or {},
        )

    def _change_to_step(self, change: SchemaChange) -> MigrationStep | None:
        """
        Convert a schema change to a migration step.

        Args:
            change: The schema change

        Returns:
            MigrationStep or None if no step needed
        """
        if change.change_type == ChangeType.FIELD_ADDED:
            if change.new_field:
                return MigrationStep(
                    step_type="add_field",
                    description=f"Add new field '{change.field_name}' with default value",
                    field_name=change.field_name,
                    new_value=self._get_default_value(change.new_field),
                )

        elif change.change_type == ChangeType.FIELD_REMOVED:
            return MigrationStep(
                step_type="remove_field",
                description=f"Remove field '{change.field_name}'",
                field_name=change.field_name,
            )

        elif change.change_type == ChangeType.FIELD_TYPE_CHANGED:
            if change.old_field and change.new_field:
                return MigrationStep(
                    step_type="transform_field",
                    description=f"Transform field '{change.field_name}' from {change.old_field.data_type.name} to {change.new_field.data_type.name}",
                    field_name=change.field_name,
                    old_value=change.old_field.data_type,
                    new_value=change.new_field.data_type,
                )

        elif change.change_type == ChangeType.FIELD_REQUIRED_CHANGED:
            if change.old_field and change.new_field:
                if not change.old_field.required and change.new_field.required:
                    return MigrationStep(
                        step_type="validate_field",
                        description=f"Validate that '{change.field_name}' is not null",
                        field_name=change.field_name,
                    )

        return None

    def _apply_custom_migrations(
        self, steps: list[MigrationStep], custom_migrations: dict[str, MigrationFn]
    ) -> list[MigrationStep]:
        """
        Apply custom migration functions to steps.

        Args:
            steps: Original steps
            custom_migrations: Custom migration functions

        Returns:
            Modified steps with custom migrations
        """
        result: list[MigrationStep] = []
        for step in steps:
            if step.field_name and step.field_name in custom_migrations:
                result.append(
                    MigrationStep(
                        step_type=step.step_type,
                        description=f"Custom migration for '{step.field_name}'",
                        field_name=step.field_name,
                        migration_fn=custom_migrations[step.field_name],
                    )
                )
            else:
                result.append(step)
        return result

    def _get_default_value(self, field: SchemaField) -> Any:
        """
        Get a default value for a field type.

        Args:
            field: The schema field

        Returns:
            Default value for the field type
        """
        if field.data_type.name == "STRING":
            return ""
        elif field.data_type.name == "INTEGER":
            return 0
        elif field.data_type.name == "FLOAT":
            return 0.0
        elif field.data_type.name == "BOOLEAN":
            return False
        elif field.data_type.name == "ARRAY":
            return []
        elif field.data_type.name == "OBJECT":
            return {}
        return None


class BackwardCompatibilityChecker:
    """
    Validates backward compatibility of schema changes.

    Ensures that new schemas are compatible with existing data
    and consumers.
    """

    def is_backward_compatible(self, old_schema: Schema, new_schema: Schema) -> bool:
        """
        Check if new schema is backward compatible with old schema.

        Args:
            old_schema: The original schema
            new_schema: The modified schema

        Returns:
            True if backward compatible, False otherwise
        """
        diff = BreakingChangeDetector().detect(old_schema, new_schema)
        return not diff.has_breaking_changes

    def get_compatibility_report(self, old_schema: Schema, new_schema: Schema) -> str:
        """
        Get a detailed compatibility report.

        Args:
            old_schema: The original schema
            new_schema: The modified schema

        Returns:
            Formatted compatibility report
        """
        diff = BreakingChangeDetector().detect(old_schema, new_schema)

        if not diff.has_breaking_changes:
            return "✓ Backward compatible: No breaking changes detected."

        report_lines = ["✗ Not backward compatible:"]
        for change in diff.breaking_changes:
            report_lines.append(f"  - {change}")

        return "\n".join(report_lines)


# =============================================================================
# Schema Version Decorator
# =============================================================================


def schema_version(
    version: str,
    description: str | None = None,
    author: str = "unknown",
    track_in_history: bool = True,
) -> Callable[[type], type]:
    """
    Decorator to add version information to schema classes.

    This decorator works with @define_schema to track schema versions
    and automatically register them in the schema history.

    Can be used as:

        >>> @define_schema
        >>> @schema_version("1.2.0", description="Add email field")
        >>> class UserSchema:
        ...     id: Integer = Integer()
        ...     email: String = String()

    Args:
        version: Semantic version string (e.g., "1.2.0")
        description: Description of changes in this version
        author: Who created this version
        track_in_history: Whether to track this version in history

    Returns:
        Decorator function
    """

    def decorator(cls: type) -> type:
        """Apply version information to the schema class."""
        # Parse version
        ver = SemanticVersion.parse(version)

        # Attach version as class attribute
        cls.__schema_version__ = ver  # type: ignore[attr-defined]
        cls.__schema_version_str__ = version  # type: ignore[attr-defined]
        cls.__schema_author__ = author  # type: ignore[attr-defined]
        cls.__schema_description__ = description or ""  # type: ignore[attr-defined]
        cls.__schema_track_in_history__ = track_in_history  # type: ignore[attr-defined]

        # Get the schema from the define_schema decorator
        if hasattr(cls, "_schema"):
            schema = cls._schema

            # Create a VersionedSchema
            versioned = VersionedSchema(
                schema=schema,
                version=ver,
            )

            # Attach versioned schema
            cls._versioned_schema = versioned  # type: ignore[attr-defined]

            # If history tracking is enabled, add to global history
            if track_in_history:
                history = get_schema_history()
                history.add_entry(
                    SchemaHistoryEntry(
                        schema_name=schema.name,
                        version=ver,
                        schema=schema,
                        author=author,
                        description=description or "",
                    )
                )

        return cls

    return decorator


# =============================================================================
# Global Schema History
# =============================================================================

# Global instance for schema history
_global_schema_history: SchemaHistory | None = None


def get_schema_history() -> SchemaHistory:
    """
    Get the global schema history instance.

    Returns:
        Global SchemaHistory instance
    """
    global _global_schema_history
    if _global_schema_history is None:
        _global_schema_history = SchemaHistory()
    return _global_schema_history


def reset_schema_history() -> None:
    """Reset the global schema history (useful for testing)."""
    global _global_schema_history
    _global_schema_history = None


# =============================================================================
# Re-exports
# =============================================================================

__all__ = [
    # Types
    "SemanticVersion",
    "SchemaChange",
    "SchemaDiff",
    "MigrationStep",
    "MigrationPlan",
    "SchemaHistoryEntry",
    "VersionedSchema",
    "SchemaHistory",
    # Enums
    "ChangeType",
    "BreakingChangeSeverity",
    # Services
    "BreakingChangeDetector",
    "MigrationPlanner",
    "BackwardCompatibilityChecker",
    # Decorator
    "schema_version",
    # Utilities
    "get_schema_history",
    "reset_schema_history",
]
