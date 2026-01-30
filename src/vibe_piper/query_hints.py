"""
Query optimization hints for SQL-based operations in Vibe Piper.

This module provides:
- Query hint annotations
- Hint-based optimization strategies
- Integration with SQL query builders
"""

from dataclasses import dataclass
from enum import Enum, auto

# =============================================================================
# Enum Types
# =============================================================================


class JoinStrategy(Enum):
    """Join optimization strategies."""

    NESTED_LOOP = auto()
    HASH_JOIN = auto()
    MERGE_JOIN = auto()


class IndexHintType(Enum):
    """Index usage hints."""

    USE_INDEX = auto()
    FORCE_INDEX = auto()
    IGNORE_INDEX = auto()


class ScanHintType(Enum):
    """Table scan hints."""

    SEQUENTIAL = auto()
    PARALLEL = auto()
    SKIP_SCANNED = auto()


# =============================================================================
# Query Hint Data Structures
# =============================================================================


@dataclass
class IndexHint:
    """
    Hint for index usage.

    Attributes:
        index_name: Name of index to use
        hint_type: Type of hint (USE, FORCE, IGNORE)
    """

    index_name: str
    hint_type: IndexHintType = IndexHintType.USE_INDEX

    def to_sql(self) -> str:
        """
        Convert to SQL hint syntax.

        Returns:
            SQL hint string
        """
        if self.hint_type == IndexHintType.USE_INDEX:
            return f"USE INDEX ({self.index_name})"
        elif self.hint_type == IndexHintType.FORCE_INDEX:
            return f"FORCE INDEX ({self.index_name})"
        else:
            return f"IGNORE INDEX ({self.index_name})"

    def __str__(self) -> str:
        """String representation."""
        return self.to_sql()


@dataclass
class JoinHint:
    """
    Hint for join optimization.

    Attributes:
        strategy: Join strategy to use
        table_name: Table to apply hint to
    """

    strategy: JoinStrategy
    table_name: str

    def to_sql(self) -> str:
        """
        Convert to SQL hint syntax.

        Returns:
            SQL hint string (database-specific)
        """
        # This is database-specific, so we provide a generic format
        hints = {
            JoinStrategy.NESTED_LOOP: "NESTED LOOP",
            JoinStrategy.HASH_JOIN: "HASH JOIN",
            JoinStrategy.MERGE_JOIN: "MERGE JOIN",
        }
        return f"{hints[self.strategy]} {self.table_name}"

    def __str__(self) -> str:
        """String representation."""
        return self.to_sql()


@dataclass
class ScanHint:
    """
    Hint for table scan optimization.

    Attributes:
        hint_type: Type of scan hint
        table_name: Table to apply hint to
    """

    hint_type: ScanHintType
    table_name: str

    def to_sql(self) -> str:
        """
        Convert to SQL hint syntax.

        Returns:
            SQL hint string
        """
        hints = {
            ScanHintType.SEQUENTIAL: "SEQUENTIAL",
            ScanHintType.PARALLEL: "PARALLEL",
            ScanHintType.SKIP_SCANNED: "SKIP SCANNED",
        }
        return f"{hints[self.hint_type]} SCAN {self.table_name}"

    def __str__(self) -> str:
        """String representation."""
        return self.to_sql()


@dataclass
class LimitHint:
    """
    Hint for result limiting.

    Attributes:
        limit: Maximum number of rows to return
        offset: Number of rows to skip
    """

    limit: int
    offset: int = 0

    def to_sql(self) -> str:
        """
        Convert to SQL syntax.

        Returns:
            SQL LIMIT/OFFSET clause
        """
        sql = f"LIMIT {self.limit}"
        if self.offset > 0:
            sql += f" OFFSET {self.offset}"
        return sql

    def __str__(self) -> str:
        """String representation."""
        return self.to_sql()


@dataclass
class ParallelHint:
    """
    Hint for parallel execution.

    Attributes:
        degree: Number of parallel workers (0 = auto-detect)
    """

    degree: int = 0

    def to_sql(self) -> str:
        """
        Convert to SQL hint syntax.

        Returns:
            SQL PARALLEL hint
        """
        if self.degree == 0:
            return "PARALLEL"
        return f"PARALLEL {self.degree}"

    def __str__(self) -> str:
        """String representation."""
        return self.to_sql()


@dataclass
class MaterializeHint:
    """
    Hint for materialization strategy.

    Attributes:
        materialize: Whether to materialize result
    """

    materialize: bool = True

    def to_sql(self) -> str:
        """
        Convert to SQL hint syntax.

        Returns:
            SQL MATERIALIZED hint
        """
        if self.materialize:
            return "MATERIALIZED"
        return "NOT MATERIALIZED"

    def __str__(self) -> str:
        """String representation."""
        return self.to_sql()


# =============================================================================
# Query Hints Container
# =============================================================================


@dataclass
class QueryHints:
    """
    Container for all query optimization hints.

    Attributes:
        index_hints: List of index usage hints
        join_hints: List of join strategy hints
        scan_hints: List of table scan hints
        limit_hint: Limit/offset hint
        parallel_hint: Parallel execution hint
        materialize_hint: Materialization hint
        custom_hints: Custom database-specific hints
    """

    index_hints: list[IndexHint] | None = None
    join_hints: list[JoinHint] | None = None
    scan_hints: list[ScanHint] | None = None
    limit_hint: LimitHint | None = None
    parallel_hint: ParallelHint | None = None
    materialize_hint: MaterializeHint | None = None
    custom_hints: dict[str, str] | None = None

    def to_sql(self, dialect: str = "postgresql") -> str:
        """
        Convert all hints to SQL syntax.

        Args:
            dialect: SQL dialect (postgresql, mysql, sqlite, etc.)

        Returns:
            SQL hints string
        """
        hints = []

        # Add index hints (database-specific)
        if self.index_hints:
            if dialect == "mysql":
                for hint in self.index_hints:
                    hints.append(hint.to_sql())

        # Add join hints
        if self.join_hints:
            for hint in self.join_hints:
                hints.append(hint.to_sql())

        # Add scan hints
        if self.scan_hints:
            for hint in self.scan_hints:
                hints.append(hint.to_sql())

        # Add parallel hint
        if self.parallel_hint:
            if dialect in ("postgresql", "oracle"):
                hints.append(self.parallel_hint.to_sql())

        # Add materialize hint
        if self.materialize_hint:
            if dialect == "postgresql":
                hints.append(self.materialize_hint.to_sql())

        # Add custom hints
        if self.custom_hints:
            hints.extend(f"{k} {v}" for k, v in self.custom_hints.items())

        # Combine hints
        if hints:
            return "/*+ " + " ".join(hints) + " */"

        return ""

    def __str__(self) -> str:
        """String representation."""
        return self.to_sql()


# =============================================================================
# Builder
# =============================================================================


class QueryHintsBuilder:
    """
    Fluent builder for query hints.

    Provides a convenient way to construct query hints.

    Example:
        Build hints for a query::

            hints = (
                QueryHintsBuilder()
                .use_index("idx_user_id")
                .hash_join("orders")
                .parallel(4)
                .build()
            )
    """

    def __init__(self) -> None:
        """Initialize hints builder."""
        self._hints = QueryHints()

    def use_index(self, index_name: str) -> "QueryHintsBuilder":
        """
        Add USE INDEX hint.

        Args:
            index_name: Name of index to use

        Returns:
            Self for chaining
        """
        if self._hints.index_hints is None:
            self._hints.index_hints = []
        self._hints.index_hints.append(IndexHint(index_name, IndexHintType.USE_INDEX))
        return self

    def force_index(self, index_name: str) -> "QueryHintsBuilder":
        """
        Add FORCE INDEX hint.

        Args:
            index_name: Name of index to force

        Returns:
            Self for chaining
        """
        if self._hints.index_hints is None:
            self._hints.index_hints = []
        self._hints.index_hints.append(IndexHint(index_name, IndexHintType.FORCE_INDEX))
        return self

    def ignore_index(self, index_name: str) -> "QueryHintsBuilder":
        """
        Add IGNORE INDEX hint.

        Args:
            index_name: Name of index to ignore

        Returns:
            Self for chaining
        """
        if self._hints.index_hints is None:
            self._hints.index_hints = []
        self._hints.index_hints.append(IndexHint(index_name, IndexHintType.IGNORE_INDEX))
        return self

    def hash_join(self, table_name: str) -> "QueryHintsBuilder":
        """
        Add HASH JOIN hint.

        Args:
            table_name: Table to apply hint to

        Returns:
            Self for chaining
        """
        if self._hints.join_hints is None:
            self._hints.join_hints = []
        self._hints.join_hints.append(JoinHint(JoinStrategy.HASH_JOIN, table_name))
        return self

    def merge_join(self, table_name: str) -> "QueryHintsBuilder":
        """
        Add MERGE JOIN hint.

        Args:
            table_name: Table to apply hint to

        Returns:
            Self for chaining
        """
        if self._hints.join_hints is None:
            self._hints.join_hints = []
        self._hints.join_hints.append(JoinHint(JoinStrategy.MERGE_JOIN, table_name))
        return self

    def nested_loop_join(self, table_name: str) -> "QueryHintsBuilder":
        """
        Add NESTED LOOP JOIN hint.

        Args:
            table_name: Table to apply hint to

        Returns:
            Self for chaining
        """
        if self._hints.join_hints is None:
            self._hints.join_hints = []
        self._hints.join_hints.append(JoinHint(JoinStrategy.NESTED_LOOP, table_name))
        return self

    def parallel_scan(self, table_name: str) -> "QueryHintsBuilder":
        """
        Add PARALLEL SCAN hint.

        Args:
            table_name: Table to apply hint to

        Returns:
            Self for chaining
        """
        if self._hints.scan_hints is None:
            self._hints.scan_hints = []
        self._hints.scan_hints.append(ScanHint(ScanHintType.PARALLEL, table_name))
        return self

    def sequential_scan(self, table_name: str) -> "QueryHintsBuilder":
        """
        Add SEQUENTIAL SCAN hint.

        Args:
            table_name: Table to apply hint to

        Returns:
            Self for chaining
        """
        if self._hints.scan_hints is None:
            self._hints.scan_hints = []
        self._hints.scan_hints.append(ScanHint(ScanHintType.SEQUENTIAL, table_name))
        return self

    def limit(self, limit: int, offset: int = 0) -> "QueryHintsBuilder":
        """
        Add LIMIT/OFFSET hint.

        Args:
            limit: Maximum number of rows
            offset: Number of rows to skip

        Returns:
            Self for chaining
        """
        self._hints.limit_hint = LimitHint(limit, offset)
        return self

    def parallel(self, degree: int = 0) -> "QueryHintsBuilder":
        """
        Add PARALLEL hint.

        Args:
            degree: Number of parallel workers (0 = auto-detect)

        Returns:
            Self for chaining
        """
        self._hints.parallel_hint = ParallelHint(degree)
        return self

    def materialize(self, materialize: bool = True) -> "QueryHintsBuilder":
        """
        Add MATERIALIZED hint.

        Args:
            materialize: Whether to materialize result

        Returns:
            Self for chaining
        """
        self._hints.materialize_hint = MaterializeHint(materialize)
        return self

    def custom(self, name: str, value: str) -> "QueryHintsBuilder":
        """
        Add custom hint.

        Args:
            name: Hint name
            value: Hint value

        Returns:
            Self for chaining
        """
        if self._hints.custom_hints is None:
            self._hints.custom_hints = {}
        self._hints.custom_hints[name] = value
        return self

    def build(self) -> QueryHints:
        """
        Build QueryHints object.

        Returns:
            QueryHints with all configured hints
        """
        return self._hints


# =============================================================================
# Decorators
# =============================================================================


def with_query_hints(hints, dialect: str = "postgresql"):
    """
    Decorator to apply query hints to a function.

    The decorated function should return a SQL string.
    The hints will be injected into the SQL.

    Args:
        hints: Query hints to apply (QueryHints or QueryHintsBuilder)
        dialect: SQL dialect for hint syntax

    Returns:
        Decorator function

    Example:
        Add hints to a SQL function::

            hints = QueryHintsBuilder().use_index("idx_id").parallel(4).build()

            @with_query_hints(hints, dialect="postgresql")
            def get_users_sql():
                return "SELECT * FROM users WHERE id = ?"

            # Result: "/*+ PARALLEL 4 */ SELECT * FROM users WHERE id = ?"
    """

    def decorator(fn):
        def wrapper(*args, **kwargs):
            # Get hints if builder
            actual_hints = hints.build() if isinstance(hints, QueryHintsBuilder) else hints

            # Get SQL from function
            sql = fn(*args, **kwargs)

            # Inject hints at beginning of SELECT
            hint_string = actual_hints.to_sql(dialect)
            if hint_string and sql.strip().upper().startswith("SELECT"):
                # Insert hints after SELECT
                sql = sql.strip()
                sql = "SELECT " + hint_string + " " + sql[6:].lstrip()
            elif hint_string:
                # Prepend hints
                sql = hint_string + " " + sql

            return sql

        return wrapper

    return decorator
