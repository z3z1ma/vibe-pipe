"""
SQL Asset Decorator for Vibe Piper

This module provides the @sql_asset decorator for creating SQL-based transformations
with Jinja2 templating, parameter binding, SQL validation, and multi-dialect support.
"""

import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

from vibe_piper.types import (
    Asset,
    AssetType,
    MaterializationStrategy,
    Schema,
)

# =============================================================================
# Optional Dependencies
# =============================================================================

if TYPE_CHECKING:
    from jinja2 import StrictUndefined, Template
    from sqlglot.dialects import Dialect as SQLDialect
    from sqlglot.errors import ParseError as SQLParseError

_JINJA2_AVAILABLE = False
_SQLGLOT_AVAILABLE = False
Template: Any = None  # type: ignore
StrictUndefined: Any = None  # type: ignore
SQLDialect: Any = None  # type: ignore
SQLParseError: Any = Exception  # type: ignore
Postgres: Any = None  # type: ignore
MySQL: Any = None  # type: ignore
Snowflake: Any = None  # type: ignore
BigQuery: Any = None  # type: ignore

# Try to import Jinja2 for templating at runtime
try:
    from jinja2 import StrictUndefined, Template  # noqa: F401

    _JINJA2_AVAILABLE = True
except ImportError:
    _JINJA2_AVAILABLE = False

# Try to import SQLGlot for SQL validation at runtime
try:
    from sqlglot import parse  # noqa: F401
    from sqlglot.dialects import (  # noqa: F401
        BigQuery,
        MySQL,
        Postgres,
        Snowflake,
    )
    from sqlglot.errors import ParseError as SQLParseError  # noqa: F401

    _SQLGLOT_AVAILABLE = True
except ImportError:
    _SQLGLOT_AVAILABLE = False

# =============================================================================
# Logger
# =============================================================================

logger = logging.getLogger(__name__)

# =============================================================================
# Type Variables
# =============================================================================

P = ParamSpec("P")
T = TypeVar("T")

# =============================================================================
# SQL Dialect Mapping
# =============================================================================

SQL_DIALECTS: dict[str, Any] = {
    "postgresql": None,
    "postgres": None,
    "mysql": None,
    "snowflake": None,
    "bigquery": None,
}

if _SQLGLOT_AVAILABLE:
    SQL_DIALECTS = {
        "postgresql": Postgres,
        "postgres": Postgres,
        "mysql": MySQL,
        "snowflake": Snowflake,
        "bigquery": BigQuery,
    }

# =============================================================================
# SQL Validation
# =============================================================================


@dataclass(frozen=True)
class SQLValidationResult:
    """Result of SQL validation."""

    is_valid: bool
    errors: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()


def validate_sql(sql: str, dialect: str = "postgresql") -> SQLValidationResult:
    """
    Validate SQL syntax and structure.

    Args:
        sql: SQL string to validate
        dialect: SQL dialect (postgresql, mysql, snowflake, bigquery)

    Returns:
        SQLValidationResult indicating if SQL is valid and any errors/warnings

    Raises:
        ImportError: If sqlglot is not installed
    """
    if not _SQLGLOT_AVAILABLE:
        msg = "sqlglot is not installed. Install it with: pip install sqlglot"
        raise ImportError(msg)

    # Normalize dialect name
    dialect_lower = dialect.lower()
    if dialect_lower not in SQL_DIALECTS:
        valid_dialects = list(SQL_DIALECTS.keys())
        msg = f"Unsupported SQL dialect: {dialect!r}. Supported dialects: {valid_dialects}"
        raise ValueError(msg)

    dialect_class = SQL_DIALECTS[dialect_lower]

    errors: list[str] = []
    warnings: list[str] = []

    try:
        # Parse SQL using sqlglot
        from sqlglot import parse as sqlglot_parse

        parsed = sqlglot_parse(sql, dialect=dialect_class)

        if parsed is None:
            errors.append("Failed to parse SQL: unknown syntax error")
            return SQLValidationResult(
                is_valid=False, errors=tuple(errors), warnings=tuple(warnings)
            )

        # Additional validation checks
        sql_upper = sql.upper()

        # Check for common dangerous patterns
        dangerous_patterns = [
            r"\bDROP\s+TABLE\b",
            r"\bTRUNCATE\b",
            r"\bDELETE\s+FROM\b",
            r"\bGRANT\s+.*\bTO\b",
            r"\bREVOKE\b",
        ]

        for pattern in dangerous_patterns:
            if re.search(pattern, sql_upper):
                warnings.append(f"Potentially dangerous SQL pattern detected: {pattern}")

        return SQLValidationResult(
            is_valid=len(errors) == 0,
            errors=tuple(errors),
            warnings=tuple(warnings),
        )

    except Exception as e:
        return SQLValidationResult(
            is_valid=False,
            errors=(f"Unexpected error validating SQL: {e}",),
            warnings=tuple(warnings),
        )

        # Additional validation checks
        sql_upper = sql.upper()

        # Check for common dangerous patterns
        dangerous_patterns = [
            r"\bDROP\s+TABLE\b",
            r"\bTRUNCATE\b",
            r"\bDELETE\s+FROM\b",
            r"\bGRANT\s+.*\bTO\b",
            r"\bREVOKE\b",
        ]

        for pattern in dangerous_patterns:
            if re.search(pattern, sql_upper):
                warnings.append(f"Potentially dangerous SQL pattern detected: {pattern}")

        return SQLValidationResult(
            is_valid=len(errors) == 0,
            errors=tuple(errors),
            warnings=tuple(warnings),
        )

    except Exception as e:
        return SQLValidationResult(
            is_valid=False,
            errors=(f"Unexpected error validating SQL: {e}",),
            warnings=tuple(warnings),
        )


# =============================================================================
# SQL Template Engine
# =============================================================================


@dataclass(frozen=True)
class SQLTemplateResult:
    """Result of SQL template rendering."""

    rendered_sql: str
    extracted_params: dict[str, Any]
    asset_dependencies: tuple[str, ...]


def render_sql_template(
    sql_template: str,
    context: dict[str, Any] | None = None,
) -> SQLTemplateResult:
    """
    Render SQL template using Jinja2 and extract parameters.

    Args:
        sql_template: SQL template string with Jinja2 syntax
        context: Context variables for template rendering

    Returns:
        SQLTemplateResult with rendered SQL, parameters, and dependencies

    Raises:
        ImportError: If Jinja2 is not installed
    """
    if not _JINJA2_AVAILABLE:
        msg = "Jinja2 is not installed. Install it with: pip install jinja2"
        raise ImportError(msg)

    context = context or {}

    try:
        # Create Jinja2 template with strict undefined handling
        template = Template(
            sql_template,
            undefined=StrictUndefined,
            trim_blocks=True,
            lstrip_blocks=True,
        )

        # Extract asset dependencies (variables used in {{ asset }} style)
        # We'll track variables that look like asset references
        asset_deps: set[str] = set()

        # Get all variables from template
        from jinja2 import meta

        ast = meta.parse(sql_template)
        variables = meta.find_undeclared_variables(ast)

        # Variables that look like asset references (no parameters passed)
        for var in variables:
            if var not in context:
                # Variable not provided, likely an asset dependency
                asset_deps.add(var)

        # Render the template
        rendered_sql = template.render(**context)

        # Extract parameters that should be bound safely
        # These are variables that were provided in context
        extracted_params = {k: v for k, v in context.items() if k not in asset_deps}

        return SQLTemplateResult(
            rendered_sql=rendered_sql,
            extracted_params=extracted_params,
            asset_dependencies=tuple(sorted(asset_deps)),
        )

    except Exception as e:
        msg = f"Failed to render SQL template: {e}"
        raise RuntimeError(msg) from e


# =============================================================================
# SQL Dependency Tracking
# =============================================================================


def extract_asset_dependencies(sql: str) -> tuple[str, ...]:
    """
    Extract asset dependencies from SQL template.

    Args:
        sql: SQL template string

    Returns:
        Tuple of asset names referenced in {{ asset }} syntax
    """
    # Pattern to match {{ asset_name }} style references
    pattern = r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}"

    matches = re.findall(pattern, sql)
    return tuple(sorted(set(matches)))


# =============================================================================
# SQL Asset Decorator
# =============================================================================


@dataclass(frozen=True)
class SQLOperator:
    """
    SQL operator for executing SQL queries.

    Attributes:
        sql_template: SQL template string
        dialect: SQL dialect to use
        sql_asset_name: Name of the SQL asset
        asset_dependencies: Names of upstream assets this SQL depends on
    """

    sql_template: str
    dialect: str
    sql_asset_name: str
    asset_dependencies: tuple[str, ...] = ()


def _create_sql_operator_from_function(
    func: Callable[..., str],
    name: str | None,
    dialect: str,
) -> SQLOperator:
    """
    Create a SQLOperator from a function that returns SQL.

    Args:
        func: Function that returns SQL template string
        name: Custom name for the SQL asset
        dialect: SQL dialect to use

    Returns:
        SQLOperator instance

    Raises:
        ValueError: If function doesn't return a string
    """
    sql_asset_name = name or func.__name__

    # Call the function to get the SQL template
    try:
        sql_template = func()
        if not isinstance(sql_template, str):
            msg = (
                f"SQL asset function '{sql_asset_name}' must return a string, "
                f"got {type(sql_template).__name__}"
            )
            raise ValueError(msg)
    except Exception as e:
        msg = f"Error calling SQL asset function '{sql_asset_name}': {e}"
        raise RuntimeError(msg) from e

    # Extract asset dependencies from SQL template
    asset_dependencies = extract_asset_dependencies(sql_template)

    return SQLOperator(
        sql_template=sql_template,
        dialect=dialect,
        sql_asset_name=sql_asset_name,
        asset_dependencies=asset_dependencies,
    )


class SQLAssetDecorator:
    """Decorator class for @sql_asset with configurable dialect."""

    def __call__(
        self,
        func_or_dialect: Callable[[], str] | str | None = None,
        **kwargs: Any,
    ) -> Asset | Callable[[Callable[[], str]], Asset]:
        """
        Decorator to create SQL-based assets.

        Can be used as:
        - @sql_asset (uses default postgresql dialect)
        - @sql_asset("mysql") (specify dialect)
        - @sql_asset(dialect="postgresql", ...) (with full config)

        Args:
            func_or_dialect: Either function to decorate (when using @sql_asset)
                             or a dialect string (when using @sql_asset(...))
            **kwargs: Additional keyword arguments (depends_on, io_manager, etc.)

        Returns:
            Either an Asset (when used as @sql_asset) or a decorator function
            (when used as @sql_asset(...))
        """
        # Extract parameters from kwargs
        dialect = kwargs.pop("dialect", "postgresql")
        depends_on = kwargs.pop("depends_on", None)
        io_manager = kwargs.pop("io_manager", None)
        materialization = kwargs.pop("materialization", None)
        schema = kwargs.pop("schema", None)
        description = kwargs.pop("description", None)
        metadata = kwargs.pop("metadata", None)
        config = kwargs.pop("config", None)
        name_param = kwargs.pop("name", None)

        # Case 1: @sql_asset (no parentheses) - func_or_dialect is function
        if callable(func_or_dialect):
            sql_operator = _create_sql_operator_from_function(
                func=func_or_dialect,
                name=name_param,
                dialect=dialect,
            )
            return _create_asset_from_sql_operator(
                sql_operator=sql_operator,
                name=name_param,
                depends_on=depends_on,
                io_manager=io_manager,
                materialization=materialization,
                schema=schema,
                description=description,
                metadata=metadata,
                config=config,
            )

        # Case 2: func_or_dialect is a string (dialect shorthand)
        if isinstance(func_or_dialect, str):
            dialect = func_or_dialect

        # Return decorator function
        def decorator(func: Callable[[], str]) -> Asset:
            sql_operator = _create_sql_operator_from_function(
                func=func,
                name=name_param,
                dialect=dialect,
            )
            return _create_asset_from_sql_operator(
                sql_operator=sql_operator,
                name=name_param,
                depends_on=depends_on,
                io_manager=io_manager,
                materialization=materialization,
                schema=schema,
                description=description,
                metadata=metadata,
                config=config,
            )

        return decorator


def _create_asset_from_sql_operator(
    sql_operator: SQLOperator,
    name: str | None,
    depends_on: tuple[str, ...] | None,
    io_manager: str | None,
    materialization: str | MaterializationStrategy | None,
    schema: Schema | None,
    description: str | None,
    metadata: dict[str, Any] | None,
    config: dict[str, Any] | None,
) -> Asset:
    """
    Create an Asset from a SQLOperator.

    Args:
        sql_operator: The SQL operator
        name: Custom asset name
        depends_on: Explicit dependencies
        io_manager: IO manager to use
        materialization: Materialization strategy
        schema: Output schema
        description: Asset description
        metadata: Asset metadata
        config: Asset config

    Returns:
        Asset instance
    """
    asset_name = name or sql_operator.sql_asset_name

    # Build asset config
    asset_config = config or {}

    # Add SQL-specific config
    asset_config = dict(asset_config)
    asset_config["dialect"] = sql_operator.dialect
    asset_config["sql_template"] = sql_operator.sql_template

    # Normalize depends_on
    if depends_on is None:
        # Use dependencies extracted from SQL template
        explicit_deps = sql_operator.asset_dependencies
    else:
        explicit_deps = depends_on

    # Store dependencies in config for the execution engine
    asset_config["depends_on"] = explicit_deps

    # Determine materialization
    if materialization is None:
        asset_materialization = MaterializationStrategy.VIEW
    elif isinstance(materialization, str):
        try:
            asset_materialization = MaterializationStrategy[materialization.upper()]
        except KeyError:
            valid = [s.name.lower() for s in MaterializationStrategy]
            msg = f"Invalid materialization strategy '{materialization}'. Must be one of: {valid}"
            raise ValueError(msg) from None
    else:
        asset_materialization = materialization

    # Generate URI based on dialect and asset type
    uri = f"{sql_operator.dialect}://{asset_name}"

    # Create the Asset
    return Asset(
        name=asset_name,
        asset_type=AssetType.VIEW,
        uri=uri,
        schema=schema,
        description=description,
        metadata=metadata or {},
        config=asset_config,
        io_manager=io_manager or "memory",
        materialization=asset_materialization,
    )


# Create an instance that can be used as a decorator
sql_asset = SQLAssetDecorator()

# =============================================================================
# Convenience Functions
# =============================================================================


def execute_sql_query(
    sql_template: str,
    connector: Any,
    params: dict[str, Any] | None = None,
    dialect: str = "postgresql",
) -> Any:
    """
    Execute a SQL query with parameter binding.

    Args:
        sql_template: SQL template string
        connector: Database connector instance
        params: Parameters for query
        dialect: SQL dialect

    Returns:
        Query results from the database

    Raises:
        ImportError: If Jinja2 is not installed
        RuntimeError: If template rendering fails
    """
    params = params or {}

    # Render the SQL template
    template_result = render_sql_template(sql_template, context=params)

    # Validate SQL before execution
    validation_result = validate_sql(template_result.rendered_sql, dialect=dialect)

    if not validation_result.is_valid:
        msg = f"SQL validation failed: {validation_result.errors}"
        raise ValueError(msg)

    if validation_result.warnings:
        for warning in validation_result.warnings:
            logger.warning(warning)

    # Execute the query using the connector
    # Note: Parameter binding is handled by the connector's execute_query method
    return connector.execute_query(
        template_result.rendered_sql, params=template_result.extracted_params
    )
