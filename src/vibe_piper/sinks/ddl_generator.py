"""
DDL Generator Utility

This module provides automatic DDL generation from Schema objects,
supporting multiple database backends (PostgreSQL, MySQL, Snowflake, BigQuery).

Key Features:
- Generate CREATE TABLE from Schema
- Generate UPSERT/MERGE statements based on upsert_key
- Handle data type mapping (DataType → DB-specific types)
- Generate proper column constraints (NOT NULL, UNIQUE, DEFAULT)
"""

from collections.abc import Mapping
from enum import Enum

from vibe_piper.types import DataType, Schema, SchemaField


class Dialect(Enum):
    """Supported database dialects for DDL generation."""

    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"


class DDLGenerator:
    """
    DDL generator for creating database objects from Schema.

    This generator handles:
    - CREATE TABLE statements with proper types and constraints
    - UPSERT statements (INSERT ON CONFLICT for PostgreSQL)
    - MERGE statements (for Snowflake/BigQuery)
    - Index generation from schema metadata

    Example:
        >>> schema = Schema(
        ...     name="users",
        ...     fields=(
        ...         SchemaField(name="id", data_type=DataType.INTEGER, required=True),
        ...         SchemaField(name="name", data_type=DataType.STRING),
        ...     )
        ... )
        >>> generator = DDLGenerator(Dialect.POSTGRESQL)
        >>> create_sql = generator.generate_create_table("users_table", schema)
        >>> print(create_sql)
        >>> CREATE TABLE users_table (
        ...     id INTEGER NOT NULL,
        ...     name VARCHAR
        ... );
    """

    def __init__(self, dialect: Dialect) -> None:
        """
        Initialize DDL generator for a specific dialect.

        Args:
            dialect: The database dialect to generate DDL for
        """
        self.dialect = dialect

    def generate_create_table(
        self,
        table_name: str,
        schema: Schema,
        schema_name: str | None = None,
    ) -> str:
        """
        Generate CREATE TABLE statement from Schema.

        Args:
            table_name: Name of the table to create
            schema: Schema object containing field definitions
            schema_name: Optional schema name (for qualified table names)

        Returns:
            CREATE TABLE SQL statement

        Raises:
            ValueError: If schema has no fields
        """
        if not schema.fields:
            msg = f"Cannot create table {table_name!r}: schema has no fields"
            raise ValueError(msg)

        # Build qualified table name
        qualified_name = self._qualify_table_name(table_name, schema_name)

        # Generate column definitions
        columns = []
        for field in schema.fields:
            column_def = self._generate_column_def(field)
            columns.append(column_def)

        # Combine into CREATE TABLE statement
        column_list = ",\n    ".join(columns)
        create_stmt = f"CREATE TABLE {qualified_name} (\n    {column_list}\n);"

        return create_stmt

    def generate_upsert(
        self,
        table_name: str,
        schema: Schema,
        upsert_key: str | list[str],
        schema_name: str | None = None,
    ) -> str:
        """
        Generate UPSERT/MERGE statement for data insertion.

        Args:
            table_name: Name of the target table
            schema: Schema object containing field definitions
            upsert_key: Column name(s) to use for conflict resolution
            schema_name: Optional schema name

        Returns:
            UPSERT/MERGE SQL statement

        Raises:
            ValueError: If upsert_key not found in schema fields
        """
        if not schema.fields:
            msg = f"Cannot generate upsert for table {table_name!r}: schema has no fields"
            raise ValueError(msg)

        # Normalize upsert_key to list
        if isinstance(upsert_key, str):
            upsert_keys = [upsert_key]
        else:
            upsert_keys = upsert_key

        # Validate upsert_keys exist in schema
        for key in upsert_keys:
            if not schema.has_field(key):
                msg = f"Upsert key {key!r} not found in schema fields"
                raise ValueError(msg)

        # Generate dialect-specific statement
        if self.dialect == Dialect.POSTGRESQL:
            return self._generate_postgres_upsert(table_name, schema, upsert_keys, schema_name)
        elif self.dialect in (Dialect.SNOWFLAKE, Dialect.BIGQUERY):
            return self._generate_merge_statement(table_name, schema, upsert_keys, schema_name)
        else:  # MySQL
            return self._generate_mysql_upsert(table_name, schema, upsert_keys, schema_name)

    def generate_drop_table(
        self,
        table_name: str,
        if_exists: bool = True,
        cascade: bool = False,
        schema_name: str | None = None,
    ) -> str:
        """
        Generate DROP TABLE statement.

        Args:
            table_name: Name of the table to drop
            if_exists: Include IF EXISTS clause
            cascade: Include CASCADE clause
            schema_name: Optional schema name

        Returns:
            DROP TABLE SQL statement
        """
        qualified_name = self._qualify_table_name(table_name, schema_name)

        if if_exists:
            drop_stmt = f"DROP TABLE IF EXISTS {qualified_name}"
        else:
            drop_stmt = f"DROP TABLE {qualified_name}"

        if cascade:
            drop_stmt += " CASCADE"

        return drop_stmt + ";"

    def generate_index(
        self,
        table_name: str,
        index_name: str,
        columns: list[str],
        unique: bool = False,
        schema_name: str | None = None,
    ) -> str:
        """
        Generate CREATE INDEX statement.

        Args:
            table_name: Name of the table
            index_name: Name for the index
            columns: List of column names to index
            unique: Whether index should be unique
            schema_name: Optional schema name

        Returns:
            CREATE INDEX SQL statement
        """
        qualified_name = self._qualify_table_name(table_name, schema_name)
        qualified_index_name = self._qualify_name(index_name, schema_name)

        index_type = "UNIQUE INDEX" if unique else "INDEX"
        column_list = ", ".join(columns)

        return f"CREATE {index_type} {qualified_index_name} ON {qualified_name} ({column_list});"

    def _qualify_table_name(self, table_name: str, schema_name: str | None) -> str:
        """Qualify table name with schema if provided."""
        if schema_name:
            return f"{schema_name}.{table_name}"
        return table_name

    def _qualify_name(self, name: str, schema_name: str | None) -> str:
        """Qualify a name with schema if provided."""
        if schema_name:
            return f"{schema_name}.{name}"
        return name

    def _generate_column_def(self, field: SchemaField) -> str:
        """
        Generate column definition for a single field.

        Args:
            field: SchemaField to generate definition for

        Returns:
            Column definition SQL string
        """
        # Get SQL type for this dialect
        sql_type = self._map_data_type(field.data_type)

        # Build column definition
        column_def = f"{field.name} {sql_type}"

        # Add NOT NULL constraint
        if field.required and not field.nullable:
            column_def += " NOT NULL"

        # Add UNIQUE constraint
        if field.constraints.get("unique", False):
            column_def += " UNIQUE"

        # Add DEFAULT value if specified
        if "default" in field.constraints:
            default_val = field.constraints["default"]
            if isinstance(default_val, str):
                column_def += f" DEFAULT '{default_val}'"
            elif default_val is None:
                column_def += " DEFAULT NULL"
            else:
                column_def += f" DEFAULT {default_val}"

        return column_def

    def _map_data_type(self, data_type: DataType) -> str:
        """
        Map DataType to dialect-specific SQL type.

        Args:
            data_type: VibePiper DataType

        Returns:
            Dialect-specific SQL type string
        """
        type_mapping = self._get_type_mapping()
        if data_type not in type_mapping:
            msg = f"No type mapping for {data_type} in dialect {self.dialect}"
            raise ValueError(msg)
        return type_mapping[data_type]

    def _get_type_mapping(self) -> Mapping[DataType, str]:
        """Get type mapping for current dialect."""
        if self.dialect == Dialect.POSTGRESQL:
            return {
                DataType.STRING: "VARCHAR",
                DataType.INTEGER: "INTEGER",
                DataType.FLOAT: "DOUBLE PRECISION",
                DataType.BOOLEAN: "BOOLEAN",
                DataType.DATETIME: "TIMESTAMP",
                DataType.DATE: "DATE",
                DataType.ARRAY: "JSONB",
                DataType.OBJECT: "JSONB",
                DataType.ANY: "TEXT",
            }
        elif self.dialect == Dialect.MYSQL:
            return {
                DataType.STRING: "VARCHAR",
                DataType.INTEGER: "INT",
                DataType.FLOAT: "DOUBLE",
                DataType.BOOLEAN: "BOOLEAN",
                DataType.DATETIME: "TIMESTAMP",
                DataType.DATE: "DATE",
                DataType.ARRAY: "JSON",
                DataType.OBJECT: "JSON",
                DataType.ANY: "TEXT",
            }
        elif self.dialect == Dialect.SNOWFLAKE:
            return {
                DataType.STRING: "VARCHAR",
                DataType.INTEGER: "NUMBER(38,0)",
                DataType.FLOAT: "FLOAT",
                DataType.BOOLEAN: "BOOLEAN",
                DataType.DATETIME: "TIMESTAMP_NTZ",
                DataType.DATE: "DATE",
                DataType.ARRAY: "VARIANT",
                DataType.OBJECT: "VARIANT",
                DataType.ANY: "TEXT",
            }
        elif self.dialect == Dialect.BIGQUERY:
            return {
                DataType.STRING: "STRING",
                DataType.INTEGER: "INT64",
                DataType.FLOAT: "FLOAT64",
                DataType.BOOLEAN: "BOOL",
                DataType.DATETIME: "TIMESTAMP",
                DataType.DATE: "DATE",
                DataType.ARRAY: "ARRAY<STRING>",
                DataType.OBJECT: "STRUCT",
                DataType.ANY: "STRING",
            }
        else:
            msg = f"Unknown dialect: {self.dialect}"
            raise ValueError(msg)

    def _generate_postgres_upsert(
        self,
        table_name: str,
        schema: Schema,
        upsert_keys: list[str],
        schema_name: str | None,
    ) -> str:
        """Generate PostgreSQL INSERT ON CONFLICT DO UPDATE statement."""
        qualified_name = self._qualify_table_name(table_name, schema_name)

        # Get all field names
        all_fields = [f.name for f in schema.fields]
        non_key_fields = [f for f in all_fields if f not in upsert_keys]

        # Build INSERT clause
        columns = ", ".join(all_fields)
        placeholders = ", ".join([f":{f}" for f in all_fields])

        # Build ON CONFLICT clause
        conflict_target = ", ".join(upsert_keys)
        update_clause = ", ".join([f"{f} = EXCLUDED.{f}" for f in non_key_fields])

        upsert_stmt = (
            f"INSERT INTO {qualified_name} ({columns}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_target}) "
            f"DO UPDATE SET {update_clause};"
        )

        return upsert_stmt

    def _generate_mysql_upsert(
        self,
        table_name: str,
        schema: Schema,
        upsert_keys: list[str],
        schema_name: str | None,
    ) -> str:
        """Generate MySQL INSERT ON DUPLICATE KEY UPDATE statement."""
        qualified_name = self._qualify_table_name(table_name, schema_name)

        # Get all field names
        all_fields = [f.name for f in schema.fields]
        non_key_fields = [f for f in all_fields if f not in upsert_keys]

        # Build INSERT clause
        columns = ", ".join(all_fields)
        placeholders = ", ".join([f":{f}" for f in all_fields])

        # Build ON DUPLICATE KEY UPDATE clause
        update_clause = ", ".join([f"{f} = VALUES({f})" for f in non_key_fields])

        upsert_stmt = (
            f"INSERT INTO {qualified_name} ({columns}) "
            f"VALUES ({placeholders}) "
            f"ON DUPLICATE KEY UPDATE {update_clause};"
        )

        return upsert_stmt

    def _generate_merge_statement(
        self,
        table_name: str,
        schema: Schema,
        upsert_keys: list[str],
        schema_name: str | None,
    ) -> str:
        """Generate MERGE statement for Snowflake/BigQuery."""
        qualified_name = self._qualify_table_name(table_name, schema_name)

        # Get all field names
        all_fields = [f.name for f in schema.fields]

        # Build USING clause (with source subquery placeholder)
        source_fields = ", ".join([f":{f}" for f in all_fields])
        using_clause = f"USING (SELECT {source_fields}) AS source"

        # Build ON clause (match condition)
        match_conditions = [f"target.{k} = source.{k}" for k in upsert_keys]
        on_clause = " AND ".join(match_conditions)

        # Build WHEN MATCHED THEN UPDATE clause
        non_key_fields = [f for f in all_fields if f not in upsert_keys]
        update_clauses = [f"target.{f} = source.{f}" for f in non_key_fields]
        when_matched = f"WHEN MATCHED THEN UPDATE SET {', '.join(update_clauses)}"

        # Build WHEN NOT MATCHED THEN INSERT clause
        insert_columns = ", ".join(all_fields)
        insert_values = ", ".join([f"source.{f}" for f in all_fields])
        when_not_matched = (
            f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
        )

        # Combine into MERGE statement
        merge_stmt = (
            f"MERGE INTO {qualified_name} AS target "
            f"{using_clause} "
            f"ON ({on_clause}) "
            f"{when_matched} "
            f"{when_not_matched};"
        )

        return merge_stmt
