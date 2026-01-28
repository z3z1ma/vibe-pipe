"""
Data schema definitions for the ETL pipeline.

This module defines the schemas for:
- Source data from PostgreSQL
- Transformed data
- Quality expectations
"""


from vibe_piper.types import DataType, Schema, SchemaField

# =============================================================================
# Source Schema (PostgreSQL customers table)
# =============================================================================

CUSTOMER_SOURCE_SCHEMA = Schema(
    name="customer_source",
    description="Raw customer data from PostgreSQL source table",
    fields=[
        SchemaField(
            name="customer_id",
            data_type=DataType.INTEGER,
            required=True,
            description="Unique customer identifier",
        ),
        SchemaField(
            name="first_name",
            data_type=DataType.STRING,
            required=True,
            description="Customer first name",
        ),
        SchemaField(
            name="last_name",
            data_type=DataType.STRING,
            required=True,
            description="Customer last name",
        ),
        SchemaField(
            name="email",
            data_type=DataType.STRING,
            required=True,
            description="Customer email address",
        ),
        SchemaField(
            name="phone",
            data_type=DataType.STRING,
            required=False,
            description="Customer phone number",
        ),
        SchemaField(
            name="status",
            data_type=DataType.STRING,
            required=True,
            description="Customer status (active/inactive/pending)",
        ),
        SchemaField(
            name="created_at",
            data_type=DataType.DATETIME,
            required=True,
            description="Record creation timestamp",
        ),
        SchemaField(
            name="updated_at",
            data_type=DataType.DATETIME,
            required=True,
            description="Record update timestamp",
        ),
        SchemaField(
            name="total_orders",
            data_type=DataType.INTEGER,
            required=False,
            description="Total number of orders",
        ),
        SchemaField(
            name="total_spent",
            data_type=DataType.FLOAT,
            required=False,
            description="Total amount spent",
        ),
        SchemaField(
            name="last_order_date",
            data_type=DataType.DATE,
            required=False,
            description="Date of last order",
        ),
        SchemaField(
            name="country",
            data_type=DataType.STRING,
            required=False,
            description="Customer country",
        ),
        SchemaField(
            name="city",
            data_type=DataType.STRING,
            required=False,
            description="Customer city",
        ),
        SchemaField(
            name="postal_code",
            data_type=DataType.STRING,
            required=False,
            description="Postal code",
        ),
    ],
)


# =============================================================================
# Transformed Schema (with additional fields)
# =============================================================================

CUSTOMER_TRANSFORMED_SCHEMA = Schema(
    name="customer_transformed",
    description="Transformed customer data with enrichment and partitioning columns",
    fields=[
        # Original fields
        SchemaField(
            name="customer_id",
            data_type=DataType.INTEGER,
            required=True,
            description="Unique customer identifier",
        ),
        SchemaField(
            name="first_name",
            data_type=DataType.STRING,
            required=True,
            description="Customer first name",
        ),
        SchemaField(
            name="last_name",
            data_type=DataType.STRING,
            required=True,
            description="Customer last name",
        ),
        SchemaField(
            name="email",
            data_type=DataType.STRING,
            required=True,
            description="Customer email address (cleaned)",
        ),
        SchemaField(
            name="phone",
            data_type=DataType.STRING,
            required=False,
            description="Original phone number",
        ),
        SchemaField(
            name="phone_clean",
            data_type=DataType.STRING,
            required=False,
            description="Cleaned phone (digits only)",
        ),
        SchemaField(
            name="status",
            data_type=DataType.STRING,
            required=True,
            description="Customer status (lowercased)",
        ),
        SchemaField(
            name="created_at",
            data_type=DataType.DATETIME,
            required=True,
            description="Record creation timestamp",
        ),
        SchemaField(
            name="updated_at",
            data_type=DataType.DATETIME,
            required=True,
            description="Record update timestamp",
        ),
        SchemaField(
            name="total_orders",
            data_type=DataType.INTEGER,
            required=False,
            description="Total number of orders",
        ),
        SchemaField(
            name="total_spent",
            data_type=DataType.FLOAT,
            required=False,
            description="Total amount spent",
        ),
        SchemaField(
            name="last_order_date",
            data_type=DataType.DATE,
            required=False,
            description="Date of last order",
        ),
        SchemaField(
            name="country",
            data_type=DataType.STRING,
            required=False,
            description="Customer country",
        ),
        SchemaField(
            name="city",
            data_type=DataType.STRING,
            required=False,
            description="Customer city",
        ),
        SchemaField(
            name="postal_code",
            data_type=DataType.STRING,
            required=False,
            description="Postal code",
        ),
        # Partition columns
        SchemaField(
            name="year",
            data_type=DataType.INTEGER,
            required=True,
            description="Partition: year from updated_at",
        ),
        SchemaField(
            name="month",
            data_type=DataType.STRING,
            required=True,
            description="Partition: month from updated_at",
        ),
    ],
)


# =============================================================================
# Analytics Output Schema (for dashboard/analytics database)
# =============================================================================

CUSTOMER_ANALYTICS_SCHEMA = Schema(
    name="customer_analytics",
    description="Aggregated customer analytics for reporting",
    fields=[
        SchemaField(
            name="customer_id",
            data_type=DataType.INTEGER,
            required=True,
            description="Unique customer identifier",
        ),
        SchemaField(
            name="full_name",
            data_type=DataType.STRING,
            required=True,
            description="Customer full name",
        ),
        SchemaField(
            name="email",
            data_type=DataType.STRING,
            required=True,
            description="Customer email",
        ),
        SchemaField(
            name="status",
            data_type=DataType.STRING,
            required=True,
            description="Customer status",
        ),
        SchemaField(
            name="customer_tenure_days",
            data_type=DataType.INTEGER,
            required=True,
            description="Days since registration",
        ),
        SchemaField(
            name="is_active",
            data_type=DataType.BOOLEAN,
            required=True,
            description="Whether customer is active",
        ),
        SchemaField(
            name="total_orders",
            data_type=DataType.INTEGER,
            required=True,
            description="Total orders (0 if null)",
        ),
        SchemaField(
            name="total_spent",
            data_type=DataType.FLOAT,
            required=True,
            description="Total spent (0 if null)",
        ),
        SchemaField(
            name="avg_order_value",
            data_type=DataType.FLOAT,
            required=True,
            description="Average order value",
        ),
        SchemaField(
            name="has_ordered",
            data_type=DataType.BOOLEAN,
            required=True,
            description="Whether customer has placed orders",
        ),
        SchemaField(
            name="days_since_last_order",
            data_type=DataType.INTEGER,
            required=False,
            description="Days since last order",
        ),
        SchemaField(
            name="country",
            data_type=DataType.STRING,
            required=False,
            description="Customer country",
        ),
        SchemaField(
            name="acquisition_year",
            data_type=DataType.INTEGER,
            required=True,
            description="Year of customer acquisition",
        ),
        SchemaField(
            name="acquisition_month",
            data_type=DataType.STRING,
            required=True,
            description="Month of customer acquisition",
        ),
    ],
)


# =============================================================================
# Schema Mappings and Utilities
# =============================================================================

SCHEMA_MAPPING = {
    "source": CUSTOMER_SOURCE_SCHEMA,
    "transformed": CUSTOMER_TRANSFORMED_SCHEMA,
    "analytics": CUSTOMER_ANALYTICS_SCHEMA,
}


def get_schema(schema_name: str) -> Schema:
    """
    Get schema by name.

    Args:
        schema_name: Name of the schema ('source', 'transformed', 'analytics')

    Returns:
        Schema object

    Raises:
        ValueError: If schema name is not found
    """
    if schema_name not in SCHEMA_MAPPING:
        raise ValueError(
            f"Unknown schema: {schema_name}. Available: {list(SCHEMA_MAPPING.keys())}"
        )

    return SCHEMA_MAPPING[schema_name]


def validate_schema_compatibility(source_schema: Schema, target_schema: Schema) -> bool:
    """
    Validate that source schema is compatible with target schema.

    Checks that all required fields in target exist in source.

    Args:
        source_schema: Source data schema
        target_schema: Target data schema

    Returns:
        True if schemas are compatible
    """
    source_field_names = {f.name for f in source_schema.fields}
    missing_fields = []

    for field in target_schema.fields:
        if field.required and field.name not in source_field_names:
            missing_fields.append(field.name)

    if missing_fields:
        print(f"Schema compatibility warning: Missing fields {missing_fields}")
        return False

    return True


# =============================================================================
# Field-level Expectations
# =============================================================================

FIELD_EXPECTATIONS = {
    "email": {
        "type": DataType.STRING,
        "required": True,
        "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
        "description": "Valid email address",
    },
    "phone": {
        "type": DataType.STRING,
        "required": False,
        "null_proportion_max": 0.1,
        "description": "Phone number (10% null tolerance)",
    },
    "status": {
        "type": DataType.STRING,
        "required": True,
        "allowed_values": {"active", "inactive", "pending"},
        "description": "Customer status in allowed set",
    },
    "customer_id": {
        "type": DataType.INTEGER,
        "required": True,
        "unique": True,
        "description": "Unique customer identifier",
    },
    "total_orders": {
        "type": DataType.INTEGER,
        "required": False,
        "min": 0,
        "description": "Non-negative order count",
    },
    "total_spent": {
        "type": DataType.FLOAT,
        "required": False,
        "min": 0,
        "description": "Non-negative amount spent",
    },
}


def get_field_expectations(field_name: str) -> dict | None:
    """
    Get expectations for a specific field.

    Args:
        field_name: Name of the field

    Returns:
        Dictionary of field expectations or None
    """
    return FIELD_EXPECTATIONS.get(field_name)


# =============================================================================
# Schema Migration Utilities
# =============================================================================


def generate_ddl_postgresql(schema: Schema, table_name: str) -> str:
    """
    Generate PostgreSQL DDL for a schema.

    Args:
        schema: Schema to generate DDL for
        table_name: Name of the table

    Returns:
        SQL DDL statement
    """
    type_mapping = {
        DataType.STRING: "VARCHAR(255)",
        DataType.INTEGER: "INTEGER",
        DataType.FLOAT: "NUMERIC(18,2)",
        DataType.BOOLEAN: "BOOLEAN",
        DataType.DATETIME: "TIMESTAMP",
        DataType.DATE: "DATE",
        DataType.ARRAY: "JSONB",
        DataType.OBJECT: "JSONB",
    }

    columns = []
    for field in schema.fields:
        sql_type = type_mapping.get(field.data_type, "VARCHAR(255)")
        null_constraint = "NOT NULL" if field.required else ""
        columns.append(f"    {field.name} {sql_type} {null_constraint}")

    ddl = f"-- DDL for {table_name}\n"
    ddl += f"-- Generated from schema: {schema.name}\n"
    ddl += f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
    ddl += ",\n".join(columns)
    ddl += "\n);\n"

    return ddl


# Generate DDL statements for reference
if __name__ == "__main__":
    print("=" * 70)
    print("PostgreSQL DDL for Source Table")
    print("=" * 70)
    print(generate_ddl_postgresql(CUSTOMER_SOURCE_SCHEMA, "customers"))
    print("\n")

    print("=" * 70)
    print("PostgreSQL DDL for Analytics Table")
    print("=" * 70)
    print(generate_ddl_postgresql(CUSTOMER_ANALYTICS_SCHEMA, "customers_analytics"))
    print("\n")

    print("Schema validation:")
    print(
        f"Source -> Transformed: {validate_schema_compatibility(CUSTOMER_SOURCE_SCHEMA, CUSTOMER_TRANSFORMED_SCHEMA)}"
    )
    print(
        f"Transformed -> Analytics: {validate_schema_compatibility(CUSTOMER_TRANSFORMED_SCHEMA, CUSTOMER_ANALYTICS_SCHEMA)}"
    )
