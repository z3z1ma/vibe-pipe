"""Sample pipeline for documentation generation."""

from vibe_piper import AssetType, MaterializationStrategy, asset
from vibe_piper.types import DataType, Schema, SchemaField

# Define schemas
user_schema = Schema(
    name="users",
    fields=(
        SchemaField(
            name="user_id",
            data_type=DataType.STRING,
            required=True,
            description="Unique user identifier",
        ),
        SchemaField(
            name="username",
            data_type=DataType.STRING,
            required=True,
            description="Username",
        ),
        SchemaField(
            name="email",
            data_type=DataType.STRING,
            required=True,
            description="User email",
        ),
        SchemaField(
            name="created_at",
            data_type=DataType.DATETIME,
            required=True,
            description="Account creation timestamp",
        ),
    ),
    description="User account information schema",
)

order_schema = Schema(
    name="orders",
    fields=(
        SchemaField(
            name="order_id",
            data_type=DataType.STRING,
            required=True,
            description="Unique order identifier",
        ),
        SchemaField(
            name="user_id",
            data_type=DataType.STRING,
            required=True,
            description="User who placed the order",
        ),
        SchemaField(
            name="total_amount",
            data_type=DataType.FLOAT,
            required=True,
            description="Total order amount",
        ),
        SchemaField(
            name="status",
            data_type=DataType.STRING,
            required=True,
            description="Order status",
        ),
        SchemaField(
            name="order_date",
            data_type=DataType.DATETIME,
            required=True,
            description="Order date",
        ),
    ),
    description="Order information schema",
)

analytics_schema = Schema(
    name="user_analytics",
    fields=(
        SchemaField(
            name="user_id",
            data_type=DataType.STRING,
            required=True,
            description="User identifier",
        ),
        SchemaField(
            name="total_orders",
            data_type=DataType.INTEGER,
            required=True,
            description="Total number of orders",
        ),
        SchemaField(
            name="total_spent",
            data_type=DataType.FLOAT,
            required=True,
            description="Total amount spent",
        ),
        SchemaField(
            name="avg_order_value",
            data_type=DataType.FLOAT,
            required=True,
            description="Average order value",
        ),
        SchemaField(
            name="last_order_date",
            data_type=DataType.DATETIME,
            required=False,
            description="Last order date",
        ),
    ),
    description="User analytics schema",
)


# Define assets
@asset(
    name="raw_users",
    asset_type=AssetType.FILE,
    description="Raw user data from CSV file source",
    metadata={"source": "csv", "owner": "data-team"},
)
def load_raw_users():
    """
    Load raw user data from CSV files.

    This asset reads user data from the source CSV files
    and returns it as a pandas DataFrame.
    """
    # In a real pipeline, this would read from actual files
    return {"users": []}


@asset(
    name="users",
    asset_type=AssetType.TABLE,
    schema=user_schema,
    description="Cleaned and validated users table",
    metadata={"owner": "data-team", "pii": "true", "update_frequency": "daily"},
)
def process_users(raw_users):
    """
    Process and clean raw user data.

    Validates user data, removes duplicates, and enforces
    schema constraints. Returns a cleaned users table.
    """
    # Processing logic here
    return {"users": []}


@asset(
    name="raw_orders",
    asset_type=AssetType.FILE,
    description="Raw order data from source system",
    metadata={"source": "api", "owner": "data-team"},
)
def load_raw_orders():
    """
    Load raw order data from external API.

    Fetches order data from the external order management
    system and returns it for processing.
    """
    return {"orders": []}


@asset(
    name="orders",
    asset_type=AssetType.TABLE,
    schema=order_schema,
    description="Cleaned orders table with validated data",
    metadata={"owner": "data-team", "update_frequency": "hourly"},
)
def process_orders(raw_orders):
    """
    Process and validate order data.

    Cleans order data, validates against schema, and
    prepares it for analytics.
    """
    return {"orders": []}


@asset(
    name="user_analytics",
    asset_type=AssetType.TABLE,
    schema=analytics_schema,
    description="User-level analytics with order summaries",
    metadata={"owner": "analytics-team", "refresh_strategy": "incremental"},
)
def compute_user_analytics(users, orders):
    """
    Compute user-level analytics.

    Aggregates order data at the user level to calculate
    total orders, total spent, and average order value.
    """
    return {"analytics": []}


@asset(
    name="active_users_view",
    asset_type=AssetType.VIEW,
    description="View of active users (users with orders in last 30 days)",
    metadata={"owner": "analytics-team"},
)
def create_active_users_view(users, orders):
    """
    Create a view of active users.

    Filters users who have placed orders in the last 30 days
    for targeted marketing campaigns.
    """
    return {"active_users": []}


@asset(
    name="high_value_customers",
    asset_type=AssetType.TABLE,
    description="Table of high-value customers (top 10% by spend)",
    metadata={"owner": "analytics-team", "sensitivity": "high"},
)
def identify_high_value_customers(user_analytics):
    """
    Identify high-value customers.

    Calculates the top 10% of customers by total spend
    for VIP treatment and special offers.
    """
    return {"high_value": []}
