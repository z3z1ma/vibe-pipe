"""Fake data generators for testing."""

import random
import string
from datetime import datetime, timedelta
from typing import Any

from vibe_piper.types import DataType, Schema, SchemaField


class FakeDataGenerator:
    """
    Generate fake data conforming to schemas.

    This class provides methods to generate random but valid test data
    that conforms to given schemas and data types.
    """

    def __init__(self, seed: int | None = None) -> None:
        """
        Initialize the fake data generator.

        Args:
            seed: Optional random seed for reproducibility
        """
        if seed is not None:
            random.seed(seed)

    def generate_for_schema(
        self,
        schema: Schema,
        count: int = 1,
        include_optional: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Generate fake data records conforming to a schema.

        Args:
            schema: The schema to generate data for
            count: Number of records to generate
            include_optional: Whether to include optional fields

        Returns:
            List of dictionaries containing fake data
        """
        records = []
        for _ in range(count):
            record = {}
            for field in schema.fields:
                # Skip optional fields if flag is set
                if not field.required and not include_optional:
                    if random.random() < 0.3:  # 30% chance to skip optional fields
                        continue

                # Generate value for the field
                record[field.name] = self.generate_for_field(field)
            records.append(record)

        return records

    def generate_for_field(self, field: SchemaField) -> Any:
        """
        Generate a fake value for a schema field.

        Args:
            field: The schema field to generate a value for

        Returns:
            A value conforming to the field's type and constraints
        """
        # Handle nullable fields
        if field.nullable and random.random() < 0.1:  # 10% chance of None
            return None

        # Generate value based on data type
        if field.data_type == DataType.STRING:
            return self.generate_string(field.constraints)
        elif field.data_type == DataType.INTEGER:
            return self.generate_integer(field.constraints)
        elif field.data_type == DataType.FLOAT:
            return self.generate_float(field.constraints)
        elif field.data_type == DataType.BOOLEAN:
            return self.generate_boolean()
        elif field.data_type == DataType.DATETIME:
            return self.generate_datetime(field.constraints)
        elif field.data_type == DataType.DATE:
            return self.generate_date(field.constraints)
        elif field.data_type == DataType.ARRAY:
            return self.generate_array(field.constraints)
        elif field.data_type == DataType.OBJECT:
            return self.generate_object(field.constraints)
        else:  # DataType.ANY
            return self.generate_any()

    def generate_string(self, constraints: dict[str, Any] | None = None) -> str:
        """Generate a fake string value."""
        constraints = constraints or {}

        min_length = constraints.get("min_length", 1)
        max_length = constraints.get("max_length", 20)
        pattern = constraints.get("pattern")

        # Ensure min_length <= max_length
        min_length = min(min_length, max_length)

        if pattern:
            # For regex patterns, return a simple alphanumeric string
            # (Full pattern matching would require regex library)
            length = random.randint(min_length, min(max_length, 20))
            return "".join(
                random.choices(string.ascii_letters + string.digits, k=length)
            )

        length = random.randint(min_length, max_length)

        # Mix of different string types
        string_types: list[() -> str] = [
            lambda: "".join(random.choices(string.ascii_lowercase, k=length)),
            lambda: "".join(random.choices(string.ascii_uppercase, k=length)),
            lambda: "".join(random.choices(string.ascii_letters, k=length)),
            lambda: "".join(
                random.choices(string.ascii_letters + string.digits, k=length)
            ),
            lambda: "email_"
            + "".join(random.choices(string.ascii_lowercase, k=length - 6))
            + "@example.com",
        ]

        chosen = random.choice(string_types)
        return chosen()

    def generate_integer(self, constraints: dict[str, Any] | None = None) -> int:
        """Generate a fake integer value."""
        constraints = constraints or {}

        min_value = constraints.get("min_value", 0)
        max_value = constraints.get("max_value", 1000)

        return random.randint(min_value, max_value)

    def generate_float(self, constraints: dict[str, Any] | None = None) -> float:
        """Generate a fake float value."""
        constraints = constraints or {}

        min_value = constraints.get("min_value", 0.0)
        max_value = constraints.get("max_value", 1000.0)

        return round(random.uniform(min_value, max_value), 2)

    def generate_boolean(self) -> bool:
        """Generate a fake boolean value."""
        return random.choice([True, False])

    def generate_datetime(self, constraints: dict[str, Any] | None = None) -> str:
        """Generate a fake datetime value (ISO format string)."""
        constraints = constraints or {}

        # Generate datetime within last year
        days_ago = random.randint(0, 365)
        random_datetime = datetime.now() - timedelta(days=days_ago)

        # Add random hours, minutes, seconds
        random_datetime += timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )

        return random_datetime.isoformat()

    def generate_date(self, constraints: dict[str, Any] | None = None) -> str:
        """Generate a fake date value (ISO format string)."""
        dt_str = self.generate_datetime(constraints)
        return dt_str.split("T")[0]  # Return just the date part

    def generate_array(self, constraints: dict[str, Any] | None = None) -> list[Any]:
        """Generate a fake array value."""
        constraints = constraints or {}

        min_items = constraints.get("min_items", 1)
        max_items = constraints.get("max_items", 5)

        count = random.randint(min_items, max_items)

        # Generate mixed type array
        items = []
        for _ in range(count):
            item_type = random.choice(["string", "integer", "float", "boolean"])
            if item_type == "string":
                items.append(self.generate_string({"min_length": 3, "max_length": 10}))
            elif item_type == "integer":
                items.append(self.generate_integer())
            elif item_type == "float":
                items.append(self.generate_float())
            else:
                items.append(self.generate_boolean())

        return items

    def generate_object(
        self, constraints: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Generate a fake object value."""
        constraints = constraints or {}

        num_keys = random.randint(1, 5)
        obj = {}

        for i in range(num_keys):
            key = f"field_{i}"
            value_type = random.choice(["string", "integer", "float", "boolean"])

            if value_type == "string":
                obj[key] = self.generate_string()
            elif value_type == "integer":
                obj[key] = self.generate_integer()
            elif value_type == "float":
                obj[key] = self.generate_float()
            else:
                obj[key] = self.generate_boolean()

        return obj

    def generate_any(self) -> Any:
        """Generate a value of any type."""
        generators = [
            self.generate_string,
            self.generate_integer,
            self.generate_float,
            self.generate_boolean,
            self.generate_array,
            self.generate_object,
        ]
        return random.choice(generators)()


# =============================================================================
# Convenience Functions
# =============================================================================


def fake_user_data(count: int = 1, seed: int | None = None) -> list[dict[str, Any]]:
    """
    Generate fake user data records.

    Args:
        count: Number of records to generate
        seed: Optional random seed

    Returns:
        List of fake user records
    """
    generator = FakeDataGenerator(seed)

    user_schema = Schema(
        name="user",
        fields=(
            SchemaField(name="user_id", data_type=DataType.INTEGER, required=True),
            SchemaField(
                name="username",
                data_type=DataType.STRING,
                required=True,
                constraints={"min_length": 3, "max_length": 20},
            ),
            SchemaField(name="email", data_type=DataType.STRING, required=True),
            SchemaField(
                name="age",
                data_type=DataType.INTEGER,
                required=False,
                constraints={"min_value": 0, "max_value": 150},
            ),
            SchemaField(name="is_active", data_type=DataType.BOOLEAN, required=False),
            SchemaField(name="created_at", data_type=DataType.DATETIME, required=True),
        ),
    )

    return generator.generate_for_schema(user_schema, count=count)


def fake_product_data(count: int = 1, seed: int | None = None) -> list[dict[str, Any]]:
    """
    Generate fake product data records.

    Args:
        count: Number of records to generate
        seed: Optional random seed

    Returns:
        List of fake product records
    """
    generator = FakeDataGenerator(seed)

    product_schema = Schema(
        name="product",
        fields=(
            SchemaField(name="product_id", data_type=DataType.INTEGER, required=True),
            SchemaField(name="name", data_type=DataType.STRING, required=True),
            SchemaField(
                name="price",
                data_type=DataType.FLOAT,
                required=True,
                constraints={"min_value": 0.01},
            ),
            SchemaField(
                name="quantity",
                data_type=DataType.INTEGER,
                required=False,
                constraints={"min_value": 0},
            ),
            SchemaField(name="in_stock", data_type=DataType.BOOLEAN, required=False),
            SchemaField(name="tags", data_type=DataType.ARRAY, required=False),
        ),
    )

    return generator.generate_for_schema(product_schema, count=count)


def fake_event_data(count: int = 1, seed: int | None = None) -> list[dict[str, Any]]:
    """
    Generate fake event/log data records.

    Args:
        count: Number of records to generate
        seed: Optional random seed

    Returns:
        List of fake event records
    """
    generator = FakeDataGenerator(seed)

    event_schema = Schema(
        name="event",
        fields=(
            SchemaField(name="event_id", data_type=DataType.STRING, required=True),
            SchemaField(name="timestamp", data_type=DataType.DATETIME, required=True),
            SchemaField(name="event_type", data_type=DataType.STRING, required=True),
            SchemaField(name="severity", data_type=DataType.STRING, required=False),
            SchemaField(name="payload", data_type=DataType.OBJECT, required=False),
        ),
    )

    return generator.generate_for_schema(event_schema, count=count)


def fake_financial_data(
    count: int = 1, seed: int | None = None
) -> list[dict[str, Any]]:
    """
    Generate fake financial transaction data.

    Args:
        count: Number of records to generate
        seed: Optional random seed

    Returns:
        List of fake transaction records
    """
    generator = FakeDataGenerator(seed)

    transaction_schema = Schema(
        name="transaction",
        fields=(
            SchemaField(
                name="transaction_id", data_type=DataType.STRING, required=True
            ),
            SchemaField(name="account_id", data_type=DataType.STRING, required=True),
            SchemaField(name="amount", data_type=DataType.FLOAT, required=True),
            SchemaField(name="currency", data_type=DataType.STRING, required=True),
            SchemaField(name="timestamp", data_type=DataType.DATETIME, required=True),
            SchemaField(name="status", data_type=DataType.STRING, required=True),
            SchemaField(name="metadata", data_type=DataType.OBJECT, required=False),
        ),
    )

    return generator.generate_for_schema(transaction_schema, count=count)
