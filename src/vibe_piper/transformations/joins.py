"""
Join transformation operators.

Provides join operations for combining datasets with support for inner,
left, right, and full outer joins. Uses pandas merge functionality for
in-memory transformations with proper schema propagation.
"""

from enum import Enum
from typing import Any

import pandas as pd

from vibe_piper.types import DataRecord, DataType, Operator, OperatorType, Schema


class JoinType(str, Enum):
    """Join type enumeration."""

    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"


class Join:
    """
    Join transformation for combining two datasets.

    Supports inner, left, right, and full outer joins with flexible
    join conditions and schema propagation.

    Example:
        Join customers with orders::

            join_op = Join(
                name="customer_orders",
                right_data=orders_data,
                on="customer_id",
                how="left",
                description="Join customers with their orders"
            )
            result = join_op.transform(customers_data, context)
    """

    def __init__(
        self,
        name: str,
        right_data: list[DataRecord],
        on: str | list[str] | tuple[str, str],
        how: str | JoinType = JoinType.INNER,
        left_suffix: str = "_x",
        right_suffix: str = "_y",
        description: str | None = None,
    ) -> None:
        """
        Initialize a Join transformation.

        Args:
            name: Unique identifier for this join
            right_data: Right dataset to join with
            on: Column(s) to join on.
                - str: Same column name in both datasets
                - list[str]: Multiple columns with same names
                - tuple[str, str]: (left_col, right_col) for different names
            how: Join type (inner, left, right, full)
            left_suffix: Suffix for overlapping left columns
            right_suffix: Suffix for overlapping right columns
            description: Optional description

        Raises:
            ValueError: If invalid join type or parameters
        """
        if isinstance(how, str):
            try:
                how = JoinType(how)
            except ValueError as e:
                msg = f"Invalid join type: {how!r}. Must be one of: inner, left, right, full"
                raise ValueError(msg) from e

        self.name = name
        self.right_data = right_data
        self.on = on
        self.how = how
        self.left_suffix = left_suffix
        self.right_suffix = right_suffix
        self.description = description or f"{how.value} join on {on}"

    def transform(
        self,
        left_data: list[DataRecord],
        ctx: Any,  # noqa: ARG002
    ) -> list[DataRecord]:
        """
        Apply the join transformation.

        Args:
            left_data: Left dataset
            ctx: Pipeline context (unused but required for interface)

        Returns:
            Joined dataset as list of DataRecords

        Raises:
            ValueError: If join columns not found in datasets
        """
        if not left_data or not self.right_data:
            # Handle empty datasets
            if self.how == JoinType.INNER:
                return []
            elif self.how == JoinType.LEFT:
                return left_data
            elif self.how == JoinType.RIGHT:
                return self.right_data
            else:  # FULL
                # Return both with nulls for missing columns
                return left_data + self.right_data

        # Convert to pandas DataFrames
        left_df = self._records_to_dataframe(left_data)
        right_df = self._records_to_dataframe(self.right_data)

        # Prepare join parameters
        left_on, right_on = self._prepare_join_columns(left_df, right_df)

        # Map JoinType to pandas merge how parameter
        # pandas uses "outer" for full outer join
        pandas_how = "outer" if self.how == JoinType.FULL else self.how.value

        # Perform the join
        merged_df = pd.merge(
            left_df,
            right_df,
            left_on=left_on,
            right_on=right_on,
            how=pandas_how,
            suffixes=(self.left_suffix, self.right_suffix),
        )

        # Convert back to DataRecords
        return self._dataframe_to_records(merged_df, left_data[0].schema)

    def _records_to_dataframe(self, records: list[DataRecord]) -> pd.DataFrame:
        """Convert DataRecords to pandas DataFrame."""
        data = [record.data for record in records]
        return pd.DataFrame(data)

    def _prepare_join_columns(
        self,
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
    ) -> tuple[list[str], list[str]]:
        """Prepare join columns for pandas merge."""
        if isinstance(self.on, str):
            # Single column with same name
            if self.on not in left_df.columns:
                msg = f"Join column '{self.on}' not found in left dataset"
                raise ValueError(msg)
            if self.on not in right_df.columns:
                msg = f"Join column '{self.on}' not found in right dataset"
                raise ValueError(msg)
            return [self.on], [self.on]
        elif isinstance(self.on, list):
            # Multiple columns with same names
            for col in self.on:
                if col not in left_df.columns:
                    msg = f"Join column '{col}' not found in left dataset"
                    raise ValueError(msg)
                if col not in right_df.columns:
                    msg = f"Join column '{col}' not found in right dataset"
                    raise ValueError(msg)
            return self.on, self.on
        elif isinstance(self.on, tuple):
            # Different column names
            left_col, right_col = self.on
            if left_col not in left_df.columns:
                msg = f"Join column '{left_col}' not found in left dataset"
                raise ValueError(msg)
            if right_col not in right_df.columns:
                msg = f"Join column '{right_col}' not found in right dataset"
                raise ValueError(msg)
            return [left_col], [right_col]
        else:
            msg = f"Invalid 'on' parameter type: {type(self.on)}"
            raise ValueError(msg)

    def _dataframe_to_records(
        self,
        df: pd.DataFrame,
        original_schema: Schema,
    ) -> list[DataRecord]:
        """Convert pandas DataFrame back to DataRecords."""
        # Create new schema based on merged columns
        new_fields = []
        for col in df.columns:
            # Infer data type from pandas dtype
            dtype = self._infer_data_type(df[col].dtype)
            field = original_schema.get_field(col)
            if field:
                # Make field nullable for joins (can introduce NULLs)
                from vibe_piper.types import SchemaField

                new_fields.append(
                    SchemaField(
                        name=field.name,
                        data_type=field.data_type,
                        required=False,  # Joins can introduce NULLs
                        nullable=True,  # Allow NULL values from joins
                        description=field.description,
                        constraints=field.constraints,
                    )
                )
            else:
                # Create new field (nullable for joins)
                from vibe_piper.types import SchemaField

                new_fields.append(
                    SchemaField(name=col, data_type=dtype, required=False, nullable=True)
                )

        new_schema = Schema(
            name=f"{original_schema.name}_joined",
            fields=tuple(new_fields),
        )

        # Convert each row to DataRecord
        records = []
        for _, row in df.iterrows():
            # Handle NaN values (replace with None)
            data = {col: (val if pd.notna(val) else None) for col, val in row.items()}
            records.append(DataRecord(data=data, schema=new_schema))

        return records

    def _infer_data_type(self, dtype: pd.dtype) -> DataType:  # type: ignore[name-defined]
        """Infer DataType from pandas dtype."""
        if pd.api.types.is_integer_dtype(dtype):
            return DataType.INTEGER
        elif pd.api.types.is_float_dtype(dtype):
            return DataType.FLOAT
        elif pd.api.types.is_bool_dtype(dtype):
            return DataType.BOOLEAN
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return DataType.DATETIME
        else:
            return DataType.STRING

    def to_operator(self) -> Operator:
        """
        Convert this join to an Operator instance.

        Returns:
            Operator that can be used in a Pipeline
        """
        return Operator(
            name=self.name,
            operator_type=OperatorType.TRANSFORM,
            fn=self.transform,
            description=self.description,
            config={
                "join_type": self.how.value,
                "on": self.on,
                "left_suffix": self.left_suffix,
                "right_suffix": self.right_suffix,
            },
        )


def join(
    left: list[DataRecord],
    right: list[DataRecord],
    on: str | list[str] | tuple[str, str],
    how: str = "inner",
) -> list[DataRecord]:
    """
    Convenience function for joining two datasets.

    Args:
        left: Left dataset
        right: Right dataset
        on: Column(s) to join on
        how: Join type (inner, left, right, full)

    Returns:
        Joined dataset

    Example:
        Simple left join::

            result = join(customers, orders, on="customer_id", how="left")
    """
    join_op = Join(
        name="join",
        right_data=right,
        on=on,
        how=how,
    )
    return join_op.transform(left, ctx=None)
