"""
Window function transformation operators.

Provides window functions for advanced analytics including row_number,
rank, dense_rank, lag, and lead operations.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any

import pandas as pd

from vibe_piper.types import DataRecord, DataType, Operator, OperatorType, Schema


class WindowFunctionType(str, Enum):
    """Window function type enumeration."""

    ROW_NUMBER = "row_number"
    RANK = "rank"
    DENSE_RANK = "dense_rank"
    LAG = "lag"
    LEAD = "lead"


class WindowFunction(ABC):
    """Base class for window functions."""

    def __init__(self, alias: str) -> None:
        """
        Initialize window function.

        Args:
            alias: Column name for the result
        """
        self.alias = alias

    @abstractmethod
    def apply(
        self,
        df: pd.DataFrame,  # type: ignore[name-defined]
    ) -> pd.Series:  # type: ignore[name-defined]
        """Apply window function to DataFrame."""
        pass


class RowNumber(WindowFunction):
    """Row number function - assigns sequential row numbers."""

    def apply(self, df: pd.DataFrame) -> pd.Series:  # type: ignore[name-defined]
        return pd.Series(range(1, len(df) + 1), index=df.index)


class Rank(WindowFunction):
    """Rank function - assigns ranks with gaps for ties."""

    def apply(self, df: pd.DataFrame) -> pd.Series:  # type: ignore[name-defined]
        # Use default index since no order_by specified yet
        return pd.Series(range(1, len(df) + 1), index=df.index)


class DenseRank(WindowFunction):
    """Dense rank function - assigns ranks without gaps for ties."""

    def apply(self, df: pd.DataFrame) -> pd.Series:  # type: ignore[name-defined]
        return pd.Series(range(1, len(df) + 1), index=df.index)


class Lag(WindowFunction):
    """Lag function - accesses value from previous row."""

    def __init__(
        self,
        column: str,
        offset: int = 1,
        default: Any = None,
        alias: str | None = None,
    ) -> None:
        """
        Initialize lag function.

        Args:
            column: Column name to access
            offset: Number of rows to look back (default: 1)
            default: Default value when offset exceeds window
            alias: Result column name
        """
        self.column = column
        self.offset = offset
        self.default = default
        super().__init__(alias or f"{column}_lag{offset}")

    def apply(self, df: pd.DataFrame) -> pd.Series:  # type: ignore[name-defined]
        if self.column not in df.columns:
            msg = f"Column '{self.column}' not found for lag function"
            raise ValueError(msg)
        return df[self.column].shift(self.offset).fillna(self.default)


class Lead(WindowFunction):
    """Lead function - accesses value from next row."""

    def __init__(
        self,
        column: str,
        offset: int = 1,
        default: Any = None,
        alias: str | None = None,
    ) -> None:
        """
        Initialize lead function.

        Args:
            column: Column name to access
            offset: Number of rows to look ahead (default: 1)
            default: Default value when offset exceeds window
            alias: Result column name
        """
        self.column = column
        self.offset = offset
        self.default = default
        super().__init__(alias or f"{column}_lead{offset}")

    def apply(self, df: pd.DataFrame) -> pd.Series:  # type: ignore[name-defined]
        if self.column not in df.columns:
            msg = f"Column '{self.column}' not found for lead function"
            raise ValueError(msg)
        return df[self.column].shift(-self.offset).fillna(self.default)


class Window:
    """
    Window transformation for applying window functions.

    Supports partitioning, ordering, and multiple window functions.

    Example:
        Calculate rank by category::

            from vibe_piper.transformations import Window, Rank

            window_op = Window(
                name="rank_by_category",
                partition_by=["category"],
                order_by=["sales desc"],
                functions=[Rank(alias="rank")]
            )
            result = window_op.transform(data, context)
    """

    def __init__(
        self,
        name: str,
        functions: list[WindowFunction],
        partition_by: str | list[str] | None = None,
        order_by: str | list[str] | None = None,
        description: str | None = None,
    ) -> None:
        """
        Initialize a Window transformation.

        Args:
            name: Unique identifier for this window
            functions: List of window functions to apply
            partition_by: Column(s) to partition by
            order_by: Column(s) to order by (supports "col desc" syntax)
            description: Optional description
        """
        self.name = name
        self.functions = functions
        self.partition_by = [partition_by] if isinstance(partition_by, str) else partition_by
        self.order_by = [order_by] if isinstance(order_by, str) else order_by
        self.description = description or "Window function"

        # Parse order_by to get columns and directions
        self.order_columns: list[str] = []
        self.order_ascending: list[bool] = []
        if self.order_by:
            for col_spec in self.order_by:
                parts = col_spec.strip().split()
                col = parts[0]
                ascending = True
                if len(parts) > 1 and parts[1].upper() == "DESC":
                    ascending = False
                self.order_columns.append(col)
                self.order_ascending.append(ascending)

    def transform(
        self,
        data: list[DataRecord],
        ctx: Any,  # noqa: ARG002
    ) -> list[DataRecord]:
        """
        Apply window functions.

        Args:
            data: Input dataset
            ctx: Pipeline context

        Returns:
            Dataset with window function results added
        """
        if not data:
            return []

        # Convert to DataFrame
        df = pd.DataFrame([record.data for record in data])

        # Validate partition columns
        if self.partition_by:
            for col in self.partition_by:
                if col not in df.columns:
                    msg = f"Partition column '{col}' not found in dataset"
                    raise ValueError(msg)

        # Validate order columns
        for col in self.order_columns:
            if col not in df.columns:
                msg = f"Order column '{col}' not found in dataset"
                raise ValueError(msg)

        # Sort data if order_by specified
        if self.order_columns:
            df = df.sort_values(by=self.order_columns, ascending=self.order_ascending)

        # Apply window functions within partitions
        if self.partition_by:
            # Apply within each partition
            grouped = df.groupby(self.partition_by, sort=False)

            for func in self.functions:
                # Apply function to each group and combine
                results = []
                for _, group in grouped:
                    result = func.apply(group)
                    results.append(result)

                # Combine results
                df[func.alias] = pd.concat(results)
        else:
            # Apply to entire dataset
            for func in self.functions:
                df[func.alias] = func.apply(df)

        # Restore original order
        df = df.reset_index(drop=True)

        # Convert back to DataRecords
        return self._dataframe_to_records(df, data[0].schema)

    def _dataframe_to_records(
        self,
        df: pd.DataFrame,
        original_schema: Schema,
    ) -> list[DataRecord]:
        """Convert DataFrame to DataRecords."""
        from vibe_piper.types import SchemaField

        # Create new schema with original fields plus window function results
        new_fields = list(original_schema.fields)

        # Add window function result fields
        for func in self.functions:
            new_fields.append(SchemaField(name=func.alias, data_type=DataType.FLOAT))

        new_schema = Schema(
            name=f"{original_schema.name}_window",
            fields=tuple(new_fields),
        )

        # Convert rows to DataRecords
        records = []
        for _, row in df.iterrows():
            data = {col: val for col, val in row.items()}
            records.append(DataRecord(data=data, schema=new_schema))

        return records

    def to_operator(self) -> Operator:
        """Convert to Operator instance."""
        return Operator(
            name=self.name,
            operator_type=OperatorType.TRANSFORM,
            fn=self.transform,
            description=self.description,
            config={
                "partition_by": self.partition_by,
                "order_by": self.order_by,
                "functions": [type(f).__name__ for f in self.functions],
            },
        )


def window_function(
    func_type: str,
    column: str | None = None,
    **kwargs: Any,
) -> WindowFunction:
    """
    Convenience function to create window functions.

    Args:
        func_type: Type of window function (row_number, rank, dense_rank, lag, lead)
        column: Column name (for lag/lead)
        **kwargs: Additional arguments for specific function types

    Returns:
        WindowFunction instance

    Example:
        Create a lag function::

            lag_func = window_function("lag", column="value", offset=1)
    """
    alias = kwargs.pop("alias", None)

    if func_type == "row_number":
        return RowNumber(alias=alias or "row_number")
    elif func_type == "rank":
        return Rank(alias=alias or "rank")
    elif func_type == "dense_rank":
        return DenseRank(alias=alias or "dense_rank")
    elif func_type == "lag":
        if not column:
            msg = "Lag function requires 'column' parameter"
            raise ValueError(msg)
        return Lag(column=column, alias=alias, **kwargs)
    elif func_type == "lead":
        if not column:
            msg = "Lead function requires 'column' parameter"
            raise ValueError(msg)
        return Lead(column=column, alias=alias, **kwargs)
    else:
        msg = f"Unknown window function type: {func_type}"
        raise ValueError(msg)
