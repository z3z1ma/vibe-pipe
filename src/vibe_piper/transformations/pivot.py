"""
Pivot and unpivot transformation operators.

Provides data reshaping operations including pivot (rows to columns)
and unpivot/melt (columns to rows).
"""

from collections.abc import Callable
from typing import Any

import pandas as pd

from vibe_piper.types import DataRecord, DataType, Operator, OperatorType, Schema


class Pivot:
    """
    Pivot transformation - converts rows to columns.

    Rotates data from long format to wide format.

    Example:
        Pivot sales data by month::

            from vibe_piper.transformations import Pivot, Sum

            pivot_op = Pivot(
                name="sales_by_month",
                index="category",
                columns="month",
                values=Sum("amount"),
                description="Pivot sales by month"
            )
            result = pivot_op.transform(data, context)
    """

    def __init__(
        self,
        name: str,
        index: str | list[str],
        columns: str,
        values: str | Callable[[pd.Series], Any] | None,  # type: ignore[name-defined]
        aggfunc: str | Callable[[pd.Series], Any] = "mean",  # type: ignore[name-defined]
        fill_value: Any = None,
        description: str | None = None,
    ) -> None:
        """
        Initialize a Pivot transformation.

        Args:
            name: Unique identifier for this pivot
            index: Column(s) to use as index (row labels)
            columns: Column to use for column labels
            values: Column(s) to aggregate or aggregation function
            aggfunc: Aggregation function ('mean', 'sum', 'count', etc.)
            fill_value: Value to fill NaN with
            description: Optional description
        """
        self.name = name
        self.index = [index] if isinstance(index, str) else index
        self.columns = columns
        self.values = values
        self.aggfunc = aggfunc
        self.fill_value = fill_value
        self.description = description or f"Pivot on {self.index} x {self.columns}"

    def transform(
        self,
        data: list[DataRecord],
        ctx: Any,  # noqa: ARG002
    ) -> list[DataRecord]:
        """
        Apply pivot transformation.

        Args:
            data: Input dataset (long format)
            ctx: Pipeline context

        Returns:
            Pivoted dataset (wide format)

        Raises:
            ValueError: If columns not found
        """
        if not data:
            return []

        # Convert to DataFrame
        df = pd.DataFrame([record.data for record in data])

        # Validate columns
        for col in self.index:
            if col not in df.columns:
                msg = f"Index column '{col}' not found in dataset"
                raise ValueError(msg)

        if self.columns not in df.columns:
            msg = f"Columns column '{self.columns}' not found in dataset"
            raise ValueError(msg)

        # Determine values column(s)
        values_cols: str | list[str] | None
        if isinstance(self.values, str):
            if self.values not in df.columns:
                msg = f"Values column '{self.values}' not found in dataset"
                raise ValueError(msg)
            values_cols = self.values
        elif callable(self.values):
            # Use all non-index, non-columns columns
            temp_values = [
                col for col in df.columns if col not in self.index and col != self.columns
            ]
            if len(temp_values) != 1:
                msg = "When values is callable, DataFrame must have exactly one value column"
                raise ValueError(msg)
            values_cols = temp_values[0]
        else:
            values_cols = None

        # Perform pivot
        pivot_df = df.pivot_table(
            index=self.index,
            columns=self.columns,
            values=values_cols,
            aggfunc=self.aggfunc,
            fill_value=self.fill_value,
        )

        # Flatten column names
        pivot_df.columns = [str(col) for col in pivot_df.columns]
        pivot_df = pivot_df.reset_index()

        # Convert back to DataRecords
        return self._dataframe_to_records(pivot_df, data[0].schema)

    def _dataframe_to_records(
        self,
        df: pd.DataFrame,
        original_schema: Schema,
    ) -> list[DataRecord]:
        """Convert DataFrame to DataRecords."""
        from vibe_piper.types import SchemaField

        # Create new schema
        new_fields = []

        # Index columns
        for col in self.index:
            field = original_schema.get_field(col)
            if field:
                new_fields.append(field)
            else:
                new_fields.append(SchemaField(name=col, data_type=DataType.STRING))

        # Pivoted columns
        for col in df.columns:
            if col not in self.index:
                # Infer type from data
                dtype = self._infer_dtype(df[col])
                new_fields.append(SchemaField(name=str(col), data_type=dtype))

        new_schema = Schema(
            name=f"{original_schema.name}_pivoted",
            fields=tuple(new_fields),
        )

        # Convert rows to DataRecords
        records = []
        for _, row in df.iterrows():
            data = {col: val for col, val in row.items()}
            records.append(DataRecord(data=data, schema=new_schema))

        return records

    def _infer_dtype(self, series: pd.Series) -> DataType:  # type: ignore[name-defined]
        """Infer DataType from pandas Series."""
        if pd.api.types.is_integer_dtype(series):
            return DataType.INTEGER
        elif pd.api.types.is_float_dtype(series):
            return DataType.FLOAT
        elif pd.api.types.is_bool_dtype(series):
            return DataType.BOOLEAN
        else:
            return DataType.STRING

    def to_operator(self) -> Operator:
        """Convert to Operator instance."""
        return Operator(
            name=self.name,
            operator_type=OperatorType.TRANSFORM,
            fn=self.transform,
            description=self.description,
            config={
                "index": self.index,
                "columns": self.columns,
                "values": str(self.values) if self.values else None,
                "aggfunc": str(self.aggfunc),
            },
        )


class Unpivot:
    """
    Unpivot transformation - converts columns to rows.

    Also known as "melt" - rotates data from wide format to long format.

    Example:
        Unpivot sales data from monthly columns to rows::

            from vibe_piper.transformations import Unpivot

            unpivot_op = Unpivot(
                name="sales_long",
                id_vars=["category"],
                value_vars=["Jan", "Feb", "Mar"],
                var_name="month",
                value_name="amount",
                description="Convert to long format"
            )
            result = unpivot_op.transform(data, context)
    """

    def __init__(
        self,
        name: str,
        id_vars: str | list[str],
        value_vars: str | list[str] | None = None,
        var_name: str = "variable",
        value_name: str = "value",
        description: str | None = None,
    ) -> None:
        """
        Initialize an Unpivot transformation.

        Args:
            name: Unique identifier for this unpivot
            id_vars: Column(s) to keep as identifier variables
            value_vars: Column(s) to unpivot (None = all other columns)
            var_name: Name for the variable column
            value_name: Name for the value column
            description: Optional description
        """
        self.name = name
        self.id_vars = [id_vars] if isinstance(id_vars, str) else id_vars
        self.value_vars = [value_vars] if isinstance(value_vars, str) else value_vars
        self.var_name = var_name
        self.value_name = value_name
        self.description = description or f"Unpivot on {self.id_vars}"

    def transform(
        self,
        data: list[DataRecord],
        ctx: Any,  # noqa: ARG002
    ) -> list[DataRecord]:
        """
        Apply unpivot transformation.

        Args:
            data: Input dataset (wide format)
            ctx: Pipeline context

        Returns:
            Unpivoted dataset (long format)

        Raises:
            ValueError: If id columns not found
        """
        if not data:
            return []

        # Convert to DataFrame
        df = pd.DataFrame([record.data for record in data])

        # Validate id columns
        for col in self.id_vars:
            if col not in df.columns:
                msg = f"ID column '{col}' not found in dataset"
                raise ValueError(msg)

        # Perform unpivot (melt)
        melted_df = df.melt(
            id_vars=self.id_vars,
            value_vars=self.value_vars,
            var_name=self.var_name,
            value_name=self.value_name,
        )

        # Convert back to DataRecords
        return self._dataframe_to_records(melted_df, data[0].schema)

    def _dataframe_to_records(
        self,
        df: pd.DataFrame,
        original_schema: Schema,
    ) -> list[DataRecord]:
        """Convert DataFrame to DataRecords."""
        from vibe_piper.types import SchemaField

        # Create new schema
        new_fields = []

        # ID variables (keep original schema)
        for col in self.id_vars:
            field = original_schema.get_field(col)
            if field:
                new_fields.append(field)
            else:
                # Infer from data
                dtype = self._infer_dtype_from_name(col)
                new_fields.append(SchemaField(name=col, data_type=dtype))

        # Variable column (string)
        new_fields.append(SchemaField(name=self.var_name, data_type=DataType.STRING))

        # Value column (float or string)
        new_fields.append(SchemaField(name=self.value_name, data_type=DataType.FLOAT))

        new_schema = Schema(
            name=f"{original_schema.name}_unpivoted",
            fields=tuple(new_fields),
        )

        # Convert rows to DataRecords
        records = []
        for _, row in df.iterrows():
            data = {col: val for col, val in row.items()}
            records.append(DataRecord(data=data, schema=new_schema))

        return records

    def _infer_dtype_from_name(self, col: str) -> DataType:
        """Infer dtype from column name (basic heuristic)."""
        col_lower = col.lower()
        if any(x in col_lower for x in ["id", "code", "key"]):
            return DataType.INTEGER
        elif any(x in col_lower for x in ["name", "desc", "type"]):
            return DataType.STRING
        else:
            return DataType.STRING

    def to_operator(self) -> Operator:
        """Convert to Operator instance."""
        return Operator(
            name=self.name,
            operator_type=OperatorType.TRANSFORM,
            fn=self.transform,
            description=self.description,
            config={
                "id_vars": self.id_vars,
                "value_vars": self.value_vars,
                "var_name": self.var_name,
                "value_name": self.value_name,
            },
        )
