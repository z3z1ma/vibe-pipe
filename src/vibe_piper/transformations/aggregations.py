"""
Aggregation transformation operators.

Provides advanced aggregation capabilities including groupby with multiple
aggregations, rollup, and cube operations.
"""

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd

from vibe_piper.types import DataRecord, DataType, Operator, OperatorType, Schema


class AggregationFunction(ABC):
    """Base class for aggregation functions."""

    def __init__(self, column: str, alias: str | None = None) -> None:
        """
        Initialize aggregation function.

        Args:
            column: Column name to aggregate
            alias: Optional alias for the result column
        """
        self.column = column
        self.alias = alias or f"{column}_{self.__class__.__name__.lower()}"

    @abstractmethod
    def apply(self, series: pd.Series) -> Any:  # type: ignore[name-defined]
        """Apply aggregation to a pandas Series."""
        pass

    def get_result_dtype(self) -> DataType:
        """Get the data type of the result."""
        return DataType.FLOAT


class Sum(AggregationFunction):
    """Sum aggregation function."""

    def apply(self, series: pd.Series) -> Any:  # type: ignore[name-defined]
        return series.sum()


class Count(AggregationFunction):
    """Count aggregation function."""

    def apply(self, series: pd.Series) -> Any:  # type: ignore[name-defined]
        return series.count()

    def get_result_dtype(self) -> DataType:
        return DataType.INTEGER


class Avg(AggregationFunction):
    """Average/mean aggregation function."""

    def apply(self, series: pd.Series) -> Any:  # type: ignore[name-defined]
        return series.mean()


class Min(AggregationFunction):
    """Minimum aggregation function."""

    def apply(self, series: pd.Series) -> Any:  # type: ignore[name-defined]
        return series.min()


class Max(AggregationFunction):
    """Maximum aggregation function."""

    def apply(self, series: pd.Series) -> Any:  # type: ignore[name-defined]
        return series.max()


class GroupBy:
    """
    GroupBy aggregation transformation.

    Groups data by specified columns and applies multiple aggregation functions.

    Example:
        Group by category and calculate sum and count::

            from vibe_piper.transformations import GroupBy, Sum, Count

            groupby_op = GroupBy(
                name="sales_by_category",
                group_by=["category"],
                aggregations=[
                    Sum(column="amount", alias="total_amount"),
                    Count(column="order_id", alias="order_count"),
                ]
            )
            result = groupby_op.transform(data, context)
    """

    def __init__(
        self,
        name: str,
        group_by: str | list[str],
        aggregations: list[AggregationFunction],
        description: str | None = None,
    ) -> None:
        """
        Initialize a GroupBy transformation.

        Args:
            name: Unique identifier for this aggregation
            group_by: Column(s) to group by
            aggregations: List of aggregation functions to apply
            description: Optional description
        """
        self.name = name
        self.group_by = [group_by] if isinstance(group_by, str) else group_by
        self.aggregations = aggregations
        self.description = description or f"Group by {self.group_by}"

    def transform(
        self,
        data: list[DataRecord],
        ctx: Any,  # noqa: ARG002
    ) -> list[DataRecord]:
        """
        Apply the groupby aggregation.

        Args:
            data: Input dataset
            ctx: Pipeline context

        Returns:
            Aggregated dataset

        Raises:
            ValueError: If group columns not found
        """
        if not data:
            return []

        # Convert to DataFrame
        df = pd.DataFrame([record.data for record in data])

        # Validate group columns exist
        for col in self.group_by:
            if col not in df.columns:
                msg = f"Group column '{col}' not found in dataset"
                raise ValueError(msg)

        # Group and aggregate
        grouped = df.groupby(self.group_by, as_index=False)

        # Apply aggregations
        result_data = {}
        for agg_func in self.aggregations:
            if agg_func.column not in df.columns:
                msg = f"Aggregation column '{agg_func.column}' not found in dataset"
                raise ValueError(msg)

            result_data[agg_func.alias] = grouped[agg_func.column].apply(agg_func.apply)

        # Combine group columns with aggregations
        result_df = grouped[self.group_by].first()
        for alias, values in result_data.items():
            result_df[alias] = values.values

        # Convert back to DataRecords
        return self._dataframe_to_records(result_df, data[0].schema)

    def _dataframe_to_records(
        self,
        df: pd.DataFrame,
        original_schema: Schema,
    ) -> list[DataRecord]:
        """Convert DataFrame to DataRecords with proper schema."""
        # Create new schema
        from vibe_piper.types import SchemaField

        new_fields = []
        for col in df.columns:
            if col in self.group_by:
                # Use original field type
                field = original_schema.get_field(col)
                if field:
                    new_fields.append(field)
                else:
                    new_fields.append(SchemaField(name=col, data_type=DataType.STRING))
            else:
                # Find aggregation function for this column
                for agg_func in self.aggregations:
                    if agg_func.alias == col:
                        new_fields.append(
                            SchemaField(name=col, data_type=agg_func.get_result_dtype())
                        )
                        break
                else:
                    new_fields.append(SchemaField(name=col, data_type=DataType.FLOAT))

        new_schema = Schema(
            name=f"{original_schema.name}_grouped",
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
            operator_type=OperatorType.AGGREGATE,
            fn=self.transform,
            description=self.description,
            config={
                "group_by": self.group_by,
                "aggregations": [
                    {
                        "type": type(agg).__name__,
                        "column": agg.column,
                        "alias": agg.alias,
                    }
                    for agg in self.aggregations
                ],
            },
        )


class Rollup:
    """
    Rollup aggregation (subtotal aggregation).

    Creates hierarchical aggregations with subtotals at different levels.

    Example:
        Rollup by region and category with subtotals::

            rollup_op = Rollup(
                name="sales_rollup",
                group_by=["region", "category"],
                aggregations=[Sum("amount", "total")]
            )
    """

    def __init__(
        self,
        name: str,
        group_by: list[str],
        aggregations: list[AggregationFunction],
        description: str | None = None,
    ) -> None:
        """
        Initialize a Rollup transformation.

        Args:
            name: Unique identifier
            group_by: Columns for hierarchical grouping
            aggregations: Aggregation functions to apply
            description: Optional description
        """
        self.name = name
        self.group_by = group_by
        self.aggregations = aggregations
        self.description = description or f"Rollup by {group_by}"

    def transform(
        self,
        data: list[DataRecord],
        ctx: Any,  # noqa: ARG002
    ) -> list[DataRecord]:
        """
        Apply rollup aggregation.

        Creates aggregations at each level of the hierarchy:
        - Grand total (no grouping)
        - Level 1 subtotals (first column)
        - Level 2 subtotals (first two columns)
        - ...
        - Full detail (all columns)
        """
        if not data:
            return []

        results: list[dict[str, Any]] = []

        # Grand total
        grand_total = self._aggregate_level(data, {})
        results.append(grand_total)

        # Subtotals at each level
        for i in range(len(self.group_by)):
            level_groups = self.group_by[: i + 1]
            df = pd.DataFrame([record.data for record in data])

            for _, group in df.groupby(level_groups, as_index=False, dropna=False):
                group_records = [
                    DataRecord(data=row, schema=data[0].schema)
                    for _, row in group.iterrows()
                ]
                subtotal = self._aggregate_level(group_records, {})
                results.append(subtotal)

        # Convert to DataRecords
        return self._results_to_records(results, data[0].schema)

    def _aggregate_level(
        self, records: list[DataRecord], group_values: dict[str, Any]
    ) -> dict[str, Any]:
        """Aggregate a single level."""
        result = dict(group_values)

        # Fill None for grouping columns not in this level
        for col in self.group_by:
            if col not in group_values:
                result[col] = None

        # Apply aggregations
        df = pd.DataFrame([record.data for record in records])
        for agg_func in self.aggregations:
            if agg_func.column in df.columns:
                result[agg_func.alias] = agg_func.apply(df[agg_func.column])

        return result

    def _results_to_records(
        self,
        results: list[dict[str, Any]],
        original_schema: Schema,
    ) -> list[DataRecord]:
        """Convert results to DataRecords."""
        from vibe_piper.types import SchemaField

        # Create schema
        new_fields = []
        for col in self.group_by:
            field = original_schema.get_field(col)
            if field:
                new_fields.append(field)
            else:
                new_fields.append(SchemaField(name=col, data_type=DataType.STRING))

        for agg_func in self.aggregations:
            new_fields.append(
                SchemaField(name=agg_func.alias, data_type=agg_func.get_result_dtype())
            )

        new_schema = Schema(
            name=f"{original_schema.name}_rollup",
            fields=tuple(new_fields),
        )

        return [DataRecord(data=result, schema=new_schema) for result in results]

    def to_operator(self) -> Operator:
        """Convert to Operator instance."""
        return Operator(
            name=self.name,
            operator_type=OperatorType.AGGREGATE,
            fn=self.transform,
            description=self.description,
        )


class Cube:
    """
    Cube aggregation (multidimensional aggregation with subtotals).

    Creates aggregations for all combinations of grouping columns.

    Example:
        Cube by region and category::

            cube_op = Cube(
                name="sales_cube",
                group_by=["region", "category"],
                aggregations=[Sum("amount", "total")]
            )
    """

    def __init__(
        self,
        name: str,
        group_by: list[str],
        aggregations: list[AggregationFunction],
        description: str | None = None,
    ) -> None:
        """
        Initialize a Cube transformation.

        Args:
            name: Unique identifier
            group_by: Columns for multidimensional analysis
            aggregations: Aggregation functions to apply
            description: Optional description
        """
        self.name = name
        self.group_by = group_by
        self.aggregations = aggregations
        self.description = description or f"Cube by {group_by}"

    def transform(
        self,
        data: list[DataRecord],
        ctx: Any,  # noqa: ARG002
    ) -> list[DataRecord]:
        """
        Apply cube aggregation.

        Creates aggregations for all combinations of grouping columns.
        """
        if not data:
            return []

        from itertools import combinations

        results: list[dict[str, Any]] = []
        df = pd.DataFrame([record.data for record in data])

        # Generate all combinations
        for r in range(len(self.group_by) + 1):
            for combo in combinations(self.group_by, r):
                if len(combo) == 0:
                    # Grand total
                    group_records = data
                    group_values = {}
                else:
                    # Group by combination
                    grouped = df.groupby(list(combo), as_index=False, dropna=False)
                    for _, group in grouped:
                        group_records = [
                            DataRecord(data=row, schema=data[0].schema)
                            for _, row in group.iterrows()
                        ]
                        group_values = {
                            col: row[col]
                            for _, row in group.iterrows()
                            for col in combo
                        }
                        result = self._aggregate_level(group_records, group_values)
                        results.append(result)
                    continue

                result = self._aggregate_level(group_records, group_values)
                results.append(result)

        # Convert to DataRecords
        return self._results_to_records(results, data[0].schema)

    def _aggregate_level(
        self, records: list[DataRecord], group_values: dict[str, Any]
    ) -> dict[str, Any]:
        """Aggregate a single combination."""
        result = dict(group_values)

        # Fill None for grouping columns not in this combination
        for col in self.group_by:
            if col not in group_values:
                result[col] = None

        # Apply aggregations
        df = pd.DataFrame([record.data for record in records])
        for agg_func in self.aggregations:
            if agg_func.column in df.columns:
                result[agg_func.alias] = agg_func.apply(df[agg_func.column])

        return result

    def _results_to_records(
        self,
        results: list[dict[str, Any]],
        original_schema: Schema,
    ) -> list[DataRecord]:
        """Convert results to DataRecords."""
        from vibe_piper.types import SchemaField

        # Create schema
        new_fields = []
        for col in self.group_by:
            field = original_schema.get_field(col)
            if field:
                new_fields.append(field)
            else:
                new_fields.append(SchemaField(name=col, data_type=DataType.STRING))

        for agg_func in self.aggregations:
            new_fields.append(
                SchemaField(name=agg_func.alias, data_type=agg_func.get_result_dtype())
            )

        new_schema = Schema(
            name=f"{original_schema.name}_cube",
            fields=tuple(new_fields),
        )

        return [DataRecord(data=result, schema=new_schema) for result in results]

    def to_operator(self) -> Operator:
        """Convert to Operator instance."""
        return Operator(
            name=self.name,
            operator_type=OperatorType.AGGREGATE,
            fn=self.transform,
            description=self.description,
        )
