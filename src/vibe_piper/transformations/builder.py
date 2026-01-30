"""
Transformation builder API.

Provides a fluent builder interface for composing complex transformations
with type safety and method chaining.
"""

from collections.abc import Callable
from typing import Any, TypeVar

from vibe_piper.types import DataRecord, PipelineContext

T = TypeVar("T")


class TransformationBuilder:
    """
    Fluent builder for composing transformations.

    Provides method chaining for building complex transformation pipelines.

    Example:
        Build a complex transformation::

            from vibe_piper.transformations import TransformationBuilder
            from vibe_piper.transformations.aggregations import Sum, Count

            builder = TransformationBuilder(data)
            result = (builder
                .filter(lambda r: r.get("status") == "active")
                .join(orders_data, on="customer_id", how="left")
                .groupby(["region"], [Sum("amount"), Count("id")])
                .execute()
            )
    """

    def __init__(
        self,
        data: list[DataRecord],
        context: PipelineContext | None = None,
    ) -> None:
        """
        Initialize the transformation builder.

        Args:
            data: Input dataset
            context: Optional pipeline context
        """
        self.data = data
        self.context = context or PipelineContext(pipeline_id="transform_builder", run_id="run")
        self.transformations: list[Callable[[list[DataRecord]], list[DataRecord]]] = []
        self._current_data = data  # Track current data for pipe() operations

    def pipe(
        self,
        transform_fn: Callable[[list[DataRecord]], list[DataRecord]],
    ) -> "TransformationBuilder":
        """
        Apply a transformation function using pipe syntax.

        This method provides an alternative fluent API pattern that allows
        chaining transformations using .pipe() instead of calling methods directly.

        Args:
            transform_fn: Function that takes a list of DataRecord and returns a transformed list

        Returns:
            self for method chaining

        Example:
            Using pipe pattern::

                from vibe_piper.transformations import extract_fields, filter_rows

                result = (transform(data)
                    .pipe(extract_fields({"company_name": "company.name"}))
                    .pipe(filter_rows(lambda r: r.get("status") == "active"))
                    .pipe(compute_field("category", lambda r: "premium" if r.get("age") > 30 else "standard"))
                    .execute())

            Using method chaining (also supported)::

                result = (transform(data)
                    .filter(lambda r: r.get("active"))
                    .map(lambda r: DataRecord(...))
                    .execute())
        """
        # Apply the transformation to current data
        self._current_data = transform_fn(self._current_data)

        # Add transformation to the list for execute() to use
        self.transformations.append(transform_fn)

        return self

    def filter(
        self,
        predicate: Callable[[DataRecord], bool] | str,
        field: str | None = None,
        value: Any = None,
    ) -> "TransformationBuilder":
        """
        Add a filter transformation.

        Args:
            predicate: Function to test each record, or "equals"/"not_null" shortcut
            field: Field name (for shortcut predicates)
            value: Value to compare (for shortcut predicates)

        Returns:
            self for method chaining

        Example:
            Using a function::

                builder.filter(lambda r: r.get("age") > 18)

            Using shortcut::

                builder.filter("equals", field="status", value="active")
        """

        def filter_fn(data: list[DataRecord]) -> list[DataRecord]:
            if callable(predicate):
                return [r for r in data if predicate(r)]
            elif predicate == "equals":
                if field is None:
                    msg = "Field must be specified for 'equals' filter"
                    raise ValueError(msg)
                return [r for r in data if r.get(field) == value]
            elif predicate == "not_null":
                if field is None:
                    msg = "Field must be specified for 'not_null' filter"
                    raise ValueError(msg)
                return [r for r in data if r.get(field) is not None]
            else:
                msg = f"Unknown filter predicate: {predicate}"
                raise ValueError(msg)

        self.transformations.append(filter_fn)
        return self

    def map(
        self,
        transform_fn: Callable[[DataRecord], DataRecord],
    ) -> "TransformationBuilder":
        """
        Add a map transformation.

        Args:
            transform_fn: Function to transform each record

        Returns:
            self for method chaining

        Example:
            Transform records::

                builder.map(lambda r: DataRecord(
                    data={**r.data, "upper_name": r.get("name").upper()},
                    schema=r.schema
                ))
        """

        def map_fn(data: list[DataRecord]) -> list[DataRecord]:
            return [transform_fn(r) for r in data]

        self.transformations.append(map_fn)
        return self

    def join(
        self,
        right_data: list[DataRecord],
        on: str,
        how: str = "inner",
    ) -> "TransformationBuilder":
        """
        Add a join transformation.

        Args:
            right_data: Right dataset to join with
            on: Column to join on
            how: Join type (inner, left, right, full)

        Returns:
            self for method chaining

        Example:
            Join with orders::

                builder.join(orders, on="customer_id", how="left")
        """
        from vibe_piper.transformations.joins import Join

        join_op = Join(
            name="builder_join",
            right_data=right_data,
            on=on,
            how=how,
        )

        def join_fn(data: list[DataRecord]) -> list[DataRecord]:
            return join_op.transform(data, self.context)

        self.transformations.append(join_fn)
        return self

    def groupby(
        self,
        group_by: str | list[str],
        aggregations: list[Any],  # AggregationFunction objects
    ) -> "TransformationBuilder":
        """
        Add a groupby aggregation transformation.

        Args:
            group_by: Column(s) to group by
            aggregations: List of aggregation functions

        Returns:
            self for method chaining

        Example:
            Group by region::

                from vibe_piper.transformations.aggregations import Sum, Count

                builder.groupby(
                    ["region"],
                    [Sum("amount"), Count("id")]
                )
        """
        from vibe_piper.transformations.aggregations import GroupBy

        groupby_op = GroupBy(
            name="builder_groupby",
            group_by=group_by,
            aggregations=aggregations,
        )

        def groupby_fn(data: list[DataRecord]) -> list[DataRecord]:
            return groupby_op.transform(data, self.context)

        self.transformations.append(groupby_fn)
        return self

    def window(
        self,
        functions: list[Any],  # WindowFunction objects
        partition_by: str | list[str] | None = None,
        order_by: str | list[str] | None = None,
    ) -> "TransformationBuilder":
        """
        Add a window function transformation.

        Args:
            functions: List of window functions
            partition_by: Column(s) to partition by
            order_by: Column(s) to order by

        Returns:
            self for method chaining

        Example:
            Add rank by category::

                from vibe_piper.transformations.windows import Rank

                builder.window(
                    [Rank("rank")],
                    partition_by="category",
                    order_by="sales desc"
                )
        """
        from vibe_piper.transformations.windows import Window

        window_op = Window(
            name="builder_window",
            functions=functions,
            partition_by=partition_by,
            order_by=order_by,
        )

        def window_fn(data: list[DataRecord]) -> list[DataRecord]:
            return window_op.transform(data, self.context)

        self.transformations.append(window_fn)
        return self

    def pivot(
        self,
        index: str | list[str],
        columns: str,
        values: str,
        aggfunc: str = "mean",
    ) -> "TransformationBuilder":
        """
        Add a pivot transformation.

        Args:
            index: Column(s) for index
            columns: Column for pivot columns
            values: Column for values
            aggfunc: Aggregation function

        Returns:
            self for method chaining

        Example:
            Pivot by month::

                builder.pivot(
                    index="category",
                    columns="month",
                    values="amount",
                    aggfunc="sum"
                )
        """
        from vibe_piper.transformations.pivot import Pivot

        pivot_op = Pivot(
            name="builder_pivot",
            index=index,
            columns=columns,
            values=values,
            aggfunc=aggfunc,
        )

        def pivot_fn(data: list[DataRecord]) -> list[DataRecord]:
            return pivot_op.transform(data, self.context)

        self.transformations.append(pivot_fn)
        return self

    def unpivot(
        self,
        id_vars: str | list[str],
        value_vars: str | list[str],
        var_name: str = "variable",
        value_name: str = "value",
    ) -> "TransformationBuilder":
        """
        Add an unpivot transformation.

        Args:
            id_vars: Column(s) to keep as identifiers
            value_vars: Column(s) to unpivot
            var_name: Name for variable column
            value_name: Name for value column

        Returns:
            self for method chaining

        Example:
            Unpivot monthly columns::

                builder.unpivot(
                    id_vars=["category"],
                    value_vars=["Jan", "Feb", "Mar"],
                    var_name="month",
                    value_name="amount"
                )
        """
        from vibe_piper.transformations.pivot import Unpivot

        unpivot_op = Unpivot(
            name="builder_unpivot",
            id_vars=id_vars,
            value_vars=value_vars,
            var_name=var_name,
            value_name=value_name,
        )

        def unpivot_fn(data: list[DataRecord]) -> list[DataRecord]:
            return unpivot_op.transform(data, self.context)

        self.transformations.append(unpivot_fn)
        return self

    def custom(
        self,
        transform_fn: Callable[[list[DataRecord]], list[DataRecord]],
    ) -> "TransformationBuilder":
        """
        Add a custom transformation.

        Args:
            transform_fn: Custom transformation function

        Returns:
            self for method chaining

        Example:
            Apply custom logic::

                def my_transform(data):
                    # Custom logic here
                    return data

                builder.custom(my_transform)
        """
        self.transformations.append(transform_fn)
        return self

    def execute(self) -> list[DataRecord]:
        """
        Execute all transformations in sequence.

        Returns:
            Transformed dataset

        Example:
            Execute the pipeline::

                result = builder.execute()
        """
        # Apply all transformations from original data
        result = self.data
        for transform_fn in self.transformations:
            result = transform_fn(result)

        # Update _current_data so subsequent execute() calls don't re-apply
        self._current_data = result
        return result

    def to_operator(self) -> Any:  # Operator
        """
        Convert the entire builder to an Operator.

        Returns:
            Operator that executes all transformations

        Example:
            Convert to operator for use in pipeline::

                from vibe_piper import custom_operator

                op = builder.to_operator()
        """
        from vibe_piper.types import Operator, OperatorType

        def execute_all(data: list[DataRecord], ctx: PipelineContext) -> list[DataRecord]:
            # Execute all transformations
            result = data
            for transform_fn in self.transformations:
                result = transform_fn(result)
            return result

        return Operator(
            name="transformation_builder",
            operator_type=OperatorType.TRANSFORM,
            fn=execute_all,
            description="Transformation builder pipeline",
        )


def transform(data: list[DataRecord]) -> TransformationBuilder:
    """
    Start a transformation builder pipeline.

    Convenience function to create a TransformationBuilder.

    Args:
        data: Input dataset

    Returns:
        TransformationBuilder instance

    Example:
        Start a transformation pipeline::

            from vibe_piper.transformations import transform

            result = (transform(data)
                .filter(lambda r: r.get("active"))
                .groupby(["category"], [Sum("amount")])
                .execute())
    """
    return TransformationBuilder(data)
