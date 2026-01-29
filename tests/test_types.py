"""
Unit tests for core type system.

Tests all core types, their validation rules, and ensure type safety
with mypy strict mode.
"""

import pytest

from vibe_piper.types import (
    Asset,
    AssetGraph,
    AssetType,
    DataRecord,
    DataType,
    Operator,
    OperatorType,
    Pipeline,
    PipelineContext,
    Schema,
    SchemaField,
)


class TestSchemaField:
    """Tests for SchemaField type."""

    def test_create_basic_field(self) -> None:
        """Test creating a basic schema field."""
        field = SchemaField(name="id", data_type=DataType.INTEGER)
        assert field.name == "id"
        assert field.data_type == DataType.INTEGER
        assert field.required is True
        assert field.nullable is False

    def test_create_field_with_options(self) -> None:
        """Test creating a field with optional parameters."""
        field = SchemaField(
            name="email",
            data_type=DataType.STRING,
            required=False,
            nullable=True,
            description="User email address",
            constraints={"max_length": 255},
        )
        assert field.name == "email"
        assert field.required is False
        assert field.nullable is True
        assert field.description == "User email address"
        assert field.constraints == {"max_length": 255}

    def test_field_empty_name_raises_error(self) -> None:
        """Test that empty field name raises ValueError."""
        with pytest.raises(ValueError, match="Schema field name cannot be empty"):
            SchemaField(name="", data_type=DataType.STRING)

    def test_field_invalid_name_raises_error(self) -> None:
        """Test that invalid field name raises ValueError."""
        with pytest.raises(ValueError, match="Invalid field name"):
            SchemaField(name="invalid-name!", data_type=DataType.STRING)

    def test_field_is_frozen(self) -> None:
        """Test that SchemaField is immutable."""
        field = SchemaField(name="test", data_type=DataType.STRING)
        with pytest.raises(Exception):  # FrozenInstanceError
            field.name = "changed"  # type: ignore[misc]


class TestSchema:
    """Tests for Schema type."""

    def test_create_basic_schema(self) -> None:
        """Test creating a basic schema."""
        schema = Schema(name="user_schema")
        assert schema.name == "user_schema"
        assert len(schema.fields) == 0

    def test_create_schema_with_fields(self) -> None:
        """Test creating a schema with fields."""
        fields = (
            SchemaField(name="id", data_type=DataType.INTEGER),
            SchemaField(name="name", data_type=DataType.STRING),
        )
        schema = Schema(name="user_schema", fields=fields)
        assert schema.name == "user_schema"
        assert len(schema.fields) == 2

    def test_schema_with_metadata(self) -> None:
        """Test creating a schema with metadata."""
        schema = Schema(
            name="user_schema",
            description="User data schema",
            metadata={"owner": "data-team", "pii": True},
        )
        assert schema.description == "User data schema"
        assert schema.metadata["owner"] == "data-team"
        assert schema.metadata["pii"] is True

    def test_schema_empty_name_raises_error(self) -> None:
        """Test that empty schema name raises ValueError."""
        with pytest.raises(ValueError, match="Schema name cannot be empty"):
            Schema(name="")

    def test_schema_duplicate_fields_raises_error(self) -> None:
        """Test that duplicate field names raise ValueError."""
        fields = (
            SchemaField(name="id", data_type=DataType.INTEGER),
            SchemaField(name="id", data_type=DataType.STRING),
        )
        with pytest.raises(ValueError, match="Duplicate field names"):
            Schema(name="test_schema", fields=fields)

    def test_get_field(self) -> None:
        """Test getting a field by name."""
        fields = (
            SchemaField(name="id", data_type=DataType.INTEGER),
            SchemaField(name="name", data_type=DataType.STRING),
        )
        schema = Schema(name="test", fields=fields)
        field = schema.get_field("name")
        assert field is not None
        assert field.name == "name"
        assert field.data_type == DataType.STRING

    def test_get_nonexistent_field(self) -> None:
        """Test getting a non-existent field."""
        schema = Schema(name="test")
        assert schema.get_field("nonexistent") is None

    def test_has_field(self) -> None:
        """Test checking if a field exists."""
        fields = (SchemaField(name="id", data_type=DataType.INTEGER),)
        schema = Schema(name="test", fields=fields)
        assert schema.has_field("id") is True
        assert schema.has_field("name") is False

    def test_schema_is_frozen(self) -> None:
        """Test that Schema is immutable."""
        schema = Schema(name="test")
        with pytest.raises(Exception):  # FrozenInstanceError
            schema.name = "changed"  # type: ignore[misc]


class TestDataRecord:
    """Tests for DataRecord type."""

    def test_create_basic_record(self) -> None:
        """Test creating a basic data record."""
        schema = Schema(name="test")
        record = DataRecord(data={"value": 42}, schema=schema)
        assert record.data["value"] == 42
        assert record.schema == schema

    def test_create_record_with_metadata(self) -> None:
        """Test creating a record with metadata."""
        schema = Schema(name="test")
        record = DataRecord(
            data={"value": 42},
            schema=schema,
            metadata={"source": "api", "timestamp": "2024-01-01"},
        )
        assert record.metadata["source"] == "api"

    def test_record_validation_required_field(self) -> None:
        """Test that missing required fields raise ValueError."""
        fields = (SchemaField(name="id", data_type=DataType.INTEGER),)
        schema = Schema(name="test", fields=fields)
        with pytest.raises(ValueError, match="Required field 'id' missing"):
            DataRecord(data={}, schema=schema)

    def test_record_validation_nullable(self) -> None:
        """Test that nullable fields can be None."""
        fields = (
            SchemaField(name="id", data_type=DataType.INTEGER),
            SchemaField(
                name="optional",
                data_type=DataType.STRING,
                required=False,
                nullable=True,
            ),
        )
        schema = Schema(name="test", fields=fields)
        record = DataRecord(data={"id": 1, "optional": None}, schema=schema)
        assert record.data["optional"] is None

    def test_record_validation_not_nullable(self) -> None:
        """Test that non-nullable fields cannot be None."""
        fields = (
            SchemaField(name="id", data_type=DataType.INTEGER),
            SchemaField(name="name", data_type=DataType.STRING, nullable=False),
        )
        schema = Schema(name="test", fields=fields)
        with pytest.raises(ValueError, match="Field 'name' is not nullable"):
            DataRecord(data={"id": 1, "name": None}, schema=schema)

    def test_get_method(self) -> None:
        """Test the get method for field access."""
        schema = Schema(name="test")
        record = DataRecord(data={"value": 42}, schema=schema)
        assert record.get("value") == 42
        assert record.get("missing", "default") == "default"

    def test_getitem_syntax(self) -> None:
        """Test dictionary-style field access."""
        schema = Schema(name="test")
        record = DataRecord(data={"value": 42}, schema=schema)
        assert record["value"] == 42

    def test_getitem_missing_field_raises_error(self) -> None:
        """Test that missing field raises KeyError."""
        schema = Schema(name="test")
        record = DataRecord(data={}, schema=schema)
        with pytest.raises(KeyError, match="Field 'missing' not found"):
            _ = record["missing"]


class TestPipelineContext:
    """Tests for PipelineContext type."""

    def test_create_basic_context(self) -> None:
        """Test creating a basic pipeline context."""
        ctx = PipelineContext(pipeline_id="test_pipe", run_id="run_1")
        assert ctx.pipeline_id == "test_pipe"
        assert ctx.run_id == "run_1"

    def test_context_with_config(self) -> None:
        """Test creating context with configuration."""
        ctx = PipelineContext(
            pipeline_id="test_pipe",
            run_id="run_1",
            config={"batch_size": 100, "parallel": True},
        )
        assert ctx.get_config("batch_size") == 100
        assert ctx.get_config("parallel") is True

    def test_context_state_mutability(self) -> None:
        """Test that context state is mutable."""
        ctx = PipelineContext(pipeline_id="test_pipe", run_id="run_1")
        ctx.set_state("counter", 42)
        assert ctx.get_state("counter") == 42

    def test_get_config_default(self) -> None:
        """Test get_config with default value."""
        ctx = PipelineContext(pipeline_id="test_pipe", run_id="run_1")
        assert ctx.get_config("missing", "default") == "default"

    def test_get_state_default(self) -> None:
        """Test get_state with default value."""
        ctx = PipelineContext(pipeline_id="test_pipe", run_id="run_1")
        assert ctx.get_state("missing", "default") == "default"


class TestOperator:
    """Tests for Operator type."""

    def sample_fn(value: int, ctx: PipelineContext) -> int:
        """Sample operator function."""
        return value * 2

    def test_create_basic_operator(self) -> None:
        """Test creating a basic operator."""
        op = Operator(
            name="double",
            operator_type=OperatorType.TRANSFORM,
            fn=self.sample_fn,
        )
        assert op.name == "double"
        assert op.operator_type == OperatorType.TRANSFORM

    def test_operator_with_schemas(self) -> None:
        """Test creating operator with input/output schemas."""
        input_schema = Schema(name="input")
        output_schema = Schema(name="output")
        op = Operator(
            name="transform",
            operator_type=OperatorType.TRANSFORM,
            fn=self.sample_fn,
            input_schema=input_schema,
            output_schema=output_schema,
        )
        assert op.input_schema == input_schema
        assert op.output_schema == output_schema

    def test_operator_with_config(self) -> None:
        """Test creating operator with configuration."""
        op = Operator(
            name="transform",
            operator_type=OperatorType.TRANSFORM,
            fn=self.sample_fn,
            config={"param1": "value1"},
        )
        assert op.config["param1"] == "value1"

    def test_operator_empty_name_raises_error(self) -> None:
        """Test that empty operator name raises ValueError."""
        with pytest.raises(ValueError, match="Operator name cannot be empty"):
            Operator(name="", operator_type=OperatorType.TRANSFORM, fn=self.sample_fn)

    def test_operator_is_frozen(self) -> None:
        """Test that Operator is immutable."""
        op = Operator(name="test", operator_type=OperatorType.TRANSFORM, fn=self.sample_fn)
        with pytest.raises(Exception):  # FrozenInstanceError
            op.name = "changed"  # type: ignore[misc]


class TestAsset:
    """Tests for Asset type."""

    def test_create_basic_asset(self) -> None:
        """Test creating a basic asset."""
        asset = Asset(
            name="users_table",
            asset_type=AssetType.TABLE,
            uri="postgresql://localhost/db/users",
        )
        assert asset.name == "users_table"
        assert asset.asset_type == AssetType.TABLE
        assert asset.uri == "postgresql://localhost/db/users"

    def test_asset_with_schema(self) -> None:
        """Test creating an asset with a schema."""
        schema = Schema(name="user_schema")
        asset = Asset(
            name="users_table",
            asset_type=AssetType.TABLE,
            uri="postgresql://localhost/db/users",
            schema=schema,
        )
        assert asset.schema == schema

    def test_asset_with_metadata(self) -> None:
        """Test creating an asset with metadata."""
        asset = Asset(
            name="users_file",
            asset_type=AssetType.FILE,
            uri="s3://bucket/users.csv",
            metadata={"format": "csv", "size_bytes": 1024},
        )
        assert asset.metadata["format"] == "csv"

    def test_asset_empty_name_raises_error(self) -> None:
        """Test that empty asset name raises ValueError."""
        with pytest.raises(ValueError, match="Asset name cannot be empty"):
            Asset(
                name="",
                asset_type=AssetType.TABLE,
                uri="postgresql://localhost/db/users",
            )

    def test_asset_empty_uri_raises_error(self) -> None:
        """Test that empty URI raises ValueError."""
        with pytest.raises(ValueError, match="Asset URI cannot be empty"):
            Asset(
                name="test",
                asset_type=AssetType.TABLE,
                uri="",
            )

    def test_asset_is_frozen(self) -> None:
        """Test that Asset is immutable."""
        asset = Asset(
            name="test",
            asset_type=AssetType.TABLE,
            uri="postgresql://localhost/db/users",
        )
        with pytest.raises(Exception):  # FrozenInstanceError
            asset.name = "changed"  # type: ignore[misc]


class TestPipeline:
    """Tests for Pipeline type."""

    def sample_fn(value: int, ctx: PipelineContext) -> int:
        """Sample operator function."""
        return value

    def test_create_basic_pipeline(self) -> None:
        """Test creating a basic pipeline."""
        pipeline = Pipeline(name="test_pipeline")
        assert pipeline.name == "test_pipeline"
        assert len(pipeline.operators) == 0

    def test_pipeline_with_operators(self) -> None:
        """Test creating a pipeline with operators."""
        op1 = Operator(name="op1", operator_type=OperatorType.SOURCE, fn=self.sample_fn)
        op2 = Operator(name="op2", operator_type=OperatorType.TRANSFORM, fn=self.sample_fn)
        pipeline = Pipeline(name="test_pipeline", operators=(op1, op2))
        assert len(pipeline.operators) == 2

    def test_pipeline_with_schemas(self) -> None:
        """Test creating a pipeline with input/output schemas."""
        input_schema = Schema(name="input")
        output_schema = Schema(name="output")
        pipeline = Pipeline(
            name="test_pipeline",
            input_schema=input_schema,
            output_schema=output_schema,
        )
        assert pipeline.input_schema == input_schema
        assert pipeline.output_schema == output_schema

    def test_pipeline_empty_name_raises_error(self) -> None:
        """Test that empty pipeline name raises ValueError."""
        with pytest.raises(ValueError, match="Pipeline name cannot be empty"):
            Pipeline(name="")

    def test_pipeline_duplicate_operators_raises_error(self) -> None:
        """Test that duplicate operator names raise ValueError."""
        op1 = Operator(name="duplicate", operator_type=OperatorType.SOURCE, fn=self.sample_fn)
        op2 = Operator(name="duplicate", operator_type=OperatorType.TRANSFORM, fn=self.sample_fn)
        with pytest.raises(ValueError, match="Duplicate operator names"):
            Pipeline(name="test_pipeline", operators=(op1, op2))

    def test_add_operator(self) -> None:
        """Test adding an operator to a pipeline."""
        op1 = Operator(name="op1", operator_type=OperatorType.SOURCE, fn=self.sample_fn)
        op2 = Operator(name="op2", operator_type=OperatorType.TRANSFORM, fn=self.sample_fn)
        pipeline = Pipeline(name="test_pipeline", operators=(op1,))
        new_pipeline = pipeline.add_operator(op2)
        assert len(new_pipeline.operators) == 2
        assert len(pipeline.operators) == 1  # Original unchanged

    def test_execute_single_operator(self) -> None:
        """Test executing a pipeline with a single operator."""

        def double_fn(value: int, ctx: PipelineContext) -> int:
            return value * 2

        op = Operator(name="double", operator_type=OperatorType.TRANSFORM, fn=double_fn)
        pipeline = Pipeline(name="test_pipeline", operators=(op,))
        ctx = PipelineContext(pipeline_id="test_pipeline", run_id="run_1")

        result = pipeline.execute(5, context=ctx)
        assert result == 10

    def test_execute_multiple_operators(self) -> None:
        """Test executing a pipeline with multiple operators."""

        def double_fn(value: int, ctx: PipelineContext) -> int:
            return value * 2

        def add_ten_fn(value: int, ctx: PipelineContext) -> int:
            return value + 10

        op1 = Operator(name="double", operator_type=OperatorType.TRANSFORM, fn=double_fn)
        op2 = Operator(name="add_ten", operator_type=OperatorType.TRANSFORM, fn=add_ten_fn)
        pipeline = Pipeline(name="test_pipeline", operators=(op1, op2))
        ctx = PipelineContext(pipeline_id="test_pipeline", run_id="run_1")

        result = pipeline.execute(5, context=ctx)
        assert result == 20  # (5 * 2) + 10

    def test_execute_without_context(self) -> None:
        """Test executing a pipeline without providing a context."""

        def identity_fn(value: str, ctx: PipelineContext) -> str:
            return value

        op = Operator(name="identity", operator_type=OperatorType.TRANSFORM, fn=identity_fn)
        pipeline = Pipeline(name="test_pipeline", operators=(op,))

        result = pipeline.execute("test")
        assert result == "test"

    def test_execute_with_data_records(self) -> None:
        """Test executing a pipeline with DataRecord objects."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="value", data_type=DataType.INTEGER),),
        )

        def double_records(records: list[DataRecord], ctx: PipelineContext) -> list[DataRecord]:
            return [
                DataRecord(
                    data={"value": r.get("value") * 2},
                    schema=r.schema,
                )
                for r in records
            ]

        op = Operator(name="double", operator_type=OperatorType.TRANSFORM, fn=double_records)
        pipeline = Pipeline(name="test_pipeline", operators=(op,))
        ctx = PipelineContext(pipeline_id="test_pipeline", run_id="run_1")

        input_records = [
            DataRecord(data={"value": 1}, schema=schema),
            DataRecord(data={"value": 2}, schema=schema),
        ]
        result = pipeline.execute(input_records, context=ctx)

        assert len(result) == 2
        assert result[0].get("value") == 2
        assert result[1].get("value") == 4

    def test_pipeline_is_frozen(self) -> None:
        """Test that Pipeline is immutable."""
        pipeline = Pipeline(name="test")
        with pytest.raises(Exception):  # FrozenInstanceError
            pipeline.name = "changed"  # type: ignore[misc]


class TestTypeAliases:
    """Tests for type aliases."""

    def test_json_primitive_types(self) -> None:
        """Test JSONPrimitive type alias usage."""
        from vibe_piper.types import JSONPrimitive

        primitives: list[JSONPrimitive] = ["string", 42, 3.14, True, None]
        assert len(primitives) == 5

    def test_record_data_type(self) -> None:
        """Test RecordData type alias usage."""
        from vibe_piper.types import RecordData

        data: RecordData = {"field1": "value1", "field2": 42}
        assert data["field1"] == "value1"


class TestEnums:
    """Tests for enum types."""

    def test_data_type_enum(self) -> None:
        """Test DataType enum values."""
        assert DataType.STRING.value == 1
        assert DataType.INTEGER.value == 2
        assert DataType.ANY.value == 9

    def test_asset_type_enum(self) -> None:
        """Test AssetType enum values."""
        assert AssetType.TABLE.value == 1
        assert AssetType.FILE.value == 3
        assert AssetType.STREAM.value == 4

    def test_operator_type_enum(self) -> None:
        """Test OperatorType enum values."""
        assert OperatorType.SOURCE.value == 1
        assert OperatorType.TRANSFORM.value == 2
        assert OperatorType.CUSTOM.value == 8


class TestAssetGraph:
    """Tests for AssetGraph type."""

    def test_create_basic_graph(self) -> None:
        """Test creating a basic asset graph."""
        asset1 = Asset(
            name="raw_data",
            asset_type=AssetType.TABLE,
            uri="postgresql://db/raw_data",
        )
        graph = AssetGraph(name="test_graph", assets=(asset1,))
        assert graph.name == "test_graph"
        assert len(graph.assets) == 1
        assert graph.assets[0].name == "raw_data"

    def test_graph_with_dependencies(self) -> None:
        """Test creating a graph with asset dependencies."""
        source = Asset(
            name="source_table",
            asset_type=AssetType.TABLE,
            uri="postgresql://db/source",
        )
        derived = Asset(
            name="derived_table",
            asset_type=AssetType.TABLE,
            uri="postgresql://db/derived",
        )
        graph = AssetGraph(
            name="test_graph",
            assets=(source, derived),
            dependencies={"derived_table": ("source_table",)},
        )
        assert len(graph.assets) == 2
        assert graph.dependencies["derived_table"] == ("source_table",)

    def test_graph_empty_name_raises_error(self) -> None:
        """Test that empty graph name raises ValueError."""
        with pytest.raises(ValueError, match="AssetGraph name cannot be empty"):
            AssetGraph(name="")

    def test_graph_duplicate_asset_names_raises_error(self) -> None:
        """Test that duplicate asset names raise ValueError."""
        asset1 = Asset(name="duplicate", asset_type=AssetType.TABLE, uri="postgresql://db/a1")
        asset2 = Asset(name="duplicate", asset_type=AssetType.TABLE, uri="postgresql://db/a2")
        with pytest.raises(ValueError, match="Duplicate asset names"):
            AssetGraph(name="test_graph", assets=(asset1, asset2))

    def test_dependency_asset_not_in_graph_raises_error(self) -> None:
        """Test that dependencies must reference assets in the graph."""
        asset = Asset(name="test", asset_type=AssetType.TABLE, uri="postgresql://db/test")
        with pytest.raises(ValueError, match="not found in assets"):
            AssetGraph(
                name="test_graph",
                assets=(asset,),
                dependencies={"test": ("nonexistent",)},
            )

    def test_dependency_reference_not_found_raises_error(self) -> None:
        """Test that dependency references must exist."""
        asset1 = Asset(name="asset1", asset_type=AssetType.TABLE, uri="postgresql://db/a1")
        with pytest.raises(ValueError, match="not found in assets"):
            AssetGraph(
                name="test_graph",
                assets=(asset1,),
                dependencies={"asset1": ("missing_asset",)},
            )

    def test_circular_dependency_raises_error(self) -> None:
        """Test that circular dependencies are detected."""
        asset1 = Asset(name="a", asset_type=AssetType.TABLE, uri="postgresql://db/a")
        asset2 = Asset(name="b", asset_type=AssetType.TABLE, uri="postgresql://db/b")
        with pytest.raises(ValueError, match="Circular dependency"):
            AssetGraph(
                name="test_graph",
                assets=(asset1, asset2),
                dependencies={"a": ("b",), "b": ("a",)},
            )

    def test_complex_circular_dependency_raises_error(self) -> None:
        """Test that complex circular dependencies are detected."""
        a = Asset(name="a", asset_type=AssetType.TABLE, uri="postgresql://db/a")
        b = Asset(name="b", asset_type=AssetType.TABLE, uri="postgresql://db/b")
        c = Asset(name="c", asset_type=AssetType.TABLE, uri="postgresql://db/c")
        with pytest.raises(ValueError, match="Circular dependency"):
            AssetGraph(
                name="test_graph",
                assets=(a, b, c),
                dependencies={"a": ("b",), "b": ("c",), "c": ("a",)},
            )

    def test_get_asset(self) -> None:
        """Test getting an asset by name."""
        asset1 = Asset(name="asset1", asset_type=AssetType.TABLE, uri="postgresql://db/a1")
        asset2 = Asset(name="asset2", asset_type=AssetType.FILE, uri="s3://bucket/data.csv")
        graph = AssetGraph(name="test_graph", assets=(asset1, asset2))
        assert graph.get_asset("asset1") == asset1
        assert graph.get_asset("asset2") == asset2
        assert graph.get_asset("nonexistent") is None

    def test_get_dependencies(self) -> None:
        """Test getting upstream dependencies."""
        raw = Asset(name="raw", asset_type=AssetType.TABLE, uri="postgresql://db/raw")
        cleaned = Asset(name="cleaned", asset_type=AssetType.TABLE, uri="postgresql://db/cleaned")
        aggregated = Asset(
            name="aggregated",
            asset_type=AssetType.TABLE,
            uri="postgresql://db/aggregated",
        )
        graph = AssetGraph(
            name="test_graph",
            assets=(raw, cleaned, aggregated),
            dependencies={"cleaned": ("raw",), "aggregated": ("cleaned",)},
        )
        assert graph.get_dependencies("aggregated") == (cleaned,)
        assert graph.get_dependencies("cleaned") == (raw,)
        assert graph.get_dependencies("raw") == ()

    def test_get_dependents(self) -> None:
        """Test getting downstream dependents."""
        raw = Asset(name="raw", asset_type=AssetType.TABLE, uri="postgresql://db/raw")
        cleaned = Asset(name="cleaned", asset_type=AssetType.TABLE, uri="postgresql://db/cleaned")
        aggregated = Asset(
            name="aggregated",
            asset_type=AssetType.TABLE,
            uri="postgresql://db/aggregated",
        )
        graph = AssetGraph(
            name="test_graph",
            assets=(raw, cleaned, aggregated),
            dependencies={"cleaned": ("raw",), "aggregated": ("cleaned",)},
        )
        assert graph.get_dependents("cleaned") == (aggregated,)
        assert graph.get_dependents("raw") == (cleaned,)
        assert graph.get_dependents("aggregated") == ()

    def test_topological_order_simple(self) -> None:
        """Test topological sorting with simple chain."""
        raw = Asset(name="raw", asset_type=AssetType.TABLE, uri="postgresql://db/raw")
        cleaned = Asset(name="cleaned", asset_type=AssetType.TABLE, uri="postgresql://db/cleaned")
        graph = AssetGraph(
            name="test_graph",
            assets=(raw, cleaned),
            dependencies={"cleaned": ("raw",)},
        )
        order = graph.topological_order()
        assert order == ("raw", "cleaned")

    def test_topological_order_complex(self) -> None:
        """Test topological sorting with complex DAG."""
        a = Asset(name="a", asset_type=AssetType.TABLE, uri="postgresql://db/a")
        b = Asset(name="b", asset_type=AssetType.TABLE, uri="postgresql://db/b")
        c = Asset(name="c", asset_type=AssetType.TABLE, uri="postgresql://db/c")
        d = Asset(name="d", asset_type=AssetType.TABLE, uri="postgresql://db/d")
        # Structure: d depends on b and c, both b and c depend on a
        graph = AssetGraph(
            name="test_graph",
            assets=(a, b, c, d),
            dependencies={"b": ("a",), "c": ("a",), "d": ("b", "c")},
        )
        order = graph.topological_order()
        # a must come before b and c, which must come before d
        assert order.index("a") < order.index("b")
        assert order.index("a") < order.index("c")
        assert order.index("b") < order.index("d")
        assert order.index("c") < order.index("d")

    def test_topological_order_independent_assets(self) -> None:
        """Test topological sorting with independent assets."""
        asset1 = Asset(name="asset1", asset_type=AssetType.TABLE, uri="postgresql://db/a1")
        asset2 = Asset(name="asset2", asset_type=AssetType.TABLE, uri="postgresql://db/a2")
        asset3 = Asset(name="asset3", asset_type=AssetType.TABLE, uri="postgresql://db/a3")
        graph = AssetGraph(name="test_graph", assets=(asset1, asset2, asset3))
        order = graph.topological_order()
        # All should be present
        assert set(order) == {"asset1", "asset2", "asset3"}

    def test_add_asset_no_dependencies(self) -> None:
        """Test adding an asset without dependencies."""
        asset1 = Asset(name="asset1", asset_type=AssetType.TABLE, uri="postgresql://db/a1")
        graph = AssetGraph(name="test_graph", assets=(asset1,))
        asset2 = Asset(name="asset2", asset_type=AssetType.FILE, uri="s3://bucket/data.csv")
        new_graph = graph.add_asset(asset2)
        assert len(new_graph.assets) == 2
        assert len(graph.assets) == 1  # Original unchanged

    def test_add_asset_with_dependencies(self) -> None:
        """Test adding an asset with dependencies."""
        source = Asset(name="source", asset_type=AssetType.TABLE, uri="postgresql://db/source")
        derived = Asset(name="derived", asset_type=AssetType.TABLE, uri="postgresql://db/derived")
        graph = AssetGraph(name="test_graph", assets=(source,))
        new_graph = graph.add_asset(derived, depends_on=("source",))
        assert len(new_graph.assets) == 2
        assert new_graph.dependencies["derived"] == ("source",)

    def test_add_asset_nonexistent_dependency_raises_error(self) -> None:
        """Test that adding an asset with missing dependencies raises error."""
        asset1 = Asset(name="asset1", asset_type=AssetType.TABLE, uri="postgresql://db/a1")
        graph = AssetGraph(name="test_graph", assets=(asset1,))
        asset2 = Asset(name="asset2", asset_type=AssetType.TABLE, uri="postgresql://db/a2")
        with pytest.raises(ValueError, match="not found in graph"):
            graph.add_asset(asset2, depends_on=("nonexistent",))

    def test_graph_is_frozen(self) -> None:
        """Test that AssetGraph is immutable."""
        asset = Asset(name="test", asset_type=AssetType.TABLE, uri="postgresql://db/test")
        graph = AssetGraph(name="test_graph", assets=(asset,))
        with pytest.raises(Exception):  # FrozenInstanceError
            graph.name = "changed"  # type: ignore[misc]

    def test_graph_with_metadata(self) -> None:
        """Test creating a graph with metadata."""
        asset = Asset(name="test", asset_type=AssetType.TABLE, uri="postgresql://db/test")
        graph = AssetGraph(
            name="test_graph",
            assets=(asset,),
            description="Test pipeline",
            metadata={"owner": "data-team", "environment": "production"},
        )
        assert graph.description == "Test pipeline"
        assert graph.metadata["owner"] == "data-team"
        assert graph.metadata["environment"] == "production"

    def test_get_upstream_dependencies(self) -> None:
        """Test getting upstream dependencies."""
        raw = Asset(name="raw", asset_type=AssetType.TABLE, uri="postgresql://db/raw")
        cleaned = Asset(name="cleaned", asset_type=AssetType.TABLE, uri="postgresql://db/cleaned")
        aggregated = Asset(
            name="aggregated",
            asset_type=AssetType.TABLE,
            uri="postgresql://db/aggregated",
        )
        graph = AssetGraph(
            name="test_graph",
            assets=(raw, cleaned, aggregated),
            dependencies={"cleaned": ("raw",), "aggregated": ("cleaned",)},
        )

        # Get upstream for aggregated
        upstream = graph.get_upstream("aggregated")
        upstream_names = {asset.name for asset in upstream}
        assert upstream_names == {"cleaned", "raw"}

        # Get upstream with depth limit
        upstream_direct = graph.get_upstream("aggregated", depth=1)
        assert len(upstream_direct) == 1
        assert upstream_direct[0].name == "cleaned"

    def test_get_downstream_dependents(self) -> None:
        """Test getting downstream dependents."""
        raw = Asset(name="raw", asset_type=AssetType.TABLE, uri="postgresql://db/raw")
        cleaned = Asset(name="cleaned", asset_type=AssetType.TABLE, uri="postgresql://db/cleaned")
        aggregated = Asset(
            name="aggregated",
            asset_type=AssetType.TABLE,
            uri="postgresql://db/aggregated",
        )
        graph = AssetGraph(
            name="test_graph",
            assets=(raw, cleaned, aggregated),
            dependencies={"cleaned": ("raw",), "aggregated": ("cleaned",)},
        )

        # Get downstream from raw
        downstream = graph.get_downstream("raw")
        downstream_names = {asset.name for asset in downstream}
        assert downstream_names == {"cleaned", "aggregated"}

        # Get downstream with depth limit
        downstream_direct = graph.get_downstream("raw", depth=1)
        assert len(downstream_direct) == 1
        assert downstream_direct[0].name == "cleaned"

    def test_get_lineage_graph(self) -> None:
        """Test getting complete lineage graph."""
        raw = Asset(name="raw", asset_type=AssetType.TABLE, uri="postgresql://db/raw")
        cleaned = Asset(name="cleaned", asset_type=AssetType.TABLE, uri="postgresql://db/cleaned")
        graph = AssetGraph(
            name="test_graph",
            assets=(raw, cleaned),
            dependencies={"cleaned": ("raw",)},
        )

        lineage = graph.get_lineage_graph()
        assert lineage == {"cleaned": ("raw",)}

    def test_to_mermaid(self) -> None:
        """Test exporting graph as Mermaid diagram."""
        raw = Asset(name="raw_data", asset_type=AssetType.TABLE, uri="db://raw")
        cleaned = Asset(name="clean_data", asset_type=AssetType.TABLE, uri="db://clean")
        graph = AssetGraph(
            name="test_graph",
            assets=(raw, cleaned),
            dependencies={"clean_data": ("raw_data",)},
        )

        mermaid = graph.to_mermaid()
        assert "graph TD" in mermaid
        assert "raw_data" in mermaid
        assert "clean_data" in mermaid
        assert "-->" in mermaid

    def test_get_upstream_nonexistent_raises_error(self) -> None:
        """Test that getting upstream for nonexistent asset raises error."""
        asset = Asset(name="test", asset_type=AssetType.TABLE, uri="postgresql://db/test")
        graph = AssetGraph(name="test_graph", assets=(asset,))

        with pytest.raises(ValueError, match="not found in graph"):
            graph.get_upstream("nonexistent")

    def test_get_downstream_nonexistent_raises_error(self) -> None:
        """Test that getting downstream for nonexistent asset raises error."""
        asset = Asset(name="test", asset_type=AssetType.TABLE, uri="postgresql://db/test")
        graph = AssetGraph(name="test_graph", assets=(asset,))

        with pytest.raises(ValueError, match="not found in graph"):
            graph.get_downstream("nonexistent")


class TestMaterializationStrategy:
    """Tests for MaterializationStrategy enum."""

    def test_materialization_strategy_enum_values(self) -> None:
        """Test that MaterializationStrategy enum has all required values."""
        from vibe_piper.types import MaterializationStrategy

        assert MaterializationStrategy.IN_MEMORY
        assert MaterializationStrategy.TABLE
        assert MaterializationStrategy.VIEW
        assert MaterializationStrategy.FILE
        assert MaterializationStrategy.INCREMENTAL

    def test_materialization_strategy_auto_values(self) -> None:
        """Test that enum values are auto-generated."""
        from vibe_piper.types import MaterializationStrategy

        assert MaterializationStrategy.IN_MEMORY.value == 1
        assert MaterializationStrategy.TABLE.value == 2
        assert MaterializationStrategy.VIEW.value == 3
        assert MaterializationStrategy.FILE.value == 4
        assert MaterializationStrategy.INCREMENTAL.value == 5


class TestAssetNewFields:
    """Tests for new Asset fields (version, partition_key)."""

    def test_asset_with_default_version(self) -> None:
        """Test that Asset has default version '1'."""
        asset = Asset(
            name="test_asset",
            asset_type=AssetType.TABLE,
            uri="postgresql://db/test",
        )
        assert asset.version == "1"

    def test_asset_with_custom_version(self) -> None:
        """Test creating an Asset with a custom version."""
        asset = Asset(
            name="test_asset",
            asset_type=AssetType.TABLE,
            uri="postgresql://db/test",
            version="2.0.0",
        )
        assert asset.version == "2.0.0"

    def test_asset_with_partition_key(self) -> None:
        """Test creating an Asset with a partition key."""
        asset = Asset(
            name="test_asset",
            asset_type=AssetType.TABLE,
            uri="postgresql://db/test",
            partition_key="date",
        )
        assert asset.partition_key == "date"

    def test_asset_without_partition_key(self) -> None:
        """Test that Asset partition_key defaults to None."""
        asset = Asset(
            name="test_asset",
            asset_type=AssetType.TABLE,
            uri="postgresql://db/test",
        )
        assert asset.partition_key is None

    def test_asset_backward_compatibility(self) -> None:
        """Test that existing Asset creation still works without new fields."""
        # This test ensures backward compatibility
        asset = Asset(
            name="users_table",
            asset_type=AssetType.TABLE,
            uri="postgresql://localhost/db/users",
        )
        assert asset.name == "users_table"
        assert asset.asset_type == AssetType.TABLE
        assert asset.version == "1"  # Default value
        assert asset.partition_key is None  # Default value
        assert asset.created_at is None  # Default value
        assert asset.updated_at is None  # Default value
        assert asset.checksum is None  # Default value

    def test_asset_with_metadata_timestamps(self) -> None:
        """Test creating an Asset with created_at and updated_at."""
        from datetime import datetime

        now = datetime.now()
        asset = Asset(
            name="test_asset",
            asset_type=AssetType.TABLE,
            uri="postgresql://db/test",
            created_at=now,
            updated_at=now,
        )
        assert asset.created_at == now
        assert asset.updated_at == now

    def test_asset_with_checksum(self) -> None:
        """Test creating an Asset with a checksum."""
        asset = Asset(
            name="test_asset",
            asset_type=AssetType.TABLE,
            uri="postgresql://db/test",
            checksum="abc123def456",
        )
        assert asset.checksum == "abc123def456"

    def test_asset_with_all_metadata(self) -> None:
        """Test creating an Asset with all metadata fields."""
        from datetime import datetime

        now = datetime.now()
        asset = Asset(
            name="test_asset",
            asset_type=AssetType.TABLE,
            uri="postgresql://db/test",
            version="2.0.0",
            partition_key="date",
            created_at=now,
            updated_at=now,
            checksum="abc123",
        )
        assert asset.version == "2.0.0"
        assert asset.partition_key == "date"
        assert asset.created_at == now
        assert asset.updated_at == now
        assert asset.checksum == "abc123"


class TestPipelineNewFields:
    """Tests for new Pipeline checkpoints field."""

    @staticmethod
    def _sample_fn(value: int, ctx: PipelineContext) -> int:
        """Sample operator function."""
        _ = ctx  # Unused
        return value

    def test_pipeline_with_default_checkpoints(self) -> None:
        """Test that Pipeline has empty checkpoints by default."""
        pipeline = Pipeline(name="test_pipeline")
        assert pipeline.checkpoints == ()

    def test_pipeline_with_checkpoints(self) -> None:
        """Test creating a Pipeline with checkpoints."""
        checkpoints = ("checkpoint_1", "checkpoint_2")
        pipeline = Pipeline(
            name="test_pipeline",
            checkpoints=checkpoints,
        )
        assert pipeline.checkpoints == checkpoints
        assert len(pipeline.checkpoints) == 2

    def test_pipeline_backward_compatibility(self) -> None:
        """Test that existing Pipeline creation still works without new field."""
        pipeline = Pipeline(name="test_pipeline")
        assert pipeline.name == "test_pipeline"
        assert pipeline.checkpoints == ()  # Default value
        assert len(pipeline.operators) == 0


class TestExecutionResultNewFields:
    """Tests for new ExecutionResult lineage field."""

    def test_execution_result_with_default_lineage(self) -> None:
        """Test that ExecutionResult has empty lineage by default."""
        from vibe_piper.types import ExecutionResult

        result = ExecutionResult(
            success=True,
            asset_results={},
        )
        assert result.lineage == {}

    def test_execution_result_with_lineage(self) -> None:
        """Test creating an ExecutionResult with lineage."""
        from vibe_piper.types import ExecutionResult

        lineage = {"asset_b": ("asset_a",), "asset_c": ("asset_b",)}
        result = ExecutionResult(
            success=True,
            asset_results={},
            lineage=lineage,
        )
        assert result.lineage == lineage
        assert len(result.lineage) == 2

    def test_execution_result_backward_compatibility(self) -> None:
        """Test that existing ExecutionResult creation still works."""
        from vibe_piper.types import AssetResult, ExecutionResult

        result = ExecutionResult(
            success=True,
            asset_results={"asset_a": AssetResult(asset_name="asset_a", success=True)},
        )
        assert result.success is True
        assert result.lineage == {}  # Default value
        assert len(result.asset_results) == 1
