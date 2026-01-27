"""
Unit tests for core type system.

Tests all core types, their validation rules, and ensure type safety
with mypy strict mode.
"""

import pytest

from vibe_piper.types import (
    Asset,
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
        op = Operator(
            name="test", operator_type=OperatorType.TRANSFORM, fn=self.sample_fn
        )
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
        op2 = Operator(
            name="op2", operator_type=OperatorType.TRANSFORM, fn=self.sample_fn
        )
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
        op1 = Operator(
            name="duplicate", operator_type=OperatorType.SOURCE, fn=self.sample_fn
        )
        op2 = Operator(
            name="duplicate", operator_type=OperatorType.TRANSFORM, fn=self.sample_fn
        )
        with pytest.raises(ValueError, match="Duplicate operator names"):
            Pipeline(name="test_pipeline", operators=(op1, op2))

    def test_add_operator(self) -> None:
        """Test adding an operator to a pipeline."""
        op1 = Operator(name="op1", operator_type=OperatorType.SOURCE, fn=self.sample_fn)
        op2 = Operator(
            name="op2", operator_type=OperatorType.TRANSFORM, fn=self.sample_fn
        )
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

        op1 = Operator(
            name="double", operator_type=OperatorType.TRANSFORM, fn=double_fn
        )
        op2 = Operator(
            name="add_ten", operator_type=OperatorType.TRANSFORM, fn=add_ten_fn
        )
        pipeline = Pipeline(name="test_pipeline", operators=(op1, op2))
        ctx = PipelineContext(pipeline_id="test_pipeline", run_id="run_1")

        result = pipeline.execute(5, context=ctx)
        assert result == 20  # (5 * 2) + 10

    def test_execute_without_context(self) -> None:
        """Test executing a pipeline without providing a context."""

        def identity_fn(value: str, ctx: PipelineContext) -> str:
            return value

        op = Operator(
            name="identity", operator_type=OperatorType.TRANSFORM, fn=identity_fn
        )
        pipeline = Pipeline(name="test_pipeline", operators=(op,))

        result = pipeline.execute("test")
        assert result == "test"

    def test_execute_with_data_records(self) -> None:
        """Test executing a pipeline with DataRecord objects."""
        schema = Schema(
            name="test",
            fields=(SchemaField(name="value", data_type=DataType.INTEGER),),
        )

        def double_records(
            records: list[DataRecord], ctx: PipelineContext
        ) -> list[DataRecord]:
            return [
                DataRecord(
                    data={"value": r.get("value") * 2},
                    schema=r.schema,
                )
                for r in records
            ]

        op = Operator(
            name="double", operator_type=OperatorType.TRANSFORM, fn=double_records
        )
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
