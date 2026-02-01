---
"id": "vp-5bf1"
"status": "open"
"deps":
- "vp-3fcc"
"links": []
"created": "2026-02-01T00:52:23Z"
"type": "task"
"priority": 2
"assignee": "z3z1ma"
"parent": "vp-ef4a"
"tags":
- "sprint:Phase-3-Core-Abstractions-Cohesiveness"
- "sources"
- "sinks"
- "assetgraph"
"external": {}
---
# Source/Sink asset adapters (async source + schema mapping)

# Source/Sink asset adapters (async source + schema mapping)

## Objective alignment
Sources and sinks exist but are not first-class in AssetGraph execution. Adapters will make them composable with @asset and reduce manual wiring.

## Scope
- Provide adapter helpers (e.g., `source_asset`, `sink_asset` or decorators) that wrap `Source` and `BaseSink` into `Asset` objects with operators.
- Use existing schema mapping utilities (`vibe_piper.schema.map_record_to_schema`) to map nested dicts into `DataRecord` when schema is provided.
- Handle async `Source.fetch` inside the sync execution engine with a safe wrapper.
- Add tests using stub Source/Sink implementations.

## Non-goals
- No new connector implementations.
- No async execution engine redesign.
- No UI or scheduling changes.

## Implementation plan
1. Add a new module (e.g., `src/vibe_piper/asset_adapters.py`) with:
   - `source_asset(name, source: Source, *, schema: Schema | None, asset_type, io_manager, materialization, depends_on)`.
   - `sink_asset(name, sink: BaseSink, *, schema: Schema | None, asset_type, io_manager, materialization, depends_on)`.
2. Implement `source_asset` operator to call `asyncio.run(source.fetch(context))` and map results with `map_record_to_schema` when needed.
3. Implement `sink_asset` operator to accept upstream data, coerce to `DataRecord` if schema provided, and call `sink.write(data, context)`.
4. Add tests in `tests/test_source_sink_adapters.py` with a stub async Source and stub Sink.
5. Export helpers in `src/vibe_piper/__init__.py` (optional feature category).

## Acceptance criteria
- `source_asset` executes `Source.fetch` and returns a list of DataRecords (schema optional).
- `sink_asset` writes upstream data via `BaseSink.write` and returns a `SinkResult` or summary.
- Tests validate mapping via `SchemaField.source_path` and adapter execution.

## Verification
- `uv run pytest tests/test_source_sink_adapters.py -q`

## Risks / edge cases
- `asyncio.run` cannot be called from a running event loop; document behavior or detect and error clearly.
- Mapping required fields may fail if source data missing; ensure errors propagate cleanly.

## Dependencies
- Depends on vp-3fcc if `depends_on` is added for Asset (optional).

## Acceptance Criteria

Source and Sink classes can be wrapped as Assets and executed in AssetGraph
