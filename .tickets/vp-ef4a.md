---
"id": "vp-ef4a"
"status": "closed"
"deps": []
"links": []
"created": "2026-02-01T00:40:38Z"
"type": "task"
"priority": 1
"assignee": "z3z1ma"
"tags":
- "sprint:Phase-3-Core-Abstractions-Cohesiveness"
- "fanout"
"external": {}
---
# Sprint prep: Phase 3: Core Abstractions & Cohesiveness

Objective:
Create the most robust python based declarative data pipeline, integration, quality, transformation, activation library ever created. Our zen is simplicity, expressiveness, composability, and maximizing function. The UX must be intuitive. Everything must work. Use TDD. This should be the most ambitious project ever created. Turn the industry on it's head. Take your time. Weeks if you must. Take the learnings from every framework declarative or otherwise ever produced in history regarding data and improve on it. Whenever out of tickets, file more. This should be like the best parts of airflow, dagster, dlt (data load tool), dbt, and so on in one tool that is beautiful and simple. We are lacking cohesiveness. And lacking the right abstractions to provide massive value, composability, expressiveness.

Sprint prep deliverable (fill this ticket in, then create tickets):

## Sprint Brief

Required sections:
- Objective restatement: Build a cohesive, production-grade declarative data pipeline library in Python; the immediate goal is to make the canonical AssetGraph model feel complete, consistent, and actually executable across the documented API surface.
- Sprint focus (2-5 words): Cohesive AssetGraph APIs
- Why this sprint focus is the best next step: The repo has strong primitives (Asset, AssetGraph, PipelineBuilder, Source/Sink, SQL assets) but they are not wired together consistently. Docs advertise workflows (build_pipeline from @asset, sql_asset usage, source_path mapping) that the code does not support. Aligning dependency declaration, SQL assets, and source/sink adapters removes confusion and unlocks real composability without inventing new surface area.
- Current state:
  - Existing tickets that matter: Only vp-ef4a is open. CORE_ABSTRACTION_CONTRACT.md (ADR vp-8783) defines canonical model; investigation_missing_abstractions.md documents missing source/sink and schema-first mapping.
  - Codebase state that matters (git status/log, key modules): git status clean except untracked .venv. Key modules: src/vibe_piper/types.py (Asset/AssetGraph), src/vibe_piper/pipeline.py (PipelineBuilder + dependency inference), src/vibe_piper/decorators.py + asset_factory.py (@asset factory), src/vibe_piper/execution.py (ExecutionEngine), src/vibe_piper/sql_assets.py (decorator + validation stubs, no execution integration), src/vibe_piper/sources/* + sinks/* (async sources, sinks with DDL). README.md and docs/ show build_pipeline(load_asset) and sql_asset usage not matched in code.
- Risks + unknowns (and how we'll resolve them):
  - SQL asset execution semantics (connector/IO manager selection, template substitution) are undefined; resolve by defining a minimal contract and tests in a focused ticket before expanding.
  - Source/Sink adapters are async while execution engine is sync; resolve with a safe sync wrapper (asyncio.run) and document limitations or create async executor if needed.
  - Dependency declaration for @asset is missing; resolve by adding explicit depends_on support and a graph builder that reads dependencies, with backwards compatibility tests.

## Ticket Set

Create the sprint tickets directly. This sprint prep ticket should be the parent.
- Tag rule: include `sprint:Phase-3-Core-Abstractions-Cohesiveness` on sprint tickets.
- Prefer: `loom ticket create ... --parent <THIS_TICKET_ID> --acceptance "..."`

## Output

When done, update THIS ticket with:
- Created/updated ticket IDs: [vp-3fcc, vp-92d4, vp-5bf1, vp-d016]
- Suggested ordering + what can run in parallel: Start vp-3fcc. Then vp-92d4 and vp-5bf1 can run in parallel. Finish with vp-d016 after API shapes settle.

Sprint name: Phase 3: Core Abstractions & Cohesiveness
Sprint tag: sprint:Phase-3-Core-Abstractions-Cohesiveness

## Notes

**2026-02-01T00:53:10Z**

Created sprint tickets: vp-3fcc (asset dependencies + build_asset_graph), vp-92d4 (sql_asset execution), vp-5bf1 (source/sink adapters), vp-d016 (docs alignment). Added dependencies: vp-92d4/vp-5bf1 -> vp-3fcc; vp-d016 -> vp-3fcc/vp-92d4/vp-5bf1.
