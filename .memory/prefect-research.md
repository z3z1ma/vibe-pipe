# Prefect Research Memo

**Date:** 2026-01-27
**Framework:** Prefect
**Focus:** Workflow Orchestration & AI Infrastructure

## Executive Summary

Prefect is a modern workflow orchestration framework that prioritizes developer experience and Pythonic design patterns. Key differentiators include hybrid architecture, decorator-based API, and focus on "turning scripts into production workflows."

## Core Architecture

### Design Philosophy
- **Python-first**: Uses standard Python decorators and functions rather than DSL
- **Hybrid architecture**: Code and data stay on user's infrastructure; orchestration layer is separate
- **Workflow as code**: Treat workflows as first-class Python objects
- **Event-driven**: Reactive to workflow events and states

### Key Components

1. **Tasks & Flows**
   - `@task` decorator: Atomic units of work
   - `@flow` decorator: Orchestrates tasks into workflows
   - Dynamic task generation and parameterization
   - Native async support

2. **Execution Model**
   - Workers poll for work (pull-based architecture)
   - Supports multiple executors (local, Docker, Kubernetes)
   - State machine for task lifecycle (pending, running, completed, failed)
   - Automatic retries and error handling

3. **Orchestration Features**
   - Scheduling (cron-based, ad-hoc)
   - Deployment management
   - Parameter handling
   - Caching and memoization
   - Concurrent execution control

## Notable Patterns for VibePiper

### Strengths to Emulate

1. **Decorator-Based API**
   ```python
   @task
   def extract_data(source: str) -> pd.DataFrame:
       # Extraction logic
       pass

   @flow
   def my_pipeline(source: str):
       data = extract_data(source)
       return transform_data(data)
   ```
   - Clean, readable syntax
   - Familiar to Python developers
   - Minimal boilerplate

2. **State Management**
   - Rich state tracking (not just success/failure)
   - State handlers for custom logic
   - Visual state transitions in UI

3. **Infrastructure Flexibility**
   - Run anywhere (local, cloud, containers)
   - No vendor lock-in
   - Separate control plane from execution

4. **Observability**
   - Built-in logging
   - UI for workflow visualization
   - Parameter tracking
   - Execution history

### Anti-Patterns to Avoid

1. **Over-abstraction**: Can be too flexible, leading to inconsistent patterns
2. **DAG purity violations**: Prefect intentionally breaks DAG purity for dynamic workflows
   - Lesson: VibePiper should decide where to draw this line
3. **Complex deployment model**: Multiple deployment options can be confusing

## Key Takeaways for VibePiper Design

1. **API Design**
   - Decorator-based syntax is proven and popular
   - Function-first approach wins over class-based APIs
   - Automatic dependency inference (like Prefect's task inputs) reduces boilerplate

2. **Architecture**
   - Hybrid architecture (control plane separate from execution) is popular
   - Workers polling for work is scalable and resilient
   - State machines provide better observability than boolean results

3. **Developer Experience**
   - "Script to production" narrative is compelling
   - Local development should be seamless
   - Clear separation between definition and execution

4. **Missing Opportunities**
   - Prefect lacks first-class data quality/validation
   - Limited transformation framework (focuses on orchestration)
   - No built-in asset management (Dagster does this better)

## Integration Ideas for VibePiper

1. **Similar decorator pattern**: `@asset` decorator aligns well with Prefect's `@task`
2. **State machine**: VibePiper could adopt richer state tracking for asset execution
3. **Hybrid execution**: Support both in-process and distributed execution models
4. **Observability first**: Build in logging/metrics from day one

## References

- https://www.prefect.io/
- https://github.com/PrefectHQ/prefect
- https://www.prefect.io/how-it-works
