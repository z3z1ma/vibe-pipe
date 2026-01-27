# Apache Airflow Research Memo

**Date:** 2026-01-27
**Framework:** Apache Airflow
**Focus:** DAG-Based Workflow Orchestration

## Executive Summary

Apache Airflow is the industry standard for workflow orchestration, pioneered the DAG (Directed Acyclic Graph) model. While powerful, it shows signs of age (complex setup, heavy resource usage, pipeline-as-code in Python but DAG definition is verbose). Airflow 3 (2026) modernizes but adds migration complexity.

## Core Architecture

### Design Philosophy
- **DAG-based everything**: All workflows are DAGs
- **Infrastructure as code**: DAGs defined in Python files
- **Task-oriented**: Focus on what tasks to run, not what data to produce
- **Scheduler-driven**: Scheduler decides when to run tasks

### Key Components

1. **DAGs**
   ```python
   from airflow import DAG
   from airflow.operators.python import PythonOperator

   with DAG('my_dag', start_date=days_ago(1)) as dag:
       task1 = PythonOperator(
           task_id='extract',
           python_callable=extract_fn
       )
       task2 = PythonOperator(
           task_id='transform',
           python_callable=transform_fn
       )
       task1 >> task2  # Dependency syntax
   ```
   - Directed Acyclic Graph structure
   - Explicit task dependencies
   - Schedules defined per DAG
   - DAG file = one Python file

2. **Operators**
   - Pre-built operators for common tasks (Postgres, S3, Spark, etc.)
   - 1000+ community operators
   - Can write custom operators
   - Strong ecosystem but fragmented

3. **Scheduler**
   - Heartbeat-based scheduling
   - Queues tasks to workers
   - Handles retry logic
   - Separate from executor (runs tasks)

4. **Executor**
   - LocalExecutor (development)
   - CeleryExecutor (production)
   - KubernetesExecutor (cloud-native)
   - Pluggable architecture

## Notable Patterns for VibePiper

### Strengths to Emulate

1. **Mature Ecosystem**
   - Operator for every integration
   - Battle-tested at scale
   - Large community knowledge base
   - Airflow 3 modernization (2026)

2. **Standard Patterns**
   - Jinja templating for parameters
   - XCom for task communication
   - Hooks for external systems
   - Clear separation of concerns

3. **Observability**
   - Web UI for DAG visualization
   - Task instance history
   - Log aggregation
   - Metrics and monitoring

4. **Scheduling Flexibility**
   - Cron expressions
   - Timetables (Airflow 3)
   - Manual triggers
   - Data-driven scheduling (sensor-based)

### Anti-Patterns to Avoid

1. **Heavy Infrastructure** ⚠️
   - Requires database, scheduler, webserver, workers
   - Complex deployment and maintenance
   - Resource-intensive
   - Lesson: Keep VibePiper lightweight

2. **Pipeline as Code... Mostly**
   - DAG files are Python
   - But Jinja templating breaks static analysis
   - No type safety
   - Hard to test in isolation
   - Lesson: Stay pure Python, avoid templating

3. **Task-Centric Blindness**
   - Focuses on tasks, not data
   - No first-class data lineage
   - Limited data quality features
   - Lesson: Assets > Tasks

4. **Global Variables Problem**
   - DAGs loaded dynamically from files
   - Top-level code executes on load
   - Can cause performance issues
   - Lesson: Lazy initialization

5. **XCom Anti-Pattern**
   - Task communication via side-channel
   - Not explicit in function signatures
   - Type-unsafe
   - Lesson: Pass data explicitly

## Key Takeaways for VibePiper Design

1. **What Airflow Does Right**
   - Clear visualization of DAG structure
   - Standardized retry/backoff patterns
   - Web UI for monitoring
   - Pluggable executor architecture

2. **What Airflow Gets Wrong**
   - Too heavy for many use cases
   - Task-centric vs. asset-centric
   - Weak typing (Python files but no type safety)
   - Jinja templating (breaks IDE tooling)
   - No built-in data validation
   - Complex setup and maintenance

3. **Architectural Lessons**
   - **Keep it lightweight**: Don't require separate infrastructure components
   - **Pure Python**: Avoid templating languages
   - **First-class data**: Assets not tasks
   - **Type-safe**: Leverage Python type hints
   - **Easy local development**: Should work on a laptop

4. **Deployment Model**
   - Airflow requires complex deployment
   - VibePiper should be:
     - Library-first (embed in apps)
     - Optional server components
     - Container-friendly
     - Cloud-native from day one

## Comparison with VibePiper (Current State)

### What VibePiper Does Better (Already)
- ✅ Type-safe decorators with full Python support
- ✅ Asset-centric model (Dagster-like, not Airflow-like)
- ✅ Automatic dependency inference (no explicit `>>` syntax needed)
- ✅ Pure Python (no Jinja)
- ✅ Lightweight (no database, scheduler, etc.)
- ✅ Schema validation built-in
- ✅ Function signatures show data flow

### What VibePiper Could Learn
- ❌ Web UI for visualization (but keep optional!)
- ❌ Standard retry patterns
- ❌ Scheduling abstractions (but keep simple)
- ❌ Better logging/observability patterns
- ❌ Executor abstraction for scaling

## Integration Ideas for VibePiper

1. **Optional Web UI**
   - Not required for operation
   - Read-only visualization of asset graph
   - Simple Flask/FastAPI server
   - Embeddable in existing apps

2. **Retry Decorator**
   ```python
   @asset(retries=3, backoff=exponential)
   def flaky_data():
       pass
   ```
   - Built-in retry logic
   - Configurable backoff strategies
   - Don't build full scheduler

3. **Executor Pattern**
   ```python
   executor = LocalExecutor()  # Default
   executor = ThreadPoolExecutor(max_workers=10)
   executor = DaskExecutor(cluster_address)
   ```
   - Pluggable execution
   - Start simple, add complexity as needed
   - Not required for basic usage

4. **DAG Export**
   - Export asset graph as Airflow DAG
   - Interoperability with existing Airflow deployments
   - Migration path from Airflow to VibePiper

5. **Scheduling Integration**
   ```python
   @asset(schedule="0 2 * * *")  # cron
   def daily_report():
       pass
   ```
   - Lightweight scheduling
   - Use APScheduler or similar
   - Don't build custom scheduler

## References

- https://airflow.apache.org/
- https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html
- https://www.astronomer.io/blog/upgrading-airflow-2-to-airflow-3-a-checklist-for-2026
