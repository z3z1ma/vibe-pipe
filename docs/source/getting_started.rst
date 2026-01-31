Getting Started
===============

Installation
------------

Install Vibe Piper using pip:

.. code-block:: bash

   pip install vibe-piper

Or for development:

.. code-block:: bash

   git clone https://github.com/your-org/vibe-piper.git
   cd vibe-piper
   uv sync --dev

Basic Usage
-----------

Defining Assets
~~~~~~~~~~~~~~~

Assets are the building blocks of Vibe Piper pipelines. Use the ``@asset`` decorator to define data transformations:

.. code-block:: python

   from vibe_piper import asset, build_pipeline

   @asset
   def source_data() -> list[dict]:
       """Extract source data."""
       return [
           {"id": 1, "name": "Alice", "score": 85},
           {"id": 2, "name": "Bob", "score": 92},
           {"id": 3, "name": "Charlie", "score": 78},
       ]

   @asset
   def filtered_users(source_data: list[dict]) -> list[dict]:
       """Filter users with high scores."""
       return [user for user in source_data if user["score"] > 80]

   @asset
   def aggregated_data(filtered_users: list[dict]) -> dict:
       """Aggregate user statistics."""
       return {
           "count": len(filtered_users),
           "avg_score": sum(u["score"] for u in filtered_users) / len(filtered_users),
       }

Building Pipelines
~~~~~~~~~~~~~~~~~

Vibe Piper provides multiple ways to build pipelines:

**Using @asset decorator with automatic inference:**

.. code-block:: python

   # Assets are automatically collected in a PipelineBuilder
   # when using @asset decorator (see example above)

**Using PipelineBuilder (fluent interface):**

.. code-block:: python

   from vibe_piper import build_pipeline, AssetType

   pipeline = build_pipeline("my_pipeline")
   pipeline.asset(
       name="source",
       fn=lambda: [1, 2, 3],
       asset_type=AssetType.MEMORY,
   )
   pipeline.asset(
       name="doubled",
       fn=lambda source: [x * 2 for x in source],
       depends_on=["source"],  # Optional: dependencies can be explicit
   )

   graph = pipeline.build()

**Using PipelineDefinitionContext (declarative syntax):**

.. code-block:: python

   from vibe_piper import PipelineDefinitionContext

   with PipelineDefinitionContext("my_pipeline") as pipeline:
       @pipeline.asset()
       def raw_data():
           return [1, 2, 3]

       @pipeline.asset()
       def processed_data(raw_data):
           return [x * 2 for x in raw_data]

   graph = pipeline.build()

**Note:** Dependencies are automatically inferred from function parameter names that match existing asset names (e.g., ``raw_data`` parameter in ``processed_data`` depends on ``raw_data`` asset).

Multi-Upstream Assets
~~~~~~~~~~~~~~~~~~~~

When an asset depends on multiple upstream assets, use the parameter name that matches the pattern:

.. code-block:: python

   from vibe_piper import UpstreamData

   @asset
   def multi_source_data(users: list[dict], orders: list[dict]) -> list[dict]:
       """
       Combine user and order data.

       When multiple dependencies exist, you can access them individually
       by parameter name, or use UpstreamData for structured access.
       """
       # Users and orders are individual parameters
       # Access by parameter name
       user_ids = {u["id"] for u in users}
       for order in orders:
           if order["user_id"] in user_ids:
               order["user_exists"] = True
       return orders

Pipeline Execution
~~~~~~~~~~~~~~~~~

Execute pipelines using the execution engine:

.. code-block:: python

   from vibe_piper import ExecutionEngine, PipelineContext

   # Create execution context
   context = PipelineContext(
       pipeline_id="my_pipeline",
       run_id="run_001",
       config={"log_level": "INFO"},
   )

   # Execute the asset graph
   engine = ExecutionEngine()
   result = engine.execute(graph, context)

   if result.success:
       print(f"Pipeline succeeded! Executed {result.assets_succeeded} assets.")
   else:
       print(f"Pipeline failed: {result.errors}")

Configuration
~~~~~~~~~~~~

Configure pipelines with PipelineContext:

.. code-block:: python

   from vibe_piper import PipelineContext

   context = PipelineContext(
       pipeline_id="my_pipeline",
       run_id="run_001",
       config={
           "checkpoint_dir": "./checkpoints",
           "log_level": "DEBUG",
           "max_workers": 4,
       },
   )

Next Steps
----------

* Check out :doc:`api/index` for detailed API documentation
* See :doc:`development` for development setup and guidelines
* Read :doc:`contributing` to learn how to contribute
