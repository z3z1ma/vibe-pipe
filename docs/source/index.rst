.. Vibe Piper documentation master file, created by sphinx-quickstart.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Vibe Piper's documentation!
======================================

Vibe Piper is a robust Python-based declarative data pipeline, integration,
quality, transformation, and activation library designed for simplicity,
expressiveness, and composability.

.. note::
   This project is currently in early development (Phase 0: Foundation).

Getting Started
---------------

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   getting_started
   execution_layering

Core Concepts
------------

.. toctree::
   :maxdepth: 2

   ../type_system.md
   ../declarative_schema_design.md
   ../migration_pipeline_to_assetgraph.md

Features
---------

.. toctree::
   :maxdepth: 2

   ../scheduling.md
   ../transformations.md
   ../config_driven_pipelines.md
   ../sql_assets.md
   ../database_connectors.md

Data Quality
------------

.. toctree::
   :maxdepth: 2

   ../anomaly_detection.md
   ../quality-scoring.md
   ../external_quality_integration.md
   ../validation_history.md

Development
-----------

.. toctree::
   :maxdepth: 2

   ../testing_patterns.md
   api/index
   contributing
   development

Features
--------

* **Declarative Pipeline Definition**: Build data pipelines using @asset decorator with automatic dependency inference
* **Type Safety**: Full type hint support for better IDE integration and fewer runtime errors
* **Asset-Based Design**: Define reusable data assets with clear dependencies
* **Flexible Execution**: Multiple pipeline building styles (decorator, builder, context manager)
* **Multi-Upstream Support**: Handle complex dependencies with UpstreamData type
* **Data Quality**: Built-in expectations and validation rules

Quick Example
-------------

.. code-block:: python

   from vibe_piper import build_pipeline

   # Build pipeline using explicit builder pattern
   pipeline = build_pipeline("user_pipeline")

   # Define and add assets to pipeline
   pipeline.asset(
       name="extract_users",
       fn=lambda: [
           {"id": 1, "name": "Alice", "status": "active"},
           {"id": 2, "name": "Bob", "status": "inactive"},
       ],
       asset_type="memory",
   )

   pipeline.asset(
       name="transform_users",
       fn=lambda extract_users: [u for u in extract_users if u.get("status") == "active"],
       depends_on=["extract_users"],
   )

   # Build the asset graph
   graph = pipeline.build()
   print(f"Pipeline graph: {graph.name}")

   # Note: See getting_started.rst for @asset decorator usage
   # and execution with ExecutionEngine


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
