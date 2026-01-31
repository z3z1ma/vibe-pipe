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

   from vibe_piper import asset, PipelineBuilder, CSVReader, CSVWriter
   from pathlib import Path

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
