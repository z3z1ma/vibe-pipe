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

   from vibe_piper import asset, build_pipeline, CSVReader, CSVWriter
   from pathlib import Path

   # Define assets using @asset decorator
   @asset
   def extract_users() -> list[dict]:
       """Extract user data from CSV."""
       reader = CSVReader(Path("data/users.csv"))
       records = reader.read()
       return [record.data for record in records]

   @asset
   def transform_users(extract_users: list[dict]) -> list[dict]:
       """Transform and filter users."""
       # Filter active users
       active_users = [user for user in extract_users if user.get("status") == "active"]
       return active_users

   @asset
   def aggregate_by_category(transform_users: list[dict]) -> list[dict]:
       """Aggregate users by category."""
       from collections import Counter
       categories = Counter(user.get("category", "unknown") for user in transform_users)
       return [{"category": k, "count": v} for k, v in categories.items()]

   # Build and execute pipeline
   pipeline = build_pipeline("user_pipeline")
   # Assets are automatically added to the builder when using @asset decorator
   # Or add assets explicitly: pipeline.asset("name", fn=func)
   graph = pipeline.build()
   print(f"Pipeline graph: {graph.name}")

   # Note: Execution is handled by the execution engine
   # See getting_started.rst for full execution examples


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
