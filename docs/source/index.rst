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

* **Declarative Pipeline Definition**: Build data pipelines using a clean, declarative syntax
* **Composable Stages**: Chain transformations together in a flexible, reusable way
* **Type Safety**: Full type hint support for better IDE integration and fewer runtime errors
* **Simple and Expressive**: Intuitive API that makes complex data transformations easy

Quick Example
-------------

.. code-block:: python

   from vibe_piper import Pipeline, Stage

   # Create a pipeline
   pipeline = Pipeline(name="data_processor")

   # Add stages
   pipeline.add_stage(
       Stage(name="clean", transform=lambda x: x.strip())
   )
   pipeline.add_stage(
       Stage(name="uppercase", transform=lambda x: x.upper())
   )

   # Run the pipeline
   result = pipeline.run("  hello  ")  # Returns "HELLO"


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
