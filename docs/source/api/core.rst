Core API
========

Pipeline Building
---------------

.. autofunction:: vibe_piper.pipeline.build_pipeline
.. autofunction:: vibe_piper.pipeline.PipelineBuilder
.. autofunction:: vibe_piper.pipeline.PipelineDefinitionContext
.. autofunction:: vibe_piper.pipeline.infer_dependencies_from_signature

Asset Decorator
---------------

.. autofunction:: vibe_piper.decorators.asset
.. autofunction:: vibe_piper.decorators.expect

Core Types
------------

.. autoclass:: vibe_piper.types.PipelineContext
   :members:
   :undoc-members:
.. autoclass:: vibe_piper.types.UpstreamData
   :members:
   :undoc-members:
.. autoclass:: vibe_piper.types.Asset
   :members:
   :undoc-members:
.. autoclass:: vibe_piper.types.AssetGraph
   :members:
   :undoc-members:
.. autoclass:: vibe_piper.types.AssetResult
   :members:
   :undoc-members:
.. autoclass:: vibe_piper.types.ExecutionResult
   :members:
   :undoc-members:

Execution
----------

.. autoclass:: vibe_piper.execution.ExecutionEngine
   :members:
   :undoc-members:
.. autoclass:: vibe_piper.execution.DefaultExecutor
   :members:
   :undoc-members:
