"""
Vibe Piper - Declarative Data Pipeline Library.

Vibe Piper is a robust Python-based declarative data pipeline, integration,
quality, transformation, and activation library designed for simplicity,
expressiveness, and composability.

Example:
    Basic usage example::

        from vibe_piper import Pipeline

        pipeline = Pipeline(name="my_pipeline")
        # Add your pipeline stages here
        result = pipeline.run(data)

This library is currently in early development (Phase 0: Foundation).
"""

__version__ = "0.1.0"

from vibe_piper.core import Pipeline, Stage

__all__ = ["Pipeline", "Stage", "__version__"]
