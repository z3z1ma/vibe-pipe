#!/usr/bin/env python
"""Test script to debug transform asset."""

import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

from vibe_piper.execution import ExecutionEngine
from vibe_piper.pipeline import PipelineDefinitionContext


def extract(upstream_data, ctx) -> list:
    logger.info(f"Extract called, upstream_data type: {type(upstream_data)}")
    logger.info(
        f"Extract keys: {list(upstream_data.keys()) if hasattr(upstream_data, 'keys') else 'no keys method'}"
    )
    return [{"a": 1, "b": 2}]


def transform(upstream_data, ctx) -> list:
    logger.info(f"Transform called, upstream_data type: {type(upstream_data)}")
    logger.info(f"Transform dir: {dir(upstream_data)}")

    # Try to access extract data
    if hasattr(upstream_data, "get"):
        extract_data = upstream_data.get("extract", [])
    elif hasattr(upstream_data, "__getitem__"):
        extract_data = upstream_data.extract
    else:
        extract_data = []

    logger.info(f"Extract data: {extract_data}")

    processed = []
    for row in extract_data:
        processed.append(row.copy())
    return processed


with PipelineDefinitionContext("test") as pipeline:

    @pipeline.asset(description="Extract")
    def extract(upstream_data, ctx):
        return extract(upstream_data, ctx)

    @pipeline.asset(description="Transform")
    def transform(upstream_data, ctx):
        return transform(upstream_data, ctx)


graph = pipeline.build()
engine = ExecutionEngine()
result = engine.execute(graph)

logger.info(f"Result: {result}")
if not result.success:
    logger.error(f"Failed: {result.errors}")
