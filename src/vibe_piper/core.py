"""
Core pipeline components for Vibe Piper.

This module provides the fundamental building blocks for creating
declarative data pipelines.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

T = TypeVar("T")
R = TypeVar("R")


@dataclass
class Stage(Generic[T, R]):
    """
    A single stage in a data pipeline.

    A Stage represents a transformation step that takes input of type T
    and produces output of type R.

    Attributes:
        name: The name of this stage for identification and logging.
        transform: A callable that performs the transformation.
        description: Optional description of what this stage does.

    Example:
        Create a simple transformation stage::

            def uppercase(text: str) -> str:
                return text.upper()

            stage = Stage(name="uppercase", transform=uppercase)
            result = stage.transform("hello")  # Returns "HELLO"
    """

    name: str
    transform: Callable[[T], R]
    description: str = ""

    def __call__(self, data: T) -> R:
        """
        Execute the stage transformation.

        Args:
            data: Input data to transform.

        Returns:
            Transformed data.

        Raises:
            Exception: If the transform function raises an exception.
        """
        return self.transform(data)


@dataclass
class Pipeline(Generic[T]):
    """
    A declarative data pipeline composed of multiple stages.

    Pipelines allow you to chain together multiple transformations
    in a declarative, composable way.

    Attributes:
        name: The name of the pipeline.
        stages: List of stages that will be executed in order.
        description: Optional description of the pipeline's purpose.

    Example:
        Create a multi-stage pipeline::

            pipeline = Pipeline(name="data_processor")

            pipeline.add_stage(
                Stage(name="clean", transform=lambda x: x.strip())
            )
            pipeline.add_stage(
                Stage(name="uppercase", transform=lambda x: x.upper())
            )

            result = pipeline.run("  hello  ")  # Returns "HELLO"
    """

    name: str
    stages: list[Stage] = field(default_factory=list)
    description: str = ""

    def add_stage(self, stage: Stage) -> None:
        """
        Add a stage to the pipeline.

        Args:
            stage: The Stage to add to the pipeline.

        Raises:
            ValueError: If a stage with the same name already exists.
        """
        if any(s.name == stage.name for s in self.stages):
            raise ValueError(f"Stage '{stage.name}' already exists in pipeline")
        self.stages.append(stage)

    def run(self, data: T) -> Any:
        """
        Execute all stages in the pipeline.

        Stages are executed in the order they were added.

        Args:
            data: Input data to process through the pipeline.

        Returns:
            The final output after all stages have been executed.

        Raises:
            Exception: If any stage raises an exception.
        """
        result = data
        for stage in self.stages:
            result = stage(result)
        return result
