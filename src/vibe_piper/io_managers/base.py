"""
Base IO Manager adapter class.

This module provides the base adapter class that all IO managers should inherit from.
It implements the IOManager protocol from the type system.
"""

from abc import ABC, abstractmethod
from typing import Any

from vibe_piper.types import IOManager, PipelineContext


class IOManagerAdapter(IOManager, ABC):
    """
    Base adapter class for IO managers.

    This class provides a default implementation of the IOManager protocol
    and should be inherited by all concrete IO manager implementations.

    Example:
        Create a custom IO manager::

            class MyIOManager(IOManagerAdapter):
                def handle_output(self, context, data):
                    # Store data
                    pass

                def load_input(self, context):
                    # Load data
                    pass
    """

    @abstractmethod
    def handle_output(self, context: PipelineContext, data: Any) -> None:
        """
        Store asset output data.

        Args:
            context: The pipeline execution context
            data: The data to store

        Raises:
            Exception: If storage fails
        """
        ...

    @abstractmethod
    def load_input(self, context: PipelineContext) -> Any:
        """
        Load asset input data.

        Args:
            context: The pipeline execution context

        Returns:
            The loaded data

        Raises:
            Exception: If loading fails
        """
        ...
