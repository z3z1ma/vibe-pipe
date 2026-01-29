"""
Base documentation generator.

Provides the abstract base class and common utilities for all documentation generators.
"""

from abc import ABC, abstractmethod
from collections.abc import Mapping
from pathlib import Path
from typing import Any


class DocumentationGenerator(ABC):
    """
    Abstract base class for documentation generators.

    All documentation generators (schema, catalog, lineage, etc.) should
    inherit from this class and implement the generate method.
    """

    def __init__(
        self,
        output_dir: Path | str,
        template_dir: Path | str | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        """
        Initialize the documentation generator.

        Args:
            output_dir: Directory where documentation will be written
            template_dir: Optional custom template directory
            context: Optional additional context for template rendering
        """
        self.output_dir = Path(output_dir)
        self.template_dir = Path(template_dir) if template_dir else None
        self.context = dict(context) if context else {}

    @abstractmethod
    def generate(self, **kwargs: Any) -> None:
        """
        Generate documentation.

        This method must be implemented by subclasses to generate
        their specific type of documentation.
        """
        pass

    def ensure_output_dir(self) -> None:
        """Create the output directory if it doesn't exist."""
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def get_context(self, additional_context: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """
        Get the full rendering context.

        Args:
            additional_context: Optional additional context to merge

        Returns:
            Combined context dictionary
        """
        ctx = dict(self.context)
        if additional_context:
            ctx.update(additional_context)
        return ctx
