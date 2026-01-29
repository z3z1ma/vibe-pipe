Development Guide
=================

This guide is for developers who want to work on Vibe Piper itself.

Development Setup
-----------------

See the :doc:`contributing` guide for detailed setup instructions.

Quick Start:

.. code-block:: bash

   git clone https://github.com/your-org/vibe-piper.git
   cd vibe-piper
   uv sync --dev
   uv pip install -e ".[dev]"

Architecture Overview
---------------------

Vibe Piper is built around these core concepts:

**Pipeline**: A container for stages that executes them in sequence
**Stage**: A single transformation step
**Type Safety**: Full type hints throughout the codebase

Design Principles
-----------------

1. **Simplicity**: APIs should be intuitive and easy to understand
2. **Composability**: Small pieces that combine in powerful ways
3. **Type Safety**: Catch errors at development time, not runtime
4. **Declarative**: Describe *what* to do, not *how* to do it

Testing Strategy
----------------

Test Organization
~~~~~~~~~~~~~~~~~

Tests are organized by module:

.. code-block:: text

   tests/
   ├── test_core.py          # Tests for core components
   ├── test_stages.py        # Tests for built-in stages
   └── test_integration.py   # Integration tests

Running Tests
~~~~~~~~~~~~~

Run all tests:

.. code-block:: bash

   pytest

Run specific test file:

.. code-block:: bash

   pytest tests/test_core.py

Run with coverage:

.. code-block:: bash

   pytest --cov=vibe_piper --cov-report=html

Code Quality
------------

Formatting
~~~~~~~~~~

We use Ruff for code formatting:

.. code-block:: bash

   uv run ruff format src tests

Configuration is in ``pyproject.toml``.

Linting
~~~~~~~

We use Ruff for fast linting:

.. code-block:: bash

   ruff check src/ tests/

Auto-fix issues:

.. code-block:: bash

   ruff check --fix src/ tests/

Type Checking
~~~~~~~~~~~~~

We use MyPy for static type checking:

.. code-block:: bash

   mypy src/

Adding New Features
-------------------

1. Create a feature branch:

.. code-block:: bash

   git checkout -b feature/your-feature

2. Implement the feature with:
   * Type hints on all functions
   * Docstrings in Google or NumPy style
   * Comprehensive tests

3. Update documentation

4. Run quality checks:

.. code-block:: bash

   pytest
   uv run ruff format src tests
   ruff check src/ tests/
   mypy src/

5. Build documentation to ensure no doc errors:

.. code-block:: bash

   cd docs && make html

Release Process
---------------

Releases are managed by the maintainers:

1. Update version in ``pyproject.toml``
2. Update CHANGELOG.md
3. Create a git tag
4. Build and publish to PyPI

Performance Considerations
---------------------------

* Minimize memory copies in pipeline stages
* Use generators for large datasets
* Profile before optimizing
* Document performance characteristics

Debugging
---------

Enable verbose logging:

.. code-block:: python

   import logging
   logging.basicConfig(level=logging.DEBUG)

Use pytest debugger:

.. code-block:: bash

   pytest --pdb
