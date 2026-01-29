Contributing Guidelines
=======================

Thank you for your interest in contributing to Vibe Piper!

Getting Started
---------------

Prerequisites
~~~~~~~~~~~~~

* Python 3.10 or higher
* `UV <https://github.com/astral-sh/uv>`_ for package management
* Git

Development Setup
~~~~~~~~~~~~~~~~~

1. Fork and clone the repository:

.. code-block:: bash

   git clone https://github.com/your-username/vibe-piper.git
   cd vibe-piper

2. Install dependencies using UV:

.. code-block:: bash

   uv sync --dev

3. Create a virtual environment and activate it:

.. code-block:: bash

   uv venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate

4. Install the package in development mode:

.. code-block:: bash

   uv pip install -e ".[dev]"

Running Tests
-------------

Execute the test suite:

.. code-block:: bash

   pytest

With coverage:

.. code-block:: bash

   pytest --cov=vibe_piper --cov-report=html

Code Style
----------

Vibe Piper uses:

* **Ruff** for code formatting
* **Ruff** for linting
* **MyPy** for type checking

Format code:

.. code-block:: bash

   uv run ruff format src tests

Run linters:

.. code-block:: bash

   ruff check src/ tests/
   mypy src/

Documentation
-------------

Building Documentation
~~~~~~~~~~~~~~~~~~~~~~

Build the documentation locally:

.. code-block:: bash

   cd docs
   make html

The built documentation will be in ``docs/build/html/index.html``.

Live Documentation Preview
~~~~~~~~~~~~~~~~~~~~~~~~~~

For development, use sphinx-autobuild to automatically rebuild the docs:

.. code-block:: bash

   cd docs
   sphinx-autobuild source build/html

Writing Documentation
~~~~~~~~~~~~~~~~~~~~~

* Use docstrings following the Google or NumPy style (Napoleon extension)
* Include type hints for all functions and methods
* Add examples in docstrings where helpful
* Update this documentation when adding new features

Submitting Changes
------------------

Workflow
~~~~~~~~

1. Create a branch for your feature or bugfix:

.. code-block:: bash

   git checkout -b feature/your-feature-name

2. Make your changes and commit them:

.. code-block:: bash

   git add .
   git commit -m "Brief description of changes"

3. Run tests and linters:

.. code-block:: bash

   pytest
   uv run ruff format src tests
   ruff check src/ tests/
   mypy src/

4. Push to your fork and create a pull request:

.. code-block:: bash

   git push origin feature/your-feature-name

Pull Request Guidelines
~~~~~~~~~~~~~~~~~~~~~~~

* Include a clear description of the changes
* Reference related issues
* Ensure all tests pass
* Update documentation as needed
* Follow the existing code style

Code Review Process
~~~~~~~~~~~~~~~~~~~

* All changes require review before merging
* Address review feedback promptly
* Keep PRs focused and reasonably sized
* Ensure CI checks pass

Project Structure
-----------------

.. code-block:: text

   vibe-piper/
   ├── src/
   │   └── vibe_piper/          # Main package
   │       ├── __init__.py
   │       └── core.py          # Core pipeline components
   ├── tests/                   # Test suite
   ├── docs/                    # Documentation
   │   └── source/
   ├── pyproject.toml           # Project configuration
   └── README.md                # Project overview

Development Guidelines
----------------------

Testing
~~~~~~~

* Write tests for all new features
* Aim for high code coverage
* Use descriptive test names
* Follow AAA pattern (Arrange, Act, Assert)

Type Hints
~~~~~~~~~~

* All public APIs must have type hints
* Enable MyPy strict mode for new modules
* Use generics for reusable components

Documentation
~~~~~~~~~~~~~

* Document all public APIs
* Use docstrings with examples
* Update user-facing docs for new features

Community Guidelines
--------------------

* Be respectful and constructive
* Welcome newcomers and help them learn
* Focus on what is best for the community
* Show empathy towards other community members

Getting Help
------------

* Open an issue for bugs or feature requests
* Start a discussion for questions
* Check existing documentation first

License
-------

By contributing, you agree that your contributions will be licensed under the MIT License.
