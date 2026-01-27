# Vibe Piper

A robust Python-based declarative data pipeline, integration, quality, transformation, and activation library.

## Vision

Vibe Piper aims to be the most comprehensive and elegant data framework ever created. Our philosophy is built on four pillars:

- **Simplicity**: Intuitive APIs that make complex data operations feel natural
- **Expressiveness**: Powerful declarative syntax that makes intent clear
- **Composability**: Build complex pipelines from simple, reusable components
- **Function**: Everything must work—robustness is not optional

## Installation

```bash
poetry install
```

## Development

### Setup

```bash
# Install dependencies
poetry install

# Install pre-commit hooks
poetry run pre-commit install
```

### Running Tests

```bash
poetry run pytest
```

### Code Quality

```bash
# Format code
poetry run black src tests

# Lint
poetry run ruff check src tests

# Type check
poetry run mypy src
```

## Project Status

This project is in early development (Phase 0). We are building the foundation for what will become the industry's most ambitious data framework.

## License

MIT License - see LICENSE file for details.
