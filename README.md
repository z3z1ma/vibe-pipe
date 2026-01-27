# Vibe Piper

A robust Python-based declarative data pipeline, integration, quality, transformation, and activation library designed for simplicity, expressiveness, and composability.

> **Status:** Early Development (Phase 0: Foundation)
>
> This project is in active development. APIs may change as we evolve the architecture.

## Features

- **Declarative Pipeline Definition**: Build data pipelines using a clean, declarative syntax
- **Composable Stages**: Chain transformations together in a flexible, reusable way
- **Type Safety**: Full type hint support for better IDE integration and fewer runtime errors
- **Simple and Expressive**: Intuitive API that makes complex data transformations easy

## Quick Start

```python
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
```

## Installation

```bash
pip install vibe-piper
```

## Documentation

Full documentation is available at: [https://your-org.github.io/vibe-piper](https://your-org.github.io/vibe-piper)

### Building Documentation Locally

```bash
# Install dependencies
uv sync --dev

# Build documentation
cd docs
uv run sphinx-build -b html source build/html

# View documentation
open build/html/index.html  # On macOS
# or
xdg-open build/html/index.html  # On Linux
```

For development with live reload:

```bash
cd docs
uv run sphinx-autobuild source build/html
```

## Development

```bash
# Clone repository
git clone https://github.com/your-org/vibe-piper.git
cd vibe-piper

# Install development dependencies
uv sync --dev

# Run tests
uv run pytest

# Format code
uv run black src/ tests/

# Type check
uv run mypy src/
```

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](docs/source/contributing.rst) for guidelines.

## License

MIT License - see LICENSE file for details

## Project Status

This project is currently in **Phase 0: Foundation**. We are establishing the core architecture and infrastructure. Features are being added rapidly as we build toward a stable release.

See our documentation for the roadmap and development plans.
