#!/usr/bin/env python
"""Generate documentation for the sample pipeline."""

import sys
from pathlib import Path

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

# Import the pipeline assets
from examples import sample_pipeline
from vibe_piper.docs.site import HTMLSiteGenerator
from vibe_piper.types import Asset

# Collect all assets from the module
assets = []
for attr_name in dir(sample_pipeline):
    attr = getattr(sample_pipeline, attr_name)
    if isinstance(attr, Asset):
        assets.append(attr)

print(f"Found {len(assets)} assets:")
for asset in assets:
    print(f"  - {asset.name} ({asset.asset_type.name})")

# Generate documentation
output_dir = Path(__file__).parent / "docs"
generator = HTMLSiteGenerator(output_dir=output_dir)

generator.generate(
    assets=assets,
    pipeline_name="Sample E-commerce Pipeline",
    description="Example data pipeline for user and order analytics",
)

print("\n✓ Documentation generated successfully!")
print(f"Open {output_dir / 'index.html'} in your browser to view")
