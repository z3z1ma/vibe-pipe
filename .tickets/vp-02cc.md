---
"id": "vp-02cc"
"status": "open"
"deps": []
"links": []
"created": "2026-01-28T01:27:17Z"
"type": "feature"
"priority": 1
"assignee": "z3z1ma"
"tags":
- "phase2"
- "documentation"
- "tooling"
"external": {}
---
# Documentation Generator

Build documentation generator for data dictionaries, asset catalog, and lineage visualization.

## Tasks
1. Create schema documentation generator
2. Create asset catalog generator
3. Create lineage visualization (Mermaid/SVG)
4. Generate HTML documentation site
5. Add search functionality
6. Integrate with CLI (vibepiper docs command)
7. Add template system for customization
8. Auto-generate docs from asset docstrings

## Example Usage
```bash
# Generate documentation
vibepiper docs my-pipeline/ --output=docs/

# Serve docs locally
vibepiper docs my-pipeline/ --serve --port=8000
```

## Generated Documentation Includes:
- Asset catalog with descriptions
- Schema definitions for each asset
- Lineage DAG (Mermaid diagram)
- Data quality metrics
- Execution history
- Search functionality

## Dependencies
- vp-104 (Asset versioning and lineage)

## Technical Notes
- Use Jinja2 for templates
- Generate static HTML site
- Use Mermaid.js for DAG visualization
- Support custom branding
- Generate metadata for search

## Acceptance Criteria

Schema docs generated from code
Asset catalog with descriptions and metadata
Lineage DAG (Mermaid/SVG) visualization
HTML site with navigation
Search functionality working
CLI integration (vibepiper docs)
Template system for customization
Auto-generation from docstrings
Example documentation generated
Tests for generators
Test coverage > 80%
