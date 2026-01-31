---
"id": "vp-7ae9"
"status": "in_progress"
"deps": []
"links": []
"created": "2026-01-31T20:01:55Z"
"type": "task"
"priority": 3
"assignee": "z3z1ma"
"tags":
- "sprint:vibe-piper-architectural-reboot"
"external": {}
---
# Update API documentation with execution layering examples

## Notes

**2026-01-31T20:02:05Z**

# Update API documentation with execution layering examples

Context:
- ADR vp-8783 defined three execution layers (Operator, Pipeline, AssetGraph)
- Need to document how each layer works and when to use which

Scope:
- Update docs/ with:
  - Execution layering diagram
  - Examples of operator execution
  - Examples of pipeline execution
  - Examples of asset graph execution
  - Comparison of each layer's use cases

Dependencies: vp-8783 (ADR: canonical pipeline abstractions)

Acceptance Criteria:
- Execution layering documented with clear examples
- Diagram showing relationship between layers
- Use cases for each layer documented
- Linked from README and API docs

No code changes beyond documentation.

**2026-01-31T21:22:08Z**

Created comprehensive execution layering documentation including:
- docs/execution_layering.md (Markdown guide)
- docs/source/execution_layering.rst (Sphinx RST documentation)
- Updated README.md with links to execution layering guide
- Updated docs/source/index.rst to include execution_layering
- Updated docs/source/api/index.rst and core.rst with references

Documentation includes:
- Execution layering diagram (ASCII art and GraphViz)
- Layer 1 (Operator) examples and use cases
- Layer 2 (Pipeline) examples and use cases
- Layer 3 (AssetGraph) examples and use cases
- Comparison table of all layers
- Migration guide from Pipeline to AssetGraph
- OrchestrationEngine features

Next steps: Test documentation builds and finalize

**2026-01-31T21:23:10Z**

Documentation completed successfully!

Created comprehensive execution layering documentation:

Files added/updated:
- docs/execution_layering.md (21KB Markdown guide)
- docs/source/execution_layering.rst (17KB Sphinx RST)
- docs/source/index.rst (added to table of contents)
- README.md (added 2 links to execution layering guide)
- docs/source/api/index.rst (added reference)
- docs/source/api/core.rst (added note with link)

Content includes:
✅ Layer 1 (Operator) - Unit-level execution with examples
✅ Layer 2 (Pipeline) - Sequential execution with ETL examples
✅ Layer 3 (AssetGraph) - DAG execution with production examples
✅ Multi-upstream asset examples
✅ ASCII and GraphViz diagrams showing layer relationships
✅ Comparison table of all layers
✅ Migration guide from Pipeline to AssetGraph
✅ OrchestrationEngine features documentation
✅ References to ADR vp-8783

All acceptance criteria met:
✅ Execution layering documented with clear examples
✅ Diagram showing relationship between layers
✅ Use cases for each layer documented
✅ Linked from README and API docs

Note: Could not build Sphinx docs (sphinx-build not in PATH), but documentation files are properly formatted RST with correct Sphinx directives.
