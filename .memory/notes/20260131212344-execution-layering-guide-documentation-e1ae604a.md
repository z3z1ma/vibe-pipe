---
id: 20260131212344-execution-layering-guide-documentation-e1ae604a
title: Execution Layering Guide Documentation
tags:
- documentation
- execution
- vp-7ae9
visibility: shared
status: active
created_at: "2026-01-31T21:23:44Z"
updated_at: "2026-01-31T21:23:44Z"
---

Execution layering guide created for ticket vp-7ae9. Documents three execution layers:
- Layer 1: Operator (unit-level transformations)
- Layer 2: Pipeline (sequential operator chains)
- Layer 3: AssetGraph (DAG-based production pipelines)

Content includes:
- ASCII and GraphViz diagrams showing layer relationships
- Detailed examples for each layer
- Comparison table of use cases
- Migration guide from Pipeline to AssetGraph
- OrchestrationEngine features

Referenced from:
- README.md (multiple links)
- docs/source/index.rst (Sphinx TOC)
- docs/source/api/*.rst (API references)

ADR vp-8783: Canonical Pipeline Abstractions
