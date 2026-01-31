---
"id": "vp-ee20"
"status": "closed"
"deps": []
"links": []
"created": "2026-01-31T19:51:43Z"
"type": "task"
"priority": 1
"assignee": "z3z1ma"
"tags":
- "sprint:vibe-piper-architectural-reboot"
- "fanout"
"external": {}
---
# Sprint prep: vibe-piper-architectural-reboot

Objective:
Create the most robust python based declararive data pipeline, integration, quality, transformation, activation library ever created. Our zen is simplicity, expressiveness, composability, and maximizing function. The UX must be intuitive. Everything must work. Use TDD. This should be the most ambitious project ever created. Turn the industry on it's head. Take your time. Weeks if you must. Take the learnings from every framework declarative or otherwise ever produced in history regarding data and improve on it. Whenever out of tickets, file more. This should be like the best parts of airflow, dagster, dlt (data load tool), dbt, and so on in one tool that is beautiful and simple. We are lacking cohesiveness. And lacking the right abstractions to provide massive value, composability, expressiveness.

Deliverable:
- Create/adjust sprint tickets directly (include the sprint tag).
- For each ticket: acceptance criteria, deps, and suggested ordering.
- Propose which tickets can run in parallel.

Sprint name: vibe-piper-architectural-reboot
Sprint tag: sprint:vibe-piper-architectural-reboot

---

## Investigation update (2026-01-31)

Audit findings:
- CORE_ABSTRACTION_CONTRACT.md is stale (references core.py + PipelineDefContext). core.py not present; code uses PipelineDefinitionContext.
- Pipeline definition APIs diverge: @asset returns Asset without operator while PipelineBuilder/PipelineDefinitionContext create executable assets; build_pipeline signature conflicts with README usage.
- Execution layer overlap: execution.py and orchestration.py both handle ordering + metrics; orchestration has duplicate execution blocks and unclear layering.
- Operator data contract ambiguous (raw data vs UpstreamData); DefaultExecutor uses implicit fallback.
- Public API surface in __init__.py mixes internal/public; no explicit stability. Docs/CLI naming mismatches (vibe-piper vs vibepiper) and operator API examples diverge from code (e.g., add_field signature).

Tickets created (sprint:vibe-piper-architectural-reboot):
- vp-8783 ADR: canonical pipeline abstractions + contract update
- vp-debe Unify pipeline definition APIs (builder/context/@asset)
- vp-64cd Consolidate ExecutionEngine + OrchestrationEngine
- vp-45d3 Finalize operator data contract (UpstreamData + context)
- vp-28e9 Curate public API surface + exports
- vp-ada1 Align docs/examples + CLI naming with canonical API

Suggested ordering:
1) vp-8783
2) vp-debe
3) vp-64cd
4) vp-45d3
5) vp-28e9
6) vp-ada1

Parallelization:
- After vp-8783: vp-debe, vp-64cd, vp-28e9 can run in parallel.
- vp-45d3 after vp-64cd.
- vp-ada1 after vp-debe + vp-28e9.
