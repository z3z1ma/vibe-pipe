# LOOM_ROADMAP

High-level direction and priorities.

<!-- BEGIN:compound:roadmap-backlog -->
- # Tickets (8)
- - `vp-1cba` P1 open - Debug TRANSFORM asset execution failure
- - `vp-debe` P1 in_progress - Unify pipeline definition APIs (builder/context/@asset)
- - `vp-28e9` P2 in_progress - Untitled
- - `vp-ada1` P2 open - Align docs/examples + CLI naming with canonical API
- - `vp-e990` P2 in_progress - Update README with canonical pipeline model guidance
- - `vp-6d4f` P3 in_progress - Create migration guide for Pipeline -> AssetGraph
- - `vp-7ae9` P3 in_progress - Update API documentation with execution layering examples
- - `vp-f498` P4 in_progress - Add AssetGraph.from_pipeline() adapter method
<!-- END:compound:roadmap-backlog -->

<!-- BEGIN:compound:roadmap-ai-notes -->
- 2026-01-31T19:34:41.806Z No learning - git summary showed only build artifact changes (uv.lock, egg-info)
- 2026-01-31T18:56:44.083Z Implemented snapshot testing framework with 17 tests, 3 example snapshot tests, and --update-snapshots pytest flag support
- 2026-01-31T18:44:00.918Z Refine autolearn guidance to better handle Plan Mode and low-signal ticket/doc-heavy diffs; strengthen related workflow instincts.
- 2026-01-31T18:36:35.799Z Refined autolearn guidance for ticket/process-heavy diffs and reinforced read-only/Plan Mode and ticket-churn heuristics.
- 2026-01-31T16:46:48.605Z Refine autolearn procedure: when diffstat is empty, emit a justified no-op spec instead of inventing learnings.
- 2026-01-31T16:25:46.611Z Strengthen heuristics for treating ticket/roadmap-only diffs as low-signal and for documenting investigation/scoping work directly in tickets.
- 2026-01-31T16:02:57.529Z Slightly strengthened the heuristic that src/*.egg-info diffs (e.g., SOURCES.txt) are usually generated noise from uv/editable installs and should not drive product inferences.
- 2026-01-31T06:29:30.337Z Strengthen heuristics that large memory-store diffs and ticket/doc churn are low-signal without accompanying product code changes.
- 2026-01-31T05:59:40.275Z Reinforce treating src/*.egg-info churn (including mass deletions) as generated-artifact cleanup and non-signal for product behavior.
- 2026-01-31T05:57:35.344Z Reinforce strict read-only behavior in Plan Mode and treat ticket/docs/memory-store churn as low-signal evidence.
- 2026-01-31T05:41:35.241Z Strengthened Phase 3 investigation and scoping instincts with evidence from vp-0429 workflow (investigation, documentation, manager communication, idle waiting)
- 2026-01-31T05:34:54.917Z Reinforced heuristics for ticket/docs-heavy diffs and Plan Mode read-only autolearn constraints.
- 2026-01-31T05:30:34.740Z Strengthen low-signal heuristic for ticket-only diffs to keep autolearn proposals minimal and avoid inventing product behavior.
- 2026-01-31T04:50:38.761Z Reinforce low-signal heuristics for memory/docs/ticket churn and mirrored skill edits; strengthen read-only/Plan Mode guardrails.
- 2026-01-31T04:47:32.470Z Strengthened Phase 3 investigation instincts with evidence from vp-0429, learned ticket investigation workflow pattern (investigate-doc-commit-notify-wait)
- 2026-01-31T04:45:50.725Z Strengthened heuristics to treat memory-store rewrites, mirrored .claude skill edits, and ticket/docs-heavy diffs as low-signal for product learning, and to obey Plan Mode read-only constraints.
- 2026-01-31T04:35:42.478Z Learned Phase 3 ticket investigation pattern from vp-0429 scope clarification: check ROADMAP context, verify implementation gaps, provide evidence-based recommendation (implement/defer/close), document findings in INVESTIGATION_NOTES.md
- 2026-01-31T04:32:45.057Z Strengthen heuristics to treat memory-store churn and mirrored skill diffs as low-signal; add guardrail for ticket/docs-heavy diffs.
- 2026-01-31T04:18:14.378Z Reinforce autolearn hygiene: treat memory/skill mirror churn as low-signal, keep outputs JSON-only, and re-emit full managed bodies for skill updates.
- 2026-01-31T04:16:53.667Z Treat mass src/*.egg-info deletions as generated-metadata cleanup and avoid inferring broader learnings from them during autolearn.
- 2026-01-31T04:03:29.821Z Add an instinct to treat duplicated .claude/.opencode skill diffs as mirror noise, and reinforce low-signal memory-store churn plus full-body skills.update and plan-mode read-only guardrails.
- 2026-01-31T03:59:11.609Z Reinforce autolearn heuristics to treat large memory-store churn as low-signal cleanup and prefer minimal, .opencode-scoped spec updates under read-only constraints.
- 2026-01-31T03:38:38.975Z Refine autolearn behavior to treat memory-store churn as low-signal and reinforce strict read-only, memory-only proposals.
- 2026-01-31T03:23:31.134Z Align compounding workflow/docs with CompoundSpec v2 and reinforce key heuristics for full-body skill updates and plan-mode read-only behavior.
- 2026-01-30T19:07:19.036Z Reinforced heuristics: treat derived index diffs as low-signal unless source files changed, and prefer structured/quoted workflows for large Loom ticket bodies.
- 2026-01-30T18:55:05.980Z Strengthened Loom ticket long-body/quoting instincts; added heuristic to treat services/index.json-only diffs as derived low-signal during autolearn.
- 2026-01-30T18:49:22.643Z Strengthened Loom ticket authoring heuristics and added a guardrail to treat services/index.json-only diffs as derived, low-signal changes.
- 2026-01-30T18:02:40.795Z Refine autolearn to treat derived index diffs (services/index.json) as low-signal and avoid inferring dependency learnings from them.
- 2026-01-30T17:58:39.370Z Added an instinct for JSON-only autolearn responses; strengthened Loom ticket long-body and safe-quoting instincts based on recent large ticket updates.
- 2026-01-30T17:50:32.221Z Added an instinct to treat services/index.json as generated; strengthened Loom ticket long-body and quoting heuristics.
<!-- END:compound:roadmap-ai-notes -->
