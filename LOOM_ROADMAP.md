# LOOM_ROADMAP

High-level direction and priorities.

<!-- BEGIN:compound:roadmap-backlog -->
- # Tickets (5)
- - `vp-7c26` P1 in_progress - Implement Transformation Library - Fluent API
- - `vp-8690` P1 open - Implement Configuration-Driven Pipelines
- - `vp-acbc` P1 open - Implement Built-in Quality & Monitoring
- - `vp-f701` P1 in_progress - Implement Schema-First Mapping
- - `vp-0429` P2 open - Untitled
<!-- END:compound:roadmap-backlog -->

<!-- BEGIN:compound:roadmap-ai-notes -->
- 2026-01-30T05:13:39.728Z Reinforced autolearn JSON-only + read-only Plan Mode instincts; updated compound-apply-spec with a repo-relative path rule.
- 2026-01-30T05:05:54.954Z Add an instinct to audit unexpected `services/index.json` diffs by regenerating via loom and ensuring it matches intentional `services/*.md` changes; slightly increase confidence in the existing services-md deps normalization instinct.
- 2026-01-30T04:59:30.789Z Reinforced service dependency hygiene: normalize `services/*.md` deps and refresh `services/index.json`; added an autolearn guardrail instinct to output strict JSON-only specs.
- 2026-01-30T04:44:58.900Z Add guidance for separating intentional uv.lock changes from generated src/*.egg-info diffs; reinforce egg-info-as-noise heuristics.
- 2026-01-30T04:32:46.473Z No durable learnings detected; only compound state bookkeeping changed.
- 2026-01-30T04:28:27.666Z Add an instinct for strict Plan Mode (read-only) compliance; clarify CompoundSpec output rules for autolearn prompts (single JSON, no fences/commentary).
- 2026-01-30T04:27:46.142Z No new learnings: no git changes or new recurring patterns detected in this session.
- 2026-01-30T04:26:13.382Z No memory updates proposed (idle session; git diffstat empty).
- 2026-01-30T04:19:03.945Z No learnings proposed: no code/tool activity detected in this session; ran docs sync only.
- 2026-01-30T04:04:24.690Z No memory updates proposed (no new signals in this session).
- 2026-01-30T03:43:45.661Z No memory updates proposed (no recent activity captured).
- 2026-01-30T03:21:22.411Z No memory updates proposed; only .opencode/compound/state.json changed.
- 2026-01-30T03:10:18.003Z Strengthened instincts for CompoundSpec v2 full-body skill updates and uv editable-install egg-info diff hygiene; synced derived docs/indexes.
- 2026-01-30T03:03:57.303Z Add instinct to treat src/*.egg-info diffs as generated artifacts from uv editable installs; avoid committing unless explicitly intended.
- 2026-01-30T02:50:09.689Z No new procedural learnings extracted; synced docs/indexes after adding ticket vp-db50 and updating LOOM roadmap/changelog.
- 2026-01-30T02:36:38.057Z No durable learnings extracted from this session; only internal compound state changed.
- 2026-01-30T02:25:31.464Z Add instinct + skill for resolving merge conflict markers in compound-managed LOOM_CHANGELOG.md / LOOM_ROADMAP.md while preserving BEGIN/END fences; request docs sync after conflict resolution.
- 2026-01-30T02:22:56.207Z No memory updates proposed; only .opencode/compound/state.json changed.
- 2026-01-30T02:01:31.634Z No new learnings; only compound internal state changed.
- 2026-01-30T01:27:06.799Z No memory updates proposed (idle session; git diffstat empty).
- 2026-01-30T01:25:09.432Z Hardened compounding guidance: compound-workflows now specifies CompoundSpec v2 and correct compound_apply() usage; added instinct to treat src/*.egg-info diffs from uv editable installs as generated noise and route decisions through python-egg-info-hygiene.
- 2026-01-30T01:22:04.760Z Add instinct to treat src/*.egg-info diffs as generated metadata and avoid committing them unless packaging changes are intentional.
- 2026-01-30T01:17:14.715Z No new learnings from this idle cycle; only compound state changed.
- 2026-01-30T01:14:06.447Z No memory updates proposed; no repo activity detected in this session.
- 2026-01-30T01:09:12.773Z Add instinct to treat src/vibe_piper.egg-info diffs as generated noise; slightly increase confidence in full-body skills.update requirement.
- 2026-01-30T00:47:21.209Z Add memory about treating `src/*.egg-info/**` diffs as generated metadata and avoiding accidental commits; introduce a small hygiene skill for handling egg-info churn in uv-driven Python repos.
- 2026-01-30T00:15:14.423Z Updated memory to align compound-apply-spec with CompoundSpec v2 and reinforced data-cleaning pandas/text + dtype safety guidance; nudged confidence up on recurring pandas/nullable/int-cast instincts.
- 2026-01-29T23:54:29.524Z Implemented drift detection features: baseline storage, history tracking, configurable thresholds, and validation check wrappers for @validate decorator integration
- 2026-01-29T23:52:32.816Z Implemented comprehensive Data Cleaning Utilities (vp-e62a) with @clean_data() decorator, 20+ functions covering deduplication, null handling (6 strategies), outlier detection (4 methods), outlier treatment (6 actions), type normalization, standardization, and text cleaning. Created CleaningConfig and CleaningReport classes. Wrote 73 tests achieving 77% pass rate, 73% module coverage. Identified critical pandas 2.x string accessor pattern issues and test fixture nullable field requirements.
- 2026-01-29T23:49:43.630Z Merge worker session: Merged vp-66b4 (validation history). Resolved SOURCES.txt conflict by resetting from index and not staging auto-generated egg-info files.
<!-- END:compound:roadmap-ai-notes -->
