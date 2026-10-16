# ATLAS algorithms and implementation authority

**Finalized 16 October 2026. One complete target system; no weaker MVP or deferred core-feature release.**

| Document | Purpose |
| --- | --- |
| [ATLAS_ALGORITHM.pdf](ATLAS_ALGORITHM.pdf) | Readable algorithm and system-design document |
| [ATLAS_SYSTEM_DESIGN.md](ATLAS_SYSTEM_DESIGN.md) | Editable source of the same algorithm; keep PDF synchronized |
| [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) | Six engineering sessions: five GPT-6.1 Sol, one GPT-6 Luna; later evidence decision |
| [REQUIREMENTS_AND_ACCEPTANCE.md](REQUIREMENTS_AND_ACCEPTANCE.md) | Complete-scope/non-degradation register, owners, tests and evidence rules |
| [CODEX_SESSION_PROMPTS.md](CODEX_SESSION_PROMPTS.md) | Copy-ready prompts with model boundaries and completion gates |
| [render_algorithm_pdf.py](render_algorithm_pdf.py) | Reproducible PDF renderer; requires ReportLab, imports no trading code |
| [MANIFEST.sha256](MANIFEST.sha256) | SHA-256 checksums of this pack's deliverables |

Code baseline: `fix/red-r9-local-complete-cycle-2026-10-14` at `96fc24a83bc90dd0b0e91eb69d55c23d85d4eefd`. Documentation branch: `docs/atlas-final-system-2026-10-16`. Begin subsequent work from its verified tip, not the older repository default branch.

The finalized algorithm supersedes conflicting earlier proposed F2 wording and recommendations to postpone core implementation. It preserves historical freezes and saved contracts for baseline replay. Necessary technical schema/model/configuration identities remain; there is no competing product-version ladder.

All core integrations are in scope, including the compulsory hosted coordinator, shared-account concurrency/recovery, complete existing strategy research surface, real selected forecaster runners, early-exit analysis and bounded Windows/Linux operation. Qualification determines financial authority; it must never substitute for missing implementation. An unimplemented runner is BLOCKED, not a completed optional feature.

Default entry permission is OBSERVE, bot allocation and paid-provider allowance are zero until explicitly configured. These documents do not activate capital or authorize exchange-changing actions. Current after-cost profitability, native lifecycle and minimum-host sustained capacity are not established. The objective is maximum validated programme value within stated risk/reliability constraints, not a guarantee of maximum profit.

Implementation order: **Sol S1 -> Sol S2 -> Sol S3 -> Sol S4 -> Luna S5 -> Sol S6**. Later **Sol E1** judges forward evidence. Keep one model per session, a verified SHA chain, complete handoffs and the repository's logging rules.

To regenerate the PDF after editing its Markdown source, run `python algorithms/render_algorithm_pdf.py` from the repository root in an environment containing ReportLab. The renderer uses DejaVu fonts when installed and standard PDF fonts otherwise. Regenerate `MANIFEST.sha256` after validation; never edit the reading edition independently of its source.
