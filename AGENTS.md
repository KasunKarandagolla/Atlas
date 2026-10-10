# Repository instructions for Codex

These instructions apply to every Codex session in this repository.

## Before ATLAS work

1. Read [`log.md`](log.md) for the latest checkpoint, active blockers, failed approaches and unresolved decisions. Treat old entries as historical evidence, not current implementation truth.
2. For every ATLAS design, review or implementation task, read the authoritative freezes in full: `ATLAS_FINAL_IMPLEMENTATION_CLARIFICATION_AND_V1_FREEZE_COMPLETED.md`, `ATLAS_V2_FINAL_INTRADAY_INTELLIGENCE_IMPLEMENTATION_FREEZE_AMENDED_2026-09-25.md`, and `docs/v2/ATLAS_AGENT_INTELLIGENCE_EXTENSION_FREEZE_V1.md`. Read current-state and consultation material when relevant. The log never replaces frozen authority.
3. Verify the actual GitHub branch, remote tip, ancestry and source SHA before changing files. Use prior findings to avoid repeating failed experiments unless new evidence justifies them.

## During work

- The root coordinator is the sole writer of `log.md`; subagents report findings to it.
- Update the log after material discoveries, meaningful failures, successful gates and engineering decisions. For each, record what was attempted and why, the result, what remains unknown and the next step. Include source SHAs, affected files, report paths and evidence hashes where available.
- Label findings **CONFIRMED** or **HYPOTHESIS**. Separate decisions inherited from a coordinating reviewer from local observations or implementation choices. Never create architectural or policy authority from the log.
- Preserve historical failures, including superseded ones. Keep the current checkpoint short; entries are normally 100–200 words. Link to validation artifacts instead of copying their contents. Do not duplicate the validation ledger, requirement matrix or handoff.
- Never record credentials, tokens, `.env` values, private account identifiers or sensitive logs.

## Before ending a session

Update the current checkpoint; record significant successes and failures, exact unresolved blockers and the recommended next bounded action. Commit `log.md` with the session handoff/evidence, then push and verify the exact remote SHA. Preserve unrelated worktree changes and review the staged diff for secrets before committing.
