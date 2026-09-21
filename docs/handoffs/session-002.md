# Session 002 Handoff — Safe Runtime (Phase 1, no orders)

Branch: `impl/session-002-safe-runtime`
Date (UTC): 2026-09-21
Freeze: `ATLAS_FINAL_IMPLEMENTATION_CLARIFICATION_AND_V1_FREEZE_COMPLETED.md` (authoritative)

## Branch inheritance

Session 002 was created from corrected Session 001 tip:

- `bac8b16e767e7a253cc41c4e73f16ac1c6658319`
  `fix: session 001 review repairs (dispatch UNKNOWN, versions, transitions, atomicity, placeholders, decimals, composite keys, pragmas, py312)`

Verified via `git fetch origin --prune` + `git pull --ff-only` before branching,
then `git push -u origin impl/session-002-safe-runtime` early.

## Session 001 corrections

All 14 review defects fixed on `impl/session-001-foundation` (commit `bac8b16`)
and pushed before Session 002 branched:

1. Dispatch marker atomically yields UNKNOWN (`mark_send_started` single tx:
   eligibility check + timestamp + UNSENT→UNKNOWN; second marker fails;
   definite/reconciled outcomes cannot regress). Old UNSENT-after-mark
   expectation replaced; restart tests: UNSENT w/o marker, UNKNOWN after
   marker + reopen with no second op.
2. Monotonic intent `state_version` (new `transitions.py` + `intents.state_version`,
   schema v2, v1→v2 migration, initial 0 deterministic, +1 per mutation,
   stale/​decrease rejected, `persist_command` binds expected version, reopen
   retains version).
3. Frozen §1.5 lifecycle validator (pure graph; CLOSED→SUBMITTING,
   OPEN_PROTECTED→INTENT_PERSISTED, SUBMIT_UNKNOWN→PLAN_APPROVED and all other
   illegal/regressive transitions rejected; protection/reconciliation stay
   independent dimensions).
4. Command-outcome safety (UNSENT→UNKNOWN/​DEFINITE_REJECT;
   UNKNOWN→ACCEPT/REJECT/RECONCILED; terminals→RECONCILED only; no silent
   regress to UNSENT; no fill inferred from DEFINITE_ACCEPT; timeout never
   auto-converts).
5. Atomic approval+intent+reservation (`consume_approval_with_intent_reservation`;
   verifies exists/unused/unexpired/matching, consumes + inserts in one tx,
   unique client-ID enforced, full rollback proven; 8-thread single-winner test).
6. Atomic command+reservation preparation (`prepare_dispatch`: loads intent +
   version, verifies reservation version, persists exact payload+SHA256, advances
   INTENT_PERSISTED→SUBMITTING with version bump in one tx before transport;
   reservation never released on UNKNOWN/CANCEL_PENDING/timeout).
7. Capability placeholder gating (REQUIRED/REQUIRED_AT_INSTALL block assisted;
   SHA256 shape enforced for artifact/lock hashes; fixture keeps placeholders
   with assisted=false; SUPPORTED+placeholder proven blocked).
8. Strict manifest booleans (non-bool `assisted_enabled` incl. "false"/"true"/"0"
   rejected).
9. Decimal normalization (TradePlan/RiskPolicy/Reservation/ProtectionObservation/
   EconomicEvent coerce str/int → Decimal via `object.__setattr__`; NaN/Infinity
   rejected; canonical serialization after str inputs tested).
10. Economic-event composite identity `(account, venue_transaction_id)` (schema v2
    rebuild + migration; same-tx/different-account allowed, same/same rejected).
11. Fail-closed pragmas (WAL + FULL + FK read back and verified; `:memory:` fails
    with PersistenceError; file journals verified active).
12. HealthSnapshot bool strictness (non-bool truthy values rejected).
13. Python/Nautilus pin correction (repo now requires Python ≥3.12; rc5 wheel
    downloaded + import-verified under 3.12; see Nautilus status below).
14. Security/docs (`.gitignore` += pem/key/p12/pfx/SSH patterns; README/handoff
    corrected — no false NONE-deviation or stale install claims).

## Phase 1 implementation

New safe runtime (no order-submission interface exists by design):

- `src/atlas/runtime/prerequisites.py`: testnet identity/account contract
  (environment/venue/product/position/margin/identity/instrument-metadata;
  mismatch blocks new risk; no auto-mutation; private verification TEST GATE
  with fixture-tested validator).
- `src/atlas/runtime/connectivity.py`: public health
  (DISCONNECTED/CONNECTING/SYNCHRONIZED/STALE/DEGRADED/FAILED with
  source/receipt/heartbeat/staleness; stale-dated receipt never fresh) +
  private config/verification boundary (UNVERIFIED/TEST_GATE without credentials;
  fakes only in tests).
- `src/atlas/runtime/recovery.py`: immutable recovery certificate
  (run/writer/schema/unresolved/unknown/health/uncertainty/timestamps/evidence/
  decision READY/REMAIN_RECOVERING/RECOVERY_REQUIRED); READY with unresolved
  uncertainty and no venue observations is unconstructable; deterministic
  `run_recovery` classifier.
- `src/atlas/runtime/coordinator.py`: deterministic BOOT → writer lock →
  journal → RECOVERING → restore → classify (no marker UNSENT / marker w/o
  terminal UNKNOWN) → block new risk on uncertainty → prerequisites → READY
  only on positive evidence. Absent venue verification keeps new risk disabled.
- `src/atlas/runtime/nautilus_boundary.py`: pin `2.0.0rc5` import/version check,
  testnet-only node config, callback protocols; mocked in tests; no orders,
  no fake acks, no SUPPORTED marking, no CCXT/OMS.
- `src/atlas/runtime/status.py`: sanitized atomic ops snapshots
  (state/epoch/journal/reconciliation/counts/cap-hash/qualified/assisted; no
  keys/secrets/signed-requests/account IDs; temp+fsync+rename; partial writes
  never fresh).
- `src/atlas/runtime/__main__.py`: `python -m atlas.runtime` (testnet default,
  lock+journal+recovery+status, refuses new risk, clean Ctrl-C release,
  fail-closed crashes).

## Tests

Exact commands (repo root; interpreter is Python 3.12 — repo requires ≥3.12):

- `PYTHONPATH=src pytest` → **113 passed, 1 skipped** (opt-in integration)
- `PYTHONPATH=src python3.12 -m pytest` → **113 passed, 1 skipped**
- `python3 -m ruff check src tests` → **All checks passed**
- `python3 -m mypy src tests` → **Success: no issues found in 42 source files**
- `PYTHONPATH=src python3.12 -m atlas.runtime --journal /tmp/e2e-j.db --lock /tmp/e2e-w.lock --status /tmp/e2e-status.json` → `state=RECOVERING new_risk_allowed=False ... NO ORDERS SUBMITTED`

Matrices covered: BOOT→RECOVERING; UNSENT w/o marker; marker→UNKNOWN + reopen;
UNKNOWN blocks READY; unresolved intent blocks entry; writer-loss/duplicate
non-READY; client-ID reuse; 6 identity mismatches + missing metadata; stale
public/private, conflicted reconciliation, protection uncertainty; READY only on
all-positive; status sanitized/atomic/no-order-interface; pinned-version or
absent + UNVERIFIED + assisted-false preserved. Plus all Session 001 repair
tests (transitions/outcomes/placeholders/booleans/decimals/composite/pragmas/
atomicity/migration/concurrency).

## Nautilus status

- Import/version boundary: IMPLEMENTED
- Boundary import check (`2.0.0rc5` under 3.12): TESTED (offline, mocked + real
  import where installed to /tmp/nt312)
- Wheel evidence: `nautilus_trader-2.0.0rc5-cp312-cp312-manylinux_2_34_x86_64.whl`,
  SHA256 `eab45fafd2312deda1236554c49a9798bfc76bc8465af864878e2f70189ebebe`,
  Python 3.12.13, ABI `cpython-312-x86_64-linux-gnu` —
  `docs/capability/nautilus-pin-status.json`
- All six Bybit wire behaviors: UNVERIFIED (installation qualifies nothing)
- Credentialed/private verification, live-node wiring beyond config: TEST GATE
- Nothing: BLOCKED BY ENVIRONMENT (Python 3.12 present; rc5 cp312 wheel available)

## Network evidence

- Offline/unit: all 113 tests deterministic, no network. IMPLEMENTED + TESTED.
- Public testnet: no live calls made; opt-in test present and skipped by default
  (`test_public_testnet_connectivity_opt_in`). UNVERIFIED.
- Authenticated/private: no credentials configured or used; private stays
  UNVERIFIED / TEST GATE by construction. No private evidence claimed.

## Remaining Phase 1 work

- Generate a committed dependency lock for the 3.12 environment (currently
  wheel + METADATA hashes recorded, no `requirements.lock`).
- Run the opt-in public-testnet health check from a networked host and record
  staleness evidence (still read-only, no orders).
- Credentialed private verification against testnet (TEST GATE) with redacted
  evidence logging — needs account + review approval, not in this session.
- Nautilus live-node event wiring beyond config/callbacks (belongs with Phase 2
  qualification).
- Ops status freshness TTL + multi-snapshot rotation (single atomic snapshot now).

## Next phase

Session 003 is Phase 2 protection/recovery qualification and the §1.8 fault
fixtures (submit/partial-fill/stop/close/reconnect/crash/cash), only after
review of this branch. No test orders, stop races, native-stop repair, or
reduce-only qualification were started here.
