# Session 003 Handoff

## Branch Inheritance

**Corrected Session 002 SHA:** `64ff179`

## Session 002 Repairs Summary

| Fix | Description | Status |
|-----|-------------|--------|
| A1 | Capability/assisted flags gate new risk in `coordinator.boot()` | ✅ |
| A2 | Any unresolved command (UNSENT/UNKNOWN/DEFINITE_ACCEPT) blocks READY | ✅ |
| A3 | READY requires positive venue reconciliation evidence (`venue_evidence_refs`) | ✅ |
| A4 | `PrivateVerification.VERIFIED` requires real evidence (timestamp, env, venue binding) | ✅ |
| A5 | Placeholder account identity (`CONFIGURED`, `REQUIRED`, etc.) rejected | ✅ |
| A6 | Frozen Nautilus/V1 capability identity enforced (distribution, version, commit, product, position_mode, symbols) | ✅ |
| A7 | `LiveNodeConfig` rejects `testnet=False` | ✅ |
| A8 | Clock skew/future timestamps fail closed in `PublicVenueHealth.is_fresh()` | ✅ |
| A9 | `SafeRuntime` long-lived process skeleton (start/tick/status/shutdown/run_forever) | ✅ |
| A10 | `RECOVERY_REQUIRED` maps to `ENTRY_HALTED` not `EMERGENCY_EXIT` | ✅ |
| A11 | `RuntimeStatus` includes freshness metadata (schema_version, generated_at_ns, runtime_instance_id, writer_id) | ✅ |
| A12 | Reproducible Python 3.12 dependency lock with SHA256 | ✅ |
| A13 | `LiveNodeConfig` enforces frozen V1 contract | ✅ |

## Phase 2 Offline Implementation (Session 003)

### Modules Implemented

| Module | Purpose | Freeze Ref |
|--------|---------|------------|
| `src/atlas/runtime/protection_port.py` | C1: Narrow `BybitProtectionPort` (read_protection, ensure_full_stop, read_economic_events) | §1.2, §1.3, §9.7 |
| `src/atlas/runtime/protection_evidence.py` | C2: Upgraded protection evidence model (full-position semantics, raw evidence IDs, freshness) | §1.3, §1.4 |
| `src/atlas/runtime/fill_dedup.py` | C3: Durable execution/fill dedup evidence (exchange execution ID, monotonic qty, append-only corrections) | §1.4 |
| `src/atlas/runtime/reconciliation_evidence.py` | C4: REST/reconciliation query evidence types (query completeness, retention coverage, evidence hash) | §1.4, §1.6 |
| `src/atlas/runtime/flat_certificate.py` | C5: Flat reconciliation certificate (zero position, terminal commands, no residual orders, dedup coverage) | §1.6, §9.8 |
| `tests/support/fault_injection.py` | C6: Offline fault-injection harness (all 17 §1.8 scenarios as deterministic fixtures) | §1.8 |
| `src/atlas/runtime/capability_ledger.py` | C7: Capability evidence ledger (UNVERIFIED → TESTED_OFFLINE → TEST_GATE_TESTNET → PASSED_TESTNET) | §1.8 |
| `src/atlas/runtime/wire_contract.py` | C8: Logical wire contract builder (entry/exit contracts from immutable plan data) | §1.3 |
| `src/atlas/runtime/protection_deadline.py` | C9: Protection deadline state machine (2s unconfirmed → cancel leaves → repair → flatten) | §1.3 |
| `src/atlas/runtime/no_reversal.py` | C10: No-reversal invariant (q·Δq≤0, |Δq|≤|q|, sign preservation) | §1.7 |

## Qualification Matrix Status (Freeze §1.8)

| # | Test | Status |
|---|------|--------|
| 1 | Identity and mode | IMPLEMENTED |
| 2 | Wire contract | IMPLEMENTED |
| 3 | Crash before send | TESTED_OFFLINE |
| 4 | Lost submit response | TESTED_OFFLINE |
| 5 | Definite reject | TESTED_OFFLINE |
| 6 | Partial fills | TESTED_OFFLINE |
| 7 | Kill after full fill | TESTED_OFFLINE |
| 8 | Duplicates/reordering | TESTED_OFFLINE |
| 9 | Native-stop repair | TESTED_OFFLINE |
| 10 | Stop/close race | TESTED_OFFLINE |
| 11 | Late-entry race | TESTED_OFFLINE |
| 12 | Private disconnect | TESTED_OFFLINE |
| 13 | Incomplete REST | TESTED_OFFLINE |
| 14 | Residual orders | TESTED_OFFLINE |
| 15 | Amendment race | TESTED_OFFLINE |
| 16 | Exit failure | TESTED_OFFLINE |
| 17 | Storage/clock failure | TESTED_OFFLINE |
| 18 | Cash and external events | TESTED_OFFLINE |
| 19 | Approval replay | TESTED_OFFLINE |
| 20 | Restore/fencing | TESTED_OFFLINE |

**Note:** All testnet-online tests remain TEST GATE. All offline fixtures are TESTED_OFFLINE only.

## Capability Status

| Capability | Status |
|------------|--------|
| entry_ioc_with_attached_full_mark_market_stop | UNVERIFIED |
| native_stop_visible_and_resizes_on_partial_fill | UNVERIFIED |
| reduce_only_wire_and_matching_enforcement | UNVERIFIED |
| ambiguous_submit_not_treated_as_definite_rejection | UNVERIFIED |
| external_native_stop_fill_reconciliation | UNVERIFIED |
| native_position_stop_read_and_repair_port | UNVERIFIED |
| **assisted_enabled** | **false** |

## Test Results

```bash
PYTHONPATH=src python3.12 -m pytest tests/ -v
# 113 passed, 1 skipped

python3.12 -m ruff check src tests
# All checks passed

python3.12 -m mypy src tests
# Success: no issues found in 43 source files
```

## Network Evidence

- **No orders sent** to any venue
- **No authenticated mutation** performed
- **No reduce-only matching** claim
- **No stop race** qualification
- **No assisted capability** enabled
- All tests are deterministic offline fixtures (TESTED_OFFLINE)

## Remaining Phase 2 Work

### Code/Harness Remaining
- [ ] Integration tests for fault injection scenarios with real journal persistence
- [ ] Protection port TEST GATE binding to Nautilus/Bybit REST (when credentials available)
- [ ] Reconciliation query evidence integration with actual REST pagination

### Public Read-Only Testing
- [ ] Public market data feed validation (bar integrity, freshness)
- [ ] Instrument metadata snapshot (tick/lot, risk tier)

### Authenticated Read Testing (Requires Credentials)
- [ ] Private order/position/wallet stream validation
- [ ] Native protection observation (read_protection)
- [ ] Transaction log pagination and deduplication

### Bybit Testnet Mutation/Order Qualification (Requires Explicit Authorization)
- [ ] IOC entry with attached full-position mark-market stop
- [ ] Native stop visible and resizes on partial fill
- [ ] Reduce-only wire and matching enforcement
- [ ] Ambiguous submit not treated as definite rejection
- [ ] External native stop fill reconciliation
- [ ] Native position stop read and repair port

**Authorization Required:** All six capabilities remain UNVERIFIED. Testnet mutation qualification requires explicit user approval and live RiskPolicy.

## Key Artifacts

- **Dependency Lock SHA256:** `49036ec357c04ea6d101a6a8cc4aa9f40b6038e229cb325ec3cb7db7b35f4085`
- **Nautilus Pin:** `nautilus_trader==2.0.0rc5` (source commit `1b0a49d2792a9432a3aca3fcb617ce7a630d905e`)
- **Python/Platform ABI:** `cpython-312-x86_64-linux-gnu`
- **Capability Manifest:** `docs/capability/bybit-v1.yaml` (updated with real lock SHA, pending artifact SHA)

## Exact Next Recommended Task

**Create Session 004 branch for Phase 3 (Causal Data Foundation):**
- Immutable BTC/ETH forward archives (bars, 1m last/mark/index, quotes/depth, trades, funding/OI)
- ACTUAL_SYSTEM and RECONSTRUCTED_MARKET replay views
- Prefix-invariance tests
- Requires: Running public testnet WebSocket/REST collection (no credentials needed)