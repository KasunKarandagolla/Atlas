# V1 Exit Audit — ATLAS V2 Phase 0

**Audit date:** 2026-09-24
**Scope:** V1 Phase 0–6 exit and the transition baseline only. No V2 Phase 1 or
Phase 2 runtime work is included.

## Authority and scope

The audit follows these sources in order:

1. `ATLAS_FINAL_IMPLEMENTATION_CLARIFICATION_AND_V1_FREEZE_COMPLETED.md`
   (tracked V1 freeze; SHA-256
   `c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c`).
2. The supplied `ATLAS_V2_FINAL_INTRADAY_INTELLIGENCE_IMPLEMENTATION_FREEZE.md`
   (read from the invoking checkout; SHA-256
   `bae3e1a9e48aec64d1292e5bc791c2e87949ed33f4124b1f9807b589cc07a484`). It
   was an untracked supplied input at the starting checkpoint and is therefore
   identified by content hash here rather than represented as an approved
   tracked V1 file.
3. The repository state at the required Session-009 checkpoint and
   `docs/handoffs/session-009.md`.

The authoritative V1 checkpoint is
`impl/session-009-v1-phase0-6-stabilization` at
`146bae0a2f10e2f794cbed3441123071c55a2baf` (`docs: record session 009 v1
acceptance handoff`). `git fetch --all --prune` was run before work. The remote
Session-009 tip matched that SHA. The fetched branch inventory ended at
Session-009: Sessions 001–009 are present; no approved Session-010 or later V2
checkpoint was present. The local audit branch was created directly from the
required SHA.

The GitHub default branch points to the older
`impl/session-001-foundation` at
`bac8b16e767e7a253cc41c4e73f16ac1c6658319`. It is stale and non-authoritative.
Use the approved branch and exact SHA for implementation checkpoints.

## Environment and dependency evidence

| Item | Observed evidence |
|---|---|
| Python | CPython 3.12.13; ABI `cpython-312-x86_64-linux-gnu`; Clang 22.1.3 build |
| Platform | Linux Lite 6.6 based on Ubuntu 22.04.5; Linux `5.15.0-177-generic`; x86_64; glibc 2.35 |
| Environment | Isolated `.venv`; lock synchronized with `uv 0.11.8` using `uv venv --python /home/kasun/.local/bin/python3.12 .venv` and `uv pip sync --python .venv/bin/python requirements-lock.txt` |
| Dependency lock | `requirements-lock.txt` SHA-256 `0d7b5cc6129aab127f07a1b5bce4b7794eec5c48db0f99451eba675031ca2b39` |
| NautilusTrader expected | `nautilus_trader==2.0.0rc5`, source commit `1b0a49d2792a9432a3aca3fcb617ce7a630d905e`; lock/reference wheel `nautilus_trader-2.0.0rc5-cp312-cp312-manylinux_2_34_x86_64.whl`, SHA-256 `eab45fafd2312deda1236554c49a9798bfc76bc8465af864878e2f70189ebebe` |
| Nautilus observed | Installed distribution reports `2.0.0rc5`; installed `_libnautilus` extension SHA-256 `a53dd5a24fe77f4c66169af84ee9c7e292018010922e3dce9a8b6c9f7f3b7021` |
| Wheel bytes | `BLOCKED BY ENVIRONMENT`: uv did not retain the downloaded wheel, so the expected wheel digest was not independently rehashed from the wheel file. It is recorded separately from the installed extension digest. |

The initial system `python` was 3.10.12 and could not run this Python 3.12
repository (`StrEnum` is unavailable there). The locked Python 3.12 environment
was then created and used for the canonical run; the mismatch did not indicate
a repository defect.

## Pristine Session-009 reproduction

Before tracked edits, the clean Session-010 worktree was at the exact
Session-009 SHA. With the locked Python 3.12 environment:

```text
PYTHONPATH=src:. .venv/bin/python -m pytest -ra
413 passed, 1 skipped in 232.73s

PYTHONPATH=src:. .venv/bin/python -m ruff check
All checks passed!

PYTHONPATH=src:. .venv/bin/python -m mypy src tests
Success: no issues found in 174 source files

PYTHONPATH=src:. .venv/bin/python -m compileall -q src tests
PASS

git diff --check
clean
```

The one skip is `tests/runtime/test_safe_runtime.py:398`, the opt-in public
testnet check. It is skipped by default; this audit used no credentials and did
not need network access. These are reproduced results, not inherited claims.

The complete test tree contains 77 `test*.py` modules. The required acceptance,
capital-control, safe-runtime, Phase-2 repair, assisted dispatch/protection,
flat-certificate, temporal reconciliation, causal data/archive, Phase-4
science/risk, Phase-5 scanner, manifest, and unit contract tests were present;
the full suite above ran the entire tree. No existing test file was edited,
deleted, weakened, or marked skipped for this audit.

## CI availability

No repository GitHub Actions workflows were present. A search of the checked
tree also found no GitLab CI, CircleCI, Azure Pipelines, Jenkins, tox, nox, or
Makefile CI configuration. CI execution is therefore not available as
repository evidence; the gates above and below were run directly in the
recorded environment.

## V1 Phase 0–6 status matrix

`TESTED` below means reproduced by offline tests in this environment. It does
not qualify a venue, account, or live capability.

| Subsystem | Implementation status | Offline evidence | External / capital status |
|---|---|---|---|
| V1 Phase 0 contracts and capability foundation | `IMPLEMENTED` | `TESTED` | `UNVERIFIED` for venue capability inputs |
| V1 Phase 1 safe runtime skeleton | `IMPLEMENTED` | `TESTED` | `TEST GATE` for operational venue behavior |
| V1 Phase 2 recovery and protection | `IMPLEMENTED` | `TESTED` | `TEST GATE` for authenticated protection behavior |
| V1 Phase 3 causal data foundation | `IMPLEMENTED` | `TESTED` | `UNVERIFIED` for unattended public collection |
| V1 Phase 4 baseline science and risk | `IMPLEMENTED` | `TESTED` | `NOT ESTIMABLE` for profitability/economic validation |
| V1 Phase 5 scanner and alert | `IMPLEMENTED` | `TESTED` | `UNVERIFIED` for unattended public operation |
| V1 Phase 6 assisted-control shell | `IMPLEMENTED` | `TESTED` | `TEST GATE`; `assisted_enabled=false` |
| Unattended public Bybit collection | adapter and replay paths `IMPLEMENTED` | offline paths `TESTED` | `UNVERIFIED`; prior handoff records live collection as `BLOCKED BY ENVIRONMENT` |
| Authenticated Bybit capability | capability ledger `IMPLEMENTED` | offline gating `TESTED` | six states `UNVERIFIED`; `TEST GATE` |
| Economic/profitability status | evaluation and `NOT ESTIMABLE` outcomes `IMPLEMENTED` | deterministic offline science `TESTED` | `NOT ESTIMABLE`; no profitability claim |
| Assisted-capital status | guarded shell `IMPLEMENTED` | block/authority checks `TESTED` | `TEST GATE`; capital dispatch disabled and `assisted_enabled=false` |

The six Bybit capability entries in `docs/capability/bybit-v1.yaml` remain
`UNVERIFIED`: entry IOC with attached full MarkPrice stop; native stop
visibility/resizing on partial fill; reduce-only wire and matching enforcement;
ambiguous-submit handling; external native-stop fill reconciliation; and
native position-stop read/repair. Offline tests exercise fail-closed handling;
they do not promote any capability.

## Frozen V1 modules and contracts

The following are frozen at their existing versions and semantics. This audit
did not modify production code under `src/atlas/` or existing V1 tests.

- Domain contracts and canonical forms: `domain/capability.py`,
  `information.py`, `trade_plan.py`, `risk.py`, `execution.py`,
  `transitions.py`, `enums.py`, `money.py`, and `time.py`. Existing canonical
  JSON/hash methods remain authoritative. Versions remain capability,
  information, risk policy, and TradePlan `1.0`; strategy
  `CRYPTO_TREND_24H_V1` remains `1.0`.
- Persistence/capital-control: `persistence/schema.py`, `migrations.py`,
  `sqlite.py`; `runtime/assisted_control.py`, `recovery.py`,
  `protection_port.py`, `protection_evidence.py`,
  `reconciliation_evidence.py`, `flat_certificate.py`, `no_reversal.py`,
  `nautilus_boundary.py`, `wire_contract.py`, `writer_lock.py`, and
  `capability_ledger.py`. SQLite schema remains version 6. No migration or
  persistence behavior changed.
- Causal data/science/scanner: existing records, availability/replay/archive
  semantics, `CRYPTO_TREND_24H_V1`, its feature and policy contracts, Phase 4
  manifests and calendar, Phase 5 scanner/universe/alert behavior, residual,
  scenario, execution/policy replay and risk-sizing code remain frozen.
- Operational authority remains: venue state is external reality; Nautilus is
  the normal order/fill/position engine; ATLAS persists intent, approvals,
  reservations, risk, recovery, and audit. No competing OMS is introduced.
  Required durable intent/command/reservation precedes external effects;
  timeout/lost ACK is `UNKNOWN`; negative lookup is not proof of nonexistence;
  uncertainty blocks new risk; protection is independent of models and
  research; and risk, not model confidence, controls quantity/margin/leverage.
- `NO_TRADE` and `NOT_ESTIMABLE` remain valid outcomes. Historical downloads
  do not invent historical `received_at`; revised/future information does not
  rewrite earlier decisions; single-writer semantics remain intact.
- Bybit V1 remains linear, one-way, isolated-profile BTCUSDT/ETHUSDT with IOC
  entry, native MarkPrice protection, no pyramiding, and no immediate reversal.
  `assisted_enabled` remains false. No capability is promoted from
  `UNVERIFIED`.

## Additive V2 extension seams

These are classifications only; no seam below was implemented in Session 010.

| Seam | Frozen V1 code that remains intact | Suitable additive/versioned seam | Migration and replay concern |
|---|---|---|---|
| Instrument identity and universe | V1 symbol strings, `V1_INSTRUMENTS`, BTC/ETH decision calendar, and capital universe | New instrument/product/universe contracts with separate V2 identifiers and versions | Preserve old symbol keys and historical universe snapshots; never reinterpret a V1 row under a new product mapping |
| Public data adapters | `data/public_bybit.py`, V1 record schema and availability rules | Separate venue/source adapters that emit explicitly versioned canonical records | Preserve source event, publication, receive, availability, revision, and ingestion times; downloaded history cannot claim historical receipt time |
| Causal feature artifacts | V1 `FeatureSnapshot`, feature hash, 721-close contract and decision slots | Separate feature-artifact schema/version with dependency and availability lineage | Keep V1 hashes stable; replay must use point-in-time inputs and cannot rewrite prior snapshots |
| Scanner and universe logic | Phase 5 V1 scanner, V1 capital-enabled symbols, immutable alert/evidence behavior | A versioned V2 watch/universe pipeline with independent candidate outputs | Retain scanner policy/universe version and point-in-time membership for every replay |
| Policy/candidate contracts | V1 TradePlan and strategy serialization/hash | New V2 policy, candidate, and evaluation contract versions | Do not deserialize new semantics as V1; retain mapping and old fixture replay paths |
| Research/evidence storage | Existing research archive, decision calendar, SQLite journal and schema-v6 meaning | Additive research/evidence namespaces or separately versioned stores | Append evidence; preserve immutability, lineage, idempotency and old replay compatibility; any schema change needs its own migration review |
| Model workers | No V1 model-worker authority; models cannot relax risk or protection | Isolated workers producing versioned, auditable evidence/candidate artifacts | Bind inputs, model/version, availability cutoff, seeds and outputs; worker output has no operational side effect |
| Desktop and read-only IPC | No V1 desktop/UI control surface | A new read-only status/evidence interface with an explicit IPC contract | Replay/access must not grant order dispatch, alter journal state, or become a second writer |
| Future venue compatibility | Bybit/Nautilus V1 boundary and current capability contract | Separate venue/product adapter and capability contract versions | Qualify each venue empirically; preserve UNKNOWN/recovery/protection semantics and do not infer support from offline compatibility |

## V1-specific fixed assumptions inventory

These are intentionally fixed V1 behavior, not general-purpose V2 abstractions.
They were inventoried and not generalized.

| Area | Locations / fixed behavior | V2 treatment |
|---|---|---|
| Capability and venue | `domain/capability.py`; `docs/capability/bybit-v1.yaml`: Bybit testnet, linear product, one-way, BTCUSDT and ETHUSDT, isolated-margin assumption, Nautilus 2.0.0rc5; all six venue facts unverified | Frozen V1 contract; a distinct versioned venue capability is an extension seam |
| Runtime/wire | `runtime/__main__.py`, `nautilus_boundary.py`, `prerequisites.py`, `connectivity.py`, `wire_contract.py`, `protection_port.py`, `assisted_control.py`: BYBIT testnet identity, linear instrument IDs, BTCUSDT/ETHUSDT, position index zero, IOC opening entry, reduce-only closing, MarkPrice stop | Preserve exact V1 wire and fail-closed gates; new adapter/contract only in a later version |
| V1 data | `data/models.py` (`V1_INSTRUMENTS`) and `data/public_bybit.py`: Bybit public data and BTCUSDT/ETHUSDT allowlist | Keep V1 records and adapter stable; additional sources are separate adapters |
| Strategy/features | `strategy/crypto_trend_24h_v1.py`, `features.py`, `policy.py`: BTC/ETH only; slots at UTC 00/04/08/12/16/20; 721 hourly closes/720 returns; EWMA seed 48 and half-life 48 hours; absolute 24-hour momentum normalized by `sqrt(24)*sigma`; signal threshold ±0.5; T+30-second snapshot deadline; 60-second plan TTL; 24-hour horizon; stop is 2 sigma times `sqrt(24)` with frozen policy bounds; IOC and MarkPrice protection | Frozen strategy ID/version and hash; V2 feature/policy contracts must be separate |
| Phase 4 science | `science/decision_calendar.py`, `scenarios.py`, `residual_blocks.py`, `huber_mean.py`, `outer_loop.py`, `oof.py`, `manifest.py`, `phase4_engine.py`, `trade_plan_mapping.py`: joint BTC/ETH calendars/residuals, synchronized universe identity, frozen scenario/model parameters, V1 plan mapping | Frozen model and replay meanings; new universe/model/evaluation artifacts require new versions and inputs |
| Risk/scanner | `risk/sizing.py` iterates BTC then ETH; `scanner/models.py` defines BTC/ETH capital-enabled instruments and enforces their V1 scope; scanner observation/universe logic can be broader than capital eligibility | Preserve V1 priority and eligibility rules; V2 watch breadth must not imply V1 capital permission |
| Persistence and versions | `persistence/schema.py` / migrations: schema version 6; contract versions in domain classes and strategy remain 1.0 | No Session-010 migration or version change; future schema/version work remains additive |

The inventory came from repository-wide searches for symbols, venue strings,
contract identifiers and fixed assumptions, followed by inspection of the
matched implementations. It is a V1 inventory, not a request to parameterize
those constants.

## Stale descriptions corrected

- `README.md` previously described a Session-005 workspace based on
  Session-004. It now identifies the stabilized Session-009 baseline, the
  Session-010 V2 Phase-0 audit, the governing freeze sources, unverified live
  capabilities, `assisted_enabled=false`, no profitability claim, and the
  stale default-branch warning.
- `pyproject.toml` project description now describes the stabilized V1 and
  V2 transition-audit state. No package version, Python requirement,
  dependency, or runtime behavior changed.
- `.env.example` had stale Session-001/Phase-1 explanatory comments. Those
  comments were updated only; no credential variable was added or populated.

## Golden baseline and reproduction commands

`docs/v2/V1_GOLDEN_BASELINE.json` records the exact source commit, lock hash,
expected and observed Nautilus identity, existing V1 canonical hashes, fixed
manifest inputs, contract versions, deterministic Phase-4 decision evidence,
and an audit-owned replay projection. Values include:

- Capability contract: `10efccc833ce552a31d3eca071c7e309e44f2c8e114cca1c8bf4e9a319bfbb9d`.
- Engineering-default RiskPolicy: `d96a7dac7c877369921dcf56fbf36bf303ea649dc531d87e0bf1d54abed2988f`.
- Fixed-input Phase-4 model manifest: `e85a1f5f24ad6143f782d0a9dab5a56490c82aa36600c0fea028432bc2c8ab41`.
- Representative existing TradePlan fixture: `b76c3de08ab4f6085014db9b36c26d67bc772ca11884e66f19e1557d10459412`.
- Deterministic Phase-4 decision-calendar record: `a2dd5ab5088a4573f4aa4149e870e6ab53cdb2399649f8170c30c78bce5272ce`.
- Audit-owned small replay projection: `6385f72bee99b73ca37e3c4fb2746d194d61b2f0a40b7a9453b944f4f573774b`.

The first five values use existing V1 canonical hash methods. The replay
projection uses sorted compact JSON owned only by the audit; no V1 runtime hash
contract was added. Raw wheel bytes remain `BLOCKED BY ENVIRONMENT`; the
installed extension digest is recorded separately.

Recompute from the repository root in the locked Python environment:

```text
PYTHONPATH=src:. .venv/bin/python -m pytest -q tests/contract/test_v1_golden_baseline.py
PYTHONPATH=src:. .venv/bin/python -m pytest -ra
PYTHONPATH=src:. .venv/bin/python -m ruff check
PYTHONPATH=src:. .venv/bin/python -m mypy src tests
PYTHONPATH=src:. .venv/bin/python -m compileall -q src tests
git diff --check
```

The additive contract test recomputes the committed hashes from existing V1
code and fixtures and checks lock/pin identity. It does not alter production
behavior. No pre-existing V1 hash, fixture, schema, version, test, or source
file was changed.

## External gates and sensitive-data scan

- All six authenticated Bybit capability states remain `UNVERIFIED` and are
  `TEST GATE` items. No authenticated venue test or order was run.
- `assisted_enabled=false`. No capital action, test order, live order, or
  account access occurred.
- Economic/profitability validation remains `NOT ESTIMABLE`. Offline replay
  fixtures are not evidence of live results.
- Secret scanning is recorded in `docs/handoffs/session-010.md`. No scanner
  binary was available, so the handoff records the tracked-file and final-diff
  fallback and its result.

## Phase-0 verdict

V2 Phase 0 passes as a V1 exit audit and transition baseline: the authoritative
Session-009 source was verified, its pristine offline suite was reproduced,
the post-change gate and additive golden guard passed, frozen V1 boundaries
and additive seams are documented, and no V1 production behavior was changed.
This verdict does not qualify any exchange capability or enable assisted
capital. Those states remain `UNVERIFIED` / `TEST GATE`; profitability remains
`NOT ESTIMABLE`.
