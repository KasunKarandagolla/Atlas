# ATLAS V2 — public research and shadow desktop

ATLAS V2 is a causal crypto-futures research system with a read-only desktop for persisted market, scanner, watch, and evaluation evidence. The current engineering checkpoint is Session 037; its [closure ledger](docs/v2/ATLAS_FINAL_DEVELOPMENT_CLOSURE_V1.json) records the exact development verdict and remaining gates. The package version remains `2.0.0.dev25` for accepted identity continuity; the Windows installer carries its own version and exact source SHA. It makes no claim of trading profitability, authenticated exchange qualification, or readiness for live capital.

Capital ships disabled: `capital_enabled = false` and `assisted_enabled = false`. Public market data and the desktop do not require exchange private credentials. Never provide withdrawal permission to an ATLAS key.

## Current release state

| Area | Status |
| --- | --- |
| V1 baseline and capital/recovery invariants | Frozen; V1 schema 6 |
| V2 causal contracts and research | Implemented; deterministic regression tested |
| S1 and S2 | Exact-action shadow research; active baseline selector |
| S3 and S6 | Research hypotheses; S3 exact-action research path, S6 context without a complete action |
| S4 and S5 | Microstructure/crowding context; no exact executable action |
| S7 | Public event safety, alerts, and directional shadow evidence |
| S8 | Two-leg basket research; outside the single-action TradePlan path |
| M0 | Chronological Huber/ridge action-value baseline; current economics not estimable |
| M1 | Bounded installed LightGBM research challenger; no model voting or capital authority |
| Analogue support | Frozen compatibility gate; unsupported relative compatibility remains not estimable pending amendment |
| Discovery Lab | Bounded offline research; cannot change risk or promote itself |
| Bybit public data | Research/public-data target; no authenticated account qualification |
| Binance USD-M public data | Research/public-data target; protection remains a capital test gate |
| Bybit authenticated testnet | `UNVERIFIED / TEST GATE` |
| Binance authenticated testnet | `UNVERIFIED / TEST GATE` |
| Binance native protection | `TEST GATE` |
| External cross-host writer fencing | `BLOCKED BY ENVIRONMENT` |
| 72-hour continuous public soak | `BLOCKED BY ENVIRONMENT` until genuine evidence completes |
| Prospective evidence and economics | `NOT ESTIMABLE`; no positive economic claim |
| Final untouched holdout | `UNTOUCHED`, identity `aa7e691c14325ec1018df74037d48e8e0ce93de5755decb00d425b1549ede5f9` |
| Capital | Disabled |

The minimum before any positive economic claim is at least eight weeks of prospective shadow, at least 200 matured candidate opportunities, adequate regime coverage, and a dependence-aware interpretation. These are evidence floors, not an automatic pass. Historical fixtures, simulation, and additional model runs do not replace elapsed prospective time.

## Architecture and authority

The repository keeps V1 and V2 authority separate.

- `atlas-crypto-live` is the only credential-bearing crypto writer. It owns the Nautilus runtime boundary, immutable plan approval, revalidation, reservations, durable commands, protection, reconciliation, and recovery. This engineering release does not enable its assisted path.
- `atlas-ops` is the built-in single writer for public collection, scanner/watch state, research artifacts, and the separate `ops.sqlite` store. The installed product starts the bounded supervisor and public composition. `OpsRepository` uses one writer; desktop projections use a read-only SQLite connection. Public collectors have no exchange credentials or mutation routes.
- `atlas-worker` describes the optional disposable role for research/inference. This candidate has no general worker daemon command; the installed public composition evaluates the bounded M1 research challenger with zero capital authority. Any deployment-specific worker must receive no trading credentials, have no live-control database write path, and be unable to approve trades, change risk, or promote its output.
- `atlas-v2-projection` is a separate, read-only process. It opens an existing `ops.sqlite` read-only, accepts authenticated loopback connections only, and exposes bounded projection methods.
- `atlas-desktop` is a separate PySide6 observer of the read-only projection service. The installed `atlas-product` shell provides first-run setup and start/stop/resume/export controls through the bounded lifecycle; it does not write research rows, change risk or submit orders.

The human approval model remains:

> **ATLAS finds exact plan → human approves exact plan → bot manages execution automatically.**

For any future assisted release, the intended sequence is an immutable `TradePlan` → approval of that exact plan → live revalidation → atomic approval, intent, and reservation persistence → Nautilus execution → automatic fill, protection, exit, and recovery management. Approval of one plan grants no continuing autonomous strategy authority. V1 schema remains `6`; the additive V2 live-authority extension remains version `2`, contract `V2_CAPITAL_AUTHORITY_ATTESTATION_V1`, hash `94d59964c503a88a13d1fd05d7f02554153e7bb74ac73e7dc4cb93c4a8b645da`.

Research, strategy, model, news, desktop, and worker components cannot grant capital authority. A desktop action cannot enable capital. Startup never migrates an old configuration into enabled capital.

## Strategies and research

S1–S8 are versioned hypotheses, not a collection of independent votes.

- **S1** is multi-timeframe trend continuation/pullback research.
- **S2** is compression breakout research.
- **S1/S2 selector** preserves the frozen baseline ranking and exact action identity.
- **S3** is VWAP/statistical mean-reversion shadow research with an explicit one-hour action contract.
- **S4** measures sequence-valid book/flow and absorption context; no exact action is emitted.
- **S5** records derivatives/crowding, deleveraging continuation, and post-cascade reversal context; no exact action is emitted.
- **S6** measures cross-sectional residual relative strength; its missing stop/action contract keeps it out of single-action execution.
- **S7** collects event evidence, applies a conservative event safety gate, and stores directional reaction research. Source coverage is not considered verified by an empty event list.
- **S8** stores an explicit two-leg hourly pairs basket forecast and replay contract. It is not a normal one-plan order or capital path.

M0 is the chronological Huber/ridge action-value baseline. M1 is the bounded installed LightGBM research challenger. Analogue retrieval preserves the frozen compatibility checks; unsupported relative compatibility remains `NOT ESTIMABLE` pending the [versioned amendment review](docs/v2/SESSION036_ANALOGUE_AMENDMENT_PROPOSAL_V1.md). Discovery proposals and failures remain in their finite preregistered family. No automatic promotion occurs; current promotion state is `INTEGRATED`, not `DECISION_ELIGIBLE`.

## Supported and research venues

Bybit and Binance USD-M are public market-data research targets. Public metadata, fees, filters, or official documentation do not establish current values for a particular live account. Installed Nautilus rc5 adapter evidence is engineering evidence only. Authenticated account evidence is absent for both venues. Bybit testnet order/protection/recovery behavior remains unverified; Binance native entry protection and account recovery remain test gates. No mainnet order was used for this release.

Current public fee/filter/margin observations must be stored with their source, product revision, and availability time. If account-specific fees, filters, margin mode, risk tiers, or protection cannot be authenticated and proven, capital qualification remains blocked.

## Install and launch

The release targets Python 3.12. The dependency lock is `requirements-lock.txt`; its SHA-256 is recorded in the release manifest. Do not change dependency versions to address a packaging convenience.

### Linux

```bash
uv venv --python 3.12.13
uv pip sync --python .venv/bin/python requirements-lock.txt
uv pip install --python .venv/bin/python --no-deps -e '.[desktop,offline-research]'
```

The checked Linux target is a PyInstaller one-folder bundle with separate desktop and projection-service executables. A source install can launch them with `atlas-desktop` and `atlas-v2-projection`. The self-contained Windows installer needs no developer tooling. See the [Windows research run guide](docs/v2/SESSION037_WINDOWS_RESEARCH_RUN_GUIDE.md) and [native build evidence](docs/v2/SESSION037_OFFLINE_VALIDATION_V1.json). Native Windows Server CI and actual owner Windows 11 qualification are separate scopes.

### Public-only connection check

This short command probes public Bybit and Binance endpoints, writes sanitized evidence, and uses a temporary ops store. It is a transport/translation check; it does not persist a user’s ongoing archive, qualify an account, or count toward the 72-hour soak.

```bash
mkdir -p evidence
atlas-v2-public-qualification --output evidence/public-qualification.json
```

### Genuine 72-hour public operations soak

Run the durable foreground soaker on an always-on host. It writes each sanitized JSONL sample and fsyncs it. Do not alter the duration or evidence timestamps to claim elapsed time. A restart must use the original evidence file and exact run ID; an interruption resets the continuous segment and the final record reports the resulting duration/status.

```bash
atlas-v2-public-soak --evidence evidence/public-soak.jsonl --duration-hours 72 --interval-seconds 60
```

Resume the same incomplete run using its `run_id` from the header:

```bash
atlas-v2-public-soak --evidence evidence/public-soak.jsonl --duration-hours 72 --interval-seconds 60 --resume-run-id RUN_ID
```

The Session-025 release records the soak as `BLOCKED BY ENVIRONMENT` until a real continuous artifact is validated for run identity, duration, continuity, sample timestamps, source health, and sanitized contents.

### Projection service and desktop

Start the V2 projection service against an existing initialized `atlas-ops` database. The service never creates or modifies that database. Use a private local token file; the service creates it with mode `0600` on POSIX. Keep the service bound to `127.0.0.1`.

```bash
atlas-v2-projection --db /path/to/ops.sqlite --archive-root /path/to/public-archive --token-file /path/to/private/atlas-ipc.token --host 127.0.0.1 --port 0
```

Copy the printed loopback port into a second terminal and launch the desktop:

```bash
atlas-desktop --host 127.0.0.1 --port PORT --token-file /path/to/private/atlas-ipc.token
```

The Linux bundle includes `atlas-v2-projection` and `atlas-desktop` as separate processes. A missing projection service produces an explicit reconnecting/unavailable state. Desktop diagnostics show desktop/service version, IPC protocol version, release classification, and evidence freshness. The health method also reports process uptime and the current persisted source, lag, queue, model-worker, capability, recovery, capital, economics, and soak states. Metrics not supplied by a runtime are explicitly unavailable rather than inferred.

`atlas-ops` remains the sole research writer and starts before its projection service. The installed product drives public collection, scanner/watch coordination, bounded maintenance and outcome maturation. The separate observer can also read an existing ops store; an empty store correctly displays unavailable scanner evidence. Use the installed product lifecycle to create and operate a continuous run.

## Configuration and evidence

Public/shadow research uses public endpoints and does not need exchange private credentials. The installed product records immutable run configuration and source identity below the selected research data folder. Its default public Bybit profile requires no secret. The separate observer uses the local ops database path, optional public archive path, loopback host/port and a private IPC token file. No `atlas-worker` configuration is shipped because there is no worker command in this candidate. Credential-bearing live configuration is isolated to `atlas-crypto-live` and remains disabled for this release.

`.env.example` contains names/placeholders only. Never commit `.env`, tokens, passwords, API keys, private account identifiers, or sensitive logs. Prefer OS-protected secret storage for any future authenticated testnet process; use least privilege and disable withdrawals.

Keep durable evidence under deployment-managed paths outside the source tree:

- `ops.sqlite` and its SQLite sidecars belong to the single `atlas-ops` writer. Back it up using SQLite’s online backup mechanism or after a clean shutdown; never copy a live database file as if that were an atomic backup.
- Parquet public observations are immutable archive evidence. Back up the archive and its index together.
- Qualification, soak, sanitized logs, release manifests, and recovery reports belong in a dated evidence directory with restrictive local permissions.
- The V1 live-control journal and backups are separate from `ops.sqlite`. Do not use research or desktop backups as a recovery journal.

## Recovery, logs, shutdown, and backup

The release desktop is not the engine and cannot resolve recovery. If an authenticated writer is ever present, startup must begin in recovery: restore the journal and reservations; query unresolved order identities, executions, positions, wallet, and native protection; reconcile buffered events; repair or reduce unprotected owned exposure; and resume new risk only after a valid reconciliation certificate. `UNKNOWN` remains unresolved until positive terminal evidence; a negative recent-order lookup or elapsed time alone cannot free its reservation. Contradictory later evidence reopens recovery.

Keep logs sanitized. Record event IDs, state transitions, evidence refs, status codes, and exception types; do not dump request headers, credential values, full account objects, or private IDs. On normal shutdown, stop the desktop first, then the projection service, flush/close the ops writer and archive, and verify the process has exited before creating backups. Never delete unresolved intents, pending commands, protection evidence, or reservations to make the UI appear healthy.

## Environment gates and security

- Authenticated Bybit and Binance testnet credentials/harnesses are absent. No authenticated exchange request was made for Session 025.
- Binance protection is not established by the pinned adapter’s separate algo-order path.
- Cross-host fencing requires an externally demonstrated control that prevents an old host from authenticating/sending before a replacement starts. A local lock or higher epoch is insufficient.
- Public docs and adapter source are not account evidence. Public fee/filter/margin values cannot promote a profile to `SUPPORTED`.
- Windows packaging/runtime, a genuine continuous 72-hour soak, and prospective economic evidence were unavailable in this environment.
- The final holdout has not been viewed or consumed. Prospective evidence is insufficient; the known genuine matured candidate count is zero at this checkpoint. Economic status remains `NOT ESTIMABLE`.
- A final repository scan and package-content scan are recorded in the Session-025 validation artifact. Release diagnostics keep capital false and do not equate engineering health with capital readiness.

To qualify authenticated testnet behavior later, use a dedicated testnet account and a reviewed credential-safe harness. Bind the account/profile, position/margin mode, instrument filter/fee/margin revisions, pinned Nautilus distribution/source/artifact, order identity, IOC partial-fill behavior, native full-position protection, funding/cash evidence, history retention, reconnect/recovery, and emergency reduce-only path. Save redacted evidence with source hashes and timestamps. Exercise the exact testnet profile; never substitute public documentation, mocks, or synthetic fixtures. Do not use mainnet for this qualification.

## Operator rule

ATLAS may present research evidence and an exact immutable plan. It does not authorize capital in this release. The `SHADOW_RELEASED` classification means only that the non-capital engineering candidate passed its recorded deterministic release gates; it is not a profitability or live-readiness claim.
