# ATLAS V2 Session 012 — Phase 1B Handoff

## Checkpoint and authority

- Starting branch: `impl/session-011-v2-foundation-contracts-memory`
- Starting SHA: `a2d79569b57004dcfb63215e1babd1a9962c62c1`
- Final branch: `impl/session-012-v2-phase1-data-model-core`
- Implementation commit(s): recorded in the Session-012 branch history and final session report.
- Final pushed SHA: recorded in the final Session-012 report after push. A commit cannot contain its own SHA.
- The tracked V1 clarification/freeze and supplied V2 freeze were both read. The supplied V2 freeze remained at `/home/kasun/Music/atlas/ATLAS_V2_FINAL_INTRADAY_INTELLIGENCE_IMPLEMENTATION_FREEZE.md` and was not copied into the worktree.
- `docs/v2/V1_EXIT_AUDIT.md`, `docs/v2/V1_GOLDEN_BASELINE.json`, and handoffs 010 and 011 were read.
- The Session-011 contracts were reused. The only Session-011 implementation seam extended is two read-only `ops.sqlite` repository helpers for artifact-index and source-health enumeration.

## Implementation

### Public data

`src/atlas/v2/data/` adds strict raw observation records with deterministic event identity, exact-byte SHA-256 validation, duplicate idempotency, and conflict quarantine. A conflicting duplicate payload is saved as a distinct Parquet quarantine record before its operational conflict index is registered. Final 15M/1H/4H bars use UTC half-open boundaries and append-only correction references. Historical replay availability stays separate from actual import receipt time. JSONL import is bounded and quarantines malformed or non-final rows; Parquet rows retain exact payload bytes, source-file SHA-256, deterministic chunk ID, import row number, actual receipt timestamp, and reconstructed replay availability. `ops.sqlite` stores only the minimal artifact index, source health, subscription plan, and cursor state.

The collector requires raw bytes to match the observation hash, persists Parquet before registering an observation index, and refuses cursor checkpoints while archival is pending. Restart restores sequence cursors and active-watch subscriptions, and moves a previously healthy source to `INCOMPLETE_SNAPSHOT` until overlap/gap repair is reconciled. Reconnect backoff is bounded. Source health supports current, stale, disconnected, reconnecting, incomplete snapshot, sequence gap/conflict, and rate-limited states.

Dynamic universe snapshots bind full instrument/product revisions and source-health availability to the information cutoff. The snapshot references content hashes for the observed inputs that drive classification. Engineering defaults are 30 observed days, required bars, USD 10m trailing 24h quote turnover, and 10 bps spread. Tier 0–3 are deterministic compute tiers; Tier 4 produces no inferred capital eligibility. Suspended/delisted products remain represented in point-in-time evidence.

### Venue surface and NautilusTrader decision

The installed pinned NautilusTrader `2.0.0rc5` artifact was inspected first. Its Bybit package exposes typed `request_instruments`, `request_bars`, `request_trades`, `request_tickers`, `request_funding_rates`, and order-book requests through the adapter HTTP/data client. These are suitable typed event surfaces, but do not preserve the exact raw response bytes and receipt provenance required by the V2 observation artifact and short REST overlap proof. The pinned Binance package exposes typed adapters/events but no corresponding read-only USD-M query helper for these snapshot/provenance requirements. The implementation therefore uses the stdlib transport with an explicit credential-free GET and query-parameter allowlist for both venues. It exposes no authenticated, account, or order endpoint and adds no execution SDK. No WebSocket channel is implemented or claimed.

- Bybit: instrument info; final klines; recent trades; ticker BBO, mark/index, current funding, and current OI fields; funding history; OI history.
- Binance USD-M: exchange info; final klines; aggregate trades; book ticker; premium index/current mark/index/funding; funding history; current and historical OI.
- No depth-diff, liquidation, announcement, or authenticated surfaces are claimed.

Short qualification reconnect evidence is REST overlap after explicit disconnect/reconnecting/incomplete transitions. It exercises collector duplicate handling and restart/resubscription, not a WebSocket transport reconnect.

### Model Arena

`src/atlas/v2/models/` adds a bounded provider queue, immutable request identity handling, original-deadline retries, strict response/hash and requested-output-dimension validation, late-output archival with unusable status, a deterministic fake provider, and an optional durable forecast archive. The local provider uses versioned stdin/stdout IPC, a small allowlisted environment, bounded input/output, POSIX CPU/address-space/file-size limits, and timeout process-group termination. The remote provider requires explicit HTTPS, sends no credential headers, and preserves request/idempotency IDs and the original deadline.

The statistical baseline is a `ModelProvider` over causal close inputs. It computes deterministic empirical horizon-return summaries and marks unsupported/insufficient outputs explicitly; it makes no alpha claim. The TiRex-2 and Kronos-mini boundaries enforce exact manifest hashes, versioned preprocessing/postprocessing, causal prefix limits, explicit hashes, and fixture-only inference. Kronos-mini validates OHLCV consistency and unit/transform metadata. No model weights or heavy model packages were added.

Worker tests verified the IPC rejects API-secret, account, order/risk, and live-control database-path fields, including filesystem paths embedded in request, manifest, or artifact input values. The child environment is allowlisted and excludes Bybit/Binance keys, live DB settings, account identity, and future private-token variables. The launch protocol passes no live-control DB path or inherited file descriptor. The child still runs under the same OS user and filesystem permissions; no stronger OS sandbox is claimed.

## Dependencies and persistence

- Dependencies: unchanged; no `httpx` or model runtime package added.
- `requirements-lock.txt` SHA-256: `0d7b5cc6129aab127f07a1b5bce4b7794eec5c48db0f99451eba675031ca2b39`.
- `ops.sqlite`: Session-011 schema version 1 retained; no migration. Added read helpers only; no market-data warehouse or second live-control store.
- Research observations/bars: immutable Parquet chunks. DuckDB is not used as a write store.

## Qualification and tests

- Short public command: `PYTHONPATH=src:. python -m atlas.v2.data.qualification --output docs/evidence/session-012-public-qualification.json`
- Short public qualification: `TESTED`, credential-free, Linux/Python 3.12.13; started `2026-09-24T19:28:54.428619+00:00`, ended `2026-09-24T19:29:10.690654+00:00` UTC.
- Both Bybit and Binance USD-M BTCUSDT public REST surfaces passed. Sanitized evidence records 170 translated observations, 22 final 15M bars, zero kline interval gaps, zero duplicate conflicts, distinct point-in-time product mappings, and active-watch subscription restoration. An earlier retry in this final qualification sequence encountered a transient Binance `PublicDataError`; the final bounded run passed.
- Disconnect, reconnecting, incomplete snapshot, and healthy-after-overlap transitions were recorded for each source. Initial and restart overlap checks both returned `DUPLICATE` for both venues.
- `PYTHONPATH=src:. python -m pytest -ra`: **466 passed, 1 skipped** in 237.33s. The skip is the pre-existing opt-in public testnet check.
- V1 invariant smoke: **11 passed**.
- Ruff: **passed**.
- Mypy `src tests`: **passed**, 215 source files.
- Compileall: **passed**.
- `git diff --check`: **passed**.
- Golden baseline SHA-256 remains `b6d47c1c7a2e416304dd57d9055599201c474c8bada600cf23c2cbc09f90e53c`; the file is unchanged.

## Status and remaining gates

- V2 public research/data foundation: `IMPLEMENTED` / `TESTED`.
- Dynamic universe and watch-aware subscription restoration: `IMPLEMENTED` / `TESTED`.
- Model Arena engineering core and deterministic isolated worker: `IMPLEMENTED` / `TESTED`.
- Statistical baseline: `IMPLEMENTED` / `TESTED`; no economic value is claimed.
- TiRex-2 and Kronos-mini fixture boundaries: `IMPLEMENTED` / `TESTED`; exact checkpoint/package integration is `UNVERIFIED` because exact model artifacts are not present. Promotion remains below `INTEGRATED`.
- Chronos-2: `UNVERIFIED`; adapter deferred as permitted for Phase 2.
- 72-hour public soak runner: `IMPLEMENTED` / `TESTED` for foreground execution and resume evidence. Starting a 72-hour process that survives this implementation environment is `BLOCKED BY ENVIRONMENT`; no long-lived or hidden process was left running. Resume command: `PYTHONPATH=src:. python -m atlas.v2.data.soak --evidence <durable-path> --resume-run-id <recorded-run-id>`.
- V2 strategy economic value: `NOT ESTIMABLE`.
- V2 capital authority: `TEST GATE` / disabled; Tier 4 remains false.
- Six V1 Bybit capabilities: `UNVERIFIED` / `TEST GATE`; `assisted_enabled=false`.
- No Phase-2 features/strategies, evaluator, desktop work, orders, approvals, reservations, or capital dispatch were added.
- Environment: Python 3.12.13 on Linux, public network available, no exchange credentials used.
- Secret scan: repository `gitleaks`/equivalent executable was unavailable. A Python tracked-file plus final-diff fallback scan checked 238 tracked/changed paths for PEM private keys, AWS/GitHub/Slack/OpenAI-style token patterns, and non-template `.env` files. It found no matches; the existing `.env.example` template was excluded from the environment-file check.

## Phase-1 verdict

`V2 Phase 1: BLOCKED BY ENVIRONMENT` — the 72-hour soak is resumable but could not be started as a durable run in this implementation environment. The short credential-free qualification and Tier C code/test gates passed. This verdict does not qualify model value, strategy value, V1 execution, or V2 capital authority.
