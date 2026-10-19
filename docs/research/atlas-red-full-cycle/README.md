# ATLAS RED — full-cycle economic evidence programme (session 2026-10-19/20)

**Status: RESEARCH EVIDENCE COLLECTED AND EVALUATED. Whole-programme economics
remain NOT ESTIMABLE. No order, no private endpoint, no account access, no
capital, no policy promotion, no merge.**

This directory records a single integrated research programme: genuine public
Binance USD-M observations → multi-coin discovery → per-strategy applicability
at each strategy's own native clock → sealed research actions → modelled
lifecycle → mature-outcome accounting → ML-ready tables.

Everything here is produced by the code in this branch. Nothing is asserted
that was not measured in this session.

---

## 1. Source base and branch

| Item | Value |
| --- | --- |
| Local starting SHA | `af78d7a` (`impl/session-035-public-host-source-qualification`, dirty working tree preserved) |
| Verified G1 correction | `7dcf68d85135f94fb47343a9fc8dced9a4557358` (`fix/atlas-red-g1b-duplicate-cache-2026-10-19`) |
| Chosen base | `7dcf68d` — the G1-B correction tip, which is a descendant of every `origin/research/atlas-red-*` and `origin/fix/atlas-red-p1-*` branch except `research/atlas-red-binance-evidence-e1-2026-10-17` (its content is carried by the `-e1-recheck` branch, which **is** an ancestor) |
| New branch | `research/atlas-red-full-cycle-evidence-ml-2026-10-19` |
| Worktree | `/home/kasun/Music/atlas-red-ws/mnt/wt/atlas-red-full-cycle` (isolated; the owner's dirty checkout was not touched) |
| Immutable freezes | V1 `c13cad1b…`, V2 `e868e3e2…`, Agent `d3e7b0b3…` — all three re-verified byte-identical in this session |

`origin/impl/session-001-foundation` remains the historical default branch and
was **not** used as an implementation base.

## 2. What was built

| Artefact | Path | Purpose |
| --- | --- | --- |
| Bounded public acquisition | `src/atlas/v2/research/full_cycle/acquisition.py` | Reuses `PublicHttpClientV2`, `PublicHttpsSessionV1`, `BinanceUsdMPublicReaderV2` and the `atlas.v2.data.binance` translators; adds only an explicit request budget and a fair-rotation deep cohort |
| Storage guardian | `src/atlas/v2/research/full_cycle/storage.py` | Measures the kernel-enforced workspace, reserves before writes, refuses over-budget writes, emits a durable manifest |
| Research evidence store | `src/atlas/v2/research/full_cycle/store.py` | Append-only SQLite projection: source observations, product metadata, bars, discovery population, market context, applicability, actions, lifecycle, outcomes, model inventory, ML provenance, missingness |
| Discovery + context | `src/atlas/v2/research/full_cycle/analysis.py` | Full observed population with inclusion/exclusion reasons; regime, volatility, compression, correlation and liquidity context computed with the production `ema`/`atr`/`adx` owners |
| Applicability matrix | `src/atlas/v2/research/full_cycle/applicability.py` | Per-strategy evaluation at each native clock using the **production predicate functions** (`s1_trend._regime`, `s1_trend._setup`, `s2_breakout.compression_passes`, `s2_breakout.range_width_valid`, `s2_breakout.breakout_side`) and the production policy hashes |
| Research action + lifecycle | `src/atlas/v2/research/full_cycle/actions.py` | Sealed entry/stop/quantity/horizon under an explicitly labelled virtual risk profile; modelled HOLD/CLOSE/TIGHTEN/PARTIAL_TP/trailing paths; production `fee_cash`/`funding_cash` owners |
| ML inventory + tables | `src/atlas/v2/research/full_cycle/models.py`, `ml.py` | Checkpoint identity/revision/hash inventory, bounded download, bounded inference probe, ML-ready rows, chronological split with embargo, empirical/ridge/LightGBM comparison |
| CLIs | `scripts/atlas_red_full_cycle_{collect,evaluate,ml,report}.py` | Reproducible collection, evaluation, ML and reporting |
| Tests | `tests/v2/test_atlas_red_full_cycle.py` | 21 tests: bounded storage, oversized-write rejection, append-only store, discovery denominator, production predicate reuse, S3/S7 source refusal, action sealing, lifecycle accounting, ML leakage rejection, no order/private path, freeze hashes |

## 3. Genuine public evidence collected

| Item | Measured |
| --- | --- |
| Source | Binance USD-M **public REST**, credential-free, allowlisted, no proxy, no private route, no order route |
| Collection window | 2026-10-19 19:21Z → 2026-10-19 21:00Z (see `evidence/EVIDENCE_INVENTORY.json`) |
| Cycles | cheap discovery cycles every 30 s plus deep refresh cycles on a bounded 10-minute bucket |
| Observed instruments | 658 admitted USDT-perpetual products; 793 symbols with a 24 h ticker observation; the full observed population is retained every cycle, including symbols outside the admitted universe |
| Eligible population | 122–124 symbols per cycle under the registered turnover/spread/product filters |
| Deep cohort | 20 symbols per cycle by deterministic fair rotation over the ranked eligible population (never relabelled as full-universe coverage) |
| Raw receipts | every request payload stored zlib-compressed with its SHA-256, local receipt time, HTTP status and availability class |
| Bar history | H1, H4 and M15 for the eligible universe; M15 backfilled to the registered 30-day S2 comparison sample for the deep cohort; M1 for the deepest cohort; depth, aggTrades, open interest and funding history for the deep cohort |

The collector is **not** the production ATLAS ten-second observer. The G1
storage preflight gate (48-hour projection 1,377,141,631,200 bytes against a
30,218,625,024-byte free-host requirement) remains open, and
`S41_MAX_BREADTH_BYTES_PER_CYCLE_V2` is still `None`. This session therefore
ran an explicitly limited, labelled **research probe**; no full-programme
coverage claim is made.

## 4. Strategy applicability — what the evidence actually says

The full `coin × market context × strategy applicability` matrix is in the
research store (`strategy_applicability`) and is summarised in
`evidence/APPLICABILITY_MATRIX.csv`. Every registered family is accounted for,
including the cases that were inapplicable, rejected, unavailable or not
evaluated. Typed states used: `NOT_DUE`, `NO_SETUP`, `INELIGIBLE`,
`SOURCE_UNAVAILABLE`, `RESOURCE_NOT_EVALUATED`, `MISSED_DEADLINE`,
`CANDIDATE`, `RESEARCH_ACTION`, `CONTEXT_ONLY`.

Material findings from this session:

1. **S1 (MTF trend pullback) is the only family that produced confirmed
   triggers in this window.** Its 4H-regime + three-1H-pullback setup is
   strict: the majority of evaluations ended `NO_SETUP / SETUP_RULE_FAILED`,
   and a substantial minority reached a setup and then failed the registered
   trigger (`TRIGGER_RULE_FAILED`).
2. **S2 (compression breakout) is gated by its own 30-day sample.** For
   symbols without the backfilled 30-day M15 history the state is
   `SOURCE_UNAVAILABLE / INSUFFICIENT_30_DAY_HISTORY_AFTER_WARMUP`; where the
   sample exists, the compression predicate is the binding constraint
   (`COMPRESSION_RULE_FAILED`).
3. **S3 (VWAP/AR(1) reversion) is not estimable from this probe.** The
   registered contract needs the *every-trade* population; the available
   public source is aggregated trades. The state is
   `SOURCE_UNAVAILABLE / EXACT_NATIVE_TRADE_POPULATION_UNAVAILABLE` and it is
   explicitly not upgraded to a "no signal" reading.
4. **S4 and S5 remain contextual.** Depth/aggTrades and open interest/funding
   were collected for the deep cohort, so those symbols are `CONTEXT_ONLY`;
   they are never converted into standalone directional signals. The complete
   liquidation population is not available, so S5 stays
   `SOURCE_UNAVAILABLE` where that input is required.
5. **S6 (cross-sectional relative strength) is computable** over the eligible
   universe with the registered formula (β on BTC, residual volatility, sum of
   the latest four aligned hourly residuals). It remains a context strategy:
   the repository's own S6 contract reserves the exact action semantics, so
   the state is `RESEARCH_ACTION` with `S6_EXACT_ACTION_CONTRACT` recorded as
   unavailable evidence.
6. **S7 is not estimable.** No verified event calendar was available; an
   unavailable calendar is never read as CLEAR.
7. **S8 is not due.** No registered pair catalogue was bound in this session,
   so the state is `NOT_DUE / NO_REGISTERED_PAIR_FOR_SYMBOL`, not "no signal".
8. **Path B** is recorded as `RESOURCE_NOT_EVALUATED` for the eligible
   population: the bounded novelty screen was not evaluated inside the
   session's time budget. It is not reported as a negative result.

## 5. Research actions, lifecycle and economic value

Every sealed action records: instrument revision, strategy and policy hash,
side and direction origin, entry reference and collar, stop price and stop
basis, quantity and notional under the explicitly labelled research risk
profile, horizon and deadline, fee/spread/funding/slippage assumptions, hard
refusal reason, evidence references and the code hash.

* The registered S1/S2 policies contain **no take-profit** (`management_rule`
  forbids `partial_tp`), so the take-profit field is recorded as
  `NOT_IN_REGISTERED_POLICY` rather than populated with an invented level.
* Lifecycle paths are modelled over genuinely observed future bars with the
  production fee and funding owners; the live order queue and intrabar
  sequence are unobservable, so every path carries
  `MODELLED_PATH_INTRABAR_SEQUENCE_UNOBSERVABLE`, and a fill beyond the
  adverse collar is `NO_FILL` rather than a fabricated fill.
* **Whole-programme economics: NOT ESTIMABLE.** See
  `evidence/ECONOMIC_EVIDENCE.json` for the exact reason in each case.

## 6. Machine learning

* In-repository models used: the arithmetic-mean family (`atlas.v2.science.m0`),
  the empirical payoff comparator, and the LightGBM L2 cash-mean challenger
  (`atlas.v2.science.m1`, LightGBM present in the environment).
* Foundation checkpoints: exact repository identity and revision were resolved
  for `NX-AI/TiRex-2`, `autogluon/chronos-2-small`, `NeoQuasar/Kronos-mini` and
  `NeoQuasar/Kronos-Tokenizer-2k`; their weight footprints are 1,117 MB,
  112 MB, 16 MB and 16 MB respectively. The inference runtime (CPU Torch)
  does not fit inside the strict 5 GB ATLAS-managed storage boundary together
  with the retained evidence, so these checkpoints are recorded as
  **BLOCKED BY ENVIRONMENT** for inference. No substitute checkpoint was used
  and no model was claimed as integrated on the strength of a download.
* ML-ready rows are built from actions joined to market context, with explicit
  `label_state` (`MATURED_NET_CASH` / `CENSORED_*` / `UNLABELLED`). A row
  without a matured after-cost outcome keeps `label_state = UNLABELLED` and
  `label_bps = null`; **no cash target is invented**.
* The chronological split is 60/20/20 with a 24-hour embargo, and the
  comparison is against the empirical mean on the same rows.

## 7. Storage

See `STORAGE_REPORT.md`. The session ran inside a kernel-enforced, size-capped
ext4 filesystem on a loop device; every write is reserved against the guard
before it happens, and an oversized synthetic write is rejected before it can
exceed the limit (covered by a test).

## 8. Reproduction

See `REPRODUCIBILITY.md`.

## 9. What this session does **not** claim

* No claim that ATLAS is profitable. The whole-programme after-cost cash is
  NOT ESTIMABLE from this session's evidence.
* No claim that the historical eight-week / 200-opportunity floor is met.
  Seven hours of collection does not meet it.
* No claim that the production observer is qualified. The G1 gate is open.
* No claim of acceptance. This is a review candidate, not a self-acceptance.
