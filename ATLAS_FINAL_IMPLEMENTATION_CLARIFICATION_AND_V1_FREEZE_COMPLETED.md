# ATLAS — Final Implementation Clarification and V1 Freeze

**Date:** 21 September 2026  
**Purpose:** Freeze implementation contracts for phases 0–6.  
**Status:** Architecture decisions specified; installation, exchange qualification and economic validation remain unperformed.

This document supplements **ATLAS_REVISED_ARCHITECTURE_AND_ADVERSARIAL_REVIEW.md**, which remains authoritative except where this clarification makes a more specific choice. It uses the supplied **ATLAS_GROUND_LEVEL_IMPLEMENTATION_ARCHITECTURE.md** and **ATLAS_ASTRA_CONTEXT_OPTIMIZED_REVIEW.md** to preserve the broader objectives. The task references **ATLAS_FINALIZED_LEVERAGED_RETAIL_PROPOSAL.md**; that exact file was not supplied and could not be found in two searches. Its contents are not presumed identical to the context review.

The objective remains a continuously operating research, scanning, alerting and optionally trading platform, with separate crypto and FX capital and strategy logic. This freeze qualifies the first crypto implementation. FX, richer features and foundation models retain their agreed interfaces and admission paths.

No ATLAS repository, installed dependency lock, account, execution log or market dataset was supplied. This is a proposed implementation specification, not a completed implementation or evidence of profitability. No backtest, exchange order, fault-injection test or runtime benchmark was executed for this consultation.

## 0. Decisions and evidence labels

Every requirement below uses the following distinctions:

| Label | Meaning |
|---|---|
| **FACT** | Supported by the available documents or a linked primary source; scope and version matter. |
| **FREEZE** | Architectural choice to implement. Changing it creates a new contract or policy version. |
| **DEFAULT** | Exact initial engineering choice, not an optimized or demonstrated value. |
| **CONFIG** | A typed parameter, versioned and recorded with every affected decision. |
| **ASSUMPTION** | Condition the design depends on; validate before the corresponding use. |
| **TEST GATE** | Unverified behavior or empirical question. Failure disables the affected capability. |

**Recommended v1:** one dedicated Bybit account/subaccount, one-way linear BTCUSDT and ETHUSDT, isolated margin, one writer, no pyramiding. Submit an IOC limit entry carrying a full-position native market stop. Retain that stop while closing with an explicit-quantity reduce-only order. A small protection port handles position-stop repair and raw protection inspection; it never becomes a second order manager. Use four-hour trend entries, a fixed stop and a 24-hour time exit. Admit new risk only after causal scenario evaluation and a serialized risk reservation.

The durable state is a **product of execution lifecycle, protection status and reconciliation health**, not a single enum pretending these conditions cannot coexist.

## 1. Bybit and Nautilus execution/protection contract

### 1.1 Pin identity and verification boundary

**DEFAULT candidate pin:** Python package **NautilusTrader 2.0.0rc5**, tag **v2.0.0rc5**, repository commit **1b0a49d2792a9432a3aca3fcb617ce7a630d905e**. The published release identifies this version and the commit; it is a prerelease. The release includes changes to reduce-only enforcement and external-order claiming relevant to this design. This is a new proposed pin, not a claim about an existing ATLAS installation. [Release](https://github.com/nautechsystems/nautilus_trader/releases/tag/v2.0.0rc5), [release commit](https://github.com/nautechsystems/nautilus_trader/commit/1b0a49d2792a9432a3aca3fcb617ce7a630d905e)

**Material verification limit:** tagged source/raw-source retrieval failed during this review. Current integration documentation was accessible, but cannot prove all behavior in that wheel. Therefore the exact requested “pinned-capability reality” is **partially verified: release identity verified, installed/source capability and account behavior not verified**. The following design is concrete; no row marked TEST GATE may be represented as passed.

**FACT, current documentation only:** Nautilus describes limit/IOC and linear reduce-only orders, native SL parameters including trigger basis and full-position mode, and price/quantity amendment. Bracket/OCO is not implemented. GTD maps to GTC. A generic whole-position parameter is not implemented by treating Bybit close-on-trigger as equivalent. Funding is not emitted as an ordinary fill. These facts motivate explicit contracts rather than assumed bracket behavior. [Current Bybit integration guide](https://nautilustrader.io/docs/latest/integrations/bybit/)

Phase 0 produces a capability manifest containing:

~~~yaml
runtime:
  distribution: nautilus_trader
  version: 2.0.0rc5
  source_commit: 1b0a49d2792a9432a3aca3fcb617ce7a630d905e
  installed_artifact_sha256: REQUIRED_AT_INSTALL
  dependency_lock_sha256: REQUIRED_AT_INSTALL
  python_platform_abi: REQUIRED_AT_INSTALL
venue:
  environment: testnet
  account_identity_hash: REQUIRED
  account_generation_and_margin_mode: REQUIRED
  product: linear
  position_mode: one_way
  symbols: [BTCUSDT, ETHUSDT]
capabilities:
  entry_ioc_with_attached_full_mark_market_stop: UNVERIFIED
  native_stop_visible_and_resizes_on_partial_fill: UNVERIFIED
  reduce_only_wire_and_matching_enforcement: UNVERIFIED
  ambiguous_submit_not_treated_as_definite_rejection: UNVERIFIED
  external_native_stop_fill_reconciliation: UNVERIFIED
  native_position_stop_read_and_repair_port: UNVERIFIED
assisted_enabled: false
~~~

The installation task reads the actual packaged source/API, captures redacted outbound payloads, and runs §1.8. Do not silently downgrade to a different release or enable a feature based on “latest” documentation. If rc5 cannot satisfy a contract, patch its adapter behind the same port or explicitly select another pin and rerun the matrix. That is bounded implementation work, not a reason to invent successful verification.

### 1.2 Authority and the narrow native port

| Owner | Owns | Must not own |
|---|---|---|
| Bybit | External account, orders, matching, positions, native conditional protection, actual cash transfers | ATLAS approval, desired risk or experiment validity |
| Nautilus execution engine and its Bybit client | All ordinary submit/amend/cancel/reduce commands, order/fill event processing, runtime cache, position bookkeeping, standard reconciliation | Research approval policy or an independent desired-capital ledger |
| ATLAS execution coordinator inside crypto-live | Durable intentions, single-use approvals, risk reservations, lifecycle projection, protection verification, recovery decisions | A competing ordinary order transport or fabricated exchange acknowledgements |
| BybitProtectionPort inside that same writer | Set/repair full-position stop; inspect raw position/conditional protection fields omitted by normalized reports | Entry submission, discretionary exit orders, ordinary cancel/replace, its own position book |
| Economic collector | Funding, fees, transfers and account-cash reconciliation | Mutating exchange positions |
| ops/worker processes | Alerts, research, signed user commands, immutable snapshot jobs | Direct exchange credentials or bypassing crypto-live authorization |

**A. Entirely through Nautilus:** entry and explicit-quantity reduce-only exit orders; ordinary order amendments/cancellations; order identity mapping; fill ingestion; normal order/position reconciliation; strategy timers and event routing. ATLAS journals intent before invoking these commands, but does not send a duplicate HTTP order alongside them.

**B. Native gap:** reserve a small typed port for Bybit position-level stop repair and raw protection inspection. Absence of a public Nautilus position-stop operation in the accessible guide is not proof that no lower-level method exists in the pin. During installation bind this port to a suitable exposed Nautilus HTTP method if verified; otherwise implement the few required V5 calls in the same process. **Default fallback is direct signed REST using the locked HTTP stack; pybit is optional, not a mandatory second client framework.** No ordinary trading endpoints are exposed through this fallback.

The port has exactly these domain methods:

~~~text
read_protection(account, instrument, position_idx=0) -> ProtectionObservation
ensure_full_stop(position_epoch, expected_signed_qty, stop_price, trigger_basis)
read_economic_events(cursor, overlap_start) -> immutable cash events
~~~

Read-only reconciliation supplements may fetch raw order/position/history information, but pass reconciled reports through the Nautilus boundary or retain them as evidence. Never directly mutate its cache from another thread. If an externally generated stop fill cannot be ingested correctly, that is a failed acceptance gate.

**FACT:** Bybit's position trading-stop endpoint creates internal conditional orders, adjusts their quantity with position size and cancels them when the position closes. Full-position mode uses market execution. This is the venue mechanism chosen here, subject to account qualification. [Set Trading Stop](https://bybit-exchange.github.io/docs/v5/position/trading-stop)

### 1.3 Exact protection and order policy

**C. FREEZE:** attach a full-position SL to the entry request; verify it after any fill; use position-level repair if missing or wrong. Do not rely on a local stop emulator or create the first protective order only after observing the fill.

Required logical entry payload, translated through the pinned Nautilus adapter:

~~~yaml
instrument: BTCUSDT-LINEAR.BYBIT   # ETH equivalent
order_type: LIMIT
time_in_force: IOC
side: approved_side
quantity: approved_rounded_base_quantity
price: approved_collar
reduce_only: false
params:
  position_idx: 0
  stop_loss: approved_absolute_stop
  sl_trigger_by: MarkPrice
  sl_order_type: Market
  tpsl_mode: Full
~~~

These are contract fields to verify on the wire, not a claim that an untested constructor signature is executable code. No TP in the first strategy; do not send zero-valued TP fields that might remove existing protection. A future TP policy must get a new strategy/capability version; it may use the same full-position native mechanism after race tests. Separate conditional reduce-only stops are a future alternative only if explicitly qualified, not an automatic fallback that silently introduces a fill-to-stop window.

The chosen stop price is absolute and persists across partial fills; do not loosen it to accommodate average entry price. Full-position protection must cover the current net position, not merely the last fill. On any additional fill, the last protection observation becomes stale until coverage is confirmed. A quantity field that briefly lags may reflect venue propagation, but cannot count as proof of current coverage.

**DEFAULT deadlines:** begin inspection immediately after a fill; if protection is unconfirmed after 2 seconds, cancel any remaining entry, attempt stop repair and issue a reduce-only flatten through Nautilus. Continue observing until flat or protected. Two seconds is an engineering alarm/action threshold, not a guarantee of bounded loss. If connectivity prevents action, remain RECOVERY_REQUIRED and alert; never claim an exit happened.

A native stop acknowledgement is not protection proof. Proof records account, instrument, position epoch, observed signed quantity, full-position semantics, stop price, trigger basis, closing-only behavior, observation time and raw evidence references. Cross-check the position stop and conditional-order view where available. Unknown, absent or conflicting evidence means unconfirmed protection.

**Exit:** cancel outstanding opening orders, retaining native protection. Submit the opposite side with an explicit quantity no greater than the freshest reconciled position and **reduce_only=true**. Do not use a generic “close all” parameter or zero quantity. The first time exit uses an IOC limit with the configured exit collar; if still open, refresh quantity and retry under the bounded exit policy. After two seconds unresolved, request an explicit-quantity reduce-only market exit; partial/no execution remains possible. All attempts are persisted and observed, never assumed successful.

**FACT:** Bybit accepts asynchronous order requests, supports unique client IDs up to 36 characters, and requires closing orders to use reduce-only. Its market orders can remain unfilled/cancel when executable liquidity is unavailable within exchange controls. An acknowledgement is not a fill guarantee. [Place Order](https://bybit-exchange.github.io/docs/v5/order/create-order)

**Amend/cancel/replace:** v1 entry IOC is never repriced or replaced after a no-fill within the same decision epoch. General amendment support is still tested: persist the expected old version and proposed new version; pending amendment does not release old risk. A replacement requires terminal confirmation of the old entry, reconciliation of any late fill and a fresh reservation. Stop repair updates desired full-position protection without first removing the old stop. If that cannot be done on the qualified profile, flatten rather than intentionally creating a protection gap.

### 1.4 Durable records and concurrency

Use SQLite WAL with a single writer and FULL synchronous commits for intent/control records. The local journal is an intent, reservation and evidence ledger; Nautilus remains the operational order engine. The venue remains external reality. A local transaction cannot be atomic with an exchange request.

Minimum durable records:

~~~text
TradePlan(plan_id, version, policy_hash, snapshot_hash, expires_at,
          side, qty_limit, collar, stop, horizon_end, risk_config_hash)
Approval(approval_id, user_identity, plan_id, version, approved_at, consumed_at)
Intent(intent_id, position_epoch, plan_version, client_order_id,
       writer_epoch, lifecycle, protection_status, reconciliation_health)
Command(command_id, intent_id, type, exact_payload_hash, payload,
        expected_state_version, created_at, send_started_at,
        outcome={UNSENT,UNKNOWN,DEFINITE_ACCEPT,DEFINITE_REJECT,RECONCILED})
Reservation(id, intent_id, remaining_open_qty, budget_vector, version)
Observation(id, source, venue_identity, source_time, receive_time,
            raw_hash, request_id, query_interval, completeness)
ProtectionObservation(position_epoch, desired_stop_version, qty,
                      trigger_basis, stop_price, semantics, evidence_ids)
EconomicEvent(account, venue_transaction_id, currency, amount,
              effective_time, received_at, type, revision)
~~~

Persist command and reservation in the same transaction before network side effects; mark dispatch started before the call. A crash between that commit and a response produces UNKNOWN, even if no bytes actually reached the venue. Exactly-once network delivery is not assumed. Persist accepted observations and deduplication keys before advancing the local projection.

Client-order-ID: 32 lowercase hexadecimal characters derived from a persisted random 128-bit UUID, unique across account lifetime; unique database constraint. Never generate a new entry ID merely because the first call timed out. A logical retry references the same intent and command; venue duplicate-ID rejection is a lookup trigger, not evidence of a second order or successful idempotent replay.

Use exchange execution IDs to deduplicate fills; order-status messages are not fills. Keep cumulative quantities monotone unless an explicit correction/void protocol applies. Store out-of-order observations without allowing stale status to undo newer economic facts. Corrections append compensating ledger events. **FACT:** Bybit explicitly documents duplicate Filled statuses during cancel/fill races. [Private order stream](https://bybit-exchange.github.io/docs/v5/websocket/private/order)

**Single writer:** one active credential-bearing host; no active-active failover. A local lock alone cannot fence another host. Replacement activation requires the old host stopped and its credential revoked or otherwise externally fenced. A writer-epoch value in ATLAS does not cause Bybit to reject an old writer. Manual account intervention changes reconciliation health and freezes new risk.

### 1.5 Durable state transition table

Represent protection separately as NONE / UNCONFIRMED / CONFIRMED / BREACHED, and reconciliation health as CURRENT / STALE / CONFLICTED. Store a monotonically increasing local state version. PARTIALLY_FILLED can therefore be protected or unprotected, and CANCEL_PENDING can still hold exposure. The names OPEN_PROTECTED and OPEN_UNPROTECTED below are useful projected states, not an alternative order book.

**Global rules:** risk-reducing commands remain permitted when new risk is disabled, subject to verified closing semantics. Unexpected fills from any state enter exposure reconciliation immediately. Contradictory account observations, unknown ownership or persistence failure route to RECOVERY_REQUIRED. Table restrictions concern this instrument; account-level health/risk gates may also block the other instrument.

| State | Authoritative observation / meaning | Allowed next states | Forbidden new risk | Recovery action | Persist before external side effect |
|---|---|---|---|---|---|
| PLAN_APPROVED | Valid single-use approval of an immutable plan; no exchange effect yet | INTENT_PERSISTED; CLOSED if expired/rejected locally | Any order without current checks and reservation | Revalidate expiry, equity, feed/account state, collar, stop and policy version | Approval consumption, intent, client ID and reservation atomically |
| INTENT_PERSISTED | Durable intent/reservation; no proven dispatch | SUBMITTING; CLOSED only if provably unsent | A second intent or unreserved quantity | Inspect command journal; if dispatch may have begun, treat as UNKNOWN | Exact payload, expected version and SUBMITTING before send |
| SUBMITTING | Dispatch recorded; response/venue state not yet resolved | ENTRY_WORKING; PARTIALLY_FILLED; OPEN_UNPROTECTED/PROTECTED; SUBMIT_UNKNOWN; FLAT_PENDING_RECONCILIATION after definite rejection | Retry with new ID; release reservation on timeout | Correlate response, private reports and identity queries | Response/evidence and any follow-up command |
| SUBMIT_UNKNOWN | Acceptance or fill cannot be excluded | ENTRY_WORKING; PARTIALLY_FILLED; OPEN states; FLAT_PENDING_RECONCILIATION with sufficient terminal evidence; RECOVERY_REQUIRED | New entries, assumed flat state, speculative duplicate submit | Query ID, order history, fills, positions and protection; retain full possible risk | Query evidence, uncertainty bounds, recovery/cancel intent |
| ENTRY_WORKING | Venue confirms opening order; no confirmed fill | PARTIALLY_FILLED; OPEN states; CANCEL_PENDING; FLAT_PENDING_RECONCILIATION | More entries or larger amendments | IOC remaining open beyond normal latency is an anomaly; request cancel/reconcile | Cancel reason, order identity, current cumulative fill and reserved leaves |
| PARTIALLY_FILLED | Unique executions or reconciled cumulative fills establish nonzero position and unfinished entry lifecycle | OPEN states; CANCEL_PENDING; EXIT_PENDING; FLAT_PENDING_RECONCILIATION | Pyramiding; release of unconfirmed remaining quantity | Verify full-position stop; cancel leaves if protection unconfirmed; reduce if deadline breached | Fill identities, current/possible exposure, protection desired version, command |
| OPEN_UNPROTECTED | Exposure known; current protection absent, stale or wrong | OPEN_PROTECTED; EXIT_PENDING; RECOVERY_REQUIRED | All new risk account-wide | Inspect/repair stop immediately; cancel entries; invoke flatten deadline | Desired stop and repair version, repair outcome, exit intent if needed |
| OPEN_PROTECTED | Reconciled exposure plus current protection evidence | EXIT_PENDING; OPEN_UNPROTECTED; FLAT_PENDING_RECONCILIATION; RECOVERY_REQUIRED | Adds, scale-ins, immediate reversal | Monitor mark/margin, native stop and time exit; refresh stale evidence | Any protection amendment or exit command and relevant observation version |
| CANCEL_PENDING | Cancel dispatched; original order may still fill | ENTRY_WORKING on definite cancel rejection; PARTIALLY_FILLED; OPEN states; EXIT_PENDING; FLAT_PENDING_RECONCILIATION | Replacement/addition or treating leaves as canceled | Reconcile terminal status and late executions; keep risk until resolved | Cancel command, dispatch marker, terminal evidence before release |
| EXIT_PENDING | One or more closing-only commands may execute; native stop retained | OPEN states after failed close; FLAT_PENDING_RECONCILIATION; RECOVERY_REQUIRED | New entry/reversal; ordinary opposite-side order | Refresh position and exit outcomes; serialize retries; keep protection | Exit command ID, reduce-only flag, position epoch, quantity evidence |
| FLAT_PENDING_RECONCILIATION | A report suggests zero position; residual orders/late fills/cash may remain | CLOSED; OPEN states on late fill; RECOVERY_REQUIRED | Any new entry, including opposite direction | Confirm zero position, terminal opening commands, no remaining opening/conditional orders, deduped fills and cash cursor | Cleanup commands, query completeness and final release transaction |
| CLOSED | Flat reconciliation certificate; no unresolved opening commands or residual protection for epoch | A new PLAN_APPROVED at a later eligible epoch; RECOVERY_REQUIRED on contrary evidence | Reuse of approval/client ID; reopening old epoch | Late contradiction opens a recovery incident, never silently edits finality | New epoch/approval/reservation before any new order |
| RECOVERY_REQUIRED | Incomplete/conflicting account state or journal health | Any fact-supported state after reconciliation; never blindly ACTIVE | All account new risk | Fence writer, restore durable records, query venue, verify/repair protection, reduce exposed risk when authorized | Incident, raw evidence, recovery commands and signed resolution record |

The final CLOSED record may retain an economic-close reconciliation task when a delayed funding posting is expected, but **risk reservation release requires execution flatness and terminal opening-order certainty**. Economic corrections cannot reopen trading automatically or erase past incident evidence.

### 1.6 UNKNOWN, restart and protection reconciliation

Use this deterministic recovery order:

1. Fence writers and enter new-risk-disabled mode. Restore plans, command outbox, approvals, reservations and last economic cursor.
2. Establish public/private sessions, buffering private events while capturing REST query windows. Validate account identity, product, one-way mode, isolated margin and leverage configuration. Do not automatically change an occupied account's mode on connect.
3. Query every unresolved client/order identity, all open ordinary/conditional orders, bounded order history, execution history, positions, wallet and protection. Paginate to completion and record retention/coverage.
4. Merge buffered private events and REST observations by exchange identity, cumulative execution and known sequence semantics. REST responses across endpoints are not an atomic snapshot; conflicting or changing snapshots trigger another pass.
5. Reconstruct exposure inside Nautilus using its reports, preserving exchange-generated/native-stop executions. Unknown external positions are quarantined; do not attribute them to a strategy merely because the symbol matches.
6. Repair missing protection on owned exposure or reduce/flatten it. Retain stops while canceling opening orders.
7. Reconcile cash separately, including signed funding and fees. Release reservations only on terminal evidence. Resume new risk only when account, protection, journal and data health all pass.

**Negative lookup is insufficient:** recent-order endpoints have limited retained closed records and may delay delivery; supplement with history and executions. [Open and closed orders](https://bybit-exchange.github.io/docs/v5/order/open-order)

An UNKNOWN submit never becomes “not submitted” just because repeated queries are empty. Definitive local unsent proof, a correlated venue rejection with no competing attempt, or authoritative terminal/history resolution is needed. Otherwise quarantine the instrument/account indefinitely and escalate for investigation. A time limit triggers an incident, not permission to re-enter.

**Nautilus integration:** standard startup and in-flight reconciliation should be enabled and its report coverage retained. Quantity repair does not establish complete cost history. [Execution reconciliation](https://nautilustrader.io/docs/latest/concepts/execution/reconciliation/)

**Economic integration:** transaction logs supplement fills for funding and other balance adjustments; paginate with overlap and deduplicate by account and venue transaction identity. [Bybit transaction log](https://bybit-exchange.github.io/docs/v5/account/transaction-log)

### 1.7 No-reversal invariant

For an original signed position \(q\), every stop, time exit, emergency exit and retry must be **venue-enforced closing-only** for positionIdx 0. For each closing execution \(\Delta q\):

\[
q\Delta q\le0,\qquad |\Delta q|\le|q|,\qquad
q+\Delta q=0\ \text{or}\ \operatorname{sign}(q+\Delta q)=\operatorname{sign}(q).
\]

This is a contract on actual matching, not merely a local quantity check. Stop and manual close may race; the exchange must clamp/reject the surplus rather than reverse. Native position stops must demonstrate the corresponding closing semantics in tests.

Also prevent the distinct **late-entry reopening race**: cancel/resolve all opening orders, keep the epoch locked, and prohibit a fresh epoch until CLOSED. Reduce-only exits cannot prevent an unresolved opening order from filling later. A new short is a new approved entry after verified flatness, never an oversized sell used to both close a long and reverse.

### 1.8 Minimum qualification matrix before assisted execution

Run deterministic transport/fault fixtures and Bybit testnet integration; use mainnet public shadow for market realism. Testnet success proves only the tested environment. Actual assisted-account profile verification remains necessary. No test result is asserted here.

| Test | Fault / action | Required evidence |
|---|---|---|
| 1. Identity and mode | Wrong environment/account; hedge/cross mode; duplicate writer | New risk denied before submit; no automatic occupied-account mode mutation |
| 2. Wire contract | Both symbols, both sides, min lot/tick, IOC and attached mark-market full SL | Exact outbound fields and venue acceptance; no silent unsupported-param drop |
| 3. Crash before send | Kill after journal commit, before transport | UNKNOWN/unsent handling; one client ID; no duplicate opening order |
| 4. Lost submit response | Drop acknowledgement after venue accepts, including immediate fill | Resolve original ID; reservations retained; no second entry |
| 5. Definite reject | Invalid lot, expired request, insufficient margin | Definite rejection distinguished from transport uncertainty; no premature release |
| 6. Partial fills | Several fills; IOC remainder cancels; process killed after first fill | Native stop covers aggregate net size without client follow-up; recovery verifies it |
| 7. Kill after full fill | Kill before local fill persistence | Exchange stop remains active; replay rebuilds quantity and costs once |
| 8. Duplicates/reordering | Replay execution/status; Filled after cancel; stale New after Filled | No double P&L, no status regression, no lost late fill |
| 9. Native-stop repair | Delete/change stop in sandbox; drop repair response | Unprotected deadline activates; set-state repair read-back; no duplicate stop authority |
| 10. Stop/close race | Trigger stop while reduce-only close is matching; exit partial/rejected | Position never reverses; remaining exposure stays tracked/protected |
| 11. Late-entry race | Entry fills while cancel/flatten begins | No premature CLOSED; late exposure protected/reduced; next epoch blocked |
| 12. Private disconnect | Fill and stop while private feed absent | New risk blocked; REST and buffered events converge without double counting |
| 13. Incomplete REST | Pagination, stale snapshot, recent-history reset, 429/5xx | Incomplete != empty; UNKNOWN persists; no inferred flatness |
| 14. Residual orders | Zero-position report while entry/conditional remains | FLAT_PENDING_RECONCILIATION; targeted cleanup; no broad stop removal while exposed |
| 15. Amendment race | Amend/cancel/fill concurrent; lost amend response | Reservation is max feasible old/new exposure; no unconfirmed replacement |
| 16. Exit failure | Collar no-fill, market partial, venue unavailable | Still EXIT_PENDING/RECOVERY_REQUIRED; no invented fill or guaranteed stop price |
| 17. Storage/clock failure | Disk full, fsync failure, corrupted tail, clock jump | No new risk; existing native protection survives; preauthorized reduction path audited |
| 18. Cash and external events | Funding, fees, manual trade, external/native SL fill | Quantity and economic reconciliation; ownership conflict freezes new risk |
| 19. Approval replay | Expired/duplicate approval, price drift, risk change | Atomic single use; revalidation; no submitted order from stale plan |
| 20. Restore/fencing | Restore backup with unresolved command; old host still alive | Old credentials fenced before replacement writer; venue reconciled before activation |

Each case must assert the invariant, expected durable states, risk-reservation balance and bounded duplication behavior. A successful HTTP status is never the sole assertion.

## 2. One exact first strategy: CRYPTO_TREND_24H_V1

### 2.1 Signal and timing

**FREEZE:** BTCUSDT and ETHUSDT only; four-hour entry decisions at 00:00, 04:00, 08:00, 12:00, 16:00 and 20:00 UTC. Inputs are closed one-hour bars. No intrabar signal entry, pyramiding, cross-symbol ranking requirement or immediate reversal. At most one position epoch per instrument.

Let \(C_t\) be the last-trade close of the one-hour bar ending at UTC hour \(t\), and \(r_t=\log(C_t/C_{t-1})\). Missing/nonpositive/duplicate conflicting closes invalidate the feature window; do not forward-fill returns.

Use a fixed 30-day window: **721 consecutive hourly closes**, yielding 720 returns. For each calculation, seed variance with the mean squared first 48 returns in that window; then recurse through the remaining returns:

\[
\lambda_v=2^{-1/48},\qquad
v_j=\lambda_vv_{j-1}+(1-\lambda_v)r_j^2,\qquad
\sigma_t=\sqrt{\max(v_t,10^{-8})}.
\]

This finite-window reseeding rule makes batch and streaming implementations identical; an indefinitely seeded EWMA is a different definition. Maintain a buffer or equivalent exact rolling computation. The floor is 1 basis point of hourly volatility, not an estimate of real risk.

\[
z_t=\frac{\log(C_t/C_{t-24})}{\sqrt{24}\,\sigma_t},\qquad
d_t=
\begin{cases}
1 & z_t>0.5,\\
-1 & z_t<-0.5,\\
0 & \text{otherwise}.
\end{cases}
\]

Equality is flat. There is no extra moving-average crossover, RSI filter or fitted regime label. The purpose is a reproducible momentum benchmark.

At a scheduled slot \(T\), wait for all required bars and the feature computation to become available. **DEFAULT:** snapshot deadline \(T+30\) seconds, plan expiry \(T+60\) seconds. Missing the deadline skips that slot; it does not permit backdating the decision. Human approval after expiry is rejected. Recompute current execution/risk gates at approval, without changing the already approved signal/stop recipe. The next opportunity is the next scheduled slot.

### 2.2 Entry, stop and exits

| Component | Exact v1 choice |
|---|---|
| Eligibility | Signal nonzero; feature/data/account health pass; flat-and-reconciled instrument; no live or UNKNOWN opening intent; strategy admission and current risk gates pass |
| Entry | One marketable IOC limit. No passive queue model, chase, averaging down or same-slot replacement |
| Collar | Let fresh bid/ask midpoint be \(m\), best ask \(a\), bid \(b\), and \(c=0.001\). Buy maximum \(a(1+c)\), rounded **down** to tick; sell minimum \(b(1-c)\), rounded **up**. Quantity rounds down to lot |
| TTL | IOC is venue immediate-or-cancel. Local 2-second unresolved-order deadline starts reconciliation/cancel; it does not mean the exchange has canceled |
| Stop distance | \(h=2\sigma_t\sqrt{24}\). Reject if \(h<0.0025\) or \(h>0.10\); never clamp the distance to force eligibility |
| Stop reference | Fresh mark \(M\) at plan creation. Long \(S=M\exp(-h)\), rounded **up** to tick; short \(S=M\exp(h)\), rounded **down**. Reject a rounded stop on the wrong side of current mark |
| Stop semantics | Absolute fixed price, MarkPrice trigger, native full-position market SL attached on entry; no trailing or break-even reset |
| Stop validity at send | Stop remains on protective side; mark-to-last dislocation within gate; worst entry within approved collar and risk budget. Otherwise expire plan |
| Time exit | At \(T+24\) hours, regardless of signal. Actual hold is at most 24 hours because entry occurs after \(T\). Cancel unresolved entry leaves; issue closing-only exit policy in §1.3 |
| Initial time-exit collar | 25 bps beyond current opposite best quote, rounded without worsening collar. Two-second escalation described in §1.3; failures retain exposure state |
| Profit taking | None. Adding TP changes payoff shape and path-order ambiguity without being necessary for the first benchmark |
| Signal change while open | Log it; do not close/reverse, refresh horizon, add size or move stop. Stop/time/risk exits are the whole declared v1 management policy |
| Re-entry | Only at a later four-hour slot after CLOSED; never within the slot that produced the preceding close |

Use decimal arithmetic for price/quantity/money, UTC integer timestamps, and documented rounding for every exchange field. Floating-point analytics are permissible; quantization precedes final risk acceptance.

### 2.3 Gates, costs and management

**DEFAULT operational inputs:** latest usable quote and mark no more than 1 second old; public/private transport health current; book synchronized; latest scheduled funding state no more than 60 seconds old; complete current instrument filters. Source timestamp skew must also be checked—recent receipt of stale data is not fresh.

**DEFAULT market gates:** spread \((a-b)/m\le0.0005\); modeled entry impact above opposite best quote \(\le0.0005\); \(|\log(M/P_{\rm last})|\le0.002\); quantity no more than 10% of displayed opposite-side depth inside the collar. These are initial engineering limits, not measured optimums. Snapshot depth does not guarantee that quantity fills. Missing depth means no executable proposal; phase-4 OHLC-only studies are explicitly diagnostic.

**Funding:** include every settlement while the position is open, with the schedule and contract terms known at the decision. Positive signed rate costs longs and benefits shorts. Do not subtract an eight-hour average rate once per trade. For the *planned-loss gate*, reserve adverse funding only; never finance stop risk with hoped-for funding receipts. Default cumulative adverse-funding budget is at most 25% of the price-distance stop budget. Expected P&L uses signed funding. Unknown future rates require a declared forecast distribution, not realized future rates as inputs.

**Event gate:** block new entries from 30 minutes before until 15 minutes after a scheduled US CPI, US payroll or FOMC rate decision whose schedule was available at the decision; block during a known venue maintenance interval and unresolved venue/asset operational incident. Store the exact event classification and calendar revision. No general news-sentiment trade signal. An empty/unavailable required event feed is not proof of no event; fail the entry gate. Retrospective tests without reconstructable calendars must report a separate gate-disabled diagnostic variant, not pretend to test the deployed policy.

**Drawdown:** the risk policy may halve new-trade budgets, stop new entries or request controlled reductions. It cannot increase size, widen a stop or override stale account state. Existing positions retain native stops when a new-entry gate fails. New market news alone does not produce an undocumented exit.

**Management cadence:** exchange-event-driven fill/protection handling; one-second local protection/health/time-deadline checks; ten-second REST verification while exposed, rate-budget permitting; immediate reconciliation on anomaly. Refresh signals hourly for logging and at four-hour slots for eligibility, without changing existing trade management. If time exit is delayed by outage, record the true extended exposure and costs.

### 2.4 Reproducible decision pseudocode and parameter classes

~~~text
on four_hour_slot(T):
    X = build_snapshot_of_bar_ending_T_when_available(deadline=T+30s)
    if absent(X): record SKIP_DATA; return
    d = +1 if z(X)>0.5 else -1 if z(X)<-0.5 else 0
    if d==0 or not instrument_closed_and_reconciled(): record NO_TRADE; return
    if not entry_gates(X, latest_execution_state): record reason; return
    policy = fixed_stop_IOC_time_exit(d, X, horizon_end=T+24h)
    result = evaluate_actions_and_portfolio(policy, X)  # section 3
    if result is NOT_ESTIMABLE or result.choice==FLAT: record reason; return
    persist immutable TradePlan(expiry=T+60s)
    alert or await permitted execution mode

on approval(plan):
    revalidate same plan, current account/quotes/protection and joint budgets
    atomically consume approval and reserve risk
    persist command; send through Nautilus
~~~

| Class | Members | Change policy |
|---|---|---|
| Frozen architecture | One-way ownership; UTC causal snapshots; fixed stop/time policy; IOC entries; native mark protection; no TP/adds; no model votes; all outcomes recorded | Contract/policy version and complete replay comparison |
| Fixed benchmark defaults | 4-hour schedule; 24-hour momentum and hold; 48-hour EWMA half-life; 720-return window; 0.5 signal band; 2-volatility stop | Not optimized for baseline v1. Changing one creates a challenger, with an experiment entry |
| Training-selected | Statistical shrinkage; residual block length; conditional execution/funding estimates; calibration and uncertainty estimates | Nested chronological training only; locked before evaluation |
| Account/operating policy | Capital budgets, leverage, collateral, DD thresholds, freshness/collars/deadlines, event windows, user-approved live mode | Versioned configuration; tighter runtime changes allowed under policy; loosening requires approval and re-evaluation |

For attribution, retain **B0**, the raw fixed trend policy with identical execution/risk constraints but no statistical alpha veto, as a shadow diagnostic; **A0**, the same policy with the §3 LCB/portfolio gate, is the first decision-system baseline. A foundation challenger is compared to A0. Report A0 minus B0 separately so a statistical filter's contribution is not mislabeled as foundation-model value.

## 3. Minimum action-conditioned scenario and P&L engine

### 3.1 Model and chronological residual archive

**FREEZE:** one-hour conditional mean, the same EWMA volatility as §2, joint BTC/ETH residual blocks, empirical one-minute path shapes, and the exact IOC/stop/time policy. No independent draws from horizon quantiles.

At each hour, forecast

\[
y_{i,t+1}=\frac{r_{i,t+1}}{\sigma_{i,t}},\qquad
\mu_{i,t}=\beta_0+\beta_E\,1_{\{i=ETH\}}+\beta_z\,\widetilde z_{i,t}.
\]

The baseline has one price-feature group; grouped shrinkage in the prior review reduces here to ridge shrinkage of this small Huber model. No derivatives, fundamentals or model forecasts enter v1's mean; their archived features remain available to later grouped challengers.

Minimize mean Huber loss with transition 1.345 plus \(\lambda_2(\beta_E^2+\beta_z^2)/2\). Do not penalize the intercept. Standardize \(z\) using training-only pooled mean/standard deviation; reject a zero-variance feature or set its coefficient to zero. Use \(\lambda_2\in\{0.01,0.1,1,10\}\), selecting minimum mean validation Huber loss; ties within \(10^{-8}\) select the larger penalty. Solver tolerances and numeric-library versions enter the model manifest.

**DEFAULT refresh:** fit each Monday 00:00 UTC from the last 180 days of matured one-hour labels; minimum 90 days. Select the penalty with three nonoverlapping seven-day validation windows at the end of that training interval, each trained only on its preceding data with at least 60 days. Never include the label crossing a fold boundary. Freeze the fitted coefficients for the next seven days. This is an algorithmic update rule; it may run prospectively without discretionary retuning.

Archive each genuine chronological forecast, then, only after its target becomes available, compute

\[
\epsilon_{i,t+1}=y_{i,t+1}-\mu^{OOF}_{i,t}.
\]

OOF means the complete fit, scaler and penalty selection used only earlier matured labels. Random k-fold is invalid. Do not replace archived OOF forecasts with predictions from today's refit. Keep up to 180 days of synchronized OOF BTC/ETH residuals; require 60 days for statistical evaluation, and more if uncertainty/support checks fail. The signal's 30-day warmup alone is not sufficient to claim a fitted scenario model.

### 3.2 Joint blocks and one-minute paths

For each residual hour store a joint record containing:

~~~text
BTC and ETH: OOF residual, forecast, pre-hour sigma and features
60 one-minute last/mark/index OHLC records, including opening gaps
available spread/depth/latency/fill information and missingness
funding observations with actual publication and settlement times
source/availability class, calendar and universe identifiers
~~~

**DEFAULT block length:** 24 consecutive hours. During training select \(L\in\{24,48,72\}\) using the same chronological validation windows and the mean energy score of joint 4-hour and 24-hour cumulative returns, standardized by training volatility. Equal weights for instruments and horizons; ties select the longer block. Barrier-hit/cost diagnostics are acceptance checks, not an additional unrecorded optimization objective. Reject block choices that produce insufficient effectively independent history.

Sample non-circular contiguous blocks uniformly from eligible **joint** BTC/ETH archive start times. Stop at real data gaps; never wrap the final observation to the first or sample each coin separately. To exceed a block, sample the next block and concatenate; preserve relative clock/funding metadata under the replay rules below. Every candidate at a decision uses common random numbers and identical sampled blocks.

**DEFAULT:** 2,048 joint paths, 24-hour risk horizon, one-minute replay grid. Fit the mean only hourly. The one-minute data supply empirical within-hour variation rather than an additional learned microstructure model.

An exact bridge for last-trade closes is:

\[
e_{i,h,k}=
\frac{\log(C_{i,h,k}/C_{i,h,k-1})}{\sigma^{archive}_{i,h-1}}
-\frac{\mu^{OOF}_{i,h-1}}{60},
\quad k=1,\ldots,60.
\]

At a simulated hour with current \(\sigma^*,\mu^*\):

\[
r^*_{i,k}=\sigma^*_{i}\left(\frac{\mu^*_i}{60}+e_{i,h,k}\right).
\]

The sampled hour's micro-increments sum to the one-hour OOF residual. Recompute momentum and variance from simulated hourly closes using the same finite-window rule; do not use realized future features. Mean coefficients stay frozen throughout the 24-hour path; a path crossing a weekly refresh conservatively uses the current fit because this trade's management does not refit or change its stop.

For each minute also retain normalized log open-to-previous-close gaps and high/low excursions beyond its open/close envelope. Apply the same \(\sigma^*/\sigma^{archive}\) scale to these excursions, with drift distributed over the minute close return. Construct highs/lows around the resulting open/close envelope so OHLC inequalities always hold. Mark and index use their own synchronized recorded returns/excursions, subtracting the same archived conditional drift and restoring the simulated drift; retain the sampled relative-basis changes. Anchor all three initial prices to the actual snapshot.

This is an explicit short-horizon empirical approximation to joint price/basis paths. It does not establish sub-minute ordering or make sampled regimes stationary. Excess basis drift, unsupported current volatility or poor barrier calibration yields NOT_ESTIMABLE. Do not repair such failures by silently clipping losses.

The first simulated minute begins at the actual proposal/approval send time, not at bar close. For minute-bar-only historical records, use the first complete minute after that time, lose the intervening opportunity, and apply the declared conservative entry bound. Forward event replay uses actual timestamps.

### 3.3 Execution, costs and stop replay

**Statistical scenarios** estimate outcomes under a stated empirical model. **Deterministic stresses** in §3.5 enforce constraints without a probability or contribution to the estimated mean.

| Mechanism | v1 replay rule |
|---|---|
| Entry arrival | Use measured/assumed decision-to-venue latency. Include human delay for assisted-policy evaluation. Price before arrival is not fillable |
| IOC entry | Match only against opposite-side liquidity at arrival inside the fixed collar. Fill up to available modeled quantity; cancel remainder. No fill is an outcome with zero trade P&L, not a discarded sample |
| Partial fill | Retain actual filled quantity, weighted entry price and corresponding fees; stop covers this quantity. No simulated later chase |
| Spread | With joint quote records, resample relative spread paired with the return block and scale to current spread by a training-estimated rule. Default scale is current spread divided by training median spread; positive floor one tick |
| Depth/impact | Use recorded ladder shape normalized to current opposite-side depth. At arrival apply sampled observed depth change and consume levels; never exceed collar or participation cap. Calibrate residual impact/latency against available shadow or actual fills |
| Thin historical execution data | Use a predeclared conservative executable-price/depth scenario envelope. Output bounds/diagnostic status; do not invent a precise fill probability from candle volume |
| Adverse selection | Price path, arrival, spread and available depth share the same historical block. Do not draw fills independently of subsequent returns |
| Passive limit orders | Disabled. If a future candidate is passive, v1 returns UNSUPPORTED_POLICY rather than treating a touched limit as filled. A later queue/trade-through model needs separate qualification |
| Stop trigger | First mark event crossing S; with one-minute mark bars, crossing occurs if long mark-low ≤ S or short mark-high ≥ S. Entry must already have filled |
| Stop execution | Executable opposite-side price after trigger plus measured/assumed latency, depth and exchange controls. Never automatically fill at S |
| Missing intraminute order | Evaluate adverse/favorable bounds. For the conservative long stop bound use the lowest available last-trade price in the trigger minute, less half-spread/impact; reverse for short. This is a bound, not an unbiased expected fill |
| Gap through stop | Trigger immediately when mark reappears beyond S; execution uses post-gap executable price. Include the full gap loss |
| Target and stop in same bar | No target in v1. Generic replay reports both orderings if a later target policy is loaded and finer ordering is absent; no optimistic choice |
| Horizon | Close at T+24h using the declared exit policy; unfilled exits extend exposure. Estimate extension using observed event paths or route to conservative stress/NOT_ESTIMABLE, never zero out residual inventory |
| Fees | Account-specific fee schedule known at decision; assume taker for IOC entry and exits. Charge each fill once |
| Funding | Cash flow at each actual/simulated settlement instant on surviving signed quantity and applicable mark notional; partial quantities matter |

The scalar spread adjustment above is an initial rule that must be checked against forward observations. Do not let a fixed narrow spread survive a simulated liquidity collapse merely because the current market is liquid.

For funding in forecasts: start from the latest observable predicted rate; use synchronized historical rate **changes between settlements**, matched to the contract's interval, for later rates. Use the currently known next-settlement time; future interval changes are stresses unless already announced. If historical predicted-rate snapshots are unavailable, use the latest **settled** rate as the anchor and a historical settlement-change model. This downgrade must be labeled and validated. Realized future funding is allowed for scoring a historical outcome, not for constructing its pre-trade forecast.

For closed linear-contract lots:

\[
Y(a,\omega)=\sum_j d\,q_j(P^{exit}_{j,\omega}-P^{entry}_{j,\omega})
-\sum_f fee_f-\sum_{\tau}d\,q_{\tau}M_{\tau}f_{\tau}.
\]

Price includes spread, impact and latency; do not subtract these again. Funding \(f_\tau>0\) costs a long and credits a short. Flat means **no new trade** and \(Y=0\); it does not close the existing portfolio. Actual emergency reductions remain a separate control path.

### 3.4 Expected-P&L uncertainty and portfolio decision

Two independent numerical quantities must remain separate:

1. Distributional dispersion of simulated trade outcomes.
2. Uncertainty about their conditional mean, parameters, calibration and execution estimates.

The 5th percentile of path P&L is **not** a confidence bound on expected P&L. Simulating more paths supplies no new historical evidence.

**DEFAULT outer bootstrap:** 200 synchronized block resamples of the *past training/evaluation records*, including prices, OOF forecasts, execution/cost observations and scanner outcomes. Refit the mean/scaler with the locked penalty and re-estimate costs for each replicate. Resample the OOF residual/shape archive jointly by timestamp; do not turn in-sample refit errors into the OOF archive. Where fitted residual bias/scale calibration is used, recompute it on chronological OOF data inside the replicate. Preserve the temporal train/validation partitions; blocks cannot move future labels into earlier fitting windows.

For each replicate, evaluate the same current immutable policy with 512 common-random-number paths, giving \(\hat\mu_b(a)\). Take the empirical lower quantile using the sorted order statistic at index \(\lceil B\delta\rceil\), 1-indexed. Default family error budget is 0.05, divided by the two capital-eligible instruments: \(\delta=0.025\). Direction is preselected by the fixed trend rule and quantity by risk, not by maximizing simulated profits. If more action variants are searched, expand the preregistered multiplicity correction.

These bootstrap bounds are approximate, conditional on dependence/stationarity/model adequacy. A block bootstrap does not guarantee conditional coverage after arbitrary selection. Validate bound behavior and the entire frozen gate chronologically; **insufficient support = NOT_ESTIMABLE**, not a narrow bound. Do not call the 200 replicates 200 independent observations.

Fit bootstrap replicas off the execution thread; evaluate a bounded batch at decision time. Use two independent inner random seeds to assess Monte Carlo instability near the threshold. If numerical uncertainty can flip acceptance, increase paths within the fixed compute deadline or return NO_TRADE_NUMERICAL. Expensive computation never delays protection.

With eligible equity \(E\), existing/pending portfolio P&L \(\Pi_0(\omega)\), and candidate \(Y\), use the common 24-hour horizon:

\[
ES_\alpha(L)=\min_v\left[v+\frac{1}{1-\alpha}\,\mathbb E(L-v)_+\right],
\quad L=-\Pi/E,\quad \alpha=0.975,
\]
\[
J(a)=LCB_\delta(\mathbb E[Y(a)])/E
-\lambda_R\{ES_\alpha[-(\Pi_0+Y)/E]-ES_\alpha[-\Pi_0/E]\}.
\]

**DEFAULT \(\lambda_R=1\)** and account-equity materiality floor **\(10^{-5}\)** (0.1 bp). Both are policy parameters, never secretly selected on test results. Require LCB \(>0\), \(J>10^{-5}\), all hard capital/margin/stress limits and adequate model support. Negative standalone speculative alpha cannot qualify simply because it happens to reduce portfolio ES. An explicit hedge strategy is outside v1.

For each symbol the scenario engine can evaluate LONG, SHORT and FLAT, but the trade selector permits only the §2 signal direction or FLAT. Report the forbidden opposite-direction result only as a diagnostic. Quantity is the largest rounded quantity satisfying §7 using declared conservative costs, then recomputed once through all scenario/risk checks; do not search sizes for the highest noisy LCB. If it fails, NO TRADE. BTC is processed before ETH, with reservations updated between them, so independent programmers produce the same allocation.

~~~text
evaluate_actions_and_portfolio(policy, FeatureSnapshot X):
    assert X and all fitted artifacts are causal and versioned
    if operational/account/data gate fails: return NO_TRADE(reason)
    if scenario support is inadequate: return NOT_ESTIMABLE
    Q = risk_size_with_pending_reservations(policy, current_account)
    if Q < venue_minimum: return NO_TRADE(MIN_SIZE)
    paths = generate_joint_paths(X, N=2048, locked_seed)
    for side in [LONG, SHORT, FLAT]:
        action = same_fixed_policy(side, Q) or NO_NEW_TRADE
        Y[side] = replay_IOC_stop_time_policy(action, paths)
        mu_lcb[side] = outer_block_refit_mean_bound(action, X)
        stress[side] = replay_declared_deterministic_stresses(action)
        risk[side] = portfolio_with_existing_and_pending(Y[side], paths)
    a = action_in_trend_direction
    if any UNKNOWN execution bound, bad calibration, failed stress or risk:
        return NO_TRADE(reason)
    if mu_lcb[a] <= 0 or J[a] <= materiality_floor: return NO_TRADE(NO_EDGE)
    atomically recheck and reserve budgets against current reservation version
    return TRADE(a, evidence_hashes, expiry)
~~~

### 3.5 Deterministic stresses and phase-4 limits

**DEFAULT engineering stress suite:** adverse last/mark jump of 5% and 10%; mark/last divergence of 1% in the adverse direction; spread 10× current; displayed depth reduced 90%; no effective exit for 60 seconds and for 15 minutes; simultaneous BTC/ETH adverse shock with correlation one; funding debit 5× the larger of current absolute rate and past-training 99th-percentile absolute rate; maintenance-margin tier increase; 10% USDT collateral-value haircut; loss of all venue collateral. Combine liquidity impairment with each price jump. The collateral-loss case is controlled by the venue-collateral cap, not misrepresented as a per-trade stop loss.

These numbers define reproducible engineering exercises, not real tail probabilities or sufficient worst cases. Store shock timings, paths and valuation currency. Re-evaluate margin along the stressed path, not merely at its endpoint. If the tested shock liquidates the position before the intended stop, record liquidation mechanics/costs and fail the profile if its margin constraint requires survival.

| Acceptable in phase 4 | Invalidates a claimed executable economic result |
|---|---|
| One-hour fitted model with synchronized one-minute empirical paths | Only terminal returns for a stop-based policy |
| Fixed transparent engineering parameters and untuned baseline | Optimizing band/stop/cost assumptions after looking at evaluation P&L |
| Conservative OHLC execution bounds, clearly labeled | Certain fill on limit touch; fills before arrival; filling a gapped stop at S |
| Public reconstructed historical facts under §4 | Invented past receipt times, revised macro masquerading as original, future covariates |
| Sparse historical depth producing diagnostic or interval-valued results | Fabricated precise queue/fill probability from OHLCV |
| Conservative funding forecast with declared missing predicted snapshots | Feeding realized future funding into entry selection |
| Recording NOT_ESTIMABLE and collecting forward data | Increasing path count to conceal insufficient independent observations |
| Bounds robustly below zero implying rejection | Reporting an exact positive mean from adverse/favorable bounds with unknown ordering |

If a positive result depends on unobserved execution ordering, it cannot qualify assisted trading. Conservative negative diagnostics may still reject an idea. Forward event data and execution calibration resolve remaining approximation uncertainty during the build.

## 4. Information time and historical backfill

### 4.1 Canonical schema: never overwrite actual receipt

The six original timestamps/fields are necessary but not sufficient to distinguish a live audit trail from a historical reconstruction. Add explicit provenance and availability intervals now.

| Field | Exact meaning |
|---|---|
| source_event_at | When the measured event occurred. For a bar, retain bar_start and bar_end separately; the completed bar's event time is bar_end |
| source_published_at | When this exact version was disseminated by the source, if evidenced. Nullable; not automatically the measurement date |
| received_at | Actual time this ATLAS installation received this payload, including a historical download received today |
| available_at | Earliest actual time this validated record/version was usable by the live consumer, at or after receipt and required validation |
| processed_at | Completion time of the transformation that created this record. A derived feature may finish much later than its raw inputs |
| source_revision | Source revision ID if supplied; otherwise an ATLAS content/version identity, clearly identified as such |
| availability_class | ACTUAL_OBSERVED / RECONSTRUCTED_PUBLIC / UNKNOWN / REVISED_NO_VINTAGE |
| availability_lower / availability_upper | Evidenced or assumed historical usability interval for a reconstruction; never substitutes for actual received_at |
| replay_available_at | Scenario-specific historical usability time derived from the chosen reconstruction rule; absent if unknowable |
| availability_method / evidence_ref | Receipt log, source publication archive, bar-finalization rule, conservative lag assumption, or missing evidence |
| data_ingested_at / content_hash | Import time and exact immutable content identity |
| revision_of / source_valid_from / recorded_at | Bitemporal version lineage: what period a value describes, when the system learned this version |
| dependency_ids / pipeline_version | Exact inputs and transformation identity for a derived record |

Timestamp units are UTC nanoseconds internally; preserve source precision and clock uncertainty. When source and local clocks disagree, record the conflict and quarantine rather than “fixing” timestamps until they appear causal. For each stage \(available\_at\ge received\_at\) and no earlier than all required dependency availabilities and stage completion. Optional later archival processing does not retroactively delay an already validated live decision input; record that archival processing as a separate stage.

The **source-data archive** stores actual records with today's true receipt times. The **replay view** supplies a separate counterfactual historical availability schedule. A download never manufactures an observation log for a system that did not exist.

### 4.2 Exact treatment by source

| Case | Live/audit rule | Legitimate historical replay treatment |
|---|---|---|
| Live WebSocket event | Actual receive, validation and feature completion; closed-bar confirmation required | Use recorded actual availability for reproducing ATLAS decisions |
| REST gap repair | Available only when repair is received/validated; do not rewrite the decisions that missed it | Separate repaired-market-data study may reconstruct old public availability, clearly distinct from actual-system replay |
| OHLCV downloaded today | received_at and data_ingested_at are today; source publication may be null | Completed public bars can enter RECONSTRUCTED_PUBLIC at bar_end plus a declared lag if bar integrity/public observability are supportable. Not proof of historical subsecond delivery |
| Historical funding settlements | Economic fact at settlement, received today | Use actual settled amount as the realized outcome at settlement; as an input, only after reconstructed publication. Not as a pre-settlement forecast |
| Historical predicted funding | Need snapshots of the predictions available then | Final realized funding cannot substitute; if unavailable, use the separately declared settled-rate anchor model |
| Historical OI | Measurement time is not proof of publication time | Require documented interval semantics plus publication/lag evidence. Otherwise exclude from decision features; keep diagnostic only |
| Revised macro | Each vintage has its own publication/availability | Use the vintage available then; final revised series without vintages cannot be historical decision evidence |
| Edited official announcement | New content hash/revision with edit receipt/publication | Original content needs an archive of that version. Today's edited body cannot be placed at original article timestamp |
| Delayed RSS/news discovery | Live availability is actual discovery and processing | Reconstructed original public availability may support a different delivery assumption, not a replay of ATLAS discovery |
| Confirmed swing pivot | Pivot's economic location is bar t; feature appears only after required right-hand bars close and processing completes | Keep pivot_at=t and confirmed_at=t+k separate; no filling the signal backward over t…t+k−1 |
| Forecast taking 8 seconds | Store input_as_of, started_at, finished_at; available at finish plus validation | Decision at 12:00:05 cannot use a forecast started at 12:00:00 and finished at 12:00:08 |
| Feature pipeline finishing late | Output availability is dependency-ready time plus actual stage completion | Simulate declared latency or replay actual latency; never assign bar-close timestamp to a later computed result |
| Period before ATLAS existed | No actual historical observation claims | Public facts can support counterfactual market replay with reconstruction and latency assumptions; unknown publication stays unknown |

Bybit's REST kline close can be the current last trade for an unfinished candle; its WebSocket kline feed distinguishes confirmed bars. Do not include a finalized historical candle at its opening timestamp. [REST klines](https://bybit-exchange.github.io/docs/v5/market/kline), [WebSocket klines](https://bybit-exchange.github.io/docs/v5/websocket/public/kline)

Funding-history and OI-history endpoints expose different event/interval concepts. Preserve those distinctions; neither endpoint retroactively proves when ATLAS could have consumed the value. [Funding history](https://bybit-exchange.github.io/docs/v5/market/history-fund-rate), [OI history](https://bybit-exchange.github.io/docs/v5/market/open-interest)

FRED's real-time periods support vintage-aware retrieval; the statistical observation date alone is not the release time. If only a date is supplied and no release timestamp can be established, use the end of that source date plus declared delivery delay, rather than inventing an intraday advantage. [FRED real-time periods](https://fred.stlouisfed.org/docs/api/fred/realtime_period.html)

### 4.3 Replay modes and executable causal rule

**ACTUAL_SYSTEM:** use actual available_at and the exact recorded revision, queue delay, compute completion, approvals and account observations. A repair learned on Friday cannot alter a Wednesday decision.

**RECONSTRUCTED_MARKET:** for each record, use historical replay_available_at from an evidence-based source policy. **DEFAULT engineering bar policy:** completed exchange bars at close + 5 seconds, with +30 and +60 seconds sensitivity runs; initial historical feature latency 1 second, plus actual measured model latency where applicable. These delays are assumptions to test, not measured historical observations. If a bound on historical availability is known, use its upper endpoint. If no defensible bound exists, exclude the variable from causal claims.

**DIAGNOSTIC_NONCAUSAL:** permits final revisions or unknown publication for descriptive/integration work, permanently tagged. It cannot feed a promotion metric.

Let \(a_R(x)\) be the effective availability in replay mode R. A decision executed at time \(D\) may use a feature only if:

\[
a_R(x)\le D,\quad
a_R(f)\ge \max_{x\in deps(f)}a_R(x)+latency(f),
\quad model\_artifact\_eligible(f,D,R).
\]

At D, choose the latest **known** version as of D, not today's latest version. The replay queue is ordered by effective availability, stable source ordering where provided, and an explicit deterministic tie-breaker. It never sorts only by source event time. Events that arrive late may update future state but do not rewrite already emitted decisions.

Model-release/pretraining issues are handled by §5. A 2026 model run on a 2023 prefix is a retrospective diagnostic even if every market feature is causal. An algorithm authored today can be a valid counterfactual historical policy; that does not mean it was actually deployed or that repeated researcher selection is absent.

**Answer to the central backfill question:** yes, public historical market facts downloaded today can support a historical backtest. Keep received_at=today, set availability_class=RECONSTRUCTED_PUBLIC, and supply a separate, documented replay_available_at. Do not falsely set received_at to a historical date. Unknown/revised publication cannot be cured by adding an arbitrary lag; it requires evidence, exclusion, or a diagnostic-only label.

### 4.4 Prefix-invariance and revision tests

For every feature implementation, including TA/SMC wrappers:

1. For each cutoff D, run on the availability-filtered prefix. Compare every emitted feature with available_at≤D to the corresponding full replay restricted to D. Values, feature identity, confirmation times and missingness must agree.
2. Append extreme future bars, arbitrary future volumes and a future gap. Previously available outputs must remain unchanged.
3. Test a swing requiring k right-hand bars: no confirmed signal at t…t+k−1; one immutable confirmation after t+k becomes available. Equal-high/low tie rule must be fixed in the feature version.
4. Deliver an old bar through a late REST repair. Actual-system decisions before receipt remain identical; later features explicitly identify the repair dependency.
5. Replace a macro/news value with a later revision. Pre-revision outputs retain the earlier content; post-revision outputs may change with a new feature version/instance.
6. Delay one dependency and one eight-second forecast. No snapshot may include either before completion; scheduled decisions expire rather than travel backward in time.
7. Compare incremental and batch calculations with identical finite history, null handling and tick rounding. A centered window, backward fill or retrospective ZigZag relabeling must fail.

Do not require later revised records to equal earlier versions forever. The invariant is that **what was emitted and available by D stays reproducible**, with later corrections appended rather than silently replacing history.

## 5. Foundation-model qualification under contamination uncertainty

### 5.1 One protocol for every checkpoint

Treat a checkpoint, tokenizer, normalization, inference wrapper, context construction, postprocessor and update schedule as one frozen predictive artifact. Record all hashes, acquisition time, declared corpus/cutoff evidence and whether provenance was independently inspectable. A family name is not a reproducible model.

| Pretraining evidence | Retrospective history can establish | What it cannot establish alone |
|---|---|---|
| Auditable cutoff before evaluation; no evaluation outcomes in any fitting/selection stage | Causal held-out predictive/economic evidence under declared execution assumptions, integration and calibration behavior | Future profitability, actual latency/fills, absence of researcher selection bias |
| Partially known cutoff/corpus | Engineering validity; performance on identifiable uncontaminated subsets; sensitivity and diagnostics | Clean alpha on unidentified overlapping periods |
| Unknown cutoff/corpus | Input/output correctness, speed/memory, failure behavior and historical diagnostic relationships | Clean historical out-of-sample alpha merely because the wrapper used chronological windows |

All foundation challengers require frozen prospective shadow before nonzero decision influence in v1. For known clean cutoffs, historical evidence strengthens the case; it does not remove forward operational qualification. For unknown/overlapping cutoffs, future observations acquired **after local artifact freeze** are the primary alpha evidence. Use a frozen local artifact; a silently changing hosted endpoint cannot satisfy this premise.

Contaminated history remains useful for integration, numerical stability, invalid-output detection, runtime benchmarking, pipeline rehearsal and selecting questions for future experiments. Tag it **ENGINEERING_ONLY** or **HISTORICAL_DIAGNOSTIC**, never combine it with clean prospective observations to inflate an admission statistic.

### 5.2 Model-specific integration order, without capability inflation

The following are integration/evaluation roles, not verified clean-pretraining declarations. No cutoff is inferred merely from the publication date.

| Candidate | Initial ATLAS role / required check |
|---|---|
| [TiRex-2](https://github.com/NX-AI/tirex-2) | First general forecaster. Test causal past/future-known covariate handling; future-known means known at the origin, not a future realized market variable |
| [Kronos mini, then small](https://github.com/shiyu-coder/Kronos) | Financial OHLCV challenger. Pin tokenizer and sampling; check OHLC consistency and transformations. Financial pretraining makes overlap investigation particularly relevant |
| [Chronos-2-small](https://github.com/amazon-science/chronos-forecasting) | Replacement challenger for the general-model slot; compare at matched origin/context/budget |
| [Toto](https://github.com/DataDog/toto) | Later challenger. Pin exact generation/size and test its actual covariate/output contract; do not transfer support between generations |
| [TimesFM](https://github.com/google-research/timesfm) | Later general/covariate challenger with version-specific wrapper and resource benchmark |
| [Granite PatchTST-FM-r2](https://huggingface.co/ibm-granite/granite-timeseries-patchtst-fm-r2) | Later distributional challenger; validate quantile shape, units, context and resource use |
| [FinCast](https://github.com/vincent05r/FinCast-fts) | Financial challenger once checkpoint reproduction and compute are feasible; no unmeasured latency promise |
| [Moirai](https://huggingface.co/Salesforce/moirai-2.0-R-small) | Small alternative distributional challenger; quantiles do not themselves specify joint barrier paths |
| [TabPFN](https://github.com/PriorLabs/TabPFN) | Tabular conditional-model challenger. Every in-context labeled row must have matured before prediction; random train/test slicing is prohibited |

Retain at most one general and one distinct financial forecast worker in the active pilot; test candidates serially. Resource use is measured on the intended host, including warmup/compilation, queue wait and tail latency. Missing/late model output invokes the separately qualified baseline or NO TRADE, never an unvalidated weight redistribution.

### 5.3 Chronological features from a pretrained model

At each historical/present origin t, build inputs from the prefix available then, run the **frozen** checkpoint and store its forecast availability. Use a fixed small feature set: median return at 4h and 24h, corresponding interval widths, and forecast-minus-statistical-baseline at those horizons. Do not infer a calibrated probability of profit from these features.

For calibration fold k:

1. Generate checkpoint forecasts using origin-causal inputs. Mark each row's contamination class.
2. Fit the calibrator/scaler only on earlier matured labels; tune penalties/features only inside earlier folds.
3. Predict the untouched next chronological fold; store these calibrator outputs as OOF.
4. Form residuals only when outcomes mature. Purge overlapping target windows at boundaries; a 24h policy label requires its full holding/exit completion before training.

The fixed foundation model need not be retrained per fold. **OOF calibrator predictions do not remove pretraining leakage from foundation features.** Both eligibility dimensions must travel with every row: chronological fitting validity and pretraining-evidence validity.

### 5.4 Fair comparison and evidence-based admission

Pair A0 and challenger on the **same entire decision calendar**, universe, action rules, account constraints, common scenario seeds, executable costs, compute/human delays and no-fill outcomes. A model can change the calibrated predictive features, not quietly receive a later quote, larger budget, different stop or selected easy sample. Count all model variants and failed trials in the experiment ledger.

Report predictive proper scores, calibration, realized/shadow policy P&L, drawdown/tail changes, turnover/fill changes, coverage of selected candidates, compute cost and missed deadlines. Use P&L increments on a common account-equity/capital basis, including zero on rejected/no-fill slots. Trade-only averages are secondary.

**Stopping/admission rule, not a universal N:**

Before forward evaluation, freeze the primary incremental-value estimand \(\Delta\), minimum economically meaningful improvement \(\Delta_{min}\), acceptable degradation bounds, maximum evaluation budget and an analysis schedule. Use weekly nonoverlapping evaluation blocks, extending block length if dependence diagnostics require it. Hourly observations and simultaneous BTC/ETH outcomes are not independent replicates.

At prespecified look k, spend \(\alpha_k=0.05/[k(k+1)]\) across the registered challenger family, then construct a dependence-aware lower bound on incremental after-cost value at that error allocation. The sum of look budgets is ≤0.05; divide further across simultaneously tested challengers. This controls repeated-look allocation only to the extent that the underlying block inference is valid. If dependence/nonstationarity invalidates that inference, no statistical pass is issued.

Admit only when all hold:

- Lower bound \(LCB(\Delta)>\Delta_{min}>0\), with the full scanner/execution policy included.
- Preregistered upper bounds on tail-risk, calibration and operational degradation pass.
- Relevant volatility/liquidity/direction states have support; unsupported states remain gated out.
- Precision is sufficient: the confidence interval width is below the preregistered economic tolerance, and results are not driven by one effective block or one asset.
- Worst declared plausible execution/cost assumptions do not erase the admission result; real-time availability/deadline rates pass.

Estimate effective sample size/dependence from the **paired block increments**, with uncertainty, and report sensitivity to block length. The required calendar duration follows observed variance, dependence, effect size and regime coverage. More Monte Carlo paths, more overlapping forecasts or repeated model seeds do not increase independent market evidence.

At the fixed maximum evaluation budget, failure to meet the rule is **INCONCLUSIVE/REJECTED**, not automatic admission. Changing features, checkpoint, trade policy, stopping threshold or discretionary update schedule starts a new prospective test. A predeclared mechanical weekly refit is allowed if identically reproduced.

| Promotion state | Exit condition |
|---|---|
| INTEGRATED | Exact artifact and wrapper load; schema and reproducibility manifest complete |
| ENGINEERING_PASS | Causal input, numerical, missingness, runtime/resource and isolation checks pass |
| HISTORICAL_DIAGNOSTIC | Versioned retrospective report with contamination strata and complete trial record |
| PROSPECTIVE_SHADOW | Artifact/policy frozen; timestamped forecasts and full decision-calendar outcomes collected without influence |
| INCREMENTAL_VALUE_PASS | Prespecified value/uncertainty/degradation/support rule above passes |
| DECISION_ELIGIBLE | Human promotion of exact version plus account-policy approval and rollback rule |

DECISION_ELIGIBLE does not imply automatic trading permission. Drift, delayed forecasts, broken availability or failed rollback thresholds return the challenger to zero influence while preserving its evidence history.

---

## 6. Scanner-selection bias and whole-policy evaluation

### 6.1 Mandatory decision-calendar ledger

**FREEZE:** every scheduled scan slot records every instrument that reached the eligible observation universe, not only top-K candidates and not only trades. The scanner is itself a versioned policy whose outcomes must be evaluated on the full decision calendar.

Minimum record per instrument/slot:

~~~text
scan_slot_id
scan_policy_version
universe_version
instrument
availability_cutoff
eligibility_status / exclusion_reason
cheap_feature_snapshot_hash
cheap_score
rank
rank_tie_break
rank_band
correlation_cluster
selected_top_k
selected_for_deep_book
exploration_selected
deep_feature_warmup_state
model_job_enqueued_at / started_at / finished_at
model_deadline_status
plan_status / rejection_reason
approval_requested_at / approved_at / approval_expiry
human_delay_ms
entry_attempted
filled_qty / no_fill_reason
execution_delay_ms
matured_counterfactual_label_id
realized_policy_outcome_id
~~~

A rejected or unselected candidate is not deleted from evidence. When its horizon matures, compute the same causal diagnostic labels that were defined before the outcome became known. A counterfactual label is research evidence only; it must never fabricate an executable historical fill when the required execution data were unavailable.

### 6.2 Deterministic exploration outside top-K

**DEFAULT:** retain the revised architecture's top-3 candidate queue and top-5 deep-book budget. At every four-hour crypto decision slot, add **one shadow exploration candidate** outside the top 3 when the eligible universe contains one. It receives no capital and requires no human approval.

Partition non-top-3 ranks into three bands:

~~~text
B1 = ranks 4..10
B2 = ranks 11..20
B3 = ranks 21..N
~~~

Rotate the exploration band deterministically by UTC slot index modulo 3. Inside the selected nonempty band, choose the instrument by a stable hash of `(scan_policy_version, universe_version, scan_slot_id, instrument_id)`. If the selected band is empty, use the next nonempty band in cyclic order. Record the resulting inclusion probability for later inverse-probability weighting.

This candidate may start deep-feature warmup after selection, but **lack of prior warmup is itself an observed scanner outcome**. Do not silently substitute another candidate merely because the selected exploration instrument lacks mature OFI/L2 state. A price-only counterfactual may still be evaluated if that policy was already qualified; an OFI-dependent counterfactual remains `NOT_ESTIMABLE_WARMUP`.

Capital-enabled BTC/ETH are always kept warm while flat or held, so their v1 baseline is not allowed to depend on a late top-K subscription. Broader-universe deep features remain research-only until their warmup and selection effects are measured.

### 6.3 Point-in-time universe and correlation rules

Archive every universe refresh with instrument metadata, listing status, contract specification and the reason an instrument entered or left eligibility. Delisted, suspended and failed instruments remain in historical research records; they are never removed from prior universes. Backtests query the universe version known at each decision time, not today's survivors.

Scanner ranking and capital allocation are separate. The scanner may rank several highly correlated instruments, but the risk layer can reject them. Record a deterministic correlation-cluster label derived only from past data. Ties in scanner score are resolved by canonical instrument ID so two implementations produce the same queue.

### 6.4 Queue, approval and no-fill selection effects

Model-compute delay, stale model output, approval delay, plan expiry, IOC no-fill, partial fill and execution rejection are part of the policy outcome. They are not filtered from performance reports.

For assisted-mode evaluation, replay or record the actual distribution of human approval delays. A candidate that would have been profitable at signal time but expired before approval remains an **expired decision outcome**, not a winning trade. For automatic-mode research, evaluate with the separately declared machine-only delay distribution.

### 6.5 Minimal blind-spot estimators

Use the top-K population plus the known-probability exploration sample to estimate, by rank band and market state:

- **selection coverage:** fraction of estimated positive-after-cost opportunities captured by top-K;
- **missed-value share:** estimated positive counterfactual value outside top-K divided by total estimated positive value;
- **warmup exclusion rate:** fraction of potentially relevant candidates whose required deep features were not mature;
- **deadline loss rate:** fraction whose model/approval/execution delay invalidated an otherwise eligible plan;
- **selection lift:** paired difference between top-K and exploration outcomes under the same fixed counterfactual policy.

Use block bootstrap by decision time for uncertainty. Apply inverse-probability weights only to the deterministic exploration design whose inclusion probabilities are known. Do not pretend the unobserved remainder of the universe has exact deep-feature outcomes.

**CONFIG:** `scanner_blindspot_tolerance` is the maximum tolerated upper confidence bound on missed-value share before widening K or changing the cheap scanner. **DEFAULT engineering value: 0.20.** This is not an alpha target. If support is inadequate, the result is `INCONCLUSIVE`, not a pass.

A scanner revision is promoted only by a paired full-calendar comparison against the previous scanner, including compute load, warmup, no-fills and delay. Performance of executed trades alone is invalid evidence for scanner quality.

---

## 7. Capital and risk configuration contract

### 7.1 Typed policy

**FREEZE:** all capital limits live in one versioned `RiskPolicy`. They are never embedded inside strategies or foundation-model wrappers. Every TradePlan stores the exact risk-policy hash used to size and approve it.

~~~yaml
RiskPolicy:
  eligible_equity_definition: string
  normal_loss_per_trade_frac: decimal
  aggregate_open_normal_loss_frac: decimal
  stress_loss_per_trade_frac: decimal
  portfolio_es_alpha: decimal
  portfolio_es_limit_frac: decimal
  account_gross_notional_limit: decimal
  instrument_notional_limit: decimal
  correlated_crypto_beta_limit: decimal
  venue_collateral_limit: decimal
  min_free_margin_reserve_frac: decimal
  drawdown_reduce_threshold: decimal
  drawdown_stop_threshold: decimal
  drawdown_reduce_recovery: decimal
  drawdown_stop_recovery: decimal
  max_contract_leverage: decimal
  max_simultaneous_new_risk_intents: integer
  external_capital_reference: optional decimal
  policy_effective_at: timestamp
  policy_version: string
~~~

All fractions are defined against eligible account equity unless a field explicitly names external declared capital. Percent units are forbidden in persisted numeric fields; `0.001` means 0.10%.

### 7.2 Reservation mathematics

For intent `i`, define its **possible exposure** as confirmed filled exposure plus every unresolved opening quantity that could still legally fill, including `SUBMIT_UNKNOWN` and `CANCEL_PENDING` leaves. Never reserve only the locally visible filled amount while a late fill remains possible.

Reserve the vector:

\[
R_i=(B^{normal}_i, B^{stress}_i, N_i, \beta_i N_i, M_i, ES_i)
\]

at the worst permitted entry inside the approved collar and the maximum still-possible quantity. Existing positions, partially filled positions and unresolved intents consume the same account limits.

Before a new reservation, require:

\[
\sum_i B^{normal}_i + B^{normal}_{new}
\le b_{open}E,
\]

\[
B^{normal}_{new}\le b_{trade}E,
\qquad
B^{stress}_{new}\le b_{stress}E,
\]

\[
ES_{0.975}(L_{portfolio+pending+new})\le b_{ES},
\]

\[
\sum_i |N_i|+|N_{new}|\le b_{gross}E,
\]

\[
|N_{symbol}|+|N_{new,symbol}|\le b_{instrument}E,
\]

\[
\left|\sum_i \beta_iN_i+\beta_{new}N_{new}\right|
\le b_{beta}E,
\]

and

\[
M_{required,current}+M_{possible,pending}+M_{new}
\le M_{available}-M_{reserve}.
\]

Each contract must also satisfy

\[
N/M_0\le L_{contract,max}.
\]

The portfolio-ES calculation assumes pending intents can fill at their adverse allowed price and uses the same joint scenarios as the decision engine. For a cancel/replace race, reserve the maximum feasible exposure of old and proposed states until terminal evidence proves the old risk is gone.

Reservations may shrink only after fact-supported fills/cancels/terminal reconciliation. `SUBMIT_UNKNOWN`, `CANCEL_PENDING`, stale private state and incomplete REST history do not release capital.

**DEFAULT:** `max_simultaneous_new_risk_intents = 1` account-wide in v1. BTC is evaluated/reserved before ETH. This intentionally sacrifices concurrency to make recovery and portfolio accounting deterministic. A later policy version may raise the limit only after race and reservation tests.

### 7.3 Drawdown interaction

Let transfer-adjusted drawdown be `D`. New-risk loss budgets are multiplied by

\[
s(D)=
\begin{cases}
1, & D\le D_{reduce},\\
\dfrac{D_{stop}-D}{D_{stop}-D_{reduce}}, & D_{reduce}<D<D_{stop},\\
0, & D\ge D_{stop}.
\end{cases}
\]

Apply `s(D)` to per-trade normal loss, aggregate new/open loss headroom and stress/ES headroom for **new risk**. Hard leverage, collateral and margin-reserve limits never become looser during drawdown. Existing positions keep their approved protection/exit policy; reaching the drawdown stop blocks new risk but does not cancel protective orders.

Recovery uses hysteresis. Falling below a recovery threshold restores only the corresponding gate. Recovery from the stop state additionally requires human review and a clean account/protection reconciliation certificate.

### 7.4 Illustrative engineering defaults

These values are for testnet/shadow/canary engineering only. They are **not validated safe levels and not a recommendation for live capital**.

| Field | V1 engineering default |
|---|---:|
| normal loss per new trade | 0.0010 E (0.10%) |
| aggregate planned open normal loss | 0.0050 E (0.50%) |
| stress loss per new trade | 0.0025 E (0.25%) |
| portfolio ES limit, 97.5%, 24h | 0.0100 E (1.00%) |
| account gross notional | 1.00 E |
| per-instrument notional | 0.50 E |
| correlated crypto-beta exposure | 0.75 E |
| minimum free-margin reserve | 0.50 E |
| maximum contract leverage | 2.0x |
| drawdown reduction begins | 5% |
| drawdown new-risk stop | 10% |
| reduce-gate recovery | below 4% |
| stop-state recovery | below 8% plus human review |
| simultaneous new-risk intents | 1 |

For a dedicated canary account, `venue_collateral_limit` may equal 100% of that **already externally capped canary account**. If ATLAS is given a larger personal-capital reference, the recommended engineering cap is 25% of declared total ATLAS capital on one venue. If no external capital reference exists, report venue concentration relative to account equity rather than pretending to know personal net worth.

Actual live values are user-approved configuration and may be tighter. Loosening any live limit creates a new policy version and requires fresh revalidation; an emergency process may tighten limits immediately.

---

## 8. Final unresolved-issue classification

### 8.1 BLOCKER BEFORE CODING

**None.** The remaining uncertainty does not require another architecture redesign. The interfaces required to discover those uncertainties are now frozen.

### 8.2 MUST RESOLVE BEFORE ASSISTED EXECUTION

1. **Pinned Nautilus/Bybit wire behavior.** The capability manifest in §1 must pass on the actual installed artifact and target account profile: attached full-position mark stop, partial-fill resizing/coverage, reduce-only enforcement, external/native stop reconciliation and protection read/repair.
2. **Account and margin contract.** Confirm one-way/isolated mode, exact risk tier, fee schedule, quantity/tick rules, maintenance-margin/liquidation accounting and every field used by the leverage engine. Unsupported/ambiguous account modes remain disabled.
3. **Unknown-submit and crash recovery.** All §1.8 P0 fixtures must pass with no duplicated exposure, premature reservation release or false `CLOSED` state.
4. **Single-writer fencing.** The operational procedure for replacing a failed writer must demonstrably prevent the old host from continuing to trade.
5. **User-approved live RiskPolicy.** Engineering defaults do not authorize capital. Live assisted mode requires an explicit signed/recorded policy version.
6. **Protection and emergency path.** The native stop must survive client death, the two-second unconfirmed-protection path must behave as specified, and direct venue access plus the local emergency control must be tested.

Failure of one item disables assisted execution but does not block research, data collection, replay or alert-only operation.

### 8.3 CAN RESOLVE EMPIRICALLY DURING BUILD

- actual spread/depth/impact and IOC fill calibration;
- whether the 24/48/72-hour residual block candidates provide adequate dependence modelling;
- whether 2,048 inner paths and the precomputed outer-bootstrap machinery fit the host resource/deadline budget;
- funding-rate forecast support and historical predicted-funding coverage;
- scanner blind-spot/warmup rates and whether top-3/top-5 should later change;
- exact data-freshness thresholds after measuring venue/session behavior;
- foundation-model runtime, contamination class, calibration and incremental value;
- whether grouped derivatives, fundamentals, technical morphology or microstructure add incremental utility beyond A0;
- how much forward shadow history is required before any positive economic claim becomes statistically informative.

These questions are intentionally answered by evidence. Their interfaces already support `NO_TRADE`, `NOT_ESTIMABLE` and zero model influence.

### 8.4 DEFERRED RESEARCH

FX live connector qualification; market making; CEX/DEX or latency arbitrage; inverse/cross/portfolio margin; options execution; passive-queue strategies; multi-venue routing; active-active execution failover; large Bayesian latent-state stacks; automatic strategy promotion; dynamic Kelly sizing; large-model fine-tuning; large frontend; institutional/paid data whose value has not first been demonstrated.

---

## 9. ATLAS V1 FREEZE SPECIFICATION

### 9.1 Authoritative process boundaries

**`atlas-crypto-live`** is the only credential-bearing crypto writer. It contains the pinned Nautilus live node, ATLAS execution coordinator, risk reservations, protection port, incremental critical features and recovery logic.

**`atlas-ops`** owns slow public/event ingestion, notifications, scheduling, local status/control and experiment metadata. It cannot submit orders.

**`atlas-worker`** is optional and disposable. It runs bounded foundation inference or research jobs without trading credentials or writable live configuration.

Research code cannot mutate live capital policy. FX remains a separate future live process and account/risk envelope.

### 9.2 Authoritative state ownership

- **Bybit:** external orders, fills, positions, native protection and cash reality.
- **Nautilus:** operational order/execution projection and standard reconciliation.
- **ATLAS SQLite journal:** durable intent, approval, command uncertainty, reservation, evidence and economic audit.
- **ProtectionPort:** only full-position stop inspection/repair plus required raw protection evidence; never a second general OMS.

No component is allowed to infer successful execution from local intention alone.

### 9.3 First crypto strategy

`CRYPTO_TREND_24H_V1` from §2 is frozen:

- BTCUSDT and ETHUSDT only;
- four-hour decision slots using closed one-hour bars;
- 24-hour volatility-normalized momentum signal with ±0.5 band;
- 48-hour EWMA half-life and 720-return finite window;
- one marketable IOC limit entry;
- fixed `2 * sigma * sqrt(24)` mark-referenced stop, subject to eligibility bounds;
- native full-position MarkPrice market SL;
- no TP, no pyramiding, no averaging down, no immediate reversal;
- fixed 24-hour time exit;
- full cost/funding/event/risk gates.

B0 is shadow diagnostic without the statistical alpha veto. A0 adds the §3 scenario/LCB/portfolio gate and is the first decision-system baseline.

### 9.4 First required data

Required before an executable proposal:

- instrument metadata, tick/lot and risk-tier snapshots;
- closed 1h bars plus 1m last/mark/index path history for scenario replay;
- fresh bid/ask and usable depth for collar/impact checks;
- trades/quotes needed for forward execution calibration;
- funding schedule, predicted/settled rate evidence and transaction-log reconciliation;
- OI for archived challenger features, not required by A0 mean;
- private order, execution, position and wallet/account state;
- native protection observations;
- exact fees and margin fields;
- scheduled CPI/payroll/FOMC calendar evidence for the v1 event veto;
- immutable provenance/availability metadata defined in §4.

Missing required executable data produces no new risk.

### 9.5 First scenario engine

The v1 engine is the §3 implementation:

- small Huber/ridge one-hour conditional mean;
- the strategy's EWMA volatility;
- true chronological OOF residual archive;
- synchronized BTC/ETH residual blocks;
- default 24-hour block candidate selected from 24/48/72 inside training;
- 2,048 joint 24-hour paths;
- one-minute empirical within-hour replay;
- explicit IOC/no-fill/partial-fill, mark-stop, gap, funding, fee and time-exit mechanics;
- 200 outer synchronized block refits for mean uncertainty;
- 97.5% portfolio ES and `J(a)` decision rule;
- deterministic stress suite kept separate from statistical expectation.

Unsupported execution ordering returns bounds or `NOT_ESTIMABLE`; additional Monte Carlo is never used as a substitute for missing market evidence.

### 9.6 Risk-policy interface

Use the versioned `RiskPolicy` in §7. Size quantity and margin jointly. Reservations include unresolved pending exposure. Default v1 serializes new-risk intents account-wide. Drawdown can only tighten new-risk authority. Actual live limits are user-approved configuration.

### 9.7 Protection mechanism

Preferred v1 mechanism: full-position native Bybit market SL triggered by MarkPrice, attached with the IOC entry when the pinned adapter/account proves the wire contract, immediately inspected after fills and repaired through the narrow protection port if needed.

The native stop remains while explicit-quantity reduce-only closes execute. A local stop emulator is not primary protection. If full-position native protection cannot be qualified, assisted execution remains disabled until an explicitly versioned replacement protection design passes the same race/recovery tests.

### 9.8 Recovery mechanism

Startup begins `RECOVERING`, fences writers, restores journal/reservations, establishes public/private sessions, queries unresolved order identities plus executions/positions/wallet/protection, merges buffered events, rebuilds the Nautilus projection, repairs or reduces unprotected owned exposure, reconciles cash separately and resumes new risk only after a signed reconciliation certificate.

Unknown submit remains unknown until positive terminal evidence resolves it. Negative recent-order lookup never authorizes a duplicate order.

### 9.9 Foundation-model qualification

Foundation models have zero v1 authority until they pass:

~~~text
INTEGRATED
 -> ENGINEERING_PASS
 -> HISTORICAL_DIAGNOSTIC
 -> PROSPECTIVE_SHADOW
 -> INCREMENTAL_VALUE_PASS
 -> DECISION_ELIGIBLE
~~~

Pretraining contamination class and chronological calibration validity are tracked separately. Historical contaminated periods may test engineering but cannot establish clean alpha. Prospective shadow uses a frozen artifact and full decision-calendar outcomes. Admission requires positive dependence-aware incremental after-cost value versus A0 plus calibration/tail/operational non-degradation. Missing or late model output falls back to the qualified baseline or `NO_TRADE`.

### 9.10 Scanner evaluation protocol

Every eligible instrument/slot is logged. Top-K is evaluated with one known-probability rotating shadow exploration candidate, point-in-time universes, matured rejected-candidate labels where defensible, warmup status, queue/approval delays and no-fill outcomes. Scanner changes require paired full-calendar evaluation; selected-trade P&L alone is invalid.

### 9.11 Exact phase 0-6 build sequence

**Phase 0 — Freeze/install contracts**
- materialize typed CapabilityContract, InformationContract, TradePlan, RiskPolicy and journal schemas;
- install the exact Nautilus candidate pin and capture artifact/dependency hashes;
- create the Bybit capability manifest with all unsupported behavior disabled.

**Phase 1 — Safe runtime skeleton**
- `atlas-crypto-live` single writer;
- SQLite WAL durable outbox/reservations;
- testnet public/private connectivity;
- identity/environment checks, fencing and restart behavior;
- `atlas-ops` status/notification boundary.

**Phase 2 — Protection/recovery qualification**
- implement the narrow protection port;
- run all §1.8 submit/partial-fill/stop/close/reconnect/crash/cash fixtures;
- no assisted mode until the required capabilities pass.

**Phase 3 — Causal data foundation**
- begin immutable BTC/ETH forward archives immediately;
- store bars, 1m last/mark/index, quotes/depth, trades, funding/OI, metadata, private/economic events;
- implement ACTUAL_SYSTEM and RECONSTRUCTED_MARKET replay views plus prefix-invariance tests.

**Phase 4 — Baseline science and risk engine**
- implement `CRYPTO_TREND_24H_V1`, B0 and A0;
- chronological Huber/EWMA model, OOF archive, joint scenario replay, cost/funding model, deterministic stresses and typed RiskPolicy;
- verify hand-calculated accounting and `NOT_ESTIMABLE` behavior.

**Phase 5 — Continuous scanner and alert**
- unattended universe refresh, cheap scan, top-K/deep warmup and exploration logging;
- full TradePlan generation with contradictory evidence and reasons;
- alert-only mode, health/status and complete decision-calendar persistence.

**Phase 6 — Assisted control shell**
- plan-version-bound one-use approval;
- fresh quote/account/risk revalidation;
- atomic reservation and durable command before submit;
- pause/protect/close/flatten controls and recovery certificate;
- assisted execution flag remains disabled until every §8.2 gate and user-approved live RiskPolicy pass.

Foundation Model Arena work begins **after** this foundation as phase 7 and is not required for a scientifically useful ATLAS v1 research/alert daemon.

### 9.12 Explicit deferrals

V1 deliberately excludes market making, passive queue strategies, latency arbitrage, CEX/DEX execution, multi-venue routing, cross/portfolio margin, inverse contracts, options trading, dynamic Kelly, active-active failover, automatic research-to-live promotion, large custom ML training and a large web frontend.

The architecture consultation is now **closed**. Future changes are handled as versioned implementation or experiment proposals. A failed exchange capability test may disable or replace a bounded adapter/protection implementation, but it does not reopen the entire architecture unless the single-writer, protection, causality or evidence contracts themselves prove impossible.

