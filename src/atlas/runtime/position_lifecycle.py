"""Bounded journal-backed position review; no transport or capital authority.

This V1 projection consumes the existing Binance recovery capture, not a second
OMS. Old captures without exact product bindings remain UNKNOWN. Bybit's frozen
attached protection/recovery path is unchanged. Economics continue through the
existing outcome/maturity machinery, never through a flat-position shortcut.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation

from atlas.domain.enums import CommandOutcome, CommandType
from atlas.persistence.sqlite import PersistenceError, SQLiteJournal
from atlas.v2.instruments import EnvironmentV2, InstrumentKeyV2, ProductContractV2, VenueV2
from atlas.v2.strategies.s1_trend import ExecutableQuote

from .binance_demo import MAX_STATE_AGE_NS, BinanceDemoIdentity
from .reconciliation_evidence import QueryType, build_reconciliation_bundle


@dataclass(frozen=True)
class PositionLifecycleProjectionV1:
    execution_key: InstrumentKeyV2
    source_key: InstrumentKeyV2 | None
    product_ref: str
    intent_id: str
    position_epoch: int
    writer_epoch: int
    reconciliation_run_id: str
    signed_quantity: Decimal | None
    state: str
    protection_verified: bool
    next_review_at_ns: int
    evidence_refs: tuple[str, ...]
    reasons: tuple[str, ...]
    economic_status: str = field(default="NOT ESTIMABLE", init=False)
    version: str = field(default="ATLAS_POSITION_LIFECYCLE_PROJECTION_V1", init=False)
    can_admit_opening: bool = field(default=False, init=False)
    can_release_reservation: bool = field(default=False, init=False)
    can_mature: bool = field(default=False, init=False)


def project_position_lifecycle(
    *,
    journal: SQLiteJournal,
    reconciliation_run_id: str,
    intent_id: str,
    identity: BinanceDemoIdentity,
    product: ProductContractV2,
    writer_id: str,
    writer_epoch: int,
    now_ns: int,
    quote: ExecutableQuote | None = None,
    funding_schedule_ref: str | None = None,
) -> PositionLifecycleProjectionV1:
    """Read actual durable exposure/protection facts; schedule compulsory local review.

    Quote/funding checks are scope diagnostics only: they cannot qualify depth,
    fees, margin, fill probability or the installed adapter. Missing quotes never
    authorize removal of protection or inhibit the existing closing-only path.
    """
    intent = journal.load_intent(intent_id)
    reasons: list[str] = []
    blocking: list[str] = []
    qty: Decimal | None = None
    source_key: InstrumentKeyV2 | None = None
    protected = False
    refs: tuple[str, ...] = ()
    state = "UNKNOWN"
    deadline = now_ns
    if (
        product.key.venue != VenueV2.BINANCE
        or product.key.environment != EnvironmentV2.DEMO
        or identity.environment != "DEMO"
    ):
        blocking.append("EXECUTION_VENUE_PROFILE_MISMATCH")
    if intent.writer_epoch != writer_epoch:
        blocking.append("WRITER_EPOCH_MISMATCH")
    if product.available_at_ns > now_ns or product.effective_at_ns > now_ns:
        blocking.append("PRODUCT_NOT_AVAILABLE")
    if quote is None or quote.key != product.key or not quote.valid_at(now_ns):
        reasons.append("EXECUTION_VENUE_QUOTE_UNAVAILABLE")
    if funding_schedule_ref != product.funding_schedule_ref or product.funding_schedule_ref is None:
        reasons.append("EXECUTION_VENUE_FUNDING_UNVERIFIED")
    try:
        run = journal.load_reconciliation_run(reconciliation_run_id)
        queries = journal.load_run_queries(reconciliation_run_id)
        commands = journal.load_commands_for_intent(intent_id, limit=33)
        if len(queries) > 16 or len(commands) > 32:
            blocking.append("PROJECTION_BOUND_EXHAUSTED")
        bundle = build_reconciliation_bundle(run, queries)
        if not bundle.complete_for_recovery:
            blocking.append("RECOVERY_EVIDENCE_INCOMPLETE")
        if (
            run.account != identity.account_scope_ref
            or run.instrument != product.key.native_symbol
            or run.writer_id != writer_id
            or run.writer_epoch != writer_epoch
        ):
            blocking.append("RECOVERY_SCOPE_MISMATCH")
        for query in queries:
            facts = query.facts
            if (
                facts.get("execution_key") != product.key.to_dict()
                or facts.get("instrument_ref") != product.content_hash
                or facts.get("venue_identity_hash") != identity.content_hash
                or facts.get("environment") != identity.environment
                or facts.get("receipt_source") != "SIGNED_BINANCE_DEMO_READER"
                or not facts.get("response_hashes")
            ):
                blocking.append("SOURCE_EXECUTION_BINDING_MISMATCH")
            if query.query_type in (
                QueryType.POSITIONS,
                QueryType.TRADING_STOP,
                QueryType.OPEN_ORDERS,
                QueryType.CONDITIONAL_ORDERS,
                QueryType.WALLET_BALANCE,
            ):
                if not 0 <= now_ns - query.receipt_time_ns < MAX_STATE_AGE_NS:
                    blocking.append("PRIVATE_STATE_STALE")
                deadline = min(
                    deadline if deadline > now_ns else now_ns + MAX_STATE_AGE_NS,
                    query.receipt_time_ns + MAX_STATE_AGE_NS,
                )
        current = {query.query_type: query for query in queries}
        positions = current[QueryType.POSITIONS]
        source_key = InstrumentKeyV2.from_dict(positions.facts["execution_key"])
        qty = Decimal(str(positions.facts["signed_qty"]))
        if not qty.is_finite() or positions.facts.get("position_epoch") != intent.position_epoch:
            blocking.append("POSITION_CONTRADICTION")
            qty = None
        opening = [c for c in commands if c.command_type == CommandType.SUBMIT_ENTRY]
        if len(opening) != 1:
            blocking.append("OPENING_IDENTITY_MISSING_OR_DUPLICATE")
        for command in commands:
            payload = json.loads(command.payload)
            if (
                payload.get("identity_hash") != identity.content_hash
                or payload.get("instrument_ref") != product.content_hash
                or payload.get("symbol") != product.key.native_symbol
            ):
                blocking.append("COMMAND_EXECUTION_BINDING_MISMATCH")
            if (
                command.command_type == CommandType.SUBMIT_ENTRY
                and payload.get("client_order_id") != intent.client_order_id
            ):
                blocking.append("OPENING_IDENTITY_MISMATCH")
        if any(
            c.outcome in (CommandOutcome.UNKNOWN, CommandOutcome.UNSENT, CommandOutcome.DEFINITE_ACCEPT)
            for c in opening
        ):
            blocking.append("OPENING_OUTCOME_UNRESOLVED")
        stops = current[QueryType.TRADING_STOP].facts.get("protection_readbacks", [])
        verified_algo_ids: set[str] = set()
        for readback in stops:
            stop_command = next(
                (
                    c
                    for c in commands
                    if c.command_id == readback.get("command_id") and c.command_type == CommandType.REPAIR_STOP
                ),
                None,
            )
            if (
                stop_command is not None
                and readback.get("verified") is True
                and readback.get("position_epoch") == intent.position_epoch
                and Decimal(str(readback.get("signed_position"))) == qty
                and readback.get("algo_order_id")
                and Decimal(str(readback.get("stop"))) == Decimal(str(json.loads(stop_command.payload).get("stop")))
            ):
                protected = True
                verified_algo_ids.add(str(readback["algo_order_id"]))
        if qty != 0 and (
            current[QueryType.OPEN_ORDERS].total_records_returned
            or current[QueryType.CONDITIONAL_ORDERS].total_records_returned > len(verified_algo_ids)
        ):
            blocking.append("EXTERNAL_OR_UNRESOLVED_ORDERS")
        if qty == 0 and (
            current[QueryType.CONDITIONAL_ORDERS].total_records_returned
            or current[QueryType.OPEN_ORDERS].total_records_returned
        ):
            blocking.append("OUTSTANDING_CONTINGENT_ORDERS")
        refs = tuple(q.evidence_hash for q in queries) + tuple(c.exact_payload_hash for c in commands)
        if not blocking:
            state = "FLAT_PENDING_ECONOMICS" if qty == 0 else "OPEN_PROTECTED" if protected else "OPEN_UNPROTECTED"
        if state != "OPEN_PROTECTED":
            deadline = now_ns
    except (PersistenceError, ValueError, KeyError, TypeError, InvalidOperation):
        blocking.append("LIFECYCLE_READBACK_UNAVAILABLE")
        state, protected, deadline = "UNKNOWN", False, now_ns
    if blocking:
        state, protected, deadline = "UNKNOWN", False, now_ns
    return PositionLifecycleProjectionV1(
        product.key,
        source_key,
        product.content_hash,
        intent_id,
        intent.position_epoch,
        writer_epoch,
        reconciliation_run_id,
        qty,
        state,
        protected,
        deadline,
        refs,
        tuple(dict.fromkeys((*blocking, *reasons))),
    )
