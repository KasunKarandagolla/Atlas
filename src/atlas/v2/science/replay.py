"""Execution-aware research replay of frozen S1/S2 actions on common minute paths."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any

from atlas.domain.money import canonical_decimal_str as _c
from atlas.science.execution_replay import MINUTE_NS, ReplayMinute, first_complete_minute
from atlas.v2._serialization import canonical_json, sha256_json, sha256_ref
from atlas.v2.contracts import CandidateActionV2
from atlas.v2.data.bars import CausalBarV2
from atlas.v2.instruments import ProductContractV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.strategies.s1_trend import S1_POLICY
from atlas.v2.strategies.s2_breakout import S2_POLICY, failed_break_exit

from .action import ActionArtifactV2
from .costs import FeeScheduleV2, FundingCashflowV2, FundingScheduleV2, fee_cash, funding_cash

HOUR_NS = 3_600_000_000_000
REPLAY_VERSION = "S1_S2_EXECUTION_REPLAY_V1"
ZERO = Decimal("0")


def _minute_dict(x: ReplayMinute) -> dict[str, Any]:
    return {"at_ns": x.at_ns, "bid": _c(x.bid) if x.bid is not None else None,
            "ask": _c(x.ask) if x.ask is not None else None,
            "bid_depth": _c(x.bid_depth) if x.bid_depth is not None else None,
            "ask_depth": _c(x.ask_depth) if x.ask_depth is not None else None,
            "mark_low": _c(x.mark_low), "mark_high": _c(x.mark_high),
            "last_low": _c(x.last_low), "last_high": _c(x.last_high), "available": x.available}


def _indexed(repo: OpsRepository, ref: str, cutoff_ns: int, kind: str | None = None) -> ArtifactIndexEntryV2 | None:
    try:
        item = repo.get_artifact(ref)
    except ValueError:
        return None
    return item if item is not None and item.content_hash == ref and item.available_at_ns <= cutoff_ns and (
        kind is None or item.artifact_type == kind) else None


@dataclass(frozen=True)
class ReplayPathV2:
    common_path_id: str
    scenario_manifest_ref: str
    available_at_ns: int
    minutes: tuple[ReplayMinute, ...]
    closed_15m: tuple[CausalBarV2, ...]
    funding: tuple[FundingCashflowV2, ...]
    source_ref: str

    def __post_init__(self) -> None:
        for name in ("common_path_id", "scenario_manifest_ref", "source_ref"):
            sha256_ref(getattr(self, name), field=name)
        if type(self.available_at_ns) is not int or self.available_at_ns < 0:
            raise ValueError("path availability invalid")
        times = tuple(x.at_ns for x in self.minutes)
        if not times or times != tuple(sorted(set(times))):
            raise ValueError("replay minutes must be unique and ordered")
        if (any(x.at_ns < 0 for x in self.minutes)
                or self.available_at_ns < self.minutes[-1].at_ns + MINUTE_NS):
            raise ValueError("replay minute availability invalid")
        for x in self.minutes:
            prices = (x.mark_low, x.mark_high, x.last_low, x.last_high)
            if (any(not isinstance(v, Decimal) or not v.is_finite() or v <= 0 for v in prices)
                    or x.mark_low > x.mark_high or x.last_low > x.last_high):
                raise ValueError("invalid replay mark/last OHLC bound")
            if x.bid is not None and (not isinstance(x.bid, Decimal) or not x.bid.is_finite() or x.bid <= 0):
                raise ValueError("invalid replay bid")
            if x.ask is not None and (not isinstance(x.ask, Decimal) or not x.ask.is_finite() or x.ask <= 0):
                raise ValueError("invalid replay ask")
            if x.bid is not None and x.ask is not None and x.bid > x.ask:
                raise ValueError("crossed replay BBO")
            for depth in (x.bid_depth, x.ask_depth):
                if depth is not None and (not isinstance(depth, Decimal) or not depth.is_finite() or depth < 0):
                    raise ValueError("invalid replay depth")
        funding_times = tuple(x.at_ns for x in self.funding)
        if len(set(funding_times)) != len(funding_times):
            raise ValueError("duplicate funding settlement")

    def to_dict(self) -> dict[str, Any]:
        return {"version": "V2_COMMON_REPLAY_PATH_V1", "common_path_id": self.common_path_id,
                "scenario_manifest_ref": self.scenario_manifest_ref, "available_at_ns": self.available_at_ns,
                "minutes": [_minute_dict(x) for x in self.minutes],
                "closed_15m_refs": [x.content_hash for x in self.closed_15m],
                "funding_refs": [x.content_hash for x in self.funding], "source_ref": self.source_ref}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def index_replay_path(repo: OpsRepository, path: ReplayPathV2) -> str:
    refs = (path.source_ref, path.scenario_manifest_ref)
    if any(_indexed(repo, ref, path.available_at_ns) is None for ref in refs):
        raise ValueError("path source/manifest unavailable")
    for bar in path.closed_15m:
        if (not bar.final or bar.close_at_ns > path.minutes[-1].at_ns + MINUTE_NS or
                bar.raw.available_at_ns > path.available_at_ns):
            raise ValueError("path closed-bar evidence invalid")
        repo.register_artifact(ArtifactIndexEntryV2(bar.content_hash, "CausalBarV2", bar.content_hash,
            bar.raw.available_at_ns, bar.raw.available_at_ns, bar.to_dict()))
    for settlement in path.funding:
        item = _indexed(repo, settlement.content_hash, path.available_at_ns, "FundingCashflowV2")
        if item is None or canonical_json(item.metadata) != canonical_json(settlement.to_dict()):
            raise ValueError("path funding evidence unavailable")
    repo.register_artifact(ArtifactIndexEntryV2(path.content_hash, "ReplayPathV2", path.content_hash,
        path.available_at_ns, path.available_at_ns, {"path": path.to_dict()}))
    return path.content_hash


@dataclass(frozen=True)
class ReplayAssumptionsV2:
    decision_to_arrival_ns: int
    human_delay_ns: int
    stop_latency_ns: int
    exit_latency_ns: int
    participation: Decimal
    exit_impact: Decimal

    def __post_init__(self) -> None:
        for name in ("decision_to_arrival_ns", "human_delay_ns", "stop_latency_ns", "exit_latency_ns"):
            if type(getattr(self, name)) is not int or getattr(self, name) < 0:
                raise ValueError(f"{name} must be nonnegative nanoseconds")
        if (not isinstance(self.participation, Decimal) or not self.participation.is_finite() or
                not ZERO < self.participation <= Decimal("1")):
            raise ValueError("participation must be Decimal fraction in (0,1]")
        if not isinstance(self.exit_impact, Decimal) or not self.exit_impact.is_finite() or self.exit_impact < 0:
            raise ValueError("exit_impact must be nonnegative Decimal price offset")

    def to_dict(self) -> dict[str, Any]:
        return {"version": "V2_REPLAY_ASSUMPTIONS_V1", "decision_to_arrival_ns": self.decision_to_arrival_ns,
                "human_delay_ns": self.human_delay_ns, "stop_latency_ns": self.stop_latency_ns,
                "exit_latency_ns": self.exit_latency_ns, "participation": _c(self.participation),
                "exit_impact": _c(self.exit_impact),
                "minute_only_arrival": "FIRST_COMPLETE_MINUTE_AT_OR_AFTER_ARRIVAL",
                "stop_ohlc_order": "ADVERSE_MARK_TRIGGER_THEN_NEXT_MINUTE_EXECUTION",
                "same_timestamp_funding": "EXIT_BEFORE_FUNDING"}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def index_replay_assumptions(repo: OpsRepository, assumptions: ReplayAssumptionsV2,
                             available_at_ns: int) -> str:
    repo.register_artifact(ArtifactIndexEntryV2(assumptions.content_hash, "ReplayAssumptionsV2",
        assumptions.content_hash, available_at_ns, available_at_ns, assumptions.to_dict()))
    return assumptions.content_hash


@dataclass(frozen=True)
class ReplayFillV2:
    at_ns: int
    quantity: Decimal
    price: Decimal
    fee: Decimal
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {"at_ns": self.at_ns, "quantity": _c(self.quantity), "price": _c(self.price),
                "fee": _c(self.fee), "reason": self.reason}


class ReplayStatusV2(StrEnum):
    NO_FILL = "NO_FILL"
    PARTIAL_FILL = "PARTIAL_FILL"
    FULL_FILL = "FULL_FILL"
    NOT_ESTIMABLE = "NOT_ESTIMABLE"


@dataclass(frozen=True)
class PolicyPayoffV2:
    action_hash: str
    action_artifact_ref: str
    path_ref: str
    common_path_id: str
    scenario_manifest_ref: str
    existing_portfolio_ref: str
    replay_assumptions_ref: str
    fee_ref: str
    funding_schedule_ref: str
    entry: ReplayFillV2 | None
    exits: tuple[ReplayFillV2, ...]
    funding_cashflows: tuple[tuple[str, Decimal], ...]
    filled_quantity: Decimal
    remaining_quantity: Decimal
    payoff: Decimal | None
    status: ReplayStatusV2
    exit_reason: str
    portfolio_horizon_end_ns: int
    available_at_ns: int
    reasons: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {"version": REPLAY_VERSION, "action_hash": self.action_hash,
                "action_artifact_ref": self.action_artifact_ref, "path_ref": self.path_ref,
                "common_path_id": self.common_path_id, "scenario_manifest_ref": self.scenario_manifest_ref,
                "existing_portfolio_ref": self.existing_portfolio_ref,
                "replay_assumptions_ref": self.replay_assumptions_ref,
                "fee_ref": self.fee_ref, "funding_schedule_ref": self.funding_schedule_ref,
                "entry": self.entry.to_dict() if self.entry is not None else None,
                "exits": [x.to_dict() for x in self.exits],
                "funding_cashflows": [[ref, _c(value)] for ref, value in self.funding_cashflows],
                "filled_quantity": _c(self.filled_quantity), "remaining_quantity": _c(self.remaining_quantity),
                "payoff": _c(self.payoff) if self.payoff is not None else None,
                "status": self.status.value, "exit_reason": self.exit_reason,
                "portfolio_horizon_end_ns": self.portfolio_horizon_end_ns,
                "available_at_ns": self.available_at_ns, "reasons": list(self.reasons)}

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())


def _economic_result(*, action: ActionArtifactV2, path: ReplayPathV2,
                     existing_portfolio_ref: str, assumptions: ReplayAssumptionsV2,
                     fee: FeeScheduleV2, schedule: FundingScheduleV2,
                     candidate: CandidateActionV2, entry: ReplayFillV2 | None,
                     exits: tuple[ReplayFillV2, ...], funding_rows: tuple[tuple[str, Decimal], ...],
                     status: ReplayStatusV2, reason: str, available_at_ns: int,
                     base_units_per_contract: Decimal, error: str | None = None) -> PolicyPayoffV2:
    filled = entry.quantity if entry is not None else ZERO
    exited = sum((x.quantity for x in exits), ZERO)
    if exited > filled:
        raise ValueError("exit quantity exceeds entry")
    remaining = filled - exited
    reasons: tuple[str, ...]
    if status == ReplayStatusV2.NOT_ESTIMABLE or remaining > 0:
        status, payoff, reasons = ReplayStatusV2.NOT_ESTIMABLE, None, (error or "RESIDUAL_EXPOSURE_UNRESOLVED",)
    else:
        direction = Decimal("1") if candidate.side.value == "LONG" else Decimal("-1")
        payoff = ZERO if entry is None else direction * base_units_per_contract * (
            sum((x.quantity * x.price for x in exits), ZERO) - entry.quantity * entry.price)
        payoff -= sum((x.fee for x in exits), ZERO) + (entry.fee if entry is not None else ZERO)
        payoff += sum((amount for _, amount in funding_rows), ZERO)
        reasons = ()
    return PolicyPayoffV2(action.action.action_hash, action.content_hash, path.content_hash,
        path.common_path_id, path.scenario_manifest_ref, existing_portfolio_ref,
        assumptions.content_hash, fee.content_hash, schedule.content_hash, entry, exits, funding_rows,
        filled, remaining, payoff, status, reason, candidate.decision_at_ns + 24 * HOUR_NS,
        available_at_ns, reasons)


def replay_action(repo: OpsRepository, *, action: ActionArtifactV2, candidate: CandidateActionV2,
                  path: ReplayPathV2, existing_portfolio_ref: str,
                  assumptions: ReplayAssumptionsV2, fee: FeeScheduleV2, schedule: FundingScheduleV2,
                  product: ProductContractV2, replay_cutoff_ns: int) -> PolicyPayoffV2:
    """Replay IOC, fixed MarkPrice stop, S2 failed break and strategy time exit."""
    if candidate.content_hash != action.candidate_ref or candidate.key != action.action.key:
        raise ValueError("action/candidate mismatch")
    if action.action.policy_hash not in (S1_POLICY.policy_hash, S2_POLICY.policy_hash):
        raise ValueError("unsupported strategy replay policy")
    if (fee.key != candidate.key or product.key != candidate.key or
            product.content_hash != action.action.product_ref or
            candidate.horizon_end_ns - candidate.decision_at_ns != (
                S1_POLICY.max_hold_ns if candidate.policy_hash == S1_POLICY.policy_hash else S2_POLICY.max_hold_ns)):
        raise ValueError("replay fee/product key or units mismatch")
    if (path.available_at_ns > replay_cutoff_ns or fee.available_at_ns > candidate.decision_at_ns
            or schedule.available_at_ns > candidate.decision_at_ns):
        raise ValueError("future replay evidence or future entry-time funding schedule")
    required = ((action.content_hash, "ActionArtifactV2"), (path.content_hash, "ReplayPathV2"),
                (candidate.content_hash, "CandidateActionV2"), (action.sizing_ref, "SizingDecisionV2"),
                (action.candidate_set_ref, "CandidateSetV2"),
                (existing_portfolio_ref, "ExistingPortfolioPathV2"),
                (assumptions.content_hash, "ReplayAssumptionsV2"),
                (fee.content_hash, "FeeScheduleV2"), (schedule.content_hash, "FundingScheduleV2"),
                (path.scenario_manifest_ref, None), (product.content_hash, "ProductContractV2"))
    if any(_indexed(repo, ref, replay_cutoff_ns, kind) is None for ref, kind in required):
        raise ValueError("replay required artifact unindexed or unavailable")
    indexed_action = repo.get_artifact(action.content_hash)
    indexed_path = repo.get_artifact(path.content_hash)
    indexed_sizing = repo.get_artifact(action.sizing_ref)
    indexed_candidate = repo.get_artifact(candidate.content_hash)
    indexed_assumptions = repo.get_artifact(assumptions.content_hash)
    indexed_fee = repo.get_artifact(fee.content_hash)
    indexed_schedule = repo.get_artifact(schedule.content_hash)
    indexed_product = repo.get_artifact(product.content_hash)
    indexed_portfolio = repo.get_artifact(existing_portfolio_ref)
    indexed_set = repo.get_artifact(action.candidate_set_ref)
    assert all(x is not None for x in (indexed_action, indexed_path, indexed_sizing,
        indexed_candidate, indexed_assumptions, indexed_fee, indexed_schedule, indexed_product))
    assert indexed_action is not None and indexed_path is not None and indexed_sizing is not None
    assert indexed_candidate is not None and indexed_assumptions is not None
    assert indexed_fee is not None and indexed_schedule is not None and indexed_product is not None
    assert indexed_portfolio is not None and indexed_set is not None
    sizing_body = indexed_sizing.metadata.get("sizing")
    portfolio_body = indexed_portfolio.metadata.get("portfolio")
    if (canonical_json(indexed_action.metadata.get("action_identity")) != canonical_json(action.action.to_dict())
            or canonical_json(indexed_action.metadata.get("action_artifact")) != canonical_json(action.to_dict())
            or canonical_json(indexed_path.metadata.get("path")) != canonical_json(path.to_dict())
            or canonical_json(indexed_candidate.metadata.get("candidate")) != candidate.to_canonical_json()
            or canonical_json(indexed_assumptions.metadata) != canonical_json(assumptions.to_dict())
            or canonical_json(indexed_fee.metadata) != canonical_json(fee.to_dict())
            or canonical_json(indexed_schedule.metadata) != canonical_json(schedule.to_dict())
            or canonical_json(indexed_product.metadata.get("product")) != canonical_json(product.to_dict())
            or not isinstance(sizing_body, Mapping)
            or sizing_body.get("status") != "SIZED"
            or sizing_body.get("candidate_ref") != candidate.content_hash
            or sizing_body.get("candidate_set_ref") != action.candidate_set_ref
            or sizing_body.get("product_ref") != product.content_hash
            or sizing_body.get("risk_policy_hash") != action.action.risk_policy_hash
            or sizing_body.get("risk_policy_v2_hash") != action.action.risk_policy_v2_hash
            or fee.content_hash not in sizing_body.get("risk_input_refs", ())
            or sizing_body.get("quantity") != _c(action.action.quantity)
            or indexed_set.metadata.get("candidate_set") is None
            or not isinstance(portfolio_body, Mapping)
            or portfolio_body.get("common_path_id") != path.common_path_id
            or portfolio_body.get("scenario_manifest_ref") != path.scenario_manifest_ref
            or portfolio_body.get("horizon_end_ns") != candidate.decision_at_ns + 24 * HOUR_NS):
        raise ValueError("replay indexed action/risk/path content mismatch")
    if action.action.policy_hash == S2_POLICY.policy_hash:
        indexed_candidate = repo.get_artifact(candidate.content_hash)
        setup_ref = indexed_candidate.metadata.get("setup_evidence_ref") if indexed_candidate else None
        if not isinstance(setup_ref, str) or _indexed(repo, setup_ref, replay_cutoff_ns, "S2SetupEvidenceV1") is None:
            raise ValueError("S2 frozen setup unavailable")
        setup = repo.get_artifact(setup_ref)
        assert setup is not None
        if sha256_json(setup.metadata) != setup_ref:
            raise ValueError("S2 setup content mismatch")
        frozen_setup = json.loads(canonical_json(setup.metadata))
        relevant = tuple(bar for bar in path.closed_15m if bar.close_at_ns in (
            candidate.decision_at_ns + 15 * MINUTE_NS, candidate.decision_at_ns + 30 * MINUTE_NS))
        if len(relevant) != 2:
            failed_at = None
            missing_failed_break_support = True
        else:
            ref = failed_break_exit(candidate, frozen_setup, relevant)
            failed_at = next((bar.close_at_ns for bar in relevant if bar.content_hash == ref), None)
            missing_failed_break_support = False
    else:
        failed_at, missing_failed_break_support = None, False
    arrival = candidate.decision_at_ns + assumptions.decision_to_arrival_ns + assumptions.human_delay_ns
    entry_minute = first_complete_minute(path.minutes, arrival_ns=arrival)
    def finish(entry: ReplayFillV2 | None, exits: tuple[ReplayFillV2, ...], funding_rows: tuple[tuple[str, Decimal], ...],
               status: ReplayStatusV2, reason: str, error: str | None = None) -> PolicyPayoffV2:
        result = _economic_result(action=action, path=path, existing_portfolio_ref=existing_portfolio_ref,
            assumptions=assumptions, fee=fee, schedule=schedule, candidate=candidate,
            entry=entry, exits=exits, funding_rows=funding_rows, status=status, reason=reason,
            available_at_ns=replay_cutoff_ns, base_units_per_contract=product.base_units_per_contract,
            error=error)
        input_refs = sorted({ref for ref, _ in required} | {x.content_hash for x in path.funding}
                            | {x.content_hash for x in path.closed_15m})
        if action.action.policy_hash == S2_POLICY.policy_hash:
            input_refs.append(str(setup_ref))
        if any(_indexed(repo, ref, replay_cutoff_ns) is None for ref in input_refs):
            raise ValueError("payoff input ref unavailable at replay cutoff")
        repo.register_artifact(ArtifactIndexEntryV2(result.content_hash, "PolicyPayoffV2", result.content_hash,
            replay_cutoff_ns, replay_cutoff_ns, {"payoff": result.to_dict(), "input_refs": sorted(set(input_refs))}))
        return result
    if arrival > candidate.deadline_ns:
        return finish(None, (), (), ReplayStatusV2.NO_FILL, "DEADLINE_EXPIRED_BEFORE_ARRIVAL")
    if entry_minute is None:
        return finish(None, (), (), ReplayStatusV2.NOT_ESTIMABLE, "NO_ARRIVAL_MINUTE", "NO_ARRIVAL_MINUTE")
    if not entry_minute.available or entry_minute.bid is None or entry_minute.ask is None or (
        entry_minute.ask_depth if candidate.side.value == "LONG" else entry_minute.bid_depth) is None:
        return finish(None, (), (), ReplayStatusV2.NOT_ESTIMABLE, "MISSING_ENTRY_DEPTH", "MISSING_ENTRY_DEPTH")
    entry_price = entry_minute.ask if candidate.side.value == "LONG" else entry_minute.bid
    entry_depth = entry_minute.ask_depth if candidate.side.value == "LONG" else entry_minute.bid_depth
    assert entry_depth is not None
    if (entry_price > candidate.entry_collar if candidate.side.value == "LONG" else entry_price < candidate.entry_collar) or entry_depth <= 0:
        return finish(None, (), (), ReplayStatusV2.NO_FILL, "IOC_NO_FILL")
    fill_qty = min(action.action.quantity, entry_depth * assumptions.participation)
    if fill_qty <= 0:
        return finish(None, (), (), ReplayStatusV2.NO_FILL, "IOC_NO_FILL")
    entry = ReplayFillV2(entry_minute.at_ns, fill_qty, entry_price,
        fee_cash(fill_qty, entry_price, product.base_units_per_contract, fee.entry_taker_rate), "IOC_ENTRY")
    status = ReplayStatusV2.FULL_FILL if fill_qty == action.action.quantity else ReplayStatusV2.PARTIAL_FILL
    if missing_failed_break_support:
        return finish(entry, (), (), ReplayStatusV2.NOT_ESTIMABLE, "MISSING_S2_CLOSED_BARS", "MISSING_S2_CLOSED_BARS")
    exits: list[ReplayFillV2] = []
    stop_ready: int | None = None
    exit_reason = "TIME_EXIT"
    for minute in path.minutes:
        if minute.at_ns < entry_minute.at_ns or minute.at_ns >= candidate.decision_at_ns + 24 * HOUR_NS:
            continue
        if stop_ready is None and minute.at_ns < candidate.horizon_end_ns and (
            minute.mark_low <= candidate.stop_price if candidate.side.value == "LONG" else minute.mark_high >= candidate.stop_price):
            stop_ready = minute.at_ns + MINUTE_NS + assumptions.stop_latency_ns
        remaining = fill_qty - sum((x.quantity for x in exits), ZERO)
        if remaining <= 0:
            break
        reason: str | None = None
        if stop_ready is not None and minute.at_ns >= stop_ready:
            reason = "STOP_EXIT"
        elif failed_at is not None and minute.at_ns >= failed_at + assumptions.exit_latency_ns:
            reason = "FAILED_BREAK_EXIT"
        elif minute.at_ns >= candidate.horizon_end_ns + assumptions.exit_latency_ns:
            reason = "TIME_EXIT"
        if reason is None:
            continue
        if not minute.available or minute.bid is None or minute.ask is None:
            return finish(entry, tuple(exits), (), ReplayStatusV2.NOT_ESTIMABLE, reason, "MISSING_EXIT_BBO")
        price = minute.bid if candidate.side.value == "LONG" else minute.ask
        depth = minute.bid_depth if candidate.side.value == "LONG" else minute.ask_depth
        if depth is None:
            return finish(entry, tuple(exits), (), ReplayStatusV2.NOT_ESTIMABLE, reason, "MISSING_EXIT_DEPTH")
        if reason == "STOP_EXIT":
            price = min(price, minute.last_low) if candidate.side.value == "LONG" else max(price, minute.last_high)
        price = price - assumptions.exit_impact if candidate.side.value == "LONG" else price + assumptions.exit_impact
        if price <= 0:
            return finish(entry, tuple(exits), (), ReplayStatusV2.NOT_ESTIMABLE, reason, "INVALID_EXIT_PRICE")
        executed = min(remaining, depth * assumptions.participation)
        if executed <= 0:
            continue
        exits.append(ReplayFillV2(minute.at_ns, executed, price,
            fee_cash(executed, price, product.base_units_per_contract, fee.exit_taker_rate), reason))
        exit_reason = reason
        if sum((x.quantity for x in exits), ZERO) == fill_qty:
            break
    remaining = fill_qty - sum((x.quantity for x in exits), ZERO)
    if remaining > 0:
        return finish(entry, tuple(exits), (), ReplayStatusV2.NOT_ESTIMABLE,
                      "UNRESOLVED_EXIT", "RESIDUAL_EXPOSURE_UNRESOLVED")
    close_at = max(x.at_ns for x in exits)
    expected = tuple(t for t in schedule.expected_settlement_times_ns if entry.at_ns <= t < close_at)
    support = {x.at_ns: x for x in path.funding if x.available_at_ns <= replay_cutoff_ns}
    if any(t not in support for t in expected):
        return finish(entry, tuple(exits), (), ReplayStatusV2.NOT_ESTIMABLE,
                      exit_reason, "FUNDING_SUPPORT_MISSING")
    funding_rows: list[tuple[str, Decimal]] = []
    for t in expected:
        settlement = support[t]
        surviving = fill_qty - sum((x.quantity for x in exits if x.at_ns <= t), ZERO)
        funding_rows.append((settlement.content_hash,
            funding_cash(candidate.side.value, surviving, product.base_units_per_contract, settlement)))
    return finish(entry, tuple(exits), tuple(funding_rows), status, exit_reason)
