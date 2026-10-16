"""Executable, preregistered offline policy science. No campaign runtime.

Calendar blocks are observations; resamples only control Monte Carlo precision.
All statistics retain the complete public opportunity denominator, including
refusals, no fills, costs and missed deadlines. None grants capital promotion.
"""

from __future__ import annotations

import math
import random
import statistics
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import ClassVar

from atlas.v2.science.mean_common import DAY_NS, ZERO, OfflineArtifact, nonnegative, positive, unique


def calendar_block_means(values: Sequence[tuple[int, float]], *, block_ns: int) -> tuple[float, ...]:
    if block_ns <= 0 or not values or any(not math.isfinite(v) for _, v in values):
        raise ValueError("finite calendar values and positive block width required")
    buckets: dict[int, list[float]] = {}
    for at, value in values:
        buckets.setdefault(at // block_ns, []).append(value)
    return tuple(statistics.mean(buckets[k]) for k in sorted(buckets))


def bootstrap_mean_interval(
    blocks: Sequence[float], *, resamples: int = 10_000, alpha: float = 0.05, seed: int = 6101
) -> tuple[float, float]:
    if not len(blocks) >= 2 or not 100 <= resamples <= 100_000 or not 0 < alpha < 1:
        raise ValueError("invalid block-bootstrap precision/support")
    if any(not math.isfinite(x) for x in blocks):
        raise ValueError("nonfinite independent block")
    rng = random.Random(seed)
    n = len(blocks)
    means = sorted(sum(blocks[rng.randrange(n)] for _ in range(n)) / n for _ in range(resamples))
    return means[max(0, math.ceil(alpha * resamples) - 1)], means[
        min(resamples - 1, math.ceil((1 - alpha) * resamples) - 1)
    ]


def holm_rejections(p_values: tuple[tuple[str, float], ...], *, alpha: float) -> tuple[str, ...]:
    unique(tuple(k for k, _ in p_values))
    if not p_values or not 0 < alpha < 1 or any(not 0 <= p <= 1 for _, p in p_values):
        raise ValueError("valid registered family p-values required")
    accepted = []
    for i, (name, p) in enumerate(sorted(p_values, key=lambda x: (x[1], x[0]))):
        if p > alpha / (len(p_values) - i):
            break
        accepted.append(name)
    return tuple(accepted)


def look_alpha(total_alpha: float, look: int, family_size: int) -> float:
    if not 0 < total_alpha < 1 or look < 1 or family_size < 1:
        raise ValueError("registered look/family required")
    return total_alpha / (look * (look + 1) * family_size)


@dataclass(frozen=True)
class SelectedPolicyResidualV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_SELECTED_POLICY_MEAN_RESIDUAL_V1"
    model_ref: str
    policy_ref: str
    compatibility_ref: str
    outcome_ref: str
    origin_ns: int
    label_available_at_ns: int
    prediction_cash: Decimal
    target_cash: Decimal
    execution_bias_already_charged: bool

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.origin_ns >= self.label_available_at_ns or self.execution_bias_already_charged:
            raise ValueError("invalid mean residual chronology or double bias correction")


@dataclass(frozen=True)
class MeanSelectionCalibrationV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_SELECTED_POLICY_MEAN_CALIBRATION_V1"
    model_ref: str
    policy_ref: str
    compatibility_ref: str
    source_ref: str
    available_at_ns: int
    expires_at_ns: int
    block_ns: int
    effective_blocks: int
    calibration_refs: tuple[str, ...]
    allowance_cash: Decimal | None
    status: str
    conditional_scope: str

    def __post_init__(self) -> None:
        super().__post_init__()
        if not self.available_at_ns < self.expires_at_ns or self.block_ns <= 0:
            raise ValueError("calibration clock invalid")
        unique(self.calibration_refs)
        if self.status not in ("SUPPORTED_OFFLINE", "NOT ESTIMABLE"):
            raise ValueError("invalid calibration status")
        if self.status == "SUPPORTED_OFFLINE" and (self.allowance_cash is None or self.effective_blocks < 20):
            raise ValueError("unsupported selected-policy mean calibration")
        if self.allowance_cash is not None:
            nonnegative(self.allowance_cash)
        if self.conditional_scope != "REGISTERED_POLICY_COMPATIBLE_ACTION_SIZE_LIQUIDITY_CELL":
            raise ValueError("aggregate programme interval is not conditional mean calibration")


def calibrate_selected_policy(
    rows: tuple[SelectedPolicyResidualV1, ...],
    *,
    model_ref: str,
    policy_ref: str,
    compatibility_ref: str,
    source_ref: str,
    cutoff_ns: int,
    expires_at_ns: int,
    block_ns: int = 7 * DAY_NS,
    alpha: float = 0.05,
    dependence_qualified: bool,
    resamples: int = 10_000,
) -> MeanSelectionCalibrationV1:
    unique(tuple(r.outcome_ref for r in rows))
    if any(
        r.model_ref != model_ref
        or r.policy_ref != policy_ref
        or r.compatibility_ref != compatibility_ref
        or r.label_available_at_ns >= cutoff_ns
        for r in rows
    ):
        raise ValueError("calibration must be mature, chronological and policy/cell/model-bound")
    blocks = (
        calendar_block_means(
            tuple((r.origin_ns, float(r.prediction_cash - r.target_cash)) for r in rows), block_ns=block_ns
        )
        if rows
        else ()
    )
    allowance = None
    # V1 residuals do not prove OOF/dependence. Keep saved readers intact, but
    # no new supported calibration can be produced from declarations alone.
    # The V2 producer consumes verified chronological prediction receipts.
    return MeanSelectionCalibrationV1(
        model_ref,
        policy_ref,
        compatibility_ref,
        source_ref,
        cutoff_ns,
        expires_at_ns,
        block_ns,
        len(blocks),
        tuple(r.content_hash for r in rows),
        allowance,
        "SUPPORTED_OFFLINE" if allowance is not None else "NOT ESTIMABLE",
        "REGISTERED_POLICY_COMPATIBLE_ACTION_SIZE_LIQUIDITY_CELL",
    )


@dataclass(frozen=True)
class ProgrammeProtocolV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_REGISTERED_PROGRAMME_PROTOCOL_V1"
    registered_at_ns: int
    family: tuple[str, ...]
    policy_refs: tuple[str, ...]
    ablations: tuple[str, ...]
    planned_looks_ns: tuple[int, ...]
    block_ns: int
    block_sensitivities_ns: tuple[int, ...]
    minimum_blocks: int
    materiality_cash: Decimal
    maximum_interval_width_cash: Decimal
    maximum_tail_loss_cash: Decimal
    starting_capital_cash: Decimal
    information_policy_ref: str
    holdout_ref: str
    total_alpha: float = 0.05
    bootstrap_resamples: int = 10_000

    def __post_init__(self) -> None:
        super().__post_init__()
        unique(self.family)
        unique(self.policy_refs)
        if not self.family or len(self.policy_refs) < 2 or self.minimum_blocks < 20:
            raise ValueError("registered policy family and dependence-support floor required")
        if not self.planned_looks_ns or tuple(sorted(set(self.planned_looks_ns))) != self.planned_looks_ns:
            raise ValueError("fixed planned looks required")
        if self.planned_looks_ns[0] <= self.registered_at_ns or self.block_ns <= 0:
            raise ValueError("registration must precede prospective evidence")
        if (
            not self.block_sensitivities_ns
            or any(b < self.block_ns for b in self.block_sensitivities_ns)
            or not 0 < self.total_alpha < 1
            or not 100 <= self.bootstrap_resamples <= 100_000
        ):
            raise ValueError("invalid preregistered inference precision/sensitivity")
        positive(self.materiality_cash, self.maximum_interval_width_cash, self.starting_capital_cash)
        nonnegative(self.maximum_tail_loss_cash)


@dataclass(frozen=True)
class ProgrammeObservationV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_PUBLIC_OPPORTUNITY_PROGRAMME_CASH_V1"
    opportunity_id: str
    slot_ns: int
    label_available_at_ns: int
    arm: str
    trading_cash: Decimal
    programme_cost_cash: Decimal
    external_cashflow_cash: Decimal
    missed_opportunity_cash: Decimal | None
    outcome: str
    provenance: str
    bill_refs: tuple[str, ...]

    def __post_init__(self) -> None:
        super().__post_init__()
        nonnegative(self.programme_cost_cash)
        if self.programme_cost_cash > 0 and not self.bill_refs:
            raise ValueError("programme costs need unique incurred bill receipts")
        unique(self.bill_refs)
        if not self.opportunity_id or not self.arm or self.label_available_at_ns <= self.slot_ns:
            raise ValueError("programme calendar identity/maturity invalid")
        if self.outcome not in ("FULL_FILL", "PARTIAL_FILL", "NO_FILL", "REFUSED", "EXPIRED", "UNAVAILABLE"):
            raise ValueError("complete opportunity status required")
        if self.outcome in ("NO_FILL", "REFUSED", "EXPIRED", "UNAVAILABLE") and self.trading_cash:
            raise ValueError("refused/no-fill opening cannot earn trading cash")
        if self.provenance not in ("ACTUAL_RECONCILED", "PROSPECTIVE_SHADOW", "CAUSAL_REPLAY"):
            raise ValueError("diagnostic/censored programme evidence cannot promote")

    @property
    def programme_cash(self) -> Decimal:
        return self.trading_cash - self.programme_cost_cash


@dataclass(frozen=True)
class ResearchLedgerV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_EVOLVING_RESEARCH_LEDGER_V1"
    arm: str
    equity_cash: Decimal
    allocation_ceiling_cash: Decimal
    locked_cash: Decimal
    cumulative_programme_cash: Decimal
    external_cashflow_cash: Decimal
    paid_bill_refs: tuple[str, ...]
    opportunity_ids: tuple[str, ...]
    exposures: tuple[ResearchExposureV1, ...] = ()

    def __post_init__(self) -> None:
        super().__post_init__()
        nonnegative(self.equity_cash, self.allocation_ceiling_cash, self.locked_cash)
        unique(self.paid_bill_refs)
        unique(self.opportunity_ids)
        if self.exposures and self.locked_cash != projected_research_lock(self.exposures):
            raise ValueError("research exposure/margin/reservation ledger mismatch")
        if self.locked_cash > min(self.equity_cash, self.allocation_ceiling_cash):
            raise ValueError("research ledger exceeds matched capital")


def advance_research_ledger(
    state: ResearchLedgerV1, observation: ProgrammeObservationV1, *, lock_delta_cash: Decimal
) -> ResearchLedgerV1:
    """Pure accounting equation, not an account controller or live reservation.

    Cash after exit stays cash; locks can change only through supplied research
    transitions. Deposits/manual profits never expand the allocation ceiling.
    Missed-opportunity estimates are diagnostics, not fictitious booked wealth.
    """
    if observation.arm != state.arm or observation.opportunity_id in state.opportunity_ids:
        raise ValueError("duplicate/cross-arm research observation")
    if set(state.paid_bill_refs) & set(observation.bill_refs):
        raise ValueError("programme bill already charged")
    net = observation.programme_cash
    return ResearchLedgerV1(
        state.arm,
        state.equity_cash + net + observation.external_cashflow_cash,
        state.allocation_ceiling_cash,
        state.locked_cash + lock_delta_cash,
        state.cumulative_programme_cash + net,
        state.external_cashflow_cash + observation.external_cashflow_cash,
        state.paid_bill_refs + observation.bill_refs,
        state.opportunity_ids + (observation.opportunity_id,),
        state.exposures,
    )


def paired_programme_blocks(
    a: tuple[ProgrammeObservationV1, ...], b: tuple[ProgrammeObservationV1, ...], *, block_ns: int
) -> tuple[float, ...]:
    unique(tuple(r.opportunity_id for r in a))
    unique(tuple(r.opportunity_id for r in b))
    if {r.opportunity_id for r in a} != {r.opportunity_id for r in b}:
        raise ValueError("public opportunity denominator differs between arms")
    by_id = {r.opportunity_id: r for r in a}
    buckets: dict[int, float] = {}
    for row in b:
        base = by_id[row.opportunity_id]
        if (
            base.slot_ns != row.slot_ns
            or base.external_cashflow_cash != row.external_cashflow_cash
            or base.provenance != row.provenance
        ):
            raise ValueError("mismatched calendar/manual stream/evidence class")
        k = row.slot_ns // block_ns
        buckets[k] = buckets.get(k, 0.0) + float(row.programme_cash - base.programme_cash)
    return tuple(buckets[k] for k in sorted(buckets))


def programme_inference(
    blocks: tuple[float, ...],
    *,
    protocol: ProgrammeProtocolV1,
    now_ns: int,
    look: int,
    dependence_qualified: bool,
    power_qualified: bool,
) -> dict[str, object]:
    if look < 1 or look > len(protocol.planned_looks_ns) or now_ns != protocol.planned_looks_ns[look - 1]:
        raise ValueError("unplanned analysis look")
    if len(blocks) < protocol.minimum_blocks or not dependence_qualified or not power_qualified:
        return {"status": "NOT ESTIMABLE", "reason": "SUPPORT_DEPENDENCE_OR_POWER", "blocks": len(blocks)}
    alpha = look_alpha(protocol.total_alpha, look, len(protocol.family))
    lo, hi = bootstrap_mean_interval(blocks, resamples=protocol.bootstrap_resamples, alpha=alpha)
    tail = max(0.0, -min(blocks))
    supported = (
        lo > float(protocol.materiality_cash)
        and hi - lo <= float(protocol.maximum_interval_width_cash)
        and tail <= float(protocol.maximum_tail_loss_cash)
    )
    return {
        "status": "SUPPORTED_OFFLINE_COMPARISON" if supported else "NOT ESTIMABLE",
        "lower_cash": lo,
        "upper_cash": hi,
        "blocks": len(blocks),
        "alpha": alpha,
        "capital_authority": "ZERO",
    }


def power_simulation(
    *, effect: float, sigma: float, blocks: int, trials: int = 500, alpha: float = 0.05, seed: int = 6101
) -> tuple[float, float]:
    """Training-only Gaussian block sensitivity: null false-promotion and power.

    This explicitly limited simulation cannot certify real market dependence.
    Test threshold is one-sided known-sigma normal; empirical block inference
    still requires the separate programme/dependence/holdout qualifications.
    """
    if effect < 0 or sigma <= 0 or blocks < 2 or trials < 100 or not 0 < alpha < 0.5:
        raise ValueError("invalid preregistered power experiment")
    threshold = statistics.NormalDist().inv_cdf(1 - alpha) * sigma / math.sqrt(blocks)
    rng = random.Random(seed)
    null = signal = 0
    for _ in range(trials):
        error = statistics.mean(rng.gauss(0, sigma) for _ in range(blocks))
        null += error > threshold
        signal += error + effect > threshold
    return null / trials, signal / trials


@dataclass(frozen=True)
class ProtectedMeanHoldoutV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_PROTECTED_MEAN_HOLDOUT_V1"
    family_ref: str
    compatibility_ref: str
    source_ref: str
    reserved_at_ns: int
    start_ns: int
    end_ns: int
    outcome_refs: tuple[str, ...]

    def __post_init__(self) -> None:
        super().__post_init__()
        unique(self.outcome_refs)
        if not self.reserved_at_ns < self.start_ns < self.end_ns or not self.outcome_refs:
            raise ValueError("fixed future holdout membership required")


def reserve_mean_holdout(repo, reservation: ProtectedMeanHoldoutV1) -> str:
    from atlas.v2._serialization import sha256_json
    from atlas.v2.memory.repository import ArtifactIndexEntryV2
    from atlas.v2.science.mean_evaluation import index_mean_artifact

    identity = sha256_json({"first_mean_holdout": reservation.family_ref, "cell": reservation.compatibility_ref})
    with repo.atomic_composition():
        prior = repo.get_artifact(identity)
        if prior is not None:
            if prior.metadata["holdout_ref"] != reservation.content_hash:
                raise ValueError("holdout cannot be rolled, replaced or reset")
            return reservation.content_hash
        index_mean_artifact(repo, reservation, available_at_ns=reservation.reserved_at_ns)
        body = {"holdout_ref": reservation.content_hash, "authority": "ZERO"}
        repo.register_artifact(
            ArtifactIndexEntryV2(
                identity,
                "MeanHoldoutReservationIdentityV1",
                sha256_json(body),
                reservation.reserved_at_ns,
                reservation.reserved_at_ns,
                body,
            )
        )
    return reservation.content_hash


def spend_mean_holdout(
    repo, reservation: ProtectedMeanHoldoutV1, *, protocol: ProgrammeProtocolV1, now_ns: int, look: int
) -> str:
    from atlas.v2._serialization import sha256_json
    from atlas.v2.memory.repository import ArtifactIndexEntryV2

    if (
        protocol.holdout_ref != reservation.content_hash
        or not 1 <= look <= len(protocol.planned_looks_ns)
        or now_ns != protocol.planned_looks_ns[look - 1]
        or now_ns < reservation.end_ns
    ):
        raise ValueError("unplanned/protected holdout access")
    identity = sha256_json({"spent_mean_holdout": reservation.content_hash})
    with repo.atomic_composition():
        if repo.get_artifact(identity) is not None:
            raise ValueError("viewed holdout is spent; redesign requires new future evidence")
        body = {
            "holdout_ref": reservation.content_hash,
            "protocol_ref": protocol.content_hash,
            "look": look,
            "authority": "ZERO",
        }
        repo.register_artifact(
            ArtifactIndexEntryV2(identity, "MeanHoldoutSpentV1", sha256_json(body), now_ns, now_ns, body)
        )
    return identity


def chronological_mean_oof(
    rows,
    *,
    compatibility_ref: str,
    provenance: str,
    embargo_ns: int = DAY_NS,
    embargo_justification_ref: str | None = None,
    minimum_rows: int = 30,
    protected_outcome_refs: tuple[str, ...] = (),
    on_prediction=None,
):
    from atlas.v2.science.mean_models import eligible_mean_rows, fit_m0_mean, predict_mean

    if embargo_ns != DAY_NS and embargo_justification_ref is None:
        raise ValueError("nonbaseline embargo needs preregistration")
    if set(protected_outcome_refs) & {r.outcome_ref for r in rows}:
        raise ValueError("protected holdout cannot enter preprocessing/search/OOF")
    result = []
    for row in sorted(rows, key=lambda r: (r.origin_ns, r.outcome_ref)):
        cutoff = row.origin_ns - embargo_ns
        earlier = eligible_mean_rows(rows, cutoff_ns=cutoff, compatibility_ref=compatibility_ref, provenance=provenance)
        if len(earlier) < minimum_rows:
            continue
        model = fit_m0_mean(
            earlier,
            compatibility_ref=compatibility_ref,
            cutoff_ns=cutoff,
            available_at_ns=cutoff,
            expires_at_ns=row.origin_ns + DAY_NS,
            provenance=provenance,
            minimum_rows=minimum_rows,
        )
        try:
            value = predict_mean(
                model, row.features, compatibility_ref=compatibility_ref, cutoff_ns=row.origin_ns, now_ns=row.origin_ns
            )
        except ValueError:
            continue  # unsupported future cell remains unavailable, never imputed
        if on_prediction is not None:
            on_prediction(row, model, earlier, value)
        result.append((row.outcome_ref, model.content_hash, model.training_refs, value, row.target_cash))
    return tuple(result)


def family_programme_inference(
    comparisons: tuple[tuple[str, tuple[ProgrammeObservationV1, ...], tuple[ProgrammeObservationV1, ...]], ...],
    *,
    protocol: ProgrammeProtocolV1,
    now_ns: int,
    look: int,
    dependence_qualified: bool,
    power_qualified: bool,
) -> dict[str, object]:
    if {name for name, _, _ in comparisons} != set(protocol.family) or len(comparisons) != len(protocol.family):
        raise ValueError("all registered hypotheses, including failed variants, are required")
    if not 1 <= look <= len(protocol.planned_looks_ns) or now_ns != protocol.planned_looks_ns[look - 1]:
        raise ValueError("unplanned family look")
    if not dependence_qualified or not power_qualified:
        return {"status": "NOT ESTIMABLE", "reason": "DEPENDENCE_OR_POWER", "capital_authority": "ZERO"}
    p_values = []
    intervals = {}
    tail_bounds = {}
    for name, a, b in comparisons:
        if any(r.slot_ns < protocol.registered_at_ns or r.label_available_at_ns > now_ns for r in (*a, *b)):
            raise ValueError("future/immature/nonprospective programme evidence")
        for block_ns in protocol.block_sensitivities_ns:
            blocks = paired_programme_blocks(a, b, block_ns=block_ns)
            if len(blocks) < protocol.minimum_blocks:
                return {"status": "NOT ESTIMABLE", "reason": "SENSITIVITY_SUPPORT", "capital_authority": "ZERO"}
        blocks = paired_programme_blocks(a, b, block_ns=protocol.block_ns)
        tail_bounds[name] = max(0.0, -min(blocks))
        observed = statistics.mean(blocks) - float(protocol.materiality_cash)
        centered = tuple(v - statistics.mean(blocks) for v in blocks)
        rng = random.Random(6101)
        count = sum(
            statistics.mean(rng.choice(centered) for _ in blocks) >= observed
            for _ in range(protocol.bootstrap_resamples)
        )
        p_values.append((name, (count + 1) / (protocol.bootstrap_resamples + 1)))
        alpha = look_alpha(protocol.total_alpha, look, len(protocol.family))
        intervals[name] = bootstrap_mean_interval(blocks, resamples=protocol.bootstrap_resamples, alpha=alpha)
    # Holm handles the registered family at the prespecified alpha spend.
    rejected = holm_rejections(tuple(p_values), alpha=protocol.total_alpha / (look * (look + 1)))
    passing = tuple(
        name
        for name in rejected
        if intervals[name][0] > float(protocol.materiality_cash)
        and tail_bounds[name] <= float(protocol.maximum_tail_loss_cash)
        and intervals[name][1] - intervals[name][0] <= float(protocol.maximum_interval_width_cash)
    )
    return {
        "status": "SUPPORTED_OFFLINE_COMPARISON" if passing else "NOT ESTIMABLE",
        "passing": passing,
        "p_values": tuple(p_values),
        "intervals": intervals,
        "capital_authority": "ZERO",
    }


def effective_blocks_for_power(
    *, effect_cash: float, sigma_cash: float, desired_power: float = 0.8, alpha: float = 0.05
) -> int:
    """Known-sigma Gaussian sensitivity only; not a market power certificate."""
    if (
        effect_cash <= 0
        or sigma_cash <= 0
        or not 0.5 < desired_power < 1
        or not 0 < alpha < 0.5
        or not all(math.isfinite(v) for v in (effect_cash, sigma_cash))
    ):
        raise ValueError("invalid materiality/power inputs")
    z = statistics.NormalDist().inv_cdf(1 - alpha) + statistics.NormalDist().inv_cdf(desired_power)
    return math.ceil((z * sigma_cash / effect_cash) ** 2)


def cash_return(*, net_cash: Decimal, capital_cash: Decimal) -> Decimal:
    positive(capital_cash)
    if not net_cash.is_finite():
        raise ValueError("finite programme cash required")
    return net_cash / capital_cash


def capital_time_cash_days(points: tuple[tuple[int, Decimal], ...], *, terminal_ns: int) -> Decimal:
    """Left-continuous locked cash integral in USDT-days; no reinvestment."""
    if (
        not points
        or tuple(sorted({t for t, _ in points})) != tuple(t for t, _ in points)
        or points[-1][0] > terminal_ns
    ):
        raise ValueError("ordered capital-use observations required")
    for _, cash in points:
        nonnegative(cash)
    return sum(
        (
            cash * Decimal(end - start) / DAY_NS
            for (start, cash), end in zip(points, tuple(t for t, _ in points[1:]) + (terminal_ns,), strict=True)
        ),
        ZERO,
    )


@dataclass(frozen=True)
class ResearchExposureV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_RESEARCH_EXPOSURE_PROJECTION_V1"
    instrument_ref: str
    position_epoch: str
    signed_quantity: Decimal
    entry_price: Decimal
    margin_cash: Decimal
    possible_increasing_quantity: Decimal
    reserved_cash: Decimal
    source_ref: str

    def __post_init__(self) -> None:
        super().__post_init__()
        positive(self.entry_price)
        nonnegative(self.margin_cash, self.possible_increasing_quantity, self.reserved_cash)
        if not self.position_epoch:
            raise ValueError("research position epoch required")


def projected_research_lock(exposures: tuple[ResearchExposureV1, ...]) -> Decimal:
    unique(tuple((e.instrument_ref, e.position_epoch) for e in exposures))
    return sum((e.margin_cash + e.reserved_cash for e in exposures), ZERO)


def validate_matched_ledger_start(a: ResearchLedgerV1, b: ResearchLedgerV1, *, protocol: ProgrammeProtocolV1) -> bool:
    return (
        a.arm != b.arm
        and a.equity_cash == b.equity_cash == protocol.starting_capital_cash
        and a.allocation_ceiling_cash == b.allocation_ceiling_cash == protocol.starting_capital_cash
        and a.locked_cash == b.locked_cash == 0
        and not a.opportunity_ids
        and not b.opportunity_ids
    )
