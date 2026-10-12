"""Bounded, read-only point-in-time H1 peer relationship research context."""

from __future__ import annotations

import math
from collections import OrderedDict
from collections.abc import Sequence
from dataclasses import dataclass, replace

from atlas.v2._serialization import sha256_json, sha256_ref, timestamp
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.instruments import InstrumentKeyV2, UniverseContractV2
from atlas.v2.strategies.s6_cross_section import (
    S6_POLICY,
    LiquidityFundingEvidenceV2,
    S6CrossSectionStateV2,
    S6RankRowV2,
    _hourly_returns,
    _same_btc_proxy_cohort,
)

HOUR_NS = BarIntervalV2.H1.duration_ns
METHOD_VERSION = "PEER_RELATIONSHIP_H1_V1"
MAX_LOOKBACK_RETURNS = 720
MIN_COMMON_RETURNS = 24
MAX_CACHE_ENTRIES = 32
DEFAULT_CACHE_TTL_NS = HOUR_NS
MIN_RETURN_VARIANCE = 1e-20
COMMON_FACTOR_CAVEAT = (
    "RAW_PAIR_METRICS_INCLUDE_SHARED_BTC_FACTOR; "
    "BTC_ADJUSTED_CORRELATION_USES_EXISTING_S6_BETAS"
)


def _bar_group(bars: Sequence[CausalBarV2], key: InstrumentKeyV2, *, name: str) -> tuple[CausalBarV2, ...]:
    result = tuple(bars)
    if any(not isinstance(bar, CausalBarV2) for bar in result):
        raise ValueError(f"{name} bars must be CausalBarV2")
    if any(not bar.final for bar in result):
        raise ValueError(f"{name} peer context requires completed H1 bars")
    if any(bar.interval != BarIntervalV2.H1 for bar in result):
        raise ValueError(f"{name} peer context requires H1 bars")
    if any(bar.instrument_revision != key.contract_revision for bar in result):
        raise ValueError(f"{name} bars do not match the exact instrument revision")
    return result


@dataclass(frozen=True)
class PeerRelationshipRequestV1:
    """One explicitly requested pair; this contract cannot create an all-pairs matrix."""

    anchor_key: InstrumentKeyV2
    peer_key: InstrumentKeyV2
    btc_proxy_key: InstrumentKeyV2
    universe: UniverseContractV2
    s6_state: S6CrossSectionStateV2
    anchor_bars: tuple[CausalBarV2, ...]
    peer_bars: tuple[CausalBarV2, ...]
    btc_bars: tuple[CausalBarV2, ...]
    cutoff_ns: int
    computed_at_ns: int
    available_at_ns: int
    lookback_returns: int = MAX_LOOKBACK_RETURNS
    replay_view: str = "ACTUAL_SYSTEM"
    anchor_evidence: LiquidityFundingEvidenceV2 | None = None
    peer_evidence: LiquidityFundingEvidenceV2 | None = None

    def __post_init__(self) -> None:
        if any(not isinstance(key, InstrumentKeyV2) for key in (
            self.anchor_key, self.peer_key, self.btc_proxy_key,
        )):
            raise ValueError("peer context requires exact InstrumentKeyV2 identities")
        if self.anchor_key == self.peer_key:
            raise ValueError("peer context requires two distinct instruments")
        if not isinstance(self.universe, UniverseContractV2):
            raise ValueError("peer context requires the current universe contract")
        if not isinstance(self.s6_state, S6CrossSectionStateV2):
            raise ValueError("peer context requires the existing S6 state")
        for name in ("cutoff_ns", "computed_at_ns", "available_at_ns"):
            timestamp(getattr(self, name), field=f"peer_context.{name}")
        if self.computed_at_ns < self.cutoff_ns or self.available_at_ns < self.computed_at_ns:
            raise ValueError("peer context computation and availability must follow its data cutoff")
        if type(self.lookback_returns) is not int or not MIN_COMMON_RETURNS <= self.lookback_returns <= MAX_LOOKBACK_RETURNS:
            raise ValueError("peer context lookback must be 24..720 completed H1 returns")
        if self.replay_view not in ("ACTUAL_SYSTEM", "RECONSTRUCTED_MARKET"):
            raise ValueError("peer context requires an explicit availability view")
        if self.universe.envelope.available_at_ns > self.cutoff_ns or self.cutoff_ns > self.universe.decision_slot_ns:
            raise ValueError("peer context universe is unavailable at its cutoff")
        if self.s6_state.policy_hash != S6_POLICY.policy_hash:
            raise ValueError("peer context requires the frozen S6 policy")
        if self.s6_state.universe_ref != self.universe.content_hash:
            raise ValueError("peer context S6 state does not match the current universe revision")
        if self.s6_state.cutoff_ns != self.cutoff_ns:
            raise ValueError("peer context S6 state must use the same information cutoff")
        if self.s6_state.replay_view != self.replay_view:
            raise ValueError("peer context S6 state availability view does not match request availability view")
        if self.s6_state.btc_proxy != self.btc_proxy_key:
            raise ValueError("peer context BTC proxy does not match the existing S6 state")
        if self.s6_state.envelope.available_at_ns > self.available_at_ns:
            raise ValueError("peer context cannot precede availability of its S6 beta state")
        if not all(_same_btc_proxy_cohort(key, self.btc_proxy_key)
                   for key in (self.anchor_key, self.peer_key)):
            raise ValueError("peer context requires exact venue/environment/product cohort")

        entries = {entry.key: entry for entry in self.universe.entries}
        for key in (self.anchor_key, self.peer_key, self.btc_proxy_key):
            entry = entries.get(key)
            if entry is None or not entry.data_eligible or entry.capital_eligible:
                raise ValueError("peer context instrument is not eligible in the current universe revision")
        rows = {row.key: row for row in self.s6_state.rows}
        if any(key not in rows for key in (self.anchor_key, self.peer_key, self.btc_proxy_key)):
            raise ValueError("peer context S6 state is missing an exact instrument row")

        object.__setattr__(self, "anchor_bars", _bar_group(self.anchor_bars, self.anchor_key, name="anchor"))
        object.__setattr__(self, "peer_bars", _bar_group(self.peer_bars, self.peer_key, name="peer"))
        object.__setattr__(self, "btc_bars", _bar_group(self.btc_bars, self.btc_proxy_key, name="BTC proxy"))
        for name, key, evidence in (("anchor", self.anchor_key, self.anchor_evidence),
                                    ("peer", self.peer_key, self.peer_evidence)):
            if evidence is not None and (
                evidence.key != key or evidence.observed_at_ns > self.cutoff_ns
                or evidence.available_at_ns > self.cutoff_ns
            ):
                raise ValueError(f"peer context {name} S6 evidence does not match its point-in-time key")


@dataclass(frozen=True)
class PeerRelationshipV1:
    method_version: str
    anchor_key: InstrumentKeyV2
    peer_key: InstrumentKeyV2
    btc_proxy_key: InstrumentKeyV2
    universe_ref: str
    s6_state_ref: str
    s6_policy_hash: str
    interval: str
    lookback_returns: int
    minimum_returns: int
    cutoff_ns: int
    computed_at_ns: int
    available_at_ns: int
    valid_until_ns: int
    data_status: str
    status: str
    reason: str | None
    status_flags: tuple[str, ...]
    common_timestamps_ns: tuple[int, ...]
    sample_count: int
    covariance: float | None
    correlation: float | None
    beta_peer_on_anchor: float | None
    btc_common_timestamps_ns: tuple[int, ...]
    btc_adjusted_sample_count: int
    beta_anchor_btc: float | None
    beta_peer_btc: float | None
    btc_adjusted_correlation: float | None
    btc_adjustment_reason: str | None
    common_factor_caveat: str
    source_evidence_refs: tuple[str, ...]
    source_hash: str
    cache_key: str

    def __post_init__(self) -> None:
        if self.method_version != METHOD_VERSION or self.interval != "1H":
            raise ValueError("unsupported peer relationship method or interval")
        if self.status not in ("AVAILABLE", "NOT_ESTIMABLE"):
            raise ValueError("invalid peer relationship status")
        if self.data_status not in ("ACTUAL_SYSTEM", "RECONSTRUCTED_MARKET"):
            raise ValueError("invalid peer relationship data view")
        if self.sample_count != len(self.common_timestamps_ns):
            raise ValueError("peer relationship timestamp count mismatch")
        if self.btc_adjusted_sample_count != len(self.btc_common_timestamps_ns):
            raise ValueError("peer relationship BTC timestamp count mismatch")
        if self.status == "NOT_ESTIMABLE" and any(
            value is not None for value in (self.covariance, self.correlation, self.beta_peer_on_anchor)
        ):
            raise ValueError("unavailable peer metrics must remain null")
        if self.status == "AVAILABLE" and any(
            value is None for value in (self.covariance, self.correlation, self.beta_peer_on_anchor)
        ):
            raise ValueError("available peer metrics must be complete")
        for value in (self.covariance, self.correlation, self.beta_peer_on_anchor,
                      self.beta_anchor_btc, self.beta_peer_btc, self.btc_adjusted_correlation):
            if value is not None and not math.isfinite(value):
                raise ValueError("peer relationship metrics must be finite or null")
        for ref in (*self.source_evidence_refs, self.source_hash, self.cache_key,
                    self.universe_ref, self.s6_state_ref, self.s6_policy_hash):
            sha256_ref(ref, field="peer_relationship_ref")
        timestamp(self.cutoff_ns, field="peer_relationship.cutoff_ns")
        timestamp(self.computed_at_ns, field="peer_relationship.computed_at_ns")
        timestamp(self.available_at_ns, field="peer_relationship.available_at_ns")
        timestamp(self.valid_until_ns, field="peer_relationship.valid_until_ns")

    @property
    def content_hash(self) -> str:
        return sha256_json(self.to_dict())

    def usable_at(self, decision_cutoff_ns: int) -> bool:
        timestamp(decision_cutoff_ns, field="peer_relationship.decision_cutoff_ns")
        return self.available_at_ns <= decision_cutoff_ns < self.valid_until_ns

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "artifact_type": "PeerRelationshipV1",
            "method_version": self.method_version,
            "anchor_key": self.anchor_key.to_dict(),
            "peer_key": self.peer_key.to_dict(),
            "btc_proxy_key": self.btc_proxy_key.to_dict(),
            "universe_ref": self.universe_ref,
            "s6_state_ref": self.s6_state_ref,
            "s6_policy_hash": self.s6_policy_hash,
            "interval": self.interval,
            "lookback_returns": self.lookback_returns,
            "minimum_returns": self.minimum_returns,
            "cutoff_ns": self.cutoff_ns,
            "computed_at_ns": self.computed_at_ns,
            "available_at_ns": self.available_at_ns,
            "valid_until_ns": self.valid_until_ns,
            "data_status": self.data_status,
            "status": self.status,
            "reason": self.reason,
            "status_flags": list(self.status_flags),
            "common_timestamps_ns": list(self.common_timestamps_ns),
            "sample_count": self.sample_count,
            "covariance": self.covariance,
            "correlation": self.correlation,
            "beta_peer_on_anchor": self.beta_peer_on_anchor,
            "btc_common_timestamps_ns": list(self.btc_common_timestamps_ns),
            "btc_adjusted_sample_count": self.btc_adjusted_sample_count,
            "beta_anchor_btc": self.beta_anchor_btc,
            "beta_peer_btc": self.beta_peer_btc,
            "btc_adjusted_correlation": self.btc_adjusted_correlation,
            "btc_adjustment_reason": self.btc_adjustment_reason,
            "common_factor_caveat": self.common_factor_caveat,
            "source_evidence_refs": list(self.source_evidence_refs),
            "source_hash": self.source_hash,
            "cache_key": self.cache_key,
        }


@dataclass(frozen=True)
class PeerCacheLookupV1:
    status: str
    reason: str | None = None
    relationship: PeerRelationshipV1 | None = None

    def __post_init__(self) -> None:
        if self.status not in ("AVAILABLE", "CACHE_MISS", "STALE_CACHE", "NOT_YET_AVAILABLE"):
            raise ValueError("invalid peer cache lookup status")
        if (self.status == "AVAILABLE") != (self.relationship is not None):
            raise ValueError("peer cache lookup result/status mismatch")


@dataclass(frozen=True)
class _PreparedPeerInputs:
    anchor_returns: dict[int, tuple[float, str, str]]
    peer_returns: dict[int, tuple[float, str, str]]
    btc_returns: dict[int, tuple[float, str, str]]
    common_times: tuple[int, ...]
    btc_times: tuple[int, ...]
    source_refs: tuple[str, ...]
    source_hash: str
    cache_key: str
    anchor_state_row: S6RankRowV2
    peer_state_row: S6RankRowV2
    anchor_state_returns: dict[int, tuple[float, str, str]]
    peer_state_returns: dict[int, tuple[float, str, str]]
    btc_state_returns: dict[int, tuple[float, str, str]]
    anchor_evidence: LiquidityFundingEvidenceV2 | None
    peer_evidence: LiquidityFundingEvidenceV2 | None


def _clip_returns(values: dict[int, tuple[float, str, str]], *, cutoff_ns: int, lookback: int
                  ) -> dict[int, tuple[float, str, str]]:
    end_ns = cutoff_ns - cutoff_ns % HOUR_NS
    start_ns = end_ns - lookback * HOUR_NS
    return {at: item for at, item in values.items() if start_ns < at <= end_ns}


def _prepare(request: PeerRelationshipRequestV1) -> _PreparedPeerInputs:
    view = request.replay_view
    anchor_all = _hourly_returns(request.anchor_bars, key=request.anchor_key,
                                 cutoff_ns=request.cutoff_ns, replay_view=view)
    peer_all = _hourly_returns(request.peer_bars, key=request.peer_key,
                               cutoff_ns=request.cutoff_ns, replay_view=view)
    btc_all = _hourly_returns(request.btc_bars, key=request.btc_proxy_key,
                              cutoff_ns=request.cutoff_ns, replay_view=view)
    anchor_state_returns = _clip_returns(anchor_all, cutoff_ns=request.cutoff_ns,
                                         lookback=MAX_LOOKBACK_RETURNS)
    peer_state_returns = _clip_returns(peer_all, cutoff_ns=request.cutoff_ns,
                                       lookback=MAX_LOOKBACK_RETURNS)
    btc_state_returns = _clip_returns(btc_all, cutoff_ns=request.cutoff_ns,
                                      lookback=MAX_LOOKBACK_RETURNS)
    anchor_returns = _clip_returns(anchor_all, cutoff_ns=request.cutoff_ns, lookback=request.lookback_returns)
    peer_returns = _clip_returns(peer_all, cutoff_ns=request.cutoff_ns, lookback=request.lookback_returns)
    btc_returns = _clip_returns(btc_all, cutoff_ns=request.cutoff_ns, lookback=request.lookback_returns)
    common_times = tuple(sorted(anchor_returns.keys() & peer_returns.keys()))
    btc_times = tuple(at for at in common_times if at in btc_returns)

    anchor_state_row = next(row for row in request.s6_state.rows if row.key == request.anchor_key)
    peer_state_row = next(row for row in request.s6_state.rows if row.key == request.peer_key)
    source_refs = {
        request.universe.content_hash,
        request.s6_state.content_hash,
        *(entry.product_ref for entry in request.universe.entries
          if entry.key in (request.anchor_key, request.peer_key, request.btc_proxy_key)),
    }
    for returns in (anchor_returns, peer_returns, btc_returns):
        for _value, prior_ref, current_ref in returns.values():
            source_refs.add(prior_ref)
            source_refs.add(current_ref)
    for evidence in (request.anchor_evidence, request.peer_evidence):
        if evidence is not None:
            source_refs.update((evidence.content_hash, evidence.liquidity_ref, evidence.funding_ref,
                                evidence.source_health_ref))
            if evidence.turnover_ref is not None:
                source_refs.add(evidence.turnover_ref)
    refs = tuple(sorted(source_refs))
    source_hash = sha256_json(list(refs))
    cache_key = sha256_json({
        "method_version": METHOD_VERSION,
        "anchor_key": request.anchor_key.to_dict(),
        "peer_key": request.peer_key.to_dict(),
        "btc_proxy_key": request.btc_proxy_key.to_dict(),
        "universe_ref": request.universe.content_hash,
        "s6_state_ref": request.s6_state.content_hash,
        "cutoff_ns": request.cutoff_ns,
        "lookback_returns": request.lookback_returns,
        "replay_view": request.replay_view,
        "source_hash": source_hash,
    })
    return _PreparedPeerInputs(anchor_returns, peer_returns, btc_returns, common_times, btc_times,
                               refs, source_hash, cache_key, anchor_state_row, peer_state_row,
                               anchor_state_returns, peer_state_returns, btc_state_returns,
                               request.anchor_evidence, request.peer_evidence)


def _mean(values: Sequence[float]) -> float:
    return math.fsum(values) / len(values)


def _sample_variance(values: Sequence[float], center: float) -> float:
    return math.fsum((value - center) ** 2 for value in values) / (len(values) - 1)


def _has_gap(times: Sequence[int]) -> bool:
    return any(right - left != HOUR_NS for left, right in zip(times, times[1:], strict=False))


def _s6_beta_matches(row: S6RankRowV2, asset_returns: dict[int, tuple[float, str, str]],
                     btc_returns: dict[int, tuple[float, str, str]],
                     evidence: LiquidityFundingEvidenceV2 | None) -> bool:
    available_refs = {ref for returns in (asset_returns, btc_returns)
                      for value in returns.values() for ref in value[1:]}
    evidence_refs: set[str] = set()
    if evidence is not None:
        if row.evidence_ref != evidence.content_hash:
            return False
        evidence_refs.update((evidence.content_hash, evidence.liquidity_ref, evidence.funding_ref,
                              evidence.source_health_ref))
        if evidence.turnover_ref is not None:
            evidence_refs.add(evidence.turnover_ref)
    elif row.evidence_ref is not None:
        return False
    return (set(row.hourly_refs) - evidence_refs).issubset(available_refs)


def build_peer_relationship(request: PeerRelationshipRequestV1) -> PeerRelationshipV1:
    """Build one point-in-time pair result using S6's completed-H1 return selector."""

    prepared = _prepare(request)
    times = prepared.common_times
    anchor = [prepared.anchor_returns[at][0] for at in times]
    peer = [prepared.peer_returns[at][0] for at in times]
    flags: set[str] = set()
    if request.replay_view == "RECONSTRUCTED_MARKET":
        flags.add("RECONSTRUCTED_MARKET_VIEW")

    reason: str | None = None
    covariance: float | None = None
    correlation: float | None = None
    beta_peer_on_anchor: float | None = None
    if not prepared.anchor_returns:
        reason = "ANCHOR_HISTORY_MISSING"
    elif not prepared.peer_returns:
        reason = "PEER_HISTORY_MISSING"
    elif not times:
        reason = "NO_COMMON_UTC_TIMESTAMPS"
    elif _has_gap(times):
        reason = "COMMON_H1_GAP"
        flags.add("COMMON_H1_GAP")
    elif len(times) < MIN_COMMON_RETURNS:
        reason = "INSUFFICIENT_COMMON_HISTORY"
    else:
        anchor_mean = _mean(anchor)
        peer_mean = _mean(peer)
        anchor_variance = _sample_variance(anchor, anchor_mean)
        peer_variance = _sample_variance(peer, peer_mean)
        if not math.isfinite(anchor_variance) or anchor_variance <= MIN_RETURN_VARIANCE:
            reason = "ZERO_VARIANCE_ANCHOR"
        elif not math.isfinite(peer_variance) or peer_variance <= MIN_RETURN_VARIANCE:
            reason = "ZERO_VARIANCE_PEER"
        else:
            covariance_value = math.fsum((a - anchor_mean) * (b - peer_mean)
                                         for a, b in zip(anchor, peer, strict=True)) / (len(times) - 1)
            correlation_value = covariance_value / math.sqrt(anchor_variance * peer_variance)
            beta_value = covariance_value / anchor_variance
            if all(math.isfinite(value) for value in (covariance_value, correlation_value, beta_value)):
                covariance = covariance_value
                correlation = max(-1.0, min(1.0, correlation_value))
                beta_peer_on_anchor = beta_value
            else:
                reason = "NONFINITE_PEER_ESTIMATE"

    if reason is not None:
        flags.add(reason)

    beta_anchor_btc: float | None = None
    beta_peer_btc: float | None = None
    btc_adjusted_correlation: float | None = None
    btc_adjustment_reason: str | None = None
    btc_times = prepared.btc_times
    if not btc_times:
        btc_adjustment_reason = "BTC_TIMESTAMP_MISALIGNMENT"
    elif _has_gap(btc_times):
        btc_adjustment_reason = "BTC_COMMON_H1_GAP"
    elif len(btc_times) < MIN_COMMON_RETURNS:
        btc_adjustment_reason = "BTC_COMMON_HISTORY_INSUFFICIENT"
    elif prepared.anchor_state_row.beta_btc is None or prepared.peer_state_row.beta_btc is None:
        btc_adjustment_reason = "S6_BTC_BETA_UNAVAILABLE"
    elif not _s6_beta_matches(prepared.anchor_state_row, prepared.anchor_state_returns,
                              prepared.btc_state_returns, prepared.anchor_evidence):
        btc_adjustment_reason = "S6_ANCHOR_BETA_SOURCE_MISMATCH"
    elif not _s6_beta_matches(prepared.peer_state_row, prepared.peer_state_returns,
                              prepared.btc_state_returns, prepared.peer_evidence):
        btc_adjustment_reason = "S6_PEER_BETA_SOURCE_MISMATCH"
    elif btc_times != times:
        btc_adjustment_reason = "BTC_TIMESTAMP_MISALIGNMENT"
    else:
        beta_anchor_btc = prepared.anchor_state_row.beta_btc
        beta_peer_btc = prepared.peer_state_row.beta_btc
        btc = [prepared.btc_returns[at][0] for at in btc_times]
        residual_anchor = [a - beta_anchor_btc * b for a, b in zip(anchor, btc, strict=True)]
        residual_peer = [p - beta_peer_btc * b for p, b in zip(peer, btc, strict=True)]
        residual_anchor_mean = _mean(residual_anchor)
        residual_peer_mean = _mean(residual_peer)
        residual_anchor_variance = _sample_variance(residual_anchor, residual_anchor_mean)
        residual_peer_variance = _sample_variance(residual_peer, residual_peer_mean)
        if not math.isfinite(residual_anchor_variance) or residual_anchor_variance <= MIN_RETURN_VARIANCE:
            btc_adjustment_reason = "ZERO_VARIANCE_ANCHOR_RESIDUAL"
        elif not math.isfinite(residual_peer_variance) or residual_peer_variance <= MIN_RETURN_VARIANCE:
            btc_adjustment_reason = "ZERO_VARIANCE_PEER_RESIDUAL"
        else:
            residual_covariance = math.fsum((a - residual_anchor_mean) * (p - residual_peer_mean)
                                            for a, p in zip(residual_anchor, residual_peer, strict=True)) / (
                                                len(btc_times) - 1)
            value = residual_covariance / math.sqrt(residual_anchor_variance * residual_peer_variance)
            if math.isfinite(value):
                btc_adjusted_correlation = max(-1.0, min(1.0, value))
            else:
                btc_adjustment_reason = "NONFINITE_BTC_ADJUSTED_ESTIMATE"

    end_ns = request.cutoff_ns - request.cutoff_ns % HOUR_NS
    latest_pair_timestamp = times[-1] if times else end_ns
    valid_until_ns = min(request.computed_at_ns + DEFAULT_CACHE_TTL_NS, latest_pair_timestamp + HOUR_NS)
    if valid_until_ns <= request.available_at_ns:
        flags.add("EXPIRED_BEFORE_AVAILABLE")
    return PeerRelationshipV1(
        METHOD_VERSION, request.anchor_key, request.peer_key, request.btc_proxy_key,
        request.universe.content_hash, request.s6_state.content_hash, S6_POLICY.policy_hash,
        "1H", request.lookback_returns, MIN_COMMON_RETURNS,
        request.cutoff_ns, request.computed_at_ns, request.available_at_ns, valid_until_ns,
        request.replay_view, "AVAILABLE" if reason is None else "NOT_ESTIMABLE", reason,
        tuple(sorted(flags)), times, len(times), covariance, correlation, beta_peer_on_anchor,
        btc_times, len(btc_times), beta_anchor_btc, beta_peer_btc, btc_adjusted_correlation,
        btc_adjustment_reason, COMMON_FACTOR_CAVEAT, prepared.source_refs,
        prepared.source_hash, prepared.cache_key,
    )


class PeerRelationshipCache:
    """Process-local FIFO cache; restart begins empty and rebuilds from current bar evidence."""

    def __init__(self, *, max_entries: int = MAX_CACHE_ENTRIES, ttl_ns: int = DEFAULT_CACHE_TTL_NS) -> None:
        if type(max_entries) is not int or not 1 <= max_entries <= MAX_CACHE_ENTRIES:
            raise ValueError("peer cache entry limit must be in 1..32")
        if type(ttl_ns) is not int or not 0 < ttl_ns <= HOUR_NS:
            raise ValueError("peer cache TTL must be positive and no longer than one H1 interval")
        self.max_entries = max_entries
        self.ttl_ns = ttl_ns
        self._items: OrderedDict[str, PeerRelationshipV1] = OrderedDict()

    def __len__(self) -> int:
        return len(self._items)

    def compute(self, request: PeerRelationshipRequestV1) -> PeerRelationshipV1:
        relationship = build_peer_relationship(request)
        cache_expiry = request.computed_at_ns + self.ttl_ns
        if cache_expiry < relationship.valid_until_ns:
            relationship = replace(relationship, valid_until_ns=cache_expiry)
        self._items.pop(relationship.cache_key, None)
        self._items[relationship.cache_key] = relationship
        while len(self._items) > self.max_entries:
            self._items.popitem(last=False)
        return relationship

    def lookup(self, request: PeerRelationshipRequestV1, *, decision_cutoff_ns: int) -> PeerCacheLookupV1:
        timestamp(decision_cutoff_ns, field="peer_cache.decision_cutoff_ns")
        key = _prepare(request).cache_key
        relationship = self._items.get(key)
        if relationship is None:
            return PeerCacheLookupV1("CACHE_MISS", "NO_EXACT_SOURCE_KEY")
        if decision_cutoff_ns >= relationship.valid_until_ns:
            return PeerCacheLookupV1("STALE_CACHE", "TTL_OR_NEXT_H1_BOUNDARY_REACHED")
        if decision_cutoff_ns < relationship.available_at_ns:
            return PeerCacheLookupV1("NOT_YET_AVAILABLE", "CACHE_RESULT_AVAILABLE_AFTER_DECISION_CUTOFF")
        return PeerCacheLookupV1("AVAILABLE", relationship=relationship)

    def invalidate_for_completed_h1(self, key: InstrumentKeyV2, bar: CausalBarV2) -> int:
        if bar.interval != BarIntervalV2.H1 or not bar.final or bar.instrument_revision != key.contract_revision:
            raise ValueError("peer cache H1 invalidation requires this exact completed instrument revision")
        return self._invalidate_matching(key, identity_wide=False)

    def invalidate_source_change(self, key: InstrumentKeyV2) -> int:
        return self._invalidate_matching(key, identity_wide=False)

    def invalidate_gap(self, key: InstrumentKeyV2) -> int:
        return self._invalidate_matching(key, identity_wide=False)

    def invalidate_product_revision(self, old_key: InstrumentKeyV2, new_key: InstrumentKeyV2) -> int:
        old_identity = (old_key.venue, old_key.environment, old_key.product, old_key.native_symbol,
                        old_key.base_asset_id, old_key.quote_asset, old_key.settlement_asset)
        new_identity = (new_key.venue, new_key.environment, new_key.product, new_key.native_symbol,
                        new_key.base_asset_id, new_key.quote_asset, new_key.settlement_asset)
        if old_identity != new_identity or old_key.contract_revision == new_key.contract_revision:
            raise ValueError("product revision invalidation requires one instrument with a new revision")
        return self._invalidate_matching(old_key, identity_wide=True)

    def _invalidate_matching(self, key: InstrumentKeyV2, *, identity_wide: bool) -> int:
        target = (key.venue, key.environment, key.product, key.native_symbol,
                  key.base_asset_id, key.quote_asset, key.settlement_asset)
        remove = []
        for cache_key, value in self._items.items():
            keys = (value.anchor_key, value.peer_key, value.btc_proxy_key)
            for candidate in keys:
                exact = candidate == key
                broad = identity_wide and (
                    candidate.venue, candidate.environment, candidate.product, candidate.native_symbol,
                    candidate.base_asset_id, candidate.quote_asset, candidate.settlement_asset,
                ) == target
                if exact or broad:
                    remove.append(cache_key)
                    break
        for cache_key in remove:
            del self._items[cache_key]
        return len(remove)
