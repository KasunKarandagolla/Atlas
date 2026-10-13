"""Causal readers for registered, indexed production feature evidence."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any

from atlas.v2._serialization import canonical_json, json_value, sha256_json, timestamp
from atlas.v2.contracts import FeatureArtifactV2, FeatureValueV2, ReplayViewV2
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.health import PublicSourceHealthV2, PublicSourceStateV2
from atlas.v2.data.raw import AvailabilityClassV2, RawObservationV2
from atlas.v2.features.pipeline import GAP_QUALIFIED_FEATURE_SET_VERSIONS
from atlas.v2.features.technical import interval_coverage_complete, technical_series
from atlas.v2.instruments import InstrumentKeyV2, UniverseContractV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.research.decision_brief import BriefFactV1, CandidateSetV2, FactProvenanceV1, technical_fact

FEATURE_VALUE_TRANSFORMATION = "FEATURE_VALUES_V1"
MAX_FEATURES_PER_KEY = 16
MAX_FEATURE_INPUT_REFS = 512
MAX_M15_BARS = 256
FEATURE_VALIDITY_NS = 3_600_000_000_000

# These fields do not depend on the optional exact-prefix EMA/ATR checkpoint.
# They are reproduced from the indexed final M15 bars by TECHNICAL_V1.
TECHNICAL_FEATURES = ("m15.rsi14", "m15.roc10", "m15.adx14")
_UNITS = {"rsi14": "index", "roc10": "fraction", "adx14": "index"}


@dataclass(frozen=True)
class VerifiedProductionFeatureFactsV1:
    """Same-writer proof that the feature facts were reconstructed from indexed sources."""

    repository_identity: int
    cutoff_ns: int
    fact_hashes: tuple[str, ...]

    def verifies(self, fact: BriefFactV1, *, repository: OpsRepository, cutoff_ns: int) -> bool:
        return (
            self.repository_identity == id(repository)
            and self.cutoff_ns == cutoff_ns
            and sha256_json(fact.to_dict()) in self.fact_hashes
        )


def _indexed(repo: OpsRepository, ref: str, cutoff_ns: int) -> ArtifactIndexEntryV2 | None:
    entry = repo.get_artifact(ref)
    effective = repo.effective_available_at_ns(ref)
    if entry is None or entry.available_at_ns > cutoff_ns or effective is None or effective > cutoff_ns:
        return None
    return entry


def _read_feature(entry: ArtifactIndexEntryV2) -> FeatureArtifactV2 | None:
    raw = entry.metadata.get("feature")
    if (
        entry.artifact_type != FeatureArtifactV2.ARTIFACT_TYPE
        or entry.artifact_ref != entry.content_hash
        or not isinstance(raw, Mapping)
    ):
        return None
    try:
        feature = FeatureArtifactV2.from_dict(json_value(raw))
    except (KeyError, TypeError, ValueError):
        return None
    if (
        feature.content_hash != entry.artifact_ref
        or entry.content_hash != feature.content_hash
        or entry.created_at_ns != feature.envelope.created_at_ns
        or entry.available_at_ns != feature.envelope.available_at_ns
        or feature.feature_set_version not in GAP_QUALIFIED_FEATURE_SET_VERSIONS
        or feature.replay_view != ReplayViewV2.ACTUAL_SYSTEM
    ):
        return None
    return feature


def _read_health(
    repo: OpsRepository, feature: FeatureArtifactV2, *, cutoff_ns: int
) -> tuple[PublicSourceHealthV2 | None, tuple[str, ...]]:
    entry = _indexed(repo, feature.source_health_ref, cutoff_ns)
    raw = entry.metadata.get("health") if entry is not None else None
    if entry is None or entry.artifact_type != "PublicSourceHealthV2" or not isinstance(raw, Mapping):
        return None, ("SOURCE_HEALTH_INDEX_MISSING_OR_FUTURE",)
    try:
        health = PublicSourceHealthV2.from_dict(json_value(raw))
    except (TypeError, ValueError):
        return None, ("SOURCE_HEALTH_TYPED_BODY_INVALID",)
    if (
        health.content_hash != feature.source_health_ref
        or health.content_hash != entry.content_hash
        or entry.artifact_ref != health.content_hash
        or entry.available_at_ns != health.available_at_ns
        or health.observed_at_ns > cutoff_ns
        or health.available_at_ns > cutoff_ns
    ):
        return None, ("SOURCE_HEALTH_IDENTITY_OR_CHRONOLOGY",)

    page = repo.latest_artifact_entries(
        "PublicSourceHealthV2",
        as_of_ns=cutoff_ns,
        limit=128,
        metadata_path=("health", "source_id"),
        identity_value=health.source_id,
    )
    if page.invalid_entry_count or not page.entries:
        return health, ("SOURCE_HEALTH_LATEST_STATE_UNAVAILABLE",)
    latest: PublicSourceHealthV2 | None = None
    failures: list[PublicSourceHealthV2] = []
    for item in page.entries:
        body = item.metadata.get("health")
        try:
            state = PublicSourceHealthV2.from_dict(json_value(body)) if isinstance(body, Mapping) else None
        except (TypeError, ValueError):
            state = None
        if state is None or state.content_hash != item.content_hash or item.artifact_ref != state.content_hash:
            return health, ("SOURCE_HEALTH_LATEST_STATE_INVALID",)
        if latest is None:
            latest = state
        if state.observed_at_ns >= feature.information_cutoff_ns and not state.data_eligible:
            failures.append(state)
    if (
        latest is None
        or page.has_more
        and min(item.available_at_ns for item in page.entries) > feature.envelope.available_at_ns
    ):
        return health, ("SOURCE_HEALTH_HISTORY_INCOMPLETE",)
    if latest.state != PublicSourceStateV2.HEALTHY_CURRENT:
        return health, ("SOURCE_HEALTH_NOT_CURRENT",)
    if cutoff_ns - latest.observed_at_ns > FEATURE_VALIDITY_NS:
        return health, ("SOURCE_HEALTH_STALE",)
    if failures:
        return health, ("SOURCE_HEALTH_FAILED_AFTER_FEATURE_CUTOFF",)
    if health.state != PublicSourceStateV2.HEALTHY_CURRENT:
        return health, ("FEATURE_SOURCE_HEALTH_NOT_ELIGIBLE",)
    return health, ()


def _read_m15_bar(
    ref: str,
    *,
    key: InstrumentKeyV2,
    cutoff_ns: int,
    indexed_inputs: Mapping[str, Mapping[str, Any]],
    indexed_observations: Mapping[str, Mapping[str, Any]],
) -> tuple[CausalBarV2, str] | None:
    entry = indexed_inputs.get(ref)
    metadata = entry.get("metadata", {}) if entry is not None else {}
    raw_bar = metadata.get("bar") if isinstance(metadata, Mapping) else None
    observation_ref = metadata.get("source_observation_ref") if isinstance(metadata, Mapping) else None
    if (
        entry is None
        or entry.get("artifact_type") != "CausalBarV2"
        or entry.get("content_hash") != ref
        or entry.get("available_at_ns", cutoff_ns + 1) > cutoff_ns
        or entry.get("effective_available_at_ns") is None
        or entry.get("effective_available_at_ns", cutoff_ns + 1) > cutoff_ns
        or not isinstance(raw_bar, Mapping)
        or not isinstance(observation_ref, str)
        or sha256_json(raw_bar) != ref
    ):
        return None
    observation = indexed_observations.get(observation_ref)
    meta = observation.get("metadata", {}) if observation is not None else {}
    if not isinstance(meta, Mapping):
        return None
    availability_class = meta.get("availability_class")
    interval_value = raw_bar.get("interval")
    if not isinstance(availability_class, str) or not isinstance(interval_value, str):
        return None
    try:
        record_id = raw_bar["record_id"]
        close_at_ns = raw_bar["close_at_ns"]
        available_at_ns = observation.get("available_at_ns", 0) if observation is not None else 0
        received_at_ns = observation.get("created_at_ns", 0) if observation is not None else 0
        raw = RawObservationV2(
            record_id=record_id,
            instrument_revision=str(meta.get("instrument_revision", "")),
            source_id=str(meta.get("source_id", "")),
            event_type=str(meta.get("event_type", "")),
            event_at_ns=meta.get("event_at_ns"),
            published_at_ns=meta.get("published_at_ns"),
            received_at_ns=received_at_ns,
            ingested_at_ns=max(received_at_ns, available_at_ns),
            available_at_ns=available_at_ns,
            raw_payload_hash=str(meta.get("raw_payload_hash", "")),
            translation_version=str(meta.get("translation_version", "INDEXED_CAUSAL_BAR_V1")),
            revision_of=meta.get("revision_of"),
            quality_flags=tuple(meta.get("quality_flags", ())),
            availability_class=AvailabilityClassV2(availability_class),
            sequence=None,
            replay_available_at_ns=meta.get("replay_available_at_ns"),
        )
        bar = CausalBarV2(
            raw,
            BarIntervalV2(interval_value),
            int(raw_bar["open_at_ns"]),
            int(close_at_ns),
            Decimal(str(raw_bar["open"])),
            Decimal(str(raw_bar["high"])),
            Decimal(str(raw_bar["low"])),
            Decimal(str(raw_bar["close"])),
            Decimal(str(raw_bar["volume"])),
            raw_bar["final"],
        )
    except (KeyError, TypeError, ValueError, ArithmeticError):
        return None
    if (
        observation is None
        or observation.get("artifact_type") != "PublicObservationIndexV2"
        or observation_ref != sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": bar.raw.record_id})
        or meta.get("record_id") != bar.raw.record_id
        or meta.get("bar_content_hash") != bar.content_hash
        or meta.get("instrument_revision") != key.contract_revision
        or meta.get("instrument_key_json") != key.to_canonical_json()
        or meta.get("event_type") != "BAR_15M"
        or meta.get("event_at_ns") != bar.close_at_ns
        or meta.get("availability_class") != AvailabilityClassV2.ACTUAL_SYSTEM.value
        or bar.interval != BarIntervalV2.M15
        or bar.instrument_revision != key.contract_revision
        or bar.raw.instrument_revision != key.contract_revision
        or bar.raw.event_type != "BAR_15M"
        or bar.raw.availability_class != AvailabilityClassV2.ACTUAL_SYSTEM
        or not bar.final
        or bar.close_at_ns > cutoff_ns
        or bar.raw.available_at_ns > cutoff_ns
        or observation.get("effective_available_at_ns") is None
        or observation.get("effective_available_at_ns", cutoff_ns + 1) > cutoff_ns
        or observation.get("available_at_ns", cutoff_ns + 1) < bar.raw.available_at_ns
    ):
        return None
    return bar, observation_ref


def _causal_feature_inputs(
    repo: OpsRepository,
    feature: FeatureArtifactV2,
    *,
    cutoff_ns: int,
    check_budget: Callable[[], None] | None = None,
) -> tuple[tuple[CausalBarV2, ...], tuple[str, ...]]:
    if check_budget is not None:
        check_budget()
    if (
        feature.information_cutoff_ns > cutoff_ns
        or feature.confirmed_at_ns > cutoff_ns
        or feature.envelope.available_at_ns > cutoff_ns
        or cutoff_ns
        >= min(
            feature.information_cutoff_ns + FEATURE_VALIDITY_NS,
            feature.envelope.available_at_ns + FEATURE_VALIDITY_NS,
        )
    ):
        return (), ("FEATURE_CUTOFF_OR_FRESHNESS",)
    if not feature.envelope.input_refs or feature.source_health_ref not in feature.envelope.input_refs:
        return (), ("FEATURE_SOURCE_GRAPH_INCOMPLETE",)
    if len(feature.envelope.input_refs) > MAX_FEATURE_INPUT_REFS:
        return (), ("FEATURE_SOURCE_REFERENCE_LIMIT",)
    indexed_inputs = repo.get_artifact_metadata_by_refs(feature.envelope.input_refs)
    if check_budget is not None:
        check_budget()
    if len(indexed_inputs) != len(feature.envelope.input_refs) or any(
        item.get("available_at_ns", cutoff_ns + 1) > cutoff_ns
        or item.get("effective_available_at_ns") is None
        or item.get("effective_available_at_ns", cutoff_ns + 1) > cutoff_ns
        for item in indexed_inputs.values()
    ):
        return (), ("FEATURE_INPUT_MISSING_OR_FUTURE",)
    health, health_reasons = _read_health(repo, feature, cutoff_ns=cutoff_ns)
    if health is None or health_reasons or not health.data_eligible:
        return (), health_reasons or ("SOURCE_HEALTH_NOT_CURRENT",)

    bars: list[CausalBarV2] = []
    obs_refs: dict[int, str] = {}
    bar_source_refs: set[str] = set()
    for item in indexed_inputs.values():
        metadata = item.get("metadata", {})
        raw = metadata.get("bar") if isinstance(metadata, Mapping) else None
        if (
            item.get("artifact_type") == "CausalBarV2"
            and isinstance(raw, Mapping)
            and raw.get("interval") == BarIntervalV2.M15.value
            and isinstance(metadata.get("source_observation_ref"), str)
        ):
            bar_source_refs.add(metadata["source_observation_ref"])
    indexed_observations = repo.get_artifact_metadata_by_refs(tuple(sorted(bar_source_refs)))
    if check_budget is not None:
        check_budget()
    if len(indexed_observations) != len(bar_source_refs):
        return (), ("FEATURE_BAR_SOURCE_OBSERVATION_MISSING",)
    for ref in feature.envelope.input_refs:
        if check_budget is not None:
            check_budget()
        entry = indexed_inputs.get(ref)
        metadata = entry.get("metadata", {}) if entry is not None else {}
        if entry is None or entry.get("artifact_type") != "CausalBarV2":
            continue
        raw = metadata.get("bar") if isinstance(metadata, Mapping) else None
        if not isinstance(raw, Mapping):
            continue
        interval_value = raw.get("interval")
        if not isinstance(interval_value, str):
            continue
        try:
            interval = BarIntervalV2(interval_value)
        except (TypeError, ValueError):
            continue
        if interval != BarIntervalV2.M15:
            continue
        resolved = _read_m15_bar(
            ref,
            key=feature.key,
            cutoff_ns=cutoff_ns,
            indexed_inputs=indexed_inputs,
            indexed_observations=indexed_observations,
        )
        if resolved is None:
            return (), ("FEATURE_BAR_IDENTITY_REVISION_OR_FINALITY",)
        bar, observation_ref = resolved
        bars.append(bar)
        obs_refs[bar.close_at_ns] = observation_ref
    bars.sort(key=lambda item: (item.open_at_ns, item.content_hash))
    if not bars or len(bars) > MAX_M15_BARS:
        return (), ("FEATURE_COMPLETED_M15_BARS_MISSING_OR_OVERFLOW",)
    try:
        gap = ("INTERVAL_COVERAGE_GAP",) if not interval_coverage_complete(bars) else ()
    except ValueError:
        return (), ("FEATURE_BAR_ORDER_OR_INTERVAL_INVALID",)

    revisions_by_close = repo.confirmed_bar_observations_for_closes(
        feature.key,
        event_type="BAR_15M",
        close_times_ns=tuple(bar.close_at_ns for bar in bars),
        as_of_ns=cutoff_ns,
        limit_per_close=128,
    )
    if check_budget is not None:
        check_budget()
    for bar in bars:
        if check_budget is not None:
            check_budget()
        eligible_revisions = revisions_by_close[bar.close_at_ns]
        latest_ref = (
            max(
                eligible_revisions,
                key=lambda item: (
                    item.available_at_ns,
                    str(item.metadata.get("record_id", "")),
                    item.artifact_ref,
                ),
            ).artifact_ref
            if eligible_revisions
            else None
        )
        if latest_ref != obs_refs[bar.close_at_ns]:
            return (), ("FEATURE_BAR_REVISED_AFTER_SOURCE_SNAPSHOT",)
    return tuple(bars), gap


def read_production_feature_facts(
    repo: OpsRepository,
    candidate_set: CandidateSetV2,
    universe: UniverseContractV2,
    *,
    cutoff_ns: int,
    check_budget: Callable[[], None] | None = None,
) -> tuple[
    tuple[BriefFactV1, ...],
    tuple[str, ...],
    tuple[str, ...],
    VerifiedProductionFeatureFactsV1 | None,
]:
    """Read an exact selected-A0-key M15 technical projection, without ranking."""
    timestamp(cutoff_ns, field="production feature cutoff")
    if candidate_set.selected_candidate_id is None:
        return (), ("NO_SELECTED_A0_CANDIDATE",), (), None
    selected = next(
        (item for item in candidate_set.candidates if item.candidate_id == candidate_set.selected_candidate_id),
        None,
    )
    if selected is None or selected.key not in {entry.key for entry in universe.entries}:
        return (), ("SELECTED_A0_PRODUCT_NOT_IN_UNIVERSE",), (), None
    page = repo.latest_artifact_entries(
        FeatureArtifactV2.ARTIFACT_TYPE,
        as_of_ns=cutoff_ns,
        limit=MAX_FEATURES_PER_KEY,
        metadata_path=("feature", "key"),
        identity_value=canonical_json(selected.key.to_dict()),
    )
    if check_budget is not None:
        check_budget()
    if page.has_more:
        return (), ("FEATURE_INDEX_PAGE_OVERFLOW",), (), None
    if page.invalid_entry_count:
        return (), ("FEATURE_INDEX_CONTENT_INVALID",), (), None

    rejection: set[str] = set()
    feature: FeatureArtifactV2 | None = None
    bars: tuple[CausalBarV2, ...] = ()
    for entry in page.entries:
        if check_budget is not None:
            check_budget()
        candidate_feature = _read_feature(entry)
        if candidate_feature is None:
            rejection.add("FEATURE_TYPED_IDENTITY_UNSUPPORTED")
            continue
        if candidate_feature.key != selected.key:
            rejection.add("FEATURE_PRODUCT_REVISION")
            continue
        candidate_bars, reasons = _causal_feature_inputs(
            repo,
            candidate_feature,
            cutoff_ns=cutoff_ns,
            check_budget=check_budget,
        )
        if reasons and reasons != ("INTERVAL_COVERAGE_GAP",):
            rejection.update(reasons)
            continue
        feature, bars = candidate_feature, candidate_bars
        break
    if feature is None:
        return (), tuple(sorted(rejection or {"NO_QUALIFIED_FEATURE_AT_CUTOFF"})), (), None

    # Reproduce the registered TECHNICAL_V1 end-of-window outputs from the
    # exact archived bars. Unavailable warmup/gap values remain explicit.
    if check_budget is not None:
        check_budget()
    series = technical_series(bars)
    if check_budget is not None:
        check_budget()
    projected: dict[str, FeatureValueV2] = {}
    missing: set[str] = set()
    for full_name in TECHNICAL_FEATURES:
        _, short_name = full_name.split(".", 1)
        source_value = feature.values.get(full_name)
        recomputed = series[-1][short_name]
        if source_value is None:
            return (), ("REGISTERED_FEATURE_VALUE_MISSING",), (), None
        expected = FeatureValueV2(
            recomputed,
            _UNITS[short_name],
            None if recomputed is not None else "INSUFFICIENT_HISTORY_OR_MISSING_TIMEFRAME",
        )
        # A feature snapshot that withheld values for a detected gap is
        # retained as missingness. A snapshot that filled over a gap is rejected.
        if not interval_coverage_complete(bars):
            expected = FeatureValueV2(None, _UNITS[short_name], "INTERVAL_COVERAGE_GAP")
        if source_value != expected:
            return (), ("FEATURE_VALUE_REPRODUCTION_MISMATCH",), (), None
        projected[full_name] = source_value
        if source_value.value is None:
            missing.add(source_value.missing_reason or "FEATURE_VALUE_MISSING")

    valid_until = min(
        feature.information_cutoff_ns + FEATURE_VALIDITY_NS,
        feature.envelope.available_at_ns + FEATURE_VALIDITY_NS,
    )
    base = technical_fact(
        key=feature.key,
        source_ref=feature.content_hash,
        bars=bars,
        cutoff_ns=cutoff_ns,
        valid_until_ns=valid_until,
        values={name: value.to_dict() for name, value in projected.items()},
    )
    fact_missing = set(base.missingness) | missing
    if any(value.missing_reason == "INTERVAL_COVERAGE_GAP" for value in projected.values()):
        fact_missing.add("INTERVAL_COVERAGE_GAP")
    fact = replace(
        base,
        observed_at_ns=max(bar.close_at_ns for bar in bars),
        available_at_ns=feature.envelope.available_at_ns,
        valid_until_ns=valid_until,
        status="NOT_ESTIMABLE" if fact_missing else "AVAILABLE",
        missingness=tuple(sorted(fact_missing)),
        provenance=FactProvenanceV1(
            FeatureArtifactV2.ARTIFACT_TYPE,
            FEATURE_VALUE_TRANSFORMATION,
            feature.information_cutoff_ns,
            feature.feature_set_version,
            TECHNICAL_FEATURES,
            feature.key,
            "LOCAL_MARKET",
        ),
    )
    if check_budget is not None:
        check_budget()
    refs = tuple(sorted({feature.content_hash, feature.source_health_ref, *feature.envelope.input_refs}))
    proof = VerifiedProductionFeatureFactsV1(id(repo), cutoff_ns, (sha256_json(fact.to_dict()),))
    return (fact,), (), refs, proof


def verify_feature_fact_source(
    fact: BriefFactV1,
    *,
    request_cutoff_ns: int,
    candidate_set: CandidateSetV2,
    universe: UniverseContractV2,
    repo: OpsRepository,
) -> bool:
    p = fact.provenance
    if (
        p is None
        or fact.section != "technical"
        or p.version != "ATLAS_BRIEF_FACT_PROVENANCE_V1"
        or p.source_type != FeatureArtifactV2.ARTIFACT_TYPE
        or p.transformation != FEATURE_VALUE_TRANSFORMATION
        or p.role != "LOCAL_MARKET"
        or p.feature_ids != TECHNICAL_FEATURES
        or p.execution_key not in {entry.key for entry in universe.entries}
        or candidate_set.selected_candidate_id is None
    ):
        return False
    selected = next(
        (item for item in candidate_set.candidates if item.candidate_id == candidate_set.selected_candidate_id),
        None,
    )
    if selected is None or selected.key != p.execution_key or fact.key != selected.key:
        return False
    entry = _indexed(repo, fact.source_ref, request_cutoff_ns)
    feature = _read_feature(entry) if entry is not None else None
    if (
        feature is None
        or feature.key != selected.key
        or p.feature_set_version != feature.feature_set_version
        or p.source_cutoff_ns != feature.information_cutoff_ns
    ):
        return False
    bars, reasons = _causal_feature_inputs(repo, feature, cutoff_ns=request_cutoff_ns)
    if (reasons and reasons != ("INTERVAL_COVERAGE_GAP",)) or not bars:
        return False
    series = technical_series(bars)
    expected_features: dict[str, dict[str, Any]] = {}
    expected_missing: set[str] = set()
    for full_name in TECHNICAL_FEATURES:
        _, short_name = full_name.split(".", 1)
        value = feature.values.get(full_name)
        recomputed = series[-1][short_name]
        expected = FeatureValueV2(
            recomputed,
            _UNITS[short_name],
            None if recomputed is not None else "INSUFFICIENT_HISTORY_OR_MISSING_TIMEFRAME",
        )
        if value is None:
            return False
        if reasons == ("INTERVAL_COVERAGE_GAP",):
            expected = FeatureValueV2(None, _UNITS[short_name], "INTERVAL_COVERAGE_GAP")
        if value != expected:
            return False
        expected_features[full_name] = value.to_dict()
        if value.missing_reason:
            expected_missing.add(value.missing_reason)
    if not interval_coverage_complete(bars):
        expected_missing.add("SPARSE_BAR_FEATURE_UNQUALIFIED")
        expected_missing.add("INTERVAL_COVERAGE_GAP")
        expected_features = {}
    expected_status = "NOT_ESTIMABLE" if expected_missing else "AVAILABLE"
    valid_until = min(
        feature.information_cutoff_ns + FEATURE_VALIDITY_NS,
        feature.envelope.available_at_ns + FEATURE_VALIDITY_NS,
    )
    values = fact.values.to_dict()
    return bool(
        fact.observed_at_ns == max(bar.close_at_ns for bar in bars)
        and fact.available_at_ns == feature.envelope.available_at_ns
        and fact.valid_until_ns == valid_until
        and fact.status == expected_status
        and fact.missingness == tuple(sorted(expected_missing))
        and canonical_json(values.get("features", {})) == canonical_json(expected_features)
        and tuple(values.get("input_bar_refs", ())) == tuple(bar.content_hash for bar in bars)
        and values.get("feature_version") == "TECHNICAL_V1"
        and values.get("qualification") == "GAP_CHECK_ONLY"
    )
