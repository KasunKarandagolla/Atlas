"""R3A completion regressions for causal production facts and bounded memory."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import canonical_json, json_value, sha256_json
from atlas.v2.contracts import CandidateSetV2
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.health import PublicSourceHealthV2, PublicSourceStateV2
from atlas.v2.data.raw import RawObservationV2
from atlas.v2.features.joins import JoinedBars
from atlas.v2.features.pipeline import feature_snapshot
from atlas.v2.instruments import UniverseContractV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.research.memory import ResearchMemoryV1
from atlas.v2.research.production_facts import read_production_feature_facts
from atlas.v2.runtime import production, production_intelligence
from atlas.v2.runtime.ops_supervisor import OpsSupervisorV2

from .test_session014_core import KEY, bar
from .test_session016_candidate_selection import CUTOFF
from .test_session027_ops_supervisor import FakeClock
from .test_session027_production import (
    ReconciledFixturePublicSource,
    StaticInputsProvider,
    _production_event,
)

_M15 = BarIntervalV2.M15.duration_ns


def _register_feature_source(
    repo: OpsRepository,
    *,
    cutoff_ns: int,
    gap_interval: BarIntervalV2 | None = None,
    persist_feature: bool = True,
    future_index_available_at_ns: int | None = None,
    corrupt_feature_body: bool = False,
    m15_count: int = 56,
    feature_key=KEY,
    h1_count: int = 56,
    h4_count: int = 56,
    auxiliary_input_ref_count: int = 0,
    unfinalized_m15: bool = False,
    later_health_state: PublicSourceStateV2 | None = None,
):
    health = PublicSourceHealthV2(
        "fixture-public",
        cutoff_ns,
        cutoff_ns,
        PublicSourceStateV2.HEALTHY_CURRENT,
        sha256_json({"source_health": cutoff_ns}),
        "causal feature reader fixture",
    )
    repo.register_artifact(
        ArtifactIndexEntryV2(
            health.content_hash,
            "PublicSourceHealthV2",
            health.content_hash,
            cutoff_ns,
            cutoff_ns,
            {"health": health.to_dict()},
        )
    )
    frames = {}
    refs = {health.content_hash}
    for interval in BarIntervalV2:
        end_index = cutoff_ns // interval.duration_ns - 1
        count = {
            BarIntervalV2.M15: m15_count,
            BarIntervalV2.H1: h1_count,
            BarIntervalV2.H4: h4_count,
        }.get(interval, 56)
        selected = [bar(i, interval=interval, key=feature_key) for i in range(end_index - count + 1, end_index + 1)]
        if interval == gap_interval:
            selected.pop(23)
        frames[interval] = tuple(selected)
        for item in selected:
            observation_ref = sha256_json(
                {
                    "artifact_type": "PublicObservationIndexV2",
                    "record_id": item.raw.record_id,
                }
            )
            observation = {
                "record_id": item.raw.record_id,
                "source_id": item.raw.source_id,
                "event_type": item.raw.event_type,
                "instrument_revision": item.instrument_revision,
                "instrument_key_json": KEY.to_canonical_json(),
                "event_at_ns": item.raw.event_at_ns,
                "published_at_ns": item.raw.published_at_ns,
                "translation_version": item.raw.translation_version,
                "revision_of": item.raw.revision_of,
                "quality_flags": list(item.raw.quality_flags),
                "availability_class": item.raw.availability_class.value,
                "replay_available_at_ns": item.raw.replay_available_at_ns,
                "raw_payload_hash": item.raw.raw_payload_hash,
                "bar_content_hash": item.content_hash,
            }
            repo.register_artifact(
                ArtifactIndexEntryV2(
                    observation_ref,
                    "PublicObservationIndexV2",
                    item.raw.content_hash,
                    item.raw.received_at_ns,
                    item.raw.available_at_ns,
                    observation,
                )
            )
            repo.register_artifact(
                ArtifactIndexEntryV2(
                    item.content_hash,
                    "CausalBarV2",
                    item.content_hash,
                    item.raw.available_at_ns,
                    item.raw.available_at_ns,
                    {"bar": item.to_dict(), "source_observation_ref": observation_ref},
                )
            )
            refs.add(item.content_hash)
    joined = JoinedBars(
        feature_key,
        cutoff_ns,
        frames[BarIntervalV2.H4],
        frames[BarIntervalV2.H1],
        frames[BarIntervalV2.M15],
        "AVAILABLE",
        None,
        health.content_hash,
    )
    if later_health_state is not None:
        later_health = PublicSourceHealthV2(
            health.source_id,
            cutoff_ns + _M15,
            cutoff_ns + _M15,
            later_health_state,
            sha256_json({"source_health_later": later_health_state.value, "cutoff": cutoff_ns + _M15}),
            "later source-health transition fixture",
        )
        repo.register_artifact(
            ArtifactIndexEntryV2(
                later_health.content_hash,
                "PublicSourceHealthV2",
                later_health.content_hash,
                later_health.available_at_ns,
                later_health.available_at_ns,
                {"health": later_health.to_dict()},
            )
        )
    feature = feature_snapshot(joined)
    if unfinalized_m15:
        final_bar = frames[BarIntervalV2.M15][-1]
        raw = RawObservationV2.build(
            instrument_revision=feature_key.contract_revision,
            source_id=final_bar.raw.source_id,
            event_type="BAR_15M",
            received_at_ns=final_bar.raw.available_at_ns,
            ingested_at_ns=final_bar.raw.available_at_ns,
            available_at_ns=final_bar.raw.available_at_ns,
            translation_version="fixture-unfinalized-v1",
            payload={"unfinalized_revision_of": final_bar.raw.record_id},
            event_at_ns=final_bar.close_at_ns,
            revision_of=final_bar.raw.record_id,
        )
        unfinalized_bar = CausalBarV2(
            raw,
            BarIntervalV2.M15,
            final_bar.open_at_ns,
            final_bar.close_at_ns,
            final_bar.open,
            final_bar.high,
            final_bar.low,
            final_bar.close,
            final_bar.volume,
            False,
        )
        observation_ref = sha256_json({"artifact_type": "PublicObservationIndexV2", "record_id": raw.record_id})
        observation = {
            "record_id": raw.record_id,
            "source_id": raw.source_id,
            "event_type": raw.event_type,
            "instrument_revision": raw.instrument_revision,
            "instrument_key_json": feature_key.to_canonical_json(),
            "event_at_ns": raw.event_at_ns,
            "published_at_ns": raw.published_at_ns,
            "translation_version": raw.translation_version,
            "revision_of": raw.revision_of,
            "quality_flags": list(raw.quality_flags),
            "availability_class": raw.availability_class.value,
            "replay_available_at_ns": raw.replay_available_at_ns,
            "raw_payload_hash": raw.raw_payload_hash,
            "bar_content_hash": unfinalized_bar.content_hash,
        }
        repo.register_artifact(
            ArtifactIndexEntryV2(
                observation_ref,
                "PublicObservationIndexV2",
                raw.content_hash,
                raw.received_at_ns,
                raw.available_at_ns,
                observation,
            )
        )
        repo.register_artifact(
            ArtifactIndexEntryV2(
                unfinalized_bar.content_hash,
                "CausalBarV2",
                unfinalized_bar.content_hash,
                raw.available_at_ns,
                raw.available_at_ns,
                {"bar": unfinalized_bar.to_dict(), "source_observation_ref": observation_ref},
            )
        )
        feature_inputs = set(feature.envelope.input_refs)
        feature_inputs.discard(final_bar.content_hash)
        feature_inputs.add(unfinalized_bar.content_hash)
        feature = replace(
            feature,
            envelope=replace(
                feature.envelope,
                content_hash="",
                input_refs=tuple(sorted(feature_inputs)),
            ),
        )
    if auxiliary_input_ref_count:
        auxiliary_refs = []
        for index in range(auxiliary_input_ref_count):
            body = {"version": "feature-aux-fixture-v1", "index": index, "authority": "ZERO"}
            ref = sha256_json(body)
            repo.register_artifact(
                ArtifactIndexEntryV2(
                    ref,
                    "FeatureAuxEvidenceV1",
                    ref,
                    cutoff_ns,
                    cutoff_ns,
                    body,
                )
            )
            auxiliary_refs.append(ref)
        feature = replace(
            feature,
            envelope=replace(
                feature.envelope,
                content_hash="",
                input_refs=tuple(sorted({*feature.envelope.input_refs, *auxiliary_refs})),
            ),
        )
    if persist_feature:
        metadata = {"feature": feature.to_dict()}
        if corrupt_feature_body:
            corrupted = feature.to_dict()
            corrupted["feature_set_version"] = "UNREGISTERED_FEATURE_VERSION"
            metadata = {"feature": corrupted}
        repo.register_artifact(
            ArtifactIndexEntryV2(
                feature.content_hash,
                "FeatureArtifactV2",
                feature.content_hash,
                feature.envelope.created_at_ns,
                future_index_available_at_ns or feature.envelope.available_at_ns,
                metadata,
            )
        )
    return feature


def _production_projection(
    tmp_path,
    *,
    gap_interval: BarIntervalV2 | None = None,
    persist_feature: bool = True,
    future_feature: bool = False,
    corrupt_feature: bool = False,
    late_revision: bool = False,
    m15_count: int = 56,
    wrong_product_revision: bool = False,
    h1_count: int = 56,
    h4_count: int = 56,
    auxiliary_input_ref_count: int = 0,
    projection_budget_ns: int = 80_000_000,
    unfinalized_m15: bool = False,
    later_health_state: PublicSourceStateV2 | None = None,
):
    path = tmp_path / "ops.sqlite"
    clock = FakeClock(CUTOFF + 100)
    with OpsRepository(path) as repository:
        feature = _register_feature_source(
            repository,
            cutoff_ns=CUTOFF - _M15,
            gap_interval=gap_interval,
            persist_feature=persist_feature,
            future_index_available_at_ns=CUTOFF + 1 if future_feature else None,
            corrupt_feature_body=corrupt_feature,
            m15_count=m15_count,
            feature_key=replace(KEY, contract_revision="f" * 64) if wrong_product_revision else KEY,
            h1_count=h1_count,
            h4_count=h4_count,
            auxiliary_input_ref_count=auxiliary_input_ref_count,
            unfinalized_m15=unfinalized_m15,
            later_health_state=later_health_state,
        )
        event, inputs, *_ = _production_event(repository)
        if late_revision and persist_feature:
            source = next(
                item
                for ref in feature.envelope.input_refs
                if (item := repository.get_artifact(ref)) is not None
                and item.artifact_type == "CausalBarV2"
                and item.metadata["bar"]["interval"] == BarIntervalV2.M15.value
            )
            old = source.metadata["bar"]
            old_index = repository.get_artifact(source.metadata["source_observation_ref"])
            assert old_index is not None
            raw = RawObservationV2.build(
                instrument_revision=KEY.contract_revision,
                source_id=old_index.metadata["source_id"],
                event_type="BAR_15M",
                received_at_ns=event.information_cutoff_ns,
                ingested_at_ns=event.information_cutoff_ns,
                available_at_ns=event.information_cutoff_ns,
                translation_version="late-revision-fixture-v1",
                payload={"late_revision": old["record_id"]},
                event_at_ns=old["close_at_ns"],
                revision_of=old["record_id"],
            )
            corrected = CausalBarV2(
                raw,
                BarIntervalV2.M15,
                old["open_at_ns"],
                old["close_at_ns"],
                Decimal(old["open"]),
                Decimal(old["high"]) + Decimal("1"),
                Decimal(old["low"]),
                Decimal(old["close"]),
                Decimal(old["volume"]),
                True,
            )
            observation_ref = sha256_json(
                {
                    "artifact_type": "PublicObservationIndexV2",
                    "record_id": raw.record_id,
                }
            )
            observation = {
                "record_id": raw.record_id,
                "source_id": raw.source_id,
                "event_type": raw.event_type,
                "instrument_revision": raw.instrument_revision,
                "instrument_key_json": KEY.to_canonical_json(),
                "event_at_ns": raw.event_at_ns,
                "published_at_ns": raw.published_at_ns,
                "translation_version": raw.translation_version,
                "revision_of": raw.revision_of,
                "quality_flags": list(raw.quality_flags),
                "availability_class": raw.availability_class.value,
                "replay_available_at_ns": raw.replay_available_at_ns,
                "raw_payload_hash": raw.raw_payload_hash,
                "bar_content_hash": corrected.content_hash,
            }
            repository.register_artifact(
                ArtifactIndexEntryV2(
                    observation_ref,
                    "PublicObservationIndexV2",
                    raw.content_hash,
                    event.information_cutoff_ns,
                    event.information_cutoff_ns,
                    observation,
                )
            )
            repository.register_artifact(
                ArtifactIndexEntryV2(
                    corrected.content_hash,
                    "CausalBarV2",
                    corrected.content_hash,
                    event.information_cutoff_ns,
                    event.information_cutoff_ns,
                    {"bar": corrected.to_dict(), "source_observation_ref": observation_ref},
                )
            )
    port = production.ProductionOpsCyclePortV1(
        public_source=ReconciledFixturePublicSource(event),
        inputs_provider=StaticInputsProvider(event.event_id, inputs),
        clock_ns=clock,
    )
    with OpsSupervisorV2(
        path,
        port,
        clock_ns=clock,
        production_intelligence_enabled=True,
        production_intelligence_budget_ns=projection_budget_ns,
    ) as supervisor:
        (receipt,) = supervisor.run_once().event_receipts
        assert supervisor.repository is not None
        identity = supervisor.repository.get_artifact(OpsSupervisorV2._receipt_identity_ref(event.event_id))
        assert identity is not None
        a0_before_projection = _a0_snapshot(supervisor.repository)
        receipt_ref = identity.metadata["receipt_ref"]
        if supervisor.repository.get_artifact(ResearchMemoryV1.production_projection_identity(receipt_ref)) is None:
            assert _a0_snapshot(supervisor.repository) == a0_before_projection
            return {}, feature, "DEFERRED"
        refs = production_intelligence.project_production_receipt(
            supervisor.repository,
            receipt,
            receipt_ref,
            now_ns=clock.now_ns,
        )
        if not refs:
            assert _a0_snapshot(supervisor.repository) == a0_before_projection
            return {}, feature, "DEFERRED"
        entry = supervisor.repository.get_artifact(refs[0])
        assert entry is not None
        return entry.metadata["brief"]["body"], feature, entry.metadata["status"]


def test_genuine_indexed_feature_enters_production_brief_without_changing_a0(tmp_path):
    body, feature, status = _production_projection(tmp_path)

    technical = [item for item in body["facts"] if item["section"] == "technical"]
    assert technical
    assert technical[0]["source_ref"] == feature.content_hash
    assert technical[0]["status"] == "AVAILABLE"
    assert technical[0]["values"]["features"]["m15.rsi14"]["value"] is not None
    assert "technical" not in body["missing_sections"]
    assert body["economic_value"] == "NOT_ESTIMABLE"
    assert body["authority"] == "ZERO"
    assert status == "BLOCKED"
    assert body["full_screening_denominator"]["whole_schedule_denominator_status"] == "UNAVAILABLE"
    assert body["full_screening_denominator"]["schedule_population_source"] == ("CURRENT_OPS_SUPERVISOR_RECEIPT_ONLY")
    assert body["full_screening_denominator"]["missing_expected_slots"] == "NOT_ESTIMABLE"
    assert body["memory_status"]["prior_analysis_qualification"]["status"] == "NO_QUALIFIED_PRIOR_ANALYSIS"


def test_gapped_feature_is_preserved_as_not_estimable_not_as_a_value(tmp_path):
    body, feature, status = _production_projection(tmp_path, gap_interval=BarIntervalV2.M15)

    technical = [item for item in body["facts"] if item["section"] == "technical"]
    assert technical
    assert technical[0]["source_ref"] == feature.content_hash
    assert technical[0]["status"] == "NOT_ESTIMABLE"
    assert "INTERVAL_COVERAGE_GAP" in technical[0]["missingness"]
    assert dict(technical[0]["values"]["features"]) == {}
    assert status == "BLOCKED"


def test_unfinalized_bar_cannot_qualify_a_technical_value(tmp_path):
    body, _feature, status = _production_projection(tmp_path, unfinalized_m15=True)

    assert all(item["section"] != "technical" for item in body["facts"])
    assert "technical" in body["missing_sections"]
    assert status == "BLOCKED"


def test_source_health_failure_after_feature_cutoff_invalidates_fact(tmp_path):
    body, _feature, status = _production_projection(
        tmp_path,
        later_health_state=PublicSourceStateV2.STALE,
    )

    assert all(item["section"] != "technical" for item in body["facts"])
    assert "technical" in body["missing_sections"]
    assert status == "BLOCKED"


@pytest.mark.parametrize(
    "options",
    [
        {"persist_feature": False},
        {"future_feature": True},
        {"corrupt_feature": True},
        {"late_revision": True},
        {"wrong_product_revision": True},
    ],
    ids=("missing", "future", "wrong-hash", "late-revision", "wrong-product"),
)
def test_unqualified_indexed_feature_does_not_enter_brief(tmp_path, options):
    body, _feature, status = _production_projection(tmp_path, **options)

    assert status == "BLOCKED"
    assert not [item for item in body["facts"] if item["section"] == "technical"]
    assert "technical" in body["missing_sections"]


def test_warmup_shortfall_keeps_typed_missingness_without_a_number(tmp_path):
    body, feature, status = _production_projection(tmp_path, m15_count=10)

    (technical,) = [item for item in body["facts"] if item["section"] == "technical"]
    assert technical["source_ref"] == feature.content_hash
    assert technical["status"] == "NOT_ESTIMABLE"
    assert "INSUFFICIENT_HISTORY_OR_MISSING_TIMEFRAME" in technical["missingness"]
    assert all(value["value"] is None for value in technical["values"]["features"].values())
    assert status == "BLOCKED"


def test_verified_feature_fact_is_reproducible_from_persisted_sources_after_restart(tmp_path):
    path = tmp_path / "feature-restart.sqlite"
    with OpsRepository(path) as repository:
        feature = _register_feature_source(repository, cutoff_ns=CUTOFF - _M15)
        event, inputs, _case, candidate_set, *_ = _production_event(repository)
        universe = inputs.universe
        first_facts, first_reasons, first_refs, _first_proof = read_production_feature_facts(
            repository,
            candidate_set,
            universe,
            cutoff_ns=event.information_cutoff_ns,
        )
        assert not first_reasons
        assert first_refs
        assert first_facts[0].source_ref == feature.content_hash
        candidate_set_ref = candidate_set.content_hash
        universe_ref = universe.content_hash

    with OpsRepository(path) as restarted:
        candidate_entry = restarted.get_artifact(candidate_set_ref)
        universe_entry = restarted.get_artifact(universe_ref)
        assert candidate_entry is not None
        assert universe_entry is not None
        replayed_candidate_set = CandidateSetV2.from_dict(json_value(candidate_entry.metadata["candidate_set"]))
        replayed_universe = UniverseContractV2.from_dict(json_value(universe_entry.metadata["universe"]))
        replayed_facts, replayed_reasons, replayed_refs, proof = read_production_feature_facts(
            restarted,
            replayed_candidate_set,
            replayed_universe,
            cutoff_ns=event.information_cutoff_ns,
        )

    assert not replayed_reasons
    assert proof is not None
    assert tuple(item.to_dict() for item in replayed_facts) == tuple(item.to_dict() for item in first_facts)
    assert replayed_refs == first_refs


_A0_TYPES = (
    "OpsSupervisorReceiptV1",
    "CandidateSetV2",
    "SizingDecisionV2",
    "ActionArtifactV2",
    "EvaluationArtifactV2",
    "DecisionCalendarEntryV2",
    "DecisionCalendarV2",
)


def _a0_snapshot(repository: OpsRepository):
    return {
        kind: tuple(
            (
                entry.artifact_ref,
                entry.content_hash,
                entry.created_at_ns,
                entry.available_at_ns,
                canonical_json(entry.metadata),
            )
            for entry in repository.artifact_entries(kind)
        )
        for kind in _A0_TYPES
    }


def test_a0_artifact_sets_are_equal_with_projection_enabled_and_disabled(tmp_path):
    snapshots = []
    clock = FakeClock(CUTOFF + 100)
    for enabled in (False, True):
        path = tmp_path / f"a0-{enabled}.sqlite"
        with OpsRepository(path) as repository:
            _register_feature_source(repository, cutoff_ns=CUTOFF - _M15)
            event, inputs, *_ = _production_event(repository)
        port = production.ProductionOpsCyclePortV1(
            public_source=ReconciledFixturePublicSource(event),
            inputs_provider=StaticInputsProvider(event.event_id, inputs),
            clock_ns=clock,
        )
        with OpsSupervisorV2(path, port, clock_ns=clock, production_intelligence_enabled=enabled) as supervisor:
            result = supervisor.run_once()
            assert len(result.event_receipts) == 1
            assert supervisor.repository is not None
            snapshots.append(_a0_snapshot(supervisor.repository))
    assert snapshots[0] == snapshots[1]


def test_projection_failure_receipts_are_sanitized_bounded_and_restart_safe(tmp_path):
    path = tmp_path / "projection-failures.sqlite"
    clock = FakeClock(CUTOFF + 100)
    with OpsRepository(path) as repository:
        event, inputs, *_ = _production_event(repository)

    attempts = 0
    cycle_a0 = None
    receipt = None
    receipt_ref = None
    for run_number in range(3):
        if run_number:
            clock.now_ns += 10
        port = production.ProductionOpsCyclePortV1(
            public_source=ReconciledFixturePublicSource(event),
            inputs_provider=StaticInputsProvider(event.event_id, inputs),
            clock_ns=clock,
        )
        order: list[str] = []
        with OpsSupervisorV2(path, port, clock_ns=clock, production_intelligence_enabled=False) as supervisor:
            original_service = supervisor.service_public_stream

            def service_then_record(order=order, original_service=original_service):
                order.append("source_service")
                original_service()

            supervisor.service_public_stream = service_then_record

            def fail_projection(_receipt, _receipt_ref, _repository, order=order):
                nonlocal attempts
                attempts += 1
                order.append("projection")
                raise RuntimeError("secret source payload must not be persisted")

            supervisor.post_receipt_projection = fail_projection
            result = supervisor.run_once()
            if run_number and not result.event_receipts:
                clock.now_ns += 10
                result = supervisor.run_once()
            assert result.cycle.failure_types == (), f"supervisor replay failed on iteration {run_number}"
            assert len(result.event_receipts) == 1
            if run_number == 0:
                receipt = result.event_receipts[0]
                identity = supervisor.repository.get_artifact(supervisor._receipt_identity_ref(event.event_id))
                assert identity is not None
                receipt_ref = identity.metadata["receipt_ref"]
            else:
                assert receipt is not None and receipt_ref is not None
                recovered = supervisor._read_final_receipt(supervisor.repository, event.event_id)
                assert recovered is not None and recovered.content_hash == receipt.content_hash
                assert result.event_receipts[0].content_hash == receipt.content_hash
            assert supervisor.repository is not None
            current_a0 = _a0_snapshot(supervisor.repository)
            if cycle_a0 is None:
                cycle_a0 = current_a0
            else:
                assert current_a0 == cycle_a0
            if run_number == 0:
                assert order.index("projection") > max(
                    index for index, item in enumerate(order) if item == "source_service"
                )
            elif run_number == 1:
                assert "projection" in order
            else:
                assert "projection" not in order
    with OpsRepository(path) as restarted:
        failures = restarted.artifact_entries("OpsProductionIntelligenceProjectionFailureV1")
        assert attempts == 2
        assert len(failures) == 2
        assert [item.metadata["attempt"] for item in failures] == [1, 2]
        assert all("secret source payload" not in canonical_json(item.metadata) for item in failures)


def test_projection_budget_expiry_keeps_a0_and_writes_failure_receipt(tmp_path):
    path = tmp_path / "projection-budget.sqlite"
    clock = FakeClock(CUTOFF + 100)
    with OpsRepository(path) as repository:
        event, inputs, *_ = _production_event(repository)
    port = production.ProductionOpsCyclePortV1(
        public_source=ReconciledFixturePublicSource(event),
        inputs_provider=StaticInputsProvider(event.event_id, inputs),
        clock_ns=clock,
    )
    ticks = iter((0, 2))
    with OpsSupervisorV2(path, port, clock_ns=clock, production_intelligence_enabled=False) as supervisor:

        def projection(receipt, receipt_ref, repository):
            before = _a0_snapshot(repository)
            try:
                production_intelligence.project_production_receipt(
                    repository,
                    receipt,
                    receipt_ref,
                    now_ns=receipt.created_at_ns,
                    monotonic_ns=lambda: next(ticks),
                    budget_ns=1,
                )
            except TimeoutError:
                assert _a0_snapshot(repository) == before
                raise

        supervisor.post_receipt_projection = projection
        result = supervisor.run_once()
        assert len(result.event_receipts) == 1
        assert supervisor.repository is not None
        assert len(supervisor.repository.artifact_entries("OpsProductionIntelligenceProjectionFailureV1")) == 1
        assert supervisor.repository.artifact_entries("ProductionDecisionBriefProjectionV1") == ()


def test_projection_precommit_expiry_rolls_back_all_memory_rows_and_restarts(tmp_path):
    path = tmp_path / "projection-precommit.sqlite"
    source_body = {"version": "fixture", "authority": "ZERO"}
    source_ref = sha256_json(source_body)
    receipt_ref = sha256_json({"receipt": "precommit"})
    with OpsRepository(path) as repository:
        repository.register_artifact(
            ArtifactIndexEntryV2(
                source_ref,
                "FixtureSourceV1",
                source_ref,
                100,
                100,
                source_body,
            )
        )
        memory = ResearchMemoryV1(repository)
        with pytest.raises(TimeoutError):
            memory.persist_production_projection(
                receipt_ref=receipt_ref,
                brief={"status": "BLOCKED"},
                brief_status="BLOCKED",
                source_refs=(source_ref,),
                source_population_ref=source_ref,
                now_ns=200,
                precommit_check=lambda: (_ for _ in ()).throw(TimeoutError("budget")),
            )
        assert repository.artifact_entries("ProductionDecisionBriefProjectionV1") == ()
        assert repository.artifact_entries("ProductionIntelligenceMemoryProjectionV1") == ()
        assert repository.artifact_entries("ProductionIntelligenceMemoryIdentityV1") == ()

    with OpsRepository(path) as restarted:
        refs = ResearchMemoryV1(restarted).persist_production_projection(
            receipt_ref=receipt_ref,
            brief={"status": "BLOCKED"},
            brief_status="BLOCKED",
            source_refs=(source_ref,),
            source_population_ref=source_ref,
            now_ns=300,
        )
        assert len(restarted.artifact_entries("ProductionDecisionBriefProjectionV1")) == 1
        assert len(restarted.artifact_entries("ProductionIntelligenceMemoryProjectionV1")) == 1
        assert len(restarted.artifact_entries("ProductionIntelligenceMemoryIdentityV1")) == 1
        assert len(refs) == 2
