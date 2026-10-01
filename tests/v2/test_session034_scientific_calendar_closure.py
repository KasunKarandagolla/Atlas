from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import canonical_json, json_value, sha256_json
from atlas.v2.contracts import CandidateSelectionStatus, CandidateSetV2
from atlas.v2.data.bars import BarIntervalV2, CausalBarV2
from atlas.v2.data.raw import AvailabilityClassV2, RawObservationV2
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentKeyV2,
    ProductContractV2,
    ProductTypeV2,
    TradingStatusV2,
    UniverseContractV2,
    VenueV2,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime import outcome_maturity, production
from atlas.v2.runtime.ops_supervisor import (
    OpsDecisionEventV1,
    OpsStageResultV1,
    OpsStageStatusV1,
    OpsTerminalStatusV1,
)
from atlas.v2.runtime.s3_native_cadence import (
    S3_M1_EVENT_TYPE,
    s3_m1_event_id,
    s3_m1_origin_metadata,
    s3_m1_origin_ref,
)
from atlas.v2.science.outcomes import (
    AdmissionStateV2,
    DecisionCalendarEntryV2,
    DecisionSourceStageV2,
    SelectionStateV2,
)
from atlas.v2.science.research_selection import MULTI_SLEEVE_SELECTION_HASH
from atlas.v2.science.s3_calendar import (
    S3DecisionCalendarMissingnessV1,
    ensure_native_s3_research_universe,
    s3_decision_calendar_denominator,
)
from atlas.v2.strategies.s3_mean_reversion import S3_POLICY

NS = 1_000_000_000
MINUTE_NS = 60 * NS


def _product(revision: str = "s34-calendar-r1") -> ProductContractV2:
    key = InstrumentKeyV2(
        VenueV2.BYBIT,
        EnvironmentV2.MAINNET,
        ProductTypeV2.LINEAR_PERPETUAL,
        "BTCUSDT",
        "BTC",
        "USDT",
        "USDT",
        sha256_json({"revision": revision}),
    )
    return ProductContractV2(
        key, 0, 0, 0, Decimal("1"), Decimal("0.1"), Decimal("0.001"), Decimal("0"),
        TradingStatusV2.TRADING, sha256_json({"metadata": revision}),
        min_notional=Decimal("1"), max_qty=Decimal("1000"),
    )


def _diagnostic_entry(
    repository: OpsRepository,
    *,
    event: OpsDecisionEventV1,
    bar_ref: str,
    artifact_type: str,
    body_key: str,
    evidence_cutoff_ns: int,
) -> str:
    started = evidence_cutoff_ns + 1
    finished = evidence_cutoff_ns + 10
    available = evidence_cutoff_ns + 11
    trigger = repository.get_artifact(event.trigger_ref)
    trigger_body = trigger.metadata["trigger"] if trigger is not None else {}
    product_ref = trigger_body.get("product_ref")
    assert isinstance(product_ref, str)
    product_entry = repository.get_artifact(product_ref)
    product_body = product_entry.metadata["product"] if product_entry is not None else {}
    product = ProductContractV2.from_dict(json_value(product_body))
    body = {
        "version": f"{artifact_type}_TEST_FIXTURE_V1",
        "key": product.key.to_dict(),
        "cutoff_ns": evidence_cutoff_ns,
        "trade_completeness_proven": False,
        "computation_context": {
            "evidence_cutoff_ns": evidence_cutoff_ns,
            "computation_started_ns": started,
            "computation_finished_ns": finished,
            "consumer_deadline_ns": event.deadline_ns,
        },
    }
    ref = sha256_json({"artifact_type": artifact_type, body_key: body})
    repository.register_artifact(ArtifactIndexEntryV2(
        ref,
        artifact_type,
        ref,
        available,
        available,
        {body_key: body, "decision_event_id": event.event_id,
         "trigger_bar_ref": bar_ref, "authority": "ZERO"},
    ))
    return ref


def _native_fixture(repository: OpsRepository, *, revision: str = "s34-calendar-r1"):
    product = _product(revision)
    close_at_ns = 2_000 * MINUTE_NS
    cutoff_ns = close_at_ns + 100
    raw_payload = canonical_json({"open": "100", "high": "101", "low": "99", "close": "100"}).encode()
    raw = RawObservationV2.build(
        instrument_revision=product.key.contract_revision,
        source_id="BYBIT_PUBLIC_WS",
        event_type="BAR_1M",
        event_at_ns=close_at_ns,
        received_at_ns=cutoff_ns,
        ingested_at_ns=cutoff_ns,
        available_at_ns=cutoff_ns,
        translation_version="s34-calendar-regression-v1",
        sequence=str(close_at_ns - MINUTE_NS),
        payload=raw_payload,
        availability_class=AvailabilityClassV2.ACTUAL_SYSTEM,
    )
    bar = CausalBarV2(
        raw,
        BarIntervalV2.M1,
        close_at_ns - MINUTE_NS,
        close_at_ns,
        Decimal("100"),
        Decimal("101"),
        Decimal("99"),
        Decimal("100"),
        Decimal("1"),
        True,
    )
    product_entry = ArtifactIndexEntryV2(
        product.content_hash, "ProductContractV2", product.content_hash,
        product.available_at_ns, product.available_at_ns, {"product": product.to_dict()},
    )
    bar_entry = ArtifactIndexEntryV2(
        bar.content_hash, "CausalBarV2", bar.content_hash,
        cutoff_ns, cutoff_ns, {"bar": bar.to_dict(), "source_observation_ref": sha256_json({"raw": raw.record_id})},
    )
    observation_ref = sha256_json({"source-observation": raw.record_id})
    health_ref = sha256_json({"health": "healthy-at-cutoff", "key": product.key.to_dict()})
    trigger_body = {
        "version": "OPS_PUBLIC_FINAL_BAR_TRIGGER_V1",
        "source_observation_ref": observation_ref,
        "bar_ref": bar.content_hash,
        "product_ref": product.content_hash,
        "source_id": raw.source_id,
        "source_event_at_ns": close_at_ns,
        "source_published_at_ns": None,
        "received_at_ns": raw.received_at_ns,
        "available_at_ns": raw.available_at_ns,
        "information_cutoff_ns": cutoff_ns,
        "authority": "ZERO",
    }
    trigger_ref = sha256_json(trigger_body)
    repository.register_artifacts((
        product_entry,
        bar_entry,
        ArtifactIndexEntryV2(observation_ref, "PublicObservationIndexV2", raw.content_hash,
                             cutoff_ns, cutoff_ns, {"record_id": raw.record_id}),
        ArtifactIndexEntryV2(health_ref, "PublicSourceHealthV2", health_ref,
                             cutoff_ns, cutoff_ns, {"health": {"available_at_ns": cutoff_ns}}),
        ArtifactIndexEntryV2(trigger_ref, "OpsPublicFinalBarTriggerV1", trigger_ref,
                             cutoff_ns, cutoff_ns, {"trigger": trigger_body}),
    ))
    deadline_ns = close_at_ns + 5 * NS
    event = OpsDecisionEventV1(
        s3_m1_event_id(product.key, close_at_ns),
        S3_M1_EVENT_TYPE,
        raw.source_id,
        trigger_ref,
        close_at_ns,
        None,
        cutoff_ns,
        cutoff_ns,
        cutoff_ns,
        deadline_ns,
        (product.content_hash, bar.content_hash, observation_ref, health_ref),
    )
    origin = s3_m1_origin_metadata(product.key, close_at_ns, bar_ref=bar.content_hash)["native_m1_origin"]
    repository.register_artifact(ArtifactIndexEntryV2(
        event.content_hash, "OpsDecisionEventSourceV1", event.content_hash,
        event.available_at_ns, event.available_at_ns,
        {"event": event.to_dict(), "native_m1_origin": origin},
    ))
    trade_ref = _diagnostic_entry(
        repository, event=event, bar_ref=bar.content_hash,
        artifact_type="S3ForwardTradeEvidenceV1", body_key="evidence",
        evidence_cutoff_ns=cutoff_ns,
    )
    readiness_ref = _diagnostic_entry(
        repository, event=event, bar_ref=bar.content_hash,
        artifact_type="S3NativeWarmupReadinessV1", body_key="readiness",
        evidence_cutoff_ns=cutoff_ns,
    )
    diagnostic_refs = tuple(sorted((trade_ref, readiness_ref)))

    class DiagnosticInputs:
        def resolve(self, _repository, _event):
            return production.ProductionEventInputsV1(None, (), {}, {}, {}, (), diagnostic_refs)

    now_ns = cutoff_ns + 100
    port = production.ProductionOpsCyclePortV1(
        inputs_provider=DiagnosticInputs(),  # type: ignore[arg-type]
        clock_ns=lambda: now_ns,
    )
    return product, bar, event, diagnostic_refs, now_ns, port


def _process(repository: OpsRepository, event: OpsDecisionEventV1, now_ns: int, port):
    checkpoints: list[OpsStageResultV1] = []
    result = port.process_event(
        repository,
        event,
        now_ns=now_ns,
        source_health_state="UNKNOWN",
        completed_stages={},
        checkpoint=checkpoints.append,
    )
    return result, checkpoints


def test_timely_native_m1_persists_exact_not_estimable_candidate_and_calendar(tmp_path):
    with OpsRepository(tmp_path / "timely.sqlite") as repository:
        product, _bar, event, _diagnostics, now_ns, port = _native_fixture(repository)
        result, checkpoints = _process(repository, event, now_ns, port)

        candidate_entries = repository.artifact_entries("CandidateSetV2")
        calendar_entries = repository.artifact_entries("DecisionCalendarEntryV2")
        assert result.terminal_status == OpsTerminalStatusV1.NOT_ESTIMABLE
        assert len(candidate_entries) == len(calendar_entries) == 1
        candidate_set = CandidateSetV2.from_dict(json_value(candidate_entries[0].metadata["candidate_set"]))
        calendar = DecisionCalendarEntryV2.from_dict(json_value(calendar_entries[0].metadata["decision_entry"]))
        identity = candidate_entries[0].metadata["identity"]
        assert identity["authority"] == "ZERO"

        checks = {
            "decision_event_id": candidate_set.decision_event_id == event.event_id,
            "empty_candidates": candidate_set.candidates == (),
            "not_estimable": candidate_set.selection_status == CandidateSelectionStatus.NOT_ESTIMABLE,
            "blocker_tie_rule": candidate_set.tie_break_rule == "NO_SELECTION_WHILE_BYBIT_TRADE_COMPLETENESS_IS_UNPROVEN",
            "no_selected_candidate": candidate_set.selected_candidate_id is None,
            "not_no_candidate": candidate_set.selection_status != CandidateSelectionStatus.NO_CANDIDATE,
            "existing_selection_policy": candidate_set.selection_policy_hash == MULTI_SLEEVE_SELECTION_HASH,
            "calendar_cutoff": calendar.decision_at_ns == event.information_cutoff_ns,
            "calendar_state": calendar.selection_state == SelectionStateV2.NOT_ESTIMABLE,
            "no_admission": calendar.admission_state == AdmissionStateV2.NOT_APPLICABLE,
            "no_candidate_action": (calendar.candidate_ref, calendar.action_hash, calendar.action_artifact_ref)
            == (None, None, None),
            "blocker_reason": "BYBIT_TRADE_COMPLETENESS_UNPROVEN" in calendar.reason_codes,
            "candidate_source": calendar.source_stage == DecisionSourceStageV2.CANDIDATE_SET,
        }
        assert all(checks.values()), checks
        assert calendar.source_artifact_ref == candidate_set.content_hash
        assert calendar.candidate_set_ref == candidate_set.content_hash
        assert calendar.created_at_ns == calendar.available_at_ns == candidate_set.envelope.available_at_ns

        universe_entry = repository.get_artifact(candidate_set.universe_ref)
        assert universe_entry is not None and universe_entry.artifact_type == "UniverseContractV2"
        universe = UniverseContractV2.from_dict(json_value(universe_entry.metadata["universe"]))
        assert universe.content_hash == candidate_set.universe_ref
        assert universe.envelope.input_refs == event.causal_input_refs
        assert universe.envelope.created_at_ns >= event.information_cutoff_ns
        assert universe.envelope.available_at_ns <= identity["computation_started_ns"]
        assert universe.entries[0].key == product.key
        assert universe.entries[0].product_ref == product.content_hash
        assert universe.entries[0].strategy_eligibility[S3_POLICY.policy_id].status.value == "NOT_ESTIMABLE"
        assert not universe.entries[0].data_eligible
        assert not universe.entries[0].scanner_eligible
        assert not universe.entries[0].capital_eligible

        assert candidate_set.envelope.created_at_ns == identity["computation_finished_ns"]
        assert (event.information_cutoff_ns <= identity["computation_started_ns"]
                <= identity["computation_finished_ns"]
                <= candidate_set.envelope.available_at_ns <= event.deadline_ns)
        assert identity["cutoff_ns"] == event.information_cutoff_ns
        assert all(repository.get_artifact(ref).available_at_ns <= event.information_cutoff_ns
                   for ref in candidate_set.envelope.input_refs)
        assert all(
            repository.get_artifact(ref).metadata[
                "evidence" if repository.get_artifact(ref).artifact_type == "S3ForwardTradeEvidenceV1"
                else "readiness"
            ]["trade_completeness_proven"] is False
            for ref in identity["diagnostic_refs"]
        )
        assert all(repository.get_artifact(ref).available_at_ns <= identity["computation_started_ns"]
                   for ref in identity["diagnostic_refs"])

        stage_candidate = next(item for item in checkpoints if item.stage == production.PipelineStageV1.CANDIDATE_SET)
        stage_calendar = next(item for item in checkpoints if item.stage == production.PipelineStageV1.DECISION_CALENDAR)
        assert stage_candidate.status == OpsStageStatusV1.COMPLETE
        assert stage_candidate.artifact_refs == (candidate_set.content_hash,)
        assert stage_calendar.status == OpsStageStatusV1.NOT_ESTIMABLE
        assert stage_calendar.artifact_refs == (calendar_entries[0].artifact_ref,)
        assert repository.artifact_entries("CandidateActionV2") == ()
        assert repository.artifact_entries("ActionArtifactV2") == ()
        denominator = s3_decision_calendar_denominator(repository)
        assert len(denominator) == 1
        assert denominator[0].population_class == "TIMELY_NOT_ESTIMABLE"
        assert denominator[0].artifact_ref == calendar_entries[0].artifact_ref
        assert S3_POLICY.capital_status == "SHADOW_ONLY"
        assert S3_POLICY.setup_parameters["standardization_count"] == 120


def test_retry_reuses_exact_candidate_set_and_calendar_identity(tmp_path):
    with OpsRepository(tmp_path / "retry.sqlite") as repository:
        _product_value, _bar, event, _diagnostics, now_ns, port = _native_fixture(repository)
        first, _ = _process(repository, event, now_ns, port)
        first_candidate = repository.artifact_entries("CandidateSetV2")[0].artifact_ref
        first_calendar = repository.artifact_entries("DecisionCalendarEntryV2")[0].artifact_ref
        first_counts = tuple(len(repository.artifact_entries(kind)) for kind in (
            "CandidateSetV2", "CandidateSetDecisionIndexV1", "DecisionCalendarEntryV2",
            "DecisionCalendarIdentityV2",
        ))

        second, _ = _process(repository, event, now_ns + 1, port)
        assert first.terminal_status == second.terminal_status == OpsTerminalStatusV1.NOT_ESTIMABLE
        assert tuple(len(repository.artifact_entries(kind)) for kind in (
            "CandidateSetV2", "CandidateSetDecisionIndexV1", "DecisionCalendarEntryV2",
            "DecisionCalendarIdentityV2",
        )) == first_counts == (1, 1, 1, 1)
        assert repository.artifact_entries("CandidateSetV2")[0].artifact_ref == first_candidate
        assert repository.artifact_entries("DecisionCalendarEntryV2")[0].artifact_ref == first_calendar


def test_retry_after_candidate_stage_checkpoint_reuses_candidate_and_calendar(tmp_path):
    with OpsRepository(tmp_path / "partial-retry.sqlite") as repository:
        _product_value, _bar, event, _diagnostics, now_ns, port = _native_fixture(repository)
        partial: list[OpsStageResultV1] = []

        def fail_after_candidate(result):
            partial.append(result)
            if result.stage == production.PipelineStageV1.CANDIDATE_SET:
                raise RuntimeError("simulated interruption after CandidateSet checkpoint")

        with pytest.raises(RuntimeError, match="simulated interruption"):
            port.process_event(
                repository, event, now_ns=now_ns, source_health_state="UNKNOWN",
                completed_stages={}, checkpoint=fail_after_candidate,
            )
        candidate_ref = repository.artifact_entries("CandidateSetV2")[0].artifact_ref
        calendar_ref = repository.artifact_entries("DecisionCalendarEntryV2")[0].artifact_ref
        completed = {item.stage: item for item in partial}
        resumed: list[OpsStageResultV1] = []
        result = port.process_event(
            repository, event, now_ns=now_ns + 1, source_health_state="UNKNOWN",
            completed_stages=completed, checkpoint=resumed.append,
        )
        assert result.terminal_status == OpsTerminalStatusV1.NOT_ESTIMABLE
        assert tuple(resumed[:len(partial)]) == tuple(partial)
        assert repository.artifact_entries("CandidateSetV2")[0].artifact_ref == candidate_ref
        assert repository.artifact_entries("DecisionCalendarEntryV2")[0].artifact_ref == calendar_ref
        assert len(repository.artifact_entries("CandidateSetV2")) == 1
        assert len(repository.artifact_entries("DecisionCalendarEntryV2")) == 1


def test_native_s3_fixed_deadline_is_not_rebased_when_candidate_calendar_is_produced(tmp_path):
    with OpsRepository(tmp_path / "deadline.sqlite") as repository:
        _product_value, _bar, event, _diagnostics, now_ns, port = _native_fixture(repository)
        late_clock_port = production.ProductionOpsCyclePortV1(
            inputs_provider=port.inputs_provider,
            clock_ns=lambda: event.deadline_ns + 1,
        )
        checkpoints: list[OpsStageResultV1] = []
        with pytest.raises(ValueError, match="fixed cutoff/deadline"):
            late_clock_port.process_event(
                repository, event, now_ns=now_ns, source_health_state="UNKNOWN",
                completed_stages={}, checkpoint=checkpoints.append,
            )
        assert checkpoints == []
        assert repository.artifact_entries("CandidateSetV2") == ()
        assert repository.artifact_entries("DecisionCalendarEntryV2") == ()


def test_conflicting_candidate_set_decision_identity_fails_closed(tmp_path):
    with OpsRepository(tmp_path / "conflict.sqlite") as repository:
        product, _bar, event, diagnostics, now_ns, port = _native_fixture(repository)
        universe = ensure_native_s3_research_universe(
            repository, event, diagnostics, created_at_ns=now_ns, available_at_ns=now_ns,
        )
        index_ref = sha256_json({
            "artifact_type": "CandidateSetDecisionIndexV1",
            "decision_event_id": event.event_id,
            "universe_ref": universe.content_hash,
            "selection_policy_hash": MULTI_SLEEVE_SELECTION_HASH,
        })
        conflict_body = {
            "candidate_set_ref": sha256_json({"conflict": "wrong immutable set"}),
            "decision_event_id": event.event_id,
            "universe_ref": universe.content_hash,
            "selection_policy_hash": MULTI_SLEEVE_SELECTION_HASH,
            "cutoff_ns": event.information_cutoff_ns,
            "deadline_ns": event.deadline_ns,
        }
        repository.register_artifact(ArtifactIndexEntryV2(
            index_ref, "CandidateSetDecisionIndexV1", sha256_json(conflict_body),
            now_ns, now_ns, conflict_body,
        ))
        with pytest.raises(ValueError, match="conflicting|conflicts|missing"):
            _process(repository, event, now_ns, port)
        assert repository.artifact_entries("CandidateSetV2") == ()
        assert repository.artifact_entries("DecisionCalendarEntryV2") == ()


def test_future_evidence_cannot_enter_native_candidateset_lineage(tmp_path):
    with OpsRepository(tmp_path / "future.sqlite") as repository:
        _product_value, _bar, event, _diagnostics, now_ns, port = _native_fixture(repository)
        future_ref = sha256_json({"future": "not cutoff visible"})
        repository.register_artifact(ArtifactIndexEntryV2(
            future_ref, "FutureEvidenceFixtureV1", future_ref,
            event.information_cutoff_ns + 1, event.information_cutoff_ns + 1, {"future": True},
        ))
        invalid_event = replace(event, causal_input_refs=(*event.causal_input_refs, future_ref))
        with pytest.raises(ValueError, match="future cutoff evidence"):
            _process(repository, invalid_event, now_ns, port)
        assert repository.artifact_entries("CandidateSetV2") == ()
        assert repository.artifact_entries("DecisionCalendarEntryV2") == ()


def test_late_origin_has_non_decision_scientific_denominator_record_and_no_outcome(tmp_path):
    product = _product("s34-late-calendar-r1")
    close_at_ns = 3_000 * MINUTE_NS
    available_at_ns = close_at_ns + 100
    raw = RawObservationV2.build(
        instrument_revision=product.key.contract_revision,
        source_id="BYBIT_PUBLIC_WS",
        event_type="BAR_1M",
        event_at_ns=close_at_ns,
        received_at_ns=available_at_ns,
        ingested_at_ns=available_at_ns,
        available_at_ns=available_at_ns,
        translation_version="s34-late-calendar-fixture-v1",
        sequence=str(close_at_ns - MINUTE_NS),
        payload=b"late-native-m1-fixture",
        availability_class=AvailabilityClassV2.ACTUAL_SYSTEM,
    )
    bar = CausalBarV2(
        raw, BarIntervalV2.M1, close_at_ns - MINUTE_NS, close_at_ns,
        Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100"), Decimal("1"), True,
    )
    with OpsRepository(tmp_path / "late.sqlite") as repository:
        gate_ref = production._persist_late_s3_m1_origin_gate(
            repository, product, bar, observed_at_ns=close_at_ns + 5 * NS + 1,
        )
        missing_entries = repository.artifact_entries("S3DecisionCalendarMissingnessV1")
        assert len(missing_entries) == 1
        missing = S3DecisionCalendarMissingnessV1.from_dict(json_value(missing_entries[0].metadata["missingness"]))
        assert missing.instrument_key == product.key
        assert missing.decision_slot_ns == close_at_ns
        assert missing.origin_ref == s3_m1_origin_ref(product.key, close_at_ns)
        assert missing.late_gate_ref == gate_ref
        assert missing.state == "TEST GATE"
        assert missing.authority == "ZERO"
        assert missing.created_at_ns == missing.available_at_ns == close_at_ns + 5 * NS + 1
        assert not {"payoff", "pnl", "candidate_ref", "action_hash"}.intersection(missing.to_dict())
        assert repository.artifact_entries("OpsDecisionEventSourceV1") == ()
        assert repository.artifact_entries("CandidateSetV2") == ()
        assert repository.artifact_entries("DecisionCalendarEntryV2") == ()

        denominator = s3_decision_calendar_denominator(repository)
        assert len(denominator) == 1
        row = denominator[0]
        assert row.population_class == "LATE_OR_MISSED_TEST_GATE"
        assert row.artifact_ref == missing.content_hash
        assert row.artifact_type == "S3DecisionCalendarMissingnessV1"
        assert row.instrument_key == product.key and row.decision_slot_ns == close_at_ns
        assert row.state == "TEST GATE"
        assert row.reason_code == "NATIVE_M1_BAR_FIRST_SEEN_AFTER_FIXED_DEADLINE"

        repeated_gate_ref = production._persist_late_s3_m1_origin_gate(
            repository, product, bar, observed_at_ns=close_at_ns + 6 * NS,
        )
        assert repeated_gate_ref == gate_ref
        assert len(repository.artifact_entries("S3DecisionCalendarMissingnessV1")) == 1
        maturity = outcome_maturity.run_outcome_maturity_cycle(
            repository,
            missing.available_at_ns + 10,
            production_clock_ns=lambda: missing.available_at_ns + 10,
        )
        assert maturity.decisions_inspected == 0
        assert repository.artifact_entries("MaturedOutcomeV2") == ()


def test_timely_not_estimable_calendar_remains_visible_to_s33_maturity_reader(tmp_path):
    with OpsRepository(tmp_path / "maturity.sqlite") as repository:
        _product_value, _bar, event, _diagnostics, now_ns, port = _native_fixture(repository)
        _process(repository, event, now_ns, port)
        calendar = repository.artifact_entries("DecisionCalendarEntryV2")[0]
        parsed = outcome_maturity._validate_calendar_entry(calendar, now_ns)
        assert parsed.selection_state == SelectionStateV2.NOT_ESTIMABLE
        assert parsed.decision_at_ns == event.information_cutoff_ns

        now = calendar.available_at_ns + 10
        monotonic_value = {"now": 0}

        def monotonic_ns() -> int:
            monotonic_value["now"] += 10
            return monotonic_value["now"]

        report = outcome_maturity.run_outcome_maturity_cycle(
            repository,
            now,
            production_clock_ns=lambda: now,
            monotonic_ns=monotonic_ns,
        )
        assert report.decisions_inspected == 1
        assert report.unsupported_count == 1
        assert repository.artifact_entries("MaturedOutcomeV2") == ()
