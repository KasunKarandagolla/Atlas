"""Deterministic Session-020 integration through production public-data seams."""

from __future__ import annotations

import hashlib
import os
import secrets
import subprocess
import sys
import time
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

from atlas.v2._serialization import canonical_json, sha256_json
from atlas.v2.contracts import EligibilityStatusV2
from atlas.v2.data.bars import BarIntervalV2, CausalBarStoreV2, CausalBarV2, close_boundary_ns
from atlas.v2.data.bybit import SOURCE_ID, translate_instrument_info, translate_kline
from atlas.v2.data.collector import PublicCollectorV2
from atlas.v2.data.health import PublicSourceStateV2
from atlas.v2.data.history import ParquetObservationArchiveV2, reconstruct_causal_bars_from_archive
from atlas.v2.data.universe import DynamicUniverseRuntimeV2, UniverseObservationV2
from atlas.v2.features.joins import asof_join
from atlas.v2.features.pipeline import feature_snapshot
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentRegistryV2,
    ProductContractV2,
    UniverseContractV2,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.science.action import freeze_action
from atlas.v2.science.admission import (
    ADMISSION_POLICY_VERSION,
    AdmissionPolicyV2,
    VenueCapabilitySnapshotV2,
    VenueCapabilityStatusV2,
)
from atlas.v2.science.evaluation_service import run_phase2_economic_evaluation
from atlas.v2.science.outcomes import (
    AdmissionStateV2,
    DecisionCalendarEntryV2,
    DecisionSourceStageV2,
    ExecutionOutcomeStateV2,
    LabelStateV2,
    MaturedOutcomeV2,
    OutcomeProvenanceV2,
    OutcomeTargetV2,
    SelectionStateV2,
    index_decision_calendar_entry,
    index_matured_outcome,
)
from atlas.v2.science.pretrade import CausalInputV2
from atlas.v2.science.replay import HOUR_NS, ReplayStatusV2
from atlas.v2.selection import accept_research_candidates
from atlas.v2.strategies.s1_trend import (
    S1_POLICY,
    EventGate,
    EventState,
    ExecutableQuote,
    MarkIndexEvidence,
    S1ShadowCoordinator,
)
from atlas.v2.strategies.s2_breakout import S2_POLICY, S2ShadowCoordinator, failed_break_exit

from . import test_session017_replay as replay_module
from . import test_session017_risk as risk_module
from .test_session020_desktop_ipc import _initialized_db

DAY_NS = 86_400_000_000_000
M15_NS = BarIntervalV2.M15.duration_ns
H1_NS = BarIntervalV2.H1.duration_ns
H4_NS = BarIntervalV2.H4.duration_ns
HISTORY_BARS = 2_900


def _kline_values(interval: BarIntervalV2, index: int) -> tuple[str, str, str, str, str]:
    if interval == BarIntervalV2.M15:
        if index < HISTORY_BARS:
            close = Decimal("99") if index % 2 else Decimal("101")
            if index >= HISTORY_BARS - 20:
                close = Decimal("100")
            high = close + Decimal("0.3")
            low = close - Decimal("0.3")
            volume = Decimal("1")
        else:
            close, high, low, volume = Decimal("100.6"), Decimal("100.7"), Decimal("99.5"), Decimal("11")
        open_price = Decimal("100") if index >= HISTORY_BARS - 20 else close
    elif interval == BarIntervalV2.H1:
        close = Decimal("95") + Decimal(index) * Decimal("0.007")
        open_price = close
        high = close + Decimal("0.2")
        low = Decimal("99.5") if index == 723 else close - Decimal("0.2")
        volume = Decimal("100")
    else:
        close = Decimal("90") + Decimal(index) * Decimal("0.0556")
        open_price, high, low, volume = close, close + Decimal("0.3"), close - Decimal("0.3"), Decimal("1000")
    return tuple(str(value) for value in (open_price, high, low, close, volume))  # type: ignore[return-value]


def _ingest_bar(
    collector: PublicCollectorV2,
    *,
    key,
    interval: BarIntervalV2,
    index: int,
    base_ns: int,
    values_override: tuple[str, str, str, str, str] | None = None,
) -> CausalBarV2:
    open_ns = base_ns + index * interval.duration_ns
    close_ns = close_boundary_ns(open_ns, interval)
    values = values_override or _kline_values(interval, index)
    payload = [str(open_ns // 1_000_000), *values, str(Decimal(values[3]) * Decimal(values[4]))]
    raw, translated, translated_open_ns, final = translate_kline(
        payload,
        key=key,
        interval=interval,
        received_at_ns=close_ns,
        server_time_ns=close_ns,
    )
    assert translated_open_ns == open_ns and final is True
    body = canonical_json(payload).encode("utf-8")
    assert hashlib.sha256(body).hexdigest() == raw.raw_payload_hash
    bar = CausalBarV2(
        raw,
        interval,
        translated_open_ns,
        close_ns,
        Decimal(translated["open"]),
        Decimal(translated["high"]),
        Decimal(translated["low"]),
        Decimal(translated["close"]),
        Decimal(translated["volume"]),
        final,
    )
    collector.ingest(raw, raw_payload=body, bar=bar)
    return bar


def _reconstruct(repository: OpsRepository, archive_root: Path, key, cutoff_ns: int):
    store = CausalBarStoreV2()
    by_interval = {}
    for interval in (BarIntervalV2.M15, BarIntervalV2.H1, BarIntervalV2.H4):
        indexed = reconstruct_causal_bars_from_archive(
            repository,
            archive_root,
            key=key,
            interval=interval,
            information_cutoff_ns=cutoff_ns,
        )
        for item in indexed:
            entry = repository.get_artifact(item.observation_index_ref)
            assert entry is not None and entry.artifact_type == "PublicObservationIndexV2"
            assert entry.metadata["instrument_key_json"] == key.to_canonical_json()
            assert entry.metadata["bar_content_hash"] == item.bar.content_hash
            assert item.bar.raw.available_at_ns <= cutoff_ns and item.bar.close_at_ns <= cutoff_ns
            store.append(item.bar)
        by_interval[interval] = indexed
    return store, by_interval


def _universe_from_public_evidence(
    repository: OpsRepository,
    *,
    product: ProductContractV2,
    bars_by_interval,
    health,
    cutoff_ns: int,
) -> UniverseContractV2:
    m15 = tuple(item.bar for item in bars_by_interval[BarIntervalV2.M15])
    h1 = tuple(item.bar for item in bars_by_interval[BarIntervalV2.H1])
    h4 = tuple(item.bar for item in bars_by_interval[BarIntervalV2.H4])
    assert m15 and h1 and h4
    assert all(bar.raw.available_at_ns <= cutoff_ns for bar in m15 + h1 + h4)
    observed_days = (m15[-1].close_at_ns - m15[0].open_at_ns) // DAY_NS
    observation = UniverseObservationV2(
        product,
        observed_days,
        len(m15) >= HISTORY_BARS and len(h1) >= 50 and len(h4) >= 50,
        Decimal("50000000"),
        Decimal("1"),
        PublicSourceStateV2(health.state),
        cutoff_ns,
        {S1_POLICY.policy_id: 30, S2_POLICY.policy_id: 30},
        source_health_available_at_ns=health.available_at_ns,
    )
    universe_observation_ref = observation.content_hash
    observation_wire = {
        "artifact_type": "UniverseObservationV2",
        "product_ref": product.content_hash,
        "observed_days": observation.observed_days,
        "required_bars_present": observation.required_bars_present,
        "trailing_24h_quote_turnover_usd": str(observation.trailing_24h_quote_turnover_usd),
        "spread_bps": str(observation.spread_bps),
        "source_health": observation.source_health.value,
        "source_health_available_at_ns": observation.source_health_available_at_ns,
        "received_at_ns": observation.received_at_ns,
        "strategy_history_days": dict(observation.strategy_history_days),
        "open_position": observation.open_position,
        "active_watch": observation.active_watch,
    }
    assert sha256_json(observation_wire) == universe_observation_ref
    repository.register_artifact(
        ArtifactIndexEntryV2(
            universe_observation_ref,
            "UniverseObservationV2",
            universe_observation_ref,
            cutoff_ns,
            cutoff_ns,
            {"observation": observation_wire},
        )
    )
    public_index_refs = {item.observation_index_ref for values in bars_by_interval.values() for item in values}
    public_input_refs = tuple(sorted(public_index_refs | {health.content_hash}))
    for ref in public_index_refs:
        entry = repository.get_artifact(ref)
        assert entry is not None and entry.available_at_ns <= cutoff_ns
    assert any(
        item.details_ref == health.content_hash and item.available_at_ns <= cutoff_ns
        for item in repository.source_health_history(health.source_id)
    )
    result = DynamicUniverseRuntimeV2().build_snapshot(
        (observation,),
        decision_slot_ns=cutoff_ns,
        information_cutoff_ns=cutoff_ns,
        created_at_ns=cutoff_ns,
        selection_policy_hash=risk_module.SELECTION_POLICY_HASH,
        input_refs=public_input_refs,
    )
    universe = result.universe
    assert universe.entries[0].scanner_eligible
    assert universe.entries[0].strategy_eligibility[S1_POLICY.policy_id].status == EligibilityStatusV2.ELIGIBLE
    assert universe.entries[0].strategy_eligibility[S2_POLICY.policy_id].status == EligibilityStatusV2.ELIGIBLE
    assert set(public_input_refs).issubset(set(universe.envelope.input_refs))
    repository.register_artifact(
        ArtifactIndexEntryV2(
            universe.content_hash,
            "UniverseContractV2",
            universe.content_hash,
            cutoff_ns,
            cutoff_ns,
            {"universe": universe.to_dict()},
        )
    )
    return universe


def _causal_input(repository: OpsRepository, kind: str, cutoff_ns: int) -> CausalInputV2:
    body = {"fixture_kind": kind, "decision_cutoff_ns": cutoff_ns}
    ref = sha256_json(body)
    repository.register_artifact(ArtifactIndexEntryV2(ref, kind, ref, cutoff_ns, cutoff_ns, body))
    return CausalInputV2(ref, kind, cutoff_ns, cutoff_ns)


def _admission_policy() -> AdmissionPolicyV2:
    profile_refs = {
        name: sha256_json({"session020-profile": name}) for name in ("nautilus-artifact", "execution", "protection")
    }
    return AdmissionPolicyV2(
        ADMISSION_POLICY_VERSION,
        Decimal("1"),
        30,
        30,
        20,
        Decimal("1.96"),
        "ISOLATED",
        "ONE_WAY",
        "nautilus_trader",
        "2.0.0rc5",
        "1b0a49d2792a9432a3aca3fcb617ce7a630d905e",
        profile_refs["nautilus-artifact"],
        profile_refs["execution"],
        profile_refs["protection"],
        "SESSION019_CAPABILITY_PROFILE_V1",
    )


def _capability_for_action(action, case, cutoff_ns: int) -> VenueCapabilitySnapshotV2:
    policy = _admission_policy()
    return VenueCapabilitySnapshotV2(
        action.action.key.venue,
        action.action.key.environment,
        case.account.account_scope,
        action.action.product_ref,
        action.action.key.content_hash,
        policy.required_margin_mode,
        policy.required_position_mode,
        policy.required_nautilus_distribution,
        policy.required_nautilus_version,
        policy.required_nautilus_source_commit,
        policy.required_nautilus_artifact_ref,
        policy.required_execution_profile_ref,
        policy.required_protection_profile_ref,
        policy.required_qualification_version,
        VenueCapabilityStatusV2.UNVERIFIED,
        (),
        cutoff_ns,
    )


def _persist_synthetic_marker(repository: OpsRepository, refs: set[str], cutoff_ns: int) -> str:
    marker_body = {"synthetic_fixture": True, "fixture_name": "SESSION020_PHASE2_E2E", "subject_refs": sorted(refs)}
    marker_ref = sha256_json(marker_body)
    repository.register_artifact(
        ArtifactIndexEntryV2(
            marker_ref,
            "SyntheticIntegrationFixtureV1",
            marker_ref,
            cutoff_ns,
            cutoff_ns,
            marker_body,
        )
    )
    return marker_ref


def test_phase2_s1_s2_persistence_ipc_and_matured_outcome(tmp_path, monkeypatch):
    from atlas.v2.desktop.ipc import ProjectionClient, ProjectionService
    from atlas.v2.desktop.projection import (
        DesktopChartSeriesV2,
        DesktopSnapshotV2,
        project_chart_series,
        project_snapshot,
    )

    db = tmp_path / "ops.sqlite"
    _initialized_db(db)
    writer_state = {"active": False, "opens": 0, "readers": 0}
    original_init = OpsRepository.__init__
    original_close = OpsRepository.close

    def guarded_init(self, path, *args, **kwargs):
        same_db = Path(path).resolve() == db.resolve()
        read_only = kwargs.get("read_only", False)
        if read_only:
            assert same_db, "desktop reader must use the decision graph's ops.sqlite"
            writer_state["readers"] += 1
        else:
            if writer_state["active"] or writer_state["opens"]:
                raise AssertionError("a second write-capable OpsRepository was opened")
            if not same_db:
                raise AssertionError("Phase-2 orchestration opened an alternate ops database")
            writer_state["active"] = True
            writer_state["opens"] += 1
        original_init(self, path, *args, **kwargs)

    def guarded_close(self):
        same_db = Path(self.path).resolve() == db.resolve()
        was_writer = same_db and not self.read_only
        try:
            original_close(self)
        finally:
            if was_writer:
                writer_state["active"] = False

    monkeypatch.setattr(OpsRepository, "__init__", guarded_init)
    monkeypatch.setattr(OpsRepository, "close", guarded_close)

    base_ns = (time.time_ns() // H4_NS) * H4_NS - 31 * DAY_NS
    setup_cutoff = base_ns + HISTORY_BARS * M15_NS
    target_cutoff = setup_cutoff + M15_NS
    archive_root = tmp_path / "archive"
    with OpsRepository(db) as repo:
        info_payload = {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "contractType": "LinearPerpetual",
                        "quoteCoin": "USDT",
                        "settleCoin": "USDT",
                        "symbol": "BTCUSDT",
                        "baseCoin": "BTC",
                        "status": "Trading",
                        "priceFilter": {"tickSize": "0.01"},
                        "lotSizeFilter": {
                            "qtyStep": "0.1",
                            "minOrderQty": "0.1",
                            "minNotionalValue": "10",
                            "maxOrderQty": "1000",
                        },
                        "launchTime": str(base_ns // 1_000_000),
                    }
                ]
            },
        }
        (product,) = translate_instrument_info(
            info_payload,
            environment=EnvironmentV2.TESTNET,
            observed_at_ns=base_ns,
            available_at_ns=base_ns,
        )
        key = product.key
        registry = InstrumentRegistryV2()
        registry.register(product)
        risk_module.index_research_evidence(
            repo,
            "BybitInstrumentInfoFixtureV1",
            product.metadata_ref,
            product.available_at_ns,
            info_payload["result"]["list"][0],
        )
        risk_module.index_risk_evidence(repo, product)
        archive = ParquetObservationArchiveV2(archive_root)
        collector = PublicCollectorV2(
            repository=repo,
            registry=registry,
            clock_ns=lambda: base_ns + M15_NS + 1,
            archive=archive,
        )

        # Establish 30 days of synthetic venue payloads through the Bybit translator and collector.
        for interval, count in ((BarIntervalV2.M15, HISTORY_BARS), (BarIntervalV2.H1, 725), (BarIntervalV2.H4, 181)):
            for index in range(count):
                _ingest_bar(collector, key=key, interval=interval, index=index, base_ns=base_ns)
        first_archive_path = collector.flush_archive()
        assert first_archive_path is not None and Path(first_archive_path).is_file()
        health_setup = collector.reconcile_after_reconnect(
            SOURCE_ID,
            at_ns=setup_cutoff,
            complete_snapshot=True,
            missed_interval_repaired=True,
        )
        assert health_setup.state == PublicSourceStateV2.HEALTHY_CURRENT

        setup_store, setup_evidence = _reconstruct(repo, archive_root, key, setup_cutoff)
        setup_m15 = tuple(item.bar for item in setup_evidence[BarIntervalV2.M15])
        assert len(setup_m15) == HISTORY_BARS
        assert setup_m15[-1].close_at_ns == setup_cutoff
        assert setup_m15[-1].raw.available_at_ns == setup_cutoff
        universe_setup = _universe_from_public_evidence(
            repo,
            product=product,
            bars_by_interval=setup_evidence,
            health=health_setup,
            cutoff_ns=setup_cutoff,
        )
        setup_join = asof_join(setup_store, key, cutoff_ns=setup_cutoff, source_health=health_setup)
        assert setup_join.status == "AVAILABLE"
        setup_feature_join = replace(setup_join, m15=setup_join.m15[-60:])
        setup_feature = feature_snapshot(setup_feature_join)
        assert setup_feature.envelope.input_refs
        setup_feature_inputs = {
            bar.content_hash for bar in setup_feature_join.h4 + setup_feature_join.h1 + setup_feature_join.m15
        }
        assert setup_feature_inputs.issubset(set(setup_feature.envelope.input_refs))
        coordinator = S1ShadowCoordinator(repo)
        setup_gate = EventGate(
            EventState.CLEAR, setup_cutoff, sha256_json({"event_gate": setup_cutoff}), "EVENT_GATE_V1"
        )
        watch_result = coordinator.create_watch(
            setup_join,
            setup_feature,
            event_gate=setup_gate,
            universe=universe_setup,
        )
        assert watch_result.status == "WATCH", watch_result.reason
        assert watch_result.watch is not None

        # The trigger candle is ingested only after the point-in-time universe and S1 watch exist.
        trigger_bar = _ingest_bar(
            collector,
            key=key,
            interval=BarIntervalV2.M15,
            index=HISTORY_BARS,
            base_ns=base_ns,
        )
        assert trigger_bar.close_at_ns == target_cutoff
        assert trigger_bar.raw.available_at_ns == target_cutoff
        trigger_archive_path = collector.flush_archive()
        assert trigger_archive_path is not None and Path(trigger_archive_path).is_file()
        collector.on_disconnect(SOURCE_ID, at_ns=target_cutoff - 3)
        collector.begin_reconnect(SOURCE_ID, attempt=0, at_ns=target_cutoff - 2)
        collector.reconnected(SOURCE_ID, at_ns=target_cutoff - 1)
        health_target = collector.reconcile_after_reconnect(
            SOURCE_ID,
            at_ns=target_cutoff,
            complete_snapshot=True,
            missed_interval_repaired=True,
        )
        assert health_target.available_at_ns == target_cutoff
        target_store, target_evidence = _reconstruct(repo, archive_root, key, target_cutoff)
        assert len(target_evidence[BarIntervalV2.M15]) == HISTORY_BARS + 1
        trigger_index = next(
            item for item in target_evidence[BarIntervalV2.M15] if item.bar.content_hash == trigger_bar.content_hash
        )
        trigger_entry = repo.get_artifact(trigger_index.observation_index_ref)
        assert trigger_entry is not None
        assert trigger_entry.metadata["event_at_ns"] == target_cutoff
        assert trigger_entry.available_at_ns == target_cutoff
        assert trigger_entry.metadata["instrument_key_json"] == key.to_canonical_json()

        target_join = asof_join(
            target_store,
            key,
            cutoff_ns=target_cutoff,
            trigger_ref=trigger_bar.content_hash,
            source_health=health_target,
        )
        assert target_join.status == "AVAILABLE"
        assert target_join.m15[-1].content_hash == trigger_bar.content_hash
        assert all(
            item.raw.available_at_ns <= target_cutoff for item in target_join.m15 + target_join.h1 + target_join.h4
        )
        trigger_feature_join = replace(target_join, m15=target_join.m15[-60:])
        trigger_feature = feature_snapshot(trigger_feature_join)
        required_trigger_refs = {
            trigger_bar.content_hash,
            target_join.h1[-1].content_hash,
            target_join.h4[-1].content_hash,
            health_target.content_hash,
        }
        assert required_trigger_refs.issubset(set(trigger_feature.envelope.input_refs))

        quote = ExecutableQuote(
            key,
            Decimal("100.59"),
            Decimal("100.61"),
            target_cutoff,
            target_cutoff,
            sha256_json({"synthetic_bbo": target_cutoff, "key": key.to_dict()}),
        )
        mark = MarkIndexEvidence(
            key,
            Decimal("100.6"),
            Decimal("100.6"),
            target_cutoff,
            sha256_json({"synthetic_mark_index": target_cutoff, "key": key.to_dict()}),
        )
        trigger_gate = EventGate(
            EventState.CLEAR, target_cutoff, sha256_json({"event_gate": target_cutoff}), "EVENT_GATE_V1"
        )
        s1_result = coordinator.on_bar(
            watch_result.watch.watch_id,
            target_join,
            trigger_feature,
            event_gate=trigger_gate,
            bbo=quote,
            mark_index=mark,
        )
        assert s1_result.status == "CANDIDATE", s1_result.reason
        assert s1_result.candidate is not None
        candidate = s1_result.candidate
        assert candidate.snapshot_hash == trigger_feature.content_hash
        assert candidate.envelope.input_refs
        assert trigger_bar.content_hash in candidate.envelope.input_refs

        universe_target = _universe_from_public_evidence(
            repo,
            product=product,
            bars_by_interval=target_evidence,
            health=health_target,
            cutoff_ns=target_cutoff,
        )
        assert trigger_index.observation_index_ref not in universe_setup.envelope.input_refs
        assert trigger_index.observation_index_ref in universe_target.envelope.input_refs
        s2_feature = feature_snapshot(trigger_feature_join)
        s2_result = S2ShadowCoordinator(repo).on_trigger_close(
            target_join,
            s2_feature,
            universe=universe_target,
            bbo=quote,
        )
        assert s2_result.status == "CANDIDATE", s2_result.reason
        assert s2_result.candidate is not None
        assert s2_result.candidate.policy_hash == S2_POLICY.policy_hash
        assert candidate.policy_hash == S1_POLICY.policy_hash
        setup_entry = repo.get_artifact(s2_result.setup_ref)
        assert setup_entry is not None and setup_entry.artifact_type == "S2SetupEvidenceV1"
        later_bars = tuple(
            _ingest_bar(
                collector,
                key=key,
                interval=BarIntervalV2.M15,
                index=HISTORY_BARS + offset,
                base_ns=base_ns,
                values_override=("100", "100.2", "99.8", "100", "1"),
            )
            for offset in (1, 2)
        )
        assert collector.flush_archive() is not None
        later_evidence = tuple(
            item.bar
            for item in reconstruct_causal_bars_from_archive(
                repo,
                archive_root,
                key=key,
                interval=BarIntervalV2.M15,
                information_cutoff_ns=later_bars[-1].close_at_ns,
            )
            if item.bar.open_at_ns >= later_bars[0].open_at_ns
        )
        failed_ref = failed_break_exit(s2_result.candidate, setup_entry.metadata.to_dict(), later_evidence)
        assert failed_ref == later_evidence[0].content_hash
        assert canonical_json(S2_POLICY.management_rule) != canonical_json(S1_POLICY.management_rule)

        # The risk fixture supplies only typed engineering risk inputs. The exact S1/S2 candidates and
        # collector-derived point-in-time universe continue through the shared production CandidateSet.
        case = risk_module.risk_case(
            repo,
            cutoff_ns=target_cutoff,
            universe_override=universe_target,
            product_override=product,
            candidate_override=candidate,
            additional_candidates=(s2_result.candidate,),
        )
        assert case.candidate_set.selected_candidate_id == candidate.candidate_id
        s2_member = next(
            item for item in case.candidate_set.candidates if item.candidate_id == s2_result.candidate.candidate_id
        )
        assert s2_member.policy_id == S2_POLICY.policy_id
        assert s2_member.eligibility_status == EligibilityStatusV2.ELIGIBLE
        assert s2_member.rank == 2
        assert case.candidate_set.selected_candidate_id == candidate.candidate_id
        assert case.candidate_set.selected_candidate_id != s2_result.candidate.candidate_id
        s2_calendar = DecisionCalendarEntryV2(
            case.candidate_set.content_hash,
            s2_result.candidate.content_hash,
            S2_POLICY.policy_id,
            S2_POLICY.version,
            S2_POLICY.policy_hash,
            target_cutoff,
            SelectionStateV2.UNSELECTED,
            AdmissionStateV2.NOT_APPLICABLE,
            None,
            None,
            DecisionSourceStageV2.CANDIDATE_SET,
            (),
            case.candidate_set.content_hash,
            case.candidate_set.envelope.available_at_ns,
            case.candidate_set.envelope.available_at_ns,
        )
        index_decision_calendar_entry(repo, s2_calendar)
        receipt = accept_research_candidates(
            repo,
            case.candidate_set,
            (candidate, s2_result.candidate),
            accepted_at_ns=target_cutoff,
        )[candidate.candidate_id]
        handed = coordinator.accept_handoff(
            watch_result.watch.watch_id,
            candidate.content_hash,
            pipeline_acceptance_ref=receipt,
            accepted_at_ns=target_cutoff,
        )
        assert handed.state.value == "HANDED_OFF"

        sizing = risk_module.size(repo, case)
        assert sizing.status.value == "SIZED"
        action = freeze_action(
            repo,
            candidate=candidate,
            candidate_set=case.candidate_set,
            sizing=sizing,
            product=product,
            policy=S1_POLICY,
            v1=case.v1,
            v2=case.v2,
        )
        admission_policy = _admission_policy()
        evaluated = run_phase2_economic_evaluation(
            repo,
            action=action,
            candidate=candidate,
            candidate_set=case.candidate_set,
            sizing=sizing,
            product=product,
            risk_policy=case.v1,
            risk_policy_v2=case.v2,
            account=case.account,
            fee=case.fee,
            admission_policy=admission_policy,
            capability=_capability_for_action(action, case, target_cutoff),
            model_input=_causal_input(repo, "Session020ModelFixtureV1", target_cutoff),
            calibration_input=_causal_input(repo, "Session020CalibrationFixtureV1", target_cutoff),
            execution_model_input=_causal_input(repo, "Session020ExecutionModelFixtureV1", target_cutoff),
            available_at_ns=target_cutoff + 10,
            scenario_seed=2001,
            scenario_count=100,
        )
        assert evaluated.evaluation.decision.value == "NOT_ESTIMABLE"
        assert evaluated.evaluation.reason_codes
        assert evaluated.calendar_ref
        calendar_entry = repo.get_artifact(evaluated.calendar_ref)
        assert calendar_entry is not None and calendar_entry.artifact_type == "DecisionCalendarEntryV2"
        original_calendar_wire = canonical_json(calendar_entry.metadata["decision_entry"])
        assert (
            calendar_entry.metadata["decision_entry"]["source_stage"] == DecisionSourceStageV2.ECONOMIC_EVALUATION.value
        )
        assert evaluated.evaluation.candidate_ref == candidate.content_hash
        assert evaluated.evaluation.action_hash == action.action.action_hash

        subject_refs = {
            product.content_hash,
            universe_setup.content_hash,
            universe_target.content_hash,
            candidate.content_hash,
            candidate.snapshot_hash,
            case.candidate_set.content_hash,
            s2_result.candidate.content_hash,
            s2_result.setup_ref,
            s2_result.trigger_ref,
            action.content_hash,
            evaluated.evaluation_ref,
            evaluated.calendar_ref,
            *(item.bar.content_hash for values in target_evidence.values() for item in values),
            *(item.observation_index_ref for values in target_evidence.values() for item in values),
        }
        _persist_synthetic_marker(repo, subject_refs, target_cutoff + 10)
        with OpsRepository(db, read_only=True) as projection_reader:
            before_maturity = project_snapshot(projection_reader, now_ns=target_cutoff + 11)
        assert writer_state["active"] and writer_state["opens"] == 1 and writer_state["readers"] == 1
        assert all(row.available_at_ns <= before_maturity.generated_at_ns for row in before_maturity.evidence)
        s1_row = next(row for row in before_maturity.scanner_rows if row.policy_hash == S1_POLICY.policy_hash)
        s2_row = next(row for row in before_maturity.scanner_rows if row.policy_hash == S2_POLICY.policy_hash)
        assert s1_row.evaluation_decision == "NOT_ESTIMABLE"
        assert s1_row.frozen_action_ref == action.content_hash and s1_row.synthetic_fixture
        assert s2_row.selection_state == "UNSELECTED"
        assert s2_row.synthetic_fixture
        assert not any(row.artifact_type == "MaturedOutcomeV2" for row in before_maturity.evidence)
        assert not before_maturity.evidence_truncated
        evidence_types = {row.artifact_type for row in before_maturity.evidence}
        assert {
            "UniverseContractV2",
            "ProductContractV2",
            "FeatureArtifactV2",
            "CandidateSetV2",
            "SizingDecisionV2",
            "ActionArtifactV2",
            "EvaluationArtifactV2",
            "DecisionCalendarEntryV2",
            "S2SetupEvidenceV1",
            "S2TriggerEvidenceV1",
        }.issubset(evidence_types)
        universe_summary = next(
            row for row in before_maturity.evidence if row.content_ref == universe_setup.content_hash
        )
        assert len(universe_summary.input_refs) == 64
        assert "DESKTOP_INPUT_REFS_BOUNDED" in universe_summary.reason_codes
        assert {item.observation_index_ref for values in setup_evidence.values() for item in values}.issubset(
            set(universe_setup.envelope.input_refs)
        )

        with OpsRepository(db, read_only=True) as chart_reader:
            chart = project_chart_series(
                chart_reader,
                key_json=key.to_canonical_json(),
                interval="1H",
                information_cutoff_ns=before_maturity.generated_at_ns,
                archive_root=archive_root,
                limit=3,
            )
            assert chart.state == "AVAILABLE" and chart.bars and chart.synthetic_fixture
            assert chart.reason_code == "SERIES_LIMIT_APPLIED"
            wrong_identity = key.to_dict()
            wrong_identity["venue"] = "BINANCE"
            wrong_identity_json = canonical_json(wrong_identity)
            mismatched_chart = project_chart_series(
                chart_reader,
                key_json=wrong_identity_json,
                interval="1H",
                information_cutoff_ns=before_maturity.generated_at_ns,
                archive_root=archive_root,
                limit=3,
            )
            assert mismatched_chart.state == "UNAVAILABLE" and not mismatched_chart.bars

        # A read-only desktop sees the terminal decision while one writer remains open.
        token = secrets.token_urlsafe(48)
        service = ProjectionService(db, token, archive_root=archive_root)
        service.start()
        host, port = service.address
        try:
            ipc = ProjectionClient(host, port, token)
            projected = DesktopSnapshotV2.from_dict(ipc.request("snapshot"))
            assert projected.freshness_state == "CURRENT"
            assert any(row.content_ref == evaluated.calendar_ref for row in projected.evidence)
            assert token not in projected.to_canonical_json()
            desktop_chart = ipc.request(
                "chart",
                {
                    "key_json": key.to_canonical_json(),
                    "interval": "1H",
                    "information_cutoff_ns": before_maturity.generated_at_ns,
                    "availability_view": "ACTUAL_SYSTEM",
                    "limit": 3,
                },
            )
            assert desktop_chart["state"] == "AVAILABLE" and desktop_chart["synthetic_fixture"] is True
            monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
            from PySide6.QtWidgets import QApplication

            from atlas.desktop.app import AtlasDesktop

            app = QApplication.instance() or QApplication([])
            desktop = AtlasDesktop(ipc)
            desktop._render_chart(DesktopChartSeriesV2.from_dict(desktop_chart))
            app.processEvents()
            assert desktop.tabs.count() == 5
            assert desktop.chart.getPlotItem().items
            assert "information cutoff" in desktop.chart_title.text()

            # A real Qt observer process can die while this writer and its
            # read-only projection service remain available to the runtime.
            child_code = """
import os
from PySide6.QtWidgets import QApplication
from atlas.desktop.app import AtlasDesktop
from atlas.v2.desktop.ipc import ProjectionClient
app = QApplication([])
client = ProjectionClient(os.environ['ATLAS_TEST_HOST'], int(os.environ['ATLAS_TEST_PORT']), os.environ['ATLAS_TEST_TOKEN'])
window = AtlasDesktop(client)
assert [window.tabs.tabText(i) for i in range(window.tabs.count())] == ['Overview', 'Scanner', 'Chart', 'Watches', 'Evidence']
app.processEvents()
os.write(1, b'QT_VIEW_READY\\n')
os._exit(37)
"""
            child_environment = os.environ.copy()
            child_environment.update(
                {
                    "ATLAS_TEST_HOST": host,
                    "ATLAS_TEST_PORT": str(port),
                    "ATLAS_TEST_TOKEN": token,
                    "QT_QPA_PLATFORM": "offscreen",
                    "PYTHONPATH": os.pathsep.join((str(Path.cwd() / "src"), child_environment.get("PYTHONPATH", ""))),
                }
            )
            child = subprocess.Popen(
                [sys.executable, "-c", child_code],
                env=child_environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            try:
                child_stdout, child_stderr = child.communicate(timeout=30)
                assert child.returncode == 37, child_stderr.decode("utf-8", errors="replace")
                assert child_stdout.strip() == b"QT_VIEW_READY"
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait(timeout=5)
                if child.stdout is not None:
                    child.stdout.close()
                if child.stderr is not None:
                    child.stderr.close()
            assert service.is_alive and writer_state["active"] and repo.get_artifact(action.content_hash)

            context = replay_module.replay_context(
                repo,
                case,
                action_override=action,
                decision_cutoff_ns=target_cutoff,
                minutes=(
                    replay_module.minute(target_cutoff),
                    replay_module.minute(
                        target_cutoff + 4 * HOUR_NS,
                        bid="110",
                        ask="110",
                        mark_low="110",
                        mark_high="110",
                        last_low="110",
                        last_high="110",
                    ),
                ),
                closed15m=(trigger_bar, *later_bars),
            )
            replay_payoff = replay_module.run(repo, case, context)
            assert replay_payoff.status == ReplayStatusV2.FULL_FILL and replay_payoff.payoff is not None
            assert context[0].content_hash == action.content_hash
            fees = (replay_payoff.entry.fee if replay_payoff.entry else Decimal(0)) + sum(
                (item.fee for item in replay_payoff.exits), Decimal(0)
            )
            funding = sum((cash for _, cash in replay_payoff.funding_cashflows), Decimal(0))
            outcome = MaturedOutcomeV2(
                decision_ref=evaluated.calendar_ref,
                candidate_set_ref=case.candidate_set.content_hash,
                candidate_ref=candidate.content_hash,
                policy_id=action.action.policy_id,
                policy_version=action.action.policy_version,
                policy_hash=action.action.policy_hash,
                action_hash=action.action.action_hash,
                action_artifact_ref=action.content_hash,
                action_absence_reason=None,
                instrument_revision=key.contract_revision,
                venue=key.venue.value,
                product=key.product.value,
                decision_at_ns=target_cutoff,
                horizon_end_ns=candidate.horizon_end_ns,
                matured_at_ns=replay_payoff.available_at_ns,
                available_at_ns=replay_payoff.available_at_ns + 1,
                label_definition="net_action_value_v2",
                label_view="RECONSTRUCTED_MARKET",
                selection_state=SelectionStateV2.SELECTED,
                admission_state=AdmissionStateV2.NOT_ESTIMABLE,
                execution_state=ExecutionOutcomeStateV2(replay_payoff.status.value),
                label_state=LabelStateV2.MATURED,
                provenance=OutcomeProvenanceV2.SIMULATED,
                payoff_unit="USDT",
                quantity_unit="CONTRACTS",
                gross_payoff=replay_payoff.payoff + fees - funding,
                fees=fees,
                funding_cashflow=funding,
                net_payoff=replay_payoff.payoff,
                fill_quantity=replay_payoff.filled_quantity,
                requested_quantity=action.action.quantity,
                mfe=None,
                mae=None,
                evidence_refs=(replay_payoff.content_hash,),
                execution_evidence_ref=replay_payoff.content_hash,
                extrema_evidence_ref=None,
                actual_closed_source_ref=None,
                evidence_resolution="MINUTE",
                evidence_quality="REPLAY_BOUND",
                ambiguity=(),
                outcome_target=OutcomeTargetV2.EXECUTABLE_ACTION_VALUE,
                diagnostic_value=None,
                diagnostic_unit=None,
                diagnostic_evidence_ref=None,
                actual_action_binding_ref=None,
            )
            outcome_ref = index_matured_outcome(repo, outcome)
            assert outcome_ref == outcome.content_hash
            assert outcome.available_at_ns > calendar_entry.available_at_ns
            assert (
                canonical_json(repo.get_artifact(evaluated.calendar_ref).metadata["decision_entry"])
                == original_calendar_wire
            )
            assert outcome.action_hash == action.action.action_hash
            subject_refs.update({outcome_ref, replay_payoff.content_hash})
            _persist_synthetic_marker(repo, subject_refs, outcome.available_at_ns)

            with OpsRepository(db, read_only=True) as refreshed_reader:
                refreshed = project_snapshot(refreshed_reader, now_ns=outcome.available_at_ns + 1)
                assert (
                    canonical_json(refreshed_reader.get_artifact(evaluated.calendar_ref).metadata["decision_entry"])
                    == original_calendar_wire
                )
            assert any(row.content_ref == evaluated.calendar_ref for row in refreshed.evidence)
            outcome_row = next(row for row in refreshed.evidence if row.content_ref == outcome_ref)
            assert outcome_row.synthetic_fixture
            assert outcome_row.available_at_ns == outcome.available_at_ns
            assert any(row.content_ref == candidate.content_hash for row in refreshed.evidence)
            desktop.close()
        finally:
            service.close()

        # Closing the desktop projection leaves the caller-owned writer/runtime alive; a fresh IPC reconnect sees maturation.
        assert repo.get_artifact(outcome_ref) is not None
        reconnect = ProjectionService(db, token, archive_root=archive_root)
        reconnect.start()
        try:
            reconnect_client = ProjectionClient(*reconnect.address, token)
            reconnected = DesktopSnapshotV2.from_dict(reconnect_client.request("snapshot"))
            assert any(row.content_ref == outcome_ref for row in reconnected.evidence)
        finally:
            reconnect.close()
        assert writer_state["active"] and writer_state["opens"] == 1
        assert writer_state["readers"] >= 4

    assert writer_state["active"] is False
    assert writer_state["opens"] == 1
