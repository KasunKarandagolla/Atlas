"""Session-029 offline tests for the sealed zero-authority shadow action critic."""

from __future__ import annotations

import ast
import json
import socket
import sqlite3
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from atlas.v2._serialization import FrozenMap, canonical_json, sha256_json
from atlas.v2.agent_intelligence import provider as action_provider_module
from atlas.v2.agent_intelligence.broker import (
    ActionAssessmentBrokerCapabilityV1,
    BrokerCapabilityV1,
    BrokerProtocolError,
    InferenceBroker,
    InferenceBrokerClient,
    InferenceBrokerServer,
)
from atlas.v2.agent_intelligence.budget import DeepSeekPriceScheduleV1
from atlas.v2.agent_intelligence.contracts import (
    ACTION_ASSESSMENT_FINDING_TYPES,
    ACTION_ASSESSMENT_SCHEMA_VERSION,
    ActionAssessmentDispatchAuthorizationV1,
    ActionAssessmentRequestV1,
    AgentAssessmentV1,
    BrokerDispatchAuthorizationV1,
    ProviderResultV1,
    SealedActionAssessmentPacketV1,
)
from atlas.v2.agent_intelligence.controller import ActionAssessmentController
from atlas.v2.agent_intelligence.persistence import AGENT_SCHEMA_VERSION, ActionAssessmentRepository
from atlas.v2.agent_intelligence.profile import deepseek_v41_flash_action_critic_profile
from atlas.v2.agent_intelligence.provider import (
    ACTION_CRITIC_SCHEMA_HASH,
    DeepSeekResponsesActionAssessmentProvider,
    ResearchProviderUnavailable,
)
from atlas.v2.agent_intelligence.validation import validate_action_assessment_output
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.runtime import production
from atlas.v2.runtime.action_critic_shadow import ActionAssessmentShadowCoordinator, build_sealed_action_assessment
from atlas.v2.runtime.ops_supervisor import (
    OPS_SUPERVISOR_VERSION,
    PIPELINE_STAGE_ORDER,
    OpsCycleBatchV1,
    OpsRecoverySnapshotV1,
    OpsSourceStateV1,
    OpsSupervisorV2,
    PipelineStageV1,
    _receipt_from_dict,
)
from tests.v2.test_session016_candidate_selection import CUTOFF
from tests.v2.test_session027_ops_supervisor import FakeClock
from tests.v2.test_session027_production import (
    ReconciledFixturePublicSource,
    StaticInputsProvider,
    _production_event,
)

ROOT = Path(__file__).resolve().parents[2]
PRICE_FILE = ROOT / "configs/agent_intelligence/provider_pricing_deepseek_v41_flash_v1.json"
AGENT_LOCK = ROOT / "requirements-agent-lock.txt"
CAPABILITY_KEY = b"s29-action-assessment-capability-key-32bytes"
REASONING_SENTINEL = "S29_PRIVATE_REASONING_SENTINEL_MUST_NOT_CROSS_BOUNDARY"


@pytest.fixture(scope="module")
def frozen_case(tmp_path_factory: pytest.TempPathFactory):
    path = tmp_path_factory.mktemp("s29-full-receipt") / "ops.sqlite"
    with OpsRepository(path) as setup:
        event, inputs, *_ = _production_event(setup)
    source = ReconciledFixturePublicSource(event)
    port = production.ProductionOpsCyclePortV1(
        public_source=source, inputs_provider=StaticInputsProvider(event.event_id, inputs))
    clock = FakeClock(CUTOFF + 100)
    with OpsSupervisorV2(path, port, clock_ns=clock) as supervisor:
        output = supervisor.run_once()
        assert len(output.event_receipts) == 1
        receipt = output.event_receipts[0]
        repository = supervisor.repository
        assert repository is not None
        identity = repository.get_artifact(OpsSupervisorV2._receipt_identity_ref(receipt.event.event_id))
        assert identity is not None
        receipt_ref = str(identity.metadata["receipt_ref"])
    schedule = DeepSeekPriceScheduleV1.load(PRICE_FILE)
    profile = deepseek_v41_flash_action_critic_profile(price_schedule=schedule, agent_lock_path=AGENT_LOCK)
    with OpsRepository(path) as repository:
        sealed = build_sealed_action_assessment(repository=repository, receipt=receipt,
            receipt_ref=receipt_ref, profile=profile)
    return path, receipt, receipt_ref, sealed, profile, schedule


def _valid_output(packet: SealedActionAssessmentPacketV1) -> str:
    return canonical_json({"version": ACTION_ASSESSMENT_SCHEMA_VERSION, "findings": [{
        "finding_type": "ARTIFACT_SEMANTIC_MISMATCH",
        "subject_artifact_ref": packet.economic_evaluation_ref,
        "supporting_evidence_refs": [packet.action_artifact_ref],
        "explanation": "The exact selected evaluation and frozen action are bound for semantic comparison.",
        "field_paths": ["/action_hash"],
    }]})


def _fresh_ledger(path: Path, schedule: DeepSeekPriceScheduleV1) -> ActionAssessmentRepository:
    with OpsRepository(path):
        pass
    return ActionAssessmentRepository(path, price_schedule=schedule)


class _FakeCritic:
    provider_id = "deepseek"
    requested_model_id = "deepseek-flash"
    endpoint = "https://api.deepseek.com/responses"
    task_identity = "ActionAssessmentProvider"

    def __init__(self, output: str | None = None, *, failure: ProviderResultV1 | None = None,
                 on_call: Any = None) -> None:
        self.output = output
        self.failure = failure
        self.on_call = on_call
        self.calls = 0

    def assess(self, *args: Any, **kwargs: Any) -> ProviderResultV1:
        self.calls += 1
        request = kwargs.get("request", args[0] if args else None)
        packet = kwargs.get("packet", args[1] if len(args) > 1 else None)
        if self.on_call is not None:
            self.on_call(kwargs.get("authorization_id", ""), kwargs.get("attempt_id", ""), request, packet)
        if self.failure is not None:
            return self.failure
        return ProviderResultV1(self.output or "", "deepseek-flash", None, False, False, 1_000, 200, "local-mock")


def test_packet_binds_exact_s27_frozen_action_evidence_and_restart_identity(frozen_case):
    path, receipt, receipt_ref, sealed, _profile, _schedule = frozen_case
    packet = sealed.packet
    assert packet.originating_receipt_ref == receipt_ref
    assert packet.decision_event_id == receipt.event.event_id
    assert packet.action_artifact_ref == receipt.action_ref
    assert packet.action_hash == receipt.to_dict()["action_hash"]
    assert packet.candidate_set_ref == receipt.candidate_set_ref
    assert packet.economic_evaluation_ref == receipt.evaluation_ref
    assert packet.selector_policy_hash == packet.summaries[packet.candidate_set_ref]["summary"]["selection_policy_hash"]
    assert packet.selected_candidate_ref == packet.summaries[packet.action_artifact_ref]["summary"]["candidate_ref"]
    assert packet.summaries[packet.m0_model_ref]["summary"]["action_hash"] == packet.action_hash
    assert packet.summaries[packet.m0_model_ref]["summary"]["action_artifact_ref"] == packet.action_artifact_ref
    assert packet.m1_diagnostic_ref == receipt.result.stages[8].artifact_refs[0]
    assert packet.analogue_diagnostic_ref == receipt.result.stages[9].artifact_refs[0]
    assert packet.m1_diagnostic_ref in packet.artifact_refs and packet.analogue_diagnostic_ref in packet.artifact_refs
    assert {packet.pretrade_scenario_ref, packet.estimation_uncertainty_ref, packet.execution_uncertainty_ref,
        packet.numerical_error_ref, packet.support_ref, packet.calibration_ref, packet.ood_ref,
        packet.deterministic_stress_ref, packet.portfolio_ref, packet.portfolio_es_ref}.issubset(packet.artifact_refs)
    assert packet.source_health_ref == receipt_ref
    assert set(packet.market_evidence_refs).issubset(packet.artifact_refs)
    assert packet.source_cutoff_t0_ns <= packet.sealed_cutoff_t_ns < packet.original_deadline_d_ns
    assert packet.content_hash == SealedActionAssessmentPacketV1.from_dict(packet.to_dict()).content_hash
    with OpsRepository(path) as repository:
        replay = build_sealed_action_assessment(repository=repository, receipt=receipt,
            receipt_ref=receipt_ref, profile=frozen_case[4])
    assert replay.packet.packet_ref == packet.packet_ref
    assert replay.request.request_id == sealed.request.request_id
    assert replay.request.content_hash == sealed.request.content_hash


def test_packet_retains_legitimate_m1_and_analogue_not_estimable_status(frozen_case):
    _path, receipt, _receipt_ref, sealed, _profile, _schedule = frozen_case
    packet = sealed.packet
    m1 = packet.summaries[packet.m1_diagnostic_ref]["summary"]
    analogue = packet.summaries[packet.analogue_diagnostic_ref]["summary"]
    assert m1["status"] == "NOT_ESTIMABLE"
    assert analogue.get("support_status", analogue.get("status")) == "NOT_ESTIMABLE"
    assert receipt.result.stages[8].bound_action_hash == receipt.result.stages[9].bound_action_hash == packet.action_hash


def test_packet_contract_rejects_future_ambiguous_and_changed_action_bindings(frozen_case):
    _path, _receipt, _receipt_ref, sealed, _profile, _schedule = frozen_case
    packet = sealed.packet
    future = packet.availability_by_ref.to_dict()
    future[packet.market_evidence_refs[0]] = packet.source_cutoff_t0_ns + 1
    with pytest.raises(ValueError, match="market/news evidence"):
        replace(packet, availability_by_ref=FrozenMap(future))
    with pytest.raises(ValueError, match="summary|deterministic binding"):
        replace(packet, action_hash="0" * 64)
    ambiguous = packet.artifact_types.to_dict()
    ambiguous.pop(packet.portfolio_es_ref)
    with pytest.raises(ValueError, match="inventory|summary"):
        replace(packet, artifact_types=FrozenMap(ambiguous))
    with pytest.raises(ValueError, match="T0 <= T < D"):
        replace(packet, sealed_cutoff_t_ns=packet.original_deadline_d_ns)


@pytest.mark.parametrize(("field", "value"), [
    ("quantity", "999"), ("side", "SHORT"), ("stop_price", "999"), ("policy_hash", "f" * 64),
])
def test_packet_rejects_changed_quantity_side_stop_or_policy(frozen_case, field: str, value: str):
    _path, _receipt, _receipt_ref, sealed, _profile, _schedule = frozen_case
    summaries = sealed.packet.summaries.to_dict()
    action_summary = summaries[sealed.packet.action_artifact_ref]["summary"]
    action_summary["action_identity"][field] = value
    with pytest.raises(ValueError, match="deterministic binding|action identity"):
        replace(sealed.packet, summaries=FrozenMap(summaries))


def test_packet_sealer_skips_when_exact_frozen_action_is_absent(tmp_path: Path):
    path = tmp_path / "no-action.sqlite"
    with OpsRepository(path) as setup:
        event, inputs, *_ = _production_event(setup, missing_account=True)
    port = production.ProductionOpsCyclePortV1(public_source=ReconciledFixturePublicSource(event),
        inputs_provider=StaticInputsProvider(event.event_id, inputs))
    with OpsSupervisorV2(path, port, clock_ns=FakeClock(CUTOFF + 100)) as supervisor:
        receipt = supervisor.run_once().event_receipts[0]
        repo = supervisor.repository
        assert repo is not None
        identity = repo.get_artifact(OpsSupervisorV2._receipt_identity_ref(event.event_id))
        assert identity is not None
        schedule = DeepSeekPriceScheduleV1.load(PRICE_FILE)
        profile = deepseek_v41_flash_action_critic_profile(price_schedule=schedule, agent_lock_path=AGENT_LOCK)
        class SkipRecorder:
            def __init__(self) -> None:
                self.skips: list[tuple[str, str]] = []
                self.dispatches = 0

            def record_skip(self, ref: str, reason: str) -> None:
                self.skips.append((ref, reason))

            def assess(self, *_args: Any) -> None:
                self.dispatches += 1

        controller = SkipRecorder()
        coordinator = ActionAssessmentShadowCoordinator(profile=profile, controller=controller, ledger=None)
        coordinator(receipt, str(identity.metadata["receipt_ref"]), repo)
        assert controller.dispatches == 0
        assert controller.skips == [(str(identity.metadata["receipt_ref"]),
                                     "EXACT_FROZEN_ACTION_OR_EVIDENCE_UNAVAILABLE")]


@pytest.mark.parametrize("mode", ["reverse", "legacy", "ambiguous", "wrong_action", "wrong_result"])
def test_packet_sealer_validates_analogue_result_and_retrieval_pair(frozen_case, mode):
    path, receipt, _receipt_ref, _sealed, profile, _schedule = frozen_case
    with OpsRepository(path) as repository:
        stages = list(receipt.result.stages)
        stage = stages[9]
        result_ref, retrieval_ref = stage.artifact_refs
        if mode == "reverse":
            refs = (retrieval_ref, result_ref)
        elif mode == "legacy":
            refs = (result_ref,)
        elif mode == "ambiguous":
            refs = (result_ref, stages[8].artifact_refs[0])
        else:
            retrieval_entry = repository.get_artifact(retrieval_ref)
            assert retrieval_entry is not None
            body = dict(retrieval_entry.metadata["receipt"])
            body["action_hash" if mode == "wrong_action" else "result_ref"] = "f" * 64
            changed_ref = sha256_json(body)
            repository.register_artifact(ArtifactIndexEntryV2(changed_ref,
                "RuntimeAnalogueRetrievalReceiptV1", changed_ref,
                retrieval_entry.created_at_ns, retrieval_entry.available_at_ns, {"receipt": body}))
            refs = (result_ref, changed_ref)
        stages[9] = replace(stage, artifact_refs=refs)
        changed_receipt = replace(receipt, result=replace(receipt.result, stages=tuple(stages)))
        changed_receipt_ref = sha256_json({"test_receipt": changed_receipt.content_hash})
        repository.register_artifact(ArtifactIndexEntryV2(changed_receipt_ref, "OpsSupervisorReceiptV1",
            changed_receipt.content_hash, receipt.created_at_ns, receipt.created_at_ns,
            {"receipt": changed_receipt.to_dict()}))
        if mode in {"reverse", "legacy"}:
            packet = build_sealed_action_assessment(repository=repository, receipt=changed_receipt,
                receipt_ref=changed_receipt_ref, profile=profile).packet
            assert packet.analogue_diagnostic_ref == result_ref
        else:
            with pytest.raises(Exception, match="ANALOGUE_RETRIEVAL_(BINDING_MISMATCH|EVIDENCE_AMBIGUOUS)"):
                build_sealed_action_assessment(repository=repository, receipt=changed_receipt,
                    receipt_ref=changed_receipt_ref, profile=profile)


def test_packet_sealer_rejects_ambiguous_portfolio_es(frozen_case):
    path, receipt, receipt_ref, _sealed, profile, _schedule = frozen_case
    with OpsRepository(path) as repository:
        es_ref = _sealed.packet.portfolio_es_ref
        es_entry = repository.get_artifact(es_ref)
        assert es_entry is not None
        body = dict(es_entry.metadata["evidence"])
        body["reason"] = "second eligible exact-match ES record"
        extra_ref = sha256_json(body)
        repository.register_artifact(ArtifactIndexEntryV2(extra_ref, "PortfolioESV2", extra_ref,
            es_entry.created_at_ns, es_entry.available_at_ns, {"evidence": body}))
        with pytest.raises(Exception, match="PORTFOLIO_ES_EVIDENCE_AMBIGUOUS_OR_MISSING"):
            build_sealed_action_assessment(repository=repository, receipt=receipt,
                receipt_ref=receipt_ref, profile=profile)


@pytest.mark.parametrize("bad_output", [
    "{not-json",
    "{\"version\":\"ATLAS_ACTION_CRITIC_OUTPUT_V1\",\"findings\":[",
    canonical_json({"version": ACTION_ASSESSMENT_SCHEMA_VERSION, "findings": [], "confidence": 0.9}),
])
def test_critic_validator_rejects_malformed_truncated_and_unknown_fields(frozen_case, bad_output: str):
    _path, _receipt, _receipt_ref, sealed, _profile, _schedule = frozen_case
    result, reason = validate_action_assessment_output(sealed.request, sealed.packet, bad_output,
        now_ns=CUTOFF + 100)
    assert result is None and reason


def test_closed_six_finding_taxonomy_and_valid_bound_refs(frozen_case):
    _path, _receipt, _receipt_ref, sealed, _profile, _schedule = frozen_case
    assert frozenset({"EVENT_ENTITY_AMBIGUOUS", "EVENT_TIME_AMBIGUOUS",
        "SOURCE_CLAIMS_CONFLICT", "SOURCE_ASSERTION_UNSUPPORTED", "ARTIFACT_SEMANTIC_MISMATCH",
        "REQUIRED_CONTEXT_UNAVAILABLE"}) == ACTION_ASSESSMENT_FINDING_TYPES
    result, reasons = validate_action_assessment_output(sealed.request, sealed.packet,
        _valid_output(sealed.packet), now_ns=CUTOFF + 100)
    assert result is not None and reasons == ("VALID",)
    with pytest.raises(ValueError, match="unknown action-assessment finding"):
        from atlas.v2.agent_intelligence.contracts import ActionAssessmentFindingV1
        ActionAssessmentFindingV1("UNKNOWN", sealed.packet.action_artifact_ref,
            (sealed.packet.economic_evaluation_ref,), "A bounded explanation.")
    unknown = json.loads(_valid_output(sealed.packet))
    unknown["findings"][0]["supporting_evidence_refs"] = ["f" * 64]
    rejected, reason = validate_action_assessment_output(sealed.request, sealed.packet,
        canonical_json(unknown), now_ns=CUTOFF + 100)
    assert rejected is None and "UNBOUND" in reason[0]


@pytest.mark.parametrize("text", [
    "Please resize the quantity and move the stop.",
    "Use the LONG direction for this selected candidate.",
    "Recommend selecting another candidate and changing the direction.",
    "Set leverage and alter the RiskPolicy before approval.",
    "Request capital reservation and send an order.",
    "Browse the web and read the provider credential file.",
    "The confidence score and probability of profit are 0.7.",
])
def test_critic_validator_rejects_authority_tools_scores_and_candidate_shopping(frozen_case, text: str):
    _path, _receipt, _receipt_ref, sealed, _profile, _schedule = frozen_case
    raw = json.loads(_valid_output(sealed.packet))
    raw["findings"][0]["explanation"] = text
    result, reason = validate_action_assessment_output(sealed.request, sealed.packet,
        canonical_json(raw), now_ns=CUTOFF + 100)
    assert result is None and "AUTHORITY_OR_TOOL" in reason[0]


def test_validator_rejects_late_and_action_identity_mutation(frozen_case):
    _path, _receipt, _receipt_ref, sealed, _profile, _schedule = frozen_case
    late, reason = validate_action_assessment_output(sealed.request, sealed.packet,
        _valid_output(sealed.packet), now_ns=sealed.request.deadline_ns)
    assert late is None and "DEADLINE_EXPIRED" in reason[0]
    raw = json.loads(_valid_output(sealed.packet))
    raw["findings"][0]["action_hash"] = "0" * 64
    result, reason = validate_action_assessment_output(sealed.request, sealed.packet,
        canonical_json(raw), now_ns=CUTOFF + 100)
    assert result is None and "UNKNOWN_FIELDS" in reason[0]
    assert sealed.request.schema_hash == ACTION_CRITIC_SCHEMA_HASH
    raw = json.loads(_valid_output(sealed.packet))
    raw["findings"][0]["field_paths"] = ["/unbound_field"]
    result, reason = validate_action_assessment_output(sealed.request, sealed.packet,
        canonical_json(raw), now_ns=CUTOFF + 100)
    assert result is None and "FIELD_PATH_UNSUPPORTED" in reason[0]


def test_controller_persists_dispatch_before_one_provider_call_and_reuses_result(frozen_case, tmp_path: Path):
    _path, receipt, _receipt_ref, sealed, profile, schedule = frozen_case
    db = tmp_path / "controller.sqlite"
    now = [CUTOFF + 100]
    with OpsRepository(db):
        pass
    ledger = ActionAssessmentRepository(db, price_schedule=schedule)

    def verify_persisted(authorization_id: str, attempt_id: str, _request: Any, _packet: Any) -> None:
        with sqlite3.connect(db) as connection:
            assert connection.execute("SELECT COUNT(*) FROM agent_action_assessment_requests").fetchone()[0] == 1
            assert connection.execute("SELECT COUNT(*) FROM agent_action_assessment_attempts WHERE attempt_id=?",
                                     (attempt_id,)).fetchone()[0] == 1
            assert connection.execute("SELECT COUNT(*) FROM agent_action_assessment_reservations WHERE attempt_id=?",
                                     (attempt_id,)).fetchone()[0] == 1
            assert connection.execute("SELECT COUNT(*) FROM agent_action_assessment_dispatches WHERE authorization_id=?",
                                     (authorization_id,)).fetchone()[0] == 1

    fake = _FakeCritic(_valid_output(sealed.packet), on_call=verify_persisted)
    controller = ActionAssessmentController(ledger=ledger, profile=profile,
        capability_signing_key=CAPABILITY_KEY, provider=fake, now_ns=lambda: now[0])
    first = controller.assess(sealed.request, sealed.packet)
    replay = controller.assess(sealed.request, sealed.packet)
    assert first.status == "COMPLETE" and first.accepted_shadow_evidence
    assert first.result is not None and replay.result == first.result
    assert fake.calls == 1
    with sqlite3.connect(db) as connection:
        assert connection.execute("SELECT COUNT(*) FROM agent_action_assessment_acceptances").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM agent_authorities").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM agent_broker_dispatches").fetchone()[0] == 0
        contents = "\n".join(str(row[0]) for row in connection.execute(
            "SELECT result_json FROM agent_action_assessment_outcomes").fetchall())
        assert "raw_output" not in contents and "reasoning" not in contents
    assert receipt.to_dict()["action_hash"] == sealed.packet.action_hash
    ledger.close()


@pytest.mark.parametrize(("failure", "expected"), [
    (ProviderResultV1("", "deepseek-flash", None, True, False, 0, 0, None), "REFUSED"),
    (ProviderResultV1("", "deepseek-flash", None, False, True, 0, 0, None, "PROVIDER_TRUNCATED"), "INVALID"),
    (ProviderResultV1("{broken", "deepseek-flash", None, False, False, 1, 1, None), "INVALID"),
    (ProviderResultV1("", "different-model", None, False, False, 1, 1, None), "INVALID"),
    (ProviderResultV1("", None, None, False, False, 0, 0, None, "PROVIDER_TIMEOUT"), "UNAVAILABLE"),
])
def test_provider_refusal_timeout_truncation_and_invalid_output_have_no_authority(
        frozen_case, tmp_path: Path, failure: ProviderResultV1, expected: str):
    _path, _receipt, _receipt_ref, sealed, profile, schedule = frozen_case
    db = tmp_path / f"failure-{expected}-{time.time_ns()}.sqlite"
    with OpsRepository(db):
        pass
    ledger = ActionAssessmentRepository(db, price_schedule=schedule)
    fake = _FakeCritic(failure=failure)
    controller = ActionAssessmentController(ledger=ledger, profile=profile,
        capability_signing_key=CAPABILITY_KEY, provider=fake, now_ns=lambda: CUTOFF + 100)
    outcome = controller.assess(sealed.request, sealed.packet)
    assert outcome.status == expected and not outcome.accepted_shadow_evidence
    assert fake.calls == 1
    with sqlite3.connect(db) as connection:
        assert connection.execute("SELECT COUNT(*) FROM agent_authorities").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM agent_action_assessment_acceptances").fetchone()[0] == 0
    ledger.close()


def test_late_output_and_expired_deadline_never_dispatch_or_become_eligible(frozen_case, tmp_path: Path):
    _path, _receipt, _receipt_ref, sealed, profile, schedule = frozen_case
    expired_db = tmp_path / "expired-before-dispatch.sqlite"
    with OpsRepository(expired_db):
        pass
    ledger = ActionAssessmentRepository(expired_db, price_schedule=schedule)
    fake = _FakeCritic(_valid_output(sealed.packet))
    controller = ActionAssessmentController(ledger=ledger, profile=profile, capability_signing_key=CAPABILITY_KEY,
        provider=fake, now_ns=lambda: sealed.request.deadline_ns)
    expired = controller.assess(sealed.request, sealed.packet)
    assert expired.status == "EXPIRED" and fake.calls == 0
    assert not ledger.has_dispatch(sealed.request.request_id)
    ledger.close()

    late_db = tmp_path / "late-output.sqlite"
    with OpsRepository(late_db):
        pass
    late_ledger = ActionAssessmentRepository(late_db, price_schedule=schedule)
    clock = [CUTOFF + 100]
    def become_late(*_args: Any) -> ProviderResultV1:
        clock[0] = sealed.request.deadline_ns
        return ProviderResultV1(_valid_output(sealed.packet), "deepseek-flash", None, False, False, 10, 10)
    class LateCritic:
        calls = 0
        def assess(self, **_kwargs: Any) -> ProviderResultV1:
            self.calls += 1
            return become_late()
    provider = LateCritic()
    late_controller = ActionAssessmentController(ledger=late_ledger, profile=profile,
        capability_signing_key=CAPABILITY_KEY, provider=provider, now_ns=lambda: clock[0])
    late = late_controller.assess(sealed.request, sealed.packet)
    assert late.status == "EXPIRED" and not late.accepted_shadow_evidence and provider.calls == 1
    with sqlite3.connect(late_db) as connection:
        assert connection.execute("SELECT eligible FROM agent_action_assessment_outcomes").fetchone()[0] == 0
    late_ledger.close()


def test_restart_after_durable_dispatch_finishes_unavailable_without_redispatch(frozen_case, tmp_path: Path):
    _path, _receipt, _receipt_ref, sealed, profile, schedule = frozen_case
    db = tmp_path / "restart-dispatched.sqlite"
    with OpsRepository(db):
        pass
    ledger = ActionAssessmentRepository(db, price_schedule=schedule)
    ledger.persist_packet_request(sealed.packet, sealed.request, now_ns=CUTOFF + 100)
    attempt = "b550c87e-bd2c-5818-a142-c2f6a70443e3"
    reservation = "88a7a3e8-a04f-52e7-b727-554fa8724f4c"
    authorization_id = "c3505ce3-42cd-57d2-9a87-2f09ad307048"
    nonce = "0f43af70-e1a6-5a03-ab61-f7fb1878f9c3"
    ledger.create_attempt(sealed.request.request_id, attempt_id=attempt, now_ns=CUTOFF + 100)
    reserved = schedule.worst_case_call_usd(input_tokens=12_000, output_tokens=2_048)
    ledger.reserve_cost(sealed.request.request_id, attempt_id=attempt, reservation_id=reservation,
        now_ns=CUTOFF + 100, reserved_usd=reserved)
    auth = ActionAssessmentDispatchAuthorizationV1.create(task_identity=sealed.request.task_identity,
        packet_ref=sealed.packet.packet_ref, packet_hash=sealed.packet.content_hash,
        request_hash=sealed.request.content_hash, action_hash=sealed.packet.action_hash,
        profile_hash=profile.content_hash, provider_binding_hash=profile.provider_binding_hash,
        price_schedule_hash=profile.price_schedule_hash, provider=profile.provider,
        requested_model_id=profile.requested_model_id, endpoint=profile.endpoint,
        attempt_id=attempt, deadline_ns=sealed.request.deadline_ns, authorized_at_ns=CUTOFF + 100,
        expires_at_ns=min(CUTOFF + 1_000_000, sealed.request.deadline_ns), reservation_id=reservation,
        reserved_cost_usd=str(reserved), max_input_tokens=12_000, max_output_tokens=2_048,
        authorization_id=authorization_id, capability_nonce=nonce)
    ledger.persist_dispatch_authorization(auth, now_ns=CUTOFF + 100)
    fake = _FakeCritic(_valid_output(sealed.packet))
    controller = ActionAssessmentController(ledger=ledger, profile=profile,
        capability_signing_key=CAPABILITY_KEY, provider=fake, now_ns=lambda: CUTOFF + 200)
    outcome = controller.assess(sealed.request, sealed.packet)
    assert outcome.status == "UNAVAILABLE" and fake.calls == 0
    assert outcome.reason_code == "DISPATCH_OUTCOME_LOST_ON_RESTART"
    ledger.close()


def test_proposal_and_critic_capabilities_are_disjoint(frozen_case):
    _path, _receipt, _receipt_ref, sealed, profile, schedule = frozen_case
    fake = _FakeCritic(_valid_output(sealed.packet))
    broker = InferenceBroker(fake, signing_key=CAPABILITY_KEY, model_profile=profile,
        price_schedule_hash=schedule.content_hash, now_ns=lambda: CUTOFF + 200)
    request = sealed.request
    auth = ActionAssessmentDispatchAuthorizationV1.create(task_identity=request.task_identity,
        packet_ref=sealed.packet.packet_ref, packet_hash=sealed.packet.content_hash,
        request_hash=request.content_hash, action_hash=sealed.packet.action_hash,
        profile_hash=profile.content_hash, provider_binding_hash=profile.provider_binding_hash,
        price_schedule_hash=schedule.content_hash, provider="deepseek", requested_model_id="deepseek-flash",
        endpoint="https://api.deepseek.com/responses", attempt_id="49a3d37c-baf8-5d01-8479-c2ad957b5573",
        deadline_ns=request.deadline_ns, authorized_at_ns=CUTOFF + 100,
        expires_at_ns=CUTOFF + 1_000_000, reservation_id="e8e573aa-45d5-57d0-9682-e32a550f86f7",
        reserved_cost_usd="0.006058", max_input_tokens=12_000, max_output_tokens=2_048,
        authorization_id="d5ddf799-ab68-5b88-bfd2-3c1790a73f07",
        capability_nonce="9026d765-8cb9-55c0-8f9a-8984be8f6de4")
    critic_cap = ActionAssessmentBrokerCapabilityV1.issue_authorized(authorization=auth,
        signing_key=CAPABILITY_KEY).capability
    with pytest.raises(BrokerProtocolError, match="ACTION_ASSESSMENT_CAPABILITY_CANNOT_INVOKE_RESEARCH"):
        broker.infer(capability=critic_cap, job_id="00000000-0000-4000-8000-000000000001",
            attempt_id=auth.attempt_id, lease_epoch=1, call_index=1,
            request_data={}, evidence=[])
    proposal_auth = BrokerDispatchAuthorizationV1.create(job_id="00000000-0000-4000-8000-000000000002",
        request_key="a" * 64, request_hash="b" * 64, attempt_id="00000000-0000-4000-8000-000000000003",
        call_index=1, lease_epoch=1, authorization_id="00000000-0000-4000-8000-000000000004",
        capability_nonce="00000000-0000-4000-8000-000000000005", evidence_hash=sha256_json([]),
        model_profile_hash="c" * 64, provider="openai", requested_model_id="gpt-6-astra",
        deadline_ns=CUTOFF + 2_000_000, authorized_at_ns=CUTOFF + 100,
        expires_at_ns=CUTOFF + 1_000_000, budget_reservation_id="00000000-0000-4000-8000-000000000006",
        reserved_cost_usd="0.35", max_input_tokens=12_000, max_output_tokens=4_000)
    proposal_cap = BrokerCapabilityV1.issue_authorized(authorization=proposal_auth,
        signing_key=CAPABILITY_KEY).capability
    with pytest.raises(BrokerProtocolError, match="INVALID_OR_EXPIRED_ACTION_ASSESSMENT_CAPABILITY"):
        broker.assess_action_v1(capability=proposal_cap, authorization_id=proposal_auth.authorization_id,
            attempt_id=proposal_auth.attempt_id, request_data=request.to_dict(), packet_data=sealed.packet.to_dict(),
            now_ns=CUTOFF + 200)


def test_action_broker_local_socket_direct_operation_has_one_provider_call(frozen_case, tmp_path: Path):
    _path, _receipt, _receipt_ref, sealed, profile, schedule = frozen_case
    fake = _FakeCritic(_valid_output(sealed.packet))
    broker = InferenceBroker(fake, signing_key=CAPABILITY_KEY, model_profile=profile,
        price_schedule_hash=schedule.content_hash, now_ns=lambda: CUTOFF + 200)
    attempt, reservation, authorization_id, nonce = (
        "1c6cc9be-267b-5d7a-84db-91bc5217c90b", "aacfcaf9-746e-541a-8b7e-a9e8eabf28ab",
        "c4dbd1c3-3745-55c6-8a32-b5c441dc869a", "f8c2bc63-392d-56a7-8b13-17c865b953e5")
    auth = ActionAssessmentDispatchAuthorizationV1.create(task_identity=sealed.request.task_identity,
        packet_ref=sealed.packet.packet_ref, packet_hash=sealed.packet.content_hash,
        request_hash=sealed.request.content_hash, action_hash=sealed.packet.action_hash,
        profile_hash=profile.content_hash, provider_binding_hash=profile.provider_binding_hash,
        price_schedule_hash=schedule.content_hash, provider=profile.provider,
        requested_model_id=profile.requested_model_id, endpoint=profile.endpoint,
        attempt_id=attempt, deadline_ns=sealed.request.deadline_ns, authorized_at_ns=CUTOFF + 100,
        expires_at_ns=CUTOFF + 1_000_000, reservation_id=reservation, reserved_cost_usd="0.006058",
        max_input_tokens=12_000, max_output_tokens=2_048, authorization_id=authorization_id,
        capability_nonce=nonce)
    cap = ActionAssessmentBrokerCapabilityV1.issue_authorized(authorization=auth,
        signing_key=CAPABILITY_KEY).capability
    socket_path = tmp_path / "action-critic.sock"
    server = InferenceBrokerServer(socket_path, broker)
    server.start()
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as channel:
            channel.connect(str(socket_path))
            result = InferenceBrokerClient(channel).assess_action_v1(capability=cap,
                authorization_id=authorization_id, attempt_id=attempt, request=sealed.request,
                packet=sealed.packet)
    finally:
        server.close()
    assert result.raw_output == _valid_output(sealed.packet)
    assert fake.calls == 1


def test_action_critic_provider_uses_local_mock_transport_and_discards_reasoning(frozen_case):
    httpx2 = pytest.importorskip("httpx2")
    _path, _receipt, _receipt_ref, sealed, _profile, _schedule = frozen_case
    calls: list[dict[str, Any]] = []
    output_text = _valid_output(sealed.packet)
    def handler(request):
        calls.append({"path": request.url.path, "authorization": request.headers.get("authorization"),
                      "body": json.loads(request.content)})
        return httpx2.Response(200, json={"id": "resp-s29-local", "object": "response",
            "created_at": 1, "model": "deepseek-flash", "status": "completed",
            "output": [{"id": "reasoning", "type": "reasoning", "summary": [{"type": "summary_text",
                "text": REASONING_SENTINEL}]}, {"id": "message", "type": "message", "status": "completed",
                "role": "assistant", "content": [{"type": "output_text", "text": output_text}]}],
            "usage": {"input_tokens": 1_200, "output_tokens": 100}})
    transport = httpx2.MockTransport(handler)
    provider = DeepSeekResponsesActionAssessmentProvider("s29mock",
        http_client_factory=lambda timeout: httpx2.AsyncClient(transport=transport, trust_env=False,
                                                               timeout=timeout),
        now_ns=lambda: CUTOFF + 100)
    result = provider.assess(sealed.request, sealed.packet)
    assert len(calls) == 1 and calls[0]["path"] == "/responses"
    assert calls[0]["authorization"] == "Bearer s29mock"
    assert calls[0]["body"]["model"] == "deepseek-flash"
    assert calls[0]["body"]["reasoning"] == {"effort": "high"}
    assert "tools" not in calls[0]["body"]
    assert result.raw_output == output_text and REASONING_SENTINEL not in result.raw_output


def test_action_adapter_enforces_input_token_ceiling_before_transport(frozen_case, monkeypatch):
    _path, _receipt, _receipt_ref, sealed, _profile, _schedule = frozen_case
    client_factories: list[float] = []
    monkeypatch.setattr(action_provider_module, "_action_assessment_input_token_count", lambda _body: 12_001)
    provider = DeepSeekResponsesActionAssessmentProvider("offline-test-key",
        http_client_factory=lambda timeout: client_factories.append(timeout), now_ns=lambda: CUTOFF + 100)
    with pytest.raises(ResearchProviderUnavailable, match="INPUT_TOKEN_BUDGET_EXCEEDED"):
        provider.assess(sealed.request, sealed.packet)
    assert client_factories == []


def test_s27_receipt_pipeline_and_old_agent_wire_remain_unchanged(frozen_case):
    _path, receipt, _receipt_ref, sealed, _profile, _schedule = frozen_case
    assert [stage.value for stage in PipelineStageV1] == [
        "UNIVERSE", "CAUSAL_FEATURES", "WATCHES_AND_SLEEVES", "CANDIDATE_SET", "SELECTION",
        "HARD_RISK", "FROZEN_ACTION", "ECONOMIC_EVALUATION", "M1_DIAGNOSTIC", "ANALOGUE_DIAGNOSTIC",
        "DECISION_CALENDAR",
    ]
    assert [stage.value for stage in PIPELINE_STAGE_ORDER] == [stage.value for stage in PipelineStageV1]
    assert len(PIPELINE_STAGE_ORDER) == 11 and OPS_SUPERVISOR_VERSION == "ATLAS_OPS_SUPERVISOR_V2_V1"
    restored = _receipt_from_dict(receipt.to_dict())
    assert restored.to_dict() == receipt.to_dict() and restored.content_hash == receipt.content_hash
    assert receipt.agent_mode == "DISABLED" and not receipt.capital_enabled and not receipt.assisted_enabled
    assert AGENT_SCHEMA_VERSION == 2
    old_request = ActionAssessmentRequestV1("00000000-0000-4000-8000-000000000011",
        sealed.packet.action_artifact_ref, (), sealed.packet.original_deadline_d_ns, ACTION_CRITIC_SCHEMA_HASH)
    old_assessment = AgentAssessmentV1(old_request.request_id, "COMPLETE", ())
    assert old_request.to_dict()["version"] == "ActionAssessmentRequestV1"
    assert old_assessment.to_dict()["version"] == "AgentAssessmentV1"
    assert "action_assessment" not in receipt.to_dict()


def test_critic_code_has_no_risk_execution_or_tradeplan_authority_imports():
    agent_root = ROOT / "src/atlas/v2/agent_intelligence"
    for name in ("controller.py", "provider.py"):
        tree = ast.parse((agent_root / name).read_text(encoding="utf-8"))
        imports = {node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
        names = {alias.name for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) for alias in node.names}
        assert not any(item.endswith(".risk") or item.endswith(".execution") for item in imports)
        assert "TradePlan" not in names and "RiskPolicy" not in names
    broker_tree = ast.parse((agent_root / "broker.py").read_text(encoding="utf-8"))
    broker_imports = {node.module or "" for node in ast.walk(broker_tree) if isinstance(node, ast.ImportFrom)}
    broker_names = {alias.name for node in ast.walk(broker_tree) if isinstance(node, ast.ImportFrom) for alias in node.names}
    assert "sqlite3" not in broker_imports and "OpsRepository" not in broker_names
    broker_source = (agent_root / "broker.py").read_text(encoding="utf-8")
    assert "OpsRepository" not in broker_source and "sqlite3" not in broker_source
    controller_source = (agent_root / "controller.py").read_text(encoding="utf-8")
    assert "AgentWorkerSupervisor" not in controller_source or "ActionAssessmentController" in controller_source


def test_deterministic_s27_receipt_is_unchanged_when_shadow_callback_fails(frozen_case):
    path, receipt, _receipt_ref, _sealed, _profile, _schedule = frozen_case
    event = receipt.event

    class ReplayPort:
        def recover(self, _repository, *, now_ns):
            state = OpsSourceStateV1("PUBLIC_MARKET", "HEALTHY_CURRENT", now_ns, now_ns)
            return OpsRecoverySnapshotV1(("PUBLIC_MARKET",), (state,), (), None, True, now_ns)

        def collect(self, _repository, *, now_ns, recovery):
            del recovery
            state = OpsSourceStateV1("PUBLIC_MARKET", "HEALTHY_CURRENT", now_ns, now_ns)
            return OpsCycleBatchV1((event,), (state,), ("PUBLIC_MARKET",), (), True, now_ns)

    calls: list[str] = []

    def failed_shadow_callback(callback_receipt, _ref, _repository) -> None:
        calls.append(callback_receipt.content_hash)
        raise RuntimeError("offline critic failure")

    with OpsSupervisorV2(path, ReplayPort(), clock_ns=FakeClock(CUTOFF + 200),
                         post_receipt_shadow=failed_shadow_callback) as supervisor:
        run = supervisor.run_once()
        assert run.event_receipts, run.cycle.to_dict()
        replay = run.event_receipts[0]
    assert calls == [receipt.content_hash]
    assert replay.to_dict() == receipt.to_dict()
    assert replay.content_hash == receipt.content_hash
