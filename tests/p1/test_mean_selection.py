from dataclasses import replace
from decimal import Decimal as D

import pytest

from atlas.v2.research.mean_evidence import ChartBarV1, ChartEvidenceBundleV1, EvidenceClaimV1, NumericalFactV1
from atlas.v2.research.mean_selection import (
    MeanSelectionRequestV1,
    MeanSelectionResponseV1,
    deterministic_a_control,
    validate_offline_b_selection,
)
from atlas.v2.science.mean_evaluation import index_mean_artifact

from .conftest import H, T
from .test_mean_evaluation import run


def request_response(case, *, decision="SELECT_ONE"):
    eligible = run(case)
    request = MeanSelectionRequestV1(
        eligible.content_hash,
        case.snapshot.content_hash,
        case.snapshot.permission_ref,
        eligible.population.instrument_revision_ref,
        T + 10,
        eligible.population.deadline_ns,
        "nonce",
        (),
    )
    index_mean_artifact(case.repo, request, available_at_ns=T + 10)
    # Lower-score member is intentionally legal under B.
    member = eligible.members[0]
    response = MeanSelectionResponseV1(
        request.content_hash,
        eligible.content_hash,
        case.snapshot.content_hash,
        case.snapshot.permission_ref,
        request.source_prefix_ref,
        "response",
        "nonce",
        decision,
        member.action_ref if decision == "SELECT_ONE" else None,
        T + 11,
        (),
        "ordinary preference among eligible members",
    )
    return eligible, request, response


def validate(case, eligible, request, response, **kw):
    return validate_offline_b_selection(
        case.repo,
        request=request,
        response=response,
        eligible=eligible,
        snapshot=case.snapshot,
        now_ns=T + 12,
        current_evaluation_refs=tuple(e.content_hash for e in eligible.evaluations),
        **kw,
    )


@pytest.mark.parametrize("decision", ["SELECT_ONE", "REJECT_ALL", "ABSTAIN"])
def test_b_legal_decisions_including_lower_rank_without_host_argmax(case, decision):
    e, q, r = request_response(case, decision=decision)
    assert deterministic_a_control(e) == e.members[1].action_ref
    result = validate(case, e, q, r)
    assert result.decision == decision and result.authority == "OFFLINE_RESEARCH_ONLY"
    if decision == "SELECT_ONE":
        assert result.action_ref != deterministic_a_control(e)


@pytest.mark.parametrize(
    "field,value",
    [
        ("action_ref", H("invented")),
        ("action_ref", H("failed-action")),
        ("eligible_set_ref", H("changed-set")),
        ("account_snapshot_ref", H("changed-account")),
        ("permission_ref", H("permission")),
        ("source_prefix_ref", H("new-source")),
        ("request_ref", H("wrong-request")),
        ("nonce", "different"),
        ("returned_at_ns", T + 31_000_000_000),
        ("returned_at_ns", T),
    ],
)
def test_outside_failed_changed_or_expired_responses_refused(case, field, value):
    e, q, r = request_response(case)
    if field == "action_ref" and value == H("failed-action"):
        value = e.evaluations[0].action_ref
    with pytest.raises(ValueError):
        validate(case, e, q, replace(r, **{field: value}))


def test_duplicate_consumption_persists_across_reopen(case):
    from atlas.v2.memory.repository import OpsRepository

    e, q, r = request_response(case)
    validate(case, e, q, r)
    with pytest.raises(ValueError, match="CONSUMED"):
        validate(case, e, q, r)
    path = case.repo.path
    case.repo.close()
    case.repo = OpsRepository(path)
    with pytest.raises(ValueError, match="CONSUMED"):
        validate(case, e, q, r)


def test_provider_unavailable_has_no_deterministic_opening_fallback(case):
    e, q, _ = request_response(case)
    with pytest.raises(ValueError, match="NO_OPENING_FALLBACK"):
        validate(case, e, q, None)


def test_changed_account_generation_cannot_reuse_old_selection(case):
    e, q, r = request_response(case)
    case.snapshot = replace(case.snapshot, order_revision=2)
    with pytest.raises(ValueError, match="CHANGED"):
        validate(case, e, q, r)


def bundle_for(case, e, action_ref):
    inp = list(case.inputs.values())[1]
    fact = NumericalFactV1(
        "registered_close", inp.product.key.content_hash, H("numeric-source"), D("100"), "USDT/base", T - 1, T, T, None
    )
    bar = ChartBarV1(T - 100, T - 1, T, D("100"), D("101"), D("99"), D("100"), D("50"), H("bar-source"))
    bundle = ChartEvidenceBundleV1(
        H("job"),
        e.content_hash,
        action_ref,
        inp.product.key,
        inp.product.content_hash,
        H("source"),
        H("health"),
        H("indicator-definitions"),
        H("template"),
        H("manifest"),
        (bar,),
        (fact,),
        T,
        e.population.deadline_ns,
        "S1",
        "LAST",
        "SUPPLIED_PERPETUAL",
        "RECONSTRUCTED_MARKET",
        "LOCAL_ONLY",
    )
    return bundle, fact


def test_chart_fact_verification_never_overrides_economic_gates(case):
    e, q, r = request_response(case)
    bundle, fact = bundle_for(case, e, r.action_ref)
    q = replace(q, allowed_bundle_refs=(bundle.content_hash,))
    index_mean_artifact(case.repo, q, available_at_ns=T + 10)
    claim = EvidenceClaimV1(bundle.content_hash, fact.content_hash, D("100"), "USDT/base", "AGREES")
    r = replace(r, request_ref=q.content_hash, claims=(claim,))
    with pytest.raises(ValueError, match="ASSERTION"):
        validate(case, e, q, replace(r, claims=(replace(claim, value=D("1000")),)), bundles=(bundle,))
    assert validate(case, e, q, r, bundles=(bundle,)).action_ref == r.action_ref
    # A verified observation is not permission to choose a failed action.
    q = replace(q, nonce="second")
    index_mean_artifact(case.repo, q, available_at_ns=T + 10)
    with pytest.raises(ValueError, match="OUTSIDE"):
        validate(
            case,
            e,
            q,
            replace(
                r,
                request_ref=q.content_hash,
                nonce="second",
                response_id="second",
                action_ref=e.evaluations[0].action_ref,
            ),
            bundles=(bundle,),
        )


def test_selection_decoder_refuses_unknown_fields_and_live_scope(case):
    _, _, r = request_response(case)
    for wire in (dict(r.to_dict(), override=True), dict(r.to_dict(), operation_scope="LIVE_ENTRY")):
        with pytest.raises(ValueError):
            MeanSelectionResponseV1.from_dict(wire)


def test_current_material_revision_projection_required(case):
    e, q, r = request_response(case)
    with pytest.raises(ValueError, match="MATERIAL"):
        validate_offline_b_selection(
            case.repo,
            request=q,
            response=r,
            eligible=e,
            snapshot=case.snapshot,
            now_ns=T + 12,
            current_evaluation_refs=(H("changed-fees-or-uncertainty"),),
        )


def test_prefit_model_expiry_is_revalidated_at_response_time(case):
    for ref, inp in tuple(case.inputs.items()):
        model = replace(inp.model, expires_at_ns=T + 11)
        calibration = replace(inp.calibration, model_ref=model.content_hash)
        allowances = {
            name: replace(getattr(inp, name), model_ref=model.content_hash)
            for name in ("execution_bias", "numerical", "incremental_programme")
        }
        changed = replace(inp, model=model, calibration=calibration, **allowances)
        case.inputs[ref] = changed
        for obj in (model, calibration, *allowances.values()):
            index_mean_artifact(case.repo, obj, available_at_ns=obj.available_at_ns)
    e, q, r = request_response(case)
    assert e.members
    with pytest.raises(ValueError, match="EXPIRED"):
        validate(case, e, q, r)
