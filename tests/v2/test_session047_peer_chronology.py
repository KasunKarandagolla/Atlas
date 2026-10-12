"""S47 adversarial dependency chronology; unchanged S46 identities."""

from dataclasses import replace

import pytest

from . import test_session046_peer_context as peer_module
from .test_session021_s6 import CUTOFF
from .test_session046_peer_context import _request


@pytest.fixture(scope="module")
def peer_fixture(tmp_path_factory):
    return peer_module.peer_fixture.__wrapped__(tmp_path_factory)


def test_s6_must_be_available_before_peer_computation(peer_fixture):
    state = peer_fixture["state"]
    late = replace(state, envelope=replace(state.envelope, available_at_ns=CUTOFF + 5, content_hash=""))
    with pytest.raises(ValueError, match="S6 beta state"):
        _request(peer_fixture, state=late, computed_offset=1, available_offset=10)


def test_s6_available_at_computation_boundary_is_valid(peer_fixture):
    state = peer_fixture["state"]
    boundary = replace(state, envelope=replace(state.envelope, available_at_ns=CUTOFF + 5, content_hash=""))
    assert _request(peer_fixture, state=boundary, computed_offset=5, available_offset=10)


def test_brief_rejects_stale_peer_and_mismatched_universe(peer_fixture):
    from atlas.v2._serialization import sha256_json
    from atlas.v2.contracts import ArtifactEnvelope, CandidateSelectionStatus, CandidateSetV2
    from atlas.v2.features.peer_context import build_peer_relationship
    from atlas.v2.research.decision_brief import BriefLimitsV1, BriefRequestV1, build_decision_brief

    u = peer_fixture["universe"]
    cs = CandidateSetV2(
        ArtifactEnvelope(1, "peer-slot", CUTOFF, CUTOFF, "fixture", (u.content_hash,)),
        "peer-slot",
        u.content_hash,
        sha256_json("selection"),
        (),
        None,
        "A0",
        CandidateSelectionStatus.NO_CANDIDATE,
    )
    peer = build_peer_relationship(_request(peer_fixture))
    request = BriefRequestV1(
        "peer-slot",
        CUTOFF,
        CUTOFF + 100,
        CUTOFF + 10,
        cs,
        u,
        sha256_json("snapshot"),
        sha256_json("research"),
        BriefLimitsV1("FIXTURE_V1", 20, 50000, 5, 0, 50000),
        (),
        (),
        (),
        peer=replace(peer, valid_until_ns=CUTOFF + 9),
    )
    assert "STALE_PEER_CONTEXT" in build_decision_brief(request).reasons
    assert (
        "PEER_PRODUCT_IDENTITY"
        in build_decision_brief(replace(request, peer=replace(peer, universe_ref=sha256_json("revision")))).reasons
    )
