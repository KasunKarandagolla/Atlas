"""Adversarial S47 reproductions; sources are independent of proposed fact values."""

import socket
import struct
from dataclasses import replace

import pytest

from atlas.v2._serialization import FrozenMap, canonical_json, sha256_json
from atlas.v2.agent_intelligence import event_extraction_transport as transport
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.research.decision_brief import BriefFactV1, build_decision_brief

from .test_session014_core import KEY
from .test_session041_event_extraction_transport import SIGNING_KEY, FakeProvider
from .test_session047_decision_brief import request


def test_timely_unrelated_artifact_cannot_certify_fabricated_market_fact(tmp_path):
    r = request()
    source = {"unrelated": "real archived event"}
    ref = sha256_json(source)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        repo.register_artifact(
            ArtifactIndexEntryV2(ref, "ResearchEventFixtureV1", ref, r.cutoff_ns, r.cutoff_ns, source)
        )
        fabricated = BriefFactV1(
            "technical", ref, KEY, r.cutoff_ns, r.cutoff_ns, r.deadline_ns, "AVAILABLE", FrozenMap({"rsi14": 99}), ()
        )
        brief = build_decision_brief(replace(r, facts=(fabricated,)), repository=repo)
        assert brief.status == "BLOCKED"
        assert "FACT_SOURCE_BINDING_INVALID" in brief.reasons


def test_saturation_receipt_waits_for_bounded_request_read_before_connection_close(tmp_path):
    broker = transport.EventExtractionBroker(
        FakeProvider(), signing_key=SIGNING_KEY, max_active=1, clock_ns=lambda: 100
    )
    with transport.EventExtractionUnixServer(tmp_path / "reject.sock", broker) as server:
        # Hold the sole handler slot deterministically; no model invocation.
        assert server._slots.acquire(blocking=False)
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as channel:
                channel.settimeout(1)
                channel.connect(str(server.socket_path))
                raw = canonical_json({"request_id": "saturation-repro"}).encode()
                frame = struct.pack(">I", len(raw)) + raw
                channel.sendall(frame[:4])
                channel.settimeout(0.02)
                # The old implementation rejects/closes with request bytes unread.
                with pytest.raises(TimeoutError):
                    channel.recv(1)
                channel.settimeout(1)
                channel.sendall(frame[4:])
                response = transport._decode_object(transport._read_socket_frame(channel))
                assert response["error"] == "BROKER_SATURATED"
        finally:
            server._slots.release()


def source_fact(repo, *, key=KEY, value=42, serial="a", replay=None):
    from atlas.v2.contracts import ArtifactEnvelope, FeatureArtifactV2, FeatureValueV2, ReplayViewV2
    from atlas.v2.research.decision_brief import FactProvenanceV1

    r = request()
    source = FeatureArtifactV2(
        ArtifactEnvelope(1, serial, r.cutoff_ns, r.cutoff_ns, "fixture", ()),
        key,
        "TECHNICAL_V1",
        r.cutoff_ns,
        r.cutoff_ns,
        FrozenMap({"rsi14": FeatureValueV2(value, "INDEX")}),
        "fixture-health",
        replay or ReplayViewV2.ACTUAL_SYSTEM,
    )
    repo.register_artifact(
        ArtifactIndexEntryV2(
            source.content_hash,
            "FeatureArtifactV2",
            source.content_hash,
            source.envelope.created_at_ns,
            source.envelope.available_at_ns,
            {"feature": source.to_dict()},
        )
    )
    p = FactProvenanceV1(
        "FeatureArtifactV2",
        "FEATURE_VALUES_V1",
        r.cutoff_ns,
        "TECHNICAL_V1",
        ("rsi14",),
        KEY,
        "LOCAL_MARKET" if key == KEY else "EXTERNAL_CONTEXT",
    )
    fact = BriefFactV1(
        "technical",
        source.content_hash,
        key,
        r.cutoff_ns,
        r.cutoff_ns,
        r.deadline_ns,
        "AVAILABLE",
        FrozenMap({"rsi14": source.values["rsi14"].to_dict()}),
        (),
        p,
    )
    return source, fact


@pytest.mark.parametrize(
    "fault",
    [
        "value",
        "type",
        "transform",
        "cutoff",
        "available",
        "product",
        "revision",
        "future_source",
        "reconstructed",
        "tampered_source",
        "missing_feature",
    ],
)
def test_typed_source_readback_fails_closed(tmp_path, fault, monkeypatch):
    from atlas.v2.contracts import ReplayViewV2

    r = request()
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        source, f = source_fact(repo, replay=ReplayViewV2.RECONSTRUCTED_MARKET if fault == "reconstructed" else None)
        if fault == "value":
            f = replace(f, values=FrozenMap({"rsi14": {"value": 99, "unit": "INDEX", "missing_reason": None}}))
        elif fault in ("type", "transform", "cutoff", "revision", "missing_feature"):
            changes = {
                "type": {"source_type": "NewsEventV2"},
                "transform": {"transformation": "UNREGISTERED"},
                "cutoff": {"source_cutoff_ns": r.cutoff_ns + 1},
                "revision": {"feature_set_version": "WRONG"},
                "missing_feature": {"feature_ids": ("imagined",)},
            }[fault]
            f = replace(f, provenance=replace(f.provenance, **changes))
        elif fault == "available":
            f = replace(f, available_at_ns=r.cutoff_ns + 1)
        elif fault == "product":
            f = replace(f, key=replace(KEY, contract_revision=sha256_json("different")))
        elif fault in ("future_source", "tampered_source"):
            original = repo.get_artifact
            row = original(f.source_ref)
            if fault == "future_source":
                changed = replace(row, available_at_ns=r.cutoff_ns + 1)
            else:
                wire = source.to_dict()
                wire["values"]["rsi14"]["value"] = 999
                changed = replace(row, metadata={"feature": wire})
            monkeypatch.setattr(repo, "get_artifact", lambda ref: changed if ref == f.source_ref else original(ref))
        result = build_decision_brief(replace(r, facts=(f,)), repository=repo)
        assert result.status == "BLOCKED"
        assert "FACT_SOURCE_BINDING_INVALID" in result.reasons


def test_verified_facts_restart_and_contradiction(tmp_path):
    r = request()
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        _, first = source_fact(repo)
        good = build_decision_brief(replace(r, facts=(first,)), repository=repo)
        assert good.status == "COMPLETE"
        _, conflict = source_fact(repo, value=99, serial="b")
        bad = build_decision_brief(replace(r, facts=(first, conflict)), repository=repo)
        assert "CONTRADICTORY_FACTS" in bad.reasons
    with OpsRepository(path) as repo:
        assert build_decision_brief(replace(r, facts=(first,)), repository=repo) == good


def test_foreign_venue_feature_is_explicit_external_context_and_stale_is_blocked(tmp_path):
    from atlas.v2.instruments import VenueV2

    from .test_session016_candidate_selection import universe

    r = request()
    foreign = replace(KEY, venue=VenueV2.BINANCE)
    u = universe((KEY, foreign))
    cs = replace(
        r.candidate_set, universe_ref=u.content_hash, envelope=replace(r.candidate_set.envelope, content_hash="")
    )
    r = replace(r, universe=u, candidate_set=cs)
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _, fact = source_fact(repo, key=foreign)
        good = build_decision_brief(replace(r, facts=(fact,)), repository=repo)
        assert good.status == "COMPLETE"
        assert good.body["facts"][0]["provenance"]["role"] == "EXTERNAL_CONTEXT"
        wrong = replace(fact, provenance=replace(fact.provenance, role="LOCAL_MARKET"))
        assert build_decision_brief(replace(r, facts=(wrong,)), repository=repo).status == "BLOCKED"
        stale = replace(
            fact, observed_at_ns=r.cutoff_ns - 2, available_at_ns=r.cutoff_ns - 2, valid_until_ns=r.cutoff_ns - 1
        )
        assert "STALE_CONTEXT" in build_decision_brief(replace(r, facts=(stale,)), repository=repo).reasons
