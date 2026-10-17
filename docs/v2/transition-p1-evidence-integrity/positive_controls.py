"""Genuine saved E/B and ineligible-action controls on N2's original database."""
import argparse
import json
import socket
from pathlib import Path


def refuse(*args, **kwargs):
    raise OSError("INTEGRITY_REPRODUCTION_NETWORK_REFUSED")


socket.socket.connect = refuse
socket.socket.connect_ex = refuse
socket.create_connection = refuse
socket.getaddrinfo = refuse

from atlas.v2.contracts import EligibleSetV1
from atlas.v2.memory.repository import OpsRepository
from atlas.v2.research.mean_selection import (
    MeanSelectionRequestV1, MeanSelectionResponseV1, validate_offline_b_selection,
)
from atlas.v2.risk import OfflineRiskSnapshotV1
from atlas.v2.science.mean_evaluation import index_mean_artifact, read_mean_artifact
from p1.conftest import T

parser = argparse.ArgumentParser()
parser.add_argument("--evidence", type=Path, required=True)
args = parser.parse_args()
repo = OpsRepository(args.evidence / "N2" / "mean.sqlite")
eligible = next(EligibleSetV1.from_dict(e.metadata["mean_artifact"])
    for e in repo.artifact_entries("EligibleSetV1")
    if all(x["mean_cash"] != "100" for x in e.metadata["mean_artifact"]["evaluations"]))
snapshot = read_mean_artifact(repo, eligible.population.account_snapshot_ref,
                             OfflineRiskSnapshotV1, consumer_ns=T+12)
negative = next(e for e in eligible.evaluations if e.mean_cash == 5)
positive = next(e for e in eligible.members if e.mean_cash == 9)
assert negative.status == "INELIGIBLE" and negative.score_cash < 0
request = MeanSelectionRequestV1(eligible.content_hash, snapshot.content_hash,
    snapshot.permission_ref, eligible.population.instrument_revision_ref,
    T+10, eligible.population.deadline_ns, "positive-control", ())
index_mean_artifact(repo, request, available_at_ns=request.issued_at_ns)


def response(action):
    return MeanSelectionResponseV1(request.content_hash, eligible.content_hash,
        snapshot.content_hash, snapshot.permission_ref, request.source_prefix_ref,
        "positive-control-response", request.nonce, "SELECT_ONE", action, T+11, (),
        "Genuine saved evaluation positive/negative control")


def validate(action):
    return validate_offline_b_selection(repo, request=request, response=response(action),
        eligible=eligible, snapshot=snapshot, now_ns=T+12,
        current_evaluation_refs=tuple(e.content_hash for e in eligible.evaluations))


refusal = ""
try:
    validate(negative.action_ref)
except ValueError as exc:
    refusal = str(exc)
assert refusal == "OUTSIDE_ELIGIBLE_SET"
result = validate(positive.action_ref)
assert result.decision == "SELECT_ONE"
repo.close()
output = {"negative_original_action": {"mean_cash": str(negative.mean_cash),
    "score_cash": str(negative.score_cash), "b_refusal": refusal},
    "genuine_positive_action": {"mean_cash": str(positive.mean_cash),
    "score_cash": str(positive.score_cash), "b_decision": result.decision,
    "b_authority": result.authority}}
(args.evidence / "positive_controls.json").write_text(json.dumps(output, indent=2)+"\n")
print(json.dumps(output, sort_keys=True))
