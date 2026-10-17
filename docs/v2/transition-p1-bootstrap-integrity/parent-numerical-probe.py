"""Append this probe to the copied current bootstrap test module in the exact parent worktree.

Historical red evidence only; do not add this permissive-parent oracle to the current P1 suite.
"""

def test_parent_numeric_capture(case):
    import json
    inp, protocol, residuals, groups, effective, lags = uncertain_chain(case)
    forged = forge_allowance(case, inp, D("0"))
    numerical = replace(inp.numerical, cash=D("2"))
    index_mean_artifact(case.repo, numerical, available_at_ns=numerical.available_at_ns)
    case.inputs[inp.candidate.content_hash] = replace(inp, calibration=forged, numerical=numerical)
    eligible = run(case)
    result = next(e for e in eligible.evaluations if e.candidate_ref == inp.candidate.content_hash)
    assert result.status == "ELIGIBLE" and result.score_cash == 3
    record = {"parent_sha": "215a0045d8203f23dbd4935639ef448556cc2353", "empirical_error_cash": "0", "actual_bootstrap_bound_cash": str(inp.calibration.allowance_cash), "forged_allowance_cash": "0", "connected_status": result.status, "connected_score_cash": str(result.score_cash), "effective_blocks": effective, "lags": lags}
    __import__("pathlib").Path("/workspace/work/p1-bootstrap/parent-numerical.json").write_text(json.dumps(record, indent=2)+"\n")
