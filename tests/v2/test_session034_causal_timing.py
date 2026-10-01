from __future__ import annotations

import pytest

from atlas.v2.data.s3_forward_evidence import S3NativeComputationContextV1


def test_s34_causal_timing_orders_close_cutoff_computation_and_persistence():
    t0_close = 1_750_000_000_000_000_000
    t1_evidence_cutoff = t0_close + 250_000_000
    t2_computation_started = t1_evidence_cutoff + 1
    t3_computation_finished = t2_computation_started + 5
    t4_persisted = t3_computation_finished + 1
    fixed_deadline = t4_persisted + 10

    context = S3NativeComputationContextV1(
        evidence_cutoff_ns=t1_evidence_cutoff,
        computation_started_ns=t2_computation_started,
        computation_finished_ns=t3_computation_finished,
        consumer_deadline_ns=fixed_deadline,
    )

    assert (
        t0_close <= context.evidence_cutoff_ns
        <= context.computation_started_ns
        <= context.computation_finished_ns
        <= t4_persisted
        <= context.consumer_deadline_ns
    )
    assert context.produced_at_ns == t3_computation_finished
    assert context.to_dict()["evidence_cutoff_ns"] == t1_evidence_cutoff


def test_s34_computation_that_finishes_after_the_fixed_deadline_is_rejected():
    cutoff = 1_750_000_000_000_000_000
    deadline = cutoff + 10

    with pytest.raises(ValueError, match="fixed causal deadline"):
        S3NativeComputationContextV1(
            evidence_cutoff_ns=cutoff,
            computation_started_ns=cutoff + 1,
            computation_finished_ns=deadline + 1,
            consumer_deadline_ns=deadline,
        )
