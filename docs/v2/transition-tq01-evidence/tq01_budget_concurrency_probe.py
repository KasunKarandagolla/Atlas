"""Scratch-only concurrent idempotent reservation probe; no provider dispatch."""
from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
import sqlite3
import uuid
import pytest
from tests.v2.test_session029_action_critic import CUTOFF, frozen_case
from atlas.v2.agent_intelligence.persistence import ActionAssessmentRepository


def test_same_immutable_cost_reservation_from_two_local_connections(frozen_case):
    path, _receipt, _receipt_ref, sealed, _profile, schedule = frozen_case
    request_id = sealed.request.request_id
    attempt_id = str(uuid.uuid5(uuid.UUID(request_id), "tq01-budget-attempt"))
    reservation_id = str(uuid.uuid5(uuid.UUID(request_id), "tq01-budget-reservation"))
    reserve_usd = schedule.worst_case_call_usd(input_tokens=12_000, output_tokens=2_048)
    with ActionAssessmentRepository(path, price_schedule=schedule) as owner:
        owner.persist_packet_request(sealed.packet, sealed.request, now_ns=CUTOFF + 100)
        owner.create_attempt(request_id, attempt_id=attempt_id, now_ns=CUTOFF + 100)

    barrier = Barrier(3)
    def reserve_once():
        with ActionAssessmentRepository(path, price_schedule=schedule) as ledger:
            barrier.wait(timeout=5)
            ledger.reserve_cost(request_id, attempt_id=attempt_id,
                reservation_id=reservation_id, now_ns=CUTOFF + 100, reserved_usd=reserve_usd)
        return "reserved"

    with ThreadPoolExecutor(max_workers=2) as pool:
        tasks = [pool.submit(reserve_once) for _ in range(2)]
        barrier.wait(timeout=5)
        results = [task.result(timeout=15) for task in tasks]
    with sqlite3.connect(path) as connection:
        rows = connection.execute("SELECT reservation_id,request_id,attempt_id,reserved_usd "
            "FROM agent_action_assessment_reservations WHERE request_id=?", (request_id,)).fetchall()
    assert results == ["reserved", "reserved"]
    assert len(rows) == 1
    assert rows[0] == (reservation_id, request_id, attempt_id, str(reserve_usd))
