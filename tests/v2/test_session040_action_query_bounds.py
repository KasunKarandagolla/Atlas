"""S40 derived critic query geometry and legacy migration, without provider I/O."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from atlas.v2.agent_intelligence.budget import DeepSeekPriceScheduleV1
from atlas.v2.agent_intelligence.persistence import ActionAssessmentRepository
from atlas.v2.memory.repository import OpsRepository
from tests.v2.test_session029_action_critic import PRICE_FILE


def _ledger(path: Path) -> ActionAssessmentRepository:
    with OpsRepository(path):
        pass
    return ActionAssessmentRepository(path, price_schedule=DeepSeekPriceScheduleV1.load(PRICE_FILE))


def _query_geometry(connection: sqlite3.Connection, count: int = 8192, pending: int = 2) -> None:
    """Synthetic SQL population only; these bodies are never scientific evidence."""
    connection.execute("BEGIN IMMEDIATE")
    connection.execute("INSERT INTO agent_action_assessment_packets VALUES('p','ph','{}','r',0)")
    for index in range(count + pending):
        identity = str(index)
        connection.execute("INSERT INTO agent_action_assessment_requests VALUES(?,?,?,?,?)",
                           (identity, 'rh' + identity, 'p', '{}', index))
        connection.execute("INSERT INTO agent_action_assessment_attempts VALUES(?,?,?,?,?)",
                           ('a' + identity, identity, 1, 'CREATED', index))
        connection.execute("INSERT INTO agent_action_assessment_reservations VALUES(?,?,?,?,?,?,?)",
                           ('b' + identity, identity, 'a' + identity, index // 2, '.0084', 'price', index))
        connection.execute("INSERT INTO agent_action_assessment_dispatches VALUES(?,?,?,?,?,?)",
                           ('d' + identity, 'a' + identity, identity, '{}', 'dh' + identity, index))
        if index < count:
            connection.execute("INSERT INTO agent_action_assessment_outcomes VALUES(?,?,?,?,?,?,?,?,?)",
                               ('o' + identity, identity, 'a' + identity, 'UNAVAILABLE', None, None,
                                'OFFLINE_SQL_GEOMETRY', index, 0))
            connection.execute("INSERT INTO agent_action_assessment_observation_projections VALUES(?,?,?)",
                               (identity, 'projection' + identity, index))
    connection.commit()


def test_pending_queries_do_not_scan_archived_population_and_triggers_preserve_fifo(tmp_path):
    with _ledger(tmp_path / 'ops.sqlite') as ledger:
        connection = ledger._connection
        _query_geometry(connection)
        # VM work is measured, so a planner choosing an archived-table scan
        # cannot pass merely because the fixture's wall-clock happened to be fast.
        callbacks = [0]

        def bound():
            callbacks[0] += 1
            return int(callbacks[0] > 50)

        connection.set_progress_handler(bound, 100)
        try:
            assert ledger.authorized_requests_without_terminal_result() == ('8192', '8193')
            assert ledger.unprojected_terminal_records() == ()
            plan = connection.execute("EXPLAIN QUERY PLAN SELECT reserved_usd FROM "
                "agent_action_assessment_reservations WHERE utc_day=?", (4096,)).fetchall()
            assert any('reservations_day_v1' in str(row['detail']) for row in plan)
        finally:
            connection.set_progress_handler(None, 0)
        assert callbacks[0] < 50
        for identity, received_at in [('8193', 10), ('8192', 20)]:
            ledger.record_outcome(identity, attempt_id='a' + identity, status='UNAVAILABLE',
                result=None, provider_output_hash=None, failure_code='OFFLINE_SQL_GEOMETRY',
                received_at_ns=received_at, eligible=False)
        assert ledger.authorized_requests_without_terminal_result() == ()
        assert [row['request_id'] for row in ledger.unprojected_terminal_records()] == ['8193', '8192']
        ledger.mark_observation_projected('8193', 'a' * 64, now_ns=30)
        assert [row['request_id'] for row in ledger.unprojected_terminal_records()] == ['8192']
        assert connection.execute('PRAGMA foreign_key_check').fetchall() == []


def test_legacy_bootstrap_retains_pending_dispatch_and_projection_on_reopen(tmp_path):
    path = tmp_path / 'ops.sqlite'
    with _ledger(path) as ledger:
        _query_geometry(ledger._connection, count=4)
        ledger.record_outcome('4', attempt_id='a4', status='UNAVAILABLE', result=None,
            provider_output_hash=None, failure_code='OFFLINE_SQL_GEOMETRY', received_at_ns=50, eligible=False)
        for name in ('dispatch', 'outcome', 'projection'):
            ledger._connection.execute(f'DROP TRIGGER agent_action_assessment_{name}_index_insert_v1')
        ledger._connection.execute('DROP TABLE agent_action_assessment_pending_dispatch_index_v1')
        ledger._connection.execute('DROP TABLE agent_action_assessment_pending_projection_index_v1')
        ledger._connection.execute('DROP INDEX agent_action_assessment_reservations_day_v1')
    with _ledger(path) as ledger:
        assert ledger.authorized_requests_without_terminal_result() == ('5',)
        assert [row['request_id'] for row in ledger.unprojected_terminal_records()] == ['4']
        assert ledger._connection.execute('SELECT count(*) FROM agent_action_assessment_outcomes').fetchone()[0] == 5
    with _ledger(path) as ledger:
        assert ledger.authorized_requests_without_terminal_result() == ('5',)


def test_partial_active_index_schema_is_rejected(tmp_path):
    path = tmp_path / 'ops.sqlite'
    with _ledger(path) as ledger:
        ledger._connection.execute('DROP TRIGGER agent_action_assessment_projection_index_insert_v1')
    with pytest.raises(RuntimeError, match='active query indexes are incomplete'):
        _ledger(path)


def test_recovery_population_overflow_is_explicit_and_retains_requests(tmp_path):
    with _ledger(tmp_path / 'ops.sqlite') as ledger:
        _query_geometry(ledger._connection, count=0, pending=65)
        with pytest.raises(RuntimeError, match='ACTIVE_RECOVERY_POPULATION_EXCEEDED'):
            ledger.authorized_requests_without_terminal_result()
        assert ledger._connection.execute(
            'SELECT count(*) FROM agent_action_assessment_pending_dispatch_index_v1').fetchone()[0] == 65
        assert ledger._connection.execute(
            'SELECT count(*) FROM agent_action_assessment_requests').fetchone()[0] == 65
