"""Cold strict export at owner-like dimensions using existing production fixtures.

No owner database, real network, evidence-policy amendment or capital authority.
This is a complementary native engineering gate, not a live endurance claim.
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import subprocess
import time
from pathlib import Path

from atlas.v2.memory.repository import OpsRepository
from atlas.v2.runtime.ops_supervisor import OpsSupervisorV2
from atlas.v2.runtime.production import create_bybit_public_ws_port
from atlas.v2.science.tuning_export import _TYPES, TuningRunIdentityV1, export_tuning_snapshot
from tests.v2.test_s38_sustained_public_stream import NOW_NS, MixedWorkload, QueueStream
from tests.v2.test_session039_long_run_public_evidence import RestartPublicSource


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    args = parser.parse_args()
    args.root.mkdir(parents=True, exist_ok=False)
    sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    clock = [NOW_NS]
    source = QueueStream(lambda: clock[0])
    port = create_bybit_public_ws_port(public_source=RestartPublicSource(),
                                     public_stream_source=source, clock_ns=lambda: clock[0])
    workload = MixedWorkload()
    database = args.root / 'ops.sqlite'
    result = {'schema_version': 1, 'status': 'TEST GATE', 'source_sha': sha,
              'scope': 'SYNTHETIC_OWNER_DIMENSIONS_WITH_EXACT_PRODUCTION_RAW_BOOK_VALIDATION',
              'owner_database_inspected': False, 'capital_enabled': False,
              'assisted_enabled': False, 'authority': 'ZERO', 'provider_calls': 0,
              'snapshot_budget_seconds': 10, 'required_snapshot_margin_seconds': 5,
              'required_whole_export_seconds': 8, 'exports': []}
    started = time.monotonic()
    original_snapshot = OpsRepository.read_snapshot
    durations = []

    @contextlib.contextmanager
    def timed_snapshot(self):
        at = time.monotonic()
        try:
            with original_snapshot(self) as connection:
                yield connection
        finally:
            durations.append(time.monotonic() - at)

    try:
        with OpsSupervisorV2(database, port, clock_ns=lambda: clock[0]) as supervisor:
            supervisor.run_once()
            repository = supervisor.repository
            assert repository is not None
            relevant = frames = 0
            placeholders = ','.join('?' for _ in _TYPES)
            while relevant < 4760 and frames < 196608:
                for ordinal in range(frames, frames + 128):
                    assert source.handoff.offer(workload.frame(NOW_NS + (ordinal + 1) * 25000000))
                frames += 128
                clock[0] = NOW_NS + frames * 25000000
                for _ in range(4):
                    port.service_public_stream(repository)
                    if source.handoff.snapshot().queue_items == 0:
                        break
                assert source.handoff.snapshot().queue_items == 0
                relevant = repository._connection.execute(
                    'SELECT count(*) FROM artifact_index WHERE artifact_type IN (' + placeholders + ')',
                    _TYPES).fetchone()[0]
            assert 4760 <= relevant < 8192
            clock[0] += 250000000
            port._collect_public_stream_evidence(repository, now_ns=clock[0])
            result.update(frames=frames, relevant_rows=relevant,
                          sqlite_rows=repository._connection.execute(
                              'SELECT count(*) FROM artifact_index').fetchone()[0],
                          raw_archive_bytes=sum(p.stat().st_size for p in args.root.rglob('*.arrow')),
                          fixture_population_seconds=time.monotonic() - started)
            identity = TuningRunIdentityV1('s40-owner-scale-synthetic', 'a' * 64, sha, NOW_NS)
            OpsRepository.read_snapshot = timed_snapshot
            before_changes = repository._connection.total_changes
            for name in ('cold-full', 'fresh-full', 'repeat-incremental'):
                output = args.root / ('cold' if name != 'fresh-full' else 'fresh')
                durations.clear()
                at = time.monotonic()
                exported = export_tuning_snapshot(database, output, identity, cutoff_ns=clock[0])
                elapsed = time.monotonic() - at
                measurement = {'name': name, 'seconds': elapsed,
                               'snapshot_seconds': sum(durations), 'rows': exported['rows_written'],
                               'has_more': exported['has_more'],
                               'validation_failures': exported['validation_failures'],
                               'budget_yielded': exported['budget_yielded']}
                result['exports'].append(measurement)
                assert not exported['validation_failures'] and not exported['has_more']
                assert not exported['blocked_future_evidence']
                assert exported['snapshot_budget_seconds'] == 10
                assert sum(durations) < 5, 'Owner-scale strict snapshot lacks 2x budget margin'
                assert elapsed < 8, 'Owner-scale whole export lacks fixed-budget margin'
                if name == 'repeat-incremental':
                    assert exported['rows_written'] == 0
                else:
                    assert exported['rows_written'] >= 4760
            assert repository._connection.total_changes == before_changes
            assert repository._connection.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
            assert not source.handoff.snapshot().overflowed and source.handoff.snapshot().frames_rejected == 0
        result.update(status='TESTED', elapsed_seconds=time.monotonic() - started,
                      benchmark_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    except Exception as error:
        result.update(error_type=type(error).__name__,
                      reason=str(error) if isinstance(error, AssertionError) else 'SANITIZED_FIXTURE_FAILURE')
        raise
    finally:
        OpsRepository.read_snapshot = original_snapshot
        (args.root / 'owner-scale-export-result.json').write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps({'status': result['status'], 'result': str(args.root / 'owner-scale-export-result.json')}))


if __name__ == '__main__':
    main()
