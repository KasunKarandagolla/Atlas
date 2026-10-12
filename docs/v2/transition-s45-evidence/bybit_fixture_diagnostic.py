from pathlib import Path
import traceback
from tests.v2.test_session031_public_shadow_readiness import CUTOFF, _FakeBybitReader, _FakeClock
from atlas.v2.data.bybit_source import BybitPublicCycleSourceV1
from atlas.v2.runtime.production import ProductionOpsCyclePortV1
from atlas.v2.runtime.ops_supervisor import OpsSupervisorV2

root = Path('/tmp/atlas-s45-bybit-diagnostic-inspect')
root.mkdir(parents=True, exist_ok=True)
reader = _FakeBybitReader(CUTOFF)
port = ProductionOpsCyclePortV1(public_source=BybitPublicCycleSourceV1(reader, clock_ns=_FakeClock(CUTOFF)))
with OpsSupervisorV2(root / 'ops.sqlite', port, clock_ns=_FakeClock(CUTOFF)) as supervisor:
    result = supervisor.run_once()
    repository = supervisor.repository
    assert repository is not None
    print({
        'cycle_health': result.cycle.source_health_state,
        'cycle_failures': result.cycle.failure_types,
        'cycle_event_count': len(result.cycle.event_ids),
        'event_receipt_count': len(result.cycle.event_receipt_refs),
        'products': len(repository.artifact_entries('ProductContractV2')),
        'observations': len(repository.artifact_entries('PublicObservationIndexV2')),
        'reconciliations': [e.metadata.get('status') for e in repository.artifact_entries('OpsPublicSourceReconciliationV1')],
        'supervisor_receipts': len(repository.artifact_entries('OpsSupervisorReceiptV1')),
        'decision_calendar': len(repository.artifact_entries('DecisionCalendarEntryV2')),
        'source_health': [(x.status, x.observed_at_ns, x.available_at_ns) for x in repository.source_health_history('BYBIT_PUBLIC_HTTP')],
        'reader_calls': len(reader.calls),
        'tradeoff': 'offline fake reader; no live connection',
    })
    try:
        port.collect(repository, now_ns=CUTOFF, recovery=supervisor.recovery)
        print({'direct_collect': 'PASS'})
    except Exception:
        traceback.print_exc()
