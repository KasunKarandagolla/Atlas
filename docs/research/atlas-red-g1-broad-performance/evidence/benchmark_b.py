from __future__ import annotations
import argparse, hashlib, importlib.metadata, json, os, platform, resource, statistics, sys, time, zlib
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from typing import Any

for key in list(os.environ):
    if any(word in key.upper() for word in ("KEY", "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL")):
        os.environ.pop(key)
attempts = {"network_or_child_attempts": 0}
def audit(event, _args):
    if event in {"socket.connect", "socket.getaddrinfo", "socket.gethostbyname", "socket.sendto", "subprocess.Popen", "os.system", "os.exec", "os.posix_spawn"}:
        attempts["network_or_child_attempts"] += 1
        raise PermissionError("G1B_OFFLINE_ONLY")
sys.addaudithook(audit)
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT),str(ROOT/'src'),str(ROOT/'tests')]
from v2.test_session041_scale_review import _products,_records,_snapshot,NOW,STEP
from atlas.v2._serialization import canonical_json,sha256_json
from atlas.v2.data.binance import SOURCE_ID as BINANCE_ID
from atlas.v2.data.bybit import SOURCE_ID as BYBIT_ID
from atlas.v2.data.collector import PublicCollectorV2
from atlas.v2.data.history import ParquetObservationArchiveV2
from atlas.v2.instruments import InstrumentRegistryV2,VenueV2
from atlas.v2.memory.repository import ArtifactIndexEntryV2,OpsRepository
from atlas.v2.runtime.production import ProductionOpsCyclePortV1
from atlas.v2.runtime.broad_universe import full_universe,latest_workset,publish_broad_workset

parser=argparse.ArgumentParser()
parser.add_argument('--per-venue',type=int,required=True)
parser.add_argument('--cycles',type=int,required=True)
parser.add_argument('--venue',choices=('both','binance'),required=True)
parser.add_argument('--sampler',choices=('none','existing'),default='none')
parser.add_argument('--output',type=Path,required=True)
parser.add_argument('--source-checkpoint',default='e75a83226cea2cf8dfee10823541848e8ffb0de4')
parser.add_argument('--dirty-source-diff-sha256')
args=parser.parse_args()
run_started_ns=time.perf_counter_ns()
profile_id=('G1A_MAX_TWO_VENUE_4096' if args.venue=='both' and args.per_venue==2048 and args.cycles==3
    else f"G1B_{args.venue.upper()}_{args.per_venue*(2 if args.venue=='both' else 1)}")
root=args.output.with_suffix('')
if root.exists():raise SystemExit(f'refusing to reuse root {root}')
root.mkdir(parents=True)
products=list(_products(args.per_venue))
if args.venue=='binance':products=[p for p in products if p.key.venue==VenueV2.BINANCE]
products=tuple(products)
venues=(VenueV2.BINANCE,VenueV2.BYBIT) if args.venue=='both' else (VenueV2.BINANCE,)
sources=tuple(x for venue,x in ((VenueV2.BINANCE,BINANCE_ID),(VenueV2.BYBIT,BYBIT_ID)) if venue in venues)
registry=InstrumentRegistryV2()
for product in products:registry.register(product)
repo_path=root/'ops.sqlite'
all_metrics=[];expected_raw={};expected_index={};workset_hashes=[]
with OpsRepository(repo_path) as repo:
    repo.register_artifacts(tuple(ArtifactIndexEntryV2(p.content_hash,'ProductContractV2',p.content_hash,NOW,NOW,
        {'product':p.to_dict()}) for p in products))
    collector=PublicCollectorV2(repository=repo,registry=registry,clock_ns=lambda:now[0],
        archive=ParquetObservationArchiveV2(root/'ops-observations',compact_stream_repository=repo,
        clock_ns=lambda:now[0]))
    collector.publish_indexes_after_archive=True
    now=[NOW]
    activity={'last_at':0.0,'max_gap':0.0,'calls':0}
    def stream_service():
        assert not repo._connection.in_transaction
        observed=time.perf_counter()
        activity['max_gap']=max(activity['max_gap'],observed-activity['last_at'])
        activity['last_at']=observed
        activity['calls']+=1
    def service_start():activity.update(last_at=time.perf_counter(),max_gap=0.0,calls=0)
    def service_end():return {'calls':activity['calls'],'max_gap_seconds':max(activity['max_gap'],time.perf_counter()-activity['last_at'])}
    port=SimpleNamespace(_collector_recovery=SimpleNamespace(collector=collector),
        public_source=SimpleNamespace(enabled_venues=venues,required_source_ids=sources,current_products=products),
        clock_ns=lambda:now[0],service_public_stream=lambda _repo:stream_service())
    def db_bytes():return repo._connection.execute('PRAGMA page_count').fetchone()[0]*repo._connection.execute('PRAGMA page_size').fetchone()[0]
    def write_bytes():
        for line in Path('/proc/self/io').read_text().splitlines():
            if line.startswith('write_bytes:'):return int(line.partition(':')[2])
        return 0
    baseline_db=db_bytes();prev_db=baseline_db;baseline_write=write_bytes();prev_write=baseline_write;prev_archive=0
    sampler=None
    if args.sampler=='existing':
        from scripts.qualify_public_observation import ResourceSampler
        sampler=ResourceSampler(root)
        sampler.__enter__()
    try:
      for cycle in range(args.cycles):
        now[0]=NOW+cycle*STEP
        prep_start=time.perf_counter_ns()
        records=_records(products,now[0]);acquired=_snapshot(records,now[0])
        prep_ns=time.perf_counter_ns()-prep_start
        for rec in records:
            obs=rec.observation
            if obs.record_id in expected_raw:raise AssertionError('fixture repeats a raw record identity')
            expected_raw[obs.record_id]=(canonical_json(obs.to_dict()),rec.raw_payload)
        service_start();service_start_ns=time.perf_counter_ns()
        adoption_start=time.perf_counter_ns()
        assert ProductionOpsCyclePortV1._persist_broad_public_snapshot(port,repo,acquired,now_ns=now[0])
        adoption_ns=time.perf_counter_ns()-adoption_start
        lookup_start=time.perf_counter_ns()
        page=repo.latest_artifact_entries('BroadPublicAcquisitionReceiptV2',as_of_ns=now[0],limit=1)
        assert page.entries and not page.invalid_entry_count
        receipt=page.entries[0]
        lookup_ns=time.perf_counter_ns()-lookup_start
        service_count_before=activity['calls']
        publication_start=time.perf_counter_ns()
        body=None;publication_error=None
        try:
            body=publish_broad_workset(repo,products=products,snapshot=acquired,available_at_ns=now[0],
                acquisition_ref=receipt.artifact_ref,source_state=dict.fromkeys(sources,'HEALTHY_CURRENT'),
                clock_ns=lambda:now[0],service=stream_service)
        except ValueError as exc:
            if str(exc)!='PER_VENUE_UNIVERSE_POPULATION_OVERFLOW':raise
            publication_error=str(exc)
        publication_ns=time.perf_counter_ns()-publication_start
        service_ns=time.perf_counter_ns()-service_start_ns
        audit_start=time.perf_counter_ns()
        refs=[ref for group in receipt.metadata['receipt']['source_observation_refs'].values() for ref in group]
        expected_refs={sha256_json({'artifact_type':'PublicObservationIndexV2','record_id':r.observation.record_id}) for r in records}
        if len(refs)!=len(records) or set(refs)!=expected_refs:raise AssertionError('receipt source-reference population changed')
        indexed=repo.get_artifact_metadata_by_refs(tuple(refs))
        if set(indexed)!=expected_refs:raise AssertionError('exact durable source index is incomplete')
        for r in records:
            ref=sha256_json({'artifact_type':'PublicObservationIndexV2','record_id':r.observation.record_id})
            entry=indexed[ref];metadata=entry['metadata'];obs=r.observation
            if (entry['artifact_type']!='PublicObservationIndexV2' or entry['content_hash']!=obs.content_hash
                or entry['available_at_ns']!=now[0] or metadata.get('record_id')!=obs.record_id
                or metadata.get('source_id')!=obs.source_id or metadata.get('raw_payload_hash')!=obs.raw_payload_hash):
                raise AssertionError('durable index identity/chronology mismatch')
            expected_index[ref]=canonical_json(metadata)
        audit_ns=time.perf_counter_ns()-audit_start
        workset_hash=sha256_json(body) if body is not None else None
        if body is not None:
            workset_hashes.append(workset_hash)
            if body['selected_count']>24:raise AssertionError('workset exceeds frozen bound')
            universe=full_universe(repo,cutoff_ns=now[0])
            if universe is None or len(universe.entries)!=len(products):raise AssertionError('universe population changed')
            if any(row.scanner_eligible or row.capital_eligible for row in universe.entries):raise AssertionError('synthetic fixture acquired eligibility')
        elif full_universe(repo,cutoff_ns=now[0]) is not None:
            raise AssertionError('rejected population unexpectedly published a universe')
        sample_db=db_bytes();sample_write=write_bytes()
        archive_files=tuple((root/'ops-observations').glob('*.parquet'))
        archive_total=sum(p.stat().st_size for p in archive_files)
        growth=sample_db-prev_db;archive_growth=archive_total-prev_archive
        all_metrics.append({'cycle':cycle+1,'profile_id':profile_id,'product_count':len(products),
            'source_record_count':len(records),'raw_payload_bytes':sum(len(r.raw_payload) for r in records),
            'input_construction_ns':prep_ns,'adoption_ns':adoption_ns,'receipt_lookup_ns':lookup_ns,
            'workset_publication_ns':publication_ns,'production_service_ns':service_ns,
            'offline_exact_index_audit_ns':audit_ns,'source_record_count_exact':len(expected_refs),
            'workset_publication_error':publication_error,
            'durable_db_logical_bytes':sample_db,'db_logical_growth_bytes':growth,
            'archive_cumulative_bytes':archive_total,'archive_growth_bytes':archive_growth,
            'db_plus_archive_growth_bytes':growth+archive_growth,
            'process_write_bytes_cycle':sample_write-prev_write,
            'wal_bytes':(root/'ops.sqlite-wal').stat().st_size if (root/'ops.sqlite-wal').exists() else 0,
            'archive_file_count':len(archive_files),'workset_hash':workset_hash,
            'service_callbacks':activity['calls']-service_count_before,
            'service_callback_max_gap_seconds':activity['max_gap']})
        prev_db=sample_db;prev_write=sample_write;prev_archive=archive_total
      final_db=db_bytes();final_writes=write_bytes()
      rows=repo._connection.execute("SELECT artifact_type,count(*),sum(length(CAST(metadata_json AS BLOB))) FROM artifact_index GROUP BY artifact_type").fetchall()
      expected_artifacts={}
      for row in repo._connection.execute('SELECT * FROM artifact_index'):
        entry=repo.artifact_entry_from_storage_row(row)
        expected_artifacts[entry.artifact_ref]=(entry.artifact_type,entry.content_hash,entry.created_at_ns,
            entry.available_at_ns,canonical_json(entry.metadata))
      repo._connection.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    finally:
      if sampler is not None:
        sampler.__exit__(None,None,None)
        resources=sampler.result()
      else:resources=None
with OpsRepository(repo_path,read_only=True) as restarted:
    for cycle,want in enumerate(workset_hashes):
        at=NOW+cycle*STEP
        got=latest_workset(restarted,cutoff_ns=at)
        if sha256_json(got)!=want or len(full_universe(restarted,cutoff_ns=at).entries)!=len(products):
            raise AssertionError('workset/universe restart mismatch')
    restored_index=restarted.get_artifact_metadata_by_refs(tuple(expected_index))
    if set(restored_index)!=set(expected_index) or {ref:canonical_json(e['metadata']) for ref,e in restored_index.items()}!=expected_index:
        raise AssertionError('exact index restart mismatch')
    restored_domains=restarted.get_artifact_metadata_by_refs(tuple(expected_artifacts))
    if set(restored_domains)!=set(expected_artifacts):raise AssertionError('domain rows missing after restart')
    created_at={}
    all_refs=tuple(sorted(expected_artifacts))
    for off in range(0,len(all_refs),500):
        batch=all_refs[off:off+500];marks=','.join('?' for _ in batch)
        found=restarted._connection.execute(f'SELECT artifact_ref,created_at_ns FROM artifact_index WHERE artifact_ref IN ({marks})',batch).fetchall()
        created_at.update((r['artifact_ref'],r['created_at_ns']) for r in found)
    for ref,expected in expected_artifacts.items():
        entry=restored_domains[ref]
        actual=(entry['artifact_type'],entry['content_hash'],created_at[ref],entry['available_at_ns'],canonical_json(entry['metadata']))
        if actual!=expected:raise AssertionError('immutable indexed domain row changed after restart')
import pyarrow.parquet as pq
archived={};archive_hashes={}
for path in (root/'ops-observations').glob('*.parquet'):
    archive_hashes[path.name]=hashlib.sha256(path.read_bytes()).hexdigest()
    for row in pq.read_table(path,columns=['record_id','observation_json','raw_payload_bytes','raw_payload_hash']).to_pylist():
        rid=row['record_id']
        if rid in archived:raise AssertionError('duplicate raw record in immutable archive')
        archived[rid]=(canonical_json(json.loads(row['observation_json'])),row['raw_payload_bytes'])
        if hashlib.sha256(row['raw_payload_bytes']).hexdigest()!=row['raw_payload_hash']:
            raise AssertionError('raw payload hash mismatch in archive')
if archived!=expected_raw:raise AssertionError('raw bytes/observations differ from deterministic inputs')
# Hash all persisted logical index rows, then compact blocks/locators/identities.
import sqlite3
con=sqlite3.connect(f'file:{repo_path}?mode=ro',uri=True)
def json_rows(table,order):
    cur=con.execute(f'SELECT * FROM {table} ORDER BY {order}')
    names=[d[0] for d in cur.description]
    rows=[]
    for row in cur.fetchall():rows.append([v.hex() if isinstance(v,bytes) else v for v in row])
    return {'columns':names,'rows':rows}
logical_tables={
 'artifact_index':json_rows('artifact_index','artifact_ref'),
 'public_instrument_identity_v1':json_rows('public_instrument_identity_v1','instrument_key_ref'),
 'public_observation_metadata_block_v1':json_rows('public_observation_metadata_block_v1','block_ref'),
 'public_observation_metadata_locator_v1':json_rows('public_observation_metadata_locator_v1','artifact_ref'),
}
logical_state_sha256=hashlib.sha256(canonical_json(logical_tables).encode()).hexdigest()
con.close()
def git_blob_sha256(path: str) -> str:
    payload=(ROOT/path).read_bytes()
    return hashlib.sha1(f'blob {len(payload)}\0'.encode()+payload).hexdigest()

body={'mode':'B_LOW_OVERHEAD_PRODUCTION_PATH','profile_id':profile_id,'venue_profile':args.venue,
 'per_venue':args.per_venue,'product_count':len(products),'cycles':args.cycles,'cadence_seconds':10,
 'source_shape':'existing three Binance bulk rows per product; existing one Bybit bulk row per product',
 'source_checkpoint':args.source_checkpoint,
 'fixture_sha256':hashlib.sha256((ROOT/'tests/v2/test_session041_scale_review.py').read_bytes()).hexdigest(),
 'source_git_blobs':{path:git_blob_sha256(path) for path in (
   'tests/v2/test_session041_scale_review.py','src/atlas/v2/runtime/production.py',
   'src/atlas/v2/data/collector.py','src/atlas/v2/memory/repository.py',
   'src/atlas/v2/data/history.py')},
 'dirty_source_diff_sha256':args.dirty_source_diff_sha256,
 'command':f'benchmark_b.py --per-venue {args.per_venue} --cycles {args.cycles} --venue {args.venue} --sampler {args.sampler}',
 'environment':{'python':sys.version,'platform':platform.platform(),'machine':platform.machine(),'cpu_count':os.cpu_count(),
   'dependencies':{n:importlib.metadata.version(n) for n in ('pyarrow','duckdb','nautilus-trader')}},
 'samples':all_metrics,'whole_run_seconds':None,
 'profile_outcome':('REJECTED_BY_FROZEN_PER_VENUE_CAP' if any(x['workset_publication_error'] for x in all_metrics) else 'PUBLISHED'),
 'integrity':{'exact_raw_rows_verified':len(archived),'exact_index_metadata_verified_after_restart':len(expected_index),
   'all_artifact_domain_metadata_verified_after_restart':len(expected_artifacts),'cutoff_worksets_verified_after_restart':len(workset_hashes),
   'zero_lost_records':True,'zero_rejected_record_indexes':True,'raw_payload_hashes':'EXACT','archive_file_sha256':archive_hashes,
   'logical_store_sha256':logical_state_sha256,'artifact_rows_by_type':{r[0]:r[1] for r in rows},
   'source_ref_ids_sha256':hashlib.sha256(canonical_json(sorted(expected_index)).encode()).hexdigest(),
   'raw_record_ids_sha256':hashlib.sha256(canonical_json(sorted(expected_raw)).encode()).hexdigest()},
 'physical_io_total_bytes':final_writes-baseline_write,'final_database_logical_bytes':final_db,
 'resources':resources,'network_or_child_attempts':attempts['network_or_child_attempts']}
# This includes setup and all post-service audit; cadence uses only production_service_ns.
body['whole_run_seconds']=(time.perf_counter_ns()-run_started_ns)/1e9
args.output.parent.mkdir(parents=True,exist_ok=True)
args.output.write_text(json.dumps(body,sort_keys=True,indent=2)+'\n')
print(json.dumps({'output':str(args.output),'profile_id':profile_id,'product_count':len(products),
 'service_seconds':[round(x['production_service_ns']/1e9,6) for x in all_metrics],
 'adoption_seconds':[round(x['adoption_ns']/1e9,6) for x in all_metrics],
 'receipt_lookup_seconds':[round(x['receipt_lookup_ns']/1e9,6) for x in all_metrics],
 'workset_seconds':[round(x['workset_publication_ns']/1e9,6) for x in all_metrics],
 'input_seconds':[round(x['input_construction_ns']/1e9,6) for x in all_metrics],
 'growth_bytes':[x['db_plus_archive_growth_bytes'] for x in all_metrics],
 'write_bytes':[x['process_write_bytes_cycle'] for x in all_metrics],
 'integrity':{k:v for k,v in body['integrity'].items() if k!='archive_file_sha256'},
 'archive_file_count':len(archive_hashes),'sampler':args.sampler,
 'resource_samples':None if resources is None else resources['sample_count']},separators=(',',':')))
