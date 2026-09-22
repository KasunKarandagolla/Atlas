"""Deterministic SQLite bootstrap/migration to schema v5."""
from __future__ import annotations

import sqlite3

from .schema import DDL_STATEMENTS, SCHEMA_VERSION


def _metadata_exists(c:sqlite3.Connection)->bool:
    return c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_metadata'").fetchone() is not None

def current_version(c:sqlite3.Connection)->int|None:
    if not _metadata_exists(c): return None
    row=c.execute("SELECT value FROM schema_metadata WHERE key='schema_version'").fetchone(); return int(row[0]) if row else None

def _columns(c:sqlite3.Connection,table:str)->set[str]: return {r[1] for r in c.execute(f'PRAGMA table_info({table})').fetchall()}

def bootstrap(c:sqlite3.Connection)->int:
    existing=current_version(c)
    if existing is not None and existing>SCHEMA_VERSION:
        raise RuntimeError(f'future SQLite schema {existing} > supported {SCHEMA_VERSION}; fail closed')
    # Preserve old data; create all missing v5 tables.
    for ddl in DDL_STATEMENTS: c.execute(ddl)
    if 'recovery_certificates' in {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")} and 'compatibility_json' not in _columns(c,'recovery_certificates'):
        c.execute("ALTER TABLE recovery_certificates ADD COLUMN compatibility_json TEXT NOT NULL DEFAULT '{}'")
    # v1 compatibility: state_version did not exist.
    if 'intents' in {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")} and 'state_version' not in _columns(c,'intents'):
        c.execute('ALTER TABLE intents ADD COLUMN state_version INTEGER NOT NULL DEFAULT 0')
    # Current v4 query table used retention start/end and lacked scope. Rebuild if needed.
    cols=_columns(c,'reconciliation_query_evidence')
    if cols and ('scope' not in cols or 'retention_segments_json' not in cols):
        c.execute('ALTER TABLE reconciliation_query_evidence RENAME TO reconciliation_query_evidence_v4')
        c.execute(next(d for d in DDL_STATEMENTS if 'CREATE TABLE IF NOT EXISTS reconciliation_query_evidence' in d))
        rows=c.execute('SELECT * FROM reconciliation_query_evidence_v4').fetchall(); names=[d[0] for d in c.execute('SELECT * FROM reconciliation_query_evidence_v4 LIMIT 0').description]
        for row in rows:
            d=dict(zip(names,row,strict=True)); start=d.get('retention_coverage_start_ns'); end=d.get('retention_coverage_end_ns'); seg=[] if start is None or end is None else [[start,end]]
            scope='account' if d.get('instrument') is None else 'instrument'
            c.execute('''INSERT INTO reconciliation_query_evidence(query_id,query_type,scope,account,instrument,requested_interval_start_ns,requested_interval_end_ns,pagination_cursors_json,pages_observed,total_records_returned,completeness,status,source_time_ns,receipt_time_ns,request_ids_json,retention_segments_json,facts_json,evidence_hash,error_message) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',(d['query_id'],d['query_type'],scope,d['account'],d.get('instrument'),d.get('requested_interval_start_ns'),d.get('requested_interval_end_ns'),d.get('pagination_cursors_json','[]'),d.get('pages_observed',0),d.get('total_records_returned',0),d.get('completeness','unknown'),d.get('status','failed'),d.get('source_time_ns'),d.get('receipt_time_ns',0),d.get('request_ids_json','[]'),__import__('json').dumps(seg),'{}',d.get('evidence_hash',''),d.get('error_message')))
        c.execute('DROP TABLE reconciliation_query_evidence_v4')
    c.execute("INSERT INTO schema_metadata(key,value) VALUES('schema_version',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(str(SCHEMA_VERSION),))
    c.commit(); return SCHEMA_VERSION
