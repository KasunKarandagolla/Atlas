# Session 041 public observation metadata blocks V1

This is a lossless repository storage representation. It changes no domain
artifact identity, receipt, cutoff, capital authority, or risk contract.

Only full `PublicObservationIndexV2` collector metadata containing the record,
source, native instrument key and raw payload hash is eligible. Minimal legacy
metadata remains plain JSON. Existing immutable rows are never rewritten.

The generic artifact row retains its original ref, type, content hash and outer
timestamps. Its JSON contains the indexed receipt, event, source, revision,
archive and hash fields plus a digest of the exact canonical instrument key.
The complete original metadata is retained in deterministic blocks sorted by
artifact ref. Each block contains at most 512 rows and 4 MiB of canonical JSON;
its identity is SHA-256 of those bytes. Zlib compression supplies no new identity.
A transaction publishes the block, artifact-to-block ordinal, instrument catalog
and generic artifact row together. A failed transaction publishes none of them.

Readback verifies codec and byte bounds, complete stream termination, block
hash, canonical JSON, row count, unique artifact identities, record identity,
ordinal, exact projection and the canonical key catalog and digest. Compressed
input is limited to 4 MiB plus 65536 bytes in SQL before copying into Python.
Expansion is limited to the declared size plus one byte. Corruption fails closed.
The immutable decode cache is bounded to eight blocks and 8 MiB of declared raw
bytes. Every block payload update increments its row revision in a SQLite trigger;
cache keys include that revision, so direct or concurrent edits force revalidation.
Every hydration still verifies the selected row projection and key catalog. Caller
mutation cannot alter the frozen cached values.

Repository read APIs hydrate the same immutable domain metadata directly, with
no JSON serialization and reparse on each cache hit. Export uses that boundary.
Receipt queries retain their chronology and complete identity filters. New
digest indexes have versioned names; writable reopen does not rebuild them.
Read-only pre-migration stores use the original plain-key indexes and expressions.
The ordinal uniqueness constraint supplies the block-order access path.

The bounded broad collector prefetches exact identities in 32-row windows.
Every supplied lookup map explicitly covers each identity, including confirmed
absence. A missing key is an error. A flush refreshes the lookup, so a negative
lookup cannot conceal a newly durable duplicate. Public stream service remains
between those windows. Source publication follows completed durable archive
capture and final receipt validation.

Focused evidence is in `test_session041_block_migration.py`,
`test_session041_compact_public_observations.py`, and the S37 receipt-query
regressions. Passing these contracts does not establish broad workload capacity,
native Windows suitability, elapsed endurance, or actual venue qualification.
