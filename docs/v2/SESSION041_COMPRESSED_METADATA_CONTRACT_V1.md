# Session 041 compressed artifact metadata contract V1

This additive representation compresses only `artifact_index.metadata_json` for
`UniverseContractV2`, `BroadUniverseWorksetV2` (the actual runtime `STATE_TYPE`),
and `UniverseObservationV2`. New metadata whose canonical UTF-8 JSON occupies at
least 4096 bytes is considered for a versioned zlib envelope. The envelope is
stored only when its complete UTF-8 representation is strictly smaller than the
original canonical JSON; incompressible content retains plain JSON. Smaller
metadata and other artifact types also use plain canonical JSON. Existing rows are never rewritten
when reopened or registered again.

The envelope is a canonical JSON object with exactly one top-level key,
`__atlas_metadata_storage__`. Its object contains exactly these fields:

| Field | Contract |
| --- | --- |
| `version` | `ATLAS_COMPRESSED_METADATA_V1` |
| `codec` | `zlib` |
| `uncompressed_bytes` | Exact canonical UTF-8 byte count, integer 4096 through 33554432 |
| `sha256` | Lowercase SHA-256 of the complete uncompressed canonical JSON bytes |
| `data` | Strict base64 of a single complete zlib stream |

The storage marker is reserved for the representation and cannot be submitted as
domain metadata for these three types. Writes use zlib level 6. Decoding accepts
any valid zlib encoding that restores the same canonical bytes; encoding choices
do not create a new artifact identity.

`ArtifactIndexEntryV2._from_storage_row`, single artifact reads, batched metadata
reads, typed reads, newest reads, and paged reads all use the central decoder.
Domain callers receive the original metadata object, including complete universe
membership, cheap quote fields, coverage, workset membership, timestamps, and
authority fields. Artifact ref, artifact type, content hash, creation time, and
availability time are unchanged. Availability and cursor filtering continue to
use the original SQL columns. The representation supplies no economic, safety,
amendment, capital, or permission authority.

Duplicate registration compares decoded canonical original metadata together with
the original immutable index columns. A legacy plain row and a compressed row
therefore compare identically when their original content agrees. Different
metadata, hashes, or times remain conflicts. An accepted existing row retains its
rowid and exact stored bytes; no migration, deletion, or in-place recompression is
performed.

The decoder checks exact envelope fields, canonical JSON, version, codec,
allowlisted type, strict base64, declared byte bound, exact output size, SHA-256,
complete stream termination, and absence of trailing or concatenated streams.
Decompression requests at most the declared size plus one byte, with a global
32 MiB declaration cap; it never flushes into an unbounded output allocation.
Compressed input is bounded to 32 MiB plus 65536 bytes before decompression.
Malformed or corrupt data raises `ValueError`. Existing paged APIs expose their
invalid-entry counts and original cursor keys; they never return the envelope as
domain metadata. No pickle, evaluation, or executable deserialization is used.

Repository/schema audit found no SQL metadata expression or partial metadata
index for these three types. Their SQL queries use index columns, so no nested
metadata projection or schema change is required. All former repository raw
`json.loads(metadata_json)` paths now use the central decoder. Outside the
repository, `tuning_export.py` is the persisted raw metadata consumer: both its
indexed insertion merge and bounded legacy scan call `_from_storage_row`.
Because the writer keeps plain JSON when base64 would grow the representation,
its existing SQL storage-length budget cannot reject an accepted new original
solely due to encoding growth. The decoder independently caps original output
at 32 MiB. `production.py` also constructs synthetic plain
metadata rows from already decoded entries. The native resilience script reads
refs and size diagnostics rather than decoding the allowlisted metadata.

Source evidence is in `tests/v2/test_session041_compressed_metadata.py`: mixed
legacy/new reads, duplicate registration, conflicting identities, restart,
read-only byte preservation, causal cutoffs, all generic read APIs, Unicode byte
thresholds, alternate valid zlib encodings, malformed envelopes, tampering,
truncated/trailing/concatenated streams, expansion beyond 32 MiB, and complete
actual broad workset/universe identities. The storage measurement includes every
artifact row in its fixture, including unchanged product and observation rows;
the XML properties retain both plain and stored UTF-8 totals and row counts.

This change addresses repeated large universe/workset metadata only. It does
not establish that public raw-source or quote-index storage amplification is
solved, and it makes no installer, venue, Windows-native, or long-duration soak
claim.
