# S39 public evidence storage remediation — version 1

Status: IMPLEMENTED. Engineering and native package validation: TEST GATE until the final validation ledger records exact executed results. Owner live endurance: UNVERIFIED.

The S38 owner run `80e0d758bf4c41f78e8dbb3f92796354` reported current sources without local queue loss, but continuity export failures accumulated and SQLite/RSS grew rapidly. The owner raw database/export is absent from this environment. Consequently the exact host database attribution remains UNVERIFIED; deterministic reproduction establishes the code defect and its matching failure mechanism.

The accepted starting checkpoint is `efd75ca4e0cb432c79e0a090b363ca09b87e9099`, descended from S37 and S38 package source `2f57e5db4fcd2e28d8bb9161ad529754fb71e00b`. No accepted branch is edited or merged.

## Reproduced defects

The pre-change accelerated fixture processes actual archived BTC/ETH book and trade frames at 160 virtual frames/second and forces five reports/second. This publication cadence is deliberately accelerated, rather than a measurement of owner cadence. At 45 virtual seconds the continuity metadata reaches 200,408 bytes, exceeding the unchanged 131,072-byte exporter guard. At 90 seconds it reaches 396,384 bytes, with 600 oversized publications, SQLite 445,448,192 bytes and peak RSS 846,987,264 bytes. The queue high-water is 32 and rejected frames are zero: the storage defect reproduces independently of S38 queue starvation.

The current-epoch book retains every frame, and a feature includes their ancestry. Repeated reports therefore serialize progressively longer prefixes. Separately, every continuity publication repeats bounded but already full 2,048-entry replay/trade caches; bounded individual objects still produce excessive persistent duplication. Restart previously decoded thousands of these full snapshots. The accelerated regression also exposed cold book objects being allocated for trade channels, where pending book evidence could never be published; the corrected composition allocates and maintains book lineage only for order-book channels. The first compact-report prototype still measured SQLite 69,550,080 bytes over 128 virtual seconds, so it was insufficient as the storage correction.

## Selected representation

The existing sole ops writer remains the only database and raw archive publisher. Historical raw bytes remain immutable. There is no second database, writer, execution authority, model, provider or scientific policy.

* `PublicStreamContinuityCheckpointV2` stores the exact scalar continuity facts and a parent checkpoint/transport reference. Lookup caches remain bounded in memory; they are explicitly incomplete after restart, requiring the durable exact trade-ID lookup. Legacy `PublicStreamContinuityStateV1` remains readable. A restarted controller does not restore a warm book.
* `PublicBookLineageCheckpointV1` binds the exact terminal book-state hash/BBO/sequence/receipt to bounded ordered transport batches, typed archive checkpoints, frame-health records, control events and a parent checkpoint. FIFO transport is necessary: sorting typed chunks by update ID would reorder an intra-batch snapshot reset. Raw parents are never removed from retained lineage.
* The live book retains the declared 30-second feature window plus its left baseline, at most 4,096 frames/seen identities and 4,096 levels per side. Exceeding capacity invalidates the book and requires snapshot recovery; it does not silently truncate a qualifying window. A historical cutoff outside the retained window requires immutable replay.
* Complete compressed Arrow IPC streams are appended as individually hash-verified extents in segments capped at 32 MiB. Each extent has row, encoded-byte, IPC-byte and decoded-byte bounds. Buffer compression and independent whole-record compression avoid repeated schema overhead without a cross-record compression dictionary. The writer fsyncs the extent before indexing it. Appending never modifies an earlier extent. Restart starts a new segment; unindexed/orphan bytes remain retained and cannot fabricate a checkpoint. Legacy closed Parquet chunks remain unchanged and readable.
* `PublicStreamTransportBatchV2` and `L2FrameArchiveCheckpointV3` bind exact archive extent identities. The original raw rows and their domain hashes/timestamps remain available. Source-health batch records are archived completely, rather than copied as a generic operational row for each high-rate batch/channel.
* `public_stream_archive_locator_v1` is a versioned auxiliary access index in the same `ops.sqlite`, containing fixed-size binary refs and row locators for raw-frame, trade-observation and batch source-health entries. A read reconstructs the original `ArtifactIndexEntryV2` metadata and verifies its original hashes against the immutable Arrow record. It does not reinterpret receipts, availability, event identities or trade IDs. The bounded decoded-extent cache retains at most eight chunks and a 32 MiB declared Arrow-byte budget. The cache is an optimization, not evidence.
* `PUBLIC_CONTINUITY_REPORT_INDEX_V2` stores the unchanged `PublicStreamContinuityReportV1` domain wire and exact `state_ref`/`source_health_ref`, with a `transport_ref` to the already immutable health record. Export validates this versioned representation, follows the exact health binding and reconstructs the same transport metrics. It does not raise the compact metadata guard or forgive oversized old records.
* Partial indexes seek at most eight restart heads, bounded current trade windows and exact gap/recovery evidence. Active S3 reconstruction does not perform an elapsed-runtime-growing raw directory audit. Overflow or unsupported exact trade completeness remains explicit NOT ESTIMABLE.

## Causal and compatibility boundaries

Actual transport receipts are unchanged. Derived archive descriptors are published after durable writes, with a separate actual availability stamp. Book/report information cutoffs are current controller times, rather than historical market cutoffs. New book/checkpoint report storage explicitly records computation start/finish and later publication availability; validators and consumers distinguish these from the earlier information cutoff. Raw source chronology and restart recovery epochs remain exact. Capital, assisted execution and critic/model authority remain disabled/ZERO. RiskPolicy, leverage, model/provider versions, V1 control serialization, final holdout and S3 completeness semantics are unchanged.

This is a versioned storage representation, not a scientific, safety or capital amendment. The logical core ops schema and existing V1 live-control database are unchanged. The new auxiliary table/index is an access projection over immutable raw evidence. Existing databases can be opened without rewriting old artifacts; read-only legacy databases do not require a migration writer. No owner evidence is deleted or rewritten into a pass.

Rollback preserves the database and archive. An old binary does not know the new physical storage versions; do not resume an S39-created run with an older product. Use a new run identity for the next owner retest. Reinstall/uninstall must preserve the external evidence directory. Review exact package/source identity before owner installation.

## Rejected alternatives

Increasing the metadata limit, queue size, disk limits or retention wiping hides the defect. Silently dropping ancestry loses reproducibility. Repeated full cache snapshots and one small Parquet file per high-rate group are avoidable duplication. A new database, asynchronous database writer or generic persistence platform would enlarge the reliability seam without resolving causal ownership.

## Qualification boundary

Accelerated storage tests establish explicit active bounds, record validity, exact replay, corruption behavior and explainable measured growth under their declared fixtures. Native sustained tests exercise the S38 producer/service/slow-REST composition with actual persistence. Neither test establishes owner hardware capacity, genuine source completeness or 48-hour endurance. The new owner run must measure actual frame sizes/cadence, disk growth, working set, WAL, queue headroom and periodic report validity while retaining the failed S38 run.
