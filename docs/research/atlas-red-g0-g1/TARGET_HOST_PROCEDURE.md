# Target host qualification procedure — not executed on the owner's host

**G1-B update (19 October 2026):** the prior e75 maximum-breadth service failure is corrected on the offline synthetic 4,096-product two-venue profile; three low-overhead service samples are 9.364–9.775 seconds. The new [G1-B report](../atlas-red-g1-broad-performance/G1B_PERFORMANCE_REPORT.md) records byte-identical before/after persistence and the remaining storage/host gates. This update does not qualify the owner’s actual host or authorize collection.

Status: **TEST GATE**. This procedure does not authorize public collection or issue a hardware certificate. The new harness is Linux-only because its resource sampler uses `/proc`; it reuses the existing Binance path validator. Windows resource/host qualification remains unimplemented/unverified by this command. Original Windows product support is unchanged.

## Offline run

Use the independently reviewed exact candidate checkout and its supported Python3.12 environment. Check immutable freeze/algorithm identities and `uv lock --check`; install the existing pinned storage/runtime/development dependencies in an isolated environment. No trading credential, provider key, model weight or account source is needed. The harness imports the existing maximum-breadth test helper, so use a complete source checkout including `tests/`, not a stripped application wheel.

Select a real local canonical absolute data path outside the checkout, on the actual intended disk. Leave production stores closed. The harness creates a unique temporary directory and deletes it on ordinary completion; it never opens an existing production DB. Output must be a new file; preserve and hash the report. Example (replace paths with the real selected paths):

```sh
PYTHONPATH=src /absolute/venv/bin/python scripts/qualify_public_observation.py \
  --storage-path /absolute/local/atlas-qualification \
  --output /absolute/local/g1-host-measurements.json \
  --maximum-breadth --environment 'TARGET HOST MEASUREMENTS'
```

Default network is disabled; no public-enabled switch exists. The parent permits only this fixed Python worker, strips credential variables, enforces240s and captures child output. The worker denies socket connect/DNS/sendto and subprocess execution, including order/private/provider transports. Explicit refusal tests verify these boundaries. This is a source-only Python isolation check, not an OS sandbox against malicious native extensions; an approved public-only host process should also use OS/network restrictions. No execution owner, account client, provider dispatcher or trading configuration is constructed.

Exit0 means the bounded measurement record was complete and internally checked, **not a passing G1/host qualification**. Exit2 means incomplete/failed execution; retain its sanitized reason and any interrupted temporary evidence. There is no imported-record or activation mode. Every result says `actual_host_qualified=false`, `capacity_qualified=false`, `status=TEST GATE`. A consistent hash is not independent provenance authentication.

The unmodified `host_preflight` must pass its original disk/dependency/service gates before collection. The separate `diagnostic_preflight` deliberately supplies a synthetic free-space hook and a storage-only dependency hook so service mechanics can be investigated even on an unsuitable cloud path. It stays `FAULT_INJECTION`; its `allowed` field is **not** host admission. Real observed disk samples and both overrides remain in the report. Never copy the diagnostic value, cloud maximum-cycle integer or an arbitrary constant into production admission.

The report contains exact source/workload hashes, versions, ns samples and nearest-rank p50/p95/p99/max/count; native raw-first capture, adoption/readback and actual SQLite commit timing; all preflight fsync/write/read samples; raw/decoded/archive/receipt bytes; queue occupancy/rejections; two ordered full readbacks after restart; process-tree RSS/HWM/PSS/private memory, available memory bounded by cgroup limits where configured,CPU,scheduler wake delay,WAL/temp high-water and disk samples. The sampler interval is50ms; intermediate disk/queue peaks may be larger. Capture throughput includes startup,drain and reopen, explicitly not just a producer-window rate. Archive capture fsync is a last-batch sample; the separate preflight records every measured fixture persistence operation. No unavailable segment is supplied as a zero duration.

Maximum-breadth mode reuses `test_session041_scale_review.measure_bounded_cycles`:4096products,8192source rows/cycle,three exact ten-second synthetic clocks,existing collector/archive/Ops/workset/readers. It verifies every raw/index/receipt and all cutoff worksets after restart. Measured cycle bytes are logical DB growth plus closed Parquet file growth, **not** physical write I/O. WAL and process I/O are separate. The largest of three samples is passed only to a report-local existing capacity assessment; startup product metadata is reported separately. Its48h,metadata and safety factors remain unchanged; the result is a projection, not endurance or eight-week retention proof. The global `S41_MAX_BREADTH_BYTES_PER_CYCLE_V2` remains `None` and normal product admission stays fail closed.

## Review and stop criteria

Reject or remain TEST GATE for incomplete/mutated samples or populations, raw/index mismatches, lost/rejected frames, corrupted journals/extents, unavailable host resources, failed preflight, service exceeding the registered limits, growing backlog, insufficient disk/pins or missing platform evidence. No threshold,queue,fsync,retention or mandatory coverage change is permitted to obtain green. Keep normal/adverse profiles, source identity and single-writer guarantees fixed. Actual4GB steady/admitted load and72h/rollover remain separate requirements; a100-second fixture is not certification.

The e75 cloud maximum-breadth run exceeded ten seconds; the G1-B report records the corrected three-cycle service pass for the same synthetic max fixture. Storage remains an independent TEST GATE: the original selected-path preflight rejects the reserve, and the actual target host is untested. See [the G1-B offline retest procedure](../atlas-red-g1-broad-performance/G1B_TARGET_HOST_RETEST.md) for the exact reviewed-build host run. Do not launch a full observation campaign from this offline result.

T+5 is not relaxed. Capture receipt→controller-ready timing is supplied; maintained context→native features→strategy-ready→P1/Brief→Agent B/provider is not executed by this storage harness. Those missing segments are explicit. Historical reconstructedT+6/T+7 remains failed under its own assumptions. A complete native maintained-state timing test needs separately qualified actual input/consumer integration and the original deadlines. No late observation can repair an earlier opportunity.

## Later bounded public diagnostic — prepared only

A subsequent request must authorize the exact reviewed build, actual Linux host/path, public-only domains/channels,population,duration,byte/disk limits and stop conditions. Verify selected-device preflight and actual profile capacity first; keep zero credentials,zero provider spending and no execution. The existing separately bounded command is:

```sh
PYTHONPATH=src /absolute/venv/bin/python src/atlas_product_entry.py \
  --commission-binance-diagnostic --data-root /absolute/local/atlas-public-diagnostic
```

**Do not run it under this session's authority.** Its existing profile is `S41_BINANCE_SINGLE_VENUE_QUALIFICATION_V1`:120s measured,60s warmup bound,300s supervision,10s polling,16stream instruments/8depth/32topics caps,512items/16,000,000bytes,raw-first,WAL/FULL,execution/provider DISABLED,no account/credential profile. It is diagnostic-only,not ordinary admission,capacity certification,prospective OBSERVE activation or programme evaluation. No profile cap or timeout changed here. Mainnet public host/network permission must be explicitly configured for that later scope; today's demo-host allowance is not authorization. No public-network test was invoked here.

After exact-SHA independent review, run the offline max profile and original host preflight on the actual selected Linux host. A Windows target remains blocked by the Linux-only harness. If the real selected path rejects headroom, stop for a separately reviewed storage-contract decision. G2 is a separate Sol-owned genuine P1 original-population/calibration/provenance and typed-reader integration gate; its synthetic-only grades cannot be renamed. No G2,Agent B,provider,model,economic experiment or campaign begins from this handoff. OBSERVE / ZERO ALLOCATION / ZERO PAID BUDGET / CAPITAL NO-GO.
