# G1-B offline target-host retest

**Prepared only; not run on the owner’s 4 GB computer.** This procedure is offline and creates only synthetic fixtures in a new temporary location. Run it on the exact independently reviewed candidate after the remote SHA is approved. It does not authorize public source access, OBSERVE activation or a capital decision.

The current `qualify_public_observation.py` harness and the B benchmark use Linux `/proc` resource and I/O facts. They are not native Windows host qualification. If the selected 4 GB computer is Windows, this procedure is blocked until an independently reviewed Windows resource sampler/runner can be run on that machine; WSL results do not qualify native Windows.

## Preconditions

Use a full source checkout of the exact accepted candidate, including `tests/`, on the actual target computer. Use its locked Python 3.12 environment, `uv lock --check`, the installed runtime dependencies, and a new empty output directory on the real intended storage volume. Close production databases. Do not use the diagnostic free-space override or a temporary RAM disk. No credentials, provider, account, model weights or market-data source are needed.

Record the host model, operating system, CPU, RAM, filesystem, selected-path free bytes and exact candidate SHA. Preserve earlier failed runs; give each invocation new output names. Keep the session on battery/AC conditions and normal background services representative of the intended operation.

## Run sequence

First run the low-overhead maximum stress B profile. It includes production adoption, receipt lookup, durable workset publication, and exact post-run archive/index/domain/restart audits; the audits do not sit inside the ten-second service timer.

```sh
/absolute/locked-venv/bin/python docs/research/atlas-red-g1-broad-performance/evidence/benchmark_b.py \
  --per-venue 2048 --cycles 3 --venue both --sampler none \
  --source-checkpoint <reviewed-remote-sha> \
  --dirty-source-diff-sha256 <reviewed-diff-sha256> \
  --output /absolute/new-output/g1b-target-b-max-4096.json
```

Then run the existing selected-path offline qualification harness without diagnostic overrides:

```sh
PYTHONPATH=src /absolute/locked-venv/bin/python scripts/qualify_public_observation.py \
  --storage-path /absolute/new-output/target-probe \
  --output /absolute/new-output/g1b-target-preflight.json \
  --maximum-breadth --environment 'G1B reviewed-candidate target retest'
```

If the actual host and selected path reject the original reserve, preserve the refusal and stop. The separate G1-B 48-hour growth estimate is 1,377,141,631,200 bytes under the existing S41 projection factors, before any owner reserve; it is a conditional synthetic estimate, not a retention authorization. Do not change the `None` capacity constant, delete or defer evidence, reduce breadth, or substitute diagnostic free space.

## Review the outputs

For B, verify all three `production_service_ns` values are below 10,000,000,000 ns and that the product/row counts, source-ID hashes, raw-ID hashes, exact receipt/index reads, archive hashes, artifact counts, restart worksets and logical-store hash agree. A service pass is limited to this fixed synthetic profile. It does not establish live population, source continuity, stream backlog, native T+5, endurance or Windows behavior.

For the host qualification output, retain the unmodified `host_preflight` result. `allowed=false`, `INSUFFICIENT_DISK_HEADROOM`, or another failed original gate means TEST GATE even if `diagnostic_preflight` succeeds. Diagnostic overrides never admit the host. Hash both JSON files and keep the full stderr/exit status. Do not run `src/atlas_product_entry.py --commission-binance-diagnostic` here; that later bounded public-only command requires a separate owner request.

If both tests pass, the physical host still needs the separately accepted endurance/retention and real-source work before any public acquisition request. If they disagree with this report, publish the exact raw evidence and stop for review; do not retune thresholds or replace the failed profile.
