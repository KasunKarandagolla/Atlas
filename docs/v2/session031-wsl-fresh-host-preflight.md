# Session-031 Linux / WSL Fresh-Host Preflight

This runbook prepares a public-only, non-capital host check. It does not qualify a host, establish market-feed continuity, or start a campaign. The Session-031 Bybit adapter is opt-in through `atlas.v2.runtime.production:create_bybit_public_port`; the ordinary `atlas-ops` factory remains archive/index-only. A genuine public-market smoke and any 72-hour segment require a later separately scoped environment qualification.

## Checkout and locked environment

Use the accepted branch and commit recorded by the Session-031 handoff. Confirm the checkout is clean except for explicitly preserved local material. Install only from the accepted dependency locks in a disposable project environment; do not install host-wide services or exchange software for this preflight.

```sh
git fetch origin --prune
git switch impl/session-031-public-shadow-campaign-readiness-evidence-controls
git status --short
git rev-parse HEAD
sha256sum requirements-lock.txt requirements-agent-lock.txt
uv venv --python 3.12 .venv
uv pip sync --python .venv/bin/python requirements-lock.txt
.venv/bin/python --version
.venv/bin/python -m pip check
```

Expected: the handoff branch/SHA, the two recorded lock hashes, Python 3.12, and `No broken requirements found.` The install uses the ordinary baseline lock only; no provider credential or agent SDK is needed for baseline collection. A successful lock check verifies installed package consistency only; it does not verify a clean machine or venue data.

## Linux storage, durability, and capacity

For WSL, keep the active SQLite WAL database and Parquet archive on the Linux guest filesystem, such as `$HOME/.local/share/atlas`, rather than an actively written Windows-mounted path such as `/mnt/c`. Use one supervisor process as the only writer for one `ops.sqlite`; use separate copied stores for fault replicas and research experiments. The existing repository configures SQLite WAL with `synchronous=FULL`. Verify the chosen filesystem, free space, backup policy, and power-loss behavior on the actual host before any campaign.

```sh
export ATLAS_DATA_DIR="$HOME/.local/share/atlas"
export ATLAS_DB="$ATLAS_DATA_DIR/ops.sqlite"
mkdir -p "$ATLAS_DATA_DIR"
findmnt -T "$ATLAS_DATA_DIR"
df -h "$ATLAS_DATA_DIR"
```

Expected: a Linux filesystem with the data path inside the guest, enough free space for the declared local retention plan, and no active WAL workload placed on a Windows-mounted filesystem. Session 031 does not set a fixed RAM, CPU, or disk-growth qualification threshold; record actual capacity and measured growth before campaign approval.

## UTC clock, interruptions, and supervision

Use UTC and verify the Linux guest's time synchronization. WSL inherits host sleep, hibernation, and power interruption behavior; prevent sleep using the machine owner's approved settings before a genuine continuous segment. Keep the external process supervisor responsible for one process instance, clean stop, restart, and exit status. Do not count any interval that contains a process, host, clock, or network interruption as one continuous segment. Recovery starts a new epoch and must preserve exact snapshot and gap-repair receipts.

```sh
date -u
timedatectl show -p NTPSynchronized -p TimeUSec
systemd-inhibit --list
```

Expected: a UTC clock, `NTPSynchronized=yes`, and a reviewed sleep/hibernation policy. These commands report the current environment; they do not configure or qualify it.

## Public-only, read-only evidence preflight

Point `ATLAS_DB` at an existing local database. This command opens it read-only and does not start collection, access credentials, or send a venue request.

```sh
ATLAS_AS_OF_NS="$(date -u +%s%N)"
.venv/bin/python -m atlas.v2.science.session031_preflight \
  --db "$ATLAS_DB" --as-of-ns "$ATLAS_AS_OF_NS" --summary
```

Expected: a deterministic report identity, explicit counts and evidence limitations, `decision influence: False`, `admission influence: False`, capital and assisted execution disabled, and economic value `NOT ESTIMABLE`. Empty evidence, missing source inputs, and unavailable outcome labels stay missing. A report schema or successful read is not a source-coverage pass.

The report sampler reads current CPU/load, process RSS, disk, SQLite page, and WAL observations where the operating system exposes them. Sampling failures remain explicit. It does not add telemetry workers or write to the ops database.

## Network and credentials

Ordinary verification is offline. Baseline collection needs no exchange credentials and the adapter contains only public GET access for Bybit BTCUSDT and ETHUSDT USDT-linear perpetual market data. Do not place exchange credentials, provider keys, or `.env` secrets on the host. Confirm outbound public-network availability and current venue rate limits during a separately authorized public-data smoke; endpoint reachability alone does not qualify strategy-ready coverage. Binance production ingestion remains unqualified.

The public adapter makes at most 13 fixed REST requests per collection cycle, with a 1.25-second request timeout and bounded response sizes; metadata is read during adapter recovery. These are code bounds, not an endorsement of any host's actual throughput, continuity, uptime, or rate-limit allowance. Its recent-trade and bar windows are insufficient by themselves for S2/S3 warmup.

## Stop conditions before campaign scheduling

Keep the host at `UNVERIFIED / TEST GATE` until the actual Linux/WSL runtime, UTC synchronization, filesystem durability, storage budget, CPU/RAM use, public-network stability, external process supervision, single-writer ownership, shutdown/restart, and interruption accounting have been evidenced. Native Windows deployment is not qualified by WSL tests. A successful public endpoint probe is not S1–S3 strategy-input qualification. Do not start a 72-hour campaign from this runbook.
