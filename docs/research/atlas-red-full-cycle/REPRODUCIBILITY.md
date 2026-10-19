# ATLAS RED full-cycle — reproducibility

## 1. Environment

```bash
# Host: Linux 5.15, 2 CPUs, 3.7 GiB RAM, /home/kasun on /dev/sda1
# ATLAS virtualenv (owner-owned, reused, not modified):
PY=/home/kasun/Music/atlas/.venv/bin/python      # Python 3.12.13
```

The research workspace is a kernel-enforced, size-capped ext4 filesystem on a
loop device. Re-create it after a crash or on a new host:

```bash
/home/kasun/Music/atlas-red-ws/mount_workspace.sh
```

## 2. Branch and base

```bash
cd /home/kasun/Music/atlas
git fetch --all --prune
git worktree add -b research/atlas-red-full-cycle-evidence-ml-2026-10-19 \
    /home/kasun/Music/atlas-red-ws/mnt/wt/atlas-red-full-cycle \
    7dcf68d85135f94fb47343a9fc8dced9a4557358
```

`7dcf68d` is the verified G1-B correction tip and is a descendant of every
`origin/research/atlas-red-*` and `origin/fix/atlas-red-p1-*` branch except
`research/atlas-red-binance-evidence-e1-2026-10-17`, whose content is carried
by `research/atlas-red-binance-evidence-e1-recheck-2026-10-17` (an ancestor of
the base).

## 3. Collection (bounded public probe)

```bash
cd /home/kasun/Music/atlas-red-ws/mnt/wt/atlas-red-full-cycle
export PYTHONPATH=$PWD/src
export TMPDIR=/home/kasun/Music/atlas-red-ws/mnt/tmp
export HF_HOME=/home/kasun/Music/atlas-red-ws/mnt/models/hf
export PIP_CACHE_DIR=/home/kasun/Music/atlas-red-ws/mnt/cache/pip
export UV_CACHE_DIR=/home/kasun/Music/atlas-red-ws/mnt/cache/uv

$PY scripts/atlas_red_full_cycle_collect.py \
    --workspace /home/kasun/Music/atlas-red-ws/mnt \
    --duration-minutes 235 --cheap-interval-seconds 30 \
    --deep-cohort-size 20 --rotation-step 7 --universe-symbols 140 \
    --external-baseline-bytes 2811588608
```

The collector is restart-safe: cycle identifiers are time-bucketed, and a
cycle that is already present in the store is skipped. It is also
read-only and credential-free: it uses the production allowlisted public HTTP
client and the production Binance translators.

## 4. Evaluation

```bash
$PY scripts/atlas_red_full_cycle_evaluate.py \
    --workspace /home/kasun/Music/atlas-red-ws/mnt --max-closes-per-symbol 16
$PY scripts/atlas_red_full_cycle_actions.py \
    --workspace /home/kasun/Music/atlas-red-ws/mnt
```

The first stage builds the discovery population, the market context and the
per-strategy applicability matrix. The second stage seals the research
actions and the modelled lifecycle paths from the stored applicability
records.

## 5. ML and reporting

```bash
$PY scripts/atlas_red_full_cycle_ml.py \
    --workspace /home/kasun/Music/atlas-red-ws/mnt \
    --output /home/kasun/Music/atlas-red-ws/mnt/logs/ml_report.json
$PY scripts/atlas_red_full_cycle_report.py \
    --workspace /home/kasun/Music/atlas-red-ws/mnt \
    --output /home/kasun/Music/atlas-red-ws/mnt/logs/report.json
```

`--download` on the ML script attempts the bounded checkpoint download; it is
off by default because the checkpoint footprints do not fit beside the
retained evidence inside the strict 5 GB boundary.

## 6. Tests

```bash
$PY -m pytest -q -o addopts='' tests/v2/test_atlas_red_full_cycle.py
```

21 tests: bounded storage and oversized-write rejection, append-only store,
discovery denominator, production predicate reuse, S3/S7 source refusal,
action sealing, lifecycle accounting, ML leakage rejection, absence of any
order/private/capital path, and the three immutable freeze hashes.

## 7. Data locations and identities

| Artefact | Location | Identity |
| --- | --- | --- |
| Research evidence store | `/home/kasun/Music/atlas-red-ws/mnt/data/research.sqlite` | local, not published |
| Raw market payloads | inside the store (`source_observation.payload_zlib`) | local, not published |
| Storage manifest | `/home/kasun/Music/atlas-red-ws/mnt/data/storage_manifest.json` | local |
| Sanitized evidence (published) | `docs/research/atlas-red-full-cycle/evidence/` | `EVIDENCE.sha256` |
| Model cache | `/home/kasun/Music/atlas-red-ws/mnt/models/hf` | local, empty (downloads blocked) |

## 8. Restore commands for the reclaimed worktrees

The session reclaim removed 22 clean, already-pushed worktrees. Each is
re-creatable with:

```bash
git -C /home/kasun/Music/atlas worktree add /home/kasun/Music/<dir> <branch>
```

with the directory/branch pairs recorded in
`/home/kasun/Music/atlas-red-ws/mnt/audit/RESTORE_MANIFEST.md` from the
session reclaim run.

## 9. Reclaiming the caches that were cleared

```bash
# uv and pip caches were cleared; they repopulate on the next install
$PY -m pip install --cache-dir ~/.cache/pip <packages>
```

## 10. What cannot be reproduced from this branch alone

* The raw market payloads and the research database are intentionally **not**
  published to the public repository. Only sanitized aggregates, manifests and
  reports are published, per the repository's own rule against publishing raw
  market archives.
* The foundation model checkpoints were not downloaded; their exact
  repository identity and revision are recorded in `MODEL_INVENTORY.csv` and
  can be re-fetched with `--download` where the storage budget allows.
