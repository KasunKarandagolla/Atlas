# ATLAS RED full-cycle — storage report (session 2026-10-19/20)

The owner requires that **total ATLAS-managed storage stays strictly below
5,000,000,000 bytes at all times**. This report records the measured baseline,
the enforced boundary, the peak, the final figure and the cleanup operations.

## 1. Enforced boundary

The session writes inside one kernel-enforced, size-capped ext4 filesystem
mounted from a loop device. The cap is a property of the filesystem, not of an
application check: a write past the cap fails with `ENOSPC` from the kernel.

| Item | Value |
| --- | --- |
| Workspace path (host-visible) | `/home/kasun/Music/atlas-red-ws/mnt` |
| Backing device | `/dev/loop22` (ext4, `mkfs -m 0 -F -E discard`) |
| Filesystem size (hard cap) | 1,830,981,632 bytes |
| Image file | `/home/kasun/Music/atlas-red-ws/volume.img` (sparse; 1,872,072,704 bytes allocated) |
| Mount recovery | `/home/kasun/Music/atlas-red-ws/mount_workspace.sh` (re-creates the loop device and mount after a crash) |
| Application guard | `src/atlas/v2/research/full_cycle/storage.py` — reserves before every write, reconciles actual bytes, refuses over-budget writes, emits `data/storage_manifest.json` |

All session-controlled paths are inside the boundary: `TMPDIR`, `HF_HOME`,
`PIP_CACHE_DIR`, `UV_CACHE_DIR`, the research database, the model cache, the
logs and the worktree.

## 2. Baseline and current occupancy

| Measurement | Bytes |
| --- | --- |
| ATLAS-managed baseline at session start (all `atlas*` trees, venvs, worktrees, caches) | 15,802,662,779 |
| ATLAS-managed baseline after reclaim (see §4) | 2,811,588,608 |
| Workspace occupancy at report time | 490,782,720 |
| **Total ATLAS-managed occupancy at report time** | **≈ 3,302,371,328** |
| Guard protective write-stop threshold | 4,800,000,000 |
| Absolute ceiling | 5,000,000,000 |
| Maximum occupied bytes observed this session | 4,676,318,464 (immediately before the workspace cache reclaim) |
| Maximum reserved bytes observed | 1,536,000,000 (a single `pip` target install) |
| Host filesystem remaining at report time | 102,614,458,368 |

The pre-existing baseline was **already above the 5,000,000,000-byte ceiling**
when the session started. Per the storage override, no owner data was deleted
to force compliance; instead ATLAS-owned, unambiguously regenerable artefacts
were reclaimed and every new write was confined to the kernel-enforced
workspace.

## 3. Peak and pause behaviour

The guard did its job twice and both events are recorded:

1. **Collector pause — `storage_guard=PROTECTIVE_WRITE_STOP` (2026-10-20
   01:46Z).** The collector stopped ingesting new observations rather than
   continuing to grow the database. It resumed after the accounting was
   corrected and the reclaim in §4 completed. No evidence was lost: the pause
   is a delay, not a discard.
2. **Package install refusal.** The CPU Torch install failed with
   `OSError: [Errno 28] No space left on device` inside the bounded
   filesystem. The install was not retried outside the boundary. This is why
   the three foundation checkpoints are recorded as `BLOCKED BY ENVIRONMENT`
   for inference rather than being run outside the boundary.

## 4. Cleanup operations (session-owned, regenerable)

| Path | Bytes freed | Why it is safe |
| --- | --- | --- |
| `~/.cache/uv` | 4,651,566,292 | Package cache; fully regenerable |
| `~/.cache/pip` | 2,185,951,497 | Package cache; fully regenerable |
| `atlas-session-037-validation/*-tmp`, `production-large-3`, `windows-native-*` | ≈ 2,500,000,000 | pytest `tmp_path` scratch and Windows build scratch; the curated evidence files in that directory were kept |
| `atlas-session-010/.venv` | 509,388,449 | Duplicate virtualenv; regenerable from the lock |
| `atlas/build`, `atlas/dist` | 606,857,788 | Build output; regenerable |
| 22 clean, already-pushed git worktrees (`atlas-session-010…019`, `atlas-s41-*`, `atlas-session-036…040`, `atlas-tq01-worktree`, `atlas-s45-worktree`) | ≈ 840,000,000 | Each had `git status` clean and its branch pushed to `origin`; the exact restore commands are in the session reclaim log and in `docs/research/atlas-red-full-cycle/REPRODUCIBILITY.md` |
| `atlas-session-039/040/036/038-validation` scratch sub-trees > 5 MB | ≈ 1,800,000,000 | Regenerable test scratch; the top-level curated evidence files were kept |
| Disposable `pip --target` staging and the pip cache inside the workspace | ≈ 1,090,000,000 | Session-owned; re-downloadable |

**Nothing was deleted that was the sole retained copy of a raw receipt, a
pinned training source, an immature outcome, active recovery evidence or a
protected test artefact.** The owner's dirty checkout
(`impl/session-035-public-host-source-qualification`, 16 modified and 15
untracked paths) was never touched.

## 5. Proof the quota is not bypassed

* A deliberately oversized synthetic write is **rejected before it can exceed
  the limit** — covered by
  `tests/v2/test_atlas_red_full_cycle.py::test_storage_guardian_rejects_oversized_reservation`.
* The cap is enforced by the kernel: the loop-backed ext4 filesystem returned
  `ENOSPC` during the package install rather than silently exceeding its size.
* The `pip` install was **not** moved outside the boundary to make it fit.
* The `data/storage_manifest.json` file records the guard state, the limits,
  the peak occupancy, the peak reservation and the pause reasons.

## 6. Remaining gate

The boundary is **verified for this session**. It is not a standing guarantee
for other ATLAS activity: the pre-existing ATLAS-managed footprint outside this
workspace is 2,811,588,608 bytes and is owned by the owner, not by this
session. If that footprint grows, the workspace cap must be reduced
accordingly, and the guard's protective stop is the mechanism that enforces
that automatically.
