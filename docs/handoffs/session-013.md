# ATLAS V2 Session 013 — Phase-1 local-worker isolation gate

## Checkpoint and scope

- Starting branch: `impl/session-012-v2-phase1-data-model-core`.
- Starting SHA and verified remote tip: `6d7d966842390cefdc4ec0782f1820d9a68044e8`.
- Final branch: `impl/session-013-v2-phase1-worker-isolation-gate`.
- Final SHA: reported after push in the Session-013 closeout; a commit cannot contain its own SHA.
- The existing Session-009 checkout and its untracked files were left untouched. This work used a clean dedicated worktree.
- Authority read: the V1 clarification/freeze, supplied V2 final freeze, V1 exit audit and golden baseline, Session-011 and Session-012 handoffs, and the Session-012 model implementation/tests.
- Scope: the Linux local model worker boundary and its qualification only. No V1 production, public-data, model ABI, remote-provider, strategy, evaluator, risk, desktop or capital behavior was changed.

## Isolation mechanism

- Backend: root-owned `/usr/bin/bwrap`, **bubblewrap 0.6.1**. `LocalProcessProvider` requires it and raises `WORKER_SANDBOX_UNAVAILABLE` if it is missing, untrusted, or cannot set up the sandbox. There is no same-user unsandboxed fallback.
- `--unshare-all` creates separate user, PID, mount, network, IPC, UTS and supported cgroup namespaces; `--proc /proc` exposes sandbox processes rather than the host process table. Tests directly compared host and worker PID/network namespace identities and tried the exact parent `/proc/<pid>/environ` path.
- The worker runs from a read-only root with a fresh `/tmp` tmpfs, minimal `/dev`, and isolated `/proc`. Bubblewrap receives a restricted environment and clears it before launching Python. No host home, repository root, `/mnt/data`, V1 live-control database, `.env`, credential directory or host `/tmp` is mounted.
- Read-only runtime mounts are `/usr`, existing `/lib` and `/lib64`, and the resolved CPython 3.12.13 base prefix `/home/kasun/Downloads/workshop/gate5-openhands/python/cpython-3.12.13-linux-x86_64-gnu`. Only empty parent directories needed for that prefix are created. The project checkout is not writable or mounted as a unit.
- Trusted provider configuration may supply explicit `approved_read_only_paths`; the immutable `ModelRequestV2` ABI carries no mount path. Each approved file or narrow directory appears under `/artifacts/<index>` read-only. Validation rejects missing paths, symlinks, special files, protected names/extensions, SQLite file signatures even under disguised names, broad root/home/project/runtime/tmp/var mounts, uninspectable directories, directory traversal across a mounted filesystem, and duplicate/overlapping mounts. No artifact mount is configured by default.
- The existing input/output bounds, POSIX CPU/address-space/file-size limits, original request deadline, `close_fds=True`, process-group kill, and Bubblewrap parent-death cleanup remain in force.

## Malicious-worker qualification

**TESTED** on the real Bubblewrap backend. Worker source contained the exact host pathnames of temporary fake exchange-secret, `.env`, V1 live-control SQLite, and account/private-token files. Every direct read and `/proc/1/root` traversal failed. The host home tree and fake files could not be found by enumeration; the host project and host `/tmp` were absent from the worker view. The child environment excluded the fake parent secret. The worker also failed to recover that secret through the exact host `/proc/<parent-pid>/environ` path or any visible `/proc` process, and the parent PID was absent.

**TESTED** network namespace separation and a failed outbound socket connection. **TESTED** an explicitly approved fake model artifact readable at `/artifacts/0` but not writable; an unapproved neighbor file remained hidden. `/tmp` accepted a temporary file, while `/` was read-only and host temporary files were hidden. **TESTED** that an inheritable parent descriptor for a fake protected file did not reach the worker. **TESTED** actual worker resource limits, output bound, timeout cleanup with a spawned helper, and a valid deterministic `ForecastArtifactV2`.

All secret strings and account identifiers in these tests are fake fixture values. Temporary fake files are outside the repository and are not tracked.

## Test and preservation evidence

- Targeted worker/model tests and Phase-1/V1 integration smoke: **TESTED**, 30 passed.
- Full `PYTHONPATH=src:. python -m pytest -ra`: **TESTED**, 471 passed, 1 skipped in 378.18s on the final code. The skip is the pre-existing opt-in public testnet check.
- `python -m ruff check`: **TESTED**, passed.
- `python -m mypy src tests`: **TESTED**, no issues in 217 source files.
- `python -m compileall -q src tests`: **TESTED**, passed.
- `git diff --check`: **TESTED**, passed.
- `docs/v2/V1_GOLDEN_BASELINE.json`: **TESTED**, no diff; SHA-256 `b6d47c1c7a2e416304dd57d9055599201c474c8bada600cf23c2cbc09f90e53c`.
- No V1 production file or schema changed; `CRYPTO_TREND_24H_V1`, V1 TradePlan/RiskPolicy/capability semantics, UNKNOWN recovery, `assisted_enabled=false`, and all six Bybit capabilities remain as before, with venue capabilities **UNVERIFIED**.
- Session-012 short public Bybit/Binance qualification is reused unchanged. Public collection and `ops.sqlite` schema version 1 were not modified.
- Secret scan: no `gitleaks`, `trufflehog` or `detect-secrets` executable was available. A tracked-file fallback scanned 241 paths for private-key headers, common service-token patterns and non-template `.env` files: **TESTED**, zero high-confidence hits. Fake test files were not staged.

## Review correction and verdict

Session-012's 72-hour soak status **BLOCKED BY ENVIRONMENT** is valid for that soak itself. The V2 freeze explicitly says to begin or continue the longer soak without blocking unrelated coding. Therefore the soak is not the Phase-1 acceptance blocker and is not claimed complete or running. The Phase-1 blocker discovered in review was the missing OS-enforced local-worker isolation; this session closes it on the qualified Linux environment.

- Linux local-worker isolation: **IMPLEMENTED / TESTED** with actual Bubblewrap denial evidence.
- Windows local-worker isolation: **UNVERIFIED**; no Windows backend was implemented or qualified in this bounded session.
- Foundation checkpoint/package integration and economic value: **UNVERIFIED / NOT ESTIMABLE** as recorded in Session 012; model influence remains zero.
- V2 capital authority and authenticated V1 venue capabilities: **TEST GATE / UNVERIFIED**; no capital authority is granted.
- V2 Phase 1 verdict: **PASS** for the qualified Linux Phase-1 engineering environment. This authorizes progression only to **V2 Phase 2 research/shadow implementation**. It does not authorize capital.
