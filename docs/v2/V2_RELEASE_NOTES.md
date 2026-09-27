# ATLAS V2 `2.0.0.dev25` release notes

**Classification:** `SHADOW_RELEASED`

**Capital:** disabled (`capital_enabled=false`, `assisted_enabled=false`)

**Purpose:** causal research, public-data shadow evidence, and a read-only desktop observer. This is not a trading-profitability release.

## What is included

- Separate V1 live-control and V2 research/evidence contracts. V1 schema 6 and V2 live-authority extension 2 are preserved.
- S1/S2 shadow action generation and frozen baseline selection; S3 research action path; S4/S5/S6/S7 research/context paths; S8 two-leg research basket outside the single-action plan.
- M0 chronological action-value baseline, offline M1 LightGBM challenger, causal analogue support, and bounded zero-authority Discovery Lab. No automatic promotion is made.
- `atlas-v2-projection`, a separate read-only process for existing `ops.sqlite`; `atlas-desktop`, a PySide6/PyQtGraph observer; a short credential-free public qualification command; and the resumable public operations soak command.
- Linux x86_64 PyInstaller one-folder bundle with separate `atlas-desktop` and `atlas-v2-projection` processes. Windows packaging is `BLOCKED BY ENVIRONMENT` in this session.

The desktop connects only to the projection service. IPC is loopback-only and token-authenticated, uses protocol 2, canonical JSON with frames capped at 1,000,000 bytes (below 1 MiB), UUID request correlation, and a strict read-only command allowlist. The projection opens an existing local ops database with SQLite read-only/query-only settings. It does not open the V1 live-control journal. Neither process has an order API or capital/risk mutation command.

## Evidence and limits

| Gate | Session-025 state |
| --- | --- |
| Deterministic engineering regression | `TESTED` |
| Linux desktop package and offscreen launch | `TESTED` after build/smoke record in the release manifest |
| Windows desktop package/runtime | `BLOCKED BY ENVIRONMENT` |
| Projection service contract | `TESTED`; separate process, read-only database, loopback and authenticated IPC |
| Real 72-hour public soak | `BLOCKED BY ENVIRONMENT`; no completed continuous artifact supplied |
| Prospective shadow floor | `NOT ESTIMABLE`; no sufficient elapsed evidence |
| Genuine matured opportunities | 0 known at this checkpoint; fixtures do not count |
| Final holdout | `UNTOUCHED`, not viewed, population not yet assigned |
| Economics | `NOT ESTIMABLE` |
| Bybit authenticated testnet | `UNVERIFIED / TEST GATE` |
| Binance authenticated testnet | `UNVERIFIED / TEST GATE` |
| Binance protection | `TEST GATE` |
| External writer fencing | `BLOCKED BY ENVIRONMENT` |
| Capital | disabled |

Public exchange documentation, public metadata, fixtures, and installed Nautilus source/API are not authenticated account qualification. No testnet credential or mainnet order was used for this release.

## Linux build and launch

Build on the target Linux architecture with the locked Python environment:

```bash
uv venv --python 3.12.13
uv pip sync --python .venv/bin/python requirements-lock.txt
uv pip install --python .venv/bin/python --no-deps -e .
.venv/bin/pyinstaller --noconfirm --clean atlas-desktop.spec
```

The tested build target is a Linux x86_64 one-folder bundle at `dist/atlas-desktop/`, containing separate desktop and projection-service executables. Its immutable file-set hash, command, Python, PyInstaller, OS and architecture are in `SESSION025_RELEASE_MANIFEST.json`. Do not copy or bundle `.env`, credentials, account identifiers, local databases, logs, or soak files into the release directory.

Start the bundled projection service and desktop as separate processes:

```bash
dist/atlas-desktop/atlas-v2-projection --db /path/to/ops.sqlite --archive-root /path/to/public-archive --token-file /path/to/private/atlas-ipc.token --host 127.0.0.1 --port 0
dist/atlas-desktop/atlas-desktop --host 127.0.0.1 --port PORT --token-file /path/to/private/atlas-ipc.token
```

The projection service prints the selected port. The service token is generated locally if absent and must stay private. The public qualification and soak utilities remain package entry points and do not require private credentials:

```bash
.venv/bin/atlas-v2-public-qualification --output evidence/public-qualification.json
.venv/bin/atlas-v2-public-soak --evidence evidence/public-soak.jsonl --duration-hours 72 --interval-seconds 60
```

The qualification command is a short public transport/translation check using a temporary store. It is not the continuous soak and does not supply authenticated venue or prospective economic evidence.

## Windows build instructions

Windows was not available for this session, so these commands are reproducible instructions and are not a tested platform claim. Run them on a native Windows 10/11 x64 host with Python 3.12.13 and uv installed:

```powershell
uv venv --python 3.12.13
uv pip sync --python .venv\Scripts\python.exe requirements-lock.txt
uv pip install --python .venv\Scripts\python.exe --no-deps -e .
.venv\Scripts\pyinstaller.exe --noconfirm --clean atlas-desktop.spec
```

Expected bundle paths: `dist\atlas-desktop\atlas-desktop.exe` and `dist\atlas-desktop\atlas-v2-projection.exe`. Run the projection executable as a separate process, bound to `127.0.0.1`, then launch the desktop executable with `--host`, `--port`, and `--token-file`. Verify the Qt window, IPC snapshot, service-unavailable behavior, token handling, package contents, and reconnect behavior on Windows before marking that platform tested. Do not cross-compile or infer Windows support from the Linux bundle.

## Operational sequence

1. Start the deployment’s single `atlas-ops` writer and confirm it uses the local `ops.sqlite`; this source tree exposes the collector/scanner/coordinator APIs but does not include a general scheduled ops-daemon command. This candidate also has no `atlas-worker` daemon command; M1 is offline research code.
2. Start `atlas-v2-projection` against that initialized database and optional public archive.
3. Start `atlas-desktop` with the same loopback port and private token file.
4. Confirm diagnostics show the service/package versions and `SHADOW_RELEASED`; inspect source freshness and explicit unavailable, `UNVERIFIED`, `TEST GATE`, and `NOT ESTIMABLE` states.
5. Keep all private exchange configuration outside the desktop, projection, and worker processes. Capital remains off.
6. Stop the desktop, projection, then ops writer cleanly. Back up `ops.sqlite` and the Parquet archive together using the documented consistent backup procedure.

## Reproducibility and validation

The machine-readable release manifest binds the tested source commit, dependency-lock hash, Nautilus package/source/artifact identity, V1 golden, all frozen research/policy identities, schema versions, Phase-2/3/4 references, capital/economic/venue states, build target and artifact hash. The Phase-5 gate separates local engineering validation from external/economic qualification. `SESSION025_VALIDATION.json` records the actual commands, test totals, skips, static checks, package/security scan results, and environment blockers. No GitHub Actions or other CI workflow result is claimed.
