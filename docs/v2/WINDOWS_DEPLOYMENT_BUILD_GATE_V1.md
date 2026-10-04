# Windows deployment/build gate V1

This document preserves S36 native evidence. Current S37 source-bound validation is in [SESSION037_OFFLINE_VALIDATION_V1.json](SESSION037_OFFLINE_VALIDATION_V1.json); owner instructions are in the [Windows research run guide](SESSION037_WINDOWS_RESEARCH_RUN_GUIDE.md).

Status: **IMPLEMENTED / TESTED (CI)**. The self-contained Windows build and installer pipeline is implemented in `packaging/windows/`, `atlas-product.spec`, the pinned Windows lock, and `scripts/windows_*.py`.
Native GitHub Windows Server 2025 execution: **TESTED** by workflow run `37085899018` against implementation SHA `e673b8a3166acd5cb66dd2d3599a916353fe74d9`. The run passed the PyInstaller build, payload/resource manifest, packaged offline report smoke, authenticated named-pipe/current-user ACL fixture, disposable DPAPI roundtrip/damaged-ciphertext rejection, retained controller process-instance/owner-death fixture, Inno installer, install, reinstall/upgrade preservation and uninstall preservation checks.
Actual clean owner Windows 11 execution remains **BLOCKED BY ENVIRONMENT** in this Linux workspace.
No signed owner delivery is claimed: the signing certificate is not present. Unsigned CI output is diagnostic only.

The Windows PyInstaller directory bundle contains the public research product, Qt/Arrow/DuckDB dependencies, versioned resources and the isolated optional critic broker entrypoint. The separate wheel-only lock is hash-pinned and excludes developer/test/secrets material. The accepted Linux lock remains unchanged.

Implemented architecture:

- Build on Windows from an exact source SHA, using pinned Python 3.12.10 and separate hash-locked, wheel-only Windows runtime/build dependencies. The public research bundle excludes the frozen Nautilus capital dependency and disabled capital imports; the accepted capital lock remains unchanged.
- Use an allowlisted PyInstaller directory bundle containing the real public ops runtime, projection, functional desktop, optional permitted broker and required versioned resources. The implemented bundle-resource locator resolves installed resources without source-tree parent assumptions.
- Produce one per-user installer/setup artifact with version, source SHA, lock hashes and checksummed payload manifest. Configure signing from the authorized signing identity; no signing key is supplied or inferred by this session.
- Install binaries below the user's normal application directory. Keep configuration, protected secret blobs, runs, SQLite/WAL, immutable archives, reports and logs below a separate application-data root. Upgrades and normal uninstall preserve research/configuration evidence. Evidence deletion requires an explicit separate operation.
- Provide first-run data-location/profile selection and optional provider setup. Public collection needs no secret. Non-secret configuration is canonical, immutable and hashable. API keys belong to a Windows protected secret backend and are loaded only by the broker; never place them in run/config/report/SQLite/log rows.
- Launch the actual credential-free public composition and exclusive ops writer before read-only projections. Preserve run identity across resume, record restart/recovery epochs and stop safely. A broken writer must remain visibly failed. Avoid automatic paid redispatch or hidden model fallback.
- The direct Windows critic broker uses authenticated named pipes, current-user ACL checks, bounded handlers and zero-authority responses. Its DPAPI epoch context v2 binds the controller PID and Windows process creation FILETIME. A retained process handle detects controller death and rejects PID reuse; the broker closes instead of surviving as an orphan. Pipe endpoint generation uses the exact transport grammar. The native disposable owner-death smoke passed at corrected build SHA e673b8a. Heavy or tool-using untrusted model isolation remains an explicit **DEFERRED_BY_FREEZE / BLOCKED BY ENVIRONMENT** capability until a separately verified Windows sandbox is available; it is not silently treated as safe.

Native validation harness requirements:

1. Build on a clean Windows runner from the exact SHA and locks; reject source distributions and verify package/resource contents.
2. Install without Python/Git/WSL/compiler tools, launch, complete first-run fixture and prove diagnostics bind the correct source/lock/payload identities.
3. Run an offline fixture through the installed public research composition, check writer exclusion, read-only projections, stop/restart, evidence/report persistence and capital/assisted flags false.
4. Prove optional provider absence, timeout, invalid output and duplicate/lost completion fail closed without changing admission. Do not use provider or exchange credentials in CI fixtures.
5. Validate application-data paths, secret protection/ACLs, log exclusion, low-disk behavior, upgrade preservation and normal uninstall preservation.
6. Verify installer signature and manifest. Native Windows success must be recorded against the exact artifact; Linux static inspection cannot mark it TESTED.

Current evidence: portable manifest, wheel-lock, payload, resource, launcher and broker harness checks pass in Linux; native GitHub Windows CI run `37085899018` passed the packaged product and installer checks. Required owner-machine gates remain: clean Windows 11 first-run operation, complete real-provider/broker integration, cross-version upgrade, signed release verification and live-public qualification.

Exact native CI artifact: `11260498264`, archive digest `sha256:1a5f6634dcdcdfef3cc61a1769139807a170cda85869ed13776a8c79e74144d0`. The broker/DPAPI fixture uses synthetic disposable material, rejects wrong authentication and damaged ciphertext, and does not read an owner key or call a provider. The CI host is Windows Server 2025; it is not the owner Windows 11 laptop.

Downloaded installer `ATLAS-2.0.36.0-e673b8a3166a-win11-x64-setup.exe` matches release SHA256 `131fdbc1f4224d07f8f8403b3babbbaa9f5800d9c261fd66e55c5b390fdc6dfd`; payload tree SHA256 `803858f4b81fec804cd752064487a7d42d5a3dfa5f6083a533f46198bcab9a6a`. Native dependency closure checked 278 PE files with no unbundled compiler runtimes. The downloaded binary was checksum-verified in Linux; it was executed by the native CI harness, not by Linux.

## Exact source bytes and historical defect

Independent downloaded-artifact review of historical run `37046927320` (`bf64ea0`) found Windows CRLF conversion changed all three deployed lock byte identities. Runtime/installer smoke passed, but that artifact remains `TEST GATE` for source-lock binding; its evidence is retained without relabeling. Builds `02741fc` and `e673b8a` add exact checkout-versus-Git-blob comparison, LF lock attributes and byte preservation for authority/resources. Portable faults reject CRLF and changed dependency content. The corrected native build above passes, and independent manifest comparison confirms:

- Core lock: `711c2abda6c2152b3acf98ba151bf62bf7e13ab6a4259e5c99034d2fa3abba2b`.
- Agent lock: `47184aa3a8ba6045e527d47f274093c4821157ba208996659329872e7892f4e3`.
- Windows lock: `984be9a8ae0454540a3a585e6a80effaf84083434a72b15bf1816781985404b5`.
- Bundled agent authority manifest: `d3e7b0b3da8a2a776db5dd05c656dbc5f04f3d8b1dfaaaec3fbd66966931682d`.

Source-byte validation runs only in the build environment; the installed laptop requires no Git or dependency tools. Native CI installed-product smoke removes Python/Git from the child PATH, but does not certify a clean owner laptop or the complete live pipeline. At the accepted S36 checkpoint, five ordinary software blockers remained. The current closure state is recorded in `ATLAS_FINAL_DEVELOPMENT_CLOSURE_V1.json`; historical successful packaging alone does not promote development readiness.

Machine-readable package/checksum/test evidence: `SESSION036_OFFLINE_VALIDATION_V1.json`. The artifact API archive digest is recorded as reported by GitHub; the downloaded installer and JSON manifests are independently checksummed.
