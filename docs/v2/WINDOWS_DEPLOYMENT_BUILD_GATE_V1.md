# Windows deployment/build gate V1

Status: **IMPLEMENTED / UNVERIFIED**. The self-contained Windows build and installer pipeline is implemented in `packaging/windows/`, `atlas-product.spec`, the pinned Windows lock, and `scripts/windows_*.py`.
Actual native Windows artifact execution: **BLOCKED BY ENVIRONMENT** in this Linux workspace. The GitHub Windows workflow is prepared but has not yet produced an artifact in this session.
No signed owner delivery is claimed: the signing certificate is not present. Unsigned CI output is diagnostic only.

The Windows PyInstaller directory bundle contains the public research product, Qt/Arrow/DuckDB dependencies, versioned resources and the isolated optional critic broker entrypoint. The separate wheel-only lock is hash-pinned and excludes developer/test/secrets material. The accepted Linux lock remains unchanged.

Required implementation:

- Build on Windows from an exact source SHA, using Python 3.12 and separate hash-locked, wheel-only Windows runtime/build dependencies. Resolve the scope of the frozen Nautilus capital dependency explicitly; public research packaging must not accidentally import a disabled capital runtime.
- Use an allowlisted PyInstaller directory bundle containing the real public ops runtime, projection, functional desktop, optional permitted broker and required versioned resources. Add a bundle-resource locator instead of source-tree parent assumptions.
- Produce one per-user installer/setup artifact with version, source SHA, lock hashes and checksummed payload manifest. Configure signing from the authorized signing identity; no signing key is supplied or inferred by this session.
- Install binaries below the user's normal application directory. Keep configuration, protected secret blobs, runs, SQLite/WAL, immutable archives, reports and logs below a separate application-data root. Upgrades and normal uninstall preserve research/configuration evidence. Evidence deletion requires an explicit separate operation.
- Provide first-run data-location/profile selection and optional provider setup. Public collection needs no secret. Non-secret configuration is canonical, immutable and hashable. API keys belong to a Windows protected secret backend and are loaded only by the broker; never place them in run/config/report/SQLite/log rows.
- Launch the actual credential-free public composition and exclusive ops writer before read-only projections. Preserve run identity across resume, record restart/recovery epochs and stop safely. A broken writer must remain visibly failed. Avoid automatic paid redispatch or hidden model fallback.
- The direct Windows critic broker uses authenticated named pipes, current-user ACL checks, bounded handlers and zero-authority responses. Heavy or tool-using untrusted model isolation remains an explicit **DEFERRED_BY_FREEZE / BLOCKED BY ENVIRONMENT** capability until a separately verified Windows sandbox is available; it is not silently treated as safe.

Native validation harness requirements:

1. Build on a clean Windows runner from the exact SHA and locks; reject source distributions and verify package/resource contents.
2. Install without Python/Git/WSL/compiler tools, launch, complete first-run fixture and prove diagnostics bind the correct source/lock/payload identities.
3. Run an offline fixture through the installed public research composition, check writer exclusion, read-only projections, stop/restart, evidence/report persistence and capital/assisted flags false.
4. Prove optional provider absence, timeout, invalid output and duplicate/lost completion fail closed without changing admission. Do not use provider or exchange credentials in CI fixtures.
5. Validate application-data paths, secret protection/ACLs, log exclusion, low-disk behavior, upgrade preservation and normal uninstall preservation.
6. Verify installer signature and manifest. Native Windows success must be recorded against the exact artifact; Linux static inspection cannot mark it TESTED.

Current evidence: portable manifest, wheel-lock, payload, resource, launcher and broker harness checks pass in Linux. Required actual Windows gates remain: clean build, installer install/upgrade/uninstall, first-run smoke, native protected-secret operation, native broker pipe/ACL behavior, and signature verification.
