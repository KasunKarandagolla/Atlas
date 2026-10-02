# Native Windows product build

The target laptop installs one versioned Inno Setup artifact. Python 3.12.10,
Qt, Arrow, DuckDB, local diagnostic ML dependencies and the pinned provider SDK
are frozen into a PyInstaller directory inside that artifact. Python, Git,
Inno Setup and pip are build-host tools; they are not target prerequisites.

NautilusTrader is excluded from this public research product. Its accepted
Linux wheel identity and V1 capital contracts remain unchanged. Authenticated
execution is disabled and has a separate qualification boundary.

## Clean build

Use the manually dispatched `Windows product build and offline qualification`
workflow on the exact committed branch when available. The exact S36 branch
also runs an unsigned diagnostic build on push, enabling native validation
without modifying or merging the default or accepted branches. It pins Python 3.12.10, the hash-locked
Windows wheel graph, the PyInstaller version and Inno Setup 6.5.4. The official
[immutable compiler release](https://github.com/jrsoftware/issrc/releases/tag/is-6_5_4)
installer SHA256 is
`fa73bf47a4da250d185d07561c2bfda387e5e20db77e4570004cf6a133cc10b1`.
The compiler artifact was downloaded and hashed during S36; it was not executed
in Linux.

Python 3.12.10 is the final CPython 3.12 release with official Windows binary
installers. Later 3.12 security releases are source only, so a 3.12.13
`setup-python` Windows pin cannot satisfy this build. This Windows interpreter
identity is recorded separately from the accepted Linux environment and locks.
See the official [3.12.10 release](https://www.python.org/downloads/release/python-31210/)
and [3.12.13 binary availability](https://www.python.org/downloads/release/python-31213/).

For a local native build, prepare the same Python interpreter on the build
machine, create an isolated environment outside the checkout, install
`requirements-windows-lock.txt` using `--no-deps --only-binary=:all:
--require-hashes`, and run:

```powershell
python scripts/windows_build.py --iscc C:\BuildTools\Inno-6.5.4\ISCC.exe --installer-version 2.0.36.0
```

The checkout must be clean and committed. `--signed-release` requires the
protected build-only `ATLAS_SIGNING_PFX_BASE64` and `ATLAS_SIGNING_PASSWORD`
environment variables. The certificate stays in ephemeral memory; it is never
included in the payload. Signed release signs the application, installer and
uninstaller and verifies Authenticode. Unsigned diagnostic builds are identified
as unsigned in the release manifest. They are not the signed owner artifact.

## Bundle and identity

`packaging/windows/resources.json` is the explicit resource allowlist; JSON run
profiles under `configs/v2` are also included. Existing freeze/pricing and lock
resources preserve their original bytes. Secrets, evidence, development tools,
tests and owner-local documents are excluded.

`build-manifest.json` sits beside `atlas-product.exe`. It binds source SHA,
package version, dependency lock hashes, installed build dependency versions,
every payload path/size/hash and the payload tree hash. The external release
manifest binds the installer hash and signing state. The build validates Qt's
Windows plugin, native Arrow/DuckDB/LightGBM binaries, Python DLL, required
resources and PE dependency closure. Missing Visual C++/OpenMP runtime DLLs
fail the build instead of silently assuming the laptop has development tools.

The Windows runtime has its own lock identity. The accepted Linux core lock and
agent lock retain their bytes. Their shared `typing-extensions` pins differ:
core 4.15.0 versus agent SDK 4.16.0. The Windows composition explicitly selects
the existing SDK 4.16.0 pin and records that override in the build manifest.
It does not claim the installed Windows dependency graph equals the Linux core
graph. Portable checks reject undeclared pin differences.

Build inputs are reproducible and source-bound. Authenticode timestamps and
Windows compiler/environment details can change final bytes; byte-identical
signed artifacts are not claimed.

## Installation and preservation

The installer requires Windows 11 build 22000 or newer and x64 compatibility.
It runs per user under `%LOCALAPPDATA%\Programs\ATLAS`, creates a Start menu
launcher and requires no administrator privilege. Runtime data and protected
configuration belong outside the installation directory. Upgrade and uninstall
do not delete external user evidence/configuration/secrets. A signed uninstaller
is generated for signed releases. Upgrade replaces the complete `_internal`
dependency tree, so removed DLLs cannot survive as undeclared dependencies.

## Native gates

The workflow runs the packaged entry's offline `--diagnostics --json` and
`--smoke --data-root PATH` contracts with Python/Git removed from child PATH and
no provider/exchange credentials. It validates package immutability, authority
flags, source identity and the declared first-run configuration fixture. A disposable-user installer harness exercises install,
same-version reinstall and uninstall while preserving external evidence. A
separate `--upgrade-installer` can qualify a real cross-version upgrade.
The harness refuses to replace an existing owner installation and requires a
disposable Windows account. It also tests removal of an obsolete dependency.
Native broker smoke uses an authenticated current-user-only Windows named pipe
and a deterministic typed fake completion, checks bounded handlers and shutdown,
and calls no intelligence provider.

Native Windows execution is **BLOCKED BY ENVIRONMENT** in the Linux development
session until an actual workflow run produces evidence. Windows Server CI does
not qualify the actual fresh Windows 11 laptop, continuous live source behavior,
GUI interaction, DPAPI account recovery or a 48-hour run. Those remain explicit
Windows hardware/live gates. Historical Linux observer packaging is not native
Windows evidence.

The build is not a runtime cloud dependency. Once installed, ATLAS uses bundled
components and normal Windows APIs; public market operation requires internet
and no secret.

Implementation references: [PyInstaller spec files](https://pyinstaller.org/en/stable/spec-files.html),
[Inno application shutdown](https://jrsoftware.org/ishelp/topic_setup_closeapplications.htm),
and [Inno signing](https://jrsoftware.org/ishelp/topic_setup_signtool.htm).
