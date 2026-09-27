# ATLAS V2 — Session-025 handoff

## Checkpoint

- Starting branch: `impl/session-024-v2-venue-qualification-capital-bridge-hardening`
- Starting SHA: `f7e0cb7de3f7cfcdee51c4d42842f6d06094467f`
- Work branch: `impl/session-025-v2-release-engineering-shadow-closure`
- Release source SHA bound by the manifest: `1bb7ec4ac0dc0af34b1fdebaea9912e1bf3ddab5`
- Final closeout classification: **SHADOW_RELEASED**
- Capital: **disabled** (`capital_enabled=false`, `assisted_enabled=false`). No Session-026 was created.

The release is a research/shadow desktop candidate. The repository has collector/scanner/coordinator APIs but does not ship a general scheduled `atlas-ops` daemon or an `atlas-worker` daemon command. A deployment must provide its single ops writer to populate research rows; the packaged desktop and `atlas-v2-projection` are read-only. The short public qualification command is not a continuous scanner or soak.

## Immutable release artifacts

- Release manifest: `docs/v2/SESSION025_RELEASE_MANIFEST.json`
- Release manifest canonical content hash: `7ff2fb153c4f0d57350c3e611e71e660b8e91fac940fd75c69bd728ee4f6f420`
- Release manifest file SHA-256: `9f26f4f73c14f5e70cee3c47df28b8e68f2cc05988b8bce8a9c49ce580a90e69`
- Phase-5 gate: `docs/v2/PHASE5_RELEASE_GATE.json`
- Phase-5 gate canonical content hash: `7fd6d97e20faf886de8547513587e676afc0a137802d87258ba1dc32a585a691`
- Phase-5 gate file SHA-256: `fe47c1f3d284c622e6f63a2b0a1eb5ac6faa8babf27ccabd4cce8975c93aa32e`
- Validation: `docs/v2/SESSION025_VALIDATION.json`
- Validation canonical content hash: `d29f1a75a61165eeff24d023e77a740ac9fc7c461c7d1f6e1ba1ebfe13934e24`
- Validation file SHA-256: `730459019f03ac48ee2a3500c738cd1884ea9917ad06c90c21952cd56d549228`
- V1 golden file SHA-256: `be2a54d2bf9a3a168fe850877d51ef241d8ae8cdad837b872270cc3a30166251`

## Engineering validation

- Complete repository regression: **887 passed, 3 skipped, 0 failed**. The skips are the opt-in Bybit and Binance authenticated testnet qualification cases and opt-in public testnet connectivity check.
- Focused release/golden/desktop/recovery/protection/authority regression: **103 passed, 1 skipped, 0 failed**.
- The full repository run covers complete V1, complete V2, Sessions 020–024, migration, runtime, fault-injection, desktop/IPC, and soak engineering tests. The V1 golden recomputation passed.
- Ruff, compileall, mypy (**320 source files**), `pip check`, and hash-locked offline dependency validation: **PASS**.
- `pip-audit` of `requirements-lock.txt`: **no known vulnerabilities**. Dependency lock SHA-256: `711c2abda6c2152b3acf98ba151bf62bf7e13ab6a4259e5c99034d2fa3abba2b`; dependency versions were not changed.
- Preserved policy identities recomputed: **16 hashes matched**, with the evidence matrix and holdout identities also checked by the full regression. V1 authority schema remains **6**; V2 live-authority extension remains **2**; `V2_CAPITAL_AUTHORITY_ATTESTATION_V1` hash remains `94d59964c503a88a13d1fd05d7f02554153e7bb74ac73e7dc4cb93c4a8b645da`.
- Repository and built-package secret/config scans found no credential values, private-key payloads, `.env` files, sensitive config files, or exchange credentials. The two PEM parser marker strings are from bundled Qt TLS/OpenSSL code, not key material. No GitHub CI run is claimed.

## Packaging and operations

- Linux x86_64 PyInstaller one-folder package: **TESTED**. It includes separate `atlas-desktop` and `atlas-v2-projection` executables.
- Bundle tree SHA-256: `a9c933ea224dc5bdd73d7227d33689e900ca3dfbaed57b6e0a4cc1a6affc5517` (1190 files; 788170494 logical bytes). Build command: `.venv/bin/pyinstaller --noconfirm --clean atlas-desktop.spec`.
- Packaged offscreen connection smoke passed: authenticated loopback snapshot, separate process lifetime, clean reconnect state with service absent, unchanged `ops.sqlite`, and no exchange credentials passed.
- Windows package/runtime: **BLOCKED BY ENVIRONMENT**; not exercised on Windows.
- Real 72-hour public operations soak: **BLOCKED BY ENVIRONMENT**; no genuine completed continuous artifact was supplied.
- Local deterministic recovery engineering: **TESTED**; direct fault harness passed **17/17** scenarios, plus the 103-test focused recovery/protection/authority group. Authenticated exchange recovery drill: **BLOCKED BY ENVIRONMENT**. No separate worker daemon is shipped; worker health stays `UNAVAILABLE / NOT_REPORTED`.

## Research and external gates

- Final holdout identity `aa7e691c14325ec1018df74037d48e8e0ce93de5755decb00d425b1549ede5f9`: population **UNASSIGNED / UNTOUCHED**, not viewed. No final-holdout evidence was consumed.
- Discovery promotion is `INTEGRATED`, not `DECISION_ELIGIBLE`. No automatic promotion occurred.
- Prospective shadow evidence: **NOT ESTIMABLE**. Genuine matured candidate opportunities known: **0**. Economics: **NOT ESTIMABLE**; no profitability claim is made.
- Bybit authenticated testnet: **UNVERIFIED / TEST GATE**.
- Binance authenticated testnet: **UNVERIFIED / TEST GATE**.
- Binance protection: **TEST GATE**.
- External cross-host writer fencing: **BLOCKED BY ENVIRONMENT**.
- Public documentation, exchange metadata, and installed adapter evidence are not authenticated account evidence. Current account fees, filters, margin, and native protection remain unqualified. No authenticated exchange requests or mainnet orders were used.

## Preserved identities

- S1: `c559659ace0239200f7d26d81a24b489a8a4ee0bc849b6954faf901126b5dff0`
- S2: `fbcdacd8ec6a79ea2595fa367d220b55b1d84c326ec3a28d787d23f356062dcd`
- S3: `b6a6ef283a5ca0b4dcbcb730b03adff92fb62c76c55fc66ef9268c906d8c62b6`
- S4_FEATURE: `327c816792290ceaf64ef6dd6b6e90e0382c04782e1dc7d9ca5895ce7f2ae146`
- S4_ABSORPTION: `fcd2e5b0f3643de92df8fb73e16ca924b3570b92285cc72eebb76ac03c964614`
- S4_EXECUTION_CONTEXT: `dae5c5519eb2d5c204607ca8e81f06fdc9b32ed07f69b6460b3fc6d46ccf995a`
- S5_CONTINUATION: `47c48f9d347957a3175523f7753531dbba5098c1c0cde7301200d4bb813f2fd2`
- S5_REVERSAL: `6e11389f7c06406acb8a8207d057f8b17edc56e4a9e619322fb64f5b083cf8c8`
- S5_CROWDING_CONTEXT: `8b29caee27d91d4ecc13b51cd605d13d83bbfca22de0789925adbef1a07e23d7`
- S6: `a2497ebad6307bd7c44155599b317119ba6cbfb4ee60da49226974ea23adffd3`
- S1_S2_SELECTOR: `36f8fd58c9e791ea580a1855f9e98555131e0828604d484eda3df4c7d5529cac`
- EVIDENCE_MATRIX: `294b47506e8a7b2a73275a70e13494acd37c4f1216df53e422f0ad863f848b81`
- M0_BASELINE_POLICY_REF: `102681d12c5c592b9a258f7a453e9fa441f649e952772afb9a9df5bb9673a3ff`
- M1_POLICY: `7ec4703f20ec9e3e2bf58c5a785f1ef179a22c3f8382b1c059679ee6306bc57f`
- M1_FEATURES: `b608c93873df82168c4d922b5d18b976efdc94a07c84159d75d1258aaa428f28`
- MULTI_SLEEVE_RESEARCH_SELECTOR: `37355c64d45a67bdce3a271a63db377b953c05847561bcda33847c27cecb0dac`
- S8_PROFILE: `c118b1e3f6732cf8897efc4c08a2d21eef50cec63b78d5bb40787f9fa5b0f7fe`
- ANALOGUE: `9b6312e42b28649877ce3f62b593b14cc769572987053abeca5b55f28b6934bf`
- DISCOVERY: `846fd322e5d0a8d91a4ae659771f7e37f26bcff384508a5895f154a93b0cfac3`

Other preserved release identities:

- Analogue: `9b6312e42b28649877ce3f62b593b14cc769572987053abeca5b55f28b6934bf`
- Discovery: `846fd322e5d0a8d91a4ae659771f7e37f26bcff384508a5895f154a93b0cfac3`
- Holdout population: `aa7e691c14325ec1018df74037d48e8e0ce93de5755decb00d425b1549ede5f9`
- Accepted Session-024 capital-authority contract hash: `94d59964c503a88a13d1fd05d7f02554153e7bb74ac73e7dc4cb93c4a8b645da`

## Operational work after engineering

1. Deploy one `atlas-ops` writer host that drives the existing public collector/scanner/coordinator APIs into `ops.sqlite`; keep private exchange credentials out of this process.
2. Start the bundled projection service against that initialized DB with a private mode-0600 token file, then start the desktop on the printed loopback port.
3. Run and validate a genuine uninterrupted 72-hour public-data soak using the documented resume procedure. Preserve run ID, continuity, samples, source health, and sanitized evidence.
4. Accumulate at least eight weeks of prospective shadow evidence and at least 200 genuinely matured candidate opportunities with adequate regime coverage and dependence-aware interpretation. Keep the holdout untouched.
5. Separately qualify exact Bybit and Binance testnet account/profile capabilities, native protection and recovery, account-specific fees/filters/margin, and external writer fencing with redacted evidence. Do not infer qualification from public docs or mocks.
6. Keep capital and assisted modes disabled until a later authorized evidence gate explicitly approves them; do not create another numbered engineering session just for calendar/environment evidence.

The manifest-bound release code SHA is `1bb7ec4ac0dc0af34b1fdebaea9912e1bf3ddab5`. The final documentation commit and remote branch tip are reported in the closeout response after push verification.
