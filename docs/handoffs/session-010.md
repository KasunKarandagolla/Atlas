# Session 010 — V1 Exit Audit & Transition Baseline

## Identity

- STARTING BRANCH: `impl/session-009-v1-phase0-6-stabilization`
- STARTING SHA: `146bae0a2f10e2f794cbed3441123071c55a2baf`
- FINAL BRANCH: `impl/session-010-v2-transition-audit`
- IMPLEMENTATION/AUDIT COMMIT SHA: single checkpoint commit; same commit as
  the final branch tip.
- FINAL SHA: the commit containing this handoff at the final branch tip; the
  exact SHA is reported in the Session-010 completion report.
- TITLE: V1 Exit Audit & Transition Baseline (mandatory V2 Phase 0 only)

The Session-009 remote ref was fetched and verified equal to the required SHA.
The remote branch inventory ended at Session-009; no approved later V2
checkpoint existed. `impl/session-001-foundation` remains the stale,
non-authoritative GitHub default branch.

## Authority used

1. `ATLAS_FINAL_IMPLEMENTATION_CLARIFICATION_AND_V1_FREEZE_COMPLETED.md`,
   tracked V1 authority, SHA-256
   `c13cad1ba2f3f8c250d55099017770f5144c6cfd71be4bee1e65c5f075806a5c`.
2. Supplied `ATLAS_V2_FINAL_INTRADAY_INTELLIGENCE_IMPLEMENTATION_FREEZE.md`,
   read from the invoking checkout, SHA-256
   `bae3e1a9e48aec64d1292e5bc791c2e87949ed33f4124b1f9807b589cc07a484`.
   This was an untracked supplied input at the required starting checkpoint.
3. Actual Session-009 repository state and `docs/handoffs/session-009.md`.

## Changes made

- Added `docs/v2/V1_EXIT_AUDIT.md` with the Phase 0–6 status matrix, frozen
  contract inventory, additive V2 seams, V1-specific assumption inventory,
  environment/test evidence, CI status, external gates, and Phase-0 verdict.
- Added deterministic `docs/v2/V1_GOLDEN_BASELINE.json` and the additive
  `tests/contract/test_v1_golden_baseline.py` regression guard.
- Updated the stale Session-005/Session-004 README description to identify the
  stabilized V1 baseline and Session-010 transition audit.
- Updated only the descriptive `pyproject.toml` project description.
- Corrected two clearly stale comments in `.env.example`; no credential
  variable was added or populated.

No file under `src/atlas/` and no pre-existing test was changed. No dependency,
version, Python requirement, production serialization/hash, strategy/risk
parameter, schema/migration, transition rule, execution behavior, or V1
capability state changed. No V2 Phase 1 or Phase 2 implementation was added.

## Environment and dependency evidence

- Python: CPython 3.12.13, ABI `cpython-312-x86_64-linux-gnu`, Clang 22.1.3.
- Platform: Linux Lite 6.6 based on Ubuntu 22.04.5; kernel
  `5.15.0-177-generic`; x86_64; glibc 2.35.
- Environment: isolated `.venv`, provisioned with `uv 0.11.8` using
  `uv venv --python /home/kasun/.local/bin/python3.12 .venv` and
  `uv pip sync --python .venv/bin/python requirements-lock.txt`.
- `requirements-lock.txt` SHA-256:
  `0d7b5cc6129aab127f07a1b5bce4b7794eec5c48db0f99451eba675031ca2b39`.
- NautilusTrader installed version: `2.0.0rc5`; candidate source commit
  `1b0a49d2792a9432a3aca3fcb617ce7a630d905e`.
- Lock/reference wheel SHA-256:
  `eab45fafd2312deda1236554c49a9798bfc76bc8465af864878e2f70189ebebe`.
  The downloaded wheel bytes were not retained, so raw wheel-byte rehash is
  `BLOCKED BY ENVIRONMENT`. Independently observed installed
  `_libnautilus.cpython-312-x86_64-linux-gnu.so` SHA-256:
  `a53dd5a24fe77f4c66169af84ee9c7e292018010922e3dce9a8b6c9f7f3b7021`.
- CI: no GitHub Actions, GitLab CI, CircleCI, Azure Pipelines, Jenkins, tox,
  nox, or Makefile CI configuration was present in the checked repository.

## Assumptions

- Reproduction evidence applies to the recorded Linux x86_64, CPython 3.12.13,
  locked-dependency environment. No CI runner or second platform was available
  for cross-environment comparison.
- The V2 freeze authority is the supplied document identified by its SHA-256
  above; it was not tracked at the required Session-009 starting commit.
- Offline tests establish code and fixture behavior only. They do not establish
  authenticated Bybit behavior, unattended public collection, live safety, or
  profitability.
- Phase-0 acceptance permits documented external capabilities to remain
  `UNVERIFIED` / `TEST GATE`; it does not enable assisted capital.

## Test and quality evidence

Pristine Session-009 result, before tracked edits, from the required source
commit and locked Python 3.12 environment:

```text
PYTHONPATH=src:. .venv/bin/python -m pytest -ra
413 passed, 1 skipped in 232.73s
SKIPPED tests/runtime/test_safe_runtime.py:398: opt-in public testnet check; no credentials, network not required

PYTHONPATH=src:. .venv/bin/python -m ruff check
PASS

PYTHONPATH=src:. .venv/bin/python -m mypy src tests
PASS; no issues in 174 source files

PYTHONPATH=src:. .venv/bin/python -m compileall -q src tests
PASS

git diff --check
clean
```

Final post-change result, after the complete bounded change set and the new
test's type assertion correction:

```text
PYTHONPATH=src:. .venv/bin/python -m pytest -ra
414 passed, 1 skipped in 195.13s
SKIPPED tests/runtime/test_safe_runtime.py:398: opt-in public testnet check; no credentials, network not required

PYTHONPATH=src:. .venv/bin/python -m pytest -q tests/contract/test_v1_golden_baseline.py
PASS (1 passed); run twice consecutively
V1_GOLDEN_BASELINE.json SHA-256 after both runs:
b6d47c1c7a2e416304dd57d9055599201c474c8bada600cf23c2cbc09f90e53c

PYTHONPATH=src:. .venv/bin/python -m ruff check
PASS

PYTHONPATH=src:. .venv/bin/python -m mypy src tests
PASS; no issues in 175 source files

PYTHONPATH=src:. .venv/bin/python -m compileall -q src tests
PASS

git diff --check
clean
```

## Golden baseline evidence

`docs/v2/V1_GOLDEN_BASELINE.json` identifies source commit
`146bae0a2f10e2f794cbed3441123071c55a2baf` and includes:

- capability contract:
  `10efccc833ce552a31d3eca071c7e309e44f2c8e114cca1c8bf4e9a319bfbb9d`;
- engineering-default RiskPolicy:
  `d96a7dac7c877369921dcf56fbf36bf303ea649dc531d87e0bf1d54abed2988f`;
- fixed-input Phase-4 model manifest:
  `e85a1f5f24ad6143f782d0a9dab5a56490c82aa36600c0fea028432bc2c8ab41`;
- representative existing TradePlan fixture:
  `b76c3de08ab4f6085014db9b36c26d67bc772ca11884e66f19e1557d10459412`;
- deterministic Phase-4 decision-calendar record:
  `a2dd5ab5088a4573f4aa4149e870e6ab53cdb2399649f8170c30c78bce5272ce`;
- audit-owned deterministic replay projection:
  `6385f72bee99b73ca37e3c4fb2746d194d61b2f0a40b7a9453b944f4f573774b`.

Existing V1 canonical hash methods are used for the V1 contracts. Only the
replay projection uses an audit-owned JSON hash. The two consecutive test runs
recomputed identical baseline-file and golden values.

## Secret scan

`gitleaks`, `trufflehog`, `detect-secrets`, and a repository-provided scanner
were checked for availability. None was available. The final staged diff and
tracked files were scanned with a fallback matching private-key PEM headers,
AWS access-key identifiers, and long values assigned to API key/secret, token,
or password names. `.env.example` empty placeholders were treated as names,
not secrets. Exact fallback command:

```sh
secret_pattern='-----BEGIN (RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----|AKIA[0-9A-Z]{16}|(ghp_[A-Za-z0-9]{36,}|xox[baprs]-[A-Za-z0-9-]{20,})|(api[_-]?(key|secret)|secret[_-]?key|access[_-]?token|refresh[_-]?token|password)[[:space:]]*[:=][[:space:]]*"?[A-Za-z0-9+/=_-]{20,}'
git grep -I -l -i -E -e "$secret_pattern" -- . >/dev/null
git diff HEAD --binary | rg -qi -e "$secret_pattern"
git ls-files --error-unmatch .env
```

The first two commands returned no matches (exit status 1, interpreted as
clean); `.env` is not tracked. No credential/private-key match was found.

## Status ledger

### `IMPLEMENTED`

- V1 Phase 0–6 contract, runtime, recovery/protection, causal data, science/risk,
  scanner/alert, and assisted-control-shell code at the Session-009 checkpoint.
- Additive V2 Phase-0 exit audit, golden baseline, and transition regression.
- Fail-closed capability/assisted-control gate; `assisted_enabled=false`.

### `TESTED`

- Pristine V1 offline suite and final complete offline suite as reported above.
- Golden-baseline regression twice consecutively with identical values.
- Ruff, mypy, compileall, and diff checks as reported above.
- Fail-closed offline checks do not validate any exchange behavior.

### `UNVERIFIED`

- All six authenticated Bybit capability states in
  `docs/capability/bybit-v1.yaml`.
- Actual unattended public Bybit collection and all live venue/account
  behavior.
- Venue-facing Nautilus/Bybit wire, margin/account, crash-recovery,
  single-writer, stop visibility/resizing, reduce-only enforcement, native-stop
  reconciliation, and protection repair behavior.

### `TEST GATE`

- Authenticated Bybit entry/protection/close/flatten and capability
  qualification; all six capability entries remain `UNVERIFIED`.
- Assisted capital remains disabled pending its pre-existing qualification
  gates and user-approved live RiskPolicy.

### `NOT ESTIMABLE`

- Economic/profitability validation. No profitability claim is made.

### `BLOCKED BY ENVIRONMENT`

- Independent raw Nautilus wheel-byte rehash, because the installed wheel file
  was not retained. Expected lock hash and installed extension hash remain
  separate evidence.
- Unattended public collection was not performed in this offline audit; the
  Session-009 handoff separately recorded it as `BLOCKED BY ENVIRONMENT` /
  `UNVERIFIED`.

## External behavior and Phase-0 verdict

No authenticated venue test, order submission, account action, or capital
dispatch occurred. `assisted_enabled=false`; six Bybit capability states remain
`UNVERIFIED` / `TEST GATE`; profitability remains `NOT ESTIMABLE`.

V2 Phase 0: **PASS**. The required source was verified, the pristine V1 suite
was reproduced, final offline gates pass, golden hashes are committed and
reproducible, frozen contracts and additive seams are documented, V1-specific
fixed assumptions are inventoried, descriptions are corrected, and no frozen
V1 production behavior was changed. No unresolved Phase-0 blocker remains.
