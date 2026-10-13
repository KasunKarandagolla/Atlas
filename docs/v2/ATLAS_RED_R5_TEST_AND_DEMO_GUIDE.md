# ATLAS RED R5 test and trusted Demo guide

This guide uses the pinned repository code. Neither script places an order or changes Atlas authority.

## Gemma smoke test

The smoke script uses the existing native `GeminiNativeClient`. It makes at most two HTTP requests: a model
identity lookup and one schema-configured synthetic generation request. It has no tools, retries, images, or
trading data. It prints sanitized capability and usage metadata, never the API key, prompt, or provider text.

Run it from a trusted environment with the configured Google AI Studio credential available through the
environment's secret manager. Confirm the account/project is eligible for no-charge use before setting the
free-tier marker:

```bash
export GEMMA_MODEL=gemma-4-31b-it
export ATLAS_GEMINI_FREE_TIER_VERIFIED=true
uv run --frozen python scripts/atlas_red_gemma_smoke.py
```

Provide `GEMINI_API_KEY` through the secret manager. Do not put the key in shell history, a file, a command
argument, or test output. The process-local marker is required by `GeminiNativeClient.from_environment`.
The request limit is two, with at most 4 KB of synthetic prompt, 256 output tokens, a 60-second deadline,
and a 64 KB response cap. A timeout is uncertain completion; inspect the printed failure code and do not
retry automatically. `pricing_verified` remains false unless independent pricing evidence establishes it.

The R5 cloud run used both allowed requests. The model listing confirmed `models/gemma-4-31b-it` and
`generateContent`; the generation request timed out. No retry was made. Structured output, provider usage,
and the provider-reported pricing state remain unverified. See the sanitized probe record in the R5 handoff.

## Trusted Binance Futures Demo read probe

Run `scripts/atlas_red_demo_read_probe.py` only on a trusted machine after a separate owner approval for
read-only authenticated Demo access. The script first checks the Demo exchange clock, then signs one
`GET /fapi/v3/account` request through Atlas's `DemoOnlyAdapter`. It has no order, cancel, replace, or
WebSocket command path. It prints only a response digest, byte count, and clock skew; it does not print or
persist account contents, credentials, or signatures.

Use a dedicated Binance Futures **Demo** credential supplied by the trusted machine's secret manager. Set:

```text
ATLAS_BINANCE_FUTURES_ENV=DEMO_ONLY
BINANCE_FUTURES_DEMO_API_KEY=<secret-manager injection>
BINANCE_FUTURES_DEMO_API_SECRET=<secret-manager injection>
ATLAS_BINANCE_DEMO_READ_ONLY_APPROVED=YES
```

The approval marker should be set only after the owner has approved this read. Never use a real-account,
testnet, live, or shared key. The trusted host needs direct HTTPS access to `demo-fapi.binance.com`; the
script rejects redirects and endpoint changes. It checks local clock skew against the Demo server and
fails closed if the difference exceeds two seconds. It does not retry an uncertain request.

Run it with the repository environment:

```bash
uv run --frozen python scripts/atlas_red_demo_read_probe.py
```

Do not run this script in Codex Cloud. The R5 Cloud network allowlist excludes Binance, and no Binance
credential was made available here. R5 did not perform signed Demo reads or submit any Demo order. The
current adapter is a pinned GET request builder; order lifecycle, exchange-side persistence, protection,
cancel/replace, and restart reconciliation remain unimplemented and are a release gate.

## Authority and evidence

Gemma output remains a `ZERO_AUTHORITY_SHADOW` proposal. The S47 comparative-selection amendment remains
`PROPOSED — NOT ACCEPTED`; effective policy identity is `NONE`. Atlas A0 sizing, risk and protection remain
independent. No live capital or assisted execution is enabled.
