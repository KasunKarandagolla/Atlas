# ATLAS RED R6 Gemma and agent results

## Transport change

R5 reported that a 60-second smoke deadline was paired with an approximately 15-second native HTTP cap; the one generation request timed out after 15.266 seconds. R6 adds `ATLAS_GEMINI_REQUEST_TIMEOUT_SECONDS`, defaults it to 60 seconds, bounds it to 0.1–120 seconds, and passes `min(configured_timeout, remaining_absolute_deadline)` to the request. The existing absolute-deadline completion check remains active. There is no automatic retry after an uncertain request.

Offline tests cover normal synthetic generation, deadline capping, configured timeout bounds, timeout and rate-limit normalization, model identity, response shape, output bounds, credential redaction, and zero-authority response validation. The R6 focused set passes 150/150 and the full suite passes 2,906/2,906 executable cases. These are fixture results.

## Actual provider status

- R6 Gemini API requests: **0**.
- Real Gemma generation and schema validation: **NO**.
- Complete genuine S47 research round: **NO**.
- Real tool-call capability: **UNVERIFIED**; no tools were offered to a live provider.
- R6 tokens, latency, request/response bytes, and provider cost: **NOT APPLICABLE; no request was sent**.
- R5's historical two-request attempt remains as prior evidence: the model listing succeeded, but generation timed out after one request and returned no response. R6 did not repeat it.
- Gemini host and API-key readiness are reported by Cloud, but that does not prove the project is configured for free/no-billing use. No billing evidence or qualifying no-billing permit was available.

The provider path remains advisory and zero-authority. The S47 comparison amendment remains **PROPOSED — NOT ACCEPTED**, effective identity `NONE`. A verified no-billing configuration and authorization are required before the bounded synthetic smoke; a genuinely COMPLETE production Brief is required before a production-shadow research round. User-selected Luna is recorded; exact runtime identity is not independently attested.
