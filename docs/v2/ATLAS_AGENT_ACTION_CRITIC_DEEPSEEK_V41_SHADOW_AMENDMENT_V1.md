# ATLAS Agent Action Critic DeepSeek V4.1 Shadow Amendment V1

**Identity:** `ATLAS_AGENT_ACTION_CRITIC_DEEPSEEK_V41_SHADOW_AMENDMENT_V1`

**Scope:** Direct structured inference for the hidden, zero-authority action assessment task only.

This amendment adds a distinct `ActionAssessmentProvider` binding. Session 028's `ResearchProposalProvider` profile, proposal request and capability contracts, provider history, validation records, and gate remain unchanged. The Session 028 DeepSeek proposer authorization does not authorize this task.

The critic binding uses provider `deepseek`, requested alias `deepseek-flash`, family `DeepSeek-V4.1-Flash`, base URL `https://api.deepseek.com`, endpoint `/responses`, high reasoning, and strict JSON Schema output. Model revision remains `ALIAS_ONLY`; the alias is decision-ineligible and carries no qualification of a fixed model revision.

One direct structured assessment call is permitted per immutable sealed packet and request. The task has zero dynamic tools, 12,000 input tokens, 2,048 output tokens, one concurrent job, and a 36,000-byte sealed packet limit. No automatic retry or provider/model fallback is permitted. The existing DeepSeek V4.1 Flash price schedule is used conservatively and the maximum one-call cost is reserved before dispatch authorization.

The critic can report only the six closed semantic finding types defined by its versioned output schema. It cannot select candidates, change direction, quantity, entry, collar, stop, leverage, policy, deterministic admission, or create or invalidate a plan. It has no approval, reservation, execution, protection, or recovery authority. A valid result is shadow research evidence only and remains separate from capital and admission tables.

The provider credential remains in `atlas-agent-broker` under `DEEPSEEK_API_KEY`. The controller, SQLite records, request, packet, prompt, capability, and logs do not receive the credential. Provider reasoning content is discarded at the provider boundary. The action assessment uses the direct broker operation and never the tool-using research worker.

This amendment does not establish prospective shadow evidence, live provider conformance, decision influence, admission influence, or economic value. Direct live DeepSeek critic contract remains `UNVERIFIED / TEST GATE`; prospective hidden shadow and any future decision or admission influence remain `TEST GATE`.
