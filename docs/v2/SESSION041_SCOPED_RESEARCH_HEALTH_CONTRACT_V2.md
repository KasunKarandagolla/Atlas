# S41 scoped research source health, version 2

Authority: amended V2 public research breadth and missing-evidence rules.
Approval state: implementation interpretation of already authorized additive
research scope; no change to capital, risk, economic or protection authority.

## Old contract

The V1 indexed cycle and supervisor require all configured public sources to
be currently healthy before a decision can run. This remains the default
contract for legacy run configurations and authenticated execution.

## Additive contract

The broad V2 research port scopes a decision to its trigger source and the
actual source IDs named by its causal raw inputs. Every dependency requires
healthy evidence both at the immutable information cutoff and at evaluation.
Missing or future inputs and unavailable health fail closed. A scoped gate
does not change current global component health or historical run validity.

The sole writer persists `OpsDecisionSourceScopeV2`, binding the event ID,
cutoff, exact health references, source identities, observation time and result.
The normal receipt records the scoped result and the complete current source
states. The cycle continues to report any configured source failure.

## Safety and migration

The hook is used only by the broad public research port. It grants zero
authority and keeps capital disabled. There is no execution venue fallback.
Legacy configurations omit the hook and retain the original global gate.
Existing event and receipt schemas and hashes are unchanged; the scope is a
new immutable sidecar artifact. Later healthy receipts cannot backdate proof.

## Validation

`tests/v2/test_session041_source_scope.py` checks independent and dependent
venue outages, missing evidence, later reconnection, unchanged legacy gating,
and retention of overall component failure when scoped research proceeds.
