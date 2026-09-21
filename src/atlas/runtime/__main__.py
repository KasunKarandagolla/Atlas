"""Safe runtime entrypoint: python -m atlas.runtime.

Default: testnet/development config, acquire writer, open journal, recover,
print/publish sanitized status, refuse new risk (qualifications incomplete),
never submit an order. Ctrl-C releases local resources cleanly.

Long-lived mode: --run-forever keeps process alive with periodic health checks.
"""

from __future__ import annotations

import argparse
import sys

from atlas.domain.capability import initial_unverified_fixture

from .connectivity import PrivateVerification, PublicState, PublicVenueHealth
from .prerequisites import IdentityExpectation, ObservedAccountState
from .safe_runtime import SafeRuntime, SafeRuntimeConfig


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="ATLAS Phase 1 safe runtime (no orders)")
    parser.add_argument("--journal", default="./atlas-journal.db")
    parser.add_argument("--lock", default="./atlas-writer.lock")
    parser.add_argument("--status", default="./atlas-status.json")
    parser.add_argument("--run-forever", action="store_true", help="Run long-lived safe runtime")
    parser.add_argument("--tick-interval", type=int, default=10, help="Tick interval in seconds")
    args = parser.parse_args(argv)

    # Build configuration
    capability_contract = initial_unverified_fixture()
    config = SafeRuntimeConfig(
        journal_path=args.journal,
        lock_path=args.lock,
        status_path=args.status,
        capability_hash=capability_contract.contract_hash(),
        all_qualified=False,
        assisted_enabled=False,
        capability_contract=capability_contract,
        identity_expected=IdentityExpectation(expected_account_identity_hash="REQUIRED"),
        identity_observed=ObservedAccountState(
            environment="testnet",
            venue="BYBIT",
            product="linear",
            position_mode="one_way",
            margin_profile="isolated",
            account_identity_hash="REQUIRED",
            instruments_with_metadata=("BTCUSDT", "ETHUSDT"),
            private_verified=False,
        ),
        public_health=PublicVenueHealth(state=PublicState.DISCONNECTED),
        private_verification=PrivateVerification(),
        max_public_staleness_ns=5_000_000_000,
        clock_uncertainty_ns=0,
        tick_interval_ns=args.tick_interval * 1_000_000_000,
    )

    runtime = SafeRuntime(config)

    try:
        if args.run_forever:
            result = runtime.start()
            print(
                f"state={result.state.value} new_risk_allowed={result.new_risk_allowed} "
                f"unresolved={result.unresolved_intents} unknown={result.unknown_commands} "
                f"unresolved_commands={result.unresolved_commands} "
                f"decision={result.certificate.decision.value}"
            )
            for reason in result.reasons:
                print(f"gate: {reason}")
            print("NO ORDERS SUBMITTED (Phase 1/2 safe runtime)")
            print("Running long-lived safe runtime (Ctrl-C to stop)...")
            runtime.run_forever()
            print("Shutdown complete.")
            return 0
        else:
            # Single-shot mode (original behavior)
            result = runtime.start()
            print(
                f"state={result.state.value} new_risk_allowed={result.new_risk_allowed} "
                f"unresolved={result.unresolved_intents} unknown={result.unknown_commands} "
                f"unresolved_commands={result.unresolved_commands} "
                f"decision={result.certificate.decision.value}"
            )
            for reason in result.reasons:
                print(f"gate: {reason}")
            print("NO ORDERS SUBMITTED (Phase 1/2 safe runtime)")
            runtime.shutdown()
            return 0
    except Exception as exc:
        print(f"BOOT FAILED (fail closed): {exc}", file=sys.stderr)
        try:
            runtime.shutdown()
        except Exception:
            pass
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
