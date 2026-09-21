"""Safe runtime entrypoint: python -m atlas.runtime.

Default: testnet/development config, acquire writer, open journal, recover,
print/publish sanitized status, refuse new risk (qualifications incomplete),
never submit an order. Ctrl-C releases local resources cleanly.
"""

from __future__ import annotations

import argparse
import sys

from atlas.domain.time import now_ns

from . import coordinator as _coordinator
from .connectivity import PrivateVerification, PublicState, PublicVenueHealth
from .prerequisites import IdentityExpectation, ObservedAccountState
from .status import RuntimeStatus, publish_status
from .writer_lock import WriterLock


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="ATLAS Phase 1 safe runtime (no orders)")
    parser.add_argument("--journal", default="./atlas-journal.db")
    parser.add_argument("--lock", default="./atlas-writer.lock")
    parser.add_argument("--status", default="./atlas-status.json")
    args = parser.parse_args(argv)
    now = now_ns()
    writer = WriterLock(args.lock)
    try:
        result = _coordinator.boot(
            journal_path=args.journal,
            lock_path=args.lock,
            capability_hash="unverified",
            all_qualified=False,
            assisted_enabled=False,
            identity_expected=IdentityExpectation(),
            identity_observed=ObservedAccountState(
                environment="testnet",
                venue="BYBIT",
                product="linear",
                position_mode="one_way",
                margin_profile="isolated",
                account_identity_hash="CONFIGURED",
                instruments_with_metadata=("BTCUSDT", "ETHUSDT"),
                private_verified=False,
            ),
            public_health=PublicVenueHealth(state=PublicState.DISCONNECTED),
            private_verification=PrivateVerification(),
            now_ns=now,
            max_public_staleness_ns=5_000_000_000,
            writer=writer,
        )
    except Exception as exc:
        print(f"BOOT FAILED (fail closed): {exc}", file=sys.stderr)
        try:
            writer.release()
        except Exception:
            pass
        return 1
    status = RuntimeStatus(
        runtime_state=result.state.value,
        writer_epoch=result.certificate.writer_epoch,
        journal_healthy=True,
        reconciliation_health=result.certificate.reconciliation_health.value,
        unresolved_intents=result.unresolved_intents,
        unknown_commands=result.unknown_commands,
        capability_hash="unverified",
        all_qualified=False,
        assisted_enabled=False,
    )
    try:
        publish_status(args.status, status)
    except Exception as exc:
        print(f"STATUS PUBLISH FAILED: {exc}", file=sys.stderr)
    print(
        f"state={result.state.value} new_risk_allowed={result.new_risk_allowed} "
        f"unresolved={result.unresolved_intents} unknown={result.unknown_commands} "
        f"decision={result.certificate.decision.value}"
    )
    for reason in result.reasons:
        print(f"gate: {reason}")
    print("NO ORDERS SUBMITTED (Phase 1 safe runtime)")
    try:
        writer.release()
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
