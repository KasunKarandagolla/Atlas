"""Narrow Nautilus integration boundary (no orders).

Allowed: import/version verification, configuration construction, environment
selection, public/testnet connection config, account/client identity plumbing,
event-callback interfaces, execution-report/reconciliation interfaces.

Forbidden: real order submission, fake acknowledgements, marking wire
capabilities SUPPORTED without qualification, CCXT bypass, custom OMS.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

PINNED_VERSION = "2.0.0rc5"
PINNED_COMMIT = "1b0a49d2792a9432a3aca3fcb617ce7a630d905e"
PINNED_DISTRIBUTION = "nautilus_trader"
PINNED_PRODUCT = "linear"
PINNED_POSITION_MODE = "one_way"
REQUIRED_SYMBOLS = ("BTCUSDT", "ETHUSDT")


@dataclass(frozen=True)
class NautilusEvidence:
    installed: bool
    version: str | None
    detail: str


def verify_installation() -> NautilusEvidence:
    try:
        import nautilus_trader  # type: ignore[import-not-found]

        ver = getattr(nautilus_trader, "__version__", None)
        if ver == PINNED_VERSION:
            return NautilusEvidence(True, str(ver), "pinned version import-verified")
        return NautilusEvidence(
            False, str(ver) if ver else None, f"version {ver!r} != pinned {PINNED_VERSION}"
        )
    except ImportError as exc:
        return NautilusEvidence(False, None, f"not installed: {exc}")


@dataclass(frozen=True)
class LiveNodeConfig:
    environment: str
    venue: str = "BYBIT"
    product: str = "linear"
    testnet: bool = True
    position_mode: str = "one_way"
    account_mode: str = "isolated"
    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT")

    def __post_init__(self) -> None:
        # Phase 1/2: testnet only - reject mainnet/live
        if self.environment not in ("development", "testnet"):
            raise ValueError("Phase 1/2 node config allows development/testnet only")
        if self.venue != "BYBIT" or self.product != "linear":
            raise ValueError("Phase 1/2 venue/product fixed to BYBIT/linear")
        if not isinstance(self.testnet, bool):
            raise ValueError("testnet must be bool")
        if not self.testnet:
            raise ValueError("testnet=False not allowed in Phase 1/2 qualification")
        # Enforce frozen V1 contract identity
        if self.position_mode != PINNED_POSITION_MODE:
            raise ValueError(f"position_mode must be {PINNED_POSITION_MODE}, got {self.position_mode!r}")
        if self.account_mode != "isolated":
            raise ValueError(f"account_mode must be isolated, got {self.account_mode!r}")
        if set(self.symbols) != set(REQUIRED_SYMBOLS):
            raise ValueError(f"symbols must match frozen V1 scope {REQUIRED_SYMBOLS}, got {self.symbols}")


class ExecutionReportHandler(Protocol):
    def on_report(self, report: dict[str, Any]) -> None: ...


class ReconciliationHandler(Protocol):
    def on_reconcile(self, summary: dict[str, Any]) -> None: ...


def build_public_config(environment: str = "testnet") -> LiveNodeConfig:
    return LiveNodeConfig(environment=environment, testnet=True)
