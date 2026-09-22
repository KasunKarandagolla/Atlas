"""Pinned Nautilus rc5 configuration boundary; no connection or order submission."""

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


@dataclass(frozen=True)
class OfflineBybitConfigs:
    data: Any
    execution: Any
    data_factory: Any
    execution_factory: Any


class ExecutionReportHandler(Protocol):
    def on_report(self, report: dict[str, Any]) -> None: ...


class ReconciliationHandler(Protocol):
    def on_reconcile(self, summary: dict[str, Any]) -> None: ...


def verify_installation() -> NautilusEvidence:
    try:
        import nautilus_trader

        v = getattr(nautilus_trader, "__version__", None)
        return NautilusEvidence(
            v == PINNED_VERSION,
            str(v) if v else None,
            "pinned version import-verified" if v == PINNED_VERSION else "version mismatch",
        )
    except ImportError as e:
        return NautilusEvidence(False, None, f"not installed: {e}")


@dataclass(frozen=True)
class LiveNodeConfig:
    environment: str = "testnet"
    venue: str = "BYBIT"
    product: str = "linear"
    testnet: bool = True
    position_mode: str = "one_way"
    account_mode: str = "isolated"
    symbols: tuple[str, ...] = REQUIRED_SYMBOLS

    def __post_init__(self):
        if (
            self.environment not in ("development", "testnet")
            or not self.testnet
            or self.venue != "BYBIT"
            or self.product != "linear"
            or self.position_mode != "one_way"
            or self.account_mode != "isolated"
            or tuple(self.symbols) != REQUIRED_SYMBOLS
        ):
            raise ValueError("config violates frozen Phase1/2 V1 profile")


def build_public_config(environment: str = "testnet") -> LiveNodeConfig:
    return LiveNodeConfig(environment=environment)


def build_offline_rc5_bybit_configs() -> OfflineBybitConfigs:
    from nautilus_trader.adapters.bybit import (
        BybitDataClientConfig,
        BybitDataClientFactory,
        BybitEnvironment,
        BybitExecutionClientConfig,
        BybitExecutionClientFactory,
        BybitMarginMode,
        BybitProductType,
    )

    products = [BybitProductType.LINEAR]
    env = BybitEnvironment.TESTNET
    return OfflineBybitConfigs(
        BybitDataClientConfig(product_types=products, environment=env),
        BybitExecutionClientConfig(
            product_types=products, environment=env, margin_mode=BybitMarginMode.ISOLATED_MARGIN
        ),
        BybitDataClientFactory(),
        BybitExecutionClientFactory(),
    )
