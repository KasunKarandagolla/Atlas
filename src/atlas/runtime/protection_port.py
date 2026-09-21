"""Narrow Bybit protection port (freeze §1.2, §1.3, §9.7).

Domain methods ONLY:
- read_protection: inspect raw position/conditional protection fields
- ensure_full_stop: set/repair full-position native stop
- read_economic_events: fetch immutable cash events (funding, fees, transfers)

Forbidden through this port:
- entry submission
- discretionary exit orders
- ordinary cancel/replace
- position book management

Production implementation may remain TEST GATE if authenticated binding unavailable.
Deterministic fake lives in tests/support only.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal

from atlas.domain.execution import EconomicEvent, ProtectionObservation


@dataclass(frozen=True)
class ProtectionPortConfig:
    """Configuration for the protection port."""
    environment: str = "testnet"
    venue: str = "BYBIT"
    product: str = "linear"
    position_mode: str = "one_way"
    account_mode: str = "isolated"


class BybitProtectionPort(ABC):
    """Narrow port for Bybit position-level stop repair and raw protection inspection.

    This port NEVER handles:
    - Entry order submission
    - Discretionary exit orders
    - Ordinary cancel/replace
    - Position bookkeeping (Nautilus owns that)

    It ONLY handles:
    - Full-position stop inspection (read_protection)
    - Full-position stop repair (ensure_full_stop)
    - Economic event fetching (read_economic_events)
    """

    @abstractmethod
    def read_protection(
        self,
        account: str,
        instrument: str,
        position_idx: int = 0,
    ) -> ProtectionObservation:
        """Read current native protection state for a position.

        Returns ProtectionObservation with:
        - position_epoch, desired_stop_version
        - observed signed quantity, trigger basis, stop price
        - full-position semantics, evidence IDs
        - observation timestamp

        Raises if account/instrument not found or venue unreachable.
        """
        ...

    @abstractmethod
    def ensure_full_stop(
        self,
        position_epoch: int,
        expected_signed_qty: Decimal,
        stop_price: Decimal,
        trigger_basis: str,
    ) -> ProtectionObservation:
        """Ensure full-position native stop is set correctly.

        If stop exists and matches expected_signed_qty/stop_price/trigger_basis,
        returns current observation.
        If missing or mismatched, attempts to set/repair via venue API.
        Returns post-repair observation (may still be unconfirmed if venue async).

        Raises on venue error or semantic mismatch (e.g., wrong position side).
        """
        ...

    @abstractmethod
    def read_economic_events(
        self,
        cursor: str,
        overlap_start_ns: int,
    ) -> tuple[tuple[EconomicEvent, ...], str]:
        """Read immutable economic events (funding, fees, transfers) from venue.

        Args:
            cursor: Pagination cursor from last successful read
            overlap_start_ns: Start of overlap window to catch late-arriving events

        Returns:
            Tuple of (events, new_cursor). Events are deduplicated by
            (account, venue_transaction_id) composite key.
        """
        ...


class BybitProtectionPortUnavailable(BybitProtectionPort):
    """TEST GATE implementation - raises NotImplementedError for all methods.

    Used when authenticated venue binding is not available.
    """

    def read_protection(
        self,
        account: str,
        instrument: str,
        position_idx: int = 0,
    ) -> ProtectionObservation:
        raise NotImplementedError("TEST GATE: authenticated venue binding required for read_protection")

    def ensure_full_stop(
        self,
        position_epoch: int,
        expected_signed_qty: Decimal,
        stop_price: Decimal,
        trigger_basis: str,
    ) -> ProtectionObservation:
        raise NotImplementedError("TEST GATE: authenticated venue binding required for ensure_full_stop")

    def read_economic_events(
        self,
        cursor: str,
        overlap_start_ns: int,
    ) -> tuple[tuple[EconomicEvent, ...], str]:
        raise NotImplementedError("TEST GATE: authenticated venue binding required for read_economic_events")


def create_protection_port(config: ProtectionPortConfig) -> BybitProtectionPort:
    """Factory for protection port.

    In Phase 2 offline qualification, returns TEST GATE implementation.
    In later phases, may return authenticated implementation.
    """
    return BybitProtectionPortUnavailable()
