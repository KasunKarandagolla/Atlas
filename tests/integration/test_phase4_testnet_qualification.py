"""Opt-in integration gate; this repository run has no authenticated venue harness."""

from __future__ import annotations

import os

import pytest


@pytest.mark.integration
@pytest.mark.parametrize("venue,credential_names", [
    ("BYBIT", ("BYBIT_TESTNET_API_KEY", "BYBIT_TESTNET_API_SECRET")),
    ("BINANCE", ("BINANCE_TESTNET_API_KEY", "BINANCE_TESTNET_API_SECRET")),
])
def test_authenticated_testnet_qualification_is_explicit_opt_in(venue, credential_names):
    if os.environ.get("ATLAS_RUN_VENUE_QUALIFICATION") != "1":
        pytest.skip("TEST GATE: authenticated testnet qualification requires explicit opt-in")
    if not all(os.environ.get(name) for name in credential_names):
        pytest.skip(f"BLOCKED BY ENVIRONMENT: {venue} testnet credentials are not configured")
    pytest.skip(f"BLOCKED BY ENVIRONMENT: no credential-safe {venue} qualification harness is installed")
