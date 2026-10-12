from dataclasses import replace
from decimal import Decimal

import pytest
from conftest import T0

from atlas.runtime.position_lifecycle import PositionLifecycleProjectionV1
from atlas.v2.research.management_shadow import VARIANT, review_partial_tp
from tests.runtime.test_session041_binance_demo import product
from tests.runtime.test_session048_position_protection import confirmed


def projection():
    p = product()
    # Explicit shadow input only, not manufactured venue readback/qualification.
    return PositionLifecycleProjectionV1(
        p.key,
        p.key,
        p.content_hash,
        "research",
        0,
        1,
        "shadow",
        Decimal("2"),
        "OPEN_PROTECTED",
        True,
        T0 + 2_000_000_000,
        ("shadow-only",),
        (),
    )


def review(**changes):
    kwargs = {
        "projection": projection(),
        "product": product(),
        "variant": VARIANT,
        "now_ns": T0 + 1,
        "expires_at_ns": T0 + 100,
        "stop_hit": False,
        "tp_2r_hit": True,
        "chronology_verified": True,
    }
    return review_partial_tp(**(kwargs | changes))


def test_one_registered_half_tp_variant_cannot_mutate_stop_or_execute():
    r = review()
    assert r.status == "SHADOW_ONLY" and r.hypothetical_close_quantity == Decimal("1")
    assert not r.can_execute and not r.can_cancel_protection


@pytest.mark.parametrize("fault", ["unsupported", "expired", "unknown", "flat", "stale", "no_llm"])
def test_optional_management_fault_never_supersedes_compulsory_protection(fault):
    changes = {
        "unsupported": {"variant": "TRAILING_OR_SCALE"},
        "expired": {"expires_at_ns": T0},
        "unknown": {"projection": replace(projection(), state="UNKNOWN")},
        "flat": {"projection": replace(projection(), state="FLAT_PENDING_ECONOMICS", signed_quantity=Decimal("0"))},
        "stale": {"now_ns": T0 + 3_000_000_000},
        "no_llm": {"review_available": False},
    }[fault]
    r = review(**changes)
    assert r.status == "BLOCKED" and r.hypothetical_close_quantity == 0
    m = confirmed()
    assert m.tick(T0 + 3_000_000_000)["intent-1"].flatten_eligible


def test_same_bar_stop_and_tp_is_not_an_estimated_profitable_fill():
    r = review(stop_hit=True, chronology_verified=False)
    assert r.status == "NOT ESTIMABLE" and r.hypothetical_close_quantity == 0


def test_stop_first_and_flat_after_stop_prevent_tp_reversal():
    assert review(stop_hit=True).hypothetical_close_quantity == 0
    flat = replace(projection(), state="FLAT_PENDING_ECONOMICS", signed_quantity=Decimal("0"))
    assert review(projection=flat).hypothetical_close_quantity == 0


def test_short_partial_tp_quantity_is_positive_closing_quantity_with_no_add():
    r = review(projection=replace(projection(), signed_quantity=Decimal("-1.01")))
    assert r.hypothetical_close_quantity == Decimal("0.50")


def test_foreign_or_unverified_local_economics_blocks_partial_tp_research():
    r = review(projection=replace(projection(), reasons=("EXECUTION_VENUE_FUNDING_UNVERIFIED",)))
    assert r.status == "BLOCKED" and r.hypothetical_close_quantity == 0
