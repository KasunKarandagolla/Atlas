"""One PROPOSED fixed partial-TP research variant. No commands or transport.

Frozen control remains native MarkPrice stop plus fixed time exit, no TP.
This function records a hypothetical closing quantity, never operative authority.
"""

from dataclasses import dataclass, field
from decimal import ROUND_DOWN, Decimal

from atlas.runtime.position_lifecycle import PositionLifecycleProjectionV1
from atlas.v2.instruments import ProductContractV2

VARIANT = "ATLAS_PARTIAL_TP_2R_HALF_SHADOW_V1"


@dataclass(frozen=True)
class ShadowManagementResultV1:
    status: str
    reason: str
    hypothetical_close_quantity: Decimal
    variant: str = VARIANT
    can_execute: bool = field(default=False, init=False)
    can_cancel_protection: bool = field(default=False, init=False)


def review_partial_tp(
    *,
    projection: PositionLifecycleProjectionV1,
    product: ProductContractV2,
    variant: str,
    now_ns: int,
    expires_at_ns: int,
    stop_hit: bool,
    tp_2r_hit: bool,
    chronology_verified: bool,
    review_available: bool = True,
) -> ShadowManagementResultV1:
    """Caller supplies research trigger observations, not verified venue fills.

    A bar containing both stop/TP cannot establish event ordering. Stop wins when
    positively first; after a real stop fill the next projection must be flat.
    """
    status, reason, qty = "BLOCKED", "UNSUPPORTED_MANAGEMENT_REQUEST", Decimal("0")
    if variant != VARIANT:
        pass
    elif now_ns >= expires_at_ns:
        reason = "REVIEW_EXPIRED"
    elif not review_available:
        reason = "OPTIONAL_REVIEW_UNAVAILABLE"
    elif (
        projection.execution_key != product.key
        or projection.product_ref != product.content_hash
        or projection.state != "OPEN_PROTECTED"
        or not projection.protection_verified
        or projection.reasons
        or projection.signed_quantity is None
        or now_ns >= projection.next_review_at_ns
    ):
        reason = "CURRENT_LOCAL_PROTECTION_UNVERIFIED"
    elif stop_hit and tp_2r_hit and not chronology_verified:
        status, reason = "NOT ESTIMABLE", "HISTORICAL_STOP_TP_ORDER_AMBIGUOUS"
    elif stop_hit:
        status, reason = "NO_ACTION", "MANDATORY_STOP_PRECEDENCE"
    elif not tp_2r_hit:
        status, reason = "NO_ACTION", "REGISTERED_2R_TRIGGER_ABSENT"
    else:
        qty = (abs(projection.signed_quantity) / 2 / product.qty_step).to_integral_value(
            rounding=ROUND_DOWN
        ) * product.qty_step
        if qty < product.min_qty:
            qty, status, reason = Decimal("0"), "NO_ACTION", "PARTIAL_QUANTITY_BELOW_MINIMUM"
        else:
            status, reason = "SHADOW_ONLY", "HYPOTHETICAL_CLOSING_ONLY_LAST_STOP_PRESERVED"
    return ShadowManagementResultV1(status, reason, qty, variant)
