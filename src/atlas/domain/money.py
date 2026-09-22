"""Money/Decimal validation helpers (freeze §2.2: decimal arithmetic for price/qty/money)."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

DecimalLike = Decimal | int | str


def ensure_decimal(value: DecimalLike, *, field: str = "value") -> Decimal:
    if isinstance(value, float):
        raise ValueError(f"{field}: float is not accepted; pass str/int/Decimal")
    if isinstance(value, bool):
        raise ValueError(f"{field}: bool is not a valid decimal")
    try:
        d = (
            value
            if isinstance(value, Decimal)
            else Decimal(str(value))
            if isinstance(value, int)
            else Decimal(value)
            if isinstance(value, str)
            else None
        )
    except (InvalidOperation, ValueError, ArithmeticError) as exc:
        raise ValueError(f"{field}: invalid decimal {value!r}") from exc
    if d is None:
        raise ValueError(f"{field}: unsupported type {type(value).__name__}")
    if d.is_nan() or d.is_infinite():
        raise ValueError(f"{field}: NaN/Infinity not allowed")
    return d


def ensure_non_negative_decimal(value: DecimalLike, *, field: str = "value") -> Decimal:
    d = ensure_decimal(value, field=field)
    if d < 0:
        raise ValueError(f"{field}: negative value not allowed: {d}")
    return d


def ensure_positive_decimal(value: DecimalLike, *, field: str = "value") -> Decimal:
    d = ensure_decimal(value, field=field)
    if d <= 0:
        raise ValueError(f"{field}: must be positive, got {d}")
    return d


def ensure_fraction(value: DecimalLike, *, field: str = "value", allow_zero: bool = True) -> Decimal:
    d = ensure_decimal(value, field=field)
    if not allow_zero and d <= 0:
        raise ValueError(f"{field}: must be positive fraction")
    if d < 0:
        raise ValueError(f"{field}: fraction must be >= 0")
    if d > 1:
        raise ValueError(f"{field}: fraction must be <= 1, got {d}")
    return d


def canonical_decimal_str(d: Decimal) -> str:
    if not isinstance(d, Decimal):
        raise ValueError("canonical_decimal_str requires Decimal")
    if d.is_nan() or d.is_infinite():
        raise ValueError("NaN/Infinity not serializable")
    if d == 0:
        return "0"
    s = format(d, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return "0" if s in ("-0", "-0.0", "") else s
