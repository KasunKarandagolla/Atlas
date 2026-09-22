"""Time primitives (freeze §2.2, §4.1)."""

from __future__ import annotations

from datetime import UTC, datetime

NANOS_PER_SECOND = 1_000_000_000


def ensure_utc_ns(value: int, *, field: str = "timestamp_ns") -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field}: bool is not a valid timestamp")
    if not isinstance(value, int):
        raise ValueError(f"{field}: must be int UTC nanoseconds, got {type(value).__name__}")
    if value < 0:
        raise ValueError(f"{field}: negative nanoseconds not allowed")
    return value


def datetime_to_ns(dt: datetime, *, field: str = "datetime") -> int:
    if not isinstance(dt, datetime):
        raise ValueError(f"{field}: must be datetime, got {type(dt).__name__}")
    if dt.tzinfo is None:
        raise ValueError(f"{field}: naive datetime rejected; require tz-aware UTC")
    utc = dt.astimezone(UTC)
    seconds = int(utc.timestamp())
    return seconds * NANOS_PER_SECOND + utc.microsecond * 1000


def ns_to_datetime(ns: int) -> datetime:
    ns = ensure_utc_ns(ns)
    seconds, rem = divmod(ns, NANOS_PER_SECOND)
    micros, _ = divmod(rem, 1000)
    return datetime.fromtimestamp(seconds, tz=UTC).replace(microsecond=micros)


def now_ns() -> int:
    return datetime_to_ns(datetime.now(tz=UTC))


def ensure_after(a_ns: int, b_ns: int, *, field_a: str = "a", field_b: str = "b") -> None:
    if a_ns <= b_ns:
        raise ValueError(f"{field_a} ({a_ns}) must be after {field_b} ({b_ns})")
