"""Generic prefix-invariance test helpers for causal transforms."""

from __future__ import annotations

from collections.abc import Callable, Sequence


def assert_prefix_invariant[T, U](
    transform: Callable[[Sequence[T]], Sequence[U]], records: Sequence[T], *, warmup: int = 0
) -> None:
    full = list(transform(records))
    for n in range(max(1, warmup + 1), len(records) + 1):
        prefix = list(transform(records[:n]))
        expected = full[: len(prefix)]
        if prefix != expected:
            raise AssertionError(f"prefix invariance failed at prefix {n}")


def assert_future_tail_independence[T, U](
    transform: Callable[[Sequence[T]], Sequence[U]],
    original: Sequence[T],
    mutated_tail: Sequence[T],
    decision_index: int,
) -> None:
    if len(original) != len(mutated_tail):
        raise ValueError("length mismatch")
    a = list(transform(original))[:decision_index]
    b = list(transform(mutated_tail))[:decision_index]
    if a != b:
        raise AssertionError("future-tail mutation changed earlier outputs")
