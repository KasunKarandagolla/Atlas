"""Typed runtime identity (freeze §1.6, §1.8 test 1)."""

from __future__ import annotations

from dataclasses import dataclass

from atlas.domain.enums import Environment


def _nonblank(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-blank string")
    return value.strip()


@dataclass(frozen=True)
class RuntimeIdentity:
    environment: Environment
    expected_venue: str
    expected_product: str
    expected_position_mode: str
    expected_margin_mode: str
    account_identity_hash: str

    def __post_init__(self) -> None:
        if not isinstance(self.environment, Environment):
            raise ValueError("environment must be Environment")
        for f in (
            "expected_venue",
            "expected_product",
            "expected_position_mode",
            "expected_margin_mode",
            "account_identity_hash",
        ):
            _nonblank(getattr(self, f), f)

    def to_dict(self) -> dict:
        return {
            "environment": self.environment.value,
            "expected_venue": self.expected_venue,
            "expected_product": self.expected_product,
            "expected_position_mode": self.expected_position_mode,
            "expected_margin_mode": self.expected_margin_mode,
            "account_identity_hash": self.account_identity_hash,
        }
