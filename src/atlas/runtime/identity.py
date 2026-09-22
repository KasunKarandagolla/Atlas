from __future__ import annotations

import hashlib
from dataclasses import dataclass

from atlas.domain.enums import Environment


def account_identity_hash(environment: str, venue: str, account_identifier: str) -> str:
    if not all(isinstance(x, str) and x.strip() for x in (environment, venue, account_identifier)):
        raise ValueError("identity fields required")
    return hashlib.sha256(f"{environment}|{venue}|{account_identifier}".encode()).hexdigest()


@dataclass(frozen=True)
class RuntimeIdentity:
    """Historical typed runtime identity retained alongside the v5 hash helper."""

    environment: Environment
    expected_venue: str
    expected_product: str
    expected_position_mode: str
    expected_margin_mode: str
    account_identity_hash: str

    def __post_init__(self) -> None:
        if not isinstance(self.environment, Environment):
            raise ValueError("environment must be Environment")
        for n in (
            "expected_venue",
            "expected_product",
            "expected_position_mode",
            "expected_margin_mode",
            "account_identity_hash",
        ):
            if not isinstance(getattr(self, n), str) or not getattr(self, n).strip():
                raise ValueError(f"{n} must be non-blank")

    def to_dict(self) -> dict[str, str]:
        return {
            "environment": self.environment.value,
            "expected_venue": self.expected_venue,
            "expected_product": self.expected_product,
            "expected_position_mode": self.expected_position_mode,
            "expected_margin_mode": self.expected_margin_mode,
            "account_identity_hash": self.account_identity_hash,
        }
