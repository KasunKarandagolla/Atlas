"""Typed settings loader: env names only, no secret values in repo."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    environment: str = "testnet"
    journal_path: str = "./atlas-journal.db"
    writer_lock_path: str = "./atlas-writer.lock"

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            environment=os.environ.get("ATLAS_ENVIRONMENT", "testnet"),
            journal_path=os.environ.get("ATLAS_JOURNAL_PATH", "./atlas-journal.db"),
            writer_lock_path=os.environ.get(
                "ATLAS_WRITER_LOCK_PATH", "./atlas-writer.lock"
            ),
        )
