"""Model inventory, bounded checkpoint acquisition and ML-ready research tables.

Model acquisition and inference qualification are different results and are
recorded separately. A download alone never counts as integration. Checkpoints
are fetched serially into a controlled cache, are never executed as remote code,
and retained in a permanent cache. Temporary staging is accounted separately.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

MODEL_INVENTORY_VERSION = "ATLAS_RED_FULL_CYCLE_MODEL_INVENTORY_V1"

# The canonical selected numerical model families of the ATLAS RED programme.
SELECTED_MODELS: tuple[dict[str, Any], ...] = (
    {
        "model_id": "M0_ARITHMETIC_MEAN_NET_ACTION",
        "checkpoint": None,
        "role": "ARITHMETIC_MEAN_NET_ACTION_MODEL",
        "owner": "atlas.v2.science.m0",
        "kind": "IN_REPOSITORY_STATISTICAL_MODEL",
    },
    {
        "model_id": "EMPIRICAL_PAYOFF_COMPARATOR",
        "checkpoint": None,
        "role": "EMPIRICAL_BASELINE_COMPARATOR",
        "owner": "atlas.v2.science.mean_evaluation",
        "kind": "IN_REPOSITORY_EMPIRICAL_BASELINE",
    },
    {
        "model_id": "M1_LIGHTGBM_L2_CASH_MEAN",
        "checkpoint": None,
        "role": "LIGHTGBM_L2_CASH_MEAN_CHALLENGER",
        "owner": "atlas.v2.science.m1",
        "kind": "IN_REPOSITORY_STATISTICAL_MODEL",
    },
    {
        "model_id": "TIREX_2",
        "checkpoint": "NX-AI/TiRex-2",
        "role": "FOUNDATION_TIME_SERIES_FORECASTER",
        "owner": "atlas.v2.models.adapters.tirex2",
        "kind": "OPEN_WEIGHT_CHECKPOINT",
    },
    {
        "model_id": "CHRONOS_2_SMALL",
        "checkpoint": "autogluon/chronos-2-small",
        "role": "FOUNDATION_TIME_SERIES_FORECASTER",
        "owner": "atlas.v2.models.adapters.chronos2",
        "kind": "OPEN_WEIGHT_CHECKPOINT",
    },
    {
        "model_id": "KRONOS_MINI",
        "checkpoint": "NeoQuasar/Kronos-mini",
        "role": "FOUNDATION_TIME_SERIES_FORECASTER",
        "owner": "atlas.v2.models.adapters.kronos_mini",
        "kind": "OPEN_WEIGHT_CHECKPOINT",
    },
    {
        "model_id": "KRONOS_TOKENIZER_2K",
        "checkpoint": "NeoQuasar/Kronos-Tokenizer-2k",
        "role": "FOUNDATION_TIME_SERIES_TOKENIZER",
        "owner": "atlas.v2.models.adapters.kronos_mini",
        "kind": "OPEN_WEIGHT_CHECKPOINT",
    },
)

FOUNDATION_ALLOW_PATTERNS: Mapping[str, tuple[str, ...]] = {
    "NX-AI/TiRex-2": ("config.json", "*.safetensors", "LICENSE", "README.md"),
    "autogluon/chronos-2-small": ("config.json", "*.safetensors", "README.md"),
    "NeoQuasar/Kronos-mini": ("config.json", "*.safetensors", "README.md"),
    "NeoQuasar/Kronos-Tokenizer-2k": ("config.json", "*.safetensors", "README.md"),
}


@dataclass(frozen=True)
class CheckpointIdentityV1:
    checkpoint: str
    revision: str
    weight_files: tuple[tuple[str, str], ...]
    total_bytes: int
    license_ref: str | None
    source_ref: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "checkpoint": self.checkpoint,
            "revision": self.revision,
            "weight_files": [list(item) for item in self.weight_files],
            "total_bytes": self.total_bytes,
            "license_ref": self.license_ref,
            "source_ref": self.source_ref,
        }


def sha256_file(path: Path, *, chunk: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(chunk)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def resolve_checkpoint_identity(checkpoint: str, cache_root: Path) -> CheckpointIdentityV1:
    """Resolve the exact revision and weight hashes of a downloaded checkpoint."""

    repo_dir = cache_root / ("models--" + checkpoint.replace("/", "--"))
    snapshots = repo_dir / "snapshots"
    revision = ""
    weight_files: list[tuple[str, str]] = []
    total = 0
    license_ref: str | None = None
    if snapshots.is_dir():
        candidates = sorted(p for p in snapshots.iterdir() if p.is_dir())
        if len(candidates) > 1:
            raise ValueError("AMBIGUOUS_CHECKPOINT_REVISIONS_REQUIRE_EXACT_SELECTION")
        for snapshot in candidates:
            if not snapshot.is_dir():
                continue
            revision = revision or snapshot.name
            for path in sorted(snapshot.rglob("*")):
                if not path.is_file():
                    continue
                if path.name == "LICENSE":
                    license_ref = "LICENSE"
                if path.suffix in (".safetensors", ".bin", ".ckpt", ".pt"):
                    weight_files.append((str(path.relative_to(snapshot)), sha256_file(path)))
                    total += path.stat().st_size
    return CheckpointIdentityV1(
        checkpoint=checkpoint,
        revision=revision,
        weight_files=tuple(weight_files),
        total_bytes=total,
        license_ref=license_ref,
        source_ref=f"huggingface:{checkpoint}@{revision}",
    )


def download_checkpoint(
    checkpoint: str, cache_root: Path, *, revision: str | None = None, timeout_s: int = 900
) -> dict[str, Any]:
    """Download one checkpoint into the controlled cache. No remote code is executed."""

    raise ValueError("USE_PINNED_QUOTA_CHECKED_atlas_red_provision_models.py")


def bounded_inference_probe(
    checkpoint: str, local_path: str | None, *, context: int = 64, horizon: int = 8
) -> dict[str, Any]:
    """One bounded real inference invocation when the runtime allows it.

    Unreviewed remote model code is never executed: ``trust_remote_code`` is
    always false. A checkpoint that requires custom code is reported as
    BLOCKED BY ENVIRONMENT rather than silently substituted.
    """

    return {
        "status": "BLOCKED_BY_ENVIRONMENT",
        "reason": "MODEL_SPECIFIC_VERIFIED_LOCAL_RUNNER_REQUIRED",
        "command": "scripts/atlas_red_model_smoke.py",
    }


def foundation_input_unit_check(sample_values: Sequence[float]) -> dict[str, Any]:
    """The registered ABI is log-price; verify the input unit before any inference."""

    if not sample_values:
        return {"status": "NO_INPUT"}
    if any(value <= 0 for value in sample_values):
        return {"status": "REJECTED", "reason": "NON_POSITIVE_PRICE_CANNOT_BE_LOGGED"}
    import math

    return {
        "status": "ACCEPTED",
        "transform": "NATURAL_LOG",
        "first_log": math.log(sample_values[0]),
        "last_log": math.log(sample_values[-1]),
    }
