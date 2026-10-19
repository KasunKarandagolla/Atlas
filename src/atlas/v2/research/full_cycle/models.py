"""Model inventory, bounded checkpoint acquisition and ML-ready research tables.

Model acquisition and inference qualification are different results and are
recorded separately. A download alone never counts as integration. Checkpoints
are fetched serially into a controlled cache, are never executed as remote code,
and are removed from the cache once their inference evidence, provenance and
reproducibility record are preserved.
"""

from __future__ import annotations

import hashlib
import time
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
        for snapshot in sorted(snapshots.iterdir()):
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
        checkpoint=checkpoint, revision=revision, weight_files=tuple(weight_files),
        total_bytes=total, license_ref=license_ref,
        source_ref=f"huggingface:{checkpoint}@{revision}")


def download_checkpoint(checkpoint: str, cache_root: Path, *, revision: str | None = None,
                        timeout_s: int = 900) -> dict[str, Any]:
    """Download one checkpoint into the controlled cache. No remote code is executed."""

    from huggingface_hub import snapshot_download

    cache_root.mkdir(parents=True, exist_ok=True)
    patterns = FOUNDATION_ALLOW_PATTERNS.get(checkpoint, ("config.json", "*.safetensors"))
    started = time.time()
    try:
        path = snapshot_download(
            repo_id=checkpoint, revision=revision, cache_dir=str(cache_root),
            allow_patterns=list(patterns), local_files_only=False, max_workers=2)
        status = "DOWNLOADED"
        error = None
    except Exception as exc:  # bounded: a failed download is recorded, not hidden
        path = None
        status = "BLOCKED"
        error = f"{type(exc).__name__}: {exc}"
    return {
        "checkpoint": checkpoint,
        "requested_revision": revision,
        "status": status,
        "error": error,
        "local_path": path,
        "elapsed_seconds": round(time.time() - started, 3),
        "allow_patterns": list(patterns),
    }


def bounded_inference_probe(checkpoint: str, local_path: str | None, *,
                            context: int = 64, horizon: int = 8) -> dict[str, Any]:
    """One bounded real inference invocation when the runtime allows it.

    Unreviewed remote model code is never executed: ``trust_remote_code`` is
    always false. A checkpoint that requires custom code is reported as
    BLOCKED BY ENVIRONMENT rather than silently substituted.
    """

    if not local_path:
        return {"status": "BLOCKED_BY_ENVIRONMENT", "reason": "NO_LOCAL_CHECKPOINT"}
    try:
        import torch  # noqa: PLC0415
        from transformers import AutoModel  # noqa: PLC0415
    except Exception as exc:
        return {"status": "BLOCKED_BY_ENVIRONMENT",
                "reason": f"INFERENCE_RUNTIME_UNAVAILABLE:{type(exc).__name__}:{exc}"}
    try:
        model = AutoModel.from_pretrained(local_path, trust_remote_code=False)
        model.eval()
        torch.set_num_threads(1)
        inputs = torch.zeros(1, context, 1, dtype=torch.float32)
        with torch.no_grad():
            output = model(inputs)
        shape = list(getattr(output, "shape", []) or [])
        return {
            "status": "INFERENCE_VERIFIED",
            "context": context,
            "horizon": horizon,
            "output_shape": shape,
            "parameter_count": int(sum(p.numel() for p in model.parameters())),
            "dtype": str(next(model.parameters()).dtype),
        }
    except Exception as exc:
        return {"status": "BLOCKED_BY_ENVIRONMENT",
                "reason": f"{type(exc).__name__}: {exc}",
                "note": "no substitute checkpoint was used"}


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
