"""Offline validation at the existing trusted Ops writer, bounded consumption.

This reserved artifact is NOT a portable assertion of trust. Every initial
registration (including generic/batch import) reconstructs the complete chain
and registered bootstrap before insertion. Only an exact immutable retry skips
that work. Admission reads this checked writer state, never a caller boolean.

Trust boundary: the current Ops writer implementation and its local database,
as for existing durable selection/holdout receipts. Arbitrary SQL/database/code
replacement is outside that boundary. Copies of untrusted artifacts must enter
through register_artifacts, not be installed as a pre-trusted SQLite database.
"""

from __future__ import annotations

import hashlib
import random
import sys
from dataclasses import dataclass
from decimal import Decimal, getcontext
from functools import cache
from pathlib import Path
from typing import ClassVar

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2
from atlas.v2.science.mean_common import OfflineArtifact, nonnegative


@dataclass(frozen=True)
class MeanCalibrationValidationV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_MEAN_CALIBRATION_VALIDATION_V1"
    calibration_ref: str
    implementation_ref: str
    required_allowance_cash: Decimal
    available_at_ns: int
    expires_at_ns: int

    def __post_init__(self):
        super().__post_init__()
        nonnegative(self.required_allowance_cash)
        if self.available_at_ns >= self.expires_at_ns:
            raise ValueError("INVALID_CALIBRATION_VALIDATION_INTERVAL")


@cache
def _implementation_sources():
    # Binds the actual verifier, producer, model/codec and durable-write boundary.
    # File I/O happens once at offline publication/startup, never per admission.
    root = Path(__file__).resolve().parents[1]
    paths = (
        "_serialization.py", "science/mean_common.py", "science/mean_models.py",
        "science/mean_research.py", "science/mean_calibration.py",
        "science/mean_calibration_validation.py", "memory/repository.py",
        "memory/compressed_metadata.py", "memory/schema.py",
        "science/mean_evaluation.py", "research/mean_selection.py",
    )
    return {
        **{p: hashlib.sha256((root / p).read_bytes()).hexdigest() for p in paths},
        "python_random": hashlib.sha256(Path(random.__file__).read_bytes()).hexdigest(),
        "python_runtime": sys.version,
    }


def validation_implementation_ref():
    context = getcontext()
    return sha256_json({
        "validator": MeanCalibrationValidationV1.VERSION,
        "sources": _implementation_sources(),
        "decimal": [context.prec, context.rounding, context.Emin, context.Emax, context.clamp,
                    sorted(k.__name__ for k, enabled in context.traps.items() if enabled)],
    })


def validation_ref(calibration_ref, implementation_ref):
    return sha256_json({
        "mean_calibration_validation": calibration_ref, "implementation_ref": implementation_ref,
    })


def calibration_validation_entry(calibration, required_allowance):
    """Constructing this request confers no trust; Ops must validate its insert."""
    receipt = MeanCalibrationValidationV1(
        calibration.content_hash, validation_implementation_ref(), required_allowance,
        calibration.available_at_ns, calibration.expires_at_ns,
    )
    return ArtifactIndexEntryV2(
        validation_ref(receipt.calibration_ref, receipt.implementation_ref),
        MeanCalibrationValidationV1.__name__, receipt.content_hash,
        receipt.available_at_ns, receipt.available_at_ns,
        {"mean_artifact": receipt.to_dict(), "authority": "ZERO"},
    )


def validate_calibration_validation_registration(repo, entry):
    """OFFLINE ONLY. Called by Ops on EVERY new reserved receipt insertion.

    Even a perfectly rehashed forged request must reproduce the actual bound.
    The claimed bound is never an input to the independent calculation.
    """
    from atlas.v2.science.mean_calibration import (
        MeanEvidenceScopeV1,
        MeanSelectionCalibrationV2,
        _verify_mean_calibration_chain,
        dependence_diagnostics,
        selected_policy_upper_cash,
    )
    from atlas.v2.science.mean_evaluation import read_mean_artifact
    from atlas.v2.science.mean_models import MeanModelV1

    try:
        receipt = MeanCalibrationValidationV1.from_dict(entry.metadata["mean_artifact"])
        cutoff = receipt.available_at_ns
        calibration = read_mean_artifact(repo, receipt.calibration_ref, MeanSelectionCalibrationV2, consumer_ns=cutoff)
        model = read_mean_artifact(repo, calibration.model_ref, MeanModelV1, consumer_ns=cutoff)
        scope = read_mean_artifact(repo, calibration.source_ref, MeanEvidenceScopeV1, consumer_ns=cutoff)
        _, _, protocol, values = _verify_mean_calibration_chain(
            repo, calibration, model=model, policy_ref=scope.policy_ref, product_ref=scope.product_ref,
            cutoff_ns=cutoff, now_ns=cutoff,
        )
        groups, _, _ = dependence_diagnostics(values, protocol)
        required = selected_policy_upper_cash(groups, alpha=protocol.alpha, resamples=protocol.resamples)
        if calibration.allowance_cash < required:
            raise ValueError("CALIBRATION_ALLOWANCE_BELOW_REGISTERED_BOOTSTRAP")
        if entry != calibration_validation_entry(calibration, required):
            raise ValueError("CALIBRATION_VALIDATION_BINDING_CHANGED")
    except (KeyError, TypeError) as exc:
        raise ValueError("INVALID_CALIBRATION_VALIDATION_RECEIPT") from exc


def require_calibration_validation(repo, calibration, *, cutoff_ns, now_ns):
    """One bounded immutable receipt lookup; no bootstrap, fit or OOF generation."""
    implementation = validation_implementation_ref()
    entry = repo.get_artifact(validation_ref(calibration.content_hash, implementation))
    if entry is None or entry.artifact_type != MeanCalibrationValidationV1.__name__:
        raise ValueError("CALIBRATION_OFFLINE_VALIDATION_UNAVAILABLE")
    try:
        receipt = MeanCalibrationValidationV1.from_dict(entry.metadata["mean_artifact"])
        if (
            entry != calibration_validation_entry(calibration, receipt.required_allowance_cash)
            or entry.available_at_ns > cutoff_ns
            or now_ns >= receipt.expires_at_ns
            or calibration.allowance_cash is None
            or calibration.allowance_cash < receipt.required_allowance_cash
        ):
            raise ValueError("CALIBRATION_OFFLINE_VALIDATION_CHANGED_OR_EXPIRED")
    except (KeyError, TypeError) as exc:
        raise ValueError("INVALID_CALIBRATION_VALIDATION_RECEIPT") from exc
