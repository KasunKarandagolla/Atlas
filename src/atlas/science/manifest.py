"""Canonical immutable Phase-4 model manifest helper."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from atlas.science.huber_mean import HUBER_TRANSITION, RIDGE_CANDIDATES, SOLVER_TOLERANCE
from atlas.science.scenarios import HORIZON_HOURS, MINUTES_PER_HOUR, PRODUCTION_PATHS
from atlas.science.uncertainty import BOOTSTRAP_INNER_PATHS, BOOTSTRAP_REPLICATES
from atlas.strategy.crypto_trend_24h_v1 import CRYPTO_TREND_24H_V1, STRATEGY_VERSION
from atlas.strategy.features import EWMA_HALF_LIFE_HOURS, EWMA_SEED_COUNT, WINDOW_RETURNS


@dataclass(frozen=True)
class ModelManifest:
    values: dict[str, Any]

    def canonical_json(self) -> str:
        return json.dumps(self.values, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    def hash(self) -> str:
        return hashlib.sha256(self.canonical_json().encode()).hexdigest()


def phase4_manifest(*, selected_ridge: float, fit_at_ns: int, training_interval: tuple[int, int], validation_folds: tuple[tuple[int, int], ...],
                    selected_block: int, seeds: tuple[int, ...], risk_policy_hash: str, availability_mode: str,
                    solver: str = "deterministic_irls", cost_model_version: str = "v1", funding_model_version: str = "v1",
                    stress_suite_version: str = "v1") -> ModelManifest:
    """Create the fixed-authority Phase-4 manifest; arbitrary omission is impossible."""
    if selected_ridge not in RIDGE_CANDIDATES or selected_block not in (24, 48, 72):
        raise ValueError("frozen selected parameter required")
    return ModelManifest({
        "strategy_id": CRYPTO_TREND_24H_V1, "strategy_version": STRATEGY_VERSION, "feature_contract_version": "1.0",
        "ewma_half_life_hours": EWMA_HALF_LIFE_HOURS, "ewma_returns_window": WINDOW_RETURNS, "ewma_seed_count": EWMA_SEED_COUNT,
        "signal_band": 0.5, "stop_multiplier": 2, "stop_bounds": [0.0025, 0.10], "hold_hours": 24, "slots": [0, 4, 8, 12, 16, 20],
        "huber_transition": HUBER_TRANSITION, "ridge_candidates": RIDGE_CANDIDATES, "selected_ridge": selected_ridge,
        "solver": solver, "solver_tolerance": SOLVER_TOLERANCE, "fit_at_ns": fit_at_ns, "training_interval": training_interval,
        "validation_folds": validation_folds, "block_candidates": [24, 48, 72], "selected_block": selected_block,
        "energy_score_version": "joint_energy_v1", "scenario_paths": PRODUCTION_PATHS, "horizon_hours": HORIZON_HOURS,
        "minutes_per_hour": MINUTES_PER_HOUR, "bootstrap_count": BOOTSTRAP_REPLICATES, "bootstrap_inner_count": BOOTSTRAP_INNER_PATHS,
        "seeds": seeds, "risk_policy_hash": risk_policy_hash, "cost_model_version": cost_model_version,
        "funding_model_version": funding_model_version, "stress_suite_version": stress_suite_version, "availability_mode": availability_mode,
    })
