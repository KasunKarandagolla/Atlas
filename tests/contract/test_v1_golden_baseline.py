"""Additive transition guard for the frozen V1 contracts and fixtures."""

from __future__ import annotations

import hashlib
import importlib.metadata
import importlib.util
import json
import re
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml
from support.phase4_factory import SLOT, decision_input, replay_minutes
from support.phase4_factory import policy as fixture_policy

from atlas.domain.capability import capability_contract_from_manifest
from atlas.domain.information import actual_observed
from atlas.domain.money import canonical_decimal_str
from atlas.domain.risk import engineering_default_policy
from atlas.persistence.schema import SCHEMA_VERSION
from atlas.science.decision_calendar import record_from_evaluation
from atlas.science.manifest import phase4_manifest
from atlas.science.phase4_engine import evaluate_phase4
from atlas.science.policy_replay import ReplayAssumptions, replay_policy
from atlas.strategy.crypto_trend_24h_v1 import CRYPTO_TREND_24H_V1, STRATEGY_VERSION
from tests.unit.test_trade_plan import make_valid_plan

ROOT = Path(__file__).resolve().parents[2]
GOLDEN_PATH = ROOT / "docs" / "v2" / "V1_GOLDEN_BASELINE.json"
BASELINE_COMMIT = "146bae0a2f10e2f794cbed3441123071c55a2baf"
EXPECTED_NAUTILUS_VERSION = "2.0.0rc5"
EXPECTED_NAUTILUS_WHEEL_SHA256 = "eab45fafd2312deda1236554c49a9798bfc76bc8465af864878e2f70189ebebe"


def _phase4_manifest_inputs() -> dict[str, Any]:
    return {
        "selected_ridge": 0.1,
        "fit_at_ns": 1_700_000_000_000_000_000,
        "training_interval": [1_680_000_000_000_000_000, 1_699_000_000_000_000_000],
        "validation_folds": [
            [1_690_000_000_000_000_000, 1_693_000_000_000_000_000],
            [1_694_000_000_000_000_000, 1_697_000_000_000_000_000],
        ],
        "selected_block": 24,
        "seeds": [101, 202],
        "availability_mode": "RECONSTRUCTED_MARKET",
    }


def _replay_projection() -> dict[str, Any]:
    policy = fixture_policy()
    horizon_at = SLOT + 24 * 3_600_000_000_000
    minutes = replay_minutes((Decimal("100"),), start_ns=SLOT) + replay_minutes(
        (Decimal("110"),), start_ns=horizon_at
    )
    result = replay_policy(
        policy,
        minutes,
        (),
        ReplayAssumptions(
            decision_to_venue_ns=0,
            human_delay_ns=0,
            tick=Decimal("0.1"),
            taker_fee_rate=Decimal("0"),
            stop_spread_impact=Decimal("0"),
            time_exit_market_escalation_supported=False,
            extension_bound_supported=False,
        ),
    )
    assert result.entry_fill is not None
    assert result.evidence_status is not None
    outcome_status = result.outcome_status()
    assert outcome_status is not None
    return {
        "fixture": "tests/science/test_policy_replay_pnl.py::test_hand_calculated_long_and_short_profit_and_loss",
        "side": "LONG",
        "status": result.status.value if result.status is not None else None,
        "outcome_status": outcome_status.value,
        "evidence_status": result.evidence_status.value,
        "entry_qty": canonical_decimal_str(result.entry_fill.quantity),
        "entry_price": canonical_decimal_str(result.entry_fill.price),
        "exit_fills": [
            {
                "qty": canonical_decimal_str(fill.quantity),
                "price": canonical_decimal_str(fill.price),
                "at_ns": fill.at_ns,
            }
            for fill in result.exit_fills
        ],
        "remaining_qty": canonical_decimal_str(result.remaining_qty),
        "pnl": canonical_decimal_str(result.pnl) if result.pnl is not None else None,
    }


def _nautilus_observation() -> dict[str, Any]:
    spec = importlib.util.find_spec("nautilus_trader")
    assert spec is not None and spec.origin is not None
    extension_files = sorted(Path(spec.origin).parent.glob("_libnautilus*.so"))
    assert len(extension_files) == 1
    core_binary_sha256 = hashlib.sha256(extension_files[0].read_bytes()).hexdigest()
    return {
        "distribution_version": importlib.metadata.version("nautilus-trader"),
        "installed_core_binary_sha256": core_binary_sha256,
        "raw_wheel_sha256": None,
        "raw_wheel_sha256_status": "BLOCKED BY ENVIRONMENT",
    }


def _lock_nautilus_pin() -> tuple[str, str]:
    lines = (ROOT / "requirements-lock.txt").read_text(encoding="utf-8").splitlines()
    pin_index = next(index for index, line in enumerate(lines) if line.startswith("nautilus-trader=="))
    version_match = re.search(r"nautilus-trader==([^\\\s]+)", lines[pin_index])
    hash_match = re.search(r"--hash=sha256:([0-9a-f]{64})", lines[pin_index + 1])
    assert version_match is not None and hash_match is not None
    return version_match.group(1), hash_match.group(1)


def _recomputed_golden_values() -> dict[str, Any]:
    capability_data = yaml.safe_load(
        (ROOT / "docs" / "capability" / "bybit-v1.yaml").read_text(encoding="utf-8")
    )
    capability = capability_contract_from_manifest(capability_data)
    risk_policy = engineering_default_policy()
    inputs = _phase4_manifest_inputs()
    manifest = phase4_manifest(
        selected_ridge=inputs["selected_ridge"],
        fit_at_ns=inputs["fit_at_ns"],
        training_interval=tuple(inputs["training_interval"]),
        validation_folds=tuple(tuple(fold) for fold in inputs["validation_folds"]),
        selected_block=inputs["selected_block"],
        seeds=tuple(inputs["seeds"]),
        risk_policy_hash=risk_policy.policy_hash(),
        availability_mode=inputs["availability_mode"],
    )
    plan = make_valid_plan()

    decision = decision_input()
    evaluation = evaluate_phase4(decision)
    snapshot = decision.snapshot
    decision_record = record_from_evaluation(
        slot_id=f"golden-slot-{SLOT}",
        strategy_version=STRATEGY_VERSION,
        availability_cutoff_ns=decision.availability_cutoff_ns,
        evaluation=evaluation,
        slot_at_ns=SLOT,
        instrument="BTCUSDT",
        feature_snapshot_hash=snapshot.snapshot_hash(),
        signal=snapshot.signal.value,
    )

    replay = _replay_projection()
    replay_hash = hashlib.sha256(json.dumps(replay, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {
        "capability_contract_sha256": capability.contract_hash(),
        "engineering_default_risk_policy_sha256": risk_policy.policy_hash(),
        "phase4_model_manifest_sha256": manifest.hash(),
        "representative_trade_plan_sha256": plan.plan_hash(),
        "phase4_decision_calendar_record_sha256": decision_record.hash(),
        "phase4_decision_state": {
            "b0": evaluation.b0.status.value,
            "a0": evaluation.a0.status.value,
            "trade_plan_sha256": decision_record.trade_plan_hash,
        },
        "small_replay_projection": replay,
        "small_replay_projection_sha256": replay_hash,
    }


def _contract_versions() -> dict[str, str | int]:
    capability_data = yaml.safe_load(
        (ROOT / "docs" / "capability" / "bybit-v1.yaml").read_text(encoding="utf-8")
    )
    capability = capability_contract_from_manifest(capability_data)
    risk_policy = engineering_default_policy()
    information = actual_observed(source_id="golden", data_type="audit", received_at_ns=1, available_at_ns=2)
    plan = make_valid_plan()
    return {
        "capability_contract": capability.contract_version,
        "information_contract": information.contract_version,
        "risk_policy_contract": risk_policy.to_dict()["contract_version"],
        "trade_plan_contract": plan.contract_version,
        "strategy_id": CRYPTO_TREND_24H_V1,
        "strategy_version": STRATEGY_VERSION,
        "sqlite_schema_version": SCHEMA_VERSION,
    }


def test_v1_golden_baseline_recomputes_from_frozen_code_and_fixtures() -> None:
    baseline = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))
    assert baseline["source_commit"] == BASELINE_COMMIT
    assert baseline["contract_versions"] == _contract_versions()
    assert baseline["dependency_lock"]["sha256"] == hashlib.sha256(
        (ROOT / "requirements-lock.txt").read_bytes()
    ).hexdigest()
    expected_version, expected_wheel_hash = _lock_nautilus_pin()
    assert expected_version == EXPECTED_NAUTILUS_VERSION
    assert expected_wheel_hash == EXPECTED_NAUTILUS_WHEEL_SHA256
    assert baseline["nautilus"]["expected"]["version"] == expected_version
    assert baseline["nautilus"]["expected"]["wheel_sha256"] == expected_wheel_hash
    observed = _nautilus_observation()
    assert observed["distribution_version"] == expected_version
    assert baseline["nautilus"]["observed"] == observed
    assert baseline["golden_values"] == _recomputed_golden_values()
