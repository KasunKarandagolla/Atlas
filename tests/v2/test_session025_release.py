"""Session-025 deterministic release classifier and manifest contract tests."""

from __future__ import annotations

import copy

import pytest

from atlas.v2.release import (
    REQUIRED_RELEASE_FIELDS,
    ReleaseClassificationV2,
    ReleaseReadinessV2,
    classify_release,
    seal_release_manifest,
    validate_release_manifest,
)


def _base_readiness(**changes: bool) -> ReleaseReadinessV2:
    values = {
        "deterministic_regression_passed": True,
        "research_scan_desktop_usable": True,
        "release_manifest_complete": True,
        "critical_engineering_defect": False,
        "capital_enabled": False,
        "missing_evidence_surfaced": True,
    }
    values.update(changes)
    return ReleaseReadinessV2(**values)


def test_release_classifier_releases_shadow_without_inventing_external_gates():
    assert classify_release(_base_readiness()) == ReleaseClassificationV2.SHADOW_RELEASED


@pytest.mark.parametrize(
    "changes",
    (
        {"deterministic_regression_passed": False},
        {"research_scan_desktop_usable": False},
        {"release_manifest_complete": False},
        {"critical_engineering_defect": True},
        {"missing_evidence_surfaced": False},
        {"capital_enabled": True},
    ),
)
def test_release_classifier_blocks_unsafe_shadow_state(changes):
    assert classify_release(_base_readiness(**changes)) == ReleaseClassificationV2.BLOCKED


def test_assisted_canary_requires_every_independent_external_gate():
    readiness = _base_readiness(
        exact_venue_profile_supported=True,
        protection_recovery_qualified=True,
        external_writer_fencing_demonstrated=True,
        economic_gate_passed=True,
        exact_policy_promoted=True,
        prospective_evidence_satisfied=True,
        live_risk_policy_explicitly_approved=True,
    )
    assert classify_release(readiness) == ReleaseClassificationV2.ASSISTED_CANARY_ELIGIBLE
    assert classify_release(
        _base_readiness(
            exact_venue_profile_supported=True,
            protection_recovery_qualified=True,
            external_writer_fencing_demonstrated=True,
            economic_gate_passed=True,
            exact_policy_promoted=True,
            prospective_evidence_satisfied=True,
        )
    ) == ReleaseClassificationV2.SHADOW_RELEASED


def test_release_manifest_is_complete_canonical_and_tamper_evident():
    payload = dict.fromkeys(REQUIRED_RELEASE_FIELDS)
    payload.update(
        {
            "release_version": "2.0.0.dev25",
            "git_sha": "a" * 40,
            "capital_enabled": False,
            "release_classification": "SHADOW_RELEASED",
        }
    )
    manifest = seal_release_manifest(payload)
    expected_hash = validate_release_manifest(manifest)
    assert expected_hash == manifest["content_hash"]

    changed = copy.deepcopy(manifest)
    changed["payload"]["capital_enabled"] = True
    with pytest.raises(ValueError, match="hash mismatch"):
        validate_release_manifest(changed)

    incomplete = seal_release_manifest({"release_version": "2.0.0.dev25"})
    with pytest.raises(ValueError, match="incomplete"):
        validate_release_manifest(incomplete)


def test_release_manifest_rejects_unknown_envelope_fields():
    payload = dict.fromkeys(REQUIRED_RELEASE_FIELDS)
    manifest = seal_release_manifest(payload)
    manifest["unexpected"] = "value"
    with pytest.raises(ValueError, match="fields"):
        validate_release_manifest(manifest)
